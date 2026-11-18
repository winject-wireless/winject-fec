#ifndef WINJECT_FEC_BLOCK_ENCODER_HPP
#define WINJECT_FEC_BLOCK_ENCODER_HPP

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <stddef.h>
#include <stdint.h>

#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/wire.hpp"

namespace winject::fec
{

enum class shard_type : uint8_t
{
    none,   /* nothing queued: pop() returns an empty buffer */
    plain,  /* bypass datagram (LC plain) */
    data,   /* FEC data shard (LC fec) */
    parity, /* FEC parity shard (LC fec) */
};

/* Where a parity shard sits in its block, and when the block started and closed (the anchors
 * of its release time, see spread_offset()). */
struct parity_info
{
    int                                   index = 0; /* row 0..count-1 */
    int                                   count = 0; /* the block's parity count */
    int                                   block_k = 0;
    std::chrono::steady_clock::time_point block_start {};
    std::chrono::steady_clock::time_point block_close {};
};

using shard_alloc = std::function<bfc::shared_sized_buffer(size_t capacity)>;

/*
 * One block at a time: start() it, push() up to k datagrams, pop() what each push queued. The
 * k-th push (or close()) encodes the block and queues its parity behind the last data shard;
 * then the encoder is free for the next start(). It keeps its row memory across blocks.
 *
 *   enc.start(seq, k, n, max_datagram, now);
 *   enc.push(datagram, now);
 *   while (enc.get_state().type != shard_type::none)
 *       send(enc.pop());          // the datagram's data shard, then any parity it released
 *
 * Zero copy: a pushed buffer is [headroom][3-byte fec slot][data] (push_offset() bytes before the
 * data). The data header goes into the slot and the same buffer comes back out. headroom is
 * set_tx_buffers()' room every shard keeps in front for the next layers' headers:
 *
 *   recv          [headroom ][fec slot][data]
 *   fec           [headroom ][fec     ][data]
 *   upstream      [   ][up  ][fec     ][data]
 *   tx mux        [80211][up][fec     ][data]
 *
 * Parity is computed from a copy of each data body (rows in PSRAM with
 * CONFIG_WINJECT_FEC_TX_PSRAM), so a popped data shard is the caller's at once. Parity buffers
 * come from set_tx_buffers()' alloc, laid out [headroom][parity header][parity].
 *
 * A push that cannot be taken is dropped and counted, never undefined: no block open, shards
 * still queued, shorter than push_offset(), no data or longer than max_original().
 */
class block_encoder
{
public:
    using time_point = std::chrono::steady_clock::time_point;

    block_encoder();
    ~block_encoder();
    block_encoder(const block_encoder&) = delete;
    block_encoder& operator=(const block_encoder&) = delete;
    block_encoder(block_encoder&&) noexcept;
    block_encoder& operator=(block_encoder&&) noexcept;

    /* k data shards, n - k parity (n == k: data only); 1 <= k <= n, n - k <= 31; max_datagram
     * bounds every wire datagram of the block. */
    [[nodiscard]] static bool valid(int k, int n, size_t max_datagram);
    /* Largest app datagram a block takes for a wire datagram limit. */
    [[nodiscard]] static size_t max_original(size_t max_datagram)
    {
        return max_app_payload(max_datagram);
    }

    void set_tx_buffers(size_t headroom, shard_alloc alloc);
    [[nodiscard]] size_t headroom() const
    {
        return tx_headroom;
    }
    /* Where the data starts in a pushed buffer: the headroom plus the fec header slot. */
    [[nodiscard]] size_t push_offset() const
    {
        return tx_headroom + k_data_header_len;
    }

    /* Opens a block whose first data shard is base. False if a block is still open or shards
     * are still queued, or on invalid parameters. */
    bool start(uint16_t base, int k, int n, size_t max_datagram, time_point now);
    /* Takes one datagram into the open block and queues its data shard; the k-th also encodes
     * the block (parity queued behind it, block closed). */
    void push(bfc::shared_sized_buffer datagram, time_point now);
    /* Closes a block short of k data shards and queues its parity. No-op when none is open. */
    void close(time_point now);

    [[nodiscard]] bool is_open() const
    {
        return open;
    }
    [[nodiscard]] int block_k() const
    {
        return cfg_k;
    }
    [[nodiscard]] int block_n() const
    {
        return cfg_n;
    }
    /* Data shards in the open block so far. */
    [[nodiscard]] int rows() const
    {
        return open_rows;
    }
    [[nodiscard]] uint16_t base() const
    {
        return block_base;
    }
    [[nodiscard]] time_point block_start() const
    {
        return block_start_;
    }

    /* The shard at the head of the queue. */
    struct state
    {
        shard_type  type = shard_type::none;
        parity_info parity; /* type parity only */
    };
    [[nodiscard]] state      get_state() const;
    bfc::shared_sized_buffer pop();
    [[nodiscard]] size_t     queued() const
    {
        return queue.size();
    }

    const char* impl_name() const;

    uint64_t blocks() const
    {
        return blocks_count;
    }
    /* Datagrams dropped by push(). */
    uint64_t drop_not_open() const
    {
        return drop_not_open_;
    }
    uint64_t drop_queued() const
    {
        return drop_queued_;
    }
    uint64_t drop_no_headroom() const
    {
        return drop_no_headroom_;
    }
    uint64_t drop_size() const
    {
        return drop_size_;
    }
    /* Blocks whose parity was lost for want of a parity buffer. */
    uint64_t drop_parity_alloc() const
    {
        return drop_parity_alloc_;
    }
    uint64_t encode_kernel_us() const
    {
        return encode_kernel_us_;
    }

private:
    struct queued_shard
    {
        bfc::shared_sized_buffer buf;
        state                    st;
    };
    [[nodiscard]] bfc::shared_sized_buffer alloc_parity(size_t body_len);
    uint8_t*                               row(int i) const;
    void                                   encode_block(time_point now);

    bool   open = false;
    int    cfg_k = 0;
    int    cfg_n = 0;
    int    p = 0;
    size_t max_datagram_ = 0;

    /* Data copies: k rows of row_stride, 16-byte aligned. Kept and reused while big enough. */
    bfc::shared_sized_buffer rows_mem;
    size_t                   row_stride = 0;
    size_t                   row_len[k_header_k_n_max] {};
    int                      open_rows = 0;

    uint16_t                 block_base = 0;
    time_point               block_start_ {};
    size_t                   tx_headroom = 0;
    shard_alloc              tx_alloc;
    std::deque<queued_shard> queue;

    uint64_t blocks_count = 0;
    uint64_t drop_not_open_ = 0;
    uint64_t drop_queued_ = 0;
    uint64_t drop_no_headroom_ = 0;
    uint64_t drop_size_ = 0;
    uint64_t drop_parity_alloc_ = 0;
    uint64_t encode_kernel_us_ = 0;
};

}  // namespace winject::fec

#endif  // WINJECT_FEC_BLOCK_ENCODER_HPP
