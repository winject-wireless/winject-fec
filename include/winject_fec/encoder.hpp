#ifndef WINJECT_FEC_ENCODER_HPP
#define WINJECT_FEC_ENCODER_HPP

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/wire.hpp"

namespace winject::fec
{

/*
 * FIFO encoder: push() an app datagram, then pop() until get_state() says none.
 *
 *   enc.push(datagram);
 *   while (enc.get_state().type != encoder::shard_type::none)
 *       send(enc.pop());          // the datagram's shard first, then any parity it released
 *
 * Zero copy: the caller receives the datagram into a buffer with room in front, and every layer
 * writes its header into that room in place; the encoder hands the same buffer back.
 *
 *   recv          [headroom ][fec slot][data]     pushed (push_offset() bytes before data)
 *   fec (this)    [headroom ][fec     ][data]     popped
 *   upstream      [   ][up  ][fec     ][data]
 *   tx mux        [80211][up][fec     ][data]
 *
 * headroom is set_tx_buffers()' headroom: the room every popped shard keeps in front for the next
 * layers. The 3-byte fec slot is reserved in every mode, so where the data starts does not depend
 * on whether FEC is on.
 *
 * Bypass (the default, no FEC): the slot is cut off the front and the rest goes out untouched,
 * [headroom][data] (shard_type::plain). configure(k, n) turns FEC on: the data header goes into
 * the slot (shard_type::data); every k data shards close a block and queue its n - k parity
 * shards (shard_type::parity) behind it, new buffers laid out the same way,
 * [headroom][parity header][parity].
 *
 * Every push either queues exactly one plain/data shard or drops the datagram and counts why:
 * shards still queued (pop them all first), shorter than push_offset(), empty / too long. Parity
 * is computed from a copy the encoder keeps, so a popped data shard is the caller's at once.
 */
class encoder
{
public:
    using clock_fn = std::function<std::chrono::steady_clock::time_point()>;
    /* Replaces steady_clock::now() (tests). */
    void set_clock(clock_fn fn)
    {
        clock = std::move(fn);
    }

    encoder();
    ~encoder();
    encoder(const encoder&) = delete;
    encoder& operator=(const encoder&) = delete;
    encoder(encoder&&) noexcept;
    encoder& operator=(encoder&&) noexcept;

    enum class shard_type : uint8_t
    {
        none,   /* queue empty: pop() returns an empty buffer */
        plain,  /* bypass datagram (LC plain) */
        data,   /* FEC data shard (LC fec) */
        parity, /* FEC parity shard (LC fec) */
    };
    /* The shard at the head of the queue. */
    struct state
    {
        shard_type type = shard_type::none;
        /* parity: its row 0..parity_count-1, the block's data count, and when its first data
         * shard was pushed (the anchor for spread_offset()). */
        int                                   parity_index = 0;
        int                                   parity_count = 0;
        int                                   block_k = 0;
        std::chrono::steady_clock::time_point block_start {};
    };

    /*
     * FEC on: k data shards per block, n - k parity shards (n == k: data shards only, no parity).
     * max_datagram bounds the whole wire datagram. Valid at any time: a partial block still open
     * is closed under its old parameters and its parity queued (as flush() would); the next block
     * uses the new ones. Configuring the same parameters leaves the block open. Returns false and
     * changes nothing on invalid parameters. The data sequence counter keeps running across
     * configure() and configure_bypass(), so a runtime change does not replay ids the receiver
     * already saw.
     */
    bool configure(int k, int n, size_t max_datagram);
    /* Back to bypass (the default). The open block's parity is queued first. */
    void configure_bypass();
    /* FEC on (configure()), or bypass. */
    bool fec() const
    {
        return active;
    }
    int k() const
    {
        return cfg_k;
    }
    int n() const
    {
        return cfg_n;
    }
    const char* impl_name() const;
    /* Largest app datagram push() takes with FEC; the same for every k/n. In bypass there is no
     * limit here (SIZE_MAX): the caller's link bounds it. */
    [[nodiscard]] size_t max_original() const
    {
        return active ? max_app_payload(max_datagram_) : SIZE_MAX;
    }
    /* Where the data starts in a pushed buffer: the headroom plus the fec header slot. */
    [[nodiscard]] size_t push_offset() const
    {
        return tx_headroom + k_data_header_len;
    }

    [[nodiscard]] uint16_t tx_sequence() const
    {
        return tx_seq;
    }
    // Overrides the random start of the sequence; tests use it to cross the u16 wrap.
    void set_tx_sequence(uint16_t seq)
    {
        tx_seq = seq;
    }

    /*
     * headroom: bytes every popped shard keeps in front of its FEC header (plain: of its data)
     * for the next layers' headers; pushed buffers carry it too (push_offset()). Parity buffers
     * come from alloc(capacity), which must return at least that much; without one they are heap
     * allocated.
     */
    using shard_alloc = std::function<bfc::shared_sized_buffer(size_t capacity)>;
    void set_tx_buffers(size_t headroom, shard_alloc alloc);

    /* datagram = [headroom][fec slot][data]. Queues its plain or data shard (the same buffer),
     * plus the block's parity when it closes the block; or drops it (see the drop counters). */
    void push(bfc::shared_sized_buffer datagram);
    /* The head of the queue (described by get_state()), or an empty buffer when none. */
    bfc::shared_sized_buffer pop();
    state                    get_state() const;
    /* Shards queued. */
    size_t queued() const
    {
        return queue.size();
    }
    /* Closes the open block early (e.g. on a send timeout) and queues its parity. */
    void flush();

    /* Blocks encoded (parity produced). */
    uint64_t blocks() const
    {
        return blocks_count;
    }
    /* Datagrams dropped by push(): shards still queued, shorter than push_offset(), no data or
     * longer than max_original(). */
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
    bool                                   encode_block();

    bool   active = false;
    int    cfg_k = 0;
    int    cfg_n = 0;
    int    p = 0;
    size_t max_datagram_ = 0;
    /* Encode tables of a full block (k = cfg_k); short blocks look theirs up. */
    const uint8_t* full_g_tbls = nullptr;

    /* The open block's data bodies: cfg_k rows of row_stride, 16-byte aligned. */
    std::unique_ptr<uint8_t[]> rows_mem;
    uint8_t*                   rows_base = nullptr;
    size_t                     row_stride = 0;
    size_t                     row_len[k_header_k_n_max] {};
    int                        open_rows = 0;

    uint16_t                              block_base = 0;
    std::chrono::steady_clock::time_point block_start_ {};
    size_t                                tx_headroom = 0;
    shard_alloc                           tx_alloc;
    uint16_t                              tx_seq = 0;
    std::deque<queued_shard>              queue;

    clock_fn clock;

    uint64_t blocks_count = 0;
    uint64_t drop_queued_ = 0;
    uint64_t drop_no_headroom_ = 0;
    uint64_t drop_size_ = 0;
    uint64_t drop_parity_alloc_ = 0;
    uint64_t encode_kernel_us_ = 0;
};

}  // namespace winject::fec

#endif  // WINJECT_FEC_ENCODER_HPP
