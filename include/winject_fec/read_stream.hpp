#ifndef WINJECT_FEC_READ_STREAM_HPP
#define WINJECT_FEC_READ_STREAM_HPP

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

#include <bfc/event_queue.hpp>
#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/stream_reactor.hpp"
#include "winject_fec/wire.hpp"

namespace winject::fec
{

using read_output_queue = bfc::reactive_circular_lossy_queue<bfc::shared_sized_buffer>;

/*
 * The receiving side of a FEC stream: push() what the link received, pop() app datagrams.
 *
 * Plain datagrams and data shards go out the moment they arrive (a data shard as a view of the
 * received buffer past its header), so nothing waits behind a hole and output is NOT in sequence
 * order. A data row lost on the air is rebuilt from its block's parity (block_decoder) and goes
 * out as soon as it is. The stream keeps a reference to recent data shards until their blocks
 * can no longer need them for a rebuild: memory, not copies.
 *
 * Give-up: the sender releases parity row j of a block of c shards at
 * max(close + j*spread/c, first + (k_total + j)*spread/c), where close and first are when its
 * last and first data shards were pushed. The receiver learns the spread from the row gap of each
 * block's parity arrivals, and the bias and jitter of the last parity's arrival against that rule.
 * A missing data shard is given up (counted lost, its block's references dropped) when its block's
 * last parity is due + bias + a margin from the jitter. Until two blocks have been measured the
 * wait is k_spread_cap_ms after the close. Without parity (a data-only stream) a hole is given up
 * as soon as a later data shard shows the sender is past it. One reactor timer, armed at the
 * earliest give-up, drives it.
 *
 * Ids: a data shard behind the give-up point by at most k_max_lo_seq_deviation is late (still
 * delivered if it never was); behind by more, or ahead by more than k_max_hi_seq_deviation, it
 * starts a new reference (a new sender session: old state is dropped).
 *
 * Output: a bfc lossy ring (output()); bind it to the consumer's reactor or poll pop(). Threads
 * and manual mode as fec_write_stream.
 */
class fec_read_stream
{
public:
    using time_point = std::chrono::steady_clock::time_point;
    using clock_fn = std::function<time_point()>;

    static constexpr int    k_max_lo_seq_deviation = 128;
    static constexpr int    k_max_hi_seq_deviation = 1024;
    /* RX ring of data shards (power of two, > hi deviation + the longest block). */
    static constexpr size_t k_rx_ring = 2048;
    static constexpr size_t k_block_max = 256;
    /* Floor of the give-up margin; a platform whose scheduling can hold a queued parity shard
     * longer raises it at build time. */
#ifndef WINJECT_FEC_RX_DEADLINE_MARGIN_MIN_MS
#define WINJECT_FEC_RX_DEADLINE_MARGIN_MIN_MS 5
#endif
    static constexpr int k_deadline_margin_min_ms = WINJECT_FEC_RX_DEADLINE_MARGIN_MIN_MS;
    static_assert(k_deadline_margin_min_ms >= 5 && k_deadline_margin_min_ms <= 100);
    /* Data shards without any parity after which the stream counts as data-only. Spread parity
     * can trail its block by many data shards, so this is well above the largest block. */
    static constexpr int k_data_only_after = 256;
    /* The same before any parity has been seen in this session: two of the largest blocks. */
    static constexpr int k_data_only_before_parity = 2 * k_header_k_n_max;

    fec_read_stream();
    ~fec_read_stream();
    fec_read_stream(const fec_read_stream&) = delete;
    fec_read_stream& operator=(const fec_read_stream&) = delete;

    bool attach(std::string_view reactor_name);
    void attach(std::shared_ptr<stream_reactor> reactor);
    void set_clock(clock_fn fn);
    /* Output ring size in datagrams (default 256). */
    void set_output_capacity(size_t datagrams);

    /* One received LC body: type plain is the payload, type fec a shard of the FEC wire. */
    void push(bfc::shared_sized_buffer body, lc_type type);
    /* The next app datagram, or an empty buffer. Any thread. */
    bfc::shared_sized_buffer           pop();
    std::shared_ptr<read_output_queue> output() const;

    /* Manual mode: gives up what is past its deadline. */
    void poll();
    bool next_deadline(time_point* at) const;
    /* Drops all FEC state and the buffers it holds (counters and estimates stay). */
    void reset();

    /* Estimated sender spread and arrival jitter (ms); the cap / 0 before two blocks. Stream
     * thread (or manual mode). */
    [[nodiscard]] bool   have_spread_estimate() const;
    [[nodiscard]] double spread_estimate_ms() const;
    [[nodiscard]] double jitter_estimate_ms() const;

    struct counters
    {
        uint64_t plain = 0;
        /* Data shards delivered as received. */
        uint64_t data = 0;
        /* Data rows rebuilt and delivered. */
        uint64_t recovered = 0;
        /* Blocks rebuilt from parity. */
        uint64_t blocks = 0;
        /* Blocks that lost data: given up, failed to decode or evicted. */
        uint64_t decode_fail = 0;
        uint64_t given_up_blocks = 0;
        uint64_t rs_failures = 0;
        uint64_t evicted_blocks = 0;
        /* Data shards given up (app datagrams lost after FEC). */
        uint64_t missing_data = 0;
        /* Parity shards never seen, counted when their block is done (wire loss). */
        uint64_t missing_parity = 0;
        /* Data shards behind the give-up point (delivered when they never were). */
        uint64_t late = 0;
        uint64_t duplicates = 0;
        uint64_t new_references = 0;
        uint64_t hdr_errors = 0;
        /* Parity whose k/n disagrees with earlier parity of the same block. */
        uint64_t kn_mismatch = 0;
        /* Datagrams the output ring dropped because the consumer did not drain it. */
        uint64_t drop_output = 0;
    };
    counters get_counters() const;

    struct impl;

private:
    std::shared_ptr<impl> d;
};

}  // namespace winject::fec

#endif  // WINJECT_FEC_READ_STREAM_HPP
