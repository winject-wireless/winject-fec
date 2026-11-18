#ifndef WINJECT_FEC_WRITE_STREAM_HPP
#define WINJECT_FEC_WRITE_STREAM_HPP

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

#include <bfc/event_queue.hpp>
#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/block_encoder.hpp"
#include "winject_fec/stream_reactor.hpp"

namespace winject::fec
{

/* A shard leaving a write stream: type plain goes out as LC plain, data and parity as LC fec. */
struct out_shard
{
    shard_type               type = shard_type::none;
    bfc::shared_sized_buffer buf;
};
using write_output_queue = bfc::reactive_circular_lossy_queue<out_shard>;

/*
 * The sending side of a FEC stream: push() app datagrams, pop() shards when they are due.
 *
 * Each datagram's shard enters the output queue at once (data is never held back). A block closes
 * after k pushes, or flush_ms after its first push, and its parity then waits for its place in the
 * parity spread: row j of a block of c = k_total + parity shards goes at
 *   max(close + spread_offset(j, c), start + spread_offset(k_total + j, c)),
 * so a block's data and parity together span spread_ms from its first push, and parity never
 * leaves in a lump. Blocks overlap, so their shards interleave. One reactor timer, armed at the
 * earliest of the open block's flush and the next parity due, releases them.
 *
 * Output: a bfc lossy ring of out_shard (output()); only due shards enter it, and when the link
 * does not drain it the oldest is dropped (drop_output). Bind it to the consumer's reactor
 * (output()->set_reactor(r); r->add_read_rdy(*output(), cb)) or poll pop(). Buffers are the zero
 * copy layout of block_encoder: [headroom][fec header][data], plain [headroom][data].
 *
 * configure() never touches a running block: the open block keeps the parameters it started
 * with and closes by its k pushes or its flush; the new ones apply from the next block. With FEC
 * off (k = 0, the default) a push outside a block goes out plain.
 *
 * Reactor: the stream runs on one (attach()). It is driven by three things there: its input ring,
 * a bfc lossy ring the reactor drains when a push from another thread lands in it (one mutex, no
 * allocation; when the reactor falls behind the oldest is dropped, drop_input); its one timer;
 * and the rare control calls (configure, set_*, poll), posted onto it after the pushes made
 * before them. A push on the reactor thread runs at once. Without a reactor (manual mode)
 * everything runs on the caller's thread, which calls poll() at next_deadline().
 *
 * Teardown: any thread, the reactor running or not.
 */
class fec_write_stream
{
public:
    using time_point = std::chrono::steady_clock::time_point;
    using clock_fn = std::function<time_point()>;

    struct config
    {
        int    k = 0; /* 0: no FEC (plain) */
        int    n = 0;
        size_t max_datagram = 0;
        int    spread_ms = 0; /* 0 .. k_spread_cap_ms */
        int    flush_ms = 0;  /* > 0 when the block has parity */
    };

    fec_write_stream();
    ~fec_write_stream();
    fec_write_stream(const fec_write_stream&) = delete;
    fec_write_stream& operator=(const fec_write_stream&) = delete;

    /* Runs the stream on this reactor (by name in bfc::cv_reactor_registry). Set up before use. */
    bool attach(std::string_view reactor_name);
    void attach(std::shared_ptr<stream_reactor> reactor);
    /* Manual mode and tests: replaces steady_clock::now(). */
    void set_clock(clock_fn fn);

    /* Room every shard keeps in front for the next layers, and the parity buffer source (heap
     * when none). push_offset() changes at once; pushes queued before keep the old layout. */
    void set_tx_buffers(size_t headroom, shard_alloc alloc);
    [[nodiscard]] size_t push_offset() const;
    /* Output ring size in shards (default 256). */
    void set_output_capacity(size_t shards);
    /* Input ring size in datagrams (default 256): pushes from other threads waiting for the
     * reactor. */
    void set_input_capacity(size_t datagrams);

    /* False (nothing changes) on invalid parameters. Takes effect from the next block. */
    bool configure(const config& cfg);
    [[nodiscard]] static bool valid(const config& cfg);
    /* Largest app datagram for a config (SIZE_MAX without FEC: the link bounds it). */
    [[nodiscard]] static size_t max_original(const config& cfg);

    /* datagram = [headroom][fec slot][data] (push_offset() bytes before the data). Any thread. */
    void push(bfc::shared_sized_buffer datagram);
    /* The next due shard, or type none. Any thread. */
    out_shard                           pop();
    std::shared_ptr<write_output_queue> output() const;

    /* Manual mode: releases due parity and closes blocks past their flush. */
    void poll();
    /* Manual mode: when poll() next has work; false when nothing waits. */
    bool next_deadline(time_point* at) const;

    // Overrides the random start of the data sequence (tests: the u16 wrap).
    void     set_tx_sequence(uint16_t seq);
    uint16_t tx_sequence() const;

    const char* impl_name() const;

    struct counters
    {
        uint64_t plain = 0;
        uint64_t data = 0;
        uint64_t parity = 0;
        uint64_t blocks = 0;
        /* Blocks closed by flush_ms short of k. */
        uint64_t flushed_blocks = 0;
        /* Datagrams dropped by push(): too short for push_offset(), empty or too long. */
        uint64_t drop_no_headroom = 0;
        uint64_t drop_size = 0;
        /* Blocks that lost their parity for want of a buffer. */
        uint64_t drop_parity_alloc = 0;
        /* Shards the output ring dropped (oldest first) because the link did not drain it. */
        uint64_t drop_output = 0;
        uint64_t encode_kernel_us = 0;
        /* Pushes the input ring dropped (oldest first) because the reactor did not drain it. */
        uint64_t drop_input = 0;
    };
    counters get_counters() const;

    struct impl;

private:
    void control(std::function<void(impl&)> fn);

    std::shared_ptr<impl> d;
};

}  // namespace winject::fec

#endif  // WINJECT_FEC_WRITE_STREAM_HPP
