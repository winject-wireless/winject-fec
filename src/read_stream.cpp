/* Read stream: in-order release, block rebuild, spread estimate and give-up. */
#include "winject_fec/read_stream.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <map>
#include <utility>
#include <vector>

#include "winject_fec/block_decoder.hpp"

namespace winject::fec
{

namespace
{

using clock_type = std::chrono::steady_clock;

constexpr size_t k_default_output_capacity = 256;
constexpr size_t k_default_input_capacity = 256;
/* How far past a block's data the receiver looks for the next received data shard. */
constexpr int k_close_scan = 64;

double median(std::vector<double>* v)
{
    const size_t mid = v->size() / 2;
    std::nth_element(v->begin(), v->begin() + static_cast<std::ptrdiff_t>(mid), v->end());
    const double hi = (*v)[mid];
    if (v->size() % 2 != 0)
    {
        return hi;
    }
    const double lo = *std::max_element(v->begin(), v->begin() + static_cast<std::ptrdiff_t>(mid));
    return 0.5 * (lo + hi);
}

std::chrono::nanoseconds seconds_ns(double s)
{
    return std::chrono::nanoseconds(static_cast<int64_t>(s * 1e9));
}

double seconds(clock_type::duration d)
{
    return std::chrono::duration<double>(d).count();
}

/*
 * When the last parity of a block is due, after its close. The sender releases parity row j of a
 * block of c = k_total + n shards at max(close + j*g, first + (k_total + j)*g), g = spread / c,
 * so a block that filled in a burst sends its parity over about the whole spread, and one that
 * filled slowly sends it right after the close.
 */
double last_parity_after_close(double fill_s, int k_total, int n, double spread_s)
{
    const double g = spread_s / static_cast<double>(k_total + n);
    return std::max(g * (n - 1), g * (k_total + n - 1) - fill_s);
}

int seq_dist(uint16_t a, uint16_t b)
{
    return static_cast<int>(static_cast<int16_t>(static_cast<uint16_t>(a - b)));
}

}  // namespace

struct fec_read_stream::impl : std::enable_shared_from_this<impl>
{
    struct in_body
    {
        bfc::shared_sized_buffer buf;
        lc_type                  type = lc_type::plain;
    };

    struct rx_slot
    {
        /* The app datagram (received or rebuilt); empty = none. Kept past its release while a
         * block may still rebuild with it. */
        bfc::shared_sized_buffer payload;
        uint16_t                 seq = 0;
        /* Received (not rebuilt): its arrival dates its block's close. */
        bool       received = false;
        time_point at {};
    };
    struct rx_block
    {
        int           k_total = 0;
        int           n = 0;
        block_decoder dec; /* the parity bodies until the block is done */
        uint32_t      parity_rows = 0;
        int           parity_have = 0;
        /* (row, arrival) of the parity received, for the spread fit. */
        std::vector<std::pair<int, time_point>> arrivals;
        time_point                              last_seen {};
        /* Rebuilt, complete without a rebuild, or failed: nothing more to recover. */
        bool done = false;
        /* Counted and learnt from; kept only so late parity of it is dropped. */
        bool finished = false;
    };

    /* Before link: the reactor points at its ring until link lets go. */
    detail::stream_input<in_body, impl> in;
    detail::reactor_link               link;
    clock_fn                           clock;
    std::shared_ptr<read_output_queue> out = std::make_shared<read_output_queue>();
    size_t                             out_capacity = 0;

    std::vector<rx_slot>         ring = std::vector<rx_slot>(k_rx_ring);
    std::map<uint16_t, rx_block> rx_blocks; /* keyed by base; ordered via seq_dist when needed */
    bool                         emit_base_set = false;
    /* The next data shard to release: every one before it is released or given up. */
    uint16_t emit_next = 0;
    uint16_t highest_seen = 0;
    /* Newest block seen in parity, for holes whose block has no parity yet. Blocks after it are
     * taken to be layout_step_k long: the larger k of it and the block before, so a short
     * flushed block does not shorten the blocks that follow. */
    bool     layout_known = false;
    uint16_t layout_base = 0;
    int      layout_k = 0;
    int      layout_n = 0;
    int      layout_step_k = 0;
    int      data_since_parity = 0;
    bool     parity_seen = false;
    /* When the current run of holes was first seen (bounds the wait when the close of their
     * block is unknown), and when the hole at emit_next is given up. */
    bool       hole_timed = false;
    time_point hole_since {};
    bool       hole_wait = false;
    time_point hole_until {};
    /* Last data shard given up and its block (given_up_blocks counts each block once; a run of
     * holes with no known block counts once). */
    bool     gave_up_any = false;
    bool     gave_up_known = false;
    uint16_t gave_up_base = 0;
    uint16_t gave_up_last = 0;

    /* Running sender spread, last-parity bias and jitter estimates, seconds. */
    double spread_est_s = 0.;
    double bias_est_s = 0.;
    double jitter_est_s = 0.;
    int    spread_blocks = 0;
    int    fitted_blocks = 0;

    stream_counter c_plain, c_data, c_recovered, c_blocks, c_given_up_blocks, c_rs_failures,
        c_evicted, c_missing_data, c_missing_parity, c_late, c_dup, c_new_ref, c_hdr_errors,
        c_kn_mismatch, c_drop_output, c_drop_input;

    impl()
    {
        set_output_capacity(k_default_output_capacity);
        set_input_capacity(k_default_input_capacity);
    }

    void set_input_capacity(size_t n)
    {
        in.set_capacity(n);
    }

    /* Any thread: queues the body for the reactor (oldest dropped when full). */
    void enqueue(bfc::shared_sized_buffer&& body, lc_type type)
    {
        if (!in.push(in_body {std::move(body), type}))
        {
            c_drop_input.add_shared();
        }
    }

    /* Stream thread: what other threads gave, in order, before anything after it. */
    void drain_input()
    {
        in.drain(*this, [this](in_body&& b) { push(std::move(b.buf), b.type); });
    }

    void set_output_capacity(size_t n)
    {
        out->set_capacity(n);
        out_capacity = n;
    }

    time_point now() const
    {
        return clock ? clock() : clock_type::now();
    }

    rx_slot* slot_for(uint16_t seq)
    {
        return &ring[seq & (k_rx_ring - 1)];
    }

    void emit(bfc::shared_sized_buffer buf)
    {
        if (out->push(std::move(buf)) == out_capacity && out_capacity > 0)
        {
            c_drop_output++;
        }
    }

    bool have_spread_estimate() const
    {
        return fitted_blocks >= 2;
    }

    /* --- spread estimate --- */

    std::chrono::nanoseconds margin_now() const
    {
        const double floor_s = k_deadline_margin_min_ms / 1000.0;
        return seconds_ns(have_spread_estimate() ? std::max(floor_s, 3.0 * jitter_est_s)
                                                 : floor_s);
    }

    /* The block closed when its last data shard was pushed: that shard's arrival, else the first
     * later data shard received (pushed after the close), else its first parity. */
    bool close_time(uint16_t base, int k_total, time_point* at) const
    {
        bool       found = false;
        time_point best {};
        const auto consider = [&](time_point t)
        {
            if (!found || t < best)
            {
                best = t;
                found = true;
            }
        };
        for (int j = k_total - 1; j < k_total + k_close_scan; j++)
        {
            const uint16_t seq = static_cast<uint16_t>(base + j);
            if (seq_dist(seq, highest_seen) > 0)
            {
                break;
            }
            const rx_slot& s = ring[seq & (k_rx_ring - 1)];
            if (s.seq == seq && s.received)
            {
                consider(s.at);
                break;
            }
        }
        if (const auto it = rx_blocks.find(base); it != rx_blocks.end())
        {
            for (const auto& [row, t] : it->second.arrivals)
            {
                (void)row;
                consider(t);
            }
        }
        if (found && nullptr != at)
        {
            *at = best;
        }
        return found;
    }

    /* The block's first data shard's arrival, else its earliest data shard received. A later
     * shard was pushed after the first, so the estimate errs toward a later deadline. */
    bool first_time(uint16_t base, int k_total, time_point* at) const
    {
        bool found = false;
        for (int j = 0; j < k_total; j++)
        {
            const uint16_t seq = static_cast<uint16_t>(base + j);
            const rx_slot& s = ring[seq & (k_rx_ring - 1)];
            if (s.seq == seq && s.received && (!found || s.at < *at))
            {
                *at = s.at;
                found = true;
            }
        }
        return found;
    }

    bool hole_deadline(uint16_t base, int k_total, int n, time_point* at) const
    {
        time_point close {};
        if (!close_time(base, k_total, &close))
        {
            return false;
        }
        const double cap = k_spread_cap_ms / 1000.0;
        double       after = cap;
        if (have_spread_estimate())
        {
            /* Not knowing when the block started means a burst: the longest wait. */
            time_point first = close;
            (void)first_time(base, k_total, &first);
            const double fill = std::max(0., seconds(close - first));
            after = std::clamp(last_parity_after_close(fill, k_total, n, spread_est_s) + bias_est_s,
                               0., cap);
        }
        *at = close + seconds_ns(after) + margin_now();
        return true;
    }

    /*
     * Theil-Sen over the block's parity arrivals against row gives the row gap (the sender's
     * spread over the block's shards) and where the last row lands. The difference from where
     * the sender's release rule puts it, given the block's own close and fill time, is the bias;
     * the scatter of both is the jitter. Blocks are weighted by the parity they gave.
     */
    void learn_block(uint16_t base, const rx_block& block)
    {
        const auto& a = block.arrivals;
        if (a.empty() || block.n < 1)
        {
            return;
        }
        /* Only a block whose first and last data shards were received has an exact fill and
         * close. */
        const uint16_t last = static_cast<uint16_t>(base + block.k_total - 1);
        const rx_slot& ls = ring[last & (k_rx_ring - 1)];
        const rx_slot& fs = ring[base & (k_rx_ring - 1)];
        if (!(ls.seq == last && ls.received && fs.seq == base && fs.received))
        {
            return;
        }
        const auto   close = ls.at;
        const double fill = std::max(0., seconds(close - fs.at));
        const double c = static_cast<double>(block.k_total + block.n);
        const auto   sec = [close](time_point t) { return seconds(t - close); };
        double       gap = -1.;
        double       resid = 0.;
        double       last_s = 0.;
        if (a.size() >= 2)
        {
            std::vector<double> slopes;
            for (size_t i = 0; i < a.size(); i++)
            {
                for (size_t j = i + 1; j < a.size(); j++)
                {
                    if (a[i].first != a[j].first)
                    {
                        slopes.push_back((sec(a[j].second) - sec(a[i].second)) /
                                         static_cast<double>(a[j].first - a[i].first));
                    }
                }
            }
            if (!slopes.empty())
            {
                gap = std::max(0., median(&slopes));
                std::vector<double> icepts;
                for (const auto& [row, at] : a)
                {
                    icepts.push_back(sec(at) - gap * row);
                }
                const double icept = median(&icepts);
                for (const auto& [row, at] : a)
                {
                    resid = std::max(resid, std::fabs(sec(at) - (icept + gap * row)));
                }
                last_s = icept + gap * (block.n - 1);
            }
        }
        const double cap = k_spread_cap_ms / 1000.0;
        if (gap < 0.)
        {
            /* One row: place the last one with the spread learnt so far (none: same as this). */
            const double g = spread_blocks > 0 ? spread_est_s / c : 0.;
            last_s = sec(a.front().second) + g * (block.n - 1 - a.front().first);
        }
        const double spread = gap >= 0. ? std::min(gap * c, cap) : spread_est_s;
        const double err = std::clamp(
            last_s - last_parity_after_close(fill, block.k_total, block.n, spread), -cap, cap);
        const double weight = static_cast<double>(a.size()) / static_cast<double>(block.n);
        /* The second sample counts as much as the first; later ones move the estimate by up to
         * half their difference, scaled by how much of the block's parity they saw. */
        const auto alpha = [weight](int samples)
        { return 1 == samples ? 0.5 * weight + 0.25 : 0.5 * weight; };
        if (gap >= 0.)
        {
            spread_est_s = 0 == spread_blocks
                               ? spread
                               : spread_est_s + alpha(spread_blocks) * (spread - spread_est_s);
            spread_blocks++;
        }
        if (0 == fitted_blocks)
        {
            bias_est_s = err;
            jitter_est_s = resid;
        }
        else
        {
            const double scatter = std::max(resid, std::fabs(err - bias_est_s));
            bias_est_s += alpha(fitted_blocks) * (err - bias_est_s);
            jitter_est_s += alpha(fitted_blocks) * (scatter - jitter_est_s);
        }
        fitted_blocks++;
    }

    /* --- blocks --- */

    /* The block that holds seq, from parity seen or the last known block layout. */
    bool block_of(uint16_t seq, uint16_t* base, int* k_total, int* n) const
    {
        for (const auto& [b, block] : rx_blocks)
        {
            const int d = seq_dist(seq, b);
            if (d >= 0 && d < block.k_total)
            {
                *base = b;
                *k_total = block.k_total;
                *n = block.n;
                return true;
            }
        }
        if (!layout_known || layout_k < 1 || layout_step_k < 1)
        {
            return false;
        }
        /* Past the newest block seen, blocks follow it at the stream's steady k; a short block
         * (flushed) does not set the step. */
        const uint16_t next = static_cast<uint16_t>(layout_base + layout_k);
        const int      step = layout_step_k;
        if (const int d = seq_dist(seq, next); d >= 0)
        {
            *base = static_cast<uint16_t>(next + (d / step) * step);
            *k_total = step;
        }
        else if (seq_dist(seq, layout_base) >= 0)
        {
            *base = layout_base;
            *k_total = layout_k;
        }
        else
        {
            const int back = -seq_dist(seq, layout_base);
            *base = static_cast<uint16_t>(layout_base - ((back + step - 1) / step) * step);
            *k_total = step;
        }
        *n = layout_n;
        return true;
    }

    void block_done(rx_block* block)
    {
        block->done = true;
        block->dec.release_rows();
    }

    /* Counts the parity it never got and learns from it. The block stays as a finished marker
     * while late parity for it can still arrive, so that parity is dropped instead of opening it
     * again. */
    void finish_block(uint16_t base, rx_block* block)
    {
        if (block->finished)
        {
            return;
        }
        c_missing_parity += static_cast<uint64_t>(block->n - block->parity_have);
        learn_block(base, *block);
        block->finished = true;
        block_done(block);
        block->arrivals.clear();
    }

    /* A block is done once all its data is behind the release point and its parity has all come
     * (or stopped coming). It is forgotten once parity for it would be too late to look at. */
    void prune_blocks(time_point t)
    {
        for (auto it = rx_blocks.begin(); it != rx_blocks.end();)
        {
            rx_block&  block = it->second;
            const int  end = seq_dist(static_cast<uint16_t>(it->first + block.k_total), emit_next);
            const bool stale = end < -k_max_lo_seq_deviation;
            const bool quiet = t - block.last_seen > std::chrono::milliseconds(k_spread_cap_ms);
            if (!block.finished && end <= 0 && (block.parity_have == block.n || quiet || stale))
            {
                finish_block(it->first, &block);
            }
            if (block.finished && stale)
            {
                it = rx_blocks.erase(it);
                continue;
            }
            ++it;
        }
    }

    /* When a block behind the release point stops waiting for its last parity. */
    bool next_quiet(time_point* at) const
    {
        bool found = false;
        for (const auto& [base, block] : rx_blocks)
        {
            const int end = seq_dist(static_cast<uint16_t>(base + block.k_total), emit_next);
            if (block.finished || end > 0)
            {
                continue;
            }
            const time_point q =
                block.last_seen + std::chrono::milliseconds(k_spread_cap_ms) +
                std::chrono::microseconds(1);
            if (!found || q < *at)
            {
                *at = q;
                found = true;
            }
        }
        return found;
    }

    void try_decode(uint16_t base, rx_block* block)
    {
        if (block->done)
        {
            return;
        }
        int  present = 0;
        bool needed = false;
        for (int j = 0; j < block->k_total; j++)
        {
            const uint16_t seq = static_cast<uint16_t>(base + j);
            const rx_slot* s = slot_for(seq);
            if (s->seq == seq && !s->payload.empty())
            {
                present++;
            }
            else if (seq_dist(seq, emit_next) >= 0)
            {
                needed = true;
            }
        }
        if (present == block->k_total)
        {
            block_done(block);
            return;
        }
        if (!needed || present + block->parity_have < block->k_total)
        {
            return;
        }
        for (int j = 0; j < block->k_total; j++)
        {
            const uint16_t seq = static_cast<uint16_t>(base + j);
            const rx_slot* s = slot_for(seq);
            if (s->seq == seq && !s->payload.empty())
            {
                (void)block->dec.push_data(j, s->payload);
            }
        }
        const bool ok = block->dec.decode();
        if (!ok)
        {
            c_rs_failures++;
            block_done(block);
            return;
        }
        c_blocks++;
        const auto t = now();
        while (block->dec.get_state().ready)
        {
            const uint16_t           seq = block->dec.get_state().seq;
            bfc::shared_sized_buffer payload = block->dec.pop();
            rx_slot*                 s = slot_for(seq);
            if ((s->seq == seq && !s->payload.empty()) || seq_dist(seq, emit_next) < 0)
            {
                continue; /* arrived meanwhile, or given up already */
            }
            /* Released in sequence by resolve(). */
            s->payload = std::move(payload);
            s->seq = seq;
            s->received = false;
            s->at = t;
            c_recovered++;
        }
        block_done(block);
    }

    /* --- in-order release --- */

    /* A block needing a rebuild has data at or past the release point and is at most
     * k_header_k_n_max long, so a payload further behind than that is never read again. */
    void advance_emit()
    {
        emit_next++;
        const uint16_t old = static_cast<uint16_t>(emit_next - 1 - k_header_k_n_max);
        if (rx_slot* s = slot_for(old); s->seq == old)
        {
            s->payload = {};
        }
    }

    void clear_session()
    {
        for (rx_slot& s : ring)
        {
            s = rx_slot {};
        }
        rx_blocks.clear();
        hole_timed = false;
        hole_wait = false;
        gave_up_any = false;
        /* A new session has its own block layout. */
        layout_known = false;
        data_since_parity = 0;
        parity_seen = false;
    }

    void reset()
    {
        clear_session();
        emit_base_set = false;
    }

    void new_reference(uint16_t seq)
    {
        /* The old session's blocks are over: count their missing parity while the ring still
         * has their data. */
        for (auto& [base, block] : rx_blocks)
        {
            finish_block(base, &block);
        }
        clear_session();
        emit_next = seq;
        highest_seen = seq;
        c_new_ref++;
    }

    /* Releases everything that can go out in sequence and gives up holes past their deadline. */
    void resolve(time_point t)
    {
        hole_wait = false;
        if (!emit_base_set)
        {
            return;
        }
        while (seq_dist(highest_seen, emit_next) >= 0)
        {
            rx_slot* s = slot_for(emit_next);
            if (s->seq == emit_next && !s->payload.empty())
            {
                /* The ring keeps its own handle: the block may still need it to rebuild a later
                 * hole. */
                if (s->received)
                {
                    c_data++;
                }
                emit(s->payload);
                advance_emit();
                hole_timed = false;
                continue;
            }
            /* A run of holes shares one wait: it starts at the first hole and ends at the next
             * shard released. */
            if (!hole_timed)
            {
                hole_timed = true;
                hole_since = t;
            }
            uint16_t   base = emit_next;
            int        k_total = 0;
            int        n = 0;
            const bool have_block = block_of(emit_next, &base, &k_total, &n);
            if (data_since_parity <= (parity_seen ? k_data_only_after : k_data_only_before_parity))
            {
                time_point deadline = hole_since + std::chrono::milliseconds(k_spread_cap_ms);
                if (have_block)
                {
                    (void)hole_deadline(base, k_total, n, &deadline);
                }
                if (t < deadline)
                {
                    hole_wait = true;
                    hole_until = deadline;
                    break;
                }
            }
            /* Given up: the sender is past it and its parity is overdue (or there is none). A
             * run of holes with no known block counts as one block. */
            c_missing_data++;
            const bool same_block =
                gave_up_any &&
                (have_block ? gave_up_known && gave_up_base == base
                            : !gave_up_known &&
                                  static_cast<uint16_t>(gave_up_last + 1) == emit_next);
            if (!same_block)
            {
                c_given_up_blocks++;
            }
            gave_up_any = true;
            gave_up_known = have_block;
            gave_up_base = base;
            gave_up_last = emit_next;
            advance_emit();
        }
    }

    void service(time_point t)
    {
        resolve(t);
        prune_blocks(t);
    }

    bool next_deadline(time_point* at) const
    {
        bool       any = false;
        time_point best {};
        if (hole_wait)
        {
            best = hole_until;
            any = true;
        }
        time_point q {};
        if (next_quiet(&q) && (!any || q < best))
        {
            best = q;
            any = true;
        }
        if (any && nullptr != at)
        {
            *at = best;
        }
        return any;
    }

    void rearm()
    {
        if (!link.attached())
        {
            return;
        }
        time_point at {};
        const bool any = next_deadline(&at);
        link.arm(any, at,
                 [w = weak_from_this()]
                 {
                     if (auto s = w.lock())
                     {
                         s->link.fired();
                         s->service(s->now());
                         s->rearm();
                     }
                 });
    }

    /* --- input --- */

    void push_data(const shard_header& h, bfc::shared_sized_buffer&& shard, time_point t)
    {
        const uint16_t seq = h.sequence_number;
        if (!emit_base_set)
        {
            emit_base_set = true;
            emit_next = seq;
            highest_seen = static_cast<uint16_t>(seq - 1);
        }
        if (data_since_parity <= k_data_only_after)
        {
            data_since_parity++;
        }
        const int d = seq_dist(seq, emit_next);
        if (d < -k_max_lo_seq_deviation || d > k_max_hi_seq_deviation)
        {
            new_reference(seq);
            highest_seen = static_cast<uint16_t>(seq - 1);
        }
        rx_slot* s = slot_for(seq);
        if (s->seq == seq && !s->payload.empty())
        {
            c_dup++; /* duplicate, or already rebuilt */
            return;
        }
        if (seq_dist(seq, emit_next) < 0)
        {
            /* Behind the release point: never released, but it may still help a block rebuild a
             * hole that is not given up yet. */
            c_late++;
            if (seq_dist(seq, emit_next) < -k_header_k_n_max)
            {
                return; /* too far behind to help any block still rebuilding */
            }
        }
        s->payload = shard.subview(h.header_len, shard.size() - h.header_len);
        s->seq = seq;
        s->received = true;
        s->at = t;
        if (seq_dist(seq, highest_seen) > 0)
        {
            highest_seen = seq;
        }
        uint16_t base = 0;
        int      k_total = 0;
        int      n = 0;
        if (block_of(seq, &base, &k_total, &n))
        {
            if (const auto it = rx_blocks.find(base); it != rx_blocks.end())
            {
                try_decode(base, &it->second);
            }
        }
    }

    void push_parity(const shard_header& h, bfc::shared_sized_buffer&& shard, time_point t)
    {
        const uint16_t base = h.sequence_number;
        data_since_parity = 0;
        parity_seen = true;
        if (!emit_base_set)
        {
            emit_base_set = true;
            emit_next = base;
            highest_seen = static_cast<uint16_t>(base - 1);
        }
        const int d = seq_dist(base, emit_next);
        if (d > k_max_hi_seq_deviation)
        {
            new_reference(base);
            highest_seen = static_cast<uint16_t>(base - 1);
        }
        else if (d + h.k_total < -k_max_lo_seq_deviation)
        {
            /* Spread parity trails its block, so parity of a block far behind is late, not a new
             * session: a new session behind the release point is found by its data. */
            return;
        }
        /* The block's data all exists at the sender, so a lost last data shard is a hole too. */
        if (const uint16_t last = static_cast<uint16_t>(base + h.k_total - 1);
            seq_dist(last, highest_seen) > 0 &&
            seq_dist(last, emit_next) <= k_max_hi_seq_deviation)
        {
            highest_seen = last;
        }
        if (!layout_known || seq_dist(base, layout_base) > 0)
        {
            const int prev_k = layout_known ? layout_k : h.k_total;
            layout_known = true;
            layout_base = base;
            layout_k = h.k_total;
            layout_n = h.n;
            layout_step_k = std::max(layout_k, prev_k);
        }
        auto it = rx_blocks.find(base);
        if (it != rx_blocks.end() && it->second.finished)
        {
            return; /* duplicate or late parity of a block already counted */
        }
        if (it != rx_blocks.end() && (it->second.k_total != h.k_total || it->second.n != h.n))
        {
            c_kn_mismatch++;
            finish_block(it->first, &it->second);
            rx_blocks.erase(it);
            it = rx_blocks.end();
        }
        if (it == rx_blocks.end())
        {
            if (rx_blocks.size() >= k_block_max)
            {
                /* Memory bound: drop the block furthest behind; one still open is evicted. */
                auto old = rx_blocks.begin();
                for (auto j = rx_blocks.begin(); j != rx_blocks.end(); ++j)
                {
                    if (seq_dist(j->first, emit_next) < seq_dist(old->first, emit_next))
                    {
                        old = j;
                    }
                }
                if (!old->second.finished)
                {
                    c_evicted++;
                    finish_block(old->first, &old->second);
                }
                rx_blocks.erase(old);
            }
            rx_block nb;
            nb.k_total = h.k_total;
            nb.n = h.n;
            nb.dec.start(base, h.k_total, h.n);
            it = rx_blocks.emplace(base, std::move(nb)).first;
        }
        rx_block&      block = it->second;
        const uint32_t bit = 1U << h.k_i;
        if (0 != (block.parity_rows & bit))
        {
            return;
        }
        block.parity_rows |= bit;
        block.parity_have++;
        block.arrivals.emplace_back(h.k_i, t);
        block.last_seen = t;
        if (!block.done)
        {
            (void)block.dec.push_parity(h.k_i,
                                        shard.subview(h.header_len, shard.size() - h.header_len));
            try_decode(base, &block);
        }
    }

    void push(bfc::shared_sized_buffer&& body, lc_type type)
    {
        if (body.empty())
        {
            return;
        }
        const time_point t = now();
        if (lc_type::plain == type)
        {
            c_plain++;
            emit(std::move(body));
            return;
        }
        shard_header h;
        if (!parse_header(body.u8(), body.size(), &h))
        {
            c_hdr_errors++;
            return;
        }
        if (h.is_data)
        {
            push_data(h, std::move(body), t);
        }
        else
        {
            push_parity(h, std::move(body), t);
        }
        service(t);
        rearm();
    }
};

fec_read_stream::fec_read_stream() : d(std::make_shared<impl>()) {}

/* As ~fec_write_stream: the input ring leaves the reactor on the reactor thread. */
fec_read_stream::~fec_read_stream()
{
    if (!d || d->link.on_thread())
    {
        return;
    }
    const auto r = d->link.get();
    r->wake_up(
        [d = std::move(d)]
        {
            d->link.unbind_input();
            d->link.disarm();
        });
}

/* A control call: on the stream thread, after the pushes queued before it. */
void fec_read_stream::control(std::function<void(impl&)> fn)
{
    if (d->link.on_thread())
    {
        d->drain_input();
        fn(*d);
        return;
    }
    d->in.add_control(std::move(fn));
    d->link.get()->wake_up(
        [w = std::weak_ptr<impl>(d)]
        {
            if (auto s = w.lock())
            {
                s->drain_input();
            }
        });
}

bool fec_read_stream::attach(std::string_view reactor_name)
{
    auto r = find_stream_reactor(reactor_name);
    if (!r)
    {
        return false;
    }
    attach(std::move(r));
    return true;
}

void fec_read_stream::attach(std::shared_ptr<stream_reactor> reactor)
{
    d->link.attach(std::move(reactor));
    d->link.bind_input(d->in.ring(),
                       [w = std::weak_ptr<impl>(d)]
                       {
                           if (auto s = w.lock())
                           {
                               s->drain_input();
                           }
                       });
    control([](impl& s) { s.rearm(); });
}

void fec_read_stream::set_clock(clock_fn fn)
{
    d->clock = std::move(fn);
}

void fec_read_stream::set_output_capacity(size_t datagrams)
{
    control([datagrams](impl& s) { s.set_output_capacity(datagrams); });
}

void fec_read_stream::set_input_capacity(size_t bodies)
{
    d->set_input_capacity(bodies);
}

void fec_read_stream::push(bfc::shared_sized_buffer body, lc_type type)
{
    if (d->link.on_thread())
    {
        d->drain_input();
        d->push(std::move(body), type);
        return;
    }
    d->enqueue(std::move(body), type);
}

bfc::shared_sized_buffer fec_read_stream::pop()
{
    auto b = d->out->pop();
    return b ? std::move(*b) : bfc::shared_sized_buffer {};
}

std::shared_ptr<read_output_queue> fec_read_stream::output() const
{
    return d->out;
}

void fec_read_stream::poll()
{
    control(
        [](impl& s)
        {
            s.service(s.now());
            s.rearm();
        });
}

bool fec_read_stream::next_deadline(time_point* at) const
{
    return d->next_deadline(at);
}

void fec_read_stream::reset()
{
    control(
        [](impl& s)
        {
            s.reset();
            s.rearm();
        });
}

bool fec_read_stream::have_spread_estimate() const
{
    return d->have_spread_estimate();
}

double fec_read_stream::spread_estimate_ms() const
{
    return (d->have_spread_estimate() ? d->spread_est_s : k_spread_cap_ms / 1000.0) * 1000.0;
}

double fec_read_stream::jitter_estimate_ms() const
{
    return d->have_spread_estimate() ? d->jitter_est_s * 1000.0 : 0.;
}

fec_read_stream::counters fec_read_stream::get_counters() const
{
    counters c;
    c.plain = d->c_plain.load();
    c.data = d->c_data.load();
    c.recovered = d->c_recovered.load();
    c.blocks = d->c_blocks.load();
    c.given_up_blocks = d->c_given_up_blocks.load();
    c.rs_failures = d->c_rs_failures.load();
    c.evicted_blocks = d->c_evicted.load();
    c.decode_fail = c.given_up_blocks + c.rs_failures + c.evicted_blocks;
    c.missing_data = d->c_missing_data.load();
    c.missing_parity = d->c_missing_parity.load();
    c.late = d->c_late.load();
    c.duplicates = d->c_dup.load();
    c.new_references = d->c_new_ref.load();
    c.hdr_errors = d->c_hdr_errors.load();
    c.kn_mismatch = d->c_kn_mismatch.load();
    c.drop_output = d->c_drop_output.load();
    c.drop_input = d->c_drop_input.load();
    return c;
}

}  // namespace winject::fec
