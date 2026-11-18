#include "winject_fec/write_stream.hpp"

#include <algorithm>
#include <atomic>
#include <deque>
#include <random>
#include <utility>

#include "rs_common.hpp"
#include "winject_fec/spread.hpp"

namespace winject::fec
{

namespace
{
constexpr size_t k_default_output_capacity = 256;
constexpr size_t k_default_input_capacity = 256;
}

struct fec_write_stream::impl : std::enable_shared_from_this<impl>
{
    struct pending
    {
        time_point at;
        out_shard  s;
    };

    /* Before link: the reactor points at its ring until link lets go. */
    detail::stream_input<bfc::shared_sized_buffer, impl> in;
    detail::reactor_link                link;
    clock_fn                            clock;
    block_encoder                       enc;
    config                              cfg;   /* for the next block */
    config                              blk;   /* the open block's */
    uint16_t                            tx_seq = 0;
    std::deque<pending>                 parity_q; /* by release time */
    std::shared_ptr<write_output_queue> out = std::make_shared<write_output_queue>();
    size_t                              out_capacity = 0;

    stream_counter c_plain, c_data, c_parity, c_blocks, c_flushed, c_drop_no_headroom,
        c_drop_size, c_drop_parity_alloc, c_drop_output, c_kernel_us, c_drop_input;

    impl()
    {
        std::random_device rd;
        /* Random start id: a restarted sender must not replay ids the receiver still holds. */
        tx_seq = static_cast<uint16_t>(rd());
        set_output_capacity(k_default_output_capacity);
        set_input_capacity(k_default_input_capacity);
    }

    void set_input_capacity(size_t n)
    {
        in.set_capacity(n);
    }

    /* Any thread: queues the datagram for the reactor (oldest dropped when full). */
    void enqueue(bfc::shared_sized_buffer&& datagram)
    {
        if (!in.push(std::move(datagram)))
        {
            c_drop_input.add_shared();
        }
    }

    /* Stream thread: what other threads gave, in order, before anything after it. */
    void drain_input()
    {
        in.drain(*this, [this](bfc::shared_sized_buffer&& b) { push(std::move(b)); });
    }

    time_point now() const
    {
        return clock ? clock() : std::chrono::steady_clock::now();
    }

    void set_output_capacity(size_t n)
    {
        out->set_capacity(n);
        out_capacity = n;
    }

    void emit(out_shard&& s)
    {
        if (out->push(std::move(s)) == out_capacity && out_capacity > 0)
        {
            c_drop_output++;
        }
    }

    /* Mirrors the encoder's own counts (it is only touched on this thread). */
    void sync_encoder_counters()
    {
        c_blocks += enc.blocks() - c_blocks.load();
        c_drop_parity_alloc += enc.drop_parity_alloc() - c_drop_parity_alloc.load();
        c_kernel_us += enc.encode_kernel_us() - c_kernel_us.load();
    }

    /* Data shards go out now; parity waits for its place in the spread. */
    void drain_encoder()
    {
        while (enc.get_state().type != shard_type::none)
        {
            const block_encoder::state st = enc.get_state();
            bfc::shared_sized_buffer   buf = enc.pop();
            if (st.type != shard_type::parity)
            {
                c_data++;
                emit({st.type, std::move(buf)});
                continue;
            }
            const parity_info& pi = st.parity;
            const size_t       cnt = static_cast<size_t>(pi.block_k + pi.count);
            const size_t       j = static_cast<size_t>(pi.index);
            const time_point   at =
                std::max(pi.block_close + spread_offset(j, cnt, blk.spread_ms),
                         pi.block_start + spread_offset(static_cast<size_t>(pi.block_k) + j, cnt,
                                                        blk.spread_ms));
            auto pos = parity_q.end();
            while (pos != parity_q.begin() && std::prev(pos)->at > at)
            {
                --pos;
            }
            parity_q.insert(pos, pending {at, {shard_type::parity, std::move(buf)}});
        }
        sync_encoder_counters();
    }

    void release_due(time_point t)
    {
        while (!parity_q.empty() && parity_q.front().at <= t)
        {
            c_parity++;
            emit(std::move(parity_q.front().s));
            parity_q.pop_front();
        }
    }

    void service(time_point t)
    {
        release_due(t);
        if (enc.is_open() && blk.flush_ms > 0 &&
            t >= enc.block_start() + std::chrono::milliseconds(blk.flush_ms))
        {
            c_flushed++;
            enc.close(t);
            drain_encoder();
            release_due(t);
        }
    }

    bool next_deadline(time_point* at) const
    {
        bool       any = false;
        time_point best {};
        if (!parity_q.empty())
        {
            best = parity_q.front().at;
            any = true;
        }
        if (enc.is_open() && blk.flush_ms > 0)
        {
            const time_point f = enc.block_start() + std::chrono::milliseconds(blk.flush_ms);
            if (!any || f < best)
            {
                best = f;
                any = true;
            }
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

    void push(bfc::shared_sized_buffer datagram)
    {
        const time_point t = now();
        const config&    eff = enc.is_open() ? blk : cfg;
        if (datagram.size() < enc.push_offset())
        {
            c_drop_no_headroom++;
            return;
        }
        const size_t len = datagram.size() - enc.push_offset();
        if (0 == len || len > max_original(eff))
        {
            c_drop_size++;
            return;
        }
        if (!enc.is_open())
        {
            if (0 == cfg.k)
            {
                /* Plain: the same buffer, the unused fec slot cut off the front. */
                c_plain++;
                emit({shard_type::plain,
                      datagram.subview(k_data_header_len, datagram.size() - k_data_header_len)});
                service(t);
                rearm();
                return;
            }
            blk = cfg;
            (void)enc.start(tx_seq, blk.k, blk.n, blk.max_datagram, t);
        }
        enc.push(std::move(datagram), t);
        tx_seq++;
        /* The data shard first, then whatever else is due. */
        drain_encoder();
        service(t);
        rearm();
    }
};

fec_write_stream::fec_write_stream() : d(std::make_shared<impl>()) {}

/* The reactor walks its contexts (the input ring among them) unlocked, so the ring leaves it on
 * the reactor thread: from another thread the impl is handed to the reactor and torn down there.
 * The timer holds the impl weakly. */
fec_write_stream::~fec_write_stream()
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
void fec_write_stream::control(std::function<void(impl&)> fn)
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

bool fec_write_stream::attach(std::string_view reactor_name)
{
    auto r = find_stream_reactor(reactor_name);
    if (!r)
    {
        return false;
    }
    attach(std::move(r));
    return true;
}

void fec_write_stream::attach(std::shared_ptr<stream_reactor> reactor)
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

void fec_write_stream::set_clock(clock_fn fn)
{
    d->clock = std::move(fn);
}

void fec_write_stream::set_tx_buffers(size_t headroom, shard_alloc alloc)
{
    control([headroom, alloc = std::move(alloc)](impl& s) mutable
            { s.enc.set_tx_buffers(headroom, std::move(alloc)); });
}

size_t fec_write_stream::push_offset() const
{
    return d->enc.push_offset();
}

void fec_write_stream::set_output_capacity(size_t shards)
{
    control([shards](impl& s) { s.set_output_capacity(shards); });
}

void fec_write_stream::set_input_capacity(size_t datagrams)
{
    d->set_input_capacity(datagrams);
}

bool fec_write_stream::valid(const config& cfg)
{
    if (0 == cfg.k)
    {
        return true;
    }
    return block_encoder::valid(cfg.k, cfg.n, cfg.max_datagram) && cfg.spread_ms >= 0 &&
           cfg.spread_ms <= k_spread_cap_ms && cfg.flush_ms >= 0 &&
           (cfg.n == cfg.k || cfg.flush_ms > 0);
}

size_t fec_write_stream::max_original(const config& cfg)
{
    return 0 == cfg.k ? SIZE_MAX : block_encoder::max_original(cfg.max_datagram);
}

bool fec_write_stream::configure(const config& cfg)
{
    if (!valid(cfg))
    {
        return false;
    }
    if (cfg.k > 0 && cfg.n > cfg.k)
    {
        /* Build the full block's encode tables now, not on the stream's first block close. */
        (void)detail::kn_encode_tables_for(cfg.k, cfg.n);
    }
    control([cfg](impl& s) { s.cfg = cfg; });
    return true;
}

void fec_write_stream::push(bfc::shared_sized_buffer datagram)
{
    if (d->link.on_thread())
    {
        d->drain_input();
        d->push(std::move(datagram));
        return;
    }
    d->enqueue(std::move(datagram));
}

out_shard fec_write_stream::pop()
{
    auto s = d->out->pop();
    return s ? std::move(*s) : out_shard {};
}

std::shared_ptr<write_output_queue> fec_write_stream::output() const
{
    return d->out;
}

void fec_write_stream::poll()
{
    control(
        [](impl& s)
        {
            s.service(s.now());
            s.rearm();
        });
}

bool fec_write_stream::next_deadline(time_point* at) const
{
    return d->next_deadline(at);
}

void fec_write_stream::set_tx_sequence(uint16_t seq)
{
    d->tx_seq = seq;
}

uint16_t fec_write_stream::tx_sequence() const
{
    return d->tx_seq;
}

const char* fec_write_stream::impl_name() const
{
    return d->enc.impl_name();
}

fec_write_stream::counters fec_write_stream::get_counters() const
{
    counters c;
    c.plain = d->c_plain.load();
    c.data = d->c_data.load();
    c.parity = d->c_parity.load();
    c.blocks = d->c_blocks.load();
    c.flushed_blocks = d->c_flushed.load();
    c.drop_no_headroom = d->c_drop_no_headroom.load();
    c.drop_size = d->c_drop_size.load();
    c.drop_parity_alloc = d->c_drop_parity_alloc.load();
    c.drop_output = d->c_drop_output.load();
    c.encode_kernel_us = d->c_kernel_us.load();
    c.drop_input = d->c_drop_input.load();
    return c;
}

}  // namespace winject::fec
