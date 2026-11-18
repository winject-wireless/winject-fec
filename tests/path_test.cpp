/* The full path on real reactor threads: a paced source -> fec_write_stream -> a channel
 * simulator (LC header, airtime, delay and jitter, random and Gilbert-Elliott loss) ->
 * fec_read_stream -> a sink that checks every datagram's content, order and latency. */
#include "test_util.hpp"

#include "winject_fec/write_stream.hpp"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

using namespace fec_test;

namespace
{

using clock_t_ = std::chrono::steady_clock;
using ms = std::chrono::milliseconds;
using us = std::chrono::microseconds;

constexpr size_t k_lc_header = 4; /* u8 bus | u8 lc_type | u16 sn */

struct running_reactor
{
    std::shared_ptr<stream_reactor> r = std::make_shared<stream_reactor>(10);
    std::thread                     th {[this] { r->run(); }};
    ~running_reactor()
    {
        r->stop();
        th.join();
    }
    /* Runs fn on the reactor thread and waits for it. */
    void run_on(std::function<void()> fn)
    {
        std::promise<void> done;
        r->wake_up(
            [&]
            {
                fn();
                done.set_value();
            });
        done.get_future().wait();
    }
};

/* As vstreamer's link emulator: per datagram the state moves good -> bad with p_gb and back with
 * p_bg (percent), then the datagram is lost with loss_bad / loss_good percent. */
struct gilbert_elliott
{
    double p_gb = 0.;
    double p_bg = 100.;
    double loss_good = 0.;
    double loss_bad = 100.;
};

struct channel_model
{
    double          loss_pct = 0.;
    gilbert_elliott ge;
    /* Fades in time: they start fade_per_s a second on average (Poisson) and last fade_ms; a
     * shard on the air during one is lost. */
    double          fade_per_s = 0.;
    double          fade_ms = 0.;
    double          rate_kbps = 12000.;
    int             delay_ms = 2;
    int             jitter_ms = 1;
    uint32_t        seed = 1;
};

/* Runs on its own reactor: drains the write stream's output, puts each shard on the air and
 * delivers what survives to the read stream (a push from another thread: its input ring). */
class channel : public std::enable_shared_from_this<channel>
{
public:
    struct counters
    {
        uint64_t shards = 0;
        uint64_t parity = 0;
        uint64_t lost = 0;
        uint64_t lost_parity = 0;
        uint64_t bursts = 0;
        uint64_t fades = 0;
        uint64_t delivered = 0;
    };

    channel(std::shared_ptr<stream_reactor> r, fec_write_stream* ws, fec_read_stream* rs,
            channel_model m)
        : reactor(std::move(r)), tx(ws), rx(rs), model(m), rng(m.seed), fade_rng(m.seed + 1)
    {
    }

    /* On the channel reactor. */
    void start()
    {
        tx->output()->set_reactor(reactor);
        reactor->add_read_rdy(*tx->output(),
                              [w = weak_from_this()]
                              {
                                  if (auto s = w.lock())
                                  {
                                      s->on_shards();
                                  }
                              });
    }
    /* On the channel reactor. */
    void stop()
    {
        stopped = true;
        reactor->remove_read_rdy(*tx->output());
        tx->output()->set_reactor(nullptr);
        air.clear();
    }

    counters get() const
    {
        std::lock_guard<std::mutex> lg(mtx);
        return c;
    }

private:
    struct in_flight
    {
        clock_t_::time_point     at;
        bfc::shared_sized_buffer frame;
    };

    bool chance(double pct)
    {
        return std::uniform_real_distribution<double>(0., 100.)(rng) < pct;
    }

    /* Whether [start, end) on the air overlaps a fade. */
    bool faded(clock_t_::time_point start, clock_t_::time_point end)
    {
        if (model.fade_per_s <= 0.)
        {
            return false;
        }
        const auto fade_len = us(static_cast<int64_t>(model.fade_ms * 1000.));
        if (fade_end == clock_t_::time_point {})
        {
            fade_end = start;
        }
        while (start >= fade_end)
        {
            const double gap_s =
                std::exponential_distribution<double>(model.fade_per_s)(fade_rng);
            fade_start = fade_end + us(static_cast<int64_t>(gap_s * 1e6));
            fade_end = fade_start + fade_len;
            fade_counted = false;
        }
        const bool hit = end > fade_start && start < fade_end;
        if (hit && !fade_counted)
        {
            fade_counted = true;
            std::lock_guard<std::mutex> lg(mtx);
            c.fades++;
        }
        return hit;
    }

    bool lost()
    {
        if (chance(model.loss_pct))
        {
            return true;
        }
        if (model.ge.p_gb <= 0.)
        {
            return false;
        }
        if (bad ? chance(model.ge.p_bg) : chance(model.ge.p_gb))
        {
            bad = !bad;
            if (bad)
            {
                std::lock_guard<std::mutex> lg(mtx);
                c.bursts++;
            }
        }
        return chance(bad ? model.ge.loss_bad : model.ge.loss_good);
    }

    void on_shards()
    {
        for (auto s = tx->pop(); s.type != shard_type::none && !stopped; s = tx->pop())
        {
            send(std::move(s));
        }
    }

    void send(out_shard s)
    {
        /* The LC header goes into the headroom the write stream left in front. */
        uint8_t* p = s.buf.u8();
        p[0] = 0x21;
        p[1] = static_cast<uint8_t>(s.type == shard_type::plain ? lc_type::plain : lc_type::fec);
        p[2] = static_cast<uint8_t>(lc_sn);
        p[3] = static_cast<uint8_t>(lc_sn >> 8);
        lc_sn++;

        const auto now = clock_t_::now();
        const auto airtime =
            us(static_cast<int64_t>(s.buf.size() * 8 * 1000. / model.rate_kbps));
        const auto on_air = std::max(air_free, now);
        air_free = on_air + airtime;
        auto at = air_free + ms(model.delay_ms) +
                  us(std::uniform_int_distribution<int>(0, model.jitter_ms * 1000)(rng));
        at = std::max(at, last_at); /* the air does not reorder */
        last_at = at;

        const bool parity = s.type == shard_type::parity;
        const bool drop = faded(on_air, air_free) | lost();
        {
            std::lock_guard<std::mutex> lg(mtx);
            c.shards++;
            c.parity += parity ? 1 : 0;
            c.lost += drop ? 1 : 0;
            c.lost_parity += drop && parity ? 1 : 0;
        }
        if (drop)
        {
            return;
        }
        /* What the receiver gets is its own copy of the frame. */
        air.push_back({at, bfc::shared_sized_buffer::copy_from(s.buf.u8(), s.buf.size())});
        const int64_t wait =
            std::max<int64_t>(0, std::chrono::duration_cast<us>(at - now).count());
        reactor->get_timer().wait_us(wait,
                                     [w = weak_from_this()]
                                     {
                                         if (auto s = w.lock())
                                         {
                                             s->deliver();
                                         }
                                     });
    }

    void deliver()
    {
        const auto now = clock_t_::now();
        while (!stopped && !air.empty() && air.front().at <= now)
        {
            bfc::shared_sized_buffer f = std::move(air.front().frame);
            air.pop_front();
            const auto type = static_cast<lc_type>(f.u8()[1]);
            rx->push(f.subview(k_lc_header, f.size() - k_lc_header), type);
            std::lock_guard<std::mutex> lg(mtx);
            c.delivered++;
        }
    }

    std::shared_ptr<stream_reactor> reactor;
    fec_write_stream*               tx;
    fec_read_stream*                rx;
    channel_model                   model;
    std::mt19937                    rng;
    std::mt19937                    fade_rng; /* its own: the same fades whatever else draws */
    bool                            bad = false;
    bool                            stopped = false;
    uint16_t                        lc_sn = 0;
    clock_t_::time_point            air_free {};
    clock_t_::time_point            last_at {};
    clock_t_::time_point            fade_start {};
    clock_t_::time_point            fade_end {};
    bool                            fade_counted = false;
    std::deque<in_flight>           air;
    mutable std::mutex              mtx;
    counters                        c;
};

/* One phase of the source: frames at fps, each a burst of datagrams, under one stream config. */
struct phase
{
    fec_write_stream::config cfg;
    int                      frames = 60;
};

struct path_result
{
    uint64_t                 sent = 0;
    uint64_t                 delivered = 0;
    uint64_t                 corrupt = 0;
    uint64_t                 out_of_order = 0;
    uint64_t                 duplicates = 0;
    double                   lat_p50_ms = 0.;
    double                   lat_p99_ms = 0.;
    double                   lat_max_ms = 0.;
    channel::counters        ch;
    fec_write_stream::counters tx;
    fec_read_stream::counters  rx;

    double wire_loss_pct() const
    {
        return ch.shards ? 100. * static_cast<double>(ch.lost) / static_cast<double>(ch.shards)
                         : 0.;
    }
    double app_loss_pct() const
    {
        return sent ? 100. * static_cast<double>(sent - delivered) / static_cast<double>(sent)
                    : 0.;
    }
};

fec_write_stream::config fec(int k, int n, int spread_ms, int flush_ms = 10)
{
    fec_write_stream::config c;
    c.k = k;
    c.n = n;
    c.max_datagram = k_datagram;
    c.spread_ms = spread_ms;
    c.flush_ms = flush_ms;
    return c;
}

path_result run_path(const char* name, const std::vector<phase>& phases, channel_model model,
                     int per_frame = 16, int fps = 30)
{
    running_reactor tx_r, ch_r, rx_r;

    int total_frames = 0;
    for (const auto& p : phases)
    {
        total_frames += p.frames;
    }
    const size_t total = static_cast<size_t>(total_frames * per_frame);
    std::mt19937 len_rng(model.seed * 7 + 3);
    std::vector<size_t> lens(total);
    for (auto& l : lens)
    {
        l = std::uniform_int_distribution<size_t>(200, 1400)(len_rng);
    }
    std::vector<clock_t_::time_point> sent_at(total);

    auto ws = std::make_unique<fec_write_stream>();
    auto rs = std::make_unique<fec_read_stream>();
    ws->set_tx_buffers(k_lc_header, nullptr);
    ws->set_output_capacity(1024);
    ws->set_input_capacity(1024);
    rs->set_output_capacity(1024);
    rs->set_input_capacity(1024);
    ws->attach(tx_r.r);
    rs->attach(rx_r.r);

    auto ch = std::make_shared<channel>(ch_r.r, ws.get(), rs.get(), model);
    ch_r.run_on([&] { ch->start(); });

    /* The sink, on the read stream's reactor. */
    std::mutex            sink_mtx;
    std::vector<uint32_t> order;
    std::vector<double>   lat_ms;
    uint64_t              corrupt = 0;
    const auto            on_rx = [&]
    {
        for (auto b = rs->pop(); !b.empty(); b = rs->pop())
        {
            const auto now = clock_t_::now();
            const bytes got(b.u8(), b.u8() + b.size());
            std::lock_guard<std::mutex> lg(sink_mtx);
            const uint32_t i = got.size() >= 4 ? app_index(got) : UINT32_MAX;
            if (i >= total || got != make_app(i, lens[i]))
            {
                corrupt++;
                continue;
            }
            order.push_back(i);
            lat_ms.push_back(std::chrono::duration<double, std::milli>(now - sent_at[i]).count());
        }
    };
    rx_r.run_on(
        [&]
        {
            rs->output()->set_reactor(rx_r.r);
            rx_r.r->add_read_rdy(*rs->output(), on_rx);
        });

    /* The source: its own thread, frames paced at fps, a burst of datagrams per frame. */
    std::thread source(
        [&]
        {
            const auto frame = us(1'000'000 / fps);
            auto       next = clock_t_::now();
            size_t     i = 0;
            for (const auto& p : phases)
            {
                EXPECT_TRUE(ws->configure(p.cfg));
                for (int f = 0; f < p.frames; f++)
                {
                    std::this_thread::sleep_until(next);
                    next += frame;
                    for (int j = 0; j < per_frame; j++, i++)
                    {
                        sent_at[i] = clock_t_::now();
                        ws->push(to_buf(make_app(static_cast<uint32_t>(i), lens[i]),
                                        ws->push_offset()));
                    }
                }
            }
        });
    source.join();
    /* Let flushes, spread parity, the channel and the give-up deadlines run out. */
    std::this_thread::sleep_for(ms(model.delay_ms + model.jitter_ms + 600));

    ch_r.run_on([&] { ch->stop(); });
    rx_r.run_on(
        [&]
        {
            rx_r.r->remove_read_rdy(*rs->output());
            rs->output()->set_reactor(nullptr);
        });

    path_result res;
    res.sent = total;
    res.ch = ch->get();
    res.tx = ws->get_counters();
    res.rx = rs->get_counters();
    {
        std::lock_guard<std::mutex> lg(sink_mtx);
        res.delivered = order.size();
        res.corrupt = corrupt;
        std::vector<bool> seen(total, false);
        for (size_t j = 0; j < order.size(); j++)
        {
            if (seen[order[j]])
            {
                res.duplicates++;
            }
            seen[order[j]] = true;
            if (j > 0 && order[j] <= order[j - 1])
            {
                res.out_of_order++;
            }
        }
        if (!lat_ms.empty())
        {
            std::sort(lat_ms.begin(), lat_ms.end());
            res.lat_p50_ms = lat_ms[lat_ms.size() / 2];
            res.lat_p99_ms = lat_ms[lat_ms.size() * 99 / 100];
            res.lat_max_ms = lat_ms.back();
        }
    }
    /* Streams leave their reactors while those still run. */
    ch.reset();
    rs.reset();
    ws.reset();

    std::printf("[path] %-22s app %5llu sent %5llu got (%.2f%% lost) | wire %5llu shards %.2f%% "
                "lost, %llu bursts %llu fades | rx data %llu rebuilt %llu missing %llu given-up blocks "
                "%llu | lat p50 %.1f p99 %.1f max %.1f ms\n",
                name, (unsigned long long)res.sent, (unsigned long long)res.delivered,
                res.app_loss_pct(), (unsigned long long)res.ch.shards, res.wire_loss_pct(),
                (unsigned long long)res.ch.bursts, (unsigned long long)res.ch.fades,
                (unsigned long long)res.rx.data,
                (unsigned long long)res.rx.recovered, (unsigned long long)res.rx.missing_data,
                (unsigned long long)res.rx.given_up_blocks, res.lat_p50_ms, res.lat_p99_ms,
                res.lat_max_ms);
    return res;
}

/* What holds on every run: intact, in order, nothing dropped inside the streams. */
void expect_sound(const path_result& r)
{
    EXPECT_EQ(r.corrupt, 0u);
    EXPECT_EQ(r.out_of_order, 0u);
    EXPECT_EQ(r.duplicates, 0u);
    EXPECT_EQ(r.tx.drop_input, 0u);
    EXPECT_EQ(r.tx.drop_output, 0u);
    EXPECT_EQ(r.tx.drop_size, 0u);
    EXPECT_EQ(r.tx.drop_no_headroom, 0u);
    EXPECT_EQ(r.rx.drop_input, 0u);
    EXPECT_EQ(r.rx.drop_output, 0u);
    EXPECT_EQ(r.rx.hdr_errors, 0u);
    EXPECT_EQ(r.rx.kn_mismatch, 0u);
    /* Every datagram the receiver released or gave up is accounted for once. */
    EXPECT_EQ(r.rx.plain + r.rx.data + r.rx.recovered, r.delivered);
}

}  // namespace

TEST(FecPath, CleanChannelDeliversEverything)
{
    const auto r = run_path("clean 8/12 spread 25", {{fec(8, 12, 25), 60}}, {});
    expect_sound(r);
    EXPECT_EQ(r.delivered, r.sent);
    EXPECT_EQ(r.rx.recovered, 0u);
    EXPECT_EQ(r.rx.missing_data, 0u);
    /* Data never waits for parity: about the channel delay plus airtime. */
    EXPECT_LT(r.lat_p99_ms, 20.);
}

TEST(FecPath, RandomLossRecovered)
{
    channel_model m;
    m.loss_pct = 10.;
    m.seed = 11;
    const auto r = run_path("10% random 8/12", {{fec(8, 12, 25), 90}}, m);
    expect_sound(r);
    EXPECT_GT(r.wire_loss_pct(), 7.);
    EXPECT_GT(r.rx.recovered, 0u);
    EXPECT_LT(r.app_loss_pct(), 1.);
    /* A hole waits for its block's parity, never past the give-up cap. */
    EXPECT_LT(r.lat_max_ms, 2. + 1. + 250. + 50.);
}

/* Bursts counted in datagrams (vstreamer's channel_sim model): data goes out back to back
 * whatever the spread, so a burst longer than a block's parity loses data either way. */
TEST(FecPath, DatagramBurstsRecovered)
{
    channel_model m;
    m.ge = {3., 30., 0., 100.}; /* ~9% bad, bursts of ~3.3 datagrams */
    m.seed = 23;
    const auto r = run_path("GE bursts 8/12", {{fec(8, 12, 25), 120}}, m);
    expect_sound(r);
    EXPECT_GT(r.ch.bursts, 20u);
    EXPECT_GT(r.rx.recovered, 0u);
    EXPECT_LT(r.app_loss_pct(), r.wire_loss_pct());
}

/* Fades in time: spread parity outlives a fade that takes the data before it; parity right
 * behind its data goes down with it. */
TEST(FecPath, FadesSpreadBeatsNoSpread)
{
    channel_model m;
    m.fade_per_s = 6.;
    m.fade_ms = 3.;
    m.seed = 29;
    const auto spread = run_path("fades spread 25", {{fec(8, 12, 25), 150}}, m);
    const auto lump = run_path("fades spread 0", {{fec(8, 12, 0), 150}}, m);
    expect_sound(spread);
    expect_sound(lump);
    EXPECT_GT(spread.ch.fades, 10u);
    EXPECT_LT(spread.app_loss_pct(), lump.app_loss_pct());
}

TEST(FecPath, ReconfigureMidStream)
{
    channel_model m;
    m.loss_pct = 5.;
    m.seed = 37;
    fec_write_stream::config plain;
    const auto r = run_path("plain -> 4/6 -> 8/12 -> plain",
                            {{plain, 20}, {fec(4, 6, 0), 30}, {fec(8, 12, 25), 30}, {plain, 20}},
                            m);
    expect_sound(r);
    EXPECT_GT(r.rx.plain, 0u);
    EXPECT_GT(r.rx.recovered, 0u);
    /* Only the plain phases lose what the air loses. */
    EXPECT_LT(r.app_loss_pct(), 5.);
}

TEST(FecPath, HeavyLossStaysSoundAndBounded)
{
    channel_model m;
    m.loss_pct = 20.;
    m.ge = {2., 20., 0., 100.};
    m.jitter_ms = 4;
    m.seed = 41;
    const auto r = run_path("20% + bursts 8/12", {{fec(8, 12, 25), 90}}, m);
    expect_sound(r);
    EXPECT_GT(r.rx.missing_data, 0u);
    EXPECT_LT(r.lat_max_ms, 2. + 4. + 250. + 50.);
}
