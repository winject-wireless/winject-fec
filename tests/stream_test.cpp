/* fec_write_stream timing and both streams on a running reactor. */
#include "test_util.hpp"

#include "winject_fec/write_stream.hpp"

#include <thread>

using namespace fec_test;

namespace
{

using ms = std::chrono::milliseconds;

struct writer
{
    fec_write_stream ws;
    fake_clock       clk;

    explicit writer(fec_write_stream::config cfg = {}, uint16_t seq = 100)
    {
        clk.attach(ws);
        ws.set_output_capacity(4096);
        EXPECT_TRUE(ws.configure(cfg));
        ws.set_tx_sequence(seq);
    }
    void push(const bytes& app)
    {
        ws.push(to_buf(app, ws.push_offset()));
    }
    std::vector<out_shard> pop_all()
    {
        std::vector<out_shard> v;
        for (auto s = ws.pop(); s.type != shard_type::none; s = ws.pop())
        {
            v.push_back(std::move(s));
        }
        return v;
    }
    void advance(int m)
    {
        clk.advance(m);
        ws.poll();
    }
};

fec_write_stream::config fec_cfg(int k, int n, int spread_ms = 0, int flush_ms = 1000)
{
    fec_write_stream::config c;
    c.k = k;
    c.n = n;
    c.max_datagram = k_datagram;
    c.spread_ms = spread_ms;
    c.flush_ms = flush_ms;
    return c;
}

shard_header header(const out_shard& s)
{
    shard_header h;
    EXPECT_TRUE(parse_header(s.buf.u8(), s.buf.size(), &h));
    return h;
}

}  // namespace

TEST(FecWriteStream, DefaultIsPlain)
{
    writer     w;
    const auto seq = w.ws.tx_sequence();
    EXPECT_EQ(w.ws.push_offset(), 3u); /* the fec slot is reserved without FEC too */
    const bytes app = make_app(1, 2000); /* no FEC size limit */
    const auto  in = to_buf(app, w.ws.push_offset());
    w.ws.push(in);
    const auto s = w.ws.pop();
    ASSERT_EQ(s.type, shard_type::plain);
    /* The pushed buffer with the slot cut off: the data, untouched. */
    EXPECT_EQ(s.buf.get_storage(), in.get_storage());
    EXPECT_EQ(s.buf.u8(), in.u8() + 3);
    EXPECT_EQ(bytes(s.buf.u8(), s.buf.u8() + s.buf.size()), app);
    EXPECT_EQ(w.ws.pop().type, shard_type::none);
    EXPECT_EQ(w.ws.tx_sequence(), seq); /* plain datagrams take no FEC sequence */
    EXPECT_EQ(w.ws.get_counters().plain, 1u);
}

TEST(FecWriteStream, ConfigValidation)
{
    EXPECT_TRUE(fec_write_stream::valid({}));
    EXPECT_TRUE(fec_write_stream::valid(fec_cfg(4, 6, 25, 40)));
    EXPECT_TRUE(fec_write_stream::valid(fec_cfg(4, 4, 0, 0))); /* no parity: no flush needed */
    EXPECT_FALSE(fec_write_stream::valid(fec_cfg(4, 6, 25, 0)));
    EXPECT_FALSE(fec_write_stream::valid(fec_cfg(4, 6, k_spread_cap_ms + 1, 40)));
    EXPECT_FALSE(fec_write_stream::valid(fec_cfg(4, 3)));
    fec_write_stream ws;
    EXPECT_FALSE(ws.configure(fec_cfg(4, 6, 25, 0)));
    EXPECT_EQ(fec_write_stream::max_original({}), SIZE_MAX);
    EXPECT_EQ(fec_write_stream::max_original(fec_cfg(4, 6)), k_datagram - 6);
}

TEST(FecWriteStream, DataAtOnceZeroSpreadParityAtClose)
{
    writer w(fec_cfg(3, 5), 40);
    for (uint32_t i = 0; i < 2; i++)
    {
        w.push(make_app(i, 30));
        auto s = w.pop_all();
        ASSERT_EQ(s.size(), 1u);
        EXPECT_EQ(s[0].type, shard_type::data);
        EXPECT_EQ(header(s[0]).sequence_number, 40 + i);
    }
    w.push(make_app(2, 30));
    auto s = w.pop_all();
    ASSERT_EQ(s.size(), 3u);
    EXPECT_EQ(s[0].type, shard_type::data);
    for (int j = 0; j < 2; j++)
    {
        EXPECT_EQ(s[1 + j].type, shard_type::parity);
        EXPECT_EQ(header(s[1 + j]).k_i, j);
        EXPECT_EQ(header(s[1 + j]).sequence_number, 40);
    }
    const auto c = w.ws.get_counters();
    EXPECT_EQ(c.data, 3u);
    EXPECT_EQ(c.parity, 2u);
    EXPECT_EQ(c.blocks, 1u);
}

/* k = 4, n = 6 (c = 6), spread 12 ms: slot 2 ms. Row j goes at
 * max(close + 2j, start + 8 + 2j). */
TEST(FecWriteStream, ParitySpreadBurstAndSlowFill)
{
    writer w(fec_cfg(4, 6, 12, 100));
    const auto t0 = w.clk.t;
    for (uint32_t i = 0; i < 4; i++)
    {
        w.push(make_app(i, 50));
    }
    EXPECT_EQ(w.pop_all().size(), 4u); /* burst: the data, no parity yet */
    fec_write_stream::time_point at {};
    ASSERT_TRUE(w.ws.next_deadline(&at));
    EXPECT_EQ(at, t0 + ms(8));
    w.advance(7);
    EXPECT_TRUE(w.pop_all().empty());
    w.advance(1);
    auto p = w.pop_all();
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(header(p[0]).k_i, 0);
    ASSERT_TRUE(w.ws.next_deadline(&at));
    EXPECT_EQ(at, t0 + ms(10));
    w.advance(2);
    p = w.pop_all();
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(header(p[0]).k_i, 1);
    EXPECT_FALSE(w.ws.next_deadline(&at));

    /* Slow fill, data 3 ms apart: close at +9, rows at max(9, 8) = 9 and max(11, 10) = 11. */
    const auto t1 = w.clk.t;
    for (uint32_t i = 0; i < 4; i++)
    {
        if (i > 0)
        {
            w.advance(3);
        }
        w.push(make_app(10 + i, 50));
    }
    p = w.pop_all();
    ASSERT_EQ(p.size(), 5u);
    EXPECT_EQ(p[4].type, shard_type::parity); /* row 0 due at the close */
    ASSERT_TRUE(w.ws.next_deadline(&at));
    EXPECT_EQ(at, t1 + ms(11));
}

TEST(FecWriteStream, BlocksInterleaveWithinSpread)
{
    writer w(fec_cfg(2, 4, 40, 100)); /* c = 4: slot 10 ms */
    for (uint32_t i = 0; i < 4; i++)
    {
        w.push(make_app(i, 50)); /* two blocks back to back */
    }
    EXPECT_EQ(w.pop_all().size(), 4u);
    std::vector<uint16_t> order;
    for (int t = 0; t < 40; t++)
    {
        w.advance(1);
        for (const auto& s : w.pop_all())
        {
            order.push_back(header(s).sequence_number);
        }
    }
    /* Both blocks' rows 0 at +20, rows 1 at +30: interleaved, not block by block. */
    EXPECT_EQ(order, (std::vector<uint16_t> {100, 102, 100, 102}));
}

TEST(FecWriteStream, DataGoesBeforeDueParity)
{
    writer w(fec_cfg(2, 3, 20, 100)); /* c = 3: row 0 at start + 13.3 ms */
    w.push(make_app(0, 50));
    w.push(make_app(1, 50));
    EXPECT_EQ(w.pop_all().size(), 2u);
    w.clk.advance(20); /* parity due, the timer has not run */
    w.push(make_app(2, 50));
    auto s = w.pop_all();
    ASSERT_EQ(s.size(), 2u);
    EXPECT_EQ(s[0].type, shard_type::data);
    EXPECT_EQ(s[1].type, shard_type::parity);
}

TEST(FecWriteStream, FlushClosesShortBlockFromFirstPush)
{
    writer w(fec_cfg(6, 8, 0, 20));
    const auto t0 = w.clk.t;
    w.push(make_app(0, 50));
    w.clk.advance(15);
    w.push(make_app(1, 50)); /* a later push does not move the flush */
    EXPECT_EQ(w.pop_all().size(), 2u);
    fec_write_stream::time_point at {};
    ASSERT_TRUE(w.ws.next_deadline(&at));
    EXPECT_EQ(at, t0 + ms(20));
    w.advance(4);
    EXPECT_TRUE(w.pop_all().empty());
    w.advance(1);
    auto p = w.pop_all();
    ASSERT_EQ(p.size(), 2u);
    EXPECT_EQ(header(p[0]).k_total, 2);
    EXPECT_EQ(header(p[0]).n, 2);
    EXPECT_EQ(w.ws.get_counters().flushed_blocks, 1u);
    EXPECT_FALSE(w.ws.next_deadline(&at));
    /* The next block continues the sequence. */
    w.push(make_app(2, 50));
    EXPECT_EQ(header(w.pop_all()[0]).sequence_number, 102);
}

TEST(FecWriteStream, ConfigureAppliesFromNextBlock)
{
    writer w(fec_cfg(6, 8), 300);
    for (uint32_t i = 0; i < 4; i++)
    {
        w.push(make_app(i, 60));
    }
    ASSERT_TRUE(w.ws.configure(fec_cfg(3, 6)));
    w.push(make_app(4, 60));
    EXPECT_EQ(w.pop_all().size(), 5u); /* the running block keeps k = 6 */
    w.push(make_app(5, 60));
    auto s = w.pop_all();
    ASSERT_EQ(s.size(), 3u);
    EXPECT_EQ(header(s[1]).k_total, 6);
    EXPECT_EQ(header(s[1]).n, 2);
    for (uint32_t i = 6; i < 9; i++)
    {
        w.push(make_app(i, 60));
    }
    s = w.pop_all();
    ASSERT_EQ(s.size(), 6u);
    EXPECT_EQ(header(s[3]).sequence_number, 306);
    EXPECT_EQ(header(s[3]).k_total, 3);
    EXPECT_EQ(header(s[3]).n, 3);

    /* FEC off with a block open: it still fills; plain only after it closes. */
    w.push(make_app(9, 60));
    ASSERT_TRUE(w.ws.configure({}));
    w.push(make_app(10, 60));
    w.push(make_app(11, 60));
    w.push(make_app(12, 60));
    s = w.pop_all();
    ASSERT_EQ(s.size(), 7u);
    EXPECT_EQ(s[2].type, shard_type::data);
    EXPECT_EQ(s[3].type, shard_type::parity);
    EXPECT_EQ(s[6].type, shard_type::plain);
}

TEST(FecWriteStream, PushDropsAndCounts)
{
    writer w(fec_cfg(2, 3));
    w.ws.set_tx_buffers(4, nullptr);
    w.ws.push(to_buf(bytes(6, 0), 0));
    w.ws.push(to_buf(bytes(7, 0), 0));
    w.push(make_app(0, k_datagram)); /* over max_original */
    const auto c = w.ws.get_counters();
    EXPECT_EQ(c.drop_no_headroom, 1u);
    EXPECT_EQ(c.drop_size, 2u);
    EXPECT_TRUE(w.pop_all().empty());
    EXPECT_EQ(w.ws.tx_sequence(), 100u);
}

TEST(FecWriteStream, OutputRingDropsOldest)
{
    writer w;
    w.ws.set_output_capacity(2);
    for (uint32_t i = 0; i < 3; i++)
    {
        w.push(make_app(i, 10));
    }
    EXPECT_EQ(w.ws.get_counters().drop_output, 1u);
    auto s = w.pop_all();
    ASSERT_EQ(s.size(), 2u);
    EXPECT_EQ(app_index(bytes(s[0].buf.u8(), s[0].buf.u8() + s[0].buf.size())), 1u);
}

TEST(FecStreams, EndToEndWithLossAndSpread)
{
    writer       w(fec_cfg(8, 11, 20, 30));
    receiver     rx;
    std::mt19937 rng(31);
    rx.rs.set_clock([&w] { return w.clk.t; });
    auto                        apps = make_apps(400, &rng, 300);
    std::bernoulli_distribution drop(0.08);
    size_t                      sent = 0;
    for (int t = 0; t < 1200; t++)
    {
        if (sent < apps.size() && t % 2 == 0)
        {
            w.push(apps[sent++]);
        }
        w.advance(1);
        rx.rs.poll();
        for (auto& s : w.pop_all())
        {
            if (!drop(rng))
            {
                rx.push(bytes(s.buf.u8(), s.buf.u8() + s.buf.size()));
            }
        }
        rx.take();
    }
    const auto c = rx.c();
    EXPECT_GT(c.recovered, 0u);
    EXPECT_EQ(rx.got.size() + c.missing_data, apps.size());
    EXPECT_LT(c.missing_data, 8u);
    EXPECT_TRUE(rx.rs.have_spread_estimate());
    EXPECT_NEAR(rx.rs.spread_estimate_ms(), 20.0, 2.0);
}

/* --- on a running reactor --- */

namespace
{

struct running_reactor
{
    std::shared_ptr<stream_reactor> r = std::make_shared<stream_reactor>(10);
    std::thread                     th {[this] { r->run(); }};
    ~running_reactor()
    {
        r->stop();
        th.join();
    }
};

template <typename F>
bool wait_for(F f, int timeout_ms = 2000)
{
    const auto end = std::chrono::steady_clock::now() + ms(timeout_ms);
    while (std::chrono::steady_clock::now() < end)
    {
        if (f())
        {
            return true;
        }
        std::this_thread::sleep_for(ms(1));
    }
    return f();
}

}  // namespace

TEST(FecStreamsReactor, WriteStreamTimerReleasesParityAndFlush)
{
    running_reactor  rr;
    fec_write_stream ws;
    ASSERT_TRUE(bfc::cv_reactor_registry::get_instance().add("fec_test_tx", rr.r));
    EXPECT_TRUE(ws.attach("fec_test_tx"));
    EXPECT_FALSE(ws.attach("no_such_reactor"));
    ASSERT_TRUE(ws.configure(fec_cfg(4, 6, 20, 30)));
    std::atomic<int> ready {0};
    ws.output()->set_reactor(rr.r);
    rr.r->add_read_rdy(*ws.output(), [&ready] { ready++; });
    std::vector<out_shard> got;
    const auto             take = [&]
    {
        for (auto s = ws.pop(); s.type != shard_type::none; s = ws.pop())
        {
            got.push_back(std::move(s));
        }
    };
    const auto t0 = std::chrono::steady_clock::now();
    for (uint32_t i = 0; i < 4; i++)
    {
        ws.push(to_buf(make_app(i, 40), ws.push_offset())); /* posted onto the reactor */
    }
    ASSERT_TRUE(wait_for([&] { take(); return got.size() == 6; }));
    const auto dt = std::chrono::steady_clock::now() - t0;
    EXPECT_GE(dt, ms(15)); /* the last row at start + 5/6 of 20 ms */
    EXPECT_EQ(got[4].type, shard_type::parity);
    EXPECT_GT(ready.load(), 0);
    /* A short block closes on its flush timer with nobody polling. */
    ws.push(to_buf(make_app(9, 40), ws.push_offset()));
    ASSERT_TRUE(wait_for([&] { take(); return got.size() == 9; }));
    EXPECT_EQ(ws.get_counters().flushed_blocks, 1u);
    rr.r->remove_read_rdy(*ws.output());
    (void)bfc::cv_reactor_registry::get_instance().rem("fec_test_tx");
}

TEST(FecStreamsReactor, ReadStreamGivesUpOnItsTimer)
{
    running_reactor rr;
    fec_read_stream rs;
    rs.attach(rr.r);
    sender tx(4, 6);
    auto   all = tx.encode_all({make_app(0, 20), make_app(1, 20), make_app(2, 20),
                                make_app(3, 20)});
    for (size_t i : {0, 1, 3})
    {
        rs.push(bfc::shared_sized_buffer::copy_from(all[i].data.data(), all[i].data.size()),
                lc_type::fec);
    }
    /* No parity and no estimate: given up about 250 ms after the close, with nobody polling. */
    ASSERT_TRUE(wait_for([&] { return rs.get_counters().missing_data == 1; }, 1000));
    int n = 0;
    while (!rs.pop().empty())
    {
        n++;
    }
    EXPECT_EQ(n, 3);
}
