/* Wire, block_encoder, block_decoder and fec_read_stream. */
#include "test_util.hpp"

#include "winject_fec/block_decoder.hpp"

using namespace fec_test;

namespace
{

using clk = std::chrono::steady_clock;

block_encoder::state st(const block_encoder& e)
{
    return e.get_state();
}

}  // namespace

TEST(FecWire, HeaderRoundTrip)
{
    uint8_t d[8] {};
    write_data_header(d, 0xBEEF);
    EXPECT_EQ(d[0], 0x80);
    shard_header h;
    ASSERT_TRUE(parse_header(d, sizeof(d), &h));
    EXPECT_TRUE(h.is_data);
    EXPECT_EQ(h.sequence_number, 0xBEEF);
    EXPECT_EQ(h.header_len, 3u);

    uint8_t p[8] {};
    ASSERT_TRUE(write_parity_header(p, 0x1234, 10, 21, 9));
    /* is_data 0 | n 01010 | k_total 10101 | k_i 01001 */
    EXPECT_EQ(p[0], 0x2A);
    EXPECT_EQ(p[1], 0xA9);
    ASSERT_TRUE(parse_header(p, sizeof(p), &h));
    EXPECT_FALSE(h.is_data);
    EXPECT_EQ(h.sequence_number, 0x1234);
    EXPECT_EQ(h.n, 10);
    EXPECT_EQ(h.k_total, 21);
    EXPECT_EQ(h.k_i, 9);
    EXPECT_EQ(h.header_len, 4u);
}

TEST(FecWire, HeaderRejectsInvalid)
{
    shard_header  h;
    const uint8_t spare[] = {0x81, 0, 0, 1};
    EXPECT_FALSE(parse_header(spare, sizeof(spare), &h));
    const uint8_t data_no_body[] = {0x80, 0, 0};
    EXPECT_FALSE(parse_header(data_no_body, sizeof(data_no_body), &h));
    uint8_t p[8] {};
    EXPECT_FALSE(write_parity_header(p, 0, 0, 4, 0));
    EXPECT_FALSE(write_parity_header(p, 0, 2, 0, 0));
    EXPECT_FALSE(write_parity_header(p, 0, 2, 4, 2));
    EXPECT_FALSE(write_parity_header(p, 0, 32, 4, 0));
    /* k_i >= n on the wire. */
    const uint8_t bad_row[] = {0x04, 0x22, 0, 0, 1, 2, 3};
    EXPECT_FALSE(parse_header(bad_row, sizeof(bad_row), &h));
}

/* --- block_encoder --- */

TEST(FecBlockEncoder, DataLeavesAtOnceParityAtClose)
{
    sender tx(6, 8, 500);
    for (int i = 0; i < 5; i++)
    {
        auto s = tx.push(make_app(static_cast<uint32_t>(i), 100));
        ASSERT_EQ(s.size(), 1u);
        shard_header h;
        ASSERT_TRUE(parse_header(s[0].data.data(), s[0].data.size(), &h));
        EXPECT_TRUE(h.is_data);
        EXPECT_EQ(h.sequence_number, 500 + i);
        EXPECT_EQ(s[0].data.size(), 3u + 100u);
    }
    EXPECT_TRUE(tx.enc.is_open());
    auto s = tx.push(make_app(5, 40));
    EXPECT_FALSE(tx.enc.is_open());
    ASSERT_EQ(s.size(), 3u);
    EXPECT_FALSE(s[0].parity);
    for (int j = 0; j < 2; j++)
    {
        shard_header h;
        ASSERT_TRUE(parse_header(s[1 + j].data.data(), s[1 + j].data.size(), &h));
        EXPECT_FALSE(h.is_data);
        EXPECT_EQ(h.sequence_number, 500);
        EXPECT_EQ(h.k_total, 6);
        EXPECT_EQ(h.n, 2);
        EXPECT_EQ(h.k_i, j);
        /* header + coded length + longest payload */
        EXPECT_EQ(s[1 + j].data.size(), 4u + 2u + 100u);
    }
    /* Next block starts right after. */
    auto         next = tx.push(make_app(6, 10));
    shard_header h;
    ASSERT_TRUE(parse_header(next[0].data.data(), next[0].data.size(), &h));
    EXPECT_EQ(h.sequence_number, 506);
}

TEST(FecBlockEncoder, SizeLimits)
{
    sender tx(2, 3);
    EXPECT_EQ(block_encoder::max_original(k_datagram), k_datagram - 6);
    EXPECT_EQ(max_data_payload(k_datagram), k_datagram - 3);
    EXPECT_TRUE(tx.push(make_app(0, k_datagram - 5)).empty());
    EXPECT_EQ(tx.enc.drop_size(), 1u);
    EXPECT_TRUE(tx.push(bytes {}).empty());
    EXPECT_EQ(tx.enc.drop_size(), 2u);
    auto a = tx.push(make_app(1, k_datagram - 6));
    auto b = tx.push(make_app(2, 1));
    ASSERT_EQ(b.size(), 2u);
    EXPECT_EQ(a[0].data.size(), k_datagram - 3);
    EXPECT_EQ(b[1].data.size(), k_datagram);
}

TEST(FecBlockEncoder, ValidRejectsInvalid)
{
    EXPECT_FALSE(block_encoder::valid(0, 2, k_datagram));
    EXPECT_FALSE(block_encoder::valid(32, 32, k_datagram));
    EXPECT_FALSE(block_encoder::valid(4, 3, k_datagram));
    EXPECT_FALSE(block_encoder::valid(4, 6, 10));
    EXPECT_FALSE(block_encoder::valid(1, 33, k_datagram));
    EXPECT_TRUE(block_encoder::valid(1, 1, k_datagram));
    EXPECT_TRUE(block_encoder::valid(31, 31, k_datagram));
    EXPECT_TRUE(block_encoder::valid(31, 62, k_datagram));
    block_encoder enc;
    EXPECT_FALSE(enc.start(0, 4, 3, k_datagram, clk::now()));
    EXPECT_FALSE(enc.is_open());
}

TEST(FecBlockEncoder, StateWalksDataThenParity)
{
    block_encoder enc;
    const auto    t0 = clk::time_point {} + std::chrono::hours(1);
    ASSERT_TRUE(enc.start(40, 3, 5, k_datagram, t0));
    EXPECT_EQ(enc.block_start(), t0);
    for (uint32_t i = 0; i < 2; i++)
    {
        enc.push(to_buf(make_app(i, 30), enc.push_offset()), t0);
        ASSERT_EQ(st(enc).type, shard_type::data);
        (void)enc.pop();
        EXPECT_EQ(st(enc).type, shard_type::none);
    }
    EXPECT_EQ(enc.rows(), 2);
    /* The closing push queues its data shard, then the parity. */
    const auto t1 = t0 + std::chrono::milliseconds(7);
    enc.push(to_buf(make_app(2, 30), enc.push_offset()), t1);
    ASSERT_EQ(enc.queued(), 3u);
    EXPECT_EQ(st(enc).type, shard_type::data);
    (void)enc.pop();
    for (int j = 0; j < 2; j++)
    {
        const auto s = st(enc);
        ASSERT_EQ(s.type, shard_type::parity);
        EXPECT_EQ(s.parity.index, j);
        EXPECT_EQ(s.parity.count, 2);
        EXPECT_EQ(s.parity.block_k, 3);
        EXPECT_EQ(s.parity.block_start, t0);
        EXPECT_EQ(s.parity.block_close, t1);
        shard_header h;
        const auto   b = enc.pop();
        ASSERT_TRUE(parse_header(b.u8(), b.size(), &h));
        EXPECT_FALSE(h.is_data);
        EXPECT_EQ(h.k_i, j);
        EXPECT_EQ(h.sequence_number, 40);
    }
    EXPECT_EQ(st(enc).type, shard_type::none);
    EXPECT_EQ(enc.blocks(), 1u);
}

TEST(FecBlockEncoder, ShortCloseThenNextBlock)
{
    sender tx(6, 9, 300);
    for (uint32_t i = 0; i < 4; i++)
    {
        ASSERT_EQ(tx.push(make_app(i, 60)).size(), 1u);
    }
    auto parity = tx.flush();
    ASSERT_EQ(parity.size(), 3u);
    shard_header h;
    ASSERT_TRUE(parse_header(parity[0].data.data(), parity[0].data.size(), &h));
    EXPECT_EQ(h.sequence_number, 300);
    EXPECT_EQ(h.k_total, 4);
    EXPECT_EQ(h.n, 3);
    /* A closed block with nothing pushed is no block. */
    EXPECT_TRUE(tx.flush().empty());
    ASSERT_TRUE(tx.enc.start(tx.seq, 2, 3, k_datagram, clk::now()));
    tx.enc.close(clk::now());
    EXPECT_FALSE(tx.enc.is_open());
    EXPECT_EQ(tx.enc.queued(), 0u);
    EXPECT_EQ(tx.enc.blocks(), 1u);
    /* The next block reuses the rows and continues the sequence. */
    tx.configure(2, 4);
    tx.push(make_app(4, 60));
    auto s = tx.push(make_app(5, 60));
    ASSERT_EQ(s.size(), 3u);
    ASSERT_TRUE(parse_header(s[1].data.data(), s[1].data.size(), &h));
    EXPECT_EQ(h.sequence_number, 304);
    EXPECT_EQ(h.k_total, 2);
    EXPECT_EQ(h.n, 2);
}

TEST(FecBlockEncoder, DataShardIsThePushedBuffer)
{
    block_encoder enc;
    enc.set_tx_buffers(7, nullptr);
    EXPECT_EQ(enc.push_offset(), 10u);
    ASSERT_TRUE(enc.start(70, 3, 5, k_datagram, clk::now()));
    for (uint32_t i = 0; i < 3; i++)
    {
        const bytes app = make_app(i, 50 + i);
        const auto  in = to_buf(app, enc.push_offset());
        enc.push(in, clk::now());
        ASSERT_EQ(st(enc).type, shard_type::data);
        const auto d = enc.pop();
        /* The same buffer: header in the slot, 7 bytes in front for the next layers. */
        EXPECT_EQ(d.u8(), in.u8());
        EXPECT_EQ(d.size(), in.size());
        shard_header h;
        ASSERT_TRUE(parse_header(d.u8() + 7, d.size() - 7, &h));
        EXPECT_EQ(h.sequence_number, 70 + i);
        EXPECT_EQ(0, std::memcmp(d.u8() + 10, app.data(), app.size()));
    }
    EXPECT_EQ(enc.queued(), 2u);
}

TEST(FecBlockEncoder, PushDropsAndCounts)
{
    block_encoder enc;
    enc.set_tx_buffers(4, nullptr);
    const auto t = clk::now();
    enc.push(to_buf(make_app(0, 40), enc.push_offset()), t);
    EXPECT_EQ(enc.drop_not_open(), 1u);
    ASSERT_TRUE(enc.start(0, 2, 4, k_datagram, t));
    /* Shorter than headroom + slot. */
    enc.push(to_buf(bytes(6, 0), 0), t);
    EXPECT_EQ(enc.drop_no_headroom(), 1u);
    /* Headroom + slot but no data. */
    enc.push(to_buf(bytes(7, 0), 0), t);
    EXPECT_EQ(enc.drop_size(), 1u);
    EXPECT_EQ(enc.queued(), 0u);
    EXPECT_EQ(enc.rows(), 0); /* a dropped datagram takes no row */

    /* Shards still queued: the push is refused. */
    enc.push(to_buf(make_app(1, 40), enc.push_offset()), t);
    enc.push(to_buf(make_app(2, 40), enc.push_offset()), t);
    EXPECT_EQ(enc.drop_queued(), 1u);
    EXPECT_EQ(enc.queued(), 1u);
    /* ... and so is a new block while they are. */
    (void)enc.pop();
    enc.push(to_buf(make_app(2, 40), enc.push_offset()), t);
    EXPECT_EQ(enc.queued(), 3u); /* data + 2 parity */
    EXPECT_FALSE(enc.start(2, 2, 4, k_datagram, t));
    while (enc.queued() > 0)
    {
        (void)enc.pop();
    }

    /* Parity buffers that cannot be allocated lose the block's parity, counted. */
    enc.set_tx_buffers(4, [](size_t) { return bfc::shared_sized_buffer {}; });
    ASSERT_TRUE(enc.start(2, 2, 4, k_datagram, t));
    enc.push(to_buf(make_app(4, 40), enc.push_offset()), t);
    (void)enc.pop();
    enc.push(to_buf(make_app(5, 40), enc.push_offset()), t);
    EXPECT_EQ(enc.queued(), 1u);
    EXPECT_EQ(enc.drop_parity_alloc(), 1u);
    EXPECT_FALSE(enc.is_open());
}

TEST(FecBlockEncoder, ParityHasHeadroomAndAlignedBody)
{
    block_encoder enc;
    enc.set_tx_buffers(8,
                       [](size_t cap)
                       {
                           auto b = bfc::shared_sized_buffer::allocate(cap + 64);
                           b.resize(cap + 64);
                           return b;
                       });
    ASSERT_TRUE(enc.start(0, 3, 5, k_datagram, clk::now()));
    for (uint32_t i = 0; i < 3; i++)
    {
        enc.push(to_buf(make_app(i, 50 + i), enc.push_offset()), clk::now());
        (void)enc.pop();
    }
    ASSERT_EQ(enc.queued(), 2u);
    while (st(enc).type == shard_type::parity)
    {
        const auto b = enc.pop();
        /* [8 headroom][4 header][2 length parity][body], body 16-byte aligned. */
        EXPECT_EQ(b.size(), 8u + 4u + 2u + 52u);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(b.u8() + 8 + 4 + 2) % 16, 0u);
        shard_header h;
        ASSERT_TRUE(parse_header(b.u8() + 8, b.size() - 8, &h));
        EXPECT_FALSE(h.is_data);
    }
}

/* --- block_decoder --- */

TEST(FecBlockDecoder, RebuildsFromViewsAndPopsInRowOrder)
{
    std::mt19937 rng(21);
    sender       tx(5, 8, 900);
    auto         apps = make_apps(5, &rng);
    auto         all = tx.encode_all(apps); /* d0..d4 p0 p1 p2 */
    ASSERT_EQ(all.size(), 8u);
    block_decoder dec;
    dec.start(900, 5, 3);
    const auto view = [](const bytes& b, size_t skip)
    { return bfc::shared_sized_buffer::copy_from(b.data() + skip, b.size() - skip); };
    EXPECT_TRUE(dec.push_data(0, view(all[0].data, 3)));
    EXPECT_FALSE(dec.push_data(0, view(all[0].data, 3)));
    EXPECT_TRUE(dec.push_data(3, view(all[3].data, 3)));
    EXPECT_TRUE(dec.push_parity(2, view(all[7].data, 4)));
    EXPECT_FALSE(dec.decodable());
    EXPECT_TRUE(dec.push_parity(0, view(all[5].data, 4)));
    EXPECT_FALSE(dec.decodable());
    EXPECT_TRUE(dec.push_parity(1, view(all[6].data, 4)));
    ASSERT_TRUE(dec.decodable());
    ASSERT_TRUE(dec.decode());
    EXPECT_EQ(dec.data_count(), 0); /* rows dropped after the rebuild */
    for (int row : {1, 2, 4})
    {
        const auto s = dec.get_state();
        ASSERT_TRUE(s.ready);
        EXPECT_EQ(s.row, row);
        EXPECT_EQ(s.seq, 900 + row);
        const auto b = dec.pop();
        EXPECT_EQ(bytes(b.u8(), b.u8() + b.size()), apps[static_cast<size_t>(row)]);
    }
    EXPECT_FALSE(dec.get_state().ready);
    dec.reset();
    EXPECT_FALSE(dec.started());
}

/* --- fec_read_stream --- */

TEST(FecReadStream, PlainPassesThrough)
{
    receiver rx;
    rx.push(make_app(7, 30), lc_type::plain);
    ASSERT_EQ(rx.got.size(), 1u);
    EXPECT_EQ(rx.got[0], make_app(7, 30));
    EXPECT_EQ(rx.c().plain, 1u);
}

TEST(FecReadStream, LosslessInOrder)
{
    std::mt19937 rng(1);
    sender       tx(6, 8);
    auto         apps = make_apps(200, &rng);
    auto         all = tx.encode_all(apps);
    receiver     rx;
    for (const auto& s : all)
    {
        rx.push(s.data);
    }
    EXPECT_EQ(rx.got, apps);
    EXPECT_EQ(rx.c().recovered, 0u);
    EXPECT_EQ(rx.c().decode_fail, 0u);
    EXPECT_EQ(rx.c().missing_data + rx.c().missing_parity, 0u);
}

/* A receiver starts at the first data shard it sees, so the block under test follows a
 * complete one. */
TEST(FecReadStream, RecoversEveryPairOfErasures)
{
    std::mt19937 rng(2);
    for (int a = 0; a < 8; a++)
    {
        for (int b = a + 1; b < 8; b++)
        {
            sender   tx(6, 8);
            auto     apps = make_apps(12, &rng);
            auto     all = tx.encode_all(apps);
            receiver rx;
            ASSERT_EQ(all.size(), 16u);
            for (int i = 0; i < 16; i++)
            {
                if (i != 8 + a && i != 8 + b)
                {
                    rx.push(all[static_cast<size_t>(i)].data);
                }
            }
            EXPECT_EQ(by_index(rx.got), apps) << "lost " << a << "," << b;
            EXPECT_EQ(rx.c().decode_fail, 0u);
        }
    }
}

TEST(FecReadStream, Kn20N31MaxErasures)
{
    std::mt19937 rng(3);
    sender       tx(20, 31);
    auto         apps = make_apps(40, &rng);
    auto         all = tx.encode_all(apps);
    ASSERT_EQ(all.size(), 62u);
    std::vector<size_t> order(31);
    for (size_t i = 0; i < 31; i++)
    {
        order[i] = 31 + i;
    }
    std::shuffle(order.begin(), order.end(), rng);
    receiver rx;
    for (size_t i = 0; i < 31; i++)
    {
        rx.push(all[i].data);
    }
    for (size_t i = 0; i < 20; i++)
    {
        rx.push(all[order[i]].data);
    }
    EXPECT_EQ(by_index(rx.got), apps);
}

TEST(FecReadStream, DeliversOnArrivalRebuildsLate)
{
    std::mt19937 rng(4);
    sender       tx(4, 6);
    auto         apps = make_apps(8, &rng);
    auto         all = tx.encode_all(apps); /* d0..d3 p0 p1 d4..d7 p0 p1 */
    ASSERT_EQ(all.size(), 12u);
    receiver rx;
    /* Block 0 loses d1; its parity is spread past block 1's data. Nothing waits behind it. */
    for (size_t i : {0, 2, 3, 6, 7})
    {
        rx.push(all[i].data);
    }
    ASSERT_EQ(rx.got.size(), 5u);
    EXPECT_EQ(app_index(rx.got[1]), 2u);
    EXPECT_EQ(app_index(rx.got[3]), 4u);
    rx.push(all[4].data); /* block 0's parity row 0 rebuilds d1 */
    ASSERT_EQ(rx.got.size(), 6u);
    EXPECT_EQ(rx.got[5], apps[1]);
    for (size_t i : {8, 9, 5, 10, 11})
    {
        rx.push(all[i].data);
    }
    EXPECT_EQ(by_index(rx.got), apps);
    EXPECT_EQ(rx.c().recovered, 1u);
    EXPECT_EQ(rx.c().data, 7u);
}

TEST(FecReadStream, FlushedPartialBlockRecovers)
{
    std::mt19937 rng(5);
    sender       tx(6, 9);
    auto         apps = make_apps(4, &rng);
    auto         all = tx.encode_all(apps);
    auto         parity = tx.flush();
    ASSERT_EQ(parity.size(), 3u);
    receiver rx;
    rx.push(all[0].data);
    rx.push(all[3].data);
    rx.push(parity[0].data);
    rx.push(parity[2].data);
    EXPECT_EQ(by_index(rx.got), apps);
}

TEST(FecReadStream, RuntimeKnChangeKeepsSequence)
{
    std::mt19937       rng(6);
    sender             tx(6, 8);
    auto               apps = make_apps(40, &rng);
    std::vector<shard> all;
    for (size_t i = 0; i < apps.size(); i++)
    {
        if (i == 15)
        {
            /* Mid-block: the running block keeps 6/8; the next one is 4/7. */
            tx.configure(4, 7);
        }
        auto s = tx.push(apps[i]);
        all.insert(all.end(), s.begin(), s.end());
    }
    auto tail = tx.flush();
    all.insert(all.end(), tail.begin(), tail.end());
    receiver rx;
    for (size_t i = 0; i < all.size(); i++)
    {
        if (i % 5 != 2) /* lose a fifth of the shards, spread out */
        {
            rx.push(all[i].data);
        }
    }
    EXPECT_EQ(by_index(rx.got), apps);
    EXPECT_EQ(rx.c().decode_fail, 0u);
}

TEST(FecReadStream, SequenceWrapNoLoss)
{
    std::mt19937 rng(7);
    sender       tx(5, 7, 65530);
    auto         apps = make_apps(30, &rng, 200);
    auto         all = tx.encode_all(apps);
    receiver     rx;
    for (size_t i = 0; i < all.size(); i++)
    {
        if (i != 3 && i != 10)
        {
            rx.push(all[i].data);
        }
    }
    EXPECT_EQ(by_index(rx.got), apps);
}

TEST(FecReadStream, HoleWithoutParityGivenUpAtCapThenAtEstimate)
{
    fake_clock clk;
    receiver   rx;
    clk.attach(rx.rs);
    std::mt19937 rng(8);
    sender       tx(4, 6);
    auto         apps = make_apps(36, &rng, 100);
    auto         all = tx.encode_all(apps); /* 9 blocks of 6 shards */
    const auto   lost = [&rx] { return rx.c().missing_data; };
    /* Block 0 loses d2 and both parity: nothing can recover it. */
    for (size_t i = 0; i < 6; i++)
    {
        if (i == 2 || i >= 4)
        {
            continue;
        }
        rx.push(all[i].data);
        clk.advance(1);
    }
    EXPECT_EQ(rx.got.size(), 3u); /* delivered on arrival, the hole does not hold them */
    /* No estimate yet: the hole waits about the 250 ms cap from its block's close. */
    fec_read_stream::time_point at {};
    ASSERT_TRUE(rx.rs.next_deadline(&at));
    EXPECT_GT(at, clk.t + std::chrono::milliseconds(240));
    clk.advance(200);
    rx.poll();
    EXPECT_EQ(lost(), 0u);
    clk.advance(60);
    rx.poll();
    EXPECT_EQ(lost(), 1u);
    EXPECT_EQ(rx.c().given_up_blocks, 1u);

    /* k = 4, n = 2 (c = 6), spread 12 ms: row gap 2 ms. The sender releases parity row j at
     * max(close + 2j, first + 8 + 2j) ms. Slowly filled blocks (data 3 ms apart) get their parity
     * right after the close, at +0 and +2 ms. */
    for (size_t b = 1; b < 6; b++)
    {
        for (size_t i = 0; i < 4; i++)
        {
            if (i > 0)
            {
                clk.advance(3);
            }
            rx.push(all[b * 6 + i].data);
        }
        rx.push(all[b * 6 + 4].data);
        clk.advance(2);
        rx.push(all[b * 6 + 5].data);
        clk.advance(10);
    }
    ASSERT_TRUE(rx.rs.have_spread_estimate());
    EXPECT_NEAR(rx.rs.spread_estimate_ms(), 12.0, 0.5);

    /* Block 6 fills in a burst (fill 0.2 ms): its parity is due at close + 7.8 and + 9.8 ms. It
     * loses d1 and parity row 0; row 1 still recovers it, though the slow blocks taught nothing
     * longer than 2 ms. */
    rx.push(all[36].data);
    clk.advance_us(100);
    rx.push(all[38].data);
    clk.advance_us(100);
    rx.push(all[39].data); /* the close */
    clk.advance(9);
    rx.poll();
    EXPECT_EQ(lost(), 1u);
    clk.advance_us(800);
    rx.push(all[41].data);
    EXPECT_EQ(rx.c().recovered, 1u);
    EXPECT_EQ(rx.c().given_up_blocks, 1u);
    clk.advance(10);

    /* Block 7, also a burst, loses d1 and both parity: given up after close + 9.8 ms + margin,
     * not at the 250 ms cap. */
    rx.push(all[42].data);
    clk.advance_us(100);
    rx.push(all[44].data);
    clk.advance_us(100);
    rx.push(all[45].data);
    clk.advance(12);
    rx.poll();
    EXPECT_EQ(lost(), 1u);
    clk.advance(10);
    rx.poll();
    EXPECT_EQ(lost(), 2u);
    clk.advance(10);

    /* Block 8 fills slowly and loses d1 and both parity: its parity was due at close + 2 ms, so
     * it is given up well before a burst block would be. */
    rx.push(all[48].data);
    clk.advance(6);
    rx.push(all[50].data);
    clk.advance(3);
    rx.push(all[51].data);
    clk.advance(4);
    rx.poll();
    EXPECT_EQ(lost(), 2u);
    clk.advance(6);
    rx.poll();
    EXPECT_EQ(lost(), 3u);
    EXPECT_EQ(rx.c().given_up_blocks, 3u);
    EXPECT_EQ(rx.got.size(), 33u);
}

TEST(FecReadStream, DataOnlyStreamSkipsHoleOnNextData)
{
    sender             tx(1, 1);
    receiver           rx;
    std::vector<bytes> sent;
    for (uint32_t i = 0; i < 300; i++)
    {
        const bytes a = make_app(i, 20);
        auto        s = tx.push(a);
        ASSERT_EQ(s.size(), 1u);
        if (i == 280)
        {
            continue;
        }
        sent.push_back(a);
        rx.push(s[0].data);
    }
    /* Past the data-only threshold a hole goes as soon as later data shows up. */
    EXPECT_EQ(rx.got, sent);
    EXPECT_EQ(rx.c().missing_data, 1u);
    fec_read_stream::time_point at {};
    EXPECT_FALSE(rx.rs.next_deadline(&at));
}

TEST(FecReadStream, MissingParityCountedWhenBlockDone)
{
    fake_clock clk;
    receiver   rx;
    clk.attach(rx.rs);
    std::mt19937 rng(9);
    sender       tx(4, 6);
    auto         all = tx.encode_all(make_apps(8, &rng, 50));
    for (size_t i = 0; i < all.size(); i++)
    {
        if (i != 5)
        {
            rx.push(all[i].data);
        }
    }
    EXPECT_EQ(rx.c().missing_parity, 0u);
    clk.advance(300);
    rx.poll();
    EXPECT_EQ(rx.c().missing_parity, 1u);
    EXPECT_EQ(rx.c().decode_fail, 0u);
}

TEST(FecReadStream, PeerRestartTakesNewReference)
{
    std::mt19937 rng(10);
    sender       a(4, 6, 1000);
    sender       b(4, 6, 30000);
    auto         apps_a = make_apps(10, &rng);
    auto         apps_b = make_apps(10, &rng);
    receiver     rx;
    for (const auto& s : a.encode_all(apps_a))
    {
        rx.push(s.data);
    }
    for (const auto& s : b.encode_all(apps_b))
    {
        rx.push(s.data);
    }
    std::vector<bytes> want = apps_a;
    want.insert(want.end(), apps_b.begin(), apps_b.end());
    EXPECT_EQ(rx.got, want);
    EXPECT_EQ(rx.c().new_references, 1u);
}

TEST(FecReadStream, LateDataDeliveredOnceDuplicatesDropped)
{
    sender             tx(1, 1);
    std::vector<bytes> wire;
    for (uint32_t i = 0; i < 300; i++)
    {
        wire.push_back(tx.push(make_app(i, 10))[0].data);
    }
    receiver rx;
    for (size_t i = 0; i < 300; i++)
    {
        if (i != 290)
        {
            rx.push(wire[i]);
        }
    }
    ASSERT_EQ(rx.got.size(), 299u);
    EXPECT_EQ(rx.c().missing_data, 1u);
    /* Late, behind the give-up point: the app never had it, so it goes out. */
    rx.push(wire[290]);
    ASSERT_EQ(rx.got.size(), 300u);
    EXPECT_EQ(rx.got.back(), make_app(290, 10));
    EXPECT_EQ(rx.c().late, 1u);
    EXPECT_EQ(rx.c().new_references, 0u);
    rx.push(wire[290]);
    rx.push(wire[299]);
    EXPECT_EQ(rx.got.size(), 300u);
    EXPECT_EQ(rx.c().duplicates, 2u);
}

TEST(FecReadStream, KnMismatchCounted)
{
    receiver rx;
    uint8_t  p[4 + 2 + 16] {};
    ASSERT_TRUE(write_parity_header(p, 50, 2, 4, 0));
    rx.push(bytes(p, p + sizeof(p)));
    ASSERT_TRUE(write_parity_header(p, 50, 3, 4, 1));
    rx.push(bytes(p, p + sizeof(p)));
    EXPECT_EQ(rx.c().kn_mismatch, 1u);
    /* The replaced block is counted: it got one of its two parity shards. */
    EXPECT_EQ(rx.c().missing_parity, 1u);
    const uint8_t junk[] = {0x7F};
    rx.push(bytes(junk, junk + 1));
    EXPECT_EQ(rx.c().hdr_errors, 1u);
}

TEST(FecReadStream, LateParityFarBehindDropped)
{
    std::mt19937 rng(12);
    sender       tx(4, 5);
    auto         apps = make_apps(204, &rng, 50);
    auto         all = tx.encode_all(apps); /* 51 blocks of 5 shards */
    receiver     rx;
    for (size_t i = 0; i < 250; i++)
    {
        if (i != 4)
        {
            rx.push(all[i].data);
        }
    }
    ASSERT_EQ(rx.got.size(), 200u);
    /* Block 0's parity arrives 196 data shards late: dropped, not a new session. */
    rx.push(all[4].data);
    EXPECT_EQ(rx.c().new_references, 0u);
    for (size_t i = 250; i < 255; i++)
    {
        rx.push(all[i].data);
    }
    EXPECT_EQ(rx.got, apps);
}

TEST(FecReadStream, DuplicateParityOfDoneBlockIgnored)
{
    fake_clock clk;
    receiver   rx;
    clk.attach(rx.rs);
    std::mt19937 rng(13);
    sender       tx(4, 6);
    auto         all = tx.encode_all(make_apps(8, &rng, 50));
    for (const auto& s : all)
    {
        rx.push(s.data);
    }
    clk.advance(300);
    rx.poll();
    EXPECT_EQ(rx.c().missing_parity, 0u);
    rx.push(all[5].data);
    clk.advance(300);
    rx.poll();
    EXPECT_EQ(rx.c().missing_parity, 0u);
    EXPECT_EQ(rx.c().decode_fail, 0u);
}

TEST(FecReadStream, DataOnlyRunOfHolesIsOneBlock)
{
    sender   tx(1, 1);
    receiver rx;
    for (uint32_t i = 0; i < 300; i++)
    {
        auto s = tx.push(make_app(i, 20));
        if (i < 280 || i >= 290)
        {
            rx.push(s[0].data);
        }
    }
    EXPECT_EQ(rx.got.size(), 290u);
    EXPECT_EQ(rx.c().missing_data, 10u);
    EXPECT_EQ(rx.c().given_up_blocks, 1u);
}

TEST(FecReadStream, ShortFlushedBlockKeepsNextBlockWait)
{
    fake_clock clk;
    receiver   rx;
    clk.attach(rx.rs);
    std::mt19937 rng(14);
    sender       tx(8, 10);
    auto         apps = make_apps(27, &rng, 100);
    /* Blocks 100 and 108 (k = 8) teach a spread: parity 1 and 2 ms after the close. */
    for (size_t b = 0; b < 2; b++)
    {
        std::vector<shard> parity;
        for (size_t i = 0; i < 8; i++)
        {
            auto s = tx.push(apps[b * 8 + i]);
            rx.push(s[0].data);
            parity.assign(s.begin() + 1, s.end());
            clk.advance(1);
        }
        for (const auto& p : parity)
        {
            rx.push(p.data);
            clk.advance(1);
        }
        clk.advance(5);
    }
    /* A flush closes block 116 after 3 data shards. */
    for (size_t i = 16; i < 19; i++)
    {
        rx.push(tx.push(apps[i])[0].data);
        clk.advance(1);
    }
    for (const auto& p : tx.flush())
    {
        rx.push(p.data);
        clk.advance(1);
    }
    ASSERT_TRUE(rx.rs.have_spread_estimate());
    /* Block 119 (k = 8 again) loses 120 and fills slowly. Taken as a 3-long block, its close
     * would be 121's arrival and 120 would be given up long before the parity comes. */
    std::vector<shard> parity;
    for (size_t i = 19; i < 27; i++)
    {
        auto s = tx.push(apps[i]);
        if (i != 20)
        {
            rx.push(s[0].data);
        }
        parity.assign(s.begin() + 1, s.end());
        clk.advance(10);
    }
    ASSERT_EQ(parity.size(), 2u);
    rx.push(parity[0].data);
    EXPECT_EQ(by_index(rx.got), apps);
    EXPECT_EQ(rx.c().given_up_blocks, 0u);
}

TEST(FecReadStream, RandomLossStreamRecoversWithinParity)
{
    std::mt19937       rng(11);
    sender             tx(10, 14);
    auto               apps = make_apps(1000, &rng);
    std::vector<shard> all;
    std::vector<int>   block_of_shard;
    int                block = 0;
    for (const auto& a : apps)
    {
        auto s = tx.push(a);
        for (const auto& x : s)
        {
            all.push_back(x);
            block_of_shard.push_back(block);
        }
        if (s.size() > 1)
        {
            block++;
        }
    }
    /* Lose up to 4 shards of each block (any of its 14). */
    std::vector<int>            lost(static_cast<size_t>(block + 1), 0);
    std::bernoulli_distribution drop(0.2);
    receiver                    rx;
    for (size_t i = 0; i < all.size(); i++)
    {
        const int b = block_of_shard[i];
        if (drop(rng) && lost[static_cast<size_t>(b)] < 4)
        {
            lost[static_cast<size_t>(b)]++;
            continue;
        }
        rx.push(all[i].data);
    }
    EXPECT_EQ(by_index(rx.got), apps);
    EXPECT_EQ(rx.c().decode_fail, 0u);
    EXPECT_GT(rx.c().recovered, 0u);
}

TEST(FecReadStream, ReleasesDatagramsBehindGiveUpPoint)
{
    std::mt19937    rng(15);
    sender          tx(4, 6);
    auto            all = tx.encode_all(make_apps(400, &rng, 60)); /* 100 blocks */
    fec_read_stream rs;
    int             released = 0;
    for (const auto& s : all)
    {
        auto* b = new std::byte[s.data.size()];
        std::memcpy(b, s.data.data(), s.data.size());
        rs.push(bfc::shared_sized_buffer::adopt(b, s.data.size(), s.data.size(),
                                                [&released](std::byte* p)
                                                {
                                                    released++;
                                                    delete[] p;
                                                }),
                lc_type::fec);
        while (!rs.pop().empty())
        {
        }
    }
    /* Only data shards the last blocks may still rebuild with, and the parity of blocks still
     * open for late parity, may be held: never the stream's history. */
    EXPECT_GE(released, static_cast<int>(all.size()) - 64);
    rs.reset();
    EXPECT_EQ(released, static_cast<int>(all.size()));
}

TEST(FecReadStream, OutputRingDropsOldest)
{
    receiver rx;
    rx.rs.set_output_capacity(2);
    for (uint32_t i = 0; i < 3; i++)
    {
        rx.rs.push(to_buf(make_app(i, 10), 0), lc_type::plain);
    }
    EXPECT_EQ(rx.c().drop_output, 1u);
    rx.take();
    ASSERT_EQ(rx.got.size(), 2u);
    EXPECT_EQ(app_index(rx.got[0]), 1u);
}
