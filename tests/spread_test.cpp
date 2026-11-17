/* Codec-level spreading and interleaving: offsets, and decode of interleaved blocks. */
#include "winject_fec/decoder.hpp"
#include "winject_fec/encoder.hpp"
#include "winject_fec/spread.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <vector>

using namespace winject::fec;

namespace
{

/* One block's shards in send order: data at once, then parity. */
std::vector<std::vector<uint8_t>> encode_tags(encoder &enc,
                                              const std::vector<uint8_t> &tags)
{
    std::vector<std::vector<uint8_t>> air;
    for (uint8_t t : tags)
    {
        auto room = bfc::shared_sized_buffer::allocate(k_data_header_len + 32);
        room.resize(k_data_header_len + 32);
        std::memset(room.u8(), t, room.size());
        enc.push(room);
        while (enc.get_state().type != encoder::shard_type::none)
        {
            const auto b = enc.pop();
            air.emplace_back(b.u8(), b.u8() + b.size());
        }
    }
    return air;
}

std::vector<uint8_t> decode_order(decoder &dec,
                                  const std::vector<std::vector<uint8_t>> &wire,
                                  const std::vector<bool> &lost)
{
    payload_list out;
    for (size_t i = 0; i < wire.size(); i++)
    {
        if (lost[i])
        {
            continue;
        }
        payload_list step;
        dec.push_air(wire[i].data(), wire[i].size(), &step);
        for (auto &p : step)
        {
            out.push_back(std::move(p));
        }
    }
    payload_list tail;
    dec.poll_rx(&tail);
    for (auto &p : tail)
    {
        out.push_back(std::move(p));
    }
    std::vector<uint8_t> tags;
    for (const auto &p : out)
    {
        tags.push_back(p.u8()[0]);
    }
    return tags;
}

}  // namespace

TEST(FecSpreadTest, OffsetsTileTheWindow)
{
    using std::chrono::microseconds;
    EXPECT_EQ(spread_offset(0, 24, 20), microseconds(0));
    EXPECT_EQ(spread_offset(1, 24, 20), microseconds(833));
    EXPECT_EQ(spread_offset(23, 24, 20), microseconds(19166));
    EXPECT_EQ(spread_offset(8, 9, 20), microseconds(17777));
    EXPECT_EQ(spread_offset(5, 24, 0), microseconds(0));
    EXPECT_EQ(spread_offset(5, 0, 20), microseconds(0));
}

/* Receiver in a running stream: shards of two blocks arriving interleaved decode in order,
 * with a loss in each (including the first shard of the earlier block). */
TEST(FecSpreadTest, ReceiverDecodesInterleavedBlocks)
{
    encoder enc;
    ASSERT_TRUE(enc.configure(4, 6, 1476));
    enc.set_tx_sequence(96);
    const auto prior = encode_tags(enc, {90, 91, 92, 93});
    const auto a = encode_tags(enc, {1, 2, 3, 4});
    const auto b = encode_tags(enc, {5, 6, 7, 8});
    std::vector<std::vector<uint8_t>> wire;
    for (size_t i = 0; i < a.size(); i++)
    {
        wire.push_back(a[i]);
        wire.push_back(b[i]);
    }
    std::vector<bool> lost(wire.size(), false);
    lost[0] = true;  // A data 0
    lost[3] = true;  // B data 1
    lost[6] = true;  // A data 3
    decoder dec;
    EXPECT_EQ((std::vector<uint8_t> {90, 91, 92, 93}),
              decode_order(dec, prior, std::vector<bool>(prior.size(), false)));
    EXPECT_EQ((std::vector<uint8_t> {1, 2, 3, 4, 5, 6, 7, 8}), decode_order(dec, wire, lost));
}

/* Start-up edge case: a fresh receiver anchors in-order delivery on the first data shard it
 * sees. If the earlier block's first shard is lost and a later block's arrives first, the
 * earlier block is behind the reference: late, and ignored. */
TEST(FecSpreadTest, FreshReceiverAnchorsOnFirstShardSeen)
{
    encoder enc;
    ASSERT_TRUE(enc.configure(4, 6, 1476));
    const auto a = encode_tags(enc, {1, 2, 3, 4});
    const auto b = encode_tags(enc, {5, 6, 7, 8});
    std::vector<std::vector<uint8_t>> wire;
    for (size_t i = 0; i < a.size(); i++)
    {
        wire.push_back(a[i]);
        wire.push_back(b[i]);
    }
    std::vector<bool> lost(wire.size(), false);
    lost[0] = true;  // A data 0: B data 0 is the first shard this receiver sees
    decoder dec;
    EXPECT_EQ((std::vector<uint8_t> {5, 6, 7, 8}), decode_order(dec, wire, lost));
    EXPECT_GT(dec.late_blocks(), 0u);
}

/* A burst of 4 consecutive wire losses: sent back to back it takes 4 shards of one 4/6 block
 * (unrecoverable); interleaved over two blocks it takes 2 of each (both recover). */
TEST(FecSpreadTest, InterleavingSurvivesBurst)
{
    encoder enc;
    ASSERT_TRUE(enc.configure(4, 6, 1476));
    const auto prior = encode_tags(enc, {90, 91, 92, 93});
    const auto a = encode_tags(enc, {1, 2, 3, 4});
    const auto b = encode_tags(enc, {5, 6, 7, 8});

    std::vector<std::vector<uint8_t>> serial(a);
    serial.insert(serial.end(), b.begin(), b.end());
    std::vector<std::vector<uint8_t>> mixed;
    for (size_t i = 0; i < a.size(); i++)
    {
        mixed.push_back(a[i]);
        mixed.push_back(b[i]);
    }
    std::vector<bool> burst(12, false);
    for (size_t i = 1; i < 5; i++)
    {
        burst[i] = true;
    }
    const std::vector<bool> none(prior.size(), false);

    decoder dec_serial;
    (void)decode_order(dec_serial, prior, none);
    const auto got_serial = decode_order(dec_serial, serial, burst);
    EXPECT_LT(got_serial.size(), 8u);

    decoder dec_mixed;
    (void)decode_order(dec_mixed, prior, none);
    EXPECT_EQ((std::vector<uint8_t> {1, 2, 3, 4, 5, 6, 7, 8}),
              decode_order(dec_mixed, mixed, burst));
}

