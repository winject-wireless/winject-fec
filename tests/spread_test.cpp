/* Codec-level spreading and interleaving: offsets, and decode of interleaved blocks. */
#include "test_util.hpp"

#include "winject_fec/spread.hpp"

using namespace fec_test;

namespace
{

/* One block's shards in send order: data at once, then parity. */
std::vector<std::vector<uint8_t>> encode_tags(sender &tx, const std::vector<uint8_t> &tags)
{
    std::vector<std::vector<uint8_t>> air;
    for (uint8_t t : tags)
    {
        for (auto &s : tx.push(std::vector<uint8_t>(32, t)))
        {
            air.push_back(std::move(s.data));
        }
    }
    return air;
}

/* Tags in the order the receiver delivered them. */
std::vector<uint8_t> decode_order(receiver &rx, const std::vector<std::vector<uint8_t>> &wire,
                                  const std::vector<bool> &lost)
{
    rx.got.clear();
    for (size_t i = 0; i < wire.size(); i++)
    {
        if (!lost[i])
        {
            rx.push(wire[i]);
        }
    }
    rx.poll();
    std::vector<uint8_t> tags;
    for (const auto &p : rx.got)
    {
        tags.push_back(p[0]);
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
    sender tx(4, 6, 96);
    const auto prior = encode_tags(tx, {90, 91, 92, 93});
    const auto a = encode_tags(tx, {1, 2, 3, 4});
    const auto b = encode_tags(tx, {5, 6, 7, 8});
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
    receiver dec;
    EXPECT_EQ((std::vector<uint8_t> {90, 91, 92, 93}),
              decode_order(dec, prior, std::vector<bool>(prior.size(), false)));
    EXPECT_EQ((std::vector<uint8_t> {1, 2, 3, 4, 5, 6, 7, 8}), decode_order(dec, wire, lost));
}

/* Start-up edge case: a fresh receiver anchors in-order release on the first data shard it
 * sees. If the earlier block's first shard is lost and a later block's arrives first, the
 * earlier block is behind the reference: late, and ignored. */
TEST(FecSpreadTest, FreshReceiverAnchorsOnFirstShardSeen)
{
    sender tx(4, 6);
    const auto a = encode_tags(tx, {1, 2, 3, 4});
    const auto b = encode_tags(tx, {5, 6, 7, 8});
    std::vector<std::vector<uint8_t>> wire;
    for (size_t i = 0; i < a.size(); i++)
    {
        wire.push_back(a[i]);
        wire.push_back(b[i]);
    }
    std::vector<bool> lost(wire.size(), false);
    lost[0] = true;  // A data 0: B data 0 is the first shard this receiver sees
    receiver dec;
    EXPECT_EQ((std::vector<uint8_t> {5, 6, 7, 8}), decode_order(dec, wire, lost));
    EXPECT_EQ(dec.c().late, 3u);
}

/* A burst of 4 consecutive wire losses: sent back to back it takes 4 shards of one 4/6 block
 * (unrecoverable); interleaved over two blocks it takes 2 of each (both recover). */
TEST(FecSpreadTest, InterleavingSurvivesBurst)
{
    sender tx(4, 6);
    const auto prior = encode_tags(tx, {90, 91, 92, 93});
    const auto a = encode_tags(tx, {1, 2, 3, 4});
    const auto b = encode_tags(tx, {5, 6, 7, 8});

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

    receiver dec_serial;
    (void)decode_order(dec_serial, prior, none);
    const auto got_serial = decode_order(dec_serial, serial, burst);
    EXPECT_LT(got_serial.size(), 8u);

    receiver dec_mixed;
    (void)decode_order(dec_mixed, prior, none);
    EXPECT_EQ((std::vector<uint8_t> {1, 2, 3, 4, 5, 6, 7, 8}),
              decode_order(dec_mixed, mixed, burst));
}

