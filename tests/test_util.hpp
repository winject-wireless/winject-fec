/* Shared test helpers: app datagrams, a block-at-a-time sender, a read-stream receiver and a
 * fake clock. */
#ifndef WINJECT_FEC_TESTS_TEST_UTIL_HPP
#define WINJECT_FEC_TESTS_TEST_UTIL_HPP

#include "winject_fec/block_encoder.hpp"
#include "winject_fec/read_stream.hpp"
#include "winject_fec/wire.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

namespace fec_test
{

using namespace winject::fec;

constexpr size_t k_datagram = 1476;

using bytes = std::vector<uint8_t>;

struct shard
{
    bytes data;
    bool  parity = false;
};

/* App datagram i: its index in the first four bytes, then a pattern. */
inline bytes make_app(uint32_t i, size_t len)
{
    bytes b(std::max<size_t>(len, 4));
    std::memcpy(b.data(), &i, 4);
    for (size_t j = 4; j < b.size(); j++)
    {
        b[j] = static_cast<uint8_t>(i * 31 + j);
    }
    return b;
}

inline uint32_t app_index(const bytes& b)
{
    uint32_t i = 0;
    std::memcpy(&i, b.data(), 4);
    return i;
}

inline std::vector<bytes> make_apps(size_t count, std::mt19937* rng, size_t max_len = 1400)
{
    std::uniform_int_distribution<size_t> len(1, max_len);
    std::vector<bytes>                    apps;
    for (size_t i = 0; i < count; i++)
    {
        apps.push_back(make_app(static_cast<uint32_t>(i), len(*rng)));
    }
    return apps;
}

/* The datagram as a receive path leaves it: room bytes free in front, then the data. */
inline bfc::shared_sized_buffer to_buf(const bytes& b, size_t room)
{
    auto buf = bfc::shared_sized_buffer::allocate(room + b.size());
    buf.resize(room + b.size());
    std::copy(b.begin(), b.end(), buf.u8() + room);
    return buf;
}

/* Drives a clock: deadlines are tested by advancing it, not by sleeping. */
struct fake_clock
{
    std::chrono::steady_clock::time_point t =
        std::chrono::steady_clock::time_point {} + std::chrono::hours(1);
    template <typename T>
    void attach(T& x)
    {
        x.set_clock([this] { return t; });
    }
    void advance(int ms)
    {
        t += std::chrono::milliseconds(ms);
    }
    void advance_us(int64_t us)
    {
        t += std::chrono::microseconds(us);
    }
};

/* A sender with no timing: block_encoder blocks back to back, parity right after the close.
 * configure() applies from the next block, as in fec_write_stream. */
struct sender
{
    block_encoder enc;
    int           k = 0;
    int           n = 0;
    uint16_t      seq = 100;

    sender(int k_, int n_, uint16_t seq_ = 100) : k(k_), n(n_), seq(seq_) {}

    void configure(int k_, int n_)
    {
        k = k_;
        n = n_;
    }
    std::vector<shard> drain()
    {
        std::vector<shard> v;
        while (enc.get_state().type != shard_type::none)
        {
            const bool parity = enc.get_state().type == shard_type::parity;
            const auto b = enc.pop();
            v.push_back({bytes(b.u8(), b.u8() + b.size()), parity});
        }
        return v;
    }
    std::vector<shard> push(const bytes& app)
    {
        const auto t = std::chrono::steady_clock::now();
        if (!enc.is_open())
        {
            EXPECT_TRUE(enc.start(seq, k, n, k_datagram, t));
        }
        enc.push(to_buf(app, enc.push_offset()), t);
        if (enc.queued() > 0)
        {
            seq++;
        }
        return drain();
    }
    std::vector<shard> flush()
    {
        enc.close(std::chrono::steady_clock::now());
        return drain();
    }
    /* Every shard in send order (data at once, parity after its block's last data). */
    std::vector<shard> encode_all(const std::vector<bytes>& apps)
    {
        std::vector<shard> all;
        for (const auto& a : apps)
        {
            auto s = push(a);
            all.insert(all.end(), s.begin(), s.end());
        }
        return all;
    }
};

/* A read stream in manual mode, collecting what it delivers. */
struct receiver
{
    fec_read_stream    rs;
    std::vector<bytes> got;

    receiver()
    {
        rs.set_output_capacity(4096);
    }
    void take()
    {
        for (auto b = rs.pop(); !b.empty(); b = rs.pop())
        {
            got.emplace_back(b.u8(), b.u8() + b.size());
        }
    }
    void push(const bytes& b, lc_type type = lc_type::fec)
    {
        rs.push(bfc::shared_sized_buffer::copy_from(b.data(), b.size()), type);
        take();
    }
    void poll()
    {
        rs.poll();
        take();
    }
    fec_read_stream::counters c() const
    {
        return rs.get_counters();
    }
};

}  // namespace fec_test

#endif  // WINJECT_FEC_TESTS_TEST_UTIL_HPP
