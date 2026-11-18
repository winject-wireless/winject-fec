#include "rs_common.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <mutex>
#include <utility>

#if defined(ESP_PLATFORM) && __has_include("sdkconfig.h")
#include "sdkconfig.h"
#endif
#if defined(ESP_PLATFORM) && defined(CONFIG_SPIRAM) && CONFIG_SPIRAM
#define WINJECT_FEC_HAVE_PSRAM 1
#include <new>

#include "esp_heap_caps.h"
#endif

extern "C"
{
#include "erasure_code.h"
}

namespace winject::fec::detail
{

const kn_encode_tables& kn_encode_tables_for(int k, int total)
{
    /* Only the (k, total) pairs in use, on the heap: a table for every pair would be ~56 KB of
     * static RAM, which on the ESP32-P4 is internal RAM the H.264 encoder needs. Map nodes never
     * move, so the returned reference stays valid. */
    static std::mutex                                       mu;
    static std::map<std::pair<int, int>, kn_encode_tables> cache;
    std::lock_guard<std::mutex>                             lock(mu);
    const auto                                              found = cache.find({k, total});
    if (found != cache.end())
    {
        return found->second;
    }
    kn_encode_tables built;
    const int        p = total - k;
    built.encode_matrix.assign(static_cast<size_t>(total) * static_cast<size_t>(k), 0);
    built.g_tbls.assign(static_cast<size_t>(k) * static_cast<size_t>(std::max(p, 1)) * 32, 0);
    gf_gen_cauchy1_matrix(built.encode_matrix.data(), total, k);
    if (p > 0)
    {
        ec_init_tables_base(k, p, built.encode_matrix.data() + k * k, built.g_tbls.data());
    }
    return cache.emplace(std::make_pair(k, total), std::move(built)).first->second;
}

int gen_decode_matrix(int k, int total, const uint8_t* encode_matrix, const uint8_t* err_list,
                      int nerrs, uint8_t* decode_matrix, uint8_t* decode_index)
{
    std::array<uint8_t, k_max_rows> in_err {};
    for (int i = 0; i < nerrs; i++)
    {
        if (err_list[i] >= total)
        {
            return -1;
        }
        in_err[err_list[i]] = 1;
    }
    std::vector<uint8_t> b(static_cast<size_t>(k) * static_cast<size_t>(k));
    std::vector<uint8_t> invert(static_cast<size_t>(k) * static_cast<size_t>(k));
    int                  r = 0;
    for (int i = 0; i < k; i++, r++)
    {
        while (r < total && in_err[static_cast<size_t>(r)])
        {
            r++;
        }
        if (r >= total)
        {
            return -1;
        }
        memcpy(b.data() + static_cast<size_t>(k) * static_cast<size_t>(i),
               encode_matrix + static_cast<size_t>(k) * static_cast<size_t>(r),
               static_cast<size_t>(k));
        decode_index[i] = static_cast<uint8_t>(r);
    }
    if (gf_invert_matrix(b.data(), invert.data(), k) < 0)
    {
        return -1;
    }
    for (int e = 0; e < nerrs && err_list[e] < k; e++)
    {
        memcpy(decode_matrix + static_cast<size_t>(k) * static_cast<size_t>(e),
               invert.data() + static_cast<size_t>(k) * static_cast<size_t>(err_list[e]),
               static_cast<size_t>(k));
    }
    return 0;
}

bfc::shared_sized_buffer alloc_aligned(size_t len, bool psram)
{
#ifdef WINJECT_FEC_HAVE_PSRAM
    if (psram)
    {
        auto* bytes = static_cast<std::byte*>(
            heap_caps_aligned_alloc(k_body_align, len, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
        if (bytes == nullptr)
        {
            throw std::bad_alloc();
        }
        return bfc::shared_sized_buffer::adopt(bytes, len, len,
                                               [](std::byte* p) { heap_caps_free(p); });
    }
#else
    (void)psram;
#endif
    auto storage = bfc::shared_sized_buffer::allocate(len + k_body_align - 1);
    storage.resize(len + k_body_align - 1);
    const size_t off =
        (k_body_align - reinterpret_cast<uintptr_t>(storage.u8()) % k_body_align) % k_body_align;
    return storage.subview(off, len);
}

bfc::shared_sized_buffer alloc_decode_row(size_t len)
{
#if defined(CONFIG_WINJECT_FEC_RX_PSRAM) && CONFIG_WINJECT_FEC_RX_PSRAM
    return alloc_aligned(len, true);
#else
    return alloc_aligned(len, false);
#endif
}

bfc::shared_sized_buffer alloc_encode_rows(size_t len)
{
#if defined(CONFIG_WINJECT_FEC_TX_PSRAM) && CONFIG_WINJECT_FEC_TX_PSRAM
    return alloc_aligned(len, true);
#else
    return alloc_aligned(len, false);
#endif
}

void init_decode_tables(int k, int rows, uint8_t* matrix, uint8_t* g_tbls)
{
    ec_init_tables_base(k, rows, matrix, g_tbls);
}

}  // namespace winject::fec::detail
