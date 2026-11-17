#include "rs_common.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <mutex>
#include <optional>

#if defined(ESP_PLATFORM) && __has_include("sdkconfig.h")
#include "sdkconfig.h"
#endif
#if defined(CONFIG_WINJECT_FEC_RX_PSRAM) && CONFIG_WINJECT_FEC_RX_PSRAM
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
    static std::mutex mu;
    static std::array<std::array<std::optional<kn_encode_tables>, k_max_rows + 1>, k_max_k + 1>
                                cache;
    std::lock_guard<std::mutex> lock(mu);
    std::optional<kn_encode_tables>& slot =
        cache[static_cast<size_t>(k)][static_cast<size_t>(total)];
    if (!slot.has_value())
    {
        kn_encode_tables built;
        const int        p = total - k;
        built.encode_matrix.assign(static_cast<size_t>(total) * static_cast<size_t>(k), 0);
        built.g_tbls.assign(static_cast<size_t>(k) * static_cast<size_t>(std::max(p, 1)) * 32, 0);
        gf_gen_cauchy1_matrix(built.encode_matrix.data(), total, k);
        if (p > 0)
        {
            ec_init_tables_base(k, p, built.encode_matrix.data() + k * k, built.g_tbls.data());
        }
        slot = std::move(built);
    }
    return *slot;
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

bfc::shared_sized_buffer alloc_decode_row(size_t len)
{
#if defined(CONFIG_WINJECT_FEC_RX_PSRAM) && CONFIG_WINJECT_FEC_RX_PSRAM
    auto* bytes = static_cast<std::byte*>(
        heap_caps_aligned_alloc(k_body_align, len, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
    if (bytes == nullptr)
    {
        throw std::bad_alloc();
    }
    return bfc::shared_sized_buffer::adopt(bytes, len, len, [](std::byte* p) { heap_caps_free(p); });
#else
    auto storage = bfc::shared_sized_buffer::allocate(len + k_body_align - 1);
    storage.resize(len + k_body_align - 1);
    const size_t off =
        (k_body_align - reinterpret_cast<uintptr_t>(storage.u8()) % k_body_align) % k_body_align;
    return storage.subview(off, len);
#endif
}

void init_decode_tables(int k, int rows, uint8_t* matrix, uint8_t* g_tbls)
{
    ec_init_tables_base(k, rows, matrix, g_tbls);
}

}  // namespace winject::fec::detail
