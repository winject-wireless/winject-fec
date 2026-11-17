/* Internal: GF tables, the encode kernel selection and buffer helpers shared by encoder and
 * decoder. Not installed. */
#ifndef WINJECT_FEC_SRC_RS_COMMON_HPP
#define WINJECT_FEC_SRC_RS_COMMON_HPP

#include <cstddef>
#include <cstdint>
#include <vector>

#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/rs_kernel.hpp"
#include "winject_fec/wire.hpp"

#ifdef WINJECT_FEC_ISAL_NEON
extern "C" void winject_fec_ec_encode_data(int len, int k, int rows, unsigned char* g_tbls,
                                           unsigned char** data, unsigned char** coding);
#define WINJECT_FEC_EC_ENCODE winject_fec_ec_encode_data
#else
#define WINJECT_FEC_EC_ENCODE winject::fec::rs_encode_scalar
#endif

namespace winject::fec::detail
{

constexpr size_t k_min_shard = 16;  // ISA-L NEON kernels need >= 16 bytes.
constexpr size_t k_body_align = 16;
constexpr int    k_max_k = k_header_k_n_max;
constexpr int    k_max_rows = 2 * k_header_k_n_max; /* data + parity */

inline void store_be16(uint8_t* p, uint16_t v)
{
    p[0] = static_cast<uint8_t>(v >> 8);
    p[1] = static_cast<uint8_t>(v & 0xFF);
}

inline uint16_t load_be16(const uint8_t* p)
{
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}

inline size_t align_shard(size_t row)
{
    return row < k_min_shard ? k_min_shard : row;
}

/* Cauchy matrix of a block with k data rows and k + p rows in all; parity row j is row k + j. */
struct kn_encode_tables
{
    std::vector<uint8_t> encode_matrix;
    std::vector<uint8_t> g_tbls;
};

/* Built once per (k, total) and never moved, so callers may keep the reference. */
const kn_encode_tables& kn_encode_tables_for(int k, int total);

/* Inverts the first k surviving rows. err_list[] is in row order, so its missing data rows come
 * first; decode_matrix gets one row for each of those (missing parity is never rebuilt). */
int gen_decode_matrix(int k, int total, const uint8_t* encode_matrix, const uint8_t* err_list,
                      int nerrs, uint8_t* decode_matrix, uint8_t* decode_index);

/* A 16-byte aligned decode row; in PSRAM where configured, to keep internal RAM for the
 * network and USB drivers. */
/* len bytes, 16-byte aligned; in PSRAM when psram and the target has it (else the heap). */
bfc::shared_sized_buffer alloc_aligned(size_t len, bool psram);
bfc::shared_sized_buffer alloc_decode_row(size_t len);
/* The encoder's row copies (CONFIG_WINJECT_FEC_TX_PSRAM: PSRAM). */
bfc::shared_sized_buffer alloc_encode_rows(size_t len);

void init_decode_tables(int k, int rows, uint8_t* matrix, uint8_t* g_tbls);

}  // namespace winject::fec::detail

#endif  // WINJECT_FEC_SRC_RS_COMMON_HPP
