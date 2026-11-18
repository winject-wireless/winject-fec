#ifndef WINJECT_FEC_WIRE_HPP
#define WINJECT_FEC_WIRE_HPP

#include <stddef.h>
#include <stdint.h>

/*
 * Streaming Reed-Solomon erasure FEC shard wire (bits MSB-first):
 *
 *   data shard:    u8  flag = is_data(1)=1 | spare(7)=0
 *                  u16 sequence_number (BE): one counter over data shards only
 *                  data (its length is the datagram's)
 *   parity shard:  u16 flag (BE) = is_data(1)=0 | n(5) | k_total(5) | k_i(5)
 *                  u16 sequence_number (BE): the block's first data shard
 *                  parity
 *
 * n is the block's parity count, k_total its data count, k_i this parity's row (0..n-1). The
 * block covers data [sequence_number, sequence_number + k_total).
 *
 * Parity is computed over rows of [u16 BE data length | data | zero pad] padded to the block's
 * longest row, so a recovered row carries its own length; the length prefix is never sent with
 * a data shard. Parity rows start at that length prefix.
 */
namespace winject::fec
{

inline constexpr size_t   k_data_header_len = 3;
inline constexpr size_t   k_parity_header_len = 4;
inline constexpr size_t   k_len_prefix = 2;
inline constexpr uint8_t  k_flag_is_data = 0x80U;
inline constexpr uint8_t  k_data_flag_spare = 0x7FU;
inline constexpr uint16_t k_parity_flag_is_data = 0x8000U;
inline constexpr int      k_header_k_n_min = 1;
inline constexpr int      k_header_k_n_max = 31;
/* Longest parity spread a sender may use; the receiver assumes it until it has measured one. */
inline constexpr int k_spread_cap_ms = 250;

/* LC body type in front of every FEC stream datagram: plain carries the payload as is, fec a
 * shard of this wire. */
enum class lc_type : uint8_t
{
    plain = 0,
    fec = 1,
};

struct shard_header
{
    bool     is_data = false;
    uint16_t sequence_number = 0;
    int      n = 0;       /* parity only: parity count */
    int      k_total = 0; /* parity only: data count */
    int      k_i = 0;     /* parity only: row */
    size_t   header_len = 0;
};

void write_data_header(uint8_t* out, uint16_t sequence_number);
bool write_parity_header(uint8_t* out, uint16_t sequence_number, int n, int k_total, int k_i);
bool parse_header(const uint8_t* data, size_t len, shard_header* out);

/* Largest app datagram an encoder takes for a wire datagram limit (parity is the largest
 * shard: header and coded length prefix before a body as long as the longest data). */
[[nodiscard]] size_t max_app_payload(size_t max_datagram);
/* Largest data payload a receiver must accept for this datagram limit. */
[[nodiscard]] size_t max_data_payload(size_t max_datagram);

}  // namespace winject::fec

#endif  // WINJECT_FEC_WIRE_HPP
