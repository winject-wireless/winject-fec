#include "winject_fec/wire.hpp"

#include "rs_common.hpp"

namespace winject::fec
{

using detail::k_min_shard;
using detail::load_be16;
using detail::store_be16;

void write_data_header(uint8_t* out, uint16_t sequence_number)
{
    out[0] = k_flag_is_data;
    store_be16(out + 1, sequence_number);
}

bool write_parity_header(uint8_t* out, uint16_t sequence_number, int n, int k_total, int k_i)
{
    if (out == nullptr || n < k_header_k_n_min || n > k_header_k_n_max ||
        k_total < k_header_k_n_min || k_total > k_header_k_n_max || k_i < 0 || k_i >= n)
    {
        return false;
    }
    const uint16_t flag = static_cast<uint16_t>(((static_cast<uint16_t>(n) & 0x1FU) << 10) |
                                                ((static_cast<uint16_t>(k_total) & 0x1FU) << 5) |
                                                (static_cast<uint16_t>(k_i) & 0x1FU));
    store_be16(out, flag);
    store_be16(out + 2, sequence_number);
    return true;
}

bool parse_header(const uint8_t* data, size_t len, shard_header* out)
{
    if (data == nullptr || out == nullptr || len == 0)
    {
        return false;
    }
    shard_header h;
    if (0 != (data[0] & k_flag_is_data))
    {
        if (len <= k_data_header_len || 0 != (data[0] & k_data_flag_spare))
        {
            return false;
        }
        h.is_data = true;
        h.sequence_number = load_be16(data + 1);
        h.header_len = k_data_header_len;
        *out = h;
        return true;
    }
    if (len <= k_parity_header_len + k_len_prefix)
    {
        return false;
    }
    const uint16_t flag = load_be16(data);
    h.n = static_cast<int>((flag >> 10) & 0x1F);
    h.k_total = static_cast<int>((flag >> 5) & 0x1F);
    h.k_i = static_cast<int>(flag & 0x1F);
    if (h.n < k_header_k_n_min || h.k_total < k_header_k_n_min || h.k_i >= h.n)
    {
        return false;
    }
    h.sequence_number = load_be16(data + 2);
    h.header_len = k_parity_header_len;
    *out = h;
    return true;
}

size_t max_app_payload(size_t max_datagram)
{
    const size_t overhead = k_parity_header_len + k_len_prefix;
    return max_datagram > overhead ? max_datagram - overhead : 0;
}

size_t max_data_payload(size_t max_datagram)
{
    return max_datagram > k_data_header_len ? max_datagram - k_data_header_len : 0;
}

}  // namespace winject::fec
