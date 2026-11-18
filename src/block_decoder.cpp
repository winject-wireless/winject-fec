/* One block's rebuild from the rows it was given. */
#include "winject_fec/block_decoder.hpp"

#include <array>
#include <cstring>
#include <utility>
#include <vector>

#include "rs_common.hpp"

namespace winject::fec
{

using namespace detail;

void block_decoder::start(uint16_t base, int k_total, int n)
{
    reset();
    base_ = base;
    k_total_ = k_total;
    n_ = n;
}

void block_decoder::release_rows()
{
    for (int i = 0; i < k_header_k_n_max; i++)
    {
        data[static_cast<size_t>(i)] = {};
        parity[static_cast<size_t>(i)] = {};
    }
    data_have = 0;
    parity_have = 0;
}

void block_decoder::reset()
{
    release_rows();
    out.clear();
    base_ = 0;
    k_total_ = 0;
    n_ = 0;
}

bool block_decoder::has_data(int row) const
{
    return row >= 0 && row < k_total_ && !data[static_cast<size_t>(row)].empty();
}

bool block_decoder::has_parity(int row) const
{
    return row >= 0 && row < n_ && !parity[static_cast<size_t>(row)].empty();
}

bool block_decoder::push_data(int row, bfc::shared_sized_buffer payload)
{
    if (row < 0 || row >= k_total_ || payload.empty() || has_data(row))
    {
        return false;
    }
    data[static_cast<size_t>(row)] = std::move(payload);
    data_have++;
    return true;
}

bool block_decoder::push_parity(int row, bfc::shared_sized_buffer body)
{
    if (row < 0 || row >= n_ || body.empty() || has_parity(row))
    {
        return false;
    }
    parity[static_cast<size_t>(row)] = std::move(body);
    parity_have++;
    return true;
}

bool block_decoder::decode()
{
    if (!decodable())
    {
        return false;
    }
    std::vector<bfc::shared_sized_buffer> d(data.begin(), data.begin() + k_total_);
    const std::vector<bfc::shared_sized_buffer> p(parity.begin(), parity.begin() + n_);
    int        rec = 0;
    const bool ok = decode_rows(k_total_, n_, &d, p, &rec);
    if (ok)
    {
        for (int i = 0; i < k_total_; i++)
        {
            if (data[static_cast<size_t>(i)].empty())
            {
                out.push_back({i, std::move(d[static_cast<size_t>(i)])});
            }
        }
    }
    release_rows();
    return ok;
}

block_decoder::state block_decoder::get_state() const
{
    state s;
    if (!out.empty())
    {
        s.ready = true;
        s.row = out.front().row;
        s.seq = static_cast<uint16_t>(base_ + out.front().row);
    }
    return s;
}

bfc::shared_sized_buffer block_decoder::pop()
{
    if (out.empty())
    {
        return {};
    }
    bfc::shared_sized_buffer b = std::move(out.front().payload);
    out.pop_front();
    return b;
}

bool block_decoder::decode_rows(int k_total, int n,
                               std::vector<bfc::shared_sized_buffer>* data,
                               const std::vector<bfc::shared_sized_buffer>& parity,
                               int* recovered)
{
    if (data == nullptr || k_total < 1 || k_total > k_header_k_n_max || n < 1 ||
        n > k_header_k_n_max || data->size() != static_cast<size_t>(k_total) ||
        parity.size() != static_cast<size_t>(n))
    {
        return false;
    }
    const int total = k_total + n;
    /* Row length: every parity body has it; data rows are [len | payload] zero-padded to it. */
    size_t row_len = 0;
    int    have = 0;
    for (const auto& body : parity)
    {
        if (!body.empty())
        {
            if (row_len != 0 && body.size() != row_len)
            {
                return false;
            }
            row_len = body.size();
            have++;
        }
    }
    int missing_data = 0;
    for (const auto& payload : *data)
    {
        if (payload.empty())
        {
            missing_data++;
        }
        else
        {
            have++;
        }
    }
    if (recovered != nullptr)
    {
        *recovered = 0;
    }
    if (0 == missing_data)
    {
        return true;
    }
    if (have < k_total || row_len < k_len_prefix + k_min_shard)
    {
        return false;
    }
    for (const auto& payload : *data)
    {
        if (!payload.empty() && k_len_prefix + payload.size() > row_len)
        {
            return false;
        }
    }

    const kn_encode_tables&         tables = kn_encode_tables_for(k_total, total);
    std::array<uint8_t, k_max_rows> err_list {};
    int                             nerrs = 0;
    for (int i = 0; i < total; i++)
    {
        const bool missing = i < k_total ? (*data)[static_cast<size_t>(i)].empty()
                                         : parity[static_cast<size_t>(i - k_total)].empty();
        if (missing)
        {
            err_list[static_cast<size_t>(nerrs++)] = static_cast<uint8_t>(i);
        }
    }
    /* Missing data rows are the head of err_list (row order). */
    std::vector<uint8_t> decode_matrix(static_cast<size_t>(missing_data) *
                                       static_cast<size_t>(k_total));
    std::array<uint8_t, k_max_rows> decode_index {};
    if (gen_decode_matrix(k_total, total, tables.encode_matrix.data(), err_list.data(), nerrs,
                          decode_matrix.data(), decode_index.data()) != 0)
    {
        return false;
    }
    /* Only the k source rows the decoder picked are read. A parity body already is a row; a data
     * row is built as [u16 BE length | payload | zero pad]. */
    std::array<bfc::shared_sized_buffer, k_max_k> data_rows {};
    std::array<unsigned char*, k_max_rows>        src_ptrs {};
    for (int i = 0; i < k_total; i++)
    {
        const int idx = decode_index[static_cast<size_t>(i)];
        if (idx >= k_total)
        {
            src_ptrs[static_cast<size_t>(i)] =
                const_cast<unsigned char*>(parity[static_cast<size_t>(idx - k_total)].u8());
            continue;
        }
        const bfc::shared_sized_buffer& src = (*data)[static_cast<size_t>(idx)];
        bfc::shared_sized_buffer&       row = data_rows[static_cast<size_t>(idx)];
        row = alloc_decode_row(row_len);
        store_be16(row.u8(), static_cast<uint16_t>(src.size()));
        memcpy(row.u8() + k_len_prefix, src.u8(), src.size());
        memset(row.u8() + k_len_prefix + src.size(), 0, row_len - k_len_prefix - src.size());
        src_ptrs[static_cast<size_t>(i)] = row.u8();
    }
    std::vector<uint8_t> decode_tbls(static_cast<size_t>(k_total) *
                                     static_cast<size_t>(missing_data) * 32);
    std::array<bfc::shared_sized_buffer, k_max_k> out_rows {};
    std::array<unsigned char*, k_max_rows>        out_ptrs {};
    for (int w = 0; w < missing_data; w++)
    {
        out_rows[static_cast<size_t>(w)] = alloc_decode_row(row_len);
        out_ptrs[static_cast<size_t>(w)] = out_rows[static_cast<size_t>(w)].u8();
    }
    init_decode_tables(k_total, missing_data, decode_matrix.data(), decode_tbls.data());
    WINJECT_FEC_EC_ENCODE(static_cast<int>(row_len), k_total, missing_data, decode_tbls.data(),
                        src_ptrs.data(), out_ptrs.data());
    for (int w = 0; w < missing_data; w++)
    {
        const bfc::shared_sized_buffer& row = out_rows[static_cast<size_t>(w)];
        const uint16_t                  len = load_be16(row.u8());
        if (0 == len || k_len_prefix + len > row_len)
        {
            return false;
        }
    }
    for (int w = 0; w < missing_data; w++)
    {
        (*data)[err_list[static_cast<size_t>(w)]] =
            out_rows[static_cast<size_t>(w)].subview(k_len_prefix,
                                      load_be16(out_rows[static_cast<size_t>(w)].u8()));
    }
    if (recovered != nullptr)
    {
        *recovered = missing_data;
    }
    return true;
}

}  // namespace winject::fec
