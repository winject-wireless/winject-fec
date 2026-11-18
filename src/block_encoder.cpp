#include "winject_fec/block_encoder.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <utility>

#include "rs_common.hpp"

namespace winject::fec
{

using detail::align_shard;
using detail::k_body_align;
using detail::k_max_k;
using detail::k_min_shard;
using detail::kn_encode_tables_for;
using detail::store_be16;

block_encoder::block_encoder() = default;
block_encoder::~block_encoder() = default;
block_encoder::block_encoder(block_encoder&&) noexcept = default;
block_encoder& block_encoder::operator=(block_encoder&&) noexcept = default;

const char* block_encoder::impl_name() const
{
#ifdef WINJECT_FEC_ISAL_NEON
    return "isa-l neon";
#else
    return rs_encode_scalar_impl();
#endif
}

bool block_encoder::valid(int k, int n, size_t max_datagram)
{
    return k >= k_header_k_n_min && k <= k_header_k_n_max && n >= k &&
           n - k <= k_header_k_n_max &&
           max_datagram >= k_parity_header_len + k_len_prefix + k_min_shard;
}

void block_encoder::set_tx_buffers(size_t headroom, shard_alloc alloc_fn)
{
    tx_headroom = headroom;
    tx_alloc = std::move(alloc_fn);
}

uint8_t* block_encoder::row(int i) const
{
    return rows_mem.u8() + static_cast<size_t>(i) * row_stride;
}

bool block_encoder::start(uint16_t base, int k, int n, size_t max_datagram, time_point now)
{
    if (open || !queue.empty() || !valid(k, n, max_datagram))
    {
        return false;
    }
    cfg_k = k;
    cfg_n = n;
    p = n - k;
    max_datagram_ = max_datagram;
    block_base = base;
    block_start_ = now;
    open_rows = 0;
    open = true;
    if (p > 0)
    {
        const size_t body_max = align_shard(max_original(max_datagram_));
        const size_t stride = (body_max + k_body_align - 1) / k_body_align * k_body_align;
        const size_t need = stride * static_cast<size_t>(cfg_k);
        if (rows_mem.empty() || rows_mem.size() < need)
        {
            rows_mem = detail::alloc_encode_rows(need);
        }
        row_stride = stride;
    }
    return true;
}

block_encoder::state block_encoder::get_state() const
{
    return queue.empty() ? state {} : queue.front().st;
}

bfc::shared_sized_buffer block_encoder::pop()
{
    if (queue.empty())
    {
        return {};
    }
    bfc::shared_sized_buffer buf = std::move(queue.front().buf);
    queue.pop_front();
    return buf;
}

void block_encoder::push(bfc::shared_sized_buffer datagram, time_point now)
{
    if (!open)
    {
        drop_not_open_++;
        return;
    }
    if (!queue.empty())
    {
        drop_queued_++;
        return;
    }
    const size_t reserve = push_offset();
    if (datagram.size() < reserve)
    {
        drop_no_headroom_++;
        return;
    }
    const size_t len = datagram.size() - reserve;
    if (0 == len || len > max_original(max_datagram_))
    {
        drop_size_++;
        return;
    }
    uint8_t* const hdr = datagram.u8() + tx_headroom;
    write_data_header(hdr, static_cast<uint16_t>(block_base + open_rows));
    if (p > 0)
    {
        memcpy(row(open_rows), hdr + k_data_header_len, len);
        row_len[open_rows] = len;
    }
    open_rows++;
    state st;
    st.type = shard_type::data;
    queue.push_back({std::move(datagram), st});
    if (open_rows >= cfg_k)
    {
        encode_block(now);
    }
}

void block_encoder::close(time_point now)
{
    if (!open)
    {
        return;
    }
    if (0 == open_rows)
    {
        open = false; /* nothing pushed: no block on the wire */
        return;
    }
    encode_block(now);
}

/* A parity buffer: tx_headroom, then header + length prefix, then a 16-byte aligned body. */
bfc::shared_sized_buffer block_encoder::alloc_parity(size_t body_len)
{
    const size_t             hdr = k_parity_header_len + k_len_prefix;
    const size_t             cap = k_body_align - 1 + tx_headroom + hdr + body_len;
    bfc::shared_sized_buffer buf;
    if (tx_alloc)
    {
        buf = tx_alloc(cap);
    }
    else
    {
        buf = bfc::shared_sized_buffer::allocate(cap);
        buf.resize(cap);
    }
    if (buf.size() < cap)
    {
        return {};
    }
    const auto   body = reinterpret_cast<uintptr_t>(buf.u8()) + tx_headroom + hdr;
    const size_t pad = (k_body_align - body % k_body_align) % k_body_align;
    /* [tx_headroom][header][body], like a data shard; the alignment pad stays outside. */
    return buf.subview(pad, tx_headroom + hdr + body_len);
}

/* Closes the block: parity over the rows, queued in row order. Two kernel passes: the data bodies
 * (zero padded to the longest) and the 2-byte length rows; parity is [length parity | data
 * parity]. */
void block_encoder::encode_block(time_point now)
{
    open = false;
    const int k_total = open_rows;
    open_rows = 0;
    if (0 == p)
    {
        return; /* data only */
    }
    size_t max_len = 0;
    for (int i = 0; i < k_total; i++)
    {
        max_len = std::max(max_len, row_len[i]);
    }
    const size_t shard_len = align_shard(max_len);
    /* The cache never moves an entry, so the reference stays valid. */
    unsigned char* const g_tbls =
        const_cast<unsigned char*>(kn_encode_tables_for(k_total, k_total + p).g_tbls.data());

    alignas(16) uint8_t len_rows[k_max_k][k_min_shard] {};
    alignas(16) uint8_t len_parity[k_max_k][k_min_shard] {};
    std::array<unsigned char*, k_max_k> data_ptrs {};
    std::array<unsigned char*, k_max_k> len_ptrs {};
    std::array<unsigned char*, k_max_k> coding_ptrs {};
    std::array<unsigned char*, k_max_k> len_coding_ptrs {};
    for (int i = 0; i < k_total; i++)
    {
        uint8_t* r = row(i);
        memset(r + row_len[i], 0, shard_len - row_len[i]);
        data_ptrs[static_cast<size_t>(i)] = r;
        store_be16(len_rows[i], static_cast<uint16_t>(row_len[i]));
        len_ptrs[static_cast<size_t>(i)] = len_rows[i];
    }
    std::array<bfc::shared_sized_buffer, k_max_k> parity {};
    for (int j = 0; j < p; j++)
    {
        parity[static_cast<size_t>(j)] = alloc_parity(shard_len);
        if (parity[static_cast<size_t>(j)].empty())
        {
            drop_parity_alloc_++;
            return;
        }
        coding_ptrs[static_cast<size_t>(j)] =
            parity[static_cast<size_t>(j)].u8() + tx_headroom + k_parity_header_len + k_len_prefix;
        len_coding_ptrs[static_cast<size_t>(j)] = len_parity[j];
    }
    const auto kernel_t0 = std::chrono::steady_clock::now();
    WINJECT_FEC_EC_ENCODE(static_cast<int>(shard_len), k_total, p, g_tbls, data_ptrs.data(),
                          coding_ptrs.data());
    WINJECT_FEC_EC_ENCODE(static_cast<int>(k_min_shard), k_total, p, g_tbls, len_ptrs.data(),
                          len_coding_ptrs.data());
    encode_kernel_us_ += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                              kernel_t0)
            .count());

    for (int j = 0; j < p; j++)
    {
        bfc::shared_sized_buffer& buf = parity[static_cast<size_t>(j)];
        uint8_t* const           hdr = buf.u8() + tx_headroom;
        (void)write_parity_header(hdr, block_base, p, k_total, j);
        memcpy(hdr + k_parity_header_len, len_parity[j], k_len_prefix);
        state st;
        st.type = shard_type::parity;
        st.parity.index = j;
        st.parity.count = p;
        st.parity.block_k = k_total;
        st.parity.block_start = block_start_;
        st.parity.block_close = now;
        queue.push_back({std::move(buf), st});
    }
    blocks_count++;
}

}  // namespace winject::fec
