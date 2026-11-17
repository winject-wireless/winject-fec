#include "winject_fec/encoder.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <random>
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

encoder::encoder()
{
    // Random start id: a restarted sender must not replay ids that the receiver's in-order
    // release still remembers.
    std::random_device rd;
    tx_seq = static_cast<uint16_t>(rd());
}

encoder::~encoder() = default;
encoder::encoder(encoder&&) noexcept = default;
encoder& encoder::operator=(encoder&&) noexcept = default;

const char* encoder::impl_name() const
{
#ifdef WINJECT_FEC_ISAL_NEON
    return "isa-l neon";
#else
    return rs_encode_scalar_impl();
#endif
}

bool encoder::configure(int k, int n, size_t max_datagram)
{
    if (k < k_header_k_n_min || k > k_header_k_n_max || n < k || n - k > k_header_k_n_max ||
        max_datagram < k_parity_header_len + k_len_prefix + k_min_shard)
    {
        return false;
    }
    if (active && k == cfg_k && n == cfg_n && max_datagram == max_datagram_)
    {
        return true; /* nothing changes: the open block stays open */
    }
    /* Release the partial block under the parameters it was started with. */
    flush();
    cfg_k = k;
    cfg_n = n;
    p = n - k;
    max_datagram_ = max_datagram;
    full_g_tbls = kn_encode_tables_for(cfg_k, cfg_n).g_tbls.data();
    active = true;

    /* Rows for the open block's data copies (parity only). */
    rows_mem = {};
    row_stride = 0;
    if (p > 0)
    {
        const size_t body_max = align_shard(max_original());
        row_stride = (body_max + k_body_align - 1) / k_body_align * k_body_align;
        rows_mem = detail::alloc_encode_rows(row_stride * static_cast<size_t>(cfg_k));
    }
    return true;
}

void encoder::configure_bypass()
{
    flush();
    active = false;
    open_rows = 0;
    max_datagram_ = 0;
    cfg_k = 0;
    cfg_n = 0;
    p = 0;
    full_g_tbls = nullptr;
    rows_mem = {};
    row_stride = 0;
}

void encoder::set_tx_buffers(size_t headroom, shard_alloc alloc_fn)
{
    tx_headroom = headroom;
    tx_alloc = std::move(alloc_fn);
}

uint8_t* encoder::row(int i) const
{
    return rows_mem.u8() + static_cast<size_t>(i) * row_stride;
}

encoder::state encoder::get_state() const
{
    return queue.empty() ? state {} : queue.front().st;
}

bfc::shared_sized_buffer encoder::pop()
{
    if (queue.empty())
    {
        return {};
    }
    bfc::shared_sized_buffer buf = std::move(queue.front().buf);
    queue.pop_front();
    return buf;
}

void encoder::push(bfc::shared_sized_buffer datagram)
{
    if (!queue.empty())
    {
        drop_queued_++;
        return;
    }
    /* [tx_headroom][fec header slot][payload]: the caller reserved the slot in every mode, so
     * where the payload starts does not depend on whether FEC is on. */
    const size_t reserve = push_offset();
    if (datagram.size() < reserve)
    {
        drop_no_headroom_++;
        return;
    }
    const size_t len = datagram.size() - reserve;
    if (0 == len || len > max_original())
    {
        drop_size_++;
        return;
    }
    if (!active)
    {
        /* Plain: the same buffer, the unused header slot cut off the front. */
        state st;
        st.type = shard_type::plain;
        queue.push_back({datagram.subview(k_data_header_len, datagram.size() - k_data_header_len),
                         st});
        return;
    }
    uint8_t* const hdr = datagram.u8() + tx_headroom;
    write_data_header(hdr, tx_seq);
    if (p > 0)
    {
        if (0 == open_rows)
        {
            block_base = tx_seq;
            block_start_ = clock ? clock() : std::chrono::steady_clock::now();
        }
        memcpy(row(open_rows), hdr + k_data_header_len, len);
        row_len[open_rows] = len;
        open_rows++;
    }
    tx_seq++;
    state st;
    st.type = shard_type::data;
    queue.push_back({std::move(datagram), st});
    if (p > 0 && open_rows >= cfg_k)
    {
        (void)encode_block();
    }
}

void encoder::flush()
{
    if (!active || 0 == open_rows)
    {
        return;
    }
    (void)encode_block();
}

/* A parity buffer: tx_headroom, then header + length prefix, then a 16-byte aligned body. */
bfc::shared_sized_buffer encoder::alloc_parity(size_t body_len)
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

/* Parity over the open rows, queued in row order. Two kernel passes: the data bodies (zero padded
 * to the longest) and the 2-byte length rows; parity is [length parity | data parity]. */
bool encoder::encode_block()
{
    const int k_total = open_rows;
    open_rows = 0;
    size_t max_len = 0;
    for (int i = 0; i < k_total; i++)
    {
        max_len = std::max(max_len, row_len[i]);
    }
    const size_t shard_len = align_shard(max_len);
    /* The cache never moves an entry, so the full block's tables can be kept from configure(). */
    unsigned char* const g_tbls = const_cast<unsigned char*>(
        k_total == cfg_k && nullptr != full_g_tbls
            ? full_g_tbls
            : kn_encode_tables_for(k_total, k_total + p).g_tbls.data());

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
            return false;
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
        st.parity_index = j;
        st.parity_count = p;
        st.block_k = k_total;
        st.block_start = block_start_;
        queue.push_back({std::move(buf), st});
    }
    blocks_count++;
    return true;
}

}  // namespace winject::fec
