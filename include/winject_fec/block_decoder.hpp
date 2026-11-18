#ifndef WINJECT_FEC_BLOCK_DECODER_HPP
#define WINJECT_FEC_BLOCK_DECODER_HPP

#include <array>
#include <cstdint>
#include <deque>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include <bfc/shared_sized_buffer.hpp>

#include "winject_fec/wire.hpp"

namespace winject::fec
{

/*
 * One block's rebuild: start() it from a parity header, push() the rows it has (data payloads and
 * parity bodies, as views of the received buffers: nothing is copied in), decode() once enough
 * rows are in, then pop() the rebuilt data rows. reset() drops every row it holds, ready for the
 * next block.
 *
 *   dec.start(base, k_total, n);
 *   dec.push_parity(row, body); dec.push_data(row, payload); ...
 *   if (dec.decodable() && dec.decode())
 *       while (dec.get_state().ready)
 *           deliver(dec.get_state().seq, dec.pop());
 */
class block_decoder
{
public:
    void start(uint16_t base, int k_total, int n);
    /* Drops the rows and anything not popped; the block is gone. */
    void reset();
    /* Drops the rows but keeps the block (and anything not popped). */
    void release_rows();

    [[nodiscard]] bool started() const
    {
        return k_total_ > 0;
    }
    [[nodiscard]] uint16_t base() const
    {
        return base_;
    }
    [[nodiscard]] int k_total() const
    {
        return k_total_;
    }
    [[nodiscard]] int n() const
    {
        return n_;
    }

    /* A data row: its payload (no header). False out of range or already in. */
    bool push_data(int row, bfc::shared_sized_buffer payload);
    /* A parity row: its body after the parity header. False out of range or already in. */
    bool push_parity(int row, bfc::shared_sized_buffer body);
    [[nodiscard]] bool has_data(int row) const;
    [[nodiscard]] bool has_parity(int row) const;
    [[nodiscard]] int  data_count() const
    {
        return data_have;
    }
    [[nodiscard]] int parity_count() const
    {
        return parity_have;
    }
    [[nodiscard]] bool complete() const
    {
        return started() && data_have == k_total_;
    }
    [[nodiscard]] bool decodable() const
    {
        return started() && data_have < k_total_ && data_have + parity_have >= k_total_;
    }

    /* Rebuilds the missing data rows and queues them (row order); drops the rows it held. False
     * when not decodable or the rows do not decode (inconsistent lengths). */
    bool decode();

    struct state
    {
        bool     ready = false; /* a rebuilt row is queued */
        int      row = 0;
        uint16_t seq = 0;
    };
    [[nodiscard]] state      get_state() const;
    bfc::shared_sized_buffer pop();

    /*
     * Decodes one block. data: k_total rows (empty = missing; else the data payload); parity: n
     * rows (empty = missing; else the body after its header). Fills the missing data rows with
     * their payloads. False if fewer than k_total rows are present or they are inconsistent.
     */
    static bool decode_rows(int k_total, int n, std::vector<bfc::shared_sized_buffer>* data,
                            const std::vector<bfc::shared_sized_buffer>& parity, int* recovered);

private:
    uint16_t base_ = 0;
    int      k_total_ = 0;
    int      n_ = 0;
    int      data_have = 0;
    int      parity_have = 0;
    std::array<bfc::shared_sized_buffer, k_header_k_n_max> data {};
    std::array<bfc::shared_sized_buffer, k_header_k_n_max> parity {};
    struct rebuilt
    {
        int                      row;
        bfc::shared_sized_buffer payload;
    };
    std::deque<rebuilt> out;
};

}  // namespace winject::fec

#endif  // WINJECT_FEC_BLOCK_DECODER_HPP
