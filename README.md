# winject-fec

Streaming Reed–Solomon erasure FEC shared by winject-l3 and vstreamer: a shard wire, an
encoder, a decoder, and the GF(2^8) kernels behind them (ISA-L, ISA-L NEON on aarch64, a
portable table path, and the ESP32-P4 PIE SIMD kernel).

Extracted from vstreamer `src/core/rs_block_erasure*`, `rs_encode_scalar*` and `fec_spread.hpp`
at vstreamer commit `6089e8a`, with the same wire. Changes: the single TX+RX class is split
into `encoder` and `decoder`; the encoder is a zero-copy push/pop FIFO that starts in
bypass (no FEC) and is set up with `configure()`; and a k/n change mid-block releases the partial block's parity instead of
dropping it.

## Wire

```
data shard:    u8  flag = is_data(1)=1 | spare(7)=0
               u16 sequence_number (BE)   one counter over data shards only
               data                       length = datagram length - 3
parity shard:  u16 flag (BE) = is_data(1)=0 | n(5) | k_total(5) | k_i(5)
               u16 sequence_number (BE)   the block's first data shard
               parity                     over rows [u16 BE len | data | zero pad]
```

`n` is the block's parity count, `k_total` its data count (both 1..31), `k_i` the parity row.
A block covers data `[sequence_number, sequence_number + k_total)`. See `include/winject_fec/wire.hpp`.

## API

- `winject::fec::encoder` is a FIFO: `push(datagram)`, then `pop()` until `get_state()` says
  `none` (`plain`, `data` or `parity` otherwise; for parity also its row, the block's k and the
  block start for `spread_offset()`). Every push queues exactly one plain or data shard, and a
  push that closes a block queues its parity behind it.
  - Zero copy: the caller receives into a buffer with room in front and every layer writes its
    header into that room in place; the encoder hands the pushed buffer back. A pushed buffer
    is `[headroom][fec slot][data]` (`push_offset()` = headroom + 3), headroom being
    `set_tx_buffers()`' room kept in front of every popped shard for the next layers:
    ```
    recv       [headroom ][fec slot][data]
    fec        [headroom ][fec     ][data]
    upstream   [   ][up  ][fec     ][data]
    tx mux     [80211][up][fec     ][data]
    ```
    The slot is reserved in every mode, so where the data lands does not depend on FEC being
    on. A mux that packs several PDUs into one frame copies them; a single PDU goes out in place.
  - Bypass (the default): the slot is cut off the front and the rest, `[headroom][data]`, goes
    out untouched (`plain`, the LC's plain type).
  - `configure(k, n, max_datagram)` turns FEC on (data header into the slot) or changes k/n at
    any time: a partial block is closed under its old parameters and its parity queued; the next
    block uses the new ones. `configure_bypass()` queues it the same way and goes back to
    bypass. `flush()` closes a short block.
  - A push is dropped and counted, never undefined: shards still queued (`drop_queued`),
    shorter than `push_offset()` (`drop_no_headroom`), no data or over `max_original()`
    (`drop_size`). A block whose parity buffers cannot be allocated loses its parity
    (`drop_parity_alloc`).
  - Parity is computed from a copy of each data body the encoder keeps (k rows of
    `max_original()`), so a popped data shard is the caller's at once. Parity shards are new
    buffers from `set_tx_buffers()`, `[headroom][parity header][parity]`.
- `winject::fec::decoder`: `push_air()` takes a shard, and `poll_rx()` releases what deadlines
  have freed. Output is in sequence: data goes out as soon as it is in order, holes are rebuilt
  from parity or given up at a deadline learnt from the parity spread. Counters cover recovered
  data, failed and given-up blocks, missing shards, late data and new sender sessions.
- Neither class runs timers or threads. The caller drives `poll_rx()` and decides when parity
  goes on air.

Shard buffers are `bfc::shared_sized_buffer`.

## Build

Host (tests included when built on its own):

```
cmake -S . -B build && cmake --build build -j && ./build/tests/winject_fec_tests
```

As a dependency: `add_subdirectory()` / FetchContent, then link `winject::fec`. If the parent
already uses bfc, set `BFC_PIN` and `BFC_INCLUDE_DIRS` first. The pin must match
`cmake/bfc.cmake`. ISA-L is pinned in `cmake/isal_ec.cmake`.

ESP-IDF: add `idf/winject_fec` to `EXTRA_COMPONENT_DIRS`. The project needs
`CONFIG_COMPILER_CXX_EXCEPTIONS` (bfc throws). Kconfig:

- `WINJECT_FEC_RS_PIE` (default y on the P4): PIE parity kernel, about 2.8x the table path.
- `WINJECT_FEC_RX_PSRAM`: decode rows and recovered payloads in PSRAM.

`WINJECT_FEC_RX_DEADLINE_MARGIN_MIN_MS` (5..100, default 5) raises the decoder's deadline
margin floor on platforms that can hold a queued parity shard longer.
