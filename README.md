# winject-fec

Streaming Reed–Solomon erasure FEC shared by winject-l3 and vstreamer: a shard wire, per-block
codecs, a write and a read stream that run them on a bfc reactor, and the GF(2^8) kernels behind
them (ISA-L, ISA-L NEON on aarch64, a portable table path, and the ESP32-P4 PIE SIMD kernel).

Extracted from vstreamer `src/core/rs_block_erasure*`, `rs_encode_scalar*` and `fec_spread.hpp`
at vstreamer commit `6089e8a`, with the same shard wire and parity math. Changes: the single
TX+RX class is replaced by `block_encoder` / `block_decoder` (one block each, zero-copy push/pop)
and `fec_write_stream` / `fec_read_stream`, which own the timing (parity spread, flush, give-up)
on one reactor timer each; and a k/n change applies from the next block.

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

Shard buffers are `bfc::shared_sized_buffer`. Every class hands buffers on without copying them.

### Zero copy

The sender receives each app datagram into a buffer with room in front, and every layer writes
its header into that room in place:

```
recv       [headroom ][fec slot][data]     pushed: push_offset() = headroom + 3
fec        [headroom ][fec     ][data]     the same buffer, popped
upstream   [   ][up  ][fec     ][data]
tx mux     [80211][up][fec     ][data]
```

`headroom` (`set_tx_buffers()`) is the room every shard keeps for the next layers. The 3-byte
slot is reserved in every mode, so where the data lands does not depend on FEC being on. A mux
that packs several PDUs into one frame copies them; a single PDU goes out in place. Without FEC
the slot is cut off the front and `[headroom][data]` goes out as LC plain. Parity shards are new
buffers from `set_tx_buffers()`' allocator (heap by default), `[headroom][parity header][parity]`
with a 16-byte aligned body.

### Streams

- `fec_write_stream`: `configure({k, n, max_datagram, spread_ms, flush_ms})`, `push(datagram)`,
  `pop()` an `out_shard {type, buf}` (`plain`, `data` or `parity`).
  - A datagram's shard is in the output at once; data never waits.
  - A block closes after k pushes, or `flush_ms` after its first push (`flush_ms` > 0 when the
    block has parity).
  - Parity waits for its place in the spread: row j of a block of c = k_total + parity shards
    goes at `max(close + spread_offset(j, c), start + spread_offset(k_total + j, c))`. A block's
    data and parity together span `spread_ms`, and parity never leaves in a lump. Blocks
    overlap, so their shards interleave.
  - `configure()` never touches the running block, which keeps its parameters until it closes.
    k = 0 (the default) is no FEC: pushes outside a block go out plain.
  - Counters: shards by type, blocks, flushed blocks, and drops (`drop_no_headroom`,
    `drop_size`, `drop_parity_alloc`, `drop_output`).
- `fec_read_stream`: `push(body, lc_type)` what the link received, `pop()` app datagrams.
  - Plain datagrams go out at once. Data goes out in sequence: data behind a hole waits until the
    hole is rebuilt from its block's parity or given up. Consumers such as an RTP depacketizer
    drop a packet that comes after a later one, so a rebuilt row must not overtake.
  - The stream keeps a reference to recent data shards until their blocks can no longer need
    them: memory, not copies.
  - A hole is given up when its block's last parity is overdue. The sender's spread, and the
    bias and jitter of parity arrivals, are learnt from the parity received; until two blocks are
    measured the wait is the 250 ms spread cap. Without parity (a data-only stream) a hole is
    given up as soon as later data shows the sender is past it.
  - Counters: recovered rows, rebuilt / given-up / failed / evicted blocks, missing data and
    parity, late (not released) and duplicate shards, new sender sessions, header errors.
- Both streams:
  - Threads: each runs on a `bfc::cv_reactor` (`attach(name)` looks it up in
    `bfc::cv_reactor_registry`), with one timer armed at its earliest deadline. Calls from other
    threads are posted onto the reactor.
  - Output: a `bfc::reactive_circular_lossy_queue` (`output()`). Only due shards enter it, and
    when the consumer does not drain it the oldest is dropped (`drop_output`). Bind it to the
    consumer's reactor (`output()->set_reactor(r)`, `r->add_read_rdy(*output(), cb)`) or poll
    `pop()`.
  - Without a reactor (manual mode) everything runs on the caller's thread, which calls `poll()`
    at `next_deadline()`.

### Block codecs

The streams use these, and they can be used on their own.

- `block_encoder`: `start(base, k, n, max_datagram, now)`, `push(datagram, now)`, then `pop()`
  until `get_state()` says `none`. The k-th push, or `close()`, encodes the block and queues its
  parity behind the last data shard, together with its row and the block's start and close times
  for the spread.
  - Parity is computed from a copy of each data body (k rows of `max_original()`, kept across
    blocks), so a popped data shard is the caller's at once.
  - A push that cannot be taken is dropped and counted, never undefined: no block open, shards
    still queued, shorter than `push_offset()`, no data or over `max_original()`.
- `block_decoder`: `start(base, k_total, n)`, `push_data()` / `push_parity()` the rows as views,
  `decode()` once `decodable()`, then `pop()` the rebuilt rows in row order. `reset()` frees it
  for the next block.

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
- `WINJECT_FEC_TX_PSRAM` (default y with PSRAM): the block encoder's data row copies in PSRAM.
- `WINJECT_FEC_RX_PSRAM`: decode rows and recovered payloads in PSRAM.

`WINJECT_FEC_RX_DEADLINE_MARGIN_MIN_MS` (5..100, default 5) raises the read stream's give-up
margin floor on platforms that can hold a queued parity shard longer.
