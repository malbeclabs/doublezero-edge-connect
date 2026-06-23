# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`doublezero-edge-connect` ingests one or more DoubleZero (DZ) Edge **binary multicast** feeds,
decodes them, runs the reference-data subscriber state machine, and re-serves normalized market
data over a **WebSocket** in an engine-agnostic JSON protocol. It speaks three edge-feed-spec
sibling protocols, each selected per feed by `FeedKind` in `src/ingest/feeds.rs`:
**Top-of-Book & Trades** (magic `0x445A` -> `quote`/`trade`), **Midpoint** (magic `0x4D44` ->
`midpoint`), and **Market-by-Order** (magic `0x4444`; the bridge reconstructs the L3 book and
re-serves it as full-state `depth`). Each feed maps to one venue. The input (multicast/binary) is
an implementation detail; the *only* external contract is the WebSocket output, fully specified in
**PROTOCOL.md** (v1). Any engine that speaks WebSocket + JSON consumes it via a thin adapter; the
consumer is not part of the protocol.

## Commands

```bash
cargo build --release
cargo test                 # codec round-trip + refdata subscriber state machine
cargo test quote_round_trip # run a single test by name
cargo clippy --all-targets

# Run against the built-in DZ Edge feeds (all of them by default):
sudo sysctl -w net.core.rmem_max=268435456   # allow a large SO_RCVBUF (recommended)
./target/release/doublezero-edge-connect --iface doublezero1 --ws-bind 0.0.0.0:8081
./target/release/doublezero-edge-connect --feed Hyperliquid   # only specific venues
```

All CLI flags also read from env vars (`DZ_FEEDS`, `DZ_IFACE`, `WS_BIND`, etc. — see the
`Args` struct in `src/main.rs`). Logging is via `tracing` with `RUST_LOG` env-filter (default `info`).

## Architecture

One WS-server task plus **one receiver task per selected feed** share a single
`tokio::sync::broadcast` channel of `FeedMessage` (the fan-out backbone) and a `Mutex<HashMap>`
instrument snapshot. `main.rs` selects feeds (`--feed`, or all of `ingest::feeds::FEEDS` by
default), builds the shared `Arbiter` around the broadcast `Sender`, spawns the receivers into a
`JoinSet` (and the optional WS input feeder), and exits if the WS server, any receiver, or the
feeder task returns.

Ingest has **two source transports** that converge on one shared `arbiter` before the broadcast:
the always-on DZ Edge **multicast** receivers, and an optional Hyperliquid **public WebSocket**
feeder (`ingest::ws_feeder`, off by default) that backstops the edge feed. Both emit the same
`FeedMessage`s and race in the arbiter's one per-`(venue, symbol)` floor (see `ingest/arbiter.rs`
below), so cross-source duplicates collapse and the public copy fills in only when the edge gaps.

Modules are grouped by role under `src/`:
- **`ingest/`** — the source→`FeedMessage` pipeline (always on): `feeds`, `receiver`,
  `processor`, `book`, `subscriber`, `arbiter`, the optional `ws_feeder`, and the codecs (`codec`,
  `codec_common`, `codec_midpoint`, `codec_mbo`). Intra-pipeline references use `crate::ingest::*`;
  this half knows nothing about how the data is re-served.
- **`sinks/`** — the output features, each off the hot path so one never affects another: `ws`
  (WebSocket, on by default). A new feature is a sibling module here + a spawn in `main.rs`.
- **`shred/`** — the Solana **shred forwarder** (peer of `ingest/`/`sinks/`, separate from the
  market-data pipeline — no `FeedMessage`, no WebSocket, no market-data decode). Joins the DoubleZero
  `edge-solana-*` shred multicast groups, combines them, and fans each raw datagram out to local
  UDP destinations. Pipeline: N receiver tasks → bounded `mpsc<ShredPacket>` → 1 forwarder task →
  fan-out `send_to` → M destinations. The single forwarder is the deliberate seam where the
  dedup/sigverify state lives (no cross-task sharing); receivers stay dumb (recv → push bytes). It
  reuses `ingest::receiver::{bind_multicast, wait_for_interface_ip}` (now `pub`) rather than
  duplicating socket plumbing. `shred/discovery.rs` shells out to `doublezero multicast group list`
  and prefix-selects the source groups. Activate-on-discovery; off when no source is found.
  **Sigverify + dedup (`--shred-rpc-url`):** with an RPC URL the forwarder forwards exactly one
  valid copy of each shred; without one it forwards everything (the bare behaviour). The forwarder
  threads each datagram through `parse` → leader lookup → `dedup`:
  - **`shred/parse.rs`** — pure decoder pulling signature/variant/`slot`/`index` and the signed
    message (legacy payload, or recomputed merkle root) from a raw datagram. ⚠️ **Offsets +
    merkle layout are transcribed from the agave shred format and NOT validated against a live
    `edge-solana-*` hexdump** — same draft status `codec_midpoint` had (`codec_mbo` is now
    validated, #4). Round-trip tests pin self-consistency only. Validate against a captured frame
    before trusting sigverify.
  - **`shred/verify.rs`** — ed25519 (`ed25519-dalek`) of the signature over the signed message;
    any malformed input fails verification rather than panicking.
  - **`shred/leader.rs`** — slot→leader from a Solana RPC (`getLeaderSchedule`/`getEpochInfo`),
    cached per epoch, refreshed by an off-hot-path task. `leader(slot)` returns `None` when the
    schedule isn't loaded / slot is out of epoch, which makes the forwarder **fail open**.
  - **`shred/dedup.rs`** — `DedupWindow`: bounded, prefer-valid window keyed by `(slot, index,
    type)`. `decide()` is the unit-tested gate: duplicate of a winner → drop with no sig check;
    no leader → forward (fail open, no record); else verify → valid forwards + records, invalid
    drops but leaves the key open. Eviction is a cheap slot range-drop trailing the tip by
    `--shred-dedup-window-slots`.
- **root** — `model` (shared wire types/clocks/snapshots) and `main`.

- **`ingest/feeds.rs`** — the hardcoded feed registry: each `Feed` is one multicast group mapped to one
  venue, with a `FeedKind` (which protocol) and `FeedPorts` (`TwoPort` for TOB/Midpoint, or
  `ThreePort` adding a snapshot port for MBO). `FEEDS` is the built-in list; add a row to ingest
  another venue (sibling-protocol rows are added once their live endpoints are known). `--feed
  <venue>` selects a subset; consumers then filter by venue over the WS.
- **`ingest/receiver.rs`** — the ingest hot path. All socket plumbing is **protocol-agnostic and shared**:
  `bind_multicast`, `recv_with_ts` (kernel timestamps), `wait_for_interface_ip`, the `IDLE_REJOIN`
  watchdog, `emit_status`, and `SeqTracker`. `drive()` is a generic receive loop over **N ports**
  (1/2/3) that hands each datagram to a `FrameProcessor` via a `FrameCtx`; `run_feed()` picks the
  processor + port roles from the feed's `FeedKind`. The watchdog tracks the **mktdata** port only
  (refdata/snapshot keep ticking when market data is wedged). `FrameCtx` carries the shared
  `arbiter` (not a raw `tx`); `ctx.emit(msg)` routes through it tagged `Publisher::Edge(src_ip)`.
- **`ingest/arbiter.rs`** — the shared **pre-broadcast emit stage** every ingest source funnels
  through. `Arbiter` owns the broadcast `Sender` plus the dedup state — the per-`(venue, symbol)`
  latch-to-leader `StalenessFloor` for quotes (keyed on `QuoteId`, the BBO `f64`-bits, with the
  `Publisher` enum as the per-tick leader identity) and the `WindowedDedup` on `trade_id` for trades
  — and exposes one `emit(msg, publisher)` (quotes → floor, trades → window, everything else
  passthrough). Wrapped `Arc<Mutex<Arbiter>>` (`SharedArbiter`) so the multicast receivers and the WS
  feeder share **one** floor per `(venue, symbol)` and race on it. The floor lived inside
  `TobProcessor` under PR #29; it was lifted here so a different transport (the WS feeder) can race in
  the same floor. `Status` routes straight to `sender()` (no business identity to dedup).
- **`ingest/ws_feeder.rs`** — the optional Hyperliquid **public** WS input feeder (off by default).
  Connects `wss://api.hyperliquid.xyz/ws` over TLS, subscribes `bbo` + `trades` per coin on one
  connection, decodes the HL JSON → `FeedMessage`, scales the public block time (ms) to ns so it
  shares the **same canonical `source_ts`** as the edge copy, and emits through the shared arbiter as
  `Publisher::PublicWs`. Gates each emission on the `(venue, symbol)` instrument being known in the
  shared snapshot (precision before price, supplied by edge refdata). Its own task with reconnect +
  backoff; decode/socket errors are logged and swallowed so the multicast hot path is never touched.
  Backstop behavior falls out of the floor: edge leads each tick in steady state (public copy dropped
  as a no-op), public fills in on an edge gap — no health check.
- **`ingest/processor.rs`** — the per-protocol `FrameProcessor` impls (own each protocol's state and
  emit `FeedMessage`s via `ctx.emit`): `TobProcessor` (quotes + trades), `MidpointProcessor` (mids),
  `MboProcessor` (feeds order deltas + the snapshot stream into `book.rs` and emits full-state `depth`
  + trades). All gate emission **per instrument** on a known definition (precision before price). The
  quote/trade cross-source dedup is **not** here anymore — it moved to `arbiter.rs`.
- **`ingest/codec.rs` / `codec_midpoint.rs` / `codec_mbo.rs`** — pure decoders for each protocol's
  little-endian fixed-size frames, all built on `ingest/codec_common.rs` (shared 24B frame header, 4B
  message header, LE readers, `cstr`, and the generic `decode_frame_with(magic, ...)` walker).
  **`codec.rs` (TOB) offsets are validated byte-for-byte** against the authoritative Go decoder in
  `edge-multicast-ref` — **do not change them without re-validating**. **`codec_mbo.rs` is now
  field-by-field validated too (#4):** shared-with-TOB types reuse the byte-validated TOB layout,
  and the MBO-specific types are pinned by offset-independent unit tests plus a real-frame decode
  test over the byte-validated committed golden fixtures (`tests/codec_mbo_fixtures.rs`). ⚠️
  **`codec_midpoint.rs` offsets still come from the edge-feed-spec draft and are NOT
  reference-validated**; its round-trip tests only pin self-consistency, so validate against a live
  frame hexdump before trusting its output (see "Conventions" below).
- **`ingest/book.rs`** — `BookState`: per-instrument L3 order book + the MBO snapshot+delta recovery state
  machine (`Synced`/`Recovering`), using the per-instrument delta sequence and snapshot anchor.
  Codec-agnostic (`DeltaOp`/raw ints) so it's unit-tested in isolation; derives top-N `depth`.
- **`ingest/subscriber.rs`** — `RefDataState<D>`, the reference-data state machine, **generic over** any
  instrument-definition type implementing `InstrumentDef` (its id + manifest seq), so all three
  protocols reuse it. Collects definitions tagged with the latest `ManifestSummary` seq; `ready()`
  (true once `defs.len() == expected_count`) reports when the *whole* set is known. **Emission gates
  per instrument, not on `ready()`**: a processor emits as soon as `definition(id)` resolves, so
  consumers never see a price before its precision, but a single symbol flows without waiting for
  the full set (an all-or-nothing gate could wedge the feed on a startup/reset race). Uses
  wraparound-safe u16 sequence comparison (`is_later`).
- **`sinks/ws.rs`** — fans the broadcast out to clients (on by default; disable with an empty
  `--ws-bind`). On connect it replays the instrument snapshot (precision first) **then the latest
  `depth` per symbol** (full state), then streams quotes/trades/midpoints/depth. Implements the
  PROTOCOL.md v1 surface: optional per-client subscribe/unsubscribe filtering (empty filter list =
  firehose), app ping/pong + server WS-ping heartbeat with idle-timeout reaping, and the limits
  (max clients/subs/inbound-rate, broadcast backpressure where a slow client drops oldest).
- **`model.rs`** — wire types (`NormalizedQuote`/`NormalizedTrade`/`NormalizedMidpoint`/
  `NormalizedDepth`/`NormalizedInstrument`, the `FeedMessage` tagged enum) and the `now_ns()` /
  `now_mono_ns()` clocks. The `InstrumentSnapshot` and `DepthSnapshot` are both keyed by
  **`(venue, symbol)`** so feeds sharing a symbol don't clobber each other.

## Conventions and gotchas

- **PROTOCOL.md is the contract.** Any change to the WebSocket JSON (field names, message types,
  control frames) must keep the forward-compat rule (consumers ignore unknown types/fields) and
  be reflected in PROTOCOL.md. There is no `v` field on the wire.
- **Midpoint offsets are still unvalidated.** The `codec_midpoint.rs` byte layout came from the
  edge-feed-spec *draft*, not a reference codec; its round-trip tests only pin self-consistency.
  Before enabling a live Midpoint feed, run the bridge with `RUST_LOG=debug` against the real
  group/ports and confirm decoded fields against a frame hexdump. **`codec_mbo.rs` is validated
  (#4):** shared-with-TOB types (frame/message headers, `InstrumentDefinition`, `Trade`,
  `ManifestSummary`, type tags) reuse the byte-validated TOB layout, and the MBO-specific types are
  pinned by offset-independent unit tests + a real-frame decode test over the committed fixtures
  (`tests/codec_mbo_fixtures.rs`). Oracle strength varies by type:
  `Order{Add,Cancel,Execute}`/`BatchBoundary`/`Snapshot{Begin,Order,End}` have **real-capture**
  backing from the two-sided TYO recorder fixture (#36 — the snapshot is BTC's full 44,598-order
  book, so `SnapshotOrder` is well-covered); `Trade` has no MBO fixture but shares the
  byte-validated TOB layout (pinned by a cross-codec equality test); and
  `InstrumentReset`/`Heartbeat`/`EndOfSession` have **no fixture** (offset-test-only — confirm
  against a live frame before a live MBO feed). No `FEEDS` row uses these kinds until their
  endpoints are confirmed.
- **MBO is re-served as derived full-state `depth`, never raw deltas.** The bridge reconstructs the
  L3 book and runs snapshot+delta recovery internally (`book.rs`), so the WS contract's "every
  message is full state and self-heals" guarantee holds. Do not expose order add/cancel/execute
  events on the wire.
- **Four latency timestamps** ride every quote: `source_ts_ns` (venue), `kernel_rx_ts_ns`
  (`SO_TIMESTAMPNS`, captured in the driver softirq — best-effort, falls back to 0), `recv_ts_ns`
  (user-space post-decode), `ws_send_ts_ns` (stamped in `sinks/ws.rs` just before send). `0` is
  the sentinel for "not available" — never treat it as a real time.
- **Manifest `Valid=0` workaround** in `ingest/processor.rs`: the live DZ Edge HL publisher
  currently emits `ManifestSummary` with `Valid=0`, which would block all quotes. It is forced to
  `valid=true` (logged once). Marked `REVISIT` — drop the override and pass `m.valid` once the
  publisher is fixed.
- `--iface` accepts an interface name (resolved to its IPv4 via `ip -4 -o addr show`) or an IPv4
  literal directly.
- **Source/sink activation is uniform**: a source or sink runs when its key config value is
  non-empty/present. ws (output) is on by default (non-empty default `--ws-bind`; `--ws-bind ""`
  disables it); the public WS input feeder is **off** by default (on when `--ws-input-coins` is
  non-empty). README has the full activation tables.
- No TLS on the **service surface** — the WebSocket output and multicast input target a trusted/local
  network; terminate TLS at a reverse proxy if exposed. The **one** exception is the outbound
  `wss://` client in `ingest/ws_feeder.rs` (public HL feed), which uses rustls + bundled webpki roots.
