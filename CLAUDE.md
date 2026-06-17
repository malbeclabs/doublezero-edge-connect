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
default), spawns the receivers into a `JoinSet`, and exits if the WS server or any receiver
returns.

Modules are grouped by role under `src/`:
- **`ingest/`** — the multicast→`FeedMessage` pipeline (always on): `feeds`, `receiver`,
  `processor`, `book`, `subscriber`, and the codecs (`codec`, `codec_common`, `codec_midpoint`,
  `codec_mbo`). Intra-pipeline references use `crate::ingest::*`; this half knows nothing about how
  the data is re-served.
- **`sinks/`** — the output features, each off the hot path so one never affects another: `ws`
  (WebSocket, on by default). A new feature is a sibling module here + a spawn in `main.rs`.
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
  (refdata/snapshot keep ticking when market data is wedged).
- **`ingest/processor.rs`** — the per-protocol `FrameProcessor` impls (own each protocol's state and
  emit `FeedMessage`s): `TobProcessor` (quotes + trades), `MidpointProcessor` (mids), `MboProcessor`
  (feeds order deltas + the snapshot stream into `book.rs` and emits full-state `depth` + trades).
  All gate emission **per instrument** on a known definition (precision before price).
- **`ingest/codec.rs` / `codec_midpoint.rs` / `codec_mbo.rs`** — pure decoders for each protocol's
  little-endian fixed-size frames, all built on `ingest/codec_common.rs` (shared 24B frame header, 4B
  message header, LE readers, `cstr`, and the generic `decode_frame_with(magic, ...)` walker).
  **`codec.rs` (TOB) offsets are validated byte-for-byte** against the authoritative Go decoder in
  `edge-multicast-ref` — **do not change them without re-validating**. ⚠️ **`codec_midpoint.rs`/`codec_mbo.rs` offsets come from the edge-feed-spec
  draft and are NOT reference-validated**; their round-trip tests only pin self-consistency, so
  validate against a live frame hexdump before trusting their output (see "Conventions" below).
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
- **Sibling-protocol offsets are unvalidated.** The Midpoint/Market-by-Order byte layouts in
  `codec_midpoint.rs`/`codec_mbo.rs` came from the edge-feed-spec *draft*, not a reference codec
  (only TOB is byte-validated). Before enabling a live Midpoint/MBO feed, run the bridge with
  `RUST_LOG=debug` against the real group/ports and confirm decoded fields against a frame
  hexdump. No `FEEDS` row uses these kinds until their endpoints + offsets are confirmed.
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
- **Sink activation is uniform**: a sink runs when its key config value is non-empty/present. ws is
  on by default (non-empty default `--ws-bind`; `--ws-bind ""` disables it). README has the full
  activation table.
- No TLS — the service targets a trusted/local network; terminate TLS at a reverse proxy if exposed.
