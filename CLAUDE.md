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
`Args` struct in `src/main.rs`). Logging is via `tracing` with `RUST_LOG` env-filter; unset it
defaults to `warn,doublezero_edge_connect=info` (our crate at `info`, deps quiet at `warn`).

## Architecture

A WS-server task plus **one receiver task per active feed** share a single
`tokio::sync::broadcast` channel of `FeedMessage` (the fan-out backbone) and a `Mutex<HashMap>`
instrument snapshot. `main.rs` selects the *candidate* feeds (`--feed`, or all of
`ingest::feeds::FEEDS` by default), builds the shared `Arbiter` around the broadcast `Sender`, and
hands everything to the **subscription reconciler** (`ingest::reconcile`), which is the single
activation authority: it decides *which* of those feeds (plus the WS sink and the shred forwarder)
actually run, based on what the host is subscribed to. `main.rs`'s top-level `select!` then awaits
the reconciler plus the independently-spawned public WS input feeders and metrics endpoint; the
process exits only if one of those tasks panics.

**Activation is subscription-driven and dynamic** (`ingest::reconcile` + `ingest::subscriptions`):
the reconciler polls the host's multicast subscriptions from `doublezero status --json` every
`--subscription-refresh-secs` and reconciles the running task set — spawning receivers for
newly-subscribed feeds, aborting ones that go away, bringing the **WS sink up only when ≥1
market-data feed is subscribed** (bound non-fatally: a taken port disables the sink but never
crash-loops the tunnel), and restarting the shred forwarder when its subscribed source set changes.
It is **default-on with fail-open**: no `doublezero` CLI (running from source) → the static
always-on set; a transient CLI error → keep current activations. `--subscription-gating-disable`
forces the static model. A single feed dying no longer exits the process — the reconciler respawns
it on the next tick.

Ingest has **two source transports** that converge on one shared `arbiter` before the broadcast:
the always-on DZ Edge **multicast** receivers, and optional **public WebSocket** feeders (off by
default) that backstop the edge feed — Hyperliquid (`ingest::ws_feeder`, quotes + trades) and
Phoenix (`ingest::phoenix_feeder`, trades only). Both transports emit the same `FeedMessage`s and
race in the arbiter's one per-`(venue, symbol)` floor (see `ingest/arbiter.rs` below), so
cross-source duplicates collapse and the public copy fills in only when the edge gaps.

Modules are grouped by role under `src/`:
- **`ingest/`** — the source→`FeedMessage` pipeline: `feeds`, `receiver`, `processor`, `book`,
  `subscriber`, `arbiter`, the **`subscriptions`** detector + **`reconcile`** activation loop (which
  decide what runs — see Architecture above), the optional public feeders (`public_feeder`
  scaffolding + `ws_feeder`/`phoenix_feeder` venues), and the codecs (`codec`, `codec_common`,
  `codec_midpoint`, `codec_mbo`). Intra-pipeline references use `crate::ingest::*`; this half knows
  nothing about how the data is re-served.
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
  venue, with a group `code` (`tiredsolid`/`scottsdale` — the identifier `doublezero status` reports,
  matched by the reconciler), a `FeedKind` (which protocol) and `FeedPorts` (`TwoPort` for
  TOB/Midpoint, or `ThreePort` adding a snapshot port for MBO). `FEEDS` is the built-in list; add a
  row to ingest another venue (sibling-protocol rows are added once their live endpoints are known).
  `--feed <venue>` selects a subset; consumers then filter by venue over the WS.
- **`ingest/subscriptions.rs`** — the single **detection** place. `detect()` shells out to
  `doublezero status --json` and returns the host's subscribed group **codes** (the `S:<code>`
  entries of `multicast_groups` — the authoritative per-host view), plus a code→IP map from
  `multicast group list` (`shred::discovery::parse_group_code_ips`) for the shred groups (market-data
  IPs come from `FEEDS`). Classifies into `market_data_feeds()` (subscribed enabled feeds) and
  `shred_sources()` (subscribed `edge-solana-*` → `ip:port`). Sync `Command` soft-fail; the
  `Detected` enum distinguishes `CliMissing` (fail open) from `Unavailable` (transient, keep current).
- **`ingest/reconcile.rs`** — the **activation authority**. `Reconciler::run()` polls `detect()`
  every `--subscription-refresh-secs`, computes the desired set (market-data receivers, WS on iff a
  market-data feed is subscribed, shred sources), and applies the diff via a pure `plan()`
  (spawn/abort). Owns all `JoinHandle`s; teardown is `abort()` (clean — sockets close on drop). Reaps
  finished handles so a died feed respawns. Fail-open / `--subscription-gating-disable` route through
  one `static_desired()`.
- **`ingest/receiver.rs`** — the ingest hot path. All socket plumbing is **protocol-agnostic and shared**:
  `bind_multicast`, `recv_with_ts` (kernel timestamps), `wait_for_interface_ip`, the `IDLE_REJOIN`
  watchdog, `emit_status`, and `SeqTracker`. `drive()` is a generic receive loop over **N ports**
  (1/2/3) that hands each datagram to a `FrameProcessor` via a `FrameCtx`; `run_feed()` picks the
  processor + port roles from the feed's `FeedKind`. The watchdog tracks the **mktdata** port only
  (refdata/snapshot keep ticking when market data is wedged). `FrameCtx` carries the shared
  `arbiter` (not a raw `tx`); `ctx.emit(msg)` routes through it tagged `Publisher::Edge(src_ip)`.
- **`ingest/arbiter.rs`** — the shared **pre-broadcast emit stage** every ingest source funnels
  through. `Arbiter` owns the broadcast `Sender` plus the dedup state — the per-`(venue, symbol)`
  latch-to-leader `StalenessFloor` for quotes (keyed on `QuoteId`, the canonical BBO fixed-point, with
  the `Publisher` enum as the per-tick leader identity), a **second `StalenessFloor` for MBO `depth`**
  (keyed on `DepthId`, the top-N book content at canonical `10^-8` fixed-point; both ids use `i128`
  so an `f64→int` saturation can't collapse distinct huge values, #66), and the
  `WindowedDedup` on `trade_id` for trades — and exposes one `emit(msg, publisher)` (quotes → quote
  floor, depth → depth floor, trades → window, everything else passthrough). Every arm returns an
  `Admit<Publisher>`: `Emitted` broadcasts and bumps the admitted/winner counter, `Contest{winner,
  lead_ns}` drops the losing cross-source copy and records the head-to-head lead-time histogram
  (`dz_quote_lead_ns`/`dz_trade_lead_ns`/`dz_depth_lead_ns`, #60), `Dropped` is a plain collapse.
  Wrapped `Arc<Mutex<Arbiter>>` (`SharedArbiter`) so the multicast receivers and the WS feeder share
  **one** floor per `(venue, symbol)` and race on it. The quote floor lived inside `TobProcessor` under
  PR #29; it was lifted here so a different transport (the WS feeder) can race in the same floor.
  **Depth diverges from quotes in one deliberate way: it has NO `source_ts == 0` bypass** (#28). For
  quotes 0 is the "not available" sentinel and is forwarded unlatched; for depth 0 is a *real* state —
  the initial synced-but-empty book each publisher emits right after its snapshot anchor — and the two
  publishers' identical empty anchors at `source_ts == 0` are routed through the floor so the
  non-leader's collapses (the content-inclusive depth oracle would otherwise flag the pair as
  duplicates). No wedge: a real later event has `source_ts > 0` and re-advances the floor. The depth
  floor assumes `source_ts` monotonicity only **within** a session: the MBO processor clears it on
  `EndOfSession` (whole venue) / `InstrumentReset` (that symbol) via `reset_depth_floor_for_*` — the
  session-reset escape hatch (#66, counted in `dz_depth_floor_resets_total{venue,reason}`) — so a
  venue that restarts its clock below the latched high-water doesn't wedge depth forever
  (`book.rs::on_instrument_reset` also drops `last_event_ts` so the re-synced book can't re-latch
  the old high-water). `Status` routes straight to `sender()` (no business identity to dedup).
- **`ingest/public_feeder.rs`** — venue-generic **public WS input feeder** scaffolding shared by all
  public backstops: the `PublicVenue` trait (`venue`/`url`/`subscribe_msgs`/`handle_text`), one
  reconnecting `run` loop (backoff: min 500ms, max 30s, stable-session 30s; metrics labelled by
  `venue`; no-op when `subscribe_msgs()` is empty; never propagates an error), the frame pump, and the
  decode helpers (`instrument_known`, `parse_decimal`, `finite_non_negative`). Each venue implements
  only its URL + subscribe frames + wire decode.
- **`ingest/ws_feeder.rs`** — the Hyperliquid `PublicVenue` (off by default), the first public backstop.
  Connects `wss://api.hyperliquid.xyz/ws` over TLS, subscribes `bbo` + `trades` per coin on one
  connection, decodes the HL JSON → `FeedMessage`, scales the public block time (ms) to ns so it
  shares the **same canonical `source_ts`** as the edge copy, and emits through the shared arbiter as
  `Publisher::PublicWs`. Gates each emission on the `(venue, symbol)` instrument being known in the
  shared snapshot (precision before price, supplied by edge refdata). Backstop behavior falls out of
  the floor: edge leads each tick in steady state (public copy dropped as a no-op), public fills in on
  an edge gap — no health check.
- **`ingest/phoenix_feeder.rs`** — the Phoenix `PublicVenue` (off by default), **trades only** (the
  edge Quote is a spline-blended BBO; the public book is resting-only, a different quantity, so no
  quote backstop). Subscribes Phoenix's public `trades` channel per market; Phoenix names each market
  with the **same bare ticker on the edge and public feeds** (edge `instrument_id == public assetId`),
  so the wire symbol is used verbatim — no mapping. Derives the trade price as `quoteAmount /
  baseAmount` and emits `NormalizedTrade`s as `Publisher::PublicWs` keyed on `trade_id` = the public
  `tradeSequenceNumber` (the arbiter's windowed trade dedup races them). Validated against a live
  edge+public capture (2026-06-30): `trade_id == tradeSequenceNumber` on 257/257 shared fills and
  `side` maps `bid->buy`/`ask->sell`. No `FEEDS` row depends on it (off until enabled).
- **`ingest/processor.rs`** — the per-protocol `FrameProcessor` impls (own each protocol's state and
  emit `FeedMessage`s via `ctx.emit`): `TobProcessor` (quotes + trades), `MidpointProcessor` (mids),
  `MboProcessor` (feeds order deltas + the snapshot stream into `book.rs` and emits full-state `depth`
  + trades). All gate emission **per instrument** on a known definition (precision before price). The
  quote/trade/depth cross-source dedup is **not** here anymore — it moved to `arbiter.rs`.
  `MboProcessor` reconstructs an **independent book per `(publisher, instrument)`** (keyed on the
  datagram source IP): two publishers mirror one feed but their instance-scoped per-instrument delta
  sequences collide, so the books can't be merged. `SnapshotOrder` carries only a `snapshot_id` (no
  instrument id) and routes **only to the originating publisher's** building book. `emit_depth` stamps
  `source_ts_ns = book.last_event_ts()` (a per-*event* time) while coalescing per *frame*, so two
  frames in one tick can emit two depths with the same `source_ts`; this is **benign** under the
  content-inclusive depth floor (same tick + same leader + new content → both admitted, distinct
  content → distinct oracle key) — we deliberately do **not** mutate `source_ts` with a synthetic
  tiebreak (it's a latency stamp; PROTOCOL.md promises only full-state/self-heal, not a unique
  `source_ts` per depth).
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
  (max clients/subs/inbound-rate, broadcast backpressure where a slow client drops oldest). The
  listener is bound via `ws::bind()` (separate from `ws::serve()`) so the reconciler can treat a bind
  failure as non-fatal — a taken port disables the sink but leaves the tunnel running — and activate
  the sink only once a market-data feed is subscribed.
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
  non-empty/present, **and** (for the subscription-gated ones — market-data receivers, the WS sink,
  the shred forwarder) when the reconciler sees the host subscribed to the relevant group. ws
  (output) is *configured* by a non-empty `--ws-bind` (`--ws-bind ""` disables it outright) but only
  *activated* when a market-data feed is subscribed; the public WS input feeder is **off** by
  default (on when `--ws-input-coins` is non-empty) and is **not** subscription-gated. README has the
  full activation tables.
- No TLS on the **service surface** — the WebSocket output and multicast input target a trusted/local
  network; terminate TLS at a reverse proxy if exposed. The **one** exception is the outbound
  `wss://` client in `ingest/ws_feeder.rs` (public HL feed), which uses rustls + bundled webpki roots.
