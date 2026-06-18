# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Shred forwarder sigverify + dedup (#25): when `--shred-rpc-url` (`DZ_SHRED_RPC_URL`) is
  set, the forwarder forwards exactly **one valid copy** of each shred. A bounded,
  prefer-valid dedup window keyed by `(slot, index, type)` (`--shred-dedup-window-slots`,
  default `512`) drops duplicates of an already-forwarded copy without a signature check;
  the first copy of a key is ed25519-verified against its slot leader (fetched per epoch
  via `getLeaderSchedule`/`getEpochInfo`) over the legacy payload or recomputed merkle
  root; an invalid copy is dropped but leaves the key open so a later valid copy can still
  win. A slot whose leader isn't known yet fails open (forwarded, not deduped). Without
  `--shred-rpc-url`, behaviour is unchanged (forward every datagram). New deps:
  `ed25519-dalek`, `sha2`, `bs58`, `reqwest` (rustls). ⚠️ The shred/merkle byte offsets are
  transcribed from the agave layout and are **not** validated against a live `edge-solana-*`
  hexdump (same status as the repo's unvalidated sibling codecs); the forwarder logs a
  one-time warning and a periodic verify tally so a misparse is visible.
- Shred forwarder (`src/shred/`): joins the DoubleZero `edge-solana-*` shred multicast
  feeds, combines them, and fans each datagram out to one or more local UDP destinations
  (no dedup / no signature verification yet). Sources are discovered via `doublezero
  multicast group list` (prefix-matched, default `edge-solana-`) or overridden with
  repeatable `--shred-source GROUP:PORT`. Activates on discovery; configured with
  `--shred-code-prefix`, `--shred-port` (default `7733`), `--shred-forward` (default
  `127.0.0.1:20000`), reusing `--iface`/`--recv-buf`. Reuses the `ingest::receiver` socket
  plumbing (`bind_multicast`, `wait_for_interface_ip`) — now `pub` (#24).
  Discovery deserializes `doublezero multicast group list --json-compact` (the machine-readable
  contract) instead of scraping the human table, and filters on `status == activated`. The
  forwarder uses one `connect`ed send socket per destination so a down destination's async ICMP
  error can't drop a datagram bound for a healthy one. A shred-side failure is logged and
  isolated — it never takes the market-data bridge down. Datagrams that fill the recv buffer
  (likely truncated, no `MSG_TRUNC`) are dropped rather than forwarded corrupt (#24).
- Hyperliquid **public** WebSocket input feeder (`src/ingest/ws_feeder.rs`), a second ingest source
  that backstops the DZ Edge multicast feed (#8). It connects to `wss://api.hyperliquid.xyz/ws` over
  TLS, subscribes `bbo` + `trades` per configured coin on one connection, decodes the HL JSON into the
  same `FeedMessage`s the multicast pipeline produces, and emits them through the shared arbiter as a
  distinct `Publisher::PublicWs`. Because it shares the per-`(venue, symbol)` latch-to-leader floor with
  the edge feed, the backstop falls out with **no health check**: the edge wins every tick in steady
  state (the public copy loses the race and is dropped as a no-op), and when the edge gaps the public
  copy is the first to cross the floor and fills in. The public block time (ms) is scaled to ns so both
  sources share the same canonical `source_ts`; trades dedup on `tid` (the edge feed's `trade_id`).
  **Off by default**, enabled with a non-empty `--ws-input-coins` (env `WS_INPUT_COINS`);
  `--ws-input-url` (env `WS_INPUT_URL`) overrides the endpoint. Failure-isolated (its own task with
  reconnect + exponential backoff; decode/socket errors are logged and swallowed), and each public
  quote/trade is gated on its `(venue, symbol)` instrument being known (precision before price). A mock
  HL WS input harness drives two new E2E cases (edge-leads-in-steady-state, edge-gap→public-fills-in).
  The feeder adds no new WebSocket output fields of its own; it populates the same `bid_n`/`ask_n`
  (from the public `bbo` level's `n`) the edge feed serves.
  - Reconnect backoff resets to the floor only after a session stays up past a minimum duration, so a
    connect-then-immediate-drop loop keeps escalating instead of hammering the public endpoint.
  - Shared mutexes (`InstrumentSnapshot`/`DepthSnapshot`/arbiter) lock via a poison-recovering helper
    (`model::lock`), so an unrelated panic in one ingest task can't cascade into the others.
- Cross-source quote identity is the canonical `bbo_hash` (`StableBBOHash`): bid/ask price + size at
  the `10^-8` scale plus `bid_n`/`ask_n`. Computing it at a fixed scale (not raw `f64` bits) collapses
  the edge's `raw * 10^exp` and the public feed's parsed float for the same economic price onto one
  identity, so a cross-source copy dedups. The arbiter also drops a quote whose `source_ts` is
  implausibly far in the future before it can advance the shared floor — one bad/hostile public
  timestamp would otherwise latch the floor ahead and drop every real edge quote as stale until restart.
- Real Hyperliquid Market-by-Order (MBO) feed ingestion: a confirmed `FEEDS` row
  (`233.84.178.15`, ports `10201`/`10202`/`10203`, depth-only) re-served as full-state
  `depth`. `--feed <venue>` now selects every protocol feed for that venue.
- Per-feed `emit_trades` flag so a venue carried by both Top-of-Book and Market-by-Order
  does not double-emit `trade` messages (Top-of-Book owns trades; MBO is depth-only).
- End-to-end test suite that drives the release binary over loopback multicast and asserts
  the WebSocket output contract, with deduplication-oracle assertions for future work.
- `examples/pcap2frames.rs` dev tool: converts a multicast pcap into the test harness's
  frame-log fixtures, demultiplexing one publisher by source IP and filtering by protocol
  (Top-of-Book/Market-by-Order) and symbol. Decoding each frame through the real codecs
  doubles as live-feed validation of the codec byte offsets.
- Live two-publisher Top-of-Book BTC fixtures (`tests/fixtures/tob_btc_pub{A,B}.*`) for the
  upcoming multi-publisher deduplication work; provenance and regeneration in
  `tests/fixtures/PROVENANCE.md`.
- `pcap2frames --combined-with <ip>`: emits one capture-ordered, source-IP-and-role-tagged stream
  of two publishers (`tob_btc_dual.combined.bin`), preserving the real interleaving the
  multi-publisher dedup must collapse.
- `pcap2frames --symbol` is now repeatable (and the combined report tallies kept quote messages
  per `(symbol, publisher)`), enabling a multi-symbol two-publisher fixture
  (`tob_multi_dual.combined.bin`: BTC busy / SOL medium / DOGE quiet) that exercises the dedup's
  per-`(venue, symbol)` independent windows.
- Multi-publisher Top-of-Book deduplication: when several independent publishers mirror one feed
  onto a multicast group, the bridge merges them into one clean stream. Datagrams are demultiplexed
  by source IP (`FrameCtx.publisher`); the frame-sequence tracker is per-publisher so a slower
  publisher's frames aren't dropped before dedup. Quotes dedup on a per-`(venue, instrument)`
  `source_ts` latch-to-leader floor keyed on the **canonical BBO identity** (the components of the
  spec's `bbo_hash`: bid/ask price + size + the `bid_n`/`ask_n` source counts): within one `source_ts`
  tick (the venue stamps coarsely, so a tick holds a whole sub-sequence of real top-of-book changes)
  it emits only the *leader* — the first publisher to open the tick — and drops other publishers'
  samples at that `source_ts`. This is because arrival order across publishers is corrupted by
  per-publisher network delay (the `hl-bbo-feed-race` board shows inter-feed skew over 100 ms), so
  interleaving two sources inside one tick can serve a stale sample as the freshest — on a falling
  price, a slower publisher's older, higher sample landing last would read as a phantom uptick. The
  leader is re-selected each new tick, so the lowest-delay publisher for a given moment naturally wins.
  A strictly-older BBO (stale laggard) and the leader's exact `(source_ts, content)` repeats are
  dropped too, so the emitted `source_ts` is non-decreasing (not strictly increasing) per instrument
  and within a tick the series is one publisher's coherent, in-order subsequence. `source_ts == 0`
  (the "not available" sentinel) bypasses the floor (always forwarded, never latched) so a feed that
  stops stamping time can't wedge non-leaders, and the per-tick content set is capacity-bounded so a
  stalled `source_ts` can't grow it without limit. The dedup key is allocation-free on the hot path
  (`(&'static venue, instrument_id)`). Trades, being point-in-time events, dedup on a windowed
  `(venue, instrument, trade_id)` identity so every distinct print is kept. (Market-by-Order depth
  dedup is tracked separately.)
- Top-of-Book `quote` messages now carry `bid_n`/`ask_n` (the edge-feed-spec "Bid/Ask Source Count":
  orders/sources at the best bid/ask, `0` if unavailable). They were decoded-and-discarded before;
  now decoded, re-served on the WebSocket (additive, forward-compatible — see PROTOCOL.md), and part
  of the canonical BBO identity, so a count-only change at an unchanged price/size is a distinct quote.

### Changed
- The quote latch-to-leader floor and the windowed trade dedup moved out of `TobProcessor` into a
  shared pre-broadcast `Arbiter` (`src/ingest/arbiter.rs`) that owns the broadcast `Sender` and
  exposes one `emit(msg, publisher)` entry point (#8). Every ingest source — each multicast receiver
  and the new public WS feeder — funnels through one `Arc<Mutex<Arbiter>>`, so they all race on the
  same per-`(venue, symbol)` floor instead of each owning a private one. A `Publisher { Edge(IpAddr),
  PublicWs }` enum is the floor's leader identity. Behavior-preserving for the edge path (the
  two-publisher and single-publisher counts are unchanged); the refactor itself adds no output fields.
- Feed registry is keyed by `(venue, kind)` instead of `venue`, so one venue can carry
  multiple protocol feeds.
- Bumped dependencies from the open Dependabot PRs: `tokio-tungstenite`
  0.23 → 0.29, `socket2` 0.5 → 0.6, `nix` 0.29 → 0.31, and the GitHub Actions
  `actions/checkout` (v6.0.3), `docker/login-action` (v4.2.0),
  `docker/setup-buildx-action` (v4.1.0), `docker/build-push-action` (v7.2.0),
  and `aws-actions/configure-aws-credentials` (v6.2.0). The `tokio-tungstenite`
  0.29 upgrade switched `Message::Text`/`Ping`/`Pong` payloads to
  `Utf8Bytes`/`Bytes`, updated in `src/sinks/ws.rs`.
- Exposed the ingest pipeline, wire model, and sinks as a library (`src/lib.rs`); the binary
  (`src/main.rs`) is now a thin wrapper, so dev tooling and tests can reuse the codecs.

### Fixed
- Docker release workflow could not push to GHCR: the reusable
  `release.docker.edge-connect.build` workflow declared a top-level `permissions:
  contents: read` block, which intersects with (and so can only narrow) the caller's
  grant — silently dropping the `packages: write` that the publish/rebuild jobs grant, so
  the push was denied. Removed the block entirely so the `workflow_call`-only workflow
  inherits each caller's permissions (publish/rebuild → write, smoke → read), which is the
  only form that both authorizes the push and keeps smoke (PR) builds push-gated.
- Corrected inverted Market-by-Order order-book side constants (`0 = Bid`, `1 = Ask` per
  the edge-feed-spec); bids and asks in `depth` were previously swapped.
- Warn instead of silently clobbering when two feeds for the same `(venue, symbol)` publish
  instrument definitions with different price/quantity exponents.

### Security
- Hardened the codec frame walker against out-of-bounds reads: the per-message body decoders
  now read every field through bounds-checked little-endian readers, so a truncated or
  malformed datagram (a runt message that under-declares its length) decodes to
  `Message::Other` instead of panicking the receiver task — which previously propagated out
  of `run_feed` and exited the whole process (a single crafted datagram could take the bridge
  down for every venue and WS consumer). Applies to all three sibling codecs (TOB / Midpoint /
  Market-by-Order).
- Bounded the per-publisher frame-sequence map (`TobProcessor`) to `MAX_PUBLISHERS` (256) with
  least-recently-inserted eviction. The map is keyed on the datagram source IP, which is
  unauthenticated and spoofable, so without a cap a forged-source flood could grow it without
  limit (memory-exhaustion DoS); an evicted legitimate publisher simply re-anchors its sequence
  on its next frame.

## [0.1.0]

### Added
- Initial release of `doublezero-edge-connect`: ingests DoubleZero Edge binary
  multicast feeds (Top-of-Book & Trades, Midpoint, Market-by-Order), runs the
  reference-data subscriber state machine, and re-serves normalized market data over a
  WebSocket in the engine-agnostic JSON protocol specified in `PROTOCOL.md` (v1).

[Unreleased]: https://github.com/malbeclabs/doublezero-edge-connect/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/malbeclabs/doublezero-edge-connect/releases/tag/v0.1.0
