# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Subscription-driven feed activation** — the bridge now activates only the feeds this host is
  actually subscribed to, and adds/removes them at runtime as subscriptions change:
  - A single detector (`src/ingest/subscriptions.rs`) reads the host's subscriptions from
    `doublezero status --json` (`multicast_groups`, the `S:<code>` entries — the authoritative
    per-host view, unlike the network-wide `multicast group list`), resolving shred-group IPs via
    `multicast group list` (`src/shred/discovery.rs::parse_group_code_ips`). Each market-data feed
    now carries its group `code` (`src/ingest/feeds.rs`: `tiredsolid` = Hyperliquid, `scottsdale` =
    Phoenix).
  - A periodic reconciler (`src/ingest/reconcile.rs`) polls every `--subscription-refresh-secs`
    (`DZ_SUBSCRIPTION_REFRESH_SECS`, default 30) and diffs desired-vs-running, spawning/aborting
    market-data receivers, the WebSocket sink, and the shred forwarder. The **WebSocket sink comes
    up only when ≥1 market-data feed is subscribed** (so a shreds-only host serves no WS and can't
    collide with an existing `:8081` service); shred sources come from the subscribed
    `edge-solana-*` groups.
  - **Default-on with fail-open**: with no `doublezero` CLI (running from source) gating falls open
    to the static always-on set; a transient CLI failure keeps the current activations rather than
    flapping. `--subscription-gating-disable` (`DZ_SUBSCRIPTION_GATING_DISABLE`) forces the static
    model. A single feed dying no longer exits the process — the reconciler respawns it.
- Cross-source de-duplication **win metrics**, surfacing how the edge feed beats the
  original/public sources in both quantity and latency at each de-dup contest:
  - Quotes/trades (`src/ingest/arbiter.rs`): the staleness floor and windowed dedup now report
    the first cross-source follower of a `source_ts` tick / `trade_id` as a contest, recording
    `dz_quote_lead_ns` and `dz_trade_lead_ns` histograms (labelled by `winner` **and** `loser`,
    each `edge`/`public`; `_count` is the head-to-head win count, the buckets are the lead margin)
    plus `dz_trades_admitted_total` (the trade-side mirror of `dz_quotes_admitted_total`). The
    `loser` label keeps an edge-vs-edge mirror race (`{winner="edge",loser="edge"}`) out of the
    headline edge-vs-public margin (`{winner="edge",loser="public"}`) in multi-mirror deployments.
  - Shreds (`src/shred/`): each datagram now carries its source multicast group and a monotonic
    arrival timestamp, and the dedup window records the winning group, so a duplicate from a
    *different* group emits `dz_shred_wins_total{winner}` and `dz_shred_lead_ns{winner}` (how far
    the group that delivered first led by). A same-group retransmit stays a plain drop.
  - Recording is always on (only the `/metrics` exposer stays gated by `--metrics-bind`); lead
    times are clamped non-negative.
- Phoenix public-API trade feeder (`ingest::phoenix_feeder`), an off-by-default backstop for the edge
  Phoenix multicast TRADE stream (#53). It subscribes Phoenix's public `trades` channel per market,
  emits `NormalizedTrade`s through the shared arbiter as `Publisher::PublicWs` (deduped on
  `trade_id` = the public `tradeSequenceNumber`), and is enabled with `--phoenix-ws-input-markets`
  (`PHOENIX_WS_INPUT_MARKETS`, bare tickers e.g. `SOL,BTC`) / `--phoenix-ws-input-url`. Trades only —
  no quote backstop (the edge BBO is spline-blended; Phoenix's public book is resting-only). Validated
  against a live edge+public capture (2026-06-30): Phoenix uses the same bare symbol on both feeds
  (edge `instrument_id == public assetId`) and `trade_id == tradeSequenceNumber` on shared fills. No
  `FEEDS` row depends on it.

### Changed
- Installer (`scripts/connect*.sh`) usability fixes after review:
  - **WebSocket port preflight**: before starting the container the installer checks whether the WS
    port is already bound on the host and, interactively, offers to pick another port, disable the
    sink, or continue (non-interactively it warns and continues — the bridge then runs without the
    sink, tunnel unaffected).
  - **`WS_BIND=""` now works through the one-liner**: `WS_BIND` is forwarded whenever it is *set*,
    including set-but-empty, so the WS sink can be disabled straight from the pipe (previously only
    non-empty values were relayed, forcing a hand-written `docker run`).
  - **Firewall guidance for default-deny-incoming hosts**: the ufw/firewalld hints now note that
    allowing GRE + UDP 44880 admits only the *outer* encapsulated packets — the decapsulated inner
    multicast re-traverses `INPUT` on the tunnel interface (`doublezero1`) and must be allowed too
    (`sudo ufw allow in on doublezero1`). Mirrored in `README.md` / `scripts/README.md`.
- Public-feeder transport scaffolding extracted into a venue-generic `ingest::public_feeder`
  (a `PublicVenue` trait + one reconnecting run loop + shared decode helpers); Hyperliquid
  (`ingest::ws_feeder`) is the first implementor (#53). The four `dz_ws_feeder_*` metrics are now
  labelled by `venue` so a second venue's series don't collide.
- Container logs can no longer fill the host disk, and the default is quieter:
  - The installer's `docker run` (`scripts/connect.sh`) now pins the `json-file` log driver with
    `max-size=20m` + `max-file=3`, capping the long-lived container's on-disk log at ~60 MB
    (previously unbounded — the default driver rotated nothing). Documented for by-hand runs in
    `docs/self-hosting.md`.
  - The default log filter (when `RUST_LOG` is unset) is now `warn,doublezero_edge_connect=info`
    instead of a blanket `info`: the bridge's own startup/operational breadcrumbs stay at `info`
    while noisy dependency chatter is held to `warn`. Set `RUST_LOG=debug` for verbose output.
    Applied in both `src/main.rs` and the image `ENV`.

### Fixed
- `select_feeds` now dedups repeated `--feed` names on `(venue, kind)`, so `--feed Hyperliquid
  --feed Hyperliquid` spawns the same receivers as `--feed Hyperliquid` (previously each match was
  spawned twice, contending for the same multicast group/port) (`src/main.rs`, #9).
- A taken WebSocket-sink port no longer takes the whole bridge down. A bind failure on `--ws-bind`
  (e.g. the default `0.0.0.0:8081` colliding with a pre-existing `127.0.0.1:8081` listener) was
  fatal: the process exited, the container's `--restart unless-stopped` restarted it, doublezerod
  and the DoubleZero tunnel came down with it, and — since `doublezero connect multicast` runs only
  once from the installer — the tunnel never re-established (status stuck `disconnected`, the real
  cause buried in the restart loop). The listener is now bound eagerly (`sinks::ws::bind`, split
  from `serve`) and a bind failure is logged and skipped: the bridge runs without the sink while
  the tunnel and shred forwarding keep going (`src/main.rs`, `src/sinks/ws.rs`).
- Installer pre-flight access-pass check (`scripts/connect*.sh`) hardened after review:
  - A confirmed miss (an identity with no pass for the host IP or `0.0.0.0`) now only hard-aborts
    when the public IP was **explicitly supplied** via `DZ_CLIENT_IP`; when the IP was only
    **auto-detected** (best-effort egress lookup, which can differ from the bound IP behind
    NAT/CGNAT/multi-homed hosts) it now **warns and continues** instead of aborting a
    legitimately-provisioned operator, leaving `doublezero connect` as the real check.
  - Reading the keypair file for the check no longer runs under `set -e`, so a root-owned `0600`
    key (readable by the root Docker mount but not by the invoking user) degrades to a warning
    instead of silently aborting the whole installer.
  - The detected/supplied public IP is now strictly validated as a dotted quad (round-tripped
    through `inet_ntoa(inet_aton(ip))`), rejecting lenient `inet_aton` forms (`1.2.3`, trailing
    junk) that could yield a confident-but-wrong verdict; a malformed IP is treated as unknown.
  - An unreadable/invalid keypair (not a 64-int JSON array) now produces a distinct "could not
    read or parse the keypair" warning instead of misattributing the failure to the ledger RPC.
  - The ledger RPC URL is asserted to be `http(s)://` before use, so a `DZ_LEDGER_RPC_URL` with a
    `file://` (or other) scheme can't be dereferenced.
- **MBO depth was silently broken on the live feed.** The live HL publisher emits MBO
  `ManifestSummary` with `Valid=0` (the same quirk `TobProcessor` already overrides); `MboProcessor`
  honored it, which clears all instrument definitions, so precision never resolved and the feed emitted
  zero `depth`. `MboProcessor` now overrides `Valid=0`→true like TOB (logged once, `REVISIT`).
  Regression test: `mbo_manifest_valid_zero_is_overridden_so_depth_flows`. The e2e MBO test missed this
  because its vendored golden carries `Valid=1`; the bug surfaced minting a real-capture MBO fixture.

### Changed
- `codec_mbo` field offsets validated and the blanket "draft" caveat lifted (#4, follow-up to #2),
  with the per-type oracle strength documented honestly rather than claimed uniform:
  - **Shared-with-TOB** layouts (frame/message headers, `InstrumentDefinition`, `Trade`,
    `ManifestSummary`, type tags) reuse the byte-validated TOB `codec.rs`; a new cross-codec test
    (`tob_shared_layouts_decode_identically`) decodes the same bytes through both codecs and asserts
    equal fields, so the sharing is self-enforcing.
  - **Real publisher capture** backs `Order{Add,Cancel,Execute}`, `BatchBoundary`, the full
    `Snapshot{Begin,Order,End}` group, and the shared `InstrumentDefinition`/`ManifestSummary` via a
    new real-frame decode test (`tests/codec_mbo_fixtures.rs`) over the two-sided TYO recorder
    fixtures (#36). The snapshot is BTC's complete 44,598-order book, so `SnapshotOrder` is
    well-covered, and the test asserts `total_orders == decoded order count` as a cross-field check.
  - **Offset-test-only** (no committed fixture; pinned by the offset-independent unit tests, confirm
    against a live frame before a live MBO feed): `InstrumentReset`, `Heartbeat`, `EndOfSession`.
  No offset discrepancies found — the side-mapping bug fixed in #2 was the only one. The "size 20 vs
  fields-to-24" `ManifestSummary` suspicion was a non-issue: the body is 20 bytes (on-wire 24),
  identical to TOB, and no size-20 constant exists in code.
- README refocused on the **operator**: it now leads with what the bridge does, the install
  one-liner (`curl -fsSL https://get.doublezero.xyz/connect | bash`, plus the testnet/devnet
  variants), and how to configure/override it via environment variables before the pipe. The
  detailed per-feature reference (self-hosting/from-source + Docker, output sinks, input sources,
  Solana shred forwarding) moved into a new `docs/` directory the README links out to. Removed the
  misleading `https://doublezero.xyz/install` command that contradicted the canonical
  `get.doublezero.xyz/connect` one-liner.

### Added
- **Multi-publisher dedup for Market-by-Order `depth`** (#28, the MBO half of #3 — TOB shipped
  earlier). `MboProcessor` now reconstructs an **independent L3 book per `(publisher, instrument)`**
  (keyed on the datagram source IP), since two publishers' instance-scoped per-instrument delta
  sequences collide and cannot be merged into one book; `SnapshotOrder` (which carries only a
  `snapshot_id`, no instrument id) routes only to the originating publisher's building book. The
  resulting redundant `depth` is collapsed at the shared `Arbiter` by a **latch-to-leader staleness
  floor** keyed on `(venue, symbol)` with a content-inclusive `DepthId` (top-N levels at canonical
  `10^-8` fixed-point) — the same primitive as the quote floor, **but with no `source_ts == 0`
  bypass**: the two identical synced-but-empty book anchors two publishers emit at `source_ts == 0`
  deliberately collapse to one. The WS-replay depth map is written by the arbiter on the admit
  decision (the leader's broadcast book), not pre-floor. New metrics
  `dz_depth_admitted_total{venue,publisher}` (who is winning the book race), `dz_depth_dropped_total`,
  `dz_depth_future_rejected_total`, plus — mirroring the cross-source win metrics (#60) — the
  head-to-head lead-time histogram `dz_depth_lead_ns{venue,winner,loser}` (how far the leading
  publisher's book beat the follower's at a contested `source_ts` tick). Fixture-backed two-publisher MBO depth test over
  `mbo_btc_dual.combined.bin` (falsifiable: bypassing the floor re-emits the duplicate empty anchor).
- Installer one-liner (`scripts/connect*.sh`) now runs a **pre-flight access-pass check before
  installing anything**. Right after reading the access secret — and before installing Docker,
  pulling the image, or touching the host network — it verifies onchain that the configured identity
  has an access pass bound to this host's public IP **or** to `0.0.0.0` (the any-IP wildcard), and
  aborts with a clear, non-technical message — directing the operator to contact DoubleZero to
  arrange access, and printing the identity + public IP to share with support — if not. The check
  is pure host-side (no Docker, no CLI): it derives the identity from the
  DZ_-token/keypair, computes the access-pass PDA, and reads it over the DoubleZero ledger's public
  JSON-RPC via an embedded `python3` helper. It **degrades to a warning** (and continues, letting
  `doublezero connect` be the fallback) when the host's public IP can't be determined, the ledger
  RPC is unreachable, or `python3` is absent. New installer env vars: `DZ_CLIENT_IP` (override the
  detected public IP) and `DZ_LEDGER_RPC_URL` (override the ledger RPC).
- Shred forwarder opt-out kill switch, `--shred-forward-disable` (`DZ_SHRED_DISABLE`), **default
  off** so existing behaviour is unchanged. The forwarder is otherwise activate-on-discovery — it
  runs whenever `doublezero multicast group list` reports an `edge-solana-*` group, which a mainnet
  access pass always makes discoverable, and there was previously no way to turn it off short of
  abusing `--shred-code-prefix` to match nothing. A deployment with no consumer on the forward
  target (`127.0.0.1:20000` by default) thus silently burned CPU forwarding the full shred firehose
  into a dead port. When set, the flag forces the forwarder off regardless of discovery and skips
  the discovery shell-out to the `doublezero` CLI entirely. The activation decision is now a single
  unit-tested contract, `shred::decide_activation(disabled, source_count) -> ShredActivation`
  (`Disabled`/`NoSources`/`Run`), that `main` matches on to drive both the spawn and its
  operator-facing log line. Dockerfile one-liner examples document the opt-out.
- **Prometheus metrics endpoint** (`--metrics-bind` / `METRICS_BIND`, **off by default**). When a
  bind address is given (e.g. `127.0.0.1:9090`) the bridge serves the Prometheus text format at
  `GET /metrics` (plus a `GET /` / `GET /healthz` liveness probe) over a hand-rolled minimal HTTP
  handler — no HTTP framework, no TLS (terminate at a reverse proxy if exposed). Metrics are
  recorded regardless of whether the endpoint is enabled. Coverage spans the whole pipeline:
  ingest reception (`dz_datagrams_received_total`, `dz_datagram_bytes_total`,
  `dz_socket_errors_total`, `dz_idle_rejoin_total`, `dz_feed_up`, `dz_feed_stale_ms`,
  `dz_seq_events_total`), the arbiter emit stage (`dz_emit_total`, `dz_quotes_dropped_total`,
  `dz_trades_dropped_total`, `dz_quotes_future_rejected_total`, `dz_quotes_no_source_ts_total`),
  `dz_quotes_admitted_total` (attributing each admitted quote to its winning `publisher`,
  `edge`/`public` — the direct signal of the public backstop filling an edge gap)), the WebSocket
  sink (`dz_ws_clients`, `dz_ws_connections_total`, `dz_ws_messages_sent_total`,
  `dz_ws_bytes_sent_total`, `dz_ws_client_lagged_total`, `dz_ws_inbound_total`,
  `dz_ws_rate_limited_total`, `dz_ws_idle_timeout_total`), the public WS input feeder
  (`dz_ws_feeder_up`, `dz_ws_feeder_reconnects_total`, `dz_ws_feeder_decode_errors_total`,
  `dz_ws_feeder_messages_total`), and the shred forwarder (`dz_shred_*` —
  datagrams and bytes received per group, processed/parsed/unparsed/forwarded/dropped, verify-ok,
  no-leader, dedup tracked slots, per-destination sends and bytes sent), plus the standard Linux
  process metrics. Both the ingest and client-output paths expose message **and** byte counters
  (UDP and WebSocket). The feed-health gauges (`dz_feed_up`/`dz_feed_stale_ms`) are initialized to
  their healthy state at startup, so a feed that never goes down still exposes a `dz_feed_up{venue}`
  series for `dz_feed_up == 0` alerting. The `/metrics` HTTP server is GET-only with per-connection
  read/write timeouts and a concurrency cap. Labels are bounded (`venue`/`group`/`dest`/`publisher`
  and small fixed enums; no per-symbol labels).
- Two-publisher **Market-by-Order** depth-dedup golden `tests/fixtures/mbo_btc_dual.combined.bin` plus
  the tooling to mint it. `examples/pcap2frames.rs` `--combined-with` now supports `--protocol mbo`
  (three port roles — refdata/snapshot/mktdata — vs TOB's two, with per-publisher `SnapshotOrder`
  routing); it keeps refdata across the whole scan while windowing snapshot+deltas to `[--from,--to]`
  (so the slow-round-robin instrument definition still resolves precision), reports a window-coherence
  summary, and adds `--empty-anchor`, which synthesizes a per-publisher empty-book snapshot anchor
  (real per-instrument snapshots ride a ~30 s, per-publisher-phased round-robin and can't be captured
  coherently in a small window — see `tests/fixtures/PROVENANCE.md`).
- Shred forwarder deduplication is now selected by a single mode flag, `--shred-dedup-mode`
  (`DZ_SHRED_DEDUP_MODE`), and **defaults to dedup-only** — the forwarder now forwards exactly one
  copy of each shred out of the box, collapsing the multicast-overlap duplicates DoubleZero delivers
  across its several `edge-solana-*` groups (previously the default forwarded every copy). The three
  modes are `dedup` (default; `(slot, index, type)` dedup, **no** signature verification or RPC),
  `sigverify` (dedup + ed25519 leader-signature check, requires `--shred-rpc-url`), and `none`
  (forward every datagram). The mode is the only method selector: an RPC URL set in a non-sigverify
  mode is ignored (logged), never auto-promoting to sigverify. Replaces the boolean `--shred-dedup`
  (`DZ_SHRED_DEDUP`) flag added earlier in this unreleased cycle. `dedup`/`sigverify` share the same
  bounded `DedupWindow` (`--shred-dedup-window-slots`). ⚠️ Dedup still depends on the unvalidated
  agave shred offsets, so a misparse could over- or under-deduplicate — confirm against a captured
  frame before relying on it. The `curl … | bash` installer scripts (`scripts/connect*.sh`) now
  relay the `DZ_SHRED_*` env vars into the container, so the shred forwarder can be tuned from the
  one-liner (e.g. `DZ_SHRED_DEDUP_MODE=sigverify DZ_SHRED_RPC_URL=… curl … | bash`).
- Explicit duplicate-packet de-duplication tests across all three dedup paths. Decoded-message unit
  tests in `arbiter.rs` (an identical quote from the same source, the same BBO mirrored by two
  multicast publishers, and an identical trade all collapse to one emission); raw-packet replay
  tests in `tests/dedup.rs` that deliver every mktdata datagram twice — byte-for-byte and from a
  second publisher IP — and assert the emitted quote/trade set is unchanged; and a shred-level
  `same_datagram_twice_forwards_once` proving the second copy is dropped without re-verifying.
- Real two-sided Market-by-Order E2E fixture (#5): `mbo_{refdata,snapshot,mktdata}.bin` are now a
  live TYO recorder capture (publisher 148.51.123.3, BTC) of a complete 44,598-order snapshot
  (28,345 bids + 16,253 asks) plus contiguous post-anchor deltas, replacing the hand-crafted
  empty-anchor anchor from PR #2. `mbo_single_publisher_depth_contract` now asserts an active,
  unconditional two-sided crossed-book check (`best_bid < best_ask`). The `pcap2frames` example
  gained `--mbo-minimal` (with `--mbo-max-deltas`) to extract this minimal fixture in one command:
  the first complete snapshot group + capped post-anchor deltas + a minimal refdata. See
  `tests/fixtures/PROVENANCE.md`.
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
- Shred sigverify mode (`--shred-dedup-mode sigverify`) now **prefetches the next epoch's leader
  schedule** and **fails closed** on an unknown leader. The leader cache holds two epochs (current + next), fetched
  by explicit slot so the result is independent of rollover timing, eliminating the routine
  ~30s-per-epoch gap where new-epoch slots had no schedule. With prefetch in place, a slot whose
  leader is unknown is now **dropped** rather than forwarded unverified — sigverify forwards only
  what it can verify. Because the full current epoch is always cached, a transient RPC glitch never
  blacks out the feed; an unknown leader now means cold start, an RPC outage past the ~epoch
  prefetch lead, or a garbled schedule, and is surfaced as a `no_leader` counter in the periodic
  tally. (Forward-when-unverified is exactly dedup-only mode; sigverify no longer degrades into it.)
  Removes the now-unused `leader_known` fail-open path from `DedupWindow::decide`.
- Shred dedup-only mode (`--shred-dedup-mode dedup`) now keys its dedup window on `(slot, index, type,
  content-fingerprint)` instead of `(slot, index, type)`, so it collapses copies that match over the
  signed content. A shred sharing `(slot, index, type)` but carrying different signed content
  (equivocation, corruption, a forged first-arriver) now still forwards rather than being silently
  dropped onto the first copy — loss-averse, since without sigverify the forwarder can't tell which
  copy is valid. The fingerprint excludes the trailing 64-byte **retransmitter signature** of
  resigned merkle shreds (variants `0x70`/`0xb0`), which is rewritten per turbine path: cross-group
  copies of the same shred differ *only* there, so hashing the whole datagram would give each its own
  key and dedup none of them. Excluding that tail needs only the already-decoded `resigned` flag and
  the datagram length, not the unvalidated merkle offsets. The fingerprint is a deterministic hash
  computed only in dedup-only mode; sigverify mode is unchanged (keyed content-agnostically, since
  the signature picks the valid winner). Adds `examples/bench_dedup_vs_sigverify.rs`, which measures
  the fingerprint's marginal cost at ~135× cheaper than an ed25519 verify.
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
- Graceful container shutdown runs `doublezero disconnect` to free the access-pass session,
  but only on an operator `docker stop` (TERM/INT) and only when a tunnel is actually up —
  so a bridge crash under `--restart unless-stopped` no longer releases the session. The
  disconnect is wrapped in a `timeout` so a wedged daemon can't consume the whole stop budget.
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
- Market-by-Order manifest `Valid=0` workaround (#5): the live HL MBO publisher emits
  `ManifestSummary` with `Valid=0`, same as the Top-of-Book publisher. `MboProcessor` passed
  `m.valid` straight through, so the manifest was rejected, no instrument definition ever
  resolved, and `depth` was silently never emitted. It now overrides to valid (logged once),
  mirroring `TobProcessor`. Surfaced by the real two-sided MBO fixture below.
- `MboProcessor` no longer re-broadcasts a duplicate full-state `depth` when a book change
  leaves the published top-N unchanged (deep-book churn): it now emits only when the top-N
  actually changes, matching the documented contract and avoiding redundant WS traffic.
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
- Gated and bounded the Market-by-Order book map (`MboProcessor`). The live Hyperliquid MBO
  `FEEDS` row processes order deltas/snapshots keyed by an unauthenticated, spoofable wire
  `instrument_id`, and previously minted an unbounded `BookState` per id with no definition gate
  (unlike the Top-of-Book/Midpoint quote paths) — a strictly larger memory-exhaustion surface
  than the sequence map above, and live (not gated behind an absent feed). A forged MBO stream
  could grow memory two ways: distinct `instrument_id`s, or a flood of never-cancelled `OrderAdd`s
  for one instrument. Now a book is created only once its instrument definition is known (an
  undefined instrument can never emit `depth`); the book map is capped at `MAX_BOOKS` (4096) with
  least-recently-inserted eviction; and each book bounds its resting-order population, in-flight
  snapshot, and `Recovering` delta buffer (`MAX_ORDERS_PER_BOOK`/`MAX_PENDING_DELTAS`), dropping to
  snapshot recovery rather than growing without limit. Real feeds stay far below every cap.

## [0.1.0]

### Added
- Initial release of `doublezero-edge-connect`: ingests DoubleZero Edge binary
  multicast feeds (Top-of-Book & Trades, Midpoint, Market-by-Order), runs the
  reference-data subscriber state machine, and re-serves normalized market data over a
  WebSocket in the engine-agnostic JSON protocol specified in `PROTOCOL.md` (v1).

[Unreleased]: https://github.com/malbeclabs/doublezero-edge-connect/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/malbeclabs/doublezero-edge-connect/releases/tag/v0.1.0
