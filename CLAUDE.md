# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`doublezero-edge-connect` ingests one or more DoubleZero (DZ) Edge **binary multicast** feeds,
decodes them, runs the reference-data subscriber state machine, and re-serves normalized market
data over a **WebSocket** in an engine-agnostic JSON protocol. It speaks four edge-feed-spec
sibling protocols, each selected per feed by `FeedKind` in `src/ingest/feeds.rs`:
**Top-of-Book & Trades** (magic `0x445A` -> `quote`/`trade`), **Midpoint** (magic `0x4D44` ->
`midpoint`), **Market-by-Order** (magic `0x4444`; the bridge reconstructs the L3 book and
re-serves it both as full-state `depth` and as the order-level incremental `book`, carrying the venue's
own `order_id`), and **Market-by-Price** (magic `0x4442`; the bridge
reconstructs the price-aggregated book and re-serves it as the incremental `book`; the `lashay-2`
row selects it, on a group that is live). Each feed maps to one venue. The
input (multicast/binary) is an implementation detail; the *only* external contract is the
WebSocket output, fully specified in
**PROTOCOL.md** (v1). Any engine that speaks WebSocket + JSON consumes it via a thin adapter; the
consumer is not part of the protocol.

## Commits & authorship

- **Never attribute commits or code to Claude/Anthropic (or any AI).** No
  `Co-Authored-By: Claude`, no "Generated with Claude Code" trailers in commit messages or
  PR bodies, and no AI-attribution comments in source. Commits and code read as the human
  author's own work.

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

A WS-server task plus **one receiver task per publisher of each active feed** share a single
`tokio::sync::broadcast` channel of `Arc<FeedMessage>` (the fan-out backbone — `Arc`-wrapped so a
per-subscriber delivery is a refcount bump, not a deep clone of the message) and a `Mutex<HashMap>`
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
  `pricebook` (the price-keyed sibling of `book`), `authority`,
  `health` (per-receiver liveness aggregated to the venue-level `status`/`dz_feed_up`),
  `subscriber`, `arbiter`, `authority` + `arm_race` (the single-arm `book` gate and the cross-arm trade
  matcher its re-election runs on), the **`subscriptions`** detector + **`reconcile`** activation loop (which
  decide what runs — see Architecture above), the optional public feeders (`public_feeder`
  scaffolding + `ws_feeder`/`phoenix_feeder` venues), and the codecs (`codec`, `codec_common`,
  `codec_midpoint`, `codec_mbo`, `codec_mbp`). Intra-pipeline references use `crate::ingest::*`; this half knows
  nothing about how the data is re-served.
- **`sinks/`** — the output features, each off the hot path so one never affects another: `ws`
  (WebSocket, on by default), `api` (read-only `/v1` query API), `metrics` (Prometheus `/metrics`,
  off by default), `admin` (the one mutation path in this crate — see below) and `hyperliquid` (the
  same data in Hyperliquid's own schema, off by default). A new feature is a sibling module here +
  a spawn in `main.rs`.
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

- **`ingest/registry.rs` + `ingest/registry.json`** — the feed registry as **data**. The rows are a
  JSON document resolved once at startup from one of three sources in precedence order —
  `--feed-registry-url` (the precursor to reading the DoubleZero ledger, which drops in as a fourth
  `Source`), `--feed-registry <path>` (a bind-mounted file), else the `include_str!`-ed built-in copy
  — parsed, validated, expanded and **leaked into `'static`**, which is what keeps `FeedKey`, the
  metric labels and every `&'static str` field unchanged downstream. ⚠️ **Never add a channel roster,
  port base or group `code` as a Rust constant.** Those numbers live in a publisher inventory that
  changes without our involvement (the same allocation was decided four times upstream in dated
  specs, each reversing the last), and compiling them in makes every upstream change a rebuild while
  making a stale copy invisible — a wrong port binds a socket that stays silent. **A rejected document
  degrades or refuses depending on where it came from, and the asymmetry is the point:** a `Url`
  failure of *any* kind (unreachable, malformed, a `version` this build predates, a validation error)
  warns and falls back to the built-in copy, because a remote registry is infrastructure that moves
  underneath a running fleet and — since resolution is startup-only — refusing would not kill the
  fleet when the document changed but each process at its next reschedule, far from the cause; a host
  that is *up* serving one new field must never be worse than one that is down. A `File` (an
  operator's explicit instruction about this one container) or the built-in copy is **fatal**.
  The binary's own `clap` default for `--feed-registry-url` is empty (running from source reaches
  no network); the **image** bakes in the hosted URL as a `Dockerfile` `ENV` instead, so it stays
  inspectable via `docker inspect` and overridable with `-e` without a rebuild either way — a `File`
  source wins only when the URL is empty, so an operator combining a bind-mounted file with the
  image must clear `DZ_FEED_REGISTRY_URL` too (`scripts/connect.sh` does this automatically when
  `DZ_FEED_REGISTRY` is set and the operator hasn't set a URL of their own). `Loaded::log_resolved`
  is the one place that announces which source actually won (`"feed registry resolved"`,
  `source`/`version`/`rows`/`receivers`); `connect.sh` greps it after startup and echoes it, since
  the URL-failure fallback above is silent by design and that log line is the only signal.
  There is deliberately **no `deny_unknown_fields`** anywhere in the module — same rule as
  `doublezero-edge/src/types.rs` — so an additive upstream change is ignored and reported by path at
  `warn`, never rejected; a *missing required* field stays fatal, so a typo cannot quietly default.
  Validation covers the structural per-row rules (non-empty `code`/`category`, a `venue` that
  `sources::source_id_of` resolves, base ports unique within a row, a non-empty roster, no port
  overflow, and a **port shape matching the protocol** — `MarketByPrice`/`MarketByOrder` bind three
  planes, `TopOfBook`/`Midpoint` two, which is what turns a misspelled optional `snapshot` key from a
  silently two-port block whose book never syncs into a startup error) **and the four cross-row
  invariants** — `(venue, category, kind)` uniqueness,
  one arbitration mode per **venue** (the granularity `Arbiter::set_mode` keys on, so disagreement
  cannot resolve last-write-wins by document order), `emit_trades` agreeing with
  `reconcile::tape_rank_is_some`, and global `(group, port)` uniqueness. Those four used to be
  `#[cfg(test)]` assertions over the built-in document, which stopped being sufficient the moment a
  document could be supplied at runtime. The rest is upstream policy this process cannot verify.
  `publishers` is a tagged union: `explicit` lists port blocks verbatim, `derived` carries a channel
  roster plus per-plane bases and expands to `base + channel_id` on every plane (one channel is an
  independent state machine — its own `Reset Count`, sequence series, manifest seq and snapshot cycle
  — so one channel is one `FeedPublisher` and one receiver task); a roster that repeats an id
  collapses it and says so. There is **no hot reload**: books and reference data are keyed to the
  topology in effect when they were built.
- **`ingest/feeds.rs`** — the registry **types** plus the one accessor. `feeds()` returns the rows
  installed by `feeds::init` (called once from `main` before any receiver spawns); the backing
  `OnceLock` is deliberately **not** `pub`, so a consumer reading it directly is a compile error
  rather than a silently-missing row. Each `Feed` is one multicast group mapped to one
  venue, with a group `code` (`tiredsolid`/`scottsdale` — the identifier `doublezero status` reports,
  matched by the reconciler), a `FeedKind` (which protocol) and **N `FeedPublisher` rows**, one per
  publisher mirroring the feed, each with its own `FeedPorts` block (`TwoPort` for TOB/Midpoint, or
  `ThreePort` adding a snapshot port for MBO). One receiver task runs per publisher. A publisher's
  identity is its **base port** (`FeedPublisher::base_port()`, the block's mktdata port) — the
  `publisher` metric label, the log field and the reconciler/health task-key component; there are
  deliberately no host names in the registry. Add a row to the **document** to ingest another venue
  (sibling-protocol rows are added once their live endpoints are known). `--feed
  <venue>` selects a subset of venues and `--publisher-port <port>` narrows the publishers within
  each (base ports are unique per feed, **not** across feeds); consumers then filter by venue over
  the WS. `emit_trades` is a static **capability** claim only — which claiming row actually serves a
  venue's tape is the reconciler's runtime decision (see `reconcile.rs`), because a venue's rows can
  ride separate groups with separate codes. The two **Lashay** perps rows (`lashay-1` TOB
  `233.84.178.3:31000/41000`, `lashay-2` MBP `233.84.178.4:32000/42000/52000` — **all three rows'
  ports confirmed against live captures on 2026-08-09**, after all three were found misprovisioned
  (see the port-provenance note in `registry.json`) — both `Sticky`, both
  claiming the tape) are exactly that case. Both groups are **live and activated** (testnet and
  mainnet, confirmed 2026-08-07), so a host subscribed to either code activates the row. ⚠️ A `code`
  that does not match its live group fails **silently** — no warning, no failed bind, just a
  permanently-zero `dz_receiver_up`. ⚠️ **No test can catch that**, and one used to claim it did:
  `feeds::tests::the_lashay_rows_expand_consistently_with_the_document` (formerly named
  `..._match_the_deployment`) asserts the document against literals written beside it — the code
  agreeing with itself. It catches a later edit that moves a value, never a value that was wrong when
  written. On 2026-08-09 **all three rows** were found on ports no publisher sends to, each carrying a
  sibling row's block, with the build green throughout. **The external check is a packet capture**, and
  the procedure lives in `registry.json`'s `PORT PROVENANCE` block; run it whenever a row is added or a
  port moves. A third **Lashay** row
  (`lashay-4`, group `233.84.178.20`, MBP, `Sticky`, claiming the tape) carries a *disjoint* universe
  under the same Source ID — hence its own `category` — and is the one `derived` row: 31 channels
  (ids 10-29, 39-48, 49) expanded to `34000`/`44000`/`54000 + id`, confirmed against the publishers'
  own deployment inventory on 2026-08-09. `33000`/`43000` is the **top-of-book sibling's** base, and a
  market-by-price row built on it joins the right group and receives nothing, which reads as a dead
  publisher rather than as a misconfiguration — this row carried those bases until that check. A
  second publisher takes the same roster with every id offset by **+100** and the ports untouched: the
  port names the league, so a subscriber binds one socket per league and hears both publishers on it,
  while `channel_id` names the league *and* the publisher so the two are sequenced separately. Never argue that the
  universes are separate because their `channel_id` ranges do not overlap — the allocation is
  mid-migration, the numbering is owned upstream and nothing here enforces it; the `category` is the
  only thing that separates them. Each expanded `FeedPublisher` records the `channel` it came from
  (`None` on an explicit block) — the fact the channel filter below is built on.
- **`ingest/channel_filter.rs`** — the **channel filter**: which channels of an activated feed this
  process decodes (`--channels`/`DZ_CHANNELS`, e.g. `lashay-4=10,11`). It is an **allowlist**, not a
  threshold — the old name, "floor", implied a minimum, which is backwards: it restricts a set, and
  every unmentioned feed keeps admitting everything. Sits between the two filters either side of it
  — `reconcile` picks which *feeds* run, `sinks::ws`'s `SubFilter` picks what one *client* receives —
  and is process-global and ops-owned, since it scopes books, history and CPU for everyone. It acts
  **only** where the publisher derives a port per channel: an excluded channel is never bound and the
  kernel discards its traffic before userspace, which is why it costs nothing and is the reason that
  derivation exists upstream. Narrowing a row whose publishers bind one base port **flat** is
  **refused at startup**, not implemented as a frame-header test: there `channel_id` identifies
  mirrors, so every publisher carries the complete universe and narrowing would give up redundancy
  without reducing a single decoded message. Keyed **by group code, never globally** (one global flag
  would filter to a league and silently half-blind an unrelated mirrored feed), and takes **numeric
  ids only** — channel *names* live in the publisher's inventory by design and a copy here would
  drift exactly as four superseded port allocations did. Every id is validated against the **loaded
  document's** roster and an unknown id or code is fatal at startup, because a channel filter that
  silently filters nothing is worse than one that refuses to start. Parsed once in `main`; the
  reconciler consumes it as an *input* to the desired receiver set (`feed_keys`), so `reconcile`
  remains the single activation authority and its spawn/abort diff is unchanged. **Two mechanisms,
  one validator:** `--channels`/`DZ_CHANNELS` only sets the *initial* value of a shared
  `Arc<Mutex<ChannelFilter>>`; `sinks::admin`'s `POST /admin/channels` can replace it at runtime
  through the identical `ChannelFilter::parse`, so nothing reachable after startup can admit a row
  startup itself would have refused.
- **`ingest/sources.rs`** — the only mirror of the upstream **Source ID registry**
  (`edge-feed-spec/sources/spec.md`), which is what names a venue: the wire Source ID is
  authoritative and a publisher stamping the wrong one is a publisher defect fixed upstream, never
  remapped here. Three production IDs are assigned. Names are **uppercase** because that is the form
  that reaches consumers — `venue`/`source` on the WebSocket, every `venue=` metric label value, and
  the `SOURCE:SYMBOL` product identifier a consumer composes from them. `source_name` is the one
  emitted name per ID; `source_id_of` additionally accepts a **legacy name** for ID 3, so operator-
  and ledger-facing strings predating the current registry name keep resolving to the same ID
  instead of falling through to `None` — only the registry name is ever emitted. A `venue` in `FEEDS`
  **must** be a name
  `source_id_of` resolves, or `receiver::record_revealed` silently drops it and the row's `status`
  stream goes unrecorded. An unassigned ID gets a stable synthesized `SOURCE_<id>` (distinct per ID,
  since the arbiter keys dedup on `(venue, symbol)`), bounded by `MAX_UNREGISTERED_SOURCES`.
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
  (spawn/abort). The desired receiver set is the subscribed rows' publishers **narrowed by the
  channel filter** (`ingest::channel_filter`) — an input to the set, not a second authority. Owns all `JoinHandle`s; teardown is `abort()` (clean — sockets close on drop). Reaps
  finished handles so a died feed respawns. Fail-open / `--subscription-gating-disable` route through
  one `static_desired()`. Also the **trade-tape row owner**: `tape_owners` ranks the running receivers
  per `(venue, category)` — one owner per *universe*, since rows sharing a Source ID can carry
  instrument sets that mirror nothing and a venue-wide rank mutes the loser's tape outright —
  (`TopOfBook` over `MarketByPrice`, base port breaking ties; MBO/Midpoint never rank) and
  `apply_feeds` publishes the result onto a `TapeOwner` (`Arc<AtomicBool>`) each processor reads per
  print, so ownership moves **without a respawn** — a respawn would drop a healthy publisher's books
  and reference data whenever a *peer* feed's subscription changed. The flag is stored **with** the
  `JoinHandle` in one `active` map so it cannot outlive its receiver on either the abort or the reap
  path, and ownership is ordered **liveness before rank** (`FeedHealth::liveness` → `TapeLiveness`,
  three states: a subscribed-but-dead row must not hold the tape while its peer decodes prints and
  drops them, and a row that never *registers* must not either — registration follows the socket
  bind, so a row that can never bind respawns every tick and, ranked as live, would hold rank 0
  forever and mute the venue. `Unregistered` therefore sits **between** `Up` and `Down`: below a live
  row, so an incumbent keeps the tape until the newcomer really registers rather than bouncing on
  activation; above a dead one, so a cold start where nothing has bound falls back to rank instead of
  leaving the venue ownerless). Changes are counted by `dz_tape_owner_changes_total{venue}`.
  `forget_departing_channel` is the other half of a channel-filter narrowing (static at startup, or via
  `sinks::admin`'s `POST /admin/channels` at runtime): once a channel's receiver key actually leaves
  `plan()`'s desired set — never before, per the `N1` `debug_assert!` guarding reordering — it purges
  that departing row's own `(venue, category, channel)` slice from all three: the catalog
  (`InstrumentSnapshot`, a `retain`), the book (routed through `Arbiter::forget_channel_books`, never
  a direct replay-map delete, so the accumulator/replay/`StickyAuthority` triple drops together), and
  `history::Store::forget_channel`. Category-precise throughout so a departing channel can never
  over-drop a live peer universe sharing the same numeric id — see `ingest/feeds.rs`'s
  mid-migration warning on `channel_id`.
- **`ingest/receiver.rs`** — the ingest hot path. All socket plumbing is **protocol-agnostic and shared**:
  `bind_multicast`, `recv_with_ts` (kernel timestamps), `wait_for_interface_ip`, the `IDLE_REJOIN`
  watchdog, `emit_status`, and `SeqTracker`. `drive()` is a generic receive loop over **N ports**
  (1/2/3) that hands each datagram to a `FrameProcessor` via a `FrameCtx`; `run_feed()` picks the
  processor + port roles from the feed's `FeedKind`. One `drive()` task serves **one publisher** —
  its own port block, its own processor state (and so its own MBO books) — and reports liveness under
  its base port via `ReceiverRegistration`, which registers only after the sockets bind (a
  never-binding receiver would otherwise flap `status` on every reconciler respawn) and deregisters
  on every exit path via `Drop`. The watchdog tracks the **mktdata** port only (refdata/snapshot keep
  ticking when market data is wedged). `FrameCtx` carries the shared `arbiter` (not a raw `tx`);
  `ctx.emit(msg)` routes through it tagged `Publisher::Edge(src_ip)`.
- **`ingest/health.rs`** — `FeedHealth`: every receiver's liveness keyed `(venue, category, kind, base port)`
  (the same tuple as `reconcile::FeedKey`; `(venue, kind)` is not an identity once a venue carries two
  universes),
  aggregated to the **venue**-level `status`/`dz_feed_up` PROTOCOL.md promises, so one wedged
  publisher never takes a venue down while a peer streams. Only quote-bearing kinds count
  (`carries_venue_status`; MBO is depth-only and must neither declare an outage nor mask one), with a
  fallback to any registered receiver for a venue this process runs **no** quote-bearing receiver for
  — gated on a sticky per-venue carrier set, since a carrier that *stopped* leaves the liveness map
  and must not hand the aggregate to a depth-only peer. The venue edge is computed and published
  inside the lock (`with_edge`), so two receivers crossing opposite edges can't publish out of order.
- **`ingest/arbiter.rs`** — the shared **pre-broadcast emit stage** every ingest source funnels
  through. `Arbiter` owns the broadcast `Sender` plus the dedup state — the per-`(venue, symbol)`
  latch-to-leader `StalenessFloor` for quotes (keyed on `QuoteId`, the canonical BBO fixed-point, with
  the `Publisher` enum as the per-tick leader identity), a **second `StalenessFloor` for MBO `depth`**
  (keyed on `DepthId`, the top-N book content at canonical `10^-8` fixed-point; both ids use `i128`
  so an `f64→int` saturation can't collapse distinct huge values, #66), and the
  `WindowedDedup` on `trade_id` for trades — and exposes one `emit(msg, publisher, category)` (quotes → quote
  floor, depth → depth floor, trades → the per-`(venue, category)` **tape leader** then the window, `book` → the
  single-arm authority gate below, `Instrument` → a rate limit on the precision pair per
  `(venue, symbol)` so mirrored publishers' identical refdata bursts collapse but unchanged content
  is still re-announced every `INSTRUMENT_REANNOUNCE_NS` (`dz_instruments_dropped_total`);
  `Midpoint`/`Status` are the only passthroughs); a surviving message is
  broadcast as `Arc<FeedMessage>` (a per-subscriber delivery is a refcount bump, not a deep clone).
  Every arm returns an
  `Admit<Publisher>`: `Emitted{opened_tick}` broadcasts and bumps the admitted/winner counter —
  plus, when the sample *opened* its `source_ts` tick, the once-per-tick
  `dz_quote_ticks_won_total`/`dz_depth_ticks_won_total` (the published win-rate primitive:
  `edge/sum`; every tick scores exactly once, walkovers included — see docs/metrics.md) —
  `Contest{winner, lead_ns}` drops the losing cross-source copy and records the head-to-head
  lead-time histogram (`dz_quote_lead_ns`/`dz_trade_lead_ns`/`dz_depth_lead_ns`, #60 — a *margin*
  diagnostic, not a win rate: one contest slot per tick, in-tick losers only), `Dropped` is a
  plain collapse. The **tape leader** (`tape_leader`, keyed `(venue, category)`, `Sticky` venues only —
  the `category` `emit` parameter is the feed row's, never a wire field: PROTOCOL.md is a consumer
  contract and this is producer-side keying) is the arm-level twin of
  the reconciler's row ownership, and both are needed before the `trade_id == 0` bypass is sound: a
  sticky venue's arms share no trade-id space (one may stamp the sentinel while its peer stamps a real
  venue id — a pair neither the sentinel latch nor `WindowedDedup` collapses), so the gate is
  **id-independent**. Four rules: first arm to print leads (a TOB-only deployment carries no `book`
  traffic, so `scope_leader()` is `None` forever and electing first would mute the tape); an arm the
  authority *tracks* displaces one it does not (the slot is first-come on an unauthenticated wire);
  the book-*elected* arm takes over, **once per election** rather than per print (arm identity is
  shared across a venue's rows — one source IP per publisher host, both protocols — and re-honouring
  per print would let a nearly-dead elected arm sawtooth the tape away from the healthy peer); and a
  silent incumbent yields after `NO_ID_TAPE_HANDOVER_NS`, marking the election it overrode as spent.
  The peer's prints are dropped on their own `dz_tape_arm_dropped_total` (not folded into
  `dz_trades_dropped_total`, whose steady state here is the challenger's whole stream); transfers are
  `dz_tape_arm_transfers_total`. ⚠️ Two residual limits, both inherited from the unauthenticated
  wire: on a venue with no `book` traffic the authority tracks nobody, so a forged source printing first
  holds the tape until it goes quiet for a window — the same primitive `StickyAuthority::admit`'s
  no-dark-start already exposes for `book` — and the gate spans a whole **category**, so rows sharing one
  that *sharded* prints rather than mirroring them would lose the non-serving arm's fills (giving them
  distinct categories is the registry's job). The two `books` lookups inside it read the authority at
  the same `(venue, category)` grain, so the deferral can only ever name an arm elected on *this*
  universe. `no_id_owner` is skipped entirely
  for `Sticky` venues: it is the `Coordinated` guard, and it cannot see a gate-approved handover.
  `emit` increments **pre-resolved per-venue metric children** (cached in the
  `Arbiter`, mirroring the receiver's `SeqEvents`) instead of a per-message `with_label_values`
  label lookup.
  Wrapped `Arc<Mutex<Arbiter>>` (`SharedArbiter`) so the multicast receivers and the WS feeder share
  **one** floor per `(venue, symbol)` and race on it. The wire `venue`/`symbol` are `Arc<str>`
  (venues interned via `model::venue_arc`), so building the dedup key allocates nothing in steady
  state. The quote floor lived inside `TobProcessor` under
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
  venue that restarts its clock below the latched high-water doesn't wedge depth forever.
  `EndOfSession` also drops the **receiving publisher's** books to `Recovering`
  (`book.rs::on_end_of_session` — sequences, buffered deltas and event clock discarded). ⚠️ That
  reset is per-publisher while the floor it clears is venue-wide: one processor per receiver task
  means a mirror that loses its own `EndOfSession` datagram keeps a `Synced` book and can re-latch
  the cleared floor at the old high-water, wedging the venue's depth until it resets on its own.
  Closing that needs a per-venue session epoch shared across the tasks (not built).
  `on_instrument_reset` likewise drops
  `last_event_ts`, scopes its clear by the symbol the depth was emitted under (the processor's
  `emitted_symbol` memo — immune to an id→symbol remap), and falls back to a venue-wide clear when
  nothing resolves. Both resets also purge the matching WS-replay `depth` entries (no ended-session
  book replayed to a new client). The *quote* floor is
  deliberately exempt (TOB `source_ts` is epoch block time, monotonic across sessions). `Status`
  routes straight to `sender()` (no business identity to dedup).
  The `Book` arm has **two** gates, chosen by what the wire supplies rather than by venue name. A batch
  carrying a non-zero `order_id` (or for a market whose `BookMarket::order_level` memo says an earlier one
  did — that covers a bare `clear`) on a `Coordinated` venue is **raced per venue event**; everything else
  takes the single-arm authority below. Routing on *content* rather than on a report is deliberate: an
  evicted market re-routes on its next order-level batch instead of reverting to the authority for good,
  and a forged sync report cannot divert a price-aggregated market off that gate. Racing keys on
  `OrderEvent` = `(order_id, action, price bits, size bits)` per market — never `per_instrument_seq`,
  which is per publisher (measured: three unrelated bases for one execution). **`size_bits` is part of
  the identity, not content compared afterwards**: successive partial fills of one order share id, action
  and resting price and differ only there, so omitting it would collapse the second fill as a duplicate
  and leave every consumer holding a quantity the venue already reduced. A partly-duplicate batch is
  republished **filtered** to its surviving changes, because a change carries an order's absolute quantity
  and re-delivering a stale one walks the consumer back.
  **`MarketEvents::resting` is the resurrection guard, and this is the only scope that can hold it**
  (`dz_book_resurrections_dropped_total`): a `size == 0` event tombstones the order and any later change
  for it is refused, because a lagging peer's *first and only* copy of an `Add` for an order another
  publisher already killed passes every check `book.rs` makes — that book legitimately still holds the
  order. Tombstones are bounded by `MAX_SEEN_ORDER_EVENTS` and deliberately **not** by the time window,
  which is what keeps `--arb-book-dedup-window-ms` a cost knob. `dz_mbo_arm_disagreement_total` is the
  drift observable and is a *monotonic* check — a resting order only shrinks, so a publisher claiming
  **more** than a peer already reported has missed a fill; an interleaved race, where an arm delivers the
  smaller remainder first, is not counted. Both copies are still published rather than preferring the
  smaller: the peer could be the drifted one, and dropping the larger would let a forged small size mute a
  real publisher's order. A `Clear`-led batch never races — it is published only when no peer is **both**
  synced and recently on the wire (`PEER_SERVING_NS`). The flag alone would let a departed publisher
  suppress this product's only self-heal forever; requiring a delivery also settles the both-recovering
  race, since whichever arm publishes first records one. A session boundary restarts the venue's id space,
  so `reset_book_events_for_market` clears the tombstones — narrower than `reset_book_for_market` on
  purpose, since a peer that missed the session end is still serving that market.
- **`ingest/authority.rs` + `ingest/arm_race.rs`** — the **single-arm gate the arbiter's `Book` arm runs**
  for a price-aggregated market, in *both* arbitration modes with no `mode_for` branch (#105): one
  `source_ts` tick can hold several
  deltas, so the quote floor's per-tick latch would interleave two arms inside one logical event, and the
  arms' per-instrument delta sequences are unrelated by construction — a consumer's book corrupts while
  every sequence check the producer ran still passes. One arm serves a market and the peer is ingested and
  dropped (`dz_book_dropped_total`). The gate is keyed on `(venue, category)` — `authority::ScopeKey`,
  which also prefixes `MarketKey` — never on the venue alone: one Source ID can carry universes that
  mirror nothing, and a venue-wide election drops the losing universe's whole book stream permanently
  (the only escape is leader silence, and a leader streaming its own universe is never silent).
  **Speed and silence are per arm, across that arm's universe; health is per market**. The arm-ordinal
  cap (`MAX_LABELLED_ARMS`, 8) is likewise **per scope**, so a venue carrying N universes admits up to
  `8 x N` arms — the forged-flood bound still holds per universe, which is the grain that decides
  anything, and the metric label set stays `{venue, arm}` (`dz_arm_markets_held` sums a venue's
  universes; two universes' `arm0` are different publishers)
  (`Arbiter::set_book_health`, the seam the MBP processor calls on a `PriceBook` status transition) and
  overrides the elected arm for that market alone. Tunables are the `--arb-*` flags (see docs/metrics.md).
  **Anything but "the arm that last reached the wire for this market" re-baselines the consumer**: a
  serving-arm change (margin, silence, or that health override), a market's first admission, or a market
  whose state was evicted. The re-baseline is a `clear` plus the new arm's complete current level set,
  `snapshot`/`last` true — so the gate accumulates **every eligible arm's** book (`BookMarket::arms`), not
  only the serving one. Three constraints shape it: it is emitted lazily on that arm's next *completed*
  logical event, never as a venue-wide burst of clears (most markets are idle, and `to_book` of a
  half-applied event goes out stamped `last` as a torn book); the wait is **bounded**
  (`MAX_WITHHELD_BATCHES`) because `last` is a promise made by an unauthenticated producer; and it
  degrades to a **bare `clear`** unless `BookAccumulator::baselined` holds — an arm that has sent no
  producer re-baseline holds only the levels that moved since it started accumulating, and publishing
  that as `snapshot` would tell the consumer to discard the rest. The replay map mirrors the serving
  arm's accumulator, that flag included, so `sinks/ws.rs` skips exactly the markets this cannot
  re-baseline.
  Per-market state is capped by `MAX_BOOK_MARKETS` (`dz_book_markets_evicted_total`) and eviction drops
  the accumulators, the replay entry **and** `StickyAuthority`'s own per-market entry together — that
  pairing is what makes eviction safe rather than a corruption primitive, since losing `last_admitted` is
  what forces the re-baseline. All three are keyed at the **same grain** (`MarketKey`, scope included);
  a narrower replay key would break exactly that pairing, because two universes with independent id
  spaces can share a `(channel, instrument_id)` — evicting one would delete the other's *live* entry
  while its `last_admitted` survived, so it would never re-baseline and would stay invisible to new
  clients (`arbiter::tests::evicting_one_universes_market_spares_the_others_replay_entry`).
  `sinks/ws.rs` and `sinks/api.rs` skip the scope element: it is a producer-side key that never
  reaches the wire. `reset_book_for_market` is the session-reset seam (no venue-wide variant: the MBP
  processor scopes `EndOfSession` per arm and channel, handing those markets to the peer). `arm_race` is the **only** producer of the matched-lead
  samples the speed re-election consumes: it pairs the two arms' copies of the same **trade** by content
  (`((venue, category), symbol, price bits, size bits, aggressor)`, FIFO per signature so identical
  repeats pair in order) and measures the gap on our own `recv_ts_ns`, never a publisher-stamped time.
  Pairs form **within one scope**: it is the sole producer of the election's evidence and that election
  is per universe, so a cross-universe pair would mint a foreign publisher into a scope's admission
  slots and could elect an arm serving no `book` there. Trades only — a
  level update's cross-arm-common fields recur on a coarse price grid and would mis-pair constantly — and
  **edge arms the authority already tracks only** (`Arbiter::race_eligible`): the public backstop decodes
  from parsed JSON and serves no `book`, and an untracked publisher would spend one of the scope's eight
  admission slots. `dz_arm_lead_ns` is fed exclusively from those pairs, never from a dropped copy's
  `Admit::Contest` lead (that is inter-arm phase against an unrelated earlier message, and structurally
  non-negative). The only `FEEDS` row of that kind is `lashay-2`, whose group is live, so these series
  populate on any host subscribed to it — and report nothing on a host that is not.
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
  `MboProcessor` (feeds order deltas + the snapshot stream into `book.rs` and emits full-state `depth`,
  the order-level `book` and trades — `emit_book` mirrors `emit_depth`'s gates exactly, and each book's
  sync state is reported to the arbiter *before* the frame's emissions so the re-baseline suppression has
  a truthful view; a gapped book must report `false` or a recovering peer sees a phantom healthy arm and
  suppresses the only re-baseline on offer), `MbpProcessor` (feeds level deltas + the snapshot stream into `pricebook.rs` and emits the
  incremental `book` + trades). All gate emission **per instrument** on a known definition (precision before price). The
  quote/trade/depth cross-source dedup is **not** here anymore — it moved to `arbiter.rs`.
  All three hold their `RefDataState` in a shared `PerPublisher<D>` map keyed on the datagram source
  IP and bounded by `MAX_PUBLISHERS` (#97): `reset_count` is per `(source_ip, group, port)`, so under
  a shared port block one publisher's restart would otherwise clear every publisher's definitions and
  blank the venue. Reads take the **non-inserting** `PerPublisher::def`; only the refdata handlers use
  the inserting `get`, so a forged-source market-data flood can't evict a real publisher's definitions.
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
  `MbpProcessor` keys its books on **`(publisher, channel, instrument)`** — two arms mirror one feed
  with unrelated per-instrument delta series, and one group is sharded across channels whose state
  machines are independent — and carries the design's cross-instrument/cross-publisher conformance
  items: `SnapshotLevel` routes by the **open group** per `(publisher, channel)` (never by
  `snapshot_id`, which is monotonic per `(channel, instrument)` and so collides across instruments
  within a rotation), a **cross-instrument** buffer budget (`MAX_BUFFERED_DELTAS_ACROSS_BOOKS`, 2^20;
  overflow drops the largest instrument's buffer and marks it `Gap` rather than taking the channel
  down — `pricebook`'s per-book cap is a quarter of it, so the budget only binds with several heavy
  books), `EndOfSession` scoped to the emitting `(publisher, channel)` (the order-keyed handler's
  venue-wide clear would tear down a live peer arm's published book), and a channel reset on any
  **change** of the frame header's `Reset Count` (`!=`, never `>`, so the `255 -> 0` wrap counts) —
  read from the **market-data role only** and only for a publisher whose reference data we already
  hold: the three ports carry one epoch on three sockets, so a memo shared across them would re-reset
  the channel on every interleaving of a restart's backlog, and minting reference-data state from the
  market-data path is what would let a forged-source flood evict the real publishers' definitions. A
  snapshot group whose epoch disagrees with the market data's is refused for the same reason — it
  belongs to the publisher's previous run, and installing it would republish a dead session's book.
  `buffered_total` is a running total maintained by the single `with_book` seam so the budget check is
  O(1); a test recomputes the true sum after every mutation path. Per-market `Ready` transitions are
  reported to the arbiter's `StickyAuthority` (`set_book_health`), which is what fails a gapped arm
  over to its peer. A price-bounded `BookClear` publishes the **exact levels it removed** (reported by
  `PriceBook::on_delta` through a reused buffer): the wire `Clear` carries no price bound, so a
  whole-side clear would tell the consumer to drop levels this book still holds. Its
  `ManifestSummary`/`InstrumentDefinition` arms are `handle_refdata`-gated exactly like the three
  siblings' — decode does not care which physical port a type arrives on, so without it one forged
  datagram on the market-data or snapshot port clears a publisher's definitions and every emission
  path (which all gate on a resolved definition) goes dark for the venue — and it drains
  `PerPublisher::take_evicted` into `forget_publisher`, which drops the evicted publisher's books,
  `revealed`/`announced_symbol` entries and per-channel snapshot/reset memos together.
- **`ingest/codec.rs` / `codec_midpoint.rs` / `codec_mbo.rs` / `codec_mbp.rs`** — pure decoders for each protocol's
  little-endian fixed-size frames, all built on `ingest/codec_common.rs` (shared 24B frame header, 4B
  message header, LE readers, `cstr`, and the generic `decode_frame_with(magic, ...)` walker).
  **`codec.rs` (TOB) offsets are validated byte-for-byte** against the authoritative Go decoder in
  `edge-multicast-ref` — **do not change them without re-validating**. **`codec_mbo.rs` is now
  field-by-field validated too (#4):** shared-with-TOB types reuse the byte-validated TOB layout,
  and the MBO-specific types are pinned by offset-independent unit tests plus a real-frame decode
  test over the byte-validated committed golden fixtures (`tests/codec_mbo_fixtures.rs`). ⚠️
  **`codec_midpoint.rs` offsets still come from the edge-feed-spec draft and are NOT
  reference-validated**; its round-trip tests only pin self-consistency, so validate against a live
  frame hexdump before trusting its output (see "Conventions" below). **`codec_mbp.rs`
  (Market-by-Price, magic `0x4442`, #95)** is validated field-for-field against the Go decoder **and
  against two committed real captures** (`tests/fixtures/mbp*.bin` — a sharded multi-channel set and
  a dense single-channel set, `tests/codec_mbp_fixtures.rs`); four types absent from both captures
  stay offset-test-only — and the `lashay-2` group is live, so those four now decode against real
  traffic for the first time. It is the one codec that enforces **exact** body-length
  equality per type rather than bounds-checked reads (`SnapshotBegin` is a prefix-superset of MBO's,
  so a lenient decode would
  read `depth_bound` — whose `0` claims a *complete* book — from whatever follows the body), and
  therefore also the one that rejects an unimplemented `SCHEMA_VERSION` itself rather than leaving
  it to the shared walker: without that gate the length rule would silently reject a v2 frame whose
  bodies legally grew, and the whole feed would decode to `Other`.
- **`ingest/book.rs`** — `BookState`: per-instrument L3 order book + the MBO snapshot+delta recovery state
  machine (`Synced`/`Recovering`), using the per-instrument delta sequence and snapshot anchor.
  Codec-agnostic (`DeltaOp`/raw ints) so it's unit-tested in isolation; derives top-N `depth` **and**
  the order-level changes the `book` product is built from — `on_delta_reporting` appends one
  `OrderChange` (raw ints, absolute resulting quantity) per applied event and nothing for one it
  rejects, and `order_set` materializes the whole book, deterministically ordered so two publishers'
  copies compare byte-for-byte. The removed-order set (`MAX_REMOVED_ORDERS`,
  `dz_mbo_removed_evicted_total`) is **defence in depth, not the cross-publisher guard**: one book sees
  one publisher's stream, where the sequence check already rejects a repeat, so what it catches is a
  forged `Add` re-using a dead id at a contiguous sequence. The racing guard is at the merge point
  (`arbiter.rs`), the only scope with the shared identity to see a *peer's* delete. A refused `Add`
  still consumes its sequence number, or the next contiguous delta would read as a gap. Session and
  instrument resets, and a snapshot install, all clear the set: each is a fresh id space.
- **`ingest/pricebook.rs`** — `PriceBook`: per-instrument **price-keyed** book + the market-by-price
  snapshot+delta recovery machine (`AwaitingSnapshot`/`BuildingSnapshot`/`Ready`/`Gap`). A **sibling**
  of `book.rs`, not a reuse: the wire is already price-aggregated and each level carries its absolute
  resulting quantity, so `Action` never gates the apply (quantity alone decides) and the
  `Add`/`Cancel`/`Execute` vocabulary does not apply. Reports the levels a `BookClear` removed through
  a caller-supplied buffer, since the wire `Clear` has no price bound.
- **`ingest/subscriber.rs`** — `RefDataState<D>`, the reference-data state machine, **generic over** any
  instrument-definition type implementing `InstrumentDef` (its id + manifest seq), so all three
  protocols reuse it. Collects definitions tagged with the latest `ManifestSummary` seq; `ready()`
  (true once `defs.len() == expected_count`) reports when the *whole* set is known. **Emission gates
  per instrument, not on `ready()`**: a processor emits as soon as `definition(id)` resolves, so
  consumers never see a price before its precision, but a single symbol flows without waiting for
  the full set (an all-or-nothing gate could wedge the feed on a startup/reset race). Uses
  wraparound-safe u16 sequence comparison (`is_later`). One instance tracks **one publisher** — the
  per-source-IP map lives in `processor.rs` (`PerPublisher`), keeping this state machine
  single-publisher and unit-testable.
- **`sinks/ws.rs`** — fans the broadcast out to clients (on by default; disable with an empty
  `--ws-bind`). **Serializes each message once, not per client:** a single serializer task reads the
  `Arc<FeedMessage>` backbone, stamps one shared `ws_send_ts_ns`, renders the JSON once, and
  re-broadcasts a ready-to-write `Arc<PreparedFrame>`; each client task only filters and writes a cheap
  `Utf8Bytes` clone (so N clients cost one serialization, and `ws_send_ts_ns` is one instant shared by
  all consumers of a message — see PROTOCOL.md). With no clients connected the serializer skips the
  work. On connect it replays the instrument snapshot (precision first) **then the latest
  `depth` per symbol and each market's accumulated `book` re-baseline** (both full state, the `book` one
  materialized from the serving arm's `BookAccumulator` and scoped by the `channel` filter dimension like
  every other dimension), then streams quotes/trades/midpoints/depth/book. Replay is one
  `replay_scoped()` used twice: unfiltered on connect (no subscriptions yet), then per `subscribe`
  scoped to the filter just added, so a client that narrows after connecting is bootstrapped without
  replaying every market. Implements the
  PROTOCOL.md v1 surface: optional per-client subscribe/unsubscribe filtering (empty filter list =
  firehose) over four dimensions — `venue` (case-insensitive), `symbol`, `channel` and message
  `type` — through **one** `SubFilter::matches` that both the symbol-bearing and the venue-level
  (`status`) paths call, so a new dimension can't silently exempt half the stream; a channelless
  message is excluded by an explicit `channel` filter, with `status` (venue-level) the one carve-out —
  `instrument` carries its own channel and is filtered like `book`, including on the replay path. Plus app ping/pong + server WS-ping heartbeat with idle-timeout reaping, and the limits
  (max clients/subs/inbound-rate, broadcast backpressure where a slow client drops oldest). The
  listener is bound via `ws::bind()` (separate from `ws::serve()`) so the reconciler can treat a bind
  failure as non-fatal — a taken port disables the sink but leaves the tunnel running — and activate
  the sink only once a market-data feed is subscribed.
- **`sinks/hyperliquid.rs`** — the **Hyperliquid-compatible** sink (`--hl-ws-bind`, off by default,
  **not** subscription-gated): the same broadcast re-served in *Hyperliquid's* schema so an existing
  Hyperliquid client needs only a URL change. A **rendering, not a second pipeline** — no ingest state,
  no dedup identity, no arbitration input — so it is documented in `docs/output-sinks.md` and **never**
  in PROTOCOL.md, which is the contract for our own protocol only. Scoped to `VENUE = "HYPERLIQUID"`
  (`coin` is our `symbol`), with `bind()` split from `serve()` like `ws`'s. Three channels off the
  order-keyed `BookAccumulator` in the shared `BookSnapshot`: `l2Book` (snapshot-per-update from
  `price_fold()` — whose per-level order count is the whole reason the accumulator is order-keyed;
  honours `nSigFigs`/`mantissa`/`nLevels`), `l4Book` (`to_book(ReplayScope::Orders)` as the publisher's
  externally-tagged `{"Snapshot":…}`, then `{"Updates":…}` order diffs) and `trades`. Two references
  define the wire and one rule settles them: **NautilusTrader v1.227.0 wins for anything it parses**
  (`l2Book`/`trades` field names and types, the `B`/`A` side spelling, `{"channel":"pong"}` for its
  30s `{"method":"ping"}`), and the **DZ Hyperliquid publisher** (`malbeclabs/hyperliquid`,
  `app/publisher/server/src`) is the only authority for `l4Book`, the significant-figure arithmetic and
  the `nLevels` extension. ⚠️ **No rendering happens under the shared `BookSnapshot` lock** — that is
  the mutex the arbiter's `apply_book_replay` takes on every published batch, so a book render held
  across it stalls every receiver on every feed; `take_market`/`take_markets` clone the accumulator out
  (itself O(book), and the cheapest snapshot on offer — measured ~0.45 ms against ~9.1 ms to fold
  under the guard) and every rendering step runs after the guard drops, and the `price_fold` behind a client's `l2Book`
  subscriptions is computed **once per market per message** (it is view-independent, so folding per
  subscription would let one client multiply a 44k-order book by `MAX_SUBS`). Both book channels are
  gated on `baselined() && is_order_level()`: an
  `l2Book` frame *replaces* a Nautilus consumer's book wholesale and an `l4Book` snapshot claims
  completeness, so a market accumulated mid-stream must be withheld, not published as if whole — and a
  price-aggregated market, whose orders this sink cannot read, would render as an empty book. An
  out-of-range `nSigFigs`/`mantissa` is **rejected** rather than coerced (a substituted bucket yields
  prices that look like the venue's and are not); `nLevels` only truncates, so it clamps. The schema's
  account-model fields (`users`/`hash`, an order's `user`/`timestamp`/`orderType`/`tif`/`cloid`,
  `height`, `order_statuses`) have no counterpart on the MBO wire and are null/zero/empty — parseable,
  never meaningful. `timestamp` is deliberately `0` rather than the book's event time: that stamp is one
  instant shared by every order in a snapshot, which a consumer ageing orders reads as real. Order diffs
  are `new`/`remove` only, since `update{origSz,newSz}` would need a prior quantity we do not carry.
  The accept loop never propagates an error (this task's `Err` reaches `main`'s `select!` and would exit
  the process; unlike `ws`'s, no reconciler respawns it), and the client limits are constants, of which
  the inbound rate cap is the load-bearing one — a subscribe frame is tens of bytes and costs a whole
  book to answer, and the `subs.contains` guard cannot see an unsubscribe-then-resubscribe loop.
- **`sinks/api.rs`**'s `GET /v1/status` carries four accounting blocks beyond per-venue
  `online`/`offline`: `registry` (which feed-registry document this process resolved — a URL, a
  bind-mounted file path, or `"built-in"` — its `version`, and its row/receiver counts; the identical
  figures `main` logs once at startup as "feed registry resolved," read from
  `ingest::feeds::registry_info()` rather than recomputed, so a fleet-wide check of which document a
  host is running doesn't need log access), `history` (`history::Store::stats()` verbatim — products tracked/at cap,
  buckets, bucket budget, estimated bytes, window, evictions, late drops), `channels` (per enabled
  row that carries a channel id — every row but a flat one, see `ingest/channel_filter.rs` — its full
  roster with `allowed` from the channel filter alone, `bound` from **real** receiver liveness
  (`SharedFeedHealth::liveness`, keyed exactly as the reconciler's own receiver map — an
  admitted-but-unsubscribed channel reads `Unregistered`/`Down` here rather than a false "bound",
  the exact field the admin surface's own `GET` had to caveat instead of fix, since it has no
  liveness handle at all), `products` from `history::products_for` at the same `(venue, category,
  channel)` grain, plus `label` (registry-supplied, display-only) or, failing that, live-derived
  `symbol_prefixes` — one linear pass over the whole instrument catalog per request (not per
  channel), ranked by how many instruments carry each prefix (alphabetical tiebreak) and capped at
  8 sent, with `symbol_prefixes_total` reporting the true distinct count regardless of the cap so a
  client can render an exact remainder; both fields omitted rather than sent empty/null so a bare
  channel id reads as "no signal yet," never an error), and `process`
  (resident memory + cumulative CPU time read straight off the Prometheus **process collector**,
  deliberately independent of `--metrics-bind` so it still answers over `--url` against a remote
  host with neither the metrics endpoint nor `/proc` access enabled; `None`, never a fabricated `0`,
  when the collector isn't registered — it's Linux-only, see `metrics::Metrics::new`).
- **`sinks/admin.rs`** — the **one mutation path** in this crate, entirely separate from `/v1`
  (which must stay provably read-only) and **on by default, at loopback**
  (`--admin-bind`/`DZ_ADMIN_BIND` defaults to `127.0.0.1:9098`; set empty to disable it outright) —
  the exposure is accepted on the condition that the default never reaches past loopback. **No
  authentication**, and under host networking a wildcard bind is genuinely network-reachable, so an
  override must stay on loopback, never a bare wildcard. Two routes, both `/admin/channels`: `GET`
  reports the channel filter in force plus,
  per enabled row, which publishers/channels it **admits** — explicitly *not* the running receiver
  set (this surface has no handle on the reconciler's `active` map or `SharedFeedHealth`, so a `note`
  field says so rather than the response naming the field "bound" and inviting the exact mistake
  `sinks/api.rs`'s own `channels` block above had to fix); `POST ?channels=<spec>` replaces the
  shared `Arc<Mutex<ChannelFilter>>` through the identical `ChannelFilter::parse` the startup path
  uses — so nothing here can admit a row startup would have refused — taking effect on the
  reconciler's *next* tick via the existing spawn/abort diff, which is also what purges a departing
  channel's catalog/book/history state (`reconcile::forget_departing_channel`, above), not the
  instant the handler returns. The spec travels as a **query parameter, not a body**: this crate's
  hand-rolled `sinks::http` scaffolding never reads one, so `POST` refuses two distinct caller
  mistakes rather than silently misfiring on the natural instinct to `POST` a body — a **missing**
  `channels` parameter (400, distinct from present-and-empty, which is how an operator explicitly
  clears the channel filter) and a **non-empty request body** (400, detected via `Content-Length` —
  otherwise silently ignored, which is how `curl -d` or a library's default `post(url, data=...)`
  would quietly widen the channel filter to admit-everything while looking, to the caller, like it
  worked). Bind/serve split exactly as `ws`/`api` (a taken port disables this surface without taking
  the tunnel down) but deliberately **not** subscription-gated — an operator must be able to inspect
  or narrow the channel filter before anything is subscribed at all.
- **`model.rs`** — wire types (`NormalizedQuote`/`NormalizedTrade`/`NormalizedMidpoint`/
  `NormalizedDepth`/`NormalizedBook`/`NormalizedInstrument`, the `FeedMessage` tagged enum) and the
  `now_ns()` / `now_mono_ns()` clocks. `InstrumentSnapshot` is keyed by
  **`(venue, channel, instrument_id)`** and `BookSnapshot` by that **prefixed with the arbitration
  scope** (`(venue, category, channel, instrument_id)` — exactly `authority::MarketKey`) — a
  market-by-price `symbol` is a truncated display label
  that collides across markets (confirmed against a real capture — two distinct instrument_ids
  sharing one truncated symbol, see `tests/fixtures/PROVENANCE.md`), so it is not an identity; a
  `(venue, symbol)`-keyed map would have the second market's insert silently destroy the first's
  entry. `DepthSnapshot` (Market-by-Order only) is still keyed **`(venue, symbol)`** — unconfirmed
  whether that feed's own truncated symbols can collide the same way; not addressed here.
  `NormalizedBook` is the **incremental** counterpart of `depth`: a batch of `BookChange`s with
  absolute per-level sizes, where a re-baseline is structurally `changes[0].action == Clear` (the
  reference consumer's book dispatcher branches on the action and never reads the advisory `snapshot`
  flag) and `last` is mandatory on the final batch or a buffering consumer wedges. `BookChange` carries
  `order_id` (`0` = price-aggregated), and `BookAccumulator` keys order-level changes by it while keeping
  the price maps for the aggregated kind; `price_fold` folds the orders into levels **with a count per
  level**, which a price-keyed accumulator structurally cannot produce, and `to_book(ReplayScope)`
  materializes either rendering, and `is_order_level` is what an unset `book_scope` follows so a client's
  bootstrap matches the granularity the market streams. A `Clear` names a *side*, so it always carries
  `order_id == 0` and clears that side of **both** populations — routing it by id would leave every order
  of a re-baselined-away book resting in the replay map forever. Its pending-change cap now bounds only an event still awaiting its
  `last` — a terminated batch folds whatever its size, or a 44k-order snapshot install could never
  baseline. `BookSnapshot`
  holds a `BookAccumulator` per market rather than the last message, because an incremental product's
  last batch bootstraps nothing — it accumulates what a consumer would and materializes a clear plus
  the full level set on demand. It commits per *logical event* (buffering until `last`), since
  `to_book` stamps `last: true` and a half-applied rebuild would replay as a complete torn book, and
  it is honest about completeness only while `baselined` holds. The arbiter's `Book` arm is the
  single-arm
  authority gate (`ingest/authority.rs`), which owns both this replay map and its own per-arm
  accumulators; `MbpProcessor` emits `book`, and the `lashay-2` row selects it on a live group.
  `NormalizedInstrument` carries the same `(channel, instrument_id)` identity pair as `NormalizedBook`,
  so a consumer joins a book to its precision on the identity rather than the colliding `symbol`; the
  arbiter's definition rate limit keys on that triple for the same reason.

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
- **MBO is re-served as two derived products, never raw deltas.** The bridge reconstructs the L3 book
  and runs snapshot+delta recovery internally (`book.rs`), then derives full-state `depth` (top-N) and
  the order-level incremental `book` from it. A `book` change is one order's **absolute resulting
  state**, not the wire's add/cancel/execute event, and a recovery still surfaces only as a
  `Clear`-led re-baseline — do not expose the raw order events. `depth` is additive and unchanged:
  there are testers on it, and deleting it is its own change (PROTOCOL.md v2).
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
  default (on when `--ws-input-coins` is non-empty) and is **not** subscription-gated, and neither is
  the Hyperliquid-compatible sink (**off**, on when `--hl-ws-bind` is non-empty) — it renders whatever
  the broadcast carries, so it has no group of its own to be subscribed to. README has the
  full activation tables.
- No TLS on the **service surface** — the WebSocket output and multicast input target a trusted/local
  network; terminate TLS at a reverse proxy if exposed. The **one** exception is the outbound
  `wss://` client in `ingest/ws_feeder.rs` (public HL feed), which uses rustls + bundled webpki roots.

## Packaging and release (the `doublezero-edge` CLI)

The bridge stays a container; only `doublezero-edge` (the read-only `/v1`/`/admin` CLI, a separate
workspace member — see `doublezero-edge/`) ships as a **signed deb/rpm**, modeled end-to-end on the
DoubleZero client's own CLI packaging (`release/.goreleaser.base.client.yaml` in the `doublezero`
repo) so it reuses that signing and distribution rather than inventing a second path:

- **`release/.goreleaser.base.edge-cli.yaml`** (+ `testnet`/`mainnet-beta` overlays that each add
  only a `cloudsmiths:` block) — `builder: rust`, `--package=doublezero-edge`, target
  `x86_64-unknown-linux-musl` with `CC_x86_64_unknown_linux_musl=musl-gcc`. **Static by design**: a
  glibc build on the CI image would fail on an older host we don't control. `before.hooks` generate
  the three shell-completion files from `doublezero-edge completion <bash|zsh|fish>` (a plain
  `clap_complete` subcommand in `doublezero-edge/src/main.rs` that runs with no config file, no
  server, no network — packaging depends on that). `nfpms:` bundles the binary at `/usr/bin` plus
  those three completions and **nothing else**: no `dependencies:`, no `scripts:`, no unit files.
  That inertness — pinned from the built artifact, not the config, by `tests/packaging.bats`
  (`dpkg -c`/`rpm -qlp` list exactly those four paths; `ldd` reports the binary as not dynamically
  linked) — is what makes the package safe to install unattended from a shell prompt; a future
  maintainer script is a design decision to revisit, not a detail to slip in.
- **`.github/workflows/release.edge-cli.yml`** — tag-triggered on `doublezero-edge/v*` (the
  `monorepo.tag_prefix`, namespaced so it never collides with this repo's `shred-proxy-v*` tags),
  mirrors `release.client.yml`: installs `rpm` + `musl-tools`, runs goreleaser-pro twice (once per
  overlay) with `GORELEASER_KEY` + `CLOUDSMITH_TOKEN`.
- **Cloudsmith, existing repos.** Publishes into `doublezero-testnet` / `doublezero-mainnet-beta`
  (org `malbeclabs`) — the same repos the DoubleZero client already publishes into — not a new one,
  so a host that already trusts that source installs `doublezero-edge` with no second key or source
  file. **One artifact serves both environments**: the CLI is a plain HTTP client with no ledger
  coupling, so unlike the client it needs no env-specific build variant.
- **`scripts/connect.sh`** offers the package only *after* the bridge container is confirmed
  healthy, and only ever warns on decline or failure — the container is the product, the CLI a
  convenience (see `tests/scripts/install_cli.bats`). It names the repo and package before touching
  anything; `DZ_INSTALL_CLI=1|0` and `DZ_ASSUME_YES=1` answer non-interactively. It also clears the
  image's default `DZ_FEED_REGISTRY_URL` when an operator sets `DZ_FEED_REGISTRY` without a URL of
  their own — see the `ingest/registry.rs` entry above (`tests/scripts/feed_registry.bats`).

Adding a fourth environment or changing the completion paths means editing the base file once; the
overlays exist only to hold each environment's `cloudsmiths:` block.
