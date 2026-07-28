# Venue-compatible WebSockets — design

**Date:** 2026-07-28
**Status:** PROPOSED — this document is the review; nothing here is implemented yet.
**Scope:** `src/ingest/{book,l3,arbiter,processor,reconcile,feeds}.rs`,
`src/sinks/{mod,ws,hl_ws,kalshi_ws}.rs`, `src/{model,main,metrics}.rs`, plus
[`PROTOCOL.md`](../../../PROTOCOL.md), [`docs/output-sinks.md`](../../output-sinks.md), the
`scripts/connect*.sh` installers and the `Dockerfile`.

## Motivation

[`doublezero-edge-connect`](../../../README.md) re-serves DZ Edge market data over exactly one output
protocol today: the native v1 JSON contract in [`PROTOCOL.md`](../../../PROTOCOL.md), served by
[`src/sinks/ws.rs`](../../../src/sinks/ws.rs). Every consumer needs a purpose-written adapter first.

But the trading systems we want on the DZ path already speak a venue's own WebSocket API. Putting them
on the low-latency edge feed should be a **URL change, not a code change**. So we add two drop-in
protocol emulations as sibling sinks, each on its own port:

- **Hyperliquid-compatible** — the surface of
  [`hyperliquid-dex/order_book_server`](https://github.com/hyperliquid-dex/order_book_server):
  `l2Book`, `trades`, `l4Book`, plus `bbo` from the standard HL API.
- **Kalshi-compatible** — the
  [Kalshi **margin** WebSocket](https://docs.kalshi.com/margin-ws/websockets/websocket-connection):
  `orderbook_delta`, `ticker`, `trade`, with the `{id,cmd,params}` command envelope.

Kalshi also gains an **ingest** side: a `FEEDS` row for its DZ Edge multicast group, reusing an
existing sibling-protocol codec.

**The governing requirement is full compatibility with each venue's published standard** — no protocol
shortcuts, no unimplemented subscription parameters, no truncated books. That single constraint drives
the architecture below, and it rules out the obvious cheap implementation.

### Why the obvious approach fails

The natural move is to translate the existing `depth` message. It cannot work:

- `NormalizedDepth` is **top-N** (10 levels). A level leaving that window is indistinguishable from a
  cancel, so any diff-based `orderbook_delta` makes a Kalshi client's book silently diverge below the
  window, permanently. HL's `l2Book` allows `nLevels` up to 100, and `l4Book` is order-level.
- HL's `bbo` and `l2Book` derive from **one** book at the venue, so they can never disagree. Feeding
  `bbo` from the TOB feed and `l2Book` from MBO would let them disagree, which real HL never does.

The unlock is that **`BookState` already holds the full untruncated ladder** (`bids`/`asks` are plain
`BTreeMap`s — `book.rs:96-98`); only the *emitted* message is truncated. Full fidelity is therefore
reachable by giving the compat sinks the L3 book rather than truncated snapshots. That one decision
removes the truncation problem, makes `bbo` consistent with `l2Book`, and supplies the substrate
`l4Book` needs.

### Irreducible gaps — sentinel-filled

Three things the DZ Edge feed does not carry, at any level of effort. All are **zero/sentinel-filled**
so a typed venue SDK deserializes unmodified, and all are documented per field:

| Field | Sentinel |
|---|---|
| HL `trade.hash` | `"0x" + 64 zeros` |
| HL `trade.users` | two zero addresses |
| HL `l4Book` per-order `user`, `tif`, `cloid`, `orderType`, trigger fields | zero address / documented defaults (real: `oid`, `side`, `limitPx`, `sz`, `timestamp`) |
| Kalshi `open_interest`, `open_interest_notional_value_dollars` | `"0"` |

Kalshi's `volume_notional_value_dollars` / `volume_24h*` are **not** in that list — we accumulate them
from observed trades (accurate from bridge start; the 24 h warm-up is documented).

Six sequenced PRs follow.

---

## PR 1 — Book per-level counts and configurable depth

### `src/ingest/book.rs`

The level maps gain an order count — needed for HL's `WsLevel.n`, Kalshi's aggregated levels, and the
L3 channel.

```rust
/// One aggregated price level: raw integers in the instrument's price/qty exponents,
/// plus how many orders rest there (Hyperliquid's `WsLevel.n`).
pub type Level = (i64, u64, u32);

/// A level's aggregate state. The level is live iff `n > 0` — the count, not the qty,
/// is its liveness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LevelAgg { qty: u64, n: u32 }
```

`bids`/`asks` and `Building.bids/asks` become `BTreeMap<i64, LevelAgg>`. Widening the value (rather
than a parallel count map, or deriving on read) is the only option that is free on the hot path: it
increments a `u32` in a node the code has *already* fetched. A parallel `BTreeMap` would double the
tree descent and allocation on every Add/Cancel/Execute; deriving on read would scan up to
`MAX_ORDERS_PER_BOOK = 262144` orders per `emit_depth`.

**The one real correctness change.** Today `DeltaKind::Execute` calls `level_remove` for both partial
and full fills (`book.rs:375-396`). With counts that is wrong — a partial fill reduces qty but the
order is still resting. Split the helper (`book.rs:115-127`):

```rust
fn level_add(levels: &mut BTreeMap<i64, LevelAgg>, price: i64, qty: u64);         // qty +=, n += 1
fn level_reduce_qty(levels: &mut BTreeMap<i64, LevelAgg>, price: i64, qty: u64);  // partial fill: qty only
fn level_remove_order(levels: &mut BTreeMap<i64, LevelAgg>, price: i64, qty: u64);// qty -=, n -= 1, drop at n == 0
```

`apply()`'s `Execute` arm picks `level_remove_order` when `remove` (`full_fill || o.qty_raw == 0`),
else `level_reduce_qty`; `Cancel` → `level_remove_order`; `Add` and `on_snapshot_order` → `level_add`.
Level removal moves from `qty == 0` to `n == 0`, which also fixes today's latent zero-qty-add level
leak. Residual to document: a saturated underflow could publish `sz = 0, n > 0` — a visible symptom
rather than a silent hole.

`top_levels(n)` keeps its signature and yields the new triples. Call sites: `processor.rs:589`
(`emit_depth`), `processor.rs:495` (`last_top`, which becomes count-sensitive — desired), and the book
tests at `book.rs:466,496,501,511,528,538,552,576,595,608,620,644,684`.

### `src/model.rs` — `NormalizedDepth`

```rust
/// Resting order count per level, index-aligned with `bids`/`asks` (`bid_n[i]` counts the
/// orders at `bids[i]`). Empty when the side is empty; a published level always has >= 1
/// order, so `0` never appears.
#[serde(default, skip_serializing_if = "Vec::is_empty")] pub bid_n: Vec<u32>,
#[serde(default, skip_serializing_if = "Vec::is_empty")] pub ask_n: Vec<u32>,
```

Parallel arrays, not `[f64; 3]` levels: PROTOCOL.md's forward-compat rule covers *unknown fields*, not
*changed field shapes* — a consumer deserializing `[f64; 2]` must keep working. `u32` not `u16`
because `MAX_ORDERS_PER_BOOK = 2^18 > u16::MAX`. Naming matches the existing
`NormalizedQuote.bid_n`/`ask_n`; PROTOCOL.md should note that the quote's scalar is the depth's
`bid_n[0]` — same concept, different shape per message type. Build both in `emit_depth`'s single
`scale` pass.

### `DepthId` must gain the counts — `src/ingest/arbiter.rs:164`

```rust
pub struct DepthId { bids: Vec<(i128, i128, u32)>, asks: Vec<(i128, i128, u32)> }
```

Counts are exact integers, no `10^-8` canonicalization. This is **required, not optional**: `QuoteId`
already carries `bid_n`/`ask_n` (`arbiter.rs:128-151`), and `tests/common/assertions.rs` documents that
"two quotes that differ only in count are NOT duplicates", with depth identity content-inclusive. Once
`last_top` is count-sensitive, a count-only book change passes the processor's suppression and reaches
the floor — where a price/qty-only `DepthId` would drop it as an exact repeat, silently losing the
update. Add `bid_n`/`ask_n` to the oracle's depth key (`assertions.rs:104`) in the same commit.

Cross-publisher collapse is unaffected: two publishers reconstructing the same book derive the same
counts from the same order population. The `source_ts == 0` invariant (`arbiter.rs:753-766`) is
preserved — an empty book yields empty count arrays, so both publishers' empty anchors still produce an
identical `DepthId` and the non-leader still collapses. Keep the arbiter test at `:1309` and comment
that this survived the change.

### `--depth-levels`

`Args` gains `--depth-levels` (`DZ_DEPTH_LEVELS`, default 10, validated `1..=100`, `bail!` in `main`
before the reconciler is built). `processor.rs:35`'s `DEPTH_LEVELS` becomes
`DEFAULT_DEPTH_LEVELS`/`MAX_DEPTH_LEVELS`; `MboProcessor` gains a `depth_levels` field; threaded
`Args → ReconcilerConfig → receiver::run_feed` (`receiver.rs:546`, forwarded at `:606`) →
`MboProcessor::new`. Callers to fix: `tests/dedup.rs:50` and the processor tests at
`1051,1120,1234,1466,1636,1674,1770`.

This governs the **native wire only** — the compat sinks read the L3 channel and are not bounded by it.
Cost is linear in N (`top_levels`, the `last_top` compare, `DepthId` hashing under the arbiter mutex,
JSON size) and the default is unchanged, so there is no regression unless an operator opts in.

**PROTOCOL.md** (`:182-217`): two table rows for `bid_n`/`ask_n`, the example at `:185` updated, a note
that *N* is operator-configurable so consumers must not hard-code 10, and a line under Versioning
(`:336`) marking them additive v1 fields.

---

## PR 2 — Multi-sink framework (prerequisite; no behavior change)

Must land exactly once, before either sink, or the metric refactor conflicts across every `ws.rs` call
site.

### `src/sinks/mod.rs`

```rust
#[derive(Clone, Debug)]
pub struct SinkLimits {
    pub heartbeat: Duration, pub idle_timeout: Duration,
    pub max_clients: usize, pub max_subs: usize,
    pub max_inbound_per_min: u32, pub broadcast_capacity: usize,
}
```

`ws::WsConfig` becomes a re-export of it — a mechanical rename, no behaviour change. Each sink holds
`{ limits: SinkLimits, ...its own extras }`. **One shared limits struct, not one per sink**: three
copies of `--*-max-clients` / `--*-idle-timeout-secs` / `--*-broadcast-capacity` is nine flags nobody
keeps in sync, and they mean the same thing everywhere. The one genuine per-sink difference — Kalshi's
contractual 10 s heartbeat vs the native 20 s — is a Kalshi field with its own flag that shadows
`limits.heartbeat`; the shadowing is documented.

Also lift `ClientGuard` (`ws.rs:140-149`) and the accept loop (`ws.rs:210-243`) into a shared
`accept_loop(listener, limits, sink, spawn_client)` so the cap / connection metric / RAII logic exists
once.

### `src/ingest/reconcile.rs`

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SinkKind { Native, Hyperliquid, Kalshi }

pub struct SinkConfigs {
    pub native:      Option<(String, sinks::ws::WsConfig)>,
    pub hyperliquid: Option<(String, sinks::hl_ws::HlWsConfig)>,
    pub kalshi:      Option<(String, sinks::kalshi_ws::KalshiWsConfig)>,
}
impl SinkConfigs { fn configured(&self) -> HashSet<SinkKind>; }
```

- `ReconcilerConfig`: `ws_bind` + `ws_cfg` → `sinks: SinkConfigs`. `main.rs` builds each `Option` as
  `None` when its bind is empty, keeping "empty bind = off" as the single activation rule.
- `Desired`: `ws_on: bool` → `sinks: HashSet<SinkKind>`, computed by **one** helper
  `desired_sinks(any_feed)` called from both `desired_from_subs` (`:181`) and `static_desired` (`:192`)
  — collapsing today's duplicated expression.
- `Reconciler.ws_task` → `sink_tasks: HashMap<SinkKind, JoinHandle<Result<()>>>`.
- `apply_ws` → `apply_sinks(&mut self, desired: &HashSet<SinkKind>)`, **reusing the existing generic
  `plan::<SinkKind>()` (`:325`) verbatim** — exactly the shape it was extracted for. A private
  `async fn spawn_sink(&self, k) -> Option<JoinHandle<..>>` matches on the kind, calls that sink's
  `bind()`, and returns `None` after a `warn!` on bind failure — preserving
  non-fatal-bind-and-retry-next-tick **independently per sink**, so a taken Kalshi port cannot disable
  the native one.
- `reap_finished` (`:215`): one `retain` over `sink_tasks` replaces the `ws_task` special case.

> Expect a borrow-checker snag: as a `&mut self` method the spawn closure captures `&self.cfg` while
> `&mut self.sink_tasks` is live. Hoist the clones (`tx`, `instruments`, `depth`, per-sink cfg) into
> locals first.

### `main.rs` — duplicate-bind validation

Collect the non-empty binds of all three sinks and `bail!` on a duplicate. Two sinks on one port means
one silently loses the bind and stays dark forever, retried every 30 s with a warning nobody reads.
Suggested defaults: native `8081`, HL `8082`, Kalshi `8083`.

### Metrics — add a `sink` label, don't fork the families

`sink="native"|"hyperliquid"|"kalshi"` on the nine `dz_ws_*` families (`metrics.rs:134-155`
declarations, `:417-465` registrations). It obeys the label rule at `metrics.rs:9-16` (a three-value
fixed enum, never per-symbol); three parallel catalogs would triple the docs and make "total clients
across sinks" un-queryable. `dz_ws_clients` `IntGauge → IntGaugeVec`; the four scalar counters
(`ws_client_lagged`, `ws_serializer_lagged`, `ws_rate_limited`, `ws_idle_timeout`) → `IntCounterVec`;
`sink` joins the existing label on the rest.

**Pre-resolve the label children once per `serve()`** into a `SinkMetrics` struct (the arbiter's
`VenueMetrics` pattern) so the per-frame path does no `with_label_values` lookup — a net improvement
over today's per-message lookup at `ws.rs:381-382`. Update the reset at `metrics.rs:610`, the
`dz_ws_clients` assertion at `:654`, `sinks/metrics.rs:204`, and the two `#[serial]` baseline tests at
`ws.rs:470`/`:535`. **The CHANGELOG must flag the scrape break**: bare `dz_ws_clients` becomes
labelled, so dashboards need `sum(dz_ws_clients)`.

---

## PR 3 — The L3 channel (`src/ingest/l3.rs`)

The centerpiece. A second internal broadcast, separate from the `FeedMessage` backbone, carrying the
full-ladder book and its order-level events to the compat sinks. It is **not** a `FeedMessage` variant:
PROTOCOL.md forbids order add/cancel/execute on the native wire, and that stays true.

```rust
pub struct L3Update {
    pub venue: Arc<str>, pub symbol: Arc<str>,
    pub publisher: Publisher,      // the arbiter's leader identity
    pub source_ts_ns: u64,
    pub gen: u64,                  // monotonic per (venue, symbol); the sink's re-anchor cursor
    pub kind: L3Kind,
}
pub enum L3Kind {
    /// Full ladder + order population. Sent on first publish, leader change, and session reset.
    Snapshot { bids: Arc<[Level]>, asks: Arc<[Level]>, orders: Arc<[OrderRaw]> },
    /// Price-level changes: `(price_raw, new_qty, new_n)`; `new_n == 0` means the level is gone.
    Levels { changed: Arc<[Level]> },
    /// Order-level events, for `l4Book`.
    Orders { events: Arc<[OrderEvent]> },
}
pub type SharedBooks = Arc<Mutex<HashMap<(Arc<str>, Arc<str>), FullBook>>>;
```

### Publisher arbitration is the arbiter's job, not the sinks'

`MboProcessor` keeps an **independent book per `(publisher, instrument)`** because two publishers mirror
one feed with colliding instance-scoped sequences. A sink that mirrored both would double-apply. So
`MboProcessor` attaches the L3 update to the depth it emits, and the arbiter publishes it **only when
that depth is admitted** (`Admit::Emitted`, `arbiter.rs:768`). Exactly one publisher's event stream
reaches the sinks, and it is the same publisher whose book the native `depth` serves — the two outputs
can never disagree.

Three re-anchor triggers, each emitting `Snapshot` instead of `Levels`:

1. **First publish** for a `(venue, symbol)`.
2. **Leader change** — `L3Update.publisher` differs from the last published one. Applying a new
   leader's deltas on top of the old leader's book would diverge; a full ladder from the new leader
   re-anchors. This is the L3 analogue of the discipline the depth floor already implies.
3. **Session reset** — hook the existing `reset_depth_floor_for_*` paths (`EndOfSession`,
   `InstrumentReset`), which already purge the depth replay map.

`SharedBooks` is maintained by the arbiter on admit — the L3 analogue of `set_depth_replay`
(`arbiter.rs:783-786`) — so a newly-connected compat client gets an immediate full-ladder snapshot
instead of waiting for the next update. Same pattern as `DepthSnapshot`.

### Gated on a compat sink being configured

`FullBook` is a second copy of the ladder plus the order population for `l4Book` — the committed BTC
fixture is 44,598 orders, roughly 1.4 MB for that symbol. Build and publish the channel **only when
`--hl-ws-bind` or `--kalshi-ws-bind` is non-empty**, so the default deployment pays nothing. Thread the
flag as `ReconcilerConfig.l3_enabled` into `MboProcessor::new`. Document the memory profile in
`docs/output-sinks.md`.

### TOB-only venues

L3 comes from MBO. A TOB-only venue (Phoenix today, and Kalshi's first row — PR 6) has no book, so the
compat sinks synthesize a **one-level book from `Quote`**, with a latch: once a real `Depth` arrives for
that `(venue, symbol)`, quotes stop touching the book forever. Without the latch the market oscillates
between a 1-level and an N-level book, emitting a delta storm every tick.

---

## PR 4 — Hyperliquid-compatible sink (`src/sinks/hl_ws.rs`)

Flags: `--hl-ws-bind` (`HL_WS_BIND`, off by default), `--hl-ws-venue` (`HL_WS_VENUE`, default
`Hyperliquid`), `--hl-ws-path` (`/ws`, matching the reference server; `accept_hdr_async` rejects other
paths). Messages from other venues are dropped before any work — a Phoenix quote costs this sink
nothing.

### Wire types

Mirroring `order_book_server/server/src/types/`:

```rust
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Subscription {
    Trades { coin: String },
    #[serde(rename_all = "camelCase")]              // <-- REQUIRED per variant, see below
    L2Book { coin: String, n_sig_figs: Option<u32>, n_levels: Option<usize>, mantissa: Option<u64> },
    L4Book { coin: String },
    Bbo { coin: String },
}
#[derive(Serialize)] struct LevelOut { px: String, sz: String, n: u32 }
#[derive(Serialize)] struct L2BookOut { coin: String, time: u64, levels: [Vec<LevelOut>; 2] }
#[derive(Serialize)] struct BboOut { coin: String, time: u64, bbo: [Option<LevelOut>; 2] }
#[derive(Serialize)] struct TradeOut { coin: String, side: &'static str, px: String, sz: String,
                                      hash: &'static str, time: u64, tid: u64, users: [&'static str; 2] }
#[derive(Serialize)] #[serde(tag = "channel", content = "data", rename_all = "camelCase")]
enum ServerResponse<'a> { SubscriptionResponse(&'a serde_json::Value), L2Book(L2BookOut),
                          L4Book(L4BookOut), Bbo(BboOut), Trades(Vec<TradeOut>), Pong, Error(String) }
```

> **Footgun:** `rename_all` on the *enum* renames variants only — struct-variant **fields** need their
> own `rename_all` on the variant, or the wire says `n_sig_figs`. Pin it with a round-trip test against
> captured payloads.
>
> **Field order matters** for byte-exact tests — serde emits declaration order, so declare in HL's
> order (`px, sz, n`; `coin, time, levels`; `coin, side, px, sz, hash, time, tid, users`).

Build `subscriptionResponse` from the **verbatim inbound `serde_json::Value`**, so the ack is
byte-compatible with whatever the client sent regardless of our serde attributes.

### `nSigFigs` / `mantissa` aggregation — implemented

Required by full compatibility, and the largest single piece of work in this PR.

```rust
/// Bucket a raw integer price to `n_sig_figs` significant figures, optionally restricted to a
/// mantissa step. Bids round DOWN and asks round UP, so a bucket never crosses the spread.
fn bucket_price(px_raw: i64, exp: i8, n_sig_figs: u32, mantissa: Option<u64>, is_bid: bool) -> i64;
```

Validation mirrors the reference exactly: `n_sig_figs` ∈ `2..=5`; `mantissa` only permitted when
`n_sig_figs == 5` and only in `{2, 5}`. Aggregation sums `sz` and `n` within a bucket.

Property tests: bucketing is monotone; a bid bucket is ≤ and an ask bucket ≥ the original price; summed
`sz` is conserved across the whole side. ⚠️ Byte-exact parity with HL's own rounding at the boundaries
is **not** provable from the docs — validate against a live `api.hyperliquid.xyz` `l2Book` capture with
`nSigFigs` set before claiming parity, the same posture `codec_midpoint` carries.

### Serialize-once, adapted

HL frames are per-coin and parameterized (`nLevels`, `nSigFigs`), so `ws.rs`'s single shared payload
only partly applies:

- **`trades`, `bbo`, `l4Book`** are parameter-free → rendered once in the serializer, shared as a
  `Utf8Bytes` clone.
- **`l2Book`** → the serializer renders the **unaggregated full ladder once** into pre-formatted
  `LevelOut`s (float→string formatting is the expensive part). A client with no aggregation and
  `nLevels` ≥ available writes the shared payload; anything else re-renders from the pre-formatted
  slices — string work only, bounded by `max_clients`. A per-`(coin, params)` cache within a tick is a
  follow-up, not v1. Instrument it: `dz_hlws_frames_total{render="shared|per_client"}`.

The serializer maintains a task-local exponent cache from `FeedMessage::Instrument` so decimal
formatting never locks `InstrumentSnapshot` in steady state.

### Channels

| Channel | Source |
|---|---|
| `l2Book` | L3 full ladder, aggregated per subscription params |
| `bbo` | **top level of the same L3 book** — byte-consistent with `l2Book`, exactly like real HL |
| `trades` | `FeedMessage::Trade`, emitted as a one-element array (HL's `data` is always an array) |
| `l4Book` | `Snapshot { coin, time, height, levels: [Vec<L4Order>; 2] }` then `Updates` from L3 `Orders` |

`bbo` from the book rather than the TOB quote is the compatibility-correct choice: the two can never
disagree, as at the venue. It costs the difference between the MBO book and the TOB feed — document
that `bbo` here is book-derived, not the venue's own BBO broadcast.

`l4Book.height` has no upstream equivalent (HL's block height); use a monotonic counter derived from
`gen` and document it. `L4Order` carries real `oid`/`side`/`limitPx`/`sz`/`timestamp`; `user` is the
zero address, and `tif`/`orderType`/`triggerCondition`/`isTrigger`/`triggerPx`/`isPositionTpsl`/
`reduceOnly`/`cloid` get documented defaults.

### Validation, mirroring the reference server

| Input | Response |
|---|---|
| `coin` not in the universe (from `InstrumentSnapshot`, served venue only) | `error` |
| `coin` starts with `@` | `error` — spot-index syntax we don't serve |
| `nLevels == Some(20)` | `error` "set n_levels to this by using null" — a real quirk of the reference; reproduce verbatim |
| `nLevels > 100` | `error` |
| `nSigFigs` outside `2..=5`, or `mantissa` without `nSigFigs == 5`, or `mantissa ∉ {2,5}` | `error` |
| duplicate subscribe | `error` "Already subscribed: {json}" |
| `subs.len() >= max_subs` | `error` |

Cold-start caveat to document: before refdata arrives `InstrumentSnapshot` is empty and every subscribe
is rejected — we cannot format decimals without `price_exponent` anyway.

### Formatting and timestamps

```rust
/// Round to the instrument's exponent grid, print shortest-round-trip, then (for prices) ensure at
/// least one fractional digit — reproducing HL's shape ("106217.0", "0.001", "0.26739").
fn hl_decimal(v: f64, exponent: i8, keep_one_decimal: bool) -> String;
```

Grid-rounding first kills `apply_exponent`'s float noise (`0.30000000000000004`); shortest-round-trip
reproduces HL's trailing-zero trimming on `sz`. Rust's `Display for f64` never emits scientific
notation. This is reverse-engineered from the reference's captured payloads — a wider capture is wanted
if a consumer depends on byte-exactness.

`time` is **milliseconds**: `source_ts_ns / 1_000_000`, falling back to `recv_ts_ns / 1_000_000` when
`source_ts_ns == 0` (never hand an HL client a 1970 timestamp). `tid` = `trade_id` (already documented
as identical to HL's `tid` at `ws_feeder.rs:74`). `side`: `Buy → "B"`, `Sell → "A"`; `Side::Unknown` is
**skipped** and counted (`dz_hlws_dropped_total{reason="unknown_side"}`) — guessing an aggressor on a
trading feed is worse than a gap.

### On subscribe

`subscriptionResponse` (verbatim echo) → immediate snapshot for `l2Book`/`l4Book`/`bbo` from
`SharedBooks` (an empty ladder with `time = now_ms` if the book is not yet known, so the client's state
machine starts). `trades` gets no backfill, as at HL.

App keepalive `{"method":"ping"}` → `{"channel":"pong"}`; heartbeat / idle-timeout / rolling-minute rate
limit copied from `ws.rs:295-390`.

---

## PR 5 — Kalshi-compatible sink (`src/sinks/kalshi_ws.rs`)

Flags: `--kalshi-ws-bind` (off by default), `--kalshi-ws-venues` (default `Kalshi`),
`--kalshi-ws-heartbeat-secs` (**10**, per Kalshi's contract), `--kalshi-ws-token` (optional),
`--kalshi-ws-snapshot-interval-secs` (0 = off), `--kalshi-ws-max-markets-per-sub` (512),
`--kalshi-ws-qualify-tickers`.

### `market_ticker`

The sink is venue-scoped, so `market_ticker` is the bare `symbol` — which for a real Kalshi ingest feed
*is* the Kalshi ticker. If `--kalshi-ws-venues` names more than one venue, tickers **must** be qualified
`VENUE-SYMBOL` (`--kalshi-ws-qualify-tickers`, with a startup error if multi-venue and unqualified), or
two venues' `BTC` silently merge into one book. The choice is deterministic per config, never per
runtime — a ticker name that changed when a second feed subscribed would be worse than either option.

### Where diff state lives

**Diff shared, `sid`/`seq` per client, `gen` bridging them.**

The expensive part — turning an L3 update into Kalshi's `(price, delta, side)` triples — is a function
of the market alone. With `max_clients = 64` and 100 markets, doing it per client is a 64× waste of this
sink's only real CPU cost. Do it once in the serializer, mirroring `ws.rs`'s `prepare()`.

The semantic part genuinely is per client: `sid` is an integer that connection chose, and `seq` must be
gap-free **as that client observes it**, which depends on when it subscribed and which markets its
subscription covers. A shared `seq` would hand a mid-stream subscriber a first `seq` of 918,342 and
break gap-freedom whenever a client's market set is a subset of the stream. So `seq` is per
`(sid, market_ticker)` — a `HashMap<Arc<str>, u64>` on each `Sub`, bumped per frame written.

```rust
struct PreparedKalshi {
    kind: &'static str,   // "orderbook_snapshot" | "orderbook_delta" | "ticker" | "trade"
    channel: Channel, venue: Arc<str>, market: Arc<str>,
    gen: u64,             // 0 for non-book frames
    msg: Arc<str>,        // pre-rendered `msg` object, sid/seq-free
}
```

The client writes `{"type":…,"sid":…,"seq":…,"msg":<msg>}` with one `format!` — no serde on the
per-client path.

**The subscribe race.** A client subscribing to market M must get M's snapshot and then exactly the
deltas that follow it. On subscribe it renders the snapshot from `SharedBooks` under the lock (render
into a `String`, drop the guard, then send — never hold across an `.await`), records
`start_gen[M] = gen`, and thereafter drops any prepared frame with `frame.gen <= start_gen[M]`. That
filters precisely the frames already sitting in the broadcast channel.

### The diff — integers, and stable strings

```rust
/// Signed level changes turning `prev` into `next`, both keyed by canonical integer price.
fn diff_side(prev: &BTreeMap<i64, i64>, next: &BTreeMap<i64, i64>) -> Vec<(i64, i64)>;
```

A merge-walk: in both and different → `next - prev`; only in `prev` → `-prev`; only in `next` →
`+next`. Two non-negotiable details:

1. **Integer arithmetic.** Convert to `i64` at the count scale (`10^count_decimals`) and price scale
   once, on ingest into the mirror. Subtracting `f64`s and formatting to 2 dp yields `"-0.00"` and
   drift, and a client applying float deltas diverges. Mirrors the arbiter's own `i128` canonical
   choice.
2. **Format from the canonical integer, never the `f64`** — one `dec_str(scaled, scale_dp, out_dp)`
   helper. Otherwise a snapshot's `"0.960"` and a delta's rounded sibling key differently in the
   client's map and the book silently forks.

Because the source is the **full L3 ladder**, not a top-N window, there is no phantom-removal problem.
`--kalshi-ws-snapshot-interval-secs` remains available as a belt-and-braces periodic resync.

### Command envelope

Two-stage parse, because per-command errors need the original `id` even when `params` is garbage:

```rust
#[derive(Deserialize)]
struct Command { #[serde(default)] id: i64, cmd: String, #[serde(default)] params: serde_json::Value }
```

`id: 0` is legal per the spec and must never be treated as absent. Then `from_value::<SubscribeParams>`
per arm. A `subscribe` naming N channels allocates N sids and emits N `subscribed` acks with the same
`id` (what the ack shape and `list_subscriptions`' array both imply). `sid` is per-connection, monotonic
from 1, never reused. No `market_ticker`/`market_tickers` → firehose, matching the native sink's `{}`
convention.

Model each server frame as its own struct — `unsubscribed` puts `sid`/`seq` at top level, `ok` has both
top-level *and* a `msg`, and `list_subscriptions`' `msg` is an **array** while every other `msg` is an
object. One generic envelope with `msg: Value` will bite.

**Error codes.** Kalshi publishes only `3 = Invalid parameters`; we own the rest and document the table:

| code | meaning |
|---|---|
| 1 | malformed JSON / frame is not an object |
| 2 | unknown `cmd` |
| 3 | invalid parameters — empty `channels`, malformed `params`, unknown market |
| 4 | unknown `sid` |
| 5 | channel not supported (`fill`, `user_orders`, `order_group_updates`) |
| 6 | subscription / market limit reached |
| 7 | inbound rate limit exceeded (frame, then close — mirrors `ws.rs:306`) |

`fill`/`user_orders`/`order_group_updates` are **account** channels with no upstream equivalent → reject
with code 5 rather than acking a sid that never delivers. A silently-empty subscription is a far worse
failure mode for a consumer than an explicit error.

Flag semantics: `skip_ticker_ack` suppresses the ack for `ticker` subs only (sid still allocated);
`send_initial_snapshot` controls only re-snapshotting markets added later by `update_subscription`,
since an `orderbook_delta` subscription **always** opens with a snapshot (deltas are meaningless without
an anchor — the deviation is documented); `update_subscription` mutates the `MarketSet`, seeding
`start_gen` on `add_markets` and dropping `seq`/`start_gen` on `delete_markets`.

Keepalive: `Ping(b"heartbeat")` every 10 s — the body is a hardcoded const, only the interval is
configurable.

### Auth

Default **accept any handshake**; `--kalshi-ws-token` enables an exact-match check on
`KALSHI-ACCESS-KEY` or `Authorization: Bearer` via `accept_hdr_async`, else 401. Reasoning: a real
Kalshi SDK sends key/signature headers unconditionally, so rejecting unexpected headers would break the
very compatibility this sink exists for — accepting them **is** client-side compatible. Implementing
Kalshi's actual RSA/Ed25519-over-canonical-string scheme needs a key registry we do not have, and a
subtly-wrong canonical string would break real SDKs worse than accepting them. Do not build it. The
token is documented as a deployment tripwire, not authentication.

### `ticker` mapping

Per-market `TickerState` folded from `Quote` + `Trade`; every value a decimal string (prices 4 dp,
counts 2 dp).

| Kalshi field | Source |
|---|---|
| `price` | last `NormalizedTrade.price`, else `(bid+ask)/2` |
| `bid` / `ask` | `NormalizedQuote.bid` / `.ask` |
| `bid_size_fp` / `ask_size_fp` | `.bid_size` / `.ask_size` |
| `last_trade_size_fp` | `NormalizedTrade.size` |
| `volume` | `NormalizedTrade.cumulative_volume` (venue-provided, authoritative) |
| `volume_notional_value_dollars` | accumulated Σ(price × count) since bridge start |
| `volume_24h`, `volume_24h_notional_value_dollars` | rolling 24 h ring buffer of observed trades |
| `open_interest`, `open_interest_notional_value_dollars` | **`"0"` sentinel** — needs venue clearing data |
| `reference_price`, `settlement_mark_price` | optional in the spec → omitted |

Document the 24 h warm-up: the rolling figures are accurate only after 24 h of uptime, and
`open_interest` is a sentinel.

`trade`: `trade_id` = `trade_id` as a decimal string; `ts_ms` = `source_ts_ns / 1_000_000`;
`taker_side` maps `Buy → "bid"`, `Sell → "ask"` (consistent with the margin book channel's bid/ask
sides). ⚠️ **Confirm the polarity against a live Kalshi margin capture before anyone trades on it** — a
taker buy lifts the ask, and the docs type this field only as `"<string>"`. Isolated in one
`fn taker_side(Side) -> Option<&'static str>`.

### One place the `ws.rs` template must NOT be copied

`ws.rs:193` skips `prepare()` entirely when `receiver_count() == 0`. Here the serializer **must still
apply every L3 update to its mirrors** with no clients attached — only the diff/render is skippable.
Otherwise the first client to connect is served a snapshot rendered from a stale book. Structure it as
`update_state(&m)` unconditionally, then `if receiver_count() > 0 { render(...) }`.

---

## PR 6 — Kalshi ingest feed

One row, `FeedKind::TopOfBook` / `FeedPorts::TwoPort`: TOB is the only codec validated byte-for-byte
against the Go reference (`codec.rs`), and it yields the `quote` + `trade` the Kalshi sink's `ticker`
and `trade` channels need. An MBO row lands later as a second row on the same `code`, exactly as
Hyperliquid did — and only then does the Kalshi venue get a real L3 book (until then the sink
synthesizes a one-level book from quotes, per PR 3).

```rust
/// Sentinel group for a row whose live multicast endpoint is not yet assigned. `0.0.0.0` is not a
/// multicast address, so such a row can never silently join a wrong group.
pub const GROUP_UNASSIGNED: Ipv4Addr = Ipv4Addr::UNSPECIFIED;
pub const CODE_UNASSIGNED: &str = "unassigned";

Feed { venue: "Kalshi", code: CODE_UNASSIGNED, kind: FeedKind::TopOfBook,
       group: GROUP_UNASSIGNED, ports: FeedPorts::TwoPort { mktdata: 0, refdata: 0 },
       emit_trades: true },   // TODO(kalshi): real code / group / ports, confirm TOB vs MBO
```

plus `impl Feed { pub fn is_placeholder(&self) -> bool { self.group.is_unspecified() || self.code == CODE_UNASSIGNED } }`.

**The placeholder must be structurally excluded, not merely unlikely to match.**
`--subscription-gating-disable` routes through `static_desired()`, which activates **every** enabled
feed unconditionally — and that flag is set by every E2E test (`tests/common/bridge.rs:55`) and by
anyone running from source without the `doublezero` CLI. A placeholder would reach
`bind_multicast(0.0.0.0:0)`. So:

- `main.rs::select_feeds` — filter `!f.is_placeholder()` in the empty-selection branch; in the named
  branch, if every match for a name is a placeholder, `bail!("feed 'Kalshi' is scaffolded but has no
  assigned multicast endpoint yet")`. Loud, not silent.
- `subscriptions.rs::HostSubs::market_data_feeds` — the same filter, as defence in depth.

**Second scaffolding hole, easy to miss:** `codec.rs:118 source_name` maps only `1→Hyperliquid`,
`2→Phoenix`. Kalshi has no assigned SourceID. `processor.rs:295` falls back to `ctx.venue` for
unregistered ids, so messages on the Kalshi group are tagged correctly today — add a `TODO(kalshi)` at
the match and a row once upstream assigns one.

Tests that must change:

| File | Test | Change |
|---|---|---|
| `feeds.rs:162` | `every_feed_has_a_group_code` | the `match f.venue { … other => panic! }` hard-codes venues — replace with a `const EXPECTED_CODES: &[(&str, &str)]` table looked up by venue, still panicking on an unlisted one. One edit point per new venue. |
| `feeds.rs` | *new* `placeholders_are_marked_and_real_rows_are_multicast` | `is_placeholder()` ⟺ `group.is_unspecified()`; every non-placeholder has `group.is_multicast()` and non-zero ports |
| `main.rs:420` | `empty_selection_is_all_feeds` | **breaks** — `assert_eq!(all.len(), FEEDS.len())` becomes a `!is_placeholder()` count; rename accordingly |
| `main.rs` | *new* `named_placeholder_feed_errors` | `select_feeds(&["Kalshi"])` is an `Err` naming the missing endpoint |

Unaffected: `feeds.rs:150` (venue/kind uniqueness), `arbiter.rs:570 vm()` (lazy per-venue metric
children), `receiver.rs:546 run_feed` (dispatches on `FeedKind` only), and the two public feeders
(self-contained `PublicVenue` impls).

---

## Docs, installer, packaging

- **`docs/output-sinks.md`** — two table rows (`:8-11`), the L3 channel's memory profile and its
  gating, and per-sink deviation sections.
- **New `docs/hl-sink.md` and `docs/kalshi-sink.md`** — the field-mapping, error-code and "what this
  sink cannot tell you" tables are too long for `output-sinks.md`. Link from `docs/README.md`.
- **`PROTOCOL.md`** — a short section stating that v1 describes the **native** sink only and that the
  compat sinks are separately-documented third-party emulations explicitly outside the v1 contract.
  Without it, "PROTOCOL.md is the contract" (`CLAUDE.md`) goes ambiguous the moment a third wire format
  ships.
- **`README.md`** (`:80-92`) — the new env vars; **`docs/metrics.md`** — the `sink` label and the
  `dz_hlws_*` / `dz_ws_books_tracked{sink}` series; **`docs/input-sources.md`** — a line noting the
  `Kalshi` row is scaffolded and inactive.
- **`CLAUDE.md`** — the `sinks/` bullet gains `hl_ws`/`kalshi_ws`, a new `ingest/l3.rs` bullet, and
  updates to the `book.rs` / `processor.rs` / `arbiter.rs` / `reconcile.rs` / `feeds.rs` bullets.
- **`scripts/connect-devnet.sh`, `connect-testnet.sh`, `connect.sh`** (the bats helper iterates all
  three, `tests/scripts/_helpers.bash:20`): add the new vars to `PASSTHROUGH` (`:565`) with **plain
  non-empty forwarding** — unlike `WS_BIND` (`:580`), these default to empty-means-off, so do not copy
  the set-but-empty special case. Refactor `preflight_ws_port` (`:492-520`) into a parameterised
  `preflight_port <label> <bind-var> <default-port>` called per configured sink, **keeping
  `preflight_ws_port` as a thin wrapper** so `tests/scripts/preflight_ws_port.bats` still exercises the
  same entry point. Extend the final status print (`:629-668`) and its readiness grep (`:655`).
- **`Dockerfile`** — `EXPOSE 8082 8083` next to `:97`; extend the env comment at `:87`.
- **`CHANGELOG.md`** — one bolded-topic bullet per PR naming the files, per Keep a Changelog. CI
  enforces an entry. The metrics `sink` label goes under `### Changed` with the scrape-break note.

No new Cargo dependencies.

---

## Tests

**`book.rs`** — existing `top_levels` assertions become triples, plus
`level_counts_track_adds_and_cancels`; **`partial_execute_keeps_the_order_counted`** (execute 3 of 8
with `full_fill: false` → `(px, 5, 1)`, then full-fill → level gone — the regression guarding the helper
split); `level_dropped_only_when_its_last_order_leaves`; `snapshot_orders_populate_counts`; and
**`counts_always_sum_to_the_order_population`** — after a scripted sequence (adds, cancels, partial/full
executes, a gap → buffer → snapshot → replay), `Σn(bids) + Σn(asks) == orders.len()`. The best single
invariant in the set.

**`arbiter.rs`** — `depth_id_includes_level_counts`; keep the empty-anchor collapse test (`:1309`) green
with a comment pinning that the `source_ts == 0` invariant survived.

**`processor.rs`** — `depth_levels_is_configurable`; `depth_carries_per_level_counts`
(`bid_n.len() == bids.len()`); `count_only_change_emits_a_new_depth` (two 5-lots replaced by one 10-lot:
px/sz identical, `n` 2→1, must not be suppressed).

**`l3.rs`** — `leader_change_emits_a_snapshot_not_levels`; `session_reset_emits_a_snapshot`;
`levels_updates_reconstruct_the_full_ladder` (apply every `Levels` to the prior `Snapshot`, compare to
the processor's book); `l3_is_not_published_when_no_compat_sink_is_configured`.

**`hl_ws.rs`** — `subscription_wire_round_trips_real_payloads` (the reference's own captured strings,
pinning the per-variant `rename_all`); **`l2_book_frame_matches_captured_payload`** byte-for-byte
against `{"channel":"l2Book","data":{"coin":"BTC","time":1751427259657,"levels":[[…],[…]]}}` — the
highest-value test here; `trades_frame_matches_captured_payload`; `bbo_frame_shape` with a `null` side;
`bbo_top_level_equals_l2book_first_level`; `hl_decimal_formatting` (`(106217.0,-1)→"106217.0"`,
`(0.001,-5)→"0.001"`, `(0.1+0.2,-8)→"0.3"`); the full validation table including the `nLevels == 20`
quirk; `sig_figs_bucketing_is_monotone_and_conserves_size`; and two `#[serial]` `serve()` tests —
subscribe ordering (`subscriptionResponse` then snapshot) and shared-vs-per-client rendering.

**`kalshi_ws.rs`** — `dec_str_is_stable` (the same canonical integer renders identically in snapshot and
delta); `diff_deltas_are_exact_integers` (`0.1 + 0.2` → exactly `"0.30"`);
**`snapshot_plus_deltas_reconstructs_the_book`** — ~200 successive L3 updates from a seeded LCG (no
`rand` dependency), applying snapshot + every delta into a fresh `BTreeMap` and asserting equality with
the current full state after **every** step, both sides, no zero-size level surviving;
`seq_is_monotonic_and_gap_free_per_market` with two markets interleaved on one sid;
`delta_before_start_gen_is_dropped` (the subscribe race); `command_parser_accepts_the_documented_shapes`
with `id: 0` preserved; `ack_frames_match_documented_shape` compared as `Value` against the doc's
examples; `unsupported_channel_yields_error_code_5`; `unknown_sid_yields_code_4`;
`quote_book_stops_once_real_depth_arrives`; and two `#[serial]` socket tests (client accounting nets
back via `ClientGuard`; the heartbeat Ping body is `heartbeat`).

**`reconcile.rs`** — `plan_over_sink_kinds_spawns_and_aborts_independently`;
`desired_sinks_empty_when_no_feeds_subscribed`; `unconfigured_sink_is_never_desired`.
**`main.rs`** — `duplicate_sink_binds_are_rejected`, plus the two placeholder-feed tests above.

**Integration.** Harness: `tests/common/bridge.rs` gains per-sink addrs and a generic port poll in
`wait_ready` (`:96` only polls the native port today); new `tests/common/hl_client.rs` and
`kalshi_client.rs` mirroring `ws_client.rs`, each with an `apply_deltas` helper.

- `tests/hl_compat.rs` — replay the MBO goldens in wire order exactly as
  `e2e.rs::mbo_single_publisher_depth_contract` (`:169-201`) does, with clients on
  `l2Book`/`trades`/`bbo`/`l4Book`. Assert ordering, `levels.len() == 2`, bids descending / asks
  ascending, every `n >= 1`, a plausible 13-digit ms `time`, the error frames, and — the strongest
  available check — that `bbo` equals `l2Book`'s first level on every tick.
- `tests/kalshi_compat.rs` — **`kalshi_book_reconstructs_the_native_depth`**: one bridge, native and
  Kalshi clients, same replay; apply the Kalshi snapshot + every delta and assert the top-10 of the
  result equals the final native `depth`. Cross-checks the two sinks against each other on the real
  44,598-order two-sided BTC book and would catch a side inversion, a scale error or a lost delta. Plus
  `seq` gap-freedom over the full replay, `ticker`/`trade` from the TOB fixtures with
  `open_interest == "0"`, the heartbeat body, `kalshi_sink_is_off_by_default`, and
  `native_sink_is_unaffected_when_kalshi_is_enabled` (re-run `tob_single_publisher_contract`'s
  assertions with both sinks up — the sink-isolation contract).
- Update `e2e.rs:233`'s `bids.len() <= 10` comment and `assertions.rs:104`'s depth key.

**Manual end-to-end** — the tests that actually settle compatibility:

```bash
cargo build --release && cargo clippy --all-targets && cargo test
./target/release/doublezero-edge-connect --iface doublezero1 \
  --ws-bind 0.0.0.0:8081 --hl-ws-bind 0.0.0.0:8082 --kalshi-ws-bind 0.0.0.0:8083 \
  --metrics-bind 127.0.0.1:9090
```

Point `order_book_server`'s own `binaries/src/bin/example_client.rs` at `:8082` — it is the reference
client, so it working unmodified *is* the compatibility test — and an official Kalshi SDK at `:8083`.
Verify `curl -s localhost:9090/metrics | grep 'sink='` shows all three sinks.

---

## Open questions for review

Four decisions and three unknowns are the places to push back.

**Decided in this design, worth challenging:**

1. **Sentinel-filling the irreducible fields** (HL `trade.hash`/`users`, `l4Book` order attribution,
   Kalshi `open_interest`) rather than omitting the keys. Sentinels keep a typed SDK deserializing
   unmodified — the whole point of the sinks — at the cost of a consumer reading `open_interest` and
   getting a plausible-looking wrong number. The alternative breaks strict SDKs.
2. **Offering `l4Book` at all**, given that per-order `user` is a zero address. A consumer keying on
   order attribution gets nothing meaningful; one that only reads the ladder gets a fully correct feed.
3. **`bbo` derived from the L3 book** rather than the earlier TOB quote. Correct for compatibility
   (they can never disagree, as at the venue) but slower than the data we actually have.
4. **The `sink` metric label** breaks existing dashboards querying bare `dz_ws_clients`.

**Cannot be settled from documentation — each isolated to one function, so the fix is a one-liner once
captured:**

1. **HL `nSigFigs` boundary rounding** — validate against a live `api.hyperliquid.xyz` `l2Book` with
   `nSigFigs` set before claiming parity.
2. **Kalshi `taker_side` polarity** — a taker buy lifts the ask; the docs type the field only as
   `"<string>"`.
3. **HL decimal rendering** — reverse-engineered from the reference's captured payloads; a wider capture
   would confirm the trailing-zero rules.
