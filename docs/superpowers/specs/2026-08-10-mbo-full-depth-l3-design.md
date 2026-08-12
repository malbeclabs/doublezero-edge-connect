# Full-depth MBO and a Hyperliquid-compatible sink — design

**Date:** 2026-08-10 (revised same day for the two-sourcing-model framing)
**Repo:** `doublezero-edge-connect` (public)
**Status:** design, approved in outline; not yet planned or built

## Goal

Two things, sequenced.

1. **Stop flattening Market-by-Order.** The bridge reconstructs a real L3 book and then throws the order identity away, emitting a top-10 price-aggregated `depth`. Make the order-level book the product: emit real per-order changes on the existing incremental `book` message, carrying the venue's own `order_id`.
2. **Serve it in a shape a Hyperliquid trader recognizes.** Add an output sink speaking Hyperliquid's own WebSocket schema (`l2Book`, `l4Book`, `trades`) so an existing Hyperliquid client — including NautilusTrader's shipped adapter — consumes edge-connect with a URL change and no adapter work.

This is the design doc the prior market-by-price design (§2.2, §7 PR 7) said this work needed before it could start.

## Two sourcing models, and why the mechanism follows the wire

This is the organizing idea, and it is the thing to get right — the rest follows from it.

Edge feeds fall into two camps by how the venue is sourced, and the two want opposite arbitration.

**Distributed venues (Hyperliquid today).** Every publisher runs its own node and independently observes the same deterministic event stream. Copies are genuinely racing: winners change constantly, margins range from a few milliseconds to hundreds, and the publisher count varies as instances come and go. Crucially the venue assigns each mutation an identity — `order_id`, and `trade_id` on executions — that **every publisher reports identically**. Here we want to race aggressively and keep best-of-N per event.

**Centralized venues (Kalshi today).** Multiple sources exist for redundancy, not speed. They may be separate transports of the venue's own feed — e.g. one FIX, one WebSocket. They may have no shared per-event identity, no stable entry id, and no comparable per-entry venue clock. Winners change rarely. Here best-of-N is not on offer: the copies cannot even be recognized as the same event, so the only sound model is to elect one arm and hold it.

**The selector is a property of the wire, not the venue name:** *does every mutation carry a venue-assigned identity that is identical across publishers?* If yes, race per event. If no, elect one arm. That maps directly onto the `ArbitrationMode` seam that already exists per `Feed`, so this is configuration rather than a rewrite — which is exactly what that seam was built for. Which `ArbitrationMode` to use will come from the JSON described in [Kalshi PR175](https://github.com/malbeclabs/kalshi/pull/175), using hardcoded defaults for different feeds, like described in [Edge Connect PR123](https://github.com/malbeclabs/doublezero-edge-connect/pull/123)

**`Coordinated` widens in meaning here, and that is deliberate.** Today it means one thing: latch per `source_ts` tick, which works for quotes because both sources stamp a comparable block time. For order-level MBO the shared coordinate is not a clock at all — it is the venue's `order_id`/`trade_id`. Both are the same idea, *race on whatever coordinate the wire supplies*, so this is one mode with two oracles rather than a third mode. Worth stating because a reader who assumes `Coordinated` implies tick-latching will look for a tick that MBO does not have.

We have no Kalshi MBO data today and may never. The point of building both paths is that the *next* venue will fall into one camp or the other, and the wire tells us which without anyone having to decide. 

## Current state, verified against `main` @ `47d25b3`

- `ingest/book.rs` holds `orders: HashMap<u64, RestingOrder>` — a genuine L3 book, bounded by `MAX_ORDERS_PER_BOOK = 1 << 18` (262,144). The order identity already exists; nothing decodes it away.
- `MboProcessor::emit_depth` calls `book.top_levels(DEPTH_LEVELS)` with `DEPTH_LEVELS = 10`. That single call is where L3 becomes L2 and the ids are discarded.
- The incremental `book` product already exists from the market-by-price work: `NormalizedBook`, `BookChange`, `BookAction`, identity `(venue, channel, instrument_id)`, structural re-baseline via `changes[0].action == Clear`, mandatory `last`, `BookAccumulator` replay, and the single-arm authority gate.
- `BookChange` is `{action, side, price, size}` — **no `order_id`**.
- `BookAccumulator` is **price-keyed** (`BTreeMap<i128, (f64, f64)>`) and buffers an in-flight rebuild under `MAX_PENDING_CHANGES = 8192`.
- MBO deltas carry `per_instrument_seq: u32`, `order_id: u64`, and on executions `trade_id: u64`.
- Today MBO books are keyed `(publisher, instrument)` and the derived `depth` is raced across publishers by the content-keyed `DepthId` floor. **Racing already happens; this design must not lose it.**
- The committed Hyperliquid BTC snapshot fixture is **44,598 orders**.

Two of those collide: a price-keyed accumulator cannot hold order-level state, and an 8,192-change buffer cannot carry a 44,598-order rebuild. **The existing re-baseline path structurally cannot express an L3 snapshot.** That is the central thing this design has to fix.

### What the wire evidence establishes

Measured from a multicast capture of the live Hyperliquid group, on one execution observed from three concurrent publishers. The capture is not committed to this repo; the findings are reproduced here because they are what the design rests on.

- `per_instrument_seq` = **409029739 → 231884478 → 412116555** — three unrelated bases, each internally consecutive. **Per-publisher; meaningless across publishers.**
- `order_id …50951` and `trade_id 766226261658948` — **identical in every copy.**
- Copies spread ~9ms then ~50ms in that window; and could be up to ~200 ms against the public feeds.
- Multi-source is intermittent; a block 23 minutes later had a single clean source.

So for Hyperliquid the shared identity exists and the sequence is useless across arms. Dedup on identity; never on sequence.

## Reference consumer: what NautilusTrader accepts

Checked against `nautechsystems/nautilus_trader` @ `v1.227.0`, the version pinned in `NautilusTraderJune`.

- **`HyperliquidDataClientConfig` has `base_url_ws`** — a WebSocket endpoint override. Pointing a stock Nautilus Hyperliquid trader at edge-connect is a config field, not a fork. Only the WS endpoint is overridable; instruments still load over HTTP from Hyperliquid, which is fine.
- **The stock adapter cannot receive L3.** Its Rust WS client has only `SubscriptionRequest::L2Book { coin, nSigFigs, mantissa }`; no `L4Book` variant exists and a test pins `"type":"l2Book"`.
- `WsBookData { coin, levels: [Vec<WsLevelData>; 2], time }`, `WsLevelData { px: String, sz: String, n: u32 }`. Prices and sizes are **strings**; `n` is the **order count at that price**.
- A zero `order_id` on an L3 feed silently degrades to price-keyed L2 in Nautilus. On MBO that is a correctness bug, not a convention.
- Only `BookAction::Clear` re-baselines; `F_SNAPSHOT` is ignored for L2/L3.

`WsLevelData.n` is the quiet constraint: an order count per price is **not derivable from a price-aggregated book**, so a faithful `l2Book` needs order-keyed state regardless of `l4Book`.

**The honest promise:** a Nautilus Hyperliquid trader sets `base_url_ws` and gets genuine full-depth L2 with no code change. Order-level is on the wire for our own adapter now, and for theirs when someone extends it. We do not claim a stock Nautilus client consumes L3 today.

## Decisions

1. **Arbitration follows the wire's identity, not the venue.** Racing where a venue-assigned per-event identity exists; single-arm election where it does not. Expressed through the existing `ArbitrationMode` config.
2. **Hyperliquid MBO keeps racing**, at order-event granularity. This reverses an earlier draft decision to make all MBO single-arm, which would have cost best-of-N latency on the flagship feed for no benefit.
3. **One order-keyed state, two renderings.** Order-level bootstrap for order-level subscribers; a price fold — with `n` — for everyone else and for the sink.
4. **Additive, not breaking.** MBO emits `book` alongside `depth`; PROTOCOL.md stays **v1**.
5. **The `depth` deletion is deferred** to its own change. There are testers on it and no production users; it needs a deprecation window and a heads-up, not silence.
6. **The sink is a rendering,** documented in `docs/output-sinks.md` — never in PROTOCOL.md, which is the contract for our normalized protocol only.
7. **We do not extend `nautilus_trader`** as part of this work.

**What PROTOCOL.md gains, and it stays v1.** Two additive edits, both covered by the forward-compat rule: `order_id` on `BookChange`, and the replay-scope field on `subscribe` (named in the plan). Plus a documentation change with no wire effect — Market-by-Order now produces `book` as well as `depth`. Nothing is withdrawn.

### Rejected: L3 live stream with L2-aggregated replay

Recorded so it is not re-proposed. A client connecting mid-stream would receive price levels carrying no order ids, then order-level cancels and executes referencing ids it never saw. Unresolvable, and the consumer's book diverges silently. An order-level stream and an order-level bootstrap are one decision.

### Rejected: racing on `per_instrument_seq`

The sequence is per-publisher — measured, three unrelated bases for one execution. Racing or deduping on it would treat every publisher's copy as a distinct event.

## Part 1 — the canonical L3 product

**`ingest/book.rs`** gains an outward change report. `on_delta` returns `bool` today; it takes a caller-supplied buffer and reports the order-level changes an event produced — reusing the idiom `PriceBook::on_delta` already uses for `BookClear`, not inventing a second one.

**`BookChange` gains `order_id: u64`**, `#[serde(default)]`. Zero means "no order identity" — correct for price-aggregated L2, never emitted by MBO.

**`MboProcessor` emits `FeedMessage::Book`** on the `(venue, channel, instrument_id)` identity that PR #110's channel-scoped key supplies. A snapshot install is a `Clear`-led batch carrying every resting order. Batching commits per logical event, `last` true on the final batch.

**The accumulator becomes order-keyed**, materializing either the full order set or a price fold with `n` per level. A `Clear`-led snapshot installs directly rather than buffering, returning `MAX_PENDING_CHANGES` to guarding only unterminated incremental events.

**Replay is gated** by an additive `subscribe` field selecting order-level or price-level bootstrap, defaulting to price-level.

### Racing mode (distributed sourcing)
**`Coordinated` mode**
The content oracle moves down a level. Today it keys on `DepthId`, the top-N book content. For order-level output it keys on the **order event**: `(instrument_id, order_id, kind)` for adds and cancels, with `trade_id` distinguishing executions — including successive partial fills of one order. First publisher to deliver an event wins and is emitted; every other copy collapses.

**Book state stays per publisher; the racing happens on the derived output.** This is the existing architecture — today's `depth` is produced per publisher and content-raced at the arbiter — and reusing it removes the largest piece of novel machinery from this design. Each publisher's book runs its own recovery state machine as it does now; each emits its own order-level changes; the arbiter collapses them on venue identity and emits first-arrival. Gap coverage falls out: a publisher that gaps drops to `Recovering` and stops emitting, and its peers' copies simply stop being deduped away.

**Re-baselines are the one thing that must not race.** A publisher recovering via snapshot emits a `Clear` plus its full order set, which would wipe a consumer that a healthy peer is serving correctly. So a re-baseline is emitted only when no other publisher of that instrument is currently synced, decided at **one** point rather than per book — two publishers recovering together must not both conclude they are alone.

**Deriving output from per-publisher state has one hazard a shared book would not have, and it is worth naming precisely.** An execution's derived change depends on prior book state: if one publisher holds an order at 100 and another at 80 because it missed a partial fill, the same venue event yields different resulting quantities. Identity matches, content differs, and first-arrival could publish the drifted view. Reaching that state requires a book that is silently wrong while still believing itself contiguous — the known `Ready`-wedge condition, not a new failure mode — but output racing gives it a path to consumers.

The mitigation is cheap because both copies are in hand at the moment of collapse: **dedup on identity, compare content, and count mismatches** (`dz_mbo_arm_disagreement_total{venue}`). Same-identity-different-content is exactly the signature of a drifted publisher, so this converts a silent corruption into an observable. **If that counter ever fires in anger, the shared-book model is the fallback** — deduping input into one book makes the divergence structurally impossible, at the cost of the novel machinery this design is choosing to avoid.

**Three guards, and the order matters — the window is the weakest of them.**

1. **Per-order state, the primary guard.** Venues do not reuse `order_id`, so an id that has been removed — cancelled, or fully executed — must never be re-added. A late `Add` for a dead order is refused outright. This is identity-based, so it holds no matter how far out of order a copy arrives, and it is what makes the whole scheme robust rather than dependent on a well-chosen timeout. Symmetrically, a `Cancel`/`Execute` for an id the book has never seen is a signal, not a no-op.
2. **The dedup window.** Bounds how long a *seen* event is remembered so a duplicate is recognized as one. **Default 250ms, operator-settable**, and bounded by event count as well as time so a wedged or hostile publisher cannot grow it. Overflow degrades the instrument to recovery rather than silently evicting the oldest seen-event, which would reopen the very path guard 1 exists to close.
3. **A staleness filter on the venue timestamp**, where the wire carries one — adopting the operator note below. Cheap, independent of seen-set size, and it discards absurdly old copies before they reach either of the above. **Caveat measured on the wire:** Hyperliquid's `ts` is truncated to **millisecond** granularity, so it cannot order events *within* a millisecond and must never be used as a dedup key. As a coarse "older than the watermark, drop it" filter it is sound; as an identity it is not.

The important correction against the earlier draft: **window *sizing* is no longer a correctness parameter.** With guard 1 in place, an undersized window costs a redundant emission and a wasted apply, not a corrupted book. That is why 250ms is a defensible default rather than something to block the design on measuring.

**Guard 1 is two guards, and the second one is bounded.** `book.rs`'s `removed` set refuses a resurrection within *one publisher's* book. It cannot see a peer's delete, so the cross-publisher guard is a separate map at the merge point in `arbiter.rs`, and that one is capped per market — not per book, but against its *product* with the 16,384-market ceiling, because the market key comes off an unauthenticated wire. A generous per-market cap would put the aggregate in the tens of gigabytes, so it is not available and raising it is not the fix.

**When either guard cannot answer, the market re-baselines rather than guessing.** A tracked order aged out of the cross-publisher map, or two publishers disagreeing about one order's resting quantity, both mean the arms' deltas can no longer be trusted to describe the book a consumer holds. The market stops being served from deltas and is republished whole from one arm's accumulated book, counted by `dz_mbo_forced_rebaselines_total{venue,reason}`. That converts both failures from silent corruption into a bandwidth cost, and the cost is contained because an eviction only counts as a *loss* while a peer's copy could still be racing — the same horizon the dedup window bounds. A book far larger than the cap therefore streams normally; the forced re-baseline fires when cap-many distinct orders churn inside that window, which is exactly the venue for which the cap is too small. A *global* order budget shared across markets, the idiom `MbpProcessor` already uses for buffered deltas, is the way out if that turns out to be the flagship feed; it was left out of scope here.

_Operator Note:_ Consider leaning on the `venue_ts` to help throw away late data if it exists.

**N varies, including down to one.** Racing must behave identically with a single live publisher — no stall waiting for a peer, no transfer machinery engaged.

### Redundancy mode (centralized sourcing)
**`Sticky` mode**

Unchanged from the market-by-price model: book state per publisher, one elected arm serving, transfers on measured margin, silence, or per-market health. Kalshi MBP stays exactly as it is. Should a centrally-sourced MBO feed appear, it selects this mode by config and reuses this path.

## Part 2 — the Hyperliquid-compatible sink

A new `sinks/hyperliquid.rs`, sibling of `sinks/ws.rs`: own bind flag, off by default, reading the same broadcast, holding no ingest state.

- **`l2Book`** — snapshot-per-update, not deltas. `px`/`sz` as strings, `n` from the order fold. Honours `nSigFigs`, `mantissa`, `nLevels` (default 20, max 100). Emits on every book change; see *Settled* below.
- **`l4Book`** — full book snapshot then order diffs, matching the contract DoubleZero's own Hyperliquid publisher defines.
- **`trades`** — mapped from our `trade`.

Scoped to the Hyperliquid venue; `coin` maps to our symbol.

## Consequences and risks

**A drifted publisher winning a race is detected, not published.** Same-identity-different-content is caught at the merge point (`dz_mbo_arm_disagreement_total`), and because neither arm is known to be the drifted one, neither copy goes out and the market re-baselines. The residual is therefore a re-baseline *rate*, not a corrupt book: a sustained `dz_mbo_forced_rebaselines_total` is the signal to reconsider the shared-book model, which makes the divergence structurally impossible.

**A shared book was considered and rejected on risk.** It is structurally more robust — deduping input into one book makes cross-publisher divergence impossible — but it has no analogue in the codebase, and a bug in novel book-merging machinery is a far more likely source of a wrong book than a wedged publisher is. Recorded because the trade should be revisited if the disagreement counter fires, not silently forgotten.

**The per-order resurrection guard is what carries correctness**, not the window. If that guard is ever weakened or bypassed, window sizing silently becomes a correctness parameter again and an undersized value corrupts books rather than duplicating work. Any change to guard 1 — including how a lost guard entry is handled — should be treated as a change to the arbitration contract.

**An echo or mirror re-injecting frames is harmless, and may be useful.** It always arrives later, so it always loses the race and costs only a dedup lookup; and if a publisher's multicast path fails while it still feeds the mirror, the mirror becomes live redundancy. The one hazard the thought experiment surfaces is not echo-specific: a mirror replaying a backlog on reconnect produces **late-but-new** events, the same class a slow publisher produces after the fast one gaps. Guard 1 covers both.

**Memory during the transition.** The MBO processor carries the L3 book, the `last_top` suppression memo, the `depth` replay map and the order-keyed accumulator at once. The shared-book change partly offsets this — one book per instrument instead of one per `(publisher, instrument)`.

**`book` gains a second venue.** A consumer filtering on `type=book` alone begins receiving Hyperliquid as well as the market-by-price venue. The `venue` filter dimension covers it, but it is visible.

**Two products, two arbitrations, during the transition.** `depth` keeps its own content floor while `book` uses the order-event oracle. Both race, so unlike the earlier draft they will not disagree about which publisher won — but they are separate mechanisms and can drift under gap or recovery. Another argument for a short transition.

## Testing

- Real-frame L3 round-trip over the committed 44,598-order BTC fixture.
- **The load-bearing property test:** replay our emitted stream into a naive consumer book and assert it equals the processor's book. This is what catches both rejected approaches and any variant.
- **Two-publisher racing:** assert each event is emitted exactly once, that the emitted copy is the earlier arrival, and that the output is identical whichever publisher leads. Two-publisher Market-by-Order frame logs exist outside this repo and are the ideal driver; synthetic two-arm input exercises the same paths and is what the plan uses, so this is not gated on them.
- **Disagreement detection:** two publishers whose books have drifted emit the same event with different resulting quantities; assert the counter fires and that exactly one copy still reaches the wire.
- **Re-baseline suppression:** a publisher recovering while a peer is synced emits no `Clear`; a publisher recovering while every peer is also recovering does. Two simultaneous recoveries produce exactly one re-baseline.
- **Resurrection test:** an `Add` for an order already cancelled or fully executed is refused, however late it arrives. This pins guard 1, and it is the single most load-bearing test in the racing path.
- **Late-copy test:** a duplicate arriving beyond the window is a redundant emission at worst, never a book change.
- **Backlog-replay test:** a publisher reconnecting and re-sending a stale batch leaves the book identical.
- **Single-publisher test:** identical output with N=1.
- MBO never emits `order_id == 0`; the price fold including `n`.
- Golden `l2Book`/`l4Book` frames pinned against the exact serde shape Nautilus parses.
- Existing `depth` behaviour unchanged — the additive claim needs a test.

## Deferred: deleting `depth`

Its own change. Deletes `depth`, `DEPTH_LEVELS`, `NormalizedDepth`, `DepthSnapshot` and the depth `StalenessFloor` with its `DepthId` oracle; withdraws connection-lifecycle step 2's promise; flips PROTOCOL.md to **v2**. Testers are on `depth` today, so this needs a deprecation window and notice — the in-house `arb/adapters/bridge` reads only `quote`, but that is not the whole population.

## Out of scope

Upstreaming `l4Book` and `BookType::L3_MBO` to `nautilus_trader`; rendering the market-by-price venue through the Hyperliquid sink; anything execution-side.

## Settled

- **There are `depth` consumers — testers, not production.** Confirms decoupling and sets the bar for the deferred deletion.
- **`l2Book` emits on every book change.** Confirm against a live capture; the expectation is per-change rather than a timer. If the rate comes back implausible against the venue's own per-block cadence, that is a finding to act on.
- **`per_instrument_seq` is per-publisher.** Measured, not assumed.

- **Dedup window defaults to 250ms, operator-settable.** Not measured, and deliberately not blocking on a measurement: with the per-order guard carrying correctness, the window is a cost/redundancy tuning knob. Worth measuring later to tune, not to unblock.
- **Concurrent Hyperliquid senders are legitimate publishers.** Settled; no confirmation needed. An echo would lose every race anyway and would be useful as a fallback if a publisher's multicast path failed while the mirror kept feeding.

## Open

Nothing blocking. The design is ready to plan.
