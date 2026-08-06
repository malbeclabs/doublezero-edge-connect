# Lashay L1/L2 in edge-connect — design

**Date:** 2026-08-06
**Status:** Draft — pending review
**Scope:** `src/ingest/{feeds,codec_mbp,pricebook,processor,arbiter,receiver}.rs`, `src/sinks/ws.rs`, `src/model.rs`, `PROTOCOL.md`. Perps L1 + L2. Sports L2 needs only its `FEEDS` rows (§8.1); converting the HL MBO **output** to a true L3 book gets its own doc (§2.2).

## Goal

Be an easy upgrade for someone already consuming Lashay's `orderbook_delta` WebSocket. Reference consumer is NautilusTrader's in-progress adapter for this venue (bootstrapped 2026-08-02 as an empty crate, no code yet). We do **not** write that adapter — we shape our output so it, or a thin consumer of the same types, maps onto us 1:1 (§6).

## 1. Ground truth

**Protocol.** `edge-feed-spec` `market-by-price/spec.md` (PR #20, 2026-07-31). Magic `0x4442`, three ports, schema v1. Design Principle 11: *"built for the complete book — every price level with resting quantity."* `Depth Bound = 0` is a positive claim of completeness.

**Validation oracle exists.** `edge-multicast-ref` `go/marketbyprice-parser` (PR #29) decodes `0x4442`, so `codec_mbp.rs` ships byte-validated rather than draft-only — the trap `codec_midpoint.rs` is in.

**Feed is live and measured.** The perps MBP group on `233.84.178.4`, prod-enabled 2026-08-05 (lashay `4ec86cd`); ports mkt 31000 / ref 41000 / snap 51000; registry `lashay`, Source ID 3. Measured 2026-08-05: **402 frames/s ≈ 321 kbps** over 13 instruments / ~1,210 levels. On the event plane, 200 busiest markets carried 210,545 deltas in 120 s — two orders larger, and the load §2.1 has to survive.

**Publisher already normalizes YES/NO**, so do not re-derive it. `event_book.rs` reflects NO bids onto the YES axis as asks at `10_000 − p`, exact integer arithmetic, endpoints excluded, pinned by `snapshot_maps_yes_to_bids_and_no_to_inverted_asks`. That is also the convention ccxt, pmxt and the third-party Python clients independently converged on, and our existing `depth` shape already matches it.

**Two arms, incomparable clocks.** prod1/prod2 become perps-only, one FIX-sourced and one WS-sourced, on separate `channel_id`s. Lashay's `2026-07-19-feed-source-selection-design.md` §2 records why the publisher refuses to race them: no stable entry id (`278`/`880` absent from `35=X`), no per-entry venue timestamp (`60`/`273` absent from `35=W`/`35=X`), and header `52` SendingTime is FIX *transport* time, "not comparable with the WS `ticker.ts_ms`". Splitting the arms by transport moves that race into edge-connect.

## 2. Decisions

### 2.1 Full depth, incremental output. No top-N, no compatibility path.

The wire is full-depth and incremental, so the output is too. Re-serving full state per delta discards the reason the spec is snapshot+delta — its own sizing case is ~5,000 levels/instrument, ~150 KB of JSON per message. A bounded top-N view was rejected because real Lashay liquidity sits well below 10 levels, so it would misrepresent the book.

### 2.2 One book-output model from edge-connect.

**Scope: the WebSocket output only.** Multicast decode is untouched on both feeds — same `0x445A`/`0x4444`/`0x4442` codecs, same `book.rs`, same recovery. What changes is what we hand a consumer. Full-state `depth` is deleted and both feeds re-serve as the incremental message in §5, because two output models would mean two shapes, two dedup identities, and a PROTOCOL.md documenting both.

**The output shape is dictated by what Nautilus takes as input** — see §5 for the message and §6 for the mapping. Both feeds land on `OrderBookDeltas` and differ only in whether a change carries an `order_id`, which is exactly the axis Nautilus uses to distinguish `L2_MBP` from `L3_MBO`.

**MBP outputs `L2_MBP`; MBO outputs `L3_MBO`, never aggregated to L2.** MBP's wire is price-aggregated, so there is no order identity to pass on. MBO's wire is order-level and always has been — `book.rs` already holds `orders: HashMap<u64, RestingOrder>` — and we currently **flatten it on the way out**, aggregating to top-10 price levels before emitting. That throws away precisely what a market-by-order feed exists to deliver. Passing the real `order_id`s through costs nothing at decode time.

**Stopping the flattening needs its own design doc.** Not a new decode, but a new output contract: order-level deltas, `L3_MBO` book semantics for consumers, and different recovery and sizing questions than price levels have. Out of scope here beyond the two hooks it needs — §5's message type accommodates `order_id`, and PR 7 is the seam.

Blast radius of deleting `depth` is nil: the June demo is quote-only with zero references to `depth` or `OrderBook`, and no other consumer exists, so the only assertions to update are ours (`tests/e2e.rs`, `tests/dedup.rs`, `tests/common/assertions.rs`). `DEPTH_LEVELS = 10` (`processor.rs:35`) goes with it.

### 2.3 One arbitration module, with the venue clock as a parameter.

Every dedup we do today buckets two copies on a **shared venue-assigned coordinate** and uses content only to discriminate within a bucket — quotes and MBO depth on `source_ts`, trades on venue `trade_id`. The Lashay FIX/WS pair has no such coordinate, and a content hash cannot substitute: cross-arm-common `LevelUpdate` fields reduce to `(side, price, quantity)`, which recurs constantly on a coarse bounded price grid. A level oscillating 100 → 0 → 100 emits byte-identical updates, and collapsing those leaves a subscriber holding **0 at a price that has liquidity** — undetectable until the next snapshot, since per §Crossed-Book Monitoring a missed level deep in the book crosses nothing.

**The abstraction:** per key, exactly one publisher is authoritative; what varies is when authority transfers.

| Mode | Transfers on unhealthy leader | Transfers on higher coordinate |
|---|---|---|
| `Coordinated` — comparable venue clock (HL; Lashay WS-vs-WS) | yes | **yes**, every tick |
| `Sticky` — no comparable coordinate (Lashay FIX-vs-WS) | yes | n/a |

`Sticky` is the agreed shape: pick a winner and hold it, other arm as backstop, rather than a true race — revisitable if unique ids or venue-side timestamps ever arrive, with leaked duplicates accepted as tolerable. It is `Coordinated` minus the coordinate rule; the stickiness substitutes for the missing anti-rewind guard. Today's `StalenessFloor` (`arbiter.rs:217-355`) becomes the `Coordinated` variant; `Publisher` (`:84-102`), `Admit<P>` (`:193-215`) and the per-venue metric children are unchanged. A `FEEDS` row declares its mode — if Lashay ever exposes `60`/`273`, that row flips to `Coordinated` as config, not a rewrite.

Two rules make it one module rather than two:

- **"Unhealthy" includes the leader sitting in `gap`/`awaiting-snapshot`, in both modes.** Under full-state depth a lost level self-heals on the next message; under incremental output it does not heal until the next snapshot.
- **`Sticky` authority is per `(venue, market)` — per instrument, never per level.** Per-level leadership interleaves the arms, which is what corrupts state.

**Initial election: a bounded sampling window.** On feed activation both arms are ingested and their per-market arrival skew measured briefly; the faster becomes authoritative. The first arm delivering a usable book is provisionally authoritative so there is no dark start, and the window closes with at most **one** re-election — one extra re-baseline during warm-up. A CLI flag pins a preferred source and skips sampling, which is also the escape hatch for a known-degraded arm. After the window authority moves only on a health verdict; flapping authority re-baselines every consumer's book.

**Rejected: state-convergence dedup** (emit only when an arm's delta changes a shared published book) — it dedups but does not *order*, so a lagging arm's copy of an older transition rewinds the book and then flaps through that arm's replay of the leader's history.

A content hash *does* belong in **metrics**: the non-authoritative arm later reporting the same `(side, price, quantity)` is a content-matched skew sample, where false matches only add histogram noise. That yields the FIX-vs-WS lead time and an arm-divergence counter for free.

### 2.4 The arm axis is the source IP. `channel_id` names the instrument set.

Settled upstream by Lashay PR #83 (`docs: revise the topology design against measurement`), which replaces the earlier per-plane `channel_id` overload with three axes, each naming one thing:

| Axis | Names |
|---|---|
| `(group, dst_port)` | the **channel** — the competition |
| `source_ip` | the **publisher instance** — the arm |
| `channel_id` | the **instrument set**, one meaning everywhere |

For us that means `channel_id` is always **part of** the published market key, and the transport arm (FIX vs WS) is identified by the **datagram source IP** and deliberately **not** in that key — so both arms collide on one key and arbitrate against each other (§2.3).

That rule is normative on the wire, per #83: *"Sequence and reset counters are scoped to `(source_ip, group, port)` … A consumer that tracks sequence per destination port alone reads that interleave as continuous loss, and a consumer that tracks reset per port alone sees one arm's restart as a reset of both."* Two consequences we inherit:

- **We already key this way, but not everywhere.** `FrameCtx.publisher` is the source IP (`receiver.rs:105-108`), `Publisher::Edge(IpAddr)` is the arbitration identity (`arbiter.rs:84-102`), `TobProcessor` holds `seq: HashMap<IpAddr, SeqTracker>` (`processor.rs:113`), and MBO books key on `(IpAddr, instrument_id)`. But **`RefDataState` is one shared instance per processor** (`processor.rs:480`, `:107`) and clears every definition on any `reset_count` change (`subscriber.rs:53-61`) — which is per-port state, and exactly what #83 says stops being correct with a second publisher. Fixing that is PR 1 (§7).
- **Any-source join receives both arms**, and #83 expects the loser discarded in userspace. That is our arbitration. Single-arm consumption would need an SSM `(S,G)` join, which #83 raises as gate G9 — not something we depend on.

**`channel` is a client filter key.** Arm identity is *not* client-selectable: `Sticky` publishes the authoritative arm and drops the other, so there is exactly one coherent book per market to hand a client. Handling failover for the consumer is the point; the cost is that the non-authoritative arm's ingest bandwidth is spent and discarded.

Note the arms do **not** share a `channel_id` yet — perps is deployed as `channel_id` 1 and 2 pending
#83's renumbering to a single `id`. Keying on source IP is correct before and after that migration,
so nothing here waits on it.

### 2.5 Identity is the instrument tuple, not `(venue, symbol)`.

Two distinct keys — conflating them is how §2.4 goes wrong:

- **Per-arm book state:** `(publisher, channel_id, instrument_id)`. Each arm reconstructs independently; their `per_instrument_seq` spaces and snapshot cycles are unrelated.
- **Published market key** (arbitration, replay map, client filter): `(venue, channel_id, instrument_id)` — no source IP, so both arms resolve to one entry.

**Assumption this rests on:** both arms mint the **same `instrument_id`** for the same market, and ids are unique within a channel. Today they would not — the registry is per host, so two arms drift — and the upstream fix is in flight (a ticker-derived `instrument_id`). We depend on the *property*, not the mechanism, so any scheme that delivers agreement plus within-channel uniqueness works unchanged here. If agreement ever fails, the two arms present as two markets and arbitration silently stops collapsing them — worth a divergence counter (§2.3) rather than trust.

The spec requires it: *"Subscribers consuming multiple channels MUST key their internal instrument map by the tuple `(channel_id, instrument_id)`."* Today we key the depth floor, the replay map (`model.rs:231`) and the client filter all on `(venue, symbol)`.

`symbol` is 16 bytes filled by `pad_symbol`, which silently keeps the **rightmost** 16 — no hash, no error, no length check. Perps are unaffected (max 11 bytes on prod). Event and sports tickers are not: 95.7% of the 74,546 open markets measured 2026-07-31 exceed 16 bytes, with 3,451 truncated forms colliding across up to 9 markets each — `KXNFLGAME-26AUG15DALSEA-SEA` and `KXNCAAFGAME-26AUG15DALSEA-SEA` both become `6AUG15DALSEA-SEA`. Those figures are stale; see §10 Q4.

Keying on the tuple makes us correct regardless, and `symbol` becomes a display label.

### 2.6 MBP emits no trades.

Trades are not required to reconstruct book state, and `FEEDS` already carries `emit_trades` so one feed owns a venue's tape. Lashay TOB owns trades. This also sidesteps the one case with no sound dedup: WS trades carry a venue trade id, FIX carries none.

## 3. Modules

**`ingest/codec_mbp.rs`** — decoder over `codec_common::decode_frame_with(buf, 0x4442, …)` (`codec_common.rs:83-87`). New payloads `LevelUpdate 0x40` / `BookClear 0x41` / `SnapshotLevel 0x42`; byte-identical to MBO: `BatchBoundary 0x13`, `InstrumentReset 0x14`, `SnapshotBegin 0x20` (prefix-superset — MBO's 36 bytes plus `Depth Bound` at offset 36), `SnapshotEnd 0x22`; shared with TOB: `Heartbeat 0x01`, `InstrumentDefinition 0x02`, `Trade 0x04`, `EndOfSession 0x06`, `ManifestSummary 0x07`, `Liquidation 0x08`. `0x03`/`0x05` are reserved and must not decode. MBP enums decode **permissively** (any `u8` accepted) — the opposite of TOB's strict decode.

**`ingest/pricebook.rs`** — new `PriceBook`: two `BTreeMap<i64, LevelState>` plus the snapshot/delta recovery machine. Codec-agnostic like `book.rs` but far thinner — no `orders` map, no per-order aggregation, since the wire delivers aggregated absolute quantities. `book.rs` (L3, order-keyed) is the wrong shape and is not reused.

**`ingest/processor.rs`** — `MbpProcessor`, one `PriceBook` per `(publisher, instrument)`, mirroring `MboProcessor` (`FrameProcessor` at `receiver.rs:120-127`).

**`ingest/arbiter.rs`** — `StalenessFloor` generalized per §2.3. **`sinks/ws.rs`** — output plus filters (§5). Plus `FeedKind::MarketByPrice` and the `FEEDS` rows. `FeedPorts::ThreePort` and `recv_any` already handle three ports, so no transport work.

## 4. Spec conformance the MBO path gets wrong for MBP

Each is a silent-corruption bug if missed, and none is shared with `book.rs`.

1. **`SnapshotLevel` attributes to the most-recent `SnapshotBegin` per channel, not by `snapshot_id`.** `snapshot_id` is monotonic per `(channel_id, instrument_id)`, so two instruments can be mid-snapshot at the same id; it *validates* the association and must never be the key. `MboProcessor` routes `SnapshotOrder` by the originating publisher's building book — a different rule.
2. **The snapshot-while-`ready` discriminator is `Last Instrument Seq`, not `Anchor Seq`.** `Anchor Seq` is channel-wide while `last_applied_mktdata_seq[I]` advances only on `I`'s own deltas, so comparing them rebuilds every instrument every rotation. The spec names this as a defect it fixes vs MBO.
3. **`depth_bound` defaults to *unknown*, never `0`.** `0` is a positive publisher claim of completeness; defaulting to it makes a never-snapshotted instrument assert completeness on its own. Levels beyond a declared bound are unknown, not empty.
4. **`Action` must not gate the apply.** Apply by quantity alone — `0` removes, else set. Count `Action` disagreements as divergence without changing the result.
5. **Bound the delta buffer with a defined overflow policy.** Spec's worst case is ~30 M messages / ~1.4 GB. Recommended: drop the largest instrument's buffer, mark it `gap`, count the event.
6. **`BookClear` with `Scope = 1` and `Clear Side = 2` is malformed** — discard and count.
7. **`EndOfSession` is per-arm.** Today's MBO handler clears every publisher's book and the venue's whole depth floor; with two arms, one arm shutting down would tear down a live published book. It must demote that arm instead.
8. **`per_instrument_seq` does not reset at snapshot boundaries** — only on `Reset Count` change.
9. **No `ChannelReset 0x05`** here; a channel reset is signalled by the frame header's `Reset Count`.

## 5. WebSocket output

One message type replaces `depth`: a batch of level changes, each carrying an explicit **action**. A re-baseline is not a separate type — it is a batch whose first entry is `clear`. That shape is forced by §6, where only a clear action re-baselines and a boolean snapshot field is a no-op.

```json
{"type":"book","venue":"Lashay","symbol":"KXBTCPERP","channel":2,"instrument_id":41,
 "changes":[{"action":"update","side":"bid","price":0.6200,"size":150},
            {"action":"delete","side":"ask","price":0.6300,"size":0}],
 "snapshot":false,"last":true,
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

**`(venue, channel, instrument_id)` is the identity; `symbol` is a label.** This is new — the wire carries only `venue` + `symbol` today, and `NormalizedInstrument` has no id field. It has to change, because `symbol` is a truncated 16-byte tail that collides across markets (§2.5), so a consumer keying on it merges two books. A consumer that wants a stable key uses the triple; `symbol` is for display and for the convenience of venues where it happens to be unique (all of perps). `instrument_id` appears on `instrument` messages too, so the mapping is learnable on connect.

- **`last`** marks the final batch of a logical book event. Mandatory — a buffering consumer wedges permanently without it, including on a re-baseline that is only a `clear`.
- **`snapshot`** is advisory, for consumers distinguishing a rebuild from ordinary activity. It is deliberately **not** what re-baselines: `changes[0].action == "clear"` is.
- **`channel`** is the wire `channel_id`, i.e. the competition — filterable (§2.4). Arm identity is not on the wire; a consumer gets one arbitrated book.
- **Re-baseline** (connect, recovery, authority switch) is `clear` + the full level set, `snapshot: true`, `last: true` on the final batch.
- Timestamps keep the four-stamp contract, `0` = not available.

**Filtering** (`SubFilter`, `ws.rs:106-125`) gains `channel` and message `type`, threaded through *two* match paths — `SubFilter::matches` and the inline venue-only comparison in the no-symbol/`status` branch at `ws.rs:363-388`, which does not call `matches`. Replay on connect runs before the select loop and is currently unfiltered; with incremental depth it must be filter-scoped, so the connect sequence changes shape. `PROTOCOL.md` goes to v2 — additive types are covered by the forward-compat rule, deleting `depth` is not.

## 6. Reference consumer: NautilusTrader

Verified against `nautechsystems/nautilus_trader` @ `05b709b` (v2.0.0rc3). The Lashay adapter has no code yet, so we design against the types it must emit, with the shipped Polymarket adapter as the prediction-market idiom.

- **`BookType`.** Lashay MBP is `L2_MBP`: `order_id` is discarded and replaced by a hash of `price.raw` (`aggregation.rs::pre_process_order`), so emit `0` — Polymarket does exactly this. Hyperliquid MBO is `L3_MBO` and carries real `order_id`s; note a zero id there silently degrades to price-keyed L2, so emitting `0` on an MBO feed is a correctness bug rather than a convention.
- **`OrderBookDeltas`** batches `OrderBookDelta { action, order, flags, sequence, ts_event, ts_init }`. Batch-level `flags`/`sequence`/`ts_event` are **not settable** — they are copied from the last delta, so our per-batch fields must ride the final entry.
- **`F_SNAPSHOT` does not clear an L2 book.** For `L2_MBP`/`L3_MBO` the dispatcher branches on `action` alone (`book.rs:361-372`); `flags` is read only by `pre_process_order`, which consults `F_TOB` and `F_MBP`, never `F_SNAPSHOT`. **Only `BookAction::Clear` re-baselines** — hence §5's structural re-baseline.
- **`F_LAST` is mandatory on the last delta of every batch**, including a lone `Clear` on an empty book. Omitting it wedges consumers with `buffer_deltas` enabled.
- **Nautilus does no sequence-gap detection for books.** Every adapter rolls its own (Binance buffers and replays against a REST snapshot; OKX runs a `BookSequenceOutcome` machine). We already do this per-arm, so we hand consumers a stream that never needs it — worth stating in `PROTOCOL.md`.
- **Instruments:** a Lashay binary market maps to `BinaryOption` (as Polymarket); a Lashay perp to `PerpetualContract`, not `CryptoPerpetual` (which forces a `base_currency` and has no asset class). `activation_ns` and `expiration_ns` are both non-optional on `BinaryOption`.

v2 renamed this surface (`subscribe_order_book_deltas` → `subscribe_book_deltas`) and the June demo pins v1. The wire is unaffected, but v2 has no Python data client and no generic JSON client, so a native consumer is a Rust crate.

### 6.1 Optional second emitter: the tardis-machine shape

Nautilus's Tardis adapter connects to `{base_url}/ws-stream-normalized?options=<json>` (`machine/mod.rs:114`, `data.rs:123`) with `base_url` configurable and a `TARDIS_MACHINE_WS_URL` env fallback (`common/urls.rs:31`, `config.rs:112`), so emitting its normalized `book_snapshot` shape makes this feed consumable by an adapter that already ships. Blocker: `TardisExchange` (`common/enums.rs:180`) is a closed enum with no `#[serde(other)]` — `Hyperliquid` is a variant (`:222`), **Lashay is not**. Works today for the existing HL feed; needs a one-line upstream PR for Lashay.

Optional, not the contract: a vendor shape we do not control, its exchange list gates which venues can use it, and `interval: 0` is a full-state model §2.1 rejects.

## 7. PR stack

1. **Per-publisher state + arbitration module.** Two changes that are one idea — scope per-publisher state correctly, then arbitrate on top of it.
   - `RefDataState` becomes `HashMap<IpAddr, RefDataState<D>>`, matching what `TobProcessor` already does for `SeqTracker`. Today it is one shared instance clearing all definitions on any `reset_count` change, which is per-port state and incorrect under #83's rule. Not currently reachable (see §7 note) but a hard prerequisite for consuming two arms.
   - `StalenessFloor` → `Coordinated`/`Sticky` with the shared unhealthy-leader transfer; `FEEDS` declares its arbitration mode. No protocol change.
2. **Filter dimensions** — `channel` + `type` on `SubFilter`, both match paths, replay scoping.
3. **`codec_mbp.rs`** — validated against `marketbyprice-parser`.
4. **`PriceBook` + `MbpProcessor`** — §4 in full, bounded delta buffer.
5. **Incremental output** — `clear` + full set, batch-end discipline; `PROTOCOL.md` v2.
6. **Lashay feed rows** — the perps TOB group (a `FEEDS` row plus extending the venue→code match in `feeds.rs`'s `every_feed_has_a_group_code`, which panics on an unknown venue) and the perps MBP group. Both group codes come from the deployment config; this doc does not restate them.
7. **MBO migration** — Hyperliquid onto the incremental output as a true `L3_MBO` book; delete `depth` and `DEPTH_LEVELS`. Needs its own design doc first (§2.2).

1 and 2 are the prerequisites and depend on nothing shipping upstream.

**Why PR 1's `RefDataState` fix is a prerequisite and not an incident:** the HL fleet is 7 publisher hosts separated **by destination port** (host index N → base + N×100), and our `FEEDS` rows bind `9201/9202` and `10201/10202/10203` — `aws-tyo-hl-mainnet2`'s offsets, labelled "host2 ports" in `feeds.rs`. We consume exactly **one** HL publisher, so there is no second `reset_count` to thrash yet. Two follow-ons to track separately, both out of scope: binding all seven so there is something to race, and HL's own fleet gating its shared-port migration on "receiver de-dupe … a separate, unstarted workstream" — which is PR 1, so their migration waits on us.

## 8. Out of scope

### 8.1 Sports L2 — adding the FEED rows, once upstream is publishing

Sports L2 is the same `0x4442` protocol on a different group, so everything in §§2–6 serves it unchanged. The only missing piece is the `FEEDS` rows, and they wait on the sports MBP group actually publishing. When it does, sports is a row plus a channel registry — not a design.

### 8.2 Also out

- **Racing FIX against WS per event** — until `60`/`273` and `278`/`880` exist. `Sticky` is the interim and the flip is config.
- **A Nautilus Lashay adapter** — upstream is building one.
- **Historical L2** — the loudest stated demand in the ecosystem, but a different product.
- **FIX or DBN output.** Firms on the venue's own FIX market-data session already have a normalized internal bus; JSON-over-WS is a step backwards for them. Named so it is a decision, not an omission.

## 9. Validation

- `codec_mbp.rs` field-by-field against `marketbyprice-parser`, plus committed real-frame fixtures from `233.84.178.4`. Shared-with-TOB types reuse the byte-validated TOB layout, pinned by cross-codec equality tests.
- `PriceBook` unit tests per §4, each conformance item named in its test.
- Arbitration: table-driven over both modes, including §2.3's lag case — a lagging arm must never rewind the published book.
- End-to-end: a live capture replayed through the pipeline, asserting book equality against `marketbyprice-parser` at every consistency point.
- **Worth measuring early:** recurrence-interval distribution of identical `(side, price, quantity)` within one arm, against inter-arm arrival skew. Decides whether a windowed content dedup could ever be safe, needs only one arm, and is runnable now by pointing `marketbyprice-parser` at the live group. Capture holds only BBO `ObservationRow`s, so this is a JSONL pass, not a ClickHouse query.

## 10. Open questions

1. **Do FIX and WS observe the same per-level transition sequence?** The spec notes a feed inherits its upstream's conflation, and FIX `35=X` is an incremental refresh that characteristically conflates. The question is whether the `orderbook_delta` WS provides is on the same snapshot as the FIX order book. Agreed method: record ~60 s of one very liquid market from each side and compare; capture access is being arranged. **Nothing here blocks on the answer** — `Sticky` needs none, which is why it is the default; a match would only make per-level counting available as an optimization behind the §2.3 seam.
2. **The transfer threshold.** §2.3 settles how authority moves; the number is owed. Too tight and jitter flaps authority, re-baselining every consumer; too loose and a dead arm holds authority through the outage the second arm exists to cover. Derivable from §1's measured rate plus observed skew.
3. **`edge-feed-spec` still contradicts #83, and that is the one piece not yet in flight.** #83 makes `(source_ip, group, port)` scoping normative for Lashay, but MBP §Channel Reset still tells a subscriber to discard *all* channel state on a `Reset Count` change, and `marketbyprice-parser` still tracks sequence per port. A third party implementing the spec has no path to #83's rule, and our own reference parser disagrees with it. The fix is a small alignment PR against MBP **and** MBO — both carry the same three sentences (`Reset Count` at `market-by-order/spec.md:88`, `Snapshot ID` at `:480`, Channel Reset at `:643`) — citing #83 rather than re-arguing it.

   Two things to settle in that PR. **Vocabulary:** #83 uses "channel" for `(group, dst_port)` and `channel_id` for the instrument set, where the spec uses "channel" for the `channel_id`-scoped state machine. Pick #83's, since it is the reviewed one. **Scope:** the spec puts transport and addressing out of scope, so naming the source address normatively reaches outside it — the honest form is to require state be scoped per publisher instance and leave *identifying* instances to the deployment, as concrete port assignments already are.

   Two facts make it consolidation rather than a new position. The same defect exists on Hyperliquid and is not Lashay-shaped: that publisher signals in-process resets with a **non-spec** `0x05 ChannelReset` (its own `GAPS.md` GAP-2) and never bumps byte 21, while the HL recorder acts on `0x05` and never compares byte 21 — agreeing by accident on a non-spec extension. And DoubleZero already wrote the per-instance rule once, in the order-intent feed design: *"`Reset Count` change re-latches per-`(host, Channel)` sequence tracking only."*
4. **§2.5's truncation figures are stale, and nothing re-measures them.** Data hygiene rather than a blocker, now that §8.1 does not gate on collisions. The universe has been re-measured twice since — PR #83 reports 79,956 open markets, `feat/sports-catalog-classifier` reports 84,360, and the flat `/markets` endpoint gives **1,371,003** (a 16× gap, so which endpoint a number came from is part of the number) — but **neither re-measured symbol length or collisions**, and PR #83 now contradicts itself: 79,956 in its §2 against 74,546 in its §10.1. Nothing anywhere computes ticker byte-length or collision counts, so cite the 2026-07-31 numbers with their date or not at all.
