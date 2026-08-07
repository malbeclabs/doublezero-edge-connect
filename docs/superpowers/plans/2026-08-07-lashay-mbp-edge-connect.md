# Lashay L1/L2 (market-by-price) in edge-connect — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest the Lashay perps market-by-price (`0x4442`) multicast feed alongside its top-of-book feed, arbitrate the two redundant publisher arms whose clocks are not comparable, and re-serve the reconstructed L2 book over the WebSocket as an incremental `book` message a NautilusTrader-shaped consumer maps 1:1.

**Architecture:** Three layers, built bottom-up. (1) **Arbitration** — today's `StalenessFloor` becomes the `Coordinated` variant of one arbitration module; a new `Sticky` variant elects exactly one authoritative arm per market when the arms have no comparable venue clock. (2) **Ingest** — a new `codec_mbp.rs` decoder plus a `PriceBook` recovery state machine and `MbpProcessor`, mirroring the existing MBO pair but price-keyed rather than order-keyed. (3) **Output** — a new incremental `book` WebSocket message carrying the `(venue, channel, instrument_id)` identity triple, plus `channel`/`type` subscription filter dimensions.

**Tech Stack:** Rust 2021, tokio, `tokio-tungstenite`, `prometheus`, `serde`/`serde_json`, `anyhow`, `tracing`, `BTreeMap` for price-ordered book sides.

**Design doc:** [`docs/superpowers/specs/2026-08-06-lashay-mbp-design.md`](../specs/2026-08-06-lashay-mbp-design.md). Read its §§2–7 before starting. Where this plan and the design disagree, the plan is newer — see *Reconciliation* and *Three decisions* below, which record every divergence and why.

---

## Progress

**Status: Tasks 1–10 are done, every step. Tasks 11–14 remain.** That is the whole stack below the processor: the trade-tape fix, per-publisher reference data, the arbitration-mode seam, single-arm authority and its matcher, the `channel`/`type` filters, the market-by-price decoder, the price book, and the incremental `book` wire message. Nothing on the ingest side is reachable from a running process yet — no `FeedKind`, no `FEEDS` row, no processor — so the branch is behaviour-neutral apart from Tasks 1 and 2, which are live bug fixes, and Task 6/10's additive WS surface.

**Task 11 is next and nothing blocks it.** `MbpProcessor` is the piece that makes everything above reachable: it drives `PriceBook` from `codec_mbp`, emits `book`, and is where Task 12 then wires `authority` + `arm_race`. Two things it inherits that are not in its own section: it must key the open snapshot group and the per-instrument sequence **per `Channel ID`** (the live sports feed shards across three), and it must not restate the decoder's wire enums.

**Wire facts the fixture capture settled, which later tasks need.** Read `tests/fixtures/PROVENANCE.md` for the full record; these are the ones that change a decision:

- **The market-by-price deployment is `Channel ID`-sharded** — the live sports feed runs three channels on one group (10/63/120), each an independent state machine with its own snapshot cycle, with zero instrument-id overlap between them. Anything tracking an open snapshot group, or a per-instrument sequence, must key **per channel**; a port-wide slot mis-attributes every level of an interleaved channel (spec §"Scoped to the channel, not to the port"). Task 11's `MbpProcessor` inherits this directly.
- **But the older perps feed's two arms stamp *different* `Channel ID`s (1 and 2) for an identical instrument set.** Task 4's `MarketKey = (venue, channel_id, instrument_id)` deliberately excludes the arm so both arms contest one key — with per-arm channel ids they never would. Treated as a defect of a publisher being retired (raised with its author 2026-08-07), so **Task 4's key stands as planned**; re-check against the next capture before building Task 12 on it.
- **Symbols overflow the 16-byte wire field on the sharded feed**, and one truncation collides across two instrument ids. `InstrumentSnapshot`/`DepthSnapshot` are keyed `(venue, symbol)`, so this is a live collision risk for Tasks 11–12, not a cosmetic one.
- **Group addresses, ports and `source_id`,** which the *Blocking open question* below was missing half of: market-by-price `233.84.178.4` (perps) / `233.84.178.20` (sports), top-of-book `233.84.178.3` (perps) / `233.84.178.17` (sports). Perps ports `31000`/`41000`/`51000` mktdata/refdata/snapshot and top-of-book `7576`/`7577`; sports encodes the channel in the port (`33010`/`43010`/`53010` for channel 10, etc.). Every frame carries `source_id = 3`, which `codec::source_name` does not map — Task 14 adds it. **Still missing: the DoubleZero group *codes* for `Feed.code`**, which is the other half of that question and the part that actually blocks Task 14.

| Task | State |
|---|---|
| 1 — `trade_id == 0` bypass | **done** (`aafc871`, `2eeb93b`; review fixes `b2e9264`, `4d19da6`) |
| 2 — per-publisher `RefDataState` | **done** (`a459c36`, `4f59d78`; review fix `23304c6`) |
| 3 — arbitration mode plumbing | **done** (`8ad3377`; review fix `4d19da6`) |
| 4 — `StickyAuthority` | **done, then amended** (`5b2d045`, `3286b1c`, rescoped in `6d004b4`) — authority is per **arm** for speed and silence, per **market** for health only. **Read Task 4's ⚠️ AMENDED block before building Task 12.** |
| 5 — re-election sampling | **done, then amended** (`4229f4e`, rebuilt in `6d004b4`) — the specified statistic was **inert**; replaced by `ingest::arm_race`, a cross-arm trade matcher on our own receive clock, pooled per arm. **Read Task 5's ⚠️ AMENDED block.** |
| 6 — `channel`/`type` filters | **done** (`bb75386`, `35c0c20`; PR #108) |
| 7 — `codec_mbp` frame walk | **done** (`d12ff44`, `f89e68b`; review fix `75ce041`) |
| 8 — `codec_mbp` price types | **done**, all steps (`1b003a3`, `f89e68b`; fixtures + real-frame tests in the Step 9 commit) |
| 9 — `PriceBook` | **done** (`0f018cb`, `cc69ea6`, `e6bccee`; review fixes `503b6a6`, `3bf9a09`) |
| 10 — `book` wire message | **done** (`294ac74`, `35c0c20`; PR #108) |
| 11 — `MbpProcessor` | not started — **the next task, and nothing blocks it** |
| 12 — book authority gate | not started |
| 13 — runtime tape ownership | not started |
| 14 — Lashay feed rows | **blocked** (see *Blocking open question*) |

**Where the work lives.** All of it is on the single long-lived branch **`bdz/lashay-mbp`**, not one branch per task — see the amended bullet below. It was first built as four stacked-in-name-only PRs (#100–#103, all off `main`), reviewed as a set, then combined and closed in favour of this branch.

**What the set review changed, and what a later task inherits from it.** Six fixes landed on top of the six tasks. Three matter to whoever picks this up:

- `pricebook` no longer restates the wire enums — it imports `SIDE_*`/`CLEAR_SIDE_*`/`SCOPE_*` from `codec_mbp`. Task 11 passes a decoded `side` byte straight through, and two copies that drifted would swap bids and asks with every sequence check still passing. **Do not reintroduce a local copy.**
- `PriceBook`'s level-cap overflow returns `DeltaOutcome::Overflow`, not `Gap`; the two leave different `status` behind. Task 11's metrics must not merge them.
- Task 1's zero-id tape owner hands over after 5s of silence rather than latching forever, so `dz_trades_no_id_conflict_total` still means *concurrent* double-printing once Tasks 4 and 13 start moving tape ownership at runtime.

**Tasks 4 and 5 shipped and were then restructured — Task 12 builds on the amended shape, not on what those sections originally specified.** Both carry a ⚠️ AMENDED block at the top; Task 4's ends with an explicit five-point *Task 12 contract changes* list. In one line each: authority is elected **per arm** for speed and silence and only overridden **per market** for health (per-market silence was flapping on every quiet market — 93 of 1,239 sports instruments saw any update at all in 39 s), and the speed statistic is a **cross-arm trade matcher on our own receive clock** because the originally specified one was provably inert.

Two tests are correct now and are **meant** to fail later; both say so in their doc comments. `feeds::tests::at_most_one_trade_emitting_row_per_venue` is superseded by Task 13's runtime assertion, and `existing_venues_are_coordinated` excludes `Lashay` so Task 14's `Sticky` rows don't read as a regression.

**How to hand this off, and how to pick it up.** This file is the working record, so keep it current as you go rather than at the end:

- Tick each `- [ ]` to `- [x]` as its step lands, and update the row above (`not started` → `in progress` → `done`, with the commit SHA once a task's final commit exists).
- Commit the plan update **with** the code it describes, in the same commit. A ticked box with no commit behind it is worse than an unticked one.
- If you stop mid-task, add a line under that task's row saying which step you stopped at and anything you learned that the plan does not already say — especially a step that turned out wrong.
- ~~Tasks are sequential: each branches off its predecessor's tip (Task 1 off `main`).~~ **Amended after Tasks 1–3/7–9 shipped:** the whole feature is built on **`bdz/lashay-mbp`**, so there is no PR gate between tasks. Start the next task from that branch's tip; a delegated build takes its own branch off it and merges back. The task *order* is still a dependency order — a later task will not compile without its predecessors — and Tasks 4–6 are unbuilt, so anything depending on `authority` (Task 12, and the `Sticky` half of the arbiter) is still blocked on them.
- Task 14 is the only blocked one and it is last, so a pickup never has to wait on it.

---

## Global Constraints

Every task's requirements implicitly include this section.

- **The venue is `Lashay`** — in source, comments, tests, fixture names, file names, branch names, commit messages, and PR titles and bodies. `Lashay` names the venue this bridge ingests; it is never part of an *identifier* that belongs to something else. Refer to external packages, crates, services and multicast group codes by their own names, and describe them rather than renaming them. (In prose, `Lashay-shaped` and the like are ordinary English — the rule is about identifiers.)
- **Never credit Claude or any AI.** No `Co-Authored-By`, no "Generated with Claude Code", no AI-attribution comments.
- **Everything lands on `bdz/lashay-mbp`**, the long-lived feature branch, so the feature is reviewed once when it is whole rather than task by task. Work on that branch directly, or on a short-lived branch off its tip that merges back. (This replaces the original "one branch per task off its predecessor's tip" rule; the four task branches that produced Tasks 1–3/7–9 were combined into it.) The task order is still a dependency order, not a preference — a later task will not compile without its predecessors.
- **This software targets Linux and is never validated on a macOS or Windows host.** CI runs on Linux and host runs diverge (rustfmt nightly availability, workspace feature unification, target-specific `cfg` gates), so run `cargo test` / `clippy` / `fmt` in whatever Linux environment you build in.
- **Two sibling checkouts are referenced below** as `<edge-feed-spec>` (`malbeclabs/edge-feed-spec` — the wire spec) and `<edge-multicast-ref>` (`malbeclabs/edge-multicast-ref` — the reference Go decoders this plan validates against). Substitute wherever they sit locally.
- **PROTOCOL.md is the contract.** Any wire change must keep the forward-compat rule (consumers ignore unknown types and fields) and be reflected there in the same commit.
- **Never hard-wrap markdown.** One paragraph is one line.
- **PR bodies: ~350 words, hard budget.** What broke and why, the non-obvious traps, what was verified, what was not. No diff restatement, no per-file walkthrough.
- **Comments: one line is the default.** A comment must never be longer than the code it describes. No comment that restates the diff or recaps the investigation.
- **Every commit compiles and every test passes** before moving to the next task. `cargo clippy --all-targets` must be clean.

---

## Reconciliation with prior work — read before starting

The `2026-08-05-edge-connect-multi-publisher-ports.md` plan is **fully executed** (shipped in `c055c5e`, PR #89). Do not re-plan any of it. What that means concretely, because the design doc predates it and gets three things wrong:

| Design doc says | Actual state on `main` (`01cb86c`) |
|---|---|
| "our `FEEDS` rows bind `9201/9202` and `10201/10202/10203`" (§7) | Six publishers bound per HL protocol: TOB `9001/9101/9201/9301/9401/9601`, MBO `10001…10601`. `src/ingest/feeds.rs:151-252` |
| "We consume exactly **one** HL publisher" (§7) | Six. The `RefDataState` conclusion still holds, for a different reason: one receiver task owns one processor and binds one port block, so each `RefDataState` still sees exactly one source IP. It becomes reachable the moment two publishers **share** a port block — which is exactly what the two Lashay arms do. |
| `processor.rs:480`, `:107`, `:113` | Stale. Current: `FrameCtx.publisher` `receiver.rs:109`; `TobProcessor.seq` `processor.rs:142`; `RefDataState` instances at `processor.rs:136`/`:391`/`:519`; `Publisher` `arbiter.rs:87`; `StalenessFloor` `arbiter.rs:285`; `WindowedDedup` `arbiter.rs:397`; `SubFilter` `sinks/ws.rs:108`; `DEPTH_LEVELS` `processor.rs:36`. |

Also inherited from that work and load-bearing here: `FeedPublisher` has **no `name` field** — a publisher's identity is its base port (`FeedPublisher::base_port() -> u16`), and the CLI flag is `--publisher-port`, not `--publisher`.

**Scope of this plan:** the design's §7 PRs **1–6**. PR 7 (migrating Hyperliquid MBO to the incremental output as a true `L3_MBO` book, and deleting `depth`/`DEPTH_LEVELS`) is explicitly gated on its own design doc (§2.2) and is **not** in this plan. Consequence: `depth` and `book` coexist on the wire when this plan finishes — MBP emits `book`, MBO keeps emitting `depth`.

Also out of scope, tracked separately: binding the seventh Hyperliquid publisher (`9501/9502`, `10501/10502/10503` — index 5, absent from `FEEDS` and pinned as `6` by `feeds.rs:413` and `main.rs:552`); `MidpointProcessor`'s single un-keyed `SeqTracker` (`processor.rs:392`); adding `channel_id` to the MBO book key (degenerate today — HL is all channel 0); **venue-compatible output sinks** (design §7 PR 8, added 2026-08-07) — the sink for Lashay depends on this plan's Task 12 and nothing else, so it becomes reachable the moment this plan lands, while the Hyperliquid `l2Book`/`l4Book` sinks wait on PR 7.

### The Hyperliquid shared-port migration

The HL fleet is moving to publish every host on **one** port block per protocol, distinguished only by source IP — the same deployment model the two Lashay arms already use, and normative per edge-feed-spec PR #25: the unique identifier is **`(source_ip, port)`**, never the port alone. That migration is gated on receiver-side de-dupe, which is this plan.

**No task in this plan changes.** But it changes what two of them mean, and it retires an assumption #89 introduced only last week.

**Task 2 stops being latent.** Today each HL publisher has its own port block, so each receiver task owns one processor that sees exactly one source IP — which is why the shared `RefDataState` has never thrashed. Under one shared block, one task sees six source IPs with six unrelated `reset_count` series, and any one host's restart clears all six publishers' definitions. Every emission path gates on a resolved definition, so the whole venue goes dark until the next reference-data burst. Task 2 is that fix, and after the migration it is a live bug fix for Hyperliquid rather than a prerequisite for Lashay.

**`FeedPublisher::base_port()` stops being a publisher identity.** This is the real consequence, and worth stating plainly because #89 deliberately chose the base port as the operator-facing identity ("the port block is the publisher property this protocol actually defines"). Under a shared block that property no longer distinguishes publishers: one `FeedPublisher` row *is* six publishers. Three things degrade, none of them correctness:

- the `publisher` metric label collapses six hosts into one series, so `dz_receiver_up` and the per-publisher receiver metrics become per-**task** and a single wedged host is invisible;
- `ReceiverKey = (venue, kind, u16)` collapses likewise, so `FeedHealth` loses the ability to report one publisher down while its peers stream — the venue aggregate stays correct, but the per-publisher signal under it is gone;
- `--publisher-port` no longer narrows anything, since there is one port.

Correctness is unaffected: every piece of *state* already keys on the source IP — `TobProcessor`'s `SeqTracker`, the MBO books, the arbiter's `Publisher::Edge(IpAddr)`, and after Task 2 reference data too. The gap is attribution only.

**Task 4's `arm_ordinal` is the mechanism that closes it**, and that is not a coincidence: it exists precisely because the source IP is unauthenticated and spoofable and so must never be a raw label value. It hands out a stable, bounded, per-venue ordinal (`arm0`, `arm1`, …, `other` past the cap) and logs the ordinal-to-IP mapping once on first sight. Re-labelling the receiver and health metrics by arm ordinal instead of base port is the natural follow-on, and it also unblocks the item the 2026-08-05 plan explicitly deferred — per-publisher win-rate attribution, which was blocked on `Publisher::label()` collapsing every edge source to `"edge"`. **Do not build it in this plan.** Note the connection in Task 4's PR body so it is on record.

**One cap flips meaning.** `MAX_BOOKS` (4096) and `MAX_PRICE_BOOKS` live in the processor, and each receiver task owns one processor. Today six tasks give a loose process-wide bound of 6 × 4096; under one shared block a single processor holds all six publishers' `(publisher, instrument)` pairs against one 4096 cap. HL's real instrument count leaves ample headroom (~200 × 6 ≈ 1,200), so this is a documentation point rather than a leak — but it is a *tightening*, and `docs/input-sources.md` currently states the loose reading.

---

## Three decisions this plan makes that the design left open

State these in the PR bodies; they are judgment calls, not transcriptions.

**1. An incremental `book` stream is arbitrated by authority only, never by a content floor — in both modes.** The design's §2.3 mode table implies `Coordinated` could race an incremental stream per tick. It cannot. A "tick" can hold several deltas, so a per-tick latch interleaves two arms' deltas on the wire, and the spec is explicit that two hosts' delta series are unrelated. `Coordinated` therefore governs **quotes**; `book` always routes through the `Sticky` authority gate regardless of the venue's declared mode. This is why Task 4 is a hard prerequisite for Task 12. *Confirmed 2026-08-07: `Coordinated` becomes the relevant mode for this venue once the FIX side carries venue timestamps, at which point the row flips as config.*

**2. Every `FEEDS` row that carries a tape emits it, and exactly one *runs* per venue at a time — the reconciler decides which.** *Revised 2026-08-07.* The earlier form of this decision (TOB owns the tape statically, MBP emits nothing) was wrong: the two Lashay groups are **separately subscription-gated**, so a host subscribed to the market-by-price group alone activates the MBP row and nothing else. Its WebSocket output must still carry the tape — a consumer that never receives the top-of-book feed on the wire still needs one.

Both rows therefore set `emit_trades: true`, and ownership becomes a **runtime** decision the reconciler publishes: when a venue's top-of-book row is active it owns the tape, otherwise the market-by-price row does. Task 13 builds that. The invariant it preserves is the one that licenses Task 1's `trade_id == 0` bypass: **at most one tape emitter per venue at any moment.** Without it, two active rows would duplicate every FIX-sourced print, because a bypassed `0` has no window to collapse against.

**3. PROTOCOL.md stays v1 through this plan; `book` lands as an additive type.** The design assigns "PROTOCOL.md v2" to PR 5, but the only non-additive change is *deleting* `depth`, which happens in PR 7. Adding `book` is covered by the forward-compat rule. Task 12 documents `book` and marks `depth` deprecated-and-removed-in-v2; the version flips when the deletion lands. *Confirmed 2026-08-07.*

---

## Blocking open question — must be answered before Task 14

`Feed.code` is the DoubleZero multicast group code the reconciler matches against `doublezero status --json`. **Neither** of the two live Lashay group codes follows the venue-neutral naming every code in `FEEDS` uses today (`tiredsolid`, `scottsdale`), so neither can go in as written. The design sidesteps it ("Both group codes come from the deployment config; this doc does not restate them") but `Feed.code` is a `&'static str` compiled into the binary — there is no deployment config path today.

Three ways out, in order of preference:

1. **Rename the multicast groups upstream** to venue-neutral codes (every other code already is — `tiredsolid`, `scottsdale`). Cleanest; costs a deployment change and coordination.
2. **Make `code` overridable at runtime** — a `--feed-code <venue>:<kind>=<code>` / `DZ_FEED_CODES` flag the reconciler consults before falling back to the compiled value, with the Lashay rows shipping neutral placeholders. Keeps the literals out of the binary and out of git. Note it must key on `(venue, kind)`, not venue alone: the two Lashay rows have **different** codes.
3. Ship the rows with neutral placeholders and accept that subscription gating never activates them until (1) or (2) lands.

**A second unknown, same shape:** the top-of-book row's **multicast group address**. Only the market-by-price group (`233.84.178.4`) is recorded anywhere. The top-of-book group is not, and the design deliberately does not name it. It must come from the same place the codes do.

Task 14 assumes **(1)** and takes both neutral codes and the group address as parameters. If the answer is (2), Task 14 gains a step; the rest of the plan is unaffected.

---

## File Structure

**New files**

| File | Responsibility |
|---|---|
| `src/ingest/codec_mbp.rs` | Pure decoder for frame magic `0x4442`. Frame walk via `codec_common::decode_frame_with`, one body decoder per message type, permissive enums. No state. |
| `src/ingest/pricebook.rs` | `PriceBook`: two `BTreeMap<i64, LevelState>` plus the snapshot+delta recovery machine. Codec-agnostic (raw ints, no wire structs) so it unit-tests in isolation. Sibling of `book.rs`, not a reuse of it. |
| `src/ingest/authority.rs` | `StickyAuthority`: per-market single-arm election, health-driven and margin-driven transfer, arm-ordinal labelling. The `Sticky` half of the arbitration module. |
| `tests/codec_mbp_fixtures.rs` | Real-frame decode tests over committed `tests/fixtures/mbp_*.bin`, plus cross-codec equality against the byte-validated TOB layouts. |

**Modified files**

| File | Change |
|---|---|
| `src/ingest/arbiter.rs` | `trade_id == 0` bypass; `ArbitrationMode` dispatch; `book` authority arm; arm metrics. |
| `src/ingest/subscriber.rs` | Unchanged API — `RefDataState` stays as-is; the per-publisher map lives in the processors. |
| `src/ingest/processor.rs` | Per-publisher `RefDataState` maps in all three processors; new `MbpProcessor`. |
| `src/ingest/feeds.rs` | `FeedKind::MarketByPrice`; `ArbitrationMode` on `Feed`; Lashay rows; venue→code match extension; new invariant tests. |
| `src/ingest/receiver.rs` | `run_feed` arm for `MarketByPrice`. |
| `src/sinks/ws.rs` | `channel` + `type` filter dimensions, both match paths; `book` in `prepare`; scoped replay on subscribe. |
| `src/model.rs` | `NormalizedBook`, `BookChange`, `BookAction`, `BookSide`; `FeedMessage::Book`; identity fields on `NormalizedInstrument`. |
| `src/metrics.rs` | `dz_trades_no_id_total`, `dz_arm_*` family. |
| `src/main.rs` | Arbitration CLI flags; wire venue modes into the arbiter. |
| `PROTOCOL.md` | `book` message; `channel`/`type` filters; `depth` deprecation note. |
| `docs/metrics.md`, `docs/input-sources.md`, `CHANGELOG.md` | Per-task documentation. |

---

## Task 1: `trade_id == 0` bypasses the trade dedup window

**Why first:** this is a silent data-loss bug that lands the moment the first FIX-sourced `FEEDS` row is bound (Task 13's TOB row), independent of anything MBP-specific. `WindowedDedup` keys on `trade_id` and returns `Dropped` on a same-publisher repeat; a FIX-sourced print has no venue trade id and arrives as `0`, so the second print collapses — and `0` never ages out, because eviction is by insertion order and `0` is inserted once. The tape becomes **one trade per `(venue, symbol)`, permanently.**

**Files:**
- Modify: `src/ingest/arbiter.rs` (the `FeedMessage::Trade` arm, ~`:752-771`; `VenueMetrics`, ~`:506-576`)
- Modify: `src/metrics.rs` (add `trades_no_id`)
- Modify: `src/ingest/feeds.rs` (add the one-trade-emitter-per-venue invariant test)
- Modify: `docs/metrics.md`, `CHANGELOG.md`
- Test: `src/ingest/arbiter.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Metrics::trades_no_id: IntCounterVec` (`dz_trades_no_id_total{venue}`); the `FEEDS` invariant "at most one `emit_trades: true` row per venue" — true for the tree this ships into, and generalized to **at most one tape emitter per venue at any moment** by Task 13, which is the form the bypass actually depends on.

- [x] **Step 1: Branch**

Branch off `main`.

- [x] **Step 2: Write the failing test**

Add to the `mod tests` block at the bottom of `src/ingest/arbiter.rs`. It drives the real `Arbiter::emit`, not `WindowedDedup` directly, because the bypass lives in the `emit` arm.

```rust
    fn trade(trade_id: u64) -> NormalizedTrade {
        NormalizedTrade {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            price: 0.62,
            size: 100.0,
            aggressor_side: Side::Buy,
            trade_id,
            cumulative_volume: 0.0,
            source_ts_ns: 1_000,
            recv_ts_ns: 2_000,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    /// A FIX-sourced publisher has no venue trade id and stamps every print `trade_id == 0`.
    /// Keying the window on `0` collapses the tape to a single print forever (`0` is inserted
    /// once and never evicted), so `0` must mean "no identity" and bypass the window entirely.
    #[test]
    fn zero_trade_id_bypasses_the_window() {
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let p = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        for _ in 0..5 {
            a.emit(FeedMessage::Trade(trade(0)), p);
        }
        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 5, "every zero-id print must be emitted");
    }

    /// The bypass must not weaken dedup for prints that DO carry an id.
    #[test]
    fn nonzero_trade_id_still_dedupes() {
        let (tx, mut rx) = broadcast::channel(64);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
        let p = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        for _ in 0..5 {
            a.emit(FeedMessage::Trade(trade(77)), p);
        }
        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, 1, "a repeated id is still a duplicate");
    }
```

`NormalizedTrade` and `Side` need importing in the test module — the existing `use crate::model::{NormalizedQuote, Side};` at `arbiter.rs:1011` becomes `use crate::model::{NormalizedQuote, NormalizedTrade, Side};`.

- [x] **Step 3: Run the test to verify it fails**

```bash
cargo test --lib zero_trade_id_bypasses_the_window
```

Expected: FAIL — `assertion \`left == right\` failed: every zero-id print must be emitted; left: 1, right: 5`.

- [x] **Step 4: Add the metric**

In `src/metrics.rs`, add the field next to `trades_dropped` (`:99`):

```rust
    pub trades_no_id: IntCounterVec,
```

and the registration next to `quotes_no_source_ts` (`:435-440`):

```rust
            trades_no_id: counter_vec(
                &registry,
                "dz_trades_no_id_total",
                "Trades forwarded with the trade_id==0 sentinel (dedup window bypassed)",
                &["venue"],
            ),
```

- [x] **Step 5: Pre-resolve the metric child**

In `src/ingest/arbiter.rs`, add to `struct VenueMetrics` next to `trades_dropped` (`:516`):

```rust
    trades_no_id: IntCounter,
```

and to `VenueMetrics::new` next to `trades_dropped` (`:565`):

```rust
            trades_no_id: m.trades_no_id.with_label_values(&[venue]),
```

- [x] **Step 6: Implement the bypass**

Replace the head of the `FeedMessage::Trade(t)` arm (`arbiter.rs:752-754`) so the sentinel short-circuits before the window:

```rust
            FeedMessage::Trade(t) => {
                // `trade_id == 0` is the "no venue trade id" sentinel (a FIX-sourced print has
                // none). Keying the window on it would drop every later print for the key: `0` is
                // inserted once, never ages out (eviction is by insertion order), and every
                // subsequent `0` reads as a same-publisher duplicate. Forward unkeyed instead.
                // Safe because an incremental venue publishes one authoritative arm (see
                // `authority`), and at most one FEEDS row per venue emits trades, so a bypassed
                // `0` has no second copy to leak.
                if t.trade_id == 0 {
                    let vm = self.vm(&t.venue);
                    vm.trades_no_id.inc();
                    vm.emit[EMIT_TRADE].inc();
                    vm.trades_admitted[pub_idx(publisher)].inc();
                    let _ = self.tx.send(Arc::new(msg));
                    return;
                }
                let key = (t.venue.clone(), t.symbol.clone());
                let decision = self.trades.admit(key, t.trade_id, publisher, t.recv_ts_ns);
```

The rest of the arm is unchanged.

- [x] **Step 7: Run the tests to verify they pass**

```bash
cargo test --lib trade
```

Expected: PASS, including the pre-existing `trade_new_admitted_repeat_dropped` and `trade_keys_independent_and_window_evicts`.

- [x] **Step 8: Pin the invariant the bypass depends on**

Add to `mod tests` in `src/ingest/feeds.rs`:

```rust
    /// At most one row per venue may emit trades. Two would double-publish every print, and with
    /// the `trade_id == 0` bypass in `arbiter::emit` there is no window to collapse the duplicate
    /// for a FIX-sourced publisher — which carries no venue trade id at all.
    ///
    /// NOTE: this static form holds until a venue carries a tape on two separately-gated feeds.
    /// Task 13 replaces it with the runtime-ownership assertion, which is the same invariant —
    /// **at most one tape emitter per venue at any moment** — enforced where it actually lives.
    #[test]
    fn at_most_one_trade_emitting_row_per_venue() {
        let mut emitters = std::collections::HashMap::new();
        for f in FEEDS.iter().filter(|f| f.emit_trades) {
            let prev = emitters.insert(f.venue, f.kind);
            assert!(
                prev.is_none(),
                "{} emits trades on both {:?} and {:?}",
                f.venue,
                prev.unwrap(),
                f.kind
            );
        }
    }
```

- [x] **Step 9: Run the full suite and clippy**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass.

- [x] **Step 10: Document**

Add to `docs/metrics.md` alongside the other arbiter counters:

```markdown
| `dz_trades_no_id_total{venue}` | counter | Trades forwarded with the `trade_id == 0` sentinel, bypassing the dedup window. A FIX-sourced publisher carries no venue trade id; keying the window on `0` would collapse the tape to one print per `(venue, symbol)` forever. A non-zero rate here means the venue's tape is un-deduped by construction — correct, but it relies on exactly one authoritative arm and one trade-emitting `FEEDS` row per venue. |
```

Add to `CHANGELOG.md` under Unreleased → Fixed:

```markdown
- Trades stamped `trade_id == 0` (the "no venue trade id" sentinel, emitted by FIX-sourced publishers) now bypass the cross-source dedup window instead of being keyed on it. Previously the second and every later such print was discarded as a same-publisher duplicate and `0` never aged out of the window, collapsing the tape to a single print per `(venue, symbol)` for the process's lifetime.
```

- [x] **Step 11: Commit**

```bash
git add -A
git commit -m "fix(arbiter): treat trade_id 0 as no identity, not a dedup key"
```

---

## Task 2: `RefDataState` becomes per-publisher

**Why:** `RefDataState::on_frame` clears **every** definition on any `reset_count` change (`subscriber.rs:53-61`). That is per-port state. Under the Lashay deployment two arms share one port block and differ only by source IP, so one arm's routine restart wipes the other arm's instrument set — and every processor gates emission on `definition(id)`, so both arms go dark until the next refdata burst. `TobProcessor` already keys its `SeqTracker` per source IP (`processor.rs:142`); this closes the same gap for reference data.

Not currently reachable — one receiver task binds one port block, so each `RefDataState` sees one source IP today. It is a hard prerequisite, not an incident.

**Files:**
- Modify: `src/ingest/processor.rs` (`TobProcessor` `:136`, `MidpointProcessor` `:391`, `MboProcessor` `:519`, and every `self.state.` call site)
- Test: `src/ingest/processor.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: the private helper pattern `fn state_for(&mut self, publisher: IpAddr) -> &mut RefDataState<D>` on each processor, plus `MAX_PUBLISHERS`-bounded eviction. `MbpProcessor` (Task 10) copies this shape.

- [x] **Step 1: Branch**

Branch off Task 1's final commit.

- [x] **Step 2: Write the failing test**

Add to `mod tests` in `src/ingest/processor.rs`. It drives `TobProcessor` with two source IPs and bumps only one's `reset_count`.

```rust
    /// Two publishers share one port block and differ only by source IP. `reset_count` is
    /// per-publisher state, so one arm's restart must not clear the other arm's instrument set —
    /// which would blank both arms until the next refdata burst, since every emission path gates
    /// on `definition(id)`.
    #[test]
    fn refdata_reset_is_scoped_to_the_publisher_that_reset() {
        let a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let mut p = TobProcessor::new(true);

        // Both arms publish the same manifest + definition at reset_count 0.
        for ip in [a, b] {
            p.state_for(ip).on_frame(0);
            p.state_for(ip).on_manifest(true, 1, 1);
            p.state_for(ip).on_instrument_definition(InstrumentDefinition {
                instrument_id: 7,
                symbol: "KXBTCPERP".into(),
                price_exponent: -4,
                qty_exponent: 0,
                manifest_seq: 1,
            });
        }
        assert!(p.state_for(a).definition(7).is_some());
        assert!(p.state_for(b).definition(7).is_some());

        // Arm A restarts: reset_count bumps on A's frames only.
        p.state_for(a).on_frame(1);

        assert!(p.state_for(a).definition(7).is_none(), "A's own state clears");
        assert!(
            p.state_for(b).definition(7).is_some(),
            "B's state must survive A's restart"
        );
    }
```

`state_for` must be visible to the test module — declare it `pub(crate)` or leave it private (the test module is a child of `processor`, so private is fine). `InstrumentDefinition` is `crate::ingest::codec::InstrumentDefinition`; check the existing test-module imports and extend them.

- [x] **Step 3: Run the test to verify it fails**

```bash
cargo test --lib refdata_reset_is_scoped
```

Expected: FAIL to compile — `no method named 'state_for' found for struct 'TobProcessor'`.

- [x] **Step 4: Add the shared per-publisher map helper**

Add near `MAX_PUBLISHERS` (`processor.rs:106`) — one generic helper all three processors use, so the eviction rule is written once:

```rust
/// Per-publisher reference-data state, bounded like the per-publisher sequence map.
///
/// `reset_count` is scoped to `(source_ip, group, port)`: two publishers sharing a port block have
/// unrelated reset counters, so a single shared `RefDataState` lets either arm's restart clear the
/// other's instrument set — blanking both, since every emission path gates on `definition(id)`.
/// The source IP is spoofable, so the map is bounded exactly as [`MAX_PUBLISHERS`] bounds the
/// sequence map: least-recently-inserted eviction, and an evicted publisher simply re-learns its
/// definitions from the next refdata burst.
#[derive(Default)]
struct PerPublisher<D> {
    states: HashMap<IpAddr, RefDataState<D>>,
    order: VecDeque<IpAddr>,
}

impl<D: crate::ingest::subscriber::InstrumentDef> PerPublisher<D> {
    fn get(&mut self, publisher: IpAddr) -> &mut RefDataState<D> {
        if !self.states.contains_key(&publisher) {
            while self.states.len() >= MAX_PUBLISHERS {
                match self.order.pop_front() {
                    Some(old) => {
                        self.states.remove(&old);
                    }
                    None => break,
                }
            }
            self.states.insert(publisher, RefDataState::new());
            self.order.push_back(publisher);
        }
        self.states.get_mut(&publisher).expect("just inserted")
    }
}
```

- [x] **Step 5: Convert `TobProcessor`**

Change the field at `processor.rs:136`:

```rust
    state: PerPublisher<InstrumentDefinition>,
```

Update `TobProcessor::new` to `state: PerPublisher::default()`. Add the accessor:

```rust
    fn state_for(&mut self, publisher: IpAddr) -> &mut RefDataState<InstrumentDefinition> {
        self.state.get(publisher)
    }
```

Then rewrite every `self.state.<method>` call in `TobProcessor::on_datagram` to `self.state_for(ctx.publisher).<method>`. Two call sites need care because they hold a borrow across an emit:

- The `definition(id)` lookups that feed a quote or trade: bind the needed fields out of the definition (`symbol.clone()`, `price_exponent`, `qty_exponent`) into locals **before** calling `ctx.emit`, so the `&mut self` borrow from `state_for` ends first. Pattern:
  ```rust
  let Some((symbol, px_exp, qty_exp)) = self
      .state_for(ctx.publisher)
      .definition(q.instrument_id)
      .map(|d| (d.symbol.clone(), d.price_exponent, d.qty_exponent))
  else {
      continue;
  };
  ```
- `on_instrument_definition(d)` consumes `d`, so build the `NormalizedInstrument` from `d` first, then hand `d` to the state, then emit.

- [x] **Step 6: Convert `MidpointProcessor` and `MboProcessor` the same way**

`MidpointProcessor.state` (`:391`) and `MboProcessor.state` (`:519`) both become `PerPublisher<...>`, each gaining a `state_for(publisher)` accessor and the same call-site rewrite. `MboProcessor` has more sites — `book_for`'s gate 1 (`self.state.definition(instrument_id)?`), the `book_for` eviction's `self.state.definition(old_id)`, `emit_depth`'s definition lookup, and the `ManifestSummary` / `InstrumentDefinition` / `Trade` / `OrderExecute` handlers.

`book_for` needs the borrow split explicitly, because it takes `&mut self` and then touches `self.books`:

```rust
    fn book_for(&mut self, instrument_id: u32, ctx: &FrameCtx) -> Option<&mut BookState> {
        // Gate 1: no definition -> no book (borrow ends here, before `books` is touched).
        self.state_for(ctx.publisher).definition(instrument_id)?;
        let key = (ctx.publisher, instrument_id);
        // ... unchanged from here
```

Do **not** change `MidpointProcessor`'s un-keyed `SeqTracker` (`:392`) — out of scope, and the 2026-08-05 plan deliberately left it.

- [x] **Step 7: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass, including the existing `tests/codec_mbo_fixtures.rs` and `tests/e2e.rs`.

- [x] **Step 8: Document and commit**

`CHANGELOG.md` under Unreleased → Fixed:

```markdown
- Reference-data state is now tracked per publisher (source IP) rather than once per receiver, matching how sequence state is already keyed. `reset_count` is scoped to `(source_ip, group, port)`, so under a shared port block one publisher's restart previously cleared every publisher's instrument definitions — blanking the whole feed until the next reference-data burst, since all emission gates on a known definition.
```

Add a line to the `ingest/subscriber.rs` module doc noting that the per-publisher map lives in the processors, so the state machine itself stays single-publisher and unit-testable.

```bash
git add -A
git commit -m "fix(ingest): scope reference-data state per publisher, not per receiver"
```

---

## Task 3: Arbitration mode plumbing — `Coordinated` is today's floor

**Why:** a pure refactor that introduces the seam without changing behavior. `ArbitrationMode` lands on `Feed`, reaches the `Arbiter` as a per-venue map, and dispatches to the existing `StalenessFloor` for `Coordinated`. Every existing test passing unchanged is the acceptance criterion.

**Files:**
- Modify: `src/ingest/feeds.rs` (`ArbitrationMode`, `Feed.arbitration`, all rows, invariant tests)
- Modify: `src/ingest/arbiter.rs` (`modes` map, `set_mode`, `mode_for`)
- Modify: `src/main.rs` (populate the map from the selected feeds)

**Interfaces:**
- Consumes: nothing.
- Produces: `pub enum ArbitrationMode { Coordinated, Sticky }` in `ingest::feeds` (`Copy + PartialEq + Eq + Debug`); `Feed.arbitration: ArbitrationMode`; `Arbiter::set_mode(&mut self, venue: &'static str, mode: ArbitrationMode)`; `Arbiter::mode_for(&self, venue: &str) -> ArbitrationMode` defaulting to `Coordinated`.

- [x] **Step 1: Branch**

Branch off Task 2's final commit.

- [x] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/feeds.rs`:

```rust
    /// A venue's arms are the same hosts whatever protocol they speak, so every row for a venue
    /// must declare the same arbitration mode. Disagreement would make the arbiter's per-venue mode
    /// depend on which row registered last.
    #[test]
    fn arbitration_mode_agrees_across_a_venues_rows() {
        let mut modes = std::collections::HashMap::new();
        for f in FEEDS {
            if let Some(prev) = modes.insert(f.venue, f.arbitration) {
                assert_eq!(prev, f.arbitration, "{} declares two arbitration modes", f.venue);
            }
        }
    }

    /// The existing venues race on a comparable venue clock and must keep doing so — this task is a
    /// seam, not a behavior change.
    #[test]
    fn existing_venues_are_coordinated() {
        for f in FEEDS.iter().filter(|f| f.venue != "Lashay") {
            assert_eq!(f.arbitration, ArbitrationMode::Coordinated, "{}", f.venue);
        }
    }
```

- [x] **Step 3: Run the tests to verify they fail**

```bash
cargo test --lib arbitration_mode_agrees
```

Expected: FAIL to compile — `no field 'arbitration' on type '&Feed'`.

- [x] **Step 4: Add the enum and the field**

In `src/ingest/feeds.rs`, above `struct Feed`:

```rust
/// How the bridge resolves two publishers mirroring one venue.
///
/// Both modes hold exactly one authoritative publisher per key; what differs is when authority
/// transfers. `Coordinated` re-latches every tick, because the publishers stamp a venue clock that
/// is comparable between them. `Sticky` cannot: its arms carry no shared coordinate — no stable
/// entry id, no per-entry venue timestamp, and the transport's own send time is not the venue's —
/// and a content hash is no substitute, since a level oscillating 100 -> 0 -> 100 emits
/// byte-identical updates and collapsing those leaves a subscriber holding 0 at a price that has
/// liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitrationMode {
    /// Comparable venue clock: latch to the tick's leader, re-latch every tick.
    Coordinated,
    /// No comparable coordinate: elect one arm and hold it, transferring only on a health verdict,
    /// on silence, or on a sustained speed margin. See [`crate::ingest::authority`].
    Sticky,
}
```

Add `pub arbitration: ArbitrationMode,` to `struct Feed` with a one-line doc, and `arbitration: ArbitrationMode::Coordinated,` to all three existing `FEEDS` rows.

- [x] **Step 5: Carry the mode into the arbiter**

In `src/ingest/arbiter.rs`, add to `struct Arbiter`:

```rust
    /// Per-venue arbitration mode, populated at startup from the selected `FEEDS` rows. A venue
    /// absent from the map arbitrates as `Coordinated` — what every venue did before modes existed,
    /// so an unregistered venue can never silently change semantics.
    modes: HashMap<&'static str, ArbitrationMode>,
```

Initialize it to `HashMap::new()` in `Arbiter::new`, and add:

```rust
    /// Declare a venue's arbitration mode. Called once per selected feed at startup; a venue's rows
    /// are pinned to one mode by `feeds::tests::arbitration_mode_agrees_across_a_venues_rows`.
    pub fn set_mode(&mut self, venue: &'static str, mode: ArbitrationMode) {
        self.modes.insert(venue, mode);
    }

    #[allow(dead_code)] // consumed by the `Sticky` dispatch in the authority task
    fn mode_for(&self, venue: &str) -> ArbitrationMode {
        self.modes
            .get(venue)
            .copied()
            .unwrap_or(ArbitrationMode::Coordinated)
    }
```

- [x] **Step 6: Populate the map in `main.rs`**

Immediately after `Arbiter::new(...)` and before the arbiter is wrapped in the `SharedArbiter`, register every selected feed's mode, using whatever local binding holds the selected feed list at that point (the value handed to `ReconcilerConfig.enabled`):

```rust
    for f in &feeds {
        arbiter.set_mode(f.venue, f.arbitration);
    }
```

- [x] **Step 7: Run everything, then commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "refactor(feeds): declare an arbitration mode per feed"
```

Expected: all pass with **no** behavior change.

---

## Task 4: `StickyAuthority` — one authoritative arm per market

> ### ⚠️ AMENDED 2026-08-07, after the task shipped — authority is scoped **per arm**, not per arm-market
>
> Built as specified below (`5b2d045`, `4229f4e`, `3286b1c`), then restructured. **Read this block instead of the scoping in the section below; the rest of the section still stands.** Task 12 inherits the amended shape — see *Task 12 contract changes* at the end of this block.
>
> **What changed and why.** The section below elects a leader independently per `(venue, channel_id, instrument_id)`. That is the wrong grain for two of the three transfer triggers, because **latency is a property of an arm, not of a market**: every message from a source IP is evidence about that arm's speed, so splitting the evidence per market splits it as finely as it can possibly be split. The three triggers have genuinely different natural scopes:
>
> | Trigger | Scope | Why |
> |---|---|---|
> | **Speed** (Task 5) | **per arm**, venue-wide | Pooled evidence. One verdict per venue per window, applied to every market the winning arm does not already hold. |
> | **Silence** | **per arm**, venue-wide | An arm is live or it is not. Per-market silence is a *bug* — see below. |
> | **Health** | **per market** | An arm can be `Synced` on 1,200 markets and `Gap` on one. Venue-wide health would either transfer the whole venue over one bad book, or serve a knowingly-stale book — the exact failure the rule exists to prevent. |
>
> **The bug per-market silence causes, which is why it had to move.** `leader_arrival_ns` advances only when the leader itself sends, so on a market quieter than `leader_timeout` the challenger's next message always reads as leader silence and takes authority — then the original arm's next message takes it back. On the sports capture **93 of 1,239 instruments saw any level update at all in 39 s**, so nearly every market is quiet for far longer than the 2 s default and nearly every update on those markets would register as a transfer, each one re-baselining the consumer's book under Task 12. Venue-wide silence measures what silence means: the arm has sent nothing *for the venue* within the timeout.
>
> **The amended model.** One leader per venue, elected by speed and silence, with a **per-market override**: when the venue leader's book for a market is unhealthy and another arm's is not, that market alone is served by the healthy arm. The override is a pure function of health and the venue leader — no stored per-market authority — so it reverts automatically when the leader's book recovers.
>
> **Two properties this buys.** The wire-keyed `(channel_id, instrument_id)` maps that both reviewers flagged as unbounded stop being growable by a forged stream: the sampling and liveness state is now keyed per `(venue, arm)`, and **only arms holding a metric ordinal are eligible at all** — past `MAX_LABELLED_ARMS` a publisher is neither recorded nor ever authoritative, so the cap that existed to bound the label set now bounds admission too. What remains per market is health plus the last-admitted arm, written only for markets that already have a book, so it is bounded transitively by `MAX_PRICE_BOOKS`.
>
> **Interface delta** (the section's `Interfaces` block is otherwise unchanged):
>
> ```rust
> // MarketKey survives: still the caller's key, the health key, and what `leader_of` answers for.
> pub fn venue_leader(&self, venue: &str) -> Option<Publisher>;             // new
> pub fn transfer_venue_to(&mut self, venue: &str, to: Publisher, at_ns: u64) -> bool;  // replaces transfer_to
> pub fn observe_matched_lead(&mut self, venue: &str, arm: Publisher, lead_ns: i64);    // replaces observe_challenger
> pub fn close_window(&mut self, now_ns: u64) -> Vec<(Arc<str>, Publisher)>;            // was Vec<(MarketKey, Publisher)>
> ```
>
> `Admit::Contest`'s `lead_ns` is **not** a lead and never was — it is how late the non-authoritative copy arrived relative to the leader's previous message, which is inter-arm phase. It stays as a drop-path diagnostic and **no longer feeds `dz_arm_lead_ns`**, which is now fed only by `observe_matched_lead`. That is what makes `{winner="challenger"}` reachable.
>
> **Task 12 contract changes.** Task 12 wires the caller and must now:
> 1. Call `set_health(&market_key, arm, healthy)` on every `PriceBook` status transition — unchanged, still per market.
> 2. Feed `observe_matched_lead(venue, arm, lead_ns)` from a **cross-arm trade matcher** (Task 5 as amended), pooled per venue. Not per market, and not from `Admit::Contest`.
> 3. Drive `close_window` on a periodic tick and apply each returned `(venue, arm)` by calling `transfer_venue_to`.
> 4. Publish `dz_arm_markets_held{venue,arm}` from `markets_held`, which is O(markets) — call it on the metrics tick, never per message.
> 5. Gate `admit` on an instrument that already resolves to a definition and a book, which is what keeps the per-market maps bounded. This is a **precondition Task 12 owns**, not something `authority` enforces.

**Why:** the Lashay arms are one FIX-sourced and one WS-sourced publisher with no comparable coordinate. Authority is per `(venue, channel_id, instrument_id)` — **per instrument, never per level**, because per-level leadership interleaves the arms, and two arms' delta series are unrelated by construction, so interleaving corrupts the book while every per-arm sequence check still passes.

Two transfer triggers here; the speed margin is Task 5.

1. **Health.** A leader sitting in `gap` or `awaiting-snapshot` is unhealthy. Under full-state output a lost level self-heals on the next message; under incremental output it does not heal until the next snapshot, so a stalled leader must yield.
2. **Silence.** A leader that stops sending is unhealthy. Data-driven, no timer task: a challenger's arrival more than `leader_timeout` after the leader's last message takes authority. *(Amended: venue-wide, not per market — see the block above.)*

**Files:**
- Create: `src/ingest/authority.rs`
- Modify: `src/ingest/mod.rs` (`pub mod authority;`), `src/ingest/arbiter.rs` (`Publisher` gains `Hash`)
- Modify: `src/metrics.rs` (the `dz_arm_*` family)

**Interfaces:**
- Consumes: `Admit<P>`, `Publisher` from `ingest::arbiter`.
- Produces, used by Tasks 5, 10 and 12:
  ```rust
  pub type MarketKey = (Arc<str>, u32, u32);            // (venue, channel_id, instrument_id)
  pub struct StickyAuthority { /* private */ }
  impl StickyAuthority {
      pub fn new(leader_timeout_ns: u64) -> Self;       // replaced by AuthorityConfig in Task 5
      pub fn admit(&mut self, key: MarketKey, publisher: Publisher, arrival_ns: u64) -> Admit<Publisher>;
      pub fn transfer_to(&mut self, key: &MarketKey, publisher: Publisher, arrival_ns: u64) -> bool;
      pub fn set_health(&mut self, key: &MarketKey, publisher: Publisher, healthy: bool);
      pub fn arm_ordinal(&mut self, venue: &str, publisher: Publisher) -> &'static str;
      pub fn markets_held(&self, venue: &str, publisher: Publisher) -> usize;
  }
  ```

- [x] **Step 1: Branch**

Branch off Task 3's final commit.

- [x] **Step 2: Write the failing tests**

Create `src/ingest/authority.rs` with the test module only, so every rule is named before the implementation exists. Add `pub mod authority;` to `src/ingest/mod.rs`.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn arm(n: u8) -> Publisher {
        Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
    }

    fn key() -> MarketKey {
        ("Lashay".into(), 2, 41)
    }

    const TIMEOUT: u64 = 2_000_000_000; // 2s

    /// The first arm to deliver a usable book is provisionally authoritative, so there is no dark
    /// start while the election window is open.
    #[test]
    fn first_arm_takes_authority() {
        let mut a = StickyAuthority::new(TIMEOUT);
        assert_eq!(a.admit(key(), arm(1), 1_000), Admit::Emitted { opened_tick: true });
    }

    /// `opened_tick` marks an authority TRANSFER, not every leader message — so the
    /// `*_ticks_won_total` family keeps meaning "took the key" in both modes.
    #[test]
    fn leader_keeps_emitting_without_reopening() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        assert_eq!(a.admit(key(), arm(1), 2_000), Admit::Emitted { opened_tick: false });
    }

    /// The non-authoritative arm is dropped, and the first drop after each leader message reports
    /// the head-to-head lead, so Task 5's sampler has one sample per leader message rather than one
    /// per challenger burst.
    #[test]
    fn challenger_is_dropped_and_reports_the_lead_once() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        assert_eq!(
            a.admit(key(), arm(2), 1_400),
            Admit::Contest { winner: arm(1), lead_ns: 400 }
        );
        assert_eq!(a.admit(key(), arm(2), 1_500), Admit::Dropped);
        a.admit(key(), arm(1), 2_000);
        assert_eq!(
            a.admit(key(), arm(2), 2_300),
            Admit::Contest { winner: arm(1), lead_ns: 300 }
        );
    }

    /// A leader in `gap`/`awaiting-snapshot` yields to a healthy challenger: under incremental
    /// output a lost level does not self-heal until the next snapshot, so holding authority through
    /// a gap serves a knowingly-wrong book.
    #[test]
    fn unhealthy_leader_yields_to_a_healthy_challenger() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        assert_eq!(a.admit(key(), arm(2), 1_100), Admit::Emitted { opened_tick: true });
        assert!(!a.admit(key(), arm(1), 1_200).emitted(), "authority actually moved");
    }

    /// An unhealthy challenger must not take over from an unhealthy leader — that flaps between two
    /// broken arms, re-baselining every consumer on each flip and fixing nothing.
    #[test]
    fn unhealthy_challenger_does_not_take_over() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        a.set_health(&key(), arm(1), false);
        a.set_health(&key(), arm(2), false);
        assert!(!a.admit(key(), arm(2), 1_100).emitted());
    }

    #[test]
    fn silent_leader_times_out() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        assert!(!a.admit(key(), arm(2), 1_000 + TIMEOUT).emitted(), "not yet past");
        assert_eq!(
            a.admit(key(), arm(2), 1_001 + TIMEOUT),
            Admit::Emitted { opened_tick: true }
        );
    }

    #[test]
    fn authority_is_per_market() {
        let mut a = StickyAuthority::new(TIMEOUT);
        let other: MarketKey = ("Lashay".into(), 2, 42);
        a.admit(key(), arm(1), 1_000);
        a.admit(other.clone(), arm(2), 1_000);
        assert!(!a.admit(key(), arm(2), 1_100).emitted());
        assert!(a.admit(other, arm(2), 1_100).emitted());
    }

    /// Arm ordinals are stable per venue, bounded, and never expose a spoofable source IP as a
    /// metric label.
    #[test]
    fn arm_ordinals_are_stable_and_bounded() {
        let mut a = StickyAuthority::new(TIMEOUT);
        assert_eq!(a.arm_ordinal("Lashay", arm(1)), "arm0");
        assert_eq!(a.arm_ordinal("Lashay", arm(2)), "arm1");
        assert_eq!(a.arm_ordinal("Lashay", arm(1)), "arm0", "stable");
        assert_eq!(a.arm_ordinal("Other", arm(9)), "arm0", "per venue");
        for n in 3..=8 {
            a.arm_ordinal("Lashay", arm(n));
        }
        assert_eq!(a.arm_ordinal("Lashay", arm(200)), "other", "cap holds");
    }

    #[test]
    fn markets_held_counts_per_arm() {
        let mut a = StickyAuthority::new(TIMEOUT);
        a.admit(key(), arm(1), 1_000);
        a.admit(("Lashay".into(), 2, 42), arm(1), 1_000);
        a.admit(("Lashay".into(), 2, 43), arm(2), 1_000);
        assert_eq!(a.markets_held("Lashay", arm(1)), 2);
        assert_eq!(a.markets_held("Lashay", arm(2)), 1);
    }
}
```

- [x] **Step 3: Run the tests to verify they fail**

```bash
cargo test --lib authority
```

Expected: FAIL to compile — `cannot find type 'StickyAuthority' in this scope`.

- [x] **Step 4: Add `Hash` to `Publisher`**

`arbiter.rs:86` becomes `#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]`. `IpAddr` is `Hash`, so this is free.

- [x] **Step 5: Implement `StickyAuthority`**

Prepend to `src/ingest/authority.rs`:

```rust
//! Single-arm authority for venues whose redundant publishers have no comparable clock.
//!
//! `Coordinated` arbitration (`arbiter::StalenessFloor`) buckets two copies on a venue-assigned
//! coordinate and re-latches the leader every tick. That needs a coordinate the arms share. A
//! FIX-sourced arm and a WS-sourced arm of one venue have none, and a content hash cannot
//! substitute: the cross-arm-common fields of a level update reduce to `(side, price, quantity)`,
//! which recurs constantly on a coarse bounded price grid.
//!
//! So: per market, exactly one arm is authoritative and its stream is published verbatim; the other
//! is ingested and discarded. Authority transfers on a health verdict, on silence, or on a
//! sustained speed margin — never on a single faster message, because flapping authority
//! re-baselines every consumer's book.
//!
//! **Authority is per instrument, never per level.** Interleaving two arms' deltas corrupts the
//! book while every per-arm sequence check still passes.

use std::{collections::HashMap, sync::Arc};

use crate::ingest::arbiter::{Admit, Publisher};

/// The published market key: venue plus the wire identity pair. Deliberately excludes the arm, so
/// both arms resolve to one entry and arbitrate against each other.
pub type MarketKey = (Arc<str>, u32, u32);

/// Cap on distinct arms given a stable metric ordinal per venue. The source IP is unauthenticated
/// and spoofable, so ordinals are handed out first-come and anything past the cap collapses to
/// `"other"` rather than growing the label set. Real deployments run two arms.
const MAX_LABELLED_ARMS: usize = 8;

const ARM_LABELS: [&str; MAX_LABELLED_ARMS] =
    ["arm0", "arm1", "arm2", "arm3", "arm4", "arm5", "arm6", "arm7"];

/// Per-market authority state.
struct Held {
    leader: Publisher,
    /// Arrival of the leader's most recent admitted message — the baseline a challenger's arrival
    /// is measured against, and the silence clock.
    leader_arrival_ns: u64,
    /// Set once a challenger has been reported since the leader's last message, so a challenger
    /// burst yields one contest sample rather than inflating the histogram.
    contest_recorded: bool,
}

pub struct StickyAuthority {
    held: HashMap<MarketKey, Held>,
    /// Per `(market, arm)` health. Absent means healthy: an arm that has never reported is presumed
    /// usable, so a market whose processor does not track health still elects a leader.
    health: HashMap<(MarketKey, Publisher), bool>,
    ordinals: HashMap<(String, Publisher), &'static str>,
    ordinal_counts: HashMap<String, usize>,
    leader_timeout_ns: u64,
}

impl StickyAuthority {
    pub fn new(leader_timeout_ns: u64) -> Self {
        Self {
            held: HashMap::new(),
            health: HashMap::new(),
            ordinals: HashMap::new(),
            ordinal_counts: HashMap::new(),
            leader_timeout_ns,
        }
    }

    fn healthy(&self, key: &MarketKey, publisher: Publisher) -> bool {
        self.health.get(&(key.clone(), publisher)).copied().unwrap_or(true)
    }

    /// Record an arm's book health for one market. `false` means `gap`/`awaiting-snapshot`; the
    /// processor calls this on every state transition.
    pub fn set_health(&mut self, key: &MarketKey, publisher: Publisher, healthy: bool) {
        self.health.insert((key.clone(), publisher), healthy);
    }

    /// The admission decision for one message from `publisher` on `key`.
    pub fn admit(
        &mut self,
        key: MarketKey,
        publisher: Publisher,
        arrival_ns: u64,
    ) -> Admit<Publisher> {
        let challenger_healthy = self.healthy(&key, publisher);
        let leader_unhealthy = self
            .held
            .get(&key)
            .is_some_and(|h| !self.healthy(&key, h.leader));
        match self.held.get_mut(&key) {
            None => {
                // No dark start: the first arm to deliver is provisionally authoritative even
                // before it has reported health.
                self.held.insert(
                    key,
                    Held { leader: publisher, leader_arrival_ns: arrival_ns, contest_recorded: false },
                );
                Admit::Emitted { opened_tick: true }
            }
            Some(h) if h.leader == publisher => {
                h.leader_arrival_ns = arrival_ns;
                h.contest_recorded = false;
                Admit::Emitted { opened_tick: false }
            }
            Some(h) => {
                let leader = h.leader;
                let silent = arrival_ns.saturating_sub(h.leader_arrival_ns) > self.leader_timeout_ns;
                if challenger_healthy && (leader_unhealthy || silent) {
                    h.leader = publisher;
                    h.leader_arrival_ns = arrival_ns;
                    h.contest_recorded = false;
                    return Admit::Emitted { opened_tick: true };
                }
                if h.contest_recorded {
                    Admit::Dropped
                } else {
                    h.contest_recorded = true;
                    Admit::Contest {
                        winner: leader,
                        lead_ns: arrival_ns.saturating_sub(h.leader_arrival_ns),
                    }
                }
            }
        }
    }

    /// Force authority for one market, returning whether it moved. Task 5's margin path; health and
    /// silence transfers go through [`Self::admit`].
    pub fn transfer_to(&mut self, key: &MarketKey, publisher: Publisher, arrival_ns: u64) -> bool {
        match self.held.get_mut(key) {
            Some(h) if h.leader != publisher => {
                h.leader = publisher;
                h.leader_arrival_ns = arrival_ns;
                h.contest_recorded = false;
                true
            }
            _ => false,
        }
    }

    /// A stable, bounded metric label for an arm within a venue, so a spoofable source IP never
    /// becomes a label value. The ordinal-to-IP mapping is logged once, on first sight.
    pub fn arm_ordinal(&mut self, venue: &str, publisher: Publisher) -> &'static str {
        if let Some(l) = self.ordinals.get(&(venue.to_string(), publisher)) {
            return l;
        }
        let n = self.ordinal_counts.entry(venue.to_string()).or_insert(0);
        let label = ARM_LABELS.get(*n).copied().unwrap_or("other");
        if *n < MAX_LABELLED_ARMS {
            *n += 1;
        }
        self.ordinals.insert((venue.to_string(), publisher), label);
        tracing::info!(venue, arm = label, ?publisher, "arbitration arm registered");
        label
    }

    /// How many of `venue`'s markets `publisher` holds — the gauge an operator reads to see which
    /// arm is live and whether the venue is split.
    pub fn markets_held(&self, venue: &str, publisher: Publisher) -> usize {
        self.held
            .iter()
            .filter(|((v, _, _), h)| v.as_ref() == venue && h.leader == publisher)
            .count()
    }
}
```

Note the `leader_unhealthy` binding is computed **before** the `match` because `healthy()` borrows `self` immutably while the match arm holds a mutable borrow.

- [x] **Step 6: Run the tests to verify they pass**

```bash
cargo test --lib authority
```

Expected: all nine PASS.

- [x] **Step 7: Add the arm metrics**

Three fields in `src/metrics.rs` plus registrations. Check the existing helper set first — `counter_vec`, `histogram_vec` and `gauge` exist; add a `gauge_vec` mirroring `counter_vec` if it is missing.

```rust
    pub arm_lead_ns: HistogramVec,
    pub arm_transfers: IntCounterVec,
    pub arm_markets_held: IntGaugeVec,
```

```rust
            arm_lead_ns: histogram_vec(
                &registry,
                "dz_arm_lead_ns",
                "Nanoseconds the authoritative arm led the challenger's copy by, per contested \
                 message. The series the re-election thresholds are read off.",
                &["venue", "winner"],
                LEAD_NS_BUCKETS,
            ),
            arm_transfers: counter_vec(
                &registry,
                "dz_arm_authority_transfers_total",
                "Authority transfers by reason (initial/health/silence/margin). A sustained rate \
                 means the thresholds are too tight — every transfer re-baselines each consumer.",
                &["venue", "reason"],
            ),
            arm_markets_held: gauge_vec(
                &registry,
                "dz_arm_markets_held",
                "Markets each arm is currently authoritative for.",
                &["venue", "arm"],
            ),
```

`winner` takes `"leader"` / `"challenger"` — relative, so the label set stays two-valued regardless of arm count.

- [x] **Step 8: Full suite, clippy, commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "feat(ingest): add single-arm sticky authority for uncoordinated publishers"
```

---

## Task 5: Election sampling and periodic re-election

> ### ⚠️ AMENDED 2026-08-07, after the task shipped — the statistic was inert; it is now a matched-trade lead
>
> Built as specified below, then corrected. **The sampling statistic in this section does not work.** The rest — the four flags, both-conditions-must-hold, the sticky philosophy — stands.
>
> **Why the specified statistic is inert, not merely weak.** `observe_challenger` computed `challenger_arrival - leader_last_arrival`. `admit` updates the leader's arrival on every leader message and messages are processed in arrival order, so that quantity is **structurally non-negative**, while `best_challenger` only counts a win at `lead < -margin`. `wins` is therefore always 0, no challenger ever clears the rate condition, and a persistently slower arm keeps authority forever — the exact failure this task exists to prevent. The five tests below passed only because they call `observe_challenger(arm2, t - 10_000)` after `admit(arm1, t)`, an arrival order no real-time caller can produce. Found by the build instance and confirmed by reading the code.
>
> **What it was measuring** is inter-arm *phase*, not lead: the arms are unpairable by wire coordinate, so "the leader's previous message" is not the same event as the challenger's message.
>
> **The replacement: a cross-arm trade matcher on our own receive clock.** Pair the two arms' copies of the *same trade* by content signature, and take `recv_A - recv_B` for the matched pair on a single ns-resolution clock. Signed, so `{winner="challenger"}` is reachable. Pooled **per arm per venue** — latency is an arm property, so every matched trade from a source IP is evidence about that arm, whatever market it came from. That is what makes the sample supply workable: the sports feed carries ~5 trades per 39 s across the whole venue, which is nothing per market but ~38 per 300 s window pooled.
>
> **Match on trades only, never level updates.** `authority`'s own module doc already gives the reason: a level update's cross-arm-common fields reduce to `(side, price, quantity)` on a coarse bounded price grid, so a content match would mis-pair constantly. A trade's `(instrument, price, size, aggressor)` plus arrival proximity is near-unique. **The key needs a time component** — the reference implementation's does not, so two identical trades inside its window collide (see *Prior art*).
>
> **Two approaches rejected, recorded so they are not revisited:**
> * **Inter-message gap** ("time since the other arm's last message"; the faster arm shows the larger median gap). Directionally right but rate-sensitive — it biases toward the chattier arm when the two differ in message granularity — and it **inverts** whenever the lead exceeds half the message period, since it compares `P - L` against `L`.
> * **`recv_ts - venue_ts` per arm.** Tempting: `LevelUpdate.Timestamp` is the venue's own time (publisher `venue.rs`, "`ts_ms` is the venue's own time for the change, lifted"), it is plentiful, and its millisecond quantization is common-mode so it cancels in a difference of medians. It fails on the asymmetry that matters: `ts_ms` is `Option`, and when the venue supplies none the publisher silently substitutes its own clock (`feed.rs:1206`, `lu.timestamp = TsNs(now_ns())`) with **no flag on the wire**. An arm with no venue timestamp then measures only the network leg and looks fastest by construction — and filtering those samples out by their non-millisecond granularity disenfranchises that arm entirely instead. Since the FIX arm is the one both expected to be faster and expected to lack venue timestamps, this is precisely backwards. *(All 30,075 / 22,453 / 643 `LevelUpdate`s in the three 2026-08-07 captures were venue-sourced, i.e. exact-millisecond — but those are presumed all-WS arms, so that measures nothing about FIX.)*
>
> **Prior art, worth reading before writing the matcher.** The upstream publisher repo already has one — `src/publisher/compare.rs` in its publisher crate (447 lines, built and unit-tested), designed in its `2026-07-14-fix-ws-comparison-design.md` for a FIX-vs-WS comparison *inside* the publisher. A bounded time-windowed `pending` map keyed on a content signature, second transport to hit a key emits a signed delta and evicts, `evict_stale` counts "seen only on <transport>". Two gaps to close when lifting it: its trade key is `(ticker, price, size, side)` with **no time component**, and it reports milliseconds.
>
> **Interface delta:** `observe_challenger(&MarketKey, Publisher, u64)` becomes `observe_matched_lead(&str /* venue */, Publisher, i64 /* signed ns */)`, and `close_window` returns `Vec<(Arc<str>, Publisher)>`. The five tests below are replaced — they encode the arrival order being abandoned. Also added here: a **minimum-sample floor** (`--arb-min-window-samples`, 32), without which a handful of lucky matches transfers a venue.
>
> **The matcher shipped as `src/ingest/arm_race.rs`** (`6d004b4`). `ArmRace::on_trade(venue, instrument, price_raw, qty_raw, aggressor, arm, recv_ns) -> Option<Match>`, and `Match::lead_for(leader) -> Option<(challenger, signed_ns)>` does the sign conversion in **one tested place** — getting that sign backwards is what made the first version inert, so it does not belong at the call site. Content-keyed on raw fixed-point integers (never floats), with a **FIFO per signature** so identical repeats pair in order instead of colliding, window eviction that attributes unmatched arrivals per arm ("seen only on this arm"), and a `MAX_PENDING` cap because the source is unauthenticated. `a_faster_challenger_actually_wins_the_election` drives a genuinely faster arm through the matcher into `StickyAuthority` in true arrival order and asserts it takes authority — the round trip the old statistic failed.
>
> **Left for Task 12, deliberately:** the matcher's window is a `ArmRace::new(window_ns)` parameter with a 5s default rather than a CLI flag, because a flag nothing reads is the same defect as an inert knob. Wire it when the caller exists. Same for counting `evict_stale`'s per-arm unmatched returns.

**Why:** without re-sampling, whichever arm delivers first at startup holds authority forever, even when persistently slower. With naive re-sampling, jitter flaps authority and re-baselines every consumer's book on each flip. The rule: **transfer only on a sustained margin, never on a single faster sample.**

The design leaves the interval, the threshold and the metric set open (§10 Q2). This task picks defensible defaults and **exposes all four as CLI flags** — the honest resolution, since the numbers are read off the series Task 4 added and retuning must not need a rebuild.

| Flag | Default | Why this value |
|---|---|---|
| `--arb-sample-interval-secs` | `300` | At the measured ~402 frames/s a five-minute window holds far more samples than the margin test needs, while bounding "a slower leader held indefinitely" to five minutes. |
| `--arb-transfer-margin-us` | `1000` (1 ms) | Below inter-arm network jitter there is nothing to win. One millisecond is an order below the >100 ms inter-feed skew seen on the sibling race and an order above host scheduling noise. |
| `--arb-transfer-win-rate` | `0.8` | The challenger must also lead in ≥80% of the window's contested samples. Two independent conditions, so a heavy tail cannot carry a transfer alone. |
| `--arb-leader-timeout-secs` | `2` | Task 4's silence trigger. Above the venue's heartbeat cadence, below any tolerable outage. |

**Files:**
- Modify: `src/ingest/authority.rs` (`AuthorityConfig`, the sampler)
- Modify: `src/main.rs` (four flags)
- Modify: `docs/metrics.md`

**Interfaces:**
- Consumes: `StickyAuthority`, `MarketKey`, `transfer_to` from Task 4.
- Produces: `pub struct AuthorityConfig { pub leader_timeout_ns: u64, pub sample_interval_ns: u64, pub transfer_margin_ns: u64, pub transfer_win_rate: f64 }` and `StickyAuthority::new(cfg: AuthorityConfig)` replacing the `u64` constructor; `observe_challenger`, `close_window`, `leader_of`. Task 12 constructs the config from the CLI args.

- [x] **Step 1: Branch**

Branch off Task 4's final commit.

- [x] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/authority.rs`:

```rust
    fn cfg() -> AuthorityConfig {
        AuthorityConfig {
            leader_timeout_ns: 2_000_000_000,
            sample_interval_ns: 1_000_000, // 1ms window keeps the tests fast
            transfer_margin_ns: 1_000,     // 1us
            transfer_win_rate: 0.8,
        }
    }

    /// A challenger consistently faster than the margin takes authority when the window closes —
    /// not on its first fast sample.
    #[test]
    fn sustained_margin_transfers_at_window_close() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t.saturating_sub(10_000)); // 10us ahead
        }
        assert_eq!(a.leader_of(&key()), Some(arm(1)), "window still open");
        let moved = a.close_window(1_000_001);
        assert_eq!(moved.len(), 1);
        assert_eq!(a.leader_of(&key()), Some(arm(2)));
    }

    /// One fast sample among slow ones must not transfer — that is the flap the sustained margin
    /// exists to prevent.
    #[test]
    fn one_fast_sample_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            let challenger = if i == 5 { t - 10_000 } else { t + 10_000 };
            a.observe_challenger(&key(), arm(2), challenger);
        }
        assert!(a.close_window(1_000_001).is_empty());
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// Winning often but only by noise must not transfer either: margin and win rate are
    /// independent conditions and both must hold.
    #[test]
    fn winning_within_the_margin_does_not_transfer() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 100); // 100ns < 1us margin
        }
        assert!(a.close_window(1_000_001).is_empty());
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// Closing a window clears its samples, so the next window judges only its own evidence.
    #[test]
    fn window_close_resets_the_sample_set() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 100_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 10_000);
        }
        a.close_window(1_000_001);
        assert_eq!(a.leader_of(&key()), Some(arm(2)));
        for i in 11..=20u64 {
            let t = i * 100_000;
            a.admit(key(), arm(2), t);
            a.observe_challenger(&key(), arm(1), t - 10_000);
        }
        a.close_window(2_000_002);
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
    }

    /// A window that has not elapsed is left open, samples intact.
    #[test]
    fn window_does_not_close_early() {
        let mut a = StickyAuthority::new(cfg());
        a.admit(key(), arm(1), 0);
        for i in 1..=10u64 {
            let t = i * 10_000;
            a.admit(key(), arm(1), t);
            a.observe_challenger(&key(), arm(2), t - 10_000);
        }
        assert!(a.close_window(500_000).is_empty(), "half the interval elapsed");
        assert_eq!(a.leader_of(&key()), Some(arm(1)));
        assert_eq!(a.close_window(1_000_001).len(), 1, "samples survived the early call");
    }
```

- [x] **Step 3: Run to verify failure**

```bash
cargo test --lib authority::tests::sustained_margin
```

Expected: FAIL to compile — `cannot find struct 'AuthorityConfig' in this scope`.

- [x] **Step 4: Add the config**

In `src/ingest/authority.rs`:

```rust
/// Tunables for [`StickyAuthority`], all CLI-settable (`--arb-*`). Read the two transfer conditions
/// off `dz_arm_lead_ns` and `dz_arm_authority_transfers_total`: a sustained transfer rate means they
/// are too loose; a leader whose `dz_arm_lead_ns{winner="challenger"}` sits persistently past the
/// margin with no transfer means they are too tight.
#[derive(Debug, Clone, Copy)]
pub struct AuthorityConfig {
    pub leader_timeout_ns: u64,
    pub sample_interval_ns: u64,
    /// The challenger must beat the leader by at least this much on median to transfer.
    pub transfer_margin_ns: u64,
    /// ...and lead in at least this fraction of the window's contested samples.
    pub transfer_win_rate: f64,
}

/// Cap on samples retained per market per window. Already two orders more than the margin test
/// needs, so overflow costs precision on a pathological market, never memory.
const MAX_WINDOW_SAMPLES: usize = 4096;
```

- [x] **Step 5: Extend `Held` and the constructor**

Add to `struct Held`:

```rust
    /// Signed leads for the open window: positive when the leader was ahead of the challenger's
    /// copy, negative when the challenger was.
    samples: Vec<(Publisher, i64)>,
    window_opened_ns: u64,
```

Initialize `samples: Vec::new(), window_opened_ns: arrival_ns` at both `Held` construction sites (`admit`'s `None` arm and nothing else — `transfer_to` mutates in place). Replace the `leader_timeout_ns: u64` field with `cfg: AuthorityConfig`, change `new` to take it, and read `self.cfg.leader_timeout_ns` in `admit`. Update the Task 4 tests' `StickyAuthority::new(TIMEOUT)` calls to `StickyAuthority::new(AuthorityConfig { leader_timeout_ns: TIMEOUT, sample_interval_ns: u64::MAX, transfer_margin_ns: 1_000, transfer_win_rate: 0.8 })` so no window closes during them.

- [x] **Step 6: Implement the sampler**

```rust
    /// Record a challenger's arrival against the leader's most recent message. The arbiter calls
    /// this on every `Contest`, so the sampler sees exactly the head-to-heads the histogram reports.
    pub fn observe_challenger(&mut self, key: &MarketKey, challenger: Publisher, arrival_ns: u64) {
        if let Some(h) = self.held.get_mut(key) {
            if h.samples.len() < MAX_WINDOW_SAMPLES {
                let lead = arrival_ns as i64 - h.leader_arrival_ns as i64;
                h.samples.push((challenger, lead));
            }
        }
    }

    /// Close every elapsed sampling window, transferring authority where a challenger cleared BOTH
    /// conditions. Returns the markets that moved, so the caller counts
    /// `dz_arm_authority_transfers_total{reason="margin"}`.
    pub fn close_window(&mut self, now_ns: u64) -> Vec<(MarketKey, Publisher)> {
        let (margin, rate, interval) = (
            self.cfg.transfer_margin_ns as i64,
            self.cfg.transfer_win_rate,
            self.cfg.sample_interval_ns,
        );
        let mut moved = Vec::new();
        for (key, h) in self.held.iter_mut() {
            if now_ns.saturating_sub(h.window_opened_ns) < interval {
                continue;
            }
            let winner = best_challenger(&h.samples, margin, rate);
            h.samples.clear();
            h.window_opened_ns = now_ns;
            if let Some(c) = winner {
                if c != h.leader {
                    h.leader = c;
                    h.leader_arrival_ns = now_ns;
                    h.contest_recorded = false;
                    moved.push((key.clone(), c));
                }
            }
        }
        moved
    }

    /// The current leader for a market, or `None` if none has been elected.
    pub fn leader_of(&self, key: &MarketKey) -> Option<Publisher> {
        self.held.get(key).map(|h| h.leader)
    }
```

and the free function encoding both conditions:

```rust
/// The challenger that beat the leader by at least `margin` on median AND led at least `rate` of
/// its own samples. `None` when none cleared both — the ordinary case, and why authority is sticky
/// rather than raced.
fn best_challenger(samples: &[(Publisher, i64)], margin: i64, rate: f64) -> Option<Publisher> {
    let mut by_arm: HashMap<Publisher, Vec<i64>> = HashMap::new();
    for &(p, lead) in samples {
        by_arm.entry(p).or_default().push(lead);
    }
    let mut best: Option<(Publisher, i64)> = None;
    for (p, mut leads) in by_arm {
        let wins = leads.iter().filter(|&&l| l < -margin).count();
        if leads.is_empty() || (wins as f64) / (leads.len() as f64) < rate {
            continue;
        }
        leads.sort_unstable();
        let median = leads[leads.len() / 2];
        if median > -margin {
            continue;
        }
        if best.is_none_or(|(_, m)| median < m) {
            best = Some((p, median));
        }
    }
    best.map(|(p, _)| p)
}
```

- [x] **Step 7: Run the tests to verify they pass**

```bash
cargo test --lib authority
```

Expected: all fourteen PASS.

- [x] **Step 8: Add the CLI flags**

In `src/main.rs`'s `Args`, following the existing `#[arg(long, env = ...)]` convention:

```rust
    /// Seconds between arm re-election samples for `Sticky` venues. Longer holds a slower arm
    /// longer; shorter risks flapping authority, which re-baselines every consumer's book.
    #[arg(long, env = "DZ_ARB_SAMPLE_INTERVAL_SECS", default_value_t = 300)]
    arb_sample_interval_secs: u64,

    /// Microseconds a challenger must beat the authoritative arm by, on median, to take authority.
    #[arg(long, env = "DZ_ARB_TRANSFER_MARGIN_US", default_value_t = 1000)]
    arb_transfer_margin_us: u64,

    /// Fraction of a window's contested samples the challenger must also lead. Independent of the
    /// margin, so a heavy tail alone cannot carry a transfer.
    #[arg(long, env = "DZ_ARB_TRANSFER_WIN_RATE", default_value_t = 0.8)]
    arb_transfer_win_rate: f64,

    /// Seconds of leader silence after which a healthy challenger takes authority.
    #[arg(long, env = "DZ_ARB_LEADER_TIMEOUT_SECS", default_value_t = 2)]
    arb_leader_timeout_secs: u64,
```

Build the `AuthorityConfig` next to the `Arbiter::new` call. The arbiter does not consume it until Task 12; bind it as `let authority_cfg = AuthorityConfig { ... };` with `#[allow(unused_variables)]` (or `let _ = &authority_cfg;`) for this commit and remove the allow in Task 12.

- [x] **Step 9: Document, then commit**

Add the three `dz_arm_*` rows to `docs/metrics.md`, each stating what an operator does with it, plus a "Tuning arm re-election" paragraph naming the four flags, their defaults, and the rule that the two transfer conditions are independent.

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "feat(ingest): re-elect the authoritative arm on a sustained speed margin"
```

---

## Task 6: `channel` and `type` subscription filter dimensions, and scoped replay

**Why:** `channel` is the client-facing filter key (the arm is not — a consumer gets one arbitrated book). `type` lets a consumer take `book` without `quote`, which matters once one venue carries both. Two traps make this bigger than it looks:

1. **There are two match paths.** `SubFilter::matches` (`ws.rs:116`) handles symbol-bearing messages; the no-symbol/`status` branch (`ws.rs:365-379`) compares venue inline and never calls `matches`. Adding a dimension to one and not the other silently exempts `status` from it.
2. **Connect-time replay is unfiltered** (`ws.rs:260-286`) and runs before the client can subscribe. Full-state `depth` made that harmless; incremental `book` does not — replaying every market's book to a client that then subscribes to one is both wasteful and, once markets number in the tens of thousands, a connect-time stall.

`channel` lands here even though no message carries one until Task 11: the filter semantics and both match paths are testable now against a synthetic frame, and splitting them would leave PR 2 depending on PR 5.

**Files:**
- Modify: `src/sinks/ws.rs` (`SubFilter`, `matches`, `PreparedFrame`, `prepare`, both match paths, `serve_client` replay)
- Modify: `PROTOCOL.md` (subscriptions section)
- Test: `src/sinks/ws.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `SubFilter { venue: Option<String>, symbol: Option<String>, channel: Option<u32>, msg_type: Option<String> }` — `msg_type` is `#[serde(rename = "type", default)]`, so the wire name is `type`.
  - `SubFilter::matches(&self, venue: &str, symbol: Option<&str>, channel: Option<u32>, kind: &str) -> bool` — one function both paths call.
  - `PreparedFrame.channel: Option<u32>`, populated by Task 11.
  - `async fn replay_scoped(write, instruments, depth, subs) -> Result<()>` — the scoped replay both the connect path and the subscribe path use.

- [x] **Step 1: Branch**

Branch off Task 5's final commit.

- [x] **Step 2: Write the failing tests**

Add to `mod tests` in `src/sinks/ws.rs`. These test `matches` directly — a pure function, so no server plumbing is needed.

```rust
    fn filter(json: &str) -> SubFilter {
        serde_json::from_str(json).expect("filter parses")
    }

    /// The omitted-field-matches-anything rule must survive the two new dimensions.
    #[test]
    fn empty_filter_still_matches_everything() {
        let f = filter("{}");
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(f.matches("Hyperliquid", Some("SOL"), None, "quote"));
        assert!(f.matches("Lashay", None, None, "status"));
    }

    #[test]
    fn type_filter_selects_one_message_kind() {
        let f = filter(r#"{"type":"book"}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("Lashay", Some("KXBTCPERP"), Some(2), "quote"));
    }

    /// `type` is matched exactly, like `symbol`: the wire values are a closed set the protocol
    /// defines, so a near-miss is a client bug worth surfacing as "no data" rather than guessing.
    #[test]
    fn type_filter_is_exact() {
        assert!(!filter(r#"{"type":"BOOK"}"#).matches("Lashay", Some("X"), None, "book"));
    }

    #[test]
    fn channel_filter_selects_one_channel() {
        let f = filter(r#"{"channel":2}"#);
        assert!(f.matches("Lashay", Some("KXBTCPERP"), Some(2), "book"));
        assert!(!f.matches("Lashay", Some("KXBTCPERP"), Some(1), "book"));
    }

    /// An explicit channel filter must not pass a message that carries no channel — otherwise
    /// `{"channel":2}` would receive every quote on every venue.
    #[test]
    fn channel_filter_excludes_channelless_messages() {
        assert!(!filter(r#"{"channel":2}"#).matches("Hyperliquid", Some("SOL"), None, "quote"));
    }

    /// `status` is venue-level: no symbol and no channel, so it matches on venue and type alone —
    /// the same carve-out `symbol` already has, extended to `channel`. Without this a
    /// `{"venue":"Lashay","channel":2}` subscriber would never learn its venue went down.
    #[test]
    fn status_matches_on_venue_despite_symbol_and_channel_filters() {
        let f = filter(r#"{"venue":"Lashay","symbol":"KXBTCPERP","channel":2}"#);
        assert!(f.matches("Lashay", None, None, "status"));
        assert!(!f.matches("Hyperliquid", None, None, "status"));
    }

    /// ...but an explicit `type` filter still excludes it, so a consumer that asked for `book` only
    /// does not get status frames it never requested.
    #[test]
    fn type_filter_still_excludes_status() {
        assert!(!filter(r#"{"type":"book"}"#).matches("Lashay", None, None, "status"));
    }

    #[test]
    fn venue_stays_case_insensitive() {
        assert!(filter(r#"{"venue":"lashay"}"#).matches("Lashay", Some("X"), None, "book"));
    }
```

- [x] **Step 3: Run the tests to verify they fail**

```bash
cargo test --lib ws::tests
```

Expected: FAIL to compile — `this method takes 2 arguments but 4 arguments were supplied`.

- [x] **Step 4: Widen `SubFilter` and fold both match paths into one function**

Replace `ws.rs:106-125` with:

```rust
/// A subscription filter: a `None` field matches any value (so `{}` = everything).
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
struct SubFilter {
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    /// The wire `channel_id` — the competition, not the arm. Arm identity is deliberately not
    /// client-selectable: exactly one arbitrated book per market reaches the wire.
    #[serde(default)]
    channel: Option<u32>,
    /// Message `type` (`quote`/`trade`/`book`/...). Named `msg_type` in Rust because `type` is a
    /// keyword; the wire name is `type`.
    #[serde(rename = "type", default)]
    msg_type: Option<String>,
}

impl SubFilter {
    /// The single match path. `symbol`/`channel` are `None` for a venue-level message (today only
    /// `status`), and a `None` on the *message* side satisfies a filter on that dimension — a
    /// venue-level message is about the whole venue, so a symbol- or channel-scoped subscriber must
    /// still receive it. A filter dimension the message *does* carry is matched normally.
    fn matches(&self, venue: &str, symbol: Option<&str>, channel: Option<u32>, kind: &str) -> bool {
        // Venue codes are registry identifiers, not free text - match case-insensitively so a
        // subscription for `PHOENIX` / `phoenix` still selects the wire venue `Phoenix`. Symbol and
        // type stay exact (venues name symbols precisely; types are a closed protocol set).
        self.venue
            .as_deref()
            .is_none_or(|v| v.eq_ignore_ascii_case(venue))
            && self.msg_type.as_deref().is_none_or(|t| t == kind)
            && match symbol {
                None => true,
                Some(s) => self.symbol.as_deref().is_none_or(|f| f == s),
            }
            && match channel {
                None => self.channel.is_none() || symbol.is_none(),
                Some(c) => self.channel.is_none_or(|f| f == c),
            }
    }
}
```

The `channel` arm is the one to read twice. A message with no channel passes a channel filter only when it is venue-level (`symbol.is_none()`); a symbol-bearing message with no channel is excluded by an explicit channel filter, which is what `channel_filter_excludes_channelless_messages` pins.

- [x] **Step 5: Carry channel on the prepared frame**

Add to `struct PreparedFrame` (`ws.rs:36-46`):

```rust
    /// The message's `channel_id`, or `None` for a type that carries none. Populated when the
    /// incremental `book` message lands; every current type is `None`.
    channel: Option<u32>,
```

In `prepare` (`ws.rs:77-90`), extend the destructuring tuple to yield `channel` — `None` for all six existing variants — and set it on the constructed frame.

- [x] **Step 6: Route both filter paths through `matches`**

Replace the `msg = rx.recv()` filter block (`ws.rs:364-379`) with a single call, deleting the duplicated inline venue comparison:

```rust
                Ok(frame) => {
                    let pass = subs.is_empty()
                        || subs.iter().any(|f| {
                            f.matches(
                                &frame.venue,
                                frame.symbol.as_deref(),
                                frame.channel,
                                frame.kind,
                            )
                        });
```

The rest of the arm (metrics, write) is unchanged. This is the fix for trap 1: there is now exactly one match path, so a future dimension cannot be added to half of it.

- [x] **Step 7: Scope the replay**

Extract the two replay loops from `serve_client` (`ws.rs:260-286`) into one function both the connect path and the subscribe path call:

```rust
/// Replay current full state matching `subs` (empty = everything): instrument definitions first so
/// precision is known before any book, then the latest `depth` per `(venue, symbol)`.
///
/// Called on connect, and again on each `subscribe` so a client that narrows after connecting is
/// bootstrapped for its new scope rather than waiting for the next event. Replay is idempotent full
/// state, so the overlap a connect-then-subscribe client sees is harmless.
async fn replay_scoped<W>(
    write: &mut W,
    instruments: &InstrumentSnapshot,
    depth: &DepthSnapshot,
    subs: &[SubFilter],
) -> Result<()>
where
    W: SinkExt<WsMessage> + Unpin,
    <W as futures_util::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    let pass = |venue: &str, symbol: &str, kind: &str| {
        subs.is_empty() || subs.iter().any(|f| f.matches(venue, Some(symbol), None, kind))
    };
    let snapshot: Vec<FeedMessage> = {
        let guard = crate::model::lock(instruments);
        guard
            .values()
            .filter(|i| pass(&i.venue, &i.symbol, "instrument"))
            .cloned()
            .map(FeedMessage::Instrument)
            .collect()
    };
    let books: Vec<FeedMessage> = {
        let guard = crate::model::lock(depth);
        guard
            .values()
            .filter(|d| pass(&d.venue, &d.symbol, "depth"))
            .cloned()
            .map(FeedMessage::Depth)
            .collect()
    };
    for m in snapshot.into_iter().chain(books) {
        write
            .send(WsMessage::Text(serde_json::to_string(&m)?.into()))
            .await?;
    }
    Ok(())
}
```

Note the two locks are taken and released before any `await` — holding a `std::sync::MutexGuard` across an await point does not compile for a `!Send` guard and would be a latency bug regardless. Also switch from `.lock().unwrap()` to `crate::model::lock`, matching the poisoning-recovery contract the rest of the codebase uses.

In `serve_client`, replace the two inline loops with `replay_scoped(&mut write, &instruments, &depth, &subs).await?;` (at that point `subs` is empty, so this is exactly today's behavior), and add a scoped replay to the `Subscribe` arm immediately after the `subscription_response` is written:

```rust
                            replay_scoped(&mut write, &instruments, &depth, std::slice::from_ref(&subscription)).await?;
```

Replaying only the newly-added filter, not all of `subs`, keeps a client that subscribes to ten symbols from getting ten full replays of the first one.

- [x] **Step 8: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass, including the existing `ws` integration tests and `tests/e2e.rs`.

- [x] **Step 9: Document**

In `PROTOCOL.md`'s *Subscriptions & filtering* section, replace the filter description:

```markdown
A subscription filter is `{ "venue"?: string, "symbol"?: string, "channel"?: uint32, "type"?: string }` - an **omitted field matches any value** (so `{}` = everything, `{"symbol":"SOL"}` = SOL on every venue, `{"type":"book"}` = book updates only). `venue` is matched **case-insensitively** (`PHOENIX` selects `Phoenix`); `symbol`, `channel` and `type` are matched exactly.

`channel` is the publisher's channel id — the instrument set a feed carries. A message type that carries no channel (everything except `book`) is **excluded** by an explicit `channel` filter, so `{"channel":2}` selects book updates on channel 2 and nothing else.

A venue-level message (`status`) carries neither symbol nor channel and is matched on `venue` and `type` alone, so a `{"venue":"Hyperliquid","symbol":"SOL"}` subscriber still receives Hyperliquid status. A `{"type":"quote"}` subscriber does not, having asked for quotes only.
```

and replace the "Instrument definitions are always replayed on connect regardless of subscriptions" sentence with:

```markdown
Instrument definitions and current book state are replayed on connect (unfiltered, since a client has no subscriptions yet) and again on each `subscribe`, scoped to the filter just added — so a client that narrows after connecting is bootstrapped for its new scope instead of waiting for the next event. Replay is idempotent full state, so the overlap is harmless.
```

Add a `CHANGELOG.md` entry under Unreleased → Added naming both dimensions and the scoped replay.

- [x] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(ws): add channel and type subscription filters, scope replay to them"
```

---

## Task 7: `codec_mbp.rs` — frame walk, exact-length discipline, inherited types

**Why:** the decoder is the foundation for Tasks 9–12, and unlike `codec_midpoint.rs` it ships **byte-validated**, because a Go oracle exists: `go/marketbyprice-parser` in `<edge-multicast-ref>` (branch `main`, `54e9476` = merged PR #29). Match it field-for-field. The wire spec is `<edge-feed-spec>/market-by-price/spec.md`.

**The one deliberate divergence from the sibling codecs: exact body-length equality per message type.** `codec_common::decode_frame_with` bounds the *advance* by the declared length but never checks that the length matches the type; short bodies fall through to `Message::Other` via the bounds-checked readers, and over-long ones decode with trailing bytes ignored. The Go oracle rejects both. Follow the oracle, for a concrete reason it pins with its own test: MBP's `SnapshotBegin` is a **prefix-superset** of MBO's — bytes 0–35 are byte-identical, with `Depth Bound` appended at offset 36. A misrouted or mis-sized 36-byte MBO-shaped `SnapshotBegin` would otherwise decode as MBP with `depth_bound` reading whatever follows, and if that reads `0` the subscriber records a **positive publisher claim of completeness** the publisher never made. That is exactly the failure `Depth Bound` exists to prevent, arrived at through our own decode.

**Files:**
- Create: `src/ingest/codec_mbp.rs`
- Modify: `src/ingest/mod.rs` (`pub mod codec_mbp;`)
- Test: `src/ingest/codec_mbp.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `codec_common::{decode_frame_with, FrameHeader, MSG_HEADER_SIZE, u8le, u16le, u32le, u64le, i64le, cstr}`.
- Produces (Task 8 extends `Message`; Tasks 9–10 consume all of it):
  ```rust
  pub const MAGIC: u16 = 0x4442;
  pub const MSG_HEARTBEAT: u8 = 0x01;  MSG_INSTRUMENT_DEFINITION = 0x02;  MSG_TRADE = 0x04;
  pub const MSG_END_OF_SESSION: u8 = 0x06;  MSG_MANIFEST_SUMMARY = 0x07;  MSG_LIQUIDATION = 0x08;
  pub struct InstrumentDefinition { instrument_id: u32, symbol: Arc<str>, price_exponent: i8, qty_exponent: i8, manifest_seq: u16 }  // impl subscriber::InstrumentDef
  pub struct Trade { instrument_id: u32, source_id: u16, aggressor_side: u8, trade_flags: u8, source_ts: u64, trade_price_raw: i64, trade_qty_raw: u64, trade_id: u64, cumulative_volume_raw: u64 }
  pub struct ManifestSummary { channel_id: u8, valid: bool, manifest_seq: u16, instrument_count: u32, ts: u64 }
  pub enum Message { Heartbeat(u64), InstrumentDefinition(InstrumentDefinition), Trade(Trade), EndOfSession(u64), ManifestSummary(ManifestSummary), Other }
  pub fn decode_frame(buf: &[u8]) -> Result<(FrameHeader, Vec<Message>)>
  ```

- [x] **Step 1: Branch, and confirm the oracle is current**

Branch off Task 6's final commit. Then check the reference decoder checkout is up to date, since this task validates against it:

```bash
git -C <edge-multicast-ref> fetch origin
git -C <edge-multicast-ref> log --oneline -1 origin/main
```

The local checkout is one commit behind `origin/main` (`b2a5b48`, merged PR #34, which adds `go/marketbyprice-bot` — the *state-machine* oracle Task 9 wants). `go/marketbyprice-parser/marketbyprice_wire.go` is the codec oracle for this task and is already present.

- [x] **Step 2: Write the failing tests**

Create `src/ingest/codec_mbp.rs`. Every test builds a frame from literal offsets transcribed from the spec, so it is offset-independent of the decoder.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 24-byte MBP frame header carrying `msg_count` messages and `body_len` body bytes.
    fn frame_header(msg_count: u8, reset_count: u8, body_len: usize) -> Vec<u8> {
        let mut h = vec![0u8; 24];
        h[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        h[2] = 1; // schema version
        h[3] = 2; // channel id
        h[4..12].copy_from_slice(&7u64.to_le_bytes()); // sequence
        h[12..20].copy_from_slice(&1_700_000_000_000_000_000u64.to_le_bytes());
        h[20] = msg_count;
        h[21] = reset_count;
        h[22..24].copy_from_slice(&((24 + body_len) as u16).to_le_bytes());
        h
    }

    /// Wrap a body in its 4-byte message header. `len` is the TOTAL message length.
    fn msg(ty: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let mut m = vec![ty, (4 + body.len()) as u8];
        m.extend_from_slice(&flags.to_le_bytes());
        m.extend_from_slice(body);
        m
    }

    fn one(ty: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let m = msg(ty, flags, body);
        let mut f = frame_header(1, 0, m.len());
        f.extend_from_slice(&m);
        f
    }

    #[test]
    fn rejects_a_sibling_protocols_magic() {
        let mut f = one(MSG_HEARTBEAT, 0, &[0u8; 12]);
        f[0..2].copy_from_slice(&0x4444u16.to_le_bytes()); // market-by-order
        assert!(decode_frame(&f).is_err());
    }

    #[test]
    fn frame_header_fields_decode() {
        let (h, _) = decode_frame(&one(MSG_HEARTBEAT, 0, &[0u8; 12])).unwrap();
        assert_eq!(h.schema_version, 1);
        assert_eq!(h.channel_id, 2);
        assert_eq!(h.sequence, 7);
        assert_eq!(h.reset_count, 0);
    }

    /// spec: Heartbeat 0x01, 16 bytes. Body: channel_id @0, ts @4.
    #[test]
    fn heartbeat_decodes() {
        let mut b = vec![0u8; 12];
        b[0] = 2;
        b[4..12].copy_from_slice(&99u64.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_HEARTBEAT, 0, &b)).unwrap();
        assert!(matches!(m[0], Message::Heartbeat(99)));
    }

    /// spec: InstrumentDefinition 0x02, 80 bytes. Body: id @0, symbol @4..20, price_exp i8 @37,
    /// qty_exp i8 @38, manifest_seq @74. Identical to the byte-validated top-of-book layout.
    #[test]
    fn instrument_definition_decodes() {
        let mut b = vec![0u8; 76];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..15].copy_from_slice(b"KXBTCPERP\0\0");
        b[37] = (-4i8) as u8;
        b[38] = 0;
        b[74..76].copy_from_slice(&3u16.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_INSTRUMENT_DEFINITION, 0, &b)).unwrap();
        let Message::InstrumentDefinition(d) = &m[0] else { panic!("{:?}", m[0]) };
        assert_eq!(d.instrument_id, 41);
        assert_eq!(&*d.symbol, "KXBTCPERP");
        assert_eq!(d.price_exponent, -4);
        assert_eq!(d.qty_exponent, 0);
        assert_eq!(d.manifest_seq, 3);
    }

    /// spec: Trade 0x04, 52 bytes. Body: id @0, source @4, aggressor @6, flags @7, ts @8,
    /// price i64 @16, qty @24, trade_id @32, cumulative @40.
    ///
    /// NOTE the aggressor encoding is 1=Buy / 2=Sell / 0=Unknown — a DIFFERENT value space from
    /// `Side` (0=Bid / 1=Ask) on the book messages. One shared constant would silently invert.
    #[test]
    fn trade_decodes() {
        let mut b = vec![0u8; 48];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..6].copy_from_slice(&3u16.to_le_bytes());
        b[6] = AGGRESSOR_SELL;
        b[8..16].copy_from_slice(&555u64.to_le_bytes());
        b[16..24].copy_from_slice(&6200i64.to_le_bytes());
        b[24..32].copy_from_slice(&150u64.to_le_bytes());
        b[32..40].copy_from_slice(&0u64.to_le_bytes()); // FIX-sourced: no venue trade id
        let (_, m) = decode_frame(&one(MSG_TRADE, 0, &b)).unwrap();
        let Message::Trade(t) = &m[0] else { panic!() };
        assert_eq!(t.instrument_id, 41);
        assert_eq!(t.aggressor_side, AGGRESSOR_SELL);
        assert_eq!(t.trade_price_raw, 6200);
        assert_eq!(t.trade_qty_raw, 150);
        assert_eq!(t.trade_id, 0, "the sentinel the arbiter bypasses on");
    }

    /// spec: ManifestSummary 0x07, 24 bytes. Body: channel @0, valid @1, seq @4, count @8, ts @12.
    #[test]
    fn manifest_summary_decodes() {
        let mut b = vec![0u8; 20];
        b[0] = 2;
        b[1] = 1;
        b[4..6].copy_from_slice(&3u16.to_le_bytes());
        b[8..12].copy_from_slice(&13u32.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_MANIFEST_SUMMARY, 0, &b)).unwrap();
        let Message::ManifestSummary(s) = &m[0] else { panic!() };
        assert!(s.valid);
        assert_eq!(s.manifest_seq, 3);
        assert_eq!(s.instrument_count, 13);
    }

    /// spec: EndOfSession 0x06, 12 bytes. Body: ts @0.
    #[test]
    fn end_of_session_decodes() {
        let (_, m) = decode_frame(&one(MSG_END_OF_SESSION, 0, &42u64.to_le_bytes())).unwrap();
        assert!(matches!(m[0], Message::EndOfSession(42)));
    }

    /// `0x03` (Quote in the top-of-book feed) and `0x05` are reserved here **specifically** so a
    /// misrouted sibling frame cannot cross-decode. They must skip by length, never decode.
    #[test]
    fn reserved_types_do_not_decode() {
        for ty in [0x03u8, 0x05] {
            let (_, m) = decode_frame(&one(ty, 0, &[0u8; 20])).unwrap();
            assert!(matches!(m[0], Message::Other), "type {ty:#04x} decoded");
        }
    }

    /// An unknown type is skipped by its declared length and the walk continues — a following
    /// known message must still decode.
    #[test]
    fn unknown_type_is_skipped_not_fatal() {
        let unknown = msg(0x7F, 0, &[0u8; 8]);
        let hb = {
            let mut b = vec![0u8; 12];
            b[4..12].copy_from_slice(&77u64.to_le_bytes());
            msg(MSG_HEARTBEAT, 0, &b)
        };
        let mut f = frame_header(2, 0, unknown.len() + hb.len());
        f.extend_from_slice(&unknown);
        f.extend_from_slice(&hb);
        let (_, m) = decode_frame(&f).unwrap();
        assert!(matches!(m[0], Message::Other));
        assert!(matches!(m[1], Message::Heartbeat(77)));
    }

    /// Exact length equality, not `>=`. The forward-compat "ignore trailing bytes" rule applies
    /// across a Schema Version bump; within v1 an unexpected body length is malformed. Matches the
    /// Go oracle's `TestNewBodies_ExactLengthOnly` / `TestInheritedBodies_ExactLengthOnly`.
    #[test]
    fn wrong_body_length_does_not_decode() {
        for (ty, correct) in [
            (MSG_HEARTBEAT, 12usize),
            (MSG_INSTRUMENT_DEFINITION, 76),
            (MSG_TRADE, 48),
            (MSG_END_OF_SESSION, 8),
            (MSG_MANIFEST_SUMMARY, 20),
        ] {
            for len in [correct - 1, correct + 1] {
                let (_, m) = decode_frame(&one(ty, 0, &vec![0u8; len])).unwrap();
                assert!(matches!(m[0], Message::Other), "type {ty:#04x} len {len} decoded");
            }
        }
    }
}
```

- [x] **Step 3: Run to verify failure**

Add `pub mod codec_mbp;` to `src/ingest/mod.rs`, then:

```bash
cargo test --lib codec_mbp
```

Expected: FAIL to compile — `cannot find value 'MAGIC' in this scope`.

- [x] **Step 4: Write the module head, constants and inherited types**

Prepend to `src/ingest/codec_mbp.rs`:

```rust
//! Decoder for the DoubleZero Edge **Market-by-Price** feed (frame magic `0x4442`).
//!
//! Price-aggregated L2: each `LevelUpdate` states the complete resulting state of one price level,
//! with in-band snapshot+delta recovery on a third port. Shares the 24-byte frame header, 4-byte
//! message header and generic frame-walker in [`crate::ingest::codec_common`]; only the magic and
//! the bodies differ.
//!
//! **Validated against `go/marketbyprice-parser`** (edge-multicast-ref, merged PR #29), so this
//! ships byte-validated rather than draft-only — the trap `codec_midpoint` is still in. Two things
//! the oracle does that the sibling codecs here do not, both deliberate:
//!
//! * **Exact body-length equality per type, not `>=`.** The forward-compatibility rule that a
//!   decoder ignores trailing bytes applies across a Schema Version bump; within v1 an unexpected
//!   length is malformed. This is load-bearing, not pedantry: `SnapshotBegin` is a prefix-superset
//!   of the market-by-order feed's — bytes 0-35 identical, `Depth Bound` appended at 36 — so a
//!   36-byte sibling-shaped body would otherwise decode with `depth_bound` reading whatever
//!   follows, and a `0` there is a positive publisher claim of a complete book that no publisher
//!   made.
//! * **Enums decode permissively**: any `u8` is accepted and unknown values mean Unknown, per the
//!   spec's "receivers MUST accept any `u8`". The opposite of the top-of-book codec's strict decode.
//!
//! `Side` (0=Bid, 1=Ask) and `Aggressor Side` (0=Unknown, 1=Buy, 2=Sell) are DIFFERENT value
//! spaces. They have separate constants here and must never share one.

use std::sync::Arc;

use anyhow::Result;

use crate::ingest::codec_common::{
    cstr, decode_frame_with, i64le, u16le, u32le, u64le, u8le, FrameHeader, MSG_HEADER_SIZE,
};

pub const MAGIC: u16 = 0x4442; // "BD"

// Shared with the top-of-book feed (byte-identical layouts).
pub const MSG_HEARTBEAT: u8 = 0x01;
pub const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const MSG_TRADE: u8 = 0x04;
pub const MSG_END_OF_SESSION: u8 = 0x06;
pub const MSG_MANIFEST_SUMMARY: u8 = 0x07;
pub const MSG_LIQUIDATION: u8 = 0x08;

/// Trade aggressor. NOT the book `Side` value space — see the module doc.
pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

/// Total on-wire message sizes, including the 4-byte header. Enforced exactly (see the module doc).
pub mod sizes {
    pub const HEARTBEAT: usize = 16;
    pub const INSTRUMENT_DEFINITION: usize = 80;
    pub const TRADE: usize = 52;
    pub const END_OF_SESSION: usize = 12;
    pub const MANIFEST_SUMMARY: usize = 24;
    pub const LIQUIDATION: usize = 48;
}

/// 80-byte instrument definition — the top-of-book layout verbatim.
#[derive(Debug, Clone)]
pub struct InstrumentDefinition {
    pub instrument_id: u32,
    pub symbol: Arc<str>,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub manifest_seq: u16,
}

impl crate::ingest::subscriber::InstrumentDef for InstrumentDefinition {
    fn id(&self) -> u32 {
        self.instrument_id
    }
    fn manifest_seq(&self) -> u16 {
        self.manifest_seq
    }
}

/// 52-byte trade print. `trade_id == 0` means the upstream had no venue trade id (a FIX source has
/// none); the arbiter bypasses its dedup window on that sentinel rather than keying on it.
#[derive(Debug, Clone)]
pub struct Trade {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub trade_flags: u8,
    pub source_ts: u64,
    pub trade_price_raw: i64,
    pub trade_qty_raw: u64,
    pub trade_id: u64,
    pub cumulative_volume_raw: u64,
}

#[derive(Debug, Clone)]
pub struct ManifestSummary {
    pub channel_id: u8,
    pub valid: bool,
    pub manifest_seq: u16,
    pub instrument_count: u32,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub enum Message {
    Heartbeat(u64),
    InstrumentDefinition(InstrumentDefinition),
    Trade(Trade),
    EndOfSession(u64),
    ManifestSummary(ManifestSummary),
    /// Reserved (`0x03`/`0x05`), unknown, or malformed-length: skipped by declared length.
    Other,
}
```

- [x] **Step 5: Write the frame walk and the body decoders**

```rust
/// Decode one datagram. `msg_len` is checked for exact equality with the type's declared size
/// before any field is read, so a mis-sized body becomes [`Message::Other`] rather than decoding
/// garbage into a field that has semantics (see the module doc's `Depth Bound` case).
pub fn decode_frame(buf: &[u8]) -> Result<(FrameHeader, Vec<Message>)> {
    decode_frame_with(buf, MAGIC, |ty, _flags, b, off| {
        let msg_len = b[off + 1] as usize;
        let body = off + MSG_HEADER_SIZE;
        let exact = |n: usize| msg_len == n;
        match ty {
            MSG_HEARTBEAT if exact(sizes::HEARTBEAT) => {
                decode_heartbeat(b, body).unwrap_or(Message::Other)
            }
            MSG_INSTRUMENT_DEFINITION if exact(sizes::INSTRUMENT_DEFINITION) => {
                decode_instrument_definition(b, body).unwrap_or(Message::Other)
            }
            MSG_TRADE if exact(sizes::TRADE) => decode_trade(b, body).unwrap_or(Message::Other),
            MSG_END_OF_SESSION if exact(sizes::END_OF_SESSION) => {
                u64le(b, body).map(Message::EndOfSession).unwrap_or(Message::Other)
            }
            MSG_MANIFEST_SUMMARY if exact(sizes::MANIFEST_SUMMARY) => {
                decode_manifest_summary(b, body).unwrap_or(Message::Other)
            }
            // `0x03`/`0x05` are reserved to stop a misrouted sibling frame cross-decoding, and
            // `MSG_LIQUIDATION` carries nothing this bridge re-serves. Both fall through here.
            _ => Message::Other,
        }
    })
}

fn decode_heartbeat(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::Heartbeat(u64le(b, o + 4)?))
}

fn decode_instrument_definition(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::InstrumentDefinition(InstrumentDefinition {
        instrument_id: u32le(b, o)?,
        symbol: Arc::from(cstr(b, o + 4, 16)?.as_str()),
        price_exponent: u8le(b, o + 37)? as i8,
        qty_exponent: u8le(b, o + 38)? as i8,
        manifest_seq: u16le(b, o + 74)?,
    }))
}

fn decode_trade(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::Trade(Trade {
        instrument_id: u32le(b, o)?,
        source_id: u16le(b, o + 4)?,
        aggressor_side: u8le(b, o + 6)?,
        trade_flags: u8le(b, o + 7)?,
        source_ts: u64le(b, o + 8)?,
        trade_price_raw: i64le(b, o + 16)?,
        trade_qty_raw: u64le(b, o + 24)?,
        trade_id: u64le(b, o + 32)?,
        cumulative_volume_raw: u64le(b, o + 40)?,
    }))
}

fn decode_manifest_summary(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::ManifestSummary(ManifestSummary {
        channel_id: u8le(b, o)?,
        valid: u8le(b, o + 1)? == 1,
        manifest_seq: u16le(b, o + 4)?,
        instrument_count: u32le(b, o + 8)?,
        ts: u64le(b, o + 12)?,
    }))
}
```

- [x] **Step 6: Run the tests to verify they pass**

```bash
cargo test --lib codec_mbp
```

Expected: all eleven PASS.

- [x] **Step 7: Cross-check every offset against the Go oracle**

Open `<edge-multicast-ref>/go/marketbyprice-parser/marketbyprice_wire.go` and confirm each struct's slice bounds against the decoder above. The Go offsets are **body-relative**; ours are body-relative too (`o` already points past the 4-byte header), so they compare directly:

| Type | Go body slices | Must equal |
|---|---|---|
| `InstrumentDefinitionBody` (76) | id `[0:4]`, symbol `[4:20]`, priceExp `int8([37])`, qtyExp `int8([38])`, manifestSeq `[74:76]` | Step 5's `decode_instrument_definition` |
| `TradeBody` (48) | id `[0:4]`, source `[4:6]`, aggressor `[6]`, flags `[7]`, ts `[8:16]`, price `int64([16:24])`, qty `[24:32]`, tradeID `[32:40]`, cumVol `[40:48]` | `decode_trade` |
| `ManifestSummaryBody` (20) | channel `[0]`, valid `[1]`, seq `[4:6]`, count `[8:12]`, ts `[12:20]` | `decode_manifest_summary` |
| `HeartbeatBody` (12) | channel `[0]`, ts `[4:12]` | `decode_heartbeat` |
| `EndOfSessionBody` (8) | ts `[0:8]` | the `MSG_END_OF_SESSION` arm |

Any disagreement is a decoder bug, not an oracle bug — fix ours.

- [x] **Step 8: Pin the sharing with the byte-validated top-of-book codec**

Add a test asserting the shared layouts really are shared, so the claim is self-enforcing rather than eyeballed (this mirrors `codec_mbo_fixtures.rs`'s `tob_shared_layouts_decode_identically`):

```rust
    /// The shared types' strongest guarantee is that they are the top-of-book layout, which is
    /// byte-validated against the reference Go decoder. Decode the SAME body bytes through both
    /// codecs and require equal fields, so a drift in either is a test failure rather than a
    /// silent divergence.
    #[test]
    fn tob_shared_layouts_decode_identically() {
        let mut b = vec![0u8; 76];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..15].copy_from_slice(b"KXBTCPERP\0\0");
        b[37] = (-4i8) as u8;
        b[38] = (-2i8) as u8;
        b[74..76].copy_from_slice(&9u16.to_le_bytes());

        let mut mbp = frame_header(1, 0, 80);
        mbp.extend_from_slice(&msg(MSG_INSTRUMENT_DEFINITION, 0, &b));
        let mut tob = mbp.clone();
        tob[0..2].copy_from_slice(&crate::ingest::codec::MAGIC.to_le_bytes());

        let (_, m) = decode_frame(&mbp).unwrap();
        let (_, t) = crate::ingest::codec::decode_frame(&tob).unwrap();
        let Message::InstrumentDefinition(a) = &m[0] else { panic!() };
        let crate::ingest::codec::Message::InstrumentDefinition(c) = &t[0] else { panic!() };
        assert_eq!(a.instrument_id, c.instrument_id);
        assert_eq!(&*a.symbol, &*c.symbol);
        assert_eq!(a.price_exponent, c.price_exponent);
        assert_eq!(a.qty_exponent, c.qty_exponent);
        assert_eq!(a.manifest_seq, c.manifest_seq);
    }
```

`codec::MAGIC` and `codec::Message` may need `pub` visibility — check `src/ingest/codec.rs` and widen if so.

- [x] **Step 9: Full suite, clippy, commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "feat(codec): add the market-by-price frame walk and inherited message types"
```

---

## Task 8: `codec_mbp.rs` — the price-keyed message types

**Files:**
- Modify: `src/ingest/codec_mbp.rs`
- Modify: `examples/pcap2frames.rs` (an `Mbp` protocol variant, for the fixture capture)
- Create: `tests/codec_mbp_fixtures.rs`
- Modify: `tests/fixtures/PROVENANCE.md`

**Interfaces:**
- Consumes: Task 7's module, constants and `Message` enum.
- Produces (Tasks 9–10 consume all of it):
  ```rust
  pub const MSG_BATCH_BOUNDARY: u8 = 0x13;  MSG_INSTRUMENT_RESET = 0x14;
  pub const MSG_SNAPSHOT_BEGIN: u8 = 0x20;  MSG_SNAPSHOT_END = 0x22;
  pub const MSG_LEVEL_UPDATE: u8 = 0x40;  MSG_BOOK_CLEAR = 0x41;  MSG_SNAPSHOT_LEVEL = 0x42;
  pub const SIDE_BID: u8 = 0;  SIDE_ASK: u8 = 1;
  pub const CLEAR_SIDE_BID: u8 = 0;  CLEAR_SIDE_ASK: u8 = 1;  CLEAR_SIDE_BOTH: u8 = 2;
  pub const SCOPE_ENTIRE_SIDE: u8 = 0;  SCOPE_FROM_PRICE: u8 = 1;
  pub struct LevelUpdate { instrument_id: u32, source_id: u16, side: u8, action: u8, per_instrument_seq: u32, price_raw: i64, qty_raw: u64, ts: u64, order_count: Option<u16>, level_index: Option<u16>, update_reason: u8, level_flags: u8 }
  pub struct BookClear { instrument_id: u32, source_id: u16, clear_side: u8, scope: u8, per_instrument_seq: u32, from_price_raw: i64, ts: u64, clear_reason: u8 }
  pub struct SnapshotLevel { snapshot_id: u32, price_raw: i64, qty_raw: u64, order_count: Option<u16>, side: u8, level_flags: u8 }
  pub struct SnapshotBegin { instrument_id: u32, anchor_seq: u64, total_levels: u32, snapshot_id: u32, last_instrument_seq: u32, ts: u64, depth_bound: u32 }
  pub struct SnapshotEnd { instrument_id: u32, anchor_seq: u64, snapshot_id: u32 }
  pub struct BatchBoundary { batch_id: u32, batch_time: u64 }
  pub struct InstrumentReset { instrument_id: u32, reason: u8, new_anchor_seq: u64, ts: u64 }
  ```
  New `Message` variants for each. `depth_bound` stays a plain `u32` here — the wire always carries it — and the *unknown* state (`Option<u32>`) belongs to `PriceBook` in Task 9, which is where "no publisher has claimed a bound yet" is a real state.

- [x] **Step 1: Branch**

Branch off Task 7's final commit.

- [x] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/codec_mbp.rs`. Offsets are transcribed from `market-by-price/spec.md`'s field tables and are **message-relative minus 4** (body-relative), matching the Go oracle's slices.

```rust
    /// spec: LevelUpdate 0x40, 48 bytes. Body: id @0, source @4, side @6, action @7, seq @8,
    /// price i64 @12, qty u64 @20, ts @28, order_count @36, level_index @38, reason @40, flags @41.
    #[test]
    fn level_update_decodes() {
        let mut b = vec![0u8; 44];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..6].copy_from_slice(&3u16.to_le_bytes());
        b[6] = SIDE_ASK;
        b[7] = 2; // Change
        b[8..12].copy_from_slice(&17u32.to_le_bytes());
        b[12..20].copy_from_slice(&(-6300i64).to_le_bytes()); // price is SIGNED
        b[20..28].copy_from_slice(&150u64.to_le_bytes());
        b[28..36].copy_from_slice(&999u64.to_le_bytes());
        b[36..38].copy_from_slice(&4u16.to_le_bytes());
        b[38..40].copy_from_slice(&1u16.to_le_bytes());
        b[40] = 1; // Trade
        b[41] = 0b10; // AMM-synthetic
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else { panic!("{:?}", m[0]) };
        assert_eq!(u.instrument_id, 41);
        assert_eq!(u.side, SIDE_ASK);
        assert_eq!(u.action, 2);
        assert_eq!(u.per_instrument_seq, 17);
        assert_eq!(u.price_raw, -6300);
        assert_eq!(u.qty_raw, 150);
        assert_eq!(u.ts, 999);
        assert_eq!(u.order_count, Some(4));
        assert_eq!(u.level_index, Some(1));
        assert_eq!(u.update_reason, 1);
        assert_eq!(u.level_flags, 0b10);
    }

    /// `0xFFFF` on order_count / level_index means "not provided, or beyond what this field can
    /// express" — both saturate at it. A subscriber MUST NOT read it as the magnitude 65535, so it
    /// decodes to `None`. `order_count = 0` by contrast is a REAL value on a LevelUpdate.
    #[test]
    fn level_update_u16_sentinels_are_none_but_zero_is_real() {
        let mut b = vec![0u8; 44];
        b[36..38].copy_from_slice(&0xFFFFu16.to_le_bytes());
        b[38..40].copy_from_slice(&0xFFFFu16.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else { panic!() };
        assert_eq!(u.order_count, None);
        assert_eq!(u.level_index, None);

        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &vec![0u8; 44])).unwrap();
        let Message::LevelUpdate(u) = &m[0] else { panic!() };
        assert_eq!(u.order_count, Some(0), "0 is a real order count");
    }

    /// `Quantity = 0` is valid and means "remove this level" — it must decode, not be rejected.
    #[test]
    fn level_update_zero_quantity_is_valid() {
        let mut b = vec![0u8; 44];
        b[12..20].copy_from_slice(&6300i64.to_le_bytes());
        b[20..28].copy_from_slice(&0u64.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else { panic!() };
        assert_eq!(u.qty_raw, 0);
    }

    /// Enums decode permissively: any `u8` is accepted and interpretation is the caller's. The
    /// decoder must not reject or remap — an `Action` byte that is wrong must never be able to
    /// corrupt a book, and the apply rule ignores `Action` entirely.
    #[test]
    fn level_update_enums_are_permissive() {
        let mut b = vec![0u8; 44];
        b[6] = 200; // side
        b[7] = 200; // action
        b[40] = 200; // update reason
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else { panic!() };
        assert_eq!((u.side, u.action, u.update_reason), (200, 200, 200));
    }

    /// spec: BookClear 0x41, 36 bytes. Body: id @0, source @4, clear_side @6, scope @7, seq @8,
    /// from_price i64 @12, ts @20, clear_reason @28.
    #[test]
    fn book_clear_decodes() {
        let mut b = vec![0u8; 32];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[6] = CLEAR_SIDE_BID;
        b[7] = SCOPE_FROM_PRICE;
        b[8..12].copy_from_slice(&18u32.to_le_bytes());
        b[12..20].copy_from_slice(&6100i64.to_le_bytes());
        b[20..28].copy_from_slice(&1_234u64.to_le_bytes());
        b[28] = 1; // Halt
        let (_, m) = decode_frame(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        let Message::BookClear(c) = &m[0] else { panic!("{:?}", m[0]) };
        assert_eq!(c.clear_side, CLEAR_SIDE_BID);
        assert_eq!(c.scope, SCOPE_FROM_PRICE);
        assert_eq!(c.per_instrument_seq, 18);
        assert_eq!(c.from_price_raw, 6100);
        assert_eq!(c.clear_reason, 1);
    }

    /// `Scope = 1` with `Clear Side = 2` is malformed: one price cannot bound both sides. A
    /// subscriber MUST discard and count it — so the decoder must not hand it up as a valid clear,
    /// or the book logic would clear both sides from one bound.
    #[test]
    fn book_clear_from_price_on_both_sides_is_malformed() {
        let mut b = vec![0u8; 32];
        b[6] = CLEAR_SIDE_BOTH;
        b[7] = SCOPE_FROM_PRICE;
        let (_, m) = decode_frame(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        assert!(matches!(m[0], Message::Other), "must not decode as a clear");
    }

    /// ...but `Clear Side = 2` with `Scope = 0` (clear both sides entirely) is the normal case.
    #[test]
    fn book_clear_both_sides_entirely_is_valid() {
        let mut b = vec![0u8; 32];
        b[6] = CLEAR_SIDE_BOTH;
        b[7] = SCOPE_ENTIRE_SIDE;
        let (_, m) = decode_frame(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        assert!(matches!(m[0], Message::BookClear(_)));
    }

    /// spec: SnapshotLevel 0x42, 32 bytes. Body: snapshot_id @0, price i64 @4, qty u64 @12,
    /// order_count @20, side @22, level_flags @23. Carries NO instrument id — it is implied by the
    /// containing SnapshotBegin, which is why routing must key on the open group (Task 9).
    #[test]
    fn snapshot_level_decodes() {
        let mut b = vec![0u8; 28];
        b[0..4].copy_from_slice(&5u32.to_le_bytes());
        b[4..12].copy_from_slice(&6200i64.to_le_bytes());
        b[12..20].copy_from_slice(&150u64.to_le_bytes());
        b[20..22].copy_from_slice(&2u16.to_le_bytes());
        b[22] = SIDE_BID;
        b[23] = 1;
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_LEVEL, 1, &b)).unwrap();
        let Message::SnapshotLevel(l) = &m[0] else { panic!("{:?}", m[0]) };
        assert_eq!(l.snapshot_id, 5);
        assert_eq!(l.price_raw, 6200);
        assert_eq!(l.qty_raw, 150);
        assert_eq!(l.order_count, Some(2));
        assert_eq!(l.side, SIDE_BID);
        assert_eq!(l.level_flags, 1);
    }

    /// spec: SnapshotBegin 0x20, 40 bytes. Body: id @0, anchor_seq @4, total_levels @12,
    /// snapshot_id @16, last_instrument_seq @20, ts @24, **depth_bound @32**. Bytes 0-35 are the
    /// market-by-order feed's 36-byte body verbatim (its `Total Orders` reads as `Total Levels`);
    /// `ts` at 24 is deliberately not 8-byte aligned, inherited, not a cost of the superset.
    #[test]
    fn snapshot_begin_decodes_including_depth_bound() {
        let mut b = vec![0u8; 36];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..12].copy_from_slice(&900u64.to_le_bytes());
        b[12..16].copy_from_slice(&1210u32.to_le_bytes());
        b[16..20].copy_from_slice(&5u32.to_le_bytes());
        b[20..24].copy_from_slice(&16u32.to_le_bytes());
        b[24..32].copy_from_slice(&7_777u64.to_le_bytes());
        b[32..36].copy_from_slice(&0u32.to_le_bytes()); // 0 = complete book
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
        let Message::SnapshotBegin(s) = &m[0] else { panic!("{:?}", m[0]) };
        assert_eq!(s.instrument_id, 41);
        assert_eq!(s.anchor_seq, 900);
        assert_eq!(s.total_levels, 1210);
        assert_eq!(s.snapshot_id, 5);
        assert_eq!(s.last_instrument_seq, 16);
        assert_eq!(s.ts, 7_777);
        assert_eq!(s.depth_bound, 0);
    }

    /// The reason exact-length matters: a 36-byte MBO-shaped SnapshotBegin body must NOT decode
    /// here. If it did, `depth_bound` would read whatever followed, and a `0` there is a positive
    /// publisher claim of a complete book that no publisher made.
    #[test]
    fn snapshot_begin_rejects_the_short_sibling_layout() {
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &vec![0u8; 32])).unwrap();
        assert!(matches!(m[0], Message::Other));
    }

    #[test]
    fn snapshot_begin_bounded_depth_decodes() {
        let mut b = vec![0u8; 36];
        b[32..36].copy_from_slice(&25u32.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
        let Message::SnapshotBegin(s) = &m[0] else { panic!() };
        assert_eq!(s.depth_bound, 25);
    }

    /// spec: SnapshotEnd 0x22, 20 bytes. Body: id @0, anchor_seq @4, snapshot_id @12.
    #[test]
    fn snapshot_end_decodes() {
        let mut b = vec![0u8; 16];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..12].copy_from_slice(&900u64.to_le_bytes());
        b[12..16].copy_from_slice(&5u32.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_END, 1, &b)).unwrap();
        let Message::SnapshotEnd(e) = &m[0] else { panic!() };
        assert_eq!((e.instrument_id, e.anchor_seq, e.snapshot_id), (41, 900, 5));
    }

    /// spec: BatchBoundary 0x13, 16 bytes. Body: batch_id @0, batch_time @4. Carries no instrument
    /// id — it applies to the whole channel.
    #[test]
    fn batch_boundary_decodes() {
        let mut b = vec![0u8; 12];
        b[0..4].copy_from_slice(&123u32.to_le_bytes());
        b[4..12].copy_from_slice(&456u64.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_BATCH_BOUNDARY, 0, &b)).unwrap();
        let Message::BatchBoundary(bb) = &m[0] else { panic!() };
        assert_eq!((bb.batch_id, bb.batch_time), (123, 456));
    }

    /// spec: InstrumentReset 0x14, 28 bytes. Body: id @0, reason @4, reserved 5-7,
    /// new_anchor_seq @8, ts @16. Carries NO per-instrument seq — it is processed regardless of
    /// sequence state.
    #[test]
    fn instrument_reset_decodes() {
        let mut b = vec![0u8; 24];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4] = 3; // UpstreamGap
        b[8..16].copy_from_slice(&1_000u64.to_le_bytes());
        b[16..24].copy_from_slice(&2_000u64.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_INSTRUMENT_RESET, 0, &b)).unwrap();
        let Message::InstrumentReset(r) = &m[0] else { panic!() };
        assert_eq!((r.instrument_id, r.reason, r.new_anchor_seq, r.ts), (41, 3, 1_000, 2_000));
    }

    /// Extend Task 7's exact-length sweep to the price-keyed types.
    #[test]
    fn wrong_body_length_does_not_decode_price_types() {
        for (ty, correct) in [
            (MSG_BATCH_BOUNDARY, 12usize),
            (MSG_INSTRUMENT_RESET, 24),
            (MSG_SNAPSHOT_BEGIN, 36),
            (MSG_SNAPSHOT_END, 16),
            (MSG_LEVEL_UPDATE, 44),
            (MSG_BOOK_CLEAR, 32),
            (MSG_SNAPSHOT_LEVEL, 28),
        ] {
            for len in [correct - 1, correct + 1] {
                let (_, m) = decode_frame(&one(ty, 0, &vec![0u8; len])).unwrap();
                assert!(matches!(m[0], Message::Other), "type {ty:#04x} len {len} decoded");
            }
        }
    }

    /// The `0x50`-`0x5F` range is reserved for a future positional-index addressing mode. There is
    /// no mode negotiation: a price-keyed subscriber skips them by length like any unknown type.
    #[test]
    fn index_addressing_range_is_skipped() {
        for ty in 0x50u8..=0x5F {
            let (_, m) = decode_frame(&one(ty, 0, &[0u8; 20])).unwrap();
            assert!(matches!(m[0], Message::Other), "type {ty:#04x} decoded");
        }
    }
```

- [x] **Step 3: Run to verify failure**

```bash
cargo test --lib codec_mbp
```

Expected: FAIL to compile — `cannot find value 'MSG_LEVEL_UPDATE'`.

- [x] **Step 4: Add the constants, sizes and structs**

Append to the constants block in `src/ingest/codec_mbp.rs`:

```rust
// Control messages, byte-identical to the market-by-order feed's.
pub const MSG_BATCH_BOUNDARY: u8 = 0x13;
pub const MSG_INSTRUMENT_RESET: u8 = 0x14;
// Snapshot group (snapshot port). `SnapshotBegin` is a prefix-superset of the sibling's; the level
// record is price-keyed and its own type, so it does not collide with the sibling's `0x21`.
pub const MSG_SNAPSHOT_BEGIN: u8 = 0x20;
pub const MSG_SNAPSHOT_END: u8 = 0x22;
// Price-keyed book messages, defined by this feed.
pub const MSG_LEVEL_UPDATE: u8 = 0x40;
pub const MSG_BOOK_CLEAR: u8 = 0x41;
pub const MSG_SNAPSHOT_LEVEL: u8 = 0x42;

/// Book side. NOT the trade `AGGRESSOR_*` value space — see the module doc.
pub const SIDE_BID: u8 = 0;
pub const SIDE_ASK: u8 = 1;

/// `BookClear`'s side, deliberately not the shared `Side`: it extends it with a value no other
/// message in this feed or any sibling accepts.
pub const CLEAR_SIDE_BID: u8 = 0;
pub const CLEAR_SIDE_ASK: u8 = 1;
pub const CLEAR_SIDE_BOTH: u8 = 2;

/// `BookClear`'s scope: the entire side(s), or from `from_price` outward to the far end.
pub const SCOPE_ENTIRE_SIDE: u8 = 0;
pub const SCOPE_FROM_PRICE: u8 = 1;

/// `0xFFFF` on `order_count`/`level_index` means "not provided, or beyond what this field can
/// express" — both saturate at it rather than wrapping, so it must never be read as a magnitude.
const U16_UNAVAILABLE: u16 = 0xFFFF;
```

Add to `mod sizes`:

```rust
    pub const BATCH_BOUNDARY: usize = 16;
    pub const INSTRUMENT_RESET: usize = 28;
    pub const SNAPSHOT_BEGIN: usize = 40;
    pub const SNAPSHOT_END: usize = 20;
    pub const LEVEL_UPDATE: usize = 48;
    pub const BOOK_CLEAR: usize = 36;
    pub const SNAPSHOT_LEVEL: usize = 32;
```

Then the seven structs, exactly as listed in **Interfaces** above. Doc lines worth carrying into the source, one each:

- `LevelUpdate`: "`qty_raw` is the level's **absolute** resulting quantity; `0` removes the level. `action` is informational and MUST NOT gate the apply."
- `LevelUpdate.level_index`: "Informational rank. Never a key, never a locator, and invalid after any later update to the same side."
- `BookClear`: "Asserts the named levels are gone. NOT a resynchronization signal — a subscriber that applies it stays ready."
- `SnapshotLevel`: "No instrument id: it is implied by the containing `SnapshotBegin`, so routing keys on the open group."
- `SnapshotBegin.depth_bound`: "`0` is a positive publisher claim that this snapshot carries the complete book. Non-zero is levels-per-side, beyond which state is **unknown, not empty**."
- `InstrumentReset`: "Carries no per-instrument seq — processed regardless of sequence state."

- [x] **Step 5: Add the `Message` variants and the decoders**

Extend `enum Message` with `LevelUpdate(LevelUpdate)`, `BookClear(BookClear)`, `SnapshotLevel(SnapshotLevel)`, `SnapshotBegin(SnapshotBegin)`, `SnapshotEnd(SnapshotEnd)`, `BatchBoundary(BatchBoundary)`, `InstrumentReset(InstrumentReset)`, then add the arms to `decode_frame`'s match (each `if exact(sizes::X)`) and the body decoders:

```rust
/// `0xFFFF` -> `None`; every other value, including `0`, is a real magnitude.
fn u16_opt(v: u16) -> Option<u16> {
    (v != U16_UNAVAILABLE).then_some(v)
}

fn decode_level_update(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::LevelUpdate(LevelUpdate {
        instrument_id: u32le(b, o)?,
        source_id: u16le(b, o + 4)?,
        side: u8le(b, o + 6)?,
        action: u8le(b, o + 7)?,
        per_instrument_seq: u32le(b, o + 8)?,
        price_raw: i64le(b, o + 12)?,
        qty_raw: u64le(b, o + 20)?,
        ts: u64le(b, o + 28)?,
        order_count: u16_opt(u16le(b, o + 36)?),
        level_index: u16_opt(u16le(b, o + 38)?),
        update_reason: u8le(b, o + 40)?,
        level_flags: u8le(b, o + 41)?,
    }))
}

fn decode_book_clear(b: &[u8], o: usize) -> Option<Message> {
    let clear_side = u8le(b, o + 6)?;
    let scope = u8le(b, o + 7)?;
    // Malformed by spec: one price cannot bound both sides. Dropping it here rather than in the
    // book logic means a bad frame can never clear both sides from a single bound.
    if scope == SCOPE_FROM_PRICE && clear_side == CLEAR_SIDE_BOTH {
        return None;
    }
    Some(Message::BookClear(BookClear {
        instrument_id: u32le(b, o)?,
        source_id: u16le(b, o + 4)?,
        clear_side,
        scope,
        per_instrument_seq: u32le(b, o + 8)?,
        from_price_raw: i64le(b, o + 12)?,
        ts: u64le(b, o + 20)?,
        clear_reason: u8le(b, o + 28)?,
    }))
}

fn decode_snapshot_level(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::SnapshotLevel(SnapshotLevel {
        snapshot_id: u32le(b, o)?,
        price_raw: i64le(b, o + 4)?,
        qty_raw: u64le(b, o + 12)?,
        order_count: u16_opt(u16le(b, o + 20)?),
        side: u8le(b, o + 22)?,
        level_flags: u8le(b, o + 23)?,
    }))
}

fn decode_snapshot_begin(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::SnapshotBegin(SnapshotBegin {
        instrument_id: u32le(b, o)?,
        anchor_seq: u64le(b, o + 4)?,
        total_levels: u32le(b, o + 12)?,
        snapshot_id: u32le(b, o + 16)?,
        last_instrument_seq: u32le(b, o + 20)?,
        ts: u64le(b, o + 24)?,
        depth_bound: u32le(b, o + 32)?,
    }))
}

fn decode_snapshot_end(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::SnapshotEnd(SnapshotEnd {
        instrument_id: u32le(b, o)?,
        anchor_seq: u64le(b, o + 4)?,
        snapshot_id: u32le(b, o + 12)?,
    }))
}

fn decode_batch_boundary(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::BatchBoundary(BatchBoundary {
        batch_id: u32le(b, o)?,
        batch_time: u64le(b, o + 4)?,
    }))
}

fn decode_instrument_reset(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::InstrumentReset(InstrumentReset {
        instrument_id: u32le(b, o)?,
        reason: u8le(b, o + 4)?,
        new_anchor_seq: u64le(b, o + 8)?,
        ts: u64le(b, o + 16)?,
    }))
}
```

- [x] **Step 6: Run the tests to verify they pass**

```bash
cargo test --lib codec_mbp
```

Expected: all twenty-six PASS.

- [x] **Step 7: Cross-check every offset against the Go oracle**

Against `marketbyprice_wire.go`, body-relative:

| Type | Go body slices |
|---|---|
| `LevelUpdateBody` (44) | id `[0:4]`, source `[4:6]`, side `[6]`, action `[7]`, seq `[8:12]`, price `int64([12:20])`, qty `[20:28]`, ts `[28:36]`, orderCount `[36:38]`, levelIndex `[38:40]`, reason `[40]`, flags `[41]` |
| `BookClearBody` (32) | id `[0:4]`, source `[4:6]`, clearSide `[6]`, scope `[7]`, seq `[8:12]`, fromPrice `int64([12:20])`, ts `[20:28]`, reason `[28]` |
| `SnapshotLevelBody` (28) | snapshotID `[0:4]`, price `int64([4:12])`, qty `[12:20]`, orderCount `[20:22]`, side `[22]`, flags `[23]` |
| `SnapshotBeginBody` (36) | id `[0:4]`, anchorSeq `[4:12]`, totalLevels `[12:16]`, snapshotID `[16:20]`, lastInstrSeq `[20:24]`, ts `[24:32]`, depthBound `[32:36]` |
| `SnapshotEndBody` (16) | id `[0:4]`, anchorSeq `[4:12]`, snapshotID `[12:16]` |
| `BatchBoundaryBody` (12) | batchID `[0:4]`, batchTime `[4:12]` |
| `InstrumentResetBody` (24) | id `[0:4]`, reason `[4]`, newAnchorSeq `[8:16]`, ts `[16:24]` |

Signedness to mirror: `i64` for every `*price*` field, `i8` for the two exponents, unsigned everywhere else. A disagreement is our bug.

- [x] **Step 8: Add the fixture test harness** — both halves: the cross-codec pinning plus the real-frame decode tests over the captures Step 9 produced.

Create `tests/codec_mbp_fixtures.rs` with the cross-codec equality test moved out of the unit module (so the integration suite also pins it) plus a real-frame decode test over `tests/fixtures/mbp_{mktdata,refdata,snapshot}.bin`, using `tests/common/replay.rs`'s `split_frames` reader exactly as `tests/codec_mbo_fixtures.rs` does — read that file first and mirror its structure. The real-frame test asserts, at minimum: zero decode errors across every frame; `total_levels` equals the decoded `SnapshotLevel` count between a `SnapshotBegin`/`SnapshotEnd` pair; every `LevelUpdate.per_instrument_seq` for one instrument is dense; and at least one `depth_bound == 0` is observed (the live perps publisher carries the complete book).

- [x] **Step 9: Capture the fixtures** — done from wire captures taken 2026-08-07, but **read `tests/fixtures/PROVENANCE.md` before trusting them as normative.** Two sets are committed rather than one, because the deployment turned out to have two shapes and each covers what the other cannot: `mbp_*` is the **sharded** feed (three `Channel ID`s on one group — the only fixture that exercises per-channel snapshot grouping) and `mbp_perps_*` is the **dense** feed (one channel, thousands of contiguous deltas — what pins sequence handling). The captures also surfaced three publisher-side deviations, all recorded in PROVENANCE: per-arm `Channel ID`s on the older feed (which would break a channel-keyed market key — relevant to Task 4), 16-byte symbol truncation with one real collision on a `(venue, symbol)`-keyed map, and no `BookClear`/`InstrumentReset`/`BatchBoundary`/`EndOfSession` in either window. A longer capture with publisher fixes is expected; the fixture tests assert invariants, not recorded counts, so it drops in without editing a number.

`examples/pcap2frames.rs` needs an `Mbp` variant: add it to `enum Protocol` (`:44`) with magic `[0x42, 0x44]` (`0x4442` LE) and a `process_mbp` arm mirroring `process_mbo` (`:1089`, `:1105`). Then, on a host with the DoubleZero tunnel up and subscribed to the perps MBP group:

```bash
sudo timeout 120 tcpdump -i doublezero1 -nn -s 0 -w lashay_mbp.pcap 'host 233.84.178.4 and udp'
cargo run --example pcap2frames -- lashay_mbp.pcap --protocol mbp --group 233.84.178.4 \
  --src <arm-source-ip> --to 40 -o tests/fixtures/mbp
```

Record the source IP, capture date, frame counts and observed `depth_bound` in `tests/fixtures/PROVENANCE.md` following the existing "MBO fixtures" section's format. Call the source the **perps market-by-price group**, matching how the design doc refers to it.

**If tunnel access is unavailable to whoever executes this task, stop here and say so rather than synthesizing a fixture and calling it a capture.** Ship Steps 1–8 with the module doc recording MBP's oracle strength honestly: offset tests plus field-for-field Go-oracle parity, **no real-frame fixture yet**. That is strictly stronger than `codec_midpoint` (self-consistency only) and weaker than `codec_mbo` (real capture), and the difference must be stated, not papered over. Add the missing fixture to the PR body's "what was not verified".

- [x] **Step 10: Full suite, clippy, commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "feat(codec): decode the market-by-price level, clear and snapshot messages"
```

---

## Task 9: `PriceBook` — the price-keyed recovery state machine

**Why a new type rather than reusing `book.rs`:** `book.rs` is order-keyed (`orders: HashMap<u64, RestingOrder>`) and derives levels by aggregating orders. The MBP wire is already price-aggregated and carries **absolute** level quantities, so there is nothing to aggregate — the order map would be dead weight and the `Add`/`Cancel`/`Execute` delta vocabulary does not apply. `PriceBook` is a sibling, far thinner.

This task implements four of the design's §4 conformance items, each with a named test. The other five are Task 10's (snapshot routing by open group, the cross-instrument buffer overflow policy, per-arm `EndOfSession`) or already done (`BookClear` scope validation in Task 8, and there is no `ChannelReset 0x05` on this feed at all — a channel reset is signalled by the frame header's `Reset Count`).

**Files:**
- Create: `src/ingest/pricebook.rs`
- Modify: `src/ingest/mod.rs` (`pub mod pricebook;`)
- Test: `src/ingest/pricebook.rs` `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `codec_mbp`'s `SIDE_BID`/`SIDE_ASK`/`CLEAR_SIDE_*`/`SCOPE_*` constants.
- Produces (Task 10 consumes all of it):
  ```rust
  pub enum Status { AwaitingSnapshot, BuildingSnapshot, Ready, Gap }
  pub struct LevelState { pub qty_raw: u64, pub order_count: Option<u16>, pub level_flags: u8 }
  pub enum BookDelta {
      Level { side: u8, price_raw: i64, qty_raw: u64, order_count: Option<u16>, level_flags: u8, action: u8 },
      Clear { clear_side: u8, scope: u8, from_price_raw: i64 },
  }
  pub struct DeltaOp { pub seq: u32, pub mktdata_seq: u64, pub ts: u64, pub delta: BookDelta }
  pub enum Divergence { NewOnPresentLevel, ChangeOnAbsentLevel, DeleteWithQuantity, ZeroQuantityWithoutDelete }
  pub enum DeltaOutcome { Buffered, Duplicate, Gap, Applied { divergence: Option<Divergence> } }
  pub struct PriceBook { /* private */ }
  impl PriceBook {
      pub fn new() -> Self;                            // also `Default`
      pub fn status(&self) -> Status;
      pub fn depth_bound(&self) -> Option<u32>;
      pub fn last_event_ts(&self) -> u64;
      pub fn buffered_len(&self) -> usize;
      pub fn drop_buffer(&mut self);
      pub fn crossed(&self) -> bool;
      pub fn bids(&self) -> impl Iterator<Item = (i64, &LevelState)>;   // descending price
      pub fn asks(&self) -> impl Iterator<Item = (i64, &LevelState)>;   // ascending price
      pub fn on_delta(&mut self, op: DeltaOp) -> DeltaOutcome;
      pub fn on_snapshot_begin(&mut self, snapshot_id: u32, anchor_seq: u64, total_levels: u32, last_instrument_seq: u32, depth_bound: u32) -> bool;
      pub fn on_snapshot_level(&mut self, snapshot_id: u32, side: u8, price_raw: i64, qty_raw: u64, order_count: Option<u16>, level_flags: u8);
      pub fn on_snapshot_end(&mut self, anchor_seq: u64, snapshot_id: u32) -> bool;
      pub fn on_instrument_reset(&mut self, new_anchor_seq: u64);
      pub fn on_end_of_session(&mut self);
  }
  ```

- [x] **Step 1: Branch**

Branch off Task 8's final commit.

- [x] **Step 2: Fetch the state-machine oracle**

```bash
git -C <edge-multicast-ref> pull --ff-only origin main
ls <edge-multicast-ref>/go/marketbyprice-bot
```

`marketbyprice-bot` (merged PR #34) is the reference book engine. Read `instrument.go` — `SnapshotAcceptable`, `BeginSnapshot`/`AddSnapshotLevel`/`EndSnapshot`, `ApplyLevelUpdate`, `ApplyBookClear`, `Crossed` — and mirror its decisions. **Two places to deliberately diverge:** its `reorderWindow = 16` (treating deltas up to 16 ahead as reordering rather than a gap) has no basis in the spec, which says `> last + 1` is a gap, full stop — do not port it. Its shard-level `maxBufferedDeltasPerShard` is a cross-instrument bound; that is Task 10's.

- [x] **Step 3: Write the failing tests**

Create `src/ingest/pricebook.rs` with the test module only.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::codec_mbp::{
        CLEAR_SIDE_ASK, CLEAR_SIDE_BID, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE,
        SIDE_ASK, SIDE_BID,
    };

    /// Action values from the spec's enum: 1=New, 2=Change, 3=Delete, 0=Unknown.
    const NEW: u8 = 1;
    const CHANGE: u8 = 2;
    const DELETE: u8 = 3;
    const UNKNOWN: u8 = 0;

    fn level(seq: u32, mktdata_seq: u64, side: u8, price: i64, qty: u64, action: u8) -> DeltaOp {
        DeltaOp {
            seq,
            mktdata_seq,
            ts: 1_000 + seq as u64,
            delta: BookDelta::Level {
                side,
                price_raw: price,
                qty_raw: qty,
                order_count: Some(1),
                level_flags: 0,
                action,
            },
        }
    }

    fn clear(seq: u32, mktdata_seq: u64, clear_side: u8, scope: u8, from: i64) -> DeltaOp {
        DeltaOp {
            seq,
            mktdata_seq,
            ts: 1_000 + seq as u64,
            delta: BookDelta::Clear { clear_side, scope, from_price_raw: from },
        }
    }

    /// Bring a book to `Ready` at anchor `S`, per-instrument seq `K`, with the given levels.
    fn synced(anchor: u64, k: u32, depth_bound: u32, levels: &[(u8, i64, u64)]) -> PriceBook {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(1, anchor, levels.len() as u32, k, depth_bound));
        for &(side, price, qty) in levels {
            b.on_snapshot_level(1, side, price, qty, Some(1), 0);
        }
        assert!(b.on_snapshot_end(anchor, 1));
        assert_eq!(b.status(), Status::Ready);
        b
    }

    fn bids_of(b: &PriceBook) -> Vec<(i64, u64)> {
        b.bids().map(|(p, l)| (p, l.qty_raw)).collect()
    }

    fn asks_of(b: &PriceBook) -> Vec<(i64, u64)> {
        b.asks().map(|(p, l)| (p, l.qty_raw)).collect()
    }

    // ---- §4.3: depth_bound defaults to unknown, never 0 ----

    /// A never-snapshotted instrument must report depth as UNKNOWN. Defaulting to `0` would make it
    /// assert completeness through our own initialisation rather than through anything the publisher
    /// sent — the exact failure `Depth Bound` exists to prevent.
    #[test]
    fn depth_bound_is_unknown_before_any_snapshot() {
        let b = PriceBook::new();
        assert_eq!(b.depth_bound(), None);
        assert_eq!(b.status(), Status::AwaitingSnapshot);
    }

    /// Wire `0` is a positive publisher claim of a complete book, and is distinct from "no claim".
    #[test]
    fn depth_bound_zero_is_a_claim_of_completeness() {
        let b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert_eq!(b.depth_bound(), Some(0));
    }

    #[test]
    fn depth_bound_nonzero_is_the_declared_bound() {
        let b = synced(100, 5, 25, &[(SIDE_BID, 6200, 150)]);
        assert_eq!(b.depth_bound(), Some(25));
    }

    // ---- §4.4: Action must not gate the apply ----

    /// Apply by quantity alone: `0` removes, else set. Every LevelUpdate states the complete
    /// resulting state of one level, so applying by quantity always produces the correct level
    /// regardless of what `Action` claims. An `Action` byte that is wrong must never corrupt a book.
    #[test]
    fn action_never_gates_the_apply() {
        let mut b = synced(100, 5, 0, &[]);
        // `Delete` on an absent level with a NON-zero quantity: the quantity wins, level is set.
        assert!(matches!(
            b.on_delta(level(6, 101, SIDE_BID, 6200, 150, DELETE)),
            DeltaOutcome::Applied { .. }
        ));
        assert_eq!(bids_of(&b), vec![(6200, 150)]);
        // `New` on a present level with quantity 0: the quantity wins, level is removed.
        assert!(matches!(
            b.on_delta(level(7, 102, SIDE_BID, 6200, 0, NEW)),
            DeltaOutcome::Applied { .. }
        ));
        assert!(bids_of(&b).is_empty());
    }

    /// ...but the disagreements are counted, so a publisher defect is visible without changing the
    /// applied result.
    #[test]
    fn action_disagreements_are_reported_as_divergence() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        let cases = [
            (level(6, 101, SIDE_BID, 6200, 200, NEW), Divergence::NewOnPresentLevel),
            (level(7, 102, SIDE_BID, 6100, 50, CHANGE), Divergence::ChangeOnAbsentLevel),
            (level(8, 103, SIDE_BID, 6100, 50, DELETE), Divergence::DeleteWithQuantity),
            (level(9, 104, SIDE_BID, 6100, 0, UNKNOWN), Divergence::ZeroQuantityWithoutDelete),
        ];
        for (op, want) in cases {
            match b.on_delta(op) {
                DeltaOutcome::Applied { divergence: Some(got) } => assert_eq!(got, want),
                other => panic!("expected divergence {want:?}, got {other:?}"),
            }
        }
    }

    /// A correct delete (`Action = 3`, quantity 0) is not a divergence.
    #[test]
    fn a_correct_delete_is_not_divergence() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(matches!(
            b.on_delta(level(6, 101, SIDE_BID, 6200, 0, DELETE)),
            DeltaOutcome::Applied { divergence: None }
        ));
    }

    // ---- §4.8: per_instrument_seq classification, dense, no reset at snapshots ----

    #[test]
    fn contiguous_deltas_apply_and_a_gap_buffers() {
        let mut b = synced(100, 5, 0, &[]);
        assert!(matches!(b.on_delta(level(6, 101, SIDE_BID, 6200, 10, NEW)), DeltaOutcome::Applied { .. }));
        // seq <= last is a duplicate or late arrival: discard silently.
        assert!(matches!(b.on_delta(level(6, 101, SIDE_BID, 6200, 99, NEW)), DeltaOutcome::Duplicate));
        assert!(matches!(b.on_delta(level(5, 100, SIDE_BID, 6200, 99, NEW)), DeltaOutcome::Duplicate));
        assert_eq!(bids_of(&b), vec![(6200, 10)], "neither duplicate applied");
        // A forward gap marks the instrument and buffers.
        assert!(matches!(b.on_delta(level(9, 104, SIDE_BID, 6100, 20, NEW)), DeltaOutcome::Gap));
        assert_eq!(b.status(), Status::Gap);
        assert!(matches!(b.on_delta(level(10, 105, SIDE_BID, 6000, 30, NEW)), DeltaOutcome::Buffered));
        assert_eq!(b.buffered_len(), 2, "the gap delta and the next are both held");
    }

    /// The counter is monotonic within the reset-count era and does NOT restart at snapshot
    /// boundaries: if it did, a subscriber that missed a snapshot but saw `seq = 1` could not tell a
    /// fresh post-snapshot delta from a late duplicate of an old one.
    #[test]
    fn a_snapshot_does_not_reset_the_per_instrument_seq() {
        let mut b = synced(100, 5, 0, &[]);
        b.on_delta(level(6, 101, SIDE_BID, 6200, 10, NEW));
        // A later snapshot at K = 20 re-baselines; seq 21 must be next, not 1.
        assert!(b.on_snapshot_begin(2, 200, 0, 20, 0));
        assert!(b.on_snapshot_end(200, 2));
        assert!(matches!(b.on_delta(level(1, 201, SIDE_BID, 6200, 10, NEW)), DeltaOutcome::Duplicate));
        assert!(matches!(b.on_delta(level(21, 201, SIDE_BID, 6200, 10, NEW)), DeltaOutcome::Applied { .. }));
    }

    // ---- §4.2: the snapshot-while-Ready discriminator is Last Instrument Seq ----

    /// `K > last_applied_instrument_seq` means we are genuinely behind — deltas were applied before
    /// the capture that we never saw — so re-bootstrap.
    #[test]
    fn snapshot_while_ready_rebuilds_when_last_instrument_seq_is_ahead() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(b.on_snapshot_begin(2, 500, 1, 9, 0), "K=9 > 5, we are behind");
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(b.on_snapshot_end(500, 2));
        assert_eq!(asks_of(&b), vec![(6300, 77)]);
        assert!(bids_of(&b).is_empty(), "the snapshot REPLACES the book");
    }

    /// `K <= last_applied_instrument_seq` is the ordinary case: deltas routinely arrive between the
    /// publisher's capture and the snapshot's delivery. Ignore it — do not rebuild.
    #[test]
    fn snapshot_while_ready_is_ignored_when_current() {
        let mut b = synced(100, 9, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(!b.on_snapshot_begin(2, 500, 1, 5, 0), "K=5 <= 9, we are current");
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(!b.on_snapshot_end(500, 2));
        assert_eq!(bids_of(&b), vec![(6200, 150)], "healthy book untouched");
        assert!(asks_of(&b).is_empty());
        assert_eq!(b.status(), Status::Ready);
    }

    /// **The trap this test exists for.** `Anchor Seq` is a channel-wide mktdata sequence while
    /// `last_applied_mktdata_seq` advances only on this instrument's own deltas — every frame for
    /// every other instrument, and every heartbeat, moves one and not the other. Comparing them
    /// makes "we are behind" true for nearly every instrument on nearly every rotation, so a
    /// subscriber would discard and rebuild a perfectly good book every cycle.
    #[test]
    fn anchor_seq_is_not_the_discriminator() {
        // Anchor is far ahead (busy channel) but K is behind (this instrument is current).
        let mut b = synced(100, 9, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            !b.on_snapshot_begin(2, 9_999_999, 1, 9, 0),
            "a huge anchor with K == ours must NOT trigger a rebuild"
        );
        assert_eq!(bids_of(&b), vec![(6200, 150)]);
    }

    // ---- Snapshot assembly integrity ----

    /// A `SnapshotLevel` whose id does not match the open `SnapshotBegin` is discarded.
    #[test]
    fn snapshot_level_with_a_mismatched_id_is_discarded() {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(7, 100, 1, 0, 0));
        b.on_snapshot_level(8, SIDE_BID, 6200, 150, Some(1), 0); // wrong id
        assert!(!b.on_snapshot_end(100, 7), "level count short of total_levels");
        assert_eq!(b.status(), Status::AwaitingSnapshot);
    }

    /// A count that does not equal `total_levels`, or a mismatched anchor/id on the end, discards
    /// the partial book — guarding against installing a snapshot truncated by packet loss.
    #[test]
    fn snapshot_end_rejects_incomplete_or_mismatched_groups() {
        for (levels, anchor, id) in [(0u32, 100u64, 7u32), (1, 999, 7), (1, 100, 8)] {
            let mut b = PriceBook::new();
            assert!(b.on_snapshot_begin(7, 100, 1, 0, 0));
            for _ in 0..levels {
                b.on_snapshot_level(7, SIDE_BID, 6200, 150, Some(1), 0);
            }
            assert!(!b.on_snapshot_end(anchor, id));
            assert_eq!(b.status(), Status::AwaitingSnapshot);
        }
    }

    /// Publishers SHOULD emit levels best-to-worst, but subscribers MUST NOT depend on it: the
    /// levels of a group are a set, and our own sorted container establishes rank.
    #[test]
    fn snapshot_level_order_does_not_matter() {
        let b = synced(100, 0, 0, &[(SIDE_BID, 6100, 10), (SIDE_BID, 6200, 20), (SIDE_ASK, 6400, 40), (SIDE_ASK, 6300, 30)]);
        assert_eq!(bids_of(&b), vec![(6200, 20), (6100, 10)], "bids descend");
        assert_eq!(asks_of(&b), vec![(6300, 30), (6400, 40)], "asks ascend");
    }

    /// Buffered deltas at/below the anchor are already in the snapshot and are dropped; those past
    /// it replay in mktdata-seq order.
    #[test]
    fn buffered_deltas_replay_past_the_anchor() {
        let mut b = PriceBook::new();
        b.on_delta(level(3, 98, SIDE_BID, 6000, 1, NEW)); // <= anchor, dropped
        b.on_delta(level(6, 101, SIDE_BID, 6100, 20, NEW));
        b.on_delta(level(7, 102, SIDE_BID, 6200, 30, NEW));
        assert_eq!(b.buffered_len(), 3);
        assert!(b.on_snapshot_begin(1, 100, 1, 5, 0));
        b.on_snapshot_level(1, SIDE_ASK, 6300, 99, Some(1), 0);
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(b.status(), Status::Ready);
        assert_eq!(bids_of(&b), vec![(6200, 30), (6100, 20)], "both post-anchor deltas replayed");
        assert_eq!(b.buffered_len(), 0);
    }

    /// A duplicate inside the replay must not cost a re-bootstrap, but a genuine forward gap must.
    #[test]
    fn a_gap_in_the_replay_reverts_to_awaiting_snapshot() {
        let mut b = PriceBook::new();
        b.on_delta(level(6, 101, SIDE_BID, 6100, 20, NEW));
        b.on_delta(level(9, 104, SIDE_BID, 6200, 30, NEW)); // gap: 7, 8 missing
        assert!(b.on_snapshot_begin(1, 100, 0, 5, 0));
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(b.status(), Status::Gap);
    }

    // ---- BookClear ----

    /// `Scope = 0` clears the entire side(s) and `From Price` is ignored. A subscriber that applies
    /// a clear stays `Ready` — it is not a resynchronization signal.
    #[test]
    fn clear_entire_side_stays_ready() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_BID, 6100, 20), (SIDE_ASK, 6300, 30)]);
        assert!(matches!(b.on_delta(clear(6, 101, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 9_999)), DeltaOutcome::Applied { .. }));
        assert!(bids_of(&b).is_empty());
        assert_eq!(asks_of(&b), vec![(6300, 30)]);
        assert_eq!(b.status(), Status::Ready);
    }

    #[test]
    fn clear_both_sides_empties_the_book() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_ASK, 6300, 30)]);
        b.on_delta(clear(6, 101, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, 0));
        assert!(bids_of(&b).is_empty() && asks_of(&b).is_empty());
    }

    /// `Scope = 1` clears from `From Price` outward: for bids every level at or BELOW it, for asks
    /// every level at or ABOVE it. Inclusive.
    #[test]
    fn clear_from_price_clears_outward_inclusively() {
        let mut b = synced(100, 5, 0, &[
            (SIDE_BID, 6200, 10), (SIDE_BID, 6100, 20), (SIDE_BID, 6000, 30),
            (SIDE_ASK, 6300, 40), (SIDE_ASK, 6400, 50), (SIDE_ASK, 6500, 60),
        ]);
        b.on_delta(clear(6, 101, CLEAR_SIDE_BID, SCOPE_FROM_PRICE, 6100));
        assert_eq!(bids_of(&b), vec![(6200, 10)], "6100 and 6000 gone");
        b.on_delta(clear(7, 102, CLEAR_SIDE_ASK, SCOPE_FROM_PRICE, 6400));
        assert_eq!(asks_of(&b), vec![(6300, 40)], "6400 and 6500 gone");
    }

    /// A clear shares the delta sequence with level updates — both mutate the book and their
    /// relative order is significant — so it is classified identically.
    #[test]
    fn clear_shares_the_delta_sequence() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        assert!(matches!(b.on_delta(clear(5, 100, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 0)), DeltaOutcome::Duplicate));
        assert_eq!(bids_of(&b), vec![(6200, 10)]);
        assert!(matches!(b.on_delta(clear(8, 103, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 0)), DeltaOutcome::Gap));
    }

    // ---- InstrumentReset ----

    /// Discard the book and any open snapshot, drop buffered deltas at/below `S'`, and await a
    /// snapshot anchored at `S'` or newer — discarding any older one.
    #[test]
    fn instrument_reset_requires_an_anchor_at_or_past_the_new_one() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        b.on_delta(level(6, 101, SIDE_BID, 6100, 20, NEW));
        b.on_instrument_reset(500);
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty());
        assert_eq!(b.buffered_len(), 0, "buffered deltas at/below S' discarded");
        assert!(!b.on_snapshot_begin(2, 499, 0, 9, 0), "older than S' -> discarded");
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(b.on_snapshot_begin(3, 501, 0, 9, 0), "at or past S' -> accepted");
    }

    /// The required anchor clears on ANY accepted snapshot at or past `S'`, not only an exact match
    /// — clearing only on equality would leave it set permanently when the publisher's next
    /// snapshot lands past the reset's anchor.
    #[test]
    fn required_anchor_clears_on_any_snapshot_at_or_past_it() {
        let mut b = synced(100, 5, 0, &[]);
        b.on_instrument_reset(500);
        assert!(b.on_snapshot_begin(2, 700, 0, 9, 0));
        assert!(b.on_snapshot_end(700, 2));
        assert_eq!(b.status(), Status::Ready);
        // A subsequent older snapshot is now judged by the ordinary Ready rule, not the anchor.
        assert!(!b.on_snapshot_begin(3, 600, 0, 9, 0), "K == ours -> ignored, not anchor-blocked");
    }

    // ---- EndOfSession ----

    /// The session's book, sequences and event clock are all over. Unlike a reset there is no
    /// forward anchor, so buffered deltas belong to the ended session and are discarded outright.
    /// Zeroing the event clock keeps the resync from stamping its first output with pre-session
    /// time.
    #[test]
    fn end_of_session_drops_everything_including_the_event_clock() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        b.on_delta(level(6, 101, SIDE_BID, 6100, 20, NEW));
        assert!(b.last_event_ts() > 0);
        b.on_end_of_session();
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty());
        assert_eq!(b.buffered_len(), 0);
        assert_eq!(b.last_event_ts(), 0);
        assert_eq!(b.depth_bound(), None, "no publisher claim survives the session");
        // A new-session snapshot with a restarted (small) anchor re-bootstraps cleanly.
        assert!(b.on_snapshot_begin(1, 3, 0, 1, 0));
    }

    // ---- Crossed-book monitoring ----

    /// Observability only: it must not change status, discard the book, or trigger a rebuild.
    /// Strict `>`, so a locked book (equal inside prices, routine on some venues) is not crossed.
    #[test]
    fn crossed_is_observability_and_strict() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_ASK, 6300, 20)]);
        assert!(!b.crossed());
        b.on_delta(level(6, 101, SIDE_ASK, 6200, 5, NEW)); // locked
        assert!(!b.crossed(), "locked is not crossed");
        b.on_delta(level(7, 102, SIDE_ASK, 6100, 5, NEW)); // crossed
        assert!(b.crossed());
        assert_eq!(b.status(), Status::Ready, "monitoring never changes status");
    }

    #[test]
    fn crossed_is_false_when_a_side_is_empty() {
        let b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        assert!(!b.crossed());
    }

    // ---- Buffer bound ----

    /// The per-instrument buffer is bounded. Excess deltas are dropped, not grown — the book
    /// re-anchors on the next snapshot regardless of which buffered deltas survived.
    #[test]
    fn buffered_deltas_are_bounded() {
        let mut b = PriceBook::new();
        for i in 1..=(MAX_BUFFERED_DELTAS as u32 + 100) {
            b.on_delta(level(i, i as u64, SIDE_BID, 6200, 1, NEW));
        }
        assert_eq!(b.buffered_len(), MAX_BUFFERED_DELTAS);
    }

    /// `drop_buffer` is the action behind the cross-instrument overflow policy: the instrument
    /// holding the most buffered data is dropped and marked `Gap`, recovering on its next snapshot
    /// exactly as any other `Gap` instrument does.
    #[test]
    fn drop_buffer_marks_the_instrument_gap() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        b.on_delta(level(9, 104, SIDE_BID, 6100, 20, NEW)); // gap
        assert!(b.buffered_len() > 0);
        b.drop_buffer();
        assert_eq!(b.buffered_len(), 0);
        assert_eq!(b.status(), Status::Gap);
    }
}
```

- [x] **Step 4: Run to verify failure**

Add `pub mod pricebook;` to `src/ingest/mod.rs`, then:

```bash
cargo test --lib pricebook
```

Expected: FAIL to compile — `cannot find type 'PriceBook' in this scope`.

- [x] **Step 5: Implement `PriceBook`**

Write the implementation against the tests. Every public item and rule is fixed by **Interfaces** and the tests above; these are the decisions the tests do not spell out in code:

- **State layout.** `bids: BTreeMap<i64, LevelState>` and `asks: BTreeMap<i64, LevelState>`. `bids()` iterates `.rev()` (descending), `asks()` forward — so the first element of each is the inside market and rank comes from our own container, not the wire.
- **Trackers.** `status: Status`, `last_applied_mktdata_seq: u64`, `last_applied_instrument_seq: u32`, `depth_bound: Option<u32>`, `required_anchor_seq: Option<u64>`, `last_event_ts: u64`, `open: Option<Building>`, `pending: Vec<DeltaOp>`.
- **`Building`** mirrors `book.rs`'s: `snapshot_id`, `anchor_seq`, `total_levels`, `received_levels`, `last_instrument_seq`, `depth_bound`, plus its own `bids`/`asks` maps. Assemble into the shadow and commit only on a valid `SnapshotEnd`, so a failed group leaves a `Ready` book untouched.
- **`on_snapshot_begin` returns whether the group was opened.** It returns `false` — and clears any half-built group — when `required_anchor_seq` is `Some(s)` and `anchor_seq < s`, or when `status == Ready` and `last_instrument_seq <= self.last_applied_instrument_seq`. Otherwise it opens the group and sets `status = BuildingSnapshot`. Note this compares `last_instrument_seq`, never `anchor_seq` — the trap `anchor_seq_is_not_the_discriminator` pins.
- **`on_snapshot_end` returns whether the snapshot installed.** On any mismatch (`snapshot_id`, `anchor_seq`, or `received_levels != total_levels`) discard the shadow and set `status = AwaitingSnapshot`. On success: install the levels, set `last_applied_mktdata_seq = anchor_seq`, `last_applied_instrument_seq = building.last_instrument_seq`, `depth_bound = Some(building.depth_bound)`, clear `required_anchor_seq`, `status = Ready`, then drop buffered deltas at/below the anchor, sort the rest by `mktdata_seq`, and replay them through the same classification as steady state — stopping and setting `status = Gap` on a genuine forward gap, ignoring duplicates.
- **`on_delta`.** When not `Ready`, buffer (bounded by `MAX_BUFFERED_DELTAS`, `1 << 18`, matching `book.rs`'s `MAX_PENDING_DELTAS`) and return `Buffered`. When `Ready`: `seq <= last` → `Duplicate`; `seq > last + 1` → set `status = Gap`, clear the buffer, push this op, return `Gap`; else advance all three trackers, apply, and return `Applied { divergence }`.
- **Applying a `Level`.** `qty_raw == 0` removes the price from the side; otherwise `insert` the `LevelState`. Compute `divergence` from the *pre-apply* presence of the price: `action == 1 && present` → `NewOnPresentLevel`; `action == 2 && !present` → `ChangeOnAbsentLevel`; `action == 3 && qty != 0` → `DeleteWithQuantity`; `qty == 0 && action != 3` → `ZeroQuantityWithoutDelete`. Report at most one; none of them changes the applied result.
- **Applying a `Clear`.** `SCOPE_ENTIRE_SIDE` clears the named side(s) wholesale and ignores `from_price_raw`. `SCOPE_FROM_PRICE` on bids retains `> from`, on asks retains `< from` (inclusive removal). `CLEAR_SIDE_BOTH` with `SCOPE_FROM_PRICE` cannot reach here — Task 8's decoder drops it — so treat it as clearing nothing and leave a `debug_assert!`.
- **`crossed()`** is `bids.last_key_value() > asks.first_key_value()` on price, strict `>`, false when either side is empty.

Mark `MAX_BUFFERED_DELTAS` `pub(crate)` so the test module and Task 10 both see it.

- [x] **Step 6: Run the tests to verify they pass**

```bash
cargo test --lib pricebook
```

Expected: all twenty-four PASS.

- [x] **Step 7: Full suite, clippy, commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "feat(ingest): add the price-keyed book with snapshot and delta recovery"
```

---

## Task 10: the incremental `book` wire message

**Why before the processor:** the wire type is the leaf dependency — the processor emits it and the arbiter routes it, so it lands first and both later tasks build on a fixed shape.

**The shape is dictated by what the reference consumer takes as input**, verified against `nautechsystems/nautilus_trader` @ `05b709b` (v2.0.0rc3). Three facts force it:

- **A boolean "snapshot" flag does not re-baseline an L2 book.** For `L2_MBP`/`L3_MBO` the dispatcher branches on `action` alone; `flags` is read only by the pre-processor, which consults the top-of-book and MBP bits and never the snapshot bit. **Only a clear action re-baselines.** So a re-baseline is not a separate message type here — it is a batch whose first change is `clear`. A `snapshot` boolean would have been silently ineffective.
- **A last-of-batch marker is mandatory** on the final delta of every batch, including a lone clear on an empty book. Omitting it wedges a buffering consumer permanently.
- **Batch-level `flags`/`sequence`/`ts_event` are not settable** — they are copied from the batch's last delta. So our per-batch fields must be legible from the final entry, which is why `last` rides the message rather than a wrapper.

**Files:**
- Modify: `src/model.rs` (`NormalizedBook`, `BookChange`, `BookAction`, `BookSide`, `FeedMessage::Book`, `BookSnapshot`)
- Modify: `src/sinks/ws.rs` (`prepare`'s `Book` arm, populate `PreparedFrame.channel`)
- Modify: `src/ingest/arbiter.rs` (a temporary passthrough `Book` arm, replaced in Task 12)
- Modify: `PROTOCOL.md`, `CHANGELOG.md`
- Test: `src/model.rs`, `src/sinks/ws.rs`

**Interfaces:**
- Consumes: Task 6's `PreparedFrame.channel`.
- Produces:
  ```rust
  pub enum BookAction { Clear, Update, Delete }              // serde lowercase
  pub enum BookSide { Bid, Ask, Both }                       // serde lowercase
  pub struct BookChange { pub action: BookAction, pub side: BookSide, pub price: f64, pub size: f64 }
  pub struct NormalizedBook {
      pub venue: Arc<str>, pub symbol: Arc<str>, pub channel: u32, pub instrument_id: u32,
      pub changes: Vec<BookChange>, pub snapshot: bool, pub last: bool,
      pub source_ts_ns: u64, pub recv_ts_ns: u64, pub kernel_rx_ts_ns: u64, pub ws_send_ts_ns: u64,
  }
  pub enum FeedMessage { /* ... */ Book(NormalizedBook) }
  pub struct BookAccumulator { /* private */ }
  impl BookAccumulator {
      pub fn new(symbol: Arc<str>) -> Self;
      pub fn apply(&mut self, b: &NormalizedBook);
      pub fn to_book(&self, venue: &Arc<str>, channel: u32, instrument_id: u32) -> NormalizedBook;
  }
  /// Accumulated book state per `(venue, channel, instrument_id)`, for connect/subscribe replay.
  pub type BookSnapshot = Arc<Mutex<HashMap<(Arc<str>, u32, u32), BookAccumulator>>>;
  ```
  **Why an accumulator and not the last message.** `depth` is full state, so storing the last one is a valid replay. `book` is incremental, so the last batch is meaningless to a client that holds nothing. The map therefore accumulates — the same operation a consumer performs — and materializes a `clear` plus the complete level set on demand. Cost is one book per market for the authoritative arm only, updated in O(changes) per batch rather than O(book). That is the honest price of offering connect-time bootstrap for an incremental product; the alternative is making every new client wait a full snapshot cycle.
  `FeedMessage::venue_symbol` gains a `Book` arm; a new `FeedMessage::channel(&self) -> Option<u32>` returns `Some` only for `Book`.

- [x] **Step 1: Branch**

Branch off Task 9's final commit.

- [x] **Step 2: Write the failing tests**

Add to `src/model.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn book(changes: Vec<BookChange>, snapshot: bool, last: bool) -> NormalizedBook {
        NormalizedBook {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            changes,
            snapshot,
            last,
            source_ts_ns: 1_781_019_263_715_344_015,
            recv_ts_ns: 1_781_019_263_715_501_230,
            kernel_rx_ts_ns: 1_781_019_263_715_300_010,
            ws_send_ts_ns: 0,
        }
    }

    /// The wire shape PROTOCOL.md documents, pinned exactly — field names and the `type` tag are the
    /// contract, so a rename is a breaking change a test must catch.
    #[test]
    fn book_serializes_to_the_documented_shape() {
        let m = FeedMessage::Book(book(
            vec![
                BookChange { action: BookAction::Update, side: BookSide::Bid, price: 0.62, size: 150.0 },
                BookChange { action: BookAction::Delete, side: BookSide::Ask, price: 0.63, size: 0.0 },
            ],
            false,
            true,
        ));
        let v: serde_json::Value = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "book");
        assert_eq!(v["venue"], "Lashay");
        assert_eq!(v["symbol"], "KXBTCPERP");
        assert_eq!(v["channel"], 2);
        assert_eq!(v["instrument_id"], 41);
        assert_eq!(v["snapshot"], false);
        assert_eq!(v["last"], true);
        assert_eq!(v["changes"][0]["action"], "update");
        assert_eq!(v["changes"][0]["side"], "bid");
        assert_eq!(v["changes"][0]["price"], 0.62);
        assert_eq!(v["changes"][0]["size"], 150.0);
        assert_eq!(v["changes"][1]["action"], "delete");
        assert_eq!(v["changes"][1]["side"], "ask");
        assert!(v["source_ts_ns"].is_u64() && v["kernel_rx_ts_ns"].is_u64());
    }

    /// A re-baseline is structural: `changes[0].action == "clear"`, because the reference consumer's
    /// book dispatcher branches on the action alone and ignores any snapshot flag. `snapshot: true`
    /// is advisory only, so a consumer must be able to re-baseline from the clear with the flag
    /// stripped.
    #[test]
    fn a_rebaseline_leads_with_a_clear_action() {
        let m = book(
            vec![
                BookChange { action: BookAction::Clear, side: BookSide::Both, price: 0.0, size: 0.0 },
                BookChange { action: BookAction::Update, side: BookSide::Bid, price: 0.62, size: 150.0 },
            ],
            true,
            true,
        );
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["changes"][0]["action"], "clear");
        assert_eq!(v["changes"][0]["side"], "both");
        assert_eq!(v["snapshot"], true);
        assert_eq!(v["last"], true, "mandatory even on a lone clear");
    }

    /// A lone clear on an empty book is a legal message and must still carry `last: true` — omitting
    /// it wedges a buffering consumer permanently.
    #[test]
    fn a_lone_clear_is_a_complete_message() {
        let m = book(
            vec![BookChange { action: BookAction::Clear, side: BookSide::Both, price: 0.0, size: 0.0 }],
            true,
            true,
        );
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["changes"].as_array().unwrap().len(), 1);
        assert_eq!(v["last"], true);
    }

    #[test]
    fn book_round_trips() {
        let m = FeedMessage::Book(book(
            vec![BookChange { action: BookAction::Update, side: BookSide::Ask, price: 0.63, size: 7.5 }],
            false,
            false,
        ));
        let back: FeedMessage = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        let FeedMessage::Book(b) = back else { panic!() };
        assert_eq!(b.channel, 2);
        assert_eq!(b.instrument_id, 41);
        assert!(!b.last);
    }

    /// `channel` is on `book` and nothing else, so the filter's channel dimension excludes every
    /// other type (see the ws filter tests).
    #[test]
    fn only_book_reports_a_channel() {
        let b = FeedMessage::Book(book(vec![], false, true));
        assert_eq!(b.channel(), Some(2));
        let q = FeedMessage::Status(FeedStatus {
            venue: "Lashay".into(),
            state: "ok".into(),
            stale_ms: 0,
            ts_ns: 1,
        });
        assert_eq!(q.channel(), None);
    }

    /// The identity triple is what a consumer keys on; `symbol` is a display label, so
    /// `venue_symbol` must still report it for the existing symbol filter to work.
    #[test]
    fn book_reports_its_venue_and_symbol_for_filtering() {
        let b = FeedMessage::Book(book(vec![], false, true));
        assert_eq!(b.venue_symbol(), ("Lashay", "KXBTCPERP"));
    }
}
```

Add to `mod tests` in `src/sinks/ws.rs`:

```rust
    /// A `book` frame must carry its channel so an explicit channel filter can select it.
    #[test]
    fn prepare_populates_the_channel_for_book_only() {
        use crate::model::{BookAction, BookChange, BookSide, NormalizedBook};
        let b = FeedMessage::Book(NormalizedBook {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            channel: 2,
            instrument_id: 41,
            changes: vec![BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price: 0.62,
                size: 150.0,
            }],
            snapshot: false,
            last: true,
            source_ts_ns: 1,
            recv_ts_ns: 2,
            kernel_rx_ts_ns: 3,
            ws_send_ts_ns: 0,
        });
        let f = prepare(&b).expect("serializes");
        assert_eq!(f.kind, "book");
        assert_eq!(f.channel, Some(2));
        assert!(f.payload.contains(r#""ws_send_ts_ns":"#));
        assert!(!f.payload.contains(r#""ws_send_ts_ns":0"#), "stamped, not left at 0");
    }
```

- [x] **Step 3: Run to verify failure**

```bash
cargo test --lib model:: sinks::ws::tests::prepare_populates
```

Expected: FAIL to compile — `cannot find struct 'NormalizedBook'`.

- [x] **Step 4: Add the wire types**

In `src/model.rs`, after `NormalizedDepth`:

```rust
/// What one entry of a [`NormalizedBook`] batch does to the consumer's book.
///
/// **A `Clear` is the only thing that re-baselines**, which is why a re-baseline is a batch led by
/// one rather than a separate message type: the reference consumer's book dispatcher branches on
/// this action alone and never reads a snapshot flag, so a boolean "this is a snapshot" field would
/// be silently ineffective there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookAction {
    /// Discard the named side(s) before applying the rest of the batch.
    Clear,
    /// Set the level at `price` to `size` (an absolute quantity, not a delta).
    Update,
    /// Remove the level at `price`. `size` is `0`.
    Delete,
}

/// Which side of the book a [`BookChange`] touches. `Both` occurs only on a `Clear`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    Bid,
    Ask,
    Both,
}

/// One price-level change. `size` is the level's **absolute** resulting quantity, never a delta, so
/// a consumer that misses nothing needs no arithmetic: set it, or remove it when the action is
/// `Delete`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookChange {
    pub action: BookAction,
    pub side: BookSide,
    pub price: f64,
    pub size: f64,
}

/// A batch of price-level changes for one instrument — the incremental order-book product, derived
/// in the bridge from the Market-by-Price feed's snapshot+delta stream.
///
/// **`(venue, channel, instrument_id)` is the identity; `symbol` is a display label.** The wire
/// `symbol` is a fixed 16-byte field the publisher fills by keeping the rightmost 16 bytes of the
/// venue's ticker — silently, with no hash and no length check — so on venues with long tickers
/// distinct markets collide on it and a consumer keying on `symbol` would merge two books.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedBook {
    pub venue: Arc<str>,
    /// Display label. Not unique in general — see the type docs.
    pub symbol: Arc<str>,
    /// The publisher's `channel_id`: the instrument set this feed carries. Filterable.
    pub channel: u32,
    /// Instrument id, unique within `channel`.
    pub instrument_id: u32,
    pub changes: Vec<BookChange>,
    /// Advisory: this batch is part of a rebuild rather than ordinary activity. Deliberately NOT
    /// what re-baselines a consumer — `changes[0].action == Clear` is.
    pub snapshot: bool,
    /// The final batch of a logical book event. **Mandatory** — a buffering consumer wedges
    /// permanently without it, including on a re-baseline that is only a clear.
    pub last: bool,
    /// Timestamp of the latest applied book event (ns since epoch), 0 if unknown.
    pub source_ts_ns: u64,
    /// When the bridge produced this batch (user-space wall clock, ns since epoch).
    pub recv_ts_ns: u64,
    /// Kernel software RX timestamp from `SO_TIMESTAMPNS` (CLOCK_REALTIME ns), 0 when unavailable.
    #[serde(default)]
    pub kernel_rx_ts_ns: u64,
    /// Wall clock (ns since epoch) stamped by the WS server just before send; 0 until stamped.
    #[serde(default)]
    pub ws_send_ts_ns: u64,
}
```

Add `Book(NormalizedBook)` to `enum FeedMessage`, a `Book` arm to `venue_symbol`, and:

```rust
    /// The `channel_id` this message is about, for per-channel subscription filtering. Only the
    /// incremental `book` product carries one; every other type is venue/symbol-scoped.
    pub fn channel(&self) -> Option<u32> {
        match self {
            FeedMessage::Book(b) => Some(b.channel),
            _ => None,
        }
    }
```

and the replay accumulator next to `DepthSnapshot`:

```rust
/// Accumulated book state for one market, so a connecting or newly-subscribing client can be
/// bootstrapped immediately instead of waiting a full snapshot cycle.
///
/// `depth` is full state, so its replay map stores the last message. `book` is incremental, so the
/// last batch tells a fresh client nothing — this performs the same accumulation a consumer does and
/// materializes a `clear` plus the complete level set on demand. Levels are keyed by the price
/// canonicalized to a `10^-8` fixed-point integer, because `f64` is not `Ord`; the original `f64` is
/// kept alongside so replayed prices are byte-identical to the streamed ones.
#[derive(Debug, Clone)]
pub struct BookAccumulator {
    symbol: Arc<str>,
    bids: std::collections::BTreeMap<i128, (f64, f64)>,
    asks: std::collections::BTreeMap<i128, (f64, f64)>,
    source_ts_ns: u64,
}

impl BookAccumulator {
    pub fn new(symbol: Arc<str>) -> Self {
        Self {
            symbol,
            bids: std::collections::BTreeMap::new(),
            asks: std::collections::BTreeMap::new(),
            source_ts_ns: 0,
        }
    }

    /// Apply one broadcast batch, in wire order.
    pub fn apply(&mut self, b: &NormalizedBook) {
        self.symbol = b.symbol.clone();
        self.source_ts_ns = b.source_ts_ns;
        for c in &b.changes {
            let key = (c.price * 10f64.powi(8)).round() as i128;
            match (c.action, c.side) {
                (BookAction::Clear, BookSide::Bid) => self.bids.clear(),
                (BookAction::Clear, BookSide::Ask) => self.asks.clear(),
                (BookAction::Clear, BookSide::Both) => {
                    self.bids.clear();
                    self.asks.clear();
                }
                (BookAction::Delete, BookSide::Bid) => {
                    self.bids.remove(&key);
                }
                (BookAction::Delete, BookSide::Ask) => {
                    self.asks.remove(&key);
                }
                (BookAction::Update, BookSide::Bid) => {
                    self.bids.insert(key, (c.price, c.size));
                }
                (BookAction::Update, BookSide::Ask) => {
                    self.asks.insert(key, (c.price, c.size));
                }
                // `Both` is only ever a clear; a delete/update on it is a producer bug, not a
                // consumer-visible state, so ignore it rather than guessing a side.
                (_, BookSide::Both) => {}
            }
        }
    }

    /// Materialize the current state as a re-baseline: `clear` first, then every level best-first.
    pub fn to_book(&self, venue: &Arc<str>, channel: u32, instrument_id: u32) -> NormalizedBook {
        let mut changes = Vec::with_capacity(self.bids.len() + self.asks.len() + 1);
        changes.push(BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
        });
        // Bids descend, asks ascend, so the first of each is the inside market.
        for &(price, size) in self.bids.values().rev() {
            changes.push(BookChange { action: BookAction::Update, side: BookSide::Bid, price, size });
        }
        for &(price, size) in self.asks.values() {
            changes.push(BookChange { action: BookAction::Update, side: BookSide::Ask, price, size });
        }
        NormalizedBook {
            venue: venue.clone(),
            symbol: self.symbol.clone(),
            channel,
            instrument_id,
            changes,
            snapshot: true,
            last: true,
            source_ts_ns: self.source_ts_ns,
            recv_ts_ns: now_ns(),
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }
}

/// Accumulated book state per `(venue, channel, instrument_id)`, replayed on connect and on each
/// subscribe. Written by the arbiter on the authority gate's admit decision, so it always holds the
/// authoritative arm's book rather than a discarded arm's copy.
pub type BookSnapshot = Arc<Mutex<HashMap<(Arc<str>, u32, u32), BookAccumulator>>>;
```

Add a test asserting `apply` then `to_book` round-trips: a `clear` + two bids + one ask, then an `update` moving one bid and a `delete` removing the ask, must materialize as `clear` + two bids descending + no asks.

- [x] **Step 5: Wire `book` through the WS serializer**

In `src/sinks/ws.rs`'s `prepare`, add to the `kind` match:

```rust
        FeedMessage::Book(b) => {
            b.ws_send_ts_ns = now;
            "book"
        }
```

and extend the `(venue, symbol, channel)` destructuring so `Book` yields `Some(b.channel)` while every other variant yields `None`.

- [x] **Step 6: Add a temporary arbiter passthrough**

`emit`'s match is exhaustive by design (a new variant must be a compile error, not a silent miss). Add, with the comment that says exactly what replaces it:

```rust
            // TEMPORARY: `book` is passed through undeduped so the processor can land before the
            // authority gate. Replaced by the `StickyAuthority` routing in the next change — an
            // incremental stream must be single-arm, and two arms passing through here would
            // interleave unrelated delta series on the wire.
            FeedMessage::Book(b) => {
                self.vm(&b.venue).emit[EMIT_BOOK].inc();
                let _ = self.tx.send(Arc::new(msg));
            }
```

Add `EMIT_BOOK: usize = 6` and widen `VenueMetrics::emit` to `[IntCounter; 7]` with `emit_kind("book")`.

- [x] **Step 7: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass.

- [x] **Step 8: Document the message in PROTOCOL.md**

Add `book` to the envelope table (`| \`book\` | A batch of incremental order-book level changes. |`) and a full `### book` section after `### depth`:

````markdown
### `book`

```json
{"type":"book","venue":"Lashay","symbol":"KXBTCPERP","channel":2,"instrument_id":41,
 "changes":[{"action":"update","side":"bid","price":0.6200,"size":150},
            {"action":"delete","side":"ask","price":0.6300,"size":0}],
 "snapshot":false,"last":true,
 "source_ts_ns":1781019263715344015,"recv_ts_ns":1781019263715501230,
 "kernel_rx_ts_ns":1781019263715300010,"ws_send_ts_ns":1781019263715600440}
```

A batch of **incremental** price-level changes for one instrument, derived in the producer from the DZ Edge Market-by-Price feed. Unlike `depth`, a `book` message is not full state: apply the changes in order to the book you already hold.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `"book"`. |
| `venue` | string | Venue code. |
| `symbol` | string | **Display label.** Not guaranteed unique — see *Identity* below. |
| `channel` | uint32 | The publisher's channel id: the instrument set this feed carries. Filterable. |
| `instrument_id` | uint32 | Instrument id, unique within `channel`. |
| `changes` | object[] | Level changes, in order. `{ "action", "side", "price", "size" }`. |
| `changes[].action` | string | `"clear"`, `"update"`, or `"delete"`. |
| `changes[].side` | string | `"bid"`, `"ask"`, or `"both"` (`"both"` only on a `clear`). |
| `changes[].price` | number | Price of the level (decimal). Ignored for a `clear`. |
| `changes[].size` | number | The level's **absolute** resulting size (decimal), not a delta. `0` on a `delete`. |
| `snapshot` | bool | Advisory: this batch is part of a rebuild. **Not** what re-baselines you. |
| `last` | bool | This is the final batch of a logical book event. |
| `source_ts_ns` | uint64 | Timestamp of the latest applied book event; `0` if unknown. |
| `recv_ts_ns` | uint64 | When the producer built this batch, ns since epoch. |
| `kernel_rx_ts_ns` | uint64 | Kernel RX timestamp (`SO_TIMESTAMPNS`); `0` if unavailable. |
| `ws_send_ts_ns` | uint64 | Wall clock the instant this batch is serialized; shared by all consumers of this message. `0` if unset. |

**Identity: key on `(venue, channel, instrument_id)`, not on `symbol`.** The upstream `symbol` is a fixed 16-byte field the publisher fills by keeping the ticker's rightmost 16 bytes — silently, with no hash and no length check — so on venues with long tickers distinct markets collide on it, and a consumer keying on `symbol` merges two books into one. `symbol` is for display, and for the convenience of venues where it happens to be unique. `instrument_id` and `channel` also appear on `instrument` messages, so the mapping is learnable on connect.

**Re-baselining is structural: `changes[0].action == "clear"`.** Do **not** key it off `snapshot`. A rebuild (on connect, after a recovery, or when the producer's authoritative source changes) arrives as a `clear` followed by the complete level set, with `snapshot: true` and `last: true` on the final batch. `snapshot` exists only so a consumer can tell a rebuild from ordinary activity; a consumer that ignores it stays correct.

**`last` is mandatory and must be honored.** A consumer that buffers a logical event until its final batch will wait forever if it is dropped — including on a re-baseline whose only change is the `clear`.

**Gap detection is the producer's job.** The producer runs the upstream feed's snapshot+delta recovery internally, per publisher, and re-serves only sequences it has verified as contiguous. There are no sequence numbers on the wire and a consumer needs no gap machinery of its own: a recovery surfaces as a re-baseline.

**One book per market, whichever upstream publisher wins.** Several independent publishers mirror each feed. The producer elects one authoritative publisher per market and republishes only its stream, so a consumer sees one coherent book and never has to merge two. A failover surfaces as a re-baseline.
````

Then, in the *Connection lifecycle* section, add `book` to step 3's list, and note that step 2 replays the latest full-state book per market alongside `depth`. In *Versioning & compatibility*, add `book` to the v1 type list and add a deprecation line:

```markdown
- **`depth` is deprecated.** It is the full-state top-*N* product served from the Market-by-Order feed; `book` supersedes it with the complete book, incrementally. `depth` is removed in v2, when the Market-by-Order feed migrates to `book`. Until then both are served: `book` for Market-by-Price feeds, `depth` for Market-by-Order ones. New consumers should implement `book`.
```

- [x] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(protocol): add the incremental book message"
```

---

## Task 11: `MbpProcessor` and the `MarketByPrice` receiver

**Why:** this is where the remaining §4 conformance items live — the ones that are cross-instrument or cross-publisher and so cannot sit in `PriceBook`. Each is a silent-corruption bug if missed.

- **§4.1 `SnapshotLevel` routes by the open group per channel, never by `snapshot_id`.** `snapshot_id` is monotonic per `(channel_id, instrument_id)`, so two instruments routinely sit at the same value within one rotation. Keying the route on `{channel_id, snapshot_id}` sends one instrument's levels into whichever instrument last claimed that id — a filed bug against the sibling reference bot. `snapshot_id` *validates* membership; the open group *routes*. And `SnapshotLevel` carries no instrument id at all, so there is no fallback.
- **§4.5 The delta buffer needs a cross-instrument bound and a defined overflow policy.** The spec's own worst case is a cold start accumulating on the order of 30 M messages, ~1.4 GB. Policy: drop the buffered deltas of the instrument holding the most, mark it `Gap`, count the event. It recovers on its next snapshot exactly as any other `Gap` instrument. Never let buffer growth take the channel down.
- **§4.7 `EndOfSession` is per-arm.** The existing MBO handler clears every publisher's books *and* the venue's whole shared depth floor. With two arms that means one arm shutting down tears down a live published book. Here it must demote that arm instead: drop its books and report its markets unhealthy, so authority transfers to the peer.
- **§4.9 A channel reset is the frame header's `Reset Count`; there is no `ChannelReset 0x05` on this feed.** Scope the reset to the emitting publisher, and **test for inequality, never ordering** — any change is a reset, including the `255 → 0` wrap.

**Files:**
- Modify: `src/ingest/processor.rs` (`MbpProcessor`)
- Modify: `src/ingest/feeds.rs` (`FeedKind::MarketByPrice`, `label()`)
- Modify: `src/ingest/receiver.rs` (`run_feed` arm)
- Modify: `src/metrics.rs` (`dz_mbp_*` family)
- Modify: `docs/metrics.md`, `docs/input-sources.md`
- Test: `src/ingest/processor.rs`, `src/ingest/feeds.rs`

**Interfaces:**
- Consumes: `codec_mbp` (Tasks 7–8), `PriceBook`/`DeltaOp`/`BookDelta`/`Status`/`DeltaOutcome`/`Divergence` (Task 9), `NormalizedBook`/`BookChange`/`BookAction`/`BookSide` (Task 10), `PerPublisher` (Task 2), `MarketKey` (Task 4).
- Produces: `FeedKind::MarketByPrice` (label `"mbp"`); `pub struct MbpProcessor` with `pub fn new(emit_trades: bool) -> Self`; `FrameCtx.channel_id: u8` (set by `drive` from the decoded frame header — see Step 5).

- [ ] **Step 1: Branch**

Branch off Task 10's final commit.

- [ ] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/processor.rs`. Read the existing MBO processor tests first and reuse their harness (a `FrameCtx` builder over a `SharedArbiter` with a subscribed receiver, so emitted messages can be drained and asserted).

```rust
    /// §4.1 — `SnapshotLevel` carries no instrument id and MUST route by the open group. Two
    /// instruments legitimately share a `snapshot_id` within one rotation (it is monotonic per
    /// `(channel, instrument)`, not per channel), so keying the route on the id sends one
    /// instrument's levels into the other's book.
    #[test]
    fn snapshot_levels_route_by_open_group_not_snapshot_id() {
        // Open instrument 41's group at snapshot_id 5, feed it a level, close it.
        // Open instrument 42's group at the SAME snapshot_id 5, feed a DIFFERENT level, close it.
        // Assert each book holds only its own level.
    }

    /// §4.5 — the cross-instrument buffer is bounded and overflow drops the largest buffer, marks
    /// that instrument `Gap`, and counts the event. It must never take the channel down.
    #[test]
    fn buffer_overflow_drops_the_largest_instrument_and_counts_it() {
        // Buffer deltas for two instruments with no snapshot, one far larger, past the budget.
        // Assert the large one's buffer is empty and `Gap`, the small one's is intact, and
        // `dz_mbp_buffer_overflows_total` incremented.
    }

    /// §4.7 — `EndOfSession` from one arm must drop only that arm's books. Under the previous MBO
    /// handler it also cleared the venue's shared floor, so one arm shutting down tore down the
    /// live published book.
    #[test]
    fn end_of_session_is_scoped_to_the_emitting_arm() {
        // Sync the same instrument for arm A and arm B. Deliver EndOfSession on A's datagram only.
        // Assert A's book is AwaitingSnapshot and B's is still Ready.
    }

    /// §4.9 — a reset is any CHANGE in `Reset Count`, including the 255 -> 0 wrap. Comparing for
    /// ordering (`>`) would silently ignore the wrap and keep applying deltas against discarded
    /// publisher state.
    #[test]
    fn reset_count_wrap_is_a_reset() {
        // Sync at reset_count 255, then deliver a frame at reset_count 0.
        // Assert the book was discarded (AwaitingSnapshot), not retained.
    }

    /// ...and it is scoped to the publisher that reset, per the same rule as reference data.
    #[test]
    fn reset_count_change_is_scoped_to_the_publisher() {
        // Sync for arm A and arm B at reset_count 0. Bump only A's.
        // Assert A dropped and B retained.
    }

    /// A batch of level updates in one frame coalesces into ONE `book` message per instrument, with
    /// `last: true`. Cross-instrument atomicity is not promised, so per-frame batching is correct;
    /// `BatchBoundary` is used only as the crossed-book consistency point.
    #[test]
    fn one_book_message_per_instrument_per_frame() {
        // Two level updates for instrument 41 and one for 42 in a single frame.
        // Assert exactly two `book` messages, 41's carrying both changes, both `last: true`.
    }

    /// A snapshot install re-baselines: `clear` first, then the complete level set, `snapshot: true`
    /// and `last: true`. `changes[0].action == Clear` is what a consumer keys on.
    #[test]
    fn a_snapshot_install_emits_clear_then_the_full_level_set() {
        // Drive begin/levels/end for one instrument, assert the emitted book's shape.
    }

    /// Emission gates per instrument on a known definition — precision before price, the same gate
    /// every other processor applies. A book for an undefined instrument is never even created.
    #[test]
    fn no_book_is_emitted_before_the_instrument_definition() {
        // Deliver a snapshot group with no prior InstrumentDefinition; assert nothing emitted.
    }

    /// Only the wire `symbol` is a label; the identity triple is what rides the message.
    #[test]
    fn emitted_books_carry_the_channel_and_instrument_id() {
        // Assert channel matches the frame header's channel_id and instrument_id the wire field.
    }

    /// Trades are emitted only when the feed row owns them, exactly as the other processors gate.
    #[test]
    fn trades_are_emitted_only_when_the_row_owns_them() {
        // Same Trade frame through `MbpProcessor::new(true)` and `::new(false)`.
    }

    /// A crossed inside market is counted and surfaced, and MUST NOT change status or discard the
    /// book — an instrument holding corrupt state is repaired by its next snapshot on exactly the
    /// schedule it would have been anyway.
    #[test]
    fn a_crossed_book_is_counted_not_acted_on() {
        // Cross the inside market; assert `dz_mbp_crossed_total` incremented and status still Ready.
    }
```

Fill each body against the MBO tests' harness. **Do not leave a test with a comment-only body** — the comments state the assertion; write it.

- [ ] **Step 3: Run to verify failure**

```bash
cargo test --lib processor::tests
```

Expected: FAIL to compile — `cannot find struct 'MbpProcessor'`.

- [ ] **Step 4: Add the feed kind**

In `src/ingest/feeds.rs`, add to `enum FeedKind` and `label()`:

```rust
    /// Market-by-Price (frame magic `0x4442`): the complete price-aggregated book with
    /// snapshot+delta recovery, re-served as the incremental `book` product.
    MarketByPrice,
```

```rust
            FeedKind::MarketByPrice => "mbp",
```

Extend `feed_kind_labels_are_stable` with `assert_eq!(FeedKind::MarketByPrice.label(), "mbp");`.

- [ ] **Step 5: Carry `channel_id` on the frame context**

`MarketKey` and the wire message both need the channel, and it comes from the frame header — which `drive` does not decode (each processor decodes its own). So the processor reads it from its own decode and threads it through, rather than `FrameCtx` gaining a field it cannot fill. **Do not add `channel_id` to `FrameCtx`** — `drive` is protocol-agnostic by design and would have to decode a header it has no magic for. Correct the Interfaces note above accordingly: `MbpProcessor` takes `header.channel_id` from `codec_mbp::decode_frame` and passes it down its own call chain.

- [ ] **Step 6: Implement `MbpProcessor`**

Add to `src/ingest/processor.rs`. Structure, mirroring `MboProcessor` where the shape carries over:

```rust
/// Cap on distinct `(publisher, channel, instrument)` books one MBP receiver tracks. The wire
/// `instrument_id` and the datagram source IP are both unauthenticated and spoofable, so this bounds
/// a forged stream exactly as [`MAX_BOOKS`] does for the order-keyed processor.
const MAX_PRICE_BOOKS: usize = 4096;

/// Total deltas this processor will hold buffered across every book before the overflow policy
/// fires. The spec's own cold-start worst case is ~30 M messages / ~1.4 GB, so an unbounded buffer
/// is a documented way to lose the process. On overflow the instrument holding the most buffered
/// data is dropped and marked `Gap`; it recovers on its next snapshot like any other `Gap`
/// instrument. Sustained overflow means the publisher's snapshot period is too long for this host's
/// memory budget — a tuning signal, which is why it is counted.
const MAX_BUFFERED_DELTAS_TOTAL: usize = 1 << 20;

/// The snapshot group currently open on one `(publisher, channel)`.
///
/// Publishers MUST NOT interleave two groups within a channel, and `SnapshotLevel` carries no
/// instrument id — so the open group is what ROUTES a level. `snapshot_id` only validates
/// membership: it is monotonic per `(channel, instrument)`, so two instruments routinely share a
/// value within one rotation and routing on it would cross their books.
struct OpenGroup {
    instrument_id: u32,
    snapshot_id: u32,
}

/// Market-by-Price processor: drives reference data per publisher, feeds level deltas and the
/// snapshot stream into a [`PriceBook`] per `(publisher, channel, instrument)`, and emits the
/// incremental `book` product plus `trade` prints.
pub struct MbpProcessor {
    state: PerPublisher<codec_mbp::InstrumentDefinition>,
    /// One independent book per `(publisher, channel_id, instrument_id)`. Two arms mirror one feed
    /// but their per-instrument delta series are unrelated, so their books can never be merged —
    /// which arm reaches the wire is the authority gate's decision, downstream.
    books: HashMap<(IpAddr, u8, u32), PriceBook>,
    books_order: VecDeque<(IpAddr, u8, u32)>,
    /// The open snapshot group per `(publisher, channel)` — see [`OpenGroup`].
    open: HashMap<(IpAddr, u8), OpenGroup>,
    /// Last `Reset Count` seen per `(publisher, channel)`. Compared for INEQUALITY, never ordering:
    /// any change is a reset, including the `255 -> 0` wrap.
    last_reset: HashMap<(IpAddr, u8), u8>,
    /// The symbol each book last emitted under, for the authority gate's per-market health and for
    /// replay purges — immune to a manifest epoch remapping the id to another symbol.
    emitted_symbol: HashMap<(IpAddr, u8, u32), Arc<str>>,
    warned_invalid_manifest: bool,
    decode_warn: WarnRateLimit,
    emit_trades: bool,
}
```

Then `on_datagram`, in order:

1. `codec_mbp::decode_frame(buf)`; on error, rate-limited warn and return.
2. **Reset check, per `(publisher, channel)`.** If `last_reset.insert(key, header.reset_count)` returned `Some(prev)` with `prev != header.reset_count`, discard all of that publisher-and-channel's state: remove its books (and their `open` group, `emitted_symbol` entries) and call `self.state_for(ctx.publisher).on_frame(header.reset_count)`. Report every dropped market unhealthy to the authority. Count `dz_mbp_channel_resets_total{venue}`. **`!=`, never `>`.**
3. Walk the messages. Accumulate, per `instrument_id`, a `Vec<BookChange>` in arrival order — a `BTreeMap<u32, Vec<BookChange>>` so multi-instrument frames emit in deterministic ascending id order, matching `MboProcessor`'s `BTreeSet`.
   - `ManifestSummary` / `InstrumentDefinition`: same handling as `MboProcessor`, including the same `Valid=0` override with its own warn-once flag and `REVISIT` comment. Emit `FeedMessage::Instrument`.
   - `LevelUpdate` / `BookClear`: build a `DeltaOp` (`seq` = `per_instrument_seq`, `mktdata_seq` = `header.sequence`, `ts` = the message ts) and call `book.on_delta`. On `Applied`, push the wire change: a level update becomes `Update` when `qty_raw != 0` and `Delete` when `0`; a clear becomes one `Clear` change with the mapped side. Count `Divergence` into `dz_mbp_divergence_total{venue,kind}`.
   - `SnapshotBegin`: set `open[(publisher, channel)] = OpenGroup { instrument_id, snapshot_id }` **only if** `book.on_snapshot_begin(...)` returned true; otherwise remove any open group for that key (the snapshot was refused, so its levels must not route anywhere).
   - `SnapshotLevel`: resolve the instrument from `open[(publisher, channel)]`, then call `book.on_snapshot_level(l.snapshot_id, ...)` — the book itself rejects a mismatched id. No open group means the level is orphaned: drop it and count `dz_mbp_orphan_snapshot_levels_total{venue}`.
   - `SnapshotEnd`: require the open group's `instrument_id` to match `e.instrument_id` (drop and count otherwise), then `book.on_snapshot_end(e.anchor_seq, e.snapshot_id)`. On `true`, clear the open group and emit a **re-baseline** for that instrument: `Clear{side: Both}` followed by every level (`bids()` then `asks()`, scaled), `snapshot: true`, `last: true`. Skip the per-frame accumulator for it — a re-baseline is its own message.
   - `InstrumentReset`: `book.on_instrument_reset(r.new_anchor_seq)`, report that market unhealthy.
   - `EndOfSession`: **this publisher's books only.** Iterate `books` filtered to `ctx.publisher`, call `on_end_of_session`, clear their `open`/`emitted_symbol` entries, and report each market unhealthy so authority transfers to the peer arm. Do **not** touch a shared floor or another publisher's state.
   - `BatchBoundary`: the crossed-book consistency point. Check `crossed()` on every instrument touched since the previous boundary and count `dz_mbp_crossed_total{venue}`. Nothing else — it must not change status or discard a book.
   - `Trade`: gated on `emit_trades` and on a resolved definition, built exactly as `MboProcessor`'s `Trade` arm does.
4. **Overflow check.** If the summed `buffered_len()` across `books` exceeds `MAX_BUFFERED_DELTAS_TOTAL`, find the book with the largest `buffered_len()`, call `drop_buffer()`, count `dz_mbp_buffer_overflows_total{venue}`, and repeat until under budget.
5. **Emit.** For each accumulated `(instrument_id, changes)` with a resolved definition and a `Ready` book, emit one `NormalizedBook` with `last: true`, `snapshot: false`, `source_ts_ns = book.last_event_ts()`, and the scaled prices/sizes. Update `emitted_symbol`.
6. **Health.** After steps 3–5, for every book whose `status()` crossed the `Ready` boundary this frame, call the authority's `set_health` for its `MarketKey`. Only on a transition, not per frame.

`book_for` mirrors `MboProcessor::book_for`: gate on a resolved definition first (releasing the `state` borrow), then bound to `MAX_PRICE_BOOKS` with least-recently-inserted eviction, dropping the evicted key's `open`/`emitted_symbol` entries in lockstep.

- [ ] **Step 7: Add the metrics**

Five counters in `src/metrics.rs`, all `&["venue"]` except the divergence one:

| Metric | Labels | Meaning |
|---|---|---|
| `dz_mbp_channel_resets_total` | `venue` | Publisher-and-channel state discarded on a `Reset Count` change. |
| `dz_mbp_buffer_overflows_total` | `venue` | Buffer-budget overflows; the largest instrument's buffer was dropped. Sustained means the snapshot period is too long for this host's memory budget. |
| `dz_mbp_orphan_snapshot_levels_total` | `venue` | `SnapshotLevel` with no open group to route it to — a publisher interleaving groups, or a lost `SnapshotBegin`. |
| `dz_mbp_crossed_total` | `venue` | Crossed inside markets observed at a consistency point. Observability only; never acted on. |
| `dz_mbp_divergence_total` | `venue`, `kind` | `Action`-vs-quantity disagreements, by kind. Never changes the applied result. |

- [ ] **Step 8: Wire the receiver**

In `src/ingest/receiver.rs`'s `run_feed`, add the `MarketByPrice` arm. It is the `MarketByOrder` arm with `MbpProcessor::new(feed.emit_trades)` in place of `MboProcessor::new(depth, feed.emit_trades)` — same `FeedPorts::ThreePort` destructuring, same `bail!` on a two-port row, same three `PortRole`s. Update the `bail!` text to name Market-by-Price. Also update `PortRole::Snapshot`'s doc comment (`receiver.rs:63`) — it says "(Constructed once the MBO receiver lands)", which is now stale twice over — and drop its `#[allow(dead_code)]` if it is no longer needed.

- [ ] **Step 9: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass.

- [ ] **Step 10: Document and commit**

Add the five metrics to `docs/metrics.md`. In `docs/input-sources.md`, add Market-by-Price alongside the other protocols: three ports, one book per `(publisher, channel, instrument)`, the `MAX_PRICE_BOOKS` and buffer-budget bounds, and the note that the caps are per receiver task — so N publishers hold N times the per-task bound, the same documentation point the order-keyed processor already carries.

```bash
git add -A
git commit -m "feat(ingest): add the market-by-price processor and receiver"
```

---

## Task 12: route `book` through the authority gate, and replay it

**Why:** Task 10 left `book` passing through the arbiter undeduped. With two arms that publishes both arms' deltas interleaved on one wire stream, and their per-instrument delta series are unrelated by construction — so a consumer's book corrupts while every per-arm sequence check the producer ran still passes. This is the change that makes the incremental product safe.

**This is where the plan's decision 1 lands: `book` uses the authority gate in *both* arbitration modes.** `Coordinated` governs quotes, where a per-tick latch is correct because a quote is full state and each tick's winner is self-contained. It is *not* correct for an incremental stream: a `source_ts` tick can hold several deltas, so a per-tick latch interleaves arms within a logical event. There is no mode in which interleaving two arms' deltas is acceptable, so there is no mode branch here.

**Files:**
- Modify: `src/ingest/arbiter.rs` (the `Book` arm, `books` authority, `book_replay`, the sampler tick)
- Modify: `src/sinks/ws.rs` (`serve`/`serve_client`/`replay_scoped` take the book map)
- Modify: `src/main.rs` (construct `BookSnapshot`, hand the `AuthorityConfig` to the arbiter, spawn the window-close tick)
- Modify: `PROTOCOL.md`, `docs/metrics.md`
- Test: `src/ingest/arbiter.rs`, `src/sinks/ws.rs`, `tests/dedup.rs`

**Interfaces:**
- Consumes: `StickyAuthority`/`AuthorityConfig`/`MarketKey` (Tasks 4–5), `NormalizedBook`/`BookAccumulator`/`BookSnapshot` (Task 10), `Arbiter::mode_for` (Task 3).
- Produces: `Arbiter::new(tx, trade_window, authority: AuthorityConfig)`; `Arbiter::set_book_replay(&mut self, books: BookSnapshot)`; `Arbiter::close_authority_windows(&mut self)`; `ws::serve(listener, tx, instruments, depth, books, cfg)`.

- [ ] **Step 1: Branch**

Branch off Task 11's final commit.

- [ ] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/arbiter.rs`:

```rust
    fn book_msg(channel: u32, instrument_id: u32, price: f64, size: f64) -> NormalizedBook {
        NormalizedBook {
            venue: "Lashay".into(),
            symbol: "KXBTCPERP".into(),
            channel,
            instrument_id,
            changes: vec![BookChange {
                action: BookAction::Update,
                side: BookSide::Bid,
                price,
                size,
            }],
            snapshot: false,
            last: true,
            source_ts_ns: 1_000,
            recv_ts_ns: 2_000,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
        }
    }

    fn arbiter_with(mode: ArbitrationMode) -> (Arbiter, broadcast::Receiver<Arc<FeedMessage>>) {
        let (tx, rx) = broadcast::channel(256);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW, test_authority_cfg());
        a.set_mode("Lashay", mode);
        (a, rx)
    }

    /// The core guarantee: exactly one arm's deltas reach the wire. Interleaving two arms corrupts a
    /// consumer's book because their per-instrument delta series are unrelated — and every per-arm
    /// sequence check the producer ran still passes, so nothing upstream catches it.
    #[test]
    fn book_publishes_one_arm_only() {
        let (mut a, mut rx) = arbiter_with(ArbitrationMode::Sticky);
        let (x, y) = (
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
        );
        for i in 0..5 {
            a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, i as f64)), x);
            a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, i as f64)), y);
        }
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 5, "only the authoritative arm published");
    }

    /// ...and in `Coordinated` mode too. There is no mode in which interleaving two arms' deltas is
    /// acceptable, so the gate is unconditional — `Coordinated` governs quotes, not books.
    #[test]
    fn book_publishes_one_arm_in_coordinated_mode_too() {
        let (mut a, mut rx) = arbiter_with(ArbitrationMode::Coordinated);
        let (x, y) = (
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
        );
        a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, 1.0)), x);
        a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, 2.0)), y);
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 1);
    }

    /// Authority is per market, so two instruments on one channel are independent and a third on
    /// another channel is too. `(venue, channel, instrument_id)` is the whole key — not `symbol`,
    /// which is a colliding display label.
    #[test]
    fn book_authority_is_per_channel_and_instrument() {
        let (mut a, mut rx) = arbiter_with(ArbitrationMode::Sticky);
        let (x, y) = (
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
        );
        a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, 1.0)), x);
        a.emit(FeedMessage::Book(book_msg(2, 42, 0.62, 1.0)), y);
        a.emit(FeedMessage::Book(book_msg(3, 41, 0.62, 1.0)), y);
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 3, "three distinct markets each elect independently");
    }

    /// The replay map accumulates the authoritative arm's book, not the losing arm's — the same
    /// single-writer discipline the depth replay map already has.
    #[test]
    fn book_replay_accumulates_the_authoritative_arm() {
        let (tx, _rx) = broadcast::channel(256);
        let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW, test_authority_cfg());
        a.set_mode("Lashay", ArbitrationMode::Sticky);
        let books: BookSnapshot = Arc::new(Mutex::new(HashMap::new()));
        a.set_book_replay(books.clone());
        let (x, y) = (
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
            Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
        );
        a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, 150.0)), x);
        a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, 999.0)), y); // dropped
        let guard = crate::model::lock(&books);
        let acc = guard.get(&(Arc::from("Lashay"), 2u32, 41u32)).expect("accumulated");
        let replayed = acc.to_book(&Arc::from("Lashay"), 2, 41);
        assert!(replayed.snapshot && replayed.last);
        assert_eq!(replayed.changes[0].action, BookAction::Clear);
        assert_eq!(replayed.changes[1].size, 150.0, "the winner's size, not the loser's");
    }

    /// A `book` message must never touch the quote or depth floors — they key on content, and
    /// identical `(side, price, size)` recurs constantly on a bounded price grid, so a content floor
    /// would collapse a level's real 100 -> 0 -> 100 oscillation and leave a subscriber holding 0 at
    /// a price that has liquidity.
    #[test]
    fn book_never_routes_through_a_content_floor() {
        let (mut a, mut rx) = arbiter_with(ArbitrationMode::Sticky);
        let x = Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        for size in [100.0, 0.0, 100.0] {
            a.emit(FeedMessage::Book(book_msg(2, 41, 0.62, size)), x);
        }
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 3, "every oscillation step must reach the wire");
    }
```

Add `fn test_authority_cfg() -> AuthorityConfig` to the test module with `sample_interval_ns: u64::MAX` so no window closes mid-test.

Add to `tests/dedup.rs` an integration test replaying two arms' interleaved MBP frames (once the Task 8 fixture exists; otherwise synthesize the two arms from the codec's own encoders as `tests/dedup.rs` already does for other cases) and asserting: exactly one arm's `book` messages reach a WS client, and applying them in order reproduces the authoritative arm's `PriceBook` level-for-level.

- [ ] **Step 3: Run to verify failure**

```bash
cargo test --lib arbiter::tests::book_
```

Expected: FAIL — `book_publishes_one_arm_only` asserts 5 but gets 10 (the Task 10 passthrough).

- [ ] **Step 4: Add the authority to the arbiter**

In `src/ingest/arbiter.rs`:

```rust
    /// Single-arm authority for the incremental `book` product. **Not** mode-dependent: an
    /// incremental stream must be single-arm in every mode, because a `source_ts` tick can hold
    /// several deltas and a per-tick latch would interleave the arms inside one logical event. The
    /// mode governs quotes.
    books: StickyAuthority,
    /// Accumulated per-market book state the WS server replays on connect and on each subscribe.
    /// Written here, on the authority gate's admit decision, so it holds the authoritative arm's
    /// book and never a discarded arm's copy — the same single-writer rule as `depth_replay`.
    book_replay: Option<BookSnapshot>,
```

`Arbiter::new` gains an `authority: AuthorityConfig` parameter and initializes `books: StickyAuthority::new(authority)`, `book_replay: None`. Add `set_book_replay`, mirroring `set_depth_replay`.

- [ ] **Step 5: Replace the passthrough with the gate**

```rust
            FeedMessage::Book(b) => {
                let key: MarketKey = (b.venue.clone(), b.channel, b.instrument_id);
                let arm = self.books.arm_ordinal(&b.venue, publisher);
                let decision = self.books.admit(key.clone(), publisher, b.recv_ts_ns);
                match decision {
                    Admit::Emitted { opened_tick } => {
                        if let Some(replay) = &self.book_replay {
                            let mut map = model::lock(replay);
                            map.entry(key)
                                .or_insert_with(|| BookAccumulator::new(b.symbol.clone()))
                                .apply(b);
                        }
                        let vm = self.vm(&b.venue);
                        vm.emit[EMIT_BOOK].inc();
                        if opened_tick {
                            metrics()
                                .arm_transfers
                                .with_label_values(&[&b.venue, "elected"])
                                .inc();
                        }
                        let _ = self.tx.send(Arc::new(msg));
                    }
                    // The losing arm's copy. Feed the sampler the head-to-head so re-election has a
                    // series to read, and record the margin for the operator.
                    Admit::Contest { lead_ns, .. } => {
                        self.books.observe_challenger(&key, publisher, b.recv_ts_ns);
                        metrics()
                            .arm_lead_ns
                            .with_label_values(&[&b.venue, "leader"])
                            .observe(lead_ns as f64);
                        self.vm(&b.venue).book_dropped.inc();
                        let _ = arm;
                    }
                    Admit::Dropped => {
                        self.vm(&b.venue).book_dropped.inc();
                    }
                }
            }
```

Add `book_dropped: IntCounter` to `VenueMetrics` backed by a new `dz_book_dropped_total{venue}`. `arm_ordinal` is called for its side effect of registering the arm and logging the ordinal-to-IP mapping once; keep the `let _ = arm;` only if clippy objects to the unused binding, otherwise drop the binding and call it as a statement.

- [ ] **Step 6: Drive the re-election window**

```rust
    /// Close every elapsed re-election window, transferring authority where a challenger cleared
    /// both conditions. Called on a timer off the hot path — a transfer re-baselines every consumer
    /// of that market, so it happens on the sampler's schedule and never per message.
    pub fn close_authority_windows(&mut self) {
        for (key, winner) in self.books.close_window(now_ns()) {
            metrics()
                .arm_transfers
                .with_label_values(&[key.0.as_ref(), "margin"])
                .inc();
            let arm = self.books.arm_ordinal(&key.0, winner);
            metrics()
                .arm_markets_held
                .with_label_values(&[key.0.as_ref(), arm])
                .set(self.books.markets_held(&key.0, winner) as i64);
        }
    }
```

In `src/main.rs`, spawn a task that ticks every `arb_sample_interval_secs` and calls `arbiter::lock(&arbiter).close_authority_windows()`. Add it to the top-level `select!` alongside the other independently-spawned tasks.

**Note on the transfer's consumer effect:** a transfer means the next batch comes from a different arm whose delta series is unrelated to the old one's. The processor for the new arm holds its own `Ready` book, so its next emission is an ordinary incremental batch — which a consumer would apply on top of the *old* arm's state. That is wrong. **A transfer must force a re-baseline.** Implement it here: on a transfer, materialize the replay accumulator for that market, `clear` it, and broadcast the new arm's full state as a `snapshot: true, last: true` re-baseline before its next incremental batch. The simplest correct form is to drop the accumulator entry and set a per-market `needs_rebaseline` flag the `Book` arm checks: while set, the first admitted batch from the new arm is replaced by `to_book()` of that arm's state — but the arbiter does not hold that arm's book. So instead: on transfer, emit a `Clear`-only re-baseline (`changes: [clear both]`, `snapshot: true`, `last: true`) immediately, and clear the accumulator. The consumer's book empties, and the new arm's subsequent incremental batches rebuild it. A lone clear is an explicitly legal message (Task 10 pins it), and `last: true` on it is exactly why that field is mandatory. Add a test: after a margin transfer, the next broadcast for that market is a clear-only re-baseline.

- [ ] **Step 7: Replay the book on connect and subscribe**

Thread `books: BookSnapshot` through `ws::serve` → `serve_client` → `replay_scoped`, and extend `replay_scoped` to materialize matching accumulators after the instruments and alongside `depth`:

```rust
    let live: Vec<FeedMessage> = {
        let guard = crate::model::lock(books);
        guard
            .iter()
            .filter(|((v, _, _), acc)| pass(v, acc.symbol(), "book"))
            .map(|((v, ch, id), acc)| FeedMessage::Book(acc.to_book(v, *ch, *id)))
            .collect()
    };
```

`BookAccumulator` needs a `pub fn symbol(&self) -> &str`. Chain `live` into the existing send loop, after `snapshot` and `books`' `depth` entries. Update `main.rs`'s `ws::serve` call and the existing `ws` tests' call sites.

- [ ] **Step 8: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass.

- [ ] **Step 9: Document and commit**

`PROTOCOL.md`: in the `book` section's "One book per market" paragraph, add that a failover surfaces as a `clear`-only re-baseline followed by the new source rebuilding the book, so a consumer that honors `clear` needs no other handling. Add `dz_book_dropped_total` to `docs/metrics.md` and the `arm_transfers` `reason` values (`elected`, `margin`, plus `health`/`silence` if Task 4's transfers are also counted — wire those counters here if they were left unwired).

```bash
git add -A
git commit -m "feat(arbiter): publish one arm's book per market and replay accumulated state"
```

---

## Task 13: runtime tape ownership

**Why:** the two Lashay groups are separately subscription-gated, so a host may be subscribed to the market-by-price group and not the top-of-book one. That host's WebSocket output must still carry a tape — a consumer that never receives the top-of-book feed on the wire still needs one. So both rows carry `emit_trades: true`, and *which one actually emits* becomes a runtime decision.

The invariant to preserve is the one that licenses Task 1's `trade_id == 0` bypass: **at most one tape emitter per venue at any moment.** Two active emitters would duplicate every FIX-sourced print, because a bypassed `0` has no window to collapse against. Ownership rule: **the top-of-book row owns a venue's tape whenever it is active; otherwise the highest-priority active row that carries one does.**

The reconciler is already the single activation authority and already recomputes the active set every tick, so it is the only place that knows the answer. Ownership is published as a shared flag the processor reads per trade rather than baked into the task at spawn time — a flag flip costs an atomic load, whereas making ownership part of the task key would abort and respawn a healthy receiver (dropping its books and reference data) every time the peer feed's subscription changed.

**Files:**
- Modify: `src/ingest/reconcile.rs` (compute and publish ownership)
- Modify: `src/ingest/receiver.rs` (`run_feed` takes the flag), `src/ingest/processor.rs` (all three processors read it)
- Modify: `src/ingest/feeds.rs` (replace the static invariant test)
- Modify: `src/metrics.rs`, `docs/metrics.md`, `README.md`
- Test: `src/ingest/reconcile.rs`, `src/ingest/processor.rs`

**Interfaces:**
- Consumes: `FeedKey`/`plan()` from `ingest::reconcile`; `Feed.emit_trades`; `FeedKind`.
- Produces:
  ```rust
  /// Whether this receiver currently owns its venue's trade tape. Shared with the reconciler, which
  /// flips it as the active feed set changes.
  pub type TapeOwner = Arc<std::sync::atomic::AtomicBool>;
  /// The owning feed for each venue, given the active set: the top-of-book row if one is active,
  /// else the first active row that carries a tape. Pure, so it is table-tested without a reconciler.
  pub fn tape_owners(active: &[FeedKey]) -> HashMap<&'static str, FeedKey>;
  ```
  `run_feed` gains a `tape: TapeOwner` parameter, replacing `feed.emit_trades` as what the processors gate on. `TobProcessor::new`, `MidpointProcessor::new`, `MboProcessor::new` and `MbpProcessor::new` take `TapeOwner` in place of `emit_trades: bool`.

- [ ] **Step 1: Branch**

Branch off Task 12's final commit.

- [ ] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/reconcile.rs`:

```rust
    const TOB: FeedKind = FeedKind::TopOfBook;
    const MBP: FeedKind = FeedKind::MarketByPrice;

    /// Top-of-book owns the tape whenever it is active, so a host subscribed to both groups gets
    /// exactly one copy of each print — the case that would otherwise duplicate every FIX-sourced
    /// print, since `trade_id == 0` bypasses the dedup window.
    #[test]
    fn top_of_book_owns_the_tape_when_active() {
        let owners = tape_owners(&[("Lashay", TOB, 30000), ("Lashay", MBP, 31000)]);
        assert_eq!(owners.get("Lashay"), Some(&("Lashay", TOB, 30000)));
    }

    /// A host subscribed to the market-by-price group alone must still get a tape. This is the case
    /// the earlier static "one row owns it forever" rule got wrong.
    #[test]
    fn market_by_price_owns_the_tape_when_it_is_the_only_active_row() {
        let owners = tape_owners(&[("Lashay", MBP, 31000)]);
        assert_eq!(owners.get("Lashay"), Some(&("Lashay", MBP, 31000)));
    }

    /// Ownership is per venue: one venue's active set never decides another's.
    #[test]
    fn tape_ownership_is_per_venue() {
        let owners = tape_owners(&[
            ("Lashay", MBP, 31000),
            ("Hyperliquid", TOB, 9001),
            ("Hyperliquid", FeedKind::MarketByOrder, 10001),
        ]);
        assert_eq!(owners.get("Lashay"), Some(&("Lashay", MBP, 31000)));
        assert_eq!(owners.get("Hyperliquid"), Some(&("Hyperliquid", TOB, 9001)));
    }

    /// A venue whose only active rows carry no tape has no owner — nothing to hand ownership to, and
    /// inventing one would emit trades from a depth-only feed.
    #[test]
    fn a_venue_with_no_tape_bearing_row_has_no_owner() {
        let owners = tape_owners(&[("Hyperliquid", FeedKind::MarketByOrder, 10001)]);
        assert!(owners.get("Hyperliquid").is_none());
    }

    /// Ownership is stable across publishers of one feed: N receivers of the owning feed all emit
    /// (they are mirrored publishers the arbiter already collapses on `trade_id`), and no receiver of
    /// a non-owning feed does. The invariant is one owning FEED, not one owning receiver.
    #[test]
    fn every_publisher_of_the_owning_feed_emits() {
        let owners = tape_owners(&[
            ("Hyperliquid", TOB, 9001),
            ("Hyperliquid", TOB, 9101),
            ("Hyperliquid", FeedKind::MarketByOrder, 10001),
        ]);
        let o = owners.get("Hyperliquid").unwrap();
        assert_eq!(o.1, TOB);
        assert!(owns(o, &("Hyperliquid", TOB, 9001)));
        assert!(owns(o, &("Hyperliquid", TOB, 9101)), "sibling publisher also emits");
        assert!(!owns(o, &("Hyperliquid", FeedKind::MarketByOrder, 10001)));
    }

    /// The reconciler flips the flag in place when the active set changes — no respawn, so the
    /// surviving receiver keeps its books and reference data. Losing the top-of-book subscription
    /// must hand the tape to market-by-price without a gap.
    #[test]
    fn ownership_flips_without_respawning_the_receiver() {
        // Drive the reconciler over two ticks: {TOB, MBP} then {MBP}. Assert MBP's TapeOwner goes
        // false -> true, that MBP's JoinHandle is the SAME one (not aborted and respawned), and that
        // `dz_tape_owner_changes_total{venue}` incremented once.
    }
```

Add to `mod tests` in `src/ingest/processor.rs`:

```rust
    /// The processors gate on the shared flag, per trade, so a flip takes effect immediately without
    /// a respawn.
    #[test]
    fn processors_honor_a_live_tape_ownership_flip() {
        // Feed one Trade frame with the flag false (assert nothing emitted), set it true, feed the
        // same frame (assert one trade emitted). Do this for TobProcessor and MbpProcessor.
    }
```

Fill both comment-only bodies — the comments state the assertions; write them.

- [ ] **Step 3: Run to verify failure**

```bash
cargo test --lib reconcile::tests::top_of_book_owns
```

Expected: FAIL to compile — `cannot find function 'tape_owners'`.

- [ ] **Step 4: Implement the pure ownership rule**

In `src/ingest/reconcile.rs`:

```rust
/// Whether this receiver currently owns its venue's trade tape. Shared with the reconciler, which
/// flips it as the active feed set changes. Read per trade, so a flip needs no respawn — and a
/// respawn would be the wrong tool, since it would drop a healthy receiver's books and reference
/// data every time a peer feed's subscription changed.
pub type TapeOwner = Arc<AtomicBool>;

/// Rank of a feed kind as a tape source, lower first. Top-of-book is the tape's natural home — it is
/// the protocol built around prints — so it wins whenever it is active; a book protocol carries the
/// tape only as a convenience for a subscriber who binds it alone. `None` means the kind carries no
/// tape and can never own one.
fn tape_rank(kind: FeedKind) -> Option<u8> {
    match kind {
        FeedKind::TopOfBook => Some(0),
        FeedKind::MarketByPrice => Some(1),
        // Depth-only: the order-keyed feed's executions are book events, not the venue tape.
        FeedKind::MarketByOrder | FeedKind::Midpoint => None,
    }
}

/// The owning feed per venue for a given active set. Exactly one feed per venue, so at most one tape
/// emitter — the invariant that licenses the `trade_id == 0` bypass in `arbiter::emit`, since a
/// FIX-sourced print carries no venue trade id and so has no dedup window to collapse against.
pub fn tape_owners(active: &[FeedKey]) -> HashMap<&'static str, FeedKey> {
    let mut best: HashMap<&'static str, (u8, FeedKey)> = HashMap::new();
    for k in active {
        let Some(rank) = tape_rank(k.1) else { continue };
        match best.get(&k.0) {
            // Tie-break on the base port so the choice is deterministic across ticks: two
            // publishers of the owning feed would otherwise alternate, flipping both their flags.
            Some((r, cur)) if (*r, cur.2) <= (rank, k.2) => {}
            _ => {
                best.insert(k.0, (rank, *k));
            }
        }
    }
    best.into_iter().map(|(v, (_, k))| (v, k)).collect()
}

/// Whether `key`'s receiver emits the tape, given its venue's owning feed. Every publisher of the
/// owning FEED emits — mirrored publishers are what the arbiter's `trade_id` window already
/// collapses — so ownership is per feed, not per receiver.
pub fn owns(owner: &FeedKey, key: &FeedKey) -> bool {
    owner.0 == key.0 && owner.1 == key.1
}
```

- [ ] **Step 5: Publish ownership from the reconcile loop**

`Reconciler` gains `tapes: HashMap<FeedKey, TapeOwner>`. In `apply_feeds`, create the flag before spawning a receiver and pass a clone into `run_feed`; drop the entry when a task is aborted or reaped, alongside the existing handle cleanup.

After the spawn/abort diff is applied — so the set is final for this tick — recompute and publish:

```rust
        let owners = tape_owners(&desired_keys);
        for (key, flag) in &self.tapes {
            let own = owners.get(key.0).is_some_and(|o| owns(o, key));
            if flag.swap(own, Ordering::Relaxed) != own {
                metrics().tape_owner_changes.with_label_values(&[key.0]).inc();
                info!(venue = key.0, kind = key.1.label(), publisher = key.2, owns_tape = own,
                      "trade tape ownership changed");
            }
        }
```

`Relaxed` is right: the flag is advisory per-message policy, not a synchronization edge — a trade decided against a one-tick-stale value is at worst one duplicated or one dropped print at a subscription boundary, and there is no invariant that requires the flip to be observed atomically with anything else.

- [ ] **Step 6: Thread the flag to the processors**

`run_feed` takes `tape: TapeOwner` and hands it to whichever processor it constructs, replacing `feed.emit_trades`. Each processor stores it and replaces its `if self.emit_trades` guard with `if self.tape.load(Ordering::Relaxed)`.

`Feed.emit_trades` stays — it is now the *static capability* ("this protocol carries a tape at all"), which `tape_rank` already encodes per kind. Rather than keep two sources of truth, make `run_feed` pass `Arc::new(AtomicBool::new(false))` for a row with `emit_trades: false` and never flip it: the reconciler only ever considers kinds `tape_rank` admits, so the two agree by construction. Add a `FEEDS` test asserting they do — `emit_trades` is true exactly when `tape_rank(kind).is_some()` — so a future row cannot declare a capability the ownership rule will not honor.

- [ ] **Step 7: Replace the static invariant test**

In `src/ingest/feeds.rs`, delete `at_most_one_trade_emitting_row_per_venue` (Task 1's static form — a venue may now legitimately have two tape-bearing rows) and replace it with the capability-agreement test from Step 6:

```rust
    /// `emit_trades` is a static capability claim; `reconcile::tape_rank` decides at runtime which
    /// capable feed actually emits. They must agree, or a row could claim a tape the ownership rule
    /// will never hand it — silently no tape for a host subscribed to that group alone.
    #[test]
    fn emit_trades_agrees_with_the_tape_ownership_rule() {
        for f in FEEDS {
            assert_eq!(
                f.emit_trades,
                crate::ingest::reconcile::tape_rank_is_some(f.kind),
                "{} {:?}",
                f.venue,
                f.kind
            );
        }
    }
```

Expose `pub fn tape_rank_is_some(kind: FeedKind) -> bool` rather than making `tape_rank` public — the rank values are an internal ordering, not a contract.

- [ ] **Step 8: Add the metric**

```rust
            tape_owner_changes: counter_vec(
                &registry,
                "dz_tape_owner_changes_total",
                "Trade-tape ownership transfers between a venue's feeds, as its subscribed set \
                 changes. Each one is a brief window where a print may duplicate or drop; a \
                 sustained rate means subscriptions are flapping.",
                &["venue"],
            ),
```

- [ ] **Step 9: Run everything**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass, including the existing `reconcile` plan tests and `tests/e2e.rs`.

- [ ] **Step 10: Document and commit**

`README.md`'s activation table: note that a venue's tape follows its active feeds — top-of-book owns it when subscribed, the market-by-price feed takes over when it is the only subscribed one, so a depth-only deployment still serves `trade`. Add the metric to `docs/metrics.md`. `CHANGELOG.md` under Unreleased → Changed: one line.

```bash
git add -A
git commit -m "feat(reconcile): hand a venue's trade tape to whichever feed is active"
```

---

## Task 14: the Lashay feed rows

**Blocked on the group-code question** at the top of this plan — which now has two parts: both codes, and the top-of-book group address. Do not start this task until both are answered. Everything else in the plan is independent of them.

**Why last:** the rows are what turn the machinery on, so they land only once every piece under them is tested. Two rows on two separately-gated groups: the perps top-of-book group (where the `trade_id == 0` fix from Task 1 first becomes load-bearing, since this is the first FIX-sourced feed the bridge binds) and the perps market-by-price group.

**Files:**
- Modify: `src/ingest/feeds.rs` (two rows, the venue→code match, the assertions)
- Modify: `README.md`, `CHANGELOG.md`
- Test: `src/ingest/feeds.rs`

**Interfaces:**
- Consumes: `FeedKind::MarketByPrice` (Task 11), `ArbitrationMode::Sticky` (Task 3), `reconcile::tape_owners` (Task 13).
- Produces: nothing further; this is the top of the stack.

- [ ] **Step 1: Branch**

Branch off Task 13's final commit.

- [ ] **Step 2: Write the failing tests**

Add to `mod tests` in `src/ingest/feeds.rs`:

```rust
    /// Both Lashay rows exist, speak the right protocols, and sit on **separate groups with
    /// separate codes** — so they are subscription-gated independently. That independence is the
    /// whole reason tape ownership had to become a runtime decision (Task 13): a host may be
    /// subscribed to the market-by-price code and not the top-of-book one.
    #[test]
    fn lashay_tob_and_mbp_are_independently_gated() {
        let rows: Vec<&Feed> = FEEDS.iter().filter(|f| f.venue == "Lashay").collect();
        assert_eq!(rows.len(), 2);
        let kinds: std::collections::HashSet<FeedKind> = rows.iter().map(|f| f.kind).collect();
        assert!(kinds.contains(&FeedKind::TopOfBook));
        assert!(kinds.contains(&FeedKind::MarketByPrice));
        assert_ne!(rows[0].group, rows[1].group, "separate groups");
        assert_ne!(rows[0].code, rows[1].code, "separate codes, so separately gated");
    }

    /// The arms are one FIX-sourced and one WS-sourced publisher with no comparable venue clock, so
    /// both rows must declare `Sticky`. Declaring `Coordinated` would race two arms on a coordinate
    /// they do not share.
    #[test]
    fn lashay_arbitrates_sticky() {
        for f in FEEDS.iter().filter(|f| f.venue == "Lashay") {
            assert_eq!(f.arbitration, ArbitrationMode::Sticky, "{:?}", f.kind);
        }
    }

    /// **Both** rows carry a tape, because either may be the only one a host is subscribed to. Which
    /// one actually emits is Task 13's runtime decision — top-of-book when it is active, otherwise
    /// market-by-price — and that keeps exactly one emitter per venue at any moment, which is what
    /// licenses the `trade_id == 0` bypass. A FIX-sourced print carries no venue trade id, so two
    /// simultaneous emitters would duplicate every print with no window to collapse them.
    #[test]
    fn both_lashay_rows_carry_a_tape() {
        for f in FEEDS.iter().filter(|f| f.venue == "Lashay") {
            assert!(f.emit_trades, "{:?} must be able to serve the tape alone", f.kind);
        }
    }

    /// ...and the runtime rule hands it to top-of-book when both are active, so a host subscribed to
    /// both codes still sees each print once.
    #[test]
    fn lashay_tape_goes_to_tob_when_both_are_active() {
        let owners = crate::ingest::reconcile::tape_owners(&[
            ("Lashay", FeedKind::TopOfBook, 30000),
            ("Lashay", FeedKind::MarketByPrice, 31000),
        ]);
        assert_eq!(owners.get("Lashay").map(|o| o.1), Some(FeedKind::TopOfBook));
    }

    /// Market-by-Price needs three ports; a two-port row would fail at `run_feed` with a bail, i.e.
    /// at runtime rather than here.
    #[test]
    fn lashay_mbp_uses_three_ports() {
        let mbp = FEEDS
            .iter()
            .find(|f| f.venue == "Lashay" && f.kind == FeedKind::MarketByPrice)
            .unwrap();
        for p in mbp.publishers {
            assert!(p.ports.snapshot().is_some(), "publisher {} has no snapshot port", p.base_port());
        }
    }
```

- [ ] **Step 3: Run to verify failure**

```bash
cargo test --lib feeds::tests::lashay
```

Expected: FAIL — `assertion failed: left == right; left: 0, right: 2`.

- [ ] **Step 4: Re-key the group-code test on `(venue, kind)`**

`every_feed_has_a_group_code` **panics on an unknown venue** (`feeds.rs:296`), so adding any row breaks it until the match is extended. But this is not just a new arm: the test is a `venue → code` map, and it asserts every row of a venue carries the *same* code. Lashay's two rows carry **different** codes, so the map's shape is wrong and must be re-keyed on `(venue, kind)`:

```rust
        for f in FEEDS {
            let expected = match (f.venue, f.kind) {
                // Both Hyperliquid protocols ride one group, so they share a code.
                ("Hyperliquid", FeedKind::TopOfBook | FeedKind::MarketByOrder) => "tiredsolid",
                ("Phoenix", FeedKind::TopOfBook) => "scottsdale",
                // Lashay's protocols are on SEPARATE groups, each gated on its own code.
                ("Lashay", FeedKind::TopOfBook) => "<neutral-tob-code>",
                ("Lashay", FeedKind::MarketByPrice) => "<neutral-mbp-code>",
                other => panic!("unexpected (venue, kind) {other:?}"),
            };
            assert_eq!(f.code, expected, "{} {:?} has wrong code", f.venue, f.kind);
        }
```

Both codes come from the blocking question's answer. **Neither may name the venue** — every existing code is a neutral word (`tiredsolid`, `scottsdale`) and these must be too.

Note this also tightens the test: it previously accepted any `kind` for a known venue, so a Hyperliquid `Midpoint` row would have silently inherited `tiredsolid`. Now every `(venue, kind)` pair is enumerated explicitly.

- [ ] **Step 5: Add the rows**

**Two groups, two codes, gated independently.** Within each group the two arms share one port block and are distinguished only by datagram source IP, so **each protocol lists exactly one `FeedPublisher`** and its single receiver task sees two source IPs. That is the shared-port-block model `FeedPublisher`'s docs already describe — and it is precisely why Task 2's per-publisher reference-data state and Task 11's per-publisher book keying are prerequisites here rather than nice-to-haves.

```rust
    // Lashay perps. Two groups, one per protocol, each subscription-gated on its own code — so a
    // host may run the market-by-price feed alone, which is why tape ownership is a runtime
    // decision (see `reconcile::tape_owners`).
    //
    // Unlike the Hyperliquid fleet, which separates publishers by port block, both arms of each
    // group publish to the SAME block and are distinguished only by datagram source IP: one
    // receiver task per protocol, two source IPs. The arms are one FIX-sourced and one WS-sourced
    // and share no comparable venue clock (no stable entry id, no per-entry venue timestamp), hence
    // `Sticky`.
    Feed {
        venue: "Lashay",
        code: "<neutral-tob-code>",
        kind: FeedKind::TopOfBook,
        group: Ipv4Addr::new(/* the perps top-of-book group — from deployment config */),
        publishers: &[FeedPublisher {
            ports: FeedPorts::TwoPort {
                mktdata: 30000,
                refdata: 40000,
            },
        }],
        emit_trades: true,
        arbitration: ArbitrationMode::Sticky,
    },
    // Carries a tape too: a host subscribed to this code alone must still serve `trade`. Which row
    // actually emits is `reconcile::tape_owners`' decision each tick — top-of-book when it is
    // active, this row otherwise — so exactly one emits at any moment. That matters because a
    // FIX-sourced print carries no venue trade id, arrives as `trade_id == 0`, and bypasses the
    // dedup window; two simultaneous emitters would duplicate every print.
    Feed {
        venue: "Lashay",
        code: "<neutral-mbp-code>",
        kind: FeedKind::MarketByPrice,
        group: Ipv4Addr::new(233, 84, 178, 4),
        publishers: &[FeedPublisher {
            ports: FeedPorts::ThreePort {
                mktdata: 31000,
                refdata: 41000,
                snapshot: 51000,
            },
        }],
        emit_trades: true,
        arbitration: ArbitrationMode::Sticky,
    },
```

**Confirm the top-of-book group and both port blocks against the live deployment before committing.** Only the market-by-price row is known: group `233.84.178.4`, mkt 31000 / ref 41000 / snap 51000, measured and recorded in the design doc. The top-of-book row's **group address is unknown** — the design deliberately does not name it — and its port block above is **inferred** from the same base-port scheme. Both must come from deployment config. A wrong group or block shows up as a permanent `dz_receiver_up == 0` for that publisher rather than as an error, so it will not announce itself. `group_port_pairs_are_globally_unique` catches only a collision with an existing row, not a wrong-but-unused address.

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

Expected: all pass. `venue_kind_pairs_are_unique`, `every_feed_has_at_least_one_publisher`, `publisher_base_ports_unique_within_a_feed`, `group_port_pairs_are_globally_unique`, `emit_trades_agrees_with_the_tape_ownership_rule` and `arbitration_mode_agrees_across_a_venues_rows` all cover the new rows automatically.

- [ ] **Step 7: Verify against the live feed**

Run it three ways, because the third is the one the old static tape rule got wrong.

With the tunnel up and subscribed to both codes:

```bash
cargo build --release
sudo sysctl -w net.core.rmem_max=268435456
RUST_LOG=info ./target/release/doublezero-edge-connect --iface doublezero1 --feed Lashay --ws-bind 0.0.0.0:8081
```

Confirm, from `/metrics` and a WS client: `dz_receiver_up == 1` for both blocks; `dz_feed_up{venue="Lashay"} == 1`; `dz_arm_markets_held` shows both arms registered and one holding every market; `dz_arm_lead_ns` populating; `dz_mbp_orphan_snapshot_levels_total == 0`; `dz_mbp_crossed_total` at or near zero; a connecting client receives `instrument` messages then a `clear`-led re-baseline per market, then incremental batches. Cross-check one instrument's book against `marketbyprice-parser` pointed at the same group.

**Then the two single-feed cases.** These are what the earlier static tape rule got wrong, so verify them explicitly rather than reasoning about them:

```bash
# Top-of-book only: quotes and a tape, no books.
RUST_LOG=info ./target/release/doublezero-edge-connect --iface doublezero1 --feed Lashay --ws-bind 0.0.0.0:8081 --publisher-port 30000
```

With only the market-by-price code subscribed (or the top-of-book row otherwise inactive), confirm the WebSocket **still carries `trade`** and that `dz_tape_owner_changes_total{venue="Lashay"}` incremented once as ownership moved. Then re-subscribe the top-of-book code and confirm the tape moves back — one increment, no receiver respawn (`dz_receiver_up` for the market-by-price block must not blip to 0), and **no duplicated prints** in the overlap. That last check is the whole point of the invariant: a FIX-sourced print carries `trade_id == 0` and bypasses the dedup window, so a moment with two emitters would double every print with nothing to collapse them.

**Also worth measuring here, and cheap now:** the recurrence-interval distribution of identical `(side, price, size)` within one arm against the inter-arm arrival skew. It needs only one arm and decides whether a windowed content dedup could ever be safe — i.e. whether `Sticky` is permanent or an interim. Record the result; do not act on it in this plan.

- [ ] **Step 8: Document and commit**

`README.md`: add Lashay to the feed table and to the activation table (subscription-gated like every other market-data feed). `CHANGELOG.md` under Unreleased → Added: the two rows, the incremental `book` product, the `channel`/`type` filters, and single-arm arbitration — four lines, not four paragraphs.

```bash
git add -A
git commit -m "feat(feeds): ingest the Lashay perps top-of-book and market-by-price groups"
```

- [ ] **Step 9: Check the venue naming before any push**

```bash
grep -rn '[A-Za-z]Lashay\|Lashay[-_][a-z]' src tests examples
```

Must return nothing. Scoped to source, not the whole tree, because the pattern is ordinary English in prose (`Lashay-shaped`) and only ever a defect in an identifier. A hit means a rename ran over a package, crate, service or group code that owns its own name — see the naming rule under *Global Constraints*.

---

## Self-review

Run against the design doc before handing off.

**Spec coverage.** Every numbered section of `2026-08-06-lashay-mbp-design.md` maps to a task:

| Design | Task |
|---|---|
| §2.1 full depth, incremental output | 10, 11, 12 |
| §2.2 one book-output model | 10 (`book` added); MBO migration is PR 7, **out of scope** |
| §2.3 one arbitration module, clock as a parameter | 3, 4, 5, 12 |
| §2.4 arm axis is the source IP, `channel_id` is the instrument set | 2, 6, 11, 14 |
| §2.5 identity is `(venue, channel_id, instrument_id)` | 10, 11, 12 |
| §2.6 MBP carries the trade tape | 1 (the `trade_id == 0` fix), 13 (runtime ownership), 14 (both rows carry it) |
| §3 modules | 7, 8 (`codec_mbp`), 9 (`pricebook`), 11 (`MbpProcessor`), 12 (arbiter), 6+10 (ws) |
| §4.1–4.9 conformance | 9 (items 2, 3, 4, 8), 11 (1, 5, 7, 9), 8 (item 6) |
| §5 WebSocket output | 10 (message), 6 (filters + replay scoping) |
| §6 reference consumer | 10 (the three constraints that force the shape) |
| §7 PR stack 1–6 | 1–14 |
| §7 PR 8 (venue-compatible sinks, added 2026-08-07) | out of scope; the sink for Lashay becomes reachable once Task 12 lands |
| §8 out of scope | untouched |
| §9 validation | 7 Step 7 + 8 Step 7 (Go oracle), 9 (unit tests per conformance item), 12 (two-arm integration), 14 Step 7 (live, all three subscription shapes, plus the recurrence measurement) |
| §10 open questions | Q1 needs no answer (`Sticky` does not depend on it, and `Coordinated` becomes relevant only once the FIX side is venue-timestamped); Q2 resolved as four flags with stated defaults (Task 5); Q3 is the open spec PR, nothing owed here; Q4 is data hygiene, not blocking |

**Gaps deliberately left, and where they are stated:** PR 7 (MBO → `book`, deleting `depth`/`DEPTH_LEVELS`) needs its own design doc; PR 8's sinks follow it. The seventh Hyperliquid publisher, `MidpointProcessor`'s un-keyed `SeqTracker`, `channel_id` in the MBO book key, and re-labelling receiver/health metrics by arm ordinal after the Hyperliquid shared-port migration are all named under *Reconciliation*. The PROTOCOL.md v2 stamp is deferred to the `depth` deletion (decision 3). The real-frame MBP fixture is gated on tunnel access (Task 8 Step 9) and its absence must be stated in the PR body, not papered over.

**Type consistency.** `MarketKey = (Arc<str>, u32, u32)` is the same triple in Tasks 4, 5, 11 and 12. `Publisher` gains `Hash` once, in Task 4. `PerPublisher<D>` is defined in Task 2 and reused by `MbpProcessor` in Task 11. `AuthorityConfig` replaces `StickyAuthority::new(u64)` in Task 5, and Task 4's tests are updated in that same task. `BookSnapshot` is the accumulator map from its first definition in Task 10 — Task 12 consumes exactly that. `FrameCtx` deliberately does **not** gain `channel_id` (Task 11 Step 5 corrects its own Interfaces block on this). `TapeOwner` replaces `emit_trades: bool` in all four processor constructors in Task 13, and `Feed.emit_trades` survives as the static capability claim that Task 13 Step 7 pins against `tape_rank`.

**Two tests are deliberately written and then replaced.** Task 1's `at_most_one_trade_emitting_row_per_venue` is correct until a venue carries a tape on two separately-gated feeds; Task 13 Step 7 replaces it with the capability-agreement test, and Task 13's reconciler tests carry the invariant itself. Task 14 Step 4 re-keys `every_feed_has_a_group_code` from `venue` to `(venue, kind)`, because Lashay's two rows carry different codes and the old shape asserted they could not. Neither is churn — in both cases the original test was right for the tree it shipped into.

---

## Execution handoff

Fourteen tasks, each a branch off its predecessor, each ending with a green suite and a commit. Track them in *Progress* at the top of this file.

**The task order is a build order and does not map cleanly onto the design's PR boundaries**, because the wire type has to precede the producer that emits it: Task 10 (`book` on the wire, design PR 5) lands before Task 11 (`MbpProcessor`, design PR 4). Ship it as:

| Design PR | Tasks |
|---|---|
| 1 — per-publisher state + arbitration | 1, 2, 3, 4, 5 |
| 2 — filter dimensions | 6 |
| 3 — `codec_mbp.rs` | 7, 8 |
| 4 + 5 — book reconstruction and incremental output | 9, 10, 11, 12 (one PR: 11 cannot compile without 10, and 12 is what makes 11 safe) |
| 6 — feed rows | 13, 14 |

**Task 14 is blocked** on the group-code question, which now has two parts: both group codes, and the top-of-book group address. Tasks 1–13 are not blocked — Task 13 is reconciler work that needs no deployment detail.
