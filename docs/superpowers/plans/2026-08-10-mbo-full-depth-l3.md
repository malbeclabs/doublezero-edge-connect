# Full-depth MBO (order-level L3) — Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop flattening Market-by-Order to a top-10 price-aggregated `depth`, and emit the real order-level book on the existing incremental `book` message — carrying the venue's own `order_id`, raced across publishers on venue event identity.

**Architecture:** Books stay per publisher and keep their current recovery state machine. Each emits order-level changes; the arbiter collapses copies on venue event identity (`order_id`, `kind`, `trade_id`) and emits first arrival. Correctness rests on a per-order guard — a removed `order_id` is never re-added — not on the dedup window. Additive throughout: `depth` keeps working and PROTOCOL.md stays v1.

**Tech Stack:** Rust 2021, tokio, `serde`/`serde_json`, `prometheus`, `HashMap`/`BTreeMap`/`VecDeque`.

**Design doc:** [`docs/superpowers/specs/2026-08-10-mbo-full-depth-l3-design.md`](../specs/2026-08-10-mbo-full-depth-l3-design.md). Read *Two sourcing models*, *Racing mode*, and *Consequences and risks* before starting.

## Global Constraints

- **Additive only. Nothing on the wire is withdrawn in this plan.** `depth`, `DEPTH_LEVELS`, `NormalizedDepth` and `DepthSnapshot` all keep working exactly as they do today. PROTOCOL.md stays **v1**. There are testers on `depth`; breaking them is out of scope and not permitted here.
- **Never credit Claude or any AI.** No `Co-Authored-By`, no "Generated with Claude Code", no AI-attribution comments.
- **This software targets Linux and is never validated on a macOS or Windows host.** Run `cargo test` / `clippy` / `fmt` in a Linux dev container.
- **PROTOCOL.md is the contract** for the WebSocket output only. The forward-compat rule (consumers ignore unknown types and fields) must hold for every change here.
- **Comments: one line is the default.** A comment must never be longer than the code it describes.
- **Never hard-wrap markdown.** One paragraph is one line.
- **Every commit compiles and every test passes.** `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` clean before moving on.
- **Base branch:** off current `main`. PRs #109 and #110 rebased onto the same tip and touch `processor.rs`; if either lands mid-flight, merge `main` in rather than rebasing.

## File Structure

| File | Responsibility in this plan |
|---|---|
| `src/model.rs` | `BookChange.order_id`; `BookAccumulator` becomes order-keyed and materializes either an order set or a price fold with per-level order counts. |
| `src/ingest/book.rs` | Report the order-level changes each event produced; the removed-order guard; the full order set for a re-baseline. |
| `src/ingest/arbiter.rs` | Order-event dedup oracle for racing mode; content-disagreement detection; re-baseline suppression. |
| `src/ingest/processor.rs` | `MboProcessor` emits `FeedMessage::Book` alongside its existing `depth`. |
| `src/sinks/ws.rs` | Replay scope: order-level bootstrap on request, price fold by default. |
| `src/metrics.rs` | `dz_mbo_arm_disagreement_total`, `dz_mbo_removed_evicted_total`, `dz_book_events_deduped_total`. |
| `src/main.rs` | `--arb-book-dedup-window-ms` (default 250). |
| `PROTOCOL.md`, `docs/input-sources.md`, `docs/metrics.md`, `CHANGELOG.md` | Documentation. |

---

### Task 1: `order_id` on the wire

**Files:**
- Modify: `src/model.rs`
- Test: `src/model.rs` (`mod tests`)

**Interfaces:**
- Produces: `BookChange { action: BookAction, side: BookSide, price: f64, size: f64, order_id: u64 }`. Every later task constructs `BookChange` with this field. `0` means "no order identity" and is what the Market-by-Price path emits.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/model.rs`:

```rust
/// `order_id` is additive: a payload written before this field still parses, and an
/// order-level change round-trips its id. Zero is the price-aggregated sentinel, which is
/// what Market-by-Price emits and what a consumer reads as "no order identity".
#[test]
fn book_change_order_id_is_additive_and_round_trips() {
    let legacy = r#"{"action":"update","side":"bid","price":1.5,"size":2.0}"#;
    let parsed: BookChange = serde_json::from_str(legacy).expect("legacy payload must parse");
    assert_eq!(parsed.order_id, 0, "absent id defaults to the no-identity sentinel");

    let c = BookChange {
        action: BookAction::Update,
        side: BookSide::Bid,
        price: 1.5,
        size: 2.0,
        order_id: 42,
    };
    let round: BookChange = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
    assert_eq!(round.order_id, 42);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib model::tests::book_change_order_id_is_additive_and_round_trips`
Expected: FAIL to compile — `struct BookChange has no field named order_id`.

- [ ] **Step 3: Add the field**

In `src/model.rs`, on `pub struct BookChange`:

```rust
    /// The venue's own order id for an order-level (`L3`) change, or `0` when the change is
    /// price-aggregated and carries no order identity. Never `0` on a Market-by-Order feed: a
    /// consumer that keys an L3 book by id treats `0` as "aggregate me", silently degrading to L2.
    #[serde(default)]
    pub order_id: u64,
```

- [ ] **Step 4: Fix every construction site**

`BookChange` is constructed in `src/ingest/processor.rs` (the Market-by-Price path) and in tests. Add `order_id: 0` to each — Market-by-Price is price-aggregated and has no order identity.

Run: `cargo build` and fix each error until it compiles.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib model:: && cargo test --lib ingest::processor::tests::mbp`
Expected: PASS, including the existing Market-by-Price shape tests.

- [ ] **Step 6: Commit**

```bash
git add src/model.rs src/ingest/processor.rs
git commit -m "feat(model): carry an order id on book changes"
```

---

### Task 2: `BookState` reports the changes an event produced

**Files:**
- Modify: `src/ingest/book.rs`
- Test: `src/ingest/book.rs` (`mod tests`)

**Interfaces:**
- Consumes: `DeltaOp { seq: u32, mktdata_seq: u64, ts: u64, kind: DeltaKind }` and `DeltaKind::{Add{order_id,is_bid,price_raw,qty_raw}, Cancel{order_id}, Execute{order_id,exec_qty_raw,full_fill}}`, both already in this file.
- Produces:
  ```rust
  /// One order-level change, in raw wire integers. The processor scales these with the
  /// instrument's exponents; `book.rs` stays codec- and precision-agnostic.
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct OrderChange {
      pub order_id: u64,
      pub is_bid: bool,
      pub price_raw: i64,
      /// The order's absolute resulting quantity. `0` means the order is gone.
      pub qty_raw: u64,
  }

  pub fn on_delta_reporting(&mut self, op: DeltaOp, out: &mut Vec<OrderChange>) -> bool;
  ```
  `on_delta` keeps its current signature and delegates, so existing callers are untouched.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/ingest/book.rs`:

```rust
/// Every applied event reports exactly the order it touched, with the order's absolute
/// resulting quantity — never a delta. A partial fill reports the remainder; a full fill and a
/// cancel both report `0`, which is how the consumer learns the order is gone.
#[test]
fn deltas_report_the_order_they_touched() {
    let mut b = synced_empty_book();
    let mut out = Vec::new();

    assert!(b.on_delta_reporting(add_op(1, 7, true, 100, 10), &mut out));
    assert_eq!(out, vec![OrderChange { order_id: 7, is_bid: true, price_raw: 100, qty_raw: 10 }]);

    out.clear();
    assert!(b.on_delta_reporting(exec_op(2, 7, 4, false), &mut out));
    assert_eq!(out, vec![OrderChange { order_id: 7, is_bid: true, price_raw: 100, qty_raw: 6 }],
        "a partial fill reports the remaining quantity");

    out.clear();
    assert!(b.on_delta_reporting(cancel_op(3, 7), &mut out));
    assert_eq!(out, vec![OrderChange { order_id: 7, is_bid: true, price_raw: 100, qty_raw: 0 }],
        "a cancel reports zero, which is how a consumer removes the order");
}

/// A delta the book rejects (stale, or out of sequence) reports nothing, so the caller cannot
/// publish a change the book did not make.
#[test]
fn a_rejected_delta_reports_nothing() {
    let mut b = synced_empty_book();
    let mut out = Vec::new();
    assert!(b.on_delta_reporting(add_op(1, 7, true, 100, 10), &mut out));
    out.clear();
    assert!(!b.on_delta_reporting(add_op(1, 8, true, 101, 10), &mut out), "duplicate seq rejected");
    assert!(out.is_empty());
}
```

Add these helpers to `mod tests` if not already present:

```rust
fn add_op(seq: u32, order_id: u64, is_bid: bool, price_raw: i64, qty_raw: u64) -> DeltaOp {
    DeltaOp { seq, mktdata_seq: seq as u64, ts: seq as u64, kind: DeltaKind::Add { order_id, is_bid, price_raw, qty_raw } }
}
fn cancel_op(seq: u32, order_id: u64) -> DeltaOp {
    DeltaOp { seq, mktdata_seq: seq as u64, ts: seq as u64, kind: DeltaKind::Cancel { order_id } }
}
fn exec_op(seq: u32, order_id: u64, exec_qty_raw: u64, full_fill: bool) -> DeltaOp {
    DeltaOp { seq, mktdata_seq: seq as u64, ts: seq as u64, kind: DeltaKind::Execute { order_id, exec_qty_raw, full_fill } }
}
```

`synced_empty_book()` already exists in this module as the fixture the recovery tests use; if its name differs, reuse whatever the existing tests call to get a `Synced` book with anchor seq 0.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::book::tests::deltas_report_the_order_they_touched`
Expected: FAIL to compile — `no method named on_delta_reporting`.

- [ ] **Step 3: Implement**

In `src/ingest/book.rs`, add the type and rename the body of `on_delta`:

```rust
/// One order-level change, in raw wire integers. The processor scales these with the instrument's
/// exponents; this module stays codec- and precision-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderChange {
    pub order_id: u64,
    pub is_bid: bool,
    pub price_raw: i64,
    /// Absolute resulting quantity. `0` means the order is gone.
    pub qty_raw: u64,
}
```

Change the existing `pub fn on_delta(&mut self, op: DeltaOp) -> bool` to delegate, and move its body into the reporting variant:

```rust
    pub fn on_delta(&mut self, op: DeltaOp) -> bool {
        let mut sink = Vec::new();
        self.on_delta_reporting(op, &mut sink)
    }

    /// [`Self::on_delta`], additionally appending the order-level change each applied event
    /// produced. Reports nothing for an event the book rejects, so a caller cannot publish a
    /// change the book did not make.
    pub fn on_delta_reporting(&mut self, op: DeltaOp, out: &mut Vec<OrderChange>) -> bool {
        // ... existing on_delta body, with a push at each mutation site ...
    }
```

At each mutation site inside the moved body, push the resulting state:

- `Add`: after inserting, `out.push(OrderChange { order_id, is_bid, price_raw, qty_raw });`
- `Cancel`: after removing, using the removed `RestingOrder`, `out.push(OrderChange { order_id, is_bid: o.is_bid, price_raw: o.price_raw, qty_raw: 0 });`
- `Execute`: after reducing, `out.push(OrderChange { order_id, is_bid: o.is_bid, price_raw: o.price_raw, qty_raw: remaining });` where `remaining` is `0` on a full fill.

Every early return that rejects the delta must leave `out` untouched.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib ingest::book::`
Expected: PASS, including every pre-existing recovery test — `on_delta`'s behaviour is unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/ingest/book.rs
git commit -m "feat(book): report the order-level change each delta produced"
```

---

### Task 3: the removed-order guard

**Files:**
- Modify: `src/ingest/book.rs`, `src/metrics.rs`
- Test: `src/ingest/book.rs`

**Interfaces:**
- Produces: `const MAX_REMOVED_ORDERS: usize = 1 << 20;` and a private `removed` set on `BookState`. No public API change — the guard acts inside `on_delta_reporting`.

**Why this task exists:** venues do not reuse `order_id`. Racing means a copy of an `Add` can arrive after the `Cancel` that removed it — from a slower publisher, or a mirror replaying a backlog. Re-adding a dead order silently corrupts the book, and this is the guard the design puts correctness on, *instead of* on the dedup window.

- [ ] **Step 1: Write the failing test**

```rust
/// A venue never reuses an order id, so an `Add` for an order this book has already removed is a
/// late copy from a slower publisher, not a new order. Re-adding it would resurrect a dead order
/// and silently corrupt the book. This is the guard correctness rests on — the dedup window is a
/// cost knob, not a correctness parameter.
#[test]
fn a_removed_order_is_never_resurrected() {
    let mut b = synced_empty_book();
    let mut out = Vec::new();
    assert!(b.on_delta_reporting(add_op(1, 7, true, 100, 10), &mut out));
    assert!(b.on_delta_reporting(cancel_op(2, 7), &mut out));
    out.clear();

    // A late `Add` for the same id, arriving at a fresh sequence so nothing else rejects it.
    assert!(!b.on_delta_reporting(add_op(3, 7, true, 100, 10), &mut out),
        "a removed id must not be re-added");
    assert!(out.is_empty(), "and must publish nothing");
    assert!(!b.has_order(7), "the book must not hold it");
}

/// A full fill removes the order just as a cancel does, so it gets the same protection.
#[test]
fn a_fully_executed_order_is_never_resurrected() {
    let mut b = synced_empty_book();
    let mut out = Vec::new();
    assert!(b.on_delta_reporting(add_op(1, 7, true, 100, 10), &mut out));
    assert!(b.on_delta_reporting(exec_op(2, 7, 10, true), &mut out));
    out.clear();
    assert!(!b.on_delta_reporting(add_op(3, 7, true, 100, 10), &mut out));
    assert!(out.is_empty());
}

/// The guard's memory is bounded, so a long-running book cannot grow it without limit. Eviction
/// reopens the resurrection path for a copy arriving later than a million removals, which is the
/// residual exposure and is counted rather than hidden.
#[test]
fn the_removed_set_is_bounded() {
    let mut b = synced_empty_book();
    let mut out = Vec::new();
    for i in 0..(MAX_REMOVED_ORDERS as u64 + 1_000) {
        let seq = (i + 1) as u32;
        b.on_delta_reporting(add_op(seq, i, true, 100, 1), &mut out);
        b.on_delta_reporting(cancel_op(seq + 1, i), &mut out);
        out.clear();
    }
    assert!(b.removed_len() <= MAX_REMOVED_ORDERS);
}
```

Add a test-only accessor pair next to the existing `is_synced`:

```rust
    #[cfg(test)]
    pub(crate) fn has_order(&self, order_id: u64) -> bool {
        self.orders.contains_key(&order_id)
    }
    #[cfg(test)]
    pub(crate) fn removed_len(&self) -> usize {
        self.removed.len()
    }
```

Note the sequence arithmetic in `the_removed_set_is_bounded` advances two per iteration; if the book's sequence checking rejects the pattern, drive the removals through `on_delta_reporting` with strictly increasing `seq` and adjust the helper calls accordingly — the assertion under test is the bound, not the sequencing.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::book::tests::a_removed_order_is_never_resurrected`
Expected: FAIL — the add succeeds and `has_order(7)` is true.

- [ ] **Step 3: Implement**

In `src/ingest/book.rs`:

```rust
/// Cap on remembered removed order ids. The wire is unauthenticated, so this cannot be unbounded;
/// past the cap the oldest id is forgotten, which reopens the resurrection path only for a copy
/// arriving later than this many removals. Counted by `dz_mbo_removed_evicted_total`.
const MAX_REMOVED_ORDERS: usize = 1 << 20;
```

On `BookState`:

```rust
    /// Order ids this book has removed — see [`MAX_REMOVED_ORDERS`]. A venue never reuses an id, so
    /// membership means "a late copy", not "a new order".
    removed: HashSet<u64>,
    /// Insertion order of `removed`, oldest first, for the bound.
    removed_order: VecDeque<u64>,
```

Initialize both in `new()`, clear both in `on_end_of_session` and `on_instrument_reset` (a session or instrument reset is a new id space), and clear both when a snapshot installs a fresh book in `on_snapshot_end`.

In `on_delta_reporting`, guard the `Add` arm before any mutation:

```rust
            DeltaKind::Add { order_id, .. } if self.removed.contains(&order_id) => return false,
```

and record on removal, in both the `Cancel` arm and the full-fill branch of `Execute`:

```rust
    fn note_removed(&mut self, order_id: u64) {
        if self.removed.insert(order_id) {
            self.removed_order.push_back(order_id);
            while self.removed.len() > MAX_REMOVED_ORDERS {
                if let Some(old) = self.removed_order.pop_front() {
                    self.removed.remove(&old);
                    metrics().mbo_removed_evicted.inc();
                }
            }
        }
    }
```

- [ ] **Step 4: Add the metric**

In `src/metrics.rs`, alongside the existing counters:

```rust
            mbo_removed_evicted: counter(
                &registry,
                "dz_mbo_removed_evicted_total",
                "Removed order ids forgotten because the per-book guard hit its cap. Non-zero \
                 means a very late duplicate could resurrect a dead order; sustained non-zero \
                 means the cap is too small for this venue's churn.",
            ),
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib ingest::book:: && cargo test --lib metrics::`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ingest/book.rs src/metrics.rs
git commit -m "feat(book): refuse to resurrect a removed order"
```

---

### Task 4: the full order set for a re-baseline

**Files:**
- Modify: `src/ingest/book.rs`
- Test: `src/ingest/book.rs`

**Interfaces:**
- Produces: `pub fn order_set(&self, out: &mut Vec<OrderChange>)` — every resting order, ascending by `(is_bid desc, price_raw, order_id)` so the output is deterministic across runs and publishers.

- [ ] **Step 1: Write the failing test**

```rust
/// A re-baseline publishes the book's whole content, so the ordering must be deterministic —
/// two publishers materializing the same book must produce byte-identical change lists, or the
/// arbiter's content comparison reports a disagreement that is really just map iteration order.
#[test]
fn order_set_is_complete_and_deterministically_ordered() {
    let mut b = synced_empty_book();
    let mut sink = Vec::new();
    for (seq, id, is_bid, px, qty) in [
        (1u32, 30u64, true, 100i64, 5u64),
        (2, 10, true, 101, 7),
        (3, 20, false, 105, 9),
        (4, 40, true, 100, 3),
    ] {
        assert!(b.on_delta_reporting(add_op(seq, id, is_bid, px, qty), &mut sink));
    }

    let mut out = Vec::new();
    b.order_set(&mut out);
    assert_eq!(
        out,
        vec![
            OrderChange { order_id: 10, is_bid: true, price_raw: 101, qty_raw: 7 },
            OrderChange { order_id: 30, is_bid: true, price_raw: 100, qty_raw: 5 },
            OrderChange { order_id: 40, is_bid: true, price_raw: 100, qty_raw: 3 },
            OrderChange { order_id: 20, is_bid: false, price_raw: 105, qty_raw: 9 },
        ],
        "bids best-first, then asks best-first, ties broken by order id"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::book::tests::order_set_is_complete_and_deterministically_ordered`
Expected: FAIL to compile — `no method named order_set`.

- [ ] **Step 3: Implement**

```rust
    /// Every resting order, bids best-first then asks best-first, ties broken by order id. The
    /// ordering is deterministic on purpose: two publishers materializing the same book must
    /// produce identical lists, or the arbiter's content comparison flags map iteration order as
    /// a disagreement.
    pub fn order_set(&self, out: &mut Vec<OrderChange>) {
        out.clear();
        out.extend(self.orders.iter().map(|(&order_id, o)| OrderChange {
            order_id,
            is_bid: o.is_bid,
            price_raw: o.price_raw,
            qty_raw: o.qty_raw,
        }));
        out.sort_by_key(|c| (!c.is_bid, if c.is_bid { -c.price_raw } else { c.price_raw }, c.order_id));
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib ingest::book::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingest/book.rs
git commit -m "feat(book): materialize the full order set for a re-baseline"
```

---

### Task 5: the order-keyed accumulator

**Files:**
- Modify: `src/model.rs`
- Test: `src/model.rs`

**Interfaces:**
- Produces, on `BookAccumulator`:
  ```rust
  /// Fold the accumulated orders into price levels. `Vec<(price, size, order_count)>`, bids
  /// best-first then asks best-first.
  pub fn price_fold(&self) -> (Vec<(f64, f64, u32)>, Vec<(f64, f64, u32)>);
  ```
  `apply` and `to_book` keep their signatures. `to_book` gains a scope argument in Task 10; leave it alone here.

**Why:** `BookAccumulator` is price-keyed today, so it cannot replay an order-level book. It also cannot produce a per-level order count, which the Part 2 sink needs for Hyperliquid's `WsLevelData.n`.

- [ ] **Step 1: Write the failing test**

```rust
/// The accumulator holds orders, and price levels are a fold over them — including the order
/// count per level, which a price-keyed accumulator structurally cannot produce and which the
/// Hyperliquid-compatible sink requires.
#[test]
fn the_accumulator_folds_orders_into_levels_with_counts() {
    let mut a = BookAccumulator::new("BTC".into());
    a.apply(&book_msg(vec![
        change(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
        change(BookAction::Update, BookSide::Bid, 100.0, 3.0, 2),
        change(BookAction::Update, BookSide::Bid, 99.0, 1.0, 3),
        change(BookAction::Update, BookSide::Ask, 101.0, 2.0, 4),
    ]));
    let (bids, asks) = a.price_fold();
    assert_eq!(bids, vec![(100.0, 8.0, 2), (99.0, 1.0, 1)], "two orders rest at 100");
    assert_eq!(asks, vec![(101.0, 2.0, 1)]);
}

/// An order removed at one price does not leave a phantom level behind.
#[test]
fn removing_the_last_order_at_a_price_removes_the_level() {
    let mut a = BookAccumulator::new("BTC".into());
    a.apply(&book_msg(vec![change(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1)]));
    a.apply(&book_msg(vec![change(BookAction::Delete, BookSide::Bid, 100.0, 0.0, 1)]));
    let (bids, _) = a.price_fold();
    assert!(bids.is_empty());
}
```

Add these helpers to `mod tests` in `src/model.rs`:

```rust
fn change(action: BookAction, side: BookSide, price: f64, size: f64, order_id: u64) -> BookChange {
    BookChange { action, side, price, size, order_id }
}

fn book_msg(changes: Vec<BookChange>) -> NormalizedBook {
    NormalizedBook {
        venue: "HYPERLIQUID".into(),
        source: "HYPERLIQUID".into(),
        source_id: 1,
        symbol: "BTC".into(),
        channel: 0,
        instrument_id: 1,
        changes,
        snapshot: false,
        last: true,
        source_ts_ns: 1,
        recv_ts_ns: 1,
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib model::tests::the_accumulator_folds_orders_into_levels_with_counts`
Expected: FAIL to compile — `no method named price_fold`.

- [ ] **Step 3: Implement**

On `BookAccumulator`, add an order map beside the existing price maps rather than replacing them — the Market-by-Price path still needs price keying, and a change with `order_id == 0` has no order identity to key by:

```rust
    /// Resting orders for an order-level (`L3`) market, keyed by the venue's order id. Empty for a
    /// price-aggregated market, whose changes carry `order_id == 0` and are held in `bids`/`asks`.
    orders: std::collections::HashMap<u64, (bool, i128, f64, f64)>,
```

storing `(is_bid, price_key, price, size)`. In `apply`, route each change by identity:

```rust
        for c in &b.changes {
            if c.order_id == 0 {
                // price-aggregated: existing bids/asks handling, unchanged
            } else if matches!(c.action, BookAction::Delete) || c.size == 0.0 {
                self.orders.remove(&c.order_id);
            } else {
                let key = price_key(c.price);
                self.orders.insert(c.order_id, (matches!(c.side, BookSide::Bid), key, c.price, c.size));
            }
        }
```

reusing whatever the existing `apply` uses to derive the `i128` price key. A `Clear` empties `orders` as well as `bids`/`asks`.

```rust
    /// Fold the accumulated orders into price levels with a count per level. Bids best-first, then
    /// asks best-first.
    pub fn price_fold(&self) -> (Vec<(f64, f64, u32)>, Vec<(f64, f64, u32)>) {
        let mut bids: BTreeMap<i128, (f64, f64, u32)> = BTreeMap::new();
        let mut asks: BTreeMap<i128, (f64, f64, u32)> = BTreeMap::new();
        for &(is_bid, key, price, size) in self.orders.values() {
            let side = if is_bid { &mut bids } else { &mut asks };
            let e = side.entry(key).or_insert((price, 0.0, 0));
            e.1 += size;
            e.2 += 1;
        }
        (
            bids.into_values().rev().collect(),
            asks.into_values().collect(),
        )
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib model::`
Expected: PASS, including the existing Market-by-Price accumulator tests, which are untouched because they carry `order_id == 0`.

- [ ] **Step 5: Commit**

```bash
git add src/model.rs
git commit -m "feat(model): accumulate orders and fold them into counted price levels"
```

---

### Task 6: metrics and the dedup window flag

**Files:**
- Modify: `src/metrics.rs`, `src/main.rs`
- Test: `src/metrics.rs`

**Interfaces:**
- Produces: `metrics().book_events_deduped` (`IntCounterVec`, label `venue`), `metrics().mbo_arm_disagreement` (`IntCounterVec`, label `venue`), and `Args.arb_book_dedup_window_ms: u64` defaulting to `250`.

- [ ] **Step 1: Write the failing test**

Extend the existing registry test in `src/metrics.rs` — it already asserts an expected-names list:

```rust
            "dz_book_events_deduped_total",
            "dz_mbo_arm_disagreement_total",
            "dz_mbo_removed_evicted_total",
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib metrics::tests::registry_encodes_and_contains_expected_names`
Expected: FAIL — the names are absent from the registry.

- [ ] **Step 3: Implement**

```rust
            book_events_deduped: counter_vec(
                &registry,
                "dz_book_events_deduped_total",
                "Order-level book events collapsed because another publisher delivered the same \
                 venue event first. In steady state this is the whole stream of every publisher \
                 but the fastest, so it is a throughput figure, not a fault.",
                &["venue"],
            ),
            mbo_arm_disagreement: counter_vec(
                &registry,
                "dz_mbo_arm_disagreement_total",
                "Two publishers reported the same venue event with different resulting state. \
                 The identity matched and the content did not, which is the signature of a book \
                 that has silently drifted. Any sustained non-zero rate is a correctness alarm.",
                &["venue"],
            ),
```

In `src/main.rs`, on `Args`:

```rust
    /// How long a delivered order-level book event is remembered so a slower publisher's copy is
    /// recognized as a duplicate. Correctness does not depend on this — a removed order id is
    /// never re-added regardless (see `ingest::book`) — so this trades memory against redundant
    /// work, not against a corrupt book.
    #[arg(long, env = "ARB_BOOK_DEDUP_WINDOW_MS", default_value_t = 250)]
    arb_book_dedup_window_ms: u64,
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib metrics:: && cargo test --bin doublezero-edge-connect`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/metrics.rs src/main.rs
git commit -m "feat(metrics): counters and a window flag for order-level book racing"
```

---

### Task 7: the order-event dedup oracle

**Files:**
- Modify: `src/ingest/arbiter.rs`
- Test: `src/ingest/arbiter.rs`

**Interfaces:**
- Consumes: `BookChange.order_id` (Task 1), `metrics().book_events_deduped` and `metrics().mbo_arm_disagreement` (Task 6), `Args.arb_book_dedup_window_ms` (Task 6).
- Produces: inside `Arbiter`, an order-event seen-map used by the `Book` arm when the venue's `ArbitrationMode` is `Coordinated`. No new public API; `emit(msg, publisher)` keeps its signature.

**The identity is `(venue, channel, instrument_id, order_id, action, price_bits)`** — `action` distinguishes an `Add` from the `Delete` that removes the same id, and the price bits distinguish successive partial fills of one order, which share id and action but differ in resulting size. Never the `per_instrument_seq`: it is per-publisher and meaningless across arms.

- [ ] **Step 1: Write the failing test**

```rust
/// Two publishers of a distributed venue deliver the same venue events. The first copy of each
/// is published and the rest collapse, so a consumer sees each event once and always from
/// whichever publisher was fastest for that event — best-of-N, per event.
#[test]
fn order_events_collapse_across_publishers_keeping_first_arrival() {
    let (a, mut rx) = coordinated_arbiter();
    let ev = |oid: u64, size: f64| order_book_msg("HYPERLIQUID", 1, oid, size);

    a.lock().emit(ev(7, 10.0), arm(1));
    a.lock().emit(ev(7, 10.0), arm(2)); // the slower publisher's copy
    a.lock().emit(ev(8, 4.0), arm(2));  // this one arm 2 won
    a.lock().emit(ev(8, 4.0), arm(1));

    let books = drain_books(&mut rx);
    assert_eq!(books.len(), 2, "each venue event reaches the wire exactly once");
    assert_eq!(books[0].changes[0].order_id, 7);
    assert_eq!(books[1].changes[0].order_id, 8);
}

/// Identity matching with content differing is the signature of a publisher whose book has
/// drifted. Exactly one copy still reaches the wire — we do not publish both — but the
/// disagreement is counted rather than silently collapsed.
#[test]
fn a_content_disagreement_is_counted_and_still_publishes_once() {
    let (a, mut rx) = coordinated_arbiter();
    let before = disagreements("HYPERLIQUID");

    a.lock().emit(order_book_msg("HYPERLIQUID", 1, 7, 10.0), arm(1));
    a.lock().emit(order_book_msg("HYPERLIQUID", 1, 7, 6.0), arm(2)); // same id, different remainder

    assert_eq!(drain_books(&mut rx).len(), 1);
    assert_eq!(disagreements("HYPERLIQUID"), before + 1);
}

/// With one live publisher the racing path is a pass-through: no stall, no waiting for a peer.
#[test]
fn a_single_publisher_streams_unimpeded() {
    let (a, mut rx) = coordinated_arbiter();
    for oid in 1..=5u64 {
        a.lock().emit(order_book_msg("HYPERLIQUID", 1, oid, 1.0), arm(1));
    }
    assert_eq!(drain_books(&mut rx).len(), 5);
}
```

Add these helpers to `mod tests` in `src/ingest/arbiter.rs`, following the shape of the existing `mbp_harness`/`drain_books` helpers:

```rust
/// An arbiter whose venues arbitrate in `Coordinated` mode — the racing model.
fn coordinated_arbiter() -> (SharedArbiter, broadcast::Receiver<Arc<FeedMessage>>) { /* mirror the existing harness, mode Coordinated */ }

/// One order-level book message carrying a single change.
fn order_book_msg(venue: &str, instrument_id: u32, order_id: u64, size: f64) -> FeedMessage { /* NormalizedBook with one Update change carrying order_id */ }

fn disagreements(venue: &str) -> u64 {
    metrics().mbo_arm_disagreement.with_label_values(&[venue]).get()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::arbiter::tests::order_events_collapse_across_publishers_keeping_first_arrival`
Expected: FAIL — both copies reach the wire, so the length is 4, not 2.

- [ ] **Step 3: Implement**

In `Arbiter`, add the seen-map and its bound:

```rust
/// Cap on remembered order events, independent of the time window, so a wedged or hostile
/// publisher cannot grow the map without limit.
const MAX_SEEN_ORDER_EVENTS: usize = 1 << 20;

/// An order-level book event's venue identity. `action` separates an add from the delete that
/// removes the same id; the price bits separate successive partial fills, which share id and
/// action. Never `per_instrument_seq` — that is per-publisher.
#[derive(PartialEq, Eq, Hash, Clone)]
struct OrderEventId {
    venue: Arc<str>,
    channel: u32,
    instrument_id: u32,
    order_id: u64,
    action: BookAction,
    price_bits: u64,
}
```

In the `Book` arm of `emit`, before the authority gate, branch on the venue's mode. For `Coordinated`, for each change carrying `order_id != 0`, build the id and look it up: unseen inserts `(size_bits, recv_ts_ns)` and admits; seen within the window collapses, and if the stored `size_bits` differs from this copy's, increments `mbo_arm_disagreement` first. Evict entries older than `arb_book_dedup_window_ms`, and evict oldest-first past `MAX_SEEN_ORDER_EVENTS`. A message all of whose changes collapse is dropped and counted on `book_events_deduped`; otherwise it is broadcast.

`Sticky` venues keep routing to `StickyAuthority` exactly as they do now — do not touch that path.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib ingest::arbiter::`
Expected: PASS, including every existing Market-by-Price authority test — those venues are `Sticky` and take the untouched path.

- [ ] **Step 5: Commit**

```bash
git add src/ingest/arbiter.rs
git commit -m "feat(arbiter): race order-level book events on venue identity"
```

---

### Task 8: re-baseline suppression

**Files:**
- Modify: `src/ingest/arbiter.rs`
- Test: `src/ingest/arbiter.rs`

**Interfaces:**
- Consumes: the seen-map from Task 7.
- Produces: `Arbiter::set_book_synced(venue: &str, channel: u32, instrument_id: u32, publisher: Publisher, synced: bool)` — the processor reports each book's sync state so the arbiter can decide whether a re-baseline is needed. No return value.

**Why:** a publisher recovering via snapshot emits a `Clear` plus its whole order set. Published unconditionally, that wipes a consumer whom a healthy peer is serving correctly. The decision must be made in **one** place: two publishers recovering together must not both conclude they are alone.

- [ ] **Step 1: Write the failing test**

```rust
/// A recovering publisher must not wipe a consumer that a healthy peer is serving. Its
/// re-baseline is dropped while any peer of the same market is synced.
#[test]
fn a_rebaseline_is_suppressed_while_a_peer_is_synced() {
    let (a, mut rx) = coordinated_arbiter();
    a.lock().set_book_synced("HYPERLIQUID", 0, 1, arm(1), true);
    a.lock().set_book_synced("HYPERLIQUID", 0, 1, arm(2), false);
    let _ = drain_books(&mut rx);

    a.lock().emit(rebaseline_msg("HYPERLIQUID", 1), arm(2));
    assert!(drain_books(&mut rx).is_empty(), "peer arm 1 is serving this market");
}

/// With every publisher recovering there is nothing to protect, so the re-baseline goes out —
/// and exactly once, however many publishers recover together.
#[test]
fn simultaneous_recoveries_produce_exactly_one_rebaseline() {
    let (a, mut rx) = coordinated_arbiter();
    a.lock().set_book_synced("HYPERLIQUID", 0, 1, arm(1), false);
    a.lock().set_book_synced("HYPERLIQUID", 0, 1, arm(2), false);
    let _ = drain_books(&mut rx);

    a.lock().emit(rebaseline_msg("HYPERLIQUID", 1), arm(1));
    a.lock().emit(rebaseline_msg("HYPERLIQUID", 1), arm(2));
    assert_eq!(drain_books(&mut rx).len(), 1);
}
```

Add to `mod tests`:

```rust
/// A `Clear`-led batch: the structural re-baseline, exactly what a recovering book emits.
fn rebaseline_msg(venue: &str, instrument_id: u32) -> FeedMessage { /* NormalizedBook whose changes[0].action == Clear, last: true */ }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::arbiter::tests::a_rebaseline_is_suppressed_while_a_peer_is_synced`
Expected: FAIL to compile — `no method named set_book_synced`.

- [ ] **Step 3: Implement**

Add a per-market sync map to `Arbiter`, keyed `(Arc<str>, u32, u32)` holding a `HashMap<Publisher, bool>`, bounded by the existing `MAX_BOOK_MARKETS` and evicted alongside the other per-market state. In the `Book` arm, when `changes[0].action == BookAction::Clear` and the venue is `Coordinated`:

- if any *other* publisher of that market is synced, drop the message and return;
- otherwise admit it and mark every publisher of that market not-synced, so a second simultaneous re-baseline sees one already published and drops.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib ingest::arbiter::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingest/arbiter.rs
git commit -m "feat(arbiter): publish one re-baseline, and only when no peer is serving"
```

---

### Task 9: `MboProcessor` emits `book`

**Files:**
- Modify: `src/ingest/processor.rs`
- Test: `src/ingest/processor.rs`

**Interfaces:**
- Consumes: `BookState::on_delta_reporting`, `BookState::order_set`, `OrderChange` (Tasks 2–4); `Arbiter::set_book_synced` (Task 8); `BookChange.order_id` (Task 1).
- Produces: no new API. `MboProcessor` gains a private `emit_book` called from the same places `emit_depth` is called, plus a reusable `Vec<OrderChange>` scratch buffer so the hot path does not allocate per event.

**Both products are emitted.** `emit_depth` is untouched and keeps running. This is the additive step the Global Constraints require.

- [ ] **Step 1: Write the failing test**

```rust
/// Market-by-Order now produces the order-level `book` alongside its existing `depth`. Every
/// change carries the venue's real order id — a zero there tells a consumer to aggregate by
/// price, silently degrading an L3 feed to L2.
#[test]
fn mbo_emits_order_level_book_alongside_depth() {
    let (arbiter, mut rx, instruments) = mbo_harness();
    let mut proc = MboProcessor::new(depth_snapshot(), tape(false));

    proc.on_datagram(
        &frame(&[enc_manifest_summary(1, 1), enc_instrument_def(0, "BTC", 1)]),
        &make_ctx(&arbiter, &instruments, PortRole::Combined),
    );
    proc.on_datagram(&frame(&[enc_snapshot_begin(&SnapshotBegin {
        instrument_id: 0, anchor_seq: 0, total_orders: 0, snapshot_id: 1, last_instrument_seq: 0, ts: 0,
    }), enc_snapshot_end(&SnapshotEnd { instrument_id: 0, anchor_seq: 0, snapshot_id: 1 })]),
        &make_ctx(&arbiter, &instruments, PortRole::Snapshot));
    proc.on_datagram(&frame(&[enc_order_add(&OrderAdd {
        instrument_id: 0, source_id: 1, side: SIDE_BID, order_flags: 0,
        per_instrument_seq: 1, order_id: 4242, enter_ts: 5000, price_raw: 100, qty_raw: 7,
    })]), &make_ctx(&arbiter, &instruments, PortRole::Mktdata));

    let msgs = drain_all(&mut rx);
    let book = msgs.iter().find_map(|m| match &**m {
        FeedMessage::Book(b) => Some(b.clone()),
        _ => None,
    }).expect("a book message must be emitted");
    assert_eq!(book.changes.len(), 1);
    assert_eq!(book.changes[0].order_id, 4242);
    assert_ne!(book.changes[0].order_id, 0, "a zero id degrades L3 to L2 in a consumer");
    assert!(msgs.iter().any(|m| matches!(&**m, FeedMessage::Depth(_))), "depth still flows");
}

/// A snapshot install re-baselines structurally: a `Clear` first, then the whole order set.
#[test]
fn a_snapshot_install_emits_clear_then_every_resting_order() {
    // Build a book from a snapshot carrying two orders, then assert the emitted batch is
    // [Clear, Update(order a), Update(order b)] with `snapshot` advisory-true and `last` true.
}
```

Fill the second test body following the first's harness: drive `SnapshotBegin`, two `SnapshotOrder`s and `SnapshotEnd`, then assert on the emitted `NormalizedBook`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ingest::processor::tests::mbo_emits_order_level_book_alongside_depth`
Expected: FAIL — no `FeedMessage::Book` is emitted, so the `expect` panics.

- [ ] **Step 3: Implement**

Give `MboProcessor` a scratch buffer field `order_changes: Vec<OrderChange>`, switch the delta path to `on_delta_reporting(op, &mut self.order_changes)`, and add:

```rust
    /// Emit the order-level `book` for one instrument. Scales the raw integers with the
    /// instrument's exponents and stamps the same identity triple `depth` uses. Gated on a
    /// resolved definition and a revealed Source ID exactly as `emit_depth` is — nothing reaches
    /// the wire for an instrument whose precision or source is still unknown.
    fn emit_book(&mut self, channel_id: u8, instrument_id: u32, snapshot: bool, ctx: &FrameCtx) {
        // resolve def + revealed source id (mirror emit_depth's gates)
        // map self.order_changes -> Vec<BookChange> via apply_exponent
        // prepend BookChange { action: Clear, side: Both, price: 0.0, size: 0.0, order_id: 0 } when snapshot
        // ctx.emit(FeedMessage::Book(NormalizedBook { .., snapshot, last: true, .. }))
    }
```

Call `emit_book` beside every existing `emit_depth` call. On a snapshot install, call `order_set` into the scratch buffer and pass `snapshot: true`. Report sync state to the arbiter on every `BookState` status transition via `Arbiter::set_book_synced`.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib ingest::processor:: && cargo test --test dedup`
Expected: PASS, including every existing `depth` test — this task adds a product, it does not change one.

- [ ] **Step 5: Commit**

```bash
git add src/ingest/processor.rs
git commit -m "feat(ingest): emit the order-level book from market-by-order"
```

---

### Task 10: replay scope on the WebSocket

**Files:**
- Modify: `src/sinks/ws.rs`, `src/model.rs`
- Test: `src/sinks/ws.rs`

**Interfaces:**
- Consumes: `BookAccumulator::price_fold` (Task 5).
- Produces: `BookAccumulator::to_book` gains a `scope: ReplayScope` argument, where `pub enum ReplayScope { Orders, Levels }`. The `subscribe` control frame gains an optional `"book_scope": "orders" | "levels"`, defaulting to `Levels`.

- [ ] **Step 1: Write the failing test**

```rust
/// A connecting client is bootstrapped with price levels by default, so an L2 consumer never
/// pays for a 44k-order burst. Asking for order scope gets the full order set instead.
#[test]
fn replay_scope_defaults_to_levels_and_orders_on_request() {
    let mut acc = BookAccumulator::new("BTC".into());
    acc.apply(&book_msg(vec![
        change(BookAction::Update, BookSide::Bid, 100.0, 5.0, 1),
        change(BookAction::Update, BookSide::Bid, 100.0, 3.0, 2),
    ]));

    let levels = acc.to_book(&"HYPERLIQUID".into(), 0, 1, ReplayScope::Levels);
    assert_eq!(levels.changes.iter().filter(|c| c.action != BookAction::Clear).count(), 1,
        "two orders at one price fold to one level");
    assert!(levels.changes.iter().all(|c| c.order_id == 0), "a level carries no order identity");

    let orders = acc.to_book(&"HYPERLIQUID".into(), 0, 1, ReplayScope::Orders);
    assert_eq!(orders.changes.iter().filter(|c| c.action != BookAction::Clear).count(), 2);
    assert!(orders.changes.iter().filter(|c| c.action != BookAction::Clear).all(|c| c.order_id != 0));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sinks::ws::tests::replay_scope_defaults_to_levels_and_orders_on_request`
Expected: FAIL to compile — `ReplayScope` not found, `to_book` takes three arguments.

- [ ] **Step 3: Implement**

Add `ReplayScope` to `src/model.rs`, give `to_book` the argument, and materialize from `price_fold()` for `Levels` and from `orders` for `Orders`. In `src/sinks/ws.rs`, parse `book_scope` on `subscribe`, store it per client, and pass it wherever `to_book` is called on the replay path. Every existing call site passes `ReplayScope::Levels`, preserving today's behaviour for the Market-by-Price venue.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib sinks::ws::`
Expected: PASS, including the existing replay and channel-filter tests.

- [ ] **Step 5: Commit**

```bash
git add src/sinks/ws.rs src/model.rs
git commit -m "feat(ws): scope the book replay to orders or levels"
```

---

### Task 11: documentation

**Files:**
- Modify: `PROTOCOL.md`, `docs/input-sources.md`, `docs/metrics.md`, `CHANGELOG.md`, `CLAUDE.md`

- [ ] **Step 1: PROTOCOL.md — additive only, still v1**

Document `order_id` on a `book` change (`0` = price-aggregated, non-zero = the venue's order id); document `book_scope` on `subscribe` with its `levels` default; and state in the message table and connection-lifecycle section that Market-by-Order now produces `book` **as well as** `depth`. Do not change the version line — nothing is withdrawn.

- [ ] **Step 2: docs/metrics.md**

Add `dz_book_events_deduped_total`, `dz_mbo_arm_disagreement_total` and `dz_mbo_removed_evicted_total`, each with what a non-zero value means. Say plainly that a sustained non-zero `dz_mbo_arm_disagreement_total` is a correctness alarm and the trigger to reconsider the shared-book model.

- [ ] **Step 3: docs/input-sources.md**

Note that Market-by-Order emits the order-level `book` and is raced across publishers on venue event identity, and that `depth` continues unchanged.

- [ ] **Step 4: CHANGELOG.md**

Under Unreleased → Added: the order-level `book` product from Market-by-Order, the `order_id` field, the `book_scope` subscription field, and the racing metrics. Four lines.

- [ ] **Step 5: CLAUDE.md**

Update the `ingest/book.rs`, `ingest/arbiter.rs` and `ingest/processor.rs` bullets: books stay per publisher, order events race on venue identity, the removed-order guard carries correctness rather than the window.

- [ ] **Step 6: Run everything and commit**

```bash
cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check
git add -A
git commit -m "docs: order-level book from market-by-order"
```

---

## Self-review

**Spec coverage.** `order_id` on the wire → Task 1. Order-level change reporting → Task 2. The per-order guard the design puts correctness on → Task 3. `Clear`-led re-baseline content → Task 4. Order-keyed accumulator and the price fold with counts (which Part 2 needs for `WsLevelData.n`) → Task 5. The 250ms settable window → Task 6. Racing on venue identity plus the disagreement counter → Task 7. Re-baseline suppression, decided once → Task 8. Emission alongside `depth` → Task 9. Replay scoping → Task 10. The additive PROTOCOL.md edits → Task 11.

**Deliberately not here.** The `depth` deletion and the PROTOCOL.md v2 flip (deferred by the design; testers are on `depth`). The Hyperliquid sink (Part 2). The venue-timestamp staleness filter — it is a cost optimization on top of a guard that already holds, so it belongs with Part 2's measurement work rather than blocking this.

**Type consistency.** `OrderChange { order_id, is_bid, price_raw, qty_raw }` is defined in Task 2 and used in Tasks 3, 4 and 9. `BookChange.order_id` is defined in Task 1 and used in Tasks 5, 7, 9 and 10. `ReplayScope` is defined in Task 10 and used only there. `Arbiter::set_book_synced` is defined in Task 8 and called in Task 9. `price_fold` is defined in Task 5 and used in Task 10 and in Part 2.

**Known soft spots.** Task 9's `emit_book` body and its second test are described rather than fully written, because both must mirror `emit_depth`'s gating exactly and that code is long and will have moved if #109/#110 land first — an implementer should read `emit_depth` and follow it rather than copy a snapshot of it from here. Task 7's helper bodies are likewise described: they mirror the existing `mbp_harness` helpers in the same module.
