//! L3 order-book reconstruction with snapshot+delta recovery for the Market-by-Order feed.
//!
//! A [`BookState`] holds one instrument's resting-order population and derives top-N price levels.
//! It implements the edge-feed-spec recovery model using the **per-instrument delta sequence** and
//! the **snapshot anchor**: deltas apply only in unbroken sequence; a gap drops the instrument to
//! `Recovering`, where deltas are buffered until a snapshot re-establishes the book, after which
//! the buffered deltas past the snapshot's `last_instrument_seq` are replayed.
//!
//! The type is deliberately **codec-agnostic** (it speaks [`DeltaOp`]/raw integers, not wire
//! structs) so the recovery logic is unit-testable in isolation; `MboProcessor` adapts decoded
//! `codec_mbo` messages into these calls and scales the raw prices/sizes by the instrument's
//! exponents when emitting `depth`.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::metrics::metrics;

/// Cap on resting orders held per instrument book — both the synced population and the in-flight
/// snapshot under assembly. A real instrument's book is far smaller; this only bounds a forged or
/// garbage book built from spoofed MBO deltas/snapshots so a single `instrument_id` can't grow
/// memory without limit. Reaching it marks the book anomalous: it drops to `Recovering` and
/// re-syncs from the next snapshot rather than growing further.
const MAX_ORDERS_PER_BOOK: usize = 1 << 18;

/// Cap on deltas buffered while `Recovering` (awaiting a snapshot). In normal operation the buffer
/// holds at most one snapshot cycle of deltas; this bounds a flood of deltas for an instrument that
/// never receives a snapshot. Excess deltas are dropped — the book re-anchors on the next snapshot
/// regardless of which buffered deltas survived.
const MAX_PENDING_DELTAS: usize = 1 << 18;

/// Cap on remembered removed order ids. The wire is unauthenticated, so this cannot be unbounded;
/// past the cap the oldest id is forgotten, which reopens the resurrection path only for a copy
/// arriving later than this many removals. Counted by `dz_mbo_removed_evicted_total`.
const MAX_REMOVED_ORDERS: usize = 1 << 20;

/// One aggregated price level as `(price_raw, qty_raw)` - raw integers in the instrument's
/// price/qty exponents (the caller scales them).
pub type Level = (i64, u64);

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

/// One resting order's mutable state.
#[derive(Debug, Clone)]
struct RestingOrder {
    is_bid: bool,
    price_raw: i64,
    qty_raw: u64,
}

/// What a delta does to the book. `seq` is the per-instrument delta sequence; `mktdata_seq` the
/// frame-level `mktdata`-port sequence that carried it (the anchor the spec keys snapshot replay on);
/// `ts` the event time.
#[derive(Debug, Clone)]
pub struct DeltaOp {
    pub seq: u32,
    pub mktdata_seq: u64,
    pub ts: u64,
    pub kind: DeltaKind,
}

#[derive(Debug, Clone)]
pub enum DeltaKind {
    Add {
        order_id: u64,
        is_bid: bool,
        price_raw: i64,
        qty_raw: u64,
    },
    Cancel {
        order_id: u64,
    },
    Execute {
        order_id: u64,
        exec_qty_raw: u64,
        full_fill: bool,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum SyncState {
    /// Awaiting (or assembling) a snapshot; live deltas are buffered, not applied.
    Recovering,
    /// In sync: deltas apply in sequence.
    Synced,
}

/// A snapshot being assembled between `SnapshotBegin` and `SnapshotEnd`.
struct Building {
    snapshot_id: u32,
    /// `mktdata`-port sequence the snapshot was captured at; carried by both begin and end and used
    /// as the post-snapshot `last_applied_mktdata_seq`.
    anchor_seq: u64,
    /// Number of `SnapshotOrder` messages promised by the begin; the end is rejected unless exactly
    /// this many arrived (guards against installing a snapshot truncated by packet loss).
    total_orders: u32,
    received_orders: u32,
    last_instrument_seq: u32,
    orders: HashMap<u64, RestingOrder>,
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
}

pub struct BookState {
    orders: HashMap<u64, RestingOrder>,
    /// price_raw -> total resting qty at that level, per side.
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    state: SyncState,
    /// Last applied per-instrument delta sequence.
    last_instr_seq: u32,
    /// `mktdata`-port sequence of the last applied delta / installed snapshot anchor. The spec keys
    /// snapshot dedup ("snapshot while ready") and buffered-delta replay on this, not on the
    /// per-instrument seq.
    last_applied_mktdata_seq: u64,
    /// Snapshot under assembly (between begin and end).
    building: Option<Building>,
    /// Deltas buffered while `Recovering`, replayed after the next snapshot.
    pending: Vec<DeltaOp>,
    /// Order ids this book has removed. A venue never reuses one, so an `Add` naming an id in here is
    /// a stale racing copy and must not resurrect the order.
    removed: HashSet<u64>,
    /// `removed` in removal order, oldest first, so the set can be bounded.
    removed_order: VecDeque<u64>,
    /// Timestamp of the most recent applied event (for the emitted depth's `source_ts_ns`).
    last_event_ts: u64,
}

/// Add `qty` to a price level.
fn level_add(levels: &mut BTreeMap<i64, u64>, price: i64, qty: u64) {
    *levels.entry(price).or_insert(0) += qty;
}

/// Remove up to `qty` from a price level, dropping the level when it reaches zero.
fn level_remove(levels: &mut BTreeMap<i64, u64>, price: i64, qty: u64) {
    if let Some(total) = levels.get_mut(&price) {
        *total = total.saturating_sub(qty);
        if *total == 0 {
            levels.remove(&price);
        }
    }
}

impl Default for BookState {
    fn default() -> Self {
        Self::new()
    }
}

impl BookState {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            state: SyncState::Recovering,
            last_instr_seq: 0,
            last_applied_mktdata_seq: 0,
            building: None,
            pending: Vec::new(),
            removed: HashSet::new(),
            removed_order: VecDeque::new(),
            last_event_ts: 0,
        }
    }

    pub fn is_synced(&self) -> bool {
        self.state == SyncState::Synced
    }

    #[cfg(test)]
    pub(crate) fn has_order(&self, order_id: u64) -> bool {
        self.orders.contains_key(&order_id)
    }

    #[cfg(test)]
    pub(crate) fn removed_len(&self) -> usize {
        self.removed.len()
    }

    pub fn last_event_ts(&self) -> u64 {
        self.last_event_ts
    }

    /// `EndOfSession`: the session's book, sequences and event clock are all over; drop everything
    /// and await the next session's snapshot. Unlike [`Self::on_instrument_reset`] there is no
    /// forward anchor — buffered deltas belong to the ended session and are discarded outright
    /// (replaying them against a new-session snapshot whose `anchor_seq` restarted low would apply
    /// stale data). Dropping to `Recovering` also skips the `Synced` "snapshot while ready"
    /// staleness check, so a new-session snapshot with a restarted (small) `anchor_seq` — which a
    /// still-`Synced` book would ignore as stale — re-bootstraps cleanly, and zeroing
    /// `last_event_ts` keeps the resync from stamping its first depth with pre-session time (which
    /// would re-latch the arbiter's depth floor at the old high-water right after the
    /// session-reset escape hatch cleared it).
    pub fn on_end_of_session(&mut self) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        self.building = None;
        self.pending.clear();
        self.clear_removed();
        self.state = SyncState::Recovering;
        self.last_instr_seq = 0;
        self.last_applied_mktdata_seq = 0;
        self.last_event_ts = 0;
    }

    /// `InstrumentReset(new_anchor_seq = S')`: drop the book and any open snapshot and await a fresh
    /// snapshot anchored at `S'`. Buffered deltas at/below `S'` are discarded (the reset supersedes
    /// them); those past `S'` are retained for post-snapshot replay.
    pub fn on_instrument_reset(&mut self, new_anchor_seq: u64) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        self.building = None;
        self.pending.retain(|d| d.mktdata_seq > new_anchor_seq);
        self.clear_removed();
        self.state = SyncState::Recovering;
        // The dropped book's event clock goes with it: the re-synced book must not stamp its first
        // depth with the pre-reset time, or (after a venue clock restart) that stale stamp would
        // re-latch the arbiter's depth floor at the old high-water right after the escape-hatch
        // reset cleared it — re-wedging the symbol the reset was meant to unwedge.
        self.last_event_ts = 0;
    }

    /// Begin assembling a snapshot. A new begin supersedes any half-built one. When the book is
    /// already `Synced` (a routine round-robin snapshot — "snapshot while ready"), a snapshot whose
    /// `anchor_seq` is at/below what we have already applied is stale and ignored, so it cannot
    /// clobber a healthy book or rewind its sequence; only a snapshot strictly ahead re-bootstraps.
    pub fn on_snapshot_begin(
        &mut self,
        snapshot_id: u32,
        anchor_seq: u64,
        total_orders: u32,
        last_instrument_seq: u32,
    ) {
        if self.state == SyncState::Synced && anchor_seq <= self.last_applied_mktdata_seq {
            self.building = None; // already advanced past this snapshot; drop it
            return;
        }
        if total_orders as usize > MAX_ORDERS_PER_BOOK {
            // A snapshot promising more orders than any real book holds is malformed or forged;
            // refuse to assemble it so `building.orders` stays bounded.
            self.building = None;
            return;
        }
        self.building = Some(Building {
            snapshot_id,
            anchor_seq,
            total_orders,
            received_orders: 0,
            last_instrument_seq,
            orders: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        });
    }

    /// Add one resting order to the snapshot under assembly (ignored if the id doesn't match).
    pub fn on_snapshot_order(
        &mut self,
        snapshot_id: u32,
        order_id: u64,
        is_bid: bool,
        price_raw: i64,
        qty_raw: u64,
    ) {
        if let Some(b) = &mut self.building {
            if b.snapshot_id == snapshot_id {
                if b.orders.len() >= MAX_ORDERS_PER_BOOK {
                    return; // refuse to grow the in-flight snapshot beyond the cap
                }
                b.orders.insert(
                    order_id,
                    RestingOrder {
                        is_bid,
                        price_raw,
                        qty_raw,
                    },
                );
                b.received_orders += 1;
                let levels = if is_bid { &mut b.bids } else { &mut b.asks };
                level_add(levels, price_raw, qty_raw);
            }
        }
    }

    /// Finalize the assembled snapshot: install it as the live book, set both anchors, then replay
    /// any buffered deltas past it. Returns whether the book changed. The snapshot is rejected
    /// (book reverts to `Recovering`) unless its `snapshot_id`/`anchor_seq` match the opening begin
    /// and exactly `total_orders` orders arrived — guarding against installing a truncated snapshot.
    pub fn on_snapshot_end(&mut self, anchor_seq: u64, snapshot_id: u32) -> bool {
        let Some(b) = self.building.take() else {
            return false;
        };
        if b.snapshot_id != snapshot_id
            || b.anchor_seq != anchor_seq
            || b.received_orders != b.total_orders
        {
            // Mismatched or incomplete (lossy) snapshot: discard it and await the next one.
            self.state = SyncState::Recovering;
            return false;
        }
        self.orders = b.orders;
        self.bids = b.bids;
        self.asks = b.asks;
        self.last_instr_seq = b.last_instrument_seq;
        self.last_applied_mktdata_seq = b.anchor_seq;
        self.state = SyncState::Synced;
        self.clear_removed(); // a snapshot is a fresh id space

        // Replay buffered deltas in mktdata-seq order; those at/below the anchor are already
        // included in the snapshot and dropped, per the spec's "discard mktdata_seq <= S" rule.
        let mut pending = std::mem::take(&mut self.pending);
        pending.retain(|d| d.mktdata_seq > b.anchor_seq);
        pending.sort_by_key(|d| d.mktdata_seq);
        // The caller re-materializes the whole book via `order_set`, so replayed changes go nowhere.
        let mut discard = Vec::new();
        for d in pending {
            self.apply_or_recover(d, &mut discard);
            if self.state == SyncState::Recovering {
                break; // a gap in the buffered deltas; await the next snapshot
            }
        }
        true
    }

    /// [`Self::on_delta_reporting`] for a caller that only needs to know whether the delta applied.
    #[cfg(test)]
    pub(crate) fn on_delta(&mut self, op: DeltaOp) -> bool {
        self.on_delta_reporting(op, &mut Vec::new())
    }

    /// Apply a delta, appending the order-level change it produced. While `Recovering`, deltas are
    /// buffered (returns `false`). While `Synced`, the per-instrument sequence is enforced: a
    /// duplicate/old delta is ignored, the next contiguous one is applied, and a forward gap drops to
    /// `Recovering`. Returns whether the top-of-book-bearing levels may have changed (i.e. a delta was
    /// actually applied), and reports nothing for an event the book rejects — so a caller cannot
    /// publish a change the book did not make.
    pub fn on_delta_reporting(&mut self, op: DeltaOp, out: &mut Vec<OrderChange>) -> bool {
        match self.state {
            SyncState::Recovering => {
                if self.pending.len() < MAX_PENDING_DELTAS {
                    self.pending.push(op);
                }
                // else: drop; the book re-anchors on the next snapshot regardless.
                false
            }
            SyncState::Synced => self.apply_or_recover(op, out),
        }
    }

    fn apply_or_recover(&mut self, op: DeltaOp, out: &mut Vec<OrderChange>) -> bool {
        if op.seq <= self.last_instr_seq {
            return false; // duplicate or already-applied
        }
        if op.seq != self.last_instr_seq + 1 {
            // Per-instrument gap: drop to recovering and buffer this delta for post-snapshot replay.
            self.state = SyncState::Recovering;
            self.pending.clear();
            self.pending.push(op);
            return false;
        }
        self.last_instr_seq = op.seq;
        self.last_applied_mktdata_seq = op.mktdata_seq;
        self.last_event_ts = op.ts;
        self.apply(op.kind, out)
    }

    /// Mutate the book, appending the resulting order-level change; returns whether it applied.
    fn apply(&mut self, kind: DeltaKind, out: &mut Vec<OrderChange>) -> bool {
        match kind {
            DeltaKind::Add {
                order_id,
                is_bid,
                price_raw,
                qty_raw,
            } => {
                // Refused here rather than before the sequence bookkeeping: the rejected event still
                // consumes its sequence number, or the next contiguous delta reads as a gap.
                if self.removed.contains(&order_id) {
                    return false;
                }
                if self.orders.len() >= MAX_ORDERS_PER_BOOK && !self.orders.contains_key(&order_id)
                {
                    // Oversized book — only reachable via a flood of forged adds. Drop it to
                    // `Recovering` rather than growing without limit; the next snapshot
                    // re-establishes a clean book.
                    self.orders.clear();
                    self.bids.clear();
                    self.asks.clear();
                    self.state = SyncState::Recovering;
                    return true;
                }
                self.orders.insert(
                    order_id,
                    RestingOrder {
                        is_bid,
                        price_raw,
                        qty_raw,
                    },
                );
                let levels = if is_bid {
                    &mut self.bids
                } else {
                    &mut self.asks
                };
                level_add(levels, price_raw, qty_raw);
                out.push(OrderChange {
                    order_id,
                    is_bid,
                    price_raw,
                    qty_raw,
                });
                true
            }
            DeltaKind::Cancel { order_id } => {
                if let Some(o) = self.orders.remove(&order_id) {
                    let levels = if o.is_bid {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    };
                    level_remove(levels, o.price_raw, o.qty_raw);
                    out.push(OrderChange {
                        order_id,
                        is_bid: o.is_bid,
                        price_raw: o.price_raw,
                        qty_raw: 0,
                    });
                    self.note_removed(order_id);
                }
                true
            }
            DeltaKind::Execute {
                order_id,
                exec_qty_raw,
                full_fill,
            } => {
                if let Some(o) = self.orders.get_mut(&order_id) {
                    let is_bid = o.is_bid;
                    let price = o.price_raw;
                    let filled = exec_qty_raw.min(o.qty_raw);
                    o.qty_raw -= filled;
                    let remaining = o.qty_raw;
                    let remove = full_fill || o.qty_raw == 0;
                    if remove {
                        self.orders.remove(&order_id);
                    }
                    let levels = if is_bid {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    };
                    level_remove(levels, price, filled);
                    out.push(OrderChange {
                        order_id,
                        is_bid,
                        price_raw: price,
                        qty_raw: if remove { 0 } else { remaining },
                    });
                    if remove {
                        self.note_removed(order_id);
                    }
                }
                true
            }
        }
    }

    /// Remember a removed order id, forgetting the oldest past [`MAX_REMOVED_ORDERS`].
    fn note_removed(&mut self, order_id: u64) {
        if !self.removed.insert(order_id) {
            return;
        }
        self.removed_order.push_back(order_id);
        while self.removed_order.len() > MAX_REMOVED_ORDERS {
            if let Some(oldest) = self.removed_order.pop_front() {
                self.removed.remove(&oldest);
                metrics().mbo_removed_evicted.inc();
            }
        }
    }

    fn clear_removed(&mut self) {
        self.removed.clear();
        self.removed_order.clear();
    }

    /// Every resting order, bids best-first then asks best-first, ties broken by order id. The
    /// ordering is deterministic on purpose: two publishers materializing the same book must produce
    /// identical lists, or the arbiter's content comparison flags map iteration order as a
    /// disagreement.
    pub fn order_set(&self, out: &mut Vec<OrderChange>) {
        out.clear();
        out.extend(self.orders.iter().map(|(&order_id, o)| OrderChange {
            order_id,
            is_bid: o.is_bid,
            price_raw: o.price_raw,
            qty_raw: o.qty_raw,
        }));
        out.sort_unstable_by(|a, b| {
            b.is_bid
                .cmp(&a.is_bid)
                .then_with(|| {
                    if a.is_bid {
                        b.price_raw.cmp(&a.price_raw)
                    } else {
                        a.price_raw.cmp(&b.price_raw)
                    }
                })
                .then_with(|| a.order_id.cmp(&b.order_id))
        });
    }

    /// Top-`n` price levels per side as `(price_raw, qty_raw)`, best first: bids high→low, asks
    /// low→high. Empty while `Recovering` is not enforced here - callers gate on [`Self::is_synced`].
    pub fn top_levels(&self, n: usize) -> (Vec<Level>, Vec<Level>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &q)| (p, q))
            .collect();
        let asks = self.asks.iter().take(n).map(|(&p, &q)| (p, q)).collect();
        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An `Add` delta whose per-instrument seq and `mktdata_seq` are aligned (the steady-state case:
    /// one delta per mktdata frame). Tests that need them to differ build the `DeltaOp` directly.
    fn add(seq: u32, order_id: u64, is_bid: bool, price: i64, qty: u64) -> DeltaOp {
        DeltaOp {
            seq,
            mktdata_seq: seq as u64,
            ts: seq as u64,
            kind: DeltaKind::Add {
                order_id,
                is_bid,
                price_raw: price,
                qty_raw: qty,
            },
        }
    }

    fn cancel_op(seq: u32, order_id: u64) -> DeltaOp {
        DeltaOp {
            seq,
            mktdata_seq: seq as u64,
            ts: seq as u64,
            kind: DeltaKind::Cancel { order_id },
        }
    }

    fn exec_op(seq: u32, order_id: u64, exec_qty_raw: u64, full_fill: bool) -> DeltaOp {
        DeltaOp {
            seq,
            mktdata_seq: seq as u64,
            ts: seq as u64,
            kind: DeltaKind::Execute {
                order_id,
                exec_qty_raw,
                full_fill,
            },
        }
    }

    /// A fully-recovered book: begin, two orders, end (anchor seq = 0 so deltas start at 1).
    fn synced_book() -> BookState {
        let mut b = BookState::new();
        assert!(!b.is_synced());
        b.on_snapshot_begin(1, 0, 2, 0); // snapshot_id=1, anchor_seq=0, total_orders=2, last_instr_seq=0
        b.on_snapshot_order(1, 100, true, 18_400, 5); // bid 184.00 x5
        b.on_snapshot_order(1, 200, false, 18_410, 8); // ask 184.10 x8
        assert!(b.on_snapshot_end(0, 1)); // anchor_seq=0, snapshot_id=1
        assert!(b.is_synced());
        b
    }

    /// A synced book with no resting orders, so deltas start from a clean id space at seq 1.
    fn synced_empty_book() -> BookState {
        let mut b = BookState::new();
        b.on_snapshot_begin(1, 0, 0, 0);
        assert!(b.on_snapshot_end(0, 1));
        b
    }

    #[test]
    fn deltas_report_the_order_they_touched() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();

        assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));
        assert_eq!(
            out,
            vec![OrderChange {
                order_id: 7,
                is_bid: true,
                price_raw: 18_400,
                qty_raw: 5,
            }]
        );

        out.clear();
        assert!(b.on_delta_reporting(exec_op(2, 7, 2, false), &mut out));
        assert_eq!(
            out,
            vec![OrderChange {
                order_id: 7,
                is_bid: true,
                price_raw: 18_400,
                qty_raw: 3, // remaining, not the executed amount
            }]
        );

        out.clear();
        assert!(b.on_delta_reporting(cancel_op(3, 7), &mut out));
        assert_eq!(
            out,
            vec![OrderChange {
                order_id: 7,
                is_bid: true,
                price_raw: 18_400,
                qty_raw: 0,
            }]
        );
    }

    #[test]
    fn a_rejected_delta_reports_nothing() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));

        out.clear();
        assert!(!b.on_delta_reporting(add(1, 8, true, 18_401, 1), &mut out));
        assert!(out.is_empty());
    }

    /// A venue never reuses an order id, so an `Add` naming a removed one is a stale racing copy: the
    /// order must not come back, nothing may be reported, and the *next* legitimate contiguous delta
    /// must still apply (the refusal consumed its sequence number).
    #[test]
    fn a_removed_order_is_never_resurrected() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));
        assert!(b.on_delta_reporting(cancel_op(2, 7), &mut out));

        out.clear();
        assert!(!b.on_delta_reporting(add(3, 7, true, 18_400, 5), &mut out));
        assert!(out.is_empty());
        assert!(!b.has_order(7));

        assert!(b.on_delta_reporting(add(4, 8, true, 18_390, 1), &mut out));
        assert!(b.is_synced());
        assert!(b.has_order(8));
    }

    #[test]
    fn a_fully_executed_order_is_never_resurrected() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));
        assert!(b.on_delta_reporting(exec_op(2, 7, 5, true), &mut out));

        out.clear();
        assert!(!b.on_delta_reporting(add(3, 7, true, 18_400, 5), &mut out));
        assert!(out.is_empty());
        assert!(!b.has_order(7));

        assert!(b.on_delta_reporting(add(4, 8, true, 18_390, 1), &mut out));
        assert!(b.is_synced());
        assert!(b.has_order(8));
    }

    /// The resurrection guard's memory is bounded: the wire is unauthenticated, so a churn of
    /// add/cancel pairs must not grow it without limit.
    #[test]
    fn the_removed_set_is_bounded() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        let mut seq = 0u32;
        for order_id in 0..(MAX_REMOVED_ORDERS as u64 + 1_000) {
            seq += 1;
            assert!(b.on_delta_reporting(add(seq, order_id, true, 100, 1), &mut out));
            seq += 1;
            assert!(b.on_delta_reporting(cancel_op(seq, order_id), &mut out));
            out.clear();
        }
        assert!(
            b.removed_len() <= MAX_REMOVED_ORDERS,
            "removed ids must stay bounded, got {}",
            b.removed_len()
        );
    }

    /// A session end or instrument reset restarts the venue's id space, so a previously removed id
    /// must be addable again.
    #[test]
    fn a_session_reset_reopens_the_id_space() {
        for end_of_session in [true, false] {
            let mut b = synced_empty_book();
            let mut out = Vec::new();
            assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));
            assert!(b.on_delta_reporting(cancel_op(2, 7), &mut out));

            if end_of_session {
                b.on_end_of_session();
            } else {
                b.on_instrument_reset(0);
            }
            assert_eq!(b.removed_len(), 0);

            b.on_snapshot_begin(2, 0, 0, 0);
            assert!(b.on_snapshot_end(0, 2));
            out.clear();
            assert!(b.on_delta_reporting(add(1, 7, true, 18_400, 5), &mut out));
            assert!(b.has_order(7), "end_of_session={end_of_session}");
        }
    }

    #[test]
    fn order_set_is_complete_and_deterministically_ordered() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        assert!(b.on_delta_reporting(add(1, 10, true, 101, 1), &mut out));
        assert!(b.on_delta_reporting(add(2, 20, false, 105, 2), &mut out));
        assert!(b.on_delta_reporting(add(3, 30, true, 100, 3), &mut out));
        assert!(b.on_delta_reporting(add(4, 40, true, 100, 4), &mut out));

        b.order_set(&mut out);
        assert_eq!(
            out,
            vec![
                OrderChange {
                    order_id: 10,
                    is_bid: true,
                    price_raw: 101,
                    qty_raw: 1,
                },
                OrderChange {
                    order_id: 30,
                    is_bid: true,
                    price_raw: 100,
                    qty_raw: 3,
                },
                OrderChange {
                    order_id: 40,
                    is_bid: true,
                    price_raw: 100,
                    qty_raw: 4,
                },
                OrderChange {
                    order_id: 20,
                    is_bid: false,
                    price_raw: 105,
                    qty_raw: 2,
                },
            ]
        );
    }

    /// A forged extreme `price_raw` must not panic the ordering (negating `i64::MIN` overflows).
    #[test]
    fn order_set_tolerates_extreme_prices() {
        let mut b = synced_empty_book();
        let mut out = Vec::new();
        assert!(b.on_delta_reporting(add(1, 1, true, i64::MIN, 1), &mut out));
        assert!(b.on_delta_reporting(add(2, 2, true, i64::MAX, 1), &mut out));
        assert!(b.on_delta_reporting(add(3, 3, false, i64::MIN, 1), &mut out));

        b.order_set(&mut out);
        let ids: Vec<u64> = out.iter().map(|c| c.order_id).collect();
        assert_eq!(ids, vec![2, 1, 3]); // bids high→low, then the ask
    }

    /// `EndOfSession` drops everything — book, sequences, buffered deltas, event clock — so the
    /// next session's snapshot re-bootstraps even with a restarted (small) `anchor_seq`, no
    /// old-session buffered delta replays into it, and the resync depth can't be stamped with
    /// pre-session time (#66).
    #[test]
    fn end_of_session_drops_book_sequences_and_event_clock() {
        let mut b = synced_book();
        assert!(b.on_delta(add(1, 101, true, 18_405, 3))); // applied: last_event_ts = 1
        assert!(!b.on_delta(add(5, 102, true, 18_406, 1))); // gap -> Recovering, delta buffered
        b.on_end_of_session();
        assert!(!b.is_synced());
        assert_eq!(b.last_event_ts(), 0);
        // A new-session snapshot with a restarted (small) anchor re-bootstraps: a still-`Synced`
        // book would have ignored it as "snapshot while ready".
        b.on_snapshot_begin(2, 0, 1, 0);
        b.on_snapshot_order(2, 300, true, 100, 1);
        assert!(b.on_snapshot_end(0, 2));
        assert!(b.is_synced());
        // Old-session buffered deltas were discarded: the book holds exactly the new snapshot.
        let (bids, asks) = b.top_levels(5);
        assert_eq!(bids, vec![(100, 1)]);
        assert_eq!(asks, vec![]);
        // And the restarted per-instrument sequence applies from the new snapshot's seq.
        assert!(b.on_delta(add(1, 301, true, 101, 2)));
        assert_eq!(b.last_event_ts(), 1);
    }

    /// `InstrumentReset` drops the book's event clock along with the book: the re-synced book's
    /// first depth must not be stamped with the pre-reset time, or (after a venue clock restart)
    /// that stale stamp would re-latch the arbiter's depth floor right after the escape-hatch
    /// reset cleared it (#66).
    #[test]
    fn instrument_reset_drops_the_event_clock() {
        let mut b = synced_book();
        assert!(b.on_delta(add(1, 101, true, 18_405, 3)));
        assert_eq!(b.last_event_ts(), 1);
        b.on_instrument_reset(1);
        assert_eq!(
            b.last_event_ts(),
            0,
            "the pre-reset event time must not survive the reset"
        );
    }

    #[test]
    fn snapshot_then_contiguous_deltas_update_top_of_book() {
        let mut b = synced_book();
        let (bids, asks) = b.top_levels(5);
        assert_eq!(bids, vec![(18_400, 5)]);
        assert_eq!(asks, vec![(18_410, 8)]);

        // A better bid arrives (seq 1, contiguous after anchor 0).
        assert!(b.on_delta(add(1, 101, true, 18_405, 3)));
        let (bids, _) = b.top_levels(5);
        assert_eq!(bids, vec![(18_405, 3), (18_400, 5)]); // best bid now 184.05

        // Cancel the original best-1 bid.
        assert!(b.on_delta(DeltaOp {
            seq: 2,
            mktdata_seq: 2,
            ts: 2,
            kind: DeltaKind::Cancel { order_id: 101 },
        }));
        let (bids, _) = b.top_levels(5);
        assert_eq!(bids, vec![(18_400, 5)]);
    }

    #[test]
    fn execute_reduces_then_removes_order() {
        let mut b = synced_book();
        // Partial fill of the resting ask (order 200, qty 8) by 3.
        assert!(b.on_delta(DeltaOp {
            seq: 1,
            mktdata_seq: 1,
            ts: 1,
            kind: DeltaKind::Execute {
                order_id: 200,
                exec_qty_raw: 3,
                full_fill: false,
            },
        }));
        let (_, asks) = b.top_levels(5);
        assert_eq!(asks, vec![(18_410, 5)]);
        // Full fill of the remaining 5.
        assert!(b.on_delta(DeltaOp {
            seq: 2,
            mktdata_seq: 2,
            ts: 2,
            kind: DeltaKind::Execute {
                order_id: 200,
                exec_qty_raw: 5,
                full_fill: true,
            },
        }));
        let (_, asks) = b.top_levels(5);
        assert!(asks.is_empty());
    }

    #[test]
    fn duplicate_and_old_deltas_are_ignored() {
        let mut b = synced_book();
        assert!(b.on_delta(add(1, 101, true, 18_405, 3)));
        // Replaying seq 1 (duplicate) and seq 0 (old) must not double-apply.
        assert!(!b.on_delta(add(1, 999, true, 18_406, 9)));
        assert!(!b.on_delta(add(0, 998, true, 18_407, 9)));
        let (bids, _) = b.top_levels(5);
        assert_eq!(bids, vec![(18_405, 3), (18_400, 5)]);
    }

    #[test]
    fn gap_triggers_recovery_then_snapshot_replays_buffered_deltas() {
        let mut b = synced_book();
        assert!(b.on_delta(add(1, 101, true, 18_405, 3))); // ok

        // Gap: seq jumps 1 -> 3. Drop to recovering; the delta is buffered, not applied.
        assert!(!b.on_delta(add(3, 102, true, 18_406, 1)));
        assert!(!b.is_synced());
        // Further deltas while recovering are buffered too.
        assert!(!b.on_delta(add(4, 103, false, 18_420, 2)));

        // A fresh snapshot at anchor seq 2 re-establishes the book; buffered deltas 3 & 4 (> 2)
        // replay in order, so the book ends up consistent and synced again.
        b.on_snapshot_begin(2, 2, 2, 2); // snapshot_id=2, anchor_seq=2, total_orders=2, last_instr_seq=2
        b.on_snapshot_order(2, 100, true, 18_400, 5);
        b.on_snapshot_order(2, 200, false, 18_410, 8);
        assert!(b.on_snapshot_end(2, 2)); // anchor_seq=2, snapshot_id=2
        assert!(b.is_synced());
        assert_eq!(b.last_instr_seq, 4); // replayed 3 and 4

        let (bids, asks) = b.top_levels(5);
        // bids: 18_406 x1 (order 102), 18_400 x5 (order 100)
        assert_eq!(bids, vec![(18_406, 1), (18_400, 5)]);
        // asks: 18_410 x8 (order 200), 18_420 x2 (order 103)
        assert_eq!(asks, vec![(18_410, 8), (18_420, 2)]);
    }

    #[test]
    fn instrument_reset_drops_book_until_resnapshot() {
        let mut b = synced_book();
        b.on_instrument_reset(0); // new_anchor_seq = 0
        assert!(!b.is_synced());
        // Deltas are buffered while recovering.
        assert!(!b.on_delta(add(1, 101, true, 18_405, 3)));
        b.on_snapshot_begin(7, 0, 1, 0); // snapshot_id=7, anchor_seq=0, total_orders=1, last_instr_seq=0
        b.on_snapshot_order(7, 300, true, 18_390, 4);
        assert!(b.on_snapshot_end(0, 7)); // anchor_seq=0, snapshot_id=7
        assert!(b.is_synced());
        let (bids, _) = b.top_levels(5);
        // snapshot bid 300, then replayed delta 1 (order 101) at 18_405.
        assert_eq!(bids, vec![(18_405, 3), (18_390, 4)]);
    }

    /// "Snapshot while ready": a routine round-robin snapshot whose anchor is at/below what the book
    /// has already applied is stale and MUST be ignored - it must not clobber the live book or rewind
    /// the per-instrument sequence. This is the core dedup regression.
    #[test]
    fn periodic_snapshot_while_ready_is_ignored() {
        let mut b = synced_book();
        // Advance the live book to mktdata_seq 2 (per-instrument seq 2).
        assert!(b.on_delta(add(1, 101, true, 18_405, 3)));
        assert!(b.on_delta(add(2, 102, true, 18_406, 1)));
        let before = b.top_levels(5);
        assert_eq!(b.last_instr_seq, 2);
        assert_eq!(b.last_applied_mktdata_seq, 2);

        // A periodic snapshot captured at an older anchor (seq 1) arrives. It must be dropped whole:
        // begin is ignored (no Building), so the orders and end no-op.
        b.on_snapshot_begin(9, 1, 1, 1); // anchor_seq=1 <= last_applied_mktdata_seq=2
        b.on_snapshot_order(9, 999, true, 99_999, 7); // would corrupt the book if applied
        assert!(!b.on_snapshot_end(1, 9));

        assert!(b.is_synced());
        assert_eq!(b.last_instr_seq, 2); // not rewound
        assert_eq!(b.top_levels(5), before); // book unchanged

        // A live delta contiguous after seq 2 still applies cleanly (would be seen as a duplicate if
        // the stale snapshot had rewound the sequence).
        assert!(b.on_delta(add(3, 103, false, 18_420, 2)));
        assert_eq!(b.last_instr_seq, 3);
    }

    /// A snapshot strictly ahead of the applied anchor (an undetected gap) re-bootstraps the book,
    /// replacing it wholesale.
    #[test]
    fn stale_snapshot_ahead_rebootstraps() {
        let mut b = synced_book();
        assert!(b.on_delta(add(1, 101, true, 18_405, 3)));
        assert_eq!(b.last_applied_mktdata_seq, 1);

        // Snapshot anchored ahead (seq 5) - the book is behind, so it must adopt the snapshot.
        b.on_snapshot_begin(2, 5, 1, 5); // anchor_seq=5 > last_applied=1
        b.on_snapshot_order(2, 500, false, 18_500, 9);
        assert!(b.on_snapshot_end(5, 2));

        assert!(b.is_synced());
        assert_eq!(b.last_instr_seq, 5);
        assert_eq!(b.last_applied_mktdata_seq, 5);
        let (bids, asks) = b.top_levels(5);
        assert!(bids.is_empty()); // old bids replaced
        assert_eq!(asks, vec![(18_500, 9)]);
    }

    /// A snapshot that promises more orders than arrive (truncated by packet loss) MUST be rejected,
    /// leaving the book in `Recovering` rather than installing a partial book.
    #[test]
    fn incomplete_snapshot_is_discarded() {
        let mut b = BookState::new();
        b.on_snapshot_begin(1, 0, 3, 0); // promises 3 orders
        b.on_snapshot_order(1, 100, true, 18_400, 5);
        b.on_snapshot_order(1, 200, false, 18_410, 8); // only 2 arrive
        assert!(!b.on_snapshot_end(0, 1));
        assert!(!b.is_synced());
    }

    /// `InstrumentReset(S')` discards buffered deltas at/below the new anchor but keeps those past it
    /// for post-snapshot replay.
    #[test]
    fn instrument_reset_keeps_post_anchor_buffered_deltas() {
        let mut b = synced_book();
        b.on_instrument_reset(5); // new_anchor_seq = 5
        assert!(!b.is_synced());

        // Buffer one delta at the anchor (discarded) and one past it (kept).
        let mut at_anchor = add(10, 400, true, 18_300, 1);
        at_anchor.mktdata_seq = 5; // <= S'
        let mut past_anchor = add(11, 401, true, 18_310, 2);
        past_anchor.mktdata_seq = 6; // > S'
        assert!(!b.on_delta(at_anchor));
        assert!(!b.on_delta(past_anchor));

        // Recovery snapshot anchored at S'=5, last_instr_seq=10 so the kept delta (seq 11) is
        // contiguous on replay; the discarded one (seq 10) is gone.
        b.on_snapshot_begin(3, 5, 0, 10); // empty book, anchor_seq=5
        assert!(b.on_snapshot_end(5, 3));
        assert!(b.is_synced());
        assert_eq!(b.last_instr_seq, 11); // only the post-anchor delta replayed
        let (bids, _) = b.top_levels(5);
        assert_eq!(bids, vec![(18_310, 2)]);
    }

    /// The pending-delta buffer must stay bounded while `Recovering`: a flood of deltas for an
    /// instrument that never receives a snapshot can't grow it without limit.
    #[test]
    fn pending_buffer_is_bounded_while_recovering() {
        let mut b = BookState::new(); // starts Recovering
        for i in 0..(MAX_PENDING_DELTAS + 1_000) {
            // Each delta is buffered (not applied) while recovering; once full, excess is dropped.
            assert!(!b.on_delta(add((i as u32).wrapping_add(1), i as u64, true, 100, 1)));
        }
        assert!(!b.is_synced());
        assert!(
            b.pending.len() <= MAX_PENDING_DELTAS,
            "pending must stay bounded, got {}",
            b.pending.len()
        );
    }

    /// A synced book's resting-order population must stay bounded under a flood of forged adds: at
    /// the cap it drops to `Recovering` rather than growing without limit.
    #[test]
    fn resting_orders_are_bounded_under_add_flood() {
        let mut b = synced_book(); // synced, last_instr_seq = 0
        for i in 0..(MAX_ORDERS_PER_BOOK + 1_000) {
            let seq = (i as u32).wrapping_add(1);
            let oid = 1_000u64 + i as u64;
            b.on_delta(add(seq, oid, true, 100 + (i as i64 % 64), 1));
        }
        assert!(
            b.orders.len() <= MAX_ORDERS_PER_BOOK,
            "resting orders must stay bounded, got {}",
            b.orders.len()
        );
        assert!(!b.is_synced(), "an oversized book drops to Recovering");
    }
}
