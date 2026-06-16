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

use std::collections::{BTreeMap, HashMap};

/// One aggregated price level as `(price_raw, qty_raw)` - raw integers in the instrument's
/// price/qty exponents (the caller scales them).
pub type Level = (i64, u64);

/// One resting order's mutable state.
#[derive(Debug, Clone)]
struct RestingOrder {
    is_bid: bool,
    price_raw: i64,
    qty_raw: u64,
}

/// What a delta does to the book. `seq` is the per-instrument delta sequence; `ts` the event time.
#[derive(Debug, Clone)]
pub struct DeltaOp {
    pub seq: u32,
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
    /// Snapshot under assembly (between begin and end).
    building: Option<Building>,
    /// Deltas buffered while `Recovering`, replayed after the next snapshot.
    pending: Vec<DeltaOp>,
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
            building: None,
            pending: Vec::new(),
            last_event_ts: 0,
        }
    }

    pub fn is_synced(&self) -> bool {
        self.state == SyncState::Synced
    }

    pub fn last_event_ts(&self) -> u64 {
        self.last_event_ts
    }

    /// `InstrumentReset`: drop the book and await a fresh snapshot.
    pub fn on_instrument_reset(&mut self) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        self.building = None;
        self.pending.clear();
        self.state = SyncState::Recovering;
    }

    /// Begin assembling a snapshot. A new begin supersedes any half-built one.
    pub fn on_snapshot_begin(&mut self, snapshot_id: u32, last_instrument_seq: u32) {
        self.building = Some(Building {
            snapshot_id,
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
                b.orders.insert(
                    order_id,
                    RestingOrder {
                        is_bid,
                        price_raw,
                        qty_raw,
                    },
                );
                let levels = if is_bid { &mut b.bids } else { &mut b.asks };
                level_add(levels, price_raw, qty_raw);
            }
        }
    }

    /// Finalize the assembled snapshot: install it as the live book, set the delta-sequence anchor,
    /// then replay any buffered deltas past it. Returns whether the book changed (a snapshot always
    /// does). A mismatched/absent `snapshot_id` is ignored.
    pub fn on_snapshot_end(&mut self, snapshot_id: u32) -> bool {
        let Some(b) = self.building.take() else {
            return false;
        };
        if b.snapshot_id != snapshot_id {
            return false;
        }
        self.orders = b.orders;
        self.bids = b.bids;
        self.asks = b.asks;
        self.last_instr_seq = b.last_instrument_seq;
        self.state = SyncState::Synced;

        // Replay buffered deltas in sequence order; those at/below the anchor are already included.
        let mut pending = std::mem::take(&mut self.pending);
        pending.sort_by_key(|d| d.seq);
        for d in pending {
            self.apply_or_recover(d);
            if self.state == SyncState::Recovering {
                break; // a gap in the buffered deltas; await the next snapshot
            }
        }
        true
    }

    /// Apply a delta. While `Recovering`, deltas are buffered (returns `false`). While `Synced`,
    /// the per-instrument sequence is enforced: a duplicate/old delta is ignored, the next
    /// contiguous one is applied, and a forward gap drops to `Recovering`. Returns whether the
    /// top-of-book-bearing levels may have changed (i.e. a delta was actually applied).
    pub fn on_delta(&mut self, op: DeltaOp) -> bool {
        match self.state {
            SyncState::Recovering => {
                self.pending.push(op);
                false
            }
            SyncState::Synced => self.apply_or_recover(op),
        }
    }

    fn apply_or_recover(&mut self, op: DeltaOp) -> bool {
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
        self.last_event_ts = op.ts;
        self.apply(op.kind);
        true
    }

    fn apply(&mut self, kind: DeltaKind) {
        match kind {
            DeltaKind::Add {
                order_id,
                is_bid,
                price_raw,
                qty_raw,
            } => {
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
            }
            DeltaKind::Cancel { order_id } => {
                if let Some(o) = self.orders.remove(&order_id) {
                    let levels = if o.is_bid {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    };
                    level_remove(levels, o.price_raw, o.qty_raw);
                }
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
                }
            }
        }
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

    fn add(seq: u32, order_id: u64, is_bid: bool, price: i64, qty: u64) -> DeltaOp {
        DeltaOp {
            seq,
            ts: seq as u64,
            kind: DeltaKind::Add {
                order_id,
                is_bid,
                price_raw: price,
                qty_raw: qty,
            },
        }
    }

    /// A fully-recovered book: begin, two orders, end (anchor seq = 0 so deltas start at 1).
    fn synced_book() -> BookState {
        let mut b = BookState::new();
        assert!(!b.is_synced());
        b.on_snapshot_begin(1, 0);
        b.on_snapshot_order(1, 100, true, 18_400, 5); // bid 184.00 x5
        b.on_snapshot_order(1, 200, false, 18_410, 8); // ask 184.10 x8
        assert!(b.on_snapshot_end(1));
        assert!(b.is_synced());
        b
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
        b.on_snapshot_begin(2, 2); // last_instrument_seq = 2
        b.on_snapshot_order(2, 100, true, 18_400, 5);
        b.on_snapshot_order(2, 200, false, 18_410, 8);
        assert!(b.on_snapshot_end(2));
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
        b.on_instrument_reset();
        assert!(!b.is_synced());
        // Deltas are buffered while recovering.
        assert!(!b.on_delta(add(1, 101, true, 18_405, 3)));
        b.on_snapshot_begin(7, 0);
        b.on_snapshot_order(7, 300, true, 18_390, 4);
        assert!(b.on_snapshot_end(7));
        assert!(b.is_synced());
        let (bids, _) = b.top_levels(5);
        // snapshot bid 300, then replayed delta 1 (order 101) at 18_405.
        assert_eq!(bids, vec![(18_405, 3), (18_390, 4)]);
    }
}
