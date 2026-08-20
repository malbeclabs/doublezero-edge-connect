//! L2 price-level book reconstruction with snapshot+delta recovery for the Market-by-Price feed.
//!
//! A [`PriceBook`] holds one instrument's price levels, one `BTreeMap` per side, and runs the
//! edge-feed-spec recovery model: deltas apply only in unbroken **per-instrument sequence**, a gap
//! buffers until a snapshot re-anchors the book, and the buffered deltas past the snapshot's
//! `anchor_seq` replay afterwards.
//!
//! This is a **sibling of [`crate::ingest::book`], not a reuse of it**. `book.rs` is order-keyed and
//! derives levels by aggregating resting orders; the market-by-price wire is already price-aggregated
//! and each `Level` carries the **absolute** resulting quantity of one level, so there is nothing to
//! aggregate and the `Add`/`Cancel`/`Execute` vocabulary does not apply.
//!
//! Like `book.rs` the type takes **raw integers, not wire structs**, so the recovery logic
//! unit-tests in isolation. It is not value-space-agnostic, though: `side`, `clear_side` and `scope`
//! arrive as the bytes [`crate::ingest::codec_mbp`] decoded, so the constants come from there rather
//! than being restated here. A second copy that drifted would swap bids and asks while every
//! sequence check still passed.

use std::collections::BTreeMap;

use crate::ingest::codec_mbp::{
    CLEAR_SIDE_ASK, CLEAR_SIDE_BID, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, SIDE_ASK, SIDE_BID,
};

/// `Action` values from the spec's enum — the one wire enum the decoder does not name, since it
/// passes the byte through untouched. Observability only, see [`PriceBook::on_delta`].
const ACTION_NEW: u8 = 1;
const ACTION_CHANGE: u8 = 2;
const ACTION_DELETE: u8 = 3;

/// Cap on deltas buffered while not `Ready`. In normal operation the buffer holds at most one
/// snapshot cycle; this bounds a flood of deltas for an instrument that never receives a snapshot.
/// Excess deltas are dropped — the book re-anchors on the next snapshot regardless of which
/// buffered deltas survived. Matches `book.rs`'s `MAX_PENDING_DELTAS`.
pub(crate) const MAX_BUFFERED_DELTAS: usize = 1 << 18;

/// Cap on price levels held per book — both the live sides and the snapshot under assembly. The
/// multicast source is unauthenticated, so a forged stream of level updates at distinct prices would
/// otherwise grow both maps without limit; ~50x the spec's own worst-case per-instrument sizing.
/// Matches `book.rs`'s `MAX_ORDERS_PER_BOOK`.
const MAX_LEVELS_PER_BOOK: usize = 1 << 18;

/// Where a book sits in the recovery model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// No usable book: awaiting a snapshot. Deltas buffer.
    AwaitingSnapshot,
    /// A snapshot group is open; the shadow is being assembled and deltas buffer.
    BuildingSnapshot,
    /// In sync: deltas apply in sequence.
    Ready,
    /// A per-instrument sequence gap was seen; the book is stale until the next snapshot.
    Gap,
}

/// One price level's state. `qty_raw` is the absolute quantity resting at the price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelState {
    pub qty_raw: u64,
    pub order_count: Option<u16>,
    pub level_flags: u8,
}

/// What a delta does to the book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookDelta {
    Level {
        side: u8,
        price_raw: i64,
        qty_raw: u64,
        order_count: Option<u16>,
        level_flags: u8,
        action: u8,
    },
    Clear {
        clear_side: u8,
        scope: u8,
        from_price_raw: i64,
    },
}

/// One sequenced delta. `seq` is the per-instrument delta sequence; `mktdata_seq` the channel-wide
/// `mktdata`-port sequence that carried it (what snapshot replay keys on); `ts` the event time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaOp {
    pub seq: u32,
    pub mktdata_seq: u64,
    pub ts: u64,
    pub delta: BookDelta,
}

/// A publisher `Action` byte that disagrees with the quantity it arrived with. Recorded, never
/// acted on — the quantity alone determines the resulting level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Divergence {
    NewOnPresentLevel,
    ChangeOnAbsentLevel,
    DeleteWithQuantity,
    ZeroQuantityWithoutDelete,
}

/// What [`PriceBook::on_delta`] did with a delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaOutcome {
    /// Held for replay after the next snapshot (the book is not `Ready`).
    Buffered,
    /// `seq` at or below the last applied one: a duplicate or late arrival, discarded.
    Duplicate,
    /// A forward sequence gap: deltas were lost. `status` becomes [`Status::Gap`] and this delta is
    /// buffered for replay after the next snapshot.
    Gap,
    /// The level cap was hit, so the book and buffer were discarded and `status` is
    /// [`Status::AwaitingSnapshot`] — nothing was missed, we refused to grow. Distinct from [`Self::Gap`]
    /// because the cause (a malformed or forged stream, never packet loss) and the resulting `status`
    /// both differ; a caller counting one as the other would read a hostile book as a lossy network.
    Overflow,
    Applied {
        divergence: Option<Divergence>,
    },
}

/// A snapshot group being assembled between `SnapshotBegin` and `SnapshotEnd`.
struct Building {
    snapshot_id: u32,
    /// `mktdata`-port sequence the snapshot was captured at; carried by both begin and end.
    anchor_seq: u64,
    /// Levels promised by the begin; the end is rejected unless exactly this many arrived (guards
    /// against installing a snapshot truncated by packet loss).
    total_levels: u32,
    received_levels: u32,
    last_instrument_seq: u32,
    depth_bound: u32,
    bids: BTreeMap<i64, LevelState>,
    asks: BTreeMap<i64, LevelState>,
}

impl Building {
    fn len(&self) -> usize {
        self.bids.len() + self.asks.len()
    }
}

pub struct PriceBook {
    bids: BTreeMap<i64, LevelState>,
    asks: BTreeMap<i64, LevelState>,
    status: Status,
    /// Last applied per-instrument delta sequence. This — never `anchor_seq` — is the
    /// snapshot-while-`Ready` discriminator.
    last_applied_instrument_seq: u32,
    /// The publisher's declared level bound, `None` until a snapshot states one. Wire `0` is a
    /// positive claim of a complete book and must stay distinct from "no claim".
    depth_bound: Option<u32>,
    /// Set by an `InstrumentReset`: no snapshot anchored before this may install.
    required_anchor_seq: Option<u64>,
    last_event_ts: u64,
    open: Option<Building>,
    /// Deltas buffered while not `Ready`, replayed after the next snapshot.
    pending: Vec<DeltaOp>,
}

impl Default for PriceBook {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            status: Status::AwaitingSnapshot,
            last_applied_instrument_seq: 0,
            depth_bound: None,
            required_anchor_seq: None,
            last_event_ts: 0,
            open: None,
            pending: Vec::new(),
        }
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn depth_bound(&self) -> Option<u32> {
        self.depth_bound
    }

    pub fn last_event_ts(&self) -> u64 {
        self.last_event_ts
    }

    pub fn buffered_len(&self) -> usize {
        self.pending.len()
    }

    /// Drop the buffered deltas and mark the instrument `Gap` — the action behind the
    /// cross-instrument buffer overflow policy. It recovers on its next snapshot like any other
    /// `Gap` instrument.
    pub fn drop_buffer(&mut self) {
        self.pending.clear();
        self.status = Status::Gap;
    }

    /// Whether the inside prices cross. Observability only: it never changes status or discards the
    /// book. Strict `>`, so a *locked* book (equal inside prices, routine on some venues) is not
    /// crossed.
    pub fn crossed(&self) -> bool {
        match (self.bids.keys().next_back(), self.asks.keys().next()) {
            (Some(bid), Some(ask)) => bid > ask,
            _ => false,
        }
    }

    /// Bids, best first (descending price).
    pub fn bids(&self) -> impl Iterator<Item = (i64, &LevelState)> {
        self.bids.iter().rev().map(|(p, l)| (*p, l))
    }

    /// Asks, best first (ascending price).
    pub fn asks(&self) -> impl Iterator<Item = (i64, &LevelState)> {
        self.asks.iter().map(|(p, l)| (*p, l))
    }

    /// Apply a delta, or buffer it when the book is not `Ready`. A sustained run of `Duplicate` is
    /// the one symptom of a baseline above the publisher's real counter — see
    /// [`Self::on_end_of_session`] for the escape — so a caller should count it.
    ///
    /// `removed` is **cleared at entry** and then collects the `(side, price_raw)` of every level a
    /// `Clear` dropped, both scopes. A consumer whose only clear primitive is whole-side cannot
    /// express `SCOPE_FROM_PRICE` without the exact prices, and telling it to drop the side would
    /// diverge from the levels we still hold.
    pub fn on_delta(&mut self, op: DeltaOp, removed: &mut Vec<(u8, i64)>) -> DeltaOutcome {
        removed.clear();
        if self.status != Status::Ready {
            self.buffer(op);
            return DeltaOutcome::Buffered;
        }
        if op.seq <= self.last_applied_instrument_seq {
            return DeltaOutcome::Duplicate;
        }
        if op.seq != self.last_applied_instrument_seq + 1 {
            // A forward gap. The buffer's contents predate this hole and can never bridge it, so
            // they go with it; this delta starts the post-gap buffer.
            self.status = Status::Gap;
            self.pending.clear();
            self.pending.push(op);
            return DeltaOutcome::Gap;
        }
        self.apply(op, removed)
    }

    /// Open a snapshot group, returning whether it was accepted. Declined when an `InstrumentReset`
    /// requires a newer anchor, or when the book is already `Ready` and the snapshot was captured no
    /// later than the deltas we have applied. That second test compares **`last_instrument_seq`**,
    /// never `anchor_seq`: `anchor_seq` is channel-wide and advances on every other instrument's
    /// deltas and on every heartbeat, so comparing it would rebuild every good book every rotation.
    pub fn on_snapshot_begin(
        &mut self,
        snapshot_id: u32,
        anchor_seq: u64,
        total_levels: u32,
        last_instrument_seq: u32,
        depth_bound: u32,
    ) -> bool {
        if self.required_anchor_seq.is_some_and(|s| anchor_seq < s)
            || (self.status == Status::Ready
                && last_instrument_seq <= self.last_applied_instrument_seq)
            || total_levels as usize > MAX_LEVELS_PER_BOOK
        {
            // A begin we refuse says nothing about a group already under assembly, so leave it —
            // discarding it would strand `status` at `BuildingSnapshot` with no group to end.
            return false;
        }
        // A duplicated datagram, which a multicast wire produces routinely: every identifying field
        // matches the group already assembling, so this is the begin we have, not a new rotation.
        // Restarting assembly here would zero `received_levels` and discard every level received so
        // far; `on_snapshot_end` would then fail its `received_levels != total_levels` test and take
        // the incomplete-group path, which clears `bids`/`asks` — destroying the **live** book on
        // the `Ready`-rebuild path and dropping the market to `AwaitingSnapshot` until the next
        // rotation. Declining leaves the group and its levels exactly as they are.
        //
        // Compared field-for-field rather than on `snapshot_id` alone: the id is monotonic per
        // `(channel, instrument)` and a begin that differs anywhere else is a genuinely new group
        // that must still replace this one, or a rotation whose end was lost would pin the book.
        if self.open.as_ref().is_some_and(|b| {
            b.snapshot_id == snapshot_id
                && b.anchor_seq == anchor_seq
                && b.total_levels == total_levels
                && b.last_instrument_seq == last_instrument_seq
                && b.depth_bound == depth_bound
        }) {
            return false;
        }
        self.open = Some(Building {
            snapshot_id,
            anchor_seq,
            total_levels,
            received_levels: 0,
            last_instrument_seq,
            depth_bound,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        });
        self.status = Status::BuildingSnapshot;
        true
    }

    /// Add one level to the group under assembly (ignored when no group is open or the id differs).
    /// Levels assemble into a shadow, so a group that never completes cannot evict live levels.
    pub fn on_snapshot_level(
        &mut self,
        snapshot_id: u32,
        side: u8,
        price_raw: i64,
        qty_raw: u64,
        order_count: Option<u16>,
        level_flags: u8,
    ) {
        let Some(b) = &mut self.open else { return };
        if b.snapshot_id != snapshot_id || b.len() >= MAX_LEVELS_PER_BOOK {
            return;
        }
        let levels = if side == SIDE_ASK {
            &mut b.asks
        } else {
            &mut b.bids
        };
        // Count distinct prices, not messages: a duplicated datagram would otherwise make up for a
        // lost one and let a group short of `total_levels` install as complete.
        if levels
            .insert(
                price_raw,
                LevelState {
                    qty_raw,
                    order_count,
                    level_flags,
                },
            )
            .is_none()
        {
            b.received_levels += 1;
        }
    }

    /// Install the assembled group, returning whether it did. A mismatched `snapshot_id`/`anchor_seq`
    /// or a level count short of `total_levels` discards the shadow **and the live levels** and
    /// leaves the book `AwaitingSnapshot`: a group only ever opens from `Ready` once we know we are
    /// behind, so a failure there means the stale book we meant to replace must stop being served.
    /// A snapshot that was never accepted (no open group) leaves everything untouched. `true` means
    /// the levels installed, not that the book is usable — the replay it triggers can gap.
    pub fn on_snapshot_end(&mut self, anchor_seq: u64, snapshot_id: u32) -> bool {
        let Some(b) = self.open.take() else {
            return false;
        };
        if b.snapshot_id != snapshot_id
            || b.anchor_seq != anchor_seq
            || b.received_levels != b.total_levels
        {
            self.bids.clear();
            self.asks.clear();
            self.depth_bound = None;
            self.status = Status::AwaitingSnapshot;
            return false;
        }
        self.bids = b.bids;
        self.asks = b.asks;
        self.last_applied_instrument_seq = b.last_instrument_seq;
        self.depth_bound = Some(b.depth_bound);
        self.required_anchor_seq = None;
        self.status = Status::Ready;
        self.replay(b.anchor_seq);
        true
    }

    /// `InstrumentReset(new_anchor_seq = S')`: drop the book and any open group, and await a snapshot
    /// anchored at `S'` or newer — so a snapshot captured before the reset but delivered after cannot
    /// reinstate the diverged book the reset exists to discard. Buffered deltas at/below `S'` are
    /// superseded by it; those past it are kept for post-snapshot replay. `depth_bound` returns to
    /// unknown: a reset instrument has made no claim about its depth.
    pub fn on_instrument_reset(&mut self, new_anchor_seq: u64) {
        self.bids.clear();
        self.asks.clear();
        self.open = None;
        self.pending.retain(|d| d.mktdata_seq > new_anchor_seq);
        self.status = Status::AwaitingSnapshot;
        self.last_applied_instrument_seq = 0;
        self.depth_bound = None;
        self.required_anchor_seq = Some(new_anchor_seq);
        self.last_event_ts = 0;
    }

    /// `EndOfSession`: the session's book, sequences and event clock are all over. Unlike a reset
    /// there is no forward anchor, so buffered deltas belong to the ended session and are discarded
    /// outright. Zeroing the event clock keeps the resync from stamping its first output with
    /// pre-session time.
    ///
    /// This is also the **only** escape from a per-instrument sequence that restarted (a publisher
    /// crash, or a garbage `last_instrument_seq` installed by a snapshot): every delta below the
    /// baseline reads as a duplicate and every snapshot as current, so the frame header's changed
    /// `Reset Count` must route here for every book of that publisher.
    pub fn on_end_of_session(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.open = None;
        self.pending.clear();
        self.status = Status::AwaitingSnapshot;
        self.last_applied_instrument_seq = 0;
        self.depth_bound = None;
        self.required_anchor_seq = None;
        self.last_event_ts = 0;
    }

    fn buffer(&mut self, op: DeltaOp) {
        if self.pending.len() < MAX_BUFFERED_DELTAS {
            self.pending.push(op);
        }
        // else: drop; the book re-anchors on the next snapshot regardless.
    }

    /// Replay buffered deltas after a snapshot installed at `anchor`. Those at/below it are already
    /// in the snapshot; the rest run in `mktdata_seq` order through the same classification as steady
    /// state, and a genuine forward gap stops the replay with the remainder re-buffered.
    fn replay(&mut self, anchor: u64) {
        let mut pending = std::mem::take(&mut self.pending);
        pending.retain(|d| d.mktdata_seq > anchor);
        pending.sort_by_key(|d| d.mktdata_seq);
        // Discarded: the snapshot install re-baselines the whole book downstream anyway.
        let mut removed = Vec::new();
        let mut ops = pending.into_iter();
        for op in ops.by_ref() {
            if op.seq <= self.last_applied_instrument_seq {
                continue;
            }
            if op.seq != self.last_applied_instrument_seq + 1 {
                self.status = Status::Gap;
                self.buffer(op);
                break;
            }
            if self.apply(op, &mut removed) == DeltaOutcome::Overflow {
                return; // the book and the buffer are gone, and the rest goes with them
            }
        }
        for op in ops {
            self.buffer(op);
        }
    }

    /// Apply an in-sequence delta, advancing the trackers. Only reachable once the sequence has been
    /// classified.
    fn apply(&mut self, op: DeltaOp, removed: &mut Vec<(u8, i64)>) -> DeltaOutcome {
        match op.delta {
            BookDelta::Level {
                side,
                price_raw,
                qty_raw,
                order_count,
                level_flags,
                action,
            } => {
                let is_ask = side == SIDE_ASK;
                let present = if is_ask {
                    self.asks.contains_key(&price_raw)
                } else {
                    self.bids.contains_key(&price_raw)
                };
                if !present
                    && qty_raw != 0
                    && self.bids.len() + self.asks.len() >= MAX_LEVELS_PER_BOOK
                {
                    // Refused before the trackers move: nothing was applied, so nothing advances.
                    self.overflow();
                    return DeltaOutcome::Overflow;
                }
                self.advance(op.seq, op.ts);
                let levels = if is_ask {
                    &mut self.asks
                } else {
                    &mut self.bids
                };
                // Quantity alone decides the result: every level update states the complete
                // resulting state of one level, so a wrong `Action` byte must never corrupt a book.
                if qty_raw == 0 {
                    levels.remove(&price_raw);
                } else {
                    levels.insert(
                        price_raw,
                        LevelState {
                            qty_raw,
                            order_count,
                            level_flags,
                        },
                    );
                }
                DeltaOutcome::Applied {
                    divergence: divergence(action, qty_raw, present),
                }
            }
            BookDelta::Clear {
                clear_side,
                scope,
                from_price_raw,
            } => {
                self.advance(op.seq, op.ts);
                if clear_side == CLEAR_SIDE_BOTH && scope != SCOPE_ENTIRE_SIDE {
                    // Malformed: one price cannot bound both sides. Consume the sequence, clear
                    // nothing — a guess at what was meant would silently empty a live book.
                    //
                    // Tested against the recognized whole-side scope, so every unassigned
                    // `2..=255` is refused too: [`clear_side_levels`] treats anything that is not
                    // `SCOPE_ENTIRE_SIDE` as price-bounded, so an `== SCOPE_FROM_PRICE` test here
                    // would clear bids at/below and asks at/above one bound — the whole book. The
                    // codec refuses the same shape (`decode_book_clear`), so in practice this is
                    // defence in depth for a caller that builds a `Clear` by hand.
                    return DeltaOutcome::Applied { divergence: None };
                }
                if clear_side == CLEAR_SIDE_BID || clear_side == CLEAR_SIDE_BOTH {
                    clear_side_levels(&mut self.bids, SIDE_BID, scope, from_price_raw, removed);
                }
                if clear_side == CLEAR_SIDE_ASK || clear_side == CLEAR_SIDE_BOTH {
                    clear_side_levels(&mut self.asks, SIDE_ASK, scope, from_price_raw, removed);
                }
                DeltaOutcome::Applied { divergence: None }
            }
        }
    }

    fn advance(&mut self, seq: u32, ts: u64) {
        self.last_applied_instrument_seq = seq;
        self.last_event_ts = ts;
    }

    /// The level cap was reached. Discard the book *and* the buffer — leaving the buffer would just
    /// relocate the flood into it — and await a snapshot. Not `Gap`: nothing was missed.
    fn overflow(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.pending.clear();
        self.status = Status::AwaitingSnapshot;
    }
}

/// The publisher's `Action` byte against what the quantity actually did. At most one is reported;
/// none of them changes the applied result.
fn divergence(action: u8, qty_raw: u64, present: bool) -> Option<Divergence> {
    if action == ACTION_NEW && present {
        Some(Divergence::NewOnPresentLevel)
    } else if action == ACTION_CHANGE && !present {
        Some(Divergence::ChangeOnAbsentLevel)
    } else if action == ACTION_DELETE && qty_raw != 0 {
        Some(Divergence::DeleteWithQuantity)
    } else if qty_raw == 0 && action != ACTION_DELETE {
        Some(Divergence::ZeroQuantityWithoutDelete)
    } else {
        None
    }
}

/// `SCOPE_ENTIRE_SIDE` empties the side and ignores `from`; `SCOPE_FROM_PRICE` clears outward from
/// `from` inclusively — for bids everything at or below it, for asks everything at or above. Each
/// dropped price is appended to `removed` as `(side, price)`.
fn clear_side_levels(
    levels: &mut BTreeMap<i64, LevelState>,
    side: u8,
    scope: u8,
    from: i64,
    removed: &mut Vec<(u8, i64)>,
) {
    let entire = scope == SCOPE_ENTIRE_SIDE;
    let is_ask = side == SIDE_ASK;
    levels.retain(|p, _| {
        let survives = !entire && if is_ask { *p < from } else { *p > from };
        if !survives {
            removed.push((side, *p));
        }
        survives
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    // Only the tests name the from-price scope now: the apply path derives its behaviour from the
    // whole-side one so that every unrecognized byte is refused (see the `Clear` branch of `on_delta`).
    use crate::ingest::codec_mbp::SCOPE_FROM_PRICE;

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
            delta: BookDelta::Clear {
                clear_side,
                scope,
                from_price_raw: from,
            },
        }
    }

    /// For the tests that assert on the outcome alone.
    fn apply_delta(b: &mut PriceBook, op: DeltaOp) -> DeltaOutcome {
        b.on_delta(op, &mut Vec::new())
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
            apply_delta(&mut b, level(6, 101, SIDE_BID, 6200, 150, DELETE)),
            DeltaOutcome::Applied { .. }
        ));
        assert_eq!(bids_of(&b), vec![(6200, 150)]);
        // `New` on a present level with quantity 0: the quantity wins, level is removed.
        assert!(matches!(
            apply_delta(&mut b, level(7, 102, SIDE_BID, 6200, 0, NEW)),
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
            (
                level(6, 101, SIDE_BID, 6200, 200, NEW),
                Divergence::NewOnPresentLevel,
            ),
            (
                level(7, 102, SIDE_BID, 6100, 50, CHANGE),
                Divergence::ChangeOnAbsentLevel,
            ),
            (
                level(8, 103, SIDE_BID, 6100, 50, DELETE),
                Divergence::DeleteWithQuantity,
            ),
            (
                level(9, 104, SIDE_BID, 6100, 0, UNKNOWN),
                Divergence::ZeroQuantityWithoutDelete,
            ),
        ];
        for (op, want) in cases {
            match apply_delta(&mut b, op) {
                DeltaOutcome::Applied {
                    divergence: Some(got),
                } => assert_eq!(got, want),
                other => panic!("expected divergence {want:?}, got {other:?}"),
            }
        }
    }

    /// A correct delete (`Action = 3`, quantity 0) is not a divergence.
    #[test]
    fn a_correct_delete_is_not_divergence() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(matches!(
            apply_delta(&mut b, level(6, 101, SIDE_BID, 6200, 0, DELETE)),
            DeltaOutcome::Applied { divergence: None }
        ));
    }

    // ---- §4.8: per_instrument_seq classification, dense, no reset at snapshots ----

    #[test]
    fn contiguous_deltas_apply_and_a_gap_buffers() {
        let mut b = synced(100, 5, 0, &[]);
        assert!(matches!(
            apply_delta(&mut b, level(6, 101, SIDE_BID, 6200, 10, NEW)),
            DeltaOutcome::Applied { .. }
        ));
        // seq <= last is a duplicate or late arrival: discard silently.
        assert!(matches!(
            apply_delta(&mut b, level(6, 101, SIDE_BID, 6200, 99, NEW)),
            DeltaOutcome::Duplicate
        ));
        assert!(matches!(
            apply_delta(&mut b, level(5, 100, SIDE_BID, 6200, 99, NEW)),
            DeltaOutcome::Duplicate
        ));
        assert_eq!(bids_of(&b), vec![(6200, 10)], "neither duplicate applied");
        // A forward gap marks the instrument and buffers.
        assert!(matches!(
            apply_delta(&mut b, level(9, 104, SIDE_BID, 6100, 20, NEW)),
            DeltaOutcome::Gap
        ));
        assert_eq!(b.status(), Status::Gap);
        assert!(matches!(
            apply_delta(&mut b, level(10, 105, SIDE_BID, 6000, 30, NEW)),
            DeltaOutcome::Buffered
        ));
        assert_eq!(
            b.buffered_len(),
            2,
            "the gap delta and the next are both held"
        );
    }

    /// The counter is monotonic within the reset-count era and does NOT restart at snapshot
    /// boundaries: if it did, a subscriber that missed a snapshot but saw `seq = 1` could not tell a
    /// fresh post-snapshot delta from a late duplicate of an old one.
    #[test]
    fn a_snapshot_does_not_reset_the_per_instrument_seq() {
        let mut b = synced(100, 5, 0, &[]);
        apply_delta(&mut b, level(6, 101, SIDE_BID, 6200, 10, NEW));
        // A later snapshot at K = 20 re-baselines; seq 21 must be next, not 1.
        assert!(b.on_snapshot_begin(2, 200, 0, 20, 0));
        assert!(b.on_snapshot_end(200, 2));
        assert!(matches!(
            apply_delta(&mut b, level(1, 201, SIDE_BID, 6200, 10, NEW)),
            DeltaOutcome::Duplicate
        ));
        assert!(matches!(
            apply_delta(&mut b, level(21, 201, SIDE_BID, 6200, 10, NEW)),
            DeltaOutcome::Applied { .. }
        ));
    }

    // ---- §4.2: the snapshot-while-Ready discriminator is Last Instrument Seq ----

    /// `K > last_applied_instrument_seq` means we are genuinely behind — deltas were applied before
    /// the capture that we never saw — so re-bootstrap.
    #[test]
    fn snapshot_while_ready_rebuilds_when_last_instrument_seq_is_ahead() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            b.on_snapshot_begin(2, 500, 1, 9, 0),
            "K=9 > 5, we are behind"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(b.on_snapshot_end(500, 2));
        assert_eq!(asks_of(&b), vec![(6300, 77)]);
        assert!(bids_of(&b).is_empty(), "the snapshot REPLACES the book");
    }

    /// A duplicated `SnapshotBegin` datagram must not restart assembly. On a multicast wire a
    /// duplicate is expected, and this module already guards it for levels
    /// (`a_duplicated_snapshot_level_does_not_satisfy_the_count`) — but the decline set
    /// (`required_anchor_seq` / `Ready` / oversized `total_levels`) does not cover it: while a group
    /// assembles the status is `BuildingSnapshot`, so an identical re-begin was accepted and
    /// overwrote the open group with `received_levels = 0`.
    ///
    /// The damage lands two steps later. `on_snapshot_end` fails its `received_levels !=
    /// total_levels` test and takes the incomplete-group path, which clears `bids`/`asks` — so on
    /// the `Ready`-rebuild path a single duplicated datagram destroys the **live** book and drops
    /// the market to `AwaitingSnapshot` until the next rotation.
    #[test]
    fn a_duplicated_snapshot_begin_does_not_restart_assembly() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            b.on_snapshot_begin(2, 500, 2, 9, 0),
            "K=9 > 5, we are behind"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);

        assert!(
            !b.on_snapshot_begin(2, 500, 2, 9, 0),
            "the duplicate must be declined, not accepted as a fresh group"
        );

        b.on_snapshot_level(2, SIDE_ASK, 6400, 88, Some(1), 0);
        assert!(
            b.on_snapshot_end(500, 2),
            "both levels arrived exactly once, so the group is complete"
        );
        assert_eq!(asks_of(&b), vec![(6300, 77), (6400, 88)]);
        assert_eq!(b.status(), Status::Ready);
    }

    /// The same duplicate on the **first** build of a book, where there is no live book to lose but
    /// the group must still not lose the levels it has already assembled.
    #[test]
    fn a_duplicated_snapshot_begin_from_cold_keeps_the_assembled_levels() {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(1, 100, 2, 5, 0));
        b.on_snapshot_level(1, SIDE_BID, 6200, 10, Some(1), 0);
        assert!(!b.on_snapshot_begin(1, 100, 2, 5, 0), "duplicate declined");
        b.on_snapshot_level(1, SIDE_BID, 6100, 20, Some(1), 0);
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(bids_of(&b), vec![(6200, 10), (6100, 20)]);
    }

    /// A begin that differs from the open group in any identifying field is a genuinely new
    /// rotation, not a duplicate, and must still replace the group under assembly — otherwise a
    /// group whose end was lost would pin the book until an `InstrumentReset`.
    #[test]
    fn a_new_snapshot_begin_still_replaces_a_group_under_assembly() {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(1, 100, 2, 5, 0));
        b.on_snapshot_level(1, SIDE_BID, 6200, 10, Some(1), 0);
        assert!(
            b.on_snapshot_begin(2, 140, 1, 6, 0),
            "a different snapshot_id is a new rotation"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 30, Some(1), 0);
        assert!(b.on_snapshot_end(140, 2));
        assert_eq!(asks_of(&b), vec![(6300, 30)]);
        assert!(
            bids_of(&b).is_empty(),
            "the abandoned group installed nothing"
        );
    }

    /// `K <= last_applied_instrument_seq` is the ordinary case: deltas routinely arrive between the
    /// publisher's capture and the snapshot's delivery. Ignore it — do not rebuild.
    #[test]
    fn snapshot_while_ready_is_ignored_when_current() {
        let mut b = synced(100, 9, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            !b.on_snapshot_begin(2, 500, 1, 5, 0),
            "K=5 <= 9, we are current"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(!b.on_snapshot_end(500, 2));
        assert_eq!(bids_of(&b), vec![(6200, 150)], "healthy book untouched");
        assert!(asks_of(&b).is_empty());
        assert_eq!(b.status(), Status::Ready);
    }

    /// **The trap this test exists for.** `Anchor Seq` is a channel-wide mktdata sequence while
    /// our own baseline advances only on this instrument's own deltas — every frame for
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
        assert!(
            !b.on_snapshot_end(100, 7),
            "level count short of total_levels"
        );
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

    /// A group is only ever opened from `Ready` once `K` proves we are behind, so the live book is
    /// already known-stale. Failing to replace it must stop us serving it — otherwise this path keeps
    /// claiming a market it knows it cannot serve, and the authority gate never fails over.
    #[test]
    fn a_failed_snapshot_end_from_ready_discards_the_stale_book() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            b.on_snapshot_begin(2, 500, 1, 9, 0),
            "K=9 > 5, we are behind"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(!b.on_snapshot_end(999, 2), "anchor mismatch");
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty(), "the known-stale book is gone");
        assert!(asks_of(&b).is_empty());
        assert_eq!(b.depth_bound(), None, "and so is its completeness claim");
    }

    /// A duplicated `SnapshotLevel` must not make up for a lost one: the count guards against
    /// truncation, so it counts distinct prices rather than datagrams.
    #[test]
    fn a_duplicated_snapshot_level_does_not_satisfy_the_count() {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(1, 100, 3, 0, 0));
        b.on_snapshot_level(1, SIDE_BID, 6200, 10, Some(1), 0);
        b.on_snapshot_level(1, SIDE_BID, 6200, 10, Some(1), 0); // duplicate of the same price
        b.on_snapshot_level(1, SIDE_BID, 6100, 20, Some(1), 0); // the third level is lost
        assert!(!b.on_snapshot_end(100, 1));
        assert_eq!(b.status(), Status::AwaitingSnapshot);
    }

    /// A begin we decline says nothing about a group already under assembly. Discarding it would
    /// strand the book in `BuildingSnapshot` with no group to end, so the real `SnapshotEnd` would
    /// find nothing and the stale levels would keep being served under a status that denies them.
    #[test]
    fn a_declined_snapshot_begin_leaves_the_open_group_alone() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(b.on_snapshot_begin(2, 500, 1, 9, 0));
        assert!(
            !b.on_snapshot_begin(3, 500, MAX_LEVELS_PER_BOOK as u32 + 1, 9, 0),
            "oversized -> declined"
        );
        b.on_snapshot_level(2, SIDE_ASK, 6300, 77, Some(1), 0);
        assert!(
            b.on_snapshot_end(500, 2),
            "the accepted group still installs"
        );
        assert_eq!(asks_of(&b), vec![(6300, 77)]);
    }

    /// The rotation path this module exists for: a `Ready` book rebuilds while deltas keep arriving.
    /// They must buffer rather than mutate the book being replaced, then replay onto the new one.
    #[test]
    fn a_rebuild_from_ready_buffers_deltas_and_replays_them() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 150)]);
        assert!(
            b.on_snapshot_begin(2, 500, 1, 9, 0),
            "K=9 > 5, we are behind"
        );
        assert!(matches!(
            apply_delta(&mut b, level(10, 501, SIDE_BID, 6100, 20, NEW)),
            DeltaOutcome::Buffered
        ));
        assert_eq!(
            bids_of(&b),
            vec![(6200, 150)],
            "the book under replacement is untouched"
        );
        b.on_snapshot_level(2, SIDE_BID, 6000, 5, Some(1), 0);
        assert!(b.on_snapshot_end(500, 2));
        assert_eq!(b.status(), Status::Ready);
        assert_eq!(
            bids_of(&b),
            vec![(6100, 20), (6000, 5)],
            "the buffered delta replayed onto the installed snapshot"
        );
        assert_eq!(b.buffered_len(), 0);
    }

    /// Publishers SHOULD emit levels best-to-worst, but subscribers MUST NOT depend on it: the
    /// levels of a group are a set, and our own sorted container establishes rank.
    #[test]
    fn snapshot_level_order_does_not_matter() {
        let b = synced(
            100,
            0,
            0,
            &[
                (SIDE_BID, 6100, 10),
                (SIDE_BID, 6200, 20),
                (SIDE_ASK, 6400, 40),
                (SIDE_ASK, 6300, 30),
            ],
        );
        assert_eq!(bids_of(&b), vec![(6200, 20), (6100, 10)], "bids descend");
        assert_eq!(asks_of(&b), vec![(6300, 30), (6400, 40)], "asks ascend");
    }

    /// Buffered deltas at/below the anchor are already in the snapshot and are dropped; those past
    /// it replay in mktdata-seq order.
    #[test]
    fn buffered_deltas_replay_past_the_anchor() {
        let mut b = PriceBook::new();
        apply_delta(&mut b, level(3, 98, SIDE_BID, 6000, 1, NEW)); // <= anchor, dropped
        apply_delta(&mut b, level(6, 101, SIDE_BID, 6100, 20, NEW));
        apply_delta(&mut b, level(7, 102, SIDE_BID, 6200, 30, NEW));
        assert_eq!(b.buffered_len(), 3);
        assert!(b.on_snapshot_begin(1, 100, 1, 5, 0));
        b.on_snapshot_level(1, SIDE_ASK, 6300, 99, Some(1), 0);
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(b.status(), Status::Ready);
        assert_eq!(
            bids_of(&b),
            vec![(6200, 30), (6100, 20)],
            "both post-anchor deltas replayed"
        );
        assert_eq!(b.buffered_len(), 0);
    }

    /// A duplicate inside the replay must not cost a re-bootstrap, but a genuine forward gap must.
    #[test]
    fn a_gap_in_the_replay_reverts_to_awaiting_snapshot() {
        let mut b = PriceBook::new();
        apply_delta(&mut b, level(6, 101, SIDE_BID, 6100, 20, NEW));
        apply_delta(&mut b, level(9, 104, SIDE_BID, 6200, 30, NEW)); // gap: 7, 8 missing
        assert!(b.on_snapshot_begin(1, 100, 0, 5, 0));
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(b.status(), Status::Gap);
    }

    // ---- BookClear ----

    /// `Scope = 0` clears the entire side(s) and `From Price` is ignored. A subscriber that applies
    /// a clear stays `Ready` — it is not a resynchronization signal.
    #[test]
    fn clear_entire_side_stays_ready() {
        let mut b = synced(
            100,
            5,
            0,
            &[
                (SIDE_BID, 6200, 10),
                (SIDE_BID, 6100, 20),
                (SIDE_ASK, 6300, 30),
            ],
        );
        assert!(matches!(
            apply_delta(
                &mut b,
                clear(6, 101, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 9_999)
            ),
            DeltaOutcome::Applied { .. }
        ));
        assert!(bids_of(&b).is_empty());
        assert_eq!(asks_of(&b), vec![(6300, 30)]);
        assert_eq!(b.status(), Status::Ready);
    }

    #[test]
    fn clear_both_sides_empties_the_book() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_ASK, 6300, 30)]);
        apply_delta(&mut b, clear(6, 101, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, 0));
        assert!(bids_of(&b).is_empty() && asks_of(&b).is_empty());
    }

    /// `Scope = 1` clears from `From Price` outward: for bids every level at or BELOW it, for asks
    /// every level at or ABOVE it. Inclusive.
    #[test]
    fn clear_from_price_clears_outward_inclusively() {
        let mut b = synced(
            100,
            5,
            0,
            &[
                (SIDE_BID, 6200, 10),
                (SIDE_BID, 6100, 20),
                (SIDE_BID, 6000, 30),
                (SIDE_ASK, 6300, 40),
                (SIDE_ASK, 6400, 50),
                (SIDE_ASK, 6500, 60),
            ],
        );
        apply_delta(
            &mut b,
            clear(6, 101, CLEAR_SIDE_BID, SCOPE_FROM_PRICE, 6100),
        );
        assert_eq!(bids_of(&b), vec![(6200, 10)], "6100 and 6000 gone");
        apply_delta(
            &mut b,
            clear(7, 102, CLEAR_SIDE_ASK, SCOPE_FROM_PRICE, 6400),
        );
        assert_eq!(asks_of(&b), vec![(6300, 40)], "6400 and 6500 gone");
    }

    /// One price cannot bound both sides. Guessing at what was meant would silently empty a live
    /// book, so the malformed clear consumes its sequence and changes nothing.
    #[test]
    fn clear_from_price_on_both_sides_is_malformed_and_clears_nothing() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_ASK, 6300, 30)]);
        assert!(matches!(
            apply_delta(
                &mut b,
                clear(6, 101, CLEAR_SIDE_BOTH, SCOPE_FROM_PRICE, 6250)
            ),
            DeltaOutcome::Applied { .. }
        ));
        assert_eq!(bids_of(&b), vec![(6200, 10)]);
        assert_eq!(asks_of(&b), vec![(6300, 30)]);
        assert!(
            matches!(
                apply_delta(&mut b, level(7, 102, SIDE_BID, 6100, 20, NEW)),
                DeltaOutcome::Applied { .. }
            ),
            "the malformed clear still consumed seq 6"
        );
    }

    /// The same rule for every scope byte the registry has not assigned. [`clear_side_levels`]
    /// derives its behaviour from the complement (`entire = scope == SCOPE_ENTIRE_SIDE`), so
    /// `2..=255` all act as from-price — which means a guard testing `== SCOPE_FROM_PRICE` lets
    /// them through to clear bids at/below and asks at/above one bound: the whole book, republished
    /// to every consumer as `Delete`s. Only the recognized whole-side scope may pass.
    #[test]
    fn clear_on_both_sides_with_an_unrecognized_scope_clears_nothing() {
        for scope in [2u8, 3, 17, 255] {
            let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10), (SIDE_ASK, 6300, 30)]);
            let mut removed = Vec::new();
            assert!(matches!(
                b.on_delta(clear(6, 101, CLEAR_SIDE_BOTH, scope, 6250), &mut removed),
                DeltaOutcome::Applied { .. }
            ));
            assert_eq!(bids_of(&b), vec![(6200, 10)], "scope {scope}");
            assert_eq!(asks_of(&b), vec![(6300, 30)], "scope {scope}");
            assert!(
                removed.is_empty(),
                "scope {scope} reported {removed:?} as removed"
            );
        }
    }

    /// A clear shares the delta sequence with level updates — both mutate the book and their
    /// relative order is significant — so it is classified identically.
    #[test]
    fn clear_shares_the_delta_sequence() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        assert!(matches!(
            apply_delta(&mut b, clear(5, 100, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 0)),
            DeltaOutcome::Duplicate
        ));
        assert_eq!(bids_of(&b), vec![(6200, 10)]);
        assert!(matches!(
            apply_delta(&mut b, clear(8, 103, CLEAR_SIDE_BID, SCOPE_ENTIRE_SIDE, 0)),
            DeltaOutcome::Gap
        ));
    }

    // ---- Reported clear removals ----

    /// A consumer whose only clear primitive is whole-side has to re-express `SCOPE_FROM_PRICE` as
    /// per-level deletes, so the exact set matters: reporting a survivor would delete a level we
    /// still hold, and omitting a removal would leave the consumer holding one we dropped.
    #[test]
    fn clear_from_price_reports_exactly_the_removed_levels() {
        let mut b = synced(
            100,
            5,
            0,
            &[
                (SIDE_BID, 6200, 10),
                (SIDE_BID, 6100, 20),
                (SIDE_BID, 6000, 30),
                (SIDE_ASK, 6300, 40),
                (SIDE_ASK, 6400, 50),
                (SIDE_ASK, 6500, 60),
            ],
        );
        let mut removed = Vec::new();
        b.on_delta(
            clear(6, 101, CLEAR_SIDE_BID, SCOPE_FROM_PRICE, 6100),
            &mut removed,
        );
        removed.sort_unstable();
        assert_eq!(removed, vec![(SIDE_BID, 6000), (SIDE_BID, 6100)]);
        b.on_delta(
            clear(7, 102, CLEAR_SIDE_ASK, SCOPE_FROM_PRICE, 6400),
            &mut removed,
        );
        removed.sort_unstable();
        assert_eq!(removed, vec![(SIDE_ASK, 6400), (SIDE_ASK, 6500)]);
    }

    #[test]
    fn clear_entire_side_reports_every_level_of_both_sides() {
        let mut b = synced(
            100,
            5,
            0,
            &[
                (SIDE_BID, 6200, 10),
                (SIDE_BID, 6100, 20),
                (SIDE_ASK, 6300, 30),
            ],
        );
        let mut removed = Vec::new();
        b.on_delta(
            clear(6, 101, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, 0),
            &mut removed,
        );
        removed.sort_unstable();
        assert_eq!(
            removed,
            vec![(SIDE_BID, 6100), (SIDE_BID, 6200), (SIDE_ASK, 6300)]
        );
    }

    /// A buffered clear has not touched the book, so it has removed nothing to report.
    #[test]
    fn a_buffered_clear_reports_nothing() {
        let mut b = PriceBook::new();
        let mut removed = Vec::new();
        assert_eq!(
            b.on_delta(
                clear(6, 101, CLEAR_SIDE_BOTH, SCOPE_ENTIRE_SIDE, 0),
                &mut removed
            ),
            DeltaOutcome::Buffered
        );
        assert!(removed.is_empty());
    }

    /// The buffer is cleared at entry, so a caller reusing one scratch across deltas can never read
    /// the previous call's levels as this one's.
    #[test]
    fn a_stale_removed_buffer_is_not_visible_to_the_next_call() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        let mut removed = vec![(SIDE_ASK, 9_999)];
        b.on_delta(level(6, 101, SIDE_BID, 6100, 20, NEW), &mut removed);
        assert!(removed.is_empty(), "a level update removes no levels");
    }

    // ---- InstrumentReset ----

    /// Discard the book and any open snapshot, drop buffered deltas at/below `S'`, and await a
    /// snapshot anchored at `S'` or newer — discarding any older one.
    #[test]
    fn instrument_reset_requires_an_anchor_at_or_past_the_new_one() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        apply_delta(&mut b, level(6, 101, SIDE_BID, 6100, 20, NEW));
        b.on_instrument_reset(500);
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty());
        assert_eq!(b.buffered_len(), 0, "buffered deltas at/below S' discarded");
        assert!(
            !b.on_snapshot_begin(2, 499, 0, 9, 0),
            "older than S' -> discarded"
        );
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(
            b.on_snapshot_begin(3, 501, 0, 9, 0),
            "at or past S' -> accepted"
        );
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
        assert!(
            !b.on_snapshot_begin(3, 600, 0, 9, 0),
            "K == ours -> ignored, not anchor-blocked"
        );
        // Below the old requirement and behind on K: accepted only because the anchor no longer gates.
        assert!(b.on_snapshot_begin(4, 400, 0, 99, 0));
    }

    /// Deltas past `S'` survive the reset and replay onto the recovery snapshot; only those the reset
    /// supersedes are dropped.
    #[test]
    fn instrument_reset_keeps_post_anchor_buffered_deltas() {
        let mut b = PriceBook::new();
        apply_delta(&mut b, level(6, 500, SIDE_BID, 6000, 1, NEW)); // at S', superseded
        apply_delta(&mut b, level(7, 501, SIDE_BID, 6100, 20, NEW)); // past S', kept
        b.on_instrument_reset(500);
        assert_eq!(b.buffered_len(), 1);
        assert!(b.on_snapshot_begin(2, 500, 0, 6, 0));
        assert!(b.on_snapshot_end(500, 2));
        assert_eq!(
            bids_of(&b),
            vec![(6100, 20)],
            "only the kept delta replayed"
        );
    }

    // ---- EndOfSession ----

    /// The session's book, sequences and event clock are all over. Unlike a reset there is no
    /// forward anchor, so buffered deltas belong to the ended session and are discarded outright.
    /// Zeroing the event clock keeps the resync from stamping its first output with pre-session
    /// time.
    #[test]
    fn end_of_session_drops_everything_including_the_event_clock() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        apply_delta(&mut b, level(6, 101, SIDE_BID, 6100, 20, NEW));
        assert!(b.last_event_ts() > 0);
        b.on_end_of_session();
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty());
        assert_eq!(b.buffered_len(), 0);
        assert_eq!(b.last_event_ts(), 0);
        assert_eq!(
            b.depth_bound(),
            None,
            "no publisher claim survives the session"
        );
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
        apply_delta(&mut b, level(6, 101, SIDE_ASK, 6200, 5, NEW)); // locked
        assert!(!b.crossed(), "locked is not crossed");
        apply_delta(&mut b, level(7, 102, SIDE_ASK, 6100, 5, NEW)); // crossed
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
            apply_delta(&mut b, level(i, i as u64, SIDE_BID, 6200, 1, NEW));
        }
        assert_eq!(b.buffered_len(), MAX_BUFFERED_DELTAS);
    }

    /// `drop_buffer` is the action behind the cross-instrument overflow policy: the instrument
    /// holding the most buffered data is dropped and marked `Gap`, recovering on its next snapshot
    /// exactly as any other `Gap` instrument does.
    #[test]
    fn drop_buffer_marks_the_instrument_gap() {
        let mut b = synced(100, 5, 0, &[(SIDE_BID, 6200, 10)]);
        apply_delta(&mut b, level(9, 104, SIDE_BID, 6100, 20, NEW)); // gap
        assert!(b.buffered_len() > 0);
        b.drop_buffer();
        assert_eq!(b.buffered_len(), 0);
        assert_eq!(b.status(), Status::Gap);
    }

    // ---- Level bound ----

    /// The multicast source is unauthenticated, so a forged stream of level updates at distinct
    /// prices must not grow the book without limit. At the cap the book *and* the buffer are
    /// discarded — leaving the buffer would just relocate the flood — and the instrument awaits a
    /// snapshot rather than declaring a gap it never had.
    #[test]
    fn levels_are_bounded_and_overflow_awaits_a_snapshot() {
        let mut b = synced(100, 0, 0, &[]);
        let mut outcome = DeltaOutcome::Buffered;
        for i in 1..=(MAX_LEVELS_PER_BOOK as u32 + 1) {
            outcome = apply_delta(&mut b, level(i, i as u64, SIDE_BID, i as i64, 1, NEW));
        }
        assert_eq!(
            outcome,
            DeltaOutcome::Overflow,
            "the level past the cap is refused, and not as a sequence gap"
        );
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(bids_of(&b).is_empty(), "the book is discarded, not grown");
        assert_eq!(b.buffered_len(), 0, "and so is the buffer");
    }

    /// A snapshot promising more levels than any real book holds is malformed or forged; refuse to
    /// open the group at all, so the shadow can never be grown past the cap.
    #[test]
    fn snapshot_begin_rejects_an_oversized_total_levels() {
        let mut b = PriceBook::new();
        assert!(!b.on_snapshot_begin(1, 100, MAX_LEVELS_PER_BOOK as u32 + 1, 0, 0));
        assert_eq!(b.status(), Status::AwaitingSnapshot);
        assert!(!b.on_snapshot_end(100, 1), "no group was opened");
    }

    /// ...and the shadow is bounded independently, since a group can keep sending levels past the
    /// `total_levels` it promised.
    #[test]
    fn snapshot_shadow_levels_are_bounded() {
        let mut b = PriceBook::new();
        assert!(b.on_snapshot_begin(1, 100, MAX_LEVELS_PER_BOOK as u32, 0, 0));
        for i in 1..=(MAX_LEVELS_PER_BOOK as i64 + 100) {
            b.on_snapshot_level(1, SIDE_BID, i, 1, Some(1), 0);
        }
        assert!(b.on_snapshot_end(100, 1));
        assert_eq!(
            b.bids().count(),
            MAX_LEVELS_PER_BOOK,
            "the surplus is refused"
        );
    }
}
