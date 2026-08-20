//! Decoder for the DoubleZero Edge **Market-by-Price** feed (datagram magic `0x4442`).
//!
//! Price-aggregated L2: each `LevelUpdate` states the complete resulting state of one price level,
//! with in-band snapshot+delta recovery on a third port. Shares the 24-byte datagram header, 4-byte
//! message header and generic datagram-walker in [`crate::ingest::codec_common`]; only the magic and
//! the bodies differ.
//!
//! **Validated field-for-field against `go/marketbyprice-parser`** (edge-multicast-ref, merged
//! PR #29), so this ships offset-validated rather than draft-only — the trap `codec_midpoint` is
//! still in. Two things the oracle does that the sibling codecs here do not, both deliberate:
//!
//! * **Exact body-length equality per type, not `>=`, paired with a `SCHEMA_VERSION` gate.** The
//!   forward-compatibility rule that a decoder ignores trailing bytes applies across a Schema
//!   Version bump; within v1 an unexpected length is malformed. The two rules are one decision: a v2
//!   datagram whose bodies legally grew fails the length rule for every message, and the version gate is
//!   what turns that from a silent feed of `Other` into a decode error. Either way a bumped schema
//!   goes dark — the gate is what makes it visible. The length rule is load-bearing because
//!   `SnapshotBegin` is a prefix-superset of
//!   the market-by-order feed's — the first 36 message bytes are identical and `Depth Bound` is
//!   appended at message offset 36 — so a sibling-shaped body would otherwise decode with
//!   `depth_bound` read from whatever follows the body: the next message's header bytes, or trailing
//!   padding, where a `0` is a positive publisher claim of a complete book that no publisher made.
//! * **Enums decode permissively**: any `u8` is accepted and unknown values mean Unknown, per the
//!   spec's "receivers MUST accept any `u8`". The opposite of the top-of-book codec's strict decode.
//!
//! `Side` (0=Bid, 1=Ask) and `Aggressor Side` (0=Unknown, 1=Buy, 2=Sell) are DIFFERENT value
//! spaces. They have separate constants here and must never share one.
//!
//! **Oracle strength: real-capture backed.** Every offset is pinned by the offset-independent unit
//! tests below plus the Go decoder above, and `tests/codec_mbp_fixtures.rs` decodes two committed
//! captures of the live publisher — a sharded multi-channel one and a dense single-channel one —
//! requiring zero unroutable messages, snapshot groups whose levels match their promised
//! `total_levels`, a gapless per-instrument delta run, and a `depth_bound` the publisher stated.
//! `BookClear`, `InstrumentReset`, `BatchBoundary` and `EndOfSession` appear in neither capture and
//! so remain offset-test-only; see `tests/fixtures/PROVENANCE.md`, which also records the publisher
//! deviations those captures contain.
//!
//! Oracle parity is per-*body*. The shared datagram walker stays looser than the oracle on two header
//! checks it does not make — a `datagram_length` disagreeing with the datagram (the walker clamps) and
//! a zero message count (an empty message list) — which is pre-existing and shared by all four
//! codecs. The third, schema version, is checked here rather than left to the walker, because only
//! this codec's body rule depends on it.

use anyhow::Result;

pub use crate::ingest::codec_common::InstrumentDefinition;
use crate::ingest::codec_common::{
    decode_datagram_with, i64le, instrument_definition, u16le, u32le, u64le, u8le, DatagramHeader,
    MSG_HEADER_SIZE, SCHEMA_V1, SCHEMA_V3,
};

pub const MAGIC: u16 = 0x4442; // "BD"

/// Wire generations this feed implements. `2` is deliberately absent — see [`SCHEMA_V1`].
///
/// This gate used to live in [`decode_datagram`] because it was the only codec that had one; it now
/// rides the shared walker with its siblings, so a codec cannot ship without it.
const SUPPORTED_VERSIONS: &[u8] = &[SCHEMA_V1, SCHEMA_V3];

// Shared with the top-of-book feed (byte-identical layouts).
pub const MSG_HEARTBEAT: u8 = 0x01;
pub const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const MSG_TRADE: u8 = 0x04;
pub const MSG_END_OF_SESSION: u8 = 0x06;
pub const MSG_MANIFEST_SUMMARY: u8 = 0x07;
pub const MSG_LIQUIDATION: u8 = 0x08;

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

/// Trade aggressor. NOT the book `Side` value space — see the module doc.
pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

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

/// Total on-wire message sizes, including the 4-byte header. Enforced exactly (see the module doc).
pub mod sizes {
    pub const HEARTBEAT: usize = 16;
    pub const INSTRUMENT_DEFINITION: usize = 80;
    pub const TRADE: usize = 52;
    pub const END_OF_SESSION: usize = 12;
    pub const MANIFEST_SUMMARY: usize = 24;
    pub const BATCH_BOUNDARY: usize = 16;
    pub const INSTRUMENT_RESET: usize = 28;
    pub const SNAPSHOT_BEGIN: usize = 40;
    pub const SNAPSHOT_END: usize = 20;
    pub const LEVEL_UPDATE: usize = 48;
    pub const BOOK_CLEAR: usize = 36;
    pub const SNAPSHOT_LEVEL: usize = 32;
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

/// The feed's core message: the complete resulting state of one price level. `qty_raw` is the
/// level's **absolute** resulting quantity; `0` removes the level. `action` is informational and
/// MUST NOT gate the apply.
#[derive(Debug, Clone)]
pub struct LevelUpdate {
    pub instrument_id: u32,
    pub source_id: u16,
    pub side: u8,
    pub action: u8,
    pub per_instrument_seq: u32,
    pub price_raw: i64,
    pub qty_raw: u64,
    pub ts: u64,
    pub order_count: Option<u16>,
    /// Informational rank. Never a key, never a locator, and invalid after any later update to the
    /// same side.
    pub level_index: Option<u16>,
    pub update_reason: u8,
    pub level_flags: u8,
}

/// Bulk removal. Asserts the named levels are gone; NOT a resynchronization signal — a subscriber
/// that applies it stays ready.
#[derive(Debug, Clone)]
pub struct BookClear {
    pub instrument_id: u32,
    pub source_id: u16,
    pub clear_side: u8,
    pub scope: u8,
    pub per_instrument_seq: u32,
    pub from_price_raw: i64,
    pub ts: u64,
    pub clear_reason: u8,
}

/// One level of a snapshot. No instrument id: it is implied by the containing `SnapshotBegin`, so
/// routing keys on the open group.
#[derive(Debug, Clone)]
pub struct SnapshotLevel {
    pub snapshot_id: u32,
    pub price_raw: i64,
    pub qty_raw: u64,
    pub order_count: Option<u16>,
    pub side: u8,
    pub level_flags: u8,
}

#[derive(Debug, Clone)]
pub struct SnapshotBegin {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub total_levels: u32,
    pub snapshot_id: u32,
    pub last_instrument_seq: u32,
    pub ts: u64,
    /// `0` is a positive publisher claim that this snapshot carries the complete book. Non-zero is
    /// levels-per-side, beyond which state is **unknown, not empty**.
    pub depth_bound: u32,
}

#[derive(Debug, Clone)]
pub struct SnapshotEnd {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub snapshot_id: u32,
}

#[derive(Debug, Clone)]
pub struct BatchBoundary {
    pub batch_id: u32,
    pub batch_time: u64,
}

/// Carries no per-instrument seq — processed regardless of sequence state.
#[derive(Debug, Clone)]
pub struct InstrumentReset {
    pub instrument_id: u32,
    pub reason: u8,
    pub new_anchor_seq: u64,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub enum Message {
    Heartbeat(u64),
    InstrumentDefinition(InstrumentDefinition),
    Trade(Trade),
    EndOfSession(u64),
    ManifestSummary(ManifestSummary),
    LevelUpdate(LevelUpdate),
    BookClear(BookClear),
    SnapshotLevel(SnapshotLevel),
    SnapshotBegin(SnapshotBegin),
    SnapshotEnd(SnapshotEnd),
    BatchBoundary(BatchBoundary),
    InstrumentReset(InstrumentReset),
    /// Reserved (`0x03`/`0x05`), unknown, or malformed-length: skipped by its declared length.
    /// Carries the type byte, so a caller can tell "this decoder rejects the feed's core message"
    /// from "a reserved type went by".
    Other(u8),
}

/// Decode one datagram. Two rules make this stricter than the sibling codecs, and they depend on
/// each other: a datagram declaring an unimplemented [`SCHEMA_VERSION`] is rejected whole, and within
/// v1 `msg_len` must equal the type's declared size exactly before any field is read, so a mis-sized
/// body becomes [`Message::Other`] rather than decoding garbage into a field that has semantics (the
/// module doc's `Depth Bound` case). Without the version gate the length rule would apply v1 sizes
/// to a v2 datagram whose bodies legally grew, and the whole feed would decode to `Other` in silence.
pub fn decode_datagram(buf: &[u8]) -> Result<(DatagramHeader, Vec<Message>)> {
    decode_datagram_with(buf, MAGIC, SUPPORTED_VERSIONS, |ty, _flags, b, off, ver| {
        // In bounds: the walker breaks before calling this unless `off + MSG_HEADER_SIZE` fits.
        let msg_len = b[off + 1] as usize;
        let body = off + MSG_HEADER_SIZE;
        let exact = |n: usize| msg_len == n;
        match ty {
            MSG_HEARTBEAT if exact(sizes::HEARTBEAT) => {
                decode_heartbeat(b, body).unwrap_or(Message::Other(ty))
            }
            // The one type whose size is version-dependent (80 at v1, 130 at v3), so the exact
            // rule cannot apply; `instrument_definition` does its own version-aware length check.
            MSG_INSTRUMENT_DEFINITION => instrument_definition(b, off, ver)
                .map(Message::InstrumentDefinition)
                .unwrap_or(Message::Other(ty)),
            MSG_TRADE if exact(sizes::TRADE) => decode_trade(b, body).unwrap_or(Message::Other(ty)),
            MSG_END_OF_SESSION if exact(sizes::END_OF_SESSION) => u64le(b, body)
                .map(Message::EndOfSession)
                .unwrap_or(Message::Other(ty)),
            MSG_MANIFEST_SUMMARY if exact(sizes::MANIFEST_SUMMARY) => {
                decode_manifest_summary(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_LEVEL_UPDATE if exact(sizes::LEVEL_UPDATE) => {
                decode_level_update(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_BOOK_CLEAR if exact(sizes::BOOK_CLEAR) => {
                decode_book_clear(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_SNAPSHOT_LEVEL if exact(sizes::SNAPSHOT_LEVEL) => {
                decode_snapshot_level(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_SNAPSHOT_BEGIN if exact(sizes::SNAPSHOT_BEGIN) => {
                decode_snapshot_begin(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_SNAPSHOT_END if exact(sizes::SNAPSHOT_END) => {
                decode_snapshot_end(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_BATCH_BOUNDARY if exact(sizes::BATCH_BOUNDARY) => {
                decode_batch_boundary(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_INSTRUMENT_RESET if exact(sizes::INSTRUMENT_RESET) => {
                decode_instrument_reset(b, body).unwrap_or(Message::Other(ty))
            }
            // `0x03`/`0x05` are reserved to stop a misrouted sibling datagram cross-decoding, and
            // `MSG_LIQUIDATION` carries nothing this bridge re-serves. Both fall through here.
            _ => Message::Other(ty),
        }
    })
}

fn decode_heartbeat(b: &[u8], o: usize) -> Option<Message> {
    Some(Message::Heartbeat(u64le(b, o + 4)?))
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
        valid: u8le(b, o + 1)? != 0,
        manifest_seq: u16le(b, o + 4)?,
        instrument_count: u32le(b, o + 8)?,
        ts: u64le(b, o + 12)?,
    }))
}

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
    // book logic means a bad datagram can never clear both sides from a single bound.
    //
    // Tested against the recognized **whole-side** scope, never against `SCOPE_FROM_PRICE`: enums
    // decode permissively by design, so an unassigned `2..=255` reaches here intact, and
    // `PriceBook::clear_side_levels` derives its behaviour from this same complement
    // (`entire = scope == SCOPE_ENTIRE_SIDE`) — every unrecognized byte therefore acts as a
    // price-bounded clear down there. An `== SCOPE_FROM_PRICE` test would let those through to
    // empty a live book from one bound.
    if clear_side == CLEAR_SIDE_BOTH && scope != SCOPE_ENTIRE_SIDE {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Build a whole datagram around `messages`, exposing the header fields a subscriber keys state on.
    pub(crate) fn datagram(
        channel_id: u8,
        reset_count: u8,
        sequence: u64,
        messages: &[Vec<u8>],
    ) -> Vec<u8> {
        let body: Vec<u8> = messages.concat();
        let mut f = vec![0u8; 24];
        f[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        f[2] = SCHEMA_V1;
        f[3] = channel_id;
        f[4..12].copy_from_slice(&sequence.to_le_bytes());
        f[12..20].copy_from_slice(&1_700_000_000_000_000_000u64.to_le_bytes());
        f[20] = messages.len() as u8;
        f[21] = reset_count;
        f[22..24].copy_from_slice(&((24 + body.len()) as u16).to_le_bytes());
        f.extend_from_slice(&body);
        f
    }

    /// Inverse of [`u16_opt`]: `None` is the unavailable sentinel, never a magnitude.
    fn u16_wire(v: Option<u16>) -> u16 {
        v.unwrap_or(U16_UNAVAILABLE)
    }

    pub(crate) fn enc_instrument_definition(d: &InstrumentDefinition) -> Vec<u8> {
        let mut b = vec![0u8; sizes::INSTRUMENT_DEFINITION - MSG_HEADER_SIZE];
        b[0..4].copy_from_slice(&d.instrument_id.to_le_bytes());
        let sym = &d.symbol.as_bytes()[..d.symbol.len().min(16)];
        b[4..4 + sym.len()].copy_from_slice(sym); // 16B NUL-padded field
        b[37] = d.price_exponent as u8;
        b[38] = d.qty_exponent as u8;
        b[74..76].copy_from_slice(&d.manifest_seq.to_le_bytes());
        msg(MSG_INSTRUMENT_DEFINITION, 0, &b)
    }

    pub(crate) fn enc_manifest_summary(m: &ManifestSummary) -> Vec<u8> {
        let mut b = vec![m.channel_id, m.valid as u8, 0, 0]; // pad -> manifest_seq @4
        b.extend_from_slice(&m.manifest_seq.to_le_bytes());
        b.extend_from_slice(&[0u8; 2]); // pad -> instrument_count @8
        b.extend_from_slice(&m.instrument_count.to_le_bytes());
        b.extend_from_slice(&m.ts.to_le_bytes());
        msg(MSG_MANIFEST_SUMMARY, 0, &b)
    }

    pub(crate) fn enc_trade(t: &Trade) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&t.instrument_id.to_le_bytes());
        b.extend_from_slice(&t.source_id.to_le_bytes());
        b.push(t.aggressor_side);
        b.push(t.trade_flags);
        b.extend_from_slice(&t.source_ts.to_le_bytes());
        b.extend_from_slice(&t.trade_price_raw.to_le_bytes());
        b.extend_from_slice(&t.trade_qty_raw.to_le_bytes());
        b.extend_from_slice(&t.trade_id.to_le_bytes());
        b.extend_from_slice(&t.cumulative_volume_raw.to_le_bytes());
        msg(MSG_TRADE, 0, &b)
    }

    pub(crate) fn enc_level_update(u: &LevelUpdate) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&u.instrument_id.to_le_bytes());
        b.extend_from_slice(&u.source_id.to_le_bytes());
        b.push(u.side);
        b.push(u.action);
        b.extend_from_slice(&u.per_instrument_seq.to_le_bytes());
        b.extend_from_slice(&u.price_raw.to_le_bytes());
        b.extend_from_slice(&u.qty_raw.to_le_bytes());
        b.extend_from_slice(&u.ts.to_le_bytes());
        b.extend_from_slice(&u16_wire(u.order_count).to_le_bytes());
        b.extend_from_slice(&u16_wire(u.level_index).to_le_bytes());
        b.push(u.update_reason);
        b.push(u.level_flags);
        b.extend_from_slice(&[0u8; 2]); // trailing pad -> 44
        msg(MSG_LEVEL_UPDATE, 0, &b)
    }

    pub(crate) fn enc_book_clear(c: &BookClear) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&c.instrument_id.to_le_bytes());
        b.extend_from_slice(&c.source_id.to_le_bytes());
        b.push(c.clear_side);
        b.push(c.scope);
        b.extend_from_slice(&c.per_instrument_seq.to_le_bytes());
        b.extend_from_slice(&c.from_price_raw.to_le_bytes());
        b.extend_from_slice(&c.ts.to_le_bytes());
        b.push(c.clear_reason);
        b.extend_from_slice(&[0u8; 3]); // trailing pad -> 32
        msg(MSG_BOOK_CLEAR, 0, &b)
    }

    pub(crate) fn enc_snapshot_level(l: &SnapshotLevel) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&l.snapshot_id.to_le_bytes());
        b.extend_from_slice(&l.price_raw.to_le_bytes());
        b.extend_from_slice(&l.qty_raw.to_le_bytes());
        b.extend_from_slice(&u16_wire(l.order_count).to_le_bytes());
        b.push(l.side);
        b.push(l.level_flags);
        b.extend_from_slice(&[0u8; 4]); // trailing pad -> 28
        msg(MSG_SNAPSHOT_LEVEL, 0, &b)
    }

    pub(crate) fn enc_snapshot_begin(s: &SnapshotBegin) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&s.instrument_id.to_le_bytes());
        b.extend_from_slice(&s.anchor_seq.to_le_bytes());
        b.extend_from_slice(&s.total_levels.to_le_bytes());
        b.extend_from_slice(&s.snapshot_id.to_le_bytes());
        b.extend_from_slice(&s.last_instrument_seq.to_le_bytes());
        b.extend_from_slice(&s.ts.to_le_bytes());
        b.extend_from_slice(&s.depth_bound.to_le_bytes());
        msg(MSG_SNAPSHOT_BEGIN, 0, &b)
    }

    pub(crate) fn enc_snapshot_end(e: &SnapshotEnd) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&e.instrument_id.to_le_bytes());
        b.extend_from_slice(&e.anchor_seq.to_le_bytes());
        b.extend_from_slice(&e.snapshot_id.to_le_bytes());
        msg(MSG_SNAPSHOT_END, 0, &b)
    }

    pub(crate) fn enc_batch_boundary(bb: &BatchBoundary) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&bb.batch_id.to_le_bytes());
        b.extend_from_slice(&bb.batch_time.to_le_bytes());
        msg(MSG_BATCH_BOUNDARY, 0, &b)
    }

    pub(crate) fn enc_instrument_reset(r: &InstrumentReset) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&r.instrument_id.to_le_bytes());
        b.push(r.reason);
        b.extend_from_slice(&[0u8; 3]); // reserved -> new_anchor_seq @8
        b.extend_from_slice(&r.new_anchor_seq.to_le_bytes());
        b.extend_from_slice(&r.ts.to_le_bytes());
        msg(MSG_INSTRUMENT_RESET, 0, &b)
    }

    pub(crate) fn enc_heartbeat(ts: u64) -> Vec<u8> {
        let mut b = vec![0u8; 4]; // channel_id + pad -> ts @4
        b.extend_from_slice(&ts.to_le_bytes());
        msg(MSG_HEARTBEAT, 0, &b)
    }

    pub(crate) fn enc_end_of_session(ts: u64) -> Vec<u8> {
        msg(MSG_END_OF_SESSION, 0, &ts.to_le_bytes())
    }

    /// Build a 24-byte MBP datagram header carrying `msg_count` messages and `body_len` body bytes.
    fn datagram_header(msg_count: u8, reset_count: u8, body_len: usize) -> Vec<u8> {
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

    /// Wrap a body in its 4-byte message header. The declared length is the TOTAL message length.
    fn msg(ty: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let mut m = vec![ty, (4 + body.len()) as u8];
        m.extend_from_slice(&flags.to_le_bytes());
        m.extend_from_slice(body);
        m
    }

    fn one(ty: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let m = msg(ty, flags, body);
        let mut f = datagram_header(1, 0, m.len());
        f.extend_from_slice(&m);
        f
    }

    #[test]
    fn rejects_a_sibling_protocols_magic() {
        let mut f = one(MSG_HEARTBEAT, 0, &[0u8; 12]);
        f[0..2].copy_from_slice(&0x4444u16.to_le_bytes()); // market-by-order
        assert!(decode_datagram(&f).is_err());
    }

    /// A datagram declaring a schema this decoder does not implement is discarded whole. Without this
    /// the exact-length rule would apply v1 sizes to a v2 datagram whose bodies legally grew, and the
    /// whole feed would decode to `Other` with no error to see.
    #[test]
    fn rejects_an_unimplemented_schema_version() {
        let mut f = one(MSG_HEARTBEAT, 0, &[0u8; 12]);
        f[2] = SCHEMA_V1 + 1;
        assert!(decode_datagram(&f).is_err());
    }

    #[test]
    fn datagram_header_fields_decode() {
        let (h, _) = decode_datagram(&one(MSG_HEARTBEAT, 0, &[0u8; 12])).unwrap();
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
        let (_, m) = decode_datagram(&one(MSG_HEARTBEAT, 0, &b)).unwrap();
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
        let (_, m) = decode_datagram(&one(MSG_INSTRUMENT_DEFINITION, 0, &b)).unwrap();
        let Message::InstrumentDefinition(d) = &m[0] else {
            panic!("{:?}", m[0])
        };
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
        let (_, m) = decode_datagram(&one(MSG_TRADE, 0, &b)).unwrap();
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
        let (_, m) = decode_datagram(&one(MSG_MANIFEST_SUMMARY, 0, &b)).unwrap();
        let Message::ManifestSummary(s) = &m[0] else {
            panic!()
        };
        assert!(s.valid);
        assert_eq!(s.manifest_seq, 3);
        assert_eq!(s.instrument_count, 13);
    }

    /// Any non-zero `Valid` is true, matching the byte-validated top-of-book and market-by-order
    /// decoders. The Go oracle keeps the raw `u8` and does not adjudicate.
    #[test]
    fn manifest_summary_valid_is_any_non_zero() {
        let mut b = vec![0u8; 20];
        b[1] = 2;
        let (_, m) = decode_datagram(&one(MSG_MANIFEST_SUMMARY, 0, &b)).unwrap();
        let Message::ManifestSummary(s) = &m[0] else {
            panic!()
        };
        assert!(s.valid);
    }

    /// spec: EndOfSession 0x06, 12 bytes. Body: ts @0.
    #[test]
    fn end_of_session_decodes() {
        let (_, m) = decode_datagram(&one(MSG_END_OF_SESSION, 0, &42u64.to_le_bytes())).unwrap();
        assert!(matches!(m[0], Message::EndOfSession(42)));
    }

    /// `0x03` (Quote in the top-of-book feed) and `0x05` are reserved here **specifically** so a
    /// misrouted sibling datagram cannot cross-decode. They must skip by length, never decode.
    #[test]
    fn reserved_types_do_not_decode() {
        for ty in [0x03u8, 0x05] {
            let (_, m) = decode_datagram(&one(ty, 0, &[0u8; 20])).unwrap();
            assert!(
                matches!(m[0], Message::Other(t) if t == ty),
                "type {ty:#04x} decoded"
            );
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
        let mut f = datagram_header(2, 0, unknown.len() + hb.len());
        f.extend_from_slice(&unknown);
        f.extend_from_slice(&hb);
        let (_, m) = decode_datagram(&f).unwrap();
        assert!(matches!(m[0], Message::Other(0x7F)));
        assert!(matches!(m[1], Message::Heartbeat(77)));
    }

    /// Exact length equality, not `>=`. The forward-compat "ignore trailing bytes" rule applies
    /// across a Schema Version bump; within one generation an unexpected body length is malformed.
    /// Matches the Go oracle's `TestNewBodies_ExactLengthOnly` /
    /// `TestInheritedBodies_ExactLengthOnly`.
    ///
    /// `InstrumentDefinition` is deliberately absent: it is the one type whose size is
    /// version-dependent (80 bytes at v1, 130 at v3), so it is length-checked per version by
    /// `codec_common::instrument_definition` instead — see
    /// [`an_over_long_instrument_definition_still_decodes`].
    #[test]
    fn wrong_body_length_does_not_decode() {
        for (ty, correct) in [
            (MSG_HEARTBEAT, 12usize),
            (MSG_TRADE, 48),
            (MSG_END_OF_SESSION, 8),
            (MSG_MANIFEST_SUMMARY, 20),
        ] {
            for len in [correct - 1, correct + 1] {
                let (_, m) = decode_datagram(&one(ty, 0, &vec![0u8; len])).unwrap();
                assert!(
                    matches!(m[0], Message::Other(t) if t == ty),
                    "type {ty:#04x} len {len} decoded"
                );
            }
        }
    }

    /// The definition's length rule is a **minimum**, so a body longer than this generation's
    /// layout still decodes — that is a conformant `3.x` publisher appending a field, which the
    /// spec says must keep working and which exact equality would take the feed dark on.
    ///
    /// Short still fails, so the direction that matters — a v1-sized body claiming a later
    /// generation, whose fields would otherwise be read from the following message — is unaffected.
    #[test]
    fn an_over_long_instrument_definition_still_decodes() {
        let mut long = vec![0u8; 76 + 8];
        long[0..4].copy_from_slice(&41u32.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_INSTRUMENT_DEFINITION, 0, &long)).unwrap();
        assert!(
            matches!(&m[0], Message::InstrumentDefinition(d) if d.instrument_id == 41),
            "a v1 definition with trailing bytes must still decode: {:?}",
            m[0]
        );

        let short = vec![0u8; 75];
        let (_, m) = decode_datagram(&one(MSG_INSTRUMENT_DEFINITION, 0, &short)).unwrap();
        assert!(
            matches!(m[0], Message::Other(MSG_INSTRUMENT_DEFINITION)),
            "a short definition is still rejected: {:?}",
            m[0]
        );
    }

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
        let (_, m) = decode_datagram(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!("{:?}", m[0])
        };
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
        let (_, m) = decode_datagram(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!()
        };
        assert_eq!(u.order_count, None);
        assert_eq!(u.level_index, None);

        let (_, m) = decode_datagram(&one(MSG_LEVEL_UPDATE, 0, &[0u8; 44])).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!()
        };
        assert_eq!(u.order_count, Some(0), "0 is a real order count");
    }

    /// `Quantity = 0` is valid and means "remove this level" — it must decode, not be rejected.
    #[test]
    fn level_update_zero_quantity_is_valid() {
        let mut b = vec![0u8; 44];
        b[12..20].copy_from_slice(&6300i64.to_le_bytes());
        b[20..28].copy_from_slice(&0u64.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!()
        };
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
        let (_, m) = decode_datagram(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!()
        };
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
        let (_, m) = decode_datagram(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        let Message::BookClear(c) = &m[0] else {
            panic!("{:?}", m[0])
        };
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
        let (_, m) = decode_datagram(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        assert!(
            matches!(m[0], Message::Other(MSG_BOOK_CLEAR)),
            "must not decode as a clear"
        );
    }

    /// The guard must key on the one **recognized** whole-side scope, not on the one recognized
    /// from-price scope. `PriceBook::clear_side_levels` derives its behaviour from the complement
    /// (`entire = scope == SCOPE_ENTIRE_SIDE`), so every unassigned byte in `2..=255` behaves as
    /// from-price down there. An exact `== SCOPE_FROM_PRICE` test therefore lets
    /// `{ clear_side: 2, scope: 2 }` through to empty a live book from a single bound — exactly
    /// what the malformed-clear rule exists to prevent.
    #[test]
    fn book_clear_on_both_sides_is_malformed_for_every_unrecognized_scope() {
        for scope in [SCOPE_FROM_PRICE, 2, 3, 17, 255] {
            let mut b = vec![0u8; 32];
            b[6] = CLEAR_SIDE_BOTH;
            b[7] = scope;
            let (_, m) = decode_datagram(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
            assert!(
                matches!(m[0], Message::Other(MSG_BOOK_CLEAR)),
                "scope {scope} with Clear Side = 2 must not decode as a clear"
            );
        }
    }

    /// ...but `Clear Side = 2` with `Scope = 0` (clear both sides entirely) is the normal case.
    #[test]
    fn book_clear_both_sides_entirely_is_valid() {
        let mut b = vec![0u8; 32];
        b[6] = CLEAR_SIDE_BOTH;
        b[7] = SCOPE_ENTIRE_SIDE;
        let (_, m) = decode_datagram(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
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
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_LEVEL, 1, &b)).unwrap();
        let Message::SnapshotLevel(l) = &m[0] else {
            panic!("{:?}", m[0])
        };
        assert_eq!(l.snapshot_id, 5);
        assert_eq!(l.price_raw, 6200);
        assert_eq!(l.qty_raw, 150);
        assert_eq!(l.order_count, Some(2));
        assert_eq!(l.side, SIDE_BID);
        assert_eq!(l.level_flags, 1);

        // Same `0xFFFF` sentinel rule as LevelUpdate: never a count of 65535.
        b[20..22].copy_from_slice(&0xFFFFu16.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_LEVEL, 1, &b)).unwrap();
        let Message::SnapshotLevel(l) = &m[0] else {
            panic!()
        };
        assert_eq!(l.order_count, None);
    }

    /// spec: SnapshotBegin 0x20, 40 bytes. Body: id @0, anchor_seq @4, total_levels @12,
    /// snapshot_id @16, last_instrument_seq @20, ts @24, **depth_bound @32**. Body bytes 0-31 are
    /// the market-by-order feed's 32-byte body verbatim (its `Total Orders` reads as `Total
    /// Levels`); `ts` at 24 is deliberately not 8-byte aligned, inherited, not a cost of the
    /// superset.
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
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
        let Message::SnapshotBegin(s) = &m[0] else {
            panic!("{:?}", m[0])
        };
        assert_eq!(s.instrument_id, 41);
        assert_eq!(s.anchor_seq, 900);
        assert_eq!(s.total_levels, 1210);
        assert_eq!(s.snapshot_id, 5);
        assert_eq!(s.last_instrument_seq, 16);
        assert_eq!(s.ts, 7_777);
        assert_eq!(s.depth_bound, 0);
    }

    /// The reason exact-length matters: a market-by-order-shaped SnapshotBegin body must NOT decode
    /// here. If it did, `depth_bound` would read whatever followed, and a `0` there is a positive
    /// publisher claim of a complete book that no publisher made.
    #[test]
    fn snapshot_begin_rejects_the_short_sibling_layout() {
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_BEGIN, 1, &[0u8; 32])).unwrap();
        assert!(matches!(m[0], Message::Other(MSG_SNAPSHOT_BEGIN)));
    }

    #[test]
    fn snapshot_begin_bounded_depth_decodes() {
        let mut b = vec![0u8; 36];
        b[32..36].copy_from_slice(&25u32.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
        let Message::SnapshotBegin(s) = &m[0] else {
            panic!()
        };
        assert_eq!(s.depth_bound, 25);
    }

    /// spec: SnapshotEnd 0x22, 20 bytes. Body: id @0, anchor_seq @4, snapshot_id @12.
    #[test]
    fn snapshot_end_decodes() {
        let mut b = vec![0u8; 16];
        b[0..4].copy_from_slice(&41u32.to_le_bytes());
        b[4..12].copy_from_slice(&900u64.to_le_bytes());
        b[12..16].copy_from_slice(&5u32.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_SNAPSHOT_END, 1, &b)).unwrap();
        let Message::SnapshotEnd(e) = &m[0] else {
            panic!()
        };
        assert_eq!((e.instrument_id, e.anchor_seq, e.snapshot_id), (41, 900, 5));
    }

    /// spec: BatchBoundary 0x13, 16 bytes. Body: batch_id @0, batch_time @4. Carries no instrument
    /// id — it applies to the whole channel.
    #[test]
    fn batch_boundary_decodes() {
        let mut b = vec![0u8; 12];
        b[0..4].copy_from_slice(&123u32.to_le_bytes());
        b[4..12].copy_from_slice(&456u64.to_le_bytes());
        let (_, m) = decode_datagram(&one(MSG_BATCH_BOUNDARY, 0, &b)).unwrap();
        let Message::BatchBoundary(bb) = &m[0] else {
            panic!()
        };
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
        let (_, m) = decode_datagram(&one(MSG_INSTRUMENT_RESET, 0, &b)).unwrap();
        let Message::InstrumentReset(r) = &m[0] else {
            panic!()
        };
        assert_eq!(
            (r.instrument_id, r.reason, r.new_anchor_seq, r.ts),
            (41, 3, 1_000, 2_000)
        );
    }

    /// Extend the exact-length sweep to the price-keyed types.
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
                let (_, m) = decode_datagram(&one(ty, 0, &vec![0u8; len])).unwrap();
                assert!(
                    matches!(m[0], Message::Other(t) if t == ty),
                    "type {ty:#04x} len {len} decoded"
                );
            }
        }
    }

    /// Every `enc_*` builder decoded back by the real decoder. These builders feed the processor's
    /// tests, where a body off by one byte decodes as `Other` and greens a test that drove nothing.
    #[test]
    fn builders_round_trip_through_the_decoder() {
        let def = InstrumentDefinition {
            instrument_id: 41,
            source_id: None,
            symbol: "KXBTCPERP".into(),
            price_exponent: -4,
            qty_exponent: -2,
            manifest_seq: 3,
        };
        let manifest = ManifestSummary {
            channel_id: 2,
            valid: true,
            manifest_seq: 3,
            instrument_count: 13,
            ts: 111,
        };
        let trade = Trade {
            instrument_id: 41,
            source_id: 3,
            aggressor_side: AGGRESSOR_SELL,
            trade_flags: 1,
            source_ts: 222,
            trade_price_raw: -6200,
            trade_qty_raw: 150,
            trade_id: 9876,
            cumulative_volume_raw: 4242,
        };
        let update = LevelUpdate {
            instrument_id: 41,
            source_id: 3,
            side: SIDE_ASK,
            action: 2,
            per_instrument_seq: 17,
            price_raw: -6300,
            qty_raw: 150,
            ts: 333,
            order_count: Some(4),
            level_index: None,
            update_reason: 1,
            level_flags: 0b10,
        };
        let clear = BookClear {
            instrument_id: 41,
            source_id: 3,
            clear_side: CLEAR_SIDE_BID,
            scope: SCOPE_FROM_PRICE,
            per_instrument_seq: 18,
            from_price_raw: 6100,
            ts: 444,
            clear_reason: 1,
        };
        let begin = SnapshotBegin {
            instrument_id: 41,
            anchor_seq: 900,
            total_levels: 2,
            snapshot_id: 5,
            last_instrument_seq: 16,
            ts: 555,
            depth_bound: 25,
        };
        let level = SnapshotLevel {
            snapshot_id: 5,
            price_raw: 6200,
            qty_raw: 150,
            order_count: None,
            side: SIDE_BID,
            level_flags: 1,
        };
        let end = SnapshotEnd {
            instrument_id: 41,
            anchor_seq: 900,
            snapshot_id: 5,
        };
        let batch = BatchBoundary {
            batch_id: 123,
            batch_time: 666,
        };
        let reset = InstrumentReset {
            instrument_id: 41,
            reason: 3,
            new_anchor_seq: 1_000,
            ts: 777,
        };

        let built: Vec<Vec<u8>> = vec![
            enc_instrument_definition(&def),
            enc_manifest_summary(&manifest),
            enc_trade(&trade),
            enc_level_update(&update),
            enc_book_clear(&clear),
            enc_snapshot_begin(&begin),
            enc_snapshot_level(&level),
            enc_snapshot_end(&end),
            enc_batch_boundary(&batch),
            enc_instrument_reset(&reset),
            enc_heartbeat(888),
            enc_end_of_session(999),
        ];
        for (m, want) in built.iter().zip([
            sizes::INSTRUMENT_DEFINITION,
            sizes::MANIFEST_SUMMARY,
            sizes::TRADE,
            sizes::LEVEL_UPDATE,
            sizes::BOOK_CLEAR,
            sizes::SNAPSHOT_BEGIN,
            sizes::SNAPSHOT_LEVEL,
            sizes::SNAPSHOT_END,
            sizes::BATCH_BOUNDARY,
            sizes::INSTRUMENT_RESET,
            sizes::HEARTBEAT,
            sizes::END_OF_SESSION,
        ]) {
            assert_eq!(m.len(), want, "type {:#04x} wrong length", m[0]);
        }

        let (h, m) = decode_datagram(&datagram(4, 9, 12_345, &built)).unwrap();
        assert_eq!((h.channel_id, h.reset_count, h.sequence), (4, 9, 12_345));
        assert_eq!(m.len(), 12);

        let Message::InstrumentDefinition(d) = &m[0] else {
            panic!("{:?}", m[0])
        };
        assert_eq!(
            (
                d.instrument_id,
                &*d.symbol,
                d.price_exponent,
                d.qty_exponent,
                d.manifest_seq
            ),
            (41, "KXBTCPERP", -4, -2, 3)
        );
        let Message::ManifestSummary(s) = &m[1] else {
            panic!("{:?}", m[1])
        };
        assert_eq!(
            (
                s.channel_id,
                s.valid,
                s.manifest_seq,
                s.instrument_count,
                s.ts
            ),
            (2, true, 3, 13, 111)
        );
        let Message::Trade(t) = &m[2] else {
            panic!("{:?}", m[2])
        };
        assert_eq!(
            (
                t.instrument_id,
                t.aggressor_side,
                t.source_ts,
                t.trade_price_raw,
                t.trade_qty_raw,
                t.trade_id,
                t.cumulative_volume_raw
            ),
            (41, AGGRESSOR_SELL, 222, -6200, 150, 9876, 4242)
        );
        let Message::LevelUpdate(u) = &m[3] else {
            panic!("{:?}", m[3])
        };
        assert_eq!(
            (
                u.instrument_id,
                u.side,
                u.action,
                u.per_instrument_seq,
                u.price_raw,
                u.qty_raw,
                u.ts,
                u.order_count,
                u.level_index,
                u.update_reason,
                u.level_flags
            ),
            (41, SIDE_ASK, 2, 17, -6300, 150, 333, Some(4), None, 1, 0b10)
        );
        let Message::BookClear(c) = &m[4] else {
            panic!("{:?}", m[4])
        };
        assert_eq!(
            (
                c.instrument_id,
                c.clear_side,
                c.scope,
                c.per_instrument_seq,
                c.from_price_raw,
                c.ts,
                c.clear_reason
            ),
            (41, CLEAR_SIDE_BID, SCOPE_FROM_PRICE, 18, 6100, 444, 1)
        );
        let Message::SnapshotBegin(sb) = &m[5] else {
            panic!("{:?}", m[5])
        };
        assert_eq!(
            (
                sb.instrument_id,
                sb.anchor_seq,
                sb.total_levels,
                sb.snapshot_id,
                sb.last_instrument_seq,
                sb.ts,
                sb.depth_bound
            ),
            (41, 900, 2, 5, 16, 555, 25)
        );
        let Message::SnapshotLevel(sl) = &m[6] else {
            panic!("{:?}", m[6])
        };
        assert_eq!(
            (
                sl.snapshot_id,
                sl.price_raw,
                sl.qty_raw,
                sl.order_count,
                sl.side,
                sl.level_flags
            ),
            (5, 6200, 150, None, SIDE_BID, 1)
        );
        let Message::SnapshotEnd(se) = &m[7] else {
            panic!("{:?}", m[7])
        };
        assert_eq!(
            (se.instrument_id, se.anchor_seq, se.snapshot_id),
            (41, 900, 5)
        );
        let Message::BatchBoundary(bb) = &m[8] else {
            panic!("{:?}", m[8])
        };
        assert_eq!((bb.batch_id, bb.batch_time), (123, 666));
        let Message::InstrumentReset(r) = &m[9] else {
            panic!("{:?}", m[9])
        };
        assert_eq!(
            (r.instrument_id, r.reason, r.new_anchor_seq, r.ts),
            (41, 3, 1_000, 777)
        );
        assert!(matches!(m[10], Message::Heartbeat(888)));
        assert!(matches!(m[11], Message::EndOfSession(999)));
    }

    /// The `0x50`-`0x5F` range is reserved for a future positional-index addressing mode. There is
    /// no mode negotiation: a price-keyed subscriber skips them by length like any unknown type.
    #[test]
    fn index_addressing_range_is_skipped() {
        for ty in 0x50u8..=0x5F {
            let (_, m) = decode_datagram(&one(ty, 0, &[0u8; 20])).unwrap();
            assert!(
                matches!(m[0], Message::Other(t) if t == ty),
                "type {ty:#04x} decoded"
            );
        }
    }
}
