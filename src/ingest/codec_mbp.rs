//! Decoder for the DoubleZero Edge **Market-by-Price** feed (frame magic `0x4442`).
//!
//! Price-aggregated L2: each `LevelUpdate` states the complete resulting state of one price level,
//! with in-band snapshot+delta recovery on a third port. Shares the 24-byte frame header, 4-byte
//! message header and generic frame-walker in [`crate::ingest::codec_common`]; only the magic and
//! the bodies differ.
//!
//! **Validated field-for-field against `go/marketbyprice-parser`** (edge-multicast-ref, merged
//! PR #29), so this ships offset-validated rather than draft-only — the trap `codec_midpoint` is
//! still in. Two things the oracle does that the sibling codecs here do not, both deliberate:
//!
//! * **Exact body-length equality per type, not `>=`, paired with a `SCHEMA_VERSION` gate.** The
//!   forward-compatibility rule that a decoder ignores trailing bytes applies across a Schema
//!   Version bump; within v1 an unexpected length is malformed. The two rules are one decision: the
//!   version gate is what keeps the length rule from silently rejecting a v2 frame whose bodies
//!   legally grew. The length rule is load-bearing because `SnapshotBegin` is a prefix-superset of
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
//! **Oracle strength: no real-frame fixture exists yet.** Every offset is pinned by the
//! offset-independent unit tests below plus the Go decoder above; the three types inherited from
//! top-of-book are additionally pinned to that byte-validated codec by
//! `tests/codec_mbp_fixtures.rs`. That is stronger than `codec_midpoint` (self-consistency only)
//! and weaker than `codec_mbo` (real capture) — capture a live frame before enabling a `FEEDS` row.
//!
//! Oracle parity is per-*body*. The shared frame walker stays looser than the oracle on two header
//! checks it does not make — a `frame_length` disagreeing with the datagram (the walker clamps) and
//! a zero message count (an empty message list) — which is pre-existing and shared by all four
//! codecs. The third, schema version, is checked here rather than left to the walker, because only
//! this codec's body rule depends on it.

use std::sync::Arc;

use anyhow::{bail, Result};

use crate::ingest::codec_common::{
    cstr, decode_frame_with, i64le, u16le, u32le, u64le, u8le, FrameHeader, MSG_HEADER_SIZE,
};

pub const MAGIC: u16 = 0x4442; // "BD"

/// The only schema this decoder implements. A frame declaring anything else is discarded whole —
/// see [`decode_frame`] for why that is load-bearing here and nowhere else.
pub const SCHEMA_VERSION: u8 = 1;

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
/// each other: a frame declaring an unimplemented [`SCHEMA_VERSION`] is rejected whole, and within
/// v1 `msg_len` must equal the type's declared size exactly before any field is read, so a mis-sized
/// body becomes [`Message::Other`] rather than decoding garbage into a field that has semantics (the
/// module doc's `Depth Bound` case). Without the version gate the length rule would apply v1 sizes
/// to a v2 frame whose bodies legally grew, and the whole feed would decode to `Other` in silence.
pub fn decode_frame(buf: &[u8]) -> Result<(FrameHeader, Vec<Message>)> {
    let (header, messages) = decode_frame_with(buf, MAGIC, |ty, _flags, b, off| {
        // In bounds: the walker breaks before calling this unless `off + MSG_HEADER_SIZE` fits.
        let msg_len = b[off + 1] as usize;
        let body = off + MSG_HEADER_SIZE;
        let exact = |n: usize| msg_len == n;
        match ty {
            MSG_HEARTBEAT if exact(sizes::HEARTBEAT) => {
                decode_heartbeat(b, body).unwrap_or(Message::Other(ty))
            }
            MSG_INSTRUMENT_DEFINITION if exact(sizes::INSTRUMENT_DEFINITION) => {
                decode_instrument_definition(b, body).unwrap_or(Message::Other(ty))
            }
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
            // `0x03`/`0x05` are reserved to stop a misrouted sibling frame cross-decoding, and
            // `MSG_LIQUIDATION` carries nothing this bridge re-serves. Both fall through here.
            _ => Message::Other(ty),
        }
    })?;
    if header.schema_version != SCHEMA_VERSION {
        bail!(
            "unsupported mbp schema version {} (expected {SCHEMA_VERSION})",
            header.schema_version
        );
    }
    Ok((header, messages))
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

    /// Wrap a body in its 4-byte message header. The declared length is the TOTAL message length.
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

    /// A frame declaring a schema this decoder does not implement is discarded whole. Without this
    /// the exact-length rule would apply v1 sizes to a v2 frame whose bodies legally grew, and the
    /// whole feed would decode to `Other` with no error to see.
    #[test]
    fn rejects_an_unimplemented_schema_version() {
        let mut f = one(MSG_HEARTBEAT, 0, &[0u8; 12]);
        f[2] = SCHEMA_VERSION + 1;
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
        let (_, m) = decode_frame(&one(MSG_MANIFEST_SUMMARY, 0, &b)).unwrap();
        let Message::ManifestSummary(s) = &m[0] else {
            panic!()
        };
        assert!(s.valid);
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
        let mut f = frame_header(2, 0, unknown.len() + hb.len());
        f.extend_from_slice(&unknown);
        f.extend_from_slice(&hb);
        let (_, m) = decode_frame(&f).unwrap();
        assert!(matches!(m[0], Message::Other(0x7F)));
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
                assert!(
                    matches!(m[0], Message::Other(t) if t == ty),
                    "type {ty:#04x} len {len} decoded"
                );
            }
        }
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
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
        let Message::LevelUpdate(u) = &m[0] else {
            panic!()
        };
        assert_eq!(u.order_count, None);
        assert_eq!(u.level_index, None);

        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &[0u8; 44])).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_LEVEL_UPDATE, 0, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_BOOK_CLEAR, 0, &b)).unwrap();
        assert!(
            matches!(m[0], Message::Other(MSG_BOOK_CLEAR)),
            "must not decode as a clear"
        );
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
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_LEVEL, 1, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &[0u8; 32])).unwrap();
        assert!(matches!(m[0], Message::Other(MSG_SNAPSHOT_BEGIN)));
    }

    #[test]
    fn snapshot_begin_bounded_depth_decodes() {
        let mut b = vec![0u8; 36];
        b[32..36].copy_from_slice(&25u32.to_le_bytes());
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_BEGIN, 1, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_SNAPSHOT_END, 1, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_BATCH_BOUNDARY, 0, &b)).unwrap();
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
        let (_, m) = decode_frame(&one(MSG_INSTRUMENT_RESET, 0, &b)).unwrap();
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
                let (_, m) = decode_frame(&one(ty, 0, &vec![0u8; len])).unwrap();
                assert!(
                    matches!(m[0], Message::Other(t) if t == ty),
                    "type {ty:#04x} len {len} decoded"
                );
            }
        }
    }

    /// The `0x50`-`0x5F` range is reserved for a future positional-index addressing mode. There is
    /// no mode negotiation: a price-keyed subscriber skips them by length like any unknown type.
    #[test]
    fn index_addressing_range_is_skipped() {
        for ty in 0x50u8..=0x5F {
            let (_, m) = decode_frame(&one(ty, 0, &[0u8; 20])).unwrap();
            assert!(
                matches!(m[0], Message::Other(t) if t == ty),
                "type {ty:#04x} decoded"
            );
        }
    }
}
