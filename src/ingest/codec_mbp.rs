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
//! * **Exact body-length equality per type, not `>=`.** The forward-compatibility rule that a
//!   decoder ignores trailing bytes applies across a Schema Version bump; within v1 an unexpected
//!   length is malformed. This is load-bearing, not pedantry: `SnapshotBegin` is a prefix-superset
//!   of the market-by-order feed's — the first 36 message bytes are identical and `Depth Bound` is
//!   appended at message offset 36 — so a sibling-shaped body would otherwise decode with
//!   `depth_bound` reading whatever follows, and a `0` there is a positive publisher claim of a
//!   complete book that no publisher made.
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
//! Frame-header validation is the shared walker's, which is looser than the oracle's: it does not
//! reject a schema version other than 1, a `frame_length` disagreeing with the datagram, or a zero
//! message count. That gap is pre-existing and shared by all four codecs.

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
            MSG_END_OF_SESSION if exact(sizes::END_OF_SESSION) => u64le(b, body)
                .map(Message::EndOfSession)
                .unwrap_or(Message::Other),
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
        valid: u8le(b, o + 1)? != 0,
        manifest_seq: u16le(b, o + 4)?,
        instrument_count: u32le(b, o + 8)?,
        ts: u64le(b, o + 12)?,
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
                assert!(
                    matches!(m[0], Message::Other),
                    "type {ty:#04x} len {len} decoded"
                );
            }
        }
    }

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
        let Message::InstrumentDefinition(a) = &m[0] else {
            panic!()
        };
        let crate::ingest::codec::Message::InstrumentDefinition(c) = &t[0] else {
            panic!()
        };
        assert_eq!(a.instrument_id, c.instrument_id);
        assert_eq!(&*a.symbol, &*c.symbol);
        assert_eq!(a.price_exponent, c.price_exponent);
        assert_eq!(a.qty_exponent, c.qty_exponent);
        assert_eq!(a.manifest_seq, c.manifest_seq);
    }
}
