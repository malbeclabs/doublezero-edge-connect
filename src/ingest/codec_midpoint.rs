//! Decoder for the DoubleZero Edge **Midpoint** feed (frame magic `0x4D44`).
//!
//! A sibling protocol to Top-of-Book that carries a single derived mid price per instrument. It
//! shares the 24-byte frame header / 4-byte message header / generic frame-walker in
//! [`crate::ingest::codec_common`]; only the magic and the message bodies differ. Its
//! `InstrumentDefinition` is a distinct 64-byte layout (a `Default Method` byte where Top-of-Book
//! has `qty_exponent`/`market_model`, and no lot/contract/settle fields), and it adds the 40-byte
//! `Midpoint` message.
//!
//! ⚠️ Byte offsets below come from the edge-feed-spec draft, **not** a byte-validated reference
//! codec (unlike Top-of-Book, which is validated against `arb/feeds/dz_edge/codec.py`). They must
//! be confirmed against a live frame hexdump before this decoder's output is trusted in
//! production - the round-trip test here only pins internal self-consistency.

use anyhow::Result;

pub use crate::ingest::codec_common::MSG_HEADER_SIZE;
use crate::ingest::codec_common::{
    cstr, decode_frame_with, i64le, u16le, u32le, u64le, FrameHeader,
};

pub const MAGIC: u16 = 0x4D44; // "DM"

pub const MSG_HEARTBEAT: u8 = 0x01;
pub const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const MSG_MIDPOINT: u8 = 0x03;
pub const MSG_END_OF_SESSION: u8 = 0x06;
pub const MSG_MANIFEST_SUMMARY: u8 = 0x07;

/// On-wire sizes (including the 4-byte message header). The decoder reads each message's length
/// from its header; these are kept for parity with the spec and for the round-trip encoders.
#[allow(dead_code)]
pub const INSTRUMENT_DEFINITION_SIZE: u8 = 64;
#[allow(dead_code)]
pub const MIDPOINT_SIZE: u8 = 40;

/// A Midpoint feed instrument definition (64-byte layout). Only the fields the bridge needs are
/// retained; `default_method` is the Midpoint-specific computation default. There is no
/// `qty_exponent` (a mid price has no size).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstrumentDefinition {
    pub instrument_id: u32,
    pub symbol: String,
    pub price_exponent: i8,
    pub default_method: u8,
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

/// A single derived mid price for an instrument (40-byte message).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Midpoint {
    pub instrument_id: u32,
    pub source_id: u16,
    /// How the mid was computed (0 = use the instrument's default method).
    pub method: u8,
    /// Quality bits: 0=stale, 1=one-sided, 2=crossed/locked, 3=synthetic.
    pub quality_flags: u8,
    /// Venue timestamp of the underlying book state.
    pub book_ts: u64,
    /// When the publisher computed the mid.
    pub compute_ts: u64,
    /// Mid price as a raw integer in the instrument's price exponent.
    pub mid_price_raw: i64,
}

#[allow(dead_code)]
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
    Midpoint(Midpoint),
    InstrumentDefinition(InstrumentDefinition),
    ManifestSummary(ManifestSummary),
    Heartbeat,
    /// 0x06 EndOfSession - no more data this session. Carries ts.
    EndOfSession(u64),
    /// Any other message type; the byte is the raw wire type, kept for diagnostics.
    Other(#[allow(dead_code)] u8),
}

/// Decode one Midpoint-feed UDP datagram into a header and its application messages.
pub fn decode_frame(buf: &[u8]) -> Result<(FrameHeader, Vec<Message>)> {
    decode_frame_with(buf, MAGIC, |msg_type, _flags, b, o| {
        decode_message(msg_type, b, o)
    })
}

fn decode_message(msg_type: u8, b: &[u8], o: usize) -> Message {
    let body = o + MSG_HEADER_SIZE;
    match msg_type {
        MSG_MIDPOINT => Message::Midpoint(Midpoint {
            instrument_id: u32le(b, body),
            source_id: u16le(b, body + 4),
            method: b[body + 6],
            quality_flags: b[body + 7],
            book_ts: u64le(b, body + 8),
            compute_ts: u64le(b, body + 16),
            mid_price_raw: i64le(b, body + 24),
        }),
        MSG_INSTRUMENT_DEFINITION => Message::InstrumentDefinition(InstrumentDefinition {
            instrument_id: u32le(b, body),
            symbol: cstr(b, body + 4, 16),
            price_exponent: b[body + 37] as i8,
            default_method: b[body + 38],
            manifest_seq: u16le(b, body + 56),
        }),
        MSG_MANIFEST_SUMMARY => Message::ManifestSummary(ManifestSummary {
            channel_id: b[body],
            valid: b[body + 1] != 0,
            manifest_seq: u16le(b, body + 4),
            instrument_count: u32le(b, body + 8),
            ts: u64le(b, body + 12),
        }),
        MSG_HEARTBEAT => Message::Heartbeat,
        MSG_END_OF_SESSION => Message::EndOfSession(u64le(b, body)),
        other => Message::Other(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::codec_common::{apply_exponent, FRAME_HEADER_SIZE};

    fn frame(body: Vec<u8>, msg_count: u8) -> Vec<u8> {
        let frame_len = (FRAME_HEADER_SIZE + body.len()) as u16;
        let mut f = Vec::new();
        f.extend_from_slice(&MAGIC.to_le_bytes());
        f.push(1); // schema version
        f.push(0); // channel
        f.extend_from_slice(&0u64.to_le_bytes()); // sequence
        f.extend_from_slice(&0u64.to_le_bytes()); // send ts
        f.push(msg_count);
        f.push(0); // reset count
        f.extend_from_slice(&frame_len.to_le_bytes());
        f.extend_from_slice(&body);
        f
    }

    fn encode_midpoint(m: &Midpoint) -> Vec<u8> {
        let mut b = vec![MSG_MIDPOINT, MIDPOINT_SIZE, 0, 0]; // header: type, len, flags
        b.extend_from_slice(&m.instrument_id.to_le_bytes());
        b.extend_from_slice(&m.source_id.to_le_bytes());
        b.push(m.method);
        b.push(m.quality_flags);
        b.extend_from_slice(&m.book_ts.to_le_bytes());
        b.extend_from_slice(&m.compute_ts.to_le_bytes());
        b.extend_from_slice(&m.mid_price_raw.to_le_bytes());
        b.extend_from_slice(&[0u8; 4]); // reserved -> 40 bytes total
        b
    }

    fn encode_instrument(d: &InstrumentDefinition) -> Vec<u8> {
        let mut b = vec![MSG_INSTRUMENT_DEFINITION, INSTRUMENT_DEFINITION_SIZE, 0, 0];
        b.extend_from_slice(&d.instrument_id.to_le_bytes()); // @4
        let mut sym = [0u8; 16];
        let raw = d.symbol.as_bytes();
        sym[..raw.len()].copy_from_slice(raw);
        b.extend_from_slice(&sym); // @8 symbol[16]
        b.extend_from_slice(&[0u8; 8]); // @24 leg1[8]
        b.extend_from_slice(&[0u8; 8]); // @32 leg2[8]
        b.push(0); // @40 asset_class
        b.push(d.price_exponent as u8); // @41 price_exponent
        b.push(d.default_method); // @42 default_method
        b.push(0); // @43 price_bound
        b.extend_from_slice(&0i64.to_le_bytes()); // @44 tick_size
        b.extend_from_slice(&0u64.to_le_bytes()); // @52 expiry
        b.extend_from_slice(&d.manifest_seq.to_le_bytes()); // @60 manifest_seq
        b.extend_from_slice(&[0u8; 2]); // @62 reserved -> 64 bytes total
        b
    }

    #[test]
    fn midpoint_round_trip() {
        // Body must be 36 bytes (4+2+1+1+8+8+8 + 4 reserved) so the 40-byte message lands.
        assert_eq!(MIDPOINT_SIZE, 40);
        let m = Midpoint {
            instrument_id: 7,
            source_id: 1,
            method: 0,
            quality_flags: 0b10, // crossed/locked
            book_ts: 1_780_000_000_000_000_000,
            compute_ts: 1_780_000_000_000_000_123,
            mid_price_raw: 18_420,
        };
        let f = frame(encode_midpoint(&m), 1);
        assert_eq!(f.len(), FRAME_HEADER_SIZE + 40);
        let (_h, msgs) = decode_frame(&f).unwrap();
        match &msgs[0] {
            Message::Midpoint(got) => {
                assert_eq!(got.instrument_id, 7);
                assert_eq!(got.source_id, 1);
                assert_eq!(got.quality_flags, 0b10);
                assert_eq!(got.book_ts, 1_780_000_000_000_000_000);
                assert_eq!(got.compute_ts, 1_780_000_000_000_000_123);
                assert_eq!(got.mid_price_raw, 18_420);
                assert!((apply_exponent(got.mid_price_raw, -2) - 184.20).abs() < 1e-9);
            }
            other => panic!("expected midpoint, got {other:?}"),
        }
    }

    #[test]
    fn midpoint_instrument_def_round_trip() {
        assert_eq!(INSTRUMENT_DEFINITION_SIZE, 64);
        let d = InstrumentDefinition {
            instrument_id: 7,
            symbol: "SOL".to_string(),
            price_exponent: -2,
            default_method: 3,
            manifest_seq: 5,
        };
        let f = frame(encode_instrument(&d), 1);
        assert_eq!(f.len(), FRAME_HEADER_SIZE + 64);
        let (_h, msgs) = decode_frame(&f).unwrap();
        match &msgs[0] {
            Message::InstrumentDefinition(got) => {
                assert_eq!(got.instrument_id, 7);
                assert_eq!(got.symbol, "SOL");
                assert_eq!(got.price_exponent, -2);
                assert_eq!(got.default_method, 3);
                assert_eq!(got.manifest_seq, 5);
            }
            other => panic!("expected instrument definition, got {other:?}"),
        }
    }

    #[test]
    fn bad_magic_errors() {
        // A Top-of-Book frame (magic 0x445A) must not decode as a Midpoint frame.
        let mut f = frame(
            encode_midpoint(&Midpoint {
                instrument_id: 1,
                source_id: 1,
                method: 0,
                quality_flags: 0,
                book_ts: 0,
                compute_ts: 0,
                mid_price_raw: 0,
            }),
            1,
        );
        f[0] = 0x5A;
        f[1] = 0x44; // overwrite magic with 0x445A
        assert!(decode_frame(&f).is_err());
    }
}
