//! Decoder for the DoubleZero Edge Top-of-Book & Trades feed (schema v1).
//!
//! Little-endian, fixed-size binary datagrams, defined by the edge-feed-spec
//! (https://github.com/malbeclabs/edge-feed-spec). The datagram
//! header, message header, little-endian readers and the generic datagram-walker are shared with
//! the other feeds in [`crate::ingest::codec_common`].

use anyhow::Result;

use crate::ingest::codec_common::{
    decode_datagram_with, i64le, instrument_definition, u16le, u32le, u64le, u8le,
};
// Re-export the shared datagram primitives under `codec::` so existing call sites
// (`crate::ingest::codec::DatagramHeader`, `apply_exponent`, ...) keep resolving here.
pub use crate::ingest::codec_common::{
    apply_exponent, DatagramHeader, InstrumentDefinition, MSG_HEADER_SIZE,
};
use crate::ingest::codec_common::{SCHEMA_V1, SCHEMA_V3};

pub const MAGIC: u16 = 0x445A;

/// Wire generations this feed implements. `2` is deliberately absent — see [`SCHEMA_V1`].
const SUPPORTED_VERSIONS: &[u8] = &[SCHEMA_V1, SCHEMA_V3];

pub const MSG_HEARTBEAT: u8 = 0x01;
pub const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const MSG_QUOTE: u8 = 0x03;
pub const MSG_TRADE: u8 = 0x04;
pub const MSG_CHANNEL_RESET: u8 = 0x05;
pub const MSG_END_OF_SESSION: u8 = 0x06;
pub const MSG_MANIFEST_SUMMARY: u8 = 0x07;

/// Total on-wire size of a `Trade` message including the 4-byte application message header.
/// Matches the reference `protocol.py` constant `TRADE_SIZE = 52`. The decoder reads the actual
/// length from each message header; this is kept for parity with the reference and for encoders.
#[allow(dead_code)]
pub const TRADE_SIZE: u8 = 52;

/// Total on-wire size of a `Quote` message including the 4-byte application message header.
pub const QUOTE_SIZE: u8 = 60;

/// Message offset of the `Quote` `Venue Update ID` (`u32`, edge-feed-spec top-of-book `3.2.0`).
/// The last four bytes of the 60; padding before `3.2.0`.
const QUOTE_VENUE_UPDATE_ID_OFFSET: usize = 56;

// Several wire fields below are decoded for byte-for-byte fidelity with the reference codec
// (so offsets stay validated) even though no consumer reads them yet; allow the dead_code.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Quote {
    pub instrument_id: u32,
    pub source_id: u16,
    pub update_flags: u8,
    pub source_ts: u64,
    pub bid_price_raw: i64,
    pub bid_qty_raw: u64,
    pub ask_price_raw: i64,
    pub ask_qty_raw: u64,
    /// Orders/sources at best bid/ask (`Bid/Ask Source Count`, edge-feed-spec TOB offsets 52/54;
    /// `BidSourceCount`/`AskSourceCount` in edge-multicast-ref, the spec's `bbo_hash` `bid_n`/`ask_n`).
    /// `0` means unavailable. Part of the canonical BBO identity.
    pub bid_n: u16,
    pub ask_n: u16,
    /// Low 32 bits of the venue-assigned per-instrument book update counter (`Venue Update ID`,
    /// edge-feed-spec TOB offset 56, `3.2.0`). `0` means unavailable, and is what every encoder that
    /// predates the field writes there. Ordered by serial-number arithmetic; the arbiter dedupes on it.
    pub venue_update_id: u32,
}

/// A trade print (last sale) from a venue. Same `instrument_id`/`source_id`/`source_ts`
/// convention as [`Quote`]; the price/qty are raw integers scaled by the instrument's
/// price/qty exponents. `aggressor_side` is 1=Buy, 2=Sell, 0=Unknown (see [`aggressor_side`]).
#[allow(dead_code)]
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
    Quote(Quote),
    Trade(Trade),
    InstrumentDefinition(InstrumentDefinition),
    ManifestSummary(ManifestSummary),
    Heartbeat,
    /// 0x05 ChannelReset - publisher reset the channel; discard cached state. Carries ts.
    ChannelReset(u64),
    /// 0x06 EndOfSession - no more data this session. Carries ts.
    EndOfSession(u64),
    /// Any other message type; the byte is the raw wire type, kept for diagnostics.
    Other(#[allow(dead_code)] u8),
}

pub use crate::ingest::sources::source_name;

/// Decode one UDP datagram into a header and its application messages.
pub fn decode_datagram(buf: &[u8]) -> Result<(DatagramHeader, Vec<Message>)> {
    decode_datagram_with(
        buf,
        MAGIC,
        SUPPORTED_VERSIONS,
        |msg_type, _flags, b, o, ver| decode_message(msg_type, b, o, ver),
    )
}

fn decode_message(msg_type: u8, b: &[u8], o: usize, schema_version: u8) -> Message {
    // A message shorter than its declared type's fields decodes to `None` -> `Other` (skipped),
    // never an out-of-bounds panic (the readers are bounds-checked; see `codec_common`).
    decode_body(msg_type, b, o, schema_version).unwrap_or(Message::Other(msg_type))
}

fn decode_body(msg_type: u8, b: &[u8], o: usize, schema_version: u8) -> Option<Message> {
    let body = o + MSG_HEADER_SIZE;
    Some(match msg_type {
        MSG_QUOTE => Message::Quote(Quote {
            instrument_id: u32le(b, body)?,
            source_id: u16le(b, body + 4)?,
            update_flags: u8le(b, body + 6)?,
            source_ts: u64le(b, body + 8)?,
            bid_price_raw: i64le(b, body + 16)?,
            bid_qty_raw: u64le(b, body + 24)?,
            ask_price_raw: i64le(b, body + 32)?,
            ask_qty_raw: u64le(b, body + 40)?,
            bid_n: u16le(b, body + 48)?,
            ask_n: u16le(b, body + 50)?,
            venue_update_id: quote_venue_update_id(b, o),
        }),
        MSG_TRADE => Message::Trade(Trade {
            instrument_id: u32le(b, body)?,
            source_id: u16le(b, body + 4)?,
            aggressor_side: u8le(b, body + 6)?,
            trade_flags: u8le(b, body + 7)?,
            source_ts: u64le(b, body + 8)?,
            trade_price_raw: i64le(b, body + 16)?,
            trade_qty_raw: u64le(b, body + 24)?,
            trade_id: u64le(b, body + 32)?,
            cumulative_volume_raw: u64le(b, body + 40)?,
        }),
        MSG_INSTRUMENT_DEFINITION => {
            Message::InstrumentDefinition(instrument_definition(b, o, schema_version)?)
        }
        MSG_MANIFEST_SUMMARY => Message::ManifestSummary(ManifestSummary {
            channel_id: u8le(b, body)?,
            valid: u8le(b, body + 1)? != 0,
            manifest_seq: u16le(b, body + 4)?,
            instrument_count: u32le(b, body + 8)?,
            ts: u64le(b, body + 12)?,
        }),
        MSG_HEARTBEAT => Message::Heartbeat,
        MSG_CHANNEL_RESET => Message::ChannelReset(u64le(b, body)?),
        MSG_END_OF_SESSION => Message::EndOfSession(u64le(b, body)?),
        other => Message::Other(other),
    })
}

/// The quote at message offset `o`'s `Venue Update ID`, or `0` when its declared length does not
/// cover the field. The walker hands the body decoder the whole datagram, not the message
/// (`codec_common::decode_datagram_with`), so the bounds-checked reader alone would let a quote that
/// under-declares its length read its ID out of the *next* message in the datagram.
fn quote_venue_update_id(b: &[u8], o: usize) -> u32 {
    let declared = b.get(o + 1).copied().map_or(0, usize::from);
    if declared < QUOTE_VENUE_UPDATE_ID_OFFSET + 4 {
        return 0;
    }
    u32le(b, o + QUOTE_VENUE_UPDATE_ID_OFFSET).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::codec_common::DATAGRAM_HEADER_SIZE;

    // Minimal encoder for a single-Quote datagram, to round-trip the decoder.
    fn encode_quote_datagram(q: &Quote) -> Vec<u8> {
        let mut body = vec![MSG_QUOTE, 60u8, 0, 0]; // msg header: type, len, flags(u16)
        body.extend_from_slice(&q.instrument_id.to_le_bytes());
        body.extend_from_slice(&q.source_id.to_le_bytes());
        body.push(q.update_flags);
        body.push(0); // reserved
        body.extend_from_slice(&q.source_ts.to_le_bytes());
        body.extend_from_slice(&q.bid_price_raw.to_le_bytes());
        body.extend_from_slice(&q.bid_qty_raw.to_le_bytes());
        body.extend_from_slice(&q.ask_price_raw.to_le_bytes());
        body.extend_from_slice(&q.ask_qty_raw.to_le_bytes());
        body.extend_from_slice(&q.bid_n.to_le_bytes()); // bid source count (offset 52)
        body.extend_from_slice(&q.ask_n.to_le_bytes()); // ask source count (offset 54)
        body.extend_from_slice(&q.venue_update_id.to_le_bytes()); // venue update id (offset 56) -> 60 bytes

        let datagram_len = (DATAGRAM_HEADER_SIZE + body.len()) as u16;
        let mut datagram = Vec::new();
        datagram.extend_from_slice(&MAGIC.to_le_bytes());
        datagram.push(1); // schema version
        datagram.push(0); // channel
        datagram.extend_from_slice(&0u64.to_le_bytes()); // sequence
        datagram.extend_from_slice(&0u64.to_le_bytes()); // send ts
        datagram.push(1); // msg count
        datagram.push(0); // reset count
        datagram.extend_from_slice(&datagram_len.to_le_bytes());
        datagram.extend_from_slice(&body);
        datagram
    }

    #[test]
    fn quote_round_trip() {
        let q = Quote {
            instrument_id: 42,
            source_id: 1,
            update_flags: 0b11,
            source_ts: 1_780_609_924_758_000_000,
            bid_price_raw: 6788,
            bid_qty_raw: 10657,
            ask_price_raw: 6790,
            ask_qty_raw: 10886,
            bid_n: 5,
            ask_n: 3,
            venue_update_id: 0xDEAD_BEEF,
        };
        let datagram = encode_quote_datagram(&q);
        assert_eq!(datagram.len(), DATAGRAM_HEADER_SIZE + QUOTE_SIZE as usize);
        let (hdr, msgs) = decode_datagram(&datagram).unwrap();
        assert_eq!(hdr.msg_count, 1);
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            Message::Quote(got) => {
                assert_eq!(got.instrument_id, 42);
                assert_eq!(got.bid_price_raw, 6788);
                assert_eq!(got.ask_qty_raw, 10886);
                assert_eq!(got.bid_n, 5);
                assert_eq!(got.ask_n, 3);
                assert_eq!(got.venue_update_id, 0xDEAD_BEEF);
                assert!((apply_exponent(got.bid_price_raw, -2) - 67.88).abs() < 1e-9);
            }
            _ => panic!("expected quote"),
        }
    }

    /// The `Venue Update ID` sits at message offset 56, little-endian: pinned against raw bytes
    /// rather than through the test encoder, which would agree with a decoder reading the wrong
    /// offset as long as both were wrong together.
    #[test]
    fn quote_venue_update_id_is_offset_56_little_endian() {
        let mut datagram = encode_quote_datagram(&sample_quote(0));
        let at = DATAGRAM_HEADER_SIZE + 56;
        datagram[at..at + 4].copy_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        let (_h, msgs) = decode_datagram(&datagram).unwrap();
        let Message::Quote(got) = &msgs[0] else {
            panic!("expected quote");
        };
        assert_eq!(got.venue_update_id, 0x0403_0201);
        // Nothing else moved: the source counts just before it still decode.
        assert_eq!((got.bid_n, got.ask_n), (5, 3));
    }

    /// Today's encoders write `dst[56..60].fill(0)` (the shared `dz-edge-tob` encoder), so every
    /// existing quote decodes to ID `0` — "unavailable" — and stays on the `source_ts` floor.
    #[test]
    fn zero_padding_decodes_to_no_venue_update_id() {
        let datagram = encode_quote_datagram(&sample_quote(0));
        let at = DATAGRAM_HEADER_SIZE + 56;
        assert_eq!(&datagram[at..at + 4], &[0, 0, 0, 0]);
        let (_h, msgs) = decode_datagram(&datagram).unwrap();
        let Message::Quote(got) = &msgs[0] else {
            panic!("expected quote");
        };
        assert_eq!(got.venue_update_id, 0);
    }

    /// A quote that declares fewer than 60 bytes has no `Venue Update ID`, even when the datagram
    /// carries more bytes after it. The walker hands the decoder the whole datagram, so reading at
    /// offset 56 regardless would take the next message's header for an ID.
    #[test]
    fn short_quote_does_not_read_its_id_from_the_next_message() {
        let full = encode_quote_datagram(&sample_quote(0));
        // The quote's first 56 bytes, declared as 56, then a 16-byte heartbeat whose header bytes
        // would read as a nonzero ID at the quote's offset 56.
        let mut quote = full[DATAGRAM_HEADER_SIZE..DATAGRAM_HEADER_SIZE + 56].to_vec();
        quote[1] = 56;
        let mut heartbeat = vec![MSG_HEARTBEAT, 16, 0, 0, 0, 0, 0, 0];
        heartbeat.extend_from_slice(&123u64.to_le_bytes());
        let mut datagram = full[..DATAGRAM_HEADER_SIZE].to_vec();
        datagram[20] = 2; // msg count
        let len = (DATAGRAM_HEADER_SIZE + quote.len() + heartbeat.len()) as u16;
        datagram[22..24].copy_from_slice(&len.to_le_bytes());
        datagram.extend_from_slice(&quote);
        datagram.extend_from_slice(&heartbeat);
        assert_ne!(u32le(&datagram, DATAGRAM_HEADER_SIZE + 56), Some(0));

        let (_h, msgs) = decode_datagram(&datagram).unwrap();
        assert_eq!(msgs.len(), 2);
        let Message::Quote(got) = &msgs[0] else {
            panic!("expected quote, got {:?}", msgs[0]);
        };
        assert_eq!(got.venue_update_id, 0);
        assert_eq!((got.bid_n, got.ask_n), (5, 3));
        assert!(matches!(msgs[1], Message::Heartbeat));
    }

    fn sample_quote(venue_update_id: u32) -> Quote {
        Quote {
            instrument_id: 42,
            source_id: 1,
            update_flags: 0b11,
            source_ts: 0,
            bid_price_raw: 6788,
            bid_qty_raw: 10657,
            ask_price_raw: 6790,
            ask_qty_raw: 10886,
            bid_n: 5,
            ask_n: 3,
            venue_update_id,
        }
    }

    // Minimal encoder for a single-Trade datagram, to round-trip the decoder. The body is 48 bytes
    // (4+2+1+1+8+8+8+8+8), so the message is 52 bytes including the header - matching TRADE_SIZE.
    fn encode_trade_datagram(t: &Trade) -> Vec<u8> {
        let mut body = vec![MSG_TRADE, TRADE_SIZE, 0, 0]; // msg header: type, len, flags(u16)
        body.extend_from_slice(&t.instrument_id.to_le_bytes());
        body.extend_from_slice(&t.source_id.to_le_bytes());
        body.push(t.aggressor_side);
        body.push(t.trade_flags);
        body.extend_from_slice(&t.source_ts.to_le_bytes());
        body.extend_from_slice(&t.trade_price_raw.to_le_bytes());
        body.extend_from_slice(&t.trade_qty_raw.to_le_bytes());
        body.extend_from_slice(&t.trade_id.to_le_bytes());
        body.extend_from_slice(&t.cumulative_volume_raw.to_le_bytes());

        let datagram_len = (DATAGRAM_HEADER_SIZE + body.len()) as u16;
        let mut datagram = Vec::new();
        datagram.extend_from_slice(&MAGIC.to_le_bytes());
        datagram.push(1); // schema version
        datagram.push(0); // channel
        datagram.extend_from_slice(&0u64.to_le_bytes()); // sequence
        datagram.extend_from_slice(&0u64.to_le_bytes()); // send ts
        datagram.push(1); // msg count
        datagram.push(0); // reset count
        datagram.extend_from_slice(&datagram_len.to_le_bytes());
        datagram.extend_from_slice(&body);
        datagram
    }

    #[test]
    fn trade_round_trip() {
        // The 4-byte message header declares len = TRADE_SIZE (52); the body must be exactly 48
        // bytes for the field offsets to land, so this also pins the layout byte-for-byte.
        assert_eq!(TRADE_SIZE, 52);
        let t = Trade {
            instrument_id: 42,
            source_id: 1,
            aggressor_side: 2, // sell
            trade_flags: 0,
            source_ts: 1_780_609_924_758_000_000,
            trade_price_raw: 6789,
            trade_qty_raw: 1500,
            trade_id: 99_887_766,
            cumulative_volume_raw: 5_000_000,
        };
        let datagram = encode_trade_datagram(&t);
        // header(24) + msg header(4) + body(48) = 76 bytes total
        assert_eq!(datagram.len(), 76);
        let (hdr, msgs) = decode_datagram(&datagram).unwrap();
        assert_eq!(hdr.msg_count, 1);
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            Message::Trade(got) => {
                assert_eq!(got.instrument_id, 42);
                assert_eq!(got.source_id, 1);
                assert_eq!(got.aggressor_side, 2);
                assert_eq!(
                    crate::model::Side::from_code(got.aggressor_side),
                    crate::model::Side::Sell
                );
                assert_eq!(got.source_ts, 1_780_609_924_758_000_000);
                assert_eq!(got.trade_price_raw, 6789);
                assert_eq!(got.trade_qty_raw, 1500);
                assert_eq!(got.trade_id, 99_887_766);
                assert_eq!(got.cumulative_volume_raw, 5_000_000);
                assert!((apply_exponent(got.trade_price_raw, -2) - 67.89).abs() < 1e-9);
            }
            other => panic!("expected trade, got {other:?}"),
        }
    }

    #[test]
    fn channel_reset_decodes() {
        // datagram header + one 0x05 message (len 12): type,len,flags + u64 ts.
        let mut body = vec![MSG_CHANNEL_RESET, 12u8, 0, 0];
        body.extend_from_slice(&777u64.to_le_bytes());
        let datagram_len = (DATAGRAM_HEADER_SIZE + body.len()) as u16;
        let mut datagram = Vec::new();
        datagram.extend_from_slice(&MAGIC.to_le_bytes());
        datagram.push(1);
        datagram.push(0);
        datagram.extend_from_slice(&0u64.to_le_bytes());
        datagram.extend_from_slice(&0u64.to_le_bytes());
        datagram.push(1);
        datagram.push(0);
        datagram.extend_from_slice(&datagram_len.to_le_bytes());
        datagram.extend_from_slice(&body);
        let (_h, msgs) = decode_datagram(&datagram).unwrap();
        match &msgs[0] {
            Message::ChannelReset(ts) => assert_eq!(*ts, 777),
            other => panic!("expected channel reset, got {other:?}"),
        }
    }

    #[test]
    fn bad_magic_errors() {
        assert!(decode_datagram(&[0u8; 30]).is_err());
    }

    #[test]
    fn runt_message_decodes_to_other_without_panicking() {
        // A datagram declaring one MSG_QUOTE but truncated so the body is absent: the walker accepts
        // the (under-declared) msg_len, but the bounds-checked body reader must yield `Other`
        // instead of indexing past the 28-byte datagram. Regression for the runt-datagram DoS.
        let mut f = Vec::new();
        f.extend_from_slice(&MAGIC.to_le_bytes());
        f.push(1); // schema
        f.push(0); // channel
        f.extend_from_slice(&0u64.to_le_bytes()); // sequence
        f.extend_from_slice(&0u64.to_le_bytes()); // send ts
        f.push(1); // msg_count = 1
        f.push(0); // reset count
        f.extend_from_slice(&28u16.to_le_bytes()); // datagram_length = 28 (== buf len)
        f.push(MSG_QUOTE);
        f.push(4); // msg_len = 4 (header only; passes the walker's >= MSG_HEADER_SIZE check)
        f.extend_from_slice(&0u16.to_le_bytes()); // flags
        assert_eq!(f.len(), 28);

        let (hdr, msgs) = decode_datagram(&f).expect("must not panic on a truncated message");
        assert_eq!(hdr.msg_count, 1);
        assert!(
            matches!(msgs.as_slice(), [Message::Other(MSG_QUOTE)]),
            "a truncated quote must decode to Other, got {msgs:?}"
        );
    }

    #[test]
    fn source_registry_maps_known_ids() {
        assert_eq!(source_name(1), Some("HYPERLIQUID"));
        assert_eq!(source_name(2), Some("PHOENIX"));
        assert_eq!(source_name(0), None); // reserved, never on wire
        assert_eq!(source_name(999), None); // unassigned -> caller falls back
    }
}
