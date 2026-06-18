//! Decoder for the DoubleZero Edge **Market-by-Order** feed (frame magic `0x4444`).
//!
//! A sibling protocol carrying the full L3 resting-order population per instrument, with in-band
//! snapshot+delta recovery. It shares the 24-byte frame header / 4-byte message header / generic
//! frame-walker in [`crate::ingest::codec_common`]; only the magic and message bodies differ. It reuses
//! the 80-byte Top-of-Book `InstrumentDefinition` layout for reference data, and adds order
//! deltas (`OrderAdd`/`OrderCancel`/`OrderExecute`), batch/reset control messages, and a snapshot
//! group (`SnapshotBegin`/`SnapshotOrder`/`SnapshotEnd`) on a dedicated port. The reconstructed
//! book is re-served as a full-state `depth` product (see PROTOCOL.md), never as raw deltas.
//!
//! ⚠️ Byte offsets below come from the edge-feed-spec draft, **not** a byte-validated reference
//! codec. They must be confirmed against a live frame hexdump before this decoder's output is
//! trusted in production; the round-trip tests here only pin internal self-consistency.

use anyhow::Result;

pub use crate::ingest::codec_common::MSG_HEADER_SIZE;
use crate::ingest::codec_common::{
    cstr, decode_frame_with, i64le, u16le, u32le, u64le, u8le, FrameHeader,
};

pub const MAGIC: u16 = 0x4444; // "DD"

// Shared / reference-data message types (same wire layout as Top-of-Book).
pub const MSG_HEARTBEAT: u8 = 0x01;
pub const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;
pub const MSG_TRADE: u8 = 0x04;
pub const MSG_END_OF_SESSION: u8 = 0x06;
pub const MSG_MANIFEST_SUMMARY: u8 = 0x07;
// Market-by-Order delta + control messages (mktdata port).
pub const MSG_ORDER_ADD: u8 = 0x10;
pub const MSG_ORDER_CANCEL: u8 = 0x11;
pub const MSG_ORDER_EXECUTE: u8 = 0x12;
pub const MSG_BATCH_BOUNDARY: u8 = 0x13;
pub const MSG_INSTRUMENT_RESET: u8 = 0x14;
// Snapshot group (snapshot port).
pub const MSG_SNAPSHOT_BEGIN: u8 = 0x20;
pub const MSG_SNAPSHOT_ORDER: u8 = 0x21;
pub const MSG_SNAPSHOT_END: u8 = 0x22;

/// Order side on the book: bid (buy) or ask (sell). Wire byte `0`=Bid, `1`=Ask (per
/// edge-feed-spec market-by-order). The processor tests `side == SIDE_BID`; `SIDE_ASK`
/// documents the other value (used by encoders/tests).
pub const SIDE_BID: u8 = 0;
#[allow(dead_code)]
pub const SIDE_ASK: u8 = 1;

/// On-wire message sizes (including the 4-byte header). The decoder reads each length from the
/// wire; these are kept for parity with the spec and for the round-trip encoders.
#[allow(dead_code)]
pub mod sizes {
    pub const ORDER_ADD: u8 = 52;
    pub const ORDER_CANCEL: u8 = 32;
    pub const ORDER_EXECUTE: u8 = 56;
    pub const BATCH_BOUNDARY: u8 = 16;
    pub const INSTRUMENT_RESET: u8 = 28;
    pub const SNAPSHOT_BEGIN: u8 = 36;
    pub const SNAPSHOT_ORDER: u8 = 44;
    pub const SNAPSHOT_END: u8 = 20;
    pub const INSTRUMENT_DEFINITION: u8 = 80;
    pub const TRADE: u8 = 52;
}

/// 80-byte instrument definition (same layout as Top-of-Book). Only the fields the bridge needs.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstrumentDefinition {
    pub instrument_id: u32,
    pub symbol: String,
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

/// A new resting order (`OrderAdd`, 0x10).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrderAdd {
    pub instrument_id: u32,
    pub source_id: u16,
    pub side: u8,
    pub order_flags: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub enter_ts: u64,
    pub price_raw: i64,
    pub qty_raw: u64,
}

/// An order removed without execution (`OrderCancel`, 0x11).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrderCancel {
    pub instrument_id: u32,
    pub source_id: u16,
    pub reason: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub ts: u64,
}

/// A resting-order partial/full fill (`OrderExecute`, 0x12).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OrderExecute {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub exec_flags: u8,
    pub per_instrument_seq: u32,
    pub order_id: u64,
    pub trade_id: u64,
    pub ts: u64,
    pub exec_price_raw: i64,
    pub exec_qty_raw: u64,
}

/// Per-instrument surgical resync (`InstrumentReset`, 0x14): drop the book and re-snapshot.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InstrumentReset {
    pub instrument_id: u32,
    pub reason: u8,
    pub new_anchor_seq: u64,
    pub ts: u64,
}

/// Start of a snapshot group (`SnapshotBegin`, 0x20).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SnapshotBegin {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub total_orders: u32,
    pub snapshot_id: u32,
    pub last_instrument_seq: u32,
    pub ts: u64,
}

/// One resting order within a snapshot (`SnapshotOrder`, 0x21).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SnapshotOrder {
    pub snapshot_id: u32,
    pub order_id: u64,
    pub side: u8,
    pub order_flags: u8,
    pub enter_ts: u64,
    pub price_raw: i64,
    pub qty_raw: u64,
}

/// Close of a snapshot group (`SnapshotEnd`, 0x22).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SnapshotEnd {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub snapshot_id: u32,
}

/// A trade print (same 52-byte layout as the Top-of-Book `Trade`).
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
    InstrumentDefinition(InstrumentDefinition),
    ManifestSummary(ManifestSummary),
    Trade(Trade),
    OrderAdd(OrderAdd),
    OrderCancel(OrderCancel),
    OrderExecute(OrderExecute),
    /// 0x13 BatchBoundary - optional atomic batch delimiter; carries (batch_id, batch_time). The
    /// bridge coalesces emission per frame rather than per batch, so the contents are diagnostic.
    BatchBoundary(#[allow(dead_code)] u32, #[allow(dead_code)] u64),
    InstrumentReset(InstrumentReset),
    SnapshotBegin(SnapshotBegin),
    SnapshotOrder(SnapshotOrder),
    SnapshotEnd(SnapshotEnd),
    Heartbeat,
    /// 0x06 EndOfSession - no more data this session. Carries ts.
    EndOfSession(u64),
    /// Any other message type; the byte is the raw wire type, kept for diagnostics.
    Other(#[allow(dead_code)] u8),
}

/// Decode one Market-by-Order UDP datagram into a header and its application messages.
pub fn decode_frame(buf: &[u8]) -> Result<(FrameHeader, Vec<Message>)> {
    decode_frame_with(buf, MAGIC, |msg_type, _flags, b, o| {
        decode_message(msg_type, b, o)
    })
}

fn decode_message(msg_type: u8, b: &[u8], o: usize) -> Message {
    // A message shorter than its declared type's fields decodes to `None` -> `Other` (skipped),
    // never an out-of-bounds panic (the readers are bounds-checked; see `codec_common`).
    decode_body(msg_type, b, o).unwrap_or(Message::Other(msg_type))
}

fn decode_body(msg_type: u8, b: &[u8], o: usize) -> Option<Message> {
    let body = o + MSG_HEADER_SIZE;
    Some(match msg_type {
        MSG_ORDER_ADD => Message::OrderAdd(OrderAdd {
            instrument_id: u32le(b, body)?,
            source_id: u16le(b, body + 4)?,
            side: u8le(b, body + 6)?,
            order_flags: u8le(b, body + 7)?,
            per_instrument_seq: u32le(b, body + 8)?,
            order_id: u64le(b, body + 12)?,
            enter_ts: u64le(b, body + 20)?,
            price_raw: i64le(b, body + 28)?,
            qty_raw: u64le(b, body + 36)?,
        }),
        MSG_ORDER_CANCEL => Message::OrderCancel(OrderCancel {
            instrument_id: u32le(b, body)?,
            source_id: u16le(b, body + 4)?,
            reason: u8le(b, body + 6)?,
            per_instrument_seq: u32le(b, body + 8)?,
            order_id: u64le(b, body + 12)?,
            ts: u64le(b, body + 20)?,
        }),
        MSG_ORDER_EXECUTE => Message::OrderExecute(OrderExecute {
            instrument_id: u32le(b, body)?,
            source_id: u16le(b, body + 4)?,
            aggressor_side: u8le(b, body + 6)?,
            exec_flags: u8le(b, body + 7)?,
            per_instrument_seq: u32le(b, body + 8)?,
            order_id: u64le(b, body + 12)?,
            trade_id: u64le(b, body + 20)?,
            ts: u64le(b, body + 28)?,
            exec_price_raw: i64le(b, body + 36)?,
            exec_qty_raw: u64le(b, body + 44)?,
        }),
        MSG_BATCH_BOUNDARY => Message::BatchBoundary(u32le(b, body)?, u64le(b, body + 4)?),
        MSG_INSTRUMENT_RESET => Message::InstrumentReset(InstrumentReset {
            instrument_id: u32le(b, body)?,
            reason: u8le(b, body + 4)?,
            new_anchor_seq: u64le(b, body + 8)?,
            ts: u64le(b, body + 16)?,
        }),
        MSG_SNAPSHOT_BEGIN => Message::SnapshotBegin(SnapshotBegin {
            instrument_id: u32le(b, body)?,
            anchor_seq: u64le(b, body + 4)?,
            total_orders: u32le(b, body + 12)?,
            snapshot_id: u32le(b, body + 16)?,
            last_instrument_seq: u32le(b, body + 20)?,
            ts: u64le(b, body + 24)?,
        }),
        MSG_SNAPSHOT_ORDER => Message::SnapshotOrder(SnapshotOrder {
            snapshot_id: u32le(b, body)?,
            order_id: u64le(b, body + 4)?,
            side: u8le(b, body + 12)?,
            order_flags: u8le(b, body + 13)?,
            enter_ts: u64le(b, body + 16)?,
            price_raw: i64le(b, body + 24)?,
            qty_raw: u64le(b, body + 32)?,
        }),
        MSG_SNAPSHOT_END => Message::SnapshotEnd(SnapshotEnd {
            instrument_id: u32le(b, body)?,
            anchor_seq: u64le(b, body + 4)?,
            snapshot_id: u32le(b, body + 12)?,
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
        MSG_INSTRUMENT_DEFINITION => Message::InstrumentDefinition(InstrumentDefinition {
            instrument_id: u32le(b, body)?,
            symbol: cstr(b, body + 4, 16)?,
            price_exponent: u8le(b, body + 37)? as i8,
            qty_exponent: u8le(b, body + 38)? as i8,
            manifest_seq: u16le(b, body + 74)?,
        }),
        MSG_MANIFEST_SUMMARY => Message::ManifestSummary(ManifestSummary {
            channel_id: u8le(b, body)?,
            valid: u8le(b, body + 1)? != 0,
            manifest_seq: u16le(b, body + 4)?,
            instrument_count: u32le(b, body + 8)?,
            ts: u64le(b, body + 12)?,
        }),
        MSG_HEARTBEAT => Message::Heartbeat,
        MSG_END_OF_SESSION => Message::EndOfSession(u64le(b, body)?),
        other => Message::Other(other),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ingest::codec_common::FRAME_HEADER_SIZE;

    pub(crate) fn frame(messages: &[Vec<u8>]) -> Vec<u8> {
        let body: Vec<u8> = messages.concat();
        let frame_len = (FRAME_HEADER_SIZE + body.len()) as u16;
        let mut f = Vec::new();
        f.extend_from_slice(&MAGIC.to_le_bytes());
        f.push(1); // schema version
        f.push(0); // channel
        f.extend_from_slice(&0u64.to_le_bytes()); // sequence
        f.extend_from_slice(&0u64.to_le_bytes()); // send ts
        f.push(messages.len() as u8);
        f.push(0); // reset count
        f.extend_from_slice(&frame_len.to_le_bytes());
        f.extend_from_slice(&body);
        f
    }

    pub(crate) fn enc_order_add(o: &OrderAdd) -> Vec<u8> {
        let mut b = vec![MSG_ORDER_ADD, sizes::ORDER_ADD, 0, 0];
        b.extend_from_slice(&o.instrument_id.to_le_bytes());
        b.extend_from_slice(&o.source_id.to_le_bytes());
        b.push(o.side);
        b.push(o.order_flags);
        b.extend_from_slice(&o.per_instrument_seq.to_le_bytes());
        b.extend_from_slice(&o.order_id.to_le_bytes());
        b.extend_from_slice(&o.enter_ts.to_le_bytes());
        b.extend_from_slice(&o.price_raw.to_le_bytes());
        b.extend_from_slice(&o.qty_raw.to_le_bytes());
        b.extend_from_slice(&[0u8; 4]); // reserved -> 52
        b
    }

    pub(crate) fn enc_snapshot_begin(s: &SnapshotBegin) -> Vec<u8> {
        let mut b = vec![MSG_SNAPSHOT_BEGIN, sizes::SNAPSHOT_BEGIN, 0, 0];
        b.extend_from_slice(&s.instrument_id.to_le_bytes());
        b.extend_from_slice(&s.anchor_seq.to_le_bytes());
        b.extend_from_slice(&s.total_orders.to_le_bytes());
        b.extend_from_slice(&s.snapshot_id.to_le_bytes());
        b.extend_from_slice(&s.last_instrument_seq.to_le_bytes());
        b.extend_from_slice(&s.ts.to_le_bytes());
        b
    }

    pub(crate) fn enc_snapshot_order(s: &SnapshotOrder) -> Vec<u8> {
        let mut b = vec![MSG_SNAPSHOT_ORDER, sizes::SNAPSHOT_ORDER, 0, 0];
        b.extend_from_slice(&s.snapshot_id.to_le_bytes());
        b.extend_from_slice(&s.order_id.to_le_bytes());
        b.push(s.side);
        b.push(s.order_flags);
        b.extend_from_slice(&[0u8; 2]); // reserved
        b.extend_from_slice(&s.enter_ts.to_le_bytes());
        b.extend_from_slice(&s.price_raw.to_le_bytes());
        b.extend_from_slice(&s.qty_raw.to_le_bytes());
        b
    }

    pub(crate) fn enc_snapshot_end(s: &SnapshotEnd) -> Vec<u8> {
        let mut b = vec![MSG_SNAPSHOT_END, sizes::SNAPSHOT_END, 0, 0];
        b.extend_from_slice(&s.instrument_id.to_le_bytes());
        b.extend_from_slice(&s.anchor_seq.to_le_bytes());
        b.extend_from_slice(&s.snapshot_id.to_le_bytes());
        b
    }

    #[test]
    fn order_add_round_trip() {
        let o = OrderAdd {
            instrument_id: 7,
            source_id: 1,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: 5,
            order_id: 12345,
            enter_ts: 1_780_000_000_000_000_000,
            price_raw: 18_420,
            qty_raw: 1000,
        };
        let f = frame(&[enc_order_add(&o)]);
        assert_eq!(f.len(), FRAME_HEADER_SIZE + 52);
        let (_h, msgs) = decode_frame(&f).unwrap();
        match &msgs[0] {
            Message::OrderAdd(got) => {
                assert_eq!(got.instrument_id, 7);
                assert_eq!(got.side, SIDE_BID);
                assert_eq!(got.per_instrument_seq, 5);
                assert_eq!(got.order_id, 12345);
                assert_eq!(got.price_raw, 18_420);
                assert_eq!(got.qty_raw, 1000);
            }
            other => panic!("expected order add, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_group_round_trips() {
        let begin = SnapshotBegin {
            instrument_id: 7,
            anchor_seq: 100,
            total_orders: 1,
            snapshot_id: 9,
            last_instrument_seq: 42,
            ts: 1,
        };
        let order = SnapshotOrder {
            snapshot_id: 9,
            order_id: 555,
            side: SIDE_ASK,
            order_flags: 0,
            enter_ts: 2,
            price_raw: 18_430,
            qty_raw: 500,
        };
        let end = SnapshotEnd {
            instrument_id: 7,
            anchor_seq: 100,
            snapshot_id: 9,
        };
        let f = frame(&[
            enc_snapshot_begin(&begin),
            enc_snapshot_order(&order),
            enc_snapshot_end(&end),
        ]);
        let (_h, msgs) = decode_frame(&f).unwrap();
        assert_eq!(msgs.len(), 3);
        match (&msgs[0], &msgs[1], &msgs[2]) {
            (Message::SnapshotBegin(b), Message::SnapshotOrder(o), Message::SnapshotEnd(e)) => {
                assert_eq!(b.snapshot_id, 9);
                assert_eq!(b.last_instrument_seq, 42);
                assert_eq!(o.order_id, 555);
                assert_eq!(o.side, SIDE_ASK);
                assert_eq!(o.price_raw, 18_430);
                assert_eq!(e.snapshot_id, 9);
            }
            other => panic!("unexpected snapshot group: {other:?}"),
        }
    }

    /// Spec (edge-feed-spec market-by-order): Side field 0=Bid/Buy, 1=Ask/Sell.
    /// Also matches the Hyperliquid publisher: SIDE_BID=0, SIDE_ASK=1.
    /// An inverted constant routes all wire bids to asks and vice versa, crossing the book.
    #[test]
    fn side_constants_match_spec() {
        assert_eq!(
            SIDE_BID, 0,
            "SIDE_BID must be 0 per edge-feed-spec (0=Bid/Buy)"
        );
        assert_eq!(
            SIDE_ASK, 1,
            "SIDE_ASK must be 1 per edge-feed-spec (1=Ask/Sell)"
        );
    }

    /// Wire byte 0 must decode as a bid and wire byte 1 as an ask.
    #[test]
    fn side_byte_zero_is_bid_one_is_ask() {
        let make = |side: u8| {
            let o = OrderAdd {
                instrument_id: 1,
                source_id: 0,
                side,
                order_flags: 0,
                per_instrument_seq: 1,
                order_id: 1,
                enter_ts: 0,
                price_raw: 100,
                qty_raw: 1,
            };
            let f = frame(&[enc_order_add(&o)]);
            let (_h, msgs) = decode_frame(&f).unwrap();
            match &msgs[0] {
                Message::OrderAdd(got) => got.side,
                other => panic!("expected OrderAdd, got {other:?}"),
            }
        };
        assert_eq!(make(0), SIDE_BID, "wire byte 0 must equal SIDE_BID");
        assert_eq!(make(1), SIDE_ASK, "wire byte 1 must equal SIDE_ASK");
    }

    #[test]
    fn bad_magic_errors() {
        let f = frame(&[enc_snapshot_end(&SnapshotEnd {
            instrument_id: 1,
            anchor_seq: 0,
            snapshot_id: 1,
        })]);
        let mut bad = f.clone();
        bad[0] = 0x5A;
        bad[1] = 0x44; // 0x445A (Top-of-Book)
        assert!(decode_frame(&bad).is_err());
        assert!(decode_frame(&f).is_ok());
    }
}
