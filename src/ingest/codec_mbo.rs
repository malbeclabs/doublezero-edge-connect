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
//! Byte offsets below are field-validated (issue #4), no longer draft — but the strength of the
//! oracle differs by message type, so be precise about what is proven:
//!   * **Shared-with-TOB types** (strongest) — the frame header, message header,
//!     `InstrumentDefinition`, `Trade`, `ManifestSummary`, and type tags `0x01/0x02/0x04/0x06/0x07`
//!     use the exact same wire layout as the **byte-validated** Top-of-Book [`crate::ingest::codec`]
//!     (validated against the edge-multicast-ref Go decoder). `tests/codec_mbo_fixtures.rs`'s
//!     `tob_shared_layouts_decode_identically` decodes the same bytes through both codecs and
//!     asserts equal fields, so this sharing is self-enforcing, not eyeballed.
//!   * **Real publisher capture** (strong) — `Order{Add,Cancel,Execute}`, `BatchBoundary`, the full
//!     `Snapshot{Begin,Order,End}` group, and the `InstrumentDefinition`/`ManifestSummary` above are
//!     decoded from the **real two-sided TYO recorder capture** (#36; `tests/fixtures/mbo_*.bin` —
//!     see `fixtures/PROVENANCE.md`) and their real field values asserted in
//!     `tests/codec_mbo_fixtures.rs`. The snapshot fixture is BTC's complete 44,598-order two-sided
//!     book, so `SnapshotOrder` (which populates the book) has tens of thousands of real orders of
//!     coverage; `total_orders == decoded order count` is asserted as a cross-field check.
//!   * **Offset-test-only** (weakest — confirm against a live frame before trusting) — `InstrumentReset`,
//!     `Heartbeat` and `EndOfSession` appear in **no** committed fixture; they are pinned solely by
//!     the offset-independent unit tests here, whose offsets are transcribed from the spec, so these
//!     catch a test-vs-decoder typo but not a shared misreading of the spec. (`Trade` has no MBO
//!     fixture either but is in the shared-with-TOB tier above, pinned to the byte-validated TOB
//!     codec.)
//!
//! `side` (the one enum the book logic branches on) is pinned to `0=Bid/1=Ask` (#2). The remaining
//! `reason`/`*_flags` bytes are opaque pass-through (no decoder branches on them), so a value
//! mismatch cannot corrupt the decode. Per-field offset citations live on the matching tests.

use std::sync::Arc;

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
    pub const END_OF_SESSION: u8 = 12;
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

// Field offsets per arm are validated against the authorities documented in the module header and
// asserted by the offset-independent tests below (e.g. `order_add_offsets_match_authority`, which
// carries the OrderAdd field map). Do not change an offset without updating its matching test.
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
            symbol: cstr(b, body + 4, 16)?.into(),
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

    pub(crate) fn enc_instrument_reset(r: &InstrumentReset) -> Vec<u8> {
        let mut b = vec![MSG_INSTRUMENT_RESET, sizes::INSTRUMENT_RESET, 0, 0];
        b.extend_from_slice(&r.instrument_id.to_le_bytes());
        b.push(r.reason);
        b.extend_from_slice(&[0u8; 3]); // reserved -> new_anchor_seq @ body+8
        b.extend_from_slice(&r.new_anchor_seq.to_le_bytes());
        b.extend_from_slice(&r.ts.to_le_bytes());
        b
    }

    pub(crate) fn enc_end_of_session(ts: u64) -> Vec<u8> {
        let mut b = vec![MSG_END_OF_SESSION, sizes::END_OF_SESSION, 0, 0]; // ts u64 @ body+0
        b.extend_from_slice(&ts.to_le_bytes());
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

    // --- Offset-independent field validation ---
    //
    // These build each message body by writing every field at a **literal** offset (via `put`),
    // wrap it in a real frame, decode through `decode_frame`, and assert each decoded field equals
    // the value written. Unlike the `enc_*` round-trips above (which mirror the decoder's own
    // sequential layout and so cannot catch a symmetric offset error), a decoder offset that
    // disagrees with the asserted layout fails here. The layouts asserted are:
    //   * shared-with-TOB types (frame header, message header, InstrumentDefinition, Trade,
    //     ManifestSummary, type tags 0x01/0x02/0x04/0x06/0x07): the byte-validated `codec.rs`
    //     (validated against the edge-multicast-ref Go decoder) — these offsets are identical there;
    //   * Order{Add,Cancel,Execute}/BatchBoundary/Snapshot{Begin,Order,End}: also backed by the real
    //     two-sided capture decoded in `tests/codec_mbo_fixtures.rs` (#36).
    // For the types with NO committed fixture (InstrumentReset, Heartbeat, EndOfSession), the test
    // below shares its sole human source with the decoder: it catches a test-vs-decoder typo, not a
    // common misreading of the spec. See the module header for the per-type oracle strength.

    /// Write `bytes` at a literal offset into a zeroed body.
    fn put(buf: &mut [u8], off: usize, bytes: &[u8]) {
        buf[off..off + bytes.len()].copy_from_slice(bytes);
    }

    /// Wrap one message body (fields placed at authoritative offsets) in a real frame. `msg_len`
    /// is the on-wire message length (4-byte header + body); reuses the `frame` helper's 24B header.
    fn frame_one(msg_type: u8, msg_len: u8, body: &[u8]) -> Vec<u8> {
        let mut msg = vec![msg_type, msg_len, 0, 0]; // type, len, flags:u16
        msg.extend_from_slice(body);
        frame(&[msg])
    }

    /// 24-byte frame header layout (codec_common::FrameHeader, same as the byte-validated TOB
    /// `codec.rs`). This test pins the fields it asserts below: schema_version u8@2, channel_id
    /// u8@3, sequence u64@4, send_ts u64@12, msg_count u8@20. (reset_count u8@21 / frame_length
    /// u16@22 round out the header but are not asserted here.)
    #[test]
    fn frame_header_offsets_match_authority() {
        let body = vec![0u8; 8]; // EndOfSession body (ts u64@0)
        let mut f = frame_one(MSG_END_OF_SESSION, sizes::END_OF_SESSION, &body);
        f[2] = 1; // schema_version
        f[3] = 0; // channel_id
        put(&mut f, 4, &0x1122u64.to_le_bytes()); // sequence
        put(&mut f, 12, &0x3344u64.to_le_bytes()); // send_ts
        let (h, _msgs) = decode_frame(&f).unwrap();
        assert_eq!(h.schema_version, 1);
        assert_eq!(h.channel_id, 0);
        assert_eq!(h.sequence, 0x1122);
        assert_eq!(h.send_ts, 0x3344);
        assert_eq!(h.msg_count, 1);
    }

    /// Message-type tags per edge-feed-spec MBO + publisher constants. A wrong tag misroutes a type.
    #[test]
    fn message_type_tags_match_authority() {
        assert_eq!(MSG_HEARTBEAT, 0x01);
        assert_eq!(MSG_INSTRUMENT_DEFINITION, 0x02);
        assert_eq!(MSG_TRADE, 0x04);
        assert_eq!(MSG_END_OF_SESSION, 0x06);
        assert_eq!(MSG_MANIFEST_SUMMARY, 0x07);
        assert_eq!(MSG_ORDER_ADD, 0x10);
        assert_eq!(MSG_ORDER_CANCEL, 0x11);
        assert_eq!(MSG_ORDER_EXECUTE, 0x12);
        assert_eq!(MSG_BATCH_BOUNDARY, 0x13);
        assert_eq!(MSG_INSTRUMENT_RESET, 0x14);
        assert_eq!(MSG_SNAPSHOT_BEGIN, 0x20);
        assert_eq!(MSG_SNAPSHOT_ORDER, 0x21);
        assert_eq!(MSG_SNAPSHOT_END, 0x22);
    }

    /// OrderAdd (0x10): instrument_id u32@0, source_id u16@4, side u8@6, order_flags u8@7,
    /// per_instrument_seq u32@8, order_id u64@12, enter_ts u64@20, price_raw i64@28, qty_raw u64@36.
    /// `source_id` is given a distinct multi-byte value (`0x0102`, not a side sentinel) and `side`
    /// is asserted both ways, so a decoder misreading `side` from the `source_id` offset can't pass.
    #[test]
    fn order_add_offsets_match_authority() {
        let decode_side = |side: u8| {
            let mut body = vec![0u8; 48]; // size 52 - 4 header
            put(&mut body, 0, &7u32.to_le_bytes());
            put(&mut body, 4, &0x0102u16.to_le_bytes());
            body[6] = side;
            body[7] = 0;
            put(&mut body, 8, &5u32.to_le_bytes());
            put(&mut body, 12, &12_345u64.to_le_bytes());
            put(&mut body, 20, &1_780_000_000_000_000_000u64.to_le_bytes());
            put(&mut body, 28, &18_420i64.to_le_bytes());
            put(&mut body, 36, &1_000u64.to_le_bytes());
            let f = frame_one(MSG_ORDER_ADD, sizes::ORDER_ADD, &body);
            match decode_frame(&f).unwrap().1.remove(0) {
                Message::OrderAdd(g) => {
                    assert_eq!(g.instrument_id, 7);
                    assert_eq!(g.source_id, 0x0102);
                    assert_eq!(g.order_flags, 0);
                    assert_eq!(g.per_instrument_seq, 5);
                    assert_eq!(g.order_id, 12_345);
                    assert_eq!(g.enter_ts, 1_780_000_000_000_000_000);
                    assert_eq!(g.price_raw, 18_420);
                    assert_eq!(g.qty_raw, 1_000);
                    g.side
                }
                other => panic!("expected OrderAdd, got {other:?}"),
            }
        };
        assert_eq!(decode_side(0), SIDE_BID);
        assert_eq!(decode_side(1), SIDE_ASK);
    }

    /// OrderCancel (0x11): instrument_id u32@0, source_id u16@4, reason u8@6,
    /// per_instrument_seq u32@8, order_id u64@12, ts u64@20.
    #[test]
    fn order_cancel_offsets_match_authority() {
        let mut body = vec![0u8; 28]; // size 32 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, &1u16.to_le_bytes());
        body[6] = 3; // reason (opaque pass-through byte)
        put(&mut body, 8, &9u32.to_le_bytes());
        put(&mut body, 12, &555u64.to_le_bytes());
        put(&mut body, 20, &42u64.to_le_bytes());
        let f = frame_one(MSG_ORDER_CANCEL, sizes::ORDER_CANCEL, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::OrderCancel(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.source_id, 1);
                assert_eq!(g.reason, 3);
                assert_eq!(g.per_instrument_seq, 9);
                assert_eq!(g.order_id, 555);
                assert_eq!(g.ts, 42);
            }
            other => panic!("expected OrderCancel, got {other:?}"),
        }
    }

    /// OrderExecute (0x12): instrument_id u32@0, source_id u16@4, aggressor_side u8@6,
    /// exec_flags u8@7, per_instrument_seq u32@8, order_id u64@12, trade_id u64@20, ts u64@28,
    /// exec_price_raw i64@36, exec_qty_raw u64@44.
    #[test]
    fn order_execute_offsets_match_authority() {
        let mut body = vec![0u8; 52]; // size 56 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, &1u16.to_le_bytes());
        body[6] = SIDE_BID;
        body[7] = 2;
        put(&mut body, 8, &11u32.to_le_bytes());
        put(&mut body, 12, &555u64.to_le_bytes());
        put(&mut body, 20, &7_777u64.to_le_bytes());
        put(&mut body, 28, &84u64.to_le_bytes());
        put(&mut body, 36, &18_400i64.to_le_bytes());
        put(&mut body, 44, &250u64.to_le_bytes());
        let f = frame_one(MSG_ORDER_EXECUTE, sizes::ORDER_EXECUTE, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::OrderExecute(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.source_id, 1);
                assert_eq!(g.aggressor_side, SIDE_BID);
                assert_eq!(g.exec_flags, 2);
                assert_eq!(g.per_instrument_seq, 11);
                assert_eq!(g.order_id, 555);
                assert_eq!(g.trade_id, 7_777);
                assert_eq!(g.ts, 84);
                assert_eq!(g.exec_price_raw, 18_400);
                assert_eq!(g.exec_qty_raw, 250);
            }
            other => panic!("expected OrderExecute, got {other:?}"),
        }
    }

    /// SnapshotBegin (0x20): instrument_id u32@0, anchor_seq u64@4, total_orders u32@12,
    /// snapshot_id u32@16, last_instrument_seq u32@20, ts u64@24.
    #[test]
    fn snapshot_begin_offsets_match_authority() {
        let mut body = vec![0u8; 32]; // size 36 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, &100u64.to_le_bytes());
        put(&mut body, 12, &3u32.to_le_bytes());
        put(&mut body, 16, &9u32.to_le_bytes());
        put(&mut body, 20, &42u32.to_le_bytes());
        put(&mut body, 24, &1u64.to_le_bytes());
        let f = frame_one(MSG_SNAPSHOT_BEGIN, sizes::SNAPSHOT_BEGIN, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::SnapshotBegin(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.anchor_seq, 100);
                assert_eq!(g.total_orders, 3);
                assert_eq!(g.snapshot_id, 9);
                assert_eq!(g.last_instrument_seq, 42);
                assert_eq!(g.ts, 1);
            }
            other => panic!("expected SnapshotBegin, got {other:?}"),
        }
    }

    /// SnapshotOrder (0x21): snapshot_id u32@0, order_id u64@4, side u8@12, order_flags u8@13,
    /// enter_ts u64@16, price_raw i64@24, qty_raw u64@32. Side asserted both ways.
    #[test]
    fn snapshot_order_offsets_match_authority() {
        let decode_side = |side: u8| {
            let mut body = vec![0u8; 40]; // size 44 - 4 header
            put(&mut body, 0, &9u32.to_le_bytes());
            put(&mut body, 4, &555u64.to_le_bytes());
            body[12] = side;
            body[13] = 0;
            put(&mut body, 16, &2u64.to_le_bytes());
            put(&mut body, 24, &18_430i64.to_le_bytes());
            put(&mut body, 32, &500u64.to_le_bytes());
            let f = frame_one(MSG_SNAPSHOT_ORDER, sizes::SNAPSHOT_ORDER, &body);
            match decode_frame(&f).unwrap().1.remove(0) {
                Message::SnapshotOrder(g) => {
                    assert_eq!(g.snapshot_id, 9);
                    assert_eq!(g.order_id, 555);
                    assert_eq!(g.enter_ts, 2);
                    assert_eq!(g.price_raw, 18_430);
                    assert_eq!(g.qty_raw, 500);
                    g.side
                }
                other => panic!("expected SnapshotOrder, got {other:?}"),
            }
        };
        assert_eq!(decode_side(0), SIDE_BID);
        assert_eq!(decode_side(1), SIDE_ASK);
    }

    /// SnapshotEnd (0x22): instrument_id u32@0, anchor_seq u64@4, snapshot_id u32@12.
    #[test]
    fn snapshot_end_offsets_match_authority() {
        let mut body = vec![0u8; 16]; // size 20 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, &100u64.to_le_bytes());
        put(&mut body, 12, &9u32.to_le_bytes());
        let f = frame_one(MSG_SNAPSHOT_END, sizes::SNAPSHOT_END, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::SnapshotEnd(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.anchor_seq, 100);
                assert_eq!(g.snapshot_id, 9);
            }
            other => panic!("expected SnapshotEnd, got {other:?}"),
        }
    }

    /// ManifestSummary (0x07): channel_id u8@0, valid u8@1, manifest_seq u16@4,
    /// instrument_count u32@8, ts u64@12 — identical to the byte-validated TOB `codec.rs` layout
    /// (body spans 0..20 -> on-wire size 24). `valid` decodes both 0 and non-0 (the `Valid=0`
    /// flag the processor overrides). Resolves the plan's "size 20 vs fields-to-24" suspicion:
    /// the body is 20 bytes; there is no size-20 constant in code.
    #[test]
    fn manifest_summary_offsets_match_authority() {
        let build = |valid: u8| {
            let mut body = vec![0u8; 20];
            body[0] = 2; // channel_id
            body[1] = valid;
            put(&mut body, 4, &13u16.to_le_bytes()); // manifest_seq
            put(&mut body, 8, &786u32.to_le_bytes()); // instrument_count
            put(&mut body, 12, &1_780u64.to_le_bytes()); // ts
            let f = frame_one(MSG_MANIFEST_SUMMARY, 24, &body);
            match decode_frame(&f).unwrap().1.remove(0) {
                Message::ManifestSummary(g) => g,
                other => panic!("expected ManifestSummary, got {other:?}"),
            }
        };
        let g = build(1);
        assert_eq!(g.channel_id, 2);
        assert!(g.valid);
        assert_eq!(g.manifest_seq, 13);
        assert_eq!(g.instrument_count, 786);
        assert_eq!(g.ts, 1_780);
        assert!(!build(0).valid, "valid byte 0 must decode false");
    }

    /// InstrumentDefinition (0x02): instrument_id u32@0, symbol cstr@4 (16B NUL-padded),
    /// price_exponent i8@37, qty_exponent i8@38, manifest_seq u16@74 — the 80-byte layout shared
    /// byte-for-byte with the byte-validated TOB `codec.rs` InstrumentDefinition.
    #[test]
    fn instrument_definition_offsets_match_authority() {
        let mut body = vec![0u8; 76]; // size 80 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, b"BTC"); // rest stays NUL
        body[37] = (-1i8) as u8; // price_exponent
        body[38] = (-8i8) as u8; // qty_exponent
        put(&mut body, 74, &13u16.to_le_bytes()); // manifest_seq
        let f = frame_one(
            MSG_INSTRUMENT_DEFINITION,
            sizes::INSTRUMENT_DEFINITION,
            &body,
        );
        match &decode_frame(&f).unwrap().1[0] {
            Message::InstrumentDefinition(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.symbol.as_ref(), "BTC");
                assert_eq!(g.price_exponent, -1);
                assert_eq!(g.qty_exponent, -8);
                assert_eq!(g.manifest_seq, 13);
            }
            other => panic!("expected InstrumentDefinition, got {other:?}"),
        }
    }

    /// Trade (0x04): instrument_id u32@0, source_id u16@4, aggressor_side u8@6, trade_flags u8@7,
    /// source_ts u64@8, trade_price_raw i64@16, trade_qty_raw u64@24, trade_id u64@32,
    /// cumulative_volume_raw u64@40 — the 52-byte layout shared byte-for-byte with TOB `codec.rs`.
    #[test]
    fn trade_offsets_match_authority() {
        let mut body = vec![0u8; 48]; // size 52 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        put(&mut body, 4, &1u16.to_le_bytes());
        body[6] = 2; // aggressor_side (sell, per TOB aggressor_side mapping)
        body[7] = 0;
        put(&mut body, 8, &1_780_000_000_000_000_000u64.to_le_bytes());
        put(&mut body, 16, &18_420i64.to_le_bytes());
        put(&mut body, 24, &1_500u64.to_le_bytes());
        put(&mut body, 32, &99_887_766u64.to_le_bytes());
        put(&mut body, 40, &5_000_000u64.to_le_bytes());
        let f = frame_one(MSG_TRADE, sizes::TRADE, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::Trade(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.source_id, 1);
                assert_eq!(g.aggressor_side, 2);
                assert_eq!(g.trade_flags, 0);
                assert_eq!(g.source_ts, 1_780_000_000_000_000_000);
                assert_eq!(g.trade_price_raw, 18_420);
                assert_eq!(g.trade_qty_raw, 1_500);
                assert_eq!(g.trade_id, 99_887_766);
                assert_eq!(g.cumulative_volume_raw, 5_000_000);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    /// BatchBoundary (0x13): batch_id u32@0, batch_time u64@4.
    #[test]
    fn batch_boundary_offsets_match_authority() {
        let mut body = vec![0u8; 12]; // size 16 - 4 header
        put(&mut body, 0, &77u32.to_le_bytes());
        put(&mut body, 4, &1_780u64.to_le_bytes());
        let f = frame_one(MSG_BATCH_BOUNDARY, sizes::BATCH_BOUNDARY, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::BatchBoundary(id, time) => {
                assert_eq!(*id, 77);
                assert_eq!(*time, 1_780);
            }
            other => panic!("expected BatchBoundary, got {other:?}"),
        }
    }

    /// InstrumentReset (0x14): instrument_id u32@0, reason u8@4, new_anchor_seq u64@8, ts u64@16.
    #[test]
    fn instrument_reset_offsets_match_authority() {
        let mut body = vec![0u8; 24]; // size 28 - 4 header
        put(&mut body, 0, &7u32.to_le_bytes());
        body[4] = 1; // reason (opaque pass-through byte)
        put(&mut body, 8, &200u64.to_le_bytes());
        put(&mut body, 16, &42u64.to_le_bytes());
        let f = frame_one(MSG_INSTRUMENT_RESET, sizes::INSTRUMENT_RESET, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::InstrumentReset(g) => {
                assert_eq!(g.instrument_id, 7);
                assert_eq!(g.reason, 1);
                assert_eq!(g.new_anchor_seq, 200);
                assert_eq!(g.ts, 42);
            }
            other => panic!("expected InstrumentReset, got {other:?}"),
        }
    }

    /// EndOfSession (0x06): ts u64@0.
    #[test]
    fn end_of_session_offsets_match_authority() {
        let mut body = vec![0u8; 8];
        put(&mut body, 0, &1_780u64.to_le_bytes());
        let f = frame_one(MSG_END_OF_SESSION, sizes::END_OF_SESSION, &body);
        match &decode_frame(&f).unwrap().1[0] {
            Message::EndOfSession(ts) => assert_eq!(*ts, 1_780),
            other => panic!("expected EndOfSession, got {other:?}"),
        }
    }

    /// Heartbeat (0x01): header only, no body. Decodes to the variant with msg_count 1.
    #[test]
    fn heartbeat_decodes_with_no_body() {
        let f = frame_one(MSG_HEARTBEAT, 4, &[]);
        let (h, msgs) = decode_frame(&f).unwrap();
        assert_eq!(h.msg_count, 1);
        assert!(matches!(msgs.as_slice(), [Message::Heartbeat]));
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
