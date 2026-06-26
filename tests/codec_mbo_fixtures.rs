//! Empirical offset validation: decode the committed MBO fixtures — a **real two-sided TYO
//! recorder capture** of the HL publisher (#36; `tests/fixtures/mbo_*.bin`, see
//! `fixtures/PROVENANCE.md`) — through the real `codec_mbo::decode_frame` and assert the decoded
//! values against the capture's real content and per-field invariants. This is the ground-truth
//! counterpart to the offset-independent unit tests in `codec_mbo.rs`: a wrong field offset that
//! the symmetric round-trip cannot catch (as with the side-mapping bug in #2) shows up here as an
//! out-of-range side byte, a non-positive price, an instrument id outside the defined set, a
//! `total_orders` that disagrees with the order count, or a garbled symbol.

mod common;

use common::replay;
use doublezero_edge_connect::ingest::{
    codec,
    codec_mbo::{decode_frame, Message, MAGIC, SIDE_ASK, SIDE_BID},
};
use std::collections::BTreeSet;

/// Build a single-message frame with the given `magic` (24B codec_common header + one message).
/// Both codecs share the header layout, so the same builder feeds each codec by magic.
fn one_msg_frame(magic: u16, msg_type: u8, msg_len: u8, body: &[u8]) -> Vec<u8> {
    let mut f = Vec::new();
    f.extend_from_slice(&magic.to_le_bytes());
    f.push(1); // schema_version
    f.push(0); // channel_id
    f.extend_from_slice(&0u64.to_le_bytes()); // sequence
    f.extend_from_slice(&0u64.to_le_bytes()); // send_ts
    f.push(1); // msg_count
    f.push(0); // reset_count
    let frame_len = (24 + 4 + body.len()) as u16;
    f.extend_from_slice(&frame_len.to_le_bytes());
    f.extend_from_slice(&[msg_type, msg_len, 0, 0]); // msg header: type, len, flags:u16
    f.extend_from_slice(body);
    f
}

/// The MBO `InstrumentDefinition`, `Trade` and `ManifestSummary` layouts are documented as
/// identical to the **byte-validated** TOB `codec.rs`. Make that self-enforcing rather than
/// eyeballed: decode the *same* body bytes through both codecs (only the frame magic differs) and
/// assert the shared fields match. If a future edit drifts one decoder's offsets from the other,
/// this fails — closing the gap that `Trade` has no MBO golden fixture and `InstrumentDefinition`/
/// `ManifestSummary` are validated transitively.
#[test]
fn tob_shared_layouts_decode_identically() {
    // InstrumentDefinition (tag 0x02 in both): instrument_id@0, symbol@4(16), pexp@37, qexp@38, seq@74.
    let mut def = vec![0u8; 76];
    def[0..4].copy_from_slice(&7u32.to_le_bytes());
    def[4..7].copy_from_slice(b"BTC");
    def[37] = (-1i8) as u8;
    def[38] = (-8i8) as u8;
    def[74..76].copy_from_slice(&13u16.to_le_bytes());
    let tob = codec::decode_frame(&one_msg_frame(codec::MAGIC, 0x02, 80, &def))
        .unwrap()
        .1;
    let mbo = decode_frame(&one_msg_frame(MAGIC, 0x02, 80, &def))
        .unwrap()
        .1;
    match (&tob[0], &mbo[0]) {
        (codec::Message::InstrumentDefinition(t), Message::InstrumentDefinition(m)) => {
            assert_eq!(t.instrument_id, m.instrument_id);
            assert_eq!(t.symbol, m.symbol);
            assert_eq!(t.price_exponent, m.price_exponent);
            assert_eq!(t.qty_exponent, m.qty_exponent);
            assert_eq!(t.manifest_seq, m.manifest_seq);
            assert_eq!(m.symbol, "BTC");
        }
        other => panic!("expected InstrumentDefinition from both, got {other:?}"),
    }

    // Trade (tag 0x04 in both).
    let mut tr = vec![0u8; 48];
    tr[0..4].copy_from_slice(&7u32.to_le_bytes());
    tr[4..6].copy_from_slice(&1u16.to_le_bytes());
    tr[6] = 2; // aggressor_side
    tr[8..16].copy_from_slice(&1_780u64.to_le_bytes()); // source_ts
    tr[16..24].copy_from_slice(&18_420i64.to_le_bytes()); // price
    tr[24..32].copy_from_slice(&1_500u64.to_le_bytes()); // qty
    tr[32..40].copy_from_slice(&99u64.to_le_bytes()); // trade_id
    tr[40..48].copy_from_slice(&5u64.to_le_bytes()); // cumulative_volume
    let tob = codec::decode_frame(&one_msg_frame(codec::MAGIC, 0x04, 52, &tr))
        .unwrap()
        .1;
    let mbo = decode_frame(&one_msg_frame(MAGIC, 0x04, 52, &tr))
        .unwrap()
        .1;
    match (&tob[0], &mbo[0]) {
        (codec::Message::Trade(t), Message::Trade(m)) => {
            assert_eq!(t.instrument_id, m.instrument_id);
            assert_eq!(t.source_id, m.source_id);
            assert_eq!(t.aggressor_side, m.aggressor_side);
            assert_eq!(t.source_ts, m.source_ts);
            assert_eq!(t.trade_price_raw, m.trade_price_raw);
            assert_eq!(t.trade_qty_raw, m.trade_qty_raw);
            assert_eq!(t.trade_id, m.trade_id);
            assert_eq!(t.cumulative_volume_raw, m.cumulative_volume_raw);
        }
        other => panic!("expected Trade from both, got {other:?}"),
    }

    // ManifestSummary (tag 0x07 in both): channel_id@0, valid@1, manifest_seq@4, count@8, ts@12.
    let mut ms = vec![0u8; 20];
    ms[0] = 2;
    ms[1] = 1;
    ms[4..6].copy_from_slice(&13u16.to_le_bytes());
    ms[8..12].copy_from_slice(&786u32.to_le_bytes());
    ms[12..20].copy_from_slice(&1_780u64.to_le_bytes());
    let tob = codec::decode_frame(&one_msg_frame(codec::MAGIC, 0x07, 24, &ms))
        .unwrap()
        .1;
    let mbo = decode_frame(&one_msg_frame(MAGIC, 0x07, 24, &ms))
        .unwrap()
        .1;
    match (&tob[0], &mbo[0]) {
        (codec::Message::ManifestSummary(t), Message::ManifestSummary(m)) => {
            assert_eq!(t.channel_id, m.channel_id);
            assert_eq!(t.valid, m.valid);
            assert_eq!(t.manifest_seq, m.manifest_seq);
            assert_eq!(t.instrument_count, m.instrument_count);
            assert_eq!(t.ts, m.ts);
        }
        other => panic!("expected ManifestSummary from both, got {other:?}"),
    }
}

fn variant_name(m: &Message) -> &'static str {
    match m {
        Message::InstrumentDefinition(_) => "InstrumentDefinition",
        Message::ManifestSummary(_) => "ManifestSummary",
        Message::Trade(_) => "Trade",
        Message::OrderAdd(_) => "OrderAdd",
        Message::OrderCancel(_) => "OrderCancel",
        Message::OrderExecute(_) => "OrderExecute",
        Message::BatchBoundary(..) => "BatchBoundary",
        Message::InstrumentReset(_) => "InstrumentReset",
        Message::SnapshotBegin(_) => "SnapshotBegin",
        Message::SnapshotOrder(_) => "SnapshotOrder",
        Message::SnapshotEnd(_) => "SnapshotEnd",
        Message::Heartbeat => "Heartbeat",
        Message::EndOfSession(_) => "EndOfSession",
        Message::Other(_) => "Other",
    }
}

fn frames(path: &str) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    replay::split_frames(&bytes, MAGIC)
}

/// Collect the instrument ids carried by every `InstrumentDefinition` in the refdata fixture.
fn defined_instrument_ids() -> BTreeSet<u32> {
    let mut ids = BTreeSet::new();
    for f in frames("tests/fixtures/mbo_refdata.bin") {
        for m in decode_frame(&f).unwrap().1 {
            if let Message::InstrumentDefinition(d) = m {
                ids.insert(d.instrument_id);
            }
        }
    }
    ids
}

/// Every committed real frame must decode without a framing error, and every decoded field must
/// satisfy the invariants that only hold when its offset is correct. The MBO trio is a real
/// two-sided TYO recorder capture (#36, PROVENANCE.md): refdata defines ~640 instruments, the
/// snapshot is BTC's full two-sided book, and mktdata carries post-anchor deltas across many
/// instruments. The cross-fixture offset check is aggregate: order `instrument_id`s are small,
/// sane venue ids that overlap heavily with the defined set and include BTC (a wrong instrument_id
/// offset — e.g. reading into an adjacent u64 — would scatter huge ids with near-zero overlap), on
/// top of the per-message side-in-range / positive-price/qty invariants.
#[test]
fn committed_mbo_fixtures_decode_cleanly() {
    let defined = defined_instrument_ids();
    assert!(
        defined.contains(&0),
        "BTC (id 0) must be defined in refdata"
    );

    let mut seen = BTreeSet::new();
    let mut order_ids = BTreeSet::new();
    for path in [
        "tests/fixtures/mbo_refdata.bin",
        "tests/fixtures/mbo_snapshot.bin",
        "tests/fixtures/mbo_mktdata.bin",
    ] {
        let fs = frames(path);
        assert!(!fs.is_empty(), "no frames in {path}");
        for f in &fs {
            let (_h, msgs) = decode_frame(f).unwrap_or_else(|e| panic!("{path}: {e}"));
            for m in &msgs {
                seen.insert(variant_name(m));
                match m {
                    Message::OrderAdd(o) => {
                        order_ids.insert(o.instrument_id);
                        assert!(
                            o.side == SIDE_BID || o.side == SIDE_ASK,
                            "{path}: OrderAdd side {} out of range -> offset bug",
                            o.side
                        );
                        assert!(
                            o.price_raw > 0 && o.qty_raw > 0,
                            "{path}: OrderAdd non-positive px/qty -> offset bug"
                        );
                    }
                    Message::OrderCancel(o) => {
                        order_ids.insert(o.instrument_id);
                    }
                    Message::OrderExecute(o) => {
                        order_ids.insert(o.instrument_id);
                        assert!(
                            o.aggressor_side == SIDE_BID || o.aggressor_side == SIDE_ASK,
                            "{path}: OrderExecute aggressor_side {} out of range",
                            o.aggressor_side
                        );
                        assert!(o.exec_price_raw > 0 && o.exec_qty_raw > 0);
                    }
                    Message::SnapshotOrder(o) => {
                        assert!(o.side == SIDE_BID || o.side == SIDE_ASK);
                        assert!(o.price_raw > 0 && o.qty_raw > 0);
                    }
                    Message::InstrumentDefinition(d) => {
                        assert!(!d.symbol.is_empty(), "{path}: empty symbol -> offset bug");
                    }
                    _ => {}
                }
            }
        }
    }

    // Aggregate instrument_id offset check (the minimal refdata does not define every instrument
    // that has deltas, so per-order membership is too strict — overlap + sane bound is the proof).
    assert!(
        order_ids.contains(&0),
        "BTC (id 0) must appear among order deltas"
    );
    assert!(
        order_ids.iter().all(|&id| id < 1_000_000),
        "order instrument_id out of sane venue range -> offset bug"
    );
    let overlap = order_ids.iter().filter(|id| defined.contains(id)).count();
    assert!(
        overlap * 2 >= order_ids.len(),
        "only {overlap}/{} order instrument_ids are in the defined set -> offset bug",
        order_ids.len()
    );
    // Real-capture coverage: order deltas, the full snapshot group, defs, manifest, batch.
    for ty in [
        "OrderAdd",
        "OrderCancel",
        "OrderExecute",
        "SnapshotBegin",
        "SnapshotOrder",
        "SnapshotEnd",
        "InstrumentDefinition",
        "ManifestSummary",
    ] {
        assert!(
            seen.contains(ty),
            "expected {ty} in the committed fixtures, saw {seen:?}"
        );
    }
}

/// The refdata fixture defines BTC (instrument_id 0) among ~640 instruments. Asserting BTC's exact
/// decoded definition pins the `InstrumentDefinition` exponent offsets (37/38) and the symbol field
/// (16B cstr @ 4) against real publisher bytes. NOTE: the real capture's BTC carries
/// price_exponent=-8, qty_exponent=-5 (the older hand-crafted fixture used -1/-8) — the bytes are
/// authoritative and the shared offset is cross-validated against the byte-validated TOB codec by
/// `tob_shared_layouts_decode_identically`).
#[test]
fn refdata_fixture_matches_btc_definition() {
    let mut def = None;
    let mut manifest = None;
    for f in frames("tests/fixtures/mbo_refdata.bin") {
        for m in decode_frame(&f).unwrap().1 {
            match m {
                Message::InstrumentDefinition(d) if d.instrument_id == 0 => def = Some(d),
                Message::ManifestSummary(s) => manifest = Some(s),
                _ => {}
            }
        }
    }
    let def = def.expect("BTC definition present in mbo_refdata.bin");
    assert_eq!(def.symbol, "BTC", "symbol@4 (16B cstr) decoded wrong");
    assert_eq!(def.price_exponent, -8, "price_exponent@37 decoded wrong");
    assert_eq!(def.qty_exponent, -5, "qty_exponent@38 decoded wrong");

    let manifest = manifest.expect("ManifestSummary present in mbo_refdata.bin");
    // The refdata defines hundreds of instruments; a wrong instrument_count offset/width would not
    // land on a large, sane count. Also confirms channel_id@0 / valid@1 sit before manifest_seq@4.
    assert!(
        manifest.instrument_count >= 100,
        "manifest instrument_count {} -> offset bug",
        manifest.instrument_count
    );
}

/// The snapshot fixture is BTC's real, complete two-sided book (#36, PROVENANCE.md):
/// SnapshotBegin(instrument_id=0, anchor_seq=11243876, total_orders=44598, snapshot_id=1106238)
/// followed by 44,598 SnapshotOrders (28,345 bids + 16,253 asks) and a matching SnapshotEnd.
/// Asserting `total_orders == decoded SnapshotOrder count` is a strong cross-field check: the
/// begin's count field and every order's side/offset must all be right for it to hold.
#[test]
fn snapshot_fixture_matches_real_two_sided_book() {
    let mut begin = None;
    let mut end = None;
    let (mut bids, mut asks) = (0u32, 0u32);
    for f in frames("tests/fixtures/mbo_snapshot.bin") {
        for m in decode_frame(&f).unwrap().1 {
            match m {
                Message::SnapshotBegin(b) => begin = Some(b),
                Message::SnapshotEnd(e) => end = Some(e),
                Message::SnapshotOrder(o) if o.side == SIDE_BID => bids += 1,
                Message::SnapshotOrder(o) if o.side == SIDE_ASK => asks += 1,
                _ => {}
            }
        }
    }
    let begin = begin.expect("SnapshotBegin present");
    assert_eq!(begin.instrument_id, 0);
    assert_eq!(begin.anchor_seq, 11_243_876);
    assert_eq!(begin.snapshot_id, 1_106_238);
    assert_eq!(begin.total_orders, 44_598);
    assert_eq!(begin.last_instrument_seq, 26_761_712);
    assert!(bids > 0 && asks > 0, "snapshot must be two-sided");
    assert_eq!(
        begin.total_orders,
        bids + asks,
        "SnapshotBegin.total_orders must equal the decoded SnapshotOrder count"
    );
    assert_eq!((bids, asks), (28_345, 16_253));

    let end = end.expect("SnapshotEnd present");
    assert_eq!(end.instrument_id, 0);
    assert_eq!(end.anchor_seq, 11_243_876);
    assert_eq!(end.snapshot_id, 1_106_238);
}
