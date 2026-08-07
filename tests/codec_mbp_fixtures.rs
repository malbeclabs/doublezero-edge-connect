//! Cross-codec pinning for the market-by-price decoder.
//!
//! **There is no committed market-by-price fixture yet** — capturing one needs a live multicast
//! tunnel (see `fixtures/PROVENANCE.md`). Until there is, this file holds the one ground-truth-ish
//! check available: the three message types market-by-price inherits from **top-of-book**, whose
//! offsets are byte-validated against the reference Go decoder. Decoding the same bytes through
//! both codecs and requiring equal fields makes "these layouts are shared" self-enforcing rather
//! than eyeballed. The price-keyed types are pinned only by the offset-independent unit tests in
//! `codec_mbp.rs` plus field-for-field parity with `go/marketbyprice-parser`.

use doublezero_edge_connect::ingest::{codec, codec_mbp};

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

/// `InstrumentDefinition`, `Trade` and `ManifestSummary` are documented as byte-identical to the
/// **byte-validated** TOB `codec.rs`. If a future edit drifts either decoder's offsets, this fails
/// instead of the two silently diverging — the only backstop these three have while no
/// market-by-price capture exists.
#[test]
fn tob_shared_layouts_decode_identically() {
    // InstrumentDefinition (tag 0x02 in both): instrument_id@0, symbol@4(16), pexp@37, qexp@38, seq@74.
    let mut def = vec![0u8; 76];
    def[0..4].copy_from_slice(&41u32.to_le_bytes());
    def[4..15].copy_from_slice(b"KXBTCPERP\0\0");
    def[37] = (-4i8) as u8;
    def[38] = (-2i8) as u8;
    def[74..76].copy_from_slice(&9u16.to_le_bytes());
    let tob = codec::decode_frame(&one_msg_frame(codec::MAGIC, 0x02, 80, &def))
        .unwrap()
        .1;
    let mbp = codec_mbp::decode_frame(&one_msg_frame(codec_mbp::MAGIC, 0x02, 80, &def))
        .unwrap()
        .1;
    match (&tob[0], &mbp[0]) {
        (codec::Message::InstrumentDefinition(t), codec_mbp::Message::InstrumentDefinition(m)) => {
            assert_eq!(t.instrument_id, m.instrument_id);
            assert_eq!(t.symbol, m.symbol);
            assert_eq!(t.price_exponent, m.price_exponent);
            assert_eq!(t.qty_exponent, m.qty_exponent);
            assert_eq!(t.manifest_seq, m.manifest_seq);
            assert_eq!(m.symbol.as_ref(), "KXBTCPERP");
        }
        other => panic!("expected InstrumentDefinition from both, got {other:?}"),
    }

    // Trade (tag 0x04 in both). `trade_id = 0` is the FIX-sourced sentinel, carried verbatim.
    let mut tr = vec![0u8; 48];
    tr[0..4].copy_from_slice(&41u32.to_le_bytes());
    tr[4..6].copy_from_slice(&3u16.to_le_bytes());
    tr[6] = codec_mbp::AGGRESSOR_SELL;
    tr[8..16].copy_from_slice(&1_780u64.to_le_bytes()); // source_ts
    tr[16..24].copy_from_slice(&18_420i64.to_le_bytes()); // price
    tr[24..32].copy_from_slice(&1_500u64.to_le_bytes()); // qty
    tr[32..40].copy_from_slice(&0u64.to_le_bytes()); // trade_id
    tr[40..48].copy_from_slice(&5u64.to_le_bytes()); // cumulative_volume
    let tob = codec::decode_frame(&one_msg_frame(codec::MAGIC, 0x04, 52, &tr))
        .unwrap()
        .1;
    let mbp = codec_mbp::decode_frame(&one_msg_frame(codec_mbp::MAGIC, 0x04, 52, &tr))
        .unwrap()
        .1;
    match (&tob[0], &mbp[0]) {
        (codec::Message::Trade(t), codec_mbp::Message::Trade(m)) => {
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
    let mbp = codec_mbp::decode_frame(&one_msg_frame(codec_mbp::MAGIC, 0x07, 24, &ms))
        .unwrap()
        .1;
    match (&tob[0], &mbp[0]) {
        (codec::Message::ManifestSummary(t), codec_mbp::Message::ManifestSummary(m)) => {
            assert_eq!(t.channel_id, m.channel_id);
            assert_eq!(t.valid, m.valid);
            assert_eq!(t.manifest_seq, m.manifest_seq);
            assert_eq!(t.instrument_count, m.instrument_count);
            assert_eq!(t.ts, m.ts);
        }
        other => panic!("expected ManifestSummary from both, got {other:?}"),
    }
}
