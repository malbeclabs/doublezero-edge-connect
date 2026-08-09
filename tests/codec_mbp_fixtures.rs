//! Empirical validation of the market-by-price decoder against **real captured frames**, plus the
//! cross-codec pinning of the types it inherits from top-of-book.
//!
//! Two fixture sets, both real captures of the Lashay publisher (`tests/fixtures/mbp*.bin`, see
//! `fixtures/PROVENANCE.md`), because they cover different things:
//!
//! * `mbp_*` — the **sharded** feed: three `Channel ID`s on one group, so it is the only sample that
//!   exercises per-channel snapshot grouping. Thin delta stream.
//! * `mbp_perps_*` — the **dense** feed: one channel, thousands of contiguous per-instrument deltas,
//!   which is what pins sequence handling. From the older publisher (see PROVENANCE).
//!
//! The assertions are deliberately **invariants, not recorded counts** — zero decode errors, a
//! snapshot group whose levels equal its promised `total_levels`, a dense delta run, a `depth_bound`
//! the publisher actually stated. A richer capture is expected to replace these fixtures, and it
//! should drop in without editing a single number here.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::replay;
use doublezero_edge_connect::ingest::{codec, codec_mbp};

/// Read one fixture's frames, asserting each carries the market-by-price magic.
fn frames(path: &str) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    replay::split_frames(&bytes, codec_mbp::MAGIC)
}

/// Every message of every frame in a fixture set's three port roles, paired with its frame's
/// `channel_id` — the key the spec scopes a snapshot group to.
fn decode_all(prefix: &str) -> Vec<(u8, codec_mbp::Message)> {
    let mut out = Vec::new();
    for role in ["refdata", "snapshot", "mktdata"] {
        for frame in frames(&format!("tests/fixtures/{prefix}.{role}.bin")) {
            let (h, msgs) = codec_mbp::decode_frame(&frame).unwrap_or_else(|e| {
                panic!("{prefix}.{role}: real captured frame failed to decode: {e}")
            });
            out.extend(msgs.into_iter().map(|m| (h.channel_id, m)));
        }
    }
    out
}

/// **The assertion the exact-length discipline lives or dies on.** Every message of every real frame
/// must decode to a known variant: `Message::Other` here means either a body length that disagrees
/// with the type's declared size, or a type this decoder does not implement. Neither is acceptable on
/// a live feed we intend to ingest, and a decode *error* would mean the frame walk itself broke.
#[test]
fn every_real_frame_decodes_with_no_unroutable_message() {
    for prefix in ["mbp", "mbp_perps"] {
        let msgs = decode_all(prefix);
        assert!(!msgs.is_empty(), "{prefix}: fixture carried no messages");
        let unrouted: BTreeSet<u8> = msgs
            .iter()
            .filter_map(|(_, m)| match m {
                codec_mbp::Message::Other(ty) => Some(*ty),
                _ => None,
            })
            .collect();
        assert!(
            unrouted.is_empty(),
            "{prefix}: type bytes decoded to Other (bad length, or unimplemented): {unrouted:?}"
        );
    }
}

/// A snapshot group's `SnapshotLevel` count must equal the `total_levels` its `SnapshotBegin`
/// promised — the check that catches a wrong offset in either field, and the one `PriceBook` refuses
/// to install a snapshot without. Grouped **per `Channel ID`**: a channel is an independent state
/// machine with its own snapshot cycle and one port may carry several, so a port-wide tally would
/// mis-attribute every level of an interleaved channel.
#[test]
fn snapshot_groups_carry_exactly_the_levels_they_promise() {
    for prefix in ["mbp", "mbp_perps"] {
        let mut open: BTreeMap<u8, (u32, u32, u32)> = BTreeMap::new(); // chan -> (id, total, seen)
        let mut complete = 0usize;
        for (chan, m) in decode_all(prefix) {
            match m {
                codec_mbp::Message::SnapshotBegin(s) => {
                    open.insert(chan, (s.snapshot_id, s.total_levels, 0));
                }
                codec_mbp::Message::SnapshotLevel(l) => {
                    if let Some((id, _, seen)) = open.get_mut(&chan) {
                        if *id == l.snapshot_id {
                            *seen += 1;
                        }
                    }
                }
                codec_mbp::Message::SnapshotEnd(e) => {
                    // A capture can start mid-group, so only judge groups whose begin we saw.
                    if let Some((id, total, seen)) = open.remove(&chan) {
                        assert_eq!(id, e.snapshot_id, "{prefix}: end closed a different group");
                        assert_eq!(
                            seen, total,
                            "{prefix} chan {chan} snapshot {id}: {seen} levels for a promised {total}"
                        );
                        assert!(total > 0, "{prefix}: an empty snapshot proves nothing");
                        complete += 1;
                    }
                }
                _ => {}
            }
        }
        assert!(
            complete > 0,
            "{prefix}: no complete snapshot group — the fixture cannot anchor a book"
        );
    }
}

/// The per-instrument delta sequence must be **dense** over a real capture: gapless and duplicate-free
/// for at least one instrument. `PriceBook` treats `> last + 1` as a gap and stops applying, so a
/// decoder reading `per_instrument_seq` from the wrong offset would show up here as a shredded run.
#[test]
fn a_real_instruments_delta_sequence_is_dense() {
    for prefix in ["mbp", "mbp_perps"] {
        let mut seqs: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for (_, m) in decode_all(prefix) {
            let (id, seq) = match m {
                codec_mbp::Message::LevelUpdate(u) => (u.instrument_id, u.per_instrument_seq),
                codec_mbp::Message::BookClear(c) => (c.instrument_id, c.per_instrument_seq),
                _ => continue,
            };
            seqs.entry(id).or_default().push(seq);
        }
        let dense = seqs
            .values()
            .filter(|s| s.len() >= 20)
            // `checked_add`, not `+ 1`: a wrong offset yields garbage sequences, and this must fail
            // with the assertion's message rather than an overflow panic.
            .filter(|s| s.windows(2).all(|w| w[0].checked_add(1) == Some(w[1])))
            .count();
        assert!(
            dense > 0,
            "{prefix}: no instrument had a gapless, in-order run of 20+ deltas; runs were {:?}",
            seqs.iter().map(|(i, s)| (i, s.len())).collect::<Vec<_>>()
        );
    }
}

/// `depth_bound` is only meaningful if it came off the wire, so require the publisher to have stated
/// one. `0` is its positive claim of a *complete* book — the value whose accidental synthesis the
/// decoder's exact-length rule exists to prevent — and it is what both captured feeds send.
#[test]
fn publishers_state_a_depth_bound_and_claim_complete_books() {
    for prefix in ["mbp", "mbp_perps"] {
        let bounds: BTreeSet<u32> = decode_all(prefix)
            .iter()
            .filter_map(|(_, m)| match m {
                codec_mbp::Message::SnapshotBegin(s) => Some(s.depth_bound),
                _ => None,
            })
            .collect();
        assert!(
            !bounds.is_empty(),
            "{prefix}: no SnapshotBegin to read a bound from"
        );
        assert!(
            bounds.contains(&0),
            "{prefix}: expected a complete-book claim (depth_bound == 0), saw {bounds:?}"
        );
    }
}

/// Field sanity across every real message, the counterpart to the offset tests: a wrong offset that a
/// symmetric round-trip cannot catch shows up here as an out-of-range side byte, a level quantity on
/// an instrument that was never defined, or a garbled symbol.
#[test]
fn real_fields_are_in_range_and_instruments_are_defined() {
    for prefix in ["mbp", "mbp_perps"] {
        let msgs = decode_all(prefix);
        let mut defined = BTreeSet::new();
        for (_, m) in &msgs {
            if let codec_mbp::Message::InstrumentDefinition(d) = m {
                assert!(!d.symbol.is_empty(), "{prefix}: empty symbol");
                assert!(
                    d.symbol.chars().all(|c| c.is_ascii_graphic()),
                    "{prefix}: garbled symbol {:?}",
                    d.symbol
                );
                assert!(
                    (-18..=0).contains(&d.price_exponent) && (-18..=0).contains(&d.qty_exponent),
                    "{prefix}: implausible exponents on {}",
                    d.symbol
                );
                defined.insert(d.instrument_id);
            }
        }
        assert!(!defined.is_empty(), "{prefix}: no instrument definitions");
        for (_, m) in &msgs {
            match m {
                codec_mbp::Message::LevelUpdate(u) => {
                    assert!(
                        u.side == codec_mbp::SIDE_BID || u.side == codec_mbp::SIDE_ASK,
                        "{prefix}: level side byte {} is neither bid nor ask",
                        u.side
                    );
                    assert!(
                        defined.contains(&u.instrument_id),
                        "{prefix}: level for undefined instrument {}",
                        u.instrument_id
                    );
                }
                codec_mbp::Message::SnapshotLevel(l) => {
                    assert!(
                        l.side == codec_mbp::SIDE_BID || l.side == codec_mbp::SIDE_ASK,
                        "{prefix}: snapshot side byte {} is neither bid nor ask",
                        l.side
                    );
                    assert!(l.qty_raw > 0, "{prefix}: a snapshot level with no quantity");
                }
                codec_mbp::Message::Trade(t) => {
                    assert!(t.trade_price_raw > 0, "{prefix}: non-positive trade price");
                    assert!(t.trade_qty_raw > 0, "{prefix}: zero-size trade");
                }
                _ => {}
            }
        }
    }
}

/// The sharded fixture is the only one that proves this decoder handles more than one channel, which
/// is the deployment shape the sports feed actually uses. Pinned so a future re-capture that
/// collapsed to a single channel is a visible loss of coverage rather than a silent one.
#[test]
fn the_sharded_fixture_really_carries_several_channels() {
    let chans: BTreeSet<u8> = decode_all("mbp").into_iter().map(|(c, _)| c).collect();
    assert!(
        chans.len() > 1,
        "mbp fixture should span several Channel IDs, saw {chans:?}"
    );
}

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
