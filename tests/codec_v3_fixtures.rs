//! Evidence that schema v3's widened `InstrumentDefinition` retires a real collision risk: a
//! reviewer flagged that trade dedup keyed on `(venue, symbol)` could silently drop a second
//! market's fills, because schema v1's 16-byte `Symbol` field truncates and two different
//! instrument ids can share a truncated symbol (recorded in `tests/fixtures/PROVENANCE.md`,
//! "known deviations" item 2 — `EAVE-27JAN01-YES` was the truncation of ids 1165 and 1403). Schema
//! v3 widens `Symbol` to 64 bytes; these fixtures are real mainnet captures on that schema
//! (`tests/fixtures/{tob_v3,mbp_perps_v3,sports_v3}.refdata.bin`, cut 2026-08-11, see PROVENANCE's
//! "Schema v3 reference data" section) cut specifically to prove the collision cannot occur there.
//!
//! **Framing differs from every other fixture here.** The rest of `tests/fixtures/*.bin` are
//! `[u32 LE len][frame]` records (`common::replay::split_frames`). These three are raw frames
//! packed back-to-back with no length prefix — the boundary between frames is each frame's own
//! `frame_length` header field (offset 22, u16 LE), so they need their own splitter below.

mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use doublezero_edge_connect::ingest::{codec, codec_common::FrameHeader, codec_mbp};

/// Split a v3 refdata fixture into raw frames using each frame's own `frame_length`, not an
/// external length prefix — see the module doc for why this differs from `replay::split_frames`.
fn split_by_header(bytes: &[u8], magic: u16) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        assert!(
            off + 24 <= bytes.len(),
            "fixture truncated: expected a 24-byte header at offset {off}, only {} bytes remain",
            bytes.len() - off
        );
        let got_magic = u16::from_le_bytes([bytes[off], bytes[off + 1]]);
        assert_eq!(
            got_magic, magic,
            "frame at offset {off}: magic 0x{got_magic:04X} != 0x{magic:04X}"
        );
        let frame_len = u16::from_le_bytes([bytes[off + 22], bytes[off + 23]]) as usize;
        assert!(
            frame_len >= 24 && off + frame_len <= bytes.len(),
            "frame at offset {off}: bad frame_length {frame_len} (remaining {})",
            bytes.len() - off
        );
        frames.push(bytes[off..off + frame_len].to_vec());
        off += frame_len;
    }
    frames
}

fn read_fixture(path: &str) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn tob_frames(path: &str) -> Vec<(FrameHeader, Vec<codec::Message>)> {
    split_by_header(&read_fixture(path), codec::MAGIC)
        .iter()
        .map(|f| {
            codec::decode_frame(f)
                .unwrap_or_else(|e| panic!("{path}: real captured frame failed to decode: {e}"))
        })
        .collect()
}

fn mbp_frames(path: &str) -> Vec<(FrameHeader, Vec<codec_mbp::Message>)> {
    split_by_header(&read_fixture(path), codec_mbp::MAGIC)
        .iter()
        .map(|f| {
            codec_mbp::decode_frame(f)
                .unwrap_or_else(|e| panic!("{path}: real captured frame failed to decode: {e}"))
        })
        .collect()
}

const TOB_V3: &str = "tests/fixtures/tob_v3.refdata.bin";
const MBP_PERPS_V3: &str = "tests/fixtures/mbp_perps_v3.refdata.bin";
const SPORTS_V3: &str = "tests/fixtures/sports_v3.refdata.bin";

/// Claim 1: every frame in every v3 fixture declares `schema_version == 3`, and the decoder really
/// took the v3 `InstrumentDefinition` branch rather than merely reading a `3` off the header — the
/// v3 branch is the only one that populates `source_id` (`codec_common::instrument_definition`), so
/// a definition's `source_id.is_some()` is direct evidence the v3 path executed rather than
/// something a header-only check could be fooled by.
#[test]
fn every_frame_decodes_as_schema_v3_and_takes_the_v3_path() {
    let tob = tob_frames(TOB_V3);
    assert!(!tob.is_empty(), "{TOB_V3}: fixture carried no frames");
    for (h, _) in &tob {
        assert_eq!(
            h.schema_version, 3,
            "{TOB_V3}: frame declared schema_version {} instead of 3",
            h.schema_version
        );
    }
    let tob_took_v3_path = tob.iter().any(|(_, msgs)| {
        msgs.iter()
            .any(|m| matches!(m, codec::Message::InstrumentDefinition(d) if d.source_id.is_some()))
    });
    assert!(
        tob_took_v3_path,
        "{TOB_V3}: no InstrumentDefinition carried a source_id, so nothing proves the v3 branch ran"
    );

    for path in [MBP_PERPS_V3, SPORTS_V3] {
        let frames = mbp_frames(path);
        assert!(!frames.is_empty(), "{path}: fixture carried no frames");
        for (h, _) in &frames {
            assert_eq!(
                h.schema_version, 3,
                "{path}: frame declared schema_version {} instead of 3",
                h.schema_version
            );
        }
        let took_v3_path = frames.iter().any(|(_, msgs)| {
            msgs.iter().any(
                |m| matches!(m, codec_mbp::Message::InstrumentDefinition(d) if d.source_id.is_some()),
            )
        });
        assert!(
            took_v3_path,
            "{path}: no InstrumentDefinition carried a source_id, so nothing proves the v3 branch ran"
        );
    }
}

/// One decoded `InstrumentDefinition`, tagged with the frame's `channel_id` (the per-arm identity
/// on these captures).
struct Def {
    channel_id: u8,
    instrument_id: u32,
    symbol: Arc<str>,
}

fn tob_defs(path: &str) -> Vec<Def> {
    tob_frames(path)
        .into_iter()
        .flat_map(|(h, msgs)| {
            msgs.into_iter().filter_map(move |m| match m {
                codec::Message::InstrumentDefinition(d) => Some(Def {
                    channel_id: h.channel_id,
                    instrument_id: d.instrument_id,
                    symbol: d.symbol,
                }),
                _ => None,
            })
        })
        .collect()
}

fn mbp_defs(path: &str) -> Vec<Def> {
    mbp_frames(path)
        .into_iter()
        .flat_map(|(h, msgs)| {
            msgs.into_iter().filter_map(move |m| match m {
                codec_mbp::Message::InstrumentDefinition(d) => Some(Def {
                    channel_id: h.channel_id,
                    instrument_id: d.instrument_id,
                    symbol: d.symbol,
                }),
                _ => None,
            })
        })
        .collect()
}

/// Claim 2: schema v3 really carries symbols that overflow the old 16-byte field, decoded whole.
///
/// A decoder that (by bug) fell back to the 16-byte `DEF_SYM_LEN_V1` width could only ever produce
/// a symbol of at most 16 bytes: a NUL inside that window truncates further, but nothing can make
/// the *decoded* string longer than the window itself. So `symbol.len() > 16` is not incidental —
/// it is only possible if the widened 64-byte field was actually read. `tob_v3`/`mbp_perps_v3`
/// carry short perps tickers (max observed 11 bytes) that never approach the old limit, so they
/// cannot demonstrate this; `sports_v3`'s event-market symbols are what the widened field was cut
/// to prove, so this test is scoped there.
#[test]
fn sports_v3_symbols_exceed_the_v1_field_width() {
    let defs = mbp_defs(SPORTS_V3);
    assert!(!defs.is_empty(), "{SPORTS_V3}: no InstrumentDefinitions");

    let longest = defs
        .iter()
        .max_by_key(|d| d.symbol.len())
        .expect("non-empty defs");
    assert!(
        longest.symbol.len() > 16,
        "{SPORTS_V3}: longest symbol {:?} (id {}) is only {} bytes, at or under the v1 field width \
         — does not demonstrate the widened field is actually read",
        longest.symbol,
        longest.instrument_id,
        longest.symbol.len()
    );

    let over_16: Vec<&Def> = defs.iter().filter(|d| d.symbol.len() > 16).collect();
    assert!(
        !over_16.is_empty(),
        "{SPORTS_V3}: no symbol exceeded 16 bytes"
    );
    for d in &over_16 {
        assert!(
            d.symbol.chars().all(|c| c.is_ascii_graphic()),
            "{SPORTS_V3}: symbol {:?} (id {}) decoded past 16 bytes but is not clean ASCII — \
             looks like it read past the real field into padding or a neighboring message",
            d.symbol,
            d.instrument_id
        );
    }
}

/// Claim 3: no two distinct instrument ids share a symbol, within any one fixture. This is the
/// direct refutation of the collision the dedup-key reviewer flagged. On a collision, name the
/// colliding pair so a future capture that reintroduces truncation is diagnosable rather than just
/// "test failed".
fn assert_no_symbol_collision(path: &str, defs: &[Def]) {
    let mut by_symbol: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
    for d in defs {
        by_symbol
            .entry(&d.symbol)
            .or_default()
            .insert(d.instrument_id);
    }
    let colliding: Vec<(&str, &BTreeSet<u32>)> = by_symbol
        .iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|(s, ids)| (*s, ids))
        .collect();
    assert!(
        colliding.is_empty(),
        "{path}: symbol(s) shared by more than one instrument id — the dedup-key collision this \
         schema is supposed to retire: {colliding:?}"
    );
}

#[test]
fn no_two_distinct_instrument_ids_share_a_symbol() {
    assert_no_symbol_collision(TOB_V3, &tob_defs(TOB_V3));
    assert_no_symbol_collision(MBP_PERPS_V3, &mbp_defs(MBP_PERPS_V3));
    assert_no_symbol_collision(SPORTS_V3, &mbp_defs(SPORTS_V3));
}

/// The strong form of claim 3: `sports_v3` holds several groups of distinct instrument ids whose
/// symbols share a common 16-byte prefix — exactly the shape that collided under schema v1's
/// truncated field (see PROVENANCE's `EAVE-27JAN01-YES` case). Find such a group and assert the
/// *full* symbols still distinguish every id in it, rather than merely asserting no collision in
/// the abstract.
#[test]
fn sports_v3_ids_that_would_collide_under_v1_truncation_do_not_under_v3() {
    let defs = mbp_defs(SPORTS_V3);
    let mut by_prefix: BTreeMap<&str, BTreeMap<u32, &str>> = BTreeMap::new();
    for d in &defs {
        let prefix = if d.symbol.len() > 16 {
            &d.symbol[..16]
        } else {
            &d.symbol[..]
        };
        by_prefix
            .entry(prefix)
            .or_default()
            .insert(d.instrument_id, &d.symbol);
    }
    let would_collide_under_v1: Vec<(&str, &BTreeMap<u32, &str>)> = by_prefix
        .iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|(p, ids)| (*p, ids))
        .collect();
    assert!(
        !would_collide_under_v1.is_empty(),
        "{SPORTS_V3}: no group of distinct instrument ids shares a 16-byte symbol prefix, so this \
         fixture cannot demonstrate the v1 collision would-be scenario"
    );

    for (prefix, ids) in &would_collide_under_v1 {
        let full_symbols: BTreeSet<&str> = ids.values().copied().collect();
        assert_eq!(
            full_symbols.len(),
            ids.len(),
            "{SPORTS_V3}: prefix {prefix:?} covers {} distinct instrument ids {:?} but only {} \
             distinct full symbols — two ids collapsed to the same full symbol",
            ids.len(),
            ids,
            full_symbols.len()
        );
    }
}

/// Claim 4: both perps publisher arms are represented in a simultaneous capture — unlike the older
/// (pre-v3) perps fixtures, whose two arms were captured at different times and so could never be
/// combined into one interleaved fixture (PROVENANCE: "the two arms' captures do not overlap in
/// time"). The arms stamp different frame-header `channel_id`s on this feed (a repurposing of that
/// field PROVENANCE already records for the retiring publisher), so distinct `channel_id`s standing
/// for the same instrument set is the observable proxy for "both sources present".
#[test]
fn both_perps_publisher_arms_are_present() {
    for (path, defs) in [
        (TOB_V3, tob_defs(TOB_V3)),
        (MBP_PERPS_V3, mbp_defs(MBP_PERPS_V3)),
    ] {
        let channels: BTreeSet<u8> = defs.iter().map(|d| d.channel_id).collect();
        assert!(
            channels.len() >= 2,
            "{path}: InstrumentDefinitions carried only channel_id(s) {channels:?} — expected at \
             least 2, one per simultaneously-captured publisher arm"
        );

        // The two arms mirror one instrument universe (PROVENANCE), so the same instrument ids
        // should reappear under a second channel_id — not just an unrelated stray channel byte.
        let mut ids_by_channel: BTreeMap<u8, BTreeSet<u32>> = BTreeMap::new();
        for d in &defs {
            ids_by_channel
                .entry(d.channel_id)
                .or_default()
                .insert(d.instrument_id);
        }
        let mut channel_iter = ids_by_channel.values();
        let first = channel_iter
            .next()
            .expect("at least 2 channels asserted above");
        let shared_with_another = channel_iter.any(|ids| !first.is_disjoint(ids));
        assert!(
            shared_with_another,
            "{path}: no two channel_ids shared any instrument id — does not look like mirrored \
             publisher arms of one universe"
        );
    }
}
