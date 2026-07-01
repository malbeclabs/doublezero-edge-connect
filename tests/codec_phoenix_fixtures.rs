//! Real-frame validation of the committed **Phoenix** TOB fixtures — a clean Phoenix-only slice
//! (publisher `148.51.122.75`, `source_id=2`) of a concurrent edge+public capture (2026-06-30; see
//! `fixtures/PROVENANCE.md`) decoded through the real `codec::decode_frame`.
//!
//! This is the ground truth behind the Phoenix public-trade backstop (`ingest::phoenix_feeder`,
//! #53): the same capture proved the dedup key — the public `tradeSequenceNumber` equals the edge
//! on-chain `trade_id` on 257/257 shared fills, and Phoenix names each market with the same bare
//! ticker on both feeds (edge `instrument_id == public assetId`). The exact `(instrument_id ->
//! symbol)` and `(instrument_id, trade_id)` pairs pinned here are the edge side of that match, so a
//! regression in the TOB codec or a re-captured fixture that broke those invariants would fail here.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::replay;
use doublezero_edge_connect::ingest::codec::{self, Message};

fn frames(path: &str) -> Vec<Vec<u8>> {
    replay::split_frames(
        &std::fs::read(path).expect("read fixture"),
        replay::TOB_MAGIC,
    )
}

fn definitions() -> BTreeMap<u32, codec::InstrumentDefinition> {
    let mut defs = BTreeMap::new();
    for f in frames("tests/fixtures/phoenix_tob_refdata.bin") {
        for m in codec::decode_frame(&f).expect("decode refdata frame").1 {
            if let Message::InstrumentDefinition(d) = m {
                defs.insert(d.instrument_id, d);
            }
        }
    }
    defs
}

/// The refdata fixture is a single complete manifest epoch (`seq=11`, 51 instruments). The replay
/// relies on the `ManifestSummary` preceding the definitions — the subscriber drops a definition
/// whose `manifest_seq` doesn't match the latest manifest, so a def-before-manifest fixture would
/// silently define nothing. Pin that the first frame leads with the manifest.
#[test]
fn phoenix_refdata_fixture_is_a_complete_manifest_epoch() {
    let fs = frames("tests/fixtures/phoenix_tob_refdata.bin");
    assert!(!fs.is_empty(), "refdata fixture is empty");
    match codec::decode_frame(&fs[0])
        .expect("decode frame 0")
        .1
        .first()
    {
        Some(Message::ManifestSummary(m)) => {
            assert!(m.valid, "manifest must be valid");
            assert_eq!(m.manifest_seq, 11);
            assert_eq!(m.instrument_count, 51);
        }
        other => panic!("refdata fixture must lead with a ManifestSummary, got {other:?}"),
    }

    let defs = definitions();
    assert_eq!(
        defs.len(),
        51,
        "the manifest declares 51 instruments; all must be defined (so RefDataState reaches ready())"
    );
    for d in defs.values() {
        assert_eq!(d.manifest_seq, 11, "every def shares the manifest epoch");
        assert!(
            !d.symbol.is_empty() && d.symbol.is_ascii(),
            "instrument {} has an implausible symbol {:?}",
            d.instrument_id,
            d.symbol
        );
        // Observed Phoenix exponents: price in [-8,-2], qty in [-4,2]. Bound generously but finitely.
        assert!(
            (-10..=0).contains(&d.price_exponent),
            "{} price_exponent {} out of range",
            d.symbol,
            d.price_exponent
        );
        assert!(
            (-8..=4).contains(&d.qty_exponent),
            "{} qty_exponent {} out of range",
            d.symbol,
            d.qty_exponent
        );
    }

    // Phoenix edge `instrument_id` equals the public `assetId`, and the symbol is the same bare
    // ticker on both feeds (no namespace, no `-PERP`). Pin a few exact mappings.
    let sym = |id: u32| defs.get(&id).map(|d| d.symbol.as_str());
    assert_eq!(sym(0), Some("SOL"));
    assert_eq!(sym(1), Some("BTC"));
    assert_eq!(sym(2), Some("ETH"));
    assert_eq!(sym(45), Some("AMD"));
    assert_eq!(sym(50), Some("AMZN"));
}

/// Every fixture trade is a real Phoenix (`source_id=2`) fill referencing a defined instrument, and
/// carries the on-chain `trade_id` that the public feed reported as `tradeSequenceNumber` for the
/// same fill. Pinning the exact `(instrument_id, trade_id)` pairs anchors the dedup key the
/// `phoenix_feeder` relies on.
#[test]
fn phoenix_mktdata_fixture_trades_validate() {
    let defined: BTreeSet<u32> = definitions().into_keys().collect();
    let mut by_id: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for f in frames("tests/fixtures/phoenix_tob_marketdata.bin") {
        for m in codec::decode_frame(&f).expect("decode mktdata frame").1 {
            if let Message::Trade(t) = m {
                assert_eq!(
                    t.source_id, 2,
                    "every fixture trade must be Phoenix (source_id 2)"
                );
                assert_eq!(codec::source_name(t.source_id), Some("Phoenix"));
                assert!(
                    t.trade_id > 0,
                    "trade_id (= public tradeSequenceNumber) must be real"
                );
                assert!(
                    defined.contains(&t.instrument_id),
                    "trade references undefined instrument {}",
                    t.instrument_id
                );
                assert!(
                    matches!(t.aggressor_side, 1 | 2),
                    "aggressor side {} is neither buy(1) nor sell(2)",
                    t.aggressor_side
                );
                by_id.entry(t.instrument_id).or_default().push(t.trade_id);
            }
        }
    }
    assert!(
        by_id.values().map(Vec::len).sum::<usize>() >= 8,
        "fixture should carry the captured Phoenix trades"
    );
    // Exact on-chain trade sequence numbers, equal to the public feed's tradeSequenceNumber for the
    // same fills (capture verification #1). BTC's 869424 was observed verbatim on the public side.
    assert!(
        by_id[&1].contains(&869424),
        "BTC (id 1) carries trade_id 869424"
    );
    assert!(
        by_id[&0].contains(&1188189),
        "SOL (id 0) carries trade_id 1188189"
    );
    assert!(
        by_id[&45].contains(&20418),
        "AMD (id 45) carries trade_id 20418"
    );
}
