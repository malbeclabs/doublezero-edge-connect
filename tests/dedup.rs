//! Two-publisher integration test: feed the combined dual-publisher TOB fixture through
//! `TobProcessor` in capture order and assert the dedup contract holds across publishers. This is
//! the cross-publisher counterpart to `tob_single_publisher_contract` in `e2e.rs`: the fixture
//! carries two independent publishers mirroring the same Hyperliquid feed. Quotes dedup by a
//! per-(venue, symbol) `source_ts` latch-to-leader floor: within one `source_ts` tick only the
//! leader (first publisher to open it) is emitted — a slower publisher's samples at the same tick
//! arrive in a delay-corrupted order and are dropped — and a strictly-older BBO (stale laggard) plus
//! the leader's exact `(source_ts, content)` repeats are dropped too. So the emitted `source_ts` is
//! non-decreasing — not strictly increasing — per symbol, and within a tick the series is one
//! publisher's coherent subsequence.

mod common;

use common::{assertions, replay as replay_helper};
use doublezero_edge_connect::ingest::{
    arbiter::{Arbiter, SharedArbiter, TRADE_DEDUP_WINDOW},
    codec,
    processor::TobProcessor,
    receiver::{FrameCtx, FrameProcessor, PortRole},
};
use serde_json::Value;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

/// Combined fixture record: `[u32 len LE][4B src_ip octets][1B role: 0=refdata,1=mktdata][frame]`.
fn read_combined(path: &str) -> Vec<(IpAddr, u8, Vec<u8>)> {
    let b = std::fs::read(path).unwrap();
    let mut out = Vec::new();
    let mut o = 0;
    while o < b.len() {
        let len = u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]) as usize;
        o += 4;
        let ip = IpAddr::V4(Ipv4Addr::new(b[o], b[o + 1], b[o + 2], b[o + 3]));
        o += 4;
        let role = b[o];
        o += 1;
        out.push((ip, role, b[o..o + len].to_vec()));
        o += len;
    }
    out
}

/// Replay combined records through a single `TobProcessor` feeding the shared `Arbiter` in capture
/// order and collect the emitted WS messages as JSON. This is the production demux+dedup path: each
/// record's source IP becomes `FrameCtx.publisher`, so the per-publisher SeqTracker runs in the
/// processor and the cross-publisher latch-to-leader floor + trade dedup run in the arbiter, exactly
/// as in the binary (where the arbiter is the one process-wide emit stage).
fn replay(recs: &[(IpAddr, u8, Vec<u8>)]) -> Vec<Value> {
    let (tx, mut rx) = broadcast::channel(1 << 16);
    let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, TRADE_DEDUP_WINDOW)));
    let instruments = Arc::new(Mutex::new(HashMap::new()));
    let mut p = TobProcessor::new(true);
    for (ip, role, frame) in recs {
        let ctx = FrameCtx {
            venue: "Hyperliquid",
            arbiter: &arbiter,
            instruments: &instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role: if *role == 0 {
                PortRole::Refdata
            } else {
                PortRole::Mktdata
            },
            publisher: *ip,
        };
        p.on_datagram(frame, &ctx);
    }
    let mut msgs = Vec::new();
    while let Ok(m) = rx.try_recv() {
        msgs.push(serde_json::to_value(&m).unwrap());
    }
    msgs
}

/// Decode every refdata frame's instrument definitions into `instrument_id -> symbol`. Built from
/// all definitions in the fixture so per-symbol counts can be keyed by the human symbol.
fn symbol_by_id(recs: &[(IpAddr, u8, Vec<u8>)]) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    for (_ip, _role, frame) in recs {
        if let Ok((_h, msgs)) = codec::decode_frame(frame) {
            for m in &msgs {
                if let codec::Message::InstrumentDefinition(d) = m {
                    map.insert(d.instrument_id, d.symbol.to_string());
                }
            }
        }
    }
    map
}

/// Raw (pre-dedup) quote-message count per symbol across the mktdata frames — the baseline the
/// emitted counts must drop below for dedup to have done anything.
fn raw_quotes_by_symbol(recs: &[(IpAddr, u8, Vec<u8>)]) -> HashMap<String, usize> {
    let by_id = symbol_by_id(recs);
    let mut counts: HashMap<String, usize> = HashMap::new();
    for (_ip, role, frame) in recs {
        if *role != 1 {
            continue; // mktdata only
        }
        if let Ok((_h, msgs)) = codec::decode_frame(frame) {
            for m in &msgs {
                if let codec::Message::Quote(q) = m {
                    if let Some(sym) = by_id.get(&q.instrument_id) {
                        *counts.entry(sym.clone()).or_default() += 1;
                    }
                }
            }
        }
    }
    counts
}

/// Emitted-quote count per symbol from collected WS messages.
fn emitted_quotes_by_symbol(msgs: &[Value]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for m in msgs.iter().filter(|m| m["type"] == "quote") {
        let sym = m["symbol"].as_str().unwrap_or_default().to_string();
        *counts.entry(sym).or_default() += 1;
    }
    counts
}

/// The set of emitted trade ids (a trade is uniquely identified by its venue `trade_id`), used to
/// prove a duplicated trade packet adds nothing to the wire.
fn emitted_trade_ids(msgs: &[Value]) -> std::collections::BTreeSet<u64> {
    msgs.iter()
        .filter(|m| m["type"] == "trade")
        .filter_map(|m| m["trade_id"].as_u64())
        .collect()
}

/// Assert that emitted quotes are non-decreasing in `source_ts_ns` per (venue, symbol). This is the
/// latch-to-leader floor contract: a lagging publisher's strictly-older BBO must never appear out of
/// order on the wire, but the leader's distinct BBO changes that share a `source_ts` (real intra-tick
/// updates) are kept, so the sequence is non-decreasing, NOT strictly increasing. Fails (proving the
/// assertion bites) if the floor is bypassed and an older quote is emitted.
fn assert_quote_source_ts_non_decreasing(msgs: &[Value]) {
    let mut last: HashMap<(String, String), u64> = HashMap::new();
    for m in msgs.iter().filter(|m| m["type"] == "quote") {
        let venue = m["venue"].as_str().unwrap_or_default().to_string();
        let symbol = m["symbol"].as_str().unwrap_or_default().to_string();
        let ts = m["source_ts_ns"].as_u64().expect("quote has source_ts_ns");
        if let Some(prev) = last.insert((venue.clone(), symbol.clone()), ts) {
            assert!(
                ts >= prev,
                "{venue}/{symbol}: emitted source_ts_ns went backwards ({prev} -> {ts})"
            );
        }
    }
}

#[test]
fn two_publishers_latch_to_leader_no_stale_or_dupes() {
    let recs = read_combined("tests/fixtures/tob_btc_dual.combined.bin");
    let msgs = replay(&recs);
    // No two emitted quotes share the oracle's business identity (exact duplicates dropped).
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);
    // Latch-to-leader floor: per (venue, symbol) the emitted source_ts is non-decreasing — a lagging
    // publisher's strictly-older BBO is never emitted, and within a tick only the leader's changes are.
    assert_quote_source_ts_non_decreasing(&msgs);

    let quotes = msgs.iter().filter(|m| m["type"] == "quote").count();
    // The fixture carries 8788 raw BTC mktdata quotes split across two publishers mirroring the same
    // feed (417 distinct source_ts). Latch-to-leader emits the leader's distinct canonical BBOs at a
    // non-decreasing floor — the `bbo_hash` identity (px, sz, bid_n, ask_n), so a count-only change at
    // an unchanged price/size is a distinct quote. Observed: 4540 (the 4468 px/sz-distinct BBOs plus
    // 72 count-only changes the source-count identity now keeps; ~1.6%). Far above a strict
    // one-per-tick watermark (~417, which over-drops real intra-tick changes).
    assert_eq!(
        quotes, 4540,
        "two-pub latch-to-leader quote count (leader's distinct canonical BBOs incl. bid_n/ask_n)"
    );
}

/// Per-`(venue, symbol)` dedup independence. The quote floor keys on `(venue, symbol)` with an
/// **independent staleness floor per symbol** (see `arbiter::StalenessFloor`), so a busy symbol's
/// volume must not perturb a quiet symbol's dedup. The single-symbol fixture above can't prove that;
/// this uses a three-symbol fixture (BTC busy, SOL medium, DOGE quiet) from the same two publishers
/// and asserts:
///   1. `no_business_duplicates` holds across ALL symbols at once (no cross-symbol key collision);
///   2. all three symbols emit quotes and each dedups (emitted < raw per symbol);
///   3. **independence**: the quiet symbol's emitted set is byte-for-byte what it produces when
///      replayed ALONE — i.e. stripping BTC/SOL from the input changes nothing for DOGE.
///
/// Falsifiability: with the quote floor bypassed (always-admit), `no_business_duplicates` and the
/// non-decreasing assertion both fail (stale/out-of-order copies re-emit) and emitted == raw, so
/// this test pins the dedup, not just the fixture.
#[test]
fn per_symbol_dedup_is_independent() {
    let recs = read_combined("tests/fixtures/tob_multi_dual.combined.bin");
    let msgs = replay(&recs);

    // (1) the dedup contract holds across the whole multi-symbol stream.
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);
    // Staleness-floor non-decreasing monotonicity holds per (venue, symbol) across all symbols.
    assert_quote_source_ts_non_decreasing(&msgs);

    let raw = raw_quotes_by_symbol(&recs);
    let emitted = emitted_quotes_by_symbol(&msgs);
    // Each symbol emits the leader's distinct intra-tick changes: well below raw (non-leader samples,
    // stale laggard replays, and exact dups dropped) but well above a strict one-per-tick watermark.
    // The exact per-symbol counts aren't pinned here (the single-symbol test above pins BTC); this
    // test pins the cross-symbol *independence* property below.

    // The fixture's three tiers (see PROVENANCE.md). Guard that the fixture still carries them so a
    // regenerated fixture that silently dropped a symbol fails loudly rather than vacuously passing.
    for sym in ["BTC", "SOL", "DOGE"] {
        let r = *raw.get(sym).unwrap_or(&0);
        let e = *emitted.get(sym).unwrap_or(&0);
        assert!(r > 0, "fixture carries no raw {sym} quotes");
        assert!(e > 0, "no {sym} quotes emitted");
        // (2) per-symbol dedup happened: two publishers mirror the feed, so emitted must drop below
        // raw for each symbol independently.
        assert!(e < r, "{sym} did not dedup: emitted {e} >= raw {r}");
    }

    // The volume spread that makes independence meaningful: BTC must dwarf DOGE, or "DOGE unaffected
    // by BTC volume" proves nothing.
    let (btc_raw, doge_raw) = (raw["BTC"], raw["DOGE"]);
    assert!(
        btc_raw > doge_raw * 5,
        "fixture volume spread too small: BTC raw {btc_raw} vs DOGE raw {doge_raw}"
    );

    // (3) Independence: replay ONLY DOGE's frames (all refdata kept so precision resolves; mktdata
    // restricted to DOGE) and confirm DOGE's emitted count is identical. DOGE has its own floor and
    // latched leader, so BTC/SOL traffic interleaved in the full run never advances or perturbs it;
    // the quiet symbol emits exactly the same set whether or not the busy symbols are present.
    let doge_id: u32 = *symbol_by_id(&recs)
        .iter()
        .find(|(_, s)| *s == "DOGE")
        .expect("DOGE definition in fixture")
        .0;
    let doge_only: Vec<_> = recs
        .iter()
        .filter(|(_ip, role, frame)| {
            *role == 0 || frame_carries(frame, doge_id) // keep all refdata + DOGE-bearing mktdata
        })
        .cloned()
        .collect();
    let doge_alone = emitted_quotes_by_symbol(&replay(&doge_only));

    assert_eq!(
        emitted.get("DOGE").copied(),
        doge_alone.get("DOGE").copied(),
        "DOGE emitted count changed when BTC/SOL were present ({:?} with vs {:?} alone) — \
         per-symbol windows are not independent",
        emitted.get("DOGE"),
        doge_alone.get("DOGE"),
    );
}

/// The literal duplicate-multicast-packet case for quotes: replay one publisher's stream, then
/// replay it again with **every mktdata frame delivered twice** (byte-for-byte, same frame
/// sequence — exactly what a redundant multicast delivery looks like). The emitted quote set must be
/// identical. The duplicate datagram is *not* rejected at the sequence gate — an equal sequence is
/// an accepted idempotent full-state update (`SeqTracker::duplicate_of_last_is_not_stale`) — so this
/// pins that the duplicate's decoded payload is collapsed by the arbiter's latch-to-leader floor.
#[test]
fn duplicate_multicast_quote_packet_collapses() {
    let recs = read_combined("tests/fixtures/tob_btc_dual.combined.bin");
    // Restrict mktdata to a single publisher so the baseline has no cross-publisher dedup; keep all
    // refdata so instrument definitions resolve.
    let pub_ip = recs
        .iter()
        .find(|(_ip, role, _)| *role == 1)
        .map(|(ip, _, _)| *ip)
        .expect("fixture has mktdata");
    let baseline: Vec<_> = recs
        .iter()
        .filter(|(ip, role, _)| *role == 0 || *ip == pub_ip)
        .cloned()
        .collect();

    // Variant: each mktdata datagram is delivered a second time, immediately, from the same source.
    let mut doubled = Vec::new();
    for r in &baseline {
        doubled.push(r.clone());
        if r.1 == 1 {
            doubled.push(r.clone());
        }
    }

    let single = emitted_quotes_by_symbol(&replay(&baseline));
    let dup_msgs = replay(&doubled);
    // No duplicate ever reaches the wire, and the emitted set is byte-identical to the single feed.
    assertions::no_business_duplicates(&dup_msgs);
    assert!(!single.is_empty(), "baseline emitted no quotes");
    assert_eq!(
        single,
        emitted_quotes_by_symbol(&dup_msgs),
        "delivering every mktdata packet twice changed the emitted quote set"
    );
}

/// Cross-source duplicate at the packet level: replay one publisher, then replay it with each
/// mktdata datagram **also** delivered from a second publisher IP (a mirror of the same feed). The
/// leader (first to open each tick) wins and the mirror is a non-leader no-op, so the emitted quote
/// set is unchanged — the multi-publisher dedup collapses the redundant feed.
#[test]
fn duplicate_packet_from_second_publisher_collapses() {
    let recs = read_combined("tests/fixtures/tob_btc_dual.combined.bin");
    let pub_ip = recs
        .iter()
        .find(|(_ip, role, _)| *role == 1)
        .map(|(ip, _, _)| *ip)
        .expect("fixture has mktdata");
    let baseline: Vec<_> = recs
        .iter()
        .filter(|(ip, role, _)| *role == 0 || *ip == pub_ip)
        .cloned()
        .collect();

    let mirror_ip = IpAddr::V4(Ipv4Addr::new(10, 255, 255, 254));
    assert_ne!(mirror_ip, pub_ip, "mirror IP must differ from the leader");
    let mut mirrored = Vec::new();
    for r in &baseline {
        mirrored.push(r.clone());
        if r.1 == 1 {
            mirrored.push((mirror_ip, 1u8, r.2.clone())); // same bytes, second publisher
        }
    }

    let single = emitted_quotes_by_symbol(&replay(&baseline));
    let mirror_msgs = replay(&mirrored);
    assertions::no_business_duplicates(&mirror_msgs);
    assert!(!single.is_empty(), "baseline emitted no quotes");
    assert_eq!(
        single,
        emitted_quotes_by_symbol(&mirror_msgs),
        "mirroring every mktdata packet from a second publisher changed the emitted quote set"
    );
}

/// The duplicate-packet case for trades: replay the single-publisher TOB golden, then replay it with
/// every mktdata frame duplicated. Trades dedup by `trade_id` in the arbiter's windowed dedup, so
/// the emitted trade set is unchanged. Guarded so a trade-less fixture fails loud rather than
/// passing vacuously.
#[test]
fn duplicate_multicast_trade_packet_collapses() {
    let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    let ref_bytes = std::fs::read("tests/fixtures/tob_refdata.bin").expect("read tob_refdata.bin");
    let mkt_bytes =
        std::fs::read("tests/fixtures/tob_marketdata.bin").expect("read tob_marketdata.bin");
    let ref_frames = replay_helper::split_frames(&ref_bytes, replay_helper::TOB_MAGIC);
    let mkt_frames = replay_helper::split_frames(&mkt_bytes, replay_helper::TOB_MAGIC);

    // Refdata first (instrument definitions before prices), then mktdata, all from one publisher.
    let mut baseline: Vec<(IpAddr, u8, Vec<u8>)> = Vec::new();
    for f in &ref_frames {
        baseline.push((ip, 0, f.clone()));
    }
    for f in &mkt_frames {
        baseline.push((ip, 1, f.clone()));
    }

    let mut doubled = baseline.clone();
    for f in &mkt_frames {
        doubled.push((ip, 1, f.clone())); // each mktdata datagram delivered a second time
    }

    let single = emitted_trade_ids(&replay(&baseline));
    assert!(
        !single.is_empty(),
        "TOB golden carried no trades — trade dedup not exercised"
    );
    assert_eq!(
        single,
        emitted_trade_ids(&replay(&doubled)),
        "delivering every mktdata packet twice changed the emitted trade set"
    );
}

/// True if the frame carries a quote for `id` (used to build the DOGE-only subset; a TOB frame
/// batches several instruments, so a DOGE-bearing frame may also carry others — kept whole, exactly
/// as the full run sees it).
fn frame_carries(frame: &[u8], id: u32) -> bool {
    match codec::decode_frame(frame) {
        Ok((_h, msgs)) => msgs
            .iter()
            .any(|m| matches!(m, codec::Message::Quote(q) if q.instrument_id == id)),
        Err(_) => false,
    }
}
