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
use doublezero_edge_connect::{
    ingest::{
        arbiter::{Arbiter, Publisher, SharedArbiter, TRADE_DEDUP_WINDOW},
        codec, codec_mbo,
        feeds::{FeedKind, FEEDS},
        processor::{MboProcessor, TobProcessor},
        receiver::{FrameCtx, FrameProcessor, PortRole},
    },
    model::{BookAccumulator, BookAction, BookChange, BookSide, FeedMessage, NormalizedBook},
};
use serde_json::Value;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{atomic::AtomicBool, Arc, Mutex},
};
use tokio::sync::broadcast;

/// Map a combined-record role byte to its `PortRole`. MBO adds a third role over TOB's two:
/// `0 = refdata`, `1 = mktdata`, `2 = snapshot` (the converter's `--combined-with --protocol mbo`
/// encoding; see `examples/pcap2frames.rs`).
fn port_role(role: u8) -> PortRole {
    match role {
        0 => PortRole::Refdata,
        2 => PortRole::Snapshot,
        _ => PortRole::Mktdata,
    }
}

/// Replay combined MBO records through a single re-keyed `MboProcessor` feeding the shared `Arbiter`
/// in capture order, collecting the emitted WS messages as JSON. The production demux+dedup path:
/// each record's source IP becomes `FrameCtx.publisher`, so the processor reconstructs an independent
/// book per `(publisher, instrument)` and the cross-publisher latch-to-leader depth floor runs in the
/// arbiter — exactly as in the binary.
fn replay_mbo(recs: &[(IpAddr, u8, Vec<u8>)]) -> Vec<Value> {
    let (tx, mut rx) = broadcast::channel(1 << 16);
    let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, TRADE_DEDUP_WINDOW)));
    let instruments = Arc::new(Mutex::new(HashMap::new()));
    let depth = Arc::new(Mutex::new(HashMap::new()));
    // Trades off, as the live MBO row is (`feeds::FEEDS`): its `OrderExecute` prints carry no venue
    // trade id, so they bypass the arbiter's dedup window — see `mbo_prints_carry_no_venue_trade_id`.
    let mut p = MboProcessor::new(depth, Arc::new(AtomicBool::new(false)));
    for (ip, role, frame) in recs {
        let ctx = FrameCtx {
            venue: "Hyperliquid",
            arbiter: &arbiter,
            instruments: &instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role: port_role(*role),
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

/// Emitted `depth` messages (full-state book snapshots).
fn depths(msgs: &[Value]) -> Vec<&Value> {
    msgs.iter().filter(|m| m["type"] == "depth").collect()
}

/// Count emitted depths whose book is empty at the `source_ts == 0` anchor (`bids == asks == []`).
fn empty_anchor_depths(msgs: &[Value]) -> usize {
    depths(msgs)
        .iter()
        .filter(|d| {
            d["source_ts_ns"].as_u64() == Some(0)
                && d["bids"].as_array().is_some_and(|a| a.is_empty())
                && d["asks"].as_array().is_some_and(|a| a.is_empty())
        })
        .count()
}

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
    let mut p = TobProcessor::new(Arc::new(AtomicBool::new(true)));
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
    // an unchanged price/size is a distinct quote. Far above a strict one-per-tick watermark (~417,
    // which over-drops real intra-tick changes).
    //
    // 4521, down from 4540 before reference-data state became per publisher: each arm now gates on
    // its OWN definitions, and the second arm's first burst lands ~280 records after the first's, so
    // its quotes in that startup window no longer ride the peer's definitions. Startup-only — both
    // arms re-burst every few seconds, and the arm that already has definitions covers the tick.
    assert_eq!(
        quotes, 4521,
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

/// Two-publisher **Market-by-Order** depth dedup over the real combined golden: two live HL
/// publishers' interleaved BTC capture, each reconstructing its own book from a synthetic empty
/// anchor + its independent delta stream. The cross-publisher contract:
///   1. `no_business_duplicates` on the emitted `depth` (content-inclusive identity) — the leader's
///      book is served per `source_ts` tick, the redundant publisher's collapsed.
///   2. The two identical synced-but-empty depths at `source_ts == 0` (one per publisher's anchor)
///      collapse to exactly ONE — the deliberate no-`source_ts==0`-bypass for depth.
///   3. Both publishers reconstruct independently: each replayed ALONE emits depth (its book syncs
///      from its own anchor + deltas — proving the per-`(publisher, instrument)` re-key).
///   4. The combined emission collapses redundancy: fewer depths than the two publishers emit
///      separately (the floor dropped the non-leader's mirror).
///
/// Falsifiable: with the depth floor bypassed (always-admit) the two anchors at `source_ts == 0`
/// both emit and `no_business_duplicates` flags the identical `(0, [], [])` pair; sharing one book
/// across publishers (the pre-#28 key) collides their delta sequence spaces and corrupts the book.
#[test]
fn two_publishers_mbo_depth_dedup() {
    let recs = read_combined("tests/fixtures/mbo_btc_dual.combined.bin");
    let pubs: Vec<IpAddr> = {
        let mut v: Vec<IpAddr> = recs.iter().map(|(ip, _, _)| *ip).collect();
        v.sort();
        v.dedup();
        v
    };
    assert_eq!(pubs.len(), 2, "golden must carry exactly two publishers");

    let msgs = replay_mbo(&recs);

    // (1) the cross-publisher dedup contract holds on the emitted depth.
    assertions::no_business_duplicates(&msgs);
    assertions::instrument_before_price(&msgs);
    let combined_depths = depths(&msgs).len();
    assert!(combined_depths > 0, "no depth emitted from the golden");

    // (2) the two empty-book anchors at source_ts==0 collapse to one.
    assert_eq!(
        empty_anchor_depths(&msgs),
        1,
        "the two publishers' identical empty-book anchors at source_ts==0 must collapse to one"
    );

    // (3) each publisher independently reconstructs its book: replayed alone it still emits depth.
    let mut alone_total = 0usize;
    for p in &pubs {
        let alone: Vec<_> = recs.iter().filter(|(ip, _, _)| ip == p).cloned().collect();
        let n = depths(&replay_mbo(&alone)).len();
        assert!(
            n > 0,
            "publisher {p} alone emitted no depth — its book never synced"
        );
        alone_total += n;
    }

    // (4) redundancy collapsed: the combined run emits fewer depths than the two publishers do
    // separately (the floor dropped the non-leader publisher's redundant book states).
    assert!(
        combined_depths < alone_total,
        "combined depth {combined_depths} not below per-publisher sum {alone_total} — \
         cross-publisher dedup collapsed nothing"
    );
}

/// Strict packet-level falsifiability for MBO depth, mirroring the TOB
/// `duplicate_packet_from_second_publisher_collapses`: replay one publisher, then replay it with each
/// snapshot+mktdata datagram **also** delivered byte-for-byte from a second publisher IP. The two
/// books reconstruct identically, so every depth the mirror produces is an exact `(source_ts,
/// content)` duplicate the leader already emitted — all dropped as non-leader no-ops, leaving the
/// emitted depth set unchanged. With dedup off the mirror would double every depth and
/// `no_business_duplicates` would flag them.
#[test]
fn mbo_depth_mirror_from_second_publisher_collapses() {
    let recs = read_combined("tests/fixtures/mbo_btc_dual.combined.bin");
    let pub_ip = recs
        .iter()
        .map(|(ip, _, _)| *ip)
        .next()
        .expect("fixture not empty");
    let baseline: Vec<_> = recs
        .iter()
        .filter(|(ip, _, _)| *ip == pub_ip)
        .cloned()
        .collect();

    let mirror_ip = IpAddr::V4(Ipv4Addr::new(10, 255, 255, 254));
    assert_ne!(mirror_ip, pub_ip, "mirror IP must differ from the original");
    let mut mirrored = Vec::new();
    for r in &baseline {
        mirrored.push(r.clone());
        mirrored.push((mirror_ip, r.1, r.2.clone())); // same bytes (all roles), second publisher
    }

    let base_depths = depth_identities(&replay_mbo(&baseline));
    let mirror_msgs = replay_mbo(&mirrored);
    assertions::no_business_duplicates(&mirror_msgs);
    assert!(!base_depths.is_empty(), "baseline emitted no depth");
    assert_eq!(
        base_depths,
        depth_identities(&mirror_msgs),
        "mirroring every packet from a second publisher changed the emitted depth set"
    );
}

/// Every print in the live Market-by-Order golden carries `trade_id == 0` — the venue stamps no
/// trade id on `OrderExecute`. That is why the arbiter treats `0` as "no identity" rather than a
/// dedup key, and why the Market-by-Order rows must stay `emit_trades: false`: two mirrored
/// publishers' zero-id prints have no window to collapse against.
#[test]
fn mbo_prints_carry_no_venue_trade_id() {
    let recs = read_combined("tests/fixtures/mbo_btc_dual.combined.bin");
    let mut prints = 0;
    for (_ip, _role, frame) in &recs {
        let Ok((_h, msgs)) = codec_mbo::decode_frame(frame) else {
            continue;
        };
        for m in &msgs {
            let id = match m {
                codec_mbo::Message::OrderExecute(o) => o.trade_id,
                codec_mbo::Message::Trade(t) => t.trade_id,
                _ => continue,
            };
            prints += 1;
            assert_eq!(
                id, 0,
                "golden carries a venue trade id — revisit the bypass"
            );
        }
    }
    assert!(
        prints > 0,
        "golden carried no prints — the fact is unpinned"
    );

    // Scoped to the venue this golden was captured from: another venue's MBO stream may well stamp
    // real trade ids, and that is its own row's call.
    for f in FEEDS
        .iter()
        .filter(|f| f.venue == "Hyperliquid" && f.kind == FeedKind::MarketByOrder)
    {
        assert!(!f.emit_trades, "{} would publish zero-id prints", f.venue);
    }
}

/// The content-inclusive identity set of emitted depths (`venue|symbol|source_ts|bids|asks`), the
/// same key the `no_business_duplicates` oracle uses — for comparing two runs' emitted depth sets.
fn depth_identities(msgs: &[Value]) -> std::collections::BTreeSet<String> {
    depths(msgs)
        .iter()
        .map(|d| {
            format!(
                "{}|{}|{}|{}|{}",
                d["venue"].as_str().unwrap_or_default(),
                d["symbol"].as_str().unwrap_or_default(),
                d["source_ts_ns"].as_u64().unwrap_or_default(),
                d["bids"],
                d["asks"]
            )
        })
        .collect()
}

const BOOK_VENUE: &str = "BookArmsInterleave";
const BOOK_CHANNEL: u32 = 2;
const BOOK_INSTRUMENT: u32 = 41;

fn arm(n: u8) -> Publisher {
    Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
}

fn level(side: BookSide, price: f64, size: f64) -> BookChange {
    BookChange {
        action: BookAction::Update,
        side,
        price,
        size,
    }
}

/// One `book` batch for the single market under test. `recv_ns` is the authority's arrival clock.
fn book_batch(changes: Vec<BookChange>, last: bool, recv_ns: u64) -> FeedMessage {
    FeedMessage::Book(NormalizedBook {
        venue: BOOK_VENUE.into(),
        symbol: "BTC-PERP".into(),
        channel: BOOK_CHANNEL,
        instrument_id: BOOK_INSTRUMENT,
        changes,
        snapshot: false,
        last,
        source_ts_ns: recv_ns,
        recv_ts_ns: recv_ns,
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    })
}

/// One arm's `(changes, last)` batch stream, parametrized on the arm's price/size base so two arms
/// built from it publish divergent level sets. Batches 2 and 3 are one logical event.
fn arm_batches(px: f64, sz: f64) -> Vec<(Vec<BookChange>, bool)> {
    vec![
        (
            vec![
                level(BookSide::Bid, px, sz),
                level(BookSide::Ask, px + 1.0, sz + 2.0),
            ],
            true,
        ),
        (vec![level(BookSide::Bid, px - 0.5, sz - 2.0)], false),
        (
            vec![
                level(BookSide::Ask, px + 1.5, sz - 3.0),
                BookChange {
                    action: BookAction::Delete,
                    side: BookSide::Bid,
                    price: px,
                    size: 0.0,
                },
            ],
            true,
        ),
        (vec![level(BookSide::Bid, px - 0.5, sz - 1.0)], true),
    ]
}

fn drain_books(rx: &mut broadcast::Receiver<Arc<FeedMessage>>) -> Vec<NormalizedBook> {
    let mut out = Vec::new();
    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            out.push(b.clone());
        }
    }
    out
}

/// The single-arm authority gate for the incremental `book` product. Two arms mirror one venue and
/// their per-instrument delta series are unrelated by construction, so interleaving both on one wire
/// stream corrupts a consumer's book while every per-arm sequence check the producer ran still passes.
/// Pinned: only the elected arm's batches reach the wire, and a `BookAccumulator` fed from the drained
/// wire messages alone reproduces that arm's level set exactly. Against the pre-gate undeduped
/// passthrough both fail — all eight batches go out, the challenger's levels enter the consumer's book,
/// and its `last: false` batch folds into the leader's logical event.
#[test]
fn interleaved_book_arms_publish_one_coherent_stream() {
    fn clear_both() -> BookChange {
        BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
        }
    }

    let (tx, mut rx) = broadcast::channel(64);
    let mut arb = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
    let (leader, challenger) = (arm_batches(100.0, 5.0), arm_batches(200.0, 50.0));
    assert_ne!(
        leader, challenger,
        "identical arms would pass with the gate removed"
    );

    // Arm-by-arm, arrivals microseconds apart: past the 2s `leader_timeout_ns` authority would
    // legitimately transfer on silence.
    for (i, (l, c)) in leader.iter().zip(&challenger).enumerate() {
        let t = 1_000 + i as u64 * 2_000;
        arb.emit(book_batch(l.0.clone(), l.1, t), arm(1));
        arb.emit(book_batch(c.0.clone(), c.1, t + 1_000), arm(2));
    }

    // The market's first admitted batch re-baselines the consumer, and this arm has sent no producer
    // re-baseline, so a bare `clear` leads the stream. Everything after it is the leader's, verbatim.
    let published = drain_books(&mut rx);
    let (first, rest) = published.split_first().expect("the re-baseline");
    assert_eq!(first.changes, vec![clear_both()]);
    assert_eq!(
        rest.iter()
            .map(|b| (b.changes.clone(), b.last))
            .collect::<Vec<_>>(),
        leader,
        "the wire must carry the elected arm's batches verbatim and none of the challenger's"
    );

    let mut acc = BookAccumulator::new(published[0].symbol.clone());
    for b in &published {
        acc.apply(b);
    }
    let full = acc.to_book(&BOOK_VENUE.into(), BOOK_CHANNEL, BOOK_INSTRUMENT);
    assert_eq!(
        full.changes[1..].to_vec(), // [0] is the re-baseline `clear`
        vec![
            level(BookSide::Bid, 99.5, 4.0),
            level(BookSide::Ask, 101.0, 7.0),
            level(BookSide::Ask, 101.5, 2.0),
        ],
        "a consumer applying only what we published must hold the elected arm's book"
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
