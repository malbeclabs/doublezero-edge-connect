//! Two-publisher integration test: feed the combined dual-publisher TOB fixture through
//! `TobProcessor` in capture order and assert the dedup contract (no business duplicates) holds
//! across publishers. This is the cross-publisher counterpart to `tob_single_publisher_contract`
//! in `e2e.rs`: the fixture carries two independent publishers mirroring the same Hyperliquid feed,
//! so the windowed-identity quote deduper must collapse each logical update to one output.

mod common;

use common::assertions;
use doublezero_edge_connect::ingest::{
    processor::TobProcessor,
    receiver::{FrameCtx, FrameProcessor, PortRole},
};
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

#[test]
fn two_publishers_dedup_collapses_to_single() {
    let recs = read_combined("tests/fixtures/tob_btc_dual.combined.bin");
    let (tx, mut rx) = broadcast::channel(1 << 16);
    let instruments = Arc::new(Mutex::new(HashMap::new()));
    let mut p = TobProcessor::new(true);
    for (ip, role, frame) in &recs {
        let ctx = FrameCtx {
            venue: "Hyperliquid",
            tx: &tx,
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
    // THE contract: a duplicate (content, source_ts) from the second publisher collapses, so no two
    // emitted quotes share the oracle's business identity.
    assertions::no_business_duplicates(&msgs);
    assertions::quotes_well_formed(&msgs);

    let quotes = msgs.iter().filter(|m| m["type"] == "quote").count();
    // Sanity bound: the fixture carries 9330 raw mktdata frames split across two publishers
    // mirroring the same feed. Dedup collapses each publisher's copy of a given (source_ts, content)
    // update to one output, so the emitted count must sit well below 9330 (proving the second
    // publisher's duplicates were dropped) while staying positive. Observed: 4470.
    assert!(
        quotes > 0 && quotes < 6000,
        "two-pub quote count = {quotes}"
    );
}
