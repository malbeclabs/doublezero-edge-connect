//! Real-traffic validation of the shred parser/verifier/dedup against a captured `edge-solana-*`
//! frame sample. Unlike the self-consistency round-trips in `parse.rs` (which build a shred the way
//! agave does and re-parse it — they cannot catch a constant that both sides share), these run the
//! pipeline over **real mainnet shreds** and check the leader's ed25519 signature. A wrong offset,
//! payload size, or variant mapping fails the signature with overwhelming probability, so a passing
//! verify is byte-level proof the layout is right.
//!
//! Fixtures (see `tests/fixtures/PROVENANCE.md`):
//! - `shred_sample.bin` — `[u32 LE len][datagram]` records, all four chained-merkle variants
//!   (`0x66/0x76/0x96/0xb6`) plus cross-group duplicates, from one mainnet slot.
//! - `shred_leaders.json` — `{slot: base58 leader pubkey}` from `getLeaderSchedule` at capture time.

use std::collections::{HashMap, HashSet};

use super::{
    dedup::{Action, DedupWindow},
    parse::{parse, ShredType},
    verify::verify,
};

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

/// Decode the `[u32 LE len][bytes]` record stream into datagrams.
fn load_datagrams() -> Vec<Vec<u8>> {
    let bytes = std::fs::read(fixture_path("shred_sample.bin")).expect("read shred_sample.bin");
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        out.push(bytes[i..i + len].to_vec());
        i += len;
    }
    out
}

fn load_leaders() -> HashMap<u64, [u8; 32]> {
    let json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture_path("shred_leaders.json")).unwrap())
            .unwrap();
    json["leaders"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(slot, pk)| {
            let slot: u64 = slot.parse().unwrap();
            let bytes = bs58::decode(pk.as_str().unwrap()).into_vec().unwrap();
            (slot, <[u8; 32]>::try_from(bytes).unwrap())
        })
        .collect()
}

/// Every captured shred must parse and verify against its slot leader. With the pre-fix parser this
/// fails: `0x96` (chained-merkle data, the dominant data variant) is unmapped → `parse` returns
/// `None`, and data shreds use a 1203-byte payload while the parser assumed 1228 → wrong merkle
/// root → bad signature.
#[test]
fn real_shreds_parse_and_verify_against_leader() {
    let datagrams = load_datagrams();
    let leaders = load_leaders();
    assert!(
        !datagrams.is_empty() && !leaders.is_empty(),
        "fixtures present"
    );

    // (parsed, verified) tallied per raw variant byte for a useful failure message.
    let mut per_variant: HashMap<u8, (usize, usize)> = HashMap::new();
    let (mut parsed, mut verified) = (0usize, 0usize);
    for pkt in &datagrams {
        let vb = pkt[64];
        let e = per_variant.entry(vb).or_default();
        if let Some(meta) = parse(pkt) {
            parsed += 1;
            e.0 += 1;
            let leader = leaders.get(&meta.slot).expect("leader for captured slot");
            if verify(&meta, leader) {
                verified += 1;
                e.1 += 1;
            }
        }
    }

    let mut by: Vec<_> = per_variant.iter().collect();
    by.sort();
    let report: Vec<String> = by
        .iter()
        .map(|(vb, (p, v))| format!("0x{vb:02x}: parsed {p} verified {v}"))
        .collect();
    let total = datagrams.len();
    assert_eq!(
        verified,
        total,
        "all {total} real shreds must verify; got parsed={parsed} verified={verified} [{}]",
        report.join(", ")
    );
}

/// The capture carries the same shred from multiple `edge-solana-*` groups (primary + retransmit).
/// The prefer-valid dedup window must forward exactly one verified copy per `(slot, index, type)`.
#[test]
fn dedup_collapses_cross_group_duplicates() {
    let datagrams = load_datagrams();
    let leaders = load_leaders();

    let mut window = DedupWindow::new(512);
    let mut forwarded = 0usize;
    let mut keys: HashSet<(u64, u32, ShredType)> = HashSet::new();
    for pkt in &datagrams {
        let meta = parse(pkt).expect("real shred parses");
        keys.insert((meta.slot, meta.index, meta.shred_type));
        let leader = leaders.get(&meta.slot).copied();
        let mut verify_fn = || leader.as_ref().is_some_and(|pk| verify(&meta, pk));
        if window.decide(
            meta.slot,
            meta.index,
            meta.shred_type,
            leader.is_some(),
            &mut verify_fn,
        ) == Action::Forward
        {
            forwarded += 1;
        }
    }
    assert!(
        datagrams.len() > keys.len(),
        "fixture must contain duplicates ({} datagrams, {} unique keys)",
        datagrams.len(),
        keys.len()
    );
    assert_eq!(
        forwarded,
        keys.len(),
        "exactly one valid copy per unique shred should be forwarded"
    );
}

/// The most literal duplicate-packet case: take one real datagram and feed it through the dedup
/// window twice. The first copy verifies and forwards; the second is a duplicate of the recorded
/// winner and is dropped *without* re-running the signature check.
#[test]
fn same_datagram_twice_forwards_once() {
    let datagrams = load_datagrams();
    let leaders = load_leaders();
    let pkt = &datagrams[0];
    let meta = parse(pkt).expect("real shred parses");
    let leader = leaders.get(&meta.slot).copied();
    assert!(leader.is_some(), "leader known for the captured slot");

    let mut window = DedupWindow::new(512);
    let mut verify_calls = 0usize;

    let first = {
        let mut verify_fn = || {
            verify_calls += 1;
            leader.as_ref().is_some_and(|pk| verify(&meta, pk))
        };
        window.decide(
            meta.slot,
            meta.index,
            meta.shred_type,
            leader.is_some(),
            &mut verify_fn,
        )
    };
    assert_eq!(first, Action::Forward, "first copy verifies and forwards");
    let after_first = verify_calls;

    let second = {
        let mut verify_fn = || {
            verify_calls += 1;
            leader.as_ref().is_some_and(|pk| verify(&meta, pk))
        };
        window.decide(
            meta.slot,
            meta.index,
            meta.shred_type,
            leader.is_some(),
            &mut verify_fn,
        )
    };
    assert_eq!(second, Action::Drop, "duplicate of the winner is dropped");
    assert_eq!(
        verify_calls, after_first,
        "second (duplicate) copy must skip the signature check"
    );
}

/// End-to-end through the real `forwarder_task` (parse → leader → verify → dedup → fan-out), driven
/// by the captured datagrams over the mpsc. This is the test that directly catches **silent
/// no-dedup**: when `parse` rejects a real variant the forwarder falls back to forwarding the
/// datagram undeduped, so every duplicate of an unparseable shred would pass through and the count
/// would exceed the unique-key count. Asserting equality pins "exactly one copy per shred, and the
/// parse/verify path actually engaged for every datagram".
#[tokio::test]
async fn forwarder_task_forwards_one_copy_per_shred_over_real_capture() {
    use std::sync::Arc;

    use tokio::{
        net::UdpSocket,
        sync::mpsc,
        time::{timeout, Duration},
    };

    use super::{forwarder_task, leader::LeaderSchedule, ShredPacket};

    let datagrams = load_datagrams();
    let leaders = load_leaders();

    // Count unique keys over only the datagrams that parse. If a real variant stops parsing, the
    // forwarder forwards each of its copies undeduped, so `forwarded` climbs above this count —
    // making the no-dedup regression show up as a count mismatch, not a setup panic.
    let mut keys = HashSet::new();
    for pkt in &datagrams {
        if let Some(m) = parse(pkt) {
            keys.insert((m.slot, m.index, m.shred_type));
        }
    }

    // Seed the schedule densely over the captured slot range so `forwarder_task` verifies (rather
    // than failing open) for every datagram — the path that exercises dedup.
    let first = *leaders.keys().min().unwrap();
    let last = *leaders.keys().max().unwrap();
    let mut dense = vec![None; (last - first + 1) as usize];
    for (slot, pk) in &leaders {
        dense[(slot - first) as usize] = Some(*pk);
    }
    let schedule = Arc::new(LeaderSchedule::with_seeded_cache(989, first, dense));

    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dst = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel::<ShredPacket>(256);
    let handle = tokio::spawn(forwarder_task(rx, vec![dst], Some(schedule), 512));
    for pkt in &datagrams {
        tx.send(pkt.clone()).await.unwrap();
    }
    drop(tx); // close the channel so the forwarder drains and exits
    handle.await.unwrap().unwrap();

    let mut buf = [0u8; 2048];
    let mut forwarded = 0usize;
    while timeout(Duration::from_millis(500), listener.recv(&mut buf))
        .await
        .is_ok()
    {
        forwarded += 1;
    }
    assert_eq!(
        forwarded,
        keys.len(),
        "forwarder must forward exactly one copy per unique shred — a larger count means \
         unparseable shreds slipped through undeduped"
    );
}
