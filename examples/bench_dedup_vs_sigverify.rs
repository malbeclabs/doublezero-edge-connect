//! Microbenchmark: per-datagram cost of the two shred-forwarder modes.
//!
//! Both modes run `parse::parse` on every datagram (shared cost). The difference is the marginal
//! work each adds before the dedup-window decision:
//!   - dedup-only  → `dedup::fingerprint(&pkt)`  (a content hash of the whole datagram)
//!   - sigverify   → `verify::verify(&meta, pk)`  (ed25519 over the recomputed merkle root)
//!
//! This times each primitive in isolation plus the two full per-datagram paths, so the hash-vs-
//! ed25519 cost is directly comparable. Run with: `cargo run --release --example bench_dedup_vs_sigverify`.

use std::{hint::black_box, time::Instant};

use doublezero_edge_connect::shred::{dedup, parse, verify};
use ed25519_dalek::{Signer, SigningKey};

/// agave merkle data-shred wire size; the fingerprint hashes the whole datagram, so size matters.
const DATA_SHRED_LEN: usize = 1203;
const OFFSET_OF_VARIANT: usize = 64;
const OFFSET_OF_SLOT: usize = 65;
const OFFSET_OF_INDEX: usize = 73;
const OFFSET_OF_FEC_SET_INDEX: usize = 79;

/// Build a parseable merkle **data** shred (proof_size 2, leaf index 1) and sign its recomputed
/// merkle root with `signing`, returning the finished datagram plus the leader pubkey. The merkle
/// proof bytes are arbitrary — `parse` only requires the leaf index to collapse to 0, it does not
/// validate sibling values — so this is a faithful stand-in for a real signed shred's *cost*.
fn build_signed_shred(signing: &SigningKey) -> (Vec<u8>, [u8; 32]) {
    let mut buf: Vec<u8> = (0..DATA_SHRED_LEN).map(|i| (i % 251) as u8).collect();
    buf[OFFSET_OF_VARIANT] = 0x80 | 2; // merkle data, proof_size = 2
    buf[OFFSET_OF_SLOT..OFFSET_OF_SLOT + 8].copy_from_slice(&123_456_789u64.to_le_bytes());
    buf[OFFSET_OF_INDEX..OFFSET_OF_INDEX + 4].copy_from_slice(&1u32.to_le_bytes());
    buf[OFFSET_OF_FEC_SET_INDEX..OFFSET_OF_FEC_SET_INDEX + 4].copy_from_slice(&0u32.to_le_bytes());

    // First parse gives us the merkle root parse computes; sign exactly that so verify() passes.
    let root = parse::parse(&buf)
        .expect("benchmark shred must parse")
        .signed_message;
    let sig = signing.sign(&root).to_bytes();
    buf[..64].copy_from_slice(&sig);
    (buf, signing.verifying_key().to_bytes())
}

fn bench<F: FnMut()>(name: &str, iters: u64, mut f: F) {
    // Warm up so we measure steady-state, not first-call effects.
    for _ in 0..iters / 10 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / iters as f64;
    let per_sec = 1e9 / per;
    println!(
        "{name:<34} {per:>10.1} ns/op   {:>10.2} M ops/s   ({iters} iters)",
        per_sec / 1e6
    );
}

fn main() {
    let signing = SigningKey::from_bytes(&[7u8; 32]);
    let (pkt, pubkey) = build_signed_shred(&signing);
    let meta = parse::parse(&pkt).expect("shred parses");
    assert!(
        verify::verify(&meta, &pubkey),
        "benchmark shred must verify"
    );

    println!(
        "shred datagram = {} bytes, merkle root (signed msg) = {} bytes\n",
        pkt.len(),
        meta.signed_message.len()
    );

    // --- primitives ---------------------------------------------------------------------------
    bench("parse (shared by both modes)", 2_000_000, || {
        black_box(parse::parse(black_box(&pkt)));
    });
    bench("fingerprint  [dedup-only adds]", 5_000_000, || {
        black_box(dedup::fingerprint(black_box(&pkt)));
    });
    bench("ed25519 verify [sigverify adds]", 100_000, || {
        black_box(verify::verify(black_box(&meta), black_box(&pubkey)));
    });

    println!();

    // --- full per-datagram path for the first (unique) copy of a key --------------------------
    bench("dedup-only: parse + fingerprint", 1_000_000, || {
        let m = parse::parse(black_box(&pkt)).unwrap();
        black_box(dedup::fingerprint(black_box(&pkt)));
        black_box(m.slot);
    });
    bench("sigverify: parse + verify", 100_000, || {
        let m = parse::parse(black_box(&pkt)).unwrap();
        black_box(verify::verify(black_box(&m), black_box(&pubkey)));
    });
}
