//! Pure shred parser: pull the fields the forwarder's dedup + sigverify need out of a raw
//! `edge-solana-*` datagram — the 64-byte signature, the variant/type, `slot`, `shred_index`, and
//! the exact bytes the leader's signature covers (the post-signature payload for legacy shreds, the
//! recomputed **merkle root** for merkle shreds).
//!
//! ⚠️ **Offsets and the merkle layout are transcribed from the agave shred format**
//! (`ledger/src/shred.rs`, `shred/legacy.rs`, `shred/merkle.rs`) and are **NOT validated against a
//! live `edge-solana-*` hexdump** — the same discipline the repo already applies to its unvalidated
//! sibling codecs (`codec_midpoint`/`codec_mbo`). The round-trip tests below pin *self-consistency*
//! only (build a shred the way agave does, recompute its root, verify); they cannot catch a wrong
//! constant that both construction and verification share. Before trusting sigverify in production,
//! confirm these offsets against a captured frame. The forwarder logs a one-time warning and a
//! periodic valid/invalid tally so a systematic misparse (≈100% "invalid") is visible immediately.

use sha2::{Digest, Sha256};

/// ed25519 signature — first field of every shred.
pub const SIZE_OF_SIGNATURE: usize = 64;
/// `shred_variant` byte: its high nibble selects legacy/merkle + data/code, low nibble (merkle only)
/// carries the proof length.
const OFFSET_OF_VARIANT: usize = 64;
const OFFSET_OF_SLOT: usize = 65; // u64 LE
const OFFSET_OF_INDEX: usize = 73; // u32 LE
const OFFSET_OF_FEC_SET_INDEX: usize = 79; // u32 LE (after version: u16 @ 77)
/// Common header = signature(64) + variant(1) + slot(8) + index(4) + version(2) + fec_set_index(4).
const SIZE_OF_COMMON_HEADER: usize = 83;
/// Coding header sits immediately after the common header (code shreds only): num_data_shreds: u16,
/// num_coding_shreds: u16, position: u16. We need num_data + position to place a code shred in its
/// FEC set's merkle tree.
const OFFSET_OF_NUM_DATA_SHREDS: usize = SIZE_OF_COMMON_HEADER; // u16 LE
const OFFSET_OF_CODING_POSITION: usize = SIZE_OF_COMMON_HEADER + 4; // u16 LE

/// Full serialized shred payload size. Data and code shreds serialize to the same width so they can
/// share one network packet size; the merkle proof always sits at the tail, before the optional
/// retransmitter signature. (agave `ShredData::SIZE_OF_PAYLOAD == ShredCode::SIZE_OF_PAYLOAD`.)
const SHRED_PAYLOAD_SIZE: usize = 1228;
const SIZE_OF_MERKLE_PROOF_ENTRY: usize = 20;
/// Domain-separation prefixes guard against second-preimage attacks (agave `merkle.rs`).
const MERKLE_PREFIX_LEAF: &[u8] = &[0x00];
const MERKLE_PREFIX_NODE: &[u8] = &[0x01];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShredType {
    Data,
    Code,
}

/// The decoded fields needed downstream. `signed_message` is exactly what `verify.rs` runs ed25519
/// over against the slot leader's pubkey.
#[derive(Debug, Clone)]
pub struct ShredMeta {
    pub slot: u64,
    pub index: u32,
    pub shred_type: ShredType,
    pub signature: [u8; 64],
    pub signed_message: Vec<u8>,
}

/// A parsed `shred_variant` byte (offset 64). Legacy variants are two fixed byte values; merkle
/// variants encode `proof_size` in the low nibble and chained/resigned flags in the high nibble.
enum Variant {
    Legacy(ShredType),
    Merkle {
        ty: ShredType,
        proof_size: usize,
        resigned: bool,
    },
}

/// Decode the variant byte. Mirrors agave's `ShredVariant` encoding exactly:
/// `0x5a`/`0xa5` are legacy code/data; otherwise the high nibble selects merkle code (0x40/0x60/
/// 0x70) or data (0x80/0xa0/0xb0), with 0x60/0xa0 = chained and 0x70/0xb0 = chained + resigned.
fn parse_variant(b: u8) -> Option<Variant> {
    match b {
        0x5a => Some(Variant::Legacy(ShredType::Code)),
        0xa5 => Some(Variant::Legacy(ShredType::Data)),
        _ => {
            let proof_size = (b & 0x0f) as usize;
            // chained-only (0x60/0xa0) needs no special handling here: the chained merkle root sits
            // before the proof and is naturally folded into the leaf hash. Only `resigned` shifts
            // where the proof ends (a trailing retransmitter signature), so that's all we track.
            let (ty, resigned) = match b & 0xf0 {
                0x40 | 0x60 => (ShredType::Code, false),
                0x70 => (ShredType::Code, true),
                0x80 | 0xa0 => (ShredType::Data, false),
                0xb0 => (ShredType::Data, true),
                _ => return None,
            };
            Some(Variant::Merkle {
                ty,
                proof_size,
                resigned,
            })
        }
    }
}

/// Parse a raw datagram into [`ShredMeta`], or `None` if it is too short or not a recognized shred.
/// Never panics on malformed input (every slice access is bounds-checked).
pub fn parse(pkt: &[u8]) -> Option<ShredMeta> {
    if pkt.len() < SIZE_OF_COMMON_HEADER {
        return None;
    }
    let variant = parse_variant(pkt[OFFSET_OF_VARIANT])?;
    let slot = u64::from_le_bytes(read_array::<8>(pkt, OFFSET_OF_SLOT)?);
    let index = u32::from_le_bytes(read_array::<4>(pkt, OFFSET_OF_INDEX)?);
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&pkt[..SIZE_OF_SIGNATURE]);

    let (shred_type, signed_message) = match variant {
        // Legacy shreds sign the whole payload after the signature.
        Variant::Legacy(ty) => (ty, pkt.get(SIZE_OF_SIGNATURE..)?.to_vec()),
        // Merkle shreds sign the merkle root, recomputed from the leaf + proof.
        Variant::Merkle {
            ty,
            proof_size,
            resigned,
        } => {
            let root = merkle_root(pkt, ty, proof_size, resigned, index)?;
            (ty, root.to_vec())
        }
    };
    Some(ShredMeta {
        slot,
        index,
        shred_type,
        signature,
        signed_message,
    })
}

/// Recompute the 32-byte merkle root the leader signed: hash the leaf (everything between the
/// signature and the proof, which already includes the chained root when present), then fold each
/// 20-byte proof sibling up the tree using the shred's index within its FEC set. Returns `None` if
/// the datagram is too short or the proof does not collapse to a single root.
fn merkle_root(
    pkt: &[u8],
    ty: ShredType,
    proof_size: usize,
    resigned: bool,
    index: u32,
) -> Option<[u8; 32]> {
    let resign = if resigned { SIZE_OF_SIGNATURE } else { 0 };
    let proof_bytes = proof_size.checked_mul(SIZE_OF_MERKLE_PROOF_ENTRY)?;
    let proof_offset = SHRED_PAYLOAD_SIZE.checked_sub(proof_bytes + resign)?;
    if pkt.len() < proof_offset + proof_bytes {
        return None;
    }

    let leaf_chunk = pkt.get(SIZE_OF_SIGNATURE..proof_offset)?;
    let mut node = hashv(&[MERKLE_PREFIX_LEAF, leaf_chunk]);

    let mut idx = erasure_shard_index(pkt, ty, index)?;
    for k in 0..proof_size {
        let off = proof_offset + k * SIZE_OF_MERKLE_PROOF_ENTRY;
        let sibling = pkt.get(off..off + SIZE_OF_MERKLE_PROOF_ENTRY)?;
        // Intermediate nodes are joined on their first 20 bytes (agave `join_nodes`); the running
        // `node` stays a full 32-byte hash so the final root is 32 bytes.
        node = if idx % 2 == 0 {
            hashv(&[
                MERKLE_PREFIX_NODE,
                &node[..SIZE_OF_MERKLE_PROOF_ENTRY],
                sibling,
            ])
        } else {
            hashv(&[
                MERKLE_PREFIX_NODE,
                sibling,
                &node[..SIZE_OF_MERKLE_PROOF_ENTRY],
            ])
        };
        idx >>= 1;
    }
    // A well-formed proof of the right depth collapses the leaf index to 0 at the root.
    (idx == 0).then_some(node)
}

/// The shred's leaf index within its FEC set's merkle tree. Data shreds are laid out first
/// (`index - fec_set_index`); code shreds follow (`num_data_shreds + position`).
fn erasure_shard_index(pkt: &[u8], ty: ShredType, index: u32) -> Option<usize> {
    match ty {
        ShredType::Data => {
            let fec_set_index = u32::from_le_bytes(read_array::<4>(pkt, OFFSET_OF_FEC_SET_INDEX)?);
            (index as usize).checked_sub(fec_set_index as usize)
        }
        ShredType::Code => {
            let num_data =
                u16::from_le_bytes(read_array::<2>(pkt, OFFSET_OF_NUM_DATA_SHREDS)?) as usize;
            let position =
                u16::from_le_bytes(read_array::<2>(pkt, OFFSET_OF_CODING_POSITION)?) as usize;
            num_data.checked_add(position)
        }
    }
}

/// SHA-256 over the concatenation of `parts`, returning the 32-byte digest.
fn hashv(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// Read a fixed-size byte array at `off`, bounds-checked.
fn read_array<const N: usize>(pkt: &[u8], off: usize) -> Option<[u8; N]> {
    pkt.get(off..off + N)?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // A shred big enough that 1228-byte tail math is valid; filled with a recognizable pattern.
    fn blank_payload() -> Vec<u8> {
        (0..SHRED_PAYLOAD_SIZE).map(|i| (i % 251) as u8).collect()
    }

    fn put_common(buf: &mut [u8], variant: u8, slot: u64, index: u32, fec_set_index: u32) {
        buf[OFFSET_OF_VARIANT] = variant;
        buf[OFFSET_OF_SLOT..OFFSET_OF_SLOT + 8].copy_from_slice(&slot.to_le_bytes());
        buf[OFFSET_OF_INDEX..OFFSET_OF_INDEX + 4].copy_from_slice(&index.to_le_bytes());
        buf[OFFSET_OF_FEC_SET_INDEX..OFFSET_OF_FEC_SET_INDEX + 4]
            .copy_from_slice(&fec_set_index.to_le_bytes());
    }

    #[test]
    fn rejects_short_and_unknown() {
        assert!(parse(&[0u8; 10]).is_none());
        let mut buf = blank_payload();
        buf[OFFSET_OF_VARIANT] = 0x00; // not a valid variant
        assert!(parse(&buf).is_none());
    }

    #[test]
    fn parses_legacy_identity_and_signed_region() {
        let mut buf = blank_payload();
        put_common(&mut buf, 0xa5, 42, 7, 0); // legacy data
        let m = parse(&buf).expect("legacy data parses");
        assert_eq!(m.slot, 42);
        assert_eq!(m.index, 7);
        assert_eq!(m.shred_type, ShredType::Data);
        assert_eq!(&m.signature[..], &buf[..64]);
        // Legacy signs everything after the signature.
        assert_eq!(m.signed_message, buf[64..].to_vec());
    }

    #[test]
    fn variant_nibbles_map_to_type() {
        assert!(matches!(
            parse_variant(0x5a),
            Some(Variant::Legacy(ShredType::Code))
        ));
        assert!(matches!(
            parse_variant(0xa5),
            Some(Variant::Legacy(ShredType::Data))
        ));
        assert!(matches!(
            parse_variant(0x86),
            Some(Variant::Merkle {
                ty: ShredType::Data,
                proof_size: 6,
                resigned: false
            })
        ));
        assert!(matches!(
            parse_variant(0xb6),
            Some(Variant::Merkle {
                ty: ShredType::Data,
                proof_size: 6,
                resigned: true
            })
        ));
        assert!(matches!(
            parse_variant(0x46),
            Some(Variant::Merkle {
                ty: ShredType::Code,
                proof_size: 6,
                resigned: false
            })
        ));
        assert!(parse_variant(0x10).is_none());
    }

    // Build a merkle tree exactly the way agave does, sign nothing here (verify.rs covers signing),
    // and confirm `parse` recomputes the same root we built the tree from. Self-consistency only.
    fn leaf_hash(chunk: &[u8]) -> [u8; 32] {
        hashv(&[MERKLE_PREFIX_LEAF, chunk])
    }
    fn join(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
        hashv(&[
            MERKLE_PREFIX_NODE,
            &a[..SIZE_OF_MERKLE_PROOF_ENTRY],
            &b[..SIZE_OF_MERKLE_PROOF_ENTRY],
        ])
    }

    #[test]
    fn merkle_data_root_roundtrips_4_leaf_tree() {
        // 4-leaf tree (proof_size = 2). Our shred is leaf index 1 (data index 1, fec_set_index 0).
        let proof_size = 2usize;
        let mut buf = blank_payload();
        put_common(&mut buf, 0x80 | proof_size as u8, 100, 1, 0); // merkle data, our leaf = index 1

        let proof_offset = SHRED_PAYLOAD_SIZE - proof_size * SIZE_OF_MERKLE_PROOF_ENTRY;
        // Our leaf chunk is bytes [64..proof_offset]; the other three leaves are arbitrary.
        let our_leaf = leaf_hash(&buf[64..proof_offset]);
        let l0 = [0x11u8; 32];
        let l2 = [0x22u8; 32];
        let l3 = [0x33u8; 32];

        // Tree: parents (l0,our),(l2,l3) -> root. Our index = 1 (odd) so sibling at level 0 is l0
        // (on our left); level-1 sibling is the right subtree root (on our right).
        let p01 = join(&l0, &our_leaf);
        let p23 = join(&l2, &l3);
        let root = join(&p01, &p23);

        // Proof for leaf 1: [l0 (left sibling), p23 (right sibling)].
        buf[proof_offset..proof_offset + 20].copy_from_slice(&l0[..20]);
        buf[proof_offset + 20..proof_offset + 40].copy_from_slice(&p23[..20]);

        let m = parse(&buf).expect("merkle data parses");
        assert_eq!(m.shred_type, ShredType::Data);
        assert_eq!(m.slot, 100);
        assert_eq!(
            m.signed_message,
            root.to_vec(),
            "recomputed root must match the built tree"
        );
    }

    #[test]
    fn merkle_code_uses_num_data_plus_position_for_leaf_index() {
        // Code shred: leaf index = num_data + position. Use num_data=2, position=1 -> leaf 3 of a
        // 4-leaf tree (odd index).
        let proof_size = 2usize;
        let mut buf = blank_payload();
        put_common(&mut buf, 0x40 | proof_size as u8, 100, 9, 0); // merkle code; common index unused for tree
        buf[OFFSET_OF_NUM_DATA_SHREDS..OFFSET_OF_NUM_DATA_SHREDS + 2]
            .copy_from_slice(&2u16.to_le_bytes());
        buf[OFFSET_OF_CODING_POSITION..OFFSET_OF_CODING_POSITION + 2]
            .copy_from_slice(&1u16.to_le_bytes());

        let proof_offset = SHRED_PAYLOAD_SIZE - proof_size * SIZE_OF_MERKLE_PROOF_ENTRY;
        let our_leaf = leaf_hash(&buf[64..proof_offset]);
        let l0 = [0x11u8; 32];
        let l1 = [0x22u8; 32];
        let l2 = [0x44u8; 32];

        // leaf index 3 (odd at level 0, odd at level 1): level-0 sibling l2 on left, level-1 sibling
        // p01 on left.
        let p01 = join(&l0, &l1);
        let p23 = join(&l2, &our_leaf);
        let root = join(&p01, &p23);

        buf[proof_offset..proof_offset + 20].copy_from_slice(&l2[..20]);
        buf[proof_offset + 20..proof_offset + 40].copy_from_slice(&p01[..20]);

        let m = parse(&buf).expect("merkle code parses");
        assert_eq!(m.shred_type, ShredType::Code);
        assert_eq!(m.signed_message, root.to_vec());
    }

    #[test]
    fn bad_proof_depth_yields_none_root() {
        // proof_size that doesn't collapse the index to 0 -> no root -> parse returns None.
        let mut buf = blank_payload();
        // leaf index 5 needs >=3 proof entries to collapse; give it only 1.
        put_common(&mut buf, 0x80 | 1u8, 1, 5, 0);
        assert!(parse(&buf).is_none());
    }
}
