//! ed25519 verification of a shred's signature against its slot leader.
//!
//! The signature (shred bytes `[0..64]`) is checked over [`ShredMeta::signed_message`] — the
//! post-signature payload for legacy shreds, the recomputed merkle root for merkle shreds (see
//! `parse.rs`). A bad/garbled pubkey or signature simply fails verification (never panics), which
//! the forwarder treats as "not a valid copy" under the prefer-valid rule.

use ed25519_dalek::{Signature, Verifier, VerifyingKey};

use super::parse::ShredMeta;

/// Verify `meta`'s signature against the 32-byte ed25519 `leader_pubkey`. Returns `false` on any
/// error (malformed key, malformed signature, signature mismatch).
pub fn verify(meta: &ShredMeta, leader_pubkey: &[u8; 32]) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(leader_pubkey) else {
        return false;
    };
    let sig = Signature::from_bytes(&meta.signature);
    vk.verify(&meta.signed_message, &sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shred::parse::ShredType;
    use ed25519_dalek::{Signer, SigningKey};

    fn meta_with_message(msg: &[u8], sig: [u8; 64]) -> ShredMeta {
        ShredMeta {
            slot: 1,
            index: 0,
            shred_type: ShredType::Data,
            signature: sig,
            signed_message: msg.to_vec(),
            resigned: false,
        }
    }

    #[test]
    fn accepts_a_good_signature_and_rejects_tampering() {
        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let pubkey = signing.verifying_key().to_bytes();
        let msg = b"the merkle root or legacy payload";
        let sig = signing.sign(msg).to_bytes();

        assert!(verify(&meta_with_message(msg, sig), &pubkey));

        // Wrong message under the same signature -> reject.
        assert!(!verify(
            &meta_with_message(b"different bytes", sig),
            &pubkey
        ));
        // Right message, wrong leader -> reject.
        let other = SigningKey::from_bytes(&[9u8; 32])
            .verifying_key()
            .to_bytes();
        assert!(!verify(&meta_with_message(msg, sig), &other));
    }

    #[test]
    fn rejects_garbage_pubkey_without_panicking() {
        // An all-FF "pubkey" is not a valid ed25519 point; verification must fail, not panic.
        let bad = [0xffu8; 32];
        assert!(!verify(&meta_with_message(b"x", [0u8; 64]), &bad));
    }
}
