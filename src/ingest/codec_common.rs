//! Shared primitives for the DoubleZero Edge family of binary protocols.
//!
//! Top-of-Book (`codec`), Midpoint (`codec_midpoint`) and Market-by-Order (`codec_mbo`) are
//! sibling protocols that share the same little-endian **24-byte frame header** and **4-byte
//! application message header**, differing only by the frame `magic` and the set of message
//! bodies they carry. This module holds those shared pieces plus a generic frame-walker each
//! codec parameterizes with its own per-type body decoder, so the header parse + length-walk
//! loop (and its bounds checks) is written and validated once.

use std::sync::Arc;

use anyhow::{bail, Result};

pub const FRAME_HEADER_SIZE: usize = 24;
pub const MSG_HEADER_SIZE: usize = 4;

/// Wire generations this crate implements, as carried in the frame header's `Schema Version`.
///
/// `2` is deliberately absent. It widened `InstrumentDefinition`'s `Symbol` to `char[64]`, but
/// `3.0.0` inserted `Source ID` after `Instrument ID` before any publisher shipped v2, so
/// publishers move from `1.x` straight to `3.x` and a v2 frame is never emitted. Decoding a
/// generation nothing produces would be an unexercised, unvalidated path that every future change
/// has to keep correct; the specs require rejecting a version we do not implement, so we do.
pub const SCHEMA_V1: u8 = 1;
pub const SCHEMA_V3: u8 = 3;

/// `InstrumentDefinition` `Symbol` widths and body lengths per generation, excluding the 4-byte
/// message header. Named to mirror `instDefSymLenV1/V3` and `instDefBodyLenV1/V3` in the reference
/// decoder, which is the oracle these were validated against.
const DEF_SYM_LEN_V1: usize = 16;
const DEF_SYM_LEN_V3: usize = 64;
const DEF_BODY_LEN_V1: usize = 76;
const DEF_BODY_LEN_V3: usize = 126;

/// The 24-byte frame header common to every edge-feed-spec protocol. Several fields are decoded
/// for byte-for-byte fidelity with the reference codec even though no consumer reads them yet.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence: u64,
    pub send_ts: u64,
    pub msg_count: u8,
    pub reset_count: u8,
    pub frame_length: u16,
}

// Little-endian fixed-width readers. All are **bounds-checked**: an out-of-range offset yields
// `None` rather than panicking, so a truncated or malformed datagram can never index past the
// buffer. The per-message body decoders thread the `None` through `?` and fall back to
// `Message::Other` (see `decode_frame_with`), so a runt message is skipped, not a crash.
#[inline]
pub fn u8le(b: &[u8], o: usize) -> Option<u8> {
    b.get(o).copied()
}
#[inline]
pub fn u16le(b: &[u8], o: usize) -> Option<u16> {
    b.get(o..o + 2)?.try_into().ok().map(u16::from_le_bytes)
}
#[inline]
pub fn u32le(b: &[u8], o: usize) -> Option<u32> {
    b.get(o..o + 4)?.try_into().ok().map(u32::from_le_bytes)
}
#[inline]
pub fn u64le(b: &[u8], o: usize) -> Option<u64> {
    b.get(o..o + 8)?.try_into().ok().map(u64::from_le_bytes)
}
#[inline]
pub fn i64le(b: &[u8], o: usize) -> Option<i64> {
    u64le(b, o).map(|v| v as i64)
}

/// Apply a raw price/qty integer's implied decimal exponent (e.g. `6788`, `-2` -> `67.88`).
/// Shared by every protocol's normalization.
pub fn apply_exponent(raw: i64, exponent: i8) -> f64 {
    raw as f64 * 10f64.powi(exponent as i32)
}

/// Decode a fixed-width, NUL-padded ASCII symbol field `b[start..start+len]` to a `String`,
/// stopping at the first NUL. Shared by the instrument-definition decoders. Bounds-checked:
/// returns `None` when the field runs past the buffer, so a truncated definition is skipped
/// rather than panicking.
pub fn cstr(b: &[u8], start: usize, len: usize) -> Option<String> {
    let field = b.get(start..start + len)?;
    let end = field.iter().position(|&c| c == 0).unwrap_or(field.len());
    Some(String::from_utf8_lossy(&field[..end]).to_string())
}

/// The reference-data `InstrumentDefinition`, shared by every feed whose layout widened together
/// (Top-of-Book, Market-by-Order, Market-by-Price). Midpoint keeps its own: its variant is a
/// different 64-byte message whose fourth field is `Default Method`, not `Qty Exponent`.
///
/// `source_id` is `Option` rather than a sentinel because its absence is a permanent property of a
/// v1 frame, not a missing value — a consumer that must handle "this publisher predates
/// per-instrument source attribution" should be made to see that in the type.
#[derive(Debug, Clone)]
pub struct InstrumentDefinition {
    pub instrument_id: u32,
    pub source_id: Option<u16>,
    pub symbol: Arc<str>,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub manifest_seq: u16,
}

impl crate::ingest::subscriber::InstrumentDef for InstrumentDefinition {
    fn id(&self) -> u32 {
        self.instrument_id
    }
    fn manifest_seq(&self) -> u16 {
        self.manifest_seq
    }
}

/// Decode one `InstrumentDefinition` at the layout its frame's `Schema Version` declares.
///
/// Takes the **message** offset (where the 4-byte message header starts), not the body offset, so
/// it can read the declared `Message Length` itself rather than depending on a caller convention.
///
/// A generation this crate does not implement yields `None`, independently of whether the frame
/// gate already rejected it: nothing should depend on that call order, and this decoder has to be
/// correct on its own.
pub fn instrument_definition(
    b: &[u8],
    msg_offset: usize,
    schema_version: u8,
) -> Option<InstrumentDefinition> {
    let body = msg_offset + MSG_HEADER_SIZE;

    // Cross-check the declared length against the declared version. The readers below are bounded
    // by the buffer, not by this message, so without this a v1 body in a frame that continues past
    // it would read the next message's bytes at the v3 offsets and yield a plausible instrument
    // rather than an error.
    //
    // **At least**, not exactly: every spec promises forward compatibility inside a MAJOR line and
    // classifies appending a field within the declared `Message Length` as a MINOR change that must
    // keep working. Requiring equality would take the feed dark on a conformant `3.1.0` frame whose
    // definition grew. The lenient direction still catches the dangerous case, since a short body
    // fails the check whatever the version claims.
    let declared_body = (u8le(b, msg_offset + 1)? as usize).checked_sub(MSG_HEADER_SIZE)?;
    let required_body = match schema_version {
        SCHEMA_V1 => DEF_BODY_LEN_V1,
        SCHEMA_V3 => DEF_BODY_LEN_V3,
        _ => return None,
    };
    if declared_body < required_body {
        return None;
    }

    match schema_version {
        SCHEMA_V1 => Some(InstrumentDefinition {
            instrument_id: u32le(b, body)?,
            source_id: None,
            symbol: cstr(b, body + 4, DEF_SYM_LEN_V1)?.into(),
            price_exponent: u8le(b, body + 37)? as i8,
            qty_exponent: u8le(b, body + 38)? as i8,
            manifest_seq: u16le(b, body + 74)?,
        }),
        SCHEMA_V3 => Some(InstrumentDefinition {
            instrument_id: u32le(b, body)?,
            source_id: Some(u16le(b, body + 4)?),
            symbol: cstr(b, body + 6, DEF_SYM_LEN_V3)?.into(),
            price_exponent: u8le(b, body + 87)? as i8,
            qty_exponent: u8le(b, body + 88)? as i8,
            manifest_seq: u16le(b, body + 124)?,
        }),
        _ => None,
    }
}

/// Decode one UDP datagram (one frame) into its header and application messages, using the
/// caller's expected `magic` and per-type body decoder.
///
/// `decode_message(msg_type, flags, buf, msg_offset)` is invoked once per application message;
/// `msg_offset` points at that message's 4-byte header, so the body starts at
/// `msg_offset + MSG_HEADER_SIZE`. Unknown/oversized messages stop the walk (mirrors the
/// reference codec): the loop advances by the declared length and bails out on a truncated or
/// nonsensical length rather than reading past the frame.
///
/// The declared `msg_len` only bounds the *advance*; it is not trusted to match the type's actual
/// field layout (a hostile or corrupt frame can under-declare it). The body decoders therefore read
/// every field through the bounds-checked LE readers above, so a message that is shorter than its
/// type requires decodes to `Message::Other` (skipped) instead of indexing past the buffer.
///
/// `supported_versions` lists the wire generations the calling feed implements. It is a set rather
/// than a ceiling because the implemented generations are not contiguous — see [`SCHEMA_V1`] — and
/// it lives here rather than in each codec so that a codec cannot ship without a gate, which is
/// exactly how two of them did.
pub fn decode_frame_with<M>(
    buf: &[u8],
    magic: u16,
    supported_versions: &[u8],
    mut decode_message: impl FnMut(u8, u16, &[u8], usize, u8) -> M,
) -> Result<(FrameHeader, Vec<M>)> {
    if buf.len() < FRAME_HEADER_SIZE {
        bail!("datagram too short: {} bytes", buf.len());
    }
    // Every offset below is within the 24-byte header guaranteed present by the length check above,
    // so the bounds-checked readers always yield `Some`; `unwrap_or(0)` is a panic-free formality.
    let got_magic = u16le(buf, 0).unwrap_or(0);
    if got_magic != magic {
        bail!("bad magic 0x{got_magic:04X} (expected 0x{magic:04X})");
    }
    let header = FrameHeader {
        schema_version: buf[2],
        channel_id: buf[3],
        sequence: u64le(buf, 4).unwrap_or(0),
        send_ts: u64le(buf, 12).unwrap_or(0),
        msg_count: buf[20],
        reset_count: buf[21],
        frame_length: u16le(buf, 22).unwrap_or(0),
    };
    // `Magic` and `Schema Version` do different jobs and both are mandatory: magic answers "is this
    // the feed I subscribed to?", the version answers "is this a wire format I implement?". A
    // version bump is the explicit signal that field offsets can no longer be trusted, so an
    // unimplemented one is rejected outright rather than parsed best-effort at some other layout.
    if !supported_versions.contains(&header.schema_version) {
        bail!(
            "unsupported schema version {} (implemented: {:?})",
            header.schema_version,
            supported_versions
        );
    }
    let frame_len = (header.frame_length as usize).min(buf.len());

    let mut messages = Vec::with_capacity(header.msg_count as usize);
    let mut off = FRAME_HEADER_SIZE;
    for _ in 0..header.msg_count {
        if off + MSG_HEADER_SIZE > frame_len {
            break;
        }
        let msg_type = buf[off];
        let msg_len = buf[off + 1] as usize;
        let flags = u16le(buf, off + 2).unwrap_or(0);
        if msg_len < MSG_HEADER_SIZE || off + msg_len > frame_len {
            break;
        }
        messages.push(decode_message(
            msg_type,
            flags,
            buf,
            off,
            header.schema_version,
        ));
        off += msg_len;
    }
    Ok((header, messages))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAGIC: u16 = 0x445A;
    const MSG_INSTRUMENT_DEFINITION: u8 = 0x02;

    /// Build a frame carrying exactly one application message.
    fn frame(schema_version: u8, msg_type: u8, body: &[u8]) -> Vec<u8> {
        let mut f = vec![0u8; FRAME_HEADER_SIZE];
        f[0..2].copy_from_slice(&TEST_MAGIC.to_le_bytes());
        f[2] = schema_version;
        f[20] = 1; // msg_count
        f.push(msg_type);
        f.push((MSG_HEADER_SIZE + body.len()) as u8);
        f.extend_from_slice(&0u16.to_le_bytes()); // flags
        f.extend_from_slice(body);
        let len = f.len() as u16;
        f[22..24].copy_from_slice(&len.to_le_bytes());
        f
    }

    /// A v1 `InstrumentDefinition` body (76 bytes), written at the offsets the **spec** gives,
    /// not at the implementation's constants — so a transposed offset fails instead of agreeing
    /// with itself. Spec offsets are message-relative; subtract the 4-byte header for these.
    fn def_body_v1(
        id: u32,
        symbol: &str,
        price_exp: i8,
        qty_exp: i8,
        manifest_seq: u16,
    ) -> Vec<u8> {
        let mut b = vec![0u8; 76];
        b[0..4].copy_from_slice(&id.to_le_bytes()); // spec 4: Instrument ID
        b[4..4 + symbol.len()].copy_from_slice(symbol.as_bytes()); // spec 8: Symbol char[16]
        b[37] = price_exp as u8; // spec 41
        b[38] = qty_exp as u8; // spec 42
        b[74..76].copy_from_slice(&manifest_seq.to_le_bytes()); // spec 78
        b
    }

    /// A v3 `InstrumentDefinition` body (126 bytes). `Source ID` is inserted after `Instrument ID`,
    /// so `Symbol` starts at spec offset 10 and every later field sits two bytes further along than
    /// it did under v2.
    fn def_body_v3(
        id: u32,
        source_id: u16,
        symbol: &str,
        price_exp: i8,
        qty_exp: i8,
        manifest_seq: u16,
    ) -> Vec<u8> {
        let mut b = vec![0u8; 126];
        b[0..4].copy_from_slice(&id.to_le_bytes()); // spec 4: Instrument ID
        b[4..6].copy_from_slice(&source_id.to_le_bytes()); // spec 8: Source ID
        b[6..6 + symbol.len()].copy_from_slice(symbol.as_bytes()); // spec 10: Symbol char[64]
        b[87] = price_exp as u8; // spec 91
        b[88] = qty_exp as u8; // spec 92
        b[124..126].copy_from_slice(&manifest_seq.to_le_bytes()); // spec 128
        b
    }

    /// The v3 layout decodes at v3 offsets, and carries the `Source ID` that is the entire reason
    /// for the release. A symbol longer than v1's 16-byte field proves the widened field is really
    /// being read rather than the old one happening to line up.
    #[test]
    fn instrument_definition_decodes_schema_v3() {
        let long = "KXNCAAFGAME-26AUG15DALSEA-SEA";
        let body = def_body_v3(41, 3, long, -8, -6, 7);
        let f = frame(3, MSG_INSTRUMENT_DEFINITION, &body);

        let d = instrument_definition(&f, FRAME_HEADER_SIZE, 3).expect("v3 definition decodes");

        assert_eq!(d.instrument_id, 41);
        assert_eq!(d.source_id, Some(3));
        assert_eq!(
            &*d.symbol, long,
            "the full 64-byte symbol, not a 16-byte prefix"
        );
        assert_eq!(d.price_exponent, -8);
        assert_eq!(d.qty_exponent, -6);
        assert_eq!(d.manifest_seq, 7);
    }

    /// v1 keeps decoding at its own offsets. This is the backward-compatibility half of the
    /// feature: publishers roll to v3 one at a time, so both layouts are live at once and a host
    /// may hold a v1 and a v3 publisher of the same venue simultaneously.
    #[test]
    fn instrument_definition_decodes_schema_v1() {
        let body = def_body_v1(41, "BTC-USDT", -8, -6, 7);
        let f = frame(1, MSG_INSTRUMENT_DEFINITION, &body);

        let d = instrument_definition(&f, FRAME_HEADER_SIZE, 1).expect("v1 definition decodes");

        assert_eq!(d.instrument_id, 41);
        assert_eq!(d.source_id, None, "v1 carries no per-instrument source id");
        assert_eq!(&*d.symbol, "BTC-USDT");
        assert_eq!(d.price_exponent, -8);
        assert_eq!(d.qty_exponent, -6);
        assert_eq!(d.manifest_seq, 7);
    }

    /// A v1-length definition that claims v3 is rejected, not read at v3 offsets.
    ///
    /// Bounds-checking alone does not catch this. The readers are bounded by the *buffer*, not by
    /// the message's declared `Message Length`, so when more frame follows the definition — the
    /// normal case, since definitions are packed several to a frame — reading at the v3 offsets
    /// succeeds and consumes the *next* message's bytes as this one's symbol and exponents. The
    /// result is a plausible instrument rather than an error, with a garbage `price_exponent`
    /// silently scaling every price for it by the wrong power of ten.
    ///
    /// So the declared length is cross-checked against the version. This is the trailing-bytes
    /// case specifically: a short body with nothing after it already fails the bounds checks.
    #[test]
    fn a_v1_length_definition_claiming_v3_is_rejected() {
        let mut f = frame(
            3,
            MSG_INSTRUMENT_DEFINITION,
            &def_body_v1(41, "BTC-USDT", -8, -6, 7),
        );
        // Whatever the publisher packed next. Non-zero, so a v3-offset read yields values rather
        // than tripping over NUL padding and looking like an empty field.
        f.extend_from_slice(&[0xAA; 96]);
        let len = f.len() as u16;
        f[22..24].copy_from_slice(&len.to_le_bytes());

        assert!(
            instrument_definition(&f, FRAME_HEADER_SIZE, 3).is_none(),
            "a 76-byte body cannot be a v3 definition, whatever the frame claims"
        );
    }

    /// The frame gate admits exactly the generations a feed implements, and **`2` is not one of
    /// them** — it is a real hole in the supported set, not a range endpoint. v2 widened `Symbol`
    /// but was superseded by v3 before any publisher shipped it, so no conformant publisher emits
    /// it and decoding it would be an unexercised path nothing validates.
    #[test]
    fn the_frame_gate_admits_only_implemented_versions() {
        let body = def_body_v3(41, 3, "BTC-USDT", -8, -6, 7);
        let supported = &[SCHEMA_V1, SCHEMA_V3];

        for v in [0u8, 2, 4, 255] {
            let f = frame(v, MSG_INSTRUMENT_DEFINITION, &body);
            assert!(
                decode_frame_with(&f, TEST_MAGIC, supported, |_, _, _, _, _| ()).is_err(),
                "schema version {v} must be rejected, not parsed at some other layout"
            );
        }
        for v in [SCHEMA_V1, SCHEMA_V3] {
            let f = frame(v, MSG_INSTRUMENT_DEFINITION, &body);
            assert!(
                decode_frame_with(&f, TEST_MAGIC, supported, |_, _, _, _, _| ()).is_ok(),
                "schema version {v} must be accepted"
            );
        }
    }

    /// A feed's supported set is its own. Midpoint kept a slimmer 64-byte definition when its
    /// siblings widened, so it stayed at Schema Version 1 and must reject a v3 frame — the version
    /// byte is per-feed, not global.
    #[test]
    fn a_feed_implementing_only_v1_rejects_v3() {
        let f = frame(
            SCHEMA_V3,
            MSG_INSTRUMENT_DEFINITION,
            &def_body_v3(41, 3, "BTC-USDT", -8, -6, 7),
        );
        assert!(decode_frame_with(&f, TEST_MAGIC, &[SCHEMA_V1], |_, _, _, _, _| ()).is_err());
    }

    /// The backward-compatibility claim, stated directly: one logical instrument encoded under
    /// each generation decodes to the same thing, apart from the `source_id` v1 cannot carry.
    ///
    /// This is the test that fails if a later edit fixes one layout and forgets the other — the
    /// failure mode a shared decoder exists to prevent, and the reason the two arms are not allowed
    /// to drift into separate files again.
    #[test]
    fn both_generations_decode_the_same_instrument_identically() {
        let v1 = instrument_definition(
            &frame(
                SCHEMA_V1,
                MSG_INSTRUMENT_DEFINITION,
                &def_body_v1(41, "BTC-USDT", -8, -6, 7),
            ),
            FRAME_HEADER_SIZE,
            SCHEMA_V1,
        )
        .expect("v1");
        let v3 = instrument_definition(
            &frame(
                SCHEMA_V3,
                MSG_INSTRUMENT_DEFINITION,
                &def_body_v3(41, 9, "BTC-USDT", -8, -6, 7),
            ),
            FRAME_HEADER_SIZE,
            SCHEMA_V3,
        )
        .expect("v3");

        assert_eq!(v1.instrument_id, v3.instrument_id);
        assert_eq!(v1.symbol, v3.symbol);
        assert_eq!(v1.price_exponent, v3.price_exponent);
        assert_eq!(v1.qty_exponent, v3.qty_exponent);
        assert_eq!(v1.manifest_seq, v3.manifest_seq);
        assert_eq!(
            (v1.source_id, v3.source_id),
            (None, Some(9)),
            "the only field that may differ across generations"
        );
    }
}
