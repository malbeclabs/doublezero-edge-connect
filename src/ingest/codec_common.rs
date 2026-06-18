//! Shared primitives for the DoubleZero Edge family of binary protocols.
//!
//! Top-of-Book (`codec`), Midpoint (`codec_midpoint`) and Market-by-Order (`codec_mbo`) are
//! sibling protocols that share the same little-endian **24-byte frame header** and **4-byte
//! application message header**, differing only by the frame `magic` and the set of message
//! bodies they carry. This module holds those shared pieces plus a generic frame-walker each
//! codec parameterizes with its own per-type body decoder, so the header parse + length-walk
//! loop (and its bounds checks) is written and validated once.

use anyhow::{bail, Result};

pub const FRAME_HEADER_SIZE: usize = 24;
pub const MSG_HEADER_SIZE: usize = 4;

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
pub fn decode_frame_with<M>(
    buf: &[u8],
    magic: u16,
    mut decode_message: impl FnMut(u8, u16, &[u8], usize) -> M,
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
        messages.push(decode_message(msg_type, flags, buf, off));
        off += msg_len;
    }
    Ok((header, messages))
}
