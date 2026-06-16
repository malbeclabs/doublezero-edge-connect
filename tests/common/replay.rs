//! Replay golden `.bin` captures as loopback UDP multicast.

/// Top-of-Book & Trades frame magic (ASCII "ZD" little-endian).
pub const TOB_MAGIC: u16 = 0x445A;
/// Market-by-Order frame magic.
pub const MBO_MAGIC: u16 = 0x4444;

/// Split a captured `.bin` (length-prefixed packet log) into individual frame byte-slices.
///
/// The file format is a sequence of `[u32 LE length][frame bytes]` records, as produced
/// by the publisher's `encode_packets` function. Each frame's first two bytes are the
/// little-endian protocol magic; bytes 22-23 are the little-endian total frame length
/// (equal to the outer length prefix). We assert both match, which doubles as a fixture
/// format check.
pub fn split_frames(bytes: &[u8], magic: u16) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        assert!(
            off + 4 <= bytes.len(),
            "fixture truncated: expected length prefix at offset {off}, only {} bytes remain",
            bytes.len() - off
        );
        let frame_len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]]) as usize;
        off += 4;
        assert!(
            frame_len >= 24 && off + frame_len <= bytes.len(),
            "frame at offset {}: bad frame_length {frame_len} (remaining {})",
            off - 4,
            bytes.len() - off
        );
        let got = u16::from_le_bytes([bytes[off], bytes[off + 1]]);
        assert_eq!(got, magic, "frame at offset {}: magic 0x{got:04X} != 0x{magic:04X}", off - 4);
        frames.push(bytes[off..off + frame_len].to_vec());
        off += frame_len;
    }
    frames
}
