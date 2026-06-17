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
        let frame_len =
            u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
                as usize;
        off += 4;
        assert!(
            frame_len >= 24 && off + frame_len <= bytes.len(),
            "frame at offset {}: bad frame_length {frame_len} (remaining {})",
            off - 4,
            bytes.len() - off
        );
        let got = u16::from_le_bytes([bytes[off], bytes[off + 1]]);
        assert_eq!(
            got,
            magic,
            "frame at offset {}: magic 0x{got:04X} != 0x{magic:04X}",
            off - 4
        );
        frames.push(bytes[off..off + frame_len].to_vec());
        off += frame_len;
    }
    frames
}

use std::net::{Ipv4Addr, SocketAddrV4};

use socket2::{Domain, Protocol, Socket, Type};

/// The real Hyperliquid mainnet-beta multicast group. Multicast routing is by interface,
/// not address class. The bridge joins this group on INADDR_ANY (the default interface,
/// see `bridge.rs`), and the sender enables multicast loopback, so locally-sent datagrams
/// reach the bridge in the same container/CI network namespace WITHOUT depending on the
/// `lo` interface having the MULTICAST flag (it usually does not inside a container).
pub const HYPERLIQUID_GROUP: Ipv4Addr = Ipv4Addr::new(233, 84, 178, 15);

/// Sender configured exactly like the HL publisher's own loopback E2E test (which runs in
/// their Linux CI): bound to UNSPECIFIED, multicast loopback on, TTL 1, and NO pinned
/// outgoing interface — let the kernel route the multicast and loop it back locally. Do
/// NOT `set_multicast_if_v4(LOCALHOST)`; pinning `lo` is the portability trap.
fn multicast_sender() -> std::io::Result<Socket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_multicast_loop_v4(true)?;
    sock.set_multicast_ttl_v4(1)?;
    sock.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into())?;
    Ok(sock)
}

/// Send each frame as one UDP datagram to `(group, port)`, with a tiny inter-packet gap so
/// the bridge's single-threaded decode keeps up.
pub fn send_frames(group: Ipv4Addr, port: u16, frames: &[Vec<u8>]) -> std::io::Result<()> {
    let sock = multicast_sender()?;
    let dst: socket2::SockAddr = SocketAddrV4::new(group, port).into();
    for f in frames {
        sock.send_to(f, &dst)?;
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
    Ok(())
}
