#!/usr/bin/env python3
# Minimal, dependency-free WebSocket probe for the edge-connect WS sink.
#
# Connects to ws://127.0.0.1:$WS_QA_PORT (firehose — no subscribe frame), reads the
# on-connect replay + live stream, and exits 0 once it has seen BOTH an `instrument`
# definition (precision) AND at least one market-data message
# (quote/trade/midpoint/depth) within $WS_READ_TIMEOUT seconds. Exits 1 otherwise.
#
# Stdlib only (raw RFC6455 client) so it runs on any host that has python3 — the
# same interpreter the installer already depends on. Server->client data frames are
# unmasked; control frames (ping/pong/close) and non-`type` control acks are ignored.
import base64
import json
import os
import socket
import sys
import time

HOST = "127.0.0.1"
PORT = int(os.environ.get("WS_QA_PORT", "18081"))
DEADLINE = time.time() + float(os.environ.get("WS_READ_TIMEOUT", "25"))
DATA_TYPES = {"quote", "trade", "midpoint", "depth"}


def readn(sock, n, pre):
    """Read exactly n bytes, returning (chunk, leftover); honours the deadline."""
    buf = pre
    while len(buf) < n:
        remaining = DEADLINE - time.time()
        if remaining <= 0:
            raise TimeoutError("deadline reached")
        sock.settimeout(max(0.1, remaining))
        data = sock.recv(4096)
        if not data:
            raise EOFError("connection closed")
        buf += data
    return buf[:n], buf[n:]


def main():
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection((HOST, PORT), timeout=5)
    handshake = (
        "GET / HTTP/1.1\r\n"
        f"Host: {HOST}:{PORT}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(handshake.encode())

    buf = b""
    while b"\r\n\r\n" not in buf:
        if time.time() > DEADLINE:
            sys.exit("timed out on handshake")
        sock.settimeout(max(0.1, DEADLINE - time.time()))
        chunk = sock.recv(4096)
        if not chunk:
            sys.exit("no handshake response")
        buf += chunk
    status_line = buf.split(b"\r\n", 1)[0]
    if b" 101 " not in status_line:
        sys.exit(f"bad WS handshake: {status_line!r}")
    leftover = buf.split(b"\r\n\r\n", 1)[1]

    seen_instrument = False
    seen_data = False
    try:
        while time.time() < DEADLINE and not (seen_instrument and seen_data):
            hdr, leftover = readn(sock, 2, leftover)
            opcode = hdr[0] & 0x0F
            masked = hdr[1] & 0x80
            length = hdr[1] & 0x7F
            if length == 126:
                ext, leftover = readn(sock, 2, leftover)
                length = int.from_bytes(ext, "big")
            elif length == 127:
                ext, leftover = readn(sock, 8, leftover)
                length = int.from_bytes(ext, "big")
            mask = b""
            if masked:
                mask, leftover = readn(sock, 4, leftover)
            payload, leftover = readn(sock, length, leftover)
            if masked:
                payload = bytes(payload[i] ^ mask[i % 4] for i in range(length))
            if opcode == 0x8:  # close
                break
            if opcode in (0x9, 0xA):  # ping / pong
                continue
            if opcode in (0x0, 0x1):  # continuation / text
                try:
                    msg = json.loads(payload.decode("utf-8", "replace"))
                except ValueError:
                    continue
                mtype = msg.get("type")
                if mtype == "instrument":
                    seen_instrument = True
                elif mtype in DATA_TYPES:
                    seen_data = True
    except (TimeoutError, EOFError):
        pass

    print(json.dumps({"instrument": seen_instrument, "data": seen_data}))
    sys.exit(0 if (seen_instrument and seen_data) else 1)


if __name__ == "__main__":
    main()
