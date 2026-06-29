#!/usr/bin/env python3
"""Concurrent capture + correlation of the DZ Edge Phoenix feed vs the public Phoenix API.

This is the "capture #3" from the Phoenix trade-backstop design: it records the edge
multicast feed and the public WebSocket *at the same time* for the same markets, so the
same fills appear in both and the dedup assumptions can be verified against real data.

It produces, in --out:
  - phoenix_tob_refdata.bin     raw edge refdata frames (port 9202) -> commit as a bridge fixture
  - phoenix_tob_marketdata.bin  raw edge mktdata frames (port 9201) -> commit as a bridge fixture
  - {refdata,mktdata}_index.jsonl  per-datagram recv timestamps/lengths
  - edge_trades.jsonl           decoded edge Trade messages (symbol, trade_id, source_ts, side, px, qty)
  - public_raw.jsonl            every public WS message verbatim (+ recv ts) -> schema confirmation
  - public_trades.jsonl         decoded public fills (symbol, tradeSequenceNumber, side, amounts, ts)
  - correlation_report.txt      the verification answers (below)

Closes the spec's verification items by matching identical fills across both sources:
  #1 (BLOCKER) public tradeSequenceNumber == edge trade_id  -> dedup key alignment
  #2           side orientation (edge buy/sell <-> public bid/ask)
  #3           symbol mapping (edge "SOL-PERP" <-> public "SOL")
  #5           1:1 element <-> trade_id (no per-id aggregation on either side)
  bonus        who delivered each shared fill first (edge vs public latency / "win rate")

The raw .bin frames are the authoritative artifact: the bridge's Rust codec (ingest::codec)
decodes them in tests. The Python decode here is only for the live correlation report.

PREREQUISITES (read before running on a new host)
  - Run on a host that (a) is on the DZ Edge network and receives the Phoenix multicast group
    (--group, default 233.84.178.18) on its edge interface, AND (b) can reach
    wss://perp-api.phoenix.trade. It is passive (read-only, SO_REUSEPORT) — safe to run next to a
    bridge/publisher already on that host; it won't disturb them.
  - Python 3.9+, and `pip install websockets` (only the public side needs it). No root required
    (ports 9201/9202 are unprivileged; multicast join is unprivileged).
  - --iface is the interface carrying the edge multicast (the DZ tunnel, default `doublezero1`);
    pass an interface NAME, an IPv4 address, or 0.0.0.0 (kernel default iface). Find it with
    `ip -4 -o addr show`.
  - The public API is in CLOSED BETA: if it rejects the connection or returns nothing, you likely
    need an access token — pass `--ws-token <token>` (appended as ?token=...; adjust the flag if
    their auth scheme differs).

RUN
    pip install websockets
    python3 scripts/phoenix_capture.py --iface doublezero1 --markets SOL,BTC --secs 120

  --markets are PUBLIC base symbols (SOL, BTC — NOT the edge "SOL-PERP"); pick ones actively trading
  and make --secs long enough to catch real fills (a few minutes on a busy market).
  WHEN DONE, SEND BACK THE WHOLE --out DIRECTORY (correlation_report.txt first, plus the two .bin files).

READING correlation_report.txt
  #1 is the gate: "OK" = public tradeSequenceNumber matches edge trade_id (the dedup key aligns);
  "*** BLOCKER" = zero overlap, so the bridge would emit duplicate trades — stop and report that.
  #2/#3/#5 confirm side (buy/sell<->bid/ask), symbol mapping, and 1:1 ids. A "DIAGNOSIS:" line
  appears if either side produced nothing, with the likely cause.

TROUBLESHOOTING
  - report shows "mktdata=0" datagrams      -> not receiving multicast: wrong --iface, not on the DZ
    edge network, or wrong --group/--mktdata-port. Confirm the bridge on this host sees Phoenix.
  - datagrams > 0 but "edge trades: 0"      -> frames arrive but no trade prints in the window:
    quiet market or too-short --secs; pick a busier --markets / increase --secs.
  - "public msgs: 0" or a connect error     -> markets not trading, wrong symbol, or the closed
    beta needs --ws-token.

Requires Python 3.9+ and the `websockets` package (public side only).
"""

import argparse
import asyncio
import json
import re
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

# --- Edge TOB wire format (authoritative source: src/ingest/codec.rs / codec_common.rs) ---
MAGIC = 0x445A
FRAME_HEADER_SIZE = 24
MSG_HEADER_SIZE = 4
MSG_INSTRUMENT_DEFINITION = 0x02
MSG_TRADE = 0x04

IP_ADD_MEMBERSHIP = getattr(socket, "IP_ADD_MEMBERSHIP", 35)


def _u16(b, o):
    return int.from_bytes(b[o : o + 2], "little") if o + 2 <= len(b) else None


def _u32(b, o):
    return int.from_bytes(b[o : o + 4], "little") if o + 4 <= len(b) else None


def _u64(b, o):
    return int.from_bytes(b[o : o + 8], "little") if o + 8 <= len(b) else None


def _i64(b, o):
    v = _u64(b, o)
    return None if v is None else v - (1 << 64) if v >= (1 << 63) else v


def _i8(b, o):
    if o >= len(b):
        return None
    v = b[o]
    return v - 256 if v >= 128 else v


def _cstr(b, o, n):
    if o + n > len(b):
        return None
    field = b[o : o + n]
    end = field.find(0)
    return field[: end if end >= 0 else n].decode("utf-8", "replace")


def walk_messages(buf):
    """Yield (msg_type, body_offset) for each app message in one frame, mirroring
    codec_common::decode_frame_with (length-walk with the same bounds checks)."""
    if len(buf) < FRAME_HEADER_SIZE or _u16(buf, 0) != MAGIC:
        return
    msg_count = buf[20]
    frame_len = min(_u16(buf, 22) or 0, len(buf))
    off = FRAME_HEADER_SIZE
    for _ in range(msg_count):
        if off + MSG_HEADER_SIZE > frame_len:
            break
        msg_type = buf[off]
        msg_len = buf[off + 1]
        if msg_len < MSG_HEADER_SIZE or off + msg_len > frame_len:
            break
        yield msg_type, off + MSG_HEADER_SIZE
        off += msg_len


def decode_instrument_def(b, body):
    return {
        "instrument_id": _u32(b, body),
        "symbol": _cstr(b, body + 4, 16),
        "price_exponent": _i8(b, body + 37),
        "qty_exponent": _i8(b, body + 38),
    }


def decode_trade(b, body):
    return {
        "instrument_id": _u32(b, body),
        "source_id": _u16(b, body + 4),
        "aggressor": {1: "buy", 2: "sell"}.get(b[body + 6] if body + 6 < len(b) else 0, "unknown"),
        "source_ts_ns": _u64(b, body + 8),
        "trade_price_raw": _i64(b, body + 16),
        "trade_qty_raw": _u64(b, body + 24),
        "trade_id": _u64(b, body + 32),
    }


def resolve_iface_ip(iface):
    """An IPv4 literal (or 0.0.0.0) is used as-is; an interface name is resolved via `ip`."""
    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", iface):
        return iface
    try:
        out = subprocess.run(
            ["ip", "-4", "-o", "addr", "show", "dev", iface],
            capture_output=True, text=True, timeout=5,
        ).stdout
        m = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", out)
        if m:
            return m.group(1)
    except Exception:
        pass
    raise SystemExit(f"could not resolve an IPv4 for --iface {iface!r}; pass an IP (or 0.0.0.0)")


def capture_multicast(group, port, role, iface_ip, deadline, out, results):
    """Join one multicast group/port, append raw frames to a .bin, and decode them.

    Passive + SO_REUSEPORT, so it co-receives alongside a running bridge without disrupting it.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    s.bind(("", port))
    s.setsockopt(socket.IPPROTO_IP, IP_ADD_MEMBERSHIP, socket.inet_aton(group) + socket.inet_aton(iface_ip))
    s.settimeout(1.0)

    instruments = {}  # instrument_id -> {symbol, exponents}
    trades = []
    n = 0
    with open(out / f"phoenix_tob_{role}.bin", "wb") as binf, open(out / f"{role}_index.jsonl", "w") as idx:
        while time.time() < deadline:
            try:
                data, addr = s.recvfrom(65535)
            except socket.timeout:
                continue
            recv_ts = time.time_ns()
            binf.write(data)
            n += 1
            idx.write(json.dumps({"recv_ts_ns": recv_ts, "src": addr[0], "len": len(data)}) + "\n")
            for msg_type, body in walk_messages(data):
                if msg_type == MSG_INSTRUMENT_DEFINITION:
                    d = decode_instrument_def(data, body)
                    if d["instrument_id"] is not None and d["symbol"]:
                        instruments[d["instrument_id"]] = d
                elif msg_type == MSG_TRADE:
                    t = decode_trade(data, body)
                    t["recv_ts_ns"] = recv_ts
                    trades.append(t)
    s.close()
    results[role] = {"datagrams": n, "instruments": instruments, "trades": trades}


async def capture_public(url, markets, token, deadline, out, results):
    """Subscribe the public `trades` channel for each market and record + decode every message."""
    try:
        import websockets
    except ImportError:
        print("!! public capture skipped: `pip install websockets` to enable it")
        results["public"] = {"trades": [], "messages": 0, "skipped": True}
        return

    if token:
        url += ("&" if "?" in url else "?") + "token=" + token

    trades = []
    messages = 0
    raw_f = open(out / "public_raw.jsonl", "w")
    dec_f = open(out / "public_trades.jsonl", "w")
    try:
        async with websockets.connect(url, ping_interval=20, max_size=None, open_timeout=15) as ws:
            for m in markets:
                await ws.send(json.dumps({"type": "subscribe", "subscription": {"channel": "trades", "symbol": m}}))
            print(f"public WS connected: {url}  (subscribed trades for {markets})")
            while time.time() < deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5))
                except asyncio.TimeoutError:
                    continue
                recv_ts = time.time_ns()
                messages += 1
                raw_f.write(json.dumps({"recv_ts_ns": recv_ts, "msg": msg}) + "\n")
                try:
                    env = json.loads(msg)
                except (ValueError, TypeError):
                    continue
                if env.get("channel") != "trades":
                    continue
                for fill in env.get("trades", []):
                    rec = {
                        "recv_ts_ns": recv_ts,
                        "symbol": fill.get("symbol", env.get("symbol")),
                        "tradeSequenceNumber": fill.get("tradeSequenceNumber"),
                        "side": fill.get("side"),
                        "baseAmount": fill.get("baseAmount"),
                        "quoteAmount": fill.get("quoteAmount"),
                        "timestamp": fill.get("timestamp"),
                        "slot": fill.get("slot"),
                        "slotIndex": fill.get("slotIndex"),
                    }
                    trades.append(rec)
                    dec_f.write(json.dumps(rec) + "\n")
    except Exception as e:  # best-effort: a public-side failure must not lose the edge capture
        print(f"!! public WS error: {e}")
    finally:
        raw_f.close()
        dec_f.close()
    results["public"] = {"trades": trades, "messages": messages, "skipped": False}


def _as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def correlate(results, markets, strip_suffix, out):
    """Match edge trades to public fills by id and write the verification report."""
    edge = results.get("mktdata", {})
    instruments = {}
    for role in ("refdata", "mktdata"):
        instruments.update(results.get(role, {}).get("instruments", {}))

    # Resolve edge trade symbols + public-base mapping (edge "SOL-PERP" -> public "SOL").
    edge_trades = []
    for t in edge.get("trades", []):
        inst = instruments.get(t["instrument_id"])
        sym = inst["symbol"] if inst else f"id={t['instrument_id']}"
        base = sym[: -len(strip_suffix)] if strip_suffix and sym.endswith(strip_suffix) else sym
        edge_trades.append({**t, "symbol": sym, "base": base})

    pub_trades = results.get("public", {}).get("trades", [])

    lines = []
    p = lines.append
    p("=" * 78)
    p("Phoenix edge-vs-public capture — correlation report")
    p("=" * 78)
    p(f"edge instruments seen: {sorted({i['symbol'] for i in instruments.values()})}")
    p(f"edge datagrams: refdata={results.get('refdata', {}).get('datagrams', 0)} "
      f"mktdata={results.get('mktdata', {}).get('datagrams', 0)}")
    p(f"edge trades: {len(edge_trades)}   public fills: {len(pub_trades)}   "
      f"public msgs: {results.get('public', {}).get('messages', 0)}")
    pub = results.get("public", {})
    if results.get("mktdata", {}).get("datagrams", 0) == 0:
        p("DIAGNOSIS: 0 edge mktdata datagrams — this host may not be on the DZ edge network, or "
          "--iface/--group/--mktdata-port are wrong (see TROUBLESHOOTING in the script header).")
    elif not edge_trades:
        p("DIAGNOSIS: edge frames arrived but no trade prints in the window — quiet market or a "
          "too-short --secs; pick a busier --markets / increase --secs.")
    if pub.get("skipped"):
        p("DIAGNOSIS: public capture skipped (websockets not installed) — `pip install websockets` and re-run.")
    elif pub.get("messages", 0) == 0:
        p("DIAGNOSIS: public API produced 0 messages — check --markets are valid & trading, or the "
          "closed beta may require --ws-token.")
    p("")

    bases = markets or sorted({e["base"] for e in edge_trades} | {p_.get("symbol") for p_ in pub_trades})
    for base in bases:
        e_ids = [e["trade_id"] for e in edge_trades if e["base"] == base and e["trade_id"] is not None]
        p_ids = [_as_int(x["tradeSequenceNumber"]) for x in pub_trades if x.get("symbol") == base]
        p_ids = [x for x in p_ids if x is not None]
        e_set, p_set = set(e_ids), set(p_ids)
        both = e_set & p_set
        p(f"market {base!r}:")
        p(f"  edge trade_ids={len(e_ids)} (distinct {len(e_set)}), "
          f"public seq#={len(p_ids)} (distinct {len(p_set)})")
        # #5 1:1 — duplicate ids within a source means per-id aggregation differs.
        if len(e_ids) != len(e_set):
            p(f"  [#5] WARNING: edge has duplicate trade_ids ({len(e_ids) - len(e_set)} dups)")
        if len(p_ids) != len(p_set):
            p(f"  [#5] WARNING: public has duplicate tradeSequenceNumbers ({len(p_ids) - len(p_set)} dups)")
        # #1 the blocker — do the id spaces actually overlap?
        if not e_set or not p_set:
            p("  [#1] inconclusive: one side produced no trades for this market in the window")
        elif both:
            p(f"  [#1] OK: {len(both)} shared id(s) -> tradeSequenceNumber == trade_id holds on the overlap")
            p(f"       (edge-only {len(e_set - p_set)}, public-only {len(p_set - e_set)} — "
              f"expected at the capture-window edges)")
        else:
            p("  [#1] *** BLOCKER: zero shared ids — public tradeSequenceNumber does NOT equal edge")
            p(f"       trade_id. Dedup would fail. edge sample={sorted(e_set)[:5]} public sample={sorted(p_set)[:5]}")
        # #2 side orientation + bonus latency, over the shared ids.
        e_by_id = {e["trade_id"]: e for e in edge_trades if e["base"] == base}
        p_by_id = {}
        for x in pub_trades:
            if x.get("symbol") == base:
                p_by_id.setdefault(_as_int(x["tradeSequenceNumber"]), x)
        side_pairs, edge_first, public_first = {}, 0, 0
        for tid in both:
            e, x = e_by_id[tid], p_by_id[tid]
            side_pairs[(e["aggressor"], x.get("side"))] = side_pairs.get((e["aggressor"], x.get("side")), 0) + 1
            if e["recv_ts_ns"] <= x["recv_ts_ns"]:
                edge_first += 1
            else:
                public_first += 1
        if side_pairs:
            p(f"  [#2] observed (edge_aggressor -> public_side) pairs: "
              + ", ".join(f"{k[0]}->{k[1]}: {v}" for k, v in sorted(side_pairs.items())))
            p(f"  [win] edge first {edge_first} / public first {public_first} "
              f"({100 * edge_first / max(1, edge_first + public_first):.0f}% edge — user-space recv ts, approximate)")
        p("")

    # #3 symbol mapping evidence.
    p(f"[#3] edge symbols: {sorted({e['symbol'] for e in edge_trades})}")
    p(f"[#3] public symbols: {sorted({x.get('symbol') for x in pub_trades if x.get('symbol')})}")
    p(f"[#3] strip rule applied to edge symbols: remove suffix {strip_suffix!r}")

    report = "\n".join(lines)
    (out / "correlation_report.txt").write_text(report + "\n")
    print("\n" + report)
    print(f"\nartifacts written to {out}")


def main():
    ap = argparse.ArgumentParser(
        description="Concurrent Phoenix edge-vs-public capture + correlation. See the module "
        "docstring at the top of this file for prerequisites, what to send back, and troubleshooting.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="example:\n"
        "  pip install websockets\n"
        "  python3 scripts/phoenix_capture.py --iface doublezero1 --markets SOL,BTC --secs 120\n\n"
        "--markets are PUBLIC base symbols (SOL, not SOL-PERP). Send back the whole --out directory.\n"
        "Closed beta? add --ws-token <token>.  Not receiving? check --iface / that this host is on DZ Edge.",
    )
    ap.add_argument("--group", default="233.84.178.18", help="edge Phoenix multicast group")
    ap.add_argument("--mktdata-port", type=int, default=9201)
    ap.add_argument("--refdata-port", type=int, default=9202)
    ap.add_argument("--iface", default="doublezero1", help="interface name, IPv4, or 0.0.0.0 (default iface)")
    ap.add_argument("--ws-url", default="wss://perp-api.phoenix.trade/v1/ws")
    ap.add_argument("--markets", default="", help="comma-separated PUBLIC base symbols, e.g. SOL,BTC")
    ap.add_argument("--ws-token", default="", help="optional ?token= for the closed-beta API")
    ap.add_argument("--strip-suffix", default="-PERP", help="edge->public symbol mapping: suffix to strip")
    ap.add_argument("--secs", type=int, default=120, help="capture duration")
    ap.add_argument("--out", default="", help="output dir (default ./phoenix-capture-<epoch>)")
    args = ap.parse_args()

    iface_ip = resolve_iface_ip(args.iface)
    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    out = Path(args.out) if args.out else Path(f"phoenix-capture-{int(time.time())}")
    out.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + args.secs
    print(f"capturing {args.secs}s -> {out}\n  edge: group {args.group} ports "
          f"{args.mktdata_port}(mktdata)/{args.refdata_port}(refdata) on {iface_ip}\n"
          f"  public: {args.ws_url} markets={markets or '(none — pass --markets to enable)'}")

    results = {}
    threads = [
        threading.Thread(target=capture_multicast,
                         args=(args.group, args.mktdata_port, "mktdata", iface_ip, deadline, out, results),
                         daemon=True),
        threading.Thread(target=capture_multicast,
                         args=(args.group, args.refdata_port, "refdata", iface_ip, deadline, out, results),
                         daemon=True),
    ]
    for t in threads:
        t.start()

    if markets:
        asyncio.run(capture_public(args.ws_url, markets, args.ws_token, deadline, out, results))
    else:
        print("!! no --markets: skipping public capture (edge-only). Pass --markets to correlate.")
        results["public"] = {"trades": [], "messages": 0, "skipped": True}
        # Still wait out the window so the edge .bin captures a useful sample.
        time.sleep(max(0, deadline - time.time()))

    for t in threads:
        t.join(timeout=10)

    correlate(results, markets, args.strip_suffix, out)


if __name__ == "__main__":
    main()
