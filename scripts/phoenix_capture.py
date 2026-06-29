#!/usr/bin/env python3
"""Concurrent capture + correlation of the DZ Edge Phoenix feed vs the public Phoenix API.

Records the edge multicast feed and the public Phoenix WebSocket at the SAME time, then checks
whether the bridge's trade-dedup assumptions hold against real data — primarily: does the public
`tradeSequenceNumber` equal the edge `trade_id` for the same fill (the dedup key)?

It removes operator guesswork that produced empty runs before:
  - it AUTO-DISCOVERS every active market from the public exchange (no need to pick symbols), and
    subscribes the whole board, so whatever trades during the window is captured;
  - it correlates edge<->public by ASSET ID (edge instrument_id == public assetId), which is robust
    to the edge feed's symbol namespacing (e.g. edge "hyna:BTC"/"xyz:TSLA" vs public bare "BTC"/"TSLA"),
    and it falls back to a namespace-normalized symbol join;
  - it surfaces the public subscription/control messages, so a zero-trade run explains itself;
  - it HARD-FAILS up front if the public side is requested but `websockets` is missing.

PREREQUISITES (run on a host with BOTH edge + public reach)
  - On the DZ Edge network, receiving the Phoenix multicast group (--group, default 233.84.178.18)
    on its edge interface (--iface, default doublezero1; pass a name, an IPv4, or 0.0.0.0).
  - Reach to https://perp-api.phoenix.trade (REST market list) and wss://perp-api.phoenix.trade/v1/ws.
  - Python 3.9+ and `pip install websockets`. No root needed. Passive (SO_REUSEPORT) — safe next to a
    running bridge/publisher.
  - Closed beta: if the API rejects, pass --ws-token <token> (and --markets-token if the REST needs it).

RUN (no need to choose markets — it discovers them)
    pip install websockets
    python3 scripts/phoenix_capture.py --iface doublezero1 --secs 180

  Optional: --markets SOL,BTC to restrict to specific public symbols. --secs longer = more trades +
  more complete refdata.  WHEN DONE, SEND BACK THE WHOLE --out DIRECTORY (correlation_report.txt first;
  the *.sample.bin + phoenix_tob_refdata.bin are small and committable; the big *.bin / *_index.jsonl
  stay on the host).

READING correlation_report.txt
  The SUMMARY names the best-correlated market and the #1 verdict. Per market it reports, joining by
  assetId==instrument_id (and by symbol as a cross-check):
    #1 (the gate) do public tradeSequenceNumber and edge trade_id overlap?  "OK" = dedup key aligns.
    #2           edge_aggressor -> public_side pairs (buy/sell <-> bid/ask).
    #3           the learned edge-symbol -> public-symbol mapping (reveals the namespacing rule).
    #5           duplicate-id check.
  A "DIAGNOSIS:" line and the captured public control messages explain any empty side.

OUTPUTS (in --out)
  correlation_report.txt, markets.json (discovered exchange list), public_raw.jsonl (every public msg),
  public_trades.jsonl, edge_trades.jsonl, phoenix_tob_{refdata,mktdata}.bin (raw frames),
  phoenix_tob_mktdata.sample.bin (small committable golden slice), {refdata,mktdata}_index.jsonl.

The .bin frames are the authoritative artifact (the bridge's Rust codec decodes them in tests);
the Python decode here is only for the live correlation.
"""

import argparse
import asyncio
import importlib.util
import json
import re
import socket
import struct
import subprocess
import threading
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

# --- Edge TOB wire format (authoritative source: src/ingest/codec.rs / codec_common.rs) ---
MAGIC = 0x445A
FRAME_HEADER_SIZE = 24
MSG_HEADER_SIZE = 4
MSG_INSTRUMENT_DEFINITION = 0x02
MSG_TRADE = 0x04

IP_ADD_MEMBERSHIP = getattr(socket, "IP_ADD_MEMBERSHIP", 35)
DEFAULT_MARKETS_URL = "https://perp-api.phoenix.trade/v1/view/exchange/markets"


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
    """Yield (msg_type, body_offset) per app message, mirroring codec_common::decode_frame_with."""
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


def norm_symbol(sym):
    """Edge symbols may be namespaced 'builder:TICKER'; public uses bare TICKER. Strip the prefix."""
    return sym.split(":", 1)[1] if sym and ":" in sym else sym


def resolve_iface_ip(iface):
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


def discover_markets(url, token, out):
    """GET the public exchange market list; return [(symbol, assetId, status)] and save markets.json.

    The endpoint returns a JSON array of market objects with `symbol`, `assetId`, `marketStatus`.
    """
    req_url = url + (("&" if "?" in url else "?") + "token=" + token) if token else url
    try:
        with urllib.request.urlopen(req_url, timeout=20) as resp:
            raw = resp.read()
    except Exception as e:
        print(f"!! could not fetch market list from {url}: {e}")
        return []
    (out / "markets.json").write_bytes(raw)
    try:
        data = json.loads(raw)
    except ValueError as e:
        print(f"!! market list is not JSON: {e}")
        return []
    items = data if isinstance(data, list) else data.get("markets", data.get("data", []))
    markets = []
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("symbol"), str):
            markets.append((it["symbol"], it.get("assetId"), it.get("marketStatus")))
    return markets


def capture_multicast(group, port, role, iface_ip, deadline, out, results):
    """Join one multicast group/port, append raw frames to a .bin, and decode them. Passive."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    s.bind(("", port))
    s.setsockopt(socket.IPPROTO_IP, IP_ADD_MEMBERSHIP, socket.inet_aton(group) + socket.inet_aton(iface_ip))
    s.settimeout(1.0)

    instruments = {}  # instrument_id -> def
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


async def capture_public(url, markets, token, deadline, out, results, max_subs):
    """Subscribe the public `trades` channel for every market; record trades + control messages."""
    import websockets  # presence guaranteed by main()'s preflight

    if token:
        url += ("&" if "?" in url else "?") + "token=" + token

    trades, control = [], []
    messages = 0
    raw_f = open(out / "public_raw.jsonl", "w")
    dec_f = open(out / "public_trades.jsonl", "w")
    subscribed = markets[:max_subs]
    try:
        async with websockets.connect(url, ping_interval=20, max_size=None, open_timeout=20) as ws:
            for m in subscribed:
                await ws.send(json.dumps({"type": "subscribe", "subscription": {"channel": "trades", "symbol": m}}))
            print(f"public WS connected: subscribed trades for {len(subscribed)} markets")
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
                    control.append(msg[:500])
                    continue
                if env.get("channel") == "trades":
                    for fill in env.get("trades", []):
                        rec = {
                            "recv_ts_ns": recv_ts,
                            "symbol": fill.get("symbol", env.get("symbol")),
                            "tradeSequenceNumber": fill.get("tradeSequenceNumber"),
                            "side": fill.get("side"),
                            "baseAmount": fill.get("baseAmount"),
                            "quoteAmount": fill.get("quoteAmount"),
                            "timestamp": fill.get("timestamp"),
                        }
                        trades.append(rec)
                        dec_f.write(json.dumps(rec) + "\n")
                else:
                    # subscription confirmations / errors / other channels — keep for diagnosis
                    if len(control) < 50:
                        control.append(env)
    except Exception as e:
        print(f"!! public WS error: {e}")
        control.append(f"<connection error: {e}>")
    finally:
        raw_f.close()
        dec_f.close()
    results["public"] = {"trades": trades, "messages": messages, "control": control, "subscribed": len(subscribed)}


def _as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def emit_marketdata_sample(out, max_bytes=65536):
    """Write a small committable golden slice: the first frames of the mktdata capture (real frames,
    decodable by the bridge codec), capped to ~max_bytes. Best-effort; never fatal."""
    try:
        idx_path, bin_path = out / "mktdata_index.jsonl", out / "phoenix_tob_mktdata.bin"
        if not (idx_path.exists() and bin_path.exists()):
            return
        lengths = []
        with open(idx_path) as f:
            for line in f:
                try:
                    lengths.append(json.loads(line)["len"])
                except (ValueError, KeyError):
                    break
        kept = total = 0
        for ln in lengths:
            if total + ln > max_bytes and kept > 0:
                break
            total += ln
            kept += 1
        with open(bin_path, "rb") as f:
            sample = f.read(total)
        (out / "phoenix_tob_mktdata.sample.bin").write_bytes(sample)
        print(f"wrote phoenix_tob_mktdata.sample.bin ({kept} frames, {len(sample)} bytes)")
    except Exception as e:
        print(f"!! sample emit skipped: {e}")


def correlate(results, market_list, out):
    """Join edge<->public by assetId==instrument_id (primary) and normalized symbol (fallback)."""
    instruments = {}
    for role in ("refdata", "mktdata"):
        instruments.update(results.get(role, {}).get("instruments", {}))
    edge_trades = results.get("mktdata", {}).get("trades", [])
    pub = results.get("public", {})
    pub_trades = pub.get("trades", [])

    sym2asset = {s: a for s, a, _ in market_list if a is not None}
    asset2sym = {a: s for s, a, _ in market_list if a is not None}

    # Edge trades grouped by instrument_id; resolve edge symbol where a def was captured.
    edge_by_id = defaultdict(list)
    for t in edge_trades:
        edge_by_id[t["instrument_id"]].append(t)
    edge_sym = {iid: instruments[iid]["symbol"] for iid in edge_by_id if iid in instruments}

    # Public trades grouped by assetId (via symbol -> assetId from the exchange list).
    pub_by_asset = defaultdict(list)
    pub_no_asset = 0
    for x in pub_trades:
        a = sym2asset.get(x.get("symbol"))
        if a is None:
            pub_no_asset += 1
        else:
            pub_by_asset[a].append(x)

    lines = []
    p = lines.append
    p("=" * 78)
    p("Phoenix edge-vs-public capture — correlation report")
    p("=" * 78)

    # ---- SUMMARY ----
    matched_assets = sorted(set(edge_by_id) & set(pub_by_asset), key=lambda a: -len(set(
        x["tradeSequenceNumber"] and _as_int(x["tradeSequenceNumber"]) for x in pub_by_asset[a]
    ) & {t["trade_id"] for t in edge_by_id[a]}))
    best = matched_assets[0] if matched_assets else None
    p("SUMMARY")
    p(f"  edge: {results.get('mktdata', {}).get('datagrams', 0)} mktdata + "
      f"{results.get('refdata', {}).get('datagrams', 0)} refdata datagrams, {len(edge_trades)} trades")
    p(f"  public: {pub.get('messages', 0)} msgs, {len(pub_trades)} fills, "
      f"{pub.get('subscribed', 0)} markets subscribed, {len(pub.get('control', []))} control msgs")
    p(f"  markets with trades on BOTH sides (assetId join): {len(matched_assets)}")
    if best is not None:
        e_ids = {t["trade_id"] for t in edge_by_id[best]}
        p_ids = {_as_int(x["tradeSequenceNumber"]) for x in pub_by_asset[best]} - {None}
        ov = e_ids & p_ids
        verdict = ("OK — tradeSequenceNumber == trade_id" if ov else "*** NO OVERLAP — keys differ")
        p(f"  best market: assetId={best} public={asset2sym.get(best)!r} edge={edge_sym.get(best, '(def not captured)')!r}"
          f" — [#1] {verdict} ({len(ov)} shared of edge {len(e_ids)}/public {len(p_ids)})")
    else:
        p("  [#1] INCONCLUSIVE: no market had trades on both sides in this window (see DIAGNOSIS).")
    p("")

    # ---- DIAGNOSIS ----
    if results.get("mktdata", {}).get("datagrams", 0) == 0:
        p("DIAGNOSIS: 0 edge mktdata datagrams — not on the DZ edge network, or wrong --iface/--group/--mktdata-port.")
    elif not edge_trades:
        p("DIAGNOSIS: edge frames arrived but no trade prints — quiet window or too-short --secs.")
    if pub.get("subscribed", 0) == 0:
        p("DIAGNOSIS: no public markets subscribed — market discovery failed (see markets.json) and no --markets given.")
    elif not pub_trades:
        p("DIAGNOSIS: public subscribed but 0 trade fills — see the control messages below (subscribe rejected?), "
          "or the board was quiet. Public control messages:")
        for c in pub.get("control", [])[:5]:
            p(f"    {json.dumps(c)[:300]}")
    if pub_no_asset:
        p(f"NOTE: {pub_no_asset} public fills had a symbol not in the exchange list (couldn't map to assetId).")
    p("")

    # ---- PER-MARKET (assetId join), sorted by overlap ----
    p("PER-MARKET (join: edge instrument_id == public assetId)")
    rows = []
    for a in set(edge_by_id) & set(pub_by_asset):
        e_ids = {t["trade_id"] for t in edge_by_id[a]}
        p_ids = {_as_int(x["tradeSequenceNumber"]) for x in pub_by_asset[a]} - {None}
        rows.append((len(e_ids & p_ids), a, e_ids, p_ids))
    for ov_n, a, e_ids, p_ids in sorted(rows, key=lambda r: -r[0])[:20]:
        e_by = {t["trade_id"]: t for t in edge_by_id[a]}
        p_by = {_as_int(x["tradeSequenceNumber"]): x for x in pub_by_asset[a]}
        both = e_ids & p_ids
        sides = Counter((e_by[i]["aggressor"], p_by[i].get("side")) for i in both)
        win = Counter("edge" if e_by[i]["recv_ts_ns"] <= p_by[i]["recv_ts_ns"] else "public" for i in both)
        verdict = "OK" if both else "no overlap"
        p(f"  assetId={a} public={asset2sym.get(a)!r} edge={edge_sym.get(a, '(uncaptured)')!r}: "
          f"edge={len(e_ids)} public={len(p_ids)} overlap={ov_n} [#1]{verdict}")
        if both:
            p(f"      [#2] {', '.join(f'{k[0]}->{k[1]}:{v}' for k, v in sides.items())}  "
              f"[win] edge {win['edge']}/public {win['public']}")
    if not rows:
        p("  (no asset-id matches — falling back to symbol join below)")
    p("")

    # ---- Learned symbol mapping (#3), from id-matched markets ----
    mapping = sorted({(edge_sym[a], asset2sym.get(a)) for a in (set(edge_by_id) & set(pub_by_asset))
                      if a in edge_sym and asset2sym.get(a)})
    p("[#3] learned edge-symbol -> public-symbol (from assetId-matched markets):")
    for e_s, p_s in mapping[:40]:
        flag = "" if norm_symbol(e_s) == p_s else "   <-- not a clean namespace-strip"
        p(f"    {e_s!r} -> {p_s!r}{flag}")
    if not mapping:
        p("    (none — either no overlap, or edge defs for matched markets weren't captured)")
    p("")

    # ---- Symbol-join fallback (only informative if the id join found nothing) ----
    if not any(r[0] for r in rows):
        edge_by_norm = defaultdict(set)
        for iid, ts in edge_by_id.items():
            s = norm_symbol(edge_sym.get(iid, ""))
            if s:
                edge_by_norm[s] |= {t["trade_id"] for t in ts}
        pub_by_sym = defaultdict(set)
        for x in pub_trades:
            sid = _as_int(x["tradeSequenceNumber"])
            if x.get("symbol") and sid is not None:
                pub_by_sym[x["symbol"]].add(sid)
        p("FALLBACK (join: normalized edge symbol == public symbol)")
        any_ov = False
        for s in sorted(set(edge_by_norm) & set(pub_by_sym), key=lambda s: -len(edge_by_norm[s] & pub_by_sym[s])):
            ov = edge_by_norm[s] & pub_by_sym[s]
            if ov:
                any_ov = True
            p(f"  {s!r}: edge={len(edge_by_norm[s])} public={len(pub_by_sym[s])} overlap={len(ov)}")
        if not any_ov:
            p("  no overlap by symbol either — likely no shared market traded, or trade ids are not the same space.")
        p("")

    # ---- source_id distribution (Phoenix-only vs superset) + refdata completeness ----
    src = Counter(t["source_id"] for t in edge_trades)
    p(f"edge source_id distribution (2=Phoenix): {dict(src)}")
    traded_ids = set(edge_by_id)
    resolved = sum(1 for i in traded_ids if i in instruments)
    p(f"refdata completeness: {resolved}/{len(traded_ids)} traded instrument_ids had a captured definition"
      + ("" if resolved == len(traded_ids) else "  (increase --secs for a fuller manifest)"))

    report = "\n".join(lines)
    (out / "correlation_report.txt").write_text(report + "\n")
    print("\n" + report)
    print(f"\nartifacts in {out}")


def main():
    ap = argparse.ArgumentParser(
        description="Concurrent Phoenix edge-vs-public capture + correlation. See the module docstring "
        "for prerequisites, what to send back, and how to read the report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="example:\n  pip install websockets\n  python3 scripts/phoenix_capture.py --iface doublezero1 --secs 180\n\n"
        "Markets are auto-discovered from the public exchange; --markets SOL,BTC restricts them.\n"
        "Send back the whole --out directory. Closed beta? add --ws-token <token>.",
    )
    ap.add_argument("--group", default="233.84.178.18", help="edge Phoenix multicast group")
    ap.add_argument("--mktdata-port", type=int, default=9201)
    ap.add_argument("--refdata-port", type=int, default=9202)
    ap.add_argument("--iface", default="doublezero1", help="interface name, IPv4, or 0.0.0.0 (default iface)")
    ap.add_argument("--ws-url", default="wss://perp-api.phoenix.trade/v1/ws")
    ap.add_argument("--markets-url", default=DEFAULT_MARKETS_URL, help="REST endpoint listing exchange markets")
    ap.add_argument("--markets", default="", help="restrict to these PUBLIC symbols (default: all active, discovered)")
    ap.add_argument("--max-subs", type=int, default=1000, help="cap on public subscriptions")
    ap.add_argument("--ws-token", default="", help="optional ?token= for the closed-beta WS")
    ap.add_argument("--markets-token", default="", help="optional ?token= for the REST market list")
    ap.add_argument("--secs", type=int, default=180, help="capture duration")
    ap.add_argument("--out", default="", help="output dir (default ./phoenix-capture-<epoch>)")
    ap.add_argument("--edge-only", action="store_true", help="skip the public side (no websockets needed)")
    args = ap.parse_args()

    # Preflight: fail fast rather than silently capturing edge-only when public was intended.
    if not args.edge_only and importlib.util.find_spec("websockets") is None:
        raise SystemExit(
            "ERROR: the public side needs `websockets` (run `pip install websockets`).\n"
            "       Re-run after installing, or pass --edge-only to capture edge frames without the correlation."
        )

    iface_ip = resolve_iface_ip(args.iface)
    out = Path(args.out) if args.out else Path(f"phoenix-capture-{int(time.time())}")
    out.mkdir(parents=True, exist_ok=True)

    market_list = []
    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    if not args.edge_only:
        market_list = discover_markets(args.markets_url, args.markets_token, out)
        active = [s for s, _, st in market_list if st in (None, "active")]
        if markets:
            active = [s for s in active if s in markets] or markets  # honor override
        markets = active
        if not markets:
            print("!! no markets to subscribe (discovery failed and no --markets) — capturing edge-only")

    deadline = time.time() + args.secs
    print(f"capturing {args.secs}s -> {out}\n  edge: group {args.group} ports "
          f"{args.mktdata_port}(mktdata)/{args.refdata_port}(refdata) on {iface_ip}\n"
          f"  public: {args.ws_url}  ({len(markets)} markets discovered)" if not args.edge_only
          else f"capturing {args.secs}s -> {out} (edge-only)")

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

    if not args.edge_only and markets:
        asyncio.run(capture_public(args.ws_url, markets, args.ws_token, deadline, out, results, args.max_subs))
    else:
        results.setdefault("public", {"trades": [], "messages": 0, "control": [], "subscribed": 0})
        time.sleep(max(0, deadline - time.time()))

    for t in threads:
        t.join(timeout=10)

    correlate(results, market_list, out)
    emit_marketdata_sample(out)


if __name__ == "__main__":
    main()
