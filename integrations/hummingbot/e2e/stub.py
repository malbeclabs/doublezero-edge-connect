"""A stand-in for edge-connect and for Hyperliquid's public API, so the E2E run needs no network.

`/` is edge-connect's normalized WebSocket, playing a synthetic Hyperliquid session built to
PROTOCOL.md. `/info` and `/ws` answer what Hyperliquid's perpetual connector asks of the venue
itself. `/report` lists every subscription each side received and flags the ones it does not serve.
"""

import argparse
import asyncio
import json
import random
import time

from aiohttp import web

VENUE = "HYPERLIQUID"
PROTOCOL_KINDS = {"instrument", "quote", "trade", "midpoint", "depth", "book", "order_book", "status"}
FILTER_KEYS = {"source_name", "venue", "source", "source_id", "symbol", "channel", "type"}
# symbol: (source_id, channel, instrument_id, mid). ETH is in the catalog but traded by no pair.
MARKETS = {"BTC": (1, 0, 0, 95000.0), "ETH": (1, 0, 1, 3500.0), "xyz:XYZ100": (7, 0, 0, 25000.0)}
STREAMED = ("BTC", "xyz:XYZ100")
REBASELINE_EVERY_EVENTS = 100

report = {"edge": {"clients": 0, "subscriptions": [], "unserved": []},
          "venue": {"subscriptions": [], "unexpected": [], "info": []}}


def header(kind, symbol):
    source_id, channel, instrument_id, _ = MARKETS[symbol]
    now = time.time_ns()
    return {"type": kind, "venue": VENUE, "source_name": VENUE, "source_id": source_id, "symbol": symbol,
            "channel": channel, "instrument_id": instrument_id, "source_ts_ns": now, "recv_ts_ns": now,
            "kernel_rx_ts_ns": 0, "ws_send_ts_ns": now}


def instrument(symbol):
    return dict(header("instrument", symbol), price_exponent=-1, qty_exponent=-5, tick_size=0)


class Book:
    """One market's resting orders, moved by a seeded random walk so every run plays the same session."""

    def __init__(self, symbol, seed):
        self.symbol, self.mid = symbol, MARKETS[symbol][3]
        self.random = random.Random(seed)
        self.orders, self.next_id = {}, 1
        for level in range(1, 11):
            for _ in range(3):
                self.add("bid")
                self.add("ask")

    def add(self, side):
        level = self.random.randint(1, 10)
        price = self.mid - level if side == "bid" else self.mid + level
        self.orders[self.next_id] = (side, price, round(self.random.uniform(0.01, 1.0), 5))
        self.next_id += 1
        return self.change(self.next_id - 1)

    def change(self, order_id, size=None):
        side, price, resting = self.orders[order_id]
        size = resting if size is None else size
        return {"action": "update" if size else "delete", "side": side, "price": price, "size": size,
                "order_id": order_id}

    def remove(self, order_id):
        change = self.change(order_id, 0.0)
        del self.orders[order_id]
        return change

    def batch(self, changes, last=True, snapshot=False):
        return dict(header("order_book", self.symbol), changes=changes, snapshot=snapshot, last=last)

    def rebaseline(self, drop=False):
        if drop:  # as a failover to a publisher missing an order would
            del self.orders[self.random.choice(list(self.orders))]
        changes = [{"action": "clear", "side": "both", "price": 0.0, "size": 0.0, "order_id": 0}]
        changes += [self.change(order_id) for order_id in sorted(self.orders)]
        return self.batch(changes, snapshot=True)

    def event(self):
        """One venue event, split over two batches a third of the time as on a batching channel."""
        changes = []
        for _ in range(self.random.randint(1, 3)):
            sides = [side for side, _, _ in self.orders.values()]
            thin = min(("bid", "ask"), key=sides.count)
            action = self.random.random()
            if action < 0.4 or sides.count(thin) < 5:
                changes.append(self.add(thin if sides.count(thin) < 5 else self.random.choice(("bid", "ask"))))
                continue
            order_id = self.random.choice(list(self.orders))
            if action < 0.7 or self.orders[order_id][2] < 0.002:
                changes.append(self.remove(order_id))
            else:
                side, price, size = self.orders[order_id]
                self.orders[order_id] = (side, price, round(size / 2, 5))
                changes.append(self.change(order_id))
        if len(changes) > 1 and self.random.random() < 0.33:
            return [self.batch(changes[:1], last=False), self.batch(changes[1:])]
        return [self.batch(changes)]

    def trade(self, trade_id):
        side = self.random.choice(("buy", "sell"))
        prices = [price for s, price, _ in self.orders.values() if s == ("ask" if side == "buy" else "bid")]
        price = min(prices) if side == "buy" else max(prices)
        return dict(header("trade", self.symbol), price=price, size=0.01, aggressor_side=side,
                    trade_id=trade_id, cumulative_volume=0.0)


def served(subscription):
    return (set(subscription) <= FILTER_KEYS
            and subscription.get("source_name", VENUE) == VENUE
            and subscription.get("symbol", "BTC") in MARKETS
            and subscription.get("type", "status") in PROTOCOL_KINDS)


async def edge_connect(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    report["edge"]["clients"] += 1
    books = {symbol: Book(symbol, seed) for seed, symbol in enumerate(MARKETS)}
    # The connect-time replay is unfiltered: every definition, then every book.
    for symbol in MARKETS:
        await ws.send_json(instrument(symbol))
    for book in books.values():
        await ws.send_json(book.rebaseline())
    live = asyncio.ensure_future(stream(ws, [books[symbol] for symbol in STREAMED]))
    try:
        async for message in ws:
            frame = json.loads(message.data)
            subscription = frame.get("subscription", {})
            report["edge"]["subscriptions"].append(frame)
            if frame.get("method") == "ping":
                await ws.send_json({"channel": "pong"})
            elif frame.get("method") in ("subscribe", "unsubscribe") and served(subscription):
                await ws.send_json({"channel": "subscription_response", "method": frame["method"],
                                    "subscription": subscription})
                symbol = subscription.get("symbol")
                if frame["method"] == "subscribe" and symbol in books and subscription.get("type") == "order_book":
                    await ws.send_json(books[symbol].rebaseline())
            else:
                report["edge"]["unserved"].append(frame)
                await ws.send_json({"channel": "error", "error": "unrecognized message"})
    finally:
        live.cancel()
    return ws


async def stream(ws, books):
    trade_id = 1_000_000
    for count in range(1, 1_000_000):
        await asyncio.sleep(0.1)
        for book in books:
            for batch in book.event():
                await ws.send_json(batch)
            if count % 3 == 0:
                trade_id += 1
                await ws.send_json(book.trade(trade_id))
            if count % REBASELINE_EVERY_EVENTS == 0:
                await ws.send_json(book.rebaseline(drop=True))


def asset_ctx(symbol, mark):
    oracle = MARKETS[symbol][3]
    return {"funding": "0.0000125", "openInterest": "1000.0", "prevDayPx": f"{oracle:.1f}",
            "dayNtlVlm": "1.0", "premium": "0.0", "oraclePx": f"{oracle:.1f}", "markPx": f"{mark:.1f}",
            "midPx": f"{mark:.1f}", "impactPxs": [f"{mark:.1f}", f"{mark:.1f}"], "dayBaseVlm": "1.0"}


def dex(name):
    """A perp dex's `[meta, asset contexts]`; the empty name is Hyperliquid's own."""
    symbols = [symbol for symbol in MARKETS if (symbol.split(":")[0] if ":" in symbol else "") == name]
    meta = {"universe": [{"name": symbol, "szDecimals": 5, "maxLeverage": 40} for symbol in symbols]}
    return [meta, [asset_ctx(symbol, MARKETS[symbol][3]) for symbol in symbols]]


async def info(request):
    body = await request.json()
    kind = body.get("type")
    report["venue"]["info"].append(kind)
    if kind == "meta":
        return web.json_response(dex("")[0])
    if kind == "metaAndAssetCtxs":
        return web.json_response(dex(body.get("dex", "")))
    if kind == "allPerpMetas":
        return web.json_response([dex(""), dex("xyz")])
    report["venue"]["unexpected"].append(body)
    return web.json_response({"error": f"the stub serves no {kind!r}"}, status=400)


async def hyperliquid_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    feeds = []
    try:
        async for message in ws:
            frame = json.loads(message.data)
            report["venue"]["subscriptions"].append(frame)
            subscription = frame.get("subscription", {})
            if frame.get("method") == "ping":
                await ws.send_json({"channel": "pong"})
            elif subscription.get("type") == "activeAssetCtx" and subscription.get("coin") in MARKETS:
                await ws.send_json({"channel": "subscriptionResponse", "data": frame})
                feeds.append(asyncio.ensure_future(funding(ws, subscription["coin"])))
            else:
                report["venue"]["unexpected"].append(frame)
    finally:
        for feed in feeds:
            feed.cancel()
    return ws


async def funding(ws, coin):
    mark = MARKETS[coin][3]
    while True:
        await asyncio.sleep(1)
        mark += 0.5
        await ws.send_json({"channel": "activeAssetCtx", "data": {"coin": coin, "ctx": asset_ctx(coin, mark)}})


async def show_report(request):
    return web.json_response(report)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18081)
    app = web.Application()
    app.router.add_get("/", edge_connect)
    app.router.add_post("/info", info)
    app.router.add_get("/ws", hyperliquid_ws)
    app.router.add_get("/report", show_report)
    web.run_app(app, host="127.0.0.1", port=parser.parse_args().port)


if __name__ == "__main__":
    main()
