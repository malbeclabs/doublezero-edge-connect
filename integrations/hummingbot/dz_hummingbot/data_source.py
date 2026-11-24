"""Order-book data sources that read edge-connect instead of the venue."""

import asyncio
import time
from collections import defaultdict, deque
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

from .assembler import BookAssembler, Level, MarketKey, market_key
from .client import EdgeConnectClient

# A pair's subscriptions, each one of the few hundred edge-connect allows a client.
KINDS = ("order_book", "trade")
SNAPSHOT_POLL_SECONDS = 0.5
WAIT_WARNING_SECONDS = 30.0
# How long a symbol may stay absent from edge-connect's catalog before its pair is refused; a bridge
# that has just started names an instrument only once it has priced it.
NOT_CARRIED_AFTER_SECONDS = 60.0


def _seconds(ts_ns: int) -> float:
    return ts_ns * 1e-9 if ts_ns else time.time()


class DoubleZeroOrderBookDataSource(OrderBookTrackerDataSource):
    """Serves a connector's order books and trades from edge-connect.

    The connector's own data source (`venue`) is kept for what edge-connect does not carry.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: Any,
        venue: OrderBookTrackerDataSource,
        url: str,
        source_name: str,
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._venue = venue
        self._source_name = source_name
        self._client = EdgeConnectClient(url, api_factory)
        self._assembler = BookAssembler()
        self._symbols: Dict[str, str] = {}
        self._pairs: Dict[str, str] = {}
        # The market each pair's book follows; diffs and trades flow only once its first snapshot is out.
        self._served: Dict[str, MarketKey] = {}
        # Per pair rather than per market, so a pair that moves to another market keeps rising.
        self._update_ids: Dict[str, int] = defaultdict(int)
        # Pairs added after start-up, which leave the connector's list again if edge-connect refuses them.
        self._added: Set[str] = set()
        self._carried: Set[str] = set()
        # Per connection: markets seen, requests awaiting a reply in order, acknowledged and refused subscriptions.
        self._markets: Dict[str, Set[MarketKey]] = defaultdict(set)
        self._requests: Deque[Tuple[str, dict]] = deque()
        self._subscribed: Set[str] = set()
        self._live: Set[Tuple[str, str]] = set()
        self._refused: Dict[str, str] = {}
        self._catalog_read_at: Optional[float] = None

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._venue.get_last_traded_prices(trading_pairs=trading_pairs, domain=domain)

    async def listen_for_subscriptions(self):
        await self._client.run(self._on_connect, self._on_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # The tracker replays its last 32 diffs over any later snapshot, resurrecting levels a
        # re-baseline removed, so only a pair's first book is a snapshot and none is fetched hourly.
        await asyncio.Event().wait()

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if await self._resolve(trading_pair) is None:
            return False
        self.add_trading_pair(trading_pair)
        self._added.add(trading_pair)
        await self._ensure_subscribed(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        self.remove_trading_pair(trading_pair)
        self._added.discard(trading_pair)
        served = self._served.pop(trading_pair, None)
        symbol = self._symbols.pop(trading_pair, None)
        if symbol is not None:
            del self._pairs[symbol]
            for key in self._markets.pop(symbol, set()) | {served}:
                self._assembler.forget(key)
            self._refused.pop(symbol, None)
            if symbol in self._subscribed:
                self._subscribed.discard(symbol)
                for kind in KINDS:
                    self._live.discard((symbol, kind))
                    await self._request("unsubscribe", {"source_name": self._source_name, "symbol": symbol, "type": kind})
        return True

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._symbol(trading_pair)
        warned = time.monotonic()
        while True:
            await self._ensure_subscribed(trading_pair)
            refusal = self._refusal(trading_pair, symbol)
            if refusal is not None:
                if trading_pair in self._added:
                    await self.unsubscribe_from_trading_pair(trading_pair)
                raise ValueError(refusal)
            key = next(iter(self._markets.get(symbol, ())), None)
            if (key is not None and self._assembler.is_complete(key) and self._client.connected
                    and all((symbol, kind) in self._live for kind in KINDS)):
                bids, asks = self._assembler.snapshot(key)
                self._served[trading_pair] = key
                return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
                    "trading_pair": trading_pair, "update_id": self._update_ids[trading_pair],
                    "bids": bids, "asks": asks,
                }, timestamp=time.time())
            if time.monotonic() - warned >= WAIT_WARNING_SECONDS:
                warned = time.monotonic()
                self.logger().warning(f"Still waiting for edge-connect's {symbol!r} book for {trading_pair}: "
                                      f"{self._waiting_on(symbol)}")
            await self._sleep(SNAPSHOT_POLL_SECONDS)

    def _refusal(self, trading_pair: str, symbol: str) -> Optional[str]:
        markets = self._markets.get(symbol, ())
        if symbol in self._refused:
            return f"edge-connect at {self._client.url} refused {trading_pair}: {self._refused[symbol]}"
        if len(markets) > 1:
            return (f"{len(markets)} edge-connect {self._source_name} markets share the symbol {symbol!r}; "
                    f"{trading_pair} cannot be served from one of them")
        read = self._catalog_read_at
        if read is not None and time.monotonic() - read >= NOT_CARRIED_AFTER_SECONDS and symbol not in self._carried:
            return (f"edge-connect at {self._client.url} carries no {self._source_name} market named {symbol!r}, "
                    f"so {trading_pair} has no book")
        return None

    def _waiting_on(self, symbol: str) -> str:
        if not self._client.connected:
            return f"not connected to {self._client.url}"
        if symbol not in self._carried:
            return "the instrument is not defined yet"
        return "no complete book yet; is the bridge subscribed to the venue's order-book feed?"

    async def _parse_order_book_diff_message(self, raw_message: OrderBookMessage, message_queue: asyncio.Queue):
        message_queue.put_nowait(raw_message)

    async def _parse_trade_message(self, raw_message: OrderBookMessage, message_queue: asyncio.Queue):
        message_queue.put_nowait(raw_message)

    async def _symbol(self, trading_pair: str) -> str:
        # edge-connect names a market by the venue's own symbol, which the connector already maps.
        symbol = self._symbols.get(trading_pair)
        if symbol is None:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            self._symbols[trading_pair] = symbol
            self._pairs[symbol] = trading_pair
        return symbol

    async def _resolve(self, trading_pair: str) -> Optional[str]:
        try:
            return await self._symbol(trading_pair)
        except KeyError:
            self.logger().error(f"The connector has no exchange symbol for {trading_pair}, so it is not subscribed")
            return None

    async def _request(self, method: str, subscription: dict):
        if self._client.connected:
            self._requests.append((method, subscription))
            await self._client.send(method, subscription)

    async def _ensure_subscribed(self, trading_pair: str):
        symbol = self._symbols[trading_pair]
        if self._client.connected and symbol not in self._subscribed:
            self._subscribed.add(symbol)
            for kind in KINDS:
                await self._request("subscribe", {"source_name": self._source_name, "symbol": symbol, "type": kind})

    async def _on_connect(self):
        for state in (self._markets, self._requests, self._subscribed, self._live, self._refused):
            state.clear()
        self._catalog_read_at = None
        await self._request("subscribe", {"source_name": self._source_name, "type": "status"})
        for trading_pair in list(self._trading_pairs):
            if await self._resolve(trading_pair) is not None:
                await self._ensure_subscribed(trading_pair)

    def _on_message(self, msg: Dict[str, Any]):
        kind = msg.get("type")
        if kind is None:
            self._on_control(msg)
        elif (msg.get("source_name") or msg.get("venue", "")).upper() != self._source_name:
            return
        elif kind == "instrument":
            self._carried.add(msg["symbol"])
        elif kind in ("book", "order_book"):
            self._on_book(msg)
        elif kind == "trade":
            self._on_trade(msg)
        elif kind == "status":
            log = self.logger().warning if msg.get("state") == "down" else self.logger().info
            log(f"edge-connect reports {self._source_name} source {msg.get('source_id')} {msg.get('state')}")

    def _on_control(self, msg: Dict[str, Any]):
        channel = msg.get("channel")
        if channel not in ("subscription_response", "error"):
            return
        # edge-connect answers each control frame once, in order, and only after its connect-time
        # replay, so the first answer also means the whole catalog has been read.
        method, subscription = self._requests.popleft() if self._requests else ("", {})
        self._catalog_read_at = self._catalog_read_at or time.monotonic()
        symbol = subscription.get("symbol")
        if channel == "subscription_response":
            if method == "subscribe" and symbol is not None:
                self._live.add((symbol, subscription.get("type")))
            return
        if method == "subscribe" and symbol in self._pairs:
            self._refused[symbol] = msg.get("error")
        self.logger().error(f"edge-connect at {self._client.url} refused {method} {subscription}: {msg.get('error')}")

    def _on_book(self, msg: Dict[str, Any]):
        symbol = msg["symbol"]
        trading_pair = self._pairs.get(symbol)
        if trading_pair is None:
            return
        key = market_key(msg)
        markets = self._markets[symbol]
        new_market = key not in markets
        markets.add(key)
        self._carried.add(symbol)
        event = self._assembler.apply(msg)
        served = self._served.get(trading_pair)
        if served == key:
            if event is not None:
                self._send_diff(trading_pair, event.bids, event.asks, event.source_ts_ns)
        elif served is not None and served not in markets and self._catalog_read_at and self._assembler.is_complete(key):
            # The served market was not in this connection's catalog and this one now carries the symbol.
            self.logger().warning(f"edge-connect now serves {symbol!r} as {key}; {trading_pair} follows it from {served}")
            bids, asks = self._assembler.replace(served, key)
            self._served[trading_pair] = key
            self._send_diff(trading_pair, bids, asks, msg.get("source_ts_ns", 0))
        elif served is not None and new_market:
            self.logger().error(f"edge-connect also serves {symbol!r} as {key}; {trading_pair} stays on {served}")

    def _send_diff(self, trading_pair: str, bids: List[Level], asks: List[Level], source_ts_ns: int):
        self._update_ids[trading_pair] += 1
        self._message_queue[self._diff_messages_queue_key].put_nowait(OrderBookMessage(
            OrderBookMessageType.DIFF, {
                "trading_pair": trading_pair, "update_id": self._update_ids[trading_pair], "bids": bids, "asks": asks,
            }, timestamp=_seconds(source_ts_ns)))

    def _on_trade(self, msg: Dict[str, Any]):
        trading_pair = self._pairs.get(msg["symbol"])
        key = self._served.get(trading_pair)
        if key is None or msg["source_id"] != key[0]:
            return
        side = TradeType.SELL if msg["aggressor_side"] == "sell" else TradeType.BUY
        self._message_queue[self._trade_messages_queue_key].put_nowait(OrderBookMessage(
            OrderBookMessageType.TRADE, {
                "trading_pair": trading_pair, "trade_type": float(side.value), "trade_id": msg["trade_id"],
                "price": msg["price"], "amount": msg["size"],
            }, timestamp=_seconds(msg["source_ts_ns"])))


class DoubleZeroPerpetualDataSource(DoubleZeroOrderBookDataSource, PerpetualAPIOrderBookDataSource):
    """Adds funding and mark price from the venue's own WebSocket, which edge-connect does not carry."""

    def __init__(self, *args, funding_subscription: Callable[[str], dict], **kwargs):
        super().__init__(*args, **kwargs)
        self._funding_subscription = funding_subscription
        # The venue's connection runs for funding alone: its book and trade channels go unsubscribed.
        self._venue._subscribe_channels = self._subscribe_venue_funding

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        return await self._venue.get_funding_info(trading_pair)

    async def listen_for_funding_info(self, output: asyncio.Queue):
        # Opened here, not with the book, so paper trading, which never reads funding, opens no venue connection.
        await asyncio.gather(self._venue.listen_for_subscriptions(), self._venue.listen_for_funding_info(output))

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._venue._parse_funding_info_message(raw_message, message_queue)

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if not await super().subscribe_to_trading_pair(trading_pair):
            return False
        if self._venue._ws_assistant is not None:
            await self._send_venue_funding(self._venue._ws_assistant, self._symbols[trading_pair])
        return True

    async def _subscribe_venue_funding(self, ws: WSAssistant):
        for trading_pair in list(self._trading_pairs):
            symbol = await self._resolve(trading_pair)
            if symbol is not None:
                await self._send_venue_funding(ws, symbol)

    async def _send_venue_funding(self, ws: WSAssistant, symbol: str):
        await ws.send(WSJSONRequest(payload=self._funding_subscription(symbol)))
