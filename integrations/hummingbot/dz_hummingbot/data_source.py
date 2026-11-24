"""Order-book data sources that read edge-connect instead of the venue."""

import asyncio
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

from .assembler import BookAssembler, MarketKey, market_key
from .client import EdgeConnectClient, subscriptions

SNAPSHOT_POLL_SECONDS = 0.5
WAIT_WARNING_SECONDS = 30.0
# How long a symbol may stay absent from the catalog before its pair is refused; a bridge that has
# just started names an instrument only once it has priced it.
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
        self._markets: Dict[str, Set[MarketKey]] = defaultdict(set)
        # The market each pair's first snapshot came from; diffs and trades flow only after it.
        self._served: Dict[str, MarketKey] = {}
        self._carried: Set[str] = set()
        self._catalog_read = False

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._venue.get_last_traded_prices(trading_pairs=trading_pairs, domain=domain)

    async def listen_for_subscriptions(self):
        await self._client.run(self._on_connect, self._on_message)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # The tracker replays its last 32 diffs over any later snapshot, resurrecting levels a
        # re-baseline removed, so only a pair's first book is a snapshot and none is fetched hourly.
        await asyncio.Event().wait()

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        self.add_trading_pair(trading_pair)
        if self._client.connected:
            await self._subscribe("subscribe", await self._symbol(trading_pair))
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        self.remove_trading_pair(trading_pair)
        self._served.pop(trading_pair, None)
        symbol = self._symbols.pop(trading_pair, None)
        if symbol is not None:
            del self._pairs[symbol]
            for key in self._markets.pop(symbol, ()):
                self._assembler.forget(key)
            await self._subscribe("unsubscribe", symbol)
        return True

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._symbol(trading_pair)
        started = warned = time.monotonic()
        while True:
            markets = self._markets.get(symbol, set())
            if len(markets) > 1:
                raise ValueError(f"{len(markets)} edge-connect {self._source_name} markets share the symbol "
                                 f"{symbol!r}; {trading_pair} cannot be served from one of them")
            key = next(iter(markets), None)
            if key is not None and self._assembler.is_complete(key):
                update_id, bids, asks = self._assembler.snapshot(key)
                self._served[trading_pair] = key
                return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
                    "trading_pair": trading_pair, "update_id": update_id, "bids": bids, "asks": asks,
                }, timestamp=time.time())
            waited = time.monotonic() - started
            if self._catalog_read and symbol not in self._carried and waited >= NOT_CARRIED_AFTER_SECONDS:
                raise ValueError(f"edge-connect at {self._client.url} carries no {self._source_name} market "
                                 f"named {symbol!r}, so {trading_pair} has no book")
            if time.monotonic() - warned >= WAIT_WARNING_SECONDS:
                warned = time.monotonic()
                self.logger().warning(f"Still waiting for edge-connect's {symbol!r} book for {trading_pair}: "
                                      f"{self._waiting_on(symbol)}")
            await self._sleep(SNAPSHOT_POLL_SECONDS)

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

    async def _subscribe(self, method: str, symbol: str):
        for subscription in subscriptions(self._source_name, symbol):
            await self._client.send(method, subscription)

    async def _on_connect(self):
        self._catalog_read = False
        await self._client.send("subscribe", {"source_name": self._source_name, "type": "status"})
        for trading_pair in list(self._trading_pairs):
            await self._subscribe("subscribe", await self._symbol(trading_pair))

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
        if channel == "subscription_response":
            # edge-connect reads no control frame before its connect-time replay, so this follows the catalog.
            self._catalog_read = True
        elif channel == "error":
            self.logger().error(f"edge-connect at {self._client.url} refused a request: {msg.get('error')}")

    def _on_book(self, msg: Dict[str, Any]):
        trading_pair = self._pairs.get(msg["symbol"])
        if trading_pair is None:
            return
        key = market_key(msg)
        markets = self._markets[msg["symbol"]]
        served = self._served.get(trading_pair)
        if key not in markets and served is not None:
            self.logger().error(f"edge-connect serves {msg['symbol']!r} under a second identity {key}; "
                                f"{trading_pair} stays on {served} until restarted")
        markets.add(key)
        event = self._assembler.apply(msg)
        if event is not None and self._served.get(trading_pair) == key:
            self._message_queue[self._diff_messages_queue_key].put_nowait(OrderBookMessage(
                OrderBookMessageType.DIFF, {
                    "trading_pair": trading_pair, "update_id": event.update_id,
                    "bids": event.bids, "asks": event.asks,
                }, timestamp=_seconds(event.source_ts_ns)))

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
        await self._venue.listen_for_funding_info(output)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        await self._venue._parse_funding_info_message(raw_message, message_queue)

    async def listen_for_subscriptions(self):
        await asyncio.gather(super().listen_for_subscriptions(), self._venue.listen_for_subscriptions())

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        await super().subscribe_to_trading_pair(trading_pair)
        if self._venue._ws_assistant is not None:
            await self._send_venue_funding(self._venue._ws_assistant, trading_pair)
        return True

    async def _subscribe_venue_funding(self, ws: WSAssistant):
        for trading_pair in list(self._trading_pairs):
            await self._send_venue_funding(ws, trading_pair)

    async def _send_venue_funding(self, ws: WSAssistant, trading_pair: str):
        await ws.send(WSJSONRequest(payload=self._funding_subscription(await self._symbol(trading_pair))))
