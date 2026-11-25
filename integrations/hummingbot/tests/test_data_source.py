import asyncio
import socket
import time
import unittest
from decimal import Decimal
from unittest.mock import patch

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import OrderBookEvent
from hummingbot.core.web_assistant.connections.connections_factory import ConnectionsFactory
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

from dz_hummingbot import data_source
from dz_hummingbot.data_source import DoubleZeroOrderBookDataSource, DoubleZeroPerpetualDataSource
from stub import StubEdgeConnect

VENUE = "HYPERLIQUID"


def instrument(symbol, source_id=1, channel=0, instrument_id=1):
    return {"type": "instrument", "venue": VENUE, "source_name": VENUE, "source_id": source_id, "symbol": symbol,
            "channel": channel, "instrument_id": instrument_id, "price_exponent": -1, "qty_exponent": -3,
            "tick_size": 0}


def order_book(symbol, changes, source_id=1, channel=0, instrument_id=1, last=True):
    return {"type": "order_book", "venue": VENUE, "source_name": VENUE, "source_id": source_id, "symbol": symbol,
            "channel": channel, "instrument_id": instrument_id, "changes": changes, "snapshot": False,
            "last": last, "source_ts_ns": 1_781_019_263_715_344_015}


def order(side, price, size, order_id):
    return {"action": "update" if size else "delete", "side": side, "price": price, "size": size,
            "order_id": order_id}


CLEAR = {"action": "clear", "side": "both", "price": 0, "size": 0, "order_id": 0}


def trade(symbol, trade_id, source_id=1):
    return {"type": "trade", "venue": VENUE, "source_name": VENUE, "source_id": source_id, "symbol": symbol,
            "channel": 0, "instrument_id": 1, "price": 100.5, "size": 0.25, "aggressor_side": "sell",
            "trade_id": trade_id, "cumulative_volume": 0, "source_ts_ns": 1_781_019_263_715_344_015}


BTC_BOOK = order_book("BTC", [CLEAR, order("bid", 100.0, 1.0, 1), order("bid", 100.0, 2.0, 2),
                              order("bid", 99.0, 5.0, 3), order("ask", 101.0, 1.5, 4)])
ETH_BOOK = order_book("ETH", [CLEAR, order("bid", 30.0, 1.0, 1), order("ask", 31.0, 2.0, 2)], instrument_id=2)


class FakeConnector:
    async def exchange_symbol_associated_to_pair(self, trading_pair):
        if trading_pair.startswith("NONE"):
            raise KeyError(trading_pair)
        return trading_pair.split("-")[0]


class FakeVenue:
    """Stands in for the connector's own data source, and for the WebSocket it subscribes on."""

    _ws_assistant = None

    def __init__(self):
        self.sent = []

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        return {pair: 100.0 for pair in trading_pairs}

    async def get_funding_info(self, trading_pair):
        return FundingInfo(trading_pair, Decimal("100"), Decimal("101"), 0, Decimal("0.0001"))

    async def listen_for_funding_info(self, output):
        output.put_nowait("funding update")
        await asyncio.Event().wait()

    async def listen_for_subscriptions(self):
        await self._subscribe_channels(self)
        await asyncio.Event().wait()

    async def send(self, request):
        self.sent.append(request.payload)


async def until(condition, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def levels(entries):
    return sorted((row.price, row.amount) for row in entries)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class DataSourceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stub = StubEdgeConnect([instrument("BTC"), instrument("ETH", instrument_id=2), BTC_BOOK, ETH_BOOK])
        await self.stub.start()
        self.venue = FakeVenue()
        self.pairs = ["BTC-USD"]
        self.tracker = None
        self.tasks = []
        self.cleanups = []
        patcher = patch.object(OrderBookTracker, "_sleep", staticmethod(lambda delay: asyncio.sleep(0)))
        patcher.start()
        self.addCleanup(patcher.stop)

    async def asyncTearDown(self):
        if self.tracker is not None:
            self.tracker.stop()
        for task in self.tasks:
            task.cancel()
        await self.stub.stop()
        for stub in self.cleanups:
            await stub.stop()
        await ConnectionsFactory().close()

    def data_source(self, cls=DoubleZeroOrderBookDataSource, url=None, **kwargs):
        factory = WebAssistantsFactory(throttler=AsyncThrottler(rate_limits=[]))
        return cls(self.pairs, FakeConnector(), self.venue, url or self.stub.url, VENUE, factory, **kwargs)

    async def start_tracker(self, ds):
        self.tracker = OrderBookTracker(data_source=ds, trading_pairs=self.pairs)
        self.tracker.start()
        await until(lambda: self.tracker.ready)
        return self.tracker.order_books["BTC-USD"]

    def listen(self, ds):
        self.tasks.append(asyncio.ensure_future(ds.listen_for_subscriptions()))

    async def test_first_book_comes_from_the_replay_then_diffs_and_trades_follow(self):
        book = await self.start_tracker(self.data_source())
        trade_ids = []
        forwarder = EventForwarder(lambda event: trade_ids.append(event.trade_id))
        book.add_listener(OrderBookEvent.TradeEvent, forwarder)

        self.assertEqual(levels(book.bid_entries()), [(99.0, 5.0), (100.0, 3.0)])
        self.assertEqual(levels(book.ask_entries()), [(101.0, 1.5)])

        await self.stub.push(order_book("BTC", [order("bid", 100.0, 0, 1), order("ask", 102.0, 4.0, 5)]))
        await until(lambda: levels(book.ask_entries()) == [(101.0, 1.5), (102.0, 4.0)])
        self.assertEqual(levels(book.bid_entries()), [(99.0, 5.0), (100.0, 2.0)])

        await self.stub.push(trade("BTC", 7), trade("BTC", 8))
        await until(lambda: trade_ids == [7, 8])
        self.assertEqual(book.last_trade_price, 100.5)

    async def test_a_rebaseline_reaches_the_tracker_as_a_diff_that_removes_stale_levels(self):
        book = await self.start_tracker(self.data_source())

        await self.stub.push(order_book("BTC", [CLEAR, order("bid", 98.0, 1.0, 9), order("ask", 101.0, 1.5, 4)]))
        await until(lambda: levels(book.bid_entries()) == [(98.0, 1.0)])

        self.assertEqual(levels(book.ask_entries()), [(101.0, 1.5)])
        self.assertEqual(self.tracker.metrics.total_snapshots_processed, 0)

    async def test_a_rebaseline_restores_a_level_hummingbot_truncated(self):
        book = await self.start_tracker(self.data_source())

        # An ask crossing the 100 bid: Hummingbot drops the older side, which edge-connect still holds.
        await self.stub.push(order_book("BTC", [order("ask", 99.5, 1.0, 6)]))
        await until(lambda: levels(book.bid_entries()) == [(99.0, 5.0)])
        await self.stub.push(order_book("BTC", [order("ask", 99.5, 0, 6)]), BTC_BOOK)

        await until(lambda: levels(book.bid_entries()) == [(99.0, 5.0), (100.0, 3.0)])

    async def test_an_event_is_applied_whole_at_last(self):
        book = await self.start_tracker(self.data_source())

        # Mid-event the book is locked at 100; published, Hummingbot would truncate the older bid.
        await self.stub.push(order_book("BTC", [order("ask", 100.0, 1.0, 6)], last=False),
                             order_book("BTC", [order("ask", 100.5, 2.0, 6)]))
        await until(lambda: levels(book.ask_entries()) == [(100.5, 2.0), (101.0, 1.5)])

        self.assertEqual(levels(book.bid_entries()), [(99.0, 5.0), (100.0, 3.0)])

    async def test_subscribes_to_each_pairs_book_and_prints_by_name(self):
        await self.start_tracker(self.data_source())

        self.assertEqual([frame["subscription"] for frame in self.stub.received], [
            {"source_name": VENUE, "type": "status"},
            {"source_name": VENUE, "symbol": "BTC", "type": "order_book"},
            {"source_name": VENUE, "symbol": "BTC", "type": "trade"},
        ])

    async def test_reconnect_rebaselines_the_book_from_the_new_replay(self):
        book = await self.start_tracker(self.data_source())

        self.stub.catalog = [instrument("BTC"), order_book("BTC", [CLEAR, order("ask", 105.0, 2.0, 11)])]
        await self.stub.drop()

        await until(lambda: levels(book.ask_entries()) == [(105.0, 2.0)])
        self.assertEqual(levels(book.bid_entries()), [])
        self.assertEqual(self.stub.connections, 2)

    async def test_a_market_renumbered_across_a_reconnect_is_followed(self):
        book = await self.start_tracker(self.data_source())

        self.stub.catalog = [instrument("BTC", instrument_id=9),
                             order_book("BTC", [CLEAR, order("bid", 97.0, 1.0, 1)], instrument_id=9)]
        await self.stub.drop()
        await until(lambda: levels(book.bid_entries()) == [(97.0, 1.0)])

        self.assertEqual(levels(book.ask_entries()), [])
        await self.stub.push(order_book("BTC", [order("ask", 98.0, 2.0, 2)], instrument_id=9))
        await until(lambda: levels(book.ask_entries()) == [(98.0, 2.0)])

    async def test_other_venues_and_symbols_are_ignored(self):
        self.stub.catalog.append(dict(BTC_BOOK, source_name="PHOENIX", venue="PHOENIX", instrument_id=3,
                                      changes=[CLEAR, order("bid", 1.0, 1.0, 1)]))
        book = await self.start_tracker(self.data_source())

        self.assertEqual(levels(book.bid_entries()), [(99.0, 5.0), (100.0, 3.0)])

    async def test_a_pair_the_bridge_does_not_carry_is_refused_by_name(self):
        ds = self.data_source()
        self.listen(ds)

        with patch.object(data_source, "NOT_CARRIED_AFTER_SECONDS", 0):
            with self.assertRaisesRegex(ValueError, "no HYPERLIQUID market named 'SOL'.*SOL-USD"):
                await ds._order_book_snapshot("SOL-USD")

    async def test_the_not_carried_grace_runs_from_the_catalog_not_the_request(self):
        port = free_port()
        ds = self.data_source(url=f"ws://127.0.0.1:{port}/")
        self.listen(ds)
        with patch.object(data_source, "NOT_CARRIED_AFTER_SECONDS", 1.0):
            request = asyncio.ensure_future(ds._order_book_snapshot("SOL-USD"))
            await asyncio.sleep(1.5)
            late = StubEdgeConnect(self.stub.catalog)
            await late.start(port)
            self.cleanups.append(late)
            await until(lambda: ds._catalog_read_at is not None, timeout=10)
            self.assertFalse(request.done())

            with self.assertRaisesRegex(ValueError, "no HYPERLIQUID market named 'SOL'"):
                await request
            self.assertGreaterEqual(time.monotonic() - ds._catalog_read_at, 1.0)

    async def test_a_symbol_shared_by_two_markets_is_refused(self):
        self.stub.catalog.append(order_book("BTC", [CLEAR, order("bid", 1.0, 1.0, 1)], source_id=7))
        ds = self.data_source()
        self.listen(ds)

        with self.assertRaisesRegex(ValueError, "2 edge-connect HYPERLIQUID markets share the symbol 'BTC'"):
            await ds._order_book_snapshot("BTC-USD")

    async def test_a_pair_past_the_subscription_cap_is_refused_by_name(self):
        self.stub.max_subscriptions = 2
        ds = self.data_source()
        self.listen(ds)

        with self.assertRaisesRegex(ValueError, "refused BTC-USD: max subscriptions reached"):
            await ds._order_book_snapshot("BTC-USD")

    async def test_a_pair_can_be_added_removed_and_added_again(self):
        ds = self.data_source()
        await self.start_tracker(ds)

        for _ in range(2):
            self.assertTrue(await self.tracker.add_trading_pair("ETH-USD"))
            self.assertEqual(levels(self.tracker.order_books["ETH-USD"].ask_entries()), [(31.0, 2.0)])
            self.assertTrue(await self.tracker.remove_trading_pair("ETH-USD"))

        self.assertEqual(self.pairs, ["BTC-USD"])
        self.assertIn({"source_name": VENUE, "symbol": "ETH", "type": "order_book"},
                      [frame["subscription"] for frame in self.stub.received if frame["method"] == "unsubscribe"])

    async def test_a_refused_pair_added_later_leaves_the_connector_so_a_restart_still_starts(self):
        ds = self.data_source()
        book = await self.start_tracker(ds)

        with patch.object(data_source, "NOT_CARRIED_AFTER_SECONDS", 0):
            self.assertFalse(await self.tracker.add_trading_pair("SOL-USD"))
        self.assertFalse(await self.tracker.add_trading_pair("NONE-USD"))
        self.assertEqual(self.pairs, ["BTC-USD"])

        self.tracker.stop()
        self.tracker.start()
        await until(lambda: self.tracker.ready)
        book = self.tracker.order_books["BTC-USD"]
        await self.stub.push(trade("BTC", 1))
        await until(lambda: book.last_trade_price == 100.5)

    async def test_trades_from_another_source_id_are_dropped(self):
        book = await self.start_tracker(self.data_source())

        await self.stub.push(trade("BTC", 1, source_id=7), trade("BTC", 2))
        await until(lambda: self.tracker.metrics.total_trades_processed == 1)
        self.assertEqual(book.last_trade_price, 100.5)

    async def test_last_traded_prices_come_from_the_venue(self):
        ds = self.data_source()

        self.assertEqual(await ds.get_last_traded_prices(["BTC-USD"]), {"BTC-USD": 100.0})


class PerpetualDataSourceTest(DataSourceTest):
    def data_source(self, cls=DoubleZeroPerpetualDataSource, url=None, **kwargs):
        return super().data_source(cls, url, funding_subscription=lambda coin: {"funding": coin}, **kwargs)

    async def test_funding_opens_the_venue_connection_for_funding_alone(self):
        ds = self.data_source()
        await self.start_tracker(ds)
        self.assertEqual(self.venue.sent, [])

        updates = asyncio.Queue()
        self.tasks.append(asyncio.ensure_future(ds.listen_for_funding_info(updates)))

        self.assertEqual((await ds.get_funding_info("BTC-USD")).mark_price, Decimal("101"))
        self.assertEqual(await asyncio.wait_for(updates.get(), 1), "funding update")
        self.assertEqual(self.venue.sent, [{"funding": "BTC"}])

    async def test_a_pair_added_later_subscribes_funding_on_the_venue_connection(self):
        ds = self.data_source()
        await self.start_tracker(ds)
        self.tasks.append(asyncio.ensure_future(ds.listen_for_funding_info(asyncio.Queue())))
        await until(lambda: self.venue.sent)
        self.venue._ws_assistant = self.venue

        self.assertTrue(await ds.subscribe_to_trading_pair("ETH-USD"))

        self.assertEqual(self.venue.sent, [{"funding": "BTC"}, {"funding": "ETH"}])
        self.assertIn("ETH-USD", self.pairs)
