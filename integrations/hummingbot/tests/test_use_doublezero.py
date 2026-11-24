import unittest

from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_api_order_book_data_source import (
    HyperliquidPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_derivative import (
    HyperliquidPerpetualDerivative,
)
from hummingbot.connector.exchange.hyperliquid.hyperliquid_api_order_book_data_source import (
    HyperliquidAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange import HyperliquidExchange

from dz_hummingbot import use_doublezero
from dz_hummingbot.data_source import DoubleZeroOrderBookDataSource, DoubleZeroPerpetualDataSource


class UseDoubleZeroTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for cls in (HyperliquidExchange, HyperliquidPerpetualDerivative):
            self.addCleanup(setattr, cls, "_create_order_book_data_source", cls._create_order_book_data_source)
            self.addCleanup(lambda c=cls: "_dz_venue_data_source" in c.__dict__ and delattr(c, "_dz_venue_data_source"))

    async def test_perpetual_connector_reads_edge_connect_and_keeps_its_own_source_for_funding(self):
        use_doublezero(["hyperliquid_perpetual"], url="ws://edge:8081")

        connector = HyperliquidPerpetualDerivative(trading_pairs=["BTC-USD"], trading_required=False)

        ds = connector._orderbook_ds
        self.assertIsInstance(ds, DoubleZeroPerpetualDataSource)
        self.assertIs(connector.order_book_tracker.data_source, ds)
        self.assertIs(type(ds._venue), HyperliquidPerpetualAPIOrderBookDataSource)
        self.assertEqual(ds._client.url, "ws://edge:8081")
        self.assertIs(ds._trading_pairs, connector.trading_pairs)

    async def test_spot_connector_reads_edge_connect(self):
        use_doublezero(["hyperliquid"])

        connector = HyperliquidExchange(trading_pairs=["HYPE-USDC"], trading_required=False)

        self.assertIs(type(connector._orderbook_ds), DoubleZeroOrderBookDataSource)
        self.assertIs(type(connector._orderbook_ds._venue), HyperliquidAPIOrderBookDataSource)
        self.assertEqual(connector._orderbook_ds._client.url, "ws://127.0.0.1:8081")

    async def test_a_second_call_wraps_the_original_once_and_takes_the_new_url(self):
        use_doublezero(["hyperliquid_perpetual"], url="ws://first:8081")
        use_doublezero(["hyperliquid_perpetual"], url="ws://second:8081")

        connector = HyperliquidPerpetualDerivative(trading_pairs=["BTC-USD"], trading_required=False)

        self.assertIs(type(connector._orderbook_ds._venue), HyperliquidPerpetualAPIOrderBookDataSource)
        self.assertEqual(connector._orderbook_ds._client.url, "ws://second:8081")

    async def test_testnet_keeps_the_venue_feed(self):
        use_doublezero(["hyperliquid_perpetual"])

        connector = HyperliquidPerpetualDerivative(
            trading_pairs=["BTC-USD"], trading_required=False, domain="hyperliquid_perpetual_testnet")

        self.assertIs(type(connector._orderbook_ds), HyperliquidPerpetualAPIOrderBookDataSource)

    async def test_an_unsupported_connector_is_refused_before_anything_is_patched(self):
        original = HyperliquidPerpetualDerivative._create_order_book_data_source

        with self.assertRaisesRegex(ValueError, "does not support binance; supported connectors: "
                                                "hyperliquid, hyperliquid_perpetual"):
            use_doublezero(["hyperliquid_perpetual", "binance"])

        self.assertIs(HyperliquidPerpetualDerivative._create_order_book_data_source, original)
