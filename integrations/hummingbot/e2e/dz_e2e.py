"""CI's V2 script: reads what a market maker reads from hyperliquid_perpetual books fed by edge-connect.

It records evidence to data/dz_e2e_evidence.json on every tick; e2e/check.py judges it.
"""

import json
import os
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.hyperliquid_perpetual import hyperliquid_perpetual_constants as CONSTANTS
from hummingbot.core.data_type.common import MarketDict
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import OrderBookEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

from scripts.dz_hummingbot import use_doublezero

STUB = os.environ.get("DZ_E2E_STUB", "127.0.0.1:18081")
# Test-only: Hyperliquid's own REST and WebSocket go to the stub too, so the run needs no network.
CONSTANTS.PERPETUAL_BASE_URL = f"http://{STUB}"
CONSTANTS.PERPETUAL_WS_URL = f"ws://{STUB}/ws"

use_doublezero(["hyperliquid_perpetual"], url=f"ws://{STUB}/")

PAPER = "hyperliquid_perpetual_paper_trade"
LIVE = "hyperliquid_perpetual"
PAIRS = ("BTC-USD", "XYZ:XYZ100-USD")
EVIDENCE = os.path.join("data", "dz_e2e_evidence.json")


class DzE2EConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[PAPER] = set(PAIRS)
        return markets


class DzE2E(StrategyV2Base):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: DzE2EConfig):
        super().__init__(connectors, config)
        self.evidence = {pair: {"trade_ids": []} for pair in PAIRS}
        self.states = {pair: set() for pair in PAIRS}
        self.marks = []
        self.forwarders = {}

    def on_tick(self):
        if not self.forwarders:
            # The non-trading `LIVE` connector runs the full perpetual start-up, funding included.
            safe_ensure_future(self.market_data_provider.initialize_order_book(LIVE, PAIRS[0]))
        for pair in PAIRS:
            book = self.connectors[PAPER].get_order_book(pair)
            evidence = self.evidence[pair]
            if pair not in self.forwarders:
                self.forwarders[pair] = EventForwarder(lambda event, ids=evidence["trade_ids"]: ids.append(event.trade_id))
                book.add_listener(OrderBookEvent.TradeEvent, self.forwarders[pair])
            bids, asks = list(book.bid_entries()), list(book.ask_entries())
            self.states[pair].add(tuple((row.price, row.amount) for row in bids + asks))
            evidence["states"] = len(self.states[pair])
            evidence["top"] = [book.get_price(False), book.get_price(True)]
            evidence["levels"] = [len(bids), len(asks)]
            # A volume reaching into the second level: the depth query a one-level book answers with NaN.
            if len(asks) > 1:
                volume = asks[0].amount + asks[1].amount / 2
                evidence["depth_price"] = book.get_price_for_volume(True, volume).result_price
        try:
            mark = float(self.market_data_provider.get_funding_info(LIVE, PAIRS[0]).mark_price)
            if mark not in self.marks:
                self.marks.append(mark)
        except KeyError:
            pass  # funding not initialised yet
        with open(EVIDENCE + ".tmp", "w") as f:
            json.dump({"pairs": self.evidence, "marks": self.marks}, f)
        os.replace(EVIDENCE + ".tmp", EVIDENCE)
