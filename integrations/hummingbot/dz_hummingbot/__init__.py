"""Feeds Hummingbot connectors' order books and trades from DoubleZero edge-connect.

Call `use_doublezero` at the top of a V2 script; order entry and account streams stay on the
connector.
"""

import importlib
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

DEFAULT_URL = "ws://127.0.0.1:8081"


def _hyperliquid_funding(coin: str) -> dict:
    return {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}}


@dataclass(frozen=True)
class _Connector:
    module: str
    cls: str
    source_name: str
    funding_subscription: Optional[Callable[[str], dict]] = None


CONNECTORS = {
    "hyperliquid": _Connector(
        "hummingbot.connector.exchange.hyperliquid.hyperliquid_exchange", "HyperliquidExchange", "HYPERLIQUID"),
    "hyperliquid_perpetual": _Connector(
        "hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_derivative",
        "HyperliquidPerpetualDerivative", "HYPERLIQUID", _hyperliquid_funding),
}


def use_doublezero(connectors: Iterable[str], url: str = DEFAULT_URL) -> None:
    """Makes every instance of the named connectors read its books and trades from edge-connect at `url`.

    Must run before the connectors are built, which is why it belongs at a V2 script's top level.
    Calling it again replaces the earlier call's URL.
    """
    connectors = list(connectors)
    unsupported = [name for name in connectors if name not in CONNECTORS]
    if unsupported:
        raise ValueError(f"dz_hummingbot does not support {', '.join(unsupported)}; "
                         f"supported connectors: {', '.join(sorted(CONNECTORS))}")
    for name in connectors:
        _install(name, CONNECTORS[name], url)


def _install(name: str, spec: _Connector, url: str) -> None:
    from .data_source import DoubleZeroOrderBookDataSource, DoubleZeroPerpetualDataSource

    cls = getattr(importlib.import_module(spec.module), spec.cls)
    # Kept on the class so a second call, or Hummingbot reloading the script, wraps the original.
    original = cls.__dict__.get("_dz_venue_data_source") or cls._create_order_book_data_source
    cls._dz_venue_data_source = original

    def _create_order_book_data_source(connector):
        venue = original(connector)
        if connector.name != name:  # the same class serves the venue's testnet
            return venue
        args = (connector.trading_pairs, connector, venue, url, spec.source_name, connector._web_assistants_factory)
        if spec.funding_subscription is None:
            return DoubleZeroOrderBookDataSource(*args)
        return DoubleZeroPerpetualDataSource(*args, funding_subscription=spec.funding_subscription)

    cls._create_order_book_data_source = _create_order_book_data_source
