# Hummingbot adapter

Feeds Hummingbot's own connectors from edge-connect's normalized WebSocket ([PROTOCOL.md](../../PROTOCOL.md)). A connector's order books and public trades come from edge-connect; order entry, account streams, funding and mark price stay on the venue. Hummingbot is neither forked nor patched on disk.

Hummingbot hard-codes connector URLs and has no plugin mechanism, so the adapter replaces a connector's order-book data source when a V2 script loads it. Pointing the stock Hyperliquid connector at edge-connect's [Hyperliquid-compatible sink](../../docs/output-sinks.md#hyperliquid-compatible-sink) instead freezes funding and mark price, and gives every trade the same ID.

## Supported connectors

| Connector | From edge-connect | From the venue |
|---|---|---|
| `hyperliquid_perpetual` | Order books and trades, for native perps and HIP-3 markets such as `XYZ:XYZ100-USD` | Funding, mark and index price, orders, account, trading rules |
| `hyperliquid` (spot) | Order books and trades | Orders, account, trading rules |

edge-connect carries no Hyperliquid spot feed today, so a `hyperliquid` pair stops at startup with an error naming it.

The bridge must be subscribed to both of a market's groups: the book comes from `edge-hyper-hl-mbo` (`edge-hyper-xyz-mbo` for `xyz:` markets) and trades from `edge-hyper-hl-tob` (`edge-hyper-xyz-tob`).

Binance is not supported: edge-connect carries only its top of book, and Hummingbot's depth queries return NaN on a one-level book.

## Install

Copy `dz_hummingbot/` into Hummingbot's `scripts/` directory, the one mounted at `/home/hummingbot/scripts` when Hummingbot runs in Docker. It needs nothing Hummingbot does not already ship.

## Use

Call `use_doublezero` at the top level of a V2 script:

```python
import os

from hummingbot.core.data_type.common import MarketDict, PriceType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

from scripts.dz_hummingbot import use_doublezero

use_doublezero(["hyperliquid_perpetual"], url="ws://127.0.0.1:8081")


class DoubleZeroBookConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets["hyperliquid_perpetual"] = {"BTC-USD"}
        return markets


class DoubleZeroBook(StrategyV2Base):
    def on_tick(self):
        connector = self.connectors["hyperliquid_perpetual"]
        mid = connector.get_price_by_type("BTC-USD", PriceType.MidPrice)
        mark = connector.get_funding_info("BTC-USD").mark_price
        self.logger().info(f"BTC-USD mid {mid}, mark {mark}")
```

Hummingbot imports a script before it builds the script's connectors, which is what lets the call take effect. Hummingbot reloads the script on every `start`; calling `use_doublezero` again replaces the URL.

**The replacement is process-wide and lasts until Hummingbot exits.** Every connector of that type reads edge-connect from then on, including those that `MarketDataProvider` and paper trading build, and those of strategies that never call `use_doublezero`, V1 strategies among them. The interactive client imports every script in `scripts/` when it starts, so there, one script that calls `use_doublezero` applies it whichever strategy you run. Run strategies that should read the venue's own feed from an install without such a script.

A live connector needs its keys as usual. To try the adapter without keys, add `hyperliquid_perpetual` to `paper_trade.paper_trade_exchanges` in `conf/conf_client.yml` and trade `hyperliquid_perpetual_paper_trade`: its book comes from edge-connect, but paper trading has no funding.

**Which URL.** The bridge serves `ws://127.0.0.1:8081` by default, on a server or in the Mac app.

- Hummingbot on the bridge's machine, outside Docker or in a container on the host network (Hummingbot's own `docker-compose.yml` runs it that way): `ws://127.0.0.1:8081`.
- Hummingbot in Docker Desktop on a Mac, with the Mac app's bridge: `ws://host.docker.internal:8081`.
- A bridge on another host: `ws://<host>:8081`, with the bridge started with `WS_BIND=0.0.0.0:8081`. It has no TLS or authentication, so firewall the port. Across a network you do not trust, anyone on the path can rewrite the books you quote against: put the bridge behind a TLS reverse proxy and use `wss://<host>` instead.

**Controllers.** A V2 controller can call `use_doublezero` at its module's top level instead: Hummingbot imports a script's controllers while it loads their configs, before it builds any connector. That holds while the script's `init_markets` calls `load_controller_configs`, as `StrategyV2Base`'s does and `v2_with_controllers.py` inherits; a script that overrides it without that call must call `use_doublezero` itself.

**V1 strategies cannot call it**, since they load no user code before their connectors are built, though the scope above still reaches them.

## What to expect

- A pair's first book comes from edge-connect's replay rather than the venue's REST API, and edge-connect's later corrections arrive as ordinary book updates. Hummingbot sees a book only at the end of each logical event, never mid-event.
- A pair edge-connect does not carry stops the connector's startup with an error naming it, once edge-connect's catalog has lacked it for 60 seconds. So does a symbol two edge-connect markets share, and a pair whose subscription edge-connect refuses. Until then, a warning every 30 seconds says what the pair is waiting for. A pair added at runtime that is refused this way is removed from the connector again.
- When edge-connect is unreachable, the adapter reconnects with back-off, and the book holds its last state meanwhile, as it does when the venue's own feed drops.
- Each pair takes two of the 256 subscriptions edge-connect allows a client by default, plus one per connector, so a connector carries up to 127 pairs.
- Testnet connectors keep the venue's feed, since edge-connect carries mainnet.

## Tested Hummingbot versions

CI runs the tests and an end-to-end check in each supported release's published image. Today that is Hummingbot v2.18.0 (`hummingbot/hummingbot:version-2.18.0`), listed in the `image` matrix of [`.github/workflows/hummingbot.yml`](../../.github/workflows/hummingbot.yml).

- `tests/` covers the book assembler against PROTOCOL.md's rules, and the data source against a stub edge-connect driving Hummingbot's real order-book tracker. Run them from this directory against a Hummingbot checkout compiled with `./compile`, as `PYTHONPATH=<hummingbot> python -m pytest`, or in an image as CI does: `docker run --rm -v "$PWD:/work:ro" -w /work -e PYTHONPATH=/home/hummingbot hummingbot/hummingbot:version-2.18.0 python -m pytest -p no:cacheprovider`.
- `e2e/run.sh <image>` runs a V2 script in a Hummingbot image against `e2e/stub.py`, which plays a synthetic edge-connect session and Hyperliquid's public API. It checks that the books move, that trades carry distinct IDs, that funding changes after startup, and that neither edge-connect nor Hyperliquid was asked for anything it does not serve.
