# Input sources

The DZ Edge **multicast** feeds are always-on inputs. A second, optional input for some feeds are a
**public** WebSocket feed which acts as a **backstop**: the edge feed should win essentially always, 
so the public feed only matters when the edge feed gaps, stalls, or dies.

Both inputs converge on one shared arbiter that races them per `(venue, symbol)` `source_ts` tick,
so no second dedup stage is needed. In steady state an edge publisher opens each tick first (sub-ms
vs. the public feed's tens of ms over the internet), so the public copy loses the race and is
dropped as a no-op; when the edge gaps, the public copy is the first to cross the floor and fills
in. The backstop needs no health check, and the WebSocket output is identical regardless of which
input delivered a given update.

| Input source | Default | Enable / disable | Config flags (env) |
|--------------|---------|------------------|--------------------|
| **DZ Edge multicast** | **on** | always on | `--feed`/`--iface`/`--recv-buf` |
| **Hyperliquid public WS** (`ingest::ws_feeder`) | **off** | on when `--ws-input-coins` is non-empty | `--ws-input-coins` (`WS_INPUT_COINS`, e.g. `BTC,ETH`) · `--ws-input-url` (`WS_INPUT_URL`, default `wss://api.hyperliquid.xyz/ws`) |

```bash
# From source — run the edge multicast feed with the public WS backstop for BTC and ETH:
./target/release/doublezero-edge-connect --feed Hyperliquid --ws-input-coins BTC,ETH

# Via the installer one-liner, as env vars before the pipe:
WS_INPUT_COINS=BTC,ETH curl -fsSL https://get.doublezero.xyz/connect | bash
```

The feeder is failure-isolated (its own task with reconnect + exponential backoff; decode/socket
errors are logged and never touch the multicast hot path) and relies on the edge reference data
for precision — it emits a public quote/trade only once that `(venue, symbol)` instrument is known.
The outbound `wss://` client is the one place TLS is used (rustls + bundled webpki roots).

> **Caveat — trade dedup window vs. reconnect lag.** Cross-source trade dedup is a fixed-size
> windowed `trade_id` cache. A long public reconnect can deliver trades whose ids have aged out of
> the window during a high-volume burst, which would re-emit a duplicate trade. Sizing the window
> against the public feed's unbounded-lag failure mode is tracked separately (window-sizing issue);
> until then the window is a compile-time constant.
