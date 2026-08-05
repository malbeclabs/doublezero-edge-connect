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
| **DZ Edge multicast** | **on** | always on | `--feed` selects feed rows by venue · `--publisher` narrows the publishers within them (default: every publisher of every selected feed) · `--iface`/`--recv-buf` |
| **Hyperliquid public WS** (`ingest::ws_feeder`) | **off** | on when `--ws-input-coins` is non-empty | `--ws-input-coins` (`WS_INPUT_COINS`, e.g. `BTC,ETH`) · `--ws-input-url` (`WS_INPUT_URL`, default `wss://api.hyperliquid.xyz/ws`) |
| **Phoenix public WS** (`ingest::phoenix_feeder`) | **off** | on when `--phoenix-ws-input-markets` is non-empty | `--phoenix-ws-input-markets` (`PHOENIX_WS_INPUT_MARKETS`, bare tickers e.g. `SOL,BTC`) · `--phoenix-ws-input-url` (`PHOENIX_WS_INPUT_URL`, default `wss://perp-api.phoenix.trade/v1/ws`) |

One receiver task runs per `(venue, protocol, publisher)`, so a six-publisher venue runs six
receivers per protocol. Each is a full receiver — and for Market-by-Order a full independent book —
so `--publisher` (`DZ_PUBLISHERS`) is the release valve for capping ingest cost or bisecting a
misbehaving publisher.

> **Size `--recv-buf` against the socket count, not one socket.** Every port of every publisher is
> its own socket requesting `--recv-buf` (default 8 MiB): the six-publisher Hyperliquid fleet binds
> 30 sockets (6 × 2 Top-of-Book + 6 × 3 Market-by-Order) where it previously bound 5, so the
> requested `SO_RCVBUF` total goes from ~40 MiB to ~240 MiB. `net.core.rmem_max` clamps each socket
> individually and will not catch the aggregate — lower `DZ_RECV_BUF` or narrow `--publisher` if that
> exceeds the host's or container's memory budget. Market-by-Order additionally holds one independent
> L3 book set per publisher.

```bash
# From source — run the edge multicast feed with the public WS backstop for BTC and ETH:
./target/release/doublezero-edge-connect --feed Hyperliquid --ws-input-coins BTC,ETH

# Via the installer one-liner, as env vars before the pipe:
WS_INPUT_COINS=BTC,ETH curl -fsSL https://get.doublezero.xyz/connect | bash
```

Every public feeder is failure-isolated (its own task with reconnect + exponential backoff;
decode/socket errors are logged and never touch the multicast hot path) and relies on the edge
reference data for precision — it emits a public quote/trade only once that `(venue, symbol)`
instrument is known. The outbound `wss://` client is the one place TLS is used (rustls + bundled
webpki roots).

The **Phoenix** feeder is **trades only** — it does not backstop quotes, because the edge Phoenix
Quote is a spline-blended BBO while Phoenix's public orderbook channel is resting-only (a different
quantity). Phoenix names each market with the same bare ticker on the edge and public feeds (the edge
`instrument_id` equals the public `assetId`), so the configured symbol is used verbatim — to subscribe
the public feed and to tag the emitted trade — and trade dedup keys on the public `tradeSequenceNumber`.
Validated against a concurrent edge+public capture (2026-06-30): the public `tradeSequenceNumber`
equals the edge `trade_id` on every shared fill (257/257) and `side` maps `bid -> buy` / `ask -> sell`.

> **Caveat — trade dedup window vs. reconnect lag.** Cross-source trade dedup is a fixed-size
> windowed `trade_id` cache. A long public reconnect can deliver trades whose ids have aged out of
> the window during a high-volume burst, which would re-emit a duplicate trade. Sizing the window
> against the public feed's unbounded-lag failure mode is tracked separately (window-sizing issue);
> until then the window is a compile-time constant.
