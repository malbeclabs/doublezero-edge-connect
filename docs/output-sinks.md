# Output sinks

The decoded feed is fanned out to one or more **output sinks** (under
[`../src/sinks/`](../src/sinks/)), each an independent consumer of the internal broadcast
running off the ingest hot path, so enabling one never affects the others and a slow/failed
sink can't stall ingest. Every flag also reads from the env var shown.

| Sink | Default | Enable / disable | Config flags (env) |
|------|---------|------------------|--------------------|
| **WebSocket** (`sinks::ws`) | **on when subscribed** | configured unless `--ws-bind` is empty (`--ws-bind ""` disables it); *activated* only when ≥1 market-data feed is subscribed | `--ws-bind` (`WS_BIND`, default `0.0.0.0:8081`) + the `--ws-*` limits |
| **Hyperliquid-compatible** (`sinks::hyperliquid`) | **off** | on when `--hl-ws-bind` is non-empty; **not** subscription-gated | `--hl-ws-bind` (`HL_WS_BIND`, default empty) |
| **Metrics** (`sinks::metrics`) | **off** | on when `--metrics-bind` is non-empty | `--metrics-bind` (`METRICS_BIND`, default empty) |

The metrics endpoint is active when its key config value is non-empty. The WebSocket sink ships a
non-empty default bind (so it's *configured* unless you clear it), but the **subscription
reconciler** only *activates* it once this host is actually subscribed to a market-data feed —
so a shreds-only host serves no WebSocket and can't collide with an existing `:8081` service, with
no manual config. Its listener is bound non-fatally: a taken port disables the sink for that cycle
(retried on the next reconcile) but never crash-loops the process or the DoubleZero tunnel. Running
from source without the `doublezero` CLI, gating falls open and the sink is active whenever
configured. See the main README for the reconciler flags (`--subscription-refresh-secs`,
`--subscription-gating-disable`).

## Hyperliquid-compatible sink

Serves the same market data in **Hyperliquid's own WebSocket schema** instead of
[PROTOCOL.md](../PROTOCOL.md)'s, so an existing Hyperliquid client consumes edge-connect by changing
one URL. It is **off by default**; give it a bind address to turn it on:

```bash
./target/release/doublezero-edge-connect --iface doublezero1 --hl-ws-bind 0.0.0.0:8082
```

It is a **rendering, not a second protocol**: it holds no book of its own, adds no deduplication and
influences no arbitration, reading the same internal broadcast the normalized sink reads. Nothing
about it belongs in PROTOCOL.md, which is the contract for our own protocol. Scoped to the
Hyperliquid venue — `coin` is our `symbol`, and no other venue is rendered. Not subscription-gated
(it has no multicast group of its own to be subscribed to); like the normalized sink, a bind failure
disables it with a warning rather than taking the process down.

Three channels, subscribed with Hyperliquid's own control frame
(`{"method":"subscribe","subscription":{"type":…,"coin":…}}`), acknowledged with a
`subscriptionResponse` echo, and `{"method":"ping"}` answered with `{"channel":"pong"}`:

| Channel | Contract |
|---|---|
| `l2Book` | Snapshot-per-update: every frame carries the whole top-N of both sides, `px`/`sz` as strings and `n` as the resting-order count at that price. Honours `nSigFigs` (2–5), `mantissa` (1, 2 or 5, only at `nSigFigs` 5) and `nLevels` (default 20, clamped to 100). Bids bucket down and asks bucket up, so aggregation never invents a price better than the book holds. |
| `l4Book` | The whole resting book order by order, then order diffs — each carrying the **venue's own order id**. Externally tagged `{"Snapshot":{…}}` / `{"Updates":{…}}`, matching the contract DoubleZero's own Hyperliquid publisher defines. A producer re-baseline arrives as another `Snapshot`, since the channel has no clear. |
| `trades` | Our `trade`, in Hyperliquid's envelope: string price and size, the aggressor side spelled `B`/`A`, and the venue's own `tid`. |

**What a stock NautilusTrader client actually gets, and what it does not.** Set
`HyperliquidDataClientConfig.base_url_ws` to this sink and a v1.227.0 Hyperliquid trader receives
genuine full-depth L2 with no adapter change. It **cannot** receive L3: its Rust WebSocket client has
no `l4Book` subscription, so nothing in Nautilus will ever ask for one, and its book path hardcodes
order id `0`. Order-level is available to a client that asks for `l4Book` — our own adapter, or
Nautilus once someone extends it. "Hyperliquid-compatible" does not mean "Nautilus gets L3".

**What the wire cannot supply.** Hyperliquid's schema describes a venue that also owns the account
model; our input is an order book. A trade's `users` and `hash`, an order's `user`, `timestamp` and
its `triggerCondition`/`orderType`/`tif`/`cloid` block, and `l4Book`'s `height` are emitted as null or
zero so a client written against the publisher still parses the frames — a consumer must not read
meaning into them. In particular `timestamp` is **0, not the book's event time**: stamping the event
time would give every order in a snapshot the same plausible placement time, which a consumer ranking
queue priority or ageing orders would read as real. `l4Book`'s `order_statuses` is always empty for
the same reason, and its order diffs use `new`/`remove` only: our changes carry an order's absolute
resulting quantity and no prior one, so the publisher's `update{origSz,newSz}` could only be
fabricated. On `trades`, a print whose aggressor side the venue did not report is **dropped** rather
than guessed: `side` is the one field on that channel a consumer acts on directionally and
Hyperliquid's schema has no "unknown", so the compat tape can be shorter than the normalized one.

Both book channels publish a market only once the bridge holds its **complete** book. `l2Book`
replaces a consumer's book wholesale on every frame and an `l4Book` snapshot claims completeness, so
a market accumulated mid-stream — before a producer re-baseline — is withheld rather than published
as if it were whole.

Client limits are fixed rather than configurable (64 clients, 256 subscriptions each, 600 inbound
control frames per minute, a 20s heartbeat with a 60s idle reap). The rate limit is the one that
matters: a subscribe frame is tens of bytes and can cost a whole book to answer. The shared book
map's mutex is the one the ingest emit path takes on every published batch, so the sink does exactly
one thing under it: clone the market's accumulator. Every rendering step — the price fold, the order
set, the decimal formatting, the JSON — runs after the guard drops. The clone is the cheapest
snapshot on offer and not a fallback: on the 44,598-order fixture it costs ~0.45 ms against ~9.1 ms
to fold under the guard and ~5.6 ms to materialize the order set there. It is still O(book) inside a
process-wide mutex, so enabling this sink on a very large book is a measurable cost to *every* feed's
ingest; a copy-on-write accumulator would remove it and has not been built.
Observability is `dz_hl_sink_clients` and `dz_hl_sink_messages_total{channel}`. There is **no TLS**,
as with the rest of the service surface.

## Metrics (Prometheus)

The metrics endpoint exposes the bridge's internal counters and gauges in the Prometheus text
format at `GET /metrics` (with a `GET /` / `GET /healthz` liveness probe). It is **off by default**;
give it a bind address to turn it on:

```bash
./target/release/doublezero-edge-connect --iface doublezero1 --metrics-bind 127.0.0.1:9090
# then: curl -s localhost:9090/metrics | grep '^dz_'
```

It is the one "sink" that does **not** consume the broadcast — it serves the metric registry on
demand, fully off the ingest hot path. Metrics are recorded regardless of whether the endpoint is
enabled; the flag only controls whether they can be scraped. There is **no TLS** (as with the rest
of the service surface) — terminate at a reverse proxy if you expose it beyond a trusted network.

Exported series (all prefixed `dz_` / `dz_ws_` / `dz_shred_`, plus the standard Linux `process_*`):
ingest reception per feed (datagrams, bytes, socket errors, idle rejoins, feed up/stale, frame
sequence events); the arbiter emit stage (messages broadcast, quotes/trades dropped by dedup,
future/zero-timestamp quotes); the WebSocket sink (connected clients, connections accepted/rejected,
messages sent, slow-client lags, inbound control messages, rate-limit/idle disconnects); and the
shred forwarder (received/dropped per group, processed/parsed/forwarded/dropped, sigverify outcomes,
dedup tracked slots, per-destination sends). Labels are deliberately low-cardinality (`venue`,
`group`, `dest`, and small fixed enums — **never per-symbol**).

The WebSocket sink implements the [PROTOCOL.md](../PROTOCOL.md) v1 surface: on connect it
replays the instrument snapshot (precision first) then the latest depth per symbol, then streams
quotes/trades/midpoints/depth, with optional per-client subscribe/unsubscribe filtering (by `venue`,
`symbol`, `channel` and message `type`, with a replay scoped to each new subscription) and
heartbeat/limit enforcement.

> **Note:** when running via the installer one-liner, set these as env vars before the pipe (or
> with `docker run -e`). `WS_BIND=""` (disable the sink) **does** go through the installer —
> `WS_BIND` is forwarded whenever it is set, including set-but-empty — and the installer runs a
> host-side port preflight that flags a taken WS port before starting the container. A taken port
> is non-fatal regardless: the bridge logs the bind failure and runs without the sink. See
> [Configure](../README.md#configure-override-the-one-liner).
