# Output sinks

The decoded feed is fanned out to one or more **output sinks** (under
[`../src/sinks/`](../src/sinks/)), each an independent consumer of the internal broadcast
running off the ingest hot path, so enabling one never affects the others and a slow/failed
sink can't stall ingest. Every flag also reads from the env var shown.

| Sink | Default | Enable / disable | Config flags (env) |
|------|---------|------------------|--------------------|
| **WebSocket** (`sinks::ws`) | **on when subscribed** | configured unless `--ws-bind` is empty (`--ws-bind ""` disables it); *activated* only when ≥1 market-data feed is subscribed | `--ws-bind` (`WS_BIND`, default `0.0.0.0:8081`) + the `--ws-*` limits |
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
quotes/trades/midpoints/depth, with optional per-client subscribe/unsubscribe filtering and
heartbeat/limit enforcement.

> **Note:** when running via the installer one-liner, set these as env vars before the pipe (or
> with `docker run -e`). `WS_BIND=""` (disable the sink) **does** go through the installer —
> `WS_BIND` is forwarded whenever it is set, including set-but-empty — and the installer runs a
> host-side port preflight that flags a taken WS port before starting the container. A taken port
> is non-fatal regardless: the bridge logs the bind failure and runs without the sink. See
> [Configure](../README.md#configure-override-the-one-liner).
