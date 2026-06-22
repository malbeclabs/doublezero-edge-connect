# Output sinks

The decoded feed is fanned out to one or more **output sinks** (under
[`../src/sinks/`](../src/sinks/)), each an independent consumer of the internal broadcast
running off the ingest hot path, so enabling one never affects the others and a slow/failed
sink can't stall ingest. Every flag also reads from the env var shown.

| Sink | Default | Enable / disable | Config flags (env) |
|------|---------|------------------|--------------------|
| **WebSocket** (`sinks::ws`) | **on** | on unless `--ws-bind` is empty; `--ws-bind ""` disables it | `--ws-bind` (`WS_BIND`, default `0.0.0.0:8081`) + the `--ws-*` limits |

A sink is active when its key config value is non-empty/present; the WebSocket sink simply ships
a non-empty default bind, so it is on unless you explicitly clear it.

The WebSocket sink implements the [PROTOCOL.md](../PROTOCOL.md) v1 surface: on connect it
replays the instrument snapshot (precision first) then the latest depth per symbol, then streams
quotes/trades/midpoints/depth, with optional per-client subscribe/unsubscribe filtering and
heartbeat/limit enforcement.

> **Note:** when running via the installer one-liner, set these as env vars before the pipe (or
> with `docker run -e`). The `WS_BIND=""` disable case can't go through the installer (it only
> forwards non-empty values) — run a hand-written `docker run` for that. See
> [Configure](../README.md#configure-override-the-one-liner).
