# Output sinks

The decoded feed is fanned out to one or more **output sinks** (under
[`../src/sinks/`](../src/sinks/)), each an independent consumer of the internal broadcast
running off the ingest hot path, so enabling one never affects the others and a slow/failed
sink can't stall ingest. Every flag also reads from the env var shown.

| Sink | Default | Enable / disable | Config flags (env) |
|------|---------|------------------|--------------------|
| **WebSocket** (`sinks::ws`) | **on when subscribed** | configured unless `--ws-bind` is empty (`--ws-bind ""` disables it); *activated* only when ≥1 market-data feed is subscribed | `--ws-bind` (`WS_BIND`, default `0.0.0.0:8081`) + the `--ws-*` limits |
| **Query API** (`sinks::api`) | **on when subscribed** | configured unless `--api-bind` is empty (`--api-bind ""` disables it); *activated* only when ≥1 market-data feed is subscribed — same condition as the WebSocket sink | `--api-bind` (`DZ_API_BIND`, default `127.0.0.1:9099`) |
| **Metrics** (`sinks::metrics`) | **off** | on when `--metrics-bind` is non-empty | `--metrics-bind` (`METRICS_BIND`, default empty) |

The metrics endpoint is active when its key config value is non-empty. The WebSocket sink and the
query API both ship a non-empty default bind (so each is *configured* unless you clear it), but the
**subscription reconciler** only *activates* either one once this host is actually subscribed to a
market-data feed — so a shreds-only host serves neither and can't collide with an existing `:8081`
or `:9099` service, with no manual config. Both listeners are bound non-fatally: a taken port
disables the sink for that cycle (retried on the next reconcile) but never crash-loops the process
or the DoubleZero tunnel. Running from source without the `doublezero` CLI, gating falls open and
both sinks are active whenever configured. See the main README for the reconciler flags
(`--subscription-refresh-secs`, `--subscription-gating-disable`).

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

## Query API (`/v1`)

A read-only, JSON-over-HTTP query API (`sinks::api`) sits beside the WebSocket and metrics sinks. It
answers from state the bridge already maintains — the instrument catalog, a rolling one-hour history
of OHLCV candles and recent prints, the MBO/MBP book replay state, and per-venue feed health — and
never mutates anything: every route is a `GET`.

```bash
./target/release/doublezero-edge-connect --iface doublezero1 --api-bind 127.0.0.1:9099
curl -s localhost:9099/v1/products | jq .
```

Routes: `GET /v1/products` (catalog; optional `limit`/`cursor` paging — unset `limit` returns every
product in one response, no page size imposed unless asked), `GET /v1/products/{id}` (one product's identity/registry
fields), `GET /v1/products/{id}/ticker` (recent trades + best bid/ask), `GET
/v1/products/{id}/candles` (OHLCV, `granularity`/`limit` query params), `GET /v1/products/{id}/book`
(order book), `GET /v1/best_bid_ask` (best bid/ask, optionally narrowed with a comma-separated
`product_ids` query param), and `GET /v1/status`
(per-venue feed health plus history-store stats). `{id}` is `SOURCE:SYMBOL` (e.g. `HYPERLIQUID:BTC`),
with an `#<channel>.<instrument_id>` suffix needed only where a bare symbol collides across markets.
The [`doublezero-edge`](../README.md#query-market-data-the-doublezero-edge-cli) CLI is a thin client
over this surface.

**Candles cover a rolling one hour, in memory, and do not survive a restart.** The history buffer
(`src/history.rs`) is a bounded per-product set of 1-second OHLCV buckets plus a ring of recent
prints, fed from the post-arbiter broadcast — so every print arriving here is already deduplicated on
`trade_id` and gated by the tape leader, one copy per print — with **no persistence of any kind**: the
window is gone the moment the process restarts. Every `candles` response carries its own `retention`
block (`window_seconds`, `oldest`/`newest`, `truncated`, `held`), so a caller can tell a full window
from a `limit`-truncated one without guessing — and, via `held`, a product the store still tracks
(genuinely no trades in the window) from one it no longer tracks at all (evicted for capacity, or
never seen), which otherwise look like the same empty response. Every `book`/depth response carries
a `coverage` block with the same honesty (`complete: false` rather than a guess whenever the served
levels might not be all of them).

**The catalog is not necessarily every instrument the feed defines.** A product is listed in
`/v1/products` once its source is known: immediately, for a publisher whose reference data carries
its own Source ID; only after its first price, for a publisher whose reference data carries no Source
ID of its own (see PROTOCOL.md's [*A symbol appears only once its source is
known*](../PROTOCOL.md#a-symbol-appears-only-once-its-source-is-known)). Both publisher generations
can be live on the same host at once, so a defined-but-never-traded instrument on the latter kind of
publisher is legitimately absent from the catalog while every instrument from the former kind appears
up front.

**Loopback (`127.0.0.1:9099`) by default, and that default is load-bearing.** The container runs host
networking, so a wildcard bind here would be genuinely reachable off the host — and this surface has
**no authentication and no TLS**, same as the rest of the service surface. Terminate at a reverse
proxy if it must be exposed beyond a trusted network.

Activation mirrors the WebSocket sink exactly: *configured* by a non-empty `--api-bind`
(`--api-bind ""` disables it outright), *activated* by the subscription reconciler only once this
host is subscribed to ≥1 market-data feed, and bound non-fatally — a taken port disables the API for
that reconcile cycle rather than crash-looping the tunnel.

## Admin surface (`/admin`)

The one **mutating** surface in this crate (`sinks::admin`), deliberately kept off `/v1` so that
stays provably read-only. It is **on by default at loopback** (`--admin-bind` / `DZ_ADMIN_BIND`,
`127.0.0.1:9098`; set empty to disable it outright) and — unlike the WebSocket sink and the query
API — is **not subscription-gated**, which is the property the whole surface hangs on: an operator
must be able to inspect a channel filter before anything is subscribed, and to diagnose a host that
is subscribed to nothing at all.

| Route | |
|---|---|
| `GET /admin/channels` | The channel filter in force, and which publishers/channels it **admits** (not which receivers are running — `GET /v1/status`'s `channels` block reports real liveness) |
| `POST /admin/channels?channels=<spec>` | Replace the channel filter, validated by the same parser `--channels` uses at startup. Applies on the reconciler's next tick |
| `GET /admin/diagnostics` | Tunnel/subscription/activation state plus one verdict — below |

### `GET /admin/diagnostics`

Answers "why is nothing being served?" on the one host where nothing else can: `/v1` activates only
once a market-data feed is subscribed, so on a host whose tunnel never came up it is not listening
and every query fails with a transport error while `docker ps` shows a healthy container.

The response is cached state plus a pure function — no shell-out on the read path. The reconciler
already runs `doublezero status --json` every `--subscription-refresh-secs` for activation; this
reports the session fields it was previously discarding (`session_status`, `tunnel_name`,
`user_type`, `current_device`, `lowest_latency_device`, `metro`, `network`), the subscribed group
codes split into market-data / shred / other, the running receivers with their real liveness, which
sinks are up, the resolved feed-registry document, and the process block. Reading them **cannot**
move activation: every field is optional and additive, and `multicast_groups` remains the sole
input to what runs.

`diagnosis` is an ordered ladder over that state, each rung reached only once the one above it is
ruled out — so a host with no `doublezero` CLI is never reported as a broken tunnel:

| `code` | |
|---|---|
| `pending` | No poll has completed yet |
| `dz_cli_missing` | No `doublezero` CLI (running from source); gating falls open |
| `daemon_unreachable` | `doublezero status` failed — quotes what it printed, usually `Please start the doublezerod service.` |
| `tunnel_down` | A session reports a status, and none of them is `BGP Session Up` |
| `tunnel_state_unknown` | The document parsed but carried no session status this build recognizes — "not up" is a claim the snapshot cannot support |
| `no_market_data_subscriptions` | Tunnel up, but no subscribed group matches a feed this build serves |
| `subscribed_no_traffic` | Receivers running, none delivering — usually a default-deny host firewall on `doublezero1` |
| `no_receivers_running` | Feeds were expected to run but none does: `--feed`/`--publisher-port`/the channel filter excluded every publisher of a subscribed row |
| `gating_disabled` / `ok` | Nothing to fix |

A receiver actually delivering packets is proof the tunnel is up, so the two tunnel rungs are
skipped outright in that case: one upstream rename of `session_status` must not report
`tunnel_down` fleet-wide and send every healthy host to reconnect a tunnel that was never down. A
transient `doublezero status` failure keeps the last good session and code data rather than blanking
it — `last_ok_at_unix` reports how stale it is, and only a successful poll stamps it (`cli_missing`
and `gating_disabled` run no status call, so they leave it unset rather than dating an empty
document to now).

A `doublezero latency --json` read is reported under `latency` (device code/IP, reachability,
min/avg/max ns) with its own `latency_at_unix`. Bounded at 20s and killed on overrun — it runs
inline in the reconciler's tick, so a stall would otherwise stop receivers being respawned as well
as freezing the diagnostic. Probed every 5 minutes rather than every tick — it
is active measurement against every device, while what it reports (a device nearer than the one
this host is on) moves with topology — and the last result is carried across the ticks that skip
it. `null` means never probed; `[]` means probed and nothing answered.

`doublezero-edge diagnose` renders this. It only reports: the retry the `tunnel_down` rung names is
`doublezero connect multicast` inside the container.

### Guards on the mutating route

`POST /admin/channels` requires an `X-DZ-Admin-Request` header (any value) and refuses a request
body. The header is not authentication — this surface has none — it rules out a request a browser
page on the same host could have caused by accident, which a `<form>` cannot produce. `GET` routes
are exempt: requiring it on the diagnostics read would make the one command a stuck operator most
needs harder to run than `curl`.

`GET /admin/diagnostics` is read-only but is the most informative route here (device/metro names,
subscribed group codes and their multicast IPs, the probed devices' codes and IPs, every
configured bind, the feed-registry URL). It is
unauthenticated like the rest of the surface, and DNS rebinding can read it — a page served from a
name re-pointed at `127.0.0.1` is same-origin, so the header above does not stop a *read*. Refusing
a `Host` that names a DNS name would close that at the cost of `--admin-url http://myhost.local:9098`;
the read is left open deliberately. Nothing on this surface can change the tunnel.
