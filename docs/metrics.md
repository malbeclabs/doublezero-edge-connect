# Metrics

`doublezero-edge-connect` exports Prometheus metrics covering the whole pipeline — multicast
ingest, the arbiter emit stage, the WebSocket output, and the Solana shred forwarder. They are
**recorded unconditionally** (a counter increment is a single relaxed atomic add, so the ingest hot
path pays nothing); the only thing the flag controls is whether they can be scraped.

## Enabling the endpoint

The metrics endpoint is **off by default**. Give it a bind address to turn it on:

```bash
./target/release/doublezero-edge-connect --iface doublezero1 --metrics-bind 127.0.0.1:9090
# then:
curl -s localhost:9090/metrics | grep '^dz_'
```

| Flag | Env | Default | Effect |
|------|-----|---------|--------|
| `--metrics-bind` | `METRICS_BIND` | *(empty)* | When non-empty, serves the Prometheus text format at `GET /metrics` (with a `GET /` / `GET /healthz` liveness probe). |

It is served by a hand-rolled minimal HTTP handler (no HTTP framework) on demand, fully off the
ingest hot path — see [`src/sinks/metrics.rs`](../src/sinks/metrics.rs). There is **no TLS**, as
with the rest of the service surface; terminate at a reverse proxy if you expose it beyond a trusted
network. See also [Output sinks](output-sinks.md).

## Naming and labels

- All series are prefixed `dz_` (`dz_ws_` for the WebSocket sink, `dz_shred_` for the shred
  forwarder). Counters end in `_total`; gauges do not. The standard Linux `process_*` collectors
  (CPU, resident memory, open fds) are also exported.
- **Labels are bounded by construction** — `venue` (a handful of feeds), `group`/`dest` (a handful
  of multicast groups / forward targets), and small fixed enums (`role`, `kind`, `outcome`). There
  are deliberately **no per-symbol labels**: a venue carries hundreds of symbols, which would
  explode the series count.

Both the ingest and client-output paths expose **message and byte** counters, so volume and
bandwidth can be tracked independently for each transport (UDP shred fan-out and WebSocket).

## Ingest reception (per feed)

Recorded by the multicast receivers (`src/ingest/receiver.rs`). Labelled by `venue`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_datagrams_received_total` | counter | `venue`, `role` | Datagrams received, split by port `role` (mktdata/refdata/snapshot/combined). |
| `dz_datagram_bytes_total` | counter | `venue` | Total bytes received (sum of datagram lengths). |
| `dz_socket_errors_total` | counter | `venue` | Socket/transport receive errors (each triggers a rejoin). |
| `dz_idle_rejoin_total` | counter | `venue` | Idle-rejoin watchdog firings (market data went silent past the idle window). |
| `dz_feed_up` | gauge | `venue` | `1` while the market-data multicast is up, `0` while down. |
| `dz_feed_stale_ms` | gauge | `venue` | Staleness in milliseconds: `0` while up; the staleness at the last `down` transition (reset to `0` on recovery). |
| `dz_seq_events_total` | counter | `venue`, `kind` | Frame-sequence classifications (`first`/`ok`/`reset`/`stale`). |

## Arbiter emit stage (per feed)

Recorded by the shared pre-broadcast emit stage (`src/ingest/arbiter.rs`). Labelled by `venue`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_emit_total` | counter | `venue`, `kind` | Messages broadcast after dedup, by `kind` (quote/trade/instrument/midpoint/depth). `status` is structurally possible but never routed through the arbiter today, so it is not recorded in practice. |
| `dz_quotes_admitted_total` | counter | `venue`, `publisher` | Quotes admitted by the staleness floor, attributed to the winning `publisher` (`edge`/`public`). A rise in `publisher="public"` is the direct signal of the public backstop filling an edge gap. |
| `dz_quotes_dropped_total` | counter | `venue` | Quotes dropped by the staleness floor (stale tick, non-leader, or exact repeat). |
| `dz_trades_dropped_total` | counter | `venue` | Trades dropped by the windowed dedup (duplicate `trade_id` still inside the window). |
| `dz_quotes_future_rejected_total` | counter | `venue` | Quotes rejected for an implausibly-far-future `source_ts`. |
| `dz_quotes_no_source_ts_total` | counter | `venue` | Quotes forwarded with the `source_ts == 0` sentinel (floor bypassed). |

## WebSocket output

Recorded by the WebSocket sink (`src/sinks/ws.rs`).

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_ws_clients` | gauge | — | Currently-connected WebSocket clients. |
| `dz_ws_connections_total` | counter | `outcome` | Connection attempts, by `outcome` (accepted/rejected). |
| `dz_ws_messages_sent_total` | counter | `kind` | Messages forwarded to clients, by `kind`. |
| `dz_ws_bytes_sent_total` | counter | `kind` | Bytes forwarded to clients, by `kind` (serialized JSON payload length). |
| `dz_ws_client_lagged_total` | counter | — | Times a slow client fell behind and the broadcast dropped messages for it. |
| `dz_ws_inbound_total` | counter | `kind` | Inbound control messages, by `kind` (ping/subscribe/unsubscribe/error). |
| `dz_ws_rate_limited_total` | counter | — | Clients disconnected for exceeding the inbound rate limit. |
| `dz_ws_idle_timeout_total` | counter | — | Clients reaped for crossing the idle timeout. |

## Public WS input feeders

Recorded by the optional public WebSocket backstops (Hyperliquid `src/ingest/ws_feeder.rs`, Phoenix
`src/ingest/phoenix_feeder.rs`; both off by default — see [Input sources](input-sources.md)). Every
series is labelled by `venue` so multiple feeders don't collide. Their quote/trade contribution to
the arbiter floor is attributed via `dz_quotes_admitted_total{publisher="public"}` above.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_ws_feeder_up` | gauge | `venue` | `1` while the public WS session is connected, `0` while down/reconnecting. |
| `dz_ws_feeder_reconnects_total` | counter | `venue` | (Re)connect cycles — a session ended or a connect attempt failed and the feeder backed off to retry. |
| `dz_ws_feeder_decode_errors_total` | counter | `venue` | Public WS frames that failed to decode (dropped best-effort). |
| `dz_ws_feeder_messages_total` | counter | `venue`, `kind` | Business messages decoded from the public WS and emitted, by `kind` (quote/trade). |

## Shred forwarder

Recorded by the Solana shred forwarder (`src/shred/mod.rs`); see
[Shred forwarding](shred-forwarding.md) for the pipeline. The receiver metrics are labelled by
source `group`; the per-stage tallies are process-wide; the fan-out is labelled by `dest`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_shred_datagrams_received_total` | counter | `group` | Shred datagrams received per source group. |
| `dz_shred_datagram_bytes_total` | counter | `group` | Total bytes received per source group. |
| `dz_shred_receiver_dropped_total` | counter | `group` | Datagrams dropped at the receiver (forwarder queue full — backpressure). |
| `dz_shred_processed_total` | counter | — | Datagrams that entered the dedup/forward gate. |
| `dz_shred_parsed_total` | counter | — | Datagrams successfully parsed (signature/slot/index extracted). |
| `dz_shred_unparsed_total` | counter | — | Datagrams that could not be parsed (forwarded undeduped, loss-averse). |
| `dz_shred_forwarded_total` | counter | — | Datagrams forwarded to destinations. |
| `dz_shred_dropped_total` | counter | — | Datagrams dropped by the dedup/sigverify gate. |
| `dz_shred_verify_ok_total` | counter | — | Shreds whose leader signature verified (sigverify mode only). |
| `dz_shred_no_leader_total` | counter | — | Shreds dropped fail-closed for want of a known slot leader (sigverify mode only). |
| `dz_shred_dedup_tracked_slots` | gauge | — | Slots currently tracked by the dedup window. |
| `dz_shred_sends_total` | counter | `dest`, `outcome` | Per-destination forward sends, by `dest` and `outcome` (ok/error). |
| `dz_shred_bytes_sent_total` | counter | `dest` | Bytes successfully forwarded per `dest` (a failed send delivers nothing and is not counted). |

## Throughput at a glance

| Path | Transport | Messages | Bytes |
|------|-----------|----------|-------|
| Ingest — market data | multicast | `dz_datagrams_received_total` | `dz_datagram_bytes_total` |
| Ingest — shreds | multicast | `dz_shred_datagrams_received_total` | `dz_shred_datagram_bytes_total` |
| Output — clients | WebSocket | `dz_ws_messages_sent_total` | `dz_ws_bytes_sent_total` |
| Output — clients | UDP (shred) | `dz_shred_sends_total` / `dz_shred_forwarded_total` | `dz_shred_bytes_sent_total` |
