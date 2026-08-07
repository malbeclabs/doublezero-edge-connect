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

## Ingest reception (per publisher)

Recorded by the multicast receivers (`src/ingest/receiver.rs`), one per `(venue, kind, publisher)` —
a venue mirrored by six publishers runs six receivers per protocol. The `publisher` label value is
the publisher's **base port** (the market-data port of its block, e.g. `9201`), which is what
`--publisher-port` selects.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_datagrams_received_total` | counter | `venue`, `kind`, `publisher`, `role` | Datagrams received per publisher, split by port `role` (mktdata/refdata/snapshot/combined). |
| `dz_datagram_bytes_total` | counter | `venue`, `kind`, `publisher` | Total bytes received per publisher. |
| `dz_socket_errors_total` | counter | `venue`, `kind`, `publisher` | Socket/transport receive errors per publisher (each triggers a rejoin). |
| `dz_idle_rejoin_total` | counter | `venue`, `kind`, `publisher` | Idle-rejoin watchdog firings per publisher. |
| `dz_receiver_up` | gauge | `venue`, `kind`, `publisher` | `1` while this publisher's market-data stream is up, `0` while down. The per-publisher counterpart of `dz_feed_up`. |
| `dz_feed_up` | gauge | `venue` | `1` while *any* publisher of the venue is up, `0` once every one has gone silent. |
| `dz_feed_stale_ms` | gauge | `venue` | Staleness in milliseconds: `0` while up; the staleness at the last venue-level `down` transition (reset to `0` on recovery). |
| `dz_seq_events_total` | counter | `venue`, `kind` | Frame-sequence classifications (`first`/`ok`/`reset`/`stale`). Incremented in the processor, which demultiplexes publishers by source IP and so has no configured base port — hence no `publisher` label. |

> **Label change (multi-publisher):** the four receiver counters gained `kind` and `publisher` when
> a feed became N publishers rather than one port block. Aggregating queries (`sum by (venue)`,
> `rate(...)` summed over labels) are unaffected; queries that match the old exact label set
> (`dz_datagram_bytes_total{venue="Hyperliquid"}` as an instant selector) now match six series per
> protocol instead of one. `dz_feed_up` / `dz_feed_stale_ms` are deliberately still venue-level
> **aggregates**: a venue reads down only once every one of its **quote-bearing** publishers (the
> Top-of-Book/Midpoint receivers — `status` is the quote feed's health, so a depth-only
> Market-by-Order mirror neither declares an outage nor masks one) has gone silent. A quote receiver
> that *stops* (aborted, exited, bind error) leaves the aggregate down rather than handing it to a
> depth-only peer, so a venue whose Top-of-Book receivers all died reads `0`, not `1`. A venue with
> `dz_feed_up == 1` and some `dz_receiver_up == 0` has a wedged mirror — worth its own alert.
>
> **Arbiter-side semantics drift under unchanged names.** Ingesting N mirrors instead of one changes
> what two existing series *mean*, without renaming them — re-baseline any dashboard or alert built
> on them:
> - `dz_quotes_dropped_total` / `dz_depth_dropped_total` go from ≈0 to ≈`(N-1)/N` of all samples.
>   That is the cross-publisher collapse working as designed, not loss; alert on the *ratio changing*
>   rather than on an absolute rate.
> - `dz_quote_lead_ns{winner="edge",loser="edge"}` — previously empty — becomes the dominant series
>   and measures **inter-mirror skew**. The headline "DZ beats the public feed" margin remains
>   `{winner="edge",loser="public"}` only.

## Arbiter emit stage (per feed)

Recorded by the shared pre-broadcast emit stage (`src/ingest/arbiter.rs`). Labelled by `venue`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_emit_total` | counter | `venue`, `kind` | Messages broadcast after dedup, by `kind` (quote/trade/instrument/midpoint/depth/book). `status` is structurally possible but never routed through the arbiter today, so it is not recorded in practice. |
| `dz_quotes_admitted_total` | counter | `venue`, `publisher` | Quotes admitted by the staleness floor, attributed to the winning `publisher` (`edge`/`public`). A rise in `publisher="public"` is the direct signal of the public backstop filling an edge gap. |
| `dz_trades_admitted_total` | counter | `venue`, `publisher` | Trades admitted by the windowed dedup, attributed to the winning `publisher` (`edge`/`public`). A rise in `publisher="public"` is the trade-side signal of the public backstop filling an edge gap — the counterpart to `dz_quotes_admitted_total` for a trades-only backstop like Phoenix. |
| `dz_quote_ticks_won_total` | counter | `venue`, `publisher` | Quote `source_ts` ticks **won** — the once-per-tick first delivery, attributed to the winning class. Every tick counts exactly once: a mirror's copy or the leader's later in-tick contents never re-count it, and a tick the public feed never delivers still counts for the edge (the walkover). `edge / sum` is the published DZ win rate (see below). `source_ts == 0` sentinel quotes bypass the floor and are not counted. |
| `dz_depth_ticks_won_total` | counter | `venue`, `publisher` | The depth mirror of `dz_quote_ticks_won_total` (for depth the `source_ts == 0` empty-anchor tick is real and counts). |
| `dz_quotes_dropped_total` | counter | `venue` | Quotes dropped by the staleness floor (stale tick, non-leader, or exact repeat). |
| `dz_trades_dropped_total` | counter | `venue` | Trades dropped by the windowed dedup (duplicate `trade_id` still inside the window). |
| `dz_trades_no_id_total` | counter | `venue` | Trades forwarded with the `trade_id == 0` sentinel, bypassing the dedup window. A FIX-sourced publisher carries no venue trade id; keying the window on `0` would collapse the tape to one print per `(venue, symbol)` forever. A non-zero rate here means the venue's tape is un-deduped by construction — correct, but it relies on exactly one publisher owning the venue's tape (watch `dz_trades_no_id_conflict_total`). Not counted as admitted: nothing was. |
| `dz_trades_no_id_conflict_total` | counter | `venue` | Zero-id trades forwarded from a **second, concurrent** publisher for a `(venue, symbol)` another already owns. A bypassed sentinel has nothing to collapse against, so every print on this counter is a duplicate already on the wire: a consumer summing `size` overstates volume by the number of emitting publishers. Should be flat at zero — a non-zero rate means two publishers are emitting one venue's tape and one of them has to stop. A *failover* is not counted: a publisher inheriting a tape that has been quiet for 5s takes ownership silently, so this stays a signal about concurrency rather than about which arm is live. |
| `dz_instruments_dropped_total` | counter | `venue` | Instrument definitions dropped as an unchanged repeat of the last content broadcast for that `(venue, symbol)`, within the re-announce interval (`INSTRUMENT_REANNOUNCE_NS`, 15s) — the mirrored publishers' identical reference-data bursts collapsing. Expect nearly all definition frames in steady state: the dedup is a rate limit, not a latch, so unchanged content is still re-broadcast on the first burst past the interval (that periodic re-announce is what heals a client which lost an `instrument` to drop-oldest backpressure). A precision *change* is never suppressed. |
| `dz_quotes_future_rejected_total` | counter | `venue` | Quotes rejected for an implausibly-far-future `source_ts`. |
| `dz_quotes_no_source_ts_total` | counter | `venue` | Quotes forwarded with the `source_ts == 0` sentinel (floor bypassed). |
| `dz_quote_lead_ns` | histogram | `venue`, `winner`, `loser` | Nanoseconds the winning publisher led the losing duplicate by, per quote-tick cross-source contest (`winner`/`loser` each `edge`/`public`). `{winner="edge",loser="public"}` is "DZ beat the public feed"; `_count` is the head-to-head win count, the buckets the lead margin. |
| `dz_trade_lead_ns` | histogram | `venue`, `winner`, `loser` | The trade-side counterpart of `dz_quote_lead_ns`, per `trade_id` cross-source contest. |
| `dz_arm_lead_ns` | histogram | `venue`, `winner` | Nanoseconds between the two arms' copies of the **same matched trade**, on our own receive clock. `winner` is `leader`/`challenger` — relative, so the label set stays two-valued whatever the arm count. This is what the transfer thresholds are read off: `{winner="challenger"}` sitting persistently past `--arb-transfer-margin-us` with no transfer means the conditions are too tight. Fed only by matched pairs, never by a dropped non-authoritative copy — the interval to the leader's *previous, unrelated* message is inter-arm phase, not a lead, and is structurally non-negative. |
| `dz_arm_authority_transfers_total` | counter | `venue`, `reason` | Authority handovers, by `reason` (`initial`/`health`/`silence`/`margin`). Every transfer re-baselines each consumer's book, so a sustained rate means the thresholds are too loose; a `health`/`silence` rate means an arm is actually broken. `health` is a single market changing hands; the other three are venue-wide. |
| `dz_arm_markets_held` | gauge | `venue`, `arm` | Markets each arm is currently **serving** — the venue's elected arm, plus any market a health override moved to a peer. `arm` is a stable per-venue ordinal (`arm0`…`arm7`, then `other`), never the spoofable source IP. All markets on one arm is the steady state; a persistent split means health overrides are fragmenting the venue, i.e. the elected arm's books keep gapping. |

**None of the three emits until the incremental book path wires a caller** (plan Task 12). The series are registered so a dashboard can be built against them, but a scrape today reports nothing for them; do not read an empty result as a healthy venue.

### Tuning arm re-election

Speed and silence are judged **per arm, venue-wide** — latency is a property of an arm, so every matched sample from a source IP counts toward it whatever market carried it. Health is the one per-market rule, and it overrides the elected arm for that market alone. Five flags govern it (all also env vars, `DZ_ARB_*`):
`--arb-sample-interval-secs` (300) is how long a window pools matched samples before it can transfer, and so the ceiling on how long a persistently slower arm keeps authority;
`--arb-transfer-margin-us` (1000) is the median lead a challenger must show;
`--arb-transfer-win-rate` (0.8) is the fraction of its own samples it must also lead;
`--arb-min-window-samples` (32) is how many matched samples it needs before the window is judged at all;
`--arb-leader-timeout-secs` (2) is the venue-wide silence after which a live arm takes over.

The margin and the win rate are **independent conditions and all three must hold** — a heavy tail alone cannot carry a transfer, neither can a high win count built on sub-margin noise, and neither can a handful of lucky matches. Health and silence ignore all three: a leader whose book for one market sits in `gap`/`awaiting-snapshot` yields *that market* to a healthy arm immediately, because under incremental output a lost level does not self-heal until the next snapshot.

### Published win rate

The DZ win rate to publish is the tick-won share:

```promql
sum(rate(dz_quote_ticks_won_total{publisher="edge"}[5m]))
/
sum(rate(dz_quote_ticks_won_total[5m]))
```

Do **not** derive a win rate from `dz_quote_lead_ns_count`. Contests sample only in-tick
head-to-heads — at most one per tick, consumed by whichever follower arrives first (on a
dual-publisher feed usually the mirror's sub-ms copy, which swallows the edge-vs-public race) —
and a losing copy that arrives after the floor has already advanced past its tick is a plain
stale drop that never counts. Ratios built on the contest histograms therefore systematically
understate the edge. The `dz_*_lead_ns` histograms remain the *margin* diagnostic: how far ahead
the winner was when the losing duplicate arrived.

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
series is labelled by `venue` so multiple feeders don't collide. A feeder's actual contribution to the
served feed shows up on the arbiter counters above, attributed to `publisher="public"`:
`dz_quotes_admitted_total` for a quote backstop, `dz_trades_admitted_total` for a trade backstop
(Phoenix is **trades-only**, so watch the trade counter, not quotes), with `dz_quote_lead_ns` /
`dz_trade_lead_ns` giving the win margin over the source it beat.

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
