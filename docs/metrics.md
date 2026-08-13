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
a venue mirrored by eleven publishers runs eleven receivers per protocol. The `publisher` label is
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

## Source ID resolution (`src/ingest/processor.rs`, `src/ingest/sources.rs`)

Recorded while a processor resolves a message's wire Source ID to a registry venue name (any
protocol — TOB, Midpoint, MBO, MBP all go through this), not by the arbiter's emit stage.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_source_id_changed_total` | counter | `venue` | A `(publisher, instrument)` already revealed under one wire Source ID named a different one on a later message — a publisher defect, not a decode issue. Labelled by the **new** (post-change) venue, which is also re-announced a fresh `instrument` under so the new venue's precision-before-price guarantee still holds. Should stay flat at zero; a nonzero rate means a publisher is relabeling an instrument's venue mid-stream. |
| `dz_unregistered_sources_total` | counter | — | Distinct Source IDs seen with no registry row. |
| `dz_unregistered_source_labels_capped_total` | counter | — | Messages labelled `UNREGISTERED` because the distinct-unregistered-ID cap was reached. |

## Market-by-Price processor (per venue)

Recorded by the Market-by-Price processor (`src/ingest/processor.rs`), which demultiplexes publishers by source IP and so carries no `publisher` label. Labelled by `venue`. **No `FEEDS` row selects `MarketByPrice` yet**, so a scrape today reports nothing here; do not read an empty result as a healthy book.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_mbp_channel_resets_total` | counter | `venue` | One publisher-and-channel's books discarded on a frame-header `Reset Count` change — any change, including the `255 -> 0` wrap. Read from the **market-data port only**: the three ports carry the same epoch on separate sockets, so a shared memo would count one publisher restart once per interleaved frame of its backlog. |
| `dz_mbp_buffer_overflows_total` | counter | `venue` | Cross-instrument delta-buffer budget overflows; each dropped the largest instrument's buffer and marked it `Gap`. Sustained means the publisher's snapshot period is too long for this host's memory budget — a tuning signal, not a fault. |
| `dz_mbp_level_overflows_total` | counter | `venue` | A book discarded on hitting its per-book price-level cap. Deliberately **not** the same series as a sequence gap: the cause is a malformed or forged stream, never packet loss, and the resulting book status differs. |
| `dz_mbp_orphan_snapshot_levels_total` | counter | `venue` | `SnapshotLevel` with no open group to route it to — a publisher interleaving snapshot groups, a lost `SnapshotBegin`, or a rotation from the publisher's previous epoch still draining after a restart (its group is refused rather than installed, so the dead session's book is never republished). An **anomaly**: a level that should have been attributable was not. A rotation the book declined is deliberately *not* counted here — see the next row. Measured at ~2.6% of snapshot levels on the live Lashay perps groups (2026-08-08), reproduced independently by `marketbyprice-parser` with zero host-side UDP errors, so upstream loss or reordering rather than a receive-side defect. |
| `dz_mbp_declined_rotation_levels_total` | counter | `venue` | Levels of a rotation the book **declined** because it is already synced past it (`Ready` and at or beyond the rotation's `Last Instrument Seq`). Expected and benign: publishers rotate snapshots continuously, so in steady state this tracks essentially the whole snapshot-level rate. It exists so that rate stays out of the orphan counter, which otherwise reads ~100% and hides the real anomaly. Alert on the *orphan* series, not this one. |
| `dz_mbp_duplicate_deltas_total` | counter | `venue` | Deltas discarded as duplicates. **Worth an alert:** a `Ready` book emitting nothing but duplicates is the signature of a baseline installed above the publisher's real counter, which is deliberately not self-healed (only a routed `Reset Count` clears it), so this counter is the only thing that surfaces that wedge. |
| `dz_mbp_crossed_total` | counter | `venue` | Crossed inside markets observed at a `BatchBoundary`. Observability only, never acted on. |
| `dz_mbp_divergence_total` | counter | `venue`, `kind` | Publisher `Action`-byte-vs-quantity disagreements, by `kind` (`new_on_present_level`/`change_on_absent_level`/`delete_with_quantity`/`zero_quantity_without_delete`). Never changes the applied result. |

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
| `dz_trades_no_id_conflict_total` | counter | `venue` | Zero-id trades forwarded from a **second, concurrent** publisher for a `(venue, symbol)` another already owns. A bypassed sentinel has nothing to collapse against, so every print on this counter is a duplicate already on the wire: a consumer summing `size` overstates volume by the number of emitting publishers. Should be flat at zero — a non-zero rate means two publishers are emitting one venue's tape and one of them has to stop. A *failover* is not counted: a publisher inheriting a tape that has been quiet for 5s takes ownership silently, so this stays a signal about concurrency rather than about which arm is live. `Coordinated` venues only: a `Sticky` venue's single-emitter guarantee comes from the tape gate upstream, so this latch is skipped there — it cannot see a gate-approved handover and would report one as a double-print. |
| `dz_instruments_dropped_total` | counter | `venue` | Instrument definitions dropped as an unchanged repeat of the last content broadcast for that `(venue, symbol)`, within the re-announce interval (`INSTRUMENT_REANNOUNCE_NS`, 15s) — the mirrored publishers' identical reference-data bursts collapsing. Expect nearly all definition frames in steady state: the dedup is a rate limit, not a latch, so unchanged content is still re-broadcast on the first burst past the interval (that periodic re-announce is what heals a client which lost an `instrument` to drop-oldest backpressure). A precision *change* is never suppressed. |
| `dz_quotes_future_rejected_total` | counter | `venue` | Quotes rejected for an implausibly-far-future `source_ts`. |
| `dz_quotes_no_source_ts_total` | counter | `venue` | Quotes forwarded with the `source_ts == 0` sentinel (floor bypassed). |
| `dz_quote_lead_ns` | histogram | `venue`, `winner`, `loser` | Nanoseconds the winning publisher led the losing duplicate by, per quote-tick cross-source contest (`winner`/`loser` each `edge`/`public`). `{winner="edge",loser="public"}` is "DZ beat the public feed"; `_count` is the head-to-head win count, the buckets the lead margin. |
| `dz_trade_lead_ns` | histogram | `venue`, `winner`, `loser` | The trade-side counterpart of `dz_quote_lead_ns`, per `trade_id` cross-source contest. |
| `dz_arm_lead_ns` | histogram | `venue`, `winner` | Nanoseconds between the two arms' copies of the **same matched trade**, on our own receive clock. `winner` is `leader`/`challenger` — relative, so the label set stays two-valued whatever the arm count. This is what the transfer thresholds are read off: `{winner="challenger"}` sitting persistently past `--arb-transfer-margin-us` with no transfer means the conditions are too tight. Fed only by matched pairs, never by a dropped non-authoritative copy — the interval to the leader's *previous, unrelated* message is inter-arm phase, not a lead, and is structurally non-negative. |
| `dz_arm_authority_transfers_total` | counter | `venue`, `reason` | Authority handovers, by `reason` (`initial`/`health`/`silence`/`margin`). Every transfer re-baselines each consumer's book, so a sustained rate means the thresholds are too loose; a `health`/`silence` rate means an arm is actually broken. `health` is a single market changing hands; the other three are venue-wide. One re-baseline is deliberately **not** counted: the elected arm taking a market back once its own book recovers, which is indistinguishable at the gate from the first market to speak after a `margin` transfer (already counted) — so `dz_emit_total{kind="book"}` is the place to see the total re-baseline volume, not this counter. |
| `dz_tape_owner_changes_total` | counter | `venue` | Trade-tape ownership moving between a venue's **feed rows**, on a subscription change. A venue's groups are gated independently, so both its rows claim the tape and the reconciler picks one (top of book over market-by-price); this counts each pick changing. One increment per subscription flip is expected; a sustained rate means subscriptions are flapping, and each move is a window in which a print can double or drop. |
| `dz_tape_arm_transfers_total` | counter | `venue` | The same move one level down: which **arm** of the owning row serves the tape, for `Sticky` venues. Transfers when the book election moves to a different arm, when a tracked arm displaces an untracked incumbent, or when the incumbent goes silent for 5s. Its steady state is zero; a sustained rate means the two arms are trading the tape, and the same double-or-drop window applies. |
| `dz_tape_arm_dropped_total` | counter | `venue` | Prints the arm gate dropped as a non-serving arm's copy. Deliberately its own counter rather than folded into `dz_trades_dropped_total`, whose steady state on a `Sticky` venue *is* the challenger arm's whole stream — mixing them would hide a gate stuck on the wrong arm inside expected noise. Read it against `dz_emit_total{kind="trade"}`: the gate holding roughly the whole venue's print rate, with `dz_tape_arm_transfers_total` flat, is the shape to alert on. |
| `dz_arm_markets_held` | gauge | `venue`, `arm` | Markets each arm is currently **serving** — the venue's elected arm, plus any market a health override moved to a peer. `arm` is a stable per-venue ordinal (`arm0`…`arm7`, then `other`), never the spoofable source IP. All markets on one arm is the steady state; a persistent split means health overrides are fragmenting the venue, i.e. the elected arm's books keep gapping. |
| `dz_arm_unmatched_trades_total` | counter | `venue`, `arm` | Trades an arm delivered that its peer never delivered inside `--arb-match-window-secs` — a drop on one arm, or a genuine one-sided print. This is election evidence lost: a rate approaching the venue's own trade rate means the arms are barely pairing, so `dz_arm_lead_ns` samples a thin slice and `--arb-min-window-samples` leaves most windows unjudged. Counts window expiries only; the matcher's overload cap is a different condition and is deliberately not merged in. |
| `dz_book_dropped_total` | counter | `venue`, `publisher` | Incremental `book` batches the authority gate did not publish: a non-authoritative arm's copy, or a batch withheld while a market waits for its new arm to close a logical event (a re-baseline cannot be built from half an event). In steady state this is the challenger arm's entire stream, so it tracks its message rate rather than any fault — read it against `dz_emit_total{kind="book"}`. `publisher` is the dropped copy's **source class** (`edge`/`public`), not its arm ordinal, so two multicast arms both count as `edge`. |
| `dz_book_markets_evicted_total` | counter | `venue` | Markets evicted from the gate's per-market `book` state on reaching its cap (16,384, an order of magnitude above the largest real venue). The key is wire-supplied, so this is the forged-market backstop and should stay flat at zero. An evicted market loses its replay bootstrap and its record of which arm the consumer's state came from, so its next batch re-baselines that consumer — which is what keeps eviction safe rather than silently resuming an unrelated delta series. |
| `dz_book_events_deduped_total` | counter | `venue` | Order-level `book` events collapsed because another publisher delivered the same venue event first. Its steady state is the whole stream of every publisher but the fastest for each event, so like `dz_book_dropped_total` it is a throughput figure, not a fault — read it against `dz_emit_total{kind="book"}`. Falling to zero on a multi-publisher venue means only one publisher is reaching us. |
| `dz_mbo_arm_disagreement_total` | counter | `venue` | Two publishers reported **more** resting quantity for one order than a peer had already reported for it. A resting order only ever shrinks, so this is a publisher that missed a fill and believes its book is still contiguous — silent drift, made observable. Neither copy is published; the market re-baselines instead (`dz_mbo_forced_rebaselines_total{reason="disagreement"}`). **Any sustained rate is a correctness alarm**, and the trigger to reconsider deriving output from per-publisher books at all (dedup on input, into one shared book, makes the divergence structurally impossible). An ordinary interleaved race, where an arm delivers a smaller remainder first, is not counted. |
| `dz_book_resurrections_dropped_total` | counter | `venue` | Order-level changes dropped because the market had already published that order as gone. A venue never reuses an order id, so each one is a lagging publisher's stale copy refused before it could resurrect a dead order in every consumer's book. **This is the guard order-level racing rests on, and a non-zero rate is it working, not a fault** — it rises with how far one publisher lags its peers. The guard's reach is the arms themselves rather than a clock — a tombstone is held until every publisher `book_sync` knows for that market has either reported the removal or been seen past it — which is what keeps `--arb-book-dedup-window-ms` a cost knob. Read the population it costs on `dz_mbo_guarded_tombstones`; what happens when that budget binds is `dz_mbo_market_invalidations_total`. |
| `dz_mbo_forced_rebaselines_total` | counter | `venue`, `reason` | Markets withheld and re-baselined because two publishers claimed different resting state for one order (`reason="disagreement"`), so neither is known to be right — publishing the larger rewinds a consumer past a fill the venue already applied, the smaller lets a forged size mute a real order. Each one costs a full re-publish of the market's book — serialized to every client, and O(book) inside the shared arbiter mutex, so it stalls every ingest receiver on every feed rather than merely spending bandwidth. Rate-limited to one per market per `--arb-book-dedup-window-ms` for that reason, since a single datagram can raise the flag. ⚠️ The market keeps withholding meanwhile and those batches are **lost, not delayed** — they never reach the map the re-publish is built from, so a consumer's book skips them until some publisher sends a re-baseline of its own. A sustained rate is the signal to reconsider the per-publisher book model. The re-publish is the book **the wire agreed on**, never one publisher's own — an arm's private accumulator is whatever that source sent, and republishing it as full state on an unauthenticated wire would make one forged datagram enough to replace a market's book. |
| `dz_mbo_market_invalidations_total` | counter | `venue` | Order-level markets **disowned**: the resurrection guard lost a tombstone no serving arm had passed, so a lagging publisher's stale `Add` for that order could no longer be refused. The market's state and its replay entry are dropped and **nothing is published for it until a publisher re-baselines it of its own accord**, which for a healthy publisher is its next recovery rather than its next snapshot rotation — tens of seconds observed, unbounded in principle. So this is an availability alarm, not a throughput figure: any non-zero value is a market that went dark. Republishing our own accumulated view instead is what this replaced, and that was worse — it is the view the guard failed to protect, so it hands consumers the resurrections stamped as a complete book and re-seeds them as live orders nothing removes again. |
| `dz_mbo_guarded_tombstones` | gauge | — | Removed orders the resurrection guard holds across every tracked market, against a process-wide ceiling of 1,048,576. Sized by how far the publishers lag each other (removal rate × lag), so it is flat and small when they are in step and rises with separation — **this is the headroom to watch, and it is readable long before the guard runs out of it**. Reaching the ceiling, or one market reaching 65,536, is what invalidates markets. |
| `dz_mbo_removed_evicted_total` | counter | — | Removed order ids one *book* forgot on reaching its own cap (2^16 per book). Defence in depth rather than the cross-publisher guard above: a book sees one publisher's stream, where the sequence check already rejects a repeat, so what this catches is a forged `Add` re-using a dead id at a contiguous sequence. Flat at zero in normal operation. |

**The `dz_arm_*` and `dz_book_*` series emit only once a venue actually serves `book`.** Two feeds do: the Market-by-Price row (`lashay-2`) drives the single-arm gate and its `dz_arm_*` election series, and Market-by-Order drives the order-level racing series (`dz_book_events_deduped_total`, `dz_mbo_*`) — the gate's own counters stay flat for it, since a raced market never reaches the election. Both groups are live, so these populate on a subscribed host and stay empty on one that is not. Do not read an empty result as a healthy venue; check the subscription first.

### Tuning arm re-election

Speed and silence are judged **per arm, venue-wide** — latency is a property of an arm, so every matched sample from a source IP counts toward it whatever market carried it. Health is the one per-market rule, and it overrides the elected arm for that market alone. Six flags govern it (all also env vars, `DZ_ARB_*`):
`--arb-sample-interval-secs` (300) is how long a window pools matched samples before it can transfer, and so the ceiling on how long a persistently slower arm keeps authority;
`--arb-transfer-margin-us` (1000) is the median lead a challenger must show;
`--arb-transfer-win-rate` (0.8) is the fraction of its own samples it must also lead;
`--arb-min-window-samples` (32) is how many matched samples it needs before the window is judged at all;
`--arb-match-window-secs` (5) is how long one arm's trade waits for the peer's copy of the same print before it is written off as unmatched, and so bounds which pairs become samples at all;
`--arb-leader-timeout-secs` (2) is the venue-wide silence after which a live arm takes over.

`--arb-book-dedup-window-ms` (250) governs the *other* gate — how long a delivered order-level book event is remembered so a slower publisher's copy is recognized as a duplicate. It is not a re-election tunable and it is not the correctness parameter: an order the market has published as gone is refused however late the copy arrives (`dz_book_resurrections_dropped_total`), and that guard's reach is an order count rather than the clock, so an undersized *window* costs a redundant emission and a wasted apply. The budget behind that guard is a different matter — when it binds the guard stops being able to answer, and the market is disowned rather than served from a book nothing vouches for (`dz_mbo_market_invalidations_total`). Read it against `dz_book_events_deduped_total`. The per-market **count** cap (1024 events) binds before 250 ms on a market fast enough to fill it, so on a busy venue the effective window is shorter than the flag says.

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

## Query API history feeder

Recorded by the feeder that keeps the query API's rolling history store fed (`ingest::reconcile::feed_history`), active only while the query API sink is.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_history_unattributable_trades_total` | counter | `venue` | A trade dropped rather than stored because the instrument catalog carries no definition for the exact `(venue, channel, instrument_id)` the message names — belt-and-braces for a definition race (or, on the unauthenticated wire, a forged identity). Should stay flat at zero. |
| `dz_history_feed_lagged_total` | counter | — | Times the feeder fell behind the post-arbiter broadcast and dropped messages (`Lagged`) — a hole in the rolling window, not a crash. |

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
