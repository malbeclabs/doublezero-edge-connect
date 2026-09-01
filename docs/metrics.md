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
| `dz_receiver_up` | gauge | `venue`, `kind`, `publisher` | `1` while this publisher's market-data feed is up, `0` while down. The per-publisher counterpart of `dz_feed_up`. |
| `dz_feed_up` | gauge | `venue` | `1` while *any* publisher of the venue is up, `0` once every one has gone silent. |
| `dz_feed_stale_ms` | gauge | `venue` | Staleness in milliseconds: `0` while up; the staleness at the last venue-level `down` transition (reset to `0` on recovery). |
| `dz_seq_events_total` | counter | `venue`, `kind` | Datagram-sequence classifications (`first`/`ok`/`reset`/`stale`). Incremented in the processor, which demultiplexes publishers by source IP address and so has no configured base port — hence no `publisher` label. |

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
| `dz_source_id_changed_total` | counter | `venue` | A `(publisher, instrument)` already revealed under one wire Source ID named a different one on a later message — a publisher defect, not a decode issue. Labelled by the **new** (post-change) venue, which is also re-announced a fresh `instrument` under so the new venue's precision-before-price guarantee still holds. Should stay flat at zero; a nonzero rate means a publisher is relabeling an instrument's venue partway through. |
| `dz_unregistered_source_ids_total` | counter | — | Distinct Source IDs seen with no registry row. |
| `dz_unregistered_source_id_labels_capped_total` | counter | — | Messages labelled `UNREGISTERED` because the distinct-unregistered-ID cap was reached. |

## Market-by-Price processor (per venue)

Recorded by the Market-by-Price processor (`src/ingest/processor.rs`), which demultiplexes publishers by source IP address and so carries no `publisher` label. Labelled by `venue`. **No `FEEDS` row selects `MarketByPrice` yet**, so a scrape today reports nothing here; do not read an empty result as a healthy book.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_mbp_channel_resets_total` | counter | `venue` | One publisher-and-channel's books discarded on a datagram-header `Reset Count` change — any change, including the `255 -> 0` wrap. Read from the **market-data port only**: the three ports carry the same era on separate sockets, so a shared memo would count one publisher restart once per interleaved datagram of its backlog. |
| `dz_mbp_buffer_overflows_total` | counter | `venue` | Cross-instrument delta-buffer budget overflows; each dropped the largest instrument's buffer and marked it `Gap`. Sustained means the publisher's snapshot period is too long for this host's memory budget — a tuning signal, not a fault. |
| `dz_mbp_level_overflows_total` | counter | `venue` | A book discarded on hitting its per-book price-level cap. Deliberately **not** the same series as a sequence gap: the cause is a malformed or forged feed, never packet loss, and the resulting book status differs. |
| `dz_mbp_orphan_snapshot_levels_total` | counter | `venue` | `SnapshotLevel` with no open group to route it to — a publisher interleaving snapshot groups, or a lost `SnapshotBegin`. An **anomaly**: a level that should have been attributable was not, which makes this the series to alert on. It counts only that: a group the processor itself declined (next row) or dropped (the row after) is attributed to why instead, so in steady state this stays at zero. Measured at ~2.6% of snapshot levels on the live Kalshi perps groups (2026-08-08) — before those exclusions — reproduced independently by `marketbyprice-parser` with zero host-side UDP errors, so upstream loss or reordering rather than a receive-side defect. |
| `dz_mbp_declined_rotation_levels_total` | counter | `venue` | Levels of a rotation the book **declined** because it is already synced past it (`Ready` and at or beyond the rotation's `Last Instrument Seq`). Expected and benign: publishers rotate snapshots continuously, so in steady state this tracks essentially the whole snapshot-level rate. It exists so that rate stays out of the orphan counter, which otherwise reads ~100% and hides the real anomaly. Alert on the *orphan* series, not this one. |
| `dz_mbp_snapshot_levels_dropped_total` | counter | `venue`, `reason` | Levels of a snapshot group the processor deliberately did not route at all, by why: `reset` (an `InstrumentReset` killed the anchor it was assembling against, so the route went with the group), `stale_era` (its `Reset Count` is the publisher's previous run, still draining off the snapshot socket after a restart — refused rather than installed, so the dead session's book is never republished), `no_definition` (its instrument's definition had not resolved yet — a cold-start transient of tens of thousands of levels that clears as reference data fills the catalog). All three are correct behaviour with a permanent floor on a live feed; they are counted here so the orphan series above stays alertable. |
| `dz_mbp_duplicate_deltas_total` | counter | `venue` | Deltas discarded as duplicates. **Worth an alert:** a `Ready` book emitting nothing but duplicates is the signature of a baseline installed above the publisher's real counter, which is deliberately not self-healed (only a routed `Reset Count` clears it), so this counter is the only thing that surfaces that wedge. |
| `dz_mbp_crossed_total` | counter | `venue` | Crossed inside markets observed at a `BatchBoundary`. Observability only, never acted on. |
| `dz_mbp_divergence_total` | counter | `venue`, `kind` | Publisher `Action`-byte-vs-quantity disagreements, by `kind` (`new_on_present_level`/`change_on_absent_level`/`delete_with_quantity`/`zero_quantity_without_delete`). Never changes the applied result. |

## Arbiter emit stage (per feed)

Recorded by the shared pre-broadcast emit stage (`src/ingest/arbiter.rs`). Labelled by `venue`.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_emit_total` | counter | `venue`, `kind` | Messages broadcast after dedup, by `kind` (quote/trade/instrument/midpoint/depth/book). `status` is structurally possible but never routed through the arbiter today, so it is not recorded in practice. |
| `dz_quotes_admitted_total` | counter | `venue`, `transport` | Quotes admitted by the staleness floor, attributed to the winning `transport` (`edge`/`public`). A rise in `transport="public"` is the direct signal of the public backstop filling an edge gap. |
| `dz_trades_admitted_total` | counter | `venue`, `transport` | Trades admitted by the windowed dedup, attributed to the winning `transport` (`edge`/`public`). A rise in `transport="public"` is the trade-side signal of the public backstop filling an edge gap — the counterpart to `dz_quotes_admitted_total` for a trades-only backstop like Phoenix. |
| `dz_quote_ticks_won_total` | counter | `venue`, `transport` | Quote `source_ts` ticks **won** — the once-per-tick first delivery, attributed to the winning class. Every tick counts exactly once: a mirror's copy or the leader's later in-tick contents never re-count it, and a tick the public feed never delivers still counts for the edge (the walkover). `edge / sum` is the published DZ win rate (see below). `source_ts == 0` sentinel quotes bypass the floor and are not counted. |
| `dz_depth_ticks_won_total` | counter | `venue`, `transport` | The depth mirror of `dz_quote_ticks_won_total` (for depth the `source_ts == 0` empty-anchor tick is real and counts). |
| `dz_quotes_dropped_total` | counter | `venue` | Quotes dropped by the staleness floor (stale tick, non-leader, or exact repeat). |
| `dz_trades_dropped_total` | counter | `venue` | Trades dropped by the windowed dedup (duplicate `trade_id` still inside the window). |
| `dz_trades_no_id_total` | counter | `venue` | Trades forwarded with the `trade_id == 0` sentinel, bypassing the dedup window. A FIX-sourced publisher carries no venue trade id; keying the window on `0` would collapse the tape to one print per `(venue, symbol)` forever. A non-zero rate here means the venue's tape is un-deduped by construction — correct, but it relies on exactly one publisher owning the venue's tape (watch `dz_trades_no_id_conflict_total`). Not counted as admitted: nothing was. |
| `dz_trades_no_id_conflict_total` | counter | `venue` | Zero-id trades forwarded from a **second, concurrent** publisher for a `(venue, symbol)` another already owns. A bypassed sentinel has nothing to collapse against, so every print on this counter is a duplicate already on the wire: a consumer summing `size` overstates volume by the number of emitting publishers. Should be flat at zero — a non-zero rate means two publishers are emitting one venue's tape and one of them has to stop. A *failover* is not counted: a publisher inheriting a tape that has been quiet for 5s takes ownership silently, so this stays a signal about concurrency rather than about which path is live. `Coordinated` venues only: a `Sticky` venue's single-emitter guarantee comes from the tape gate upstream, so this latch is skipped there — it cannot see a gate-approved handover and would report one as a double-print. |
| `dz_instruments_dropped_total` | counter | `venue` | Instrument definitions dropped as an unchanged repeat of the last content broadcast for that `(venue, symbol)`, within the re-announce interval (`INSTRUMENT_REANNOUNCE_NS`, 15s) — the mirrored publishers' identical reference-data bursts collapsing. Expect nearly all definition datagrams in steady state: the dedup is a rate limit, not a latch, so unchanged content is still re-broadcast on the first burst past the interval (that periodic re-announce is what heals a client which lost an `instrument` to drop-oldest backpressure). A precision *change* is never suppressed. |
| `dz_quotes_future_rejected_total` | counter | `venue` | Quotes rejected for an implausibly-far-future `source_ts`. |
| `dz_quotes_no_source_ts_total` | counter | `venue` | Quotes forwarded with the `source_ts == 0` sentinel (floor bypassed). |
| `dz_quote_lead_ns` | histogram | `venue`, `winner`, `loser` | Nanoseconds the winning publisher led the losing duplicate by, per quote-tick cross-publisher contest (`winner`/`loser` each `edge`/`public`). `{winner="edge",loser="public"}` is "DZ beat the public feed"; `_count` is the head-to-head win count, the buckets the lead margin. |
| `dz_trade_lead_ns` | histogram | `venue`, `winner`, `loser` | The trade-side counterpart of `dz_quote_lead_ns`, per `trade_id` cross-publisher contest. |
| `dz_path_lead_ns` | histogram | `venue`, `winner` | Nanoseconds between the two paths' copies of the **same matched trade**, on our own receive clock. `winner` is `leader`/`challenger` — relative, so the label set stays two-valued whatever the path count. This is what the transfer thresholds are read off: `{winner="challenger"}` sitting persistently past `--arb-transfer-margin-us` with no transfer means the conditions are too tight. Fed only by matched pairs, never by a dropped non-authoritative copy — the interval to the leader's *previous, unrelated* message is inter-path phase, not a lead, and is structurally non-negative. |
| `dz_path_authority_transfers_total` | counter | `venue`, `reason` | Authority handovers, by `reason` (`initial`/`health`/`silence`/`margin`). Every transfer re-baselines each consumer's book, so a sustained rate means the thresholds are too loose; a `health`/`silence` rate means a path is actually broken. `health` is a single market changing hands; the other three are venue-wide. One re-baseline is deliberately **not** counted: the elected path taking a market back once its own book recovers, which is indistinguishable at the gate from the first market to speak after a `margin` transfer (already counted) — so `dz_emit_total{kind="book"}` is the place to see the total re-baseline volume, not this counter. |
| `dz_tape_owner_changes_total` | counter | `venue` | Trade-tape ownership moving between a venue's **feed rows**, on a subscription change. A venue's groups are gated independently, so both its rows claim the tape and the reconciler picks one (top of book over market-by-price); this counts each pick changing. One increment per subscription flip is expected; a sustained rate means subscriptions are flapping, and each move is a window in which a print can double or drop. |
| `dz_tape_path_transfers_total` | counter | `venue` | The same move one level down: which **path** of the owning row serves the tape, for `Sticky` venues. Transfers when the book election moves to a different path, when a tracked path displaces an untracked incumbent, or when the incumbent goes silent for 5s. Its steady state is zero; a sustained rate means the two paths are trading the tape, and the same double-or-drop window applies. |
| `dz_tape_path_dropped_total` | counter | `venue` | Prints the path gate dropped as a non-serving path's copy. Deliberately its own counter rather than folded into `dz_trades_dropped_total`, whose steady state on a `Sticky` venue *is* the challenger path's whole feed — mixing them would hide a gate stuck on the wrong path inside expected noise. Read it against `dz_emit_total{kind="trade"}`: the gate holding roughly the whole venue's print rate, with `dz_tape_path_transfers_total` flat, is the shape to alert on. |
| `dz_path_markets_held` | gauge | `venue`, `path` | Markets each path is currently **serving** — the venue's elected path, plus any market a health override moved to a peer. `path` is a stable per-venue ordinal (`path0`…`path7`, then `other`), never the spoofable source IP address. All markets on one path is the steady state; a persistent split means health overrides are fragmenting the venue, i.e. the elected path's books keep gapping. |
| `dz_path_unmatched_trades_total` | counter | `venue`, `path` | Trades a path delivered that its peer never delivered inside `--arb-match-window-secs` — a drop on one path, or a genuine one-sided print. This is election evidence lost: a rate approaching the venue's own trade rate means the paths are barely pairing, so `dz_path_lead_ns` samples a thin slice and `--arb-min-window-samples` leaves most windows unjudged. ⚠️ **That reading assumes the venue has two paths.** A row whose group carries a *single* publisher (`edge-phoenix-mbp` today) sits at exactly 100% by construction — the authority tracks its one path, so every print enters the matcher and every one expires unpaired — and there is no election to lose, since a lone path is never displaced. Alert on the ratio only for a venue that is actually mirrored. Counts window expiries only; the matcher's overload cap is a different condition and is deliberately not merged in. |
| `dz_book_dropped_total` | counter | `venue`, `transport` | Incremental `book` batches the authority gate did not publish: a non-authoritative path's copy, or a batch withheld while a market waits for its new path to close a logical event (a re-baseline cannot be built from half an event). In steady state this is the challenger path's entire feed, so it tracks its message rate rather than any fault — read it against `dz_emit_total{kind="book"}`. `transport` is the dropped copy's **transport class** (`edge`/`public`), not its path ordinal, so two multicast paths both count as `edge`. |
| `dz_book_markets_evicted_total` | counter | `venue` | Markets evicted from the gate's per-market `book` state on reaching its cap (16,384, an order of magnitude above the largest real venue). The key is wire-supplied, so this is the forged-market backstop and should stay flat at zero. An evicted market loses its replay bootstrap and its record of which path the consumer's state came from, so its next batch re-baselines that consumer — which is what keeps eviction safe rather than silently resuming an unrelated delta series. |
| `dz_book_events_deduped_total` | counter | `venue` | Order-level `book` events collapsed because another publisher delivered the same venue event first. Its steady state is the whole feed of every publisher but the fastest for each event, so like `dz_book_dropped_total` it is a throughput figure, not a fault — read it against `dz_emit_total{kind="book"}`. Falling to zero on a multi-publisher venue means only one publisher is reaching us. |
| `dz_mbo_path_disagreement_total` | counter | `venue` | Two publishers reported **more** resting quantity for one order than a peer had already reported for it. A resting order only ever shrinks, so this is a publisher that missed a fill and believes its book is still contiguous — silent drift, made observable. Neither copy is published; the market re-baselines instead (`dz_mbo_forced_rebaselines_total{reason="disagreement"}`). **Any sustained rate is a correctness alarm**, and the trigger to reconsider deriving output from per-publisher books at all (dedup on input, into one shared book, makes the divergence structurally impossible). An ordinary interleaved race, where a path delivers a smaller remainder first, is not counted. |
| `dz_book_resurrections_dropped_total` | counter | `venue` | Order-level changes dropped because the market had already published that order as gone. A venue never reuses an order id, so each one is a lagging publisher's stale copy refused before it could resurrect a dead order in every consumer's book. **This is the guard order-level racing rests on, and a non-zero rate is it working, not a fault** — it rises with how far one publisher lags its peers. The guard's reach is a clock, and it is the venue's rather than ours: per channel, the newest `source_ts` accepted on it less `--arb-book-retention-secs`, so an order is remembered for that much *venue* time and forgotten after it. That is what keeps `--arb-book-dedup-window-ms` a cost knob. Read the population it costs on `dz_mbo_guarded_tombstones_max`, whether the frontier is still tracking the venue on `dz_mbo_frontier_bounded_total` / `dz_mbo_frontier_reseats_total`, and whether the window is holding more than the host was sized for on `dz_mbo_guard_ceiling_evictions_total`. |
| `dz_mbo_forced_rebaselines_total` | counter | `venue`, `reason` | Markets withheld and re-baselined because two publishers claimed different resting state for one order (`reason="disagreement"`), so neither is known to be right — publishing the larger rewinds a consumer past a fill the venue already applied, the smaller lets a forged size mute a real order. Each one costs a full re-publish of the market's book — serialized to every client, and O(book) inside the shared arbiter mutex, so it stalls every ingest receiver on every feed rather than merely spending bandwidth. Rate-limited to one per market per 250 ms for that reason, since a single datagram can raise the flag. That interval is a fixed constant and deliberately **not** `--arb-book-dedup-window-ms`, which it once shared: widening the window for its dedup reach would otherwise lengthen how much of a real disagreement's feed is skipped. ⚠️ The market keeps withholding meanwhile and those batches are **lost, not delayed** — they never reach the map the re-publish is built from, so a consumer's book skips them until some publisher sends a re-baseline of its own. A sustained rate is the signal to reconsider the per-publisher book model. The re-publish is the book **the wire agreed on**, never one publisher's own — a path's private accumulator is whatever that publisher sent, and republishing it as full state on an unauthenticated wire would make one forged datagram enough to replace a market's book. |
| `dz_mbo_events_past_frontier_total` | counter | `venue` | Order-level changes refused because the batch carrying them is older than its channel's retention window (`--arb-book-retention-secs` behind the newest venue stamp accepted on that channel). The refusal is wholesale — a link returning with a backlog, or a replay of one, describes a book the venue left seconds to minutes ago, and admitting any of it walks every consumer backwards. Expect a spike once per recovery and zero in between. A sustained rate is one publisher running further behind the venue than the window allows for; decide which of the two is wrong before widening the flag. |
| `dz_mbo_events_stale_total` | counter | `venue` | Order-level changes refused for being older than the last change this market published for that same order — the lagging copy the batch-level window above is too coarse to catch. Its steady state is the trailing publisher's copies of orders the leader has since filled, so it tracks inter-path separation rather than a fault. It is evaluated **before** the resting-size comparison, which is the point: a stale copy of the add for a partially-filled order claims more than the peer reported, and comparing sizes first reads that as drift and forces a re-baseline. A rate here with `dz_mbo_path_disagreement_total` flat is that false alarm being subsumed, not suppressed. |
| `dz_mbo_frontier_bounded_total` | counter | `venue`, `reason` | Batches whose venue stamp was refused as an advance. **The batch is still served**, judged and recorded at the channel's newest stamp rather than its own, so one garbage stamp neither carries the frontier forward nor pins the forgetting queue behind it. `reason="jump"` is more than `--arb-book-ts-jump-secs` ahead of the channel's own newest: occasional increments are a publisher stamping the odd bad datagram, a sustained rate means the bound is catching ordinary jitter and the flag is too tight. `reason="anchor"` is implausibly ahead of the **host** clock — the absolute bound that stops a stream of in-bound stamps ratcheting the frontier past real venue time and refusing every honest publisher on the channel. ⚠️ **No flag widens `anchor`**; its bound is a constant this guard shares with the quote and depth floors. A sustained `anchor` rate is either a publisher stamping the wrong units, or *this host's* clock running behind the venue's — and the second also drives `dz_mbo_path_disagreement_total` up for a reason that has nothing to do with the publishers disagreeing, because every order on the channel ends up recorded at the same stamp and the stale-copy rule can no longer fire. **Check this series before believing that alarm.** |
| `dz_mbo_frontier_reseats_total` | counter | `venue` | Channel frontiers re-seated from the batches actually arriving, after the newest stamp failed to **move** for `--arb-book-reseat-secs`, or after the forward bound refused advances continuously for that long (the second trigger catches a publisher whose clock crawls: its tiny advances are all accepted, so the movement timer never elapses while every honest path is refused). It moves the frontier in **either** direction, and only one of those is an outage: stranded *ahead* of its whole channel — a bad stamp inside the forward bound, a publisher clock jump, or the only surviving path sitting behind a dead leader's stamp — every arriving batch is past the frontier and the market is dark until this fires, so each such increment is the end of an outage. Stranded *behind*, after the forward bound refused a legitimate jump, nothing is dark and the cost is only that the removed population stops ageing out. A session end needs neither: it unsets the frontier outright. One per genuine gap; a repeating rate on a healthy channel means the interval is set below the paths' own separation. |
| `dz_mbo_guard_ceiling_evictions_total` | counter | — | Removed-order entries forgotten by the process-wide ceiling (1,048,576) rather than by age. Zero is the design point — the retention window sizes the population at ~119k on the flagship channel — so non-zero means the window is holding more than the host was sized for, and until it clears the guard's reach is **shorter than the window claims**: exactly the interval in which a lagging publisher's stale add can resurrect a dead order. Unlabelled, because the ceiling is process-wide and charging it to a venue would be a guess. |
| `dz_mbo_guarded_tombstones` | gauge | — | Removed orders the resurrection guard holds across every tracked market, against a process-wide ceiling of 1,048,576. Sized by the retention window and the venue's own removal rate (~119k at 30 s and 3,958 removals/s per publisher per channel), so it is flat while each channel's frontier tracks the venue and rises when one stops moving and nothing ages out. There is no per-market cap any more, so this is the budget — but it is still a sum, and one market walking away from the rest reads as flat headroom here, which is what `dz_mbo_guarded_tombstones_max` is for. Crossing the ceiling forgets the oldest entries **of the market being admitted**, which may hold none, so the figure can sit briefly over the ceiling rather than being clamped to it (`dz_mbo_guard_ceiling_evictions_total`). |
| `dz_mbo_guarded_tombstones_max` | gauge | — | The largest removed population any **single** market holds, against that same process-wide ceiling of 1,048,576 — it is the aggregate's distribution, not a second and tighter budget. **Still the one to alert on**, since the guard's reach is per channel and one channel stalling is invisible in the sum. Unlabelled by market on purpose: the key is wire-supplied and there are up to 16,384 of them. A lagging high-water rather than a true maximum: it reads stale-**high** after the market holding it goes quiet, since only a market that publishes is observed, and by up to **twice** the true maximum while the holder is live and shrinking — the figure is re-seated only on a material fall, because under the frontier the largest holder forgets on most of its batches and re-seating on every dip would put a pass over every tracked market on the ingest hot path. It never reads stale-**low**, which is the direction that would mislead — the figure is re-seated on the largest surviving market whenever the one holding it stops describing it, whether it was dropped (session reset, evicted) or its own entries aged past the frontier. Rising steadily on one market now means that market's frontier is stuck — its channel's newest venue stamp has stopped moving, so nothing is being forgotten — and `dz_mbo_frontier_reseats_total` is the event that ends it. |
| `dz_mbo_removed_evicted_total` | counter | — | Removed order ids one *book* forgot on reaching its own cap (2^16 per book). Defence in depth rather than the cross-publisher guard above: a book sees one publisher's feed, where the sequence check already rejects a repeat, so what this catches is a forged `Add` re-using a dead id at a contiguous sequence. Flat at zero in normal operation. |

**The `dz_path_*` and `dz_book_*` series emit only once a venue actually serves `book`.** Two feeds do: the Market-by-Price row (`edge-kalshi-perps-mbp`) drives the single-path gate and its `dz_path_*` election series, and Market-by-Order drives the order-level racing series (`dz_book_events_deduped_total`, `dz_mbo_*`) — the gate's own counters stay flat for it, since a raced market never reaches the election. Both groups are live, so these populate on a subscribed host and stay empty on one that is not. Do not read an empty result as a healthy venue; check the subscription first.

### Tuning path re-election

Speed and silence are judged **per path, venue-wide** — latency is a property of a path, so every matched sample from a source IP address counts toward it whatever market carried it. Health is the one per-market rule, and it overrides the elected path for that market alone. Six flags govern it (all also env vars, `DZ_ARB_*`):
`--arb-sample-interval-secs` (300) is how long a window pools matched samples before it can transfer, and so the ceiling on how long a persistently slower path keeps authority;
`--arb-transfer-margin-us` (1000) is the median lead a challenger must show;
`--arb-transfer-win-rate` (0.8) is the fraction of its own samples it must also lead;
`--arb-min-window-samples` (32) is how many matched samples it needs before the window is judged at all;
`--arb-match-window-secs` (5) is how long one path's trade waits for the peer's copy of the same print before it is written off as unmatched, and so bounds which pairs become samples at all;
`--arb-leader-timeout-secs` (2) is the venue-wide silence after which a live path takes over.

`--arb-book-dedup-window-ms` (1000) governs the *other* gate — how long a delivered order-level book event is remembered so a slower publisher's copy is recognized as a duplicate. It is not a re-election tunable and it does not gate resurrection: an order the market has published as gone is refused however late the copy arrives (`dz_book_resurrections_dropped_total`), and that guard's reach is the channel's venue-time frontier below, not this window. An undersized window is no longer a correctness risk. Past it, a lagging path's copy of an add for an order the leader has since partially filled stops reading as a duplicate, but it is refused for being older than the last change published for that order — before its size is compared — so it lands on `dz_mbo_events_stale_total` instead of manufacturing the false `dz_mbo_path_disagreement_total`, forced re-baseline and lost withheld batches it used to. That is also why the per-market **count** cap (1024 events, about 1.15 s of the flagship market's event rate) no longer sets a lag ceiling: it still bounds what the window remembers, so the effective dedup reach remains `min(flag, that)` and a value much above a second is inert — but exceeding it now moves a copy from `dz_book_events_deduped_total` to a refusal, not into an alarm. It does **not** set the re-baseline rate limit (a fixed 250 ms), so widening it costs no extra withholding. Read it against `dz_book_events_deduped_total`.

Three further flags govern the resurrection guard itself, which runs on **venue** time rather than on this process's clock: per channel it tracks the newest `source_ts` it has accepted, and the frontier is that stamp less the retention window.
`--arb-book-retention-secs` (30) is both how far behind the frontier an order-level batch may be and still be admitted (`dz_mbo_events_past_frontier_total`) and how long a removed order is remembered so a lagging publisher's stale add for it is refused. Sized against a measured p99.99 inter-path separation of 2.77 s and 3,958 removals/s per publisher per channel — ~119k entries, 11% of the process-wide ceiling. Set below the paths' real separation, a returning link's backlog publishes as live; set far above it, every removal inside the window is held;
`--arb-book-ts-jump-secs` (5) is how far ahead of the channel's newest a batch may be and still advance it (`dz_mbo_frontier_bounded_total`). Keep it comfortably below the retention window, or one accepted jump puts the whole channel outside that window at once;
`--arb-book-reseat-secs` (10) is how long the newest stamp may fail to **move** before the frontier is re-seated from the batches actually arriving (`dz_mbo_frontier_reseats_total`). It is a genuine tradeoff rather than a safety margin: below the paths' worst separation (2.77 s measured at p99.99) the hatch fires on ordinary jitter, and above it, it bounds both how long a stuck frontier grows the removed population unforgotten and how long a market whose only surviving path sits behind that frontier stays dark. A session end unsets the frontier outright rather than zeroing it, so the next batch re-seeds it without waiting for this interval.

The margin and the win rate are **independent conditions and all three must hold** — a heavy tail alone cannot carry a transfer, neither can a high win count built on sub-margin noise, and neither can a handful of lucky matches. Health and silence ignore all three: a leader whose book for one market sits in `gap`/`awaiting-snapshot` yields *that market* to a healthy path immediately, because under incremental output a lost level does not self-heal until the next snapshot.

### Published win rate

The DZ win rate to publish is the tick-won share:

```promql
sum(rate(dz_quote_ticks_won_total{transport="edge"}[5m]))
/
sum(rate(dz_quote_ticks_won_total[5m]))
```

The `winner`/`loser` labels on `dz_quote_lead_ns`, `dz_trade_lead_ns` and `dz_depth_lead_ns` carry
**transport** classes (`edge`/`public`), the same value space as the `transport` label above; they
keep their own names because `winner`/`loser` is what distinguishes the two ends of one contest.

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

## Public WS inputs

Recorded by the optional public WebSocket backstops (Hyperliquid `src/ingest/ws_input.rs`, Phoenix
`src/ingest/phoenix_input.rs`; both off by default — see [Inputs](input-sources.md)). Every
series is labelled by `venue` so multiple inputs don't collide. An input's actual contribution to the
served feed shows up on the arbiter counters above, attributed to `transport="public"`:
`dz_quotes_admitted_total` for a quote backstop, `dz_trades_admitted_total` for a trade backstop
(Phoenix is **trades-only**, so watch the trade counter, not quotes), with `dz_quote_lead_ns` /
`dz_trade_lead_ns` giving the win margin over the transport it beat.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_ws_input_up` | gauge | `venue` | `1` while the public WS session is connected, `0` while down/reconnecting. |
| `dz_ws_input_reconnects_total` | counter | `venue` | (Re)connect cycles — a session ended or a connect attempt failed and the input backed off to retry. |
| `dz_ws_input_decode_errors_total` | counter | `venue` | Public WS frames that failed to decode (dropped best-effort). |
| `dz_ws_input_messages_total` | counter | `venue`, `kind` | Business messages decoded from the public WS and emitted, by `kind` (quote/trade). |

## Query API history writer

Recorded by the task that keeps the query API's rolling history store fed (`ingest::reconcile::feed_history`), active only while the query API sink is.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `dz_history_unattributable_trades_total` | counter | `venue` | A trade dropped rather than stored because the instrument catalog carries no definition for the exact `(venue, channel, instrument_id)` the message names — belt-and-braces for a definition race (or, on the unauthenticated wire, a forged identity). Should stay flat at zero. |
| `dz_history_feed_lagged_total` | counter | — | Times the writer fell behind the post-arbiter broadcast and dropped messages (`Lagged`) — a hole in the rolling window, not a crash. |

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
