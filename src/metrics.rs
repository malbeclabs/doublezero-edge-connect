//! Process-wide Prometheus metrics: one [`Registry`] plus every metric handle, exposed through a
//! global accessor so any task can record without threading a handle through its call chain.
//!
//! Recording is **always on** — [`metrics`] lazily initializes the registry on first use, and a
//! counter increment is a single relaxed atomic add, so the ingest hot path pays no `Option` check.
//! Only the HTTP exposer ([`crate::sinks::metrics`]) is gated behind `--metrics-bind`; when that is
//! empty the counters still advance, they are simply never scraped.
//!
//! **Label cardinality is bounded by construction.** Labels are `venue` (a handful of feeds),
//! `group`/`dest` (a handful of multicast groups / forward targets), and small fixed enums
//! (`role`, `kind`, `outcome`). There are deliberately **no per-symbol labels** — a venue carries
//! hundreds of symbols, which would explode the series count.
//!
//! On a per-message path, resolve the label-specific child once at task setup
//! (`with_label_values(&[venue])` returns a cheap cloneable handle) and reuse it, rather than doing
//! the label lookup on every datagram.

use std::sync::OnceLock;

use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

/// Buckets (nanoseconds) for the de-dup *lead-time* histograms: how far ahead the winning source
/// was when the losing duplicate arrived. Spans ~50µs … 1s, dense in the sub-millisecond range
/// where the edge feed beats the public/cross-group copy in steady state, with a long tail for the
/// tens-to-hundreds-of-ms inter-feed skew seen when a path is slow.
const LEAD_NS_BUCKETS: &[f64] = &[
    50_000.0,
    100_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
    2_500_000.0,
    5_000_000.0,
    10_000_000.0,
    25_000_000.0,
    50_000_000.0,
    100_000_000.0,
    250_000_000.0,
    500_000_000.0,
    1_000_000_000.0,
];

/// Every metric the bridge exports, plus the [`Registry`] they are registered against. Built once
/// via [`Metrics::new`] and reachable through [`metrics`].
pub struct Metrics {
    registry: Registry,

    // --- Ingest receivers (labelled by `venue`, `kind`, `publisher` — one receiver per publisher
    // of a feed, where `publisher` is its base port (`FeedPublisher::base_port`);
    // `feed_up`/`feed_stale_ms` are deliberately venue-level aggregates) ---
    /// Datagrams received per publisher, split by port `role` (mktdata/refdata/snapshot/combined).
    pub datagrams_received: IntCounterVec,
    /// Total bytes received per publisher (sum of datagram lengths).
    pub datagram_bytes: IntCounterVec,
    /// Socket/transport receive errors per publisher (each triggers a rejoin).
    pub socket_errors: IntCounterVec,
    /// Idle-rejoin watchdog firings per publisher (market data went silent past the idle window).
    pub idle_rejoin: IntCounterVec,
    /// Feed health: 1 while the venue's market-data multicast is up, 0 while it is considered down.
    /// A **venue-level aggregate** over its publishers (up if any is up) — see
    /// [`receiver_up`](Self::receiver_up) and `ingest::health`.
    pub feed_up: IntGaugeVec,
    /// Market-data staleness: 0 while up; the staleness in milliseconds at the last `down`
    /// transition (reset to 0 on recovery). Venue-level, like [`feed_up`](Self::feed_up).
    pub feed_stale_ms: IntGaugeVec,
    /// Per-receiver health: 1 while this publisher's market-data stream is up, 0 while it is
    /// considered down. The per-publisher counterpart of [`feed_up`](Self::feed_up), which is the
    /// venue-level aggregate (up if ANY publisher is up). A venue can be `dz_feed_up == 1` while
    /// one of its publishers reads `dz_receiver_up == 0`; that is the wedged-mirror signal.
    pub receiver_up: IntGaugeVec,
    /// Frame-sequence classifications per feed, by `kind` (first/ok/reset/stale).
    pub seq_events: IntCounterVec,

    // --- Arbiter emit stage (labelled by `venue`) ---
    /// Messages that survived dedup and were broadcast, by `kind` (quote/trade/instrument/midpoint/
    /// depth). `status` is structurally possible but currently never routed through the arbiter, so
    /// that child is not recorded in practice.
    pub emit: IntCounterVec,
    /// Quotes admitted by the staleness floor, attributed to the winning `publisher` (edge/public).
    /// A rise in `publisher="public"` is the direct signal of the public backstop filling a gap.
    pub quotes_admitted: IntCounterVec,
    /// Quote `source_ts` ticks *won*: the once-per-tick first delivery, attributed to the winning
    /// `publisher` class. Every tick counts exactly once — a mirror's copy or the leader's later
    /// in-tick contents never re-count it, and a tick the public feed never delivers still counts
    /// for the edge (the walkover) — so `edge / sum` is the published DZ win rate. Contrast
    /// [`quote_lead_ns`](Self::quote_lead_ns), which samples only in-tick head-to-head contests
    /// (at most one per tick, consumed by whichever follower arrives first — a mirror's sub-ms
    /// copy usually) and is a dedup diagnostic, not a win rate. `source_ts == 0` sentinel quotes
    /// bypass the floor and are not counted.
    pub quote_ticks_won: IntCounterVec,
    /// Quotes dropped by the staleness floor (stale tick, non-leader, or exact repeat — collapsed).
    pub quotes_dropped: IntCounterVec,
    /// Trades admitted by the windowed dedup, attributed to the winning `publisher` (edge/public) —
    /// the trade-side mirror of [`quotes_admitted`].
    pub trades_admitted: IntCounterVec,
    /// Trades collapsed before the wire: a duplicate `trade_id` still inside the dedup window, or —
    /// on a `Sticky` venue — a non-serving arm's copy dropped by the per-venue tape gate. In steady
    /// state on such a venue this is the challenger arm's whole print stream.
    pub trades_dropped: IntCounterVec,
    /// Trades forwarded with the `trade_id == 0` sentinel, bypassing the dedup window.
    pub trades_no_id: IntCounterVec,
    /// Zero-id trades forwarded from a second publisher for a `(venue, symbol)` another publisher
    /// already owns — the tape is double-printing, since a bypassed sentinel has no window to
    /// collapse against. `Coordinated` venues only in practice: on a `Sticky` venue the tape gate
    /// upstream has already dropped the peer arm's copy.
    pub trades_no_id_conflict: IntCounterVec,
    /// Instrument definitions dropped as an exact content repeat of the last one broadcast for the
    /// `(venue, symbol)` - the mirrored publishers' identical refdata bursts collapsing.
    pub instruments_dropped: IntCounterVec,
    /// Quote-tick *cross-source* contest lead time (ns): on a `source_ts` tick another publisher
    /// already led, how far ahead the leader was when this publisher's first copy arrived, labelled
    /// by the `winner` **and** `loser` (edge/public). Its `_count` is the head-to-head contest
    /// count. Labelling both ends keeps an edge-vs-edge mirror race (small, sub-ms leads in a
    /// multi-mirror deployment) from diluting the headline edge-vs-public margin: the buckets of
    /// `{winner="edge",loser="public"}` are the margin by which DZ (edge) beats the public feed,
    /// while `{winner="edge",loser="edge"}` is the inter-mirror skew. See [`LEAD_NS_BUCKETS`].
    pub quote_lead_ns: HistogramVec,
    /// Trade *cross-source* contest lead time (ns): when a duplicate `trade_id` arrives from a
    /// different publisher than the one that first delivered it, how far ahead the first was,
    /// labelled by the `winner` and `loser` (see [`quote_lead_ns`](Self::quote_lead_ns)).
    pub trade_lead_ns: HistogramVec,
    /// MBO `depth` snapshots admitted by the staleness floor, attributed to the winning `publisher`
    /// (edge/public) — the depth mirror of [`quotes_admitted`]. Per-publisher books race on one floor
    /// per (venue, symbol); a rise here for a given publisher class shows which source is currently
    /// leading the reconstructed book.
    pub depth_admitted: IntCounterVec,
    /// MBO `depth` `source_ts` ticks *won* — the depth mirror of
    /// [`quote_ticks_won`](Self::quote_ticks_won) (for depth, the `source_ts == 0` empty-anchor
    /// tick is real and counts).
    pub depth_ticks_won: IntCounterVec,
    /// MBO `depth` snapshots dropped by the staleness floor (stale tick, non-leader publisher's
    /// redundant book, or the leader's exact content repeat — the cross-publisher collapse),
    /// attributed to the `publisher` class whose copy was dropped — the symmetric counterpart of
    /// [`depth_admitted`](Self::depth_admitted)'s winner attribution, so a lagging source (who is
    /// *losing* the book race) is directly visible.
    pub depth_dropped: IntCounterVec,
    /// Depth-floor entries cleared by the session-reset escape hatch, by `reason`
    /// (`end_of_session` / `instrument_reset`). A venue restarting its event clock below the
    /// latched high-water would otherwise wedge depth permanently; see
    /// [`crate::ingest::arbiter::Arbiter::reset_depth_floor_for_venue`].
    pub depth_floor_resets: IntCounterVec,
    /// Depth *cross-source* contest lead time (ns): when a second publisher's book snapshot arrives
    /// at a `source_ts` tick the leader already opened, how far ahead the leader was, labelled by the
    /// `winner` and `loser` — the depth mirror of [`quote_lead_ns`](Self::quote_lead_ns).
    pub depth_lead_ns: HistogramVec,
    /// `depth` rejected for an implausibly-far-future `source_ts` before it could advance the floor.
    pub depth_future_rejected: IntCounterVec,
    /// Quotes rejected for an implausibly-far-future `source_ts` before they could advance the floor.
    pub quotes_future_rejected: IntCounterVec,
    /// Quotes forwarded with the `source_ts == 0` "not available" sentinel (bypass the floor).
    pub quotes_no_source_ts: IntCounterVec,
    /// Nanoseconds between the two arms' copies of one **matched trade**, on our own receive clock —
    /// the series `--arb-transfer-margin-us` is read off. Fed only by
    /// [`crate::ingest::arm_race::ArmRace`] pairs, never by a dropped copy's inter-arm phase.
    pub arm_lead_ns: HistogramVec,
    /// Authority transfers by `reason` (initial/health/silence/margin). A sustained rate means the
    /// thresholds are too loose: every transfer re-baselines each consumer's book.
    pub arm_transfers: IntCounterVec,
    /// Trade-tape ownership moving from one of a venue's **feed rows** to another (the reconciler's
    /// decision, on a subscription change). Each move is a window in which a print may double or
    /// drop, so a sustained rate means subscriptions are flapping.
    pub tape_owner_changes: IntCounterVec,
    /// Trade-tape ownership moving from one **arm** to another within a venue — the arm-level twin of
    /// [`tape_owner_changes`](Self::tape_owner_changes), and the same read: each transfer is a window
    /// where a print may double or drop.
    pub tape_arm_transfers: IntCounterVec,
    /// Markets each `arm` is currently authoritative for. Split across arms means the venue's
    /// authority is fragmented; all on one arm is the steady state.
    pub arm_markets_held: IntGaugeVec,
    /// Trades an `arm` delivered that its peer never did inside the match window — a drop on one arm,
    /// or a genuine one-sided print. The denominator for how much of the election's evidence is being
    /// lost; a rate near the trade rate means the arms are barely pairing at all.
    pub arm_unmatched_trades: IntCounterVec,
    /// Incremental `book` batches the authority gate did not publish, by the `publisher` class whose
    /// copy was dropped: a non-authoritative arm's copy, or a batch withheld while a market waits for
    /// its new arm to close a logical event. In steady state this is the challenger's whole stream, so
    /// it tracks its message rate rather than any fault.
    pub book_dropped: IntCounterVec,
    /// Markets evicted from the `book` authority gate's tracked set because the cap was reached. The
    /// key is wire-supplied, so this is the forged-market backstop: an evicted market loses its replay
    /// bootstrap, and its next batch re-baselines the consumer from whatever it accumulates again.
    pub book_markets_evicted: IntCounterVec,

    // --- Market-by-price processor (per `venue`) ---
    /// One publisher-and-channel's books discarded on a frame-header `Reset Count` change.
    pub mbp_channel_resets: IntCounterVec,
    /// Cross-instrument delta-buffer budget overflows; each dropped the largest instrument's buffer.
    /// Sustained means the publisher's snapshot period is too long for this host's memory budget.
    pub mbp_buffer_overflows: IntCounterVec,
    /// A book discarded because its per-book price-level cap was hit — a malformed or forged stream,
    /// never packet loss. Deliberately not counted as a sequence gap: the cause and the resulting
    /// status differ, and merging them would read a hostile book as a lossy network.
    pub mbp_level_overflows: IntCounterVec,
    /// `SnapshotLevel` with no open group to route it to — a publisher interleaving snapshot groups,
    /// or a lost `SnapshotBegin`.
    pub mbp_orphan_snapshot_levels: IntCounterVec,
    /// Deltas discarded as duplicates (`seq` at or below the applied baseline). A `Ready` book
    /// emitting nothing but duplicates is the signature of a baseline installed above the
    /// publisher's real counter, which only a routed `Reset Count` clears — so this is the one
    /// series that surfaces that wedge.
    pub mbp_duplicate_deltas: IntCounterVec,
    /// Crossed inside markets observed at a `BatchBoundary`. Observability only; never acted on.
    pub mbp_crossed: IntCounterVec,
    /// Publisher `Action`-vs-quantity disagreements by `kind`. Never changes the applied result.
    pub mbp_divergence: IntCounterVec,

    // --- WebSocket sink ---
    /// Currently-connected WebSocket clients.
    pub ws_clients: IntGauge,
    /// Connection attempts by `outcome` (accepted/rejected).
    pub ws_connections: IntCounterVec,
    /// Messages forwarded to clients, by `kind` (quote/trade/midpoint/depth/status/instrument).
    pub ws_messages_sent: IntCounterVec,
    /// Bytes forwarded to clients, by `kind` (sum of serialized JSON payload lengths).
    pub ws_bytes_sent: IntCounterVec,
    /// Times a client fell behind and the broadcast dropped messages for it (`Lagged`).
    pub ws_client_lagged: IntCounter,
    /// Times the single serializer task fell behind the backbone and dropped messages (`Lagged`).
    /// Distinct from [`Self::ws_client_lagged`]: this is a global stall (every client misses those
    /// messages), not one slow consumer, so it must not hide behind the per-client counter.
    pub ws_serializer_lagged: IntCounter,
    /// Inbound control messages, by `kind` (ping/subscribe/unsubscribe/error).
    pub ws_inbound: IntCounterVec,
    /// Clients disconnected for exceeding the inbound rate limit.
    pub ws_rate_limited: IntCounter,
    /// Clients reaped for crossing the idle timeout.
    pub ws_idle_timeout: IntCounter,

    // --- Public WS input feeders (per-venue backstops; off by default) ---
    /// Feeder health per `venue`: 1 while the public WebSocket session is connected, 0 while
    /// down/reconnecting.
    pub ws_feeder_up: IntGaugeVec,
    /// Public WS (re)connect cycles per `venue` — incremented each time a session ends or a connect
    /// attempt fails and the feeder backs off to retry.
    pub ws_feeder_reconnects: IntCounterVec,
    /// Public WS frames that failed to decode (undecodable envelope; dropped best-effort), by `venue`.
    pub ws_feeder_decode_errors: IntCounterVec,
    /// Business messages decoded from the public WS and emitted through the arbiter, by `venue` and
    /// `kind` (quote/trade).
    pub ws_feeder_messages: IntCounterVec,

    // --- Shred forwarder ---
    /// Shred datagrams received per source `group`.
    pub shred_datagrams_received: IntCounterVec,
    /// Total bytes received per source `group` (sum of shred datagram lengths).
    pub shred_datagram_bytes: IntCounterVec,
    /// Shred datagrams dropped at the receiver per `group` (forwarder queue full — backpressure).
    pub shred_receiver_dropped: IntCounterVec,
    /// Shred datagrams that entered the dedup/forward gate.
    pub shred_processed: IntCounter,
    /// Shred datagrams successfully parsed (signature/slot/index extracted).
    pub shred_parsed: IntCounter,
    /// Shred datagrams that could not be parsed (forwarded undeduped, loss-averse).
    pub shred_unparsed: IntCounter,
    /// Shred datagrams forwarded to destinations.
    pub shred_forwarded: IntCounter,
    /// Shred datagrams dropped by the dedup/sigverify gate.
    pub shred_dropped: IntCounter,
    /// Shreds whose leader signature verified (sigverify mode only).
    pub shred_verify_ok: IntCounter,
    /// Shreds dropped fail-closed for want of a known slot leader (sigverify mode only).
    pub shred_no_leader: IntCounter,
    /// Slots currently tracked by the dedup window.
    pub shred_dedup_tracked_slots: IntGauge,
    /// Cross-group shred contests won, by the multicast `winner` group that delivered first. Each
    /// increment is a duplicate from a *different* group dropped because this group's copy already
    /// forwarded — the head-to-head "this group beat the others" count.
    pub shred_wins: IntCounterVec,
    /// Cross-group shred contest lead time (ns): when a duplicate arrives from a different group
    /// than the one that first forwarded the shred, how far ahead the winner was, labelled by the
    /// `winner` group. See [`LEAD_NS_BUCKETS`].
    pub shred_lead_ns: HistogramVec,
    /// Per-destination forward sends, by `dest` and `outcome` (ok/error).
    pub shred_sends: IntCounterVec,
    /// Bytes successfully forwarded to each destination, by `dest` (sum of datagram lengths on a
    /// successful send; a failed send delivers nothing and is not counted here).
    pub shred_bytes_sent: IntCounterVec,
}

/// Build an [`IntCounterVec`] and register it, panicking on a registration error (a duplicate name
/// or bad label set is a programming bug, surfaced loudly at startup).
fn counter_vec(reg: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid counter vec");
    reg.register(Box::new(c.clone()))
        .expect("register counter vec");
    c
}

fn counter(reg: &Registry, name: &str, help: &str) -> IntCounter {
    let c = IntCounter::with_opts(Opts::new(name, help)).expect("valid counter");
    reg.register(Box::new(c.clone())).expect("register counter");
    c
}

fn gauge_vec(reg: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid gauge vec");
    reg.register(Box::new(g.clone()))
        .expect("register gauge vec");
    g
}

fn gauge(reg: &Registry, name: &str, help: &str) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(name, help)).expect("valid gauge");
    reg.register(Box::new(g.clone())).expect("register gauge");
    g
}

fn histogram_vec(
    reg: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: &[f64],
) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid histogram vec");
    reg.register(Box::new(h.clone()))
        .expect("register histogram vec");
    h
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();

        // Linux process metrics (CPU, resident memory, open fds) — free via the `process` feature.
        #[cfg(target_os = "linux")]
        {
            let pc = prometheus::process_collector::ProcessCollector::for_self();
            registry
                .register(Box::new(pc))
                .expect("register process collector");
        }

        Self {
            datagrams_received: counter_vec(
                &registry,
                "dz_datagrams_received_total",
                "DZ Edge multicast datagrams received per publisher and port role",
                &["venue", "kind", "publisher", "role"],
            ),
            datagram_bytes: counter_vec(
                &registry,
                "dz_datagram_bytes_total",
                "Total bytes received per publisher",
                &["venue", "kind", "publisher"],
            ),
            socket_errors: counter_vec(
                &registry,
                "dz_socket_errors_total",
                "Socket/transport receive errors per publisher (each triggers a rejoin)",
                &["venue", "kind", "publisher"],
            ),
            idle_rejoin: counter_vec(
                &registry,
                "dz_idle_rejoin_total",
                "Idle-rejoin watchdog firings per publisher",
                &["venue", "kind", "publisher"],
            ),
            feed_up: gauge_vec(
                &registry,
                "dz_feed_up",
                "Feed health: 1 if any of the venue's publishers has market data up, 0 if all down",
                &["venue"],
            ),
            feed_stale_ms: gauge_vec(
                &registry,
                "dz_feed_stale_ms",
                "Market-data staleness in ms: 0 while up; staleness at the last down transition",
                &["venue"],
            ),
            receiver_up: gauge_vec(
                &registry,
                "dz_receiver_up",
                "Per-publisher receiver health: 1 if this publisher's market data is up, 0 if down",
                &["venue", "kind", "publisher"],
            ),
            seq_events: counter_vec(
                &registry,
                "dz_seq_events_total",
                "Frame-sequence classifications per feed (first/ok/reset/stale)",
                &["venue", "kind"],
            ),
            emit: counter_vec(
                &registry,
                "dz_emit_total",
                "Messages broadcast after dedup, by venue and kind",
                &["venue", "kind"],
            ),
            quotes_admitted: counter_vec(
                &registry,
                "dz_quotes_admitted_total",
                "Quotes admitted by the staleness floor, by winning publisher (edge/public)",
                &["venue", "publisher"],
            ),
            quote_ticks_won: counter_vec(
                &registry,
                "dz_quote_ticks_won_total",
                "Quote source_ts ticks won (first delivery of each tick), by publisher class \
                 (edge/public); every tick counts exactly once, so edge/sum is the published \
                 win rate",
                &["venue", "publisher"],
            ),
            quotes_dropped: counter_vec(
                &registry,
                "dz_quotes_dropped_total",
                "Quotes dropped by the staleness floor",
                &["venue"],
            ),
            trades_admitted: counter_vec(
                &registry,
                "dz_trades_admitted_total",
                "Trades admitted by the windowed dedup, by winning publisher (edge/public)",
                &["venue", "publisher"],
            ),
            trades_dropped: counter_vec(
                &registry,
                "dz_trades_dropped_total",
                "Trades collapsed before the wire: a duplicate trade_id inside the window, or a \
                 non-serving arm's copy dropped by a Sticky venue's tape gate",
                &["venue"],
            ),
            instruments_dropped: counter_vec(
                &registry,
                "dz_instruments_dropped_total",
                "Instrument definitions dropped as an exact repeat of the last broadcast content",
                &["venue"],
            ),
            quote_lead_ns: histogram_vec(
                &registry,
                "dz_quote_lead_ns",
                "Nanoseconds the winning publisher led the losing duplicate by, per quote-tick \
                 cross-source contest, by winner and loser (edge/public). Splitting on both ends \
                 keeps an edge-vs-edge mirror race out of the headline edge-vs-public margin: \
                 {winner=\"edge\",loser=\"public\"} is 'DZ beats the public feed'.",
                &["venue", "winner", "loser"],
                LEAD_NS_BUCKETS,
            ),
            trade_lead_ns: histogram_vec(
                &registry,
                "dz_trade_lead_ns",
                "Nanoseconds the winning publisher led the losing duplicate by, per trade \
                 cross-source contest, by winner and loser (edge/public). See dz_quote_lead_ns \
                 for why both ends are labelled.",
                &["venue", "winner", "loser"],
                LEAD_NS_BUCKETS,
            ),
            depth_admitted: counter_vec(
                &registry,
                "dz_depth_admitted_total",
                "MBO depth admitted by the staleness floor, by winning publisher (edge/public)",
                &["venue", "publisher"],
            ),
            depth_ticks_won: counter_vec(
                &registry,
                "dz_depth_ticks_won_total",
                "MBO depth source_ts ticks won (first delivery of each tick), by publisher class \
                 (edge/public); the depth mirror of dz_quote_ticks_won_total",
                &["venue", "publisher"],
            ),
            depth_dropped: counter_vec(
                &registry,
                "dz_depth_dropped_total",
                "MBO depth dropped by the staleness floor, by the publisher whose copy was dropped",
                &["venue", "publisher"],
            ),
            depth_floor_resets: counter_vec(
                &registry,
                "dz_depth_floor_resets_total",
                "Depth-floor entries cleared by the session-reset escape hatch, by reason",
                &["venue", "reason"],
            ),
            depth_lead_ns: histogram_vec(
                &registry,
                "dz_depth_lead_ns",
                "Nanoseconds the winning publisher led the losing duplicate by, per depth \
                 cross-publisher contest, by winner and loser (edge/public). See dz_quote_lead_ns \
                 for why both ends are labelled.",
                &["venue", "winner", "loser"],
                LEAD_NS_BUCKETS,
            ),
            depth_future_rejected: counter_vec(
                &registry,
                "dz_depth_future_rejected_total",
                "MBO depth rejected for an implausibly-far-future source_ts",
                &["venue"],
            ),
            quotes_future_rejected: counter_vec(
                &registry,
                "dz_quotes_future_rejected_total",
                "Quotes rejected for an implausibly-far-future source_ts",
                &["venue"],
            ),
            quotes_no_source_ts: counter_vec(
                &registry,
                "dz_quotes_no_source_ts_total",
                "Quotes forwarded with the source_ts==0 sentinel (floor bypassed)",
                &["venue"],
            ),
            arm_lead_ns: histogram_vec(
                &registry,
                "dz_arm_lead_ns",
                "Nanoseconds between the two arms' copies of one matched trade, on our own receive \
                 clock. The series the re-election thresholds are read off.",
                &["venue", "winner"],
                LEAD_NS_BUCKETS,
            ),
            arm_transfers: counter_vec(
                &registry,
                "dz_arm_authority_transfers_total",
                "Authority transfers by reason (initial/health/silence/margin). A sustained rate \
                 means the thresholds are too loose — every transfer re-baselines each consumer.",
                &["venue", "reason"],
            ),
            arm_markets_held: gauge_vec(
                &registry,
                "dz_arm_markets_held",
                "Markets each arm is currently authoritative for.",
                &["venue", "arm"],
            ),
            tape_owner_changes: counter_vec(
                &registry,
                "dz_tape_owner_changes_total",
                "Trade-tape ownership moving between a venue's feed rows on a subscription change. \
                 Each move is a window in which a print may double or drop.",
                &["venue"],
            ),
            tape_arm_transfers: counter_vec(
                &registry,
                "dz_tape_arm_transfers_total",
                "Trade-tape ownership moving between a venue's arms. The arm-level twin of \
                 dz_tape_owner_changes_total.",
                &["venue"],
            ),
            mbp_channel_resets: counter_vec(
                &registry,
                "dz_mbp_channel_resets_total",
                "Publisher-and-channel book state discarded on a frame-header Reset Count change",
                &["venue"],
            ),
            mbp_buffer_overflows: counter_vec(
                &registry,
                "dz_mbp_buffer_overflows_total",
                "Cross-instrument delta-buffer budget overflows; the largest instrument's buffer \
                 was dropped. Sustained means the snapshot period is too long for this host.",
                &["venue"],
            ),
            mbp_level_overflows: counter_vec(
                &registry,
                "dz_mbp_level_overflows_total",
                "Books discarded on hitting the per-book price-level cap (malformed or forged \
                 stream, never packet loss — distinct from a sequence gap)",
                &["venue"],
            ),
            mbp_orphan_snapshot_levels: counter_vec(
                &registry,
                "dz_mbp_orphan_snapshot_levels_total",
                "SnapshotLevel with no open group to route it to (interleaved groups, or a lost \
                 SnapshotBegin)",
                &["venue"],
            ),
            mbp_duplicate_deltas: counter_vec(
                &registry,
                "dz_mbp_duplicate_deltas_total",
                "Deltas discarded as duplicates. A Ready book emitting only these is a baseline \
                 above the publisher's real counter, which only a Reset Count clears.",
                &["venue"],
            ),
            mbp_crossed: counter_vec(
                &registry,
                "dz_mbp_crossed_total",
                "Crossed inside markets observed at a BatchBoundary (observability only)",
                &["venue"],
            ),
            mbp_divergence: counter_vec(
                &registry,
                "dz_mbp_divergence_total",
                "Publisher Action-vs-quantity disagreements by kind; never changes the applied \
                 result",
                &["venue", "kind"],
            ),
            arm_unmatched_trades: counter_vec(
                &registry,
                "dz_arm_unmatched_trades_total",
                "Trades one arm delivered that its peer never did inside the match window — a drop \
                 on that arm, or a one-sided print. Election evidence lost.",
                &["venue", "arm"],
            ),
            book_dropped: counter_vec(
                &registry,
                "dz_book_dropped_total",
                "Incremental book batches the authority gate did not publish, by the publisher class \
                 whose copy was dropped (in steady state, the challenger arm's whole stream).",
                &["venue", "publisher"],
            ),
            book_markets_evicted: counter_vec(
                &registry,
                "dz_book_markets_evicted_total",
                "Markets evicted from the book authority gate's tracked set because the cap was \
                 reached; an evicted market loses its replay bootstrap.",
                &["venue"],
            ),
            trades_no_id: counter_vec(
                &registry,
                "dz_trades_no_id_total",
                "Trades forwarded with the trade_id==0 sentinel (dedup window bypassed)",
                &["venue"],
            ),
            trades_no_id_conflict: counter_vec(
                &registry,
                "dz_trades_no_id_conflict_total",
                "Zero-id trades from a second publisher for a (venue, symbol) another owns \
                 (the tape is double-printing)",
                &["venue"],
            ),
            ws_clients: gauge(
                &registry,
                "dz_ws_clients",
                "Currently-connected WebSocket clients",
            ),
            ws_connections: counter_vec(
                &registry,
                "dz_ws_connections_total",
                "WebSocket connection attempts by outcome (accepted/rejected)",
                &["outcome"],
            ),
            ws_messages_sent: counter_vec(
                &registry,
                "dz_ws_messages_sent_total",
                "Messages forwarded to WebSocket clients, by kind",
                &["kind"],
            ),
            ws_bytes_sent: counter_vec(
                &registry,
                "dz_ws_bytes_sent_total",
                "Bytes forwarded to WebSocket clients, by kind",
                &["kind"],
            ),
            ws_client_lagged: counter(
                &registry,
                "dz_ws_client_lagged_total",
                "Times a slow client fell behind and the broadcast dropped messages for it",
            ),
            ws_serializer_lagged: counter(
                &registry,
                "dz_ws_serializer_lagged_total",
                "Times the serializer task fell behind the backbone and dropped messages (global)",
            ),
            ws_inbound: counter_vec(
                &registry,
                "dz_ws_inbound_total",
                "Inbound control messages from clients, by kind",
                &["kind"],
            ),
            ws_rate_limited: counter(
                &registry,
                "dz_ws_rate_limited_total",
                "Clients disconnected for exceeding the inbound rate limit",
            ),
            ws_idle_timeout: counter(
                &registry,
                "dz_ws_idle_timeout_total",
                "Clients reaped for crossing the idle timeout",
            ),
            ws_feeder_up: gauge_vec(
                &registry,
                "dz_ws_feeder_up",
                "Public WS input feeder health: 1 while connected, 0 while down/reconnecting",
                &["venue"],
            ),
            ws_feeder_reconnects: counter_vec(
                &registry,
                "dz_ws_feeder_reconnects_total",
                "Public WS (re)connect cycles (session ended or connect attempt failed)",
                &["venue"],
            ),
            ws_feeder_decode_errors: counter_vec(
                &registry,
                "dz_ws_feeder_decode_errors_total",
                "Public WS frames that failed to decode (dropped best-effort)",
                &["venue"],
            ),
            ws_feeder_messages: counter_vec(
                &registry,
                "dz_ws_feeder_messages_total",
                "Business messages decoded from the public WS and emitted, by venue and kind",
                &["venue", "kind"],
            ),
            shred_datagrams_received: counter_vec(
                &registry,
                "dz_shred_datagrams_received_total",
                "Shred datagrams received per source group",
                &["group"],
            ),
            shred_datagram_bytes: counter_vec(
                &registry,
                "dz_shred_datagram_bytes_total",
                "Total bytes received per source group",
                &["group"],
            ),
            shred_receiver_dropped: counter_vec(
                &registry,
                "dz_shred_receiver_dropped_total",
                "Shred datagrams dropped at the receiver (forwarder queue full)",
                &["group"],
            ),
            shred_processed: counter(
                &registry,
                "dz_shred_processed_total",
                "Shred datagrams that entered the dedup/forward gate",
            ),
            shred_parsed: counter(
                &registry,
                "dz_shred_parsed_total",
                "Shred datagrams successfully parsed",
            ),
            shred_unparsed: counter(
                &registry,
                "dz_shred_unparsed_total",
                "Shred datagrams that could not be parsed (forwarded undeduped)",
            ),
            shred_forwarded: counter(
                &registry,
                "dz_shred_forwarded_total",
                "Shred datagrams forwarded to destinations",
            ),
            shred_dropped: counter(
                &registry,
                "dz_shred_dropped_total",
                "Shred datagrams dropped by the dedup/sigverify gate",
            ),
            shred_verify_ok: counter(
                &registry,
                "dz_shred_verify_ok_total",
                "Shreds whose leader signature verified (sigverify mode)",
            ),
            shred_no_leader: counter(
                &registry,
                "dz_shred_no_leader_total",
                "Shreds dropped fail-closed for want of a known slot leader (sigverify mode)",
            ),
            shred_dedup_tracked_slots: gauge(
                &registry,
                "dz_shred_dedup_tracked_slots",
                "Slots currently tracked by the dedup window",
            ),
            shred_wins: counter_vec(
                &registry,
                "dz_shred_wins_total",
                "Cross-group shred contests won, by the multicast group that delivered first",
                &["winner"],
            ),
            shred_lead_ns: histogram_vec(
                &registry,
                "dz_shred_lead_ns",
                "Nanoseconds the winning group led the losing duplicate by, per cross-group shred \
                 contest, by winner group",
                &["winner"],
                LEAD_NS_BUCKETS,
            ),
            shred_sends: counter_vec(
                &registry,
                "dz_shred_sends_total",
                "Per-destination forward sends, by dest and outcome",
                &["dest", "outcome"],
            ),
            shred_bytes_sent: counter_vec(
                &registry,
                "dz_shred_bytes_sent_total",
                "Bytes successfully forwarded to each destination",
                &["dest"],
            ),
            registry,
        }
    }

    /// The registry, for the HTTP exposer to `gather()` and encode.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// The process-wide [`Metrics`], initialized on first use. Cheap to call repeatedly.
///
/// **Test isolation.** This registry is a single process-global, shared by every test in a binary
/// that runs in parallel. A test asserting an exact metric *value* must therefore either key on a
/// label value unique to that test (so no other test touches the same child) or assert relative to a
/// captured baseline under `#[serial_test::serial]` — never assume a counter/gauge starts at zero.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    #[test]
    fn registry_encodes_and_contains_expected_names() {
        let m = metrics();
        // Touch a few families so they appear in the text output (a zero CounterVec child only
        // materializes once a label set is observed).
        m.datagrams_received
            .with_label_values(&["Hyperliquid", "tob", "9201", "mktdata"])
            .inc();
        m.emit.with_label_values(&["Hyperliquid", "quote"]).inc();
        m.ws_clients.set(0);
        m.shred_processed.inc();
        m.trades_admitted
            .with_label_values(&["Hyperliquid", "edge"])
            .inc();
        m.quote_lead_ns
            .with_label_values(&["Hyperliquid", "edge", "public"])
            .observe(123_456.0);
        m.trade_lead_ns
            .with_label_values(&["Hyperliquid", "edge", "public"])
            .observe(123_456.0);
        m.depth_admitted
            .with_label_values(&["Hyperliquid", "edge"])
            .inc();
        m.depth_dropped
            .with_label_values(&["Hyperliquid", "edge"])
            .inc();
        m.depth_floor_resets
            .with_label_values(&["Hyperliquid", "end_of_session"])
            .inc();
        m.depth_lead_ns
            .with_label_values(&["Hyperliquid", "edge", "public"])
            .observe(123_456.0);
        m.quote_ticks_won
            .with_label_values(&["Hyperliquid", "edge"])
            .inc();
        m.depth_ticks_won
            .with_label_values(&["Hyperliquid", "edge"])
            .inc();
        m.arm_lead_ns
            .with_label_values(&["Lashay", "leader"])
            .observe(123_456.0);
        m.arm_transfers
            .with_label_values(&["Lashay", "silence"])
            .inc();
        m.arm_markets_held
            .with_label_values(&["Lashay", "arm0"])
            .set(1);
        m.mbp_channel_resets.with_label_values(&["Lashay"]).inc();
        m.mbp_buffer_overflows.with_label_values(&["Lashay"]).inc();
        m.mbp_level_overflows.with_label_values(&["Lashay"]).inc();
        m.mbp_orphan_snapshot_levels
            .with_label_values(&["Lashay"])
            .inc();
        m.mbp_duplicate_deltas.with_label_values(&["Lashay"]).inc();
        m.mbp_crossed.with_label_values(&["Lashay"]).inc();
        m.mbp_divergence
            .with_label_values(&["Lashay", "delete_with_quantity"])
            .inc();
        m.arm_unmatched_trades
            .with_label_values(&["Lashay", "arm1"])
            .inc();
        m.book_dropped.with_label_values(&["Lashay", "edge"]).inc();
        m.book_markets_evicted.with_label_values(&["Lashay"]).inc();
        m.shred_wins.with_label_values(&["239.0.0.1"]).inc();
        m.shred_lead_ns
            .with_label_values(&["239.0.0.1"])
            .observe(123_456.0);

        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&m.registry().gather(), &mut buf)
            .expect("encode metrics");
        let out = String::from_utf8(buf).expect("utf8 metrics output");

        for name in [
            "dz_datagrams_received_total",
            "dz_emit_total",
            "dz_ws_clients",
            "dz_shred_processed_total",
            "dz_trades_admitted_total",
            "dz_quote_lead_ns",
            "dz_trade_lead_ns",
            "dz_depth_admitted_total",
            "dz_depth_dropped_total",
            "dz_depth_floor_resets_total",
            "dz_depth_lead_ns",
            "dz_quote_ticks_won_total",
            "dz_depth_ticks_won_total",
            "dz_arm_lead_ns",
            "dz_arm_authority_transfers_total",
            "dz_arm_markets_held",
            "dz_mbp_channel_resets_total",
            "dz_mbp_buffer_overflows_total",
            "dz_mbp_level_overflows_total",
            "dz_mbp_orphan_snapshot_levels_total",
            "dz_mbp_duplicate_deltas_total",
            "dz_mbp_crossed_total",
            "dz_mbp_divergence_total",
            "dz_arm_unmatched_trades_total",
            "dz_book_dropped_total",
            "dz_book_markets_evicted_total",
            "dz_shred_wins_total",
            "dz_shred_lead_ns",
        ] {
            assert!(out.contains(name), "expected `{name}` in metrics output");
        }
    }

    /// The receiver-side counters carry `kind` and `publisher` so a six-publisher venue's series
    /// don't collapse into one. A unique venue label keeps this independent of other tests
    /// touching the process-global registry.
    #[test]
    fn receiver_metrics_are_labelled_per_publisher() {
        let m = metrics();
        let venue = "PerPublisherLabelTest";
        m.datagrams_received
            .with_label_values(&[venue, "tob", "9101", "mktdata"])
            .inc();
        m.datagrams_received
            .with_label_values(&[venue, "tob", "9201", "mktdata"])
            .inc_by(3);
        m.datagram_bytes
            .with_label_values(&[venue, "tob", "9101"])
            .inc_by(100);
        m.socket_errors
            .with_label_values(&[venue, "tob", "9101"])
            .inc();
        m.idle_rejoin
            .with_label_values(&[venue, "mbo", "10601"])
            .inc();
        m.receiver_up
            .with_label_values(&[venue, "mbo", "10601"])
            .set(0);

        // Distinct publishers are distinct series, not a merged total.
        assert_eq!(
            m.datagrams_received
                .with_label_values(&[venue, "tob", "9101", "mktdata"])
                .get(),
            1
        );
        assert_eq!(
            m.datagrams_received
                .with_label_values(&[venue, "tob", "9201", "mktdata"])
                .get(),
            3
        );
        assert_eq!(
            m.receiver_up
                .with_label_values(&[venue, "mbo", "10601"])
                .get(),
            0
        );
    }
}
