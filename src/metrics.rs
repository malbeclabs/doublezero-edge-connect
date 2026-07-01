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

    // --- Ingest receivers (labelled by `venue`) ---
    /// Datagrams received per feed, split by port `role` (mktdata/refdata/snapshot/combined).
    pub datagrams_received: IntCounterVec,
    /// Total bytes received per feed (sum of datagram lengths).
    pub datagram_bytes: IntCounterVec,
    /// Socket/transport receive errors per feed (each triggers a rejoin).
    pub socket_errors: IntCounterVec,
    /// Idle-rejoin watchdog firings per feed (market data went silent past the idle window).
    pub idle_rejoin: IntCounterVec,
    /// Feed health: 1 while the market-data multicast is up, 0 while it is considered down.
    pub feed_up: IntGaugeVec,
    /// Market-data staleness: 0 while up; the staleness in milliseconds at the last `down`
    /// transition (reset to 0 on recovery).
    pub feed_stale_ms: IntGaugeVec,
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
    /// Quotes dropped by the staleness floor (stale tick, non-leader, or exact repeat — collapsed).
    pub quotes_dropped: IntCounterVec,
    /// Trades admitted by the windowed dedup, attributed to the winning `publisher` (edge/public) —
    /// the trade-side mirror of [`quotes_admitted`].
    pub trades_admitted: IntCounterVec,
    /// Trades dropped by the windowed dedup (a duplicate `trade_id` still inside the window).
    pub trades_dropped: IntCounterVec,
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
    /// Quotes rejected for an implausibly-far-future `source_ts` before they could advance the floor.
    pub quotes_future_rejected: IntCounterVec,
    /// Quotes forwarded with the `source_ts == 0` "not available" sentinel (bypass the floor).
    pub quotes_no_source_ts: IntCounterVec,

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
    /// Inbound control messages, by `kind` (ping/subscribe/unsubscribe/error).
    pub ws_inbound: IntCounterVec,
    /// Clients disconnected for exceeding the inbound rate limit.
    pub ws_rate_limited: IntCounter,
    /// Clients reaped for crossing the idle timeout.
    pub ws_idle_timeout: IntCounter,

    // --- Public WS input feeder (Hyperliquid backstop; off by default) ---
    /// Feeder health: 1 while the public WebSocket session is connected, 0 while down/reconnecting.
    pub ws_feeder_up: IntGauge,
    /// Public WS (re)connect cycles — incremented each time a session ends or a connect attempt fails
    /// and the feeder backs off to retry.
    pub ws_feeder_reconnects: IntCounter,
    /// Public WS frames that failed to decode (undecodable envelope; dropped best-effort).
    pub ws_feeder_decode_errors: IntCounter,
    /// Business messages decoded from the public WS and emitted through the arbiter, by `kind`
    /// (quote/trade).
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
                "DZ Edge multicast datagrams received per feed and port role",
                &["venue", "role"],
            ),
            datagram_bytes: counter_vec(
                &registry,
                "dz_datagram_bytes_total",
                "Total bytes received per feed",
                &["venue"],
            ),
            socket_errors: counter_vec(
                &registry,
                "dz_socket_errors_total",
                "Socket/transport receive errors per feed (each triggers a rejoin)",
                &["venue"],
            ),
            idle_rejoin: counter_vec(
                &registry,
                "dz_idle_rejoin_total",
                "Idle-rejoin watchdog firings per feed",
                &["venue"],
            ),
            feed_up: gauge_vec(
                &registry,
                "dz_feed_up",
                "Feed health: 1 if market data is up, 0 if down",
                &["venue"],
            ),
            feed_stale_ms: gauge_vec(
                &registry,
                "dz_feed_stale_ms",
                "Market-data staleness in ms: 0 while up; staleness at the last down transition",
                &["venue"],
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
                "Trades dropped by the windowed dedup",
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
            ws_feeder_up: gauge(
                &registry,
                "dz_ws_feeder_up",
                "Public WS input feeder health: 1 while connected, 0 while down/reconnecting",
            ),
            ws_feeder_reconnects: counter(
                &registry,
                "dz_ws_feeder_reconnects_total",
                "Public WS (re)connect cycles (session ended or connect attempt failed)",
            ),
            ws_feeder_decode_errors: counter(
                &registry,
                "dz_ws_feeder_decode_errors_total",
                "Public WS frames that failed to decode (dropped best-effort)",
            ),
            ws_feeder_messages: counter_vec(
                &registry,
                "dz_ws_feeder_messages_total",
                "Business messages decoded from the public WS and emitted, by kind",
                &["kind"],
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
            .with_label_values(&["Hyperliquid", "mktdata"])
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
            "dz_shred_wins_total",
            "dz_shred_lead_ns",
        ] {
            assert!(out.contains(name), "expected `{name}` in metrics output");
        }
    }
}
