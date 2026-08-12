//! What this process knows about *why* it is serving no data — the answer to "the container is
//! running, `docker ps` is green, and `doublezero-edge` still says nothing is there."
//!
//! The `/v1` query API is subscription-gated: on a host whose tunnel never came up it is not
//! listening at all, so every CLI command fails with a transport error and the operator has no way
//! to tell "edge-connect isn't running" from "edge-connect is running and correctly serving
//! nothing." The admin surface ([`crate::sinks::admin`]) is the one HTTP surface that is *not*
//! subscription-gated, which makes it the right place to answer that question — and this module is
//! the state it answers from.
//!
//! Two halves:
//! - [`DiagnosticsSnapshot`] — plain cached state, written by the reconciler at the end of every
//!   tick from what it already fetched ([`DiagnosticsSnapshot::publish_tick`]). **Nothing here
//!   polls, and nothing here shells out on the read path** — a diagnostics request is a lock, a
//!   clone and a pure function.
//! - [`diagnose`] — an ordered ladder from that state to one `{code, summary, remediation}`
//!   verdict. Server-side on purpose: an agent reading `.diagnosis.code` and an operator reading
//!   the table then get the same answer, and the ladder is unit-testable against a captured
//!   `doublezero status` document with no container in the loop.
//!
//! The ladder never claims more than the snapshot knows. Every rung above `ok` is reached only
//! when the rung above it was ruled out, so "tunnel down" cannot be reported on a host that simply
//! has no `doublezero` CLI (running from source), and "no traffic" cannot be reported on a host
//! that has not finished its first poll.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::ingest::{
    feeds::FeedKind,
    health::TapeLiveness,
    subscriptions::{Session, HEALTHY_SESSION_STATUS},
};

/// Written by the reconciler once per tick, read by the admin surface. This process's only writer
/// is [`DiagnosticsSnapshot::publish_tick`]; every HTTP path over it is read-only.
pub type SharedDiagnostics = Arc<Mutex<DiagnosticsSnapshot>>;

/// What the last completed subscription poll produced. Mirrors
/// [`crate::ingest::subscriptions::Detected`] plus the two states that exist before or instead of a
/// real poll.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Detection {
    /// No tick has completed yet — the process just started.
    #[default]
    Pending,
    /// `doublezero status --json` answered and parsed.
    Ok,
    /// No `doublezero` binary on this host (running from source). Gating falls open.
    CliMissing,
    /// The CLI is present but the query failed; [`Polled::detail`] carries what it printed.
    Unavailable,
    /// `--subscription-gating-disable`: the static always-on model, no poll at all.
    GatingDisabled,
}

impl Detection {
    fn label(self) -> &'static str {
        match self {
            Detection::Pending => "pending",
            Detection::Ok => "ok",
            Detection::CliMissing => "cli_missing",
            Detection::Unavailable => "unavailable",
            Detection::GatingDisabled => "gating_disabled",
        }
    }
}

/// One running receiver, as the reconciler sees it: its identity (the `FeedKey` tuple) and its real
/// liveness off [`crate::ingest::health::FeedHealth`] — not "the channel filter admits it", which
/// is what `GET /admin/channels` reports and deliberately does not conflate with this.
#[derive(Debug, Clone)]
pub struct ReceiverState {
    pub venue: &'static str,
    pub category: &'static str,
    pub kind: FeedKind,
    pub base_port: u16,
    pub liveness: TapeLiveness,
}

/// What one poll read off `doublezero status --json`. Built by the reconciler, which keeps this as
/// one value rather than six positional arguments.
#[derive(Debug, Default, Clone)]
pub struct Polled {
    pub detection: Detection,
    /// Why detection failed, where the CLI said so (`Detection::Unavailable` only).
    pub detail: Option<String>,
    pub sessions: Vec<Session>,
    /// Subscribed codes that match a feed row this process may run.
    pub market_data_codes: Vec<String>,
    /// Subscribed `edge-solana-*` codes.
    pub shred_codes: Vec<String>,
    /// Every other subscribed code — a group this host holds that this build has no row for.
    pub other_codes: Vec<String>,
}

/// What the reconciler is actually running as of that tick — read off its own task maps, not off
/// the desired set that produced them, so an inconclusive tick reports reality.
#[derive(Debug, Default)]
pub struct Activation {
    pub receivers: Vec<ReceiverState>,
    pub ws_on: bool,
    pub api_on: bool,
    pub shred_sources: Vec<String>,
}

/// Everything the diagnostics route reports, written by the reconciler once per tick.
#[derive(Debug, Default)]
pub struct DiagnosticsSnapshot {
    pub polled: Polled,
    /// Wall clock of the last completed tick; `None` before the first.
    pub checked_at_unix: Option<u64>,
    /// Wall clock of the last tick whose detection actually *succeeded* — the age of `polled`'s
    /// session and code data, which a failed tick deliberately leaves in place (see
    /// [`Self::publish_tick`]).
    pub last_ok_at_unix: Option<u64>,
    pub activation: Activation,
    /// How often the reconciler polls, so a remediation can name the real wait rather than a
    /// hardcoded 30s.
    pub refresh_secs: u64,
}

impl DiagnosticsSnapshot {
    /// Publish one tick.
    ///
    /// An `Unavailable` tick updates the detection outcome but **keeps the previous poll's sessions
    /// and codes**. The transient case is a `doublezero status` blip on a host that is streaming
    /// fine, and blanking them would report "zero subscriptions, no sessions, freshly checked"
    /// beside five live receivers, which is a worse answer than a stale one. `last_ok_at_unix` is
    /// what makes the staleness visible; the reconciler makes the same distinction for activations,
    /// which it also keeps.
    ///
    /// Only a `Detection::Ok` tick stamps `last_ok_at_unix`: it dates the session and code data
    /// above, and `CliMissing`/`GatingDisabled` never ran a status call to produce any. Stamping
    /// those would date an empty document to now and read as freshly-confirmed.
    pub fn publish_tick(&mut self, polled: Polled, activation: Activation) {
        let now = crate::model::now_ns() / 1_000_000_000;
        self.checked_at_unix = Some(now);
        if polled.detection == Detection::Unavailable {
            self.polled.detection = polled.detection;
            self.polled.detail = polled.detail;
        } else {
            if polled.detection == Detection::Ok {
                self.last_ok_at_unix = Some(now);
            }
            self.polled = polled;
        }
        self.activation = activation;
    }

    /// The `tunnel`, `subscriptions` and `activation` blocks of the diagnostics response.
    pub fn to_json(&self) -> Value {
        let sessions: Vec<Value> = self
            .polled
            .sessions
            .iter()
            .map(|s| {
                json!({
                    "session_status": s.session_status,
                    "tunnel_name": s.tunnel_name,
                    "user_type": s.user_type,
                    "current_device": s.current_device,
                    "lowest_latency_device": s.lowest_latency_device,
                    "metro": s.metro,
                    "network": s.network,
                    "reconciler_enabled": s.reconciler_enabled,
                    "multicast_groups": s.multicast_groups,
                    "subscriptions": s.subscriptions.iter().map(|r| json!({
                        "code": r.code,
                        "multicast_ip": r.multicast_ip,
                        "publisher": r.publisher,
                        "subscriber": r.subscriber,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        let receivers: Vec<Value> = self
            .activation
            .receivers
            .iter()
            .map(|r| {
                json!({
                    "venue": r.venue,
                    "category": r.category,
                    "kind": r.kind.label(),
                    "publisher": r.base_port,
                    "liveness": liveness_label(r.liveness),
                })
            })
            .collect();
        json!({
            "tunnel": {
                "detection": self.polled.detection.label(),
                "detail": self.polled.detail,
                "checked_at_unix": self.checked_at_unix,
                "last_ok_at_unix": self.last_ok_at_unix,
                "poll_seconds": self.refresh_secs,
                "sessions": sessions,
            },
            "subscriptions": {
                "market_data_codes": self.polled.market_data_codes,
                "shred_codes": self.polled.shred_codes,
                "other_codes": self.polled.other_codes,
            },
            "activation": {
                "receivers": receivers,
                "ws_on": self.activation.ws_on,
                "api_on": self.activation.api_on,
                "shred_sources": self.activation.shred_sources,
            },
        })
    }
}

fn liveness_label(l: TapeLiveness) -> &'static str {
    match l {
        TapeLiveness::Up => "up",
        TapeLiveness::Unregistered => "unregistered",
        TapeLiveness::Down => "down",
    }
}

/// One verdict: a stable machine-readable `code`, what is true, and what to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnosis {
    pub code: &'static str,
    pub summary: String,
    pub remediation: String,
}

impl Diagnosis {
    fn new(code: &'static str, summary: impl Into<String>, remediation: impl Into<String>) -> Self {
        Self {
            code,
            summary: summary.into(),
            remediation: remediation.into(),
        }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code,
            "summary": self.summary,
            "remediation": self.remediation,
        })
    }
}

/// The ladder. Ordered most-fundamental first, and every rung is reached only once the one above it
/// is ruled out — which is what stops it reporting a broken tunnel on a host that never had one to
/// break (`cli_missing`), or an empty subscription set on a host whose daemon never answered.
pub fn diagnose(s: &DiagnosticsSnapshot) -> Diagnosis {
    match s.polled.detection {
        Detection::Pending => {
            return Diagnosis::new(
                "pending",
                "No subscription poll has completed yet; this process just started.",
                format!(
                    "Wait up to {}s for the first reconciler tick, then run \
                     `doublezero-edge diagnose` again.",
                    s.refresh_secs
                ),
            )
        }
        Detection::CliMissing => {
            return Diagnosis::new(
                "dz_cli_missing",
                "No `doublezero` CLI on this host, so tunnel and subscription state cannot be \
                 read. Subscription gating falls open: every selected feed runs.",
                "Expected when running edge-connect from source rather than in its container. \
                 Inside the container this means the DoubleZero client is missing from the image.",
            )
        }
        Detection::Unavailable => {
            let detail = s
                .polled
                .detail
                .as_deref()
                .unwrap_or("no output was captured");
            return Diagnosis::new(
                "daemon_unreachable",
                format!("`doublezero status` failed: {detail}"),
                "The DoubleZero daemon (doublezerod) is most likely not running — every client \
                 verb checks it first. Current activations are being kept unchanged until it \
                 answers again.",
            );
        }
        Detection::GatingDisabled => {
            // Nothing to say about the tunnel: gating is off, so the tunnel was never consulted.
            // Fall through to the traffic rung, which is still meaningful.
            return traffic_verdict(
                s,
                "gating_disabled",
                "Subscription gating is disabled (--subscription-gating-disable); every selected \
                 feed runs regardless of what this host is subscribed to.",
            );
        }
        Detection::Ok => {}
    }

    // Packets arriving is proof the tunnel is up, whatever the status string says — so the two
    // rungs below are skipped outright when a receiver is delivering. Without that, one upstream
    // rename of `session_status` would report `tunnel_down` on every healthy host in the fleet and
    // send each one to reconnect a tunnel that was never down. Activation is armored against
    // exactly that rename (see `subscriptions::parse_status_codes`'s F1 note); the verdict an
    // operator acts on has to be too.
    let delivering = s
        .activation
        .receivers
        .iter()
        .any(|r| r.liveness == TapeLiveness::Up);
    if !delivering && !s.polled.sessions.iter().any(session_is_up) {
        let reported = s
            .polled
            .sessions
            .iter()
            .filter_map(|s| s.session_status.as_deref())
            .collect::<Vec<_>>()
            .join(", ");
        // No session reported a status at all: the document parsed but this build did not
        // recognize the field. "Not up" is a claim the snapshot cannot support, so it isn't made.
        if reported.is_empty() {
            return Diagnosis::new(
                "tunnel_state_unknown",
                format!(
                    "`doublezero status` answered but reported no session status for this host \
                     ({} session entr(ies)), and no receiver is delivering.",
                    s.polled.sessions.len()
                ),
                "Read it directly — `doublezero status` inside the container — since this build \
                 could not find the field it reads. If the tunnel really is down, \
                 `doublezero connect multicast` there is the retry.",
            );
        }
        return Diagnosis::new(
            "tunnel_down",
            format!(
                "The DoubleZero tunnel is not up — session status: {reported} (healthy is \
                 \"{HEALTHY_SESSION_STATUS}\"). No multicast traffic can reach this host."
            ),
            "Retry the tunnel with `doublezero connect multicast` inside the container. If it \
             fails, the usual causes are a missing access pass for this host's IP, or a provider \
             firewall/NAT blocking the tunnel.",
        );
    }

    if !delivering && s.polled.market_data_codes.is_empty() {
        return Diagnosis::new(
            "no_market_data_subscriptions",
            "The tunnel is up, but this host is subscribed to no multicast group this build \
             serves market data for, so no feed is activated.",
            "Subscribe to a market-data group (`doublezero connect multicast` requests the ones \
             this host has an access pass for), then wait one reconciler poll. \
             `doublezero-edge diagnose` lists the groups this host does hold.",
        );
    }

    traffic_verdict(
        s,
        "ok",
        "The tunnel is up and a market-data group is subscribed.",
    )
}

/// The last two rungs, shared by the `Ok` and `GatingDisabled` paths: receivers are meant to be
/// running — are any of them actually delivering? `prefix` is what the caller already established
/// about *why* those receivers are running.
fn traffic_verdict(s: &DiagnosticsSnapshot, ok_code: &'static str, prefix: &str) -> Diagnosis {
    let receivers = &s.activation.receivers;
    // Zero receivers is its own answer, never `ok`. Reaching here means the feeds were expected to
    // be running — a subscribed group, or gating off entirely — so an empty set is a selection that
    // excluded everything (`--feed`, `--publisher-port`, or a channel filter narrower than the
    // row's roster), not a healthy idle state. Its remediation is the opposite of the no-traffic
    // rung's: nothing bound a socket, so no firewall can be at fault.
    if receivers.is_empty() {
        return Diagnosis::new(
            "no_receivers_running",
            format!("{prefix} No receiver is running, so nothing is being decoded."),
            "Every publisher of the subscribed row is excluded by this process's own selection. \
             Check `--feed`/`--publisher-port` and the channel filter (`--channels`, or \
             `GET /admin/channels` for the one in force) against the groups this host holds.",
        );
    }
    if !receivers.iter().any(|r| r.liveness == TapeLiveness::Up) {
        return Diagnosis::new(
            "subscribed_no_traffic",
            format!(
                "{prefix} {} receiver(s) are running but none has delivered a packet.",
                receivers.len()
            ),
            "A default-deny host firewall dropping the decapsulated inner multicast is the usual \
             cause: allow it on the tunnel interface (e.g. `ufw allow in on doublezero1`). A \
             just-activated receiver can also read this way for one poll interval — re-check \
             before changing anything.",
        );
    }
    Diagnosis::new(
        ok_code,
        format!("{prefix} {} receiver(s) running.", receivers.len()),
        if s.activation.api_on {
            "Nothing to fix — the /v1 query API is up.".to_string()
        } else {
            "The /v1 query API is not up; check --api-bind is set and its port is free.".to_string()
        },
    )
}

/// A session counts as up only on the exact healthy literal — anything else, including a string
/// this build has never seen, is reported verbatim by the caller rather than guessed at.
fn session_is_up(s: &Session) -> bool {
    s.session_status.as_deref() == Some(HEALTHY_SESSION_STATUS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::subscriptions::{
        parse_status_sessions, DISCONNECTED_STATUS_JSON, STATUS_JSON,
    };

    fn snap(detection: Detection) -> DiagnosticsSnapshot {
        DiagnosticsSnapshot {
            polled: Polled {
                detection,
                ..Polled::default()
            },
            refresh_secs: 30,
            ..DiagnosticsSnapshot::default()
        }
    }

    fn receiver(liveness: TapeLiveness) -> ReceiverState {
        ReceiverState {
            venue: "HYPERLIQUID",
            category: "perps",
            kind: FeedKind::TopOfBook,
            base_port: 31000,
            liveness,
        }
    }

    /// The issue's own host: the container is up, the tunnel is not. Driven off the real capture
    /// rather than a hand-built struct, so a change to how the CLI reports "disconnected" breaks
    /// this test rather than silently degrading the verdict.
    #[test]
    fn the_disconnected_capture_diagnoses_as_tunnel_down() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(DISCONNECTED_STATUS_JSON.as_bytes());
        let d = diagnose(&s);
        assert_eq!(d.code, "tunnel_down");
        assert!(
            d.summary.contains("disconnected"),
            "the verdict must quote the status the CLI actually reported: {}",
            d.summary
        );
        assert!(
            d.remediation.contains("doublezero connect multicast"),
            "the remediation must name the retry verb: {}",
            d.remediation
        );
    }

    /// The mirror image: a healthy capture with a subscribed feed and a live receiver is `ok`.
    #[test]
    fn a_connected_host_with_a_live_receiver_diagnoses_as_ok() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.polled.market_data_codes = vec!["tiredsolid".to_string()];
        s.activation.receivers = vec![receiver(TapeLiveness::Up)];
        s.activation.api_on = true;
        assert_eq!(diagnose(&s).code, "ok");
    }

    /// Running from source is not a broken tunnel. Without the ladder's ordering this host — which
    /// reports no sessions at all, because nothing was ever polled — would land on `tunnel_down`
    /// and send a developer chasing a tunnel that was never meant to exist.
    #[test]
    fn a_cli_missing_host_never_reports_tunnel_down() {
        let d = diagnose(&snap(Detection::CliMissing));
        assert_eq!(d.code, "dz_cli_missing");
    }

    /// A daemon-down host reports the CLI's own bail message rather than a guess. That message is
    /// the entire answer and today reaches only the container log.
    #[test]
    fn a_daemon_down_host_quotes_the_cli_error() {
        let mut s = snap(Detection::Unavailable);
        s.polled.detail = Some("Please start the doublezerod service.".to_string());
        let d = diagnose(&s);
        assert_eq!(d.code, "daemon_unreachable");
        assert!(d.summary.contains("Please start the doublezerod service."));
    }

    /// Before the first tick nothing is known, and the verdict says exactly that instead of
    /// reporting the empty snapshot as a broken tunnel.
    #[test]
    fn a_process_that_has_not_polled_yet_reports_pending() {
        let d = diagnose(&snap(Detection::Pending));
        assert_eq!(d.code, "pending");
        assert!(d.remediation.contains("30s"), "{}", d.remediation);
    }

    /// Tunnel up, but nothing this build serves is subscribed — distinct from `tunnel_down`, and
    /// the fix is an access pass, not a reconnect.
    #[test]
    fn a_connected_host_with_no_market_data_group_reports_no_subscriptions() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.polled.shred_codes = vec!["edge-solana-shreds".to_string()];
        assert_eq!(diagnose(&s).code, "no_market_data_subscriptions");
    }

    /// The rung that would otherwise read as `ok`: receivers are running and none is delivering.
    /// This is the default-deny-firewall shape, and reporting it as healthy is what would send an
    /// operator looking at the publisher instead of their own host.
    #[test]
    fn receivers_running_with_no_traffic_is_not_ok() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.polled.market_data_codes = vec!["tiredsolid".to_string()];
        s.activation.receivers = vec![
            receiver(TapeLiveness::Down),
            receiver(TapeLiveness::Unregistered),
        ];
        let d = diagnose(&s);
        assert_eq!(d.code, "subscribed_no_traffic");
        assert!(d.remediation.contains("doublezero1"), "{}", d.remediation);
    }

    /// One live receiver beside a dead one is `ok`: a venue's peer publisher going quiet is not a
    /// host-level fault, and reporting it as one would cry wolf on every mirrored deployment.
    #[test]
    fn one_live_receiver_among_dead_peers_is_ok() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.polled.market_data_codes = vec!["tiredsolid".to_string()];
        s.activation.receivers = vec![receiver(TapeLiveness::Down), receiver(TapeLiveness::Up)];
        assert_eq!(diagnose(&s).code, "ok");
    }

    /// `--subscription-gating-disable` consulted no tunnel, so the verdict must not pretend to know
    /// one — but the traffic rung still applies and is still the useful answer.
    #[test]
    fn gating_disabled_skips_the_tunnel_rungs_but_keeps_the_traffic_one() {
        let mut s = snap(Detection::GatingDisabled);
        s.activation.receivers = vec![receiver(TapeLiveness::Down)];
        assert_eq!(diagnose(&s).code, "subscribed_no_traffic");

        let mut s = snap(Detection::GatingDisabled);
        s.activation.receivers = vec![receiver(TapeLiveness::Up)];
        assert_eq!(diagnose(&s).code, "gating_disabled");
    }

    /// A subscribed row whose every publisher this process excluded runs no receiver at all. The
    /// traffic rung's `!is_empty()` guard used to skip that case straight to `ok` — "nothing to
    /// fix" on a host serving nothing, which is the one answer this module exists to prevent.
    #[test]
    fn a_subscribed_row_with_every_publisher_excluded_is_not_ok() {
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.polled.market_data_codes = vec!["tiredsolid".to_string()];
        s.activation.api_on = true;
        assert!(s.activation.receivers.is_empty());
        let d = diagnose(&s);
        assert_eq!(d.code, "no_receivers_running");
        assert!(d.remediation.contains("--channels"), "{}", d.remediation);

        // Same shape with gating off: nothing selected, so nothing runs.
        let s = snap(Detection::GatingDisabled);
        assert_eq!(diagnose(&s).code, "no_receivers_running");
    }

    /// `last_ok_at_unix` dates the session and code data. `CliMissing` and `GatingDisabled` never
    /// run a status call, so stamping them would date an empty document to now and render as
    /// freshly-confirmed beside a verdict that consulted nothing.
    #[test]
    fn only_a_successful_poll_stamps_the_last_ok_time() {
        for detection in [Detection::CliMissing, Detection::GatingDisabled] {
            let mut s = snap(Detection::Pending);
            s.publish_tick(
                Polled {
                    detection,
                    ..Polled::default()
                },
                Activation::default(),
            );
            assert!(s.checked_at_unix.is_some(), "{detection:?} was checked");
            assert_eq!(
                s.last_ok_at_unix, None,
                "{detection:?} ran no status call to date"
            );
        }
    }

    /// A `doublezero status` blip on a streaming host must not blank what the last good poll knew.
    /// Reporting "zero subscriptions, no sessions, freshly checked" beside live receivers sends an
    /// operator after a fault that isn't there; `last_ok_at_unix` is what makes the staleness
    /// visible instead.
    #[test]
    fn an_unavailable_tick_keeps_the_last_good_sessions_and_codes() {
        let mut s = snap(Detection::Pending);
        s.publish_tick(
            Polled {
                detection: Detection::Ok,
                sessions: parse_status_sessions(STATUS_JSON.as_bytes()),
                market_data_codes: vec!["tiredsolid".to_string()],
                ..Polled::default()
            },
            Activation::default(),
        );
        let ok_at = s.last_ok_at_unix.expect("a successful tick stamps it");

        s.publish_tick(
            Polled {
                detection: Detection::Unavailable,
                detail: Some("Please start the doublezerod service.".to_string()),
                ..Polled::default()
            },
            Activation::default(),
        );
        assert_eq!(s.polled.detection, Detection::Unavailable);
        assert_eq!(s.polled.market_data_codes, vec!["tiredsolid".to_string()]);
        assert_eq!(s.polled.sessions.len(), 1);
        assert_eq!(
            s.last_ok_at_unix,
            Some(ok_at),
            "the staleness of the kept data must stay visible"
        );
    }

    /// M2: one upstream rename of `session_status` must not report `tunnel_down` fleet-wide.
    /// Packets arriving is proof the tunnel is up whatever the string says, and a document that
    /// reports no status at all is unknown rather than down. Activation is armored against exactly
    /// this rename; the verdict an operator acts on has to be too.
    #[test]
    fn an_unrecognized_session_status_never_reports_tunnel_down() {
        // Every field renamed away: sessions parse, none carries a status.
        let renamed = br#"[{"multicast_groups":"S:tiredsolid","doublezeroStatus":{"sessionStatus":"BGP Session Up"}}]"#;

        // With traffic flowing, the tunnel is demonstrably up — no tunnel rung at all.
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(renamed);
        s.polled.market_data_codes = vec!["tiredsolid".to_string()];
        s.activation.receivers = vec![receiver(TapeLiveness::Up)];
        assert_eq!(diagnose(&s).code, "ok");

        // With nothing delivering, "not up" is still a claim the snapshot cannot support.
        let mut s = snap(Detection::Ok);
        s.polled.sessions = parse_status_sessions(renamed);
        let d = diagnose(&s);
        assert_eq!(d.code, "tunnel_state_unknown");
        assert!(
            d.remediation.contains("doublezero status"),
            "{}",
            d.remediation
        );
    }
}
