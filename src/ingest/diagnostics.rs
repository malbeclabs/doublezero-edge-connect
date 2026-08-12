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
//! - [`DiagnosticsSnapshot`] — plain cached state. The reconciler publishes it at the end of every
//!   tick from what it already fetched ([`DiagnosticsSnapshot::publish_tick`]); the admin surface's
//!   connect/disconnect route records its attempt into the same struct. **Nothing here polls, and
//!   nothing here shells out on the read path** — a diagnostics request is a lock, a clone and a
//!   pure function.
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

/// Shared with the reconciler (which writes the tick half) and the admin surface (which reads it
/// and writes the attempt half).
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
    /// The CLI is present but the query failed. `detail` carries what it printed.
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

/// One `POST /admin/connect` or `POST /admin/disconnect` run. `finished_at_unix == None` means it
/// is still running, which is also what makes the endpoint single-flight.
#[derive(Debug, Clone)]
pub struct CommandAttempt {
    /// The exact argv that was run, for display — a constant in `sinks::admin`, never composed
    /// from request input.
    pub command: String,
    pub started_at_unix: u64,
    pub finished_at_unix: Option<u64>,
    pub exit_code: Option<i32>,
    pub output_tail: String,
}

impl CommandAttempt {
    pub fn running(&self) -> bool {
        self.finished_at_unix.is_none()
    }

    fn to_json(&self) -> Value {
        json!({
            "command": self.command,
            "started_at_unix": self.started_at_unix,
            "finished_at_unix": self.finished_at_unix,
            "running": self.running(),
            "exit_code": self.exit_code,
            "output_tail": self.output_tail,
        })
    }
}

/// Everything the diagnostics route reports, written by the reconciler once per tick.
#[derive(Debug, Default)]
pub struct DiagnosticsSnapshot {
    pub detection: Detection,
    /// Why detection failed, where the CLI said so (`Detection::Unavailable` only).
    pub detail: Option<String>,
    /// Wall clock of the last completed tick; `None` before the first.
    pub checked_at_unix: Option<u64>,
    pub sessions: Vec<Session>,
    /// Subscribed codes that match a feed row this process may run.
    pub market_data_codes: Vec<String>,
    /// Subscribed `edge-solana-*` codes.
    pub shred_codes: Vec<String>,
    /// Every other subscribed code — a group this host holds that this build has no row for.
    pub other_codes: Vec<String>,
    pub receivers: Vec<ReceiverState>,
    pub ws_on: bool,
    pub api_on: bool,
    pub shred_sources: Vec<String>,
    /// How often the reconciler polls, so a remediation can name the real wait rather than a
    /// hardcoded 30s.
    pub refresh_secs: u64,
    /// The last connect/disconnect attempt, if any. Deliberately **not** touched by
    /// [`Self::publish_tick`]: the two halves of this struct have different writers, and a tick
    /// landing mid-attempt must not erase it.
    pub last_attempt: Option<CommandAttempt>,
}

impl DiagnosticsSnapshot {
    /// Overwrite the polled half. Leaves `last_attempt` alone — see its doc.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_tick(
        &mut self,
        detection: Detection,
        detail: Option<String>,
        sessions: Vec<Session>,
        market_data_codes: Vec<String>,
        shred_codes: Vec<String>,
        other_codes: Vec<String>,
        receivers: Vec<ReceiverState>,
        ws_on: bool,
        api_on: bool,
        shred_sources: Vec<String>,
    ) {
        self.detection = detection;
        self.detail = detail;
        self.checked_at_unix = Some(crate::model::now_ns() / 1_000_000_000);
        self.sessions = sessions;
        self.market_data_codes = market_data_codes;
        self.shred_codes = shred_codes;
        self.other_codes = other_codes;
        self.receivers = receivers;
        self.ws_on = ws_on;
        self.api_on = api_on;
        self.shred_sources = shred_sources;
    }

    /// Whether a connect/disconnect attempt is in flight — the single-flight gate.
    pub fn attempt_running(&self) -> bool {
        self.last_attempt.as_ref().is_some_and(|a| a.running())
    }

    /// The `tunnel`, `subscriptions` and `activation` blocks of the diagnostics response.
    pub fn to_json(&self) -> Value {
        let sessions: Vec<Value> = self
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
                "detection": self.detection.label(),
                "detail": self.detail,
                "checked_at_unix": self.checked_at_unix,
                "poll_seconds": self.refresh_secs,
                "sessions": sessions,
            },
            "subscriptions": {
                "market_data_codes": self.market_data_codes,
                "shred_codes": self.shred_codes,
                "other_codes": self.other_codes,
            },
            "activation": {
                "receivers": receivers,
                "ws_on": self.ws_on,
                "api_on": self.api_on,
                "shred_sources": self.shred_sources,
            },
            "last_attempt": self.last_attempt.as_ref().map(CommandAttempt::to_json),
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
    match s.detection {
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
            let detail = s.detail.as_deref().unwrap_or("no output was captured");
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

    if !s.sessions.iter().any(session_is_up) {
        let reported = s
            .sessions
            .iter()
            .filter_map(|s| s.session_status.as_deref())
            .collect::<Vec<_>>()
            .join(", ");
        let reported = if reported.is_empty() {
            "no sessions at all".to_string()
        } else {
            format!("session status: {reported}")
        };
        return Diagnosis::new(
            "tunnel_down",
            format!(
                "The DoubleZero tunnel is not up — {reported} (healthy is \
                 \"{HEALTHY_SESSION_STATUS}\"). No multicast traffic can reach this host."
            ),
            "Run `doublezero-edge connect` to retry `doublezero connect multicast` inside the \
             container. If it fails, the usual causes are a missing access pass for this host's \
             IP, or a provider firewall/NAT blocking the tunnel.",
        );
    }

    if s.market_data_codes.is_empty() {
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
    if !s.receivers.is_empty() && !s.receivers.iter().any(|r| r.liveness == TapeLiveness::Up) {
        return Diagnosis::new(
            "subscribed_no_traffic",
            format!(
                "{prefix} {} receiver(s) are running but none has delivered a packet.",
                s.receivers.len()
            ),
            "A default-deny host firewall dropping the decapsulated inner multicast is the usual \
             cause: allow it on the tunnel interface (e.g. `ufw allow in on doublezero1`). A \
             just-activated receiver can also read this way for one poll interval — re-check \
             before changing anything.",
        );
    }
    Diagnosis::new(
        ok_code,
        format!("{prefix} {} receiver(s) running.", s.receivers.len()),
        if s.api_on {
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
            detection,
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
        s.sessions = parse_status_sessions(DISCONNECTED_STATUS_JSON.as_bytes());
        let d = diagnose(&s);
        assert_eq!(d.code, "tunnel_down");
        assert!(
            d.summary.contains("disconnected"),
            "the verdict must quote the status the CLI actually reported: {}",
            d.summary
        );
        assert!(
            d.remediation.contains("doublezero-edge connect"),
            "the remediation must name the retry this CLI offers: {}",
            d.remediation
        );
    }

    /// The mirror image: a healthy capture with a subscribed feed and a live receiver is `ok`.
    #[test]
    fn a_connected_host_with_a_live_receiver_diagnoses_as_ok() {
        let mut s = snap(Detection::Ok);
        s.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.market_data_codes = vec!["tiredsolid".to_string()];
        s.receivers = vec![receiver(TapeLiveness::Up)];
        s.api_on = true;
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
        s.detail = Some("Please start the doublezerod service.".to_string());
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
        s.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.shred_codes = vec!["edge-solana-shreds".to_string()];
        assert_eq!(diagnose(&s).code, "no_market_data_subscriptions");
    }

    /// The rung that would otherwise read as `ok`: receivers are running and none is delivering.
    /// This is the default-deny-firewall shape, and reporting it as healthy is what would send an
    /// operator looking at the publisher instead of their own host.
    #[test]
    fn receivers_running_with_no_traffic_is_not_ok() {
        let mut s = snap(Detection::Ok);
        s.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.market_data_codes = vec!["tiredsolid".to_string()];
        s.receivers = vec![
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
        s.sessions = parse_status_sessions(STATUS_JSON.as_bytes());
        s.market_data_codes = vec!["tiredsolid".to_string()];
        s.receivers = vec![receiver(TapeLiveness::Down), receiver(TapeLiveness::Up)];
        assert_eq!(diagnose(&s).code, "ok");
    }

    /// `--subscription-gating-disable` consulted no tunnel, so the verdict must not pretend to know
    /// one — but the traffic rung still applies and is still the useful answer.
    #[test]
    fn gating_disabled_skips_the_tunnel_rungs_but_keeps_the_traffic_one() {
        let mut s = snap(Detection::GatingDisabled);
        s.receivers = vec![receiver(TapeLiveness::Down)];
        assert_eq!(diagnose(&s).code, "subscribed_no_traffic");

        let mut s = snap(Detection::GatingDisabled);
        s.receivers = vec![receiver(TapeLiveness::Up)];
        assert_eq!(diagnose(&s).code, "gating_disabled");
    }

    /// A tick must never erase an attempt recorded by the admin surface — the two halves of the
    /// snapshot have different writers, and losing the attempt is how a `202` becomes unobservable.
    #[test]
    fn publishing_a_tick_preserves_a_recorded_attempt() {
        let mut s = snap(Detection::Pending);
        s.last_attempt = Some(CommandAttempt {
            command: "doublezero connect multicast".to_string(),
            started_at_unix: 1,
            finished_at_unix: None,
            exit_code: None,
            output_tail: String::new(),
        });
        s.publish_tick(
            Detection::Ok,
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
            false,
            false,
            vec![],
        );
        assert!(
            s.attempt_running(),
            "the in-flight attempt must survive a tick"
        );
    }
}
