//! Host multicast-subscription detection — the single place all feed activation is derived from.
//!
//! `doublezero status --json` reports exactly the multicast groups **this host** is subscribed to,
//! in its `multicast_groups` field: a comma-separated list of `ROLE:code` entries where `S:` is a
//! subscriber and `P:` a publisher, e.g.
//! `"S:edge-solana-root,S:edge-solana-shreds,S:tiredsolid,S:scottsdale"`. That is the source of
//! truth the reconciler (`crate::ingest::reconcile`) uses to decide which market-data receivers,
//! shred sources, and the WebSocket sink to run.
//!
//! The network-wide `doublezero multicast group list` (what the shred forwarder discovered from
//! before) does **not** reflect per-host subscription, so it can't gate activation — here it's used
//! only to map subscribed group *codes* to their multicast *IPs*, and only for the shred groups
//! (the market-data groups already carry their IP in the `FEEDS` registry).
//!
//! Detection is a sync `std::process::Command` shell-out (soft-fail, mirroring
//! `crate::shred::discovery`); the async reconciler invokes it via `spawn_blocking`.

use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddrV4},
};

use serde::Deserialize;
use tracing::warn;

use crate::{ingest::feeds::Feed, shred::discovery::parse_group_code_ips};

/// Outcome of one detection attempt. The reconciler treats the three cases differently: `Ok` is
/// authoritative (reconcile to it, even when empty), `CliMissing` means **fail open** (no DZ CLI on
/// this host — run the static always-on set), and `Unavailable` is a transient hiccup (skip the
/// tick, keep the current activations rather than flapping everything off).
#[derive(Debug)]
pub enum Detected {
    Ok(HostSubs),
    /// The `doublezero` binary isn't installed/spawnable — e.g. running the bridge from source.
    CliMissing,
    /// The CLI is present but the query failed (non-zero exit, unparseable output).
    Unavailable,
}

/// The host's current multicast subscriptions plus the code→IP map for groups outside the
/// market-data `FEEDS` registry (the shred groups).
#[derive(Debug, Default, Clone)]
pub struct HostSubs {
    /// Group codes this host subscribes to (the `S:` entries of `doublezero status`).
    pub subscribed_codes: HashSet<String>,
    /// code → multicast IP, from `doublezero multicast group list` (activated rows only).
    pub code_ip: HashMap<String, Ipv4Addr>,
}

/// One entry of `doublezero status --json`. Only `multicast_groups` is read; every other field
/// (`response`, `current_device`, …) is ignored.
///
/// Modeled as `Option<Option<String>>` so the **key being absent entirely** (the outer
/// `Option::None`, supplied by `#[serde(default)]`) can never collapse into the **same value** as
/// the key being present with an explicit JSON `null` (`Some(None)`). That distinction is
/// load-bearing: a session with no multicast groups (e.g. IBRL) reports the key present, as `null`
/// or `""` — a real zero-subscription signal — while the key being missing from the object
/// altogether is a shape this process does not recognize (an upstream rename/removal of the
/// field), and must not be read as "zero subscriptions" (see `parse_status_codes`'s F1 note).
///
/// ⚠️ Naive `Option<Option<T>>` with a bare `#[serde(default)]` does **not** achieve this: serde's
/// `Option<T>` deserializer intercepts a JSON `null` at the *outer* layer and short-circuits
/// straight to the outer `None` (`visit_none`) without ever consulting the inner `Option<String>`
/// — so `null` and "key absent" collapse to the identical outer `None` regardless of nesting depth
/// (a well-known serde gotcha, see serde-rs/serde#984). `deserialize_with = "present_and_null_ok"`
/// is what actually separates them: it only runs at all when the key is present (`#[serde(default)]`
/// supplies the outer `None` before the deserializer is ever invoked otherwise), and it explicitly
/// wraps whatever `Option<String>` the value deserializes to (`null` → `None`, a string → `Some`) in
/// an outer `Some(..)` — so "present" is encoded by the act of the deserializer having run, not by
/// what the value happened to be.
#[derive(Debug, Deserialize)]
struct StatusEntry {
    #[serde(default, deserialize_with = "present_and_null_ok")]
    multicast_groups: Option<Option<String>>,
}

/// Deserialize a present field's value as `T` and wrap it in `Some` unconditionally — the seam that
/// makes "the key was present at all" a fact recorded independently of the value itself (`null`
/// included). Only ever invoked when the key exists, via `#[serde(default, deserialize_with = ...)]`
/// on [`StatusEntry::multicast_groups`] above.
fn present_and_null_ok<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

/// Parse the subscribed group **codes** from `doublezero status --json`. Each `multicast_groups`
/// token is `ROLE:code`; we keep subscriber (`S:`) entries (the bridge only ever receives), strip
/// the role prefix, and tolerate a bare `code` with no prefix.
///
/// Returns `None` when the top-level JSON does not even parse as the expected array-of-entries
/// shape — an upstream CLI output-format change, not a legitimate host state. That distinction
/// matters to the caller (`detect`): a format change must read as `Detected::Unavailable` (fail
/// open, keep the current activations — the same promise fail-open already makes everywhere else),
/// never as "this host is subscribed to nothing," which would tear down every market-data receiver
/// plus (since the channel-departure purge landed) the catalog, book and trade history for every
/// channel this process runs.
///
/// `Some(codes)` — `codes` possibly empty — is the honest, structurally-parsed answer: an IBRL
/// session, or any host truly holding no multicast subscriptions, is a *real* zero-subscription
/// state and must proceed as such, not be conflated with a parse failure.
///
/// **F1:** the likeliest real-world way this parse "fails" is not malformed JSON at all — it is an
/// upstream rename of `multicast_groups` itself, which `serde_json::from_slice` shrugs off
/// entirely (the field is optional) and would, with a plain `Option<String>`, quietly land on
/// `None` → `Some(vec![])` here: a field rename reading as "this host subscribes to nothing,"
/// fleet-wide, the instant the CLI ships it — silently discarding every channel's catalog, books
/// and history via the reconciler's departure purge. An entry whose `multicast_groups` key is
/// missing entirely is therefore treated exactly like a top-level parse failure: `None`, not an
/// empty list.
pub fn parse_status_codes(stdout: &[u8]) -> Option<Vec<String>> {
    let entries: Vec<StatusEntry> = match serde_json::from_slice(stdout) {
        Ok(e) => e,
        Err(e) => {
            warn!(%e, "could not parse `doublezero status --json`; reporting Unavailable (not zero subscriptions)");
            return None;
        }
    };
    let mut out = Vec::new();
    for entry in entries {
        // The outer `None` is the key-absent case (F1): report the whole read as unparseable
        // rather than silently treating a missing/renamed field as zero subscriptions.
        let Some(groups_present) = entry.multicast_groups else {
            warn!(
                "doublezero status --json entry has no multicast_groups key at all; reporting \
                 Unavailable (not zero subscriptions) — a renamed/removed field, not a legitimate \
                 empty session"
            );
            return None;
        };
        // The inner `None` is the key present as explicit JSON `null` — a genuine "no groups"
        // signal once the key exists at all (e.g. an IBRL session).
        let Some(groups) = groups_present else {
            continue;
        };
        for tok in groups.split(',') {
            let tok = tok.trim();
            if tok.is_empty() {
                continue;
            }
            let code = match tok.split_once(':') {
                // subscriber entry -> the code after the role prefix
                Some((role, code)) if role.eq_ignore_ascii_case("s") => code,
                // publisher (or any other role) -> not a receive subscription; skip
                Some(_) => continue,
                // no role prefix (older CLI) -> treat the whole token as a subscribed code
                None => tok,
            };
            if !code.is_empty() {
                out.push(code.to_string());
            }
        }
    }
    Some(out)
}

/// Run `doublezero status --json` (always) and `doublezero multicast group list --json-compact`
/// (only when `need_group_ips`, i.e. shred sources aren't explicitly overridden). See [`Detected`]
/// for how the three outcomes are classified.
pub fn detect(need_group_ips: bool) -> Detected {
    let status = match run_cli(&["status", "--json"]) {
        CliOut::Ok(bytes) => bytes,
        CliOut::Missing => return Detected::CliMissing,
        CliOut::Err => return Detected::Unavailable,
    };
    // `None` here means the JSON didn't even parse as the expected shape (a format change), which
    // is transient/`Unavailable` by the same fail-open rule as every other soft-fail in this
    // module — never read as "this host subscribes to nothing" (see `parse_status_codes`'s doc).
    let Some(codes) = parse_status_codes(&status) else {
        return Detected::Unavailable;
    };
    let subscribed_codes: HashSet<String> = codes.into_iter().collect();

    // The group list is only needed to resolve shred-group IPs (market-data IPs come from FEEDS).
    // A failure here doesn't invalidate the status-based market-data/WS gating, so it degrades to an
    // empty map (shred sources just won't resolve this tick) rather than an `Unavailable`.
    let code_ip = if need_group_ips {
        match run_cli(&["multicast", "group", "list", "--json-compact"]) {
            CliOut::Ok(bytes) => parse_group_code_ips(&bytes).into_iter().collect(),
            _ => HashMap::new(),
        }
    } else {
        HashMap::new()
    };

    Detected::Ok(HostSubs {
        subscribed_codes,
        code_ip,
    })
}

impl HostSubs {
    /// The subset of `enabled` feeds whose group `code` this host is subscribed to.
    pub fn market_data_feeds<'a>(&self, enabled: &'a [Feed]) -> Vec<&'a Feed> {
        enabled
            .iter()
            .filter(|f| self.subscribed_codes.contains(f.code))
            .collect()
    }

    /// Subscribed group codes matching `prefix` (the shred groups, `edge-solana-`), resolved to
    /// `ip:port` via the group-list map. Sorted for deterministic diffing. A subscribed group with
    /// no known IP is warned about and skipped.
    pub fn shred_sources(&self, prefix: &str, port: u16) -> Vec<SocketAddrV4> {
        let mut out: Vec<SocketAddrV4> = self
            .subscribed_codes
            .iter()
            .filter(|c| c.starts_with(prefix))
            .filter_map(|code| match self.code_ip.get(code) {
                Some(ip) => Some(SocketAddrV4::new(*ip, port)),
                None => {
                    warn!(%code, "subscribed shred group has no multicast IP in `multicast group list`; skipping");
                    None
                }
            })
            .collect();
        out.sort();
        out
    }
}

/// Result of a single `doublezero` shell-out, distinguishing "binary absent" (fail open) from a
/// runtime error (transient) so [`detect`] can classify the outcome.
enum CliOut {
    Ok(Vec<u8>),
    Err,
    Missing,
}

fn run_cli(args: &[&str]) -> CliOut {
    match std::process::Command::new("doublezero").args(args).output() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => CliOut::Missing,
        Err(e) => {
            warn!(?args, %e, "could not run `doublezero`");
            CliOut::Err
        }
        Ok(o) if !o.status.success() => {
            warn!(
                ?args,
                status = %o.status,
                stderr = %String::from_utf8_lossy(&o.stderr).trim(),
                "`doublezero` exited non-zero"
            );
            CliOut::Err
        }
        Ok(o) => CliOut::Ok(o.stdout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::feeds::feeds;

    // A real `doublezero status --json` capture from a host subscribed to both shred and
    // market-data groups (the field that matters is `multicast_groups`).
    const STATUS_JSON: &str = r#"[
      {
        "response": {
          "doublezero_status": {"session_status": "BGP Session Up", "last_session_update": 1782920453},
          "tunnel_name": "doublezero1",
          "user_type": "Multicast"
        },
        "reconciler_enabled": true,
        "current_device": "tyo002-dz002",
        "network": "mainnet-beta",
        "multicast_groups": "S:edge-solana-root,S:edge-solana-retrans-apac,S:edge-solana-shreds,S:tiredsolid,S:scottsdale"
      }
    ]"#;

    fn codes(json: &str) -> HashSet<String> {
        parse_status_codes(json.as_bytes())
            .expect("must parse for this fixture")
            .into_iter()
            .collect()
    }

    #[test]
    fn parses_subscribed_codes_from_real_status() {
        let got = codes(STATUS_JSON);
        assert_eq!(
            got,
            [
                "edge-solana-root",
                "edge-solana-retrans-apac",
                "edge-solana-shreds",
                "tiredsolid",
                "scottsdale",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn keeps_subscriber_skips_publisher() {
        let json = r#"[{"multicast_groups":"S:tiredsolid,P:mine,s:scottsdale"}]"#;
        // Both S: (case-insensitive) kept, P: dropped.
        assert_eq!(
            codes(json),
            codes(r#"[{"multicast_groups":"S:tiredsolid,S:scottsdale"}]"#)
        );
    }

    #[test]
    fn tolerates_bare_codes_and_whitespace() {
        let json = r#"[{"multicast_groups":" tiredsolid , S:scottsdale ,"}]"#;
        assert_eq!(
            codes(json),
            codes(r#"[{"multicast_groups":"S:tiredsolid,S:scottsdale"}]"#)
        );
    }

    /// A well-formed response reporting no multicast groups at all — the key **present** as an
    /// explicit `null` or empty string (an IBRL session reports the key, it just has nothing in
    /// it) — is a **real** zero-subscription state: `Some(vec![])`, never `None`. Distinct from
    /// both `a_missing_multicast_groups_key_reports_none_not_zero_subscriptions` (the key present
    /// vs. absent — F1) and `unparseable_json_reports_none_not_an_empty_list` (malformed JSON)
    /// below on purpose — a test covering only one of these cannot tell a `None`-vs-`Some(empty)`
    /// regression apart, which is exactly the bug this group of tests exists to catch.
    #[test]
    fn a_well_formed_response_with_no_groups_reports_some_empty_not_none() {
        assert_eq!(
            parse_status_codes(br#"[{"response":{"user_type":"IBRL"},"multicast_groups":null}]"#),
            Some(Vec::new()),
            "the key present as null is a legitimate empty session"
        );
        assert_eq!(
            parse_status_codes(br#"[{"multicast_groups":""}]"#),
            Some(Vec::new())
        );
        assert_eq!(
            parse_status_codes(br#"[{"multicast_groups":null}]"#),
            Some(Vec::new())
        );
        assert_eq!(
            parse_status_codes(b"[]"),
            Some(Vec::new()),
            "an empty array of entries is also a legitimate zero-subscription response"
        );
    }

    /// The other branch: the JSON does not even parse as the expected array-of-entries shape — an
    /// upstream CLI output-format change, not a legitimate host state. Must report `None` so
    /// `detect` returns `Detected::Unavailable` (fail open, keep current activations) rather than
    /// reading the format change as "this host subscribes to nothing" and, since the
    /// channel-departure purge landed, tearing down the catalog/book/history for every channel this
    /// process runs.
    #[test]
    fn unparseable_json_reports_none_not_an_empty_list() {
        assert_eq!(parse_status_codes(b""), None);
        assert_eq!(parse_status_codes(b"not json"), None);
        // A JSON object, not an array — the same "wrong shape" case, just a different flavor of it.
        assert_eq!(parse_status_codes(b"{\"multicast_groups\":\"S:x\"}"), None);
    }

    /// F1, the finding this round exists for: an entry whose JSON *object parses fine* but whose
    /// `multicast_groups` **key is entirely absent** — modeling an upstream field rename or removal
    /// — must report `None`, exactly like `unparseable_json_reports_none_not_an_empty_list` above,
    /// and must NOT be conflated with `a_well_formed_response_with_no_groups_reports_some_empty_not_none`,
    /// whose fixtures all carry the key (as `null` or `""`). A fixture that only ever omits the key
    /// entirely could not tell "the field was renamed" apart from "the field legitimately holds
    /// nothing" — these two tests, one per case, are what make that distinction real.
    #[test]
    fn a_missing_multicast_groups_key_reports_none_not_zero_subscriptions() {
        // Well-formed JSON, valid entry object, but the key itself is not present at all.
        assert_eq!(
            parse_status_codes(br#"[{"response":{"user_type":"IBRL"}}]"#),
            None,
            "an entry with no multicast_groups key at all must not silently read as zero \
             subscriptions"
        );
        assert_eq!(
            parse_status_codes(br#"[{"current_device":"tyo002-dz002"}]"#),
            None
        );
        // The specific real-world shape this guards against: the field renamed rather than absent
        // outright — its replacement present under a different name changes nothing, since the
        // *old* name is still the one this process reads.
        assert_eq!(
            parse_status_codes(br#"[{"multicastGroups":"S:tiredsolid"}]"#),
            None,
            "a renamed multicast_groups field must not silently parse as zero subscriptions"
        );
    }

    fn subs(codes: &[&str], code_ip: &[(&str, Ipv4Addr)]) -> HostSubs {
        HostSubs {
            subscribed_codes: codes.iter().map(|s| s.to_string()).collect(),
            code_ip: code_ip.iter().map(|(c, ip)| (c.to_string(), *ip)).collect(),
        }
    }

    #[test]
    fn market_data_feeds_match_by_code() {
        let enabled: &[Feed] = feeds();

        // Subscribed to Hyperliquid's group only -> both HL rows (TOB + MBO), not Phoenix.
        let hl = subs(&["tiredsolid", "edge-solana-shreds"], &[]);
        let got = hl.market_data_feeds(enabled);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|f| f.venue == "HYPERLIQUID"));

        // Subscribed to Phoenix only.
        let px = subs(&["scottsdale"], &[]);
        let got = px.market_data_feeds(enabled);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].venue, "PHOENIX");

        // Shreds-only host -> no market-data feeds.
        let shreds_only = subs(&["edge-solana-shreds", "edge-solana-root"], &[]);
        assert!(shreds_only.market_data_feeds(enabled).is_empty());
    }

    #[test]
    fn shred_sources_resolve_subscribed_prefix_to_ips() {
        let s = subs(
            &["edge-solana-shreds", "edge-solana-root", "tiredsolid"],
            &[
                ("edge-solana-shreds", Ipv4Addr::new(233, 84, 178, 1)),
                ("edge-solana-root", Ipv4Addr::new(233, 84, 178, 5)),
                ("tiredsolid", Ipv4Addr::new(233, 84, 178, 15)),
            ],
        );
        let got = s.shred_sources("edge-solana-", 7733);
        assert_eq!(
            got,
            vec![
                SocketAddrV4::new(Ipv4Addr::new(233, 84, 178, 1), 7733),
                SocketAddrV4::new(Ipv4Addr::new(233, 84, 178, 5), 7733),
            ]
        );
    }

    #[test]
    fn shred_source_without_ip_is_skipped() {
        // Subscribed to a shred group whose IP the group list didn't provide -> skipped, not panicked.
        let s = subs(&["edge-solana-shreds"], &[]);
        assert!(s.shred_sources("edge-solana-", 7733).is_empty());
    }
}
