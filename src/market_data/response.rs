//! What a market-data response says about itself: the quote's freshness, the `Cache-Control` the
//! CDN obeys, the RFC 9457 problem catalog and the origin's fixed response headers.
//!
//! Every header value here is a `&'static str`, so no feed or request text can reach a header.

use serde_json::Value;

/// The CDN may serve a fresh body this many seconds past its TTL. A constant, not a setting: a
/// setting could stretch how long a quote outlives its feed.
pub const STALE_WINDOW_S: u32 = 5;
/// How long the CDN keeps an answer that depends only on the cache key and the snapshot.
pub const NEGATIVE_TTL_S: u32 = 60;

/// How current a quote is, in precedence order: a halt outranks a dead receiver, which outranks a
/// quote the channel has not confirmed since it was last live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Live,
    Stale,
    FeedDown,
    Halted,
}

impl Freshness {
    /// `confirmed` is the channel's own verdict that it is live and the quote postdates its last
    /// gap (or, with no quote, that the channel is live). It is taken as given so the channel
    /// state stays the one place freshness is timed.
    pub fn of(halted: bool, receiver_up: bool, confirmed: bool) -> Self {
        if halted {
            Freshness::Halted
        } else if !receiver_up {
            Freshness::FeedDown
        } else if !confirmed {
            Freshness::Stale
        } else {
            Freshness::Live
        }
    }
}

/// What the CDN may do with a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CachePolicy {
    /// A live quote: one second, plus the stale window.
    Fresh,
    /// A quote that is not live: one second and never past it, since nothing confirms the body.
    Stale,
    /// An answer fixed by the cache key and the snapshot alone.
    Negative,
    /// Anything else, including any response that names no policy.
    #[default]
    NoStore,
}

impl From<Freshness> for CachePolicy {
    fn from(f: Freshness) -> Self {
        match f {
            Freshness::Live => CachePolicy::Fresh,
            Freshness::Stale | Freshness::FeedDown | Freshness::Halted => CachePolicy::Stale,
        }
    }
}

/// The `Cache-Control` value for a policy. `s-maxage` binds only the CDN; `max-age=0` makes a
/// browser revalidate, since without it a browser may heuristically reuse a `404` or `410`.
pub fn cache_control(p: CachePolicy) -> &'static str {
    match p {
        CachePolicy::Fresh => {
            "public, max-age=0, s-maxage=1, stale-while-revalidate=5, stale-if-error=5"
        }
        CachePolicy::Stale => {
            "public, max-age=0, s-maxage=1, stale-while-revalidate=0, stale-if-error=0"
        }
        CachePolicy::Negative => "public, max-age=0, s-maxage=60",
        CachePolicy::NoStore => "no-store",
    }
}

/// Every problem this server can answer with. The slug is the problem's `code` and its anchor on
/// the docs page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemKind {
    UnknownParameter,
    RepeatedParameter,
    InvalidParameter,
    InstrumentRequired,
    InvalidVenue,
    UnknownVenue,
    UnknownInstrument,
    AmbiguousInstrument,
    Retired,
    NotWarm,
    NotReady,
    Overloaded,
    UnknownRoute,
    MethodNotAllowed,
    RequestTimeout,
    BodyNotAllowed,
    InternalError,
}

impl ProblemKind {
    pub const ALL: [ProblemKind; 17] = [
        ProblemKind::UnknownParameter,
        ProblemKind::RepeatedParameter,
        ProblemKind::InvalidParameter,
        ProblemKind::InstrumentRequired,
        ProblemKind::InvalidVenue,
        ProblemKind::UnknownVenue,
        ProblemKind::UnknownInstrument,
        ProblemKind::AmbiguousInstrument,
        ProblemKind::Retired,
        ProblemKind::NotWarm,
        ProblemKind::NotReady,
        ProblemKind::Overloaded,
        ProblemKind::UnknownRoute,
        ProblemKind::MethodNotAllowed,
        ProblemKind::RequestTimeout,
        ProblemKind::BodyNotAllowed,
        ProblemKind::InternalError,
    ];

    pub fn slug(self) -> &'static str {
        match self {
            ProblemKind::UnknownParameter => "unknown-parameter",
            ProblemKind::RepeatedParameter => "repeated-parameter",
            ProblemKind::InvalidParameter => "invalid-parameter",
            ProblemKind::InstrumentRequired => "instrument-required",
            ProblemKind::InvalidVenue => "invalid-venue",
            ProblemKind::UnknownVenue => "unknown-venue",
            ProblemKind::UnknownInstrument => "unknown-instrument",
            ProblemKind::AmbiguousInstrument => "ambiguous-instrument",
            ProblemKind::Retired => "retired",
            ProblemKind::NotWarm => "not-warm",
            ProblemKind::NotReady => "not-ready",
            ProblemKind::Overloaded => "overloaded",
            ProblemKind::UnknownRoute => "unknown-route",
            ProblemKind::MethodNotAllowed => "method-not-allowed",
            ProblemKind::RequestTimeout => "request-timeout",
            ProblemKind::BodyNotAllowed => "body-not-allowed",
            ProblemKind::InternalError => "internal-error",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            ProblemKind::UnknownParameter => "Unknown parameter",
            ProblemKind::RepeatedParameter => "Repeated parameter",
            ProblemKind::InvalidParameter => "Invalid parameter",
            ProblemKind::InstrumentRequired => "Instrument required",
            ProblemKind::InvalidVenue => "Invalid venue",
            ProblemKind::UnknownVenue => "Unknown venue",
            ProblemKind::UnknownInstrument => "Unknown instrument",
            ProblemKind::AmbiguousInstrument => "Ambiguous instrument",
            ProblemKind::Retired => "Feed retired",
            ProblemKind::NotWarm => "Feed not ready",
            ProblemKind::NotReady => "Service not ready",
            ProblemKind::Overloaded => "Too many requests",
            ProblemKind::UnknownRoute => "Unknown route",
            ProblemKind::MethodNotAllowed => "Method not allowed",
            ProblemKind::RequestTimeout => "Request timeout",
            ProblemKind::BodyNotAllowed => "Request body not allowed",
            ProblemKind::InternalError => "Internal error",
        }
    }

    pub fn status(self) -> u16 {
        match self {
            ProblemKind::UnknownParameter
            | ProblemKind::RepeatedParameter
            | ProblemKind::InvalidParameter
            | ProblemKind::InstrumentRequired
            | ProblemKind::InvalidVenue
            | ProblemKind::BodyNotAllowed => 400,
            ProblemKind::UnknownVenue
            | ProblemKind::UnknownInstrument
            | ProblemKind::UnknownRoute => 404,
            ProblemKind::MethodNotAllowed => 405,
            ProblemKind::RequestTimeout => 408,
            ProblemKind::AmbiguousInstrument => 409,
            ProblemKind::Retired => 410,
            ProblemKind::Overloaded => 429,
            ProblemKind::InternalError => 500,
            ProblemKind::NotWarm | ProblemKind::NotReady => 503,
        }
    }

    /// The HTTP reason phrase, which RFC 9457 §4.2.1 makes the `title` of an `about:blank` problem.
    pub fn status_phrase(self) -> &'static str {
        match self {
            ProblemKind::UnknownParameter
            | ProblemKind::RepeatedParameter
            | ProblemKind::InvalidParameter
            | ProblemKind::InstrumentRequired
            | ProblemKind::InvalidVenue
            | ProblemKind::BodyNotAllowed => "Bad Request",
            ProblemKind::UnknownVenue
            | ProblemKind::UnknownInstrument
            | ProblemKind::UnknownRoute => "Not Found",
            ProblemKind::MethodNotAllowed => "Method Not Allowed",
            ProblemKind::RequestTimeout => "Request Timeout",
            ProblemKind::AmbiguousInstrument => "Conflict",
            ProblemKind::Retired => "Gone",
            ProblemKind::Overloaded => "Too Many Requests",
            ProblemKind::InternalError => "Internal Server Error",
            ProblemKind::NotWarm | ProblemKind::NotReady => "Service Unavailable",
        }
    }

    /// Only an answer fixed by the cache key and the snapshot is cached. A `400` never is: one can
    /// come from a request part outside the key, and cached under the shared key it would take that
    /// quote offline for every caller. A `404` from the server itself (no such route) is not a
    /// lookup answer either.
    pub fn cache_policy(self) -> CachePolicy {
        match self {
            ProblemKind::UnknownVenue
            | ProblemKind::UnknownInstrument
            | ProblemKind::AmbiguousInstrument
            | ProblemKind::Retired => CachePolicy::Negative,
            ProblemKind::UnknownParameter
            | ProblemKind::RepeatedParameter
            | ProblemKind::InvalidParameter
            | ProblemKind::InstrumentRequired
            | ProblemKind::InvalidVenue
            | ProblemKind::NotWarm
            | ProblemKind::NotReady
            | ProblemKind::Overloaded
            | ProblemKind::UnknownRoute
            | ProblemKind::MethodNotAllowed
            | ProblemKind::RequestTimeout
            | ProblemKind::BodyNotAllowed
            | ProblemKind::InternalError => CachePolicy::NoStore,
        }
    }
}

const BASE: [(&str, &str); 3] = [
    ("X-Content-Type-Options", "nosniff"),
    ("Vary", "Accept-Encoding"),
    ("Access-Control-Allow-Origin", "*"),
];
const NOINDEX: (&str, &str) = ("X-Robots-Tag", "noindex");
const ALLOW: (&str, &str) = ("Allow", "GET, HEAD");

static INDEXABLE: [(&str, &str); 3] = BASE;
static NOT_INDEXABLE: [(&str, &str); 4] = [BASE[0], BASE[1], BASE[2], NOINDEX];
static NOT_ALLOWED: [(&str, &str); 5] = [BASE[0], BASE[1], BASE[2], NOINDEX, ALLOW];

/// The headers every response carries. Only a `200` from `/`, `/openapi.json` or `/llms.txt` may
/// be indexed, so a new route is `noindex` until it is added here.
pub fn common_headers(path: &str, status: u16) -> &'static [(&'static str, &'static str)] {
    if status == 405 {
        &NOT_ALLOWED
    } else if status == 200 && matches!(path, "/" | "/openapi.json" | "/llms.txt") {
        &INDEXABLE
    } else {
        &NOT_INDEXABLE
    }
}

/// An RFC 9457 problem. `detail` is one factual sentence that names no symbol; symbols travel only
/// in structured extensions. The server that builds problems carries the test for that rule.
#[derive(Debug, Clone)]
pub struct Problem {
    pub kind: ProblemKind,
    pub detail: String,
    pub extensions: Vec<(&'static str, Value)>,
}

impl Problem {
    /// Members in the order `type`, `title`, `status`, `detail`, `code`, then the extensions.
    /// With no docs URL, `type` is `about:blank` and `title` the status phrase (RFC 9457 §4.2.1);
    /// `code` carries the slug either way.
    pub fn to_json(&self, docs_url: &str) -> Vec<u8> {
        let (ty, title) = if docs_url.is_empty() {
            ("about:blank".to_owned(), self.kind.status_phrase())
        } else {
            (
                format!("{docs_url}#{}", self.kind.slug()),
                self.kind.title(),
            )
        };
        // The crate's `serde_json` map is unordered, so the members are written by hand.
        let mut out = Vec::with_capacity(256);
        let mut member = |key: &str, value: &Value| {
            out.push(if out.is_empty() { b'{' } else { b',' });
            serde_json::to_writer(&mut out, key).expect("a string serializes");
            out.push(b':');
            serde_json::to_writer(&mut out, value).expect("a JSON value serializes");
        };
        member("type", &Value::from(ty));
        member("title", &Value::from(title));
        member("status", &Value::from(self.kind.status()));
        member("detail", &Value::from(self.detail.as_str()));
        member("code", &Value::from(self.kind.slug()));
        for (i, (key, value)) in self.extensions.iter().enumerate() {
            // Checked in release builds too: parsers disagree on which copy of a repeated member wins.
            assert!(
                !["type", "title", "status", "detail", "code"].contains(key)
                    && !self.extensions[..i].iter().any(|(k, _)| k == key),
                "extension {key} would repeat a member"
            );
            member(key, value);
        }
        out.push(b'}');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn live_when_confirmed() {
        assert_eq!(Freshness::of(false, true, true), Freshness::Live);
    }

    #[test]
    fn stale_when_not_confirmed() {
        assert_eq!(Freshness::of(false, true, false), Freshness::Stale);
    }

    #[test]
    fn halted_wins_over_feed_down_and_stale() {
        for (up, confirmed) in [(true, true), (true, false), (false, true), (false, false)] {
            assert_eq!(Freshness::of(true, up, confirmed), Freshness::Halted);
        }
    }

    #[test]
    fn feed_down_wins_over_stale() {
        assert_eq!(Freshness::of(false, false, false), Freshness::FeedDown);
        assert_eq!(Freshness::of(false, false, true), Freshness::FeedDown);
    }

    #[test]
    fn live_is_fresh_and_the_rest_are_stale() {
        assert_eq!(CachePolicy::from(Freshness::Live), CachePolicy::Fresh);
        for f in [Freshness::Stale, Freshness::FeedDown, Freshness::Halted] {
            assert_eq!(CachePolicy::from(f), CachePolicy::Stale);
        }
    }

    #[test]
    fn every_problem_kind_has_a_unique_slug() {
        let mut slugs: Vec<_> = ProblemKind::ALL.iter().map(|k| k.slug()).collect();
        slugs.sort_unstable();
        slugs.dedup();
        assert_eq!(slugs.len(), ProblemKind::ALL.len());
    }

    /// Fails to compile when a variant is added, as a reminder to add it to `ALL` too.
    #[test]
    fn all_lists_every_kind() {
        let index = |k: ProblemKind| match k {
            ProblemKind::UnknownParameter => 0,
            ProblemKind::RepeatedParameter => 1,
            ProblemKind::InvalidParameter => 2,
            ProblemKind::InstrumentRequired => 3,
            ProblemKind::InvalidVenue => 4,
            ProblemKind::UnknownVenue => 5,
            ProblemKind::UnknownInstrument => 6,
            ProblemKind::AmbiguousInstrument => 7,
            ProblemKind::Retired => 8,
            ProblemKind::NotWarm => 9,
            ProblemKind::NotReady => 10,
            ProblemKind::Overloaded => 11,
            ProblemKind::UnknownRoute => 12,
            ProblemKind::MethodNotAllowed => 13,
            ProblemKind::RequestTimeout => 14,
            ProblemKind::BodyNotAllowed => 15,
            ProblemKind::InternalError => 16,
        };
        for (i, k) in ProblemKind::ALL.into_iter().enumerate() {
            assert_eq!(index(k), i);
        }
    }

    #[test]
    fn status_phrase_matches_the_status() {
        for k in ProblemKind::ALL {
            let want = match k.status() {
                400 => "Bad Request",
                404 => "Not Found",
                405 => "Method Not Allowed",
                408 => "Request Timeout",
                409 => "Conflict",
                410 => "Gone",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                503 => "Service Unavailable",
                other => panic!("no phrase for {other}"),
            };
            assert_eq!(k.status_phrase(), want, "{}", k.slug());
        }
    }

    /// Pins the slugs already published for the quote endpoints, which clients and the docs page
    /// anchor on, against a rename. Nothing else in the crate uses them yet.
    #[test]
    fn published_endpoint_slugs_are_unchanged() {
        let native = [
            "unknown-parameter",
            "repeated-parameter",
            "invalid-parameter",
            "instrument-required",
            "invalid-venue",
            "unknown-venue",
            "unknown-instrument",
            "ambiguous-instrument",
            "retired",
            "not-warm",
            "not-ready",
        ];
        let slugs: Vec<_> = ProblemKind::ALL.iter().map(|k| k.slug()).collect();
        for slug in native {
            assert!(slugs.contains(&slug), "{slug} missing");
        }
    }

    #[test]
    fn fresh_cache_control() {
        assert_eq!(
            cache_control(CachePolicy::Fresh),
            "public, max-age=0, s-maxage=1, stale-while-revalidate=5, stale-if-error=5"
        );
    }

    #[test]
    fn stale_cache_control() {
        assert_eq!(
            cache_control(CachePolicy::Stale),
            "public, max-age=0, s-maxage=1, stale-while-revalidate=0, stale-if-error=0"
        );
    }

    #[test]
    fn negative_cache_control() {
        assert_eq!(
            cache_control(CachePolicy::Negative),
            "public, max-age=0, s-maxage=60"
        );
    }

    #[test]
    fn every_public_policy_makes_browsers_revalidate() {
        for p in [
            CachePolicy::Fresh,
            CachePolicy::Stale,
            CachePolicy::Negative,
        ] {
            assert!(cache_control(p).contains("max-age=0,"), "{p:?}");
        }
    }

    #[test]
    fn no_store_cache_control() {
        assert_eq!(cache_control(CachePolicy::NoStore), "no-store");
    }

    #[test]
    fn cache_control_follows_the_constants() {
        let fresh = cache_control(CachePolicy::Fresh);
        assert!(fresh.contains(&format!("stale-while-revalidate={STALE_WINDOW_S}")));
        assert!(fresh.contains(&format!("stale-if-error={STALE_WINDOW_S}")));
        assert!(
            cache_control(CachePolicy::Negative).ends_with(&format!("s-maxage={NEGATIVE_TTL_S}"))
        );
    }

    #[test]
    fn a_response_with_no_policy_is_not_stored() {
        assert_eq!(CachePolicy::default(), CachePolicy::NoStore);
    }

    #[test]
    fn every_400_and_server_error_is_no_store() {
        for k in ProblemKind::ALL {
            if matches!(k.status(), 400 | 405 | 408 | 429 | 500 | 503) {
                assert_eq!(k.cache_policy(), CachePolicy::NoStore, "{}", k.slug());
            }
        }
    }

    #[test]
    fn only_lookup_answers_are_negative() {
        for k in ProblemKind::ALL {
            let negative = matches!(
                k,
                ProblemKind::UnknownVenue
                    | ProblemKind::UnknownInstrument
                    | ProblemKind::AmbiguousInstrument
                    | ProblemKind::Retired
            );
            assert_eq!(
                k.cache_policy() == CachePolicy::Negative,
                negative,
                "{}",
                k.slug()
            );
        }
        assert_eq!(
            ProblemKind::UnknownRoute.cache_policy(),
            CachePolicy::NoStore
        );
        assert_eq!(
            ProblemKind::AmbiguousInstrument.cache_policy(),
            CachePolicy::Negative
        );
    }

    fn names(h: &[(&str, &str)]) -> Vec<String> {
        h.iter().map(|(n, _)| n.to_string()).collect()
    }

    fn has_the_three(h: &[(&str, &str)]) {
        assert!(h.contains(&("X-Content-Type-Options", "nosniff")));
        assert!(h.contains(&("Vary", "Accept-Encoding")));
        assert!(h.contains(&("Access-Control-Allow-Origin", "*")));
    }

    const CASES: [(&str, u16, bool); 7] = [
        ("/", 200, false),
        ("/openapi.json", 200, false),
        ("/llms.txt", 200, false),
        ("/", 404, true),
        ("/robots.txt", 200, true),
        ("/v1/quote", 200, true),
        ("/v1/quote", 405, true),
    ];

    #[test]
    fn noindex_everywhere_but_the_three_indexable_200s() {
        for (path, status, noindex) in CASES {
            let h = common_headers(path, status);
            assert_eq!(h.contains(&NOINDEX), noindex, "{path} {status}");
        }
    }

    #[test]
    fn common_headers_always_has_the_three() {
        for (path, status, _) in CASES {
            let h = common_headers(path, status);
            has_the_three(h);
            let mut n = names(h);
            n.sort();
            n.dedup();
            assert_eq!(n.len(), h.len(), "a header repeats on {path} {status}");
        }
    }

    #[test]
    fn allow_only_on_405() {
        for (path, status, _) in CASES {
            assert_eq!(common_headers(path, status).contains(&ALLOW), status == 405);
        }
    }

    fn problem(kind: ProblemKind) -> Problem {
        Problem {
            kind,
            detail: "No instrument with that symbol on hyperliquid.".into(),
            extensions: vec![("hints", json!([])), ("parameter", json!("venue"))],
        }
    }

    fn parse(p: &Problem, docs: &str) -> Value {
        serde_json::from_slice(&p.to_json(docs)).expect("valid JSON")
    }

    #[test]
    fn problem_type_links_the_docs_heading() {
        let v = parse(
            &problem(ProblemKind::UnknownInstrument),
            "https://docs.example/md",
        );
        assert_eq!(v["type"], "https://docs.example/md#unknown-instrument");
        assert_eq!(v["title"], "Unknown instrument");
    }

    #[test]
    fn about_blank_uses_the_status_phrase_and_keeps_the_code() {
        let v = parse(&problem(ProblemKind::NotReady), "");
        assert_eq!(v["type"], "about:blank");
        assert_eq!(v["title"], "Service Unavailable");
        assert_eq!(v["code"], "not-ready");
    }

    #[test]
    fn code_is_always_the_slug() {
        for k in ProblemKind::ALL {
            for docs in ["", "https://docs.example/md"] {
                assert_eq!(parse(&problem(k), docs)["code"], k.slug());
            }
        }
    }

    #[test]
    fn problem_member_order_is_type_title_status_detail_code_then_extensions() {
        let json = String::from_utf8(problem(ProblemKind::UnknownInstrument).to_json("")).unwrap();
        let at = |key: &str| json.find(&format!("\"{key}\":")).expect(key);
        let order = [
            "type",
            "title",
            "status",
            "detail",
            "code",
            "hints",
            "parameter",
        ]
        .map(at);
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{json}");
    }

    #[test]
    fn problem_status_matches_the_kind() {
        for k in ProblemKind::ALL {
            assert_eq!(parse(&problem(k), "")["status"], k.status());
        }
    }

    #[test]
    fn detail_is_escaped_not_spliced() {
        let mut p = problem(ProblemKind::InternalError);
        p.detail = "a \"quoted\"\r\nline".into();
        assert_eq!(parse(&p, "")["detail"], "a \"quoted\"\r\nline");
    }

    #[test]
    #[should_panic(expected = "would repeat a member")]
    fn repeated_extension_key_panics() {
        Problem {
            kind: ProblemKind::ALL[0],
            detail: String::new(),
            extensions: vec![("hints", json!([])), ("hints", json!([]))],
        }
        .to_json("");
    }

    #[test]
    #[should_panic(expected = "would repeat a member")]
    fn extension_repeating_a_core_member_panics() {
        Problem {
            kind: ProblemKind::ALL[0],
            detail: String::new(),
            extensions: vec![("code", json!("x"))],
        }
        .to_json("");
    }
}
