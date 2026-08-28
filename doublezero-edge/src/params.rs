//! `key==value` query-parameter parsing — the emulated tool's convention for passing query-string
//! parameters as trailing positional arguments rather than as `--flag value` pairs.
//!
//! Every trailing argument on a `products <action>` command is one of two things: a bare
//! positional (the product id) or a `key==value` query parameter. This module's job is telling
//! those apart *before* anything downstream treats an argument as positional, so a parameter never
//! gets mistaken for an id (or vice versa).

/// One trailing argument, classified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arg {
    /// A `key==value` query parameter. `value` may itself contain `=` (or even further `==`) —
    /// only the *first* `==` is the delimiter, so anything after it is taken verbatim.
    Param(String, String),
    /// Anything else — most commonly the product id.
    Positional(String),
}

/// Classify one raw argument. A `==` at position 0 (an empty key, e.g. `"==foo"`) is not treated as
/// a parameter — a key names *something*, so an empty one is nonsensical as a query key and the
/// argument is passed through as positional instead of being silently swallowed.
pub fn classify(raw: &str) -> Arg {
    match raw.find("==") {
        Some(0) | None => Arg::Positional(raw.to_string()),
        Some(idx) => {
            let key = &raw[..idx];
            let value = &raw[idx + 2..];
            Arg::Param(key.to_string(), value.to_string())
        }
    }
}

/// Split a full trailing-argument list into query parameters (wire order preserved) and
/// positionals (wire order preserved). This is the one place callers should reach for — most just
/// want "the params" and "the first positional (if any)".
pub fn split(args: &[String]) -> (Vec<(String, String)>, Vec<String>) {
    let mut params = Vec::new();
    let mut positionals = Vec::new();
    for raw in args {
        match classify(raw) {
            Arg::Param(k, v) => params.push((k, v)),
            Arg::Positional(p) => positionals.push(p),
        }
    }
    (params, positionals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_value_pair_splits_on_the_first_double_equals() {
        assert_eq!(
            classify("granularity==ONE_MINUTE"),
            Arg::Param("granularity".to_string(), "ONE_MINUTE".to_string())
        );
    }

    /// The defect this exists to prevent: a value containing `=` (e.g. a base64 blob, or a filter
    /// expression) must survive whole rather than being truncated at the first `=` inside it.
    #[test]
    fn a_value_containing_equals_survives_whole() {
        assert_eq!(
            classify("filter==a=b=c"),
            Arg::Param("filter".to_string(), "a=b=c".to_string())
        );
    }

    /// A value that itself contains a literal `==` must not be re-split — only the first `==`
    /// delimits key from value.
    #[test]
    fn a_value_containing_a_double_equals_is_not_re_split() {
        assert_eq!(
            classify("expr==a==b"),
            Arg::Param("expr".to_string(), "a==b".to_string())
        );
    }

    #[test]
    fn a_plain_token_is_positional() {
        assert_eq!(
            classify("HYPERLIQUID:BTC"),
            Arg::Positional("HYPERLIQUID:BTC".to_string())
        );
    }

    /// A leading `==` has no key before it, so it is not treated as a parameter.
    #[test]
    fn a_leading_double_equals_has_no_key_and_is_positional() {
        assert_eq!(classify("==oops"), Arg::Positional("==oops".to_string()));
    }

    #[test]
    fn split_separates_params_from_positionals_preserving_order() {
        let args = vec![
            "HYPERLIQUID:BTC".to_string(),
            "granularity==ONE_MINUTE".to_string(),
            "limit==60".to_string(),
        ];
        let (params, positionals) = split(&args);
        assert_eq!(
            params,
            vec![
                ("granularity".to_string(), "ONE_MINUTE".to_string()),
                ("limit".to_string(), "60".to_string()),
            ]
        );
        assert_eq!(positionals, vec!["HYPERLIQUID:BTC".to_string()]);
    }

    #[test]
    fn split_handles_a_param_before_the_positional() {
        let args = vec!["limit==10".to_string(), "HYPERLIQUID:BTC".to_string()];
        let (params, positionals) = split(&args);
        assert_eq!(params, vec![("limit".to_string(), "10".to_string())]);
        assert_eq!(positionals, vec!["HYPERLIQUID:BTC".to_string()]);
    }
}
