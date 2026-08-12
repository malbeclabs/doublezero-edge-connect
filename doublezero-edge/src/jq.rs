//! A minimal `jq`-subset extractor for `--jq <filter>` — not a `jq` reimplementation, just the
//! handful of path operations an agent actually reaches for against these response shapes:
//! `.field`, `.array[N]`, `.array[]` (iterate/flatten), chained (`.trades[0].price`,
//! `.products[].product_id`). No filters, pipes, or arithmetic — if that's ever needed, shelling
//! out to the real `jq` on the already-emitted JSON is the answer, not growing this module into one.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Field(String),
    Index(usize),
    Iterate,
}

/// Parse a filter string like `.trades[0].price` into a step sequence. `.` alone (or an empty
/// string) is the identity filter — zero steps.
fn parse(filter: &str) -> Result<Vec<Step>, String> {
    let filter = filter.trim();
    if filter.is_empty() || filter == "." {
        return Ok(Vec::new());
    }
    let bytes = filter.as_bytes();
    if bytes[0] != b'.' {
        return Err(format!("filter must start with '.': \"{filter}\""));
    }
    let mut steps = Vec::new();
    let mut i = 1usize;
    while i < bytes.len() {
        // An identifier segment: alnum/underscore/hyphen, stopping at '.' or '['.
        let start = i;
        while i < bytes.len() && bytes[i] != b'.' && bytes[i] != b'[' {
            i += 1;
        }
        if i > start {
            steps.push(Step::Field(filter[start..i].to_string()));
        }
        // Zero or more `[...]` suffixes on this segment.
        while i < bytes.len() && bytes[i] == b'[' {
            i += 1; // consume '['
            let digit_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] != b']' {
                return Err(format!("unterminated '[' in filter: \"{filter}\""));
            }
            if i == digit_start {
                steps.push(Step::Iterate);
            } else {
                let n: usize = filter[digit_start..i]
                    .parse()
                    .map_err(|_| format!("bad index in filter: \"{filter}\""))?;
                steps.push(Step::Index(n));
            }
            i += 1; // consume ']'
        }
        if i < bytes.len() {
            if bytes[i] == b'.' {
                i += 1;
                continue;
            }
            return Err(format!(
                "unexpected character at byte {i} in filter: \"{filter}\""
            ));
        }
    }
    Ok(steps)
}

/// Apply `filter` to `root`, returning the stream of matched values in order. `Iterate` (`[]`)
/// expands one value into each of its array elements / object values; every other step maps
/// one-to-one, substituting `Value::Null` for a miss (indexing past the end, a missing field, or
/// indexing into the wrong shape) rather than erroring — same tolerance-over-rejection default the
/// rest of this crate uses for version skew.
pub fn extract(root: &Value, filter: &str) -> Result<Vec<Value>, String> {
    let steps = parse(filter)?;
    let mut current = vec![root.clone()];
    for step in steps {
        current = match step {
            Step::Field(name) => current
                .into_iter()
                .map(|v| v.get(&name).cloned().unwrap_or(Value::Null))
                .collect(),
            Step::Index(idx) => current
                .into_iter()
                .map(|v| v.get(idx).cloned().unwrap_or(Value::Null))
                .collect(),
            Step::Iterate => current
                .into_iter()
                .flat_map(|v| match v {
                    Value::Array(a) => a,
                    Value::Object(o) => o.into_values().collect(),
                    _ => Vec::new(),
                })
                .collect(),
        };
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc() -> Value {
        json!({
            "products": [
                {"product_id": "HYPERLIQUID:BTC", "status": "online"},
                {"product_id": "HYPERLIQUID:ETH", "status": "offline"}
            ],
            "trades": [
                {"price": "67000.12", "size": "0.5"},
                {"price": "66999.00", "size": "1.0"}
            ],
            "pricebook": {"bids": [["100.0", "1.0"]], "asks": []}
        })
    }

    #[test]
    fn identity_returns_the_whole_document() {
        assert_eq!(extract(&doc(), ".").unwrap(), vec![doc()]);
        assert_eq!(extract(&doc(), "").unwrap(), vec![doc()]);
    }

    #[test]
    fn a_bare_field_extracts_its_value() {
        assert_eq!(
            extract(&doc(), ".pricebook").unwrap(),
            vec![doc()["pricebook"].clone()]
        );
    }

    #[test]
    fn an_index_selects_one_array_element() {
        let got = extract(&doc(), ".trades[0].price").unwrap();
        assert_eq!(got, vec![json!("67000.12")]);
    }

    #[test]
    fn an_iterate_then_field_streams_one_value_per_element() {
        let got = extract(&doc(), ".products[].product_id").unwrap();
        assert_eq!(
            got,
            vec![json!("HYPERLIQUID:BTC"), json!("HYPERLIQUID:ETH")]
        );
    }

    #[test]
    fn a_missing_field_yields_null_rather_than_erroring() {
        assert_eq!(extract(&doc(), ".nonexistent").unwrap(), vec![Value::Null]);
    }

    #[test]
    fn an_out_of_range_index_yields_null() {
        assert_eq!(
            extract(&doc(), ".trades[99].price").unwrap(),
            vec![Value::Null]
        );
    }

    #[test]
    fn a_filter_not_starting_with_dot_is_rejected() {
        assert!(extract(&doc(), "trades[0]").is_err());
    }
}
