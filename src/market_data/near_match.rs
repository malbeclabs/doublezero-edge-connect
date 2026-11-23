//! Near-match hints for an unknown instrument: the venue's symbols closest to what the caller sent.
//!
//! Bounded per request whatever the caller sends: three binary searches, at most
//! [`DISTANCE_SCAN_MAX`] names examined per prefix range, and an edit-distance scan only on a venue
//! of at most that many names.

/// A venue with more symbols than this gets prefix hints only.
pub const DISTANCE_SCAN_MAX: usize = 5_000;
/// The most hints one answer carries.
pub const MAX_HINTS: usize = 5;
/// The longest query that gets hints, the most bytes a symbol can have.
pub const MAX_QUERY: usize = 64;

/// The farthest edit distance that still counts as a near match.
const MAX_DISTANCE: usize = 2;

/// Up to [`MAX_HINTS`] symbols from `sorted` (the venue's symbols in byte order), best first: a
/// case-insensitive exact match, then names starting with the query, then names within edit
/// distance 2. Ties break by distance, then length, then bytes.
///
/// Symbols are ASCII, so a non-ASCII or over-long query cannot be near one and gets no hints;
/// refusing it up front also means nothing here ever slices inside a character.
///
/// Above [`DISTANCE_SCAN_MAX`] names only the three binary searches run, so a mixed-case symbol
/// (`xyz:TSLA`) is found from a query in neither its case nor all-upper or all-lower (`XYZ:tsla`)
/// only on a smaller venue.
pub fn near_matches<'a>(query: &str, sorted: &[&'a str]) -> Vec<&'a str> {
    if query.is_empty() || query.len() > MAX_QUERY || !query.is_ascii() {
        return Vec::new();
    }
    let q = query.as_bytes();
    // (tier, distance, length, symbol): tier 0 exact, 1 prefix, 2 distance.
    let mut found: Vec<(u8, usize, usize, &'a str)> = Vec::new();

    let variants = [
        query.to_owned(),
        query.to_ascii_uppercase(),
        query.to_ascii_lowercase(),
    ];
    for (i, prefix) in variants.iter().enumerate() {
        if variants[..i].contains(prefix) {
            continue;
        }
        let start = sorted.partition_point(|s| s.as_bytes() < prefix.as_bytes());
        for &s in sorted[start..]
            .iter()
            .take(DISTANCE_SCAN_MAX)
            .take_while(|s| s.starts_with(prefix.as_str()))
        {
            let tier = if s.eq_ignore_ascii_case(query) { 0 } else { 1 };
            found.push((tier, 0, s.len(), s));
        }
    }

    if sorted.len() <= DISTANCE_SCAN_MAX {
        let mut rows = [
            vec![0; MAX_QUERY + 1],
            vec![0; MAX_QUERY + 1],
            vec![0; MAX_QUERY + 1],
        ];
        for &s in sorted {
            if let Some(d) = distance_within(q, s.as_bytes(), &mut rows) {
                let tier = if d == 0 { 0 } else { 2 };
                found.push((tier, d, s.len(), s));
            }
        }
    }

    found.sort_unstable();
    // A name can be found more than once (by two case variants, or as a prefix and a near match);
    // the sort puts its best entry first.
    let mut hints: Vec<&'a str> = Vec::with_capacity(MAX_HINTS);
    for (_, _, _, s) in found {
        if !hints.contains(&s) {
            hints.push(s);
            if hints.len() == MAX_HINTS {
                break;
            }
        }
    }
    hints
}

/// Case-insensitive optimal-string-alignment distance between `q` (at most [`MAX_QUERY`] bytes)
/// and `s`, or `None` once it must exceed [`MAX_DISTANCE`]. Rows are indexed by `q`'s bytes, so
/// their length is bounded by the query, not by the symbol.
fn distance_within(q: &[u8], s: &[u8], rows: &mut [Vec<usize>; 3]) -> Option<usize> {
    if q.len().abs_diff(s.len()) > MAX_DISTANCE {
        return None;
    }
    let lower = |b: u8| b.to_ascii_lowercase();
    let n = q.len();
    let [two_back, prev, cur] = rows;
    for (j, v) in prev.iter_mut().enumerate().take(n + 1) {
        *v = j;
    }
    for i in 1..=s.len() {
        cur[0] = i;
        let mut row_min = i;
        for j in 1..=n {
            let cost = usize::from(lower(s[i - 1]) != lower(q[j - 1]));
            let mut d = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            if i > 1
                && j > 1
                && lower(s[i - 1]) == lower(q[j - 2])
                && lower(s[i - 2]) == lower(q[j - 1])
            {
                d = d.min(two_back[j - 2] + 1);
            }
            cur[j] = d;
            row_min = row_min.min(d);
        }
        // No later row can drop below this one's minimum, transpositions included.
        if row_min > MAX_DISTANCE {
            return None;
        }
        std::mem::swap(two_back, prev);
        std::mem::swap(prev, cur);
    }
    Some(prev[n]).filter(|&d| d <= MAX_DISTANCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn venue(names: &[&'static str]) -> Vec<&'static str> {
        let mut v = names.to_vec();
        v.sort_unstable();
        v
    }

    #[test]
    fn exact_case_insensitive_match_comes_first() {
        let v = venue(&["BTCUSD", "BTC", "ETH", "BT"]);
        assert_eq!(near_matches("btc", &v)[0], "BTC");
    }

    #[test]
    fn exact_case_insensitive_match_on_mixed_case_symbol() {
        let v = venue(&["xyz:TSLA", "xyz:TSLB"]);
        assert_eq!(near_matches("XYZ:tsla", &v)[0], "xyz:TSLA");
    }

    #[test]
    fn prefix_matches_in_each_case() {
        let v = venue(&["BTC", "BTCUSD", "ETH", "xyz:TSLA"]);
        for q in ["BT", "bt"] {
            let h = near_matches(q, &v);
            assert_eq!(&h[..2], &["BTC", "BTCUSD"], "{q}");
        }
        assert_eq!(near_matches("XYZ:", &v), vec!["xyz:TSLA"]);
    }

    #[test]
    fn distance_two_matches_follow() {
        let v = venue(&["BTC", "SOL", "DOGE"]);
        assert_eq!(near_matches("BTX", &v), vec!["BTC"]);
        // A transposition is one edit.
        assert_eq!(near_matches("TBC", &v), vec!["BTC"]);
        assert!(near_matches("XYZW", &v).is_empty());
    }

    #[test]
    fn at_most_five_hints() {
        let v = venue(&["A1", "A2", "A3", "A4", "A5", "A6", "A7"]);
        assert_eq!(near_matches("A", &v).len(), MAX_HINTS);
    }

    #[test]
    fn ties_break_by_distance_then_length_then_bytes() {
        let v = venue(&["ABCD", "ABXY", "ABCDE", "ABCDEF", "ABDC", "XBCD"]);
        // Exact first, then the prefix match, then distance 1 by bytes, then distance 2.
        assert_eq!(
            near_matches("abcd", &v),
            vec!["ABCD", "ABCDE", "ABCDEF", "ABDC", "XBCD"]
        );
        let v = venue(&["BTCB", "BTCA", "BTCAA", "BTC"]);
        assert_eq!(
            near_matches("BTC", &v),
            vec!["BTC", "BTCA", "BTCB", "BTCAA"]
        );
    }

    #[test]
    fn no_distance_scan_above_the_cap() {
        let names: Vec<String> = (0..=DISTANCE_SCAN_MAX)
            .map(|i| format!("S{i:05}"))
            .collect();
        let v: Vec<&str> = names.iter().map(String::as_str).collect();
        assert!(v.windows(2).all(|w| w[0] < w[1]));
        // One edit from S00001, but no name starts with it.
        assert!(near_matches("S0000X", &v).is_empty());
        let under: Vec<&str> = v[..DISTANCE_SCAN_MAX].to_vec();
        assert!(!near_matches("S0000X", &under).is_empty());
    }

    #[test]
    fn empty_venue_gives_no_hints() {
        assert!(near_matches("BTC", &[]).is_empty());
        assert!(near_matches("", &venue(&["BTC"])).is_empty());
    }

    #[test]
    fn long_query_gets_no_hints() {
        let name: &'static str = "A".repeat(MAX_QUERY).leak();
        let v = venue(&[name]);
        assert_eq!(near_matches(name, &v), vec![name]);
        assert!(near_matches(&format!("{name}A"), &v).is_empty());
    }

    #[test]
    fn multibyte_character_across_byte_64_does_not_panic() {
        let q = format!("{}é", "A".repeat(MAX_QUERY - 1));
        assert!(!q.is_char_boundary(MAX_QUERY));
        let name: &'static str = "A".repeat(MAX_QUERY).leak();
        assert!(near_matches(&q, &venue(&[name])).is_empty());
    }

    #[test]
    fn non_ascii_query_gets_no_hints() {
        assert!(near_matches("BTé", &venue(&["BTC", "BT"])).is_empty());
    }

    #[test]
    fn distance_matches_a_reference() {
        fn reference(a: &[u8], b: &[u8]) -> usize {
            let (n, m) = (a.len(), b.len());
            let mut d = vec![vec![0; m + 1]; n + 1];
            for (i, row) in d.iter_mut().enumerate() {
                row[0] = i;
            }
            d[0] = (0..=m).collect();
            for i in 1..=n {
                for j in 1..=m {
                    let cost = usize::from(a[i - 1] != b[j - 1]);
                    d[i][j] = (d[i - 1][j] + 1)
                        .min(d[i][j - 1] + 1)
                        .min(d[i - 1][j - 1] + cost);
                    if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                        d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
                    }
                }
            }
            d[n][m]
        }
        let words = [
            "", "a", "ab", "ba", "abc", "acb", "bca", "abcd", "badc", "xyz", "abxd", "dcba",
        ];
        let mut rows = [
            vec![0; MAX_QUERY + 1],
            vec![0; MAX_QUERY + 1],
            vec![0; MAX_QUERY + 1],
        ];
        for a in words {
            for b in words {
                let want =
                    Some(reference(a.as_bytes(), b.as_bytes())).filter(|&d| d <= MAX_DISTANCE);
                assert_eq!(
                    distance_within(a.as_bytes(), b.as_bytes(), &mut rows),
                    want,
                    "{a} {b}"
                );
            }
        }
    }
}
