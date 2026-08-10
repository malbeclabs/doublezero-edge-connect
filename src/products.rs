//! Product identity for the query API and the CLI.
//!
//! A product id is the CLI-facing name for one market. It is `SOURCE:SYMBOL` where the symbol is
//! unique within its source, and `SOURCE:SYMBOL#<channel>.<instrument_id>` where it is not — the
//! price-aggregated protocol's `symbol` is a truncated display label that collides, so it is not an
//! identity on its own.
//!
//! Rendering the suffix only when needed keeps the common case readable; parsing accepts either form
//! always, so a consumer that pinned a suffixed id keeps working if the collision later clears.

use std::sync::Arc;

use crate::{ingest::sources::source_label, model::InstrumentSnapshot};

/// One market's full identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductId {
    pub source_id: u16,
    pub symbol: Arc<str>,
    pub channel: u32,
    pub instrument_id: u32,
    /// The instrument universe this market belongs to (`ingest::feeds::Feed::category`) —
    /// producer-side only, carried so a caller holding a `ProductId` can look the market back up
    /// in `InstrumentSnapshot`/`BookSnapshot` (both keyed on it) without re-resolving. Deliberately
    /// **not** rendered by [`Self::render`] and not accepted by [`parse`]: it never reaches the
    /// wire, so it cannot be part of the consumer-facing product-id syntax.
    pub category: Arc<str>,
}

impl ProductId {
    /// Render as a CLI-facing id. `ambiguous` is the caller's finding that this symbol is not unique
    /// within its source — it is not derivable here, because it depends on the whole catalog.
    pub fn render(&self, ambiguous: bool) -> String {
        let src = source_label(self.source_id);
        if ambiguous {
            format!(
                "{src}:{}#{}.{}",
                self.symbol, self.channel, self.instrument_id
            )
        } else {
            format!("{src}:{}", self.symbol)
        }
    }
}

/// A syntactically valid id, not yet matched against the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedId {
    /// Uppercased short name.
    pub source: String,
    pub symbol: String,
    /// `(channel, instrument_id)` when the id carried the disambiguating suffix.
    pub identity: Option<(u32, u32)>,
}

/// Parse a product id. Returns `None` for anything malformed rather than guessing — an agent that
/// mistypes an id must get an error naming the problem, not a silent match on something else.
pub fn parse(s: &str) -> Option<ParsedId> {
    let (source, rest) = s.split_once(':')?;
    if source.is_empty() || rest.is_empty() {
        return None;
    }
    // Only a trailing `#` introduces the identity; `#` cannot appear in a wire symbol. Split from the
    // right so a symbol containing `#` would still lose only its last segment rather than its first.
    let (symbol, identity) = match rest.rsplit_once('#') {
        Some((sym, id)) => {
            let (ch, inst) = id.split_once('.')?;
            (sym, Some((ch.parse().ok()?, inst.parse().ok()?)))
        }
        None => (rest, None),
    };
    if symbol.is_empty() {
        return None;
    }
    Some(ParsedId {
        source: source.to_uppercase(),
        symbol: symbol.to_string(),
        identity,
    })
}

/// What matching a [`ParsedId`] against the catalog found.
#[derive(Debug)]
pub enum Resolution {
    One(ProductId),
    /// The bare symbol matched more than one market. Carries the candidates, rendered, so the caller
    /// can list them — an ambiguous id is an error that names its alternatives, never a silent pick.
    Ambiguous(Vec<String>),
    None,
}

/// Match a parsed id against the instrument snapshot.
///
/// `instruments` is now keyed `(venue, category, channel, instrument_id)` — two disjoint universes
/// under one Source ID can share `(channel, instrument_id)`, so filtering by source+symbol alone
/// can legitimately produce hits from more than one category. That is not a defect to paper over:
/// each hit already carries the category its own `NormalizedInstrument` entry was stored under (no
/// cross-category merge ever happens upstream any more), so genuinely distinct markets surface as
/// `Ambiguous` exactly as a same-category symbol collision already does, and the `#<channel>.
/// <instrument_id>` suffix disambiguates them the same way — without a category ever entering the
/// wire syntax.
pub fn resolve(instruments: &InstrumentSnapshot, id: &ParsedId) -> Resolution {
    let map = crate::model::lock(instruments);
    let mut hits: Vec<ProductId> = map
        .values()
        .filter(|i| {
            source_label(i.source_id).eq_ignore_ascii_case(&id.source)
                && i.symbol.as_ref() == id.symbol
        })
        .map(|i| ProductId {
            source_id: i.source_id,
            symbol: i.symbol.clone(),
            channel: i.channel,
            instrument_id: i.instrument_id,
            category: i.category.clone(),
        })
        .collect();

    if let Some((ch, inst)) = id.identity {
        hits.retain(|p| p.channel == ch && p.instrument_id == inst);
    }

    match hits.len() {
        0 => Resolution::None,
        1 => Resolution::One(hits.remove(0)),
        _ => Resolution::Ambiguous(hits.iter().map(|p| p.render(true)).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NormalizedInstrument;

    #[test]
    fn a_unique_symbol_renders_as_source_and_symbol() {
        let p = ProductId {
            source_id: 1,
            symbol: "BTC".into(),
            channel: 0,
            instrument_id: 41,
            category: "default".into(),
        };
        assert_eq!(p.render(false), "HYPERLIQUID:BTC");
    }

    /// A symbol that collides within its source must carry the identity that does not collide.
    /// `category` plays no part in the rendered form — it never reaches the wire.
    #[test]
    fn a_colliding_symbol_renders_with_its_identity_suffix() {
        let p = ProductId {
            source_id: 1,
            symbol: "BTC".into(),
            channel: 2,
            instrument_id: 41,
            category: "perps".into(),
        };
        assert_eq!(p.render(true), "HYPERLIQUID:BTC#2.41");
    }

    #[test]
    fn a_bare_id_parses_into_source_and_symbol() {
        let got = parse("HYPERLIQUID:BTC").unwrap();
        assert_eq!(got.source, "HYPERLIQUID");
        assert_eq!(got.symbol, "BTC");
        assert_eq!(got.identity, None);
    }

    #[test]
    fn a_suffixed_id_parses_into_its_identity() {
        let got = parse("HYPERLIQUID:BTC#2.41").unwrap();
        assert_eq!(got.symbol, "BTC");
        assert_eq!(got.identity, Some((2, 41)));
    }

    /// Source matching is case-insensitive, as the WS filter's is.
    #[test]
    fn parsing_is_case_insensitive_on_the_source() {
        assert_eq!(parse("hyperliquid:BTC").unwrap().source, "HYPERLIQUID");
    }

    /// Only the LAST `#` introduces the identity suffix; an earlier `#` is part of the symbol. This
    /// is the case that actually distinguishes splitting from the left vs. from the right — a
    /// symbol with no `#` at all (as in the punctuation test above) can't tell `split_once` and
    /// `rsplit_once` apart, since both agree when there is at most one match.
    #[test]
    fn only_the_trailing_hash_introduces_the_identity_suffix() {
        let got = parse("PHOENIX:WEIRD#SYMBOL#2.5").unwrap();
        assert_eq!(got.symbol, "WEIRD#SYMBOL");
        assert_eq!(got.identity, Some((2, 5)));
    }

    /// A symbol may legitimately contain a dash or a dot; only the FIRST colon separates, and only a
    /// trailing `#` introduces the identity. Getting this wrong silently mangles real symbols.
    #[test]
    fn a_symbol_containing_punctuation_survives_parsing() {
        let got = parse("PHOENIX:SOL-PERP.2").unwrap();
        assert_eq!(got.symbol, "SOL-PERP.2");
        assert_eq!(got.identity, None);
    }

    #[test]
    fn malformed_ids_are_rejected_rather_than_guessed() {
        assert!(parse("NOCOLON").is_none());
        assert!(parse(":BTC").is_none());
        assert!(parse("HYPERLIQUID:").is_none());
        assert!(parse("HYPERLIQUID:BTC#notanumber").is_none());
        assert!(
            parse("HYPERLIQUID:BTC#2").is_none(),
            "identity needs both parts"
        );
    }

    fn instrument(
        category: &str,
        symbol: &str,
        channel: u32,
        instrument_id: u32,
    ) -> NormalizedInstrument {
        NormalizedInstrument {
            venue: "HYPERLIQUID".into(),
            source: "HYPERLIQUID".into(),
            source_id: 1,
            symbol: symbol.into(),
            channel,
            instrument_id,
            category: category.into(),
            price_exponent: -2,
            qty_exponent: -4,
        }
    }

    fn snapshot(defs: Vec<NormalizedInstrument>) -> InstrumentSnapshot {
        let mut map = std::collections::HashMap::new();
        for d in defs {
            map.insert(
                (
                    d.venue.clone(),
                    d.category.clone(),
                    d.channel,
                    d.instrument_id,
                ),
                d,
            );
        }
        std::sync::Arc::new(std::sync::Mutex::new(map))
    }

    /// Two disjoint universes ("perps" and "sports") under one Source ID both happen to use
    /// `channel=5, instrument_id=41` — the exact scenario `channel_id` ranges are never safe to
    /// assume disjoint (see this module's docs). Each names a genuinely different market there, so
    /// resolving one's product id by its own symbol must return *that* market's identity — not the
    /// peer's — even though the map holds two entries at the same `(channel, instrument_id)` pair.
    /// Before `InstrumentSnapshot` carried `category` in its key, one of these two entries would
    /// never even have survived `upsert_instrument`'s last-writer-wins overwrite; this pins that
    /// both now coexist and resolve independently.
    #[test]
    fn resolving_one_universes_symbol_does_not_return_its_peers_market() {
        let snap = snapshot(vec![
            instrument("perps", "BTC-PERP", 5, 41),
            instrument("sports", "LAKERS-WIN", 5, 41),
        ]);

        let perps = parse("HYPERLIQUID:BTC-PERP").unwrap();
        match resolve(&snap, &perps) {
            Resolution::One(p) => {
                assert_eq!(p.category.as_ref(), "perps");
                assert_eq!(p.channel, 5);
                assert_eq!(p.instrument_id, 41);
            }
            other => panic!(
                "expected exactly the perps market, got a resolution that would serve \
                              the wrong universe: {other:?}"
            ),
        }

        let sports = parse("HYPERLIQUID:LAKERS-WIN").unwrap();
        match resolve(&snap, &sports) {
            Resolution::One(p) => assert_eq!(p.category.as_ref(), "sports"),
            other => panic!("expected exactly the sports market: {other:?}"),
        }
    }
}
