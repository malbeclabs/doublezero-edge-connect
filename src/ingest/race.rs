//! The DoubleZero-vs-public race behind `GET /v1/race`: for each symbol a public input races, who
//! delivered each quote tick and each trade first, and by how much.
//!
//! `won` reads the arbiter's own decisions, so it is `dz_quote_ticks_won_total`'s rule restricted to
//! raced symbols: a tick goes to the input that opened it, a walkover included, and a trade to the
//! input whose copy was admitted. `lead_ns` cannot come from those decisions: the floor reports one
//! contest per tick, and an edge mirror's copy usually takes it. Each input's first copy of a tick or
//! trade is paired here instead, stale copies included.
//!
//! A symbol counts only while its public subscription is [`SubState::Live`]. Counted otherwise, every
//! tick of a symbol the public input is not delivering would score as an edge walkover.

use std::collections::{HashMap, VecDeque};

use crate::model::{now_ns, NormalizedQuote, NormalizedTrade};

/// Indexes into the `[edge, public]` pairs, in the arbiter's `pub_idx` order.
pub const EDGE: usize = 0;
pub const PUBLIC: usize = 1;

/// Ticks or trades remembered per symbol and kind for pairing. A copy arriving more than this many
/// keys behind its peer goes unpaired, which understates the lead.
const PAIRING_CAP: usize = 1024;

/// How long after a public input first connects its confirmed symbols read `pending`, rather than
/// `no_edge`, while the edge has defined none of the venue's instruments: several ~8 s
/// reference-data cycles.
pub const NO_EDGE_GRACE_NS: u64 = 30_000_000_000;

/// What one public subscription races.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    Quotes,
    Trades,
}

impl Kind {
    pub const ALL: [Kind; 2] = [Kind::Quotes, Kind::Trades];

    fn idx(self) -> usize {
        self as usize
    }

    pub fn label(self) -> &'static str {
        match self {
            Kind::Quotes => "quotes",
            Kind::Trades => "trades",
        }
    }
}

/// One public subscription's standing, as its input last reported it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SubState {
    /// Unsent, awaiting its ack, or its session ended.
    #[default]
    Pending,
    /// Acked, with the venue's replay of earlier state still to come.
    Acked,
    /// Acked and past that replay: counting.
    Live,
    /// The venue rejected the name.
    Refused,
}

/// A raced symbol's `status` on `/v1/race`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Pending,
    Racing,
    NoEdge,
    Refused,
}

impl Status {
    pub fn label(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Racing => "racing",
            Status::NoEdge => "no_edge",
            Status::Refused => "refused",
        }
    }
}

/// Lead times in log buckets, four per doubling, so a quantile read off it is within ~9% of the
/// true value.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct LeadHist {
    buckets: Vec<u64>,
    count: u64,
}

impl LeadHist {
    fn bucket(ns: u64) -> usize {
        if ns == 0 {
            0
        } else {
            ((ns as f64).log2() * 4.0) as usize + 1
        }
    }

    /// The geometric midpoint of `bucket`.
    fn midpoint(bucket: usize) -> u64 {
        if bucket == 0 {
            0
        } else {
            2f64.powf((bucket as f64 - 0.5) / 4.0).round() as u64
        }
    }

    pub fn record(&mut self, ns: u64) {
        let b = Self::bucket(ns);
        if self.buckets.len() <= b {
            self.buckets.resize(b + 1, 0);
        }
        self.buckets[b] += 1;
        self.count += 1;
    }

    pub fn merge(&mut self, other: &LeadHist) {
        if self.buckets.len() < other.buckets.len() {
            self.buckets.resize(other.buckets.len(), 0);
        }
        for (mine, theirs) in self.buckets.iter_mut().zip(&other.buckets) {
            *mine += theirs;
        }
        self.count += other.count;
    }

    /// The `q` quantile, `0 < q <= 1`; `None` with no samples.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        let rank = ((q * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        self.buckets.iter().enumerate().find_map(|(b, &n)| {
            seen += n;
            (seen >= rank).then(|| Self::midpoint(b))
        })
    }
}

/// One symbol's figures in one kind: wins and lead samples, each by `[edge, public]`.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Figures {
    pub won: [u64; 2],
    pub lead: [LeadHist; 2],
}

impl Figures {
    fn add(&mut self, other: &Figures) {
        for side in [EDGE, PUBLIC] {
            self.won[side] += other.won[side];
            self.lead[side].merge(&other.lead[side]);
        }
    }
}

/// The first copy each input delivered of one tick or trade.
struct First {
    side: usize,
    at_ns: u64,
    /// The venue's time for the tick or trade: what forgetting is judged by.
    venue_ns: u64,
    paired: bool,
}

/// Pairs the two inputs' first copies of each key, once.
#[derive(Default)]
struct Firsts {
    seen: HashMap<u64, First>,
    order: VecDeque<u64>,
    /// The newest venue time of any key forgotten. A key unknown here at or before it may have been
    /// seen and forgotten, so it is skipped: paired afresh, a late copy could pair the wrong way
    /// round or score a tick twice. Venue time, not the key, because trade ids follow no order.
    forgotten_through: Option<u64>,
    newest_ns: Option<u64>,
}

impl Firsts {
    /// `(winner, lead_ns)` when this is the first copy from the input that has not yet delivered
    /// `key`.
    fn pair(&mut self, key: u64, venue_ns: u64, side: usize, at_ns: u64) -> Option<(usize, u64)> {
        if let Some(f) = self.seen.get_mut(&key) {
            if f.paired || f.side == side {
                return None;
            }
            f.paired = true;
            return Some((f.side, at_ns.saturating_sub(f.at_ns)));
        }
        if self.forgotten_through.is_some_and(|t| venue_ns <= t) {
            return None;
        }
        self.newest_ns = self.newest_ns.max(Some(venue_ns));
        self.seen.insert(
            key,
            First {
                side,
                at_ns,
                venue_ns,
                paired: false,
            },
        );
        self.order.push_back(key);
        if self.order.len() > PAIRING_CAP {
            if let Some(old) = self.order.pop_front().and_then(|k| self.seen.remove(&k)) {
                self.forgotten_through = self.forgotten_through.max(Some(old.venue_ns));
            }
        }
        None
    }

    /// Forget every key, as eviction forgets one.
    fn forget_all(&mut self) {
        self.seen.clear();
        self.order.clear();
        self.forgotten_through = self.forgotten_through.max(self.newest_ns);
    }
}

#[derive(Default)]
struct Race {
    state: SubState,
    figures: Figures,
    firsts: Firsts,
}

impl Race {
    /// One copy of `key`, at venue time `venue_ns`, from `side`, arriving at `at_ns` (`0` is "not
    /// stamped"); `won` when the arbiter credited it with the tick or trade.
    fn observe(&mut self, key: u64, venue_ns: u64, side: usize, at_ns: u64, won: bool) {
        if at_ns != 0 {
            if let Some((winner, lead)) = self.firsts.pair(key, venue_ns, side, at_ns) {
                self.figures.lead[winner].record(lead);
            }
        }
        if won {
            self.figures.won[side] += 1;
        }
    }
}

struct VenueRace {
    venue: String,
    up: bool,
    reconnects: u64,
    first_up_ns: Option<u64>,
    /// In the order the input sends them, each with its races by [`Kind`].
    symbols: Vec<(String, [Option<Race>; 2])>,
    index: HashMap<String, usize>,
}

/// Every public input's race, keyed by its Source ID. Empty when no public input runs.
pub struct RaceBoard {
    since_ns: u64,
    venues: HashMap<u16, VenueRace>,
}

impl Default for RaceBoard {
    fn default() -> Self {
        Self {
            since_ns: now_ns(),
            venues: HashMap::new(),
        }
    }
}

impl RaceBoard {
    /// Register a public input's subscriptions, in the order it sends them.
    pub fn register<'a>(
        &mut self,
        source_id: u16,
        venue: &str,
        subs: impl IntoIterator<Item = (&'a str, Kind)>,
    ) {
        let v = self.venues.entry(source_id).or_insert_with(|| VenueRace {
            venue: venue.to_string(),
            up: false,
            reconnects: 0,
            first_up_ns: None,
            symbols: Vec::new(),
            index: HashMap::new(),
        });
        for (symbol, kind) in subs {
            let i = *v.index.entry(symbol.to_string()).or_insert_with(|| {
                v.symbols.push((symbol.to_string(), [None, None]));
                v.symbols.len() - 1
            });
            v.symbols[i].1[kind.idx()].get_or_insert_with(Race::default);
        }
    }

    pub fn set_up(&mut self, source_id: u16, up: bool) {
        if let Some(v) = self.venues.get_mut(&source_id) {
            v.up = up;
            if up {
                v.first_up_ns.get_or_insert_with(now_ns);
            }
        }
    }

    pub fn count_reconnect(&mut self, source_id: u16) {
        if let Some(v) = self.venues.get_mut(&source_id) {
            v.reconnects += 1;
        }
    }

    pub fn set_state(&mut self, source_id: u16, symbol: &str, kind: Kind, state: SubState) {
        if let Some(race) = self.race(source_id, symbol, kind) {
            if race.state == SubState::Live && state != SubState::Live {
                // A copy from before the gap must not pair with one after it.
                race.firsts.forget_all();
            }
            race.state = state;
        }
    }

    fn race(&mut self, source_id: u16, symbol: &str, kind: Kind) -> Option<&mut Race> {
        if self.venues.is_empty() {
            return None;
        }
        let v = self.venues.get_mut(&source_id)?;
        let i = *v.index.get(symbol)?;
        v.symbols[i].1[kind.idx()].as_mut()
    }

    fn live(&mut self, source_id: u16, symbol: &str, kind: Kind) -> Option<&mut Race> {
        self.race(source_id, symbol, kind)
            .filter(|r| r.state == SubState::Live)
    }

    /// A quote that reached the staleness floor from `side`; `opened` when it opened its tick.
    pub fn on_quote(&mut self, q: &NormalizedQuote, side: usize, opened: bool) {
        if let Some(race) = self.live(q.source_id, &q.symbol, Kind::Quotes) {
            race.observe(q.source_ts_ns, q.source_ts_ns, side, q.recv_ts_ns, opened);
        }
    }

    /// A trade that reached the `trade_id` window from `side`; `admitted` when its copy went out.
    pub fn on_trade(&mut self, t: &NormalizedTrade, side: usize, admitted: bool) {
        if let Some(race) = self.live(t.source_id, &t.symbol, Kind::Trades) {
            race.observe(t.trade_id, t.source_ts_ns, side, t.recv_ts_ns, admitted);
        }
    }

    /// A copy of the board for rendering outside the arbiter's lock, venues sorted by name.
    pub fn snapshot(&self) -> RaceSnapshot {
        let mut venues: Vec<VenueSnapshot> = self
            .venues
            .iter()
            .map(|(&source_id, v)| VenueSnapshot {
                source_id,
                venue: v.venue.clone(),
                up: v.up,
                reconnects: v.reconnects,
                first_up_ns: v.first_up_ns,
                symbols: v
                    .symbols
                    .iter()
                    .map(|(symbol, races)| SymbolSnapshot {
                        symbol: symbol.clone(),
                        races: races
                            .each_ref()
                            .map(|r| r.as_ref().map(|r| (r.state, r.figures.clone()))),
                    })
                    .collect(),
            })
            .collect();
        venues.sort_by(|a, b| a.venue.cmp(&b.venue));
        RaceSnapshot {
            since_ns: self.since_ns,
            venues,
        }
    }
}

pub struct RaceSnapshot {
    pub since_ns: u64,
    pub venues: Vec<VenueSnapshot>,
}

pub struct VenueSnapshot {
    pub source_id: u16,
    pub venue: String,
    pub up: bool,
    pub reconnects: u64,
    pub first_up_ns: Option<u64>,
    pub symbols: Vec<SymbolSnapshot>,
}

impl VenueSnapshot {
    /// Every symbol's figures in `kind` merged; `None` when this venue's input does not race it.
    pub fn total(&self, kind: Kind) -> Option<Figures> {
        let mut races = self
            .symbols
            .iter()
            .filter_map(|s| s.races[kind.idx()].as_ref())
            .peekable();
        races.peek()?;
        let mut total = Figures::default();
        for (_, figures) in races {
            total.add(figures);
        }
        Some(total)
    }

    /// Whether [`NO_EDGE_GRACE_NS`] has passed since this input first connected.
    pub fn grace_over(&self, now_ns: u64) -> bool {
        self.first_up_ns
            .is_some_and(|t| now_ns.saturating_sub(t) >= NO_EDGE_GRACE_NS)
    }
}

pub struct SymbolSnapshot {
    pub symbol: String,
    /// By [`Kind`]; `None` where the venue's input does not race it.
    pub races: [Option<(SubState, Figures)>; 2],
}

impl SymbolSnapshot {
    pub fn figures(&self, kind: Kind) -> Option<&Figures> {
        self.races[kind.idx()].as_ref().map(|(_, f)| f)
    }

    /// `defined`: the edge catalog holds this symbol under the venue's Source ID. `venue_defined`:
    /// it holds any of the venue's instruments.
    pub fn status(&self, defined: bool, venue_defined: bool, grace_over: bool) -> Status {
        let mut states = self.races.iter().flatten().map(|(s, _)| *s);
        if states.clone().any(|s| s == SubState::Refused) {
            Status::Refused
        } else if !states.all(|s| matches!(s, SubState::Acked | SubState::Live)) {
            Status::Pending
        } else if defined {
            Status::Racing
        } else if venue_defined || grace_over {
            Status::NoEdge
        } else {
            Status::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{QuoteWire, Side};

    const SRC: u16 = 1;

    fn quote(symbol: &str, source_ts_ns: u64, recv_ts_ns: u64) -> NormalizedQuote {
        NormalizedQuote {
            venue: "HYPERLIQUID".into(),
            source_name: "HYPERLIQUID".into(),
            source_id: SRC,
            symbol: symbol.into(),
            bid: 1.0,
            ask: 2.0,
            bid_size: 1.0,
            ask_size: 1.0,
            bid_n: 0,
            ask_n: 0,
            source_ts_ns,
            recv_ts_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            channel: 0,
            instrument_id: 0,
            category: crate::model::empty_category(),
            venue_update_id: 0,
            wire: QuoteWire::default(),
        }
    }

    fn trade(symbol: &str, trade_id: u64, recv_ts_ns: u64) -> NormalizedTrade {
        NormalizedTrade {
            venue: "HYPERLIQUID".into(),
            source_name: "HYPERLIQUID".into(),
            source_id: SRC,
            symbol: symbol.into(),
            channel: 0,
            instrument_id: 0,
            category: "c".into(),
            price: 1.0,
            size: 1.0,
            aggressor_side: Side::Buy,
            trade_id,
            cumulative_volume: 0.0,
            source_ts_ns: 1,
            recv_ts_ns,
            kernel_rx_ts_ns: 0,
            ws_send_ts_ns: 0,
            wire: Default::default(),
        }
    }

    /// BTC and ETH raced in both kinds and live.
    fn board() -> RaceBoard {
        let mut b = RaceBoard::default();
        b.register(
            SRC,
            "HYPERLIQUID",
            ["BTC", "ETH"]
                .into_iter()
                .flat_map(|s| Kind::ALL.map(|k| (s, k))),
        );
        for s in ["BTC", "ETH"] {
            for k in Kind::ALL {
                b.set_state(SRC, s, k, SubState::Live);
            }
        }
        b
    }

    fn figures(b: &RaceBoard, symbol: &str, kind: Kind) -> Figures {
        b.snapshot().venues[0]
            .symbols
            .iter()
            .find(|s| s.symbol == symbol)
            .and_then(|s| s.figures(kind).cloned())
            .unwrap()
    }

    #[test]
    fn a_symbol_not_raced_never_counts() {
        let mut b = board();
        b.on_quote(&quote("SOL", 1_000, 10), EDGE, true);
        b.on_trade(&trade("SOL", 7, 10), EDGE, true);
        b.on_quote(&quote("BTC", 1_000, 10), EDGE, true);
        let snap = b.snapshot();
        assert_eq!(
            snap.venues[0]
                .symbols
                .iter()
                .map(|s| s.symbol.as_str())
                .collect::<Vec<_>>(),
            ["BTC", "ETH"]
        );
        assert_eq!(snap.venues[0].total(Kind::Quotes).unwrap().won, [1, 0]);
        assert_eq!(snap.venues[0].total(Kind::Trades).unwrap().won, [0, 0]);
    }

    /// Down, unconfirmed, still replaying, or refused: none of them counts or pairs.
    #[test]
    fn nothing_counts_unless_the_subscription_is_live() {
        for state in [SubState::Pending, SubState::Acked, SubState::Refused] {
            let mut b = board();
            b.set_state(SRC, "BTC", Kind::Quotes, state);
            b.on_quote(&quote("BTC", 1_000, 10), EDGE, true);
            b.on_quote(&quote("BTC", 1_000, 50), PUBLIC, false);
            assert_eq!(figures(&b, "BTC", Kind::Quotes), Figures::default());
        }
    }

    /// A tick the public input never delivers is still the edge's, as on `dz_quote_ticks_won_total`.
    #[test]
    fn a_walkover_counts_for_the_edge() {
        let mut b = board();
        b.on_quote(&quote("BTC", 1_000, 10), EDGE, true);
        let f = figures(&b, "BTC", Kind::Quotes);
        assert_eq!(f.won, [1, 0]);
        assert_eq!(f.lead[EDGE].quantile(0.5), None, "a walkover has no lead");
    }

    #[test]
    fn three_mirrors_and_a_public_copy_give_one_sample() {
        let mut b = board();
        b.on_quote(&quote("BTC", 1_000, 100), EDGE, true);
        b.on_quote(&quote("BTC", 1_000, 101), EDGE, false);
        b.on_quote(&quote("BTC", 1_000, 102), EDGE, false);
        b.on_quote(&quote("BTC", 1_000, 1_100), PUBLIC, false);
        b.on_quote(&quote("BTC", 1_000, 1_200), PUBLIC, false);
        let f = figures(&b, "BTC", Kind::Quotes);
        assert_eq!(f.lead[EDGE].count, 1);
        assert_eq!(f.lead[PUBLIC].count, 0);
        let p50 = f.lead[EDGE].quantile(0.5).unwrap();
        assert!((920..=1_090).contains(&p50), "{p50}");
    }

    /// The floor drops a copy of a tick it has passed without a contest; the pairing still sees it.
    #[test]
    fn a_stale_copy_still_pairs() {
        let mut b = board();
        b.on_trade(&trade("BTC", 9, 5_000), PUBLIC, true);
        b.on_trade(&trade("BTC", 9, 25_000), EDGE, false);
        let f = figures(&b, "BTC", Kind::Trades);
        assert_eq!(f.won, [0, 1]);
        assert_eq!(f.lead[PUBLIC].count, 1);
    }

    #[test]
    fn a_copy_from_before_the_input_went_down_does_not_pair() {
        let mut b = board();
        b.on_quote(&quote("BTC", 1_000, 100), EDGE, true);
        b.set_state(SRC, "BTC", Kind::Quotes, SubState::Pending);
        b.set_state(SRC, "BTC", Kind::Quotes, SubState::Live);
        b.on_quote(&quote("BTC", 1_000, 9_000), PUBLIC, false);
        b.on_quote(&quote("BTC", 1_000, 9_100), EDGE, false); // a slower edge mirror
        let f = figures(&b, "BTC", Kind::Quotes);
        assert_eq!((f.lead[EDGE].count, f.lead[PUBLIC].count), (0, 0));
        b.on_quote(&quote("BTC", 2_000, 9_200), EDGE, true);
        b.on_quote(&quote("BTC", 2_000, 9_300), PUBLIC, false);
        assert_eq!(figures(&b, "BTC", Kind::Quotes).lead[EDGE].count, 1);
    }

    /// Past the pairing window a tick is skipped, never paired afresh: a late public copy then a
    /// slower edge mirror must not read as a public lead, nor a paired tick score again.
    #[test]
    fn a_tick_past_the_pairing_window_is_skipped() {
        let mut b = board();
        b.on_quote(&quote("BTC", 1_000, 100), EDGE, true);
        b.on_quote(&quote("BTC", 2_000, 200), EDGE, true);
        b.on_quote(&quote("BTC", 2_000, 300), PUBLIC, false);
        for tick in 3..3 + PAIRING_CAP as u64 {
            b.on_quote(&quote("BTC", tick * 1_000, 400), EDGE, true);
        }
        b.on_quote(&quote("BTC", 1_000, 9_000), PUBLIC, false);
        b.on_quote(&quote("BTC", 1_000, 9_100), EDGE, false);
        b.on_quote(&quote("BTC", 2_000, 9_000), EDGE, false);
        b.on_quote(&quote("BTC", 2_000, 9_100), PUBLIC, false);
        let f = figures(&b, "BTC", Kind::Quotes);
        assert_eq!((f.lead[EDGE].count, f.lead[PUBLIC].count), (1, 0));
    }

    /// The same for trades, whose ids follow no order: here each is above both evicted ones.
    #[test]
    fn a_trade_past_the_pairing_window_is_skipped_whatever_its_id() {
        let mut b = board();
        let t = |id: u64, venue_ns: u64, recv_ts_ns: u64| NormalizedTrade {
            source_ts_ns: venue_ns,
            ..trade("BTC", id, recv_ts_ns)
        };
        b.on_trade(&t(900, 1, 100), EDGE, true);
        b.on_trade(&t(5, 2, 200), EDGE, true);
        b.on_trade(&t(5, 2, 300), PUBLIC, false);
        for n in 0..PAIRING_CAP as u64 {
            b.on_trade(&t(1_000_000 - n, 3 + n, 400), EDGE, true);
        }
        b.on_trade(&t(900, 1, 9_000), PUBLIC, false);
        b.on_trade(&t(900, 1, 9_100), EDGE, false);
        b.on_trade(&t(5, 2, 9_000), EDGE, false);
        b.on_trade(&t(5, 2, 9_100), PUBLIC, false);
        let f = figures(&b, "BTC", Kind::Trades);
        assert_eq!((f.lead[EDGE].count, f.lead[PUBLIC].count), (1, 0));
    }

    #[test]
    fn venue_figures_are_the_symbols_merged() {
        let mut b = board();
        b.on_quote(&quote("BTC", 1_000, 100), EDGE, true);
        b.on_quote(&quote("BTC", 1_000, 200), PUBLIC, false);
        b.on_quote(&quote("ETH", 1_000, 100), PUBLIC, true);
        b.on_quote(&quote("ETH", 1_000, 1_100_000), EDGE, false);
        b.on_quote(&quote("ETH", 2_000, 100), EDGE, true);
        let snap = b.snapshot();
        let v = &snap.venues[0];
        let mut merged = Figures::default();
        for s in &v.symbols {
            merged.add(s.figures(Kind::Quotes).unwrap());
        }
        assert_eq!(v.total(Kind::Quotes).unwrap(), merged);
        assert_eq!(merged.won, [2, 1]);
        assert_eq!(merged.lead[EDGE].count + merged.lead[PUBLIC].count, 2);
    }

    #[test]
    fn a_venue_that_races_trades_only_has_no_quote_figures() {
        let mut b = RaceBoard::default();
        b.register(2, "PHOENIX", [("SOL", Kind::Trades)]);
        let snap = b.snapshot();
        assert!(snap.venues[0].total(Kind::Quotes).is_none());
        assert!(snap.venues[0].symbols[0].figures(Kind::Quotes).is_none());
        assert!(snap.venues[0].total(Kind::Trades).is_some());
    }

    #[test]
    fn quantiles_are_null_without_samples_and_close_with_them() {
        let mut h = LeadHist::default();
        assert_eq!((h.quantile(0.5), h.quantile(0.99)), (None, None));
        for ns in 1..=10_000u64 {
            h.record(ns * 1_000);
        }
        let near = |got: u64, want: f64| (got as f64 / want - 1.0).abs() < 0.1;
        assert!(near(h.quantile(0.5).unwrap(), 5_000_000.0));
        assert!(near(h.quantile(0.99).unwrap(), 9_900_000.0));
        h.record(0);
        assert_eq!(h.quantile(0.00001), Some(0));
    }

    #[test]
    fn status_follows_the_subscription_then_the_edge_catalog() {
        let sym = |states: &[SubState]| SymbolSnapshot {
            symbol: "BTC".into(),
            races: [0, 1].map(|i| states.get(i).map(|s| (*s, Figures::default()))),
        };
        let confirmed = sym(&[SubState::Acked, SubState::Live]);
        assert_eq!(confirmed.status(true, true, false), Status::Racing);
        assert_eq!(confirmed.status(false, true, false), Status::NoEdge);
        assert_eq!(confirmed.status(false, false, false), Status::Pending);
        assert_eq!(confirmed.status(false, false, true), Status::NoEdge);
        let half = sym(&[SubState::Live, SubState::Pending]);
        assert_eq!(half.status(true, true, true), Status::Pending);
        let refused = sym(&[SubState::Refused, SubState::Refused]);
        assert_eq!(refused.status(true, true, true), Status::Refused);
        assert_eq!(
            sym(&[SubState::Live]).status(true, true, false),
            Status::Racing
        );
    }
}
