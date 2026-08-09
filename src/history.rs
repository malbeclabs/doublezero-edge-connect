//! A bounded in-memory market-data history: one-second OHLCV buckets plus a ring of recent prints,
//! over a rolling one-hour window.
//!
//! Pre-aggregating into fixed one-second buckets is what makes the footprint independent of trade
//! rate: a product costs the same whether it prints once a second or five hundred times. Retaining
//! raw prints for the hour would not — a busy market alone would be tens of megabytes.
//!
//! Fed from the post-arbiter broadcast, so prints arriving here are already deduplicated on
//! `trade_id` and gated by the tape leader: one copy of each print, no cross-publisher doubling.
//!
//! Pure: no I/O, no locks of its own, no clock. `now_secs` is always supplied by the caller, exactly
//! as `book.rs` and `pricebook.rs` take their inputs pre-decoded and their clocks as parameters. Two
//! consequences of that follow directly: `now_secs` must share the same clock domain as the print
//! timestamps fed to `ingest` (a local wall clock queried against venue-stamped prints that are
//! running ahead of it would silently drop the newest seconds, reading as an outage that is really
//! just a clock disagreement), and the `now_secs` filter only excludes the single not-yet-closed
//! 1-second bucket — a coarser-granularity query can and does return a partial, still-accumulating
//! candle for its own newest group, since "one second closed" and "one 60-second group closed" are
//! different claims.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// Rolling window, in seconds.
pub const WINDOW_SECS: u64 = 3_600;
/// Recent raw prints retained per product, for `ticker`.
pub const TRADE_RING: usize = 1_000;
/// Cap on tracked products — a pure cardinality guard on the `HashMap` and the LRU eviction scan,
/// evicting the least recently traded. It does **not** bound total memory on its own: a product's
/// real cost is one bucket per second it traded in, so it varies as much as ~60x between a market
/// printing once a minute and one printing every second, and `MAX_PRODUCTS x worst-case-per-product`
/// is exactly the over-provisioned-for-the-quiet-case multiply `MAX_BUCKETS_ACROSS_PRODUCTS` below
/// exists to avoid — see its doc for the bound that actually holds. The print ring *is* a fixed
/// per-product cost regardless of trade rate, so this cap does bound it directly:
/// `MAX_PRODUCTS * TRADE_RING * size_of::<Print>()` is ~1,024 * 1,000 * 24 B =~ 24 MiB.
pub const MAX_PRODUCTS: usize = 1_024;
/// Total 1-second buckets held across every product before the overflow policy evicts whole
/// products — least recently traded first — until back under budget. This, not `MAX_PRODUCTS`, is
/// the bound that actually holds, for the same reason `MAX_BUFFERED_DELTAS_ACROSS_BOOKS` exists in
/// `ingest/processor.rs`: a per-item cap sized for the worst case multiplies into a huge total, and
/// sized for the common case it fails to bound anything. It is cheap for the wire to fill fast: the
/// keys are wire-supplied, and a burst of synthetic ascending print timestamps fills a product's
/// whole hour-window on arrival rather than over a real hour. At ~93 bytes resident per retained
/// bucket (measured, not the ~48-byte logical `(u64, Bucket)` payload — `BTreeMap`'s node overhead
/// is real), 2^20 buckets is ~97 MiB; together with the print-ring bound above, worst-case history
/// memory is on the order of ~121 MiB.
pub const MAX_BUCKETS_ACROSS_PRODUCTS: usize = 1 << 20;

/// One market's identity within the history store — the identity, not the display symbol (a
/// price-aggregated `symbol` can collide across markets; see `products::ProductId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key {
    pub source_id: u16,
    pub channel: u32,
    pub instrument_id: u32,
}

/// One raw print as it arrives, already resolved to a single timestamp (`source_ts_ns`-else-
/// `recv_ts_ns`; never the `0` sentinel — see Task 4).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Print {
    pub ts_ns: u64,
    pub price: f64,
    pub size: f64,
}

/// One OHLCV candle at whatever granularity was requested.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candle {
    /// Unix seconds, floored to the granularity.
    pub start: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// What a caller can rely on having, for one `candles`/`retention` query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Retention {
    /// The store's fixed rolling window (`WINDOW_SECS`), reported for the caller's convenience.
    pub window_seconds: u64,
    /// Start of the oldest candle the query could have returned, deliberately **before** `limit`
    /// truncation — this and `newest` describe what the window held, not what one page of it was.
    pub oldest: u64,
    /// Start of the newest candle the query could have returned, before `limit` truncation.
    pub newest: u64,
    /// True when `limit` cut the result short of what the window actually held — the caller cannot
    /// tell a full window from a truncated one without this: at second granularity 3,600 candles
    /// exist against any reasonably small page size, so this fires routinely.
    pub truncated: bool,
}

/// One second's aggregated trade activity. `close` is the last print seen for this bucket by
/// *arrival* — the wire gives no intra-second ordering, so this is the honest reading rather than
/// implying a precision the data does not have.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl Bucket {
    fn opened_by(print: &Print) -> Self {
        Bucket {
            open: print.price,
            high: print.price,
            low: print.price,
            close: print.price,
            volume: print.size,
        }
    }

    /// Widen this bucket with a later (by arrival) print. Never called for the print that opens the
    /// bucket — see `opened_by`.
    fn widen(&mut self, print: &Print) {
        self.high = self.high.max(print.price);
        self.low = self.low.min(print.price);
        self.close = print.price;
        self.volume += print.size;
    }
}

/// Per-product state: the bucket window and the raw-print ring.
#[derive(Debug, Default)]
struct Product {
    buckets: BTreeMap<u64, Bucket>,
    /// The latest bucket second observed for this product. `None` before the first print. Drives
    /// both the late-print rejection and the window eviction, scoped per product since two products'
    /// clocks need not agree (different venues, different session resets).
    newest_seen: Option<u64>,
    ring: VecDeque<Print>,
    /// Monotonic touch counter for LRU eviction, bumped on every ingest for this product. Cheaper
    /// than reordering a recency list on every message: the store only has to scan for the minimum
    /// when the cap is actually hit, which is the rare path (`MAX_PRODUCTS` is meant to be generous).
    last_touch: u64,
}

/// A rolling one-hour window of market data across all tracked products.
pub struct Store {
    products: HashMap<Key, Product>,
    touch_clock: u64,
    late_drops: u64,
    evicted: u64,
    /// Running total of `buckets.len()` summed across every product — the accounting seam
    /// `MAX_BUCKETS_ACROSS_PRODUCTS` checks against. Every path that can add or remove a bucket
    /// (a fresh insert, this product's own window eviction, or a whole product being evicted by
    /// either `touch`'s count cap or `enforce_bucket_budget`) must keep this in step, mirroring
    /// `MboProcessor::buffered_total`'s discipline in `ingest/processor.rs`.
    buckets_total: usize,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        Self {
            products: HashMap::new(),
            touch_clock: 0,
            late_drops: 0,
            evicted: 0,
            buckets_total: 0,
        }
    }

    /// Number of tracked products.
    pub fn len(&self) -> usize {
        self.products.len()
    }

    pub fn is_empty(&self) -> bool {
        self.products.is_empty()
    }

    /// Prints rejected for arriving older than their product's window. Global across products: the
    /// counter exists to confirm the drop happened at all, not to attribute it to one market.
    pub fn late_drops(&self) -> u64 {
        self.late_drops
    }

    /// Products evicted, whether to stay within `MAX_PRODUCTS` (cardinality) or
    /// `MAX_BUCKETS_ACROSS_PRODUCTS` (the aggregate bucket budget) — both remove a whole product,
    /// least recently traded first, so one counter covers both.
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Record one print for `key`.
    ///
    /// 1. Its bucket is `print.ts_ns / 1_000_000_000` (unix seconds).
    /// 2. A print older than `newest_seen - WINDOW_SECS` for this product is rejected and counted
    ///    (`late_drops`) rather than folded into the oldest bucket, which would silently corrupt it.
    ///    This check runs, and the product's LRU position is left untouched, **before** anything
    ///    else: a product fed nothing but stale prints must not stay LRU-hot merely by contacting
    ///    the store, or an attacker could pin a spent identity in the cap with stale traffic alone
    ///    while a real market's slot starves.
    /// 3. Otherwise its bucket is upserted: `open` set once, `high`/`low` widened, `close`
    ///    overwritten, `volume` summed — so a late print (one that lands in an already-touched
    ///    bucket) widens only its own bucket and never touches a later one.
    /// 4. Buckets older than the window are evicted from the front of the map.
    /// 5. The print is pushed onto the recent-prints ring, dropping the oldest past `TRADE_RING`.
    /// 6. The product's LRU position is touched; the least recently traded product is evicted past
    ///    `MAX_PRODUCTS`, counted.
    /// 7. If the aggregate bucket count across every product now exceeds
    ///    `MAX_BUCKETS_ACROSS_PRODUCTS`, whole products are evicted — least recently traded first —
    ///    until back under budget (see that constant's doc for why this bound, not step 6's, is the
    ///    one that actually holds).
    pub fn ingest(&mut self, key: Key, print: Print) {
        let bucket_secs = print.ts_ns / 1_000_000_000;

        // Late-drop check against the product's existing watermark, before touching (creating or
        // LRU-marking) anything — step 2's ordering guarantee. A brand-new key has no watermark
        // yet, so this lookup only ever rejects a print against a product that already exists; a
        // first print is never late.
        if let Some(existing) = self.products.get(&key) {
            if let Some(newest) = existing.newest_seen {
                if bucket_secs + WINDOW_SECS < newest {
                    self.late_drops += 1;
                    return;
                }
            }
        }

        self.touch(&key);
        let product = self.products.get_mut(&key).expect("touch always inserts");
        product.newest_seen = Some(
            product
                .newest_seen
                .map_or(bucket_secs, |n| n.max(bucket_secs)),
        );

        let is_new_bucket = !product.buckets.contains_key(&bucket_secs);
        product
            .buckets
            .entry(bucket_secs)
            .and_modify(|b| b.widen(&print))
            .or_insert_with(|| Bucket::opened_by(&print));

        // `floor` only ever moves forward (it tracks this product's own high-water mark), so
        // eviction only ever removes from the map's front. A full `BTreeMap::retain` would rescan
        // every retained bucket on every single print — almost all of which evict nothing; popping
        // from the front until the floor is reached costs exactly the buckets actually evicted.
        let floor = product
            .newest_seen
            .expect("just set above")
            .saturating_sub(WINDOW_SECS);
        let mut evicted_buckets = 0usize;
        while let Some((&start, _)) = product.buckets.first_key_value() {
            if start >= floor {
                break;
            }
            product.buckets.pop_first();
            evicted_buckets += 1;
        }

        product.ring.push_back(print);
        while product.ring.len() > TRADE_RING {
            product.ring.pop_front();
        }

        // `product`'s borrow ends above (last used in the ring eviction loop); free to touch other
        // `self` fields now.
        if is_new_bucket {
            self.buckets_total += 1;
        }
        self.buckets_total -= evicted_buckets;

        self.enforce_bucket_budget();
    }

    /// Fetch-or-create `key`'s product and mark it most-recently-traded, evicting the least
    /// recently traded product first if this is a new key past `MAX_PRODUCTS`. The map is keyed on
    /// wire-supplied identity over an unauthenticated multicast feed, so it must never grow without
    /// bound. This is a cardinality guard only — see `MAX_PRODUCTS`'s doc for the aggregate-memory
    /// bound, which is `enforce_bucket_budget` below.
    fn touch(&mut self, key: &Key) {
        self.touch_clock += 1;
        let clock = self.touch_clock;

        if !self.products.contains_key(key) && self.products.len() >= MAX_PRODUCTS {
            if let Some(oldest) = self
                .products
                .iter()
                .min_by_key(|(_, p)| p.last_touch)
                .map(|(k, _)| *k)
            {
                if let Some(removed) = self.products.remove(&oldest) {
                    self.buckets_total -= removed.buckets.len();
                }
                self.evicted += 1;
            }
        }

        let product = self.products.entry(*key).or_default();
        product.last_touch = clock;
    }

    /// The bound that actually holds (see `MAX_BUCKETS_ACROSS_PRODUCTS`'s doc): while the aggregate
    /// bucket count across every product exceeds it, evict whole products — least recently traded
    /// first — until back under budget.
    fn enforce_bucket_budget(&mut self) {
        while self.buckets_total > MAX_BUCKETS_ACROSS_PRODUCTS {
            let Some(oldest) = self
                .products
                .iter()
                .min_by_key(|(_, p)| p.last_touch)
                .map(|(k, _)| *k)
            else {
                break; // Nothing left to evict; stop rather than spin.
            };
            if let Some(removed) = self.products.remove(&oldest) {
                self.buckets_total -= removed.buckets.len();
                self.evicted += 1;
            }
        }
    }

    /// Roll the retained 1-second buckets up into `granularity_secs` candles, newest first. Shared
    /// by `candles` and `retention` so both agree on what "available" means: only buckets in
    /// `[now_secs - WINDOW_SECS, now_secs)` are considered — the upper bound because a bucket at or
    /// after `now_secs` has not finished and is not yet a candle, the lower bound because the served
    /// window must track `now_secs`, not merely this product's own last-activity watermark. Without
    /// it, a product that stopped trading ten hours ago (so nothing has advanced its own eviction
    /// floor since) would still hand back a full hour of stale candles to a caller querying the
    /// real "now" — the window would be a full hour trailing the product's last print, not the
    /// rolling one-hour window the module promises.
    ///
    /// `granularity_secs` is floored to 1 (a caller-supplied `0` would otherwise divide by zero in
    /// the grouping below; Task 4's query layer takes this from outside the process).
    ///
    /// `BTreeMap` iterates by ascending bucket start, and `start / granularity_secs * granularity_secs`
    /// is monotonic non-decreasing in `start`, so every bucket belonging to one group arrives
    /// contiguously — no separate grouping pass is needed. `open` comes from the first bucket
    /// visited in a group, `close` from the last (both by bucket start, which is the store's only
    /// notion of order), `high`/`low` widen across the group, `volume` sums.
    fn rollup(&self, key: &Key, granularity_secs: u64, now_secs: u64) -> Vec<Candle> {
        let granularity_secs = granularity_secs.max(1);
        let Some(product) = self.products.get(key) else {
            return Vec::new();
        };

        let floor = now_secs.saturating_sub(WINDOW_SECS);
        let mut groups: Vec<Candle> = Vec::new();
        for (&start, bucket) in product.buckets.range(floor..now_secs) {
            let group_start = (start / granularity_secs) * granularity_secs;
            match groups.last_mut() {
                Some(g) if g.start == group_start => {
                    g.high = g.high.max(bucket.high);
                    g.low = g.low.min(bucket.low);
                    g.close = bucket.close;
                    g.volume += bucket.volume;
                }
                _ => groups.push(Candle {
                    start: group_start,
                    open: bucket.open,
                    high: bucket.high,
                    low: bucket.low,
                    close: bucket.close,
                    volume: bucket.volume,
                }),
            }
        }
        groups.reverse();
        groups
    }

    /// Candles at `granularity_secs`, newest first, truncated to `limit`. Empty intervals are
    /// omitted, never zero-filled — a zero-volume candle at price 0 is indistinguishable from a real
    /// one and would wreck any indicator computed over it.
    pub fn candles(
        &self,
        key: &Key,
        granularity_secs: u64,
        limit: usize,
        now_secs: u64,
    ) -> Vec<Candle> {
        let mut groups = self.rollup(key, granularity_secs, now_secs);
        groups.truncate(limit);
        groups
    }

    /// What a `candles` call at the same parameters can be relied on to have covered.
    pub fn retention(
        &self,
        key: &Key,
        granularity_secs: u64,
        limit: usize,
        now_secs: u64,
    ) -> Retention {
        let groups = self.rollup(key, granularity_secs, now_secs);
        let oldest = groups.last().map(|c| c.start).unwrap_or(now_secs);
        let newest = groups.first().map(|c| c.start).unwrap_or(now_secs);
        Retention {
            window_seconds: WINDOW_SECS,
            oldest,
            newest,
            truncated: groups.len() > limit,
        }
    }

    /// The most recent raw prints for `key`, newest first, truncated to `limit`.
    pub fn recent_trades(&self, key: &Key, limit: usize) -> Vec<Print> {
        let Some(product) = self.products.get(key) else {
            return Vec::new();
        };
        product.ring.iter().rev().take(limit).copied().collect()
    }

    /// Drop every product belonging to `(source_id, channel)` — every `instrument_id` tracked under
    /// that channel — regardless of the other identity components. Returns the number of products
    /// dropped, so a caller can tell "nothing was there" from "the drop happened".
    ///
    /// The seam a caller (the reconciler) uses when a channel leaves the ingest floor: a channel
    /// that no longer ingests must not keep answering `/candles`/`/ticker` from a frozen window that
    /// looks live, so its buckets and print ring are removed with it rather than left to age out of
    /// the window on their own. `(group code, channel_id) -> source_id` resolution is the caller's
    /// job (via `ingest::feeds::feeds()` + `ingest::sources::source_id_of`) — this store has no
    /// notion of a group code, only the wire `source_id` every `Key` already carries.
    ///
    /// ⚠️ **Known over-drop risk, not fixed here.** [`Key`] carries no `category` — two disjoint
    /// universes sharing one Source ID (a venue-wide `source_id`) can collide on `(channel,
    /// instrument_id)`, and this filter cannot tell them apart. Shedding one universe's channel
    /// therefore also wipes a live peer universe's window under the same channel id, if one
    /// happens to exist. This is a real, not hypothetical, gap — never assume `channel_id` ranges
    /// stay disjoint across universes, they are a mid-migration numbering convention owned
    /// upstream and enforced nowhere in this code. The proper fix is a category-carrying identity
    /// (mirroring `model::BookKey`, which already has one — see `model::BookReplay::forget_channel`,
    /// which is therefore exempt from this risk), which ripples into `model.rs`/`products.rs`/
    /// `sinks/api.rs` and is out of this method's scope; it is folded into the next task. Do not
    /// paper over this by claiming a channel id is safe to shed — it is not, in general.
    ///
    /// Keeps `buckets_total` in step (subtracting exactly the removed products' own bucket counts),
    /// the same discipline every other removal path in this module follows — see that field's doc.
    pub fn forget_channel(&mut self, source_id: u16, channel: u32) -> usize {
        let doomed: Vec<Key> = self
            .products
            .keys()
            .copied()
            .filter(|k| k.source_id == source_id && k.channel == channel)
            .collect();
        let mut dropped = 0usize;
        for key in doomed {
            if let Some(product) = self.products.remove(&key) {
                self.buckets_total -= product.buckets.len();
                dropped += 1;
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Key {
        Key {
            source_id: 1,
            channel: 0,
            instrument_id: 41,
        }
    }

    fn trade(ts_s: u64, price: f64, size: f64) -> Print {
        Print {
            ts_ns: ts_s * 1_000_000_000,
            price,
            size,
        }
    }

    #[test]
    fn one_second_of_prints_forms_one_bucket() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(100, 12.0, 2.0));
        s.ingest(key(), trade(100, 8.0, 3.0));
        let c = s.candles(&key(), 1, 10, 101);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].start, 100);
        assert_eq!(c[0].open, 10.0);
        assert_eq!(c[0].high, 12.0);
        assert_eq!(c[0].low, 8.0);
        assert_eq!(c[0].close, 8.0, "close is the last print, not the lowest");
        assert_eq!(c[0].volume, 6.0);
    }

    /// Roll-up must aggregate, not sample: open from the first bucket, close from the last, high/low
    /// across all, volume summed.
    #[test]
    fn buckets_roll_up_into_a_coarser_granularity() {
        let mut s = Store::new();
        s.ingest(key(), trade(60, 10.0, 1.0));
        s.ingest(key(), trade(61, 20.0, 1.0));
        s.ingest(key(), trade(119, 5.0, 1.0));
        let c = s.candles(&key(), 60, 10, 120);
        assert_eq!(c.len(), 1, "all three fall in the same 60s bucket");
        assert_eq!(c[0].start, 60);
        assert_eq!(c[0].open, 10.0);
        assert_eq!(c[0].high, 20.0);
        assert_eq!(c[0].low, 5.0);
        assert_eq!(c[0].close, 5.0);
        assert_eq!(c[0].volume, 3.0);
    }

    /// An interval with no prints is omitted, not zero-filled. A zero-volume candle at price 0 would
    /// be indistinguishable from a real one and would wreck any indicator computed over it.
    #[test]
    fn empty_intervals_are_omitted() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(300, 11.0, 1.0));
        let c = s.candles(&key(), 1, 500, 301);
        assert_eq!(c.len(), 2, "199 silent seconds produce no candles");
    }

    /// Newest first, matching the emulated API.
    #[test]
    fn candles_are_returned_newest_first() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(200, 11.0, 1.0));
        let c = s.candles(&key(), 1, 10, 201);
        assert_eq!(c[0].start, 200);
        assert_eq!(c[1].start, 100);
    }

    /// A print whose bucket already closed still lands in that bucket, and must not corrupt it.
    #[test]
    fn a_late_print_updates_its_own_bucket_in_place() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(200, 11.0, 1.0));
        s.ingest(key(), trade(100, 99.0, 1.0)); // late, belongs to bucket 100
        let c = s.candles(&key(), 1, 10, 201);
        let b100 = c.iter().find(|c| c.start == 100).unwrap();
        assert_eq!(b100.high, 99.0, "late print widens its own bucket");
        assert_eq!(b100.volume, 2.0);
        let b200 = c.iter().find(|c| c.start == 200).unwrap();
        assert_eq!(b200.high, 11.0, "and does not touch a later one");
    }

    /// Older than the window is dropped and counted, not silently folded into the oldest bucket.
    #[test]
    fn a_print_older_than_the_window_is_dropped_and_counted() {
        let mut s = Store::new();
        s.ingest(key(), trade(10_000, 10.0, 1.0));
        s.ingest(key(), trade(10_000 - WINDOW_SECS - 5, 99.0, 1.0));
        assert_eq!(s.late_drops(), 1);
        let c = s.candles(&key(), 1, 5000, 10_001);
        assert!(
            c.iter().all(|c| c.high != 99.0),
            "the stale print is nowhere"
        );
    }

    #[test]
    fn the_window_evicts_buckets_older_than_its_span() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(100 + WINDOW_SECS + 10, 11.0, 1.0));
        let c = s.candles(&key(), 1, 5000, 100 + WINDOW_SECS + 11);
        assert_eq!(c.len(), 1, "the first bucket aged out");
        assert_eq!(c[0].high, 11.0);
    }

    /// `limit` binds before the window at second granularity; the caller must be able to tell.
    #[test]
    fn a_limit_that_binds_is_reported_as_truncated() {
        let mut s = Store::new();
        for t in 0..100 {
            s.ingest(key(), trade(1_000 + t, 10.0, 1.0));
        }
        let c = s.candles(&key(), 1, 10, 1_100);
        assert_eq!(c.len(), 10);
        assert!(s.retention(&key(), 1, 10, 1_100).truncated);
        assert!(!s.retention(&key(), 1, 500, 1_100).truncated);
    }

    #[test]
    fn the_trade_ring_keeps_the_most_recent_prints_newest_first() {
        let mut s = Store::new();
        for t in 0..(TRADE_RING + 50) {
            s.ingest(key(), trade(1_000 + t as u64, t as f64, 1.0));
        }
        let r = s.recent_trades(&key(), TRADE_RING + 100);
        assert_eq!(r.len(), TRADE_RING, "bounded");
        assert!(r[0].price > r[1].price, "newest first");
    }

    /// Per-product state is capped, evicting the least recently traded — the same discipline as
    /// MAX_BOOK_MARKETS. Nothing keyed on wire-controlled data may grow without limit.
    #[test]
    fn product_state_is_bounded_by_evicting_the_least_recently_traded() {
        let mut s = Store::new();
        for i in 0..(MAX_PRODUCTS + 10) {
            let k = Key {
                source_id: 1,
                channel: 0,
                instrument_id: i as u32,
            };
            s.ingest(k, trade(1_000 + i as u64, 10.0, 1.0));
        }
        assert_eq!(s.len(), MAX_PRODUCTS);
        assert_eq!(s.evicted(), 10);
        let oldest = Key {
            source_id: 1,
            channel: 0,
            instrument_id: 0,
        };
        // `now_secs` stays close to `oldest`'s own print (ts 1_000): far enough in the future to
        // close its bucket, but well inside one window of it, so an empty result can only mean the
        // product itself is gone — not that `rollup`'s own `now_secs` floor excluded merely-old data
        // from a product that is still tracked (see `rollup_is_bounded_by_now_secs_not_only_by_last_activity`).
        assert!(
            s.candles(&oldest, 1, 10, 1_010).is_empty(),
            "the coldest went first"
        );
    }

    /// A caller-supplied `0` granularity must not divide by zero; it is floored to one second.
    #[test]
    fn zero_granularity_is_treated_as_one_second_rather_than_panicking() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        let c = s.candles(&key(), 0, 10, 101);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].start, 100);
    }

    /// Serving must track `now_secs`, not merely the product's own last-activity watermark. A
    /// product that stopped trading long ago has nothing left to advance its own eviction floor, so
    /// without a lower bound tied to `now_secs` a caller querying the real "now" would still be
    /// served a stale hour of candles trailing that product's last print instead of an empty result.
    #[test]
    fn rollup_is_bounded_by_now_secs_not_only_by_last_activity() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        // No further prints for this product: nothing about its own state alone would ever evict
        // this bucket.
        let now = 100 + WINDOW_SECS + 500;
        let c = s.candles(&key(), 1, 10, now);
        assert!(
            c.is_empty(),
            "a print more than an hour before now_secs must not still be served"
        );
    }

    #[test]
    fn now_secs_excludes_the_not_yet_closed_current_second() {
        let mut s = Store::new();
        s.ingest(key(), trade(100, 10.0, 1.0));
        s.ingest(key(), trade(101, 11.0, 1.0));
        let c = s.candles(&key(), 1, 10, 101);
        assert_eq!(
            c.len(),
            1,
            "the bucket at now_secs itself has not closed yet"
        );
        assert_eq!(c[0].start, 100);
    }

    /// `oldest`/`newest` report the pre-limit span deliberately: two different `limit`s over the
    /// same data must report the same span even though only one of them is actually truncated.
    #[test]
    fn retention_oldest_and_newest_report_the_pre_limit_span() {
        let mut s = Store::new();
        for t in 0..100 {
            s.ingest(key(), trade(1_000 + t, 10.0, 1.0));
        }
        let r10 = s.retention(&key(), 1, 10, 1_100);
        assert_eq!(
            r10.oldest, 1_000,
            "pre-limit span, not narrowed to the 10 actually returned"
        );
        assert_eq!(r10.newest, 1_099);

        let r500 = s.retention(&key(), 1, 500, 1_100);
        assert_eq!(
            r500.oldest, 1_000,
            "identical span, regardless of a limit that doesn't bind"
        );
        assert_eq!(r500.newest, 1_099);
    }

    #[test]
    fn retention_for_an_unknown_key_reports_a_zero_width_window_at_now() {
        let s = Store::new();
        let r = s.retention(&key(), 1, 10, 12_345);
        assert_eq!(r.oldest, 12_345);
        assert_eq!(r.newest, 12_345);
        assert!(!r.truncated);
    }

    /// A product must not stay LRU-hot merely by being ingested if every such call is a rejected
    /// late print — otherwise an attacker could pin a spent identity in the cap by re-sending stale
    /// traffic for it alone, starving a real market's slot.
    #[test]
    fn a_product_fed_only_stale_prints_after_its_first_real_one_stays_cold() {
        let mut s = Store::new();
        let active = Key {
            source_id: 1,
            channel: 0,
            instrument_id: 0,
        };
        s.ingest(active, trade(10_000, 10.0, 1.0)); // active becomes the very first (coldest) product

        for i in 1..MAX_PRODUCTS {
            let k = Key {
                source_id: 1,
                channel: 0,
                instrument_id: i as u32,
            };
            s.ingest(k, trade(1_000 + i as u64, 10.0, 1.0));
        }
        assert_eq!(s.len(), MAX_PRODUCTS);

        // Feed `active` nothing but late (rejected) prints. If these bumped its LRU position it
        // would no longer be the coldest by the time the cap forces the eviction below.
        for _ in 0..5 {
            s.ingest(active, trade(10_000 - WINDOW_SECS - 100, 1.0, 1.0));
        }
        assert_eq!(s.late_drops(), 5);

        // One more distinct product forces a cardinality eviction; the coldest (`active`, untouched
        // by the stale contact above) must be the one that goes.
        let extra = Key {
            source_id: 1,
            channel: 0,
            instrument_id: MAX_PRODUCTS as u32,
        };
        s.ingest(extra, trade(1_000 + MAX_PRODUCTS as u64, 10.0, 1.0));

        // `now_secs` stays close to `active`'s own real print (ts 10_000), for the same reason as in
        // `product_state_is_bounded_by_evicting_the_least_recently_traded`: it must be `rollup`'s
        // eviction of the whole product we observe, not its `now_secs` floor excluding merely-old
        // data from a product that is in fact still tracked.
        assert!(
            s.candles(&active, 1, 10, 10_010).is_empty(),
            "the coldest product, left cold by the stale contact, was evicted"
        );
    }

    /// The O(1) budget check in `enforce_bucket_budget` rests on `buckets_total` matching the true
    /// sum of every product's bucket count, so every path that can add or remove a bucket must keep
    /// it in step. Drives each such path in turn and recomputes, mirroring
    /// `MboProcessor::buffered_total`'s test discipline in `ingest/processor.rs`.
    #[test]
    fn buckets_total_matches_the_recomputed_sum_across_mutations() {
        let mut s = Store::new();
        let recomputed = |s: &Store| s.products.values().map(|p| p.buckets.len()).sum::<usize>();

        // Fresh inserts across a couple of products.
        let a = Key {
            source_id: 1,
            channel: 0,
            instrument_id: 1,
        };
        let b = Key {
            source_id: 1,
            channel: 0,
            instrument_id: 2,
        };
        s.ingest(a, trade(100, 10.0, 1.0));
        s.ingest(a, trade(101, 10.0, 1.0));
        s.ingest(b, trade(100, 10.0, 1.0));
        assert_eq!(s.buckets_total, recomputed(&s), "after fresh inserts");

        // Widening an existing bucket must not change the count.
        s.ingest(a, trade(100, 11.0, 1.0));
        assert_eq!(s.buckets_total, recomputed(&s), "after widening");

        // This product's own window eviction.
        s.ingest(a, trade(100 + WINDOW_SECS + 10, 12.0, 1.0));
        assert_eq!(
            s.buckets_total,
            recomputed(&s),
            "after a product's own window eviction"
        );

        // Whole-product eviction via the MAX_PRODUCTS cardinality cap.
        for i in 0..MAX_PRODUCTS {
            let k = Key {
                source_id: 2,
                channel: 0,
                instrument_id: i as u32,
            };
            s.ingest(k, trade(1_000 + i as u64, 10.0, 1.0));
        }
        assert_eq!(
            s.buckets_total,
            recomputed(&s),
            "after MAX_PRODUCTS eviction"
        );
    }

    /// A burst of synthetic prints can fill many products' one-hour windows far faster than an hour
    /// of real trading — cheap to reach because the keys are wire-supplied — so the aggregate must
    /// cap independently of any single product's own window. Pre-fills several products directly
    /// with a realistic full `WINDOW_SECS` of buckets each (the state an hour of real trading would
    /// leave, built here without spending 3,600 real `ingest` calls per product) to reach the budget
    /// quickly, then exercises the real eviction path with one more `ingest` call.
    #[test]
    fn total_bucket_budget_evicts_whole_products_least_recently_traded() {
        let mut s = Store::new();

        let per_product = WINDOW_SECS as usize; // the real structural per-product cap
        let n = MAX_BUCKETS_ACROSS_PRODUCTS / per_product + 1; // enough to exceed the budget outright
        for i in 0..n {
            let k = Key {
                source_id: 1,
                channel: 0,
                instrument_id: i as u32,
            };
            let mut buckets = BTreeMap::new();
            for sec in 0..per_product as u64 {
                buckets.insert(
                    sec,
                    Bucket::opened_by(&Print {
                        ts_ns: 0,
                        price: 1.0,
                        size: 1.0,
                    }),
                );
            }
            s.touch_clock += 1;
            let clock = s.touch_clock;
            s.products.insert(
                k,
                Product {
                    buckets,
                    newest_seen: Some(per_product as u64 - 1),
                    ring: VecDeque::new(),
                    last_touch: clock,
                },
            );
            s.buckets_total += per_product;
        }
        assert!(
            s.buckets_total > MAX_BUCKETS_ACROSS_PRODUCTS,
            "fixture is deliberately over budget"
        );

        let coldest = Key {
            source_id: 1,
            channel: 0,
            instrument_id: 0,
        };
        let new_key = Key {
            source_id: 1,
            channel: 0,
            instrument_id: n as u32,
        };
        let evicted_before = s.evicted();
        s.ingest(new_key, trade(per_product as u64, 5.0, 1.0));

        assert!(
            s.buckets_total <= MAX_BUCKETS_ACROSS_PRODUCTS,
            "back under budget, got {}",
            s.buckets_total
        );
        assert!(
            s.evicted() > evicted_before,
            "the overflow evicted at least one whole product"
        );
        assert!(
            s.candles(&coldest, 1, 10, per_product as u64 + 1)
                .is_empty(),
            "the coldest product went first, not merely the newest bucket"
        );
        let recomputed: usize = s.products.values().map(|p| p.buckets.len()).sum();
        assert_eq!(
            s.buckets_total, recomputed,
            "accounting matches after the overflow eviction"
        );
    }

    // -------------------------------------------------------------------------------------------
    // forget_channel
    // -------------------------------------------------------------------------------------------

    /// Dropping a channel drops its history with it: a later `candles` query must come back empty,
    /// not serve a frozen window that looks live. Asserts a named survivor count too (below), not
    /// merely the drop itself — see that test for why a bare `is_empty()` here would not be enough
    /// on its own to prove `forget_channel` is scoped correctly.
    #[test]
    fn forgetting_a_channel_drops_its_history() {
        let mut s = Store::new();
        let dropped_key = Key {
            source_id: 3,
            channel: 10,
            instrument_id: 1,
        };
        s.ingest(dropped_key, trade(1_000, 10.0, 1.0));
        assert!(
            !s.candles(&dropped_key, 60, 10, 1_100).is_empty(),
            "fixture sanity: the print must be queryable before the drop"
        );

        let dropped = s.forget_channel(3, 10);
        assert_eq!(dropped, 1, "exactly the one product on that channel");
        assert!(
            s.candles(&dropped_key, 60, 10, 1_100).is_empty(),
            "history survived its channel being dropped"
        );
    }

    /// Only the named channel is dropped. Two peers, each differing from the dropped key on
    /// exactly one identity component: a **different channel, same source_id** (what a blanket
    /// clear would also spare, so this alone would not distinguish `forget_channel` from clearing
    /// everything) and — the fixture M3 exists for — a **different source_id, same channel id**.
    /// A source-id-blind filter (matching on `channel` alone) would wrongly drop the latter, and a
    /// blanket-clear implementation would drop both; only a correct `(source_id, channel)` filter
    /// spares both. Candle **count** (not merely presence) is asserted for each, so a
    /// coincidentally-non-empty peer can't paper over a partial over-drop.
    #[test]
    fn forgetting_a_channel_leaves_its_peers_intact() {
        let mut s = Store::new();
        let dropped_key = Key {
            source_id: 3,
            channel: 10,
            instrument_id: 1,
        };
        let peer_diff_channel = Key {
            source_id: 3,
            channel: 11,
            instrument_id: 1,
        };
        let peer_diff_source = Key {
            source_id: 7,
            channel: 10,
            instrument_id: 1,
        };
        s.ingest(dropped_key, trade(1_000, 10.0, 1.0));
        s.ingest(peer_diff_channel, trade(1_000, 20.0, 1.0));
        s.ingest(peer_diff_channel, trade(1_060, 21.0, 1.0));
        s.ingest(peer_diff_source, trade(1_000, 30.0, 1.0));
        s.ingest(peer_diff_source, trade(1_060, 31.0, 1.0));
        s.ingest(peer_diff_source, trade(1_120, 32.0, 1.0));

        s.forget_channel(3, 10);

        let peer_channel_candles = s.candles(&peer_diff_channel, 60, 10, 1_200);
        assert_eq!(
            peer_channel_candles.len(),
            2,
            "the untouched channel's full candle count must survive the peer's drop"
        );
        let peer_source_candles = s.candles(&peer_diff_source, 60, 10, 1_200);
        assert_eq!(
            peer_source_candles.len(),
            3,
            "a peer sharing the channel id under a different source_id must survive — a \
             source-id-blind filter would wrongly drop it"
        );
    }

    /// `buckets_total` is the accounting `enforce_bucket_budget`'s O(1) check rests on; every path
    /// that removes a product must keep it in step or the budget silently corrupts (see that
    /// field's doc). Drives `forget_channel` through the same recompute-and-compare discipline as
    /// `buckets_total_matches_the_recomputed_sum_across_mutations`, with a peer sharing the channel
    /// id under a different `source_id` so a source-id-blind filter would also miscount here.
    #[test]
    fn forget_channel_keeps_buckets_total_in_step() {
        let mut s = Store::new();
        let dropped_key = Key {
            source_id: 3,
            channel: 10,
            instrument_id: 1,
        };
        let peer_diff_channel = Key {
            source_id: 3,
            channel: 11,
            instrument_id: 1,
        };
        let peer_diff_source = Key {
            source_id: 7,
            channel: 10,
            instrument_id: 1,
        };
        s.ingest(dropped_key, trade(1_000, 10.0, 1.0));
        s.ingest(dropped_key, trade(1_100, 11.0, 1.0)); // a second bucket on the doomed product
        s.ingest(peer_diff_channel, trade(1_000, 20.0, 1.0));
        s.ingest(peer_diff_source, trade(1_000, 30.0, 1.0)); // same channel id, different source_id
        let before = s.buckets_total;

        let dropped = s.forget_channel(3, 10);
        assert_eq!(dropped, 1);

        assert_eq!(
            s.buckets_total,
            before - 2,
            "exactly the dropped product's two buckets must leave the budget"
        );
        let recomputed: usize = s.products.values().map(|p| p.buckets.len()).sum();
        assert_eq!(
            s.buckets_total, recomputed,
            "accounting must match the true sum after forgetting a channel"
        );
    }

    /// Forgetting a channel nothing tracks is a harmless no-op, reported honestly as zero rather
    /// than as an error — the reconciler calls this for every departing publisher, and a departing
    /// channel with no history yet (nothing traded) is the common case, not a fault.
    #[test]
    fn forgetting_an_untracked_channel_drops_nothing() {
        let mut s = Store::new();
        s.ingest(
            Key {
                source_id: 3,
                channel: 11,
                instrument_id: 1,
            },
            trade(1_000, 10.0, 1.0),
        );
        assert_eq!(s.forget_channel(3, 10), 0);
        assert_eq!(s.len(), 1, "the untouched product is still there");
    }
}
