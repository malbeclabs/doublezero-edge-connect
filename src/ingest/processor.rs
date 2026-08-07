//! Per-protocol frame processors: the [`FrameProcessor`] implementations the receiver's shared
//! driver dispatches to. Each owns its protocol state (reference-data state machine, sequence
//! tracker, warn-once flags, book state) and turns decoded frames into normalized `FeedMessage`s.
//!
//! - [`TobProcessor`] - Top-of-Book & Trades (`codec`, magic `0x445A`).
//! - [`MidpointProcessor`] - Midpoint (`codec_midpoint`, magic `0x4D44`).
//! - [`MboProcessor`] - Market-by-Order (`codec_mbo`, magic `0x4444`): reconstructs the L3 book
//!   in [`crate::ingest::book`] and re-serves it as full-state `depth` + `trade`.
//! - [`MbpProcessor`] - Market-by-Price (`codec_mbp`, magic `0x4442`): reconstructs the
//!   price-aggregated book in [`crate::ingest::pricebook`] and re-serves it as the incremental
//!   `book` product + `trade`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tracing::{debug, info, warn};

use crate::{
    ingest::{
        arbiter::{lock, Publisher},
        authority::MarketKey,
        book::{BookState, DeltaKind, DeltaOp, Level},
        codec::{apply_exponent, decode_frame, source_name, InstrumentDefinition, Message},
        codec_mbo, codec_mbp, codec_midpoint,
        pricebook::{
            BookDelta, DeltaOp as PriceDeltaOp, DeltaOutcome, Divergence, PriceBook,
            Status as BookStatus,
        },
        receiver::{FrameCtx, FrameProcessor, SeqCheck, SeqTracker},
        subscriber::{InstrumentDef, RefDataState},
    },
    metrics::metrics,
    model::{
        venue_arc, BookAction, BookChange, BookSide, DepthSnapshot, FeedMessage, NormalizedBook,
        NormalizedDepth, NormalizedInstrument, NormalizedMidpoint, NormalizedQuote,
        NormalizedTrade, Side,
    },
};

/// How many price levels per side a `depth` snapshot carries.
const DEPTH_LEVELS: usize = 10;

/// Minimum gap between two decode-error log lines from one processor.
const DECODE_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Rate limit for a warning that can fire **per datagram**. A decode error is per-frame, so a
/// `(group, port)` block that turns out to carry another protocol's traffic (several of the MBO/TOB
/// port blocks are inferred, not confirmed on-wire) would otherwise warn at full market-data rate.
#[derive(Default)]
struct WarnRateLimit {
    last: Option<Instant>,
    suppressed: u64,
}

impl WarnRateLimit {
    /// `Some(suppressed_since_the_last_line)` when the caller should log, `None` to stay quiet.
    fn allow(&mut self) -> Option<u64> {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|t| now.duration_since(t) < DECODE_WARN_INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Pre-resolved `dz_seq_events_total{venue, kind}` children (one per [`SeqCheck`] outcome) for a
/// single feed, so the per-frame hot path increments a cached counter instead of doing a label-map
/// lookup. The processor doesn't know its venue until the first frame (`ctx.venue`, fixed for the
/// feed's lifetime), so the children are bound lazily on first use.
#[derive(Default)]
struct SeqEvents {
    children: Option<[prometheus::IntCounter; 4]>,
}

impl SeqEvents {
    /// Index into `children`, matching the order they are resolved in below.
    fn index(check: &SeqCheck) -> usize {
        match check {
            SeqCheck::First => 0,
            SeqCheck::Ok => 1,
            SeqCheck::Reset => 2,
            SeqCheck::Stale => 3,
        }
    }

    fn record(&mut self, venue: &str, check: &SeqCheck) {
        let children = self.children.get_or_insert_with(|| {
            let m = metrics();
            [
                m.seq_events.with_label_values(&[venue, "first"]),
                m.seq_events.with_label_values(&[venue, "ok"]),
                m.seq_events.with_label_values(&[venue, "reset"]),
                m.seq_events.with_label_values(&[venue, "stale"]),
            ]
        });
        children[Self::index(check)].inc();
    }
}

/// Cap on the number of distinct publishers (source IPs) tracked by [`TobProcessor`]'s per-publisher
/// sequence map. The source IP comes from an *unauthenticated, spoofable* UDP datagram, so without a
/// bound an attacker who can inject into the multicast group could mint a fresh `SeqTracker` per
/// forged source IP and grow the map without limit (memory-exhaustion DoS). Real deployments have a
/// handful of mirrored publishers, so this is set far above that; once full, the least-recently-
/// inserted publisher is evicted (it simply re-anchors its sequence on its next frame).
const MAX_PUBLISHERS: usize = 256;

/// Per-publisher reference-data state, bounded exactly like the per-publisher sequence map.
///
/// `reset_count` is scoped to `(source_ip, group, port)`, so two publishers sharing a port block
/// carry unrelated reset counters: under one shared [`RefDataState`] either arm's restart clears the
/// other's instrument set, blanking both, since every emission path gates on a resolved definition.
/// The source IP is spoofable, so the map takes the same [`MAX_PUBLISHERS`] least-recently-inserted
/// eviction as the sequence map — an evicted publisher re-learns its definitions from the next
/// reference-data burst.
struct PerPublisher<D> {
    states: HashMap<IpAddr, RefDataState<D>>,
    /// Insertion order of `states` keys, oldest at the front, for the [`MAX_PUBLISHERS`] eviction.
    order: VecDeque<IpAddr>,
}

impl<D> Default for PerPublisher<D> {
    fn default() -> Self {
        // Not `#[derive(Default)]`: that would impose `D: Default`, which the definition types
        // don't (and needn't) implement - only the collections need defaulting.
        Self {
            states: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<D: InstrumentDef> PerPublisher<D> {
    /// The state for `publisher`, **creating it on first sight** — reference-data writes only. A
    /// read must use [`Self::def`]: minting an entry from the market-data path would let a forged-
    /// source flood evict the real publishers' definitions without ever sending reference data.
    fn get(&mut self, publisher: IpAddr) -> &mut RefDataState<D> {
        if !self.states.contains_key(&publisher) {
            while self.states.len() >= MAX_PUBLISHERS {
                match self.order.pop_front() {
                    Some(old) => {
                        self.states.remove(&old);
                    }
                    None => break,
                }
            }
            self.states.insert(publisher, RefDataState::new());
            self.order.push_back(publisher);
        }
        self.states.get_mut(&publisher).expect("just inserted")
    }

    /// `publisher`'s definition for `instrument_id`, creating nothing. Borrows only this field, so
    /// callers keep the disjoint borrows of their other state.
    fn def(&self, publisher: IpAddr, instrument_id: u32) -> Option<&D> {
        self.states.get(&publisher)?.definition(instrument_id)
    }
}

/// Insert or replace an instrument definition in the shared snapshot, warning if an existing
/// entry for the same `(venue, symbol)` carries different exponents. When one venue is served by
/// multiple feeds (e.g. Hyperliquid TOB + MBO), both write the same key; they are expected to
/// agree on precision, so a disagreement is a publisher inconsistency worth surfacing rather than
/// silently clobbering.
fn upsert_instrument(instruments: &crate::model::InstrumentSnapshot, inst: &NormalizedInstrument) {
    let key = (inst.venue.clone(), inst.symbol.clone());
    let mut map = crate::model::lock(instruments);
    if let Some(prev) = map.get(&key) {
        if prev.price_exponent != inst.price_exponent || prev.qty_exponent != inst.qty_exponent {
            warn!(
                venue = %inst.venue,
                symbol = %inst.symbol,
                prev_price_exp = prev.price_exponent,
                new_price_exp = inst.price_exponent,
                prev_qty_exp = prev.qty_exponent,
                new_qty_exp = inst.qty_exponent,
                "conflicting instrument definitions for the same (venue, symbol) across feeds; last writer wins"
            );
        }
    }
    map.insert(key, inst.clone());
}

/// Top-of-Book & Trades processor: drives the reference-data state machine on the refdata stream
/// and emits normalized quotes (gated per-instrument on a known definition) on the market-data
/// stream. Holds the per-channel sequence tracker used to drop stale/out-of-order quote frames.
pub struct TobProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<InstrumentDefinition>,
    /// Per-publisher, per-channel frame sequence tracker. Independent publishers mirror this feed
    /// onto one group sharing `channel_id=0`, so a single tracker would mark the slower publisher's
    /// frames stale and drop them before dedup; keying by source IP keeps each publisher's sequence
    /// state separate. Bounded to [`MAX_PUBLISHERS`] entries (the source IP is spoofable, so the map
    /// must not grow without limit); `seq_order` records insertion order for the eviction.
    seq: HashMap<IpAddr, SeqTracker>,
    /// Insertion order of `seq` keys, oldest at the front, for the [`MAX_PUBLISHERS`] eviction.
    seq_order: VecDeque<IpAddr>,
    /// Log the manifest `Valid=0` publisher workaround once, not on every (~1/s) manifest.
    warned_invalid_manifest: bool,
    /// Log an unregistered quote SourceID once, not on every quote.
    warned_source_mismatch: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
    /// Pre-resolved frame-sequence metric children (bound lazily on the first frame).
    seq_events: SeqEvents,
}

impl TobProcessor {
    pub fn new(emit_trades: bool) -> Self {
        Self {
            state: PerPublisher::default(),
            seq: HashMap::new(),
            seq_order: VecDeque::new(),
            warned_invalid_manifest: false,
            warned_source_mismatch: false,
            decode_warn: WarnRateLimit::default(),
            emit_trades,
            seq_events: SeqEvents::default(),
        }
    }

    /// The sequence tracker for `publisher`, creating it on first sight. The map is bounded to
    /// [`MAX_PUBLISHERS`]: when a *new* publisher would overflow it, the least-recently-inserted one
    /// is evicted first. Source IPs are spoofable, so this bound is what stops a forged-source flood
    /// from growing the map without limit; a legitimate publisher evicted under such a flood simply
    /// re-anchors (`SeqCheck::First`) on its next frame, with no data loss beyond a stale-check reset.
    fn seq_for(&mut self, publisher: IpAddr) -> &mut SeqTracker {
        if !self.seq.contains_key(&publisher) {
            while self.seq.len() >= MAX_PUBLISHERS {
                match self.seq_order.pop_front() {
                    Some(old) => {
                        self.seq.remove(&old);
                    }
                    None => break,
                }
            }
            self.seq.insert(publisher, SeqTracker::default());
            self.seq_order.push_back(publisher);
        }
        self.seq.get_mut(&publisher).expect("just inserted/present")
    }
}

impl FrameProcessor for TobProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx) {
        let (header, messages) = match decode_frame(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "decode error: {e}");
                }
                return;
            }
        };

        let handle_refdata = ctx.role.handles_refdata();
        let handle_quotes = ctx.role.handles_mktdata();

        if handle_refdata {
            self.state.get(ctx.publisher).on_frame(header.reset_count);
        }

        // Per edge-feed-spec, the frame Sequence Number is monotonically increasing per channel and
        // a `Reset Count` change signals a publisher reset. On the quote feed we drop only the stale
        // (out-of-order/replayed) frames - those whose sequence is below the last seen within the
        // same reset epoch - so an old datagram can never overwrite a fresher top-of-book. Forward
        // jumps are accepted without comment (the channel-0 sequence is global across groups, so
        // per-group gaps are expected, not loss).
        let quotes_fresh = if handle_quotes {
            let check = self.seq_for(ctx.publisher).check(
                header.channel_id,
                header.reset_count,
                header.sequence,
            );
            self.seq_events.record(ctx.venue, &check);
            match check {
                SeqCheck::Stale => {
                    debug!(
                        venue = ctx.venue,
                        channel = header.channel_id,
                        sequence = header.sequence,
                        "dropping stale/out-of-order quote frame (sequence below last seen)"
                    );
                    false
                }
                SeqCheck::Reset => {
                    info!(
                        venue = ctx.venue,
                        channel = header.channel_id,
                        reset_count = header.reset_count,
                        sequence = header.sequence,
                        "quote channel reset; sequence restarted"
                    );
                    true
                }
                SeqCheck::First | SeqCheck::Ok => true,
            }
        } else {
            true
        };

        for msg in messages {
            match msg {
                Message::ManifestSummary(m) if handle_refdata => {
                    // TEMP WORKAROUND: the live DZ Edge HL publisher currently emits
                    // ManifestSummary with Valid=0 (verified against edge-feed-spec: the
                    // Valid byte at message offset 5 is genuinely 0x00, not a decode bug),
                    // even though Quotes and the SOL InstrumentDefinition flow correctly.
                    // Per spec Valid=0 means "no established instrument set", which would
                    // keep RefDataState from ever reaching ready() and block all quotes.
                    // Until the publisher is fixed to emit Valid=1, force valid=true here so
                    // the bridge can consume the otherwise-healthy feed. REVISIT: drop this
                    // override and pass `m.valid` once the publisher manifest is corrected.
                    if !m.valid && !self.warned_invalid_manifest {
                        self.warned_invalid_manifest = true;
                        warn!(
                            manifest_seq = m.manifest_seq,
                            instrument_count = m.instrument_count,
                            "manifest Valid=0 from publisher; overriding to valid (temporary, logged once)"
                        );
                    }
                    self.state.get(ctx.publisher).on_manifest(
                        true,
                        m.manifest_seq,
                        m.instrument_count,
                    );
                }
                Message::InstrumentDefinition(d) if handle_refdata => {
                    let inst = NormalizedInstrument {
                        venue: venue_arc(ctx.venue),
                        symbol: d.symbol.clone(),
                        channel: header.channel_id as u32,
                        instrument_id: d.instrument_id,
                        price_exponent: d.price_exponent,
                        qty_exponent: d.qty_exponent,
                    };
                    // Update the shared snapshot so newly-connecting subscribers get this
                    // instrument before any quote.
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                Message::ChannelReset(ts) if handle_refdata => {
                    warn!(ts, "channel reset; discarding reference-data state");
                    // A channel reset belongs to the publisher that sent it, not the port block.
                    *self.state.get(ctx.publisher) = RefDataState::new();
                }
                Message::EndOfSession(ts) if handle_refdata => {
                    info!(ts, "end of session");
                }
                Message::Quote(q) if handle_quotes && quotes_fresh => {
                    // Per-instrument readiness: emit a quote as soon as we hold *this*
                    // instrument's definition, rather than gating on the whole set being
                    // complete (`state.ready()`). This still upholds the precision guarantee
                    // - we never emit a price without knowing its exponents, because the
                    // definition lookup below supplies them - but it removes the fragility of
                    // the all-or-nothing gate. Instrument definitions arrive in an infrequent
                    // burst (~every 8s on the live Phoenix feed) while quotes stream
                    // continuously, so a startup/reset race that left `defs` short of
                    // `expected_count` would otherwise wedge the feed: every quote dropped
                    // until a *full* burst landed within a single valid manifest epoch.
                    // Gating per instrument lets each symbol's quotes flow the moment its
                    // definition is known, independent of the others.
                    let Some(def) = self.state.def(ctx.publisher, q.instrument_id) else {
                        continue; // no definition for this instrument yet; drop until we have it
                    };
                    // This feed maps to a single venue (see feeds.rs), so instruments and quotes
                    // are tagged alike with it. Cross-check the wire SourceID against the source
                    // registry and warn once if it names a different venue - that means the feed
                    // table and the wire disagree about what this group carries.
                    if let Some(name) = source_name(q.source_id) {
                        if name != ctx.venue && !self.warned_source_mismatch {
                            self.warned_source_mismatch = true;
                            warn!(
                                source_id = q.source_id, registry_venue = name, feed_venue = %ctx.venue,
                                "quote SourceID maps to a venue different from this feed's venue (logged once)"
                            );
                        }
                    }
                    // Venue is the wire SourceID's registered venue (2 -> Phoenix); anything
                    // unregistered (the source_id 3 Hyperliquid superset incl. HIP-3 builder DEXs)
                    // falls back to the feed default (Hyperliquid). So venues are exactly
                    // Hyperliquid + Phoenix; the builder DEX, if any, stays in the symbol. Resolved
                    // once as `&'static str` so the dedup key is allocation-free, and it is
                    // publisher-independent (mirrors share a venue) so they dedup against each other.
                    let venue: &'static str = source_name(q.source_id).unwrap_or(ctx.venue);
                    let quote = NormalizedQuote {
                        venue: venue_arc(venue),
                        symbol: def.symbol.clone(),
                        bid: apply_exponent(q.bid_price_raw, def.price_exponent),
                        ask: apply_exponent(q.ask_price_raw, def.price_exponent),
                        bid_size: apply_exponent(q.bid_qty_raw as i64, def.qty_exponent),
                        ask_size: apply_exponent(q.ask_qty_raw as i64, def.qty_exponent),
                        bid_n: q.bid_n,
                        ask_n: q.ask_n,
                        source_ts_ns: q.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    // Cross-source dedup happens downstream in the shared arbiter: the per-(venue,
                    // instrument) source_ts latch-to-leader floor races this edge publisher against
                    // the other edge publishers and the public WS feeder for the tick, emitting only
                    // the leader. `ctx.emit` tags the quote with this datagram's source IP as the
                    // floor's leader identity, and the arbiter keys the BBO identity on the canonical
                    // bbo_hash (incl. the bid_n/ask_n carried on `quote`). (See `ingest::arbiter`.)
                    ctx.emit(FeedMessage::Quote(quote));
                }
                Message::Trade(t) if handle_quotes && quotes_fresh => {
                    // Same per-instrument precision gate as quotes: a trade is dropped until we
                    // hold its definition, so we never emit a price without knowing its exponents.
                    let Some(def) = self.state.def(ctx.publisher, t.instrument_id) else {
                        continue;
                    };
                    let venue: &'static str = source_name(t.source_id).unwrap_or(ctx.venue);
                    let trade = NormalizedTrade {
                        venue: venue_arc(venue),
                        symbol: def.symbol.clone(),
                        price: apply_exponent(t.trade_price_raw, def.price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, def.qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            def.qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    // The arbiter's windowed trade dedup (on trade_id) collapses any cross-source
                    // copy downstream; this feed only gates on whether it owns this venue's trades.
                    if self.emit_trades {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                _ => {}
            }
        }
    }
}

/// Midpoint processor: drives the reference-data state machine on the refdata stream and emits a
/// normalized mid price (gated per-instrument on a known definition) on the market-data stream.
/// Structurally parallel to [`TobProcessor`] but for the `0x4D44` sibling protocol.
pub struct MidpointProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<codec_midpoint::InstrumentDefinition>,
    seq: SeqTracker,
    warned_source_mismatch: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Pre-resolved frame-sequence metric children (bound lazily on the first frame).
    seq_events: SeqEvents,
}

impl Default for MidpointProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl MidpointProcessor {
    pub fn new() -> Self {
        Self {
            state: PerPublisher::default(),
            seq: SeqTracker::default(),
            warned_source_mismatch: false,
            decode_warn: WarnRateLimit::default(),
            seq_events: SeqEvents::default(),
        }
    }
}

impl FrameProcessor for MidpointProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx) {
        let (header, messages) = match codec_midpoint::decode_frame(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "midpoint decode error: {e}");
                }
                return;
            }
        };

        let handle_refdata = ctx.role.handles_refdata();
        let handle_mids = ctx.role.handles_mktdata();

        if handle_refdata {
            self.state.get(ctx.publisher).on_frame(header.reset_count);
        }

        // Same stale/out-of-order rejection as quotes: a midpoint is full state per instrument.
        let mids_fresh = if handle_mids {
            let check = self
                .seq
                .check(header.channel_id, header.reset_count, header.sequence);
            self.seq_events.record(ctx.venue, &check);
            !matches!(check, SeqCheck::Stale)
        } else {
            true
        };

        for msg in messages {
            match msg {
                codec_midpoint::Message::ManifestSummary(m) if handle_refdata => {
                    // Unlike the Top-of-Book HL publisher (which emits Valid=0 - see TobProcessor),
                    // we pass the Midpoint manifest's `valid` honestly; if the Midpoint publisher
                    // turns out to share that defect, apply the same override here.
                    self.state.get(ctx.publisher).on_manifest(
                        m.valid,
                        m.manifest_seq,
                        m.instrument_count,
                    );
                }
                codec_midpoint::Message::InstrumentDefinition(d) if handle_refdata => {
                    // A mid price has no size, so there is no qty exponent on the Midpoint feed;
                    // report qty_exponent = 0 in the shared snapshot (consumers ignore it for mids).
                    let inst = NormalizedInstrument {
                        venue: venue_arc(ctx.venue),
                        symbol: d.symbol.clone(),
                        channel: header.channel_id as u32,
                        instrument_id: d.instrument_id,
                        price_exponent: d.price_exponent,
                        qty_exponent: 0,
                    };
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                codec_midpoint::Message::EndOfSession(ts) if handle_refdata => {
                    info!(ts, "midpoint end of session");
                }
                codec_midpoint::Message::Midpoint(mp) if handle_mids && mids_fresh => {
                    let Some(def) = self.state.def(ctx.publisher, mp.instrument_id) else {
                        continue; // no definition yet; drop until we know precision
                    };
                    if let Some(name) = source_name(mp.source_id) {
                        if name != ctx.venue && !self.warned_source_mismatch {
                            self.warned_source_mismatch = true;
                            warn!(
                                source_id = mp.source_id, registry_venue = name, feed_venue = %ctx.venue,
                                "midpoint SourceID maps to a venue different from this feed's venue (logged once)"
                            );
                        }
                    }
                    let midpoint = NormalizedMidpoint {
                        venue: venue_arc(source_name(mp.source_id).unwrap_or(ctx.venue)),
                        symbol: def.symbol.clone(),
                        mid: apply_exponent(mp.mid_price_raw, def.price_exponent),
                        method: mp.method,
                        quality_flags: mp.quality_flags,
                        book_ts_ns: mp.book_ts,
                        compute_ts_ns: mp.compute_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    ctx.emit(FeedMessage::Midpoint(midpoint));
                }
                _ => {}
            }
        }
    }
}

/// Cap on the number of distinct instrument books [`MboProcessor`] tracks. The MBO `instrument_id`
/// is an unauthenticated, spoofable wire field, so without a bound a forged stream could mint a
/// `BookState` per distinct id and grow the map without limit (memory-exhaustion DoS) — the same
/// threat the [`MAX_PUBLISHERS`] cap addresses for the per-publisher sequence map. Real venues
/// carry far fewer instruments than this, so it never evicts in normal operation; once full, the
/// least-recently-inserted book is evicted (it simply re-syncs from the next snapshot).
const MAX_BOOKS: usize = 4096;

/// Market-by-Order processor: drives the reference-data state machine (refdata port), feeds order
/// deltas and the snapshot stream into a per-instrument [`BookState`] (mktdata + snapshot ports),
/// and emits a full-state `depth` snapshot whenever an instrument's top-N changes - plus `trade`
/// prints. The reconstructed book lives here so consumers never see raw deltas (PROTOCOL.md).
pub struct MboProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<codec_mbo::InstrumentDefinition>,
    /// One independent L3 book per `(publisher, instrument)`. Two publishers mirror the same feed but
    /// reconstruct from independent, instance-scoped per-instrument delta sequences (whose sequence
    /// spaces collide), so their books CANNOT be merged — each runs its own recovery state machine.
    /// The redundant publishers' resulting `depth` is collapsed downstream at the shared arbiter's
    /// latch-to-leader floor (see [`crate::ingest::arbiter`]), not here.
    books: HashMap<(IpAddr, u32), BookState>,
    /// Insertion order of `books` keys, oldest at the front, for the [`MAX_BOOKS`] eviction.
    books_order: VecDeque<(IpAddr, u32)>,
    /// Shared latest-depth map the WS server replays on connect.
    depth: DepthSnapshot,
    /// Last emitted top-N levels per `(publisher, instrument)`, so a book change that leaves the
    /// published top-N identical (deep-book churn) does not re-broadcast a duplicate full-state
    /// `depth`. Evicted in lockstep with `books` and cleared on `InstrumentReset`, so it can never
    /// outgrow the book map (its keys are always a subset of `books`' keys).
    last_top: HashMap<(IpAddr, u32), (Vec<Level>, Vec<Level>)>,
    /// The symbol each `(publisher, instrument)` last emitted `depth` under — the symbol the
    /// arbiter's depth floor actually LATCHED for that instrument. `InstrumentReset` clears the
    /// floor by this memo rather than the *current* definition, which can differ: a manifest epoch
    /// bump may reassign the id to another symbol, and clearing the new symbol would leave the
    /// wedged old-symbol entry latched. Written in `emit_depth`, evicted in lockstep with `books`,
    /// cleared on `EndOfSession` (the venue-wide clear covers everything), so its keys are always
    /// a subset of `books`' keys.
    emitted_symbol: HashMap<(IpAddr, u32), Arc<str>>,
    warned_source_mismatch: bool,
    /// One-shot guard for the manifest `Valid=0` override warning (see the handler).
    warned_invalid_manifest: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
}

impl MboProcessor {
    pub fn new(depth: DepthSnapshot, emit_trades: bool) -> Self {
        Self {
            state: PerPublisher::default(),
            books: HashMap::new(),
            books_order: VecDeque::new(),
            depth,
            last_top: HashMap::new(),
            emitted_symbol: HashMap::new(),
            warned_source_mismatch: false,
            warned_invalid_manifest: false,
            decode_warn: WarnRateLimit::default(),
            emit_trades,
        }
    }

    /// Get-or-create the [`BookState`] for `instrument_id`, **gated and bounded** — the two checks
    /// that keep the unauthenticated, spoofable wire `instrument_id` from growing memory without
    /// limit:
    ///
    /// 1. Returns `None` (creating no book) unless we already hold this instrument's definition. A
    ///    book for an undefined instrument can never emit `depth` ([`Self::emit_depth`] requires the
    ///    definition for precision), so it would be pure dead memory; this mirrors the per-instrument
    ///    definition gate the Top-of-Book / Midpoint quote paths already apply.
    /// 2. Bounds the map to [`MAX_BOOKS`] `(publisher, instrument)` pairs with least-recently-inserted
    ///    eviction, so even a flood of *defined* forged instrument_ids (the source IP is also
    ///    spoofable — the same threat the cap already addresses) can't grow it without limit. Eviction
    ///    also drops the evicted pair's `last_top` and (when no other publisher still serves that
    ///    symbol) the shared `depth` (WS replay) entry in lockstep, so neither sibling map outgrows
    ///    `books`. An evicted legitimate book simply re-syncs from the next snapshot.
    fn book_for(&mut self, instrument_id: u32, ctx: &FrameCtx) -> Option<&mut BookState> {
        // Gate 1: no definition → no book (and release the `state` borrow before touching `books`).
        self.state.def(ctx.publisher, instrument_id)?;
        let key = (ctx.publisher, instrument_id);
        if !self.books.contains_key(&key) {
            while self.books.len() >= MAX_BOOKS {
                match self.books_order.pop_front() {
                    Some(old) => {
                        self.books.remove(&old);
                        // Evict the two sibling maps keyed off this book in lockstep, or each would
                        // grow without limit while `books` stays bounded - the exact
                        // forged-`(publisher, instrument)`-flood vector `MAX_BOOKS` guards against.
                        // `last_top` by the same key; the shared `depth` (WS replay) map by
                        // (venue, symbol) — purge it ONLY when no other publisher still holds a book
                        // for the same instrument (else that publisher's depth would be wrongly
                        // dropped from replay; it self-heals via full-state otherwise).
                        self.last_top.remove(&old);
                        self.emitted_symbol.remove(&old);
                        let (old_pub, old_id) = old;
                        let symbol_still_served = self.books.keys().any(|(_p, i)| *i == old_id);
                        if !symbol_still_served {
                            // Resolved against the evicted book's OWN publisher: reference data is
                            // per publisher, so two arms can map one id to different symbols.
                            if let Some(def) = self.state.def(old_pub, old_id) {
                                crate::model::lock(&self.depth)
                                    .remove(&(venue_arc(ctx.venue), def.symbol.clone()));
                            }
                        }
                    }
                    None => break,
                }
            }
            self.books.insert(key, BookState::default());
            self.books_order.push_back(key);
        }
        self.books.get_mut(&key)
    }

    /// Build and broadcast a full-state `depth` snapshot for one instrument, updating the shared
    /// replay map. No-op unless the book is synced and the instrument's precision is known.
    fn emit_depth(&mut self, instrument_id: u32, ctx: &FrameCtx) {
        let key = (ctx.publisher, instrument_id);
        let Some(book) = self.books.get(&key) else {
            return;
        };
        if !book.is_synced() {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return; // precision unknown; don't emit a book we can't scale
        };
        let (bids_raw, asks_raw) = book.top_levels(DEPTH_LEVELS);
        // Suppress a re-broadcast when this publisher's published top-N is byte-for-byte unchanged: a
        // delta deep in the book (outside the top-N) still flips `changed`, but re-sending an
        // identical full-state `depth` is pure duplication. Compare the raw integer levels
        // (pre-scaling) by reference - no clone. (Cross-publisher dedup is the arbiter floor's job;
        // this only collapses one publisher's own consecutive identical top-N.)
        if matches!(self.last_top.get(&key), Some((b, a)) if *b == bids_raw && *a == asks_raw) {
            return;
        }
        let scale = |levels: &[(i64, u64)]| -> Vec<[f64; 2]> {
            levels
                .iter()
                .map(|&(p, q)| {
                    [
                        apply_exponent(p, def.price_exponent),
                        apply_exponent(q as i64, def.qty_exponent),
                    ]
                })
                .collect()
        };
        let depth = NormalizedDepth {
            venue: venue_arc(ctx.venue),
            symbol: def.symbol.clone(),
            bids: scale(&bids_raw),
            asks: scale(&asks_raw),
            source_ts_ns: book.last_event_ts(),
            recv_ts_ns: ctx.recv_ts_ns,
            kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
            ws_send_ts_ns: 0, // stamped by the WS server just before send
        };
        // Record the published top-N, moving the raw vectors in (no clone), so the next identical
        // book state suppresses.
        self.last_top.insert(key, (bids_raw, asks_raw));
        // Remember the symbol this key's depth latched the floor under (clone only when it
        // actually changes — i.e. once, or on an id→symbol remap), for the InstrumentReset clear.
        if self.emitted_symbol.get(&key) != Some(&def.symbol) {
            let symbol = def.symbol.clone();
            self.emitted_symbol.insert(key, symbol);
        }
        // The shared WS-replay map is written by the arbiter on the floor's admit decision (so it
        // holds the leader's broadcast book, not a dropped non-leader's), NOT here — emitting
        // pre-floor would record a book that may never reach the wire. The processor still purges
        // this map on book eviction (see `book_for`).
        ctx.emit(FeedMessage::Depth(depth));
    }
}

impl FrameProcessor for MboProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx) {
        let (header, messages) = match codec_mbo::decode_frame(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "mbo decode error: {e}");
                }
                return;
            }
        };

        if ctx.role.handles_refdata() {
            self.state.get(ctx.publisher).on_frame(header.reset_count);
        }

        // Instruments whose book changed this frame; depth is emitted once per frame per instrument
        // (coalescing many order events into a single full-state snapshot). BTreeSet gives
        // deterministic ascending instrument_id order across frames touching multiple instruments.
        let mut changed: BTreeSet<u32> = BTreeSet::new();

        for msg in messages {
            match msg {
                codec_mbo::Message::ManifestSummary(m) => {
                    // Same TEMP WORKAROUND as TobProcessor: the live DZ Edge HL publisher emits
                    // ManifestSummary with Valid=0 on the MBO feed too (verified against a real
                    // capture — see tests/fixtures/PROVENANCE.md). Per spec Valid=0 means "no
                    // established instrument set", which keeps RefDataState from ever resolving a
                    // definition and so blocks ALL depth (`book_for` gates on the definition). Force
                    // valid=true so the otherwise-healthy feed produces depth. REVISIT: drop this
                    // override and pass `m.valid` once the publisher manifest is corrected.
                    if !m.valid && !self.warned_invalid_manifest {
                        self.warned_invalid_manifest = true;
                        warn!(
                            manifest_seq = m.manifest_seq,
                            instrument_count = m.instrument_count,
                            "mbo manifest Valid=0 from publisher; overriding to valid (temporary, logged once)"
                        );
                    }
                    self.state.get(ctx.publisher).on_manifest(
                        true,
                        m.manifest_seq,
                        m.instrument_count,
                    );
                }
                codec_mbo::Message::InstrumentDefinition(d) => {
                    let inst = NormalizedInstrument {
                        venue: venue_arc(ctx.venue),
                        symbol: d.symbol.clone(),
                        channel: header.channel_id as u32,
                        instrument_id: d.instrument_id,
                        price_exponent: d.price_exponent,
                        qty_exponent: d.qty_exponent,
                    };
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                codec_mbo::Message::EndOfSession(ts) => {
                    info!(ts, "mbo end of session");
                    // Session boundary: the venue may restart its event clock and sequences, so
                    // drop this publisher's books to `Recovering` and clear the venue's latched
                    // depth-floor entries — a post-session `source_ts` below the old high-water
                    // would otherwise be dropped as stale forever (no full-state self-heal
                    // rescues a latched floor). Idempotent, so the duplicate copies arriving on
                    // the other ports are harmless no-ops (deliberately not role-gated: an extra
                    // clear also backs up a lost copy).
                    //
                    // SCOPE TRAP: `books` holds only THIS publisher's books (one processor per
                    // receiver task) while the floor cleared below is venue-wide and shared. A
                    // mirror that loses its own EndOfSession datagram keeps a `Synced` book and
                    // can re-latch the cleared floor at the old high-water, wedging the venue's
                    // depth until that mirror resets on its own. Closing it needs a per-venue
                    // session epoch shared across the receiver tasks.
                    for book in self.books.values_mut() {
                        book.on_end_of_session();
                    }
                    self.last_top.clear();
                    self.emitted_symbol.clear();
                    lock(ctx.arbiter).reset_depth_floor_for_venue(ctx.venue, "end_of_session");
                }
                codec_mbo::Message::OrderAdd(o) => {
                    let op = DeltaOp {
                        seq: o.per_instrument_seq,
                        mktdata_seq: header.sequence,
                        ts: o.enter_ts,
                        kind: DeltaKind::Add {
                            order_id: o.order_id,
                            is_bid: o.side == codec_mbo::SIDE_BID,
                            price_raw: o.price_raw,
                            qty_raw: o.qty_raw,
                        },
                    };
                    if let Some(book) = self.book_for(o.instrument_id, ctx) {
                        if book.on_delta(op) {
                            changed.insert(o.instrument_id);
                        }
                    }
                }
                codec_mbo::Message::OrderCancel(o) => {
                    let op = DeltaOp {
                        seq: o.per_instrument_seq,
                        mktdata_seq: header.sequence,
                        ts: o.ts,
                        kind: DeltaKind::Cancel {
                            order_id: o.order_id,
                        },
                    };
                    if let Some(book) = self.book_for(o.instrument_id, ctx) {
                        if book.on_delta(op) {
                            changed.insert(o.instrument_id);
                        }
                    }
                }
                codec_mbo::Message::OrderExecute(o) => {
                    let op = DeltaOp {
                        seq: o.per_instrument_seq,
                        mktdata_seq: header.sequence,
                        ts: o.ts,
                        kind: DeltaKind::Execute {
                            order_id: o.order_id,
                            exec_qty_raw: o.exec_qty_raw,
                            full_fill: o.exec_flags & 0x01 != 0,
                        },
                    };
                    if let Some(book) = self.book_for(o.instrument_id, ctx) {
                        if book.on_delta(op) {
                            changed.insert(o.instrument_id);
                        }
                    }
                    // An execution is also a public trade print; emit it like a Top-of-Book trade.
                    if let Some(def) = self.state.def(ctx.publisher, o.instrument_id) {
                        let trade = NormalizedTrade {
                            venue: venue_arc(ctx.venue),
                            symbol: def.symbol.clone(),
                            price: apply_exponent(o.exec_price_raw, def.price_exponent),
                            size: apply_exponent(o.exec_qty_raw as i64, def.qty_exponent),
                            aggressor_side: Side::from_code(o.aggressor_side),
                            trade_id: o.trade_id,
                            cumulative_volume: 0.0,
                            source_ts_ns: o.ts,
                            recv_ts_ns: ctx.recv_ts_ns,
                            kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                            ws_send_ts_ns: 0,
                        };
                        if self.emit_trades {
                            ctx.emit(FeedMessage::Trade(trade));
                        }
                    }
                }
                codec_mbo::Message::Trade(t) => {
                    let Some(def) = self.state.def(ctx.publisher, t.instrument_id) else {
                        continue;
                    };
                    let trade = NormalizedTrade {
                        venue: venue_arc(source_name(t.source_id).unwrap_or(ctx.venue)),
                        symbol: def.symbol.clone(),
                        price: apply_exponent(t.trade_price_raw, def.price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, def.qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            def.qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0,
                    };
                    if let Some(name) = source_name(t.source_id) {
                        if name != ctx.venue && !self.warned_source_mismatch {
                            self.warned_source_mismatch = true;
                            warn!(source_id = t.source_id, registry_venue = name, feed_venue = %ctx.venue,
                                  "mbo SourceID maps to a venue different from this feed's venue (logged once)");
                        }
                    }
                    if self.emit_trades {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                codec_mbo::Message::InstrumentReset(r) => {
                    // Drop the stale suppression entry so the first depth after the book re-syncs is
                    // always published (and its timestamps are fresh), never suppressed against the
                    // pre-reset top-N. Per-publisher: only this publisher's book is resetting.
                    self.last_top.remove(&(ctx.publisher, r.instrument_id));
                    // The re-snapshot may anchor at a `source_ts` below the latched floor (e.g. the
                    // venue reset this instrument's clock); clear the `(venue, symbol)` floor entry
                    // so the post-reset depth re-opens the tick. The symbol is resolved in
                    // safest-first order:
                    //   1. `emitted_symbol` — the symbol this publisher's depth actually LATCHED
                    //      the floor under. The *current* definition can disagree: a manifest
                    //      epoch bump may have remapped the id to another symbol, and clearing the
                    //      new symbol would leave the wedged old-symbol entry latched.
                    //   2. The current definition — right whenever ids are venue-stable (this
                    //      publisher just never emitted depth for the id, e.g. the mirror latched
                    //      the floor).
                    //   3. Venue-wide — the definition can be transiently missing even for a
                    //      symbol with a latched entry (RefDataState clears all defs on a channel
                    //      reset / manifest bump), and a missing definition must not silently skip
                    //      the clear: fall back to the safe over-approximation (a spurious clear
                    //      self-heals; a skipped one can leave the exact permanent wedge this
                    //      hatch exists to remove).
                    let latched_symbol = self
                        .emitted_symbol
                        .get(&(ctx.publisher, r.instrument_id))
                        .cloned()
                        .or_else(|| {
                            self.state
                                .def(ctx.publisher, r.instrument_id)
                                .map(|d| d.symbol.clone())
                        });
                    let mut arb = lock(ctx.arbiter);
                    match latched_symbol {
                        Some(symbol) => {
                            arb.reset_depth_floor_for_symbol(ctx.venue, &symbol, "instrument_reset")
                        }
                        None => arb.reset_depth_floor_for_venue(ctx.venue, "instrument_reset"),
                    }
                    drop(arb);
                    // Reset the existing book directly — NOT via `book_for`, whose definition gate
                    // would skip the reset in the same transient-no-definition window as above
                    // (leaving a stale `Synced` book whose old sequences/event clock then reject
                    // the post-reset re-snapshot). A reset for a book we never built needs nothing.
                    if let Some(book) = self.books.get_mut(&(ctx.publisher, r.instrument_id)) {
                        book.on_instrument_reset(r.new_anchor_seq);
                    }
                }
                codec_mbo::Message::SnapshotBegin(s) => {
                    if let Some(book) = self.book_for(s.instrument_id, ctx) {
                        book.on_snapshot_begin(
                            s.snapshot_id,
                            s.anchor_seq,
                            s.total_orders,
                            s.last_instrument_seq,
                        );
                    }
                }
                codec_mbo::Message::SnapshotOrder(s) => {
                    // SnapshotOrder carries only the snapshot_id, not the instrument id; route it to
                    // whichever of THIS publisher's books is currently assembling that snapshot.
                    // snapshot_id is monotonic per (channel, instrument) - not globally unique, and
                    // certainly not across publishers - but the spec forbids interleaving snapshot
                    // groups across instruments per channel, so at most one of this publisher's books
                    // is `building` at a time. Restricting to `ctx.publisher` keeps a SnapshotOrder
                    // from leaking into the other publisher's same-snapshot_id building book.
                    for ((_p, _id), book) in self
                        .books
                        .iter_mut()
                        .filter(|((p, _), _)| *p == ctx.publisher)
                    {
                        book.on_snapshot_order(
                            s.snapshot_id,
                            s.order_id,
                            s.side == codec_mbo::SIDE_BID,
                            s.price_raw,
                            s.qty_raw,
                        );
                    }
                }
                codec_mbo::Message::SnapshotEnd(s) => {
                    if let Some(book) = self.book_for(s.instrument_id, ctx) {
                        if book.on_snapshot_end(s.anchor_seq, s.snapshot_id) {
                            changed.insert(s.instrument_id);
                        }
                    }
                }
                // BatchBoundary is an emission-coalescing hint; we already emit once per frame.
                codec_mbo::Message::BatchBoundary(_, _) | codec_mbo::Message::Heartbeat => {}
                codec_mbo::Message::Other(_) => {}
            }
        }

        for instrument_id in changed {
            self.emit_depth(instrument_id, ctx);
        }
    }
}

/// Cap on distinct `(publisher, channel, instrument)` books one Market-by-Price receiver tracks. The
/// wire `channel_id`/`instrument_id` and the datagram source IP are all unauthenticated and
/// spoofable, so this bounds a forged stream exactly as [`MAX_BOOKS`] does for the order-keyed
/// processor. Nothing may be sized off the instrument *count*: ids are sequential in today's
/// captures but a ticker hash would spread them sparsely across the whole `u32`.
const MAX_PRICE_BOOKS: usize = 4096;

/// Cap on distinct `(publisher, channel)` pairs whose reset counter and open snapshot group are
/// tracked. Both key components are unauthenticated wire data, so an unbounded map is a
/// memory-exhaustion vector; two arms across a handful of shards sit far below this.
const MAX_CHANNEL_KEYS: usize = 256;

/// Deltas [`MbpProcessor`] holds buffered **across every book** before the overflow policy fires —
/// distinct from `pricebook`'s per-book `MAX_BUFFERED_DELTAS` (2^18), which with [`MAX_PRICE_BOOKS`]
/// books (2^12 × 2^18 = 2^30) can never bind first. The spec's own cold-start worst case is ~30 M
/// buffered messages / ~1.4 GB, so an unbounded total is a documented way to lose the process. On
/// overflow the instrument holding the most buffered data is dropped and marked `Gap`, recovering on
/// its next snapshot like any other `Gap` instrument; sustained overflow means the publisher's
/// snapshot period is too long for this host's memory budget, which is why it is counted.
const MAX_BUFFERED_DELTAS_ACROSS_BOOKS: usize = 1 << 20;

/// One reconstructed book's identity within a receiver: `(publisher, channel, instrument)`.
type PriceBookKey = (IpAddr, u8, u32);

/// The snapshot group currently open on one `(publisher, channel)`.
///
/// Publishers must not interleave two groups within a channel, and `SnapshotLevel` carries no
/// instrument id — so the open group is what ROUTES a level. `snapshot_id` only validates
/// membership: it is monotonic per `(channel, instrument)`, so two instruments routinely share a
/// value within one rotation and routing on it would cross their books.
#[derive(Debug, Clone, Copy)]
struct OpenGroup {
    instrument_id: u32,
    snapshot_id: u32,
}

/// Market-by-Price processor: drives reference data per publisher, feeds level deltas and the
/// snapshot stream into a [`PriceBook`] per `(publisher, channel, instrument)`, and emits the
/// incremental `book` product plus `trade` prints.
pub struct MbpProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<codec_mbp::InstrumentDefinition>,
    /// One independent book per `(publisher, channel, instrument)`. Two arms mirror one feed but
    /// their per-instrument delta series are unrelated by construction, so their books can never be
    /// merged — which arm reaches the wire is the authority gate's decision, downstream.
    books: HashMap<PriceBookKey, PriceBook>,
    /// Insertion order of `books` keys, oldest at the front, for the [`MAX_PRICE_BOOKS`] eviction.
    books_order: VecDeque<PriceBookKey>,
    /// The open snapshot group per `(publisher, channel)` — see [`OpenGroup`].
    open: HashMap<(IpAddr, u8), OpenGroup>,
    /// Last `Reset Count` seen per `(publisher, channel)`, compared for inequality only (see
    /// [`Self::note_reset_count`]).
    last_reset: HashMap<(IpAddr, u8), u8>,
    /// Insertion order of the `last_reset`/`open` keys, for the [`MAX_CHANNEL_KEYS`] eviction.
    channel_order: VecDeque<(IpAddr, u8)>,
    /// Deltas buffered across every book, kept in step by [`Self::with_book`] so the budget check is
    /// O(1) rather than a sweep over `books` per datagram — which would cost most during exactly the
    /// cold start the budget exists for.
    buffered_total: usize,
    /// Last `Ready`-ness reported per book, so health reaches the authority on transitions only.
    /// Evicted in lockstep with `books`, so its keys are always a subset of `books`' keys.
    health_reported: HashMap<PriceBookKey, bool>,
    /// Reused buffer for the levels a `BookClear` removed, so the clear path never allocates.
    cleared: Vec<(u8, i64)>,
    /// One-shot guard for the manifest `Valid=0` override warning (see the handler).
    warned_invalid_manifest: bool,
    warned_source_mismatch: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
}

impl MbpProcessor {
    pub fn new(emit_trades: bool) -> Self {
        Self {
            state: PerPublisher::default(),
            books: HashMap::new(),
            books_order: VecDeque::new(),
            open: HashMap::new(),
            last_reset: HashMap::new(),
            channel_order: VecDeque::new(),
            buffered_total: 0,
            health_reported: HashMap::new(),
            cleared: Vec::new(),
            warned_invalid_manifest: false,
            warned_source_mismatch: false,
            decode_warn: WarnRateLimit::default(),
            emit_trades,
        }
    }

    /// One instrument's `(price, qty)` exponents, or `None` while its definition is unknown — the
    /// precision-before-price gate, copied out so the `state` borrow ends here.
    fn exponents(&self, publisher: IpAddr, instrument_id: u32) -> Option<(i8, i8)> {
        self.state
            .def(publisher, instrument_id)
            .map(|d| (d.price_exponent, d.qty_exponent))
    }

    /// Record this frame's `Reset Count` for `(publisher, channel)`, returning the previous one.
    /// Bounded to [`MAX_CHANNEL_KEYS`] with least-recently-inserted eviction; an evicted live
    /// publisher simply re-anchors its baseline on its next frame (reporting no reset for it).
    fn note_reset_count(&mut self, publisher: IpAddr, channel: u8, reset_count: u8) -> Option<u8> {
        let key = (publisher, channel);
        if !self.last_reset.contains_key(&key) {
            while self.last_reset.len() >= MAX_CHANNEL_KEYS {
                match self.channel_order.pop_front() {
                    Some(old) => {
                        self.last_reset.remove(&old);
                        self.open.remove(&old);
                    }
                    None => break,
                }
            }
            self.channel_order.push_back(key);
        }
        self.last_reset.insert(key, reset_count)
    }

    /// Get-or-create the book for one `(publisher, channel, instrument)`, **gated and bounded** the
    /// same way [`MboProcessor::book_for`] is: no book without a resolved definition (it could never
    /// emit, so it would be dead memory), and [`MAX_PRICE_BOOKS`] with least-recently-inserted
    /// eviction. Returns the key rather than the book: every mutation runs through
    /// [`Self::with_book`], which is what keeps [`Self::buffered_total`] honest.
    fn ensure_book(
        &mut self,
        ctx: &FrameCtx,
        channel: u8,
        instrument_id: u32,
    ) -> Option<PriceBookKey> {
        self.state.def(ctx.publisher, instrument_id)?;
        let key = (ctx.publisher, channel, instrument_id);
        if !self.books.contains_key(&key) {
            while self.books.len() >= MAX_PRICE_BOOKS {
                match self.books_order.pop_front() {
                    Some(old) => self.forget_book(&old),
                    None => break,
                }
            }
            self.books.insert(key, PriceBook::new());
            self.books_order.push_back(key);
        }
        Some(key)
    }

    /// Drop one book and every map keyed off it, keeping the buffer total in step. Does not touch
    /// `books_order`: the eviction path has already popped the key, and the reset path clears the
    /// whole channel's keys in one pass.
    fn forget_book(&mut self, key: &PriceBookKey) {
        if let Some(book) = self.books.remove(key) {
            self.buffered_total = self.buffered_total.saturating_sub(book.buffered_len());
        }
        self.health_reported.remove(key);
        if self
            .open
            .get(&(key.0, key.1))
            .is_some_and(|g| g.instrument_id == key.2)
        {
            self.open.remove(&(key.0, key.1));
        }
    }

    /// Run `f` against one book, keeping [`Self::buffered_total`] in step with that book's buffer.
    /// **Every** path that can change a buffer must go through here.
    fn with_book<R>(
        &mut self,
        key: &PriceBookKey,
        f: impl FnOnce(&mut PriceBook) -> R,
    ) -> Option<R> {
        let book = self.books.get_mut(key)?;
        let before = book.buffered_len();
        let out = f(book);
        let after = book.buffered_len();
        self.buffered_total = (self.buffered_total + after).saturating_sub(before);
        Some(out)
    }

    /// §4.5: hold the cross-instrument buffer inside [`MAX_BUFFERED_DELTAS_ACROSS_BOOKS`] by dropping
    /// the largest buffer (`drop_buffer` marks that instrument `Gap` in the same step) until back
    /// under budget. Finding the largest is O(books), which is fine because overflow is rare and the
    /// check that gates it is O(1). Never takes the channel down: every other instrument keeps
    /// streaming and the dropped one recovers on its next snapshot.
    fn enforce_buffer_budget(&mut self, ctx: &FrameCtx) {
        while self.buffered_total > MAX_BUFFERED_DELTAS_ACROSS_BOOKS {
            let largest = self
                .books
                .iter()
                .max_by_key(|(_, b)| b.buffered_len())
                .filter(|(_, b)| b.buffered_len() > 0)
                .map(|(k, _)| *k);
            // Nothing left to drop: the total disagrees with the books, so stop rather than spin.
            let Some(key) = largest else { return };
            self.with_book(&key, |b| b.drop_buffer());
            metrics()
                .mbp_buffer_overflows
                .with_label_values(&[ctx.venue])
                .inc();
            self.report_health(ctx, &key, false);
        }
    }

    /// Report one book's `Ready`-ness for its market, but only when it changed: an unhealthy arm
    /// loses the market to its peer, so this is a transition signal rather than a per-frame one.
    fn report_health(&mut self, ctx: &FrameCtx, key: &PriceBookKey, healthy: bool) {
        if self.health_reported.get(key) == Some(&healthy) {
            return;
        }
        self.health_reported.insert(*key, healthy);
        let market: MarketKey = (venue_arc(ctx.venue), key.1 as u32, key.2);
        lock(ctx.arbiter).set_book_health(&market, Publisher::Edge(key.0), healthy);
    }

    /// §4.9: discard everything a `Reset Count` change invalidated for one `(publisher, channel)` —
    /// its books and their open snapshot group, plus that publisher's reference data, whose
    /// `reset_count` epoch just ended. Routed from any port, since the change can be seen on market
    /// data first. `RefDataState` is per publisher rather than per channel, so a sharded publisher's
    /// reset clears every channel's definitions — an over-approximation that self-heals on the next
    /// reference-data burst.
    fn on_channel_reset(&mut self, ctx: &FrameCtx, channel: u8, reset_count: u8) {
        let keys: Vec<PriceBookKey> = self
            .books
            .keys()
            .copied()
            .filter(|(p, c, _)| *p == ctx.publisher && *c == channel)
            .collect();
        for key in &keys {
            // Unhealthy before forgetting: `forget_book` drops the memo this reads.
            self.report_health(ctx, key, false);
            self.forget_book(key);
        }
        self.books_order
            .retain(|(p, c, _)| !(*p == ctx.publisher && *c == channel));
        self.open.remove(&(ctx.publisher, channel));
        self.state.get(ctx.publisher).on_frame(reset_count);
        metrics()
            .mbp_channel_resets
            .with_label_values(&[ctx.venue])
            .inc();
    }

    /// Count what a delta did. `Overflow` (the per-book level cap: a malformed or forged stream) is
    /// deliberately its own series rather than a gap — the cause and the resulting status both
    /// differ, and merging them would read a hostile book as a lossy network.
    fn record_outcome(&self, venue: &str, outcome: &DeltaOutcome) {
        let m = metrics();
        match outcome {
            DeltaOutcome::Duplicate => m.mbp_duplicate_deltas.with_label_values(&[venue]).inc(),
            DeltaOutcome::Overflow => m.mbp_level_overflows.with_label_values(&[venue]).inc(),
            DeltaOutcome::Applied {
                divergence: Some(d),
            } => m
                .mbp_divergence
                .with_label_values(&[venue, divergence_label(*d)])
                .inc(),
            _ => {}
        }
    }

    /// Emit one instrument's accumulated changes as a single `book` batch. Gated on a `Ready` book
    /// and a resolved definition (precision before price). `last: true` because one frame is one
    /// logical event per instrument — cross-instrument atomicity is not promised, so per-frame
    /// batching is correct.
    fn emit_book(&self, ctx: &FrameCtx, channel: u8, instrument_id: u32, changes: Vec<BookChange>) {
        if changes.is_empty() {
            return;
        }
        self.send_book(ctx, channel, instrument_id, changes, false);
    }

    /// Emit a full re-baseline for one instrument: `Clear{Both}` then every level it now holds.
    /// `changes[0].action == Clear` is what re-baselines a consumer (the `snapshot` flag is
    /// advisory), so this is a batch rather than a distinct message type.
    fn emit_rebaseline(&self, ctx: &FrameCtx, channel: u8, instrument_id: u32) {
        let Some(book) = self.books.get(&(ctx.publisher, channel, instrument_id)) else {
            return;
        };
        let Some((price_exp, qty_exp)) = self.exponents(ctx.publisher, instrument_id) else {
            return;
        };
        let level = |side: BookSide, price_raw: i64, qty_raw: u64| BookChange {
            action: BookAction::Update,
            side,
            price: apply_exponent(price_raw, price_exp),
            size: apply_exponent(qty_raw as i64, qty_exp),
        };
        let mut changes = vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
        }];
        changes.extend(book.bids().map(|(p, l)| level(BookSide::Bid, p, l.qty_raw)));
        changes.extend(book.asks().map(|(p, l)| level(BookSide::Ask, p, l.qty_raw)));
        self.send_book(ctx, channel, instrument_id, changes, true);
    }

    /// The one place a `book` reaches the arbiter: resolves the display symbol and the book's event
    /// clock, and refuses to publish a book that is not `Ready`.
    fn send_book(
        &self,
        ctx: &FrameCtx,
        channel: u8,
        instrument_id: u32,
        changes: Vec<BookChange>,
        snapshot: bool,
    ) {
        let Some(book) = self.books.get(&(ctx.publisher, channel, instrument_id)) else {
            return;
        };
        if book.status() != BookStatus::Ready {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return; // precision unknown; don't emit prices we can't scale
        };
        ctx.emit(FeedMessage::Book(NormalizedBook {
            venue: venue_arc(ctx.venue),
            symbol: def.symbol.clone(),
            channel: channel as u32,
            instrument_id,
            changes,
            snapshot,
            last: true,
            source_ts_ns: book.last_event_ts(),
            recv_ts_ns: ctx.recv_ts_ns,
            kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
            ws_send_ts_ns: 0, // stamped by the WS server just before send
        }));
    }
}

/// The wire book `Side` mapped to the published side. Only `SIDE_ASK` is distinguished, matching
/// [`PriceBook`]'s own apply.
fn book_side(side: u8) -> BookSide {
    if side == codec_mbp::SIDE_ASK {
        BookSide::Ask
    } else {
        BookSide::Bid
    }
}

/// `BookClear`'s side, a value space of its own (it extends `Side` with `Both`).
fn clear_book_side(clear_side: u8) -> BookSide {
    match clear_side {
        codec_mbp::CLEAR_SIDE_ASK => BookSide::Ask,
        codec_mbp::CLEAR_SIDE_BOTH => BookSide::Both,
        _ => BookSide::Bid,
    }
}

/// Stable, low-cardinality `kind` label for `dz_mbp_divergence_total`.
fn divergence_label(d: Divergence) -> &'static str {
    match d {
        Divergence::NewOnPresentLevel => "new_on_present_level",
        Divergence::ChangeOnAbsentLevel => "change_on_absent_level",
        Divergence::DeleteWithQuantity => "delete_with_quantity",
        Divergence::ZeroQuantityWithoutDelete => "zero_quantity_without_delete",
    }
}

impl FrameProcessor for MbpProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx) {
        let (header, messages) = match codec_mbp::decode_frame(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "mbp decode error: {e}");
                }
                return;
            }
        };
        // The channel comes from this codec's own frame header rather than `FrameCtx`: `drive` is
        // protocol-agnostic and would have to decode a header it has no magic for.
        let channel = header.channel_id;

        if ctx.role.handles_refdata() {
            self.state.get(ctx.publisher).on_frame(header.reset_count);
        }
        // §4.9: a reset is any CHANGE of `Reset Count` — `!=`, never `>`, so the `255 -> 0` wrap is
        // not silently ignored while deltas keep applying against discarded publisher state.
        if self
            .note_reset_count(ctx.publisher, channel, header.reset_count)
            .is_some_and(|prev| prev != header.reset_count)
        {
            self.on_channel_reset(ctx, channel, header.reset_count);
        }

        // Wire changes per instrument, emitted once per frame; a `BTreeMap` gives deterministic
        // ascending-id order across a multi-instrument frame, matching `MboProcessor`'s `BTreeSet`.
        let mut accum: BTreeMap<u32, Vec<BookChange>> = BTreeMap::new();
        // Instruments touched since the previous `BatchBoundary`, and since the frame started (for
        // the health sweep). Both are frame-scoped: the publisher and channel are fixed per datagram.
        let mut since_boundary: BTreeSet<u32> = BTreeSet::new();
        let mut touched: BTreeSet<u32> = BTreeSet::new();
        // Moved out so the `&mut self` book calls below can borrow it; put back before returning.
        let mut cleared = std::mem::take(&mut self.cleared);

        for msg in messages {
            match msg {
                codec_mbp::Message::ManifestSummary(m) => {
                    // Same TEMP WORKAROUND as the sibling processors: a live publisher emitting
                    // `Valid=0` would keep `RefDataState` from ever resolving a definition, and
                    // every emission path gates on one — so the whole venue would go dark. Force
                    // valid=true. REVISIT: pass `m.valid` once publishers are corrected.
                    if !m.valid && !self.warned_invalid_manifest {
                        self.warned_invalid_manifest = true;
                        warn!(
                            manifest_seq = m.manifest_seq,
                            instrument_count = m.instrument_count,
                            "mbp manifest Valid=0 from publisher; overriding to valid (temporary, logged once)"
                        );
                    }
                    self.state.get(ctx.publisher).on_manifest(
                        true,
                        m.manifest_seq,
                        m.instrument_count,
                    );
                }
                codec_mbp::Message::InstrumentDefinition(d) => {
                    let inst = NormalizedInstrument {
                        venue: venue_arc(ctx.venue),
                        symbol: d.symbol.clone(),
                        channel: channel as u32,
                        instrument_id: d.instrument_id,
                        price_exponent: d.price_exponent,
                        qty_exponent: d.qty_exponent,
                    };
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                codec_mbp::Message::LevelUpdate(l) => {
                    let Some((price_exp, qty_exp)) = self.exponents(ctx.publisher, l.instrument_id)
                    else {
                        continue;
                    };
                    let Some(key) = self.ensure_book(ctx, channel, l.instrument_id) else {
                        continue;
                    };
                    let op = PriceDeltaOp {
                        seq: l.per_instrument_seq,
                        mktdata_seq: header.sequence,
                        ts: l.ts,
                        delta: BookDelta::Level {
                            side: l.side,
                            price_raw: l.price_raw,
                            qty_raw: l.qty_raw,
                            order_count: l.order_count,
                            level_flags: l.level_flags,
                            action: l.action,
                        },
                    };
                    touched.insert(l.instrument_id);
                    since_boundary.insert(l.instrument_id);
                    let Some(outcome) = self.with_book(&key, |b| b.on_delta(op, &mut cleared))
                    else {
                        continue;
                    };
                    self.record_outcome(ctx.venue, &outcome);
                    if matches!(outcome, DeltaOutcome::Applied { .. }) {
                        // Quantity alone decides the action, exactly as the apply does: `0` removes
                        // the level, anything else states its complete resulting state.
                        accum.entry(l.instrument_id).or_default().push(BookChange {
                            action: if l.qty_raw == 0 {
                                BookAction::Delete
                            } else {
                                BookAction::Update
                            },
                            side: book_side(l.side),
                            price: apply_exponent(l.price_raw, price_exp),
                            size: if l.qty_raw == 0 {
                                0.0
                            } else {
                                apply_exponent(l.qty_raw as i64, qty_exp)
                            },
                        });
                    }
                }
                codec_mbp::Message::BookClear(c) => {
                    let Some((price_exp, _)) = self.exponents(ctx.publisher, c.instrument_id)
                    else {
                        continue;
                    };
                    let Some(key) = self.ensure_book(ctx, channel, c.instrument_id) else {
                        continue;
                    };
                    let op = PriceDeltaOp {
                        seq: c.per_instrument_seq,
                        mktdata_seq: header.sequence,
                        ts: c.ts,
                        delta: BookDelta::Clear {
                            clear_side: c.clear_side,
                            scope: c.scope,
                            from_price_raw: c.from_price_raw,
                        },
                    };
                    touched.insert(c.instrument_id);
                    since_boundary.insert(c.instrument_id);
                    let Some(outcome) = self.with_book(&key, |b| b.on_delta(op, &mut cleared))
                    else {
                        continue;
                    };
                    self.record_outcome(ctx.venue, &outcome);
                    if !matches!(outcome, DeltaOutcome::Applied { .. }) {
                        continue;
                    }
                    let changes = accum.entry(c.instrument_id).or_default();
                    if c.scope == codec_mbp::SCOPE_ENTIRE_SIDE {
                        changes.push(BookChange {
                            action: BookAction::Clear,
                            side: clear_book_side(c.clear_side),
                            price: 0.0,
                            size: 0.0,
                        });
                    } else {
                        // The wire `Clear` carries no price bound, so a from-price clear is
                        // published as the exact levels it removed. A whole-side `Clear` would tell
                        // the consumer to drop levels this book still holds, and the two would
                        // diverge silently with every sequence check passing.
                        changes.extend(cleared.iter().map(|&(side, price_raw)| BookChange {
                            action: BookAction::Delete,
                            side: book_side(side),
                            price: apply_exponent(price_raw, price_exp),
                            size: 0.0,
                        }));
                    }
                }
                codec_mbp::Message::SnapshotBegin(s) => {
                    let Some(key) = self.ensure_book(ctx, channel, s.instrument_id) else {
                        continue;
                    };
                    touched.insert(s.instrument_id);
                    let accepted = self
                        .with_book(&key, |b| {
                            b.on_snapshot_begin(
                                s.snapshot_id,
                                s.anchor_seq,
                                s.total_levels,
                                s.last_instrument_seq,
                                s.depth_bound,
                            )
                        })
                        .unwrap_or(false);
                    let group = (ctx.publisher, channel);
                    if accepted {
                        self.open.insert(
                            group,
                            OpenGroup {
                                instrument_id: s.instrument_id,
                                snapshot_id: s.snapshot_id,
                            },
                        );
                    } else if self
                        .open
                        .get(&group)
                        .is_some_and(|g| g.instrument_id != s.instrument_id)
                    {
                        // A refused begin for a DIFFERENT instrument than the one assembling means
                        // the publisher interleaved groups: close the route so its levels are
                        // orphaned and counted rather than landing in the open instrument's book.
                        // A refused re-begin for the same instrument leaves the route alone — the
                        // book deliberately keeps assembling (see `PriceBook::on_snapshot_begin`).
                        self.open.remove(&group);
                    }
                }
                codec_mbp::Message::SnapshotLevel(l) => {
                    // §4.1: routed by the OPEN GROUP, never by `snapshot_id` — that is monotonic per
                    // `(channel, instrument)`, so two instruments routinely share a value within one
                    // rotation and routing on it sends one's levels into the other's book.
                    let route = self
                        .open
                        .get(&(ctx.publisher, channel))
                        .filter(|g| g.snapshot_id == l.snapshot_id)
                        .copied();
                    let Some(group) = route else {
                        metrics()
                            .mbp_orphan_snapshot_levels
                            .with_label_values(&[ctx.venue])
                            .inc();
                        continue;
                    };
                    let key = (ctx.publisher, channel, group.instrument_id);
                    self.with_book(&key, |b| {
                        b.on_snapshot_level(
                            l.snapshot_id,
                            l.side,
                            l.price_raw,
                            l.qty_raw,
                            l.order_count,
                            l.level_flags,
                        )
                    });
                }
                codec_mbp::Message::SnapshotEnd(e) => {
                    let group = (ctx.publisher, channel);
                    if !self
                        .open
                        .get(&group)
                        .is_some_and(|g| g.instrument_id == e.instrument_id)
                    {
                        // A stray end for an instrument that is not the one assembling. Dropping it
                        // is the whole action: the open group belongs to another instrument and must
                        // keep assembling.
                        debug!(
                            venue = ctx.venue,
                            channel,
                            instrument_id = e.instrument_id,
                            "mbp SnapshotEnd for an instrument with no open group"
                        );
                        continue;
                    }
                    self.open.remove(&group);
                    let key = (ctx.publisher, channel, e.instrument_id);
                    touched.insert(e.instrument_id);
                    let installed = self
                        .with_book(&key, |b| b.on_snapshot_end(e.anchor_seq, e.snapshot_id))
                        .unwrap_or(false);
                    if installed {
                        // The re-baseline replaces everything accumulated for this instrument so
                        // far, and goes out here so a delta later in the same frame follows it.
                        accum.remove(&e.instrument_id);
                        self.emit_rebaseline(ctx, channel, e.instrument_id);
                    }
                }
                codec_mbp::Message::InstrumentReset(r) => {
                    // Reset the existing book directly, NOT via `ensure_book`, whose definition gate
                    // would skip the reset in a transient no-definition window and leave a stale
                    // `Ready` book whose sequences then reject the post-reset snapshot.
                    let key = (ctx.publisher, channel, r.instrument_id);
                    if self
                        .with_book(&key, |b| b.on_instrument_reset(r.new_anchor_seq))
                        .is_some()
                    {
                        accum.remove(&r.instrument_id);
                        self.report_health(ctx, &key, false);
                    }
                    // The book dropped any group it was assembling, so the route goes with it.
                    if self
                        .open
                        .get(&(ctx.publisher, channel))
                        .is_some_and(|g| g.instrument_id == r.instrument_id)
                    {
                        self.open.remove(&(ctx.publisher, channel));
                    }
                }
                codec_mbp::Message::EndOfSession(ts) => {
                    info!(ts, channel, "mbp end of session");
                    // §4.7: per-arm and per-channel — the shard whose session ended. Dropping every
                    // publisher's books (as the order-keyed processor does) would tear down a live
                    // peer arm's published book; reporting each market unhealthy is what hands
                    // authority to that peer instead.
                    let keys: Vec<PriceBookKey> = self
                        .books
                        .keys()
                        .copied()
                        .filter(|(p, c, _)| *p == ctx.publisher && *c == channel)
                        .collect();
                    for key in keys {
                        self.with_book(&key, |b| b.on_end_of_session());
                        self.report_health(ctx, &key, false);
                        accum.remove(&key.2);
                    }
                    self.open.remove(&(ctx.publisher, channel));
                }
                codec_mbp::Message::BatchBoundary(_) => {
                    // The crossed-book consistency point: within a batch the inside market may
                    // legitimately cross. Observability only — it must never change status or
                    // discard a book. An instrument holding corrupt state is repaired by its next
                    // snapshot on exactly the schedule it would have been anyway.
                    for id in std::mem::take(&mut since_boundary) {
                        if self
                            .books
                            .get(&(ctx.publisher, channel, id))
                            .is_some_and(|b| b.crossed())
                        {
                            metrics().mbp_crossed.with_label_values(&[ctx.venue]).inc();
                        }
                    }
                }
                codec_mbp::Message::Trade(t) => {
                    let Some(def) = self.state.def(ctx.publisher, t.instrument_id) else {
                        continue;
                    };
                    let trade = NormalizedTrade {
                        venue: venue_arc(source_name(t.source_id).unwrap_or(ctx.venue)),
                        symbol: def.symbol.clone(),
                        price: apply_exponent(t.trade_price_raw, def.price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, def.qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            def.qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0,
                    };
                    if let Some(name) = source_name(t.source_id) {
                        if name != ctx.venue && !self.warned_source_mismatch {
                            self.warned_source_mismatch = true;
                            warn!(source_id = t.source_id, registry_venue = name, feed_venue = %ctx.venue,
                                  "mbp SourceID maps to a venue different from this feed's venue (logged once)");
                        }
                    }
                    if self.emit_trades {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                codec_mbp::Message::Heartbeat(_) | codec_mbp::Message::Other(_) => {}
            }
        }

        cleared.clear();
        self.cleared = cleared;
        self.enforce_buffer_budget(ctx);
        for (instrument_id, changes) in accum {
            self.emit_book(ctx, channel, instrument_id, changes);
        }
        for id in touched {
            let key = (ctx.publisher, channel, id);
            let Some(healthy) = self
                .books
                .get(&key)
                .map(|b| b.status() == BookStatus::Ready)
            else {
                continue;
            };
            self.report_health(ctx, &key, healthy);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tokio::sync::broadcast;

    use std::net::IpAddr;

    use super::{
        upsert_instrument, MboProcessor, MbpProcessor, TobProcessor, WarnRateLimit,
        MAX_BUFFERED_DELTAS_ACROSS_BOOKS, MAX_CHANNEL_KEYS, MAX_PRICE_BOOKS,
    };
    use crate::{
        ingest::{
            arbiter::{lock, Arbiter, Publisher, SharedArbiter},
            codec_mbo::{
                tests::{
                    enc_end_of_session, enc_instrument_reset, enc_order_add, enc_snapshot_begin,
                    enc_snapshot_end, frame,
                },
                InstrumentReset, OrderAdd, SnapshotBegin, SnapshotEnd, MSG_INSTRUMENT_DEFINITION,
                MSG_MANIFEST_SUMMARY, SIDE_ASK, SIDE_BID,
            },
            codec_mbp::{self, tests as mbp_wire, SIDE_ASK as MBP_ASK, SIDE_BID as MBP_BID},
            pricebook::{BookDelta, DeltaOp as PriceDeltaOp, Status as BookStatus},
            receiver::{FrameCtx, FrameProcessor, PortRole},
        },
        metrics::metrics,
        model::{
            BookAction, BookSide, DepthSnapshot, FeedMessage, NormalizedBook, NormalizedInstrument,
        },
    };

    // The quote latch-to-leader floor and the trade windowed dedup now live in the shared
    // `ingest::arbiter` (lifted out of `TobProcessor` so the multicast processors and the WS feeder
    // converge on one floor per (venue, symbol)). Their unit coverage — leader latch, non-leader
    // drop, stale-tick drop, capacity bound, source_ts==0 bypass, the bbo_hash identity incl.
    // bid_n/ask_n, and the public-loses-to-edge backstop — lives in `arbiter::tests`.

    /// A decode error is per-datagram, so the warning must collapse to one line per interval and
    /// report how many it swallowed - a mis-inferred port block carrying another protocol's traffic
    /// would otherwise log at market-data rate.
    #[test]
    fn decode_warn_rate_limit_collapses_a_burst_and_counts_it() {
        let mut w = WarnRateLimit::default();
        assert_eq!(w.allow(), Some(0), "first error logs immediately");
        assert_eq!(w.allow(), None, "second within the interval is suppressed");
        assert_eq!(w.allow(), None);
        // Simulate the interval elapsing; the next line carries the suppressed count.
        w.last = None;
        assert_eq!(w.allow(), Some(2));
        assert_eq!(w.allow(), None, "count resets after being reported");
    }

    /// The per-publisher sequence map must stay bounded under a flood of distinct (spoofable) source
    /// IPs, evicting the oldest first — otherwise a forged-source flood grows it without limit.
    #[test]
    fn tob_seq_map_is_bounded_under_publisher_flood() {
        use super::MAX_PUBLISHERS;
        use std::net::{IpAddr, Ipv4Addr};

        let mut p = TobProcessor::new(true);
        let ip = |i: u32| IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i)); // 10.x.y.z
        let flood = (MAX_PUBLISHERS as u32) + 50;
        for i in 0..flood {
            let _ = p.seq_for(ip(i));
        }
        assert!(
            p.seq.len() <= MAX_PUBLISHERS,
            "seq map must stay bounded, got {}",
            p.seq.len()
        );
        // The oldest publishers were evicted; the most-recent one is still tracked.
        assert!(
            p.seq.contains_key(&ip(flood - 1)),
            "newest publisher retained"
        );
        assert!(!p.seq.contains_key(&ip(0)), "oldest publisher evicted");
    }

    /// The per-publisher reference-data map carries a full instrument set per entry, so it needs the
    /// same [`MAX_PUBLISHERS`] bound as the sequence map — the source IP is spoofable. A *read* must
    /// mint nothing, or a market-data flood from forged sources would evict the real publishers'
    /// definitions without ever sending reference data.
    #[test]
    fn refdata_state_map_is_bounded_and_reads_mint_nothing() {
        use std::net::{IpAddr, Ipv4Addr};

        use super::{PerPublisher, MAX_PUBLISHERS};
        use crate::ingest::codec::InstrumentDefinition;

        let mut m: PerPublisher<InstrumentDefinition> = PerPublisher::default();
        let ip = |i: u32| IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i));
        let flood = (MAX_PUBLISHERS as u32) + 50;
        for i in 0..flood {
            let _ = m.get(ip(i));
        }
        assert!(
            m.states.len() <= MAX_PUBLISHERS,
            "refdata map must stay bounded, got {}",
            m.states.len()
        );
        assert!(m.states.contains_key(&ip(flood - 1)), "newest retained");
        assert!(!m.states.contains_key(&ip(0)), "oldest evicted");

        let before = m.states.len();
        assert!(
            m.def(ip(flood + 1), 7).is_none(),
            "unseen publisher has no definition"
        );
        assert_eq!(m.states.len(), before, "a read must not create an entry");
    }

    /// Two publishers share one port block and differ only by source IP. `reset_count` is
    /// per-publisher state, so one arm's restart must not clear the other arm's instrument set —
    /// which would blank both arms until the next refdata burst, since every emission path gates on
    /// a resolved definition. Driven through `on_datagram` so it pins the wiring, not just the map.
    #[test]
    fn refdata_reset_is_scoped_to_the_publisher_that_reset() {
        use std::net::{IpAddr, Ipv4Addr};

        /// Rewrite a frame's `reset_count` (frame-header byte 21) to simulate a publisher restart.
        fn with_reset_count(mut f: Vec<u8>, n: u8) -> Vec<u8> {
            f[21] = n;
            f
        }

        let pub_a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);
        let ctx_for = |publisher: IpAddr, role: PortRole| {
            let mut c = make_ctx(&arbiter, &instruments, role);
            c.publisher = publisher;
            c
        };
        let burst = frame(&[
            enc_manifest_summary(1, 1),
            enc_instrument_def(0, "INST-0", 1),
        ]);
        let anchor = |sid: u32| {
            frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: sid,
                    last_instrument_seq: 0,
                    ts: 0,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: sid,
                }),
            ])
        };
        for publisher in [pub_a, pub_b] {
            proc.on_datagram(&burst, &ctx_for(publisher, PortRole::Combined));
            proc.on_datagram(&anchor(1), &ctx_for(publisher, PortRole::Snapshot));
        }
        let _ = drain_depth_ts(&mut rx);

        // A restarts: its `reset_count` bumps on A's frames only, clearing A's definitions. An
        // empty frame, so the clear isn't immediately undone by the burst that would follow it.
        proc.on_datagram(
            &with_reset_count(frame(&[]), 1),
            &ctx_for(pub_a, PortRole::Refdata),
        );

        // A is dark until its next burst (no definition -> no book -> no depth) ...
        proc.on_datagram(
            &frame(&[add(1, 100, 7000)]),
            &ctx_for(pub_a, PortRole::Mktdata),
        );
        assert!(
            drain_depth_ts(&mut rx).is_empty(),
            "A's own definitions clear on its reset"
        );

        // ... but B, which never reset, keeps streaming.
        proc.on_datagram(
            &frame(&[add(1, 101, 8000)]),
            &ctx_for(pub_b, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![8000],
            "B's definitions must survive A's restart"
        );
    }

    /// Encode a ManifestSummary wire message (24 bytes total, valid=true).
    ///
    /// Body layout matches `codec_mbo::decode_message` offsets:
    ///   +0 channel_id (u8), +1 valid (u8), +2..+4 pad,
    ///   +4 manifest_seq (u16le), +6..+8 pad,
    ///   +8 instrument_count (u32le), +12 ts (u64le).
    fn enc_manifest_summary(manifest_seq: u16, instrument_count: u32) -> Vec<u8> {
        let mut out = vec![MSG_MANIFEST_SUMMARY, 24, 0, 0]; // 4-byte hdr + 20-byte body
        out.push(0u8); // body+0: channel_id
        out.push(1u8); // body+1: valid = true
        out.extend_from_slice(&[0u8; 2]); // body+2..+4: pad
        out.extend_from_slice(&manifest_seq.to_le_bytes()); // body+4..+6
        out.extend_from_slice(&[0u8; 2]); // body+6..+8: pad
        out.extend_from_slice(&instrument_count.to_le_bytes()); // body+8..+12
        out.extend_from_slice(&0u64.to_le_bytes()); // body+12..+20: ts
        out
    }

    /// Encode an InstrumentDefinition wire message (80 bytes total, exponents=0).
    ///
    /// Body layout matches `codec_mbo::decode_message` offsets:
    ///   +0 instrument_id (u32le), +4 symbol (16 B NUL-padded),
    ///   +20..+37 pad, +37 price_exponent (i8), +38 qty_exponent (i8),
    ///   +39..+74 pad, +74 manifest_seq (u16le).
    /// Total: 4 (hdr) + 76 (body) = 80 bytes = sizes::INSTRUMENT_DEFINITION.
    fn enc_instrument_def(id: u32, symbol: &str, manifest_seq: u16) -> Vec<u8> {
        let mut out = vec![MSG_INSTRUMENT_DEFINITION, 80, 0, 0];
        out.extend_from_slice(&id.to_le_bytes()); // body+0..+4
        let mut sym = [0u8; 16];
        let sb = symbol.as_bytes();
        sym[..sb.len().min(16)].copy_from_slice(&sb[..sb.len().min(16)]);
        out.extend_from_slice(&sym); // body+4..+20
        out.extend_from_slice(&[0u8; 17]); // body+20..+37: pad
        out.push(0u8); // body+37: price_exponent = 0
        out.push(0u8); // body+38: qty_exponent = 0
        out.extend_from_slice(&[0u8; 35]); // body+39..+74: pad
        out.extend_from_slice(&manifest_seq.to_le_bytes()); // body+74..+76
                                                            // 4 + 4 + 16 + 17 + 1 + 1 + 35 + 2 = 80 bytes total.
        out
    }

    /// The single-publisher source IP `make_ctx` stamps on every frame, so book-map keys in these
    /// tests are `(TEST_PUB, instrument_id)` (the MBO books re-key by publisher).
    const TEST_PUB: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));

    fn make_ctx<'a>(
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
    ) -> FrameCtx<'a> {
        FrameCtx {
            venue: "TV",
            arbiter,
            instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role,
            publisher: TEST_PUB,
        }
    }

    /// Drain all available `Depth` messages and return the numeric instrument ids
    /// encoded in their symbol field (`"INST-{id}"`).
    fn drain_depth_ids(rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>) -> Vec<u32> {
        let mut ids = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Depth(d) = &*m {
                ids.push(d.symbol.trim_start_matches("INST-").parse::<u32>().unwrap());
            }
        }
        ids
    }

    /// The live DZ Edge HL publisher emits MBO `ManifestSummary` with Valid=0 (confirmed against a
    /// real capture). Honoring it would clear every definition, so `book_for` would find none, the
    /// snapshot would never assemble, and NO depth would ever emit — the MBO feed would be silent in
    /// production. `MboProcessor` overrides Valid=0 to true, mirroring `TobProcessor`. This pins that:
    /// fed a Valid=0 manifest + definition + empty-book anchor + one delta, depth still flows. The
    /// Valid=1 goldens the other MBO tests use never exercised this path (that's why it shipped).
    #[test]
    fn mbo_manifest_valid_zero_is_overridden_so_depth_flows() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);

        // Refdata: a Valid=0 manifest (the live publisher's quirk) + the BTC definition under it.
        let mut manifest = enc_manifest_summary(5, 1);
        manifest[5] = 0; // body+1 is the `valid` byte; force the live-feed Valid=0 case
        proc.on_datagram(
            &frame(&[manifest, enc_instrument_def(0, "INST-0", 5)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        // Empty-book anchor (anchor_seq=0, last_instrument_seq=0), then a contiguous delta (seq 1).
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 1,
                    last_instrument_seq: 0,
                    ts: 1,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 1,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &frame(&[enc_order_add(&OrderAdd {
                instrument_id: 0,
                source_id: 0,
                side: SIDE_BID,
                order_flags: 0,
                per_instrument_seq: 1,
                order_id: 100,
                enter_ts: 10,
                price_raw: 100,
                qty_raw: 5,
            })]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let ids = drain_depth_ids(&mut rx);
        assert!(
            ids.contains(&0),
            "no BTC depth — a Valid=0 manifest blocked precision (the override is missing)"
        );
    }

    /// Drain all available `Depth` messages and return their `source_ts_ns`.
    fn drain_depth_ts(rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>) -> Vec<u64> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Depth(d) = &*m {
                out.push(d.source_ts_ns);
            }
        }
        out
    }

    /// Set up an MBO processor with instrument 0 defined and synced via an empty-book anchor
    /// snapshot (which emits the initial `depth(source_ts=0)`), for the floor-reset tests.
    fn synced_mbo_proc(
        arbiter: &SharedArbiter,
        instruments: &crate::model::InstrumentSnapshot,
    ) -> MboProcessor {
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);
        proc.on_datagram(
            &frame(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &make_ctx(arbiter, instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 1,
                    last_instrument_seq: 0,
                    ts: 1,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 1,
                }),
            ]),
            &make_ctx(arbiter, instruments, PortRole::Snapshot),
        );
        proc
    }

    /// One bid `OrderAdd` for instrument 0 at per-instrument `seq`, stamped `ts`.
    fn add(seq: u32, order_id: u64, ts: u64) -> Vec<u8> {
        enc_order_add(&OrderAdd {
            instrument_id: 0,
            source_id: 0,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: seq,
            order_id,
            enter_ts: ts,
            price_raw: 100,
            qty_raw: 5,
        })
    }

    /// `EndOfSession` clears the venue's latched depth-floor entries AND drops the books to
    /// `Recovering` (the session-reset escape hatch, #66): after the next session's re-snapshot, a
    /// depth whose `source_ts` is BELOW the pre-session high-water — the venue restarted its clock
    /// (and its sequences: the post-boundary delta seq restarts at 1 too) — is re-admitted instead
    /// of being dropped as stale forever (there is no full-state self-heal while the floor stays
    /// latched).
    #[test]
    fn mbo_end_of_session_unwedges_depth_floor() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(&frame(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
        proc.on_datagram(&frame(&[add(2, 101, 100)]), &mkt); // stale-clock tick -> dropped (the wedge)
        proc.on_datagram(&frame(&[enc_end_of_session(6000)]), &mkt); // floor cleared, book -> Recovering
                                                                     // New session: re-snapshot (empty anchor; the fresh book's depth(0) re-opens the cleared
                                                                     // floor), then a restarted-seq, restarted-clock delta.
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 2,
                    last_instrument_seq: 0,
                    ts: 2,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 2,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(&frame(&[add(1, 102, 50)]), &mkt); // new-session tick below the old high-water

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 0, 50],
            "the pre-reset lower tick (100) is dropped; after EndOfSession the floor re-opens"
        );
    }

    /// `EndOfSession` is a FEED-level boundary even though it arrives per publisher: publisher A's
    /// copy resets publisher B's book too, so B's still-in-flight old-session tail is buffered
    /// (book `Recovering`), emits nothing, and cannot re-latch the just-cleared floor at the old
    /// high-water — the failure mode where a lost EndOfSession from B would otherwise restore the
    /// permanent wedge #66 removes.
    #[test]
    fn mbo_end_of_session_resets_peer_publisher_books() {
        use std::net::{IpAddr, Ipv4Addr};
        let pub_a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        fn ctx_for<'a>(
            publisher: IpAddr,
            arbiter: &'a SharedArbiter,
            instruments: &'a crate::model::InstrumentSnapshot,
            role: PortRole,
        ) -> FrameCtx<'a> {
            let mut c = make_ctx(arbiter, instruments, role);
            c.publisher = publisher;
            c
        }
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);
        // Reference-data state is per publisher, so each arm publishes its own manifest burst -
        // which is what they do on the wire, sharing one refdata port.
        for publisher in [pub_a, pub_b] {
            proc.on_datagram(
                &frame(&[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(0, "INST-0", 1),
                ]),
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Combined),
            );
        }
        let anchor = |sid: u32| {
            frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: sid,
                    last_instrument_seq: 0,
                    ts: sid as u64,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: sid,
                }),
            ])
        };
        // Both publishers sync and mirror the same tick; A leads, B's copy collapses.
        proc.on_datagram(
            &anchor(1),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &anchor(1),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &frame(&[add(1, 100, 5000)]),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Mktdata),
        );
        proc.on_datagram(
            &frame(&[add(1, 100, 5000)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );
        // A's EndOfSession resets BOTH books and clears the floor.
        proc.on_datagram(
            &frame(&[enc_end_of_session(6000)]),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Mktdata),
        );
        // B's old-session tail (would be depth(5001), re-latching the old high-water) is buffered
        // by B's now-Recovering book instead: nothing emits, the floor stays open.
        proc.on_datagram(
            &frame(&[add(2, 101, 5001)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );
        // B re-syncs in the new session and its restarted-clock depth is admitted.
        proc.on_datagram(
            &anchor(2),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &frame(&[add(1, 102, 50)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 0, 50],
            "B's old-session tail (5001) must not emit after A's EndOfSession; B's new-session \
             depth (50) must be admitted"
        );
    }

    /// An `InstrumentReset` whose id resolves to neither an emitted symbol (memo) nor a current
    /// definition must NOT silently skip the floor clear: it falls back to the venue-wide reset,
    /// so a wedged sibling entry is still re-opened.
    #[test]
    fn mbo_instrument_reset_for_unknown_id_falls_back_to_venue_clear() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(&frame(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
                                                              // Reset for an id with no definition, no emitted depth, no book -> venue-wide clear.
        proc.on_datagram(
            &frame(&[enc_instrument_reset(&InstrumentReset {
                instrument_id: 99,
                reason: 1,
                new_anchor_seq: 0,
                ts: 5500,
            })]),
            &mkt,
        );
        // Instrument 0's still-synced book emits at the restarted (lower) clock: admitted only if
        // the venue-wide fallback cleared the floor.
        proc.on_datagram(&frame(&[add(2, 101, 100)]), &mkt);

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 100],
            "an unresolvable InstrumentReset id must still clear the floor (venue-wide fallback)"
        );
    }

    /// If a manifest epoch bump remaps the instrument id to a DIFFERENT symbol between the last
    /// latched depth and the reset, the floor entry is wedged under the symbol the depth was
    /// EMITTED as — the `emitted_symbol` memo — not the current definition's. Clearing the current
    /// definition's symbol would leave the old-symbol entry latched forever.
    #[test]
    fn mbo_instrument_reset_after_id_remap_clears_the_latched_symbol() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments); // id 0 -> "INST-0" (manifest 1)
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(&frame(&[add(1, 100, 5000)]), &mkt); // floor latched under INST-0
                                                              // Manifest bump remaps id 0 to another symbol; the reset must clear the LATCHED
                                                              // symbol (INST-0), not the current definition's (INST-9).
        proc.on_datagram(
            &frame(&[
                enc_manifest_summary(2, 1),
                enc_instrument_def(0, "INST-9", 2),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &frame(&[enc_instrument_reset(&InstrumentReset {
                instrument_id: 0,
                reason: 1,
                new_anchor_seq: 0,
                ts: 5500,
            })]),
            &mkt,
        );
        // The venue maps the id back to INST-0 and re-syncs; the restarted-clock depths under
        // INST-0 flow only if the memo-scoped clear hit (venue, "INST-0").
        proc.on_datagram(
            &frame(&[
                enc_manifest_summary(3, 1),
                enc_instrument_def(0, "INST-0", 3),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 2,
                    last_instrument_seq: 0,
                    ts: 2,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 2,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(&frame(&[add(1, 101, 100)]), &mkt);

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 0, 100],
            "the reset must clear the latched symbol (memo), not the remapped current definition"
        );
    }

    /// `InstrumentReset` clears the `(venue, symbol)` depth-floor entry AND the book's event clock,
    /// so the re-synced book's depth — stamped by the venue's restarted (lower) clock — is admitted.
    /// Without the floor reset the post-resync depths are stale-dropped forever; without the
    /// event-clock reset the first post-resync depth would carry the pre-reset time and re-latch
    /// the floor at the old high-water, re-wedging what the reset just cleared.
    #[test]
    fn mbo_instrument_reset_unwedges_depth_floor() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(&frame(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
        proc.on_datagram(
            &frame(&[enc_instrument_reset(&InstrumentReset {
                instrument_id: 0,
                reason: 1,
                new_anchor_seq: 0,
                ts: 5500,
            })]),
            &mkt,
        );
        // Re-sync via a fresh empty anchor: the post-resync depth is stamped source_ts=0 (the
        // event clock was dropped with the book) and re-opens the cleared floor.
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 2,
                    last_instrument_seq: 0,
                    ts: 2,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 2,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(&frame(&[add(1, 101, 100)]), &mkt); // new (restarted) clock -> admitted

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 0, 100],
            "post-reset depths must flow at the restarted clock, not be stale-dropped"
        );
    }

    /// `depth` messages for a frame touching multiple instruments must arrive in ascending
    /// instrument_id order regardless of the wire order of their `OrderAdd`s. The invariant is
    /// guaranteed by draining a `BTreeSet<u32>` rather than a `HashSet`.
    #[test]
    fn mbo_depth_emit_order_is_ascending_instrument_id() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);

        // Refdata: manifest declares 2 instruments; then their definitions.
        proc.on_datagram(
            &frame(&[
                enc_manifest_summary(1, 2),
                enc_instrument_def(0, "INST-0", 1),
                enc_instrument_def(1, "INST-1", 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        // Sync each instrument via an empty-book anchor snapshot (0 orders, anchor_seq=0).
        let snap = |iid: u32, sid: u32| {
            frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: iid,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: sid,
                    last_instrument_seq: 0,
                    ts: sid as u64,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: iid,
                    anchor_seq: 0,
                    snapshot_id: sid,
                }),
            ])
        };
        let snap_ctx = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
        proc.on_datagram(&snap(0, 1), &snap_ctx);
        proc.on_datagram(&snap(1, 2), &snap_ctx);
        drain_depth_ids(&mut rx); // discard snapshot-triggered emits

        // Mktdata frame: instrument 1 appears before instrument 0 in the wire order. BTreeSet must
        // still drain 0 → 1.
        let mktdata_frame = frame(&[
            enc_order_add(&OrderAdd {
                instrument_id: 1,
                source_id: 0,
                side: SIDE_ASK,
                order_flags: 0,
                per_instrument_seq: 1,
                order_id: 101,
                enter_ts: 10,
                price_raw: 200,
                qty_raw: 5,
            }),
            enc_order_add(&OrderAdd {
                instrument_id: 0,
                source_id: 0,
                side: SIDE_BID,
                order_flags: 0,
                per_instrument_seq: 1,
                order_id: 100,
                enter_ts: 11,
                price_raw: 100,
                qty_raw: 10,
            }),
        ]);
        proc.on_datagram(
            &mktdata_frame,
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let ids = drain_depth_ids(&mut rx);
        assert_eq!(
            ids.len(),
            2,
            "expected one depth per instrument; got {ids:?}"
        );
        assert_eq!(
            ids,
            vec![0, 1],
            "depth must arrive in ascending instrument_id order"
        );

        // Replay with incremented per_instrument_seqs to confirm the order is stable across frames,
        // not a lucky hash ordering on the first run.
        let mktdata_frame2 = frame(&[
            enc_order_add(&OrderAdd {
                instrument_id: 1,
                source_id: 0,
                side: SIDE_ASK,
                order_flags: 0,
                per_instrument_seq: 2,
                order_id: 201,
                enter_ts: 20,
                price_raw: 201,
                qty_raw: 5,
            }),
            enc_order_add(&OrderAdd {
                instrument_id: 0,
                source_id: 0,
                side: SIDE_BID,
                order_flags: 0,
                per_instrument_seq: 2,
                order_id: 200,
                enter_ts: 21,
                price_raw: 101,
                qty_raw: 10,
            }),
        ]);
        proc.on_datagram(
            &mktdata_frame2,
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx),
            vec![0, 1],
            "order must be stable across frames"
        );
    }

    /// `upsert_instrument` is idempotent for matching exponents and last-writer-wins for
    /// conflicting ones (exercising the warn path; the warn itself is not asserted).
    #[test]
    fn upsert_instrument_idempotent_and_last_writer_wins() {
        let instruments: crate::model::InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));

        let base = NormalizedInstrument {
            venue: "TestVenue".into(),
            symbol: "BTC".into(),
            channel: 0,
            instrument_id: 1,
            price_exponent: -2,
            qty_exponent: -4,
        };

        // First insert.
        upsert_instrument(&instruments, &base);
        {
            let map = instruments.lock().unwrap();
            assert_eq!(map.len(), 1);
            let entry = map.get(&("TestVenue".into(), "BTC".into())).unwrap();
            assert_eq!(entry.price_exponent, -2);
            assert_eq!(entry.qty_exponent, -4);
        }

        // Second insert with identical exponents — idempotent, still one entry.
        upsert_instrument(&instruments, &base);
        assert_eq!(instruments.lock().unwrap().len(), 1);

        // Third insert with DIFFERENT exponents — exercises the divergence warn path.
        // Last writer wins: the snapshot ends with the new exponents.
        let conflicting = NormalizedInstrument {
            price_exponent: -3,
            qty_exponent: -5,
            ..base.clone()
        };
        upsert_instrument(&instruments, &conflicting);
        {
            let map = instruments.lock().unwrap();
            assert_eq!(map.len(), 1, "still one entry after conflicting write");
            let entry = map.get(&("TestVenue".into(), "BTC".into())).unwrap();
            assert_eq!(
                entry.price_exponent, -3,
                "last writer's price_exponent wins"
            );
            assert_eq!(entry.qty_exponent, -5, "last writer's qty_exponent wins");
        }
    }

    /// An `OrderAdd` for an instrument whose definition we don't yet hold must not mint a book — an
    /// undefined instrument can never emit usable `depth`, and the wire `instrument_id` is spoofable.
    #[test]
    fn mbo_undefined_instrument_creates_no_book() {
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);

        // No manifest/definition: an OrderAdd for an unknown instrument must be dropped, not booked.
        let f = frame(&[enc_order_add(&OrderAdd {
            instrument_id: 42,
            source_id: 0,
            side: SIDE_BID,
            order_flags: 0,
            per_instrument_seq: 1,
            order_id: 1,
            enter_ts: 1,
            price_raw: 100,
            qty_raw: 1,
        })]);
        proc.on_datagram(&f, &make_ctx(&arbiter, &instruments, PortRole::Mktdata));
        assert!(
            proc.books.is_empty(),
            "undefined instrument must not create a book"
        );
    }

    /// The book map **and** the `last_top` depth-suppression map must both stay bounded under a flood
    /// of distinct (defined) instrument_ids, evicting the oldest first — otherwise a forged MBO
    /// stream grows them without limit. Each instrument is driven all the way to `Synced` with an
    /// emitted `depth`, so `last_top` is actually populated (an unsynced book never reaches it).
    #[test]
    fn mbo_books_map_is_bounded_under_instrument_flood() {
        use super::MAX_BOOKS;
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        // Wire the shared replay map so the arbiter populates it on admit (the processor only purges
        // it on eviction now), keeping the in-lockstep bounding assertion below meaningful.
        let arbiter: SharedArbiter = {
            let mut a = Arbiter::new(tx, 8);
            a.set_depth_replay(depth.clone());
            Arc::new(Mutex::new(a))
        };
        let mut proc = MboProcessor::new(depth, false);

        let flood = (MAX_BOOKS as u32) + 50;
        // Declare and define every instrument so the definition gate admits each one.
        proc.on_datagram(
            &frame(&[enc_manifest_summary(1, flood)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        for i in 0..flood {
            proc.on_datagram(
                &frame(&[enc_instrument_def(i, &format!("INST-{i}"), 1)]),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
        }
        // For each instrument: an empty-anchor snapshot syncs the book, then one OrderAdd gives it a
        // resting level, so emit_depth fires and records a `last_top` entry. book_for must evict the
        // oldest from BOTH maps as the flood grows past MAX_BOOKS.
        for i in 0..flood {
            proc.on_datagram(
                &frame(&[
                    enc_snapshot_begin(&SnapshotBegin {
                        instrument_id: i,
                        anchor_seq: 0,
                        total_orders: 0,
                        snapshot_id: i + 1,
                        last_instrument_seq: 0,
                        ts: 0,
                    }),
                    enc_snapshot_end(&SnapshotEnd {
                        instrument_id: i,
                        anchor_seq: 0,
                        snapshot_id: i + 1,
                    }),
                    enc_order_add(&OrderAdd {
                        instrument_id: i,
                        source_id: 0,
                        side: SIDE_BID,
                        order_flags: 0,
                        per_instrument_seq: 1,
                        order_id: 1,
                        enter_ts: 1,
                        price_raw: 100,
                        qty_raw: 1,
                    }),
                ]),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
        }
        assert!(
            proc.books.len() <= MAX_BOOKS,
            "books map must stay bounded, got {}",
            proc.books.len()
        );
        assert!(
            proc.last_top.len() <= MAX_BOOKS,
            "last_top map must stay bounded in lockstep with books, got {}",
            proc.last_top.len()
        );
        assert!(
            proc.books.contains_key(&(TEST_PUB, flood - 1))
                && proc.last_top.contains_key(&(TEST_PUB, flood - 1)),
            "newest instrument retained in both maps"
        );
        assert!(
            !proc.books.contains_key(&(TEST_PUB, 0)) && !proc.last_top.contains_key(&(TEST_PUB, 0)),
            "oldest instrument evicted from both maps"
        );
        // The shared `depth` (WS replay) map is keyed by (venue, symbol) and must be purged in
        // lockstep too, or it grows without limit and a reconnecting client replays evicted books.
        let depth_map = crate::model::lock(&proc.depth);
        assert!(
            depth_map.len() <= MAX_BOOKS,
            "depth replay map must stay bounded in lockstep with books, got {}",
            depth_map.len()
        );
        assert!(
            depth_map.contains_key(&("TV".into(), format!("INST-{}", flood - 1).into())),
            "newest instrument's depth replay entry retained"
        );
        assert!(
            !depth_map.contains_key(&("TV".into(), "INST-0".into())),
            "oldest instrument's depth replay entry evicted too"
        );
    }

    /// The full-state `depth` re-broadcast suppression: a book change that leaves the published
    /// top-N byte-identical (deep-book churn outside the top-N) must NOT re-emit `depth`, while a
    /// change that moves a top-N level must. Without suppression every deep delta would duplicate
    /// the whole book on the wire.
    #[test]
    fn mbo_depth_suppressed_when_top_n_unchanged() {
        use super::DEPTH_LEVELS;
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, false);

        // Define instrument 0 and sync it with an empty-anchor snapshot.
        proc.on_datagram(
            &frame(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &frame(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 1,
                    last_instrument_seq: 0,
                    ts: 0,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 1,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        drain_depth_ids(&mut rx); // discard the snapshot-triggered (empty) depth

        // Frame 1: add DEPTH_LEVELS+1 bids at distinct ascending prices. The lowest price is the
        // (N+1)th level — outside the published top-N. One coalesced depth for the frame.
        let bid = |seq: u32, price: i64| {
            enc_order_add(&OrderAdd {
                instrument_id: 0,
                source_id: 0,
                side: SIDE_BID,
                order_flags: 0,
                per_instrument_seq: seq,
                order_id: seq as u64,
                enter_ts: seq as u64,
                price_raw: price,
                qty_raw: 10,
            })
        };
        let levels = DEPTH_LEVELS as u32 + 1;
        let establish: Vec<_> = (0..levels).map(|k| bid(k + 1, 100 + k as i64)).collect();
        proc.on_datagram(
            &frame(&establish),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx).len(),
            1,
            "frame 1 establishes the book: exactly one depth"
        );

        // Frame 2: churn the worst (lowest) bid price 100 — outside the top-N. Book changes, but the
        // top-N is byte-identical, so depth must be suppressed.
        proc.on_datagram(
            &frame(&[bid(levels + 1, 100)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx).len(),
            0,
            "deep-book churn outside the top-N must be suppressed"
        );

        // Frame 3: add a new best bid above every existing level — moves the top-N, must emit.
        proc.on_datagram(
            &frame(&[bid(levels + 2, 100 + levels as i64)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx).len(),
            1,
            "a top-N change must re-emit depth"
        );
    }

    // ---- Market-by-Price ----

    /// A reference-data burst defining `INST-{id}` for each id, exponents 0 so raw ints reach the
    /// wire unscaled and the assertions read as the numbers the builders wrote.
    fn mbp_refdata(ids: &[u32]) -> Vec<Vec<u8>> {
        let mut out = vec![mbp_wire::enc_manifest_summary(
            &codec_mbp::ManifestSummary {
                channel_id: 0,
                valid: true,
                manifest_seq: 1,
                instrument_count: ids.len() as u32,
                ts: 0,
            },
        )];
        out.extend(ids.iter().map(|id| {
            mbp_wire::enc_instrument_definition(&codec_mbp::InstrumentDefinition {
                instrument_id: *id,
                symbol: format!("INST-{id}").into(),
                price_exponent: 0,
                qty_exponent: 0,
                manifest_seq: 1,
            })
        }));
        out
    }

    /// One level update: `qty` is the level's absolute resulting quantity, `0` removing it.
    fn mbp_level(id: u32, seq: u32, side: u8, price: i64, qty: u64, ts: u64) -> Vec<u8> {
        mbp_wire::enc_level_update(&codec_mbp::LevelUpdate {
            instrument_id: id,
            source_id: 0,
            side,
            action: if qty == 0 { 3 } else { 1 },
            per_instrument_seq: seq,
            price_raw: price,
            qty_raw: qty,
            ts,
            order_count: Some(1),
            level_index: None,
            update_reason: 0,
            level_flags: 0,
        })
    }

    /// A complete snapshot group for `id`: begin, its levels, end. `k` is the publisher's
    /// `last_instrument_seq` at capture and `anchor` the mktdata sequence it was captured at.
    fn mbp_snapshot(
        id: u32,
        sid: u32,
        anchor: u64,
        k: u32,
        levels: &[(u8, i64, u64)],
    ) -> Vec<Vec<u8>> {
        let mut out = vec![mbp_wire::enc_snapshot_begin(&codec_mbp::SnapshotBegin {
            instrument_id: id,
            anchor_seq: anchor,
            total_levels: levels.len() as u32,
            snapshot_id: sid,
            last_instrument_seq: k,
            ts: 1,
            depth_bound: 0,
        })];
        out.extend(levels.iter().map(|&(side, price, qty)| {
            mbp_wire::enc_snapshot_level(&codec_mbp::SnapshotLevel {
                snapshot_id: sid,
                price_raw: price,
                qty_raw: qty,
                order_count: Some(1),
                side,
                level_flags: 0,
            })
        }));
        out.push(mbp_wire::enc_snapshot_end(&codec_mbp::SnapshotEnd {
            instrument_id: id,
            anchor_seq: anchor,
            snapshot_id: sid,
        }));
        out
    }

    fn drain_books(
        rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>,
    ) -> Vec<NormalizedBook> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Book(b) = &*m {
                out.push(b.clone());
            }
        }
        out
    }

    /// `(action, side, price, size)` per change, so a batch's shape asserts on one line.
    fn shape(b: &NormalizedBook) -> Vec<(BookAction, BookSide, f64, f64)> {
        b.changes
            .iter()
            .map(|c| (c.action, c.side, c.price, c.size))
            .collect()
    }

    /// The status of one reconstructed book, or `None` when the processor tracks no such book.
    fn mbp_status(p: &MbpProcessor, publisher: IpAddr, channel: u8, id: u32) -> Option<BookStatus> {
        p.books.get(&(publisher, channel, id)).map(|b| b.status())
    }

    /// A `FrameCtx` on a venue unique to one test. The Prometheus registry is process-global, so a
    /// metric assertion driven through `make_ctx`'s shared `"TV"` venue would count every other
    /// test's increments too.
    fn mbp_ctx<'a>(
        venue: &'static str,
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
    ) -> FrameCtx<'a> {
        let mut c = make_ctx(arbiter, instruments, role);
        c.venue = venue;
        c
    }

    /// An `MbpProcessor` with `ids` defined and each synced from an empty-book anchor, in the drive
    /// order the wire uses: reference data, then the snapshot stream, then deltas.
    fn synced_mbp_proc(
        arbiter: &SharedArbiter,
        instruments: &crate::model::InstrumentSnapshot,
        channel: u8,
        reset_count: u8,
        ids: &[u32],
    ) -> MbpProcessor {
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(channel, reset_count, 1, &mbp_refdata(ids)),
            &make_ctx(arbiter, instruments, PortRole::Combined),
        );
        for (n, id) in ids.iter().enumerate() {
            proc.on_datagram(
                &mbp_wire::frame(
                    channel,
                    reset_count,
                    2 + n as u64,
                    &mbp_snapshot(*id, 1, 0, 0, &[]),
                ),
                &make_ctx(arbiter, instruments, PortRole::Snapshot),
            );
        }
        proc
    }

    /// A shared arbiter over a fresh broadcast channel — the four lines every processor test needs.
    fn mbp_harness() -> (
        SharedArbiter,
        broadcast::Receiver<std::sync::Arc<FeedMessage>>,
        crate::model::InstrumentSnapshot,
    ) {
        let (tx, rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        (arbiter, rx, instruments)
    }

    /// §4.1 — `SnapshotLevel` carries no instrument id and MUST route by the open group. Two
    /// instruments legitimately share a `snapshot_id` within one rotation (it is monotonic per
    /// `(channel, instrument)`, not per channel), so keying the route on the id sends one
    /// instrument's levels into the other's book.
    #[test]
    fn mbp_snapshot_levels_route_by_open_group_not_snapshot_id() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41, 42])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        // Both rotations use snapshot_id 5 — the collision the route must not key on.
        for (id, price) in [(41u32, 6200i64), (42, 6300)] {
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 2, &mbp_snapshot(id, 5, 0, 0, &[(MBP_BID, price, 10)])),
                &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
            );
        }

        let books = drain_books(&mut rx);
        assert_eq!(books.len(), 2, "one re-baseline per instrument");
        for (b, price) in books.iter().zip([6200.0, 6300.0]) {
            assert_eq!(
                shape(b),
                vec![
                    (BookAction::Clear, BookSide::Both, 0.0, 0.0),
                    (BookAction::Update, BookSide::Bid, price, 10.0),
                ],
                "instrument {} holds only its own level",
                b.instrument_id
            );
        }
    }

    /// A snapshot install re-baselines: `clear` first, then the complete level set, `snapshot: true`
    /// and `last: true`. `changes[0].action == Clear` is what a consumer keys on.
    #[test]
    fn mbp_a_snapshot_install_emits_clear_then_the_full_level_set() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                2,
                &mbp_snapshot(
                    41,
                    1,
                    0,
                    0,
                    &[
                        (MBP_BID, 6100, 20),
                        (MBP_BID, 6200, 10),
                        (MBP_ASK, 6300, 30),
                    ],
                ),
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );

        let books = drain_books(&mut rx);
        assert_eq!(books.len(), 1);
        assert!(books[0].snapshot, "advisory rebuild flag");
        assert!(books[0].last, "a buffering consumer wedges without it");
        assert_eq!(
            shape(&books[0]),
            vec![
                (BookAction::Clear, BookSide::Both, 0.0, 0.0),
                (BookAction::Update, BookSide::Bid, 6200.0, 10.0),
                (BookAction::Update, BookSide::Bid, 6100.0, 20.0),
                (BookAction::Update, BookSide::Ask, 6300.0, 30.0),
            ],
            "clear, then bids best-first and asks best-first"
        );
    }

    /// A batch of level updates in one frame coalesces into ONE `book` message per instrument, with
    /// `last: true`. Cross-instrument atomicity is not promised, so per-frame batching is correct.
    #[test]
    fn mbp_one_book_message_per_instrument_per_frame() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41, 42]);
        let _ = drain_books(&mut rx);

        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                100,
                &[
                    mbp_level(41, 1, MBP_BID, 6200, 10, 7_000),
                    mbp_level(42, 1, MBP_BID, 6300, 20, 7_001),
                    mbp_level(41, 2, MBP_ASK, 6400, 30, 7_002),
                ],
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let books = drain_books(&mut rx);
        assert_eq!(books.len(), 2, "one batch per instrument, not per message");
        assert_eq!(
            books.iter().map(|b| b.instrument_id).collect::<Vec<_>>(),
            vec![41, 42],
            "ascending instrument id"
        );
        assert!(books.iter().all(|b| b.last && !b.snapshot));
        assert_eq!(
            shape(&books[0]),
            vec![
                (BookAction::Update, BookSide::Bid, 6200.0, 10.0),
                (BookAction::Update, BookSide::Ask, 6400.0, 30.0),
            ],
            "41 carries both of its changes in arrival order"
        );
        assert_eq!(
            books[0].source_ts_ns, 7_002,
            "the latest applied event's time"
        );
    }

    /// A level update to quantity `0` removes the level, so it publishes as `Delete` with size 0 —
    /// the wire `Action` byte never decides this, the quantity does.
    #[test]
    fn mbp_a_zero_quantity_level_publishes_as_a_delete() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41]);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &mkt,
        );
        let _ = drain_books(&mut rx);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 101, &[mbp_level(41, 2, MBP_BID, 6200, 0, 7_001)]),
            &mkt,
        );

        assert_eq!(
            shape(&drain_books(&mut rx)[0]),
            vec![(BookAction::Delete, BookSide::Bid, 6200.0, 0.0)]
        );
    }

    /// A `BookClear` scoped to a price publishes the exact levels it removed. The wire `Clear`
    /// carries no price bound, so publishing one would tell the consumer to drop the whole side
    /// while this book keeps its inside levels — the two would diverge with every sequence check
    /// passing. A whole-side clear is expressible and stays one `Clear`.
    #[test]
    fn mbp_a_from_price_clear_publishes_exact_deletes() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                2,
                &mbp_snapshot(
                    41,
                    1,
                    0,
                    0,
                    &[
                        (MBP_BID, 6200, 10),
                        (MBP_BID, 6100, 20),
                        (MBP_BID, 6000, 30),
                        (MBP_ASK, 6300, 40),
                    ],
                ),
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        let _ = drain_books(&mut rx);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        let clear = |seq: u32, clear_side: u8, scope: u8, from: i64| {
            mbp_wire::enc_book_clear(&codec_mbp::BookClear {
                instrument_id: 41,
                source_id: 0,
                clear_side,
                scope,
                per_instrument_seq: seq,
                from_price_raw: from,
                ts: 7_000 + seq as u64,
                clear_reason: 0,
            })
        };
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                100,
                &[clear(
                    1,
                    codec_mbp::CLEAR_SIDE_BID,
                    codec_mbp::SCOPE_FROM_PRICE,
                    6100,
                )],
            ),
            &mkt,
        );
        let books = drain_books(&mut rx);
        assert_eq!(
            shape(&books[0]),
            vec![
                (BookAction::Delete, BookSide::Bid, 6000.0, 0.0),
                (BookAction::Delete, BookSide::Bid, 6100.0, 0.0),
            ],
            "only the levels the clear actually removed, not the surviving 6200"
        );

        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                101,
                &[clear(
                    2,
                    codec_mbp::CLEAR_SIDE_ASK,
                    codec_mbp::SCOPE_ENTIRE_SIDE,
                    0,
                )],
            ),
            &mkt,
        );
        assert_eq!(
            shape(&drain_books(&mut rx)[0]),
            vec![(BookAction::Clear, BookSide::Ask, 0.0, 0.0)],
            "a whole-side clear is expressible verbatim"
        );
    }

    /// Emission gates per instrument on a known definition — precision before price, the same gate
    /// every other processor applies. A book for an undefined instrument is never even created.
    #[test]
    fn mbp_no_book_is_emitted_before_the_instrument_definition() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10)])),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 2, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        assert!(
            drain_books(&mut rx).is_empty(),
            "no price without precision"
        );
        assert!(proc.books.is_empty(), "and no book to hold it");
    }

    /// Only the wire `symbol` is a label; the identity triple is what rides the message.
    #[test]
    fn mbp_emitted_books_carry_the_channel_and_instrument_id() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 7, 0, &[41]);
        let _ = drain_books(&mut rx);
        proc.on_datagram(
            &mbp_wire::frame(7, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let books = drain_books(&mut rx);
        assert_eq!(books[0].channel, 7, "the frame header's channel_id");
        assert_eq!(books[0].instrument_id, 41, "the wire instrument id");
        assert_eq!(&*books[0].symbol, "INST-41", "a display label only");
    }

    /// The `instrument` definition carries the same identity pair, so a consumer joins a book to its
    /// precision on `(venue, channel, instrument_id)` rather than the colliding `symbol`.
    #[test]
    fn mbp_instrument_definitions_carry_the_identity_pair() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(7, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.channel, i.instrument_id));
            }
        }
        assert_eq!(seen, vec![(7, 41)]);
        assert!(proc.books.is_empty(), "reference data alone builds no book");
    }

    /// Trades are emitted only when the feed row owns them, exactly as the other processors gate.
    #[test]
    fn mbp_trades_are_emitted_only_when_the_row_owns_them() {
        let trade = mbp_wire::enc_trade(&codec_mbp::Trade {
            instrument_id: 41,
            source_id: 0,
            aggressor_side: codec_mbp::AGGRESSOR_BUY,
            trade_flags: 0,
            source_ts: 7_000,
            trade_price_raw: 6200,
            trade_qty_raw: 5,
            trade_id: 99,
            cumulative_volume_raw: 500,
        });
        for (emit_trades, want) in [(true, 1), (false, 0)] {
            let (arbiter, mut rx, instruments) = mbp_harness();
            let mut proc = MbpProcessor::new(emit_trades);
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 2, std::slice::from_ref(&trade)),
                &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
            );
            let trades = std::iter::from_fn(|| rx.try_recv().ok())
                .filter(|m| matches!(&**m, FeedMessage::Trade(_)))
                .count();
            assert_eq!(trades, want, "emit_trades = {emit_trades}");
        }
    }

    /// §4.7 — `EndOfSession` from one arm must drop only that arm's books. Under the order-keyed
    /// processor's handler it also cleared the venue's shared floor, so one arm shutting down tore
    /// down the live published book.
    #[test]
    fn mbp_end_of_session_is_scoped_to_the_emitting_arm() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let pub_a = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));
        let mut proc = MbpProcessor::new(false);
        // Reference-data state is per publisher, so each arm sends its own burst — which is what
        // they do on the wire, sharing one refdata port.
        for publisher in [pub_a, pub_b] {
            let mut refdata = make_ctx(&arbiter, &instruments, PortRole::Combined);
            refdata.publisher = publisher;
            proc.on_datagram(&mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])), &refdata);
            let mut snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
            snap.publisher = publisher;
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[])),
                &snap,
            );
        }
        assert_eq!(mbp_status(&proc, pub_b, 0, 41), Some(BookStatus::Ready));

        let mut a_mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        a_mkt.publisher = pub_a;
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 100, &[mbp_wire::enc_end_of_session(9_000)]),
            &a_mkt,
        );

        assert_eq!(
            mbp_status(&proc, pub_a, 0, 41),
            Some(BookStatus::AwaitingSnapshot),
            "the ending arm's book is dropped"
        );
        assert_eq!(
            mbp_status(&proc, pub_b, 0, 41),
            Some(BookStatus::Ready),
            "the peer arm keeps serving; authority transfers to it"
        );
    }

    /// §4.9 — a reset is any CHANGE in `Reset Count`, including the 255 -> 0 wrap. Comparing for
    /// ordering (`>`) would silently ignore the wrap and keep applying deltas against discarded
    /// publisher state.
    #[test]
    fn mbp_reset_count_wrap_is_a_reset() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 255, &[41]);
        assert_eq!(mbp_status(&proc, TEST_PUB, 0, 41), Some(BookStatus::Ready));

        proc.on_datagram(
            &mbp_wire::frame(0, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            None,
            "the pre-wrap book is discarded, not applied to"
        );
    }

    /// ...and it is scoped to the publisher that reset, per the same rule as reference data.
    #[test]
    fn mbp_reset_count_change_is_scoped_to_the_publisher() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let pub_a = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));
        let mut proc = MbpProcessor::new(false);
        for publisher in [pub_a, pub_b] {
            let mut refdata = make_ctx(&arbiter, &instruments, PortRole::Combined);
            refdata.publisher = publisher;
            proc.on_datagram(&mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])), &refdata);
            let mut snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
            snap.publisher = publisher;
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[])),
                &snap,
            );
        }

        let mut a_mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        a_mkt.publisher = pub_a;
        proc.on_datagram(&mbp_wire::frame(0, 1, 100, &[]), &a_mkt);

        assert_eq!(mbp_status(&proc, pub_a, 0, 41), None, "A's state discarded");
        assert_eq!(
            mbp_status(&proc, pub_b, 0, 41),
            Some(BookStatus::Ready),
            "B never reset"
        );
    }

    /// §4.5 — the cross-instrument buffer is bounded and overflow drops the largest buffer, marks
    /// that instrument `Gap`, and counts it. It must never take the channel down.
    ///
    /// The budget is only reachable with several heavily-buffering instruments: `pricebook`'s
    /// per-book cap is a quarter of it, so one instrument alone is clamped there first.
    #[test]
    fn mbp_buffer_overflow_drops_the_largest_instrument_and_counts_it() {
        let venue = "MbpBufferOverflowTest";
        let heavy: Vec<u32> = (41..=44).collect();
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        let mut ids = heavy.clone();
        ids.push(99);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&ids)),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );
        // Deltas for a never-snapshotted instrument buffer. Filled through `with_book` rather than
        // millions of datagrams: the path under test is the budget check on the next datagram, and
        // `with_book` is the one accounting seam the running total depends on.
        let mkt = mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata);
        let fill = |proc: &mut MbpProcessor, id: u32, n: usize| {
            let key = proc.ensure_book(&mkt, 0, id).expect("defined instrument");
            proc.with_book(&key, |b| {
                for seq in 1..=n as u32 {
                    b.on_delta(
                        PriceDeltaOp {
                            seq,
                            mktdata_seq: seq as u64,
                            ts: seq as u64,
                            delta: BookDelta::Level {
                                side: MBP_BID,
                                price_raw: 6_200,
                                qty_raw: 1,
                                order_count: None,
                                level_flags: 0,
                                action: 1,
                            },
                        },
                        &mut Vec::new(),
                    );
                }
            });
        };
        let per_book = MAX_BUFFERED_DELTAS_ACROSS_BOOKS / heavy.len();
        for id in &heavy {
            fill(&mut proc, *id, per_book);
        }
        fill(&mut proc, 99, 10);
        assert_eq!(
            proc.buffered_total,
            MAX_BUFFERED_DELTAS_ACROSS_BOOKS + 10,
            "the running total tracks every book"
        );

        let before = metrics()
            .mbp_buffer_overflows
            .with_label_values(&[venue])
            .get();
        proc.on_datagram(&mbp_wire::frame(0, 0, 2, &[]), &mkt);

        assert!(
            proc.buffered_total <= MAX_BUFFERED_DELTAS_ACROSS_BOOKS,
            "back under budget, got {}",
            proc.buffered_total
        );
        let dropped: Vec<u32> = heavy
            .iter()
            .copied()
            .filter(|id| proc.books[&(TEST_PUB, 0, *id)].buffered_len() == 0)
            .collect();
        assert_eq!(dropped.len(), 1, "exactly one heavy buffer is dropped");
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, dropped[0]),
            Some(BookStatus::Gap),
            "and recovers on its next snapshot like any other gapped instrument"
        );
        assert_eq!(
            proc.books[&(TEST_PUB, 0, 99)].buffered_len(),
            10,
            "the small instrument is untouched — the channel never goes down"
        );
        assert_eq!(
            metrics()
                .mbp_buffer_overflows
                .with_label_values(&[venue])
                .get(),
            before + 1
        );
    }

    /// The O(1) budget check rests on `buffered_total` matching the true sum, so every path that can
    /// change a buffer must run through `with_book`. This drives all of them and compares.
    #[test]
    fn mbp_buffered_total_matches_the_recomputed_sum() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41, 42]);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        let recomputed =
            |p: &MbpProcessor| p.books.values().map(|b| b.buffered_len()).sum::<usize>();

        // Gap both instruments (a forward jump buffers), then exercise each mutation in turn.
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                100,
                &[
                    mbp_level(41, 9, MBP_BID, 6200, 10, 7_000),
                    mbp_level(42, 9, MBP_BID, 6300, 10, 7_001),
                    mbp_level(41, 10, MBP_BID, 6100, 10, 7_002),
                ],
            ),
            &mkt,
        );
        assert!(proc.buffered_total > 0, "the gap buffered something");
        assert_eq!(proc.buffered_total, recomputed(&proc), "after buffering");

        // A snapshot install replays past the anchor and drains what it consumed.
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 101, &mbp_snapshot(41, 2, 100, 8, &[])),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        assert_eq!(proc.buffered_total, recomputed(&proc), "after a replay");

        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                102,
                &[mbp_wire::enc_instrument_reset(
                    &codec_mbp::InstrumentReset {
                        instrument_id: 42,
                        reason: 0,
                        new_anchor_seq: 200,
                        ts: 7_003,
                    },
                )],
            ),
            &mkt,
        );
        assert_eq!(proc.buffered_total, recomputed(&proc), "after a reset");

        proc.on_datagram(
            &mbp_wire::frame(0, 0, 103, &[mbp_level(42, 20, MBP_BID, 6300, 10, 7_004)]),
            &mkt,
        );
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 104, &[mbp_wire::enc_end_of_session(9_000)]),
            &mkt,
        );
        assert_eq!(
            proc.buffered_total, 0,
            "an ended session's buffers belong to it"
        );
        assert_eq!(
            proc.buffered_total,
            recomputed(&proc),
            "after end of session"
        );

        // ...and a forgotten book takes its buffer out of the total.
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 105, &[mbp_level(41, 30, MBP_BID, 6200, 10, 7_005)]),
            &mkt,
        );
        proc.forget_book(&(TEST_PUB, 0, 41));
        assert_eq!(proc.buffered_total, recomputed(&proc), "after an eviction");
    }

    /// A crossed inside market is counted and surfaced, and MUST NOT change status or discard the
    /// book — an instrument holding corrupt state is repaired by its next snapshot on exactly the
    /// schedule it would have been anyway.
    #[test]
    fn mbp_a_crossed_book_is_counted_not_acted_on() {
        let venue = "MbpCrossedTest";
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                2,
                &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10), (MBP_ASK, 6300, 20)]),
            ),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot),
        );

        let before = metrics().mbp_crossed.with_label_values(&[venue]).get();
        // The boundary is the consistency point: mid-batch a crossed inside market is legitimate.
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                100,
                &[
                    mbp_level(41, 1, MBP_ASK, 6100, 5, 7_000),
                    mbp_wire::enc_batch_boundary(&codec_mbp::BatchBoundary {
                        batch_id: 1,
                        batch_time: 7_001,
                    }),
                ],
            ),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata),
        );

        assert_eq!(
            metrics().mbp_crossed.with_label_values(&[venue]).get(),
            before + 1
        );
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::Ready),
            "monitoring never changes status"
        );
    }

    /// A `last_applied_instrument_seq` above the publisher's real counter makes every delta read as
    /// a duplicate and every snapshot as current while the book still reports `Ready`. We do not
    /// self-heal that (the reference implementation does not either — a routed `Reset Count` clears
    /// it), so the counter is the only thing that surfaces the wedge.
    #[test]
    fn mbp_duplicate_deltas_are_counted() {
        let venue = "MbpDuplicateDeltaTest";
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );
        // A snapshot claiming a baseline of 100 while the publisher is really at 1.
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 2, &mbp_snapshot(41, 1, 0, 100, &[])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot),
        );
        let _ = drain_books(&mut rx);

        let before = metrics()
            .mbp_duplicate_deltas
            .with_label_values(&[venue])
            .get();
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                100,
                &[
                    mbp_level(41, 1, MBP_BID, 6200, 10, 7_000),
                    mbp_level(41, 2, MBP_BID, 6100, 10, 7_001),
                ],
            ),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata),
        );

        assert_eq!(
            metrics()
                .mbp_duplicate_deltas
                .with_label_values(&[venue])
                .get(),
            before + 2,
            "every delta below the installed baseline is counted"
        );
        assert!(
            drain_books(&mut rx).is_empty(),
            "a Ready book publishing nothing is the wedge's signature"
        );
        assert_eq!(mbp_status(&proc, TEST_PUB, 0, 41), Some(BookStatus::Ready));
    }

    /// A `SnapshotLevel` that no open group can route is dropped and counted — a publisher
    /// interleaving groups, or a lost `SnapshotBegin`. It must never land in another book.
    #[test]
    fn mbp_orphan_snapshot_levels_are_counted() {
        let venue = "MbpOrphanLevelTest";
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        proc.on_datagram(
            &mbp_wire::frame(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );

        let before = metrics()
            .mbp_orphan_snapshot_levels
            .with_label_values(&[venue])
            .get();
        proc.on_datagram(
            &mbp_wire::frame(
                0,
                0,
                2,
                &[mbp_wire::enc_snapshot_level(&codec_mbp::SnapshotLevel {
                    snapshot_id: 5,
                    price_raw: 6200,
                    qty_raw: 10,
                    order_count: Some(1),
                    side: MBP_BID,
                    level_flags: 0,
                })],
            ),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot),
        );

        assert_eq!(
            metrics()
                .mbp_orphan_snapshot_levels
                .with_label_values(&[venue])
                .get(),
            before + 1
        );
        assert!(proc.books.is_empty(), "and it built no book to hold it");
    }

    /// The authority gate decides which arm reaches the wire from per-market health, so a book
    /// leaving `Ready` has to be reported. Only transitions are reported, not every frame.
    #[test]
    fn mbp_book_health_reaches_the_authority() {
        let (arbiter, _rx, instruments) = mbp_harness();
        lock(&arbiter).set_authority(crate::ingest::authority::AuthorityConfig {
            leader_timeout_ns: 1_000_000_000,
            sample_interval_ns: 1_000_000_000,
            transfer_margin_ns: 1_000,
            transfer_win_rate: 0.6,
            min_window_samples: 10,
        });
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 3, 0, &[41]);
        let market = (crate::model::venue_arc("TV"), 3u32, 41u32);
        let arm = Publisher::Edge(TEST_PUB);
        assert!(
            lock(&arbiter)
                .authority()
                .expect("configured")
                .healthy(&market, arm),
            "a synced book serves its market"
        );

        proc.on_datagram(
            &mbp_wire::frame(3, 0, 100, &[mbp_wire::enc_end_of_session(9_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert!(
            !lock(&arbiter)
                .authority()
                .expect("configured")
                .healthy(&market, arm),
            "an ended session hands the market to the peer arm"
        );
    }

    /// The `(publisher, channel)` reset/open-group maps take their keys from unauthenticated wire
    /// data, so they must stay bounded under a forged-source flood, evicting the oldest first.
    #[test]
    fn mbp_channel_key_maps_are_bounded_under_publisher_flood() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));
        let flood = (MAX_CHANNEL_KEYS as u32) + 50;
        for i in 0..flood {
            let mut ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
            ctx.publisher = ip(i);
            proc.on_datagram(&mbp_wire::frame(0, 0, 1, &[]), &ctx);
        }
        assert!(
            proc.last_reset.len() <= MAX_CHANNEL_KEYS,
            "reset map must stay bounded, got {}",
            proc.last_reset.len()
        );
        assert!(
            proc.last_reset.contains_key(&(ip(flood - 1), 0)),
            "newest kept"
        );
        assert!(!proc.last_reset.contains_key(&(ip(0), 0)), "oldest evicted");
    }

    /// The book map is bounded the same way, since the wire `instrument_id` is spoofable too.
    #[test]
    fn mbp_books_map_is_bounded_under_instrument_flood() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(false);
        let ids: Vec<u32> = (0..(MAX_PRICE_BOOKS as u32 + 50)).collect();
        // One burst per 200 definitions keeps each frame's message count inside the header's u8.
        for chunk in ids.chunks(200) {
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 1, &mbp_refdata(chunk)),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
        }
        for chunk in ids.chunks(100) {
            let deltas: Vec<Vec<u8>> = chunk
                .iter()
                .map(|id| mbp_level(*id, 1, MBP_BID, 6200, 10, 7_000))
                .collect();
            proc.on_datagram(
                &mbp_wire::frame(0, 0, 100, &deltas),
                &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
            );
        }

        assert!(
            proc.books.len() <= MAX_PRICE_BOOKS,
            "book map must stay bounded, got {}",
            proc.books.len()
        );
        assert_eq!(
            proc.books.len(),
            proc.books_order.len(),
            "the eviction order tracks the map exactly"
        );
        assert!(
            proc.health_reported.len() <= proc.books.len(),
            "sibling maps stay a subset"
        );
    }
}
