//! Per-protocol frame processors: the [`FrameProcessor`] implementations the receiver's shared
//! driver dispatches to. Each owns its protocol state (reference-data state machine, sequence
//! tracker, warn-once flags, book state) and turns decoded frames into normalized `FeedMessage`s.
//!
//! - [`TobProcessor`] - Top-of-Book & Trades (`codec`, magic `0x445A`).
//! - [`MidpointProcessor`] - Midpoint (`codec_midpoint`, magic `0x4D44`).
//! - [`MboProcessor`] - Market-by-Order (`codec_mbo`, magic `0x4444`): reconstructs the L3 book
//!   in [`crate::ingest::book`] and re-serves it as full-state `depth` + `trade`.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    net::IpAddr,
};

use tracing::{debug, info, warn};

use crate::{
    ingest::{
        arbiter::lock,
        book::{BookState, DeltaKind, DeltaOp, Level},
        codec::{
            aggressor_side, apply_exponent, decode_frame, source_name, InstrumentDefinition,
            Message,
        },
        codec_mbo, codec_midpoint,
        receiver::{FrameCtx, FrameProcessor, SeqCheck, SeqTracker},
        subscriber::RefDataState,
    },
    metrics::metrics,
    model::{
        DepthSnapshot, FeedMessage, NormalizedDepth, NormalizedInstrument, NormalizedMidpoint,
        NormalizedQuote, NormalizedTrade,
    },
};

/// How many price levels per side a `depth` snapshot carries.
const DEPTH_LEVELS: usize = 10;

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
                venue = inst.venue,
                symbol = inst.symbol,
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
    state: RefDataState<InstrumentDefinition>,
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
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
    /// Pre-resolved frame-sequence metric children (bound lazily on the first frame).
    seq_events: SeqEvents,
}

impl TobProcessor {
    pub fn new(emit_trades: bool) -> Self {
        Self {
            state: RefDataState::new(),
            seq: HashMap::new(),
            seq_order: VecDeque::new(),
            warned_invalid_manifest: false,
            warned_source_mismatch: false,
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
                warn!(role = ?ctx.role, "decode error: {e}");
                return;
            }
        };

        let handle_refdata = ctx.role.handles_refdata();
        let handle_quotes = ctx.role.handles_mktdata();

        if handle_refdata {
            self.state.on_frame(header.reset_count);
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
                    self.state
                        .on_manifest(true, m.manifest_seq, m.instrument_count);
                }
                Message::InstrumentDefinition(d) if handle_refdata => {
                    let inst = NormalizedInstrument {
                        venue: ctx.venue.to_string(),
                        symbol: d.symbol.clone(),
                        price_exponent: d.price_exponent,
                        qty_exponent: d.qty_exponent,
                    };
                    // Update the shared snapshot so newly-connecting subscribers get this
                    // instrument before any quote.
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                Message::ChannelReset(ts) if handle_refdata => {
                    warn!(ts, "channel reset; discarding reference-data state");
                    self.state = RefDataState::new();
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
                    let Some(def) = self.state.definition(q.instrument_id) else {
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
                        venue: venue.to_string(),
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
                    let Some(def) = self.state.definition(t.instrument_id) else {
                        continue;
                    };
                    let venue: &'static str = source_name(t.source_id).unwrap_or(ctx.venue);
                    let trade = NormalizedTrade {
                        venue: venue.to_string(),
                        symbol: def.symbol.clone(),
                        price: apply_exponent(t.trade_price_raw, def.price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, def.qty_exponent),
                        aggressor_side: aggressor_side(t.aggressor_side).to_string(),
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
    state: RefDataState<codec_midpoint::InstrumentDefinition>,
    seq: SeqTracker,
    warned_source_mismatch: bool,
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
            state: RefDataState::new(),
            seq: SeqTracker::default(),
            warned_source_mismatch: false,
            seq_events: SeqEvents::default(),
        }
    }
}

impl FrameProcessor for MidpointProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &FrameCtx) {
        let (header, messages) = match codec_midpoint::decode_frame(buf) {
            Ok(v) => v,
            Err(e) => {
                warn!(role = ?ctx.role, "midpoint decode error: {e}");
                return;
            }
        };

        let handle_refdata = ctx.role.handles_refdata();
        let handle_mids = ctx.role.handles_mktdata();

        if handle_refdata {
            self.state.on_frame(header.reset_count);
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
                    self.state
                        .on_manifest(m.valid, m.manifest_seq, m.instrument_count);
                }
                codec_midpoint::Message::InstrumentDefinition(d) if handle_refdata => {
                    // A mid price has no size, so there is no qty exponent on the Midpoint feed;
                    // report qty_exponent = 0 in the shared snapshot (consumers ignore it for mids).
                    let inst = NormalizedInstrument {
                        venue: ctx.venue.to_string(),
                        symbol: d.symbol.clone(),
                        price_exponent: d.price_exponent,
                        qty_exponent: 0,
                    };
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                codec_midpoint::Message::EndOfSession(ts) if handle_refdata => {
                    info!(ts, "midpoint end of session");
                }
                codec_midpoint::Message::Midpoint(mp) if handle_mids && mids_fresh => {
                    let Some(def) = self.state.definition(mp.instrument_id) else {
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
                        venue: source_name(mp.source_id).unwrap_or(ctx.venue).to_string(),
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
    state: RefDataState<codec_mbo::InstrumentDefinition>,
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
    warned_source_mismatch: bool,
    /// One-shot guard for the manifest `Valid=0` override warning (see the handler).
    warned_invalid_manifest: bool,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
}

impl MboProcessor {
    pub fn new(depth: DepthSnapshot, emit_trades: bool) -> Self {
        Self {
            state: RefDataState::new(),
            books: HashMap::new(),
            books_order: VecDeque::new(),
            depth,
            last_top: HashMap::new(),
            warned_source_mismatch: false,
            warned_invalid_manifest: false,
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
        self.state.definition(instrument_id)?;
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
                        let (_old_pub, old_id) = old;
                        let symbol_still_served = self.books.keys().any(|(_p, i)| *i == old_id);
                        if !symbol_still_served {
                            if let Some(def) = self.state.definition(old_id) {
                                crate::model::lock(&self.depth)
                                    .remove(&(ctx.venue.to_string(), def.symbol.clone()));
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
        let Some(def) = self.state.definition(instrument_id) else {
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
            venue: ctx.venue.to_string(),
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
                warn!(role = ?ctx.role, "mbo decode error: {e}");
                return;
            }
        };

        if ctx.role.handles_refdata() {
            self.state.on_frame(header.reset_count);
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
                    self.state
                        .on_manifest(true, m.manifest_seq, m.instrument_count);
                }
                codec_mbo::Message::InstrumentDefinition(d) => {
                    let inst = NormalizedInstrument {
                        venue: ctx.venue.to_string(),
                        symbol: d.symbol.clone(),
                        price_exponent: d.price_exponent,
                        qty_exponent: d.qty_exponent,
                    };
                    upsert_instrument(ctx.instruments, &inst);
                    self.state.on_instrument_definition(d);
                    ctx.emit(FeedMessage::Instrument(inst));
                }
                codec_mbo::Message::EndOfSession(ts) => {
                    info!(ts, "mbo end of session");
                    // The venue may restart its event clock next session. Clear this venue's
                    // latched depth-floor entries so a post-session `source_ts` below the old
                    // high-water re-opens the tick instead of being dropped as stale forever
                    // (the floor stays latched otherwise — no full-state self-heal rescues it).
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
                    if let Some(def) = self.state.definition(o.instrument_id) {
                        let trade = NormalizedTrade {
                            venue: ctx.venue.to_string(),
                            symbol: def.symbol.clone(),
                            price: apply_exponent(o.exec_price_raw, def.price_exponent),
                            size: apply_exponent(o.exec_qty_raw as i64, def.qty_exponent),
                            aggressor_side: aggressor_side(o.aggressor_side).to_string(),
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
                    let Some(def) = self.state.definition(t.instrument_id) else {
                        continue;
                    };
                    let trade = NormalizedTrade {
                        venue: source_name(t.source_id).unwrap_or(ctx.venue).to_string(),
                        symbol: def.symbol.clone(),
                        price: apply_exponent(t.trade_price_raw, def.price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, def.qty_exponent),
                        aggressor_side: aggressor_side(t.aggressor_side).to_string(),
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
                    // so the post-reset depth re-opens the tick. No definition → nothing was ever
                    // emitted for this instrument, so there is no floor entry to clear.
                    if let Some(symbol) = self
                        .state
                        .definition(r.instrument_id)
                        .map(|d| d.symbol.clone())
                    {
                        lock(ctx.arbiter).reset_depth_floor_for_symbol(
                            ctx.venue,
                            &symbol,
                            "instrument_reset",
                        );
                    }
                    if let Some(book) = self.book_for(r.instrument_id, ctx) {
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tokio::sync::broadcast;

    use super::{upsert_instrument, MboProcessor, TobProcessor};
    use crate::{
        ingest::{
            arbiter::{Arbiter, SharedArbiter},
            codec_mbo::{
                tests::{
                    enc_end_of_session, enc_instrument_reset, enc_order_add, enc_snapshot_begin,
                    enc_snapshot_end, frame,
                },
                InstrumentReset, OrderAdd, SnapshotBegin, SnapshotEnd, MSG_INSTRUMENT_DEFINITION,
                MSG_MANIFEST_SUMMARY, SIDE_ASK, SIDE_BID,
            },
            receiver::{FrameCtx, FrameProcessor, PortRole},
        },
        model::{DepthSnapshot, FeedMessage, NormalizedInstrument},
    };

    // The quote latch-to-leader floor and the trade windowed dedup now live in the shared
    // `ingest::arbiter` (lifted out of `TobProcessor` so the multicast processors and the WS feeder
    // converge on one floor per (venue, symbol)). Their unit coverage — leader latch, non-leader
    // drop, stale-tick drop, capacity bound, source_ts==0 bypass, the bbo_hash identity incl.
    // bid_n/ask_n, and the public-loses-to-edge backstop — lives in `arbiter::tests`.

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
    fn drain_depth_ids(rx: &mut broadcast::Receiver<FeedMessage>) -> Vec<u32> {
        let mut ids = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(FeedMessage::Depth(d)) => {
                    ids.push(d.symbol.trim_start_matches("INST-").parse::<u32>().unwrap());
                }
                Ok(_) => {}
                Err(_) => break,
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
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(64);
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
    fn drain_depth_ts(rx: &mut broadcast::Receiver<FeedMessage>) -> Vec<u64> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Depth(d) = m {
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

    /// `EndOfSession` clears the venue's latched depth-floor entries (the session-reset escape
    /// hatch, #66): a post-session depth whose `source_ts` is BELOW the pre-session high-water is
    /// re-admitted instead of being dropped as stale forever (there is no full-state self-heal
    /// while the floor stays latched).
    #[test]
    fn mbo_end_of_session_unwedges_depth_floor() {
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(&frame(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
        proc.on_datagram(&frame(&[add(2, 101, 100)]), &mkt); // venue clock restarted -> stale, dropped
        proc.on_datagram(&frame(&[enc_end_of_session(6000)]), &mkt); // clears the venue's floor
        proc.on_datagram(&frame(&[add(3, 102, 50)]), &mkt); // lower tick re-opens -> admitted

        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![0, 5000, 50],
            "the pre-reset lower tick (100) is dropped; after EndOfSession the floor re-opens"
        );
    }

    /// `InstrumentReset` clears the `(venue, symbol)` depth-floor entry AND the book's event clock,
    /// so the re-synced book's depth — stamped by the venue's restarted (lower) clock — is admitted.
    /// Without the floor reset the post-resync depths are stale-dropped forever; without the
    /// event-clock reset the first post-resync depth would carry the pre-reset time and re-latch
    /// the floor at the old high-water, re-wedging what the reset just cleared.
    #[test]
    fn mbo_instrument_reset_unwedges_depth_floor() {
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(64);
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
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(64);
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
            venue: "TestVenue".to_string(),
            symbol: "BTC".to_string(),
            price_exponent: -2,
            qty_exponent: -4,
        };

        // First insert.
        upsert_instrument(&instruments, &base);
        {
            let map = instruments.lock().unwrap();
            assert_eq!(map.len(), 1);
            let entry = map
                .get(&("TestVenue".to_string(), "BTC".to_string()))
                .unwrap();
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
            let entry = map
                .get(&("TestVenue".to_string(), "BTC".to_string()))
                .unwrap();
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
        let (tx, _rx) = broadcast::channel::<FeedMessage>(64);
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
        let (tx, _rx) = broadcast::channel::<FeedMessage>(256);
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
            depth_map.contains_key(&("TV".to_string(), format!("INST-{}", flood - 1))),
            "newest instrument's depth replay entry retained"
        );
        assert!(
            !depth_map.contains_key(&("TV".to_string(), "INST-0".to_string())),
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
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(256);
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
}
