//! Per-protocol frame processors: the [`FrameProcessor`] implementations the receiver's shared
//! driver dispatches to. Each owns its protocol state (reference-data state machine, sequence
//! tracker, warn-once flags, book state) and turns decoded frames into normalized `FeedMessage`s.
//!
//! - [`TobProcessor`] - Top-of-Book & Trades (`codec`, magic `0x445A`).
//! - [`MidpointProcessor`] - Midpoint (`codec_midpoint`, magic `0x4D44`).
//! - [`MboProcessor`] - Market-by-Order (`codec_mbo`, magic `0x4444`): reconstructs the L3 book
//!   in [`crate::ingest::book`] and re-serves it as full-state `depth` + `trade`.

use std::{
    collections::{BTreeSet, HashMap},
    net::IpAddr,
};

use tracing::{debug, info, warn};

use crate::{
    ingest::{
        arbiter::{StalenessFloor, WindowedDedup},
        book::{BookState, DeltaKind, DeltaOp},
        codec::{
            aggressor_side, apply_exponent, decode_frame, source_name, InstrumentDefinition,
            Message,
        },
        codec_mbo, codec_midpoint,
        receiver::{FrameCtx, FrameProcessor, SeqCheck, SeqTracker},
        subscriber::RefDataState,
    },
    model::{
        DepthSnapshot, FeedMessage, NormalizedDepth, NormalizedInstrument, NormalizedMidpoint,
        NormalizedQuote, NormalizedTrade,
    },
};

/// How many price levels per side a `depth` snapshot carries.
const DEPTH_LEVELS: usize = 10;

/// Recent trade IDs remembered per (venue, symbol) for cross-publisher trade dedup. Const for now;
/// promote to config alongside the multi-publisher trade test that can size it.
const TRADE_DEDUP_WINDOW: usize = 8192;

/// The raw top-of-book identity of a quote, used as the per-tick content key for the quote dedup
/// floor. Compares the raw fixed-point BBO fields **plus the instrument exponents** and EXCLUDES
/// `source_ts` (tracked separately by the floor): two BBOs with the same raw ints but different
/// exponents are distinct prices, and the exponents are carried so a registry/precision change can't
/// false-dedup them. Identical content at the same `source_ts` — a republish or the other
/// publisher's copy — is a true duplicate.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct QuoteContent {
    bid_price_raw: i64,
    bid_qty_raw: u64,
    ask_price_raw: i64,
    ask_qty_raw: u64,
    price_exponent: i8,
    qty_exponent: i8,
}

/// Insert or replace an instrument definition in the shared snapshot, warning if an existing
/// entry for the same `(venue, symbol)` carries different exponents. When one venue is served by
/// multiple feeds (e.g. Hyperliquid TOB + MBO), both write the same key; they are expected to
/// agree on precision, so a disagreement is a publisher inconsistency worth surfacing rather than
/// silently clobbering.
fn upsert_instrument(instruments: &crate::model::InstrumentSnapshot, inst: &NormalizedInstrument) {
    let key = (inst.venue.clone(), inst.symbol.clone());
    let mut map = instruments.lock().unwrap();
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
    /// state separate.
    seq: HashMap<IpAddr, SeqTracker>,
    /// Log the manifest `Valid=0` publisher workaround once, not on every (~1/s) manifest.
    warned_invalid_manifest: bool,
    /// Log an unregistered quote SourceID once, not on every quote.
    warned_source_mismatch: bool,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
    /// Cross-publisher quote dedup: a per-(venue, symbol) `source_ts` staleness floor keyed on raw
    /// BBO content. Keeps every distinct top-of-book change at the newest `source_ts` (incl. multiple
    /// distinct BBOs sharing a `source_ts` — real intra-tick changes), but drops a strictly-older BBO
    /// from a lagging publisher (stale: the market moved on) and any exact `(source_ts, content)`
    /// duplicate. Output `source_ts` is non-decreasing per (venue, symbol).
    quote_dedup: StalenessFloor<(String, String), QuoteContent>,
    /// Cross-publisher trade dedup by venue trade_id per (venue, symbol).
    trade_dedup: WindowedDedup<(String, String), u64>,
}

impl TobProcessor {
    pub fn new(emit_trades: bool) -> Self {
        Self {
            state: RefDataState::new(),
            seq: HashMap::new(),
            warned_invalid_manifest: false,
            warned_source_mismatch: false,
            emit_trades,
            quote_dedup: StalenessFloor::new(),
            trade_dedup: WindowedDedup::new(TRADE_DEDUP_WINDOW),
        }
    }

    /// Whether this quote should be forwarded under the per-(venue, symbol) staleness floor: true if
    /// `source_ts` is at or beyond the floor AND its `content` is new at that tick. Drops strictly-
    /// older (stale laggard) BBOs and exact `(source_ts, content)` duplicates; keeps distinct intra-
    /// tick BBO changes.
    fn admit_quote(
        &mut self,
        venue: &str,
        symbol: &str,
        source_ts: u64,
        content: QuoteContent,
    ) -> bool {
        self.quote_dedup
            .admit((venue.to_string(), symbol.to_string()), source_ts, content)
    }

    /// Whether this trade should be forwarded: false if its trade_id was seen recently.
    fn admit_trade(&mut self, venue: &str, symbol: &str, trade_id: u64) -> bool {
        self.trade_dedup
            .is_new((venue.to_string(), symbol.to_string()), trade_id)
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
            match self.seq.entry(ctx.publisher).or_default().check(
                header.channel_id,
                header.reset_count,
                header.sequence,
            ) {
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
                    let _ = ctx.tx.send(FeedMessage::Instrument(inst));
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
                    let quote = NormalizedQuote {
                        // Venue is the wire SourceID's registered venue (2 -> Phoenix); anything
                        // unregistered (the source_id 3 Hyperliquid superset incl. HIP-3 builder
                        // DEXs) falls back to the feed default (Hyperliquid). So venues are exactly
                        // Hyperliquid + Phoenix; the builder DEX, if any, stays in the symbol.
                        venue: source_name(q.source_id).unwrap_or(ctx.venue).to_string(),
                        symbol: def.symbol.clone(),
                        bid: apply_exponent(q.bid_price_raw, def.price_exponent),
                        ask: apply_exponent(q.ask_price_raw, def.price_exponent),
                        bid_size: apply_exponent(q.bid_qty_raw as i64, def.qty_exponent),
                        ask_size: apply_exponent(q.ask_qty_raw as i64, def.qty_exponent),
                        source_ts_ns: q.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    // Cross-publisher dedup on a per-(venue, symbol) source_ts staleness floor keyed
                    // on raw BBO content: a strictly-older sample from a lagging publisher is stale
                    // (the market moved on) and is dropped, as is an exact (source_ts, content)
                    // duplicate, but a distinct BBO change at the current source_ts is kept. Output
                    // source_ts is non-decreasing per (venue, symbol).
                    let content = QuoteContent {
                        bid_price_raw: q.bid_price_raw,
                        bid_qty_raw: q.bid_qty_raw,
                        ask_price_raw: q.ask_price_raw,
                        ask_qty_raw: q.ask_qty_raw,
                        price_exponent: def.price_exponent,
                        qty_exponent: def.qty_exponent,
                    };
                    if self.admit_quote(&quote.venue, &quote.symbol, quote.source_ts_ns, content) {
                        let _ = ctx.tx.send(FeedMessage::Quote(quote));
                    }
                }
                Message::Trade(t) if handle_quotes && quotes_fresh => {
                    // Same per-instrument precision gate as quotes: a trade is dropped until we
                    // hold its definition, so we never emit a price without knowing its exponents.
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
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    if self.emit_trades
                        && self.admit_trade(&trade.venue, &trade.symbol, trade.trade_id)
                    {
                        let _ = ctx.tx.send(FeedMessage::Trade(trade));
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
            !matches!(
                self.seq
                    .check(header.channel_id, header.reset_count, header.sequence),
                SeqCheck::Stale
            )
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
                    let _ = ctx.tx.send(FeedMessage::Instrument(inst));
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
                    let _ = ctx.tx.send(FeedMessage::Midpoint(midpoint));
                }
                _ => {}
            }
        }
    }
}

/// Market-by-Order processor: drives the reference-data state machine (refdata port), feeds order
/// deltas and the snapshot stream into a per-instrument [`BookState`] (mktdata + snapshot ports),
/// and emits a full-state `depth` snapshot whenever an instrument's top-N changes - plus `trade`
/// prints. The reconstructed book lives here so consumers never see raw deltas (PROTOCOL.md).
pub struct MboProcessor {
    state: RefDataState<codec_mbo::InstrumentDefinition>,
    books: HashMap<u32, BookState>,
    /// Shared latest-depth map the WS server replays on connect.
    depth: DepthSnapshot,
    warned_source_mismatch: bool,
    /// Whether to emit `trade` messages (false when another feed owns this venue's trades).
    emit_trades: bool,
}

impl MboProcessor {
    pub fn new(depth: DepthSnapshot, emit_trades: bool) -> Self {
        Self {
            state: RefDataState::new(),
            books: HashMap::new(),
            depth,
            warned_source_mismatch: false,
            emit_trades,
        }
    }

    /// Build and broadcast a full-state `depth` snapshot for one instrument, updating the shared
    /// replay map. No-op unless the book is synced and the instrument's precision is known.
    fn emit_depth(&mut self, instrument_id: u32, ctx: &FrameCtx) {
        let Some(book) = self.books.get(&instrument_id) else {
            return;
        };
        if !book.is_synced() {
            return;
        }
        let Some(def) = self.state.definition(instrument_id) else {
            return; // precision unknown; don't emit a book we can't scale
        };
        let (bids_raw, asks_raw) = book.top_levels(DEPTH_LEVELS);
        let scale = |levels: Vec<(i64, u64)>| -> Vec<[f64; 2]> {
            levels
                .into_iter()
                .map(|(p, q)| {
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
            bids: scale(bids_raw),
            asks: scale(asks_raw),
            source_ts_ns: book.last_event_ts(),
            recv_ts_ns: ctx.recv_ts_ns,
            kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
            ws_send_ts_ns: 0, // stamped by the WS server just before send
        };
        self.depth
            .lock()
            .unwrap()
            .insert((depth.venue.clone(), depth.symbol.clone()), depth.clone());
        let _ = ctx.tx.send(FeedMessage::Depth(depth));
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
                    self.state
                        .on_manifest(m.valid, m.manifest_seq, m.instrument_count);
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
                    let _ = ctx.tx.send(FeedMessage::Instrument(inst));
                }
                codec_mbo::Message::EndOfSession(ts) => info!(ts, "mbo end of session"),
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
                    if self.books.entry(o.instrument_id).or_default().on_delta(op) {
                        changed.insert(o.instrument_id);
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
                    if self.books.entry(o.instrument_id).or_default().on_delta(op) {
                        changed.insert(o.instrument_id);
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
                    if self.books.entry(o.instrument_id).or_default().on_delta(op) {
                        changed.insert(o.instrument_id);
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
                            let _ = ctx.tx.send(FeedMessage::Trade(trade));
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
                        let _ = ctx.tx.send(FeedMessage::Trade(trade));
                    }
                }
                codec_mbo::Message::InstrumentReset(r) => {
                    self.books
                        .entry(r.instrument_id)
                        .or_default()
                        .on_instrument_reset(r.new_anchor_seq);
                }
                codec_mbo::Message::SnapshotBegin(s) => {
                    self.books
                        .entry(s.instrument_id)
                        .or_default()
                        .on_snapshot_begin(
                            s.snapshot_id,
                            s.anchor_seq,
                            s.total_orders,
                            s.last_instrument_seq,
                        );
                }
                codec_mbo::Message::SnapshotOrder(s) => {
                    // SnapshotOrder carries only the snapshot_id, not the instrument id; route it to
                    // whichever book is currently assembling that snapshot. snapshot_id is monotonic
                    // per (channel, instrument) - not globally unique - but the spec forbids
                    // interleaving snapshot groups across instruments, so at most one book is
                    // `building` at a time and only it matches.
                    for book in self.books.values_mut() {
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
                    if self
                        .books
                        .entry(s.instrument_id)
                        .or_default()
                        .on_snapshot_end(s.anchor_seq, s.snapshot_id)
                    {
                        changed.insert(s.instrument_id);
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

    use super::{upsert_instrument, MboProcessor, QuoteContent, TobProcessor};
    use crate::{
        ingest::{
            codec_mbo::{
                tests::{enc_order_add, enc_snapshot_begin, enc_snapshot_end, frame},
                OrderAdd, SnapshotBegin, SnapshotEnd, MSG_INSTRUMENT_DEFINITION,
                MSG_MANIFEST_SUMMARY, SIDE_ASK, SIDE_BID,
            },
            receiver::{FrameCtx, FrameProcessor, PortRole},
        },
        model::{DepthSnapshot, FeedMessage, NormalizedInstrument},
    };

    #[test]
    fn tob_quote_dedup_is_staleness_floor() {
        let mut p = TobProcessor::new(true);
        // Two distinct BBO contents that can share a source_ts (a real intra-tick change).
        let c1 = QuoteContent {
            bid_price_raw: 100,
            bid_qty_raw: 5,
            ask_price_raw: 101,
            ask_qty_raw: 8,
            price_exponent: -2,
            qty_exponent: 0,
        };
        let c2 = QuoteContent {
            bid_price_raw: 100,
            bid_qty_raw: 6, // bid size changed
            ..c1
        };
        // Per-(venue, symbol) staleness floor.
        assert!(p.admit_quote("Hyperliquid", "BTC", 1000, c1)); // first sample emits
        assert!(!p.admit_quote("Hyperliquid", "BTC", 1000, c1)); // exact (ts, content) dup dropped
        assert!(p.admit_quote("Hyperliquid", "BTC", 1000, c2)); // NEW content at SAME tick -> KEPT
        assert!(!p.admit_quote("Hyperliquid", "BTC", 999, c1)); // strictly older (stale) dropped
        assert!(p.admit_quote("Hyperliquid", "BTC", 2000, c1)); // new tick emits; content set resets
        assert!(p.admit_quote("Hyperliquid", "ETH", 1000, c1)); // independent symbol's floor
    }

    #[test]
    fn tob_trade_dedup_drops_repeat() {
        let mut p = TobProcessor::new(true);
        assert!(p.admit_trade("Hyperliquid", "BTC", 7));
        assert!(!p.admit_trade("Hyperliquid", "BTC", 7));
        assert!(p.admit_trade("Hyperliquid", "BTC", 8));
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

    fn make_ctx<'a>(
        tx: &'a broadcast::Sender<FeedMessage>,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
    ) -> FrameCtx<'a> {
        FrameCtx {
            venue: "TV",
            tx,
            instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role,
            publisher: std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
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

    /// `depth` messages for a frame touching multiple instruments must arrive in ascending
    /// instrument_id order regardless of the wire order of their `OrderAdd`s. The invariant is
    /// guaranteed by draining a `BTreeSet<u32>` rather than a `HashSet`.
    #[test]
    fn mbo_depth_emit_order_is_ascending_instrument_id() {
        let (tx, mut rx) = broadcast::channel::<FeedMessage>(64);
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
            &make_ctx(&tx, &instruments, PortRole::Combined),
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
        let snap_ctx = make_ctx(&tx, &instruments, PortRole::Snapshot);
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
            &make_ctx(&tx, &instruments, PortRole::Mktdata),
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
            &make_ctx(&tx, &instruments, PortRole::Mktdata),
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
}
