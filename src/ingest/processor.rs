//! Per-protocol frame processors: the [`FrameProcessor`] implementations the receiver's shared
//! driver dispatches to. Each owns its protocol state (reference-data state machine, sequence
//! tracker, warn-once flags, book state) and turns decoded frames into normalized `FeedMessage`s.
//!
//! - [`TobProcessor`] - Top-of-Book & Trades (`codec`, magic `0x445A`).
//! - [`MidpointProcessor`] - Midpoint (`codec_midpoint`, magic `0x4D44`).
//! - [`MboProcessor`] - Market-by-Order (`codec_mbo`, magic `0x4444`): reconstructs the L3 book
//!   in [`crate::ingest::book`] and re-serves it as full-state `depth` + `trade`.

use std::collections::{HashMap, HashSet};

use tracing::{debug, info, warn};

use crate::{
    ingest::{
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

/// Top-of-Book & Trades processor: drives the reference-data state machine on the refdata stream
/// and emits normalized quotes (gated per-instrument on a known definition) on the market-data
/// stream. Holds the per-channel sequence tracker used to drop stale/out-of-order quote frames.
pub struct TobProcessor {
    state: RefDataState<InstrumentDefinition>,
    /// Per-channel frame sequence tracker, used to drop stale/out-of-order quote frames.
    seq: SeqTracker,
    /// Log the manifest `Valid=0` publisher workaround once, not on every (~1/s) manifest.
    warned_invalid_manifest: bool,
    /// Log an unregistered quote SourceID once, not on every quote.
    warned_source_mismatch: bool,
}

impl TobProcessor {
    pub fn new() -> Self {
        Self {
            state: RefDataState::new(),
            seq: SeqTracker::default(),
            warned_invalid_manifest: false,
            warned_source_mismatch: false,
        }
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
            match self
                .seq
                .check(header.channel_id, header.reset_count, header.sequence)
            {
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
                    ctx.instruments
                        .lock()
                        .unwrap()
                        .insert((inst.venue.clone(), inst.symbol.clone()), inst.clone());
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
                    let _ = ctx.tx.send(FeedMessage::Quote(quote));
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
                    let _ = ctx.tx.send(FeedMessage::Trade(trade));
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
                    ctx.instruments
                        .lock()
                        .unwrap()
                        .insert((inst.venue.clone(), inst.symbol.clone()), inst.clone());
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
}

impl MboProcessor {
    pub fn new(depth: DepthSnapshot) -> Self {
        Self {
            state: RefDataState::new(),
            books: HashMap::new(),
            depth,
            warned_source_mismatch: false,
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
        // (coalescing many order events into a single full-state snapshot).
        let mut changed: HashSet<u32> = HashSet::new();

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
                    ctx.instruments
                        .lock()
                        .unwrap()
                        .insert((inst.venue.clone(), inst.symbol.clone()), inst.clone());
                    self.state.on_instrument_definition(d);
                    let _ = ctx.tx.send(FeedMessage::Instrument(inst));
                }
                codec_mbo::Message::EndOfSession(ts) => info!(ts, "mbo end of session"),
                codec_mbo::Message::OrderAdd(o) => {
                    let op = DeltaOp {
                        seq: o.per_instrument_seq,
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
                        let _ = ctx.tx.send(FeedMessage::Trade(trade));
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
                    let _ = ctx.tx.send(FeedMessage::Trade(trade));
                }
                codec_mbo::Message::InstrumentReset(r) => {
                    self.books
                        .entry(r.instrument_id)
                        .or_default()
                        .on_instrument_reset();
                }
                codec_mbo::Message::SnapshotBegin(s) => {
                    self.books
                        .entry(s.instrument_id)
                        .or_default()
                        .on_snapshot_begin(s.snapshot_id, s.last_instrument_seq);
                }
                codec_mbo::Message::SnapshotOrder(s) => {
                    // SnapshotOrder carries only the snapshot_id, not the instrument id; route it
                    // to whichever book is currently assembling that snapshot.
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
                        .on_snapshot_end(s.snapshot_id)
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
