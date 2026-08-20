//! Per-protocol datagram processors: the [`DatagramProcessor`] implementations the receiver's shared
//! driver dispatches to. Each owns its protocol state (reference-data state machine, sequence
//! tracker, warn-once flags, book state) and turns decoded datagrams into normalized `FeedMessage`s.
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
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use tracing::{debug, info, warn};

use crate::{
    ingest::{
        arbiter::{lock, Transport},
        authority::MarketKey,
        book::{BookState, DeltaKind, DeltaOp, Level, OrderChange},
        codec::{apply_exponent, decode_datagram, InstrumentDefinition, Message},
        codec_mbo, codec_mbp, codec_midpoint,
        pricebook::{
            BookDelta, DeltaOp as PriceDeltaOp, DeltaOutcome, Divergence, PriceBook,
            Status as BookStatus,
        },
        receiver::{DatagramCtx, DatagramProcessor, SeqCheck, SeqTracker},
        reconcile::TapeOwner,
        sources::source_label,
        subscriber::{InstrumentDef, RefDataState},
    },
    metrics::metrics,
    model::{
        category_arc, now_mono_ns, venue_arc, BookAction, BookChange, BookSide, DepthSnapshot,
        FeedMessage, NormalizedBook, NormalizedDepth, NormalizedInstrument, NormalizedMidpoint,
        NormalizedQuote, NormalizedTrade, Side,
    },
};

/// How many price levels per side a `depth` snapshot carries. `pub(crate)` so a reader of
/// `NormalizedDepth` (the query API sink) can tell a top-N slice that happens to be the whole book
/// from one that isn't, rather than a second, hand-copied constant silently drifting from this one.
pub(crate) const DEPTH_LEVELS: usize = 10;

/// Minimum gap between two decode-error log lines from one processor.
const DECODE_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Rate limit for a warning that can fire **per datagram**. A decode error is per-datagram, so a
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
/// single feed, so the per-datagram hot path increments a cached counter instead of doing a label-map
/// lookup. The processor doesn't know its venue until the first datagram (`ctx.venue`, fixed for the
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
/// inserted publisher is evicted (it simply re-anchors its sequence on its next datagram).
pub(crate) const MAX_PUBLISHERS: usize = 256;

/// Per-publisher reference-data state, bounded exactly like the per-publisher sequence map.
///
/// `reset_count` is scoped to `(source_ip, group, port)`, so two publishers sharing a port block
/// carry unrelated reset counters: under one shared [`RefDataState`] either path's restart clears the
/// other's instrument set, blanking both, since every emission path gates on a resolved definition.
/// The source IP is spoofable, so the map takes the same [`MAX_PUBLISHERS`] least-recently-inserted
/// eviction as the sequence map — an evicted publisher re-learns its definitions from the next
/// reference-data burst.
struct PerPublisher<D> {
    states: HashMap<IpAddr, RefDataState<D>>,
    /// Insertion order of `states` keys, oldest at the front, for the [`MAX_PUBLISHERS`] eviction.
    order: VecDeque<IpAddr>,
    /// The publisher [`Self::get`] most recently evicted, if any — set only when a `get()` call
    /// actually pops one, and meant to be drained via [`Self::take_evicted`] right after that call.
    /// A processor's own sibling per-publisher maps (`revealed`/`pending_channel`, keyed the same
    /// spoofable `(source_ip, instrument_id)` way `MAX_PUBLISHERS` exists to bound, but with no
    /// eviction path of their own) poll this to drop that publisher's entries in the same pass, so
    /// they stay bounded exactly as tightly as `states` is.
    last_evicted: Option<IpAddr>,
}

impl<D> Default for PerPublisher<D> {
    fn default() -> Self {
        // Not `#[derive(Default)]`: that would impose `D: Default`, which the definition types
        // don't (and needn't) implement - only the collections need defaulting.
        Self {
            states: HashMap::new(),
            order: VecDeque::new(),
            last_evicted: None,
        }
    }
}

impl<D: InstrumentDef> PerPublisher<D> {
    /// The state for `publisher`, **creating it on first sight** — reference-data writes only. A
    /// read must use [`Self::def`]: minting an entry from the market-data path would let a forged-
    /// source flood evict the real publishers' definitions without ever sending reference data.
    ///
    /// A `get()` on a not-yet-tracked publisher is the only place an eviction can happen (the cap is
    /// a fixed constant, so at most one pop per call); see [`Self::take_evicted`] for what a caller
    /// keeping sibling per-publisher state does with it.
    fn get(&mut self, publisher: IpAddr) -> &mut RefDataState<D> {
        if !self.states.contains_key(&publisher) {
            while self.states.len() >= MAX_PUBLISHERS {
                match self.order.pop_front() {
                    Some(old) => {
                        self.states.remove(&old);
                        self.last_evicted = Some(old);
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

    /// `publisher`'s state for mutation that must **not** mint one — a reset era can be observed on
    /// the market-data path, where [`Self::get`] would let a forged-source flood evict the real
    /// publishers' definitions.
    fn state_mut(&mut self, publisher: IpAddr) -> Option<&mut RefDataState<D>> {
        self.states.get_mut(&publisher)
    }

    /// The publisher evicted by the most recent [`Self::get`] call, if any — consumed once, so a
    /// caller polling after every `get()` can't act twice on the same eviction (and a caller that
    /// never polls simply never sees it, rather than it silently piling up).
    fn take_evicted(&mut self) -> Option<IpAddr> {
        self.last_evicted.take()
    }
}

/// Insert or replace an instrument definition in the shared snapshot, warning if an existing
/// entry for the same `(venue, category, channel, instrument_id)` carries different exponents.
/// When one universe is served by multiple feeds sharing a channel/instrument id (e.g. Hyperliquid
/// TOB + MBO, both `channel=0`, both the registry's default category), both write the same key;
/// they are expected to agree on precision, so a disagreement is a publisher inconsistency worth
/// surfacing rather than silently clobbering. `category` is part of the key precisely so this
/// never fires across two *different* universes that legitimately disagree on precision — see
/// `InstrumentSnapshot`'s doc.
fn upsert_instrument(instruments: &crate::model::InstrumentSnapshot, inst: &NormalizedInstrument) {
    let key = (
        inst.venue.clone(),
        inst.category.clone(),
        inst.channel,
        inst.instrument_id,
    );
    let mut map = crate::model::lock(instruments);
    if let Some(prev) = map.get(&key) {
        if prev.price_exponent != inst.price_exponent || prev.qty_exponent != inst.qty_exponent {
            warn!(
                venue = %inst.venue,
                category = %inst.category,
                symbol = %inst.symbol,
                channel = inst.channel,
                instrument_id = inst.instrument_id,
                prev_price_exp = prev.price_exponent,
                new_price_exp = inst.price_exponent,
                prev_qty_exp = prev.qty_exponent,
                new_qty_exp = inst.qty_exponent,
                "conflicting instrument definitions for the same (venue, category, channel, instrument_id) across feeds; last writer wins"
            );
        }
    }
    map.insert(key, inst.clone());
}

/// Remove one `(venue, category, channel, instrument_id)` entry from the shared snapshot — the
/// inverse of [`upsert_instrument`], which only ever inserts. There is no other removal path for
/// this map in the crate, so anything that stops naming an identity (chiefly a Source ID change:
/// `reveal_if_needed`'s `previous.is_some()` branch) must call this explicitly, or the stale entry
/// sits in the connect-time replay snapshot for the life of the process — describing a source
/// that no longer carries this data, alongside the correct one, with nothing on the wire saying
/// which is current.
fn remove_instrument(
    instruments: &crate::model::InstrumentSnapshot,
    venue: &Arc<str>,
    category: &Arc<str>,
    channel: u8,
    instrument_id: u32,
) {
    crate::model::lock(instruments).remove(&(
        venue.clone(),
        category.clone(),
        channel,
        instrument_id,
    ));
}

/// Top-of-Book & Trades processor: drives the reference-data state machine on the refdata feed
/// and emits normalized quotes (gated per-instrument on a known definition) on the market-data
/// feed. Holds the per-channel sequence tracker used to drop stale/out-of-order quote datagrams.
pub struct TobProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<InstrumentDefinition>,
    /// Per-publisher, per-channel datagram sequence tracker. Independent publishers mirror this feed
    /// onto one group sharing `channel_id=0`, so a single tracker would mark the slower publisher's
    /// datagrams stale and drop them before dedup; keying by source IP keeps each publisher's sequence
    /// state separate. Bounded to [`MAX_PUBLISHERS`] entries (the source IP is spoofable, so the map
    /// must not grow without limit); `seq_order` records insertion order for the eviction.
    seq: HashMap<IpAddr, SeqTracker>,
    /// Insertion order of `seq` keys, oldest at the front, for the [`MAX_PUBLISHERS`] eviction.
    seq_order: VecDeque<IpAddr>,
    /// Log the manifest `Valid=0` publisher workaround once, not on every (~1/s) manifest.
    warned_invalid_manifest: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether this receiver currently owns its venue's tape. A runtime flag, not a static one: a
    /// venue's rows are separately subscription-gated, so which of them serves the tape is the
    /// reconciler's decision and moves without respawning this task (see [`TapeOwner`]).
    tape: TapeOwner,
    /// Pre-resolved datagram-sequence metric children (bound lazily on the first datagram).
    seq_events: SeqEvents,
    /// The wire Source ID revealed for `(publisher, instrument)` — absent until the first `Quote`/
    /// `Trade` for the key (both carry one). See the module-level deferral note on
    /// [`MboProcessor::revealed`] for the full design; this is TOB's instance of the same pattern.
    revealed: HashMap<(IpAddr, u32), u16>,
    /// The channel `InstrumentDefinition` most recently arrived on for `(publisher, instrument)` —
    /// remembered because the deferred `NormalizedInstrument`, emitted once a price message
    /// reveals the instrument, is built outside that definition's own datagram and so no longer has
    /// its `header.channel_id` in scope. Refreshed on every definition burst (last-definition-wins,
    /// matching how `RefDataState.defs` itself already treats a same-`instrument_id` redefinition).
    /// Also what a Source ID change purges the stale `InstrumentSnapshot` entry by: the identity
    /// key is `(old_venue, channel, instrument_id)`, and `channel`/`instrument_id` don't change
    /// with the Source ID, so no separate "what was it last announced under" memo is needed the
    /// way a mutable `symbol` key used to require.
    pending_channel: HashMap<(IpAddr, u32), u8>,
}

impl TobProcessor {
    pub fn new(tape: TapeOwner) -> Self {
        Self {
            state: PerPublisher::default(),
            seq: HashMap::new(),
            seq_order: VecDeque::new(),
            warned_invalid_manifest: false,
            decode_warn: WarnRateLimit::default(),
            tape,
            seq_events: SeqEvents::default(),
            revealed: HashMap::new(),
            pending_channel: HashMap::new(),
        }
    }

    /// Emit the deferred `NormalizedInstrument` the moment `(publisher, instrument_id)` reveals a
    /// wire Source ID (the first `Quote`/`Trade` for it), and remember that it has been revealed.
    /// No-op if the id is unchanged from what's already remembered, or if no definition is known yet
    /// (nothing to announce).
    ///
    /// Also re-announces — under the SAME rules, not just once — if a later message names a
    /// DIFFERENT Source ID for a key already revealed: the wire is allowed to be wrong (this
    /// plan's own fixtures prove a publisher can mislabel every message it sends), and pinning the
    /// first id forever would leave the new venue with no definition anywhere once the wire moves
    /// on — not on the wire, not in `InstrumentSnapshot`, not in the WS replay map, not in the
    /// public feeder's precision gate — while quotes/trades kept naming themselves correctly from
    /// their own verbatim field. Counted in `dz_source_id_changed_total{venue}` (the new venue).
    ///
    /// A Source ID change also PURGES the stale `(old_venue, channel, instrument_id)` entry from
    /// `InstrumentSnapshot`: `upsert_instrument` only ever inserts, so without this the old entry
    /// would sit in the connect-time replay for the life of the process, describing a source that
    /// no longer carries this data. `channel`/`instrument_id` are unaffected by the Source ID
    /// change, so the current `pending_channel` value and this call's own `instrument_id` are
    /// exactly what the old entry was filed under — unlike the display `symbol`, the identity
    /// needs no separate "what was it last announced under" memo.
    fn reveal_if_needed(&mut self, ctx: &DatagramCtx, instrument_id: u32, source_id: u16) {
        let key = (ctx.publisher, instrument_id);
        let previous = self.revealed.get(&key).copied();
        if previous == Some(source_id) {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return;
        };
        let channel = self.pending_channel.get(&key).copied().unwrap_or(0);
        if let Some(old_id) = previous {
            metrics()
                .source_id_changed
                .with_label_values(&[source_label(source_id)])
                .inc();
            remove_instrument(
                ctx.instruments,
                &venue_arc(source_label(old_id)),
                &category_arc(ctx.category),
                channel,
                instrument_id,
            );
        }
        self.revealed.insert(key, source_id);
        let source = venue_arc(source_label(source_id));
        let inst = NormalizedInstrument {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol: def.symbol.clone(),
            channel,
            instrument_id,
            category: category_arc(ctx.category),
            price_exponent: def.price_exponent,
            qty_exponent: def.qty_exponent,
        };
        upsert_instrument(ctx.instruments, &inst);
        ctx.emit(FeedMessage::Instrument(inst));
    }

    /// Drop every `revealed`/`pending_channel` entry for `publisher`. Both are keyed by the same
    /// spoofable `(source_ip, instrument_id)` pair `MAX_PUBLISHERS` exists to bound, but neither has
    /// an eviction path of its own — called both when [`PerPublisher::get`] evicts `publisher` (see
    /// [`PerPublisher::take_evicted`]) and when a `ChannelReset` discards its whole definition set
    /// (the same remap risk `InstrumentReset` already guards in MBO/MBP: the old Source ID must not
    /// survive to misdescribe whatever this publisher's ids mean under the new era). Does NOT
    /// purge `InstrumentSnapshot`: that map is shared across every publisher of every venue, and a
    /// publisher going away (evicted, or reset) says nothing about whether its last-announced
    /// identity is still current — unlike a Source ID change, which does.
    fn forget_publisher(&mut self, publisher: IpAddr) {
        self.revealed.retain(|(p, _), _| *p != publisher);
        self.pending_channel.retain(|(p, _), _| *p != publisher);
    }

    /// The sequence tracker for `publisher`, creating it on first sight. The map is bounded to
    /// [`MAX_PUBLISHERS`]: when a *new* publisher would overflow it, the least-recently-inserted one
    /// is evicted first. Source IPs are spoofable, so this bound is what stops a forged-source flood
    /// from growing the map without limit; a legitimate publisher evicted under such a flood simply
    /// re-anchors (`SeqCheck::First`) on its next datagram, with no data loss beyond a stale-check reset.
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

impl DatagramProcessor for TobProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &DatagramCtx) {
        let (header, messages) = match decode_datagram(buf) {
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
            self.state
                .get(ctx.publisher)
                .on_datagram(header.reset_count);
            // `get()` above is the only place a new publisher can evict an old one this datagram
            // (every other `get()` call this function makes lands on an already-tracked publisher,
            // gated by the same `handle_refdata`); drop the evicted publisher's `revealed`/
            // `pending_channel` entries in the same pass, or they outlive `state` forever.
            if let Some(evicted) = self.state.take_evicted() {
                self.forget_publisher(evicted);
            }
        }

        // Per edge-feed-spec, the datagram Sequence Number is monotonically increasing per channel and
        // a `Reset Count` change signals a publisher reset. On the quote feed we drop only the stale
        // (out-of-order/replayed) datagrams - those whose sequence is below the last seen within the
        // same reset era - so an old datagram can never overwrite a fresher top-of-book. Forward
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
                        "dropping stale/out-of-order quote datagram (sequence below last seen)"
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
                    // A v1 `InstrumentDefinition` carries no wire Source ID, so this instrument's
                    // announcement is DEFERRED until the first `Quote`/`Trade` reveals one (see
                    // `reveal_if_needed`). A v3 definition carries its own (`d.source_id`) and is
                    // named below, right after its definition is stored. Remember the channel for
                    // the deferred case's later emission, and — if this instrument was already
                    // revealed by an earlier burst's price data, under the SAME id — re-announce
                    // now, exactly as an unconditional emission always did (the periodic
                    // reannounce this feed's manifest bursts already drive). If this definition's
                    // own `source_id` differs from what's already revealed, skip this and let the
                    // eager `reveal_if_needed` call below handle it instead: emitting here too,
                    // under the STALE id, would make correctness rest on the arbiter incidentally
                    // deduping a redundant broadcast rather than on this decision.
                    let key = (ctx.publisher, d.instrument_id);
                    self.pending_channel.insert(key, header.channel_id);
                    if let Some(&source_id) = self.revealed.get(&key) {
                        if d.source_id.is_none() || d.source_id == Some(source_id) {
                            let source = venue_arc(source_label(source_id));
                            let inst = NormalizedInstrument {
                                venue: source.clone(),
                                source: source.clone(),
                                source_id,
                                symbol: d.symbol.clone(),
                                channel: header.channel_id,
                                instrument_id: d.instrument_id,
                                category: category_arc(ctx.category),
                                price_exponent: d.price_exponent,
                                qty_exponent: d.qty_exponent,
                            };
                            upsert_instrument(ctx.instruments, &inst);
                            ctx.emit(FeedMessage::Instrument(inst));
                        }
                    }
                    let instrument_id = d.instrument_id;
                    let eager_source_id = d.source_id;
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    // `reveal_if_needed` requires the definition it just stored above, so this
                    // must come after `on_instrument_definition`, not before.
                    if let Some(source_id) = eager_source_id {
                        self.reveal_if_needed(ctx, instrument_id, source_id);
                    }
                }
                Message::ChannelReset(ts) if handle_refdata => {
                    warn!(ts, "channel reset; discarding reference-data state");
                    // A channel reset belongs to the publisher that sent it, not the port block.
                    *self.state.get(ctx.publisher) = RefDataState::new();
                    // The whole definition set just went with it; the old Source ID must not
                    // survive to misdescribe whatever this publisher's ids mean under the new
                    // era (see `forget_publisher`).
                    self.forget_publisher(ctx.publisher);
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
                    // until a *full* burst landed within a single valid manifest era.
                    // Gating per instrument lets each symbol's quotes flow the moment its
                    // definition is known, independent of the others.
                    let Some(def) = self.state.def(ctx.publisher, q.instrument_id) else {
                        continue; // no definition for this instrument yet; drop until we have it
                    };
                    let symbol = def.symbol.clone();
                    let (price_exponent, qty_exponent) = (def.price_exponent, def.qty_exponent);
                    // A `Quote` carries a wire Source ID; if this is the first for this instrument
                    // it reveals it, emitting the deferred `NormalizedInstrument` first.
                    self.reveal_if_needed(ctx, q.instrument_id, q.source_id);
                    // The wire Source ID is authoritative: `source_label` gives its registered
                    // name, or a stable synthesized label if it names no registry row. Resolved
                    // once as `&'static str` so the dedup key is allocation-free, and it is
                    // publisher-independent (mirrors that stamp the same id dedup against each
                    // other).
                    let venue: &'static str = source_label(q.source_id);
                    let source = venue_arc(venue);
                    let quote = NormalizedQuote {
                        venue: source.clone(),
                        source: source.clone(),
                        source_id: q.source_id,
                        symbol,
                        bid: apply_exponent(q.bid_price_raw, price_exponent),
                        ask: apply_exponent(q.ask_price_raw, price_exponent),
                        bid_size: apply_exponent(q.bid_qty_raw as i64, qty_exponent),
                        ask_size: apply_exponent(q.ask_qty_raw as i64, qty_exponent),
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
                    let symbol = def.symbol.clone();
                    let (price_exponent, qty_exponent) = (def.price_exponent, def.qty_exponent);
                    self.reveal_if_needed(ctx, t.instrument_id, t.source_id);
                    // Same identity `reveal_if_needed` announced this instrument's `instrument` under
                    // — see `pending_channel`'s doc. Read after the reveal call above so a trade that
                    // is itself the reveal still carries the channel that call just resolved.
                    let channel = self
                        .pending_channel
                        .get(&(ctx.publisher, t.instrument_id))
                        .copied()
                        .unwrap_or(0);
                    let venue: &'static str = source_label(t.source_id);
                    let source = venue_arc(venue);
                    let trade = NormalizedTrade {
                        venue: source.clone(),
                        source: source.clone(),
                        source_id: t.source_id,
                        symbol,
                        channel,
                        instrument_id: t.instrument_id,
                        category: category_arc(ctx.category),
                        price: apply_exponent(t.trade_price_raw, price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0, // stamped by the WS server just before send
                    };
                    // The arbiter's windowed trade dedup (on trade_id) collapses any cross-source
                    // copy downstream; this feed only gates on whether it owns this venue's trades.
                    if self.tape.load(Ordering::Relaxed) {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                _ => {}
            }
        }
    }
}

/// Midpoint processor: drives the reference-data state machine on the refdata feed and emits a
/// normalized mid price (gated per-instrument on a known definition) on the market-data feed.
/// Structurally parallel to [`TobProcessor`] but for the `0x4D44` sibling protocol.
pub struct MidpointProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<codec_midpoint::InstrumentDefinition>,
    seq: SeqTracker,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Pre-resolved datagram-sequence metric children (bound lazily on the first datagram).
    seq_events: SeqEvents,
    /// The wire Source ID revealed for `(publisher, instrument)` — absent until the first
    /// `Midpoint` for the key. See [`MboProcessor::revealed`] for the full deferral design.
    revealed: HashMap<(IpAddr, u32), u16>,
    /// The channel `InstrumentDefinition` most recently arrived on for `(publisher, instrument)` —
    /// see [`TobProcessor::pending_channel`], including why a Source ID change's
    /// `InstrumentSnapshot` purge reads its current value directly rather than a separate memo.
    pending_channel: HashMap<(IpAddr, u32), u8>,
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
            decode_warn: WarnRateLimit::default(),
            seq_events: SeqEvents::default(),
            revealed: HashMap::new(),
            pending_channel: HashMap::new(),
        }
    }

    /// Emit the deferred `NormalizedInstrument` the moment `(publisher, instrument_id)` reveals a
    /// wire Source ID (the first `Midpoint` for it), and re-announce if a later `Midpoint` names a
    /// DIFFERENT id for a key already revealed. See [`TobProcessor::reveal_if_needed`], including
    /// the `InstrumentSnapshot` purge on a Source ID change.
    fn reveal_if_needed(&mut self, ctx: &DatagramCtx, instrument_id: u32, source_id: u16) {
        let key = (ctx.publisher, instrument_id);
        let previous = self.revealed.get(&key).copied();
        if previous == Some(source_id) {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return;
        };
        let channel = self.pending_channel.get(&key).copied().unwrap_or(0);
        if let Some(old_id) = previous {
            metrics()
                .source_id_changed
                .with_label_values(&[source_label(source_id)])
                .inc();
            remove_instrument(
                ctx.instruments,
                &venue_arc(source_label(old_id)),
                &category_arc(ctx.category),
                channel,
                instrument_id,
            );
        }
        self.revealed.insert(key, source_id);
        let source = venue_arc(source_label(source_id));
        let inst = NormalizedInstrument {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol: def.symbol.clone(),
            channel,
            instrument_id,
            category: category_arc(ctx.category),
            price_exponent: def.price_exponent,
            // A mid price has no size, so there is no qty exponent on the Midpoint feed; report 0
            // in the shared snapshot (consumers ignore it for mids) — same as the previous
            // unconditional emission.
            qty_exponent: 0,
        };
        upsert_instrument(ctx.instruments, &inst);
        ctx.emit(FeedMessage::Instrument(inst));
    }

    /// Drop every `revealed`/`pending_channel` entry for `publisher` — see
    /// [`TobProcessor::forget_publisher`]. Midpoint has no `ChannelReset` message of its own, so
    /// this is called only from [`PerPublisher::take_evicted`].
    fn forget_publisher(&mut self, publisher: IpAddr) {
        self.revealed.retain(|(p, _), _| *p != publisher);
        self.pending_channel.retain(|(p, _), _| *p != publisher);
    }
}

impl DatagramProcessor for MidpointProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &DatagramCtx) {
        let (header, messages) = match codec_midpoint::decode_datagram(buf) {
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
            self.state
                .get(ctx.publisher)
                .on_datagram(header.reset_count);
            // See `TobProcessor::on_datagram`: this is the only `get()` call this function makes
            // that can evict a publisher.
            if let Some(evicted) = self.state.take_evicted() {
                self.forget_publisher(evicted);
            }
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
                    // `InstrumentDefinition` carries no wire Source ID; deferred until the first
                    // `Midpoint` reveals one (see `reveal_if_needed`). Remember the channel for
                    // that later emission, and re-announce now if already revealed by an earlier
                    // burst (the periodic reannounce this feed's manifest bursts already drive).
                    let key = (ctx.publisher, d.instrument_id);
                    self.pending_channel.insert(key, header.channel_id);
                    if let Some(&source_id) = self.revealed.get(&key) {
                        let source = venue_arc(source_label(source_id));
                        let inst = NormalizedInstrument {
                            venue: source.clone(),
                            source: source.clone(),
                            source_id,
                            symbol: d.symbol.clone(),
                            channel: header.channel_id,
                            instrument_id: d.instrument_id,
                            category: category_arc(ctx.category),
                            price_exponent: d.price_exponent,
                            qty_exponent: 0,
                        };
                        upsert_instrument(ctx.instruments, &inst);
                        ctx.emit(FeedMessage::Instrument(inst));
                    }
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                }
                codec_midpoint::Message::EndOfSession(ts) if handle_refdata => {
                    info!(ts, "midpoint end of session");
                }
                codec_midpoint::Message::Midpoint(mp) if handle_mids && mids_fresh => {
                    let Some(def) = self.state.def(ctx.publisher, mp.instrument_id) else {
                        continue; // no definition yet; drop until we know precision
                    };
                    let symbol = def.symbol.clone();
                    let price_exponent = def.price_exponent;
                    self.reveal_if_needed(ctx, mp.instrument_id, mp.source_id);
                    let venue: &'static str = source_label(mp.source_id);
                    let source = venue_arc(venue);
                    let midpoint = NormalizedMidpoint {
                        venue: source.clone(),
                        source: source.clone(),
                        source_id: mp.source_id,
                        symbol,
                        mid: apply_exponent(mp.mid_price_raw, price_exponent),
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

/// Minimum gap between two reveal-forced full-book republishes for one `(publisher, instrument)` — see
/// [`MboProcessor::reveal_rebaseline_due`]. Matches the arbiter's own definition re-announce interval,
/// since the republish exists to give the consumer a book under the identity that reveal announced.
const REVEAL_REBASELINE_INTERVAL_NS: u64 = 15_000_000_000; // 15s

/// Cap on the number of distinct instrument books [`MboProcessor`] tracks. The MBO `instrument_id`
/// is an unauthenticated, spoofable wire field, so without a bound a forged feed could mint a
/// `BookState` per distinct id and grow the map without limit (memory-exhaustion DoS) — the same
/// threat the [`MAX_PUBLISHERS`] cap addresses for the per-publisher sequence map. Real venues
/// carry far fewer instruments than this, so it never evicts in normal operation; once full, the
/// least-recently-inserted book is evicted (it simply re-syncs from the next snapshot).
const MAX_BOOKS: usize = 4096;

/// Market-by-Order processor: drives the reference-data state machine (refdata port), feeds order
/// deltas and the snapshot feed into a per-instrument [`BookState`] (mktdata + snapshot ports),
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
    /// floor by this memo rather than the *current* definition, which can differ: a manifest era
    /// bump may reassign the id to another symbol, and clearing the new symbol would leave the
    /// wedged old-symbol entry latched. Written in `emit_depth`, evicted in lockstep with `books`,
    /// cleared on `EndOfSession` (the venue-wide clear covers everything), so its keys are always
    /// a subset of `books`' keys.
    emitted_symbol: HashMap<(IpAddr, u32), Arc<str>>,
    /// The wire Source ID revealed for `(publisher, instrument)` — absent until the FIRST
    /// delta-carrying message (`OrderAdd`/`OrderCancel`/`OrderExecute`/`Trade` — the snapshot
    /// machinery carries no such field: `SnapshotBegin`/`SnapshotOrder`/`SnapshotEnd` have none)
    /// arrives for this key, frozen at that first value thereafter ("once known, it stays known").
    ///
    /// Presence is the deferred-emission gate for the WHOLE instrument, not just `depth`: nothing —
    /// not the definition, not `depth` — reaches the wire for a key until this map holds an entry
    /// (see [`Self::reveal_if_needed`]). Keyed per `(publisher, instrument)` deliberately, not
    /// coarser: one of the registry's ids is a superset covering builder DEXs alongside the primary
    /// market, so distinct instruments on one feed can legitimately carry distinct ids — a
    /// per-publisher or per-channel cache would stamp some instruments with a neighbour's id, which
    /// is confidently wrong rather than visibly absent.
    ///
    /// Bounded by the same (pre-existing, not introduced by this deferral) ceiling
    /// `RefDataState.defs` already has: a key can only ever be revealed once
    /// `self.state.def(publisher, id)` resolves, and that map has no explicit per-instrument-count
    /// cap of its own (only the *outer* per-publisher map is bounded, by `MAX_PUBLISHERS`). This
    /// map's cardinality can never exceed `defs`'s, so it is not a new unbounded vector, only a
    /// companion of an existing one.
    revealed: HashMap<(IpAddr, u32), u16>,
    /// The channel `InstrumentDefinition` most recently arrived on for `(publisher, instrument)` —
    /// remembered because MBO's book key carries no channel dimension, so it isn't otherwise in
    /// scope when a market-data message reveals the instrument. Refreshed on every definition burst
    /// (last-definition-wins, matching `RefDataState.defs`'s own per-`instrument_id` semantics).
    /// Same bound reasoning as `revealed` above. Also what a Source ID change's `InstrumentSnapshot`
    /// purge reads its current value from directly — see [`TobProcessor::pending_channel`].
    pending_channel: HashMap<(IpAddr, u32), u8>,
    /// Order-level changes the current datagram's applied deltas produced, tagged with the instrument they
    /// touched. Reused across datagrams (cleared at the top of each) so the hot path allocates nothing per
    /// event, and bounded by one datagram's message count.
    datagram_changes: Vec<(u32, OrderChange)>,
    /// Scratch for the order changes one delta or one whole-book materialization produces, swapped
    /// out of `self` while `books` is borrowed. Transient within a call, never state.
    order_scratch: Vec<OrderChange>,
    /// The sync state last reported to the arbiter per `(publisher, instrument)`, so only transitions
    /// are reported. Evicted in lockstep with `books`, whose keys it mirrors.
    synced_reported: HashMap<(IpAddr, u32), bool>,
    /// When a reveal last forced a full-book republish per `(publisher, instrument)` — see
    /// [`MboProcessor::reveal_rebaseline_due`]. Same lifecycle as `synced_reported`.
    reveal_rebaselined_ns: HashMap<(IpAddr, u32), u64>,
    /// One-shot guard for the manifest `Valid=0` override warning (see the handler).
    warned_invalid_manifest: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether this receiver currently owns its venue's tape — see [`TobProcessor::tape`].
    tape: TapeOwner,
}

impl MboProcessor {
    pub fn new(depth: DepthSnapshot, tape: TapeOwner) -> Self {
        Self {
            state: PerPublisher::default(),
            books: HashMap::new(),
            books_order: VecDeque::new(),
            depth,
            last_top: HashMap::new(),
            emitted_symbol: HashMap::new(),
            revealed: HashMap::new(),
            pending_channel: HashMap::new(),
            datagram_changes: Vec::new(),
            order_scratch: Vec::new(),
            synced_reported: HashMap::new(),
            reveal_rebaselined_ns: HashMap::new(),
            warned_invalid_manifest: false,
            decode_warn: WarnRateLimit::default(),
            tape,
        }
    }

    /// Reveal `(publisher, instrument)` on its first delta-carrying message, emitting the deferred
    /// `NormalizedInstrument` first, and remember that it has been revealed. No-op (returns
    /// `false`) if the id is unchanged from what's already remembered, or if no definition is known
    /// yet (nothing to announce). Returns whether THIS call emitted an announcement, so callers
    /// whose revealing/changed message has no direct wire representation of its own
    /// (`OrderAdd`/`OrderCancel`) can force a full `depth` re-baseline — the book may already hold
    /// real content the consumer has never been shown under this (possibly new) identity.
    ///
    /// Also re-announces if a LATER message names a DIFFERENT Source ID for a key already revealed
    /// — see [`TobProcessor::reveal_if_needed`] for why pinning the first id forever is wrong.
    /// Counted in `dz_source_id_changed_total{venue}` (the new venue), and purges the stale
    /// `(old_venue, channel, instrument_id)` `InstrumentSnapshot` entry (see
    /// [`TobProcessor::reveal_if_needed`]'s doc comment).
    fn reveal_if_needed(&mut self, ctx: &DatagramCtx, instrument_id: u32, source_id: u16) -> bool {
        let key = (ctx.publisher, instrument_id);
        let previous = self.revealed.get(&key).copied();
        if previous == Some(source_id) {
            return false;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return false;
        };
        let channel = self.pending_channel.get(&key).copied().unwrap_or(0);
        if let Some(old_id) = previous {
            metrics()
                .source_id_changed
                .with_label_values(&[source_label(source_id)])
                .inc();
            // `emit_depth`'s `last_top` suppresses a re-broadcast when this publisher's top-N is
            // byte-for-byte unchanged from the last one it sent — which says nothing about whether
            // the IDENTITY that book was last shown under is still current. Left in place, a caller
            // that returns `true` here specifically BECAUSE the top-N didn't move (a `Trade` or a
            // deep delta, per this method's own doc comment) would still have its forced
            // re-baseline suppressed at `emit_depth`, leaving the new venue with an `instrument` and
            // no `depth` until the top-N next actually moves. `InstrumentReset` already clears this
            // same map for the identical reason (a remapped id must not inherit a stale suppression
            // memo); a Source ID change is the same case at the reveal layer instead of the id layer.
            self.last_top.remove(&key);
            remove_instrument(
                ctx.instruments,
                &venue_arc(source_label(old_id)),
                &category_arc(ctx.category),
                channel,
                instrument_id,
            );
        }
        self.revealed.insert(key, source_id);
        let source = venue_arc(source_label(source_id));
        let inst = NormalizedInstrument {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol: def.symbol.clone(),
            channel,
            instrument_id,
            category: category_arc(ctx.category),
            price_exponent: def.price_exponent,
            qty_exponent: def.qty_exponent,
        };
        upsert_instrument(ctx.instruments, &inst);
        ctx.emit(FeedMessage::Instrument(inst));
        true
    }

    /// Drop everything keyed on `publisher` — see [`TobProcessor::forget_publisher`]. MBO has no
    /// `ChannelReset` message of its own (per-instrument `InstrumentReset` already drops its own
    /// key's entry), so this is called only from [`PerPublisher::take_evicted`].
    ///
    /// Drops the **same set** [`Self::book_for`]'s eviction does, and for the same reason: a sibling
    /// map left behind outlives the state that gave it meaning. `synced_reported` is the one that
    /// corrupts rather than leaks — the arbiter reads a departed path's `synced = true` as a healthy
    /// peer and suppresses the surviving path's only re-baseline — and it cannot be corrected later,
    /// because `market_key` resolves through `revealed`. So the path is released here, while
    /// `revealed` still resolves it.
    fn forget_publisher(&mut self, publisher: IpAddr, ctx: &DatagramCtx) {
        let stale: Vec<(IpAddr, u32)> = self
            .synced_reported
            .iter()
            .filter(|((p, _), synced)| *p == publisher && **synced)
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            self.set_synced(key, false, ctx);
        }
        self.books.retain(|(p, _), _| *p != publisher);
        self.books_order.retain(|(p, _)| *p != publisher);
        self.last_top.retain(|(p, _), _| *p != publisher);
        self.emitted_symbol.retain(|(p, _), _| *p != publisher);
        self.synced_reported.retain(|(p, _), _| *p != publisher);
        self.reveal_rebaselined_ns
            .retain(|(p, _), _| *p != publisher);
        self.revealed.retain(|(p, _), _| *p != publisher);
        self.pending_channel.retain(|(p, _), _| *p != publisher);
    }

    /// The wire venue to key arbiter state by for `(publisher, instrument)`, or `None` before it
    /// has been revealed (nothing has been emitted for the key yet, so there is nothing to key by).
    /// `depth` content is admitted and floor-cleared under this exact venue (see `emit_depth`), so
    /// anything that reaches into the arbiter's per-venue state for this key — a floor clear, a
    /// WS-replay purge, a health report — must resolve it the same way or it targets a key nothing
    /// was ever filed under.
    fn wire_venue(&self, key: &(IpAddr, u32)) -> Option<&'static str> {
        self.revealed.get(key).copied().map(source_label)
    }

    /// Clear the depth floor (and its WS-replay entries) for every `(wire venue, symbol)` this
    /// processor has ever latched. The old single-venue sweep (`ctx.venue`) assumed one feed serves
    /// exactly one venue; it no longer does; a receiver's instruments can carry distinct wire Source
    /// IDs (one registry id is a superset spanning builder DEXs alongside the primary market), so
    /// there is no longer one venue string to sweep by. `emitted_symbol` is exactly the set of keys
    /// whose depth actually latched the floor, each resolved to its own last-known wire venue.
    /// This is a superset of `ctx.venue`'s old reach, not a subset: still a safe over-approximation
    /// (a spurious clear self-heals via full-state depth), same as the call sites it replaces.
    fn reset_all_known_depth_floors(&self, ctx: &DatagramCtx, reason: &'static str) {
        let mut arb = lock(ctx.arbiter);
        for (key, symbol) in &self.emitted_symbol {
            if let Some(venue) = self.wire_venue(key) {
                arb.reset_depth_floor_for_symbol(venue, symbol, reason);
            }
        }
    }

    /// Clear the raced order-event state for every market this processor has emitted a book under — the
    /// `EndOfSession` counterpart of [`Self::reset_all_known_depth_floors`]. A session boundary restarts
    /// the venue's order-id space, so the ended session's tombstones would refuse the new session's
    /// legitimately-reused ids. Scoped to the racing state only: it leaves the replay accumulator and
    /// the authority entry alone, which is what stops it tearing down a published book.
    ///
    /// **Every publisher's markets, matching the book reset that precedes it.** The handler drops all of
    /// them to `Recovering` and reports all of them unsynced, so leaving a peer's tombstones behind
    /// refuses the new session's re-used ids for exactly the markets nothing is serving any more — the
    /// `InstrumentReset` failure, one handler over. It also means an `EndOfSession` from a source
    /// holding no books of its own still clears the state of the ones that were just reset.
    fn reset_all_known_book_events(&self, ctx: &DatagramCtx) {
        let mut arb = lock(ctx.arbiter);
        for key in self.books.keys() {
            if let Some(market) = self.market_key(key, ctx) {
                // The session variant: a boundary restarts the venue's *clock* as well as its id
                // space, so the channel's venue-time frontier goes with the racing state. The
                // per-instrument seam (`InstrumentReset`) must not, which is why they are two calls.
                arb.reset_book_session_for_market(&market);
            }
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
    fn book_for(&mut self, instrument_id: u32, ctx: &DatagramCtx) -> Option<&mut BookState> {
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
                        self.synced_reported.remove(&old);
                        self.reveal_rebaselined_ns.remove(&old);
                        // Resolve the wire venue this evicted key's depth was actually filed under
                        // BEFORE dropping the cache entry that supplies it — the replay-map purge
                        // below must key by the same venue `emit_depth` used, not `ctx.venue`. `None`
                        // when the key was never revealed (nothing was ever filed, so nothing to
                        // purge).
                        let evicted_venue = self.wire_venue(&old);
                        self.revealed.remove(&old);
                        // NOT `pending_channel` here: it mirrors `RefDataState.defs`'s lifecycle
                        // (populated straight from refdata, independent of whether a book was ever
                        // built), not `books`'s — removing it on a `books` eviction would zero the
                        // channel (`unwrap_or(0)`) for a later reveal even though the definition,
                        // and the channel it arrived on, are both still known. It is instead bounded
                        // and evicted alongside `revealed` when `PerPublisher` evicts the publisher
                        // (see `forget_publisher`).
                        let (old_pub, old_id) = old;
                        let symbol_still_served = self.books.keys().any(|(_p, i)| *i == old_id);
                        if !symbol_still_served {
                            // Resolved against the evicted book's OWN publisher: reference data is
                            // per publisher, so two paths can map one id to different symbols.
                            if let (Some(def), Some(venue)) =
                                (self.state.def(old_pub, old_id), evicted_venue)
                            {
                                crate::model::lock(&self.depth)
                                    .remove(&(venue_arc(venue), def.symbol.clone()));
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
    /// replay map. No-op unless the book is synced, the instrument's precision is known, AND the
    /// instrument has been revealed (see [`Self::revealed`]) — deferral applies to `depth` exactly
    /// as it does to the definition: nothing is emitted for an instrument until its Source ID is
    /// known, so a book built purely from a snapshot must not reach the wire ahead of it either.
    fn emit_depth(&mut self, instrument_id: u32, ctx: &DatagramCtx) {
        let key = (ctx.publisher, instrument_id);
        let Some(&source_id) = self.revealed.get(&key) else {
            return;
        };
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
        // No message triggers a `depth` re-baseline directly (it is coalesced per datagram from
        // whichever deltas/snapshot messages touched the book this datagram); `source_id` is the one
        // this key was revealed under (the gate above guarantees it is `Some`).
        let venue: &'static str = source_label(source_id);
        let source = venue_arc(venue);
        let depth = NormalizedDepth {
            venue: source.clone(),
            source: source.clone(),
            source_id,
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

    /// Whether a reveal-forced full-book republish is due for `(publisher, instrument)`.
    ///
    /// A reveal fires whenever the wire Source ID for a key *changes*, and the wire is unauthenticated:
    /// one spoofed datagram flipping the id would otherwise make this materialize the whole book — up to
    /// `MAX_ORDERS_PER_BOOK` changes — once per datagram, at ~10^5 amplification. Rate-limited to the
    /// same interval the arbiter applies to the definition the reveal announces, so a genuine Source ID
    /// change still republishes promptly while a flip-flop costs one republish per interval. A snapshot
    /// install is deliberately not limited: it needs no rate limit because forging one costs the
    /// attacker the whole book.
    fn reveal_rebaseline_due(&mut self, key: (IpAddr, u32)) -> bool {
        let now = now_mono_ns();
        match self.reveal_rebaselined_ns.get(&key) {
            Some(&last) if now.saturating_sub(last) < REVEAL_REBASELINE_INTERVAL_NS => false,
            _ => {
                self.reveal_rebaselined_ns.insert(key, now);
                true
            }
        }
    }

    /// Apply one order delta, recording the instrument as touched, as changed if the book took it, and
    /// collecting the order-level change it produced for this datagram's `book`.
    fn apply_delta(
        &mut self,
        instrument_id: u32,
        op: DeltaOp,
        ctx: &DatagramCtx,
        changed: &mut BTreeSet<u32>,
        touched: &mut BTreeSet<u32>,
    ) {
        touched.insert(instrument_id);
        let mut produced = std::mem::take(&mut self.order_scratch);
        produced.clear();
        if let Some(book) = self.book_for(instrument_id, ctx) {
            if book.on_delta_reporting(op, &mut produced) {
                changed.insert(instrument_id);
            }
        }
        self.datagram_changes
            .extend(produced.iter().map(|c| (instrument_id, *c)));
        self.order_scratch = produced;
    }

    /// The market this instrument's order-level `book` is published under, or `None` before the
    /// instrument is revealed (nothing has been emitted for it, so there is no market yet). Resolved
    /// exactly as `emit_depth` resolves its venue, so a health report and an emission cannot land on
    /// different keys.
    fn market_key(&self, key: &(IpAddr, u32), ctx: &DatagramCtx) -> Option<MarketKey> {
        let venue = self.wire_venue(key)?;
        let channel = self.pending_channel.get(key).copied().unwrap_or(0);
        Some((venue_arc(venue), category_arc(ctx.category), channel, key.1))
    }

    /// Handle a delta-carrying message's Source ID: reveal the instrument if it moved, and decide what
    /// the datagram owes the `book` consumer as a result. Identical at all four delta-carrying call sites,
    /// so it lives here rather than four times over.
    ///
    /// A reveal moves the instrument to a **new** `MarketKey`, under which the consumer has seen
    /// nothing. Publishing the datagram's incremental changes there is the one outcome that must not
    /// happen: the consumer applies order updates onto a book it was never given, and the replay
    /// accumulator for that key never reaches `baselined()`, so new clients never see it either. So a
    /// reveal always says *something* — the whole book when [`Self::reveal_rebaseline_due`] allows it,
    /// and otherwise a bare `Clear`, which is honest, costs one change, and cannot be amplified by the
    /// forged-Source-ID flood the rate limit exists to bound.
    fn on_reveal(
        &mut self,
        ctx: &DatagramCtx,
        instrument_id: u32,
        source_id: u16,
        changed: &mut BTreeSet<u32>,
        rebaselined: &mut BTreeSet<u32>,
        cleared: &mut BTreeSet<u32>,
    ) {
        if !self.reveal_if_needed(ctx, instrument_id, source_id) {
            return;
        }
        changed.insert(instrument_id);
        if self.reveal_rebaseline_due((ctx.publisher, instrument_id)) {
            rebaselined.insert(instrument_id);
        } else {
            cleared.insert(instrument_id);
        }
    }

    /// Report this instrument's book sync state to the arbiter when it has changed. The arbiter's
    /// re-baseline suppression reads these, so a book that gaps must say so — otherwise a peer that is
    /// itself recovering would see a phantom healthy path and suppress the only re-baseline on offer.
    fn report_synced(&mut self, instrument_id: u32, ctx: &DatagramCtx) {
        let key = (ctx.publisher, instrument_id);
        let synced = self.books.get(&key).is_some_and(|b| b.is_synced());
        self.set_synced(key, synced, ctx);
    }

    /// [`Self::report_synced`] for a caller that already knows the state, and reports it *before* the
    /// book reaches it — an `InstrumentReset` clears the `revealed` entry that resolves the market, so
    /// waiting until the book has actually dropped leaves nothing to key the report by.
    ///
    /// Reported under **`key`'s own publisher**, not `ctx.publisher`: a feed-wide `EndOfSession` and a
    /// publisher eviction both report for paths other than the one whose datagram is being handled, and
    /// naming the wrong path there would leave the departed one's `synced = true` standing while
    /// clearing an innocent peer's.
    fn set_synced(&mut self, key: (IpAddr, u32), synced: bool, ctx: &DatagramCtx) {
        if self.synced_reported.get(&key) == Some(&synced) {
            return;
        }
        let Some(market) = self.market_key(&key, ctx) else {
            return;
        };
        self.synced_reported.insert(key, synced);
        lock(ctx.arbiter).set_book_synced(&market, Transport::Edge(key.0), synced);
    }

    /// Emit the order-level `book` for one instrument — the real L3 product, carrying the venue's own
    /// `order_id` on every change. Gated exactly as [`Self::emit_depth`] is: nothing reaches the wire
    /// for an instrument whose book is unsynced, whose precision is unknown, or whose Source ID has not
    /// been revealed.
    ///
    /// [`BookEmit::Rebaseline`] materializes the whole book behind a `Clear` (`changes[0].action ==
    /// Clear` is what re-baselines a consumer; the `snapshot` flag is advisory) — for a snapshot
    /// install, and for a reveal, after which the consumer has never seen this identity's book at
    /// all. [`BookEmit::Clear`] is the same statement without the content, for a reveal the rate
    /// limit refused to materialize. Otherwise the datagram's applied changes are published as they
    /// came.
    fn emit_book(&mut self, instrument_id: u32, mode: BookEmit, ctx: &DatagramCtx) {
        let key = (ctx.publisher, instrument_id);
        let Some(&source_id) = self.revealed.get(&key) else {
            return;
        };
        let Some(book) = self.books.get(&key) else {
            return;
        };
        if !book.is_synced() {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return; // precision unknown; don't emit a book we can't scale
        };
        let (price_exponent, qty_exponent) = (def.price_exponent, def.qty_exponent);
        let symbol = def.symbol.clone();
        let source_ts_ns = book.last_event_ts();
        let scale = |c: &OrderChange| BookChange {
            // A zero resulting quantity is how the venue says the order is gone; `Delete` states that
            // to a consumer whose dispatcher branches on the action rather than inspecting the size.
            action: if c.qty_raw == 0 {
                BookAction::Delete
            } else {
                BookAction::Update
            },
            side: if c.is_bid {
                BookSide::Bid
            } else {
                BookSide::Ask
            },
            price: apply_exponent(c.price_raw, price_exponent),
            size: apply_exponent(c.qty_raw as i64, qty_exponent),
            order_id: c.order_id,
        };
        let mut changes = Vec::new();
        if mode != BookEmit::Delta {
            changes.push(BookChange {
                action: BookAction::Clear,
                side: BookSide::Both,
                price: 0.0,
                size: 0.0,
                order_id: 0,
            });
        }
        if mode == BookEmit::Rebaseline {
            let mut scratch = std::mem::take(&mut self.order_scratch);
            book.order_set(&mut scratch);
            changes.extend(scratch.iter().map(scale));
            self.order_scratch = scratch;
        } else if mode == BookEmit::Delta {
            changes.extend(
                self.datagram_changes
                    .iter()
                    .filter(|(id, _)| *id == instrument_id)
                    .map(|(_, c)| scale(c)),
            );
            // An event the book applied without touching any order — a cancel for an id it never held —
            // leaves nothing to say.
            if changes.is_empty() {
                return;
            }
        }
        let venue: &'static str = source_label(source_id);
        let source = venue_arc(venue);
        ctx.emit(FeedMessage::Book(NormalizedBook {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol,
            channel: self.pending_channel.get(&key).copied().unwrap_or(0),
            instrument_id,
            category: category_arc(ctx.category),
            order_level: true,
            changes,
            snapshot: mode != BookEmit::Delta,
            last: true,
            source_ts_ns,
            recv_ts_ns: ctx.recv_ts_ns,
            kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
            ws_send_ts_ns: 0, // stamped by the WS server just before send
        }));
    }
}

/// What one instrument's `book` emission says this datagram — see [`MboProcessor::emit_book`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookEmit {
    /// The datagram's applied changes, as they came.
    Delta,
    /// A bare `Clear`: discard this market, with no replacement content.
    Clear,
    /// A `Clear` followed by the book's whole current order set.
    Rebaseline,
}

impl DatagramProcessor for MboProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &DatagramCtx) {
        let (header, messages) = match codec_mbo::decode_datagram(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "mbo decode error: {e}");
                }
                return;
            }
        };

        let handle_refdata = ctx.role.handles_refdata();

        if handle_refdata {
            self.state
                .get(ctx.publisher)
                .on_datagram(header.reset_count);
            // This is the only `get()` call this function makes that can evict a publisher: every
            // other `get()` call below (`ManifestSummary`/`InstrumentDefinition`) is now gated on
            // this same `handle_refdata`, so it lands on a publisher already inserted by the line
            // above and never triggers a fresh eviction of its own. Before this gate existed, a
            // forged datagram to the Mktdata/Snapshot port carrying those two message types could
            // still call `get()` — decode doesn't care what physical port a message type arrives
            // on — evicting a publisher this drain never saw, unboundedly growing `pending_channel`
            // for a publisher that never sent a single byte of real reference data.
            if let Some(evicted) = self.state.take_evicted() {
                self.forget_publisher(evicted, ctx);
            }
        }

        // Instruments whose book changed this datagram; depth is emitted once per datagram per instrument
        // (coalescing many order events into a single full-state snapshot). BTreeSet gives
        // deterministic ascending instrument_id order across datagrams touching multiple instruments.
        let mut changed: BTreeSet<u32> = BTreeSet::new();
        // Instruments whose book the datagram *reached*, changed or not: a delta that opened a gap drops
        // the book to `Recovering` without changing it, and the arbiter must hear about that.
        let mut touched: BTreeSet<u32> = BTreeSet::new();
        // Instruments whose whole book must be republished: a snapshot install, or a reveal, after which
        // the consumer has never seen this identity's book.
        let mut rebaselined: BTreeSet<u32> = BTreeSet::new();
        // Instruments a reveal moved to a new `MarketKey` that the rate limit refused to republish
        // whole: they owe the consumer a bare `Clear` rather than this datagram's incrementals — see
        // [`MboProcessor::on_reveal`].
        let mut cleared: BTreeSet<u32> = BTreeSet::new();
        self.datagram_changes.clear();

        for msg in messages {
            match msg {
                codec_mbo::Message::ManifestSummary(m) if handle_refdata => {
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
                codec_mbo::Message::InstrumentDefinition(d) if handle_refdata => {
                    // A v1 `InstrumentDefinition` carries no wire Source ID; deferred until the
                    // first delta-carrying message reveals one (see `reveal_if_needed`). A v3
                    // definition carries its own (`d.source_id`) and is named below, right after
                    // its definition is stored. Remember the channel for the deferred case's later
                    // emission, and — if already revealed by an earlier burst, under the SAME id
                    // — re-announce now (the periodic reannounce this feed's manifest bursts
                    // already drive). If this definition's own `source_id` differs from what's
                    // already revealed, skip this and let the eager `reveal_if_needed` call below
                    // handle it: see `TobProcessor`'s definition path for why.
                    let key = (ctx.publisher, d.instrument_id);
                    self.pending_channel.insert(key, header.channel_id);
                    if let Some(&source_id) = self.revealed.get(&key) {
                        if d.source_id.is_none() || d.source_id == Some(source_id) {
                            let source = venue_arc(source_label(source_id));
                            let inst = NormalizedInstrument {
                                venue: source.clone(),
                                source: source.clone(),
                                source_id,
                                symbol: d.symbol.clone(),
                                channel: header.channel_id,
                                instrument_id: d.instrument_id,
                                category: category_arc(ctx.category),
                                price_exponent: d.price_exponent,
                                qty_exponent: d.qty_exponent,
                            };
                            upsert_instrument(ctx.instruments, &inst);
                            ctx.emit(FeedMessage::Instrument(inst));
                        }
                    }
                    let instrument_id = d.instrument_id;
                    let eager_source_id = d.source_id;
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    // Discard the re-baseline signal deliberately: no book exists yet for a
                    // freshly-defined instrument (only refdata has arrived), so forcing one here
                    // would emit a spurious empty `depth`. `reveal_if_needed` also requires the
                    // definition just stored above, so this must come after
                    // `on_instrument_definition`, not before.
                    if let Some(source_id) = eager_source_id {
                        let _ = self.reveal_if_needed(ctx, instrument_id, source_id);
                    }
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
                    // SCOPE TRAP: `books` can hold several publishers' books (they share a port
                    // block, and so one processor), while the floor cleared below is venue-wide and
                    // shared. A mirror that loses its own EndOfSession datagram keeps a `Synced`
                    // book and can re-latch the cleared floor at the old high-water, wedging the
                    // venue's depth until that mirror resets on its own. Resetting every
                    // publisher's book here is what closes that; a per-venue session era shared
                    // across the receiver tasks is what would close it properly.
                    for book in self.books.values_mut() {
                        book.on_end_of_session();
                    }
                    // EVERY book just dropped to `Recovering`, so every one of them must say so —
                    // the reset above is feed-wide, and reporting only this publisher's would leave
                    // the arbiter reading a peer's stale `synced = true` as a healthy path and
                    // suppressing the surviving path's only re-baseline. `touched` can carry only
                    // this publisher's (it is keyed by instrument id alone), so the peers are
                    // reported here.
                    let peers: Vec<(IpAddr, u32)> = self
                        .books
                        .keys()
                        .filter(|(p, _)| *p != ctx.publisher)
                        .copied()
                        .collect();
                    for key in peers {
                        self.set_synced(key, false, ctx);
                    }
                    touched.extend(
                        self.books
                            .keys()
                            .filter(|(p, _)| *p == ctx.publisher)
                            .map(|(_, id)| *id)
                            .collect::<Vec<_>>(),
                    );
                    // Sweep every (wire venue, symbol) this processor has latched — not a single
                    // `ctx.venue` (see `reset_all_known_depth_floors`) — BEFORE clearing the memo
                    // that supplies it.
                    self.reset_all_known_depth_floors(ctx, "end_of_session");
                    self.reset_all_known_book_events(ctx);
                    self.last_top.clear();
                    self.emitted_symbol.clear();
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
                    self.apply_delta(o.instrument_id, op, ctx, &mut changed, &mut touched);
                    // `OrderAdd` has no direct wire representation on our output (raw order events
                    // are never re-served), so a reveal here forces a `depth` re-baseline —
                    // otherwise a delta that didn't move the visible top-N would reveal the
                    // instrument's definition but never follow it with any book content.
                    self.on_reveal(
                        ctx,
                        o.instrument_id,
                        o.source_id,
                        &mut changed,
                        &mut rebaselined,
                        &mut cleared,
                    );
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
                    self.apply_delta(o.instrument_id, op, ctx, &mut changed, &mut touched);
                    self.on_reveal(
                        ctx,
                        o.instrument_id,
                        o.source_id,
                        &mut changed,
                        &mut rebaselined,
                        &mut cleared,
                    );
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
                    self.apply_delta(o.instrument_id, op, ctx, &mut changed, &mut touched);
                    self.on_reveal(
                        ctx,
                        o.instrument_id,
                        o.source_id,
                        &mut changed,
                        &mut rebaselined,
                        &mut cleared,
                    );
                    // An execution is also a public trade print; emit it like a Top-of-Book trade.
                    // `reveal_if_needed` above guarantees this instrument is revealed whenever a
                    // definition exists (the only way it could still return `false`), so no
                    // separate "is this revealed yet" check is needed here.
                    if let Some(def) = self.state.def(ctx.publisher, o.instrument_id) {
                        let venue: &'static str = source_label(o.source_id);
                        let source = venue_arc(venue);
                        // Same identity `reveal_if_needed` above just announced (or already holds)
                        // this instrument's `instrument` under — see `pending_channel`'s doc.
                        let channel = self
                            .pending_channel
                            .get(&(ctx.publisher, o.instrument_id))
                            .copied()
                            .unwrap_or(0);
                        let trade = NormalizedTrade {
                            venue: source.clone(),
                            source: source.clone(),
                            source_id: o.source_id,
                            symbol: def.symbol.clone(),
                            channel,
                            instrument_id: o.instrument_id,
                            category: category_arc(ctx.category),
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
                        if self.tape.load(Ordering::Relaxed) {
                            ctx.emit(FeedMessage::Trade(trade));
                        }
                    }
                }
                codec_mbo::Message::Trade(t) => {
                    let Some(def) = self.state.def(ctx.publisher, t.instrument_id) else {
                        continue;
                    };
                    let symbol = def.symbol.clone();
                    let (price_exponent, qty_exponent) = (def.price_exponent, def.qty_exponent);
                    // A Trade doesn't touch the book, but the book may already hold real content
                    // from an earlier (silently applied) snapshot — if this is the reveal, force a
                    // `depth` re-baseline so that content isn't left permanently unshown.
                    self.on_reveal(
                        ctx,
                        t.instrument_id,
                        t.source_id,
                        &mut changed,
                        &mut rebaselined,
                        &mut cleared,
                    );
                    // Same identity `reveal_if_needed` above just announced (or already holds) this
                    // instrument's `instrument` under — see `pending_channel`'s doc.
                    let channel = self
                        .pending_channel
                        .get(&(ctx.publisher, t.instrument_id))
                        .copied()
                        .unwrap_or(0);
                    let venue: &'static str = source_label(t.source_id);
                    let source = venue_arc(venue);
                    let trade = NormalizedTrade {
                        venue: source.clone(),
                        source: source.clone(),
                        source_id: t.source_id,
                        symbol,
                        channel,
                        instrument_id: t.instrument_id,
                        category: category_arc(ctx.category),
                        price: apply_exponent(t.trade_price_raw, price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0,
                    };
                    if self.tape.load(Ordering::Relaxed) {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                codec_mbo::Message::InstrumentReset(r) => {
                    let key = (ctx.publisher, r.instrument_id);
                    // The book is about to drop to `Recovering`; report it while the `revealed` entry
                    // that resolves this key's market is still there to resolve it.
                    self.set_synced(key, false, ctx);
                    // Drop the stale suppression entry so the first depth after the book re-syncs is
                    // always published (and its timestamps are fresh), never suppressed against the
                    // pre-reset top-N. Per-publisher: only this publisher's book is resetting.
                    self.last_top.remove(&key);
                    // The `book` product's equivalent, and for the same reason: the reset drops
                    // `revealed` below, so the post-reset feed re-reveals — and a rate limit left
                    // standing from the pre-reset reveal would downgrade that republish to a bare
                    // clear, leaving the re-synced book unshown until the next snapshot rotation.
                    self.reveal_rebaselined_ns.remove(&key);
                    // Resolve the wire venue the pre-reset depth actually latched under BEFORE
                    // dropping the cache entry that supplies it. Same remap risk as `last_top`/
                    // `emitted_symbol`: a manifest era bump can reassign this instrument_id to a
                    // different market, and the old Source ID would then misdescribe the new one —
                    // it must not survive past this reset either.
                    let latched_venue = self.wire_venue(&key);
                    // Resolved BEFORE the removal for the same reason `set_synced` runs before it:
                    // `market_key` reaches through `revealed`, so afterwards it is `None` and the
                    // raced-state drop below would be dead code — leaving the ended session's
                    // tombstones to refuse the new session's re-used order ids, which drops those
                    // orders from every consumer's book until retention expires.
                    let market = self.market_key(&key, ctx);
                    self.revealed.remove(&key);
                    // The re-snapshot may anchor at a `source_ts` below the latched floor (e.g. the
                    // venue reset this instrument's clock); clear the `(venue, symbol)` floor entry
                    // so the post-reset depth re-opens the tick. The symbol is resolved in
                    // safest-first order:
                    //   1. `emitted_symbol` — the symbol this publisher's depth actually LATCHED
                    //      the floor under. The *current* definition can disagree: a manifest
                    //      era bump may have remapped the id to another symbol, and clearing the
                    //      new symbol would leave the wedged old-symbol entry latched.
                    //   2. The current definition — right whenever ids are venue-stable (this
                    //      publisher just never emitted depth for the id, e.g. the mirror latched
                    //      the floor).
                    //   3. Every wire venue this processor has latched anything under — the
                    //      definition can be transiently missing even for a symbol with a latched
                    //      entry (RefDataState clears all defs on a channel reset / manifest bump),
                    //      and a missing definition must not silently skip the clear: fall back to
                    //      the safe over-approximation (a spurious clear self-heals; a skipped one
                    //      can leave the exact permanent wedge this hatch exists to remove). There is
                    //      no longer one "venue-wide" sweep to fall back to (see
                    //      `reset_all_known_depth_floors`), since this feed's instruments can carry
                    //      distinct wire Source IDs.
                    let latched_symbol = self.emitted_symbol.get(&key).cloned().or_else(|| {
                        self.state
                            .def(ctx.publisher, r.instrument_id)
                            .map(|d| d.symbol.clone())
                    });
                    match (latched_symbol, latched_venue) {
                        (Some(symbol), Some(venue)) => {
                            lock(ctx.arbiter).reset_depth_floor_for_symbol(
                                venue,
                                &symbol,
                                "instrument_reset",
                            );
                        }
                        _ => self.reset_all_known_depth_floors(ctx, "instrument_reset"),
                    }
                    // The new session may reuse this instrument's order ids, so the raced state's
                    // tombstones must go with the book or they would refuse the reused ids.
                    if let Some(market) = market {
                        lock(ctx.arbiter).reset_book_events_for_market(&market);
                    }
                    // Reset the existing book directly — NOT via `book_for`, whose definition gate
                    // would skip the reset in the same transient-no-definition window as above
                    // (leaving a stale `Synced` book whose old sequences/event clock then reject
                    // the post-reset re-snapshot). A reset for a book we never built needs nothing.
                    if let Some(book) = self.books.get_mut(&key) {
                        book.on_instrument_reset(r.new_anchor_seq);
                    }
                }
                codec_mbo::Message::SnapshotBegin(s) => {
                    touched.insert(s.instrument_id);
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
                            rebaselined.insert(s.instrument_id);
                        }
                    }
                    touched.insert(s.instrument_id);
                }
                // BatchBoundary is an emission-coalescing hint; we already emit once per datagram.
                codec_mbo::Message::BatchBoundary(_, _) | codec_mbo::Message::Heartbeat => {}
                // Catches `Other`, and — since match guards don't count toward exhaustiveness —
                // also `ManifestSummary`/`InstrumentDefinition` on a role that doesn't handle
                // refdata (their guarded paths above fell through): the same silent drop
                // `handle_refdata`-gated paths get in `TobProcessor`/`MidpointProcessor`.
                _ => {}
            }
        }

        // Sync state first: the arbiter decides whether a re-baseline is safe from these, so a path must
        // have declared itself before the book it publishes arrives.
        for instrument_id in touched {
            self.report_synced(instrument_id, ctx);
        }
        for instrument_id in changed {
            self.emit_depth(instrument_id, ctx);
            // `depth` is full state, so a reveal never has to withhold it; only the incremental
            // product does. A snapshot install in the same datagram outranks the rate limit's bare
            // clear — it already has the content the clear would have withheld.
            let mode = if rebaselined.contains(&instrument_id) {
                BookEmit::Rebaseline
            } else if cleared.contains(&instrument_id) {
                BookEmit::Clear
            } else {
                BookEmit::Delta
            };
            self.emit_book(instrument_id, mode, ctx);
        }
    }
}

/// Cap on distinct `(publisher, channel, instrument)` books one Market-by-Price receiver tracks. The
/// wire `channel_id`/`instrument_id` and the datagram source IP are all unauthenticated and
/// spoofable, so this bounds a forged feed exactly as [`MAX_BOOKS`] does for the order-keyed
/// processor. Nothing may be sized off the instrument *count*: ids are sequential in today's
/// captures but a ticker hash would spread them sparsely across the whole `u32`.
const MAX_PRICE_BOOKS: usize = 4096;

/// Cap on distinct `(publisher, channel)` pairs whose reset counter and open snapshot group are
/// tracked. Both key components are unauthenticated wire data, so an unbounded map is a
/// memory-exhaustion vector; two paths across a handful of shards sit far below this.
const MAX_CHANNEL_KEYS: usize = 256;

/// Deltas [`MbpProcessor`] holds buffered **across every book** before the overflow policy fires —
/// distinct from `pricebook`'s per-book `MAX_BUFFERED_DELTAS` (2^18), which bounds nothing useful in
/// aggregate: [`MAX_PRICE_BOOKS`] books at that cap is 2^30 deltas. This is the bound that holds, and
/// it binds only once several instruments buffer heavily, since one alone clamps at 2^18. The spec's
/// own cold-start worst case is ~30 M buffered messages / ~1.4 GB, so an unbounded total is a
/// documented way to lose the process. On overflow the instrument holding the most buffered data is
/// dropped and marked `Gap`, recovering on its next snapshot like any other `Gap` instrument;
/// sustained overflow means the publisher's snapshot period is too long for this host's memory
/// budget, which is why it is counted.
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
    /// Whether the book took the rotation. A **declined** one still holds the route — its levels
    /// arrive either way, and only a route makes them attributable — but they are discarded
    /// instead of applied, and counted as declined rather than as orphans.
    ///
    /// Declining is the ordinary steady state, not an error: a book that is `Ready` and already
    /// past the rotation's `Last Instrument Seq` refuses it (§4.2), and publishers rotate
    /// snapshots continuously. Without this flag every level of every declined rotation scored as
    /// an orphan, which on the live Lashay perps feed is ~100% of all snapshot levels — burying
    /// the genuine anomaly the orphan counter exists to surface.
    accepted: bool,
}

/// Market-by-Price processor: drives reference data per publisher, feeds level deltas and the
/// snapshot feed into a [`PriceBook`] per `(publisher, channel, instrument)`, and emits the
/// incremental `book` product plus `trade` prints.
pub struct MbpProcessor {
    /// Per-publisher reference-data state (see [`PerPublisher`]).
    state: PerPublisher<codec_mbp::InstrumentDefinition>,
    /// One independent book per `(publisher, channel, instrument)`. Two paths mirror one feed but
    /// their per-instrument delta series are unrelated by construction, so their books can never be
    /// merged — which path reaches the wire is the authority gate's decision, downstream.
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
    /// The wire Source ID revealed for a book key — absent until the FIRST message that carries one
    /// (`LevelUpdate`/`BookClear`/`Trade` — the snapshot machinery carries no such field:
    /// `SnapshotBegin`/`SnapshotLevel`/`SnapshotEnd` have none) arrives for it, frozen at that first
    /// value thereafter ("once known, it stays known").
    ///
    /// Presence is the deferred-emission gate for the WHOLE market, not just `book`: nothing — not
    /// the definition, not `book` — reaches the wire for a key until this map holds an entry (see
    /// [`Self::reveal_if_needed`]). Keyed per `(publisher, channel, instrument)` deliberately, not
    /// coarser: one of the registry's ids is a superset covering builder DEXs alongside the primary
    /// market, so distinct instruments on one feed can legitimately carry distinct ids — a
    /// coarser cache would stamp some instruments with a neighbour's id, which is confidently wrong
    /// rather than visibly absent. Evicted in lockstep with `books` via [`Self::forget_book`] — every
    /// key that can reveal is routed through [`Self::ensure_book`] first (including the `Trade`
    /// handler, which otherwise touches no book content), so a `revealed` entry never outlives, or
    /// exists without, a matching `books` entry, and stays bounded exactly as tightly as
    /// [`MAX_PRICE_BOOKS`] already bounds `books`.
    revealed: HashMap<PriceBookKey, u16>,
    /// One-shot guard for the manifest `Valid=0` override warning (see the handler).
    warned_invalid_manifest: bool,
    /// Rate limit for the per-datagram decode-error warning.
    decode_warn: WarnRateLimit,
    /// Whether this receiver currently owns its venue's tape — see [`TobProcessor::tape`].
    tape: TapeOwner,
}

impl MbpProcessor {
    pub fn new(tape: TapeOwner) -> Self {
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
            revealed: HashMap::new(),
            warned_invalid_manifest: false,
            decode_warn: WarnRateLimit::default(),
            tape,
        }
    }

    /// Reveal a book key on its first delta/print-carrying message, emitting the deferred
    /// `NormalizedInstrument` first, and remember that it has been revealed. No-op (returns
    /// `false`) if the id is unchanged from what's already remembered, or if no definition is known
    /// yet (nothing to announce). Channel is `key.1` — MBP's book key already carries it, unlike
    /// MBO's.
    ///
    /// Also re-announces if a LATER message names a DIFFERENT Source ID for a key already revealed
    /// — see [`TobProcessor::reveal_if_needed`] for why pinning the first id forever is wrong.
    /// Counted in `dz_source_id_changed_total{venue}` (the new venue), and purges the stale
    /// `(old_venue, channel, instrument_id)` `InstrumentSnapshot` entry the same way (see there) —
    /// `key.1`/`key.2` are unaffected by the Source ID change, so no separate memo is needed.
    fn reveal_if_needed(&mut self, ctx: &DatagramCtx, key: PriceBookKey, source_id: u16) -> bool {
        let previous = self.revealed.get(&key).copied();
        if previous == Some(source_id) {
            return false;
        }
        let Some(def) = self.state.def(ctx.publisher, key.2) else {
            return false;
        };
        if let Some(old_id) = previous {
            metrics()
                .source_id_changed
                .with_label_values(&[source_label(source_id)])
                .inc();
            remove_instrument(
                ctx.instruments,
                &venue_arc(source_label(old_id)),
                &category_arc(ctx.category),
                ctx.canonical_channel(key.1),
                key.2,
            );
        }
        self.revealed.insert(key, source_id);
        let source = venue_arc(source_label(source_id));
        let inst = NormalizedInstrument {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol: def.symbol.clone(),
            channel: ctx.canonical_channel(key.1),
            instrument_id: key.2,
            category: category_arc(ctx.category),
            price_exponent: def.price_exponent,
            qty_exponent: def.qty_exponent,
        };
        upsert_instrument(ctx.instruments, &inst);
        ctx.emit(FeedMessage::Instrument(inst));
        true
    }

    /// The wire venue to key arbiter state by for a book key, or `None` before it has been
    /// revealed (nothing has been emitted for the key yet, so there is nothing to key by). `book`
    /// content is admitted (and its `MarketKey` built) under this exact venue (`send_book`, and the
    /// arbiter's own `b.venue.clone()` at admission), so anything that reaches into the arbiter's
    /// per-market state for this key — a health report — must resolve it the same way or it targets
    /// a `MarketKey` nothing was ever filed under.
    fn wire_venue(&self, key: &PriceBookKey) -> Option<&'static str> {
        self.revealed.get(key).copied().map(source_label)
    }

    /// One instrument's `(price, qty)` exponents, or `None` while its definition is unknown — the
    /// precision-before-price gate, copied out so the `state` borrow ends here.
    fn exponents(&self, publisher: IpAddr, instrument_id: u32) -> Option<(i8, i8)> {
        self.state
            .def(publisher, instrument_id)
            .map(|d| (d.price_exponent, d.qty_exponent))
    }

    /// Record this datagram's `Reset Count` for `(publisher, channel)`, returning the previous one.
    /// Bounded to [`MAX_CHANNEL_KEYS`] with least-recently-inserted eviction; an evicted live
    /// publisher simply re-anchors its baseline on its next datagram (reporting no reset for it).
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
        ctx: &DatagramCtx,
        channel: u8,
        instrument_id: u32,
    ) -> Option<PriceBookKey> {
        self.state.def(ctx.publisher, instrument_id)?;
        let key = (ctx.publisher, channel, instrument_id);
        if !self.books.contains_key(&key) {
            while self.books.len() >= MAX_PRICE_BOOKS {
                match self.books_order.pop_front() {
                    Some(old) => {
                        // Unhealthy before forgetting: this path no longer holds that market, and
                        // leaving the authority its last `healthy` report would keep electing it
                        // while a peer path's live book is dropped.
                        self.report_health(ctx, &old, false);
                        self.forget_book(&old);
                    }
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
        self.revealed.remove(key);
        if self
            .open
            .get(&(key.0, key.1))
            .is_some_and(|g| g.instrument_id == key.2)
        {
            self.open.remove(&(key.0, key.1));
        }
    }

    /// Drop every trace of `publisher` — the sibling of [`MboProcessor::forget_publisher`], called
    /// only from the [`PerPublisher::take_evicted`] drain.
    ///
    /// MBP carries more per-publisher state than its siblings, and all of it is derived from the
    /// reference data that was just evicted: the books keyed `(publisher, channel, instrument)`,
    /// their `revealed`/`health_reported` companions, and the per-`(publisher,
    /// channel)` snapshot group and reset-count memos. Leaving any of it behind would strand a book
    /// whose definition is gone — `exponents` can no longer resolve, so it can never emit again, but
    /// it still holds its buffered deltas against [`MAX_BUFFERED_DELTAS_ACROSS_BOOKS`] and its slot
    /// against [`MAX_PRICE_BOOKS`]. Routed through [`Self::forget_book`] so `buffered_total` stays
    /// in step, exactly as every other book-dropping path does.
    fn forget_publisher(&mut self, publisher: IpAddr) {
        let keys: Vec<PriceBookKey> = self
            .books
            .keys()
            .filter(|(p, _, _)| *p == publisher)
            .copied()
            .collect();
        for key in keys {
            self.forget_book(&key);
        }
        self.books_order.retain(|(p, _, _)| *p != publisher);
        // `forget_book` only clears an `open` group whose instrument matches the book it dropped; a
        // group open for an instrument that never got a book needs this sweep.
        self.open.retain(|(p, _), _| *p != publisher);
        self.last_reset.retain(|(p, _), _| *p != publisher);
        self.channel_order.retain(|(p, _)| *p != publisher);
        // Belt and braces: `forget_book` already removed these for every key that had a book, but a
        // reveal without a surviving book must not leave an orphan behind.
        self.revealed.retain(|(p, _, _), _| *p != publisher);
        self.health_reported.retain(|(p, _, _), _| *p != publisher);
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

    /// The key of the book holding the most buffered deltas among those matching `pick`, or `None`
    /// when none of them holds any.
    fn largest_buffer(&self, pick: impl Fn(&PriceBookKey) -> bool) -> Option<PriceBookKey> {
        self.books
            .iter()
            .filter(|(k, b)| pick(k) && b.buffered_len() > 0)
            .max_by_key(|(_, b)| b.buffered_len())
            .map(|(k, _)| *k)
    }

    /// §4.5: hold the cross-instrument buffer inside [`MAX_BUFFERED_DELTAS_ACROSS_BOOKS`] by dropping
    /// the largest buffer (`drop_buffer` marks that instrument `Gap` in the same step) until back
    /// under budget. Finding the largest is O(books), which is fine because overflow is rare and the
    /// check that gates it is O(1). Never takes the channel down: every other instrument keeps
    /// streaming and the dropped one recovers on its next snapshot.
    fn enforce_buffer_budget(&mut self, ctx: &DatagramCtx) {
        while self.buffered_total > MAX_BUFFERED_DELTAS_ACROSS_BOOKS {
            // The path that filled the budget is the one sending, so take its own largest buffer
            // first: a global maximum would let one flooding path cost a peer path its recovering book.
            let largest = self
                .largest_buffer(|(p, _, _)| *p == ctx.publisher)
                .or_else(|| self.largest_buffer(|_| true));
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

    /// Report one book's `Ready`-ness for its market, but only when it changed: an unhealthy path
    /// loses the market to its peer, so this is a transition signal rather than a per-datagram one.
    fn report_health(&mut self, ctx: &DatagramCtx, key: &PriceBookKey, healthy: bool) {
        if self.health_reported.get(key) == Some(&healthy) {
            return;
        }
        self.health_reported.insert(*key, healthy);
        // `self.wire_venue(key)` — not `ctx.venue` — to match the `MarketKey` the arbiter itself
        // builds off the emitted `book`'s own venue field on admission (`b.venue.clone()`): a
        // health report keyed by the feed row's static venue would target a `MarketKey` no book
        // was ever admitted under, once this feed's instruments carry distinct wire Source IDs.
        // `None` before this key is revealed: nothing was ever admitted for it, so there is no
        // `MarketKey` to report health against yet either — it self-heals once revealed (the
        // rebaseline that follows is what actually establishes the market).
        let Some(venue) = self.wire_venue(key) else {
            return;
        };
        // `ctx.category` for the same reason: the arbiter keys the market on the emitting row's
        // instrument universe, so a report filed without it targets nothing the gate ever admitted.
        // `ctx.canonical_channel`, not the raw `key.1`, so a mirror path's health report lands on
        // the SAME `MarketKey` `send_book` admits its `book` under — see `DatagramCtx::canonical_channel`.
        let market: MarketKey = (
            venue_arc(venue),
            category_arc(ctx.category),
            ctx.canonical_channel(key.1),
            key.2,
        );
        lock(ctx.arbiter).set_book_health(&market, Transport::Edge(key.0), healthy);
    }

    /// §4.9: discard everything a `Reset Count` change invalidated for one `(publisher, channel)` —
    /// its books and their open snapshot group, plus that publisher's reference data, whose
    /// `reset_count` era just ended. Routed from any port, since the change can be seen on market
    /// data first. `RefDataState` is per publisher rather than per channel, so a sharded publisher's
    /// reset clears every channel's definitions — an over-approximation that self-heals on the next
    /// reference-data burst.
    fn on_channel_reset(&mut self, ctx: &DatagramCtx, channel: u8, reset_count: u8) {
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
        if let Some(state) = self.state.state_mut(ctx.publisher) {
            state.on_datagram(reset_count);
        }
        metrics()
            .mbp_channel_resets
            .with_label_values(&[ctx.venue])
            .inc();
    }

    /// Count what a delta did. `Overflow` (the per-book level cap: a malformed or forged feed) is
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

    /// Emit a full re-baseline for one instrument: `Clear{Both}` then every level it now holds.
    /// `changes[0].action == Clear` is what re-baselines a consumer (the `snapshot` flag is
    /// advisory), so this is a batch rather than a distinct message type.
    fn emit_rebaseline(&self, ctx: &DatagramCtx, channel: u8, instrument_id: u32) {
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
            size: scaled_qty(qty_raw, qty_exp),
            order_id: 0,
        };
        let mut changes = vec![BookChange {
            action: BookAction::Clear,
            side: BookSide::Both,
            price: 0.0,
            size: 0.0,
            order_id: 0,
        }];
        changes.extend(book.bids().map(|(p, l)| level(BookSide::Bid, p, l.qty_raw)));
        changes.extend(book.asks().map(|(p, l)| level(BookSide::Ask, p, l.qty_raw)));
        self.send_book(ctx, channel, instrument_id, changes, true);
    }

    /// The one place a `book` reaches the arbiter: resolves the display symbol and the book's event
    /// clock, and refuses to publish a book that is not `Ready` OR not yet revealed (deferral
    /// applies to `book` exactly as it does to the definition — nothing is emitted for a market
    /// until its Source ID is known; this is the safety net every caller ultimately routes through,
    /// so a snapshot-only re-baseline from `emit_rebaseline` can never slip out ahead of a reveal).
    fn send_book(
        &self,
        ctx: &DatagramCtx,
        channel: u8,
        instrument_id: u32,
        changes: Vec<BookChange>,
        snapshot: bool,
    ) {
        let key = (ctx.publisher, channel, instrument_id);
        let Some(&source_id) = self.revealed.get(&key) else {
            return;
        };
        let Some(book) = self.books.get(&key) else {
            return;
        };
        if book.status() != BookStatus::Ready {
            return;
        }
        let Some(def) = self.state.def(ctx.publisher, instrument_id) else {
            return; // precision unknown; don't emit prices we can't scale
        };
        let venue: &'static str = source_label(source_id);
        let source = venue_arc(venue);
        ctx.emit(FeedMessage::Book(NormalizedBook {
            venue: source.clone(),
            source: source.clone(),
            source_id,
            symbol: def.symbol.clone(),
            channel: ctx.canonical_channel(channel),
            instrument_id,
            category: category_arc(ctx.category),
            order_level: false,
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

/// `BookClear`'s side, a value space of its own (it extends `Side` with `Both`). `None` for a value
/// the apply does not act on: the book clears nothing for it, so the wire must claim nothing either.
fn clear_book_side(clear_side: u8) -> Option<BookSide> {
    match clear_side {
        codec_mbp::CLEAR_SIDE_BID => Some(BookSide::Bid),
        codec_mbp::CLEAR_SIDE_ASK => Some(BookSide::Ask),
        codec_mbp::CLEAR_SIDE_BOTH => Some(BookSide::Both),
        _ => None,
    }
}

/// Scale a wire quantity. Saturates rather than reinterpreting: a `u64` past `i64::MAX` is nonsense
/// either way, but a negative `size` would contradict the book we hold.
fn scaled_qty(qty_raw: u64, exponent: i8) -> f64 {
    apply_exponent(qty_raw.min(i64::MAX as u64) as i64, exponent)
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

impl DatagramProcessor for MbpProcessor {
    fn on_datagram(&mut self, buf: &[u8], ctx: &DatagramCtx) {
        let (header, messages) = match codec_mbp::decode_datagram(buf) {
            Ok(v) => v,
            Err(e) => {
                if let Some(suppressed) = self.decode_warn.allow() {
                    warn!(role = ?ctx.role, suppressed, "mbp decode error: {e}");
                }
                return;
            }
        };
        // The channel comes from this codec's own datagram header rather than `DatagramCtx`: `drive` is
        // protocol-agnostic and would have to decode a header it has no magic for.
        let channel = header.channel_id;

        let handle_refdata = ctx.role.handles_refdata();

        if handle_refdata {
            self.state
                .get(ctx.publisher)
                .on_datagram(header.reset_count);
            // The only `get()` this function makes that can evict a publisher: the
            // `ManifestSummary`/`InstrumentDefinition` branches below are gated on this same
            // `handle_refdata`, so they land on a publisher already inserted here and never
            // trigger a fresh eviction of their own. Draining here is what keeps the derived
            // per-publisher maps (`books`/`revealed`/`open`/`last_reset`) from
            // outliving the reference data they are meaningless without — see
            // [`Self::forget_publisher`], and `MboProcessor`'s identical drain.
            if let Some(evicted) = self.state.take_evicted() {
                self.forget_publisher(evicted);
            }
        }
        // §4.9: a reset is any CHANGE of `Reset Count` — `!=`, never `>`, so the `255 -> 0` wrap is
        // not silently ignored while deltas keep applying against discarded publisher state.
        //
        // Tracked from the **market-data role only**, and only for a publisher we already hold
        // reference data for. The role restriction is what makes the era monotone: all three ports
        // carry the same era but are separate sockets with separate kernel queues, so one memo
        // shared across them would flip on every interleaving of a restart's backlog and re-reset the
        // channel thousands of times. One socket is FIFO, so the market-data era never goes
        // backwards. The publisher restriction keeps [`PerPublisher::get`]'s rule: a publisher with no
        // definitions has no books to invalidate, and minting state from the market-data path is how a
        // forged-source flood would evict the real publishers' definitions.
        if ctx.role.handles_mktdata()
            && self.state.state_mut(ctx.publisher).is_some()
            && self
                .note_reset_count(ctx.publisher, channel, header.reset_count)
                .is_some_and(|prev| prev != header.reset_count)
        {
            self.on_channel_reset(ctx, channel, header.reset_count);
        }

        // Wire changes per instrument, emitted once per datagram; a `BTreeMap` gives deterministic
        // ascending-id order across a multi-instrument datagram, matching `MboProcessor`'s `BTreeSet`.
        let mut accum: BTreeMap<u32, Vec<BookChange>> = BTreeMap::new();
        // Instruments touched since the previous `BatchBoundary`, and since the datagram started (for
        // the health sweep). Both are datagram-scoped: the publisher and channel are fixed per datagram.
        let mut since_boundary: BTreeSet<u32> = BTreeSet::new();
        let mut touched: BTreeSet<u32> = BTreeSet::new();
        // Instruments that revealed their Source ID for the FIRST time this datagram. Decided once,
        // at datagram's end (not inline per message): a reveal can be followed by more deltas for the
        // SAME instrument later in the SAME datagram, and those must still coalesce into ONE batch
        // with the reveal, not a second separate message — so the choice between "full
        // re-baseline" and "this datagram's incremental batch" is made per instrument after every
        // message has been applied, exactly where `accum` is already drained per instrument.
        let mut revealed_this_datagram: BTreeSet<u32> = BTreeSet::new();
        // Moved out so the `&mut self` book calls below can borrow it; put back before returning.
        let mut cleared = std::mem::take(&mut self.cleared);

        for msg in messages {
            match msg {
                codec_mbp::Message::ManifestSummary(m) if handle_refdata => {
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
                codec_mbp::Message::InstrumentDefinition(d) if handle_refdata => {
                    // A v1 `InstrumentDefinition` carries no wire Source ID; deferred until the
                    // first `LevelUpdate`/`BookClear`/`Trade` reveals one (see `reveal_if_needed`).
                    // A v3 definition carries its own (`d.source_id`) and is named below, right
                    // after its definition is stored. Re-announce now if this book key was already
                    // revealed by an earlier burst, under the SAME id (the periodic reannounce
                    // this feed's manifest bursts already drive). If this definition's own
                    // `source_id` differs from what's already revealed, skip this and let the
                    // eager `reveal_if_needed` call below handle it: see `TobProcessor`'s
                    // definition path for why.
                    let key = (ctx.publisher, channel, d.instrument_id);
                    if let Some(&source_id) = self.revealed.get(&key) {
                        if d.source_id.is_none() || d.source_id == Some(source_id) {
                            let source = venue_arc(source_label(source_id));
                            let inst = NormalizedInstrument {
                                venue: source.clone(),
                                source: source.clone(),
                                source_id,
                                symbol: d.symbol.clone(),
                                channel: ctx.canonical_channel(channel),
                                instrument_id: d.instrument_id,
                                category: category_arc(ctx.category),
                                price_exponent: d.price_exponent,
                                qty_exponent: d.qty_exponent,
                            };
                            upsert_instrument(ctx.instruments, &inst);
                            ctx.emit(FeedMessage::Instrument(inst));
                        }
                    }
                    let eager_source_id = d.source_id;
                    self.state.get(ctx.publisher).on_instrument_definition(d);
                    // Discard the re-baseline signal deliberately: no book exists yet for a
                    // freshly-defined instrument (only refdata has arrived), so forcing one here
                    // would emit a spurious empty `book`. `reveal_if_needed` also requires the
                    // definition just stored above, so this must come after
                    // `on_instrument_definition`, not before.
                    if let Some(source_id) = eager_source_id {
                        let _ = self.reveal_if_needed(ctx, key, source_id);
                    }
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
                    if self.reveal_if_needed(ctx, key, l.source_id) {
                        revealed_this_datagram.insert(l.instrument_id);
                    }
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
                                scaled_qty(l.qty_raw, qty_exp)
                            },
                            order_id: 0,
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
                    if self.reveal_if_needed(ctx, key, c.source_id) {
                        revealed_this_datagram.insert(c.instrument_id);
                    }
                    if !matches!(outcome, DeltaOutcome::Applied { .. }) {
                        continue;
                    }
                    let changes = accum.entry(c.instrument_id).or_default();
                    if c.scope == codec_mbp::SCOPE_ENTIRE_SIDE {
                        // Only for a side the apply acted on: an unrecognized byte clears nothing in
                        // the book, and a `Clear` would tell the consumer to drop a side we still
                        // hold — silently, with every later sequence check passing.
                        if let Some(side) = clear_book_side(c.clear_side) {
                            changes.push(BookChange {
                                action: BookAction::Clear,
                                side,
                                price: 0.0,
                                size: 0.0,
                                order_id: 0,
                            });
                        }
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
                            order_id: 0,
                        }));
                    }
                }
                codec_mbp::Message::SnapshotBegin(s) => {
                    // A group whose era disagrees with the market data's belongs to a different run
                    // of the publisher: the snapshot port is its own socket, so a restart leaves the
                    // previous era's rotation queued, and installing it would republish the dead
                    // session's book as a fresh re-baseline. Its levels are then counted as orphans.
                    // Before any market data the era is unknown, so the group is accepted.
                    if self
                        .last_reset
                        .get(&(ctx.publisher, channel))
                        .is_some_and(|era| *era != header.reset_count)
                    {
                        continue;
                    }
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
                                accepted: true,
                            },
                        );
                    } else if self
                        .open
                        .get(&group)
                        .is_none_or(|g| g.instrument_id != s.instrument_id)
                    {
                        // A refused begin with no route, or one for a DIFFERENT instrument than the
                        // one assembling (the publisher interleaved groups). Either way take the
                        // route for it: that keeps its levels out of the other instrument's book —
                        // what the old `remove` achieved — while still attributing them, so a
                        // declined rotation is not scored as an orphan.
                        //
                        // A refused re-begin for the SAME instrument leaves the route alone — the
                        // book deliberately keeps assembling (see `PriceBook::on_snapshot_begin`),
                        // and overwriting it would discard the levels of a live assembly.
                        self.open.insert(
                            group,
                            OpenGroup {
                                instrument_id: s.instrument_id,
                                snapshot_id: s.snapshot_id,
                                accepted: false,
                            },
                        );
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
                    if !group.accepted {
                        // The book declined this rotation because it is already synced past it. Its
                        // levels are expected, carry nothing the book needs, and must not be
                        // confused with an unroutable one.
                        metrics()
                            .mbp_declined_rotation_levels
                            .with_label_values(&[ctx.venue])
                            .inc();
                        continue;
                    }
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
                    let Some(open) = self
                        .open
                        .get(&group)
                        .copied()
                        .filter(|g| g.instrument_id == e.instrument_id)
                    else {
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
                    };
                    self.open.remove(&group);
                    if !open.accepted {
                        // A declined rotation closing. The route existed only to attribute its
                        // levels; there is no assembly to install, and calling into the book here
                        // would test sequences against a group it never opened.
                        continue;
                    }
                    let key = (ctx.publisher, channel, e.instrument_id);
                    touched.insert(e.instrument_id);
                    let installed = self
                        .with_book(&key, |b| b.on_snapshot_end(e.anchor_seq, e.snapshot_id))
                        .unwrap_or(false);
                    if installed {
                        // The re-baseline replaces everything accumulated for this instrument so
                        // far, and goes out here (not deferred to the end-of-datagram sweep below) so a
                        // delta later in the same datagram follows it as an incremental batch. Also
                        // clears `revealed_this_datagram` for it: an earlier message this same datagram
                        // may have already revealed this instrument, but the "needs a full
                        // re-baseline because it was just revealed" need is satisfied by the
                        // unconditional full re-baseline right here — leaving the entry would make
                        // the end-of-datagram sweep either double-emit an identical re-baseline (nothing
                        // else touches this instrument again this datagram) or turn a later delta's
                        // legitimate incremental batch into a second, redundant full one.
                        accum.remove(&e.instrument_id);
                        revealed_this_datagram.remove(&e.instrument_id);
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
                        // Same remap risk the health/report reset above guards: a manifest era
                        // bump can reassign this instrument_id to a different market, and the old
                        // Source ID would then misdescribe the new one. Drop it too, so the
                        // post-reset market is deferred again until a fresh delta reveals it.
                        self.revealed.remove(&key);
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
                    // §4.7: per-path and per-channel — the shard whose session ended. Dropping every
                    // publisher's books (as the order-keyed processor does) would tear down a live
                    // peer path's published book; reporting each market unhealthy is what hands
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
                    let symbol = def.symbol.clone();
                    let (price_exponent, qty_exponent) = (def.price_exponent, def.qty_exponent);
                    // A Trade doesn't touch the book, but the book may already hold real content
                    // from an earlier (silently applied) snapshot — if this is the reveal, the
                    // end-of-datagram sweep below forces a full re-baseline so that content isn't
                    // left permanently unshown, even though this message never touches `accum`.
                    // Routed through `ensure_book` (not a bare key tuple) so `revealed` never holds
                    // an entry `books` doesn't also hold — the same lockstep every other revealing
                    // message keeps (see `revealed`'s doc comment). A trade-only instrument's book
                    // simply never leaves `AwaitingSnapshot`, so no content ever actually reaches
                    // `send_book` for it; this only fixes the bookkeeping invariant, not emission.
                    if let Some(key) = self.ensure_book(ctx, channel, t.instrument_id) {
                        if self.reveal_if_needed(ctx, key, t.source_id) {
                            revealed_this_datagram.insert(t.instrument_id);
                        }
                    }
                    let venue: &'static str = source_label(t.source_id);
                    let source = venue_arc(venue);
                    let trade = NormalizedTrade {
                        venue: source.clone(),
                        source: source.clone(),
                        source_id: t.source_id,
                        symbol,
                        // `channel` is this datagram's header channel id, canonicalized for
                        // consumer-facing identity (see `DatagramCtx::canonical_channel`) — a mirror
                        // publisher's `N + offset` becomes the same `N` its peer path carries, so
                        // the history/catalog see one market rather than two. This is still the
                        // field that disambiguates a price-aggregated venue's mirrored paths
                        // (identical instrument set, distinct wire `channel` per path) from each
                        // other's identical-looking content, so it must ride the message rather
                        // than be re-derived downstream by a `symbol` match that can't tell them
                        // apart — the raw wire book/sequence state that actually keeps the two
                        // paths independent is untouched by this canonicalization.
                        channel: ctx.canonical_channel(channel),
                        instrument_id: t.instrument_id,
                        category: category_arc(ctx.category),
                        price: apply_exponent(t.trade_price_raw, price_exponent),
                        size: apply_exponent(t.trade_qty_raw as i64, qty_exponent),
                        aggressor_side: Side::from_code(t.aggressor_side),
                        trade_id: t.trade_id,
                        cumulative_volume: apply_exponent(
                            t.cumulative_volume_raw as i64,
                            qty_exponent,
                        ),
                        source_ts_ns: t.source_ts,
                        recv_ts_ns: ctx.recv_ts_ns,
                        kernel_rx_ts_ns: ctx.kernel_rx_ts_ns,
                        ws_send_ts_ns: 0,
                    };
                    if self.tape.load(Ordering::Relaxed) {
                        ctx.emit(FeedMessage::Trade(trade));
                    }
                }
                codec_mbp::Message::Heartbeat(_) | codec_mbp::Message::Other(_) => {}
                // Match guards don't count toward exhaustiveness, so this also catches
                // `ManifestSummary`/`InstrumentDefinition` on a role that doesn't handle refdata
                // (their guarded paths above fell through) — the same silent drop the
                // `handle_refdata`-gated paths get in the three sibling processors.
                _ => {}
            }
        }

        cleared.clear();
        self.cleared = cleared;
        self.enforce_buffer_budget(ctx);
        // One batch per instrument, `last: true`: a datagram is one logical event per instrument, since
        // cross-instrument atomicity is not promised. A clear that removed nothing has no changes to
        // publish.
        //
        // An instrument revealed THIS datagram always gets the full current book (`emit_rebaseline`),
        // never this datagram's own incremental `accum` batch alone — a consumer that has never seen
        // the market has no prior state an incremental update could apply against, and a reveal can
        // be followed by more deltas for the SAME instrument later in the SAME datagram, which must
        // still coalesce into that one re-baseline rather than a second, separate message.
        let accum_keys: BTreeSet<u32> = accum.keys().copied().collect();
        for (instrument_id, changes) in accum {
            if revealed_this_datagram.contains(&instrument_id) {
                self.emit_rebaseline(ctx, channel, instrument_id);
            } else if !changes.is_empty() {
                self.send_book(ctx, channel, instrument_id, changes, false);
            }
        }
        // Revealed this datagram via a message with no book-content representation of its own
        // (`Trade`) and no accompanying delta this datagram, so it never entered `accum` above — still
        // show whatever the book already holds (e.g. from an earlier silently-applied snapshot).
        for instrument_id in &revealed_this_datagram {
            if !accum_keys.contains(instrument_id) {
                self.emit_rebaseline(ctx, channel, *instrument_id);
            }
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

    use super::{MarketKey, MAX_PUBLISHERS};

    use tokio::sync::broadcast;

    use std::net::IpAddr;

    use super::{
        upsert_instrument, MboProcessor, MbpProcessor, TobProcessor, WarnRateLimit,
        MAX_BUFFERED_DELTAS_ACROSS_BOOKS, MAX_CHANNEL_KEYS, MAX_PRICE_BOOKS,
    };
    use crate::{
        ingest::{
            arbiter::{lock, Arbiter, SharedArbiter, Transport},
            codec_mbo::{
                tests::{
                    datagram, enc_end_of_session, enc_instrument_reset, enc_order_add,
                    enc_order_cancel, enc_snapshot_begin, enc_snapshot_end, enc_snapshot_order,
                },
                InstrumentReset, OrderAdd, OrderCancel, SnapshotBegin, SnapshotEnd, SnapshotOrder,
                MSG_INSTRUMENT_DEFINITION, MSG_MANIFEST_SUMMARY, SIDE_ASK, SIDE_BID,
            },
            codec_mbp::{self, tests as mbp_wire, SIDE_ASK as MBP_ASK, SIDE_BID as MBP_BID},
            pricebook::{BookDelta, DeltaOp as PriceDeltaOp, Status as BookStatus},
            receiver::{DatagramCtx, DatagramProcessor, PortRole},
        },
        metrics::metrics,
        model::{
            category_arc, venue_arc, BookAction, BookSide, DepthSnapshot, FeedMessage,
            NormalizedBook, NormalizedInstrument,
        },
    };

    /// A tape-ownership flag pinned to one value, for the tests that only need the static behaviour.
    fn tape(on: bool) -> super::TapeOwner {
        Arc::new(std::sync::atomic::AtomicBool::new(on))
    }

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

        let mut p = TobProcessor::new(tape(true));
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
    /// per-publisher state, so one path's restart must not clear the other path's instrument set —
    /// which would blank both paths until the next refdata burst, since every emission path gates on
    /// a resolved definition. Driven through `on_datagram` so it pins the wiring, not just the map.
    #[test]
    fn refdata_reset_is_scoped_to_the_publisher_that_reset() {
        use std::net::{IpAddr, Ipv4Addr};

        /// Rewrite a datagram's `reset_count` (datagram-header byte 21) to simulate a publisher restart.
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
        let mut proc = MboProcessor::new(depth, tape(false));
        let ctx_for = |publisher: IpAddr, role: PortRole| {
            let mut c = make_ctx(&arbiter, &instruments, role);
            c.publisher = publisher;
            c
        };
        let burst = datagram(&[
            enc_manifest_summary(1, 1),
            enc_instrument_def(0, "INST-0", 1),
        ]);
        let anchor = |sid: u32| {
            datagram(&[
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

        // A restarts: its `reset_count` bumps on A's datagrams only, clearing A's definitions. An
        // empty datagram, so the clear isn't immediately undone by the burst that would follow it.
        proc.on_datagram(
            &with_reset_count(datagram(&[]), 1),
            &ctx_for(pub_a, PortRole::Refdata),
        );

        // A is dark until its next burst (no definition -> no book -> no depth) ...
        proc.on_datagram(
            &datagram(&[add(1, 100, 7000)]),
            &ctx_for(pub_a, PortRole::Mktdata),
        );
        assert!(
            drain_depth_ts(&mut rx).is_empty(),
            "A's own definitions clear on its reset"
        );

        // ... but B, which never reset, keeps streaming.
        proc.on_datagram(
            &datagram(&[add(1, 101, 8000)]),
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

    /// Encode a v3 `InstrumentDefinition` wire message (130 bytes total, exponents=0) — the layout
    /// `TobProcessor`, `MboProcessor` and `MbpProcessor` all share (`codec_common`'s widened
    /// `InstrumentDefinition`), carrying its own `source_id` at body offset 4. Body layout matches
    /// `codec_common::instrument_definition`'s v3 offsets:
    ///   +0 instrument_id (u32le), +4 source_id (u16le), +6 symbol (64 B NUL-padded),
    ///   +70..+87 pad, +87 price_exponent (i8), +88 qty_exponent (i8),
    ///   +89..+124 pad, +124 manifest_seq (u16le).
    /// Total: 4 (hdr) + 126 (body) = 130 bytes.
    fn enc_instrument_def_v3(id: u32, source_id: u16, symbol: &str, manifest_seq: u16) -> Vec<u8> {
        enc_instrument_def_v3_with_exponents(id, source_id, symbol, manifest_seq, 0, 0)
    }

    /// [`enc_instrument_def_v3`], with explicit exponents — for the test that needs the arbiter's
    /// own precision-pair identity to differ between two bursts (so its rate limit can't
    /// incidentally collapse the very message under test; see
    /// `tob_v3_definition_id_change_emits_exactly_one_instrument`).
    fn enc_instrument_def_v3_with_exponents(
        id: u32,
        source_id: u16,
        symbol: &str,
        manifest_seq: u16,
        price_exponent: i8,
        qty_exponent: i8,
    ) -> Vec<u8> {
        let mut out = vec![MSG_INSTRUMENT_DEFINITION, 130, 0, 0];
        out.extend_from_slice(&id.to_le_bytes()); // body+0..+4
        out.extend_from_slice(&source_id.to_le_bytes()); // body+4..+6
        let mut sym = [0u8; 64];
        let sb = symbol.as_bytes();
        sym[..sb.len().min(64)].copy_from_slice(&sb[..sb.len().min(64)]);
        out.extend_from_slice(&sym); // body+6..+70
        out.extend_from_slice(&[0u8; 17]); // body+70..+87: pad
        out.push(price_exponent as u8); // body+87
        out.push(qty_exponent as u8); // body+88
        out.extend_from_slice(&[0u8; 35]); // body+89..+124: pad
        out.extend_from_slice(&manifest_seq.to_le_bytes()); // body+124..+126
        out
    }

    /// The single-publisher source IP `make_ctx` stamps on every datagram, so book-map keys in these
    /// tests are `(TEST_PUB, instrument_id)` (the MBO books re-key by publisher).
    const TEST_PUB: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));

    fn make_ctx<'a>(
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
    ) -> DatagramCtx<'a> {
        DatagramCtx {
            venue: "TV",
            category: "testcategory",
            arbiter,
            instruments,
            kernel_rx_ts_ns: 0,
            recv_ts_ns: 0,
            role,
            publisher: TEST_PUB,
            mirror_offset: None,
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

    /// TOB-magic (0x445A) datagram wrapper — same header shape as `codec_mbo::tests::datagram`, but with
    /// `codec::MAGIC` so these decode under `TobProcessor` rather than `MboProcessor`. `sequence` is
    /// explicit (unlike the MBO/MBP helpers' fixed `0`) so a caller can send several datagrams on one
    /// channel without a later one reading as stale against an earlier one.
    fn tob_datagram(sequence: u64, messages: &[Vec<u8>]) -> Vec<u8> {
        let body: Vec<u8> = messages.concat();
        let datagram_len = (crate::ingest::codec_common::DATAGRAM_HEADER_SIZE + body.len()) as u16;
        let mut f = Vec::new();
        f.extend_from_slice(&crate::ingest::codec::MAGIC.to_le_bytes());
        f.push(1); // schema version
        f.push(0); // channel
        f.extend_from_slice(&sequence.to_le_bytes());
        f.extend_from_slice(&0u64.to_le_bytes()); // send ts
        f.push(messages.len() as u8);
        f.push(0); // reset count
        f.extend_from_slice(&datagram_len.to_le_bytes());
        f.extend_from_slice(&body);
        f
    }

    /// [`tob_datagram`], but stamped with Schema Version 3 — for the tests exercising a v3
    /// `InstrumentDefinition`'s own Source ID.
    fn tob_datagram_v3(sequence: u64, messages: &[Vec<u8>]) -> Vec<u8> {
        let mut f = tob_datagram(sequence, messages);
        f[2] = 3;
        f
    }

    /// Minimal TOB `Quote` body encoder (60-byte body — matches `codec::tests::encode_quote_datagram`'s
    /// layout byte-for-byte) — enough to reveal `(publisher, instrument_id)` under a given Source ID.
    /// Content (prices/qtys) is arbitrary; these tests assert on the `Instrument` announcement, not
    /// the quote itself.
    fn enc_tob_quote(instrument_id: u32, source_id: u16, source_ts: u64) -> Vec<u8> {
        let mut b = vec![crate::ingest::codec::MSG_QUOTE, 60u8, 0, 0];
        b.extend_from_slice(&instrument_id.to_le_bytes());
        b.extend_from_slice(&source_id.to_le_bytes());
        b.push(0b11); // update_flags: both sides present
        b.push(0); // reserved
        b.extend_from_slice(&source_ts.to_le_bytes());
        b.extend_from_slice(&100i64.to_le_bytes()); // bid_price_raw
        b.extend_from_slice(&10u64.to_le_bytes()); // bid_qty_raw
        b.extend_from_slice(&101i64.to_le_bytes()); // ask_price_raw
        b.extend_from_slice(&10u64.to_le_bytes()); // ask_qty_raw
        b.extend_from_slice(&1u16.to_le_bytes()); // bid_n
        b.extend_from_slice(&1u16.to_le_bytes()); // ask_n
        b.extend_from_slice(&[0u8; 4]); // reserved -> 60 bytes total
        b
    }

    /// Minimal TOB `ChannelReset` body encoder (matches `codec::tests::channel_reset_decodes`'s
    /// layout: type/len/flags header + a `u64` ts).
    fn enc_tob_channel_reset(ts: u64) -> Vec<u8> {
        let mut b = vec![crate::ingest::codec::MSG_CHANNEL_RESET, 12u8, 0, 0];
        b.extend_from_slice(&ts.to_le_bytes());
        b
    }

    /// [`make_ctx`], but with an explicit publisher source IP — for the tests below that need
    /// several distinct (spoofable) publishers rather than the single fixed `TEST_PUB`.
    fn tob_ctx<'a>(
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
        publisher: IpAddr,
    ) -> DatagramCtx<'a> {
        let mut c = make_ctx(arbiter, instruments, role);
        c.publisher = publisher;
        c
    }

    /// A publisher naming a different Source ID for an already-revealed instrument is re-announced
    /// under the new id (not silently pinned to the first one seen), and counted. Without this, a
    /// venue's precision-before-price guarantee breaks for whichever id shows up second: no
    /// `Instrument` for it anywhere (not the wire, not `InstrumentSnapshot`, not the WS replay map).
    #[test]
    fn tob_source_id_change_reannounces_and_is_counted() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = |role| tob_ctx(&arbiter, &instruments, role, TEST_PUB);

        proc.on_datagram(
            &tob_datagram(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(41, "INST-41", 1),
                ],
            ),
            &ctx(PortRole::Combined),
        );
        proc.on_datagram(
            &tob_datagram(1, &[enc_tob_quote(41, 1, 1_000)]),
            &ctx(PortRole::Mktdata),
        );
        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.source_id, i.venue.to_string()));
            }
        }
        assert_eq!(seen, vec![(1, "HYPERLIQUID".to_string())], "first reveal");

        let before = metrics()
            .source_id_changed
            .with_label_values(&["PHOENIX"])
            .get();
        proc.on_datagram(
            &tob_datagram(2, &[enc_tob_quote(41, 2, 2_000)]),
            &ctx(PortRole::Mktdata),
        );
        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.source_id, i.venue.to_string()));
            }
        }
        assert_eq!(
            seen,
            vec![(2, "PHOENIX".to_string())],
            "a changed id is re-announced under the new venue"
        );
        assert_eq!(
            metrics()
                .source_id_changed
                .with_label_values(&["PHOENIX"])
                .get(),
            before + 1,
            "the change is counted, labelled by the NEW venue"
        );
    }

    /// The root-cause fix: a Source ID change purges the stale `(old_venue, symbol)` entry from
    /// `InstrumentSnapshot`. `upsert_instrument` only ever inserts and there is no other removal
    /// path for that map anywhere in the crate, so without this the old entry would sit in the
    /// connect-time replay snapshot for the life of the process — describing a source that no
    /// longer carries this data, alongside the correct one, with nothing saying which is current.
    /// Price-triggered (the general path every Source ID change goes through, predating and
    /// independent of the v3 eager reveal), so this pins the fix in `reveal_if_needed` itself
    /// rather than in a definition handler.
    #[test]
    fn tob_source_id_change_purges_the_stale_instrument_snapshot_entry() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = |role| tob_ctx(&arbiter, &instruments, role, TEST_PUB);

        proc.on_datagram(
            &tob_datagram(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(41, "INST-41", 1),
                ],
            ),
            &ctx(PortRole::Combined),
        );
        proc.on_datagram(
            &tob_datagram(1, &[enc_tob_quote(41, 1, 1_000)]),
            &ctx(PortRole::Mktdata),
        );
        proc.on_datagram(
            &tob_datagram(2, &[enc_tob_quote(41, 2, 2_000)]),
            &ctx(PortRole::Mktdata),
        );
        while rx.try_recv().is_ok() {}

        let map = crate::model::lock(&instruments);
        assert_eq!(
            map.len(),
            1,
            "exactly one entry for this symbol must survive the change, not a stale survivor \
             alongside the current one: {:?}",
            map.keys().collect::<Vec<_>>()
        );
        assert!(
            !map.contains_key(&(
                venue_arc("HYPERLIQUID"),
                category_arc("testcategory"),
                0,
                41
            )),
            "the stale entry under the OLD source must be purged, not merely superseded"
        );
        assert!(
            map.contains_key(&(venue_arc("PHOENIX"), category_arc("testcategory"), 0, 41)),
            "the entry under the CURRENT source must remain"
        );
    }

    /// A v3 definition burst that changes an already-revealed instrument's Source ID must emit
    /// exactly ONE `Instrument`, under the new source — not the stale re-announce under the OLD id
    /// (the existing "already revealed" block) followed by the eager reveal's own announcement
    /// under the new one. Correctness here must not rest on the arbiter incidentally deduping a
    /// redundant broadcast — the re-announce block is skipped outright when this definition's own
    /// `source_id` is about to supersede it.
    ///
    /// The second burst's `price_exponent` deliberately differs from the first: the arbiter rate
    /// limits an `Instrument` only when its `(venue, channel, instrument_id)` key AND precision pair
    /// are unchanged within `INSTRUMENT_REANNOUNCE_NS` — an unchanged-content reannounce under the
    /// OLD id would collide with THIS SAME test's own first message and be suppressed there,
    /// passing the assertion below for the wrong reason. Varying the exponent guarantees the
    /// arbiter would forward a stale reannounce if the processor ever emitted one, so this test
    /// actually exercises the processor's own decision, not the arbiter's.
    #[test]
    fn tob_v3_definition_id_change_emits_exactly_one_instrument() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = tob_ctx(&arbiter, &instruments, PortRole::Combined, TEST_PUB);

        proc.on_datagram(
            &tob_datagram_v3(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def_v3(41, 1, "INST-41", 1),
                ],
            ),
            &ctx,
        );
        while rx.try_recv().is_ok() {}

        proc.on_datagram(
            &tob_datagram_v3(
                1,
                &[enc_instrument_def_v3_with_exponents(
                    41, 2, "INST-41", 1, -2, 0,
                )],
            ),
            &ctx,
        );

        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.source_id, i.venue.to_string()));
            }
        }
        assert_eq!(
            seen,
            vec![(2, "PHOENIX".to_string())],
            "exactly one Instrument, under the new source — no redundant re-announce under the old one"
        );
    }

    /// v3's `InstrumentDefinition` carries its own Source ID, so this is named the moment its
    /// definition lands — no `Quote`/`Trade` needed at all. New behaviour: under v1 this instrument
    /// would still be unnamed here.
    #[test]
    fn tob_v3_definition_reveals_eagerly_with_no_price() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = tob_ctx(&arbiter, &instruments, PortRole::Combined, TEST_PUB);

        proc.on_datagram(
            &tob_datagram_v3(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def_v3(41, 1, "INST-41", 1),
                ],
            ),
            &ctx,
        );

        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            match &*m {
                FeedMessage::Instrument(i) => {
                    seen.push((i.source_id, i.venue.to_string(), i.instrument_id))
                }
                other => panic!("no price message was sent; unexpected {other:?}"),
            }
        }
        assert_eq!(
            seen,
            vec![(1, "HYPERLIQUID".to_string(), 41)],
            "the definition names itself, with no price at all"
        );
    }

    /// v1 (`source_id: None`) is unaffected by the v3 change: nothing is emitted for the instrument
    /// until a price reveals it, exactly as before. This is the invariant that must not regress.
    #[test]
    fn tob_v1_definition_still_defers_until_a_price_reveals_it() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = tob_ctx(&arbiter, &instruments, PortRole::Combined, TEST_PUB);

        proc.on_datagram(
            &tob_datagram(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(41, "INST-41", 1),
                ],
            ),
            &ctx,
        );
        assert!(
            rx.try_recv().is_err(),
            "a v1 definition carries no Source ID; nothing is emitted at definition time"
        );

        proc.on_datagram(
            &tob_datagram(1, &[enc_tob_quote(41, 1, 1_000)]),
            &tob_ctx(&arbiter, &instruments, PortRole::Mktdata, TEST_PUB),
        );
        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.source_id, i.instrument_id));
            }
        }
        assert_eq!(seen, vec![(1, 41)], "the price reveals it, as before");
    }

    /// `revealed`/`pending_channel` are keyed `(source_ip, instrument_id)` — the same spoofable axis
    /// `MAX_PUBLISHERS` bounds `state`/`seq` on — but had no eviction path of their own. A forged-
    /// source flood that only ever sends refdata (never a quote) would otherwise grow `pending_channel`
    /// without limit. `PerPublisher::take_evicted` (drained on every `get()`-driven eviction) is what
    /// closes that: the oldest publisher's entries in both maps must disappear in the same pass its
    /// `state`/`seq` entry does.
    #[test]
    fn tob_publisher_eviction_drops_revealed_and_pending_channel() {
        use super::MAX_PUBLISHERS;

        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));

        // The first publisher gets a full reveal (both maps populated for it); every later one only
        // sends refdata, so `pending_channel` is the map that would otherwise leak unboundedly.
        proc.on_datagram(
            &tob_datagram(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(41, "INST-41", 1),
                ],
            ),
            &tob_ctx(&arbiter, &instruments, PortRole::Combined, ip(0)),
        );
        proc.on_datagram(
            &tob_datagram(1, &[enc_tob_quote(41, 1, 1_000)]),
            &tob_ctx(&arbiter, &instruments, PortRole::Mktdata, ip(0)),
        );
        let _ = rx.try_recv();
        assert!(proc.revealed.contains_key(&(ip(0), 41)));
        assert!(proc.pending_channel.contains_key(&(ip(0), 41)));

        let flood = (MAX_PUBLISHERS as u32) + 50;
        for i in 1..flood {
            proc.on_datagram(
                &tob_datagram(
                    0,
                    &[
                        enc_manifest_summary(1, 1),
                        enc_instrument_def(41, "INST-41", 1),
                    ],
                ),
                &tob_ctx(&arbiter, &instruments, PortRole::Combined, ip(i)),
            );
        }

        assert!(
            proc.revealed.len() <= MAX_PUBLISHERS,
            "revealed must stay bounded, got {}",
            proc.revealed.len()
        );
        assert!(
            proc.pending_channel.len() <= MAX_PUBLISHERS,
            "pending_channel must stay bounded, got {}",
            proc.pending_channel.len()
        );
        assert!(
            !proc.revealed.contains_key(&(ip(0), 41)),
            "the evicted publisher's reveal must not outlive its refdata state"
        );
        assert!(
            !proc.pending_channel.contains_key(&(ip(0), 41)),
            "the evicted publisher's pending channel must not outlive its refdata state"
        );
    }

    /// A `ChannelReset` discards the whole reference-data set for the publisher that sent it — the
    /// same remap risk MBO/MBP's `InstrumentReset` already guards (a manifest era bump can reassign
    /// an `instrument_id` to a different market), applying with more force here since the entire
    /// definition set goes at once. `revealed`/`pending_channel` must go with it, or a later reveal
    /// under the new era could replay the old Source ID against a different instrument.
    #[test]
    fn tob_channel_reset_drops_revealed_and_pending_channel() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = TobProcessor::new(tape(false));
        let ctx = |role| tob_ctx(&arbiter, &instruments, role, TEST_PUB);

        proc.on_datagram(
            &tob_datagram(
                0,
                &[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(41, "INST-41", 1),
                ],
            ),
            &ctx(PortRole::Combined),
        );
        proc.on_datagram(
            &tob_datagram(1, &[enc_tob_quote(41, 1, 1_000)]),
            &ctx(PortRole::Mktdata),
        );
        let _ = rx.try_recv();
        assert!(proc.revealed.contains_key(&(TEST_PUB, 41)));
        assert!(proc.pending_channel.contains_key(&(TEST_PUB, 41)));

        proc.on_datagram(
            &tob_datagram(2, &[enc_tob_channel_reset(9_999)]),
            &ctx(PortRole::Combined),
        );

        assert!(
            !proc.revealed.contains_key(&(TEST_PUB, 41)),
            "a channel reset must drop the publisher's reveal along with its definitions"
        );
        assert!(
            !proc.pending_channel.contains_key(&(TEST_PUB, 41)),
            "a channel reset must drop the publisher's pending channel along with its definitions"
        );
    }

    /// Minimal MBO `Trade` body encoder (52-byte message, same layout as TOB's `Trade` — see
    /// `codec::tests::encode_trade_datagram`) — a print with no book effect at all, for the tests
    /// below that need a reveal/id-change with zero chance of moving the book's own top-N.
    fn enc_mbo_trade(instrument_id: u32, source_id: u16) -> Vec<u8> {
        let mut b = vec![
            crate::ingest::codec_mbo::MSG_TRADE,
            crate::ingest::codec_mbo::sizes::TRADE,
            0,
            0,
        ];
        b.extend_from_slice(&instrument_id.to_le_bytes());
        b.extend_from_slice(&source_id.to_le_bytes());
        b.push(0); // aggressor_side: unknown
        b.push(0); // trade_flags
        b.extend_from_slice(&9_000u64.to_le_bytes()); // source_ts
        b.extend_from_slice(&1i64.to_le_bytes()); // trade_price_raw
        b.extend_from_slice(&1u64.to_le_bytes()); // trade_qty_raw
        b.extend_from_slice(&0u64.to_le_bytes()); // trade_id
        b.extend_from_slice(&0u64.to_le_bytes()); // cumulative_volume_raw
        b
    }

    /// Round-3 review, finding A: `ManifestSummary`/`InstrumentDefinition` must be gated on
    /// `handle_refdata` exactly like TOB/Midpoint's sibling paths. Before that gate, decode doesn't
    /// care what physical port a message type arrives on, so a forged datagram to the **Mktdata**
    /// port carrying those two message types still called `state.get()` for a never-before-seen
    /// publisher — evicting an old one on the same axis `pending_channel` shares — while the drain
    /// that keeps `pending_channel` bounded only runs behind the `handles_refdata()` gate at the
    /// top of `on_datagram`, which a Mktdata-role datagram never satisfies. Reproduces the review's
    /// exact scenario: one refdata burst per forged IP, sent to `PortRole::Mktdata`.
    #[test]
    fn mbo_manifest_burst_via_mktdata_port_does_not_leak_pending_channel() {
        use super::MAX_PUBLISHERS;

        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));

        let burst = datagram(&[
            enc_manifest_summary(1, 1),
            enc_instrument_def(0, "INST-0", 1),
        ]);
        let flood = (MAX_PUBLISHERS as u32) + 50;
        for i in 0..flood {
            let mut ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
            ctx.publisher = ip(i);
            proc.on_datagram(&burst, &ctx);
        }

        assert!(
            proc.state.states.len() <= MAX_PUBLISHERS,
            "refdata state map must stay bounded, got {}",
            proc.state.states.len()
        );
        assert!(
            proc.pending_channel.len() <= MAX_PUBLISHERS,
            "pending_channel must stay bounded even when refdata-shaped messages arrive on the \
             mktdata port — a role that must never mint refdata state in the first place, got {}",
            proc.pending_channel.len()
        );
        assert!(
            proc.pending_channel.is_empty(),
            "a role that doesn't handle refdata must process NO refdata message at all, not just \
             stay under the cap; got {} entries",
            proc.pending_channel.len()
        );
    }

    /// [`datagram`], but stamped with Schema Version 3 — for the tests exercising a v3
    /// `InstrumentDefinition`'s own Source ID.
    fn mbo_datagram_v3(messages: &[Vec<u8>]) -> Vec<u8> {
        let mut f = datagram(messages);
        f[2] = 3;
        f
    }

    /// v3's `InstrumentDefinition` carries its own Source ID, so this is named the moment its
    /// definition lands — no order-book delta needed at all. New behaviour: under v1 this
    /// instrument would still be unnamed here. No book exists yet for a freshly-defined instrument,
    /// so the eager reveal must not force a `depth` re-baseline either — none accompanies it.
    #[test]
    fn mbo_v3_definition_reveals_eagerly_with_no_depth() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let combined = make_ctx(&arbiter, &instruments, PortRole::Combined);

        proc.on_datagram(
            &mbo_datagram_v3(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def_v3(0, 1, "INST-0", 1),
            ]),
            &combined,
        );

        let seen = drain_all(&mut rx);
        let insts: Vec<_> = seen
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Instrument(i) => Some((i.source_id, i.instrument_id)),
                _ => None,
            })
            .collect();
        assert_eq!(
            insts,
            vec![(1, 0)],
            "the definition names itself, with no delta at all"
        );
        assert!(
            !seen.iter().any(|m| matches!(m, FeedMessage::Depth(_))),
            "no book exists yet for a freshly-defined instrument; the eager reveal must not force \
             a depth re-baseline"
        );
    }

    /// v1 (`source_id: None`) is unaffected by the v3 change: nothing is emitted for the instrument
    /// until a delta-carrying message reveals it, exactly as before. This is the invariant that
    /// must not regress.
    #[test]
    fn mbo_v1_definition_still_defers_until_a_delta_reveals_it() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let combined = make_ctx(&arbiter, &instruments, PortRole::Combined);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &combined,
        );
        assert!(
            rx.try_recv().is_err(),
            "a v1 definition carries no Source ID; nothing is emitted at definition time"
        );

        proc.on_datagram(&datagram(&[add(1, 100, 5_000)]), &mkt);
        let seen = drain_all(&mut rx);
        assert!(
            seen.iter().any(|m| matches!(m, FeedMessage::Instrument(_))),
            "the first delta reveals it, as before"
        );
    }

    /// Round-3 review, finding B: a Source ID change revealed by a message that doesn't move the
    /// book's own top-N (a standalone `Trade`, which never touches the book at all) must not have
    /// its forced re-baseline silently swallowed by `emit_depth`'s duplicate-suppression memo
    /// (`last_top`, keyed by CONTENT, not by identity). `last_top` is now cleared on the same
    /// id-change branch `reveal_if_needed` already takes the metric/re-announce action on —
    /// mirroring what `InstrumentReset` already does for the identical reason.
    #[test]
    fn mbo_id_change_forces_a_depth_rebaseline_even_when_top_n_is_unchanged() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let combined = make_ctx(&arbiter, &instruments, PortRole::Combined);
        let snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &combined,
        );
        proc.on_datagram(
            &datagram(&[
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
            &snap,
        );
        // Reveal + establish the top-N under Source ID 0 (`add`'s fixed id — synthesizes
        // "SOURCE_0", since 0 has no registry row): one resting bid.
        proc.on_datagram(&datagram(&[add(1, 100, 5_000)]), &mkt);
        let first = drain_all(&mut rx);
        let venues: Vec<String> = first
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Instrument(i) => Some(i.venue.to_string()),
                _ => None,
            })
            .collect();
        let depth_ts: Vec<u64> = first
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Depth(d) => Some(d.source_ts_ns),
                _ => None,
            })
            .collect();
        assert_eq!(venues, vec!["SOURCE_0".to_string()]);
        assert_eq!(
            depth_ts,
            vec![5_000],
            "first reveal shows the resting order"
        );

        // A standalone `Trade` for the SAME instrument under a DIFFERENT Source ID (2, Phoenix).
        // It never touches the book — the published top-N is byte-for-byte identical to what
        // `emit_depth` last recorded — which is exactly the case `last_top` would otherwise
        // suppress, leaving the new venue with an `instrument` and no `depth` at all.
        proc.on_datagram(&datagram(&[enc_mbo_trade(0, 2)]), &mkt);
        let second = drain_all(&mut rx);
        let venues: Vec<String> = second
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Instrument(i) => Some(i.venue.to_string()),
                _ => None,
            })
            .collect();
        let depth_ts: Vec<u64> = second
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Depth(d) => Some(d.source_ts_ns),
                _ => None,
            })
            .collect();
        assert_eq!(
            venues,
            vec!["PHOENIX".to_string()],
            "the id change is re-announced (finding 2 — already covered elsewhere, checked here \
             only to confirm the id change is really what's driving this scenario)"
        );
        assert_eq!(
            depth_ts,
            vec![5_000],
            "the id change must still force a depth re-baseline under the new venue even though \
             the top-N content didn't move"
        );
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
        let mut proc = MboProcessor::new(depth, tape(false));

        // Refdata: a Valid=0 manifest (the live publisher's quirk) + the BTC definition under it.
        let mut manifest = enc_manifest_summary(5, 1);
        manifest[5] = 0; // body+1 is the `valid` byte; force the live-feed Valid=0 case
        proc.on_datagram(
            &datagram(&[manifest, enc_instrument_def(0, "INST-0", 5)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        // Empty-book anchor (anchor_seq=0, last_instrument_seq=0), then a contiguous delta (seq 1).
        proc.on_datagram(
            &datagram(&[
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
            &datagram(&[enc_order_add(&OrderAdd {
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
        let mut proc = MboProcessor::new(depth, tape(false));
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &make_ctx(arbiter, instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &datagram(&[
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

        proc.on_datagram(&datagram(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
        proc.on_datagram(&datagram(&[add(2, 101, 100)]), &mkt); // stale-clock tick -> dropped (the wedge)
        proc.on_datagram(&datagram(&[enc_end_of_session(6000)]), &mkt); // floor cleared, book -> Recovering
                                                                        // New session: re-snapshot (empty anchor; the fresh book's depth(0) re-opens the cleared
                                                                        // floor), then a restarted-seq, restarted-clock delta.
        proc.on_datagram(
            &datagram(&[
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
        proc.on_datagram(&datagram(&[add(1, 102, 50)]), &mkt); // new-session tick below the old high-water

        // No leading `0`: `synced_mbo_proc`'s own empty-anchor setup carries no Source ID (the
        // snapshot machinery never does), so it stays deferred and emits nothing — the first
        // `add` both reveals the instrument AND is the first emission, showing its own post-apply
        // state (5000) directly rather than an empty anchor first. Revealed state survives
        // `EndOfSession` (it is an identity fact, not book-sync state), so the SECOND empty-anchor
        // re-sync (already revealed) does emit its `depth(0)`, re-opening the floor as before.
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![5000, 0, 50],
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
        ) -> DatagramCtx<'a> {
            let mut c = make_ctx(arbiter, instruments, role);
            c.publisher = publisher;
            c
        }
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        // Reference-data state is per publisher, so each path publishes its own manifest burst -
        // which is what they do on the wire, sharing one refdata port.
        for publisher in [pub_a, pub_b] {
            proc.on_datagram(
                &datagram(&[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(0, "INST-0", 1),
                ]),
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Combined),
            );
        }
        let anchor = |sid: u32| {
            datagram(&[
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
            &datagram(&[add(1, 100, 5000)]),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Mktdata),
        );
        proc.on_datagram(
            &datagram(&[add(1, 100, 5000)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );
        // A's EndOfSession resets BOTH books and clears the floor.
        proc.on_datagram(
            &datagram(&[enc_end_of_session(6000)]),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Mktdata),
        );
        // B's old-session tail (would be depth(5001), re-latching the old high-water) is buffered
        // by B's now-Recovering book instead: nothing emits, the floor stays open.
        proc.on_datagram(
            &datagram(&[add(2, 101, 5001)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );
        // B re-syncs in the new session and its restarted-clock depth is admitted.
        proc.on_datagram(
            &anchor(2),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &datagram(&[add(1, 102, 50)]),
            &ctx_for(pub_b, &arbiter, &instruments, PortRole::Mktdata),
        );

        // No leading `0`: both publishers' initial empty-anchor sync carries no Source ID, so
        // neither emits until its own `add` reveals it (A's add(1,100,5000) is the first
        // emission; B's identical reveal-forced depth(5000) collapses against A's at the same
        // floor tick, exactly as before deferral).
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![5000, 0, 50],
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

        proc.on_datagram(&datagram(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
                                                                 // Reset for an id with no definition, no emitted depth, no book -> venue-wide clear.
        proc.on_datagram(
            &datagram(&[enc_instrument_reset(&InstrumentReset {
                instrument_id: 99,
                reason: 1,
                new_anchor_seq: 0,
                ts: 5500,
            })]),
            &mkt,
        );
        // Instrument 0's still-synced book emits at the restarted (lower) clock: admitted only if
        // the venue-wide fallback cleared the floor.
        proc.on_datagram(&datagram(&[add(2, 101, 100)]), &mkt);

        // No leading `0`: the setup's empty-anchor sync carries no Source ID, so it stays
        // deferred and emits nothing until `add(1,100,5000)` both reveals instrument 0 and shows
        // its own post-apply state directly.
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![5000, 100],
            "an unresolvable InstrumentReset id must still clear the floor (venue-wide fallback)"
        );
    }

    /// If a manifest era bump remaps the instrument id to a DIFFERENT symbol between the last
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

        proc.on_datagram(&datagram(&[add(1, 100, 5000)]), &mkt); // floor latched under INST-0
                                                                 // Manifest bump remaps id 0 to another symbol; the reset must clear the LATCHED
                                                                 // symbol (INST-0), not the current definition's (INST-9).
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(2, 1),
                enc_instrument_def(0, "INST-9", 2),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &datagram(&[enc_instrument_reset(&InstrumentReset {
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
            &datagram(&[
                enc_manifest_summary(3, 1),
                enc_instrument_def(0, "INST-0", 3),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &datagram(&[
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
        proc.on_datagram(&datagram(&[add(1, 101, 100)]), &mkt);

        // No leading/middle `0`s: the setup's empty-anchor sync stays deferred until
        // `add(1,100,5000)` reveals it (showing 5000 directly), and `InstrumentReset` clears the
        // reveal too (the same remap risk as the floor entry it clears — the old Source ID must
        // not survive to misdescribe a possibly-different post-remap market), so the re-sync
        // empty anchor ALSO stays deferred; `add(1,101,100)` re-reveals and shows its own
        // post-apply state (100) directly. The floor-clear itself is still proven: 100 is well
        // below the old 5000 high-water and is admitted, not stale-dropped.
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![5000, 100],
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

        proc.on_datagram(&datagram(&[add(1, 100, 5000)]), &mkt); // depth(5000) latches the floor
        proc.on_datagram(
            &datagram(&[enc_instrument_reset(&InstrumentReset {
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
            &datagram(&[
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
        proc.on_datagram(&datagram(&[add(1, 101, 100)]), &mkt); // new (restarted) clock -> admitted

        // No leading/middle `0`s — same deferral reasoning as
        // `mbo_instrument_reset_after_id_remap_clears_the_latched_symbol`: the setup anchor and
        // the post-reset re-sync anchor both carry no Source ID and stay deferred, so `100` (the
        // restarted clock, well below the old 5000 high-water) is what proves the floor reopened.
        assert_eq!(
            drain_depth_ts(&mut rx),
            vec![5000, 100],
            "post-reset depths must flow at the restarted clock, not be stale-dropped"
        );
    }

    /// `depth` messages for a datagram touching multiple instruments must arrive in ascending
    /// instrument_id order regardless of the wire order of their `OrderAdd`s. The invariant is
    /// guaranteed by draining a `BTreeSet<u32>` rather than a `HashSet`.
    #[test]
    fn mbo_depth_emit_order_is_ascending_instrument_id() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));

        // Refdata: manifest declares 2 instruments; then their definitions.
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 2),
                enc_instrument_def(0, "INST-0", 1),
                enc_instrument_def(1, "INST-1", 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        // Sync each instrument via an empty-book anchor snapshot (0 orders, anchor_seq=0).
        let snap = |iid: u32, sid: u32| {
            datagram(&[
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

        // Mktdata datagram: instrument 1 appears before instrument 0 in the wire order. BTreeSet must
        // still drain 0 → 1.
        let mktdata_datagram = datagram(&[
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
            &mktdata_datagram,
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

        // Replay with incremented per_instrument_seqs to confirm the order is stable across datagrams,
        // not a lucky hash ordering on the first run.
        let mktdata_datagram2 = datagram(&[
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
            &mktdata_datagram2,
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx),
            vec![0, 1],
            "order must be stable across datagrams"
        );
    }

    /// `upsert_instrument` is idempotent for matching exponents and last-writer-wins for
    /// conflicting ones (exercising the warn path; the warn itself is not asserted).
    #[test]
    fn upsert_instrument_idempotent_and_last_writer_wins() {
        let instruments: crate::model::InstrumentSnapshot = Arc::new(Mutex::new(HashMap::new()));

        let base = NormalizedInstrument {
            venue: "TestVenue".into(),
            source: "TestVenue".into(),
            source_id: 0,
            symbol: "BTC".into(),
            channel: 0,
            instrument_id: 1,
            category: "default".into(),
            price_exponent: -2,
            qty_exponent: -4,
        };

        // First insert.
        upsert_instrument(&instruments, &base);
        {
            let map = instruments.lock().unwrap();
            assert_eq!(map.len(), 1);
            let entry = map
                .get(&("TestVenue".into(), "default".into(), 0u8, 1u32))
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
                .get(&("TestVenue".into(), "default".into(), 0u8, 1u32))
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
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));

        // No manifest/definition: an OrderAdd for an unknown instrument must be dropped, not booked.
        let f = datagram(&[enc_order_add(&OrderAdd {
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
    /// feed grows them without limit. Each instrument is driven all the way to `Synced` with an
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
        let mut proc = MboProcessor::new(depth, tape(false));

        let flood = (MAX_BOOKS as u32) + 50;
        // Declare and define every instrument so the definition gate admits each one.
        proc.on_datagram(
            &datagram(&[enc_manifest_summary(1, flood)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        for i in 0..flood {
            proc.on_datagram(
                &datagram(&[enc_instrument_def(i, &format!("INST-{i}"), 1)]),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
        }
        // For each instrument: an empty-anchor snapshot syncs the book, then one OrderAdd gives it a
        // resting level, so emit_depth fires and records a `last_top` entry. book_for must evict the
        // oldest from BOTH maps as the flood grows past MAX_BOOKS.
        for i in 0..flood {
            proc.on_datagram(
                &datagram(&[
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
        // Keyed on "SOURCE_0", not the harness's "TV" feed venue: every OrderAdd above stamps wire
        // Source ID 0 (unregistered), and the emitted depth's venue is the wire id's label, never
        // the feed row's venue.
        let depth_map = crate::model::lock(&proc.depth);
        assert!(
            depth_map.len() <= MAX_BOOKS,
            "depth replay map must stay bounded in lockstep with books, got {}",
            depth_map.len()
        );
        assert!(
            depth_map.contains_key(&("SOURCE_0".into(), format!("INST-{}", flood - 1).into())),
            "newest instrument's depth replay entry retained"
        );
        assert!(
            !depth_map.contains_key(&("SOURCE_0".into(), "INST-0".into())),
            "oldest instrument's depth replay entry evicted too"
        );
    }

    /// Round-3 review, finding 4: `book_for`'s eviction must NOT remove `pending_channel` — it
    /// mirrors `RefDataState.defs`'s lifecycle (populated straight from refdata), not `books`'s, so
    /// removing it on a `books` eviction would zero the channel (`unwrap_or(0)`) for a later reveal
    /// even though the definition, and the channel it arrived on, are both still known. Proven
    /// behaviorally: instrument 0 is defined on a NON-zero channel, evicted from `books`/`revealed`
    /// by a flood of other instruments on the same publisher, then re-revealed — the re-announced
    /// `Instrument`'s `channel` must still be the original one, not the `unwrap_or(0)` default a
    /// wrongly-evicted `pending_channel` would fall back to.
    #[test]
    fn mbo_book_eviction_preserves_pending_channel_for_a_later_reveal() {
        use super::MAX_BOOKS;

        /// Rewrite a datagram's channel byte (datagram-header byte 3) — `codec_mbo::tests::datagram` always
        /// writes `0`, so this is the only way to get a non-zero channel onto the wire from here.
        fn with_channel(mut f: Vec<u8>, ch: u8) -> Vec<u8> {
            f[3] = ch;
            f
        }

        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let mut proc = MboProcessor::new(depth, tape(false));

        // Instrument 0's definition arrives on channel 7 (non-zero, so a wrongly-zeroed
        // `pending_channel` entry is observable).
        proc.on_datagram(
            &with_channel(
                datagram(&[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(0, "INST-0", 1),
                ]),
                7,
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        // Sync and reveal instrument 0's book (channel doesn't matter here; only the definition's
        // channel, above, feeds `pending_channel`).
        proc.on_datagram(
            &datagram(&[
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
                add(1, 1, 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        let _ = drain_all(&mut rx);
        assert_eq!(proc.pending_channel.get(&(TEST_PUB, 0)), Some(&7));

        // Flood MAX_BOOKS other DEFINED instruments (same publisher) through the full
        // define+sync+reveal cycle, evicting instrument 0's `books`/`revealed`/`last_top` entries
        // (oldest-first) while leaving its `pending_channel` entry alone (finding 4's fix).
        let flood = (MAX_BOOKS as u32) + 5;
        proc.on_datagram(
            &datagram(&[enc_manifest_summary(1, flood + 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        for i in 1..=flood {
            proc.on_datagram(
                &datagram(&[enc_instrument_def(i, &format!("INST-{i}"), 1)]),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
            proc.on_datagram(
                &datagram(&[
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
        let _ = drain_all(&mut rx);

        assert!(
            !proc.books.contains_key(&(TEST_PUB, 0)),
            "instrument 0's book must have been evicted by the flood"
        );
        assert!(
            !proc.revealed.contains_key(&(TEST_PUB, 0)),
            "instrument 0's reveal is evicted in lockstep with its book"
        );
        assert_eq!(
            proc.pending_channel.get(&(TEST_PUB, 0)),
            Some(&7),
            "pending_channel must survive a books eviction — it mirrors RefDataState.defs's \
             lifecycle, not books's"
        );

        // Re-reveal instrument 0 (a fresh book, fresh reveal — the definition never left). Read the
        // re-announced definition from the shared `InstrumentSnapshot` (written unconditionally by
        // `reveal_if_needed`'s `upsert_instrument`, ahead of the arbiter's own re-announce
        // throttling on the wire — irrelevant to what's being proven here) rather than off the
        // broadcast channel: its channel must be the ORIGINAL 7, not the `unwrap_or(0)` fallback a
        // wrongly-evicted `pending_channel` would produce.
        proc.on_datagram(
            &datagram(&[add(1, 999, 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        let snapshot = crate::model::lock(&instruments);
        let reannounced = snapshot
            .get(&(
                std::sync::Arc::<str>::from("SOURCE_0"),
                category_arc("testcategory"),
                7,
                0,
            ))
            .expect("instrument 0 must have been re-revealed under SOURCE_0/INST-0");
        assert_eq!(
            reannounced.channel, 7,
            "the re-reveal must still carry the original channel, not the pending_channel \
             unwrap_or(0) fallback"
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
        let mut proc = MboProcessor::new(depth, tape(false));

        // Define instrument 0 and sync it with an empty-anchor snapshot.
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &datagram(&[
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

        // Datagram 1: add DEPTH_LEVELS+1 bids at distinct ascending prices. The lowest price is the
        // (N+1)th level — outside the published top-N. One coalesced depth for the datagram.
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
            &datagram(&establish),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx).len(),
            1,
            "datagram 1 establishes the book: exactly one depth"
        );

        // Datagram 2: churn the worst (lowest) bid price 100 — outside the top-N. Book changes, but the
        // top-N is byte-identical, so depth must be suppressed.
        proc.on_datagram(
            &datagram(&[bid(levels + 1, 100)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert_eq!(
            drain_depth_ids(&mut rx).len(),
            0,
            "deep-book churn outside the top-N must be suppressed"
        );

        // Datagram 3: add a new best bid above every existing level — moves the top-N, must emit.
        proc.on_datagram(
            &datagram(&[bid(levels + 2, 100 + levels as i64)]),
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
                source_id: None,
                symbol: format!("INST-{id}").into(),
                price_exponent: 0,
                qty_exponent: 0,
                manifest_seq: 1,
            })
        }));
        out
    }

    /// [`mbp_wire::datagram`], but stamped with Schema Version 3 — for the tests exercising a v3
    /// `InstrumentDefinition`'s own Source ID.
    fn mbp_datagram_v3(
        channel_id: u8,
        reset_count: u8,
        sequence: u64,
        messages: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut f = mbp_wire::datagram(channel_id, reset_count, sequence, messages);
        f[2] = 3;
        f
    }

    /// v3's `InstrumentDefinition` carries its own Source ID, so this book key is named the moment
    /// its definition lands — no `LevelUpdate`/`BookClear`/`Trade` needed at all. New behaviour:
    /// under v1 this instrument would still be unnamed here. No book exists yet for a
    /// freshly-defined instrument, so the eager reveal must not force a `book` re-baseline either —
    /// none accompanies it.
    #[test]
    fn mbp_v3_definition_reveals_eagerly_with_no_book() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));

        proc.on_datagram(
            &mbp_datagram_v3(
                0,
                0,
                0,
                &[
                    mbp_wire::enc_manifest_summary(&codec_mbp::ManifestSummary {
                        channel_id: 0,
                        valid: true,
                        manifest_seq: 1,
                        instrument_count: 1,
                        ts: 0,
                    }),
                    enc_instrument_def_v3(41, 1, "INST-41", 1),
                ],
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        let seen = drain_all(&mut rx);
        let insts: Vec<_> = seen
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Instrument(i) => Some((i.source_id, i.instrument_id)),
                _ => None,
            })
            .collect();
        assert_eq!(
            insts,
            vec![(1, 41)],
            "the definition names itself, with no book content at all"
        );
        assert!(
            !seen.iter().any(|m| matches!(m, FeedMessage::Book(_))),
            "no book exists yet for a freshly-defined instrument; the eager reveal must not force \
             a book re-baseline"
        );
    }

    /// v1 (`source_id: None`) is unaffected by the v3 change: nothing is emitted for the book key
    /// until a print reveals it, exactly as before. This is the invariant that must not regress.
    #[test]
    fn mbp_v1_definition_still_defers_until_a_print_reveals_it() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));

        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 0, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        assert!(
            rx.try_recv().is_err(),
            "a v1 definition carries no Source ID; nothing is emitted at definition time"
        );

        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &[mbp_reveal(41, 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let seen = drain_all(&mut rx);
        assert!(
            seen.iter().any(|m| matches!(m, FeedMessage::Instrument(_))),
            "the first print reveals it, as before"
        );
    }

    /// One level update: `qty` is the level's absolute resulting quantity, `0` removing it.
    fn mbp_level(id: u32, seq: u32, side: u8, price: i64, qty: u64, ts: u64) -> Vec<u8> {
        mbp_level_with_source(id, seq, side, price, qty, ts, 0)
    }

    /// Same as `mbp_level`, but with an explicit wire Source ID — for the tests that assert on it
    /// passing through to the emitted `book`. `mbp_level` fixes it at `0` for every other test.
    fn mbp_level_with_source(
        id: u32,
        seq: u32,
        side: u8,
        price: i64,
        qty: u64,
        ts: u64,
        source_id: u16,
    ) -> Vec<u8> {
        mbp_wire::enc_level_update(&codec_mbp::LevelUpdate {
            instrument_id: id,
            source_id,
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

    /// A minimal `Trade` that reveals a book's Source ID without touching its content — for tests
    /// that need a book/instrument revealed (deferral otherwise holds `send_book`/
    /// `emit_rebaseline` back indefinitely) while asserting on a snapshot's own shape, untouched by
    /// any delta. `tape(false)` (used throughout this module's MBP tests) suppresses the `Trade`
    /// itself from reaching the channel, so it adds no noise to `drain_books`/`drain_all`.
    fn mbp_reveal(id: u32, source_id: u16) -> Vec<u8> {
        mbp_wire::enc_trade(&codec_mbp::Trade {
            instrument_id: id,
            source_id,
            aggressor_side: 0,
            trade_flags: 0,
            source_ts: 0,
            trade_price_raw: 1,
            trade_qty_raw: 1,
            trade_id: 0,
            cumulative_volume_raw: 0,
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

    /// A `DatagramCtx` on a venue unique to one test. The Prometheus registry is process-global, so a
    /// metric assertion driven through `make_ctx`'s shared `"TV"` venue would count every other
    /// test's increments too.
    fn mbp_ctx<'a>(
        venue: &'static str,
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
    ) -> DatagramCtx<'a> {
        let mut c = make_ctx(arbiter, instruments, role);
        c.venue = venue;
        c
    }

    /// [`mbp_ctx`], with a mirror offset — for the tests exercising a second publisher that mirrors
    /// the same channels raised by `offset` (`Feed::mirror_offset`/`registry.json`'s
    /// `publisher_offset`).
    fn mbp_ctx_mirrored<'a>(
        venue: &'static str,
        arbiter: &'a SharedArbiter,
        instruments: &'a crate::model::InstrumentSnapshot,
        role: PortRole,
        offset: u8,
    ) -> DatagramCtx<'a> {
        let mut c = mbp_ctx(venue, arbiter, instruments, role);
        c.mirror_offset = Some(offset);
        c
    }

    /// **The mirror finding.** A second publisher mirrors this channel's whole roster on the SAME
    /// socket, stamping every wire `channel_id` raised by `publisher_offset` — so one receiver
    /// decodes datagrams stamped both channel 10 and channel 110 for the identical market. The
    /// datagram source IP is deliberately the SAME for both datagrams here (the registry's own
    /// `DEPLOYMENT` note keys the two paths apart by `channel_id`, never by host), which is what
    /// makes this a real regression guard: if `ensure_book`/`note_reset_count`/`revealed` ever
    /// canonicalized their channel instead of using the raw wire one, the two paths below would
    /// collapse into ONE producer-side key (same IP, same canonical channel) and corrupt book
    /// recovery — the exact failure `Feed::mirror_offset`'s docs warn against.
    ///
    /// Catalog identity must go the other way: `reveal_if_needed` canonicalizes, so both paths'
    /// eager v3 reveals resolve to ONE catalog entry, not two.
    #[test]
    fn mbp_mirror_offset_collapses_catalog_identity_but_keeps_paths_separate() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let base = mbp_ctx_mirrored("TV", &arbiter, &instruments, PortRole::Combined, 100);
        let mirror = mbp_ctx_mirrored("TV", &arbiter, &instruments, PortRole::Combined, 100);

        let def_datagram = |channel_id: u8| {
            mbp_datagram_v3(
                channel_id,
                0,
                0,
                &[
                    mbp_wire::enc_manifest_summary(&codec_mbp::ManifestSummary {
                        channel_id,
                        valid: true,
                        manifest_seq: 1,
                        instrument_count: 1,
                        ts: 0,
                    }),
                    enc_instrument_def_v3(41, 3, "MARKET-X", 1),
                ],
            )
        };
        // The base path, wire channel 10.
        proc.on_datagram(&def_datagram(10), &base);
        // The mirror path, wire channel 110 (10 + the 100 offset) — same source IP, same
        // instrument, same Source ID: the same market as far as a consumer is concerned.
        proc.on_datagram(&def_datagram(110), &mirror);

        // Catalog: ONE market at the canonical channel, not two.
        {
            let cat = instruments.lock().unwrap();
            let keys: Vec<_> = cat.keys().cloned().collect();
            assert_eq!(
                cat.len(),
                1,
                "the mirror must not mint a second catalog entry: {keys:?}"
            );
            assert!(
                cat.contains_key(&("KALSHI".into(), "testcategory".into(), 10u8, 41u32)),
                "the single entry must live under the canonical channel: {keys:?}"
            );
        }

        // Only ONE `Instrument` reaches the wire: canonicalizing both paths onto channel 10 means
        // the mirror's reveal is, correctly, an identical-precision reannounce of the SAME
        // `(venue, channel, instrument_id)` the arbiter already rate-limits — the same collapse
        // ordinary mirrored refdata bursts get, now reached via the channel offset instead of a
        // repeated burst on one channel.
        let seen = drain_all(&mut rx);
        let inst_channels: Vec<u8> = seen
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Instrument(i) => Some(i.channel),
                _ => None,
            })
            .collect();
        assert_eq!(
            inst_channels,
            vec![10],
            "the mirror's reveal must ride the canonical channel and collapse into the base path's"
        );

        // Producer-side state stays keyed on the RAW wire channel: two distinct reveal entries,
        // one per path, even though they collapsed to one catalog entry above.
        assert_eq!(
            proc.revealed.len(),
            2,
            "each path's own reveal memo must survive independently"
        );

        // Sync each path's own book to `Ready` from an empty anchor (an empty snapshot cycle, same
        // shape `synced_mbp_proc` below drives) so a following delta actually applies rather than
        // merely buffering — the only way to observe divergent book *content*, not just presence.
        proc.on_datagram(
            &mbp_wire::datagram(10, 0, 1, &mbp_snapshot(41, 1, 0, 0, &[])),
            &base,
        );
        proc.on_datagram(
            &mbp_wire::datagram(110, 0, 1, &mbp_snapshot(41, 1, 0, 0, &[])),
            &mirror,
        );
        assert_eq!(mbp_status(&proc, TEST_PUB, 10, 41), Some(BookStatus::Ready));
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 110, 41),
            Some(BookStatus::Ready)
        );

        // Now apply a DIFFERENT price on each path's book — a stand-in for the two paths' genuinely
        // independent delta sequences.
        let level = |price: i64| {
            mbp_wire::enc_level_update(&codec_mbp::LevelUpdate {
                instrument_id: 41,
                source_id: 3,
                side: codec_mbp::SIDE_BID,
                action: 1,
                per_instrument_seq: 1,
                price_raw: price,
                qty_raw: 5,
                ts: 1,
                order_count: Some(1),
                level_index: None,
                update_reason: 0,
                level_flags: 0,
            })
        };
        proc.on_datagram(&mbp_wire::datagram(10, 0, 2, &[level(100)]), &base);
        proc.on_datagram(&mbp_wire::datagram(110, 0, 2, &[level(200)]), &mirror);

        assert_eq!(
            proc.books.len(),
            2,
            "one book per (publisher, wire channel, instrument) — the mirror must not share the \
             base path's book"
        );
        let base_book = proc
            .books
            .get(&(TEST_PUB, 10, 41))
            .expect("the base path's own book, keyed on its raw wire channel");
        let mirror_book = proc
            .books
            .get(&(TEST_PUB, 110, 41))
            .expect("the mirror path's own book, keyed on ITS raw wire channel");
        assert_eq!(
            base_book.bids().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![100]
        );
        assert_eq!(
            mirror_book.bids().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![200],
            "the two paths' books must evolve independently"
        );

        // Every `book` this datagram pair emitted still carries the canonical channel, proving the
        // collapse is consumer-facing only and not a side effect of the books above having stayed
        // apart.
        let book_channels: Vec<u8> = drain_all(&mut rx)
            .iter()
            .filter_map(|m| match m {
                FeedMessage::Book(b) => Some(b.channel),
                _ => None,
            })
            .collect();
        assert!(
            !book_channels.is_empty() && book_channels.iter().all(|&c| c == 10),
            "every emitted book must ride the canonical channel: {book_channels:?}"
        );
    }

    /// **The live regression, end to end.** The mirror offset here does not come from a
    /// hand-built `DatagramCtx` — it comes from parsing an `explicit` registry row (the shape the
    /// live mirrored feeds actually use: one shared port block, two paths separated only by
    /// `channel_id`), exactly the document shape that used to be hard-wired to `mirror_offset:
    /// None`. Two paths stamp the same market at channel 1 and channel 101 (the offsets seen on
    /// the live host); if the registry still dropped an `explicit` row's `publisher_offset`, this
    /// collapses to zero (`None`) and the catalog would carry two entries instead of one.
    #[test]
    fn explicit_row_publisher_offset_from_registry_collapses_catalog_identity() {
        let feed = crate::ingest::registry::parse_one_row(
            r#"{
                "venue":"KALSHI","category":"perps","code":"d","kind":"MarketByPrice",
                "group":"233.84.178.3","emit_trades":true,"arbitration":"Sticky",
                "publisher_offset":100,
                "publishers":{"explicit":[{"mktdata":32000,"refdata":42000,"snapshot":52000}]}}"#,
        );
        let offset = feed
            .mirror_offset
            .expect("an explicit row must be able to declare a mirror offset");

        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let base = mbp_ctx_mirrored("KALSHI", &arbiter, &instruments, PortRole::Combined, offset);
        let mirror = mbp_ctx_mirrored("KALSHI", &arbiter, &instruments, PortRole::Combined, offset);

        let def_datagram = |channel_id: u8| {
            mbp_datagram_v3(
                channel_id,
                0,
                0,
                &[
                    mbp_wire::enc_manifest_summary(&codec_mbp::ManifestSummary {
                        channel_id,
                        valid: true,
                        manifest_seq: 1,
                        instrument_count: 1,
                        ts: 0,
                    }),
                    enc_instrument_def_v3(41, 3, "MARKET-X", 1),
                ],
            )
        };
        proc.on_datagram(&def_datagram(1), &base);
        proc.on_datagram(&def_datagram(101), &mirror);

        let cat = instruments.lock().unwrap();
        assert_eq!(
            cat.len(),
            1,
            "an explicit row's declared mirror must collapse the two paths into one catalog entry"
        );
    }

    /// An `MbpProcessor` with `ids` defined and each synced from an empty-book anchor, in the drive
    /// order the wire uses: reference data, then the snapshot feed, then deltas.
    fn synced_mbp_proc(
        arbiter: &SharedArbiter,
        instruments: &crate::model::InstrumentSnapshot,
        channel: u8,
        reset_count: u8,
        ids: &[u32],
    ) -> MbpProcessor {
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(channel, reset_count, 1, &mbp_refdata(ids)),
            &make_ctx(arbiter, instruments, PortRole::Combined),
        );
        for (n, id) in ids.iter().enumerate() {
            proc.on_datagram(
                &mbp_wire::datagram(
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41, 42])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        // Both rotations use snapshot_id 5 — the collision the route must not key on.
        for (id, price) in [(41u32, 6200i64), (42, 6300)] {
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(id, 5, 0, 0, &[(MBP_BID, price, 10)])),
                &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
            );
        }
        // A snapshot alone carries no Source ID and stays deferred; reveal each instrument via a
        // no-op Trade so the already-installed book's rebaseline reaches the wire, unmodified.
        for id in [41u32, 42] {
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(id, 1)]),
                &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::datagram(
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
        // A snapshot alone carries no Source ID and stays deferred; reveal via a no-op Trade so
        // the already-installed book's rebaseline reaches the wire, unmodified.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
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

    /// Round-3 review, finding 5: `SnapshotEnd`'s inline re-baseline (fired immediately so a later
    /// delta in the same datagram follows it incrementally) must also clear `revealed_this_datagram` for
    /// the instrument it just re-baselined — an earlier message in the SAME datagram may have already
    /// revealed it, and leaving the entry behind makes the end-of-datagram sweep double-emit an identical,
    /// redundant second `book` for it. Reproduced with a `Trade` that reveals instrument 41 (routed
    /// through `ensure_book` per finding 1, so the book exists but stays `AwaitingSnapshot`) followed,
    /// in the SAME datagram, by a complete snapshot rotation for the same instrument — only reachable via
    /// `PortRole::Combined` (the live three-port row never carries a Trade and a snapshot rotation on
    /// one socket).
    #[test]
    fn mbp_reveal_and_snapshot_install_in_one_datagram_emits_exactly_one_rebaseline() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        let mut msgs = vec![mbp_reveal(41, 1)]; // Trade: reveals + creates the book (AwaitingSnapshot)
        msgs.extend(mbp_snapshot(
            41,
            1,
            0,
            0,
            &[(MBP_BID, 6200, 10), (MBP_ASK, 6300, 20)],
        ));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &msgs),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );

        let books = drain_books(&mut rx);
        assert_eq!(
            books.len(),
            1,
            "exactly one book message for an instrument revealed and snapshot-installed in the \
             same datagram, got {books:?}"
        );
        assert!(books[0].snapshot, "advisory rebuild flag");
        assert!(books[0].last, "a buffering consumer wedges without it");
        assert_eq!(
            shape(&books[0]),
            vec![
                (BookAction::Clear, BookSide::Both, 0.0, 0.0),
                (BookAction::Update, BookSide::Bid, 6200.0, 10.0),
                (BookAction::Update, BookSide::Ask, 6300.0, 20.0),
            ],
            "the installed snapshot's full re-baseline, not a second redundant one"
        );
    }

    /// Round-3 review, finding C (coverage) for the `Trade` -> `ensure_book` routing (finding 1): a
    /// trade-only instrument's `revealed` entry must never outlive, or exist without, a matching
    /// `books` entry — the exact bound `revealed`'s own doc comment claims. Proven two ways: right
    /// after a trade-only reveal, both maps hold the key (the book stuck permanently
    /// `AwaitingSnapshot`, since a `Trade` never touches book content); and under a flood past
    /// `MAX_PRICE_BOOKS`, both are evicted TOGETHER for the oldest trade-only-revealed instrument —
    /// never `revealed` alone, which is what the pre-fix bare-key-tuple reveal would have produced.
    #[test]
    fn mbp_trade_only_reveal_keeps_books_and_revealed_in_lockstep() {
        use super::{PriceBookKey, MAX_PRICE_BOOKS};

        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &[mbp_reveal(41, 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let key41: PriceBookKey = (TEST_PUB, 0u8, 41u32);
        assert!(
            proc.books.contains_key(&key41),
            "a trade-only reveal must create a book entry"
        );
        assert!(proc.revealed.contains_key(&key41));
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::AwaitingSnapshot),
            "a trade-only book never leaves AwaitingSnapshot — no content is ever emitted for it"
        );

        // Flood MAX_PRICE_BOOKS other instruments through refdata + a trade-only reveal, evicting
        // instrument 41's book (oldest-first).
        let flood = (MAX_PRICE_BOOKS as u32) + 5;
        for i in 100..100 + flood {
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[i])),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 2, &[mbp_reveal(i, 1)]),
                &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
            );
        }

        assert!(
            proc.books.len() <= MAX_PRICE_BOOKS,
            "books must stay bounded, got {}",
            proc.books.len()
        );
        assert!(
            !proc.books.contains_key(&key41),
            "instrument 41's book must have been evicted by the flood"
        );
        assert!(
            !proc.revealed.contains_key(&key41),
            "revealed must be evicted IN LOCKSTEP with books — never outliving it, per the \
             field's own doc comment"
        );
    }

    /// Drain every message currently queued, in wire order, as owned `FeedMessage`s — for tests
    /// that need to see message TYPE and ORDER together (`drain_books` only sees `book`s).
    fn drain_all(rx: &mut broadcast::Receiver<std::sync::Arc<FeedMessage>>) -> Vec<FeedMessage> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push((*m).clone());
        }
        out
    }

    /// Nothing is emitted for an instrument until its wire Source ID is known — not the
    /// definition, not the book. The moment a `LevelUpdate` reveals it: the deferred
    /// `NormalizedInstrument` goes out FIRST, stamped with the wire id, then a `book`. The feed
    /// row's own venue must NOT appear anywhere — driving through `mbp_ctx` with a deliberately
    /// wrong row venue is what proves the fallback is gone. `SnapshotBegin`/`SnapshotLevel`/
    /// `SnapshotEnd` carry no Source ID field on the wire (only `LevelUpdate`/`BookClear`/`Trade`
    /// do), which is exactly why the snapshot alone cannot reveal anything and a `LevelUpdate` is
    /// driven through after it.
    ///
    /// A second `LevelUpdate` follows and its `book` is what the `source_id`/`venue` assertions
    /// run against, not the first: a market's FIRST-EVER admission is re-baselined by the arbiter's
    /// authority gate from `BookAccumulator::to_book` (`src/model.rs`), which still hardcodes
    /// `source_id: 0` — a separate, already-tracked gap (outside this task's scope: `model.rs` is
    /// off limits here) that a first-admission book runs into regardless of what this file stamped
    /// on it. The second admission is not a first admission, so it reaches the wire with this
    /// file's own fields intact, which is what this test is actually about.
    #[test]
    fn an_emitted_book_takes_its_source_from_the_wire() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        // Row venue is deliberately a lie; the wire id must win.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Combined,
            ),
        );
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6100, 20)])),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Snapshot,
            ),
        );
        assert!(
            drain_all(&mut rx).is_empty(),
            "the snapshot alone carries no Source ID, so nothing is emitted yet"
        );

        proc.on_datagram(
            &mbp_wire::datagram(
                0,
                0,
                3,
                &[mbp_level_with_source(41, 1, MBP_BID, 6150, 25, 2, 1)],
            ),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Mktdata,
            ),
        );
        let msgs = drain_all(&mut rx);
        assert_eq!(
            msgs.len(),
            2,
            "the deferred definition, then the book: {msgs:?}"
        );
        let FeedMessage::Instrument(inst) = &msgs[0] else {
            panic!("expected the deferred instrument first, got {:?}", msgs[0]);
        };
        assert_eq!(inst.source_id, 1, "verbatim wire Source ID");
        assert_eq!(
            &*inst.source, "HYPERLIQUID",
            "named from the wire id, not the feed row"
        );
        assert_eq!(&*inst.venue, "HYPERLIQUID");
        assert!(
            matches!(msgs[1], FeedMessage::Book(_)),
            "expected the book second, got {:?}",
            msgs[1]
        );

        proc.on_datagram(
            &mbp_wire::datagram(
                0,
                0,
                4,
                &[mbp_level_with_source(41, 2, MBP_BID, 6160, 15, 3, 1)],
            ),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Mktdata,
            ),
        );
        let books = drain_books(&mut rx);
        assert_eq!(
            books.len(),
            1,
            "no second Instrument re-announce; already revealed"
        );
        assert_eq!(books[0].source_id, 1, "verbatim wire Source ID");
        assert_eq!(
            &*books[0].source, "HYPERLIQUID",
            "named from the wire id, not the feed row"
        );
        assert_eq!(&*books[0].venue, "HYPERLIQUID");
    }

    /// An instrument that never receives an id-bearing message never appears on the wire at all —
    /// the snapshot machinery carries no Source ID field, so a snapshot-only instrument stays
    /// deferred forever. Not the definition, not the book. Reintroducing a `0`/feed-row fallback to
    /// fill that gap is exactly the regression this test guards against.
    #[test]
    fn a_snapshot_only_instrument_emits_nothing_at_all() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Combined,
            ),
        );
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6100, 20)])),
            &mbp_ctx(
                "NotTheWireSource",
                &arbiter,
                &instruments,
                PortRole::Snapshot,
            ),
        );

        let msgs = drain_all(&mut rx);
        assert!(
            msgs.is_empty(),
            "no definition, no book — nothing is emitted before a Source ID is known: {msgs:?}"
        );
    }

    /// A batch of level updates in one datagram coalesces into ONE `book` message per instrument, with
    /// `last: true`. Cross-instrument atomicity is not promised, so per-datagram batching is correct.
    #[test]
    fn mbp_one_book_message_per_instrument_per_datagram() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41, 42]);
        // `synced_mbp_proc`'s empty-anchor snapshots carry no Source ID and stay deferred; reveal
        // both instruments first (via a no-op Trade) so the datagram below tests ordinary incremental
        // coalescing, not the first-reveal re-baseline case covered separately. Source ID 0, matching
        // the `mbp_level` deltas below: a mismatched id here would itself be a (real, and now
        // detected) Source ID change, forcing a re-baseline instead of the incremental batch this
        // test means to exercise.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 0), mbp_reveal(42, 0)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);

        proc.on_datagram(
            &mbp_wire::datagram(
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
            &mbp_wire::datagram(0, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &mkt,
        );
        let _ = drain_books(&mut rx);
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 101, &[mbp_level(41, 2, MBP_BID, 6200, 0, 7_001)]),
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::datagram(
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
        // A market's FIRST-EVER admission is re-baselined by the arbiter's authority gate
        // (`BookAccumulator::to_book`) regardless of what this file sends, so reveal here (via a
        // no-op Trade) rather than letting the from-price clear below be that first admission —
        // otherwise its exact-deletes shape would be replaced by a full re-baseline before this
        // test ever sees it. Source ID 0, matching the `clear` deltas below (see
        // `mbp_one_book_message_per_instrument_per_datagram` for why a mismatch here matters).
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 0)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
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
            &mbp_wire::datagram(
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
            &mbp_wire::datagram(
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

    /// `ManifestSummary`/`InstrumentDefinition` must be gated on `handle_refdata` exactly like the
    /// three sibling processors' paths. Decode does not care what physical port a message type
    /// arrives on, so before the gate one forged datagram to the **market-data** port — source IP
    /// spoofed to the real publisher's, carrying `ManifestSummary { manifest_seq: latest + 1 }` —
    /// reached `RefDataState::on_manifest` and cleared `defs`. Every MBP emission path gates on a
    /// resolved definition (`ensure_book`/`send_book`/`exponents`), so the venue's `book` and tape
    /// go dark until the next refdata burst.
    #[test]
    fn mbp_forged_manifest_on_the_mktdata_port_cannot_clear_definitions() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41]);
        let _ = drain_books(&mut rx);

        // The attacker's datagram: same publisher IP, same channel, a manifest one seq ahead of the
        // real one, delivered to the market-data port.
        let forged = mbp_wire::datagram(
            0,
            0,
            50,
            &[mbp_wire::enc_manifest_summary(
                &codec_mbp::ManifestSummary {
                    channel_id: 0,
                    valid: true,
                    manifest_seq: 2,
                    instrument_count: 1,
                    ts: 0,
                },
            )],
        );
        proc.on_datagram(
            &forged,
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert!(
            !drain_books(&mut rx).is_empty(),
            "a refdata-shaped message on the market-data port must not clear the publisher's \
             definitions — the venue's book would go dark until the next refdata burst"
        );
    }

    /// The bounding half of the same gate, mirroring
    /// `mbo_manifest_burst_via_mktdata_port_does_not_leak_pending_channel`: a role that does not
    /// handle reference data must process no reference-data message at all, so a forged-source
    /// burst to the market-data port mints no `PerPublisher` state whatsoever.
    #[test]
    fn mbp_manifest_burst_via_mktdata_port_mints_no_refdata_state() {
        use super::MAX_PUBLISHERS;

        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));

        let burst = mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[0]));
        for i in 0..(MAX_PUBLISHERS as u32) + 50 {
            let mut ctx = mbp_ctx("MBPFORGE", &arbiter, &instruments, PortRole::Mktdata);
            ctx.publisher = ip(i);
            proc.on_datagram(&burst, &ctx);
        }

        assert!(
            proc.state.states.is_empty(),
            "a role that doesn't handle refdata must mint NO per-publisher state, got {}",
            proc.state.states.len()
        );
    }

    /// `PerPublisher` evicts the oldest publisher once `MAX_PUBLISHERS` is exceeded; the three
    /// sibling processors drain `take_evicted()` and drop that publisher's derived state. MBP did
    /// not, so an evicted publisher's books, revealed ids, and channel state outlived the reference
    /// data they depend on.
    #[test]
    fn mbp_an_evicted_publisher_leaves_no_derived_state_behind() {
        use super::MAX_PUBLISHERS;

        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));
        let first = ip(0);

        // Give the first publisher a fully-built book, then push it out of the map.
        for i in 0..(MAX_PUBLISHERS as u32) + 1 {
            let mut refdata = mbp_ctx("MBPEVICT", &arbiter, &instruments, PortRole::Combined);
            refdata.publisher = ip(i);
            proc.on_datagram(&mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])), &refdata);
            if i == 0 {
                let mut snap = mbp_ctx("MBPEVICT", &arbiter, &instruments, PortRole::Snapshot);
                snap.publisher = first;
                proc.on_datagram(
                    &mbp_wire::datagram(
                        0,
                        0,
                        2,
                        &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10)]),
                    ),
                    &snap,
                );
                let mut mkt = mbp_ctx("MBPEVICT", &arbiter, &instruments, PortRole::Mktdata);
                mkt.publisher = first;
                proc.on_datagram(
                    &mbp_wire::datagram(0, 0, 3, &[mbp_level(41, 2, MBP_BID, 6300, 5, 7_000)]),
                    &mkt,
                );
                assert!(
                    proc.books.keys().any(|(p, _, _)| *p == first),
                    "precondition: the first publisher must have a book to lose"
                );
            }
        }

        assert!(
            proc.state.state_mut(first).is_none(),
            "precondition: the first publisher must have been evicted"
        );
        assert!(
            !proc.books.keys().any(|(p, _, _)| *p == first),
            "an evicted publisher's books must go with it"
        );
        assert!(
            !proc.revealed.keys().any(|(p, _, _)| *p == first),
            "and its revealed source ids"
        );
        assert!(
            !proc.open.keys().any(|(p, _)| *p == first)
                && !proc.last_reset.keys().any(|(p, _)| *p == first),
            "and its per-channel snapshot/reset state"
        );
    }

    /// The wire-level form of `a_duplicated_snapshot_begin_does_not_restart_assembly`: a duplicated
    /// **datagram** carrying the rotation's `SnapshotBegin`, which is routine on multicast. Before
    /// the `PriceBook` guard, the re-begin restarted assembly, the group then ended one level short,
    /// and the incomplete-group path cleared the live book — so a market that was `Ready` and
    /// serving dropped to `AwaitingSnapshot` on one duplicated packet.
    #[test]
    fn mbp_a_duplicated_snapshot_begin_datagram_keeps_the_live_book() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41]);
        // Give the live book a level, so there is something to lose.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 10, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::Ready),
            "precondition: the market is serving"
        );

        // A rebuild rotation, split across datagrams so the duplicate lands mid-assembly.
        let rotation = mbp_snapshot(41, 2, 500, 9, &[(MBP_BID, 6300, 20), (MBP_ASK, 6400, 30)]);
        let (begin, rest) = rotation.split_first().unwrap();
        let snap = || make_ctx(&arbiter, &instruments, PortRole::Snapshot);
        let begins = std::slice::from_ref(begin);
        proc.on_datagram(&mbp_wire::datagram(0, 0, 11, begins), &snap());
        proc.on_datagram(&mbp_wire::datagram(0, 0, 12, &rest[..1]), &snap());
        // The duplicate: the very same begin datagram, redelivered.
        proc.on_datagram(&mbp_wire::datagram(0, 0, 11, begins), &snap());
        proc.on_datagram(&mbp_wire::datagram(0, 0, 13, &rest[1..]), &snap());

        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::Ready),
            "the rotation completed; a duplicated begin must not have stranded it"
        );
        let books = drain_books(&mut rx);
        let last = books.last().expect("the rebuild re-baselines the market");
        assert_eq!(
            shape(last)
                .iter()
                .filter(|(a, _, _, _)| *a != BookAction::Clear)
                .count(),
            2,
            "both snapshot levels installed, got {:?}",
            shape(last)
        );
    }

    /// Emission gates per instrument on a known definition — precision before price, the same gate
    /// every other processor applies. A book for an undefined instrument is never even created.
    #[test]
    fn mbp_no_book_is_emitted_before_the_instrument_definition() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10)])),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
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
            &mbp_wire::datagram(7, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let books = drain_books(&mut rx);
        assert_eq!(books[0].channel, 7, "the datagram header's channel_id");
        assert_eq!(books[0].instrument_id, 41, "the wire instrument id");
        assert_eq!(&*books[0].symbol, "INST-41", "a display label only");
        // The mirror of the Market-by-Order assertion: marked order-level, this market would be
        // served as `order_book` and disappear from every `{"type":"book"}` subscriber it has today.
        // Asserted on a *second* datagram as well: the first batch of a market is the re-baseline the
        // arbiter materializes from its own accumulator, so only the later, forwarded batch carries
        // the flag this processor stamped.
        assert!(
            !books[0].order_level,
            "the re-baseline must be price-aggregated"
        );
        proc.on_datagram(
            &mbp_wire::datagram(7, 0, 101, &[mbp_level(41, 2, MBP_BID, 6300, 20, 8_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let later = drain_books(&mut rx);
        assert!(
            later.last().is_some_and(|b| !b.order_level),
            "and so must the batches the processor stamps itself: {later:?}"
        );
    }

    /// The `instrument` definition carries the same identity pair, so a consumer joins a book to its
    /// precision on `(venue, channel, instrument_id)` rather than the colliding `symbol`.
    #[test]
    fn mbp_instrument_definitions_carry_the_identity_pair() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(7, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        // Checked before any reveal: revealing (below) now runs even a `Trade` through
        // `ensure_book` (a trade-only instrument's `revealed` entry must not outlive `books`'
        // bound), so this invariant only holds for refdata ALONE, not after a subsequent reveal.
        assert!(proc.books.is_empty(), "reference data alone builds no book");

        // Refdata alone carries no Source ID and stays deferred; reveal via a no-op Trade (same
        // channel as the refdata burst above) so the deferred Instrument reaches the wire.
        proc.on_datagram(
            &mbp_wire::datagram(7, 0, 2, &[mbp_reveal(41, 1)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        let mut seen = Vec::new();
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Instrument(i) = &*m {
                seen.push((i.channel, i.instrument_id));
            }
        }
        assert_eq!(seen, vec![(7, 41)]);
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
            let mut proc = MbpProcessor::new(tape(emit_trades));
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 2, std::slice::from_ref(&trade)),
                &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
            );
            let trades = std::iter::from_fn(|| rx.try_recv().ok())
                .filter(|m| matches!(&**m, FeedMessage::Trade(_)))
                .count();
            assert_eq!(trades, want, "emit_trades = {emit_trades}");
        }
    }

    /// §4.7 — `EndOfSession` from one path must drop only that path's books. Under the order-keyed
    /// processor's handler it also cleared the venue's shared floor, so one path shutting down tore
    /// down the live published book.
    #[test]
    fn mbp_end_of_session_is_scoped_to_the_emitting_path() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let pub_a = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));
        let mut proc = MbpProcessor::new(tape(false));
        // Reference-data state is per publisher, so each path sends its own burst — which is what
        // they do on the wire, sharing one refdata port.
        for publisher in [pub_a, pub_b] {
            let mut refdata = make_ctx(&arbiter, &instruments, PortRole::Combined);
            refdata.publisher = publisher;
            proc.on_datagram(&mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])), &refdata);
            let mut snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
            snap.publisher = publisher;
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[])),
                &snap,
            );
        }
        assert_eq!(mbp_status(&proc, pub_b, 0, 41), Some(BookStatus::Ready));

        let mut a_mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        a_mkt.publisher = pub_a;
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 100, &[mbp_wire::enc_end_of_session(9_000)]),
            &a_mkt,
        );

        assert_eq!(
            mbp_status(&proc, pub_a, 0, 41),
            Some(BookStatus::AwaitingSnapshot),
            "the ending path's book is dropped"
        );
        assert_eq!(
            mbp_status(&proc, pub_b, 0, 41),
            Some(BookStatus::Ready),
            "the peer path keeps serving; authority transfers to it"
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
            &mbp_wire::datagram(0, 0, 100, &[mbp_level(41, 1, MBP_BID, 6200, 10, 7_000)]),
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
        let mut proc = MbpProcessor::new(tape(false));
        for publisher in [pub_a, pub_b] {
            let mut refdata = make_ctx(&arbiter, &instruments, PortRole::Combined);
            refdata.publisher = publisher;
            proc.on_datagram(&mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])), &refdata);
            let mut snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);
            snap.publisher = publisher;
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[])),
                &snap,
            );
        }

        let mut a_mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        a_mkt.publisher = pub_a;
        proc.on_datagram(&mbp_wire::datagram(0, 1, 100, &[]), &a_mkt);

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
        let mut proc = MbpProcessor::new(tape(false));
        let mut ids = heavy.clone();
        ids.push(99);
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&ids)),
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
        proc.on_datagram(&mbp_wire::datagram(0, 0, 2, &[]), &mkt);

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
            &mbp_wire::datagram(
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
            &mbp_wire::datagram(0, 0, 101, &mbp_snapshot(41, 2, 100, 8, &[])),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        assert_eq!(proc.buffered_total, recomputed(&proc), "after a replay");

        proc.on_datagram(
            &mbp_wire::datagram(
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
            &mbp_wire::datagram(0, 0, 103, &[mbp_level(42, 20, MBP_BID, 6300, 10, 7_004)]),
            &mkt,
        );
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 104, &[mbp_wire::enc_end_of_session(9_000)]),
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
            &mbp_wire::datagram(0, 0, 105, &[mbp_level(41, 30, MBP_BID, 6200, 10, 7_005)]),
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::datagram(
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
            &mbp_wire::datagram(
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );
        // A snapshot claiming a baseline of 100 while the publisher is really at 1.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 100, &[])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot),
        );
        // Reveal now (a no-op Trade) so the duplicate deltas below are not ALSO this instrument's
        // first-ever reveal — a reveal always shows the current book regardless of whether the
        // message that triggered it actually applied, which would otherwise put an (empty)
        // re-baseline on the wire and defeat the "publishes nothing" assertion below. Source ID 0,
        // matching the `mbp_level` deltas below: a mismatched id here would itself be a Source ID
        // change, forcing an unwanted re-baseline instead of exercising the duplicate-delta path.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 0)]),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);

        let before = metrics()
            .mbp_duplicate_deltas
            .with_label_values(&[venue])
            .get();
        proc.on_datagram(
            &mbp_wire::datagram(
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
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined),
        );

        let before = metrics()
            .mbp_orphan_snapshot_levels
            .with_label_values(&[venue])
            .get();
        proc.on_datagram(
            &mbp_wire::datagram(
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

    /// A synced book **correctly** declines a rotation it does not need (§4.2: `Ready` plus a
    /// `Last Instrument Seq` it has already applied). Its levels still arrive, and they are
    /// counted apart from orphans rather than summed with them.
    ///
    /// This is the steady state, not an edge case: publishers rotate snapshots continuously, so
    /// once the books sync every rotation is declined. Measured against the live Lashay perps feed
    /// 2026-08-08, that was ~415 levels/s — 100% of the reference parser's ~410 levels/s — all of
    /// it landing on the orphan counter and burying the genuine anomaly that counter exists to
    /// surface (a lost `SnapshotBegin`, an interleaved group, or the stale era covered by
    /// `mbp_a_stale_era_snapshot_neither_installs_nor_resets`).
    #[test]
    fn mbp_a_declined_rotation_is_counted_apart_from_orphans() {
        let venue = "MbpDeclinedRotationTest";
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let refdata = mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined);
        let snap = mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot);

        proc.on_datagram(&mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])), &refdata);
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 2, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10)])),
            &snap,
        );
        // The snapshot alone carries no Source ID and stays deferred; reveal via a no-op Trade so
        // the installed book's re-baseline reaches the wire.
        let mkt = mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata);
        proc.on_datagram(&mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 1)]), &mkt);
        assert_eq!(drain_books(&mut rx).len(), 1, "the first rotation installs");
        assert_eq!(mbp_status(&proc, TEST_PUB, 0, 41), Some(BookStatus::Ready));

        let orphans = || {
            metrics()
                .mbp_orphan_snapshot_levels
                .with_label_values(&[venue])
                .get()
        };
        let declined = || {
            metrics()
                .mbp_declined_rotation_levels
                .with_label_values(&[venue])
                .get()
        };
        let before = (orphans(), declined());

        // The next rotation for the same instrument: `Ready` and already at this
        // `last_instrument_seq`, so `on_snapshot_begin` declines it — the correct call.
        proc.on_datagram(
            &mbp_wire::datagram(
                0,
                0,
                3,
                &mbp_snapshot(
                    41,
                    2,
                    0,
                    0,
                    &[
                        (MBP_BID, 6200, 10),
                        (MBP_BID, 6199, 20),
                        (MBP_ASK, 6300, 30),
                    ],
                ),
            ),
            &snap,
        );

        assert!(
            drain_books(&mut rx).is_empty(),
            "declining republishes nothing, which is the point of declining"
        );
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::Ready),
            "and leaves the live book synced"
        );
        assert_eq!(
            orphans(),
            before.0,
            "a declined rotation is not an orphan: nothing was unroutable"
        );
        assert_eq!(
            declined(),
            before.1 + 3,
            "all three of its levels are counted as declined instead"
        );
    }

    /// The authority gate decides which path reaches the wire from per-market health, so a book
    /// leaving `Ready` has to be reported. Only transitions are reported, not every datagram.
    #[test]
    fn mbp_book_health_reaches_the_authority() {
        let (arbiter, _rx, instruments) = mbp_harness();
        lock(&arbiter).set_authority(
            crate::ingest::authority::AuthorityConfig {
                leader_timeout_ns: 1_000_000_000,
                sample_interval_ns: 1_000_000_000,
                transfer_margin_ns: 1_000,
                transfer_win_rate: 0.6,
                min_window_samples: 10,
            },
            5_000_000_000,
        );
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 3, 0, &[41]);
        // `synced_mbp_proc`'s snapshot alone carries no Source ID and stays deferred — which
        // defers the health report too (`report_health` skips a not-yet-revealed key, since there
        // is no `MarketKey` yet for the authority to know about). Reveal via a `LevelUpdate` (not
        // the no-op `mbp_reveal` Trade): it also marks the instrument `touched`, which is what
        // triggers this datagram's own end-of-datagram health-report sweep, so the book's `Ready` status
        // actually reaches the authority in this same call.
        proc.on_datagram(
            &mbp_wire::datagram(
                3,
                0,
                50,
                &[mbp_level_with_source(41, 1, MBP_BID, 6200, 10, 3, 1)],
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let market = (
            crate::model::venue_arc("HYPERLIQUID"),
            crate::model::category_arc("testcategory"),
            3u8,
            41u32,
        );
        let path = Transport::Edge(TEST_PUB);
        let healthy = |a: &SharedArbiter| lock(a).authority().healthy(&market, path);

        proc.on_datagram(
            &mbp_wire::datagram(3, 0, 100, &[mbp_wire::enc_end_of_session(9_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        assert!(
            !healthy(&arbiter),
            "an ended session hands the market to the peer path"
        );

        // The recovery direction is what pins a real report: `healthy` answers true for a market the
        // authority has never heard of, so only the unhealthy -> healthy transition proves the call.
        proc.on_datagram(
            &mbp_wire::datagram(3, 0, 101, &mbp_snapshot(41, 2, 0, 0, &[])),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        assert!(healthy(&arbiter), "a re-synced book takes its market back");
    }

    /// The `(publisher, channel)` reset/open-group maps take their keys from unauthenticated wire
    /// data, so they must stay bounded — and a publisher we hold no reference data for must not enter
    /// them at all: it has no books to invalidate, and minting state from the market-data path is what
    /// would let a forged-source flood evict the real publishers' definitions.
    #[test]
    fn mbp_channel_key_maps_are_bounded_and_untracked_publishers_mint_nothing() {
        let (arbiter, _rx, instruments) = mbp_harness();
        let mut proc = synced_mbp_proc(&arbiter, &instruments, 0, 0, &[41]);
        let ip = |i: u32| IpAddr::V4(std::net::Ipv4Addr::from(0x0a00_0000 + i));

        // A market-data flood from publishers that never sent reference data, each bumping its era.
        for i in 0..(MAX_CHANNEL_KEYS as u32 + 50) {
            for reset_count in [0, 1] {
                let mut ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
                ctx.publisher = ip(i);
                proc.on_datagram(&mbp_wire::datagram(0, reset_count, 1, &[]), &ctx);
            }
        }
        assert_eq!(
            proc.last_reset.len(),
            1,
            "only the real publisher is tracked"
        );
        assert!(
            proc.state.def(TEST_PUB, 41).is_some(),
            "its definitions survive"
        );
        assert_eq!(
            mbp_status(&proc, TEST_PUB, 0, 41),
            Some(BookStatus::Ready),
            "and so does its book"
        );

        // Two tracked publishers across the whole channel space overflow the map, oldest first.
        let peer = ip(1_000);
        let mut refdata = make_ctx(&arbiter, &instruments, PortRole::Combined);
        refdata.publisher = peer;
        proc.on_datagram(&mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])), &refdata);
        for publisher in [TEST_PUB, peer] {
            for channel in 0..=u8::MAX {
                let mut ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
                ctx.publisher = publisher;
                proc.on_datagram(&mbp_wire::datagram(channel, 0, 1, &[]), &ctx);
            }
        }
        assert!(
            proc.last_reset.len() <= MAX_CHANNEL_KEYS,
            "reset map must stay bounded, got {}",
            proc.last_reset.len()
        );
        assert!(
            proc.last_reset.contains_key(&(peer, u8::MAX)),
            "newest kept"
        );
        assert!(
            !proc.last_reset.contains_key(&(TEST_PUB, 1)),
            "oldest evicted"
        );
    }

    /// An unrecognized `clear_side` clears nothing in the book (`PriceBook::apply` matches the three
    /// known values exactly), so the wire must claim nothing either. Publishing a whole-side `Clear`
    /// would drop a side the consumer should keep, silently, with every later sequence check passing
    /// and no snapshot rotation to repair it.
    #[test]
    fn mbp_an_unrecognized_clear_side_publishes_nothing() {
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 1, &mbp_refdata(&[41])),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        proc.on_datagram(
            &mbp_wire::datagram(
                0,
                0,
                2,
                &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10), (MBP_ASK, 6300, 20)]),
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );
        // Reveal now (a no-op Trade) so the unrecognized-clear-side BookClear below is not ALSO
        // this instrument's first-ever reveal — a reveal always shows the current book regardless
        // of what the revealing message itself did, which would otherwise put a re-baseline on the
        // wire and defeat the "publishes nothing" assertion below. Source ID 0, matching the
        // `BookClear` below: a mismatched id here would itself be a Source ID change, forcing an
        // unwanted re-baseline instead of exercising the unrecognized-clear-side path.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &[mbp_reveal(41, 0)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);

        proc.on_datagram(
            &mbp_wire::datagram(
                0,
                0,
                100,
                &[mbp_wire::enc_book_clear(&codec_mbp::BookClear {
                    instrument_id: 41,
                    source_id: 0,
                    clear_side: 3, // outside the wire's three defined values
                    scope: codec_mbp::SCOPE_ENTIRE_SIDE,
                    per_instrument_seq: 1,
                    from_price_raw: 0,
                    ts: 7_000,
                    clear_reason: 0,
                })],
            ),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );

        assert!(
            drain_books(&mut rx).is_empty(),
            "the book cleared nothing, so nothing is published"
        );
        let book = &proc.books[&(TEST_PUB, 0, 41)];
        assert_eq!(book.bids().count(), 1, "both sides intact");
        assert_eq!(book.asks().count(), 1);
    }

    /// A publisher restart bumps the era on all three ports, but they are separate sockets with
    /// separate queues: the previous era's snapshot rotation is still arriving after the market-data
    /// port has moved on. Installing it would republish the dead session's book as a fresh
    /// re-baseline, and a reset memo shared across the roles would re-reset the channel on every
    /// interleaving of the backlog.
    #[test]
    fn mbp_a_stale_era_snapshot_neither_installs_nor_resets() {
        let venue = "MbpStaleEraTest";
        let (arbiter, mut rx, instruments) = mbp_harness();
        let mut proc = MbpProcessor::new(tape(false));
        let refdata = mbp_ctx(venue, &arbiter, &instruments, PortRole::Combined);
        let snap = mbp_ctx(venue, &arbiter, &instruments, PortRole::Snapshot);
        // Era 1: definitions, then a snapshot that installs and publishes its re-baseline.
        proc.on_datagram(&mbp_wire::datagram(0, 1, 1, &mbp_refdata(&[41])), &refdata);
        proc.on_datagram(
            &mbp_wire::datagram(0, 1, 2, &mbp_snapshot(41, 1, 0, 0, &[(MBP_BID, 6200, 10)])),
            &snap,
        );
        // The snapshot alone carries no Source ID and stays deferred; reveal via a no-op Trade so
        // the installed book's re-baseline reaches the wire.
        let mkt = mbp_ctx(venue, &arbiter, &instruments, PortRole::Mktdata);
        proc.on_datagram(&mbp_wire::datagram(0, 1, 3, &[mbp_reveal(41, 1)]), &mkt);
        assert_eq!(drain_books(&mut rx).len(), 1, "the current era installs");

        let resets = || {
            metrics()
                .mbp_channel_resets
                .with_label_values(&[venue])
                .get()
        };
        let before = (
            resets(),
            metrics()
                .mbp_orphan_snapshot_levels
                .with_label_values(&[venue])
                .get(),
        );
        // The previous run's rotation, still draining off the snapshot socket.
        proc.on_datagram(
            &mbp_wire::datagram(0, 0, 3, &mbp_snapshot(41, 9, 0, 0, &[(MBP_BID, 1, 1)])),
            &snap,
        );

        assert!(
            drain_books(&mut rx).is_empty(),
            "the dead era's book is not republished"
        );
        assert_eq!(
            resets(),
            before.0,
            "and a snapshot-port datagram resets nothing"
        );
        assert_eq!(
            metrics()
                .mbp_orphan_snapshot_levels
                .with_label_values(&[venue])
                .get(),
            before.1 + 1,
            "its levels are counted as unroutable"
        );
        assert_eq!(
            proc.books[&(TEST_PUB, 0, 41)].bids().next().map(|(p, _)| p),
            Some(6200),
            "the live book is untouched"
        );
    }

    /// The book map is bounded the same way, since the wire `instrument_id` is spoofable too. An
    /// evicted book must also be reported unhealthy: this path no longer holds that market, and a stale
    /// `healthy` report would keep the authority electing it while a peer path's live book is dropped.
    #[test]
    fn mbp_books_map_is_bounded_under_instrument_flood() {
        let (arbiter, _rx, instruments) = mbp_harness();
        lock(&arbiter).set_authority(
            crate::ingest::authority::AuthorityConfig {
                leader_timeout_ns: 1_000_000_000,
                sample_interval_ns: 1_000_000_000,
                transfer_margin_ns: 1_000,
                transfer_win_rate: 0.6,
                min_window_samples: 10,
            },
            5_000_000_000,
        );
        let mut proc = MbpProcessor::new(tape(false));
        let ids: Vec<u32> = (0..(MAX_PRICE_BOOKS as u32 + 50)).collect();
        // One burst per 200 definitions keeps each datagram's message count inside the header's u8.
        for chunk in ids.chunks(200) {
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 1, &mbp_refdata(chunk)),
                &make_ctx(&arbiter, &instruments, PortRole::Combined),
            );
        }
        for chunk in ids.chunks(100) {
            let deltas: Vec<Vec<u8>> = chunk
                .iter()
                .map(|id| mbp_level(*id, 1, MBP_BID, 6200, 10, 7_000))
                .collect();
            proc.on_datagram(
                &mbp_wire::datagram(0, 0, 100, &deltas),
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
        assert!(
            !proc.books.contains_key(&(TEST_PUB, 0, 0)),
            "the oldest book was evicted"
        );
        assert!(
            !lock(&arbiter).authority().healthy(
                // "SOURCE_0": every `mbp_level` delta above stamps wire Source ID 0.
                &(
                    crate::model::venue_arc("SOURCE_0"),
                    crate::model::category_arc("testcategory"),
                    0u8,
                    0u32,
                ),
                Transport::Edge(TEST_PUB)
            ),
            "an evicted book leaves its market unhealthy for this path"
        );
    }

    // ---- the order-level (L3) `book` product ----

    /// Market-by-Order now produces the order-level `book` alongside its existing `depth`, and every
    /// change carries the venue's real `order_id` — a zero there tells a consumer to aggregate by
    /// price, silently degrading an L3 feed to L2.
    #[test]
    fn mbo_emits_the_order_level_book_alongside_depth() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let _ = drain_books(&mut rx);

        let ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        // The first delta reveals the instrument, so its book goes out as a re-baseline: the consumer
        // has never seen this identity, and the book may already hold snapshot content.
        proc.on_datagram(&datagram(&[add(1, 4242, 5_000)]), &ctx);
        let revealed = drain_books(&mut rx);
        let first = revealed.last().expect("a book message must be emitted");
        assert!(first.snapshot);
        // This flag is what `sinks::ws` renders as `type: "order_book"`. Unset here, an order-level
        // market goes out tagged `book` and a consumer keying by price collapses co-priced orders —
        // the corruption the separate type exists to prevent. Asserted on the emission, not on a
        // hand-built fixture, because this is the only production writer.
        assert!(
            first.order_level,
            "a Market-by-Order batch must be marked order-level"
        );
        assert_eq!(
            first
                .changes
                .iter()
                .map(|c| (c.action, c.order_id, c.size))
                .collect::<Vec<_>>(),
            vec![(BookAction::Clear, 0, 0.0), (BookAction::Update, 4242, 5.0),]
        );

        // From there each event is published as it came, carrying the venue's own order id.
        proc.on_datagram(&datagram(&[add(2, 4343, 6_000)]), &ctx);
        let books = drain_books(&mut rx);
        let last = books.last().expect("a book message must be emitted");
        assert_eq!(
            last.changes
                .iter()
                .map(|c| (c.action, c.order_id, c.size))
                .collect::<Vec<_>>(),
            vec![(BookAction::Update, 4343, 5.0)]
        );
        assert_eq!(last.source_ts_ns, 6_000);
        assert!(!last.snapshot);
        assert!(last.order_level, "every batch, not just the re-baseline");
    }

    /// The same run still produces `depth`: this adds a product, it does not replace one.
    #[test]
    fn mbo_still_emits_depth_alongside_the_book() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        proc.on_datagram(
            &datagram(&[add(1, 4242, 5_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let msgs = drain_all(&mut rx);
        assert!(msgs.iter().any(|m| matches!(m, FeedMessage::Depth(_))));
        assert!(msgs.iter().any(|m| matches!(m, FeedMessage::Book(_))));
    }

    /// A cancel publishes the order as `Delete`: a consumer's dispatcher branches on the action, so
    /// leaving it an `Update` with size zero would rest a phantom order it keeps forever.
    #[test]
    fn a_cancelled_order_is_published_as_a_delete() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        proc.on_datagram(&datagram(&[add(1, 4242, 5_000)]), &ctx);
        let _ = drain_books(&mut rx);
        proc.on_datagram(
            &datagram(&[enc_order_cancel(&OrderCancel {
                instrument_id: 0,
                source_id: 0,
                reason: 0,
                per_instrument_seq: 2,
                order_id: 4242,
                ts: 6_000,
            })]),
            &ctx,
        );
        let books = drain_books(&mut rx);
        let last = books.last().expect("a book message must be emitted");
        assert_eq!(
            last.changes
                .iter()
                .map(|c| (c.action, c.order_id, c.size))
                .collect::<Vec<_>>(),
            vec![(BookAction::Delete, 4242, 0.0)]
        );
    }

    /// A snapshot install re-baselines structurally: a `Clear` first, then every resting order, ids
    /// included. `changes[0].action == Clear` is what re-baselines a consumer; `snapshot` is advisory.
    #[test]
    fn a_snapshot_install_emits_clear_then_every_resting_order() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Combined),
        );
        // A delta first, so the instrument is revealed and the snapshot install is the re-baseline
        // under test rather than the reveal's.
        proc.on_datagram(
            &datagram(&[add(1, 1, 1_000)]),
            &make_ctx(&arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);

        let snap = |order_id, side, price_raw, qty_raw| {
            enc_snapshot_order(&SnapshotOrder {
                snapshot_id: 4,
                order_id,
                side,
                order_flags: 0,
                enter_ts: 2_000,
                price_raw,
                qty_raw,
            })
        };
        proc.on_datagram(
            &datagram(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 9,
                    total_orders: 2,
                    snapshot_id: 4,
                    last_instrument_seq: 9,
                    ts: 2_000,
                }),
                snap(11, SIDE_BID, 100, 5),
                snap(22, SIDE_ASK, 105, 9),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 9,
                    snapshot_id: 4,
                }),
            ]),
            &make_ctx(&arbiter, &instruments, PortRole::Snapshot),
        );

        let books = drain_books(&mut rx);
        let b = books.last().expect("a re-baseline must be emitted");
        assert!(b.snapshot && b.last);
        assert_eq!(
            b.changes
                .iter()
                .map(|c| (c.action, c.side, c.order_id, c.size))
                .collect::<Vec<_>>(),
            vec![
                (BookAction::Clear, BookSide::Both, 0, 0.0),
                (BookAction::Update, BookSide::Bid, 11, 5.0),
                (BookAction::Update, BookSide::Ask, 22, 9.0),
            ]
        );
    }

    /// The arbiter's re-baseline suppression reads each path's sync state, so the processor must report
    /// one — and report a gap, or a recovering peer would see a phantom healthy path and suppress the
    /// only re-baseline on offer.
    #[test]
    fn a_gapped_book_reports_itself_unsynced() {
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(64);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let ctx = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        proc.on_datagram(&datagram(&[add(1, 1, 1_000)]), &ctx);
        assert_eq!(
            proc.synced_reported.values().copied().collect::<Vec<_>>(),
            vec![true]
        );

        // A sequence jump opens a gap: the book drops to `Recovering` and must say so.
        proc.on_datagram(&datagram(&[add(9, 2, 2_000)]), &ctx);
        assert_eq!(
            proc.synced_reported.values().copied().collect::<Vec<_>>(),
            vec![false]
        );
    }
    /// **Item A.** An `InstrumentReset` restarts the venue's order-id space for that instrument, so
    /// the arbiter's raced order state must go with the book. The market is resolved *before* the
    /// `revealed` entry that resolves it is dropped; resolved after, the drop is dead code and every
    /// re-used order id is refused as a resurrection — silently missing from every consumer's book
    /// until the retention window expires.
    #[test]
    fn an_instrument_reset_lets_the_venue_reuse_an_order_id() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        lock(&arbiter).set_book_replay(Arc::new(Mutex::new(Default::default())));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);
        let snap = make_ctx(&arbiter, &instruments, PortRole::Snapshot);

        // Order 7 rests, then dies: the arbiter now holds a tombstone for it.
        proc.on_datagram(&datagram(&[add(1, 7, 1_000)]), &mkt);
        proc.on_datagram(
            &datagram(&[enc_order_cancel(&OrderCancel {
                instrument_id: 0,
                source_id: 0,
                reason: 0,
                per_instrument_seq: 2,
                order_id: 7,
                ts: 1_100,
            })]),
            &mkt,
        );
        let _ = drain_books(&mut rx);

        proc.on_datagram(
            &datagram(&[enc_instrument_reset(&InstrumentReset {
                instrument_id: 0,
                reason: 0,
                new_anchor_seq: 0,
                ts: 1_150,
            })]),
            &mkt,
        );
        // Re-sync on an empty anchor, then re-reveal on an unrelated order so the re-used id below
        // arrives as an ordinary delta rather than inside a `Clear`-led batch (which bypasses the
        // guard entirely).
        proc.on_datagram(
            &datagram(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: 2,
                    last_instrument_seq: 0,
                    ts: 1_200,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: 2,
                }),
            ]),
            &snap,
        );
        proc.on_datagram(&datagram(&[add(1, 9, 1_300)]), &mkt);
        let _ = drain_books(&mut rx);

        // The venue re-uses order id 7 in the new session.
        proc.on_datagram(&datagram(&[add(2, 7, 1_400)]), &mkt);
        let ids: Vec<u64> = drain_books(&mut rx)
            .iter()
            .flat_map(|b| b.changes.iter().map(|c| c.order_id))
            .collect();
        assert!(
            ids.contains(&7),
            "the re-used order id must reach the wire, not be refused as a resurrection: {ids:?}"
        );
    }

    /// `EndOfSession` drops every publisher's book, so the raced state of every one of them has to go
    /// with it. Scoped to the sending publisher, an `EndOfSession` from a source holding no books of
    /// its own — a refdata-only mirror, an evicted publisher, one forged datagram — resets every real
    /// book to `Recovering` and clears no tombstones, and the new session's re-used order ids are then
    /// refused for exactly the markets nothing is serving any more.
    #[test]
    fn an_end_of_session_from_a_bookless_source_still_clears_the_raced_state() {
        use std::net::Ipv4Addr;
        fn ctx_for<'a>(
            publisher: IpAddr,
            arbiter: &'a SharedArbiter,
            instruments: &'a crate::model::InstrumentSnapshot,
            role: PortRole,
        ) -> DatagramCtx<'a> {
            let mut c = make_ctx(arbiter, instruments, role);
            c.publisher = publisher;
            c
        }
        let holder = IpAddr::V4(Ipv4Addr::new(10, 3, 0, 1));
        let bookless = IpAddr::V4(Ipv4Addr::new(10, 3, 0, 2));
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        lock(&arbiter).set_book_replay(Arc::new(Mutex::new(Default::default())));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let anchor = |sid: u32, ts: u64| {
            datagram(&[
                enc_snapshot_begin(&SnapshotBegin {
                    instrument_id: 0,
                    anchor_seq: 0,
                    total_orders: 0,
                    snapshot_id: sid,
                    last_instrument_seq: 0,
                    ts,
                }),
                enc_snapshot_end(&SnapshotEnd {
                    instrument_id: 0,
                    anchor_seq: 0,
                    snapshot_id: sid,
                }),
            ])
        };
        for publisher in [holder, bookless] {
            proc.on_datagram(
                &datagram(&[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(0, "INST-0", 1),
                ]),
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Combined),
            );
        }
        // Only `holder` builds a book: order 7 rests, then dies, leaving a tombstone.
        proc.on_datagram(
            &anchor(1, 1),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &datagram(&[add(1, 7, 1_000)]),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Mktdata),
        );
        proc.on_datagram(
            &datagram(&[enc_order_cancel(&OrderCancel {
                instrument_id: 0,
                source_id: 0,
                reason: 0,
                per_instrument_seq: 2,
                order_id: 7,
                ts: 1_100,
            })]),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);

        proc.on_datagram(
            &datagram(&[enc_end_of_session(1_200)]),
            &ctx_for(bookless, &arbiter, &instruments, PortRole::Mktdata),
        );
        // The new session re-syncs and re-uses order id 7, arriving as an ordinary delta (a
        // `Clear`-led batch bypasses the guard, so the reveal is spent on an unrelated order first).
        proc.on_datagram(
            &anchor(2, 1_300),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Snapshot),
        );
        proc.on_datagram(
            &datagram(&[add(1, 9, 1_400)]),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Mktdata),
        );
        let _ = drain_books(&mut rx);
        proc.on_datagram(
            &datagram(&[add(2, 7, 1_500)]),
            &ctx_for(holder, &arbiter, &instruments, PortRole::Mktdata),
        );
        let ids: Vec<u64> = drain_books(&mut rx)
            .iter()
            .flat_map(|b| b.changes.iter().map(|c| c.order_id))
            .collect();
        assert!(
            ids.contains(&7),
            "the ended session's tombstone must not refuse the new session's re-used id: {ids:?}"
        );
    }

    /// **Item F.** `EndOfSession` drops EVERY publisher's book to `Recovering` (see
    /// `mbo_end_of_session_resets_peer_publisher_books` for why), so every one of them must report it.
    /// A peer left claiming `synced` is a phantom healthy path, and the arbiter suppresses the
    /// surviving path's only re-baseline against it.
    #[test]
    fn end_of_session_reports_every_publishers_book_unsynced() {
        use crate::ingest::sources::source_label;
        use std::net::Ipv4Addr;
        fn ctx_for<'a>(
            publisher: IpAddr,
            arbiter: &'a SharedArbiter,
            instruments: &'a crate::model::InstrumentSnapshot,
            role: PortRole,
        ) -> DatagramCtx<'a> {
            let mut c = make_ctx(arbiter, instruments, role);
            c.publisher = publisher;
            c
        }
        let pub_a = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let pub_b = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let anchor = datagram(&[
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
        ]);
        for publisher in [pub_a, pub_b] {
            proc.on_datagram(
                &datagram(&[
                    enc_manifest_summary(1, 1),
                    enc_instrument_def(0, "INST-0", 1),
                ]),
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Combined),
            );
            proc.on_datagram(
                &anchor,
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Snapshot),
            );
            proc.on_datagram(
                &datagram(&[add(1, 100, 5_000)]),
                &ctx_for(publisher, &arbiter, &instruments, PortRole::Mktdata),
            );
        }
        let market: MarketKey = (
            venue_arc(source_label(0)),
            category_arc("testcategory"),
            0,
            0,
        );
        assert!(
            lock(&arbiter).book_path_synced(&market, Transport::Edge(pub_b)),
            "B's path must be synced before the session ends"
        );

        proc.on_datagram(
            &datagram(&[enc_end_of_session(6_000)]),
            &ctx_for(pub_a, &arbiter, &instruments, PortRole::Mktdata),
        );
        let a = lock(&arbiter).book_path_synced(&market, Transport::Edge(pub_a));
        let b = lock(&arbiter).book_path_synced(&market, Transport::Edge(pub_b));
        assert!(
            !a && !b,
            "both books dropped to Recovering, so both paths must say so (a={a}, b={b})"
        );
    }

    /// **Item G.** A reveal moves the instrument to a new `MarketKey`. When the rate limit refuses to
    /// materialize the whole book there, the datagram owes the consumer a bare `Clear` — never its
    /// incremental changes, which would land under a key the consumer was never baselined for and
    /// leave the replay accumulator un-baselined so new clients never see the market either.
    #[test]
    fn a_rate_limited_reveal_clears_rather_than_streaming_under_a_new_key() {
        let (tx, mut rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(256);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = synced_mbo_proc(&arbiter, &instruments);
        let mkt = make_ctx(&arbiter, &instruments, PortRole::Mktdata);

        // First reveal (Source ID 0): the whole book, behind a `Clear`.
        proc.on_datagram(&datagram(&[add(1, 7, 1_000)]), &mkt);
        let _ = drain_books(&mut rx);

        // A second Source ID inside the rate-limit interval: the reveal still fires, but the
        // republish does not.
        proc.on_datagram(
            &datagram(&[enc_order_add(&OrderAdd {
                instrument_id: 0,
                source_id: 1,
                side: SIDE_BID,
                order_flags: 0,
                per_instrument_seq: 2,
                order_id: 8,
                enter_ts: 1_100,
                price_raw: 100,
                qty_raw: 5,
            })]),
            &mkt,
        );
        let books = drain_books(&mut rx);
        let shapes: Vec<Vec<(BookAction, u64)>> = books
            .iter()
            .filter(|b| b.venue.as_ref() == crate::ingest::sources::source_label(1))
            .map(|b| b.changes.iter().map(|c| (c.action, c.order_id)).collect())
            .collect();
        assert_eq!(
            shapes,
            vec![vec![(BookAction::Clear, 0u64)]],
            "the new identity gets a bare clear, never incremental order changes"
        );
    }

    /// **Item H.** A publisher evicted from the reference-data map takes its `revealed` entry with it,
    /// and `revealed` is what resolves the market — so a `synced = true` left behind in the arbiter can
    /// never be corrected afterwards. The path is released while the key still resolves, and the
    /// eviction drops the same sibling maps a book eviction does.
    #[test]
    fn an_evicted_publisher_releases_its_path_and_its_sibling_state() {
        use crate::ingest::sources::source_label;
        use std::net::Ipv4Addr;
        let (tx, _rx) = broadcast::channel::<std::sync::Arc<FeedMessage>>(1024);
        let arbiter: SharedArbiter = Arc::new(Mutex::new(Arbiter::new(tx, 8)));
        let instruments = Arc::new(Mutex::new(HashMap::new()));
        let depth: DepthSnapshot = Arc::new(Mutex::new(HashMap::new()));
        let mut proc = MboProcessor::new(depth, tape(false));
        let ctx_for = |publisher: IpAddr, role: PortRole| {
            let mut c = make_ctx(&arbiter, &instruments, role);
            c.publisher = publisher;
            c
        };
        let victim = IpAddr::V4(Ipv4Addr::new(10, 1, 0, 1));
        proc.on_datagram(
            &datagram(&[
                enc_manifest_summary(1, 1),
                enc_instrument_def(0, "INST-0", 1),
            ]),
            &ctx_for(victim, PortRole::Combined),
        );
        proc.on_datagram(
            &datagram(&[
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
            &ctx_for(victim, PortRole::Snapshot),
        );
        proc.on_datagram(
            &datagram(&[add(1, 7, 1_000)]),
            &ctx_for(victim, PortRole::Mktdata),
        );
        let market: MarketKey = (
            venue_arc(source_label(0)),
            category_arc("testcategory"),
            0,
            0,
        );
        assert!(lock(&arbiter).book_path_synced(&market, Transport::Edge(victim)));

        // Fill the per-publisher reference-data map past its cap; the victim was inserted first, so
        // it is the one evicted.
        for i in 0..=MAX_PUBLISHERS {
            let ip = IpAddr::V4(Ipv4Addr::new(10, 2, (i / 256) as u8, (i % 256) as u8));
            proc.on_datagram(
                &datagram(&[enc_manifest_summary(1, 1)]),
                &ctx_for(ip, PortRole::Combined),
            );
        }
        assert!(
            !lock(&arbiter).book_path_synced(&market, Transport::Edge(victim)),
            "an evicted publisher's serving claim must go with its reference data"
        );
        assert!(
            !proc.books.keys().any(|(p, _)| *p == victim)
                && !proc.synced_reported.keys().any(|(p, _)| *p == victim)
                && !proc.last_top.keys().any(|(p, _)| *p == victim)
                && !proc.emitted_symbol.keys().any(|(p, _)| *p == victim)
                && !proc.reveal_rebaselined_ns.keys().any(|(p, _)| *p == victim),
            "the eviction must drop the same sibling maps a book eviction does"
        );
    }
}
