//! The load-bearing property test the Market-by-Order design named: drive a synthetic two-publisher
//! order-level stream through the arbiter, apply what it publishes with a naive consumer that does
//! exactly what PROTOCOL.md tells a consumer to do, and assert that book equals the venue's at every
//! re-baseline boundary and at the end.
//!
//! It exercises the four races that make the merge point non-trivial — a duplicate past the dedup
//! window, a partial fill both publishers see, a stale `Add` for an order a peer already killed, and
//! a publisher that departs while the other is recovering — because each of them corrupts the
//! consumer's book silently, while every per-publisher check upstream still passes.

use doublezero_edge_connect::{
    ingest::{
        arbiter::{Arbiter, Publisher, TRADE_DEDUP_WINDOW},
        feeds::ArbitrationMode,
    },
    model::{BookAction, BookChange, BookKey, BookReplay, BookSide, FeedMessage, NormalizedBook},
};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

const VENUE: &str = "HYPERLIQUID";
const CHANNEL: u32 = 1;
const INSTRUMENT: u32 = 7;
/// A second market on the **same channel**, for a scenario that has to show one market's traffic
/// leaving another's alone.
const INSTRUMENT_B: u32 = 9;
const CATEGORY: &str = "perp";
const DEDUP_WINDOW_NS: u64 = 1_000;

/// One order's resting state: which side, at what price, in what quantity.
type Book = BTreeMap<u64, (BookSide, f64, f64)>;

fn arm(n: u8) -> Publisher {
    Publisher::Edge(IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)))
}

fn change(order_id: u64, side: BookSide, price: f64, size: f64) -> BookChange {
    BookChange {
        action: if size == 0.0 {
            BookAction::Delete
        } else {
            BookAction::Update
        },
        side,
        price,
        size,
        order_id,
    }
}

fn clear_both() -> BookChange {
    BookChange {
        action: BookAction::Clear,
        side: BookSide::Both,
        price: 0.0,
        size: 0.0,
        order_id: 0,
    }
}

fn market(instrument_id: u32) -> BookKey {
    (VENUE.into(), CATEGORY.into(), CHANNEL, instrument_id)
}

/// A market's wire symbol. Not part of [`BookKey`], but two markets sharing one must not be how a
/// scenario distinguishes them, so each carries its own.
fn symbol_of(instrument_id: u32) -> &'static str {
    match instrument_id {
        INSTRUMENT => "BTC",
        INSTRUMENT_B => "ETH",
        other => panic!("no symbol for instrument {other}"),
    }
}

/// The venue's own stamp and our arrival stamp are **different quantities** and every helper here
/// takes them apart: the real publishers stamp identical `source_ts_ns` on an event they both saw,
/// and a lagging arm is late in `recv_ts_ns` alone.
fn batch(changes: Vec<BookChange>, source_ts_ns: u64, recv_ts_ns: u64) -> FeedMessage {
    batch_for(INSTRUMENT, changes, source_ts_ns, recv_ts_ns)
}

fn batch_for(
    instrument_id: u32,
    changes: Vec<BookChange>,
    source_ts_ns: u64,
    recv_ts_ns: u64,
) -> FeedMessage {
    FeedMessage::Book(NormalizedBook {
        venue: VENUE.into(),
        source: VENUE.into(),
        source_id: 1,
        symbol: symbol_of(instrument_id).into(),
        channel: CHANNEL,
        instrument_id,
        category: CATEGORY.into(),
        changes,
        snapshot: false,
        last: true,
        source_ts_ns,
        recv_ts_ns,
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    })
}

/// A publisher's whole book as the `Clear`-led re-baseline it emits after a snapshot install.
fn snapshot(book: &Book, source_ts_ns: u64, recv_ts_ns: u64) -> FeedMessage {
    snapshot_for(INSTRUMENT, book, source_ts_ns, recv_ts_ns)
}

fn snapshot_for(
    instrument_id: u32,
    book: &Book,
    source_ts_ns: u64,
    recv_ts_ns: u64,
) -> FeedMessage {
    let mut changes = vec![clear_both()];
    changes.extend(
        book.iter()
            .map(|(&id, &(side, px, sz))| change(id, side, px, sz)),
    );
    batch_for(instrument_id, changes, source_ts_ns, recv_ts_ns)
}

/// Exactly what PROTOCOL.md tells a `book` consumer to do: a `Clear` re-baselines the side(s) it
/// names, and every other change sets the order's absolute resulting size, removing it at zero.
fn apply(book: &mut Book, b: &NormalizedBook) {
    for c in &b.changes {
        match c.action {
            BookAction::Clear if c.side == BookSide::Both => book.clear(),
            BookAction::Clear => book.retain(|_, v| v.0 != c.side),
            _ if c.size == 0.0 => {
                book.remove(&c.order_id);
            }
            _ => {
                book.insert(c.order_id, (c.side, c.price, c.size));
            }
        }
    }
}

/// Drive the whole scenario, returning the consumer's book and the venue's for comparison.
#[test]
fn a_naive_consumers_book_matches_the_venue_across_gaps_and_races() {
    let (tx, mut rx) = broadcast::channel(4096);
    let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
    a.set_mode(VENUE, ArbitrationMode::Coordinated);
    a.set_book_dedup_window(DEDUP_WINDOW_NS);
    a.set_book_replay(Arc::new(Mutex::new(BookReplay::default())));
    let market = market(INSTRUMENT);
    let (fast, slow) = (arm(1), arm(2));
    a.set_book_synced(&market, fast, true);
    a.set_book_synced(&market, slow, true);

    let mut venue = Book::new();
    let mut consumer = Book::new();
    let mut drain = |consumer: &mut Book| {
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Book(b) = &*m {
                apply(consumer, b);
            }
        }
    };

    // Both publishers install their books. The first through is published; the peer's copy is a
    // re-baseline for a market someone is already serving, so it is dropped.
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001, 1_001), slow, CATEGORY);

    // Ordinary flow, every event mirrored by both publishers.
    let events = [
        (1u64, BookSide::Bid, 100.0, 5.0),
        (2, BookSide::Bid, 99.0, 3.0),
        (3, BookSide::Ask, 101.0, 7.0),
        (4, BookSide::Ask, 102.0, 1.0),
    ];
    for (i, &(id, side, px, sz)) in events.iter().enumerate() {
        let t = 2_000 + i as u64 * 10;
        venue.insert(id, (side, px, sz));
        a.emit(batch(vec![change(id, side, px, sz)], t, t), fast, CATEGORY);
        a.emit(
            batch(vec![change(id, side, px, sz)], t + 1, t + 1),
            slow,
            CATEGORY,
        );
    }

    // A partial fill both publishers see: order 1 goes from 5 to 2. Same id, same action, same
    // resting price — only the quantity differs, so collapsing it as a duplicate would leave the
    // consumer holding a quantity the venue has already reduced.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 2_100, 2_100),
        fast,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 2_101, 2_101),
        slow,
        CATEGORY,
    );

    // The slow publisher re-sends its copy of an earlier event long past the dedup window. It is a
    // redundant emission at worst: the order's absolute state has not moved since.
    a.emit(
        batch(
            vec![change(3, BookSide::Ask, 101.0, 7.0)],
            9_000_000,
            9_000_000,
        ),
        slow,
        CATEGORY,
    );

    // Order 2 is cancelled. Only the fast publisher's copy lands.
    venue.remove(&2);
    a.emit(
        batch(
            vec![change(2, BookSide::Bid, 99.0, 0.0)],
            9_000_100,
            9_000_100,
        ),
        fast,
        CATEGORY,
    );
    drain(&mut consumer);
    assert_eq!(consumer, venue, "steady-state racing must not drift");

    // The slow publisher gaps; its own recovery is still in flight.
    a.set_book_synced(&market, slow, false);

    // The fast publisher gaps and recovers too. With no peer serving, its re-baseline goes out — and
    // its snapshot does not contain the cancelled order 2.
    a.emit(snapshot(&venue, 9_000_200, 9_000_200), fast, CATEGORY);
    drain(&mut consumer);
    assert_eq!(
        consumer, venue,
        "a re-baseline replaces the consumer's book"
    );

    // The slow publisher's reconnect replays its backlog, carrying its first and only copy of the add
    // for order 2 — an order the venue killed while it was behind. Its own book legitimately still
    // holds it, so nothing upstream of the merge point can refuse this, and the re-baseline above must
    // not have wiped the record that says it is dead.
    a.emit(
        batch(
            vec![change(2, BookSide::Bid, 99.0, 3.0)],
            9_100_000,
            9_100_000,
        ),
        slow,
        CATEGORY,
    );
    drain(&mut consumer);
    assert_eq!(consumer, venue, "a dead order must not be resurrected");

    // The fast publisher's host is drained: its receiver exits, which is the moment its claim to be
    // serving this market ends.
    a.forget_publisher_books(VENUE, fast);

    // The venue moves on while the only remaining publisher is still recovering, so the consumer's
    // book is stale by the time that publisher resyncs.
    venue.insert(3, (BookSide::Ask, 101.0, 4.0));
    venue.remove(&4);
    a.emit(snapshot(&venue, 9_200_000, 9_200_000), slow, CATEGORY);
    drain(&mut consumer);
    assert_eq!(
        consumer, venue,
        "the surviving publisher's re-baseline must not be held hostage by a departed peer"
    );

    // And the survivor streams on from there.
    venue.insert(5, (BookSide::Bid, 98.0, 9.0));
    a.emit(
        batch(
            vec![change(5, BookSide::Bid, 98.0, 9.0)],
            9_300_000,
            9_300_000,
        ),
        slow,
        CATEGORY,
    );
    drain(&mut consumer);
    assert_eq!(consumer, venue);
}

/// Two publishers that disagree about a resting order have one drifted book between them, and the
/// merge point cannot tell which. Publishing either walks the consumer somewhere the venue never
/// went, so the market re-baselines — and the consumer still ends up holding the venue's book.
#[test]
fn a_drifted_publisher_cannot_walk_a_consumers_order_backwards() {
    let (tx, mut rx) = broadcast::channel(4096);
    let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
    a.set_mode(VENUE, ArbitrationMode::Coordinated);
    a.set_book_dedup_window(DEDUP_WINDOW_NS);
    a.set_book_replay(Arc::new(Mutex::new(BookReplay::default())));
    let market = market(INSTRUMENT);
    let (fast, drifted) = (arm(1), arm(2));
    a.set_book_synced(&market, fast, true);
    a.set_book_synced(&market, drifted, true);

    let mut venue = Book::new();
    let mut consumer = Book::new();

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);

    // The venue fills order 1 down to 2. The drifted publisher missed that fill and reports the
    // order still resting at 5.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 1_100, 1_100),
        fast,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_200, 1_200),
        drifted,
        CATEGORY,
    );

    // Whatever comes next re-baselines the market rather than resuming either arm's deltas.
    venue.insert(2, (BookSide::Ask, 101.0, 4.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 4.0)], 1_300, 1_300),
        fast,
        CATEGORY,
    );

    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            apply(&mut consumer, b);
        }
    }
    assert_eq!(consumer, venue);
}

/// Mirrors the arbiter's private `MAX_SEEN_ORDER_EVENTS`. A scenario that has to cross the guard's
/// cap has to name it; a wrong value here makes the test weaker, never wrong.
const GUARD_CAP: u64 = 1024;

/// Mirrors the arbiter's private `MAX_GUARDED_ORDERS`: the live resting-quantity floors, bounded
/// apart from the tombstones and to half of [`GUARD_CAP`].
const GUARDED_ORDERS: u64 = GUARD_CAP / 2;

/// Mirrors the arbiter's private `MAX_MARKET_TOMBSTONES`: how many removed orders one market's
/// resurrection guard holds before it can no longer answer.
const MARKET_TOMBSTONES: u64 = 65_536;

/// The wiring every scenario shares: one order-level market on a two-publisher venue, both arms
/// synced, racing exactly as the Market-by-Order processor drives it.
fn harness() -> (
    Arbiter,
    broadcast::Receiver<Arc<FeedMessage>>,
    Publisher,
    Publisher,
) {
    harness_over(&[INSTRUMENT])
}

/// The same wiring over several markets of one channel, for a scenario that has to show one
/// market's traffic reaching another's state.
fn harness_over(
    instruments: &[u32],
) -> (
    Arbiter,
    broadcast::Receiver<Arc<FeedMessage>>,
    Publisher,
    Publisher,
) {
    let (tx, rx) = broadcast::channel(4096);
    let mut a = Arbiter::new(tx, TRADE_DEDUP_WINDOW);
    a.set_mode(VENUE, ArbitrationMode::Coordinated);
    a.set_book_dedup_window(DEDUP_WINDOW_NS);
    a.set_book_replay(Arc::new(Mutex::new(BookReplay::default())));
    let (fast, slow) = (arm(1), arm(2));
    for &instrument_id in instruments {
        let key = market(instrument_id);
        a.set_book_synced(&key, fast, true);
        a.set_book_synced(&key, slow, true);
    }
    (a, rx, fast, slow)
}

fn drain_into(rx: &mut broadcast::Receiver<Arc<FeedMessage>>, consumer: &mut Book) {
    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            apply(consumer, b);
        }
    }
}

/// One consumer book per market, so a scenario over several markets of a channel reads each one's
/// state apart. Keyed by `instrument_id` — a market-by-price `symbol` collides, which is why
/// [`BookKey`] does not carry one.
fn drain_markets(
    rx: &mut broadcast::Receiver<Arc<FeedMessage>>,
    markets: &mut BTreeMap<u32, Book>,
) {
    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            apply(markets.entry(b.instrument_id).or_default(), b);
        }
    }
}

/// Two markets on one channel, carrying **colliding order ids**: what one market's stream does to
/// an order must not reach the other's. Every state the merge point keeps is per market, and one
/// kept per channel would cross them.
#[test]
fn one_markets_stream_leaves_another_on_the_same_channel_alone() {
    let (mut a, mut rx, fast, slow) = harness_over(&[INSTRUMENT, INSTRUMENT_B]);
    let (mut venue_a, mut venue_b) = (Book::new(), Book::new());
    let mut markets: BTreeMap<u32, Book> = BTreeMap::new();

    // Market A installs a book holding order 1; market B installs an empty one.
    venue_a.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(
        snapshot_for(INSTRUMENT, &venue_a, 1_000, 1_000),
        fast,
        CATEGORY,
    );
    a.emit(
        snapshot_for(INSTRUMENT_B, &venue_b, 1_000, 1_000),
        fast,
        CATEGORY,
    );

    // Market A's order 1 is cancelled.
    venue_a.remove(&1);
    a.emit(
        batch_for(
            INSTRUMENT,
            vec![change(1, BookSide::Bid, 100.0, 0.0)],
            1_100,
            1_100,
        ),
        fast,
        CATEGORY,
    );

    // Market B opens an order under the **same id**. It is live and has nothing to do with A's dead
    // one, so it has to reach the consumer.
    venue_b.insert(1, (BookSide::Ask, 50.0, 2.0));
    a.emit(
        batch_for(
            INSTRUMENT_B,
            vec![change(1, BookSide::Ask, 50.0, 2.0)],
            1_200,
            1_200,
        ),
        fast,
        CATEGORY,
    );

    // And the lagging arm's first and only copy of A's add for that id, which is dead there and
    // must not come back on either market.
    a.emit(
        batch_for(
            INSTRUMENT,
            vec![change(1, BookSide::Bid, 100.0, 5.0)],
            1_300,
            1_300,
        ),
        slow,
        CATEGORY,
    );

    drain_markets(&mut rx, &mut markets);
    assert_eq!(markets.remove(&INSTRUMENT).unwrap_or_default(), venue_a);
    assert_eq!(markets.remove(&INSTRUMENT_B).unwrap_or_default(), venue_b);
}

/// A forced re-baseline republishes the view no single arm owns. If the arm whose batch discharged
/// the flag is stamped as owning the floors it seeds, that arm is exempt from the size gate and
/// simply repeats the stale claim the re-baseline was called to correct.
#[test]
fn a_discharged_rebaseline_does_not_let_the_raising_arm_repeat_its_claim() {
    let (mut a, mut rx, fast, drifted) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);

    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 1_100, 1_100),
        fast,
        CATEGORY,
    );
    // The drifted arm missed the fill, claims 5, and is withheld — then discharges the flag with an
    // unrelated event and repeats the claim.
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_200, 1_200),
        drifted,
        CATEGORY,
    );
    venue.insert(2, (BookSide::Ask, 101.0, 4.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 4.0)], 1_300, 1_300),
        drifted,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_400, 1_400),
        drifted,
        CATEGORY,
    );

    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// A market with more live orders than the guard's cap must not spend the guard on them: nothing
/// re-seeds a tombstone, and losing one lets a lagging arm's only copy of the add resurrect an order
/// the venue removed.
#[test]
fn a_book_larger_than_the_guard_does_not_resurrect_a_removed_order() {
    let (mut a, mut rx, fast, slow) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    let dead = change(7, BookSide::Bid, 100.0, 6.0);
    a.emit(batch(vec![dead], 1_000, 1_000), fast, CATEGORY);
    a.emit(
        batch(vec![change(7, BookSide::Bid, 100.0, 0.0)], 1_100, 1_100),
        fast,
        CATEGORY,
    );

    // Well past the cap, and spaced well outside the dedup window: nothing here is a copy anyone is
    // still racing, so every one of these is free for the guard to forget.
    for i in 0..(GUARD_CAP + 64) {
        let id = 100 + i;
        venue.insert(id, (BookSide::Ask, 200.0 + i as f64, 1.0));
        a.emit(
            batch(
                vec![change(id, BookSide::Ask, 200.0 + i as f64, 1.0)],
                2_000 + i * 10_000,
                2_000 + i * 10_000,
            ),
            fast,
            CATEGORY,
        );
        drain_into(&mut rx, &mut consumer);
    }

    // The lagging arm's first and only copy of the add for the order that is already gone.
    a.emit(batch(vec![dead], 90_000_000, 90_000_000), slow, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// A recovery snapshot larger than the guard's cap seeds its own orders into it. Those are live
/// floors and re-seed themselves; the tombstones they would displace do not.
#[test]
fn a_rebaseline_larger_than_the_guard_does_not_resurrect_a_removed_order() {
    let (mut a, mut rx, fast, slow) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    let dead = change(7, BookSide::Bid, 100.0, 6.0);
    a.emit(batch(vec![dead], 1_000, 1_000), fast, CATEGORY);
    a.emit(
        batch(vec![change(7, BookSide::Bid, 100.0, 0.0)], 1_100, 1_100),
        fast,
        CATEGORY,
    );

    // The fast arm gaps and recovers with a book far larger than the cap, containing no order 7.
    for i in 0..(GUARD_CAP + 77) {
        let id = 100 + i;
        venue.insert(id, (BookSide::Ask, 200.0 + i as f64, 1.0));
    }
    a.emit(snapshot(&venue, 1_200, 1_200), fast, CATEGORY);
    drain_into(&mut rx, &mut consumer);

    // Twice, either side of a fast-arm event: a guard the seeding spent would withhold the first
    // copy behind a forced re-baseline and then publish the second onto the market it just healed.
    a.emit(batch(vec![dead], 90_000_000, 90_000_000), slow, CATEGORY);
    venue.insert(9, (BookSide::Bid, 50.0, 2.0));
    a.emit(
        batch(
            vec![change(9, BookSide::Bid, 50.0, 2.0)],
            91_000_000,
            91_000_000,
        ),
        fast,
        CATEGORY,
    );
    a.emit(batch(vec![dead], 92_000_000, 92_000_000), slow, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// Past the guard's cap the merge point can no longer tell a lagging arm's stale `Add` from a live
/// one, and **our own accumulated view is not an answer to that** — it is what the guard was
/// protecting. Republishing it hands the consumer whatever resurrections got in, stamped as a
/// complete book, and re-seeds them as live orders nothing will remove again. The market must go dark
/// instead, and the removals that reached the wire on the way must still have reached it.
#[test]
fn an_unanswerable_guard_does_not_republish_our_own_view() {
    let (mut a, mut rx, fast, slow) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    // Both arms install their books. The peer's copy is dropped as a re-baseline for a market someone
    // is already serving, which is still the moment it starts counting as serving — and from here it
    // reports none of the removals below, so every tombstone stays open for a copy it has yet to send.
    venue.insert(1, (BookSide::Bid, 90.0, 1.0));
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001, 1_001), slow, CATEGORY);

    // The order the lagging arm is still holding an add for, killed by the fast arm before it starts.
    let stale_add = change(7, BookSide::Bid, 100.0, 6.0);
    a.emit(batch(vec![stale_add], 2_000, 2_000), fast, CATEGORY);
    a.emit(
        batch(vec![change(7, BookSide::Bid, 100.0, 0.0)], 2_100, 2_100),
        fast,
        CATEGORY,
    );

    // One order past the cap. Chunked only so the scenario is a few dozen batches rather than 131,072
    // of them; what matters is that the slow arm reports none of these removals, so none of the
    // tombstones can be retired and the cap is what gives way.
    // Every `Clear`-led batch's size: a disowning is a bare one, and republishing our own view — the
    // behaviour this replaced — is a whole book.
    fn drain(
        rx: &mut broadcast::Receiver<Arc<FeedMessage>>,
        consumer: &mut Book,
        clears: &mut Vec<usize>,
    ) {
        while let Ok(m) = rx.try_recv() {
            if let FeedMessage::Book(b) = &*m {
                if b.changes
                    .first()
                    .is_some_and(|c| c.action == BookAction::Clear)
                {
                    clears.push(b.changes.len());
                }
                apply(consumer, b);
            }
        }
    }
    // The producers' own install re-baselines, drained and discarded: what follows must be the only
    // `Clear` the market sees.
    let mut clears: Vec<usize> = Vec::new();
    drain(&mut rx, &mut consumer, &mut clears);
    clears.clear();

    const CHUNK: u64 = 2_048;
    for c in 0..=(MARKET_TOMBSTONES / CHUNK) {
        let (base, at) = (100 + c * CHUNK, 10_000 + c * 10_000);
        let ids = base..base + CHUNK;
        let adds = ids
            .clone()
            .map(|id| change(id, BookSide::Ask, 200.0, 1.0))
            .collect();
        let deletes = ids
            .map(|id| change(id, BookSide::Ask, 200.0, 0.0))
            .collect();
        a.emit(batch(adds, at, at), fast, CATEGORY);
        a.emit(batch(deletes, at + 1_000, at + 1_000), fast, CATEGORY);
        // Up to the disowning the consumer tracks the venue exactly: every removal that crossed the
        // guard while it could still answer reached the wire, including the one that broke it.
        let before = clears.is_empty();
        drain(&mut rx, &mut consumer, &mut clears);
        if before && clears.is_empty() {
            assert_eq!(
                consumer, venue,
                "a guard eviction must not swallow the removals that raised it"
            );
        }
    }
    assert_eq!(
        clears,
        vec![1],
        "disowning is a bare clear, told exactly once"
    );
    assert!(consumer.is_empty(), "the consumer is told to drop the book");

    // The lagging arm's first and only copy of the add for the order that is already gone, and the
    // fast arm streaming on. The market is disowned, so neither reaches the consumer.
    a.emit(
        batch(vec![stale_add], 90_000_000, 90_000_000),
        slow,
        CATEGORY,
    );
    a.emit(
        batch(
            vec![change(2, BookSide::Bid, 91.0, 1.0)],
            90_100_000,
            90_100_000,
        ),
        fast,
        CATEGORY,
    );
    drain(&mut rx, &mut consumer, &mut clears);
    assert!(consumer.is_empty(), "and stays dark, holding no dead order");

    // A producer's own snapshot is what ends the outage, and it is the venue's book, not ours.
    venue.insert(3, (BookSide::Ask, 105.0, 2.0));
    a.emit(snapshot(&venue, 91_000_000, 91_000_000), fast, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(
        consumer, venue,
        "a producer re-baseline must re-establish it"
    );
}

/// One venue event every [`SPACING_NS`], whatever an order's lifecycle costs. A lag of L therefore
/// leaves `L / SPACING_NS` events inside the guard's own [`GUARD_CAP`] count bound, and
/// `0.875 * L / (SPACING_NS * events_per_order)` removals inside its tombstone population.
const SPACING_NS: u64 = 1_120_000; // ~890 events/s, the flagship market's measured change rate

/// How far the slow arm trails the leader, in the two clocks that move independently.
#[derive(Clone, Copy)]
struct Lag {
    /// The datagram reaches us late. This is the lag the wire actually shows: on 271,455 paired
    /// events the two publishers stamp **identical** venue times, so a real lagging arm is late
    /// here and nowhere else.
    arrival_ns: u64,
    /// The arm's own `source_ts_ns` reads older than the leader's for the same event. A separate
    /// quantity from arrival, and none of the scenarios below use it — it exists so a phase
    /// keying on venue time can drive the two apart.
    venue_ns: u64,
}

impl Lag {
    fn arrival(ns: u64) -> Self {
        Self {
            arrival_ns: ns,
            venue_ns: 0,
        }
    }
}

/// Drive `pairs` orders through both arms with the slow one behind by `lag` in **arrival order**,
/// not merely in its timestamps, and return the consumer's book beside the venue's. Every eighth
/// order is never removed, so the end state is a book rather than an empty map.
///
/// `partial_fills` inserts a size-decreasing step in each order's life (add 1.0 → fill 0.5 → remove).
/// Without it every order is only ever seen at `1.0` and then `0.0`, so a stale copy arriving past the
/// dedup window either hits a tombstone (refused) or matches the floor exactly (idempotent) — the
/// replay **structurally cannot produce a `Disagreement`**, which is the mechanism behind the real
/// capture's divergence, and a regression at any lag would pass. With it, an order's lifecycle costs
/// three events rather than two, which is what a caller measuring the tombstone population has to
/// divide by.
fn replay_with_lag(pairs: u64, lag: Lag, partial_fills: bool) -> (Book, Book) {
    let (mut a, mut rx, fast, slow) = harness();
    a.set_book_dedup_window(1_000_000_000); // the shipped --arb-book-dedup-window-ms
    let (mut venue, mut consumer) = (Book::new(), Book::new());
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001, 1_001), slow, CATEGORY);

    let per_order = if partial_fills { 3 } else { 2 };
    // (arrival time, the arm's own venue stamp, is the fast arm, the change).
    let mut arrivals: Vec<(u64, u64, bool, BookChange)> = Vec::new();
    for i in 0..pairs {
        let (id, at) = (100 + i, 10_000 + i * per_order * SPACING_NS);
        let px = 200.0 + (i % 50) as f64;
        let resting = if partial_fills { 0.5 } else { 1.0 };
        let mut events = vec![(at, change(id, BookSide::Ask, px, 1.0))];
        if partial_fills {
            events.push((at + SPACING_NS, change(id, BookSide::Ask, px, 0.5)));
        }
        if i % 8 == 0 {
            venue.insert(id, (BookSide::Ask, px, resting));
        } else {
            events.push((
                at + (per_order - 1) * SPACING_NS,
                change(id, BookSide::Ask, px, 0.0),
            ));
        }
        for (t, c) in events {
            arrivals.push((t, t, true, c));
            arrivals.push((t + lag.arrival_ns, t.saturating_sub(lag.venue_ns), false, c));
        }
    }
    arrivals.sort_by_key(|&(at, _, is_fast, _)| (at, !is_fast));
    for (at, venue_ts, is_fast, c) in arrivals {
        let arm = if is_fast { fast } else { slow };
        a.emit(batch(vec![c], venue_ts, at), arm, CATEGORY);
        drain_into(&mut rx, &mut consumer);
    }
    (consumer, venue)
}

/// A lagging arm holds every tombstone the leader makes open until it catches up, so the population the
/// guard has to hold is the removals inside that lag — thousands on a busy market, where the per-market
/// count it used to be bounded by was 512. Crossing that count must no longer cost the market anything.
///
/// Orders that are added and cancelled without ever trading, deliberately: a lag wide enough to hold
/// more than 512 removals is necessarily wider than [`GUARD_CAP`] events, and past that a *filled*
/// order's stale add reads as a size disagreement rather than as a duplicate — which is the count cap
/// binding, a different limit measured by the sweep below. This one is about the tombstones.
#[test]
fn a_lagging_arm_past_the_old_per_market_cap_costs_the_market_nothing() {
    // 300ms of lag at two events per order: ~1,300 removals in flight, against an old cap of 512.
    let lag = Lag::arrival(300 * SPACING_NS * 10);
    let (consumer, venue) = replay_with_lag(GUARDED_ORDERS * 8, lag, false);
    assert_eq!(consumer, venue);
}

/// The lag sweep: how far the two publishers can drift apart before the merge point stops being able
/// to keep a consumer's book identical to the venue's.
///
/// **Before this guard was sized by the arms' lag rather than by a per-market count of 512, that
/// figure was 150 ms**, reproducing the same cliff a real two-publisher capture showed at 153 ms,
/// where the consumer ended 994 orders wrong and never self-healed. It now holds to **1 s**, the last
/// step below — twice the 500 ms at which that capture first diverges, and five times the widest
/// separation those publishers ever showed.
///
/// What sets the ceiling is the guard's [`GUARD_CAP`] **count** bound, not the 1 s dedup window: past
/// 1,024 events in flight (1.15 s at this rate) an arm's copy of an add for an order the leader has
/// already partially filled is no longer recognized as a duplicate — it reads as a second publisher
/// claiming a larger resting size, a false `dz_mbo_arm_disagreement_total`, and the batches withheld
/// behind the re-baseline it forces are lost rather than delayed. Measured: exact at 1 s, 223 orders
/// wrong at 1.2 s. `docs/metrics.md` states that ceiling as `min(flag, 1024 / event_rate)`; this is it.
/// It is also why raising the window alone buys nothing here, and why [`SPACING_NS`] has to be the
/// real market's rate for the figure to mean anything.
#[test]
fn the_consumer_book_matches_the_venue_up_to_a_one_second_inter_arm_lag() {
    for lag_ms in [10u64, 50, 100, 200, 300, 500, 800, 1_000] {
        let pairs = (lag_ms * 1_000_000 / SPACING_NS).max(GUARDED_ORDERS) * 2;
        let (consumer, venue) = replay_with_lag(pairs, Lag::arrival(lag_ms * 1_000_000), true);
        assert_eq!(consumer, venue, "diverged at {lag_ms}ms of inter-arm lag");
    }
}

/// `forced` is one-shot and first-cause-wins, so the reason it carries does not say what the batch
/// contained. One batch can both remove an order and claim a drifted size for another — and dropping it
/// strands the removal exactly as a guard eviction did, whatever label the flag ended up with.
#[test]
fn a_batch_that_both_removes_and_disagrees_does_not_strand_the_removal() {
    let (mut a, mut rx, fast, drifted) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    venue.insert(2, (BookSide::Bid, 99.0, 3.0));
    a.emit(snapshot(&venue, 1_000, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001, 1_001), drifted, CATEGORY);

    // Order 1 is filled down to 2. The drifted arm misses that fill.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 1_100, 1_100),
        fast,
        CATEGORY,
    );

    // Its next batch cancels order 2 — its own first and only copy of that removal — and in the same
    // logical event repeats the stale size for order 1.
    venue.remove(&2);
    a.emit(
        batch(
            vec![
                change(2, BookSide::Bid, 99.0, 0.0),
                change(1, BookSide::Bid, 100.0, 5.0),
            ],
            1_200,
            1_200,
        ),
        drifted,
        CATEGORY,
    );

    // An unrelated event discharges the re-baseline the disagreement forced.
    venue.insert(3, (BookSide::Ask, 101.0, 4.0));
    a.emit(
        batch(vec![change(3, BookSide::Ask, 101.0, 4.0)], 1_300, 1_300),
        fast,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

// ---------------------------------------------------------------------------------------------
// The consumer-visible contract, at behavioural altitude. Nothing below reaches into the arbiter's
// internals: each scenario drives batches from N arms through the real `Arbiter`, reads what
// reaches the broadcast, rebuilds a book exactly as PROTOCOL.md tells a consumer to, and compares
// it to the venue's. That is what makes them survive a replacement of the mechanism underneath.
// ---------------------------------------------------------------------------------------------

/// One venue event: an order's id, side, resting price and **absolute resulting** quantity, with
/// zero meaning it left the book.
type Event = (u64, BookSide, f64, f64);

/// One market's life as the wire delivers it — orders arrive, are partially filled, and leave.
const LIFECYCLE: [Event; 8] = [
    (1, BookSide::Bid, 100.0, 5.0),
    (2, BookSide::Ask, 101.0, 7.0),
    (1, BookSide::Bid, 100.0, 2.0),
    (3, BookSide::Bid, 99.0, 4.0),
    (2, BookSide::Ask, 101.0, 0.0),
    (3, BookSide::Bid, 99.0, 1.0),
    (1, BookSide::Bid, 100.0, 0.0),
    (4, BookSide::Ask, 102.0, 6.0),
];

/// `rounds` copies of [`LIFECYCLE`], each on its own order ids and prices, so a scenario can run
/// long enough for a lagging arm to stay behind for the whole of it.
fn lifecycle_stream(rounds: u64) -> Vec<Event> {
    (0..rounds)
        .flat_map(|r| {
            LIFECYCLE
                .iter()
                .map(move |&(id, side, px, sz)| (id + r * 10, side, px + r as f64, sz))
        })
        .collect()
}

fn venue_apply(venue: &mut Book, (id, side, px, sz): Event) {
    if sz == 0.0 {
        venue.remove(&id);
    } else {
        venue.insert(id, (side, px, sz));
    }
}

fn ev_change((id, side, px, sz): Event) -> BookChange {
    change(id, side, px, sz)
}

/// Drain into the consumer's book **and** keep what was published, so a scenario can assert what
/// reached the wire rather than only where the consumer ended up.
fn drain_published(
    rx: &mut broadcast::Receiver<Arc<FeedMessage>>,
    consumer: &mut Book,
) -> Vec<NormalizedBook> {
    let mut out = Vec::new();
    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            apply(consumer, b);
            out.push(b.clone());
        }
    }
    out
}

/// The two clocks reach the wire as the harness set them. Nothing on the order-level path reads
/// `source_ts_ns` today, so without this the whole split is unfalsifiable: the arguments could be
/// swapped at every call site and every scenario below would still pass, which is exactly how a
/// venue-time-keyed test comes to measure nothing.
#[test]
fn a_published_batch_carries_the_stamps_it_was_given() {
    let (mut a, mut rx, only, _peer) = harness();
    let mut consumer = Book::new();
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 4_000, 7_000),
        only,
        CATEGORY,
    );
    let published = drain_published(&mut rx, &mut consumer);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].source_ts_ns, 4_000, "venue time");
    assert_eq!(published[0].recv_ts_ns, 7_000, "arrival time");
}

/// Venue-time skew on its own: the trailing arm stamps `source_ts_ns` 5 ms older than the leader's
/// for the same event and arrives in lockstep with it. The order-level path reads only arrival
/// today, so this changes nothing — which is the before-picture a design keying on venue time is
/// measured against, and the one caller that drives [`Lag::venue_ns`].
#[test]
fn a_venue_time_skew_alone_does_not_drift_the_consumer() {
    let skew = Lag {
        arrival_ns: 0,
        venue_ns: 5_000_000,
    };
    let (_a, _rx, consumer, venue) =
        arrival_lagged_stream(&lifecycle_stream(1), 1_000_000, skew, true);
    assert_eq!(consumer, venue);
}

/// A single publisher streaming a market's whole life. Nothing races, so the consumer's book has to
/// equal the venue's after **every** event, not merely at the end.
#[test]
fn a_single_arms_stream_reaches_the_consumer_exactly() {
    let (mut a, mut rx, only, peer) = harness();
    a.forget_publisher_books(VENUE, peer);
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    for (i, &e) in lifecycle_stream(1).iter().enumerate() {
        let t = 1_000 + i as u64 * 100;
        venue_apply(&mut venue, e);
        a.emit(batch(vec![ev_change(e)], t, t), only, CATEGORY);
        drain_into(&mut rx, &mut consumer);
        assert_eq!(consumer, venue, "diverged at event {i}");
    }
}

/// Both arms deliver every event, a nanosecond apart. The consumer sees each event **once**: a
/// second copy carries the order's absolute quantity again, so republishing it after the wire has
/// moved on walks the consumer back to a size the venue already reduced.
#[test]
fn two_arms_in_lockstep_publish_each_event_once() {
    let (mut a, mut rx, one, two) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());
    let (mut published, mut expected) = (Vec::new(), Vec::new());

    for (i, &e) in lifecycle_stream(1).iter().enumerate() {
        let t = 1_000 + i as u64 * 100;
        venue_apply(&mut venue, e);
        expected.push(ev_change(e));
        a.emit(batch(vec![ev_change(e)], t, t), one, CATEGORY);
        a.emit(batch(vec![ev_change(e)], t, t + 1), two, CATEGORY);
        published.extend(drain_published(&mut rx, &mut consumer));
    }

    let changes: Vec<BookChange> = published.iter().flat_map(|b| b.changes.clone()).collect();
    assert_eq!(changes, expected, "each venue event reaches the wire once");
    assert_eq!(consumer, venue);
}

/// Drive `events` through two arms, the trailing one `lag_ns` late **in arrival only** — its
/// `source_ts_ns` is the leader's, which is what the wire shows on 271,455 paired events. One venue
/// event every `spacing_ns`, so a lag wider than the spacing leaves the trailer permanently behind.
///
/// The invariant is asserted **after every arrival**, not at the end. A trailer replaying the venue's
/// whole life in order converges on its own: a resurrection or a stale size it publishes is corrected
/// by its own next copy, so a terminal comparison alone passes even with the racing guard removed
/// outright. Step by step it does not — under correct behaviour every trailer copy is collapsed and
/// publishes nothing, so the consumer never leaves the venue's state as of the last leader arrival.
fn arrival_lagged_stream(
    events: &[Event],
    spacing_ns: u64,
    lag: Lag,
    leader_is_first_arm: bool,
) -> (Arbiter, broadcast::Receiver<Arc<FeedMessage>>, Book, Book) {
    let (mut a, mut rx, one, two) = harness();
    a.set_book_dedup_window(1_000_000_000); // the shipped --arb-book-dedup-window-ms
    let (leader, trailer) = if leader_is_first_arm {
        (one, two)
    } else {
        (two, one)
    };
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    // (arrival, the venue's own stamp — identical on both arms' copies, arm, change, and the venue
    // event the leader's copy advances; `None` on the trailer's, which advances nothing).
    let mut arrivals: Vec<(u64, u64, Publisher, BookChange, Option<Event>)> = Vec::new();
    for (i, &e) in events.iter().enumerate() {
        let t = 1_000 + i as u64 * spacing_ns;
        arrivals.push((t, t, leader, ev_change(e), Some(e)));
        arrivals.push((
            t + lag.arrival_ns,
            t.saturating_sub(lag.venue_ns),
            trailer,
            ev_change(e),
            None,
        ));
    }
    arrivals.sort_by_key(|&(at, _, p, ..)| (at, p != leader)); // the leader's copy wins a tie
    for (at, venue_ts, p, c, applied) in arrivals {
        if let Some(e) = applied {
            venue_apply(&mut venue, e);
        }
        a.emit(batch(vec![c], venue_ts, at), p, CATEGORY);
        drain_into(&mut rx, &mut consumer);
        assert_eq!(consumer, venue, "diverged at arrival {at}");
    }
    (a, rx, consumer, venue)
}

/// The trailing arm is late in arrival and stamps the leader's venue times. Whichever arm leads,
/// the consumer ends holding the venue's book.
#[test]
fn an_arm_behind_in_arrival_only_does_not_drift_the_consumer() {
    let lag = Lag::arrival(5_000_000);
    for leader_is_first_arm in [true, false] {
        let (_a, _rx, consumer, venue) =
            arrival_lagged_stream(&lifecycle_stream(1), 1_000_000, lag, leader_is_first_arm);
        assert_eq!(consumer, venue, "leader_is_first_arm={leader_is_first_arm}");
    }
}

/// Two arms recovered from **different snapshot anchors**: the first holds an order the venue
/// killed before the second's newer snapshot was taken. The arm that holds it is the only one
/// that can remove it, and the market must go on being served either way.
#[test]
fn arms_synced_from_different_snapshot_anchors_keep_the_consumer_exact() {
    let (mut a, mut rx, early, late) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    // The early arm's anchor, taken while order 2 still rested.
    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    venue.insert(2, (BookSide::Ask, 101.0, 7.0));
    a.emit(snapshot(&venue, 1_000, 1_000), early, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);

    // The venue removes order 2; the late arm's anchor is taken after that, so it never held it and
    // can never report its removal. Its re-baseline is dropped — the early arm is serving.
    let served = venue.clone();
    venue.remove(&2);
    a.emit(snapshot(&venue, 2_000, 2_000), late, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(
        consumer, served,
        "a peer's re-baseline must not displace a served book"
    );

    // Only the arm that held order 2 reports its removal, and it has to reach the consumer.
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 0.0)], 3_000, 3_000),
        early,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue, "the holder's removal must reach the wire");

    // And the market keeps being served, from either arm.
    venue.insert(3, (BookSide::Bid, 98.0, 4.0));
    a.emit(
        batch(vec![change(3, BookSide::Bid, 98.0, 4.0)], 4_000, 4_000),
        late,
        CATEGORY,
    );
    venue.insert(4, (BookSide::Ask, 103.0, 2.0));
    a.emit(
        batch(vec![change(4, BookSide::Ask, 103.0, 2.0)], 5_000, 5_000),
        early,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// The arm that is **serving** a market leaves, and the peer recovering behind it has to be able to
/// re-baseline the consumer. Then it comes back, and neither its stale anchor nor its first deltas
/// may walk that consumer back.
///
/// Departure is driven by [`Arbiter::forget_publisher_books`], the signal a receiver's registration
/// sends as it exits, because that is the one an integration test can produce: the wall-clock
/// `PEER_SERVING_NS` backstop is measured on a monotonic clock this side of the API, so "silent for
/// 30 s" is unreachable without a clock seam in `src/`. Same consumer-visible property either way,
/// and the departure is load-bearing here — without it the peer's re-baseline stays suppressed and
/// the consumer holds a book the venue has left, for the life of the process.
#[test]
fn an_arm_that_departs_and_returns_keeps_the_consumer_exact() {
    let (mut a, mut rx, staying, leaving) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    // The arm about to leave is the one serving the market.
    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000, 1_000), leaving, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);

    // The venue moves on. The peer gapped and recovered, and its re-baseline is dropped: the serving
    // arm is healthy, so a recovering peer must not wipe a book that is correct.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    venue.insert(2, (BookSide::Ask, 101.0, 3.0));
    a.emit(snapshot(&venue, 2_000, 2_000), staying, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(
        consumer,
        Book::from([(1, (BookSide::Bid, 100.0, 5.0))]),
        "a recovering peer must not displace a served book"
    );

    // The serving arm's receiver exits, ending its claim. Nobody is serving now, so the peer's next
    // re-baseline is the consumer's only route back to the venue's book.
    a.forget_publisher_books(VENUE, leaving);
    a.emit(snapshot(&venue, 2_100, 2_100), staying, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue, "a departure must release the suppression");

    // The venue moves on again while the departed arm is still away.
    venue.insert(2, (BookSide::Ask, 101.0, 1.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 1.0)], 2_200, 2_200),
        staying,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);

    // It comes back the only way a restarted receiver can: synced first, then the re-baseline its
    // snapshot install produced. That anchor is one the venue has already moved past, and it is
    // dropped rather than published — the arm that stayed is serving this market correctly now.
    a.set_book_synced(&market(INSTRUMENT), leaving, true);
    let anchor = Book::from([
        (1, (BookSide::Bid, 100.0, 2.0)),
        (2, (BookSide::Ask, 101.0, 3.0)),
    ]);
    a.emit(snapshot(&anchor, 3_000, 3_000), leaving, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(
        consumer, venue,
        "a returning arm must not displace the book"
    );

    // Its first deltas off that anchor are copies of events the serving arm has already published,
    // and they claim a size the venue has since reduced. Neither may walk the consumer back.
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 3.0)], 3_100, 3_100),
        leaving,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue, "a stale copy must not walk an order back");

    // And the market goes on being served, from both arms.
    venue.insert(3, (BookSide::Bid, 97.0, 8.0));
    a.emit(
        batch(vec![change(3, BookSide::Bid, 97.0, 8.0)], 4_000, 4_000),
        staying,
        CATEGORY,
    );
    venue.insert(4, (BookSide::Ask, 102.0, 6.0));
    a.emit(
        batch(vec![change(4, BookSide::Ask, 102.0, 6.0)], 4_100, 4_100),
        leaving,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// One arm trails the other for the **whole run** — 25 events behind, never catching up. It must
/// not drift the consumer, and it must not stop the market being served: both arms still reach
/// the wire afterwards.
#[test]
fn a_permanently_slower_arm_never_stops_the_market_being_served() {
    let events = lifecycle_stream(5);
    let (mut a, mut rx, mut consumer, mut venue) =
        arrival_lagged_stream(&events, 10_000_000, Lag::arrival(250_000_000), true);
    assert_eq!(consumer, venue, "a permanent lag must not drift the book");

    // Each arm in turn, on a fresh order: the market is still being served from both.
    let after = 1_000 + events.len() as u64 * 10_000_000 + 250_000_000;
    for (i, p) in [arm(1), arm(2)].into_iter().enumerate() {
        let (id, t) = (900 + i as u64, after + i as u64 * 10_000_000);
        venue.insert(id, (BookSide::Bid, 10.0, 1.0));
        a.emit(
            batch(vec![change(id, BookSide::Bid, 10.0, 1.0)], t, t),
            p,
            CATEGORY,
        );
        drain_into(&mut rx, &mut consumer);
        assert_eq!(consumer, venue, "market not served after the lagging run");
    }
}

/// While one arm is serving a market, a peer's `Clear`-led re-baseline is **dropped**, not
/// published: it would tell the consumer to discard a book that is correct and replace it with the
/// peer's, which on a recovering arm is older. Exactly one `Clear` reaches the wire.
#[test]
fn a_peers_rebaseline_does_not_displace_a_served_book() {
    let (mut a, mut rx, serving, peer) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());
    let mut published = Vec::new();

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000, 1_000), serving, CATEGORY);
    venue.insert(2, (BookSide::Ask, 101.0, 7.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 7.0)], 1_100, 1_100),
        serving,
        CATEGORY,
    );
    published.extend(drain_published(&mut rx, &mut consumer));
    assert_eq!(consumer, venue);

    // The peer recovers and offers its own whole book, which is a state the venue has left.
    let stale_book: Book = BTreeMap::from([(9, (BookSide::Bid, 50.0, 1.0))]);
    a.emit(snapshot(&stale_book, 1_200, 1_200), peer, CATEGORY);
    published.extend(drain_published(&mut rx, &mut consumer));
    assert_eq!(consumer, venue, "the served book must survive the peer");

    let clears = published
        .iter()
        .filter(|b| {
            b.changes
                .first()
                .is_some_and(|c| c.action == BookAction::Clear)
        })
        .count();
    assert_eq!(
        clears, 1,
        "only the serving arm's re-baseline reaches the wire"
    );
}
