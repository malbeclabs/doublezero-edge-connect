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

fn batch(changes: Vec<BookChange>, recv_ns: u64) -> FeedMessage {
    FeedMessage::Book(NormalizedBook {
        venue: VENUE.into(),
        source: VENUE.into(),
        source_id: 1,
        symbol: "BTC".into(),
        channel: CHANNEL,
        instrument_id: INSTRUMENT,
        category: CATEGORY.into(),
        changes,
        snapshot: false,
        last: true,
        source_ts_ns: recv_ns,
        recv_ts_ns: recv_ns,
        kernel_rx_ts_ns: 0,
        ws_send_ts_ns: 0,
    })
}

/// A publisher's whole book as the `Clear`-led re-baseline it emits after a snapshot install.
fn snapshot(book: &Book, recv_ns: u64) -> FeedMessage {
    let mut changes = vec![clear_both()];
    changes.extend(
        book.iter()
            .map(|(&id, &(side, px, sz))| change(id, side, px, sz)),
    );
    batch(changes, recv_ns)
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
    let market = (VENUE.into(), CATEGORY.into(), CHANNEL, INSTRUMENT);
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
    a.emit(snapshot(&venue, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001), slow, CATEGORY);

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
        a.emit(batch(vec![change(id, side, px, sz)], t), fast, CATEGORY);
        a.emit(batch(vec![change(id, side, px, sz)], t + 1), slow, CATEGORY);
    }

    // A partial fill both publishers see: order 1 goes from 5 to 2. Same id, same action, same
    // resting price — only the quantity differs, so collapsing it as a duplicate would leave the
    // consumer holding a quantity the venue has already reduced.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 2_100),
        fast,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 2_101),
        slow,
        CATEGORY,
    );

    // The slow publisher re-sends its copy of an earlier event long past the dedup window. It is a
    // redundant emission at worst: the order's absolute state has not moved since.
    a.emit(
        batch(vec![change(3, BookSide::Ask, 101.0, 7.0)], 9_000_000),
        slow,
        CATEGORY,
    );

    // Order 2 is cancelled. Only the fast publisher's copy lands.
    venue.remove(&2);
    a.emit(
        batch(vec![change(2, BookSide::Bid, 99.0, 0.0)], 9_000_100),
        fast,
        CATEGORY,
    );
    drain(&mut consumer);
    assert_eq!(consumer, venue, "steady-state racing must not drift");

    // The slow publisher gaps; its own recovery is still in flight.
    a.set_book_synced(&market, slow, false);

    // The fast publisher gaps and recovers too. With no peer serving, its re-baseline goes out — and
    // its snapshot does not contain the cancelled order 2.
    a.emit(snapshot(&venue, 9_000_200), fast, CATEGORY);
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
        batch(vec![change(2, BookSide::Bid, 99.0, 3.0)], 9_100_000),
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
    a.emit(snapshot(&venue, 9_200_000), slow, CATEGORY);
    drain(&mut consumer);
    assert_eq!(
        consumer, venue,
        "the surviving publisher's re-baseline must not be held hostage by a departed peer"
    );

    // And the survivor streams on from there.
    venue.insert(5, (BookSide::Bid, 98.0, 9.0));
    a.emit(
        batch(vec![change(5, BookSide::Bid, 98.0, 9.0)], 9_300_000),
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
    let market: BookKey = (VENUE.into(), CATEGORY.into(), CHANNEL, INSTRUMENT);
    let (fast, drifted) = (arm(1), arm(2));
    a.set_book_synced(&market, fast, true);
    a.set_book_synced(&market, drifted, true);

    let mut venue = Book::new();
    let mut consumer = Book::new();

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000), fast, CATEGORY);

    // The venue fills order 1 down to 2. The drifted publisher missed that fill and reports the
    // order still resting at 5.
    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 1_100),
        fast,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_200),
        drifted,
        CATEGORY,
    );

    // Whatever comes next re-baselines the market rather than resuming either arm's deltas.
    venue.insert(2, (BookSide::Ask, 101.0, 4.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 4.0)], 1_300),
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

/// Mirrors the arbiter's private `MAX_GUARDED_ORDERS`: the live floors and the tombstones are bounded
/// apart, each to half of [`GUARD_CAP`].
const GUARDED_ORDERS: u64 = GUARD_CAP / 2;

/// The wiring every scenario shares: one order-level market on a two-publisher venue, both arms
/// synced, racing exactly as the Market-by-Order processor drives it.
fn harness() -> (
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
    let market: BookKey = (VENUE.into(), CATEGORY.into(), CHANNEL, INSTRUMENT);
    let (fast, slow) = (arm(1), arm(2));
    a.set_book_synced(&market, fast, true);
    a.set_book_synced(&market, slow, true);
    (a, rx, fast, slow)
}

fn drain_into(rx: &mut broadcast::Receiver<Arc<FeedMessage>>, consumer: &mut Book) {
    while let Ok(m) = rx.try_recv() {
        if let FeedMessage::Book(b) = &*m {
            apply(consumer, b);
        }
    }
}

/// A forced re-baseline republishes the view no single arm owns. If the arm whose batch discharged
/// the flag is stamped as owning the floors it seeds, that arm is exempt from the size gate and
/// simply repeats the stale claim the re-baseline was called to correct.
#[test]
fn a_discharged_rebaseline_does_not_let_the_raising_arm_repeat_its_claim() {
    let (mut a, mut rx, fast, drifted) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    venue.insert(1, (BookSide::Bid, 100.0, 5.0));
    a.emit(snapshot(&venue, 1_000), fast, CATEGORY);

    venue.insert(1, (BookSide::Bid, 100.0, 2.0));
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 2.0)], 1_100),
        fast,
        CATEGORY,
    );
    // The drifted arm missed the fill, claims 5, and is withheld — then discharges the flag with an
    // unrelated event and repeats the claim.
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_200),
        drifted,
        CATEGORY,
    );
    venue.insert(2, (BookSide::Ask, 101.0, 4.0));
    a.emit(
        batch(vec![change(2, BookSide::Ask, 101.0, 4.0)], 1_300),
        drifted,
        CATEGORY,
    );
    a.emit(
        batch(vec![change(1, BookSide::Bid, 100.0, 5.0)], 1_400),
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
    a.emit(batch(vec![dead], 1_000), fast, CATEGORY);
    a.emit(
        batch(vec![change(7, BookSide::Bid, 100.0, 0.0)], 1_100),
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
            ),
            fast,
            CATEGORY,
        );
        drain_into(&mut rx, &mut consumer);
    }

    // The lagging arm's first and only copy of the add for the order that is already gone.
    a.emit(batch(vec![dead], 90_000_000), slow, CATEGORY);
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
    a.emit(batch(vec![dead], 1_000), fast, CATEGORY);
    a.emit(
        batch(vec![change(7, BookSide::Bid, 100.0, 0.0)], 1_100),
        fast,
        CATEGORY,
    );

    // The fast arm gaps and recovers with a book far larger than the cap, containing no order 7.
    for i in 0..(GUARD_CAP + 77) {
        let id = 100 + i;
        venue.insert(id, (BookSide::Ask, 200.0 + i as f64, 1.0));
    }
    a.emit(snapshot(&venue, 1_200), fast, CATEGORY);
    drain_into(&mut rx, &mut consumer);

    // Twice, either side of a fast-arm event: a guard the seeding spent would withhold the first
    // copy behind a forced re-baseline and then publish the second onto the market it just healed.
    a.emit(batch(vec![dead], 90_000_000), slow, CATEGORY);
    venue.insert(9, (BookSide::Bid, 50.0, 2.0));
    a.emit(
        batch(vec![change(9, BookSide::Bid, 50.0, 2.0)], 91_000_000),
        fast,
        CATEGORY,
    );
    a.emit(batch(vec![dead], 92_000_000), slow, CATEGORY);
    drain_into(&mut rx, &mut consumer);
    assert_eq!(consumer, venue);
}

/// The dead population's cap can only be crossed by a batch that carries a removal, so the batch that
/// raises a guard eviction is always the batch that killed an order. Discarding it leaves that order
/// resting in the consumer's book, the forced re-baseline republishes it as live, and the seed marks it
/// live in the guard too — nothing removes it again.
#[test]
fn a_guard_eviction_does_not_leak_the_removal_that_raised_it() {
    let (mut a, mut rx, fast, slow) = harness();
    let (mut venue, mut consumer) = (Book::new(), Book::new());

    // Both arms install their books. The peer's copy is dropped as a re-baseline for a market someone
    // is already serving, which is still the moment it starts counting as serving — and from here it
    // reports none of the removals below, so every tombstone stays open for a copy it has yet to send.
    venue.insert(1, (BookSide::Bid, 90.0, 1.0));
    a.emit(snapshot(&venue, 1_000), fast, CATEGORY);
    a.emit(snapshot(&venue, 1_001), slow, CATEGORY);

    // One order past the cap, each add and its delete in their own batch: the delete that crosses the
    // cap evicts the oldest tombstone, which no peer has reported and is therefore not spent.
    for i in 0..=GUARDED_ORDERS {
        let (id, at) = (100 + i, 10_000 + i * 10_000);
        a.emit(
            batch(vec![change(id, BookSide::Ask, 200.0, 1.0)], at),
            fast,
            CATEGORY,
        );
        a.emit(
            batch(vec![change(id, BookSide::Ask, 200.0, 0.0)], at + 1_000),
            fast,
            CATEGORY,
        );
        drain_into(&mut rx, &mut consumer);
    }
    assert_eq!(
        consumer, venue,
        "a guard eviction must not swallow the removal that raised it"
    );

    // And the re-baseline that eviction forced must not bless the leak: it republishes what reached
    // the wire, then re-seeds every order in it as a live floor.
    venue.insert(2, (BookSide::Bid, 91.0, 1.0));
    a.emit(
        batch(vec![change(2, BookSide::Bid, 91.0, 1.0)], 90_000_000),
        fast,
        CATEGORY,
    );
    drain_into(&mut rx, &mut consumer);
    assert_eq!(
        consumer, venue,
        "and the forced re-baseline must not revive it"
    );
}
