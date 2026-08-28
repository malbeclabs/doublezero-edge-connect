//! Our Hyperliquid-compatible output must deserialize into the types its consumer parses.
//!
//! For `l2Book`/`trades` these mirror NautilusTrader's
//! `crates/adapters/hyperliquid/src/websocket/messages.rs` at **v1.227.0**, the version pinned by the
//! reference consumer, and `common/enums.rs` for the side spelling. `l4Book` has no Nautilus reader,
//! so those types mirror the DoubleZero publisher's own (`malbeclabs/hyperliquid`,
//! `app/publisher/server/src/types/mod.rs` + `types/node_data.rs`) instead. They are a copy, so they
//! go stale silently: when bumping either reference, re-read those files and update these first.
//!
//! Every field here is required with no `serde(default)` upstream, which is the point — a field we
//! rename or retype fails our build instead of a trader's session. Nautilus sets no
//! `deny_unknown_fields`, so extra fields of ours are tolerated and are deliberately not pinned.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct WsLevelData {
    px: String,
    sz: String,
    n: u32,
}

#[derive(Debug, Deserialize)]
struct WsBookData {
    coin: String,
    levels: [Vec<WsLevelData>; 2],
    #[allow(dead_code)]
    time: u64,
}

/// Nautilus deserializes only `"B"` and `"A"`; the container's `rename_all` is overridden per variant.
#[derive(Debug, Deserialize, PartialEq, Eq)]
enum HyperliquidSide {
    #[serde(rename = "B")]
    Buy,
    #[serde(rename = "A")]
    Sell,
}

#[derive(Debug, Deserialize)]
struct WsTradeData {
    coin: String,
    side: HyperliquidSide,
    px: String,
    sz: String,
    #[allow(dead_code)]
    hash: String,
    #[allow(dead_code)]
    time: u64,
    tid: u64,
    #[allow(dead_code)]
    users: [String; 2],
}

/// The envelope is internally tagged on `channel`; `l2Book`'s `data` is an object and `trades`' is an
/// array. An unknown `channel` is a hard deserialization error upstream, so the tag must match exactly.
#[derive(Debug, Deserialize)]
#[serde(tag = "channel")]
enum HyperliquidWsMessage {
    #[serde(rename = "l2Book")]
    L2Book { data: WsBookData },
    #[serde(rename = "trades")]
    Trades { data: Vec<WsTradeData> },
}

/// A golden `l2Book` frame, byte-for-byte as the sink emits it (the unit test
/// `golden_l2book_frame_matches_the_committed_fixture` pins that), must parse as Nautilus parses it.
#[test]
fn a_golden_l2book_frame_parses_as_nautilus_parses_it() {
    let frame = include_str!("fixtures/hl_l2book_golden.json");
    let HyperliquidWsMessage::L2Book { data } =
        serde_json::from_str(frame.trim_end()).expect("must parse into Nautilus's shape")
    else {
        panic!("must dispatch as l2Book")
    };
    assert_eq!(data.coin, "BTC");
    assert_eq!(data.levels[0][0].px, "100.5");
    assert_eq!(data.levels[0][0].sz, "8");
    assert_eq!(data.levels[0][0].n, 2);
    assert_eq!(data.levels[1][0].px, "101");
}

/// `px`/`sz` must be JSON strings and `n`/`time` JSON numbers. A number where Nautilus wants a string
/// fails with `invalid type: floating point`, which is exactly the silent break this pins.
#[test]
fn numeric_prices_would_not_parse() {
    let bad = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":100.5,"sz":"8","n":2}],[]]}}"#;
    assert!(serde_json::from_str::<HyperliquidWsMessage>(bad).is_err());
}

/// The trade envelope: `data` is an array, `side` is `"B"`/`"A"`, `tid` is a number.
#[test]
fn a_trade_frame_parses_as_nautilus_parses_it() {
    let frame = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100.5","sz":"2","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","time":1700000000000,"tid":424242,"users":["0x0000000000000000000000000000000000000000","0x0000000000000000000000000000000000000000"]}]}"#;
    let HyperliquidWsMessage::Trades { data } =
        serde_json::from_str(frame).expect("must parse into Nautilus's shape")
    else {
        panic!("must dispatch as trades")
    };
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].coin, "BTC");
    assert_eq!(data[0].side, HyperliquidSide::Buy);
    assert_eq!(data[0].px, "100.5");
    assert_eq!(data[0].sz, "2");
    assert_eq!(data[0].tid, 424_242);
}

/// `"buy"` is not a side Nautilus accepts, however natural it looks.
#[test]
fn a_spelled_out_side_would_not_parse() {
    let bad = r#"{"channel":"trades","data":[{"coin":"BTC","side":"buy","px":"1","sz":"1","hash":"0x0","time":1,"tid":1,"users":["",""]}]}"#;
    assert!(serde_json::from_str::<HyperliquidWsMessage>(bad).is_err());
}

// --- l4Book: the publisher's types, not Nautilus's ---

/// The casing is mixed on purpose: only `L4Order` carries `rename_all = "camelCase"` upstream, so
/// `limitPx` sits beside `L4BookUpdates`'s `book_diffs`. Do not "fix" one to match the other.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct L4Order {
    coin: String,
    side: HyperliquidSide,
    limit_px: String,
    sz: String,
    oid: u64,
    #[allow(dead_code)]
    timestamp: u64,
    #[allow(dead_code)]
    trigger_condition: String,
    #[allow(dead_code)]
    is_trigger: bool,
    #[allow(dead_code)]
    trigger_px: String,
    #[allow(dead_code)]
    is_position_tpsl: bool,
    #[allow(dead_code)]
    reduce_only: bool,
    #[allow(dead_code)]
    order_type: String,
}

/// `OrderDiff`'s container `rename_all` renames the *variants*, so the tags are lowercase and the
/// unit variant is a bare string, not an object. `Update` carries a `rename_all` of its **own**,
/// which is what spells its fields `origSz`/`newSz` while the variant tag stays `update` — the two
/// attributes do different jobs, and mirroring only the container's would put `orig_sz` on the wire.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum OrderDiff {
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Deserialize)]
struct NodeDataOrderDiff {
    #[allow(dead_code)]
    user: String,
    oid: u64,
    px: String,
    #[allow(dead_code)]
    coin: String,
    raw_book_diff: OrderDiff,
}

#[derive(Debug, Deserialize)]
struct L4BookUpdates {
    #[allow(dead_code)]
    time: u64,
    #[allow(dead_code)]
    height: u64,
    order_statuses: Vec<serde_json::Value>,
    book_diffs: Vec<NodeDataOrderDiff>,
}

/// Externally tagged with verbatim variant names: the publisher's enum carries no serde attribute.
#[derive(Debug, Deserialize)]
enum L4Book {
    Snapshot {
        coin: String,
        #[allow(dead_code)]
        time: u64,
        #[allow(dead_code)]
        height: u64,
        levels: [Vec<L4Order>; 2],
    },
    Updates(L4BookUpdates),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "channel", content = "data")]
enum L4Envelope {
    #[serde(rename = "l4Book")]
    L4Book(L4Book),
}

/// The golden `l4Book` snapshot must parse as the publisher's `L4Book::Snapshot`.
#[test]
fn a_golden_l4book_snapshot_parses_as_the_publisher_emits_it() {
    let frame = include_str!("fixtures/hl_l4book_snapshot_golden.json");
    let L4Envelope::L4Book(L4Book::Snapshot { coin, levels, .. }) =
        serde_json::from_str(frame.trim_end()).expect("must parse into the publisher's shape")
    else {
        panic!("must dispatch as a Snapshot")
    };
    assert_eq!(coin, "BTC");
    let (bids, asks) = (&levels[0], &levels[1]);
    assert_eq!(bids[0].oid, 1);
    assert_eq!(bids[0].limit_px, "100.5", "camelCase on the order only");
    assert_eq!(bids[0].sz, "5");
    assert_eq!(bids[0].coin, "BTC");
    assert_eq!(bids[0].side, HyperliquidSide::Buy);
    assert_eq!(asks[0].oid, 4);
    assert_eq!(asks[0].side, HyperliquidSide::Sell);
}

/// The golden `l4Book` updates frame: snake_case throughout, `remove` as a bare string, and
/// `update`'s own camelCase fields.
#[test]
fn a_golden_l4book_updates_frame_parses_as_the_publisher_emits_it() {
    let frame = include_str!("fixtures/hl_l4book_updates_golden.json");
    let L4Envelope::L4Book(L4Book::Updates(u)) =
        serde_json::from_str(frame.trim_end()).expect("must parse into the publisher's shape")
    else {
        panic!("must dispatch as Updates")
    };
    assert!(u.order_statuses.is_empty());
    assert_eq!(u.book_diffs[0].oid, 1);
    assert_eq!(u.book_diffs[0].px, "100.5");
    assert_eq!(
        u.book_diffs[0].raw_book_diff,
        OrderDiff::New { sz: "4".into() }
    );
    assert_eq!(u.book_diffs[1].raw_book_diff, OrderDiff::Remove);
    // A change to an order the channel has already published: `update`, with the prior size. `new`
    // asserts an order the recipient does not have is now resting, which a partial fill is not.
    assert_eq!(
        u.book_diffs[2].raw_book_diff,
        OrderDiff::Update {
            orig_sz: "3".into(),
            new_sz: "1.5".into()
        }
    );
}
