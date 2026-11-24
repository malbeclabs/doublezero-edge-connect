from dz_hummingbot.assembler import BookAssembler, market_key


def batch(changes, last=True, kind="book", channel=2, instrument_id=41, symbol="SOL", source_ts_ns=0):
    return {
        "type": kind, "source_name": "BookVenue", "source_id": 0, "symbol": symbol,
        "channel": channel, "instrument_id": instrument_id, "changes": changes,
        "snapshot": False, "last": last, "source_ts_ns": source_ts_ns,
    }


def clear(side="both"):
    return {"action": "clear", "side": side, "price": 0, "size": 0, "order_id": 0}


def update(side, price, size, order_id=0):
    return {"action": "update", "side": side, "price": price, "size": size, "order_id": order_id}


def delete(side, price, order_id=0):
    return {"action": "delete", "side": side, "price": price, "size": 0, "order_id": order_id}


KEY = (0, 2, 41)


def test_protocol_example_applies_to_the_held_book():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 0.62, 100), update("ask", 0.63, 40), update("ask", 0.64, 10)]))

    # PROTOCOL.md's `book` example, verbatim.
    event = book.apply(batch([update("bid", 0.62, 150), delete("ask", 0.63)]))

    assert (event.bids, event.asks) == ([(0.62, 150)], [(0.63, 0.0)])
    assert book.snapshot(KEY) == ([(0.62, 150)], [(0.64, 10)])


def test_rebaseline_split_across_batches_publishes_nothing_until_last():
    book = BookAssembler()

    assert book.apply(batch([clear(), update("bid", 10.0, 1)], last=False)) is None
    assert not book.is_complete(KEY)
    event = book.apply(batch([update("ask", 11.0, 2)]))

    assert (event.bids, event.asks) == ([(10.0, 1)], [(11.0, 2)])
    assert book.is_complete(KEY)


def test_rebaseline_sends_its_book_whole_and_removes_levels_it_lacks():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1), update("bid", 9.0, 3), update("ask", 11.0, 2)]))

    event = book.apply(batch([clear(), update("bid", 10.0, 1), update("ask", 12.0, 5)]))

    assert sorted(event.bids) == [(9.0, 0.0), (10.0, 1)]
    assert sorted(event.asks) == [(11.0, 0.0), (12.0, 5)]
    assert book.snapshot(KEY) == ([(10.0, 1)], [(12.0, 5)])


def test_clear_mid_event_discards_the_open_event():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1), update("ask", 11.0, 2)]))

    assert book.apply(batch([update("bid", 10.5, 7), update("ask", 11.0, 9)], last=False)) is None
    event = book.apply(batch([clear(), update("bid", 10.0, 4), update("ask", 11.0, 2)]))

    # 10.5 was never published, so dropping it changes nothing the consumer holds.
    assert (event.bids, event.asks) == ([(10.0, 4)], [(11.0, 2)])
    assert book.snapshot(KEY) == ([(10.0, 4)], [(11.0, 2)])


def test_intermediate_crossed_state_is_never_published():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1), update("ask", 11.0, 2)]))

    assert book.apply(batch([update("bid", 11.0, 3)], last=False)) is None
    event = book.apply(batch([delete("bid", 11.0), update("ask", 11.0, 1)]))

    assert (event.bids, event.asks) == ([], [(11.0, 1)])


def test_an_event_with_no_net_change_publishes_nothing():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1)]))

    assert book.apply(batch([update("bid", 10.0, 5), update("bid", 10.0, 1)])) is None
    assert book.apply(batch([], last=True)) is None


def test_one_sided_clear_does_not_complete_a_book():
    book = BookAssembler()

    book.apply(batch([clear("bid"), update("bid", 10.0, 1)]))
    assert not book.is_complete(KEY)

    book.apply(batch([clear("bid"), clear("ask"), update("ask", 11.0, 1)]))
    assert book.is_complete(KEY)


def test_order_level_fold_sums_orders_sharing_a_price():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1.5, order_id=1), update("bid", 10.0, 2.5, order_id=2)],
                     kind="order_book"))
    assert book.snapshot(KEY)[0] == [(10.0, 4.0)]

    event = book.apply(batch([delete("bid", 10.0, order_id=1)], kind="order_book"))

    assert event.bids == [(10.0, 2.5)]
    assert book.snapshot(KEY)[0] == [(10.0, 2.5)]


def test_order_level_partial_fill_and_price_move():
    book = BookAssembler()
    book.apply(batch([clear(), update("ask", 11.0, 3, order_id=7), update("ask", 11.0, 1, order_id=8)],
                     kind="order_book"))

    fill = book.apply(batch([update("ask", 11.0, 2, order_id=7)], kind="order_book"))
    move = book.apply(batch([update("ask", 12.0, 2, order_id=7)], kind="order_book"))

    assert fill.asks == [(11.0, 3)]
    assert sorted(move.asks) == [(11.0, 1), (12.0, 2)]


def test_a_level_whose_orders_all_leave_is_removed_despite_float_residue():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 0.1, order_id=1), update("bid", 10.0, 0.2, order_id=2)],
                     kind="order_book"))

    book.apply(batch([delete("bid", 10.0, order_id=1)], kind="order_book"))
    event = book.apply(batch([delete("bid", 10.0, order_id=2)], kind="order_book"))

    assert event.bids == [(10.0, 0.0)]
    assert book.snapshot(KEY)[0] == []


def test_clear_empties_order_level_side():
    book = BookAssembler()
    book.apply(batch([clear(), update("bid", 10.0, 1, order_id=1), update("ask", 11.0, 1, order_id=2)],
                     kind="order_book"))

    event = book.apply(batch([clear("bid")], kind="order_book"))
    book.apply(batch([update("bid", 9.0, 1, order_id=1)], kind="order_book"))

    assert event.bids == [(10.0, 0.0)]
    assert book.snapshot(KEY) == ([(9.0, 1)], [(11.0, 1)])


def test_markets_sharing_a_symbol_are_kept_apart_by_identity():
    book = BookAssembler()
    first = batch([clear(), update("bid", 1.0, 1)], channel=2, instrument_id=41, symbol="LONGTICKER")
    second = batch([clear(), update("bid", 2.0, 2)], channel=2, instrument_id=42, symbol="LONGTICKER")

    book.apply(first)
    book.apply(second)

    assert book.snapshot(market_key(first))[0] == [(1.0, 1)]
    assert book.snapshot(market_key(second))[0] == [(2.0, 2)]


def test_replace_turns_one_markets_book_into_anothers():
    book = BookAssembler()
    old = batch([clear(), update("bid", 10.0, 1), update("ask", 11.0, 2)], instrument_id=41)
    new = batch([clear(), update("bid", 10.0, 1), update("ask", 12.0, 3)], instrument_id=42)
    book.apply(old)
    book.apply(new)

    bids, asks = book.replace(market_key(old), market_key(new))

    assert (sorted(bids), sorted(asks)) == ([(10.0, 1)], [(11.0, 0.0), (12.0, 3)])
    assert not book.is_complete(market_key(old))


def test_source_timestamp_is_the_latest_nonzero_one():
    book = BookAssembler()
    book.apply(batch([clear()], last=False, source_ts_ns=5))
    event = book.apply(batch([update("bid", 10.0, 1)], source_ts_ns=0))

    assert event.source_ts_ns == 5
