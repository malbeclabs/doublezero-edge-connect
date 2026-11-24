"""Folds edge-connect's `book` and `order_book` batches (PROTOCOL.md) into price levels.

Pure Python, so it is testable without Hummingbot installed.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

BID = "bid"
ASK = "ask"
SIDES = (BID, ASK)

# (source_id, channel, instrument_id): PROTOCOL.md's book identity. `symbol` is a display label.
MarketKey = Tuple[int, int, int]
Level = Tuple[float, float]


def market_key(msg: dict) -> MarketKey:
    return (msg["source_id"], msg["channel"], msg["instrument_id"])


@dataclass
class Event:
    """One logical book event's net effect on the published levels. A size of 0 removes the level."""

    update_id: int
    bids: List[Level]
    asks: List[Level]
    source_ts_ns: int


@dataclass
class _Market:
    # The price-aggregated population (`book`) and the order-level one (`order_book`). A market uses
    # one; a `clear` empties both on its side.
    levels: Dict[str, Dict[float, float]] = field(default_factory=lambda: {BID: {}, ASK: {}})
    orders: Dict[int, Tuple[str, float, float]] = field(default_factory=dict)
    level_orders: Dict[Tuple[str, float], Dict[int, float]] = field(default_factory=dict)
    # What the consumer holds: the levels as of the last closed event.
    published: Dict[str, Dict[float, float]] = field(default_factory=lambda: {BID: {}, ASK: {}})
    touched: Dict[str, Set[float]] = field(default_factory=lambda: {BID: set(), ASK: set()})
    cleared: Set[str] = field(default_factory=set)
    complete: bool = False
    update_id: int = 0
    source_ts_ns: int = 0

    def clear(self, side: str) -> None:
        for s in SIDES if side == "both" else (side,):
            self.cleared.add(s)
            self.touched[s].update(self.published[s])
            self.levels[s].clear()
            for level in [k for k in self.level_orders if k[0] == s]:
                for order_id in self.level_orders.pop(level):
                    del self.orders[order_id]

    def set_level(self, side: str, price: float, size: float) -> None:
        self.touched[side].add(price)
        if size > 0:
            self.levels[side][price] = size
        else:
            self.levels[side].pop(price, None)

    def set_order(self, order_id: int, side: str, price: float, size: float) -> None:
        previous = self.orders.pop(order_id, None)
        if previous is not None:
            old_level = previous[:2]
            resting = self.level_orders[old_level]
            del resting[order_id]
            if not resting:
                del self.level_orders[old_level]
            self.touched[old_level[0]].add(old_level[1])
        if size > 0:
            self.orders[order_id] = (side, price, size)
            self.level_orders.setdefault((side, price), {})[order_id] = size
            self.touched[side].add(price)

    def size(self, side: str, price: float) -> float:
        # Summed afresh from the resting orders: a running total would keep float residue after
        # its last order leaves, and the level would never be removed.
        return self.levels[side].get(price, 0.0) + sum(self.level_orders.get((side, price), {}).values())

    def close(self) -> Optional[Event]:
        if self.cleared == set(SIDES):
            self.complete = True
        self.cleared.clear()
        bids, asks = self._publish(BID), self._publish(ASK)
        if not bids and not asks:
            return None
        self.update_id += 1
        return Event(self.update_id, bids, asks, self.source_ts_ns)

    def _publish(self, side: str) -> List[Level]:
        published, out = self.published[side], []
        for price in self.touched[side]:
            size = self.size(side, price)
            if size != published.get(price, 0.0):
                out.append((price, size))
                if size > 0:
                    published[price] = size
                else:
                    del published[price]
        self.touched[side].clear()
        return out


class BookAssembler:
    """Turns edge-connect book batches into net level changes, one per closed logical event.

    Changes are applied as they arrive but published only at `last: true`, so a consumer never sees
    an event's intermediate (possibly crossed) book. A `clear` empties its side and the rebuild
    that follows is published as the difference from what the consumer already holds, which makes
    a re-baseline an ordinary event rather than a snapshot.
    """

    def __init__(self) -> None:
        self._markets: Dict[MarketKey, _Market] = {}

    def apply(self, msg: dict) -> Optional[Event]:
        """Applies one `book`/`order_book` message; returns the net change if it closes an event."""
        market = self._markets.setdefault(market_key(msg), _Market())
        for change in msg["changes"]:
            action, side = change["action"], change["side"]
            if action == "clear":
                market.clear(side)
                continue
            size = change["size"] if action == "update" else 0.0
            order_id = change.get("order_id", 0)
            if order_id:
                market.set_order(order_id, side, change["price"], size)
            else:
                market.set_level(side, change["price"], size)
        market.source_ts_ns = msg.get("source_ts_ns") or market.source_ts_ns
        return market.close() if msg["last"] else None

    def is_complete(self, key: MarketKey) -> bool:
        """Whether a re-baseline of both sides has closed, so the published levels are the whole book."""
        market = self._markets.get(key)
        return market is not None and market.complete

    def snapshot(self, key: MarketKey) -> Tuple[int, List[Level], List[Level]]:
        """The published book, best first, and the update id of the event it stands at."""
        market = self._markets[key]
        bids = sorted(market.published[BID].items(), reverse=True)
        asks = sorted(market.published[ASK].items())
        return market.update_id, bids, asks

    def forget(self, key: MarketKey) -> None:
        self._markets.pop(key, None)
