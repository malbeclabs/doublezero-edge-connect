"""Judges an E2E run: `check.py EVIDENCE REPORT`. Exits 0 when every check passes."""

import json
import math
import sys

MIN_STATES = 5
MIN_TRADES = 10


def failures(evidence, report):
    for pair, book in evidence["pairs"].items():
        bid, ask = book.get("top", (math.nan, math.nan))
        if not bid < ask:
            yield f"{pair}: no two-sided book (bid {bid}, ask {ask})"
        if book.get("states", 0) < MIN_STATES:
            yield f"{pair}: the book took {book.get('states', 0)} distinct states, wanted {MIN_STATES}"
        if min(book.get("levels", [0])) < 2:
            yield f"{pair}: fewer than two levels a side ({book.get('levels')})"
        if not math.isfinite(book.get("depth_price", math.nan)):
            yield f"{pair}: a depth query into the second level returned {book.get('depth_price')}"
        ids = book["trade_ids"]
        if len(ids) < MIN_TRADES or len(set(ids)) != len(ids):
            yield f"{pair}: {len(ids)} trades, {len(set(ids))} distinct ids, wanted {MIN_TRADES} distinct"
    if len(evidence["marks"]) < 2:
        yield f"funding mark price took {len(evidence['marks'])} values after start-up, wanted it to change"
    if report["edge"]["unserved"]:
        yield f"edge-connect was sent requests it does not serve: {report['edge']['unserved']}"
    if not report["edge"]["subscriptions"]:
        yield "edge-connect received no subscription"
    if report["venue"]["unexpected"]:
        yield f"the venue's WebSocket was asked for more than funding: {report['venue']['unexpected']}"


def main():
    with open(sys.argv[1]) as f:
        evidence = json.load(f)
    with open(sys.argv[2]) as f:
        report = json.load(f)
    problems = list(failures(evidence, report))
    for problem in problems:
        print(f"FAIL {problem}")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
