"""collect_* must see one posting date's rows, not the whole book.

collect_by_instrument() and collect_all() read the raw event data directly and
span every date they are handed. Business Preview and the agent's dry run both
pass date-filtered data for exactly that reason; the full-book run passed the
unfiltered dict.

The consequence was severe and quiet: a rule that collects a per-line array and
fans it into createTransaction gathered one value per row across ALL posting
dates, so a 9-date book emitted the same transaction 32 times with the same
date, sub-instrument and amount. The Transaction Report and the regression
suite disagreed with Business Preview for the same rule and the same data.
"""

import asyncio
import os
import sys
from collections import Counter

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402

DATES = ["2026-01-31", "2026-02-28", "2026-03-31"]
FIELDS = {"SOD": [{"name": "amt", "datatype": "decimal"}]}
META = {"SOD": {"eventType": "activity"}}

# The shape of a real revenue rule: collect per instrument, fan out per line.
FANOUT_DSL = """subs = collect_by_instrument(SOD.subinstrumentid)
amts = collect_by_instrument(SOD.amt)
createTransaction(postingdate, effectivedate, "ALLOCATED_REVENUE", amts, subs)
"""


def rows_on_every_date(subs=("1",)):
    return [{"instrumentid": "I1", "subinstrumentid": s, "postingdate": d,
             "effectivedate": d, "amt": 10.0}
            for d in DATES for s in subs]


def run_book(dsl, rows):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            S._run_full_book(dsl, FIELDS, META, {"SOD": rows}))
    finally:
        loop.close()


def test_a_collecting_rule_does_not_repeat_across_posting_dates():
    out = run_book(FANOUT_DSL, rows_on_every_date())
    assert not out["errors"]
    assert len(out["transactions"]) == len(DATES)


def test_no_transaction_is_emitted_more_than_once():
    out = run_book(FANOUT_DSL, rows_on_every_date())
    counts = Counter(
        (t["instrumentid"], t["postingdate"], t["transactiontype"],
         t["subinstrumentid"], t["amount"])
        for t in out["transactions"])
    assert max(counts.values()) == 1


def test_each_date_contributes_exactly_its_own_rows():
    out = run_book(FANOUT_DSL, rows_on_every_date())
    assert [(e["posting_date"], e["transactions"]) for e in out["per_date"]] == [
        (d, 1) for d in DATES]


def test_multiple_line_items_still_fan_out_within_a_date():
    """The per-line fan-out is the point — scoping must not flatten it."""
    out = run_book(FANOUT_DSL, rows_on_every_date(subs=("1", "2", "3")))
    assert len(out["transactions"]) == len(DATES) * 3
    per_date = {e["posting_date"]: e["transactions"] for e in out["per_date"]}
    assert per_date == {d: 3 for d in DATES}
    # Each date carries all three sub-instruments, once each.
    for d in DATES:
        subs = [t["subinstrumentid"] for t in out["transactions"]
                if t["postingdate"] == d]
        assert sorted(subs) == ["1", "2", "3"]


def test_the_full_book_agrees_with_a_single_date_run():
    """The property the report was violating: running one date and running the
    whole book must produce the same rows for that date."""
    rows = rows_on_every_date(subs=("1", "2"))
    whole = run_book(FANOUT_DSL, rows)
    one_date = [r for r in rows if r["postingdate"] == DATES[1]]
    single = run_book(FANOUT_DSL, one_date)

    def key(t):
        return (t["instrumentid"], t["postingdate"], t["transactiontype"],
                t["subinstrumentid"], t["amount"])

    from_whole = sorted(key(t) for t in whole["transactions"]
                        if t["postingdate"] == DATES[1])
    assert from_whole == sorted(key(t) for t in single["transactions"])


def test_reference_events_are_still_visible_on_every_date():
    """Scoping filters activity rows; reference tables must pass through, or
    every lookup against them would come back empty."""
    rows = rows_on_every_date()
    ref = [{"instrumentid": "I1", "rate": 0.05}]
    fields = {**FIELDS, "CAT": [{"name": "rate", "datatype": "decimal"}]}
    meta = {**META, "CAT": {"eventType": "reference"}}
    dsl = ('rates = collect_all(CAT.rate)\n'
           'createTransaction(postingdate, effectivedate, "R", max(rates))\n')
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(
            S._run_full_book(dsl, fields, meta, {"SOD": rows, "CAT": ref}))
    finally:
        loop.close()
    assert not out["errors"]
    # A rate was found on every date, so nothing was filtered away.
    assert len(out["transactions"]) == len(DATES)
    assert {t["amount"] for t in out["transactions"]} == {0.05}
