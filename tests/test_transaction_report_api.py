"""Tests for GET /api/transaction-reports — the Transaction Report view.

Each template execution stores its own transaction_reports document holding
that run's transactions. The report needs the whole history as one table, in a
canonical order:

    instrumentid, postingdate, effectivedate, subinstrumentid, amount

(the spec listed instrumentid twice; the second occurrence is redundant once
subinstrumentid follows it).

Sub-instrument ids sort NUMERICALLY — a plain string sort puts 10 between 1 and
2, which silently interleaves one instrument's lines in the report.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402


class _NoDB:
    """Force the in-memory fallback so these tests need no Mongo."""
    def __getattr__(self, name):
        raise Exception("no db")


def _txn(iid, sub, pdate, amount, edate=None, ttype="Rev"):
    return {"instrumentid": iid, "subinstrumentid": sub, "postingdate": pdate,
            "effectivedate": edate or pdate, "transactiontype": ttype,
            "amount": amount}


@pytest.fixture
def reports(monkeypatch):
    docs = [
        {"template_name": "REVREC", "executed_at": "2026-01-31T10:00:00",
         "transactions": [
             _txn("SO-2", "10", "2026-02-28", 50.0),
             _txn("SO-1", "2", "2026-01-31", 213.3333),
             _txn("SO-1", "10", "2026-01-31", 10.0),
             _txn("SO-1", "1", "2026-01-31", 320.0),
         ]},
        {"template_name": "DELIVERY", "executed_at": "2026-02-01T09:00:00",
         "transactions": [_txn("SO-1", "1", "2026-02-28", 320.0)]},
    ]
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", docs)
    monkeypatch.setattr(S, "db", _NoDB())
    return docs


def _get(**kw):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(S.get_transaction_reports(**kw))
    finally:
        loop.close()


# -- flattening and ordering ----------------------------------------------
def test_flattens_every_run_into_one_table(reports):
    r = _get()
    assert r["total"] == 5
    assert r["summary"]["run_count"] == 2


def test_canonical_order(reports):
    rows = _get()["transactions"]
    key = [(t["instrumentid"], t["postingdate"], t["subinstrumentid"]) for t in rows]
    assert key == [
        ("SO-1", "2026-01-31", "1"),
        ("SO-1", "2026-01-31", "2"),
        ("SO-1", "2026-01-31", "10"),   # numeric, not lexicographic
        ("SO-1", "2026-02-28", "1"),
        ("SO-2", "2026-02-28", "10"),
    ]


def test_subinstrument_sorts_numerically_not_as_text(reports):
    rows = [t for t in _get()["transactions"]
            if t["instrumentid"] == "SO-1" and t["postingdate"] == "2026-01-31"]
    assert [t["subinstrumentid"] for t in rows] == ["1", "2", "10"]


def test_rows_carry_their_provenance(reports):
    for t in _get()["transactions"]:
        assert t["template_name"]
        assert t["executed_at"]


# -- summary + filter options ---------------------------------------------
def test_summary_totals(reports):
    r = _get()
    assert r["summary"]["total_amount"] == pytest.approx(913.3333)
    assert r["summary"]["instrument_count"] == 2


def test_filter_options_are_offered(reports):
    f = _get()["filters"]
    assert f["instruments"] == ["SO-1", "SO-2"]
    assert f["templates"] == ["DELIVERY", "REVREC"]
    assert f["transaction_types"] == ["Rev"]


# -- filtering ------------------------------------------------------------
def test_filter_by_instrument(reports):
    r = _get(instrumentid="SO-1")
    assert r["total"] == 4
    assert {t["instrumentid"] for t in r["transactions"]} == {"SO-1"}


def test_filter_by_template(reports):
    r = _get(template_name="DELIVERY")
    assert r["total"] == 1
    assert r["transactions"][0]["template_name"] == "DELIVERY"


def test_unknown_filter_returns_empty_not_an_error(reports):
    r = _get(instrumentid="NOPE")
    assert r["total"] == 0 and r["transactions"] == []


# -- paging ---------------------------------------------------------------
def test_paging_reports_truncation(reports):
    r = _get(limit=2)
    assert r["returned"] == 2
    assert r["total"] == 5
    assert r["truncated"] is True


def test_offset_walks_the_table(reports):
    first = _get(limit=2)["transactions"]
    second = _get(limit=2, offset=2)["transactions"]
    assert first != second
    assert _get(limit=2, offset=4)["truncated"] is False


def test_limit_is_capped(reports):
    """The table is contracts x lines x periods — never unbounded."""
    assert _get(limit=10 ** 9)["limit"] <= 50000


# -- robustness -----------------------------------------------------------
def test_no_reports_is_an_empty_report_not_an_error(monkeypatch):
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    monkeypatch.setattr(S, "db", _NoDB())
    r = _get()
    assert r["total"] == 0
    assert r["summary"]["total_amount"] == 0


def test_malformed_rows_are_skipped(monkeypatch):
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [
        {"template_name": "X", "executed_at": "", "transactions": [
            "not a dict", 42, _txn("SO-1", "1", "2026-01-31", 5.0),
            {"instrumentid": "SO-9", "amount": "not-a-number"},
        ]}])
    monkeypatch.setattr(S, "db", _NoDB())
    r = _get()
    assert r["total"] == 2                       # the two dicts
    assert r["summary"]["total_amount"] == 5.0   # unparseable amount -> 0
