"""Tests for POST /api/transaction-reports/run — the Play button.

One press generates transactions for the loaded rules across EVERY instrument
and EVERY posting date present in the activity data, rather than the single
date a normal execution targets.

Two behaviours that matter:
  * prior reports are cleared first — the report view aggregates every stored
    run, so appending would double every row on a second press;
  * one bad posting date does not abandon the rest of the book.
"""
import asyncio
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402

EVT = "SO_EVENT"


class _Col:
    def __init__(self, docs):
        self.docs = list(docs)

    async def find_one(self, q, proj=None):
        for d in self.docs:
            for k, v in q.items():
                if isinstance(v, dict) and "$regex" in v:
                    if re.match(v["$regex"], str(d.get(k, "")), re.I):
                        return d
                elif d.get(k) == v:
                    return d
        return None

    def find(self, q=None, proj=None):
        docs = self.docs

        class _L:
            async def to_list(self, n):
                return list(docs)
        return _L()

    async def delete_many(self, q):
        self.docs = []
        return type("R", (), {"deleted_count": 0})()

    async def insert_one(self, doc):
        self.docs.append(doc)


def _rows():
    out = []
    for d, amts in (("2026-01-31", [100.0, 200.0]), ("2026-02-28", [300.0, 0.0])):
        for i, a in enumerate(amts):
            out.append({"instrumentid": f"SO-{i + 1}", "subinstrumentid": "1",
                        "postingdate": d, "effectivedate": d, "amt": a})
    return out


DSL = (f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
       f'"Rev", {EVT}.amt)\n')


class _DB:
    def __init__(self, dsl=DSL, rows=None):
        self.event_definitions = _Col([
            {"event_name": EVT, "eventType": "activity",
             "fields": [{"name": "amt", "datatype": "decimal"}]}])
        self.event_data = _Col([{"event_name": EVT,
                                 "data_rows": rows if rows is not None else _rows()}])
        self.transaction_reports = _Col([])
        self.saved_rules = _Col([])
        self.saved_schedules = _Col([])
        self.dsl_templates = _Col([
            {"id": "t1", "name": "REVREC", "dsl_code": dsl}])


@pytest.fixture
def db(monkeypatch):
    d = _DB()
    monkeypatch.setattr(S, "db", d)
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    return d


def _run(**kw):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(S.run_transaction_report(**kw))
    finally:
        loop.close()


def _report(**kw):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(S.get_transaction_reports(**kw))
    finally:
        loop.close()


# -- every posting date, one press ----------------------------------------
def test_runs_every_posting_date_in_the_data(db):
    r = _run(template_id="t1")
    assert r["posting_dates"] == ["2026-01-31", "2026-02-28"]
    assert r["dates_total"] == 2
    assert r["dates_succeeded"] == 2
    assert r["dates_failed"] == 0


def test_covers_every_instrument_on_each_date(db):
    r = _run(template_id="t1")
    assert [run["instruments"] for run in r["runs"]] == [2, 2]


def test_transactions_land_in_the_report(db):
    _run(template_id="t1")
    rep = _report()
    assert [(t["instrumentid"], t["postingdate"], t["amount"]) for t in rep["transactions"]] == [
        ("SO-1", "2026-01-31", 100.0),
        ("SO-1", "2026-02-28", 300.0),
        ("SO-2", "2026-01-31", 200.0),
    ]
    # the zero-amount line is suppressed by the engine guard
    assert rep["total"] == 3
    assert rep["summary"]["total_amount"] == 600.0


def test_one_report_document_per_posting_date(db):
    _run(template_id="t1")
    assert len(db.transaction_reports.docs) == 2
    assert _report()["summary"]["run_count"] == 2


# -- pressing Play twice must not double the book -------------------------
def test_second_press_replaces_rather_than_appends(db):
    _run(template_id="t1")
    first = _report()["total"]
    _run(template_id="t1")
    assert _report()["total"] == first


def test_append_mode_is_available_but_not_the_default(db):
    _run(template_id="t1")
    first = _report()["total"]
    _run(template_id="t1", replace=False)
    # The report defaults to the CURRENT run, so an appended second run does not
    # inflate it; the history is still there under scope="all".
    assert _report()["total"] == first
    assert _report(scope="all")["total"] == first * 2


# -- the report shows the CURRENT rule set, not every run ever -------------
def test_each_play_is_tagged_with_one_run_id(db):
    r = _run(template_id="t1")
    assert r["report_run_id"]
    assert {d["report_run_id"] for d in db.transaction_reports.docs} == {r["report_run_id"]}


def test_report_defaults_to_the_current_run(db):
    r = _run(template_id="t1")
    rep = _report()
    assert rep["scope"] == "current"
    assert rep["report_run_id"] == r["report_run_id"]


def test_stale_runs_are_hidden_but_still_reachable(db):
    """Transactions from a rule set that has since changed must not linger in
    the report — that is what made it show unrelated rows and load slowly."""
    db.transaction_reports.docs.insert(0, {
        "template_name": "DELETED_RULE", "executed_at": "2020-01-01T00:00:00",
        "transactions": [{"instrumentid": "OLD", "subinstrumentid": "1",
                          "postingdate": "2020-01-01", "effectivedate": "2020-01-01",
                          "transactiontype": "Stale", "amount": 999.0}]})
    _run(template_id="t1", replace=False)
    current = _report()
    assert all(t["instrumentid"] != "OLD" for t in current["transactions"])
    assert any(t["instrumentid"] == "OLD"
               for t in _report(scope="all")["transactions"])


def test_run_reports_which_rules_it_covered(db):
    r = _run(template_id="t1")
    assert r["rule_names"] == ["REVREC"]
    assert _report()["rule_names"] == ["REVREC"]


# -- a report run does not pay for print output it never reads ------------
def test_print_statements_are_stripped_from_report_runs():
    code = 'print("noise")\nx = add(1,2)\n  print(x)\ny = concat("a","print(")\n'
    out = S._strip_print_statements(code)
    assert 'print("noise")' not in out
    assert "  print(x)" not in out          # indented prints too
    assert "x = add(1,2)" in out
    assert 'concat("a","print(")' in out    # a print( inside a string survives


def test_report_run_produces_no_print_payload(db):
    """A schedule rule dumps its whole grid per posting date otherwise."""
    r = _run(template_id="t1")
    assert r["transactions_created"] > 0
    for doc in db.transaction_reports.docs:
        assert "print_outputs" not in doc


# -- resilience -----------------------------------------------------------
def test_a_failing_date_does_not_abandon_the_rest(monkeypatch):
    d = _DB()
    monkeypatch.setattr(S, "db", d)
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    real = S.execute_python_template
    calls = {"n": 0}

    async def flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom on the first date")
        return await real(*a, **kw)

    monkeypatch.setattr(S, "execute_python_template", flaky)
    r = _run(template_id="t1")
    assert r["dates_failed"] == 1
    assert r["dates_succeeded"] == 1          # the second date still ran
    assert "boom" in r["errors"][0]["error"]


def test_undated_data_still_runs_once(monkeypatch):
    rows = [{"instrumentid": "SO-1", "subinstrumentid": "1", "amt": 50.0}]
    d = _DB(rows=rows)
    monkeypatch.setattr(S, "db", d)
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    r = _run(template_id="t1")
    assert r["posting_dates"] == []
    assert r["dates_total"] == 1              # one unscoped run, not "nothing to do"


def test_unknown_template_is_a_404(db):
    with pytest.raises(Exception) as ei:
        _run(template_id="does-not-exist")
    assert "404" in str(ei.value) or "not found" in str(ei.value).lower()


def test_empty_workspace_is_rejected_clearly(monkeypatch):
    d = _DB()
    monkeypatch.setattr(S, "db", d)
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    monkeypatch.setitem(S.in_memory_data, "templates", [])
    with pytest.raises(Exception) as ei:
        _run()                                 # no template, no saved rules
    msg = str(ei.value).lower()
    assert "nothing to run" in msg or "no events" in msg


# -- the Play button on the Transaction Report ----------------------------
# It posts with no template_id, so the server combines the saved rules by
# priority — which is exactly the template last loaded into the Rule Manager.

@pytest.fixture
def workspace(db, monkeypatch):
    """A workspace holding one saved rule.

    Code generation from rule steps has its own tests; what matters here is
    that the no-template path runs whatever the workspace combines to.
    """
    db.saved_rules = _Col([{"id": "r1", "name": "Rev", "priority": 1}])

    async def _combined():
        return {"code": DSL}
    monkeypatch.setattr(S, "get_combined_code", _combined)
    return db


def test_play_runs_the_workspace_rules_without_a_template(workspace):
    r = _run()                                   # no template_id — the Play path
    assert r["template_name"] == "Workspace rules"
    assert r["dates_total"] == 2
    assert r["transactions_created"] == 3        # the zero-amount line is dropped


def test_play_covers_every_instrument_on_every_posting_date(workspace):
    r = _run()
    assert r["posting_dates"] == ["2026-01-31", "2026-02-28"]
    assert [run["instruments"] for run in r["runs"]] == [2, 2]


def test_play_names_the_rules_it_used(workspace):
    """The report states its source rather than offering a choice of one."""
    assert _run()["rule_names"] == ["Rev"]


def test_pressing_play_twice_does_not_double_the_book(workspace):
    _run()
    first = _report()["total"]
    _run()
    assert _report()["total"] == first


def test_play_lands_every_transaction_in_the_report(workspace):
    _run()
    rep = _report()
    assert [(t["instrumentid"], t["postingdate"], t["amount"]) for t in rep["transactions"]] == [
        ("SO-1", "2026-01-31", 100.0),
        ("SO-1", "2026-02-28", 300.0),
        ("SO-2", "2026-01-31", 200.0),
    ]
