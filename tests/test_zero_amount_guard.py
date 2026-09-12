"""The engine does not persist a transaction whose amount is zero.

createTransaction is the single point where a transaction is born, so the guard
sits there: it covers preview, dry run and the persisted report alike, for every
rule. Rounding to 4dp happens FIRST, so floating-point dust is suppressed too
while anything that survives to 4dp is kept.

The suppression is counted (_get_skipped_zero_amount, surfaced as
`zero_amount_skipped` on the execution result) so a run that emits fewer rows
than its input can explain the gap. A silent drop would make reconciliation by
row count impossible to account for.

Also covers a bug found while verifying this: execute_python_template treated
process_standalone's (transactions, print_outputs) TUPLE as a list of
transactions, so every standalone rule came back with zero transactions.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.dsl_functions import (  # noqa: E402
    _clear_transaction_results,
    _get_skipped_zero_amount,
    _get_transaction_results,
    createTransaction,
)
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    dsl_to_python_standalone,
    execute_python_template,
    merge_event_data_by_instrument,
)

DATE = "2026-01-31"


def _emit(amount, sub="1"):
    _clear_transaction_results()
    createTransaction(DATE, DATE, "Rev", amount, sub)
    return _get_transaction_results(), _get_skipped_zero_amount()


# -- what is suppressed ----------------------------------------------------
@pytest.mark.parametrize("amount", [0, 0.0, -0.0, "0", 1e-15, 0.00004, -0.00001])
def test_zero_amounts_are_not_persisted(amount):
    txns, skipped = _emit(amount)
    assert txns == []
    assert skipped == 1


# -- what survives ---------------------------------------------------------
@pytest.mark.parametrize("amount", [1250.50, 0.0001, -0.0001, -500.0, 42])
def test_non_zero_amounts_are_kept(amount):
    txns, skipped = _emit(amount)
    assert len(txns) == 1
    assert txns[0]["amount"] == round(float(amount), 4)
    assert skipped == 0


def test_negative_amounts_are_not_treated_as_empty():
    """A credit is real economic content; only ZERO is dropped."""
    txns, _ = _emit(-500.0)
    assert txns[0]["amount"] == -500.0


# -- fan-out ---------------------------------------------------------------
def test_only_the_zero_lines_are_dropped_from_a_fan_out():
    _clear_transaction_results()
    createTransaction(DATE, DATE, "Rev", [960.0, 0.0, 400.0], ["1", "2", "3"])
    txns = _get_transaction_results()
    assert [(t["subinstrumentid"], t["amount"]) for t in txns] == [
        ("1", 960.0), ("3", 400.0)]
    assert _get_skipped_zero_amount() == 1


def test_an_all_zero_rule_emits_nothing():
    _clear_transaction_results()
    assert createTransaction(DATE, DATE, "Rev", [0.0, 0.0], ["1", "2"]) is None
    assert _get_transaction_results() == []
    assert _get_skipped_zero_amount() == 2


# -- the count is reset per run and reported -------------------------------
def test_counter_resets_between_runs():
    _emit(0)
    assert _get_skipped_zero_amount() == 1
    _emit(10.0)
    assert _get_skipped_zero_amount() == 0


def _run_standalone(dsl):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(execute_python_template(
            dsl_to_python_standalone(dsl), [{}], {}, None, None))
    finally:
        loop.close()


def test_execution_result_reports_the_suppressed_count():
    out = _run_standalone(
        f'createTransaction("{DATE}","{DATE}","Rev", 0)\n'
        f'createTransaction("{DATE}","{DATE}","Rev", 42)\n')
    assert [t.amount for t in out["transactions"]] == [42.0]
    assert out["zero_amount_skipped"] == 1


def test_clean_run_reports_zero_suppressed():
    out = _run_standalone(f'createTransaction("{DATE}","{DATE}","Rev", 42)\n')
    assert out["zero_amount_skipped"] == 0


# -- standalone rules return their transactions at all ---------------------
def test_standalone_rules_return_transactions():
    """execute_python_template treated process_standalone's
    (transactions, print_outputs) tuple as a list of transactions, so both
    entries failed TransactionOutput(**...) and were dropped -- every
    standalone rule came back empty."""
    out = _run_standalone(f'createTransaction("{DATE}","{DATE}","Rev", 42)\n')
    assert len(out["transactions"]) == 1
    assert out["transactions"][0].amount == 42.0


# -- event-driven path behaves the same ------------------------------------
EVT = "SO_EVENT"
ROWS = [{"instrumentid": "SO-1", "subinstrumentid": str(i + 1),
         "postingdate": DATE, "effectivedate": DATE, "amt": a}
        for i, a in enumerate([960.0, 0.0, 400.0])]
FIELDS = {EVT: {"eventType": "activity",
                "fields": [{"name": "amt", "datatype": "decimal"}]}}


def test_zero_lines_dropped_on_the_event_driven_path():
    dsl = (f"amts = collect_by_instrument({EVT}.amt)\n"
           f"subs = collect_by_instrument({EVT}.subinstrumentid)\n"
           f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
           f'"Rev", amts, subs)\n')
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(execute_python_template(
            dsl_to_python_multi_event(dsl, FIELDS),
            merge_event_data_by_instrument({EVT: [dict(r) for r in ROWS]}),
            {EVT: [dict(r) for r in ROWS]}, None, None))
    finally:
        loop.close()
    txns = out["transactions"]
    assert [(t.subinstrumentid, t.amount) for t in txns] == [
        ("1", 960.0), ("3", 400.0)]
    assert out["zero_amount_skipped"] == 1
