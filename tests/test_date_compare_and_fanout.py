"""Regression tests for date-aware comparison and per-line schedule fan-out.

1. eq(date_array, date_scalar) produced a mask, but every element was False
   whenever the two sides were written differently -- '2026-02-28' vs
   '2026-02-28T00:00:00' vs a datetime vs '02/28/2026'. lookup() already
   normalised its keys before comparing; the comparison family did not. The
   explicit date_* helpers did not vectorise at all.

2. A schedule fans out into one schedule per item only when period() receives
   ARRAY start/end dates. Real date windows live on the order HEADER -- one
   window for every line -- so the whole instrument collapsed into a single
   schedule: item_name empty, subinstrument_id stuck at the row's own id, and a
   per-line array in context silently read as a per-PERIOD series (three line
   amounts spread across three months). scheduleConfig.splitBy / .itemNames now
   declare the item dimension explicitly.
"""
import asyncio
import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.agent import tools as T  # noqa: E402
from backend.dsl_functions import (  # noqa: E402
    _RowAwareArray,
    date_after,
    date_before,
    date_compare,
    date_equals,
    eq,
    gt,
    multiply,
    neq,
    normalize_date,
    period,
    schedule,
)
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    execute_python_template,
    merge_event_data_by_instrument,
)

DATES = ["2026-01-31", "2026-02-28", "2026-03-31"]
MASK = [False, True, False]


# -- 1. date-aware comparison ---------------------------------------------
@pytest.mark.parametrize("array,target", [
    (DATES, "2026-02-28"),
    (DATES, "2026-02-28T00:00:00"),
    ([d + "T00:00:00" for d in DATES], "2026-02-28"),
    (DATES, dt.datetime(2026, 2, 28)),
    (["2026-1-31", "2026-2-28", "2026-3-31"], "2026-02-28"),
    (DATES, "02/28/2026"),
])
def test_eq_masks_dates_across_representations(array, target):
    assert eq(array, target) == MASK


def test_neq_is_the_inverse():
    assert neq(DATES, "2026-02-28T00:00:00") == [True, False, True]


def test_ordering_is_date_aware_across_formats():
    assert gt(["01/31/2026", "03/31/2026"], "2026-02-28") == [False, True]


def test_scalar_date_comparison():
    assert eq("2026-02-28T00:00:00", "2026-02-28") is True
    assert eq("2026-02-28", "2026-03-01") is False


def test_row_aware_date_still_compares_as_the_current_row():
    raa = _RowAwareArray(DATES, row_value="2026-01-31")
    assert eq(raa, "2026-01-31") is True


@pytest.mark.parametrize("fn,expected", [
    (date_equals, [False, True, False]),
    (date_before, [True, False, False]),
    (date_after, [False, False, True]),
    (date_compare, [-1, 0, 1]),
])
def test_explicit_date_helpers_vectorise(fn, expected):
    assert fn(DATES, "2026-02-28") == expected


def test_explicit_date_helpers_still_work_on_scalars():
    assert date_equals("2026-02-28", "02/28/2026") is True
    assert date_before("2026-01-01", "2026-02-01") is True


# -- non-dates must be untouched by all of this ---------------------------
def test_strings_and_numbers_unaffected():
    assert eq(["A", "B", "C"], "B") == MASK
    assert eq([1, 2, 3], 2) == MASK
    assert eq(["1", "2"], "2") == [False, True]
    assert gt([1, 2, 3], 2) == [False, False, True]
    assert eq(["A", "B"], ["A", "B"]) is True


@pytest.mark.parametrize("value", [
    "COS_PRTDIG_MGRT_US_New_15_1200_0300_Print", "PO-A", "RATABLE",
    "New York Branch",
])
def test_normalize_date_still_leaves_non_dates_alone(value):
    assert normalize_date(value) == value


def test_bare_single_digit_dates_now_canonicalise():
    assert normalize_date("2026-1-31") == "2026-01-31"


# -- 2. per-line schedule fan-out -----------------------------------------
PRODS = ["PO-A", "PO-B", "PO-C"]
SUBS = ["1", "2", "3"]
AMTS = [960.0, 640.0, 400.0]
COLS = {"d": "period_date", "line": "item_name",
        "sub": "subinstrument_id", "amt": "AllocatedAmounts"}


def test_shared_window_fans_out_per_item():
    """Scalar (header) dates plus a declared item dimension."""
    res = schedule(period("2026-01-01", "2026-03-31", "M"), COLS,
                   {"AllocatedAmounts": AMTS, "subinstrument_ids": SUBS,
                    "item_names": PRODS})
    assert len(res) == 3
    assert [r["item_name"] for r in res] == PRODS
    assert [r["subinstrument_id"] for r in res] == SUBS
    firsts = [r["schedule"][0] for r in res]
    assert [f["line"] for f in firsts] == PRODS
    assert [f["sub"] for f in firsts] == SUBS
    # each line's own allocation, NOT a per-period series
    assert [f["amt"] for f in firsts] == AMTS
    assert all(len(r["schedule"]) == 3 for r in res)


def test_item_name_falls_back_to_the_subinstrument_id():
    res = schedule(period("2026-01-01", "2026-02-28", "M"), COLS,
                   {"AllocatedAmounts": AMTS, "subinstrument_ids": SUBS})
    assert [r["item_name"] for r in res] == SUBS


def test_without_a_declared_item_dimension_nothing_changes():
    rows = schedule(period("2026-01-01", "2026-03-31", "M"), COLS,
                    {"AllocatedAmounts": AMTS})
    assert isinstance(rows, list) and "schedule" not in rows[0]
    assert len(rows) == 3          # periods, not items
    assert rows[0]["sub"] == "1" and rows[0]["line"] == ""


def test_array_dates_still_fan_out():
    res = schedule(period(["2026-01-01"] * 3, ["2026-03-31"] * 3, "M"),
                   COLS, {"AllocatedAmounts": AMTS})
    assert len(res) == 3


# -- validator + codegen --------------------------------------------------
def _sc(**over):
    sc = {"periodType": "date", "frequency": "M",
          "startDateSource": "field", "startDateField": "EVT.start_dt",
          "endDateSource": "field", "endDateField": "EVT.end_dt",
          "columns": [{"name": "period_date", "formula": "period_date"},
                      {"name": "line", "formula": "item_name"}]}
    sc.update(over)
    return sc


def test_split_by_and_item_names_are_kept():
    out, _ = T._validate_schedule_step_shape(
        "S", _sc(splitBy="sub_ids", itemNames="ProductIds"), [])
    assert out["splitBy"] == "sub_ids"
    assert out["itemNames"] == "ProductIds"


@pytest.mark.parametrize("bad", ["collect_all(x)", "'sub_ids'", "EVT.sub"])
def test_non_identifier_is_rejected(bad):
    with pytest.raises(Exception, match="must be the NAME of an earlier step"):
        T._validate_schedule_step_shape("S", _sc(splitBy=bad), [])


def test_codegen_emits_the_item_dimension():
    sc, _ = T._validate_schedule_step_shape(
        "Sched", _sc(splitBy="sub_ids", itemNames="ProductIds"), [])
    code = T._generate_rule_code({
        "id": "r", "name": "t", "outputs": {},
        "steps": [{"name": "Sched", "stepType": "schedule",
                   "scheduleConfig": sc, "outputVars": []}]})
    assert '"subinstrument_ids": sub_ids' in code, code
    assert '"item_names": ProductIds' in code, code


def test_undeclared_schedule_emits_no_item_dimension():
    sc, _ = T._validate_schedule_step_shape("Sched", _sc(), [])
    code = T._generate_rule_code({
        "id": "r", "name": "t", "outputs": {},
        "steps": [{"name": "Sched", "stepType": "schedule",
                   "scheduleConfig": sc, "outputVars": []}]})
    assert "subinstrument_ids" not in code
    assert "item_names" not in code


# -- end to end: header dates, one transaction per PO line ----------------
EVT = "SO_EVENT"
ROWS = [{"instrumentid": "SO-1", "subinstrumentid": str(i + 1),
         "postingdate": "2026-01-31", "effectivedate": "2026-01-31",
         "product_id": p, "ssp": s, "order_amount": 2000.0,
         "start_dt": "2026-01-01", "end_dt": "2026-03-31"}
        for i, (p, s) in enumerate([("PO-A", 1200.0), ("PO-B", 800.0),
                                    ("PO-C", 500.0)])]
FIELDS = {EVT: {"eventType": "activity", "fields": [
    {"name": "product_id", "datatype": "string"},
    {"name": "ssp", "datatype": "decimal"},
    {"name": "order_amount", "datatype": "decimal"},
    {"name": "start_dt", "datatype": "date"},
    {"name": "end_dt", "datatype": "date"}]}}


def test_order_grain_model_with_header_dates():
    sc = {"periodType": "date", "frequency": "M",
          "startDateSource": "field", "startDateField": f"{EVT}.start_dt",
          "endDateSource": "field", "endDateField": f"{EVT}.end_dt",
          "splitBy": "sub_ids", "itemNames": "ProductIds",
          "columns": [
              {"name": "period_date", "formula": "period_date"},
              {"name": "month_end", "formula": "end_of_month(period_date)"},
              {"name": "line", "formula": "item_name"},
              {"name": "sub", "formula": "subinstrument_id"},
              {"name": "period_revenue",
               "formula": "divide(AllocatedAmounts, total_periods)"}]}
    outs = [{"name": "PeriodRevenue", "type": "filter",
             "column": "period_revenue", "matchCol": "month_end",
             "matchValue": "postingdate"}]
    sc, outs = T._validate_schedule_step_shape("Sched", sc, outs)

    def collect(n, f):
        return {"name": n, "stepType": "calc", "source": "collect",
                "collectType": "collect_by_instrument",
                "eventField": f"{EVT}.{f}"}

    rule = {"id": "r", "name": "PO line revenue", "steps": [
        collect("SSPs", "ssp"), collect("ProductIds", "product_id"),
        collect("sub_ids", "subinstrumentid"),
        {"name": "OrderAmount", "stepType": "calc", "source": "event_field",
         "eventField": f"{EVT}.order_amount"},
        {"name": "TotalSSP", "stepType": "calc", "source": "formula",
         "formula": "sum(SSPs)"},
        {"name": "AllocatedAmounts", "stepType": "iteration", "iterations": [
            {"type": "apply_each", "sourceArray": "SSPs",
             "expression": "multiply(divide(each, TotalSSP), OrderAmount)",
             "resultVar": "AllocatedAmounts"}]},
        {"name": "Sched", "stepType": "schedule", "scheduleConfig": sc,
         "outputVars": outs}],
        "outputs": {"transactions": [
            {"type": "RevenueRecognised", "amount": "PeriodRevenue",
             "subInstrumentId": "sub_ids",
             "postingDate": f"{EVT}.postingdate",
             "effectiveDate": f"{EVT}.effectivedate"}]}}

    code = T._generate_rule_code(rule)
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(execute_python_template(
            dsl_to_python_multi_event(code, FIELDS),
            merge_event_data_by_instrument({EVT: [dict(r) for r in ROWS]}),
            {EVT: [dict(r) for r in ROWS]}, None, None))
    finally:
        loop.close()

    txns = out.get("transactions") or []
    assert len(txns) == 3, txns
    assert [t.subinstrumentid for t in txns] == SUBS
    assert [round(t.amount, 2) for t in txns] == [320.0, 213.33, 133.33]
    assert round(sum(t.amount for t in txns), 2) == 666.67
