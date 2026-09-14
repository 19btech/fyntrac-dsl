"""Regression tests for schedule date sources and the end-to-end order-grain model.

A schedule driven by variables produced NO rows and therefore NO transactions,
silently:

  scheduleConfig used startDateSource='value' with startDate='start_dates'.
  'value' means a LITERAL, so the generated code read
      p = period("start_dates", "end_dates", "M")
  and period() could not parse those as dates. It returned {'dates': []}
  without an error, so the schedule produced nothing, schedule_filter returned
  nothing, and createTransaction emitted nothing. Canonical pattern B specified
  exactly this shape.

Two fixes: a bare identifier in a tri-source field is recognised as a variable
reference (coerced to 'formula'), and period() now refuses a non-empty bound
that is not a date. An EMPTY bound stays silent -- that is how `runIf` switches
a schedule off on purpose.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.agent import tools as T  # noqa: E402
from backend.dsl_functions import period  # noqa: E402
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    execute_python_template,
    merge_event_data_by_instrument,
)


def _sc(**over):
    sc = {"periodType": "date", "frequency": "M",
          "startDateSource": "value", "startDate": "start_dates",
          "endDateSource": "value", "endDate": "end_dates",
          "columns": [{"name": "period_date", "formula": "period_date"}]}
    sc.update(over)
    return sc


# -- tri-source coercion ---------------------------------------------------
def test_bare_identifier_becomes_a_variable_reference():
    out, _ = T._validate_schedule_step_shape("Sched", _sc(), [])
    assert out["startDateSource"] == "formula"
    assert out["startDateFormula"] == "start_dates"
    assert out["endDateSource"] == "formula"
    assert out["endDateFormula"] == "end_dates"


def test_real_date_literals_stay_literals():
    out, _ = T._validate_schedule_step_shape(
        "Sched", _sc(startDate="2026-01-01", endDate="2026-12-31"), [])
    assert out["startDateSource"] == "value"
    assert out["startDate"] == "2026-01-01"


def test_event_field_and_call_coercions_still_work():
    out, _ = T._validate_schedule_step_shape(
        "Sched", _sc(startDate="EVT.start_dt", endDate="end_of_month(x)"), [])
    assert out["startDateSource"] == "field"
    assert out["endDateSource"] == "formula"


def test_generated_period_call_is_unquoted():
    sc, _ = T._validate_schedule_step_shape("Sched", _sc(), [])
    code = T._generate_rule_code({
        "id": "r", "name": "t", "outputs": {},
        "steps": [{"name": "Sched", "stepType": "schedule",
                   "scheduleConfig": sc, "outputVars": []}]})
    assert "p = period(start_dates, end_dates" in code, code
    assert 'period("start_dates"' not in code, code


# -- period() diagnostics --------------------------------------------------
@pytest.mark.parametrize("start,end", [
    ("start_dates", "end_dates"),
    ("2026-01-01", "not_a_date"),
])
def test_period_refuses_a_non_date_bound(start, end):
    with pytest.raises(ValueError, match="is not a date"):
        period(start, end, "M")


@pytest.mark.parametrize("end", ["", None])
def test_empty_bound_still_switches_a_schedule_off_quietly(end):
    """`runIf` rewrites the end date to "" to force zero rows — not an error."""
    assert period("2026-01-01", end, "M").get("dates") == []


def test_valid_forms_all_still_work():
    assert len(period("2026-01-01", "2026-03-31", "M")["dates"]) == 3
    assert len(period(3, "M")["dates"]) == 3
    assert period(["2026-01-01"], ["2026-03-31"], "M")["type"] == "period_array"


# -- the whole order-grain model ------------------------------------------
EVT = "SO_EVENT"
ROWS = [
    {"instrumentid": "SO-1", "subinstrumentid": "1", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-A", "ssp": 1200.0,
     "order_amount": 2000.0, "start_dt": "2026-01-01", "end_dt": "2026-03-31"},
    {"instrumentid": "SO-1", "subinstrumentid": "2", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-B", "ssp": 800.0,
     "order_amount": 2000.0, "start_dt": "2026-01-01", "end_dt": "2026-03-31"},
    {"instrumentid": "SO-1", "subinstrumentid": "3", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-C", "ssp": 500.0,
     "order_amount": 2000.0, "start_dt": "2026-01-01", "end_dt": "2026-03-31"},
]
FIELDS = {EVT: {"eventType": "activity", "fields": [
    {"name": "product_id", "datatype": "string"},
    {"name": "ssp", "datatype": "decimal"},
    {"name": "order_amount", "datatype": "decimal"},
    {"name": "start_dt", "datatype": "date"},
    {"name": "end_dt", "datatype": "date"}]}}


def test_order_grain_model_end_to_end():
    """One sales order, three PO lines, SSP-proportional allocation spread over
    three months. Exercises every layer: collect_* across sub-instruments, an
    apply_each allocation, a variable-driven per-item schedule, the schedule
    column built-ins, and a fanned-out transaction."""
    sc = _sc(startDate="start_dates", endDate="end_dates", columns=[
        {"name": "period_date", "formula": "period_date"},
        {"name": "month_end", "formula": "end_of_month(period_date)"},
        {"name": "line", "formula": "item_name"},
        {"name": "sub", "formula": "subinstrument_id"},
        {"name": "n_lines", "formula": "array_length(ProductIds_full)"},
        {"name": "period_revenue", "formula": "divide(AllocatedAmounts, total_periods)"}])
    outs = [{"name": "PeriodRevenue", "type": "filter", "column": "period_revenue",
             "matchCol": "month_end", "matchValue": "postingdate"}]
    sc, outs = T._validate_schedule_step_shape("Sched", sc, outs)
    assert sc["contextVars"] == ["AllocatedAmounts", "ProductIds"]

    def collect(name, field):
        return {"name": name, "stepType": "calc", "source": "collect",
                "collectType": "collect_by_instrument",
                "eventField": f"{EVT}.{field}"}

    rule = {"id": "r", "name": "PO line revenue", "steps": [
        collect("SSPs", "ssp"), collect("ProductIds", "product_id"),
        collect("sub_ids", "subinstrumentid"),
        collect("start_dates", "start_dt"), collect("end_dates", "end_dt"),
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
    assert [t.subinstrumentid for t in txns] == ["1", "2", "3"]
    # 1200/800/500 of 2500 * 2000 = 960/640/400, spread over 3 months
    assert [round(t.amount, 2) for t in txns] == [320.0, 213.33, 133.33]
    assert round(sum(t.amount for t in txns), 2) == 666.67
