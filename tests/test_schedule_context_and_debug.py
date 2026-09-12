"""Regression tests for three bugs hit while authoring a model through MCP.

1. `debug_step` always 500'd with "expected an indented block after 'try'
   statement" — the probe it appends was a block-indented try/except, but both
   DSL translators strip and re-indent every line.
2. A string literal in a schedule column formula (eq(x, "RATABLE")) had its
   CONTENTS scanned for identifiers, so RATABLE was auto-derived into
   scheduleConfig.contextVars and blew up as a NameError on the first column.
3. Schedule context handling: arrays were padded out to the period count
   (array_length returned the period count) and scalars were broadcast into
   lists (so lookup(values, keys, item_name) returned a list), while most
   schedule column built-ins were never bound at all.
"""
import ast
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.dsl_functions import (  # noqa: E402
    generate_schedules,
    period,
    schedule,
    _set_current_subinstrumentid,
)
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    dsl_to_python_standalone,
    execute_python_template,
)

EVENT_FIELDS = {"EVT": {"fields": [{"name": "amt", "datatype": "decimal"}],
                        "eventType": "activity"}}

DEBUG_PROBE = (
    'x = add(1, 2)\n'
    'try: print("__DEBUG_STEP__ x =", x)\n'
    'except Exception as _e: print("__DEBUG_STEP__ x = <unavailable:", _e, ">")'
)


# -- 1. debug_step probe ---------------------------------------------------
@pytest.mark.parametrize("translate", [
    lambda code: dsl_to_python_standalone(code),
    lambda code: dsl_to_python_multi_event(code, EVENT_FIELDS),
])
def test_debug_probe_translates_to_valid_python(translate):
    ast.parse(translate(DEBUG_PROBE))


def test_debug_probe_actually_prints():
    py = dsl_to_python_multi_event(DEBUG_PROBE, EVENT_FIELDS)
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(execute_python_template(
            py,
            [{"instrumentid": "I1", "postingdate": "2024-01-01", "EVT_amt": 100}],
            {"EVT": [{"instrumentid": "I1", "amt": 100}]}, None, None))
    finally:
        loop.close()
    assert any("__DEBUG_STEP__ x = 3" in str(s)
               for s in out.get("print_outputs") or []), out


def test_block_indented_probe_would_still_be_broken():
    """Guards the REASON for the single-line form: if someone 'tidies' the
    probe back into a block, the translator flattens it into invalid Python."""
    bad = ('x = add(1, 2)\n'
           'try:\n'
           '    print("x =", x)\n'
           'except Exception as _e:\n'
           '    print("boom", _e)')
    with pytest.raises(SyntaxError):
        ast.parse(dsl_to_python_standalone(bad))


# -- 2. string literals in schedule column formulas ------------------------
def test_string_literal_not_pulled_into_context_vars():
    import backend.server  # noqa: F401  (registers DSL_FUNCTIONS)
    from backend.agent import tools as T

    sc = {"periodType": "number", "frequency": "M", "periodCount": 3,
          "columns": [
              {"name": "period_date", "formula": "period_date"},
              {"name": "is_ratable",
               "formula": 'if(eq(item_method, "RATABLE"), 1, 0)'},
              {"name": "amt", "formula": "multiply(alloc_amount, is_ratable)"},
          ]}
    out, _ = T._validate_schedule_step_shape("sched", dict(sc), [])
    assert "RATABLE" not in out["contextVars"]
    assert out["contextVars"] == ["alloc_amount", "item_method"]


def test_single_quoted_literal_evaluates_in_a_column():
    rows = schedule(period("2024-01-01", "2024-03-01", "M"),
                    {"isr": "if(eq(item_method, 'RATABLE'), 1, 0)"},
                    {"item_method": "RATABLE"})
    assert [r["isr"] for r in rows] == [1, 1, 1]


# -- 3. schedule context arrays / scalars / built-ins ----------------------
def test_context_array_keeps_its_own_length():
    p = period("2024-01-01", "2026-12-01", "M")
    assert len(p["dates"]) == 36
    rows = schedule(p, {"n": "array_length(line_products)",
                        "first": "array_get(line_products, 0)"},
                    {"line_products": ["PO-A", "PO-B", "PO-C"]})
    assert rows[0]["n"] == 3          # was 36 (the period count)
    assert rows[0]["first"] == "PO-A"


def test_short_array_still_reads_as_missing_past_its_end():
    rows = schedule(period("2024-01-01", "2024-04-01", "M"),
                    {"v": "add(replay_remit, 0)"},
                    {"replay_remit": [50, 275, 350]})
    assert [r["v"] for r in rows] == [50, 275, 350, 0]


def test_scalar_context_stays_scalar_so_lookup_returns_one_value():
    rows = schedule(period("2024-01-01", "2024-03-01", "M"),
                    {"alloc": "lookup(amounts_arr, product_ids, item_name)"},
                    {"amounts_arr": [800.0, 400.0, 250.0],
                     "product_ids": ["PO-A", "PO-B", "PO-C"],
                     "item_name": "PO-B"})
    assert rows[0]["alloc"] == 400.0   # was a list of per-period matches


def test_every_advertised_schedule_builtin_is_bound():
    p = period("2024-01-01", "2024-03-01", "M")
    cols = {n: n for n in (
        "period_date", "period_index", "period_start", "period_number",
        "dcf", "days_in_current_period", "total_periods", "daily_basis",
        "item_name", "subinstrument_id", "s_no", "index",
        "start_date", "end_date")}
    row = schedule(p, cols, {})[0]
    for name in cols:
        assert not str(row[name]).startswith("ERROR"), (name, row[name])
    assert row["total_periods"] == len(p["dates"])
    assert row["period_number"] == 1 and row["s_no"] == 1
    assert row["start_date"] == p["dates"][0]
    assert row["end_date"] == p["dates"][-1]
    assert row["daily_basis"] == 365


def test_subinstrument_id_follows_the_current_row():
    _set_current_subinstrumentid("7")
    try:
        rows = schedule(period("2024-01-01", "2024-02-01", "M"),
                        {"sub": "subinstrument_id"}, {})
        assert rows[0]["sub"] == "7"
    finally:
        _set_current_subinstrumentid("1")


def test_undefined_identifier_in_a_column_still_errors():
    rows = schedule(period("2024-01-01", "2024-02-01", "M"),
                    {"bad": "add(no_such_var, 1)"}, {})
    assert str(rows[0]["bad"]).startswith("ERROR")


# -- per-item (order-grain) schedules --------------------------------------
def test_per_item_schedule_binds_item_name_and_slices_arrays():
    res = generate_schedules(
        [800.0, 400.0, 250.0], ["2026-01-01"] * 3,
        ["2026-03-31", "2026-02-28", "2026-03-31"],
        {"item": "item_name", "sub": "subinstrument_id",
         "alloc": "AllocatedAmounts",
         "n_lines": "array_length(ProductIds_full)"},
        "M",
        {"AllocatedAmounts": [800.0, 400.0, 250.0],
         "ProductIds": ["PO-A", "PO-B", "PO-C"]},
        ["PO-A", "PO-B", "PO-C"], ["1", "2", "3"])
    first_rows = [r["schedule"][0] for r in res]
    assert [r["item"] for r in first_rows] == ["PO-A", "PO-B", "PO-C"]
    assert [r["sub"] for r in first_rows] == ["1", "2", "3"]
    assert [r["alloc"] for r in first_rows] == [800.0, 400.0, 250.0]
    # `_full` reaches the whole array even though context arrays are sliced,
    # and is NOT clobbered by the sliced scalar's broadcast alias.
    assert [r["n_lines"] for r in first_rows] == [3, 3, 3]


def test_canonical_pattern_b_formula_allocates_per_item():
    """The documented pattern used lookup(..., item_name), which silently
    produced 0 for every item because item_name is a display label."""
    rows = schedule(
        period(["2026-01-01"] * 3, ["2026-03-31"] * 3, "M"),
        {"period_revenue": "divide(AllocatedAmounts, total_periods)"},
        {"AllocatedAmounts": [800.0, 400.0, 250.0],
         "ProductIds": ["PO-A", "PO-B", "PO-C"]})
    got = [round(r["schedule"][0]["period_revenue"], 4) for r in rows]
    assert got == [266.6667, 133.3333, 83.3333]
