"""Regression tests for the second wave of reported engine bugs.

BUG 1  lookup() returned the wrong row for string keys.
       Not a positional join: normalize_date() -- used to make comparison
       type-agnostic -- split ANY string on the first 'T' or space to strip an
       ISO time part. 'COS_PRTDIG_MGRT_US_New_15_1200_0300_Print' became
       'COS_PR', so every product code sharing that 6-char stub collapsed onto
       one key and lookup() matched the first of them.

BUG 8  eq(array, scalar) collapsed to a single False while arithmetic
       vectorised, so there was no element-wise mask to build a sum-product
       join from.

BUG 14 collect_all('REF_EVENT_field') returned an array of blanks sized to the
       ACTIVITY row count when the reference event was named only inside a
       quoted string (nothing detects the reference, so it is never loaded).

BUG 15 create_saved_rule / update_saved_rule / add_step_to_rule / update_step
       called _save_rule_doc BEFORE the scalar-source validation that raises
       "blocked", so a rejected create was already in the database and the
       retry failed with "a rule named X already exists".
"""
import asyncio
import inspect
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.agent import tools as T  # noqa: E402
from backend.dsl_functions import (  # noqa: E402
    DSL_FUNCTIONS,
    _RowAwareArray,
    eq,
    gt,
    if_op,
    lookup,
    multiply,
    neq,
    normalize_date,
)
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    execute_python_template,
    merge_event_data_by_instrument,
)

CODES = ["COS_PRTDIG_MGRT_US_New_15_1200_0300_Digital",
         "COS_PRTDIG_MGRT_US_New_15_1200_0300",
         "COS_PRTDIG_MGRT_US_New_15_1200_0300_Print"]
CAT_PRODUCT = ["A1", "A2", "A3"] + CODES + ["C1", "C2", "C3"]
CAT_SSP = [1.0, 2.0, 2.5, 3.0, 0.0, 12.0, 7.0, 8.0, 9.0]


# -- BUG 1: normalize_date must not mangle non-dates -----------------------
@pytest.mark.parametrize("value", CODES + [
    "PO-A", "New York Branch", "RATABLE", "T", "PRT", "Total Revenue",
])
def test_non_dates_pass_through_untouched(value):
    assert normalize_date(value) == value


@pytest.mark.parametrize("value,expected", [
    ("2026-01-31", "2026-01-31"),
    ("2026-01-31T00:00:00", "2026-01-31"),
    ("2026-01-31T10:30:00Z", "2026-01-31"),
    ("2026-01-31 10:30:00", "2026-01-31"),
    ("01/31/2026", "2026-01-31"),
])
def test_real_dates_still_normalise(value, expected):
    assert normalize_date(value) == expected


@pytest.mark.parametrize("target,expected", [
    (CODES[2], 12.0),   # the reported repro: expected 12.0, got 3.0
    (CODES[0], 3.0),
    (CODES[1], 0.0),
    ("A2", 2.0),
    ("no-such-code", None),
])
def test_lookup_matches_the_requested_key(target, expected):
    assert lookup(CAT_SSP, CAT_PRODUCT, target) == expected


def test_lookup_still_matches_dates_across_formats():
    assert lookup([10, 20], ["2026-01-31T00:00:00", "2026-02-28"],
                  "2026-01-31") == 10


def test_apply_each_over_lookup_gives_per_element_results():
    """The reported BUG 2 ([3.0, 3.0, 3.0]) was entirely a symptom of BUG 1."""
    assert DSL_FUNCTIONS["apply_each"](
        CODES, "lookup(cat_ssp, cat_product, each)",
        {"cat_ssp": CAT_SSP, "cat_product": CAT_PRODUCT}) == [3.0, 0.0, 12.0]


def test_totals_are_no_longer_corrupted():
    """TotalSSP came out 9.00 instead of 15.00 because every line looked up the
    same catalog row."""
    per_line = DSL_FUNCTIONS["apply_each"](
        CODES, "lookup(cat_ssp, cat_product, each)",
        {"cat_ssp": CAT_SSP, "cat_product": CAT_PRODUCT})
    assert DSL_FUNCTIONS["sum"](per_line) == 15.0


# -- BUG 8: comparisons vectorise like arithmetic --------------------------
def test_comparison_against_an_array_returns_a_mask():
    assert eq(["A", "B", "C"], "B") == [False, True, False]
    assert neq(["A", "B", "C"], "B") == [True, False, True]
    assert gt([0.0, 15.0, 0.0], 1) == [False, True, False]


def test_scalar_comparison_unchanged():
    assert eq("B", "B") is True
    assert eq(1, 2) is False


def test_array_vs_array_stays_whole_object_equality():
    """Deliberately NOT vectorised — changing it would alter existing rules."""
    assert eq(["A", "B"], ["A", "B"]) is True
    assert eq(["A", "B"], ["A", "C"]) is False


def test_row_aware_array_still_compares_as_the_current_row():
    """Schedule column semantics must not change."""
    raa = _RowAwareArray(["RATABLE", "POINT"], row_value="RATABLE")
    assert eq(raa, "RATABLE") is True
    assert eq(raa, "POINT") is False


def test_sum_product_join_now_possible():
    prods = ["A", "B", "C"]
    prices = [0.0, 15.0, 0.0]
    mask = eq(prods, "B")
    assert DSL_FUNCTIONS["sum"](multiply(mask, prices)) == 15.0


def test_arithmetic_vectorisation_unchanged():
    assert multiply([0.0, 15.0, 0.0], 2) == [0.0, 30.0, 0.0]


def test_if_refuses_a_mask_instead_of_picking_a_branch():
    with pytest.raises(ValueError, match="array of 3 values"):
        if_op(eq(["A", "B", "C"], "B"), 1, 0)
    assert if_op(eq("B", "B"), 1, 0) == 1


# -- BUG 14: a collect for a field nothing supplies must not invent blanks --
ACT = "SALE_ORDER_DETAILS"
REF = "SSP_RULE"
ACT_ROWS = [{"instrumentid": "SO-1", "subinstrumentid": str(i + 1),
             "postingdate": "2026-01-31", "effectivedate": "2026-01-31",
             "product_id": f"P{i}"} for i in range(3)]
REF_ROWS = [{"product_code": f"P{i}", "ssp_amount": float(i)} for i in range(3)]
ACT_FIELDS = {ACT: {"eventType": "activity",
                    "fields": [{"name": "product_id", "datatype": "string"}]}}


def _exec(dsl, fields, raw, act=None):
    py = dsl_to_python_multi_event(dsl, fields)
    merged = merge_event_data_by_instrument({ACT: act if act is not None else ACT_ROWS})
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            execute_python_template(py, merged, raw, None, None))
    finally:
        loop.close()


def test_quoted_reference_event_raises_instead_of_returning_blanks():
    with pytest.raises(Exception, match="no loaded event supplies"):
        _exec("x = collect_all('SSP_RULE_ssp_amount')\nprint('x', x)\n",
              ACT_FIELDS, {ACT: ACT_ROWS})


def test_dotted_reference_event_loads_and_works():
    fields = dict(ACT_FIELDS)
    fields[REF] = {"eventType": "reference", "fields": [
        {"name": "product_code", "datatype": "string"},
        {"name": "ssp_amount", "datatype": "decimal"}]}
    out = _exec(f"x = collect_all({REF}.ssp_amount)\nprint('x', x)\n",
                fields, {ACT: ACT_ROWS, REF: REF_ROWS})
    printed = " ".join(str(p) for p in out.get("print_outputs") or [])
    assert "0.0" in printed and "2.0" in printed


def test_misspelled_field_is_caught():
    with pytest.raises(Exception, match="no loaded event supplies"):
        _exec(f"x = collect_by_instrument({ACT}.prodcut_id)\nprint('x', x)\n",
              ACT_FIELDS, {ACT: ACT_ROWS})


def test_no_matching_rows_is_still_quiet():
    """The guard fires only when rows were SCANNED and the field was on none of
    them. No rows for THIS instrument is ordinary "no data" -> [], no error."""
    other = [dict(r, instrumentid="SOMEONE-ELSE") for r in ACT_ROWS]
    out = _exec(f"x = collect_by_instrument({ACT}.product_id)\nprint('x', x)\n",
                ACT_FIELDS, {ACT: other})
    assert "[]" in " ".join(str(p) for p in out.get("print_outputs") or [])


# -- BUG 15: never persist before validating -------------------------------
@pytest.mark.parametrize("fn_name", [
    "tool_create_saved_rule", "tool_update_saved_rule",
    "tool_add_step_to_rule", "tool_update_step",
])
def test_scalar_source_check_runs_before_the_rule_is_saved(fn_name):
    """A tool that reports 'blocked' must not have written anything first."""
    src = inspect.getsource(getattr(T, fn_name))
    code = [ln for ln in src.split("\n") if not ln.strip().startswith("#")]
    save = next((i for i, ln in enumerate(code) if "_save_rule_doc" in ln), None)
    guard = next((i for i, ln in enumerate(code)
                  if "_raise_if_scalar_on_multi_subid" in ln
                  or "SCALAR SOURCE ON NON-SCALAR" in ln), None)
    assert guard is not None, f"{fn_name}: no scalar-source guard found"
    assert save is not None, f"{fn_name}: no save found"
    assert guard < save, (
        f"{fn_name}: saves at line {save} before validating at line {guard} — "
        "a rejected call would leave the rule persisted")


def test_block_message_says_nothing_was_saved():
    src = inspect.getsource(T._raise_if_scalar_on_multi_subid)
    assert "Nothing has been saved" in src
