"""Regression tests for the DSL iteration primitives.

Reported symptoms, all of them silent wrong answers:

  * apply_each "evaluates once and broadcasts"; `each` bound to the whole array
  * for_each returns []
  * index is stuck at 0
  * array_get(arr, n, default) ignores n

Four distinct causes:

1. The loop's own bindings were applied BEFORE the caller-supplied context, so
   a rule with a step named `index`, `count`, or the loop variable silently
   clobbered the per-element values. `each` became the whole source array, the
   formula evaluated once and broadcast over it, and `index` froze -- which in
   turn made array_get(arr, index, d) keep returning the same slot.
2. The rule builder emitted for_each(src, [], ...) for a single-array loop, and
   min(len(src), 0) == 0 returned [] without a word.
3. array_get/array_length/array_first/... tested emptiness with `not array`.
   _RowAwareArray answers truthiness with the CURRENT ROW's scalar, so a
   context array whose row value was 0 looked empty inside a schedule column.
4. array_get rejected a float index, and every DSL number is a float.

Plus: safe_eval_expression ran with __builtins__=None, so an undefined variable
raised "'NoneType' object is not subscriptable" instead of a NameError.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401  (registers DSL_FUNCTIONS)
from backend.agent import tools as T  # noqa: E402
from backend.dsl_functions import (  # noqa: E402
    _RowAwareArray,
    apply_each,
    array_filter,
    array_first,
    array_get,
    array_last,
    array_length,
    array_slice,
    for_each,
    for_each_with_index,
    safe_eval_expression,
)
from backend.server import (  # noqa: E402
    dsl_to_python_standalone,
    execute_python_template,
)

ARR = [10, 20, 30]
OTHER = [1, 2, 3]


def _run_rule(steps):
    """Generate a rule's DSL exactly like the agent does, then execute it."""
    code = T._generate_rule_code({"id": "r", "name": "t", "steps": steps,
                                  "outputs": {}})
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(execute_python_template(
            dsl_to_python_standalone(code), [{}], {}, None, None))
    finally:
        loop.close()
    return code, out


def _printed(out):
    return " ".join(str(p).replace("\n", "") for p in out.get("print_outputs") or [])


BASE = [{"name": "arr", "stepType": "calc", "source": "value", "value": "[10, 20, 30]"},
        {"name": "other", "stepType": "calc", "source": "value", "value": "[1, 2, 3]"}]
SHOW = {"name": "d", "stepType": "calc", "source": "formula",
        "formula": 'print("R", res)'}


# -- 1. the loop owns its bindings -----------------------------------------
def test_external_context_cannot_shadow_the_loop_index():
    assert for_each_with_index(
        ARR, "each", "array_get(other, index, 0)",
        {"other": OTHER, "index": 0}) == [1, 2, 3]


def test_external_context_cannot_shadow_the_loop_variable():
    """`each` used to become the whole array: the formula then evaluated once
    and _broadcast_binary spread it, so every slot held the same list."""
    assert for_each_with_index(
        ARR, "each", "multiply(each, 2)", {"each": ARR}) == [20, 40, 60]


def test_external_context_cannot_shadow_count():
    assert for_each_with_index(ARR, "each", "count", {"count": 99}) == [3, 3, 3]


def test_array_filter_loop_bindings_win_too():
    assert array_filter(ARR, "x", "gt(array_get(other, index, 0), 1)",
                        {"other": OTHER, "index": 0}) == [20, 30]


def test_paired_apply_each_bindings_win():
    assert apply_each(ARR, OTHER, "multiply(first, second)",
                      {"first": 999, "index": 0}) == [10, 40, 90]


@pytest.mark.parametrize("shadow", ["index", "each", "count"])
def test_rule_with_a_step_named_like_a_loop_binding(shadow):
    """End-to-end: a rule may legitimately define a step called `index`."""
    steps = BASE + [
        {"name": shadow, "stepType": "calc", "source": "value", "value": "0"},
        {"name": "res", "stepType": "iteration", "iterations": [
            {"type": "apply_each", "sourceArray": "arr",
             "expression": "array_get(other, index, 0)", "resultVar": "res"}]},
        SHOW]
    code, out = _run_rule(steps)
    assert f'"{shadow}": {shadow}' not in code, code
    assert "1" in _printed(out) and "3" in _printed(out), _printed(out)


# -- 2. for_each with a single array ---------------------------------------
def test_for_each_with_only_one_array_iterates_it():
    assert for_each(ARR, [], "item", "second", "multiply(item, 2)") == [20, 40, 60]


def test_for_each_paired_still_pairs():
    assert for_each(["a", "b"], [1, 2], "d", "amt",
                    "concat(d, str(amt))") == ["a1", "b2"]


def test_single_array_iteration_step_emits_the_right_primitive():
    code, out = _run_rule(BASE + [
        {"name": "res", "stepType": "iteration", "iterations": [
            {"type": "for_each", "sourceArray": "arr", "varName": "item",
             "expression": "multiply(item, 2)", "resultVar": "res"}]},
        SHOW])
    assert "for_each_with_index(arr" in code, code
    assert "20" in _printed(out) and "60" in _printed(out)


def test_for_each_passes_outer_context_to_the_formula():
    assert for_each(ARR, OTHER, "a", "b", "multiply(a, rate)",
                    {"rate": 10}) == [100, 200, 300]


def test_for_each_raises_when_every_iteration_fails():
    """A formula typo used to yield [] — indistinguishable from no data."""
    with pytest.raises(ValueError, match="every iteration failed"):
        for_each(ARR, OTHER, "a", "b", "multiply(a, nope)")


def test_for_each_on_genuinely_empty_input_is_still_quiet():
    assert for_each([], [], "a", "b", "multiply(a, b)") == []


# -- 3. row-aware arrays are not "empty" when the row value is falsy --------
@pytest.mark.parametrize("row_value", [0, "", 0.0])
def test_row_aware_array_is_not_mistaken_for_empty(row_value):
    a = _RowAwareArray(ARR, row_value=row_value)
    assert array_length(a) == 3
    assert array_get(a, 2, "D") == 30
    assert array_first(a, "D") == 10
    assert array_last(a, "D") == 30
    assert array_slice(a, 1) == [20, 30]


def test_genuinely_empty_still_reads_as_empty():
    assert array_length([]) == 0
    assert array_get([], 0, "D") == "D"
    assert array_first(None, "D") == "D"


# -- 4. float indices ------------------------------------------------------
@pytest.mark.parametrize("idx,expected", [(0.0, 10), (1.0, 20), (2.0, 30)])
def test_array_get_accepts_a_computed_float_index(idx, expected):
    assert array_get(ARR, idx, "D") == expected


def test_array_get_still_bounds_checks():
    assert array_get(ARR, 3.0, "D") == "D"
    assert array_get(ARR, -1.0, "D") == "D"
    assert array_get(ARR, "not a number", "D") == "D"


def test_computed_index_end_to_end():
    code, out = _run_rule(BASE + [
        {"name": "n", "stepType": "calc", "source": "formula", "formula": "divide(4, 2)"},
        {"name": "res", "stepType": "calc", "source": "formula",
         "formula": "array_get(arr, n, 0)"},
        SHOW])
    assert "30" in _printed(out), _printed(out)


# -- apply_each mis-dispatch -----------------------------------------------
def test_unquoted_formula_is_rejected_not_silently_zeroed():
    """apply_each(arr, multiply(each, 2)) evaluates the formula BEFORE the call.
    That landed in the paired-array slot and produced a list of zeros."""
    with pytest.raises(ValueError, match="QUOTED string"):
        apply_each(ARR, [20, 40, 60])


def test_paired_mode_without_a_formula_is_rejected():
    with pytest.raises(ValueError, match="QUOTED formula"):
        apply_each(ARR, OTHER, "")


def test_apply_each_normal_modes_unaffected():
    assert apply_each(ARR, "multiply(each, 2)") == [20, 40, 60]
    assert apply_each(ARR, OTHER, "multiply(first, second)") == [10, 40, 90]


# -- undefined names report their own name ---------------------------------
def test_undefined_variable_raises_a_named_error():
    with pytest.raises(NameError, match="nonexistent_var"):
        safe_eval_expression("multiply(a, nonexistent_var)", {"a": 10})


@pytest.mark.parametrize("attack", [
    "__import__('os').system('x')", "open('x')", "eval('1')", "exec('x')",
])
def test_builtins_remain_unreachable(attack):
    with pytest.raises(Exception):
        safe_eval_expression(attack, {})


def test_ordinary_expressions_still_evaluate():
    assert safe_eval_expression("multiply(add(a, 2), 3)", {"a": 1}) == 9
