"""Tests for the DSL expression evaluation caches.

safe_eval_expression runs once per schedule cell and once per iteration
element, so one rule can reach it hundreds of thousands of times in a single
book. It now shares one globals mapping and compiles each distinct expression
once, which took it from ~28us to ~2us per call.

Caching an evaluator is only safe if results still depend solely on the
caller's context, so that is what these tests hammer: the same expression must
give different answers for different data, and nothing may leak between calls.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import dsl_functions as D  # noqa: E402


@pytest.fixture(autouse=True)
def clear_caches():
    D._EVAL_CODE_CACHE.clear()
    D._EVAL_REWRITE_CACHE.clear()
    yield
    D._EVAL_CODE_CACHE.clear()
    D._EVAL_REWRITE_CACHE.clear()


# ── results still follow the data, not the cache ────────────────────────

def test_same_expression_different_context_gives_different_results():
    expr = "multiply(a, b)"
    assert D.safe_eval_expression(expr, {"a": 2, "b": 3}) == 6
    assert D.safe_eval_expression(expr, {"a": 10, "b": 4}) == 40
    assert D.safe_eval_expression(expr, {"a": 0, "b": 99}) == 0


def test_repeated_evaluation_is_stable():
    expr = "add(x, 1)"
    assert [D.safe_eval_expression(expr, {"x": i}) for i in range(5)] == [1, 2, 3, 4, 5]


def test_a_cached_expression_does_not_leak_variables_to_the_next_call():
    D.safe_eval_expression("multiply(a, b)", {"a": 5, "b": 5})
    with pytest.raises(NameError):
        D.safe_eval_expression("multiply(a, b)", {"a": 5})


def test_undefined_variable_still_raises_a_named_error():
    """The empty-builtins mapping exists so this is a NameError naming the
    variable, not 'NoneType is not subscriptable'."""
    with pytest.raises(NameError) as excinfo:
        D.safe_eval_expression("multiply(missing_var, 2)", {})
    assert "missing_var" in str(excinfo.value)


# ── the keyword rewrites survive caching ────────────────────────────────

def test_if_is_rewritten_and_cached():
    expr = "if(gt(x, 10), 100, 200)"
    assert D.safe_eval_expression(expr, {"x": 50}) == 100
    assert D.safe_eval_expression(expr, {"x": 1}) == 200


def test_boolean_keyword_calls_are_rewritten():
    assert D.safe_eval_expression("and(gt(x, 1), lt(x, 10))", {"x": 5}) is True
    assert D.safe_eval_expression("or(gt(x, 100), lt(x, 10))", {"x": 5}) is True
    assert D.safe_eval_expression("not(gt(x, 100))", {"x": 5}) is True


def test_nested_if_inside_a_larger_expression():
    expr = "add(if(gt(x, 0), 10, 20), 1)"
    assert D.safe_eval_expression(expr, {"x": 5}) == 11
    assert D.safe_eval_expression(expr, {"x": -5}) == 21


def test_rewrite_is_reused_not_recomputed():
    expr = "if(gt(x, 1), 1, 0)"
    D.safe_eval_expression(expr, {"x": 5})
    # The lazy top-level if() path recurses, so the cache holds its branches.
    assert len(D._EVAL_REWRITE_CACHE) >= 1
    before = dict(D._EVAL_REWRITE_CACHE)
    D.safe_eval_expression(expr, {"x": 5})
    assert D._EVAL_REWRITE_CACHE == before


# ── the shared globals mapping must stay pristine ───────────────────────

def test_globals_mapping_is_reused():
    D.safe_eval_expression("add(1, 2)", {})
    first = D._eval_globals()
    D.safe_eval_expression("add(3, 4)", {})
    assert D._eval_globals() is first


def test_evaluation_cannot_mutate_the_shared_globals():
    g = D._eval_globals()
    snapshot = set(g)
    D.safe_eval_expression("add(a, 1)", {"a": 1})
    D.safe_eval_expression("multiply(b, 2)", {"b": 3})
    assert set(g) == snapshot


def test_context_shadows_a_dsl_function_without_corrupting_it():
    """A variable named like a function must shadow it for that call only."""
    assert D.safe_eval_expression("add(1, 2)", {}) == 3
    assert D.safe_eval_expression("sum", {"sum": 42}) == 42
    assert D.safe_eval_expression("add(1, 2)", {}) == 3


def test_dsl_functions_are_present_in_the_shared_mapping():
    g = D._eval_globals()
    for name in ("add", "multiply", "subtract", "iif", "and_op", "or_op", "not_op"):
        assert name in g, name


# ── bounds ──────────────────────────────────────────────────────────────

def test_code_cache_is_bounded():
    for i in range(D._EVAL_CACHE_MAX + 50):
        D.safe_eval_expression(f"add({i}, 1)", {})
    assert len(D._EVAL_CODE_CACHE) <= D._EVAL_CACHE_MAX


def test_a_compiled_expression_is_reused():
    expr = "multiply(a, 2)"
    D.safe_eval_expression(expr, {"a": 1})
    code = D._compile_dsl_expression(expr)
    D.safe_eval_expression(expr, {"a": 2})
    assert D._compile_dsl_expression(expr) is code


# ── the schedule path this exists for ───────────────────────────────────

def test_a_schedule_still_computes_the_same_grid():
    columns = {
        "opening": "lag('closing', 1, 1000)",
        "interest": "multiply(opening, 0.01)",
        "closing": "subtract(opening, 100)",
    }
    rows = D.schedule(D.period("2026-01-31", "2026-04-30", "M"), columns, {})
    assert [r["opening"] for r in rows] == [1000, 900, 800, 700]
    assert [r["closing"] for r in rows] == [900, 800, 700, 600]
    assert rows[0]["interest"] == pytest.approx(10.0)


def test_two_schedules_with_different_context_do_not_share_results():
    columns = {"amount": "multiply(base, 2)"}
    first = D.schedule(D.period("2026-01-31", "2026-03-31", "M"), columns, {"base": 5})
    second = D.schedule(D.period("2026-01-31", "2026-03-31", "M"), columns, {"base": 50})
    assert [r["amount"] for r in first] == [10, 10, 10]
    assert [r["amount"] for r in second] == [100, 100, 100]
