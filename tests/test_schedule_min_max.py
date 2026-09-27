"""min()/max() must behave the same inside a schedule column as anywhere else.

The schedule-column evaluation context injects its own built-ins over the DSL
function table. It bound Python's `min`/`max`, which raise
"max() iterable argument is empty" on an empty collection — while the DSL's
own min_val/max_val return 0. A rule that asked for max() of a set that
happened to be empty for one instrument on one date therefore killed that
entire posting date, and only inside schedules.

`sum` was already mapped to the DSL's sum_vals; min/max were missed.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import dsl_functions as D  # noqa: E402


def col(expr, context=None, periods=("2026-01-31", "2026-03-31")):
    """Evaluate one schedule column and return its first row's value."""
    per = D.period(periods[0], periods[1], "M")
    return D.schedule(per, {"c": expr}, context or {})[0]["c"]


# ── the reported failure ────────────────────────────────────────────────

def test_max_of_an_empty_collection_does_not_kill_the_date():
    assert col("max(empty)", {"empty": []}) == 0


def test_min_of_an_empty_collection_does_not_kill_the_date():
    assert col("min(empty)", {"empty": []}) == 0


def test_an_empty_collection_no_longer_raises_inside_a_schedule():
    value = col("max(empty)", {"empty": []})
    assert not (isinstance(value, str) and value.startswith('ERROR'))


# ── and the normal cases still work ─────────────────────────────────────

def test_max_and_min_over_values():
    assert col("max(vals)", {"vals": [3, 9, 4]}) == 9
    assert col("min(vals)", {"vals": [3, 9, 4]}) == 3


def test_multi_argument_form_still_works():
    assert col("max(a, b)", {"a": 5, "b": 9}) == 9
    assert col("min(a, b)", {"a": 5, "b": 9}) == 5


def test_nulls_are_skipped_rather_than_compared():
    assert col("max(vals)", {"vals": [1, None, 7]}) == 7
    assert col("min(vals)", {"vals": [4, None, 2]}) == 2


def test_a_single_scalar_passes_through():
    assert col("max(v)", {"v": 42}) == 42


# ── the property that was broken: same answer in every context ──────────

@pytest.mark.parametrize("expr,ctx", [
    ("max(x)", {"x": []}),
    ("min(x)", {"x": []}),
    ("max(x)", {"x": [2, 8, 5]}),
    ("min(x)", {"x": [2, 8, 5]}),
])
def test_schedule_columns_agree_with_plain_formulas(expr, ctx):
    assert col(expr, ctx) == D.safe_eval_expression(expr, dict(ctx))


def test_sum_was_already_consistent_and_stays_so():
    assert col("sum(x)", {"x": []}) == D.safe_eval_expression("sum(x)", {"x": []})
    assert col("sum(x)", {"x": [1, 2, 3]}) == 6


# ── the compiled template body: the third context ───────────────────────
# Iteration expressions and schedule columns were only two of the three places
# min/max resolve. Ordinary rule code runs inside the generated template, whose
# preamble deliberately restores Python's built-ins over the DSL table — so
# max([]) raised there long after the other two were consistent.

import backend.server as S  # noqa: E402


def template_namespace():
    """The namespace ordinary rule code executes in."""
    py = S.dsl_to_python_multi_event(
        'createTransaction(EV.postingdate, EV.effectivedate, "T", EV.amt)\n',
        {"EV": [{"name": "amt", "datatype": "decimal"}]})
    g = {'__file__': __file__, '__name__': '__t__',
         '__builtins__': S._make_sandbox_builtins()}
    exec(compile(py, '<t>', 'exec'), g)
    return g


@pytest.fixture(scope="module")
def ns():
    return template_namespace()


def test_max_of_empty_returns_zero_in_rule_code(ns):
    assert ns['max']([]) == 0


def test_min_of_empty_returns_zero_in_rule_code(ns):
    assert ns['min']([]) == 0


def test_rule_code_agrees_with_the_other_two_contexts(ns):
    assert ns['max']([]) == col("max(x)", {"x": []}) == D.safe_eval_expression(
        "max(x)", {"x": []})


# ── native Python usage must keep working ───────────────────────────────
# Custom Code steps rely on the real built-ins, which is why the preamble
# restored them in the first place. The guard intercepts only the single
# empty-collection case.

def test_ordinary_collections_are_unaffected(ns):
    assert ns['max']([3, 9, 4]) == 9
    assert ns['min']([3, 9, 4]) == 3


def test_multi_argument_form_reaches_the_builtin(ns):
    assert ns['max'](5, 9) == 9
    assert ns['min'](5, 9) == 5


def test_key_argument_still_works(ns):
    assert ns['max']('ab', 'c', key=len) == 'ab'


def test_generators_still_work(ns):
    assert ns['max'](x for x in [2, 8]) == 8


def test_nulls_are_skipped(ns):
    assert ns['max']([1, None, 7]) == 7


def test_a_non_empty_tuple_behaves_like_a_list(ns):
    assert ns['max']((4, 1, 9)) == 9
    assert ns['max'](()) == 0
