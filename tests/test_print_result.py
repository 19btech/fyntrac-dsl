"""Regression tests for printResult — the log-volume blocker.

Three independent defects made it impossible to silence a rule:

1. The schedule step emitted `print(<Sched>)` UNCONDITIONALLY, dumping the
   whole grid — every period of every instrument — on every run. At ~6KB per
   instrument that is hundreds of MB on a real portfolio, and nothing could
   turn it off.
2. `outputs.printResult` was stored, and advertised in the tool schema, but no
   code generator ever read it. Setting it to false did nothing at all.
3. `_validate_step_shape` wrote printResult only when truthy, so an explicit
   `printResult: false` was dropped from the step entirely — and an absent key
   reads as "print" downstream. Every patch_step that tried to switch printing
   off silently switched it back on.

Plus: update_saved_rule re-ran a full schedule preview for every schedule step
on EVERY patch, including one that only touched `outputs` — which is what made
that patch time out.
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
from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    execute_python_template,
    merge_event_data_by_instrument,
)

EVT = "REV"


def _schedule_step(**over):
    sc, outs = T._validate_schedule_step_shape("Sched", {
        "periodType": "date", "frequency": "M",
        "startDateSource": "value", "startDate": "2026-01-01",
        "endDateSource": "value", "endDate": "2026-06-01",
        "columns": [
            {"name": "period_date", "formula": "period_date"},
            {"name": "month_end", "formula": "end_of_month(period_date)"},
            {"name": "amount", "formula": "divide(base_amt, total_periods)"}]},
        [{"name": "Total", "type": "sum", "column": "amount"}])
    step = {"name": "Sched", "stepType": "schedule",
            "scheduleConfig": sc, "outputVars": outs}
    step.update(over)
    return step


def _code(rule_print=None, step_print=None, calc_print=True):
    calc = {"name": "base_amt", "stepType": "calc", "source": "event_field",
            "eventField": f"{EVT}.amt", "printResult": calc_print}
    step = _schedule_step() if step_print is None else _schedule_step(printResult=step_print)
    outputs = {"transactions": []}
    if rule_print is not None:
        outputs["printResult"] = rule_print
    return T._generate_rule_code({"id": "r", "name": "rec",
                                  "steps": [calc, step], "outputs": outputs})


# -- the schedule grid dump is now switchable ------------------------------
def test_grid_dump_still_happens_by_default():
    """Previews depend on it, so absent printResult must keep printing."""
    assert "print(Sched)" in _code()


def test_rule_level_false_silences_the_grid():
    assert "print(Sched)" not in _code(rule_print=False)


def test_rule_level_true_is_the_same_as_absent():
    assert "print(Sched)" in _code(rule_print=True)


def test_step_level_false_silences_just_that_schedule():
    code = _code(step_print=False)
    assert "print(Sched)" not in code
    assert 'print("base_amt =' in code      # other steps unaffected


def test_rule_level_false_silences_every_step_print():
    code = _code(rule_print=False)
    assert "print(" not in code, code


# -- printResult: false survives normalisation -----------------------------
@pytest.mark.parametrize("value", [True, False])
def test_explicit_print_result_is_preserved(value):
    out = T._validate_step_shape({"name": "x", "stepType": "calc",
                                  "source": "value", "value": "1",
                                  "printResult": value})
    assert out["printResult"] is value


def test_absent_print_result_stays_absent():
    out = T._validate_step_shape({"name": "x", "stepType": "calc",
                                  "source": "value", "value": "1"})
    assert "printResult" not in out


def test_patching_a_step_to_false_survives_a_round_trip():
    """The reported symptom: patch_step stripped printResult: false."""
    step = T._validate_step_shape(_schedule_step(printResult=False))
    assert step["printResult"] is False
    code = T._generate_rule_code({"id": "r", "name": "rec", "steps": [step],
                                  "outputs": {"transactions": []}})
    assert "print(Sched)" not in code


# -- it actually suppresses output at run time -----------------------------
ROWS = [{"instrumentid": f"I{i}", "subinstrumentid": "1",
         "postingdate": "2026-01-31", "effectivedate": "2026-01-31",
         "amt": 1000.0} for i in range(5)]
FIELDS = {EVT: {"eventType": "activity",
                "fields": [{"name": "amt", "datatype": "decimal"}]}}


def _run(code):
    loop = asyncio.new_event_loop()
    try:
        out = loop.run_until_complete(execute_python_template(
            dsl_to_python_multi_event(code, FIELDS),
            merge_event_data_by_instrument({EVT: [dict(r) for r in ROWS]}),
            {EVT: [dict(r) for r in ROWS]}, None, None))
    finally:
        loop.close()
    return sum(len(str(p)) for p in out.get("print_outputs") or []), out


def test_default_run_emits_the_grid():
    chars, _ = _run(_code())
    assert chars > 1000, chars


def test_silenced_run_emits_nothing():
    chars, out = _run(_code(rule_print=False))
    assert chars == 0
    # the rule still computes: outputVars are unaffected by printing
    assert out["print_outputs"] == []


def test_silencing_does_not_change_transactions():
    calc = {"name": "base_amt", "stepType": "calc", "source": "event_field",
            "eventField": f"{EVT}.amt"}
    def build(rp):
        outputs = {"transactions": [
            {"type": "Rev", "amount": "Total",
             "postingDate": f"{EVT}.postingdate",
             "effectiveDate": f"{EVT}.effectivedate"}]}
        if rp is not None:
            outputs["printResult"] = rp
        return T._generate_rule_code({"id": "r", "name": "rec",
                                      "steps": [calc, _schedule_step()],
                                      "outputs": outputs})
    _, loud = _run(build(None))
    _, quiet = _run(build(False))
    assert [t.amount for t in loud["transactions"]] == \
           [t.amount for t in quiet["transactions"]]
    assert len(quiet["transactions"]) == len(ROWS)


# -- update_saved_rule no longer re-tests schedules for free ---------------
def test_update_skips_schedule_previews_when_no_step_changed():
    src = inspect.getsource(T.tool_update_saved_rule)
    assert '_steps_touched = "steps" in patch' in src
    assert re.search(r"if _steps_touched:\s*\n\s*sched_results = await _auto_test_schedule_steps",
                     src), src
    assert "schedule_tests_skipped" in src


def test_create_still_always_tests_schedules():
    """A brand-new rule has never been previewed, so it must still be."""
    src = inspect.getsource(T.tool_create_saved_rule)
    assert "sched_results = await _auto_test_schedule_steps(rule)" in src
    assert "_steps_touched" not in src
