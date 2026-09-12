"""Regression tests for the update_saved_rule hang and the outputs hygiene items.

BUG A — update_saved_rule never returned on a rule with a materialised
schedule. tool_test_schedule_step probed the columns INCREMENTALLY: it rebuilt
the rule with columns[:k] and re-executed the whole thing for every k, then ran
once more for the full set. A 16-column schedule therefore executed the rule 17
times, and _auto_test_schedule_steps does that for EVERY schedule step on every
create / update / finish. Two fixes:
  * the full column set runs ONCE; the per-column bisect is paid for only when
    there is a failure to localise;
  * update_saved_rule skips the schedule previews entirely when the patch did
    not touch any step (the reported repro patched only `outputs`).

Hygiene — unknown keys on outputs.transactions[] were carried along and then
ignored, so `skipIfZero: true` looked accepted but did nothing.
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


# -- BUG A: one execution on the happy path --------------------------------
def _rule_with_columns(n_cols):
    cols = [{"name": "period_date", "formula": "period_date"}]
    cols += [{"name": f"c{i}", "formula": "period_index"} for i in range(n_cols - 1)]
    sc, outs = T._validate_schedule_step_shape("rev_schedule", {
        "periodType": "number", "frequency": "M", "periodCount": 12,
        "columns": cols}, [])
    return {"id": "r1", "name": "REVREC", "outputs": {}, "steps": [
        {"name": "rev_schedule", "stepType": "schedule",
         "scheduleConfig": sc, "outputVars": outs}]}


@pytest.fixture
def counted(monkeypatch):
    """Count full rule executions performed by tool_test_schedule_step."""
    calls = {"n": 0, "fail": False}

    async def fake_exec(rule, code, posting_date, effective_date, extra_events=None):
        calls["n"] += 1
        if calls["fail"]:
            return {"success": False, "error": "boom"}, 0
        return {"success": True, "print_outputs": []}, 1

    async def fake_events(rule):
        return []

    monkeypatch.setattr(T, "_execute_dsl_for_rule", fake_exec)
    monkeypatch.setattr(T, "_events_referenced_by_rule", fake_events)
    return calls


def _run(rule, counted):
    async def go():
        monkey_rule = rule

        async def fake_load(rid):
            return monkey_rule
        import backend.agent.tools as mod
        orig = mod._load_rule
        mod._load_rule = fake_load
        try:
            return await T.tool_test_schedule_step(
                {"rule_id": "r1", "step_index": 0, "sample_limit": 3})
        finally:
            mod._load_rule = orig
    return asyncio.new_event_loop().run_until_complete(go())


@pytest.mark.parametrize("n_cols", [1, 4, 16])
def test_passing_schedule_executes_the_rule_exactly_once(n_cols, counted):
    res = _run(_rule_with_columns(n_cols), counted)
    assert res["ok"] is True, res
    assert counted["n"] == 1, (
        f"{n_cols} columns caused {counted['n']} full rule executions; "
        "the happy path must cost exactly one")
    # every column still reported
    assert len(res["column_results"]) == n_cols
    assert all(c["ok"] for c in res["column_results"])


def test_failing_schedule_still_localises_to_a_column(counted):
    counted["fail"] = True
    res = _run(_rule_with_columns(4), counted)
    assert res["ok"] is False
    assert res["failed_at"] == "column"
    assert res["failed_column"] == "period_date"    # first column probed
    # 1 full run + the bisect probes
    assert counted["n"] > 1


# -- BUG A: update skips previews when no step changed ---------------------
def test_update_skips_previews_for_an_outputs_only_patch():
    src = inspect.getsource(T.tool_update_saved_rule)
    assert '_steps_touched = "steps" in patch' in src
    assert "if _steps_touched:" in src
    assert "schedule_tests_skipped" in src
    # the auto-test must sit INSIDE the conditional, not beside it
    guarded = re.search(
        r"if _steps_touched:\s*\n\s*sched_results = await _auto_test_schedule_steps",
        src)
    assert guarded, src


# -- hygiene: unknown transaction keys are rejected ------------------------
STEPS = [{"name": "rev_amount", "stepType": "calc", "source": "value", "value": "100"}]


def _txn(**extra):
    t = {"type": "Revenue", "amount": "rev_amount",
         "postingDate": "postingdate", "effectiveDate": "effectivedate"}
    t.update(extra)
    return {"transactions": [t]}


def test_unknown_transaction_key_is_rejected():
    with pytest.raises(Exception, match="unknown propert"):
        T._normalise_transaction_outputs(STEPS, _txn(skipIfZero=True))


def test_error_names_the_offending_key_and_the_allowed_set():
    with pytest.raises(Exception) as ei:
        T._normalise_transaction_outputs(STEPS, _txn(skipIfZero=True, foo=1))
    msg = str(ei.value)
    assert "skipIfZero" in msg and "foo" in msg
    assert "subInstrumentId" in msg      # lists what IS allowed


@pytest.mark.parametrize("key", ["type", "amount", "postingDate",
                                 "effectiveDate", "subInstrumentId", "disabled"])
def test_canonical_keys_are_accepted(key):
    out = T._normalise_transaction_outputs(STEPS, _txn(**{key: "x"}))
    assert out["transactions"]


def test_snake_case_aliases_still_accepted():
    out = T._normalise_transaction_outputs(STEPS, {"transactions": [
        {"type": "R", "amount": "rev_amount", "posting_date": "postingdate",
         "effective_date": "effectivedate", "sub_instrument_id": "1"}]})
    assert set(out["transactions"][0]) == {
        "type", "amount", "postingDate", "effectiveDate", "subInstrumentId"}


def test_legacy_side_is_still_tolerated_and_dropped():
    """update_transaction deliberately drops `side` for old callers; the
    unknown-key check must not turn that into a hard error."""
    out = T._normalise_transaction_outputs(STEPS, _txn(side="debit"))
    assert "side" not in out["transactions"][0]


# -- hygiene: destructive wipe is attributable -----------------------------
def test_clear_all_data_reports_what_it_destroyed():
    src = inspect.getsource(T.tool_clear_all_data)
    assert "deleted_counts" in src
    assert "logger.warning" in src
    assert "confirm" in src          # still gated
