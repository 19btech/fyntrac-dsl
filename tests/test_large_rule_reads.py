"""Large rules must be readable and debuggable through the tools.

Every MCP tool response was cut at 12,000 chars. REVREC_Revenue_Recognition
(94 steps, ~86K chars) therefore lost everything past ~step 60:
  * get_saved_rule never reached the steps array;
  * debug_step put the generated code BEFORE the debug value, so for a late
    step the value was always cut off;
  * update_step / patch_step never echoed the step, so a step could be edited
    without ever being read;
  * dry_run_rule's print_outputs was always empty, because the rule's master
    print switch (outputs.printResult: false) silenced the step-level flags.
"""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.agent import tools as T  # noqa: E402
from tests.fake_mongo import FakeDB  # noqa: E402

EVT = "DELIVERY"
N_STEPS = 40


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _steps():
    steps = [{"id": "s0", "name": "postingdate", "stepType": "calc",
              "source": "event_field", "eventField": f"{EVT}.postingdate"},
             {"id": "s1", "name": "units", "stepType": "calc",
              "source": "event_field", "eventField": f"{EVT}.units"}]
    for i in range(2, N_STEPS):
        steps.append({"id": f"s{i}", "name": f"v{i}", "stepType": "calc",
                      "source": "formula", "formula": f"units + {i}"})
    steps[5]["printResult"] = True
    return steps


@pytest.fixture
def bridge(monkeypatch):
    rows = [{"instrumentid": "I1", "subinstrumentid": "1", "postingdate": "2026-01-31",
             "effectivedate": "2026-01-31", "units": 1.0}]
    fake = FakeDB(
        event_definitions=[{"event_name": EVT, "eventType": "activity",
                            "eventTable": "standard",
                            "fields": [{"name": "units", "datatype": "decimal"}]}],
        event_data=[{"event_name": EVT, "data_rows": rows}],
        saved_rules=[{"id": "r1", "name": "Big", "steps": _steps(),
                      "outputs": {"printResult": False, "createTransaction": False,
                                  "transactions": []}}],
    )
    monkeypatch.setattr(T._ServerBridge, "db", fake)
    return fake


# -- get_step -------------------------------------------------------------
@pytest.mark.parametrize("ident", [{"step_id": "s37"}, {"step_name": "v37"},
                                   {"step_index": 37}])
def test_get_step_returns_one_full_step(bridge, ident):
    out = _run(T.tool_get_step({"rule_id": "r1", **ident}))
    assert out["step_index"] == 37 and out["step_count"] == N_STEPS
    assert out["step"]["formula"] == "units + 37"


def test_get_step_is_exposed_over_mcp():
    import backend.mcp_server as M
    assert "get_step" in M._EXPOSED_NAMES


# -- get_saved_rule windows -----------------------------------------------
def test_full_rule_is_unchanged_by_default(bridge):
    out = _run(T.tool_get_saved_rule({"rule_id": "r1"}))
    assert set(out) == {"rule"} and len(out["rule"]["steps"]) == N_STEPS


def test_step_window_returns_only_those_steps_with_indexes(bridge):
    out = _run(T.tool_get_saved_rule({"rule_id": "r1", "from_index": 30, "to_index": 34}))
    assert [s["index"] for s in out["steps"]] == [30, 31, 32, 33, 34]
    assert out["steps"][0]["name"] == "v30"
    assert "outputs" not in out and out["step_count"] == N_STEPS


def test_steps_only_returns_every_step_and_nothing_else(bridge):
    out = _run(T.tool_get_saved_rule({"rule_id": "r1", "steps_only": True}))
    assert len(out["steps"]) == N_STEPS and "outputs" not in out


def test_window_is_clamped_to_the_rule(bridge):
    out = _run(T.tool_get_saved_rule({"rule_id": "r1", "from_index": 35, "to_index": 999}))
    assert out["to_index"] == N_STEPS - 1 and len(out["steps"]) == 5


# -- debug_step: values before code ---------------------------------------
def test_debug_value_comes_before_code(bridge):
    out = _run(T.tool_debug_step({"rule_id": "r1", "step_name": "v37",
                                  "posting_date": "2026-01-31"}))
    text = json.dumps(out)
    assert text.index('"debug_outputs"') < text.index('"code"')
    assert "38.0" in " ".join(map(str, out["debug_outputs"]))


def test_debug_step_can_omit_code(bridge):
    out = _run(T.tool_debug_step({"rule_id": "r1", "step_name": "v37",
                                  "posting_date": "2026-01-31", "include_code": False}))
    assert "code" not in out and out["debug_outputs"]


# -- edits echo the saved step --------------------------------------------
def test_update_step_returns_the_saved_step(bridge):
    out = _run(T.tool_update_step({"rule_id": "r1", "step_id": "s37",
                                   "formula": "units + 100"}))
    assert out["step"]["formula"] == "units + 100" and out["step"]["id"] == "s37"


def test_patch_step_returns_the_saved_step(bridge):
    out = _run(T.tool_patch_step({"rule_id": "r1", "step_id": "s37", "ops": [
        {"op": "replace", "path": "/formula", "value": "units + 200"}]}))
    assert out["step"]["formula"] == "units + 200"


# -- dry_run_rule shows flagged prints even with the master switch off ----
def test_dry_run_prints_flagged_steps_when_rule_printing_is_off(bridge):
    out = _run(T.tool_dry_run_rule({"rule_id": "r1", "posting_date": "2026-01-31"}))
    prints = " ".join(map(str, out["result"]["print_outputs"]))
    assert "v5" in prints, out["result"]["print_outputs"]
    # Only the flagged step -- unflagged steps stay quiet.
    assert "v6" not in prints


def test_dry_run_probe_does_not_change_the_saved_rule(bridge):
    _run(T.tool_dry_run_rule({"rule_id": "r1", "posting_date": "2026-01-31"}))
    rule = _run(T._load_rule("r1"))
    assert rule["outputs"]["printResult"] is False
    assert "printResult" not in rule["steps"][6]


# -- MCP response cap -----------------------------------------------------
def test_mcp_oversized_response_is_saved_in_full(tmp_path, monkeypatch):
    import backend.mcp_server as M
    monkeypatch.setattr(M, "_MAX_RESPONSE_CHARS", 1000)
    monkeypatch.setattr(M, "_SPILL_DIR", str(tmp_path))
    big = {"steps": [{"name": f"v{i}", "formula": "x" * 50} for i in range(200)]}
    text = M._fmt(big, "get_saved_rule")
    assert "truncated" in text and str(tmp_path) in text
    saved = list(tmp_path.iterdir())
    assert len(saved) == 1 and json.loads(saved[0].read_text(encoding="utf-8")) == big


def test_mcp_cap_is_well_above_the_old_12k():
    import backend.mcp_server as M
    assert M._MAX_RESPONSE_CHARS >= 60000
