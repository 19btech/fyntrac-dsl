"""Regression tests for falsy step flags being dropped by the serializer.

BUG B1 as reported: patch_step(ops=[{"op":"add","path":"/printResult",
"value":false}]) returned ops_applied ok:true but persisted ok:false with
"path '/printResult' not present after save".

Root cause was `if step.get(x):` rather than `if step.get(x) is not None:` in
_validate_step_shape. Copying a field only when it is truthy discards the "off"
state, and every consumer reads an absent key as the default — so the write does
not merely fail, it INVERTS.

Auditing for the whole class turned up two more:

  * `disabled` was never carried at all — not even when true. Any agent edit to
    a disabled step silently re-activated it and the generated code started
    running it again.
  * `inlineComment: false` was dropped, and `commentText` went with it, so
    turning a comment off also destroyed its text.
"""
import asyncio
import copy
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
import backend.agent.tools as T  # noqa: E402

BASE = {"name": "rev_amount", "stepType": "calc", "source": "value", "value": "100"}

# Every boolean flag the step serializer is responsible for carrying.
BOOL_FLAGS = ["printResult", "disabled", "inlineComment"]


@pytest.mark.parametrize("flag", BOOL_FLAGS)
@pytest.mark.parametrize("value", [True, False])
def test_boolean_flag_round_trips(flag, value):
    out = T._validate_step_shape({**BASE, flag: value})
    assert flag in out, f"{flag}={value} was dropped by the serializer"
    assert out[flag] is value


@pytest.mark.parametrize("flag", BOOL_FLAGS)
def test_absent_flag_stays_absent(flag):
    """Absent must stay absent — it is not the same as an explicit False."""
    assert flag not in T._validate_step_shape(dict(BASE))


def test_comment_text_survives_turning_the_comment_off():
    out = T._validate_step_shape(
        {**BASE, "inlineComment": False, "commentText": "keep me"})
    assert out["inlineComment"] is False
    assert out["commentText"] == "keep me"


# -- the flags must actually reach the generated code ----------------------
def _code(step):
    return T._generate_rule_code({"id": "r", "name": "t",
                                  "steps": [step], "outputs": {}})


def test_a_disabled_step_does_not_execute():
    """The serializer dropping `disabled` meant a disabled step ran anyway."""
    code = _code(T._validate_step_shape({**BASE, "disabled": True}))
    assert "# [DISABLED] rev_amount = 100" in code
    assert not any(l.strip().startswith("rev_amount = 100")
                   for l in code.split("\n"))


def test_an_enabled_step_still_executes():
    code = _code(T._validate_step_shape({**BASE, "disabled": False}))
    assert any(l.strip() == "rev_amount = 100" for l in code.split("\n"))


def test_print_result_false_suppresses_the_print():
    code = _code(T._validate_step_shape({**BASE, "printResult": False}))
    assert 'print("rev_amount =' not in code


# -- the reported repro, through the real patch_step path ------------------
@pytest.fixture
def patch_env(monkeypatch):
    """Run tool_patch_step against an in-memory rule store."""
    store = {"id": "r1", "name": "REVREC", "outputs": {}, "steps": [
        T._validate_step_shape({"name": "rev_schedule", "stepType": "calc",
                                "source": "value", "value": "1"})]}

    async def fake_load(rid):
        return copy.deepcopy(store)

    async def fake_save(rule, *, is_new, snapshot=True):
        store.update(copy.deepcopy(rule))
        return copy.deepcopy(store)

    async def no_events(rule):
        return []

    async def no_multi(steps):
        return []

    monkeypatch.setattr(T, "_load_rule", fake_load)
    monkeypatch.setattr(T, "_save_rule_doc", fake_save)
    monkeypatch.setattr(T, "_events_referenced_by_rule", no_events)
    monkeypatch.setattr(T, "_detect_multi_subid_events", no_multi)
    return store


def _patch(ops):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(T.tool_patch_step(
            {"rule_id": "r1", "step_name": "rev_schedule", "ops": ops}))
    finally:
        loop.close()


@pytest.mark.parametrize("flag,value", [
    ("printResult", False),      # the exact reported repro
    ("printResult", True),
    ("disabled", True),
    ("disabled", False),
    ("inlineComment", False),
])
def test_patch_step_persists_the_flag(patch_env, flag, value):
    res = _patch([{"op": "add", "path": f"/{flag}", "value": value}])
    persisted = res.get("persisted", {})
    assert persisted.get("ok") is True, persisted.get("mismatches")
    assert not persisted.get("mismatches")
    assert patch_env["steps"][0][flag] is value


def test_patch_step_reported_success_matches_reality(patch_env):
    """ops_applied said ok while the read-back said not-present. Both must
    agree now — silently accepting and discarding is the worst outcome."""
    res = _patch([{"op": "add", "path": "/printResult", "value": False}])
    assert all(o["ok"] for o in res["ops_applied"])
    assert res["persisted"]["ok"] is True
    assert patch_env["steps"][0]["printResult"] is False
