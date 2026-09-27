"""Running a regression suite against the workspace rules.

A case is pinned to the template it was captured with, but the question a
suite usually has to answer is "do the rules I am editing right now still
reproduce every baseline?" — before those rules have been packaged into a
template, and sometimes before they have even been saved.

Two routes into that, and both are covered here:

  * `use_workspace_rules` with no code — the workspace is assembled
    server-side from the saved rules. This is what the MCP connector gets,
    since it has no editor to read from.
  * `use_workspace_rules` with `workspace_code` — the editor's exact buffer,
    replayed as-is. This is the only way an unsaved edit can be tested.

The flag has to override `template_id`, and `use_pinned_code` has to keep
winning over both, or "run the pinned baseline" would silently stop meaning
that the moment someone left the workspace option selected.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402
from backend import regression as R  # noqa: E402
from tests.fake_mongo import FakeDB  # noqa: E402

EVT = "SO_EVENT"

# The baseline: amount straight through.
DSL = (f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
       f'"Rev", {EVT}.amt)\n')

# The workspace has drifted — every amount is doubled. A run against the
# workspace must see this; a run against the pinned template must not.
DSL_DOUBLED = (f'twice = multiply({EVT}.amt, 2)\n'
               f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
               f'"Rev", twice)\n')

# What someone has typed into the editor and not saved — amounts tripled.
DSL_TRIPLED_UNSAVED = (f'thrice = multiply({EVT}.amt, 3)\n'
                       f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
                       f'"Rev", thrice)\n')


def _rows():
    return [{"instrumentid": "SO-1", "subinstrumentid": "1",
             "postingdate": "2026-01-31", "effectivedate": "2026-01-31",
             "amt": 100.0}]


def _mkdb():
    return FakeDB(
        event_definitions=[{
            "event_name": EVT, "eventType": "activity", "eventTable": "standard",
            "fields": [{"name": "amt", "datatype": "decimal"}]}],
        event_data=[{"event_name": EVT, "data_rows": _rows()}],
        user_templates=[
            {"id": "tmpl-base", "name": "Revenue", "combinedCode": DSL},
        ],
        transaction_reports=[],
        saved_rules=[],
    )


@pytest.fixture
def db(monkeypatch):
    fake = _mkdb()
    monkeypatch.setattr(S, "db", fake)
    monkeypatch.setitem(S.in_memory_data, "transaction_reports", [])
    R.configure(
        db=fake,
        run_full_book=S._run_full_book,
        get_combined_code=S.get_combined_code,
        extract_event_names_from_dsl=S.extract_event_names_from_dsl,
        settings=type("Cfg", (), {"require_agent_approval": False})(),
        keep_runs=50,
    )
    return fake


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _Background:
    """Stand-in for FastAPI's BackgroundTasks that runs the task inline."""

    def __init__(self):
        self.tasks = []

    def add_task(self, fn, *args, **kwargs):
        self.tasks.append((fn, args, kwargs))

    async def drain(self):
        for fn, args, kwargs in self.tasks:
            await fn(*args, **kwargs)


def _set_workspace(monkeypatch, code):
    """Make the app's combined-code endpoint report `code` as the workspace."""
    async def _combined():
        return {"code": code}
    monkeypatch.setattr(S, "get_combined_code", _combined)
    R.configure(get_combined_code=_combined)


def capture(template_id="tmpl-base"):
    return run(R.capture_case(R.CaptureCaseRequest(
        name="Base case", template_id=template_id)))


def start(case_id, **kw):
    """Run one case to completion and return (response, batch, this run).

    The run is found by diffing run ids rather than taking the newest:
    two runs in one test land in the same millisecond, and `started_at`
    ties then resolve arbitrarily.
    """
    before = {r["run_id"] for r in run(R.list_runs(case_id=case_id))}

    async def _go():
        bg = _Background()
        started = await R.run_regression(
            R.RunRequest(case_ids=[case_id], **kw), bg)
        await bg.drain()
        return started, await R.get_batch(started["batch_id"])

    started, batch = run(_go())
    fresh = [r for r in run(R.list_runs(case_id=case_id))
             if r["run_id"] not in before]
    assert len(fresh) == 1, "expected exactly one new run, got %d" % len(fresh)
    return started, batch, fresh[0]


def used(run_doc):
    """The label recorded against a run ("Workspace rules (editor . ab12cd34"))."""
    return ((run_doc.get("template_used") or {}).get("name") or "")


# ── the saved workspace ─────────────────────────────────────────────────

def test_workspace_run_uses_saved_rules_not_the_origin_template(db, monkeypatch):
    """With no buffer supplied, the workspace is the saved rules."""
    capture()
    _set_workspace(monkeypatch, DSL_DOUBLED)

    case_id = run(R.list_cases())[0]["id"]
    _, _, latest = start(case_id, use_workspace_rules=True)

    # The baseline holds 100; the workspace now produces 200.
    assert latest["status"] == "failed"
    assert latest["counts"]["changed"] == 1
    assert "Workspace rules" in used(latest)


def test_workspace_flag_overrides_a_named_template(db, monkeypatch):
    """template_id must not win over the explicit workspace request."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL_DOUBLED)

    _, _, latest = start(case_id, template_id="tmpl-base",
                         use_workspace_rules=True)

    # tmpl-base would have matched the baseline exactly; the workspace does not.
    assert latest["counts"]["changed"] == 1


def test_without_the_flag_the_origin_template_still_wins(db, monkeypatch):
    """The default path is untouched: a drifted workspace is ignored."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL_DOUBLED)

    _, _, latest = start(case_id)

    assert latest["status"] == "passed"
    assert latest["counts"]["changed"] == 0


# ── the unsaved editor buffer ───────────────────────────────────────────

def test_unsaved_editor_buffer_is_what_gets_replayed(db, monkeypatch):
    """`workspace_code` beats the saved rules — the whole point of the option."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL_DOUBLED)      # saved rules: x2

    _, _, latest = start(case_id, use_workspace_rules=True,
                         workspace_code=DSL_TRIPLED_UNSAVED)   # editor: x3

    diff = run(R.get_run_diff(latest["run_id"]))
    rows = [r for r in diff["rows"] if r["status"].lower() == "changed"]
    assert len(rows) == 1
    # 100 tripled, not doubled: the buffer ran, not the saved rules.
    assert float(rows[0]["actual_amount"]) == pytest.approx(300.0)


def test_the_run_is_labelled_so_two_buffers_are_told_apart(db, monkeypatch):
    """Every editor run carries a digest, or the run list is ambiguous."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL)

    _, _, first = start(case_id, use_workspace_rules=True,
                        workspace_code=DSL_DOUBLED)
    _, _, second = start(case_id, use_workspace_rules=True,
                         workspace_code=DSL_TRIPLED_UNSAVED)

    assert used(first) != used(second)
    assert "editor" in used(first)


def test_a_blank_buffer_falls_back_to_the_saved_rules(db, monkeypatch):
    """An empty editor must not be read as "run nothing"."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL_DOUBLED)

    _, _, latest = start(case_id, use_workspace_rules=True, workspace_code="   ")

    assert latest["counts"]["changed"] == 1          # the saved x2 ran
    assert "editor" not in used(latest)


# ── precedence ──────────────────────────────────────────────────────────

def test_pinned_code_still_wins_over_the_workspace(db, monkeypatch):
    """Otherwise "replay the baseline exactly" would quietly stop doing that."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL_DOUBLED)

    _, _, latest = start(case_id, use_pinned_code=True,
                         use_workspace_rules=True,
                         workspace_code=DSL_TRIPLED_UNSAVED)

    assert latest["status"] == "passed"
    assert "pinned" in used(latest).lower()


# ── what the caller is told ─────────────────────────────────────────────

def test_the_response_says_what_it_is_running_against(db, monkeypatch):
    """The MCP connector echoes this back, so it has to be accurate."""
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, DSL)

    saved, _, _ = start(case_id, use_workspace_rules=True)
    assert saved["running_against"] == "the saved workspace rules"

    edited, _, _ = start(case_id, use_workspace_rules=True,
                         workspace_code=DSL_DOUBLED)
    assert edited["running_against"] == "the workspace rules as they stand in the editor"

    default, _, _ = start(case_id)
    assert default["running_against"] == "each case's origin template"


def test_an_empty_workspace_is_a_clear_error_not_a_crash(db, monkeypatch):
    capture()
    case_id = run(R.list_cases())[0]["id"]
    _set_workspace(monkeypatch, "")

    _, _, latest = start(case_id, use_workspace_rules=True)

    assert latest["status"] == "error"
    assert any("workspace has no rules" in str(e).lower()
               for e in (latest.get("errors") or []))
