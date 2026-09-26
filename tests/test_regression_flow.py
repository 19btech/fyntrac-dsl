"""End-to-end tests for the regression feature, against real execution.

These drive the actual endpoint functions — capture, run, diff, accept — with
an in-memory Mongo and the real DSL engine, so they exercise compilation,
execution across every posting date, snapshot storage and the comparison in
one path. What they are protecting:

  * a capture freezes the dataset, so later edits to live event data cannot
    change what the case expects;
  * a replay never touches live collections, in particular the transaction
    report the user is looking at;
  * accepting a run writes a NEW version and leaves history intact.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402
from backend import regression as R  # noqa: E402
from tests.fake_mongo import FakeDB  # noqa: E402

EVT = "SO_EVENT"

DSL = (f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
       f'"Rev", {EVT}.amt)\n')

# A second template that halves every amount — the "someone changed the rules"
# case the whole feature exists to catch.
DSL_HALVED = (f'half = divide({EVT}.amt, 2)\n'
              f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
              f'"Rev", half)\n')


def _rows():
    out = []
    for date, amounts in (("2026-01-31", [100.0, 200.0]),
                          ("2026-02-28", [300.0, 400.0])):
        for i, amount in enumerate(amounts):
            out.append({"instrumentid": f"SO-{i + 1}", "subinstrumentid": "1",
                        "postingdate": date, "effectivedate": date, "amt": amount})
    return out


def _mkdb(rows=None, combined=DSL):
    return FakeDB(
        event_definitions=[{
            "event_name": EVT, "eventType": "activity", "eventTable": "standard",
            "fields": [{"name": "amt", "datatype": "decimal"}]}],
        event_data=[{"event_name": EVT,
                     "data_rows": rows if rows is not None else _rows()}],
        user_templates=[
            {"id": "tmpl-base", "name": "Revenue", "combinedCode": combined},
            {"id": "tmpl-half", "name": "Revenue (halved)", "combinedCode": DSL_HALVED},
        ],
        transaction_reports=[],
        saved_rules=[],
    )


@pytest.fixture
def db(monkeypatch):
    """Point both the server and the regression module at one fake database."""
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


def capture(name="Base case", template_id="tmpl-base", **kw):
    return run(R.capture_case(R.CaptureCaseRequest(
        name=name, template_id=template_id, **kw)))


class _Background:
    """Stand-in for FastAPI's BackgroundTasks that runs the task inline."""

    def __init__(self):
        self.tasks = []

    def add_task(self, fn, *args, **kwargs):
        self.tasks.append((fn, args, kwargs))

    async def drain(self):
        for fn, args, kwargs in self.tasks:
            await fn(*args, **kwargs)


def run_case(case_id, template_id=None, pinned=False):
    """Start a batch and drive its background task to completion."""
    async def _go():
        bg = _Background()
        started = await R.run_regression(
            R.RunRequest(case_ids=[case_id], template_id=template_id,
                         use_pinned_code=pinned), bg)
        await bg.drain()
        batch = await R.get_batch(started["batch_id"])
        return batch
    return run(_go())


def latest_run(case_id):
    runs = run(R.list_runs(case_id=case_id))
    return runs[0]


# ── capture ─────────────────────────────────────────────────────────────

def test_capture_stores_the_produced_transactions(db):
    result = capture()
    assert result["version"] == 1
    # 4 rows, all non-zero, so 4 transactions across 2 posting dates.
    assert result["transactions_captured"] == 4
    assert result["posting_dates"] == 2


def test_capture_summarises_the_frozen_dataset(db):
    summary = capture()["dataset_summary"]
    assert summary["total_rows"] == 4
    assert summary["posting_date_count"] == 2
    assert summary["instrument_count"] == 2
    assert summary["events"][0]["event_name"] == EVT


def test_capture_refuses_a_duplicate_name(db):
    capture(name="Same")
    with pytest.raises(Exception) as excinfo:
        capture(name="same")          # case-insensitive clash
    assert "already exists" in str(excinfo.value.detail)


def test_capture_does_not_touch_the_live_transaction_report(db):
    """A capture runs the book; it must not write the user's report."""
    capture()
    assert db.transaction_reports.docs == []


# ── the dataset really is frozen ────────────────────────────────────────

def test_replay_uses_the_snapshot_not_current_live_data(db):
    case = capture()
    # Change live data out from under the case. A referencing implementation
    # would now compare against different inputs and report differences.
    db.event_data.docs[0]["data_rows"] = [
        {"instrumentid": "SO-9", "subinstrumentid": "1",
         "postingdate": "2027-06-30", "effectivedate": "2027-06-30", "amt": 9999.0}]
    batch = run_case(case["id"])
    assert batch["passed"] == 1
    assert latest_run(case["id"])["counts"]["differences"] == 0


def test_replay_does_not_write_the_live_transaction_report(db):
    case = capture()
    run_case(case["id"])
    assert db.transaction_reports.docs == []


# ── detecting a real regression ─────────────────────────────────────────

def test_changed_rules_are_caught_as_amount_differences(db):
    case = capture()
    batch = run_case(case["id"], template_id="tmpl-half")
    assert batch["failed"] == 1

    counts = latest_run(case["id"])["counts"]
    assert counts["expected_total"] == 4
    assert counts["actual_total"] == 4
    assert counts["changed"] == 4
    assert counts["matched"] == 0
    assert counts["missing"] == 0 and counts["added"] == 0


def test_diff_rows_name_the_transaction_and_both_amounts(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    diff = run(R.get_run_diff(latest_run(case["id"])["run_id"]))

    assert diff["total"] == 4
    row = next(r for r in diff["rows"]
               if r["instrumentid"] == "SO-1" and r["postingdate"] == "2026-01-31")
    assert row["status"] == "CHANGED"
    assert row["transactiontype"] == "Rev"
    assert row["expected_amount"] == 100.0
    assert row["actual_amount"] == 50.0
    assert row["delta"] == pytest.approx(-50.0)


def test_diff_can_be_filtered_by_status(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]
    assert run(R.get_run_diff(run_id, status="CHANGED"))["total"] == 4
    assert run(R.get_run_diff(run_id, status="MISSING"))["total"] == 0


def test_rerunning_the_same_template_passes(db):
    case = capture()
    assert run_case(case["id"])["passed"] == 1


def test_pinned_code_replays_the_baseline_template(db):
    """Pinned mode ignores the template argument and uses the frozen code."""
    case = capture()
    batch = run_case(case["id"], template_id="tmpl-half", pinned=True)
    assert batch["passed"] == 1


# ── accepting new results ───────────────────────────────────────────────

def test_accept_writes_a_new_version_and_leaves_the_old_one(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]

    accepted = run(R.accept_run(run_id, R.AcceptRequest(note="halved on purpose")))
    assert accepted["version"] == 2

    detail = run(R.get_case(case["id"]))
    assert detail["active_version"] == 2
    assert [v["version"] for v in detail["versions"]] == [2, 1]

    # v1's expected results are still readable and unchanged.
    v1 = run(R.get_expected(case["id"], 1))
    assert sorted(t["amount"] for t in v1["transactions"]) == [100.0, 200.0, 300.0, 400.0]
    v2 = run(R.get_expected(case["id"], 2))
    assert sorted(t["amount"] for t in v2["transactions"]) == [50.0, 100.0, 150.0, 200.0]


def test_after_accepting_the_changed_template_passes(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))

    assert run_case(case["id"], template_id="tmpl-half")["passed"] == 1
    # …and the original template is now the regression.
    assert run_case(case["id"], template_id="tmpl-base")["failed"] == 1


def test_accept_records_where_the_baseline_came_from(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]
    run(R.accept_run(run_id, R.AcceptRequest(note="deliberate")))

    version = next(v for v in run(R.list_versions(case["id"]))["versions"]
                   if v["version"] == 2)
    assert version["created_from_run_id"] == run_id
    assert version["note"] == "deliberate"
    assert version["accepted_diff"]["changed"] == 4


def test_accept_is_blocked_when_maker_checker_is_on(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]
    R.configure(settings=type("Cfg", (), {"require_agent_approval": True})())
    try:
        with pytest.raises(Exception) as excinfo:
            run(R.accept_run(run_id, R.AcceptRequest()))
        assert "reviewer" in str(excinfo.value.detail)
    finally:
        R.configure(settings=type("Cfg", (), {"require_agent_approval": False})())


# ── versions ────────────────────────────────────────────────────────────

def test_restoring_an_old_version_copies_it_forward(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))

    restored = run(R.activate_version(case["id"], 1, R.AcceptRequest()))
    assert restored["version"] == 3           # append-only: never rewinds to 1

    detail = run(R.get_case(case["id"]))
    assert detail["active_version"] == 3
    assert [v["version"] for v in detail["versions"]] == [3, 2, 1]
    v3 = run(R.get_expected(case["id"], 3))
    assert sorted(t["amount"] for t in v3["transactions"]) == [100.0, 200.0, 300.0, 400.0]


def test_recapture_snapshots_the_current_data_as_a_new_version(db):
    case = capture()
    db.event_data.docs[0]["data_rows"] = [
        {"instrumentid": "SO-1", "subinstrumentid": "1",
         "postingdate": "2026-03-31", "effectivedate": "2026-03-31", "amt": 42.0}]

    result = run(R.recapture_case(case["id"], R.AcceptRequest(note="new month")))
    assert result["version"] == 2
    assert result["transactions_captured"] == 1

    v2 = run(R.get_expected(case["id"], 2))
    assert [t["amount"] for t in v2["transactions"]] == [42.0]
    # v1 still holds the original book.
    assert run(R.get_expected(case["id"], 1))["total"] == 4


def test_identical_datasets_are_stored_once(db):
    """Re-baselining must not duplicate the dataset."""
    case = capture()
    before = len(db.regression_datasets.docs)
    run_case(case["id"], template_id="tmpl-half")
    run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))
    # v2 reuses v1's dataset — same content, same hash, no new documents.
    assert len(db.regression_datasets.docs) == before


# ── listing and deletion ────────────────────────────────────────────────

def test_case_list_carries_the_last_run_status(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    listed = run(R.list_cases())
    assert len(listed) == 1
    assert listed[0]["last_run"]["status"] == "failed"
    assert listed[0]["expected_count"] == 4
    assert listed[0]["version_count"] == 1


def test_deleting_a_case_removes_its_snapshot_and_runs(db):
    case = capture()
    run_case(case["id"])
    run(R.delete_case(case["id"]))

    assert run(R.list_cases()) == []
    assert db.regression_datasets.docs == []
    assert db.regression_expected.docs == []
    assert db.regression_runs.docs == []
    assert db.regression_run_diffs.docs == []


def test_deleting_one_case_keeps_another_cases_dataset(db):
    first = capture(name="First")
    second = capture(name="Second")
    run(R.delete_case(first["id"]))
    # Both captured the same live data, so they share one content-addressed
    # snapshot; deleting one must not pull it out from under the other.
    assert db.regression_datasets.docs != []
    assert run_case(second["id"])["passed"] == 1


# ── failure surfaces ────────────────────────────────────────────────────

def test_running_a_template_that_needs_a_missing_event_reports_it(db):
    case = capture()
    db.user_templates.docs.append({
        "id": "tmpl-other", "name": "Other",
        "combinedCode": 'createTransaction(OTHER_EVENT.postingdate, '
                        'OTHER_EVENT.effectivedate, "X", OTHER_EVENT.amt)\n'})
    run_case(case["id"], template_id="tmpl-other")
    failed = latest_run(case["id"])
    assert failed["status"] == "error"
    assert "OTHER_EVENT" in failed["errors"][0]["error"]


def test_capture_with_no_event_data_is_refused(db):
    db.event_definitions.docs = []
    with pytest.raises(Exception) as excinfo:
        capture(name="Empty")
    assert "nothing to capture" in str(excinfo.value.detail).lower()


def test_a_broken_case_does_not_hang_the_batch(db):
    """A case the runner cannot even set up must still close out the batch.

    The UI polls the batch document, so an exception escaping the background
    task would leave it spinning on "running" forever.
    """
    good = capture(name="Good")
    broken = capture(name="Broken")
    # Destroy the broken case's active version, which _run_one_case needs.
    db.regression_case_versions.docs = [
        v for v in db.regression_case_versions.docs
        if v["case_id"] != broken["id"]]

    async def _go():
        bg = _Background()
        started = await R.run_regression(
            R.RunRequest(case_ids=[broken["id"], good["id"]]), bg)
        await bg.drain()
        return await R.get_batch(started["batch_id"])

    batch = run(_go())
    assert batch["status"] == "complete"
    assert batch["errored"] == 1
    assert batch["passed"] == 1        # the healthy case still ran


# ── accepting promotes exactly what was reviewed ────────────────────────

def test_accept_promotes_the_reviewed_output_not_a_fresh_run(db):
    """Editing the template between reviewing a diff and accepting it must
    not change what gets baselined.

    Re-running at accept time reads the template as it stands then, so a rule
    edited in between would silently become the expectation without anyone
    having seen its numbers.
    """
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]

    # The reviewer saw halves: 50 / 100 / 150 / 200.
    diff = run(R.get_run_diff(run_id))
    assert sorted(r["actual_amount"] for r in diff["rows"]) == [50.0, 100.0, 150.0, 200.0]

    # Someone edits the template to quarter instead of halve.
    for tmpl in db.user_templates.docs:
        if tmpl["id"] == "tmpl-half":
            tmpl["combinedCode"] = DSL_HALVED.replace(
                f"divide({EVT}.amt, 2)", f"divide({EVT}.amt, 4)")

    run(R.accept_run(run_id, R.AcceptRequest()))
    saved = run(R.get_expected(case["id"], 2))
    assert sorted(t["amount"] for t in saved["transactions"]) == [50.0, 100.0, 150.0, 200.0]


def test_a_run_cannot_be_accepted_twice(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]
    run(R.accept_run(run_id, R.AcceptRequest()))
    with pytest.raises(Exception) as excinfo:
        run(R.accept_run(run_id, R.AcceptRequest()))
    assert "already accepted" in str(excinfo.value.detail)


def test_run_output_is_discarded_with_the_run(db):
    case = capture()
    run_case(case["id"])
    run_id = latest_run(case["id"])["run_id"]
    assert db.regression_actuals.docs != []
    run(R.delete_run(run_id))
    assert db.regression_actuals.docs == []


# ── indexes ─────────────────────────────────────────────────────────────

def test_every_regression_collection_is_indexed(db):
    run(R.ensure_indexes())
    for collection, specs in R._INDEXES.items():
        built = db[collection].indexes
        assert built, f"{collection} has no index"
        assert [i["keys"] for i in built] == [list(k) for k, _ in specs]


def test_lookup_keys_are_unique_where_they_must_be(db):
    run(R.ensure_indexes())
    unique = {c: [i["keys"] for i in db[c].indexes if i["unique"]]
              for c in R._INDEXES}
    assert [("id", 1)] in unique["regression_cases"]
    assert [("run_id", 1)] in unique["regression_runs"]
    assert [("batch_id", 1)] in unique["regression_batches"]
    assert [("case_id", 1), ("version", -1)] in unique["regression_case_versions"]


def test_index_creation_survives_a_failing_collection(db, monkeypatch):
    """A restricted Mongo user must not stop the app from serving."""
    async def boom(*a, **k):
        raise RuntimeError("not authorised")
    monkeypatch.setattr(db["regression_cases"], "create_index", boom)
    run(R.ensure_indexes())          # must not raise
    assert db["regression_runs"].indexes


# ── per-case settings really take effect ────────────────────────────────

def test_case_tolerance_is_applied_at_run_time(db):
    """A case's tolerance must govern its own comparison, not the default."""
    case = capture(name="Loose", amount_tolerance=1000.0)
    assert run_case(case["id"], template_id="tmpl-half")["passed"] == 1

    tight = capture(name="Tight", amount_tolerance=0.01)
    assert run_case(tight["id"], template_id="tmpl-half")["failed"] == 1


def test_tolerance_can_be_changed_after_capture(db):
    case = capture()
    assert run_case(case["id"], template_id="tmpl-half")["failed"] == 1
    run(R.update_case(case["id"], R.UpdateCaseRequest(amount_tolerance=1000.0)))
    assert run_case(case["id"], template_id="tmpl-half")["passed"] == 1


# ── fewer / more transactions, end to end ───────────────────────────────

def test_a_template_producing_fewer_rows_reports_them_missing(db):
    case = capture()
    db.user_templates.docs.append({
        "id": "tmpl-one", "name": "Big amounts only",
        "combinedCode": (f'big = if(gt({EVT}.amt, 250), {EVT}.amt, 0)\n'
                         f'createTransaction({EVT}.postingdate, '
                         f'{EVT}.effectivedate, "Rev", big)\n')})
    run_case(case["id"], template_id="tmpl-one")
    counts = latest_run(case["id"])["counts"]
    # Zero amounts are suppressed by the engine, so 100 and 200 vanish.
    assert counts["missing"] == 2
    assert counts["matched"] == 2
    assert counts["added"] == 0


def test_a_template_producing_an_extra_type_reports_it_added(db):
    case = capture()
    db.user_templates.docs.append({
        "id": "tmpl-extra", "name": "Plus fee",
        "combinedCode": (f'createTransaction({EVT}.postingdate, '
                         f'{EVT}.effectivedate, "Rev", {EVT}.amt)\n'
                         f'createTransaction({EVT}.postingdate, '
                         f'{EVT}.effectivedate, "Fee", 5)\n')})
    run_case(case["id"], template_id="tmpl-extra")
    counts = latest_run(case["id"])["counts"]
    assert counts["matched"] == 4
    assert counts["added"] == 4
    assert counts["missing"] == 0

    diff = run(R.get_run_diff(latest_run(case["id"])["run_id"], status="ADDED"))
    assert {r["transactiontype"] for r in diff["rows"]} == {"Fee"}


# ── capture guard rails ─────────────────────────────────────────────────

def test_capture_refuses_when_a_posting_date_fails(db):
    db.user_templates.docs.append({
        "id": "tmpl-bad", "name": "Broken",
        "combinedCode": f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", undefined_variable)\n'})
    with pytest.raises(Exception) as excinfo:
        capture(name="Bad", template_id="tmpl-bad")
    detail = excinfo.value.detail
    assert detail["can_force"] is True
    assert detail["errors"]


def test_capture_can_be_forced_past_failures_and_says_so(db):
    db.user_templates.docs.append({
        "id": "tmpl-bad", "name": "Broken",
        "combinedCode": f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", undefined_variable)\n'})
    result = capture(name="Bad", template_id="tmpl-bad", allow_errors=True)
    version = run(R.list_versions(result["id"]))["versions"][0]
    assert version["captured_with_errors"] is True


def test_capture_flags_nondeterministic_rules(db):
    """The scan is textual, so it warns on any mention of a clock or an RNG.

    A false positive costs a dismissible banner; missing a rule that really
    does read the wall clock costs a case that fails against itself forever.
    """
    db.user_templates.docs.append({
        "id": "tmpl-clock", "name": "Clocky",
        "combinedCode": f'# seeded from uuid4() upstream\n'
                        f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", {EVT}.amt)\n'})
    result = capture(name="Clocky", template_id="tmpl-clock")
    assert "uuid4" in result["nondeterminism_warnings"]
    assert "uuid4" in run(R.get_case(result["id"]))["nondeterminism_warnings"]


# ── retention ───────────────────────────────────────────────────────────

def test_old_runs_are_pruned_with_their_diffs(db):
    R.configure(keep_runs=2)
    try:
        case = capture()
        for _ in range(4):
            run_case(case["id"], template_id="tmpl-half")
        runs = run(R.list_runs(case_id=case["id"]))
        assert len(runs) == 2
        kept = {r["run_id"] for r in runs}
        assert {d["run_id"] for d in db.regression_run_diffs.docs} <= kept
        assert {a["run_id"] for a in db.regression_actuals.docs} <= kept
    finally:
        R.configure(keep_runs=50)


def test_an_execution_failure_reports_error_not_a_regression(db):
    """A template that blows up produces no transactions, which the diff would
    otherwise report as the whole book going missing.

    That sends the user hunting for a regression when the real problem is a
    broken rule, so any execution error wins over the comparison result.
    """
    db.user_templates.docs.append({
        "id": "tmpl-boom", "name": "Broken",
        "combinedCode": f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", nonexistent_var)\n'})
    case = capture()
    batch = run_case(case["id"], template_id="tmpl-boom")

    assert batch["errored"] == 1
    assert batch["failed"] == 0
    failed = latest_run(case["id"])
    assert failed["status"] == "error"
    assert failed["errors"]


def test_an_errored_run_cannot_be_accepted(db):
    db.user_templates.docs.append({
        "id": "tmpl-boom", "name": "Broken",
        "combinedCode": f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", nonexistent_var)\n'})
    case = capture()
    run_case(case["id"], template_id="tmpl-boom")
    with pytest.raises(Exception) as excinfo:
        run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))
    assert "errored" in str(excinfo.value.detail)


def test_zero_tolerance_means_exact_and_is_not_swallowed(db):
    """`or DEFAULT` would turn a deliberate 0 back into a cent of slack."""
    case = capture(name="Exact", amount_tolerance=0)
    stored = run(R.get_case(case["id"]))
    assert stored["amount_tolerance"] == 0.0

    db.user_templates.docs.append({
        "id": "tmpl-dust", "name": "Dust",
        "combinedCode": f'x = add({EVT}.amt, 0.005)\n'
                        f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", x)\n'})
    # Half a cent is inside the default tolerance but outside an exact match.
    run_case(case["id"], template_id="tmpl-dust")
    assert latest_run(case["id"])["counts"]["changed"] == 4


def test_accepted_version_records_the_code_that_produced_it(db):
    """The snapshot stored with a version must be the source that actually
    generated its expected transactions.

    Recording the template's current text instead would both misattribute the
    numbers and corrupt any later pinned replay of that version.
    """
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_id = latest_run(case["id"])["run_id"]

    edited = DSL_HALVED.replace(f"divide({EVT}.amt, 2)", f"divide({EVT}.amt, 4)")
    for tmpl in db.user_templates.docs:
        if tmpl["id"] == "tmpl-half":
            tmpl["combinedCode"] = edited

    run(R.accept_run(run_id, R.AcceptRequest()))
    version = next(v for v in db.regression_case_versions.docs if v["version"] == 2)
    assert version["template_snapshot"]["dsl_code"] == DSL_HALVED
    assert version["template_snapshot"]["dsl_code"] != edited

    # And a pinned replay of v2 reproduces the accepted numbers exactly.
    assert run_case(case["id"], pinned=True)["passed"] == 1


# ── run progress ────────────────────────────────────────────────────────

def test_progress_counts_posting_dates_within_a_case(db):
    """A single-case run must move as its dates complete, not sit at 0%
    until the whole case finishes."""
    assert R._batch_progress({
        "total": 1, "results": [], "date_total": 14, "date_index": 7,
        "status": "running"})["percent"] == 50.0


def test_progress_blends_completed_cases_with_the_one_in_flight(db):
    p = R._batch_progress({
        "total": 4, "results": [{}, {}], "date_total": 10, "date_index": 5,
        "status": "running"})
    assert p["percent"] == 62.5           # 2 of 4 done, plus half of the third
    assert p["cases_done"] == 2
    assert p["cases_total"] == 4


def test_progress_never_reports_complete_while_running(db):
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 10, "date_index": 10,
        "status": "running"})
    assert p["percent"] < 100


def test_progress_reports_complete_when_the_batch_finishes(db):
    p = R._batch_progress({
        "total": 3, "results": [{}, {}, {}], "date_total": 0, "date_index": 0,
        "status": "complete"})
    assert p["percent"] == 100.0


def test_progress_survives_a_case_with_no_dates_yet(db):
    p = R._batch_progress({"total": 2, "results": [], "status": "running"})
    assert p["percent"] == 0.0
    assert p["eta_ms"] is None


def test_progress_estimates_remaining_time_from_elapsed(db):
    started = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 10, "date_index": 5,
        "status": "running", "started_at": started})
    assert p["elapsed_ms"] >= 29000
    # Half done after ~30s implies roughly another 30s.
    assert 25000 <= p["eta_ms"] <= 35000


def test_no_estimate_is_offered_too_early(db):
    """Below a few percent the extrapolation swings wildly, so it is withheld."""
    started = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    p = R._batch_progress({
        "total": 100, "results": [], "date_total": 100, "date_index": 1,
        "status": "running", "started_at": started})
    assert p["eta_ms"] is None


def test_a_running_batch_exposes_progress_over_the_api(db):
    case = capture()
    batch = run_case(case["id"])
    assert batch["progress"]["percent"] == 100.0
    assert batch["progress"]["cases_total"] == 1
    assert batch["progress"]["elapsed_ms"] is not None


def test_posting_date_progress_is_published_during_a_run(db):
    """The batch document must carry date-level position while a case runs."""
    seen = []
    real = R._run_one_case

    async def spy(case, batch_id, template_id, pinned, on_progress=None, **kw):
        if on_progress:
            await on_progress(3, 14, "2026-03-31")
            doc = await db.regression_batches.find_one({"batch_id": batch_id})
            seen.append((doc.get("date_index"), doc.get("date_total"),
                         doc.get("current_date")))
        return await real(case, batch_id, template_id, pinned, on_progress, **kw)

    R._run_one_case = spy
    try:
        case = capture()
        run_case(case["id"])
    finally:
        R._run_one_case = real
    assert seen == [(3, 14, "2026-03-31")]


# ── the estimate must appear immediately when history exists ────────────

def test_a_first_run_with_no_history_says_it_is_estimating(db):
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 14, "date_index": 0,
        "status": "running", "baseline_ms": 0,
        "started_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()})
    assert p["eta_ms"] is None
    assert p["source"] == "live"


def test_history_drives_the_estimate_before_any_date_completes(db):
    """The bar sat at 0% for the whole first posting date; with a prior run
    recorded it can show a real estimate from the first second."""
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 14, "date_index": 0,
        "status": "running", "baseline_ms": 600000,
        "started_at": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()})
    assert p["source"] == "history"
    assert 9.0 <= p["percent"] <= 11.0          # 60s of an expected 600s
    assert 530000 <= p["eta_ms"] <= 545000      # ~9 minutes left


def test_history_estimate_never_parks_the_bar_at_full(db):
    """A run slower than last time must not sit at 100% while still working."""
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 14, "date_index": 0,
        "status": "running", "baseline_ms": 10000,
        "started_at": (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()})
    assert p["percent"] == 95.0
    assert p["eta_ms"] == 0


def test_live_measurement_takes_over_once_dates_complete(db):
    p = R._batch_progress({
        "total": 1, "results": [], "date_total": 14, "date_index": 7,
        "status": "running", "baseline_ms": 600000,
        "started_at": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()})
    assert p["source"] == "live"
    assert p["percent"] == 50.0


def test_a_new_batch_records_the_previous_runs_duration(db):
    case = capture()
    run_case(case["id"])
    first = latest_run(case["id"])
    assert first["duration_ms"] is not None

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case["id"]]), bg)
        doc = await db.regression_batches.find_one({"batch_id": started["batch_id"]})
        await bg.drain()
        return doc
    assert run(_go())["baseline_ms"] == first["duration_ms"]


def test_an_errored_prior_run_is_not_used_as_a_baseline(db):
    """An error aborts early, so its duration would under-estimate wildly."""
    db.user_templates.docs.append({
        "id": "tmpl-boom", "name": "Broken",
        "combinedCode": f'createTransaction({EVT}.postingdate, '
                        f'{EVT}.effectivedate, "Rev", nonexistent_var)\n'})
    case = capture()
    run_case(case["id"], template_id="tmpl-boom")
    assert latest_run(case["id"])["status"] == "error"

    errored = latest_run(case["id"])

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case["id"]]), bg)
        doc = await db.regression_batches.find_one({"batch_id": started["batch_id"]})
        await bg.drain()
        return doc

    # It falls back to the capture's cost rather than the aborted run's.
    # Durations are not compared to each other: at test speed both are often
    # 0ms, which made that assertion flaky rather than meaningful.
    assert errored["status"] == "error"
    version = run(R.list_versions(case["id"]))["versions"][0]
    assert run(_go())["baseline_ms"] == version["run_ms"]


def test_the_first_ever_run_estimates_from_the_capture(db):
    """Capture executed the same book, so even a never-run case has a signal
    instead of showing 'estimating...' through its whole first date."""
    case = capture()
    version = run(R.list_versions(case["id"]))["versions"][0]
    assert version["run_ms"] is not None

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case["id"]]), bg)
        doc = await db.regression_batches.find_one({"batch_id": started["batch_id"]})
        await bg.drain()
        return doc
    assert run(_go())["baseline_ms"] == version["run_ms"]


# ── clearing run history ────────────────────────────────────────────────

def test_clearing_history_removes_runs_diffs_and_outputs(db):
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run_case(case["id"])
    assert len(run(R.list_runs(case_id=case["id"]))) == 2

    result = run(R.clear_case_runs(case["id"]))
    assert result["deleted"] == 2
    assert run(R.list_runs(case_id=case["id"])) == []
    assert db.regression_run_diffs.docs == []
    assert db.regression_actuals.docs == []


def test_clearing_history_keeps_the_baseline_intact(db):
    """History is not the expectation: clearing it must not change what the
    case compares against."""
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))

    before = run(R.get_expected(case["id"], 2))["transactions"]
    run(R.clear_case_runs(case["id"]))

    detail = run(R.get_case(case["id"]))
    assert detail["active_version"] == 2
    assert len(detail["versions"]) == 2
    assert run(R.get_expected(case["id"], 2))["transactions"] == before
    # …and the case still passes against the template it was accepted from.
    assert run_case(case["id"], template_id="tmpl-half")["passed"] == 1


def test_clearing_one_case_does_not_touch_another(db):
    first = capture(name="First")
    second = capture(name="Second")
    run_case(first["id"])
    run_case(second["id"])

    run(R.clear_case_runs(first["id"]))
    assert run(R.list_runs(case_id=first["id"])) == []
    assert len(run(R.list_runs(case_id=second["id"]))) == 1
    assert db.regression_actuals.docs != []


def test_clearing_an_empty_history_is_not_an_error(db):
    case = capture()
    result = run(R.clear_case_runs(case["id"]))
    assert result["deleted"] == 0
    assert "No run history" in result["message"]


def test_clearing_history_for_an_unknown_case_is_rejected(db):
    with pytest.raises(Exception) as excinfo:
        run(R.clear_case_runs("no-such-case"))
    assert "not found" in str(excinfo.value.detail).lower()


def test_deleting_a_single_run_leaves_the_others(db):
    case = capture()
    run_case(case["id"])
    run_case(case["id"], template_id="tmpl-half")
    runs = run(R.list_runs(case_id=case["id"]))
    assert len(runs) == 2

    run(R.delete_run(runs[0]["run_id"]))
    remaining = run(R.list_runs(case_id=case["id"]))
    assert [r["run_id"] for r in remaining] == [runs[1]["run_id"]]


def test_after_clearing_history_the_estimate_falls_back_to_the_capture(db):
    """The progress estimate reads the last run; with none, it must still
    have the capture's cost rather than going blind."""
    case = capture()
    run_case(case["id"])
    run(R.clear_case_runs(case["id"]))

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case["id"]]), bg)
        doc = await db.regression_batches.find_one({"batch_id": started["batch_id"]})
        await bg.drain()
        return doc
    version = run(R.list_versions(case["id"]))["versions"][0]
    assert run(_go())["baseline_ms"] == version["run_ms"]


# ── profiling ───────────────────────────────────────────────────────────

def test_profiling_is_off_by_default(db):
    case = capture()
    run_case(case["id"])
    assert latest_run(case["id"])["timing"].get("profile") is None


def test_a_profiled_run_attributes_its_time_to_functions(db):
    case = capture()

    async def _go():
        bg = _Background()
        await R.run_regression(
            R.RunRequest(case_ids=[case["id"]], profile=True), bg)
        await bg.drain()
    run(_go())

    profile = latest_run(case["id"])["timing"]["profile"]
    assert profile, "a profiled run must report its heaviest functions"
    top = profile[0]
    assert {"function", "calls", "self_ms", "total_ms"} <= set(top)
    # Ordered by self time, heaviest first.
    assert all(profile[i]["self_ms"] >= profile[i + 1]["self_ms"]
               for i in range(len(profile) - 1))


def test_profiling_does_not_change_the_result(db):
    """A diagnostic that alters the numbers is worse than none."""
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    plain = latest_run(case["id"])["counts"]

    async def _go():
        bg = _Background()
        await R.run_regression(R.RunRequest(
            case_ids=[case["id"]], template_id="tmpl-half", profile=True), bg)
        await bg.drain()
    run(_go())
    assert latest_run(case["id"])["counts"] == plain


# ── each case carries its own posting dates ─────────────────────────────

def _rows_for(dates, instruments, amount):
    return [{"instrumentid": ins, "subinstrumentid": "1",
             "postingdate": d, "effectivedate": d, "amt": amount}
            for d in dates for ins in instruments]


def test_each_case_derives_posting_dates_from_its_own_snapshot(db):
    """Two cases captured from different data must each replay only their
    own dates — the dates are read from the frozen snapshot, never shared
    and never taken from whatever is loaded now."""
    # Case A: two 2026 dates, one instrument.
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2026-01-31", "2026-02-28"], ["A-1"], 100.0)
    case_a = capture(name="Case A")

    # Case B: three 2027 dates, two instruments, different amounts.
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2027-06-30", "2027-07-31", "2027-08-31"], ["B-1", "B-2"], 250.0)
    case_b = capture(name="Case B")

    assert case_a["posting_dates"] == 2
    assert case_a["transactions_captured"] == 2
    assert case_b["posting_dates"] == 3
    assert case_b["transactions_captured"] == 6

    a_dates = {t["postingdate"] for t in run(R.get_expected(case_a["id"], 1))["transactions"]}
    b_dates = {t["postingdate"] for t in run(R.get_expected(case_b["id"], 1))["transactions"]}
    assert a_dates == {"2026-01-31", "2026-02-28"}
    assert b_dates == {"2027-06-30", "2027-07-31", "2027-08-31"}
    assert not (a_dates & b_dates)


def test_running_both_cases_together_keeps_them_separate(db):
    """A batch runs cases one after another, each against its own dataset —
    no interleaving, no shared posting-date list, no leaked transactions."""
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2026-01-31", "2026-02-28"], ["A-1"], 100.0)
    case_a = capture(name="Case A")
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2027-06-30", "2027-07-31", "2027-08-31"], ["B-1", "B-2"], 250.0)
    case_b = capture(name="Case B")

    # Live data is now something else entirely; neither case may notice.
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2099-12-31"], ["Z-9"], 1.0)

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[]), bg)
        await bg.drain()
        return await R.get_batch(started["batch_id"])

    batch = run(_go())
    assert batch["passed"] == 2
    assert batch["failed"] == 0 and batch["errored"] == 0

    a_run = latest_run(case_a["id"])
    b_run = latest_run(case_b["id"])
    # Each compared only its own book: 2 transactions vs 6.
    assert a_run["counts"]["expected_total"] == 2
    assert a_run["counts"]["actual_total"] == 2
    assert b_run["counts"]["expected_total"] == 6
    assert b_run["counts"]["actual_total"] == 6


def test_a_case_with_one_posting_date_still_runs(db):
    db.event_data.docs[0]["data_rows"] = _rows_for(["2026-03-31"], ["X-1"], 50.0)
    case = capture(name="Single date")
    assert case["posting_dates"] == 1
    assert run_case(case["id"])["passed"] == 1


def test_undated_data_runs_once_unscoped(db):
    """Data with no posting date runs a single unscoped pass rather than
    reporting nothing to do.

    The engine still suppresses the transactions themselves, because
    createTransaction requires both dates — so the case captures an empty
    baseline, which is a faithful record of what these rules produce from
    this data.
    """
    db.event_data.docs[0]["data_rows"] = [
        {"instrumentid": "R-1", "subinstrumentid": "1",
         "postingdate": "", "effectivedate": "2026-01-31", "amt": 10.0}]
    case = capture(name="Undated")
    assert case["posting_dates"] == 0
    assert case["transactions_captured"] == 0
    # And it is stable: replaying reproduces the same empty book.
    assert run_case(case["id"])["passed"] == 1


def test_transactions_do_not_leak_between_consecutive_cases(db):
    """The engine keeps transactions in module state; a case must start clean
    or it would inherit the previous one's book."""
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2026-01-31"], ["A-1", "A-2", "A-3"], 100.0)
    big = capture(name="Three instruments")
    db.event_data.docs[0]["data_rows"] = _rows_for(["2026-01-31"], ["B-1"], 100.0)
    small = capture(name="One instrument")

    async def _go():
        bg = _Background()
        await R.run_regression(R.RunRequest(case_ids=[big["id"], small["id"]]), bg)
        await bg.drain()
    run(_go())

    assert latest_run(big["id"])["counts"]["actual_total"] == 3
    assert latest_run(small["id"])["counts"]["actual_total"] == 1


# ── accept vs recapture: what each one actually moves ───────────────────

def test_accept_changes_the_expectation_but_keeps_the_input(db):
    """Accept answers 'the rules changed on purpose'. The dataset is
    untouched, so v1 and v2 remain comparable on identical inputs."""
    case = capture()
    run_case(case["id"], template_id="tmpl-half")
    run(R.accept_run(latest_run(case["id"])["run_id"], R.AcceptRequest()))

    versions = {v["version"]: v for v in run(R.list_versions(case["id"]))["versions"]}
    assert versions[1]["dataset_hash"] == versions[2]["dataset_hash"]
    assert versions[1]["expected_count"] == versions[2]["expected_count"]
    assert versions[1]["expected_hash"] != versions[2]["expected_hash"]


def test_recapture_changes_the_input_as_well(db):
    """Recapture answers 'the data changed on purpose'. Both the dataset and
    the expectation move, so the two versions are no longer like-for-like."""
    case = capture()
    db.event_data.docs[0]["data_rows"] = _rows_for(
        ["2026-09-30"], ["NEW-1"], 777.0)
    run(R.recapture_case(case["id"], R.AcceptRequest(note="new month")))

    versions = {v["version"]: v for v in run(R.list_versions(case["id"]))["versions"]}
    assert versions[1]["dataset_hash"] != versions[2]["dataset_hash"]
    assert versions[2]["dataset_summary"]["posting_dates"] == ["2026-09-30"]


def test_recapture_uses_the_cases_origin_template_not_the_last_one_run(db):
    """Worth knowing: recapture re-runs the template the case was captured
    with, regardless of what the most recent run used."""
    case = capture(template_id="tmpl-base")
    run_case(case["id"], template_id="tmpl-half")
    assert latest_run(case["id"])["template_used"]["id"] == "tmpl-half"

    run(R.recapture_case(case["id"], R.AcceptRequest()))
    v2 = next(v for v in run(R.list_versions(case["id"]))["versions"]
              if v["version"] == 2)
    assert v2["template_snapshot"]["id"] == "tmpl-base"
    # Full amounts, not halved — it re-ran the origin template.
    assert sorted(t["amount"] for t in run(R.get_expected(case["id"], 2))["transactions"]) \
        == [100.0, 200.0, 300.0, 400.0]


# ── stopping a run ──────────────────────────────────────────────────────

def run_and_stop(case_id):
    """Start a case, then request the stop once it is already executing.

    Pressing Stop before the batch task starts breaks at the case loop and
    records nothing, which is correct but is not what a user hits. Requesting
    it from inside the run exercises the per-posting-date checkpoint.
    """
    real = R._run_one_case

    async def spy(case, batch_id, template_id, pinned, on_progress=None, **kw):
        await R.cancel_batch(batch_id)
        return await real(case, batch_id, template_id, pinned, on_progress, **kw)

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case_id]), bg)
        R._run_one_case = spy
        try:
            await bg.drain()
        finally:
            R._run_one_case = real
        return await R.get_batch(started["batch_id"])

    return run(_go())


def test_a_batch_can_be_asked_to_stop(db):
    case = capture()
    batch = run_and_stop(case["id"])
    assert batch["cancelled"] is True
    assert batch["status"] == "complete"      # nothing is running any more


def test_a_stopped_case_is_not_recorded_as_a_pass_or_a_failure(db):
    """A partial book is missing whole posting dates. Scored as a result it
    would read as thousands of missing transactions -- a fake regression."""
    case = capture()
    batch = run_and_stop(case["id"])
    assert batch["passed"] == 0
    assert batch["failed"] == 0
    assert batch["stopped"] == 1

    stopped = latest_run(case["id"])
    assert stopped["status"] == "cancelled"
    assert stopped["counts"] == {}


def test_a_stopped_run_keeps_no_diff_to_be_mistaken_for_a_result(db):
    case = capture()
    run_and_stop(case["id"])
    assert db.regression_run_diffs.docs == []
    assert db.regression_actuals.docs == []


def test_a_stopped_run_cannot_become_a_baseline(db):
    case = capture()
    run_and_stop(case["id"])
    run_id = latest_run(case["id"])["run_id"]
    with pytest.raises(HTTPException) as excinfo:
        run(R.accept_run(run_id, R.AcceptRequest()))
    assert "no longer stored" in str(excinfo.value.detail).lower()


def test_stopping_before_anything_starts_records_nothing(db):
    """The other valid path: nothing had begun, so there is no phantom run."""
    case = capture()

    async def _go():
        bg = _Background()
        started = await R.run_regression(R.RunRequest(case_ids=[case["id"]]), bg)
        await R.cancel_batch(started["batch_id"])
        await bg.drain()
        return await R.get_batch(started["batch_id"])

    batch = run(_go())
    assert batch["cancelled"] is True
    assert run(R.list_runs(case_id=case["id"])) == []


def test_stopping_a_finished_batch_says_so_instead_of_failing(db):
    case = capture()
    batch = run_case(case["id"])
    ack = run(R.cancel_batch(batch["batch_id"]))
    assert ack["already_finished"] is True


def test_stopping_an_unknown_batch_is_rejected(db):
    with pytest.raises(HTTPException) as excinfo:
        run(R.cancel_batch("no-such-batch"))
    assert "not found" in str(excinfo.value.detail).lower()


def test_the_baseline_survives_a_stopped_run(db):
    """Stopping must not disturb what the case expects."""
    case = capture()
    before = run(R.get_expected(case["id"], 1))["transactions"]
    run_and_stop(case["id"])
    assert run(R.get_expected(case["id"], 1))["transactions"] == before
    # And a normal run afterwards still passes.
    assert run_case(case["id"])["passed"] == 1


# ── the collect-scoping fix reaches the regression path too ─────────────
# collect_by_instrument() spans every date it is handed. The full-book runner
# used to pass the entire book, so a rule that collects a per-line array and
# fans it into createTransaction emitted the same transaction once per posting
# date. Regression replays through that same runner, so it inflated baselines
# exactly as the Transaction Report did.

FANOUT_DSL = """subs = collect_by_instrument(SO_EVENT.subinstrumentid)
amts = collect_by_instrument(SO_EVENT.amt)
createTransaction(SO_EVENT.postingdate, SO_EVENT.effectivedate,
                  "ALLOCATED_REVENUE", amts, subs)
"""


@pytest.fixture
def fanout_db(db):
    """One instrument with a row on each of the fixture's two posting dates."""
    db.event_data.docs[0]["data_rows"] = [
        {"instrumentid": "I1", "subinstrumentid": "1", "postingdate": d,
         "effectivedate": d, "amt": 10.0}
        for d in ("2026-01-31", "2026-02-28")]
    db.user_templates.docs.append(
        {"id": "tmpl-fanout", "name": "Fan-out", "combinedCode": FANOUT_DSL})
    return db


def test_a_captured_baseline_is_not_inflated_by_the_collect_span(fanout_db):
    """Two dates, one line item -> two transactions, not four."""
    case = capture(name="Fanout", template_id="tmpl-fanout")
    assert case["transactions_captured"] == 2

    txns = run(R.get_expected(case["id"], 1))["transactions"]
    seen = {}
    for t in txns:
        key = (t["instrumentid"], t["postingdate"], t["transactiontype"],
               t["subinstrumentid"], t["amount"])
        seen[key] = seen.get(key, 0) + 1
    assert max(seen.values()) == 1, "a transaction was captured more than once"


def test_replaying_a_collecting_rule_is_stable(fanout_db):
    case = capture(name="Fanout", template_id="tmpl-fanout")
    assert run_case(case["id"])["passed"] == 1
    assert latest_run(case["id"])["counts"]["differences"] == 0


def test_each_posting_date_contributes_only_its_own_rows(fanout_db):
    case = capture(name="Fanout", template_id="tmpl-fanout")
    dates = [t["postingdate"]
             for t in run(R.get_expected(case["id"], 1))["transactions"]]
    assert sorted(dates) == ["2026-01-31", "2026-02-28"]


def test_per_line_fan_out_still_works_inside_a_date(fanout_db):
    """Scoping must not flatten the fan-out it was hiding."""
    fanout_db.event_data.docs[0]["data_rows"] = [
        {"instrumentid": "I1", "subinstrumentid": s, "postingdate": d,
         "effectivedate": d, "amt": 10.0}
        for d in ("2026-01-31", "2026-02-28") for s in ("1", "2", "3")]
    case = capture(name="Fanout3", template_id="tmpl-fanout")
    assert case["transactions_captured"] == 6      # 2 dates x 3 line items

    for d in ("2026-01-31", "2026-02-28"):
        subs = [t["subinstrumentid"]
                for t in run(R.get_expected(case["id"], 1))["transactions"]
                if t["postingdate"] == d]
        assert sorted(subs) == ["1", "2", "3"]
