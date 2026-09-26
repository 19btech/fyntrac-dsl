"""Regression testing: frozen datasets, expected transactions, and diffs.

A regression *case* is a frozen triple — a dataset snapshot, a template
snapshot, and the transactions that pairing produced. Replaying the case runs
the pinned dataset through a template of the user's choosing and compares the
result, transaction by transaction, against the expected set.

The dataset is *copied* into the case rather than referenced. Referencing live
event data would let every case rot silently the moment someone uploads a new
workbook; copying is the only way a baseline stays reproducible. Datasets are
content-addressed by hash so re-baselining expected results (the common case)
costs nothing extra in storage.

This module holds the pure comparison logic — `normalise_txn`, `diff_transactions`
— with no database or FastAPI dependency, so it is directly unit-testable. The
HTTP surface lives below it and reaches the rest of the app through `configure()`
rather than importing server.py, which would be circular.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Mongo caps a document at 16MB, so payload collections are chunked rather
# than stored as one array per case.
EXPECTED_CHUNK_SIZE = 5000
DIFF_CHUNK_SIZE = 2000

# Every transaction field, in the canonical report order.
TXN_FIELDS = ("postingdate", "effectivedate", "instrumentid",
              "subinstrumentid", "transactiontype", "amount")

# The five fields that identify a transaction; `amount` is the value compared.
KEY_FIELDS = ("instrumentid", "subinstrumentid", "postingdate",
              "effectivedate", "transactiontype")

DEFAULT_TOLERANCE = 0.01

# Constructs that make a replay non-reproducible: the same input would not
# yield the same output twice, so any diff they cause is noise.
_NONDETERMINISTIC_RE = re.compile(
    r"\b(random|uuid4|uuid1|datetime\.now|date\.today|time\.time|now|today)\s*\(")


# ── Pure helpers ────────────────────────────────────────────────────────

def _canonical_date(value: Any) -> str:
    """Reduce a date to YYYY-MM-DD so formatting drift is not read as a diff.

    Both sides of a comparison pass through here, so a run that starts
    emitting "03/31/2026" where the baseline held "2026-03-31" compares equal
    — which is right, it is the same date.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text or text.lower() in ("none", "nan", "nat"):
        return ""
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S.%f",
                "%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:26], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def _get_ci(row: Dict[str, Any], field: str, default: Any = "") -> Any:
    """Read `field` from `row` ignoring key case, as the rest of the app does."""
    if field in row:
        return row[field]
    lowered = field.lower()
    for key, value in row.items():
        if str(key).lower() == lowered:
            return value
    return default


def _canonical_amount(value: Any) -> float:
    """Coerce an amount to float, tolerating the formats spreadsheets carry.

    Thousands separators, currency symbols and parenthesised negatives all
    reach here from Excel-sourced data. Left unhandled they collapse to 0.0,
    which in a money comparison reads as a legitimate zero rather than as the
    parse failure it actually is.
    """
    if isinstance(value, bool):
        return 0.0
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    text = re.sub(r"[,\s$\u00a3\u20ac]", "", text)
    try:
        amount = round(float(text), 6)
    except (TypeError, ValueError):
        return 0.0
    return -amount if negative else amount


def normalise_txn(row: Dict[str, Any]) -> Dict[str, Any]:
    """Put a transaction into the one shape the comparison understands.

    Ids and transaction types are trimmed and case-folded, dates canonicalised,
    amount coerced to float. Everything else on the row is dropped: the output
    contract is these six fields, and comparing anything else would make the
    diff depend on incidental bookkeeping columns.
    """
    return {
        "instrumentid": str(_get_ci(row, "instrumentid", "")).strip(),
        "subinstrumentid": str(_get_ci(row, "subinstrumentid", "1")).strip() or "1",
        "postingdate": _canonical_date(_get_ci(row, "postingdate", "")),
        "effectivedate": _canonical_date(_get_ci(row, "effectivedate", "")),
        "transactiontype": str(_get_ci(row, "transactiontype", "")).strip(),
        "amount": _canonical_amount(_get_ci(row, "amount", 0)),
    }


def _key_of(txn: Dict[str, Any]) -> Tuple[str, ...]:
    """The identity tuple, case-folded so casing drift is not a diff."""
    return tuple(str(txn[f]).casefold() for f in KEY_FIELDS)


def diff_transactions(expected: List[Dict[str, Any]],
                      actual: List[Dict[str, Any]],
                      tolerance: float = DEFAULT_TOLERANCE) -> Dict[str, Any]:
    """Compare two books of transactions and return every difference.

    The same key legitimately repeats — two accruals on one instrument, one
    date, one type are a normal outcome — so this is a multiset comparison,
    not a dict lookup. Within a key, amounts are sorted and paired positionally:
    pairs within `tolerance` match, pairs outside it are CHANGED, and whatever
    is left over on either side is MISSING or ADDED. That makes the result
    independent of the order rows came back in, which matters because posting
    dates are executed in sequence and nothing guarantees a stable order
    within a date.

    Returns a summary plus a `rows` list of only the differences; matched rows
    are counted, never listed, because a passing case would otherwise return
    the entire book.
    """
    exp_norm = [normalise_txn(r) for r in (expected or [])]
    act_norm = [normalise_txn(r) for r in (actual or [])]

    exp_by_key: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    act_by_key: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    for txn in exp_norm:
        exp_by_key.setdefault(_key_of(txn), []).append(txn)
    for txn in act_norm:
        act_by_key.setdefault(_key_of(txn), []).append(txn)

    rows: List[Dict[str, Any]] = []
    matched = 0

    def _row(status: str, txn: Dict[str, Any],
             exp_amt: Optional[float], act_amt: Optional[float]) -> Dict[str, Any]:
        delta = None
        if exp_amt is not None and act_amt is not None:
            delta = round(act_amt - exp_amt, 6)
        elif act_amt is not None:
            delta = act_amt
        elif exp_amt is not None:
            delta = -exp_amt
        return {
            "status": status,
            "instrumentid": txn["instrumentid"],
            "subinstrumentid": txn["subinstrumentid"],
            "postingdate": txn["postingdate"],
            "effectivedate": txn["effectivedate"],
            "transactiontype": txn["transactiontype"],
            "expected_amount": exp_amt,
            "actual_amount": act_amt,
            "delta": delta,
        }

    for key in sorted(set(exp_by_key) | set(act_by_key)):
        exp_rows = sorted(exp_by_key.get(key, []), key=lambda r: r["amount"])
        act_rows = sorted(act_by_key.get(key, []), key=lambda r: r["amount"])
        paired = min(len(exp_rows), len(act_rows))
        for i in range(paired):
            e_amt = exp_rows[i]["amount"]
            a_amt = act_rows[i]["amount"]
            if abs(a_amt - e_amt) <= tolerance:
                matched += 1
            else:
                rows.append(_row("CHANGED", exp_rows[i], e_amt, a_amt))
        for leftover in exp_rows[paired:]:
            rows.append(_row("MISSING", leftover, leftover["amount"], None))
        for leftover in act_rows[paired:]:
            rows.append(_row("ADDED", leftover, None, leftover["amount"]))

    counts = {
        "expected_total": len(exp_norm),
        "actual_total": len(act_norm),
        "matched": matched,
        "missing": sum(1 for r in rows if r["status"] == "MISSING"),
        "added": sum(1 for r in rows if r["status"] == "ADDED"),
        "changed": sum(1 for r in rows if r["status"] == "CHANGED"),
    }
    counts["differences"] = counts["missing"] + counts["added"] + counts["changed"]

    # Sorting by instrument then date makes the diff table read like the
    # transaction report the user already knows.
    rows.sort(key=lambda r: (r["instrumentid"], r["postingdate"],
                             r["effectivedate"], r["transactiontype"],
                             r["status"]))
    return {"counts": counts, "rows": rows, "passed": counts["differences"] == 0}


def hash_payload(payload: Any) -> str:
    """Stable sha256 over a JSON-serialisable payload."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def dataset_hash(events: List[Dict[str, Any]]) -> str:
    """Content address for a dataset snapshot.

    Two captures of identical data produce the same hash, so a re-baseline
    that changes only the expected transactions reuses the stored rows instead
    of duplicating what can be tens of thousands of rows.
    """
    return hash_payload([
        {
            "event_name": ev["event_name"],
            "definition": ev.get("event_definition"),
            "rows": ev.get("data_rows"),
        }
        for ev in sorted(events, key=lambda e: e["event_name"])
    ])


def scan_nondeterminism(code: str) -> List[str]:
    """Report constructs that would make a replay non-reproducible.

    Custom Code steps can call anything, so a rule reaching for the wall clock
    or a random number will diff against itself on every run. Better to warn
    at capture time than to hand someone a case that fails for no reason.
    """
    if not code:
        return []
    # Matched as calls (`now(`, `today(`), so the closing paren that broke a
    # trailing \b anchor is now part of the pattern rather than after it.
    hits = set(_NONDETERMINISTIC_RE.findall(code))
    hits.update(re.findall(r"\b(random|uuid)\s*\.", code))
    return sorted(hits)


def chunked(items: List[Any], size: int):
    for start in range(0, len(items), size):
        yield start // size, items[start:start + size]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Dependency wiring ───────────────────────────────────────────────────
# server.py owns the Mongo handle and the execution engine. Importing it from
# here would be circular, so it pushes what this module needs at startup.

_deps: Dict[str, Any] = {}


def configure(**kwargs) -> None:
    """Inject server-owned collaborators (db, engine functions, settings)."""
    _deps.update(kwargs)


def _db():
    db = _deps.get("db")
    if db is None:
        raise HTTPException(status_code=500,
                            detail="Regression storage is not configured.")
    return db


router = APIRouter()


# ── Request models ──────────────────────────────────────────────────────

class CaptureCaseRequest(BaseModel):
    name: str
    description: str = ""
    template_id: Optional[str] = None      # None => current workspace rules
    amount_tolerance: Optional[float] = DEFAULT_TOLERANCE
    tags: List[str] = []
    allow_errors: bool = False


class UpdateCaseRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    amount_tolerance: Optional[float] = None
    tags: Optional[List[str]] = None


class RunRequest(BaseModel):
    case_ids: List[str] = []           # empty => every case
    template_id: Optional[str] = None  # None => each case's origin template
    use_pinned_code: bool = False      # replay the code frozen in the baseline
    # Run every case against the workspace rather than a template. `None` is
    # already taken on template_id (it means "the case's own origin"), so this
    # needs its own flag rather than another sentinel.
    use_workspace_rules: bool = False
    # The editor's exact buffer, which may hold edits that were never saved.
    # Omitted => the workspace is assembled server-side from the saved rules,
    # which is what a caller with no editor (the MCP connector) gets.
    workspace_code: Optional[str] = None
    profile: bool = False              # attribute the time, at ~2x the cost


class AcceptRequest(BaseModel):
    note: str = ""


# ── Resolving code to run ───────────────────────────────────────────────

async def _resolve_template_code(template_id: Optional[str]) -> Tuple[str, str, Optional[str]]:
    """Return (dsl_code, template_name, template_id) for a run or a capture.

    Reads `user_templates.combinedCode` directly rather than going through
    deploy: a regression run must never write to dsl_templates or otherwise
    disturb the live runtime. `None` means the current workspace rules.
    """
    db = _db()
    if not template_id:
        get_combined_code = _deps["get_combined_code"]
        combined = await get_combined_code()
        return (combined or {}).get("code") or "", "Workspace rules", None

    tmpl = await db.user_templates.find_one({"id": template_id}, {"_id": 0})
    if tmpl:
        return (tmpl.get("combinedCode") or "",
                tmpl.get("name") or template_id, template_id)

    # Fall back to a deployed dsl_template so a case pinned to a template that
    # only exists in the runtime still runs.
    dsl_doc = await db.dsl_templates.find_one({"id": template_id}, {"_id": 0})
    if dsl_doc:
        return (dsl_doc.get("dsl_code") or "",
                dsl_doc.get("name") or template_id, template_id)

    raise HTTPException(status_code=404, detail="Template not found.")


async def _replay(dsl_code: str, events: List[Dict[str, Any]],
                  on_progress=None, profile: bool = False) -> Dict[str, Any]:
    """Execute `dsl_code` against a frozen dataset snapshot.

    The snapshot carries its own event *definitions*, so the DSL compiles
    against the schema as it stood at capture time. A schema that has since
    changed therefore shows up as a diff rather than as a crash.
    """
    extract_event_names = _deps["extract_event_names_from_dsl"]
    run_full_book = _deps["run_full_book"]

    referenced = extract_event_names(dsl_code) or []
    if not referenced:
        raise HTTPException(status_code=400,
                            detail="The rules reference no events, so there is nothing to run.")

    by_lower = {ev["event_name"].lower(): ev for ev in events}
    all_event_fields, event_metadata, event_data_dict = {}, {}, {}
    missing = []
    for name in referenced:
        ev = by_lower.get(str(name).lower())
        if not ev:
            missing.append(name)
            continue
        canonical = ev["event_name"]
        definition = ev.get("event_definition") or {}
        all_event_fields[canonical] = definition.get("fields", [])
        event_metadata[canonical] = {
            "eventType": definition.get("eventType", "activity")}
        event_data_dict[canonical] = ev.get("data_rows") or []
    if missing:
        raise HTTPException(
            status_code=400,
            detail=("This case's dataset has no data for: "
                    f"{', '.join(missing)}. The template references events the "
                    "baseline was not captured with — recapture the case, or run "
                    "it against a template that matches."))

    return await run_full_book(dsl_code, all_event_fields,
                               event_metadata, event_data_dict,
                               on_progress=on_progress, profile=profile)


# ── Dataset storage (content-addressed, ref-counted) ────────────────────

async def _store_dataset(events: List[Dict[str, Any]]) -> str:
    """Store a dataset snapshot under its content hash, once."""
    db = _db()
    digest = dataset_hash(events)
    existing = await db.regression_datasets.find_one({"dataset_hash": digest},
                                                     {"_id": 0, "dataset_hash": 1})
    if existing:
        return digest
    for ev in events:
        rows = ev.get("data_rows") or []
        # One document per event *chunk*: a single event can hold more rows
        # than a 16MB document allows.
        for index, chunk in chunked(rows, EXPECTED_CHUNK_SIZE):
            await db.regression_datasets.insert_one({
                "dataset_hash": digest,
                "event_name": ev["event_name"],
                "event_definition": ev.get("event_definition"),
                "chunk_index": index,
                "data_rows": chunk,
            })
        if not rows:
            await db.regression_datasets.insert_one({
                "dataset_hash": digest,
                "event_name": ev["event_name"],
                "event_definition": ev.get("event_definition"),
                "chunk_index": 0,
                "data_rows": [],
            })
    return digest


async def _load_dataset(digest: str) -> List[Dict[str, Any]]:
    """Reassemble a dataset snapshot from its chunks."""
    db = _db()
    docs = await db.regression_datasets.find(
        {"dataset_hash": digest}, {"_id": 0}).sort("chunk_index", 1).to_list(None)
    if not docs:
        raise HTTPException(status_code=404,
                            detail="This case's dataset snapshot is missing.")
    by_event: Dict[str, Dict[str, Any]] = {}
    for doc in docs:
        entry = by_event.setdefault(doc["event_name"], {
            "event_name": doc["event_name"],
            "event_definition": doc.get("event_definition"),
            "data_rows": [],
        })
        entry["data_rows"].extend(doc.get("data_rows") or [])
    return list(by_event.values())


async def _release_dataset(digest: str) -> None:
    """Drop a dataset once no version references it.

    Counted rather than fetched: a projected find_one is only truthy by
    accident of the projection, and getting this wrong deletes a snapshot
    another case is still relying on.
    """
    db = _db()
    still_used = await db.regression_case_versions.count_documents(
        {"dataset_hash": digest})
    if not still_used:
        await db.regression_datasets.delete_many({"dataset_hash": digest})


# ── Expected-transaction storage ────────────────────────────────────────

async def _store_expected(case_id: str, version: int,
                          transactions: List[Dict[str, Any]]) -> None:
    db = _db()
    await db.regression_expected.delete_many({"case_id": case_id, "version": version})
    for index, chunk in chunked(transactions, EXPECTED_CHUNK_SIZE):
        await db.regression_expected.insert_one({
            "case_id": case_id, "version": version,
            "chunk_index": index, "transactions": chunk,
        })
    if not transactions:
        await db.regression_expected.insert_one({
            "case_id": case_id, "version": version,
            "chunk_index": 0, "transactions": [],
        })


async def _load_expected(case_id: str, version: int) -> List[Dict[str, Any]]:
    db = _db()
    docs = await db.regression_expected.find(
        {"case_id": case_id, "version": version},
        {"_id": 0}).sort("chunk_index", 1).to_list(None)
    out: List[Dict[str, Any]] = []
    for doc in docs:
        out.extend(doc.get("transactions") or [])
    return out


async def _store_actuals(run_id: str, transactions: List[Dict[str, Any]]) -> None:
    """Keep the exact output a run produced, so accepting promotes THAT.

    Re-running at accept time would be cheaper, but it reads whatever the
    template says *now* — so editing a rule between reviewing a diff and
    accepting it would silently baseline numbers nobody ever saw. The reviewed
    output is the only thing that may become the expectation.
    """
    db = _db()
    await db.regression_actuals.delete_many({"run_id": run_id})
    for index, chunk in chunked(transactions, EXPECTED_CHUNK_SIZE):
        await db.regression_actuals.insert_one({
            "run_id": run_id, "chunk_index": index, "transactions": chunk,
        })
    if not transactions:
        await db.regression_actuals.insert_one({
            "run_id": run_id, "chunk_index": 0, "transactions": [],
        })


async def _load_actuals(run_id: str) -> List[Dict[str, Any]]:
    db = _db()
    docs = await db.regression_actuals.find(
        {"run_id": run_id}, {"_id": 0}).sort("chunk_index", 1).to_list(None)
    out: List[Dict[str, Any]] = []
    for doc in docs:
        out.extend(doc.get("transactions") or [])
    return out


async def _store_diff(run_id: str, rows: List[Dict[str, Any]]) -> None:
    db = _db()
    for index, chunk in chunked(rows, DIFF_CHUNK_SIZE):
        await db.regression_run_diffs.insert_one({
            "run_id": run_id, "chunk_index": index, "rows": chunk,
        })


async def _snapshot_live_dataset() -> List[Dict[str, Any]]:
    """Copy every event definition and its rows out of live storage."""
    db = _db()
    defs = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
    if not defs:
        raise HTTPException(
            status_code=400,
            detail="No event data is loaded, so there is nothing to capture.")
    events = []
    for ev in defs:
        rows_doc = await db.event_data.find_one(
            {"event_name": ev["event_name"]}, {"_id": 0})
        events.append({
            "event_name": ev["event_name"],
            "event_definition": {
                "fields": ev.get("fields", []),
                "eventType": ev.get("eventType", "activity"),
                "eventTable": ev.get("eventTable", "standard"),
            },
            "data_rows": (rows_doc or {}).get("data_rows") or [],
        })
    return events


def _dataset_summary(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_rows = sum(len(ev.get("data_rows") or []) for ev in events)
    dates, instruments = set(), set()
    for ev in events:
        if str((ev.get("event_definition") or {}).get(
                "eventType", "activity")).lower() == "reference":
            continue
        for row in ev.get("data_rows") or []:
            pdate = _canonical_date(_get_ci(row, "postingdate", ""))
            if pdate:
                dates.add(pdate)
            inst = str(_get_ci(row, "instrumentid", "")).strip()
            if inst:
                instruments.add(inst)
    return {
        "events": [{"event_name": ev["event_name"],
                    "row_count": len(ev.get("data_rows") or []),
                    "event_type": (ev.get("event_definition") or {}).get(
                        "eventType", "activity")}
                   for ev in events],
        "total_rows": total_rows,
        "posting_dates": sorted(dates),
        "posting_date_count": len(dates),
        "instrument_count": len(instruments),
    }


# ── Case endpoints ──────────────────────────────────────────────────────

@router.post("/regression/cases")
async def capture_case(request: CaptureCaseRequest):
    """Capture the current dataset + template as a new regression case.

    Capture *executes* the book: a baseline is only meaningful alongside the
    transactions it produced. If any posting date errored, the capture is
    refused unless `allow_errors` is set — a baseline built on a failed run is
    worse than no baseline, because every later run inherits the failure as
    "expected".
    """
    db = _db()
    name = (request.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Case name is required.")
    clash = await db.regression_cases.count_documents(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}})
    if clash:
        raise HTTPException(status_code=409,
                            detail=f'A regression case named "{name}" already exists.')

    dsl_code, template_name, template_id = await _resolve_template_code(request.template_id)
    if not dsl_code.strip():
        raise HTTPException(status_code=400,
                            detail="Nothing to capture — no saved rules and no template selected.")

    events = await _snapshot_live_dataset()
    outcome = await _replay(dsl_code, events)
    if outcome["errors"] and not request.allow_errors:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (f"{len(outcome['errors'])} posting date(s) failed during "
                            "capture. Capturing now would bake those failures into the "
                            "baseline."),
                "errors": outcome["errors"][:20],
                "can_force": True,
            })

    case_id = str(uuid.uuid4())
    digest = await _store_dataset(events)
    transactions = [normalise_txn(t) for t in outcome["transactions"]]
    await _store_expected(case_id, 1, transactions)

    warnings = scan_nondeterminism(dsl_code)
    now = _now()
    await db.regression_cases.insert_one({
        "id": case_id,
        "name": name,
        "description": (request.description or "").strip(),
        "source_template_id": template_id,
        "source_template_name": template_name,
        "amount_tolerance": float(
            DEFAULT_TOLERANCE if request.amount_tolerance is None
            else request.amount_tolerance),
        "tags": request.tags or [],
        "active_version": 1,
        "nondeterminism_warnings": warnings,
        "created_at": now,
        "updated_at": now,
    })
    await db.regression_case_versions.insert_one({
        "case_id": case_id,
        "version": 1,
        "dataset_hash": digest,
        "dataset_summary": _dataset_summary(events),
        "template_snapshot": {"id": template_id, "name": template_name,
                              "dsl_code": dsl_code},
        "expected_count": len(transactions),
        "expected_hash": hash_payload(transactions),
        "note": "Initial capture",
        "captured_with_errors": bool(outcome["errors"]),
        # Capture executed this exact book, so its cost is what the first
        # run should estimate from -- otherwise the bar has no signal at
        # all until a posting date completes.
        "run_ms": (outcome.get("timing") or {}).get("total_ms"),
        "created_from_run_id": None,
        "created_at": now,
    })

    return {
        "success": True,
        "id": case_id,
        "version": 1,
        "transactions_captured": len(transactions),
        "posting_dates": len(outcome["posting_dates"]),
        "dataset_summary": _dataset_summary(events),
        "nondeterminism_warnings": warnings,
        "timing": outcome.get("timing", {}),
        "message": (f'Captured {len(transactions)} transaction(s) across '
                    f'{len(outcome["posting_dates"])} posting date(s) — saved as v1.'),
    }


@router.get("/regression/cases")
async def list_cases():
    """Every case with its latest run status, for the list pane."""
    db = _db()
    cases = await db.regression_cases.find({}, {"_id": 0}).sort("created_at", -1).to_list(500)
    for case in cases:
        version = await db.regression_case_versions.find_one(
            {"case_id": case["id"], "version": case.get("active_version", 1)},
            {"_id": 0, "expected_count": 1, "dataset_summary": 1}) or {}
        case["expected_count"] = version.get("expected_count", 0)
        case["dataset_summary"] = version.get("dataset_summary", {})
        case["version_count"] = await db.regression_case_versions.count_documents(
            {"case_id": case["id"]})
        last = await db.regression_runs.find_one(
            {"case_id": case["id"]}, {"_id": 0, "diff_rows": 0},
            sort=[("started_at", -1)])
        case["last_run"] = last
    return cases


@router.get("/regression/cases/{case_id}")
async def get_case(case_id: str):
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    case["versions"] = await db.regression_case_versions.find(
        {"case_id": case_id},
        {"_id": 0, "template_snapshot.dsl_code": 0}).sort("version", -1).to_list(200)
    return case


@router.put("/regression/cases/{case_id}")
async def update_case(case_id: str, request: UpdateCaseRequest):
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    fields: Dict[str, Any] = {"updated_at": _now()}
    if request.name is not None:
        new_name = request.name.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="Case name cannot be empty.")
        clash = await db.regression_cases.count_documents(
            {"name": {"$regex": f"^{re.escape(new_name)}$", "$options": "i"},
             "id": {"$ne": case_id}})
        if clash:
            raise HTTPException(status_code=409,
                                detail=f'A regression case named "{new_name}" already exists.')
        fields["name"] = new_name
    if request.description is not None:
        fields["description"] = request.description.strip()
    if request.amount_tolerance is not None:
        fields["amount_tolerance"] = float(request.amount_tolerance)
    if request.tags is not None:
        fields["tags"] = request.tags
    await db.regression_cases.update_one({"id": case_id}, {"$set": fields})
    return {"success": True, "message": "Case updated."}


@router.delete("/regression/cases/{case_id}")
async def delete_case(case_id: str):
    """Delete a case and everything hanging off it."""
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0, "name": 1})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    digests = {v["dataset_hash"] for v in await db.regression_case_versions.find(
        {"case_id": case_id}, {"_id": 0, "dataset_hash": 1}).to_list(None)}
    runs = await db.regression_runs.find(
        {"case_id": case_id}, {"_id": 0, "run_id": 1}).to_list(None)
    run_ids = [r["run_id"] for r in runs]
    await db.regression_run_diffs.delete_many({"run_id": {"$in": run_ids}})
    await db.regression_actuals.delete_many({"run_id": {"$in": run_ids}})
    await db.regression_runs.delete_many({"case_id": case_id})
    await db.regression_expected.delete_many({"case_id": case_id})
    await db.regression_case_versions.delete_many({"case_id": case_id})
    await db.regression_cases.delete_one({"id": case_id})
    for digest in digests:
        await _release_dataset(digest)
    return {"success": True, "message": f'Case "{case["name"]}" deleted.'}


@router.get("/regression/cases/{case_id}/versions")
async def list_versions(case_id: str):
    db = _db()
    versions = await db.regression_case_versions.find(
        {"case_id": case_id},
        {"_id": 0, "template_snapshot.dsl_code": 0}).sort("version", -1).to_list(200)
    if not versions:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    return {"case_id": case_id, "versions": versions}


@router.get("/regression/cases/{case_id}/versions/{version}/expected")
async def get_expected(case_id: str, version: int, limit: int = 500, offset: int = 0):
    """Paged view of a version's expected transactions."""
    rows = await _load_expected(case_id, version)
    return {
        "case_id": case_id, "version": version, "total": len(rows),
        "offset": offset, "limit": limit,
        "transactions": rows[offset:offset + limit],
    }


@router.post("/regression/cases/{case_id}/versions/{version}/activate")
async def activate_version(case_id: str, version: int, request: AcceptRequest):
    """Make an older version the baseline again, by copying it forward.

    History stays append-only: reverting to v1 writes v4 as a copy of v1
    rather than rewinding, so the record of what was expected when never
    loses a step. Same shape as the existing saved-rule revert.
    """
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    source = await db.regression_case_versions.find_one(
        {"case_id": case_id, "version": version}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail=f"Version {version} not found.")

    latest = await db.regression_case_versions.find_one(
        {"case_id": case_id}, {"_id": 0, "version": 1}, sort=[("version", -1)])
    new_version = int(latest["version"]) + 1

    await _store_expected(case_id, new_version, await _load_expected(case_id, version))
    doc = dict(source)
    doc.update({
        "version": new_version,
        "note": (request.note or "").strip() or f"Restored from v{version}",
        "restored_from": version,
        "created_from_run_id": None,
        "created_at": _now(),
    })
    await db.regression_case_versions.insert_one(doc)
    await db.regression_cases.update_one(
        {"id": case_id}, {"$set": {"active_version": new_version, "updated_at": _now()}})
    return {"success": True, "version": new_version,
            "message": f"v{version} restored as v{new_version}."}


@router.post("/regression/cases/{case_id}/recapture")
async def recapture_case(case_id: str, request: AcceptRequest):
    """Re-snapshot the CURRENT live dataset into a new version of this case.

    For when the input data legitimately changed — a new month of activity —
    and the baseline should move with it.
    """
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")

    dsl_code, template_name, template_id = await _resolve_template_code(
        case.get("source_template_id"))
    events = await _snapshot_live_dataset()
    outcome = await _replay(dsl_code, events)

    latest = await db.regression_case_versions.find_one(
        {"case_id": case_id}, {"_id": 0, "version": 1}, sort=[("version", -1)])
    new_version = int(latest["version"]) + 1

    digest = await _store_dataset(events)
    transactions = [normalise_txn(t) for t in outcome["transactions"]]
    await _store_expected(case_id, new_version, transactions)
    await db.regression_case_versions.insert_one({
        "case_id": case_id,
        "version": new_version,
        "dataset_hash": digest,
        "dataset_summary": _dataset_summary(events),
        "template_snapshot": {"id": template_id, "name": template_name,
                              "dsl_code": dsl_code},
        "expected_count": len(transactions),
        "expected_hash": hash_payload(transactions),
        "note": (request.note or "").strip() or "Recaptured from current dataset",
        "captured_with_errors": bool(outcome["errors"]),
        # Capture executed this exact book, so its cost is what the first
        # run should estimate from -- otherwise the bar has no signal at
        # all until a posting date completes.
        "run_ms": (outcome.get("timing") or {}).get("total_ms"),
        "created_from_run_id": None,
        "created_at": _now(),
    })
    await db.regression_cases.update_one(
        {"id": case_id}, {"$set": {"active_version": new_version, "updated_at": _now()}})
    return {
        "success": True, "version": new_version,
        "transactions_captured": len(transactions),
        "errors": outcome["errors"],
        "message": (f"Recaptured {len(transactions)} transaction(s) — saved as "
                    f"v{new_version}."),
    }


# ── Running ─────────────────────────────────────────────────────────────

async def _run_one_case(case: Dict[str, Any], batch_id: str,
                        template_id: Optional[str],
                        use_pinned_code: bool,
                        on_progress=None, profile: bool = False,
                        use_workspace_rules: bool = False,
                        workspace_code: Optional[str] = None) -> Dict[str, Any]:
    """Replay one case and record the run + its diff."""
    db = _db()
    case_id = case["id"]
    run_id = str(uuid.uuid4())
    version = int(case.get("active_version", 1))
    started_ts = datetime.now(timezone.utc)
    started = started_ts.isoformat()

    version_doc = await db.regression_case_versions.find_one(
        {"case_id": case_id, "version": version}, {"_id": 0})
    if not version_doc:
        raise HTTPException(status_code=404,
                            detail=f"Case {case_id} has no version {version}.")

    run_doc = {
        "run_id": run_id, "batch_id": batch_id, "case_id": case_id,
        "case_name": case.get("name"), "version_compared": version,
        "status": "running", "started_at": started, "finished_at": None,
        "counts": {}, "errors": [], "template_used": None,
    }
    await db.regression_runs.insert_one(dict(run_doc))

    try:
        if use_pinned_code:
            snapshot = version_doc.get("template_snapshot") or {}
            dsl_code = snapshot.get("dsl_code") or ""
            template_name = f'{snapshot.get("name") or "Baseline"} (pinned v{version})'
            used_id = snapshot.get("id")
        elif use_workspace_rules and workspace_code is not None and workspace_code.strip():
            # Exactly what is on screen, saved or not. The short digest goes in
            # the label so two workspace runs of different edits are told apart
            # in the run list instead of both reading "Workspace rules".
            dsl_code = workspace_code
            template_name = f"Workspace rules (editor \u00b7 {hash_payload(workspace_code)[:8]})"
            used_id = None
        elif use_workspace_rules:
            # No buffer supplied: assemble the workspace from the saved rules.
            dsl_code, template_name, used_id = await _resolve_template_code(None)
        else:
            requested = template_id if template_id is not None else case.get("source_template_id")
            dsl_code, template_name, used_id = await _resolve_template_code(requested)

        if not dsl_code.strip():
            raise HTTPException(
                status_code=400,
                detail=("The workspace has no rules to run." if use_workspace_rules
                        else "The selected template has no code to run."))

        events = await _load_dataset(version_doc["dataset_hash"])
        outcome = await _replay(dsl_code, events, on_progress=on_progress,
                                profile=profile)
        expected = await _load_expected(case_id, version)
        # `or` would swallow a deliberate 0 — someone asking for an exact
        # match must get one, not the default cent.
        configured = case.get("amount_tolerance")
        tolerance = float(DEFAULT_TOLERANCE if configured is None else configured)
        result = diff_transactions(expected, outcome["transactions"], tolerance)

        if outcome.get("cancelled"):
            # A partial book proves nothing: its missing dates would read as
            # thousands of missing transactions. Record the stop and keep no
            # diff, so it can never be mistaken for a result.
            update = {
                "status": "cancelled",
                "finished_at": _now(),
                "counts": {},
                "errors": [],
                "template_used": {"id": used_id, "name": template_name,
                                  "pinned": use_pinned_code},
                "duration_ms": int(
                    (datetime.now(timezone.utc) - started_ts).total_seconds() * 1000),
                "timing": outcome.get("timing", {}),
            }
            await db.regression_runs.update_one({"run_id": run_id}, {"$set": update})
            run_doc.update(update)
            return run_doc

        await _store_diff(run_id, result["rows"])
        # Normalise once here so an accept promotes byte-identical rows.
        await _store_actuals(run_id, [normalise_txn(t) for t in outcome["transactions"]])
        # Any execution error makes the comparison meaningless: a template that
        # failed on every date produces no transactions, which the diff would
        # otherwise report as "everything went missing" — a regression the user
        # would go hunting for instead of fixing the broken rule.
        if outcome["errors"]:
            status = "error"
        elif result["passed"]:
            status = "passed"
        else:
            status = "failed"
        update = {
            "status": status,
            "finished_at": _now(),
            "counts": result["counts"],
            "errors": outcome["errors"],
            # The code is kept with the run, not just its id: accepting must
            # record the source that produced these numbers, and a pinned
            # replay of that version must re-execute the same text even if the
            # template has moved on since.
            "template_used": {"id": used_id, "name": template_name,
                              "pinned": use_pinned_code,
                              "dsl_code": dsl_code},
            "duration_ms": int(
                (datetime.now(timezone.utc) - started_ts).total_seconds() * 1000),
            "timing": outcome.get("timing", {}),
        }
        await db.regression_runs.update_one({"run_id": run_id}, {"$set": update})
        run_doc.update(update)
        return run_doc
    except HTTPException as exc:
        update = {"status": "error", "finished_at": _now(),
                  "errors": [{"posting_date": None, "error": str(exc.detail)}]}
        await db.regression_runs.update_one({"run_id": run_id}, {"$set": update})
        run_doc.update(update)
        return run_doc
    except Exception as exc:                                   # noqa: BLE001
        logger.error(f"Regression run {run_id} failed: {exc}")
        update = {"status": "error", "finished_at": _now(),
                  "errors": [{"posting_date": None, "error": str(exc)}]}
        await db.regression_runs.update_one({"run_id": run_id}, {"$set": update})
        run_doc.update(update)
        return run_doc


async def _prune_runs(case_id: str) -> None:
    """Keep only the most recent runs per case, with their diffs."""
    db = _db()
    keep = int(_deps.get("keep_runs", 50))
    runs = await db.regression_runs.find(
        {"case_id": case_id}, {"_id": 0, "run_id": 1}).sort("started_at", -1).to_list(None)
    stale = [r["run_id"] for r in runs[keep:]]
    if stale:
        await db.regression_run_diffs.delete_many({"run_id": {"$in": stale}})
        await db.regression_actuals.delete_many({"run_id": {"$in": stale}})
        await db.regression_runs.delete_many({"run_id": {"$in": stale}})


async def _execute_batch(batch_id: str, cases: List[Dict[str, Any]],
                         template_id: Optional[str], use_pinned_code: bool,
                         profile: bool = False,
                         use_workspace_rules: bool = False,
                         workspace_code: Optional[str] = None) -> None:
    """Run a batch of cases sequentially, updating the batch doc as it goes.

    Nothing here is allowed to escape. This runs detached as a background
    task with the UI polling the batch document for progress, so an exception
    that got out would leave the batch stuck on "running" and the modal
    spinning forever. Every case is therefore isolated, and the batch is
    closed out in a finally.
    """
    db = _db()
    try:
        for index, case in enumerate(cases):
            stop = await db.regression_batches.find_one(
                {"batch_id": batch_id}, {"_id": 0, "cancel_requested": 1})
            if (stop or {}).get("cancel_requested"):
                break
            await db.regression_batches.update_one(
                {"batch_id": batch_id},
                {"$set": {"current_index": index, "current_case": case.get("name")}})
            async def _progress(done, total_dates, pdate, _i=index, _c=case):
                """Publish posting-date progress, and honour a stop request.

                One small update and one small read per date. A book runs a
                handful to a few dozen dates, so this is negligible next to
                executing one. Returning False stops the book.
                """
                await db.regression_batches.update_one(
                    {"batch_id": batch_id},
                    {"$set": {"date_index": done, "date_total": total_dates,
                              "current_date": pdate}})
                doc = await db.regression_batches.find_one(
                    {"batch_id": batch_id}, {"_id": 0, "cancel_requested": 1})
                return not (doc or {}).get("cancel_requested")

            try:
                await db.regression_batches.update_one(
                    {"batch_id": batch_id},
                    {"$set": {"date_index": 0, "date_total": 0,
                              "current_date": None}})
                run = await _run_one_case(case, batch_id, template_id,
                                          use_pinned_code, on_progress=_progress,
                                          profile=profile,
                                          use_workspace_rules=use_workspace_rules,
                                          workspace_code=workspace_code)
                await _prune_runs(case["id"])
                entry = {"case_id": case["id"], "case_name": case.get("name"),
                         "run_id": run["run_id"], "status": run["status"],
                         "counts": run.get("counts", {})}
            except Exception as exc:                               # noqa: BLE001
                # A case that cannot even be set up (missing version document,
                # unreadable snapshot) must not take the rest of the suite
                # down with it.
                logger.error(f"Regression case {case.get('id')} could not run: {exc}")
                entry = {"case_id": case["id"], "case_name": case.get("name"),
                         "run_id": None, "status": "error", "counts": {},
                         "error": str(exc)}
            await db.regression_batches.update_one(
                {"batch_id": batch_id}, {"$push": {"results": entry}})
    finally:
        results = (await db.regression_batches.find_one(
            {"batch_id": batch_id}, {"_id": 0, "results": 1}) or {}).get("results", [])
        final = await db.regression_batches.find_one(
            {"batch_id": batch_id}, {"_id": 0, "cancel_requested": 1}) or {}
        await db.regression_batches.update_one(
            {"batch_id": batch_id},
            {"$set": {
                # Still "complete" in the sense that nothing is running: the
                # poller stops either way. `cancelled` says which it was.
                "status": "complete",
                "cancelled": bool(final.get("cancel_requested")),
                "finished_at": _now(),
                "passed": sum(1 for r in results if r["status"] == "passed"),
                "failed": sum(1 for r in results if r["status"] == "failed"),
                "errored": sum(1 for r in results if r["status"] == "error"),
                "stopped": sum(1 for r in results if r["status"] == "cancelled"),
            }})


@router.post("/regression/run")
async def run_regression(request: RunRequest, background: BackgroundTasks):
    """Start a regression batch and return immediately with a batch id.

    Running a whole suite means replaying every posting date of every case, so
    this cannot be a blocking request. The caller polls
    /regression/batches/{batch_id} for progress.
    """
    db = _db()
    if request.case_ids:
        cases = await db.regression_cases.find(
            {"id": {"$in": request.case_ids}}, {"_id": 0}).to_list(500)
    else:
        cases = await db.regression_cases.find({}, {"_id": 0}).to_list(500)
    if not cases:
        raise HTTPException(status_code=404, detail="No regression cases to run.")

    batch_id = str(uuid.uuid4())
    # What did these cases cost last time? A regression case replays a frozen
    # dataset, so its previous duration is a good predictor -- and it means the
    # bar can show a real estimate immediately instead of waiting out the first
    # posting date for a live signal.
    baseline_ms = 0
    for case in cases:
        prior = await db.regression_runs.find_one(
            {"case_id": case["id"], "status": {"$ne": "error"}},
            {"_id": 0, "duration_ms": 1},
            sort=[("started_at", -1)])
        # `is None` rather than falsy: a genuinely instant run is history, and
        # must not be mistaken for the absence of it.
        cost = int(prior.get("duration_ms") or 0) if prior is not None else None
        if cost is None:
            # Never run before: the capture executed the same book, so use it.
            version = await db.regression_case_versions.find_one(
                {"case_id": case["id"], "version": case.get("active_version", 1)},
                {"_id": 0, "run_ms": 1})
            cost = int((version or {}).get("run_ms") or 0)
        baseline_ms += cost

    await db.regression_batches.insert_one({
        "batch_id": batch_id,
        "status": "running",
        "total": len(cases),
        "current_index": 0,
        "current_case": cases[0].get("name"),
        "results": [],
        "baseline_ms": baseline_ms,
        "started_at": _now(),
        "finished_at": None,
    })
    background.add_task(_execute_batch, batch_id, cases,
                        request.template_id, request.use_pinned_code,
                        request.profile,
                        request.use_workspace_rules, request.workspace_code)
    if request.use_pinned_code:
        against = "each case's pinned baseline code"
    elif request.use_workspace_rules:
        against = ("the workspace rules as they stand in the editor"
                   if (request.workspace_code or "").strip()
                   else "the saved workspace rules")
    elif request.template_id:
        against = "the selected template"
    else:
        against = "each case's origin template"
    return {"success": True, "batch_id": batch_id, "total": len(cases),
            "running_against": against,
            "message": f"Running {len(cases)} regression case(s) against {against}…"}


def _batch_progress(batch: Dict[str, Any]) -> Dict[str, Any]:
    """Overall completion and a time estimate for a running batch.

    Cases are weighted equally and the case in flight contributes the
    fraction of its posting dates already executed, so a single-case run --
    where case-level progress alone would read 0% until it finished -- still
    moves. The estimate is elapsed-over-fraction, which assumes dates cost
    roughly the same; it is labelled an estimate for that reason.
    """
    total_cases = max(int(batch.get("total") or 0), 1)
    done_cases = len(batch.get("results") or [])
    date_total = int(batch.get("date_total") or 0)
    date_index = int(batch.get("date_index") or 0)

    live_fraction = 0.0
    if date_total:
        live_fraction = min((done_cases + date_index / date_total) / total_cases, 0.999)
    elif done_cases:
        live_fraction = min(done_cases / total_cases, 0.999)

    elapsed_ms = None
    started = batch.get("started_at")
    if started:
        try:
            start = datetime.fromisoformat(started)
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            end = batch.get("finished_at")
            stop = (datetime.fromisoformat(end) if end
                    else datetime.now(timezone.utc))
            if stop.tzinfo is None:
                stop = stop.replace(tzinfo=timezone.utc)
            elapsed_ms = int((stop - start).total_seconds() * 1000)
        except Exception:
            elapsed_ms = None

    baseline_ms = int(batch.get("baseline_ms") or 0)
    fraction, source = live_fraction, "live"

    if batch.get("status") == "complete":
        fraction, source = 1.0, "done"
    elif live_fraction < 0.05 and baseline_ms > 0 and elapsed_ms is not None:
        # Not enough dates have finished to extrapolate from, but this case has
        # run before. Cap below 95% so a slower-than-usual run cannot park the
        # bar at 100% while it is still working.
        fraction = min(elapsed_ms / baseline_ms, 0.95)
        source = "history"

    eta_ms = None
    if fraction >= 1.0:
        eta_ms = 0
    elif source == "history" and elapsed_ms is not None:
        eta_ms = max(baseline_ms - elapsed_ms, 0)
    elif 0.02 < fraction < 1.0 and elapsed_ms is not None:
        # Below a couple of percent with no history, the extrapolation swings
        # too wildly to show.
        eta_ms = max(int(elapsed_ms / fraction - elapsed_ms), 0)

    return {
        "percent": round(fraction * 100, 1),
        "source": source,
        "cases_done": done_cases,
        "cases_total": total_cases,
        "date_index": date_index,
        "date_total": date_total,
        "current_date": batch.get("current_date"),
        "elapsed_ms": elapsed_ms,
        "eta_ms": eta_ms,
    }


@router.post("/regression/batches/{batch_id}/cancel")
async def cancel_batch(batch_id: str):
    """Ask a running batch to stop.

    Cooperative, not immediate: a posting date is one indivisible call into
    the engine, so the run stops at the next date boundary. On a book whose
    dates take a couple of minutes, that is how long it can take to wind up.
    """
    db = _db()
    batch = await db.regression_batches.find_one({"batch_id": batch_id}, {"_id": 0})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found.")
    if batch.get("status") == "complete":
        return {"success": True, "already_finished": True,
                "message": "That run has already finished."}
    await db.regression_batches.update_one(
        {"batch_id": batch_id}, {"$set": {"cancel_requested": True}})
    return {"success": True, "already_finished": False,
            "message": "Stopping after the current posting date…"}


@router.get("/regression/batches/{batch_id}")
async def get_batch(batch_id: str):
    db = _db()
    batch = await db.regression_batches.find_one({"batch_id": batch_id}, {"_id": 0})
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found.")
    batch["progress"] = _batch_progress(batch)
    return batch


@router.get("/regression/runs")
async def list_runs(case_id: Optional[str] = None, limit: int = 50):
    db = _db()
    query = {"case_id": case_id} if case_id else {}
    return await db.regression_runs.find(query, {"_id": 0}).sort(
        "started_at", -1).to_list(limit)


@router.get("/regression/runs/{run_id}")
async def get_run(run_id: str):
    db = _db()
    run = await db.regression_runs.find_one({"run_id": run_id}, {"_id": 0})
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    return run


@router.get("/regression/runs/{run_id}/diff")
async def get_run_diff(run_id: str, status: Optional[str] = None,
                       limit: int = 200, offset: int = 0):
    """Paged diff rows, optionally filtered to one status.

    A failing case can produce tens of thousands of differences, so the table
    pages server-side rather than shipping the whole diff to the browser.
    """
    db = _db()
    run = await db.regression_runs.find_one({"run_id": run_id}, {"_id": 0})
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    docs = await db.regression_run_diffs.find(
        {"run_id": run_id}, {"_id": 0}).sort("chunk_index", 1).to_list(None)
    rows: List[Dict[str, Any]] = []
    for doc in docs:
        rows.extend(doc.get("rows") or [])
    if status:
        wanted = status.strip().upper()
        rows = [r for r in rows if r.get("status") == wanted]
    return {
        "run_id": run_id, "total": len(rows), "offset": offset, "limit": limit,
        "counts": run.get("counts", {}), "status": run.get("status"),
        "rows": rows[offset:offset + limit],
    }


@router.post("/regression/runs/{run_id}/accept")
async def accept_run(run_id: str, request: AcceptRequest):
    """Promote a run's actual output to be the new expected baseline.

    Writes a NEW version rather than editing the current one, carrying the run
    it came from and the diff that was accepted, so there is always a record of
    who moved the baseline and what moved with it.
    """
    db = _db()
    settings = _deps.get("settings")
    if settings is not None and getattr(settings, "require_agent_approval", False):
        raise HTTPException(
            status_code=403,
            detail=("Maker-checker is enabled: accepting new expected results "
                    "requires a reviewer. Approve the change outside the UI or "
                    "set REQUIRE_AGENT_APPROVAL=false."))

    run = await db.regression_runs.find_one({"run_id": run_id}, {"_id": 0})
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    if run.get("status") == "error":
        raise HTTPException(status_code=409,
                            detail="This run errored, so its output cannot become a baseline.")

    case_id = run["case_id"]
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")
    source_version = await db.regression_case_versions.find_one(
        {"case_id": case_id, "version": run["version_compared"]}, {"_id": 0})
    if not source_version:
        raise HTTPException(status_code=404, detail="Compared version no longer exists.")

    if run.get("accepted_as_version"):
        raise HTTPException(
            status_code=409,
            detail=(f"This run was already accepted as v{run['accepted_as_version']}. "
                    "Run the case again if you want to move the baseline further."))

    # Promote exactly what this run produced. Re-running here would read the
    # template as it stands NOW, so a rule edited between reviewing the diff
    # and pressing accept would baseline numbers the reviewer never saw.
    transactions = await _load_actuals(run_id)
    if not transactions:
        raise HTTPException(
            status_code=409,
            detail=("This run's output is no longer stored, so there is nothing "
                    "to promote. Run the case again and accept that run."))

    template = run.get("template_used") or {}
    template_name = template.get("name")
    template_id = template.get("id")
    # The run kept the code it executed. Re-reading the template here would
    # record source that never produced these numbers, and would then be
    # replayed by any later pinned run of this version.
    dsl_code = template.get("dsl_code")
    if dsl_code is None:
        snapshot = source_version.get("template_snapshot") or {}
        dsl_code = (snapshot.get("dsl_code") or "") if template.get("pinned")             else (await _resolve_template_code(template_id))[0]

    latest = await db.regression_case_versions.find_one(
        {"case_id": case_id}, {"_id": 0, "version": 1}, sort=[("version", -1)])
    new_version = int(latest["version"]) + 1

    await _store_expected(case_id, new_version, transactions)
    await db.regression_case_versions.insert_one({
        "case_id": case_id,
        "version": new_version,
        "dataset_hash": source_version["dataset_hash"],
        "dataset_summary": source_version.get("dataset_summary", {}),
        "template_snapshot": {"id": template_id, "name": template_name,
                              "dsl_code": dsl_code},
        "expected_count": len(transactions),
        "expected_hash": hash_payload(transactions),
        "note": (request.note or "").strip() or f"Accepted from run {run_id[:8]}",
        "captured_with_errors": bool(run.get("errors")),
        "created_from_run_id": run_id,
        "accepted_diff": run.get("counts", {}),
        "created_at": _now(),
    })
    await db.regression_cases.update_one(
        {"id": case_id}, {"$set": {"active_version": new_version, "updated_at": _now()}})
    await db.regression_runs.update_one(
        {"run_id": run_id}, {"$set": {"accepted_as_version": new_version}})

    return {
        "success": True, "version": new_version,
        "expected_count": len(transactions),
        "message": (f"New expected results saved as v{new_version} "
                    f"({len(transactions)} transaction(s))."),
    }


@router.delete("/regression/runs/{run_id}")
async def delete_run(run_id: str):
    db = _db()
    result = await db.regression_runs.delete_one({"run_id": run_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Run not found.")
    await db.regression_run_diffs.delete_many({"run_id": run_id})
    await db.regression_actuals.delete_many({"run_id": run_id})
    return {"success": True, "message": "Run deleted."}


@router.delete("/regression/cases/{case_id}/runs")
async def clear_case_runs(case_id: str):
    """Delete every run recorded for one case.

    Clears history only. The case, its dataset snapshot and every baseline
    version survive, so the next run still compares against the same expected
    results — this is for discarding noisy or superseded runs, not for
    resetting what the case expects.

    A version accepted from one of these runs is untouched; it keeps its own
    record of where it came from.
    """
    db = _db()
    case = await db.regression_cases.find_one({"id": case_id}, {"_id": 0, "name": 1})
    if not case:
        raise HTTPException(status_code=404, detail="Regression case not found.")

    runs = await db.regression_runs.find(
        {"case_id": case_id}, {"_id": 0, "run_id": 1}).to_list(None)
    run_ids = [r["run_id"] for r in runs]
    if run_ids:
        await db.regression_run_diffs.delete_many({"run_id": {"$in": run_ids}})
        await db.regression_actuals.delete_many({"run_id": {"$in": run_ids}})
    await db.regression_runs.delete_many({"case_id": case_id})

    return {
        "success": True,
        "deleted": len(run_ids),
        "message": (f"Cleared {len(run_ids)} run(s) from \"{case['name']}\"."
                    if run_ids else "No run history to clear."),
    }


@router.get("/regression/runs/{run_id}/export")
async def export_run_diff(run_id: str):
    """Download a run's differences as .xlsx.

    Two sheets: a one-line summary so the file explains itself, and every
    difference row. Reviewers work in Excel, and a diff that only exists
    inside the modal cannot be circulated or attached to a change record.
    """
    import io as _io
    import openpyxl
    from fastapi.responses import Response as _Response

    db = _db()
    run = await db.regression_runs.find_one({"run_id": run_id}, {"_id": 0})
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    docs = await db.regression_run_diffs.find(
        {"run_id": run_id}, {"_id": 0}).sort("chunk_index", 1).to_list(None)
    rows: List[Dict[str, Any]] = []
    for doc in docs:
        rows.extend(doc.get("rows") or [])

    counts = run.get("counts", {})
    wb = openpyxl.Workbook()

    ws = wb.active
    ws.title = "summary"
    ws.append(["Case", run.get("case_name", "")])
    ws.append(["Run id", run_id])
    ws.append(["Baseline version", run.get("version_compared")])
    ws.append(["Template used", (run.get("template_used") or {}).get("name", "")])
    ws.append(["Status", run.get("status", "")])
    ws.append(["Started", run.get("started_at", "")])
    ws.append(["Finished", run.get("finished_at", "")])
    ws.append([])
    ws.append(["Expected total", counts.get("expected_total", 0)])
    ws.append(["Actual total", counts.get("actual_total", 0)])
    ws.append(["Matched", counts.get("matched", 0)])
    ws.append(["Missing", counts.get("missing", 0)])
    ws.append(["Added", counts.get("added", 0)])
    ws.append(["Amount changed", counts.get("changed", 0)])

    ws_diff = wb.create_sheet("differences")
    ws_diff.append(["Status", "InstrumentId", "SubInstrumentId", "PostingDate",
                    "EffectiveDate", "TransactionType", "ExpectedAmount",
                    "ActualAmount", "Delta"])
    for row in rows:
        ws_diff.append([
            row.get("status"), row.get("instrumentid"), row.get("subinstrumentid"),
            row.get("postingdate"), row.get("effectivedate"),
            row.get("transactiontype"), row.get("expected_amount"),
            row.get("actual_amount"), row.get("delta"),
        ])

    buffer = _io.BytesIO()
    wb.save(buffer)
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", run.get("case_name") or "regression")
    return _Response(
        content=buffer.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":
                 f'attachment; filename="{safe_name}_diff_{run_id[:8]}.xlsx"'},
    )


# ── Indexes ─────────────────────────────────────────────────────────────

# Every query this module makes, in the shape it makes it. Regression data is
# the one part of the app that grows without bound — a case keeps every
# version, and every version keeps a whole book of transactions — so the
# collection scans that are fine for a handful of saved rules are not fine
# here.
_INDEXES = {
    "regression_cases":         [([("id", 1)], True), ([("name", 1)], False)],
    "regression_case_versions": [([("case_id", 1), ("version", -1)], True),
                                 ([("dataset_hash", 1)], False)],
    "regression_datasets":      [([("dataset_hash", 1), ("event_name", 1),
                                   ("chunk_index", 1)], False)],
    "regression_expected":      [([("case_id", 1), ("version", 1),
                                   ("chunk_index", 1)], False)],
    "regression_runs":          [([("run_id", 1)], True),
                                 ([("case_id", 1), ("started_at", -1)], False),
                                 ([("batch_id", 1)], False)],
    "regression_run_diffs":     [([("run_id", 1), ("chunk_index", 1)], False)],
    "regression_actuals":       [([("run_id", 1), ("chunk_index", 1)], False)],
    "regression_batches":       [([("batch_id", 1)], True)],
}


async def ensure_indexes() -> None:
    """Create the regression indexes, once, at startup.

    Idempotent — Mongo ignores a create_index for an index that already
    exists with the same spec. Never fatal: an index that cannot be built
    (no Mongo, a permission-restricted user, a pre-existing conflicting
    index) must not stop the app from serving, so every failure is logged
    and stepped over.
    """
    db = _deps.get("db")
    if db is None:
        return
    for collection, specs in _INDEXES.items():
        for keys, unique in specs:
            try:
                await db[collection].create_index(keys, unique=unique,
                                                  background=True)
            except Exception as exc:                            # noqa: BLE001
                logger.warning(
                    f"Could not create index {keys} on {collection}: {exc}")
