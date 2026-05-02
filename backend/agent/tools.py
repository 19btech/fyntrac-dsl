"""Tool registry for the autonomous agent.

Every tool is a thin async wrapper around an existing service or repository
operation. Tools are exposed to the LLM with a JSON-Schema description so
provider-native function calling can be used.

DESIGN RULES
------------
* No tool exposes `customCode:` or any Python-escape hatch. The agent must
  compose all logic from rules + DSL functions only. This is enforced both
  here (at the tool boundary) and inside the existing DSL AST validators in
  `server.py` (which reject imports / dunder access / dangerous builtins).
* Tools that mutate shared state (`clear_all_data`, `delete_template`) are
  marked DESTRUCTIVE and require an explicit `confirm=True` argument that the
  caller (the runtime) only forwards after the user clicks Approve.
* Every tool returns a JSON-serialisable dict. Errors raise `ToolError` with
  a user-readable message; the runtime feeds these back to the LLM as
  observations so it can self-correct.
* Tool handlers are deliberately idempotent where possible — the LLM will
  retry on errors and the same tool call must not duplicate state.
"""

from __future__ import annotations

import logging
import random
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────────────────

class ToolError(Exception):
    """Raised by a tool when an invocation fails. The runtime serialises the
    message back to the LLM so it can attempt a fix on the next turn."""


# ──────────────────────────────────────────────────────────────────────────
# Server bridge
# ──────────────────────────────────────────────────────────────────────────
# Tools need access to the live Mongo handle, in-memory fallback dict, and the
# DSL helpers defined inside server.py. We resolve them lazily via a bridge
# object that server.py wires up at module import time. This avoids a cyclic
# import (agent <-> server).

class _ServerBridge:
    """Late-bound references to server-level objects."""

    db = None                     # AsyncIOMotorDatabase
    in_memory_data: dict | None = None
    use_in_memory_getter: Callable[[], bool] | None = None
    helpers: dict[str, Callable] = {}     # name -> callable

    @classmethod
    def is_in_memory(cls) -> bool:
        if cls.use_in_memory_getter is None:
            return False
        try:
            return bool(cls.use_in_memory_getter())
        except Exception:
            return False


def configure_bridge(*, db, in_memory_data, use_in_memory_getter, helpers: dict):
    """Called once from server.py after both modules have loaded."""
    _ServerBridge.db = db
    _ServerBridge.in_memory_data = in_memory_data
    _ServerBridge.use_in_memory_getter = use_in_memory_getter
    _ServerBridge.helpers = dict(helpers)


def _h(name: str) -> Callable:
    fn = _ServerBridge.helpers.get(name)
    if fn is None:
        raise ToolError(f"Internal: helper '{name}' is not registered")
    return fn


# ──────────────────────────────────────────────────────────────────────────
# Storage helpers (DB or in-memory fallback — mirror server.py behaviour)
# ──────────────────────────────────────────────────────────────────────────

async def _find_event_def(event_name: str) -> dict | None:
    db = _ServerBridge.db
    try:
        if db is not None:
            doc = await db.event_definitions.find_one(
                {"event_name": {"$regex": f"^{re.escape(event_name)}$", "$options": "i"}},
                {"_id": 0},
            )
            if doc:
                return doc
    except Exception as exc:
        logger.warning("DB lookup for event_def failed: %s", exc)
    mem = (_ServerBridge.in_memory_data or {}).get("event_definitions") or []
    for e in mem:
        if str(e.get("event_name", "")).lower() == event_name.lower():
            return e
    return None


async def _list_event_defs() -> list[dict]:
    db = _ServerBridge.db
    try:
        if db is not None:
            docs = await db.event_definitions.find({}, {"_id": 0}).to_list(1000)
            if docs:
                return docs
    except Exception as exc:
        logger.warning("DB list events failed: %s", exc)
    return list((_ServerBridge.in_memory_data or {}).get("event_definitions") or [])


async def _list_templates() -> list[dict]:
    db = _ServerBridge.db
    try:
        if db is not None:
            docs = await db.dsl_templates.find({}, {"_id": 0}).to_list(1000)
            if docs:
                return docs
    except Exception as exc:
        logger.warning("DB list templates failed: %s", exc)
    return list((_ServerBridge.in_memory_data or {}).get("templates") or [])


async def _find_template(template_id_or_name: str) -> dict | None:
    db = _ServerBridge.db
    if not template_id_or_name:
        return None
    try:
        if db is not None:
            doc = await db.dsl_templates.find_one({"id": template_id_or_name}, {"_id": 0})
            if doc:
                return doc
            doc = await db.dsl_templates.find_one({"name": template_id_or_name}, {"_id": 0})
            if doc:
                return doc
    except Exception:
        pass
    for t in (_ServerBridge.in_memory_data or {}).get("templates") or []:
        if t.get("id") == template_id_or_name or t.get("name") == template_id_or_name:
            return t
    return None


# ──────────────────────────────────────────────────────────────────────────
# Sample-data generation
# ──────────────────────────────────────────────────────────────────────────

_DATATYPE_DEFAULTS: dict[str, Callable[[random.Random, dict], Any]] = {
    "decimal": lambda rng, hints: round(rng.uniform(*hints.get("range", (100.0, 100000.0))), 2),
    "integer": lambda rng, hints: rng.randint(*hints.get("range", (1, 1000))),
    "int": lambda rng, hints: rng.randint(*hints.get("range", (1, 1000))),
    "boolean": lambda rng, hints: rng.choice([True, False]),
    "string": lambda rng, hints: rng.choice(hints.get("choices", ["A", "B", "C"])),
    "date": lambda rng, hints: hints.get("default_date", "2026-01-01"),
}


# Domain-aware field-name heuristics. Each entry: (matcher_predicate, range,
# datatype_override). The first match wins. Ranges are calibrated so that
# downstream multiplications (PD × LGD × EAD, principal × rate, etc.) produce
# realistic — not astronomical — accounting amounts.
def _name_match(name: str, *needles: str) -> bool:
    return any(n in name for n in needles)


_FIELD_HEURISTICS: list[tuple[Callable[[str], bool], dict, str | None]] = [
    # --- Probabilities & ratios (0..1) -----------------------------------
    (lambda n: _name_match(n, "pd", "prob_default", "probability"),
        {"range": (0.001, 0.10), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "lgd", "loss_given"),
        {"range": (0.20, 0.60), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "ccf", "credit_conversion"),
        {"range": (0.20, 1.00), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "recovery_rate", "recoveryrate"),
        {"range": (0.10, 0.70), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "ltv", "loan_to_value"),
        {"range": (0.40, 0.95), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "dti", "debt_to_income"),
        {"range": (0.10, 0.50), "decimals": 4}, "decimal"),
    (lambda n: _name_match(n, "utilization", "utilisation", "drawdown"),
        {"range": (0.10, 0.95), "decimals": 4}, "decimal"),
    # --- Interest rates (annualised, decimal form) -----------------------
    (lambda n: _name_match(n, "eir", "effective_int", "effective_yield",
                            "rate", "coupon", "yield", "apr", "interest"),
        {"range": (0.01, 0.12), "decimals": 6}, "decimal"),
    (lambda n: _name_match(n, "spread", "margin"),
        {"range": (0.005, 0.05), "decimals": 6}, "decimal"),
    # --- Money amounts (notional / balance / principal / EAD) -----------
    (lambda n: _name_match(n, "ead", "exposure_at_default", "exposure"),
        {"range": (10_000.0, 500_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "principal", "notional", "face_value", "facevalue"),
        {"range": (10_000.0, 1_000_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "balance", "outstanding", "carrying", "book_value"),
        {"range": (5_000.0, 800_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "fee", "commission", "charge"),
        {"range": (10.0, 5_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "payment", "installment", "instalment", "premium"),
        {"range": (100.0, 10_000.0), "decimals": 2}, "decimal"),
    (lambda n: n.endswith("_amount") or n.endswith("amount") or n == "amount",
        {"range": (100.0, 100_000.0), "decimals": 2}, "decimal"),
    # --- Counts & terms --------------------------------------------------
    (lambda n: _name_match(n, "term_months", "tenor_months", "n_periods", "num_periods"),
        {"range": (12, 360)}, "integer"),
    (lambda n: _name_match(n, "term", "tenor"),
        {"range": (1, 30)}, "integer"),  # years
    (lambda n: _name_match(n, "stage"),
        {"range": (1, 3)}, "integer"),
    (lambda n: _name_match(n, "rating"),
        {"range": (1, 10)}, "integer"),
    (lambda n: _name_match(n, "fico", "credit_score"),
        {"range": (550, 820)}, "integer"),
    (lambda n: _name_match(n, "days_past_due", "dpd", "days_overdue", "delinquent_days"),
        {"range": (0, 180)}, "integer"),
    (lambda n: _name_match(n, "period", "month_number", "year_number"),
        {"range": (1, 60)}, "integer"),
    (lambda n: _name_match(n, "count", "qty", "quantity"),
        {"range": (1, 100)}, "integer"),
    # --- String enums by name -------------------------------------------
    (lambda n: _name_match(n, "currency", "ccy"),
        {"choices": ["USD", "EUR", "GBP", "JPY", "CAD"]}, "string"),
    (lambda n: _name_match(n, "country"),
        {"choices": ["US", "GB", "DE", "FR", "JP", "CA"]}, "string"),
    (lambda n: _name_match(n, "product", "product_type", "producttype"),
        {"choices": ["Mortgage", "AutoLoan", "CreditCard", "PersonalLoan", "CommercialLoan"]}, "string"),
    (lambda n: _name_match(n, "segment", "portfolio"),
        {"choices": ["Retail", "Corporate", "SME", "Sovereign"]}, "string"),
    (lambda n: _name_match(n, "status"),
        {"choices": ["Active", "Performing", "Closed", "Defaulted"]}, "string"),
    (lambda n: _name_match(n, "frequency", "freq"),
        {"choices": ["Monthly", "Quarterly", "Annual"]}, "string"),
    (lambda n: _name_match(n, "side", "drcr"),
        {"choices": ["Debit", "Credit"]}, "string"),
]


# Hard sanity bounds — even a user-supplied field_hint cannot escape these.
# The agent has been seen to set range=(1,100) for a "rate" field, producing
# a 5000% interest rate that compounded into trillions in dry-runs.
_SANITY_BOUNDS: list[tuple[Callable[[str], bool], tuple[float, float], str]] = [
    (lambda n: _name_match(n, "pd", "lgd", "ccf", "ltv", "dti", "recovery_rate"),
        (0.0, 1.0), "probability/ratio fields must be in [0, 1]"),
    (lambda n: _name_match(n, "rate", "coupon", "yield", "apr", "eir", "spread", "margin"),
        (0.0, 1.0), "annualised rate fields must be in [0, 1] (5% = 0.05, NOT 5)"),
    (lambda n: _name_match(n, "principal", "notional", "balance", "ead", "exposure"),
        (0.0, 1_000_000_000.0), "principal/balance must be < $1B per row"),
]


def _enforce_sanity_bounds(field_name: str, hints: dict) -> None:
    """If the agent supplied range hints, make sure they are within the hard
    bounds for the field's domain. This prevents the 'rate=(1,100)' class of
    bug that produces astronomical transaction amounts during dry-run."""
    rng = hints.get("range")
    if not rng or len(rng) != 2:
        return
    lo, hi = rng
    name = field_name.lower()
    for matcher, (mn, mx), reason in _SANITY_BOUNDS:
        if matcher(name):
            if lo < mn or hi > mx:
                raise ToolError(
                    f"field '{field_name}': hint range ({lo}, {hi}) violates "
                    f"sanity bound [{mn}, {mx}]. {reason}. "
                    f"Adjust the range and try again."
                )
            return


def _generate_value(field: dict, rng: random.Random, field_hints: dict) -> Any:
    name = (field.get("name") or "").lower()
    dtype = (field.get("datatype") or "decimal").lower()
    user_hints = (field_hints or {}).get(name, {})
    _enforce_sanity_bounds(field.get("name") or "", user_hints)

    # Apply heuristic match (first hit wins) unless the user already pinned it
    chosen_hints: dict = {}
    chosen_dtype = dtype
    if dtype in ("decimal", "integer", "int", "string"):
        for matcher, hint, dtype_override in _FIELD_HEURISTICS:
            if matcher(name):
                chosen_hints = dict(hint)
                if dtype_override:
                    chosen_dtype = dtype_override
                break

    # User hints win over heuristics
    chosen_hints.update(user_hints)

    fn = _DATATYPE_DEFAULTS.get(chosen_dtype, _DATATYPE_DEFAULTS["string"])
    val = fn(rng, chosen_hints)
    # Honour decimal precision if requested
    if isinstance(val, float):
        decimals = chosen_hints.get("decimals")
        if isinstance(decimals, int) and 0 <= decimals <= 8:
            val = round(val, decimals)
    return val


def _make_sample_rows(
    event_def: dict,
    instrument_ids: list[str],
    posting_dates: list[str],
    field_hints: dict | None = None,
    seed: int = 42,
) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    is_reference = event_def.get("eventType") == "reference"
    for posting_date in posting_dates:
        for inst in instrument_ids:
            row: dict[str, Any] = {}
            if not is_reference:
                row["postingdate"] = posting_date
                row["effectivedate"] = posting_date
                row["instrumentid"] = inst
            for f in event_def.get("fields", []):
                row[f["name"]] = _generate_value(f, rng, field_hints or {})
            rows.append(row)
    return rows


def _audit_sample_rows(rows: list[dict], event_def: dict) -> list[str]:
    """Catch pathological generated data: all-zero columns, identical values
    across all rows for a numeric field, or values that would make the
    downstream accounting nonsensical."""
    warnings: list[str] = []
    if not rows:
        return ["no rows generated"]
    field_specs = {(f.get("name") or "").lower(): f for f in event_def.get("fields", [])}
    columns: dict[str, list[Any]] = {}
    for r in rows:
        for k, v in r.items():
            columns.setdefault(k, []).append(v)
    for col, vals in columns.items():
        if col in ("postingdate", "effectivedate", "instrumentid"):
            continue
        spec = field_specs.get(col.lower())
        dtype = (spec or {}).get("datatype", "").lower()
        if dtype not in ("decimal", "integer", "int"):
            continue
        numeric = [v for v in vals if isinstance(v, (int, float))]
        if not numeric:
            continue
        if all(v == 0 for v in numeric):
            warnings.append(f"field '{col}': all values are zero")
        elif len(set(numeric)) == 1 and len(numeric) > 1:
            warnings.append(f"field '{col}': identical value {numeric[0]} across all {len(numeric)} rows")
        # absurdly large
        if any(abs(v) > 1e10 for v in numeric):
            warnings.append(
                f"field '{col}': value > 1e10 detected — likely wrong scale "
                f"(rates should be 0.05 not 5)"
            )
    return warnings


# Common debit/credit transaction-type pairs. When the agent registers one
# side, we can suggest the other to keep journal entries balanced.
_TXN_PAIR_HINTS: list[tuple[str, str]] = [
    ("ECLAllowance", "ECLExpense"),
    ("InterestReceivable", "InterestIncome"),
    ("FeeReceivable", "FeeIncome"),
    ("LoanPrincipal", "CashSettlement"),
    ("Drawdown", "CashDisbursement"),
    ("Repayment", "CashReceipt"),
    ("Writeoff", "ECLAllowance"),
    ("AmortisedCost", "InterestIncome"),
]


def _suggest_txn_pairs(registered: list[str], existing: list[str]) -> list[str]:
    have = {n.lower() for n in registered + existing}
    suggestions: list[str] = []
    for a, b in _TXN_PAIR_HINTS:
        la, lb = a.lower(), b.lower()
        if la in have and lb not in have:
            suggestions.append(f"'{a}' is registered but '{b}' is not — double-entry usually needs both")
        if lb in have and la not in have:
            suggestions.append(f"'{b}' is registered but '{a}' is not — double-entry usually needs both")
    # de-dup while preserving order
    seen = set()
    out = []
    for s in suggestions:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# ──────────────────────────────────────────────────────────────────────────
# Tool implementations
# ──────────────────────────────────────────────────────────────────────────

async def tool_list_events(_args: dict) -> dict:
    events = await _list_event_defs()
    return {
        "count": len(events),
        "events": [
            {
                "event_name": e.get("event_name"),
                "eventType": e.get("eventType", "activity"),
                "eventTable": e.get("eventTable", "standard"),
                "fields": e.get("fields", []),
            }
            for e in events
        ],
    }


async def tool_list_dsl_functions(args: dict) -> dict:
    metadata = list(_ServerBridge.helpers.get("DSL_FUNCTION_METADATA", []) or [])
    category_filter = (args.get("category") or "").strip().lower()
    out = []
    for m in metadata:
        if category_filter and (m.get("category") or "").lower() != category_filter:
            continue
        out.append({
            "name": m.get("name"),
            "params": m.get("params"),
            "description": m.get("description"),
            "category": m.get("category"),
        })
    return {"count": len(out), "functions": out}


async def tool_list_templates(_args: dict) -> dict:
    templates = await _list_templates()
    return {
        "count": len(templates),
        "templates": [
            {"id": t.get("id"), "name": t.get("name"), "deployed": t.get("deployed", False)}
            for t in templates
        ],
    }


async def tool_create_event_definitions(args: dict) -> dict:
    EventDefinition = _h("EventDefinition")
    db = _ServerBridge.db
    raw = args.get("events") or []
    if not isinstance(raw, list) or not raw:
        raise ToolError("`events` must be a non-empty list")

    created = []
    skipped = []
    for spec in raw:
        if not isinstance(spec, dict):
            raise ToolError("Each event must be an object")
        event_name = (spec.get("event_name") or "").strip()
        if not event_name:
            raise ToolError("event_name is required")
        existing = await _find_event_def(event_name)
        if existing:
            skipped.append({"event_name": event_name, "reason": "already exists"})
            continue
        event_type = (spec.get("eventType") or "activity").lower()
        event_table = (spec.get("eventTable") or "standard").lower()
        if event_type not in ("activity", "reference"):
            raise ToolError(f"Invalid eventType '{event_type}' for {event_name}")
        if event_table not in ("standard", "custom"):
            raise ToolError(f"Invalid eventTable '{event_table}' for {event_name}")
        if event_table == "standard" and event_type != "activity":
            raise ToolError(
                f"Event '{event_name}': standard eventTable requires eventType=activity"
            )
        fields = spec.get("fields") or []
        if not isinstance(fields, list) or not fields:
            raise ToolError(f"Event '{event_name}' must declare at least one field")
        norm_fields = []
        valid_dtypes = {"string", "date", "boolean", "decimal", "integer", "int"}
        for fld in fields:
            if not isinstance(fld, dict) or not fld.get("name"):
                raise ToolError(f"Invalid field in event '{event_name}': {fld}")
            dtype = (fld.get("datatype") or "decimal").lower()
            if dtype not in valid_dtypes:
                raise ToolError(
                    f"Invalid datatype '{dtype}' on field '{fld.get('name')}' "
                    f"of event '{event_name}'. Allowed: {sorted(valid_dtypes)}"
                )
            norm_fields.append({"name": fld["name"], "datatype": dtype})

        evt = EventDefinition(
            event_name=event_name,
            fields=norm_fields,
            eventType=event_type,
            eventTable=event_table,
        )
        doc = evt.model_dump()
        doc["created_at"] = doc["created_at"].isoformat()
        wrote_db = False
        try:
            if db is not None:
                await db.event_definitions.insert_one(doc)
                wrote_db = True
        except Exception as exc:
            logger.warning("Could not insert event def to DB: %s", exc)
        if not wrote_db:
            (_ServerBridge.in_memory_data.setdefault("event_definitions", [])).append(doc)
        created.append({"event_name": event_name, "fields": norm_fields, "eventType": event_type})

    return {"created": created, "skipped": skipped}


async def tool_add_transaction_types(args: dict) -> dict:
    types = args.get("transaction_types") or []
    if not isinstance(types, list) or not types:
        raise ToolError("`transaction_types` must be a non-empty list of strings")
    db = _ServerBridge.db
    added = []
    pre_existing: list[str] = []
    for t in types:
        if not isinstance(t, str) or not t.strip():
            continue
        name = t.strip()
        try:
            if db is not None:
                exists = await db.transaction_definitions.find_one({"transactiontype": name}, {"_id": 0})
                if not exists:
                    await db.transaction_definitions.insert_one({"transactiontype": name})
                    added.append(name)
                else:
                    pre_existing.append(name)
                continue
        except Exception as exc:
            logger.warning("DB insert txn type failed: %s", exc)
        mem = _ServerBridge.in_memory_data.setdefault("transaction_definitions", [])
        if not any(d.get("transactiontype") == name for d in mem):
            mem.append({"transactiontype": name})
            added.append(name)
        else:
            pre_existing.append(name)

    # Pull the full registered list to give pairing suggestions
    all_registered: list[str] = []
    try:
        if db is not None:
            cursor = db.transaction_definitions.find({}, {"_id": 0, "transactiontype": 1})
            async for doc in cursor:
                if doc.get("transactiontype"):
                    all_registered.append(doc["transactiontype"])
    except Exception:
        all_registered = [
            d.get("transactiontype")
            for d in (_ServerBridge.in_memory_data.get("transaction_definitions") or [])
            if d.get("transactiontype")
        ]

    suggestions = _suggest_txn_pairs(added, all_registered)
    return {
        "added": added,
        "already_existed": pre_existing,
        "total_requested": len(types),
        "pair_suggestions": suggestions,
    }


async def tool_generate_sample_event_data(args: dict) -> dict:
    EventData = _h("EventData")
    db = _ServerBridge.db

    event_name = (args.get("event_name") or "").strip()
    if not event_name:
        raise ToolError("event_name is required")
    event_def = await _find_event_def(event_name)
    if not event_def:
        raise ToolError(
            f"Event definition '{event_name}' not found. "
            f"Use create_event_definitions first."
        )
    instrument_count = int(args.get("instrument_count") or 0)
    instrument_ids = args.get("instrument_ids") or []
    if not instrument_ids:
        if instrument_count <= 0 or instrument_count > 200:
            raise ToolError("Provide instrument_ids OR instrument_count (1..200)")
        prefix = (args.get("instrument_prefix") or "INST").strip()
        instrument_ids = [f"{prefix}-{i+1:03d}" for i in range(instrument_count)]
    posting_dates = args.get("posting_dates") or ["2026-01-01"]
    if not isinstance(posting_dates, list) or not posting_dates:
        raise ToolError("posting_dates must be a non-empty list of YYYY-MM-DD strings")
    seed = int(args.get("seed") or 42)
    field_hints = args.get("field_hints") or {}
    append = bool(args.get("append", False))

    new_rows = _make_sample_rows(event_def, instrument_ids, posting_dates, field_hints, seed)

    existing_doc = None
    try:
        if db is not None:
            existing_doc = await db.event_data.find_one(
                {"event_name": event_def["event_name"]}, {"_id": 0}
            )
    except Exception:
        pass

    final_rows = list(new_rows)
    if append and existing_doc and existing_doc.get("data_rows"):
        final_rows = list(existing_doc["data_rows"]) + new_rows

    payload = EventData(event_name=event_def["event_name"], data_rows=final_rows)
    doc = payload.model_dump()
    doc["created_at"] = doc["created_at"].isoformat()
    wrote_db = False
    try:
        if db is not None:
            await db.event_data.delete_many({"event_name": event_def["event_name"]})
            await db.event_data.insert_one(doc)
            wrote_db = True
    except Exception as exc:
        logger.warning("DB write event_data failed: %s", exc)
    if not wrote_db:
        mem = _ServerBridge.in_memory_data.setdefault("event_data", [])
        mem[:] = [d for d in mem if d.get("event_name") != event_def["event_name"]]
        mem.append(doc)

    return {
        "event_name": event_def["event_name"],
        "rows_inserted": len(new_rows),
        "rows_total": len(final_rows),
        "instruments": instrument_ids,
        "posting_dates": posting_dates,
        "sample_row": new_rows[0] if new_rows else None,
        "data_quality_warnings": _audit_sample_rows(new_rows, event_def),
    }


async def tool_get_event_data(args: dict) -> dict:
    db = _ServerBridge.db
    event_name = (args.get("event_name") or "").strip()
    if not event_name:
        raise ToolError("event_name is required")
    limit = int(args.get("limit") or 5)
    doc = None
    try:
        if db is not None:
            doc = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(event_name)}$", "$options": "i"}},
                {"_id": 0},
            )
    except Exception:
        pass
    if not doc:
        for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
            if str(d.get("event_name", "")).lower() == event_name.lower():
                doc = d
                break
    if not doc:
        return {"event_name": event_name, "row_count": 0, "rows": []}
    rows = doc.get("data_rows") or []
    return {
        "event_name": doc.get("event_name"),
        "row_count": len(rows),
        "rows": rows[:limit],
    }


# ──────────────────────────────────────────────────────────────────────────
# DSL guardrail: the agent must never inject Python escape hatches.
# ──────────────────────────────────────────────────────────────────────────

_FORBIDDEN_DSL_PATTERNS = [
    (re.compile(r"^\s*customCode\s*:", re.IGNORECASE | re.MULTILINE),
        "Custom Code blocks are not allowed for the agent. Compose with rules and DSL functions only."),
    (re.compile(r"\b__import__\b"),
        "__import__ is forbidden in DSL."),
    (re.compile(r"\beval\s*\("),
        "eval() is forbidden in DSL."),
    (re.compile(r"\bexec\s*\("),
        "exec() is forbidden in DSL."),
    (re.compile(r"\bsubprocess\b"),
        "subprocess is forbidden in DSL."),
    (re.compile(r"\bos\.system\b"),
        "os.system is forbidden in DSL."),
    (re.compile(r"\bopen\s*\("),
        "open() is forbidden in DSL."),
]


def _enforce_dsl_guardrails(dsl_code: str) -> None:
    if not dsl_code:
        raise ToolError("dsl_code is empty")
    for pattern, reason in _FORBIDDEN_DSL_PATTERNS:
        if pattern.search(dsl_code):
            raise ToolError(reason)


# ──────────────────────────────────────────────────────────────────────────
# Pre-flight expression validators — catch the structural mistakes that
# show up as cryptic 'unterminated string literal' or 'invalid syntax'
# errors during dry-run, BEFORE the rule reaches the database. Each error
# message names the offending construct, points at the right alternative,
# and (where useful) suggests a known DSL function via did-you-mean.
# ──────────────────────────────────────────────────────────────────────────

# Patterns that indicate the agent is using a syntax this DSL does not
# support. Each entry: (compiled regex, human-readable diagnosis + fix).
_UNSUPPORTED_EXPR_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\boutputs\.events\.push\s*\("),
        "`outputs.events.push(...)` does NOT exist. To create synthetic events, "
        "pre-load them via create_event_definitions + generate_sample_event_data. "
        "Expressions cannot emit new events."),
    (re.compile(r"\bcreateEventRow\s*\("),
        "`createEventRow(...)` is not a DSL function. Synthetic events must be "
        "created with create_event_definitions + generate_sample_event_data BEFORE "
        "the rule runs. There is no in-rule event creation."),
    (re.compile(r"\boutputs\.transactions\.push\s*\("),
        "`outputs.transactions.push(...)` is not how transactions are emitted. "
        "Put the transaction object in the rule's `outputs.transactions` array "
        "via create_saved_rule, OR call createTransaction(postingdate, "
        "effectivedate, \"TxnType\", amount) directly in a calc step's formula."),
    (re.compile(r"[A-Za-z_]\w*\s*\["),
        "Bracket indexing `arr[i]` is not supported in DSL expressions. "
        "Use lookup(arr, idx) or element_at(arr, idx) instead."),
    (re.compile(r"\blet\s+[A-Za-z_]\w*\s*="),
        "`let` bindings are not supported. Each step has ONE expression. "
        "Break multi-step logic into multiple steps."),
    (re.compile(r";\s*\S"),
        "Semicolon-separated statements are not supported in a single expression. "
        "Break into multiple steps or multiple iterations."),
]


def _check_iteration_expression(expr: str, *, where: str) -> None:
    """Iteration `expression` must be one line, one expression. The DSL's
    Python-target translator chokes on multi-line / multi-statement strings
    with a generic 'unterminated string literal' error."""
    if not isinstance(expr, str) or not expr:
        return
    # Multi-line check (the #1 failure mode in the trace)
    if "\n" in expr:
        raise ToolError(
            f"In {where}: iteration expression must be SINGLE-LINE, "
            f"SINGLE-EXPRESSION (no newlines, no `let`, no `;`). "
            f"To do multiple things, add more iteration entries or split "
            f"into multiple steps. Got: {expr[:120]!r}"
        )
    # Generic structural patterns (push/createEventRow/brackets/let/semicolons)
    for pat, reason in _UNSUPPORTED_EXPR_PATTERNS:
        if pat.search(expr):
            raise ToolError(f"In {where}: {reason} Got: {expr[:120]!r}")


def _check_formula_expression(expr: str, *, where: str) -> None:
    """A formula/condition/value field. Multi-line is technically allowed
    in some contexts but the structural patterns are always wrong."""
    if not isinstance(expr, str) or not expr:
        return
    for pat, reason in _UNSUPPORTED_EXPR_PATTERNS:
        if pat.search(expr):
            raise ToolError(f"In {where}: {reason} Got: {expr[:120]!r}")
    # Python `for ... in` / `while` loops inside an expression are never valid
    if re.search(r"\bfor\s+[A-Za-z_]\w*\s+in\b", expr) or re.search(r"\bwhile\b", expr):
        raise ToolError(
            f"In {where}: Python loops (`for`/`while`) are not allowed inside "
            f"an expression. Use stepType='iteration' with sourceArray instead. "
            f"Got: {expr[:120]!r}"
        )


def _known_dsl_function_names() -> set[str]:
    """Set of all DSL function names known to the runtime, plus the always-
    available builtins/keywords. Used for did-you-mean suggestions."""
    meta = _ServerBridge.helpers.get("DSL_FUNCTION_METADATA") or []
    names = {(m.get("name") or "").strip() for m in meta if m.get("name")}
    # Always-available
    names.update({
        "createTransaction", "print", "if",
        "True", "False", "None", "and", "or", "not", "in",
        "postingdate", "effectivedate", "instrumentid", "subinstrumentid",
        "each", "second",
    })
    return names


_CALL_RE = re.compile(r"\b([a-zA-Z_]\w*)\s*\(")


def _check_function_calls(expr: str, *, where: str, extra_names: set[str] | None = None) -> None:
    """Every `name(` in the expression must be a known DSL function (or a
    user-defined variable that happens to be callable, which we allow)."""
    if not isinstance(expr, str) or not expr:
        return
    known = _known_dsl_function_names()
    if extra_names:
        known = known | extra_names
    seen: set[str] = set()
    import difflib
    for fn in _CALL_RE.findall(expr):
        if fn in seen or fn in known:
            seen.add(fn)
            continue
        seen.add(fn)
        sug = difflib.get_close_matches(fn, list(known), n=3, cutoff=0.6)
        hint = (f" Did you mean: {', '.join(sug)}?" if sug
                else " Call `list_dsl_functions` to see available functions.")
        raise ToolError(
            f"In {where}: `{fn}(...)` is not a known DSL function.{hint}"
        )


async def tool_validate_dsl(args: dict) -> dict:
    dsl_code = args.get("dsl_code") or ""
    event_name = (args.get("event_name") or "").strip()
    _enforce_dsl_guardrails(dsl_code)

    extract_event_names = _h("extract_event_names_from_dsl")
    referenced = list(extract_event_names(dsl_code) or [])

    # Pick fields for translation. If event_name is provided use that;
    # otherwise use all referenced events.
    all_event_fields: dict[str, Any] = {}
    if event_name:
        evt = await _find_event_def(event_name)
        if not evt:
            raise ToolError(f"Event definition '{event_name}' not found")
        all_event_fields[evt["event_name"]] = {
            "fields": evt.get("fields", []),
            "eventType": evt.get("eventType", "activity"),
        }
    else:
        for nm in referenced:
            evt = await _find_event_def(nm)
            if not evt:
                raise ToolError(f"Event definition '{nm}' referenced in DSL not found")
            all_event_fields[evt["event_name"]] = {
                "fields": evt.get("fields", []),
                "eventType": evt.get("eventType", "activity"),
            }

    dsl_to_python_multi_event = _h("dsl_to_python_multi_event")
    dsl_to_python_standalone = _h("dsl_to_python_standalone")

    try:
        if all_event_fields:
            python_code = dsl_to_python_multi_event(dsl_code, all_event_fields)
        else:
            python_code = dsl_to_python_standalone(dsl_code)
    except Exception as exc:
        raise ToolError(f"DSL translation failed: {exc}") from exc

    # Compile to catch syntax errors against the python target.
    try:
        compile(python_code, "<dsl_validate>", "exec")
    except SyntaxError as se:
        raise ToolError(f"Generated python has syntax error: {se.msg} at line {se.lineno}") from se

    return {
        "valid": True,
        "referenced_events": referenced,
        "lines": len(dsl_code.splitlines()),
    }


async def tool_create_or_replace_template(args: dict) -> dict:
    DSLTemplate = _h("DSLTemplate")
    db = _ServerBridge.db
    name = (args.get("name") or "").strip()
    dsl_code = args.get("dsl_code") or ""
    event_name = (args.get("event_name") or "").strip()
    description = (args.get("description") or "").strip()
    if not name:
        raise ToolError("Template `name` is required")
    if not event_name:
        raise ToolError("`event_name` (primary event) is required")
    _enforce_dsl_guardrails(dsl_code)

    event = await _find_event_def(event_name)
    if not event:
        raise ToolError(f"Event '{event_name}' not found")

    # Translate using the same path as save_template
    dsl_to_python = _h("dsl_to_python")
    try:
        python_code = dsl_to_python(dsl_code, event["fields"])
    except Exception as exc:
        raise ToolError(f"DSL translation failed: {exc}") from exc

    # Replace existing
    try:
        if db is not None:
            await db.dsl_templates.delete_many({"name": name})
    except Exception:
        pass
    mem = _ServerBridge.in_memory_data.setdefault("templates", [])
    mem[:] = [t for t in mem if str(t.get("name", "")).lower() != name.lower()]

    template = DSLTemplate(name=name, dsl_code=dsl_code, python_code=python_code)
    doc = template.model_dump()
    doc["created_at"] = doc["created_at"].isoformat()
    wrote_db = False
    try:
        if db is not None:
            await db.dsl_templates.insert_one(doc)
            wrote_db = True
    except Exception as exc:
        logger.warning("DB write template failed: %s", exc)
    if not wrote_db:
        mem.append(doc)

    # Also mirror into `user_templates` so the template shows up in the
    # "User Created Templates" tab of the Templates wizard. The UI's
    # Standard/User-Created tabs read from /user-templates, not /templates.
    user_template_id = None
    if db is not None:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            existing_user = await db.user_templates.find_one(
                {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
                {"_id": 0, "id": 1},
            )
            user_doc = {
                "name": name,
                "description": description or f"Generated by AI agent for {event_name}.",
                "category": "AI Generated",
                "rules": [],
                "schedules": [],
                "combinedCode": dsl_code,
                "updated_at": now_iso,
            }
            if existing_user and existing_user.get("id"):
                user_template_id = existing_user["id"]
                await db.user_templates.update_one(
                    {"id": user_template_id}, {"$set": user_doc}
                )
            else:
                user_template_id = str(uuid.uuid4())
                user_doc["id"] = user_template_id
                user_doc["created_at"] = now_iso
                await db.user_templates.insert_one(user_doc)
        except Exception as exc:
            logger.warning("DB write user_template failed: %s", exc)

    return {
        "template_id": template.id,
        "user_template_id": user_template_id,
        "name": name,
        "event_name": event_name,
    }


async def _diagnose_template_failure(template: dict, err_msg: str) -> str:
    """Inspect the saved rules attached to a template and try to pinpoint
    which rule/step likely produced the failure. Returns a hint string
    appended to the ToolError, or '' if nothing useful was found."""
    db = _ServerBridge.db
    if db is None:
        return ""
    rule_ids = list(template.get("rule_ids") or [])
    if not rule_ids:
        return ""
    try:
        rules = await db.saved_rules.find(
            {"id": {"$in": rule_ids}},
            {"_id": 0, "id": 1, "name": 1, "steps": 1},
        ).to_list(200)
    except Exception:
        return ""
    suspects: list[str] = []
    err_lower = err_msg.lower()
    is_string_err = ("unterminated" in err_lower or "eol" in err_lower
                      or "eof" in err_lower or "invalid syntax" in err_lower)
    for r in rules:
        for s in r.get("steps") or []:
            st = s.get("stepType") or "calc"
            if st == "iteration":
                for i, it in enumerate(s.get("iterations") or []):
                    expr = it.get("expression") or ""
                    if is_string_err and "\n" in expr:
                        suspects.append(
                            f"rule '{r.get('name')}' → step '{s.get('name')}' "
                            f".iterations[{i}].expression contains a NEWLINE "
                            f"(iteration expressions must be single-line)"
                        )
                    for pat, _ in _UNSUPPORTED_EXPR_PATTERNS:
                        if pat.search(expr):
                            suspects.append(
                                f"rule '{r.get('name')}' → step '{s.get('name')}' "
                                f".iterations[{i}].expression contains unsupported syntax: "
                                f"{expr[:80]!r}"
                            )
                            break
            elif st == "calc":
                for fld in ("formula", "value"):
                    expr = s.get(fld) or ""
                    for pat, _ in _UNSUPPORTED_EXPR_PATTERNS:
                        if pat.search(expr):
                            suspects.append(
                                f"rule '{r.get('name')}' → step '{s.get('name')}' "
                                f".{fld} contains unsupported syntax: {expr[:80]!r}"
                            )
                            break
    if not suspects:
        return ""
    bullets = "\n  • " + "\n  • ".join(suspects[:5])
    return (
        f"\n\nLikely cause(s):{bullets}\n"
        f"Fix with `update_step` or `update_saved_rule`. If unsure of the "
        f"correct shape, call `get_dsl_syntax_guide` first."
    )


async def tool_dry_run_template(args: dict) -> dict:
    """Run a saved template against current event data and return summary.

    Re-implements the core of /templates/execute but in-process, without
    persisting transaction reports (true dry-run).
    """
    extract_event_names_from_dsl = _h("extract_event_names_from_dsl")
    dsl_to_python_multi_event = _h("dsl_to_python_multi_event")
    execute_python_template = _h("execute_python_template")
    merge_event_data_by_instrument = _h("merge_event_data_by_instrument")
    filter_event_data_by_posting_date = _h("filter_event_data_by_posting_date")
    db = _ServerBridge.db

    template_id = (args.get("template_id") or args.get("name") or "").strip()
    if not template_id:
        raise ToolError("template_id or name is required")
    template = await _find_template(template_id)
    if not template:
        raise ToolError(f"Template '{template_id}' not found")

    posting_date = args.get("posting_date")
    effective_date = args.get("effective_date")
    sample_limit = int(args.get("sample_limit") or 5)

    dsl_code = template["dsl_code"]
    referenced = list(extract_event_names_from_dsl(dsl_code) or [])

    all_event_fields: dict[str, Any] = {}
    event_data_dict: dict[str, list[dict]] = {}
    activity_with_data: list[str] = []
    reference_with_data: list[str] = []
    missing_data: list[str] = []

    for evt_name in referenced:
        evt = await _find_event_def(evt_name)
        if not evt:
            raise ToolError(f"Event definition '{evt_name}' not found")
        all_event_fields[evt["event_name"]] = {
            "fields": evt.get("fields", []),
            "eventType": evt.get("eventType", "activity"),
        }
        rows = []
        try:
            if db is not None:
                doc = await db.event_data.find_one(
                    {"event_name": {"$regex": f"^{re.escape(evt_name)}$", "$options": "i"}},
                    {"_id": 0},
                )
                if doc and doc.get("data_rows"):
                    rows = doc["data_rows"]
        except Exception:
            pass
        if not rows:
            for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
                if str(d.get("event_name", "")).lower() == evt_name.lower():
                    rows = d.get("data_rows") or []
                    break
        event_data_dict[evt["event_name"]] = rows
        if evt.get("eventType") == "reference":
            if rows:
                reference_with_data.append(evt["event_name"])
        else:
            if rows:
                activity_with_data.append(evt["event_name"])
            else:
                missing_data.append(evt["event_name"])

    if activity_with_data:
        activity_data = {k: v for k, v in event_data_dict.items()
                          if all_event_fields[k]["eventType"] == "activity"}
        scoped = (filter_event_data_by_posting_date(activity_data, posting_date)
                   if posting_date else activity_data)
        merged = merge_event_data_by_instrument(scoped)
        if not merged and posting_date:
            merged = merge_event_data_by_instrument(activity_data)
    elif reference_with_data:
        merged = [{}]
    else:
        raise ToolError(
            f"No data found for any referenced events: {referenced}. "
            f"Generate sample data first."
        )

    if not merged:
        raise ToolError("No data after merging events")

    try:
        python_code = dsl_to_python_multi_event(dsl_code, all_event_fields)
    except Exception as exc:
        hint = await _diagnose_template_failure(template, str(exc))
        raise ToolError(f"DSL translation failed: {exc}{hint}") from exc

    raw_for_collect = (
        filter_event_data_by_posting_date(event_data_dict, posting_date, all_event_fields)
        if posting_date else event_data_dict
    )

    try:
        result = await execute_python_template(
            python_code, merged, raw_for_collect, posting_date, effective_date,
        )
    except Exception as exc:
        hint = await _diagnose_template_failure(template, str(exc))
        raise ToolError(f"Execution failed: {exc}{hint}") from exc

    transactions = result.get("transactions") or []
    txn_dicts = [t.model_dump() if hasattr(t, "model_dump") else t for t in transactions]
    total_amount = 0.0
    by_type: dict[str, dict] = {}
    absurd_txns: list[dict] = []
    for t in txn_dicts:
        amt = float(t.get("amount") or 0)
        total_amount += amt
        ty = t.get("transactiontype", "?")
        b = by_type.setdefault(ty, {"count": 0, "total": 0.0})
        b["count"] += 1
        b["total"] += amt
        if abs(amt) > 1e9:
            absurd_txns.append({
                "transactiontype": ty,
                "amount": amt,
                "instrumentid": t.get("instrumentid"),
            })

    sanity_warnings: list[str] = []
    if absurd_txns:
        sanity_warnings.append(
            f"{len(absurd_txns)} transaction(s) have amount > $1B — almost "
            f"certainly a unit error (e.g., a rate stored as 5 instead of 0.05, "
            f"or a balance multiplied by an unbounded factor). "
            f"Inspect the formulas/iterations and the source event-data ranges."
        )
    # Check for double-entry imbalance per instrument
    by_instr_signed: dict[str, float] = {}
    for t in txn_dicts:
        amt = float(t.get("amount") or 0)
        ty_lower = str(t.get("transactiontype", "")).lower()
        # very rough sign convention: receivable/expense/asset = +, income/payable/allowance = -
        sign = 1 if any(k in ty_lower for k in ("receivable", "expense", "asset", "drawdown")) else (
               -1 if any(k in ty_lower for k in ("income", "payable", "allowance", "writeoff")) else 0)
        if sign:
            by_instr_signed[t.get("instrumentid", "?")] = (
                by_instr_signed.get(t.get("instrumentid", "?"), 0.0) + sign * amt
            )
    unbalanced = {k: round(v, 2) for k, v in by_instr_signed.items() if abs(v) > 0.01}
    if unbalanced and len(txn_dicts) > 1:
        sanity_warnings.append(
            f"signed-sum of recognised debit/credit transactions is non-zero "
            f"for {len(unbalanced)} instrument(s) — double-entry may be incomplete"
        )

    return {
        "template_id": template.get("id"),
        "template_name": template.get("name"),
        "events_used": activity_with_data + reference_with_data,
        "missing_data": missing_data,
        "row_count_input": len(merged),
        "transaction_count": len(txn_dicts),
        "total_amount": round(total_amount, 2),
        "by_transaction_type": {k: {"count": v["count"], "total": round(v["total"], 2)}
                                  for k, v in by_type.items()},
        "sample_transactions": txn_dicts[:sample_limit],
        "print_outputs": (result.get("print_outputs") or [])[:10],
        "sanity_warnings": sanity_warnings,
    }


async def tool_delete_template(args: dict) -> dict:
    db = _ServerBridge.db
    template_id = (args.get("template_id") or "").strip()
    if not template_id:
        raise ToolError("template_id required")
    if not bool(args.get("confirm")):
        raise ToolError("This is a destructive action — call again with confirm=true after user approval")
    deleted = 0
    try:
        if db is not None:
            r = await db.dsl_templates.delete_one({"id": template_id})
            deleted += getattr(r, "deleted_count", 0) or 0
            r = await db.dsl_templates.delete_one({"name": template_id})
            deleted += getattr(r, "deleted_count", 0) or 0
    except Exception:
        pass
    mem = _ServerBridge.in_memory_data.setdefault("templates", [])
    before = len(mem)
    mem[:] = [t for t in mem if t.get("id") != template_id and t.get("name") != template_id]
    deleted += before - len(mem)
    return {"deleted": deleted, "template_id": template_id}


async def tool_clear_all_data(args: dict) -> dict:
    if not bool(args.get("confirm")):
        raise ToolError("This is a destructive action — call again with confirm=true after user approval")
    db = _ServerBridge.db
    cleared: list[str] = []
    if db is not None:
        for col in (
            "event_definitions", "event_data", "transaction_reports",
            "custom_functions", "saved_rules", "saved_schedules",
            "transaction_definitions",
        ):
            try:
                await db[col].delete_many({})
                cleared.append(col)
            except Exception as exc:
                logger.warning("clear %s failed: %s", col, exc)
    mem = _ServerBridge.in_memory_data
    if mem is not None:
        for k in ("event_definitions", "event_data", "transaction_reports",
                   "custom_functions", "transaction_definitions"):
            mem[k] = []
        mem.pop("saved_rules", None)
        mem.pop("saved_schedules", None)
    return {"cleared": cleared, "preserved": ["templates"]}


# ──────────────────────────────────────────────────────────────────────────
# Rule / Step / Schedule helpers — Python port of the JS rule-builder logic
# in frontend/src/components/rulebuilder/AccountingRuleBuilder.js so DSL we
# emit round-trips perfectly through the UI's load/save pipeline.
# ──────────────────────────────────────────────────────────────────────────

def _build_calc_line(v: dict) -> str | None:
    src = v.get("source") or "formula"
    name = v.get("name")
    if not name:
        return None
    if src == "value":
        return f"{name} = {v.get('value', 0) or 0}"
    if src == "event_field":
        return f"{name} = {v.get('eventField', '')}"
    if src == "formula":
        return f"{name} = {v.get('formula') or 0}"
    if src == "collect":
        ct = v.get("collectType") or "collect_by_instrument"
        return f"{name} = {ct}({v.get('eventField', '')})"
    return None


def _build_condition_expr(conditions: list[dict], else_formula: str) -> str:
    valid = [c for c in (conditions or []) if c.get("condition")]
    if not valid:
        return else_formula or "0"
    nested = else_formula or "0"
    for c in reversed(valid):
        sub = c.get("nestedConditions") or []
        if sub:
            then_part = _build_condition_expr(sub, c.get("nestedElse") or c.get("thenFormula") or "0")
        else:
            then_part = c.get("thenFormula") or "0"
        nested = f"if({c['condition']}, {then_part}, {nested})"
    return nested


def _build_iteration_lines(iters: list[dict], available: list[str]) -> list[str]:
    lines: list[str] = []
    iter_results: list[str] = []
    ident_re = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
    bare_id_re = re.compile(r"^[a-zA-Z_]\w*$")
    for it in iters or []:
        avail = list(available) + iter_results
        expr_ids = set(ident_re.findall(it.get("expression") or ""))
        if it.get("sourceArray") and bare_id_re.match(it["sourceArray"]):
            expr_ids.add(it["sourceArray"])
        if it.get("secondArray") and bare_id_re.match(it["secondArray"]):
            expr_ids.add(it["secondArray"])
        ctx = [v for v in avail if v in expr_ids]
        ctx_str = (", {" + ", ".join(f'"{v}": {v}' for v in ctx) + "}") if ctx else ""
        rv = it.get("resultVar") or ""
        src = it.get("sourceArray") or ""
        sec = it.get("secondArray") or "[]"
        expr = it.get("expression") or ""
        if it.get("type") == "apply_each":
            lines.append(f'{rv} = apply_each({src}, "{expr}"{ctx_str})')
        elif it.get("type") == "apply_each_paired":
            lines.append(f'{rv} = apply_each({src}, {sec}, "{expr}"{ctx_str})')
        else:
            vn = it.get("varName") or "each"
            sv = it.get("secondVar") or "second"
            lines.append(f'{rv} = for_each({src}, {sec}, "{vn}", "{sv}", "{expr}")')
        if rv:
            iter_results.append(rv)
    return lines


def _generate_rule_code(rule: dict) -> str:
    """Build generatedCode for a single rule from its steps + outputs.

    Mirrors the `generatedCode` useMemo in AccountingRuleBuilder.js but for an
    isolated rule (no prior-rules dependency injection — that is added when the
    rule is combined into a template via _generate_combined_code).
    """
    name = (rule.get("name") or "CUSTOM CALCULATION").upper()
    lines: list[str] = []
    lines.append("## ═══════════════════════════════════════════════════════════════")
    lines.append(f"## {name}")
    lines.append("## ═══════════════════════════════════════════════════════════════")
    lines.append("")

    steps = rule.get("steps") or []
    if steps:
        lines.append("## Steps")

    defined: list[str] = []
    for s in steps:
        st = s.get("stepType") or "calc"
        if not s.get("name") and st == "calc":
            continue
        if s.get("inlineComment") and (s.get("commentText") or "").strip():
            for line in s["commentText"].strip().split("\n"):
                lines.append(f"## {line}")
        if st == "calc":
            line = _build_calc_line(s)
            if line:
                lines.append(line)
                defined.append(s["name"])
            if s.get("printResult") and s.get("name"):
                lines.append(f'print("{s["name"]} =", {s["name"]})')
        elif st == "condition":
            lines.append("## Conditional Logic")
            expr = _build_condition_expr(s.get("conditions") or [], s.get("elseFormula") or "")
            lines.append(f"{s['name']} = {expr}")
            defined.append(s["name"])
            if s.get("printResult") and s.get("name"):
                lines.append(f'print("{s["name"]} =", {s["name"]})')
            lines.append("")
        elif st == "iteration":
            lines.append("## Iteration")
            iter_lines = _build_iteration_lines(s.get("iterations") or [], list(defined))
            lines.extend(iter_lines)
            for it in s.get("iterations") or []:
                if it.get("resultVar"):
                    defined.append(it["resultVar"])
            if s.get("printResult"):
                last = (s.get("iterations") or [])
                if last:
                    rv = last[-1].get("resultVar")
                    if rv:
                        lines.append(f'print("{rv} =", {rv})')
            lines.append("")
        elif st == "schedule":
            sc = s.get("scheduleConfig") or {}
            lines.append("## Schedule")
            # Period definition
            if sc.get("periodType") == "number":
                src_t = sc.get("periodCountSource")
                if src_t == "field" and sc.get("periodCountField"):
                    count_expr = sc["periodCountField"]
                elif src_t == "formula" and sc.get("periodCountFormula"):
                    count_expr = sc["periodCountFormula"]
                else:
                    count_expr = sc.get("periodCount") or 12
                lines.append(f'p = period({count_expr}, "{sc.get("frequency") or "M"}")')
            else:
                if sc.get("startDateSource") == "field" and sc.get("startDateField"):
                    start_expr = sc["startDateField"]
                elif sc.get("startDateSource") == "formula" and sc.get("startDateFormula"):
                    start_expr = sc["startDateFormula"]
                else:
                    start_expr = f'"{sc.get("startDate") or "2026-01-01"}"'
                if sc.get("endDateSource") == "field" and sc.get("endDateField"):
                    end_expr = sc["endDateField"]
                elif sc.get("endDateSource") == "formula" and sc.get("endDateFormula"):
                    end_expr = sc["endDateFormula"]
                else:
                    end_expr = f'"{sc.get("endDate") or "2026-12-31"}"'
                period_call = f'p = period({start_expr}, {end_expr}, "{sc.get("frequency") or "M"}"'
                if sc.get("convention"):
                    period_call += f', "{sc["convention"]}"'
                period_call += ")"
                lines.append(period_call)
            # Schedule call
            valid_cols = [c for c in (sc.get("columns") or []) if c.get("name") and c.get("formula")]
            lines.append(f'{s["name"]} = schedule(p, {{')
            for i, col in enumerate(valid_cols):
                comma = "," if i < len(valid_cols) - 1 else ""
                lines.append(f'    "{col["name"]}": "{col["formula"]}"{comma}')
            ctx_vars = [v for v in (sc.get("contextVars") or []) if v != s["name"]]
            if ctx_vars:
                ctx_pairs = ", ".join(f'"{v}": {v}' for v in ctx_vars)
                lines.append(f"}}, {{{ctx_pairs}}})")
            else:
                lines.append("})")
            lines.append(f'print({s["name"]})')
            defined.append(s["name"])
            for o in s.get("outputVars") or []:
                otype = o.get("type")
                oname = o.get("name")
                col = o.get("column")
                if otype == "first":
                    lines.append(f'{oname} = schedule_first({s["name"]}, "{col}")')
                elif otype == "last":
                    lines.append(f'{oname} = schedule_last({s["name"]}, "{col}")')
                elif otype == "sum":
                    lines.append(f'{oname} = schedule_sum({s["name"]}, "{col}")')
                elif otype == "column":
                    lines.append(f'{oname} = schedule_column({s["name"]}, "{col}")')
                elif otype == "filter":
                    lines.append(f'{oname} = schedule_filter({s["name"]}, "{o.get("matchCol")}", {o.get("matchValue")}, "{col}")')
                if oname:
                    defined.append(oname)
            lines.append("")

    outputs = rule.get("outputs") or {}
    txns = [t for t in (outputs.get("transactions") or []) if t and t.get("type")]
    if txns:
        lines.append("")
        lines.append("## Create Transactions")
        for txn in txns:
            if not (txn.get("postingDate") and txn.get("effectiveDate")):
                continue
            amt = txn.get("amount") or (defined[-1] if defined else "0")
            pd = txn["postingDate"]
            ed = txn["effectiveDate"]
            sid = txn.get("subInstrumentId") or ""
            ttype = txn["type"]
            if sid:
                lines.append(f'createTransaction({pd}, {ed}, "{ttype}", {amt}, {sid})')
            else:
                lines.append(f'createTransaction({pd}, {ed}, "{ttype}", {amt})')

    return "\n".join(lines)


def _effective_rule_type(steps: list[dict]) -> str:
    types = {s.get("stepType") for s in steps or []}
    if "schedule" in types:
        return "schedule"
    if "custom_code" in types:
        return "custom_code"
    if "iteration" in types:
        return "iteration"
    if "condition" in types:
        return "conditional"
    return "simple_calc"


def _rule_to_legacy_payload(rule: dict) -> dict:
    """Derive the legacy denormalised fields (variables/conditions/iterations)
    that the old rule-builder code expects, from the unified `steps` array."""
    steps = rule.get("steps") or []
    variables = []
    for s in steps:
        if s.get("stepType") == "calc":
            variables.append({
                "name": s.get("name"),
                "source": s.get("source") or "formula",
                "formula": s.get("formula") or "",
                "value": s.get("value") or "",
                "eventField": s.get("eventField") or "",
                "collectType": s.get("collectType") or "collect_by_instrument",
            })
    cond_step = next((s for s in steps if s.get("stepType") == "condition"), None)
    iter_step = next((s for s in steps if s.get("stepType") == "iteration"), None)
    iterations = (iter_step or {}).get("iterations") or []
    return {
        "variables": variables,
        "conditions": (cond_step or {}).get("conditions") or [],
        "elseFormula": (cond_step or {}).get("elseFormula") or "",
        "conditionResultVar": (cond_step or {}).get("name") or "result",
        "iterations": iterations,
        "iterConfig": iterations[0] if iterations else {},
        "ruleType": _effective_rule_type(steps),
    }


async def _load_rule(rule_id: str) -> dict:
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    doc = await db.saved_rules.find_one({"id": rule_id}, {"_id": 0})
    if not doc:
        # Try by name
        doc = await db.saved_rules.find_one(
            {"name": {"$regex": f"^{re.escape(rule_id)}$", "$options": "i"}},
            {"_id": 0},
        )
    if not doc:
        raise ToolError(f"Rule '{rule_id}' not found")
    return doc


async def _next_priority() -> int:
    db = _ServerBridge.db
    if db is None:
        return 1
    used: set[int] = set()
    async for r in db.saved_rules.find({}, {"_id": 0, "priority": 1}):
        if isinstance(r.get("priority"), int):
            used.add(r["priority"])
    async for s in db.saved_schedules.find({}, {"_id": 0, "priority": 1}):
        if isinstance(s.get("priority"), int):
            used.add(s["priority"])
    p = 1
    while p in used:
        p += 1
    return p


def _validate_step_shape(step: dict) -> dict:
    """Normalise & lightly validate a step dict provided by the agent."""
    if not isinstance(step, dict):
        raise ToolError("step must be an object")
    st = step.get("stepType") or "calc"
    if st not in ("calc", "condition", "iteration", "schedule", "custom_code"):
        raise ToolError(f"Unknown stepType '{st}'")
    if st == "custom_code":
        raise ToolError("custom_code steps are forbidden — compose with calc/condition/iteration/schedule only")
    if not step.get("name"):
        raise ToolError("step.name is required")
    name = step["name"]
    out: dict = {"name": name, "stepType": st}
    if st == "calc":
        src = step.get("source") or "formula"
        if src not in ("formula", "value", "event_field", "collect"):
            raise ToolError(f"Unknown calc source '{src}'")
        out.update({
            "source": src,
            "formula": step.get("formula") or "",
            "value": step.get("value") or "",
            "eventField": step.get("eventField") or "",
            "collectType": step.get("collectType") or "collect_by_instrument",
        })
        # Guardrails on formula content
        if src == "formula" and out["formula"]:
            _enforce_dsl_guardrails(out["formula"])
            _check_formula_expression(out["formula"], where=f"step '{name}'.formula")
            _check_function_calls(out["formula"], where=f"step '{name}'.formula")
            # Anti-pattern: agent putting createTransaction(...) inside a calc
            # formula. The Rule Builder UI exposes a dedicated "+ Add Transaction"
            # panel that maps to outputs.transactions[]. Calc-step formulas with
            # createTransaction calls hide the transaction from that panel and
            # are the wrong abstraction.
            if re.search(r"\bcreateTransaction\s*\(", out["formula"]):
                raise ToolError(
                    f"step '{name}'.formula calls createTransaction(...) "
                    f"directly. This is the WRONG place — transactions must "
                    f"live in the rule's `outputs.transactions[]` array so "
                    f"they appear in the Transactions panel of the Rule "
                    f"Builder UI. Move the transaction into outputs (using "
                    f"create_saved_rule's `outputs` argument or "
                    f"update_saved_rule), and use this calc step ONLY to "
                    f"compute the amount that the transaction will reference "
                    f"by variable name."
                )
        if src == "value" and out["value"]:
            _enforce_dsl_guardrails(out["value"])
            _check_formula_expression(out["value"], where=f"step '{name}'.value")
    elif st == "condition":
        out["conditions"] = step.get("conditions") or []
        out["elseFormula"] = step.get("elseFormula") or ""
        if not out["conditions"]:
            raise ToolError(f"step '{name}': condition step requires at least one entry in `conditions`")
        for i, c in enumerate(out["conditions"]):
            for k in ("condition", "thenFormula"):
                v = c.get(k)
                if isinstance(v, str) and v:
                    _enforce_dsl_guardrails(v)
                    _check_formula_expression(v, where=f"step '{name}'.conditions[{i}].{k}")
                    _check_function_calls(v, where=f"step '{name}'.conditions[{i}].{k}")
        if out["elseFormula"]:
            _enforce_dsl_guardrails(out["elseFormula"])
            _check_formula_expression(out["elseFormula"], where=f"step '{name}'.elseFormula")
            _check_function_calls(out["elseFormula"], where=f"step '{name}'.elseFormula")
    elif st == "iteration":
        out["iterations"] = step.get("iterations") or []
        if not out["iterations"]:
            raise ToolError(f"step '{name}': iteration step requires at least one entry in `iterations`")
        for i, it in enumerate(out["iterations"]):
            if not it.get("resultVar"):
                raise ToolError(f"step '{name}'.iterations[{i}].resultVar is required")
            # sourceArray must be a variable name, not a literal `[...]`
            sa = it.get("sourceArray")
            if isinstance(sa, str) and sa.strip().startswith("["):
                raise ToolError(
                    f"step '{name}'.iterations[{i}].sourceArray must be a "
                    f"VARIABLE NAME (a previously-defined collection), not a "
                    f"literal array. Got: {sa[:80]!r}. To iterate over a fresh "
                    f"list, define it in a prior calc step first."
                )
            for k in ("expression", "sourceArray", "secondArray"):
                v = it.get(k)
                if isinstance(v, str) and v:
                    _enforce_dsl_guardrails(v)
            # Iteration expression has the strictest single-line rule
            if it.get("expression"):
                where = f"step '{name}'.iterations[{i}].expression"
                _check_iteration_expression(it["expression"], where=where)
                # `each` and `second` are loop-locals available inside expression
                _check_function_calls(it["expression"], where=where,
                                       extra_names={"each", "second", it.get("resultVar") or ""})
    elif st == "schedule":
        sc = step.get("scheduleConfig") or {}
        if not isinstance(sc, dict):
            raise ToolError("schedule.scheduleConfig must be an object")
        if not sc.get("columns"):
            raise ToolError("schedule must define at least one column in scheduleConfig.columns")
        for c in sc.get("columns") or []:
            if not c.get("name") or not c.get("formula"):
                raise ToolError("each schedule column needs name + formula")
            _enforce_dsl_guardrails(c["formula"])
            _check_formula_expression(c["formula"],
                                       where=f"step '{name}'.scheduleConfig.columns['{c.get('name')}'].formula")
        for k in ("periodCountFormula", "startDateFormula", "endDateFormula"):
            v = sc.get(k)
            if isinstance(v, str) and v:
                _enforce_dsl_guardrails(v)
                _check_formula_expression(v, where=f"step '{name}'.scheduleConfig.{k}")
        out["scheduleConfig"] = sc
        out["outputVars"] = step.get("outputVars") or []
        for ov in out["outputVars"]:
            if not ov.get("name") or not ov.get("type"):
                raise ToolError("schedule outputVar requires name + type")
            if ov["type"] not in ("first", "last", "sum", "column", "filter"):
                raise ToolError(f"unknown outputVar type '{ov['type']}'")
    if step.get("inlineComment"):
        out["inlineComment"] = True
        out["commentText"] = step.get("commentText") or ""
    if step.get("printResult"):
        out["printResult"] = True
    return out


async def _save_rule_doc(rule: dict, *, is_new: bool) -> dict:
    """Insert or replace a saved_rules document. Refreshes generatedCode &
    legacy denorm fields so the rule loads cleanly in the UI."""
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    legacy = _rule_to_legacy_payload(rule)
    rule.update(legacy)
    rule["generatedCode"] = _generate_rule_code(rule)
    rule["updated_at"] = datetime.now(timezone.utc).isoformat()
    if is_new:
        rule.setdefault("id", str(uuid.uuid4()))
        rule.setdefault("created_at", rule["updated_at"])
        rule.setdefault("outputs", {"printResult": True, "createTransaction": False, "transactions": []})
        rule.setdefault("inlineComment", False)
        rule.setdefault("commentText", "")
        rule.setdefault("customCode", "")
        await db.saved_rules.insert_one(rule)
    else:
        await db.saved_rules.replace_one({"id": rule["id"]}, rule, upsert=True)
    rule.pop("_id", None)
    return rule


# ──────────────────────────────────────────────────────────────────────────
# Rule tools
# ──────────────────────────────────────────────────────────────────────────

async def tool_list_saved_rules(args: dict) -> dict:
    db = _ServerBridge.db
    if db is None:
        return {"rules": []}
    name_filter = (args.get("name_filter") or "").strip()
    query: dict = {}
    if name_filter:
        query["name"] = {"$regex": re.escape(name_filter), "$options": "i"}
    docs = await db.saved_rules.find(query, {"_id": 0, "generatedCode": 0}).sort("priority", 1).to_list(500)
    return {"rules": [{
        "id": d.get("id"),
        "name": d.get("name"),
        "priority": d.get("priority"),
        "ruleType": d.get("ruleType"),
        "step_count": len(d.get("steps") or []),
        "step_names": [s.get("name") for s in (d.get("steps") or []) if s.get("name")],
    } for d in docs]}


async def tool_get_saved_rule(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    return {"rule": rule}


async def tool_create_saved_rule(args: dict) -> dict:
    name = (args.get("name") or "").strip()
    if not name:
        raise ToolError("`name` is required")
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    existing = await db.saved_rules.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        {"_id": 0, "id": 1},
    )
    if existing:
        raise ToolError(f"A rule named '{name}' already exists. Use update_saved_rule or pick a different name.")
    steps_in = args.get("steps") or []
    steps = [_validate_step_shape(s) for s in steps_in]
    priority = args.get("priority")
    if priority is None:
        priority = await _next_priority()
    else:
        priority = int(priority)
        # Check uniqueness across rules and schedules
        clash = await db.saved_rules.find_one({"priority": priority}, {"_id": 0, "name": 1})
        if clash:
            raise ToolError(f"Priority {priority} is already used by rule '{clash['name']}'")
        clash_s = await db.saved_schedules.find_one({"priority": priority}, {"_id": 0, "name": 1})
        if clash_s:
            raise ToolError(f"Priority {priority} is already used by schedule '{clash_s['name']}'")
    rule = {
        "name": name,
        "priority": priority,
        "steps": steps,
        "outputs": args.get("outputs") or {"printResult": True, "createTransaction": False, "transactions": []},
        "inlineComment": False,
        "commentText": "",
    }
    rule = await _save_rule_doc(rule, is_new=True)
    return {"rule_id": rule["id"], "name": name, "priority": priority, "step_count": len(steps)}


async def tool_update_saved_rule(args: dict) -> dict:
    rule_id = (args.get("rule_id") or "").strip()
    if not rule_id:
        raise ToolError("rule_id is required")
    rule = await _load_rule(rule_id)
    patch = args.get("patch") or {}
    for k in ("name", "priority", "outputs", "inlineComment", "commentText"):
        if k in patch:
            rule[k] = patch[k]
    if "steps" in patch:
        rule["steps"] = [_validate_step_shape(s) for s in (patch["steps"] or [])]
    rule = await _save_rule_doc(rule, is_new=False)
    return {"rule_id": rule["id"], "name": rule["name"], "step_count": len(rule.get("steps") or [])}


async def tool_delete_saved_rule(args: dict) -> dict:
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    rule_id = (args.get("rule_id") or "").strip()
    if not rule_id:
        raise ToolError("rule_id is required")
    if not bool(args.get("confirm")):
        raise ToolError("Destructive — call again with confirm=true after user approval")
    rule = await _load_rule(rule_id)
    await db.saved_rules.delete_one({"id": rule["id"]})
    return {"deleted": rule["name"], "id": rule["id"]}


# ──────────────────────────────────────────────────────────────────────────
# Step tools (operate on a parent rule)
# ──────────────────────────────────────────────────────────────────────────

def _resolve_step_index(rule: dict, args: dict) -> int:
    steps = rule.get("steps") or []
    if "step_index" in args and args["step_index"] is not None:
        idx = int(args["step_index"])
        if idx < 0 or idx >= len(steps):
            raise ToolError(f"step_index {idx} out of range (0..{len(steps)-1})")
        return idx
    name = (args.get("step_name") or "").strip()
    if not name:
        raise ToolError("step_index or step_name is required")
    for i, s in enumerate(steps):
        if (s.get("name") or "") == name:
            return i
    raise ToolError(f"step '{name}' not found in rule '{rule.get('name')}'")


async def tool_add_step_to_rule(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    step = _validate_step_shape(args.get("step") or {})
    steps = list(rule.get("steps") or [])
    if any((s.get("name") or "") == step["name"] for s in steps):
        raise ToolError(f"step '{step['name']}' already exists in rule '{rule['name']}'")
    pos = args.get("position")
    if pos is None or int(pos) >= len(steps):
        steps.append(step)
    else:
        steps.insert(max(0, int(pos)), step)
    rule["steps"] = steps
    rule = await _save_rule_doc(rule, is_new=False)
    return {"rule_id": rule["id"], "step_name": step["name"], "step_count": len(steps)}


async def tool_update_step(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    patch = args.get("patch") or {}
    if not isinstance(patch, dict) or not patch:
        raise ToolError("patch must be a non-empty object")
    merged = {**rule["steps"][idx], **patch}
    merged = _validate_step_shape(merged)
    rule["steps"][idx] = merged
    rule = await _save_rule_doc(rule, is_new=False)
    return {"rule_id": rule["id"], "step_index": idx, "step_name": merged["name"]}


async def tool_delete_step(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    removed = rule["steps"].pop(idx)
    rule = await _save_rule_doc(rule, is_new=False)
    return {"rule_id": rule["id"], "deleted_step": removed.get("name"), "step_count": len(rule["steps"])}


async def tool_debug_step(args: dict) -> dict:
    """Run the rule's DSL up to and including a chosen step, printing the
    step's variable so the agent can observe its value (and any prior vars
    referenced via prints embedded in the rule)."""
    dsl_to_python_multi_event = _h("dsl_to_python_multi_event")
    execute_python_template = _h("execute_python_template")
    merge_event_data_by_instrument = _h("merge_event_data_by_instrument")
    filter_event_data_by_posting_date = _h("filter_event_data_by_posting_date")
    extract_event_names_from_dsl = _h("extract_event_names_from_dsl")

    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    steps = rule.get("steps") or []
    target = steps[idx]

    # Re-build code only up to the target step (drop later steps)
    truncated = {**rule, "steps": steps[: idx + 1], "outputs": {}}
    code = _generate_rule_code(truncated)

    # Append a print line for the target var so we capture its value
    if target.get("stepType") == "iteration":
        last = (target.get("iterations") or [])
        var = (last[-1].get("resultVar") if last else None) or target.get("name")
    else:
        var = target.get("name")
    if var:
        code += f'\nprint("__DEBUG_STEP__ {var} =", {var})'

    referenced = list(extract_event_names_from_dsl(code) or [])
    all_event_fields: dict[str, Any] = {}
    event_data: dict[str, list[dict]] = {}
    for nm in referenced:
        evt = await _find_event_def(nm)
        if not evt:
            raise ToolError(f"Event '{nm}' referenced by rule not found")
        all_event_fields[nm] = {"fields": evt.get("fields", []), "eventType": evt.get("eventType", "activity")}
        rows = []
        db = _ServerBridge.db
        if db is not None:
            doc = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(nm)}$", "$options": "i"}}, {"_id": 0}
            )
            if doc:
                rows = doc.get("data_rows") or []
        if not rows:
            for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
                if str(d.get("event_name", "")).lower() == nm.lower():
                    rows = d.get("data_rows") or []
                    break
        event_data[nm] = rows

    posting_date = args.get("posting_date")
    activity_data = {k: v for k, v in event_data.items()
                     if all_event_fields[k]["eventType"] == "activity"}
    scoped = (filter_event_data_by_posting_date(activity_data, posting_date) if posting_date else activity_data)
    merged = merge_event_data_by_instrument(scoped) if activity_data else [{}]
    if not merged:
        merged = [{}]

    try:
        py = dsl_to_python_multi_event(code, all_event_fields) if all_event_fields else _h("dsl_to_python_standalone")(code)
    except Exception as exc:
        raise ToolError(f"DSL translation failed for debug step: {exc}") from exc
    try:
        result = await execute_python_template(py, merged, event_data, posting_date, args.get("effective_date"))
    except Exception as exc:
        raise ToolError(f"Execution failed: {exc}") from exc

    prints = result.get("print_outputs") or []
    debug_lines = [p for p in prints if "__DEBUG_STEP__" in str(p)][:50]
    return {
        "rule_id": rule["id"],
        "step_name": target.get("name"),
        "variable": var,
        "code": code,
        "row_count": len(merged),
        "debug_outputs": debug_lines,
        "all_prints": prints[:50],
    }


# ──────────────────────────────────────────────────────────────────────────
# Schedule tools
# ──────────────────────────────────────────────────────────────────────────

async def tool_list_saved_schedules(_args: dict) -> dict:
    db = _ServerBridge.db
    if db is None:
        return {"schedules": []}
    docs = await db.saved_schedules.find({}, {"_id": 0}).sort("priority", 1).to_list(500)
    return {"schedules": [{
        "id": d.get("id"),
        "name": d.get("name"),
        "priority": d.get("priority"),
    } for d in docs]}


async def tool_create_saved_schedule(args: dict) -> dict:
    """Create a standalone saved schedule. Agent provides DSL code that defines
    the schedule (using period() + schedule() DSL functions). The schedule then
    becomes available as a building block that can be attached to a template
    via attach_rules_to_template."""
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    name = (args.get("name") or "").strip()
    if not name:
        raise ToolError("`name` is required")
    dsl_code = (args.get("dsl_code") or "").strip()
    if not dsl_code:
        raise ToolError("`dsl_code` is required (must define `period(...)` then `schedule(...)`)")
    _enforce_dsl_guardrails(dsl_code)
    if "schedule(" not in dsl_code:
        raise ToolError("dsl_code must contain a schedule(...) call")

    existing = await db.saved_schedules.find_one(
        {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        {"_id": 0, "id": 1},
    )
    if existing:
        raise ToolError(f"A schedule named '{name}' already exists.")

    priority = args.get("priority")
    if priority is None:
        priority = await _next_priority()
    else:
        priority = int(priority)
        clash = await db.saved_rules.find_one({"priority": priority}, {"_id": 0, "name": 1})
        if clash:
            raise ToolError(f"Priority {priority} is already used by rule '{clash['name']}'")
        clash_s = await db.saved_schedules.find_one({"priority": priority}, {"_id": 0, "name": 1})
        if clash_s:
            raise ToolError(f"Priority {priority} is already used by schedule '{clash_s['name']}'")

    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "id": str(uuid.uuid4()),
        "name": name,
        "priority": priority,
        "generatedCode": dsl_code,
        "config": args.get("config") or {},
        "created_at": now,
        "updated_at": now,
    }
    await db.saved_schedules.insert_one(doc)
    return {"schedule_id": doc["id"], "name": name, "priority": priority}


async def tool_delete_saved_schedule(args: dict) -> dict:
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    sid = (args.get("schedule_id") or "").strip()
    if not sid:
        raise ToolError("schedule_id is required")
    if not bool(args.get("confirm")):
        raise ToolError("Destructive — call again with confirm=true after user approval")
    res = await db.saved_schedules.delete_one({"id": sid})
    if res.deleted_count == 0:
        raise ToolError(f"schedule '{sid}' not found")
    return {"deleted_id": sid}


async def tool_debug_schedule(args: dict) -> dict:
    """Execute a saved schedule's DSL code in isolation and return the
    materialised rows. This is the equivalent of clicking the play button
    on a schedule card in the UI."""
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    sid = (args.get("schedule_id") or args.get("name") or "").strip()
    if not sid:
        raise ToolError("schedule_id or name is required")
    sched = await db.saved_schedules.find_one({"id": sid}, {"_id": 0})
    if not sched:
        sched = await db.saved_schedules.find_one(
            {"name": {"$regex": f"^{re.escape(sid)}$", "$options": "i"}}, {"_id": 0}
        )
    if not sched:
        raise ToolError(f"Schedule '{sid}' not found")

    dsl_code = sched.get("generatedCode") or sched.get("dsl_code") or ""
    if not dsl_code:
        raise ToolError(f"Schedule '{sched.get('name')}' has no generatedCode")

    extract_event_names_from_dsl = _h("extract_event_names_from_dsl")
    dsl_to_python_multi_event = _h("dsl_to_python_multi_event")
    dsl_to_python_standalone = _h("dsl_to_python_standalone")
    execute_python_template = _h("execute_python_template")
    merge_event_data_by_instrument = _h("merge_event_data_by_instrument")
    filter_event_data_by_posting_date = _h("filter_event_data_by_posting_date")

    referenced = list(extract_event_names_from_dsl(dsl_code) or [])
    all_event_fields: dict[str, Any] = {}
    event_data: dict[str, list[dict]] = {}
    for nm in referenced:
        evt = await _find_event_def(nm)
        if not evt:
            raise ToolError(f"Event '{nm}' referenced by schedule not found")
        all_event_fields[nm] = {"fields": evt.get("fields", []), "eventType": evt.get("eventType", "activity")}
        rows: list = []
        doc = await db.event_data.find_one(
            {"event_name": {"$regex": f"^{re.escape(nm)}$", "$options": "i"}}, {"_id": 0}
        )
        if doc:
            rows = doc.get("data_rows") or []
        event_data[nm] = rows

    posting_date = args.get("posting_date")
    activity_data = {k: v for k, v in event_data.items()
                     if all_event_fields.get(k, {}).get("eventType") == "activity"}
    scoped = (filter_event_data_by_posting_date(activity_data, posting_date) if posting_date else activity_data)
    merged = merge_event_data_by_instrument(scoped) if activity_data else [{}]
    if not merged:
        merged = [{}]

    try:
        py = (dsl_to_python_multi_event(dsl_code, all_event_fields) if all_event_fields
              else dsl_to_python_standalone(dsl_code))
    except Exception as exc:
        raise ToolError(f"Schedule DSL translation failed: {exc}") from exc
    try:
        result = await execute_python_template(py, merged, event_data, posting_date, args.get("effective_date"))
    except Exception as exc:
        raise ToolError(f"Schedule execution failed: {exc}") from exc

    prints = (result.get("print_outputs") or [])[:30]
    sample_limit = int(args.get("sample_limit") or 6)
    return {
        "schedule_id": sched.get("id"),
        "schedule_name": sched.get("name"),
        "row_count_input": len(merged),
        "print_outputs": prints[:sample_limit + 5],
        "tip": "Inspect print_outputs for the materialised schedule rows.",
    }


async def tool_verify_rule_complete(args: dict) -> dict:
    """Run a comprehensive readiness check on a saved rule: every step is
    debug-runnable, every transaction-emitting rule has outputs.transactions
    populated (not createTransaction calls in formulas), every referenced
    transaction type is registered, and every referenced event has data.

    Returns a checklist. Until all items report `ok: true`, the rule is not
    ready and the agent should NOT call `finish`."""
    rule = await _load_rule((args.get("rule_id") or "").strip())
    db = _ServerBridge.db
    steps = rule.get("steps") or []
    outputs = rule.get("outputs") or {}
    txns = outputs.get("transactions") or []

    items: list[dict] = []

    # 1. Every step has a name + valid stepType
    bad_steps = [i for i, s in enumerate(steps) if not s.get("name") or not s.get("stepType")]
    items.append({
        "check": "all_steps_named_and_typed",
        "ok": not bad_steps and bool(steps),
        "detail": (f"{len(steps)} steps" if not bad_steps else
                   f"unnamed/untyped steps at indexes {bad_steps}"),
    })

    # 2. createTransaction not used inside any calc formula (anti-pattern)
    inline_txn_steps: list[str] = []
    for s in steps:
        if s.get("stepType") == "calc" and re.search(r"\bcreateTransaction\s*\(", s.get("formula") or ""):
            inline_txn_steps.append(s.get("name") or "?")
    items.append({
        "check": "no_inline_createTransaction",
        "ok": not inline_txn_steps,
        "detail": ("clean" if not inline_txn_steps
                   else f"steps using createTransaction in formula (move to outputs.transactions): {inline_txn_steps}"),
    })

    # 3. outputs.transactions populated and balanced (debit/credit pair)
    has_txns = bool(txns)
    valid_txn_types = sum(1 for t in txns if (t.get("type") or "").strip() and (t.get("amount") or "").strip())
    sides = [(t.get("side") or "").lower() for t in txns]
    has_debit = any(s == "debit" for s in sides)
    has_credit = any(s == "credit" for s in sides)
    items.append({
        "check": "outputs_transactions_populated",
        "ok": has_txns and valid_txn_types == len(txns),
        "detail": (f"{valid_txn_types}/{len(txns)} transactions valid"
                   if has_txns else "no transactions in outputs.transactions[]"),
    })
    items.append({
        "check": "double_entry_pair",
        "ok": (not has_txns) or (has_debit and has_credit),
        "detail": ("balanced (has debit and credit)" if has_debit and has_credit
                   else "MISSING SIDE — every txn-emitting rule needs both a debit and a credit"),
    })

    # 4. Every referenced transaction type is registered
    txn_types_used = sorted({(t.get("type") or "").strip() for t in txns if t.get("type")})
    registered: set[str] = set()
    if db is not None:
        try:
            async for d in db.transaction_types.find({}, {"_id": 0, "name": 1}):
                if d.get("name"):
                    registered.add(d["name"])
        except Exception:
            pass
    missing_types = [t for t in txn_types_used if t and t not in registered]
    items.append({
        "check": "transaction_types_registered",
        "ok": not missing_types,
        "detail": ("all registered" if not missing_types
                   else f"unregistered: {missing_types} — call add_transaction_types"),
    })

    # 5. Every referenced event has sample data loaded
    extract = _h("extract_event_names_from_dsl")
    code = rule.get("generatedCode") or _generate_rule_code(rule)
    referenced_events = list(extract(code) or [])
    missing_data: list[str] = []
    if db is not None:
        for nm in referenced_events:
            evt = await _find_event_def(nm)
            if not evt:
                missing_data.append(f"{nm} (no definition)")
                continue
            if (evt.get("eventType") or "activity") == "reference":
                continue
            doc = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(nm)}$", "$options": "i"}}, {"_id": 0}
            )
            if not doc or not (doc.get("data_rows") or []):
                missing_data.append(nm)
    items.append({
        "check": "event_data_present",
        "ok": not missing_data,
        "detail": ("all events have data" if not missing_data
                   else f"no sample data for: {missing_data} — call generate_sample_event_data"),
    })

    # 6. Every step is debug-runnable (try debug_step on each)
    debug_results: list[dict] = []
    for i, s in enumerate(steps):
        try:
            res = await tool_debug_step({"rule_id": rule["id"], "step_index": i,
                                           "posting_date": args.get("posting_date")})
            debug_results.append({
                "step": s.get("name"), "ok": True,
                "preview": (res.get("debug_outputs") or [None])[0],
            })
        except ToolError as te:
            debug_results.append({"step": s.get("name"), "ok": False, "error": str(te)[:200]})
    all_steps_ok = all(d["ok"] for d in debug_results)
    items.append({
        "check": "all_steps_debug_run_clean",
        "ok": all_steps_ok,
        "detail": (f"{sum(1 for d in debug_results if d['ok'])}/{len(debug_results)} steps debugged successfully"),
    })

    overall_ok = all(it["ok"] for it in items)
    return {
        "rule_id": rule["id"],
        "rule_name": rule.get("name"),
        "overall_ready": overall_ok,
        "checklist": items,
        "step_debug_results": debug_results,
        "next_action": (
            "Rule is READY. You may now attach it to a template and dry_run_template."
            if overall_ok else
            "Fix the failing checks above before declaring the rule complete."
        ),
    }


# ──────────────────────────────────────────────────────────────────────────
# Template assembly
# ──────────────────────────────────────────────────────────────────────────

async def tool_attach_rules_to_template(args: dict) -> dict:
    """Attach a set of saved rules (and optional schedules) to a user template,
    rebuilding `combinedCode` so the template runs end-to-end."""
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    template_id_or_name = (args.get("template_id") or args.get("name") or "").strip()
    if not template_id_or_name:
        raise ToolError("template_id or name is required")
    tpl = await db.user_templates.find_one({"id": template_id_or_name}, {"_id": 0})
    if not tpl:
        tpl = await db.user_templates.find_one(
            {"name": {"$regex": f"^{re.escape(template_id_or_name)}$", "$options": "i"}}, {"_id": 0}
        )
    if not tpl:
        raise ToolError(f"Template '{template_id_or_name}' not found")

    rule_ids = args.get("rule_ids") or []
    schedule_ids = args.get("schedule_ids") or []

    # Load rules and sort by priority
    rules: list[dict] = []
    for rid in rule_ids:
        rule = await _load_rule(rid)
        rules.append(rule)
    rules.sort(key=lambda r: r.get("priority") if isinstance(r.get("priority"), int) else 9999)

    schedules: list[dict] = []
    for sid in schedule_ids:
        s = await db.saved_schedules.find_one({"id": sid}, {"_id": 0})
        if not s:
            raise ToolError(f"Schedule '{sid}' not found")
        schedules.append(s)

    combined_parts: list[str] = []
    for r in rules:
        code = r.get("generatedCode") or _generate_rule_code(r)
        combined_parts.append(code)
    combined_code = "\n\n".join(combined_parts)

    now = datetime.now(timezone.utc).isoformat()
    update_fields = {
        "rules": rules,
        "schedules": schedules,
        "combinedCode": combined_code,
        "updated_at": now,
    }
    await db.user_templates.update_one({"id": tpl["id"]}, {"$set": update_fields})

    # Also mirror combinedCode into dsl_templates so dry_run_template works.
    try:
        evt_names = list(_h("extract_event_names_from_dsl")(combined_code) or [])
        primary_event = evt_names[0] if evt_names else None
        if primary_event:
            evt = await _find_event_def(primary_event)
            if evt:
                py = _h("dsl_to_python")(combined_code, evt["fields"])
                DSLTemplate = _h("DSLTemplate")
                await db.dsl_templates.delete_many({"name": tpl["name"]})
                t = DSLTemplate(name=tpl["name"], dsl_code=combined_code, python_code=py)
                tdoc = t.model_dump()
                tdoc["created_at"] = tdoc["created_at"].isoformat()
                await db.dsl_templates.insert_one(tdoc)
    except Exception as exc:
        logger.warning("Mirror to dsl_templates failed during attach: %s", exc)

    return {
        "template_id": tpl["id"],
        "template_name": tpl["name"],
        "rule_count": len(rules),
        "schedule_count": len(schedules),
        "combined_lines": len(combined_code.splitlines()),
    }


async def tool_finish(args: dict) -> dict:
    summary = (args.get("summary") or "").strip() or "Done."
    return {"summary": summary, "done": True}


# ──────────────────────────────────────────────────────────────────────────
# DSL syntax cheat sheet — the agent should call this whenever it is
# unsure how to express something, and ALWAYS after a syntax error.
# ──────────────────────────────────────────────────────────────────────────

_DSL_SYNTAX_GUIDE = """\
FYNTRAC DSL — SYNTAX & STRUCTURE GUIDE
=======================================

This DSL is a Python-like EXPRESSION language. It is NOT a general-purpose
language. The constraints below are BINDING — violating them produces
errors that look like "unterminated string literal" or "invalid syntax".

------------------------------------------------------------------
ABSOLUTE RULES
------------------------------------------------------------------
1. Every step has ONE expression. SINGLE-LINE, SINGLE-STATEMENT.
   - No newlines inside an expression.
   - No `let` bindings.
   - No `;`-separated statements.
2. There is NO `for` / `while` loop. Iteration is done via
   stepType='iteration' with sourceArray.
3. There is NO `arr[i]` bracket indexing.
   - Use lookup(arr, idx) or element_at(arr, idx).
4. There is NO `outputs.events.push(...)` and NO `createEventRow(...)`.
   - Synthetic events must be pre-loaded with create_event_definitions
     + generate_sample_event_data BEFORE the rule runs.
5. Conditionals INSIDE expressions: if(cond, then_value, else_value).
   - For multi-branch logic, use a stepType='condition' step.
6. iteration.sourceArray is a VARIABLE NAME (a previously-defined
   collection), NOT a literal `[1,2,3]`.
7. Math: ALWAYS use multiply(a,b), divide(a,b), add(a,b), subtract(a,b),
   modulo(a,b), power(a,b). The function form is always safe.
8. Globals available everywhere: postingdate, effectivedate,
   instrumentid, subinstrumentid (lowercase, no event prefix).
9. Event fields: EVENTNAME.fieldname — case-insensitive but the event
   must exist (verify with list_events).

------------------------------------------------------------------
STEP SHAPES — copy these exactly when authoring rules
------------------------------------------------------------------

CALC (formula source) — most common
{
  "name": "interest_amount",
  "stepType": "calc",
  "source": "formula",
  "formula": "multiply(LoanEvent.principal, divide(LoanEvent.rate, 12))"
}

CALC (event_field source) — copy a field into a variable
{
  "name": "principal",
  "stepType": "calc",
  "source": "event_field",
  "eventField": "LoanEvent.principal"
}

CALC (collect source) — collect values across instruments
{
  "name": "all_principals",
  "stepType": "calc",
  "source": "collect",
  "eventField": "LoanEvent.principal",
  "collectType": "collect_by_instrument"
}

CONDITION
{
  "name": "stage",
  "stepType": "condition",
  "conditions": [
    {"condition": "gt(days_overdue, 90)", "thenFormula": "3"},
    {"condition": "gt(days_overdue, 30)", "thenFormula": "2"}
  ],
  "elseFormula": "1"
}

ITERATION (apply_each — operate on each element)
{
  "name": "doubled_balances",
  "stepType": "iteration",
  "iterations": [{
    "type": "apply_each",
    "sourceArray": "all_principals",   // VARIABLE NAME, not [...]
    "expression": "multiply(each, 2)", // SINGLE-LINE only
    "resultVar": "doubled_balances"
  }]
}

ITERATION (multi-step calculation per element)
WRONG — multi-line, will fail:
  "expression": "let x = multiply(each, rate)\\nlookup(x, 0)"
RIGHT — split into two iteration entries:
  iterations: [
    {"type":"apply_each","sourceArray":"items","expression":"multiply(each, rate)","resultVar":"scaled"},
    {"type":"apply_each","sourceArray":"scaled","expression":"add(each, 1)","resultVar":"adjusted"}
  ]

SCHEDULE (amortisation, ECL projection, etc.)
{
  "name": "amort_schedule",
  "stepType": "schedule",
  "scheduleConfig": {
    "periodType": "number",
    "periodCount": 12,
    "frequency": "M",
    "columns": [
      {"name": "period",   "formula": "i"},
      {"name": "interest", "formula": "multiply(balance, monthly_rate)"},
      {"name": "principal","formula": "subtract(payment, interest)"}
    ],
    "contextVars": ["balance", "monthly_rate", "payment"]
  },
  "outputVars": [
    {"name": "total_interest", "type": "sum",  "column": "interest"}
  ]
}

------------------------------------------------------------------
EMITTING TRANSACTIONS
------------------------------------------------------------------
Preferred — put them in the rule's outputs.transactions[]:
{
  "outputs": {
    "createTransaction": true,
    "transactions": [
      {"type": "InterestIncome",     "amount": "interest_amount", "side": "credit"},
      {"type": "InterestReceivable", "amount": "interest_amount", "side": "debit"}
    ]
  }
}
- "amount" is a VARIABLE NAME defined in a prior step.
- Always emit BOTH SIDES of the double-entry; debits must equal credits.
- Register every transaction TYPE via add_transaction_types FIRST.

------------------------------------------------------------------
WHEN A DRY-RUN FAILS WITH "unterminated string literal" OR "invalid syntax"
------------------------------------------------------------------
The error is almost always one of:
  (a) An iteration.expression contains a NEWLINE — split into multiple
      iteration entries.
  (b) An expression contains `arr[i]` — replace with lookup(arr, i).
  (c) An expression contains `outputs.events.push` or `createEventRow` —
      remove it; pre-load synthetic events instead.
  (d) An expression contains a Python `for`/`while` loop — convert to
      stepType='iteration'.
  (e) A function name is misspelled — call list_dsl_functions.

After ONE retry, if the error class is the same: STOP. Call
get_saved_rule on a working rule with the same step type and copy its shape.
"""


async def tool_get_dsl_syntax_guide(_args: dict) -> dict:
    """Return the binding DSL constraints + worked examples of every step
    shape. Call this when you are unsure how to express something or after
    a syntax-class error."""
    return {
        "guide": _DSL_SYNTAX_GUIDE,
        "function_count": len(_ServerBridge.helpers.get("DSL_FUNCTION_METADATA") or []),
        "next_step_hint": (
            "Copy one of the step-shape examples above EXACTLY, then adapt "
            "the names/formulas. Use list_dsl_functions for the catalog of "
            "available functions."
        ),
    }


# ──────────────────────────────────────────────────────────────────────────
# Tool registry
# ──────────────────────────────────────────────────────────────────────────

DESTRUCTIVE_TOOLS = {
    "delete_template",
    "clear_all_data",
    "delete_saved_rule",
    "delete_saved_schedule",
}

TOOLS: dict[str, Callable[[dict], Awaitable[dict]]] = {
    "list_events": tool_list_events,
    "list_dsl_functions": tool_list_dsl_functions,
    "list_templates": tool_list_templates,
    "get_dsl_syntax_guide": tool_get_dsl_syntax_guide,
    "create_event_definitions": tool_create_event_definitions,
    "add_transaction_types": tool_add_transaction_types,
    "generate_sample_event_data": tool_generate_sample_event_data,
    "get_event_data": tool_get_event_data,
    "validate_dsl": tool_validate_dsl,
    "create_or_replace_template": tool_create_or_replace_template,
    "dry_run_template": tool_dry_run_template,
    "delete_template": tool_delete_template,
    "clear_all_data": tool_clear_all_data,
    # Rule / step / schedule / template-assembly tools
    "list_saved_rules": tool_list_saved_rules,
    "get_saved_rule": tool_get_saved_rule,
    "create_saved_rule": tool_create_saved_rule,
    "update_saved_rule": tool_update_saved_rule,
    "delete_saved_rule": tool_delete_saved_rule,
    "add_step_to_rule": tool_add_step_to_rule,
    "update_step": tool_update_step,
    "delete_step": tool_delete_step,
    "debug_step": tool_debug_step,
    "list_saved_schedules": tool_list_saved_schedules,
    "create_saved_schedule": tool_create_saved_schedule,
    "delete_saved_schedule": tool_delete_saved_schedule,
    "debug_schedule": tool_debug_schedule,
    "verify_rule_complete": tool_verify_rule_complete,
    "attach_rules_to_template": tool_attach_rules_to_template,
    "finish": tool_finish,
}


# JSON-Schema descriptors handed to the LLM (provider-agnostic, OpenAI-style).
TOOL_SCHEMAS: list[dict] = [
    {
        "name": "list_events",
        "description": "List all currently defined events with their fields and types. Call this first to understand the data model before generating code.",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_dsl_functions",
        "description": "List available DSL functions. Optionally filter by category. Use this to discover the exact function names available before writing rules.",
        "parameters": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional category filter (e.g. 'Math', 'Date', 'Schedule')"},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "list_templates",
        "description": "List existing DSL templates (rules) saved in the workspace.",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "get_dsl_syntax_guide",
        "description": (
            "Return the binding DSL constraints + worked examples of every "
            "step shape (calc/condition/iteration/schedule). Call this BEFORE "
            "authoring a non-trivial rule, and ALWAYS after a syntax-class "
            "error (unterminated string literal, invalid syntax, EOF). Costs "
            "no DB lookups; safe to call any time."
        ),
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "create_event_definitions",
        "description": "Create one or more event definitions with their fields. Idempotent: existing names are skipped.",
        "parameters": {
            "type": "object",
            "properties": {
                "events": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "event_name": {"type": "string"},
                            "eventType": {"type": "string", "enum": ["activity", "reference"]},
                            "eventTable": {"type": "string", "enum": ["standard", "custom"]},
                            "fields": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "datatype": {"type": "string", "enum": ["string", "date", "boolean", "decimal", "integer"]},
                                    },
                                    "required": ["name", "datatype"],
                                },
                            },
                        },
                        "required": ["event_name", "fields"],
                    },
                },
            },
            "required": ["events"],
        },
    },
    {
        "name": "add_transaction_types",
        "description": "Register transaction-type names that DSL rules will emit (e.g. ECLAllowance, StageTransition).",
        "parameters": {
            "type": "object",
            "properties": {
                "transaction_types": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["transaction_types"],
        },
    },
    {
        "name": "generate_sample_event_data",
        "description": "Generate deterministic synthetic rows for an event definition. Used to seed the workspace before validating rules. Replaces existing data unless append=true.",
        "parameters": {
            "type": "object",
            "properties": {
                "event_name": {"type": "string"},
                "instrument_count": {"type": "integer", "minimum": 1, "maximum": 200},
                "instrument_ids": {"type": "array", "items": {"type": "string"}},
                "instrument_prefix": {"type": "string"},
                "posting_dates": {"type": "array", "items": {"type": "string"}, "description": "List of YYYY-MM-DD strings — one row per (instrument × posting_date)."},
                "field_hints": {"type": "object", "description": "Optional per-field hints, e.g. {\"rate\": {\"range\": [0.03, 0.08]}}"},
                "seed": {"type": "integer", "default": 42},
                "append": {"type": "boolean", "default": False},
            },
            "required": ["event_name"],
        },
    },
    {
        "name": "get_event_data",
        "description": "Return up to `limit` rows of stored event data for a given event.",
        "parameters": {
            "type": "object",
            "properties": {
                "event_name": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 5},
            },
            "required": ["event_name"],
        },
    },
    {
        "name": "validate_dsl",
        "description": "Translate DSL to Python and check for syntax/translation errors WITHOUT executing. Always call this before create_or_replace_template.",
        "parameters": {
            "type": "object",
            "properties": {
                "dsl_code": {"type": "string"},
                "event_name": {"type": "string", "description": "Optional primary event for translation context"},
            },
            "required": ["dsl_code"],
        },
    },
    {
        "name": "create_or_replace_template",
        "description": "Save a DSL template (rule) under the given name. Replaces any existing template with the same name. The template will appear in the 'User Created Templates' tab of the Templates wizard. customCode: blocks are FORBIDDEN — compose with rules and DSL functions only.",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dsl_code": {"type": "string"},
                "event_name": {"type": "string", "description": "Primary event the template attaches to"},
                "description": {"type": "string", "description": "Short human-readable description shown in the UI template card."},
            },
            "required": ["name", "dsl_code", "event_name"],
        },
    },
    {
        "name": "dry_run_template",
        "description": "Execute a template against current event data WITHOUT persisting transaction reports. Returns transaction counts, totals by type, and sample rows so you can verify correctness.",
        "parameters": {
            "type": "object",
            "properties": {
                "template_id": {"type": "string"},
                "name": {"type": "string", "description": "Alternative to template_id"},
                "posting_date": {"type": "string"},
                "effective_date": {"type": "string"},
                "sample_limit": {"type": "integer", "default": 5},
            },
        },
    },
    {
        "name": "delete_template",
        "description": "DESTRUCTIVE: delete a template by id or name. Requires confirm=true and is gated by user approval.",
        "parameters": {
            "type": "object",
            "properties": {
                "template_id": {"type": "string"},
                "confirm": {"type": "boolean"},
            },
            "required": ["template_id", "confirm"],
        },
    },
    {
        "name": "clear_all_data",
        "description": "DESTRUCTIVE: wipe all event definitions, event data, and transaction reports (templates are preserved). Requires confirm=true and user approval.",
        "parameters": {
            "type": "object",
            "properties": {"confirm": {"type": "boolean"}},
            "required": ["confirm"],
        },
    },
    {
        "name": "finish",
        "description": "Signal that the task is complete. Provide a short user-facing summary of what was accomplished.",
        "parameters": {
            "type": "object",
            "properties": {"summary": {"type": "string"}},
            "required": ["summary"],
        },
    },
]


# Append rule/step/schedule/template-assembly tool schemas. Defined separately
# to keep the literal above readable and to allow building a shared `step`
# schema once.
_STEP_SCHEMA = {
    "type": "object",
    "description": (
        "Atomic unit inside a rule. Three supported types:\n"
        "  • calc: name + source('formula'|'value'|'event_field'|'collect') + matching field "
        "(formula | value | eventField | collectType+eventField)\n"
        "  • condition: name + conditions[{condition, thenFormula, nestedConditions?, nestedElse?}] + elseFormula\n"
        "  • iteration: name + iterations[{type:'apply_each'|'apply_each_paired'|'for_each', "
        "sourceArray, secondArray?, varName?, secondVar?, expression, resultVar}]"
    ),
    "properties": {
        "name": {"type": "string"},
        "stepType": {"type": "string", "enum": ["calc", "condition", "iteration", "schedule"]},
        "source": {"type": "string", "enum": ["formula", "value", "event_field", "collect"]},
        "formula": {"type": "string"},
        "value": {"type": "string"},
        "eventField": {"type": "string"},
        "collectType": {"type": "string", "enum": ["collect_by_instrument", "collect_all", "collect_by_subinstrument"]},
        "conditions": {"type": "array", "items": {"type": "object"}},
        "elseFormula": {"type": "string"},
        "iterations": {"type": "array", "items": {"type": "object"}},
        "scheduleConfig": {"type": "object", "description": "For stepType:'schedule'. {periodType:'number'|'date_range', frequency:'D'|'M'|'Y', periodCount?:int, startDate?, endDate?, columns:[{name, formula}], contextVars?:[varname], convention?}"},
        "outputVars": {"type": "array", "items": {"type": "object"}, "description": "For stepType:'schedule'. [{name, type:'first'|'last'|'sum'|'column'|'filter', column, matchCol?, matchValue?}]"},
        "printResult": {"type": "boolean"},
        "inlineComment": {"type": "boolean"},
        "commentText": {"type": "string"},
    },
    "required": ["name", "stepType"],
}

TOOL_SCHEMAS.extend([
    {
        "name": "list_saved_rules",
        "description": "List saved rules (id, name, priority, step names). Use before creating new rules to avoid name/priority clashes.",
        "parameters": {
            "type": "object",
            "properties": {"name_filter": {"type": "string"}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_saved_rule",
        "description": "Fetch a single saved rule (full document including all steps).",
        "parameters": {
            "type": "object",
            "properties": {"rule_id": {"type": "string", "description": "id or exact name"}},
            "required": ["rule_id"],
        },
    },
    {
        "name": "create_saved_rule",
        "description": (
            "Create a new saved rule with an ordered list of steps. Each step is one calculation, "
            "condition, or iteration. The rule appears in the Rule Builder UI and can be edited/debugged "
            "by the user. PREFER THIS over create_or_replace_template when building reusable building blocks. "
            "Combine multiple rules into a template via attach_rules_to_template."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "priority": {"type": "integer", "description": "Optional. Auto-assigned if omitted; must be unique across rules+schedules."},
                "steps": {"type": "array", "items": _STEP_SCHEMA},
                "outputs": {
                    "type": "object",
                    "description": "Optional. {printResult, transactions:[{type, amount, postingDate, effectiveDate, subInstrumentId?}]}",
                },
            },
            "required": ["name", "steps"],
        },
    },
    {
        "name": "update_saved_rule",
        "description": "Patch fields on a saved rule. Supply patch={name?, priority?, steps?, outputs?, ...}. If steps is in the patch, ALL steps are replaced.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "patch": {"type": "object"},
            },
            "required": ["rule_id", "patch"],
        },
    },
    {
        "name": "delete_saved_rule",
        "description": "DESTRUCTIVE: delete a saved rule. Requires confirm=true and user approval.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "confirm": {"type": "boolean"},
            },
            "required": ["rule_id", "confirm"],
        },
    },
    {
        "name": "add_step_to_rule",
        "description": "Append (or insert at position) a single step into an existing rule. Step name must be unique within the rule.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step": _STEP_SCHEMA,
                "position": {"type": "integer", "description": "Optional 0-based insert index. Defaults to append."},
            },
            "required": ["rule_id", "step"],
        },
    },
    {
        "name": "update_step",
        "description": "Patch one step inside a rule. Identify by step_index OR step_name. patch is shallow-merged then re-validated.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
                "patch": {"type": "object"},
            },
            "required": ["rule_id", "patch"],
        },
    },
    {
        "name": "delete_step",
        "description": "Remove one step from a rule. Identify by step_index OR step_name.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
            },
            "required": ["rule_id"],
        },
    },
    {
        "name": "debug_step",
        "description": (
            "Execute a rule's DSL only up to and including the chosen step, printing the step's variable. "
            "Use this when the user asks to debug or inspect a step's value. Returns the printed value plus "
            "any other prints encountered along the way."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
                "posting_date": {"type": "string"},
                "effective_date": {"type": "string"},
            },
            "required": ["rule_id"],
        },
    },
    {
        "name": "list_saved_schedules",
        "description": "List saved schedules (id, name, priority).",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "create_saved_schedule",
        "description": (
            "Create a standalone saved schedule. Provide a `dsl_code` block that defines a period "
            "and a schedule, e.g.:\n"
            "  p = period(LoanEvent.term_months, \"M\")\n"
            "  amort = schedule(p, {\n"
            "      \"interest\": \"multiply(balance, rate)\",\n"
            "      \"principal\": \"subtract(payment, interest)\",\n"
            "      \"balance\":   \"subtract(balance, principal)\"\n"
            "  }, {\"balance\": LoanEvent.principal, \"rate\": LoanEvent.rate, \"payment\": pmt(LoanEvent.rate, LoanEvent.term_months, LoanEvent.principal)})\n"
            "The schedule then becomes available to attach to a template via attach_rules_to_template."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dsl_code": {"type": "string"},
                "priority": {"type": "integer"},
                "config": {"type": "object", "description": "Optional structured config (frequency, columns, etc.) used by the Schedule Builder UI."},
            },
            "required": ["name", "dsl_code"],
        },
    },
    {
        "name": "delete_saved_schedule",
        "description": "DESTRUCTIVE: delete a saved schedule by id. Requires confirm=true and user approval.",
        "parameters": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "confirm": {"type": "boolean"},
            },
            "required": ["schedule_id", "confirm"],
        },
    },
    {
        "name": "debug_schedule",
        "description": (
            "Execute a saved schedule's DSL in isolation and return its "
            "materialised rows. This is the equivalent of clicking the play "
            "button on a schedule card in the UI. ALWAYS run this on every "
            "schedule you create before declaring the model complete."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "schedule_id": {"type": "string"},
                "name": {"type": "string", "description": "Alternative to schedule_id"},
                "posting_date": {"type": "integer"},
                "effective_date": {"type": "integer"},
                "sample_limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
        },
    },
    {
        "name": "verify_rule_complete",
        "description": (
            "Run a comprehensive readiness check on a saved rule: every step "
            "is debug-runnable, outputs.transactions is populated with both "
            "debit and credit sides, all transaction types are registered, "
            "all referenced events have sample data, and createTransaction "
            "is NOT used inside calc formulas. Returns a checklist. You MUST "
            "call this and confirm `overall_ready: true` BEFORE calling "
            "`finish` for any rule-authoring task."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "posting_date": {"type": "integer"},
            },
            "required": ["rule_id"],
        },
    },
    {
        "name": "attach_rules_to_template",
        "description": (
            "Attach a set of saved rules (and optional schedules) to a user template, sorted by priority, "
            "and rebuild the template's combinedCode. This is how you assemble the final model that users see "
            "in the Templates tab. Create the template shell first via create_or_replace_template (with placeholder "
            "DSL), then call this to populate it with structured rules."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "template_id": {"type": "string"},
                "name": {"type": "string", "description": "Alternative to template_id"},
                "rule_ids": {"type": "array", "items": {"type": "string"}},
                "schedule_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
])


async def dispatch_tool(name: str, args: dict) -> dict:
    """Look up `name` in the registry and execute. Raises ToolError on unknown."""
    fn = TOOLS.get(name)
    if fn is None:
        raise ToolError(f"Unknown tool '{name}'. Available: {sorted(TOOLS.keys())}")
    if not isinstance(args, dict):
        args = {}
    return await fn(args)
