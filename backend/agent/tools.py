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

import json
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
    # IMPORTANT: useful_life / asset_life / depreciation_years and friends
    # MUST be matched BEFORE the generic 'term_months' / 'term' / 'tenor'
    # patterns. Without this guard the agent has been seen to generate
    # 74,180-year asset lives, which propagate into add_months() and blow
    # the date arithmetic past the proleptic Gregorian range.
    (lambda n: _name_match(n, "useful_life_months", "life_months",
                            "asset_life_months", "depreciation_months",
                            "amortization_months", "amortisation_months"),
        {"range": (12, 480)}, "integer"),
    (lambda n: _name_match(n, "useful_life", "asset_life", "life_years",
                            "depreciation_years", "amortization_years",
                            "amortisation_years"),
        {"range": (3, 40)}, "integer"),
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
    # Asset / loan life caps. Without these, a hint of (1, 100000) on
    # `useful_life_years` produces dates beyond year 9999 when fed into
    # add_months / add_years. Cap years at 100, months at 1200.
    (lambda n: _name_match(n, "useful_life_years", "asset_life", "life_years",
                            "term_years", "tenor_years",
                            "depreciation_years", "amortization_years",
                            "amortisation_years"),
        (0.0, 100.0), "asset/loan life in YEARS must be in [0, 100]"),
    (lambda n: _name_match(n, "useful_life_months", "life_months",
                            "asset_life_months", "term_months", "tenor_months",
                            "depreciation_months", "amortization_months",
                            "amortisation_months", "n_periods", "num_periods"),
        (0.0, 1200.0), "asset/loan life in MONTHS must be in [0, 1200]"),
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
    name_filter = (args.get("name") or "").strip().lower()
    out = []
    for m in metadata:
        if category_filter and (m.get("category") or "").lower() != category_filter:
            continue
        if name_filter and name_filter not in (m.get("name") or "").lower():
            continue
        entry = {
            "name": m.get("name"),
            "params": m.get("params"),
            "description": m.get("description"),
            "category": m.get("category"),
        }
        if m.get("example"):
            entry["example"] = m["example"]
        out.append(entry)
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
    reused_from: str | None = None
    if not instrument_ids and instrument_count <= 0:
        # Auto-reuse: if any prior event_data exists, pick its instrument
        # IDs so cross-event lookups work. This is the #1 cause of "all
        # lookups return null" in multi-event templates.
        candidate_doc = None
        try:
            if db is not None:
                cursor = db.event_data.find({}, {"_id": 0, "event_name": 1, "data_rows": 1})
                async for d in cursor:
                    if d.get("event_name", "").lower() == event_name.lower():
                        continue  # skip self if regenerating
                    rows = d.get("data_rows") or []
                    ids = sorted({str(r.get("instrumentid")) for r in rows if r.get("instrumentid")})
                    if ids:
                        candidate_doc = (d.get("event_name"), ids)
                        break
        except Exception:
            pass
        if candidate_doc is None:
            for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
                if d.get("event_name", "").lower() == event_name.lower():
                    continue
                rows = d.get("data_rows") or []
                ids = sorted({str(r.get("instrumentid")) for r in rows if r.get("instrumentid")})
                if ids:
                    candidate_doc = (d.get("event_name"), ids)
                    break
        if candidate_doc:
            reused_from, instrument_ids = candidate_doc
        else:
            # Sensible default: 2 instruments matches the standard-template convention
            instrument_count = 2
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
        "instrument_ids_reused_from": reused_from,
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
    # Cross-event instrument-ID overlap check. If two activity events have
    # ZERO instruments in common, every cross-event lookup will return null
    # and the rule will silently emit no transactions. This is the #1 cause
    # of "dry-run produced 0 rows" bugs in multi-event templates.
    if len(activity_with_data) >= 2:
        per_event_ids: dict[str, set[str]] = {}
        for evname in activity_with_data:
            ids: set[str] = set()
            for r in event_data_dict.get(evname) or []:
                iid = r.get("instrumentid")
                if iid is not None:
                    ids.add(str(iid))
            per_event_ids[evname] = ids
        names = list(per_event_ids.keys())
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                a, b = names[i], names[j]
                if per_event_ids[a] and per_event_ids[b] and not (per_event_ids[a] & per_event_ids[b]):
                    sanity_warnings.append(
                        f"events '{a}' and '{b}' have ZERO instrument IDs in common "
                        f"({sorted(per_event_ids[a])[:3]} vs {sorted(per_event_ids[b])[:3]}) "
                        f"— cross-event lookups will return null. Regenerate one event "
                        f"with `instrument_ids` matching the other."
                    )
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

    # Zero-transactions despite declared transactions: catches the silent
    # "rule looks correct but nothing posts" failure mode (e.g. amount field
    # references an undefined variable, condition gate filters everything out,
    # createTransaction flag is false).
    if len(txn_dicts) == 0:
        declared_total = 0
        declared_per_rule: list[str] = []
        try:
            rule_ids = list(template.get("rule_ids") or [])
            if rule_ids and db is not None:
                attached = await db.saved_rules.find(
                    {"id": {"$in": rule_ids}},
                    {"_id": 0, "name": 1, "outputs": 1},
                ).to_list(200)
                for r in attached:
                    rt = ((r.get("outputs") or {}).get("transactions") or [])
                    rt = [t for t in rt if t and t.get("type")]
                    if rt:
                        declared_total += len(rt)
                        declared_per_rule.append(f"{r.get('name')}({len(rt)})")
        except Exception:
            pass
        if declared_total > 0:
            sanity_warnings.append(
                f"Rules attached to this template declare {declared_total} "
                f"transaction(s) ({', '.join(declared_per_rule)}) in their "
                f"outputs.transactions[], but dry-run produced ZERO. Likely "
                f"causes: (a) the `amount` variable evaluates to 0/None — run "
                f"`debug_step` on the calc step that defines it; (b) a "
                f"condition gate filters out every row — check the `condition` "
                f"step's elseFormula and the rule's runConditions; (c) "
                f"`outputs.createTransaction` is false on the rule. DO NOT "
                f"call finish until this is resolved — do NOT blame a 'sync "
                f"issue' or 'registration' problem."
            )

    # Hard next-action when the rule declared transactions but emitted none.
    # `sanity_warnings` alone is too easy for the agent to ignore — surface
    # it as a structured imperative so rule #19 (NEVER STOP ON …) fires.
    next_action = None
    if len(txn_dicts) == 0 and declared_total > 0:
        next_action = (
            "ZERO_TRANSACTIONS_BUT_DECLARED: dry-run produced 0 transactions "
            "even though the rule(s) declare {n} in outputs.transactions[]. "
            "DO NOT finish and DO NOT ask the user. Required next step: "
            "(1) call `debug_step` on the calc step that holds each "
            "transaction's `amount` to see what value it evaluates to; "
            "(2) if it evaluates to 0/None, regenerate sample event data "
            "with field_hints that force the upstream inputs to non-zero "
            "values (e.g. for ECL: ensure ecl_delta > 0 between successive "
            "posting dates by varying pd/lgd/ead, or pre-seed prior_ecl < "
            "current_ecl); (3) if a condition gate is the culprit, relax "
            "or fix the gate; (4) re-run dry_run_template. Iterate until "
            "transaction_count > 0."
        ).format(n=declared_total)

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
        "next_action": next_action,
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


# ──────────────────────────────────────────────────────────────────────────
# Schedule-step deep validator
# ──────────────────────────────────────────────────────────────────────────
# Identifiers the schedule engine auto-injects into every column-expression
# scope (mirrors SCHEDULE_BUILTINS in ScheduleStepModal.js). Anything in this
# set is NOT pulled into contextVars and is NOT flagged as undefined.
_SCHEDULE_COLUMN_BUILTINS: set[str] = {
    "period_date", "period_index", "period_start", "period_number",
    "dcf", "lag", "days_in_current_period", "total_periods",
    "daily_basis", "item_name", "subinstrument_id", "s_no",
    "index", "start_date", "end_date",
}

_VALID_FREQUENCIES = {"D", "W", "M", "Q", "Y"}
_VALID_DC_CONVENTIONS = {
    "", "30/360", "Actual/360", "Actual/365", "Actual/Actual", "30E/360",
}
_VALID_PERIOD_TYPES = {"date", "number"}
_VALID_SOURCE_TYPES = {"value", "field", "formula"}
_VALID_OUTPUT_VAR_TYPES = {"first", "last", "sum", "column", "filter"}

_IDENT_FOR_CTX_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_schedule_step_shape(name: str, sc: dict, outputVars: list) -> tuple[dict, list]:
    """Deep validate a stepType='schedule' configuration. Mirrors the
    field-by-field rules of ScheduleStepModal.js so the agent cannot save a
    schedule that the visual modal would reject. Auto-derives contextVars
    from the column formulas (the modal's `autoDetectedVars` useMemo) so the
    agent never has to remember to populate it.

    Returns the (possibly amended) (scheduleConfig, outputVars) tuple.
    """
    if not isinstance(sc, dict):
        raise ToolError(f"step '{name}': scheduleConfig must be an object")

    period_type = (sc.get("periodType") or "date").strip()
    if period_type not in _VALID_PERIOD_TYPES:
        raise ToolError(
            f"step '{name}': scheduleConfig.periodType='{period_type}' is "
            f"invalid. Must be one of {sorted(_VALID_PERIOD_TYPES)}."
        )
    sc["periodType"] = period_type

    freq = (sc.get("frequency") or "M").strip().upper()
    if freq not in _VALID_FREQUENCIES:
        raise ToolError(
            f"step '{name}': scheduleConfig.frequency='{freq}' is invalid. "
            f"Must be one of {sorted(_VALID_FREQUENCIES)} (D/W/M/Q/Y)."
        )
    sc["frequency"] = freq

    convention = (sc.get("convention") or "").strip()
    if convention and convention not in _VALID_DC_CONVENTIONS:
        raise ToolError(
            f"step '{name}': scheduleConfig.convention='{convention}' is "
            f"invalid. Must be one of {sorted(_VALID_DC_CONVENTIONS - {''})}."
        )
    sc["convention"] = convention

    # Aggregate errors so the agent sees ALL schedule-config problems in one
    # turn instead of one-error-per-round-trip. Synonym aliases (eg. the
    # agent setting `startDateSource='event_field'`) and obvious source-kind
    # mismatches (eg. value="EVENTNAME.field" → coerce to field) are silently
    # rewritten before validation so the agent doesn't burn a turn on each.
    errs: list[str] = []

    _SOURCE_ALIASES = {
        "event_field": "field", "eventfield": "field",
        "event": "field", "col": "field", "column": "field",
        "expr": "formula", "expression": "formula",
        "calc": "formula", "compute": "formula",
        "literal": "value", "const": "value", "constant": "value",
    }
    _CALL_HINT_RE = re.compile(r"[A-Za-z_]\w*\s*\(")

    def _check_tri_source(prefix: str) -> None:
        """One of value / field / formula must yield a non-empty expression.
        Auto-coerces the obvious agent mistakes:
          - source='value' but the value is 'EVENTNAME.field'  -> field
          - source='value' but the value contains 'foo(...)'   -> formula
          - source aliases like 'event_field', 'expression'    -> canonical
        Errors are appended to the outer `errs` list so the agent sees
        every problem in one round-trip.
        """
        raw_src = (sc.get(f"{prefix}Source") or "value").strip()
        src = _SOURCE_ALIASES.get(raw_src.lower(), raw_src)
        if src not in _VALID_SOURCE_TYPES:
            errs.append(
                f"scheduleConfig.{prefix}Source='{raw_src}' is invalid. "
                f"Must be one of {sorted(_VALID_SOURCE_TYPES)} "
                f"(value | field | formula)."
            )
            return
        # Auto-coerce 'value' -> 'field' or 'formula' when the literal value
        # is obviously the wrong kind.
        if src == "value":
            v = sc.get(prefix)
            if isinstance(v, str):
                vs = v.strip()
                if _CALL_HINT_RE.search(vs):
                    src = "formula"
                    sc[f"{prefix}Formula"] = vs
                    sc[prefix] = None
                elif (
                    "." in vs
                    and " " not in vs
                    and re.fullmatch(r"[A-Za-z_]\w*\.[A-Za-z_]\w*", vs)
                ):
                    src = "field"
                    sc[f"{prefix}Field"] = vs
                    sc[prefix] = None
        sc[f"{prefix}Source"] = src
        if src == "value":
            v = sc.get(prefix)
            if v in (None, ""):
                errs.append(
                    f"scheduleConfig.{prefix} is required when "
                    f"{prefix}Source='value'. Set {prefix}=<literal>, "
                    f"OR switch {prefix}Source to 'field' (then set "
                    f"{prefix}Field='EVENTNAME.fieldname') or 'formula' "
                    f"(then set {prefix}Formula='<DSL expression>')."
                )
        elif src == "field":
            v = (sc.get(f"{prefix}Field") or "").strip()
            if not v:
                errs.append(
                    f"scheduleConfig.{prefix}Field is required when "
                    f"{prefix}Source='field'. Use 'EVENTNAME.fieldname'."
                )
            elif "." not in v:
                errs.append(
                    f"scheduleConfig.{prefix}Field='{v}' must be "
                    f"'EVENTNAME.fieldname'. Bare field names are not "
                    f"resolved by the schedule engine."
                )
        elif src == "formula":
            v = (sc.get(f"{prefix}Formula") or "").strip()
            if not v:
                errs.append(
                    f"scheduleConfig.{prefix}Formula is required when "
                    f"{prefix}Source='formula'."
                )
            else:
                try:
                    _enforce_dsl_guardrails(v)
                    _check_formula_expression(
                        v, where=f"step '{name}'.scheduleConfig.{prefix}Formula"
                    )
                except ToolError as e:
                    errs.append(str(e))

    if period_type == "date":
        _check_tri_source("startDate")
        _check_tri_source("endDate")
    else:  # number
        _check_tri_source("periodCount")

    cols = sc.get("columns") or []
    if not cols:
        errs.append(
            "scheduleConfig.columns is empty. A schedule must have at "
            f"least one column. Each column needs {{name, formula}}. "
            f"Built-in identifiers available inside column formulas: "
            f"{sorted(_SCHEDULE_COLUMN_BUILTINS)}."
        )
    seen_col_names: set[str] = set()
    for c in cols:
        cname = (c.get("name") or "").strip()
        formula = (c.get("formula") or "").strip()
        if not cname or not formula:
            errs.append(
                "every schedule column needs a non-empty name + formula. "
                f"Got name={cname!r}, formula={formula!r}."
            )
            continue
        if cname in seen_col_names:
            errs.append(f"duplicate schedule column name '{cname}'.")
            continue
        seen_col_names.add(cname)
        try:
            _enforce_dsl_guardrails(formula)
            _check_formula_expression(
                formula,
                where=f"step '{name}'.scheduleConfig.columns['{cname}'].formula",
            )
        except ToolError as e:
            errs.append(str(e))

    # Auto-derive contextVars: any identifier used in a column formula that
    # is NOT a built-in, NOT a DSL function name, and NOT another column
    # name must be a context variable from outer scope. The schedule engine
    # also exposes each context array as `<name>_full`, so strip that suffix
    # before resolving. Mirrors ScheduleStepModal autoDetectedVars.
    dsl_fn_names = _known_dsl_function_names()
    declared_ctx = {v for v in (sc.get("contextVars") or []) if isinstance(v, str)}
    derived_ctx: set[str] = set(declared_ctx)
    for c in cols:
        ids = _IDENT_FOR_CTX_RE.findall(c.get("formula") or "")
        for raw in ids:
            ident = raw[:-5] if raw.endswith("_full") else raw
            if not ident:
                continue
            if ident in _VALIDATOR_BUILTINS:
                continue
            if ident in _SCHEDULE_COLUMN_BUILTINS:
                continue
            if ident in dsl_fn_names:
                continue
            if ident in seen_col_names:
                continue
            if ident == name:
                # The step's own variable (sched = schedule(...)) — skip.
                continue
            if ident.isdigit() or ident in {"True", "False", "None"}:
                continue
            derived_ctx.add(ident)
    # Never pull the step's own name into contextVars
    derived_ctx.discard(name)
    sc["contextVars"] = sorted(derived_ctx)

    # outputVars: validate against the final column set.
    norm_outs: list[dict] = []
    seen_out_names: set[str] = set()
    for ov in outputVars or []:
        if not isinstance(ov, dict):
            errs.append("each outputVar must be an object")
            continue
        ov_name = (ov.get("name") or "").strip()
        ov_type = (ov.get("type") or "").strip()
        if not ov_name:
            errs.append("outputVar.name is required")
            continue
        if not ov_name.replace("_", "").isalnum() or ov_name[:1].isdigit():
            errs.append(
                f"outputVar.name='{ov_name}' must be a valid Python identifier."
            )
            continue
        if ov_name in seen_out_names:
            errs.append(f"duplicate outputVar name '{ov_name}'")
            continue
        seen_out_names.add(ov_name)
        if ov_type not in _VALID_OUTPUT_VAR_TYPES:
            errs.append(
                f"outputVar '{ov_name}'.type='{ov_type}' is invalid. "
                f"Must be one of {sorted(_VALID_OUTPUT_VAR_TYPES)}: "
                f"first / last / sum / column / filter."
            )
            continue
        col = (ov.get("column") or "").strip()
        if not col:
            errs.append(
                f"outputVar '{ov_name}' (type={ov_type}) needs a `column` "
                f"referring to one of {sorted(seen_col_names)}."
            )
            continue
        if col not in seen_col_names:
            import difflib as _dl
            sug = _dl.get_close_matches(col, sorted(seen_col_names), n=2, cutoff=0.5)
            hint = f" Did you mean: {', '.join(sug)}?" if sug else ""
            errs.append(
                f"outputVar '{ov_name}'.column='{col}' does not match any "
                f"defined schedule column. Defined columns: "
                f"{sorted(seen_col_names)}.{hint}"
            )
            continue
        entry: dict = {"name": ov_name, "type": ov_type, "column": col}
        if ov_type == "filter":
            mc = (ov.get("matchCol") or "").strip()
            mv = ov.get("matchValue")
            if mv is not None:
                mv = str(mv).strip()
            if not mc or mv in (None, ""):
                errs.append(
                    f"outputVar '{ov_name}' (type=filter) requires both "
                    f"matchCol and matchValue."
                )
                continue
            entry["matchCol"] = mc
            entry["matchValue"] = mv
        norm_outs.append(entry)

    if errs:
        raise ToolError(
            f"step '{name}': {len(errs)} schedule-config error(s):\n  - "
            + "\n  - ".join(errs)
        )

    return sc, norm_outs


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
    # HARD-BLOCK: agents (esp. weak models) sometimes try to satisfy "emit
    # transactions" by creating a calc step literally named `outputs_transactions`
    # or `transactions` with a placeholder value of 0. That step does NOTHING —
    # the Rule Builder's Transactions panel reads ONLY from the rule's
    # `outputs.transactions[]` array, never from a step. Reject the antipattern.
    _txnish_names = {
        "outputs_transactions", "outputstransactions", "output_transactions",
        "transactions", "transaction", "outputs", "output", "txns", "txn",
        "create_transaction", "createtransaction", "emit_transactions",
        "emit_transaction", "post_transactions", "post_transaction",
    }
    if name.strip().lower() in _txnish_names:
        raise ToolError(
            f"step name '{name}' is reserved/forbidden. Steps cannot emit "
            f"transactions — only the rule's `outputs.transactions[]` array "
            f"does. The Transactions panel in the Rule Builder reads ONLY "
            f"from `outputs.transactions[]`. \n"
            f"FIX: do NOT create this step. Instead call `add_transaction_to_rule` "
            f"(or pass `outputs.transactions=[...]` to create_saved_rule / "
            f"update_saved_rule) with entries shaped like:\n"
            f"  {{ \"type\": \"YourTxnType\", \"amount\": \"<calc_step_var>\", \"side\": \"debit\" }}\n"
            f"  {{ \"type\": \"YourTxnType\", \"amount\": \"<calc_step_var>\", \"side\": \"credit\" }}\n"
            f"where <calc_step_var> is the NAME of a prior calc step that holds "
            f"the computed amount. Register transaction types via "
            f"`add_transaction_types` first."
        )
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
            # HARD-BLOCK the most common hallucination: `all_instruments`.
            # The engine already runs the rule body once per instrument-row, so
            # there is no such collection to iterate over.
            if isinstance(sa, str) and sa.strip().lower() in {
                "all_instruments", "allinstruments", "all_loans", "allloans",
                "all_accounts", "allaccounts", "instruments", "loans",
            }:
                raise ToolError(
                    f"step '{name}'.iterations[{i}].sourceArray='{sa}' is NOT a "
                    f"valid variable. There is no `all_instruments` (or similar) "
                    f"collection — the engine ALREADY runs your rule body once "
                    f"per instrument-row automatically.\n"
                    f"FIX: DELETE this iteration step entirely and replace it "
                    f"with a `calc` step whose formula references the merged "
                    f"event field directly (e.g. `EVENTNAME.fieldname`). "
                    f"Transactions in `outputs.transactions[]` are also emitted "
                    f"once per row — no manual fan-out needed.\n"
                    f"Use iteration ONLY for arrays that genuinely have multiple "
                    f"values within a single row (e.g. a time-series collected "
                    f"via `collect_by_instrument`)."
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
        outputVars = step.get("outputVars") or []
        sc, outputVars = _validate_schedule_step_shape(name, sc, outputVars)
        out["scheduleConfig"] = sc
        out["outputVars"] = outputVars
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
    # Final safety net: backfill missing postingDate/effectiveDate/
    # subInstrumentId on any transaction entry, regardless of which tool
    # produced this rule. Catches paths that bypass per-tool normalisation.
    try:
        subid_default, _ = await _resolve_subid_default(rule.get("steps") or [])
        _normalise_transaction_outputs(
            rule.get("steps") or [], rule.get("outputs") or {},
            multi_subid_default=subid_default,
        )
    except Exception:
        pass
    # HARD GATE: refuse to persist a rule whose static validator reports
    # undefined-variable errors. Without this, the agent has been seen to
    # save a rule whose `outputs.transactions[].amount` references a
    # variable like `depreciation_expense` that no step ever defines, then
    # iterate on update_saved_rule (which silently re-saves the same
    # broken rule). Better to force a fix in the same turn.
    try:
        _verrs = await _validate_rule_static(rule)
    except Exception:
        _verrs = []
    _undef = [e for e in _verrs if e.get("kind") == "undefined_variable"]
    if _undef:
        bullets = "\n  - ".join(
            f"{e.get('where')}: '{e.get('name')}' is not defined. "
            f"{e.get('fix_hint')}"
            for e in _undef[:8]
        )
        more = (
            f"\n  ... and {len(_undef) - 8} more"
            if len(_undef) > 8 else ""
        )
        raise ToolError(
            f"Refusing to save rule '{rule.get('name')}': "
            f"{len(_undef)} undefined-variable error(s):\n  - {bullets}{more}\n"
            f"Fix every reference (add the missing calc step, OR change the "
            f"expression to use an existing variable / event field) and "
            f"resubmit."
        )
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


def _normalise_transaction_outputs(steps: list[dict], outputs: dict,
                                    *, multi_subid_default: str | None = None) -> dict:
    """Ensure every entry in `outputs.transactions[]` has the camelCase
    postingDate / effectiveDate / subInstrumentId fields the code generator
    requires. Without this, weak models calling create_saved_rule /
    update_saved_rule with `outputs={transactions:[{type, amount, side}]}`
    silently produce rules that emit ZERO transactions because
    `_generate_rule_code` skips entries missing those fields.

    Mutates and returns `outputs`. Defaults:
      • postingDate / effectiveDate ← '<FirstReferencedEvent>.postingdate'
        / '.effectivedate', extracted from the rule's steps.
      • subInstrumentId ← '1.0' (matches the UI default) UNLESS
        `multi_subid_default` is supplied — then that value (typically the
        row builtin `subinstrumentid` or a collected variable name) is used
        AND any existing hardcoded "1" / "1.0" entries are overridden so
        the rule actually carries the row's per-subid context. Callers
        determine multi-subid via `_detect_multi_subid_events`.
    Accepts snake_case / lowercase input keys (postingdate, posting_date,
    effective_date, etc.) and rewrites them to canonical camelCase so the
    rest of the pipeline sees a single shape.
    """
    if not outputs:
        return outputs
    txns = outputs.get("transactions") or []
    if not txns:
        return outputs

    # Resolve a default event name once
    default_event: str | None = None
    try:
        extract = _h("extract_event_names_from_dsl")
    except Exception:
        extract = None
    if extract:
        blob = "\n".join(
            str(s.get("formula") or "") + "\n" + str(s.get("value") or "")
            + "\n" + str(s.get("eventField") or "")
            for s in (steps or [])
        )
        try:
            names = list(extract(blob) or [])
            if names:
                default_event = names[0]
        except Exception:
            pass
    if not default_event:
        for s in steps or []:
            ef = (s.get("eventField") or "").strip()
            if "." in ef:
                head = ef.split(".", 1)[0].strip()
                if head:
                    default_event = head
                    break

    _ALIASES = {
        "postingdate": "postingDate", "posting_date": "postingDate",
        "effectivedate": "effectiveDate", "effective_date": "effectiveDate",
        "subinstrumentid": "subInstrumentId", "sub_instrument_id": "subInstrumentId",
    }

    fixed: list[dict] = []
    _DEFAULT_SUBIDS = {"", "1", "1.0", "0", "0.0", None}
    for t in txns:
        if not isinstance(t, dict):
            fixed.append(t)
            continue
        # Rewrite alias keys → canonical
        nt: dict = {}
        for k, v in t.items():
            nt[_ALIASES.get(k, k)] = v
        if not str(nt.get("postingDate") or "").strip() and default_event:
            nt["postingDate"] = f"{default_event}.postingdate"
        if not str(nt.get("effectiveDate") or "").strip() and default_event:
            nt["effectiveDate"] = f"{default_event}.effectivedate"
        sid_now = str(nt.get("subInstrumentId") or "").strip()
        if multi_subid_default:
            # Multi-subid event detected: ALWAYS prefer the row-level identifier
            # over the literal default "1" / "1.0", because the data carries
            # multiple subIds per instrument and a hardcoded value mis-tags
            # every transaction. Honour explicit non-default agent input.
            if sid_now in _DEFAULT_SUBIDS:
                nt["subInstrumentId"] = multi_subid_default
        elif not sid_now:
            nt["subInstrumentId"] = "1.0"
        fixed.append(nt)
    outputs["transactions"] = fixed
    if fixed and not outputs.get("createTransaction"):
        outputs["createTransaction"] = True
    return outputs


async def _detect_multi_subid_events(steps: list[dict]) -> list[str]:
    """Inspect event_data for every event referenced by the rule's steps.
    Return the names of events that have ANY instrumentid carrying more than
    one distinct subinstrumentid value across rows. These are the events
    where a hardcoded subInstrumentId='1.0' on a transaction is wrong: the
    txn would mis-tag every per-subid line in the data.
    """
    db = _ServerBridge.db
    if db is None:
        return []
    try:
        extract = _h("extract_event_names_from_dsl")
    except Exception:
        return []
    blob_parts: list[str] = []
    for s in steps or []:
        if not isinstance(s, dict):
            continue
        for k in ("formula", "value", "eventField"):
            v = s.get(k)
            if v:
                blob_parts.append(str(v))
        for c in (s.get("conditions") or []):
            if isinstance(c, dict):
                for k in ("condition", "thenFormula"):
                    v = c.get(k)
                    if v:
                        blob_parts.append(str(v))
        if s.get("elseFormula"):
            blob_parts.append(str(s["elseFormula"]))
        for it in (s.get("iterations") or []):
            if isinstance(it, dict):
                for k in ("expression", "sourceArray", "secondArray"):
                    v = it.get(k)
                    if v:
                        blob_parts.append(str(v))
        sc = s.get("scheduleConfig") or {}
        for c in (sc.get("columns") or []):
            if isinstance(c, dict) and c.get("formula"):
                blob_parts.append(str(c["formula"]))
        for k in ("startDateField", "endDateField", "periodCountField",
                  "startDateFormula", "endDateFormula", "periodCountFormula"):
            v = sc.get(k)
            if v:
                blob_parts.append(str(v))
    blob = "\n".join(blob_parts)
    try:
        names = list(extract(blob) or [])
    except Exception:
        names = []
    # Fallback: harvest event names directly from eventField prefixes and
    # any "WORD.field" pattern in the blob. The regex-based extractor in
    # server.py rejects names with leading underscores and other unusual
    # shapes, so we add a permissive scan here.
    name_set: set[str] = {str(n) for n in names if n}
    for s in steps or []:
        if not isinstance(s, dict):
            continue
        ef = s.get("eventField")
        if isinstance(ef, str) and "." in ef:
            head = ef.split(".", 1)[0].strip()
            if head:
                name_set.add(head)
    try:
        for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)\.[A-Za-z_]", blob):
            name_set.add(m.group(1))
    except Exception:
        pass
    # Drop obvious non-events (DSL function names, builtins, keywords).
    _NON_EVENT = {
        "EVT", "EVENT", "self", "math", "datetime", "date", "time",
        "true", "false", "True", "False", "None", "null",
    }
    names = [n for n in name_set if n and n not in _NON_EVENT]
    if not names:
        return []
    multi: list[str] = []
    for nm in names:
        try:
            doc = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(nm)}$", "$options": "i"}},
                {"_id": 0, "data_rows": 1},
            )
        except Exception:
            doc = None
        if not doc:
            continue
        rows = doc.get("data_rows") or []
        per_inst: dict[str, set] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            iid = (r.get("instrumentid") or r.get("instrumentID")
                    or r.get("InstrumentId") or r.get("InstrumentID") or "")
            sid = (r.get("subinstrumentid") or r.get("subInstrumentId")
                    or r.get("SubInstrumentId") or "")
            if iid is None:
                continue
            iid = str(iid)
            sid_str = str(sid).strip() if sid is not None else ""
            if not sid_str or sid_str.lower() == "none":
                sid_str = "1"
            per_inst.setdefault(iid, set()).add(sid_str)
        if any(len(v) > 1 for v in per_inst.values()):
            multi.append(nm)
    return multi


async def _resolve_subid_default(steps: list[dict]) -> tuple[str | None, list[str]]:
    """Return (multi_subid_default_value, [event_names_with_multi_subid]).
    The value is `subinstrumentid` (the row builtin) when ANY referenced event
    has multiple subIds per instrument; otherwise None (caller falls back to
    the literal '1.0')."""
    multi = await _detect_multi_subid_events(steps)
    if not multi:
        return None, []
    return "subinstrumentid", multi


def _validate_transaction_outputs(steps: list[dict], outputs: dict) -> None:
    """Pre-flight check: every `outputs.transactions[].amount` must be either
    a numeric literal or the name of a variable defined by a prior step.
    Catches the dominant `name 'amount' is not defined` dry-run failure."""
    txns = (outputs or {}).get("transactions") or []
    if not txns:
        return
    defined: set[str] = set()
    for s in steps or []:
        if isinstance(s, dict) and s.get("name"):
            defined.add(s["name"])
        for ov in (s.get("outputVars") or []):
            if isinstance(ov, dict) and ov.get("name"):
                defined.add(ov["name"])
    import difflib
    for i, t in enumerate(txns):
        if not isinstance(t, dict):
            continue
        amt_raw = t.get("amount")
        if amt_raw is None:
            continue
        amt = str(amt_raw).strip()
        if not amt:
            continue
        # numeric literal is fine
        try:
            float(amt)
            continue
        except ValueError:
            pass
        # bare identifier must resolve to a known step var
        if re.fullmatch(r"[A-Za-z_]\w*", amt):
            if amt not in defined:
                sug = difflib.get_close_matches(amt, sorted(defined), n=2, cutoff=0.5)
                hint = f" Did you mean: {', '.join(sug)}?" if sug else ""
                raise ToolError(
                    f"outputs.transactions[{i}].amount='{amt}' does not match "
                    f"any step variable name in this rule. Defined: "
                    f"{sorted(defined) or '[]'}. The `amount` field is "
                    f"evaluated as a Python expression at dry-run time, NOT a "
                    f"label — set it to the name of the calc step that holds "
                    f"the computed amount, or to a numeric literal.{hint}"
                )


# ──────────────────────────────────────────────────────────────────────────
# Static rule validator — used by tool_validate_rule and auto-attached to
# the response of every mutation tool so the agent SEES validation errors
# in the same turn it caused them.
# ──────────────────────────────────────────────────────────────────────────

# Identifiers we should NOT flag as undefined: control words, common Python
# builtins that survive translation, the always-injected globals, and DSL
# loop-locals.
_VALIDATOR_BUILTINS: set[str] = {
    "True", "False", "None", "and", "or", "not", "in", "is",
    "if", "else", "for", "while", "return", "lambda",
    "len", "str", "int", "float", "bool", "list", "dict", "tuple", "set",
    "range", "min", "max", "sum", "abs", "round", "any", "all", "print",
    "postingdate", "effectivedate", "instrumentid", "subinstrumentid",
    "each", "second", "first", "i", "idx", "index", "row",
    "createTransaction",
}

_IDENT_RE = re.compile(r"\b([A-Za-z_]\w*)\b")
_DOTTED_RE = re.compile(r"\b([A-Za-z_]\w*)\.[A-Za-z_]\w*")


def _step_defined_names(step: dict) -> set[str]:
    """Names that this step CONTRIBUTES to the variable scope."""
    out: set[str] = set()
    nm = (step.get("name") or "").strip()
    if nm:
        out.add(nm)
    if step.get("stepType") == "iteration":
        for it in step.get("iterations") or []:
            rv = (it.get("resultVar") or "").strip()
            if rv:
                out.add(rv)
    if step.get("stepType") == "schedule":
        for ov in step.get("outputVars") or []:
            ovn = (ov.get("name") or "").strip()
            if ovn:
                out.add(ovn)
    return out


def _step_referenced_names(step: dict) -> list[tuple[str, str]]:
    """Identifiers this step REFERENCES, paired with a `where` label so
    error messages can point at the offending field."""
    refs: list[tuple[str, str]] = []
    nm = step.get("name") or "?"
    st = step.get("stepType")
    if st == "calc":
        src = step.get("source") or "formula"
        if src == "formula":
            f = step.get("formula") or ""
            if f:
                refs.append((f"step '{nm}'.formula", f))
        elif src == "value":
            v = step.get("value") or ""
            if v:
                refs.append((f"step '{nm}'.value", v))
    elif st == "condition":
        for i, c in enumerate(step.get("conditions") or []):
            for k in ("condition", "thenFormula"):
                v = c.get(k) or ""
                if v:
                    refs.append((f"step '{nm}'.conditions[{i}].{k}", v))
        ef = step.get("elseFormula") or ""
        if ef:
            refs.append((f"step '{nm}'.elseFormula", ef))
    elif st == "iteration":
        for i, it in enumerate(step.get("iterations") or []):
            for k in ("sourceArray", "secondArray", "expression"):
                v = it.get(k) or ""
                if v:
                    refs.append((f"step '{nm}'.iterations[{i}].{k}", v))
    elif st == "schedule":
        sc = step.get("scheduleConfig") or {}
        for c in sc.get("columns") or []:
            f = c.get("formula") or ""
            if f:
                refs.append((f"step '{nm}'.scheduleConfig.columns['{c.get('name')}'].formula", f))
        for k in ("periodCountFormula", "startDateFormula", "endDateFormula"):
            v = sc.get(k) or ""
            if v:
                refs.append((f"step '{nm}'.scheduleConfig.{k}", v))
        # contextVars are required to have been defined before
        for cv in sc.get("contextVars") or []:
            cv = (cv or "").strip()
            if cv:
                refs.append((f"step '{nm}'.scheduleConfig.contextVars", cv))
    return refs


def _extract_identifiers(expr: str) -> set[str]:
    """Return bare identifiers used in an expression, EXCLUDING anything that
    appears as the LHS of a `.` (event-field access) and excluding tokens
    that are immediately followed by `(` (function calls — those are
    already validated by `_check_function_calls`)."""
    if not isinstance(expr, str) or not expr:
        return set()
    # Strip string literals so identifiers inside strings aren't flagged
    stripped = re.sub(r"'(?:[^'\\]|\\.)*'", "''", expr)
    stripped = re.sub(r'"(?:[^"\\]|\\.)*"', '""', stripped)
    # Track identifiers that are LHS of a dot (event-name prefix) — those
    # are validated separately against the event registry, not the step scope.
    dotted_lhs = set(m.group(1) for m in _DOTTED_RE.finditer(stripped))
    # Strip dotted attribute access entirely so the rhs isn't flagged
    no_dotted = re.sub(r"\b[A-Za-z_]\w*\.[A-Za-z_]\w*", "", stripped)
    # Strip function-call names: foo(  → leave the args, drop the name
    no_calls = re.sub(r"\b([A-Za-z_]\w*)\s*\(", "(", no_dotted)
    out = set()
    for m in _IDENT_RE.finditer(no_calls):
        tok = m.group(1)
        if tok and tok not in dotted_lhs:
            out.add(tok)
    return out


async def _validate_rule_static(rule: dict) -> list[dict]:
    """Walk the rule's steps in order, building the variable scope and
    flagging any reference to an identifier that isn't defined yet, isn't
    a global, isn't a known DSL function, and isn't an event-table prefix.

    Returns a list of {step, kind, name, where, fix_hint} error dicts.
    Empty list = clean.
    """
    steps = rule.get("steps") or []
    # Build event-name lookup (case-insensitive). Also collect a
    # field -> [event names] map so an undefined identifier that happens
    # to match a known event field can be suggested as 'EVENTNAME.field'.
    db = _ServerBridge.db
    event_names: set[str] = set()
    event_field_index: dict[str, list[str]] = {}

    def _index_event(ev: dict) -> None:
        nm = ev.get("event_name")
        if not nm:
            return
        event_names.add(str(nm).lower())
        for f in (ev.get("fields") or []):
            fn = (f.get("name") or "").strip() if isinstance(f, dict) else ""
            if fn:
                event_field_index.setdefault(fn.lower(), []).append(str(nm))

    try:
        if db is not None:
            async for d in db.event_definitions.find({}, {"_id": 0, "event_name": 1, "fields": 1}):
                _index_event(d)
    except Exception:
        pass
    for e in (_ServerBridge.in_memory_data or {}).get("event_definitions") or []:
        _index_event(e)
    # The translation layer also accepts `EVENTNAME_field` flattened form
    flat_event_names = {nm.replace(" ", "_") for nm in event_names}

    known_fns = _known_dsl_function_names()

    scope: set[str] = set()
    errors: list[dict] = []

    for step in steps:
        # Validate references BEFORE adding this step's defined names
        # (a step cannot reference its own name on the RHS).
        for where, expr in _step_referenced_names(step):
            # Schedule column formulas have an extra layer of injected
            # builtins (period_index, period_date, etc.) — exempt those
            # from the undefined-variable check; otherwise the validator
            # double-flags identifiers that the schedule engine actually
            # provides at runtime.
            in_schedule_col = ".scheduleConfig.columns[" in where
            idents = _extract_identifiers(expr)
            for tok in sorted(idents):
                if tok in scope:
                    continue
                if tok in _VALIDATOR_BUILTINS:
                    continue
                if tok in known_fns:
                    continue
                if in_schedule_col and tok in _SCHEDULE_COLUMN_BUILTINS:
                    continue
                if tok.lower() in event_names or tok.lower() in flat_event_names:
                    continue
                # Numeric literal? extract_identifiers already excludes those
                # (Python identifiers don't start with a digit).
                # Otherwise it's an undefined reference — flag it.
                import difflib as _dl
                sug = _dl.get_close_matches(tok, sorted(scope), n=2, cutoff=0.6)
                fix = (
                    f"Add a step named '{tok}' BEFORE step '{step.get('name')}', "
                    f"OR change this expression to use one of the variables "
                    f"already defined: {sorted(scope) or '[]'}."
                )
                if sug:
                    fix += f" Did you mean: {', '.join(sug)}?"
                # Suggest event-prefix form when the bare identifier matches
                # a known event field.
                ev_hits = event_field_index.get(tok.lower()) or []
                if ev_hits:
                    fix += (
                        f" Or reference the event field directly: "
                        f"{', '.join(f'{e}.{tok}' for e in ev_hits[:3])}."
                    )
                # Suggest schedule builtin when close to one.
                if in_schedule_col:
                    sb_sug = _dl.get_close_matches(
                        tok, sorted(_SCHEDULE_COLUMN_BUILTINS), n=2, cutoff=0.6
                    )
                    if sb_sug:
                        fix += (
                            f" Or use a schedule built-in: "
                            f"{', '.join(sb_sug)}."
                        )
                errors.append({
                    "step": step.get("name"),
                    "kind": "undefined_variable",
                    "name": tok,
                    "where": where,
                    "fix_hint": fix,
                })
        # Now add this step's contributions to scope for subsequent steps
        scope |= _step_defined_names(step)

    # Validate outputs.transactions[].amount references
    outputs = rule.get("outputs") or {}
    for i, t in enumerate(outputs.get("transactions") or []):
        if not isinstance(t, dict):
            continue
        amt = (t.get("amount") or "").strip()
        if not amt:
            continue
        try:
            float(amt)
            continue
        except ValueError:
            pass
        # Check identifiers in the amount expression
        for tok in sorted(_extract_identifiers(amt)):
            if tok in scope or tok in _VALIDATOR_BUILTINS or tok in known_fns:
                continue
            if tok.lower() in event_names or tok.lower() in flat_event_names:
                continue
            import difflib as _dl
            sug = _dl.get_close_matches(tok, sorted(scope), n=2, cutoff=0.6)
            fix = (
                f"`amount` must reference a step variable or numeric literal. "
                f"Add a calc step named '{tok}' that computes the amount, OR "
                f"change `amount` to one of the existing variables: "
                f"{sorted(scope) or '[]'}."
            )
            if sug:
                fix += f" Did you mean: {', '.join(sug)}?"
            errors.append({
                "step": "outputs.transactions",
                "kind": "undefined_variable",
                "name": tok,
                "where": f"outputs.transactions[{i}].amount",
                "fix_hint": fix,
            })
    return errors


async def tool_validate_rule(args: dict) -> dict:
    """Static-analyse a saved rule for the most common authoring mistakes:
    undefined variable references, transaction amount fields that point at
    nonexistent steps, and missing event prefixes. Returns ok + errors[]."""
    rule = await _load_rule((args.get("rule_id") or "").strip())
    errors = await _validate_rule_static(rule)
    return {
        "rule_id": rule.get("id"),
        "rule_name": rule.get("name"),
        "ok": not errors,
        "error_count": len(errors),
        "errors": errors,
        "next_action": (
            "Rule passes static validation. Proceed to debug_step / "
            "verify_rule_complete."
            if not errors else
            "Fix the listed errors via update_step / add_step_to_rule / "
            "delete_step BEFORE calling finish."
        ),
    }


async def _attach_validation(rule: dict, payload: dict) -> dict:
    """Run the static validator on `rule` and merge any errors into `payload`
    so the agent observes them in the same turn it made the mutation. Used by
    create_saved_rule / update_saved_rule / add_step_to_rule / update_step /
    delete_step / add_transaction_to_rule."""
    try:
        errs = await _validate_rule_static(rule)
    except Exception as exc:
        logger.warning("Rule static validation failed unexpectedly: %s", exc)
        return payload
    if errs:
        payload["validation"] = {
            "ok": False,
            "error_count": len(errs),
            "errors": errs[:20],
            "fix_now": (
                "⚠️ The rule was saved but has static-validation errors that "
                "will fail at dry-run. You MUST fix these BEFORE calling "
                "finish. Use update_step / add_step_to_rule / delete_step. "
                "DO NOT call finish or attach_rules_to_template until "
                "validate_rule returns ok=true."
            ),
        }
    else:
        payload["validation"] = {"ok": True, "error_count": 0}
    return payload


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
        raise ToolError(
            f"A rule named '{name}' already exists (id={existing.get('id')}). "
            f"DO NOT create a duplicate. Either:\n"
            f"  • Call `update_saved_rule(rule_id='{existing.get('id')}', "
            f"patch={{...}})` to fix it in place, OR\n"
            f"  • Call `delete_saved_rule(rule_id='{existing.get('id')}', "
            f"confirm=true)` first (with user approval) and recreate under "
            f"the SAME name '{name}'.\n"
            f"NEVER append _v2/_final/_fixed/_auto suffixes — see system "
            f"rule #18."
        )
    # Fuzzy duplicate guard: reject obvious near-duplicates of an existing
    # rule (suffixed names, near-identical step shapes). Forces the agent to
    # edit the existing rule instead of cluttering the workspace.
    try:
        all_rules = await db.saved_rules.find(
            {}, {"_id": 0, "id": 1, "name": 1, "steps": 1}
        ).to_list(2000)
    except Exception:
        all_rules = []
    if all_rules:
        import difflib as _dl

        def _strip_suffix(s: str) -> str:
            return re.sub(
                r"[_\-\s](?:v\d+|final|fixed|auto|new|copy|temp|tmp|attempt\d*)\b",
                "",
                s,
                flags=re.IGNORECASE,
            ).strip()

        target_stem = _strip_suffix(name).lower()
        steps_in = args.get("steps") or []
        target_sig = sorted(
            (s.get("stepType") or "calc", (s.get("name") or "").lower())
            for s in steps_in if isinstance(s, dict)
        )
        for r in all_rules:
            other = (r.get("name") or "").strip()
            if not other:
                continue
            other_stem = _strip_suffix(other).lower()
            # Stem collision (e.g. "ECL_v2" vs existing "ECL")
            if target_stem and other_stem and target_stem == other_stem:
                raise ToolError(
                    f"'{name}' is a near-duplicate of existing rule '{other}' "
                    f"(id={r.get('id')}) — same root name with a suffix. "
                    f"DO NOT create a parallel rule. Use update_saved_rule on "
                    f"the existing one (or delete it first and recreate under "
                    f"the original name). System rule #18: never append "
                    f"_v2/_final/_fixed/_auto."
                )
            # Strong fuzzy similarity on the cleaned name
            if (
                target_stem
                and other_stem
                and _dl.SequenceMatcher(None, target_stem, other_stem).ratio() >= 0.88
            ):
                raise ToolError(
                    f"'{name}' is very similar to existing rule '{other}' "
                    f"(id={r.get('id')}). If you intend to modify '{other}', "
                    f"call update_saved_rule. If you intend to replace it, "
                    f"call delete_saved_rule first then recreate under the "
                    f"original name."
                )
            # Identical step-shape signature (same names + types in same set)
            other_sig = sorted(
                (s.get("stepType") or "calc", (s.get("name") or "").lower())
                for s in (r.get("steps") or []) if isinstance(s, dict)
            )
            if target_sig and target_sig == other_sig:
                raise ToolError(
                    f"'{name}' has the same step shape as existing rule "
                    f"'{other}' (id={r.get('id')}) — identical step names + "
                    f"types. This is almost certainly a duplicate. Use "
                    f"update_saved_rule on '{other}' instead."
                )
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
    outputs = args.get("outputs") or {"printResult": True, "createTransaction": False, "transactions": []}
    subid_default, multi_evts = await _resolve_subid_default(steps)
    _normalise_transaction_outputs(steps, outputs, multi_subid_default=subid_default)
    _validate_transaction_outputs(steps, outputs)
    rule = {
        "name": name,
        "priority": priority,
        "steps": steps,
        "outputs": outputs,
        "inlineComment": False,
        "commentText": "",
    }
    rule = await _save_rule_doc(rule, is_new=True)
    payload = {"rule_id": rule["id"], "name": name, "priority": priority, "step_count": len(steps)}
    if multi_evts:
        payload["multi_subid_events"] = multi_evts
        payload["multi_subid_hint"] = (
            f"Detected multiple subInstrumentIds per instrument in event(s) "
            f"{multi_evts}. The transaction subInstrumentId default has been "
            f"set to the row builtin `subinstrumentid` instead of '1.0'. If "
            f"you need to fan-out across ALL of an instrument's subIds, add "
            f"a calc step `sub_ids = collect_by_instrument(\"{multi_evts[0]}.subinstrumentid\")` "
            f"and reference `sub_ids` from your transactions."
        )
    sched_results = await _auto_test_schedule_steps(rule)
    if sched_results:
        payload["schedule_tests"] = sched_results
        if any(not r.get("ok") for r in sched_results):
            payload["schedule_tests_hint"] = (
                "⚠️ one or more schedule steps failed their automatic "
                "preview test. Fix the failing column/output via update_step "
                "or call test_schedule_step directly to iterate. The rule "
                "is saved but is NOT runnable yet."
            )
    return await _attach_validation(rule, payload)


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
    subid_default, multi_evts = await _resolve_subid_default(rule.get("steps") or [])
    _normalise_transaction_outputs(
        rule.get("steps") or [], rule.get("outputs") or {},
        multi_subid_default=subid_default,
    )
    _validate_transaction_outputs(rule.get("steps") or [], rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {"rule_id": rule["id"], "name": rule["name"], "step_count": len(rule.get("steps") or [])}
    if multi_evts:
        payload["multi_subid_events"] = multi_evts
        payload["multi_subid_hint"] = (
            f"Detected multiple subInstrumentIds per instrument in event(s) "
            f"{multi_evts}. Transaction subInstrumentId default is the row "
            f"builtin `subinstrumentid`. To fan-out across ALL subIds, add "
            f"a calc step `sub_ids = collect_by_instrument(\"{multi_evts[0]}.subinstrumentid\")` "
            f"and reference `sub_ids` from transactions."
        )
    sched_results = await _auto_test_schedule_steps(rule)
    if sched_results:
        payload["schedule_tests"] = sched_results
        if any(not r.get("ok") for r in sched_results):
            payload["schedule_tests_hint"] = (
                "⚠️ one or more schedule steps failed their automatic "
                "preview test. Fix the failing column/output via update_step "
                "or call test_schedule_step directly to iterate. The rule "
                "is saved but is NOT runnable yet."
            )
    return await _attach_validation(rule, payload)


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
    _validate_transaction_outputs(steps, rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {"rule_id": rule["id"], "step_name": step["name"], "step_count": len(steps)}
    return await _attach_validation(rule, payload)


async def tool_update_step(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    patch = args.get("patch") or {}
    # Auto-wrap: weaker models (gpt-4.1-mini) reliably forget the nested
    # `patch` envelope and emit step fields at the top level. Accept both.
    if not isinstance(patch, dict) or not patch:
        _STEP_FIELDS = {
            "name", "stepType", "source", "formula", "value", "eventField",
            "collectType", "conditions", "elseFormula", "iterations",
            "scheduleConfig", "outputVars", "inlineComment", "commentText",
            "printResult",
        }
        flat = {k: v for k, v in (args or {}).items() if k in _STEP_FIELDS}
        if flat:
            patch = flat
        else:
            raise ToolError(
                "patch must be a non-empty object. Pass step fields either "
                "wrapped as `patch={...}` OR at the top level alongside "
                "rule_id/step_name (e.g. {rule_id, step_name, formula})."
            )
    merged = {**rule["steps"][idx], **patch}
    merged = _validate_step_shape(merged)
    rule["steps"][idx] = merged
    _validate_transaction_outputs(rule["steps"], rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {"rule_id": rule["id"], "step_index": idx, "step_name": merged["name"]}
    return await _attach_validation(rule, payload)


async def tool_delete_step(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    removed = rule["steps"].pop(idx)
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {"rule_id": rule["id"], "deleted_step": removed.get("name"), "step_count": len(rule["steps"])}
    return await _attach_validation(rule, payload)


async def tool_add_transaction_to_rule(args: dict) -> dict:
    """Append one entry to the rule's `outputs.transactions[]` array. This is
    the ONLY supported way to make a rule emit a transaction — steps cannot
    emit transactions on their own. Always call this in PAIRS (one debit + one
    credit) so the entry is balanced.

    Args:
      rule_id  – id of the saved rule
      type     – the transaction type name (must be registered first via
                 `add_transaction_types`)
      amount   – name of a prior calc-step variable (or a numeric literal)
                 holding the amount
      side     – "debit" or "credit"
    """
    rule = await _load_rule((args.get("rule_id") or "").strip())
    txn_type = (args.get("type") or "").strip()
    amount = (args.get("amount") or "").strip()
    side = (args.get("side") or "").strip().lower()
    if not txn_type:
        raise ToolError("`type` is required (the transaction type name)")
    if not amount:
        raise ToolError("`amount` is required — pass the NAME of a prior "
                        "calc-step variable, or a numeric literal")
    if side not in ("debit", "credit"):
        raise ToolError("`side` must be 'debit' or 'credit'")
    # Ensure the transaction type is registered. Use the SAME collection +
    # field that `add_transaction_types` writes: db.transaction_definitions,
    # field `transactiontype`. (Earlier this looked at db.transaction_types /
    # `name`, which never matched and produced an unbreakable loop.) If the
    # type is missing, AUTO-REGISTER it here rather than erroring — the
    # registry is freeform and forcing the agent to call a separate tool only
    # creates failure loops when lookup misses.
    db = _ServerBridge.db
    auto_registered = False
    registered = False
    if db is not None:
        try:
            reg = await db.transaction_definitions.find_one(
                {"transactiontype": {"$regex": f"^{re.escape(txn_type)}$", "$options": "i"}},
                {"_id": 0, "transactiontype": 1},
            )
            if reg:
                registered = True
            else:
                await db.transaction_definitions.insert_one({"transactiontype": txn_type})
                auto_registered = True
                registered = True
        except Exception as exc:
            logger.warning("DB lookup/insert txn type failed: %s", exc)
    if not registered:
        mem = _ServerBridge.in_memory_data.setdefault("transaction_definitions", [])
        if not any(
            (d.get("transactiontype") or "").lower() == txn_type.lower()
            for d in mem
        ):
            mem.append({"transactiontype": txn_type})
            auto_registered = True
    outputs = dict(rule.get("outputs") or {})
    txns = list(outputs.get("transactions") or [])

    # Posting/effective date and sub-instrument id are REQUIRED by the code
    # generator (`_generate_rule_code` skips any txn missing postingDate or
    # effectiveDate, which silently drops the transaction at runtime). Default
    # them from any event referenced in the rule's steps so that the agent
    # rarely needs to think about them, but allow explicit override.
    def _first_event_name() -> str | None:
        try:
            extract = _h("extract_event_names_from_dsl")
        except Exception:
            extract = None
        if extract:
            blob = "\n".join(
                str(s.get("formula") or "") + "\n" + str(s.get("value") or "")
                + "\n" + str(s.get("eventField") or "")
                for s in (rule.get("steps") or [])
            )
            try:
                names = list(extract(blob) or [])
                if names:
                    return names[0]
            except Exception:
                pass
        # Fall back: pluck the leading identifier from the first eventField that
        # looks like `EVENT.field`.
        for s in rule.get("steps") or []:
            ef = (s.get("eventField") or "").strip()
            if "." in ef:
                head = ef.split(".", 1)[0].strip()
                if head:
                    return head
        return None

    def _arg(*keys: str) -> str:
        for k in keys:
            v = args.get(k)
            if v is None:
                continue
            sv = str(v).strip()
            if sv:
                return sv
        return ""

    posting_date = _arg("postingdate", "posting_date", "postingDate")
    effective_date = _arg("effectivedate", "effective_date", "effectiveDate")
    sub_inst = _arg("subinstrumentid", "sub_instrument_id", "subInstrumentId")

    if not posting_date or not effective_date:
        evt = _first_event_name()
        if evt:
            if not posting_date:
                posting_date = f"{evt}.postingdate"
            if not effective_date:
                effective_date = f"{evt}.effectivedate"
    if not sub_inst:
        # Multi-subid detection: if any event referenced by this rule has
        # multiple subInstrumentIds per instrument in its data, the row
        # builtin `subinstrumentid` is far safer than the hardcoded '1.0'.
        try:
            multi_default, _ = await _resolve_subid_default(rule.get("steps") or [])
        except Exception:
            multi_default = None
        sub_inst = multi_default or "1.0"

    if not posting_date or not effective_date:
        raise ToolError(
            "Could not determine postingdate/effectivedate for the transaction. "
            "Either reference an event in a prior step (so 'EVENT.postingdate' "
            "can be inferred), or pass the `postingdate` and `effectivedate` "
            "arguments explicitly (e.g. 'EOD.postingdate', 'EOD.effectivedate')."
        )

    txn_doc = {
        "type": txn_type,
        "amount": amount,
        "side": side,
        "postingDate": posting_date,
        "effectiveDate": effective_date,
        "subInstrumentId": sub_inst,
    }
    txns.append(txn_doc)
    outputs["transactions"] = txns
    outputs["createTransaction"] = True
    rule["outputs"] = outputs
    # _validate_transaction_outputs will reject `amount` if it doesn't resolve
    # to a known step variable or numeric literal.
    _validate_transaction_outputs(rule.get("steps") or [], outputs)
    rule = await _save_rule_doc(rule, is_new=False)
    sides = [(t.get("side") or "").lower() for t in txns]
    balanced = any(s == "debit" for s in sides) and any(s == "credit" for s in sides)
    # Round-trip verification: re-load the rule from storage and confirm the
    # transaction landed exactly as expected. Catches silent persistence
    # failures and gives the agent positive proof of success.
    try:
        reloaded = await _load_rule(rule["id"])
        persisted = ((reloaded.get("outputs") or {}).get("transactions") or [])
        if not any(
            (t.get("type") == txn_type and (t.get("side") or "").lower() == side
             and str(t.get("amount") or "").strip() == amount
             and str(t.get("postingDate") or "").strip() == posting_date
             and str(t.get("effectiveDate") or "").strip() == effective_date)
            for t in persisted
        ):
            raise ToolError(
                f"Round-trip check failed: transaction "
                f"({side} {txn_type} amount={amount} postingDate={posting_date} "
                f"effectiveDate={effective_date}) was not found in the "
                f"reloaded rule. The save did not persist correctly."
            )
    except ToolError:
        raise
    except Exception as exc:
        logger.warning("Transaction round-trip verification skipped: %s", exc)
    payload = {
        "rule_id": rule["id"],
        "transaction_count": len(txns),
        "balanced": balanced,
        "auto_registered_type": txn_type if auto_registered else None,
        "next_step_hint": (
            f"Added {side} '{txn_type}' for amount={amount}"
            + (" (auto-registered the transaction type)." if auto_registered else ".")
            + " "
            + ("Pair is balanced." if balanced else
               f"NOT balanced — add the matching {'credit' if side == 'debit' else 'debit'} entry next.")
        ),
    }
    return await _attach_validation(rule, payload)


def _resolve_txn_index(txns: list, args: dict) -> int:
    """Locate one transaction inside outputs.transactions[] by index, by type
    (+ optional side), or by an exact-match dict on `match`. Raises ToolError
    with a helpful listing if 0 or >1 candidates are found."""
    if not txns:
        raise ToolError("Rule has no transactions to operate on.")
    if "transaction_index" in args and args["transaction_index"] is not None:
        idx = int(args["transaction_index"])
        if idx < 0 or idx >= len(txns):
            raise ToolError(
                f"transaction_index {idx} out of range (0..{len(txns)-1}). "
                f"Current entries: " + ", ".join(
                    f"[{i}] {t.get('side')} {t.get('type')}" for i, t in enumerate(txns)
                )
            )
        return idx
    txn_type = (args.get("type") or "").strip()
    side = (args.get("side") or "").strip().lower()
    if not txn_type and not args.get("match"):
        raise ToolError(
            "Identify the transaction by `transaction_index`, by `type` "
            "(+ optional `side`), or by an exact-match `match` dict."
        )
    candidates: list[int] = []
    match = args.get("match") or {}
    for i, t in enumerate(txns):
        if txn_type and t.get("type") != txn_type:
            continue
        if side and (t.get("side") or "").lower() != side:
            continue
        if match and not all(t.get(k) == v for k, v in match.items()):
            continue
        candidates.append(i)
    if not candidates:
        raise ToolError(
            f"No transaction matched (type={txn_type or '*'}, side={side or '*'}). "
            f"Current entries: " + ", ".join(
                f"[{i}] {t.get('side')} {t.get('type')} amount={t.get('amount')}"
                for i, t in enumerate(txns)
            )
        )
    if len(candidates) > 1:
        raise ToolError(
            f"{len(candidates)} transactions matched (type={txn_type or '*'}, "
            f"side={side or '*'}). Disambiguate by passing `transaction_index` "
            f"or a more specific `match` dict. Matches: " + ", ".join(
                f"[{i}] {txns[i].get('side')} {txns[i].get('type')} "
                f"amount={txns[i].get('amount')}" for i in candidates
            )
        )
    return candidates[0]


async def tool_delete_transaction_from_rule(args: dict) -> dict:
    """Remove one entry from `outputs.transactions[]`. Identify by
    `transaction_index`, by `type` (+ optional `side`), or by an exact-match
    `match` dict (e.g. {"type": "X", "side": "debit", "amount": "y"}).
    Pass `delete_all=true` to clear the entire transactions array."""
    rule = await _load_rule((args.get("rule_id") or "").strip())
    outputs = dict(rule.get("outputs") or {})
    txns = list(outputs.get("transactions") or [])
    if bool(args.get("delete_all")):
        removed_count = len(txns)
        outputs["transactions"] = []
        outputs["createTransaction"] = False
        rule["outputs"] = outputs
        rule = await _save_rule_doc(rule, is_new=False)
        # Round-trip verification
        reloaded = await _load_rule(rule["id"])
        remaining = ((reloaded.get("outputs") or {}).get("transactions") or [])
        if remaining:
            raise ToolError(
                f"Round-trip check failed: delete_all left {len(remaining)} "
                f"transaction(s) behind. The save did not persist correctly."
            )
        payload = {
            "rule_id": rule["id"],
            "deleted_count": removed_count,
            "transaction_count": 0,
            "next_step_hint": f"Cleared all {removed_count} transaction(s).",
        }
        return await _attach_validation(rule, payload)

    idx = _resolve_txn_index(txns, args)
    removed = txns.pop(idx)
    outputs["transactions"] = txns
    outputs["createTransaction"] = len(txns) > 0
    rule["outputs"] = outputs
    rule = await _save_rule_doc(rule, is_new=False)
    # Round-trip verification
    reloaded = await _load_rule(rule["id"])
    persisted = ((reloaded.get("outputs") or {}).get("transactions") or [])
    if len(persisted) != len(txns):
        raise ToolError(
            f"Round-trip check failed: expected {len(txns)} transaction(s) "
            f"after delete, found {len(persisted)}. Save did not persist."
        )
    sides = [(t.get("side") or "").lower() for t in txns]
    balanced = (not txns) or (
        any(s == "debit" for s in sides) and any(s == "credit" for s in sides)
    )
    payload = {
        "rule_id": rule["id"],
        "deleted_index": idx,
        "deleted": {
            "type": removed.get("type"),
            "side": removed.get("side"),
            "amount": removed.get("amount"),
        },
        "transaction_count": len(txns),
        "balanced": balanced,
        "next_step_hint": (
            f"Removed [{idx}] {removed.get('side')} {removed.get('type')}. "
            + (f"{len(txns)} transaction(s) remain." if txns else "Rule now emits no transactions.")
            + ("" if balanced else " WARNING: remaining entries are NOT balanced (debit/credit pair broken).")
        ),
    }
    return await _attach_validation(rule, payload)


async def tool_update_transaction_in_rule(args: dict) -> dict:
    """Patch one entry in `outputs.transactions[]`. Identify by
    `transaction_index`, by `type` (+ optional `side`), or by `match` dict.
    `patch` may set any of: type, amount, side, postingdate / postingDate,
    effectivedate / effectiveDate, subinstrumentid / subInstrumentId."""
    rule = await _load_rule((args.get("rule_id") or "").strip())
    outputs = dict(rule.get("outputs") or {})
    txns = list(outputs.get("transactions") or [])
    idx = _resolve_txn_index(txns, args)
    patch = args.get("patch") or {}
    if not isinstance(patch, dict) or not patch:
        # Auto-wrap top-level fields like update_step does
        _TXN_FIELDS_LC = {
            "type", "amount", "side",
            "postingdate", "posting_date", "postingDate",
            "effectivedate", "effective_date", "effectiveDate",
            "subinstrumentid", "sub_instrument_id", "subInstrumentId",
        }
        flat = {k: v for k, v in (args or {}).items() if k in _TXN_FIELDS_LC}
        if not flat:
            raise ToolError(
                "patch must be a non-empty object. Pass transaction fields "
                "either wrapped as `patch={...}` or at the top level."
            )
        patch = flat

    _CAMEL = {
        "postingdate": "postingDate", "posting_date": "postingDate",
        "effectivedate": "effectiveDate", "effective_date": "effectiveDate",
        "subinstrumentid": "subInstrumentId", "sub_instrument_id": "subInstrumentId",
    }
    normalised: dict = {}
    for k, v in patch.items():
        normalised[_CAMEL.get(k, k)] = v
    side_in = normalised.get("side")
    if side_in is not None:
        side_in = str(side_in).strip().lower()
        if side_in not in ("debit", "credit"):
            raise ToolError("`side` must be 'debit' or 'credit'")
        normalised["side"] = side_in

    merged = {**txns[idx], **normalised}
    if not merged.get("type"):
        raise ToolError("`type` cannot be cleared.")
    if not merged.get("amount"):
        raise ToolError("`amount` cannot be cleared.")
    if not merged.get("side") or merged["side"].lower() not in ("debit", "credit"):
        raise ToolError("`side` must be 'debit' or 'credit'.")
    if not merged.get("postingDate") or not merged.get("effectiveDate"):
        raise ToolError("`postingDate` and `effectiveDate` are required.")
    txns[idx] = merged
    outputs["transactions"] = txns
    outputs["createTransaction"] = True
    rule["outputs"] = outputs
    _validate_transaction_outputs(rule.get("steps") or [], outputs)
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {
        "rule_id": rule["id"],
        "updated_index": idx,
        "transaction": {
            "type": merged.get("type"),
            "side": merged.get("side"),
            "amount": merged.get("amount"),
            "postingDate": merged.get("postingDate"),
            "effectiveDate": merged.get("effectiveDate"),
            "subInstrumentId": merged.get("subInstrumentId"),
        },
        "next_step_hint": f"Updated [{idx}] {merged.get('side')} {merged.get('type')}.",
    }
    return await _attach_validation(rule, payload)


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


async def _execute_dsl_for_rule(rule: dict, code: str, posting_date: str | None,
                                effective_date: str | None) -> tuple[dict, int]:
    """Resolve event data for a rule, translate DSL → Python, execute. Helper
    for schedule-step testing. Returns (execution_result, row_count)."""
    dsl_to_python_multi_event = _h("dsl_to_python_multi_event")
    execute_python_template = _h("execute_python_template")
    merge_event_data_by_instrument = _h("merge_event_data_by_instrument")
    filter_event_data_by_posting_date = _h("filter_event_data_by_posting_date")
    extract_event_names_from_dsl = _h("extract_event_names_from_dsl")
    referenced = list(extract_event_names_from_dsl(code) or [])
    all_event_fields: dict[str, Any] = {}
    event_data: dict[str, list[dict]] = {}
    for nm in referenced:
        evt = await _find_event_def(nm)
        if not evt:
            raise ToolError(f"Event '{nm}' referenced by schedule not found")
        all_event_fields[nm] = {"fields": evt.get("fields", []),
                                 "eventType": evt.get("eventType", "activity")}
        rows: list = []
        db = _ServerBridge.db
        if db is not None:
            doc = await db.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(nm)}$", "$options": "i"}},
                {"_id": 0},
            )
            if doc:
                rows = doc.get("data_rows") or []
        if not rows:
            for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
                if str(d.get("event_name", "")).lower() == nm.lower():
                    rows = d.get("data_rows") or []
                    break
        event_data[nm] = rows
    activity_data = {k: v for k, v in event_data.items()
                     if all_event_fields[k]["eventType"] == "activity"}
    scoped = (filter_event_data_by_posting_date(activity_data, posting_date)
              if posting_date else activity_data)
    merged = merge_event_data_by_instrument(scoped) if activity_data else [{}]
    if not merged:
        merged = [{}]
    try:
        py = (dsl_to_python_multi_event(code, all_event_fields) if all_event_fields
              else _h("dsl_to_python_standalone")(code))
    except Exception as exc:
        raise ToolError(f"DSL translation failed: {exc}") from exc
    try:
        result = await execute_python_template(py, merged, event_data,
                                                posting_date, effective_date)
    except Exception as exc:
        raise ToolError(f"Execution failed: {exc}") from exc
    return result, len(merged)


async def _resolve_default_posting_date() -> str | None:
    """Pick the first posting date from the event_data collection. Mirrors the
    modal's fallback when the user hasn't selected one."""
    db = _ServerBridge.db
    if db is None:
        return None
    try:
        cursor = db.event_data.find({}, {"_id": 0, "data_rows": {"$slice": 1}})
        async for doc in cursor:
            rows = doc.get("data_rows") or []
            for r in rows:
                pd = r.get("postingdate") or r.get("postingDate")
                if pd:
                    return str(pd)[:10]
    except Exception:
        return None
    return None


async def tool_test_schedule_step(args: dict) -> dict:
    """Execute a stepType='schedule' step the same way the visual
    ScheduleStepModal does: build period(...) + schedule(...) DSL, run it
    incrementally column-by-column, then test each outputVar. Returns
    per-column pass/fail and the materialised schedule preview.

    Args:
        rule_id (str)         — required, parent rule id
        step_index (int)      — or step_name; locate the schedule step
        step_name (str)
        posting_date (str)    — optional; defaults to first available
        effective_date (str)  — optional
        sample_limit (int)    — max preview rows to return (default 6)
    """
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    steps = rule.get("steps") or []
    target = steps[idx]
    if (target.get("stepType") or "") != "schedule":
        raise ToolError(
            f"Step '{target.get('name')}' (index {idx}) is stepType="
            f"'{target.get('stepType')}', not 'schedule'. Use debug_step "
            f"for non-schedule steps."
        )
    sc = target.get("scheduleConfig") or {}
    cols = [c for c in (sc.get("columns") or []) if c.get("name") and c.get("formula")]
    if not cols:
        raise ToolError("Schedule has no columns to test.")
    out_vars = list(target.get("outputVars") or [])

    posting_date = args.get("posting_date") or await _resolve_default_posting_date()
    effective_date = args.get("effective_date")
    sample_limit = max(1, int(args.get("sample_limit") or 6))

    # Build prior code: every step BEFORE the schedule step (calc/condition/
    # iteration). This supplies the contextVars the schedule references.
    prior_rule = {**rule, "steps": steps[:idx], "outputs": {}}
    prior_code = _generate_rule_code(prior_rule)

    # Incremental column tests: replace the schedule step with one that has
    # only columns 1..k, then re-run. Fail fast on the first column that
    # errors out.
    column_results: list[dict] = []
    for k in range(1, len(cols) + 1):
        partial_step = {
            **target,
            "scheduleConfig": {**sc, "columns": cols[:k]},
            "outputVars": [],   # Skip outputVars during column probe
        }
        partial_rule = {**rule, "steps": steps[:idx] + [partial_step], "outputs": {}}
        code = _generate_rule_code(partial_rule)
        try:
            result, _ = await _execute_dsl_for_rule(
                partial_rule, code, posting_date, effective_date
            )
            err = None
            if not result.get("success", True):
                err = result.get("error") or "execution returned success=false"
        except ToolError as exc:
            err = str(exc)
        col = cols[k - 1]
        column_results.append({
            "column": col["name"],
            "formula": col["formula"],
            "ok": err is None,
            "error": err,
        })
        if err is not None:
            return {
                "rule_id": rule["id"],
                "step_name": target.get("name"),
                "ok": False,
                "failed_at": "column",
                "failed_column": col["name"],
                "error": err,
                "column_results": column_results,
                "fix_hint": (
                    f"Column '{col['name']}' formula failed. Check that all "
                    f"identifiers are EITHER (a) other columns defined ABOVE "
                    f"this one, (b) schedule built-ins "
                    f"({sorted(_SCHEDULE_COLUMN_BUILTINS)}), (c) DSL function "
                    f"names, or (d) variables from prior calc steps that "
                    f"appear in scheduleConfig.contextVars (auto-derived from "
                    f"this step's column formulas)."
                ),
            }

    # Full schedule + outputVars run.
    full_rule = {**rule, "steps": steps[:idx + 1], "outputs": {}}
    full_code = _generate_rule_code(full_rule)
    result, row_count = await _execute_dsl_for_rule(
        full_rule, full_code, posting_date, effective_date
    )
    if not result.get("success", True):
        return {
            "rule_id": rule["id"],
            "step_name": target.get("name"),
            "ok": False,
            "failed_at": "full_schedule",
            "error": result.get("error") or "execution failed",
            "column_results": column_results,
        }

    # Parse the schedule output from print_outputs (last print is the schedule).
    prints = result.get("print_outputs") or []
    preview_rows: list = []
    for p in reversed(prints):
        s = str(p).strip()
        if not s.startswith("["):
            continue
        try:
            parsed = json.loads(s)
        except Exception:
            continue
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], list):
            parsed = parsed[0]
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict) and "schedule" in parsed[0]:
            preview_rows = []
            for item in parsed:
                for r in (item.get("schedule") or []):
                    preview_rows.append(r)
        elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            preview_rows = parsed
        if preview_rows:
            break

    # Test each outputVar by name appearing in the prints.
    output_results: list[dict] = []
    for ov in out_vars:
        ov_name = ov.get("name")
        # Look for a print line like "<ov_name> = ..." or any line containing it.
        found = next((p for p in prints if isinstance(p, str) and ov_name in p), None)
        output_results.append({
            "name": ov_name,
            "type": ov.get("type"),
            "column": ov.get("column"),
            "ok": found is not None,
            "sample": (str(found)[:200] if found else None),
        })

    return {
        "rule_id": rule["id"],
        "step_name": target.get("name"),
        "ok": True,
        "row_count_input": row_count,
        "column_results": column_results,
        "output_results": output_results,
        "preview_sample": preview_rows[:sample_limit],
        "preview_total_rows": len(preview_rows),
        "next_action_hint": (
            "Schedule executed successfully. Safe to call finish or proceed "
            "to attach_rules_to_template."
        ),
    }


async def _auto_test_schedule_steps(rule: dict) -> list[dict]:
    """For each schedule step in `rule`, run tool_test_schedule_step. Returns
    a list of {step_name, ok, error?} entries. Used by create/update/finish
    so the agent SEES schedule failures in the same turn."""
    results: list[dict] = []
    for i, s in enumerate(rule.get("steps") or []):
        if (s.get("stepType") or "") != "schedule":
            continue
        try:
            r = await tool_test_schedule_step({
                "rule_id": rule["id"],
                "step_index": i,
                "sample_limit": 3,
            })
            results.append({
                "step_name": s.get("name"),
                "ok": bool(r.get("ok")),
                "failed_at": r.get("failed_at"),
                "error": r.get("error"),
                "failed_column": r.get("failed_column"),
            })
        except ToolError as exc:
            results.append({"step_name": s.get("name"), "ok": False, "error": str(exc)})
        except Exception as exc:  # pragma: no cover — defensive
            results.append({"step_name": s.get("name"), "ok": False,
                             "error": f"unexpected: {exc}"})
    return results


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
    # Hard block: standalone schedules render in the read-only code viewer with
    # no visual editor. Force the agent down the visual `add_step_to_rule`
    # path unless the user EXPLICITLY asked for a shared library schedule.
    if not bool(args.get("force_standalone")):
        raise ToolError(
            "REFUSED: standalone saved schedules render in the read-only code "
            "viewer with NO visual editor — the user will see an unfilled "
            "card and cannot edit columns/period/outputs.\n"
            "FIX: call `add_step_to_rule` with step.stepType='schedule' and a "
            "populated `scheduleConfig` (periodType, frequency, columns, "
            "contextVars auto-derived, outputVars). That renders inside the "
            "visual ScheduleStepModal where every field is editable. Then "
            "call `test_schedule_step` to verify it executes cleanly.\n"
            "Pass `force_standalone=true` ONLY when the user has explicitly "
            "asked for a SHARED, REUSABLE library schedule attached to "
            "multiple templates."
        )
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
    has_config = bool(args.get("config"))
    return {
        "schedule_id": doc["id"],
        "name": name,
        "priority": priority,
        "next_action_hint": (
            "Schedule saved. NOTE: this build has no standalone visual schedule "
            "editor — clicking the schedule in the Rule Manager will open it in "
            "the code editor showing its generatedCode. If the user needs a "
            "visually-editable schedule, instead add a `stepType:'schedule'` "
            "step to a saved rule via add_step_to_rule (with a populated "
            "scheduleConfig)."
            if not has_config
            else "Schedule saved with config; safe for visual rendering."
        ),
    }


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
            async for d in db.transaction_definitions.find({}, {"_id": 0, "transactiontype": 1}):
                if d.get("transactiontype"):
                    registered.add(d["transactiontype"].lower())
        except Exception:
            pass
    if not registered:
        for d in (_ServerBridge.in_memory_data.get("transaction_definitions") or []):
            if d.get("transactiontype"):
                registered.add(d["transactiontype"].lower())
    missing_types = [t for t in txn_types_used if t and t.lower() not in registered]
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

    # 7. Schedule-step presence (informational). Agents have been seen to
    # silently drop a schedule the user asked for. This surfaces it.
    schedule_step_names = [
        s.get("name") for s in steps if (s.get("stepType") or "") == "schedule"
    ]
    items.append({
        "check": "schedule_steps_present",
        "ok": True,  # informational — does not block ready
        "detail": (
            f"{len(schedule_step_names)} schedule step(s): {schedule_step_names}"
            if schedule_step_names else
            "no stepType='schedule' steps in this rule. If the user asked "
            "for a depreciation/amortisation/runoff/payment-plan schedule, "
            "add one via add_step_to_rule with stepType='schedule' BEFORE "
            "calling finish — a calc-step approximation is not a substitute."
        ),
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


_FORBIDDEN_FINISH_PHRASES = (
    "would you like me to",
    "would you like to",
    "would you like further",
    "shall i",
    "should i proceed",
    "should i continue",
    "do you want me to",
    "do you want to",
    "let me know if you want",
    "let me know if you'd like",
    "let me know if you would like",
    "if you want me to",
    "if you'd like me to",
    # Excuse-language for skipping a user-requested deliverable. The agent
    # has been seen to claim victory while admitting it didn't build what
    # was asked. These phrases are red flags that finish should refuse.
    "schedule was requested but",
    "schedule step was requested",
    "schedule was not created",
    "could not create a schedule",
    "could not create the schedule",
    "did not create the schedule",
    "skipped the schedule",
    "without the schedule",
    "in lieu of a schedule",
    "current workspace dsl only",
    "only exposed standalone schedule",
    "standalone schedule functions",
    "the existing rule path was used",
)


async def tool_finish(args: dict) -> dict:
    summary = (args.get("summary") or "").strip() or "Done."
    # `user_request` is injected by runtime.py from the original task prompt
    # so this gate cannot be circumvented by the agent omitting the field.
    user_request = (args.get("user_request") or "").lower()
    # If the caller passes a rule_id, do a final correctness gate: the rule
    # MUST have at least one entry in outputs.transactions[]. A rule with no
    # transactions emits NOTHING — that is never a valid completion state.
    rule_id = (args.get("rule_id") or "").strip()
    if rule_id:
        try:
            rule = await _load_rule(rule_id)
        except ToolError:
            rule = None
        if rule is not None:
            steps = rule.get("steps") or []
            # Hard gate: when the user explicitly asked for a schedule
            # (depreciation / amortisation / runoff / payment plan / EIR
            # term-structure / "create a schedule for ..."), refuse to
            # finish unless at least one stepType='schedule' step exists.
            # Prevents the agent from substituting a calc-step approximation.
            _SCHEDULE_KEYWORDS = (
                "schedule", "depreciation", "depreciate",
                "amortisation", "amortization", "amortise", "amortize",
                "accretion", "runoff", "run-off", "payment plan",
                "amortization schedule", "amortisation schedule",
            )
            asked_for_schedule = any(k in user_request for k in _SCHEDULE_KEYWORDS)
            asked_for_schedule = asked_for_schedule or any(
                k in summary.lower() for k in (
                    "depreciation", "depreciate", "amortisation",
                    "amortization", "runoff", "payment plan",
                )
            )
            has_schedule_step = any(
                (s.get("stepType") or "") == "schedule" for s in steps
            )
            if asked_for_schedule and not has_schedule_step:
                raise ToolError(
                    f"Rule '{rule.get('name')}' has ZERO stepType='schedule' "
                    f"steps, but the user's request and/or your summary "
                    f"explicitly mention a schedule "
                    f"(depreciation/amortisation/runoff/payment plan). A "
                    f"calc-step approximation is NOT a substitute and a "
                    f"standalone create_saved_schedule call is NOT a "
                    f"substitute either. FIX:\n"
                    f"  1. call add_step_to_rule with stepType='schedule' and "
                    f"a populated scheduleConfig (periodType, frequency, "
                    f"startDate*, endDate*/periodCount*, columns).\n"
                    f"  2. Schedule columns CAN reference outer calc vars, "
                    f"EVENTNAME.field, prior columns, and built-ins like "
                    f"period_index / period_number / lag / dcf.\n"
                    f"  3. call test_schedule_step until ok=true.\n"
                    f"  4. THEN add_transaction_to_rule referencing the "
                    f"schedule's outputVar (e.g. type='last' or 'sum').\n"
                    f"DO NOT call finish again until a schedule step exists "
                    f"and passes its preview."
                )
            txns = ((rule.get("outputs") or {}).get("transactions") or [])
            if not txns:
                raise ToolError(
                    f"Rule '{rule.get('name')}' has ZERO entries in "
                    f"`outputs.transactions[]`. The output of every accounting "
                    f"rule IS its transactions. A rule with no transactions "
                    f"produces no output and is never complete.\n"
                    f"FIX: call `add_transaction_to_rule` (twice — one debit, "
                    f"one credit) referencing the calc-step variable that "
                    f"holds the computed amount. DO NOT create a calc step "
                    f"named 'outputs_transactions' or 'transactions' — those "
                    f"steps do nothing; only the rule's `outputs.transactions[]` "
                    f"array drives the Transactions panel."
                )
            sides = [(t.get("side") or "").lower() for t in txns]
            if not (any(s == "debit" for s in sides) and any(s == "credit" for s in sides)):
                raise ToolError(
                    f"Rule '{rule.get('name')}' has transactions but the "
                    f"double-entry pair is unbalanced (sides={sides}). Add "
                    f"the missing side via `add_transaction_to_rule` before "
                    f"calling `finish`."
                )
            # Static-validation hard gate (rule #19): refuse to finish while
            # the rule has any undefined-variable references etc.
            try:
                static_errs = await _validate_rule_static(rule)
            except Exception:
                static_errs = []
            if static_errs:
                preview = "; ".join(
                    f"{e['where']}: undefined '{e['name']}'"
                    for e in static_errs[:5]
                )
                raise ToolError(
                    f"Rule '{rule.get('name')}' has {len(static_errs)} "
                    f"static-validation error(s) that will fail at dry-run: "
                    f"{preview}. FIX with update_step / add_step_to_rule, "
                    f"then call validate_rule to confirm ok=true BEFORE "
                    f"calling finish. (Rule #19: NEVER stop on validation "
                    f"failure.)"
                )
            # Schedule-test gate: every schedule step must pass its preview.
            # We re-run the tests here so the agent cannot skip them.
            try:
                sched_results = await _auto_test_schedule_steps(rule)
            except Exception:
                sched_results = []
            sched_failures = [r for r in sched_results if not r.get("ok")]
            if sched_failures:
                preview2 = "; ".join(
                    f"{r.get('step_name')}: "
                    f"{r.get('error') or 'failed'}"
                    for r in sched_failures[:5]
                )
                raise ToolError(
                    f"Rule '{rule.get('name')}' has {len(sched_failures)} "
                    f"schedule step(s) that fail their preview test: "
                    f"{preview2}. FIX each failing column/output via "
                    f"`update_step` and re-run `test_schedule_step` until "
                    f"ok=true BEFORE calling finish. A schedule that does "
                    f"not preview cannot run end-to-end."
                )
    low = summary.lower()
    for phrase in _FORBIDDEN_FINISH_PHRASES:
        if phrase in low:
            raise ToolError(
                f"`finish` summary contains the phrase '{phrase}', which means "
                f"the task is NOT actually complete — you are asking the user "
                f"to authorise more work. Two valid paths forward:\n"
                f"  (a) DO that work yourself NOW (call the relevant tools), "
                f"then call `finish` ONLY after `verify_rule_complete` returns "
                f"overall_ready=true AND `dry_run_template` returns balanced "
                f"transactions with no sanity_warnings.\n"
                f"  (b) If you genuinely need a business decision from the user "
                f"(e.g. an ambiguous threshold), state the SPECIFIC choice in "
                f"ONE declarative sentence — no questions, no offers, no "
                f"'would you like'. Example: 'Need the user to confirm whether "
                f"the PD floor is 0.0001 or 0.001 before continuing.'"
            )
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
STEP-TYPE DECISION TREE — READ THIS BEFORE PICKING A stepType
------------------------------------------------------------------
For each step you intend to add, walk this tree top-to-bottom and pick
the FIRST type that fits. Picking the wrong type is the #1 source of
authoring failure.

  Q1. Is the result a TABLE OF TIME-SERIES ROWS (amortisation, ECL
      projection, payment runoff, lease ROU, depreciation)?
        → use stepType = "schedule" (NOT iteration, NOT calc).
        → Add it via `add_step_to_rule` with `step.stepType='schedule'`
          and a populated `scheduleConfig` (periodType, frequency, columns,
          contextVars). Expose values via outputVars or a later calc step
          using schedule_filter / schedule_last / schedule_sum.
        → ⚠️ DO NOT use `create_saved_schedule` for typical user requests
          like "create a depreciation schedule". That tool produces a
          standalone DSL-only schedule with NO visual editor; the user
          will see an unfilled card. ALWAYS prefer the schedule STEP path
          unless the user explicitly asks for a shared/reusable library
          schedule.

  Q2. Do you need to APPLY THE SAME EXPRESSION TO EACH ELEMENT of an
      array that already exists in scope (e.g. a collected time-series
      from collect_by_instrument)?
        → use stepType = "iteration" with iterations[].type="apply_each".
        → sourceArray must be the NAME of a previously-defined array
          variable. NEVER "all_instruments" / "all_loans" — those do not
          exist; the engine already runs your rule once per instrument.

  Q3. Do you need MULTI-BRANCH IF/ELSE on a value (e.g. stage = 1/2/3
      based on days_overdue)?
        → use stepType = "condition" with conditions[] + elseFormula.
        → For ONE-LEVEL ternary INSIDE an expression you can also use
          if(cond, then, else) inline; reach for stepType="condition"
          when you have 2+ branches you want to read clearly.

  Q4. Otherwise — single value derived from event fields, prior steps,
      and DSL functions?
        → use stepType = "calc". Pick `source`:
            • "event_field"  → just copy a field. eventField = "EVT.fld".
            • "collect"      → collect_by_instrument / collect_all /
                                collect_by_subinstrument with eventField.
            • "value"        → numeric / date literal.
            • "formula"      → anything else; formula = "fn(a, b)".

  ❌ NEVER use stepType = "custom_code". The validator rejects it.
  ❌ NEVER create a calc step named "outputs_transactions",
     "transactions", "transaction", "output", "txns", "txn",
     "create_transaction", "emit_transactions", "post_transactions".
     Steps cannot emit transactions — only the rule's
     `outputs.transactions[]` array does. Use add_transaction_to_rule
     (twice — one debit + one credit) to populate it.

------------------------------------------------------------------
ONE RULE OR MANY? — DECIDE BEFORE YOU BUILD
------------------------------------------------------------------
Default: ONE rule per accounting event. A "rule" represents one
debit/credit pair (or set of pairs) tied to a single business event.

Split into multiple rules ONLY when ANY of these is true:
  • The rules emit different transaction-type pairs that the user wants
    auditable independently (e.g. ECL recognition vs. interest accrual).
  • The rules need different priorities because one consumes another's
    transactions (Rule B at priority 20 reads what Rule A at priority 10
    posted).
  • The rules attach to different event types (ECL → LoanCreditRiskData,
    revenue → SaleEvent).

Two calc steps inside the SAME rule are almost always better than two
single-step rules. Splitting unrelated logic into separate rules is OK;
splitting steps that share variables is NOT.

------------------------------------------------------------------
HOW THE ENGINE EXECUTES YOUR RULE  (READ THIS FIRST)
------------------------------------------------------------------
THE ENGINE ALREADY ITERATES PER ROW. Your rule body runs inside an
implicit `for row in merged_event_data:` loop. Each row represents
ONE (instrumentid × postingdate) tuple, and ALL referenced activity-
event fields are already JOINED onto that row.

You DO NOT iterate over instruments yourself. There is NO `all_instruments`
variable. There is NO `all_loans` variable. If you write
`sourceArray: "all_instruments"` the validator will reject it.

Three ways to access related data within a rule:

  1. ACTIVITY-EVENT field on the SAME row → reference DIRECTLY:
        formula: "LoanCreditRiskData.credit_impaired_flag"
     NOT:
        formula: "lookup(LoanCreditRiskData.credit_impaired_flag, instrumentid)"

  2. REFERENCE TABLE (small lookup) → collect_all + lookup/element_at:
        principals = collect_all('LoanRef_principal')
        rate       = lookup(principals, instrumentid)

  3. PER-INSTRUMENT TIME-SERIES (multiple postingdates of the same event)
     → collect_by_instrument:
        history = collect_by_instrument('UPB_balance')
        prior   = element_at(history, subtract(length(history), 1))

`outputs.transactions[]` are emitted ONCE PER ROW automatically. You do
NOT need an iteration step to fan them out per instrument.

WORKED EXAMPLE — IFRS9 Stage Assignment + ECL (no iteration needed):
  Steps:
    {name:"days_overdue",  stepType:"calc", source:"event_field",
                            eventField:"LoanCreditRiskData.days_past_due"},
    {name:"stage", stepType:"condition",
       conditions:[
         {condition:"gte(days_overdue, 90)", thenFormula:"3"},
         {condition:"gte(days_overdue, 30)", thenFormula:"2"}],
       elseFormula:"1"},
    {name:"pd",  stepType:"calc", source:"formula",
                  formula:"if(eq(stage,1), 0.01, if(eq(stage,2), 0.05, 1.0))"},
    {name:"lgd", stepType:"calc", source:"event_field",
                  eventField:"LoanCreditRiskData.lgd"},
    {name:"ead", stepType:"calc", source:"event_field",
                  eventField:"EOD_BALANCES_BEGINNINGBALANCE.upb"},
    {name:"ecl", stepType:"calc", source:"formula",
                  formula:"multiply(multiply(pd, lgd), ead)"}
  outputs.transactions:
    [{type:"ECLAllowance", amount:"ecl", side:"credit"},
     {type:"ECLExpense",   amount:"ecl", side:"debit"}]
  → The engine runs this once per (loan × postingdate). Done.

------------------------------------------------------------------
CANONICAL PATTERNS — pick ONE before authoring
------------------------------------------------------------------
~95% of accounting models in this codebase fit one of four patterns.
Call `list_templates` and `get_saved_rule` on the closest match before
writing anything from scratch.

PATTERN A — SCHEDULE + ROW EXTRACTION FOR postingdate
  Use for: amortisation, interest accrual, fee amortisation, lease ROU,
           IFRS9 stage projection, any tabular monthly time-series.
  Skeleton:
    1. calc steps capture inputs (principal, rate, term, …) from event fields
    2. schedule step produces N periodic rows (columns = period, balance,
       principal, interest, …) with contextVars listing the inputs
    3. calc step uses schedule_filter or lookup(scheduleColumn, periodColumn,
       postingdate) to pick THIS PERIOD'S row
    4. outputs.transactions[] emit debit/credit using the picked values
  Reference templates: loan_amortization, interest_accrual, fee_amortization,
                       lease_accounting, IFRSStage3.

PATTERN B — COLLECT + apply_each + AGGREGATE
  Use for: revenue recognition (POB allocation), weighted-average pricing,
           portfolio-level aggregation across collected values.
  Skeleton:
    1. calc step uses collect_by_instrument('EVT_field') → per-instrument array
    2. calc step computes denominator (e.g. sum(prices))
    3. iteration step (apply_each) computes per-element ratio/share
    4. calc step aggregates (sum/avg) the result
    5. outputs.transactions[] emit using the aggregate
  Reference templates: revenue_recognition, RevenueFinal111.

PATTERN C — REPLAY + LAG SCHEDULE + DELTA
  Use for: SBO replay, period-over-period adjustments, true-up postings.
  Skeleton:
    1. schedule step recomputes the FULL historical series with current inputs
    2. calc step uses lag('column', n) inside schedule columns to get prior period
    3. calc step subtracts prior posted amount from new amount → delta
    4. outputs.transactions[] post ONLY the delta
  Reference templates: SBO_Replay_M1, SBO_REPLAY_M2.

PATTERN D — SCALAR FINANCE
  Use for: NPV, IRR, single-row valuation where there is no schedule.
  Skeleton:
    1. calc steps read cash flows + discount rate from event fields
    2. calc step calls npv(rate, cashflow_array) or irr(cashflow_array)
    3. outputs.transactions[] post the resulting scalar
  Reference template: npv_analysis.

If your model does not fit A/B/C/D, STOP and ask the user — do not
invent a 5th pattern.

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
10. BOOLEAN LITERALS are Python-style: `True` and `False` (capitalised
    first letter). NOT `true`, NOT `TRUE`, NOT `"true"` / `"false"`.
       WRONG: eq(LoanCreditRiskData.flag, true)
       WRONG: eq(LoanCreditRiskData.flag, TRUE)
       WRONG: eq(LoanCreditRiskData.flag, "true")
       RIGHT: eq(LoanCreditRiskData.flag, True)
    A boolean field can also be used directly in a condition:
       RIGHT: condition: "LoanCreditRiskData.is_impaired"
11. TRANSACTION `amount` FIELD — when you put a transaction in
    `outputs.transactions[]`, the `amount` value is interpolated as a
    raw expression. It MUST be EITHER:
       (a) a numeric literal: `"100.0"`, OR
       (b) the NAME of a variable defined by a prior step:
              steps: [{name:"ecl", stepType:"calc", formula:"…"}]
              outputs.transactions: [{type:"X", amount:"ecl", side:"credit"}]
    Writing `amount: "amount"` (the literal word) fails at dry-run with
    `name 'amount' is not defined`. The string is NOT a label — it is
    the expression that gets evaluated.

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
SCHEDULE STEP — FULL FIELD REFERENCE  (mirrors ScheduleStepModal)
------------------------------------------------------------------
Every schedule step is rendered by the visual ScheduleStepModal. Every
scheduleConfig key below corresponds to a field in that modal. Missing
or invalid values are rejected by `_validate_schedule_step_shape`.

  scheduleConfig:
    periodType:  "date"   → use start/end dates
                 "number" → use periodCount

    frequency:   "D" | "W" | "M" | "Q" | "Y"   (REQUIRED)

    convention:  "" | "30/360" | "Actual/360" | "Actual/365" |
                 "Actual/Actual" | "30E/360"   (optional, date period only)

    # Date-range mode (periodType=="date"):
    startDateSource: "value" | "field" | "formula"
      • value   → startDate          = "2026-01-01"
      • field   → startDateField     = "EVENTNAME.fieldname"
      • formula → startDateFormula   = "<DSL expression>"
    endDateSource:   same shape on endDate / endDateField / endDateFormula

    # Count mode (periodType=="number"):
    periodCountSource: "value" | "field" | "formula"
      • value   → periodCount         = 12
      • field   → periodCountField    = "EVENTNAME.fieldname"
      • formula → periodCountFormula  = "<DSL expression>"

    columns: [{name, formula}, ...]   (REQUIRED, at least one)
      • Each column formula may reference, in order of preference:
          (a) a column DEFINED ABOVE it in the same array
          (b) a SCHEDULE BUILT-IN (see list below)
          (c) any DSL function name
          (d) a contextVar (auto-derived; see below)

    contextVars: AUTO-DERIVED — the validator scans every column formula
                 and pulls in any identifier that is not (a)/(b)/(c) above.
                 You DO NOT need to populate this manually; whatever you
                 supply is merged with the auto-derived set.

  IMPORTANT — what schedule column formulas CAN reference:
    • Schedule built-ins (period_index, period_date, period_number,
      period_start, total_periods, dcf, lag, days_in_current_period,
      daily_basis, item_name, subinstrument_id, s_no, index,
      start_date, end_date) — auto-injected, do NOT define them.
    • EVENTNAME.field directly — eg.  multiply(FixedAssetData.acquisition_cost, 0.1)
    • Any variable defined by a calc / iteration / condition step BEFORE
      this schedule step. These are auto-pulled into contextVars.
    • Any DSL function name.
    • Any column DEFINED ABOVE the current one in the same `columns` array.
  Schedule column formulas CANNOT reference: another schedule's outputVars,
    a calc step that comes AFTER this schedule, or anything you wrote inside
    a transaction's `amount` field.

  outputVars: [{name, type, column, ...}, ...]
    type ∈ {"first","last","sum","column","filter"}.
      • first   → schedule_first(sched, "<column>")    ⇒ scalar
      • last    → schedule_last(sched, "<column>")     ⇒ scalar
      • sum     → schedule_sum(sched, "<column>")      ⇒ scalar
      • column  → schedule_column(sched, "<column>")   ⇒ array
      • filter  → schedule_filter(sched, "<matchCol>", <matchValue>, "<column>")
                  REQUIRES matchCol + matchValue + column.
    Every `column` MUST be the name of a defined column in
    scheduleConfig.columns. The validator rejects unknown column names.

SCHEDULE COLUMN BUILT-INS (auto-injected into every column expression;
do NOT put them in contextVars):
  period_date, period_index, period_start, period_number,
  dcf, lag, days_in_current_period, total_periods,
  daily_basis, item_name, subinstrument_id, s_no,
  index, start_date, end_date

REQUIRED VALIDATION FLOW for any schedule step you author:
  1. add_step_to_rule with stepType='schedule' and a populated
     scheduleConfig (validator runs immediately; fix any errors it returns).
  2. test_schedule_step(rule_id, step_name) — runs column-by-column +
     each outputVar exactly like the visual modal's preview button.
     This MUST return ok=true. If `failed_at='column'` the named column
     formula references something undefined; either rename to a column
     defined above, fix to a built-in, or define the variable in a calc
     step BEFORE the schedule step.
  3. Only then proceed to add transactions / call finish.
  Finish refuses to close while any schedule step in the rule fails its
  preview.

NEVER call `create_saved_schedule` for a typical "create a depreciation
/ amortization / ECL / runoff schedule" request. That tool is hard-blocked
unless `force_standalone=true` and is reserved for shared library
schedules attached to multiple templates by the user.

------------------------------------------------------------------
EMITTING TRANSACTIONS  (THE OUTPUT OF A RULE *IS* ITS TRANSACTIONS)
------------------------------------------------------------------
A rule with ZERO entries in `outputs.transactions[]` produces NO
OUTPUT and is never a valid result. A calc step does NOT emit a
transaction — no matter what you name it.

DO NOT do any of these (all are WRONG and the validator will reject them):
  ✗ Create a calc step named `outputs_transactions` / `transactions` /
    `transaction` / `output` with a placeholder value of 0.
  ✗ Put `createTransaction(...)` inside a calc-step formula.
  ✗ Put `createTransaction(...)` inside an iteration expression.
  ✗ Wrap an `outputs.transactions` object inside a step's value field.

The ONE correct pattern:
  1. Register the transaction types ONCE up-front:
        add_transaction_types([{name:'ECLAllowance'}, {name:'ECLExpense'}])
  2. Add a calc step that COMPUTES the amount (this step's `name` becomes
     the variable referenced below):
        { name:'ecl_amount', stepType:'calc', source:'formula',
          formula:'multiply(multiply(pd, lgd), ead)' }
  3. Add the transactions to the rule's `outputs.transactions[]` (preferred
     interface: call `add_transaction_to_rule` twice, once per side):
        add_transaction_to_rule(rule_id, type='ECLAllowance', amount='ecl_amount', side='credit')
        add_transaction_to_rule(rule_id, type='ECLExpense',   amount='ecl_amount', side='debit')
     Equivalent shape on create_saved_rule / update_saved_rule:
        outputs: { createTransaction:true, transactions:[
           {type:'ECLAllowance', amount:'ecl_amount', side:'credit'},
           {type:'ECLExpense',   amount:'ecl_amount', side:'debit'} ] }

RULES:
- `amount` is the NAME of a prior calc step (or a numeric literal).
- ALWAYS pair debit + credit so the entry balances.
- The engine emits these transactions ONCE PER ROW automatically — no
  iteration step needed for fan-out across instruments.

------------------------------------------------------------------
SUB-INSTRUMENTS — when an instrument has MORE THAN ONE subId
------------------------------------------------------------------
Most data has one subinstrumentid per (instrumentid × postingdate). For
that case, transactions just default `subInstrumentId: "1.0"` and the
engine fans out one txn per instrument-row. Done.

But many models (lease ROU components, POB allocations, multi-tranche
loans, scheduled draws) have MULTIPLE distinct subInstrumentIds within
the same instrumentid. The engine's row-merge collapses them: only ONE
subId survives in the merged row, so a hardcoded `subInstrumentId: "1.0"`
mis-tags every transaction (or worse, the engine writes everything to
"1" silently).

THE RULE — when an event has multi-subid data:
  1. NEVER hardcode `subInstrumentId: "1.0"`. Use the row builtin
     `subinstrumentid` so each row's transaction carries that row's subId:
        outputs.transactions: [
          {type:"X", amount:"v", side:"debit",
           postingDate:"EVT.postingdate", effectiveDate:"EVT.effectivedate",
           subInstrumentId:"subinstrumentid"}
        ]
     The validator does this for you AUTOMATICALLY: when any event
     referenced by your rule has >1 subId per instrument in the loaded
     data, `_normalise_transaction_outputs` overrides any literal
     "1" / "1.0" / "" with `subinstrumentid` and reports
     `multi_subid_events` + `multi_subid_hint` in the response.
  2. To fan out a transaction PER SUB-INSTRUMENT explicitly (i.e. emit N
     transactions where N = number of subIds for the current instrument),
     add a calc step:
        {name:"sub_ids", stepType:"calc", source:"collect",
         eventField:"EVT.subinstrumentid",
         collectType:"collect_by_instrument"}
     Then reference `sub_ids` from the transaction's `subInstrumentId`.
     The engine will iterate the array and emit one txn per subId.
  3. To compute a per-subid amount (e.g. allocate by subId), use
     `collect_by_subinstrument(EVT.field)` — it returns the array of that
     field's values restricted to the current (instrumentid, subinstrumentid)
     pair so it does not collapse across subIds.

DETECTION — the validator surfaces multi-subid events automatically:
  When `tool_create_saved_rule` / `tool_update_saved_rule` returns a
  payload containing `multi_subid_events: ["EVT", ...]`, the loaded data
  for those events has >1 subId per instrumentid. Read the
  `multi_subid_hint` and either accept the auto-defaulted `subinstrumentid`
  OR add the explicit `sub_ids = collect_by_instrument(...)` step shown
  above.

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
    "add_transaction_to_rule": tool_add_transaction_to_rule,
    "delete_transaction_from_rule": tool_delete_transaction_from_rule,
    "update_transaction_in_rule": tool_update_transaction_in_rule,
    "debug_step": tool_debug_step,
    "test_schedule_step": tool_test_schedule_step,
    "list_saved_schedules": tool_list_saved_schedules,
    "create_saved_schedule": tool_create_saved_schedule,
    "delete_saved_schedule": tool_delete_saved_schedule,
    "debug_schedule": tool_debug_schedule,
    "verify_rule_complete": tool_verify_rule_complete,
    "attach_rules_to_template": tool_attach_rules_to_template,
    "finish": tool_finish,
    "validate_rule": tool_validate_rule,
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
        "description": "List available DSL functions with worked single-line examples. Optionally filter by category or name substring. Use this to discover the exact function names AND see how to use them before writing rules.",
        "parameters": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional category filter (e.g. 'Math', 'Date', 'Schedule')"},
                "name": {"type": "string", "description": "Optional name substring filter (e.g. 'collect' or 'schedule')"},
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
        "description": (
            "Signal that the task is complete. Provide a short user-facing summary. "
            "If you built or modified a rule, ALSO pass `rule_id` so the runtime can "
            "verify the rule has at least one balanced debit/credit pair in "
            "`outputs.transactions[]` before accepting completion."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "rule_id": {"type": "string"},
            },
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
        "name": "add_transaction_to_rule",
        "description": (
            "Append ONE transaction entry to a rule's `outputs.transactions[]` array. "
            "This is the ONLY supported way to make a rule emit a transaction — "
            "DO NOT create a calc step named 'outputs_transactions' or 'transactions'. "
            "Always call this tool TWICE in a row to add a balanced debit + credit pair. "
            "`amount` must be the NAME of a prior calc-step variable that holds the "
            "computed amount (or a numeric literal). The transaction `type` must already "
            "be registered via `add_transaction_types`. "
            "`postingdate`/`effectivedate` should be event-field references such as "
            "'EOD.postingdate' — if omitted they are inferred from the first event "
            "referenced in the rule's steps. `subinstrumentid` defaults to '1.0'."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "type": {"type": "string", "description": "Registered transaction type name"},
                "amount": {"type": "string", "description": "Name of a prior calc-step variable, or a numeric literal"},
                "side": {"type": "string", "enum": ["debit", "credit"]},
                "postingdate": {"type": "string", "description": "e.g. 'EOD.postingdate' — inferred if omitted"},
                "effectivedate": {"type": "string", "description": "e.g. 'EOD.effectivedate' — inferred if omitted"},
                "subinstrumentid": {"type": "string", "description": "Defaults to '1.0' if omitted"},
            },
            "required": ["rule_id", "type", "amount", "side"],
        },
    },
    {
        "name": "delete_transaction_from_rule",
        "description": (
            "Remove ONE entry from a rule's `outputs.transactions[]` array, "
            "OR clear the whole array via `delete_all=true`. "
            "Identify a single entry via `transaction_index`, OR `type` "
            "(+ optional `side`), OR a `match` dict (e.g. "
            "{type:'X', side:'debit', amount:'y'}). "
            "If multiple entries match (e.g. you only pass `type` and there "
            "are duplicates), the tool errors and lists candidates so you "
            "can disambiguate. Use this for: 'remove duplicate transactions', "
            "'delete the AccumulatedDepreciation entry', 'remove all "
            "transactions where postingdate is empty' (call once per offending "
            "index), or 'delete all transactions' (delete_all=true)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "transaction_index": {"type": "integer", "description": "0-based index inside outputs.transactions[]"},
                "type": {"type": "string"},
                "side": {"type": "string", "enum": ["debit", "credit"]},
                "match": {"type": "object", "description": "Exact-match filter on transaction fields, e.g. {type, side, amount, postingDate}"},
                "delete_all": {"type": "boolean", "description": "If true, clear the entire transactions array."},
            },
            "required": ["rule_id"],
        },
    },
    {
        "name": "update_transaction_in_rule",
        "description": (
            "Patch ONE entry in a rule's `outputs.transactions[]` array. "
            "Identify it via `transaction_index`, OR `type` (+ optional "
            "`side`), OR a `match` dict. Pass the new values inside `patch` "
            "(or at the top level) — supported fields: type, amount, side, "
            "postingdate, effectivedate, subinstrumentid."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "transaction_index": {"type": "integer"},
                "type": {"type": "string", "description": "Locator: existing type name"},
                "side": {"type": "string", "enum": ["debit", "credit"], "description": "Locator: existing side"},
                "match": {"type": "object"},
                "patch": {
                    "type": "object",
                    "description": "Fields to overwrite",
                    "properties": {
                        "type": {"type": "string"},
                        "amount": {"type": "string"},
                        "side": {"type": "string", "enum": ["debit", "credit"]},
                        "postingdate": {"type": "string"},
                        "effectivedate": {"type": "string"},
                        "subinstrumentid": {"type": "string"},
                    },
                },
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
        "name": "test_schedule_step",
        "description": (
            "Execute a stepType='schedule' step the same way the visual "
            "ScheduleStepModal preview button does: builds period(...) + "
            "schedule(...) DSL, runs it INCREMENTALLY column-by-column "
            "(failing fast on the first broken column), then evaluates each "
            "outputVar and returns a materialised preview. ALWAYS call this "
            "after add_step_to_rule / update_step on a schedule step BEFORE "
            "calling finish — finish will refuse to close until every "
            "schedule step in the rule has passed this test."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
                "posting_date": {"type": "string"},
                "effective_date": {"type": "string"},
                "sample_limit": {"type": "integer"},
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
            "⛔ BLOCKED BY DEFAULT — returns ToolError unless `force_standalone=true`.\n"
            "\n"
            "Standalone saved schedules render in the read-only code viewer with NO visual editor; "
            "the user sees an unfilled schedule card with no way to edit columns, period, or outputs. "
            "For ANY user request like 'create a depreciation/amortization/ECL/runoff schedule', "
            "call `add_step_to_rule` with step.stepType='schedule' and a populated `scheduleConfig`, "
            "then `test_schedule_step` to verify. That path is fully editable in the visual modal.\n"
            "\n"
            "Pass `force_standalone=true` ONLY when the user has EXPLICITLY asked for a shared, "
            "reusable library schedule attached to multiple templates via `attach_rules_to_template`."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dsl_code": {"type": "string"},
                "priority": {"type": "integer"},
                "config": {"type": "object", "description": "Optional structured config (frequency, columns, etc.) used by the Schedule Builder UI."},
                "force_standalone": {"type": "boolean", "description": "REQUIRED to be true. Set only when the user explicitly asked for a shared library schedule."},
            },
            "required": ["name", "dsl_code", "force_standalone"],
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
        "name": "validate_rule",
        "description": (
            "Static-analyse a saved rule for the most common authoring "
            "mistakes WITHOUT executing it: undefined variable references, "
            "transaction `amount` fields that point at nonexistent steps, "
            "missing event prefixes. FAST and SAFE — call after every "
            "mutation to catch errors immediately. Returns ok + errors[] "
            "with a fix_hint per error. You MUST resolve all errors before "
            "calling finish."
        ),
        "parameters": {
            "type": "object",
            "properties": {"rule_id": {"type": "string"}},
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
