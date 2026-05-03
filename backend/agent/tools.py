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

import contextvars
import json
import logging
import random
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


# Per-request run id, set by the runtime before each tool dispatch. Lets
# tools.py associate state (e.g. submit_plan acceptance) with the in-flight
# agent run without changing every tool signature.
current_run_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "current_run_id", default=""
)


def set_current_run_id(run_id: str) -> None:
    """Called by runtime.py at the top of each step. Safe to call with ''."""
    try:
        current_run_id.set(run_id or "")
    except Exception:
        pass


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
    "date": lambda rng, hints: hints.get(
        "default_date",
        # If a start/end range is provided use it, otherwise fall back to 2026-01-01
        hints.get("value", "2026-01-01"),
    ),
}


# Domain-aware field-name heuristics. Each entry: (matcher_predicate, range,
# datatype_override). The first match wins. Ranges are calibrated so that
# downstream multiplications (PD × LGD × EAD, principal × rate, etc.) produce
# realistic — not astronomical — accounting amounts.
def _name_match(name: str, *needles: str) -> bool:
    return any(n in name for n in needles)


# ---------------------------------------------------------------------------
# Per-use-case recommended field sets.  Keyed by lowercase keyword substrings
# that appear in an event_name.  If any of those keywords match, the agent
# receives a hint listing the canonical accounting-standard field names that
# SHOULD be present in the event definition.
# ---------------------------------------------------------------------------
_ACCOUNTING_EVENT_TEMPLATES: list[dict] = [
    {
        "keywords": ["fas91", "fee_amort", "amortization", "amortisation",
                     "loan_fee", "origination_fee", "deferred_fee"],
        "standard": "FAS 91 / Amortised Cost",
        "recommended_fields": [
            "loan_amount", "outstanding_balance", "origination_fee",
            "amortized_fee", "note_rate", "eir_rate",
            "origination_date", "maturity_date", "term_months",
        ],
    },
    {
        "keywords": ["ecl", "ifrs9", "ifrs_9", "credit_risk", "impairment",
                     "provision", "allowance", "cecl"],
        "standard": "IFRS 9 / CECL",
        "recommended_fields": [
            "pd", "lgd", "ead", "ecl", "stage",
            "days_past_due", "outstanding_balance", "collateral_value",
        ],
    },
    {
        "keywords": ["lease", "rou", "ifrs16", "ifrs_16", "asc842",
                     "asc_842", "right_of_use", "rightofuse"],
        "standard": "IFRS 16 / ASC 842",
        "recommended_fields": [
            "rou_asset", "lease_liability", "lease_payment",
            "discount_rate", "lease_term",
            "lease_start_date", "lease_end_date",
        ],
    },
    {
        "keywords": ["depreciation", "fixed_asset", "ias16", "ias_16",
                     "asc360", "asc_360", "property_plant"],
        "standard": "IAS 16 / ASC 360",
        "recommended_fields": [
            "acquisition_cost", "residual_value", "useful_life",
            "accumulated_depreciation", "depreciation_charge",
            "nbv", "acquisition_date", "depreciation_method",
        ],
    },
    {
        "keywords": ["revenue", "ifrs15", "ifrs_15", "asc606", "asc_606",
                     "contract", "performance_obligation"],
        "standard": "IFRS 15 / ASC 606",
        "recommended_fields": [
            "contract_amount", "ssp", "allocated_amount",
            "recognized_revenue", "deferred_revenue",
            "contract_start_date", "contract_end_date",
        ],
    },
    {
        "keywords": ["sbo", "statement_of_obligations", "bond", "security",
                     "mtm", "fair_value", "market_value"],
        "standard": "Securities / Fair Value Measurement",
        "recommended_fields": [
            "face_value", "book_value", "market_value", "coupon_rate",
            "maturity_date", "purchase_date", "accrued_interest",
        ],
    },
]


def _check_accounting_field_hints(event_name: str, field_names: list[str]) -> dict | None:
    """Return a hint dict if the event matches a known standard but is missing key fields."""
    name_lc = event_name.lower()
    for template in _ACCOUNTING_EVENT_TEMPLATES:
        if any(kw in name_lc for kw in template["keywords"]):
            recommended = template["recommended_fields"]
            field_names_lc = {f.lower() for f in field_names}
            missing = [f for f in recommended if f not in field_names_lc]
            if missing:
                return {
                    "accounting_standard": template["standard"],
                    "recommended_fields": recommended,
                    "missing_standard_fields": missing,
                    "hint": (
                        f"This event appears to follow {template['standard']}. "
                        f"Standard field names are missing: {missing}. "
                        "Use the exact names above — the sample-data generator and "
                        "system prompt both key off these names for realistic values."
                    ),
                }
            return None  # all recommended fields present
    return None  # no matching standard


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
    # --- FAS91 / loan fee amortization ------------------------------------
    (lambda n: _name_match(n, "origination_fee", "upfront_fee", "loan_fee",
                            "deferred_fee", "unamortized_fee", "net_fee"),
        {"range": (500.0, 15_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "amortized_fee", "period_amortization",
                            "fee_amortized", "amortization_amount"),
        {"range": (10.0, 2_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "note_rate", "contract_rate", "stated_rate", "coupon_rate"),
        {"range": (0.02, 0.12), "decimals": 6}, "decimal"),
    (lambda n: _name_match(n, "eir_rate", "effective_interest_rate", "yield_rate"),
        {"range": (0.02, 0.14), "decimals": 6}, "decimal"),
    # --- Lease accounting (IFRS 16 / ASC 842) ----------------------------
    (lambda n: _name_match(n, "rou_asset", "right_of_use", "lease_asset",
                            "rouasset", "rightofuse"),
        {"range": (10_000.0, 500_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "lease_liability", "lease_obligation",
                            "leaseLiability", "lease_balance"),
        {"range": (10_000.0, 500_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "lease_payment", "lease_installment",
                            "monthly_rent", "annual_rent", "annual_lease"),
        {"range": (500.0, 20_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "discount_rate", "incremental_borrowing_rate",
                            "ibr", "lessee_rate"),
        {"range": (0.02, 0.10), "decimals": 6}, "decimal"),
    (lambda n: _name_match(n, "lease_term", "lease_period"),
        {"range": (12, 120)}, "integer"),
    # --- Depreciation / fixed assets (IAS 16 / ASC 360) -----------------
    (lambda n: _name_match(n, "acquisition_cost", "purchase_cost", "gross_cost",
                            "asset_cost", "historical_cost"),
        {"range": (5_000.0, 500_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "residual_value", "salvage_value", "scrap_value"),
        {"range": (0.0, 50_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "accumulated_depreciation", "accum_depr"),
        {"range": (0.0, 400_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "depreciation_charge", "depreciation_amount",
                            "annual_depreciation", "period_depreciation",
                            "depr_amount", "depr_charge"),
        {"range": (500.0, 50_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "nbv", "net_book_value", "carrying_value",
                            "book_value", "carrying_amount"),
        {"range": (0.0, 500_000.0), "decimals": 2}, "decimal"),
    # --- Revenue recognition (IFRS 15 / ASC 606) -------------------------
    (lambda n: _name_match(n, "contract_amount", "transaction_price",
                            "contract_value", "revenue_amount"),
        {"range": (1_000.0, 500_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "ssp", "standalone_selling_price",
                            "standalone_price", "sspprice"),
        {"range": (100.0, 50_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "allocated_amount", "allocated_revenue",
                            "allocation"),
        {"range": (500.0, 200_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "recognized_revenue", "revenue_recognized",
                            "period_revenue", "recognition_amount"),
        {"range": (100.0, 50_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "deferred_revenue", "contract_asset",
                            "contract_liability"),
        {"range": (0.0, 200_000.0), "decimals": 2}, "decimal"),
    # --- IFRS 9 / CECL impairment ----------------------------------------
    (lambda n: _name_match(n, "ecl", "expected_credit_loss", "allowance",
                            "impairment", "provision"),
        {"range": (0.0, 50_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "collateral_value", "collateral"),
        {"range": (0.0, 1_000_000.0), "decimals": 2}, "decimal"),
    (lambda n: _name_match(n, "credit_impaired", "credit_impaired_flag",
                            "is_defaulted", "in_default"),
        {"choices": [True, False]}, "boolean"),
    # --- General product / instrument attributes --------------------------
    (lambda n: _name_match(n, "instrument_type", "loan_type", "asset_type",
                            "product_category"),
        {"choices": ["FixedRate", "VariableRate", "Hybrid", "FloatingRate"]}, "string"),
    (lambda n: _name_match(n, "industry", "sector", "industry_code"),
        {"choices": ["Manufacturing", "Retail", "Financial", "Healthcare",
                     "Technology", "RealEstate", "Construction"]}, "string"),
    (lambda n: _name_match(n, "region", "geography"),
        {"choices": ["North", "South", "East", "West", "Central"]}, "string"),
    (lambda n: _name_match(n, "subinstrumentid", "sub_instrument_id", "sub_id"),
        {"choices": ["1.0", "2.0", "3.0"]}, "string"),
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


def _date_heuristic(name: str, rng: random.Random) -> str | None:
    """Return a plausible date string for common date field names.

    Origination / inception / issue dates → a past date within the last 2 yrs.
    Maturity / expiry / end dates         → a future date 1-5 years from today.
    Purchase / settlement / trade dates   → yesterday-ish.
    Reporting / posting / effective dates → today (2026-01-31) by default.
    Unknown                               → None (caller uses fallback).
    """
    from datetime import date as _date, timedelta as _td
    today = _date(2026, 1, 31)  # stable reference — matches typical posting date in tests

    ORIGINATION = ("origination", "inception", "issue", "start", "acquisition",
                   "booking", "drawdown", "open", "funded", "disbursement")
    MATURITY     = ("maturity", "expiry", "expiration", "end", "close", "final",
                    "term_end", "redemption")
    TRADE        = ("trade", "settlement", "value", "purchase", "sale")
    REPORTING    = ("report", "posting", "effective", "period", "as_of", "asof",
                    "entry", "record")

    if any(k in name for k in ORIGINATION):
        # Random date 3-24 months before today
        days_back = rng.randint(90, 730)
        d = today - _td(days=days_back)
        # Snap to month-end: last day of that month
        import calendar as _cal
        last = _cal.monthrange(d.year, d.month)[1]
        return _date(d.year, d.month, last).isoformat()

    if any(k in name for k in MATURITY):
        # Random date 12-60 months after today
        days_fwd = rng.randint(365, 1825)
        d = today + _td(days=days_fwd)
        import calendar as _cal
        last = _cal.monthrange(d.year, d.month)[1]
        return _date(d.year, d.month, last).isoformat()

    if any(k in name for k in TRADE):
        days_back = rng.randint(1, 10)
        return (today - _td(days=days_back)).isoformat()

    if any(k in name for k in REPORTING):
        return today.isoformat()

    return None  # let caller fall back to 2026-01-01


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

    # Date fields: apply name-based heuristics so origination/start dates
    # are earlier than maturity/end dates and values look realistic.
    if chosen_dtype == "date" and "default_date" not in user_hints and "value" not in user_hints:
        _date_val = _date_heuristic(name, rng)
        if _date_val:
            chosen_hints["default_date"] = _date_val

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


# ---------------------------------------------------------------------------
# Accounting-domain-aware coherent profile generation
# ---------------------------------------------------------------------------

def _detect_accounting_domain(event_name: str, field_names: list[str]) -> str:
    """Identify the accounting standard domain from event name + fields.

    Returns one of: fas91 | ifrs9 | lease | fixed_asset | revenue | securities | generic.
    """
    name_lc = event_name.lower()
    fields_lc = {f.lower() for f in field_names}

    def _hits_name(*kws: str) -> bool:
        return any(k in name_lc for k in kws)

    def _hits_fields(*kws: str) -> bool:
        return any(k in fields_lc for k in kws)

    if _hits_name("fas91", "fee_amort", "loan_fee", "deferred_fee") or \
       _hits_fields("origination_fee", "loan_fee", "deferred_fee",
                    "amortized_fee", "eir_rate", "note_rate"):
        return "fas91"
    if _hits_name("ecl", "ifrs9", "ifrs_9", "credit_risk",
                  "impairment", "provision", "allowance", "cecl") or \
       _hits_fields("pd", "lgd", "ecl", "stage",
                    "days_past_due", "credit_impaired"):
        return "ifrs9"
    if _hits_name("lease", "ifrs16", "ifrs_16", "asc842", "asc_842",
                  "rou", "rightofuse") or \
       _hits_fields("rou_asset", "lease_liability", "lease_payment",
                    "incremental_borrowing_rate", "ibr", "right_of_use"):
        return "lease"
    if _hits_name("depreciation", "fixed_asset", "ias16", "ias_16",
                  "asc360", "asc_360", "property_plant") or \
       _hits_fields("acquisition_cost", "residual_value", "useful_life",
                    "accumulated_depreciation", "nbv"):
        return "fixed_asset"
    if _hits_name("revenue", "ifrs15", "ifrs_15", "asc606", "asc_606",
                  "contract", "performance_obligation") or \
       _hits_fields("contract_amount", "ssp", "recognized_revenue",
                    "deferred_revenue", "allocated_amount"):
        return "revenue"
    if _hits_name("bond", "security", "sbo", "fair_value", "mtm",
                  "market_value") or \
       _hits_fields("face_value", "market_value", "accrued_interest",
                    "coupon_rate"):
        return "securities"
    return "generic"


def _generate_instrument_profiles(
    event_def: dict,
    instrument_ids: list[str],
    posting_dates: list[str],
    seed: int = 42,
) -> dict:
    """Build a per-instrument profile that drives coherent row generation.

    Each instrument gets:
      - ``static``: field values that are constant across all posting dates
        (e.g. principal, rate, origination date, stage).
      - ``time_series``: lists of values aligned to *sorted* posting dates
        (e.g. declining balance, increasing accumulated depreciation).

    Returns::

        {
          "domain": str,
          "profiles": {
              instrument_id: {"static": {...}, "time_series": {field: [...]}}
          },
          "sorted_dates": [sorted posting dates]
        }
    """
    import calendar as _cal
    from datetime import date as _date, timedelta as _td

    fields = event_def.get("fields", [])
    field_names_lc = {f.get("name", "").lower() for f in fields}
    event_name = event_def.get("event_name", "")
    domain = _detect_accounting_domain(event_name, list(field_names_lc))

    sorted_dates = sorted(posting_dates)
    n_periods = len(sorted_dates)

    def _parse_date(s: str) -> _date:
        try:
            return _date.fromisoformat(s)
        except Exception:
            return _date(2026, 1, 1)

    def _round_thousands(v: float) -> float:
        return round(v / 1000) * 1000

    profiles: dict[str, dict] = {}

    for i, inst_id in enumerate(instrument_ids):
        inst_rng = random.Random(seed + i * 1997)  # deterministic per instrument
        static: dict[str, Any] = {}
        time_series: dict[str, list] = {}

        if domain == "fas91":
            # ----------------------------------------------------------------
            # FAS 91 / Amortised Cost — loan fee amortization
            # ----------------------------------------------------------------
            _LOAN_TIERS = [50_000, 75_000, 100_000, 125_000, 150_000, 175_000,
                           200_000, 250_000, 300_000, 350_000, 400_000, 500_000]
            loan_amount = _round_thousands(
                inst_rng.choice(_LOAN_TIERS) * inst_rng.choice([1.0, 1.1, 0.9, 1.25, 0.75])
            )
            note_rate = round(inst_rng.uniform(0.030, 0.095), 6)
            fee_pct = inst_rng.uniform(0.005, 0.025)          # 0.5 % – 2.5 %
            origination_fee = round(loan_amount * fee_pct, 2)
            eir_rate = round(note_rate + fee_pct / inst_rng.uniform(3.0, 7.0), 6)
            term_months = inst_rng.choice(
                [24, 36, 48, 60, 72, 84, 120, 180, 240, 300, 360]
            )

            first_pd = _parse_date(sorted_dates[0])
            months_back = inst_rng.randint(1, min(36, term_months - 1))
            orig_month = first_pd.month - (months_back % 12)
            orig_year = first_pd.year - (months_back // 12)
            if orig_month <= 0:
                orig_month += 12
                orig_year -= 1
            orig_day = min(first_pd.day,
                           _cal.monthrange(orig_year, orig_month)[1])
            origination_date = _date(orig_year, orig_month, orig_day)

            mat_total_months = origination_date.month + term_months
            mat_year = origination_date.year + (mat_total_months - 1) // 12
            mat_month = ((mat_total_months - 1) % 12) + 1
            mat_day = _cal.monthrange(mat_year, mat_month)[1]
            maturity_date = _date(mat_year, mat_month, mat_day)

            static.update({
                "loan_amount": loan_amount,
                "origination_fee": origination_fee,
                "note_rate": note_rate,
                "eir_rate": eir_rate,
                "effective_interest_rate": eir_rate,
                "yield_rate": eir_rate,
                "term_months": term_months,
                "status": inst_rng.choices(
                    ["Active", "Performing", "Closed", "Defaulted"],
                    weights=[65, 22, 8, 5]
                )[0],
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
                "product": inst_rng.choice(
                    ["Mortgage", "PersonalLoan", "CommercialLoan", "AutoLoan"]
                ),
            })
            for fname in ("origination_date", "issue_date",
                          "booking_date", "start_date"):
                if fname in field_names_lc:
                    static[fname] = origination_date.isoformat()
            for fname in ("maturity_date", "end_date", "term_end_date"):
                if fname in field_names_lc:
                    static[fname] = maturity_date.isoformat()

            # Amortising time-series
            monthly_principal = loan_amount / term_months
            monthly_fee_amort = origination_fee / term_months
            outstanding_s, amortized_s = [], []
            for pd_str in sorted_dates:
                pd_d = _parse_date(pd_str)
                m_elapsed = max(0,
                    (pd_d.year - origination_date.year) * 12
                    + pd_d.month - origination_date.month)
                bal = max(0.0, round(loan_amount - monthly_principal * m_elapsed, 2))
                amort = round(min(origination_fee,
                                  monthly_fee_amort * m_elapsed), 2)
                outstanding_s.append(bal)
                amortized_s.append(amort)

            for fname in ("outstanding_balance", "balance", "ead",
                          "exposure_at_default"):
                time_series[fname] = outstanding_s
            for fname in ("amortized_fee", "fee_amortized",
                          "amortization_amount"):
                time_series[fname] = amortized_s
            for fname in ("unamortized_fee", "net_fee", "deferred_fee"):
                time_series[fname] = [round(origination_fee - v, 2)
                                      for v in amortized_s]

        elif domain == "ifrs9":
            # ----------------------------------------------------------------
            # IFRS 9 / CECL — ECL measurement
            # Stage distribution: 60 % S1, 30 % S2, 10 % S3
            # ----------------------------------------------------------------
            stage = inst_rng.choices([1, 2, 3], weights=[60, 30, 10])[0]
            ead = round(inst_rng.uniform(10_000, 500_000), 2)

            if stage == 1:
                pd_val = round(inst_rng.uniform(0.0010, 0.0200), 6)
                dpd = inst_rng.randint(0, 29)
                credit_impaired = False
            elif stage == 2:
                pd_val = round(inst_rng.uniform(0.0200, 0.1500), 6)
                dpd = inst_rng.randint(30, 89)
                credit_impaired = False
            else:
                pd_val = round(inst_rng.uniform(0.20, 0.70), 6)
                dpd = inst_rng.randint(90, 365)
                credit_impaired = True

            lgd = round(inst_rng.uniform(0.30, 0.60), 6)
            collateral_value = round(ead * inst_rng.uniform(0.50, 1.50), 2)
            ecl_base = round(pd_val * lgd * ead, 2)
            rating = max(1, min(10, 11 - stage * 3 + inst_rng.randint(0, 2)))

            static.update({
                "stage": stage,
                "ead": ead,
                "outstanding_balance": ead,
                "balance": ead,
                "pd": pd_val,
                "lgd": lgd,
                "collateral_value": collateral_value,
                "credit_impaired": credit_impaired,
                "credit_impaired_flag": credit_impaired,
                "is_defaulted": credit_impaired,
                "in_default": credit_impaired,
                "rating": rating,
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
                "segment": inst_rng.choices(
                    ["Retail", "Corporate", "SME", "Sovereign"],
                    weights=[40, 30, 25, 5]
                )[0],
                "product": inst_rng.choice(
                    ["Mortgage", "PersonalLoan", "CommercialLoan", "AutoLoan"]
                ),
            })

            # ECL and DPD evolve modestly over time
            ecl_s, dpd_s = [], []
            for j in range(n_periods):
                drift = inst_rng.uniform(-0.04, 0.04)
                ecl_s.append(round(max(0, ecl_base * (1 + drift * (j + 1))), 2))
                dpd_s.append(max(0, dpd + j * inst_rng.randint(0, 4)))

            for fname in ("ecl", "expected_credit_loss",
                          "allowance", "impairment", "provision"):
                time_series[fname] = ecl_s
            for fname in ("days_past_due", "dpd", "days_overdue",
                          "delinquent_days"):
                time_series[fname] = dpd_s

        elif domain == "lease":
            # ----------------------------------------------------------------
            # IFRS 16 / ASC 842 — right-of-use asset & lease liability
            # ----------------------------------------------------------------
            rou_initial = _round_thousands(inst_rng.uniform(20_000, 400_000))
            ibr = round(inst_rng.uniform(0.030, 0.090), 6)
            lease_term = inst_rng.choice([12, 24, 36, 48, 60, 84, 120])
            monthly_rate = ibr / 12
            if monthly_rate > 0:
                pmt = rou_initial * monthly_rate / (1 - (1 + monthly_rate) ** -lease_term)
            else:
                pmt = rou_initial / lease_term
            pmt = round(pmt, 2)

            first_pd = _parse_date(sorted_dates[0])
            months_back = inst_rng.randint(1, min(24, lease_term - 1))
            ls_month = first_pd.month - (months_back % 12)
            ls_year = first_pd.year - (months_back // 12)
            if ls_month <= 0:
                ls_month += 12
                ls_year -= 1
            lease_start = _date(ls_year, ls_month, 1)

            le_total = lease_start.month + lease_term
            le_year = lease_start.year + (le_total - 1) // 12
            le_month = ((le_total - 1) % 12) + 1
            lease_end = _date(le_year, le_month,
                              _cal.monthrange(le_year, le_month)[1])

            static.update({
                "lease_payment": pmt,
                "monthly_rent": pmt,
                "annual_rent": round(pmt * 12, 2),
                "annual_lease": round(pmt * 12, 2),
                "lease_installment": pmt,
                "discount_rate": ibr,
                "incremental_borrowing_rate": ibr,
                "ibr": ibr,
                "lessee_rate": ibr,
                "lease_term": lease_term,
                "lease_period": lease_term,
                "lease_start_date": lease_start.isoformat(),
                "lease_end_date": lease_end.isoformat(),
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
            })

            rou_s, liability_s = [], []
            rou_monthly_depr = rou_initial / lease_term
            liability_running = rou_initial
            for j, pd_str in enumerate(sorted_dates):
                pd_d = _parse_date(pd_str)
                m_elapsed = max(0,
                    (pd_d.year - lease_start.year) * 12
                    + pd_d.month - lease_start.month)
                current_rou = max(0.0, round(
                    rou_initial - rou_monthly_depr * m_elapsed, 2))
                # Walk liability forward from initial
                liab = rou_initial
                for _ in range(m_elapsed):
                    interest = liab * monthly_rate
                    liab = max(0.0, liab - (pmt - interest))
                current_liab = round(liab, 2)
                rou_s.append(current_rou)
                liability_s.append(current_liab)

            for fname in ("rou_asset", "right_of_use", "rouasset",
                          "lease_asset", "rightofuse"):
                time_series[fname] = rou_s
            for fname in ("lease_liability", "lease_obligation",
                          "leaseLiability", "lease_balance"):
                time_series[fname] = liability_s

        elif domain == "fixed_asset":
            # ----------------------------------------------------------------
            # IAS 16 / ASC 360 — PP&E depreciation
            # ----------------------------------------------------------------
            cost = _round_thousands(inst_rng.uniform(10_000, 500_000))
            useful_life_years = inst_rng.choice([3, 5, 7, 10, 15, 20, 25, 40])
            residual_pct = inst_rng.uniform(0.05, 0.20)
            residual = round(cost * residual_pct, 2)
            depreciable = cost - residual
            annual_depr = round(depreciable / useful_life_years, 2)
            monthly_depr = round(annual_depr / 12, 2)

            first_pd = _parse_date(sorted_dates[0])
            months_back = inst_rng.randint(6, min(useful_life_years * 12 - 1, 60))
            acq_month = first_pd.month - (months_back % 12)
            acq_year = first_pd.year - (months_back // 12)
            if acq_month <= 0:
                acq_month += 12
                acq_year -= 1
            acq_date = _date(acq_year, acq_month,
                             _cal.monthrange(acq_year, acq_month)[1])

            static.update({
                "acquisition_cost": cost,
                "purchase_cost": cost,
                "gross_cost": cost,
                "historical_cost": cost,
                "asset_cost": cost,
                "residual_value": residual,
                "salvage_value": residual,
                "scrap_value": residual,
                "useful_life": useful_life_years,
                "useful_life_years": useful_life_years,
                "depreciation_charge": annual_depr,
                "annual_depreciation": annual_depr,
                "period_depreciation": monthly_depr,
                "depr_charge": annual_depr,
                "depr_amount": annual_depr,
                "acquisition_date": acq_date.isoformat(),
                "asset_type": inst_rng.choice(
                    ["Plant", "Equipment", "Machinery", "Vehicle", "Building"]
                ),
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
            })

            accum_s, nbv_s = [], []
            for pd_str in sorted_dates:
                pd_d = _parse_date(pd_str)
                m_elapsed = max(0,
                    (pd_d.year - acq_date.year) * 12
                    + pd_d.month - acq_date.month)
                accum = round(min(depreciable, monthly_depr * m_elapsed), 2)
                nbv_s.append(round(cost - accum, 2))
                accum_s.append(accum)

            for fname in ("accumulated_depreciation", "accum_depr"):
                time_series[fname] = accum_s
            for fname in ("nbv", "net_book_value", "carrying_value",
                          "book_value", "carrying_amount"):
                time_series[fname] = nbv_s

        elif domain == "revenue":
            # ----------------------------------------------------------------
            # IFRS 15 / ASC 606 — revenue recognition
            # ----------------------------------------------------------------
            contract_val = _round_thousands(inst_rng.uniform(5_000, 300_000))
            n_obligations = inst_rng.choice([1, 2, 3])
            alloc_pct = 1.0 / n_obligations
            allocated = round(contract_val * alloc_pct, 2)
            ssp_val = round(allocated * inst_rng.uniform(0.85, 1.15), 2)
            contract_duration = inst_rng.choice([12, 24, 36])

            first_pd = _parse_date(sorted_dates[0])
            months_back = inst_rng.randint(1, 18)
            cs_month = first_pd.month - (months_back % 12)
            cs_year = first_pd.year - (months_back // 12)
            if cs_month <= 0:
                cs_month += 12
                cs_year -= 1
            contract_start = _date(cs_year, cs_month, 1)

            ce_total = contract_start.month + contract_duration
            ce_year = contract_start.year + (ce_total - 1) // 12
            ce_month = ((ce_total - 1) % 12) + 1
            contract_end = _date(ce_year, ce_month,
                                 _cal.monthrange(ce_year, ce_month)[1])

            monthly_rev = round(contract_val / contract_duration, 2)

            static.update({
                "contract_amount": contract_val,
                "transaction_price": contract_val,
                "contract_value": contract_val,
                "revenue_amount": contract_val,
                "ssp": ssp_val,
                "standalone_selling_price": ssp_val,
                "sspprice": ssp_val,
                "allocated_amount": allocated,
                "allocated_revenue": allocated,
                "allocation": allocated,
                "contract_start_date": contract_start.isoformat(),
                "contract_end_date": contract_end.isoformat(),
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
            })

            recog_s, deferred_s = [], []
            for pd_str in sorted_dates:
                pd_d = _parse_date(pd_str)
                m_elapsed = max(0,
                    (pd_d.year - contract_start.year) * 12
                    + pd_d.month - contract_start.month)
                recognized = round(min(contract_val,
                                       monthly_rev * m_elapsed), 2)
                deferred_s.append(round(max(0, contract_val - recognized), 2))
                recog_s.append(recognized)

            for fname in ("recognized_revenue", "revenue_recognized"):
                time_series[fname] = recog_s
            for fname in ("period_revenue", "recognition_amount"):
                time_series[fname] = [monthly_rev] * n_periods
            for fname in ("deferred_revenue", "contract_asset",
                          "contract_liability"):
                time_series[fname] = deferred_s

        elif domain == "securities":
            # ----------------------------------------------------------------
            # Securities / Fair Value — bonds, SBO
            # ----------------------------------------------------------------
            face = float(inst_rng.choice(
                [1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]
            ))
            coupon = round(inst_rng.uniform(0.020, 0.080), 6)
            mkt_premium = inst_rng.uniform(-0.15, 0.15)
            mkt_val_base = round(face * (1 + mkt_premium), 2)
            book_val = round(face * inst_rng.uniform(0.92, 1.08), 2)

            first_pd = _parse_date(sorted_dates[0])
            days_back = inst_rng.randint(30, 1460)
            purchase_date = first_pd - _td(days=days_back)
            mat_years = inst_rng.choice([1, 2, 3, 5, 7, 10, 20, 30])
            mat_year = first_pd.year + mat_years
            mat_day = _cal.monthrange(mat_year, first_pd.month)[1]
            maturity_date = _date(mat_year, first_pd.month, mat_day)

            static.update({
                "face_value": face,
                "principal": face,
                "notional": face,
                "facevalue": face,
                "book_value": book_val,
                "coupon_rate": coupon,
                "yield": round(coupon + inst_rng.uniform(-0.010, 0.020), 6),
                "purchase_date": purchase_date.isoformat(),
                "maturity_date": maturity_date.isoformat(),
                "currency": inst_rng.choice(["USD", "EUR", "GBP"]),
                "product": inst_rng.choice(
                    ["GovernmentBond", "CorporateBond", "Treasury",
                     "MunicipalBond"]
                ),
            })

            daily_accrual = round(face * coupon / 365, 4)
            mkt_s, accrued_s = [], []
            mkt_running = mkt_val_base
            for j, pd_str in enumerate(sorted_dates):
                pd_d = _parse_date(pd_str)
                shock = inst_rng.uniform(-0.008, 0.008)
                mkt_running = round(max(face * 0.70, mkt_running * (1 + shock)), 2)
                mkt_s.append(mkt_running)
                days_since_purchase = (pd_d - purchase_date).days % 180
                accrued_s.append(round(daily_accrual * max(0, days_since_purchase), 2))

            for fname in ("market_value", "fair_value", "mtm"):
                time_series[fname] = mkt_s
            for fname in ("accrued_interest",):
                time_series[fname] = accrued_s

        profiles[inst_id] = {"static": static, "time_series": time_series}

    return {"domain": domain, "profiles": profiles, "sorted_dates": sorted_dates}


def _make_sample_rows(
    event_def: dict,
    instrument_ids: list[str],
    posting_dates: list[str],
    field_hints: dict | None = None,
    seed: int = 42,
    reference_constraints: dict[str, list] | None = None,
) -> list[dict]:
    """Generate synthetic rows for *event_def*.

    Priority for each field value:
      1. **Reference constraint** — if an already-stored reference event
         contains the same field name, pick from its actual distinct values
         (cycling deterministically over the list so every value is used).
         This guarantees that e.g. ``product_type`` in an activity event only
         ever contains values that exist in the PRODUCT_CATALOG reference table.
      2. **Accounting-domain profile time-series** — amortising balances,
         accumulating depreciation, etc.
      3. **Accounting-domain profile static fields** — loan amount, rate, dates.
      4. **Name-based heuristic generator** — fallback for any custom field.
    """
    rng = random.Random(seed)
    rows: list[dict] = []
    is_reference = event_def.get("eventType") == "reference"
    ref_constraints = reference_constraints or {}

    # Build coherent per-instrument profiles for accounting domains
    profile_data = _generate_instrument_profiles(
        event_def, instrument_ids, posting_dates, seed
    )
    profiles = profile_data["profiles"]
    sorted_dates = profile_data["sorted_dates"]
    date_index = {d: i for i, d in enumerate(sorted_dates)}

    # Pre-compute per-field reference value cycle so that across all rows
    # we rotate through the available reference values, not just always
    # picking index 0. Each field gets its own counter.
    _ref_cycle_counter: dict[str, int] = {}

    for posting_date in posting_dates:
        pd_idx = date_index.get(posting_date, 0)
        for inst in instrument_ids:
            row: dict[str, Any] = {}
            if not is_reference:
                row["postingdate"] = posting_date
                row["effectivedate"] = posting_date
                row["instrumentid"] = inst

            profile = profiles.get(inst, {})
            static = profile.get("static", {})
            time_series = profile.get("time_series", {})

            for f in event_def.get("fields", []):
                fname = f.get("name", "")
                fname_lc = fname.lower()

                # 1. Reference constraint — exact field match from reference data
                if fname in ref_constraints and ref_constraints[fname]:
                    choices = ref_constraints[fname]
                    idx = _ref_cycle_counter.get(fname, 0)
                    row[fname] = choices[idx % len(choices)]
                    _ref_cycle_counter[fname] = idx + 1
                    continue

                # 2. Profile time-series (accounting-domain coherence)
                if fname_lc in time_series:
                    series = time_series[fname_lc]
                    if pd_idx < len(series):
                        row[fname] = series[pd_idx]
                        continue

                # 3. Profile static values
                if fname_lc in static:
                    row[fname] = static[fname_lc]
                    continue

                # 4. Heuristic fallback for custom / non-standard fields
                row[fname] = _generate_value(f, rng, field_hints or {})

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
# side, we can suggest the other to keep the transaction pair complete.
# NOTE: these are TRANSACTIONS emitted for downstream journal posting —
# this app does NOT create journal entries directly.
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
            suggestions.append(f"'{a}' is registered but '{b}' is not — a complete transaction pair usually needs both (downstream system uses these to post journals)")
        if lb in have and la not in have:
            suggestions.append(f"'{b}' is registered but '{a}' is not — a complete transaction pair usually needs both (downstream system uses these to post journals)")
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

        # ── Reference-table auto-coerce ──────────────────────────────────
        # If the event name strongly suggests a static lookup / reference
        # table (catalog, ref, lookup, master, mapping, …) but the caller
        # forgot to set eventType='reference' / eventTable='custom', fix
        # it automatically and record it in `coerced_to_reference` so the
        # caller knows.
        _REF_KEYWORDS = {
            "catalog", "catalogue", "catalogue", "ref", "reference",
            "lookup", "lookup_table", "lut", "master", "mapping",
            "static", "rate_table", "rates_table", "ssptable", "ssp_table",
            "product_table", "product_list", "chart_of_accounts",
            "coa", "tariff", "schedule_of_rates",
        }
        _name_lower = event_name.lower()
        _name_parts = set(re.split(r"[_\-\s]", _name_lower))
        _is_ref_by_name = bool(_name_parts & _REF_KEYWORDS or
                               any(_name_lower.endswith(k) for k in _REF_KEYWORDS) or
                               any(_name_lower.startswith(k) for k in _REF_KEYWORDS))
        _coerced = False
        if event_type == "reference" and event_table != "custom":
            # Hard rule: reference type MUST use custom table.
            event_table = "custom"
            _coerced = True
        elif event_type == "activity" and event_table == "standard" and _is_ref_by_name:
            # Agent forgot to label a reference table — fix silently.
            event_type = "reference"
            event_table = "custom"
            _coerced = True
        # ── End auto-coerce ──────────────────────────────────────────────

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
        entry: dict = {
            "event_name": event_name, "fields": norm_fields,
            "eventType": event_type, "eventTable": event_table,
        }
        if _coerced:
            entry["coerced_to_reference"] = True
            entry["coercion_note"] = (
                f"Auto-set eventType='reference' and eventTable='custom' for "
                f"'{event_name}'. Reference/lookup tables MUST use these settings "
                f"so the engine loads them as static tables, not activity streams."
            )
        # Warn when field names don't match known accounting-standard conventions
        _field_names = [f["name"] for f in norm_fields]
        _acct_hint = _check_accounting_field_hints(event_name, _field_names)
        if _acct_hint:
            entry["accounting_field_hint"] = _acct_hint
        created.append(entry)

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


# ---------------------------------------------------------------------------
# Reference-event cross-seeding helpers
# ---------------------------------------------------------------------------

async def _load_all_reference_data() -> dict[str, list[dict]]:
    """Return {event_name: [rows]} for every stored reference event.

    Only events whose definition has eventType='reference' are included.
    Both DB and in-memory stores are consulted.
    """
    db = _ServerBridge.db
    ref_names: set[str] = set()

    # Collect names of all reference event definitions
    try:
        if db is not None:
            async for d in db.event_definitions.find({"eventType": "reference"}, {"_id": 0, "event_name": 1}):
                nm = d.get("event_name")
                if nm:
                    ref_names.add(str(nm).lower())
    except Exception:
        pass
    for d in (_ServerBridge.in_memory_data or {}).get("event_definitions") or []:
        if d.get("eventType") == "reference":
            nm = d.get("event_name")
            if nm:
                ref_names.add(str(nm).lower())

    if not ref_names:
        return {}

    result: dict[str, list[dict]] = {}

    # Load stored rows for each reference event
    try:
        if db is not None:
            async for d in db.event_data.find({}, {"_id": 0, "event_name": 1, "data_rows": 1}):
                nm = str(d.get("event_name") or "").lower()
                if nm in ref_names:
                    result[nm] = d.get("data_rows") or []
    except Exception:
        pass
    for d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
        nm = str(d.get("event_name") or "").lower()
        if nm in ref_names and nm not in result:
            result[nm] = d.get("data_rows") or []

    return result


def _build_reference_constraints(
    event_def: dict,
    reference_data: dict[str, list[dict]],
) -> dict[str, list]:
    """For each field on *event_def*, search all reference-event rows and
    collect the distinct values already stored for that field name.

    Returns ``{field_name_original_case: [distinct_values]}`` for fields
    where matching reference data was found.  The returned list preserves
    the insertion order of distinct values (first occurrence wins).

    Priority logic:
    - Exact field-name match (case-insensitive) against any reference event's
      rows wins outright.
    - An empty value list in the reference data is ignored (not yet generated).
    """
    constraints: dict[str, list] = {}
    if not reference_data:
        return constraints

    for f in event_def.get("fields") or []:
        fname = f.get("name") or ""
        fname_lc = fname.lower()
        if not fname_lc:
            continue

        distinct: list = []
        seen_vals: set = set()

        for _ref_rows in reference_data.values():
            for row in _ref_rows:
                # Match field by exact name (case-insensitive)
                for rk, rv in row.items():
                    if rk.lower() == fname_lc:
                        key = str(rv)  # hashable repr for dedup
                        if key not in seen_vals and rv is not None:
                            seen_vals.add(key)
                            distinct.append(rv)
                        break  # found in this row

        if distinct:
            constraints[fname] = distinct

    return constraints


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

    # Load reference-event data and build field-level constraints so that
    # activity-event fields whose names match a reference-event field are
    # populated exclusively from the values that already exist in the
    # reference data — never independently mocked.
    # For *reference* events being generated now, no constraints apply
    # (they ARE the source of truth).
    ref_constraints: dict[str, list] = {}
    if event_def.get("eventType") != "reference":
        try:
            ref_data = await _load_all_reference_data()
            ref_constraints = _build_reference_constraints(event_def, ref_data)
        except Exception as exc:
            logger.debug("Reference constraint load failed (non-fatal): %s", exc)

    new_rows = _make_sample_rows(
        event_def, instrument_ids, posting_dates, field_hints, seed,
        reference_constraints=ref_constraints,
    )

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
        "reference_seeded_fields": {
            fname: vals[:5]  # show up to 5 sample reference values
            for fname, vals in ref_constraints.items()
        } if ref_constraints else {},
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
# Boolean / null literal coercion.
# Weak / non-Python-trained models (gpt-5-mini, deepseek-chat, …) routinely
# emit JS-style `true` / `false` / `null` inside DSL formulas. The validator
# then surfaces those as `undefined name`, the agent loops, and the run
# halts with a "what's the boolean syntax?" question to the user. Auto-fix
# them here so the rule saves AND so test_schedule_step / dry_run succeed.
# Only standalone identifiers OUTSIDE string literals are rewritten — text
# inside `'…'` / `"…"` and substrings of other identifiers (e.g. `truearg`)
# are preserved.
# Even-weaker models also emit Excel/VBA `iif(cond,a,b)` instead of the DSL
# `if(cond,a,b)`. Auto-coerce that too so the model doesn't loop on it.
# ──────────────────────────────────────────────────────────────────────────
_BOOLEAN_LITERAL_MAP = {
    "true": "True", "false": "False",
    "null": "None", "nil": "None", "undefined": "None",
}
_BOOLEAN_TOKEN_RE = re.compile(r"\b(true|false|null|nil|undefined)\b")
_STRING_LITERAL_RE = re.compile(r"('([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\")")
# iif(…) is Excel/VBA syntax — DSL uses if(…)
_IIF_RE = re.compile(r"\biif\s*\(", re.IGNORECASE)


def _coerce_lower_booleans(text):
    """Return `text` with lowercase boolean/null literals and Excel-style
    `iif()` coerced to the DSL equivalents. Preserves all string literals
    byte-for-byte. Idempotent."""
    if not isinstance(text, str) or not text:
        return text
    # iif(…) → if(…) first (simple prefix replace, safe globally)
    text = _IIF_RE.sub("if(", text)
    parts: list[str] = []
    last = 0
    for m in _STRING_LITERAL_RE.finditer(text):
        seg = text[last:m.start()]
        parts.append(_BOOLEAN_TOKEN_RE.sub(
            lambda mm: _BOOLEAN_LITERAL_MAP[mm.group(1).lower()], seg
        ))
        parts.append(m.group(0))
        last = m.end()
    tail = text[last:]
    parts.append(_BOOLEAN_TOKEN_RE.sub(
        lambda mm: _BOOLEAN_LITERAL_MAP[mm.group(1).lower()], tail
    ))
    return "".join(parts)


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
    # Catch attribute access on a schedule output variable, e.g.
    # `DepreciationSchedule.depreciation_charge` or `result.Schedule`.
    # Schedule outputs are LIST objects; column values must be accessed via
    # schedule_sum / schedule_first / schedule_last / schedule_column /
    # schedule_filter, NOT via dot-attribute notation.
    (re.compile(r'\b[A-Za-z_]\w*\.(?:Schedule|schedule|columns?|rows?|data|results?|output)\b'),
        "Dot-attribute access on a schedule variable is not valid. "
        "Schedule outputs are lists — extract column values using "
        "schedule_sum(StepName, 'col'), schedule_last(StepName, 'col'), "
        "schedule_first(StepName, 'col'), or schedule_column(StepName, 'col'). "
        "Replace `result.columnName` → `schedule_sum(ScheduleStepName, 'columnName')`."),
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
    # Curly braces in a formula are ALWAYS a mistake — the DSL has no dict /
    # set literals and no f-strings. Weak models occasionally emit
    # `if(cond, {field: value}, ...)` or `f"{x}"` which trips the Python
    # tokenizer with a cryptic "closing parenthesis '}' does not match
    # opening parenthesis '('" message at code-gen time. Reject up front.
    if "{" in expr or "}" in expr:
        raise ToolError(
            f"In {where}: curly braces `{{` `}}` are not allowed in DSL "
            f"expressions. The DSL has NO dict/set literals and NO f-strings. "
            f"For multi-branch logic use stepType='condition'. For string "
            f"concatenation use the `concat(a, b, ...)` DSL function. "
            f"Got: {expr[:160]!r}"
        )
    # Sanity: balanced parentheses (the actual culprit behind the cryptic
    # tokenizer error in the screenshot the user reported). We do this in a
    # token-aware way that ignores parens inside string literals.
    depth = 0
    in_str: str | None = None
    i = 0
    while i < len(expr):
        ch = expr[i]
        if in_str is not None:
            if ch == "\\" and i + 1 < len(expr):
                i += 2
                continue
            if ch == in_str:
                in_str = None
        else:
            if ch in ('"', "'"):
                in_str = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    raise ToolError(
                        f"In {where}: unbalanced parentheses — extra `)` at "
                        f"position {i}. Got: {expr[:160]!r}"
                    )
        i += 1
    if depth > 0:
        raise ToolError(
            f"In {where}: unbalanced parentheses — {depth} unclosed `(`. "
            f"Got: {expr[:160]!r}"
        )
    if in_str is not None:
        raise ToolError(
            f"In {where}: unterminated string literal (missing closing "
            f"`{in_str}`). Got: {expr[:160]!r}"
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
        # Special case: schedule-column-only builtins used outside a
        # schedule step.  Give a targeted, actionable error instead of
        # the useless "Did you mean: avg?" suggestion.
        if fn in _SCHEDULE_COLUMN_BUILTINS:
            if fn == "lag":
                raise ToolError(
                    f"In {where}: `lag(...)` is ONLY valid inside a "
                    f"schedule step's column formula. It cannot be used in "
                    f"a calc/condition/iteration step.\n"
                    f"CORRECT PATTERN: put lag() INSIDE scheduleConfig.columns "
                    f"to read a prior period's value, e.g.:\n"
                    f"  opening_nbv column formula: "
                    f"  lag('closing_nbv', 1, <starting_value>)\n"
                    f"where <starting_value> is a contextVar or literal that "
                    f"seeds the first period.\n"
                    f"To extract the FINAL period's value INTO a calc step, "
                    f"use: schedule_last(ScheduleStepName, 'closing_nbv')"
                )
            raise ToolError(
                f"In {where}: `{fn}(...)` is a schedule-column built-in "
                f"that is ONLY available inside a schedule step's column "
                f"formula — it cannot be used in a calc/condition/iteration "
                f"step. Move this logic into a schedule step's column "
                f"definition, or use a schedule accessor function like "
                f"schedule_sum / schedule_last / schedule_first to extract "
                f"a column value from the schedule output."
            )
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

    # GUARD: if the caller passed substantial inline DSL code, this is almost
    # always a mistake — the correct flow is to build the rule with
    # create_saved_rule, then call attach_rules_to_template or
    # assemble_template_from_rules passing rule_ids.  Writing inline DSL
    # by hand almost always produces syntax errors at dry_run time.
    # Allow a short placeholder (up to ~200 chars) but block large hand-
    # written DSL with an actionable error.
    if len(dsl_code.strip()) > 200:
        raise ToolError(
            "create_or_replace_template was called with a large inline "
            "`dsl_code` string. This is the WRONG approach and nearly always "
            "produces a syntax error when dry_run_template is called.\n\n"
            "CORRECT WORKFLOW:\n"
            "  1. create_or_replace_template with event_name only (leave "
            "     dsl_code empty or omit it) to register the template shell.\n"
            "  2. attach_rules_to_template (or assemble_template_from_rules) "
            "     passing rule_ids=[<id of the rule you just created>] to "
            "     populate the template from the saved rule's generatedCode.\n"
            "  3. Call dry_run_template to verify.\n\n"
            "Do NOT write DSL code by hand here — use the rule builder "
            "(create_saved_rule / add_step_to_rule) instead."
        )

    event = await _find_event_def(event_name)
    if not event:
        raise ToolError(f"Event '{event_name}' not found")

    # When dsl_code is empty this is a shell-creation call (correct usage).
    # Skip translation — attach_rules_to_template will populate the code later.
    if not dsl_code.strip():
        python_code = "# template shell — populated by attach_rules_to_template\n_noop = 0\n"
    else:
        _enforce_dsl_guardrails(dsl_code)
        # Translate using the same path as save_template
        dsl_to_python = _h("dsl_to_python")
        try:
            python_code = dsl_to_python(dsl_code, event["fields"])
        except Exception as exc:
            raise ToolError(f"DSL translation failed: {exc}") from exc

        # Compile-check the generated Python immediately so syntax errors are
        # caught here (in the same turn) rather than surfacing at dry_run_template
        # one turn later.
        try:
            compile(python_code, "<dsl_template_validate>", "exec")
        except SyntaxError as se:
            raise ToolError(
                f"Template '{name}': DSL translated to Python but the result "
                f"has a syntax error at line {se.lineno}: {se.msg}.\n"
                f"Do NOT write inline DSL by hand in dsl_code — build the "
                f"logic via create_saved_rule then call "
                f"attach_rules_to_template / assemble_template_from_rules "
                f"to populate the template from the saved rule."
            ) from se

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
    # Check for transaction imbalance per instrument (debit total should equal credit total
    # so the downstream journal-posting system receives balanced input)
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
            f"debit/credit transaction totals are unequal "
            f"for {len(unbalanced)} instrument(s) — the downstream journal-posting system expects balanced input"
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


def _validate_schedule_step_shape(name: str, sc: dict, outputVars: list,
                                  context_var_names: list | None = None) -> tuple[dict, list]:
    """Deep validate a stepType='schedule' configuration. Mirrors the
    field-by-field rules of ScheduleStepModal.js so the agent cannot save a
    schedule that the visual modal would reject. Auto-derives contextVars
    from the column formulas (the modal's `autoDetectedVars` useMemo) so the
    agent never has to remember to populate it.

    Returns the (possibly amended) (scheduleConfig, outputVars) tuple.
    """
    if not isinstance(sc, dict):
        raise ToolError(f"step '{name}': scheduleConfig must be an object")

    # Auto-coerce periodType: if the agent set startDate* or endDate* keys it
    # intends date-based scheduling even if it forgot to flip periodType.
    # (Common after a failed patch where the periodType op was in the dropped
    # batch but only the corrected field was re-sent on retry.)
    _DATE_CONFIG_KEYS = {
        "startDateSource", "startDateField", "startDateFormula",
        "endDateSource", "endDateField", "endDateFormula",
    }
    if (sc.get("periodType", "number") or "number") == "number" and any(
        sc.get(k) for k in _DATE_CONFIG_KEYS
    ):
        sc["periodType"] = "date"

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
                # ── Auto-heal missing start/end date ──────────────────────
                # Priority:
                #  1. Look for a date-like calc-step variable in scope
                #     (e.g. a step that reads EVENT.startdate / postingdate).
                #  2. Look for a date-like event field in contextVars.
                #  3. Final fallback hard-coded:
                #       startDate  → formula "postingdate"
                #       endDate    → formula "add_years(postingdate, 1)"
                # This prevents saving a schedule with blank time bounds, which
                # would silently produce zero rows at runtime.
                all_vars = list(sc.get("contextVars") or []) + list(
                    (context_var_names or [])
                )
                # Narrow to names that smell like dates.
                _DATE_RE = re.compile(
                    r"date|start|end|from|until|acquisition|inception|origination"
                    r"|maturity|effective|posting",
                    re.IGNORECASE,
                )
                date_like = [cv for cv in all_vars if _DATE_RE.search(cv)]
                # Pick the most relevant match by prefix hint.
                if prefix == "startDate":
                    _preferred = [v for v in date_like if re.search(r"start|origination|inception|acquisition|effective|posting", v, re.IGNORECASE)]
                    chosen_var = (_preferred or date_like or [None])[0]
                    fallback_formula = "postingdate"
                else:  # endDate
                    _preferred = [v for v in date_like if re.search(r"end|maturity|expir|until", v, re.IGNORECASE)]
                    chosen_var = (_preferred or date_like or [None])[0]
                    fallback_formula = "add_years(postingdate, 1)"

                if chosen_var:
                    # Promote the found variable to a formula source.
                    sc[f"{prefix}Source"] = "formula"
                    sc[f"{prefix}Formula"] = chosen_var
                    src = "formula"
                    sc.setdefault("_autohealed", []).append(
                        f"{prefix}: no value supplied — auto-set to formula "
                        f"'{chosen_var}' (found in scope; review and adjust if "
                        f"this is not the correct date field)."
                    )
                else:
                    # Hard fallback: postingdate / add_years(postingdate, 1)
                    sc[f"{prefix}Source"] = "formula"
                    sc[f"{prefix}Formula"] = fallback_formula
                    src = "formula"
                    sc.setdefault("_autohealed", []).append(
                        f"{prefix}: no value supplied and no date variable found "
                        f"in scope — auto-set to formula '{fallback_formula}'. "
                        f"Review: if your event has a dedicated start/end date "
                        f"field, add a calc step to read it and set "
                        f"{prefix}Source='formula', {prefix}Formula='<that step name>'."
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
    # C9: Auto-prepend a `period_date` column for date-range schedules when
    # the agent forgot it. Every working template has this as col 0; absence
    # makes downstream filter outputVars silently break. Emit a hint so the
    # agent learns.
    if (
        period_type == "date"
        and cols
        and not any((c.get("name") or "").strip() == "period_date" for c in cols)
    ):
        cols = [{"name": "period_date", "formula": "period_date"}] + list(cols)
        sc["columns"] = cols
        sc.setdefault("_auto_inserted", []).append("period_date")
    if not cols:
        errs.append(
            "scheduleConfig.columns is empty. A schedule must have at "
            f"least one column. Each column needs {{name, formula}}. "
            f"Built-in identifiers available inside column formulas: "
            f"{sorted(_SCHEDULE_COLUMN_BUILTINS)}."
        )
    seen_col_names: set[str] = set()
    # C7: track which columns are likely numeric (every formula returns a
    # number). Used to reject `lag('col', n, '')` defaults on numeric cols.
    _numeric_col_hints: set[str] = set()
    _NUMERIC_FN_HINTS = {
        "add", "subtract", "multiply", "divide", "power", "pow",
        "abs", "sign", "round", "floor", "ceil", "truncate", "percentage",
        "sum", "avg", "min", "max", "count", "median", "std_dev",
        "pmt", "pv", "fv", "rate", "nper", "npv", "irr", "xnpv", "xirr",
        "discount_factor", "accumulation_factor", "effective_rate",
        "nominal_rate", "yield_to_maturity", "days_between", "months_between",
        "years_between", "day_count_fraction", "days_in_year", "quarter",
        "day_of_week", "lag", "weighted_avg", "cumulative_sum", "to_number",
    }
    _LAG_CALL_RE = re.compile(
        r"\blag\s*\(\s*['\"]([A-Za-z_]\w*)['\"]\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)"
    )
    # C6: column-dependency-graph errors aggregated separately so we can show
    # them as a single block (most informative for the agent to fix in one go).
    _dep_errors: list[str] = []
    _known_dsl_fns = _known_dsl_function_names()
    for c in cols:
        cname = (c.get("name") or "").strip()
        formula = _coerce_lower_booleans((c.get("formula") or "").strip())
        c["formula"] = formula
        if not cname or not formula:
            errs.append(
                "every schedule column needs a non-empty name + formula. "
                f"Got name={cname!r}, formula={formula!r}."
            )
            continue
        if cname in seen_col_names:
            errs.append(f"duplicate schedule column name '{cname}'.")
            continue
        try:
            _enforce_dsl_guardrails(formula)
            _check_formula_expression(
                formula,
                where=f"step '{name}'.scheduleConfig.columns['{cname}'].formula",
            )
        except ToolError as e:
            errs.append(str(e))
        # ── C6: dependency-graph check ─────────────────────────────────
        # Each identifier referenced must be: a built-in, a DSL function,
        # the column itself (only via lag), the step's own variable, OR a
        # column DEFINED ABOVE this one. Forward references (column N
        # referring to column N+k) silently produced None at runtime.
        ids_in_formula = set(_IDENT_FOR_CTX_RE.findall(formula))
        # Strip lag('xxx',…) string-literal column names — they are dynamic
        # references handled by the schedule engine and may target THIS
        # column (recursive lag is allowed). Add them to a separate set.
        lag_cols = set(re.findall(r"\blag\s*\(\s*['\"]([A-Za-z_]\w*)['\"]", formula))
        for ident in ids_in_formula:
            base = ident[:-5] if ident.endswith("_full") else ident
            if not base:
                continue
            if base in _VALIDATOR_BUILTINS:
                continue
            if base in _SCHEDULE_COLUMN_BUILTINS:
                continue
            if base in _known_dsl_fns:
                continue
            if base in seen_col_names:
                continue   # column defined above — OK
            if base == cname:
                continue   # self-reference (only legal via lag — engine handles)
            if base == name:
                continue   # the step's own assignment name
            if base.isdigit() or base in {"True", "False", "None"}:
                continue
            # Anything else is presumed to be an outer-scope context var
            # (the auto-derived contextVars block below picks it up). We
            # only emit a hard dep error when the identifier IS the name
            # of a column defined LATER (forward reference).
        # Forward-reference: identifier matches a column name that hasn't
        # been seen yet (i.e. defined LATER in the columns array).
        future_cols = {
            (cc.get("name") or "").strip()
            for cc in cols
            if (cc.get("name") or "").strip() not in seen_col_names
            and (cc.get("name") or "").strip() != cname
        }
        forward_refs = (ids_in_formula & future_cols)
        # lag('col', n, default) reads the PRIOR period's value of 'col', so
        # it can legally reference ANY column — including ones defined later in
        # the array. Remove lag targets from forward_refs so the canonical
        # reducing-balance pattern `opening_nbv = lag('closing_nbv', 1, seed)`
        # is NOT flagged even when closing_nbv is defined after opening_nbv.
        forward_refs -= lag_cols
        if forward_refs:
            # Build a concrete, copy-pasteable lag() example for the FIRST
            # forward reference. Generic "use lag()" advice gets ignored;
            # showing the exact replacement string usually unblocks the
            # model in one shot. The most common case (depreciation roll-
            # forward) is opening_X = closing_X from prior period \u2192
            # `lag('closing_X', 1, <starting value>)`.
            first_fwd = sorted(forward_refs)[0]
            example = (
                f"\n  EXAMPLE FIX for column '{cname}': replace any "
                f"reference to '{first_fwd}' with "
                f"`lag('{first_fwd}', 1, <starting_value>)` \u2014 e.g. for "
                f"a reducing-balance depreciation schedule, "
                f"`opening_nbv` should be defined as "
                f"`lag('closing_nbv', 1, opening_net_carrying_amount)` "
                f"where `opening_net_carrying_amount` is the calc-step "
                f"variable holding the asset's starting NBV. The lag() "
                f"call lets a column read the PRIOR period's value of "
                f"any other column (including ones defined later), which "
                f"is the canonical pattern for recursive schedules."
            )
            _dep_errors.append(
                f"column '{cname}' references column(s) "
                f"{sorted(forward_refs)} that are defined LATER in the "
                f"schedule. Reorder the columns so each one only references "
                f"columns above it (or use lag('col',1,default) to read the "
                f"prior period's value of any column).{example}"
            )
        # ── C7: lag default-type check ─────────────────────────────────
        # If this column's formula is clearly numeric (uses arithmetic ops
        # or numeric DSL functions only), then any lag(...) default in any
        # other column targeting THIS column must also be numeric.
        is_numeric = bool(
            re.search(r"[+\-*/]|\b(?:" + "|".join(_NUMERIC_FN_HINTS) + r")\s*\(", formula)
            or re.fullmatch(r"-?\d+(?:\.\d+)?", formula.strip())
        )
        if is_numeric:
            _numeric_col_hints.add(cname)
        # Inspect lag(...) calls inside this formula for type mismatches.
        for m in _LAG_CALL_RE.finditer(formula):
            target_col, _n_expr, default_expr = m.group(1), m.group(2), m.group(3).strip()
            # Default '' against a numeric column → almost always wrong.
            if default_expr in ("''", '""') and target_col in _numeric_col_hints:
                _dep_errors.append(
                    f"column '{cname}' uses lag('{target_col}', …, '') with an "
                    f"empty-string default, but '{target_col}' is numeric. "
                    f"Use 0 (or another numeric default) so the first period "
                    f"doesn't blow up arithmetic."
                )
            # Numeric default against a date-like column (heuristic: column
            # formula contains a date function).
            elif default_expr in ("0", "0.0") and target_col in seen_col_names:
                target_formula = next(
                    (cc.get("formula") or "")
                    for cc in cols if (cc.get("name") or "").strip() == target_col
                )
                if re.search(r"\b(?:end_of_month|start_of_month|add_days|add_months|add_years|period_date)\b", target_formula):
                    _dep_errors.append(
                        f"column '{cname}' uses lag('{target_col}', …, 0) with a "
                        f"numeric default, but '{target_col}' is date-typed. "
                        f"Pass a date default (e.g. start_date or period_date)."
                    )
        seen_col_names.add(cname)
    if _dep_errors:
        errs.extend(_dep_errors)

    # Auto-derive contextVars: any identifier used in a column formula that
    # is NOT a built-in, NOT a DSL function name, and NOT another column
    # name must be a context variable from outer scope. The schedule engine
    # also exposes each context array as `<name>_full`, so strip that suffix
    # before resolving. Mirrors ScheduleStepModal autoDetectedVars.
    #
    # IMPORTANT: We deliberately IGNORE any contextVars the agent declared.
    # Weak models often dump bare event-field names (e.g. `revaluation_date`)
    # into contextVars even when the column formulas reference them via the
    # dotted form (`MyEvent.revaluation_date`). Trusting the declared list
    # caused a degenerate loop where every retry re-introduced the same
    # bogus bare names and the static validator kept rejecting them. The
    # only contextVars that should survive are those actually referenced
    # as bare identifiers in some column formula.
    dsl_fn_names = _known_dsl_function_names()
    derived_ctx: set[str] = set()
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
            entry["matchValue"] = _coerce_lower_booleans(mv)
        norm_outs.append(entry)

    if errs:
        raise ToolError(
            f"step '{name}': {len(errs)} schedule-config error(s):\n  - "
            + "\n  - ".join(errs)
        )

    # ── Auto-generate outputVars when the agent omitted them ──────────────
    # A schedule step with NO outputVars is invisible to downstream steps and
    # to the frontend modal — none of the computed columns can be referenced.
    # We auto-generate at least one sensible outputVar so the step is always
    # immediately usable.  Priority:
    #   1. filter type  (→ schedule_filter match on period_date = postingdate)
    #      — preferred for date-range schedules; picks the first non-date /
    #      non-index value column.
    #   2. last  type   (→ schedule_last)
    #      — preferred for number-period schedules; picks the first value col.
    #   3. sum   type   (→ schedule_sum) as a final fallback.
    if not norm_outs and seen_col_names:
        _NON_VALUE_COLS = {
            "period_date", "period_index", "period_number", "period_start",
            "s_no", "index", "item_name", "subinstrument_id",
        }
        # Value columns: anything that isn't a built-in bookkeeping column.
        _value_cols = [
            c for c in (cc.get("name", "") for cc in (sc.get("columns") or []))
            if c and c not in _NON_VALUE_COLS
        ]
        # If all columns are bookkeeping columns, fall back to the last one.
        if not _value_cols:
            _value_cols = [
                c for c in (cc.get("name", "") for cc in (sc.get("columns") or []))
                if c
            ]
        _primary_col = _value_cols[0] if _value_cols else next(iter(seen_col_names))
        _safe_name = re.sub(r"[^a-z0-9_]", "_", name.lower())

        _has_period_date = "period_date" in seen_col_names
        if period_type == "date" and _has_period_date:
            # filter: match the row where period_date equals the posting date
            norm_outs.append({
                "name": f"{_safe_name}_current",
                "type": "filter",
                "column": _primary_col,
                "matchCol": "period_date",
                "matchValue": "postingdate",
            })
            # Also emit a last var for the terminal/closing value
            if len(_value_cols) > 0:
                norm_outs.append({
                    "name": f"{_safe_name}_last",
                    "type": "last",
                    "column": _primary_col,
                })
        else:
            # number-period schedule: last + sum are most useful
            norm_outs.append({
                "name": f"{_safe_name}_last",
                "type": "last",
                "column": _primary_col,
            })
            norm_outs.append({
                "name": f"{_safe_name}_total",
                "type": "sum",
                "column": _primary_col,
            })
        sc["_auto_generated_outputVars"] = True

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
    # Immutable step_id: assigned once, preserved across renames so
    # update_step / delete_step / patch_step can target a step by id even
    # after the agent renames it. _resolve_step_index prefers this over
    # step_name. The id round-trips through the rule document.
    _existing_id = step.get("id") or step.get("step_id")
    if _existing_id and isinstance(_existing_id, str) and _existing_id.strip():
        _step_id = _existing_id.strip()
    else:
        _step_id = str(uuid.uuid4())
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
    # HARD-BLOCK: never create a calc step named 'instrumentid'.
    # instrumentid is always available as an implicit global on every row —
    # creating a step for it is redundant noise that clutters the rule.
    # Transactions needing instrumentid reference it directly via the global.
    if name.strip().lower() == "instrumentid":
        raise ToolError(
            "Creating a calc step named 'instrumentid' is FORBIDDEN. "
            "'instrumentid' is already an implicit global available on every "
            "rule row — no step is needed. Simply use it by name wherever you "
            "need it (e.g. in a formula or as a transaction field).\n"
            "If you need the instrument identifier in a transaction entry, the "
            "engine injects it automatically. Delete this step."
        )
    # GUIDANCE: a 'subinstrumentid' step IS expected, but its source must
    # reflect whether the event has scalar (one sub-id per instrument) or
    # non-scalar (multiple sub-ids per instrument) data.
    if name.strip().lower() == "subinstrumentid":
        src_hint = (step.get("source") or "").strip().lower()
        ef = (step.get("eventField") or "").strip()
        ct = (step.get("collectType") or "").strip().lower()
        # If the agent tried to make this a formula step or left it as a
        # plain value step, coerce it to the correct shape with a clear error.
        if src_hint not in ("event_field", "collect", ""):
            raise ToolError(
                "Step 'subinstrumentid' must use source='event_field' (when each "
                "instrument has exactly ONE subinstrumentid per posting date — "
                "scalar) or source='collect' + collectType='collect_by_instrument' "
                "(when an instrument has MULTIPLE subinstrumentids on the same "
                "posting date — non-scalar).\n"
                f"You supplied source='{src_hint}'. Fix:\n"
                "  SCALAR:     {name:'subinstrumentid', stepType:'calc', source:'event_field', eventField:'EVENTNAME.subinstrumentid'}\n"
                "  NON-SCALAR: {name:'subinstrumentid', stepType:'calc', source:'collect', collectType:'collect_by_instrument', eventField:'EVENTNAME.subinstrumentid'}"
            )
        # If source is collect but collectType is not collect_by_instrument, fix it.
        if src_hint == "collect" and ct and ct != "collect_by_instrument":
            raise ToolError(
                f"Step 'subinstrumentid' with source='collect' must use "
                f"collectType='collect_by_instrument', not '{ct}'. "
                f"subinstrumentid varies per instrument, not globally."
            )

    out: dict = {"id": _step_id, "name": name, "stepType": st}
    if st == "calc":
        src = step.get("source") or "formula"
        if src not in ("formula", "value", "event_field", "collect"):
            raise ToolError(f"Unknown calc source '{src}'")
        out.update({
            "source": src,
            "formula": _coerce_lower_booleans(step.get("formula") or ""),
            "value": _coerce_lower_booleans(step.get("value") or ""),
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
        out["elseFormula"] = _coerce_lower_booleans(step.get("elseFormula") or "")
        if not out["conditions"]:
            raise ToolError(f"step '{name}': condition step requires at least one entry in `conditions`")
        for i, c in enumerate(out["conditions"]):
            for k in ("condition", "thenFormula"):
                v = c.get(k)
                if isinstance(v, str) and v:
                    v = _coerce_lower_booleans(v)
                    c[k] = v
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
                    v = _coerce_lower_booleans(v)
                    it[k] = v
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
        # Snapshot the CURRENT version before overwriting so the user can
        # revert to it. We keep up to 20 snapshots per rule (the newest 20).
        try:
            existing = await db.saved_rules.find_one(
                {"id": rule["id"]}, {"_id": 0}
            )
            if existing:
                existing.pop("_id", None)
                snap = {
                    "rule_id": rule["id"],
                    "snapshot_at": rule["updated_at"],
                    "rule_doc": existing,
                }
                await db.rule_history.insert_one(snap)
                # Prune: keep only the 20 most recent snapshots for this rule.
                all_snaps = await db.rule_history.find(
                    {"rule_id": rule["id"]}, {"_id": 1}
                ).sort("snapshot_at", -1).to_list(None)
                if len(all_snaps) > 20:
                    ids_to_delete = [s["_id"] for s in all_snaps[20:]]
                    await db.rule_history.delete_many({"_id": {"$in": ids_to_delete}})
        except Exception:
            pass  # Never block a save because of snapshot failure
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

    # Detect which mandatory alias steps the rule already defines.
    # If postingdate/effectivedate/subinstrumentid calc steps exist, we ALWAYS
    # use their variable names in transactions so the generated code references
    # a defined Python variable rather than raw EVT.field dot-notation.
    step_names = {(s.get("name") or "").strip().lower() for s in (steps or []) if isinstance(s, dict)}
    has_postingdate_step   = "postingdate"    in step_names
    has_effectivedate_step = "effectivedate"  in step_names
    has_subinstrumentid_step = "subinstrumentid" in step_names

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

        # If the rule has mandatory alias steps, always wire transactions to them.
        # This guarantees the generated code references a defined variable.
        if has_postingdate_step:
            nt["postingDate"] = "postingdate"
        elif not str(nt.get("postingDate") or "").strip() and default_event:
            nt["postingDate"] = f"{default_event}.postingdate"

        if has_effectivedate_step:
            nt["effectiveDate"] = "effectivedate"
        elif not str(nt.get("effectiveDate") or "").strip() and default_event:
            nt["effectiveDate"] = f"{default_event}.effectivedate"

        # Normalize any agent-supplied date reference:
        # Convert Python-style underscore refs (e.g. REV_PostingDate, REV_postingdate)
        # to DSL dot notation (REV.postingdate) so the code generator handles them.
        # Also force the field part to lowercase (postingdate / effectivedate).
        # (Only applies when alias steps don't exist — skip if already set above.)
        for date_key, canonical_field in (("postingDate", "postingdate"), ("effectiveDate", "effectivedate")):
            already_aliased = (date_key == "postingDate" and has_postingdate_step) or \
                              (date_key == "effectiveDate" and has_effectivedate_step)
            if already_aliased:
                continue
            raw = str(nt.get(date_key) or "").strip()
            if not raw:
                continue
            # Already valid DSL dot notation → just lowercase the field part
            if "." in raw:
                parts = raw.split(".", 1)
                nt[date_key] = f"{parts[0]}.{parts[1].lower()}"
                continue
            # Python underscore form: EVENT_PostingDate → EVENT.postingdate
            _DATE_SUFFIXES = ("_postingdate", "_posting_date", "_effectivedate", "_effective_date",
                              "_PostingDate", "_EffectiveDate")
            fixed_date = False
            for suf in _DATE_SUFFIXES:
                if raw.lower().endswith(suf.lower()):
                    evt_part = raw[: len(raw) - len(suf)]
                    if evt_part:
                        nt[date_key] = f"{evt_part}.{canonical_field}"
                        fixed_date = True
                        break
            if fixed_date:
                continue
            # Bare identifier that isn't a known DSL global → override with default
            if raw not in ("postingdate", "effectivedate") and default_event:
                nt[date_key] = f"{default_event}.{canonical_field}"

        sid_now = str(nt.get("subInstrumentId") or "").strip()
        # If the rule has a subinstrumentid alias step, always use it in transactions.
        if has_subinstrumentid_step:
            nt["subInstrumentId"] = "subinstrumentid"
        elif multi_subid_default:
            # Multi-subid event detected: prefer the row-level identifier
            # over the literal default "1" / "1.0".
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


def _scalar_event_field_warnings(steps: list[dict],
                                  multi_evt_set: set[str]) -> list[dict]:
    """Scan `steps` for calc steps using source='event_field' that reference
    an event in `multi_evt_set` (i.e. the event has multiple subInstrumentIds
    per instrument in the loaded data).

    For those steps, a scalar event_field read returns only ONE subId's value
    because the engine merges multiple subId rows into a single row per
    instrument before executing the rule body.

    Returns a list of warning dicts (may be empty). Callers add these to the
    tool payload so the agent knows to switch to collect_by_instrument or
    collect_by_subinstrument where per-subId values are needed.

    IMPORTANT: if the rule has a 'subinstrumentid' step with source='event_field',
    the rule is in per-subinstrument mode — the engine executes the rule once per
    (instrumentId, subInstrumentId) pair, so every event field IS scalar within
    that single-row execution context. This function returns [] in that case,
    because event_field is correct and collect_by_instrument would be wrong.
    """
    # Detect per-subinstrument mode: subinstrumentid step using event_field means
    # each execution sees exactly one subId's row — all fields are scalar.
    has_per_subinstrument_mode = any(
        isinstance(s, dict)
        and (s.get("name") or "").strip().lower() == "subinstrumentid"
        and (s.get("source") or "").strip().lower() == "event_field"
        for s in (steps or [])
    )
    if has_per_subinstrument_mode:
        return []
    warnings: list[dict] = []
    for s in steps or []:
        if not isinstance(s, dict):
            continue
        if (s.get("stepType") or "calc") != "calc":
            continue
        if (s.get("source") or "formula") != "event_field":
            continue
        ef = (s.get("eventField") or "").strip()
        if not ef or "." not in ef:
            continue
        evt_name = ef.split(".", 1)[0].strip()
        field_name = ef.split(".", 1)[1].strip()
        # Case-insensitive match against multi-subid event set
        matched = next((e for e in multi_evt_set
                        if e.lower() == evt_name.lower()), None)
        if not matched:
            continue
        # Shared / identifier fields are safe as scalars even in multi-subid
        # events (they carry the same value on every subId row).
        _SAFE_SCALAR_FIELDS = {
            "postingdate", "effectivedate", "instrumentid", "subinstrumentid",
            "postingDate", "effectiveDate", "instrumentId", "subInstrumentId",
        }
        if field_name in _SAFE_SCALAR_FIELDS:
            continue
        step_name = s.get("name") or "<unnamed>"
        warnings.append({
            "step_name": step_name,
            "event": matched,
            "field": field_name,
            "issue": (
                f"Step '{step_name}' reads {ef!r} as a scalar but event "
                f"'{matched}' has multiple subInstrumentIds per instrument. "
                f"The engine collapses multi-subId rows before rule execution, "
                f"so this step sees only ONE subId's '{field_name}' value "
                f"(the last row that survived the merge)."
            ),
            "fix": (
                f"If you need the '{field_name}' value for EACH subId, change "
                f"this step to:\n"
                f"  {{name: '{step_name}', stepType: 'calc', source: 'collect',\n"
                f"   eventField: '{ef}',\n"
                f"   collectType: 'collect_by_instrument'}}         # → array of values, one per subId\n"
                f"Then use apply_each / lookup to process each element, or\n"
                f"use collectType: 'collect_by_subinstrument' if you want the\n"
                f"values restricted to the current (instrumentid, subinstrumentid) pair.\n"
                f"If '{field_name}' is the SAME across all subIds for an instrument\n"
                f"(e.g. a shared attribute), the scalar read is fine — ignore this warning."
            ),
        })
    return warnings


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


_SCHED_ACCESSOR_RE = re.compile(
    r'\bschedule_(?:sum|last|first|column|filter)\s*\(\s*([A-Za-z_]\w*)'
)


def _validate_schedule_accessor_calls(steps: list[dict]) -> None:
    """Cross-step validation: every schedule_sum / schedule_last /
    schedule_first / schedule_column / schedule_filter call's FIRST argument
    must be a schedule step name or one of its outputVar names — NOT a
    regular calc-step variable.

    Common agent mistake: `schedule_sum(opening_nbv, 'col')` where
    `opening_nbv` is a calc-step scalar.  Correct form:
    `schedule_sum(DepreciationSchedule, 'col')`.
    """
    # Collect schedule identifiers (step name + all outputVar names)
    sched_vars: set[str] = set()
    for step in steps:
        if step.get("stepType") == "schedule":
            nm = (step.get("name") or "").strip()
            if nm:
                sched_vars.add(nm)
            for ov in step.get("outputVars") or []:
                ovn = (ov.get("name") or "").strip()
                if ovn:
                    sched_vars.add(ovn)

    if not sched_vars:
        return  # No schedule steps in this rule; nothing to check.

    def _scan(expr: str, where: str) -> None:
        for m in _SCHED_ACCESSOR_RE.finditer(expr):
            arg = m.group(1)
            if arg not in sched_vars:
                raise ToolError(
                    f"In {where}: `{m.group(0)}(...)` — "
                    f"first argument '{arg}' is NOT a schedule step variable.\n"
                    f"The first argument to schedule_sum / schedule_first / "
                    f"schedule_last / schedule_column / schedule_filter MUST be "
                    f"the NAME of a schedule step (or one of its outputVars), "
                    f"NOT a regular calc-step result.\n"
                    f"Valid schedule variables in this rule: {sorted(sched_vars)}.\n"
                    f"FIX: Replace '{arg}' with the schedule step name, e.g.:\n"
                    f"  schedule_sum({next(iter(sorted(sched_vars)))!r}, "
                    f"'<column_name>')"
                )

    for step in steps:
        st = step.get("stepType") or "calc"
        nm = step.get("name") or "?"
        if st == "calc" and (step.get("source") or "formula") == "formula":
            f = step.get("formula") or ""
            if f:
                _scan(f, f"step '{nm}'.formula")
        elif st == "condition":
            for i, c in enumerate(step.get("conditions") or []):
                for k in ("condition", "thenFormula"):
                    v = c.get(k) or ""
                    if v:
                        _scan(v, f"step '{nm}'.conditions[{i}].{k}")
            ef = step.get("elseFormula") or ""
            if ef:
                _scan(ef, f"step '{nm}'.elseFormula")
        elif st == "iteration":
            for i, it in enumerate(step.get("iterations") or []):
                v = it.get("expression") or ""
                if v:
                    _scan(v, f"step '{nm}'.iterations[{i}].expression")


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
        # Track column names defined SO FAR within this schedule. Each
        # column formula can reference any column declared earlier in the
        # same `columns` array — the schedule engine evaluates them in
        # order. Tag refs with a marker (`@col=`) so the validator can
        # extend its scope per-column.
        prior_cols: list[str] = []
        for c in sc.get("columns") or []:
            cname = (c.get("name") or "").strip()
            f = c.get("formula") or ""
            if f:
                marker = (
                    f"@cols={','.join(prior_cols)}"
                    if prior_cols else "@cols="
                )
                refs.append((
                    f"step '{nm}'.scheduleConfig.columns['{cname}'].formula{marker}",
                    f,
                ))
            if cname:
                prior_cols.append(cname)
        for k in ("periodCountFormula", "startDateFormula", "endDateFormula"):
            v = sc.get(k) or ""
            if v:
                refs.append((f"step '{nm}'.scheduleConfig.{k}", v))
        # NOTE: contextVars are auto-derived from column formula identifiers by
        # _validate_schedule_step_shape and always exactly mirror what the column
        # formulas use. Validating them here would produce duplicate errors for
        # every undefined identifier already caught by the column formula checks
        # above. Skip contextVars validation — the column checks are sufficient.
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
            # Pull the prior-column allowlist out of the marker that
            # _step_referenced_names appended to the `where` label, so a
            # later column can reference earlier columns in the same
            # schedule. The marker has the form `…@cols=a,b,c`.
            prior_col_names: set[str] = set()
            if "@cols=" in where:
                where, _, marker = where.partition("@cols=")
                prior_col_names = {
                    n.strip() for n in marker.split(",") if n.strip()
                }
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
                if in_schedule_col and tok in prior_col_names:
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
                # SCHEDULE OUTPUTVAR HINT: If the undefined token looks like
                # it could be a schedule outputVar name or an alias the agent
                # invented for one, tell it that outputVar names are already
                # in scope directly — no wrapper calc step is needed.
                for _sch in steps:
                    if _sch.get("stepType") != "schedule":
                        continue
                    _sn = (_sch.get("name") or "").strip()
                    _ov_names = [
                        (ov.get("name") or "").strip()
                        for ov in (_sch.get("outputVars") or [])
                        if (ov.get("name") or "").strip()
                    ]
                    # tok matches the pattern <scheduleName>_<anything> — the
                    # agent is guessing an auto-generated name.
                    if _sn and tok.startswith(_sn + "_"):
                        fix += (
                            f" SCHEDULE OUTPUTVAR HINT: '{tok}' looks like an "
                            f"auto-generated name derived from schedule '{_sn}'. "
                            f"outputVar names are set EXPLICITLY in "
                            f"outputVars[].name and are in scope DIRECTLY — "
                            f"no alias calc step is needed. "
                            f"Current outputVar names on '{_sn}': "
                            f"{_ov_names or '(none yet)'}. "
                            f"If you want '{tok}' in scope, set "
                            f"outputVars[].name = '{tok}' on '{_sn}', "
                            f"then DELETE any alias step for it."
                        )
                        break
                    # tok IS a known outputVar but isn't in scope yet —
                    # probably an ordering problem (schedule step after
                    # the step that references it).
                    if tok in _ov_names:
                        fix += (
                            f" NOTE: '{tok}' IS defined as an outputVar of "
                            f"schedule '{_sn}' but is not yet in scope — "
                            f"ensure the schedule step '{_sn}' appears BEFORE "
                            f"the step that references '{tok}'."
                        )
                        break
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

    # -------------------------------------------------------------------------
    # ALIAS-STEP ANTI-PATTERN CHECK
    # Schedule outputVar names are already in scope directly — a calc step
    # whose formula is exactly the name of a schedule step or outputVar is a
    # redundant alias that the agent must NOT create.
    # -------------------------------------------------------------------------
    _sched_scope: set[str] = set()
    for _st in steps:
        if _st.get("stepType") == "schedule":
            _sn = (_st.get("name") or "").strip()
            if _sn:
                _sched_scope.add(_sn)
            for _ov in (_st.get("outputVars") or []):
                _ovn = (_ov.get("name") or "").strip()
                if _ovn:
                    _sched_scope.add(_ovn)

    for _st in steps:
        if _st.get("stepType") == "schedule":
            continue
        _src = (_st.get("source") or "formula").strip().lower()
        if _src != "formula":
            continue
        _formula = (_st.get("formula") or "").strip()
        _sname = (_st.get("name") or "").strip()
        # Formula is a single identifier that is already a schedule-scoped
        # variable — and the step doesn't share the same name (which would
        # just be a self-referencing step, a separate problem).
        if _formula in _sched_scope and _formula != _sname:
            _formula_idents = _extract_identifiers(_formula)
            if len(_formula_idents) == 1:
                errors.append({
                    "step": _sname,
                    "kind": "alias_step_antipattern",
                    "name": _formula,
                    "where": f"step '{_sname}'.formula",
                    "fix_hint": (
                        f"Step '{_sname}' is a REDUNDANT ALIAS. Its formula "
                        f"'{_formula}' is already in scope directly as a "
                        f"schedule outputVar or schedule step variable. "
                        f"ACTION: DELETE step '{_sname}'. Reference '{_formula}' "
                        f"directly in transaction amounts and other step formulas. "
                        f"Schedule outputVar names need NO wrapper calc step."
                    ),
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
    _validate_schedule_accessor_calls(steps)
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
    # Check for mandatory alias steps: postingdate, effectivedate, subinstrumentid.
    # These must be the first three steps of every rule so that downstream steps
    # and transactions reference defined Python variables rather than raw event refs.
    _defined_step_names = {(s.get("name") or "").strip().lower() for s in steps if isinstance(s, dict)}
    _missing_aliases = [n for n in ("postingdate", "effectivedate", "subinstrumentid")
                        if n not in _defined_step_names]
    if _missing_aliases:
        payload["missing_mandatory_alias_steps"] = _missing_aliases
        payload["missing_mandatory_alias_steps_hint"] = (
            f"⚠️ MANDATORY alias step(s) missing: {_missing_aliases}. "
            f"Every rule MUST begin with calc steps named 'postingdate', "
            f"'effectivedate', and 'subinstrumentid' (see Rule 0a STEP C). "
            f"Transactions reference these names as Python variables — omitting "
            f"them causes 'name is not defined' runtime errors. "
            f"Call add_step_to_rule immediately to add the missing steps BEFORE "
            f"any other calc/condition/schedule steps."
        )
    # Warn about any auto-generated outputVars
    _auto_ov_steps = [
        s["name"] for s in steps
        if s.get("stepType") == "schedule"
        and (s.get("scheduleConfig") or {}).get("_auto_generated_outputVars")
    ]
    if _auto_ov_steps:
        payload["auto_generated_outputVars_steps"] = _auto_ov_steps
        payload["auto_generated_outputVars_hint"] = (
            f"⚠️ Schedule step(s) {_auto_ov_steps} had no outputVars supplied — "
            "defaults were auto-generated. Review the outputVars on each schedule "
            "step (via get_saved_rule) and patch via update_step if the generated "
            "names or types don't match your column names. filter→schedule_filter "
            "(preferred for date-range), last→schedule_last, sum→schedule_sum."
        )
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
        _scalar_warns = _scalar_event_field_warnings(steps, set(multi_evts))
        if _scalar_warns:
            _bad = ", ".join(f"'{w['step_name']}' ({w['event']}.{w['field']})" for w in _scalar_warns)
            raise ToolError(
                f"SCALAR SOURCE ON NON-SCALAR EVENT — rule creation blocked.\n"
                f"{len(_scalar_warns)} step(s) use source='event_field' on event(s) "
                f"{multi_evts} which have multiple subInstrumentIds per instrument: "
                f"{_bad}.\n"
                f"Using event_field on a multi-subId event silently discards all but "
                f"one subId's row. This is always wrong for per-instrument data.\n"
                f"Fix EVERY affected step before creating this rule:\n"
                f"  WRONG: {{name:'product_id', source:'event_field', eventField:'REV.FIELD'}}\n"
                f"  RIGHT: {{name:'product_id', source:'collect', "
                f"collectType:'collect_by_instrument', eventField:'REV.FIELD'}}\n"
                f"See MANDATORY FIELD-PLANNING GATE (Rule 0a) in the system prompt."
            )
    sched_results = await _auto_test_schedule_steps(rule)
    if sched_results:
        payload["schedule_tests"] = sched_results
        if any(not r.get("ok") for r in sched_results):
            if any(r.get("failed_at") == "no_event_data" for r in sched_results):
                missing_hints = [
                    r["fix_hint"] for r in sched_results
                    if r.get("failed_at") == "no_event_data" and r.get("fix_hint")
                ]
                payload["schedule_tests_hint"] = (
                    "⚠️ Schedule step has no sample event data — formula is NOT broken. "
                    + (missing_hints[0] if missing_hints else
                       "Call generate_sample_event_data for each referenced activity event, "
                       "then re-run test_schedule_step. DO NOT patch formulas.")
                )
            else:
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
        _validate_schedule_accessor_calls(rule["steps"])
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
        _scalw = _scalar_event_field_warnings(rule.get("steps") or [], set(multi_evts))
        if _scalw:
            _bad = ", ".join(f"'{w['step_name']}' ({w['event']}.{w['field']})" for w in _scalw)
            raise ToolError(
                f"SCALAR SOURCE ON NON-SCALAR EVENT — rule update blocked.\n"
                f"{len(_scalw)} step(s) use source='event_field' on event(s) "
                f"{multi_evts} which have multiple subInstrumentIds per instrument: "
                f"{_bad}.\n"
                f"event_field silently discards all but one subId's row. "
                f"This is always wrong for per-instrument data.\n"
                f"Fix every affected step to use source='collect', "
                f"collectType='collect_by_instrument' before updating the rule."
            )
    sched_results = await _auto_test_schedule_steps(rule)
    if sched_results:
        payload["schedule_tests"] = sched_results
        if any(not r.get("ok") for r in sched_results):
            if any(r.get("failed_at") == "no_event_data" for r in sched_results):
                missing_hints = [
                    r["fix_hint"] for r in sched_results
                    if r.get("failed_at") == "no_event_data" and r.get("fix_hint")
                ]
                payload["schedule_tests_hint"] = (
                    "⚠️ Schedule step has no sample event data — formula is NOT broken. "
                    + (missing_hints[0] if missing_hints else
                       "Call generate_sample_event_data for each referenced activity event, "
                       "then re-run test_schedule_step. DO NOT patch formulas.")
                )
            else:
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
    # ── J20: reference-integrity check ───────────────────────────────
    # Reject the delete if any user_template still references this rule
    # by id, UNLESS force=true (caller acknowledges orphaning).
    referenced: list[dict] = []
    try:
        cursor = db.user_templates.find(
            {"$or": [{"rule_ids": rule["id"]}, {"rules.id": rule["id"]}]},
            {"_id": 0, "id": 1, "name": 1},
        )
        referenced = await cursor.to_list(length=50)
    except Exception as exc:
        logger.warning("Reference check on delete_saved_rule failed: %s", exc)
    if referenced and not bool(args.get("force")):
        names = [t.get("name") for t in referenced]
        raise ToolError(
            f"Refusing to delete rule '{rule['name']}' — still referenced by "
            f"{len(referenced)} template(s): {names}. Detach via "
            f"`attach_rules_to_template` (omit this rule_id from rule_ids), "
            f"OR pass force=true to delete anyway and orphan the references."
        )
    await db.saved_rules.delete_one({"id": rule["id"]})
    return {
        "deleted": rule["name"],
        "id": rule["id"],
        "orphaned_templates": [t.get("name") for t in referenced] if referenced else [],
    }


# ──────────────────────────────────────────────────────────────────────────
# Step tools (operate on a parent rule)
# ──────────────────────────────────────────────────────────────────────────

def _resolve_step_index(rule: dict, args: dict) -> int:
    steps = rule.get("steps") or []
    # 1. Prefer immutable step_id (set once at validate-time, survives renames).
    sid = (args.get("step_id") or "").strip() if isinstance(args.get("step_id"), str) else ""
    if sid:
        for i, s in enumerate(steps):
            if (s.get("id") or "") == sid:
                return i
        raise ToolError(
            f"step_id '{sid}' not found in rule '{rule.get('name')}'. "
            f"Available step_ids: "
            f"{[(s.get('name'), s.get('id')) for s in steps]}"
        )
    # 2. Then explicit numeric index.
    if "step_index" in args and args["step_index"] is not None:
        idx = int(args["step_index"])
        if idx < 0 or idx >= len(steps):
            raise ToolError(f"step_index {idx} out of range (0..{len(steps)-1})")
        return idx
    # 3. Fallback: mutable step_name (case-sensitive).
    name = (args.get("step_name") or "").strip()
    if not name:
        raise ToolError("step_id, step_index OR step_name is required")
    for i, s in enumerate(steps):
        if (s.get("name") or "") == name:
            return i
    raise ToolError(
        f"step '{name}' not found in rule '{rule.get('name')}'. "
        f"Existing step names: {[s.get('name') for s in steps]}. "
        f"Tip: pass step_id (immutable) instead — list it via get_saved_rule."
    )


# ──────────────────────────────────────────────────────────────────────────
# Deep-merge / JSON-Pointer / read-back verification helpers (shared by
# tool_update_step, tool_patch_step, tool_replace_schedule_column). Without
# these, the prior shallow merge wiped sibling sub-fields whenever the
# agent patched a nested object like `scheduleConfig`.
# ──────────────────────────────────────────────────────────────────────────

def _deep_merge_step_patch(existing: dict, patch: dict) -> dict:
    """Recursively merge `patch` into a copy of `existing`.

    Rules:
      - dict   ← dict      → recurse
      - list   ← list      → REPLACE (caller must pass the full list — schedule
                              column reorders / removals would otherwise be
                              impossible to express)
      - scalar ← anything  → REPLACE
      - missing key on RHS → keep LHS
    """
    if not isinstance(existing, dict):
        return dict(patch) if isinstance(patch, dict) else patch
    if not isinstance(patch, dict):
        return patch
    out: dict = dict(existing)
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(existing.get(k), dict):
            out[k] = _deep_merge_step_patch(existing[k], v)
        else:
            out[k] = v
    return out


def _split_pointer(path: str) -> list[str]:
    """RFC 6901 minimal — splits '/a/b/0' into ['a','b','0']. '~1' → '/', '~0' → '~'."""
    if not path or path == "/":
        return []
    if not path.startswith("/"):
        raise ToolError(f"JSON Pointer must start with '/' (got {path!r})")
    parts = path[1:].split("/")
    return [p.replace("~1", "/").replace("~0", "~") for p in parts]


def _ptr_get_parent(root: Any, parts: list[str]) -> tuple[Any, str | int]:
    """Walk to the parent container. Returns (parent, last_token)."""
    if not parts:
        raise ToolError("JSON Pointer path '/' refers to the root — use the parent tool instead")
    cur = root
    for tok in parts[:-1]:
        if isinstance(cur, list):
            try:
                cur = cur[int(tok)]
            except (ValueError, IndexError) as e:
                raise ToolError(f"JSON Pointer step '/{tok}' invalid: {e}")
        elif isinstance(cur, dict):
            if tok not in cur:
                raise ToolError(
                    f"JSON Pointer step '/{tok}' missing in object "
                    f"(keys: {sorted(cur.keys())[:10]})"
                )
            cur = cur[tok]
        else:
            raise ToolError(f"JSON Pointer step '/{tok}' cannot descend into {type(cur).__name__}")
    last = parts[-1]
    if isinstance(cur, list):
        # '-' means "append" per RFC 6901
        return cur, (last if last == "-" else int(last))
    return cur, last


def _apply_json_pointer_op(root: dict, op: dict) -> None:
    """Apply ONE RFC 6902-style op in place. Supported: replace, add, remove."""
    if not isinstance(op, dict):
        raise ToolError(f"each op must be an object, got {type(op).__name__}")
    kind = (op.get("op") or "").strip().lower()
    if kind not in ("replace", "add", "remove"):
        raise ToolError(
            f"unsupported op '{kind}'. Use one of: replace | add | remove."
        )
    parts = _split_pointer(op.get("path") or "")
    parent, key = _ptr_get_parent(root, parts)
    if kind == "remove":
        if isinstance(parent, list):
            try:
                del parent[key]
            except IndexError as e:
                raise ToolError(f"remove at /{'/'.join(parts)}: {e}")
        elif isinstance(parent, dict):
            parent.pop(key, None)
        return
    if "value" not in op:
        raise ToolError(f"op '{kind}' at /{'/'.join(parts)} requires 'value'")
    val = op["value"]
    if isinstance(parent, list):
        if key == "-":
            parent.append(val)
        elif kind == "add":
            parent.insert(int(key), val)
        else:  # replace
            try:
                parent[int(key)] = val
            except IndexError as e:
                raise ToolError(f"replace at /{'/'.join(parts)}: {e}")
    elif isinstance(parent, dict):
        parent[key] = val
    else:
        raise ToolError(f"cannot apply op at /{'/'.join(parts)} — parent is {type(parent).__name__}")


def _normalize_for_compare(v: Any) -> Any:
    """Strip whitespace from strings so 'foo ' == 'foo' for verification."""
    if isinstance(v, str):
        return v.strip()
    return v


def _walk_diff(expected: Any, actual: Any, path: str = "") -> list[str]:
    """Return a list of human-readable mismatches between expected and actual.

    Only checks the SHAPE present in `expected`. Extra keys in `actual` are
    fine (the rest of the step doc may carry other fields).
    """
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return [f"{path or '<root>'}: expected dict, got {type(actual).__name__}"]
        out: list[str] = []
        for k, v in expected.items():
            out.extend(_walk_diff(v, actual.get(k), f"{path}.{k}" if path else k))
        return out
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return [f"{path}: expected list, got {type(actual).__name__}"]
        if len(expected) != len(actual):
            return [f"{path}: list length {len(actual)} != expected {len(expected)}"]
        out = []
        for i, (e, a) in enumerate(zip(expected, actual)):
            out.extend(_walk_diff(e, a, f"{path}[{i}]"))
        return out
    if _normalize_for_compare(expected) != _normalize_for_compare(actual):
        return [f"{path}: expected {expected!r}, got {actual!r}"]
    return []


async def _verify_step_persisted(rule_id: str, step_id: str, expected_subset: dict) -> dict:
    """Re-fetch the rule from the DB and confirm the step now contains the
    expected fields. Returns {ok, mismatches[]}.

    `expected_subset` is matched as a SUBTREE: only the keys present in it
    are checked, so callers can pass `{scheduleConfig: {columns: [...]}}`
    without listing every other step field.
    """
    try:
        fresh = await _load_rule(rule_id)
    except Exception as e:
        return {"ok": False, "mismatches": [f"reload failed: {e}"]}
    target = None
    for s in fresh.get("steps") or []:
        if (s.get("id") or "") == step_id:
            target = s
            break
    if target is None:
        return {"ok": False, "mismatches": [f"step_id {step_id} no longer present after save"]}
    diffs = _walk_diff(expected_subset, target)
    return {"ok": not diffs, "mismatches": diffs}


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
    _validate_schedule_accessor_calls(steps)
    _validate_transaction_outputs(steps, rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {"rule_id": rule["id"], "step_name": step["name"], "step_count": len(steps)}
    # Block if new step reads a scalar event_field from a multi-subId event.
    _add_multi_evts = await _detect_multi_subid_events(steps)
    if _add_multi_evts:
        _add_scalw = _scalar_event_field_warnings([step], set(_add_multi_evts))
        if _add_scalw:
            w = _add_scalw[0]
            raise ToolError(
                f"SCALAR SOURCE ON NON-SCALAR EVENT — step addition blocked.\n"
                f"Step '{w['step_name']}' uses source='event_field' on event "
                f"'{w['event']}' which has multiple subInstrumentIds per instrument.\n"
                f"event_field silently discards all but one subId's row. "
                f"This is always wrong for per-instrument data.\n"
                f"Fix: {{name:'{w['step_name']}', source:'collect', "
                f"collectType:'collect_by_instrument', eventField:'{w['event']}.{w['field']}'}}"
            )
    # Surface auto-generated outputVars hint
    if step.get("stepType") == "schedule" and (
        step.get("scheduleConfig") or {}
    ).get("_auto_generated_outputVars"):
        payload["auto_generated_outputVars"] = step.get("outputVars", [])
        payload["auto_generated_outputVars_hint"] = (
            "⚠️ You did not supply outputVars for this schedule step — they were "
            "auto-generated so the step is usable. Review the generated names below "
            "and patch them via update_step if they don't match your column names or "
            "intended usage. Remember: filter type uses schedule_filter (preferred for "
            "date-range), last uses schedule_last, sum uses schedule_sum."
        )
    return await _attach_validation(rule, payload)


async def tool_update_step(args: dict) -> dict:
    """Patch one step inside a rule via a DEEP MERGE of `patch` into the
    existing step doc. Nested objects (scheduleConfig, conditions[i], etc.)
    are merged recursively — so e.g. `patch={scheduleConfig:{frequency:"Q"}}`
    only changes the frequency and preserves all columns/outputs/dates. To
    REPLACE a list (e.g. swap the entire columns array) pass the full new
    list; to surgically edit ONE column use `patch_step` or
    `replace_schedule_column`. Always re-fetches the rule afterwards and
    confirms the patch persisted (returns ok=false with mismatches[] if not).
    """
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
        # Detect the most common agent mistake: passing `ops=[...]` (the
        # patch_step / JSON-Pointer format) to update_step, which uses deep-merge.
        if isinstance(args.get("ops"), list):
            raise ToolError(
                "You called `update_step` with `ops=[...]` — that is the "
                "`patch_step` format. Either:\n"
                "  (A) Call `patch_step` with your `ops` array as-is, OR\n"
                "  (B) Rewrite as `update_step` with a `patch` object:\n"
                "      patch={'scheduleConfig': {'periodType': 'date',\n"
                "        'startDateSource': 'field',\n"
                "        'startDateField': 'EVENTNAME.fieldname',\n"
                "        'endDateSource': 'formula',\n"
                "        'endDateFormula': '<DSL expr>'}}.\n"
                "The `update_step` patch is deep-merged so you only need to "
                "include the keys you want to change."
            )
        flat = {k: v for k, v in (args or {}).items() if k in _STEP_FIELDS}
        if flat:
            patch = flat
        else:
            raise ToolError(
                "patch must be a non-empty object. Pass step fields either "
                "wrapped as `patch={...}` OR at the top level alongside "
                "rule_id/step_id (e.g. {rule_id, step_id, formula})."
            )
    # Block id mutation through the patch — id is immutable.
    if "id" in patch:
        patch = {k: v for k, v in patch.items() if k != "id"}
    existing_step = rule["steps"][idx]
    step_id = existing_step.get("id")
    merged = _deep_merge_step_patch(existing_step, patch)
    # Preserve the immutable id even if the existing step somehow lacked one.
    if step_id:
        merged["id"] = step_id
    merged = _validate_step_shape(merged)
    rule["steps"][idx] = merged
    _validate_schedule_accessor_calls(rule["steps"])
    _validate_transaction_outputs(rule["steps"], rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {
        "rule_id": rule["id"],
        "step_index": idx,
        "step_id": merged.get("id"),
        "step_name": merged["name"],
        "merge_mode": "deep",
    }
    # Block if the (updated) step reads a scalar event_field from a multi-subId event.
    _upd_multi_evts = await _detect_multi_subid_events(rule.get("steps") or [])
    if _upd_multi_evts:
        _upd_scalw = _scalar_event_field_warnings([merged], set(_upd_multi_evts))
        if _upd_scalw:
            w = _upd_scalw[0]
            raise ToolError(
                f"SCALAR SOURCE ON NON-SCALAR EVENT — step update blocked.\n"
                f"Step '{w['step_name']}' uses source='event_field' on event "
                f"'{w['event']}' which has multiple subInstrumentIds per instrument.\n"
                f"event_field silently discards all but one subId's row. "
                f"This is always wrong for per-instrument data.\n"
                f"Fix: {{name:'{w['step_name']}', source:'collect', "
                f"collectType:'collect_by_instrument', eventField:'{w['event']}.{w['field']}'}}"
            )
    # Read-back: re-fetch and confirm the patched fields actually persisted.
    if merged.get("id"):
        verify = await _verify_step_persisted(rule["id"], merged["id"], patch)
        payload["persisted"] = verify
        if not verify["ok"]:
            payload["persisted_hint"] = (
                "⚠️ Patch did not fully land. Likely cause: the field name in "
                "your patch does not match the canonical step shape — call "
                "get_saved_rule and inspect the step doc to see the actual "
                "field names, then re-issue update_step (or use patch_step "
                "with explicit JSON-Pointer paths)."
            )
    return await _attach_validation(rule, payload)


async def tool_delete_step(args: dict) -> dict:
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    removed = rule["steps"].pop(idx)
    rule = await _save_rule_doc(rule, is_new=False)
    payload = {
        "rule_id": rule["id"],
        "deleted_step": removed.get("name"),
        "deleted_step_id": removed.get("id"),
        "step_count": len(rule["steps"]),
    }
    # Read-back: confirm the step really left the persisted doc.
    if removed.get("id"):
        try:
            fresh = await _load_rule(rule["id"])
            still_there = any(
                (s.get("id") or "") == removed.get("id")
                for s in (fresh.get("steps") or [])
            )
            payload["persisted"] = {"ok": not still_there}
            if still_there:
                payload["persisted"]["mismatches"] = [
                    f"step_id {removed.get('id')} still present after delete"
                ]
        except Exception as e:
            payload["persisted"] = {"ok": False, "mismatches": [f"reload failed: {e}"]}
    return await _attach_validation(rule, payload)


async def tool_patch_step(args: dict) -> dict:
    """Surgical step editor using JSON-Pointer (RFC 6902) ops.

    Use this — NOT update_step — when you need to change ONE leaf inside a
    deeply nested step doc, e.g. fix a single schedule column's formula:

        patch_step(rule_id, step_id, ops=[
          {"op":"replace","path":"/scheduleConfig/columns/2/formula","value":"..."}
        ])

    Supported ops: replace, add, remove. Index '-' on a list means append.
    The full step is re-validated after the ops are applied, so syntactic
    guardrails still fire. Returns ok=false with mismatches[] if the
    re-fetched step doesn't reflect the requested ops.
    """
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    ops = args.get("ops") or []
    if not isinstance(ops, list) or not ops:
        raise ToolError(
            "`ops` must be a non-empty array of "
            "{op:'replace'|'add'|'remove', path:'/...', value:?} entries."
        )
    existing_step = rule["steps"][idx]
    step_id = existing_step.get("id")
    # Operate on a deep-ish copy so a failing op doesn't leave a half-mutated step.
    import copy as _copy
    working = _copy.deepcopy(existing_step)
    applied: list[dict] = []
    for i, op in enumerate(ops):
        try:
            _apply_json_pointer_op(working, op)
            applied.append({"index": i, "op": op.get("op"), "path": op.get("path"), "ok": True})
        except ToolError as e:
            raise ToolError(f"ops[{i}] failed: {e}")
    if step_id:
        working["id"] = step_id
    working = _validate_step_shape(working)
    rule["steps"][idx] = working
    _validate_transaction_outputs(rule["steps"], rule.get("outputs") or {})
    rule = await _save_rule_doc(rule, is_new=False)
    payload: dict = {
        "rule_id": rule["id"],
        "step_index": idx,
        "step_id": working.get("id"),
        "step_name": working.get("name"),
        "ops_applied": applied,
    }
    if working.get("id"):
        # Build an "expected subset" by re-applying ops to an empty mirror of
        # the relevant paths — for now we just verify each /path/value pair.
        mismatches: list[str] = []
        try:
            fresh = await _load_rule(rule["id"])
            target = next(
                (s for s in (fresh.get("steps") or [])
                 if (s.get("id") or "") == working.get("id")),
                None,
            )
            if target is None:
                mismatches.append(f"step_id {working['id']} missing after save")
            else:
                for op in ops:
                    if op.get("op") == "remove":
                        continue
                    parts = _split_pointer(op.get("path") or "")
                    cur: Any = target
                    ok = True
                    for tok in parts:
                        if isinstance(cur, list):
                            try:
                                cur = cur[int(tok)] if tok != "-" else None
                            except (ValueError, IndexError):
                                ok = False; break
                        elif isinstance(cur, dict):
                            if tok not in cur:
                                ok = False; break
                            cur = cur[tok]
                        else:
                            ok = False; break
                    if not ok:
                        mismatches.append(f"path {op.get('path')!r} not present after save")
                        continue
                    expected_v = _normalize_for_compare(op.get("value"))
                    actual_v = _normalize_for_compare(cur)
                    if expected_v != actual_v:
                        mismatches.append(
                            f"path {op.get('path')!r}: expected {expected_v!r}, got {actual_v!r}"
                        )
        except Exception as e:
            mismatches.append(f"reload failed: {e}")
        payload["persisted"] = {"ok": not mismatches, "mismatches": mismatches}
    return await _attach_validation(rule, payload)


async def tool_replace_schedule_column(args: dict) -> dict:
    """Convenience wrapper around patch_step for the most common surgical
    edit: change ONE schedule column's formula (or rename it).

    args: {rule_id, step_id|step_name|step_index, column_name,
           new_formula?, new_name?}
    """
    rule = await _load_rule((args.get("rule_id") or "").strip())
    idx = _resolve_step_index(rule, args)
    step = rule["steps"][idx]
    if (step.get("stepType") or "") != "schedule":
        raise ToolError(
            f"step '{step.get('name')}' is not a schedule step "
            f"(stepType={step.get('stepType')!r}); replace_schedule_column "
            f"only applies to schedule steps."
        )
    col_name = (args.get("column_name") or "").strip()
    if not col_name:
        raise ToolError("`column_name` is required (the existing schedule column to replace).")
    cols = (step.get("scheduleConfig") or {}).get("columns") or []
    col_idx = next((i for i, c in enumerate(cols) if (c.get("name") or "") == col_name), -1)
    if col_idx < 0:
        raise ToolError(
            f"column '{col_name}' not found in schedule '{step.get('name')}'. "
            f"Existing columns: {[c.get('name') for c in cols]}."
        )
    new_formula = args.get("new_formula")
    new_name = args.get("new_name")
    if new_formula is None and new_name is None:
        raise ToolError("Pass at least one of `new_formula` or `new_name`.")
    ops: list[dict] = []
    if new_formula is not None:
        ops.append({"op": "replace",
                    "path": f"/scheduleConfig/columns/{col_idx}/formula",
                    "value": str(new_formula)})
    if new_name is not None:
        ops.append({"op": "replace",
                    "path": f"/scheduleConfig/columns/{col_idx}/name",
                    "value": str(new_name)})
    return await tool_patch_step({
        "rule_id": args.get("rule_id"),
        "step_id": step.get("id"),
        "ops": ops,
    })


# ──────────────────────────────────────────────────────────────────────────
# Knowledge-base tools (canonical patterns + similar templates + plan)
# ──────────────────────────────────────────────────────────────────────────

async def tool_list_canonical_patterns(args: dict) -> dict:
    """Return the menu of canonical patterns (A/B/C/D) the agent should pick
    BEFORE writing a non-trivial rule. Each entry has id, name, title,
    when_to_use[], and the transaction types it typically emits."""
    from .knowledge import list_patterns
    return {"patterns": list_patterns()}


async def tool_get_canonical_pattern(args: dict) -> dict:
    """Fetch a canonical pattern by id (A/B/C/D). Returns the full step
    scaffold (copy-pasteable into create_saved_rule), parameter substitutions
    needed, transaction types, and anti-patterns to avoid."""
    from .knowledge import get_pattern
    pid = (args.get("pattern_id") or "").strip()
    if not pid:
        raise ToolError("`pattern_id` is required (one of: A, B, C, D)")
    pat = get_pattern(pid)
    if not pat:
        from .knowledge import list_patterns
        raise ToolError(
            f"unknown pattern_id '{pid}'. Available: "
            f"{[p['id'] for p in list_patterns()]}"
        )
    return {"pattern": pat}


async def tool_find_similar_template(args: dict) -> dict:
    """Suggest the closest canonical pattern AND any saved_rule whose name
    looks similar to the user's intent. Use BEFORE building a new rule.

    args: {intent: str, keywords?: [str]}
    """
    from .knowledge import match_pattern_by_intent
    intent = (args.get("intent") or "").strip()
    keywords = args.get("keywords") or []
    if not intent and not keywords:
        raise ToolError("pass `intent` (free text) and/or `keywords` (list of strings)")
    ranked = match_pattern_by_intent(intent, keywords)
    similar_rules: list[dict] = []
    db = _ServerBridge.db
    if db is not None:
        try:
            text = (intent + " " + " ".join(keywords)).lower()
            tokens = [
                t for t in re.split(r"[^a-z0-9]+", text)
                if len(t) >= 4 and t not in {
                    "rule", "rules", "with", "from", "that", "have",
                    "have", "this", "into", "they", "them", "model",
                }
            ]
            docs = await db.saved_rules.find(
                {}, {"_id": 0, "id": 1, "name": 1, "ruleType": 1}
            ).to_list(500)
            scored: list[tuple[int, dict]] = []
            for d in docs:
                nm = (d.get("name") or "").lower()
                hits = sum(1 for t in tokens if t in nm)
                if hits:
                    scored.append((hits, d))
            scored.sort(key=lambda x: -x[0])
            similar_rules = [
                {"id": d.get("id"), "name": d.get("name"),
                 "ruleType": d.get("ruleType"), "score": h}
                for h, d in scored[:8]
            ]
        except Exception:
            similar_rules = []
    return {
        "ranked_patterns": ranked,
        "top_pattern_id": ranked[0]["pattern_id"] if ranked else None,
        "similar_saved_rules": similar_rules,
        "next_step_hint": (
            "Call get_canonical_pattern(pattern_id=<top>) to retrieve the "
            "full scaffold, OR get_saved_rule(rule_id=<closest match>) to "
            "copy from a working rule. Then call submit_plan with your "
            "chosen pattern and the rule outline."
        ),
    }


# Per-process plan registry. Lightweight — survives until process restart;
# real persistence not needed because plans are advisory only.
_RUN_PLANS: dict[str, dict] = {}


async def tool_submit_plan(args: dict) -> dict:
    """Record an explicit build plan BEFORE creating any rule/step. Forces
    the agent to commit to one pattern, list the rules it intends to create,
    and the transactions each rule will emit. The plan is echoed back with
    the canonical pattern fixture pre-attached, so the next tool call can
    paste the scaffold directly into create_saved_rule.

    args: {
      run_id?: str,                # optional grouping key (session/run id)
      intent: str,                 # one-line natural language goal
      pattern_id: 'A'|'B'|'C'|'D', # canonical pattern to follow
      events_needed: [str],        # event names to ensure exist
      transaction_types: [str],    # txn types to register
      rules: [{
         name: str,
         intent: str,              # what this rule computes
         steps_outline: [str],     # short bullet list of step names
         transactions: [str]       # txn types this rule will emit
      }]
    }
    """
    from .knowledge import get_pattern
    intent = (args.get("intent") or "").strip()
    pid = (args.get("pattern_id") or "").strip().upper()
    rules = args.get("rules") or []
    if not intent:
        raise ToolError("`intent` (one-line description) is required")
    if not pid:
        raise ToolError(
            "`pattern_id` is required. Call list_canonical_patterns OR "
            "find_similar_template to pick one of A/B/C/D."
        )
    pat = get_pattern(pid)
    if not pat:
        raise ToolError(f"unknown pattern_id '{pid}' — must be one of A/B/C/D")
    if not isinstance(rules, list) or not rules:
        raise ToolError(
            "`rules` must be a non-empty list of "
            "{name, intent, steps_outline:[…], transactions:[…]} entries."
        )
    norm_rules: list[dict] = []
    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise ToolError(f"rules[{i}] must be an object")
        nm = (r.get("name") or "").strip()
        if not nm:
            raise ToolError(f"rules[{i}].name is required")
        norm_rules.append({
            "name": nm,
            "intent": (r.get("intent") or "").strip(),
            "steps_outline": [str(s).strip() for s in (r.get("steps_outline") or []) if str(s).strip()],
            "transactions": [str(t).strip() for t in (r.get("transactions") or []) if str(t).strip()],
        })
    plan = {
        "run_id": (args.get("run_id") or "").strip() or "default",
        "intent": intent,
        "pattern_id": pid,
        "events_needed": [str(e).strip() for e in (args.get("events_needed") or []) if str(e).strip()],
        "transaction_types": [str(t).strip() for t in (args.get("transaction_types") or []) if str(t).strip()],
        "rules": norm_rules,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }
    _RUN_PLANS[plan["run_id"]] = plan
    return {
        "ok": True,
        "plan": plan,
        "canonical_pattern": pat,
        "next_steps": [
            "1. Ensure events exist: create_event_definitions(events=[...]).",
            "2. Register every transaction type: add_transaction_types(types=[...]).",
            "3. For each rule in the plan, call create_saved_rule with steps "
            "shaped like the canonical pattern's steps[] (substitute the "
            "parameters listed in `pattern.parameters`).",
            "4. After creating each schedule step, call test_schedule_step "
            "and inspect the preview rows.",
            "5. Call verify_rule_complete on every rule before finish.",
        ],
    }





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

    # ── Early guard: detect missing event sample data ──────────────────────
    # If the activity events referenced by this schedule step have no data
    # rows, the schedule will produce 0 rows regardless of formula correctness.
    # Return a specific diagnostic so the agent calls generate_sample_event_data
    # instead of endlessly patching formulas.
    _extract_evts = _h("extract_event_names_from_dsl")
    _full_sched_rule = {**rule, "steps": steps[:idx + 1], "outputs": {}}
    _full_sched_code = _generate_rule_code(_full_sched_rule)
    _all_refs = list(_extract_evts(prior_code + " " + _full_sched_code) or [])
    _missing_data: list[str] = []
    for _nm in _all_refs:
        _evt_def = await _find_event_def(_nm)
        if not _evt_def or (_evt_def.get("eventType") or "activity") != "activity":
            continue
        _evt_rows: list = []
        _edb = _ServerBridge.db
        if _edb is not None:
            _edoc = await _edb.event_data.find_one(
                {"event_name": {"$regex": f"^{re.escape(_nm)}$", "$options": "i"}},
                {"_id": 0, "data_rows": {"$slice": 1}},
            )
            if _edoc:
                _evt_rows = _edoc.get("data_rows") or []
        if not _evt_rows:
            for _d in (_ServerBridge.in_memory_data or {}).get("event_data") or []:
                if str(_d.get("event_name", "")).lower() == _nm.lower():
                    _evt_rows = _d.get("data_rows") or []
                    break
        if not _evt_rows:
            _missing_data.append(_nm)
    if _missing_data:
        _first_missing = _missing_data[0]
        return {
            "rule_id": rule["id"],
            "step_name": target.get("name"),
            "ok": False,
            "failed_at": "no_event_data",
            "error": (
                f"Activity event(s) {_missing_data} have no sample data. "
                f"The schedule will produce 0 rows regardless of formula. "
                f"The DSL formula is NOT broken — data is missing."
            ),
            "fix_hint": (
                f"MANDATORY NEXT STEPS — in order:\n"
                f"1. Call generate_sample_event_data(event_name='{_first_missing}', "
                f"instrument_ids=['LOAN-001'], posting_dates=['2024-12-31']) "
                f"for EACH missing event: {_missing_data}.\n"
                f"2. Then call test_schedule_step again.\n"
                f"DO NOT patch or update the schedule step formula — it is not "
                f"broken. The only issue is absent sample data."
            ),
        }
    # ── End early guard ────────────────────────────────────────────────────

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
                "fix_hint": r.get("fix_hint"),
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
    # Surrender / hallucinated-gate language. The agent has been seen to
    # invent fake "plan-gating", "session gate", "can't through the normal
    # path" excuses after a SINGLE recoverable tool error and then declare
    # itself "completed" while leaving the deliverable half-built. None of
    # these phrases describe real runtime behavior — every gate the runtime
    # actually enforces is overcome by submitting a plan, fixing the args,
    # or calling the indicated tool. Treat any of these as proof the agent
    # gave up too early.
    "i'm blocked",
    "i am blocked",
    "blocked by the workspace",
    "blocked by the session",
    "plan-gating behavior",
    "plan gating behavior",
    "session gate",
    "session's submitted-plan gate",
    "submitted-plan gate",
    "can't add the required",
    "cannot add the required",
    "can't persist",
    "cannot persist",
    "through the normal path here",
    "can't honestly mark",
    "cannot honestly mark",
    "build is not complete yet because",
    "what still needs to be done",
    "if you want, i can continue",
    "if you want me to continue",
    # Variants observed after the first round of blacklist work — the model
    # keeps inventing new "gate" / "path" / "commit the build" phrasings to
    # justify quitting before the deliverable is done.
    "workspace write gate",
    "write gate",
    "hit a workspace",
    "hit the workspace",
    "plan-compliant",
    "plan compliant",
    "rule-edit path",
    "rule edit path",
    "commit the build",
    "the next required step is to commit",
    "i couldn't persist",
    "i could not persist",
    "i couldn\u2019t persist",
    "couldn't persist edits",
    "could not persist edits",
    "persist edits yet",
    "then i can finish",
    "then i could finish",
    # "Asking for confirmation on well-known accounting standards" pattern.
    # The agent knows IAS 16, IFRS 9, IFRS 15, etc. Asking the user to
    # confirm debit/credit conventions for a named standard is a failure;
    # the standard defines them. Apply the known conventions and proceed.
    "confirm these assumptions",
    "confirm my assumptions",
    "confirm that assumption",
    "confirm the accounting",
    "confirm the journal",
    "confirm the entries",
    "once you confirm",
    "what i need from you",
    "what i need from the user",
    "what i need is a",
    "give me a small",
    "give me a concrete",
    "provide a sample dataset",
    "provide/allow a specific",
    "or confirm these",
    "either give me",
    "either confirm",
    "depreciation entry should be",
    "upward revaluation entry should be",
    "downward revaluation entry should be",
)

# Regex pattern: any combination of "gate" / "path" with surrender or
# permission language. Catches future invented phrasings without needing
# new literal strings every time the model gets creative.
_HALLUCINATED_GATE_RE = re.compile(
    r"\b(?:write|workspace|session|plan(?:-|\s)?(?:gating|gated|compliant)?"
    r"|rule(?:-|\s)?edit|normal|alternate)\s+(?:gate|path)\b"
    r"|\b(?:hit|blocked\s+by|behind)\s+(?:a|the|this)\s+(?:workspace|session|plan|write|edit)\b"
    r"|\bplan(?:-|\s)?gating\s+behavior\b",
    re.IGNORECASE,
)


async def tool_finish(args: dict) -> dict:
    summary = (args.get("summary") or "").strip() or "Done."
    # `user_request` is injected by runtime.py from the original task prompt
    # so this gate cannot be circumvented by the agent omitting the field.
    user_request = (args.get("user_request") or "").lower()
    # `rule_ids` is injected by runtime.py and contains every rule the agent
    # created/updated this turn. Combined with the agent-supplied `rule_id`,
    # this lets the gate check ALL touched rules instead of letting the
    # agent silently bypass the transaction/schedule check by omitting an id.
    rule_ids: list[str] = []
    raw_rids = args.get("rule_ids")
    if isinstance(raw_rids, list):
        for rid in raw_rids:
            rid_s = str(rid or "").strip()
            if rid_s and rid_s not in rule_ids:
                rule_ids.append(rid_s)
    single_rid = (args.get("rule_id") or "").strip()
    if single_rid and single_rid not in rule_ids:
        rule_ids.append(single_rid)

    for rule_id in rule_ids:
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
                    f"schedule's outputVar (e.g. type='filter' with matchCol=\"<period_end_col>\" "
                    f"matchValue=\"<postingdate>\", or type='last' for closing balance).\n"
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
                no_data_failures = [r for r in sched_failures
                                    if r.get("failed_at") == "no_event_data"]
                formula_failures = [r for r in sched_failures
                                    if r.get("failed_at") != "no_event_data"]
                if no_data_failures:
                    _hints = [r.get("fix_hint") for r in no_data_failures if r.get("fix_hint")]
                    raise ToolError(
                        f"Rule '{rule.get('name')}' has {len(no_data_failures)} "
                        f"schedule step(s) with no sample event data. The formula "
                        f"is NOT broken — data is missing. "
                        + (_hints[0] if _hints else
                           "Call generate_sample_event_data for each referenced "
                           "activity event, then re-run test_schedule_step until "
                           "ok=true BEFORE calling finish.")
                    )
                preview2 = "; ".join(
                    f"{r.get('step_name')}: "
                    f"{r.get('error') or 'failed'}"
                    for r in formula_failures[:5]
                )
                raise ToolError(
                    f"Rule '{rule.get('name')}' has {len(formula_failures)} "
                    f"schedule step(s) that fail their preview test: "
                    f"{preview2}. FIX each failing column/output via "
                    f"`update_step` and re-run `test_schedule_step` until "
                    f"ok=true BEFORE calling finish. A schedule that does "
                    f"not preview cannot run end-to-end."
                )
    low = summary.lower()
    # Generic catch for hallucinated "gate" / "path" surrender language.
    # The runtime has exactly ONE plan-gate (submit_plan) and never describes
    # itself to the user using these phrases. If the model is using them, it
    # is fabricating a permission system to justify quitting.
    gate_match = _HALLUCINATED_GATE_RE.search(summary)
    if gate_match:
        raise ToolError(
            f"`finish` summary mentions a fabricated runtime restriction "
            f"('{gate_match.group(0)}'). The runtime has exactly ONE plan-gate "
            f"(`submit_plan`, called once per run) and NO 'write gate', "
            f"'session gate', 'plan-compliant path', 'rule-edit path', or "
            f"'normal vs alternate path'. If a write tool returned a ToolError, "
            f"that error names the missing arg or invalid value — fix it and "
            f"call the SAME tool again. Do not invent a permission layer to "
            f"justify giving up. Re-issue the failed write tool call with "
            f"corrected arguments, then call `finish` only after the rule's "
            f"`outputs.transactions[]` is non-empty and balanced."
        )
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
EVENT DEFINITION TYPES — SET THESE CORRECTLY BEFORE ANYTHING ELSE
------------------------------------------------------------------
Every event must be created via create_event_definitions. The two fields
`eventType` and `eventTable` determine how the engine ingests and joins data.

ACTIVITY EVENTS (the default — transactional data):
  eventType = "activity"   eventTable = "standard"
  Use for: day-end balances, loan originations, payment events, credit risk
  snapshots — any data that arrives with a new row EACH posting date.
  The engine runs your rule ONCE PER (instrumentid × postingdate) row.
  Example:
    {event_name:"EOD_BALANCES", eventType:"activity", eventTable:"standard",
     fields:[{name:"upb",datatype:"decimal"}, {name:"rate",datatype:"decimal"}]}

REFERENCE TABLES (static lookup data — catalogs, rate tables, mappings):
  eventType = "reference"  eventTable = "custom"
  BOTH fields are REQUIRED together. A reference event with eventTable ≠ "custom"
  is invalid and will be auto-corrected by the validator.
  Use for: product catalogs, SSP/price tables, chart of accounts, rate lookup
  tables, country/currency mappings, any table that does NOT change per
  posting date and is JOINed into a rule via collect_all() + lookup().
  Example:
    {event_name:"PRODUCT_CATALOG", eventType:"reference", eventTable:"custom",
     fields:[{name:"product_id",datatype:"string"},
             {name:"ssp",datatype:"decimal"},
             {name:"product_name",datatype:"string"}]}

RECOGNITION RULE:
  • Name ends/starts with: catalog, ref, reference, lookup, lut, master,
    mapping, static, rate_table, ssp_table, product_table, chart_of_accounts
    → ALWAYS set eventType="reference" + eventTable="custom".
  • Anything else (balances, transactions, snapshots, originations) → activity.
  • The validator auto-corrects obvious mismatches and reports
    coerced_to_reference=true in the created[] payload — check it.

------------------------------------------------------------------
EVENT FIELD NAMING & DATA STANDARDS (ACCOUNTING CONVENTIONS)
------------------------------------------------------------------
CRITICAL: Field names in event definitions MUST use the conventional
terminology of the accounting standard that applies to the use case.
Generic names like "amount", "rate", "date" MUST be replaced with the
specific standard-compliant names listed below.  The sample-data
generator uses these names to produce realistic value ranges
automatically — wrong names produce wrong data.

FAS 91 / IFRS 9 — AMORTISED COST / LOAN FEE AMORTISATION
  loan_amount         decimal   50,000 – 500,000        (original loan)
  outstanding_balance decimal   0 – loan_amount         (current UPB)
  beginning_balance   decimal   0 – loan_amount
  ending_balance      decimal   0 – loan_amount
  origination_fee     decimal   500 – 15,000            (upfront fee booked)
  amortized_fee       decimal   10 – 2,000              (fee amortised this period)
  note_rate           decimal   0.02 – 0.12  (annual)   (stated/contract rate)
  eir_rate            decimal   0.02 – 0.14  (annual)   (effective interest rate)
  origination_date    date      past
  maturity_date       date      future (10–30 years from orig)
  term_months         integer   60 – 360

IFRS 9 / CECL — EXPECTED CREDIT LOSS
  pd                  decimal   0.001 – 0.30            (probability of default)
  lgd                 decimal   0.10 – 0.80             (loss given default)
  ead / outstanding_balance  decimal  10,000 – 500,000  (exposure at default)
  ecl                 decimal   0 – 50,000              (PD × LGD × EAD)
  stage               integer   1 / 2 / 3               (IFRS 9 stage)
  days_past_due       integer   0 – 365
  collateral_value    decimal   0 – 1,000,000

IFRS 16 / ASC 842 — LEASE ACCOUNTING
  rou_asset           decimal   10,000 – 500,000        (right-of-use asset)
  lease_liability     decimal   10,000 – 500,000
  lease_payment       decimal   500 – 20,000            (periodic payment)
  discount_rate / incremental_borrowing_rate
                      decimal   0.02 – 0.10  (annual)
  lease_term          integer   12 – 120  (months)
  lease_start_date    date      past
  lease_end_date      date      future

IAS 16 / ASC 360 — FIXED ASSETS / DEPRECIATION
  acquisition_cost    decimal   5,000 – 500,000
  residual_value      decimal   0 – 50,000              (salvage/scrap)
  accumulated_depreciation  decimal  0 – acquisition_cost
  depreciation_charge decimal   500 – 50,000            (period charge)
  nbv / net_book_value decimal  0 – acquisition_cost
  useful_life         integer   3 – 40  (years)
  acquisition_date    date      past
  depreciation_method string    "StraightLine" | "DecliningBalance" | "UnitsOfProduction"

IFRS 15 / ASC 606 — REVENUE RECOGNITION
  contract_amount / transaction_price  decimal  1,000 – 500,000
  ssp / standalone_selling_price       decimal  100 – 50,000
  allocated_amount    decimal   500 – 200,000           (obligation allocation)
  recognized_revenue  decimal   100 – 50,000            (this-period recognition)
  deferred_revenue    decimal   0 – 200,000             (unearned balance)
  contract_start_date / start_date  date  past or today
  contract_end_date / end_date      date  future

GOOD EXAMPLES VS BAD EXAMPLES:
  ✗ BAD:  fields: [{name:"amount"}, {name:"rate"}, {name:"date"}]
  ✓ GOOD: fields: [{name:"loan_amount"}, {name:"note_rate"}, {name:"origination_date"}]

  ✗ BAD:  fields: [{name:"value"}, {name:"percent"}]          (for IFRS9)
  ✓ GOOD: fields: [{name:"pd"}, {name:"lgd"}, {name:"ead"}, {name:"ecl"}, {name:"stage"}]

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
          contextVars). ALWAYS include outputVars (see OUTPUT VARIABLE RULES
          below) — a schedule step without outputVars is invisible to the
          modal and to downstream steps.
        → ⚠️ DO NOT use `create_saved_schedule` for typical user requests
          like "create a depreciation schedule". That tool produces a
          standalone DSL-only schedule with NO visual editor; the user
          will see an unfilled card. ALWAYS prefer the schedule STEP path
          unless the user explicitly asks for a shared/reusable library
          schedule.

  SCHEDULE STEP — OUTPUT VARIABLE RULES (mandatory):
  ────────────────────────────────────────────────────
  Every schedule step MUST have at least one entry in `outputVars`.
  Without outputVars the values inside the schedule cannot be referenced
  by transactions, other steps, or the visual modal.

  PRIORITY ORDER — use the highest priority that fits the use case:

  1. filter (PREFERRED for date-range schedules) — schedule_filter
     Picks the single row where a date column matches, e.g. the row
     for the current reporting period.
     Schema: {name, type:"filter", column:<value-col>,
               matchCol:"period_date", matchValue:"postingdate"}
     Example:
       outputVars: [
         {name:"depr_current",   type:"filter",  column:"depreciation_charge",
          matchCol:"period_date", matchValue:"postingdate"},
         {name:"depr_last",      type:"last",    column:"closing_nbv"}
       ]
     ALWAYS include a filter var for date-range schedules so the
     current-period amount is available for transactions.

  2. last (use when you need the FINAL / CLOSING value)
     Schema: {name, type:"last", column:<value-col>}
     Use for: closing balance, ending NBV, final liability balance etc.
     schedule_last(StepName, 'col') — retrieves the last row's value.

  3. sum (use when you need the LIFETIME TOTAL across all periods)
     Schema: {name, type:"sum", column:<value-col>}
     schedule_sum(StepName, 'col') — totals the column over all rows.

  4. first (use when you need the OPENING / FIRST value)
     Schema: {name, type:"first", column:<value-col>}

  5. column (use when you need the ENTIRE ARRAY of values)
     Schema: {name, type:"column", column:<col>}

  ❌ NEVER submit a schedule step with outputVars=[] or without outputVars.
  ✅ If in doubt, always include a filter var + a last var as defaults.

  FULL EXAMPLE:
    {
      "stepType": "schedule",
      "name": "dep_schedule",
      "scheduleConfig": {
        "periodType": "date",
        "startDateSource": "formula", "startDateFormula": "acquisition_date",
        "endDateSource": "formula",   "endDateFormula": "dispose_date",
        "frequency": "M",
        "columns": [
          {"name": "period_date",          "formula": "period_date"},
          {"name": "depreciation_charge",  "formula": "divide(cost_minus_residual, useful_life_periods)"},
          {"name": "accumulated_depr",     "formula": "cumulative_sum('depreciation_charge')"},
          {"name": "closing_nbv",          "formula": "subtract(acquisition_cost, accumulated_depr)"}
        ]
      },
      "outputVars": [
        {"name": "depr_current", "type": "filter",
         "column": "depreciation_charge",
         "matchCol": "period_date", "matchValue": "postingdate"},
        {"name": "closing_nbv",  "type": "last",   "column": "closing_nbv"},
        {"name": "total_depr",   "type": "sum",     "column": "depreciation_charge"}
      ]
    }

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
  Use for: amortisation, interest accrual, fee amortisation (FAS91/IFRS9),
           lease ROU, ECL projection, any tabular monthly time-series.
  Skeleton:
    1. calc steps capture inputs (principal, rate, term, …) from event fields
    2. schedule step produces N periodic rows (columns = period_date,
       month_end, balance, principal, interest, fee_amort, …) with
       contextVars listing the inputs
    3. outputVar type='filter' with matchCol='month_end' (or your period-end
       column) and matchValue='postingdate' extracts THIS PERIOD'S row.
       ★ Prefer filter over sum — sum gives the lifetime total, not one period.
    4. outputs.transactions[] emit debit/credit using the filtered values
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

⚠️  ALWAYS set periodType explicitly. Financial models almost always use
"date" so periods align with real loan/contract dates coming from the event.
Use "number" only when you have a fixed iteration count with no calendar dates.

// DATE-BASED schedule (preferred for loans, fees, amortisation):
{
  "name": "amort_schedule",
  "stepType": "schedule",
  "scheduleConfig": {
    "periodType": "date",
    "startDateSource": "field",
    "startDateField": "FeeEvent.origination_date",
    "endDateSource": "field",
    "endDateField": "FeeEvent.maturity_date",
    "frequency": "M",
    "columns": [
      {"name": "period_date", "formula": "period_date"},
      {"name": "interest",    "formula": "multiply(balance, monthly_rate)"},
      {"name": "principal",   "formula": "subtract(payment, interest)"}
    ],
    "contextVars": ["balance", "monthly_rate", "payment"]
  },
  "outputVars": [
    {"name": "total_interest", "type": "sum",  "column": "interest"}
  ]
}

// COUNT-BASED schedule (fixed number of periods, no real dates):
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
          (b) lag('colname', n, default) — reads the PRIOR PERIOD value of
              ANY column, including ones defined LATER in the array. This
              is the ONLY way to write rolling/recursive schedules.
          (c) a SCHEDULE BUILT-IN (see list below)
          (d) any DSL function name
          (e) a contextVar (auto-derived; see below)

    CANONICAL REDUCING-BALANCE EXAMPLE (copy this pattern directly):
      columns:
        - name: opening_nbv
          formula: "lag('closing_nbv', 1, opening_net_carrying_amount)"
            # reads prior period closing_nbv; seeds with calc-step var
            # opening_net_carrying_amount on period 0
        - name: depreciation_charge
          formula: "opening_nbv * reducing_balance_rate"
        - name: closing_nbv
          formula: "opening_nbv - depreciation_charge"
      # KEY RULE: lag('colname', …) may point to a column defined LATER —
      # it is NOT a forward-reference error. Bare references (without lag)
      # to a future column ARE errors. Only wrap in lag() to fix them.

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
      • filter  → schedule_filter(sched, "<matchCol>", <matchValue>, "<column>")
                  REQUIRES matchCol + matchValue + column.
                  ★ PREFERRED for amortisation/fee/accrual rules: use
                  matchCol='<period_end_column>' and matchValue='posting_date'
                  (or 'postingdate') to extract THIS PERIOD'S value.
                  Returns a list; if only one match is expected, the engine
                  will unwrap it to a scalar automatically.
                  Example: {name:'period_amount', type:'filter',
                            column:'interest', matchCol:'month_end',
                            matchValue:'postingdate'}
      • first   → schedule_first(sched, "<column>")    ⇒ scalar
                  Use when you need the opening-period value (e.g. initial balance).
      • last    → schedule_last(sched, "<column>")     ⇒ scalar
                  Use when you need the closing value at end of full schedule.
      • sum     → schedule_sum(sched, "<column>")      ⇒ scalar
                  Sums ALL rows — use for lifetime totals only (e.g. total
                  interest paid). Do NOT use sum to extract the current
                  period's value; use filter instead.
      • column  → schedule_column(sched, "<column>")   ⇒ array
    Every `column` MUST be the name of a defined column in
    scheduleConfig.columns. The validator rejects unknown column names.

  IMPORTANT — for FAS91, IFRS9, interest accrual, fee amortisation, and
  any rule where the transaction amount = the CURRENT PERIOD'S schedule
  row (not the lifetime sum), ALWAYS use type='filter' with
  matchCol='<your month-end date column>' and matchValue='postingdate'.
  Using type='sum' in these cases will post the TOTAL of ALL periods to
  every single posting date, which is wrong.

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
  2. NEVER use source='event_field' (scalar read) for amount or
     subId-specific fields when the event has multiple subIds per
     instrument. The engine merges multi-subId rows into ONE row before
     rule execution — so a scalar read only sees ONE subId's value (the
     last row in the merge). Instead:
       • Use source='collect', collectType='collect_by_instrument' to get
         ALL subId values for the current instrument as an array:
           {name:"balances", stepType:"calc", source:"collect",
            eventField:"EVT.balance", collectType:"collect_by_instrument"}
         Then iterate or index: apply_each(balances, "multiply(each, rate)")
       • Use collectType='collect_by_subinstrument' to get values for the
         (instrumentid, subinstrumentid) pair of the CURRENT row only.
     Fields that are identical across all subIds (postingdate, effectivedate,
     instrumentid, subinstrumentid, and shared attributes) are safe as
     scalars — the warning does not apply to those.
  3. To fan out a transaction PER SUB-INSTRUMENT explicitly (i.e. emit N
     transactions where N = number of subIds for the current instrument),
     add a calc step:
        {name:"sub_ids", stepType:"calc", source:"collect",
         eventField:"EVT.subinstrumentid",
         collectType:"collect_by_instrument"}
     Then reference `sub_ids` from the transaction's `subInstrumentId`.
     The engine will iterate the array and emit one txn per subId.
  4. To compute a per-subid amount (e.g. allocate by subId), use
     `collect_by_subinstrument(EVT.field)` — it returns the array of that
     field's values restricted to the current (instrumentid, subinstrumentid)
     pair so it does not collapse across subIds.

DETECTION — the validator surfaces multi-subid events automatically:
  When `tool_create_saved_rule` / `tool_update_saved_rule` /
  `tool_add_step_to_rule` / `tool_update_step` returns a payload
  containing `multi_subid_events: ["EVT", ...]`, read both:
  • `multi_subid_hint` — transaction subId fix (auto-applied)
  • `multi_subid_scalar_warnings[]` — list of calc steps using scalar
    event_field reads against a multi-subId event, with step-by-step
    fix instructions. Check each warning and convert the flagged steps
    to source='collect' where per-subId values are needed.

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


# ──────────────────────────────────────────────────────────────────────────
# D10: section-keyed syntax guide. The static `_DSL_SYNTAX_GUIDE` string is
# parsed once into headed sections so the agent can fetch one slice at a
# time when it only needs (e.g.) the schedule semantics or anti-patterns.
# ──────────────────────────────────────────────────────────────────────────
def _build_syntax_guide_sections(text: str) -> dict[str, str]:
    """Split _DSL_SYNTAX_GUIDE into named sections keyed by stable slugs.
    Sections are delimited by the all-caps banner lines (>= 3 dashes).
    Returns {slug: section_text}. Always includes 'all'."""
    sections: dict[str, str] = {"all": text}
    lines = text.splitlines()
    current_title: str | None = None
    buf: list[str] = []

    def _slug(t: str) -> str:
        s = re.sub(r"[^A-Za-z0-9]+", "_", t.strip().lower()).strip("_")
        return s or "section"

    def _flush():
        nonlocal buf, current_title
        if current_title is not None:
            sections[_slug(current_title)] = "\n".join(buf).strip()
        buf = []

    for i, ln in enumerate(lines):
        # A banner is two lines: the dashes, the TITLE, the dashes.
        if re.fullmatch(r"-{3,}", ln.strip()) and i + 2 < len(lines) \
           and re.fullmatch(r"-{3,}", lines[i + 2].strip()):
            _flush()
            current_title = lines[i + 1].strip()
            buf = [ln, lines[i + 1], lines[i + 2]]
            continue
        buf.append(ln)
    _flush()
    return sections


_DSL_SYNTAX_GUIDE_SECTIONS: dict[str, str] | None = None


def _syntax_guide_sections() -> dict[str, str]:
    global _DSL_SYNTAX_GUIDE_SECTIONS
    if _DSL_SYNTAX_GUIDE_SECTIONS is None:
        _DSL_SYNTAX_GUIDE_SECTIONS = _build_syntax_guide_sections(_DSL_SYNTAX_GUIDE)
    return _DSL_SYNTAX_GUIDE_SECTIONS


async def tool_get_dsl_syntax_guide(args: dict) -> dict:
    """Return the binding DSL constraints + worked examples of every step
    shape. Call this when you are unsure how to express something or after
    a syntax-class error.

    Optional args:
      section : str  — one of the section slugs returned by
                       list_sections=true; defaults to the full guide.
      list_sections : bool — if true, return only the available section
                             slugs (cheap).
    """
    args = args or {}
    secs = _syntax_guide_sections()
    if args.get("list_sections"):
        return {
            "sections": sorted(s for s in secs.keys() if s != "all"),
            "hint": "Pass section='<slug>' to get_dsl_syntax_guide to fetch one slice.",
        }
    sec = (args.get("section") or "").strip().lower()
    # Normalise the requested slug the same way section keys are built
    # so that e.g. "schedule" or "canonical_patterns" resolves correctly.
    def _slug(t: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", t).strip("_")
    sec_slug = _slug(sec)
    if sec_slug and sec_slug in secs:
        return {
            "section": sec_slug,
            "guide": secs[sec_slug],
            "available_sections": sorted(s for s in secs.keys() if s != "all"),
        }
    # Prefix / substring alias resolution — lets the model use short names
    # like "schedule" or "canonical_patterns" without knowing the full slug.
    if sec_slug:
        all_keys = [s for s in secs.keys() if s != "all"]
        # 1. prefix match
        prefix_hits = [k for k in all_keys if k.startswith(sec_slug)]
        # 2. substring match
        substr_hits = [k for k in all_keys if sec_slug in k]
        resolved = prefix_hits or substr_hits
        if len(resolved) == 1:
            return {
                "section": resolved[0],
                "guide": secs[resolved[0]],
                "available_sections": sorted(all_keys),
                "alias_resolved_from": sec,
            }
        if resolved:
            # Multiple matches — show them so the model can pick one
            raise ToolError(
                f"Ambiguous syntax-guide section '{sec}' — matches: {resolved}. "
                f"Use one of those exact slugs."
            )
        raise ToolError(
            f"Unknown syntax-guide section '{sec}'. Available: "
            f"{sorted(s for s in secs.keys() if s != 'all')}"
        )
    return {
        "guide": _DSL_SYNTAX_GUIDE,
        "function_count": len(_ServerBridge.helpers.get("DSL_FUNCTION_METADATA") or []),
        "available_sections": sorted(s for s in secs.keys() if s != "all"),
        "next_step_hint": (
            "Copy one of the step-shape examples above EXACTLY, then adapt "
            "the names/formulas. Use list_dsl_functions for the catalog of "
            "available functions."
        ),
    }


# ──────────────────────────────────────────────────────────────────────────
# A3: apply_canonical_pattern  — one-shot scaffold-to-rule.
# ──────────────────────────────────────────────────────────────────────────
def _substitute_pattern_tokens(node, mapping: dict[str, str]):
    if isinstance(node, str):
        out = node
        for tok, val in mapping.items():
            if not tok or not val:
                continue
            out = re.sub(rf"\b{re.escape(tok)}\b", val, out)
        return out
    if isinstance(node, list):
        return [_substitute_pattern_tokens(x, mapping) for x in node]
    if isinstance(node, dict):
        return {k: _substitute_pattern_tokens(v, mapping) for k, v in node.items()}
    return node


async def tool_apply_canonical_pattern(args: dict) -> dict:
    """Pre-fill a `create_saved_rule` payload from a canonical pattern."""
    from .knowledge import get_pattern
    pid = (args.get("pattern_id") or "").strip().upper()
    nm = (args.get("name") or "").strip()
    if not pid:
        raise ToolError("pattern_id is required (A|B|C|D)")
    if not nm:
        raise ToolError("name (new rule name) is required")
    pat = get_pattern(pid)
    if not pat:
        raise ToolError(f"unknown pattern_id '{pid}' — must be A/B/C/D")
    overrides = args.get("parameter_overrides") or {}
    if not isinstance(overrides, dict):
        raise ToolError("parameter_overrides must be an object")
    required = list((pat.get("parameters") or {}).keys())
    missing = [k for k in required if not str(overrides.get(k) or "").strip()]
    if missing:
        raise ToolError(
            f"Pattern {pid} requires overrides for {missing}. "
            f"Each parameter description: {pat.get('parameters')}"
        )
    mapping = {k: str(v).strip() for k, v in overrides.items() if str(v).strip()}
    sub_steps = _substitute_pattern_tokens(pat.get("steps") or [], mapping)
    sub_outputs = _substitute_pattern_tokens(pat.get("outputs") or {}, mapping)
    scaffold = {
        "name": nm,
        "priority": int(args.get("priority") or 100),
        "steps": sub_steps,
        "outputs": sub_outputs,
        "metadata": {
            "applied_pattern": pid,
            "pattern_name": pat.get("name"),
            "parameter_overrides": mapping,
        },
        "force_unplanned": bool(args.get("force_unplanned")),
    }
    if bool(args.get("preview_only")):
        return {
            "ok": True,
            "preview": scaffold,
            "pattern": {"id": pid, "name": pat.get("name"), "title": pat.get("title")},
            "transactions_emitted_hint": pat.get("transaction_types_emitted") or [],
        }
    created = await tool_create_saved_rule(scaffold)
    created["applied_pattern"] = {"id": pid, "name": pat.get("name")}
    return created


# ──────────────────────────────────────────────────────────────────────────
# F14: auto_pair_arrays — verify two collected arrays are index-aligned.
# ──────────────────────────────────────────────────────────────────────────
async def tool_auto_pair_arrays(args: dict) -> dict:
    rule_id = (args.get("rule_id") or "").strip()
    array_steps = args.get("array_step_names") or args.get("array_var_names") or []
    if not rule_id:
        raise ToolError("rule_id is required")
    if not isinstance(array_steps, list) or len(array_steps) < 2:
        raise ToolError("array_step_names must be a list of at least 2 step names")
    posting_date = args.get("posting_date")
    instrument_id = args.get("instrument_id")
    out: dict[str, dict] = {}
    for sn in array_steps:
        try:
            r = await tool_debug_step({
                "rule_id": rule_id,
                "step_name": sn,
                "posting_date": posting_date,
                "instrument_id": instrument_id,
            })
        except ToolError as e:
            out[sn] = {"ok": False, "error": str(e)}
            continue
        val = r.get("value")
        if isinstance(val, list):
            out[sn] = {"ok": True, "length": len(val), "head": val[:5]}
        else:
            out[sn] = {
                "ok": False,
                "error": f"step '{sn}' did not produce an array (got {type(val).__name__}).",
                "value_preview": val,
            }
    lengths = {k: v.get("length") for k, v in out.items() if v.get("ok")}
    aligned = bool(lengths) and len(set(lengths.values())) == 1
    suggested_fix = None
    if not aligned and lengths:
        max_len = max(lengths.values())
        shorter = [k for k, n in lengths.items() if n != max_len]
        suggested_fix = (
            f"Arrays have different lengths {lengths}. The shorter array(s) "
            f"{shorter} likely use `collect_all` over a reference event "
            f"while the others use `collect_by_instrument` over an activity "
            f"event. Make sure every collect step uses the SAME collector "
            f"(typically collect_by_instrument) over the SAME event so the "
            f"indices line up."
        )
    return {
        "rule_id": rule_id,
        "lengths": lengths,
        "aligned": aligned,
        "details": out,
        "suggested_fix": suggested_fix,
    }


# ──────────────────────────────────────────────────────────────────────────
# G15: dry_run_rule — execute one rule in isolation.
# ──────────────────────────────────────────────────────────────────────────
async def tool_dry_run_rule(args: dict) -> dict:
    db = _ServerBridge.db
    if db is None:
        raise ToolError("Database is not available")
    rule_id = (args.get("rule_id") or "").strip()
    if not rule_id:
        raise ToolError("rule_id is required")
    rule = await _load_rule(rule_id)
    code = rule.get("generatedCode") or _generate_rule_code(rule)
    transient_name = f"__dryrun_rule__{rule['id']}"
    extract_event_names = _h("extract_event_names_from_dsl")
    dsl_to_python = _h("dsl_to_python")
    DSLTemplate = _h("DSLTemplate")
    evt_names = list(extract_event_names(code) or [])
    if not evt_names:
        raise ToolError(
            f"Rule '{rule['name']}' references no events — nothing to run."
        )
    primary_event = evt_names[0]
    evt = await _find_event_def(primary_event)
    if not evt:
        raise ToolError(f"Event definition '{primary_event}' not found")
    try:
        py = dsl_to_python(code, evt["fields"])
    except Exception as exc:
        raise ToolError(f"Failed to compile rule to python: {exc}") from exc
    tmpl = DSLTemplate(name=transient_name, dsl_code=code, python_code=py)
    tdoc = tmpl.model_dump()
    if hasattr(tdoc.get("created_at"), "isoformat"):
        tdoc["created_at"] = tdoc["created_at"].isoformat()
    await db.dsl_templates.delete_many({"name": transient_name})
    await db.dsl_templates.insert_one(tdoc)
    try:
        result = await tool_dry_run_template({
            "name": transient_name,
            "posting_date": args.get("posting_date"),
            "effective_date": args.get("effective_date"),
            "sample_limit": args.get("sample_limit") or 5,
        })
    finally:
        try:
            await db.dsl_templates.delete_many({"name": transient_name})
        except Exception:
            pass
    return {
        "rule_id": rule["id"],
        "rule_name": rule["name"],
        "events_referenced": evt_names,
        "result": result,
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
    "patch_step": tool_patch_step,
    "replace_schedule_column": tool_replace_schedule_column,
    "list_canonical_patterns": tool_list_canonical_patterns,
    "get_canonical_pattern": tool_get_canonical_pattern,
    "find_similar_template": tool_find_similar_template,
    "submit_plan": tool_submit_plan,
    "apply_canonical_pattern": tool_apply_canonical_pattern,
    "auto_pair_arrays": tool_auto_pair_arrays,
    "dry_run_rule": tool_dry_run_rule,
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
            "no DB lookups; safe to call any time. Pass section='<slug>' to "
            "fetch one slice (call with list_sections=true to discover slugs)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "section": {"type": "string", "description": "Optional section slug to return only one slice."},
                "list_sections": {"type": "boolean", "description": "If true, return only the available section slugs."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "create_event_definitions",
        "description": (
            "Create one or more event definitions with their fields. Idempotent: existing names are skipped. "
            "REQUIRED RULES for eventType / eventTable: "
            "(1) ACTIVITY events (transactional, one row per posting date): eventType='activity', eventTable='standard'. "
            "(2) REFERENCE / LOOKUP TABLES (catalogs, rate tables, product lists, SSP tables, chart of accounts, "
            "any static lookup): eventType='reference', eventTable='custom'. BOTH fields are mandatory together — "
            "never set eventType='reference' with eventTable='standard'. "
            "The validator auto-corrects obvious name-based mismatches (catalog/ref/lookup/master/mapping in the "
            "event name) and reports coerced_to_reference=true in the response."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "events": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "event_name": {"type": "string"},
                            "eventType": {"type": "string", "enum": ["activity", "reference"],
                                          "description": "activity = transactional (default). reference = static lookup table (must pair with eventTable='custom')."},
                            "eventTable": {"type": "string", "enum": ["standard", "custom"],
                                           "description": "standard = activity events (default). custom = reference/lookup tables (must pair with eventType='reference')."},
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
        "description": (
            "Generate deterministic, accounting-standards-coherent synthetic rows for an event definition. "
            "The generator detects the accounting domain from the event name and field names and produces "
            "INTERNALLY CONSISTENT data: loan balances amortise over time, ECL scales with PD×LGD×EAD, "
            "ROU assets and lease liabilities decline via annuity amortisation, NBV = cost − accumulated "
            "depreciation, recognised revenue accumulates straight-line, bond accrued interest follows "
            "coupon/365. MANDATORY USAGE RULES:\n"
            "0. REFERENCE EVENTS FIRST — CRITICAL: if any of the event definitions are reference/lookup "
            "tables (eventType='reference'), you MUST call generate_sample_event_data for those reference "
            "events BEFORE calling it for activity events. The generator automatically detects field-name "
            "matches between reference and activity events and seeds the activity data from the reference "
            "values — so the reference data must exist first. For example: if PRODUCT_CATALOG has field "
            "'product_type' with rows ['SaaS','Service'], and CONTRACTS also has 'product_type', then "
            "CONTRACTS rows will only contain 'SaaS' or 'Service' — never a made-up value. The response "
            "will include 'reference_seeded_fields' listing which fields were constrained this way.\n"
            "1. INSTRUMENT IDS: use domain-meaningful IDs — 'LN-001','LN-002' for loans/FAS91; "
            "'LEASE-001','LEASE-002' for IFRS16/ASC842; 'FA-001','FA-002' for fixed assets; "
            "'CONT-001','CONT-002' for revenue contracts; 'BOND-001','BOND-002' for securities; "
            "'ECL-001','ECL-002' for credit/IFRS9 events.\n"
            "2. POSTING DATES: always supply 3–6 monthly dates (e.g. ['2025-10-31','2025-11-30',"
            "'2025-12-31','2026-01-31','2026-02-28','2026-03-31']) so time-series fields "
            "(balance, accumulated depreciation, deferred revenue, etc.) show realistic evolution.\n"
            "3. FIELD NAMES DRIVE DOMAIN DETECTION: use exact accounting-standard names so the "
            "right profile is selected. FAS91: loan_amount, origination_fee, note_rate, eir_rate, "
            "outstanding_balance, amortized_fee, origination_date, maturity_date. "
            "IFRS9: ead, pd, lgd, ecl, stage, days_past_due, credit_impaired, collateral_value. "
            "IFRS16: rou_asset, lease_liability, lease_payment, incremental_borrowing_rate, lease_term. "
            "IAS16: acquisition_cost, residual_value, useful_life_years, accumulated_depreciation, nbv. "
            "IFRS15: contract_amount, ssp, allocated_amount, recognized_revenue, deferred_revenue. "
            "Securities: face_value, coupon_rate, market_value/fair_value, accrued_interest.\n"
            "4. RATES must be in decimal form: 5 % = 0.05, NEVER 5.\n"
            "5. Use field_hints only to override ranges for custom fields not in the standard templates."
        ),
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
            "If you built or modified one or more rules, ALSO pass `rule_id` (single) "
            "OR `rule_ids` (list) so the runtime can verify each rule has at least "
            "one balanced debit/credit pair in `outputs.transactions[]` before "
            "accepting completion. The runtime auto-injects every rule it has seen "
            "you touch this turn, so the gate runs even if you forget to pass an id."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "rule_id": {"type": "string"},
                "rule_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of rule ids to gate (in addition to rule_id).",
                },
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
        "id": {"type": "string", "description": "Immutable step identifier (UUID). Auto-generated on first save; preserve verbatim when patching. Used by update_step / delete_step / patch_step to address a step even after rename."},
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
        "outputVars": {
            "type": "array",
            "items": {"type": "object"},
            "description": (
                "REQUIRED for stepType:'schedule'. Must contain at least one entry — a schedule step "
                "without outputVars is invisible in the modal and cannot be referenced by downstream steps. "
                "PRIORITY ORDER: "
                "1) filter (preferred for date-range schedules) — picks the row matching a date: "
                "{name, type:'filter', column:<value-col>, matchCol:'period_date', matchValue:'postingdate'}. "
                "2) last — final/closing value: {name, type:'last', column:<col>}. "
                "3) sum — lifetime total: {name, type:'sum', column:<col>}. "
                "4) first — opening value: {name, type:'first', column:<col>}. "
                "5) column — full array: {name, type:'column', column:<col>}. "
                "Always include a filter+last pair for date-range schedules, last+sum for number-period."
            ),
        },
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
        "description": (
            "DESTRUCTIVE: delete a saved rule. Requires confirm=true and "
            "user approval. Refuses if any user_template still references "
            "the rule unless force=true (which orphans the references)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "confirm": {"type": "boolean"},
                "force":   {"type": "boolean", "description": "Bypass reference-integrity check; orphans templates."},
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
        "description": (
            "Patch one step inside a rule via DEEP MERGE. Identify by "
            "`step_id` (preferred — immutable, survives renames), "
            "`step_index`, OR `step_name`. The `patch` is recursively "
            "merged into the existing step doc — nested objects like "
            "`scheduleConfig` keep their sibling fields intact (FIXED: "
            "earlier shallow merge wiped them). To swap a whole list "
            "(e.g. all schedule columns), pass the full new list. To "
            "surgically edit ONE leaf (e.g. one column's formula), prefer "
            "`patch_step` or `replace_schedule_column`. Always re-fetches "
            "and verifies the patch persisted; check `persisted.ok` in the "
            "response."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_id": {"type": "string", "description": "PREFERRED — immutable UUID from get_saved_rule"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
                "patch": {"type": "object"},
            },
            "required": ["rule_id", "patch"],
        },
    },
    {
        "name": "delete_step",
        "description": "Remove one step from a rule. Identify by step_id (preferred), step_index, OR step_name. Verifies removal in the persisted doc.",
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
            },
            "required": ["rule_id"],
        },
    },
    {
        "name": "patch_step",
        "description": (
            "Surgical step editor using JSON-Pointer (RFC 6902) ops. Use "
            "this when you need to change ONE leaf inside a deeply nested "
            "step doc — e.g. fix a single schedule column's formula without "
            "re-sending all the others. ops is a list of "
            "{op:'replace'|'add'|'remove', path:'/scheduleConfig/columns/2/formula', value:'...'}. "
            "Index '-' on a list means append. Re-validates the full step "
            "after applying ops, and verifies every requested path landed."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "step_id": {"type": "string"},
                "step_index": {"type": "integer"},
                "step_name": {"type": "string"},
                "ops": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "op":    {"type": "string", "enum": ["replace", "add", "remove"]},
                            "path":  {"type": "string", "description": "JSON-Pointer, e.g. /scheduleConfig/columns/0/formula"},
                            "value": {},
                        },
                        "required": ["op", "path"],
                    },
                },
            },
            "required": ["rule_id", "ops"],
        },
    },
    {
        "name": "replace_schedule_column",
        "description": (
            "Convenience: rename or change the formula of ONE existing "
            "schedule column without re-sending the entire scheduleConfig. "
            "Internally translates to patch_step with the right JSON-Pointer "
            "path. Pass column_name (the existing column to target) plus "
            "at least one of new_formula / new_name."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id":     {"type": "string"},
                "step_id":     {"type": "string"},
                "step_name":   {"type": "string"},
                "column_name": {"type": "string"},
                "new_formula": {"type": "string"},
                "new_name":    {"type": "string"},
            },
            "required": ["rule_id", "column_name"],
        },
    },
    {
        "name": "list_canonical_patterns",
        "description": (
            "Return the menu of canonical accounting patterns A/B/C/D the "
            "agent should pick BEFORE writing a non-trivial rule. Each "
            "entry has id, name, title, when_to_use[], transaction types "
            "typically emitted. Cheap; safe to call any time."
        ),
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "get_canonical_pattern",
        "description": (
            "Fetch a canonical pattern (A/B/C/D) — returns the FULL step "
            "scaffold (copy-pasteable into create_saved_rule), parameter "
            "substitutions, transaction types and anti-patterns. Use BEFORE "
            "authoring schedule/iteration steps to avoid trial-and-error."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "pattern_id": {"type": "string", "enum": ["A", "B", "C", "D"]},
            },
            "required": ["pattern_id"],
        },
    },
    {
        "name": "find_similar_template",
        "description": (
            "Given a free-text intent (and optional keywords), rank the "
            "canonical patterns by relevance AND list saved rules with "
            "similar names. Call this first when the user describes what "
            "they want — it routes you to the right pattern + lets you "
            "reuse existing rules instead of rebuilding."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "intent":   {"type": "string"},
                "keywords": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "submit_plan",
        "description": (
            "Record an explicit build plan BEFORE any create/update tool "
            "call. Forces commitment to one canonical pattern, lists the "
            "rules to be created and the transactions each will emit. "
            "Returns the plan + the canonical pattern fixture so the next "
            "create_saved_rule call can paste the scaffold directly. "
            "Skipping this step is the dominant cause of trial-and-error "
            "loops — ALWAYS call it for any 'build me…' request."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "run_id":            {"type": "string"},
                "intent":            {"type": "string"},
                "pattern_id":        {"type": "string", "enum": ["A", "B", "C", "D"]},
                "events_needed":     {"type": "array", "items": {"type": "string"}},
                "transaction_types": {"type": "array", "items": {"type": "string"}},
                "rules": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name":          {"type": "string"},
                            "intent":        {"type": "string"},
                            "steps_outline": {"type": "array", "items": {"type": "string"}},
                            "transactions":  {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["name"],
                    },
                },
            },
            "required": ["intent", "pattern_id", "rules"],
        },
    },
    {
        "name": "apply_canonical_pattern",
        "description": (
            "One-shot: pre-fill a `create_saved_rule` payload from canonical "
            "pattern A/B/C/D, substituting parameter tokens (EVENT, "
            "AMOUNT_FIELD, …) with the supplied values, then create the rule. "
            "Use this INSTEAD of hand-authoring steps when the request maps "
            "cleanly onto a known pattern. Pass preview_only=true to inspect "
            "the substituted scaffold without persisting."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "pattern_id": {"type": "string", "enum": ["A", "B", "C", "D"]},
                "name": {"type": "string"},
                "parameter_overrides": {
                    "type": "object",
                    "description": "Map of pattern parameter names to actual values (e.g. {EVENT:'EOD', AMOUNT_FIELD:'ATTRIBUTE_LOAN_AMOUNT'}).",
                },
                "priority": {"type": "integer", "default": 100},
                "preview_only": {"type": "boolean", "default": False},
                "force_unplanned": {"type": "boolean", "default": False},
            },
            "required": ["pattern_id", "name", "parameter_overrides"],
        },
    },
    {
        "name": "auto_pair_arrays",
        "description": (
            "Verify two (or more) array-producing steps in a rule yield "
            "lengths that line up index-for-index. Call this BEFORE writing "
            "an iteration step that walks multiple arrays so misaligned "
            "collects (collect_all vs collect_by_instrument) are caught "
            "early instead of silently producing wrong rows."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "array_step_names": {"type": "array", "items": {"type": "string"}, "minItems": 2},
                "posting_date": {"type": "string"},
                "instrument_id": {"type": "string"},
            },
            "required": ["rule_id", "array_step_names"],
        },
    },
    {
        "name": "dry_run_rule",
        "description": (
            "Execute ONE saved rule against current event data and return "
            "summary (transactions emitted, prints, errors). Use this for "
            "per-rule debugging before assembling a multi-rule template. "
            "Does NOT persist transaction reports."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "posting_date": {"type": "string"},
                "effective_date": {"type": "string"},
                "sample_limit": {"type": "integer", "default": 5},
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
                "step_id": {"type": "string"},
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
                "step_id": {"type": "string"},
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


# ──────────────────────────────────────────────────────────────────────────
# A1 + A2: gate write tools behind an explicit submitted plan. A new run
# (run_id) MUST call submit_plan before invoking any of the mutator tools
# below, so the agent commits to a canonical pattern up front instead of
# trial-and-erroring its way into half-built rules. Read-only tools are
# never gated. The `force_unplanned=true` arg on the call is an explicit
# escape hatch for programmatic callers / test harnesses.
# ──────────────────────────────────────────────────────────────────────────
PLAN_GATED_TOOLS: set[str] = {
    # Only NEW-rule / NEW-template creation is gated. Edits to an existing
    # rule (add_step_to_rule, update_step, patch_step, add_transaction_to_rule,
    # etc.) are continuation work — they imply a plan was already committed
    # in a prior turn, and gating them strands follow-up turns ("yes go
    # ahead", "now extend this rule") because each turn gets a fresh
    # run_id. The dispatch-time gate also auto-synthesises a default plan
    # for any gated tool that names an existing rule_id, so the only path
    # that ever surfaces the gate error is: brand new conversation, no
    # prior rule, calling create_saved_rule with no submit_plan first.
    "create_saved_rule",
    "create_or_replace_template",
    "apply_canonical_pattern",
}


async def dispatch_tool(name: str, args: dict) -> dict:
    """Look up `name` in the registry and execute. Raises ToolError on unknown."""
    fn = TOOLS.get(name)
    if fn is None:
        raise ToolError(f"Unknown tool '{name}'. Available: {sorted(TOOLS.keys())}")
    if not isinstance(args, dict):
        args = {}
    # ── A1 + A2 gate ─────────────────────────────────────────────────
    if name in PLAN_GATED_TOOLS and not bool(args.get("force_unplanned")):
        rid = (current_run_id.get() or "").strip()
        plan_key = rid or "default"
        if plan_key not in _RUN_PLANS:
            # Self-healing: if a plan already exists for ANY recent run_id
            # in this process, inherit the most-recent one. This handles the
            # common case where the user kicks off a follow-up turn ("yes go
            # ahead") that gets a fresh run_id but is semantically the same
            # build. Without this, plan-gating becomes a one-shot footgun
            # that strands every continuation turn.
            if _RUN_PLANS:
                last_key = next(reversed(_RUN_PLANS))
                _RUN_PLANS[plan_key] = dict(_RUN_PLANS[last_key])
                _RUN_PLANS[plan_key]["run_id"] = plan_key
                _RUN_PLANS[plan_key]["inherited_from"] = last_key
                logger.info(
                    "plan-gate: inheriting plan from run_id=%s into %s for "
                    "tool '%s' (continuation turn)", last_key, plan_key, name)
            else:
                # Genuinely brand-new conversation, no prior plan anywhere.
                # Synthesise a minimal default so the gate never becomes a
                # dead-end. The model can still submit a richer plan later;
                # this only ensures the build is unblocked. Pattern A is
                # the most common (schedule-with-filter); for a bare
                # create_saved_rule we don't actually need a real pattern,
                # we just need the gate to pass.
                from .knowledge import get_pattern
                default_intent = (
                    (isinstance(args, dict) and (args.get("name") or args.get("intent")))
                    or "auto-synthesised plan (no submit_plan was called)"
                )
                _RUN_PLANS[plan_key] = {
                    "run_id": plan_key,
                    "intent": str(default_intent),
                    "pattern_id": "A",
                    "events_needed": [],
                    "transaction_types": [],
                    "rules": [],
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                    "auto_synthesised": True,
                }
                logger.info(
                    "plan-gate: auto-synthesised default plan for run_id=%s "
                    "(tool '%s' was called without submit_plan)", plan_key, name)
    return await fn(args)
