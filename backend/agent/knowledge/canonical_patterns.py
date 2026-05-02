"""Canonical accounting/DSL patterns extracted from production templates.

Each pattern is a copy-pasteable scaffold the agent should retrieve via
`get_canonical_pattern(pattern_id)` BEFORE writing schedule/iteration steps.
The `steps[]` field is shaped exactly like `_validate_step_shape` expects so
the agent can paste it into `create_saved_rule` with only the parameter
substitutions called out in `parameters[]`.

Patterns
--------
A : Schedule + filter (amortisation / IFRS9 / FAS91 fee amortisation style)
    - One `period(start_date, end_date, "M")` schedule
    - Lag references previous-period values
    - outputVar type='filter' (matchCol=month_end, matchValue=postingdate) ← PRIMARY
    - outputVar type='first' for opening-balance extraction
    - outputVar type='last' for closing-balance extraction
    - schedule_sum only for lifetime totals (NOT per-period amounts)
    - Source: IFRSStage3, loan_amortization template

B : Collect + apply_each + schedule (revenue allocation style)
    - collect_all() over a reference table
    - apply_each iteration to allocate amounts (e.g. SSP-weighted)
    - Schedule with allocation column, schedule_filter at posting_date
    - Source: RevenueFinal111

C : Replay timeline + lag + delta (SBO style)
    - collect_by_instrument() builds an event timeline
    - period(N, "M") in count form
    - schedule columns use lag('col',1,default)
    - schedule_last + delta arithmetic
    - Source: SBO_Replay_M1, SBO_REPLAY_M2

D : Scalar finance (NPV / present value / single-figure)
    - One or more calc steps using pmt/npv/pow/discount math
    - No schedule needed
    - Source: npv_analysis template
"""

from __future__ import annotations
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Pattern A — Amortisation / IFRS9 ECL projection
# ─────────────────────────────────────────────────────────────────────────────
PATTERN_A = {
    "id": "A",
    "name": "schedule_with_filter",
    "title": "Date-range amortisation schedule with filter (per-period) outputs",
    "when_to_use": [
        "Loan amortisation (principal + interest run-off)",
        "IFRS 9 / CECL ECL projection over remaining term",
        "Lease liability run-off (IFRS 16 / ASC 842)",
        "Any period-by-period balance roll-forward where each row depends on prior row",
    ],
    "events_required": [
        "One activity event with: postingdate, effectivedate, instrumentid, "
        "subinstrumentid, plus the loan/lease attributes (LoanAmount, Term, "
        "NoteRate, OriginationDate, beginningBalance, endingBalance).",
    ],
    "transaction_types_emitted": [
        "INTEREST_ACCRUAL", "ORIGINATION_PRINCIPAL", "IMPAIRMENT_GAIN",
        "IMPAIRMENT_LOSS",
    ],
    "parameters": {
        "EVENT": "Name of the activity event (e.g. EOD).",
        "AMOUNT_FIELD": "Field holding loan amount (e.g. ATTRIBUTE_LOANAMOUNT_CURRENT).",
        "TERM_FIELD": "Field holding term in months.",
        "RATE_FIELD": "Field holding annual rate (percent, NOT decimal).",
        "ORIG_DATE_FIELD": "Field holding origination date.",
    },
    "steps": [
        # --- Stage 1: parameter calc steps ---
        {"name": "postingdate",  "stepType": "calc", "source": "event_field", "eventField": "EVENT.postingdate"},
        {"name": "effectivedate","stepType": "calc", "source": "event_field", "eventField": "EVENT.effectivedate"},
        {"name": "subinstrumentid","stepType": "calc","source": "event_field","eventField": "EVENT.subinstrumentid"},
        {"name": "OriginationDate","stepType": "calc","source": "event_field","eventField": "EVENT.ORIG_DATE_FIELD"},
        {"name": "LoanAmount",   "stepType": "calc", "source": "event_field", "eventField": "EVENT.AMOUNT_FIELD"},
        {"name": "Term",         "stepType": "calc", "source": "event_field", "eventField": "EVENT.TERM_FIELD"},
        {"name": "NoteRate",     "stepType": "calc", "source": "event_field", "eventField": "EVENT.RATE_FIELD"},
        {"name": "Monthly_Rate", "stepType": "calc", "source": "formula",
         "formula": "NoteRate/1200"},
        {"name": "PMT_AM",       "stepType": "calc", "source": "formula",
         "formula": "pmt(Monthly_Rate, Term, -LoanAmount)"},
        {"name": "First_Month",  "stepType": "calc", "source": "formula",
         "formula": "months_between(OriginationDate, postingdate)"},
        {"name": "maturitydate", "stepType": "calc", "source": "formula",
         "formula": "add_months(OriginationDate, Term)"},
        # Reset UPB on origination date else use prior ending balance
        {"name": "UPB", "stepType": "condition",
         "conditions": [{
             "condition": "eq(OriginationDate, postingdate)",
             "thenFormula": "LoanAmount",
         }],
         "elseFormula": "EVENT.endingBalance"},
        # --- Stage 2: the schedule ---
        {
            "name": "Schedule",
            "stepType": "schedule",
            "scheduleConfig": {
                "periodType": "date",
                "frequency": "M",
                "startDateSource": "value", "startDate": "postingdate",
                "endDateSource":   "value", "endDate":   "maturitydate",
                "columns": [
                    {"name": "period_date",      "formula": "period_date"},
                    {"name": "month_end",        "formula": "end_of_month(period_date)"},
                    {"name": "monthNumber",      "formula": "iif(eq(period_index,0), First_Month, lag('monthNumber',1,First_Month)+1)"},
                    {"name": "openingBalance",   "formula": "iif(eq(period_index,0), UPB, lag('closingBalance',1,0))"},
                    {"name": "interestAccrued",  "formula": "multiply(openingBalance, Monthly_Rate)"},
                    {"name": "contractualCF",    "formula": "iif(eq(month_end, maturitydate), add(interestAccrued, lag('closingBalance',1,0)), PMT_AM)"},
                    {"name": "principalPayment", "formula": "contractualCF - interestAccrued"},
                    {"name": "closingBalance",   "formula": "subtract(openingBalance, principalPayment)"},
                ],
            },
            "outputVars": [
                # filter = PREFERRED: extracts THIS PERIOD'S row by matching
                # the schedule's month-end date column to the posting date.
                # Use this for any amount that should be posted once per period.
                {"name": "interestAccrual",  "type": "filter", "column": "interestAccrued",
                 "matchCol": "month_end", "matchValue": "postingdate"},
                {"name": "MonthEndPrincipal","type": "filter","column": "principalPayment",
                 "matchCol": "month_end", "matchValue": "postingdate"},
                # first / last: opening and closing balance scalars.
                {"name": "OpeningBalance",   "type": "first", "column": "openingBalance"},
                {"name": "FinalBalance",     "type": "last",  "column": "closingBalance"},
                # sum: LIFETIME total — only use when you truly need all-period aggregate.
                # {"name": "TotalInterest", "type": "sum",    "column": "interestAccrued"},
            ],
        },
        # --- Stage 3: derive transaction amounts ---
        {"name": "Interest_Accrual", "stepType": "condition",
         "conditions": [{"condition": "eq(OriginationDate, postingdate)",
                         "thenFormula": "0"}],
         "elseFormula": "interestAccrual"},
        {"name": "Origination_Principal", "stepType": "condition",
         "conditions": [{"condition": "eq(OriginationDate, postingdate)",
                         "thenFormula": "LoanAmount"}],
         "elseFormula": "0"},
    ],
    "outputs_transactions_template": [
        {"type": "INTEREST_ACCRUAL",      "amount": "Interest_Accrual",      "side": "debit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
        {"type": "INTEREST_ACCRUAL",      "amount": "Interest_Accrual",      "side": "credit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
        {"type": "ORIGINATION_PRINCIPAL", "amount": "Origination_Principal", "side": "debit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
        {"type": "ORIGINATION_PRINCIPAL", "amount": "Origination_Principal", "side": "credit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
    ],
    "anti_patterns": [
        "Do NOT use iteration steps to roll a balance forward — use a schedule.",
        "Do NOT reference `arr[i]` indexing in formulas — use `lag('col',1,default)` inside a schedule.",
        "Do NOT put `createTransaction(...)` in a calc formula — populate `outputs.transactions[]`.",
        "Do NOT forget the `subinstrumentid` field on transactions — use the row builtin or a calc step.",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Pattern B — Revenue allocation (collect + apply_each + schedule)
# ─────────────────────────────────────────────────────────────────────────────
PATTERN_B = {
    "id": "B",
    "name": "collect_apply_each_schedule",
    "title": "Collect catalog → allocate via apply_each → schedule with filter",
    "when_to_use": [
        "IFRS 15 / ASC 606 revenue recognition with multiple POs",
        "SSP-based transaction-price allocation",
        "Any pattern where a per-row event combines with a reference catalog "
        "and revenue must be recognised over a date range",
    ],
    "events_required": [
        "Activity event (e.g. SO) with order amount + start/end dates per PO.",
        "Reference table (e.g. CATALOG) with product → SSP mapping.",
    ],
    "transaction_types_emitted": ["RevenueRecognised", "ContractAsset"],
    "parameters": {
        "SO_EVENT": "Sales order activity event.",
        "CATALOG_EVENT": "Catalog reference table.",
        "PRODUCT_FIELD": "Product id field on both events.",
        "AMOUNT_FIELD": "Order amount on SO.",
        "SSP_FIELD": "Standalone Selling Price on CATALOG.",
        "START_FIELD": "Performance obligation start date.",
        "END_FIELD":   "Performance obligation end date.",
    },
    "steps": [
        {"name": "ProductIds", "stepType": "calc", "source": "collect",
         "collectType": "collect_all", "eventField": "CATALOG_EVENT.PRODUCT_FIELD"},
        {"name": "SSPs",       "stepType": "calc", "source": "collect",
         "collectType": "collect_all", "eventField": "CATALOG_EVENT.SSP_FIELD"},
        {"name": "OrderAmount","stepType": "calc", "source": "event_field",
         "eventField": "SO_EVENT.AMOUNT_FIELD"},
        {"name": "TotalSSP",   "stepType": "calc", "source": "formula",
         "formula": "sum(SSPs)"},
        {"name": "AllocatedAmounts", "stepType": "iteration",
         "iterations": [{
             "type": "apply_each",
             "sourceArray": "SSPs",
             "varName": "each",
             "expression": "multiply(divide(each, TotalSSP), OrderAmount)",
             "resultVar": "AllocatedAmounts",
         }]},
        {"name": "start_dates","stepType": "calc","source": "event_field",
         "eventField": "SO_EVENT.START_FIELD"},
        {"name": "end_dates",  "stepType": "calc","source": "event_field",
         "eventField": "SO_EVENT.END_FIELD"},
        {
            "name": "Schedule",
            "stepType": "schedule",
            "scheduleConfig": {
                "periodType": "date",
                "frequency": "M",
                "startDateSource": "value", "startDate": "start_dates",
                "endDateSource":   "value", "endDate":   "end_dates",
                "columns": [
                    {"name": "period_date", "formula": "period_date"},
                    {"name": "month_end",   "formula": "end_of_month(period_date)"},
                    {"name": "period_revenue",
                     "formula": "divide(lookup(AllocatedAmounts, ProductIds, item_name), total_periods)"},
                    {"name": "LTD_revenue",
                     "formula": "iif(eq(period_index,0), period_revenue, lag('LTD_revenue',1,0)+period_revenue)"},
                ],
            },
            "outputVars": [
                {"name": "PeriodRevenue", "type": "filter", "column": "period_revenue",
                 "matchCol": "month_end", "matchValue": "postingdate"},
                {"name": "LTDRevenue",    "type": "sum",    "column": "period_revenue"},
            ],
        },
    ],
    "outputs_transactions_template": [
        {"type": "RevenueRecognised", "amount": "PeriodRevenue", "side": "credit",
         "postingdate": "SO_EVENT.postingdate", "effectivedate": "SO_EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
        {"type": "ContractAsset",     "amount": "PeriodRevenue", "side": "debit",
         "postingdate": "SO_EVENT.postingdate", "effectivedate": "SO_EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
    ],
    "anti_patterns": [
        "Do NOT iterate over `all_instruments` — there is no such variable.",
        "`AllocatedAmounts` and `SSPs` and `ProductIds` MUST stay index-aligned — "
        "all three come from the same CATALOG reference table.",
        "Use schedule_filter (matchCol=month_end, matchValue=postingdate) to pick "
        "the row that lands on the current posting date.",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Pattern C — Replay timeline + lag + delta (SBO style)
# ─────────────────────────────────────────────────────────────────────────────
PATTERN_C = {
    "id": "C",
    "name": "replay_timeline_lag_delta",
    "title": "Collect event timeline → period-count schedule with lag → delta",
    "when_to_use": [
        "Replay/back-fill calculations (e.g. servicing balance restatement)",
        "Patterns that build a per-instrument timeline from prior events and "
        "recompute a balance period-by-period",
        "Anywhere you need `period(N, \"M\")` count form (no end date known)",
    ],
    "events_required": [
        "Activity event with one row per (instrument, subinstrument) timestep "
        "carrying the inputs to the recomputation.",
    ],
    "transaction_types_emitted": ["ReplayAdjustment", "BalanceTransfer"],
    "parameters": {
        "EVENT": "The activity event holding the replay inputs.",
        "EFFECTIVE_FIELD": "Field holding each step's effectivedate.",
        "BALANCE_FIELD": "Field holding the per-step balance to replay.",
    },
    "steps": [
        {"name": "timeline", "stepType": "calc", "source": "collect",
         "collectType": "collect_by_instrument", "eventField": "EVENT.EFFECTIVE_FIELD"},
        {"name": "balances", "stepType": "calc", "source": "collect",
         "collectType": "collect_by_instrument", "eventField": "EVENT.BALANCE_FIELD"},
        {"name": "timeline_count", "stepType": "calc", "source": "formula",
         "formula": "len(timeline)"},
        {
            "name": "Schedule",
            "stepType": "schedule",
            "scheduleConfig": {
                "periodType": "number",
                "frequency": "M",
                "periodCountSource": "value", "periodCount": "timeline_count",
                "columns": [
                    {"name": "Begin_UPB",  "formula": "lag('End_UPB', 1, 0)"},
                    {"name": "Activity",   "formula": "lookup(balances, timeline, period_index)"},
                    {"name": "End_UPB",    "formula": "Begin_UPB + Activity"},
                ],
            },
            "outputVars": [
                {"name": "FinalBalance", "type": "last", "column": "End_UPB"},
                {"name": "TotalActivity","type": "sum",  "column": "Activity"},
            ],
        },
        {"name": "Adjustment", "stepType": "calc", "source": "formula",
         "formula": "FinalBalance - EVENT.BALANCE_FIELD"},
    ],
    "outputs_transactions_template": [
        {"type": "ReplayAdjustment", "amount": "Adjustment", "side": "debit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
        {"type": "ReplayAdjustment", "amount": "Adjustment", "side": "credit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate",
         "subinstrumentid": "subinstrumentid"},
    ],
    "anti_patterns": [
        "`periodCountSource` must be 'value' (or 'formula') with the count expression — "
        "not a date range.",
        "lag default MUST match the column type (0 for numeric).",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Pattern D — Scalar finance (NPV / single-figure)
# ─────────────────────────────────────────────────────────────────────────────
PATTERN_D = {
    "id": "D",
    "name": "scalar_finance",
    "title": "Scalar present value / NPV / IRR — no schedule",
    "when_to_use": [
        "NPV of a fixed cashflow stream",
        "IRR / yield-to-maturity calculations",
        "Single-figure financial computations (no period-by-period roll-forward)",
    ],
    "events_required": [
        "One activity event carrying cashflows array + discount rate.",
    ],
    "transaction_types_emitted": ["ValuationGain", "ValuationLoss"],
    "parameters": {
        "EVENT": "Activity event with cashflows + rate.",
        "CF_FIELD": "Field holding the cashflow array.",
        "RATE_FIELD": "Field holding the discount rate (decimal).",
    },
    "steps": [
        {"name": "cashflows", "stepType": "calc", "source": "collect",
         "collectType": "collect_by_instrument", "eventField": "EVENT.CF_FIELD"},
        {"name": "rate",      "stepType": "calc", "source": "event_field",
         "eventField": "EVENT.RATE_FIELD"},
        {"name": "NPV",       "stepType": "calc", "source": "formula",
         "formula": "npv(rate, cashflows)"},
        {"name": "Gain", "stepType": "condition",
         "conditions": [{"condition": "gt(NPV, 0)", "thenFormula": "NPV"}],
         "elseFormula": "0"},
        {"name": "Loss", "stepType": "condition",
         "conditions": [{"condition": "lt(NPV, 0)", "thenFormula": "abs(NPV)"}],
         "elseFormula": "0"},
    ],
    "outputs_transactions_template": [
        {"type": "ValuationGain", "amount": "Gain", "side": "credit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate"},
        {"type": "ValuationGain", "amount": "Gain", "side": "debit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate"},
        {"type": "ValuationLoss", "amount": "Loss", "side": "debit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate"},
        {"type": "ValuationLoss", "amount": "Loss", "side": "credit",
         "postingdate": "EVENT.postingdate", "effectivedate": "EVENT.effectivedate"},
    ],
    "anti_patterns": [
        "Don't build a schedule when no period-by-period state is needed.",
    ],
}


CANONICAL_PATTERNS: dict[str, dict[str, Any]] = {
    "A": PATTERN_A,
    "B": PATTERN_B,
    "C": PATTERN_C,
    "D": PATTERN_D,
}


def list_patterns() -> list[dict]:
    """Return brief summaries (id/name/title/when_to_use) for every pattern."""
    return [
        {
            "id": p["id"],
            "name": p["name"],
            "title": p["title"],
            "when_to_use": p["when_to_use"],
            "transaction_types_emitted": p["transaction_types_emitted"],
        }
        for p in CANONICAL_PATTERNS.values()
    ]


def get_pattern(pattern_id: str) -> dict | None:
    if not pattern_id:
        return None
    return CANONICAL_PATTERNS.get(pattern_id.strip().upper())


# Lightweight intent → pattern matcher used by `find_similar_template`.
_INTENT_KEYWORDS = {
    "A": [
        "amortis", "amortiz", "loan", "ifrs9", "ifrs 9", "ecl", "stage",
        "impairment", "lease", "ifrs16", "asc842", "depreciation", "interest",
        "principal", "schedule", "run-off", "runoff",
    ],
    "B": [
        "revenue", "ifrs15", "ifrs 15", "asc606", "ssp", "allocation",
        "performance obligation", "po ", "catalog", "deferred revenue",
        "contract asset", "recognise revenue", "recognize revenue",
    ],
    "C": [
        "replay", "back-fill", "backfill", "restate", "restatement", "sbo",
        "servicing", "timeline", "balance roll", "rollforward",
        "recompute", "rebuild balance",
    ],
    "D": [
        "npv", "present value", "irr", "yield", "valuation", "fair value",
        "discount", "scalar", "single value",
    ],
}


def match_pattern_by_intent(intent: str, keywords: list[str] | None = None) -> list[dict]:
    """Score each pattern against the user intent + extra keywords.

    Returns a list of {pattern_id, score, why} sorted by score desc. Score is
    the count of matched keywords (case-insensitive). Patterns with score 0
    are still returned (so the agent always sees the menu) but ranked last.
    """
    text = (intent or "").lower()
    extra = " ".join((k or "").lower() for k in (keywords or []))
    haystack = f"{text}\n{extra}"
    out: list[dict] = []
    for pid, kws in _INTENT_KEYWORDS.items():
        hits = [k for k in kws if k in haystack]
        out.append({
            "pattern_id": pid,
            "score": len(hits),
            "matched": hits,
            "title": CANONICAL_PATTERNS[pid]["title"],
        })
    out.sort(key=lambda r: (-r["score"], r["pattern_id"]))
    return out
