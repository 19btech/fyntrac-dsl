"""Agent runtime: plan → act → observe loop with auto-debug, approval gates,
SSE event streaming, and persisted run records.

The runtime is provider-agnostic: it talks to any AIProvider that implements
`chat_with_tools(messages, tools, model, temperature)`.

Public surface:
    run_agent(task, *, db, in_memory_data, provider, model, ...)
        Async generator yielding event dicts (str-serialisable JSON).

    submit_approval(run_id, call_id, decision)
        Resolve a pending destructive-tool approval.

Events emitted (each has at minimum {type, ts}):
    {"type":"run_started", "run_id":..., "task":..., "model":..., "max_steps":...}
    {"type":"thinking", "step":N}
    {"type":"assistant_message", "step":N, "content":"..."}
    {"type":"tool_pending", "step":N, "call_id":..., "name":..., "args":{...}}
    {"type":"tool_start",   "step":N, "call_id":..., "name":..., "args":{...}}
    {"type":"tool_done",    "step":N, "call_id":..., "name":..., "result":{...}}
    {"type":"tool_error",   "step":N, "call_id":..., "name":..., "error":"..."}
    {"type":"warning",      "message":"..."}
    {"type":"final",        "status":"completed"|"failed"|"cancelled"|"halted",
                            "summary":"...", "steps":N}
    {"type":"error",        "message":"..."}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from .tools import (
    DESTRUCTIVE_TOOLS,
    TOOL_SCHEMAS,
    ToolError,
    dispatch_tool,
)

logger = logging.getLogger(__name__)


class AgentRunError(Exception):
    """Fatal runtime error that aborts a run."""


# ──────────────────────────────────────────────────────────────────────────
# Per-run approval registry
# ──────────────────────────────────────────────────────────────────────────

class _PendingApproval:
    __slots__ = ("event", "decision")

    def __init__(self) -> None:
        self.event = asyncio.Event()
        self.decision: str | None = None  # "approve" or "deny"


_PENDING: dict[str, dict[str, _PendingApproval]] = {}
_RUN_STATUS: dict[str, str] = {}      # run_id -> "running" | "cancelled" | ...
_RUN_LOCK = asyncio.Lock()


async def _register_pending(run_id: str, call_id: str) -> _PendingApproval:
    async with _RUN_LOCK:
        _PENDING.setdefault(run_id, {})[call_id] = _PendingApproval()
        return _PENDING[run_id][call_id]


async def _wait_for_approval(run_id: str, call_id: str, timeout: float = 600.0) -> str:
    pa = _PENDING.get(run_id, {}).get(call_id)
    if pa is None:
        raise AgentRunError("Internal: no pending approval registered")
    try:
        await asyncio.wait_for(pa.event.wait(), timeout=timeout)
    except asyncio.TimeoutError:
        return "deny"
    finally:
        _PENDING.get(run_id, {}).pop(call_id, None)
    return pa.decision or "deny"


def submit_approval(run_id: str, call_id: str, decision: str) -> bool:
    """Resolve a pending approval. Returns True if accepted."""
    pa = _PENDING.get(run_id, {}).get(call_id)
    if pa is None:
        return False
    pa.decision = "approve" if str(decision).lower() == "approve" else "deny"
    pa.event.set()
    return True


def cancel_run(run_id: str) -> bool:
    if run_id in _RUN_STATUS:
        _RUN_STATUS[run_id] = "cancelled"
        # Resolve any pending approvals as deny so the runtime can unblock.
        for call_id, pa in list(_PENDING.get(run_id, {}).items()):
            pa.decision = "deny"
            pa.event.set()
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate_for_observation(value: Any, max_chars: int = 6000) -> str:
    """Serialise a tool result and truncate so the context window can't blow up."""
    try:
        serialised = json.dumps(value, default=str, ensure_ascii=False)
    except Exception:
        serialised = str(value)
    if len(serialised) > max_chars:
        return serialised[:max_chars] + f"... [truncated {len(serialised) - max_chars} chars]"
    return serialised


# Coarse buckets for recognising "the same kind of error twice in a row".
# Keep this list short — broader buckets produce stronger nudges.
_ERROR_SIGNATURE_PATTERNS: list[tuple[str, str]] = [
    (r"unterminated string literal|EOL while scanning|EOF while parsing",
     "unterminated_string_literal"),
    (r"invalid syntax|unexpected EOF|unexpected token",
     "invalid_syntax"),
    (r"single-line|SINGLE-LINE|iteration expression must",
     "iteration_multiline"),
    (r"bracket indexing|`arr\[i\]`|element_at",
     "bracket_indexing"),
    (r"outputs\.events\.push|createEventRow",
     "synthetic_event_push"),
    (r"is not a known DSL function|Did you mean:",
     "unknown_function"),
    (r"is not defined|NameError|not found",
     "undefined_name"),
    (r"DSL translation failed",
     "translation_failed"),
    (r"Generated python has syntax error",
     "generated_python_syntax"),
]


def _error_signature(err: str) -> str:
    """Map an error string to a coarse category label so we can detect when
    the agent is looping on the same class of mistake."""
    if not isinstance(err, str):
        return "unknown"
    import re as _re
    for pat, label in _ERROR_SIGNATURE_PATTERNS:
        if _re.search(pat, err, _re.IGNORECASE):
            return label
    return "other"


def _build_loop_nudge(tool_name: str, signature: str) -> str:
    """Construct a forceful steering message when the agent loops on the same
    error category. Tells the agent exactly what to do next."""
    base = (
        f"⚠️ LOOP DETECTED: tool `{tool_name}` has failed multiple times in a "
        f"row with the same error category (`{signature}`). STOP retrying the "
        f"same approach. Your next action MUST be one of:\n"
        f"  1. Call `get_dsl_syntax_guide` to read the binding DSL constraints "
        f"and see worked examples of each step type.\n"
        f"  2. Call `get_saved_rule` on an existing rule that uses the same "
        f"step type and copy its expression shape exactly.\n"
        f"  3. Call `finish` with a question for the user explaining what you "
        f"are stuck on.\n"
        f"Do NOT make a third variation of the failing call before doing one "
        f"of those three things."
    )
    if signature in ("unterminated_string_literal", "invalid_syntax", "iteration_multiline"):
        base += (
            "\nNote: errors in this category are almost always caused by "
            "putting MULTIPLE LINES or a `let` binding inside a single "
            "iteration expression. Iteration expressions are SINGLE-LINE, "
            "SINGLE-EXPRESSION. Split into multiple iterations or steps."
        )
    elif signature == "synthetic_event_push":
        base += (
            "\nNote: this DSL has NO `outputs.events.push(...)` and NO "
            "`createEventRow(...)`. Synthetic events must be pre-loaded via "
            "create_event_definitions + generate_sample_event_data BEFORE the "
            "rule runs. There is no in-rule event creation."
        )
    elif signature == "bracket_indexing":
        base += (
            "\nNote: `arr[i]` bracket indexing is not supported. Use "
            "lookup(arr, idx) or element_at(arr, idx)."
        )
    return base


def _system_prompt() -> str:
    return (
        "You are Fyntrac DSL Studio's autonomous accounting agent — a chartered "
        "accountant and financial-modelling expert. You author IFRS- and US-GAAP-"
        "compliant accounting models and answer questions about the standards.\n\n"
        "ACCOUNTING DOMAIN KNOWLEDGE — apply these standards by default:\n"
        "  • IFRS 9 (Financial Instruments): three-stage Expected Credit Loss "
        "(ECL) model — Stage 1 (12-month ECL, performing), Stage 2 (lifetime ECL, "
        "significant increase in credit risk / SICR), Stage 3 (lifetime ECL, "
        "credit-impaired); SPPI test for classification (Amortised Cost, FVOCI, "
        "FVTPL); EIR (effective interest rate) for amortised cost interest income; "
        "POCI (purchased or originated credit-impaired) assets.\n"
        "  • IFRS 15 / ASC 606 (Revenue): five-step model — identify contract, "
        "identify performance obligations, determine transaction price, allocate "
        "price, recognise revenue when (or as) each PO is satisfied. Use "
        "point-in-time vs over-time recognition.\n"
        "  • IFRS 16 / ASC 842 (Leases): Right-of-Use (ROU) asset and lease "
        "liability at PV of payments using IBR or implicit rate; subsequent "
        "amortisation of ROU and unwinding of liability with interest.\n"
        "  • IFRS 17 (Insurance Contracts): General Measurement Model (BBA), "
        "Premium Allocation Approach (PAA), Variable Fee Approach (VFA); "
        "Contractual Service Margin (CSM); fulfilment cashflows.\n"
        "  • US GAAP CECL (ASC 326): lifetime expected credit loss for "
        "financial assets at amortised cost; pool-based or individual estimation.\n"
        "  • Hedging (IFRS 9 / ASC 815): fair-value, cashflow, net-investment "
        "hedges; effectiveness testing.\n"
        "  • Always use double-entry: every economic event produces matched debit "
        "and credit transactions of equal magnitude. When the user asks for an "
        "accounting model, emit transaction TYPES that name both sides (e.g. "
        "InterestIncomeAccrual, InterestReceivable, ECLAllowance, ECLExpense, "
        "StageTransition, RevenueRecognised, ContractAssetIncrease).\n"
        "  • If the user references a specific standard or jurisdiction (e.g. "
        "\"IFRS 9 stage 1\", \"ASC 842 ROU asset\", \"CECL pool\"), follow that "
        "standard's recognition and measurement rules. State your assumptions "
        "explicitly in the rule's commentText so the user can audit them.\n"
        "  • If the user asks a knowledge question (no build/edit), answer "
        "directly without calling tools beyond the optional `list_dsl_functions` "
        "lookup, then `finish` with the explanation.\n\n"
        "GOAL: Build event definitions, generate sample data, and author DSL "
        "templates that produce the user's desired transactions.\n\n"
        "FIRST-RESPONSE PROTOCOL: For ANY rule-authoring task, your FIRST "
        "tool batch should include `get_dsl_syntax_guide` (or you must have "
        "called it earlier in this run). It is cheap, has no side effects, "
        "and gives you the binding step-shape examples that prevent the "
        "single most common failure mode (multi-line iteration expressions, "
        "unsupported `arr[i]` indexing, fictitious `outputs.events.push`).\n\n"
        "ARCHITECTURE — STEPS → RULES → TEMPLATES:\n"
        "  • A STEP is one calculation, condition, or iteration (atomic).\n"
        "  • A RULE is an ordered list of steps with optional output transactions, "
        "stored in `saved_rules` and editable in the Rule Builder UI.\n"
        "  • A TEMPLATE/MODEL is a set of rules (and schedules) combined in "
        "priority order, stored in `user_templates`.\n"
        "  • A SCHEDULE is a tabular projection (amortisation, ECL forecast, "
        "revenue recognition timeline) saved to `saved_schedules` and editable "
        "in the Schedule Builder UI via `create_saved_schedule`.\n"
        "PREFERRED WORKFLOW for 'build me a model' requests:\n"
        "  1. `list_events`, `list_dsl_functions`, `list_saved_rules`, `list_templates` (discover).\n"
        "  2. `create_event_definitions` for any missing event(s) — both activity "
        "events (e.g. LoanOrigination) and reference tables (e.g. PDCurve, "
        "LGDCurve) as needed.\n"
        "  3. `add_transaction_types` for every transaction the model will emit "
        "(both sides of the double-entry).\n"
        "  4. `generate_sample_event_data` for every activity event so the model "
        "has data to dry-run against.\n"
        "  5. Build small `create_saved_rule` rules — one logical concern per rule "
        "(stage assignment, ECL computation, allowance booking, etc.). Each rule "
        "has ordered steps; transactions go in the rule's `outputs.transactions[]` "
        "array (NOT inside calc-step formulas as createTransaction calls — that "
        "is rejected by validation and hides the txn from the UI's Transactions "
        "panel).\n"
        "  6. Use `create_saved_schedule` for any cashflow/amortisation/ECL-"
        "projection table. ALWAYS prefer a schedule step over hand-rolling an "
        "iteration when the user asks for amortisation, depreciation, lease "
        "liability run-off, ECL projection, payment schedules, or any tabular "
        "time-series calculation.\n"
        "  7. **TEST EVERY STEP**: call `debug_step` on EACH step of every rule "
        "you create. This is the equivalent of clicking the play button on the "
        "step card in the UI. Do NOT proceed to template assembly until each "
        "step prints a sane value.\n"
        "  8. **TEST EVERY SCHEDULE**: call `debug_schedule` on EACH schedule. "
        "This is the play button on the schedule card. Inspect the materialised "
        "rows in the response.\n"
        "  9. `create_or_replace_template` to create the template shell (with a "
        "placeholder DSL like `noop = 0`), then `attach_rules_to_template` to "
        "populate it with rule_ids and schedule_ids in priority order.\n"
        " 10. `dry_run_template` to verify end-to-end: check transaction counts, "
        "totals by type, that debits equal credits.\n"
        " 11. **READINESS GATE**: for every rule you authored, call "
        "`verify_rule_complete`. It returns a checklist confirming all steps "
        "debug-run cleanly, outputs.transactions has both debit and credit "
        "sides, transaction types are registered, and event data is loaded. "
        "Do NOT call `finish` unless every rule's `overall_ready` is true.\n"
        " 12. `finish` with a summary that lists each rule + transactions emitted "
        "+ confirmation that all steps and schedules were tested.\n\n"
        "PREFERRED WORKFLOW for 'debug my template/rule/step' requests:\n"
        "  • `list_saved_rules` (or `list_templates`) → `get_saved_rule` to fetch "
        "the structure → `debug_step` to inspect intermediate values → "
        "`update_step` / `update_saved_rule` / `add_step_to_rule` / `delete_step` "
        "to fix → `dry_run_template` to confirm.\n\n"
        "PREFERRED WORKFLOW for 'how do I…' / advisory questions:\n"
        "  • `list_dsl_functions` (and any narrowly scoped category filter) to "
        "ground your suggestion in real function signatures, then `finish` with "
        "a concise answer plus a worked DSL snippet. Only modify state if the "
        "user explicitly asks.\n\n"
        "REAL-TIME UI: every successful tool call refreshes the Templates, Rules, "
        "Schedules, Events, Transactions and Combined-Code panels automatically. "
        "The user sees changes appear live, so prefer many small, observable "
        "steps over one giant change.\n\n"
        "STRICT RULES:\n"
        "1. NEVER write Python or use the `customCode:` block. Compose all "
        "logic with built-in DSL functions only.\n"
        "2. Discover before you act: call `list_events`, `list_dsl_functions`, "
        "`list_templates`, `list_saved_rules` early so you reuse existing "
        "primitives and avoid name/priority clashes.\n"
        "3. Always call `validate_dsl` before `create_or_replace_template` "
        "(rules built via `create_saved_rule` are validated automatically).\n"
        "4. After assembling a template call `dry_run_template` and inspect "
        "the result. If counts/totals look wrong, use `debug_step` to inspect "
        "individual variables, then `update_step` / `update_saved_rule` to fix.\n"
        "5. Generate sample data BEFORE dry-running any template that depends "
        "on it.\n"
        "6. When the user asks to add/edit/remove/debug a step or rule, use "
        "the targeted tools (`add_step_to_rule`, `update_step`, `delete_step`, "
        "`debug_step`, `update_saved_rule`, `delete_saved_rule`) rather than "
        "rewriting the whole template.\n"
        "7. When a tool returns an error, read the message, fix the problem, "
        "and try a different approach. Do NOT repeat the same failing call.\n"
        "8. Destructive tools (`delete_template`, `delete_saved_rule`, "
        "`delete_saved_schedule`, `clear_all_data`) require user approval — "
        "only call them when the user explicitly asks.\n"
        "9. End the run with `finish(summary=...)` describing what you built "
        "and the verified results.\n\n"
        "STEP DATA SHAPE (for create_saved_rule / add_step_to_rule):\n"
        "  • calc:        {name, stepType:'calc', source:'formula', formula:'multiply(a,b)'}\n"
        "                 source can also be 'value' (literal), 'event_field' (Evt.field), 'collect' (collect_by_instrument(Evt.field)).\n"
        "  • condition:   {name, stepType:'condition', conditions:[{condition:'gt(x,0)', thenFormula:'x'}], elseFormula:'0'}\n"
        "  • iteration:   {name, stepType:'iteration', iterations:[{type:'apply_each', sourceArray:'arr', expression:'multiply(each, 2)', resultVar:'doubled'}]}\n\n"
        "════════════════════════════════════════════════════════════════════\n"
        "DSL CONSTRAINTS — BINDING. Violating any of these causes errors that\n"
        "look like 'unterminated string literal' or 'invalid syntax' but are\n"
        "actually structural. Read this list before authoring any expression.\n"
        "════════════════════════════════════════════════════════════════════\n"
        "  1. EVERY expression in a step is SINGLE-LINE, SINGLE-EXPRESSION.\n"
        "     - No `let` bindings. No `;` separators. No newlines.\n"
        "     - Do NOT write multi-statement expressions in iteration.expression,\n"
        "       calc.formula, condition.condition, condition.thenFormula, or\n"
        "       schedule column formulas.\n"
        "     - To do multiple things, use multiple steps or multiple iterations.\n"
        "  2. There is NO Python `for` loop and NO `while` loop. Iteration is\n"
        "     ONLY done via stepType='iteration' with a sourceArray.\n"
        "  3. There is NO `outputs.events.push(...)`, NO `createEventRow(...)`,\n"
        "     NO `arr[i]` bracket indexing in expressions. Instead:\n"
        "       • Array element access: lookup(arr, idx) or element_at(arr, idx)\n"
        "       • Synthetic events cannot be emitted from expressions. Either\n"
        "         pre-load them via create_event_definitions + generate_sample_event_data,\n"
        "         OR compute the values inline and emit transactions directly.\n"
        "  4. Conditionals INSIDE expressions: if(cond, then_value, else_value).\n"
        "     Do NOT use Python ternary `a if c else b` and do NOT use `if:/else:`\n"
        "     blocks inside an expression. For multi-branch logic, use a\n"
        "     stepType='condition' step.\n"
        "  5. String literals: use double quotes. Do NOT embed unescaped quotes,\n"
        "     newlines, or curly braces inside a string literal.\n"
        "  6. `iteration.sourceArray` is a VARIABLE NAME (a string referring to a\n"
        "     previously-defined collection, e.g. an event field collected with\n"
        "     collect_by_instrument). It is NOT a literal `[...]` array.\n"
        "  7. Reference event fields with EVENTNAME.fieldname (case-insensitive),\n"
        "     e.g. `principal = LoanEvent.principal`. The event must exist and\n"
        "     the field must be declared on it (verify with `list_events`).\n"
        "  8. Math operators: ALWAYS use the DSL functions multiply(a,b),\n"
        "     divide(a,b), add(a,b), subtract(a,b), modulo(a,b), power(a,b).\n"
        "     `a * b`, `a / b` etc. are accepted in some contexts but the\n"
        "     function form is always safe — use it.\n"
        "  9. Use the global `postingdate` and `effectivedate` (lowercase, no\n"
        "     event prefix). They are injected automatically.\n"
        " 10. Emit transactions by putting them in the rule's `outputs.transactions[]`\n"
        "     array (preferred). Manual `createTransaction(...)` calls work too\n"
        "     but the structured form is what shows up in the Transactions panel.\n"
        " 11. Register every transaction type via `add_transaction_types` BEFORE\n"
        "     any rule emits it.\n"
        " 12. TRANSACTIONS — STRICT: NEVER write `createTransaction(...)` inside\n"
        "     a calc step's formula. The Rule Builder UI shows transactions in a\n"
        "     dedicated 'Transactions' panel that ONLY reads from the rule's\n"
        "     `outputs.transactions[]` array. A formula like\n"
        "        formula: 'createTransaction(postingdate, effectivedate, \"X\", amt)'\n"
        "     is REJECTED by validation. Instead, compute the amount in a calc\n"
        "     step (e.g. `amount = multiply(...)`) and put the transaction in\n"
        "     `outputs.transactions[]` referencing that variable by name:\n"
        "        outputs: { transactions: [\n"
        "            {type:'ECLAllowance', amount:'amount', side:'credit'},\n"
        "            {type:'ECLExpense',   amount:'amount', side:'debit'} ] }\n"
        " 13. SCHEDULES: when the user asks for amortisation, depreciation, ECL\n"
        "     projection, payment runoff, or ANY tabular time-series, use a\n"
        "     `schedule` step or `create_saved_schedule`. Do NOT hand-roll an\n"
        "     iteration that re-implements a schedule.\n"
        " 14. DEFINITION OF DONE: a rule is NOT complete until\n"
        "       (a) every step's `debug_step` returns a sane value,\n"
        "       (b) every schedule's `debug_schedule` returns rows,\n"
        "       (c) `verify_rule_complete` returns `overall_ready: true`,\n"
        "       (d) `dry_run_template` shows balanced debits = credits.\n"
        "     Do NOT call `finish` until all four are confirmed in this run.\n"
        " 15. SAMPLE DATA QUALITY — make generated test data ACCOUNTING-SENSIBLE:\n"
        "     • Rates, PD, LGD, LTV, CCF must be DECIMALS in [0,1] (5% = 0.05).\n"
        "       NEVER pass `field_hints={\"interest_rate\":{\"range\":(1,15)}}`.\n"
        "       Correct: `{\"range\":(0.01,0.15)}`. The generator will reject\n"
        "       impossible ranges with a sanity-bound error.\n"
        "     • Money fields (principal, balance, EAD, exposure) should stay\n"
        "       under $10M per row unless the user specifies otherwise.\n"
        "     • For amortisation/ECL projection give 12+ monthly posting_dates\n"
        "       (e.g. ['2026-01-31','2026-02-28',…]) — a single date cannot\n"
        "       prove a schedule works.\n"
        "     • `generate_sample_event_data` returns `data_quality_warnings`.\n"
        "       If non-empty, FIX the field_hints and regenerate before testing.\n"
        "     • `dry_run_template` returns `sanity_warnings`. If a transaction\n"
        "       amount > $1B appears, STOP and inspect — it's almost always a\n"
        "       unit error (rate as integer) or an unbounded multiplication.\n"
        "     • Register transaction types in DEBIT/CREDIT pairs upfront\n"
        "       (e.g. ECLAllowance + ECLExpense, InterestReceivable +\n"
        "       InterestIncome). `add_transaction_types` will suggest the\n"
        "       missing partner if you forget.\n"
        "════════════════════════════════════════════════════════════════════\n"
        "FAILURE LOOP PROTOCOL — IMPORTANT:\n"
        "  • If the SAME tool fails TWICE in a row with what looks like a syntax\n"
        "    error, STOP guessing. Your next action MUST be one of:\n"
        "      (a) Call `get_dsl_syntax_guide` to read the constraints + examples.\n"
        "      (b) Call `get_saved_rule` on a working rule that uses the same\n"
        "          step type and copy its expression shape.\n"
        "      (c) Ask the user for clarification (call `finish` with a question).\n"
        "    Do NOT try a third variation of the same broken expression.\n"
        "  • Errors like 'unterminated string literal', 'invalid syntax', and\n"
        "    'unexpected EOF' are almost ALWAYS caused by violating constraint\n"
        "    1 (multi-line) or constraint 3 (using unsupported syntax like\n"
        "    bracket indexing or .push). Re-read those rules before retrying.\n"
        "════════════════════════════════════════════════════════════════════\n"
    )


# ──────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────

async def _save_run(db, in_memory_data, run_doc: dict) -> None:
    try:
        if db is not None:
            await db.agent_runs.update_one(
                {"run_id": run_doc["run_id"]},
                {"$set": run_doc},
                upsert=True,
            )
            return
    except Exception as exc:
        logger.warning("Persist agent_run failed: %s", exc)
    if in_memory_data is not None:
        runs = in_memory_data.setdefault("agent_runs", [])
        for i, r in enumerate(runs):
            if r.get("run_id") == run_doc["run_id"]:
                runs[i] = run_doc
                return
        runs.append(run_doc)


# ──────────────────────────────────────────────────────────────────────────
# Main runtime
# ──────────────────────────────────────────────────────────────────────────

async def run_agent(
    *,
    task: str,
    provider,                         # AIProvider instance
    api_key: str,
    model: str,
    db=None,
    in_memory_data: dict | None = None,
    max_steps: int = 50,
    auto_approve_destructive: bool = False,
    approval_timeout: float = 600.0,
) -> AsyncGenerator[dict, None]:
    """Execute the agent loop and stream events.

    Yields dict events that the SSE endpoint can serialise as `data: {...}\\n\\n`.
    """
    if not task or not task.strip():
        yield {"type": "error", "message": "Empty task"}
        return
    if provider is None:
        yield {"type": "error", "message": "No AI provider configured"}
        return

    run_id = uuid.uuid4().hex
    _RUN_STATUS[run_id] = "running"
    started_at = _now_iso()
    history: list[dict] = []
    steps_used = 0
    final_status = "halted"
    final_summary = ""
    # Loop detector: track recent tool errors so we can break repetition.
    # Each entry: (tool_name, error_signature). When the same signature
    # appears N+ times we inject a nudge into the conversation.
    recent_errors: list[tuple[str, str]] = []
    nudge_already_sent_for: set[tuple[str, str]] = set()

    yield {
        "type": "run_started", "ts": _now_iso(), "run_id": run_id,
        "task": task, "model": model, "max_steps": max_steps,
    }

    messages: list[dict] = [
        {"role": "system", "content": _system_prompt()},
        {"role": "user", "content": task.strip()},
    ]

    try:
        for step in range(1, max_steps + 1):
            steps_used = step
            if _RUN_STATUS.get(run_id) == "cancelled":
                final_status = "cancelled"
                final_summary = "Run cancelled by user."
                break

            yield {"type": "thinking", "ts": _now_iso(), "step": step}

            # Emit a "calling_model" event with elapsed-time heartbeats so the
            # UI shows progress even while the (blocking) provider call runs.
            call_started = time.time()
            yield {"type": "calling_model", "ts": _now_iso(), "step": step,
                    "model": model, "message": f"Calling {model}…"}

            provider_task = asyncio.create_task(
                provider.chat_with_tools(
                    messages=messages,
                    tools=TOOL_SCHEMAS,
                    model=model,
                    api_key=api_key,
                    temperature=0.1,
                )
            )
            try:
                while not provider_task.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(provider_task), timeout=4.0)
                    except asyncio.TimeoutError:
                        if _RUN_STATUS.get(run_id) == "cancelled":
                            provider_task.cancel()
                            break
                        elapsed = int(time.time() - call_started)
                        yield {"type": "heartbeat", "ts": _now_iso(),
                                "step": step, "elapsed_s": elapsed,
                                "message": f"Waiting for {model}… {elapsed}s"}
                if _RUN_STATUS.get(run_id) == "cancelled":
                    final_status = "cancelled"
                    final_summary = "Run cancelled by user."
                    break
                resp = provider_task.result()
            except NotImplementedError:
                yield {"type": "error",
                        "message": f"Provider does not support tool calling. Use OpenAI, DeepSeek, or Anthropic."}
                final_status = "failed"
                break
            except asyncio.CancelledError:
                final_status = "cancelled"
                final_summary = "Run cancelled by user."
                break
            except Exception as exc:
                logger.exception("Provider call failed")
                yield {"type": "error", "message": f"Provider error: {exc}"}
                final_status = "failed"
                break

            assistant_msg = resp.get("message") or {}
            assistant_text = (assistant_msg.get("content") or "").strip()
            tool_calls = resp.get("tool_calls") or []

            # Persist assistant message in our local conversation history
            messages.append({
                "role": "assistant",
                "content": assistant_text or None,
                "tool_calls": tool_calls,
            })

            if assistant_text:
                yield {"type": "assistant_message", "ts": _now_iso(),
                        "step": step, "content": assistant_text}
                history.append({"step": step, "type": "assistant_message",
                                 "content": assistant_text})

            if not tool_calls:
                # The model produced only text — treat as final answer.
                final_status = "completed" if assistant_text else "halted"
                final_summary = assistant_text or "(no summary)"
                break

            # Process every tool call the model emitted this step.
            should_finish = False
            for call in tool_calls:
                call_id = call.get("id") or uuid.uuid4().hex
                name = call.get("name") or ""
                args = call.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}

                history.append({"step": step, "type": "tool_call",
                                 "call_id": call_id, "name": name, "args": args})

                # Approval gate for destructive tools
                if name in DESTRUCTIVE_TOOLS and not auto_approve_destructive:
                    await _register_pending(run_id, call_id)
                    yield {"type": "tool_pending", "ts": _now_iso(), "step": step,
                            "call_id": call_id, "name": name, "args": args,
                            "message": "User approval required"}
                    decision = await _wait_for_approval(run_id, call_id,
                                                          timeout=approval_timeout)
                    if decision != "approve":
                        err = f"User denied execution of '{name}'"
                        yield {"type": "tool_error", "ts": _now_iso(), "step": step,
                                "call_id": call_id, "name": name, "error": err}
                        messages.append({"role": "tool", "tool_call_id": call_id,
                                          "name": name,
                                          "content": json.dumps({"error": err})})
                        history.append({"step": step, "type": "tool_error",
                                         "call_id": call_id, "name": name, "error": err})
                        continue
                    # Force confirm=true so the underlying tool accepts it
                    if isinstance(args, dict):
                        args["confirm"] = True

                yield {"type": "tool_start", "ts": _now_iso(), "step": step,
                        "call_id": call_id, "name": name, "args": args}

                t0 = time.time()
                try:
                    result = await dispatch_tool(name, args)
                    duration_ms = int((time.time() - t0) * 1000)
                    obs = _truncate_for_observation(result)
                    yield {"type": "tool_done", "ts": _now_iso(), "step": step,
                            "call_id": call_id, "name": name,
                            "duration_ms": duration_ms, "result": result}
                    messages.append({"role": "tool", "tool_call_id": call_id,
                                      "name": name, "content": obs})
                    history.append({"step": step, "type": "tool_done",
                                     "call_id": call_id, "name": name,
                                     "duration_ms": duration_ms,
                                     "result_preview": obs[:1000]})
                    # Success — reset loop tracking for this tool
                    recent_errors = [e for e in recent_errors if e[0] != name]
                    if name == "finish":
                        final_status = "completed"
                        final_summary = (result or {}).get("summary") or ""
                        should_finish = True
                except ToolError as te:
                    duration_ms = int((time.time() - t0) * 1000)
                    err = str(te)
                    yield {"type": "tool_error", "ts": _now_iso(), "step": step,
                            "call_id": call_id, "name": name,
                            "duration_ms": duration_ms, "error": err}
                    messages.append({"role": "tool", "tool_call_id": call_id,
                                      "name": name,
                                      "content": json.dumps({"error": err})})
                    history.append({"step": step, "type": "tool_error",
                                     "call_id": call_id, "name": name, "error": err})
                    # Loop-detector: track this error and nudge if needed
                    sig = _error_signature(err)
                    recent_errors.append((name, sig))
                    # Keep only the most recent 6 entries
                    recent_errors = recent_errors[-6:]
                    same = [e for e in recent_errors if e == (name, sig)]
                    if len(same) >= 2 and (name, sig) not in nudge_already_sent_for:
                        nudge_already_sent_for.add((name, sig))
                        nudge_text = _build_loop_nudge(name, sig)
                        messages.append({"role": "user", "content": nudge_text})
                        yield {"type": "warning", "ts": _now_iso(),
                                "message": f"Loop detected on {name} ({sig}); "
                                           f"steering agent toward syntax guide / "
                                           f"existing rule lookup."}
                except Exception as exc:
                    logger.exception("Tool '%s' raised", name)
                    err = f"Internal tool error: {exc}"
                    yield {"type": "tool_error", "ts": _now_iso(), "step": step,
                            "call_id": call_id, "name": name, "error": err}
                    messages.append({"role": "tool", "tool_call_id": call_id,
                                      "name": name,
                                      "content": json.dumps({"error": err})})
                    history.append({"step": step, "type": "tool_error",
                                     "call_id": call_id, "name": name, "error": err})

            if should_finish:
                break
        else:
            # Loop exited without break => max steps reached
            final_status = "halted"
            final_summary = f"Max steps ({max_steps}) reached without finish()."
            yield {"type": "warning", "ts": _now_iso(), "message": final_summary}
    finally:
        _PENDING.pop(run_id, None)
        _RUN_STATUS.pop(run_id, None)

    final_event = {
        "type": "final", "ts": _now_iso(), "run_id": run_id,
        "status": final_status, "summary": final_summary, "steps": steps_used,
    }
    yield final_event

    # Persist run record (best-effort)
    run_doc = {
        "run_id": run_id, "task": task, "model": model,
        "started_at": started_at, "finished_at": _now_iso(),
        "status": final_status, "summary": final_summary,
        "steps": steps_used, "history": history,
    }
    await _save_run(db, in_memory_data, run_doc)
