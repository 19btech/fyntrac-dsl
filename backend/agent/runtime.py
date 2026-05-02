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

# ──────────────────────────────────────────────────────────────────────────
# Per-chat-session conversation memory.
# Without this, every agent run starts from a blank slate and re-discovers
# the workspace from scratch (re-listing events, re-reading rules, retrying
# duplicate creates). Keyed by the chat session_id supplied by the frontend.
# ──────────────────────────────────────────────────────────────────────────
_SESSION_HISTORY: dict[str, list[dict]] = {}
# Cap kept history per session. Oldest pairs are dropped when exceeded.
_SESSION_MAX_MESSAGES = 60


def reset_session_history(session_id: str) -> bool:
    """Drop persisted conversation history for the given chat session.
    Returns True if anything was cleared."""
    if not session_id:
        return False
    return _SESSION_HISTORY.pop(session_id, None) is not None


def _trim_history(msgs: list[dict]) -> list[dict]:
    """Cap stored history to keep token usage bounded. Drops oldest assistant/
    tool pairs but preserves any leading user message."""
    if len(msgs) <= _SESSION_MAX_MESSAGES:
        return msgs
    # Always keep the most recent _SESSION_MAX_MESSAGES messages.
    return msgs[-_SESSION_MAX_MESSAGES:]


async def _build_workspace_context(*, db, in_memory_data: dict | None) -> str:
    """Snapshot the workspace BEFORE the agent's first turn so it can plan
    against real state instead of re-discovering everything from scratch
    (and without making collisions with existing rules / events / txn types).

    Returns a markdown-ish string suitable for a system message. Always
    succeeds — falls back to "(unavailable)" lines on any error so a flaky
    Mongo never blocks a run.
    """
    from .tools import (
        tool_list_events,
        tool_list_saved_rules,
        tool_list_templates,
        tool_list_dsl_functions,
    )

    parts: list[str] = [
        "WORKSPACE SNAPSHOT (taken just before this turn). Use these names "
        "BEFORE creating new ones so you don't duplicate or collide. If "
        "what the user asked for already exists, prefer get_saved_rule + "
        "update_saved_rule over create_saved_rule.\n"
    ]

    # Events
    try:
        ev = await tool_list_events({})
        events = ev.get("events") or []
        if events:
            parts.append(f"EVENTS ({len(events)}):")
            for e in events[:50]:
                fields = e.get("fields") or []
                fnames = ", ".join(
                    (f.get("name") if isinstance(f, dict) else str(f))
                    for f in fields[:30]
                )
                more = "" if len(fields) <= 30 else f", …(+{len(fields)-30})"
                parts.append(
                    f"  • {e.get('event_name')} "
                    f"[{e.get('eventType')}/{e.get('eventTable')}]: "
                    f"{fnames}{more}"
                )
            if len(events) > 50:
                parts.append(f"  • …(+{len(events)-50} more events)")
        else:
            parts.append("EVENTS: (none defined yet)")
    except Exception:
        parts.append("EVENTS: (unavailable)")

    # Saved rules
    try:
        sr = await tool_list_saved_rules({})
        rules = sr.get("rules") or []
        if rules:
            parts.append(f"\nSAVED RULES ({len(rules)}):")
            for r in rules[:40]:
                parts.append(
                    f"  • {r.get('name')} (id={r.get('id')}, "
                    f"priority={r.get('priority')}, "
                    f"steps={r.get('step_count')})"
                )
            if len(rules) > 40:
                parts.append(f"  • …(+{len(rules)-40} more rules)")
        else:
            parts.append("\nSAVED RULES: (none)")
    except Exception:
        parts.append("\nSAVED RULES: (unavailable)")

    # Templates
    try:
        tpl = await tool_list_templates({})
        tpls = tpl.get("templates") or []
        if tpls:
            parts.append(f"\nTEMPLATES ({len(tpls)}):")
            for t in tpls[:25]:
                marker = " [deployed]" if t.get("deployed") else ""
                parts.append(f"  • {t.get('name')} (id={t.get('id')}){marker}")
            if len(tpls) > 25:
                parts.append(f"  • …(+{len(tpls)-25} more templates)")
        else:
            parts.append("\nTEMPLATES: (none)")
    except Exception:
        parts.append("\nTEMPLATES: (unavailable)")

    # Transaction types
    try:
        tx_types: list[str] = []
        if db is not None:
            cursor = db.transaction_definitions.find(
                {}, {"_id": 0, "transactiontype": 1}
            )
            async for d in cursor:
                if d.get("transactiontype"):
                    tx_types.append(d["transactiontype"])
        for d in (in_memory_data or {}).get("transaction_definitions", []) or []:
            if d.get("transactiontype") and d["transactiontype"] not in tx_types:
                tx_types.append(d["transactiontype"])
        if tx_types:
            parts.append(
                f"\nREGISTERED TRANSACTION TYPES ({len(tx_types)}): "
                + ", ".join(sorted(set(tx_types))[:80])
            )
        else:
            parts.append("\nREGISTERED TRANSACTION TYPES: (none)")
    except Exception:
        parts.append("\nREGISTERED TRANSACTION TYPES: (unavailable)")

    # DSL function index — names only, by category, to keep it cheap
    try:
        fns = await tool_list_dsl_functions({})
        flist = fns.get("functions") or []
        if flist:
            by_cat: dict[str, list[str]] = {}
            for f in flist:
                by_cat.setdefault(f.get("category") or "other", []).append(
                    f.get("name") or ""
                )
            parts.append(f"\nAVAILABLE DSL FUNCTIONS ({len(flist)}, by category):")
            for cat in sorted(by_cat):
                names = sorted(n for n in by_cat[cat] if n)
                parts.append(f"  • {cat}: {', '.join(names)}")
            parts.append(
                "  (call list_dsl_functions with category= or name= filters "
                "for full signatures + examples)"
            )
    except Exception:
        parts.append("\nAVAILABLE DSL FUNCTIONS: (unavailable — call list_dsl_functions)")

    parts.append(
        "\nUSE THIS CONTEXT to: (a) reuse existing event names/fields rather "
        "than recreating them, (b) avoid priority collisions with existing "
        "saved rules, (c) reuse already-registered transaction types when "
        "their names match, (d) skip redundant list_* calls — only refetch "
        "if you specifically modify one of these collections during this "
        "turn. If the user's request requires something NOT listed above, "
        "create it; if a sufficiently similar item exists, prefer to update "
        "or extend it."
    )
    return "\n".join(parts)


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
    (r"contextVars",
     "schedule_contextvars"),
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
    elif signature == "schedule_contextvars":
        base += (
            "\nNote: this error is about `scheduleConfig.contextVars`. "
            "`contextVars` MUST list ONLY names of variables defined by "
            "EARLIER calc/condition/iteration steps in the SAME rule. They "
            "are NOT for event fields. If your column formula references an "
            "event field, use the dotted form `EventName.field_name` IN THE "
            "FORMULA and DO NOT add `field_name` to contextVars. The simplest "
            "fix is to remove `contextVars` from the schedule step entirely "
            "(it is auto-derived from formulas) — only add a calc step BEFORE "
            "the schedule step if you need to reuse a computed value."
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
        "tool batch MUST include ALL of:\n"
        "  • `get_dsl_syntax_guide` — binding constraints + canonical patterns.\n"
        "  • `list_templates` — see what already exists; reuse > rebuild.\n"
        "  • `get_saved_rule` on the closest existing rule (by name match) — "
        "    copy its step shapes rather than authoring from scratch. If no "
        "    saved rule looks close, at minimum read the syntax guide's "
        "    CANONICAL PATTERNS section and pick A/B/C/D before writing.\n"
        "These are cheap, have no side effects, and prevent the dominant "
        "failure modes (multi-line iteration expressions, fictitious "
        "`all_instruments` variable, unsupported `arr[i]` indexing, "
        "fictitious `outputs.events.push`, picking the wrong pattern).\n\n"
        "ARCHITECTURE — STEPS → RULES → TEMPLATES:\n"
        "  • A STEP is one calculation, condition, iteration, OR schedule (atomic).\n"
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
        "primitives and avoid name/priority clashes. EXCEPTION: if a "
        "WORKSPACE SNAPSHOT system message was already injected at the start "
        "of this turn, you ALREADY HAVE the lists of events, saved rules, "
        "templates, transaction types, and DSL function names — DO NOT "
        "re-call those list_* tools just to re-read what's already in your "
        "context. Only refetch a list AFTER you've modified that collection "
        "during this turn.\n"
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
        "and the verified results.\n"
        " 10. TRANSACTIONS ARE THE OUTPUT. A rule with zero entries in "
        "`outputs.transactions[]` produces NOTHING and is never complete. "
        "Every accounting rule MUST end with at least one balanced "
        "debit/credit pair in `outputs.transactions[]`. Use "
        "`add_transaction_to_rule` (twice — one debit, one credit per "
        "economic event) AFTER your calc/schedule steps compute the amount. "
        "The Transactions panel reads ONLY from `outputs.transactions[]` — "
        "calc steps named 'transactions' / 'outputs_transactions' do "
        "nothing. The `finish` gate will reject your run if any rule you "
        "touched lacks balanced transactions.\n"
        " 11. EXPRESSIONS NEVER USE CURLY BRACES `{` `}`. The DSL has NO "
        "dict literals, NO set literals, NO f-strings. For multi-branch "
        "logic use stepType='condition'. For string concatenation use "
        "`concat(a, b, ...)`. Putting `{...}` in a formula causes a "
        "cryptic 'closing parenthesis }' does not match opening "
        "parenthesis (' error at code-gen time.\n"
        " 12. PARENTHESES MUST BALANCE in every formula. Count `(` and `)` "
        "before submitting. Unbalanced parens are the #1 source of the "
        "'Failed' badge users see in the Rule Builder.\n\n"
        "STEP DATA SHAPE (for create_saved_rule / add_step_to_rule):\n"
        "  • calc:        {name, stepType:'calc', source:'formula', formula:'multiply(a,b)'}\n"
        "                 source can also be 'value' (literal), 'event_field' (Evt.field), 'collect' (collect_by_instrument(Evt.field)).\n"
        "  • condition:   {name, stepType:'condition', conditions:[{condition:'gt(x,0)', thenFormula:'x'}], elseFormula:'0'}\n"
        "  • iteration:   {name, stepType:'iteration', iterations:[{type:'apply_each', sourceArray:'arr', expression:'multiply(each, 2)', resultVar:'doubled'}]}\n"
        "  • schedule:    {name, stepType:'schedule', scheduleConfig:{periodType:'date', frequency:'M',\n"
        "                  startDateSource:'field', startDateField:'EVT.postingdate',\n"
        "                  endDateSource:'formula', endDateFormula:'add_months(postingdate, 12)',\n"
        "                  columns:[{name:'depr', formula:'divide(cost, life_months)'}]},\n"
        "                  outputVars:[{name:'total_depr', type:'sum', column:'depr'}]}\n"
        "                 USE schedule FOR: depreciation / amortisation / amortization /\n"
        "                 accretion / runoff / payment plans / EIR / PIT-PD term-structure /\n"
        "                 any 'over the life of' calc that produces ONE row per period.\n"
        "                 Schedule columns CAN reference outer calc-step variables,\n"
        "                 EVENTNAME.field, prior columns in the same array, and built-ins\n"
        "                 (period_index, period_date, period_number, total_periods, lag,\n"
        "                 dcf, days_in_current_period, daily_basis). NEVER substitute a\n"
        "                 calc step or a standalone create_saved_schedule call for an\n"
        "                 inline schedule step inside a rule.\n\n"
        "════════════════════════════════════════════════════════════════════\n"
        "DSL CONSTRAINTS — BINDING. Violating any of these causes errors that\n"
        "look like 'unterminated string literal' or 'invalid syntax' but are\n"
        "actually structural. Read this list before authoring any expression.\n"
        "════════════════════════════════════════════════════════════════════\n"
        "  0. RULE EXECUTION MODEL — READ THIS FIRST. THIS IS THE #1 SOURCE\n"
        "     OF AGENT ERRORS:\n"
        "     The engine ALREADY iterates per-row internally. Every rule body\n"
        "     runs inside an implicit `for row in merged_event_data:` loop.\n"
        "     Each row represents ONE (instrumentid × postingdate) tuple,\n"
        "     with ALL referenced activity-event fields already JOINED onto\n"
        "     it (e.g. EOD_BALANCES_BEGINNINGBALANCE_UPB, REV_PRICE).\n"
        "       • Globals available on every step: postingdate, effectivedate,\n"
        "         instrumentid, subinstrumentid (lowercase, no prefix).\n"
        "       • Activity event fields → reference DIRECTLY as\n"
        "         EVENTNAME.fieldname (or EVENTNAME_fieldname). They are\n"
        "         already JOINED for the current instrument — do NOT use\n"
        "         lookup() to read another activity event's value.\n"
        "         WRONG:  lookup(LoanCreditRiskData.credit_impaired_flag, loan)\n"
        "         RIGHT:  LoanCreditRiskData.credit_impaired_flag\n"
        "       • Reference (small lookup) tables → collect_all('REF_field')\n"
        "         then lookup(arr, key) or element_at(arr, idx).\n"
        "       • Per-instrument time-series (multiple postingdates of the\n"
        "         same activity event) → collect_by_instrument('EVT_field').\n"
        "       • Indexed lookup inside an apply_each iteration uses\n"
        "         array_get(arr, index, default) where `index` is the\n"
        "         iteration index variable.\n"
        "       • Date-keyed lookup inside a schedule column formula uses\n"
        "         lookup(values_arr, keys_arr, target_key).\n"
        "       • Prior-period values inside schedule columns use\n"
        "         lag('column_name', n, default).\n"
        "       • Transactions emitted from `outputs.transactions[]` are\n"
        "         AUTOMATICALLY emitted ONCE PER ROW. You do NOT fan out\n"
        "         manually.\n"
        "     >>> THERE IS NO `all_instruments` VARIABLE. <<<\n"
        "     If you write iteration over `all_instruments`, STOP. Delete\n"
        "     that step. Replace with a `calc` step whose formula references\n"
        "     the merged event field directly. The engine will run that calc\n"
        "     once per instrument automatically.\n"
        "     Use `iteration` ONLY for operating on an array within a single\n"
        "     row (e.g. doubling each element of a collected time-series),\n"
        "     or when an array genuinely has multiple values per row.\n"
        "  0a. AUTHORING PATTERN — ALWAYS DO THIS FIRST:\n"
        "     Before writing any calculation, derivation, or schedule step,\n"
        "     enumerate EVERY field your model will need and create ONE named\n"
        "     calc step per field at the TOP of the rule. Then reference those\n"
        "     variable NAMES (not the raw EVENTNAME.field expression) in every\n"
        "     downstream step. This eliminates typo-driven undefined-variable\n"
        "     errors, makes debug_step actually useful, and matches how the\n"
        "     visual Rule Builder presents rules to users.\n"
        "       • Include the implicit globals too if the model uses them:\n"
        "             postingdate, effectivedate, instrumentid, subinstrumentid.\n"
        "         (Each becomes a calc step with source='value', value='postingdate'\n"
        "          — i.e. a one-liner that aliases the global.)\n"
        "       • SCALAR field on the current row (one value per\n"
        "         instrumentid×postingdate) → calc step:\n"
        "             {name:'cost', stepType:'calc', source:'event_field',\n"
        "              eventField:'AssetEvent.original_cost'}\n"
        "         OR equivalently source:'formula', formula:'AssetEvent.original_cost'.\n"
        "       • PER-INSTRUMENT TIME-SERIES (e.g. multiple sub-instruments\n"
        "         under one instrumentid, or multiple postingdates of the\n"
        "         same activity event for one instrument) → calc step:\n"
        "             {name:'subinstrument_ids', stepType:'calc', source:'collect',\n"
        "              collectType:'collect_by_instrument',\n"
        "              eventField:'AssetEvent.subinstrumentid'}\n"
        "       • REFERENCE / LOOKUP TABLE (small, instrument-independent) →\n"
        "             {name:'rate_table', stepType:'calc', source:'formula',\n"
        "              formula:\"collect_all('RATES.rate')\"}\n"
        "       • Heuristic for picking collect_all vs collect_by_instrument:\n"
        "         • If the field varies per instrument (e.g. subinstrumentid,\n"
        "           historical balances for THIS instrument) → collect_by_instrument.\n"
        "         • If the field is a global lookup table shared across all\n"
        "           instruments (e.g. PD curves, FX rates, region codes) →\n"
        "           collect_all.\n"
        "       • Naming convention: snake_case, descriptive, matches the\n"
        "         business term (e.g. `original_cost`, `revaluation_date`,\n"
        "         `reducing_balance_rate`, `subinstrument_ids`). Do NOT prefix\n"
        "         with the event name — the calc step IS the alias.\n"
        "       • After this 'inputs block' of calc steps, every subsequent\n"
        "         calc/condition/iteration/schedule step MUST reference these\n"
        "         variable names. Schedule columns reference them as bare\n"
        "         identifiers; the validator will auto-derive contextVars\n"
        "         from them — you do NOT need to fill in contextVars yourself.\n"
        "     Why this matters: weak models that try to inline EVENTNAME.field\n"
        "     references everywhere routinely produce undefined-variable loops\n"
        "     because they then ALSO list bare field names in scheduleConfig.\n"
        "     contextVars. Following this pattern makes that mistake impossible.\n"
        "  0b. CLOSE EVERY RULE WITH TRANSACTIONS — NON-NEGOTIABLE.\n"
        "     A rule is INCOMPLETE until `outputs.transactions[]` contains\n"
        "     at least one balanced debit/credit pair. The Transactions panel\n"
        "     in the UI reads ONLY from this array. The `finish` tool will\n"
        "     refuse to accept your run otherwise.\n"
        "       • After your calc/schedule steps compute the amounts, call\n"
        "         `add_transaction_to_rule` ONCE PER SIDE per economic event:\n"
        "             { type:'DepreciationExpense', amount:'depreciation_charge', side:'debit' }\n"
        "             { type:'AccumulatedDepreciation', amount:'depreciation_charge', side:'credit' }\n"
        "       • `amount` MUST be the NAME of a calc step (or a schedule\n"
        "         outputVar). It cannot be an inline expression and cannot\n"
        "         reference a step that doesn't exist.\n"
        "       • Register every transaction type via `add_transaction_types`\n"
        "         BEFORE referencing it.\n"
        "       • For schedule-driven amounts, expose the period total via\n"
        "         scheduleConfig.outputVars (type='sum' or 'last') and use\n"
        "         that outputVar's name as `amount`.\n"
        "       • Multi-leg accounting events (e.g. revaluation surplus to\n"
        "         OCI + accumulated dep reset + period depreciation) need\n"
        "         MULTIPLE pairs — one debit/credit per leg.\n"
        "       • Once you call `finish`, the runtime auto-injects every\n"
        "         rule_id you've touched and re-runs the transaction /\n"
        "         schedule / static-validation gates against ALL of them.\n"
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
        " 10. THE OUTPUT OF A RULE *IS* ITS TRANSACTIONS. A rule with zero\n"
        "     entries in `outputs.transactions[]` produces NO OUTPUT and is\n"
        "     never complete. Emit transactions by calling\n"
        "     `add_transaction_to_rule` (twice — one debit, one credit pair),\n"
        "     OR by passing `outputs.transactions=[...]` to create_saved_rule /\n"
        "     update_saved_rule. NEVER create a calc step named\n"
        "     `outputs_transactions`, `transactions`, `output`, or similar —\n"
        "     such steps do nothing; only `outputs.transactions[]` drives the\n"
        "     Transactions panel and the actual transaction emission. The\n"
        "     `_validate_step_shape` validator hard-rejects those step names.\n"
        " 11. Register every transaction type via `add_transaction_types` BEFORE\n"
        "     any rule emits it.\n"
        " 12. TRANSACTIONS — STRICT: NEVER write `createTransaction(...)` inside\n"
        "     a calc step's formula OR inside an iteration step's expression.\n"
        "     The Rule Builder UI shows transactions in a dedicated\n"
        "     'Transactions' panel that ONLY reads from the rule's\n"
        "     `outputs.transactions[]` array. A formula or iteration body like\n"
        "        formula: 'createTransaction(postingdate, effectivedate, \"X\", amt)'\n"
        "     is REJECTED by validation. Instead, compute the amount in a calc\n"
        "     step (e.g. `amount = multiply(...)`) and put the transaction in\n"
        "     `outputs.transactions[]` referencing that variable by name:\n"
        "        outputs: { transactions: [\n"
        "            {type:'ECLAllowance', amount:'amount', side:'credit'},\n"
        "            {type:'ECLExpense',   amount:'amount', side:'debit'} ] }\n"
        "     The engine emits these transactions ONCE PER ROW automatically;\n"
        "     you do NOT need an iteration step to fan them out per instrument.\n"
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
        " 16. PATTERN SELECTION — before authoring a non-trivial template,\n"
        "     call `list_templates` and `get_saved_rule` on the closest match.\n"
        "     Most accounting models fall into ONE of four canonical patterns\n"
        "     (see `get_dsl_syntax_guide` → CANONICAL PATTERNS):\n"
        "       A. Schedule + extract row for postingdate (amortisation,\n"
        "          interest accrual, fee amortisation, lease, IFRS9 stage).\n"
        "       B. Collect + apply_each + aggregate (revenue recognition,\n"
        "          weighted-average pricing).\n"
        "       C. Replay + lag schedule + delta (SBO replay, period-over-\n"
        "          period adjustments).\n"
        "       D. Scalar finance (NPV, IRR, single-row valuation).\n"
        "     Pick the pattern FIRST, then fill in the fields. Do not invent\n"
        "     a 5th pattern.\n"
        " 17. NO 'WOULD YOU LIKE ME TO…' ENDINGS. When you detect a problem\n"
        "     in your own draft (e.g. dry_run shows 0 transactions, sample\n"
        "     data has wrong IDs, debug_step returns null), DO NOT call\n"
        "     `finish` with a message asking the user whether to fix it. FIX\n"
        "     IT FIRST. Only call `finish` when:\n"
        "       (a) verify_rule_complete returned overall_ready=true, AND\n"
        "       (b) dry_run_template returned balanced debits=credits with\n"
        "           no sanity_warnings, AND\n"
        "       (c) you have nothing more to investigate.\n"
        "     If you genuinely need user input (e.g. an ambiguous business\n"
        "     rule), state the SPECIFIC choice you need them to make in one\n"
        "     sentence — never end with 'would you like me to'.\n"
        " 18. EDIT IN PLACE — NEVER DUPLICATE A RULE. If a rule named X\n"
        "     already exists and the user wants to change it, you MUST:\n"
        "       (a) call `update_step` / `add_step_to_rule` / `delete_step` /\n"
        "           `update_saved_rule` to fix it in place, OR\n"
        "       (b) call `delete_saved_rule` first (with user approval) and\n"
        "           then create the replacement under the SAME name X.\n"
        "     NEVER append `_v2`, `_final`, `_fixed`, `_auto`, `_new` or\n"
        "     similar suffixes — that just clutters the workspace and means\n"
        "     the broken original still exists. The `create_saved_rule` tool\n"
        "     will reject suffixed near-duplicates of an existing rule.\n"
        "     Same applies to schedules and templates: edit existing first;\n"
        "     only create new when the use case is genuinely different.\n"
        " 19. NEVER STOP ON VALIDATION FAILURE. When ANY tool returns an\n"
        "     `errors` array, an `ok: false` flag, or a ToolError mentioning\n"
        "     `undefined`, `not defined`, `unbalanced`, `missing`, or\n"
        "     `failed`, you are NOT done. Your next action MUST be a fix:\n"
        "       • undefined variable → `add_step_to_rule` to define it\n"
        "         BEFORE the step that references it, OR `update_step` to\n"
        "         change the reference to an existing variable.\n"
        "       • unbalanced transactions → call `add_transaction_to_rule`\n"
        "         for the missing side.\n"
        "       • amount_step not in rule → `add_step_to_rule` to compute it,\n"
        "         OR change the transaction's `amount` to a real step name.\n"
        "       • undefined function → call `list_dsl_functions` to find\n"
        "         the correct name, then `update_step` to fix the formula.\n"
        "       • dry_run returns `next_action: ZERO_TRANSACTIONS_BUT_DECLARED`\n"
        "         → the rule's logic is correct but its inputs evaluate to 0.\n"
        "         You MUST act on this: call `debug_step` on the amount's\n"
        "         calc step to see why it is zero, then either fix the\n"
        "         formula OR call `generate_sample_event_data` again with\n"
        "         `field_hints` that force the upstream fields to values\n"
        "         that produce non-zero results, then re-run dry_run. Do\n"
        "         NOT finish, do NOT ask the user, do NOT say 'no transactions\n"
        "         because sample data is zero' as if it were an answer.\n"
        "     You may NOT call `finish` while ANY rule's\n"
        "     `verify_rule_complete` returns `overall_ready: false`. If you\n"
        "     have tried 3 distinct fixes and the same error class persists,\n"
        "     call `get_dsl_syntax_guide` and re-read the relevant section\n"
        "     before the 4th attempt.\n"
        " 20. ONE RULE OR MANY? Default to ONE rule per accounting event\n"
        "     (e.g. one ECL rule, one revenue-recognition rule). Split into\n"
        "     multiple rules ONLY when:\n"
        "       (a) the rules emit different debit/credit transaction pairs\n"
        "           that should be auditable independently, OR\n"
        "       (b) different rules need different priorities (run order)\n"
        "           because one consumes another's transactions, OR\n"
        "       (c) different rules attach to different event types.\n"
        "     Two calc steps inside the same rule are almost always better\n"
        "     than two single-step rules.\n"
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
    max_steps: int = 80,
    auto_approve_destructive: bool = False,
    approval_timeout: float = 600.0,
    session_id: str | None = None,
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
    # Track every rule the agent has touched (created/updated/added steps to)
    # during this run so `finish` can gate on ALL of them, not just one the
    # agent happens to pass an id for. Maps rule_id -> last-known name.
    touched_rules: dict[str, str] = {}

    yield {
        "type": "run_started", "ts": _now_iso(), "run_id": run_id,
        "task": task, "model": model, "max_steps": max_steps,
        "session_id": session_id,
    }

    # Load prior conversation history for this chat session (if any). This
    # lets follow-up turns reference earlier discoveries (events created,
    # rules saved, transaction types registered) instead of restarting from
    # scratch every time.
    prior_history: list[dict] = []
    if session_id:
        prior_history = list(_SESSION_HISTORY.get(session_id) or [])

    # Preflight: snapshot the workspace on the FIRST turn of a session so the
    # model plans against real state and doesn't waste steps re-listing
    # events / rules / functions / transaction types. On subsequent turns
    # the history already contains those discoveries — don't re-inject.
    preflight_msgs: list[dict] = []
    if not prior_history:
        try:
            ctx = await _build_workspace_context(
                db=db, in_memory_data=in_memory_data
            )
            preflight_msgs.append({"role": "system", "content": ctx})
            yield {"type": "warning", "ts": _now_iso(),
                    "message": "Loaded workspace context (events, rules, "
                               "templates, transaction types, DSL functions)."}
        except Exception as exc:
            logger.warning("Workspace preflight failed: %s", exc)

    messages: list[dict] = (
        [{"role": "system", "content": _system_prompt()}]
        + preflight_msgs
        + prior_history
        + [{"role": "user", "content": task.strip()}]
    )
    # Index where this turn's NEW messages begin — used at the end to
    # persist only the delta (not the whole transcript).
    new_msg_start_idx = len(messages) - 1

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
            # Loop-nudge MUST be appended only AFTER every tool_call_id in this
            # assistant message has its `tool` response, otherwise OpenAI's
            # invariant ("an assistant message with tool_calls must be followed
            # by tool messages responding to each tool_call_id") is violated
            # and the next request 400s. Defer until after the for-loop.
            pending_nudge: tuple[str, str, str] | None = None
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

                # Forward the original user prompt to `tool_finish` so it can
                # gate on user-asked-for-X invariants (e.g. "make sure you
                # create a schedule for depreciation"). Always overwrite —
                # the agent must not be able to spoof this field.
                if name == "finish" and isinstance(args, dict):
                    args["user_request"] = task
                    # Inject every rule_id we've seen the agent create or
                    # mutate this turn so finish can gate them ALL — not
                    # just one the agent remembers to pass. Without this,
                    # weak models call finish with no rule_id and the
                    # transactions/schedule gates are silently skipped.
                    if touched_rules and not args.get("rule_ids"):
                        args["rule_ids"] = list(touched_rules.keys())

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
                    # Track rule-touching tools so the finish gate sees them.
                    _RULE_TOUCHING_TOOLS = {
                        "create_saved_rule", "update_saved_rule",
                        "add_step_to_rule", "update_step", "delete_step",
                        "add_transaction_to_rule", "update_transaction",
                        "delete_transaction_from_rule",
                    }
                    if name in _RULE_TOUCHING_TOOLS and isinstance(result, dict):
                        rid = (result.get("rule_id")
                               or (result.get("rule") or {}).get("id")
                               or (isinstance(args, dict) and args.get("rule_id"))
                               or "")
                        rname = ((result.get("rule") or {}).get("name")
                                 or result.get("name")
                                 or (isinstance(args, dict) and args.get("name"))
                                 or "")
                        if rid:
                            touched_rules[str(rid)] = str(rname or touched_rules.get(str(rid), ""))
                    if name == "delete_saved_rule" and isinstance(args, dict):
                        rid = str(args.get("rule_id") or "")
                        touched_rules.pop(rid, None)
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
                    # Keep only the most recent 12 entries so we can detect
                    # protracted loops (some weak models retry 5+ times).
                    recent_errors = recent_errors[-12:]
                    same = [e for e in recent_errors if e == (name, sig)]
                    same_count = len(same)
                    # First nudge after 2 repeats; re-fire once at 4 repeats
                    # with stronger framing so a model that ignored the first
                    # nudge gets a second chance to course-correct. After 6+
                    # repeats, hard-abort the run — no point burning steps.
                    if same_count >= 2 and (name, sig) not in nudge_already_sent_for:
                        nudge_already_sent_for.add((name, sig))
                        pending_nudge = (name, sig, _build_loop_nudge(name, sig))
                    elif same_count == 4:
                        pending_nudge = (
                            name, sig,
                            _build_loop_nudge(name, sig)
                            + "\n\nFINAL WARNING: this is the 4th identical "
                            "failure. If your next attempt produces the same "
                            "error category again the run will be aborted. "
                            "Do something materially different — read the "
                            "syntax guide, inspect a working rule, or call "
                            "`finish` and ask the user for help.",
                        )
                    elif same_count >= 6:
                        final_status = "halted"
                        final_summary = (
                            f"Aborted after {same_count} consecutive "
                            f"`{name}` failures with the same error category "
                            f"(`{sig}`). The model is stuck in a loop and "
                            f"could not self-correct. Last error: {err[:400]}"
                        )
                        yield {"type": "warning", "ts": _now_iso(),
                                "message": final_summary}
                        should_finish = True
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

            # All tool_call_ids in this assistant message now have their
            # `tool` responses. Safe to inject the deferred loop-nudge as a
            # follow-up user message without breaking OpenAI's invariant.
            if pending_nudge is not None:
                _ln_name, _ln_sig, _ln_text = pending_nudge
                messages.append({"role": "user", "content": _ln_text})
                yield {"type": "warning", "ts": _now_iso(),
                        "message": f"Loop detected on {_ln_name} ({_ln_sig}); "
                                   f"steering agent toward syntax guide / "
                                   f"existing rule lookup."}

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

    # Persist this turn's messages back into the per-session history so the
    # next user turn in the same chat sees them. We persist regardless of
    # final_status — even a partial/failed turn left useful tool observations
    # the next turn should not have to redo (e.g. event listings).
    if session_id:
        try:
            new_msgs = messages[new_msg_start_idx:]
            updated = list(_SESSION_HISTORY.get(session_id) or []) + new_msgs
            _SESSION_HISTORY[session_id] = _trim_history(updated)
        except Exception as exc:
            logger.warning("Could not persist session history: %s", exc)

    # Persist run record (best-effort)
    run_doc = {
        "run_id": run_id, "task": task, "model": model,
        "started_at": started_at, "finished_at": _now_iso(),
        "status": final_status, "summary": final_summary,
        "steps": steps_used, "history": history,
    }
    await _save_run(db, in_memory_data, run_doc)
