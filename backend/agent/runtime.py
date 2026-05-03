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
    tool pairs but preserves any leading user message.

    Also sanitizes the retained slice so it never starts with an orphaned
    `tool` message (which OpenAI rejects with 'messages with role tool must
    be a response to a preceding message with tool_calls'). After trimming
    we advance past any leading tool/assistant-tool-response messages until
    the first `user` or a clean `assistant` without tool-call residue.
    """
    if len(msgs) <= _SESSION_MAX_MESSAGES:
        trimmed = msgs
    else:
        # Always keep the most recent _SESSION_MAX_MESSAGES messages.
        trimmed = msgs[-_SESSION_MAX_MESSAGES:]

    # Sanitize: drop any leading messages that would violate OpenAI's
    # role-ordering invariant (tool must follow assistant-with-tool_calls).
    # Walk forward until we find a safe starting point.
    start = 0
    while start < len(trimmed):
        role = trimmed[start].get("role", "")
        if role == "tool":
            # Orphaned tool message — skip it.
            start += 1
            continue
        if role == "assistant":
            # Only safe to start on an assistant message if it has no
            # tool_calls (otherwise the matching tool responses are gone).
            if trimmed[start].get("tool_calls"):
                # Skip this assistant + all its tool responses.
                start += 1
                while start < len(trimmed) and trimmed[start].get("role") == "tool":
                    start += 1
                continue
        break  # user message or clean assistant — safe to start here

    return trimmed[start:]


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

    # Event data (loaded row counts per event)
    try:
        event_data_counts: dict[str, int] = {}
        if db is not None:
            async for d in db.event_data.aggregate([
                {"$project": {
                    "event_name": 1,
                    "row_count": {"$size": {"$ifNull": ["$data_rows", []]}}
                }}
            ]):
                if d.get("event_name"):
                    event_data_counts[d["event_name"]] = d.get("row_count", 0)
        for d in (in_memory_data or {}).get("event_data", []) or []:
            name = d.get("event_name")
            if name and name not in event_data_counts:
                event_data_counts[name] = len(d.get("data_rows") or [])
        if event_data_counts:
            loaded = {k: v for k, v in event_data_counts.items() if v > 0}
            empty = sorted(k for k, v in event_data_counts.items() if v == 0)
            parts.append("\nEVENT DATA (rows loaded per event):")
            for evt, cnt in sorted(loaded.items()):
                parts.append(f"  • {evt}: {cnt} rows")
            if empty:
                parts.append(f"  • No data loaded for: {chr(44).join(empty)}")
        else:
            parts.append("\nEVENT DATA: (none loaded)")
    except Exception:
        parts.append("\nEVENT DATA: (unavailable)")

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


def _build_loop_nudge(tool_name: str, signature: str, err_text: str = "") -> str:
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
            "the schedule step if you need to reuse a computed value.\n"
            "COMMON MISTAKE: using `iif(cond, a, b)` (Excel syntax) inside a "
            "column formula. The DSL uses `if(cond, a, b)`. Replace every "
            "`iif(` with `if(` in your column formulas."
        )
    elif signature == "undefined_name" and err_text:
        import re as _re
        m = _re.search(r"\b(true|false|null|nil|undefined)\b", err_text)
        if m:
            base += (
                f"\nNote: the offending name `{m.group(1)}` is a JS-style "
                f"boolean/null literal. The DSL accepts ONLY Python-style "
                f"`True` / `False` / `None`. Replace `true`→`True`, "
                f"`false`→`False`, `null`→`None`. Better still, test a "
                f"boolean field DIRECTLY in a condition without `eq(...)` — "
                f"e.g. condition: \"EVT.is_impaired\" instead of "
                f"`eq(EVT.is_impaired, True)`."
            )
        # Check for _EVENT_FIELD pattern (EVENT_fieldname) — this means the
        # event row doesn't have that field in sample data. Fix = regenerate sample.
        elif tool_name in {"debug_step", "dry_run_rule", "dry_run_template"}:
            evt_field = _re.search(r"name '([A-Za-z_]\w+)_(\w+)' is not defined", err_text)
            if evt_field:
                evt_name = evt_field.group(1)
                field_name = evt_field.group(2)
                base += (
                    f"\nThis error means the event '{evt_name}' does NOT have a "
                    f"field named '{field_name}' in its sample data. "
                    f"The DSL engine translates `{evt_name}.{field_name}` in your "
                    f"formula into `{evt_name}_{field_name}` in the executed code; "
                    f"if that field is absent from the event rows, execution fails. "
                    f"\nFIX (two steps):\n"
                    f"  1. Call `get_event_data(event_name='{evt_name}')` to see "
                    f"     what fields currently exist.\n"
                    f"  2. Call `generate_sample_event_data(event_name='{evt_name}', "
                    f"     field_hints={{'{field_name}': {{'type': 'date', 'range': "
                    f"['2022-01-01','2024-01-01']}}}})` (adjust type to 'number' if "
                    f"it is a numeric field) to add the missing field to sample rows.\n"
                    f"  3. THEN re-run debug_step. Do NOT patch the step formula to "
                    f"     remove the reference — the field belongs in the data."
                )
    # schedule_sum / schedule_last / etc. called with wrong first arg type:
    # the model passed a regular scalar variable (a list object) instead of
    # the schedule step output variable. Catches the runtime error form
    # "'list' object has no attribute 'Schedule'" or similar.
    if err_text and "'list' object has no attribute" in err_text:
        import re as _re
        attr_m = _re.search(r"'list' object has no attribute '([^']+)'", err_text)
        bad_attr = attr_m.group(1) if attr_m else None
        base += (
            "\n\nSCHEDULE ACCESS ERROR: A schedule output is a list of dicts — "
            "it does NOT support dot-attribute access. "
            + (f"You wrote something like `<var>.{bad_attr}` — " if bad_attr else "")
            + "this fails at runtime.\n"
            "CORRECT PATTERN — use schedule accessor functions:\n"
            "  schedule_sum(ScheduleStepName, 'column_name')   → scalar (total)\n"
            "  schedule_last(ScheduleStepName, 'column_name')  → scalar (final period)\n"
            "  schedule_first(ScheduleStepName, 'column_name') → scalar (first period)\n"
            "  schedule_column(ScheduleStepName, 'column_name')→ list of scalars\n"
            "The FIRST argument MUST be the NAME of a schedule step (its `name` field),\n"
            "NOT a regular calc step variable. E.g.:\n"
            "  schedule_sum('DepreciationSchedule', 'depreciation_charge')   ← step name as string\n"
            "  schedule_sum(DepreciationSchedule, 'depreciation_charge')     ← step name as ident\n"
            "NEVER: schedule_sum(opening_nbv, 'depreciation_charge')  ← `opening_nbv` is a scalar calc step"
        )
    # Schedule forward-reference loop: model keeps reordering columns instead
    # of using lag(). Inject a copy-pasteable canonical reducing-balance
    # depreciation pattern.
    if (tool_name in {"create_saved_rule", "add_step_to_rule", "update_step",
                      "patch_step", "replace_schedule_column"}
            and err_text and "defined LATER in the schedule" in err_text):
        import re as _re
        fwd = _re.search(r"references column\(s\) \[([^\]]+)\]", err_text)
        fwd_name = (fwd.group(1).strip().strip("'\"") if fwd else "closing_nbv")
        base += (
            f"\n\nSCHEDULE RECURSION FIX — STOP REORDERING COLUMNS. The "
            f"forward reference cannot be resolved by reordering because "
            f"`opening_<X>` and `closing_<X>` are mutually recursive across "
            f"periods. Use `lag()` instead. Canonical reducing-balance "
            f"depreciation schedule (paste this shape directly into your "
            f"scheduleConfig.columns, in this exact order):\n"
            f"  columns:\n"
            f"  - {{name: 'opening_nbv', formula: \"lag('closing_nbv', 1, "
            f"opening_net_carrying_amount)\"}}\n"
            f"  - {{name: 'depreciation_charge', formula: "
            f"\"opening_nbv * reducing_balance_rate\"}}\n"
            f"  - {{name: 'closing_nbv', formula: "
            f"\"opening_nbv - depreciation_charge\"}}\n"
            f"Where `opening_net_carrying_amount` and `reducing_balance_rate` "
            f"are calc-step variables defined BEFORE the schedule step. The "
            f"`lag('closing_nbv', 1, <seed>)` call returns the prior period's "
            f"closing NBV, or the seed value on period 0. This is the ONLY "
            f"correct pattern for any rolling-balance schedule (depreciation, "
            f"amortisation, accretion, runoff). Do NOT try to reorder columns "
            f"or split into multiple schedule steps."
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
        "  • IAS 16 (Property, Plant & Equipment): two measurement models — "
        "Cost Model (carry at cost less accumulated depreciation less impairment) "
        "and Revaluation Model (carry at revalued amount = fair value at date of "
        "revaluation less subsequent accumulated depreciation and impairment). "
        "DEFAULT ACCOUNTING ENTRIES — apply these WITHOUT asking the user:\n"
        "    DEPRECIATION (any method):  Dr DepreciationExpense  /  Cr AccumulatedDepreciation\n"
        "    UPWARD REVALUATION: Dr AssetCarryingAmountAdjustment  /  Cr RevaluationSurplusOCI\n"
        "      (increase goes to OCI / revaluation surplus, NOT P&L)\n"
        "    DOWNWARD REVALUATION — reverses prior surplus first:\n"
        "      Within existing surplus: Dr RevaluationSurplusOCI  /  Cr AssetCarryingAmountAdjustment\n"
        "      Excess beyond surplus:   Dr RevaluationDecreasePL  /  Cr AssetCarryingAmountAdjustment\n"
        "    DISPOSAL: Dr AccumulatedDepreciation + Dr<if surplus> RevaluationSurplusOCI\n"
        "              Cr AssetCostAccount; gain/loss to P&L.\n"
        "  Depreciation methods available in DSL: straight-line (cost-residual)/useful_life_months\n"
        "    reducing-balance (opening_nbv * rate) — model as inline schedule step using\n"
        "    lag('closing_nbv', 1, opening_net_carrying_amount). NEVER ask the user\n"
        "    which transaction types to use for IAS 16 — apply the above defaults.\n"
        "  Sample data for IAS 16 MUST include these fields (add via field_hints if needed):\n"
        "    acquisition_date (date), original_cost (number), useful_life_months (integer),\n"
        "    reducing_balance_rate (decimal e.g. 0.25), residual_value (number),\n"
        "    revaluation_date (date), revalued_amount (number).\n"
        "  • IFRS 17 (Insurance Contracts): General Measurement Model (BBA), "
        "Premium Allocation Approach (PAA), Variable Fee Approach (VFA); "
        "Contractual Service Margin (CSM); fulfilment cashflows.\n"
        "  • US GAAP CECL (ASC 326): lifetime expected credit loss for "
        "financial assets at amortised cost; pool-based or individual estimation.\n"
        "  • Hedging (IFRS 9 / ASC 815): fair-value, cashflow, net-investment "
        "hedges; effectiveness testing.\n"
        "  • This app emits TRANSACTIONS — not journal entries. Transactions are "
        "consumed by a downstream accounting system which posts them as journals. "
        "NEVER describe rule outputs as 'journal entries'. "
        "Always define matched debit and credit transaction types (e.g. "
        "InterestIncomeAccrual / InterestReceivable, ECLAllowance / ECLExpense, "
        "StageTransition, RevenueRecognised / ContractAssetIncrease). "
        "The downstream system decides how each transaction maps to a GL posting.\n"
        "  • If the user references a specific standard or jurisdiction (e.g. "
        "\"IFRS 9 stage 1\", \"ASC 842 ROU asset\", \"CECL pool\"), follow that "
        "standard's recognition and measurement rules. State your assumptions "
        "explicitly in the rule's commentText so the user can audit them.\n"
        "  • If the user asks a knowledge question (no build/edit), answer "
        "directly without calling tools beyond the optional `list_dsl_functions` "
        "lookup, then `finish` with the explanation.\n\n"
        "GOAL: Build event definitions, generate sample data, and author DSL "
        "templates that produce the user's desired transactions.\n\n"
        "PROACTIVE CONTEXT CHECK — READ THE SNAPSHOT BEFORE EVERY BUILD:\n"
        "The WORKSPACE SNAPSHOT injected at the start of this turn shows you\n"
        "exactly what events, transaction types, and data are already loaded.\n"
        "Read it first and branch as follows — do NOT assume a blank slate.\n\n"
        "USE CASE 1 — BLANK SLATE (snapshot: no events, no data loaded):\n"
        "  Proceed directly with the full build workflow below.\n\n"
        "USE CASE 2 — EVENTS + TRANSACTION TYPES LOADED, NO DATA YET\n"
        "  (snapshot has events listed + registered transaction types, but\n"
        "  EVENT DATA shows 'none loaded'):\n"
        "  Do NOT recreate existing events or transaction types.\n"
        "  Call `finish` and ask the user:\n"
        "    'I can see [N] event definitions ([names]) and [M] transaction\n"
        "     types are already configured. Should I:\n"
        "     (a) Use these and generate sample data, then build the template?\n"
        "     (b) Start completely from scratch?'\n"
        "  If (a): skip workflow steps 2-3, go straight to step 4.\n"
        "  If (b): proceed with full workflow.\n\n"
        "USE CASE 3 — EVENTS + DATA ROWS ALREADY LOADED\n"
        "  (snapshot shows N rows already loaded per event):\n"
        "  Do NOT overwrite the loaded data without asking.\n"
        "  Call `finish` and ask the user:\n"
        "    'I can see [event]: [N rows] already loaded. Should I:\n"
        "     (a) Use this data and go straight to building the template?\n"
        "     (b) Regenerate sample data (overwrites your loaded data)?\n"
        "     (c) Start from scratch?'\n"
        "  If (a): skip steps 2-4, start from step 5.\n"
        "  If (b): run generate_sample_event_data then continue at step 5.\n\n"
        "USE CASE 4 — EDITING STEPS OR RULES (user asks to add / update /\n"
        "  delete a specific step, condition, schedule, iteration, or rule):\n"
        "  Proceed directly with the edit — see dedicated editing workflows\n"
        "  below. No confirmation gate needed for explicitly requested edits.\n\n"
        "FIRST-RESPONSE PROTOCOL: For ANY rule-authoring task, your FIRST "
        "tool batch MUST include ALL of:\n"
        "  • `find_similar_template` — pass the user intent + keywords to "
        "    discover the right canonical pattern AND any saved rules to "
        "    reuse. ALWAYS call this BEFORE picking a pattern.\n"
        "  • `get_canonical_pattern` — fetch the FULL step scaffold of the "
        "    pattern A/B/C/D the matcher recommended. Copy its `steps[]` "
        "    array verbatim into create_saved_rule, substituting only the "
        "    `parameters` it lists.\n"
        "  • `get_event_data` — call this for EVERY event the rule will use.\n"
        "    Inspect the rows to: (a) list every field needed, (b) determine\n"
        "    scalar vs non-scalar (check subinstrumentid counts per instrumentid),\n"
        "    (c) confirm the loaded data is suitable. This call is MANDATORY\n"
        "    before create_saved_rule. Do not guess field names or data shapes.\n"
        "  • `submit_plan` — record your chosen pattern, the rules you'll "
        "    create, the FULL list of input-block field steps planned (name,\n"
        "    source type, collect type if non-scalar), and the transaction types\n"
        "    each rule will emit (with confirmation whether each type already\n"
        "    exists in the snapshot or is new). This is mandatory: skipping it\n"
        "    is the dominant cause of trial-and-error loops.\n"
        "  • `get_dsl_syntax_guide` — binding constraints + canonical patterns.\n"
        "  • `list_templates` — see what already exists; reuse > rebuild.\n"
        "  • `get_saved_rule` on the closest existing rule (returned by "
        "    find_similar_template) — copy its step shapes rather than "
        "    authoring from scratch.\n"
        "These are cheap, have no side effects, and prevent the dominant "
        "failure modes (multi-line iteration expressions, fictitious "
        "`all_instruments` variable, unsupported `arr[i]` indexing, "
        "fictitious `outputs.events.push`, picking the wrong pattern, "
        "schedule columns referencing nonexistent variables).\n\n"
        "STEP EDITING PROTOCOL — silent updates are now impossible:\n"
        "  • Every step has an immutable `step_id` (UUID) returned by "
        "    get_saved_rule. ALWAYS pass `step_id` to update_step / "
        "    delete_step / patch_step / debug_step / test_schedule_step. "
        "    `step_name` still works but breaks across renames.\n"
        "  • `update_step` now performs a DEEP MERGE of `patch` into the "
        "    existing step doc — patching `scheduleConfig.frequency` no "
        "    longer wipes the columns. To swap a whole list (e.g. all "
        "    columns), pass the full new list. To surgically edit ONE leaf "
        "    (e.g. one column's formula), prefer `patch_step` or the "
        "    `replace_schedule_column` convenience tool.\n"
        "  • `patch_step(rule_id, step_id, ops=[{op,path,value}])` uses "
        "    JSON-Pointer (RFC 6902) — paths look like "
        "    `/scheduleConfig/columns/2/formula`. Returns "
        "    `persisted.ok=false` with `mismatches[]` if the requested "
        "    paths did NOT land in the saved doc. ALWAYS check this field.\n"
        "  • Every write tool now re-fetches the rule and verifies the "
        "    requested values persisted. If `persisted.ok` is false, the "
        "    update DID NOT take effect — read the mismatches[], fix the "
        "    field name / path, and retry.\n\n"
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
        "  2. `create_event_definitions` — SKIP if the needed events already appear\n"
        "in the snapshot. Only create events that are genuinely absent. Include both\n"
        "activity events (e.g. LoanOrigination) and reference tables (e.g. PDCurve)\n"
        "as needed.\n"
        "  3. `add_transaction_types` — ALWAYS check the snapshot first.\n"
        "  *** TRANSACTION TYPE REUSE RULE (NON-NEGOTIABLE) ***\n"
        "  The WORKSPACE SNAPSHOT lists all REGISTERED TRANSACTION TYPES.\n"
        "  If the user's request can be served by existing types, USE THEM.\n"
        "  NEVER invent new transaction type names when matching ones exist.\n"
        "  Decision tree:\n"
        "    a) Do existing types match the required debit/credit pair?\n"
        "       → Use them AS-IS. Do NOT call add_transaction_types.\n"
        "    b) Only SOME types exist (e.g. debit registered, credit missing)?\n"
        "       → Call add_transaction_types for ONLY the missing side.\n"
        "    c) No matching types exist at all?\n"
        "       → Call add_transaction_types for both sides.\n"
        "    d) User did NOT explicitly ask to change transaction types?\n"
        "       → NEVER replace or rename existing types. Ask the user\n"
        "          if you think a rename would be beneficial.\n"
        "  Register both debit-side and credit-side type names.\n"
        "  4. `generate_sample_event_data` — MANDATORY ORDERING: call it for "
        "REFERENCE events (eventType='reference') FIRST, then for activity events. "
        "The generator cross-seeds activity-event fields from reference-event data: "
        "if PRODUCT_CATALOG has product_type=['SaaS','Service'], any activity event "
        "with a field also named product_type will automatically receive only 'SaaS' "
        "or 'Service' — never a made-up value. The response includes "
        "'reference_seeded_fields' confirming which fields were constrained. "
        "Generating activity events before reference events loses this guarantee.\n"
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
        "  9. `create_or_replace_template` to create the template shell (pass "
        "event_name only — do NOT write inline dsl_code by hand, that almost "
        "always produces syntax errors). Then immediately call "
        "`attach_rules_to_template` (or `assemble_template_from_rules`) "
        "passing rule_ids=[<rule_id>] to populate it from the saved rule's "
        "generated code.\n"
        " 10. `dry_run_template` to verify end-to-end: check transaction counts, "
        "totals by type, that debit-side totals equal credit-side totals.\n"
        " 11. **READINESS GATE**: for every rule you authored, call "
        "`verify_rule_complete`. It returns a checklist confirming all steps "
        "debug-run cleanly, outputs.transactions has both debit and credit "
        "transaction types registered, and event data is loaded. "
        "Do NOT call `finish` unless every rule's `overall_ready` is true.\n"
        " 12. `finish` with a summary that lists each rule + transactions emitted "
        "+ confirmation that all steps and schedules were tested.\n\n"
        "PREFERRED WORKFLOW for 'debug my template/rule/step' requests:\n"
        "  • `list_saved_rules` (or `list_templates`) → `get_saved_rule` to fetch "
        "the structure → `debug_step` to inspect intermediate values → "
        "`update_step` / `update_saved_rule` / `add_step_to_rule` / `delete_step` "
        "to fix → `dry_run_template` to confirm.\n\n"
        "PREFERRED WORKFLOW for 'diagnose an error in user-authored steps':\n"
        "(Use this when the USER built a step/rule/schedule and hit an error)\n"
        "  1. `get_saved_rule(rule_id)` to fetch the rule structure and all step_ids.\n"
        "  2. `debug_step(rule_id, step_id)` on the failing step to capture the\n"
        "     exact error text and intermediate values.\n"
        "  3. DIAGNOSE: identify the root cause. Map it to one of these categories:\n"
        "       SYNTAX ERROR  — invalid expression, mismatched parens, multi-line\n"
        "         expression in a single-line context, unsupported operator\n"
        "       UNDEFINED NAME — field not in sample data, typo in step name,\n"
        "         EVENTNAME.field pattern used where a plain variable was expected\n"
        "       WRONG SOURCE TYPE — scalar event_field used on a non-scalar event\n"
        "         (multiple subinstrumentids), should be collect_by_instrument\n"
        "       WRONG FUNCTION — DSL function name misspelled or does not exist\n"
        "       LOGIC ERROR — formula runs but produces wrong/zero/null result\n"
        "       SCHEDULE ERROR — forward reference, missing lag(), wrong column order\n"
        "  4. EXPLAIN the problem clearly to the user in plain language:\n"
        "       • What the error means (no jargon unless necessary)\n"
        "       • Which part of their step/formula caused it\n"
        "       • What the correct approach is\n"
        "  5. PROPOSE the fix — show the corrected step shape or formula.\n"
        "  6. ASK: 'Would you like me to apply this fix?' and WAIT for the\n"
        "     user's confirmation before making any changes.\n"
        "  7. If user confirms: apply the fix via `update_step` or `patch_step`,\n"
        "     then `debug_step` again to confirm it resolves the error.\n"
        "  IMPORTANT: Do NOT silently fix user-authored steps. Explain first,\n"
        "  then fix only with explicit user approval. The user owns their work.\n\n"
        "PREFERRED WORKFLOW for 'how do I…' / advisory questions:\n"
        "  • `list_dsl_functions` (and any narrowly scoped category filter) to "
        "ground your suggestion in real function signatures, then `finish` with "
        "a concise answer plus a worked DSL snippet. Only modify state if the "
        "user explicitly asks.\n\n"
        "PREFERRED WORKFLOW for 'add / update / delete a STEP':\n"
        "  1. `get_saved_rule(rule_id)` to fetch current steps and their step_ids.\n"
        "  2. ADD step: `add_step_to_rule(rule_id, step)` — use insert_before_step_id\n"
        "     or insert_after_step_id to control where it lands in the step list.\n"
        "  3. UPDATE step: `update_step(rule_id, step_id, patch={field:value})`\n"
        "     for one or more fields. DEEP-MERGE — only listed fields change.\n"
        "     OR `patch_step(rule_id, step_id, ops=[{op,path,value}])` for a\n"
        "     single JSON-Pointer leaf. Check `persisted.ok` — false = edit failed.\n"
        "  4. DELETE step: `delete_step(rule_id, step_id)`.\n"
        "  5. Verify: `debug_step(rule_id, step_id)` after any change.\n"
        "  6. Confirm: `dry_run_template` to validate end-to-end output.\n\n"
        "PREFERRED WORKFLOW for 'add / update / delete a RULE':\n"
        "  ADD: `create_saved_rule` (add steps incrementally, test each),\n"
        "    then `attach_rules_to_template(template_id, rule_ids=[...])` to wire it in.\n"
        "  UPDATE: `get_saved_rule` → use `update_step` / `add_step_to_rule` /\n"
        "    `delete_step` / `update_saved_rule` as needed. NEVER duplicate a rule\n"
        "    with a _v2/_fixed/_new suffix — always edit the existing rule in place.\n"
        "  DELETE: call `delete_saved_rule` (requires explicit user approval) →\n"
        "    `attach_rules_to_template` with remaining rule_ids to update the template.\n\n"
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
        "3. NEVER pass inline `dsl_code` to `create_or_replace_template`. "
        "Build the rule with `create_saved_rule` / `add_step_to_rule`, then "
        "call `attach_rules_to_template` passing `rule_ids` to assemble the "
        "template. `create_or_replace_template` is ONLY for registering the "
        "template shell (event_name + name, no dsl_code).\n"
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
        "debit/credit transaction pair in `outputs.transactions[]`. Use "
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
        "                 *** OUTPUTVAR SCOPING RULE — READ THIS FIRST ***\n"
        "                 An outputVar's `name` IS the downstream variable. Once\n"
        "                 you set outputVar.name='foo', the identifier `foo` is\n"
        "                 directly in scope for every step that follows the\n"
        "                 schedule step. There is NO extra layer, NO accessor\n"
        "                 function, NO alias calc step required.\n"
        "                 WRONG (hard-blocked): creating a calc step named\n"
        "                   'foo' with formula='amortization_schedule_foo'\n"
        "                 WRONG: naming the outputVar 'amortization_schedule_foo'\n"
        "                   and then referencing 'foo' in a downstream step.\n"
        "                 RIGHT: name the outputVar exactly what you want to use\n"
        "                   downstream. E.g.:\n"
        "                   outputVars:[{name:'current_amortization', type:'filter',\n"
        "                     column:'amortization', matchCol:'period_date',\n"
        "                     matchValue:'postingdate'}]\n"
        "                   → `current_amortization` is now directly available.\n"
        "                   Do NOT create any further step for it.\n"
        "                 NAMING ANTI-PATTERN TO AVOID: the auto-generated default\n"
        "                 names follow '<scheduleName>_current' / '<scheduleName>_last'.\n"
        "                 If you override them (which you should, for clarity), you MUST\n"
        "                 use your custom name everywhere downstream — NOT the auto name.\n"
        "                 *** START/END DATE MANDATE ***\n"
        "                 For every date-based schedule you MUST supply BOTH\n"
        "                 startDateSource+value AND endDateSource+value.\n"
        "                 NEVER leave either blank. Decision tree:\n"
        "                   1. Does the event have a field for start/end? Use\n"
        "                      startDateSource:'field', startDateField:'EVT.fieldname'.\n"
        "                   2. Is there a calc step that computes it? Use\n"
        "                      startDateSource:'formula', startDateFormula:'stepVarName'.\n"
        "                   3. No relevant date field at all? FALL BACK to:\n"
        "                      startDateSource:'formula', startDateFormula:'postingdate'\n"
        "                      endDateSource:'formula',   endDateFormula:'add_years(postingdate,1)'\n"
        "                 The validator auto-heals missing dates with this fallback\n"
        "                 and records the change in scheduleConfig._autohealed —\n"
        "                 CHECK that array after saving and fix if a better field exists.\n"
        "                 USE schedule FOR: depreciation / amortisation / amortization /\n"
        "                 accretion / runoff / payment plans / EIR / PIT-PD term-structure /\n"
        "                 any 'over the life of' calc that produces ONE row per period.\n"
        "                 Schedule columns CAN reference outer calc-step variables,\n"
        "                 EVENTNAME.field, prior columns in the same array, and built-ins\n"
        "                 (period_index, period_date, period_number, total_periods, lag,\n"
        "                 dcf, days_in_current_period, daily_basis). NEVER substitute a\n"
        "                 calc step or a standalone create_saved_schedule call for an\n"
        "                 inline schedule step inside a rule.\n"
        "                 *** contextVars — DO NOT SET THIS FIELD ***\n"
        "                 contextVars is AUTO-DERIVED from your column formulas.\n"
        "                 NEVER include `contextVars` in your scheduleConfig.\n"
        "                 If you include it, the server DISCARDS it and replaces\n"
        "                 it with the auto-derived list. More importantly, if you\n"
        "                 put an identifier in contextVars that isn't a real step\n"
        "                 variable the validator will block your save. The fix is\n"
        "                 always to OMIT contextVars entirely.\n\n"
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
        "     *** instrumentid / subinstrumentid STEP RULES (HARD MANDATORY) ***\n"
        "       • NEVER create a calc step named 'instrumentid'. Hard-blocked.\n"
        "       • ALWAYS create a calc step named 'subinstrumentid'.\n"
        "         BEFORE deciding scalar vs non-scalar you MUST call\n"
        "         get_event_data(event_name='<event>') and inspect the rows.\n"
        "         Count distinct subinstrumentid values for any instrumentid:\n"
        "           • If ALL instrumentids have exactly 1 subinstrumentid:\n"
        "               → SCALAR:\n"
        "               {name:'subinstrumentid', stepType:'calc',\n"
        "                source:'event_field', eventField:'EVENTNAME.subinstrumentid'}\n"
        "           • If ANY instrumentid has 2+ distinct subinstrumentids:\n"
        "               → NON-SCALAR (use this even if MOST instruments are scalar):\n"
        "               {name:'subinstrumentid', stepType:'calc', source:'collect',\n"
        "                collectType:'collect_by_instrument',\n"
        "                eventField:'EVENTNAME.subinstrumentid'}\n"
        "         *** CASCADING RULE: if subinstrumentid is NON-SCALAR,\n"
        "             then EVERY other field from that event that varies\n"
        "             per sub-instrument MUST also use collect_by_instrument.\n"
        "             Using source:'event_field' on a non-scalar event\n"
        "             silently drops all but the first row. ***\n"
        "         DO NOT GUESS. DO NOT DEFAULT TO SCALAR. Always check\n"
        "         get_event_data first.\n"
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
        "  0a. MANDATORY FIELD-PLANNING GATE — YOU MAY NOT SKIP THIS.\n"
        "     BEFORE calling create_saved_rule or add_step_to_rule you MUST\n"
        "     complete ALL of the following planning steps first:\n"
        "\n"
        "     STEP A — CHECK THE DATA (mandatory for every new rule):\n"
        "       Call get_event_data(event_name='<event>') for EVERY event the rule\n"
        "       will reference. Inspect the returned rows carefully:\n"
        "         1. List every field the model needs from each event.\n"
        "         2. For each field, decide: scalar or non-scalar?\n"
        "            SCALAR  = one value per (instrumentid × postingdate).\n"
        "            NON-SCALAR = multiple values per instrumentid (e.g. multiple\n"
        "              sub-instruments sharing one instrumentid, or multiple\n"
        "              postingdates of the same event for one instrument).\n"
        "         3. Count distinct subinstrumentid values per instrumentid.\n"
        "            If ANY instrumentid has >1 distinct subinstrumentid in\n"
        "            the loaded data → the event is NON-SCALAR for that field.\n"
        "\n"
        "     STEP B — MAP EVERY FIELD TO A STEP TYPE:\n"
        "       For EACH field you listed in Step A, choose EXACTLY ONE:\n"
        "         SCALAR field on the current row (one value per\n"
        "           instrumentid×postingdate) → source:'event_field'\n"
        "             {name:'cost', stepType:'calc', source:'event_field',\n"
        "              eventField:'AssetEvent.original_cost'}\n"
        "         NON-SCALAR / MULTI-SUB-INSTRUMENT (multiple subinstrumentids\n"
        "           under one instrumentid) → source:'collect',\n"
        "           collectType:'collect_by_instrument':\n"
        "             {name:'balances', stepType:'calc', source:'collect',\n"
        "              collectType:'collect_by_instrument',\n"
        "              eventField:'BalanceEvent.balance_amount'}\n"
        "           *** IF ANY subinstrumentid IS NON-SCALAR, EVERY\n"
        "               field from that event MUST also use\n"
        "               collect_by_instrument, not event_field. Using\n"
        "               event_field on a non-scalar event returns ONLY\n"
        "               the first matching row and silently drops the rest. ***\n"
        "         REFERENCE TABLE (instrument-independent lookup) → collect_all:\n"
        "             {name:'rate_table', stepType:'calc', source:'formula',\n"
        "              formula:\"collect_all('RATES.rate')\"}\n"
        "\n"
        "     STEP C — BUILD THE INPUTS BLOCK FIRST:\n"
        "       The FIRST THREE steps of EVERY rule MUST be the mandatory\n"
        "       date + subinstrumentid alias steps below. No exceptions.\n"
        "\n"
        "       MANDATORY STEP 1 (always event_field):\n"
        "         {name:'postingdate', stepType:'calc', source:'event_field',\n"
        "          eventField:'EVT.postingdate'}\n"
        "         (replace EVT with the primary event name, e.g. REV, LOAN)\n"
        "\n"
        "       MANDATORY STEP 2 (always event_field):\n"
        "         {name:'effectivedate', stepType:'calc', source:'event_field',\n"
        "          eventField:'EVT.effectivedate'}\n"
        "\n"
        "       MANDATORY STEP 3 — pick the source based on distinct subId count:\n"
        "\n"
        "         CASE A — distinct subinstrumentids per instrumentid = 1 (scalar):\n"
        "           {name:'subinstrumentid', stepType:'calc', source:'event_field',\n"
        "            eventField:'EVT.subinstrumentid'}\n"
        "\n"
        "         CASE B — distinct subinstrumentids per instrumentid > 1\n"
        "           AND you want per-subinstrument execution\n"
        "           (engine runs rule once per subId row — DEFAULT for IFRS 15\n"
        "           performance obligations and similar line-item models):\n"
        "           {name:'subinstrumentid', stepType:'calc', source:'event_field',\n"
        "            eventField:'EVT.subinstrumentid'}\n"
        "           (All other data fields also use source:'event_field' — each\n"
        "           execution sees exactly one subId's row.)\n"
        "\n"
        "         CASE C — distinct subinstrumentids per instrumentid > 1\n"
        "           AND you need to aggregate across ALL subIds in one pass:\n"
        "           {name:'subinstrumentid', stepType:'calc', source:'collect',\n"
        "            collectType:'collect_by_instrument',\n"
        "            eventField:'EVT.subinstrumentid'}\n"
        "           (All other data fields from that event also need\n"
        "           source:'collect', collectType:'collect_by_instrument'.)\n"
        "\n"
        "       TRANSACTION WIRING — MANDATORY:\n"
        "       Every transaction entry MUST reference the alias step names:\n"
        "         postingDate:     'postingdate'    <- NOT 'EVT.postingdate'\n"
        "         effectiveDate:   'effectivedate'  <- NOT 'EVT.effectivedate'\n"
        "         subInstrumentId: 'subinstrumentid'<- NOT '1.0'\n"
        "       The code generator inlines these as Python variable names.\n"
        "       Using raw EVT.postingdate or a literal '1.0' in a transaction\n"
        "       field causes 'name is not defined' errors at runtime.\n"
        "\n"
        "       AFTER the three mandatory steps, add:\n"
        "         • One calc step per business data field (snake_case names).\n"
        "         NOTE: NEVER create a step named 'instrumentid' — hard-blocked.\n"
        "       Every subsequent calc / condition / iteration / schedule step\n"
        "       MUST reference these variable names only — NO raw EVENTNAME.field\n"
        "       expressions after the inputs block.\n"
        "\n"
        "     Why: agents that skip the mandatory date/subid alias steps end up\n"
        "     passing raw EVT.postingdate or REV_PostingDate into transactions,\n"
        "     producing 'name is not defined' errors. Agents that default\n"
        "     subInstrumentId to '1.0' on a multi-subId event mis-tag every\n"
        "     transaction. These three alias steps eliminate both failure modes.\n"
        "  0b. CLOSE EVERY RULE WITH TRANSACTIONS — NON-NEGOTIABLE.\n"
        "     A rule is INCOMPLETE until `outputs.transactions[]` contains\n"
        "     at least one balanced debit/credit transaction pair. The Transactions panel\n"
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
        "         MULTIPLE transaction pairs — one debit/credit type per leg.\n"
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
        "     • Register transaction types in matched debit/credit pairs upfront\n"
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
        "       (b) dry_run_template returned balanced debit/credit totals with\n"
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
        " 19. NEVER STOP ON VALIDATION FAILURE — with one critical exception.\n"
        "     *** EXCEPTION — USER-AUTHORED STEPS ***\n"
        "     If the user BUILT the step themselves (or says 'I created this'\n"
        "     / 'I wrote this' / 'I added this') and hit an error, do NOT\n"
        "     silently fix it. Instead follow the 'diagnose an error in\n"
        "     user-authored steps' workflow above: diagnose → explain →\n"
        "     propose fix → ask for confirmation → then fix.\n"
        "     *** FOR AGENT-AUTHORED STEPS (the normal case) ***\n"
        "     When ANY tool returns an `errors` array, an `ok: false` flag,\n"
        "     or a ToolError mentioning `undefined`, `not defined`,\n"
        "     `unbalanced`, `missing`, or `failed`, you are NOT done.\n"
        "     Your next action MUST be a fix:\n"
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
        " 19c. DIAGNOSING USER-AUTHORED ERRORS — ALWAYS EXPLAIN BEFORE FIXING.\n"
        "      When the user shares an error they got while testing their own\n"
        "      step, rule, or schedule, your job is DIAGNOSTICIAN first,\n"
        "      then implementer (only if asked).\n"
        "      Required response structure:\n"
        "        1. ROOT CAUSE: one sentence naming the exact problem.\n"
        "        2. WHY IT FAILS: explain in plain terms what the DSL/runtime\n"
        "           tried to do and why it hit this error.\n"
        "        3. WHAT NEEDS TO CHANGE: the specific field, formula, or\n"
        "           structure that needs to be different.\n"
        "        4. CORRECTED EXAMPLE: show the fixed step/formula as a concrete\n"
        "           code snippet the user can understand and optionally copy.\n"
        "        5. QUESTION: 'Would you like me to apply this fix now?'\n"
        "      Do NOT call `update_step`, `patch_step`, `add_step_to_rule`,\n"
        "      or `delete_step` until the user explicitly says yes.\n"
        "      Do NOT say 'I cannot fix this' — always provide the corrected\n"
        "      example even if you do not apply it yet.\n\n"
" 19a. DO NOT INVENT RUNTIME LIMITATIONS. The runtime has exactly\n"
        "      ONE plan-gate (`submit_plan` once per run, then every mutator\n"
        "      tool works). There is NO 'session gate', NO 'transaction\n"
        "      edit gate', NO 'normal path vs alternate path'. If you have\n"
        "      already called `submit_plan` this run, then EVERY write tool\n"
        "      below — `add_transaction_to_rule`, `update_step`,\n"
        "      `add_step_to_rule`, `attach_rules_to_template`, etc. — is\n"
        "      open to you. If a write tool returns a ToolError, READ the\n"
        "      error text — it always names the missing/invalid arg or the\n"
        "      exact fix. Do NOT claim 'I'm blocked by plan-gating' or\n"
        "      'I can't through the normal path' — the runtime will reject\n"
        "      a `finish` summary containing such language. Fix the args\n"
        "      and retry the SAME tool.\n"
        " 19b. NEVER ABANDON A USER-REQUESTED DELIVERABLE. If the user\n"
        "      asked for stages 1/2/3 with sample data AND balanced ECL\n"
        "      transactions, finishing with 'I built the event and rule but\n"
        "      transactions are missing — want me to continue?' is a\n"
        "      FAILURE, not a partial success. Build EVERYTHING the user\n"
        "      asked for inside this run. Stopping mid-way and asking the\n"
        "      user to re-prompt is the worst possible outcome.\n"
        " 20. TRANSACTION TYPE POLICY — REUSE BEFORE REGISTER.\n"
        "     The WORKSPACE SNAPSHOT injected at the start of this turn list\n"
        "     all already-registered transaction types. Before calling\n"
        "     `add_transaction_types` or writing `outputs.transactions[]`:\n"
        "       (a) Check the snapshot's REGISTERED TRANSACTION TYPES section.\n"
        "       (b) If matching debit/credit types exist → use them. Do NOT\n"
        "           rename or replace them without explicit user instruction.\n"
        "       (c) If the user never asked you to change transaction types,\n"
        "           treat existing types as LOCKED. Ask before changing them.\n"
        "       (d) Only call `add_transaction_types` for genuinely new types\n"
        "           that have NO equivalent in the snapshot.\n"
        "     This rule applies even when generating sample data or building\n"
        "     rules from scratch — the transaction type namespace is SHARED\n"
        "     across all templates and must not be polluted with duplicates.\n\n"
        " 21. ONE RULE OR MANY? Default to ONE rule per accounting event\n"
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
        "      (c) Ask the user for clarification ONLY if the question is truly\n"
        "          business-specific (e.g. unknown threshold value, unknown rate).\n"
        "          NEVER ask which transaction types to emit for a named IFRS/GAAP\n"
        "          standard — you know the conventions. Remember: this app\n"
        "          produces TRANSACTIONS consumed by a downstream system for\n"
        "          journal posting. Do NOT call outputs 'journal entries'.\n"
        "          NEVER ask for sample data — generate it yourself with\n"
        "          `generate_sample_event_data(field_hints={...})` supplying\n"
        "          realistic values for every field the rule references.\n"
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
    # Soft-loop detector: track ok=False tool results (not ToolErrors) and
    # alternating patch↔test cycles. Uses the last-N tool call names.
    recent_tool_calls: list[str] = []   # last 10 successful tool call names
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
                    # I19: force a tool call on step 1 so the agent cannot
                    # silently bail out before submit_plan / find_similar_template.
                    tool_choice=("required" if step == 1 else None),
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
                    # Plumb the run_id into tool-side context so dispatch_tool's
                    # plan-gate can find the active plan in _RUN_PLANS.
                    try:
                        from .tools import set_current_run_id as _set_rid
                        _set_rid(run_id)
                    except Exception:
                        pass
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
                    # ── Soft-loop detection ───────────────────────────────
                    # Track this call name for alternating-cycle detection.
                    recent_tool_calls.append(name)
                    recent_tool_calls[:] = recent_tool_calls[-10:]
                    # (A) ok=False result repeated 2+ times on the same tool
                    if isinstance(result, dict) and result.get("ok") is False:
                        _sfail_at = (result.get("failed_at") or
                                     result.get("error") or "ok_false")
                        _sfail_sig = _sfail_at[:60]
                        _soft_key = (name, f"soft:{_sfail_sig}")
                        recent_errors.append(_soft_key)
                        recent_errors = recent_errors[-12:]
                        _soft_count = sum(
                            1 for e in recent_errors if e == _soft_key
                        )
                        if (_soft_count >= 2
                                and _soft_key not in nudge_already_sent_for):
                            nudge_already_sent_for.add(_soft_key)
                            _fh = (result.get("fix_hint") or "")
                            _soft_nudge = (
                                f"SOFT-LOOP DETECTED: `{name}` has returned "
                                f"ok=false (failed_at='{_sfail_sig}') "
                                f"{_soft_count} times in a row. "
                            )
                            if _fh:
                                _soft_nudge += f"The tool says: {_fh}"
                            else:
                                _soft_nudge += (
                                    "Stop repeating the same call. "
                                    "Try a different approach."
                                )
                            pending_nudge = (_soft_key[0], _soft_key[1],
                                             _soft_nudge)
                    # (B) patch/update → test_schedule_step alternating cycle
                    _PATCH_TOOLS = {
                        "patch_step", "update_step", "add_step_to_rule",
                        "replace_schedule_column",
                    }
                    if len(recent_tool_calls) >= 4:
                        _alts = sum(
                            1 for _i in range(len(recent_tool_calls) - 1)
                            if (recent_tool_calls[_i] in _PATCH_TOOLS
                                and recent_tool_calls[_i + 1]
                                == "test_schedule_step")
                        )
                        _cycle_key: tuple[str, str] = (
                            "patch_test_cycle", "schedule_loop"
                        )
                        if (_alts >= 3
                                and _cycle_key not in nudge_already_sent_for):
                            nudge_already_sent_for.add(_cycle_key)
                            pending_nudge = (
                                _cycle_key[0], _cycle_key[1],
                                "LOOP DETECTED: you have alternated between "
                                "patching/updating the schedule step and calling "
                                "test_schedule_step at least 3 times without "
                                "convergence. The most common root cause is "
                                "MISSING SAMPLE DATA, not a broken formula. "
                                "MANDATORY: call generate_sample_event_data for "
                                "each activity event referenced by the schedule "
                                "step, then call test_schedule_step. "
                                "Do NOT patch formulas again until you have data."
                            )
                    # ── End soft-loop detection ───────────────────────────
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
                        # E11/E12: append targeted recovery suggestions.
                        nudge_text = _build_loop_nudge(name, sig, err)
                        _step_update_tools = {
                            "update_step", "patch_step", "replace_schedule_column",
                        }
                        if name in _step_update_tools and isinstance(args, dict):
                            sid = (args.get("step_id") or args.get("step_name")
                                   or "<this step>")
                            rid_arg = args.get("rule_id") or "<rule_id>"
                            nudge_text += (
                                f"\n\nE12 — STEP REWRITE PROTOCOL: stop trying to "
                                f"patch step `{sid}`. Call `delete_step(rule_id="
                                f"'{rid_arg}', step_id='{sid}')` then "
                                f"`add_step_to_rule(rule_id='{rid_arg}', step={{...}})` "
                                f"with the corrected step shape from scratch. A clean "
                                f"rewrite is faster than another partial patch."
                            )
                        else:
                            nudge_text += (
                                "\n\nE11 — PATTERN-MATCH PROTOCOL: call "
                                "`find_similar_template(intent='<one-line goal>', "
                                "keywords=[...])` to discover a saved rule of "
                                "the same shape, or `list_canonical_patterns` to "
                                "pick A/B/C/D, then `apply_canonical_pattern` to "
                                "scaffold the rule in one shot instead of "
                                "hand-authoring it."
                            )
                        pending_nudge = (name, sig, nudge_text)
                    elif same_count == 4:
                        pending_nudge = (
                            name, sig,
                            _build_loop_nudge(name, sig, err)
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
