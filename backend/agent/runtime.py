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
import re
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
#
# Persisted to db.agent_sessions when a DB is available (durable across
# restarts / workers); the in-process dict below is the fallback used only when
# running in in-memory mode (db is None).
# ──────────────────────────────────────────────────────────────────────────
_SESSION_HISTORY: dict[str, list[dict]] = {}
# Cap kept history per session. Oldest pairs are dropped when exceeded.
_SESSION_MAX_MESSAGES = 60

# Delta workspace refresh cadence (see run_agent). Re-inject a fresh snapshot
# every this-many steps when the workspace was mutated since the last refresh.
_REFRESH_EVERY = 12
# Tools whose success changes the broad workspace shape (events / rules /
# txn types / schedules / templates) — i.e. things the first-turn snapshot
# listed. After any of these the snapshot is potentially stale.
_WORKSPACE_MUTATING_TOOLS = {
    "create_event_definitions", "add_transaction_types",
    "generate_sample_event_data",
    "create_saved_rule", "update_saved_rule", "delete_saved_rule",
    "create_saved_schedule", "delete_saved_schedule",
    "create_or_replace_template", "delete_template",
    "attach_rules_to_template", "apply_canonical_pattern",
    "clear_all_data",
}

# ── In-run context compaction ────────────────────────────────────────────
# Within a single run the `messages` transcript grows every step (up to
# max_steps). `_truncate_for_observation` caps each individual tool result,
# but the TOTAL still grows unbounded, so a long run eventually ships a huge
# context on every provider call (slow + costly + can exceed the window).
#
# We solve this deterministically (no extra LLM summarisation call — important
# for auditability in a regulated deployment): once the estimated transcript
# size crosses `_CONTEXT_CHAR_BUDGET`, the OLDEST tool results and assistant
# reasoning are shrunk to a short stub, oldest-first, while the system prompt
# and the most recent `_COMPACT_KEEP_RECENT` messages stay full-fidelity.
# Message STRUCTURE is never altered — every assistant tool_calls entry keeps
# its matching `tool` responses — so the OpenAI/Anthropic tool-ordering
# invariant can't be broken. Compaction runs on a COPY each step; the real
# `messages` list stays intact for persistence and for the next step.
#
# ~120k chars ≈ ~30k tokens — comfortably inside every provider's window
# (gpt-4o-mini 128k, Claude 200k, Gemini) with headroom for the reply.
_CONTEXT_CHAR_BUDGET = 120_000
# Most-recent messages kept at full fidelity (≈ last 5-7 steps).
_COMPACT_KEEP_RECENT = 14
# Chars retained from the head of a compacted message (status/counts live here).
_COMPACT_STUB_CHARS = 600


async def reset_session_history(session_id: str, *, db=None) -> bool:
    """Drop persisted conversation history for the given chat session.
    Returns True if anything was cleared. Clears both the DB record (when a DB
    is provided) and the in-process fallback cache."""
    if not session_id:
        return False
    cleared = False
    if db is not None:
        try:
            res = await db.agent_sessions.delete_one({"session_id": session_id})
            cleared = bool(getattr(res, "deleted_count", 0))
        except Exception as exc:
            logger.warning("reset_session_history DB delete failed: %s", exc)
    if _SESSION_HISTORY.pop(session_id, None) is not None:
        cleared = True
    return cleared


async def _load_session_history(db, session_id: str) -> list[dict]:
    """Load prior conversation history for a session. DB is the source of truth
    when present; falls back to the in-process cache only in in-memory mode."""
    if not session_id:
        return []
    if db is not None:
        try:
            doc = await db.agent_sessions.find_one(
                {"session_id": session_id}, {"_id": 0, "messages": 1}
            )
            return list((doc or {}).get("messages") or [])
        except Exception as exc:
            logger.warning("load_session_history failed: %s", exc)
            return []
    return list(_SESSION_HISTORY.get(session_id) or [])


async def _save_session_history(db, session_id: str, msgs: list[dict]) -> None:
    """Persist (trimmed) conversation history for a session. Writes to the DB
    when present, otherwise to the in-process fallback cache."""
    if not session_id:
        return
    trimmed = _trim_history(msgs)
    if db is not None:
        try:
            await db.agent_sessions.update_one(
                {"session_id": session_id},
                {"$set": {"session_id": session_id, "messages": trimmed,
                          "updated_at": _now_iso()}},
                upsert=True,
            )
            return
        except Exception as exc:
            logger.warning("save_session_history failed: %s", exc)
    _SESSION_HISTORY[session_id] = trimmed


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

    # Uploaded Excel workbooks awaiting analysis / import
    try:
        from . import workbook as _wb
        wbs = _wb.list_workbooks()
        if wbs:
            parts.append(f"\nUPLOADED EXCEL WORKBOOKS ({len(wbs)}):")
            for m in wbs[:10]:
                roles = m.get("roles") or {}
                role_note = (
                    ", ".join(f"{s}={r}" for s, r in roles.items())
                    if roles else "roles NOT confirmed yet"
                )
                parts.append(
                    f"  • {m.get('filename')} (workbook_id={m.get('workbook_id')}, "
                    f"sheets: {', '.join(m.get('sheets') or [])}; {role_note})"
                )
            parts.append(
                "  If the user's request concerns one of these workbooks, "
                "follow the workbook-import workflow (get_dsl_syntax_guide "
                "section='excel_translation_guide')."
            )
    except Exception:
        pass

    # Uploaded requirement documents (PDF / Word) awaiting analysis
    try:
        from . import requirements_doc as _rd
        docs = _rd.list_documents()
        if docs:
            parts.append(f"\nUPLOADED REQUIREMENT DOCUMENTS ({len(docs)}):")
            for m in docs[:10]:
                unit = (f"{m.get('pages')} pages" if m.get("pages")
                        else f"{m.get('paragraphs', 0)} paragraphs")
                parts.append(
                    f"  • {m.get('filename')} (document_id={m.get('document_id')}, "
                    f"{m.get('kind')}, {unit})"
                )
            parts.append(
                "  If the user's request concerns one of these documents, "
                "read it with read_requirement_document, summarise your "
                "understanding, CONFIRM the details with the user, then build."
            )
    except Exception:
        pass

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


def _estimate_msg_chars(m: dict) -> int:
    """Cheap proxy for a message's context cost (content + serialised tool_calls)."""
    content = m.get("content")
    n = len(content) if isinstance(content, str) else 0
    for tc in (m.get("tool_calls") or []):
        try:
            n += len(json.dumps(tc, default=str))
        except Exception:
            n += 200
    return n


def _compact_messages(
    messages: list[dict],
    *,
    budget: int = _CONTEXT_CHAR_BUDGET,
    keep_recent: int = _COMPACT_KEEP_RECENT,
    stub: int = _COMPACT_STUB_CHARS,
) -> tuple[list[dict], int]:
    """Return a size-bounded COPY of `messages` for sending to the provider.

    Deterministic, structure-preserving compaction: when the transcript
    exceeds `budget`, the head-most tool/assistant message bodies are shrunk
    to a `stub`-char excerpt, oldest-first, until under budget. The system
    prompt (index 0) and the last `keep_recent` messages are never shrunk.
    No message is removed and `tool_calls` arrays are left intact, so the
    provider's "every tool_call_id needs a tool response" invariant holds.

    Returns (possibly-new list, number of messages compacted). When nothing
    needs compacting the ORIGINAL list is returned unchanged (0).
    """
    total = sum(_estimate_msg_chars(m) for m in messages)
    if total <= budget:
        return messages, 0

    n = len(messages)
    protected_tail_start = max(1, n - keep_recent)
    out: list[dict] = []
    compacted = 0
    running = total
    for i, m in enumerate(messages):
        # Never shrink the system prompt or the recent working window; and
        # stop shrinking as soon as we're back under budget.
        if i == 0 or i >= protected_tail_start or running <= budget:
            out.append(m)
            continue
        role = m.get("role")
        content = m.get("content")
        if isinstance(content, str) and len(content) > stub:
            saved = len(content) - stub
            note = (
                "… [older tool result compacted]" if role == "tool"
                else "… [earlier reasoning compacted]"
            )
            new_m = dict(m)          # shallow copy — keeps tool_calls / ids
            new_m["content"] = content[:stub] + note
            out.append(new_m)
            compacted += 1
            running -= saved
        else:
            out.append(m)
    return out, compacted


# ──────────────────────────────────────────────────────────────────────────
# Summary grounding — anti-hallucination guard.
#
# A dangerous failure mode is the agent stating a MONEY figure in its final
# summary that it never actually computed ("I booked $12,345.67 of interest")
# when the real dry-run produced something else. We can't stop the model from
# writing it, but we CAN detect it deterministically: every material money
# amount in the summary should match a number the agent actually OBSERVED in a
# tool result during the run. Unmatched amounts are surfaced as a warning (not
# a hard failure — for a regulated deployment we flag for human review rather
# than silently editing the model's text). All three functions are pure and
# unit-tested in tests/test_hallucination_guards.py.
# ──────────────────────────────────────────────────────────────────────────

# Matches $-prefixed / thousands-separated / 2-decimal numbers in prose.
_MONEY_RE = re.compile(r"(?<![\w.])(\$\s?)?(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+\.\d{2}|\d+)(?![\w])")


def _money_numbers_in_text(text: str, *, min_magnitude: float = 100.0) -> set[float]:
    """Extract MATERIAL money figures from prose. Only numbers that read like
    money — a `$` prefix, a thousands separator, or exactly two decimals — and
    whose magnitude is >= min_magnitude. Bare small integers and 4-digit years
    are ignored so counts like '5 loans' / '12 months' / '2024' never trip it."""
    out: set[float] = set()
    for m in _MONEY_RE.finditer(text or ""):
        dollar, raw = m.group(1), m.group(2)
        money_like = bool(dollar) or ("," in raw) or bool(re.fullmatch(r"\d+\.\d{2}", raw))
        if not money_like:
            continue
        try:
            val = round(float(raw.replace(",", "")), 2)
        except ValueError:
            continue
        if abs(val) >= min_magnitude:
            out.add(val)
    return out


def _numbers_in_result(value, _depth: int = 0) -> set[float]:
    """Recursively collect every numeric value in a tool result (the universe
    of numbers the agent legitimately observed). Bounded depth so a pathological
    payload can't blow the stack."""
    out: set[float] = set()
    if _depth > 8:
        return out
    if isinstance(value, bool):
        return out
    if isinstance(value, (int, float)):
        try:
            out.add(round(float(value), 2))
        except (ValueError, OverflowError):
            pass
    elif isinstance(value, str):
        s = value.replace(",", "").replace("$", "").strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            try:
                out.add(round(float(s), 2))
            except ValueError:
                pass
    elif isinstance(value, dict):
        for v in value.values():
            out |= _numbers_in_result(v, _depth + 1)
    elif isinstance(value, (list, tuple)):
        for v in value:
            out |= _numbers_in_result(v, _depth + 1)
    return out


def _ungrounded_amounts(summary: str, observed: set[float]) -> list[float]:
    """Material money figures in the summary that don't match (within a small
    tolerance) any number the agent observed in a tool result. Non-empty ==
    possible fabrication worth a human's eye."""
    ungrounded: list[float] = []
    for s in sorted(_money_numbers_in_text(summary)):
        tol = max(0.02, abs(s) * 0.005)          # 2 cents or 0.5%, whichever larger
        if not any(abs(s - o) <= tol for o in observed):
            ungrounded.append(s)
    return ungrounded


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
        f"  1. Call `explain_error(error_text=..., tool_name='{tool_name}')` to "
        f"get the root-cause diagnosis and exact fix recipe for this error.\n"
        f"  2. If the error is in an expression, call `lint_expression` on the "
        f"corrected expression to confirm it passes BEFORE re-saving.\n"
        f"  3. Call `preview_generated_code` on the rule to see the Python it "
        f"compiles to and spot the undefined/misordered variable.\n"
        f"  4. Call `get_dsl_syntax_guide` to read the binding DSL constraints "
        f"and worked examples of each step type.\n"
        f"  5. Call `get_saved_rule` on an existing rule that uses the same "
        f"step type and copy its expression shape exactly.\n"
        f"  6. If a recent edit broke the rule, call `revert_rule` to restore "
        f"the last good version, then re-apply the change correctly.\n"
        f"  7. Call `finish` with a question for the user explaining what you "
        f"are stuck on.\n"
        f"Do NOT make another variation of the failing call before doing one "
        f"of those things."
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
            "array_get(arr, idx) (or array_first/array_last for the ends)."
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
    # NOTE: DSL semantics documented here are duplicated in
    # backend/agent/knowledge/dsl_authoring_guide.md (the canonical reference,
    # served verbatim via get_dsl_syntax_guide section='authoring_guide') and
    # in _DSL_SYNTAX_GUIDE in tools.py. When DSL semantics change, update the
    # authoring guide FIRST, then mirror the change here and in tools.py.
    return (
        "You are Fyntrac DSL Studio's autonomous accounting agent — a chartered "
        "accountant and financial-modelling expert. You author IFRS- and US-GAAP-"
        "compliant accounting models and answer questions about the standards.\n\n"
        "WHO YOU ARE TALKING TO — READ FIRST:\n"
        "  Your users are FINANCE AND ACCOUNTING professionals, NOT software "
        "developers. Every message you show them must be in plain business "
        "English. In all user-facing text (the `finish` summary, any question "
        "you ask, any explanation):\n"
        "  • NEVER show code, JSON, tool names (e.g. 'add_transaction_to_rule'), "
        "field paths (e.g. 'outputs.transactions[].amount'), stack traces, HTTP "
        "status codes, or internal identifiers. Describe things the way an "
        "accountant would: 'the monthly interest amount', 'the loan schedule', "
        "'the transactions this rule produces'.\n"
        "  • Talk about WHAT was built and WHAT the numbers mean, not HOW the "
        "engine did it. Instead of 'I called dry_run_rule and it returned "
        "transaction_count=15', say 'I tested the rule on your 5 loans and it "
        "produced 15 transactions.'\n"
        "  • If something failed, explain it in one plain sentence and say what "
        "you did or what they can do — never paste the raw error.\n"
        "  • Use short paragraphs, bullet points, and tables of numbers where "
        "helpful. Amounts should read like money ($1,234.56).\n\n"
        "INPUT & OUTPUT CONTRACT — NON-NEGOTIABLE, APPLIES TO EVERY REQUEST:\n"
        "  This platform has ONE fixed input shape and ONE fixed output shape. "
        "It is identical whether the work comes from an uploaded Excel model, "
        "an uploaded requirements document, or a direct chat instruction — "
        "never invent a different structure for a different source.\n"
        "  OUTPUT — the ONLY output this platform produces is TRANSACTIONS. "
        "There is no other output type (no journal entries, no GL postings, no "
        "reports, no files, no custom objects). Every transaction has EXACTLY "
        "these six fields and no others:\n"
        "    • instrumentid    (which instrument the amount is for)\n"
        "    • subinstrumentid (defaults to '1' when there's only one)\n"
        "    • postingdate     (when it posts)\n"
        "    • effectivedate   (when it takes economic effect)\n"
        "    • transactiontype (the economic result, e.g. InterestIncomeAccrual)\n"
        "    • amount          (a single signed number — negatives are allowed)\n"
        "  These go in the rule's outputs.transactions[] array. One computed "
        "result = ONE transaction. NO debit/credit side, NO balancing, NO "
        "contra/clearing transaction. If the user asks for output in any other "
        "form (a report, a spreadsheet, journal entries, a balance), EXPLAIN "
        "in plain English that the platform only emits transactions in this "
        "fixed shape, show them the six fields above with a small example, and "
        "map their request onto transactions — do not attempt another format.\n"
        "  INPUT — all input data lives in EVENTS, in one of two table shapes:\n"
        "    • STANDARD table (activity data — the normal case): MUST include "
        "the four default columns instrumentid, subinstrumentid, postingdate, "
        "effectivedate, PLUS the business fields the calculation needs. When "
        "you design an input event from Excel, a document, or a prompt, always "
        "map a key column to instrumentid and stamp posting/effective dates "
        "(ask the user which date if the source has none). instrumentid is an "
        "implicit global — never author a step that creates it; always author "
        "the subinstrumentid step.\n"
        "    • REFERENCE table (custom, static lookups — rate tables, product "
        "catalogs, mappings): FREEFORM — any columns, and NONE of the four "
        "standard columns are required. Use eventType='reference', "
        "eventTable='custom', and read it with lookup(...).\n"
        "  Do NOT deviate from these shapes. When you propose an input event or "
        "a transaction output, it MUST fit this contract exactly.\n\n"
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
        "  • IAS 21 / ASC 830 (Foreign Exchange): remeasure monetary items at "
        "the closing rate each period; remeasurement gains/losses to P&L "
        "(translation of a net investment in a foreign operation goes to "
        "OCI/CTA). Emit matched FX gain/loss + carrying-amount-adjustment "
        "transaction pairs.\n"
        "  • IAS 12 / ASC 740 (Income Taxes): current vs deferred tax; deferred "
        "tax on temporary differences (carrying amount vs tax base) at enacted "
        "rates; recognise DTAs only to the extent recoverable.\n"
        "  • IAS 37 (Provisions): recognise when present obligation + probable "
        "outflow + reliable estimate; discount long-dated provisions and unwind "
        "the discount as a period accretion charge (use a schedule step).\n"
        "  • IAS 2 (Inventories): lower of cost and NRV; FIFO or weighted-"
        "average cost; write-down and reversal transactions.\n"
        "  • IFRS 13 / ASC 820 (Fair Value): remeasurement gains/losses to P&L "
        "or OCI as the host standard directs.\n"
        "  • ANY OTHER use case (statutory, regulatory, fund, payroll, tax, "
        "industry-specific): the same mechanics always apply — identify the "
        "measurement basis, compute amounts via calc/schedule steps, and emit "
        "one transaction per computed result. Use your own accounting "
        "knowledge for standards not listed above and state every assumption "
        "in the rule's commentText so the user can audit it.\n"
        "  • This app emits TRANSACTIONS — plain signed amounts, NOT journal "
        "entries and NOT double-entry postings. There is NO debit/credit side "
        "and NO balancing requirement: each computed result is ONE transaction. "
        "A downstream system decides how each transaction maps to a GL posting "
        "— that is not your concern. NEVER describe rule outputs as 'journal "
        "entries', and NEVER add a debit/credit side or a balancing 'contra' "
        "transaction. Name transaction types after the economic result they "
        "represent (e.g. InterestIncomeAccrual, ECLAllowance, "
        "PrincipalRepayment, RevenueRecognised).\n"
        "  • If the user references a specific standard or jurisdiction (e.g. "
        "\"IFRS 9 stage 1\", \"ASC 842 ROU asset\", \"CECL pool\"), follow that "
        "standard's recognition and measurement rules. State your assumptions "
        "explicitly in the rule's commentText so the user can audit them.\n"
        "  • If the user asks a knowledge question (no build/edit), answer "
        "directly without calling tools beyond the optional `list_dsl_functions` "
        "lookup, then `finish` with the explanation.\n\n"
        "GOAL: Build event definitions, generate sample data, and author DSL "
        "templates that produce the user's desired transactions.\n\n"
        "EXCEL WORKBOOK IMPORT — when the snapshot lists uploaded workbooks "
        "or the user mentions a spreadsheet model:\n"
        "  Follow the dedicated workflow in get_dsl_syntax_guide "
        "section='excel_translation_guide'. In short: get_workbook_overview "
        "→ ASK the user to confirm which sheets are inputs / calc / outputs "
        "(set_workbook_sheet_roles) → get_sheet_formulas + "
        "trace_workbook_dependencies → import_workbook_inputs (the user's "
        "REAL data — never generate sample data for imported events) → "
        "submit_plan → build the rule (one step per unique formula pattern; "
        "row-recursive patterns become schedule steps with lag()) → "
        "reconcile_workbook_outputs and iterate until status='reconciled'. "
        "Never finish with unexplained mismatches; state every translation "
        "assumption in the rule's commentText.\n"
        "  REUSE FIRST: if the snapshot already lists an event whose fields "
        "cover a workbook sheet, use it instead of creating a duplicate; "
        "reuse registered transaction types that match the workbook's "
        "output names. Create new events/data ONLY when nothing suitable "
        "exists, and say why.\n"
        "  CALC-ONLY WORKBOOK (no input/output sheets): derive the input "
        "schema from trace_workbook_dependencies (nodes with no incoming "
        "edges + fixed parameter cells), confirm it with the user, and "
        "remember the OUTPUT IS ALWAYS TRANSACTIONS — map the terminal "
        "computed columns to transaction types. With no output sheet, "
        "verify by dry-run + user review instead of reconcile.\n"
        "  TRANSACTIONS ARE JUST AMOUNTS: each output column (or computed "
        "result) becomes exactly ONE transaction with that signed amount. "
        "There is no debit/credit side and no balancing — NEVER add a "
        "contra/clearing account (e.g. a '_Control' type) to balance an "
        "entry. Keep the workbook's sign (negatives are fine).\n\n"
        "REQUIREMENTS DOCUMENT (PDF / Word) — when the snapshot lists uploaded "
        "requirement documents or the user mentions an attached spec / business "
        "requirement doc:\n"
        "  A requirements document describes IN PROSE what to build (it is NOT "
        "a spreadsheet of formulas — that's the Excel workflow above). Follow "
        "this workflow:\n"
        "  1. list_requirement_documents → read_requirement_document for the "
        "relevant document_id. If the response has_more=true, call it again "
        "with the returned next_offset until you have read the WHOLE document. "
        "Never analyse from a partial read.\n"
        "  2. ANALYSE it: identify the accounting treatment / standard, the "
        "inputs (what data each rule needs), the calculations, and the outputs "
        "(the transactions to emit). Map it onto the existing workspace — reuse "
        "events, data, and transaction types from the snapshot where they fit.\n"
        "  3. CONFIRM BEFORE BUILDING — this is mandatory. Call finish with a "
        "SHORT plain-English summary of what you understood and a numbered list "
        "of specific questions on anything ambiguous, missing, or assumed "
        "(e.g. exact rates, periods, day-count, which existing data to use, "
        "how to name transactions). Do NOT create events, rules, or data yet. "
        "Only after the user answers do you proceed with the normal build "
        "workflow (submit_plan → build → test → verify → finish).\n"
        "  4. If the document is completely unambiguous AND fully covered by "
        "existing workspace data, you may state your read and the plan, then "
        "proceed — but still surface every assumption in the rule's "
        "commentText.\n"
        "  Transactions from a requirements doc follow the SAME rule as "
        "everywhere else: plain signed amounts, one per computed result, no "
        "debits/credits, no balancing contra.\n\n"
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
        "  Do NOT call generate_sample_event_data without overwrite=True.\n"
        "  Call `finish` and ask the user:\n"
        "    'I can see [event]: [N rows] already loaded. Should I:\n"
        "     (a) Use this data and go straight to building the template?\n"
        "     (b) Overwrite with fresh sample data?\n"
        "     (c) Append additional rows to the existing data?\n"
        "     (d) Start from scratch?'\n"
        "  If (a): skip steps 2-4, start from step 5.\n"
        "  If (b): call generate_sample_event_data with overwrite=True.\n"
        "  If (c): call generate_sample_event_data with append=True.\n\n"
        "HARD RULE — generate_sample_event_data CONFIRMATION GATE:\n"
        "  If generate_sample_event_data returns {\"status\": \"confirmation_required\"}\n"
        "  you MUST STOP immediately. Do NOT call any write tools. Do NOT retry.\n"
        "  Call `finish` and show the user the exact question from the tool's\n"
        "  `message` field. Only after the user explicitly says to overwrite or\n"
        "  append may you call generate_sample_event_data again — with\n"
        "  overwrite=True (to replace) or append=True (to add rows).\n"
        "  NEVER pass overwrite=True proactively before checking if data exists.\n\n"
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
        "    a) Do existing types match the results this rule produces?\n"
        "       → Use them AS-IS. Do NOT call add_transaction_types.\n"
        "    b) Only SOME of the needed types exist?\n"
        "       → Call add_transaction_types for ONLY the missing ones.\n"
        "    c) No matching types exist at all?\n"
        "       → Call add_transaction_types for the ones you need.\n"
        "    d) User did NOT explicitly ask to change transaction types?\n"
        "       → NEVER replace or rename existing types. Ask the user\n"
        "          if you think a rename would be beneficial.\n"
        "  Register one transaction type per distinct economic result (no "
        "debit/credit sides — a transaction is just an amount).\n"
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
        " 10. `dry_run_template` to verify end-to-end: check transaction counts "
        "and totals by type look sane for the input data.\n"
        " 11. **READINESS GATE**: for every rule you authored, call "
        "`verify_rule_complete`. It returns a checklist confirming all steps "
        "debug-run cleanly, outputs.transactions is populated, transaction "
        "types are registered, and event data is loaded. "
        "Do NOT call `finish` unless every rule's `overall_ready` is true.\n"
        " 12. `finish` with a summary that lists each rule + transactions emitted "
        "+ confirmation that all steps and schedules were tested.\n"
        "SUMMARY WRITING STYLE (for the `finish` summary AND any answer you "
        "give the user):\n"
        "  • Plain business English. NO code, NO tool names, NO field-shape "
        "jargon (say 'monthly interest amount', not "
        "'outputs.transactions[].amount'). The user is an accountant, not a "
        "programmer.\n"
        "  • Use Markdown so it renders nicely: a one-line headline first, "
        "then short **bold** labels, bullet points for lists, and a Markdown "
        "TABLE whenever you show numbers per item (e.g. transactions and their "
        "amounts, or reconciliation results). Keep paragraphs to 1-2 "
        "sentences.\n"
        "  • Lead with the outcome ('Built a loan interest model that posts 3 "
        "transactions per loan'), then the details. End with what the user "
        "can do next, if anything.\n\n"
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
        "Every rule MUST end with at least one transaction in "
        "`outputs.transactions[]`. A transaction is just a signed amount — "
        "there is NO debit/credit side and NO balancing/contra requirement. "
        "Use `add_transaction_to_rule` (once per distinct result you want "
        "posted) AFTER your calc/schedule steps compute the amount. "
        "The Transactions panel reads ONLY from `outputs.transactions[]` — "
        "calc steps named 'transactions' / 'outputs_transactions' do "
        "nothing. The `finish` gate will reject your run if any rule you "
        "touched has no transactions.\n"
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
        "                 *** CONDITIONAL SCHEDULES — scheduleConfig.runIf ***\n"
        "                 To do 'if X build schedule 1 else schedule 2', set\n"
        "                 scheduleConfig.runIf to a boolean DSL expression on each\n"
        "                 schedule. When runIf is false the schedule produces ZERO\n"
        "                 rows (its outputVars become 0/[]), so it does not fire.\n"
        "                 Pair two schedules with inverse runIf (e.g. 'is_lease' and\n"
        "                 'not(is_lease)'), then a condition step picks the result:\n"
        "                 period_charge = if(is_lease, lease_total, depr_total).\n"
        "                 runIf references EARLIER calc-step vars. For a CONDITIONAL\n"
        "                 FREQUENCY use scheduleConfig.frequencyFormula (e.g.\n"
        "                 if(report_monthly, \"M\", \"Q\")). When only the math differs\n"
        "                 (same periods), prefer one schedule with if(...) in columns.\n"
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
        "         then lookup(arr, key) or array_get(arr, idx).\n"
        "       • Per-instrument time-series (multiple postingdates of the\n"
        "         same activity event) → collect_by_instrument('EVT_field').\n"
        "       • Indexed lookup inside an apply_each iteration uses\n"
        "         array_get(arr, index, default) where `index` is the\n"
        "         iteration index variable.\n"
        "       • Date-keyed lookup inside a schedule column formula uses\n"
        "         lookup(values_arr, keys_arr, target_key).\n"
        "         *** CRITICAL: arg order is (VALUES, KEYS, TARGET). ***\n"
        "         values_arr = what you want to RETURN\n"
        "         keys_arr   = what you SEARCH IN to find the target\n"
        "         target_key = the value you are looking for\n"
        "         Example — find SSP mode for product 'P1':\n"
        "           lookup(CatalogSSPMode, CatalogProductIds, 'P1')\n"
        "           → finds 'P1' in CatalogProductIds, returns the\n"
        "             corresponding CatalogSSPMode entry.\n"
        "         WRONG: lookup(CatalogProductIds, CatalogSSPMode, 'P1')\n"
        "           → searches for 'P1' INSIDE the SSP mode array ('AMOUNT',\n"
        "             'USE SALES PRICE', …) — will never match. Returns None.\n"
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
        "           All other fields from that event may also use source:'event_field'.\n"
        "\n"
        "         CASE C — distinct subinstrumentids per instrumentid > 1\n"
        "           (MANDATORY — no exceptions, no alternative mode):\n"
        "           {name:'subinstrumentid', stepType:'calc', source:'collect',\n"
        "            collectType:'collect_by_instrument',\n"
        "            eventField:'EVT.subinstrumentid'}\n"
        "           EVERY field from that event MUST also use source:'collect',\n"
        "           collectType:'collect_by_instrument'. Using source:'event_field'\n"
        "           on ANY field of a multi-subId event is a hard error — the\n"
        "           engine collapses rows before execution and you get only the\n"
        "           last surviving value. There is NO per-subinstrument execution\n"
        "           mode. If distinct subIds per instrumentid > 1, use Case C.\n"
        "           DEFAULT: when the data shows >1 distinct subinstrumentid per\n"
        "           instrumentid, ALWAYS use CASE C.\n"
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
        "  0b. DEBUGGING & DISABLED STEPS — FOR AGENT USE ONLY.\n"
        "     The UI provides disable/enable toggles for each step, rule,\n"
        "     and transaction. When a step or rule is disabled:\n"
        "       • Its generated code is completely skipped (not emitted).\n"
        "       • Disabled steps are NOT added to the execution context.\n"
        "       • Downstream steps that reference a disabled step will fail\n"
        "         with 'undefined name' errors until you re-enable it or fix\n"
        "         the downstream reference.\n"
        "     Use this to debug: disable a suspected problem step, re-run the\n"
        "     rule, and see if the output changes. If it does, the disabled\n"
        "     step was the issue. If it doesn't, that step is not the problem.\n"
        "     Disabled transactions are also skipped at generation time.\n"
        "     The disabled state is persisted with the rule, so you can\n"
        "     disable steps across multiple debugging sessions.\n"
        "  0c. CLOSE EVERY RULE WITH TRANSACTIONS — NON-NEGOTIABLE.\n"
        "     A rule is INCOMPLETE until `outputs.transactions[]` contains\n"
        "     at least one transaction. A transaction is just a signed amount\n"
        "     — there is NO debit/credit side and NO balancing/contra pairing.\n"
        "     The Transactions panel in the UI reads ONLY from this array.\n"
        "     The `finish` tool will refuse to accept your run otherwise.\n"
        "       • After your calc/schedule steps compute the amounts, call\n"
        "         `add_transaction_to_rule` ONCE PER distinct result:\n"
        "             { type:'DepreciationCharge', amount:'depreciation_charge' }\n"
        "       • `amount` MUST be the NAME of a calc step (or a schedule\n"
        "         outputVar). It cannot be an inline expression and cannot\n"
        "         reference a step that doesn't exist.\n"
        "       • Register every transaction type via `add_transaction_types`\n"
        "         BEFORE referencing it.\n"
        "       • For schedule-driven amounts, expose the period total via\n"
        "         scheduleConfig.outputVars (type='sum' or 'last') and use\n"
        "         that outputVar's name as `amount`.\n"
        "       • Multi-result events (e.g. separate interest, principal, and\n"
        "         fee amounts) emit MULTIPLE transactions — one per result.\n"
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
        "       • Array element access: array_get(arr, idx) (array_first/array_last for ends)\n"
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
        "     `add_transaction_to_rule` (once per result — a transaction is\n"
        "     just an amount, no debit/credit side),\n"
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
        "            {type:'ECLAllowance', amount:'amount'} ] }\n"
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
        "       (d) `dry_run_template` emits the expected transactions with\n"
        "           sane amounts and no sanity_warnings.\n"
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
        "     • Register one transaction type per distinct result upfront\n"
        "       (e.g. ECLAllowance, InterestIncome, PrincipalRepayment).\n"
        "       There are no debit/credit pairs — a transaction is just an\n"
        "       amount.\n"
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
        "     Pick the pattern FIRST, then fill in the fields. If the use\n"
        "     case genuinely fits none of the four, start from the CLOSEST\n"
        "     pattern and record in submit_plan (and the rule's commentText)\n"
        "     exactly where and why you deviated — never author free-form\n"
        "     from a blank page.\n"
        " 17. NO 'WOULD YOU LIKE ME TO…' ENDINGS. When you detect a problem\n"
        "     in your own draft (e.g. dry_run shows 0 transactions, sample\n"
        "     data has wrong IDs, debug_step returns null), DO NOT call\n"
        "     `finish` with a message asking the user whether to fix it. FIX\n"
        "     IT FIRST. Only call `finish` when:\n"
        "       (a) verify_rule_complete returned overall_ready=true, AND\n"
        "       (b) dry_run_template emitted the expected transactions with\n"
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
        "     `missing`, or `failed`, you are NOT done.\n"
        "     Your next action MUST be a fix:\n"
        "       • undefined variable → `add_step_to_rule` to define it\n"
        "         BEFORE the step that references it, OR `update_step` to\n"
        "         change the reference to an existing variable.\n"
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
        "      asked for stages 1/2/3 with sample data AND ECL\n"
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
        "       (b) If matching types exist → use them. Do NOT\n"
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
        "       (a) the rules emit different transaction types\n"
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
        "          produces TRANSACTIONS (plain amounts), NOT journal entries.\n"
        "          There are no debit/credit sides. Do NOT call outputs\n"
        "          'journal entries'.\n"
        "          NEVER ask for sample data — generate it yourself with\n"
        "          `generate_sample_event_data(field_hints={...})` supplying\n"
        "          realistic values for every field the rule references.\n"
        "    Do NOT try a third variation of the same broken expression.\n"
        "  • Errors like 'unterminated string literal', 'invalid syntax', and\n"
        "    'unexpected EOF' are almost ALWAYS caused by violating constraint\n"
        "    1 (multi-line) or constraint 3 (using unsupported syntax like\n"
        "    bracket indexing or .push). Re-read those rules before retrying.\n"
        "════════════════════════════════════════════════════════════════════\n"
        "SELF-CHECK TOOLS — USE THESE TO AVOID MISTAKES (cheap, no side effects):\n"
        "  • `lint_expression(expression=..., kind='formula'|'condition'|\n"
        "    'iteration')` — statically validate ONE expression BEFORE putting it\n"
        "    in a step. It runs the exact checks the save path enforces, so what\n"
        "    passes here passes at write time. Use it whenever you are unsure\n"
        "    about a formula's syntax — do NOT save-and-pray.\n"
        "  • `preview_generated_code(rule_id=...)` — see the Python your rule\n"
        "    compiles to, plus any undefined-variable findings, BEFORE dry-run.\n"
        "    The fastest way to find a 'name is not defined' bug is to read the\n"
        "    generated code and check each variable is assigned before use.\n"
        "  • `explain_error(error_text=..., tool_name=...)` — turn a confusing\n"
        "    error into a root-cause + fix recipe. Call it the FIRST time an\n"
        "    error confuses you, not after looping.\n"
        "  • `suggest_field_hints(event_name=...)` — get accounting-sensible\n"
        "    field_hints (rates as decimals, money under $10M, dated fields)\n"
        "    to pass into generate_sample_event_data so sample data is realistic\n"
        "    and rules don't evaluate to zero.\n"
        "  • `revert_rule(rule_id=...)` — undo your last edit to a rule by\n"
        "    restoring its previous saved version. Use this to back out a bad\n"
        "    change cleanly instead of trying to hand-reconstruct prior steps.\n"
        "  RECOMMENDED SELF-CHECK FLOW for authoring a rule:\n"
        "    1. lint_expression on each non-trivial formula as you compose it.\n"
        "    2. create_saved_rule / add_step_to_rule.\n"
        "    3. preview_generated_code to confirm variables resolve.\n"
        "    4. debug_step on each step; test_schedule_step on each schedule.\n"
        "    5. verify_rule_complete → dry_run_template → finish.\n"
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

# Provider error types (and message fragments) that warrant rotating to the
# next fallback candidate rather than dead-ending the whole run.
_RETRYABLE_PROVIDER_ERR_TYPES = {
    "quota_exceeded", "rate_limited", "model_deprecated", "model_premium",
}


def _is_retryable_provider_error(exc: Exception) -> bool:
    """True when a provider failure should trigger fallback to the next
    provider/model candidate (quota exhausted, rate-limited, model unavailable
    on this provider). Duck-types AIError.error_type to avoid a hard import,
    and falls back to message matching for plain exceptions."""
    et = getattr(exc, "error_type", None)
    if et in _RETRYABLE_PROVIDER_ERR_TYPES:
        return True
    m = str(exc).lower()
    return any(k in m for k in (
        "quota", "rate limit", "rate_limit", "not available", "model_not_found",
        "does not exist", "insufficient", "overloaded", "429",
    ))


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
    fallbacks: list[dict] | None = None,   # [{provider, api_key, model}, …]
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
    # Provider/model fallback chain. Each candidate is (provider, api_key,
    # model). On a retryable provider failure (quota, rate-limit, model
    # unavailable) the run rotates to the next candidate and retries the SAME
    # step — so one provider's quota outage no longer kills the whole build.
    # Sticky: once rotated, later steps stay on the working candidate.
    _candidates: list[tuple] = [(provider, api_key, model)]
    for _fb in (fallbacks or []):
        _fp, _fk, _fm = _fb.get("provider"), _fb.get("api_key"), _fb.get("model")
        if _fp is not None and _fk and _fm:
            _candidates.append((_fp, _fk, _fm))
    _cand_idx = 0
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
    # Delta workspace refresh: on long runs the first-turn snapshot goes stale
    # (the agent has since created events / rules / txn types). Every
    # _REFRESH_EVERY steps, if the workspace changed since the last refresh,
    # re-inject a compact snapshot as a USER message (not system — keeps the
    # cached system prefix stable) so the agent plans against current state.
    mutated_since_refresh = False
    last_refresh_step = 0
    # Context compaction: remember how many messages we've collapsed so we only
    # announce a compaction boundary when it grows (analogous to the Agent
    # SDK's compact_boundary signal).
    last_compacted_count = 0
    # Empty-response guard: some models (esp. reasoning models) occasionally
    # return a completion with NO text and NO tool call. That is a transient
    # dropped completion, NOT a genuine "needs your input" pause — nudge and
    # retry once rather than halting the run with an empty summary.
    empty_responses = 0
    # Every material number the agent OBSERVED in a tool result, for the
    # summary-grounding check at the end of the run (anti-hallucination).
    observed_numbers: set[float] = set()

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
        prior_history = await _load_session_history(db, session_id)

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

            # Bound the context sent to the provider on long runs. Compaction
            # is non-destructive — `messages` stays full-fidelity for
            # persistence; only the copy we send is shrunk.
            send_messages, n_compacted = _compact_messages(messages)
            if n_compacted > last_compacted_count:
                last_compacted_count = n_compacted
                yield {"type": "warning", "ts": _now_iso(), "step": step,
                        "message": (
                            "Condensed earlier steps to stay within the model's "
                            "context window (recent steps kept in full)."
                        )}

            resp = None
            _provider_failed = False
            while True:
                cur_provider, cur_api_key, cur_model = _candidates[_cand_idx]
                model = cur_model  # reflect the active candidate in labels/summary
                provider_task = asyncio.create_task(
                    cur_provider.chat_with_tools(
                        messages=send_messages,
                        tools=TOOL_SCHEMAS,
                        model=cur_model,
                        api_key=cur_api_key,
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
                                    "message": f"Waiting for {cur_model}… {elapsed}s"}
                    if _RUN_STATUS.get(run_id) == "cancelled":
                        final_status = "cancelled"
                        final_summary = "Run cancelled by user."
                        _provider_failed = True
                        break
                    resp = provider_task.result()
                    break  # success — leave the candidate loop
                except NotImplementedError:
                    yield {"type": "error",
                            "message": f"Provider does not support tool calling. Use OpenAI or Anthropic."}
                    final_status = "failed"
                    _provider_failed = True
                    break
                except asyncio.CancelledError:
                    final_status = "cancelled"
                    final_summary = "Run cancelled by user."
                    _provider_failed = True
                    break
                except Exception as exc:
                    # Rotate to the next fallback candidate on a retryable
                    # failure (quota/rate-limit/model-unavailable) and retry the
                    # SAME step. Otherwise surface the error and stop.
                    if _is_retryable_provider_error(exc) and _cand_idx + 1 < len(_candidates):
                        _cand_idx += 1
                        _next_model = _candidates[_cand_idx][2]
                        yield {"type": "warning", "ts": _now_iso(), "step": step,
                                "message": (f"{cur_model} unavailable ({exc}). "
                                            f"Falling back to {_next_model}.")}
                        continue
                    logger.exception("Provider call failed")
                    _hint = ("" if len(_candidates) > 1 else
                             " No fallback model is configured — add a second AI "
                             "provider under Settings → AI Agent Setup so builds "
                             "survive a provider outage.")
                    yield {"type": "error", "message": f"Provider error: {exc}.{_hint}"}
                    final_status = "failed"
                    _provider_failed = True
                    break
            if _provider_failed:
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
                if assistant_text:
                    # The model produced a final text answer — done.
                    final_status = "completed"
                    final_summary = assistant_text
                    break
                # Empty response: no text AND no tool call. Almost always a
                # transient dropped completion, not a real pause. Nudge and
                # retry once; only halt (with a clear, plain-English message)
                # if it happens twice in a row.
                empty_responses += 1
                # Make the just-appended assistant message well-formed for
                # replay (content=None + no tool_calls can be rejected).
                messages[-1]["content"] = "(no response)"
                if empty_responses >= 2:
                    final_status = "halted"
                    final_summary = (
                        "I couldn't produce a response for this request. This "
                        "is usually a temporary issue with the AI model. "
                        "Please try again, or rephrase what you'd like me to "
                        "do — and if it keeps happening, switch to another "
                        "model in the dropdown below."
                    )
                    yield {"type": "warning", "ts": _now_iso(), "step": step,
                            "message": final_summary}
                    break
                messages.append({"role": "user", "content": (
                    "You returned an empty response — no message and no "
                    "action. If the task is complete, call finish with a "
                    "short plain-English summary of what you did. Otherwise, "
                    "continue with your next tool call."
                )})
                yield {"type": "warning", "ts": _now_iso(), "step": step,
                        "message": "The model returned an empty response — "
                                   "asking it to continue."}
                continue

            # A real (non-empty) response arrived — reset the empty guard.
            empty_responses = 0

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
                    # Plumb the run_id AND session_id into tool-side context so
                    # dispatch_tool's plan-gate keys the persisted plan by the
                    # stable session id (found again on continuation turns).
                    try:
                        from .tools import (
                            set_current_run_id as _set_rid,
                            set_current_session_id as _set_sid,
                        )
                        _set_rid(run_id)
                        _set_sid(session_id or "")
                    except Exception:
                        pass
                    result = await dispatch_tool(name, args)
                    duration_ms = int((time.time() - t0) * 1000)
                    obs = _truncate_for_observation(result)
                    # Record numbers the agent actually observed, for the
                    # end-of-run summary-grounding check.
                    try:
                        observed_numbers |= _numbers_in_result(result)
                    except Exception:
                        pass
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
                    # Mark the workspace dirty so the next refresh checkpoint
                    # re-grounds the agent against current state.
                    if name in _WORKSPACE_MUTATING_TOOLS:
                        mutated_since_refresh = True
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
                        fsum = ((result or {}).get("summary") or "").strip()
                        if not fsum:
                            # Model called finish without a summary — synthesize
                            # a sensible one so the user never sees a blank result.
                            named = sorted({v for v in touched_rules.values() if v})
                            if named:
                                fsum = ("Done. I finished working on "
                                        + ", ".join(named) + ".")
                            elif touched_rules:
                                fsum = (f"Done. I finished working on "
                                        f"{len(touched_rules)} rule(s).")
                            else:
                                fsum = "Done."
                        final_summary = fsum
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

            # Delta workspace refresh: on long runs, periodically re-ground the
            # agent against current state if it has mutated the workspace since
            # the last refresh. Injected as a USER message so the cached system
            # prefix stays intact. Skipped when finishing.
            if (not should_finish
                    and mutated_since_refresh
                    and (step - last_refresh_step) >= _REFRESH_EVERY):
                try:
                    refreshed = await _build_workspace_context(
                        db=db, in_memory_data=in_memory_data
                    )
                    messages.append({
                        "role": "user",
                        "content": (
                            "WORKSPACE REFRESH — the workspace has changed since "
                            "you started. Use this CURRENT state (do not rely on "
                            "the original snapshot for names/ids/priorities):\n\n"
                            + refreshed
                        ),
                    })
                    mutated_since_refresh = False
                    last_refresh_step = step
                    yield {"type": "warning", "ts": _now_iso(),
                            "message": "Re-grounded agent with a refreshed "
                                       "workspace snapshot."}
                except Exception as exc:
                    logger.warning("Workspace refresh failed: %s", exc)

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

    # Safety net: no terminal path may emit a blank summary (the UI would show
    # an empty "Paused — needs your input" card). Supply a status-appropriate
    # fallback in plain English.
    if not (final_summary or "").strip():
        final_summary = {
            "completed": "Done.",
            "failed": "The AI model ran into a problem finishing this request. "
                      "Please try again, or switch to another model below.",
            "cancelled": "Run cancelled.",
        }.get(final_status,
               "This didn't finish. Please try again, or rephrase your request.")

    # Summary-grounding check (anti-hallucination): flag any material money
    # figure in the summary that the agent never observed in a tool result.
    # Only meaningful on a completed run that actually looked at some numbers.
    ungrounded: list[float] = []
    if final_status == "completed" and observed_numbers:
        try:
            ungrounded = _ungrounded_amounts(final_summary, observed_numbers)
        except Exception:
            ungrounded = []
    if ungrounded:
        yield {
            "type": "warning", "ts": _now_iso(),
            "message": (
                "Some amounts in the summary weren't found in the model's own "
                "computed results and may be inaccurate — please verify: "
                + ", ".join(f"{a:,.2f}" for a in ungrounded[:8])
            ),
            "grounding": {"ungrounded_amounts": ungrounded[:20]},
        }

    final_event = {
        "type": "final", "ts": _now_iso(), "run_id": run_id,
        "status": final_status, "summary": final_summary, "steps": steps_used,
    }
    if ungrounded:
        final_event["ungrounded_amounts"] = ungrounded[:20]
    yield final_event

    # Persist this turn's messages back into the per-session history so the
    # next user turn in the same chat sees them. We persist regardless of
    # final_status — even a partial/failed turn left useful tool observations
    # the next turn should not have to redo (e.g. event listings).
    if session_id:
        try:
            new_msgs = messages[new_msg_start_idx:]
            updated = list(prior_history) + new_msgs
            await _save_session_history(db, session_id, updated)
        except Exception as exc:
            logger.warning("Could not persist session history: %s", exc)

    # Persist run record (best-effort)
    run_doc = {
        "run_id": run_id, "task": task, "model": model,
        "started_at": started_at, "finished_at": _now_iso(),
        "status": final_status, "summary": final_summary,
        "steps": steps_used, "history": history,
        "ungrounded_amounts": ungrounded[:20],
    }
    await _save_run(db, in_memory_data, run_doc)
