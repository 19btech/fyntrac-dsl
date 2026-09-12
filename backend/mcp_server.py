"""MCP server — drive the Fyntrac agent from a personal Claude client (HYBRID).

Two ways to build, exposed side by side:

  1. LOW-LEVEL builder tools (create_saved_rule, add_step_to_rule, dry_run_rule,
     …) — a curated slice of the app's real tools. These are plain
     CRUD+validation with NO LLM inside, so YOUR Claude client drives the build
     with its own reasoning. Nothing runs on the app's configured AI provider,
     so the app's provider quota (e.g. Gemini) can never block you. The tools'
     own write-time validation still applies (unknown functions rejected, no
     debit/credit sides, persistence verified, …).

  2. run_agent_task — the high-level, DELEGATED path: hand a plain-English
     request to the app's OWN autonomous runtime (full plan→build→test→verify
     loop with every guardrail). This one runs on the app's configured provider.

Plus read tools (list_events, list_saved_rules, …) come along in the curated
set for inspection.

It reuses the app's stored provider/key (only run_agent_task needs it) and
connects to the SAME MongoDB as the running app.

  ── Scope note ─────────────────────────────────────────────────────────────
  Personal / developer convenience — you driving your own dev instance. NOT a
  way to ship Claude to end users. Destructive tools (delete rule/template,
  clear data) are intentionally NOT exposed here.

Run (stdio, for Claude Desktop):
    python C:\\path\\to\\backend\\mcp_server.py
"""

from __future__ import annotations

import json
import os
import sys

# ── Make the repo importable ────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ── Import the app (reuses its DB, providers, key handling, agent bridge) ────
# CRITICAL: importing the app prints a banner to STDOUT, but stdio MCP uses
# stdout as its JSON-RPC channel. Redirect stdout→stderr during import.
_real_stdout = sys.stdout
sys.stdout = sys.stderr
try:
    import backend.server as app            # configures the agent bridge on import
    from backend.agent.tools import (
        dispatch_tool, ToolError, TOOL_SCHEMAS, DESTRUCTIVE_TOOLS,
        set_current_run_id, set_current_session_id,
    )
    from backend.agent.runtime import run_agent
finally:
    sys.stdout = _real_stdout

import mcp.types as types                    # noqa: E402
from mcp.server.lowlevel import Server       # noqa: E402
from mcp.server.stdio import stdio_server    # noqa: E402

# Stable session id → multi-turn continuity + a single plan-gate context.
_SESSION_ID = "claude-desktop-mcp"


# ──────────────────────────────────────────────────────────────────────────
# Curated low-level builder tools Claude may drive directly. Ordered
# discovery → plan → build → test/verify → template. Destructive tools are
# deliberately excluded (see _EXPOSED filter below).
# ──────────────────────────────────────────────────────────────────────────
_BUILDER_TOOLS = [
    # discovery
    "list_events", "get_event_data", "list_dsl_functions", "list_saved_rules",
    "get_saved_rule", "list_templates", "list_saved_schedules",
    "get_dsl_syntax_guide", "find_similar_template", "list_canonical_patterns",
    "get_canonical_pattern",
    # plan
    "submit_plan",
    # build
    "create_event_definitions", "add_transaction_types",
    "generate_sample_event_data", "insert_event_rows",
    "create_saved_rule", "update_saved_rule",
    "add_step_to_rule", "update_step", "delete_step", "patch_step",
    "replace_schedule_column", "add_transaction_to_rule",
    "update_transaction_in_rule", "delete_transaction_from_rule",
    "create_saved_schedule", "apply_canonical_pattern",
    # test / verify / fix
    "lint_expression", "preview_generated_code", "validate_dsl", "debug_step",
    "debug_schedule", "test_schedule_step", "dry_run_rule",
    "verify_rule_complete", "explain_error", "suggest_field_hints",
    "auto_pair_arrays", "revert_rule",
    # template
    "create_or_replace_template", "attach_rules_to_template", "dry_run_template",
    # Excel workbook import (upload happens in the app; analysis/import here)
    "list_workbooks", "get_workbook_overview", "set_workbook_sheet_roles",
    "get_sheet_data", "get_sheet_formulas", "trace_workbook_dependencies",
    "import_workbook_inputs", "reconcile_workbook_outputs",
    # Requirement documents (PDF/Word uploaded in the app)
    "list_requirement_documents", "read_requirement_document",
]

_SCHEMA_BY_NAME = {s["name"]: s for s in TOOL_SCHEMAS}
# Keep only tools that actually exist and are NOT destructive.
_EXPOSED = [_SCHEMA_BY_NAME[n] for n in _BUILDER_TOOLS
            if n in _SCHEMA_BY_NAME and n not in DESTRUCTIVE_TOOLS]
_EXPOSED_NAMES = {s["name"] for s in _EXPOSED}

_RUN_AGENT_TASK_TOOL = {
    "name": "run_agent_task",
    "description": (
        "DELEGATED build: hand a plain-English request to the app's OWN "
        "autonomous agent (full plan→build→test→verify loop). ASYNC: a full "
        "build can take minutes, so this starts the run in the background and "
        "returns a run_token immediately (with a short grace period so fast "
        "builds and instant errors come back inline). Poll "
        "get_agent_task_status(run_token) until it reports COMPLETED / HALTED / "
        "FAILED. Runs on the APP's configured AI provider and can be blocked by "
        "that provider's quota. To build with YOUR OWN reasoning and never touch "
        "the app's provider, use the individual builder tools instead "
        "(create_saved_rule, add_step_to_rule, dry_run_rule, …)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "task": {"type": "string", "description": "What to build or change, in plain English."},
            "model": {"type": "string", "description": "Optional model id override; blank uses the app default."},
            "allow_destructive": {"type": "boolean", "default": False,
                                  "description": "Allow mid-run delete/clear steps (default false)."},
            "max_steps": {"type": "integer", "default": 60},
        },
        "required": ["task"],
    },
}

_GET_AGENT_TASK_STATUS_TOOL = {
    "name": "get_agent_task_status",
    "description": (
        "Check the status/result of a delegated build started by run_agent_task. "
        "Because run_agent_task returns before a long build finishes, poll this "
        "with the run_token until it reports a terminal status "
        "(COMPLETED / HALTED / CANCELLED / FAILED). While running it shows the "
        "tools used so far. Omit run_token if exactly one build is active."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "run_token": {"type": "string",
                          "description": "Token returned by run_agent_task. Optional if only one build is active."},
        },
    },
}

server = Server("fyntrac-dsl")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _fmt(result) -> str:
    try:
        s = json.dumps(result, indent=2, default=str, ensure_ascii=False)
    except Exception:
        s = str(result)
    return s if len(s) <= 12000 else s[:12000] + "\n… (truncated)"


async def _resolve_run_config(model_override: str = ""):
    """Provider + decrypted key + model for the DELEGATED path (run_agent_task)."""
    try:
        cfg = await app.db.ai_provider_config.find_one({}, {"_id": 0})
    except Exception as exc:
        raise RuntimeError(
            "Couldn't reach the app's database. Make sure MongoDB is running "
            f"and the app is configured. ({exc})") from exc
    if not cfg:
        raise RuntimeError(
            "No AI provider is configured yet. Open the app → Settings → "
            "AI Agent Setup, add a provider and API key, then try again.")
    provider_name = cfg.get("provider", "")
    model = (model_override or "").strip() or cfg.get("selected_model", "")
    try:
        api_key = app.decrypt_key(cfg["encrypted_api_key"])
    except Exception as exc:
        raise RuntimeError(
            "The stored API key could not be read. Re-enter it in the app "
            "under Settings → AI Agent Setup.") from exc
    try:
        provider = app.get_provider(provider_name)
    except Exception as exc:
        raise RuntimeError(f"Unknown AI provider '{provider_name}': {exc}") from exc
    return provider, api_key, model, provider_name


import asyncio          # noqa: E402
import time             # noqa: E402
import uuid             # noqa: E402

# Background agent runs keyed by a short token. run_agent_task starts a run in
# the server's event loop and returns immediately; get_agent_task_status polls
# it. This is what fixes the MCP-layer timeout: a full build can take minutes,
# far longer than a single Claude Desktop tool call is allowed to block.
_AGENT_TASKS: dict[str, dict] = {}


def _format_task_status(token: str, st: dict) -> str:
    from collections import Counter
    run_state = st.get("run_state")
    final = st.get("final") or {}
    tools_done = st.get("tools_done") or []
    errors = st.get("errors") or []
    model_id = st.get("model_id") or "?"
    counts = Counter(t for t in tools_done if t)
    tool_note = ", ".join(f"{n}×{k}" if n > 1 else k
                          for k, n in counts.most_common(8)) or "no tool calls yet"

    if run_state == "running":
        elapsed = int(time.time() - st.get("started_at", time.time()))
        lines = [f"⏳ RUNNING ({elapsed}s) — model: {model_id}",
                 f"run_token: {token}",
                 f"progress · tools: {tool_note}"]
        if errors:
            lines += ["", "Issues so far:"] + [f"  • {e}" for e in errors[-4:]]
        lines += ["", f'Still working — call get_agent_task_status(run_token="{token}") again to check.']
        return "\n".join(lines)

    # Terminal (done / failed).
    status = final.get("status") or ("failed" if run_state == "failed" else "unknown")
    summary = (final.get("summary") or "").strip()
    steps = final.get("steps")
    icon = {"completed": "✅", "halted": "⏸️", "cancelled": "🛑",
            "failed": "❌", "error": "❌"}.get(status, "•")
    lines = [f"{icon} {status.upper()} — model: {model_id}  (run_token: {token})"]
    if steps is not None:
        lines.append(f"{steps} steps · tools: {tool_note}")
    if st.get("start_error"):
        lines += ["", st["start_error"]]
    if summary:
        lines += ["", summary]
    if errors:
        lines += ["", "Issues encountered:"] + [f"  • {e}" for e in errors[:6]]
    return "\n".join(lines)


async def _drive_agent_task(token: str, args: dict) -> None:
    st = _AGENT_TASKS[token]
    task = (args.get("task") or "").strip()
    try:
        provider, api_key, model_id, provider_name = await _resolve_run_config(
            args.get("model") or "")
        st["model_id"] = model_id or provider_name
    except RuntimeError as exc:
        st["run_state"] = "failed"
        st["start_error"] = str(exc)
        return
    try:
        async for ev in run_agent(
            task=task, provider=provider, api_key=api_key, model=model_id,
            db=app.db, in_memory_data=app.in_memory_data,
            max_steps=int(args.get("max_steps") or 60),
            auto_approve_destructive=bool(args.get("allow_destructive")),
            # No approval UI exists on this connector — without a short timeout
            # a destructive step would block on _wait_for_approval for its
            # 600 s default and hang the run.
            approval_timeout=10.0,
            session_id=_SESSION_ID,
        ):
            t = ev.get("type")
            if t == "tool_done":
                st["tools_done"].append(ev.get("name", ""))
            elif t == "tool_error":
                st["errors"].append(f"{ev.get('name', '')}: {ev.get('error', '')}")
            elif t == "final":
                st["final"] = ev
                st["model_id"] = ev.get("model") or st.get("model_id")
            elif t == "error":
                st["errors"].append(ev.get("error_message") or ev.get("message") or "error")
        st["run_state"] = "done"
    except Exception as exc:
        st["run_state"] = "failed"
        st["errors"].append(f"run failed: {exc}")


async def _run_agent_task(args: dict) -> str:
    task = (args.get("task") or "").strip()
    if not task:
        return "Please provide a task describing what to build or change."
    token = uuid.uuid4().hex[:12]
    _AGENT_TASKS[token] = {
        "run_state": "running", "tools_done": [], "errors": [],
        "final": None, "model_id": "", "started_at": time.time(), "task": task,
    }
    bg = asyncio.create_task(_drive_agent_task(token, dict(args)))
    _AGENT_TASKS[token]["_task"] = bg
    # Short grace period so instant config errors and very fast builds return
    # inline; anything longer is handed back as a run_token to poll — the run
    # keeps going in the server's event loop across tool calls.
    try:
        await asyncio.wait_for(asyncio.shield(bg), timeout=6.0)
    except (asyncio.TimeoutError, Exception):
        pass
    return _format_task_status(token, _AGENT_TASKS[token])


async def _get_agent_task_status(args: dict) -> str:
    token = (args.get("run_token") or "").strip()
    if not token:
        if len(_AGENT_TASKS) == 1:
            token = next(iter(_AGENT_TASKS))
        else:
            active = ", ".join(_AGENT_TASKS) or "none"
            return f"run_token is required. Known tasks: {active}"
    st = _AGENT_TASKS.get(token)
    if not st:
        active = ", ".join(_AGENT_TASKS) or "none"
        return f"No agent task with run_token '{token}'. Known tasks: {active}"
    return _format_task_status(token, st)


# ── MCP handlers ────────────────────────────────────────────────────────────

@server.list_tools()
async def _list_tools() -> list[types.Tool]:
    tools = [
        types.Tool(
            name=_RUN_AGENT_TASK_TOOL["name"],
            description=_RUN_AGENT_TASK_TOOL["description"],
            inputSchema=_RUN_AGENT_TASK_TOOL["parameters"],
        ),
        types.Tool(
            name=_GET_AGENT_TASK_STATUS_TOOL["name"],
            description=_GET_AGENT_TASK_STATUS_TOOL["description"],
            inputSchema=_GET_AGENT_TASK_STATUS_TOOL["parameters"],
        ),
    ]
    for s in _EXPOSED:
        tools.append(types.Tool(
            name=s["name"],
            description=s.get("description", ""),
            inputSchema=s.get("parameters") or {"type": "object", "properties": {}},
        ))
    return tools


@server.call_tool()
async def _call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    arguments = arguments or {}
    if name == "run_agent_task":
        return [types.TextContent(type="text", text=await _run_agent_task(arguments))]
    if name == "get_agent_task_status":
        return [types.TextContent(type="text", text=await _get_agent_task_status(arguments))]
    if name not in _EXPOSED_NAMES:
        return [types.TextContent(
            type="text",
            text=(f"Tool '{name}' is not available on this connector. "
                  f"Available builder tools: {sorted(_EXPOSED_NAMES)}"))]
    # Route to the app's real tool. A stable session context makes the
    # plan-gate (submit_plan) behave consistently across calls.
    set_current_run_id(_SESSION_ID)
    set_current_session_id(_SESSION_ID)
    try:
        result = await dispatch_tool(name, arguments)
        return [types.TextContent(type="text", text=_fmt(result))]
    except ToolError as exc:
        return [types.TextContent(type="text", text=f"Tool error: {exc}")]
    except Exception as exc:                                  # pragma: no cover
        # Don't leak raw driver stack dumps (pymongo topology descriptions...)
        # into the chat — translate the common case to something actionable.
        msg = str(exc)
        if any(k in msg for k in ("refused", "ServerSelectionTimeout",
                                  "Topology", "AutoReconnect")):
            msg = ("Couldn't reach the app's database. Make sure MongoDB is "
                   "running (this project uses mongodb://localhost:27018 via "
                   "the dsl-mongo container) and try again.")
        return [types.TextContent(type="text", text=f"Unexpected error: {msg}")]


async def _main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> None:
    import asyncio
    asyncio.run(_main())


if __name__ == "__main__":
    main()
