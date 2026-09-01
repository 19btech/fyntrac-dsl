"""Tests for the Phase 4 self-correctness tools added to the agent:
lint_expression, explain_error, suggest_field_hints, plus registry/schema
parity for preview_generated_code and revert_rule.

These run in in-memory mode (no Mongo required), so they cover the DB-free
tools. preview_generated_code / revert_rule need a real DB (rule persistence)
and are exercised via the registry-parity check here + the live agent flow.

Run:  python tests/test_agent_new_tools.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


async def main():
    # Force in-memory mode by pointing at an unreachable Mongo.
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test_new_tools"

    import server  # noqa: F401  — registers the agent bridge + helpers

    from agent.tools import (
        TOOLS, TOOL_SCHEMAS, DESTRUCTIVE_TOOLS, PLAN_GATED_TOOLS,
        tool_lint_expression, tool_explain_error, tool_suggest_field_hints,
        tool_create_event_definitions,
    )

    failures = []

    def check(cond, msg):
        if cond:
            print(f"  ok   — {msg}")
        else:
            print(f"  FAIL — {msg}")
            failures.append(msg)

    # ── Registry / schema parity ──────────────────────────────────────────
    new_tools = {
        "lint_expression", "preview_generated_code", "explain_error",
        "suggest_field_hints", "revert_rule",
    }
    schema_names = {t["name"] for t in TOOL_SCHEMAS}
    check(new_tools <= set(TOOLS), "all new tools registered in TOOLS")
    check(new_tools <= schema_names, "all new tools have schemas")
    check(schema_names == set(TOOLS), "schema/registry parity (no orphans)")
    check(not (new_tools & DESTRUCTIVE_TOOLS),
          "new tools are not flagged destructive")
    check(not (new_tools & PLAN_GATED_TOOLS),
          "new tools are not plan-gated (read-only self-check)")

    # ── lint_expression: good expression passes ───────────────────────────
    r = await tool_lint_expression({"expression": "multiply(principal, rate)", "kind": "formula"})
    check(r["ok"] is True, "lint passes a valid formula")

    # ── lint_expression: multi-line iteration expression rejected ─────────
    r = await tool_lint_expression({"expression": "let x = 1\nmultiply(x, 2)", "kind": "iteration"})
    check(r["ok"] is False and r["errors"], "lint rejects multi-line iteration expression")

    # ── lint_expression: bracket indexing rejected ────────────────────────
    r = await tool_lint_expression({"expression": "arr[0]", "kind": "formula"})
    check(r["ok"] is False, "lint rejects bracket indexing arr[0]")

    # ── lint_expression: curly braces rejected ────────────────────────────
    r = await tool_lint_expression({"expression": "if(cond, {a: 1}, 0)", "kind": "formula"})
    check(r["ok"] is False, "lint rejects curly braces")

    # ── lint_expression: unknown function rejected ────────────────────────
    r = await tool_lint_expression({"expression": "frobnicate(principal)", "kind": "formula"})
    check(r["ok"] is False, "lint rejects unknown function")

    # ── lint_expression: JS boolean coerced, then passes ──────────────────
    r = await tool_lint_expression({"expression": "if(is_active, principal, 0)", "kind": "formula"})
    check(r["ok"] is True, "lint passes plain boolean field reference")
    r = await tool_lint_expression({"expression": "eq(flag, true)", "kind": "formula"})
    check(r.get("coerced") == "eq(flag, True)", "lint surfaces JS->Python boolean coercion")

    # ── lint_expression: bad args raises ──────────────────────────────────
    try:
        await tool_lint_expression({"expression": ""})
        check(False, "empty expression should raise ToolError")
    except Exception:
        check(True, "empty expression raises")

    # ── explain_error: maps a known signature ─────────────────────────────
    r = await tool_explain_error({
        "error_text": "SyntaxError: unterminated string literal (detected at line 1)",
        "tool_name": "add_step_to_rule",
    })
    check(r["error_signature"] == "unterminated_string_literal",
          "explain_error classifies unterminated string literal")
    check(bool(r["guidance"]), "explain_error returns guidance text")

    # ── explain_error: bad args raises ────────────────────────────────────
    try:
        await tool_explain_error({})
        check(False, "missing error_text should raise")
    except Exception:
        check(True, "explain_error requires error_text")

    # ── suggest_field_hints: needs an event def (in-memory) ───────────────
    await tool_create_event_definitions({
        "events": [{
            "event_name": "HintLoan", "eventType": "activity", "eventTable": "standard",
            "fields": [
                {"name": "principal", "datatype": "decimal"},
                {"name": "interest_rate", "datatype": "decimal"},
                {"name": "useful_life_months", "datatype": "integer"},
                {"name": "acquisition_date", "datatype": "date"},
                {"name": "instrumentid", "datatype": "string"},
            ],
        }],
    })
    r = await tool_suggest_field_hints({"event_name": "HintLoan"})
    hints = r["field_hints"]
    check(hints.get("interest_rate", {}).get("range") == [0.01, 0.15],
          "rate field -> decimal range [0.01, 0.15]")
    check(hints.get("principal", {}).get("range") == [1000, 1000000],
          "money field -> range under $10M")
    check(hints.get("useful_life_months", {}).get("range") == [12, 360],
          "months field -> integer tenor range [12, 360]")
    check("acquisition_date" in r["date_fields_auto_handled"]
          and "acquisition_date" not in hints,
          "date field -> auto-handled (no range emitted)")
    check("instrumentid" not in hints,
          "identity field instrumentid is skipped (not hinted)")

    # ── suggest_field_hints: fractional rate range NOT emitted on int field ─
    await tool_create_event_definitions({
        "events": [{
            "event_name": "HintIntRate", "eventType": "activity", "eventTable": "standard",
            "fields": [{"name": "score_rate", "datatype": "integer"}],
        }],
    })
    r2 = await tool_suggest_field_hints({"event_name": "HintIntRate"})
    check("score_rate" not in r2["field_hints"]
          and "score_rate" in r2["unmapped_fields"],
          "fractional rate range skipped on integer-typed field (no randint crash)")

    # ── suggest_field_hints: unknown event raises ─────────────────────────
    try:
        await tool_suggest_field_hints({"event_name": "DoesNotExist"})
        check(False, "unknown event should raise")
    except Exception:
        check(True, "suggest_field_hints raises on unknown event")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s) failed")
        sys.exit(1)
    print("OK — all new-tool checks passed")


if __name__ == "__main__":
    asyncio.run(main())
