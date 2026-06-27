"""Conditional schedules: runIf gating + frequencyFormula.

Verifies the codegen emits a period that collapses to zero rows when runIf is
false, emits a dynamic (unquoted) frequency when frequencyFormula is set, leaves
ordinary schedules unchanged, and that validation lints runIf/frequencyFormula.

Run:  python tests/test_agent_conditional_schedule.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test_cond_sched"
    import server  # noqa: F401 — registers helpers used by codegen/validation
    from agent import tools
    from agent.tools import ToolError

    failures = []

    def check(cond, msg):
        print(f"  {'ok  ' if cond else 'FAIL'} — {msg}")
        if not cond:
            failures.append(msg)

    def gen(step):
        vstep = tools._validate_step_shape(step)
        rule = {"name": "R", "steps": [vstep], "outputs": {"transactions": []}}
        return tools._generate_rule_code(rule)

    base_cols = [{"name": "charge", "formula": "multiply(opening, rate)"}]

    # ── number mode + runIf → period count gated to 0 when false ──────────
    code = gen({
        "name": "Sched", "stepType": "schedule",
        "scheduleConfig": {
            "periodType": "number", "periodCountSource": "value",
            "periodCount": 12, "frequency": "M", "runIf": "is_lease",
            "columns": base_cols,
        },
        "outputVars": [{"name": "total", "type": "sum", "column": "charge"}],
    })
    check("if(is_lease, 12, 0)" in code,
          "number mode: runIf gates the period count (->0 when false)")

    # ── date mode + runIf → end date gated to "" when false ───────────────
    code = gen({
        "name": "Sched", "stepType": "schedule",
        "scheduleConfig": {
            "periodType": "date", "frequency": "M", "runIf": "not(is_lease)",
            "startDateSource": "formula", "startDateFormula": "postingdate",
            "endDateSource": "formula", "endDateFormula": "lease_end",
            "columns": base_cols,
        },
        "outputVars": [{"name": "total", "type": "sum", "column": "charge"}],
    })
    check('if(not(is_lease), lease_end, "")' in code,
          'date mode: runIf gates the end date (-> empty when false)')

    # ── frequencyFormula → emitted UNQUOTED in the period() call ──────────
    code = gen({
        "name": "Sched", "stepType": "schedule",
        "scheduleConfig": {
            "periodType": "date", "frequencyFormula": 'if(report_monthly, "M", "Q")',
            "startDateSource": "formula", "startDateFormula": "postingdate",
            "endDateSource": "formula", "endDateFormula": "end_d",
            "columns": base_cols,
        },
        "outputVars": [{"name": "total", "type": "sum", "column": "charge"}],
    })
    check('if(report_monthly, "M", "Q")' in code and '"if(report_monthly' not in code,
          "frequencyFormula emitted unquoted (dynamic frequency)")

    # ── ordinary schedule (no runIf) unchanged: quoted freq, no if() gate ──
    code = gen({
        "name": "Sched", "stepType": "schedule",
        "scheduleConfig": {
            "periodType": "number", "periodCountSource": "value",
            "periodCount": 6, "frequency": "Q", "columns": base_cols,
        },
        "outputVars": [{"name": "total", "type": "sum", "column": "charge"}],
    })
    check('period(6, "Q")' in code,
          "plain schedule unchanged (quoted frequency, no runIf gate)")

    # ── validation rejects an unknown function inside runIf ────────────────
    try:
        tools._validate_step_shape({
            "name": "Sched", "stepType": "schedule",
            "scheduleConfig": {
                "periodType": "number", "periodCountSource": "value",
                "periodCount": 6, "frequency": "M",
                "runIf": "frobnicate(x)", "columns": base_cols,
            },
        })
        check(False, "bad runIf should raise ToolError")
    except ToolError:
        check(True, "validation rejects unknown function in runIf")

    # ── valid runIf with a known function passes ──────────────────────────
    try:
        tools._validate_step_shape({
            "name": "Sched", "stepType": "schedule",
            "scheduleConfig": {
                "periodType": "number", "periodCountSource": "value",
                "periodCount": 6, "frequency": "M",
                "runIf": "gt(days_past_due, 30)", "columns": base_cols,
            },
        })
        check(True, "valid runIf (gt(...)) passes validation")
    except ToolError as e:
        check(False, f"valid runIf wrongly rejected: {e}")

    # ── runtime chain: gated (empty) period really yields zero rows ───────
    from dsl_functions import period, schedule, schedule_sum, schedule_last
    p_off = period(0, "M")                       # runIf false -> count 0
    s_off = schedule(p_off, {"charge": "100"})
    check(len(s_off) == 0, "empty period -> schedule has zero rows")
    check(schedule_sum(s_off, "charge") == 0,
          "schedule_sum on empty schedule -> 0 (outputVar safe when runIf false)")
    check(schedule_last(s_off, "charge") == 0,
          "schedule_last on empty schedule -> 0 (scalar, consistent with schedule_sum)")
    p_on = period(3, "M")                        # runIf true -> 3 periods
    s_on = schedule(p_on, {"charge": "100"})
    check(len(s_on) == 3, "non-empty period -> schedule materialises rows")

    # date mode empty: end before/empty -> zero rows
    s_date_off = schedule(period("2026-01-01", "", "M"), {"charge": "100"})
    check(len(s_date_off) == 0, 'date mode with empty end -> zero rows')

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s)")
        sys.exit(1)
    print("OK — conditional-schedule checks passed")


if __name__ == "__main__":
    main()
