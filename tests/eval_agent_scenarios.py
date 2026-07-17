"""Golden-scenario eval harness — runs the REAL LLM agent against a battery of
canonical accounting briefs and asserts the workspace it builds is correct:
rules exist, schedules exist where required, transactions are declared in
balanced debit/credit pairs, and a dry-run emits balanced, non-zero output.

This is the regression net for prompt/tool changes: every edit to
runtime._system_prompt(), the tool schemas, or the validators should be
followed by a run of this harness before it ships.

Requirements (opt-in — NOT part of the unit-test suite):
  * A reachable MongoDB (default mongodb://localhost:27017). The harness
    creates a throwaway database per run and drops it afterwards.
  * An API key for the chosen provider:
      AGENT_EVAL_API_KEY, or ANTHROPIC_API_KEY / OPENAI_API_KEY.

Usage:
  python tests/eval_agent_scenarios.py --list
  python tests/eval_agent_scenarios.py                       # all scenarios
  python tests/eval_agent_scenarios.py --scenario ias16_depreciation
  python tests/eval_agent_scenarios.py --provider anthropic --model claude-opus-4-8

Env overrides: AGENT_EVAL_PROVIDER, AGENT_EVAL_MODEL, AGENT_EVAL_MAX_STEPS,
MONGO_URL. Results are printed and written to tests/eval_results/.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

DEFAULT_MODELS = {
    "anthropic": "claude-opus-4-8",
    "openai": "gpt-4o",
}

# Collections wiped between scenarios so runs are independent.
_WORKSPACE_COLLECTIONS = (
    "event_definitions", "event_data", "transaction_reports",
    "custom_functions", "saved_rules", "saved_schedules",
    "transaction_definitions", "dsl_templates", "user_templates",
    "agent_plans", "agent_sessions", "agent_runs",
)

# ──────────────────────────────────────────────────────────────────────────
# Scenario battery. Each brief is a realistic self-contained user request;
# `expect` drives the post-run assertions.
# ──────────────────────────────────────────────────────────────────────────

SCENARIOS: list[dict] = [
    {
        "id": "ias16_depreciation",
        "brief": (
            "Build an IAS 16 straight-line depreciation model for fixed assets. "
            "Create the asset event, generate sample data for 5 assets with 12 "
            "monthly posting dates, build the rule with a monthly depreciation "
            "schedule, emit balanced depreciation transactions, assemble the "
            "template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1,
                   "txn_keywords": ["depreciation"]},
    },
    {
        "id": "ifrs9_ecl_staging",
        "brief": (
            "Build an IFRS 9 expected credit loss model: assign each loan to "
            "stage 1, 2 or 3 based on days past due and a credit-impaired flag, "
            "compute 12-month ECL for stage 1 and lifetime ECL for stages 2 and "
            "3 as PD x LGD x EAD, and book the ECL allowance with balanced "
            "transactions. Create the events, generate realistic sample data, "
            "build the rules and template, and dry-run end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1,
                   "txn_keywords": ["ecl"]},
    },
    {
        "id": "asc606_revenue_recognition",
        "brief": (
            "Build an ASC 606 revenue recognition model for annual SaaS "
            "subscription contracts recognised rateably over 12 months. Create "
            "the contract event, generate sample data, build a rule with a "
            "monthly recognition schedule, emit balanced revenue transactions "
            "(deferred revenue release against recognised revenue), assemble "
            "the template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1,
                   "txn_keywords": ["revenue"]},
    },
    {
        "id": "asc842_lease",
        "brief": (
            "Build an ASC 842 operating lease model: right-of-use asset "
            "amortisation and lease liability interest unwinding at the "
            "incremental borrowing rate over the lease term. Create the lease "
            "event, generate sample data, build the rule with an amortisation "
            "schedule, emit balanced transactions for both legs, assemble the "
            "template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1,
                   "txn_keywords": ["lease", "rou", "amort"]},
    },
    {
        "id": "eir_interest_accrual",
        "brief": (
            "Build an amortised-cost interest accrual model using the effective "
            "interest rate method for a portfolio of fixed-rate loans. Create "
            "the loan event, generate sample data with 12 monthly posting "
            "dates, build the rule with an EIR amortisation schedule, emit a "
            "balanced interest income / interest receivable pair, assemble the "
            "template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1,
                   "txn_keywords": ["interest"]},
    },
    {
        "id": "ias21_fx_remeasurement",
        "brief": (
            "Build an IAS 21 FX remeasurement model: remeasure foreign-currency "
            "loan balances to the functional currency at the closing rate each "
            "period and book the unrealised FX gain or loss with balanced "
            "transactions. Create the balance event and an FX-rate reference "
            "table, generate sample data, build the rule and template, and "
            "dry-run end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1,
                   "txn_keywords": ["fx", "exchange", "currency"]},
    },
    {
        "id": "ias37_provision_unwind",
        "brief": (
            "Build an IAS 37 provision model for a decommissioning obligation: "
            "discount the future outflow to present value and unwind the "
            "discount as a periodic accretion charge over the life of the "
            "obligation using a schedule. Create the provision event, generate "
            "sample data, emit balanced accretion transactions, assemble the "
            "template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1,
                   "txn_keywords": ["provision", "accretion", "unwind"]},
    },
    {
        "id": "cecl_pool_allowance",
        "brief": (
            "Build a US GAAP CECL pool-based allowance model: group loans into "
            "pools by risk rating, apply a lifetime loss rate per pool to the "
            "amortised cost basis, and book the allowance for credit losses "
            "with balanced transactions. Create the events, generate sample "
            "data, build the rules and template, and dry-run end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1,
                   "txn_keywords": ["allowance", "cecl", "credit loss"]},
    },
]


# ──────────────────────────────────────────────────────────────────────────
# Harness
# ──────────────────────────────────────────────────────────────────────────

def _resolve_provider_config(args) -> tuple[str, str, str]:
    provider_name = (args.provider or os.environ.get("AGENT_EVAL_PROVIDER")
                     or "anthropic").lower()
    key = (os.environ.get("AGENT_EVAL_API_KEY")
           or os.environ.get({"anthropic": "ANTHROPIC_API_KEY",
                              "openai": "OPENAI_API_KEY"}.get(provider_name, ""), ""))
    model = (args.model or os.environ.get("AGENT_EVAL_MODEL")
             or DEFAULT_MODELS.get(provider_name, ""))
    if not key:
        sys.exit(f"No API key found for provider '{provider_name}'. Set "
                 f"AGENT_EVAL_API_KEY or the provider's own env var.")
    if not model:
        sys.exit(f"No default model for provider '{provider_name}' — pass --model.")
    return provider_name, key, model


async def _wipe_workspace(db, eval_db_name: str) -> None:
    # Hard safety guard: never wipe anything but the throwaway eval database.
    # Protects against config changes that silently ignore the DB_NAME
    # override and point `server.db` at a real dev/prod database.
    if db.name != eval_db_name:
        raise RuntimeError(
            f"Refusing to wipe database '{db.name}' — expected the eval "
            f"database '{eval_db_name}'. The DB_NAME override did not take "
            f"effect; aborting before touching any data.")
    for col in _WORKSPACE_COLLECTIONS:
        await db[col].delete_many({})


async def _run_scenario(scenario: dict, *, server, provider, api_key: str,
                        model: str, max_steps: int, eval_db_name: str) -> dict:
    from agent.runtime import run_agent
    from agent import tools as agent_tools

    db = server.db
    await _wipe_workspace(db, eval_db_name)

    started = time.time()
    events: list[dict] = []
    final = {"status": "halted", "summary": "(no final event)"}
    tool_errors: list[str] = []

    async for ev in run_agent(
        task=scenario["brief"],
        provider=provider, api_key=api_key, model=model,
        db=db, in_memory_data=server.in_memory_data,
        max_steps=max_steps,
        approval_timeout=15.0,          # evals never approve destructive tools
        session_id=f"eval-{scenario['id']}-{uuid.uuid4().hex[:8]}",
    ):
        events.append({k: v for k, v in ev.items() if k != "result"})
        if ev["type"] == "tool_error":
            tool_errors.append(f"{ev.get('name')}: {str(ev.get('error'))[:200]}")
        if ev["type"] == "final":
            final = ev

    duration_s = round(time.time() - started, 1)
    expect = scenario["expect"]
    failures: list[str] = []
    warnings: list[str] = []

    # 1. Run completed
    if final.get("status") != "completed":
        failures.append(f"final status = {final.get('status')!r} "
                        f"(summary: {str(final.get('summary'))[:200]})")

    # 2. Rules exist, with steps and balanced transaction declarations
    rules = await db.saved_rules.find({}, {"_id": 0}).to_list(100)
    if len(rules) < expect.get("min_rules", 1):
        failures.append(f"expected >= {expect.get('min_rules', 1)} saved rule(s), "
                        f"found {len(rules)}")
    any_schedule = False
    for r in rules:
        steps = r.get("steps") or []
        if any((s.get("stepType") or "") == "schedule" for s in steps):
            any_schedule = True
        txns = ((r.get("outputs") or {}).get("transactions") or [])
        sides = {(t.get("side") or "").lower() for t in txns}
        if not txns:
            failures.append(f"rule '{r.get('name')}' declares no transactions")
        elif not ({"debit", "credit"} <= sides):
            failures.append(f"rule '{r.get('name')}' transaction sides "
                            f"unbalanced: {sorted(sides)}")

    # 3. Schedule expectation (set-level, mirrors the finish gate)
    if expect.get("schedule_step") and not any_schedule:
        failures.append("expected at least one stepType='schedule' step "
                        "across the rules, found none")

    # 4. Transaction-type keyword coverage
    kw = [k.lower() for k in expect.get("txn_keywords", [])]
    if kw:
        registered = [d.get("transactiontype", "")
                      for d in await db.transaction_definitions.find(
                          {}, {"_id": 0, "transactiontype": 1}).to_list(200)]
        joined = " ".join(registered).lower()
        if not any(k in joined for k in kw):
            failures.append(f"no registered transaction type matches any of "
                            f"{kw} (registered: {registered[:10]})")

    # 5. Dry-run every template: non-zero output, balanced by declared side
    templates = await db.dsl_templates.find({}, {"_id": 0}).to_list(20)
    if not templates:
        failures.append("no template was created")
    for t in templates:
        try:
            dry = await agent_tools.tool_dry_run_template(
                {"template_id": t.get("id")})
        except Exception as exc:
            failures.append(f"dry_run of '{t.get('name')}' raised: "
                            f"{str(exc)[:200]}")
            continue
        if dry.get("transaction_count", 0) <= 0:
            failures.append(f"template '{t.get('name')}' dry-run emitted 0 "
                            f"transactions")
        bc = dry.get("balance_check") or {}
        if bc and not bc.get("balanced"):
            failures.append(
                f"template '{t.get('name')}' is UNBALANCED: debit="
                f"{bc.get('debit_total')} credit={bc.get('credit_total')} "
                f"unknown_side_types={bc.get('unknown_side_types')}")
        if bc.get("unknown_side_types"):
            warnings.append(f"types with no declared side (excluded from "
                            f"balance check): {bc['unknown_side_types']}")
        for w in (dry.get("sanity_warnings") or []):
            warnings.append(f"sanity: {w[:200]}")

    steps_used = final.get("steps", len(events))
    return {
        "id": scenario["id"],
        "passed": not failures,
        "failures": failures,
        "warnings": warnings,
        "tool_errors": tool_errors,
        "steps": steps_used,
        "duration_s": duration_s,
        "final_status": final.get("status"),
        "rules": len(rules),
        "templates": len(templates),
    }


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--list", action="store_true", help="List scenario ids")
    ap.add_argument("--scenario", help="Comma-separated scenario ids to run")
    ap.add_argument("--provider", help="anthropic | openai")
    ap.add_argument("--model", help="Model id override")
    ap.add_argument("--max-steps", type=int,
                    default=int(os.environ.get("AGENT_EVAL_MAX_STEPS", "60")))
    ap.add_argument("--keep-db", action="store_true",
                    help="Keep the eval database after the run")
    args = ap.parse_args()

    if args.list:
        for s in SCENARIOS:
            print(f"  {s['id']:32s} {s['brief'][:80]}...")
        return 0

    selected = SCENARIOS
    if args.scenario:
        wanted = {x.strip() for x in args.scenario.split(",") if x.strip()}
        unknown = wanted - {s["id"] for s in SCENARIOS}
        if unknown:
            sys.exit(f"Unknown scenario id(s): {sorted(unknown)}")
        selected = [s for s in SCENARIOS if s["id"] in wanted]

    # Point the app at a throwaway eval DB BEFORE importing server: config.py
    # runs `load_dotenv(...)` (non-overriding) at import, so a value set here
    # wins over backend/.env.
    os.environ.setdefault("MONGO_URL", "mongodb://localhost:27017")
    eval_db_name = f"agent_eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.environ["DB_NAME"] = eval_db_name

    import server  # noqa: F401 — wires the agent bridge

    # Resolve provider AFTER importing server so API keys defined only in
    # backend/.env (loaded during import) are visible here too.
    provider_name, api_key, model = _resolve_provider_config(args)
    from ai_providers.registry import get_provider

    # Verify Mongo is actually reachable — the rule tools require it.
    try:
        await server.client.admin.command("ping")
    except Exception as exc:
        sys.exit(f"MongoDB not reachable at {os.environ['MONGO_URL']} "
                 f"({exc}). The eval harness requires a live Mongo.")

    provider = get_provider(provider_name)
    print(f"Eval run: provider={provider_name} model={model} "
          f"max_steps={args.max_steps} db={eval_db_name} "
          f"scenarios={[s['id'] for s in selected]}\n")

    results: list[dict] = []
    try:
        for scenario in selected:
            print(f"-- {scenario['id']} ".ljust(74, "-"))
            try:
                res = await _run_scenario(
                    scenario, server=server, provider=provider,
                    api_key=api_key, model=model, max_steps=args.max_steps,
                    eval_db_name=eval_db_name)
            except Exception as exc:
                res = {"id": scenario["id"], "passed": False,
                       "failures": [f"harness exception: {exc!r}"],
                       "warnings": [], "tool_errors": [], "steps": 0,
                       "duration_s": 0, "final_status": "error",
                       "rules": 0, "templates": 0}
            results.append(res)
            mark = "PASS" if res["passed"] else "FAIL"
            print(f"  [{mark}] steps={res['steps']} rules={res['rules']} "
                  f"templates={res['templates']} {res['duration_s']}s")
            for f in res["failures"]:
                print(f"    FAIL: {f}")
            for w in res["warnings"][:5]:
                print(f"    warn: {w}")
            print()
    finally:
        if not args.keep_db:
            try:
                await server.client.drop_database(eval_db_name)
            except Exception as exc:
                print(f"(could not drop eval db {eval_db_name}: {exc})")

    passed = sum(1 for r in results if r["passed"])
    print(f"{'=' * 74}\n{passed}/{len(results)} scenarios passed")

    out_dir = Path(__file__).parent / "eval_results"
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_file.write_text(json.dumps({
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "provider": provider_name, "model": model,
        "max_steps": args.max_steps,
        "results": results,
    }, indent=2), encoding="utf-8")
    print(f"Report written to {out_file}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
