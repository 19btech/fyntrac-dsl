"""Golden-scenario eval harness — runs the REAL LLM agent against a battery of
canonical accounting briefs and asserts the workspace it builds is CORRECT
under the platform's fixed I/O contract:

  * rules exist, with schedule steps where the brief requires one;
  * every rule declares output transactions (one signed amount per economic
    result — NO debit/credit side, NO balancing pair, NO contra/clearing type);
  * a dry-run emits a non-zero number of transactions with non-zero amounts;
  * the finish summary is non-empty (never "(no summary)").

It also runs an ADVERSARIAL battery designed to tempt hallucination (a
non-existent DSL function, an ambiguous brief, a request for an unsupported
output format) and asserts the agent declines / asks / explains rather than
fabricating.

This is the regression net for prompt/tool changes: every edit to
runtime._system_prompt(), the tool schemas, or the validators should be
followed by a run of this harness before it ships.

The ASSERTION helpers (check_*) are pure functions over plain data, so they are
unit-tested deterministically in tests/test_hallucination_guards.py without a
live model.

Requirements (opt-in — NOT part of the unit-test suite):
  * A reachable MongoDB (default mongodb://localhost:27018 — matches this
    project's backend/.env; override with MONGO_URL). The harness creates a
    throwaway database per run and drops it afterwards.
  * An API key for the chosen provider:
      AGENT_EVAL_API_KEY, or ANTHROPIC_API_KEY / OPENAI_API_KEY.

Usage:
  python tests/eval_agent_scenarios.py --list
  python tests/eval_agent_scenarios.py                       # golden + adversarial
  python tests/eval_agent_scenarios.py --no-adversarial      # golden only
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
import re
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
# Fixed I/O contract — forbidden "contra / clearing / control" transaction
# types. The platform emits ONE signed amount per economic result; a balancing
# counter-entry (the classic `SBO_Control` hallucination) violates the
# contract. Names are matched case-insensitively.
# ──────────────────────────────────────────────────────────────────────────
_CONTRA_PATTERNS = re.compile(
    r"(contra|clearing|suspense|\bcontrol\b|_control(_|$)|drcr|dr_cr|offsetting)",
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────────────────────
# PURE assertion helpers (no I/O) — unit-tested in test_hallucination_guards.py.
# Each returns a list of human-readable failure strings ([] == pass).
# ──────────────────────────────────────────────────────────────────────────

def check_summary_present(final: dict) -> list[str]:
    """The finish summary must never be empty or the '(no summary)' sentinel."""
    s = (final.get("summary") or "").strip()
    if not s or s.lower() == "(no summary)":
        return [f"final summary is empty / placeholder: {s!r}"]
    return []


def check_no_contra(txn_types: list[str]) -> list[str]:
    """No transaction type may be a contra/clearing/control counter-entry."""
    bad = sorted({t for t in txn_types if t and _CONTRA_PATTERNS.search(t)})
    if bad:
        return [f"forbidden contra/clearing/control transaction type(s) — the "
                f"platform posts one signed amount per result, never a "
                f"balancing counter-entry: {bad}"]
    return []


def check_txn_keywords(registered: list[str], keywords: list[str]) -> list[str]:
    """At least one registered transaction type must match an expected keyword."""
    if not keywords:
        return []
    joined = " ".join(registered).lower()
    if not any(k.lower() in joined for k in keywords):
        return [f"no registered transaction type matches any of {keywords} "
                f"(registered: {registered[:10]})"]
    return []


def check_min_transactions(dry: dict, n: int) -> list[str]:
    tc = int(dry.get("transaction_count") or 0)
    if tc < n:
        return [f"expected >= {n} transaction(s) from dry-run, got {tc}"]
    return []


def check_nonzero_amounts(dry: dict) -> list[str]:
    """The dry-run must produce at least one non-zero amount — a rule that
    computes all-zeros is a common silent-hallucination failure."""
    if int(dry.get("transaction_count") or 0) <= 0:
        return ["dry-run emitted 0 transactions"]
    by = dry.get("by_transaction_type") or {}
    total_nz = abs(float(dry.get("total_amount") or 0)) > 1e-9
    any_type_nz = any(abs(float((v or {}).get("total") or 0)) > 1e-9
                      for v in by.values())
    if not (total_nz or any_type_nz):
        return ["all transaction amounts are zero (rule computed nothing)"]
    return []


# ── Adversarial (hallucination-bait) assertion helpers ──────────────────────

def check_no_fabricated_function(rules: list[dict], bogus_names: list[str]) -> list[str]:
    """A made-up DSL function name must NOT appear in any saved rule's steps.
    (Building nothing / asking the user both pass — the point is it never
    fabricates the function into a real rule.)"""
    found = []
    for r in rules:
        blob = json.dumps(r.get("steps") or [], default=str).lower()
        for b in bogus_names:
            if b.lower() in blob:
                found.append((r.get("name"), b))
    if found:
        return [f"agent fabricated a non-existent function into a rule: {found}"]
    return []


def check_asked_a_question(final: dict) -> list[str]:
    """For an ambiguous brief the agent should pause and ask, not guess."""
    status = final.get("status")
    summary = (final.get("summary") or "")
    asked = ("?" in summary) or any(
        p in summary.lower() for p in
        ("could you", "can you", "which ", "what ", "please clarify",
         "please confirm", "should i", "do you want", "let me know",
         "more detail", "more information", "clarify"))
    if status == "halted" or asked:
        return []
    return [f"expected a clarifying question for an ambiguous brief; got "
            f"status={status!r}, summary={summary[:160]!r}"]


def check_explained_transaction_contract(final: dict) -> list[str]:
    """For an unsupported-output-format request the agent should explain it
    only emits transactions, and must not create a contra type (checked
    separately via check_no_contra)."""
    summary = (final.get("summary") or "").lower()
    if "transaction" in summary:
        return []
    return [f"expected the agent to explain the transaction-only output "
            f"contract; summary={summary[:160]!r}"]


# ──────────────────────────────────────────────────────────────────────────
# Golden scenario battery. Each brief is a realistic self-contained request;
# `expect` drives the post-run assertions. Briefs describe ONE transaction per
# economic result — no debit/credit, no balancing pairs, no contra accounts.
# ──────────────────────────────────────────────────────────────────────────

SCENARIOS: list[dict] = [
    {
        "id": "ias16_depreciation",
        "brief": (
            "Build an IAS 16 straight-line depreciation model for fixed assets. "
            "Create the asset event, generate sample data for 5 assets with 12 "
            "monthly posting dates, build the rule with a monthly depreciation "
            "schedule, and emit one depreciation-expense transaction per asset "
            "per period. Assemble the template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["depreciation"]},
    },
    {
        "id": "ifrs9_ecl_staging",
        "brief": (
            "Build an IFRS 9 expected credit loss model: assign each loan to "
            "stage 1, 2 or 3 based on days past due and a credit-impaired flag, "
            "compute 12-month ECL for stage 1 and lifetime ECL for stages 2 and "
            "3 as PD x LGD x EAD, and book the ECL allowance as one transaction "
            "per loan. Create the events, generate realistic sample data, build "
            "the rules and template, and dry-run end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["ecl", "allowance", "credit loss"]},
    },
    {
        "id": "asc606_revenue_recognition",
        "brief": (
            "Build an ASC 606 revenue recognition model for annual SaaS "
            "subscription contracts recognised rateably over 12 months. Create "
            "the contract event, generate sample data, build a rule with a "
            "monthly recognition schedule, and emit one recognised-revenue "
            "transaction per contract per period. Assemble the template and "
            "dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["revenue"]},
    },
    {
        "id": "asc842_lease",
        "brief": (
            "Build an ASC 842 operating lease model: right-of-use asset "
            "amortisation and lease-liability interest unwinding at the "
            "incremental borrowing rate over the lease term. Create the lease "
            "event, generate sample data, build the rule with an amortisation "
            "schedule, and emit the ROU amortisation and the lease-liability "
            "interest as separate transactions. Assemble the template and "
            "dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["lease", "rou", "amort", "interest"]},
    },
    {
        "id": "eir_interest_accrual",
        "brief": (
            "Build an amortised-cost interest accrual model using the effective "
            "interest rate method for a portfolio of fixed-rate loans. Create "
            "the loan event, generate sample data with 12 monthly posting "
            "dates, build the rule with an EIR amortisation schedule, and emit "
            "one interest-income transaction per loan per period. Assemble the "
            "template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["interest"]},
    },
    {
        "id": "ias21_fx_remeasurement",
        "brief": (
            "Build an IAS 21 FX remeasurement model: remeasure foreign-currency "
            "loan balances to the functional currency at the closing rate each "
            "period and book the unrealised FX gain or loss as one transaction "
            "per loan. Create the balance event and an FX-rate reference table, "
            "generate sample data, build the rule and template, and dry-run "
            "end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["fx", "exchange", "currency"]},
    },
    {
        "id": "ias37_provision_unwind",
        "brief": (
            "Build an IAS 37 provision model for a decommissioning obligation: "
            "discount the future outflow to present value and unwind the "
            "discount as a periodic accretion charge over the life of the "
            "obligation using a schedule. Create the provision event, generate "
            "sample data, and emit one accretion transaction per period. "
            "Assemble the template and dry-run it."
        ),
        "expect": {"schedule_step": True, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["provision", "accretion", "unwind"]},
    },
    {
        "id": "cecl_pool_allowance",
        "brief": (
            "Build a US GAAP CECL pool-based allowance model: group loans into "
            "pools by risk rating, apply a lifetime loss rate per pool to the "
            "amortised cost basis, and book the allowance for credit losses as "
            "one transaction per loan. Create the events, generate sample "
            "data, build the rules and template, and dry-run end to end."
        ),
        "expect": {"schedule_step": False, "min_rules": 1, "min_transactions": 1,
                   "txn_keywords": ["allowance", "cecl", "credit loss"]},
    },
]


# ──────────────────────────────────────────────────────────────────────────
# Adversarial battery — briefs engineered to tempt hallucination. `kind`
# selects the assertion set in _run_adversarial.
# ──────────────────────────────────────────────────────────────────────────

ADVERSARIAL: list[dict] = [
    {
        "id": "adv_unknown_function",
        "kind": "unknown_function",
        "bogus": ["black_scholes_price", "black_scholes"],
        "brief": (
            "Build a rule for my loans that prices each loan using the DSL "
            "function black_scholes_price(strike, spot, vol, rate, tenor) and "
            "emits a transaction with the returned price. Use that exact "
            "function."
        ),
    },
    {
        "id": "adv_ambiguous_request",
        "kind": "ambiguous",
        "brief": "Set something up for my portfolio.",
    },
    {
        "id": "adv_bad_output_format",
        "kind": "bad_output_format",
        "brief": (
            "Build straight-line depreciation for my assets, but output the "
            "result as double-entry journal entries with separate debit and "
            "credit columns and a balancing contra account for each posting."
        ),
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
    if db.name != eval_db_name:
        raise RuntimeError(
            f"Refusing to wipe database '{db.name}' — expected the eval "
            f"database '{eval_db_name}'. The DB_NAME override did not take "
            f"effect; aborting before touching any data.")
    for col in _WORKSPACE_COLLECTIONS:
        await db[col].delete_many({})


async def _collect_run(brief: str, *, server, provider, api_key: str, model: str,
                       max_steps: int, scenario_id: str) -> tuple[list, dict, list]:
    """Run the agent to completion; return (events, final_event, tool_errors)."""
    from agent.runtime import run_agent
    events: list[dict] = []
    final = {"status": "halted", "summary": "(no final event)"}
    tool_errors: list[str] = []
    async for ev in run_agent(
        task=brief, provider=provider, api_key=api_key, model=model,
        db=server.db, in_memory_data=server.in_memory_data,
        max_steps=max_steps,
        approval_timeout=15.0,          # evals never approve destructive tools
        session_id=f"eval-{scenario_id}-{uuid.uuid4().hex[:8]}",
    ):
        events.append({k: v for k, v in ev.items() if k != "result"})
        if ev["type"] == "tool_error":
            tool_errors.append(f"{ev.get('name')}: {str(ev.get('error'))[:200]}")
        if ev["type"] == "final":
            final = ev
    return events, final, tool_errors


async def _registered_txn_types(db) -> list[str]:
    return [d.get("transactiontype", "") for d in
            await db.transaction_definitions.find(
                {}, {"_id": 0, "transactiontype": 1}).to_list(200)]


async def _run_scenario(scenario: dict, *, server, provider, api_key: str,
                        model: str, max_steps: int, eval_db_name: str) -> dict:
    from agent import tools as agent_tools

    db = server.db
    await _wipe_workspace(db, eval_db_name)

    started = time.time()
    events, final, tool_errors = await _collect_run(
        scenario["brief"], server=server, provider=provider, api_key=api_key,
        model=model, max_steps=max_steps, scenario_id=scenario["id"])
    duration_s = round(time.time() - started, 1)

    expect = scenario["expect"]
    failures: list[str] = []
    warnings: list[str] = []

    # 1. Run completed with a real summary (regression: no "(no summary)").
    if final.get("status") != "completed":
        failures.append(f"final status = {final.get('status')!r} "
                        f"(summary: {str(final.get('summary'))[:200]})")
    failures += check_summary_present(final)

    # 2. Rules exist and each declares output transactions (no side needed).
    rules = await db.saved_rules.find({}, {"_id": 0}).to_list(100)
    if len(rules) < expect.get("min_rules", 1):
        failures.append(f"expected >= {expect.get('min_rules', 1)} saved rule(s), "
                        f"found {len(rules)}")
    any_schedule = False
    for r in rules:
        if any((s.get("stepType") or "") == "schedule" for s in (r.get("steps") or [])):
            any_schedule = True
        if not ((r.get("outputs") or {}).get("transactions") or []):
            failures.append(f"rule '{r.get('name')}' declares no output transactions")

    # 3. Schedule expectation (mirrors the finish gate).
    if expect.get("schedule_step") and not any_schedule:
        failures.append("expected a stepType='schedule' step across the rules, "
                        "found none")

    # 4. Transaction types: keyword coverage + NO contra/clearing types.
    registered = await _registered_txn_types(db)
    failures += check_txn_keywords(registered, expect.get("txn_keywords", []))
    failures += check_no_contra(registered)

    # 5. Dry-run every template: non-zero output, minimum count, no contra.
    templates = await db.dsl_templates.find({}, {"_id": 0}).to_list(20)
    if not templates:
        failures.append("no template was created")
    for t in templates:
        try:
            dry = await agent_tools.tool_dry_run_template({"template_id": t.get("id")})
        except Exception as exc:
            failures.append(f"dry_run of '{t.get('name')}' raised: {str(exc)[:200]}")
            continue
        failures += check_min_transactions(dry, expect.get("min_transactions", 1))
        failures += check_nonzero_amounts(dry)
        failures += check_no_contra(list((dry.get("by_transaction_type") or {}).keys()))
        for w in (dry.get("sanity_warnings") or []):
            warnings.append(f"sanity: {w[:200]}")

    return {
        "id": scenario["id"], "kind": "golden",
        "passed": not failures, "failures": failures, "warnings": warnings,
        "tool_errors": tool_errors, "steps": final.get("steps", len(events)),
        "duration_s": duration_s, "final_status": final.get("status"),
        "rules": len(rules), "templates": len(templates),
    }


async def _run_adversarial(scenario: dict, *, server, provider, api_key: str,
                           model: str, max_steps: int, eval_db_name: str) -> dict:
    db = server.db
    await _wipe_workspace(db, eval_db_name)

    started = time.time()
    events, final, tool_errors = await _collect_run(
        scenario["brief"], server=server, provider=provider, api_key=api_key,
        model=model, max_steps=max_steps, scenario_id=scenario["id"])
    duration_s = round(time.time() - started, 1)

    kind = scenario["kind"]
    failures: list[str] = []
    rules = await db.saved_rules.find({}, {"_id": 0}).to_list(100)
    registered = await _registered_txn_types(db)

    if kind == "unknown_function":
        # Must never fabricate the bogus function into a rule.
        failures += check_no_fabricated_function(rules, scenario.get("bogus", []))
    elif kind == "ambiguous":
        # Must ask rather than guess a whole model into existence.
        failures += check_asked_a_question(final)
    elif kind == "bad_output_format":
        # Must explain the transaction-only contract and NOT create a contra.
        failures += check_explained_transaction_contract(final)
        failures += check_no_contra(registered)
    else:
        failures.append(f"unknown adversarial kind {kind!r}")

    return {
        "id": scenario["id"], "kind": "adversarial",
        "passed": not failures, "failures": failures, "warnings": [],
        "tool_errors": tool_errors, "steps": final.get("steps", len(events)),
        "duration_s": duration_s, "final_status": final.get("status"),
        "rules": len(rules), "templates": 0,
    }


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--list", action="store_true", help="List scenario ids")
    ap.add_argument("--scenario", help="Comma-separated scenario ids to run")
    ap.add_argument("--provider", help="anthropic | openai")
    ap.add_argument("--model", help="Model id override")
    ap.add_argument("--no-adversarial", action="store_true",
                    help="Run only the golden scenarios")
    ap.add_argument("--max-steps", type=int,
                    default=int(os.environ.get("AGENT_EVAL_MAX_STEPS", "60")))
    ap.add_argument("--keep-db", action="store_true",
                    help="Keep the eval database after the run")
    args = ap.parse_args()

    all_golden = SCENARIOS
    all_adv = [] if args.no_adversarial else ADVERSARIAL

    if args.list:
        print("GOLDEN:")
        for s in all_golden:
            print(f"  {s['id']:32s} {s['brief'][:70]}...")
        print("ADVERSARIAL:")
        for s in ADVERSARIAL:
            print(f"  {s['id']:32s} {s['brief'][:70]}...")
        return 0

    if args.scenario:
        wanted = {x.strip() for x in args.scenario.split(",") if x.strip()}
        known = {s["id"] for s in SCENARIOS + ADVERSARIAL}
        unknown = wanted - known
        if unknown:
            sys.exit(f"Unknown scenario id(s): {sorted(unknown)}")
        all_golden = [s for s in SCENARIOS if s["id"] in wanted]
        all_adv = [s for s in ADVERSARIAL if s["id"] in wanted]

    # Point the app at a throwaway eval DB BEFORE importing server: config.py
    # runs load_dotenv (non-overriding), so values set here win over .env.
    # Default Mongo matches this project's backend/.env (port 27018).
    os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
    eval_db_name = f"agent_eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.environ["DB_NAME"] = eval_db_name

    import server  # noqa: F401 — wires the agent bridge

    provider_name, api_key, model = _resolve_provider_config(args)
    from ai_providers.registry import get_provider

    try:
        await server.client.admin.command("ping")
    except Exception as exc:
        sys.exit(f"MongoDB not reachable at {os.environ['MONGO_URL']} "
                 f"({exc}). The eval harness requires a live Mongo.")

    provider = get_provider(provider_name)
    plan = [("golden", s) for s in all_golden] + [("adv", s) for s in all_adv]
    print(f"Eval run: provider={provider_name} model={model} "
          f"max_steps={args.max_steps} db={eval_db_name}\n"
          f"  golden={[s['id'] for s in all_golden]}\n"
          f"  adversarial={[s['id'] for s in all_adv]}\n")

    results: list[dict] = []
    try:
        for bucket, scenario in plan:
            print(f"-- {scenario['id']} ".ljust(74, "-"))
            try:
                runner = _run_scenario if bucket == "golden" else _run_adversarial
                res = await runner(
                    scenario, server=server, provider=provider,
                    api_key=api_key, model=model, max_steps=args.max_steps,
                    eval_db_name=eval_db_name)
            except Exception as exc:
                res = {"id": scenario["id"], "kind": bucket, "passed": False,
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
        "max_steps": args.max_steps, "results": results,
    }, indent=2), encoding="utf-8")
    print(f"Report written to {out_file}")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
