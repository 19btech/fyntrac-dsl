"""Deterministic unit tests for the anti-hallucination guards.

Covers, with NO live model / DB required:
  * runtime summary-grounding: _money_numbers_in_text, _numbers_in_result,
    _ungrounded_amounts
  * eval assertion helpers: check_* in eval_agent_scenarios

Run:  python tests/test_hallucination_guards.py
"""

import os
import sys

_HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(_HERE, "..", "backend"))
sys.path.insert(0, _HERE)

from agent.runtime import (            # noqa: E402
    _money_numbers_in_text, _numbers_in_result, _ungrounded_amounts,
)
import eval_agent_scenarios as ev      # noqa: E402


_failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        _failures.append(name)


# ── Runtime: money extraction from prose ────────────────────────────────────

def test_money_numbers_in_text():
    got = _money_numbers_in_text(
        "I booked $12,345.67 of interest across 5 loans over 12 months in 2024")
    check("money: picks $ thousands-decimal", got == {12345.67}, got)

    got = _money_numbers_in_text("the monthly charge is 1,234.00 and total 9999.99")
    check("money: comma and 2-decimal forms", got == {1234.00, 9999.99}, got)

    check("money: ignores small counts", _money_numbers_in_text("3 transactions, 5 loans") == set())
    check("money: ignores sub-threshold $50", _money_numbers_in_text("just $50") == set())
    check("money: keeps $500", _money_numbers_in_text("a fee of $500") == {500.0})
    check("money: ignores bare year 2024", _money_numbers_in_text("in the year 2024") == set())


# ── Runtime: numbers observed in a tool result ──────────────────────────────

def test_numbers_in_result():
    res = {"transaction_count": 15, "total_amount": 12345.67,
           "transaction_summary": {"net_amount": 12345.67,
                                   "per_instrument_net": {"L1": 2469.13, "L2": 9876.54}},
           "flag": True, "name": "InterestIncome", "note": "1,000.00"}
    got = _numbers_in_result(res)
    check("observed: collects nested numbers",
          {15.0, 12345.67, 2469.13, 9876.54, 1000.0} <= got, got)
    check("observed: excludes booleans (no stray 1.0 from True)", 1.0 not in got, got)
    check("observed: parses numeric strings", 1000.0 in got, got)


# ── Runtime: grounding verdict ──────────────────────────────────────────────

def test_ungrounded_amounts():
    observed = {12345.67, 2469.13}
    check("grounded: exact match clears", _ungrounded_amounts("posted $12,345.67", observed) == [])
    check("ungrounded: unseen amount flagged",
          _ungrounded_amounts("posted $9,999.99", observed) == [9999.99],
          _ungrounded_amounts("posted $9,999.99", observed))
    check("grounded: within tolerance",
          _ungrounded_amounts("about $2,469.14", observed) == [], "")
    check("grounded: no money in summary => nothing to flag",
          _ungrounded_amounts("Built a model for 5 loans.", observed) == [])
    check("grounded: empty observed but no money => ok",
          _ungrounded_amounts("Done.", set()) == [])


# ── Eval: core assertion helpers ────────────────────────────────────────────

def test_check_summary_present():
    check("summary: empty fails", ev.check_summary_present({"summary": ""}))
    check("summary: placeholder fails", ev.check_summary_present({"summary": "(no summary)"}))
    check("summary: real passes", ev.check_summary_present({"summary": "Built a loan model."}) == [])


def test_check_no_contra():
    check("contra: SBO_Control flagged", ev.check_no_contra(["InterestIncome", "SBO_Control"]))
    check("contra: ContraAsset flagged", ev.check_no_contra(["ContraAsset"]))
    check("contra: ClearingAccount flagged", ev.check_no_contra(["ClearingAccount"]))
    check("contra: SuspenseHolding flagged", ev.check_no_contra(["SuspenseHolding"]))
    check("contra: clean set passes",
          ev.check_no_contra(["InterestIncomeAccrual", "ECLAllowance", "RevenueRecognised"]) == [])
    check("contra: 'Controlled' word not a false positive",
          ev.check_no_contra(["ControlledDisbursement"]) == [],
          ev.check_no_contra(["ControlledDisbursement"]))


def test_check_txn_keywords():
    check("kw: match passes", ev.check_txn_keywords(["InterestIncomeAccrual"], ["interest"]) == [])
    check("kw: no match fails", ev.check_txn_keywords(["Foo"], ["interest"]))
    check("kw: empty expectation passes", ev.check_txn_keywords(["Foo"], []) == [])


def test_check_min_transactions():
    check("min_txn: below fails", ev.check_min_transactions({"transaction_count": 0}, 1))
    check("min_txn: meets passes", ev.check_min_transactions({"transaction_count": 5}, 1) == [])


def test_check_nonzero_amounts():
    check("nonzero: zero count fails", ev.check_nonzero_amounts({"transaction_count": 0}))
    check("nonzero: all-zero fails",
          ev.check_nonzero_amounts({"transaction_count": 3, "total_amount": 0,
                                    "by_transaction_type": {"X": {"total": 0}}}))
    check("nonzero: has amount passes",
          ev.check_nonzero_amounts({"transaction_count": 3, "total_amount": 100.0,
                                    "by_transaction_type": {"X": {"total": 100.0}}}) == [])
    check("nonzero: nets-zero-but-types-nonzero passes",
          ev.check_nonzero_amounts({"transaction_count": 2, "total_amount": 0.0,
                                    "by_transaction_type": {"A": {"total": 100.0},
                                                            "B": {"total": -100.0}}}) == [])


# ── Eval: adversarial assertion helpers ─────────────────────────────────────

def test_check_no_fabricated_function():
    bogus = ["black_scholes_price"]
    check("fab: used in rule fails",
          ev.check_no_fabricated_function(
              [{"name": "R", "steps": [{"formula": "black_scholes_price(a,b)"}]}], bogus))
    check("fab: real function passes",
          ev.check_no_fabricated_function(
              [{"name": "R", "steps": [{"formula": "pmt(a,b,c)"}]}], bogus) == [])
    check("fab: no rules (asked instead) passes",
          ev.check_no_fabricated_function([], bogus) == [])


def test_check_asked_a_question():
    check("ask: halted passes", ev.check_asked_a_question({"status": "halted", "summary": "x"}) == [])
    check("ask: question mark passes",
          ev.check_asked_a_question({"status": "completed", "summary": "Which product line?"}) == [])
    check("ask: phrase passes",
          ev.check_asked_a_question({"status": "completed", "summary": "Could you clarify the term?"}) == [])
    check("ask: silent build fails",
          ev.check_asked_a_question({"status": "completed", "summary": "Built the model."}))


def test_check_explained_transaction_contract():
    check("contract: mentions transactions passes",
          ev.check_explained_transaction_contract(
              {"summary": "This platform only produces transactions, not journal entries."}) == [])
    check("contract: no mention fails",
          ev.check_explained_transaction_contract({"summary": "Here are your journal entries."}))


def main():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    for t in tests:
        print(f"[{t.__name__}]")
        t()
    print()
    if _failures:
        print(f"FAILED ({len(_failures)}): {_failures}")
        return 1
    print(f"ALL {sum(1 for _ in tests)} test groups passed — hallucination guards OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
