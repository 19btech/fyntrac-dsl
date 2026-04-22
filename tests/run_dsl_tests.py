#!/usr/bin/env python3
"""
run_dsl_tests.py — Executes all DSL function test cases against the live backend.

Usage:
    python3 tests/run_dsl_tests.py [--url http://localhost:8000] [--verbose]
"""

import sys
import json
import time
import argparse
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8000"

# ─── Test cases (mirrored from frontend/src/agent/testing/sampleData.js) ────

TEST_CASES = [
    # ── Financial (18) ────────────────────────────────────────────
    {"name": "pv",               "cat": "Financial",     "dsl": "print(pv(0.05, 10, -1000))"},
    {"name": "fv",               "cat": "Financial",     "dsl": "print(fv(0.05, 10, -1000))"},
    {"name": "pmt",              "cat": "Financial",     "dsl": "print(pmt(0.05, 10, -50000))"},
    {"name": "rate",             "cat": "Financial",     "dsl": "print(rate(10, -1000, 7722))"},
    {"name": "nper",             "cat": "Financial",     "dsl": "print(nper(0.05, -1000, 7722))"},
    {"name": "npv",              "cat": "Financial",     "dsl": "print(npv(0.1, [-1000, 300, 400, 500, 600]))"},
    {"name": "irr",              "cat": "Financial",     "dsl": "print(irr([-1000, 300, 400, 500, 600]))"},
    {"name": "xnpv",             "cat": "Financial",     "dsl": 'print(xnpv(0.1, [-1000, 300, 400, 500], ["2024-01-01", "2024-06-01", "2025-01-01", "2025-06-01"]))'},
    {"name": "xirr",             "cat": "Financial",     "dsl": 'print(xirr([-1000, 300, 400, 500], ["2024-01-01", "2024-06-01", "2025-01-01", "2025-06-01"]))'},
    {"name": "discount_factor",  "cat": "Financial",     "dsl": "print(discount_factor(0.05, 1.0))"},
    {"name": "accumulation_factor", "cat": "Financial",  "dsl": "print(accumulation_factor(0.05, 1.0))"},
    {"name": "effective_rate",   "cat": "Financial",     "dsl": "print(effective_rate(0.12, 12))"},
    {"name": "nominal_rate",     "cat": "Financial",     "dsl": "print(nominal_rate(0.1268, 12))"},
    {"name": "yield_to_maturity","cat": "Financial",     "dsl": "print(yield_to_maturity(950, 1000, 50, 10))"},
    {"name": "compound_interest","cat": "Financial",     "dsl": "print(compound_interest(10000, 0.05, 5))"},
    {"name": "interest_on_balance","cat":"Financial",    "dsl": "print(interest_on_balance(100000, 0.05, 90))"},
    {"name": "capitalization",   "cat": "Financial",     "dsl": "print(capitalization(500, 10000))"},
    {"name": "amortized_cost",   "cat": "Financial",     "dsl": "print(amortized_cost(10000, 500, 1200))"},

    # ── Depreciation (5) ──────────────────────────────────────────
    {"name": "straight_line",    "cat": "Depreciation",  "dsl": "print(straight_line(10000, 1000, 5))"},
    {"name": "reducing_balance", "cat": "Depreciation",  "dsl": "print(reducing_balance(10000, 0.2))"},
    {"name": "double_declining", "cat": "Depreciation",  "dsl": "print(double_declining(10000, 5))"},
    {"name": "sum_of_years",     "cat": "Depreciation",  "dsl": "print(sum_of_years(10000, 1000, 5, 1))"},
    {"name": "units_of_production","cat":"Depreciation", "dsl": "print(units_of_production(10000, 2000, 10000))"},

    # ── Allocation (5) ────────────────────────────────────────────
    {"name": "prorate",          "cat": "Allocation",    "dsl": "print(prorate(1000, 3, 12))"},
    {"name": "allocate",         "cat": "Allocation",    "dsl": "print(allocate(1000, [0.5, 0.3, 0.2]))"},
    {"name": "split",            "cat": "Allocation",    "dsl": "print(split(1000, 3))"},
    {"name": "percentage_of",    "cat": "Allocation",    "dsl": "print(percentage_of(500, 15))"},
    {"name": "ratio_split",      "cat": "Allocation",    "dsl": "print(ratio_split(1000, [2, 3, 5]))"},

    # ── Balance (3) ───────────────────────────────────────────────
    {"name": "rolling_balance",  "cat": "Balance",       "dsl": "print(rolling_balance(1000, [100, -200, 300, -50]))"},
    {"name": "average_balance",  "cat": "Balance",       "dsl": "print(average_balance([1000, 1100, 900, 1200, 1150]))"},
    {"name": "weighted_balance", "cat": "Balance",       "dsl": "print(weighted_balance([1000, 1100, 900], [30, 31, 29]))"},

    # ── Arithmetic (15) ───────────────────────────────────────────
    {"name": "add",              "cat": "Arithmetic",    "dsl": "print(add(100, 250))"},
    {"name": "subtract",         "cat": "Arithmetic",    "dsl": "print(subtract(500, 175))"},
    {"name": "multiply",         "cat": "Arithmetic",    "dsl": "print(multiply(25, 40))"},
    {"name": "divide",           "cat": "Arithmetic",    "dsl": "print(divide(1000, 8))"},
    {"name": "power",            "cat": "Arithmetic",    "dsl": "print(power(2, 10))"},
    {"name": "sqrt",             "cat": "Arithmetic",    "dsl": "print(sqrt(144))"},
    {"name": "abs",              "cat": "Arithmetic",    "dsl": "print(abs(-42.5))"},
    {"name": "sign",             "cat": "Arithmetic",    "dsl": "print(sign(-100))"},
    {"name": "round",            "cat": "Arithmetic",    "dsl": "print(round(3.14159, 2))"},
    {"name": "floor",            "cat": "Arithmetic",    "dsl": "print(floor(3.9))"},
    {"name": "ceil",             "cat": "Arithmetic",    "dsl": "print(ceil(3.1))"},
    {"name": "mod",              "cat": "Arithmetic",    "dsl": "print(mod(17, 5))"},
    {"name": "truncate",         "cat": "Arithmetic",    "dsl": "print(truncate(3.7891, 2))"},
    {"name": "percentage",       "cat": "Arithmetic",    "dsl": "print(percentage(25, 200))"},
    {"name": "change_pct",       "cat": "Arithmetic",    "dsl": "print(change_pct(100, 125))"},

    # ── Comparison (10) ───────────────────────────────────────────
    {"name": "eq",               "cat": "Comparison",    "dsl": "print(eq(100, 100))"},
    {"name": "neq",              "cat": "Comparison",    "dsl": "print(neq(100, 200))"},
    {"name": "gt",               "cat": "Comparison",    "dsl": "print(gt(200, 100))"},
    {"name": "gte",              "cat": "Comparison",    "dsl": "print(gte(100, 100))"},
    {"name": "lt",               "cat": "Comparison",    "dsl": "print(lt(50, 100))"},
    {"name": "lte",              "cat": "Comparison",    "dsl": "print(lte(100, 100))"},
    {"name": "between",          "cat": "Comparison",    "dsl": "print(between(5, 1, 10))"},
    {"name": "is_null",          "cat": "Comparison",    "dsl": "print(is_null(None))"},
    {"name": "is_positive",      "cat": "Comparison",    "dsl": "print(is_positive(42))"},
    {"name": "is_negative",      "cat": "Comparison",    "dsl": "print(is_negative(-5))"},

    # ── Logical (10) ──────────────────────────────────────────────
    {"name": "and",              "cat": "Logical",       "dsl": "print(True and True)"},
    {"name": "or",               "cat": "Logical",       "dsl": "print(False or True)"},
    {"name": "not",              "cat": "Logical",       "dsl": "print(not(False))"},
    {"name": "xor",              "cat": "Logical",       "dsl": "print(xor(True, False))"},
    {"name": "all",              "cat": "Logical",       "dsl": "print(all([True, True, True]))"},
    {"name": "any",              "cat": "Logical",       "dsl": "print(any([False, False, True]))"},
    {"name": "iif",              "cat": "Logical",       "dsl": 'print(iif(True, "yes", "no"))'},
    {"name": "coalesce",         "cat": "Logical",       "dsl": "print(coalesce(None, None, 42, 99))"},
    {"name": "clamp",            "cat": "Logical",       "dsl": "print(clamp(150, 0, 100))"},
    {"name": "switch",           "cat": "Logical",       "dsl": 'print(switch("B", {"A": 1, "B": 2, "C": 3}, 0))'},

    # ── Date (20) ─────────────────────────────────────────────────
    {"name": "normalize_date",   "cat": "Date",          "dsl": 'print(normalize_date("01/15/2024"))'},
    {"name": "days_between",     "cat": "Date",          "dsl": 'print(days_between("2024-01-01", "2024-03-15"))'},
    {"name": "months_between",   "cat": "Date",          "dsl": 'print(months_between("2024-01-15", "2024-09-15"))'},
    {"name": "years_between",    "cat": "Date",          "dsl": 'print(years_between("2020-01-01", "2024-01-01"))'},
    {"name": "add_days",         "cat": "Date",          "dsl": 'print(add_days("2024-01-01", 45))'},
    {"name": "add_months",       "cat": "Date",          "dsl": 'print(add_months("2024-01-31", 1))'},
    {"name": "add_years",        "cat": "Date",          "dsl": 'print(add_years("2024-02-29", 1))'},
    {"name": "subtract_days",    "cat": "Date",          "dsl": 'print(subtract_days("2024-03-01", 1))'},
    {"name": "subtract_months",  "cat": "Date",          "dsl": 'print(subtract_months("2024-03-31", 1))'},
    {"name": "subtract_years",   "cat": "Date",          "dsl": 'print(subtract_years("2024-01-01", 5))'},
    {"name": "start_of_month",   "cat": "Date",          "dsl": 'print(start_of_month("2024-06-15"))'},
    {"name": "end_of_month",     "cat": "Date",          "dsl": 'print(end_of_month("2024-02-15"))'},
    {"name": "day_count_fraction","cat":"Date",          "dsl": 'print(day_count_fraction("2024-01-01", "2024-07-01", "ACT/360"))'},
    {"name": "is_leap_year",     "cat": "Date",          "dsl": "print(is_leap_year(2024))"},
    {"name": "days_in_year",     "cat": "Date",          "dsl": "print(days_in_year(2024))"},
    {"name": "quarter",          "cat": "Date",          "dsl": 'print(quarter("2024-08-15"))'},
    {"name": "day_of_week",      "cat": "Date",          "dsl": 'print(day_of_week("2024-01-01"))'},
    {"name": "is_weekend",       "cat": "Date",          "dsl": 'print(is_weekend("2024-01-06"))'},
    {"name": "normalize_arraydate","cat":"Date",         "dsl": 'print(normalize_arraydate(["01/15/2024", "2024-02-28", "03-15-2024"]))'},
    {"name": "business_days",    "cat": "Date",          "dsl": 'print(business_days("2024-01-01", "2024-01-31"))'},

    # ── Schedule (7) ──────────────────────────────────────────────
    {"name": "schedule",         "cat": "Schedule",      "dsl": (
        'result = schedule(\n'
        '  period("2024-01-01", "2024-06-30", "M"),\n'
        '  {"period_date": "period_date", "amount": "1000"}\n'
        ')\nprint(result)'
    )},
    {"name": "period",           "cat": "Schedule",      "dsl": 'print(period("2024-01-01", "2024-12-31", "Q"))'},
    {"name": "schedule_sum",     "cat": "Schedule",      "dsl": (
        's = schedule(\n'
        '  period("2024-01-01", "2024-03-31", "M"),\n'
        '  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n'
        ')\nprint(schedule_sum(s, "amount"))'
    )},
    {"name": "schedule_last",    "cat": "Schedule",      "dsl": (
        's = schedule(\n'
        '  period("2024-01-01", "2024-03-31", "M"),\n'
        '  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n'
        ')\nprint(schedule_last(s, "amount"))'
    )},
    {"name": "schedule_first",   "cat": "Schedule",      "dsl": (
        's = schedule(\n'
        '  period("2024-01-01", "2024-03-31", "M"),\n'
        '  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n'
        ')\nprint(schedule_first(s, "amount"))'
    )},
    {"name": "schedule_column",  "cat": "Schedule",      "dsl": (
        's = schedule(\n'
        '  period("2024-01-01", "2024-03-31", "M"),\n'
        '  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n'
        ')\nprint(schedule_column(s, "amount"))'
    )},
    {"name": "schedule_filter",  "cat": "Schedule",      "dsl": (
        's = schedule(\n'
        '  period("2024-01-01", "2024-03-31", "M"),\n'
        '  {"seq": "period_index + 1", "amount": "(period_index + 1) * 100"}\n'
        ')\nprint(schedule_filter(s, "seq", 2, "amount"))'
    )},

    # ── Aggregation (13) ──────────────────────────────────────────
    {"name": "sum",              "cat": "Aggregation",   "dsl": "print(sum([10, 20, 30, 40, 50]))"},
    {"name": "sum_field",        "cat": "Aggregation",   "dsl": 'print(sum_field([{"amount": 100}, {"amount": 200}, {"amount": 300}], "amount"))'},
    {"name": "avg",              "cat": "Aggregation",   "dsl": "print(avg([10, 20, 30, 40, 50]))"},
    {"name": "min",              "cat": "Aggregation",   "dsl": "print(min([42, 17, 99, 3, 56]))"},
    {"name": "max",              "cat": "Aggregation",   "dsl": "print(max([42, 17, 99, 3, 56]))"},
    {"name": "count",            "cat": "Aggregation",   "dsl": "print(count([1, 2, 3, 4, 5]))"},
    {"name": "weighted_avg",     "cat": "Aggregation",   "dsl": "print(weighted_avg([100, 200, 300], [1, 2, 3]))"},
    {"name": "cumulative_sum",   "cat": "Aggregation",   "dsl": "print(cumulative_sum([10, 20, 30, 40]))"},
    {"name": "median",           "cat": "Aggregation",   "dsl": "print(median([3, 7, 1, 9, 5]))"},
    {"name": "variance",         "cat": "Aggregation",   "dsl": "print(variance([2, 4, 4, 4, 5, 5, 7, 9]))"},
    {"name": "std_dev",          "cat": "Aggregation",   "dsl": "print(std_dev([2, 4, 4, 4, 5, 5, 7, 9]))"},
    {"name": "percentile",       "cat": "Aggregation",   "dsl": "print(percentile([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], 0.75))"},
    {"name": "range",            "cat": "Aggregation",   "dsl": "print(range([10, 50, 30, 90, 20]))"},

    # ── Conversion (6) ────────────────────────────────────────────
    {"name": "fx_convert",       "cat": "Conversion",    "dsl": "print(fx_convert(1000, 1.35))"},
    {"name": "normalize",        "cat": "Conversion",    "dsl": "print(normalize(50, 200))"},
    {"name": "basis_points",     "cat": "Conversion",    "dsl": "print(basis_points(0.0525))"},
    {"name": "from_bps",         "cat": "Conversion",    "dsl": "print(from_bps(525))"},
    {"name": "to_percentage",    "cat": "Conversion",    "dsl": "print(to_percentage(0.1275))"},
    {"name": "from_percentage",  "cat": "Conversion",    "dsl": "print(from_percentage(12.75))"},

    # ── Statistical (3) ───────────────────────────────────────────
    {"name": "correlation",      "cat": "Statistical",   "dsl": "print(correlation([1, 2, 3, 4, 5], [2, 4, 5, 4, 5]))"},
    {"name": "covariance",       "cat": "Statistical",   "dsl": "print(covariance([1, 2, 3, 4, 5], [2, 4, 5, 4, 5]))"},
    {"name": "zscore",           "cat": "Statistical",   "dsl": "print(zscore(85, 70, 10))"},

    # ── String (9) ────────────────────────────────────────────────
    {"name": "lower",            "cat": "String",        "dsl": 'print(lower("HELLO WORLD"))'},
    {"name": "upper",            "cat": "String",        "dsl": 'print(upper("hello world"))'},
    {"name": "concat",           "cat": "String",        "dsl": 'print(concat("Hello", " ", "World"))'},
    {"name": "contains",         "cat": "String",        "dsl": 'print(contains("Hello World", "World"))'},
    {"name": "eq_ignore_case",   "cat": "String",        "dsl": 'print(eq_ignore_case("Hello", "hello"))'},
    {"name": "starts_with",      "cat": "String",        "dsl": 'print(starts_with("FyntracDSL", "Fyn"))'},
    {"name": "ends_with",        "cat": "String",        "dsl": 'print(ends_with("report.pdf", ".pdf"))'},
    {"name": "trim",             "cat": "String",        "dsl": 'print(trim("  hello  "))'},
    {"name": "str_length",       "cat": "String",        "dsl": 'print(str_length("Fyntrac"))'},

    # ── Iteration (4) ─────────────────────────────────────────────
    {"name": "for_each",         "cat": "Iteration",     "dsl": (
        'result = for_each(["2024-01-01", "2024-02-01"], [100, 200], "d", "a", "str(d) + \': $\' + str(a)")\n'
        'print(result)'
    )},
    {"name": "for_each_with_index","cat":"Iteration",    "dsl": (
        'result = for_each_with_index([10, 20, 30], "x", "x * 2")\n'
        'print(result)'
    )},
    {"name": "map_array",        "cat": "Iteration",     "dsl": 'print(map_array([1, 2, 3, 4, 5], "x", "x * x"))'},
    {"name": "array_filter",     "cat": "Iteration",     "dsl": 'print(array_filter([1, 2, 3, 4, 5, 6, 7, 8], "x", "x > 4"))'},

    # ── Array Utilities (10) ──────────────────────────────────────
    {"name": "lookup",           "cat": "Array Utilities","dsl": 'print(lookup([100, 200, 300], ["A", "B", "C"], "B"))'},
    {"name": "zip_arrays",       "cat": "Array Utilities","dsl": 'print(zip_arrays([1, 2, 3], ["a", "b", "c"]))'},
    {"name": "array_length",     "cat": "Array Utilities","dsl": "print(array_length([10, 20, 30, 40, 50]))"},
    {"name": "array_get",        "cat": "Array Utilities","dsl": 'print(array_get(["a", "b", "c", "d"], 2))'},
    {"name": "array_first",      "cat": "Array Utilities","dsl": "print(array_first([100, 200, 300]))"},
    {"name": "array_last",       "cat": "Array Utilities","dsl": "print(array_last([100, 200, 300]))"},
    {"name": "array_slice",      "cat": "Array Utilities","dsl": "print(array_slice([10, 20, 30, 40, 50], 1, 4))"},
    {"name": "array_reverse",    "cat": "Array Utilities","dsl": "print(array_reverse([1, 2, 3, 4, 5]))"},
    {"name": "array_append",     "cat": "Array Utilities","dsl": "print(array_append([1, 2, 3], 4))"},
    {"name": "array_extend",     "cat": "Array Utilities","dsl": "print(array_extend([1, 2], [3, 4, 5]))"},

    # ── Transaction (1) ───────────────────────────────────────────
    {"name": "createTransaction","cat": "Transaction",   "dsl": 'print(createTransaction("2024-01-15", "2024-01-15", "PAYMENT", 5000))'},

    # ── Array Collection (6 — require events, tested with simplified args) ──
    {"name": "collect_by_instrument",            "cat": "Array", "dsl": "print(collect_by_instrument([100, 200]))",  "skip": True},
    {"name": "collect_all",                      "cat": "Array", "dsl": "print(collect_all([100, 200, 300]))",        "skip": True},
    {"name": "collect_by_subinstrument",         "cat": "Array", "dsl": "print(collect_by_subinstrument([100]))",    "skip": True},
    {"name": "collect_subinstrumentids",         "cat": "Array", "dsl": "print(collect_subinstrumentids())",         "skip": True},
    {"name": "collect_effectivedates_for_subinstrument","cat":"Array","dsl":'print(collect_effectivedates_for_subinstrument("1"))',"skip": True},
]


def run_test(tc, base_url, verbose):
    """POST dsl_code to /api/dsl/run and return (passed, output, error_msg)."""
    url = f"{base_url}/api/dsl/run"
    payload = json.dumps({"dsl_code": tc["dsl"]}).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        return False, None, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, None, str(e)

    if data.get("success") is False:
        err = data.get("error") or data.get("error_message") or "Unknown error"
        return False, None, err

    outputs = data.get("print_outputs") or []
    output = "\n".join(str(o) for o in outputs).strip()
    if not output:
        output = "(no output)"
    return True, output, None


def main():
    parser = argparse.ArgumentParser(description="Run all DSL function tests")
    parser.add_argument("--url", default=BASE_URL, help="Backend base URL")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show output for every test")
    parser.add_argument("--only", help="Comma-separated function names to run (e.g. pv,fv)")
    args = parser.parse_args()

    only_set = set(args.only.split(",")) if args.only else None

    cases = [t for t in TEST_CASES if not t.get("skip")]
    if only_set:
        cases = [t for t in cases if t["name"] in only_set]

    skipped = [t for t in TEST_CASES if t.get("skip")]

    print(f"\n{'='*70}")
    print(f"  Fyntrac DSL Function Test Runner")
    print(f"  Backend: {args.url}")
    print(f"  Tests:   {len(cases)}  |  Skipped (event-context): {len(skipped)}")
    print(f"{'='*70}\n")

    passed_list, failed_list = [], []
    current_cat = None

    for tc in cases:
        cat = tc["cat"]
        if cat != current_cat:
            current_cat = cat
            print(f"\n── {cat} {'─'*(50 - len(cat))}")

        t0 = time.perf_counter()
        passed, output, err = run_test(tc, args.url, args.verbose)
        ms = int((time.perf_counter() - t0) * 1000)

        if passed:
            passed_list.append(tc["name"])
            status = "✓"
            detail = f"→ {output}"
            line = f"  {status} {tc['name']:<35} {detail}  ({ms}ms)"
            print(line)
        else:
            failed_list.append({"name": tc["name"], "cat": cat, "error": err, "dsl": tc["dsl"]})
            status = "✗"
            line = f"  {status} {tc['name']:<35} ERROR: {err[:80]}  ({ms}ms)"
            print(line)

    # ── Summary ────────────────────────────────────────────────────────────────
    total = len(cases)
    n_pass = len(passed_list)
    n_fail = len(failed_list)

    print(f"\n{'='*70}")
    print(f"  RESULTS: {n_pass}/{total} passed", end="")
    print(f"  |  {n_fail} failed" if n_fail else "  |  ALL PASSED ✓")
    print(f"  Skipped (require events): {len(skipped)}")
    print(f"{'='*70}")

    if failed_list:
        print("\n── Failed tests ──────────────────────────────────────────────────────")
        for f in failed_list:
            print(f"\n  ✗ [{f['cat']}] {f['name']}")
            print(f"    Error : {f['error']}")
            print(f"    Code  : {f['dsl'][:120]}")

    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
