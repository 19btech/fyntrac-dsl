"""Security tests for the FyntracPythonModel export runtime (model_runner.py):
  - AST validator rejects disallowed imports + dunder access before exec
  - guarded __import__ blocks arbitrary modules in the sandbox
  - a legitimate generated template still validates AND executes end-to-end
  - a tampered/malicious template is rejected by run() (returns an error)

Run:  python tests/test_model_runner_security.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    from FyntracPythonModel.model_runner import (
        ModelRunner, _validate_template_ast, ModelSecurityError,
    )

    failures = []

    def check(cond, msg):
        print(f"  {'ok  ' if cond else 'FAIL'} - {msg}")
        if not cond:
            failures.append(msg)

    def raises(src):
        try:
            _validate_template_ast(src)
            return False
        except ModelSecurityError:
            return True

    # ── AST validator: legitimate scaffolding-style code passes ───────────
    legit = (
        "import sys, os\n"
        "from FyntracPythonModel.dsl_functions import DSL_FUNCTIONS\n"
        "from datetime import datetime\n"
        "import json\n"
        "globals().update(DSL_FUNCTIONS)\n"
        "def process_event_data(a, b=None, c=None, d=None):\n"
        "    return []\n"
    )
    try:
        _validate_template_ast(legit)
        check(True, "legit template passes AST validation")
    except ModelSecurityError as e:
        check(False, f"legit template wrongly rejected: {e}")

    # ── AST validator: malicious constructs rejected ──────────────────────
    check(raises("import subprocess\n"), "blocks `import subprocess`")
    check(raises("import socket as s\n"), "blocks `import socket`")
    check(raises("from os import system\n") is False,
          "`from os import system` allowed (os is needed by scaffolding) - "
          "covered by user-code validator at gen time, not here")
    check(raises("x = __import__('os')\n"), "blocks bare `__import__` name")
    check(raises("y = ().__class__.__bases__\n"), "blocks dunder attribute access")
    check(raises("z = __builtins__\n"), "blocks `__builtins__` reference")
    # allowed dunders stay allowed
    check(not raises("p = __file__\nq = __name__\n"),
          "allows __file__ / __name__")

    # ── run(): legitimate template validates, imports, and executes ───────
    runner = ModelRunner()
    good_code = (
        "from dsl_functions import DSL_FUNCTIONS\n"   # rewritten by _fix_import_paths
        "import json\n"
        "_print_outputs = []\n"
        "def get_print_outputs():\n"
        "    return _print_outputs\n"
        "def process_event_data(event_data, raw_event_data=None,"
        " override_postingdate=None, override_effectivedate=None):\n"
        "    return [{'postingdate': override_postingdate or '2026-01-01',\n"
        "             'effectivedate': '2026-01-01', 'instrumentid': 'L1',\n"
        "             'subinstrumentid': '1', 'transactiontype': 'INT',\n"
        "             'amount': 100.0}]\n"
    )
    res = runner.run(good_code, event_data=[{"instrumentid": "L1"}],
                     override_postingdate="2026-01-01")
    check(res["error"] is None, "legit template runs without error")
    check(len(res["transactions"]) == 1 and res["transactions"][0]["amount"] == 100.0,
          "legit template returns the expected transaction")

    # ── run(): malicious template is blocked (returns an error) ───────────
    bad_code = (
        "import subprocess\n"
        "def process_event_data(*a, **k):\n"
        "    return []\n"
    )
    res2 = runner.run(bad_code, event_data=[{"instrumentid": "L1"}],
                      override_postingdate="2026-01-01")
    check(res2["error"] is not None and "subprocess" in res2["error"],
          "malicious template blocked by run() with a clear error")
    check(res2["transactions"] == [], "blocked template produced no transactions")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s)")
        sys.exit(1)
    print("OK - model_runner security checks passed")


if __name__ == "__main__":
    main()
