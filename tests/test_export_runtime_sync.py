"""FyntracPythonModel must stay aligned with the backend it is exported from.

FyntracPythonModel/ is the runtime shipped into the main Fyntrac app to run
exported models. Its README says dsl_functions.py "is a copy of the same file
from the playground" — and a hand-maintained copy drifts silently. It had fallen
470 lines behind, missing every engine fix, so a model verified in the playground
behaved differently in production.

These tests fail the moment the copy diverges again.

Two divergences are DELIBERATE and allowlisted below:

  * filter_event_data_by_posting_date sorts rows into canonical order in the
    export runtime. The backend does that at ingestion time instead
    (server.py calls _sort_activity_rows when data is imported); the export
    runtime receives raw event JSON from the host app and has no ingestion
    step, so it must sort at filter time or collect_* alignment is
    order-dependent.
"""
import ast
import io
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BACKEND_DSL = os.path.join(ROOT, "backend", "dsl_functions.py")
EXPORT_DSL = os.path.join(ROOT, "FyntracPythonModel", "dsl_functions.py")
BACKEND_SRV = os.path.join(ROOT, "backend", "server.py")
EXPORT_DT = os.path.join(ROOT, "FyntracPythonModel", "data_transformer.py")
EXPORT_RUNNER = os.path.join(ROOT, "FyntracPythonModel", "model_runner.py")


def _read(path):
    return io.open(path, encoding="utf-8").read()


# -- dsl_functions.py is a verbatim copy -----------------------------------
def test_dsl_functions_is_byte_identical():
    """Any engine change must be copied across in the same commit."""
    backend = io.open(BACKEND_DSL, "rb").read()
    export = io.open(EXPORT_DSL, "rb").read()
    assert backend == export, (
        "FyntracPythonModel/dsl_functions.py has drifted from "
        "backend/dsl_functions.py. It is a verbatim copy — re-copy it:\n"
        "  cp backend/dsl_functions.py FyntracPythonModel/dsl_functions.py"
    )


def test_export_dsl_functions_has_no_backend_imports():
    """The copy must stay importable standalone inside the host app."""
    tree = ast.parse(_read(EXPORT_DSL))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            imported.add(node.module.split(".")[0])
    assert not (imported & {"backend", "fastapi", "motor", "pymongo", "pydantic"}), (
        f"export copy pulled in backend-only imports: {sorted(imported)}")


# -- shared transformer helpers -------------------------------------------
def _fn_logic(src_text, name):
    """Function body as normalised source, docstrings removed."""
    for node in ast.walk(ast.parse(src_text)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            norm = ast.parse(ast.unparse(node))
            for sub in ast.walk(norm):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
                    if (sub.body and isinstance(sub.body[0], ast.Expr)
                            and isinstance(sub.body[0].value, ast.Constant)
                            and isinstance(sub.body[0].value.value, str)):
                        sub.body.pop(0)
            return ast.unparse(norm)
    return None


# Deliberate divergences — see the module docstring.
ALLOWED_DIVERGENCE = {"filter_event_data_by_posting_date"}

SHARED_HELPERS = [
    "get_field_case_insensitive",
    "get_latest_data_per_instrument",
    "merge_event_data_by_instrument",
]


@pytest.mark.parametrize("fn", SHARED_HELPERS)
def test_shared_transformer_helpers_match(fn):
    backend = _fn_logic(_read(BACKEND_SRV), fn)
    export = _fn_logic(_read(EXPORT_DT), fn)
    assert backend is not None, f"{fn} missing from server.py"
    assert export is not None, f"{fn} missing from data_transformer.py"
    assert backend == export, (
        f"{fn}() has drifted between backend/server.py and "
        f"FyntracPythonModel/data_transformer.py")


def test_the_allowlisted_divergence_is_still_the_only_one():
    """If the sort moves into the backend, drop it from the allowlist."""
    backend = _fn_logic(_read(BACKEND_SRV), "filter_event_data_by_posting_date")
    export = _fn_logic(_read(EXPORT_DT), "filter_event_data_by_posting_date")
    assert backend is not None and export is not None
    assert "_sort_activity_rows" in export, (
        "the export runtime has no ingestion step, so it must sort at filter time")
    assert "filter_event_data_by_posting_date" in ALLOWED_DIVERGENCE


# -- the standalone tuple bug must not come back --------------------------
def test_model_runner_unpacks_the_standalone_tuple():
    """process_standalone returns (transactions, print_outputs). Treating the
    tuple as a list of transactions made every event-less model return none."""
    src = _read(EXPORT_RUNNER)
    assert "process_standalone" in src
    assert "isinstance(transactions, tuple)" in src, (
        "model_runner.py must unpack process_standalone's (transactions, "
        "print_outputs) tuple — see backend/server.py::execute_python_template")


# -- the export runtime carries this session's engine fixes ---------------
@pytest.mark.parametrize("marker,what", [
    ("_date_part_before", "normalize_date no longer truncates non-dates"),
    ("_broadcast_compare", "eq/neq/gt vectorise and are date-aware"),
    ("_iteration_context", "loop bindings cannot be shadowed"),
    ("_is_empty_seq", "row-aware arrays are not mistaken for empty"),
    ("_skipped_zero_amount", "zero-amount transactions are not persisted"),
    ("PER-ITEM FAN-OUT", "per-line schedule fan-out"),
])
def test_engine_fixes_reached_the_export_runtime(marker, what):
    assert marker in _read(EXPORT_DSL), f"export runtime is missing: {what}"

# -- end-to-end: the same model must produce the same transactions ---------
def _run_both(code, merged, raw):
    """Execute one generated model through the backend and through the
    embedded export runtime."""
    import asyncio
    import importlib.util as iu
    from backend.server import execute_python_template

    loop = asyncio.new_event_loop()
    try:
        a = loop.run_until_complete(
            execute_python_template(code, merged, raw, None, None))
    finally:
        loop.close()

    spec = iu.spec_from_file_location("_mr", EXPORT_RUNNER)
    mr = iu.module_from_spec(spec)
    sys.modules["_mr"] = mr
    spec.loader.exec_module(mr)
    b = mr.ModelRunner().run(code, merged, raw, None, None)
    return a, b


def _norm(txns):
    out = []
    for t in txns:
        g = (lambda k: t.get(k) if isinstance(t, dict) else getattr(t, k, None))
        out.append((g("instrumentid"), g("subinstrumentid"), g("transactiontype"),
                    round(float(g("amount") or 0), 4), g("postingdate")))
    return sorted(out)


EVT = "SO_EVENT"
PARITY_FIELDS = {EVT: {"eventType": "activity", "fields": [
    {"name": "amt", "datatype": "decimal"}]}}
PARITY_ROWS = [{"instrumentid": "I1", "subinstrumentid": str(i + 1),
                "postingdate": "2026-01-31", "effectivedate": "2026-01-31",
                "amt": a} for i, a in enumerate([960.0, 0.0, 400.0])]


def test_both_runtimes_produce_identical_transactions():
    """A model deployed into the host app must behave exactly as it does in the
    playground — that is the entire purpose of this folder."""
    from backend.server import (dsl_to_python_multi_event,
                                merge_event_data_by_instrument)
    dsl = "\n".join([
        f"amts = collect_by_instrument({EVT}.amt)",
        f"subs = collect_by_instrument({EVT}.subinstrumentid)",
        f'createTransaction({EVT}.postingdate, {EVT}.effectivedate, '
        f'"Rev", amts, subs)',
        "",
    ])
    code = dsl_to_python_multi_event(dsl, PARITY_FIELDS)
    merged = merge_event_data_by_instrument({EVT: [dict(r) for r in PARITY_ROWS]})
    raw = {EVT: [dict(r) for r in PARITY_ROWS]}
    a, b = _run_both(code, merged, raw)
    assert b.get("error") is None, b.get("error")
    assert _norm(a["transactions"]) == _norm(b["transactions"])
    # result contract, not just the numbers
    assert a.get("zero_amount_skipped") == b.get("zero_amount_skipped") == 1


def test_both_sandboxes_expose_the_same_builtins():
    """A model that runs in the playground must not hit a missing builtin in
    production, or vice versa."""
    import importlib.util as iu
    import backend.server as S
    spec = iu.spec_from_file_location("_mr2", EXPORT_RUNNER)
    mr = iu.module_from_spec(spec)
    sys.modules["_mr2"] = mr
    spec.loader.exec_module(mr)
    a = set(S._make_sandbox_builtins().keys())
    b = set(mr.ModelRunner()._build_safe_builtins().keys())
    assert a == b, (f"sandbox drift — backend only: {sorted(a - b)}, "
                    f"export only: {sorted(b - a)}")
    for blocked in ("eval", "exec", "open", "compile", "input"):
        assert blocked not in a and blocked not in b
