#!/usr/bin/env python3
"""Verify backend/dsl_functions.py and FyntracPythonModel/dsl_functions.py are in sync.

Two layers of check:

    1. Byte-level diff ignoring whitespace and blank lines (catches drift in
       function bodies, signatures, docstrings, comments, registrations).
    2. DSL_FUNCTIONS registry-key set equality (catches the case where a
       function is renamed or added/removed from the public registry even if
       the source diff is hidden by an apparently-cosmetic change).

Exits non-zero on any drift so it can be wired into CI.
"""
from __future__ import annotations

import difflib
import importlib.util
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_PATH = os.path.join(ROOT, "backend", "dsl_functions.py")
FYNTRAC_PATH = os.path.join(ROOT, "FyntracPythonModel", "dsl_functions.py")


def _normalize(text: str) -> list[str]:
    """Strip trailing whitespace and drop blank lines for diffing."""
    out = []
    for line in text.splitlines():
        line = line.rstrip()
        if line:
            out.append(line)
    return out


def _load_dsl_functions(path: str, module_name: str) -> set[str]:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    funcs = getattr(module, "DSL_FUNCTIONS", None)
    if not isinstance(funcs, dict):
        raise RuntimeError(f"{path} did not expose a DSL_FUNCTIONS dict")
    return set(funcs.keys())


def main() -> int:
    if not os.path.isfile(BACKEND_PATH):
        print(f"ERROR: missing {BACKEND_PATH}", file=sys.stderr)
        return 2
    if not os.path.isfile(FYNTRAC_PATH):
        print(f"ERROR: missing {FYNTRAC_PATH}", file=sys.stderr)
        return 2

    with open(BACKEND_PATH, encoding="utf-8") as f:
        backend_src = f.read()
    with open(FYNTRAC_PATH, encoding="utf-8") as f:
        fyntrac_src = f.read()

    backend_lines = _normalize(backend_src)
    fyntrac_lines = _normalize(fyntrac_src)

    drift = False

    if backend_lines != fyntrac_lines:
        drift = True
        diff = difflib.unified_diff(
            backend_lines,
            fyntrac_lines,
            fromfile="backend/dsl_functions.py",
            tofile="FyntracPythonModel/dsl_functions.py",
            lineterm="",
            n=2,
        )
        print("DRIFT: source files differ (whitespace/blank-line ignored):")
        # Cap diff output so CI logs stay readable.
        for i, line in enumerate(diff):
            if i >= 200:
                print("... (diff truncated; run locally for full output) ...")
                break
            print(line)
    else:
        print("OK: source files are byte-equivalent (modulo whitespace/blank lines)")

    try:
        backend_keys = _load_dsl_functions(BACKEND_PATH, "_dsl_sync_backend")
        fyntrac_keys = _load_dsl_functions(FYNTRAC_PATH, "_dsl_sync_fyntrac")
    except Exception as exc:
        print(f"ERROR: could not import DSL_FUNCTIONS: {exc}", file=sys.stderr)
        return 2

    only_backend = sorted(backend_keys - fyntrac_keys)
    only_fyntrac = sorted(fyntrac_keys - backend_keys)
    if only_backend or only_fyntrac:
        drift = True
        print("DRIFT: DSL_FUNCTIONS registry keys differ:")
        if only_backend:
            print(f"  only in backend ({len(only_backend)}): {only_backend}")
        if only_fyntrac:
            print(f"  only in FyntracPythonModel ({len(only_fyntrac)}): {only_fyntrac}")
    else:
        print(f"OK: DSL_FUNCTIONS registries match ({len(backend_keys)} functions)")

    return 1 if drift else 0


if __name__ == "__main__":
    sys.exit(main())
