"""Make both runtimes importable from the repo root.

FyntracPythonModel/ is written to live inside the main Fyntrac app, so its
modules import each other by the names they have there:

    try:    from app.python_model.dsl_functions import normalize_date
    except: from dsl_functions import normalize_date

Neither resolves when pytest runs from this repo, so importing
FyntracPythonModel.data_transformer raised ModuleNotFoundError at COLLECTION
time. Every test that touches the export runtime was therefore dead --
including the ones written specifically to catch datatype-inference drift --
and they failed silently as collection errors rather than as failures.

Putting FyntracPythonModel/ on sys.path satisfies the bare-name fallback
without changing the shipped files. `backend` keeps importing its own copy via
the fully-qualified `backend.dsl_functions`, so the two stay distinct.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_RUNTIME = os.path.join(ROOT, "FyntracPythonModel")

for path in (ROOT, EXPORT_RUNTIME):
    if path not in sys.path:
        sys.path.insert(0, path)
