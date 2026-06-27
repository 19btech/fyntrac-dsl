"""
Model Runner for Fyntrac
========================
Executes DSL-generated Python templates against event data.

Accepts either:
  A) Pre-transformed data (event_data + raw_event_data)
  B) Raw import JSON (same format uploaded via Import in DSL Studio)

In case B, the transformer is called automatically.

Usage:
    from FyntracPythonModel.model_runner import ModelRunner

    runner = ModelRunner()
    result = runner.run_from_json(python_code, raw_json_records)
"""

import ast
import os
import re
from typing import Any, Dict, List, Optional

try:
    from FyntracPythonModel.data_transformer import transform
except ImportError:
    from data_transformer import transform


# ---------------------------------------------------------------------------
# Execution-sandbox security (mirrors backend/server.py)
# ---------------------------------------------------------------------------
# Modules a generated template legitimately imports (after import-path rewriting
# in _fix_import_paths). Used by BOTH the AST validator below and the guarded
# __import__ in _build_safe_builtins so the allow-list has a single source.
_ALLOWED_IMPORT_MODULES = frozenset({
    'sys', 'os', 'json', 'datetime', 'inspect',
    'dsl_functions', 'backend', 'backend.dsl_functions',
    'FyntracPythonModel', 'FyntracPythonModel.dsl_functions',
})

# Dunders the trusted scaffolding itself uses; everything else is blocked.
_ALLOWED_DUNDER_NAMES = frozenset({'__file__', '__name__'})


class ModelSecurityError(Exception):
    """Raised when a generated template contains a forbidden construct."""


def _validate_template_ast(source: str, label: str = '<dsl_template>') -> None:
    """Defense-in-depth: walk the AST of a generated template BEFORE exec and
    reject any import outside the allow-list, or any dunder reference/attribute
    outside the small allow-list (e.g. __import__, __class__, __subclasses__,
    __globals__). Mirrors backend/server.py._validate_template_ast so the export
    runtime is no more permissive than the playground that produced the code."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        # Let the caller's normal syntax-error handling surface the message.
        return

    def _is_forbidden_dunder(name: str) -> bool:
        if not isinstance(name, str):
            return False
        if not (name.startswith('__') and name.endswith('__') and len(name) > 4):
            return False
        return name not in _ALLOWED_DUNDER_NAMES

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = (alias.name or '').split('.')[0]
                if alias.name not in _ALLOWED_IMPORT_MODULES and root not in _ALLOWED_IMPORT_MODULES:
                    raise ModelSecurityError(
                        f"Disallowed import '{alias.name}' in {label}."
                    )
            continue
        if isinstance(node, ast.ImportFrom):
            module = node.module or ''
            root = module.split('.')[0]
            if module not in _ALLOWED_IMPORT_MODULES and root not in _ALLOWED_IMPORT_MODULES:
                raise ModelSecurityError(
                    f"Disallowed import 'from {module} import ...' in {label}."
                )
            continue
        if isinstance(node, ast.Name) and _is_forbidden_dunder(node.id):
            raise ModelSecurityError(
                f"Use of '{node.id}' is not allowed in {label}."
            )
        if isinstance(node, ast.Attribute) and _is_forbidden_dunder(node.attr):
            raise ModelSecurityError(
                f"Access to attribute '{node.attr}' is not allowed in {label}."
            )


class TransactionOutput:
    """Simple transaction container matching the playground's output shape."""
    __slots__ = ('postingdate', 'effectivedate', 'instrumentid',
                 'subinstrumentid', 'transactiontype', 'amount')

    def __init__(self, postingdate: str, effectivedate: str, instrumentid: str,
                 transactiontype: str, amount: float, subinstrumentid: str = '1', **kwargs):
        self.postingdate = str(postingdate)
        self.effectivedate = str(effectivedate)
        self.instrumentid = str(instrumentid)
        self.subinstrumentid = str(subinstrumentid) if subinstrumentid else '1'
        self.transactiontype = str(transactiontype)
        self.amount = float(amount)

    def to_dict(self) -> dict:
        return {
            'postingdate': self.postingdate,
            'effectivedate': self.effectivedate,
            'instrumentid': self.instrumentid,
            'subinstrumentid': self.subinstrumentid,
            'transactiontype': self.transactiontype,
            'amount': self.amount,
        }


class ModelRunner:
    """Runs a DSL-generated Python template against event data and returns transactions."""

    def __init__(self):
        self._this_dir = os.path.dirname(os.path.abspath(__file__))

    # ------------------------------------------------------------------
    # Import path rewriting
    # ------------------------------------------------------------------
    def _fix_import_paths(self, python_code: str) -> str:
        """
        Rewrite dsl_functions imports in the generated Python code so they
        resolve to the copy sitting in this package (FyntracPythonModel/).
        """
        python_code = python_code.replace(
            "from backend.dsl_functions import",
            "from FyntracPythonModel.dsl_functions import"
        )
        python_code = python_code.replace(
            "from dsl_functions import",
            "from FyntracPythonModel.dsl_functions import"
        )
        return python_code

    # ------------------------------------------------------------------
    # Safe execution sandbox
    # ------------------------------------------------------------------
    def _build_safe_builtins(self) -> dict:
        """Return a restricted __builtins__ dict that blocks dangerous operations
        and confines __import__ to the fixed module allow-list
        (_ALLOWED_IMPORT_MODULES) — defense in depth beyond blocking exec/eval/
        open, and beyond the AST validator."""
        import builtins
        blocked = {'exec', 'eval', 'compile', 'open', 'input', 'breakpoint'}
        safe = {}
        for name in dir(builtins):
            if name not in blocked:
                safe[name] = getattr(builtins, name)
        real_import = getattr(builtins, '__import__')

        def _guarded_import(name, _globals=None, _locals=None, fromlist=(), level=0):
            root = (name or '').split('.')[0]
            if name in _ALLOWED_IMPORT_MODULES or root in _ALLOWED_IMPORT_MODULES:
                return real_import(name, _globals, _locals, fromlist, level)
            raise ImportError(
                f"Import of '{name}' is not permitted in the model execution sandbox."
            )

        safe['__import__'] = _guarded_import
        return safe

    # ------------------------------------------------------------------
    # Error diagnostics
    # ------------------------------------------------------------------
    def _extract_dsl_line(self, python_code: str, exc: Exception) -> Optional[int]:
        """Find the DSL line number from a # DSL_LINE:N marker in the failing Python line."""
        try:
            tb = exc.__traceback__
            if tb is None:
                return None
            while tb.tb_next:
                tb = tb.tb_next
            py_lineno = tb.tb_lineno
            code_lines = python_code.split('\n')
            if 1 <= py_lineno <= len(code_lines):
                m = re.search(r'# DSL_LINE:(\d+)', code_lines[py_lineno - 1])
                if m:
                    return int(m.group(1))
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Core runner (pre-transformed data)
    # ------------------------------------------------------------------
    def run(
        self,
        python_code: str,
        event_data: List[Dict[str, Any]],
        raw_event_data: Optional[Dict[str, List[Dict]]] = None,
        override_postingdate: Optional[str] = None,
        override_effectivedate: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a generated Python template against pre-transformed event data.

        Args:
            python_code: The generated Python code string (from dsl_template_artifacts).
            event_data: List of merged row dicts — one per instrument.
            raw_event_data: Dict of event_name -> raw row lists (for collect() functions).
            override_postingdate: Optional override for posting date.
            override_effectivedate: Optional override for effective date.

        Returns:
            {
                "transactions": list of dicts,
                "print_outputs": list of strings,
                "error": None or error message string,
                "instrument_count": number of instruments processed
            }
        """
        try:
            python_code = self._fix_import_paths(python_code)

            # Defense-in-depth: AST-validate the (rewritten) template BEFORE exec
            # so a malformed/tampered artifact cannot import arbitrary modules or
            # reach dunder introspection, even though exec/eval/open are blocked.
            _validate_template_ast(python_code, label='<dsl_template>')

            exec_globals = {
                '__file__': os.path.abspath(__file__),
                '__name__': '__dsl_template__',
                '__builtins__': self._build_safe_builtins(),
            }

            # Compile and execute the template (defines process_event_data, etc.)
            exec(compile(python_code, '<dsl_template>', 'exec'), exec_globals)

            # Call the processing function. Inspect the signature explicitly so
            # we never swallow internal TypeErrors as a "wrong signature" — that
            # would cause the 3-arg fallback to bind raw_event_data =
            # override_postingdate (a string), corrupting global state and
            # producing a cryptic "'str' object has no attribute 'items'" later.
            if 'process_event_data' in exec_globals:
                import inspect as _inspect
                _proc = exec_globals['process_event_data']
                try:
                    _param_count = len(_inspect.signature(_proc).parameters)
                except (TypeError, ValueError):
                    _param_count = 4
                if _param_count >= 4:
                    transactions = _proc(
                        event_data, raw_event_data,
                        override_postingdate, override_effectivedate,
                    )
                else:
                    # Older template signature without raw_event_data
                    transactions = _proc(
                        event_data, override_postingdate, override_effectivedate,
                    )
            elif 'process_standalone' in exec_globals:
                transactions = exec_globals['process_standalone'](
                    override_postingdate, override_effectivedate
                )
            else:
                return {
                    "transactions": [],
                    "print_outputs": [],
                    "error": "Template did not define a process function",
                    "instrument_count": 0,
                }

            # Normalise transactions to plain dicts
            normalized = []
            for t in (transactions or []):
                try:
                    if isinstance(t, dict):
                        normalized.append(TransactionOutput(**t).to_dict())
                    elif hasattr(t, 'model_dump'):
                        normalized.append(t.model_dump())
                    elif hasattr(t, '__dict__'):
                        normalized.append(TransactionOutput(**t.__dict__).to_dict())
                except Exception:
                    pass

            # Collect print outputs
            print_outputs = []
            if 'get_print_outputs' in exec_globals:
                try:
                    print_outputs = exec_globals['get_print_outputs']()
                except Exception:
                    pass

            return {
                "transactions": normalized,
                "print_outputs": print_outputs,
                "error": None,
                "instrument_count": len(event_data),
            }

        except Exception as e:
            dsl_line = self._extract_dsl_line(python_code, e)
            error_msg = str(e)
            if dsl_line:
                error_msg = f"[DSL Line {dsl_line}] {error_msg}"
            return {
                "transactions": [],
                "print_outputs": [],
                "error": error_msg,
                "instrument_count": 0,
            }

    # ------------------------------------------------------------------
    # Convenience: raw JSON → transform → run (all instruments)
    # ------------------------------------------------------------------
    def run_from_json(
        self,
        python_code: str,
        raw_json_records: list,
        posting_date: str,
        effective_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        End-to-end: takes the raw import JSON (same format as DSL Studio Import),
        transforms it, and runs the model for the given posting date across
        ALL instruments that have data for that date.

        Args:
            python_code: The generated Python code string (from dsl_template_artifacts).
            raw_json_records: The raw JSON array — same format as uploaded to
                             DSL Studio's Import functionality.
            posting_date: Required. Only instruments with this posting date are processed.
            effective_date: Optional override for effective date.

        Returns: Same shape as run().
        """
        if not posting_date or not posting_date.strip():
            return {
                "transactions": [],
                "print_outputs": [],
                "error": "posting_date is required. Specify which posting date to process.",
                "instrument_count": 0,
            }
        try:
            event_data, raw_event_data = transform(raw_json_records, posting_date)
        except ValueError as e:
            return {
                "transactions": [],
                "print_outputs": [],
                "error": f"Data transformation error: {e}",
                "instrument_count": 0,
            }

        return self.run(
            python_code=python_code,
            event_data=event_data,
            raw_event_data=raw_event_data,
            override_postingdate=posting_date,
            override_effectivedate=effective_date,
        )
