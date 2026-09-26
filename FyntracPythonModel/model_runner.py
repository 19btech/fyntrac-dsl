"""
Model Runner for Fyntrac
========================
Executes DSL-generated Python templates against event data.

Accepts either:
  A) Pre-transformed data (event_data + raw_event_data)
  B) Raw import JSON (same format uploaded via Import in DSL Studio)

In case B, the transformer is called automatically.

Usage:
    from app.python_model.model_runner import ModelRunner

    runner = ModelRunner()
    result = runner.run_from_json(python_code, raw_json_records, posting_date='2023-01-01')
"""

import ast
import functools
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Missing-field tolerance for the generated collect_* helpers
# ---------------------------------------------------------------------------
# The collect_* family is emitted INTO the template by DSL Studio (they appear
# as "<dsl_template>" frames in tracebacks), so they cannot be changed here.
# They raise ValueError when a referenced field is absent from every loaded
# event. That is correct for a genuine typo, but it also hard-fails the
# legitimate first-period case where a balance event exists with no balance
# columns yet -- e.g.
#     REVENUE_BALANCE(EffectiveDate, InstrumentId, PostingDate, SubInstrumentId)
# The template's functions resolve their own names through exec_globals (that
# dict IS their module namespace), so rebinding an entry there also intercepts
# the template's internal calls.
_COLLECT_FUNCTION_NAMES = (
    'collect',
    'collect_all',
    'collect_by_instrument',
    'collect_by_subinstrument',
    'collect_effectivedates_for_subinstrument',
    'collect_subinstrumentids',
)

# Matched against the message so ONLY the missing-field case is softened;
# every other ValueError from the template still propagates.
_MISSING_FIELD_MARKER = 'no loaded event supplies a field named'

# Default ON. Set FYNTRAC_LENIENT_COLLECT=false to restore hard failures.
_LENIENT_COLLECT = os.getenv(
    'FYNTRAC_LENIENT_COLLECT', 'true'
).strip().lower() not in ('0', 'false', 'no', 'off')


def _make_collect_lenient(fn, name: str):
    """Wrap a generated collect_* helper so a missing field yields no rows
    instead of aborting the whole instrument.

    Returns an EMPTY _RowAwareArray (dsl_functions' hybrid list/scalar type,
    the same one schedule() injects for context arrays). It is still an empty
    sequence -- len() == 0, iterates as empty, falsy -- but it also answers 0
    in arithmetic, so a template doing a raw `total + prior_balance` on the
    collected value gets 0 instead of
    "TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'".

    A plain [] was not enough: array_get([], i) / array_first([]) /
    array_last([]) default to None, and the generated template performs raw
    Python arithmetic on the result. Note this only covers the value itself --
    if the template extracts with array_get(x, i) and NO default, that call
    still yields None and the fix belongs in the model, not here.

    Every substitution is logged at WARNING with the original message, so a
    real typo stays visible instead of silently becoming zero.
    """
    @functools.wraps(fn)
    def _lenient(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError as e:
            if _MISSING_FIELD_MARKER not in str(e):
                raise
            _ref = args[0] if args else kwargs.get('field', '?')
            logger.warning(
                "%s(%r): field not present in any loaded event — substituting an "
                "empty result for this instrument. If this is not the expected "
                "first-period/no-data case, the reference is wrong. Detail: %s",
                name, _ref, str(e).split('. Loaded events:')[0],
            )
            try:
                return _dsl_functions()._RowAwareArray([], row_value=0)
            except Exception:
                return []
    _lenient.__wrapped_by_fyntrac__ = True
    return _lenient


def _describe_template_failure(python_code: str, exc: Exception, max_names: int = 40):
    """Pinpoint a failure that happened inside the generated template.

    A traceback frame gives a line NUMBER in "<dsl_template>", but that file
    exists only in memory, so the log shows no source and no variable names --
    which makes an error like "int + NoneType" impossible to act on. We still
    hold the template text in `python_code`, so print the offending line and
    name the locals that are None in that frame.

    Only variable NAMES are logged (plus types for non-None operands on the
    line), never values, so no business data reaches the log.
    """
    try:
        frames = []
        tb = exc.__traceback__
        while tb:
            frames.append(tb)
            tb = tb.tb_next
        target = None
        for t in reversed(frames):
            if t.tb_frame.f_code.co_filename == '<dsl_template>':
                target = t
                break
        if target is None:
            return None
        lineno = target.tb_lineno
        lines = (python_code or '').split('\n')
        src = lines[lineno - 1].strip() if 1 <= lineno <= len(lines) else '<unavailable>'
        loc = target.tb_frame.f_locals
        none_names = sorted(
            k for k, v in loc.items() if v is None and not k.startswith('__')
        )
        # Types of the identifiers that actually appear on the failing line --
        # narrows it down when several locals are None.
        on_line = sorted({
            k for k in loc
            if not k.startswith('__') and re.search(r'\b%s\b' % re.escape(k), src)
        })
        types_on_line = ', '.join(
            f"{k}={type(loc[k]).__name__}" for k in on_line[:max_names]
        )
        return lineno, src, none_names[:max_names], types_on_line
    except Exception:
        return None


def _apply_collect_leniency(exec_globals: dict) -> None:
    """Rebind the template's collect_* helpers in place (no-op if disabled)."""
    if not _LENIENT_COLLECT:
        return
    for _name in _COLLECT_FUNCTION_NAMES:
        _fn = exec_globals.get(_name)
        if callable(_fn) and not getattr(_fn, '__wrapped_by_fyntrac__', False):
            exec_globals[_name] = _make_collect_lenient(_fn, _name)

try:
    from app.python_model.data_transformer import transform
except ImportError:
    try:
        from data_transformer import transform
    except ImportError:
        # Last resort: this package's own directory is not on sys.path, which is
        # the case whenever the folder is imported as a package
        # (FyntracPythonModel.model_runner) rather than from inside it. Resolve
        # against __file__ so the runtime is genuinely drop-in in any host
        # layout. Mirrors the same fallback in data_transformer.py.
        import os as _os
        import sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
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
    'app', 'app.python_model', 'app.python_model.dsl_functions',
})

# Dunders the trusted scaffolding itself uses; everything else is blocked.
_ALLOWED_DUNDER_NAMES = frozenset({'__file__', '__name__'})


@functools.lru_cache(maxsize=1)
def _dsl_functions():
    """The dsl_functions MODULE OBJECT that generated templates import.

    It must be the same object the template got, not merely an equivalent copy.
    State in that module lives in module-level thread-local storage, so a second
    import creates a second, empty store: the zero-amount counter read back 0
    while the template had been incrementing the other instance, and a lenient
    collect handed back a _RowAwareArray from a different class object.
    """
    import importlib
    return importlib.import_module(_resolve_dsl_functions_module())


@functools.lru_cache(maxsize=1)
def _resolve_dsl_functions_module() -> str:
    """The importable module path for this package's dsl_functions copy.

    Checked once per process, in host-layout order. Every candidate is already
    on _ALLOWED_IMPORT_MODULES, so resolving here cannot widen what a template
    is permitted to import.
    """
    import importlib.util
    for name in ("app.python_model.dsl_functions",
                 "FyntracPythonModel.dsl_functions",
                 "dsl_functions"):
        try:
            if importlib.util.find_spec(name) is not None:
                return name
        except (ImportError, ValueError, AttributeError):
            continue
    # Nothing resolved: keep the host-app path so the error names the layout
    # the runtime expects rather than something incidental.
    return "app.python_model.dsl_functions"


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


# Template source -> (post-processed source, compiled code object). compile_template() runs once per
# instrument (each gets its own namespace), and the source -> code step -- import rewriting, field
# injection, the full-AST safety check, compile() -- is a pure function of the source that took ~25%
# of per-instrument model time on Hearst. Keyed by source, so a changed model file is recompiled.
_COMPILED_TEMPLATES: Dict[str, tuple] = {}
_COMPILED_TEMPLATES_MAX = 16


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
        resolve to the copy sitting in this package (app/python_model/).

        The target is resolved to whichever candidate path actually imports,
        rather than hardcoding `app.python_model`. In the host app that IS the
        answer, so behaviour there is unchanged; anywhere else the old rewrite
        produced "No module named 'app'" for every model. That made the
        playground-vs-production parity check -- the one test that verifies a
        deployed model behaves exactly as it did in the playground -- impossible
        to run outside the host, which is precisely where it is needed.
        """
        target = _resolve_dsl_functions_module()
        for candidate in ("from backend.dsl_functions import",
                          "from FyntracPythonModel.dsl_functions import",
                          "from dsl_functions import"):
            python_code = python_code.replace(
                candidate, "from %s import" % target)
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

        # `sum` must follow DSL semantics, not Python's.
        #
        # The DSL defines sum as sum_vals, which routes every element through
        # to_number() and so treats None / '' / 'None' as 0. That binding is
        # already used in the DSL_FUNCTIONS registry and in schedule()'s column
        # eval context ("sum": sum_vals). Generated template code, however,
        # calls a bare sum(...) at Python level, which resolved to the builtin
        # and raised on any None element:
        #     sum(balance_row_count) ->
        #     TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
        # even though the same expression evaluates fine inside a schedule
        # column. Bind it here so the template sees one consistent `sum`.
        try:
            _dslf = _dsl_functions()
            _sum_vals, _to_number = _dslf.sum_vals, _dslf.to_number

            def _dsl_sum(iterable, start=0):
                """DSL sum: None/blank elements count as 0 (mirrors sum_vals)."""
                return _to_number(start) + _sum_vals(iterable)

            _dsl_sum.__name__ = 'sum'
            safe['sum'] = _dsl_sum
        except Exception:
            # dsl_functions unavailable -> leave the builtin in place.
            pass

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
            if python_code is None:
                return None
            code_lines = python_code.split('\n')
            if 1 <= py_lineno <= len(code_lines):
                m = re.search(r'# DSL_LINE:(\d+)', code_lines[py_lineno - 1])
                if m:
                    return int(m.group(1))
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # DSL code pre-processing
    # ------------------------------------------------------------------
    def _remove_self_assignments(self, python_code: str) -> str:
        """
        Remove no-op self-assignments generated by the DSL playground, e.g.:
            EOD_postingdate = EOD_postingdate
        These cause an UnboundLocalError because Python marks the LHS name
        as a local variable for the whole function scope, making the RHS
        reference undefined before assignment.
        """
        return re.sub(
            r'^([ \t]*)([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\2\s*$',
            r'\1# (removed self-assignment: \2)',
            python_code,
            flags=re.MULTILINE,
        )

    def _inject_missing_field_extractions(self, python_code: str) -> str:
        """
        Auto-inject get_field_case_insensitive() extraction lines for any
        EVENT_FIELD variables (ALL_CAPS_WITH_UNDERSCORE) that are referenced
        in the DSL template but have no extraction line generated by the playground.

        The DSL playground sometimes generates DEFAULT_xxx extraction lines but
        forgets to generate the corresponding INT_ACC_xxx or EOD_xxx lines even
        though the logic block uses them.  This pre-processor detects and fixes
        those gaps before the code is compiled.
        """
        # --- 1. Variables already assigned anywhere in the code ---
        assigned = set(re.findall(
            r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?!=)',
            python_code,
            flags=re.MULTILINE,
        ))

        # --- 2. All identifiers that look like event-prefixed fields ---
        # Matches: EVENTID_fieldname  (e.g. INT_ACC_rate, EOD_BILLING,
        #          INT_ACC_BALANCES_Unpaid_Principal_Balance)
        # Must start with an uppercase letter and contain at least one underscore.
        used = set(re.findall(r'\b([A-Z][A-Za-z0-9_]{2,})\b', python_code))

        # --- 3. Skip Python constants and very generic names ---
        _SKIP = {'None', 'True', 'False', 'NULL', 'NAN', 'INF'}

        missing = {
            name for name in used
            if '_' in name and name not in assigned and name not in _SKIP
        }

        if not missing:
            return python_code

        # --- 4. Build injection lines (best-effort type inference) ---
        _DATE_SUFFIXES = ('date', '_id', 'name', 'code', 'type', 'description', 'text')
        injection_lines = ["        # Auto-injected: missing event field extractions"]
        for var in sorted(missing):
            vl = var.lower()
            if any(vl.endswith(s) for s in _DATE_SUFFIXES):
                injection_lines.append(
                    f"        {var} = str(get_field_case_insensitive(row, '{var}', '') or '')"
                )
            else:
                injection_lines.append(
                    f"        {var} = get_field_case_insensitive(row, '{var}', 0) or 0"
                )

        # --- 5. Inject before the "# Execute DSL logic" section ---
        marker = "        # Execute DSL logic"
        if marker in python_code:
            block = "\n".join(injection_lines) + "\n\n"
            python_code = python_code.replace(marker, block + marker, 1)

        return python_code

    # ------------------------------------------------------------------
    def compile_template(self, python_code: str) -> dict:
        """Compile the DSL template once and return the exec_globals dict containing the executable functions."""
        try:
            cached = _COMPILED_TEMPLATES.get(python_code)
            if cached is None:
                source = python_code
                python_code = self._fix_import_paths(python_code)
                python_code = self._remove_self_assignments(python_code)
                python_code = self._inject_missing_field_extractions(python_code)
                _validate_template_ast(python_code, label='<dsl_template>')
                code = compile(python_code, '<dsl_template>', 'exec')
                if len(_COMPILED_TEMPLATES) >= _COMPILED_TEMPLATES_MAX:
                    _COMPILED_TEMPLATES.clear()
                _COMPILED_TEMPLATES[source] = (python_code, code)
            else:
                python_code, code = cached
            # Always a FRESH namespace: only the source->code step above is reused, so one
            # instrument's module-level state can never reach the next one's.
            exec_globals = {
                '__file__': os.path.abspath(__file__),
                '__name__': '__dsl_template__',
                '__builtins__': self._build_safe_builtins(),
            }
            exec(code, exec_globals)
            # Soften missing-field failures in the template's collect_* helpers.
            _apply_collect_leniency(exec_globals)
            # Keep the POST-PROCESSED source. _inject_missing_field_extractions
            # inserts lines, so "<dsl_template>" line numbers in a traceback
            # refer to THIS text, not to the original artifact the caller holds.
            # Resolving a traceback against the original yields the wrong line
            # (often a blank one) and mis-numbered # DSL_LINE markers.
            exec_globals['__fyntrac_source__'] = python_code
            return exec_globals
        except Exception as e:
            dsl_line = self._extract_dsl_line(python_code, e)
            error_msg = str(e)
            if dsl_line:
                error_msg = f"[DSL Line {dsl_line}] {error_msg}"
            raise ValueError(f"Template compilation failed: {error_msg}")

    # ------------------------------------------------------------------
    # Core runner (pre-transformed data)
    # ------------------------------------------------------------------
    def run(
        self,
        python_code: Optional[str] = None,
        event_data: List[Dict[str, Any]] = None,
        raw_event_data: Optional[Dict[str, List[Dict]]] = None,
        override_postingdate: Optional[str] = None,
        override_effectivedate: Optional[str] = None,
        exec_globals: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Execute a generated Python template against pre-transformed event data.

        Args:
            python_code: The generated Python code string. Optional if exec_globals is provided.
            event_data: List of merged row dicts — one per instrument.
            raw_event_data: Dict of event_name -> raw row lists (for collect() functions).
            override_postingdate: Optional override for posting date.
            override_effectivedate: Optional override for effective date.
            exec_globals: Pre-compiled template globals.

        Returns:
            {
                "transactions": list of dicts,
                "print_outputs": list of strings,
                "error": None or error message string,
                "instrument_count": number of instruments processed
            }
        """
        try:
            if exec_globals is None:
                if not python_code:
                    raise ValueError("Must provide either python_code or exec_globals")
                exec_globals = self.compile_template(python_code)

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
                # process_standalone returns (transactions, print_outputs)
                # whereas process_event_data returns just the transactions.
                # Treating the tuple as a list of transactions meant EVERY
                # standalone model (one with no events) came back with zero
                # transactions — both entries failed conversion below and were
                # dropped silently. Mirrors the fix in
                # backend/server.py::execute_python_template.
                if isinstance(transactions, tuple):
                    transactions = transactions[0] if transactions else []
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

            # createTransaction() now suppresses zero-amount transactions at
            # the source rather than generating them and relying on the
            # caller to discard them before persistence. Report the count so
            # a batch whose row count is lower than its input can explain
            # the gap instead of it looking like missing data.
            zero_skipped = 0
            try:
                # Read from the SAME module instance the template incremented --
                # see _dsl_functions(). A second import has its own empty
                # thread-local store and always reported 0.
                zero_skipped = _dsl_functions()._get_skipped_zero_amount()
            except Exception:
                zero_skipped = 0

            return {
                "transactions": normalized,
                "print_outputs": print_outputs,
                "zero_amount_skipped": zero_skipped,
                "error": None,
                "instrument_count": len(event_data),
            }

        except Exception as e:
            # Resolve line numbers against the source that was actually
            # compiled (see __fyntrac_source__), falling back to the caller's
            # copy when compilation never got that far.
            _src = (exec_globals or {}).get('__fyntrac_source__') or python_code
            dsl_line = self._extract_dsl_line(_src, e)
            error_msg = str(e)
            if dsl_line:
                error_msg = f"[DSL Line {dsl_line}] {error_msg}"
            # The returned dict carries only str(e), which for a bare
            # KeyError/NameError inside a generated template is often a single
            # opaque token (e.g. "collect") with no indication of where it came
            # from. Log the real traceback so the failing line is recoverable.
            logger.error(
                "Model execution failed (%s): %s", type(e).__name__, error_msg,
                exc_info=True,
            )
            _info = _describe_template_failure(_src, e)
            if _info:
                _ln, _src, _nones, _types = _info
                logger.error("  template line %d: %s", _ln, _src)
                logger.error("  operands on that line: %s", _types or '(none resolved)')
                logger.error("  locals that are None here: %s",
                             ', '.join(_nones) if _nones else '(none)')
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
        python_code: Optional[str],
        raw_json_records: list,
        posting_date: str,
        effective_date: Optional[str] = None,
        exec_globals: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        End-to-end: takes the raw import JSON (same format as DSL Studio Import),
        transforms it, and runs the model for the given posting date across
        ALL instruments that have data for that date.

        Args:
            python_code: The generated Python code string. Optional if exec_globals provided.
            raw_json_records: The raw JSON array — same format as uploaded to
                             DSL Studio's Import functionality.
            posting_date: Required. Only instruments with this posting date are processed.
            effective_date: Optional override for effective date.
            exec_globals: Pre-compiled template globals.

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
            exec_globals=exec_globals,
        )
