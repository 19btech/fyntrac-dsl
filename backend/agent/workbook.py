"""Excel workbook analysis for the agent's model-import workflow.

Phase 1 — storage + inspection: uploaded .xlsx files live on disk under
`uploads/workbooks/` next to this module ({workbook_id}.xlsx plus a
{workbook_id}.meta.json sidecar holding the original filename and the
user-confirmed sheet roles). The directory scan IS the registry — no DB
dependency, so these functions work identically in Mongo and in-memory mode.

Phase 2 — formula understanding: `sheet_formula_patterns` deduplicates the
sheet's formulas by normalising every cell reference to relative R1C1 form
(a column of 10,000 dragged-down formulas collapses to ONE pattern), and
renders a "friendly" version with column headers substituted for cell
references so the LLM reasons over `principal * annual_rate / 12` instead of
`=$B2*C$1`. `dependency_graph` lifts per-cell references to column-level
edges (Sheet!header -> Sheet!header) — the workbook's data flow.

Everything here is synchronous and side-effect-free apart from the explicit
save/delete/set-role entry points; the async agent tools in tools.py are thin
wrappers. No LLM access, no bridge access.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

UPLOAD_DIR = Path(__file__).parent / "uploads" / "workbooks"

MAX_WORKBOOK_BYTES = 15 * 1024 * 1024      # refuse uploads beyond 15 MB
MAX_SCAN_CELLS_PER_SHEET = 200_000         # analysis truncates beyond this

VALID_ROLES = ("input", "calc", "output", "reference", "ignore")


class WorkbookError(Exception):
    """User-facing failure (bad id, bad sheet, corrupt file, ...)."""


# ──────────────────────────────────────────────────────────────────────────
# Storage / registry
# ──────────────────────────────────────────────────────────────────────────

def _xlsx_path(workbook_id: str) -> Path:
    return UPLOAD_DIR / f"{workbook_id}.xlsx"


def _meta_path(workbook_id: str) -> Path:
    return UPLOAD_DIR / f"{workbook_id}.meta.json"


def _read_meta(workbook_id: str) -> dict:
    try:
        return json.loads(_meta_path(workbook_id).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_meta(workbook_id: str, meta: dict) -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _meta_path(workbook_id).write_text(
        json.dumps(meta, indent=2, default=str), encoding="utf-8"
    )


def save_workbook_bytes(filename: str, content: bytes) -> dict:
    """Persist an uploaded workbook; returns its registry entry."""
    name = (filename or "workbook.xlsx").strip()
    if not name.lower().endswith(".xlsx"):
        raise WorkbookError("Only .xlsx files are supported (not .xls/.csv)")
    if not content:
        raise WorkbookError("Uploaded file is empty")
    # .xlsx is a ZIP container — every valid one starts with the ZIP magic
    # bytes. This rejects a non-Excel file renamed to .xlsx (e.g. a .csv or
    # .txt) up front with a clear message, before openpyxl chokes on it.
    if content[:4] not in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"):
        raise WorkbookError(
            "This file is not a valid .xlsx workbook (it is not an Excel ZIP "
            "container — it may be a .csv, .xls, or renamed file). Re-save it "
            "as .xlsx in Excel and try again."
        )
    if len(content) > MAX_WORKBOOK_BYTES:
        raise WorkbookError(
            f"File is {len(content) // (1024 * 1024)} MB — the limit is "
            f"{MAX_WORKBOOK_BYTES // (1024 * 1024)} MB"
        )

    # Re-uploading the same bytes returns the existing entry instead of
    # minting a second id — duplicate copies confuse the agent about which
    # workbook to analyse (and would fork the recorded sheet roles).
    sha256 = hashlib.sha256(content).hexdigest()
    for existing in list_workbooks():
        if existing.get("sha256") == sha256 and \
                _xlsx_path(existing.get("workbook_id", "")).is_file():
            existing["duplicate_of_existing"] = True
            return existing

    workbook_id = uuid.uuid4().hex[:12]
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _xlsx_path(workbook_id).write_bytes(content)

    # Validate it actually opens as a workbook; delete on failure.
    try:
        wb_f, _ = _load(workbook_id)
        sheets = list(wb_f.sheetnames)
    except WorkbookError:
        raise
    except Exception as exc:
        _xlsx_path(workbook_id).unlink(missing_ok=True)
        raise WorkbookError(f"File could not be opened as an Excel workbook: {exc}")

    meta = {
        "workbook_id": workbook_id,
        "filename": name,
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
        "size_bytes": len(content),
        "sha256": sha256,
        "sheets": sheets,
        "roles": {},           # sheet name -> input|calc|output|reference|ignore
    }
    _write_meta(workbook_id, meta)
    return meta


def list_workbooks() -> list[dict]:
    if not UPLOAD_DIR.is_dir():
        return []
    out = []
    for p in sorted(UPLOAD_DIR.glob("*.meta.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    out.sort(key=lambda m: m.get("uploaded_at") or "", reverse=True)
    return out


def get_meta(workbook_id: str) -> dict:
    wid = (workbook_id or "").strip()
    if not wid or not _xlsx_path(wid).is_file():
        known = [m.get("workbook_id") for m in list_workbooks()]
        raise WorkbookError(
            f"Workbook '{workbook_id}' not found. Uploaded workbooks: {known or '(none)'}"
        )
    return _read_meta(wid) or {"workbook_id": wid, "filename": wid, "roles": {}}


def delete_workbook(workbook_id: str) -> dict:
    meta = get_meta(workbook_id)
    _xlsx_path(meta["workbook_id"]).unlink(missing_ok=True)
    _meta_path(meta["workbook_id"]).unlink(missing_ok=True)
    _WB_CACHE.pop(meta["workbook_id"], None)
    return {"deleted": meta["workbook_id"], "filename": meta.get("filename")}


def set_sheet_roles(workbook_id: str, roles: dict) -> dict:
    """Record user-confirmed sheet roles (input/calc/output/reference/ignore)."""
    meta = get_meta(workbook_id)
    wb_f, _ = _load(meta["workbook_id"])
    if not isinstance(roles, dict) or not roles:
        raise WorkbookError("`roles` must be a non-empty object {sheet_name: role}")
    for sheet, role in roles.items():
        if sheet not in wb_f.sheetnames:
            raise WorkbookError(
                f"Sheet '{sheet}' not in workbook. Sheets: {wb_f.sheetnames}"
            )
        if str(role).lower() not in VALID_ROLES:
            raise WorkbookError(
                f"Invalid role '{role}' for sheet '{sheet}'. Allowed: {VALID_ROLES}"
            )
    meta.setdefault("roles", {}).update(
        {s: str(r).lower() for s, r in roles.items()}
    )
    _write_meta(meta["workbook_id"], meta)
    return meta


# ──────────────────────────────────────────────────────────────────────────
# Workbook loading (mtime-keyed cache of the two openpyxl views)
# ──────────────────────────────────────────────────────────────────────────

# workbook_id -> (mtime, wb_formulas, wb_values)
_WB_CACHE: dict[str, tuple[float, Any, Any]] = {}


def _load(workbook_id: str):
    """Return (wb_formulas, wb_values): the same file opened with formulas
    preserved and with cached values. wb_values cells are None for formula
    cells if the file was never recalculated/saved by Excel."""
    from openpyxl import load_workbook

    path = _xlsx_path(workbook_id)
    if not path.is_file():
        raise WorkbookError(f"Workbook '{workbook_id}' not found on disk")
    mtime = path.stat().st_mtime
    hit = _WB_CACHE.get(workbook_id)
    if hit and hit[0] == mtime:
        return hit[1], hit[2]
    wb_f = load_workbook(path, data_only=False)
    wb_v = load_workbook(path, data_only=True)
    _WB_CACHE[workbook_id] = (mtime, wb_f, wb_v)
    return wb_f, wb_v


def _get_sheet(wb, sheet: str):
    for name in wb.sheetnames:
        if name.lower() == (sheet or "").strip().lower():
            return wb[name]
    raise WorkbookError(f"Sheet '{sheet}' not found. Sheets: {wb.sheetnames}")


# ──────────────────────────────────────────────────────────────────────────
# Cell / header helpers
# ──────────────────────────────────────────────────────────────────────────

def _formula_text(cell) -> str | None:
    """Return the formula string of a cell, or None if it isn't a formula."""
    if cell.data_type != "f":
        return None
    v = cell.value
    if v is None:
        return None
    if isinstance(v, str):
        return v
    # ArrayFormula and similar wrapper objects expose .text
    return getattr(v, "text", None) or str(v)


def sanitize_field_name(header: Any) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(header or "").strip()).strip("_").lower()
    if not s:
        return "col"
    if s[0].isdigit():
        s = "f_" + s
    return s


def _headers_for(ws, header_row: int = 1) -> dict[int, str]:
    """{column_index (1-based): header text} from the header row."""
    out: dict[int, str] = {}
    for cell in ws[header_row]:
        if cell.value is not None and str(cell.value).strip() != "":
            out[cell.column] = str(cell.value).strip()
    return out


def _json_safe(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d") if (v.hour, v.minute, v.second) == (0, 0, 0) \
            else v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


# ──────────────────────────────────────────────────────────────────────────
# Formula reference parsing
# ──────────────────────────────────────────────────────────────────────────
# A single A1-style reference with optional sheet qualifier. Guards:
#  * lookbehind: not glued to an identifier char (so `TAX1` in a defined name
#    is not read as a cell ref)
#  * lookahead: not followed by `(` (kills LOG10( / ATAN2( style function
#    names) and not followed by more identifier chars.
_REF_CORE = (
    r"(?:(?:'(?P<qsheet>[^']+)'|(?P<sheet>[A-Za-z_][A-Za-z0-9_.]*))!)?"
    r"(?P<cabs>\$?)(?P<col>[A-Za-z]{1,3})(?P<rabs>\$?)(?P<row>[0-9]+)"
)
_REF_RE = re.compile(r"(?<![A-Za-z0-9_.$])" + _REF_CORE + r"(?![A-Za-z0-9_(])")

_STRING_RE = re.compile(r'"(?:[^"]|"")*"')


def _col_to_idx(letters: str) -> int:
    n = 0
    for ch in letters.upper():
        n = n * 26 + (ord(ch) - 64)
    return n


def _idx_to_col(idx: int) -> str:
    s = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s


def _mask_strings(formula: str) -> tuple[str, list[str]]:
    """Replace string literals with \x00<i>\x01 placeholders so their content
    is never mistaken for cell references."""
    literals: list[str] = []

    def repl(m):
        literals.append(m.group(0))
        return f"\x00{len(literals) - 1}\x01"

    return _STRING_RE.sub(repl, formula), literals


def _unmask_strings(text: str, literals: list[str]) -> str:
    for i, lit in enumerate(literals):
        text = text.replace(f"\x00{i}\x01", lit)
    return text


def parse_refs(formula: str, own_sheet: str) -> list[dict]:
    """All cell references in a formula (strings masked out first).
    Each: {sheet, col_idx, row, col_abs, row_abs, span_start, span_end}."""
    masked, _ = _mask_strings(formula)
    refs = []
    for m in _REF_RE.finditer(masked):
        refs.append({
            "sheet": m.group("qsheet") or m.group("sheet") or own_sheet,
            "col_idx": _col_to_idx(m.group("col")),
            "row": int(m.group("row")),
            "col_abs": bool(m.group("cabs")),
            "row_abs": bool(m.group("rabs")),
            "start": m.start(),
            "end": m.end(),
        })
    # Mark A1:B10-style ranges: two refs joined by a colon.
    for a, b in zip(refs, refs[1:]):
        between = masked[a["end"]:b["start"]]
        if between.strip() == ":":
            a["range_with"] = (b["sheet"], b["col_idx"], b["row"])
            b["range_tail"] = True
    return refs


def normalize_r1c1(formula: str, own_row: int, own_col: int) -> str:
    """Rewrite every A1 reference relative to the formula's own cell.
    Two formulas dragged down/across a column normalise to the SAME string,
    which is what makes deduplication work."""
    masked, literals = _mask_strings(formula)

    def repl(m):
        col_idx = _col_to_idx(m.group("col"))
        row_idx = int(m.group("row"))
        sheet = m.group("qsheet") or m.group("sheet")
        if m.group("rabs"):
            rpart = f"R{row_idx}"
        else:
            dr = row_idx - own_row
            rpart = "R" if dr == 0 else f"R[{dr}]"
        if m.group("cabs"):
            cpart = f"C{col_idx}"
        else:
            dc = col_idx - own_col
            cpart = "C" if dc == 0 else f"C[{dc}]"
        return (f"{sheet}!" if sheet else "") + rpart + cpart

    return _unmask_strings(_REF_RE.sub(repl, masked), literals)


def _friendly(formula: str, own_sheet: str, own_row: int,
              headers_by_sheet: dict[str, dict[int, str]],
              values_by_sheet: dict[str, Any]) -> str:
    """Substitute header names for references so the LLM reads
    `principal * annual_rate / 12` instead of `=$B2*C2/12`.

    Same-row relative ref            -> header
    Previous-row relative ref        -> prev(header)
    Other relative row offset        -> offset(header, dr)
    Absolute single cell             -> Sheet!B1(=cached_value)
    Range endpoints                  -> left as-is (ranges stay explicit)
    """
    masked, literals = _mask_strings(formula)

    # Pre-compute which match positions are part of a range (keep those raw).
    range_positions = set()
    for m1, m2 in zip(_REF_RE.finditer(masked),
                      list(_REF_RE.finditer(masked))[1:]):
        if masked[m1.end():m2.start()].strip() == ":":
            range_positions.add(m1.start())
            range_positions.add(m2.start())

    def repl(m):
        if m.start() in range_positions:
            return m.group(0)
        sheet = m.group("qsheet") or m.group("sheet") or own_sheet
        col_idx = _col_to_idx(m.group("col"))
        row_idx = int(m.group("row"))
        header = (headers_by_sheet.get(sheet) or {}).get(col_idx)
        prefix = "" if sheet == own_sheet else f"{sheet}."
        if m.group("rabs") or header is None:
            # Fixed cell (assumption/parameter) — show its cached value when scalar.
            label = f"{sheet}!{_idx_to_col(col_idx)}{row_idx}"
            wsv = values_by_sheet.get(sheet)
            if wsv is not None:
                try:
                    v = wsv.cell(row=row_idx, column=col_idx).value
                    if isinstance(v, (int, float)):
                        return f"{label}(={v})"
                except Exception:
                    pass
            return label
        dr = row_idx - own_row
        name = sanitize_field_name(header)
        if dr == 0:
            return prefix + name
        if dr == -1:
            return f"prev({prefix}{name})"
        return f"offset({prefix}{name}, {dr})"

    return _unmask_strings(_REF_RE.sub(repl, masked), literals)


# ──────────────────────────────────────────────────────────────────────────
# Public analysis API
# ──────────────────────────────────────────────────────────────────────────

def _sheet_stats(ws) -> dict:
    n_formulas = 0
    n_values = 0
    scanned = 0
    truncated = False
    for row in ws.iter_rows():
        for cell in row:
            scanned += 1
            if scanned > MAX_SCAN_CELLS_PER_SHEET:
                truncated = True
                break
            if cell.value is None:
                continue
            if cell.data_type == "f":
                n_formulas += 1
            else:
                n_values += 1
        if truncated:
            break
    return {
        "formula_cells": n_formulas,
        "value_cells": n_values,
        "scan_truncated": truncated,
    }


def workbook_overview(workbook_id: str, header_row: int = 1) -> dict:
    """Per-sheet dimensions, headers, formula density, cached-value
    availability, suggested roles, and named ranges — everything the agent
    needs to interview the user about sheet roles."""
    meta = get_meta(workbook_id)
    wb_f, wb_v = _load(meta["workbook_id"])

    # Which sheets does each sheet's formulas reference? (for role suggestion)
    refs_out: dict[str, set[str]] = {s: set() for s in wb_f.sheetnames}
    for sname in wb_f.sheetnames:
        ws = wb_f[sname]
        scanned = 0
        for row in ws.iter_rows():
            for cell in row:
                scanned += 1
                if scanned > MAX_SCAN_CELLS_PER_SHEET:
                    break
                f = _formula_text(cell)
                if not f:
                    continue
                for r in parse_refs(f, sname):
                    if r["sheet"] != sname:
                        refs_out[sname].add(r["sheet"])
            if scanned > MAX_SCAN_CELLS_PER_SHEET:
                break
    referenced_by: dict[str, set[str]] = {s: set() for s in wb_f.sheetnames}
    for src, targets in refs_out.items():
        for t in targets:
            for actual in wb_f.sheetnames:
                if actual.lower() == t.lower():
                    referenced_by[actual].add(src)

    sheets = []
    for sname in wb_f.sheetnames:
        ws = wb_f[sname]
        wsv = wb_v[sname]
        stats = _sheet_stats(ws)
        headers = _headers_for(ws, header_row)

        # Does this sheet have cached values for its formula cells?
        has_cached = None
        if stats["formula_cells"]:
            has_cached = False
            checked = 0
            for row in ws.iter_rows():
                for cell in row:
                    if cell.data_type == "f":
                        checked += 1
                        if wsv.cell(row=cell.row, column=cell.column).value is not None:
                            has_cached = True
                            break
                        if checked >= 25:
                            break
                if has_cached or checked >= 25:
                    break

        if stats["formula_cells"] == 0:
            suggested = "input"
        elif referenced_by[sname]:
            suggested = "calc"
        else:
            suggested = "calc" if not refs_out[sname] else "output"

        sheets.append({
            "name": sname,
            "declared_role": (meta.get("roles") or {}).get(sname),
            "suggested_role": suggested,
            "max_row": ws.max_row,
            "max_col": ws.max_column,
            "headers": [
                {"column": _idx_to_col(i), "name": h,
                 "field_name": sanitize_field_name(h)}
                for i, h in sorted(headers.items())
            ],
            "references_sheets": sorted(refs_out[sname]),
            "referenced_by_sheets": sorted(referenced_by[sname]),
            "has_cached_formula_values": has_cached,
            **stats,
        })

    named_ranges = []
    try:
        for name, dn in (wb_f.defined_names or {}).items():
            named_ranges.append({"name": name, "refers_to": dn.attr_text})
    except Exception:
        pass

    warnings = []
    if any(s["has_cached_formula_values"] is False for s in sheets):
        warnings.append(
            "Some formula sheets have NO cached values (the file was saved by "
            "a tool other than Excel, or never recalculated). get_sheet_data "
            "cannot return computed numbers for those sheets — reconcile "
            "against a values-only output sheet instead."
        )

    return {
        "workbook_id": meta["workbook_id"],
        "filename": meta.get("filename"),
        "uploaded_at": meta.get("uploaded_at"),
        "sheets": sheets,
        "named_ranges": named_ranges,
        "warnings": warnings,
        "roles_confirmed": bool(meta.get("roles")),
    }


def sheet_rows(workbook_id: str, sheet: str, header_row: int = 1,
               limit: int = 20, offset: int = 0) -> dict:
    """Rows of a sheet as {header: value} dicts, using cached values for
    formula cells (None + warning when the file carries no cached values)."""
    meta = get_meta(workbook_id)
    wb_f, wb_v = _load(meta["workbook_id"])
    ws_f = _get_sheet(wb_f, sheet)
    ws_v = _get_sheet(wb_v, sheet)
    headers = _headers_for(ws_f, header_row)
    if not headers:
        raise WorkbookError(
            f"Sheet '{sheet}' has no headers in row {header_row}. "
            f"Pass header_row if the header lives elsewhere."
        )
    rows = []
    none_formula_cells = 0
    first = header_row + 1 + max(0, int(offset))
    last = min(ws_f.max_row, first + max(1, int(limit)) - 1)
    for r in range(first, last + 1):
        row_out = {}
        empty = True
        for c, h in headers.items():
            v = ws_v.cell(row=r, column=c).value
            if v is None and ws_f.cell(row=r, column=c).data_type == "f":
                none_formula_cells += 1
            if v is not None:
                empty = False
            row_out[sanitize_field_name(h)] = _json_safe(v)
        if not empty:
            rows.append(row_out)
    total_data_rows = max(0, ws_f.max_row - header_row)
    out = {
        "sheet": ws_f.title,
        "headers": [sanitize_field_name(h) for _, h in sorted(headers.items())],
        "original_headers": [h for _, h in sorted(headers.items())],
        "row_count_total": total_data_rows,
        "rows_returned": len(rows),
        "offset": offset,
        "rows": rows,
    }
    if none_formula_cells:
        out["warning"] = (
            f"{none_formula_cells} formula cell(s) in this range have no "
            f"cached value — the numbers shown as null were never computed by "
            f"Excel. Analyse formulas via get_sheet_formulas instead."
        )
    return out


def declared_role(workbook_id: str, sheet: str) -> str | None:
    """The user-confirmed role of a sheet (case-insensitive), or None."""
    meta = get_meta(workbook_id)
    for s, r in (meta.get("roles") or {}).items():
        if s.lower() == (sheet or "").strip().lower():
            return r
    return None


def sheet_diagnostics(workbook_id: str, sheet: str, header_row: int = 1) -> dict:
    """Explain what a sheet actually contains — used to turn a bare 'no data
    rows' failure into an actionable message (empty template vs uncached
    formulas vs headers elsewhere)."""
    meta = get_meta(workbook_id)
    wb_f, wb_v = _load(meta["workbook_id"])
    ws_f = _get_sheet(wb_f, sheet)
    ws_v = _get_sheet(wb_v, sheet)
    headers = _headers_for(ws_f, header_row)
    stats = _sheet_stats(ws_f)
    data_cells = 0          # non-empty non-formula cells below the header row
    uncached_formulas = 0
    cached_formulas = 0
    scanned = 0
    for row in ws_f.iter_rows(min_row=header_row + 1):
        for cell in row:
            scanned += 1
            if scanned > MAX_SCAN_CELLS_PER_SHEET:
                break
            if cell.value is None:
                continue
            if cell.data_type == "f":
                if ws_v.cell(row=cell.row, column=cell.column).value is None:
                    uncached_formulas += 1
                else:
                    cached_formulas += 1
            else:
                data_cells += 1
        if scanned > MAX_SCAN_CELLS_PER_SHEET:
            break
    return {
        "sheet": ws_f.title,
        "header_row": header_row,
        "header_count": len(headers),
        "headers": [h for _, h in sorted(headers.items())][:20],
        "data_cells_below_header": data_cells,
        "cached_formula_cells_below_header": cached_formulas,
        "uncached_formula_cells_below_header": uncached_formulas,
        **stats,
    }


def sheet_formula_patterns(workbook_id: str, sheet: str,
                           header_row: int = 1) -> dict:
    """The heart of Phase 2: every formula cell in the sheet, deduplicated by
    normalised R1C1 pattern. 10k dragged-down copies of one formula come back
    as ONE entry with count=10k, its target column header, a friendly
    header-name rendering, and flags for row-recursion / cross-sheet refs."""
    meta = get_meta(workbook_id)
    wb_f, wb_v = _load(meta["workbook_id"])
    ws = _get_sheet(wb_f, sheet)
    own = ws.title

    headers_by_sheet = {s: _headers_for(wb_f[s], header_row) for s in wb_f.sheetnames}
    values_by_sheet = {s: wb_v[s] for s in wb_v.sheetnames}

    groups: dict[str, dict] = {}
    scanned = 0
    truncated = False
    for row in ws.iter_rows():
        for cell in row:
            scanned += 1
            if scanned > MAX_SCAN_CELLS_PER_SHEET:
                truncated = True
                break
            f = _formula_text(cell)
            if not f:
                continue
            key = normalize_r1c1(f, cell.row, cell.column)
            g = groups.get(key)
            if g is None:
                refs = parse_refs(f, own)
                row_recursive = any(
                    (r["sheet"].lower() == own.lower()) and not r["row_abs"]
                    and (r["row"] - cell.row) < 0
                    for r in refs
                )
                cross = sorted({
                    r["sheet"] for r in refs
                    if r["sheet"].lower() != own.lower()
                })
                header = headers_by_sheet[own].get(cell.column)
                groups[key] = {
                    "pattern_r1c1": key,
                    "example_cell": f"{_idx_to_col(cell.column)}{cell.row}",
                    "example_formula": f,
                    "friendly": _friendly(f, own, cell.row,
                                          headers_by_sheet, values_by_sheet),
                    "target_column": _idx_to_col(cell.column),
                    "target_header": header,
                    "target_field_name": sanitize_field_name(header) if header else None,
                    "count": 1,
                    "rows_min": cell.row,
                    "rows_max": cell.row,
                    "columns": {_idx_to_col(cell.column)},
                    "row_recursive": row_recursive,
                    "cross_sheet_refs": cross,
                }
            else:
                g["count"] += 1
                g["rows_min"] = min(g["rows_min"], cell.row)
                g["rows_max"] = max(g["rows_max"], cell.row)
                g["columns"].add(_idx_to_col(cell.column))
        if truncated:
            break

    patterns = []
    for g in sorted(groups.values(),
                    key=lambda g: (min(_col_to_idx(c) for c in g["columns"]),
                                   g["rows_min"])):
        g["columns"] = sorted(g["columns"], key=_col_to_idx)
        patterns.append(g)

    return {
        "sheet": own,
        "unique_patterns": len(patterns),
        "total_formula_cells": sum(g["count"] for g in patterns),
        "scan_truncated": truncated,
        "patterns": patterns,
        "hint": (
            "Each pattern is ONE spreadsheet calculation dragged across "
            "`count` cells. `friendly` shows it with column-header names — "
            "translate that expression into a DSL calc-step formula. "
            "`row_recursive: true` means the column references its own "
            "previous row -> model it as a SCHEDULE step with lag(). "
            "Absolute refs like Sheet!B1(=0.05) are fixed parameters — "
            "either hardcode the value or add it as an event field."
        ),
    }


def dependency_graph(workbook_id: str, header_row: int = 1) -> dict:
    """Column-level data-flow graph across the whole workbook.
    Node = 'Sheet!field_name' (or 'Sheet!A' when a column has no header);
    edge src->dst = dst's formula reads src."""
    meta = get_meta(workbook_id)
    wb_f, _ = _load(meta["workbook_id"])
    headers_by_sheet = {s: _headers_for(wb_f[s], header_row) for s in wb_f.sheetnames}
    canonical = {s.lower(): s for s in wb_f.sheetnames}

    def node_id(sheet: str, col_idx: int) -> str:
        sheet = canonical.get(sheet.lower(), sheet)
        h = headers_by_sheet.get(sheet, {}).get(col_idx)
        return f"{sheet}!{sanitize_field_name(h) if h else _idx_to_col(col_idx)}"

    edges: set[tuple[str, str]] = set()
    nodes: dict[str, dict] = {}
    for sname in wb_f.sheetnames:
        ws = wb_f[sname]
        scanned = 0
        for row in ws.iter_rows():
            for cell in row:
                scanned += 1
                if scanned > MAX_SCAN_CELLS_PER_SHEET:
                    break
                f = _formula_text(cell)
                if not f:
                    continue
                dst = node_id(sname, cell.column)
                nodes.setdefault(dst, {"id": dst, "sheet": sname, "kind": "computed"})
                refs = parse_refs(f, sname)
                i = 0
                while i < len(refs):
                    r = refs[i]
                    if "range_with" in r:
                        r2sheet, r2col, _r2row = r["range_with"]
                        for ci in range(min(r["col_idx"], r2col),
                                        max(r["col_idx"], r2col) + 1):
                            src = node_id(r["sheet"], ci)
                            if src != dst:
                                nodes.setdefault(src, {"id": src,
                                                       "sheet": canonical.get(r["sheet"].lower(), r["sheet"]),
                                                       "kind": "value"})
                                edges.add((src, dst))
                        i += 2  # skip the range tail ref
                        continue
                    src = node_id(r["sheet"], r["col_idx"])
                    if src != dst:
                        nodes.setdefault(src, {"id": src,
                                               "sheet": canonical.get(r["sheet"].lower(), r["sheet"]),
                                               "kind": "value"})
                        edges.add((src, dst))
                    i += 1
            if scanned > MAX_SCAN_CELLS_PER_SHEET:
                break

    roles = meta.get("roles") or {}
    for n in nodes.values():
        n["sheet_role"] = roles.get(n["sheet"])

    # Order sheets by flow: sheets nothing depends on first.
    sheet_edges = {(nodes[a]["sheet"], nodes[b]["sheet"])
                   for a, b in edges if nodes[a]["sheet"] != nodes[b]["sheet"]}
    flow = [f"{a} -> {b}" for a, b in sorted(sheet_edges)]

    return {
        "workbook_id": meta["workbook_id"],
        "nodes": sorted(nodes.values(), key=lambda n: n["id"]),
        "edges": [{"from": a, "to": b} for a, b in sorted(edges)],
        "sheet_flow": flow,
        "hint": (
            "Edges show which columns feed which. 'value' nodes with no "
            "incoming edge are raw inputs -> event fields. 'computed' nodes "
            "-> calc/schedule step variables, in dependency order."
        ),
    }


def expected_output_values(workbook_id: str, sheet: str, key_column: str,
                           value_columns: list[str] | None = None,
                           header_row: int = 1) -> dict:
    """Read the output sheet's numbers (cached values) keyed by the key
    column — the reconciliation baseline. Returns
    {key: {field_name: value}} plus bookkeeping."""
    meta = get_meta(workbook_id)
    wb_f, wb_v = _load(meta["workbook_id"])
    ws_f = _get_sheet(wb_f, sheet)
    ws_v = _get_sheet(wb_v, sheet)
    headers = _headers_for(ws_f, header_row)
    by_field = {sanitize_field_name(h): c for c, h in headers.items()}
    key_field = sanitize_field_name(key_column)
    if key_field not in by_field:
        raise WorkbookError(
            f"key_column '{key_column}' not found in sheet '{sheet}'. "
            f"Available: {sorted(by_field)}"
        )
    want = ([sanitize_field_name(c) for c in value_columns]
            if value_columns else
            [f for f in by_field if f != key_field])
    missing = [c for c in want if c not in by_field]
    if missing:
        raise WorkbookError(
            f"Column(s) {missing} not found in sheet '{sheet}'. "
            f"Available: {sorted(by_field)}"
        )
    expected: dict[str, dict] = {}
    uncached = 0
    for r in range(header_row + 1, ws_f.max_row + 1):
        key_v = ws_v.cell(row=r, column=by_field[key_field]).value
        if key_v is None or str(key_v).strip() == "":
            continue
        vals = {}
        for f in want:
            c = by_field[f]
            v = ws_v.cell(row=r, column=c).value
            if v is None and ws_f.cell(row=r, column=c).data_type == "f":
                uncached += 1
            vals[f] = _json_safe(v)
        expected[str(key_v).strip()] = vals
    return {
        "sheet": ws_f.title,
        "key_field": key_field,
        "value_fields": want,
        "row_count": len(expected),
        "uncached_formula_cells": uncached,
        "expected": expected,
    }


def infer_field_types(rows: list[dict]) -> dict[str, str]:
    """Map each column to a DSL datatype from observed values."""
    types: dict[str, str] = {}
    fields = set()
    for r in rows:
        fields.update(r.keys())
    for f in fields:
        vals = [r.get(f) for r in rows if r.get(f) is not None and r.get(f) != ""]
        if not vals:
            types[f] = "string"
            continue
        if all(isinstance(v, bool) for v in vals):
            types[f] = "boolean"
        elif all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
            types[f] = "integer" if all(
                float(v).is_integer() for v in vals) else "decimal"
        elif all(isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)
                 for v in vals):
            types[f] = "date"
        else:
            types[f] = "string"
    return types
