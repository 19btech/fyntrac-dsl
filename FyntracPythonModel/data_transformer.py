"""
Data Transformer for Fyntrac Model Runner
==========================================
Converts raw import JSON (same format uploaded via Import in DSL Studio)
into the merged event_data shape that generated Python templates expect.

The input JSON is an array of event records — the same JSON your main app
produces and uploads to DSL Studio's /import-events/transform endpoint.

This module:
1. Parses the raw JSON into per-event data rows
2. Merges rows across events by instrumentid
3. Builds the raw_event_data dict for collect() functions
4. Iterates ALL instruments (no limit)
"""

import json
import logging
import re
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from app.python_model.dsl_functions import normalize_date
except ImportError:  # pragma: no cover - layout-dependent
    try:
        from dsl_functions import normalize_date
    except ImportError:
        # Last resort: this package's own directory is not on sys.path. Happens
        # whenever the folder is imported as a package (FyntracPythonModel.x)
        # rather than from inside it -- the bare-name fallback above then fails
        # and the whole module becomes unimportable. Resolve against __file__ so
        # the runtime is genuinely drop-in, in any host layout.
        import os as _os
        import sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
        from dsl_functions import normalize_date


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_EVENT_FIELDS = {
    "instrumentId", "eventId", "eventName", "postingDate",
    "effectiveDate", "status", "eventDetail", "_class",
}

# Fixed/system keys to exclude from dynamic field extraction
_IMPORT_FIXED_KEYS = {
    "PostingDate", "EffectiveDate", "InstrumentId", "AttributeId",
    "postingDate", "effectiveDate", "instrumentId", "attributeId",
    "_id", "_metadata_version", "_imported_at",
}

# The only fields this pipeline actually reads. `status` and `_class` are
# metadata that nothing here touches, so a record missing them is still
# perfectly processable -- rejecting the whole file over them meant one
# malformed record among a hundred thousand aborted an entire EOD run.
_ESSENTIAL_EVENT_FIELDS = {"eventId", "eventDetail"}

# Standard row columns, lowercased. These are built explicitly from the outer
# record and must never be run through value-based type coercion: a numeric
# instrument id inferred as `decimal` would be rewritten from "12345" to
# 12345.0 and stop matching anything.
_STANDARD_ROW_COLUMNS = frozenset(
    {"instrumentid", "postingdate", "effectivedate", "subinstrumentid"}
)

# Values that mean "no value". Deliberately matches the playground's ingest
# (backend/server.py: pd.isna, '' , 'none', 'null') and NOTHING more.
#
# Placeholders like "N/A" are NOT blanks. Treating them as blank would type a
# money column as decimal and then rewrite "N/A" to 0.0 -- silently destroying
# the distinction between "no value" and "zero". Instead a non-numeric value
# demotes the whole column to `string` in _infer_field_datatype, so float()
# never runs on it. That is the contract
# tests/test_event_config_import.py::test_value_inference_is_row_order_independent
# pins, and it mirrors the backend's _reconcile_field_types.
_BLANKISH = {"", "none", "null", "nan"}

# Per-field sample cap for datatype inference. A column's type is obvious from
# a small sample, and scanning millions of values to decide it is pure cost.
_INFER_SAMPLE_CAP = 200


def _as_clean_str(value: Any) -> str:
    """`str`-safe strip.

    The outer/inner instrumentId lookups used to call .strip() on whatever the
    JSON held. A numeric instrument id (12345, entirely normal for loan
    numbers) is an int, and int has no .strip() -- the AttributeError escaped
    _is_custom_event, then transform(), and killed the whole run.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _ci_get(row: Dict[str, Any], *names: str) -> Any:
    """First non-empty value among `names`, matched case-insensitively.

    Every other field read in this pipeline is case-insensitive
    (get_field_case_insensitive). The import extraction was not: it probed two
    exact spellings, so `instrumentid` or `INSTRUMENTID` read as absent. For
    _is_custom_event that silently reclassified a normal activity event as
    reference data, which skipped the posting-date filter AND dropped the
    date/instrument columns -- a three-date time series collapsed to one
    undated row.
    """
    if not isinstance(row, dict):
        return None
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    lowered = {str(k).lower(): k for k in row}
    for name in names:
        key = lowered.get(name.lower())
        if key is not None and row[key] not in (None, ""):
            return row[key]
    # Last tier: ignore separators, so Instrument_Id / "instrument id" /
    # INSTRUMENT-ID all resolve. Only the four standard columns are looked up
    # this way; freeform field names keep their exact spelling.
    squashed = {re.sub(r"[^a-z0-9]", "", str(k).lower()): k for k in row}
    for name in names:
        key = squashed.get(re.sub(r"[^a-z0-9]", "", name.lower()))
        if key is not None and row[key] not in (None, ""):
            return row[key]
    return None


def _is_blankish(value: Any) -> bool:
    """True for None/NaN/empty/placeholder values."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN
        return True
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return str(value).strip().lower() in _BLANKISH


def _normalize_date_field_value(value: Any) -> Any:
    """Normalize a date-typed field to yyyy-mm-dd, or to a LIST of them.

    Mirrors backend/server.py::_normalize_ingest_date_value. A date field may
    legitimately hold several dates as a JSON array string or a delimited
    string; the playground splits those into a real list at ingest. This
    runtime did not, so `array_length(PaymentDates)` counted CHARACTERS and any
    schedule driven off such a field was quietly wrong.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        out = []
        for v in value:
            try:
                nv = normalize_date(v)
            except Exception:
                nv = ""
            if nv:
                out.append(nv)
        return out

    if isinstance(value, dict):
        return _parse_import_date(value)

    s = value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return ""
        try:
            parsed = json.loads(s)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [d for d in (_parse_import_date(p) for p in parsed) if d]
        if "," in s or ";" in s or "|" in s:
            parts = [p.strip() for p in re.split(r"[,;|]", s) if p.strip()]
            normalized = [d for d in (_parse_import_date(p) for p in parts) if d]
            # Only treat it as a list when every piece really parsed as a date;
            # otherwise it was ordinary text that happened to contain a comma.
            if len(normalized) == len(parts) and len(parts) > 1:
                return normalized
            return _parse_import_date(s)

    return _parse_import_date(s)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_import_date(val) -> str:
    """Normalise a date value from an imported event record to YYYY-MM-DD."""
    if val is None:
        return ""
    if isinstance(val, dict) and "$date" in val:
        return str(val["$date"])[:10]
    if isinstance(val, int):
        s = str(val)
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s
    try:
        return normalize_date(str(val))
    except Exception:
        return str(val)


def _is_custom_event(records: list, event_id: str) -> bool:
    """
    Return True if the event has no real InstrumentId.

    An event is 'standard' only when at least one inner value row contains
    an instrumentId that matches the outer instrumentId of the same event
    record. Otherwise it's a custom/reference event.

    Both lookups are case-insensitive and str-safe (see _ci_get/_as_clean_str).
    Getting this wrong is expensive in both directions: a misread activity
    event becomes 'reference', which skips the posting-date filter and strips
    the date columns, while a misread reference table gets date-filtered down
    to nothing.
    """
    for event in records:
        if not isinstance(event, dict) or event.get("eventId") != event_id:
            continue
        outer = _as_clean_str(_ci_get(event, "instrumentId"))
        if not outer:
            continue
        values = (event.get("eventDetail") or {}).get("values") or {}
        if not isinstance(values, dict):
            continue
        for row_val in values.values():
            if not isinstance(row_val, dict):
                continue
            inner = _as_clean_str(_ci_get(row_val, "instrumentId"))
            # Compare case-insensitively: the outer record and the inner row
            # come from different systems often enough that casing differs.
            if inner and inner.lower() == outer.lower():
                return False
    return True


def _looks_numeric(text: str) -> bool:
    """True for "1250.50", "-42", "1,234.56"; False for zero-padded ids.

    A leading zero marks an identifier ("00123"), not a quantity. Typing such a
    column as decimal drops the padding and the id stops matching.
    """
    stripped = text.strip().lstrip("-").replace(",", "")
    if not stripped.replace(".", "", 1).isdigit():
        return False
    whole = stripped.split(".")[0]
    if len(whole) > 1 and whole.startswith("0"):
        return False
    return True


_DATE_TEXT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _looks_like_date_text(text: str) -> bool:
    """True for a single ISO date, or for several of them in one string.

    A date field may arrive as a JSON array string or a delimited string. If
    inference does not recognise those as dates, _coerce_value never splits
    them and the model gets one long string instead of a list of dates.
    """
    s = text.strip()
    if not s:
        return False
    if _DATE_TEXT_RE.match(s):
        return True
    if s.startswith("["):
        try:
            parsed = json.loads(s)
        except Exception:
            return False
        return (isinstance(parsed, list) and len(parsed) > 0
                and all(isinstance(p, str) and _DATE_TEXT_RE.match(p.strip())
                        for p in parsed))
    if "," in s or ";" in s or "|" in s:
        parts = [p.strip() for p in re.split(r"[,;|]", s) if p.strip()]
        return len(parts) > 1 and all(_DATE_TEXT_RE.match(p) for p in parts)
    return False


def _infer_field_datatype(values: list) -> str:
    """Infer the best datatype for a field from a list of sample values.

    Scans ALL non-blank values and applies a precedence:
        boolean > date > string > decimal

    It previously returned on the FIRST non-null value, so the answer depended
    on row order: ['N/A', 900.0] inferred `string` while [900.0, 'N/A'] inferred
    `decimal`, and a single blank leading cell was enough to type a money column
    as `string`. Blanks are now ignored for inference, and one non-numeric value
    demotes the whole column to `string` -- matching how the playground's
    _reconcile_field_types corrects a bad guess rather than coercing values to
    0.0 and destroying them.
    """
    saw_date = False
    saw_string = False
    saw_number = False

    for v in values:
        if _is_blankish(v):
            continue
        if isinstance(v, bool):
            return "boolean"
        if isinstance(v, dict):
            if "$date" in v:
                saw_date = True
            continue
        if isinstance(v, str):
            if _looks_like_date_text(v):
                saw_date = True
            elif _looks_numeric(v):
                saw_number = True
            else:
                saw_string = True
            continue
        if isinstance(v, (int, float)):
            saw_number = True

    if saw_date:
        return "date"
    if saw_string:
        return "string"
    if saw_number:
        return "decimal"
    # Nothing conclusive (all blank). Keep the historical default.
    return "decimal"


def get_field_case_insensitive(row: Dict[str, Any], field_name: str, default: Any = '') -> Any:
    """Get field value with case-insensitive key matching."""
    if field_name in row:
        return row[field_name]
    field_lower = field_name.lower()
    for key in row:
        if key.lower() == field_lower:
            return row[key]
    return default


def _sort_activity_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enforce canonical activity-data ordering:
        instrumentid ASC, postingdate ASC, effectivedate ASC, subinstrumentid ASC

    Mirrors backend/server.py::_sort_activity_rows so the export runtime
    delivers activity rows to the generated template in the same canonical
    order as the playground. Reference / custom event data is intentionally
    skipped by the caller — it has no instrument/date axis.
    """
    if not isinstance(rows, list) or len(rows) <= 1:
        return rows

    def _ci(row, name):
        if not isinstance(row, dict):
            return ''
        if name in row:
            v = row[name]
        else:
            lname = name.lower()
            v = ''
            for k, val in row.items():
                if str(k).lower() == lname:
                    v = val
                    break
        if v is None:
            return ''
        return str(v)

    try:
        rows.sort(key=lambda r: (
            _ci(r, 'instrumentid'),
            _ci(r, 'postingdate'),
            _ci(r, 'effectivedate'),
            _ci(r, 'subinstrumentid') or '1',
        ))
    except Exception:
        pass
    return rows


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _record_is_processable(item: Any) -> bool:
    """True when this one record carries enough to be extracted."""
    if not isinstance(item, dict):
        return False
    if _ESSENTIAL_EVENT_FIELDS - item.keys():
        return False
    detail = item.get("eventDetail")
    return isinstance(detail, dict) and isinstance(detail.get("values"), dict)


def validate_import_json(data: Any) -> Optional[str]:
    """
    Validate that the input JSON matches the expected import format.
    Returns an error message string if invalid, or None if valid.

    Only STRUCTURAL problems fail the run -- the payload is not an array, it is
    empty, or not one record in it is processable. Individual bad records are
    skipped and reported by build_event_data_from_import instead.

    This used to require all eight of REQUIRED_EVENT_FIELDS on every record,
    including `status` and `_class`, which nothing in this pipeline reads. One
    record missing a metadata field aborted the entire batch. The playground
    enforces none of this (backend/server.py defines REQUIRED_EVENT_FIELDS and
    never uses it), so a model verified there could hard-fail here on data the
    playground accepted.
    """
    if not isinstance(data, list):
        return "Input must be a JSON array of event objects."
    if len(data) == 0:
        return "The JSON array is empty — no events to process."

    processable = sum(1 for item in data if _record_is_processable(item))
    if processable == 0:
        return (
            "No processable event records found. Every record needs "
            f"{', '.join(sorted(_ESSENTIAL_EVENT_FIELDS))} and an "
            "'eventDetail' object containing a 'values' object."
        )
    if processable < len(data):
        logger.warning(
            "%d of %d event records are not processable (missing %s, or a "
            "malformed eventDetail.values) and will be skipped.",
            len(data) - processable, len(data),
            ", ".join(sorted(_ESSENTIAL_EVENT_FIELDS)),
        )
    return None


def _coerce_value(value: Any, field_type: str) -> Any:
    """Coerce one value to its column's type.

    Deliberately gentler than the playground in one place: a `string` column's
    values are left EXACTLY as they are rather than run through str(). The
    playground stringifies them, but doing that here would turn numbers that
    models currently do raw Python arithmetic on into strings and break rules
    that work today. Blank handling and numeric/date coercion match.

    A value that will not convert is kept, not zeroed. Replacing it with 0.0
    (which the playground does) silently destroys the number and is exactly the
    failure the backend's _reconcile_field_types was added to prevent.
    """
    if _is_blankish(value):
        if field_type in ("decimal", "float"):
            return 0.0
        if field_type in ("integer", "int"):
            return 0
        return ""

    if field_type == "date":
        return _normalize_date_field_value(value)

    if field_type in ("decimal", "float"):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).strip().replace(",", ""))
        except (TypeError, ValueError):
            return value

    if field_type in ("integer", "int"):
        try:
            return int(float(str(value).strip().replace(",", "")))
        except (TypeError, ValueError):
            return value

    return value


def _coerce_event_rows(rows: List[Dict[str, Any]], samples: Dict[str, list],
                       event_id: str) -> Dict[str, str]:
    """Infer each field's type from its sampled values and coerce every row.

    Standard columns are skipped: they are built explicitly above and are
    already normalised, and inferring a type for them would rewrite a numeric
    instrument id as a float.
    """
    if not rows or not samples:
        return {}

    field_types = {
        name: _infer_field_datatype(vals) for name, vals in samples.items()
    }
    unconverted: Dict[str, int] = {}

    for row in rows:
        for key in list(row.keys()):
            if str(key).lower() in _STANDARD_ROW_COLUMNS:
                continue
            ftype = field_types.get(key)
            if ftype is None:
                continue
            before = row[key]
            after = _coerce_value(before, ftype)
            row[key] = after
            if (ftype in ("decimal", "float", "integer", "int")
                    and not _is_blankish(before)
                    and not isinstance(after, (int, float))):
                unconverted[key] = unconverted.get(key, 0) + 1

    for name, count in unconverted.items():
        logger.warning(
            "Event '%s' field '%s': %d value(s) typed as %s would not convert "
            "to a number and were left unchanged. Check the source data.",
            event_id, name, count, field_types.get(name),
        )
    return field_types


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------
def build_event_data_from_import(
    records: list,
    allowed_instruments: Optional[set] = None,
) -> List[Dict]:
    """
    Build event data rows from imported records.
    Groups rows by eventId. Each value entry in eventDetail.values becomes one data row.

    If allowed_instruments is given, standard event records whose outer instrumentId
    is not in that set are skipped. Custom/reference events are never filtered.

    Returns a list of dicts: [{"event_name": "...", "data_rows": [...]}]
    """
    usable = [evt for evt in records if _record_is_processable(evt)]
    skipped_records = len(records) - len(usable)
    if skipped_records:
        logger.warning(
            "Skipped %d unprocessable event record(s) of %d during extraction.",
            skipped_records, len(records),
        )

    event_ids = list({evt.get("eventId", "") for evt in usable})
    custom_events = {eid for eid in event_ids if _is_custom_event(usable, eid)}

    event_rows: dict = defaultdict(list)
    seen_custom_value_ids: dict = defaultdict(set)
    # field -> sample values, per event, capped. Gathered on the way through so
    # types can be inferred without a second pass over the source JSON.
    field_samples: dict = defaultdict(lambda: defaultdict(list))

    for event in usable:
        event_id = event.get("eventId", "")
        is_custom = event_id in custom_events

        outer_posting = _parse_import_date(_ci_get(event, "postingDate"))
        outer_effective = _parse_import_date(_ci_get(event, "effectiveDate"))
        outer_instrument = _as_clean_str(_ci_get(event, "instrumentId"))

        # Filter standard events by allowed instrument list
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue

        raw_values = (event.get("eventDetail") or {}).get("values") or {}
        for value_id, row_val in raw_values.items():
            if is_custom:
                if value_id in seen_custom_value_ids[event_id]:
                    continue
                seen_custom_value_ids[event_id].add(value_id)

            if not isinstance(row_val, dict):
                continue

            if is_custom:
                row: dict = {}
            else:
                inner_posting = _parse_import_date(_ci_get(row_val, "PostingDate")) or outer_posting
                inner_effective = _parse_import_date(_ci_get(row_val, "EffectiveDate")) or outer_effective
                inner_instrument = _as_clean_str(_ci_get(row_val, "InstrumentId")) or outer_instrument
                inner_subinstr = _as_clean_str(
                    _ci_get(row_val, "AttributeId", "SubInstrumentId")
                )
                row = {
                    "PostingDate": inner_posting,
                    "EffectiveDate": inner_effective,
                    "InstrumentId": inner_instrument,
                    "SubInstrumentId": inner_subinstr,
                }

            for key, value in row_val.items():
                if key in _IMPORT_FIXED_KEYS:
                    continue
                if isinstance(value, dict) and "$date" in value:
                    row[key] = _parse_import_date(value)
                elif isinstance(value, dict) and "$oid" in value:
                    continue
                else:
                    row[key] = value
                    if str(key).lower() not in _STANDARD_ROW_COLUMNS:
                        _samples = field_samples[event_id][key]
                        if len(_samples) < _INFER_SAMPLE_CAP:
                            _samples.append(value)

            event_rows[event_id].append(row)

    # Reconcile each field's type against its values and coerce, so the model
    # receives the same shapes it saw in the playground (which coerces at
    # ingest -- see backend/server.py). Without this a blank numeric cell
    # arrived as None and raw arithmetic in the generated template raised
    # "unsupported operand type(s) for +: 'float' and 'NoneType'".
    for _eid, _rows in event_rows.items():
        _coerce_event_rows(_rows, field_samples.get(_eid, {}), _eid)

    # Activity-data only: enforce canonical sort
    # (instrumentid ASC, postingdate ASC, effectivedate ASC, subinstrumentid ASC)
    # for every non-custom event. Custom/reference events are left untouched.
    for _eid, _rows in event_rows.items():
        if _eid not in custom_events:
            _sort_activity_rows(_rows)

    return [
        {"event_name": eid, "data_rows": rows}
        for eid, rows in event_rows.items()
    ]


def build_event_definitions_from_import(
    records: list,
    allowed_instruments: Optional[set] = None,
) -> List[Dict]:
    """
    Derive event definitions (field names + inferred types) from imported records.
    Returns a list of dicts with event_name, fields, eventType, eventTable.
    """
    event_fields: dict = defaultdict(lambda: defaultdict(list))

    usable = [evt for evt in records if _record_is_processable(evt)]
    # Classify once per event id. This used to call _is_custom_event(records,
    # ...) inside the per-record loop, and that helper rescans every record --
    # O(n^2) over the whole payload.
    custom_events = {
        eid for eid in {evt.get("eventId", "") for evt in usable}
        if _is_custom_event(usable, eid)
    }

    for event in usable:
        event_id = event.get("eventId", "")
        outer_instrument = _as_clean_str(_ci_get(event, "instrumentId"))
        is_custom = event_id in custom_events
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue
        for row_val in ((event.get("eventDetail") or {}).get("values") or {}).values():
            if not isinstance(row_val, dict):
                continue
            for key, value in row_val.items():
                if key not in _IMPORT_FIXED_KEYS:
                    _samples = event_fields[event_id][key]
                    if len(_samples) < _INFER_SAMPLE_CAP:
                        _samples.append(value)

    definitions = []
    ts = datetime.now(timezone.utc).isoformat()
    for event_id, fields in event_fields.items():
        field_list = [
            {"name": fn, "datatype": _infer_field_datatype(sv)}
            for fn, sv in fields.items()
        ]
        is_custom = event_id in custom_events
        definitions.append({
            "id": str(uuid.uuid4()),
            "event_name": event_id,
            "fields": field_list,
            "eventType": "reference" if is_custom else "activity",
            "eventTable": "custom" if is_custom else "standard",
            "created_at": ts,
        })
    return definitions


# ---------------------------------------------------------------------------
# Merging: combine multiple events by instrumentid
# ---------------------------------------------------------------------------
def get_latest_data_per_instrument(data_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Get latest postingdate row per instrumentid (case-insensitive field matching).

    Defensively skips any row that is not a dict (e.g. a stringified JSON object
    that slipped through during import). Such rows are logged so the user can fix
    the source data instead of seeing a cryptic ``'str' object has no attribute 'items'``.
    """
    latest_data = {}
    for idx, row in enumerate(data_rows):
        if not isinstance(row, dict):
            logger.warning(
                "Skipping non-dict row at index %d in event data (got %s). "
                "Re-import the source file — each row must be a JSON object.",
                idx, type(row).__name__,
            )
            continue
        instrument_id = get_field_case_insensitive(row, 'instrumentid', '')
        posting_date = get_field_case_insensitive(row, 'postingdate', '')

        if not instrument_id:
            continue

        if instrument_id not in latest_data:
            latest_data[instrument_id] = row
        else:
            existing_date = get_field_case_insensitive(latest_data[instrument_id], 'postingdate', '')
            if posting_date > existing_date:
                latest_data[instrument_id] = row

    return latest_data


_STANDARD_ROW_KEYS = {'instrumentid', 'postingdate', 'effectivedate', 'subinstrumentid'}


def _report_collapsed_subinstruments(event_name, data_rows, latest_data, seen):
    """Warn when the instrument-grain merge is about to discard real values.

    ``merge_event_data_by_instrument`` keeps ONE row per instrumentid. When an
    event is sub-instrument grained, the surviving row is simply the first
    after the canonical sort (ties on postingDate are not broken by anything
    meaningful), so a differing value on another sub-instrument disappears --
    e.g. three REVENUE_BALANCE rows where only SSP6KXFYZJZB carries
    -2.4988 collapse to the alphabetically-first row's 0, and a rule reading
    the merged field nets off nothing.

    Behaviour is unchanged; this only makes the loss visible. Reported once
    per (event, field) so a wide batch cannot flood the log. Values are
    business data, so this logs at WARNING only when divergence is real.
    """
    if not logger.isEnabledFor(logging.WARNING):
        return
    by_instrument = defaultdict(list)
    for row in data_rows:
        if not isinstance(row, dict):
            continue
        iid = get_field_case_insensitive(row, 'instrumentid', '')
        if iid:
            by_instrument[iid].append(row)

    for iid, rows in by_instrument.items():
        if len(rows) < 2:
            continue
        kept = latest_data.get(iid)
        if not isinstance(kept, dict):
            continue
        kept_sub = get_field_case_insensitive(kept, 'subinstrumentid', '')
        discarded = [r for r in rows if r is not kept]

        # (a) fields the kept row HAS, where a discarded row disagrees
        for key in list(kept.keys()):
            if key.lower() in _STANDARD_ROW_KEYS or (event_name, key) in seen:
                continue
            kept_val = kept[key]
            diverging = [
                (get_field_case_insensitive(r, 'subinstrumentid', ''), r[key])
                for r in discarded if key in r and r[key] != kept_val
            ]
            if diverging:
                seen.add((event_name, key))
                logger.warning(
                    "%s.%s differs across %d sub-instrument rows for instrument %s. "
                    "The instrument-grain merge keeps sub=%s value=%r and DISCARDS %s. "
                    "If the rule posts per sub-instrument, read this with "
                    "collect_by_subinstrument(%s.%s) instead of the merged field.",
                    event_name, key, len(rows), iid, kept_sub, kept_val,
                    '; '.join(f"sub={s} value={v!r}" for s, v in diverging[:5]),
                    event_name, key,
                )

        # (b) fields that exist ONLY on discarded rows -- lost entirely
        for r in discarded:
            for key, val in r.items():
                if key.lower() in _STANDARD_ROW_KEYS or key in kept:
                    continue
                if (event_name, key) in seen:
                    continue
                seen.add((event_name, key))
                logger.warning(
                    "%s.%s exists only on sub-instrument %s (value=%r) for instrument "
                    "%s and is ABSENT from the merged row (kept sub=%s). A rule "
                    "referencing the merged field will see nothing; use "
                    "collect_by_subinstrument(%s.%s).",
                    event_name, key,
                    get_field_case_insensitive(r, 'subinstrumentid', ''), val,
                    iid, kept_sub, event_name, key,
                )


def merge_event_data_by_instrument(event_data_dict: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Merge data from multiple events by instrumentid.
    Each event's fields are prefixed with EVENT_NAME_ to avoid conflicts.
    Also provides event-specific postingdate, effectivedate, and subinstrumentid.

    Hierarchy: postingDate → instrumentId → subInstrumentId → effectiveDates

    If subInstrumentId is missing or null, it defaults to "1".
    Iterates ALL instruments — no limit.
    """
    merged_data = {}
    bad_row_events = []
    _collapse_seen = set()

    for event_name, data_rows in event_data_dict.items():
        # Pre-flight check: record any row that is not a dict so the offending
        # event/row can be named in the diagnostic below.
        if isinstance(data_rows, list):
            for idx, row in enumerate(data_rows):
                if not isinstance(row, dict):
                    bad_row_events.append((event_name, idx, type(row).__name__))
        _safe_rows = data_rows if isinstance(data_rows, list) else []
        latest_data = get_latest_data_per_instrument(_safe_rows)
        # Surface any sub-instrument values this instrument-grain merge drops.
        _report_collapsed_subinstruments(
            event_name, _safe_rows, latest_data, _collapse_seen
        )

        for instrument_id, row in latest_data.items():
            if instrument_id not in merged_data:
                subinstrument_id = get_field_case_insensitive(row, 'subinstrumentid', '')
                if not subinstrument_id or subinstrument_id == 'None' or str(subinstrument_id).strip() == '':
                    subinstrument_id = '1'

                merged_data[instrument_id] = {
                    'instrumentid': instrument_id,
                    'subinstrumentid': str(subinstrument_id),
                    'postingdate': get_field_case_insensitive(row, 'postingdate', ''),
                    'effectivedate': get_field_case_insensitive(row, 'effectivedate', ''),
                }

            event_postingdate = get_field_case_insensitive(row, 'postingdate', '')
            event_effectivedate = get_field_case_insensitive(row, 'effectivedate', '')
            event_subinstrumentid = get_field_case_insensitive(row, 'subinstrumentid', '')
            if not event_subinstrumentid or event_subinstrumentid == 'None' or str(event_subinstrumentid).strip() == '':
                event_subinstrumentid = '1'

            merged_data[instrument_id][f"{event_name}_postingdate"] = event_postingdate
            merged_data[instrument_id][f"{event_name}_effectivedate"] = event_effectivedate
            merged_data[instrument_id][f"{event_name}_subinstrumentid"] = str(event_subinstrumentid)

            if not isinstance(row, dict):
                # Already logged above; skip safely.
                continue
            for key, value in row.items():
                key_lower = key.lower()
                if key_lower not in ['instrumentid', 'postingdate', 'effectivedate', 'subinstrumentid']:
                    prefixed_key = f"{event_name}_{key}"
                    merged_data[instrument_id][prefixed_key] = value
                    merged_data[instrument_id][key] = value

    if bad_row_events:
        # Name the first offending row so the source file can be fixed.
        #
        # NOTE: the upstream FyntracPythonModel copy raises ValueError here.
        # That is deliberately NOT done: these rows are already skipped today
        # and the good instruments still process, so raising would turn one
        # malformed row into a whole-batch (all-instrument) abort. Logged
        # instead, which keeps the diagnostic without the availability change.
        # Summarise EVERY affected event, not just the first. With one line for
        # the first bad row only, a second event's malformed data was invisible
        # and the run's row count could not be reconciled.
        per_event: Dict[str, List[tuple]] = defaultdict(list)
        for evt, idx, kind in bad_row_events:
            per_event[evt].append((idx, kind))
        for evt, items in per_event.items():
            first_idx, first_kind = items[0]
            logger.error(
                "Event '%s' has malformed data: %d row(s) skipped, first is "
                "row #%d (a %s, not an object). Re-import the source file — "
                "each row must be a JSON object. Skipped rows: %s",
                evt, len(items), first_idx, first_kind,
                ", ".join(str(i) for i, _ in items[:20])
                + (" ..." if len(items) > 20 else ""),
            )

    return list(merged_data.values())


def filter_event_data_by_posting_date(
    event_data_dict: Dict[str, List[Dict]],
    posting_date: str,
    event_metadata: Optional[Dict[str, Dict]] = None,
) -> Dict[str, List[Dict]]:
    """Filter each event's rows to only those matching the given posting_date.

    Reference events (e.g. CATALOG) have no postingdate column — filtering
    them by date would discard every row and break collect_all() lookups in
    the generated template. When ``event_metadata`` says an event's
    ``eventType`` is ``'reference'``, its rows are passed through unchanged.
    Activity events are scoped to ``posting_date`` and re-sorted in the
    canonical activity-data order so that collect_by_instrument() /
    collect_all() and similar primitives iterate rows deterministically.
    """
    target = posting_date.strip()
    filtered: Dict[str, List[Dict]] = {}
    for event_name, rows in event_data_dict.items():
        safe_rows = rows if isinstance(rows, list) else []
        meta = (event_metadata or {}).get(event_name) or {}
        if str(meta.get("eventType", "activity")).lower() == "reference":
            # Reference tables have no postingdate — keep all rows untouched.
            filtered[event_name] = list(safe_rows)
            continue
        scoped = [
            row for row in safe_rows
            if isinstance(row, dict)
            and str(get_field_case_insensitive(row, "postingdate", "")).strip() == target
        ]
        _sort_activity_rows(scoped)
        filtered[event_name] = scoped
    return filtered


# ---------------------------------------------------------------------------
# Main entry point: JSON → ready-to-run data
# ---------------------------------------------------------------------------
def transform(
    records: list,
    posting_date: str,
) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
    """
    Full transformation pipeline: raw import JSON → (event_data, raw_event_data).

    The input JSON must be the EXACT same format that the DSL Studio UI receives
    when you click the Import button in the left sidebar — an array of event
    objects each containing instrumentId, eventId, eventName, postingDate,
    effectiveDate, status, _class, and an eventDetail with a values dict.

    Custom/reference event data is already included per-instrument in the
    incoming JSON from the main repo, so no separate broadcast is needed.

    Processing steps:
      1. Validate incoming JSON structure
      2. Extract per-event data rows from eventDetail.values
      3. Merge all events by instrument, scoped to the given posting date
      4. Return merged rows + raw data for collect() functions

    Args:
        records: The raw JSON array (same format as uploaded to DSL Studio Import).
        posting_date: Required. Only rows matching this posting date are processed.

    Returns:
        A tuple of:
        - event_data: List of merged row dicts (one per instrument), ready for
                      the generated Python template's process_event_data().
        - raw_event_data: Dict of event_name → list of raw rows, needed for
                          collect() functions in the generated template.

    Iterates ALL instruments in the data — no limit.
    """
    if not posting_date or not posting_date.strip():
        raise ValueError("posting_date is required. Specify which posting date to process.")
    # Validate
    error = validate_import_json(records)
    if error:
        raise ValueError(error)

    # Build per-event data rows (all instruments — no filtering)
    event_data_list = build_event_data_from_import(records, allowed_instruments=None)
    if not event_data_list:
        raise ValueError("No event data rows could be extracted from the input.")

    # Build dict of event_name → rows
    all_event_data: Dict[str, List[Dict]] = {}
    for ed in event_data_list:
        all_event_data[ed["event_name"]] = ed["data_rows"]

    # Build per-event metadata so the posting-date filter can recognise reference
    # tables (CATALOG-style) and pass them through without dropping every row.
    definitions = build_event_definitions_from_import(records, allowed_instruments=None)
    event_metadata: Dict[str, Dict] = {
        d["event_name"]: {"eventType": d.get("eventType", "activity")}
        for d in definitions
    }

    # raw_event_data is restricted to the requested posting date so that
    # collect_by_instrument() / collect_all() — which otherwise span every date
    # in the dataset — only see rows for the posting date being processed.
    # collect() already filters by date and is unaffected. Reference events
    # are passed through unchanged (they have no postingdate).
    scoped = filter_event_data_by_posting_date(all_event_data, posting_date, event_metadata)
    raw_event_data = scoped

    # Merge all events by instrument, scoped to the given posting date
    merged_data = merge_event_data_by_instrument(scoped)

    if not merged_data:
        raise ValueError("No instrument data found after merging events for the given posting date.")

    return merged_data, raw_event_data
