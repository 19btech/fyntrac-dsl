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

import re
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from FyntracPythonModel.dsl_functions import normalize_date
except ImportError:
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
    """
    for event in records:
        if event.get("eventId") != event_id:
            continue
        outer = (event.get("instrumentId") or event.get("InstrumentId") or "").strip()
        if not outer:
            continue
        for row_val in event.get("eventDetail", {}).get("values", {}).values():
            if not isinstance(row_val, dict):
                continue
            inner = (row_val.get("instrumentId") or row_val.get("InstrumentId") or "").strip()
            if inner and inner == outer:
                return False
    return True


def _infer_field_datatype(values: list) -> str:
    """Infer the best datatype for a field from a list of sample values."""
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return "boolean"
        if isinstance(v, dict) and "$date" in v:
            return "date"
        if isinstance(v, str):
            if re.match(r"^\d{4}-\d{2}-\d{2}", v):
                return "date"
            return "string"
        if isinstance(v, float) and v != int(v):
            return "decimal"
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


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_import_json(data: Any) -> Optional[str]:
    """
    Validate that the input JSON matches the expected import format.
    Returns an error message string if invalid, or None if valid.
    """
    if not isinstance(data, list):
        return "Input must be a JSON array of event objects."
    if len(data) == 0:
        return "The JSON array is empty — no events to process."
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            return f"Item at index {i} is not a JSON object."
        missing = REQUIRED_EVENT_FIELDS - item.keys()
        if missing:
            return f"Item at index {i} is missing required fields: {', '.join(sorted(missing))}."
        if not isinstance(item.get("eventDetail"), dict):
            return f"Item at index {i}: 'eventDetail' must be a JSON object."
        if "values" not in item["eventDetail"]:
            return f"Item at index {i}: 'eventDetail' must contain a 'values' field."
    return None


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
    event_ids = list({evt.get("eventId", "") for evt in records})
    custom_events = {eid for eid in event_ids if _is_custom_event(records, eid)}

    event_rows: dict = defaultdict(list)
    seen_custom_value_ids: dict = defaultdict(set)

    for event in records:
        event_id = event.get("eventId", "")
        is_custom = event_id in custom_events

        outer_posting = _parse_import_date(event.get("postingDate") or event.get("PostingDate", ""))
        outer_effective = _parse_import_date(event.get("effectiveDate") or event.get("EffectiveDate", ""))
        outer_instrument = (event.get("instrumentId") or event.get("InstrumentId", "")).strip()

        # Filter standard events by allowed instrument list
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue

        raw_values = event.get("eventDetail", {}).get("values", {})
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
                inner_posting = _parse_import_date(
                    row_val.get("PostingDate") or row_val.get("postingDate")
                ) or outer_posting
                inner_effective = _parse_import_date(
                    row_val.get("EffectiveDate") or row_val.get("effectiveDate")
                ) or outer_effective
                inner_instrument = (
                    row_val.get("InstrumentId") or row_val.get("instrumentId") or outer_instrument
                )
                inner_subinstr = str(
                    row_val.get("AttributeId") or row_val.get("attributeId") or ""
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

            event_rows[event_id].append(row)

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

    for event in records:
        event_id = event.get("eventId", "")
        outer_instrument = (event.get("instrumentId") or event.get("InstrumentId") or "").strip()
        is_custom = _is_custom_event(records, event_id)
        if not is_custom and allowed_instruments is not None and outer_instrument not in allowed_instruments:
            continue
        for row_val in event.get("eventDetail", {}).get("values", {}).values():
            if not isinstance(row_val, dict):
                continue
            for key, value in row_val.items():
                if key not in _IMPORT_FIXED_KEYS:
                    event_fields[event_id][key].append(value)

    definitions = []
    ts = datetime.now(timezone.utc).isoformat()
    for event_id, fields in event_fields.items():
        field_list = [
            {"name": fn, "datatype": _infer_field_datatype(sv)}
            for fn, sv in fields.items()
        ]
        is_custom = _is_custom_event(records, event_id)
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
    """Get latest postingdate row per instrumentid."""
    latest_data = {}
    for row in data_rows:
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


def merge_event_data_by_instrument(event_data_dict: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Merge data from multiple events by instrumentid.
    Each event's fields are prefixed with EVENT_NAME_ to avoid conflicts.
    Iterates ALL instruments — no limit.
    """
    merged_data = {}

    for event_name, data_rows in event_data_dict.items():
        latest_data = get_latest_data_per_instrument(data_rows)

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

            for key, value in row.items():
                key_lower = key.lower()
                if key_lower not in ['instrumentid', 'postingdate', 'effectivedate', 'subinstrumentid']:
                    prefixed_key = f"{event_name}_{key}"
                    merged_data[instrument_id][prefixed_key] = value
                    merged_data[instrument_id][key] = value

    return list(merged_data.values())


def filter_event_data_by_posting_date(
    event_data_dict: Dict[str, List[Dict]],
    posting_date: str,
) -> Dict[str, List[Dict]]:
    """Filter each event's rows to only those matching the given posting_date."""
    target = posting_date.strip()
    filtered: Dict[str, List[Dict]] = {}
    for event_name, rows in event_data_dict.items():
        filtered[event_name] = [
            row for row in rows
            if str(get_field_case_insensitive(row, "postingdate", "")).strip() == target
        ]
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

    # raw_event_data includes everything (activity + reference) for collect() functions
    raw_event_data = all_event_data

    # Merge all events by instrument, scoped to the given posting date
    scoped = filter_event_data_by_posting_date(all_event_data, posting_date)
    merged_data = merge_event_data_by_instrument(scoped)

    if not merged_data:
        raise ValueError("No instrument data found after merging events for the given posting date.")

    return merged_data, raw_event_data
