"""
TNT001 EventHistory → Master Event Definitions Generator
=========================================================
Reads TNT001_EventHistory_SBO.json and produces master_event_definitions.json.

For each unique event (by eventId), it:
  - Collects all dynamic field names (excludes fixed: PostingDate, EffectiveDate, InstrumentId, AttributeId)
  - Infers the datatype from actual values: date | string | boolean | integer | decimal
  - Sets eventType = "activity" (always)

Datatype inference rules (applied in order):
  1. bool              → "boolean"
  2. dict with $date   → "date"
  3. str matching YYYY-MM-DD → "date"
  4. str               → "string"
  5. float with decimal part → "decimal"
  6. int / whole float → "decimal"  (financial fields default to decimal)

Usage:
    python tnt001_to_event_definitions.py \
        --input  TNT001_EventHistory_SBO.json \
        --output master_event_definitions_generated.json

Requirements:
    Python 3.7+  (no external dependencies)
"""

import json
import uuid
import re
import argparse
from datetime import datetime, timezone
from collections import defaultdict


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Fields that are fixed/structural — excluded from event field definitions
FIXED_KEYS = {"PostingDate", "EffectiveDate", "InstrumentId", "AttributeId"}


# ---------------------------------------------------------------------------
# Datatype inference
# ---------------------------------------------------------------------------

def infer_datatype(values: list) -> str:
    """
    Infer the datatype of a field from its collected sample values.
    Checks all non-None values and returns the most specific type found.
    """
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

    # All values are numeric (int or whole float) — financial fields use decimal
    return "decimal"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_definitions(tnt_records: list, created_at: str) -> list:
    """
    Scan all TNT001 records, collect unique fields per event type,
    infer datatypes, and return master_event_definitions records.
    """
    # event_id -> {field_name -> [sample_values]}
    event_fields: dict = defaultdict(lambda: defaultdict(list))

    for event in tnt_records:
        event_id = event["eventId"]
        for row_val in event.get("eventDetail", {}).get("values", {}).values():
            for key, value in row_val.items():
                if key not in FIXED_KEYS:
                    event_fields[event_id][key].append(value)

    definitions = []
    for event_id, fields in event_fields.items():
        field_list = [
            {
                "name":     field_name,
                "datatype": infer_datatype(sample_values),
            }
            for field_name, sample_values in fields.items()
        ]

        definitions.append({
            "_id":        {"$oid": uuid.uuid4().hex[:24]},
            "id":         str(uuid.uuid4()),
            "event_name": event_id,
            "fields":     field_list,
            "eventType":  "activity",
            "created_at": created_at,
        })

    return definitions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def convert(input_path: str, output_path: str) -> None:
    print(f"Reading  : {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        tnt_records: list = json.load(f)

    created_at = datetime.now(timezone.utc).isoformat()
    definitions = build_definitions(tnt_records, created_at)

    print(f"Writing  : {output_path}  ({len(definitions)} event definitions)")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(definitions, f, indent=2)

    for d in definitions:
        print(f"  {d['event_name']}: {len(d['fields'])} fields")

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate master_event_definitions JSON from TNT001 EventHistory."
    )
    parser.add_argument("--input",  "-i", default="TNT001_EventHistory_SBO.json")
    parser.add_argument("--output", "-o", default="master_event_definitions_generated.json")
    args = parser.parse_args()
    convert(args.input, args.output)
