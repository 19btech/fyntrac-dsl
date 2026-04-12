"""
TNT001 EventHistory → Master Event Data Converter  (v3)
========================================================
Each data_row is structured as:

  Fixed columns (always first, always present):
    1. PostingDate      (normalised to YYYY-MM-DD)
    2. EffectiveDate    (normalised to YYYY-MM-DD)
    3. InstrumentId
    4. AttributeId

  Dynamic columns (vary by event type, appended after fixed columns):
    e.g. ATTRIBUTE_INTEREST_RATE_CURRENT,
         BALANCES_ENDINGBALANCE_UNPAID_PRINCIPAL_BALANCE,
         BALANCES_ENDINGBALANCE_ACCRUED_INTEREST_RECEIVABLE, ...

  event_name lives on the record wrapper, NOT inside data_rows.

Usage:
    python tnt001_to_master_event_converter_v3.py \
        --input  TNT001_EventHistory_SBO.json \
        --output converted_master_event_SBO_v3.json

Requirements:
    Python 3.7+  (no external dependencies)
"""

import json
import uuid
import argparse
from datetime import datetime, timezone


# Keys consumed into the fixed columns — excluded from dynamic section
FIXED_KEYS = {"PostingDate", "EffectiveDate", "InstrumentId", "AttributeId"}


def parse_date(val) -> str:
    """Normalise a date to YYYY-MM-DD string."""
    if isinstance(val, dict) and "$date" in val:
        return val["$date"][:10]
    if isinstance(val, int):
        s = str(val)
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(val)


def build_data_row(raw_row: dict) -> dict:
    """
    Build a data_row with 4 fixed columns first, then dynamic columns.
    event_name is NOT included here — it lives on the parent record.
    """
    row = {
        "PostingDate":   parse_date(raw_row.get("PostingDate", "")),
        "EffectiveDate": parse_date(raw_row.get("EffectiveDate", "")),
        "InstrumentId":  raw_row.get("InstrumentId", ""),
        "AttributeId":   raw_row.get("AttributeId", ""),
    }
    # Append dynamic columns
    for key, value in raw_row.items():
        if key not in FIXED_KEYS:
            row[key] = value
    return row


def convert_event(tnt_event: dict, created_at: str) -> dict:
    raw_values: dict = tnt_event.get("eventDetail", {}).get("values", {})
    data_rows = [build_data_row(row) for row in raw_values.values()]

    return {
        "_id":        tnt_event["_id"],
        "id":         str(uuid.uuid4()),
        "event_name": tnt_event["eventId"],
        "data_rows":  data_rows,
        "created_at": created_at,
    }


def convert(input_path: str, output_path: str) -> None:
    print(f"Reading  : {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        tnt_records: list = json.load(f)

    created_at = datetime.now(timezone.utc).isoformat()
    master_records = [convert_event(rec, created_at) for rec in tnt_records]

    print(f"Writing  : {output_path}  ({len(master_records)} records)")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(master_records, f, indent=2)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert TNT001 EventHistory JSON to master_event_data format."
    )
    parser.add_argument("--input",  "-i", default="TNT001_EventHistory_SBO.json")
    parser.add_argument("--output", "-o", default="converted_master_event_SBO_v3.json")
    args = parser.parse_args()
    convert(args.input, args.output)
