"""
Diagnostic: compare three execution modes for SO1 against RevenueFinal111 to
identify which one the user's other service is using.

Mode A (canonical): run_from_json per posting_date — raw_event_data is scoped
                    to the current date only (this is what FyntracPythonModel's
                    documented API does).
Mode B: same as A but everything in one Python process, no fresh module
        state between dates (state-leak test).
Mode C: run() with unfiltered raw_event_data containing ALL dates, called once
        per posting_date via override_postingdate.
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from FyntracPythonModel.model_runner import ModelRunner
from FyntracPythonModel.data_transformer import (
    build_event_data_from_import,
    build_event_definitions_from_import,
    merge_event_data_by_instrument,
)

ARTIFACT_PATH = os.path.join(ROOT, "exports", "RevenueFinal111.dsl_template_artifact.json")
EVENTS_PATH = os.path.join(ROOT, "exports", "Revenue.EventHistory.json")
TARGET = "SO1"


def _load():
    with open(ARTIFACT_PATH, encoding="utf-8") as f:
        artifact = json.load(f)
    with open(EVENTS_PATH, encoding="utf-8") as f:
        records = json.load(f)
    return artifact["python_code"], records


def _so1(txns):
    return [t for t in txns if str(t.get("instrumentid")) == TARGET]


def _print(label, txns):
    print(f"\n=== {label}  ({len(txns)} txns for {TARGET}) ===")
    for t in txns:
        print(f"  posting={t['postingdate']}  type={t['transactiontype']:<20}  sub={t['subinstrumentid']:<5}  amount={t['amount']}")


def mode_a(python_code, records, dates):
    """Canonical per-date isolated run."""
    runner = ModelRunner()
    out = []
    for pd in dates:
        r = runner.run_from_json(python_code=python_code, raw_json_records=records, posting_date=pd)
        out.extend(_so1(r.get("transactions", [])))
    return out


def mode_c_unfiltered(python_code, records, dates):
    """Run once per date but pass FULL unfiltered raw_event_data spanning all dates."""
    # Build full unfiltered event data
    event_list = build_event_data_from_import(records)
    full_raw = {ed["event_name"]: ed["data_rows"] for ed in event_list}
    defs = build_event_definitions_from_import(records)
    # event_metadata not needed here
    # For event_data (merged per-instrument), we still need to merge — use ALL
    merged = merge_event_data_by_instrument(full_raw)
    runner = ModelRunner()
    out = []
    for pd in dates:
        r = runner.run(
            python_code=python_code,
            event_data=merged,
            raw_event_data=full_raw,
            override_postingdate=pd,
        )
        out.extend(_so1(r.get("transactions", [])))
    return out


def main():
    python_code, records = _load()
    so1_dates = sorted({r["postingDate"] for r in records if r.get("instrumentId") == TARGET})
    fmt = lambda d: f"{str(d)[:4]}-{str(d)[4:6]}-{str(d)[6:8]}"
    dates = [fmt(d) for d in so1_dates]
    print(f"Posting dates for {TARGET}: {dates}")

    a = mode_a(python_code, records, dates)
    _print("MODE A: canonical run_from_json per date (raw scoped to current date)", a)

    c = mode_c_unfiltered(python_code, records, dates)
    _print("MODE C: run() with FULL raw_event_data spanning all dates", c)


if __name__ == "__main__":
    main()
