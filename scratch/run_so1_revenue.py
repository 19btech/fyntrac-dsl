"""
One-off driver: run RevenueFinal111 template against Revenue.EventHistory.json
using FyntracPythonModel and print all transactions produced for SO1.
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from FyntracPythonModel.model_runner import ModelRunner

ARTIFACT_PATH = os.path.join(ROOT, "exports", "RevenueFinal111.dsl_template_artifact.json")
EVENTS_PATH = os.path.join(ROOT, "exports", "Revenue.EventHistory.json")
TARGET_INSTRUMENT = "SO1"


def _format_yyyymmdd(d):
    s = str(d)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def main() -> int:
    with open(ARTIFACT_PATH, encoding="utf-8") as f:
        artifact = json.load(f)
    python_code = artifact["python_code"]
    template_name = artifact.get("template_name", "<unknown>")
    print(f"Template: {template_name}  (version {artifact.get('version')})")

    with open(EVENTS_PATH, encoding="utf-8") as f:
        records = json.load(f)
    print(f"Loaded {len(records)} event records from {os.path.basename(EVENTS_PATH)}")

    so1_dates = sorted({r["postingDate"] for r in records if r.get("instrumentId") == TARGET_INSTRUMENT})
    posting_dates = [_format_yyyymmdd(d) for d in so1_dates]
    print(f"{TARGET_INSTRUMENT} posting dates: {posting_dates}")

    runner = ModelRunner()
    all_so1_txns = []
    errors = []

    for posting_date in posting_dates:
        result = runner.run_from_json(
            python_code=python_code,
            raw_json_records=records,
            posting_date=posting_date,
        )
        if result.get("error"):
            errors.append((posting_date, result["error"]))
            continue
        for txn in result.get("transactions", []):
            if str(txn.get("instrumentid")) == TARGET_INSTRUMENT:
                all_so1_txns.append(txn)

    print()
    print("=" * 80)
    print(f"Transactions for {TARGET_INSTRUMENT}: {len(all_so1_txns)}")
    print("=" * 80)
    for i, txn in enumerate(all_so1_txns, start=1):
        print(
            f"{i:>3}. posting={txn.get('postingdate')}  "
            f"effective={txn.get('effectivedate')}  "
            f"sub={txn.get('subinstrumentid')}  "
            f"type={txn.get('transactiontype'):<22}  "
            f"amount={txn.get('amount')}"
        )

    if errors:
        print()
        print("Errors encountered:")
        for pd, err in errors:
            print(f"  [{pd}] {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
