"""
Experiment: try to reproduce SO1 April divergence purely by carrying state
across runs in one Python process.

We compare:
  RUN-X: run April in isolation (clean process state)
  RUN-Y: run Jan, Feb, Mar, Apr, May sequentially in the SAME process
         and look at April's output

If Y differs from X, that proves state leak across calls.
"""
import json, os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from FyntracPythonModel.model_runner import ModelRunner

art = json.load(open(os.path.join(ROOT, "exports", "RevenueFinal111.dsl_template_artifact.json")))
records = json.load(open(os.path.join(ROOT, "exports", "Revenue.EventHistory.json")))
code = art["python_code"]

DATES = ["2022-01-31", "2022-02-28", "2022-03-31", "2022-04-30", "2022-05-31"]
TARGET = "SO1"


def so1(txns):
    return [(t["postingdate"], t["transactiontype"], t["subinstrumentid"], t["amount"])
            for t in txns if str(t.get("instrumentid")) == TARGET]


def run_one(pd):
    r = ModelRunner().run_from_json(python_code=code, raw_json_records=records, posting_date=pd)
    return [t for t in so1(r["transactions"]) if t[0] == pd]


# RUN-X: April only
x = run_one("2022-04-30")
print("RUN-X (April only, fresh ModelRunner):")
for row in x: print("  ", row)

# RUN-Y: same Python process, same single ModelRunner instance, all 5 dates
runner = ModelRunner()
print("\nRUN-Y (Jan->May sequential, same ModelRunner, same process):")
for d in DATES:
    r = runner.run_from_json(python_code=code, raw_json_records=records, posting_date=d)
    if d == "2022-04-30":
        for row in [t for t in so1(r["transactions"]) if t[0] == d]:
            print("  ", row)

# RUN-Z: simulate caller passing FULL raw history into one April run.
# This forces collect()/for_each to see Feb invoice rows during April.
print("\nRUN-Z (April only but raw_event_data spans ALL months):")
from FyntracPythonModel.data_transformer import (
    build_event_data_from_import, merge_event_data_by_instrument
)
full = {ed["event_name"]: ed["data_rows"] for ed in build_event_data_from_import(records)}
merged = merge_event_data_by_instrument(full)
r = ModelRunner().run(python_code=code, event_data=merged,
                      raw_event_data=full, override_postingdate="2022-04-30")
seen = set()
for t in so1(r["transactions"]):
    if t[0] == "2022-04-30" and t not in seen:
        seen.add(t)
        print("  ", t)
