# FyntracParentCode — Model Runner for Main Repo

Copy this entire `FyntracParentCode/` folder into your main Fyntrac repository's Python codebase.

---

## What's In This Folder

| File                 | Purpose                                                                 |
|----------------------|-------------------------------------------------------------------------|
| `dsl_functions.py`   | The 145+ DSL function library (exact copy from the playground)          |
| `data_transformer.py`| Converts raw import JSON into the format the generated Python expects   |
| `model_runner.py`    | Executes the generated Python code and returns transactions             |
| `__init__.py`        | Python package marker                                                   |

---

## MongoDB Setup

### Collection: `dsl_template_artifacts`

This is the **only** collection you need in the main repo. It already exists in the shared MongoDB — the playground writes to it every time you save a template.

**Document shape:**

| Field           | Type   | Description                                        |
|-----------------|--------|----------------------------------------------------|
| `template_id`   | string | Unique ID of the template                          |
| `template_name` | string | Human-readable name (e.g., "LoanAmortization")     |
| `version`       | int    | Version number (increments on each save)           |
| `python_code`   | string | The generated Python code to execute               |
| `created_at`    | string | ISO timestamp                                      |
| `read_only`     | bool   | Always `true`                                      |

**To fetch the latest version of a model:**
```python
artifact = db.dsl_template_artifacts.find_one(
    {"template_name": "YourTemplateName"},
    sort=[("version", -1)]
)
python_code = artifact["python_code"]
```

---

## Environment / Config

| Variable    | Description                                           |
|-------------|-------------------------------------------------------|
| `MONGO_URL` | MongoDB connection string (same DB as the playground)  |
| `DB_NAME`   | Database name (e.g., `"fyntrac_dsl"`)                  |

No other configuration is needed.

---

## How to Use

### Option A: Feed Raw Import JSON (recommended)

Your main app already produces the JSON format used by DSL Studio's Import. Feed it directly — the transformer handles everything:

```python
import json
from pymongo import MongoClient
from FyntracParentCode.model_runner import ModelRunner

# 1. Connect to the shared MongoDB
client = MongoClient(MONGO_URL)
db = client["fyntrac_dsl"]

# 2. Load the model (latest version)
artifact = db.dsl_template_artifacts.find_one(
    {"template_name": "LoanAmortization"},
    sort=[("version", -1)]
)
python_code = artifact["python_code"]

# 3. Load the raw event JSON (same format as Import)
with open("events.json") as f:
    raw_records = json.load(f)

# 4. Run — processes ALL instruments, no limit
runner = ModelRunner()
result = runner.run_from_json(
    python_code=python_code,
    raw_json_records=raw_records,
    posting_date="2026-01-01",   # optional: scope to a posting date
)

# 5. Use results
if result["error"]:
    print(f"Error: {result['error']}")
else:
    print(f"Processed {result['instrument_count']} instruments")
    for txn in result["transactions"]:
        print(txn)
```

### Option B: Pre-transformed data

If you've already transformed the data yourself:

```python
runner = ModelRunner()
result = runner.run(
    python_code=python_code,
    event_data=merged_rows,           # list of row dicts (one per instrument)
    raw_event_data=raw_by_event,      # {"LoanEvent": [...], "PMT": [...]}
)
```

---

## Input JSON Format

The raw JSON is an **array of event records** — the exact same format your main app produces for the Import functionality:

```json
[
  {
    "instrumentId": "LOAN-001",
    "eventId": "INT_ACC",
    "eventName": "Interest Accrual",
    "postingDate": "2026-01-01",
    "effectiveDate": "2026-01-01",
    "status": "active",
    "_class": "com.fyntrac.common.entity.AccountingEvent",
    "eventDetail": {
      "values": {
        "row1": {
          "InstrumentId": "LOAN-001",
          "PostingDate": "2026-01-01",
          "EffectiveDate": "2026-01-01",
          "principal": 100000,
          "rate": 0.05,
          "term": 12
        }
      }
    }
  },
  {
    "instrumentId": "LOAN-002",
    "eventId": "INT_ACC",
    "eventName": "Interest Accrual",
    "postingDate": "2026-01-01",
    "effectiveDate": "2026-01-01",
    "status": "active",
    "_class": "com.fyntrac.common.entity.AccountingEvent",
    "eventDetail": {
      "values": {
        "row1": {
          "InstrumentId": "LOAN-002",
          "PostingDate": "2026-01-01",
          "EffectiveDate": "2026-01-01",
          "principal": 250000,
          "rate": 0.04,
          "term": 24
        }
      }
    }
  }
]
```

**Required fields per event record:** `instrumentId`, `eventId`, `eventName`, `postingDate`, `effectiveDate`, `status`, `eventDetail`, `_class`

---

## Output Shape

```python
{
    "transactions": [
        {
            "postingdate": "2026-01-01",
            "effectivedate": "2026-01-01",
            "instrumentid": "LOAN-001",
            "subinstrumentid": "1",
            "transactiontype": "INTEREST",
            "amount": 416.67
        },
        # ... transactions for ALL instruments
    ],
    "print_outputs": ["any print() calls from the DSL"],
    "error": None,              # or an error string
    "instrument_count": 50      # how many instruments were processed
}
```

---

## Keeping This In Sync

| What Changed in the Playground             | What to Do Here                                |
|--------------------------------------------|------------------------------------------------|
| Added/changed DSL functions                | Re-copy `backend/dsl_functions.py` → here      |
| Saved a new/updated template               | Nothing — it's in MongoDB, picked up automatically |
| Changed how Import transformation works    | Re-copy the relevant logic into `data_transformer.py` |
| Changed `execute_python_template` (rare)   | Update `model_runner.py` to match              |

The most common action: **re-copy `dsl_functions.py`** when you add new DSL functions in the playground. Everything else is rare.

---

## How It Works Internally

1. **`run_from_json()`** receives the raw event JSON (same format as Import)
2. **`data_transformer.transform()`** parses it into per-event rows, classifies activity vs reference events, and merges across all instruments
3. The merged data is passed to **`run()`** which:
   - Rewrites import paths in the generated Python so they find `dsl_functions.py` in this folder
   - Executes the generated code in a sandboxed context
   - Calls `process_event_data()` which iterates every instrument row and runs the DSL logic
   - Collects all `createTransaction()` calls and returns them
