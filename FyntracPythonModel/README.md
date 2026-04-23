# FyntracPythonModel — Optional Export Runtime

This package is intended for running exported/generated models outside the main DSL Studio backend.

In this repository, the primary application runtime is the FastAPI service in `backend/` started by `startup.sh`. This package is a secondary integration target, not the default server runtime.

This folder contains everything the main Fyntrac app needs to run
calculation models that were built and tested in the DSL Studio playground.

---

## What's In This Folder

| File                 | What It Does                                                         |
|----------------------|----------------------------------------------------------------------|
| `dsl_functions.py`   | All 152 financial functions (like `pv`, `compound_interest`, `schedule`, etc.). The generated Python code calls these functions. This is a copy of the same file from the playground. |
| `data_transformer.py`| Takes the raw event JSON (same format your main app already produces) and cleans it up into the shape the model needs. |
| `model_runner.py`    | Runs the model: takes the generated Python code + the cleaned-up data, loops through every instrument, and gives back the transactions. |
| `__init__.py`        | Empty file that tells Python this folder is a package (required for imports to work). |

---

## Setup Guide — Step by Step

### Step 1: Copy this folder into your main repo

Take the entire `FyntracPythonModel/` folder and place it inside your main
Fyntrac repository, wherever your Python code lives. For example:

```
your-main-repo/
    src/
        FyntracPythonModel/
            __init__.py
            dsl_functions.py
            data_transformer.py
            model_runner.py
        your_other_code/
            ...
```

Make sure `FyntracPythonModel/` is somewhere on your Python path so you can
import from it.

### Step 2: Set up the MongoDB collection

You need **one collection** called `dsl_template_artifacts`. This is where
the playground saves the generated Python code when you **deploy** a user
template (by clicking the 🚀 Rocket button on the template card in the
Template Library, which calls `POST /api/user-templates/{template_id}/deploy`).

Saving or editing a user template no longer writes to this collection —
deployment is now an explicit, opt-in action.

**If your main app and the playground share the same MongoDB database,
this collection already exists — you don't need to create it.**

If they use different databases, you'll need to either:
- Point both at the same database, OR
- Copy the documents from the playground's database to the main app's database

Here's what each document in this collection looks like:

```
{
    "template_id":   "abc-123",                    // unique ID
    "template_name": "InterestAccrual",            // the name you gave the template
    "version":       3,                            // goes up by 1 each time you save
    "python_code":   "import sys, os\n...",        // the actual generated Python code
    "created_at":    "2026-01-15T10:30:00+00:00",  // when it was saved
    "read_only":     true                          // always true
}
```

**You don't write to this collection from the main app. You only read from it.**
The playground writes to it when you save templates.

### Step 3: Set up environment variables

Your main app needs to know how to connect to the database:

| Variable    | What It Is                                                   | Example                                    |
|-------------|--------------------------------------------------------------|--------------------------------------------|
| `MONGO_URL` | The MongoDB connection string (same database as the playground) | `mongodb://localhost:27017`              |
| `DB_NAME`   | The name of the database (the backend defaults to `dsl_db`) | `dsl_db`                                  |

That's it. No other config needed.

### Step 4: Install Python dependencies

The code only needs standard Python libraries (`re`, `json`, `datetime`, `collections`).
No extra pip packages are required — everything is self-contained.

---

## How to Call It From Your Main App

### The Simple Version (one function call)

```python
import json
from pymongo import MongoClient
from FyntracPythonModel.model_runner import ModelRunner

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["dsl_db"]

# Get the model (the generated Python code) from the database
artifact = db.dsl_template_artifacts.find_one(
    {"template_name": "InterestAccrual"},
    sort=[("version", -1)]       # always get the latest version
)
python_code = artifact["python_code"]

# Get your event data — same JSON format your app already produces
raw_records = [...]  # your event JSON array

# Run the model
runner = ModelRunner()
result = runner.run_from_json(
    python_code=python_code,
    raw_json_records=raw_records,
    posting_date="2026-01-01",   # REQUIRED — which posting date to run
)

# Check results
if result["error"]:
    print(f"Something went wrong: {result['error']}")
else:
    print(f"Processed {result['instrument_count']} instruments")
    for txn in result["transactions"]:
        # Each txn is a dict — save it to your transactions table
        save_transaction(txn)
```

### Important: `posting_date` is required

You must always tell the runner which posting date to process. It will only
process instruments that have event data for that specific date. This is
the same posting date your EOD batch is running for.

If you don't pass it, you'll get an error back.

---

## Input: What Data Goes In

The input is a JSON array — the **exact same format** your main app already
creates for events. Each item in the array is one event record for one instrument.

### Example: Two loans, one event type

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

### Required fields in each event record

Every event record must have these fields:

| Field          | What It Is                                           |
|----------------|------------------------------------------------------|
| `instrumentId` | Which instrument (loan, bond, etc.) this belongs to  |
| `eventId`      | The event type code (e.g., `INT_ACC`, `PMT`)         |
| `eventName`    | Human-readable name of the event                     |
| `postingDate`  | The posting date of this event                       |
| `effectiveDate`| The effective date of this event                     |
| `status`       | Status string (e.g., `"active"`)                     |
| `eventDetail`  | Object containing a `values` dict with the actual data |
| `_class`       | Java class name (e.g., `"com.fyntrac.common.entity.AccountingEvent"`) |

### Inside `eventDetail.values`

This is where the actual numbers live. Each entry is one data row.
The keys inside (like `principal`, `rate`, `term`) are your event fields —
whatever you defined when you set up the event in the playground.

Standard fields like `InstrumentId`, `PostingDate`, `EffectiveDate` are
automatically handled. Everything else is treated as your custom data fields.

---

## Output: What Comes Back

You get a Python dictionary with four things:

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
        {
            "postingdate": "2026-01-01",
            "effectivedate": "2026-01-01",
            "instrumentid": "LOAN-002",
            "subinstrumentid": "1",
            "transactiontype": "INTEREST",
            "amount": 833.33
        }
    ],
    "print_outputs": [],
    "error": None,
    "instrument_count": 2
}
```

| Field              | What It Is                                                        |
|--------------------|-------------------------------------------------------------------|
| `transactions`     | A list of all output transactions for every instrument that was processed. Each transaction has 6 fields: `postingdate`, `effectivedate`, `instrumentid`, `subinstrumentid`, `transactiontype`, `amount`. |
| `print_outputs`    | Any `print()` calls from the DSL code (useful for debugging).     |
| `error`            | `None` if everything worked. If something went wrong, this is a string describing the error. |
| `instrument_count` | How many instruments were processed.                              |

---

## How It Works — Step by Step

Here's what happens when you call `runner.run_from_json()`:

### Step 1: Validate the input

The code checks that the JSON array is valid — every record has the required
fields, `eventDetail` exists, etc. If something is wrong, it stops and returns
an error message.

### Step 2: Parse the raw JSON into clean data rows

Each event record has data buried inside `eventDetail.values`. The transformer
pulls it out and creates simple flat rows like:

```
{PostingDate: "2026-01-01", InstrumentId: "LOAN-001", principal: 100000, rate: 0.05, term: 12}
```

Custom/reference event data (like rate tables or product configs) is already
included per-instrument in the incoming JSON from the main repo, so it gets
parsed just like any other event — no special handling needed.

### Step 3: Filter by the posting date you specified

Only rows that match the posting date you passed in are kept.
If your JSON has data for January, February, and March, but you said
`posting_date="2026-01-01"`, only January data is used.

### Step 4: Merge data across events for each instrument

If a single instrument (say LOAN-001) has data from multiple events
(like INT_ACC for interest and PMT for payments), all that data gets
combined into one row for that instrument.

Each field is prefixed with the event name to avoid name clashes:
- `INT_ACC_principal = 100000`
- `INT_ACC_rate = 0.05`
- `PMT_payment = 5000`

After this step, you have **one row per instrument** with all the data
from all events merged together.

### Step 5: Fix the import paths in the generated Python code

The generated Python code was created inside the playground and has import
statements like `from backend.dsl_functions import ...`. That won't work in
your main app. So the runner rewrites those imports to point to the
`dsl_functions.py` sitting in this folder.

### Step 6: Execute the generated Python in a safe sandbox

The runner compiles and runs the generated Python code. It uses a restricted
environment that blocks dangerous operations like `open()`, `eval()`, `exec()`,
etc. — so even if the generated code has a bug, it can't harm your system.

Running the code doesn't process any data yet — it just loads the
`process_event_data()` function into memory.

### Step 7: Loop through every instrument and run the DSL logic

Now the `process_event_data()` function runs. For each instrument:

1. It reads the merged data row (principal, rate, term, etc.)
2. It sets up the context (which instrument we're on, what the posting date is)
3. It runs your DSL logic line by line
4. Each `createTransaction()` call in the DSL adds a transaction to a list
5. It moves to the next instrument and repeats

If you have 500 instruments, this loop runs 500 times.

### Step 8: Collect and return the results

All the transactions from all instruments are gathered into one list.
Any `print()` calls from the DSL are captured separately.
Everything is packaged into the result dictionary and returned to your code.

---

## Keeping This Code Up to Date

| When This Happens in the Playground        | What You Need to Do                            |
|--------------------------------------------|------------------------------------------------|
| You add or change a DSL function           | Copy `backend/dsl_functions.py` from the playground into this folder, replacing the old one. |
| You save or edit a user template           | **Nothing in this folder.** Saving only updates the editable `user_templates` collection — it does not change what the main app sees. |
| You **deploy** a user template (🚀 Rocket button) | **Nothing.** The playground compiles the template and writes a new versioned document to `dsl_template_artifacts`. The next time your main app reads from the database, it gets the latest deployed version. |
| The Import transformation logic changes    | Update `data_transformer.py` in this folder to match the new logic. This is rare. |
| The template execution logic changes       | Update `model_runner.py` in this folder to match. This is very rare. |

**The most common thing you'll do:** re-copy `dsl_functions.py` when you add
new functions in the playground. Everything else is automatic (after deploy)
or very rare.

---

## Quick Reference: MongoDB Collections

| Collection                  | Who Writes To It | Who Reads From It | What's In It                    |
|-----------------------------|-------------------|--------------------|---------------------------------|
| `dsl_template_artifacts`    | The playground    | Your main app      | Generated Python code for each saved template. One document per template version. |

That's the only collection the main app needs to read from. Nothing else.
