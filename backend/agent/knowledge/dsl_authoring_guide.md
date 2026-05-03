# DSL Authoring Guide (agent reference)

This is the canonical reference the agent retrieves via
`get_dsl_syntax_guide` and the canonical-pattern tools. Update this file when
DSL semantics change.

## 1. Step shapes — binding contracts

Every step has `name`, `stepType`, and an immutable `id` (UUID, set
automatically). Use `step_id` to address a step in `update_step` /
`delete_step` / `patch_step` — `step_name` is mutable.

### calc

```jsonc
{ "name": "x", "stepType": "calc",
  "source": "formula" | "value" | "event_field" | "collect",
  "formula": "...",                  // when source=formula
  "value": "literal",                // when source=value
  "eventField": "EVENT.fieldname",   // when source=event_field OR collect
  "collectType": "collect_by_instrument" | "collect_all" | "collect_by_subinstrument"
}
```

### condition

```jsonc
{ "name": "y", "stepType": "condition",
  "conditions": [
     { "condition": "eq(a,b)", "thenFormula": "1" }
  ],
  "elseFormula": "0"
}
```

### iteration

```jsonc
{ "name": "z", "stepType": "iteration",
  "iterations": [{
     "type": "apply_each" | "apply_each_paired" | "for_each",
     "sourceArray": "<variable_name>",   // never a literal [...]
     "secondArray": "<variable_name>",   // for apply_each_paired
     "varName": "each",
     "secondVar": "second",
     "expression": "<single-line DSL expression>",
     "resultVar": "<output_var_name>"
  }]
}
```

### schedule

```jsonc
{ "name": "Schedule", "stepType": "schedule",
  "scheduleConfig": {
     "periodType": "date" | "number",
     "frequency": "D"|"W"|"M"|"Q"|"Y",
     "convention": ""|"30/360"|"Actual/360"|"Actual/365"|"Actual/Actual"|"30E/360",
     // when periodType=date:
     "startDateSource":"value"|"field"|"formula", "startDate"|"startDateField"|"startDateFormula": "...",
     "endDateSource":"value"|"field"|"formula",   "endDate"|"endDateField"|"endDateFormula": "...",
     // when periodType=number:
     "periodCountSource":"value"|"field"|"formula", "periodCount"|"periodCountField"|"periodCountFormula": "...",
     "columns": [{"name": "...", "formula": "..."}, ...]
     // contextVars is auto-derived from formulas — never set it manually
  },
  "outputVars": [
     {"name": "X", "type": "first"|"last"|"sum"|"column",  "column": "<existing column name>"},
     {"name": "Y", "type": "filter", "column": "...", "matchCol": "...", "matchValue": "..."}
  ]
}
```

#### ⚠️ MANDATORY: Start date and end date MUST always be populated

For every `periodType:"date"` schedule you **must** supply both
`startDateSource` + its value AND `endDateSource` + its value. Leaving either
blank produces a schedule with zero rows — the model silently outputs nothing.

**Decision tree** (apply in priority order):

1. Does the event have a start/end date field?
   → `startDateSource:"field"`, `startDateField:"EVENTNAME.fieldname"`
2. Is there a calc step that computes the date (e.g. reads from the event)?
   → `startDateSource:"formula"`, `startDateFormula:"calcStepVarName"`
3. No relevant date field exists? Use the hard fallback:
   → `startDateSource:"formula"`, `startDateFormula:"postingdate"`
   → `endDateSource:"formula"`,   `endDateFormula:"add_years(postingdate, 1)"`

The validator auto-applies the fallback (3) and records it in
`scheduleConfig._autohealed`. If you see that key after saving, review it and
upgrade to (1) or (2) if a better date field is available.

#### Schedule built-in identifiers (always available inside column formulas)

`period_date, period_index, period_start, period_number, dcf, lag,
days_in_current_period, total_periods, daily_basis, item_name,
subinstrument_id, s_no, index, start_date, end_date`

#### Schedule output-var semantics

| type     | generated DSL line                                            | use when                                  |
|----------|---------------------------------------------------------------|-------------------------------------------|
| `first`  | `X = schedule_first(Schedule, "col")`                         | take row 0 of `col`                       |
| `last`   | `X = schedule_last(Schedule, "col")`                          | take final row of `col`                   |
| `sum`    | `X = schedule_sum(Schedule, "col")`                           | sum every row of `col`                    |
| `column` | `X = schedule_column(Schedule, "col")` (returns array)        | feed an iteration / further calc          |
| `filter` | `X = schedule_filter(Schedule, "matchCol", matchValue, "col")`| pick the row where matchCol == matchValue |

`column` MUST be the name of an existing schedule column (validated at save).

#### ⚠️ CRITICAL: outputVar names ARE the variable — never create an alias step

When you define `outputVars: [{"name": "current_amortization", ...}]` on a
schedule step, the identifier `current_amortization` is directly in scope for
all subsequent steps. **There is no auto-generated suffix** like
`scheduleName_current` or `scheduleName_last` — the `name` you write IS the
variable name.

**The alias-step antipattern** — NEVER do this:

```jsonc
// WRONG ✗ — redundant alias step
{ "name": "current_amortization", "stepType": "calc", "source": "formula",
  "formula": "amortization_schedule_current" }  // ← this variable doesn't exist
```

Instead, set the outputVar name exactly as you want the variable to be called:

```jsonc
// CORRECT ✓ — outputVar name is the variable; use it directly
{
  "name": "amortization_schedule",
  "stepType": "schedule",
  "outputVars": [
    { "name": "current_amortization", "type": "filter", "column": "amortization",
      "matchCol": "period_date", "matchValue": "postingdate" }
  ]
}
// downstream steps and transactions just write:  "current_amortization"
```

**Validator behaviour**: any calc step whose formula is exactly a schedule
outputVar name (i.e. a single identifier already in scope from a schedule) is
flagged as `alias_step_antipattern` and must be deleted.



| Step | Rule |
|------|------|
| `instrumentid` | **NEVER create this step.** It is always an implicit global on every row. Hard-blocked by the validator. |
| `subinstrumentid` | **ALWAYS create this step.** Choose `source` based on data shape — see below. |

### Choosing the source for `subinstrumentid`

**Scalar** — each instrument has exactly one `subinstrumentid` per posting date:
```jsonc
{"name": "subinstrumentid", "stepType": "calc",
 "source": "event_field", "eventField": "EVENTNAME.subinstrumentid"}
```

**Non-scalar** — an instrument has multiple `subinstrumentid` values on the same posting date:
```jsonc
{"name": "subinstrumentid", "stepType": "calc",
 "source": "collect", "collectType": "collect_by_instrument",
 "eventField": "EVENTNAME.subinstrumentid"}
```

If you are unsure: check the loaded event data. If any instrument row has more than one distinct `subinstrumentid`, use non-scalar. Otherwise default to scalar.

## 2. Transactions live in `outputs.transactions[]`

Every entry needs: `type`, `amount`, `side` (debit|credit). Optionally
`postingdate`, `effectivedate`, `subinstrumentid` (auto-inferred when omitted).
Always emit balanced PAIRS (one debit + one credit transaction type per economic event).
This app produces TRANSACTIONS only — journal entries are created by the downstream
accounting system that consumes these transactions. Do not describe outputs as
'journal entries'.

NEVER:
- Put `createTransaction(...)` inside a calc formula — rejected by validator.
- Create a calc step named `outputs_transactions` / `transactions` / `txn` —
  these are reserved.

## 3. Workflow (state machine view)

```
PLAN     → submit_plan(pattern_id, rules:[…])    # records intent
DRAFT    → create_saved_rule / add_step_to_rule  # build the steps
VERIFY   → debug_step / test_schedule_step / verify_rule_complete
COMMIT   → outputs.transactions[] populated
TEST     → dry_run_template
FINISH   → finish
```

The agent SHOULD call `submit_plan` first on any non-trivial request so the
runtime can pre-fetch the canonical pattern.

## 4. Common anti-patterns (rejected by validator)

| anti-pattern                                        | fix                                            |
|----------------------------------------------------|------------------------------------------------|
| iterate over `all_instruments`                      | engine already runs once per instrument-row    |
| `arr[i]` indexing in formulas                       | use `lag('col',1,default)` inside a schedule   |
| `outputs.events.push(...)`                          | use `outputs.transactions[]` array             |
| `customCode:` block / raw Python                    | compose with calc / condition / iteration / schedule |
| schedule with NO columns                            | every schedule needs ≥ 1 column                |
| outputVar.column referencing nonexistent column     | use exactly one of the defined column names    |
| `_v2`, `_final`, `_fixed`, `_auto` rule-name suffixes | edit the existing rule via `update_saved_rule` |

## 5. Update / patch semantics

- `update_step(rule_id, step_id, patch)` performs a **DEEP MERGE** of `patch`
  into the step — nested fields like `scheduleConfig.columns` are merged
  intelligently: pass `{scheduleConfig: {columns: [...]}}` to replace the
  whole columns array, OR call `patch_step` for surgical edits.
- `patch_step(rule_id, step_id, ops)` accepts a JSON-Pointer (RFC 6902) op
  list. Example to fix one column formula:

  ```jsonc
  patch_step(rule_id, step_id, ops=[
     {"op":"replace", "path":"/scheduleConfig/columns/2/formula", "value":"..."}
  ])
  ```

- `replace_schedule_column(rule_id, step_id, column_name, new_formula)` is a
  convenience wrapper around `patch_step`.
- After every write, the tools re-fetch the rule from the DB and verify the
  requested values landed. A mismatch returns `{ok:false, mismatch:[…]}` so
  silent failures are impossible.
