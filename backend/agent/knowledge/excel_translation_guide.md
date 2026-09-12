# Excel Workbook → DSL Translation Guide

How to convert an uploaded Excel model workbook into events, rules, and steps,
and prove the translation is correct. This guide is served via
`get_dsl_syntax_guide` with `section='excel_translation_guide'`.

---------------------------------------------------------------------------
WORKFLOW (follow in order)
---------------------------------------------------------------------------

1. **Discover** — `list_workbooks`, then `get_workbook_overview(workbook_id)`.
2. **Interview** — present each sheet with its `suggested_role` and headers to
   the user and ask them to confirm which sheets are **inputs** (raw data),
   **calc** (formulas to translate), **outputs** (expected results), and
   **reference** (lookup tables). Ask for the row grain of the calc sheet
   (one row per instrument? per period?). Record answers with
   `set_workbook_sheet_roles`. Never guess roles silently.
3. **Analyse** — `get_sheet_formulas` on every calc/output sheet and
   `trace_workbook_dependencies` for the flow. Work from the `friendly`
   renderings, which use column-header names.
4. **Import inputs** — `import_workbook_inputs` per input sheet. The tool
   REFUSES to run until roles are confirmed, and only sheets declared
   `input`/`reference` can be imported — never loop it over every sheet.
   The key column must map to `instrumentid` (use `column_map`). If the
   sheet has no posting/effective date columns, ask the user which date to
   stamp and pass `default_posting_date` / `default_effective_date`. This
   loads the user's REAL data — never call `generate_sample_event_data` for
   these events. If a sheet turns out to be an empty template (headers, no
   rows), STOP and ask the user for data instead of retrying or moving to
   the next empty sheet.
5. **Plan** — `submit_plan` describing: events created, one step per unique
   formula pattern (in dependency order), transaction outputs, and which
   output-sheet columns you will reconcile against.
6. **Build** — `create_saved_rule` with calc/schedule steps translated from
   the formula patterns. Register transaction types, then wire
   `outputs.transactions[]` — one transaction per computed output result
   (see the fixed transaction shape below). NO debit/credit pairs.
7. **Reconcile (mandatory)** — `reconcile_workbook_outputs` comparing the
   rule's dry-run against the output sheet, keyed by instrument. Iterate on
   mismatches pattern-by-pattern until `status='reconciled'`, then report the
   reconciliation summary (n of m matched, tolerance) to the user. Do NOT
   `finish` with unexplained mismatches.

---------------------------------------------------------------------------
REUSE THE EXISTING WORKSPACE FIRST
---------------------------------------------------------------------------

Before creating ANYTHING from a workbook, read the WORKSPACE SNAPSHOT:

* If an existing event already covers the sheet's columns (same fields, or a
  superset), REUSE it — import into that event or use its loaded data
  instead of defining a near-duplicate (`SBO_PMT_INPUT` next to `PMT` is
  exactly the smell to avoid). Only create a new event when nothing
  suitable exists, and say in one sentence why the existing ones didn't fit.
* Same for transaction types: reuse registered names that match the
  workbook's output names before inventing new ones.
* If matching event data is already loaded, ask the user whether to use it
  or replace it with the workbook's rows — never assume.

---------------------------------------------------------------------------
CALC-ONLY WORKBOOKS (no input or output sheets)
---------------------------------------------------------------------------

Users often upload just a calculation sheet — no input tab, no output tab.
Do not stall asking for sheets that don't exist. Instead:

1. Run `trace_workbook_dependencies`. The 'value' nodes with NO incoming
   edges plus the absolute parameter cells (`Sheet!B1(=0.05)`) ARE the
   input schema. Propose it to the user as an event definition (field
   names + inferred types) and confirm the row grain / key column.
2. For input DATA: if the calc sheet itself contains literal input values,
   read them with `get_sheet_data`; otherwise ask the user for data or
   offer `generate_sample_event_data` (allowed here — the "never sample
   data" rule applies only to events whose real rows WERE imported from a
   sheet).
3. THE OUTPUT IS ALWAYS TRANSACTIONS. There is no other output form in
   this platform. Identify the terminal computed columns of the dependency
   graph (nothing downstream consumes them) — those are the transaction
   amounts. Name the transaction types from the column headers, confirm
   with the user, and register them.
4. With no output sheet there is nothing to reconcile against — instead
   dry-run, show per-instrument results, and ask the user to eyeball a few
   rows against Excel before you `finish`.

---------------------------------------------------------------------------
FIXED INPUT & OUTPUT SHAPE — DO NOT DEVIATE
---------------------------------------------------------------------------

The platform has ONE input shape and ONE output shape. A workbook does not get
to change them — you translate the workbook INTO them.

**Inputs become EVENTS, in one of two table shapes:**

* **Standard table** (the raw activity data — loans, contracts, positions):
  MUST have the four default columns `instrumentid`, `subinstrumentid`,
  `postingdate`, `effectivedate`, plus the workbook's business fields. When
  importing, map the sheet's key column to `instrumentid` (via `column_map`)
  and stamp the posting/effective dates (ask the user which date if the sheet
  has none). `subinstrumentid` defaults to `'1'`.
* **Reference table** (static lookups — rate tables, product catalogs,
  assumption sheets): FREEFORM — any columns, NONE of the four standard
  columns required. Import with `eventType='reference'`, `eventTable='custom'`
  and read via `lookup(...)`.

**Outputs are ALWAYS transactions**, each with EXACTLY these six fields:
`instrumentid`, `subinstrumentid`, `postingdate`, `effectivedate`,
`transactiontype`, `amount`. Nothing else is a valid output. If the workbook's
"output" is a report layout or a pivot, you still emit one transaction per
terminal computed value — never reproduce the report format.

---------------------------------------------------------------------------
TRANSACTIONS ARE JUST AMOUNTS — NO DEBITS, NO CREDITS
---------------------------------------------------------------------------

This platform is NOT a general ledger. A transaction is a single signed
amount posted for an instrument — there is NO debit/credit side, NO
balancing requirement, and NO contra/clearing account.

* One output column (or one computed result) = ONE transaction. If the
  workbook's output sheet lists Payment_Interest = -155.56, emit exactly
  one transaction of type Payment_Interest with amount -155.56. Do NOT
  invent a second "contra" transaction to balance it.
* Negative amounts are fine — carry the sign the workbook uses.
* NEVER create a synthetic control/clearing account (e.g. "SBO_Control").
  If you see one in an existing rule, it is legacy — do not replicate it.

---------------------------------------------------------------------------
READING FORMULA PATTERNS
---------------------------------------------------------------------------

`get_sheet_formulas` deduplicates dragged-down formulas into patterns:

* `friendly` — the formula with header names substituted:
  `ROUND(principal*annual_rate/12, 2)` → step formula
  `round(multiply(principal, divide(annual_rate, 12)), 2)`.
* `count` / `rows` — how many cells share the pattern (one pattern ≈ one
  step variable, regardless of row count).
* `row_recursive: true` — the column reads its own previous row
  (`prev(closing_balance)`): model the WHOLE sheet region as a **schedule
  step**; the previous-row reference becomes `lag('closing_balance', 1,
  opening_value)`.
* `Sheet!B1(=0.05)` — absolute reference to a fixed parameter cell. Either
  inline the constant into the formula or (better, if the user may change
  it) import the assumptions sheet as a reference event and `lookup` it.
* `cross_sheet_refs` — same-row references to another sheet are joins on the
  row key; if both sheets were imported against the same `instrumentid`,
  the fields are directly available (or via `collect`/`lookup` for
  reference tables).

---------------------------------------------------------------------------
EXCEL → DSL FUNCTION MAP
---------------------------------------------------------------------------

| Excel | DSL |
|---|---|
| `+ - * /` | `add / subtract / multiply / divide` (or infix where allowed) |
| `IF(c,a,b)` | `if(c, a, b)` — or a condition step for branch-per-row logic |
| `IFS/nested IF` | `switch(...)` or nested `if()` |
| `AND/OR/NOT` | `and(...)/or(...)/not(...)` |
| `ROUND/ROUNDUP/ROUNDDOWN` | `round(x, n)` / `ceil` / `floor` |
| `ABS/SIGN/POWER/TRUNC` | `abs_val / sign / power / truncate` |
| `MAX/MIN` (scalars) | `max_val / min_val` |
| `SUM(range)` of a schedule column | schedule `outputVars` with `type:'sum'`, or `schedule_sum` |
| `SUM/AVERAGE/COUNT/MEDIAN/STDEV` over collected rows | `sum_vals / avg / count / median / std_dev` on a `collect` variable |
| `SUMPRODUCT(a,b)` | `weighted_avg` or `for_each` + `sum_vals` |
| `PMT/PV/FV/RATE/NPER` | `pmt / pv / fv / rate / nper` |
| `NPV/IRR/XNPV/XIRR` | `npv / irr / xnpv / xirr` |
| `EDATE(d,n)` | `add_months(d, n)` |
| `EOMONTH(d,0)` | `end_of_month(d)` |
| `YEARFRAC(a,b,basis)` | `day_count_fraction(a, b, convention)` |
| `DATEDIF` / date subtraction | `days_between / months_between / years_between` |
| `TODAY()` | ASK THE USER — pin to `postingdate` or a fixed date (volatile functions must be pinned for reproducibility) |
| `VLOOKUP/XLOOKUP/INDEX+MATCH` (exact match) | reference event + `lookup(...)` |
| `CONCAT/&`, `UPPER/LOWER/TRIM/LEN` | `concat`, `upper / lower / trim / str_length` |
| `ISBLANK/IFERROR` | `is_null / coalesce` |
| Prev-row self reference | `lag('col', 1, initial_value)` inside a schedule step |

---------------------------------------------------------------------------
NOT TRANSLATABLE — FLAG, NEVER SILENTLY APPROXIMATE
---------------------------------------------------------------------------

* `INDIRECT`, `OFFSET` (dynamic ranges), array/CSE formulas, `RAND/RANDBETWEEN`
* VBA macros, pivot tables, external-workbook links, circular/iterative calc
* Approximate-match `VLOOKUP` (`range_lookup=TRUE`) — confirm the intended
  banding logic with the user, then model it with `switch`/`between`.

When you meet one of these: tell the user exactly which cells/patterns are
affected, propose the closest DSL modelling, and get their sign-off in the
plan. List every such assumption in the rule's `commentText`.

---------------------------------------------------------------------------
RECONCILIATION POLICY
---------------------------------------------------------------------------

* Default tolerance 0.01 (a cent). Excel float/date arithmetic can differ in
  the last decimals — a systematic mismatch is NOT rounding:
  * constant offset → missing term in the formula
  * ×12 / ÷12 factor → annual↔monthly conversion error
  * missing instruments → a condition gate or a wrong join key
* `status='no_cached_values'` means the workbook was never recalculated by
  Excel — ask the user to open and save it in Excel, or to point you at a
  values-only outputs sheet.
* Report the final summary like: "247 of 250 values reconcile within $0.01;
  3 differ because <reason> — accepted by user / fixed by <change>."
