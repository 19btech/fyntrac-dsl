"""End-to-end test of the Excel workbook import pipeline:

  build sample workbook (tools/create_sample_workbook.py)
    -> upload (workbook.save_workbook_bytes, same call the endpoint makes)
    -> list_workbooks / get_workbook_overview / set_workbook_sheet_roles
    -> get_sheet_formulas   (R1C1 dedup, friendly rendering, row-recursion)
    -> trace_workbook_dependencies
    -> import_workbook_inputs (event def + event data from the Loans sheet)
    -> create_saved_rule translating the Calc formulas to DSL steps
    -> reconcile_workbook_outputs (dry-run vs the Outputs sheet numbers)

Uses a small regex-aware fake of the async Mongo API; no Mongo, no LLM.

Run:  python tests/test_excel_workbook_tools.py
"""

import asyncio
import importlib.util
import os
import re
import sys

# Production layout: repo root on sys.path, everything under the backend.*
# namespace (dry-run's generated code does `from backend.dsl_functions import`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _load_sample_builder():
    """Load tools/create_sample_workbook.py by path — putting the repo root on
    sys.path would shadow the backend package layout the server expects."""
    path = os.path.join(os.path.dirname(__file__), "..", "tools",
                        "create_sample_workbook.py")
    spec = importlib.util.spec_from_file_location("create_sample_workbook", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Minimal async-Mongo fake (regex-aware, unlike the maker-checker one) ──

class _R:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    async def to_list(self, n=None):
        return list(self._docs)

    def __aiter__(self):
        self._i = 0
        return self

    async def __anext__(self):
        if self._i >= len(self._docs):
            raise StopAsyncIteration
        d = self._docs[self._i]
        self._i += 1
        return d


class FakeCollection:
    def __init__(self):
        self.docs = []

    @staticmethod
    def _match(d, flt):
        for k, v in (flt or {}).items():
            if isinstance(v, dict):
                if "$regex" in v:
                    flags = re.I if "i" in (v.get("$options") or "") else 0
                    if not re.search(v["$regex"], str(d.get(k, "")), flags):
                        return False
                elif "$in" in v:
                    if d.get(k) not in v["$in"]:
                        return False
                elif "$exists" in v:
                    if (k in d) != bool(v["$exists"]):
                        return False
                elif k not in d:
                    return False
                continue
            if d.get(k) != v:
                return False
        return True

    async def find_one(self, flt=None, projection=None, **kw):
        for d in self.docs:
            if self._match(d, flt):
                return dict(d)
        return None

    def find(self, flt=None, projection=None, **kw):
        return FakeCursor([dict(d) for d in self.docs if self._match(d, flt)])

    def aggregate(self, pipeline):
        return FakeCursor([])

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return _R(inserted_id=1)

    async def replace_one(self, flt, doc, upsert=False):
        for i, d in enumerate(self.docs):
            if self._match(d, flt):
                self.docs[i] = dict(doc)
                return _R(modified_count=1)
        if upsert:
            self.docs.append(dict(doc))
        return _R(modified_count=0)

    async def update_one(self, flt, update, upsert=False):
        setv = update.get("$set", {})
        for d in self.docs:
            if self._match(d, flt):
                for k, v in setv.items():
                    d[k] = v
                return _R(modified_count=1)
        if upsert:
            nd = dict(flt)
            nd.update(setv)
            self.docs.append(nd)
        return _R(modified_count=0)

    async def delete_one(self, flt):
        for i, d in enumerate(self.docs):
            if self._match(d, flt):
                del self.docs[i]
                return _R(deleted_count=1)
        return _R(deleted_count=0)

    async def delete_many(self, flt):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not self._match(d, flt)]
        return _R(deleted_count=before - len(self.docs))

    async def count_documents(self, flt=None):
        return len([d for d in self.docs if self._match(d, flt)])


class FakeDB:
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        c = self.__dict__.setdefault("_c", {})
        if name not in c:
            c[name] = FakeCollection()
        return c[name]


async def main():
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test_excel"
    os.environ.pop("REQUIRE_AGENT_APPROVAL", None)

    import backend.server  # noqa: F401 — registers the agent bridge + helpers
    from backend.agent import tools, workbook
    build_sample_workbook = _load_sample_builder().build_sample_workbook

    failures = []

    def check(cond, msg):
        print(f"  {'ok  ' if cond else 'FAIL'} — {msg}")
        if not cond:
            failures.append(msg)

    fake = FakeDB()
    tools._ServerBridge.db = fake

    # ── Registry / schema parity for the new tools ────────────────────────
    new_tools = {
        "list_workbooks", "get_workbook_overview", "set_workbook_sheet_roles",
        "get_sheet_data", "get_sheet_formulas", "trace_workbook_dependencies",
        "import_workbook_inputs", "reconcile_workbook_outputs",
    }
    schema_names = {t["name"] for t in tools.TOOL_SCHEMAS}
    check(new_tools <= set(tools.TOOLS), "workbook tools registered in TOOLS")
    check(new_tools <= schema_names, "workbook tools have schemas")
    check(schema_names == set(tools.TOOLS), "schema/registry parity")
    secs = tools._syntax_guide_sections()
    check("excel_translation_guide" in secs and "EXCEL" in secs["excel_translation_guide"].upper(),
          "excel_translation_guide served as a syntax-guide section")

    # ── R1C1 normalisation unit checks ────────────────────────────────────
    check(workbook.normalize_r1c1("=B2*C2", 2, 4) == "=RC[-2]*RC[-1]",
          "R1C1: same-row relative refs")
    check(workbook.normalize_r1c1("=E2", 3, 2) == "=R[-1]C[3]",
          "R1C1: previous-row ref")
    check(workbook.normalize_r1c1("=Loans!$C$2/12", 5, 3) == "=Loans!R2C3/12",
          "R1C1: absolute cross-sheet ref")
    check(workbook.normalize_r1c1('=IF(A2>1, "B2", LOG10(5))', 2, 3)
          == '=IF(RC[-2]>1, "B2", LOG10(5))',
          "R1C1: strings and function names that look like refs are untouched")
    # Dragged-down copies normalise identically (the dedup property):
    check(workbook.normalize_r1c1("=B2*$D$1", 2, 3)
          == workbook.normalize_r1c1("=B9*$D$1", 9, 3),
          "R1C1: dragged copies produce the same pattern")

    # ── Build + upload the sample workbook ────────────────────────────────
    sample_path = os.path.join(os.path.dirname(__file__), "_sample_loan_model.xlsx")
    info = build_sample_workbook(sample_path)
    with open(sample_path, "rb") as fh:
        meta = workbook.save_workbook_bytes("loan_model.xlsx", fh.read())
    wid = meta["workbook_id"]
    check(set(meta["sheets"]) == {"Loans", "Assumptions", "Calc",
                                  "AmortSchedule", "PMT_Template", "Outputs"},
          "upload registers all 6 sheets")

    # Re-uploading identical bytes must reuse the existing entry, not fork it.
    with open(sample_path, "rb") as fh:
        dup = workbook.save_workbook_bytes("loan_model_copy.xlsx", fh.read())
    check(dup["workbook_id"] == wid and dup.get("duplicate_of_existing"),
          "duplicate upload returns the existing workbook_id")

    try:
        r = await tools.tool_list_workbooks({})
        check(any(w["workbook_id"] == wid for w in r["workbooks"]),
              "list_workbooks sees the upload")

        # ── Overview + roles ──────────────────────────────────────────────
        ov = await tools.tool_get_workbook_overview({"workbook_id": wid})
        by_name = {s["name"]: s for s in ov["sheets"]}
        check(by_name["Loans"]["suggested_role"] == "input",
              "overview suggests Loans as input (no formulas)")
        check(by_name["Calc"]["suggested_role"] == "calc",
              "overview suggests Calc as calc (referenced by AmortSchedule)")
        check(by_name["Calc"]["has_cached_formula_values"] is False,
              "overview flags missing cached values (file not saved by Excel)")
        check("next_action" in ov, "overview demands role confirmation")
        headers = [h["field_name"] for h in by_name["Loans"]["headers"]]
        check(headers == ["loan_id", "principal", "annual_rate",
                          "term_months", "origination_date"],
              "Loans headers sanitised to field names")

        # Importing before the interview must be refused (weak-model guard).
        try:
            await tools.tool_import_workbook_inputs({
                "workbook_id": wid, "sheet": "Loans", "event_name": "TooSoon",
            })
            check(False, "import before role confirmation should be refused")
        except tools.ToolError as exc:
            check("NOT confirmed" in str(exc),
                  "import refused until roles are confirmed")

        await tools.tool_set_workbook_sheet_roles({
            "workbook_id": wid,
            "roles": {"Loans": "input", "Assumptions": "reference",
                      "Calc": "calc", "AmortSchedule": "calc",
                      "PMT_Template": "input", "Outputs": "output"},
        })
        ov2 = await tools.tool_get_workbook_overview({"workbook_id": wid})
        check(ov2.get("roles_confirmed") is True, "roles recorded and confirmed")

        # A sheet declared calc can never be imported as event data.
        try:
            await tools.tool_import_workbook_inputs({
                "workbook_id": wid, "sheet": "Calc", "event_name": "CalcAsInput",
            })
            check(False, "importing a calc sheet should be refused")
        except tools.ToolError as exc:
            check("declared as 'calc'" in str(exc),
                  "import refuses calc-role sheets with a role-specific error")

        # An empty headers-only template gets a diagnostic, not a bare error.
        try:
            await tools.tool_import_workbook_inputs({
                "workbook_id": wid, "sheet": "PMT_Template",
                "event_name": "PMT_Template",
            })
            check(False, "importing an empty template should be refused")
        except tools.ToolError as exc:
            check("EMPTY TEMPLATE" in str(exc) and "Do NOT retry" in str(exc),
                  "empty template import returns an actionable diagnostic")

        # ── Formula patterns: Calc ────────────────────────────────────────
        pats = await tools.tool_get_sheet_formulas({"workbook_id": wid, "sheet": "Calc"})
        check(pats["unique_patterns"] == 7 and pats["total_formula_cells"] == 35,
              f"Calc: 35 formula cells dedupe to 7 patterns "
              f"(got {pats['unique_patterns']}/{pats['total_formula_cells']})")
        by_header = {p["target_field_name"]: p for p in pats["patterns"]}
        check(by_header["interest_m1"]["friendly"]
              == "=ROUND(Loans.principal*monthly_rate, 2)",
              "friendly rendering uses header names "
              f"(got {by_header['interest_m1']['friendly']!r})")
        check("Assumptions!B1(=0.0025)" in by_header["servicing_fee"]["friendly"],
              "absolute assumption ref rendered with its cached value")
        check(all(p["count"] == 5 for p in pats["patterns"]),
              "each Calc pattern covers all 5 loan rows")
        check(not any(p["row_recursive"] for p in pats["patterns"]),
              "no false row-recursion on Calc")

        # ── Formula patterns: AmortSchedule (row recursion) ───────────────
        am = await tools.tool_get_sheet_formulas({"workbook_id": wid,
                                                  "sheet": "AmortSchedule"})
        rec = [p for p in am["patterns"] if p["row_recursive"]]
        check(len(rec) == 1 and rec[0]["target_field_name"] == "opening_balance",
              "AmortSchedule: opening_balance detected as row-recursive")
        check(rec[0]["friendly"] == "=prev(closing_balance)",
              f"row-recursive ref rendered as prev() "
              f"(got {rec[0]['friendly']!r})")

        # ── Dependency graph ──────────────────────────────────────────────
        deps = await tools.tool_trace_workbook_dependencies({"workbook_id": wid})
        edges = {(e["from"], e["to"]) for e in deps["edges"]}
        check(("Loans!principal", "Calc!interest_m1") in edges,
              "dependency edge Loans!principal -> Calc!interest_m1")
        check(("Calc!closing_balance", "Calc!interest_m1") not in edges,
              "no fabricated edges")
        check("Loans -> Calc" in deps["sheet_flow"],
              "sheet-level flow includes Loans -> Calc")

        # ── Import inputs as an event ─────────────────────────────────────
        imp = await tools.tool_import_workbook_inputs({
            "workbook_id": wid, "sheet": "Loans", "event_name": "LoanBook",
            "column_map": {"loan_id": "instrumentid"},
            "default_posting_date": "2026-03-31",
            "default_effective_date": "2026-03-31",
        })
        check(imp["rows_imported"] == 5 and imp["event_created"],
              "Loans sheet imported: event created + 5 rows loaded")
        check(imp["fields"].get("instrumentid") == "string"
              and imp["fields"].get("annual_rate") == "decimal"
              and imp["fields"].get("origination_date") == "date",
              f"field types inferred sensibly (got {imp['fields']})")
        ev = await tools.tool_get_event_data({"event_name": "LoanBook"})
        check(ev["row_count"] == 5 and ev["rows"][0]["instrumentid"] == "101",
              "imported event data readable, instrumentid stringified")

        # ── Build the translated rule ─────────────────────────────────────
        await tools.tool_add_transaction_types({"transaction_types": [
            "InterestAccrual", "InterestReceivable",
            "PrincipalRepayment", "LoanPrincipal",
            "ServicingFee", "ServicingFeeIncome",
        ]})
        rule = await tools.tool_create_saved_rule({
            "name": "LoanModelFromWorkbook",
            "force_unplanned": True,
            "steps": [
                {"name": "principal", "stepType": "calc",
                 "source": "event_field", "eventField": "LoanBook.principal"},
                {"name": "annual_rate", "stepType": "calc",
                 "source": "event_field", "eventField": "LoanBook.annual_rate"},
                {"name": "term_months", "stepType": "calc",
                 "source": "event_field", "eventField": "LoanBook.term_months"},
                {"name": "monthly_rate", "stepType": "calc",
                 "source": "formula", "formula": "divide(annual_rate, 12)"},
                {"name": "monthly_payment", "stepType": "calc",
                 "source": "formula",
                 "formula": ("round(divide(multiply(principal, monthly_rate), "
                             "subtract(1, power(add(1, monthly_rate), "
                             "multiply(-1, term_months)))), 2)")},
                {"name": "interest_m1", "stepType": "calc",
                 "source": "formula",
                 "formula": "round(multiply(principal, monthly_rate), 2)"},
                {"name": "principal_m1", "stepType": "calc",
                 "source": "formula",
                 "formula": "subtract(monthly_payment, interest_m1)"},
                {"name": "servicing_fee", "stepType": "calc",
                 "source": "formula",
                 "formula": "round(multiply(principal, 0.0025), 2)"},
            ],
            "outputs": {
                "createTransaction": True,
                "transactions": [
                    {"type": "InterestAccrual", "amount": "interest_m1", "side": "debit"},
                    {"type": "InterestReceivable", "amount": "interest_m1", "side": "credit"},
                    {"type": "PrincipalRepayment", "amount": "principal_m1", "side": "debit"},
                    {"type": "LoanPrincipal", "amount": "principal_m1", "side": "credit"},
                    {"type": "ServicingFee", "amount": "servicing_fee", "side": "debit"},
                    {"type": "ServicingFeeIncome", "amount": "servicing_fee", "side": "credit"},
                ],
            },
        })
        rule_id = rule.get("rule_id") or rule.get("id")
        check(bool(rule_id), f"rule created from workbook formulas (id={rule_id})")

        # ── Reconcile: rule output vs the workbook's Outputs sheet ────────
        rec = await tools.tool_reconcile_workbook_outputs({
            "workbook_id": wid, "sheet": "Outputs", "key_column": "loan_id",
            "rule_id": rule_id,
            "column_to_transaction_type": {
                "interest_accrual": "InterestAccrual",
                "principal_repayment": "PrincipalRepayment",
                "servicing_fee": "ServicingFee",
            },
        })
        check(rec["status"] == "reconciled",
              f"reconciliation status=reconciled (got {rec['status']}: "
              f"mismatches={rec.get('mismatches')!r:.300} "
              f"missing={rec.get('missing_actual')!r:.300})")
        check(rec["compared"] == 15 and rec["matched"] == 15,
              f"all 15 expected values matched (got {rec['matched']}/{rec['compared']})")

        # Cross-check against the generator's own expected numbers:
        exp0 = info["expected_outputs"][0]
        check(abs(exp0["interest_accrual"] - 500.0) < 1e-9,
              "sanity: sample math is what we think it is (loan 101 interest=500)")

        # ── Negative path: wrong mapping must be caught ───────────────────
        bad = await tools.tool_reconcile_workbook_outputs({
            "workbook_id": wid, "sheet": "Outputs", "key_column": "loan_id",
            "rule_id": rule_id,
            "column_to_transaction_type": {
                "principal_repayment": "ServicingFee",   # deliberately wrong
            },
        })
        check(bad["status"] == "mismatch" and bad["mismatches"],
              "reconcile flags a deliberately wrong mapping as mismatch")

    finally:
        try:
            workbook.delete_workbook(wid)
        except Exception:
            pass
        try:
            os.unlink(sample_path)
        except Exception:
            pass

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("All Excel workbook pipeline checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
