"""Build a sample loan-model .xlsx that exercises the agent's workbook-import
pipeline end to end:

  Loans          input sheet — 5 loans of raw values
  Assumptions    fixed parameter cell (absolute reference target)
  Calc           one row per loan; dragged-down formulas with IF, ROUND,
                 power, cross-sheet refs and an absolute assumptions ref
  AmortSchedule  row-recursive region (opening = previous row's closing)
  Outputs        values-only expected results (as if paste-values), because
                 openpyxl cannot compute cached formula values — this mirrors
                 a workbook whose outputs tab was pasted as values

Run:  python tools/create_sample_workbook.py [out.xlsx]
The builder returns the expected output numbers so tests can reconcile
against exactly what the sheet contains.
"""

from __future__ import annotations

import sys
from pathlib import Path

LOANS = [
    # loan_id, principal, annual_rate, term_months, origination_date
    ("101", 100_000.00, 0.060, 360, "2026-01-01"),
    ("102", 250_000.00, 0.045, 240, "2026-01-15"),
    ("103", 50_000.00, 0.072, 60, "2026-02-01"),
    ("104", 750_000.00, 0.055, 300, "2026-02-10"),
    ("105", 12_000.00, 0.090, 24, "2026-03-01"),
]

SERVICING_FEE_RATE = 0.0025


def expected_outputs() -> list[dict]:
    """The numbers the Outputs sheet holds — computed with the same math the
    Calc sheet's formulas express."""
    out = []
    for loan_id, principal, rate, term, _orig in LOANS:
        mr = rate / 12
        payment = round(principal * mr / (1 - (1 + mr) ** -term), 2)
        interest = round(principal * mr, 2)
        out.append({
            "loan_id": loan_id,
            "interest_accrual": interest,
            "principal_repayment": round(payment - interest, 2),
            "servicing_fee": round(principal * SERVICING_FEE_RATE, 2),
        })
    return out


def build_sample_workbook(path: str | Path) -> dict:
    from openpyxl import Workbook

    wb = Workbook()

    # ── Loans (input) ────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Loans"
    ws.append(["loan_id", "principal", "annual_rate", "term_months",
               "origination_date"])
    for row in LOANS:
        ws.append(list(row))

    # ── Assumptions (fixed parameters) ───────────────────────────────────
    wa = wb.create_sheet("Assumptions")
    wa["A1"] = "servicing_fee_rate"
    wa["B1"] = SERVICING_FEE_RATE

    # ── Calc (dragged-down formulas, one row per loan) ───────────────────
    wc = wb.create_sheet("Calc")
    wc.append(["loan_id", "monthly_rate", "monthly_payment",
               "interest_m1", "principal_m1", "servicing_fee", "rate_band"])
    for i in range(2, 2 + len(LOANS)):
        wc[f"A{i}"] = f"=Loans!A{i}"
        wc[f"B{i}"] = f"=Loans!C{i}/12"
        wc[f"C{i}"] = f"=ROUND(Loans!B{i}*B{i}/(1-(1+B{i})^-Loans!D{i}), 2)"
        wc[f"D{i}"] = f"=ROUND(Loans!B{i}*B{i}, 2)"
        wc[f"E{i}"] = f"=C{i}-D{i}"
        wc[f"F{i}"] = f"=ROUND(Loans!B{i}*Assumptions!$B$1, 2)"
        wc[f"G{i}"] = f'=IF(Loans!C{i}>0.06, "HIGH", "NORMAL")'

    # ── AmortSchedule (row-recursive: first loan, 6 periods) ─────────────
    wsch = wb.create_sheet("AmortSchedule")
    wsch.append(["period", "opening_balance", "interest",
                 "principal_paid", "closing_balance"])
    wsch["A2"] = 1
    wsch["B2"] = "=Loans!B2"
    wsch["C2"] = "=ROUND(B2*Loans!$C$2/12, 2)"
    wsch["D2"] = "=Calc!$C$2-C2"
    wsch["E2"] = "=B2-D2"
    for i in range(3, 8):
        wsch[f"A{i}"] = i - 1
        wsch[f"B{i}"] = f"=E{i - 1}"            # previous row's closing
        wsch[f"C{i}"] = f"=ROUND(B{i}*Loans!$C$2/12, 2)"
        wsch[f"D{i}"] = f"=Calc!$C$2-C{i}"
        wsch[f"E{i}"] = f"=B{i}-D{i}"

    # ── PMT_Template (headers-only empty input template — mirrors real
    # model workbooks whose input tabs were never filled in) ─────────────
    wt = wb.create_sheet("PMT_Template")
    wt.append(["PostingDate", "EffectiveDate", "InstrumentId", "Amount"])

    # ── Outputs (values-only expected results) ───────────────────────────
    wo = wb.create_sheet("Outputs")
    wo.append(["loan_id", "interest_accrual", "principal_repayment",
               "servicing_fee"])
    for row in expected_outputs():
        wo.append([row["loan_id"], row["interest_accrual"],
                   row["principal_repayment"], row["servicing_fee"]])

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return {
        "path": str(path),
        "loans": LOANS,
        "expected_outputs": expected_outputs(),
        "servicing_fee_rate": SERVICING_FEE_RATE,
    }


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "exports/sample_loan_model.xlsx"
    info = build_sample_workbook(out)
    print(f"Wrote {info['path']}")
    for r in info["expected_outputs"]:
        print("  ", r)
