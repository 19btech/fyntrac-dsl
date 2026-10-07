"""Downloading a case's frozen event data and its expected transactions.

The event-data workbook must use the layout the event-data upload reads (one
sheet per event, named after it), so a case can be reloaded into the app and
reproduced by hand. The expected workbook is the baseline transaction report.
"""

import io
import os
import sys

import openpyxl
import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import regression as R  # noqa: E402
from tests.test_regression_workspace import EVT, capture, db, run  # noqa: E402,F401


def _book(response):
    return openpyxl.load_workbook(io.BytesIO(response.body))


def test_dataset_export_is_one_sheet_per_event(db):
    case_id = capture()["id"]
    resp = run(R.export_case_dataset(case_id))
    assert "eventdata_v1.xlsx" in resp.headers["content-disposition"]
    wb = _book(resp)
    assert wb.sheetnames == [EVT]
    rows = list(wb[EVT].values)
    header = [str(c).lower() for c in rows[0]]
    record = dict(zip(header, rows[1]))
    assert record["instrumentid"] == "SO-1"
    assert record["amt"] == 100.0
    assert len(rows) == 2


def test_expected_export_is_the_baseline(db):
    case_id = capture()["id"]
    resp = run(R.export_case_expected(case_id))
    assert "expected_v1.xlsx" in resp.headers["content-disposition"]
    rows = list(_book(resp)["transactions"].values)
    assert list(rows[0]) == list(R.TXN_FIELDS)
    record = dict(zip(rows[0], rows[1]))
    assert record["instrumentid"] == "SO-1"
    assert record["transactiontype"] == "Rev"
    assert record["amount"] == 100.0


def test_unknown_case_or_version_is_404(db):
    with pytest.raises(HTTPException) as exc:
        run(R.export_case_expected("nope"))
    assert exc.value.status_code == 404
    case_id = capture()["id"]
    with pytest.raises(HTTPException) as exc:
        run(R.export_case_dataset(case_id, version=9))
    assert exc.value.status_code == 404
