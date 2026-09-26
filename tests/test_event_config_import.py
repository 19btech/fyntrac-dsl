"""Event datatypes must not be invented by the field name alone.

EventConfigurations.json is a schema file: it lists column names and carries no
values and no declared types, so backend/server.py infers a type from the name.
That guess is load-bearing — it drives the cast emitted into generated code and
the coercion applied at event-data ingest, where a `decimal` field whose value
fails float() used to be silently replaced with 0.0.

Two independent guards:

  * the name heuristic no longer types identifiers as numbers, and its keyword
    matching no longer fires inside unrelated words;
  * the ingest reconciles the declared type against the actual data before any
    coercion runs, so a wrong guess is corrected instead of destroying values.

FyntracPythonModel/data_transformer.py infers from values instead of names. It
returned on the FIRST non-null value while its docstring promised a scan with a
precedence, which made a column's type depend on row order.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.server import (  # noqa: E402
    _infer_field_dt,
    _reconcile_field_types,
    _transform_event_configurations,
)
from FyntracPythonModel.data_transformer import _infer_field_datatype  # noqa: E402

IMPORTFLOW = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Importflow"
)


# -- name heuristic ---------------------------------------------------------
@pytest.mark.parametrize("name,expected", [
    # Identifiers stay strings. Typing these as decimal made the ingest run
    # float("SKU-001") -> except -> 0.0, and dropped the padding from "00123".
    ("ProductId", "string"),
    ("ATTRIBUTE_PRODUCT_ID_CURRENT", "string"),
    ("customer_id", "string"),
    # Genuine numerics still resolve, including run-together words.
    ("TRANSACTIONS_AMOUNT_REMIT", "decimal"),
    ("ATTRIBUTE_LOANAMOUNT_CURRENT", "decimal"),
    ("ATTRIBUTE_INTEREST_RATE_CURRENT", "decimal"),
    ("ATTRIBUTE_NOTERATE_CURRENT", "decimal"),
    ("ATTRIBUTE_TERM_CURRENT", "decimal"),
    ("ExpectedCF", "decimal"),
    # Genuine dates still resolve.
    ("StartDate", "date"),
    ("EFFECTIVE_DATE", "date"),
    ("ATTRIBUTE_ITEM_STARTDATE_CURRENT", "date"),
    # Keywords must not fire inside unrelated words.
    ("UPDATEDBY", "string"),
    ("MANDATE", "string"),
    ("VALIDATED", "string"),
    ("CORPORATE_NAME", "string"),
    ("TERMINATION_REASON", "string"),
    ("FEEDBACK", "string"),
    ("ProductName", "string"),
])
def test_infer_field_dt(name, expected):
    assert _infer_field_dt(name) == expected


# -- the real sample files --------------------------------------------------
def _defs_for(stem):
    import json
    path = os.path.join(IMPORTFLOW, f"{stem}.EventConfigurations.json")
    with open(path, encoding="utf-8") as fh:
        return {d["event_name"]: d for d in _transform_event_configurations(json.load(fh))}


def _types(defn):
    return {f["name"]: f["datatype"] for f in defn["fields"]}


def test_revenue_catalog_product_id_is_a_string():
    """The reported bug: CATALOG.ProductId came back decimal."""
    types = _types(_defs_for("Revenue")["CATALOG"])
    assert types["ProductId"] == "string"
    assert types["ProductName"] == "string"
    assert types["Amount"] == "decimal"


def test_balance_phases_remain_decimal():
    """BALANCES_* phases are hardcoded decimal and must not regress."""
    types = _types(_defs_for("Revenue")["EOD"])
    phases = [n for n in types if n.startswith("BALANCES_")]
    assert phases, "expected BALANCES_* fields"
    assert all(types[n] == "decimal" for n in phases)


def test_ifrs9_attribute_types():
    types = _types(_defs_for("IFRS9Stage3")["EOD"])
    assert types["ATTRIBUTE_ORIGINATIONDATE_CURRENT"] == "date"
    assert types["ATTRIBUTE_LOANAMOUNT_CURRENT"] == "decimal"
    assert types["ATTRIBUTE_NOTERATE_CURRENT"] == "decimal"


# -- ingest reconciliation --------------------------------------------------
def test_non_numeric_values_demote_a_decimal_field():
    """The guess loses to the data instead of overwriting it with 0.0."""
    rows = [{"ProductId": "SKU-001"}, {"ProductId": "SKU-002"}]
    types, corrections = _reconcile_field_types(rows, {"ProductId": "decimal"})
    assert types["ProductId"] == "string"
    assert corrections["ProductId"]["from"] == "decimal"
    assert corrections["ProductId"]["sample"] == "SKU-001"


def test_zero_padded_ids_are_not_numeric():
    """float("00123") succeeds but discards padding that carries meaning."""
    rows = [{"AccountId": "00123"}]
    types, _ = _reconcile_field_types(rows, {"AccountId": "decimal"})
    assert types["AccountId"] == "string"


def test_genuinely_numeric_fields_are_left_alone():
    rows = [{"Amount": 100.5}, {"Amount": "1,200.75"}, {"Amount": ""}]
    types, corrections = _reconcile_field_types(rows, {"Amount": "decimal"})
    assert types["Amount"] == "decimal"
    assert corrections == {}


def test_blank_only_columns_keep_their_declared_type():
    """No values means no evidence — do not demote on emptiness alone."""
    rows = [{"Amount": ""}, {"Amount": None}]
    types, corrections = _reconcile_field_types(rows, {"Amount": "decimal"})
    assert types["Amount"] == "decimal"
    assert corrections == {}


def test_non_numeric_declared_types_are_untouched():
    rows = [{"Name": "abc"}]
    types, corrections = _reconcile_field_types(rows, {"Name": "string"})
    assert types["Name"] == "string"
    assert corrections == {}


# -- value inference in the export runtime ----------------------------------
def test_value_inference_is_row_order_independent():
    """Returning on the first non-null value made this order-dependent."""
    assert _infer_field_datatype([None, "N/A", 42]) == "string"
    assert _infer_field_datatype([42, "N/A"]) == "string"
    assert _infer_field_datatype(["N/A", 42]) == "string"


@pytest.mark.parametrize("values,expected", [
    ([1250.50, 42], "decimal"),
    (["1250.50", "42"], "decimal"),
    (["1,234.56"], "decimal"),
    (["2026-01-31"], "date"),
    ([{"$date": "2026-01-31T00:00:00Z"}], "date"),
    ([True, 1], "boolean"),
    (["SKU-001"], "string"),
    (["00123"], "string"),
    ([], "decimal"),
    ([None, "", "  "], "decimal"),
])
def test_value_inference(values, expected):
    assert _infer_field_datatype(values) == expected
