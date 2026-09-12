"""Regression tests for reading MULTIPLE sub-instrument rows out of one event.

Every documented way to read the N line-items of one parent instrument
returned nothing:

  * collect_by_instrument(...)      -> []
  * collect_all(...) on a reference -> []
  * collect_by_subinstrument(...)   -> 500 "'float' object has no attribute 'split'"
  * an array-valued transaction amount -> 0 transactions

Two root causes:

1. The collect_*() helpers parsed the flattened 'EVENTNAME_fieldname' argument
   with field_name.split('_', 1). Any event name containing an underscore
   (SO_EVENT, line_items, sales_order) split at the wrong boundary, matched no
   event, and returned []. The empty array then made createTransaction emit
   nothing, which looked like a separate "array amounts don't work" bug.
2. The translator rewrote collect_by_instrument(EVT.f) and collect_all(EVT.f)
   into their quoted-string form but NOT collect_by_subinstrument(EVT.f), so
   that one received the flattened row VARIABLE (a float) and crashed.

Plus: identifier fields were coerced to float, so subinstrumentid '1' became
1.0 and joins against the row built-in silently missed.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.server import (  # noqa: E402
    dsl_to_python_multi_event,
    execute_python_template,
    merge_event_data_by_instrument,
)

# Event names that exercise the underscore-boundary bug from every angle.
EVENT_NAMES = ["SOEVENT", "SO_EVENT", "sales_order", "SalesOrder", "PO_LINE_ITEMS"]

LINES = [
    {"instrumentid": "SO-1", "subinstrumentid": "1", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-A", "line_amount": 800.0},
    {"instrumentid": "SO-1", "subinstrumentid": "2", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-B", "line_amount": 400.0},
    {"instrumentid": "SO-1", "subinstrumentid": "3", "postingdate": "2026-01-31",
     "effectivedate": "2026-01-31", "product_id": "PO-C", "line_amount": 250.0},
]


def _fields(evt, event_type="activity"):
    return {evt: {"fields": [{"name": "product_id", "datatype": "string"},
                             {"name": "line_amount", "datatype": "decimal"}],
                  "eventType": event_type}}


def _run(evt, dsl, event_type="activity"):
    fields = _fields(evt, event_type)
    py = dsl_to_python_multi_event(dsl, fields)
    rows = [dict(r) for r in LINES]
    merged = ([{}] if event_type == "reference"
              else merge_event_data_by_instrument({evt: rows}))
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            execute_python_template(py, merged, {evt: rows}, None, None))
    finally:
        loop.close()


def _printed(out, label):
    for line in out.get("print_outputs") or []:
        if str(line).startswith(label):
            return str(line)
    raise AssertionError(f"no print starting {label!r} in {out.get('print_outputs')}")


# -- collect_by_instrument -------------------------------------------------
@pytest.mark.parametrize("evt", EVENT_NAMES)
def test_collect_by_instrument_reads_every_subinstrument(evt):
    out = _run(evt, (f"amts = collect_by_instrument({evt}.line_amount)\n"
                     f"prods = collect_by_instrument({evt}.product_id)\n"
                     'print("n=", array_length(amts), array_length(prods))\n'
                     'print("sum=", sum(amts))\n'))
    assert "3 3" in _printed(out, "n=")
    assert "1450" in _printed(out, "sum=")


# -- collect_all on a reference event --------------------------------------
@pytest.mark.parametrize("evt", ["CATALOG", "PO_LINES", "po_line_items"])
def test_collect_all_reads_reference_rows(evt):
    out = _run(evt, (f"prods = collect_all({evt}.product_id)\n"
                     'print("n=", array_length(prods))\n'),
               event_type="reference")
    assert "3" in _printed(out, "n=")


# -- collect_by_subinstrument ----------------------------------------------
@pytest.mark.parametrize("evt", EVENT_NAMES)
def test_collect_by_subinstrument_does_not_crash(evt):
    """Used to raise 500 "'float' object has no attribute 'split'" because the
    translator never rewrote this form into its quoted-string argument."""
    out = _run(evt, (f"v = collect_by_subinstrument({evt}.line_amount)\n"
                     'print("v=", v)\n'))
    # The current row is subId 1, so its line_amount is the one value returned.
    assert "800" in _printed(out, "v=")


@pytest.mark.parametrize("evt", EVENT_NAMES)
def test_translator_quotes_collect_by_subinstrument_argument(evt):
    py = dsl_to_python_multi_event(
        f"v = collect_by_subinstrument({evt}.line_amount)", _fields(evt))
    call = [ln for ln in py.split("\n")
            if "collect_by_subinstrument(" in ln and "def " not in ln]
    assert any(f"'{evt}_line_amount'" in ln for ln in call), call


# -- identifier fields stay strings ----------------------------------------
@pytest.mark.parametrize("evt", ["SO_EVENT", "SalesOrder"])
def test_collected_subinstrumentid_joins_against_the_row_builtin(evt):
    """subinstrumentid used to come back as 1.0/2.0/3.0 while the row built-in
    is the string '1', so this lookup silently returned None."""
    out = _run(evt, (f"sub_ids = collect_by_instrument({evt}.subinstrumentid)\n"
                     f"amts = collect_by_instrument({evt}.line_amount)\n"
                     'print("types=", str([type(x).__name__ for x in sub_ids]))\n'
                     'print("mine=", lookup(amts, sub_ids, subinstrumentid))\n'))
    assert "float" not in _printed(out, "types=")
    assert "800" in _printed(out, "mine=")


def test_numeric_fields_are_still_coerced_to_float():
    out = _run("SO_EVENT", ("amts = collect_by_instrument(SO_EVENT.line_amount)\n"
                            'print("types=", str([type(x).__name__ for x in amts]))\n'))
    assert "float" in _printed(out, "types=")


# -- array-valued transaction amounts --------------------------------------
@pytest.mark.parametrize("evt", EVENT_NAMES)
def test_array_amount_fans_out_one_transaction_per_subinstrument(evt):
    """The documented fan-out from the validator's own block message. Emitted
    0 transactions only because collect_by_instrument returned []."""
    out = _run(evt, (f"amts = collect_by_instrument({evt}.line_amount)\n"
                     f"sub_ids = collect_by_instrument({evt}.subinstrumentid)\n"
                     f'createTransaction({evt}.postingdate, {evt}.effectivedate, '
                     f'"Revenue", amts, sub_ids)\n'))
    txns = out.get("transactions") or []
    assert len(txns) == 3, txns
    assert [t.subinstrumentid for t in txns] == ["1", "2", "3"]
    assert [t.amount for t in txns] == [800.0, 400.0, 250.0]


def test_bare_field_name_without_event_prefix_still_resolves():
    """A name with no known event prefix means 'look in every event' — it used
    to be split as event 'line' + field 'amount' and match nothing."""
    out = _run("SO_EVENT", ("amts = collect_all('line_amount')\n"
                            'print("n=", array_length(amts))\n'))
    assert "3" in _printed(out, "n=")
