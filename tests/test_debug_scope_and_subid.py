"""debug_step must see the data a real run sees; explicit sub-ids must survive.

1. A full-book run hands the engine only the posting date's rows, and
   collect_by_instrument() reads that raw dict. debug_step handed it EVERY
   date's rows, so a collector showed the whole history in the probe while the
   run saw one date -- a cumulative cap looked binding under debug_step (0)
   and the run booked the uncapped amount.

2. `_normalise_transaction_outputs` overwrote an explicit per-element
   subInstrumentId (e.g. `del_subs`) with the row-scalar `subinstrumentid`
   whenever the rule had that alias step. createTransaction then broadcast the
   one sub across every amount, so a surviving amount landed on the wrong sub.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server  # noqa: E402,F401
from backend.agent import tools as T  # noqa: E402
from tests.fake_mongo import FakeDB  # noqa: E402

EVT = "DELIVERY"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.fixture
def bridge(monkeypatch):
    rows = [
        {"instrumentid": "I1", "subinstrumentid": "S1", "postingdate": "2026-01-31",
         "effectivedate": "2026-01-31", "units": 1.0},
        {"instrumentid": "I1", "subinstrumentid": "S1", "postingdate": "2026-02-28",
         "effectivedate": "2026-02-28", "units": 2.0},
        {"instrumentid": "I1", "subinstrumentid": "S1", "postingdate": "2026-03-31",
         "effectivedate": "2026-03-31", "units": 4.0},
    ]
    fake = FakeDB(
        event_definitions=[{"event_name": EVT, "eventType": "activity",
                            "eventTable": "standard",
                            "fields": [{"name": "units", "datatype": "decimal"}]}],
        event_data=[{"event_name": EVT, "data_rows": rows}],
        saved_rules=[{
            "id": "r1", "name": "Probe", "outputs": {},
            "steps": [
                {"name": "postingdate", "stepType": "calc", "source": "event_field",
                 "eventField": f"{EVT}.postingdate"},
                {"name": "all_units", "stepType": "calc", "source": "formula",
                 "formula": f"collect_by_instrument('{EVT}_units')"},
            ],
        }],
    )
    monkeypatch.setattr(T._ServerBridge, "db", fake)
    return fake


def test_debug_step_collects_only_the_posting_dates_rows(bridge):
    out = _run(T.tool_debug_step({"rule_id": "r1", "step_name": "all_units",
                                  "posting_date": "2026-02-28"}))
    line = " ".join(str(x) for x in out["debug_outputs"])
    assert "2.0" in line
    assert "1.0" not in line and "4.0" not in line, line


def _outputs(sid):
    steps = [{"name": "subinstrumentid", "stepType": "calc",
              "formula": f"{EVT}.subinstrumentid"}]
    txn = {"type": "Revenue", "amount": "amt", "postingDate": "postingdate",
           "effectiveDate": "effectivedate"}
    if sid is not None:
        txn["subInstrumentId"] = sid
    return T._normalise_transaction_outputs(steps, {"transactions": [txn]})


def test_explicit_subinstrument_array_is_kept():
    assert _outputs("del_subs")["transactions"][0]["subInstrumentId"] == "del_subs"


@pytest.mark.parametrize("sid", [None, "", "1", "1.0"])
def test_default_subinstrument_uses_the_alias_step(sid):
    assert _outputs(sid)["transactions"][0]["subInstrumentId"] == "subinstrumentid"
