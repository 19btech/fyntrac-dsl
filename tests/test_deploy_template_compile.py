"""Tests for the DSL->Python compile inside POST /api/user-templates/{id}/deploy.

This is the ONLY path that compiles the artifact the production runtime
(FyntracPythonModel.ModelRunner, deployed as fyntrac-py-model) executes, and it
was the only event lookup on the platform that was case-SENSITIVE. A model whose
rules spelled an event differently from event_definitions therefore resolved no
events at all, fell through to dsl_to_python_standalone, and shipped an artifact
in which every `EVENT.field` survives unrewritten -- and in which the collect_*
functions are not even defined, since only the multi-event scaffolding declares
them. Every instrument then failed with `name 'EVENT' is not defined` on the
first event reference, while the dry-run harness (which resolves events
case-insensitively) stayed green.

Two behaviours that matter:
  * the lookup matches regardless of case, like /templates/execute,
    run_transaction_report and agent tools._find_event_def;
  * a model that references events but resolves none is REFUSED, not quietly
    compiled standalone -- that artifact can never run correctly.
"""
import asyncio
import os
import re
import sys

import pytest
from fastapi import HTTPException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402

EVT = "SALE_ORDER_DETAILS"
DSL = (f'postingdate = {EVT}.postingdate\n'
       f'subs = collect_by_instrument({EVT}.subinstrumentid)\n'
       f'createTransaction(postingdate, postingdate, "Rev", {EVT}.amt)\n')


class _Col:
    def __init__(self, docs=()):
        self.docs = list(docs)

    async def find_one(self, q, proj=None, sort=None):
        hits = [d for d in self.docs if self._match(d, q)]
        if sort:
            key, direction = sort[0]
            hits.sort(key=lambda d: d.get(key) or 0, reverse=direction < 0)
        return hits[0] if hits else None

    @staticmethod
    def _match(d, q):
        for k, v in q.items():
            if isinstance(v, dict) and "$regex" in v:
                if not re.match(v["$regex"], str(d.get(k, "")), re.I):
                    return False
            elif isinstance(v, dict) and "$lt" in v:
                if not (d.get(k) is not None and d[k] < v["$lt"]):
                    return False
            elif d.get(k) != v:
                return False
        return True

    async def insert_one(self, doc):
        self.docs.append(doc)

    async def update_one(self, q, update, upsert=False):
        for d in self.docs:
            if self._match(d, q):
                d.update(update.get("$set") or {})
                return
        if upsert:
            doc = dict(update.get("$setOnInsert") or {})
            doc.update(update.get("$set") or {})
            self.docs.append(doc)

    async def delete_many(self, q):
        self.docs = [d for d in self.docs if not self._match(d, q)]


class _DB:
    def __init__(self, stored_event_name):
        self.event_definitions = _Col([
            {"event_name": stored_event_name, "eventType": "activity",
             "fields": [{"name": "amt", "datatype": "decimal"},
                        {"name": "subinstrumentid", "datatype": "string"}]}])
        self.user_templates = _Col([
            {"id": "u1", "name": "Hearst_FinalV7", "combinedCode": DSL,
             "rules": [{"id": "r1"}]}])
        self.dsl_templates = _Col()
        self.dsl_template_artifacts = _Col()


# settings is a frozen dataclass; require_agent_approval defaults to False,
# so the maker-checker gate in deploy_user_template is inert here.
assert S.settings.require_agent_approval is False


def _deploy(db, monkeypatch, template_id="u1"):
    monkeypatch.setattr(S, "db", db)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(S.deploy_user_template(template_id))
    finally:
        loop.close()


def _artifact(db):
    assert db.dsl_template_artifacts.docs, "no artifact written"
    return db.dsl_template_artifacts.docs[-1]["python_code"]


# -- the lookup matches regardless of case --------------------------------
@pytest.mark.parametrize("stored", [EVT, EVT.lower(), "Sale_Order_Details"])
def test_event_resolves_whatever_the_stored_casing(stored, monkeypatch):
    db = _DB(stored)
    _deploy(db, monkeypatch)
    py = _artifact(db)
    assert "def process_event_data" in py
    assert "def process_standalone" not in py


def test_dotted_event_references_are_rewritten(monkeypatch):
    db = _DB(EVT.lower())
    _deploy(db, monkeypatch)
    py = _artifact(db)
    assert f"{EVT}.postingdate" not in py, "EVENT.field survived the compile"
    assert "collect_by_instrument('" in py, "collector argument was not quoted"


def test_field_reads_use_the_stored_spelling(monkeypatch):
    """The compiler keys field extraction off all_event_fields, and merged rows
    are prefixed with the STORED event name -- so both must agree."""
    db = _DB("Sale_Order_Details")
    _deploy(db, monkeypatch)
    py = _artifact(db)
    assert "Sale_Order_Details_amt = float(" in py


# -- a model that references events but resolves none is refused ----------
def test_unresolvable_events_refuse_the_deploy(monkeypatch):
    db = _DB("SOMETHING_ELSE_ENTIRELY")
    with pytest.raises(HTTPException) as exc:
        _deploy(db, monkeypatch)
    assert exc.value.status_code == 400
    assert EVT in str(exc.value.detail)


def test_a_refused_deploy_writes_nothing(monkeypatch):
    db = _DB("SOMETHING_ELSE_ENTIRELY")
    with pytest.raises(HTTPException):
        _deploy(db, monkeypatch)
    assert db.dsl_template_artifacts.docs == []
    assert db.dsl_templates.docs == []


def test_standalone_artifact_is_never_emitted_for_an_event_model(monkeypatch):
    """The regression itself: the old code compiled this standalone."""
    db = _DB("SOMETHING_ELSE_ENTIRELY")
    with pytest.raises(HTTPException):
        _deploy(db, monkeypatch)
    assert not any("process_standalone" in (d.get("python_code") or "")
                   for d in db.dsl_template_artifacts.docs)


# -- a genuine standalone model still deploys -----------------------------
def test_event_free_model_still_compiles_standalone(monkeypatch):
    db = _DB(EVT)
    db.user_templates.docs[0]["combinedCode"] = (
        'createTransaction("2026-01-31", "2026-01-31", "Fee", 100)\n')
    _deploy(db, monkeypatch)
    py = _artifact(db)
    assert "def process_standalone" in py
