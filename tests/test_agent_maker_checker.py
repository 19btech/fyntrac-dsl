"""Maker-checker controls for agent-authored rules:
  - REQUIRE_AGENT_APPROVAL toggles enforcement
  - _save_rule_doc marks agent writes `pending` with maker=agent when enabled
  - flag OFF leaves rules unmarked (backward compatible)
  - unapproved_rules_in_template (the deploy gate) blocks pending/rejected rules
    and treats legacy (no status) / approved rules as deployable

Uses a small in-memory fake of the async Mongo API; no real Mongo required.

Run:  python tests/test_agent_maker_checker.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


class _R:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *a, **k):
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
            if isinstance(v, dict):      # operator filter ($regex, …): wildcard
                if k not in d:
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
                    if "." in k:
                        parts = k.split(".")
                        cur = d
                        for p in parts[:-1]:
                            cur = cur.setdefault(p, {})
                        cur[parts[-1]] = v
                    else:
                        d[k] = v
                return _R(modified_count=1)
        if upsert:
            nd = dict(flt)
            nd.update({k: v for k, v in setv.items() if "." not in k})
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

    def aggregate(self, pipeline):
        return FakeCursor([])


class FakeDB:
    def __init__(self):
        self._c = {}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        c = self.__dict__.setdefault("_c", {})
        if name not in c:
            c[name] = FakeCollection()
        return c[name]


async def main():
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test_mc"
    import server  # noqa: F401 — configures bridge + helpers
    from agent import tools

    failures = []

    def check(cond, msg):
        print(f"  {'ok  ' if cond else 'FAIL'} — {msg}")
        if not cond:
            failures.append(msg)

    fake = FakeDB()
    tools._ServerBridge.db = fake

    def minimal_rule(name):
        return {
            "name": name,
            "priority": 100,
            "steps": [
                {"id": "s1", "name": "amount", "stepType": "calc",
                 "source": "value", "value": "100"},
            ],
            "outputs": {"printResult": True, "createTransaction": False,
                        "transactions": []},
            "inlineComment": False, "commentText": "",
        }

    # ── Flag OFF (default): no approval fields added ───────────────────────
    os.environ.pop("REQUIRE_AGENT_APPROVAL", None)
    check(tools._require_agent_approval() is False, "flag reads False when unset")
    r_off = await tools._save_rule_doc(minimal_rule("RuleOff"), is_new=True)
    check("approval_status" not in r_off,
          "flag OFF -> rule has no approval_status (backward compatible)")

    # ── Flag ON: agent write marked pending with maker=agent ──────────────
    os.environ["REQUIRE_AGENT_APPROVAL"] = "true"
    check(tools._require_agent_approval() is True, "flag reads True when set")
    r_on = await tools._save_rule_doc(minimal_rule("RuleOn"), is_new=True)
    check(r_on.get("approval_status") == "pending",
          "flag ON -> new rule marked pending")
    check((r_on.get("approval") or {}).get("maker") == "agent",
          "approval.maker recorded as agent")
    check(bool((r_on.get("approval") or {}).get("change_summary")),
          "approval.change_summary populated")

    # ── Editing an approved rule sends it back to pending ─────────────────
    # Simulate prior approval, then update via _save_rule_doc.
    for d in fake.saved_rules.docs:
        if d.get("name") == "RuleOn":
            d["approval_status"] = "approved"
    updated = minimal_rule("RuleOn")
    updated["id"] = r_on["id"]
    r_edit = await tools._save_rule_doc(updated, is_new=False)
    check(r_edit.get("approval_status") == "pending",
          "editing an approved rule resets it to pending (re-approval needed)")

    # ── Deploy gate: unapproved_rules_in_template ─────────────────────────
    sr = fake.saved_rules
    sr.docs = [
        {"id": "ra", "name": "Approved",  "approval_status": "approved"},
        {"id": "rp", "name": "Pending",   "approval_status": "pending"},
        {"id": "rj", "name": "Rejected",  "approval_status": "rejected"},
        {"id": "rl", "name": "Legacy"},  # no approval_status (human-authored)
    ]
    tpl = {"rules": [{"id": "ra"}, {"id": "rp"}, {"id": "rj"}, {"id": "rl"},
                     {"id": "ra"}]}  # duplicate id to test dedupe
    blocked = await tools.unapproved_rules_in_template(fake, tpl)
    blocked_names = sorted(b["name"] for b in blocked)
    check(blocked_names == ["Pending", "Rejected"],
          "deploy gate blocks only pending+rejected (legacy/approved deployable)")
    check(len(blocked) == 2, "deploy gate dedupes repeated rule ids")

    # ── All-approved template is deployable ───────────────────────────────
    tpl_ok = {"rules": [{"id": "ra"}, {"id": "rl"}]}
    check(await tools.unapproved_rules_in_template(fake, tpl_ok) == [],
          "template with only approved/legacy rules is not blocked")

    os.environ.pop("REQUIRE_AGENT_APPROVAL", None)
    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s)")
        sys.exit(1)
    print("OK — maker-checker checks passed")


if __name__ == "__main__":
    asyncio.run(main())
