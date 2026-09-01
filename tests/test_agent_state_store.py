"""Verify the DB-backed migration of agent run/session state:
  - plans persist to agent_plans and survive a cache flush (restart/worker)
  - plans are keyed by session_id, so a NEW run_id in the SAME session still
    finds the plan (the continuation case the old inheritance hack handled)
  - session history round-trips through agent_sessions and reset clears it

Uses a minimal in-memory fake of the async Mongo collection API so no real
Mongo is required.

Run:  python tests/test_agent_state_store.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


class FakeResult:
    def __init__(self, deleted_count=0):
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self):
        self.docs = []

    @staticmethod
    def _match(doc, flt):
        return all(doc.get(k) == v for k, v in flt.items())

    async def find_one(self, flt, projection=None):
        for d in self.docs:
            if self._match(d, flt):
                return dict(d)
        return None

    async def update_one(self, flt, update, upsert=False):
        setv = update.get("$set", {})
        for d in self.docs:
            if self._match(d, flt):
                d.update(setv)
                return FakeResult()
        if upsert:
            newd = dict(flt)
            newd.update(setv)
            self.docs.append(newd)
        return FakeResult()

    async def delete_one(self, flt):
        for i, d in enumerate(self.docs):
            if self._match(d, flt):
                del self.docs[i]
                return FakeResult(deleted_count=1)
        return FakeResult(deleted_count=0)


class FakeDB:
    def __init__(self):
        self.agent_plans = FakeCollection()
        self.agent_sessions = FakeCollection()


async def main():
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test_state"
    import server  # noqa: F401 — configures the bridge
    from agent import tools, runtime

    failures = []

    def check(cond, msg):
        print(f"  {'ok  ' if cond else 'FAIL'} — {msg}")
        if not cond:
            failures.append(msg)

    fake = FakeDB()
    tools._ServerBridge.db = fake  # point the plan store at the fake DB

    # ── Plan persistence + cache-miss reload (restart/worker simulation) ──
    tools.set_current_session_id("SESS-A")
    tools.set_current_run_id("run-1")
    key = tools._plan_key_from_context()
    check(key == "session:SESS-A", "plan key derives from session id")
    await tools._store_plan(key, {"key": key, "pattern_id": "A", "rules": []})
    check(any(d.get("_key") == key for d in fake.agent_plans.docs),
          "plan persisted to agent_plans collection")
    tools._RUN_PLANS.clear()  # simulate a fresh process / different worker
    got = await tools._get_plan(key)
    check(got is not None and got.get("pattern_id") == "A",
          "plan reloads from DB after cache flush")

    # ── Continuation: NEW run_id, SAME session → plan still found ─────────
    tools.set_current_run_id("run-2")
    tools._RUN_PLANS.clear()
    key2 = tools._plan_key_from_context()
    check(key2 == key, "new run_id in same session yields same plan key")
    check((await tools._get_plan(key2)) is not None,
          "plan survives across runs in the same session (no inheritance hack)")

    # ── No session → falls back to run-id key (isolated per turn) ─────────
    tools.set_current_session_id("")
    tools.set_current_run_id("run-x")
    check(tools._plan_key_from_context() == "run-x",
          "no session id -> plan key falls back to run id")

    # ── Session history round-trip via DB ─────────────────────────────────
    msgs = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello", "tool_calls": []},
    ]
    await runtime._save_session_history(fake, "SESS-A", msgs)
    loaded = await runtime._load_session_history(fake, "SESS-A")
    check(loaded and loaded[-1]["content"] == "hello",
          "session history round-trips through agent_sessions")
    check("SESS-A" not in runtime._SESSION_HISTORY,
          "in-memory session cache NOT used when a DB is present")

    # ── reset clears the DB record ────────────────────────────────────────
    cleared = await runtime.reset_session_history("SESS-A", db=fake)
    check(cleared, "reset_session_history reports cleared=True")
    check((await runtime._load_session_history(fake, "SESS-A")) == [],
          "session history empty after reset")

    # ── In-memory fallback still works when db is None ───────────────────
    await runtime._save_session_history(None, "SESS-MEM",
                                        [{"role": "user", "content": "x"}])
    check(runtime._SESSION_HISTORY.get("SESS-MEM"),
          "db=None falls back to in-process session cache")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s)")
        sys.exit(1)
    print("OK — state-store (DB-backed) checks passed")


if __name__ == "__main__":
    asyncio.run(main())
