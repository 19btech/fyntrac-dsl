"""Smoke-test the agent runtime with a stub provider that scripts the LLM
turns. Verifies tool dispatch, persistence, error feedback, and finish.

Run:  python tests/test_agent_runtime.py
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


class StubProvider:
    """Replays a pre-canned list of LLM responses turn-by-turn."""

    def __init__(self, script):
        self.script = list(script)
        self.turn = 0

    async def chat_with_tools(self, *, api_key, model, messages, tools, temperature=0.1):
        if self.turn >= len(self.script):
            return {
                "message": {"role": "assistant", "content": "[stub: no more turns]",
                              "tool_calls": []},
                "tool_calls": [], "finish_reason": "stop", "usage": None,
            }
        out = self.script[self.turn]
        self.turn += 1
        return out


def tc(call_id, name, args):
    return {"id": call_id, "name": name, "arguments": args}


async def main():
    # Force in-memory mode by pointing at an unreachable Mongo
    os.environ["MONGO_URL"] = "mongodb://127.0.0.1:1/__nope__"
    os.environ["DB_NAME"] = "agent_test"

    import server  # noqa: F401  — registers agent bridge

    from agent.runtime import run_agent

    provider = StubProvider([
        # Turn 1: list events
        {"message": {"role": "assistant", "content": "Looking at the data model.",
                       "tool_calls": [tc("c1", "list_events", {})]},
          "tool_calls": [tc("c1", "list_events", {})]},
        # Turn 2: create an event
        {"message": {"role": "assistant", "content": "Creating Loan event.",
                       "tool_calls": [tc("c2", "create_event_definitions", {
                           "events": [{
                               "event_name": "Loan", "eventType": "activity",
                               "eventTable": "standard",
                               "fields": [
                                   {"name": "principal", "datatype": "decimal"},
                                   {"name": "rate", "datatype": "decimal"},
                                   {"name": "term", "datatype": "integer"},
                               ],
                           }],
                       })]},
          "tool_calls": [tc("c2", "create_event_definitions", {
              "events": [{
                  "event_name": "Loan", "eventType": "activity",
                  "eventTable": "standard",
                  "fields": [
                      {"name": "principal", "datatype": "decimal"},
                      {"name": "rate", "datatype": "decimal"},
                      {"name": "term", "datatype": "integer"},
                  ],
              }],
          })]},
        # Turn 3: try a bad DSL — expect tool_error
        {"message": {"role": "assistant", "content": "Trying bad code.",
                       "tool_calls": [tc("c3", "validate_dsl", {
                           "dsl_code": "customCode:\n  pass\nend",
                       })]},
          "tool_calls": [tc("c3", "validate_dsl", {
              "dsl_code": "customCode:\n  pass\nend",
          })]},
        # Turn 4: generate sample data
        {"message": {"role": "assistant", "content": "Generating sample data.",
                       "tool_calls": [tc("c4", "generate_sample_event_data", {
                           "event_name": "Loan",
                           "instrument_count": 3,
                           "posting_dates": ["2026-01-01"],
                       })]},
          "tool_calls": [tc("c4", "generate_sample_event_data", {
              "event_name": "Loan", "instrument_count": 3,
              "posting_dates": ["2026-01-01"],
          })]},
        # Turn 5: validate good DSL
        {"message": {"role": "assistant", "content": "Validating good DSL.",
                       "tool_calls": [tc("c5", "validate_dsl", {
                           "dsl_code": (
                               'monthly_interest = multiply(Loan.principal, divide(Loan.rate, 12))\n'
                               'createTransaction(postingdate, effectivedate, "InterestAccrual", monthly_interest)\n'
                           ),
                           "event_name": "Loan",
                       })]},
          "tool_calls": [tc("c5", "validate_dsl", {
              "dsl_code": (
                  'monthly_interest = multiply(Loan.principal, divide(Loan.rate, 12))\n'
                  'createTransaction(postingdate, effectivedate, "InterestAccrual", monthly_interest)\n'
              ),
              "event_name": "Loan",
          })]},
        # Turn 6: finish
        {"message": {"role": "assistant", "content": "All done.",
                       "tool_calls": [tc("c6", "finish", {"summary": "Built Loan event with 3 sample rows; validated InterestAccrual rule."})]},
          "tool_calls": [tc("c6", "finish", {"summary": "Built Loan event with 3 sample rows; validated InterestAccrual rule."})]},
    ])

    events = []
    async for ev in run_agent(
        task="Build a tiny Loan setup",
        provider=provider, api_key="x", model="stub",
        db=None, in_memory_data=server.in_memory_data,
        max_steps=10,
    ):
        events.append(ev)
        if ev["type"] in ("tool_done", "tool_error", "final", "error"):
            preview = (ev.get("result") or ev.get("error") or ev.get("summary") or "")
            print(f"  [{ev['type']:10s}] {ev.get('name','')}  {str(preview)[:140]}")
        else:
            print(f"  [{ev['type']:10s}] step={ev.get('step','-')}  {ev.get('name','') or ev.get('content','')[:80]}")

    types = [e["type"] for e in events]
    assert "run_started" in types
    assert "tool_done" in types
    assert "tool_error" in types, "Expected the customCode DSL to be rejected"
    assert "final" in types
    final = [e for e in events if e["type"] == "final"][0]
    assert final["status"] == "completed", f"Run did not complete: {final}"
    print("\nOK — runtime produced", len(events), "events; final status =", final["status"])


if __name__ == "__main__":
    asyncio.run(main())
