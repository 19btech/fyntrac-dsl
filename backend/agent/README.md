# Autonomous Agent

Tool-calling LLM agent that can build event definitions, generate sample data,
author DSL templates, and dry-run them — all without writing a single line of
custom Python code.

## Components

| File | Purpose |
| --- | --- |
| `tools.py` | 48 LLM-callable tools wrapping existing services |
| `runtime.py` | Plan→Act→Observe loop with auto-debug, approval gates, SSE event stream |
| `__init__.py` | Public surface (`run_agent`, `submit_approval`, `cancel_run`, …) |

## Tools exposed to the LLM

Read-only / discovery: `list_events`, `list_dsl_functions` (filterable by
`name`/`category`, returns full signatures), `list_templates`,
`list_saved_rules`, `get_saved_rule`, `get_event_data`, `get_dsl_syntax_guide`,
`find_similar_template`, `list_canonical_patterns`, `get_canonical_pattern`

Self-correctness / introspection (no side effects — let the agent check its
own work before committing):
- `lint_expression` — statically validate one DSL expression with the exact
  checks the write path enforces.
- `preview_generated_code` — see the Python a rule compiles to + undefined-var
  findings, before dry-run.
- `explain_error` — map an error to its root cause + a copy-pasteable fix.
- `suggest_field_hints` — accounting-sensible sample-data ranges for an event.
- `revert_rule` — undo the last edit by restoring the previous saved version.

Write: `create_event_definitions`, `add_transaction_types`,
`generate_sample_event_data`, `validate_dsl`, `create_saved_rule`,
`update_saved_rule`, `add_step_to_rule`, `update_step`, `patch_step`,
`add_transaction_to_rule`, `create_saved_schedule`,
`create_or_replace_template`, `attach_rules_to_template`, `dry_run_template`,
`debug_step`, `verify_rule_complete`, …

Destructive (require user approval): `delete_template`, `delete_saved_rule`,
`delete_saved_schedule`, `clear_all_data`

Terminal: `finish`

## Guardrails

1. **No custom code escape hatch.** Tools reject any DSL that contains
   `customCode:`, `__import__`, `eval`, `exec`, `subprocess`, `os.system`, or
   `open(`. The existing `_validate_dsl_user_code` and `_validate_template_ast`
   in `server.py` provide a second AST-level layer. The exec sandbox builtins
   (`_make_sandbox_builtins` in `server.py`) drop `exec`/`eval`/`compile`/
   `open`/`input`/`breakpoint` AND replace `__import__` with a guard bound to a
   fixed module allow-list, so arbitrary imports fail even if AST validation is
   bypassed.
2. **Hard step cap.** `max_steps` configurable per request (50 from the HTTP
   endpoint; 80 default in `run_agent`).
3. **Approval gate.** Destructive tools emit `tool_pending` and block until the
   user clicks Approve via `POST /api/agent/runs/{run_id}/approve`.
4. **Truncated observations.** Tool results larger than 6000 chars are
   truncated before being fed back into the LLM context.
5. **Persisted runs.** Every run is saved to `db.agent_runs` (or in-memory
   fallback) with full event history.
6. **Cancellation.** `POST /api/agent/runs/{run_id}/cancel` aborts mid-loop.
7. **Auto-debug.** Tool errors are returned to the LLM as structured
   observations so the next turn can fix the problem instead of repeating it.
8. **Write-time validation.** Every rule write funnels through `_save_rule_doc`
   → `_validate_rule_static` (undefined-variable hard gate) and
   `_validate_step_shape` (expression linter: multi-line, bracket-indexing,
   `let`, semicolons, curly braces, unbalanced parens, unknown functions,
   JS-boolean coercion). Bad rules are rejected before they persist.
9. **Loop detection + self-correction.** The runtime buckets repeated errors,
   injects targeted recovery nudges that steer toward `explain_error` /
   `lint_expression` / `preview_generated_code` / `get_dsl_syntax_guide`, and
   hard-aborts after 6 identical failures.
10. **Delta workspace refresh.** On long runs the runtime re-injects a current
    workspace snapshot (as a user message, preserving the cached system prefix)
    when the agent has mutated state, so it never plans against stale names/ids.

## Performance

The Anthropic provider marks the (large, static) system prompt and tool schemas
with `cache_control: ephemeral`, so the prefix is served from Anthropic's prompt
cache (5-min TTL) on the 2nd+ turn of a run — cutting input-token cost and
latency materially on multi-step builds.

## Maker-checker (segregation of duties)

Set `REQUIRE_AGENT_APPROVAL=true` to require human sign-off on agent-authored
rules before they can reach production. When enabled:

- Every agent rule write (`_save_rule_doc`) is saved with
  `approval_status="pending"` and an `approval` block recording the agent as
  *maker*, the submission time, and a change summary. Editing an
  already-approved rule sends it back to `pending` (re-approval required).
- A **reviewer** lists the queue and approves/rejects:
  ```
  GET  /api/agent/approvals
  POST /api/agent/approvals/{rule_id}/approve   body: {checker, note}
  POST /api/agent/approvals/{rule_id}/reject    body: {checker, note}
  ```
  The `checker` must differ from the `maker` (the agent), enforcing
  segregation of duties; the decision is recorded for audit.
- **Deploy gate:** `POST /api/user-templates/{id}/deploy` refuses (409) while
  any rule in the template is `pending`/`rejected`. Rules with no
  `approval_status` (legacy / human-authored) are treated as approved, so
  existing templates never break.

The agent still freely authors, debugs, and dry-runs pending rules — approval
only gates the production deploy. Default is OFF (no behaviour change); turn it
ON for regulated/bank deployments. Covered by
`tests/test_agent_maker_checker.py`.

## Durable agent state

Run/session state is persisted to Mongo so it survives restarts and is shared
across workers (with an in-process fallback when no DB is configured):

- **Plans** → `db.agent_plans`, keyed by `session:<session_id>` (falling back to
  run id). Because the key is the stable session id, a continuation turn finds
  the plan submitted in an earlier turn directly — the old "inherit the most
  recent run's plan" heuristic was removed. `_RUN_PLANS` is now a write-through
  cache, not the source of truth.
- **Conversation history** → `db.agent_sessions`, keyed by `session_id`. Loaded
  at run start, saved (trimmed) at run end. `POST /api/agent/sessions/{id}/reset`
  clears both the DB record and the cache.

When `db is None` (in-memory mode) the behaviour is identical to before — the
in-process dicts are used as the fallback. Covered by
`tests/test_agent_state_store.py` (fake-Mongo round-trip + continuation case).

## HTTP endpoints

```
POST /api/agent/run                     SSE stream of run events
POST /api/agent/runs/{id}/approve?call_id=…  Approve / deny destructive tool
POST /api/agent/runs/{id}/cancel        Cancel a running agent
GET  /api/agent/runs                    List recent runs
GET  /api/agent/runs/{id}               Fetch full run history
GET  /api/agent/destructive-tools       Returns the list of guarded tools
POST /api/agent/sessions/{id}/reset     Clear a chat session's memory
GET  /api/agent/approvals               List rules pending human approval
POST /api/agent/approvals/{id}/approve  Approve a pending rule (maker-checker)
POST /api/agent/approvals/{id}/reject   Reject a pending rule
```

## Provider support

Tool-calling is implemented for **OpenAI** and **Anthropic Claude**.
Gemini falls back to `NotImplementedError`.

Recommended setup: Anthropic Claude (Sonnet/Opus class) as primary for
highest tool-calling reliability; OpenAI as an alternative.

## Sanity test

```
python tests/test_agent_runtime.py      # 6-turn scripted LLM session
python tests/test_agent_new_tools.py     # self-correctness tools + registry parity
python tests/test_agent_state_store.py   # DB-backed plan/session persistence
```

`test_agent_runtime.py` replays a scripted session and verifies tool dispatch,
error feedback, and final completion. `test_agent_new_tools.py` covers the
self-correctness tools (`lint_expression`, `explain_error`,
`suggest_field_hints`) and asserts schema/registry parity. `test_agent_state_store.py`
verifies plans and session history persist/round-trip through a fake Mongo and
that the no-session and db=None fallbacks behave correctly.
