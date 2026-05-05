# Autonomous Agent

Tool-calling LLM agent that can build event definitions, generate sample data,
author DSL templates, and dry-run them — all without writing a single line of
custom Python code.

## Components

| File | Purpose |
| --- | --- |
| `tools.py` | 13 LLM-callable tools wrapping existing services |
| `runtime.py` | Plan→Act→Observe loop with auto-debug, approval gates, SSE event stream |
| `__init__.py` | Public surface (`run_agent`, `submit_approval`, `cancel_run`, …) |

## Tools exposed to the LLM

Read-only: `list_events`, `list_dsl_functions`, `list_templates`, `get_event_data`

Write: `create_event_definitions`, `add_transaction_types`,
`generate_sample_event_data`, `validate_dsl`, `create_or_replace_template`,
`dry_run_template`

Destructive (require user approval): `delete_template`, `clear_all_data`

Terminal: `finish`

## Guardrails

1. **No custom code escape hatch.** Tools reject any DSL that contains
   `customCode:`, `__import__`, `eval`, `exec`, `subprocess`, `os.system`, or
   `open(`. The existing `_validate_dsl_user_code` and `_validate_template_ast`
   in `server.py` provide a second AST-level layer.
2. **Hard step cap.** `max_steps=25` by default; configurable per request.
3. **Approval gate.** Destructive tools emit `tool_pending` and block until the
   user clicks Approve via `POST /api/agent/runs/{run_id}/approve`.
4. **Truncated observations.** Tool results larger than 6000 chars are
   truncated before being fed back into the LLM context.
5. **Persisted runs.** Every run is saved to `db.agent_runs` (or in-memory
   fallback) with full event history.
6. **Cancellation.** `POST /api/agent/runs/{run_id}/cancel` aborts mid-loop.
7. **Auto-debug.** Tool errors are returned to the LLM as structured
   observations so the next turn can fix the problem instead of repeating it.

## HTTP endpoints

```
POST /api/agent/run                     SSE stream of run events
POST /api/agent/runs/{id}/approve?call_id=…  Approve / deny destructive tool
POST /api/agent/runs/{id}/cancel        Cancel a running agent
GET  /api/agent/runs                    List recent runs
GET  /api/agent/runs/{id}               Fetch full run history
GET  /api/agent/destructive-tools       Returns the list of guarded tools
```

## Provider support

Tool-calling is implemented for **OpenAI** (and OpenAI-compatible **DeepSeek**)
and **Anthropic Claude**. Gemini falls back to `NotImplementedError`.

Recommended cheap setup: OpenAI `gpt-4o-mini` as primary,
DeepSeek `deepseek-chat` as backup.

## Sanity test

```
python tests/test_agent_runtime.py
```

Replays a 6-turn scripted LLM session and verifies tool dispatch, error
feedback, and final completion.
