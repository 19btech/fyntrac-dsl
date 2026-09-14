# Drive the Fyntrac agent from your personal Claude (MCP)

`backend/mcp_server.py` exposes this app's autonomous agent over the **Model
Context Protocol (MCP)**, so you can ask **Claude Desktop** (or a claude.ai
custom connector) to build rules/steps/schedules in the app for you.

It reuses the app's **stored provider + API key** and connects to the **same
MongoDB** as the running app — so it works on your real workspace.

> **Scope:** this is a personal/developer convenience — *you* using *your* Claude
> to drive *your* dev instance. It is **not** a way to ship Claude to end users
> (that still needs the API-key/product path). Never point a personal connector
> at production bank data.

---

## What Claude gets — two ways to build (hybrid)

**1. Low-level builder tools (Claude builds directly).** A curated slice of the
app's real tools — `create_event_definitions`, `create_saved_rule`,
`add_step_to_rule`, `debug_step`, `dry_run_rule`, `add_transaction_to_rule`,
`verify_rule_complete`, plus all the read/discovery tools (`list_events`,
`list_saved_rules`, `get_saved_rule`, `get_dsl_syntax_guide`, …). These are
plain CRUD + validation with **no LLM inside**, so **your Claude client drives
the build with its own reasoning** — nothing runs on the app's configured AI
provider, so the app's provider quota (e.g. Gemini) can never block a build.
The tools' own write-time validation still applies (unknown functions rejected,
no debit/credit sides, persistence verified). ~52 tools — build/test/verify,
Excel-workbook import (list_workbooks, get_sheet_formulas,
reconcile_workbook_outputs, …), requirements-document reading
(list_requirement_documents, read_requirement_document), and revert_rule for
undo. **Destructive tools (delete rule/template, clear data) are intentionally
excluded** — calling one returns a clear refusal, not an error.

**2. `run_agent_task(task, model?, allow_destructive?, max_steps?)` (delegated).**
Hands a plain-English request to the app's **own** `run_agent` runtime — full
plan → build → test → verify loop with every guardrail. This one **runs on the
app's configured provider**, so it *can* be blocked by that provider's quota.
Best for one-shot "build me an X" jobs.

So: use the **individual builder tools** to build step-by-step on your own
Claude capacity; use **`run_agent_task`** for a hands-off delegated build.

---

## Prerequisites

1. **Install the dependency** (into the same Python the app uses):
   ```bash
   pip install -r backend/requirements.txt      # or: pip install "mcp>=1.28,<2.0"
   ```
2. **Configure a provider in the app** at least once (Settings → AI Agent
   Setup). The MCP server reuses that stored, encrypted key — it holds no key
   of its own.
3. **MongoDB running** (the same instance the app uses), so the agent reads and
   writes your real workspace.

---

## Claude Desktop setup

Edit Claude Desktop's config file:

- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

Add a `fyntrac-dsl` server (Windows example — adjust paths to your machine):

```json
{
  "mcpServers": {
    "fyntrac-dsl": {
      "command": "C:\\Users\\raf19\\AppData\\Local\\Programs\\Python\\Python312\\python.exe",
      "args": ["C:\\Repos\\fyntrac-dsl\\backend\\mcp_server.py"],
      "env": {
        "MONGO_URL": "mongodb://localhost:27018",
        "DB_NAME": "dsl_db"
      }
    }
  }
}
```

> **`MONGO_URL` / `DB_NAME` must exactly match `backend/.env`.** This project's
> `startup.ps1` runs Mongo as the `dsl-mongo` container mapped to host port
> **27018** (`-p 27018:27017`), so the URL is `mongodb://localhost:27018` — not
> the Mongo default 27017. If these don't match, the tools connect but fail
> with "connection refused."

Notes:
- `command` must be the **same Python** where you installed the requirements
  (`python -c "import sys; print(sys.executable)"` prints it).
- Pass the **full path to `mcp_server.py`** (not `-m backend.mcp_server`). The
  script adds the repo to its own import path, so it works no matter which
  working directory Claude Desktop launches it from. Using `-m` fails with
  `No module named 'backend'` because some builds ignore a `cwd` setting.
- The encrypted-key file (`backend/.ai_secret_key`) is found automatically. If
  your app instead sets `AI_ENCRYPTION_KEY` via the environment, add it to
  the `env` block too, or the stored key can't be decrypted.

> **Microsoft Store / packaged build of Claude Desktop:** it does NOT read
> `%APPDATA%\Claude\claude_desktop_config.json`. Its real config is sandboxed
> under
> `%LOCALAPPDATA%\Packages\Claude_*\LocalCache\Roaming\Claude\claude_desktop_config.json`.
> Always use the Developer tab's **Edit Config** button — it opens the correct
> file — and merge `mcpServers` into whatever is already there.

Then **fully quit and reopen Claude Desktop**. The `fyntrac-dsl` tools appear
under the connectors/tools (🔌) menu.

---

## Using it

Just talk to Claude:

- *"Using fyntrac-dsl, list my rules, then build an IFRS 9 ECL model with
  stage 1/2/3 and sample data for 5 loans."*
- *"Add a straight-line depreciation schedule step to the PPE rule."*
- *"Show me the steps in rule &lt;id&gt; and explain what each one does."*

Claude will call `list_*` / `get_rule` to inspect, then `run_agent_task` to
build. Claude Desktop asks your approval before each tool call.

**Deletions are gated.** `run_agent_task` runs with `allow_destructive=false`
by default, so mid-run delete/clear steps are declined. Tell Claude to pass
`allow_destructive: true` only when you actually intend deletions.

---

## Troubleshooting

- **Tools don't appear** → the server failed to start. Check Claude Desktop's
  MCP logs; usually a wrong `command` path or `cwd`, or `mcp` not installed in
  that Python.
- **"No AI provider is configured"** → configure one in the app first.
- **"Couldn't reach the app's database"** → MongoDB isn't running, or
  `MONGO_URL`/`DB_NAME` don't match the app's.
- **"stored API key could not be read"** → the MCP process can't see the same
  encryption key; set `AI_ENCRYPTION_KEY` in the `env` block to match the app.
