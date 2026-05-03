/**
 * Agent Pipeline — full cycle: thinking → reading → planning → executing → streaming reply.
 * Emits events to agentEventBus which AgentMessage subscribes to and renders.
 *
 * Two flows are exported:
 *   • `runAgentPipeline`  — teaching/explanation flow (calls /chat/stream).
 *   • `runAgentBuildSSE`  — autonomous build flow (calls /api/agent/run SSE
 *     and surfaces real tool-call events, including step_id / persisted /
 *     mismatches so the UI can show a per-step audit trail). H16 + H17.
 */

import { agentEventBus } from './agentEventBus';
import { classifyIntent, buildPlanSteps } from './buildExecutionPlan';
import { API } from '../config';

let _idCounter = 0;
export function generateMessageId() {
  return `agent-msg-${Date.now()}-${++_idCounter}`;
}

/**
 * Run the agent pipeline for a user message.
 * Returns the messageId (caller mounts <AgentMessage messageId={id} />).
 *
 * @param {string} userMessage
 * @param {object} opts
 * @param {string[]} opts.events        - event definitions
 * @param {string}   opts.editorCode    - current editor content
 * @param {object[]} opts.consoleOutput - console log entries
 * @param {object[]} opts.dslFunctions  - available DSL functions (used locally for hints; not sent to server)
 * @param {object}   opts.uiContext     - { mode, editingRule, editingSchedule, editingCustomCode, activeTemplate, lastExecutionSummary }
 * @param {string}   opts.selectedModel - model id
 * @param {string}   opts.sessionId     - chat session id
 * @param {object[]} opts.history       - conversation history
 * @returns {Promise<{ messageId: string, fullText: string, sessionId: string }>}
 */
export async function runAgentPipeline(userMessage, opts = {}) {
  const msgId = opts.messageId || generateMessageId();
  const {
    events = [],
    editorCode = '',
    consoleOutput = [],
    dslFunctions = [],
    editorRef,
    monacoRef,
    selectedModel,
    sessionId,
    history = [],
    uiContext = null,
  } = opts;

  try {
    // ── STAGE 1: THINKING ──────────────────────────────────────
    // Instant — no AI call. Local intent classification only.
    const intent = classifyIntent(userMessage);
    agentEventBus.thinking(msgId, intent.thinkingText);

    // Small delay so the thinking block renders before reading steps
    await tick(30);

    // ── STAGE 2: READING ───────────────────────────────────────
    // Capture real editor state from Monaco ref
    let editorCursor = null;
    let editorSelection = null;
    let editorSyntaxErrors = null;

    const editor = editorRef?.current;
    const monaco = monacoRef?.current;
    if (editor) {
      try {
        const pos = editor.getPosition();
        if (pos) editorCursor = { line: pos.lineNumber, column: pos.column };

        const sel = editor.getSelection();
        if (sel && !sel.isEmpty()) {
          editorSelection = editor.getModel()?.getValueInRange(sel) || null;
        }

        if (monaco && editor.getModel()) {
          const markers = monaco.editor.getModelMarkers({ resource: editor.getModel().uri });
          if (markers && markers.length > 0) {
            editorSyntaxErrors = markers
              .filter(m => m.severity >= 8) // Error severity
              .slice(0, 10)
              .map(m => ({
                startLineNumber: m.startLineNumber,
                message: m.message,
              }));
          }
        }
      } catch (e) {
        // editor not ready — ignore
      }
    }

    // Show reads as they resolve
    const editorLines = editorCode.trim() ? editorCode.split('\n').length : 0;
    const errorCount = (consoleOutput || []).filter(
      (l) => l.type === 'error' || l.type === 'stderr'
    ).length;
    const eventsCount = (events || []).length;

    agentEventBus.readStep(msgId, '◧', 'Reading editor...',
      editorLines > 0
        ? `${editorLines} lines` + (editorCursor ? ` · cursor L${editorCursor.line}` : '') + (editorSelection ? ' · has selection' : '') + (editorSyntaxErrors?.length ? ` · ${editorSyntaxErrors.length} error(s) ✗` : ' · 0 errors ✓')
        : 'Empty editor'
    );
    await tick(60);

    agentEventBus.readStep(msgId, '⊡', 'Checking console...',
      errorCount === 0 ? 'Clean ✓' : `${errorCount} active error(s) ✗`
    );
    await tick(60);

    const funcCount = (dslFunctions || []).length;
    const matchedFuncs = findRelevantFunctions(userMessage, dslFunctions);
    agentEventBus.readStep(msgId, '⊞', 'Scanning DSL registry...',
      matchedFuncs.length > 0
        ? matchedFuncs.slice(0, 5).join(', ') + ' ✓'
        : `${funcCount} functions loaded ✓`
    );
    await tick(60);

    agentEventBus.readStep(msgId, '◎', 'Loading context...',
      `${eventsCount} event(s) · ${history.length} prior message(s)` + (errorCount > 0 ? ` · ${errorCount} console error(s)` : '') + ' ✓'
    );
    await tick(40);

    // ── STAGE 3: PLANNING ──────────────────────────────────────
    const planSteps = buildPlanSteps(intent, { editorLines, errorCount, matchedFuncs });
    agentEventBus.planReady(msgId, planSteps.map((s) => s.label));

    await tick(60);

    // ── STAGE 4: EXECUTING ─────────────────────────────────────
    for (let i = 0; i < planSteps.length; i++) {
      const step = planSteps[i];
      agentEventBus.execStepStart(msgId, step.id, step.label);
      await tick(80 + Math.random() * 120);
      agentEventBus.execStepDone(msgId, step.id, step.detail || 'Done');
    }

    await tick(40);

    // ── STAGE 5: STREAMING REPLY ───────────────────────────────
    // Call the SSE streaming endpoint

    const context = {
      events: events || [],
      editor_code: editorCode || '',
      editor_cursor: editorCursor,
      editor_selection: editorSelection,
      editor_syntax_errors: editorSyntaxErrors,
      console_output: consoleOutput || [],
      ui_mode: uiContext || undefined,
    };

    const body = {
      message: userMessage,
      session_id: sessionId || undefined,
      context,
      model: selectedModel || undefined,
      history: history.slice(-10).map((m) => ({ role: m.role, content: m.content })),
    };

    const response = await fetch(`${API}/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let fullText = '';
    let newSessionId = sessionId;
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      // Keep the last potentially incomplete line in buffer
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const payload = line.slice(6).trim();
        if (payload === '[DONE]') continue;

        let data;
        try {
          data = JSON.parse(payload);
        } catch {
          continue;
        }

        switch (data.type) {
          case 'session':
            newSessionId = data.session_id;
            break;
          case 'token':
            fullText += data.token;
            agentEventBus.replyToken(msgId, data.token);
            break;
          case 'error':
            agentEventBus.error(msgId, data.error_message || data.error_type);
            return { messageId: msgId, fullText: '', sessionId: newSessionId, error: data };
          case 'done':
            break;
          default:
            break;
        }
      }
    }

    // Extract DSL code from completed response.
    // The AI may return: ```dsl\n...```, ```json\n{...dsl_code:...}```, or ```\n...```
    let dslCode = null;

    // Try: JSON block with dsl_code field
    const jsonMatch = fullText.match(/```(?:json)?\s*\n([\s\S]*?)```/);
    if (jsonMatch) {
      try {
        const parsed = JSON.parse(jsonMatch[1]);
        if (parsed.dsl_code) {
          dslCode = parsed.dsl_code.replace(/\\n/g, '\n');
        }
      } catch {
        // Not JSON — fall through
      }
    }

    // Try: ```dsl block
    if (!dslCode) {
      const dslMatch = fullText.match(/```dsl\s*\n([\s\S]*?)```/);
      if (dslMatch) dslCode = dslMatch[1].trim();
    }

    // Try: any code block
    if (!dslCode) {
      const anyBlock = fullText.match(/```\w*\s*\n([\s\S]*?)```/);
      if (anyBlock) dslCode = anyBlock[1].trim();
    }

    if (dslCode) {
      agentEventBus.dslReady(msgId, dslCode, 'append');
    }

    agentEventBus.complete(msgId);
    // Clean up event history to prevent memory leak
    setTimeout(() => agentEventBus.cleanup(msgId), 5000);
    return { messageId: msgId, fullText, sessionId: newSessionId };

  } catch (err) {
    agentEventBus.error(msgId, err.message || 'An unexpected error occurred.');
    setTimeout(() => agentEventBus.cleanup(msgId), 5000);
    return { messageId: msgId, fullText: '', sessionId: opts.sessionId, error: err };
  }
}

// ── Helpers ──

function tick(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function findRelevantFunctions(message, dslFunctions) {
  if (!dslFunctions || dslFunctions.length === 0) return [];
  const words = message.toLowerCase().split(/\s+/);
  const matched = [];
  for (const fn of dslFunctions) {
    const name = (fn.name || fn || '').toString().toLowerCase();
    const desc = (fn.description || '').toLowerCase();
    for (const w of words) {
      if (w.length > 3 && (name.includes(w) || desc.includes(w))) {
        matched.push(fn.name || fn);
        break;
      }
    }
  }
  return matched;
}

// ──────────────────────────────────────────────────────────────────────
// H16 + H17: real autonomous-build flow over /api/agent/run SSE.
// Surfaces every backend tool-call event so the chat UI can show:
//   • which step_id was created / mutated
//   • whether the read-back verification passed (`persisted.ok`)
//   • the exact mismatches[] when a deep-merge or patch silently no-op'd
// ──────────────────────────────────────────────────────────────────────
export async function runAgentBuildSSE(userMessage, opts = {}) {
  const msgId = opts.messageId || generateMessageId();
  const {
    selectedModel,
    sessionId,
    maxSteps = 50,
    autoApproveDestructive = false,
    onEvent,           // optional raw-event callback for callers that want full payload
  } = opts;

  agentEventBus.thinking(msgId, 'Starting autonomous agent run…');

  const body = {
    task: userMessage,
    session_id: sessionId || undefined,
    model: selectedModel || undefined,
    max_steps: maxSteps,
    auto_approve_destructive: !!autoApproveDestructive,
  };

  let response;
  try {
    response = await fetch(`${API}/agent/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (e) {
    agentEventBus.error(msgId, `Failed to reach agent endpoint: ${e.message}`);
    setTimeout(() => agentEventBus.cleanup(msgId), 5000);
    return { messageId: msgId, fullText: '', sessionId, error: e };
  }

  if (!response.ok || !response.body) {
    const err = `HTTP ${response?.status} from /agent/run`;
    agentEventBus.error(msgId, err);
    setTimeout(() => agentEventBus.cleanup(msgId), 5000);
    return { messageId: msgId, fullText: '', sessionId, error: new Error(err) };
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let fullText = '';
  let runId = null;
  let finalSummary = '';

  // Map call_id → {name, step_id, ok} so tool_done can correlate.
  const toolCallMeta = new Map();

  const shortId = (s) => (s || '').slice(0, 8);

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const raw of lines) {
      if (!raw.startsWith('data: ')) continue;
      const payload = raw.slice(6).trim();
      if (!payload || payload === '[DONE]') continue;
      let evt;
      try { evt = JSON.parse(payload); } catch { continue; }
      if (typeof onEvent === 'function') {
        try { onEvent(evt); } catch { /* swallow */ }
      }

      switch (evt.type) {
        case 'run_started':
          runId = evt.run_id || null;
          break;
        case 'thinking':
          agentEventBus.thinking(msgId, `Step ${evt.step}: thinking…`);
          break;
        case 'calling_model':
          agentEventBus.readStep(msgId, '◎', `Calling ${evt.model}…`,
            evt.message || `step ${evt.step}`);
          break;
        case 'heartbeat':
          // optional: reflect long waits in UI
          break;
        case 'assistant_message': {
          const txt = (evt.content || '').trim();
          if (txt) {
            fullText += txt + '\n';
            agentEventBus.replyToken(msgId, txt + '\n');
          }
          break;
        }
        case 'tool_start': {
          toolCallMeta.set(evt.call_id, { name: evt.name, step_id: null, ok: null });
          agentEventBus.execStepStart(msgId, evt.call_id,
            `→ ${evt.name}` + (evt.args && evt.args.step_id
              ? ` (step ${shortId(evt.args.step_id)})`
              : evt.args && evt.args.step_name
                ? ` (${evt.args.step_name})`
                : ''));
          break;
        }
        case 'tool_done': {
          const meta = toolCallMeta.get(evt.call_id) || { name: evt.name };
          const r = evt.result || {};
          // Pull step_id and persistence verification from the standard
          // shape returned by update_step / patch_step / add_step_to_rule.
          const stepId = r.step_id
            || (r.step && r.step.id)
            || (r.persisted && r.persisted.step_id);
          const persisted = r.persisted || (r.verification ? { ok: r.verification.ok, mismatches: r.verification.mismatches } : null);
          let detail = `✓ ${evt.duration_ms || 0} ms`;
          if (stepId) detail += ` · step ${shortId(stepId)}`;
          if (persisted && persisted.ok === false) {
            detail += ` · ⚠ NOT persisted (${(persisted.mismatches || []).length} mismatches)`;
          } else if (persisted && persisted.ok === true) {
            detail += ' · ✓ persisted';
          }
          agentEventBus.execStepDone(msgId, evt.call_id, detail);
          // If a write tool reports persisted=false, emit a visible warning
          // line so the user sees the silent-update class of failure that
          // motivated this whole hardening pass.
          if (persisted && persisted.ok === false) {
            agentEventBus.error(
              msgId,
              `Tool ${meta.name || evt.name} reported a successful return but ` +
              `the read-back check failed. Mismatches: ` +
              JSON.stringify(persisted.mismatches || []).slice(0, 400)
            );
          }
          break;
        }
        case 'tool_error': {
          agentEventBus.execStepDone(msgId, evt.call_id,
            `✗ ${evt.name}: ${(evt.error || '').slice(0, 200)}`);
          break;
        }
        case 'warning':
          agentEventBus.error(msgId, evt.message || 'warning');
          break;
        case 'final': {
          finalSummary = evt.summary || '';
          if (finalSummary) {
            fullText += '\n' + finalSummary;
            agentEventBus.replyToken(msgId, '\n' + finalSummary);
          }
          break;
        }
        case 'error':
          agentEventBus.error(msgId, evt.error_message || evt.message || 'agent error');
          break;
        default:
          break;
      }
    }
  }

  agentEventBus.complete(msgId);
  setTimeout(() => agentEventBus.cleanup(msgId), 8000);
  return { messageId: msgId, fullText: fullText.trim(), sessionId, runId, summary: finalSummary };
}
