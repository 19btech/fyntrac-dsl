/**
 * Agent Pipeline — full cycle: thinking → reading → planning → executing → streaming reply.
 * Emits events to agentEventBus which AgentMessage subscribes to and renders.
 */

import { agentEventBus } from './agentEventBus';
import { classifyIntent, buildPlanSteps } from './buildExecutionPlan';

let _idCounter = 0;
export function generateMessageId() {
  return `agent-msg-${Date.now()}-${++_idCounter}`;
}

const API = '/api';

/**
 * Run the agent pipeline for a user message.
 * Returns the messageId (caller mounts <AgentMessage messageId={id} />).
 *
 * @param {string} userMessage
 * @param {object} opts
 * @param {string[]} opts.events        - event definitions
 * @param {string}   opts.editorCode    - current editor content
 * @param {object[]} opts.consoleOutput - console log entries
 * @param {object[]} opts.dslFunctions  - available DSL functions
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
    selectedModel,
    sessionId,
    history = [],
  } = opts;

  try {
    // ── STAGE 1: THINKING ──────────────────────────────────────
    // Instant — no AI call. Local intent classification only.
    const intent = classifyIntent(userMessage);
    agentEventBus.thinking(msgId, intent.thinkingText);

    // Small delay so the thinking block renders before reading steps
    await tick(30);

    // ── STAGE 2: READING ───────────────────────────────────────
    // Show reads as they resolve

    const editorLines = editorCode.trim() ? editorCode.split('\n').length : 0;
    const errorCount = (consoleOutput || []).filter(
      (l) => l.type === 'error' || l.type === 'stderr'
    ).length;
    const eventsCount = (events || []).length;

    agentEventBus.readStep(msgId, '◧', 'Reading editor...',
      editorLines > 0
        ? `${editorLines} lines · ${errorCount === 0 ? '0 errors ✓' : errorCount + ' error(s) ✗'}`
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
      `${eventsCount} event(s) · ${history.length} prior message(s) ✓`
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
      console_output: consoleOutput || [],
      dsl_functions: dslFunctions || [],
      ai_requirements: "IMPORTANT: Follow these code-generation rules for ALL DSL examples: use ## for inline comments (never //), do NOT create transactions or call createTransaction/createTransactions unless the user explicitly asks for them, compute required values and use print() to output the final variable when transactions are NOT requested, use only DSL functions supported by both frontend and backend, never output Python or other languages, and ensure code is syntactically valid and runnable. Wrap code in ```dsl blocks when providing examples.",
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
    return { messageId: msgId, fullText, sessionId: newSessionId };

  } catch (err) {
    agentEventBus.error(msgId, err.message || 'An unexpected error occurred.');
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
