/**
 * Execution Plan Builder — classifies intent and builds concrete plan steps.
 * All local computation — no AI calls. Must be fast (<5ms).
 */

// ── Intent keywords ──
const INTENT_PATTERNS = {
  generate_dsl: [
    'create', 'generate', 'write', 'add', 'build', 'make', 'compose',
    'calculate', 'compute', 'formula', 'rule', 'validation', 'schedule',
    'amortization', 'interest', 'payment', 'transaction', 'accrue',
  ],
  debug: [
    'error', 'fix', 'debug', 'wrong', 'broken', 'fail', 'issue', 'bug',
    'not working', 'doesn\'t work', 'problem', 'crash', 'exception',
  ],
  explain: [
    'explain', 'what', 'how', 'why', 'describe', 'tell me', 'show me',
    'difference', 'meaning', 'purpose', 'help', 'understand', 'documentation',
  ],
  modify: [
    'change', 'update', 'modify', 'edit', 'refactor', 'replace', 'remove',
    'delete', 'rename', 'move', 'adjust', 'tweak',
  ],
};

/**
 * Classify the user's intent from their message.
 * Returns { type, thinkingText }
 */
export function classifyIntent(userMessage) {
  const lower = userMessage.toLowerCase();

  let bestType = 'general';
  let bestScore = 0;

  for (const [type, keywords] of Object.entries(INTENT_PATTERNS)) {
    let score = 0;
    for (const kw of keywords) {
      if (lower.includes(kw)) score++;
    }
    if (score > bestScore) {
      bestScore = score;
      bestType = type;
    }
  }

  const thinkingTexts = {
    generate_dsl: 'Reading your request — identifying required DSL functions and structure',
    debug: 'Analyzing the issue — checking for errors and tracing the cause',
    explain: 'Understanding your question — finding relevant context and documentation',
    modify: 'Reading your request — planning modifications to existing code',
    general: 'Reading your request — preparing a helpful response',
  };

  return {
    type: bestType,
    thinkingText: thinkingTexts[bestType] || thinkingTexts.general,
  };
}

/**
 * Build concrete execution plan steps from intent and environment.
 * Each step: { id, label, detail }
 */
export function buildPlanSteps(intent, env = {}) {
  const { editorLines = 0, errorCount = 0, matchedFuncs = [] } = env;
  const steps = [];

  // Always: read editor if there's content
  if (editorLines > 0) {
    steps.push({
      id: 'read_editor',
      label: 'Read current rule structure from editor',
      detail: `${editorLines} lines analyzed`,
    });
  }

  // If errors exist, always check them
  if (errorCount > 0) {
    steps.push({
      id: 'check_errors',
      label: `Investigate ${errorCount} active console error(s)`,
      detail: `${errorCount} error(s) reviewed`,
    });
  }

  // Intent-specific steps
  switch (intent.type) {
    case 'generate_dsl':
      if (matchedFuncs.length > 0) {
        steps.push({
          id: 'match_functions',
          label: 'Match request to DSL functions',
          detail: matchedFuncs.slice(0, 4).join(', '),
        });
      }
      if (editorLines > 0) {
        steps.push({
          id: 'find_insertion',
          label: 'Identify insertion point in current rule',
          detail: 'Located',
        });
      }
      steps.push({
        id: 'compose_dsl',
        label: 'Compose DSL code',
        detail: 'Generating with AI...',
      });
      break;

    case 'debug':
      steps.push({
        id: 'trace_error',
        label: 'Trace error to its origin',
        detail: 'Traced',
      });
      steps.push({
        id: 'identify_fix',
        label: 'Identify the fix',
        detail: 'Analyzing...',
      });
      break;

    case 'modify':
      steps.push({
        id: 'locate_target',
        label: 'Locate code to modify',
        detail: 'Found',
      });
      steps.push({
        id: 'plan_changes',
        label: 'Plan modifications',
        detail: 'Ready',
      });
      break;

    case 'explain':
      steps.push({
        id: 'gather_context',
        label: 'Gather relevant context and documentation',
        detail: 'Collected',
      });
      break;

    default:
      steps.push({
        id: 'prepare',
        label: 'Prepare response context',
        detail: 'Ready',
      });
  }

  // Always end with "Generate response"
  steps.push({
    id: 'generate',
    label: 'Stream response from AI',
    detail: 'Complete',
  });

  return steps;
}
