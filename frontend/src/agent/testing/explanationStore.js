/**
 * explanationStore.js — In-memory store of chatbot-ready function explanations.
 *
 * Populated by dslTestRunner after testing each function. Keyed by lowercase function name.
 * Used by ChatAssistant to provide instant explanations without an API call.
 */

/** @type {Map<string, ExplanationEntry>} */
const store = new Map();

// ── UI concept store (separate, no DSL example needed) ────────────────────
// Keyed by lowercase concept phrase. Used by detectConceptMention to give
// instant answers for questions about UI surfaces (Rule Builder, Schedule
// Builder, Saved Rules, Templates, Live Preview, etc.) without an AI call.
/** @type {Map<string, { title: string, body: string, aliases?: string[] }>} */
const conceptStore = new Map();

const UI_CONCEPTS = [
  {
    key: 'rule builder',
    aliases: ['accounting rule builder', 'rulebuilder'],
    title: 'Accounting Rule Builder',
    body: 'A visual editor where you compose a rule step-by-step: Parameters, Schedule, Iteration, Conditional, Custom Code, and Journal Entry. Each saved rule is mirrored to DSL behind the scenes and shows up under **Saved Rules**. Use it when you want to model an accounting policy without writing DSL by hand.',
  },
  {
    key: 'schedule builder',
    aliases: ['schedulebuilder'],
    title: 'Schedule Builder',
    body: 'A visual editor for time-based tables (amortization, depreciation, revenue recognition). You define a `period(...)` (start, end, frequency, day-count) and column expressions; the result is a `schedule(...)` call you can save and reuse. Lag references between rows use `lag(\'column\', offset, default)`.',
  },
  {
    key: 'custom code',
    aliases: ['custom code step', 'customcode'],
    title: 'Custom Code Step',
    body: 'An inline DSL snippet that lives inside a rule. Use it when a step needs logic that the visual builder does not cover. The snippet runs in the same context as the rest of the rule and can read variables produced by earlier steps.',
  },
  {
    key: 'live preview',
    aliases: ['preview pane', 'preview'],
    title: 'Live Preview',
    body: 'Shows the most recent execution result: the transactions that would be created and any `print(...)` outputs. It re-renders after every Run. Use it to verify a rule before deploying it as a template.',
  },
  {
    key: 'saved rules',
    aliases: ['rule manager', 'savedrules'],
    title: 'Saved Rules',
    body: 'The Rule Manager — your library of saved rules, schedules, and user templates. From here you can edit, duplicate, reorder, deploy, and clear rules. Deploying a user template mirrors it into the DSL template artifacts so it can run end-to-end.',
  },
  {
    key: 'template wizard',
    aliases: ['template library', 'templates', 'accounting templates'],
    title: 'Template Wizard / Accounting Templates',
    body: 'Built-in starter templates for ASC 310 (loan amortization), ASC 360 (depreciation), ASC 606 (revenue recognition), ASC 842 (leases), FAS-91 (origination fees), and IFRS-9 (impairment). Each template loads as a multi-step rule you can customize, save, and deploy.',
  },
  {
    key: 'event data viewer',
    aliases: ['eventdataviewer', 'event viewer'],
    title: 'Event Data Viewer',
    body: 'Inspector for the event data currently loaded in memory: rows per event, fields, posting dates, instrument IDs. Use it to confirm what your rule will see before running it.',
  },
  {
    key: 'ai agent setup',
    aliases: ['ai setup', 'ai provider', 'aiagentsetup'],
    title: 'AI Agent Setup',
    body: 'Configure the AI provider (OpenAI, Anthropic, Gemini, DeepSeek), test the API key, choose a model, and save it. The configured provider powers both this chat assistant and the AI Rule Generator inside the Rule Builder.',
  },
  {
    key: 'ai rule generator',
    aliases: ['ai rule translator', 'airuletranslator'],
    title: 'AI Rule Generator',
    body: 'Inside the Rule Builder, lets you describe a calculation in plain English and get DSL code back. The generated code is shown in a preview before you load it into the editor.',
  },
  {
    key: 'deploy template',
    aliases: ['deploy', 'deploy user template'],
    title: 'Deploying a Template',
    body: 'Copies a user template into the DSL template artifacts collection so it becomes runnable end-to-end (rocket button). The deploy step also runs the topo-sort so dependencies execute before dependents.',
  },
];

for (const c of UI_CONCEPTS) {
  conceptStore.set(c.key, c);
  if (c.aliases) for (const a of c.aliases) conceptStore.set(a.toLowerCase(), c);
}

/**
 * @typedef {Object} ExplanationEntry
 * @property {string} name          - Function name
 * @property {string} category      - Category (e.g. "Financial")
 * @property {string} params        - Parameter signature
 * @property {string} description   - Short description from metadata
 * @property {string} dslExample    - DSL code example that was tested
 * @property {string} sampleOutput  - Actual output from running the test (or null)
 * @property {boolean} tested       - Whether the function was successfully tested
 * @property {string|null} error    - Error message if test failed
 */

/**
 * Store an explanation entry.
 * @param {ExplanationEntry} entry
 */
export function setExplanation(entry) {
  if (!entry || !entry.name) return;
  store.set(entry.name.toLowerCase(), entry);
}

/**
 * Get an explanation by function name (case-insensitive).
 * @param {string} name
 * @returns {ExplanationEntry|null}
 */
export function getExplanation(name) {
  if (!name) return null;
  return store.get(name.toLowerCase()) || null;
}

/**
 * Check if a user message mentions a known DSL function.
 * Returns the first matched function name, or null.
 * @param {string} message
 * @returns {string|null}
 */
export function detectFunctionMention(message) {
  if (!message || store.size === 0) return null;
  const lower = message.toLowerCase();
  // Check for exact function name mentions (word boundaries)
  for (const [key] of store) {
    // Match function name as a whole word (not part of a larger word)
    const regex = new RegExp(`\\b${escapeRegex(key)}\\b`);
    if (regex.test(lower)) return key;
  }
  return null;
}

/**
 * Check if a user message mentions a known UI concept (Rule Builder, Saved
 * Rules, Live Preview, etc.). Returns the matched concept key or null.
 * @param {string} message
 * @returns {string|null}
 */
export function detectConceptMention(message) {
  if (!message) return null;
  const lower = message.toLowerCase();
  // Sort longer keys first so "accounting rule builder" wins over "rule builder"
  const keys = Array.from(conceptStore.keys()).sort((a, b) => b.length - a.length);
  for (const key of keys) {
    const regex = new RegExp(`\\b${escapeRegex(key)}\\b`);
    if (regex.test(lower)) return key;
  }
  return null;
}

/**
 * Get a UI concept entry by key (case-insensitive).
 * @param {string} key
 * @returns {{title:string, body:string}|null}
 */
export function getConcept(key) {
  if (!key) return null;
  return conceptStore.get(key.toLowerCase()) || null;
}

/**
 * Format a UI concept entry as chatbot-ready markdown.
 * @param {{title:string, body:string}} concept
 */
export function formatConceptForChat(concept) {
  if (!concept) return '';
  return `## ${concept.title}\n\n${concept.body}`;
}

/**
 * Format an explanation as chatbot-ready markdown.
 * @param {ExplanationEntry} entry
 * @returns {string}
 */
export function formatForChat(entry) {
  if (!entry) return '';
  const lines = [
    `## ${entry.name}()`,
    '',
    `**Category:** ${entry.category}`,
    `**Parameters:** \`${entry.params}\``,
    `**Description:** ${entry.description}`,
    '',
    '**Example:**',
    '```dsl',
    entry.dslExample,
    '```',
  ];
  if (entry.tested && entry.sampleOutput != null) {
    lines.push('', `**Output:** \`${entry.sampleOutput}\``);
  }
  if (entry.error) {
    lines.push('', `**Note:** This function returned an error in testing: ${entry.error}`);
  }
  return lines.join('\n');
}

/**
 * Populate the store from DSL metadata + test results.
 * Called by dslTestRunner after all tests complete.
 * @param {object[]} metadata - DSL_FUNCTION_METADATA array
 * @param {object[]} testResults - Array of { name, passed, output, error }
 * @param {object[]} sampleData - SAMPLE_DATA array
 */
export function populateFromResults(metadata, testResults, sampleData) {
  const resultMap = new Map();
  for (const r of testResults) {
    resultMap.set(r.name.toLowerCase(), r);
  }
  const sampleMap = new Map();
  for (const s of sampleData) {
    sampleMap.set(s.name.toLowerCase(), s);
  }

  for (const meta of metadata) {
    const key = meta.name.toLowerCase();
    const result = resultMap.get(key);
    const sample = sampleMap.get(key);
    setExplanation({
      name: meta.name,
      category: meta.category,
      params: meta.params,
      description: meta.description,
      dslExample: sample?.dsl || `print(${meta.name}(...))`,
      sampleOutput: result?.output ?? null,
      tested: result?.passed ?? false,
      error: result?.error ?? null,
    });
  }
}

/**
 * Get summary stats of the store.
 * @returns {{ total: number, tested: number, passed: number, failed: number }}
 */
export function getStats() {
  let total = 0, tested = 0, passed = 0, failed = 0;
  for (const [, entry] of store) {
    total++;
    if (entry.tested) { tested++; passed++; }
    else if (entry.error) { tested++; failed++; }
  }
  return { total, tested, passed, failed };
}

/**
 * Return the full store as a plain array (for debugging / reporting).
 * @returns {ExplanationEntry[]}
 */
export function getAllExplanations() {
  return Array.from(store.values());
}

/**
 * Clear the store (for testing).
 */
export function clearStore() {
  store.clear();
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export default {
  setExplanation,
  getExplanation,
  detectFunctionMention,
  formatForChat,
  populateFromResults,
  getStats,
  getAllExplanations,
  clearStore,
};
