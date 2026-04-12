/**
 * explanationStore.js — In-memory store of chatbot-ready function explanations.
 *
 * Populated by dslTestRunner after testing each function. Keyed by lowercase function name.
 * Used by ChatAssistant to provide instant explanations without an API call.
 */

/** @type {Map<string, ExplanationEntry>} */
const store = new Map();

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
