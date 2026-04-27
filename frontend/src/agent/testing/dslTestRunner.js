/**
 * dslTestRunner.js — Orchestrates testing all 145 DSL functions.
 *
 * Runs each test case via POST /api/dsl/run (the real execution endpoint).
 * Populates the explanationStore with results.
 * Returns a structured report for developer consumption.
 *
 * Designed to run at startup (background, non-blocking).
 */

import { API } from '../../config';
import SAMPLE_DATA from './sampleData';
import { populateFromResults, getStats } from './explanationStore';
import { translateError } from './translateError';

/**
 * @typedef {Object} TestResult
 * @property {string}  name     - Function name
 * @property {string}  category - Category
 * @property {boolean} passed   - Whether execution succeeded
 * @property {string|null}  output  - Captured print output
 * @property {string|null}  error   - Error message if failed
 * @property {object|null}  translatedError - User-friendly error if failed
 * @property {number}  durationMs - Execution time in ms
 */

/**
 * Run a single DSL test case.
 * @param {{ name: string, dsl: string, category: string }} testCase
 * @returns {Promise<TestResult>}
 */
async function runSingleTest(testCase) {
  const start = performance.now();
  try {
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: testCase.dsl }),
    });

    const durationMs = Math.round(performance.now() - start);

    if (!response.ok) {
      const text = await response.text().catch(() => `HTTP ${response.status}`);
      const translated = translateError(text);
      return {
        name: testCase.name,
        category: testCase.category,
        passed: false,
        output: null,
        error: text.slice(0, 500),
        translatedError: translated,
        durationMs,
      };
    }

    const data = await response.json();
    // The /dsl/run endpoint returns { success, print_outputs: [...], transactions, ... }
    const printOutputs = Array.isArray(data.print_outputs) ? data.print_outputs : [];
    const output = printOutputs.length > 0 ? printOutputs.join('\n') : '';

    if (data.success === false) {
      const errMsg = data.error || data.error_message || 'Unknown error';
      return {
        name: testCase.name,
        category: testCase.category,
        passed: false,
        output: typeof output === 'string' ? output : JSON.stringify(output),
        error: errMsg,
        translatedError: translateError(errMsg),
        durationMs,
      };
    }

    return {
      name: testCase.name,
      category: testCase.category,
      passed: true,
      output: typeof output === 'string' ? output.trim() : JSON.stringify(output),
      error: null,
      translatedError: null,
      durationMs,
    };
  } catch (err) {
    const durationMs = Math.round(performance.now() - start);
    return {
      name: testCase.name,
      category: testCase.category,
      passed: false,
      output: null,
      error: err.message || 'Network error',
      translatedError: translateError(err.message),
      durationMs,
    };
  }
}

/**
 * Check if the backend is reachable.
 * @returns {Promise<boolean>}
 */
async function isBackendReady() {
  try {
    const res = await fetch(`${API}/dsl-functions`, { method: 'GET' });
    return res.ok;
  } catch {
    return false;
  }
}

/**
 * Run all DSL function tests sequentially.
 * Skips event-dependent functions (collect_*) unless events are loaded.
 *
 * @param {object} opts
 * @param {object[]} opts.dslFunctions - Metadata array from GET /api/dsl-functions
 * @param {function} [opts.onProgress] - Callback (completed, total, currentName)
 * @param {boolean} [opts.skipEventFunctions=true] - Skip collect_* functions
 * @returns {Promise<{ results: TestResult[], report: object }>}
 */
export async function runAllTests(opts = {}) {
  const {
    dslFunctions = [],
    onProgress,
    skipEventFunctions = true,
  } = opts;

  // Wait for backend
  const ready = await isBackendReady();
  if (!ready) {
    console.warn('[DSL Test Runner] Backend not reachable — skipping tests');
    // Still populate store with metadata (no test results)
    populateFromResults(dslFunctions, [], SAMPLE_DATA);
    return { results: [], report: { skipped: true, reason: 'Backend not reachable' } };
  }

  const testCases = skipEventFunctions
    ? SAMPLE_DATA.filter((t) => !t.requiresEvents)
    : SAMPLE_DATA;

  const results = [];
  const total = testCases.length;

  for (let i = 0; i < testCases.length; i++) {
    const tc = testCases[i];
    if (onProgress) onProgress(i, total, tc.name);

    const result = await runSingleTest(tc);
    results.push(result);

    // Small yield to avoid blocking UI
    if (i % 10 === 9) {
      await new Promise((r) => setTimeout(r, 0));
    }
  }

  if (onProgress) onProgress(total, total, 'done');

  // Populate explanation store
  populateFromResults(dslFunctions, results, SAMPLE_DATA);

  // Build report
  const passed = results.filter((r) => r.passed);
  const failed = results.filter((r) => !r.passed);
  const skippedEvents = SAMPLE_DATA.filter((t) => t.requiresEvents);
  const totalDuration = results.reduce((s, r) => s + r.durationMs, 0);

  const report = {
    total: results.length,
    passed: passed.length,
    failed: failed.length,
    skippedEventFunctions: skippedEvents.length,
    durationMs: totalDuration,
    failures: failed.map((f) => ({
      name: f.name,
      category: f.category,
      error: f.error,
      translated: f.translatedError,
    })),
    timestamp: new Date().toISOString(),
  };

  // Log summary
  const stats = getStats();
  console.log(
    `[DSL Test Runner] ✓ ${report.passed}/${report.total} passed` +
    (report.failed > 0 ? ` · ✗ ${report.failed} failed` : '') +
    ` · ${report.skippedEventFunctions} skipped (event-dependent)` +
    ` · ${report.durationMs}ms` +
    ` · Store: ${stats.total} explanations loaded`
  );

  if (failed.length > 0) {
    console.group('[DSL Test Runner] Failed tests:');
    for (const f of failed) {
      console.warn(`  ✗ ${f.name} (${f.category}): ${f.error}`);
    }
    console.groupEnd();
  }

  return { results, report };
}

export default runAllTests;
