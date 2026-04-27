/**
 * translateError.js — Translates raw DSL execution errors into plain English.
 *
 * Input:  raw error string from backend (e.g. "TypeError: unsupported operand type(s)")
 * Output: { whatWentWrong, whyItHappened, howToFix, category }
 *
 * Enhanced with accounting-context-aware patterns for finance professionals.
 */

// ── Accounting-context patterns (checked first for more specific messages) ──
const ACCOUNTING_ERROR_PATTERNS = [
  // Schedule column reference errors
  {
    pattern: /NameError.*name '(\w+)' is not defined/i,
    category: 'schedule_context',
    extract: (raw) => {
      const m = raw.match(/name '(\w+)' is not defined/i);
      return m ? m[1] : null;
    },
    build: (varName) => ({
      whatWentWrong: `The schedule is trying to use "${varName}" but this value hasn't been defined.`,
      whyItHappened: `The column formula references "${varName}", which isn't available. This could be: an event field not added to the schedule context, a column name that hasn't been defined yet, or a misspelled variable name.`,
      howToFix: `If "${varName}" is an event field, add it to the schedule's data context (the third argument). If it's another column, make sure it appears earlier in the column definitions. Check spelling against your event field names.`,
    }),
  },
  // createTransaction missing date
  {
    pattern: /createTransaction.*(?:posting.*(?:empty|missing|None|null)|effectivedate.*(?:empty|missing|None|null))/i,
    category: 'transaction_date',
    build: () => ({
      whatWentWrong: 'The transaction couldn\'t be created because a required date is missing.',
      whyItHappened: 'The posting date or effective date is empty. This usually means the event data doesn\'t have a row for this instrument on the selected posting date.',
      howToFix: 'Check your event data to confirm each instrument has a row for this posting date. Use the Event Data Viewer to inspect the loaded data.',
    }),
  },
  // NoneType in arithmetic (common when event field is missing)
  {
    pattern: /TypeError.*(?:unsupported operand|NoneType).*(?:float|int|subtract|multiply|divide|add)/i,
    category: 'missing_data',
    build: () => ({
      whatWentWrong: 'A calculation failed because one of the values is empty (missing data).',
      whyItHappened: 'One of the fields used in this formula returned no value. This typically happens when an event field has no data for this instrument or posting date, or when a previous calculation returned nothing.',
      howToFix: 'Check that all fields referenced in the formula have data. You can use coalesce() to provide fallback values, e.g. coalesce(MY_FIELD, 0) to default to zero when data is missing.',
    }),
  },
  // Schedule lag reference errors
  {
    pattern: /lag.*(?:not found|not defined|invalid|KeyError)/i,
    category: 'schedule_lag',
    build: () => ({
      whatWentWrong: 'A schedule formula tried to reference a previous row value that doesn\'t exist.',
      whyItHappened: 'The lag() function references a column name that doesn\'t match any defined column in the schedule. Column names in lag() must exactly match the keys in your column definitions.',
      howToFix: 'Double-check the column name in lag(\'column_name\', offset, default). It must match exactly, including capitalization. Also ensure you provide a default value for the first row.',
    }),
  },
  // Period date errors
  {
    pattern: /period.*(?:invalid|cannot parse|format|start.*after.*end)/i,
    category: 'period_date',
    build: () => ({
      whatWentWrong: 'The schedule period definition has invalid dates.',
      whyItHappened: 'Either the start date is after the end date, or one of the dates isn\'t in the correct format (YYYY-MM-DD).',
      howToFix: 'Verify that your start date comes before your end date and both are in YYYY-MM-DD format. If using event fields, check the data contains valid dates.',
    }),
  },
  // Division by zero in financial context
  {
    pattern: /(?:ZeroDivisionError|division by zero).*|divide.*(?:by zero|zero denominator)/i,
    category: 'financial_division',
    build: () => ({
      whatWentWrong: 'A calculation tried to divide by zero.',
      whyItHappened: 'A denominator in your formula evaluated to zero. In financial calculations, this commonly happens when: total SSP is zero (no standalone prices), term or period count is zero, or a balance has been fully paid down.',
      howToFix: 'Wrap the division with a zero check: if(gt(denominator, 0), divide(numerator, denominator), 0). For allocation calculations, ensure at least one item has a positive standalone value.',
    }),
  },
  // collect/collect_by_instrument errors
  {
    pattern: /collect.*(?:no data|empty|not found|no event)/i,
    category: 'event_data',
    build: () => ({
      whatWentWrong: 'The data collection function couldn\'t find any matching event data.',
      whyItHappened: 'Either no event data has been uploaded for this event type, or there are no rows matching the current instrument ID and posting date.',
      howToFix: 'Make sure you\'ve uploaded event data via the Upload Data tab. Use the Event Data Viewer to confirm data exists for the instruments and dates you expect.',
    }),
  },
  // Schedule returns None/empty
  {
    pattern: /schedule.*(?:returned None|empty|no periods|no rows)/i,
    category: 'empty_schedule',
    build: () => ({
      whatWentWrong: 'The schedule generated zero periods — it produced an empty table.',
      whyItHappened: 'The period definition resulted in no dates. This happens when the start and end dates are the same, or the frequency is larger than the date range (e.g., yearly frequency for a 3-month range).',
      howToFix: 'Verify your schedule\'s start/end dates span a long enough period for the chosen frequency. For monthly schedules, the range should be at least one month.',
    }),
  },
  // KeyError in schedule context
  {
    pattern: /KeyError.*'(\w+)'/i,
    category: 'key_error',
    extract: (raw) => {
      const m = raw.match(/KeyError.*'(\w+)'/i);
      return m ? m[1] : null;
    },
    build: (keyName) => ({
      whatWentWrong: `The field "${keyName}" was not found in the data.`,
      whyItHappened: `Your code references "${keyName}", but it doesn't exist in the current context. This could be: a column name in a schedule that wasn't defined, an event field name that doesn't match the uploaded data, or a dictionary key that doesn't exist.`,
      howToFix: `Check the exact spelling of "${keyName}" against your event definition fields and schedule column names. Field names are case-sensitive.`,
    }),
  },
];

const ERROR_PATTERNS = [
  // ── Syntax ──────────────────────────────────────────────────
  {
    pattern: /SyntaxError|invalid syntax|unexpected (EOF|token|indent)/i,
    category: 'syntax_error',
    whatWentWrong: 'Your code has a syntax error — something is written in a way the system cannot understand.',
    whyItHappened: 'Usually caused by a missing parenthesis, bracket, quote, or comma.',
    howToFix: 'Check for matching parentheses/brackets, missing commas between arguments, and unclosed quotes.',
  },
  // ── Type errors ─────────────────────────────────────────────
  {
    pattern: /TypeError|unsupported operand|cannot (convert|unpack)|expected (str|int|float|list|number)/i,
    category: 'type_error',
    whatWentWrong: 'A function received the wrong type of value (e.g. text instead of a number).',
    whyItHappened: 'The arguments passed to the function don\'t match what it expects.',
    howToFix: 'Check the function\'s expected parameters. Numbers should not be in quotes; arrays use square brackets [...].',
  },
  // ── Name / Reference ────────────────────────────────────────
  {
    pattern: /NameError|is not defined|not found|unknown function/i,
    category: 'reference_error',
    whatWentWrong: 'The system couldn\'t find a variable or function you referenced.',
    whyItHappened: 'Either the name is misspelled or the function doesn\'t exist.',
    howToFix: 'Check the spelling carefully. Use the Function Browser to find the correct function name.',
  },
  // ── Division / Math ─────────────────────────────────────────
  {
    pattern: /ZeroDivisionError|division by zero|divide by zero/i,
    category: 'range_error',
    whatWentWrong: 'A calculation tried to divide by zero.',
    whyItHappened: 'One of the values used as a divisor is zero.',
    howToFix: 'Add a check before dividing: use if(denominator != 0, divide(a, b), 0).',
  },
  // ── Value / Range ───────────────────────────────────────────
  {
    pattern: /ValueError|could not convert|invalid literal|out of range|math domain/i,
    category: 'range_error',
    whatWentWrong: 'A value is outside the acceptable range or in the wrong format.',
    whyItHappened: 'You may have passed a negative number to sqrt(), a non-numeric string to a math function, or dates in the wrong format.',
    howToFix: 'Verify that input values are in the correct format and within valid ranges. Dates should be "YYYY-MM-DD".',
  },
  // ── Index / Key ─────────────────────────────────────────────
  {
    pattern: /IndexError|KeyError|out of (range|bounds)|index/i,
    category: 'range_error',
    whatWentWrong: 'An array index or key is out of bounds.',
    whyItHappened: 'You tried to access an element that doesn\'t exist in the array or object.',
    howToFix: 'Check the array length with array_length() before accessing elements. Use array_get() with a default value.',
  },
  // ── Timeout ─────────────────────────────────────────────────
  {
    pattern: /timeout|timed out|took too long|execution.*exceeded/i,
    category: 'timeout',
    whatWentWrong: 'The code took too long to run and was stopped.',
    whyItHappened: 'The calculation may be too complex, have an infinite loop, or process too much data.',
    howToFix: 'Simplify the logic, reduce array sizes, or break complex calculations into smaller steps.',
  },
  // ── Network ─────────────────────────────────────────────────
  {
    pattern: /network|fetch|connection|ECONNREFUSED|500|502|503|504/i,
    category: 'network_error',
    whatWentWrong: 'The system couldn\'t connect to the server.',
    whyItHappened: 'The backend server may be down or there\'s a network issue.',
    howToFix: 'Check that the server is running. Try refreshing the page.',
  },
  // ── Sandbox / Security ──────────────────────────────────────
  {
    pattern: /blocked|forbidden|not allowed|restricted|sandbox/i,
    category: 'runtime_error',
    whatWentWrong: 'Your code tried to use a blocked operation.',
    whyItHappened: 'For security, some Python built-ins (exec, eval, open, compile) are disabled.',
    howToFix: 'Use only DSL functions. Check the Function Browser for available operations.',
  },

  // ── Attribute ───────────────────────────────────────────────
  {
    pattern: /AttributeError|object has no attribute|has no (method|property)/i,
    category: 'type_error',
    whatWentWrong: 'You tried to use a property or method that doesn\'t exist on this value.',
    whyItHappened: 'The variable may be a different type than expected (e.g. None or a number instead of a list).',
    howToFix: 'Make sure the variable holds the right type of value before accessing its properties.',
  },

  // ── None / Null ─────────────────────────────────────────────
  {
    pattern: /NoneType|NoneType.*has no attribute|object is None|cannot be None|null.*is not/i,
    category: 'type_error',
    whatWentWrong: 'A value expected to hold data is empty (None/null).',
    whyItHappened: 'A function returned nothing, or a variable was never assigned a value.',
    howToFix: 'Check that the variable is assigned a value before using it. Use if() to provide a fallback.',
  },

  // ── Import ──────────────────────────────────────────────────
  {
    pattern: /ImportError|ModuleNotFoundError|No module named|cannot import/i,
    category: 'runtime_error',
    whatWentWrong: 'The code tried to import a Python module, which is not allowed.',
    whyItHappened: 'The DSL sandbox does not support Python import statements.',
    howToFix: 'Remove any import statements. All available functions are already loaded — use the Function Browser.',
  },

  // ── Recursion ────────────────────────────────────────────────
  {
    pattern: /RecursionError|maximum recursion depth|stack overflow/i,
    category: 'runtime_error',
    whatWentWrong: 'The code called itself too many times and ran out of stack space.',
    whyItHappened: 'A recursive or circular calculation kept repeating without stopping.',
    howToFix: 'Restructure the logic to avoid deep nesting or circular references.',
  },

  // ── Memory ──────────────────────────────────────────────────
  {
    pattern: /MemoryError|out of memory|allocation failed/i,
    category: 'runtime_error',
    whatWentWrong: 'The code tried to use too much memory.',
    whyItHappened: 'An array or loop generated more data than the system can hold.',
    howToFix: 'Reduce the size of arrays or the number of iterations in your code.',
  },

  // ── Overflow ────────────────────────────────────────────────
  {
    pattern: /OverflowError|overflow|number too large|result too large|math range/i,
    category: 'range_error',
    whatWentWrong: 'A calculation produced a number too large to store.',
    whyItHappened: 'An exponent, compound interest, or iterative calculation grew without limit.',
    howToFix: 'Use smaller input values or add a ceiling with min().',
  },

  // ── Assertion ────────────────────────────────────────────────
  {
    pattern: /AssertionError/i,
    category: 'runtime_error',
    whatWentWrong: 'A built-in check in the code failed.',
    whyItHappened: 'An assertion statement expected a condition to be true, but it was false.',
    howToFix: 'Review the values being passed to the function and ensure they meet the required conditions.',
  },

  // ── Stop / Exit ──────────────────────────────────────────────
  {
    pattern: /SystemExit|KeyboardInterrupt/i,
    category: 'runtime_error',
    whatWentWrong: 'The code execution was interrupted.',
    whyItHappened: 'A sys.exit() call or interrupt signal was triggered.',
    howToFix: 'Remove any sys.exit() calls from your code.',
  },

  // ── OS / File ───────────────────────────────────────────────
  {
    pattern: /OSError|FileNotFoundError|PermissionError|IsADirectoryError|IOError/i,
    category: 'runtime_error',
    whatWentWrong: 'The code tried to access the file system, which is not allowed.',
    whyItHappened: 'The DSL sandbox does not support reading or writing files.',
    howToFix: 'Remove any file or OS operations. Use event data and DSL functions instead.',
  },

  // ── Unbound / Scope ─────────────────────────────────────────
  {
    pattern: /UnboundLocalError|local variable.*referenced before assignment/i,
    category: 'reference_error',
    whatWentWrong: 'A variable was used before it was assigned a value.',
    whyItHappened: 'The variable is referenced in a conditional branch where it may not have been set yet.',
    howToFix: 'Assign a default value to the variable at the top of your code before using it.',
  },

  // ── Runtime generic ─────────────────────────────────────────
  {
    pattern: /RuntimeError/i,
    category: 'runtime_error',
    whatWentWrong: 'A general runtime error occurred.',
    whyItHappened: 'The code failed during execution for an unexpected reason.',
    howToFix: 'Review your code logic and check all function arguments for correct types and ranges.',
  },

  // ── Not implemented ─────────────────────────────────────────
  {
    pattern: /NotImplementedError/i,
    category: 'runtime_error',
    whatWentWrong: 'A feature called in the code is not yet implemented.',
    whyItHappened: 'The function or operation being called is a stub or placeholder.',
    howToFix: 'Use a supported DSL function. Check the Function Browser for available alternatives.',
  },

  // ── Stop iteration ──────────────────────────────────────────
  {
    pattern: /StopIteration/i,
    category: 'runtime_error',
    whatWentWrong: 'An iterator ran out of values unexpectedly.',
    whyItHappened: 'next() was called on an empty iterator or generator.',
    howToFix: 'Check array / list sizes before iterating. Use array_length() to guard against empty arrays.',
  },

  // ── Environment / variable ──────────────────────────────────
  {
    pattern: /EnvironmentError|environment variable/i,
    category: 'runtime_error',
    whatWentWrong: 'The code tried to read an environment variable or system setting.',
    whyItHappened: 'The DSL sandbox does not expose system environment variables.',
    howToFix: 'Remove environment variable access. Pass required values as function arguments or event fields.',
  },
];

/**
 * Translate a raw error message into user-friendly explanation.
 * @param {string} rawError - The raw error string from the backend
 * @returns {{ whatWentWrong: string, whyItHappened: string, howToFix: string, category: string, rawError: string }}
 */
export function translateError(rawError) {
  if (!rawError || typeof rawError !== 'string') {
    return {
      whatWentWrong: 'An unknown error occurred.',
      whyItHappened: 'The system returned an unexpected response.',
      howToFix: 'Try running your code again. If the problem persists, check your syntax.',
      category: 'runtime_error',
      rawError: rawError || '',
    };
  }

  // Check accounting-context patterns FIRST (more specific, better messages)
  for (const acctPattern of ACCOUNTING_ERROR_PATTERNS) {
    if (acctPattern.pattern.test(rawError)) {
      const extracted = acctPattern.extract ? acctPattern.extract(rawError) : null;
      const result = acctPattern.build(extracted);
      return { ...result, category: acctPattern.category, rawError };
    }
  }

  for (const { pattern, category, whatWentWrong, whyItHappened, howToFix } of ERROR_PATTERNS) {
    if (pattern.test(rawError)) {
      return { whatWentWrong, whyItHappened, howToFix, category, rawError };
    }
  }

  // Fallback — unclassified
  return {
    whatWentWrong: 'Something went wrong while running your code.',
    whyItHappened: rawError.length > 200 ? rawError.slice(0, 200) + '…' : rawError,
    howToFix: 'Check your code syntax and function arguments. Refer to the Function Browser for correct usage.',
    category: 'runtime_error',
    rawError,
  };
}

/**
 * Format a translated error as readable markdown for the chatbot.
 * @param {{ whatWentWrong: string, whyItHappened: string, howToFix: string }} translated
 * @returns {string}
 */
export function formatErrorForChat(translated) {
  return [
    `**What went wrong:** ${translated.whatWentWrong}`,
    `**Why:** ${translated.whyItHappened}`,
    `**How to fix:** ${translated.howToFix}`,
  ].join('\n\n');
}

/**
 * Format a translated error as a single plain-text line for the console.
 * Extracts [Line N] prefix from backend errors and prepends it.
 * @param {string} rawError - The raw error string from the backend
 * @returns {string}
 */
export function formatErrorForConsole(rawError) {
  if (!rawError || typeof rawError !== 'string') {
    const t = translateError(rawError);
    return `${t.whatWentWrong} — ${t.howToFix}`;
  }

  // Extract line number if present (e.g. "[Line 3] division by zero")
  const lineMatch = rawError.match(/^\[Line (\d+)\]\s*/);
  let linePrefix = '';
  let cleanError = rawError;
  if (lineMatch) {
    linePrefix = `Line ${lineMatch[1]}: `;
    cleanError = rawError.slice(lineMatch[0].length);
  }

  const t = translateError(cleanError);
  return `${linePrefix}${t.whatWentWrong} — ${t.howToFix}`;
}

export default translateError;
