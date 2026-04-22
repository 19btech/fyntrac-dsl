import React, { useState, useEffect, useMemo, useCallback } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, Stepper, Step,
  StepLabel, FormControlLabel, Checkbox, Switch, Alert, IconButton, Tooltip, Divider,
  Dialog, DialogTitle, DialogContent, DialogActions, Paper, InputAdornment, Select, FormControl,
  InputLabel, CircularProgress,
} from "@mui/material";
import {
  BookOpen, Search, ArrowRight, ArrowLeft, Play, Code, Eye, CheckCircle2,
  TrendingUp, TrendingDown, DollarSign, Percent, Receipt, Calculator, Building,
  Sparkles, Copy, Settings2, X, Trash2, FileText, Users, GitBranch, Repeat, Database,
  Download, Upload, AlertCircle,
} from "lucide-react";
import ACCOUNTING_TEMPLATES from "./AccountingTemplates";
import { API } from "../../config";

/**
 * Parse generated DSL code into multiple Rule Builder-compatible rules.
 * Splits code into: simple_calc (parameters), schedule (custom_code),
 * conditional, iteration blocks — each as a separate rule.
 */
function parseDSLToRules(code, templateTitle) {
  const lines = code.split('\n');
  const statements = []; // { type, name, rhs, rawLines, ... }
  let i = 0;

  // Phase 1: Parse lines into statements
  while (i < lines.length) {
    const line = lines[i].trim();
    const rawLine = lines[i];

    if (!line || line.startsWith('##')) {
      statements.push({ type: 'skip', raw: rawLine });
      i++; continue;
    }

    if (line.startsWith('print(') || line.startsWith('print (')) {
      statements.push({ type: 'print', raw: rawLine });
      i++; continue;
    }

    if (line.startsWith('createTransaction(')) {
      const inner = line.slice('createTransaction('.length, -1);
      const args = splitArgs(inner);
      statements.push({
        type: 'createTransaction', raw: rawLine,
        txnType: (args[2] || '').replace(/^"|"$/g, ''),
        amount: args[3] || '', postingDate: args[0] || '',
        effectiveDate: args[1] || '', subInstrumentId: args[4] || '',
      });
      i++; continue;
    }

    const assignMatch = line.match(/^([a-zA-Z_]\w*)\s*=\s*(.*)/);
    if (assignMatch) {
      const name = assignMatch[1];
      let rhs = assignMatch[2];
      const startLine = i;
      let depth = 0;
      for (const ch of rhs) { if ('({['.includes(ch)) depth++; if (')}]'.includes(ch)) depth--; }
      while (depth > 0 && i + 1 < lines.length) {
        i++;
        rhs += '\n' + lines[i];
        for (const ch of lines[i]) { if ('({['.includes(ch)) depth++; if (')}]'.includes(ch)) depth--; }
      }
      rhs = rhs.trim();
      const rawLines = lines.slice(startLine, i + 1).join('\n');

      // Classify by RHS content
      const stype = classifyAssignment(name, rhs);
      statements.push({ ...stype, name, rhs, raw: rawLines });
      i++; continue;
    }

    statements.push({ type: 'other', raw: rawLine });
    i++;
  }

  // Phase 2: Bucket statements by type (collect ALL variables into one group)
  const defaultOutputs = { printResult: true, createTransaction: false, transactions: [{ type: '', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }] };
  const defaultRule = { conditions: [], elseFormula: '', conditionResultVar: 'result', iterConfig: {}, outputs: { ...defaultOutputs } };

  const paramStmts = [];     // all simple variable assignments
  const iterStmts = [];      // iteration statements + their prints
  const schedStmts = [];     // schedule statements + their prints
  const condStmts = [];      // conditional statements
  const journalStmts = [];   // createTransaction statements
  let lastComplexType = null; // for assigning prints to the right bucket

  for (const stmt of statements) {
    if (stmt.type === 'skip') continue;

    if (stmt.type === 'variable') {
      paramStmts.push(stmt);
      continue;
    }

    if (stmt.type === 'schedule') {
      schedStmts.push(stmt);
      lastComplexType = 'schedule';
      continue;
    }

    if (stmt.type === 'iteration') {
      iterStmts.push(stmt);
      lastComplexType = 'iteration';
      continue;
    }

    if (stmt.type === 'conditional') {
      condStmts.push(stmt);
      lastComplexType = 'conditional';
      continue;
    }

    if (stmt.type === 'createTransaction') {
      journalStmts.push(stmt);
      lastComplexType = 'journal';
      continue;
    }

    if (stmt.type === 'print') {
      if (lastComplexType === 'schedule') schedStmts.push(stmt);
      else if (lastComplexType === 'iteration') iterStmts.push(stmt);
      else if (lastComplexType === 'conditional') condStmts.push(stmt);
      // prints before any complex block are standalone; skip them from rules
      continue;
    }
  }

  // Phase 2b: (reserved — chained iterations are now kept as separate iteration rules)

  // Phase 3: Create rules in logical execution order
  const rules = [];
  const schedules = []; // Schedule builder entries (saved-schedules)
  const canMergeJournalIntoParams =
    paramStmts.length > 0 && condStmts.length === 0 && iterStmts.length === 0 && schedStmts.length === 0;

  // 1. Parameters rule (simple_calc) — all simple variables
  if (paramStmts.length > 0) {
    const vars = paramStmts.map(s => classifyToVariable(s));
    const txnRows = canMergeJournalIntoParams
      ? journalStmts.map(txn => ({
          type: txn.txnType || '',
          amount: txn.amount || '',
          postingDate: txn.postingDate || '',
          effectiveDate: txn.effectiveDate || '',
          subInstrumentId: txn.subInstrumentId || '',
        }))
      : [];
    const genCode = canMergeJournalIntoParams
      ? [...paramStmts.map(s => s.raw), ...journalStmts.map(s => s.raw)].join('\n')
      : paramStmts.map(s => s.raw).join('\n');
    // Build unified steps from variables
    const calcSteps = vars.map(v => ({
      name: v.name, stepType: 'calc',
      source: v.source || 'formula', formula: v.formula || '', value: v.value || '',
      eventField: v.eventField || '', collectType: v.collectType || 'collect',
    }));
    rules.push({
      ...defaultRule,
      name: `${templateTitle} - Parameters`,
      ruleType: 'simple_calc',
      variables: vars,
      steps: calcSteps,
      outputs: canMergeJournalIntoParams
        ? {
            printResult: true,
            createTransaction: txnRows.length > 0,
            transactions: txnRows.length > 0 ? txnRows : [{ type: '', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }],
          }
        : { ...defaultOutputs },
      generatedCode: genCode,
      customCode: '',
    });
  }

  // 2. Conditional rules — one per iif() assignment
  for (const stmt of condStmts) {
    if (stmt.type !== 'conditional') continue;
    const parsed = parseIif(stmt.rhs);
    rules.push({
      ...defaultRule,
      name: `${templateTitle} - Conditional`,
      ruleType: 'conditional',
      variables: [],
      steps: [{
        name: stmt.name,
        stepType: 'condition',
        conditions: parsed.conditions,
        elseFormula: parsed.elseFormula,
      }],
      conditions: parsed.conditions,
      elseFormula: parsed.elseFormula,
      conditionResultVar: stmt.name,
      generatedCode: stmt.raw,
      customCode: '',
    });
  }

  // 3. Iteration rule — all iterations merge into a single rule with an iterations array
  if (iterStmts.length > 0) {
    const iterAssigns = iterStmts.filter(s => s.type === 'iteration');
    const genCode = iterStmts.map(s => s.raw).join('\n');
    const allIterCfgs = iterAssigns.map(ia => parseIterConfig(ia.rhs, ia.name));
    const lastIterName = allIterCfgs.length > 0 ? (allIterCfgs[allIterCfgs.length - 1].resultVar || 'mapped_result') : 'mapped_result';

    rules.push({
      ...defaultRule,
      name: `${templateTitle} - Iteration`,
      ruleType: 'iteration',
      variables: [],
      steps: [{
        name: lastIterName,
        stepType: 'iteration',
        iterations: allIterCfgs,
      }],
      iterations: allIterCfgs,
      iterConfig: allIterCfgs[0] || {},
      generatedCode: genCode,
      customCode: '',
    });
  }

  // 4. Schedule → saved as a rule with schedule step type
  if (schedStmts.length > 0) {
    const allLines = [...schedStmts, ...journalStmts];
    const genCode = allLines.map(s => s.raw).join('\n');
    const schedCfg = parseScheduleConfig(schedStmts, journalStmts);

    // Find the schedule variable name (e.g., "sched")
    const schedAssign = schedStmts.find(s => s.rhs && /^schedule\s*\(/.test(s.rhs));
    const schedVarName = schedAssign?.name || 'sched';

    // Build outputVars from the parsed config
    const outputVars = [];
    if (schedCfg.extractFirst && schedCfg.extractColumn) {
      const firstStmt = schedStmts.find(s => s.rhs && /^schedule_first\s*\(/.test(s.rhs));
      outputVars.push({ name: firstStmt?.name || `first_${schedCfg.extractColumn}`, type: 'first', column: schedCfg.extractColumn });
    }
    if (schedCfg.extractLast && schedCfg.extractColumn) {
      const lastStmt = schedStmts.find(s => s.rhs && /^schedule_last\s*\(/.test(s.rhs));
      outputVars.push({ name: lastStmt?.name || `last_${schedCfg.extractColumn}`, type: 'last', column: schedCfg.extractColumn });
    }
    if (schedCfg.enableSum && schedCfg.sumColumn) {
      outputVars.push({ name: schedCfg.sumVarName || `sum_${schedCfg.sumColumn}`, type: 'sum', column: schedCfg.sumColumn });
    }
    if (schedCfg.enableCol && schedCfg.colColumn) {
      outputVars.push({ name: schedCfg.colVarName || `col_${schedCfg.colColumn}`, type: 'column', column: schedCfg.colColumn });
    }
    if (schedCfg.enableFilter && schedCfg.filterReturnCol) {
      outputVars.push({ name: schedCfg.filterVarName || `filtered_${schedCfg.filterReturnCol}`, type: 'filter', column: schedCfg.filterReturnCol, matchCol: schedCfg.filterMatchCol, matchValue: schedCfg.filterMatchValue });
    }

    // Build transactions if present
    const txnRows = journalStmts.map(txn => ({
      type: txn.txnType || '', amount: txn.amount || '',
      postingDate: txn.postingDate || '', effectiveDate: txn.effectiveDate || '',
      subInstrumentId: txn.subInstrumentId || '',
    }));

    rules.push({
      ...defaultRule,
      name: `${templateTitle} - Schedule`,
      ruleType: 'schedule',
      variables: [],
      steps: [{
        name: schedVarName,
        stepType: 'schedule',
        printResult: true,
        scheduleConfig: {
          periodType: schedCfg.periodType,
          startDate: schedCfg.startDate, startDateSource: schedCfg.startDateSource,
          startDateField: schedCfg.startDateField, startDateFormula: schedCfg.startDateFormula,
          endDate: schedCfg.endDate, endDateSource: schedCfg.endDateSource,
          endDateField: schedCfg.endDateField, endDateFormula: schedCfg.endDateFormula,
          periodCount: schedCfg.periodCount, periodCountSource: schedCfg.periodCountSource,
          periodCountField: schedCfg.periodCountField, periodCountFormula: schedCfg.periodCountFormula,
          frequency: schedCfg.frequency, convention: schedCfg.convention,
          columns: schedCfg.columns,
          extractFirst: schedCfg.extractFirst, extractLast: schedCfg.extractLast,
          extractColumn: schedCfg.extractColumn,
          firstVarName: schedCfg.extractFirst ? (outputVars.find(o => o.type === 'first')?.name || '') : '',
          lastVarName: schedCfg.extractLast ? (outputVars.find(o => o.type === 'last')?.name || '') : '',
          enableSum: schedCfg.enableSum, sumColumn: schedCfg.sumColumn, sumVarName: schedCfg.sumVarName,
          enableCol: schedCfg.enableCol, colColumn: schedCfg.colColumn, colVarName: schedCfg.colVarName,
          enableFilter: schedCfg.enableFilter, filterVarName: schedCfg.filterVarName,
          filterMatchCol: schedCfg.filterMatchCol, filterMatchValue: schedCfg.filterMatchValue,
          filterReturnCol: schedCfg.filterReturnCol,
          contextVars: schedCfg.contextVars,
        },
        outputVars,
      }],
      outputs: {
        printResult: true,
        createTransaction: txnRows.length > 0,
        transactions: txnRows.length > 0 ? txnRows : [{ type: '', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }],
      },
      generatedCode: genCode,
      customCode: '',
    });
  } else if (journalStmts.length > 0 && !canMergeJournalIntoParams) {
    // No schedule but has createTransaction — standalone rule
    const txn = journalStmts[0];
    const genCode = journalStmts.map(s => s.raw).join('\n');
    rules.push({
      ...defaultRule,
      name: `${templateTitle} - Transactions`,
      ruleType: 'simple_calc',
      variables: [],
      steps: [],
      outputs: {
        printResult: false,
        createTransaction: true,
        transactions: [{ type: txn.txnType, amount: txn.amount, postingDate: txn.postingDate, effectiveDate: txn.effectiveDate, subInstrumentId: txn.subInstrumentId }],
      },
      generatedCode: genCode,
      customCode: '',
    });
  }

  // Fallback: if nothing created, one catch-all custom_code rule
  if (rules.length === 0 && schedules.length === 0) {
    rules.push({
      ...defaultRule,
      name: templateTitle,
      ruleType: 'custom_code',
      variables: [],
      steps: [],
      generatedCode: code,
      customCode: code,
    });
  }

  // Deduplicate rule names
  const nameCounts = {};
  for (const rule of [...rules, ...schedules]) {
    if (nameCounts[rule.name]) {
      nameCounts[rule.name]++;
      rule.name = `${rule.name} ${nameCounts[rule.name]}`;
    } else {
      nameCounts[rule.name] = 1;
    }
  }

  return { rules, schedules };
}

/** Split comma-separated args respecting nesting */
function splitArgs(str) {
  const args = [];
  let depth = 0, current = '';
  for (const ch of str) {
    if (ch === '(' || ch === '[' || ch === '{') depth++;
    if (ch === ')' || ch === ']' || ch === '}') depth--;
    if (ch === ',' && depth === 0) { args.push(current.trim()); current = ''; }
    else { current += ch; }
  }
  if (current.trim()) args.push(current.trim());
  return args;
}

/** Classify an assignment statement by its RHS */
function classifyAssignment(name, rhs) {
  // Schedule-related (only core schedule functions)
  if (/^(schedule|period|schedule_sum|schedule_filter|schedule_first|schedule_last)\s*\(/.test(rhs)) {
    return { type: 'schedule' };
  }
  // Conditional
  if (/^(iif|if)\s*\(/.test(rhs)) {
    return { type: 'conditional' };
  }
  // Iteration
  if (/^(map_array|for_each|apply_each)\s*\(/.test(rhs)) {
    return { type: 'iteration' };
  }
  return { type: 'variable' };
}

/** Convert a parsed statement into a Rule Builder variable object */
function classifyToVariable(stmt) {
  const rhs = stmt.rhs;
  const base = { name: stmt.name, value: '', formula: '', eventField: '', collectType: 'collect' };

  // Collect functions
  const collectMatch = rhs.match(/^(collect_by_instrument|collect_all|collect_by_subinstrument|collect_subinstrumentids|collect)\(([^)]*)\)$/);
  if (collectMatch) return { ...base, source: 'collect', collectType: collectMatch[1], eventField: collectMatch[2] || '' };

  // Plain number
  if (/^-?\d+(\.\d+)?$/.test(rhs)) return { ...base, source: 'value', value: rhs };

  // Quoted string
  if (/^"[^"]*"$/.test(rhs)) return { ...base, source: 'value', value: rhs };

  // Event field reference (EventName.field_name)
  if (/^[A-Z][a-zA-Z0-9]*\.[a-zA-Z_]\w*$/.test(rhs)) return { ...base, source: 'event_field', eventField: rhs };

  // Array literal — treat as value
  if (/^\[.*\]$/.test(rhs)) return { ...base, source: 'value', value: rhs };

  // Formula
  return { ...base, source: 'formula', formula: rhs };
}

/** Parse a (possibly nested) if() / iif() call into conditions array + elseFormula */
function parseIif(rhs) {
  const conditions = [];
  let current = rhs;
  while (true) {
    const m = current.match(/^(iif|if)\s*\((.*)\)$/s);
    if (!m) break;
    const inner = m[2];
    const args = splitArgs(inner);
    if (args.length < 3) break;
    conditions.push({ condition: args[0], thenFormula: args[1] });
    const rest = args.slice(2).join(', ');
    if (/^(iif|if)\s*\(/.test(rest)) {
      current = rest;
    } else {
      return { conditions, elseFormula: rest };
    }
  }
  return { conditions: conditions.length ? conditions : [{ condition: '', thenFormula: '' }], elseFormula: current };
}

/** Parse map_array/for_each/apply_each into iterConfig */
function parseIterConfig(rhs, resultVar) {
  // apply_each: single or paired mode
  const aeMatch = rhs.match(/^apply_each\s*\((.*)\)$/s);
  if (aeMatch) {
    const args = splitArgs(aeMatch[1]);
    // Detect mode: if the second arg is a quoted string, it's single-array mode
    const secondArg = (args[1] || '').trim();
    if (secondArg.startsWith('"') || secondArg.startsWith("'")) {
      // Single-array mode: apply_each(array, "expr", {ctx})
      return {
        type: 'apply_each',
        sourceArray: args[0] || '',
        varName: 'each',
        expression: secondArg.replace(/^"|"$/g, ''),
        resultVar: resultVar || 'mapped_result',
        secondArray: '', secondVar: 'second',
      };
    } else {
      // Paired mode: apply_each(array1, array2, "expr", {ctx})
      return {
        type: 'apply_each_paired',
        sourceArray: args[0] || '',
        varName: 'each',
        expression: (args[2] || '').replace(/^"|"$/g, ''),
        resultVar: resultVar || 'mapped_result',
        secondArray: secondArg, secondVar: 'second',
      };
    }
  }
  const mapMatch = rhs.match(/^map_array\s*\((.*)\)$/s);
  if (mapMatch) {
    const args = splitArgs(mapMatch[1]);
    return {
      type: 'map_array',
      sourceArray: args[0] || '',
      varName: (args[1] || '').replace(/^"|"$/g, ''),
      expression: (args[2] || '').replace(/^"|"$/g, ''),
      resultVar: resultVar || 'mapped_result',
      secondArray: '', secondVar: 'amount',
    };
  }
  const feMatch = rhs.match(/^for_each\s*\((.*)\)$/s);
  if (feMatch) {
    const args = splitArgs(feMatch[1]);
    return {
      type: 'for_each',
      sourceArray: args[0] || '',
      secondArray: args[1] || '',
      varName: (args[2] || '').replace(/^"|"$/g, ''),
      secondVar: (args[3] || '').replace(/^"|"$/g, ''),
      expression: (args[4] || '').replace(/^"|"$/g, ''),
      resultVar: resultVar || 'mapped_result',
    };
  }
  return { type: 'apply_each', sourceArray: '', varName: 'each', expression: '', resultVar: resultVar || 'mapped_result', secondArray: '', secondVar: 'second' };
}

/** Parse schedule statements into ScheduleBuilder-compatible config */
function parseScheduleConfig(schedStmts, journalStmts) {
  const cfg = {
    periodType: 'date', startDate: '', startDateSource: 'value', startDateField: '', startDateFormula: '',
    endDate: '', endDateSource: 'value', endDateField: '', endDateFormula: '',
    periodCount: '12', periodCountSource: 'value', periodCountField: '', periodCountFormula: '',
    frequency: 'M', convention: '', columns: [{ name: 'date', formula: 'period_date' }],
    createTxn: false, txnType: '', txnAmountCol: '',
    extractFirst: false, extractLast: false, extractColumn: '',
    enableSum: false, sumColumn: '', sumVarName: '',
    enableCol: false, colColumn: '', colVarName: '',
    enableFilter: false, filterVarName: '', filterMatchCol: '', filterMatchValue: '', filterReturnCol: '',
  };

  for (const stmt of schedStmts) {
    if (stmt.type !== 'schedule') continue;
    const rhs = stmt.rhs || '';
    const name = stmt.name || '';

    // period(start, end, "M") or period(start, end, "M", "convention")
    if (/^period\s*\(/.test(rhs)) {
      const inner = rhs.match(/^period\s*\((.*)\)$/s);
      if (inner) {
        const args = splitArgs(inner[1]);
        if (args.length >= 2) {
          // Date-based period
          cfg.periodType = 'date';
          const startArg = args[0].trim();
          if (/^".*"$/.test(startArg)) { cfg.startDateSource = 'value'; cfg.startDate = startArg.replace(/^"|"$/g, ''); }
          else if (/^[A-Z]/.test(startArg) && startArg.includes('.')) { cfg.startDateSource = 'field'; cfg.startDateField = startArg; }
          else { cfg.startDateSource = 'formula'; cfg.startDateFormula = startArg; }
          const endArg = args[1].trim();
          if (/^".*"$/.test(endArg)) { cfg.endDateSource = 'value'; cfg.endDate = endArg.replace(/^"|"$/g, ''); }
          else if (/^[A-Z]/.test(endArg) && endArg.includes('.')) { cfg.endDateSource = 'field'; cfg.endDateField = endArg; }
          else { cfg.endDateSource = 'formula'; cfg.endDateFormula = endArg; }
          if (args[2]) cfg.frequency = args[2].replace(/^"|"$/g, '');
          if (args[3]) cfg.convention = args[3].replace(/^"|"$/g, '');
        } else if (args.length === 1) {
          cfg.periodType = 'number';
          const countArg = args[0].trim();
          if (/^\d+$/.test(countArg)) { cfg.periodCountSource = 'value'; cfg.periodCount = countArg; }
          else if (/^[A-Z]/.test(countArg) && countArg.includes('.')) { cfg.periodCountSource = 'field'; cfg.periodCountField = countArg; }
          else { cfg.periodCountSource = 'formula'; cfg.periodCountFormula = countArg; }
        }
      }
    }

    // schedule(p, {columns}, {context})
    if (/^schedule\s*\(/.test(rhs)) {
      // Extract column definitions from the first {…} block
      const colMatch = rhs.match(/\{([^}]*)\}/);
      if (colMatch) {
        const cols = [];
        // Line-by-line parsing handles formulas with commas (e.g. lag('x', 1, y))
        const colLines = colMatch[1].split('\n');
        for (const cl of colLines) {
          const trimmed = cl.trim().replace(/,\s*$/, '');
          const kv = trimmed.match(/^"([^"]+)"\s*:\s*"(.*)"$/);
          if (kv) cols.push({ name: kv[1], formula: kv[2] });
        }
        // Fallback for single-line dict: "key": "val", "key2": "val2"
        if (cols.length === 0) {
          const allKV = [...colMatch[1].matchAll(/"([^"]+)"\s*:\s*"([^"]*)"/g)];
          for (const m of allKV) cols.push({ name: m[1], formula: m[2] });
        }
        if (cols.length > 0) cfg.columns = cols;
      }
      // Extract context variable references from the second {…} block
      const allBlocks = [...rhs.matchAll(/\{([^}]*)\}/g)];
      if (allBlocks.length >= 2) {
        const ctxBlock = allBlocks[allBlocks.length - 1][1]; // last {} is context
        const ctxVars = [];
        const ctxPairs = [...ctxBlock.matchAll(/"([^"]+)"\s*:\s*([a-zA-Z_]\w*)/g)];
        for (const m of ctxPairs) {
          ctxVars.push(m[2]); // the variable name (value side)
        }
        if (ctxVars.length > 0) {
          cfg.contextVars = ctxVars;
        }
      }
    }

    // schedule_sum(sched, "col") → enableSum
    if (/^schedule_sum\s*\(/.test(rhs)) {
      const match = rhs.match(/schedule_sum\s*\(\w+,\s*"([^"]+)"\)/);
      if (match) { cfg.enableSum = true; cfg.sumColumn = match[1]; cfg.sumVarName = name; }
    }

    // schedule_filter(sched, ...) → enableFilter
    if (/^schedule_filter\s*\(/.test(rhs)) {
      const inner = rhs.match(/^schedule_filter\s*\((.*)\)$/s);
      if (inner) {
        const args = splitArgs(inner[1]);
        if (args.length >= 4) {
          cfg.enableFilter = true;
          cfg.filterVarName = name;
          cfg.filterMatchCol = (args[1] || '').replace(/^"|"$/g, '');
          cfg.filterMatchValue = (args[2] || '').trim();
          cfg.filterReturnCol = (args[3] || '').replace(/^"|"$/g, '');
        }
      }
    }

    // schedule_first / schedule_last
    if (/^schedule_first\s*\(/.test(rhs)) {
      const match = rhs.match(/schedule_first\s*\(\w+,\s*"([^"]+)"\)/);
      if (match) { cfg.extractFirst = true; cfg.extractColumn = match[1]; }
    }
    if (/^schedule_last\s*\(/.test(rhs)) {
      const match = rhs.match(/schedule_last\s*\(\w+,\s*"([^"]+)"\)/);
      if (match) { cfg.extractLast = true; if (!cfg.extractColumn) cfg.extractColumn = match[1]; }
    }
  }

  // Merge createTransaction into schedule config
  if (journalStmts.length > 0) {
    const txn = journalStmts[0];
    cfg.createTxn = true;
    cfg.txnType = txn.txnType || '';
    cfg.txnAmountCol = txn.amount || '';
  }

  return cfg;
}

const ICON_MAP = {
  TrendingUp, TrendingDown, DollarSign, Percent, Receipt, Calculator, Building,
};

const FieldInput = ({ field, value, source, fieldRef, events, onChange }) => {
  const eventFields = useMemo(() => {
    if (!events || events.length === 0) return [];
    const result = [];
    events.forEach((event) => {
      result.push({ label: `${event.event_name}.postingdate`, value: `${event.event_name}.postingdate`, type: 'date' });
      result.push({ label: `${event.event_name}.effectivedate`, value: `${event.event_name}.effectivedate`, type: 'date' });
      event.fields.forEach((f) => {
        result.push({ label: `${event.event_name}.${f.name}`, value: `${event.event_name}.${f.name}`, type: f.datatype });
      });
    });
    return result;
  }, [events]);

  const isFieldType = field.type === 'field';
  const canChooseSource = field.type === 'number_or_field' || field.type === 'date_or_field';
  const currentSource = isFieldType ? 'field' : (source || 'value');

  return (
    <Box sx={{ mb: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
        <Typography variant="body2" fontWeight={600} color="text.primary">{field.label}</Typography>
        {field.required && <Chip label="Required" size="small" sx={{ fontSize: '0.625rem', height: 16, bgcolor: '#FFF3CD', color: '#856404' }} />}
      </Box>
      {field.helpText && (
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.75 }}>{field.helpText}</Typography>
      )}

      {canChooseSource && (
        <Box sx={{ display: 'flex', gap: 0.5, mb: 1 }}>
          <Chip label="Enter Value" size="small" onClick={() => onChange(field.key, value, 'value', fieldRef)}
            sx={{ cursor: 'pointer', bgcolor: currentSource === 'value' ? '#EEF0FE' : '#F8F9FA', color: currentSource === 'value' ? '#5B5FED' : '#6C757D', border: currentSource === 'value' ? '1px solid #5B5FED' : '1px solid #E9ECEF' }} />
          <Chip label="From Event Data" size="small" onClick={() => onChange(field.key, value, 'field', fieldRef)}
            sx={{ cursor: 'pointer', bgcolor: currentSource === 'field' ? '#EEF0FE' : '#F8F9FA', color: currentSource === 'field' ? '#5B5FED' : '#6C757D', border: currentSource === 'field' ? '1px solid #5B5FED' : '1px solid #E9ECEF' }} />
        </Box>
      )}

      {(currentSource === 'field' || isFieldType) ? (
        <FormControl fullWidth size="small">
          <Select
            value={fieldRef || ''}
            onChange={(e) => onChange(field.key, value, 'field', e.target.value)}
            displayEmpty
            sx={{ fontSize: '0.875rem' }}
          >
            <MenuItem value="" disabled><em>Select event field...</em></MenuItem>
            {eventFields.map((ef) => (
              <MenuItem key={ef.value} value={ef.value}>{ef.label} ({ef.type})</MenuItem>
            ))}
          </Select>
        </FormControl>
      ) : field.type === 'select' ? (
        <FormControl fullWidth size="small">
          <Select
            value={value || field.default || ''}
            onChange={(e) => onChange(field.key, e.target.value, 'value', fieldRef)}
            sx={{ fontSize: '0.875rem' }}
          >
            {field.options.map((opt) => (
              <MenuItem key={opt} value={opt}>{opt}</MenuItem>
            ))}
          </Select>
        </FormControl>
      ) : (
        <TextField
          fullWidth size="small"
          type={field.type === 'date_or_field' ? 'date' : 'text'}
          value={value || ''}
          placeholder={field.placeholder || ''}
          onChange={(e) => onChange(field.key, e.target.value, 'value', fieldRef)}
          InputLabelProps={field.type === 'date_or_field' ? { shrink: true } : undefined}
        />
      )}
    </Box>
  );
};

const TemplateWizard = ({ template, events, onGenerate, onClose }) => {
  const [activeStep, setActiveStep] = useState(0);
  const [config, setConfig] = useState(() => {
    const initial = {};
    template.fields.forEach((f) => {
      initial[f.key] = f.default || '';
      initial[`${f.key}_source`] = f.type === 'field' ? 'field' : 'value';
      initial[`${f.key}_field`] = '';
    });
    template.outputs.forEach((o) => {
      initial[`outputs_${o.key}`] = o.default;
      if (o.txnType) initial['txn_type'] = o.txnType;
    });
    return initial;
  });
  const [generatedCode, setGeneratedCode] = useState('');
  const [showCode, setShowCode] = useState(false);
  const [localEvents, setLocalEvents] = useState(events || []);
  const [sampleLoaded, setSampleLoaded] = useState(false);
  const [loadingSample, setLoadingSample] = useState(false);

  const handleLoadSampleData = useCallback(async () => {
    setLoadingSample(true);
    try {
      const res = await fetch(`${API}/template-sample-data/${template.id}`, { method: 'POST' });
      const data = await res.json();
      if (data.success && data.events) {
        setLocalEvents(data.events);
        setSampleLoaded(true);
      }
    } catch (err) {
      console.error('Failed to load sample data:', err);
    } finally {
      setLoadingSample(false);
    }
  }, [template.id]);

  const steps = ['Configure Parameters', 'Select Outputs', 'Preview & Generate'];

  const handleFieldChange = useCallback((key, value, source, fieldRef) => {
    setConfig((prev) => ({
      ...prev,
      [key]: value,
      [`${key}_source`]: source,
      [`${key}_field`]: fieldRef || prev[`${key}_field`],
    }));
  }, []);

  const handleOutputToggle = useCallback((key) => {
    setConfig((prev) => ({ ...prev, [`outputs_${key}`]: !prev[`outputs_${key}`] }));
  }, []);

  const handleGenerate = useCallback(() => {
    const code = template.generateDSL(config);
    setGeneratedCode(code);
    setActiveStep(2);
  }, [template, config]);

  const handleApply = useCallback(() => {
    const code = generatedCode || template.generateDSL(config);
    const { rules, schedules } = parseDSLToRules(code, template.title);
    onGenerate(code, { rules, schedules });
  }, [generatedCode, template, config, onGenerate]);

  const isStep1Valid = useMemo(() => {
    return template.fields.filter(f => f.required).every((f) => {
      const source = config[`${f.key}_source`];
      if (source === 'field' || f.type === 'field') return !!config[`${f.key}_field`];
      return !!config[f.key];
    });
  }, [template, config]);

  const Icon = ICON_MAP[template.icon] || Settings2;

  return (
    <Dialog open={true} onClose={onClose} maxWidth="md" fullWidth PaperProps={{ sx: { height: '85vh' } }}>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Icon size={22} color="#5B5FED" />
          <Box>
            <Typography variant="h5">{template.title}</Typography>
            <Typography variant="caption" color="text.secondary">{template.description}</Typography>
          </Box>
          {template.standard && (
            <Chip label={template.standard} size="small" sx={{ ml: 'auto', bgcolor: '#EEF0FE', color: '#5B5FED' }} />
          )}
        </Box>
      </DialogTitle>

      <DialogContent sx={{ display: 'flex', flexDirection: 'column', p: 3 }}>
        <Stepper activeStep={activeStep} sx={{ mb: 3 }}>
          {steps.map((label) => (
            <Step key={label}><StepLabel>{label}</StepLabel></Step>
          ))}
        </Stepper>

        <Box sx={{ flex: 1, overflowY: 'auto' }}>
          {activeStep === 0 && (
            <Box>
              {/* Sample Data Banner */}
              <Box sx={{
                mb: 2, p: 1.5, borderRadius: 1.5, display: 'flex', alignItems: 'center', gap: 1.5,
                bgcolor: sampleLoaded ? '#D4EDDA' : '#F0F1FF', border: sampleLoaded ? '1px solid #C3E6CB' : '1px solid #D6D8FE',
              }}>
                {sampleLoaded ? (
                  <>
                    <CheckCircle2 size={18} color="#28A745" />
                    <Typography variant="body2" color="#155724" sx={{ flex: 1 }}>
                      Sample data loaded — select <strong>From Event Data</strong> on any field to use it.
                    </Typography>
                  </>
                ) : (
                  <>
                    <Database size={18} color="#5B5FED" />
                    <Typography variant="body2" color="text.secondary" sx={{ flex: 1 }}>
                      No event data? Load sample data to try this template with pre-configured events.
                    </Typography>
                    <Button
                      variant="contained" size="small"
                      startIcon={loadingSample ? <CircularProgress size={14} color="inherit" /> : <Download size={14} />}
                      onClick={handleLoadSampleData}
                      disabled={loadingSample}
                      sx={{ textTransform: 'none', whiteSpace: 'nowrap' }}
                    >
                      {loadingSample ? 'Loading…' : 'Load Sample Data'}
                    </Button>
                  </>
                )}
              </Box>

              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Set the parameters for your calculation. You can enter values directly or reference fields from your uploaded event data.
              </Typography>
              {template.fields.map((field) => (
                <FieldInput
                  key={field.key} field={field} events={localEvents}
                  value={config[field.key]}
                  source={config[`${field.key}_source`]}
                  fieldRef={config[`${field.key}_field`]}
                  onChange={handleFieldChange}
                />
              ))}
            </Box>
          )}

          {activeStep === 1 && (
            <Box>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Choose what outputs this calculation should produce.
              </Typography>
              {template.outputs.map((output) => (
                <Card key={output.key} sx={{ mb: 1.5 }}>
                  <CardContent sx={{ p: 2, '&:last-child': { pb: 2 }, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Box>
                      <Typography variant="body2" fontWeight={600}>{output.label}</Typography>
                      {output.txnType && config[`outputs_${output.key}`] && (
                        <TextField
                          size="small" label="Transaction Type" sx={{ mt: 1, minWidth: 200 }}
                          value={config.txn_type || output.txnType}
                          onChange={(e) => setConfig(prev => ({ ...prev, txn_type: e.target.value }))}
                        />
                      )}
                    </Box>
                    <Switch
                      checked={!!config[`outputs_${output.key}`]}
                      onChange={() => handleOutputToggle(output.key)}
                      color="primary"
                    />
                  </CardContent>
                </Card>
              ))}
            </Box>
          )}

          {activeStep === 2 && (
            <Box>
              <Alert severity="success" sx={{ mb: 2 }}>
                Your calculation logic has been generated. Review and load it into the editor.
              </Alert>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                <FormControlLabel
                  control={<Switch checked={showCode} onChange={(e) => setShowCode(e.target.checked)} size="small" />}
                  label={<Typography variant="body2">Show generated logic</Typography>}
                />
              </Box>
              {showCode && (
                <Paper variant="outlined" sx={{ p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 300, overflow: 'auto' }}>
                  <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
                    {generatedCode || template.generateDSL(config)}
                  </pre>
                </Paper>
              )}

              <Box sx={{ mt: 2 }}>
                <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>Summary</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {template.fields.filter(f => config[f.key] || config[`${f.key}_field`]).map((f) => (
                    <Chip key={f.key} size="small"
                      label={`${f.label}: ${config[`${f.key}_source`] === 'field' ? config[`${f.key}_field`] : config[f.key]}`}
                      sx={{ bgcolor: '#F8F9FA' }}
                    />
                  ))}
                  {template.outputs.filter(o => config[`outputs_${o.key}`]).map((o) => (
                    <Chip key={o.key} size="small" label={o.label} icon={<CheckCircle2 size={12} />}
                      sx={{ bgcolor: '#D4EDDA', color: '#155724' }}
                    />
                  ))}
                </Box>
              </Box>
            </Box>
          )}
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2, borderTop: '1px solid #E9ECEF' }}>
        <Button onClick={onClose} color="inherit">Cancel</Button>
        <Box sx={{ flex: 1 }} />
        {activeStep > 0 && (
          <Button onClick={() => setActiveStep(s => s - 1)} startIcon={<ArrowLeft size={16} />}>Back</Button>
        )}
        {activeStep < 2 && (
          <Button variant="contained" onClick={() => { if (activeStep === 1) handleGenerate(); else setActiveStep(s => s + 1); }}
            disabled={activeStep === 0 && !isStep1Valid}
            endIcon={<ArrowRight size={16} />}>
            {activeStep === 1 ? 'Generate' : 'Next'}
          </Button>
        )}
        {activeStep === 2 && (
          <Button variant="contained" onClick={handleApply} startIcon={<Play size={16} />}>
            Load into Editor
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

/* ── Rule-type display metadata (for UserTemplateWizard) ── */
const RULE_TYPE_META_WIZ = {
  simple_calc: { label: 'Calculation', color: '#5B5FED', icon: Calculator },
  conditional: { label: 'Conditional', color: '#FF9800', icon: GitBranch },
  iteration: { label: 'Iteration', color: '#00BCD4', icon: Repeat },
  collect: { label: 'Collect', color: '#8BC34A', icon: Database },
  custom_code: { label: 'Custom Code', color: '#9C27B0', icon: Code },
};

/**
 * UserTemplateWizard — Wizard-based experience for loading user-created templates.
 * Step 1: Review rules (toggle on/off), Step 2: Preview & Apply.
 */
const UserTemplateWizard = ({ template, onApply, onClose }) => {
  const [activeStep, setActiveStep] = useState(0);
  const [selectedRules, setSelectedRules] = useState(() =>
    (template.rules || []).map(() => true)
  );
  const [showCode, setShowCode] = useState(false);

  const steps = ['Review Rules', 'Preview & Apply'];
  const rules = template.rules || [];
  const selectedCount = selectedRules.filter(Boolean).length;

  const combinedCode = rules
    .filter((_, i) => selectedRules[i])
    .map(r => r.generatedCode || '')
    .filter(Boolean)
    .join('\n\n');

  const handleApplyClick = () => {
    const filteredRules = rules.filter((_, i) => selectedRules[i]);
    // Schedules saved in the template are always applied — they have no
    // toggle in this wizard. Forwarding them is what lets the load path
    // recreate them in Rule Manager with their original priorities.
    const filteredSchedules = template.schedules || [];
    onApply(combinedCode, {
      rules: filteredRules,
      schedules: filteredSchedules,
      templateId: template.id,
    });
  };

  return (
    <Dialog open maxWidth="md" fullWidth PaperProps={{ sx: { height: '85vh' } }}>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Users size={22} color="#FF9800" />
          <Box>
            <Typography variant="h5">{template.name}</Typography>
            <Typography variant="caption" color="text.secondary">
              {template.description || 'User created template'}
            </Typography>
          </Box>
          <Chip label={template.category || 'User Created'} size="small"
            sx={{ ml: 'auto', bgcolor: '#FFF3E0', color: '#FF9800' }} />
        </Box>
      </DialogTitle>

      <DialogContent sx={{ display: 'flex', flexDirection: 'column', p: 3 }}>
        <Stepper activeStep={activeStep} sx={{ mb: 3 }}>
          {steps.map(label => (
            <Step key={label}><StepLabel>{label}</StepLabel></Step>
          ))}
        </Stepper>

        <Box sx={{ flex: 1, overflowY: 'auto' }}>
          {activeStep === 0 && (
            <Box>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Review the rules in this template. Toggle off any rules you don't need.
              </Typography>
              {rules.map((rule, idx) => {
                const meta = RULE_TYPE_META_WIZ[rule.ruleType] || RULE_TYPE_META_WIZ.simple_calc;
                const RuleIcon = meta.icon;
                return (
                  <Card key={idx} sx={{ mb: 1.5, opacity: selectedRules[idx] ? 1 : 0.5, transition: 'opacity 0.2s' }}>
                    <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                        <Switch checked={selectedRules[idx]}
                          onChange={() => setSelectedRules(prev => prev.map((v, i) => i === idx ? !v : v))}
                          size="small" />
                        <RuleIcon size={18} color={meta.color} />
                        <Box sx={{ flex: 1 }}>
                          <Typography variant="body2" fontWeight={600}>{rule.name}</Typography>
                          <Chip label={meta.label} size="small"
                            sx={{ fontSize: '0.625rem', height: 18, mt: 0.5, bgcolor: `${meta.color}15`, color: meta.color }} />
                        </Box>
                      </Box>
                      {selectedRules[idx] && rule.generatedCode && (
                        <Paper variant="outlined" sx={{ mt: 1.5, p: 1.5, bgcolor: '#F8F9FA', maxHeight: 120, overflow: 'auto' }}>
                          <pre style={{ margin: 0, fontSize: '0.75rem', fontFamily: 'monospace', whiteSpace: 'pre-wrap' }}>
                            {rule.generatedCode}
                          </pre>
                        </Paper>
                      )}
                    </CardContent>
                  </Card>
                );
              })}
            </Box>
          )}

          {activeStep === 1 && (
            <Box>
              <Alert severity="success" sx={{ mb: 2 }}>
                {selectedCount} rule{selectedCount !== 1 ? 's' : ''} will be created in Rule Manager and loaded into the editor.
              </Alert>
              <FormControlLabel
                control={<Switch checked={showCode} onChange={(e) => setShowCode(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Show generated logic</Typography>}
              />
              {showCode && (
                <Paper variant="outlined" sx={{ mt: 1, p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 300, overflow: 'auto' }}>
                  <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
                    {combinedCode}
                  </pre>
                </Paper>
              )}
              <Box sx={{ mt: 2 }}>
                <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>Rules to create:</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {rules.filter((_, i) => selectedRules[i]).map((rule, idx) => (
                    <Chip key={idx} size="small" label={rule.name} icon={<CheckCircle2 size={12} />}
                      sx={{ bgcolor: '#D4EDDA', color: '#155724' }} />
                  ))}
                </Box>
              </Box>
            </Box>
          )}
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2, borderTop: '1px solid #E9ECEF' }}>
        <Button onClick={onClose} color="inherit">Cancel</Button>
        <Box sx={{ flex: 1 }} />
        {activeStep > 0 && (
          <Button onClick={() => setActiveStep(0)} startIcon={<ArrowLeft size={16} />}>Back</Button>
        )}
        {activeStep === 0 && (
          <Button variant="contained" onClick={() => setActiveStep(1)}
            disabled={selectedCount === 0} endIcon={<ArrowRight size={16} />}>
            Next
          </Button>
        )}
        {activeStep === 1 && (
          <Button variant="contained" onClick={handleApplyClick} startIcon={<Play size={16} />}
            disabled={selectedCount === 0}>
            Apply Template
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

// ═══════════════════════════════════════════════════════════════════════
// .fyn Export / Import utilities
// ═══════════════════════════════════════════════════════════════════════
const FYN_VERSION = '1.0';

/** Build and download a .fyn file from a user template object (from API). */
function exportUserTemplateAsFyn(template) {
  const payload = {
    fyn_version: FYN_VERSION,
    type: 'custom',
    exported_at: new Date().toISOString(),
    template: {
      name: template.name,
      description: template.description || '',
      category: template.category || 'User Created',
      rules: template.rules || [],
      schedules: template.schedules || [],
      combinedCode: template.combinedCode || '',
      created_at: template.created_at,
      updated_at: template.updated_at,
    },
  };
  triggerFynDownload(payload, template.name);
}

/** Build and download a .fyn file for a built-in ACCOUNTING_TEMPLATE (uses default config). */
function exportStandardTemplateAsFyn(template) {
  // Build default config from fields
  const config = {};
  template.fields.forEach((f) => {
    config[f.key] = f.default || '';
    config[`${f.key}_source`] = 'value';
    config[`${f.key}_field`] = '';
  });
  template.outputs.forEach((o) => {
    config[`outputs_${o.key}`] = o.default;
    if (o.txnType) config['txn_type'] = o.txnType;
  });

  const code = template.generateDSL(config);
  const { rules } = parseDSLToRules(code, template.title);

  const payload = {
    fyn_version: FYN_VERSION,
    type: 'standard',
    source_standard_id: template.id,
    exported_at: new Date().toISOString(),
    template: {
      name: template.title,
      description: template.description || '',
      category: template.category || 'Standard',
      rules: rules || [],
      combinedCode: code,
      fields: template.fields,       // metadata only for reference
      outputs: template.outputs,
      standard: template.standard || '',
    },
  };
  triggerFynDownload(payload, template.title);
}

function triggerFynDownload(payload, name) {
  const json = JSON.stringify(payload, null, 2);
  const blob = new Blob([json], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${(name || 'template').replace(/[^a-z0-9_\-\s]/gi, '_').trim()}.fyn`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

/** Validate a parsed .fyn object. Returns { valid, error }. */
function validateFynFile(obj) {
  if (!obj || typeof obj !== 'object') return { valid: false, error: 'File is not valid JSON.' };
  if (!obj.fyn_version) return { valid: false, error: 'Missing fyn_version field. This may not be a .fyn file.' };
  if (obj.fyn_version !== FYN_VERSION) return { valid: false, error: `Unsupported version "${obj.fyn_version}". Expected "${FYN_VERSION}".` };
  if (!obj.template) return { valid: false, error: 'Missing template data in file.' };
  if (!obj.template.name) return { valid: false, error: 'Template has no name.' };
  if (!Array.isArray(obj.template.rules)) return { valid: false, error: 'Template rules must be an array.' };
  return { valid: true };
}

/**
 * ImportFynModal — upload & preview a .fyn file, then create a user template.
 */
const ImportFynModal = ({ open, onClose, onImported }) => {
  const [parsed, setParsed] = useState(null);
  const [parseError, setParseError] = useState('');
  const [fileName, setFileName] = useState('');
  const [importName, setImportName] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const fileInputRef = React.useRef(null);

  const reset = () => {
    setParsed(null);
    setParseError('');
    setFileName('');
    setImportName('');
    setSaving(false);
    setSaveError('');
  };

  const handleClose = () => { reset(); onClose(); };

  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setFileName(file.name);
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const obj = JSON.parse(ev.target.result);
        const { valid, error } = validateFynFile(obj);
        if (!valid) { setParseError(error); setParsed(null); return; }
        setParseError('');
        setParsed(obj);
        // Pre-fill import name, appending " (imported)" if it's a standard template
        const baseName = obj.template.name || '';
        setImportName(obj.type === 'standard' ? `${baseName} (imported)` : baseName);
      } catch {
        setParseError('Could not parse file. Make sure it is a valid .fyn JSON file.');
        setParsed(null);
      }
    };
    reader.readAsText(file);
    // Reset input so same file can be re-selected
    e.target.value = '';
  };

  const handleImport = async () => {
    if (!parsed) return;
    setSaving(true);
    setSaveError('');

    const body = {
      name: importName.trim() || parsed.template.name,
      description: parsed.template.description || '',
      category: parsed.template.category || 'User Created',
      rules: parsed.template.rules || [],
      schedules: parsed.template.schedules || [],
      combinedCode: parsed.template.combinedCode || '',
    };

    try {
      const res = await fetch(`${API}/user-templates`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (res.status === 409) {
        const data = await res.json();
        setSaveError(data.detail || 'A template with this name already exists. Please rename it.');
        setSaving(false);
        return;
      }
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setSaveError(data.detail || 'Failed to import template.');
        setSaving(false);
        return;
      }
      onImported();
      handleClose();
    } catch (err) {
      setSaveError(err.message || 'Network error during import.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Upload size={20} color="#5B5FED" />
          <Typography variant="h6">Import Template (.fyn)</Typography>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <input type="file" accept=".fyn,application/json" ref={fileInputRef}
          style={{ display: 'none' }} onChange={handleFileChange} />

        {/* File picker */}
        <Box sx={{ mb: 2 }}>
          <Button variant="outlined" size="small" startIcon={<Upload size={14} />}
            onClick={() => fileInputRef.current?.click()}
            sx={{ textTransform: 'none', borderColor: '#5B5FED', color: '#5B5FED' }}>
            {fileName ? `Change file` : 'Choose .fyn file'}
          </Button>
          {fileName && (
            <Typography variant="caption" color="text.secondary" sx={{ ml: 1.5 }}>
              {fileName}
            </Typography>
          )}
        </Box>

        {/* Parse error */}
        {parseError && (
          <Alert severity="error" icon={<AlertCircle size={16} />} sx={{ mb: 2 }}>
            {parseError}
          </Alert>
        )}

        {/* Preview */}
        {parsed && (
          <Box>
            <Alert severity="info" sx={{ mb: 2, py: 0.5 }}>
              <Typography variant="caption">
                {parsed.type === 'standard'
                  ? 'This is a standard template — it will be imported as an editable copy.'
                  : 'User template ready to import.'}
                {parsed.exported_at && ` Exported: ${new Date(parsed.exported_at).toLocaleString()}`}
              </Typography>
            </Alert>

            {/* Editable name */}
            <TextField fullWidth size="small" label="Template Name" value={importName}
              onChange={(e) => setImportName(e.target.value)}
              sx={{ mb: 2 }} required />

            {/* Summary chips */}
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, mb: 2 }}>
              <Chip size="small" label={parsed.template.category || 'User Created'}
                sx={{ bgcolor: '#EEF0FE', color: '#5B5FED', fontSize: '0.6875rem', height: 20 }} />
              <Chip size="small" label={`${(parsed.template.rules || []).length} rules`}
                sx={{ bgcolor: '#F8F9FA', fontSize: '0.6875rem', height: 20 }} />
              {parsed.template.standard && (
                <Chip size="small" label={parsed.template.standard}
                  sx={{ bgcolor: '#EEF0FE', color: '#5B5FED', fontSize: '0.6875rem', height: 20 }} />
              )}
            </Box>

            {/* Description */}
            {parsed.template.description && (
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
                {parsed.template.description}
              </Typography>
            )}

            {/* Rules list */}
            {(parsed.template.rules || []).length > 0 && (
              <Box>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                  Rules included:
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {parsed.template.rules.map((r, i) => (
                    <Chip key={i} size="small" label={r.name || `Rule ${i + 1}`}
                      icon={<CheckCircle2 size={11} />}
                      sx={{ bgcolor: '#D4EDDA', color: '#155724', fontSize: '0.6875rem', height: 20 }} />
                  ))}
                </Box>
              </Box>
            )}

            {/* Parameters list (for standard templates) */}
            {parsed.type === 'standard' && Array.isArray(parsed.template.fields) && parsed.template.fields.length > 0 && (
              <Box sx={{ mt: 1.5 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                  Parameters ({parsed.template.fields.length}):
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {parsed.template.fields.map((f) => (
                    <Chip key={f.key} size="small" label={f.label}
                      sx={{ bgcolor: '#F8F9FA', fontSize: '0.6875rem', height: 20 }} />
                  ))}
                </Box>
              </Box>
            )}

            {/* Save error */}
            {saveError && (
              <Alert severity="error" icon={<AlertCircle size={16} />} sx={{ mt: 2 }}>
                {saveError}
              </Alert>
            )}
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={handleClose} color="inherit">Cancel</Button>
        <Button variant="contained" onClick={handleImport}
          disabled={!parsed || !importName.trim() || saving}
          startIcon={saving ? <CircularProgress size={14} color="inherit" /> : <Upload size={14} />}
          sx={{ textTransform: 'none' }}>
          {saving ? 'Importing…' : 'Import Template'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

/**
/**
 * TemplateLibrary — Browse standard and user-created accounting templates.
 */
const TemplateLibrary = ({ events, onLoadTemplate, onClose, inline }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [activeTemplate, setActiveTemplate] = useState(null);
  const [activeUserTemplate, setActiveUserTemplate] = useState(null);
  const [userTemplates, setUserTemplates] = useState([]);
  const [loadingUser, setLoadingUser] = useState(true);
  const [deletingId, setDeletingId] = useState(null);
  const [section, setSection] = useState('standard'); // 'standard' | 'user'
  const [showImport, setShowImport] = useState(false);

  // Fetch user templates
  const loadUserTemplates = useCallback(async () => {
    setLoadingUser(true);
    try {
      const res = await fetch(`${API}/user-templates`);
      const data = await res.json();
      setUserTemplates(Array.isArray(data) ? data : []);
    } catch { /* ignore */ }
    finally { setLoadingUser(false); }
  }, []);

  useEffect(() => {
    loadUserTemplates();
  }, [loadUserTemplates]);

  const categories = useMemo(() => {
    return ['All', ...new Set(ACCOUNTING_TEMPLATES.map(t => t.category))];
  }, []);

  const userCategories = useMemo(() => {
    return ['All', ...new Set(userTemplates.map(t => t.category || 'User Created'))];
  }, [userTemplates]);

  const filteredTemplates = useMemo(() => {
    return ACCOUNTING_TEMPLATES.filter((t) => {
      const matchesSearch = !searchQuery ||
        t.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
        t.description.toLowerCase().includes(searchQuery.toLowerCase()) ||
        t.standard.toLowerCase().includes(searchQuery.toLowerCase());
      const matchesCat = selectedCategory === 'All' || t.category === selectedCategory;
      return matchesSearch && matchesCat;
    });
  }, [searchQuery, selectedCategory]);

  const filteredUserTemplates = useMemo(() => {
    return userTemplates.filter((t) => {
      const matchesSearch = !searchQuery ||
        t.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        (t.description || '').toLowerCase().includes(searchQuery.toLowerCase()) ||
        (t.category || '').toLowerCase().includes(searchQuery.toLowerCase());
      const matchesCat = selectedCategory === 'All' || (t.category || 'User Created') === selectedCategory;
      return matchesSearch && matchesCat;
    });
  }, [searchQuery, selectedCategory, userTemplates]);

  const handleGenerate = useCallback((code, metadata) => {
    onLoadTemplate(code, metadata);
    setActiveTemplate(null);
    onClose();
  }, [onLoadTemplate, onClose]);

  const handleDeleteUserTemplate = useCallback(async (id) => {
    setDeletingId(id);
    try {
      await fetch(`${API}/user-templates/${id}`, { method: 'DELETE' });
      setUserTemplates(prev => prev.filter(t => t.id !== id));
      // Clear the saved template id from localStorage so Rule Manager doesn't
      // try to overwrite this deleted template on the next bookmark save.
      try {
        if (localStorage.getItem('savedRulesTemplateId') === String(id)) {
          localStorage.removeItem('savedRulesTemplateId');
        }
      } catch { /* ignore */ }
    } catch { /* ignore */ }
    finally { setDeletingId(null); }
  }, []);

  if (activeUserTemplate) {
    return (
      <UserTemplateWizard
        template={activeUserTemplate}
        onApply={(code, metadata) => {
          onLoadTemplate(code, metadata);
          onClose();
        }}
        onClose={() => setActiveUserTemplate(null)}
      />
    );
  }

  if (activeTemplate) {
    return (
      <TemplateWizard
        template={activeTemplate}
        events={events}
        onGenerate={handleGenerate}
        onClose={() => setActiveTemplate(null)}
      />
    );
  }

  const content = (
    <>
      {/* Import modal */}
      <ImportFynModal open={showImport} onClose={() => setShowImport(false)}
        onImported={() => { loadUserTemplates(); setSection('user'); }} />

      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, ...(inline ? { px: 2, py: 1.5, borderBottom: '1px solid #E9ECEF' } : {}) }}>
        <BookOpen size={24} color="#5B5FED" />
        <Box sx={{ flex: 1 }}>
          <Typography variant={inline ? "h6" : "h4"}>Accounting Templates</Typography>
          <Typography variant="body2" color="text.secondary">
            Pre-built and user-created calculation templates
          </Typography>
        </Box>
        <Tooltip title="Import a .fyn template file">
          <Button size="small" variant="outlined" startIcon={<Upload size={14} />}
            onClick={() => setShowImport(true)}
            sx={{ textTransform: 'none', borderColor: '#5B5FED', color: '#5B5FED', flexShrink: 0 }}>
            Import
          </Button>
        </Tooltip>
        {!inline && (
          <IconButton onClick={onClose} sx={{ alignSelf: 'flex-start' }}>
            <X size={20} />
          </IconButton>
        )}
      </Box>

      <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, p: inline ? 2 : 3, overflow: 'auto' }}>
        {/* Section Toggle */}
        <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
          <Button
            variant={section === 'standard' ? 'contained' : 'outlined'}
            size="small"
            startIcon={<FileText size={14} />}
            onClick={() => { setSection('standard'); setSelectedCategory('All'); }}
            sx={{ textTransform: 'none', ...(section === 'standard' ? {} : { borderColor: '#CED4DA', color: '#495057' }) }}
          >
            Standard Templates ({ACCOUNTING_TEMPLATES.length})
          </Button>
          <Button
            variant={section === 'user' ? 'contained' : 'outlined'}
            size="small"
            startIcon={<Users size={14} />}
            onClick={() => { setSection('user'); setSelectedCategory('All'); }}
            sx={{ textTransform: 'none', ...(section === 'user' ? {} : { borderColor: '#CED4DA', color: '#495057' }) }}
          >
            User Created Templates ({userTemplates.length})
          </Button>
        </Box>

        <Box sx={{ mb: 2 }}>
          <TextField
            placeholder={section === 'standard' ? "Search templates by name, description, or standard..." : "Search user templates by name or description..."}
            value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)}
            fullWidth size="small"
            InputProps={{ startAdornment: <InputAdornment position="start"><Search size={16} color="#6C757D" /></InputAdornment> }}
            sx={{ mb: 1.5 }}
          />
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75 }}>
            {(section === 'standard' ? categories : userCategories).map((cat) => (
              <Chip key={cat} label={cat} onClick={() => setSelectedCategory(cat)} size="small"
                sx={{
                  cursor: 'pointer',
                  bgcolor: selectedCategory === cat ? '#EEF0FE' : '#FFFFFF',
                  color: selectedCategory === cat ? '#5B5FED' : '#6C757D',
                  border: selectedCategory === cat ? '1px solid #5B5FED' : '1px solid #E9ECEF',
                }} />
            ))}
          </Box>
        </Box>

        <Box sx={{ flex: 1, overflowY: 'auto' }}>
          {/* Standard Templates */}
          {section === 'standard' && (
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' }, gap: 2 }}>
              {filteredTemplates.map((template) => {
                const Icon = ICON_MAP[template.icon] || Settings2;
                return (
                  <Card key={template.id} sx={{ cursor: 'pointer', '&:hover': { borderColor: '#5B5FED' } }}
                    onClick={() => setActiveTemplate(template)}>
                    <CardContent sx={{ p: 2.5 }}>
                      <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5, mb: 1.5 }}>
                        <Box sx={{ p: 1, bgcolor: '#EEF0FE', borderRadius: 1.5, display: 'flex' }}>
                          <Icon size={20} color="#5B5FED" />
                        </Box>
                        <Box sx={{ flex: 1, minWidth: 0 }}>
                          <Typography variant="h6" sx={{ mb: 0.25 }}>{template.title}</Typography>
                          <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.4 }}>
                            {template.description}
                          </Typography>
                        </Box>
                        <Tooltip title="Export as .fyn file">
                          <IconButton size="small"
                            onClick={(e) => { e.stopPropagation(); exportStandardTemplateAsFyn(template); }}
                            sx={{ color: '#5B5FED', flexShrink: 0 }}>
                            <Download size={16} />
                          </IconButton>
                        </Tooltip>
                      </Box>
                      <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap' }}>
                        <Chip label={template.category} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#F8F9FA' }} />
                        {template.standard && (
                          <Chip label={template.standard} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#EEF0FE', color: '#5B5FED' }} />
                        )}
                        <Chip label={`${template.fields.length} parameters`} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#F8F9FA' }} />
                      </Box>
                    </CardContent>
                  </Card>
                );
              })}
              {filteredTemplates.length === 0 && (
                <Typography variant="body2" color="text.secondary" sx={{ py: 4, textAlign: 'center', gridColumn: '1 / -1' }}>
                  No matching standard templates found.
                </Typography>
              )}
            </Box>
          )}

          {/* User Created Templates */}
          {section === 'user' && (
            <>
              {loadingUser ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}><CircularProgress size={32} /></Box>
              ) : filteredUserTemplates.length === 0 ? (
                <Box sx={{ textAlign: 'center', py: 6, color: 'text.secondary' }}>
                  <Users size={40} style={{ margin: '0 auto 12px', opacity: 0.3 }} />
                  <Typography variant="body1" fontWeight={500}>No user templates yet</Typography>
                  <Typography variant="body2" sx={{ mt: 0.5 }}>
                    Go to Rule Manager and use the bookmark icon to save your rules as a template.
                  </Typography>
                </Box>
              ) : (
                <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' }, gap: 2 }}>
                  {filteredUserTemplates.map((template) => (
                    <Card key={template.id} sx={{ cursor: 'pointer', '&:hover': { borderColor: '#FF9800' } }}
                      onClick={() => setActiveUserTemplate(template)}>
                      <CardContent sx={{ p: 2.5 }}>
                        <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5, mb: 1.5 }}>
                          <Box sx={{ p: 1, bgcolor: '#FFF3E0', borderRadius: 1.5, display: 'flex' }}>
                            <Users size={20} color="#FF9800" />
                          </Box>
                          <Box sx={{ flex: 1, minWidth: 0 }}>
                            <Typography variant="h6" sx={{ mb: 0.25 }} noWrap>{template.name}</Typography>
                            <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.4 }}>
                              {template.description || 'No description'}
                            </Typography>
                          </Box>
                          <Tooltip title="Export as .fyn file">
                            <IconButton size="small"
                              onClick={(e) => { e.stopPropagation(); exportUserTemplateAsFyn(template); }}
                              sx={{ color: '#5B5FED', flexShrink: 0 }}>
                              <Download size={16} />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Delete template">
                            <IconButton size="small"
                              onClick={(e) => { e.stopPropagation(); handleDeleteUserTemplate(template.id); }}
                              disabled={deletingId === template.id}
                              sx={{ color: '#F44336', flexShrink: 0 }}>
                              {deletingId === template.id ? <CircularProgress size={14} /> : <Trash2 size={16} />}
                            </IconButton>
                          </Tooltip>
                        </Box>
                        <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap' }}>
                          <Chip label={template.category || 'User Created'} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#FFF3E0', color: '#FF9800' }} />
                          <Chip label={`${(template.rules || []).length} rules`} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#F8F9FA' }} />
                          {template.created_at && (
                            <Chip label={new Date(template.created_at).toLocaleDateString()} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#F8F9FA' }} />
                          )}
                        </Box>
                      </CardContent>
                    </Card>
                  ))}
                </Box>
              )}
            </>
          )}
        </Box>
      </Box>
    </>
  );

  if (inline) {
    return content;
  }

  return (
    <Dialog open={true} onClose={onClose} maxWidth="lg" fullWidth PaperProps={{ sx: { height: '85vh', display: 'flex', flexDirection: 'column' } }}>
      {content}
    </Dialog>
  );
};

export default TemplateLibrary;
