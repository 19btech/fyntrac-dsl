import React, { useState, useMemo, useCallback, useEffect, useRef } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  ToggleButtonGroup, ToggleButton, CircularProgress, Dialog, DialogTitle, DialogContent, DialogActions,
  Autocomplete,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, Play, Calendar, Save, X,
  Table as TableIcon, BarChart3, Filter as FilterIcon, Sigma, ListOrdered, ChevronsDown, ChevronsUp,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";
import TestResultCard from "./TestResultCard";

const FREQUENCY_OPTIONS = [
  { value: 'M', label: 'Monthly', description: '12 periods per year' },
  { value: 'Q', label: 'Quarterly', description: '4 periods per year' },
  { value: 'S', label: 'Semi-Annual', description: '2 periods per year' },
  { value: 'A', label: 'Annual', description: '1 period per year' },
];

// Tiny inline helper used inside Select MenuItems for the "Add Output" dropdown.
const ListItemIconLike = ({ color, children }) => (
  <Box sx={{ width: 26, height: 26, mr: 1.25, borderRadius: 1, flexShrink: 0,
    bgcolor: `${color}1A`, display: 'flex', alignItems: 'center', justifyContent: 'center', color }}>
    {children}
  </Box>
);

// ─── Column Card (reused from ScheduleBuilder) ───────────────────────
const ColumnCard = ({ column, index, events, variables, onUpdate, onRemove, onMoveUp, onMoveDown, isFirst, isLast, onTest }) => {
  const [colTesting, setColTesting] = useState(false);
  const [colTestResult, setColTestResult] = useState(null);

  return (
    <Card sx={{ mb: 1, borderLeft: `3px solid ${column.formula === 'period_date' ? '#2196F3' : '#4CAF50'}` }}>
      <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.25, pt: 0.5, cursor: 'grab' }}>
            <IconButton size="small" onClick={() => onMoveUp(index)} disabled={isFirst}><ArrowUp size={12} /></IconButton>
            <IconButton size="small" onClick={() => onMoveDown(index)} disabled={isLast}><ArrowDown size={12} /></IconButton>
          </Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Box sx={{ display: 'flex', gap: 1, mb: 0.5 }}>
              <TextField size="small" label="Column Name"
                value={column.name}
                onChange={(e) => onUpdate(index, { ...column, name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') })}
                sx={{ flex: '0 0 160px' }}
                placeholder="e.g., interest" />
              <Box sx={{ flex: 1 }}>
                <FormulaBar
                  value={column.formula}
                  onChange={(val) => onUpdate(index, { ...column, formula: val })}
                  events={events}
                  variables={variables}
                  label="Column Formula"
                  placeholder="e.g., multiply(opening_bal, rate)"
                />
              </Box>
            </Box>
            {column.formula && column.formula.includes('lag(') && (
              <Alert severity="info" sx={{ py: 0, px: 1, fontSize: '0.6875rem', '& .MuiAlert-message': { py: 0.25 } }}>
                References previous row — will use default value for first period
              </Alert>
            )}
            {colTestResult && (
              <Alert severity={colTestResult.success ? 'success' : 'error'} sx={{ mt: 0.5, '& .MuiAlert-message': { width: '100%' } }}
                onClose={() => setColTestResult(null)}>
                {colTestResult.success ? (
                  <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap', maxHeight: 100, overflow: 'auto' }}>
                    {colTestResult.output}
                  </Typography>
                ) : (
                  <Typography variant="body2">{colTestResult.error}</Typography>
                )}
              </Alert>
            )}
          </Box>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.25, pt: 0.5 }}>
            <Tooltip title="Test schedule up to this column">
              <IconButton size="small" onClick={async () => {
                if (!column.name || !onTest) return;
                setColTesting(true);
                setColTestResult(null);
                try {
                  const result = await onTest(index);
                  setColTestResult(result);
                } catch (e) {
                  setColTestResult({ success: false, error: e.message });
                } finally {
                  setColTesting(false);
                }
              }} disabled={colTesting || !column.name} sx={{ color: '#4CAF50' }}>
                {colTesting ? <CircularProgress size={14} /> : <Play size={14} />}
              </IconButton>
            </Tooltip>
            <IconButton size="small" onClick={() => onRemove(index)} sx={{ color: '#F44336' }}>
              <Trash2 size={14} />
            </IconButton>
          </Box>
        </Box>
      </CardContent>
    </Card>
  );
};


/**
 * ScheduleStepModal — Full-screen modal for configuring a schedule step
 * inside the Rule Builder. Shows Time Period config, schedule columns,
 * preview, and output options (no rule name/priority/createTransaction).
 */
const ScheduleStepModal = ({ open, step, onClose, onSaveStep, events, dslFunctions, definedVarNames, currentRulePreStepCode, freshPriorCode, testPostingDate }) => {
  const cfg = step?.scheduleConfig || {};

  // Period config
  const [periodType, setPeriodType] = useState(cfg.periodType || 'date');
  const [startDate, setStartDate] = useState(cfg.startDate || '');
  const [startDateSource, setStartDateSource] = useState(cfg.startDateSource || 'value');
  const [startDateField, setStartDateField] = useState(cfg.startDateField || '');
  const [startDateFormula, setStartDateFormula] = useState(cfg.startDateFormula || '');
  const [endDate, setEndDate] = useState(cfg.endDate || '');
  const [endDateSource, setEndDateSource] = useState(cfg.endDateSource || 'value');
  const [endDateField, setEndDateField] = useState(cfg.endDateField || '');
  const [endDateFormula, setEndDateFormula] = useState(cfg.endDateFormula || '');
  const [periodCount, setPeriodCount] = useState(cfg.periodCount || '12');
  const [periodCountSource, setPeriodCountSource] = useState(cfg.periodCountSource || 'value');
  const [periodCountField, setPeriodCountField] = useState(cfg.periodCountField || '');
  const [periodCountFormula, setPeriodCountFormula] = useState(cfg.periodCountFormula || '');
  const [frequency, setFrequency] = useState(cfg.frequency || 'M');
  const [convention, setConvention] = useState(cfg.convention || '');
  const [columns, setColumns] = useState(cfg.columns?.length ? cfg.columns : [{ name: 'date', formula: 'period_date' }]);
  const [stepName, setStepName] = useState(step?.name || '');

  // Output options — unified list. Each entry has a stable id and a `type`
  // (first|last|sum|column|filter). Users can add unlimited outputs of any type.
  // Migration: when opening a step saved with the legacy enableSum/enableFilter/etc
  // toggles, we synthesize one entry per enabled toggle so nothing is lost.
  const _migrateLegacyOutputs = useCallback((c, savedOutputVars) => {
    // Newest schema: scheduleConfig.outputs (the unified array we now persist).
    if (Array.isArray(c.outputs) && c.outputs.length > 0) {
      return c.outputs.map((o, i) => ({ ...o, id: o.id || `o_${Date.now()}_${i}` }));
    }
    // Newer schema: step.outputVars array.
    if (Array.isArray(savedOutputVars) && savedOutputVars.length > 0) {
      return savedOutputVars.map((o, i) => ({
        id: `o_${Date.now()}_${i}`,
        type: o.type === 'column' ? 'column' : o.type,
        name: o.name || '',
        column: o.column || '',
        matchCol: o.matchCol || '',
        matchValue: o.matchValue != null ? String(o.matchValue) : '',
      }));
    }
    // Migrate from legacy toggle-based config.
    const out = [];
    let i = 0;
    if (c.extractFirst && c.extractColumn) out.push({ id: `o_${Date.now()}_${i++}`, type: 'first', name: c.firstVarName || `first_${c.extractColumn}`, column: c.extractColumn });
    if (c.extractLast && c.extractColumn) out.push({ id: `o_${Date.now()}_${i++}`, type: 'last', name: c.lastVarName || `last_${c.extractColumn}`, column: c.extractColumn });
    if (c.enableSum && c.sumVarName && c.sumColumn) out.push({ id: `o_${Date.now()}_${i++}`, type: 'sum', name: c.sumVarName, column: c.sumColumn });
    if (c.enableCol && c.colVarName && c.colColumn) out.push({ id: `o_${Date.now()}_${i++}`, type: 'column', name: c.colVarName, column: c.colColumn });
    if (c.enableFilter && c.filterVarName) {
      out.push({ id: `o_${Date.now()}_${i++}`, type: 'filter', name: c.filterVarName,
        column: c.filterReturnCol || '', matchCol: c.filterMatchCol || '', matchValue: c.filterMatchValue || '' });
    }
    return out;
  }, []);

  const [outputs, setOutputs] = useState(() => _migrateLegacyOutputs(cfg, step?.outputVars));

  // Step-level options
  const [localInlineComment, setLocalInlineComment] = useState(step?.inlineComment || false);
  const [localCommentText, setLocalCommentText] = useState(step?.commentText || '');
  const [localPrintResult, setLocalPrintResult] = useState(step?.printResult !== undefined ? step.printResult : true);
  const [showCode, setShowCode] = useState(false);

  // Preview
  const [schedulePreviewTesting, setSchedulePreviewTesting] = useState(false);
  const [schedulePreviewData, setSchedulePreviewData] = useState(null);
  const [schedulePreviewError, setSchedulePreviewError] = useState(null);
  const [previewSelectedSubId, setPreviewSelectedSubId] = useState('__all__');
  const [outputTests, setOutputTests] = useState({});

  // Saved rules vars for context detection
  const [savedRulesVarNames, setSavedRulesVarNames] = useState([]);
  const [savedRulesVars, setSavedRulesVars] = useState([]);
  const [priorRulesCode, setPriorRulesCode] = useState('');
  // Latest freshPriorCode value, accessible from the saved-rules fetch effect
  // without making it a dep (which would re-trigger the fetch).
  const freshPriorCodeRef = useRef(freshPriorCode);
  useEffect(() => { freshPriorCodeRef.current = freshPriorCode; }, [freshPriorCode]);

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API}/saved-rules`);
        if (!res.ok) return;
        const data = await res.json();
        const rules = (Array.isArray(data) ? data : (data.rules || []))
          .sort((a, b) => (a.priority ?? Infinity) - (b.priority ?? Infinity));
        const names = new Set();
        const allVars = [];
        rules.forEach(r => {
          (r.variables || []).forEach(v => { if (v.name) { names.add(v.name); allVars.push(v); } });
          if (r.ruleType === 'iteration') {
            const iters = r.iterations?.length ? r.iterations : (r.iterConfig?.type ? [r.iterConfig] : []);
            for (const iter of iters) {
              const rv = iter.resultVar;
              if (rv && !names.has(rv)) {
                names.add(rv);
                const codeLine = (r.generatedCode || '').split('\n').find(l => l.trim().startsWith(rv + ' ='));
                const formula = codeLine ? codeLine.trim().replace(new RegExp('^' + rv + '\\s*=\\s*'), '') : rv;
                allVars.push({ name: rv, source: 'formula', formula, value: '', eventField: '', collectType: 'collect_by_instrument', _isIterResult: true });
              }
            }
          }
        });
        setSavedRulesVarNames([...names]);
        setSavedRulesVars(allVars);
        // Build prior code from saved rules, stripping print/createTransaction side effects.
        // Also strip the "## Dependencies from saved rules" section from all rules except the
        // first — later rules re-emit prior-rule variables in potentially wrong order (stale
        // generatedCode), which overwrites the correct values from earlier rules.
        const stripDeps = (code) => {
          const out = []; let skip = false;
          for (const line of code.split('\n')) {
            const t = line.trim();
            if (t === '## Dependencies from saved rules') { skip = true; out.push(line); continue; }
            if (skip && t.startsWith('## ') && !t.startsWith('## \u2550')) { skip = false; }
            if (!skip) out.push(line);
          }
          return out.join('\n');
        };
        // Sanitize legacy/broken `X = schedule(p, {...}, {... "X": X ...})`
        // emitted by older versions of this modal. Without this strip, the
        // saved code triggers a Python UnboundLocalError when re-executed.
        const stripSelfRefSchedules = (code) => (code || '').split('\n').map(line => {
          const m = line.match(/^(\s*)([A-Za-z_]\w*)\s*=\s*schedule\(/);
          if (!m) return line;
          const name = m[2];
          const re = new RegExp(`(,\\s*)?"${name}"\\s*:\\s*${name}\\b(\\s*,)?`, 'g');
          return line.replace(re, (match, leadingComma, trailingComma) => {
            if (leadingComma && trailingComma) return ',';
            return '';
          });
        }).join('\n');
        const priorLines = rules
          .map((r, idx) => idx === 0 ? (r.generatedCode || '') : stripDeps(r.generatedCode || ''))
          .map(stripSelfRefSchedules)
          .filter(Boolean).join('\n\n')
          .split('\n')
          .filter(l => {
            const t = l.trim();
            return t && !t.startsWith('print(') && !t.startsWith('print (') && !t.startsWith('createTransaction(');
          })
          .join('\n');
        // Only apply this fallback if the parent hasn't already given us
        // freshly-generated (and properly filtered) prior code. Otherwise this
        // fetch — which doesn't exclude the current rule and races the
        // freshPriorCode effect — could overwrite the good code with a stale
        // copy that still contains the current rule's own definitions.
        if (freshPriorCodeRef.current == null) {
          setPriorRulesCode(priorLines);
        }
      } catch { /* ignore */ }
    })();
  }, []);

  // When parent provides freshly-generated prior code (correctly ordered), prefer it
  // over the stale generatedCode fetched above (which may have ordering bugs).
  useEffect(() => {
    if (freshPriorCode != null) setPriorRulesCode(freshPriorCode);
  }, [freshPriorCode]);

  // Reset state when step changes
  useEffect(() => {
    if (!open) return;
    const c = step?.scheduleConfig || {};
    setStepName(step?.name || '');
    setPeriodType(c.periodType || 'date');
    setStartDate(c.startDate || '');
    setStartDateSource(c.startDateSource || 'value');
    setStartDateField(c.startDateField || '');
    setStartDateFormula(c.startDateFormula || '');
    setEndDate(c.endDate || '');
    setEndDateSource(c.endDateSource || 'value');
    setEndDateField(c.endDateField || '');
    setEndDateFormula(c.endDateFormula || '');
    setPeriodCount(c.periodCount || '12');
    setPeriodCountSource(c.periodCountSource || 'value');
    setPeriodCountField(c.periodCountField || '');
    setPeriodCountFormula(c.periodCountFormula || '');
    setFrequency(c.frequency || 'M');
    setConvention(c.convention || '');
    setColumns(c.columns?.length ? c.columns : [{ name: 'date', formula: 'period_date' }]);
    setOutputs(_migrateLegacyOutputs(c, step?.outputVars));
    setLocalInlineComment(step?.inlineComment || false);
    setLocalCommentText(step?.commentText || '');
    setLocalPrintResult(step?.printResult !== undefined ? step.printResult : true);
    setShowCode(false);
    setSchedulePreviewData(null);
    setSchedulePreviewError(null);
    setPreviewSelectedSubId('__all__');
    setOutputTests({});
  }, [open, step]);

  // All var names (parent-defined + saved rules)
  const allVarNames = useMemo(() => [...new Set([...(definedVarNames || []), ...savedRulesVarNames])], [definedVarNames, savedRulesVarNames]);

  const allEventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  const dateEventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.filter(f => f.datatype === 'date' || f.name.includes('date')).forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  const SCHEDULE_BUILTINS = useMemo(() => new Set([
    'period_date', 'period_index', 'period_start', 'period_number', 'dcf', 'lag',
    'days_in_current_period', 'total_periods', 'daily_basis', 'item_name',
    'subinstrument_id', 's_no', 'index', 'start_date', 'end_date',
    ...(dslFunctions || []).map(f => f.name),
  ]), [dslFunctions]);

  const autoDetectedVars = useMemo(() => {
    const colNames = new Set(columns.filter(c => c.name).map(c => c.name));
    const savedVarNameSet = new Set(savedRulesVarNames);
    const externalRefs = new Set();
    if (cfg.contextVars) cfg.contextVars.forEach(v => externalRefs.add(v));
    for (const col of columns) {
      if (!col.formula) continue;
      const identifiers = col.formula.match(/[a-zA-Z_][a-zA-Z0-9_]*/g) || [];
      for (const rawId of identifiers) {
        // The schedule engine auto-exposes every context array as `<name>_full`
        // inside column expressions. Resolve to the base name so we pass the
        // underlying array (e.g. ExpectedCF) into context, not the synthesized
        // alias (which doesn't exist in the outer scope).
        const id = rawId.endsWith('_full') ? rawId.slice(0, -5) : rawId;
        if (!id) continue;
        if (SCHEDULE_BUILTINS.has(id)) continue;
        if (savedVarNameSet.has(id)) { externalRefs.add(id); continue; }
        if (colNames.has(id)) continue;
        externalRefs.add(id);
      }
    }
    // Never reference the step's own variable name as context — it is being
    // assigned by THIS step (sched = schedule(...)) and would cause a Python
    // UnboundLocalError when emitted as `{"Schedule": Schedule}` because the
    // wrapper function sees a later `Schedule = ...` and treats it as local.
    if (stepName) externalRefs.delete(stepName);
    return [...externalRefs];
  }, [columns, SCHEDULE_BUILTINS, savedRulesVarNames, cfg.contextVars, stepName]);

  // Filter value options for schedule_filter
  const filterValueOptions = useMemo(() => {
    const opts = [];
    opts.push({ label: 'postingdate', group: 'Built-in' });
    opts.push({ label: 'effectivedate', group: 'Built-in' });
    savedRulesVarNames.forEach(v => opts.push({ label: v, group: 'Defined Variable' }));
    if (events?.length) {
      events.forEach(ev => {
        (ev.fields || []).forEach(f => {
          opts.push({ label: `${ev.event_name}.${f.name}`, group: `Event: ${ev.event_name}` });
        });
      });
    }
    return opts;
  }, [savedRulesVarNames, events]);

  // Column CRUD
  const addColumn = useCallback(() => setColumns(prev => [...prev, { name: '', formula: '' }]), []);
  const updateColumn = useCallback((index, updated) => setColumns(prev => prev.map((c, i) => i === index ? updated : c)), []);
  const removeColumn = useCallback((index) => setColumns(prev => prev.filter((_, i) => i !== index)), []);
  const moveColumn = useCallback((index, direction) => {
    setColumns(prev => {
      const arr = [...prev];
      const target = index + direction;
      if (target < 0 || target >= arr.length) return arr;
      [arr[index], arr[target]] = [arr[target], arr[index]];
      return arr;
    });
  }, []);

  // Build schedule-only DSL code (for testing)
  const buildScheduleCode = useCallback(() => {
    const lines = [];
    if (periodType === 'number') {
      const countExpr = periodCountSource === 'field' && periodCountField ? periodCountField
        : periodCountSource === 'formula' && periodCountFormula ? periodCountFormula
        : (periodCount || 12);
      lines.push(`p = period(${countExpr})`);
    } else {
      const startExpr = startDateSource === 'field' && startDateField ? startDateField
        : startDateSource === 'formula' && startDateFormula ? startDateFormula
        : `"${startDate || '2026-01-01'}"`;
      const endExpr = endDateSource === 'field' && endDateField ? endDateField
        : endDateSource === 'formula' && endDateFormula ? endDateFormula
        : `"${endDate || '2026-12-31'}"`;
      let periodCall = `p = period(${startExpr}, ${endExpr}, "${frequency}"`;
      if (convention) periodCall += `, "${convention}"`;
      periodCall += ')';
      lines.push(periodCall);
    }
    const validCols = columns.filter(c => c.name && c.formula);
    lines.push('sched = schedule(p, {');
    validCols.forEach((col, idx) => {
      const comma = idx < validCols.length - 1 ? ',' : '';
      lines.push(`    "${col.name}": "${col.formula}"${comma}`);
    });
    const contextPairs = autoDetectedVars.map(v => `"${v}": ${v}`);
    // Always include instrumentid so schedule result rows carry it for the composite-ID dropdown
    if (!autoDetectedVars.includes('instrumentid')) contextPairs.push('"instrumentid": instrumentid');
    if (contextPairs.length > 0) lines.push(`}, {${contextPairs.join(', ')}})`);
    else lines.push('})');
    lines.push('print(sched)');
    return lines.join('\n');
  }, [periodType, periodCount, periodCountSource, periodCountField, periodCountFormula,
      startDate, startDateSource, startDateField, startDateFormula,
      endDate, endDateSource, endDateField, endDateFormula,
      frequency, convention, columns, autoDetectedVars]);

  // Test column
  const testColumn = useCallback(async (colIndex) => {
    const colsToTest = columns.slice(0, colIndex + 1).filter(c => c.name && c.formula);
    if (colsToTest.length === 0) return { success: false, error: 'No valid columns to test' };
    const schedLines = [];
    if (periodType === 'number') {
      const countExpr = periodCountSource === 'field' && periodCountField ? periodCountField
        : periodCountSource === 'formula' && periodCountFormula ? periodCountFormula
        : (periodCount || 12);
      schedLines.push(`p = period(${countExpr})`);
    } else {
      const startExpr = startDateSource === 'field' && startDateField ? startDateField
        : startDateSource === 'formula' && startDateFormula ? startDateFormula
        : `"${startDate || '2026-01-01'}"`;
      const endExpr = endDateSource === 'field' && endDateField ? endDateField
        : endDateSource === 'formula' && endDateFormula ? endDateFormula
        : `"${endDate || '2026-12-31'}"`;
      let periodCall = `p = period(${startExpr}, ${endExpr}, "${frequency}"`;
      if (convention) periodCall += `, "${convention}"`;
      periodCall += ')';
      schedLines.push(periodCall);
    }
    schedLines.push('sched = schedule(p, {');
    colsToTest.forEach((col, idx) => {
      const comma = idx < colsToTest.length - 1 ? ',' : '';
      schedLines.push(`    "${col.name}": "${col.formula}"${comma}`);
    });
    const contextPairs = autoDetectedVars.map(v => `"${v}": ${v}`);
    if (contextPairs.length > 0) schedLines.push(`}, {${contextPairs.join(', ')}})`);
    else schedLines.push('})');
    schedLines.push('print(sched)');
    const allPriorCode = [priorRulesCode, currentRulePreStepCode].filter(Boolean).join('\n\n');
    const combinedCode = [allPriorCode, ...schedLines].filter(Boolean).join('\n');
    let postingDate = testPostingDate || new Date().toISOString().split('T')[0];
    if (!testPostingDate) {
      try {
        const pdRes = await fetch(`${API}/event-data/posting-dates`);
        const pdData = await pdRes.json();
        if (pdData?.posting_dates?.length) postingDate = pdData.posting_dates[0];
      } catch { /* ignore */ }
    }
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: combinedCode, posting_date: postingDate }),
    });
    const data = await response.json();
    if (response.ok && data.success) {
      return { success: true, output: (data.print_outputs || []).map(String).join('\n') || 'OK' };
    }
    return { success: false, error: data.error || data.detail || 'Execution failed' };
  }, [columns, autoDetectedVars, priorRulesCode, currentRulePreStepCode, periodType, periodCount, periodCountSource, periodCountField, periodCountFormula,
      startDateSource, startDateField, startDateFormula, startDate,
      endDateSource, endDateField, endDateFormula, endDate, frequency, convention, testPostingDate]);

  // Test schedule preview
  const testSchedulePreview = useCallback(async () => {
    setSchedulePreviewTesting(true);
    setSchedulePreviewData(null);
    setSchedulePreviewError(null);
    try {
      const validCols = columns.filter(c => c.name && c.formula);
      if (validCols.length === 0) { setSchedulePreviewError('No valid columns defined yet.'); return; }
      const schedCode = buildScheduleCode();
      const allPriorCode = [priorRulesCode, currentRulePreStepCode].filter(Boolean).join('\n\n');
      const combinedCode = allPriorCode ? (allPriorCode + '\n\n' + schedCode) : schedCode;
      let postingDate = testPostingDate;
      if (!postingDate) {
        let dates = [];
        try {
          const pdRes = await fetch(`${API}/event-data/posting-dates`);
          const pdData = await pdRes.json();
          dates = pdData?.posting_dates || [];
        } catch { /* ignore */ }
        postingDate = dates[0] || new Date().toISOString().split('T')[0];
      }
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: combinedCode, posting_date: postingDate }),
      });
      const data = await response.json();
      if (response.ok && data.success && data.print_outputs?.length > 0) {
        try {
          // Use the last print output (the schedule print), prior code may have earlier prints
          let parsed = JSON.parse(data.print_outputs[data.print_outputs.length - 1]);
          if (Array.isArray(parsed) && Array.isArray(parsed[0])) parsed = parsed[0];
          if (Array.isArray(parsed) && parsed[0]?.schedule) {
            parsed = parsed.flatMap(item => {
              const sid = item.subinstrument_id;
              const iid = item.instrumentid ?? '';
              const compositeId = iid ? `${iid}_${sid ?? ''}` : String(sid ?? '');
              return (item.schedule || []).map(row => sid != null ? { _composite_id: compositeId, subinstrument_id: sid, ...row } : row);
            });
          }
          if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object' && !Array.isArray(parsed[0])) {
            // Sort by subinstrument_id (numeric if possible), then by period_date
            parsed.sort((a, b) => {
              const sa = a.subinstrument_id ?? '';
              const sb = b.subinstrument_id ?? '';
              const na = parseFloat(sa), nb = parseFloat(sb);
              const sidCmp = (!isNaN(na) && !isNaN(nb)) ? na - nb : String(sa).localeCompare(String(sb));
              if (sidCmp !== 0) return sidCmp;
              return String(a.period_date ?? '').localeCompare(String(b.period_date ?? ''));
            });
            setSchedulePreviewData(parsed);
          } else {
            setSchedulePreviewError('Schedule ran but returned no rows.');
          }
        } catch { setSchedulePreviewError('Ran successfully but output was not parseable JSON.'); }
      } else {
        setSchedulePreviewError(data.error || data.detail || 'Execution failed');
      }
    } catch (err) { setSchedulePreviewError(err.message || 'Network error'); }
    finally { setSchedulePreviewTesting(false); }
  }, [buildScheduleCode, priorRulesCode, currentRulePreStepCode, columns, testPostingDate]);

  // Test a single output entry by id (runs schedule + the one output's DSL line)
  const testOutputEntry = useCallback(async (entryId) => {
    setOutputTests(prev => ({ ...prev, [entryId]: { testing: true, result: null, error: null } }));
    const setResult = (result, error) =>
      setOutputTests(prev => ({ ...prev, [entryId]: { testing: false, result, error } }));
    const entry = outputs.find(o => o.id === entryId);
    if (!entry) { setResult(null, 'Output not found.'); return; }
    if (!entry.name) { setResult(null, 'Variable name is required.'); return; }
    const extraLines = [];
    try {
      switch (entry.type) {
        case 'first':
          if (!entry.column) { setResult(null, 'Pick a column.'); return; }
          extraLines.push(`${entry.name} = schedule_first(sched, "${entry.column}")`);
          break;
        case 'last':
          if (!entry.column) { setResult(null, 'Pick a column.'); return; }
          extraLines.push(`${entry.name} = schedule_last(sched, "${entry.column}")`);
          break;
        case 'sum':
          if (!entry.column) { setResult(null, 'Pick a column.'); return; }
          extraLines.push(`${entry.name} = schedule_sum(sched, "${entry.column}")`);
          break;
        case 'column':
          if (!entry.column) { setResult(null, 'Pick a column.'); return; }
          extraLines.push(`${entry.name} = schedule_column(sched, "${entry.column}")`);
          break;
        case 'filter':
          if (!entry.matchCol || !entry.matchValue || !entry.column) { setResult(null, 'Fill in all filter fields.'); return; }
          extraLines.push(`${entry.name} = schedule_filter(sched, "${entry.matchCol}", ${entry.matchValue}, "${entry.column}")`);
          break;
        default: setResult(null, 'Unknown output type.'); return;
      }
      // Mirror per-step test output: tag each print with the row's instrument id
      // (and sub-instrument for sorting only) so TestResultCard can render a
      // per-instrument table identical to the inline step tests.
      // Standalone DSL (no event refs) falls back to a plain print to avoid
      // referencing instrumentid/subinstrumentid which won't be defined.
      const _hasEventRefs = (code) => /\b[A-Z][A-Z0-9_]*\.[a-zA-Z_]\w*/.test(code || '');
      // Drop the trailing `print(sched)` that buildScheduleCode appends — we
      // only want the marker print for the output variable. Otherwise the raw
      // schedule dump (item_index, item_name, ...) leaks into the result row.
      const schedCode = buildScheduleCode()
        .split('\n')
        .filter(l => l.trim() !== 'print(sched)')
        .join('\n');
      const allPriorCode = [priorRulesCode, currentRulePreStepCode].filter(Boolean).join('\n\n');
      const printLine = _hasEventRefs([allPriorCode, schedCode, ...extraLines].join('\n'))
        ? `print("__TEST_ROW__|" + str(instrumentid) + "|" + str(subinstrumentid) + "| ${entry.name} =", ${entry.name})`
        : `print("${entry.name} =", ${entry.name})`;
      extraLines.push(printLine);
      const combinedCode = [allPriorCode, schedCode, ...extraLines].filter(Boolean).join('\n\n');
      let postingDate = testPostingDate || new Date().toISOString().split('T')[0];
      if (!testPostingDate) {
        try {
          const pdRes = await fetch(`${API}/event-data/posting-dates`);
          const pdData = await pdRes.json();
          if (pdData?.posting_dates?.length) postingDate = pdData.posting_dates[0];
        } catch { /* ignore */ }
      }
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: combinedCode, posting_date: postingDate }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const allPrints = data.print_outputs || [];
        // Return ALL prints joined so the card can group __TEST_ROW__ markers
        // by instrument (matches inline-step test behavior).
        const out = allPrints.length > 0 ? allPrints.map(String).join('\n') : 'Ran (no output)';
        setResult(String(out), null);
      } else {
        setResult(null, data.error || data.detail || 'Execution failed');
      }
    } catch (err) { setResult(null, err.message); }
  }, [outputs, priorRulesCode, currentRulePreStepCode, buildScheduleCode, testPostingDate]);

  const previewHeaders = useMemo(() => columns.filter(c => c.name).map(c => c.name), [columns]);

  // Collect all output variable names — emitted to the saved step's `outputVars`
  // array, which is what AccountingRuleBuilder.buildScheduleStepLines iterates
  // when generating the rule's runtime DSL.
  const collectOutputVars = useCallback(() => {
    return outputs
      .filter(o => o.name && (o.type === 'filter'
        ? (o.matchCol && o.matchValue && o.column)
        : !!o.column))
      .map(o => o.type === 'filter'
        ? { name: o.name, type: 'filter', column: o.column, matchCol: o.matchCol, matchValue: o.matchValue }
        : { name: o.name, type: o.type, column: o.column });
  }, [outputs]);

  const handleSave = () => {
    if (!stepName) return;
    onSaveStep({
      name: stepName,
      stepType: 'schedule',
      inlineComment: localInlineComment,
      commentText: localCommentText,
      printResult: localPrintResult,
      scheduleConfig: {
        periodType, startDate, startDateSource, startDateField, startDateFormula,
        endDate, endDateSource, endDateField, endDateFormula,
        periodCount, periodCountSource, periodCountField, periodCountFormula,
        frequency, convention, columns,
        // Persist the unified outputs array on the config too so re-opens fully
        // round-trip (parent only reads outputVars for code generation).
        outputs,
        contextVars: autoDetectedVars,
      },
      outputVars: collectOutputVars(),
    });
    onClose();
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth
      PaperProps={{ sx: { maxHeight: '90vh', height: '90vh' } }}>
      <DialogTitle sx={{ pb: 1, borderBottom: '1px solid #E9ECEF' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1 }}>
          <TableIcon size={20} color="#9C27B0" />
          <Typography variant="h6" sx={{ flex: 1 }}>{step?.name ? `Edit Schedule Step: ${step.name}` : 'Add Schedule Step'}</Typography>
          <IconButton size="small" onClick={onClose} sx={{ color: '#6C757D' }}><X size={18} /></IconButton>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 2, overflow: 'auto' }}>
        {/* Step Name */}
        <TextField size="small" fullWidth label="Variable Name *" value={stepName}
          onChange={(e) => setStepName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
          placeholder="e.g., loan_schedule" sx={{ mb: 2, mt: 1 }} />

        {/* ── Time Period ── */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
          <Calendar size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
          Time Period
        </Typography>
        <Box sx={{ display: 'flex', gap: 0.5, mb: 1.5 }}>
          <ToggleButtonGroup size="small" exclusive value={periodType}
            onChange={(e, v) => { if (v) setPeriodType(v); }}
            sx={{ '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1.5, py: 0.25 } }}>
            <ToggleButton value="date">Date Range</ToggleButton>
            <ToggleButton value="number">Number of Periods</ToggleButton>
          </ToggleButtonGroup>
        </Box>

        {periodType === 'number' ? (
          <Box sx={{ mb: 2.5 }}>
            <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>Number of Periods</Typography>
            <Box sx={{ display: 'flex', gap: 0.5, mb: 0.5 }}>
              <ToggleButtonGroup size="small" exclusive value={periodCountSource}
                onChange={(e, v) => { if (v) setPeriodCountSource(v); }}
                sx={{ '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1, py: 0.25 } }}>
                <ToggleButton value="value">Fixed Value</ToggleButton>
                <ToggleButton value="field">Event Field</ToggleButton>
                <ToggleButton value="formula">Formula</ToggleButton>
              </ToggleButtonGroup>
            </Box>
            {periodCountSource === 'value' && (
              <TextField size="small" type="number" value={periodCount}
                onChange={(e) => setPeriodCount(e.target.value)} sx={{ width: 220 }}
                placeholder="e.g., 12" helperText="e.g., 12 for monthly, 60 for 5 years" />
            )}
            {periodCountSource === 'field' && (
              <FormControl size="small" sx={{ width: 280 }}>
                <InputLabel shrink>Event Field</InputLabel>
                <Select value={periodCountField} label="Event Field"
                  onChange={(e) => setPeriodCountField(e.target.value)} notched displayEmpty
                  renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                  {allEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                </Select>
              </FormControl>
            )}
            {periodCountSource === 'formula' && (
              <Box sx={{ maxWidth: 400 }}>
                <FormulaBar value={periodCountFormula} onChange={setPeriodCountFormula}
                  events={events} variables={allVarNames}
                  label="Period Count Formula" placeholder="e.g., multiply(years, 12)" />
              </Box>
            )}
          </Box>
        ) : (
          <>
            <Box sx={{ display: 'flex', gap: 1.5, mb: 1.5 }}>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>Start Date</Typography>
                <Box sx={{ display: 'flex', gap: 0.5, mb: 0.5, flexWrap: 'wrap' }}>
                  <ToggleButtonGroup size="small" exclusive value={startDateSource}
                    onChange={(e, v) => { if (v) setStartDateSource(v); }}
                    sx={{ flexWrap: 'wrap', '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1, py: 0.25 } }}>
                    <ToggleButton value="value">Fixed</ToggleButton>
                    <ToggleButton value="field">Event Field</ToggleButton>
                    <ToggleButton value="formula">Formula</ToggleButton>
                  </ToggleButtonGroup>
                </Box>
                {startDateSource === 'value' && (
                  <TextField size="small" fullWidth type="date" value={startDate}
                    onChange={(e) => setStartDate(e.target.value)} />
                )}
                {startDateSource === 'field' && (
                  <FormControl size="small" fullWidth>
                    <Select value={startDateField} onChange={(e) => setStartDateField(e.target.value)}
                      displayEmpty renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {startDateSource === 'formula' && (
                  <Box sx={{ mt: 1 }}>
                    <FormulaBar value={startDateFormula} onChange={setStartDateFormula}
                      events={events} variables={allVarNames}
                      placeholder="e.g., add_months(effectivedate, 12)" />
                  </Box>
                )}
              </Box>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>End Date</Typography>
                <Box sx={{ display: 'flex', gap: 0.5, mb: 0.5, flexWrap: 'wrap' }}>
                  <ToggleButtonGroup size="small" exclusive value={endDateSource}
                    onChange={(e, v) => { if (v) setEndDateSource(v); }}
                    sx={{ flexWrap: 'wrap', '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1, py: 0.25 } }}>
                    <ToggleButton value="value">Fixed</ToggleButton>
                    <ToggleButton value="field">Event Field</ToggleButton>
                    <ToggleButton value="formula">Formula</ToggleButton>
                  </ToggleButtonGroup>
                </Box>
                {endDateSource === 'value' && (
                  <TextField size="small" fullWidth type="date" value={endDate}
                    onChange={(e) => setEndDate(e.target.value)} />
                )}
                {endDateSource === 'field' && (
                  <FormControl size="small" fullWidth>
                    <Select value={endDateField} onChange={(e) => setEndDateField(e.target.value)}
                      displayEmpty renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {endDateSource === 'formula' && (
                  <Box sx={{ mt: 1 }}>
                    <FormulaBar value={endDateFormula} onChange={setEndDateFormula}
                      events={events} variables={allVarNames}
                      placeholder="e.g., add_months(effectivedate, 60)" />
                  </Box>
                )}
              </Box>
            </Box>
          </>
        )}

        <Box sx={{ display: 'flex', gap: 1.5, mb: 2.5 }}>
          {periodType === 'date' && (
            <FormControl size="small" sx={{ minWidth: 140 }}>
              <InputLabel>Frequency</InputLabel>
              <Select value={frequency} label="Frequency" onChange={(e) => setFrequency(e.target.value)}>
                {FREQUENCY_OPTIONS.map(f => <MenuItem key={f.value} value={f.value}>{f.label}</MenuItem>)}
              </Select>
            </FormControl>
          )}
          {periodType === 'date' && (
            <FormControl size="small" sx={{ minWidth: 160 }}>
              <InputLabel>Day Count Convention</InputLabel>
              <Select value={convention} label="Day Count Convention" onChange={(e) => setConvention(e.target.value)}>
                <MenuItem value="">None (default)</MenuItem>
                <MenuItem value="act/360">ACT/360</MenuItem>
                <MenuItem value="act/365">ACT/365</MenuItem>
                <MenuItem value="30/360">30/360</MenuItem>
                <MenuItem value="act/act">ACT/ACT</MenuItem>
              </Select>
            </FormControl>
          )}
        </Box>

        <Divider sx={{ mb: 2 }} />

        {/* ── Schedule Columns ── */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="body2" fontWeight={600}>Schedule Columns ({columns.length})</Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={addColumn}>Custom Column</Button>
        </Box>
        <Box sx={{ mb: 1.5, px: 1.5, py: 1, bgcolor: '#F0F4FF', borderRadius: 1, border: '1px solid #E0E7FF' }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5, fontWeight: 600, fontSize: '0.7rem' }}>
            Built-in variables you can use in column formulas:
          </Typography>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
            {[
              { name: 'period_date', tip: 'Current period date' },
              { name: 'period_index', tip: 'Period number (0, 1, 2…)' },
              { name: 'total_periods', tip: 'Total number of periods' },
              { name: 'subinstrument_id', tip: 'Current sub-instrument ID' },
              { name: 'item_name', tip: 'Current item name' },
              { name: 'start_date', tip: 'Schedule start date' },
              { name: 'end_date', tip: 'Schedule end date' },
              { name: 'dcf', tip: 'Day count fraction' },
              { name: 'lag(col, n)', tip: 'Previous row value' },
            ].map(v => (
              <Tooltip key={v.name} title={v.tip} arrow>
                <Chip label={v.name} size="small"
                  sx={{ fontSize: '0.675rem', height: 20, bgcolor: '#fff', border: '1px solid #C7D2FE',
                    fontFamily: 'monospace', cursor: 'help', '&:hover': { bgcolor: '#EEF2FF' } }} />
              </Tooltip>
            ))}
          </Box>
        </Box>

        {columns.map((col, idx) => (
          <ColumnCard key={idx} column={col} index={idx} events={events}
            variables={[...new Set([...autoDetectedVars, ...allVarNames])]}
            onUpdate={updateColumn} onRemove={removeColumn}
            onMoveUp={() => moveColumn(idx, -1)} onMoveDown={() => moveColumn(idx, 1)}
            isFirst={idx === 0} isLast={idx === columns.length - 1}
            onTest={testColumn} />
        ))}

        {/* ── Schedule Preview ── */}
        {previewHeaders.length > 0 && (
          <>
            <Divider sx={{ my: 2 }} />
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="body2" fontWeight={600}>
                <BarChart3 size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
                Schedule Preview
              </Typography>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                {schedulePreviewData && (() => {
                  const compositeIds = [...new Set(schedulePreviewData.map(r => r._composite_id ?? String(r.subinstrument_id ?? '')).filter(Boolean))].sort((a, b) => {
                    const na = parseFloat(a), nb = parseFloat(b);
                    return (!isNaN(na) && !isNaN(nb)) ? na - nb : String(a).localeCompare(String(b));
                  });
                  if (compositeIds.length <= 1) return null;
                  return (
                    <FormControl size="small" sx={{ minWidth: 160 }}>
                      <Select
                        value={previewSelectedSubId}
                        onChange={e => setPreviewSelectedSubId(e.target.value)}
                        displayEmpty
                        sx={{ fontSize: '0.75rem', height: 28 }}
                      >
                        <MenuItem value="__all__" sx={{ fontSize: '0.75rem' }}>All Subinstruments</MenuItem>
                        {compositeIds.map(cid => (
                          <MenuItem key={cid} value={cid} sx={{ fontSize: '0.75rem' }}>{cid}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  );
                })()}
                {schedulePreviewData && (
                  <Chip label={`${
                    previewSelectedSubId === '__all__'
                      ? schedulePreviewData.length
                      : schedulePreviewData.filter(r => (r._composite_id ?? String(r.subinstrument_id ?? '')) === previewSelectedSubId).length
                  } rows`} size="small"
                    sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#EEF0FE', color: '#5B5FED' }} />
                )}
                <Tooltip title="Run schedule and show real data">
                  <Button size="small" variant="outlined"
                    startIcon={schedulePreviewTesting ? <CircularProgress size={12} /> : <Play size={12} />}
                    onClick={testSchedulePreview} disabled={schedulePreviewTesting}
                    sx={{ borderColor: '#4CAF50', color: '#4CAF50', fontSize: '0.7rem', py: 0.25, px: 1,
                      '&:hover': { borderColor: '#388E3C', bgcolor: '#E8F5E9' } }}>
                    {schedulePreviewTesting ? 'Running...' : 'Test'}
                  </Button>
                </Tooltip>
              </Box>
            </Box>
            {schedulePreviewError && (
              <Alert severity="error" sx={{ mb: 1, fontSize: '0.8125rem' }} onClose={() => setSchedulePreviewError(null)}>
                {schedulePreviewError}
              </Alert>
            )}
            <TableContainer component={Paper} variant="outlined" sx={{ mb: 2, maxHeight: 280, overflow: 'auto' }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow sx={{ bgcolor: '#F8F9FA' }}>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: '#F8F9FA' }}>#</TableCell>
                    {(schedulePreviewData
                      ? (() => {
                          const keys = Object.keys(schedulePreviewData[0] || {}).filter(k => k !== '_composite_id');
                          const idx = keys.indexOf('subinstrument_id');
                          if (idx > 0) { keys.splice(idx, 1); keys.unshift('subinstrument_id'); }
                          return keys;
                        })()
                      : previewHeaders
                    ).map(h => (
                      <TableCell key={h} sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: '#F8F9FA', whiteSpace: 'nowrap' }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schedulePreviewData ? (
                    (() => {
                      const filtered = previewSelectedSubId === '__all__'
                        ? schedulePreviewData
                        : schedulePreviewData.filter(r => (r._composite_id ?? String(r.subinstrument_id ?? '')) === previewSelectedSubId);
                      return filtered.slice(0, 20).map((row, rowIdx) => (
                      <TableRow key={rowIdx} hover sx={{ '&:last-child td': { borderBottom: 0 } }}>
                        <TableCell sx={{ fontSize: '0.75rem', color: '#6C757D' }}>{rowIdx + 1}</TableCell>
                        {(() => {
                          const keys = Object.keys(row).filter(k => k !== '_composite_id');
                          const idx = keys.indexOf('subinstrument_id');
                          if (idx > 0) { keys.splice(idx, 1); keys.unshift('subinstrument_id'); }
                          return keys;
                        })().map((k, ci) => {
                          const val = row[k];
                          return (
                          <TableCell key={ci} sx={{ fontSize: '0.75rem',
                            fontFamily: typeof val === 'number' ? 'monospace' : 'inherit',
                            fontWeight: typeof val === 'number' ? 500 : 400 }}>
                            {typeof val === 'number'
                              ? (Number.isInteger(val) ? val.toLocaleString() : val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 }))
                              : String(val ?? '—')}
                          </TableCell>
                          );
                        })}
                      </TableRow>
                    ));
                    })()
                  ) : (
                    [1, 2, 3].map(row => (
                      <TableRow key={row} sx={{ '&:last-child td': { borderBottom: 0 } }}>
                        <TableCell sx={{ fontSize: '0.75rem', color: '#6C757D' }}>{row}</TableCell>
                        {previewHeaders.map(h => (
                          <TableCell key={h} sx={{ fontSize: '0.75rem', color: '#ADB5BD', fontStyle: 'italic' }}>
                            {h === 'date' || h.includes('date') ? '2026-01-31' : '...'}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
            {schedulePreviewData && (() => {
              const filtered = previewSelectedSubId === '__all__'
                ? schedulePreviewData
                : schedulePreviewData.filter(r => (r._composite_id ?? String(r.subinstrument_id ?? '')) === previewSelectedSubId);
              return filtered.length > 20 ? (
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                  Showing 20 of {filtered.length} rows
                </Typography>
              ) : null;
            })()}
          </>
        )}

        <Divider sx={{ my: 2 }} />

        {/* ── Output Options (unified, multi-add) ────────────────────────
            Each output produces one variable usable by later rule steps.
            Users can add unlimited outputs of any type.
        */}
        {(() => {
          const OUTPUT_TYPES = [
            { key: 'first',  label: 'First Value',   icon: ChevronsUp,    color: '#9C27B0', desc: 'First non-null value of a column', fn: 'schedule_first' },
            { key: 'last',   label: 'Last Value',    icon: ChevronsDown,  color: '#673AB7', desc: 'Last non-null value of a column',  fn: 'schedule_last' },
            { key: 'sum',    label: 'Sum',           icon: Sigma,         color: '#FF9800', desc: 'Total of a numeric column',         fn: 'schedule_sum' },
            { key: 'column', label: 'Column Array',  icon: ListOrdered,   color: '#00BCD4', desc: 'All values of a column as a list',  fn: 'schedule_column' },
            { key: 'filter', label: 'Filter Lookup', icon: FilterIcon,    color: '#2196F3', desc: 'Find a row matching a condition, return one column', fn: 'schedule_filter' },
          ];
          const TYPE_BY_KEY = OUTPUT_TYPES.reduce((m, t) => { m[t.key] = t; return m; }, {});
          const COL_OPTIONS_NO_DATE = columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number');
          const COL_OPTIONS_ALL = columns.filter(c => c.name);

          const addOutput = (type) => setOutputs(prev => [...prev, {
            id: `o_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
            type, name: '', column: '', matchCol: '', matchValue: '',
          }]);
          const updateOutput = (id, patch) => setOutputs(prev => prev.map(o => o.id === id ? { ...o, ...patch } : o));
          const removeOutput = (id) => setOutputs(prev => prev.filter(o => o.id !== id));

          return (
            <>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
                <Box>
                  <Typography variant="body2" fontWeight={600}>
                    Output Variables ({outputs.length})
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Variables extracted from this schedule that other steps & rules can reference
                  </Typography>
                </Box>
                <FormControl size="small" sx={{ minWidth: 190 }}>
                  <Select
                    value=""
                    displayEmpty
                    onChange={(e) => { if (e.target.value) addOutput(e.target.value); }}
                    renderValue={() => (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, color: '#5B5FED', fontWeight: 600 }}>
                        <Plus size={14} /> Add Output
                      </Box>
                    )}
                    sx={{ bgcolor: '#EEF0FE', '& .MuiOutlinedInput-notchedOutline': { borderColor: '#5B5FED' } }}
                  >
                    {OUTPUT_TYPES.map(t => {
                      const Icon = t.icon;
                      return (
                        <MenuItem key={t.key} value={t.key}>
                          <ListItemIconLike color={t.color}><Icon size={16} /></ListItemIconLike>
                          <Box>
                            <Typography variant="body2" fontWeight={600}>{t.label}</Typography>
                            <Typography variant="caption" color="text.secondary">{t.desc}</Typography>
                          </Box>
                        </MenuItem>
                      );
                    })}
                  </Select>
                </FormControl>
              </Box>

              {outputs.length === 0 && (
                <Box sx={{ p: 3, textAlign: 'center', bgcolor: '#F8F9FA', borderRadius: 1, border: '1px dashed #DEE2E6', mb: 1 }}>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                    No output variables defined yet
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Use <strong>Add Output</strong> above to extract values from the schedule (sum, filter, first/last, etc.)
                  </Typography>
                </Box>
              )}

              {outputs.map((o) => {
                const meta = TYPE_BY_KEY[o.type] || OUTPUT_TYPES[0];
                const Icon = meta.icon;
                const test = outputTests[o.id] || {};
                const colOpts = (o.type === 'column' || o.type === 'filter') ? COL_OPTIONS_ALL : COL_OPTIONS_NO_DATE;
                return (
                  <Card key={o.id} sx={{ mb: 1, borderLeft: `3px solid ${meta.color}` }}>
                    <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                        <Box sx={{ width: 28, height: 28, borderRadius: 1, bgcolor: `${meta.color}1A`, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                          <Icon size={14} color={meta.color} />
                        </Box>
                        <Box sx={{ flex: 1 }}>
                          <Typography variant="body2" fontWeight={600} sx={{ color: meta.color }}>{meta.label}</Typography>
                          <Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'monospace', fontSize: '0.65rem' }}>{meta.fn}(…)</Typography>
                        </Box>
                        <Tooltip title="Test this output">
                          <span>
                            <IconButton size="small" onClick={() => testOutputEntry(o.id)} disabled={!o.name || test.testing}
                              sx={{ color: '#4CAF50' }}>
                              {test.testing ? <CircularProgress size={14} /> : <Play size={14} />}
                            </IconButton>
                          </span>
                        </Tooltip>
                        <Tooltip title="Remove output">
                          <IconButton size="small" onClick={() => removeOutput(o.id)} sx={{ color: '#F44336' }}>
                            <Trash2 size={14} />
                          </IconButton>
                        </Tooltip>
                      </Box>

                      {/* Fields row — varies by type */}
                      {o.type === 'filter' ? (
                        <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 1 }}>
                          <TextField size="small" label="Variable Name *"
                            value={o.name}
                            onChange={(e) => updateOutput(o.id, { name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') })}
                            placeholder="e.g., matched_revenue" />
                          <FormControl size="small">
                            <InputLabel>Match Column *</InputLabel>
                            <Select value={o.matchCol || ''} label="Match Column *"
                              onChange={(e) => updateOutput(o.id, { matchCol: e.target.value })}>
                              {COL_OPTIONS_ALL.map(c => <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>)}
                            </Select>
                          </FormControl>
                          <Autocomplete
                            freeSolo size="small"
                            options={filterValueOptions}
                            groupBy={(opt) => opt.group}
                            getOptionLabel={(opt) => (typeof opt === 'string' ? opt : opt.label)}
                            value={o.matchValue || ''}
                            onChange={(_, v) => updateOutput(o.id, { matchValue: v == null ? '' : (typeof v === 'string' ? v : v.label) })}
                            onInputChange={(_, v, reason) => { if (reason === 'input') updateOutput(o.id, { matchValue: v }); }}
                            renderInput={(params) => (
                              <TextField {...params} label="Match Value *" placeholder="postingdate"
                                InputProps={{ ...params.InputProps, sx: { fontFamily: 'monospace', fontSize: '0.8125rem' } }} />
                            )}
                          />
                          <FormControl size="small">
                            <InputLabel>Return Column *</InputLabel>
                            <Select value={o.column || ''} label="Return Column *"
                              onChange={(e) => updateOutput(o.id, { column: e.target.value })}>
                              {COL_OPTIONS_NO_DATE.map(c => <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>)}
                            </Select>
                          </FormControl>
                        </Box>
                      ) : (
                        <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1 }}>
                          <TextField size="small" label="Variable Name *"
                            value={o.name}
                            onChange={(e) => updateOutput(o.id, { name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') })}
                            placeholder={o.type === 'first' ? 'first_revenue' : o.type === 'last' ? 'last_balance' : o.type === 'sum' ? 'total_revenue' : 'revenue_array'} />
                          <FormControl size="small">
                            <InputLabel>Column *</InputLabel>
                            <Select value={o.column || ''} label="Column *"
                              onChange={(e) => updateOutput(o.id, { column: e.target.value })}>
                              {colOpts.map(c => <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>)}
                            </Select>
                          </FormControl>
                        </Box>
                      )}

                      {(test.result || test.error) && (
                        <Box sx={{ mt: 1 }}>
                          <TestResultCard
                            success={!!test.result}
                            output={test.result}
                            error={test.error}
                            variableName={o.name}
                            onClose={() => setOutputTests(p => ({ ...p, [o.id]: { ...p[o.id], result: null, error: null } }))}
                          />
                        </Box>
                      )}
                    </CardContent>
                  </Card>
                );
              })}
            </>
          );
        })()}

        {/* Step-level options */}
        <Divider sx={{ my: 2 }} />
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: localInlineComment ? 1 : 0 }}>
              <Typography variant="body2">Inline comment</Typography>
              <Switch checked={localInlineComment} onChange={(e) => setLocalInlineComment(e.target.checked)} size="small" />
            </Box>
            {localInlineComment && (
              <TextField size="small" fullWidth multiline minRows={2} maxRows={4} label="Description"
                placeholder="Describe what this step does — will appear as ## comment"
                value={localCommentText} onChange={(e) => setLocalCommentText(e.target.value)} />
            )}
          </CardContent>
        </Card>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 }, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="body2">Print Results for Preview</Typography>
            <Switch checked={localPrintResult} onChange={(e) => setLocalPrintResult(e.target.checked)} size="small" />
          </CardContent>
        </Card>

        {/* Show generated logic */}
        <FormControlLabel
          control={<Switch checked={showCode} onChange={(e) => setShowCode(e.target.checked)} size="small" />}
          label={<Typography variant="body2" fontWeight={500}>Show generated logic</Typography>}
        />
        {showCode && (
          <Paper variant="outlined" sx={{ mt: 1, p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 200, overflow: 'auto' }}>
            <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
              {buildScheduleCode()}
            </pre>
          </Paper>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleSave} disabled={!stepName} variant="contained" startIcon={<Save size={14} />}>
          Save Step
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ScheduleStepModal;
