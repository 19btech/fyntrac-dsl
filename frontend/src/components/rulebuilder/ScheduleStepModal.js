import React, { useState, useMemo, useCallback, useEffect } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  ToggleButtonGroup, ToggleButton, CircularProgress, Dialog, DialogTitle, DialogContent, DialogActions,
  Autocomplete,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, Play, Calendar, Save, X,
  Table as TableIcon, BarChart3,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";

const FREQUENCY_OPTIONS = [
  { value: 'M', label: 'Monthly', description: '12 periods per year' },
  { value: 'Q', label: 'Quarterly', description: '4 periods per year' },
  { value: 'S', label: 'Semi-Annual', description: '2 periods per year' },
  { value: 'A', label: 'Annual', description: '1 period per year' },
];

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
const ScheduleStepModal = ({ open, step, onClose, onSaveStep, events, dslFunctions, definedVarNames }) => {
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

  // Output options
  const [extractFirst, setExtractFirst] = useState(cfg.extractFirst || false);
  const [extractLast, setExtractLast] = useState(cfg.extractLast || false);
  const [extractColumn, setExtractColumn] = useState(cfg.extractColumn || '');
  const [firstVarName, setFirstVarName] = useState(cfg.firstVarName || '');
  const [lastVarName, setLastVarName] = useState(cfg.lastVarName || '');
  const [enableSum, setEnableSum] = useState(cfg.enableSum || false);
  const [sumColumn, setSumColumn] = useState(cfg.sumColumn || '');
  const [sumVarName, setSumVarName] = useState(cfg.sumVarName || '');
  const [enableCol, setEnableCol] = useState(cfg.enableCol || false);
  const [colColumn, setColColumn] = useState(cfg.colColumn || '');
  const [colVarName, setColVarName] = useState(cfg.colVarName || '');
  const [enableFilter, setEnableFilter] = useState(cfg.enableFilter || false);
  const [filterVarName, setFilterVarName] = useState(cfg.filterVarName || '');
  const [filterMatchCol, setFilterMatchCol] = useState(cfg.filterMatchCol || '');
  const [filterMatchValue, setFilterMatchValue] = useState(cfg.filterMatchValue || '');
  const [filterReturnCol, setFilterReturnCol] = useState(cfg.filterReturnCol || '');

  // Step-level options
  const [localInlineComment, setLocalInlineComment] = useState(step?.inlineComment || false);
  const [localCommentText, setLocalCommentText] = useState(step?.commentText || '');
  const [localPrintResult, setLocalPrintResult] = useState(step?.printResult !== undefined ? step.printResult : true);
  const [showCode, setShowCode] = useState(false);

  // Preview
  const [schedulePreviewTesting, setSchedulePreviewTesting] = useState(false);
  const [schedulePreviewData, setSchedulePreviewData] = useState(null);
  const [schedulePreviewError, setSchedulePreviewError] = useState(null);
  const [outputTests, setOutputTests] = useState({});

  // Saved rules vars for context detection
  const [savedRulesVarNames, setSavedRulesVarNames] = useState([]);
  const [savedRulesVars, setSavedRulesVars] = useState([]);
  const [priorRulesCode, setPriorRulesCode] = useState('');

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
                allVars.push({ name: rv, source: 'formula', formula, value: '', eventField: '', collectType: 'collect', _isIterResult: true });
              }
            }
          }
        });
        setSavedRulesVarNames([...names]);
        setSavedRulesVars(allVars);
        // Build prior code from saved rules, stripping print/createTransaction side effects
        const priorLines = rules.map(r => r.generatedCode || '').filter(Boolean).join('\n\n')
          .split('\n')
          .filter(l => {
            const t = l.trim();
            return t && !t.startsWith('print(') && !t.startsWith('print (') && !t.startsWith('createTransaction(');
          })
          .join('\n');
        setPriorRulesCode(priorLines);
      } catch { /* ignore */ }
    })();
  }, []);

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
    setExtractFirst(c.extractFirst || false);
    setExtractLast(c.extractLast || false);
    setExtractColumn(c.extractColumn || '');
    setFirstVarName(c.firstVarName || '');
    setLastVarName(c.lastVarName || '');
    setEnableSum(c.enableSum || false);
    setSumColumn(c.sumColumn || '');
    setSumVarName(c.sumVarName || '');
    setEnableCol(c.enableCol || false);
    setColColumn(c.colColumn || '');
    setColVarName(c.colVarName || '');
    setEnableFilter(c.enableFilter || false);
    setFilterVarName(c.filterVarName || '');
    setFilterMatchCol(c.filterMatchCol || '');
    setFilterMatchValue(c.filterMatchValue || '');
    setFilterReturnCol(c.filterReturnCol || '');
    setLocalInlineComment(step?.inlineComment || false);
    setLocalCommentText(step?.commentText || '');
    setLocalPrintResult(step?.printResult !== undefined ? step.printResult : true);
    setShowCode(false);
    setSchedulePreviewData(null);
    setSchedulePreviewError(null);
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
      for (const id of identifiers) {
        if (SCHEDULE_BUILTINS.has(id)) continue;
        if (savedVarNameSet.has(id)) { externalRefs.add(id); continue; }
        if (colNames.has(id)) continue;
        externalRefs.add(id);
      }
    }
    return [...externalRefs];
  }, [columns, SCHEDULE_BUILTINS, savedRulesVarNames, cfg.contextVars]);

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
    const combinedCode = [priorRulesCode, ...schedLines].filter(Boolean).join('\n');
    let postingDate = new Date().toISOString().split('T')[0];
    try {
      const pdRes = await fetch(`${API}/event-data/posting-dates`);
      const pdData = await pdRes.json();
      if (pdData?.posting_dates?.length) postingDate = pdData.posting_dates[0];
    } catch { /* ignore */ }
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
  }, [columns, autoDetectedVars, priorRulesCode, periodType, periodCount, periodCountSource, periodCountField, periodCountFormula,
      startDateSource, startDateField, startDateFormula, startDate,
      endDateSource, endDateField, endDateFormula, endDate, frequency, convention]);

  // Test schedule preview
  const testSchedulePreview = useCallback(async () => {
    setSchedulePreviewTesting(true);
    setSchedulePreviewData(null);
    setSchedulePreviewError(null);
    try {
      const validCols = columns.filter(c => c.name && c.formula);
      if (validCols.length === 0) { setSchedulePreviewError('No valid columns defined yet.'); return; }
      const schedCode = buildScheduleCode();
      const combinedCode = priorRulesCode ? (priorRulesCode + '\n\n' + schedCode) : schedCode;
      let dates = [];
      try {
        const pdRes = await fetch(`${API}/event-data/posting-dates`);
        const pdData = await pdRes.json();
        dates = pdData?.posting_dates || [];
      } catch { /* ignore */ }
      if (dates.length === 0) dates.push(new Date().toISOString().split('T')[0]);
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: combinedCode, posting_date: dates[0] }),
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
              return (item.schedule || []).map(row => sid != null ? { subinstrument_id: sid, ...row } : row);
            });
          }
          if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object' && !Array.isArray(parsed[0])) {
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
  }, [buildScheduleCode, priorRulesCode, columns]);

  // Test output option
  const testOutputOption = useCallback(async (optType) => {
    setOutputTests(prev => ({ ...prev, [optType]: { testing: true, result: null, error: null } }));
    const setResult = (result, error) =>
      setOutputTests(prev => ({ ...prev, [optType]: { testing: false, result, error } }));
    try {
      const extraLines = [];
      switch (optType) {
        case 'first':
          if (!extractColumn) { setResult(null, 'Select a column first.'); return; }
          { const vn = firstVarName || `first_${extractColumn}`;
          extraLines.push(`${vn} = schedule_first(sched, "${extractColumn}")`);
          extraLines.push(`print("${vn}:", ${vn})`); }
          break;
        case 'last':
          if (!extractColumn) { setResult(null, 'Select a column first.'); return; }
          { const vn = lastVarName || `last_${extractColumn}`;
          extraLines.push(`${vn} = schedule_last(sched, "${extractColumn}")`);
          extraLines.push(`print("${vn}:", ${vn})`); }
          break;
        case 'sum':
          if (!sumVarName || !sumColumn) { setResult(null, 'Fill in variable name and column.'); return; }
          extraLines.push(`${sumVarName} = schedule_sum(sched, "${sumColumn}")`);
          extraLines.push(`print("${sumVarName}:", ${sumVarName})`);
          break;
        case 'col':
          if (!colVarName || !colColumn) { setResult(null, 'Fill in variable name and column.'); return; }
          extraLines.push(`${colVarName} = schedule_column(sched, "${colColumn}")`);
          extraLines.push(`print("${colVarName}:", ${colVarName})`);
          break;
        case 'filter':
          if (!filterVarName || !filterMatchCol || !filterMatchValue || !filterReturnCol) {
            setResult(null, 'Fill in all filter fields.'); return;
          }
          extraLines.push(`${filterVarName} = schedule_filter(sched, "${filterMatchCol}", ${filterMatchValue}, "${filterReturnCol}")`);
          extraLines.push(`print("${filterVarName}:", ${filterVarName})`);
          break;
        default: return;
      }
      const schedCode = buildScheduleCode();
      const combinedCode = [priorRulesCode, schedCode, ...extraLines].filter(Boolean).join('\n\n');
      let postingDate = new Date().toISOString().split('T')[0];
      try {
        const pdRes = await fetch(`${API}/event-data/posting-dates`);
        const pdData = await pdRes.json();
        if (pdData?.posting_dates?.length) postingDate = pdData.posting_dates[0];
      } catch { /* ignore */ }
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: combinedCode, posting_date: postingDate }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const allPrints = data.print_outputs || [];
        const out = allPrints.length > 0 ? allPrints[allPrints.length - 1] : 'Ran (no output)';
        setResult(String(out), null);
      } else {
        setResult(null, data.error || data.detail || 'Execution failed');
      }
    } catch (err) { setResult(null, err.message); }
  }, [priorRulesCode, buildScheduleCode, extractColumn, firstVarName, lastVarName, sumVarName, sumColumn, colVarName, colColumn,
      filterVarName, filterMatchCol, filterMatchValue, filterReturnCol]);

  const previewHeaders = useMemo(() => columns.filter(c => c.name).map(c => c.name), [columns]);

  // Collect all output variable names
  const collectOutputVars = useCallback(() => {
    const vars = [];
    if (extractFirst && extractColumn) vars.push({ name: firstVarName || `first_${extractColumn}`, type: 'first', column: extractColumn });
    if (extractLast && extractColumn) vars.push({ name: lastVarName || `last_${extractColumn}`, type: 'last', column: extractColumn });
    if (enableSum && sumVarName && sumColumn) vars.push({ name: sumVarName, type: 'sum', column: sumColumn });
    if (enableCol && colVarName && colColumn) vars.push({ name: colVarName, type: 'column', column: colColumn });
    if (enableFilter && filterVarName && filterMatchCol && filterMatchValue && filterReturnCol) {
      vars.push({ name: filterVarName, type: 'filter', column: filterReturnCol, matchCol: filterMatchCol, matchValue: filterMatchValue });
    }
    return vars;
  }, [extractFirst, extractLast, extractColumn, firstVarName, lastVarName, enableSum, sumVarName, sumColumn,
      enableCol, colVarName, colColumn, enableFilter, filterVarName, filterMatchCol, filterMatchValue, filterReturnCol]);

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
        extractFirst, extractLast, extractColumn,
        firstVarName, lastVarName,
        enableSum, sumColumn, sumVarName,
        enableCol, colColumn, colVarName,
        enableFilter, filterVarName, filterMatchCol, filterMatchValue, filterReturnCol,
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
                  <FormulaBar value={startDateFormula} onChange={setStartDateFormula}
                    events={events} variables={allVarNames}
                    placeholder="e.g., add_months(effectivedate, 12)" />
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
                  <FormulaBar value={endDateFormula} onChange={setEndDateFormula}
                    events={events} variables={allVarNames}
                    placeholder="e.g., add_months(effectivedate, 60)" />
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
                {schedulePreviewData && (
                  <Chip label={`${schedulePreviewData.length} rows`} size="small"
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
                      ? (() => { const keys = Object.keys(schedulePreviewData[0] || {}); const idx = keys.indexOf('subinstrument_id'); if (idx > 0) { keys.splice(idx, 1); keys.unshift('subinstrument_id'); } return keys; })()
                      : previewHeaders
                    ).map(h => (
                      <TableCell key={h} sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: '#F8F9FA', whiteSpace: 'nowrap' }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schedulePreviewData ? (
                    schedulePreviewData.slice(0, 20).map((row, rowIdx) => (
                      <TableRow key={rowIdx} hover sx={{ '&:last-child td': { borderBottom: 0 } }}>
                        <TableCell sx={{ fontSize: '0.75rem', color: '#6C757D' }}>{rowIdx + 1}</TableCell>
                        {(() => { const keys = Object.keys(row); const idx = keys.indexOf('subinstrument_id'); if (idx > 0) { keys.splice(idx, 1); keys.unshift('subinstrument_id'); } return keys; })().map((k, ci) => {
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
                    ))
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
            {schedulePreviewData && schedulePreviewData.length > 20 && (
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                Showing 20 of {schedulePreviewData.length} rows
              </Typography>
            )}
          </>
        )}

        <Divider sx={{ my: 2 }} />

        {/* ── Output Options ── */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1.5 }}>
          Output Options — variables other steps can reference
        </Typography>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap', mb: (extractFirst || extractLast || enableSum || enableCol || enableFilter) ? 1 : 0 }}>
              <FormControlLabel
                control={<Switch checked={extractFirst} onChange={(e) => setExtractFirst(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule First</Typography>} />
              <FormControlLabel
                control={<Switch checked={extractLast} onChange={(e) => setExtractLast(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Last</Typography>} />
              <FormControlLabel
                control={<Switch checked={enableSum} onChange={(e) => setEnableSum(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Sum</Typography>} />
              <FormControlLabel
                control={<Switch checked={enableCol} onChange={(e) => setEnableCol(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Column</Typography>} />
              <FormControlLabel
                control={<Switch checked={enableFilter} onChange={(e) => setEnableFilter(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Filter</Typography>} />
            </Box>

            {(extractFirst || extractLast) && (
              <FormControl size="small" fullWidth sx={{ mb: 0.75 }}>
                <InputLabel>Column to extract (first/last)</InputLabel>
                <Select value={extractColumn} label="Column to extract (first/last)"
                  onChange={(e) => setExtractColumn(e.target.value)}>
                  {columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number').map(c => (
                    <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}

            {(extractFirst || extractLast) && extractColumn && (
              <Box sx={{ display: 'flex', gap: 1, mb: 0.75 }}>
                {extractFirst && (
                  <TextField size="small" label="First Variable Name" value={firstVarName}
                    onChange={(e) => setFirstVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                    placeholder={`first_${extractColumn}`} sx={{ flex: 1 }} />
                )}
                {extractLast && (
                  <TextField size="small" label="Last Variable Name" value={lastVarName}
                    onChange={(e) => setLastVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                    placeholder={`last_${extractColumn}`} sx={{ flex: 1 }} />
                )}
              </Box>
            )}

            {extractColumn && (extractFirst || extractLast) && (
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 1 }}>
                {extractFirst && (
                  <Box sx={{ flex: 1, minWidth: 160 }}>
                    <Button size="small" variant="outlined"
                      startIcon={outputTests.first?.testing ? <CircularProgress size={12} /> : <Play size={12} />}
                      onClick={() => testOutputOption('first')} disabled={outputTests.first?.testing}
                      sx={{ fontSize: '0.7rem', py: 0.25, borderColor: '#4CAF50', color: '#4CAF50' }}>
                      Test First
                    </Button>
                    {outputTests.first?.result && (
                      <Alert severity="success" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                        onClose={() => setOutputTests(p => ({ ...p, first: { ...p.first, result: null } }))}>
                        <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap', maxHeight: 60, overflow: 'auto' }}>{outputTests.first.result}</Typography>
                      </Alert>
                    )}
                    {outputTests.first?.error && (
                      <Alert severity="error" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                        onClose={() => setOutputTests(p => ({ ...p, first: { ...p.first, error: null } }))}>
                        <Typography variant="body2">{outputTests.first.error}</Typography>
                      </Alert>
                    )}
                  </Box>
                )}
                {extractLast && (
                  <Box sx={{ flex: 1, minWidth: 160 }}>
                    <Button size="small" variant="outlined"
                      startIcon={outputTests.last?.testing ? <CircularProgress size={12} /> : <Play size={12} />}
                      onClick={() => testOutputOption('last')} disabled={outputTests.last?.testing}
                      sx={{ fontSize: '0.7rem', py: 0.25, borderColor: '#4CAF50', color: '#4CAF50' }}>
                      Test Last
                    </Button>
                    {outputTests.last?.result && (
                      <Alert severity="success" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                        onClose={() => setOutputTests(p => ({ ...p, last: { ...p.last, result: null } }))}>
                        <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap', maxHeight: 60, overflow: 'auto' }}>{outputTests.last.result}</Typography>
                      </Alert>
                    )}
                    {outputTests.last?.error && (
                      <Alert severity="error" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                        onClose={() => setOutputTests(p => ({ ...p, last: { ...p.last, error: null } }))}>
                        <Typography variant="body2">{outputTests.last.error}</Typography>
                      </Alert>
                    )}
                  </Box>
                )}
              </Box>
            )}

            {enableSum && (
              <Box sx={{ mb: 1 }}>
                <Box sx={{ display: 'flex', gap: 1, mb: 0.5 }}>
                  <TextField size="small" label="Variable Name" value={sumVarName}
                    onChange={(e) => setSumVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                    placeholder="e.g., total_interest" sx={{ flex: 1 }} />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Sum Column</InputLabel>
                    <Select value={sumColumn} label="Sum Column" onChange={(e) => setSumColumn(e.target.value)}>
                      {columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number').map(c => (
                        <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  <Tooltip title="Run schedule and compute sum">
                    <span>
                      <IconButton size="small" onClick={() => testOutputOption('sum')}
                        disabled={!sumVarName || !sumColumn || outputTests.sum?.testing}
                        sx={{ color: '#4CAF50', mt: 0.5 }}>
                        {outputTests.sum?.testing ? <CircularProgress size={14} /> : <Play size={14} />}
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>
                {outputTests.sum?.result && (
                  <Alert severity="success" sx={{ py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, sum: { ...p.sum, result: null } }))}>
                    <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem">{outputTests.sum.result}</Typography>
                  </Alert>
                )}
                {outputTests.sum?.error && (
                  <Alert severity="error" sx={{ py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, sum: { ...p.sum, error: null } }))}>
                    <Typography variant="body2">{outputTests.sum.error}</Typography>
                  </Alert>
                )}
              </Box>
            )}

            {enableCol && (
              <Box sx={{ mb: 1 }}>
                <Box sx={{ display: 'flex', gap: 1, mb: 0.5 }}>
                  <TextField size="small" label="Variable Name" value={colVarName}
                    onChange={(e) => setColVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                    placeholder="e.g., interest_arr" sx={{ flex: 1 }} />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Column</InputLabel>
                    <Select value={colColumn} label="Column" onChange={(e) => setColColumn(e.target.value)}>
                      {columns.filter(c => c.name).map(c => <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>)}
                    </Select>
                  </FormControl>
                  <Tooltip title="Run schedule and extract column array">
                    <span>
                      <IconButton size="small" onClick={() => testOutputOption('col')}
                        disabled={!colVarName || !colColumn || outputTests.col?.testing}
                        sx={{ color: '#4CAF50', mt: 0.5 }}>
                        {outputTests.col?.testing ? <CircularProgress size={14} /> : <Play size={14} />}
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>
                {outputTests.col?.result && (
                  <Alert severity="success" sx={{ py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, col: { ...p.col, result: null } }))}>
                    <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap', maxHeight: 80, overflow: 'auto' }}>{outputTests.col.result}</Typography>
                  </Alert>
                )}
                {outputTests.col?.error && (
                  <Alert severity="error" sx={{ py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, col: { ...p.col, error: null } }))}>
                    <Typography variant="body2">{outputTests.col.error}</Typography>
                  </Alert>
                )}
              </Box>
            )}

            {enableFilter && (
              <Box sx={{ mb: 1 }}>
                <Box sx={{ display: 'flex', gap: 1, mb: 0.75 }}>
                  <TextField size="small" label="Variable Name" value={filterVarName}
                    onChange={(e) => setFilterVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                    placeholder="e.g., interest_period" sx={{ flex: '0 0 150px' }} />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Match Column</InputLabel>
                    <Select value={filterMatchCol} label="Match Column" onChange={(e) => setFilterMatchCol(e.target.value)}>
                      {columns.filter(c => c.name).map(c => <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>)}
                    </Select>
                  </FormControl>
                  <Autocomplete
                    freeSolo size="small"
                    options={filterValueOptions}
                    groupBy={(opt) => opt.group}
                    getOptionLabel={(opt) => (typeof opt === 'string' ? opt : opt.label)}
                    value={filterMatchValue || ''}
                    onChange={(_, newVal) => { setFilterMatchValue(newVal === null ? '' : (typeof newVal === 'string' ? newVal : newVal.label)); }}
                    onInputChange={(_, val, reason) => { if (reason === 'input') setFilterMatchValue(val); }}
                    sx={{ flex: 1 }}
                    renderInput={(params) => (
                      <TextField {...params} label="Match Value" placeholder="e.g., postingdate"
                        helperText="Variable, event field, or quoted string"
                        InputProps={{ ...params.InputProps, sx: { fontFamily: 'monospace', fontSize: '0.8125rem' } }} />
                    )}
                  />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Return Column</InputLabel>
                    <Select value={filterReturnCol} label="Return Column" onChange={(e) => setFilterReturnCol(e.target.value)}>
                      {columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number').map(c => (
                        <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  <Tooltip title="Run schedule and apply filter">
                    <span>
                      <IconButton size="small" onClick={() => testOutputOption('filter')}
                        disabled={!filterVarName || !filterMatchCol || !filterMatchValue || !filterReturnCol || outputTests.filter?.testing}
                        sx={{ color: '#4CAF50', mt: 0.5 }}>
                        {outputTests.filter?.testing ? <CircularProgress size={14} /> : <Play size={14} />}
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>
                {outputTests.filter?.result && (
                  <Alert severity="success" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, filter: { ...p.filter, result: null } }))}>
                    <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem">{outputTests.filter.result}</Typography>
                  </Alert>
                )}
                {outputTests.filter?.error && (
                  <Alert severity="error" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, filter: { ...p.filter, error: null } }))}>
                    <Typography variant="body2">{outputTests.filter.error}</Typography>
                  </Alert>
                )}
              </Box>
            )}
          </CardContent>
        </Card>

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
