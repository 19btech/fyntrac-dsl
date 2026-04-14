import React, { useState, useMemo, useCallback, useEffect } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  ToggleButtonGroup, ToggleButton, CircularProgress, Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
  Autocomplete,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, GripVertical, Play, Code, Eye, Calendar,
  Table as TableIcon, BarChart3, RefreshCw, Save,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";

const FREQUENCY_OPTIONS = [
  { value: 'M', label: 'Monthly', description: '12 periods per year' },
  { value: 'Q', label: 'Quarterly', description: '4 periods per year' },
  { value: 'S', label: 'Semi-Annual', description: '2 periods per year' },
  { value: 'A', label: 'Annual', description: '1 period per year' },
];

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
 * ScheduleBuilder — Visual drag-and-drop schedule column builder.
 * Builds schedule() DSL code using a column palette and formula bars.
 */
const ScheduleBuilder = ({ events, dslFunctions, onClose, onSave, initialData }) => {
  const cfg = initialData?.config || {};
  const [scheduleName, setScheduleName] = useState(initialData?.name || '');
  const [schedulePriority, setSchedulePriority] = useState(initialData?.priority ?? '');
  const [scheduleId, setScheduleId] = useState(initialData?.id || null);
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState(null);
  const [validationMsg, setValidationMsg] = useState('');

  // Fetch all saved-rules for auto-detecting context variables
  const [savedRulesVarNames, setSavedRulesVarNames] = useState([]);
  const [savedRulesVars, setSavedRulesVars] = useState([]);
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API}/saved-rules`);
        if (!res.ok) return;
        const data = await res.json();
        const rules = Array.isArray(data) ? data : (data.rules || []);
        const names = new Set();
        const allVars = [];
        rules.forEach(r => {
          (r.variables || []).forEach(v => {
            if (v.name) {
              names.add(v.name);
              allVars.push(v);
            }
          });
        });
        setSavedRulesVarNames([...names]);
        setSavedRulesVars(allVars);
      } catch { /* ignore */ }
    })();
  }, []);
  // Period type: 'date' (date-based) or 'number' (count-based)
  const [periodType, setPeriodType] = useState(cfg.periodType || 'date');
  // Date-based: start/end source = 'value' | 'field' | 'formula'
  const [startDate, setStartDate] = useState(cfg.startDate || '');
  const [startDateSource, setStartDateSource] = useState(cfg.startDateSource || 'value');
  const [startDateField, setStartDateField] = useState(cfg.startDateField || '');
  const [startDateFormula, setStartDateFormula] = useState(cfg.startDateFormula || '');
  const [endDate, setEndDate] = useState(cfg.endDate || '');
  const [endDateSource, setEndDateSource] = useState(cfg.endDateSource || 'value');
  const [endDateField, setEndDateField] = useState(cfg.endDateField || '');
  const [endDateFormula, setEndDateFormula] = useState(cfg.endDateFormula || '');
  // Number-based: count of periods with source (value, field, formula)
  const [periodCount, setPeriodCount] = useState(cfg.periodCount || '12');
  const [periodCountSource, setPeriodCountSource] = useState(cfg.periodCountSource || 'value');
  const [periodCountField, setPeriodCountField] = useState(cfg.periodCountField || '');
  const [periodCountFormula, setPeriodCountFormula] = useState(cfg.periodCountFormula || '');
  const [frequency, setFrequency] = useState(cfg.frequency || 'M');
  const [convention, setConvention] = useState(cfg.convention || '');
  const [columns, setColumns] = useState(
    cfg.columns?.length ? cfg.columns : [{ name: 'date', formula: 'period_date' }]
  );
  const allEventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  const [showCode, setShowCode] = useState(false);
  const [createTxn, setCreateTxn] = useState(cfg.createTxn || false);
  const [txnType, setTxnType] = useState(cfg.txnType || '');
  const [txnAmountCol, setTxnAmountCol] = useState(cfg.txnAmountCol || '');
  const [extractFirst, setExtractFirst] = useState(cfg.extractFirst || false);
  const [extractLast, setExtractLast] = useState(cfg.extractLast || false);
  const [extractColumn, setExtractColumn] = useState(cfg.extractColumn || '');
  // Schedule Sum toggle
  const [enableSum, setEnableSum] = useState(cfg.enableSum || false);
  const [sumColumn, setSumColumn] = useState(cfg.sumColumn || '');
  const [sumVarName, setSumVarName] = useState(cfg.sumVarName || '');
  // Schedule Column toggle
  const [enableCol, setEnableCol] = useState(cfg.enableCol || false);
  const [colColumn, setColColumn] = useState(cfg.colColumn || '');
  const [colVarName, setColVarName] = useState(cfg.colVarName || '');
  // Schedule Filter toggle: schedule_filter(sched, matchCol, matchValue, returnCol)
  const [enableFilter, setEnableFilter] = useState(cfg.enableFilter || false);
  const [filterVarName, setFilterVarName] = useState(cfg.filterVarName || '');
  const [filterMatchCol, setFilterMatchCol] = useState(cfg.filterMatchCol || '');
  const [filterMatchValue, setFilterMatchValue] = useState(cfg.filterMatchValue || '');
  const [filterReturnCol, setFilterReturnCol] = useState(cfg.filterReturnCol || '');
  // Schedule Preview: run the actual schedule and show real rows
  const [schedulePreviewTesting, setSchedulePreviewTesting] = useState(false);
  const [schedulePreviewData, setSchedulePreviewData] = useState(null);
  const [schedulePreviewError, setSchedulePreviewError] = useState(null);
  // Per-output-option test states keyed by option type
  const [outputTests, setOutputTests] = useState({});

  const dateEventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.filter(f => f.datatype === 'date' || f.name.includes('date')).forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  // Options for the Filter Match Value searchable field
  const filterValueOptions = useMemo(() => {
    const opts = [];
    // Built-ins always available as match targets
    opts.push({ label: 'postingdate', group: 'Built-in' });
    opts.push({ label: 'effectivedate', group: 'Built-in' });
    // Defined variables from saved rules
    savedRulesVarNames.forEach(v => opts.push({ label: v, group: 'Defined Variable' }));
    // Event fields (EventName.fieldName)
    if (events?.length) {
      events.forEach(ev => {
        (ev.fields || []).forEach(f => {
          opts.push({ label: `${ev.event_name}.${f.name}`, group: `Event: ${ev.event_name}` });
        });
      });
    }
    return opts;
  }, [savedRulesVarNames, events]);

  const addColumn = useCallback(() => {
    setColumns(prev => [...prev, { name: '', formula: '' }]);
  }, []);

  const updateColumn = useCallback((index, updated) => {
    setColumns(prev => prev.map((c, i) => i === index ? updated : c));
  }, []);

  const removeColumn = useCallback((index) => {
    setColumns(prev => prev.filter((_, i) => i !== index));
  }, []);

  const moveColumn = useCallback((index, direction) => {
    setColumns(prev => {
      const arr = [...prev];
      const target = index + direction;
      if (target < 0 || target >= arr.length) return arr;
      [arr[index], arr[target]] = [arr[target], arr[index]];
      return arr;
    });
  }, []);

  // Built-in schedule identifiers that should NOT be treated as external variables
  const SCHEDULE_BUILTINS = useMemo(() => new Set([
    'period_date', 'period_index', 'period_start', 'period_number', 'dcf', 'lag',
    'days_in_current_period',
    ...(dslFunctions || []).map(f => f.name),
  ]), [dslFunctions]);

  // Auto-detect external variable references from column formulas.
  // KEY FIX: identifiers that appear in savedRulesVarNames are ALWAYS added to context,
  // even when a column shares the same name (e.g. column "payment" with formula "payment"
  // references the saved-rule var "payment" — without this fix the context omits it
  // and all subsequent columns that depend on it show NoneType errors).
  const autoDetectedVars = useMemo(() => {
    const colNames = new Set(columns.filter(c => c.name).map(c => c.name));
    const savedVarNameSet = new Set(savedRulesVarNames);
    const externalRefs = new Set();
    for (const col of columns) {
      if (!col.formula) continue;
      const identifiers = col.formula.match(/[a-zA-Z_][a-zA-Z0-9_]*/g) || [];
      for (const id of identifiers) {
        if (SCHEDULE_BUILTINS.has(id)) continue;
        // Always include if it's a known saved-rules variable, even if same name as column
        if (savedVarNameSet.has(id)) { externalRefs.add(id); continue; }
        if (colNames.has(id)) continue;
        externalRefs.add(id);
      }
    }
    return [...externalRefs];
  }, [columns, SCHEDULE_BUILTINS, savedRulesVarNames]);

  // Test a single column — generates schedule code for columns up to this index and executes
  const testColumn = useCallback(async (colIndex) => {
    const colsToTest = columns.slice(0, colIndex + 1).filter(c => c.name && c.formula);
    if (colsToTest.length === 0) return { success: false, error: 'No valid columns to test' };
    const lines = [];
    // Emit ALL saved rule vars in order so transitive dependencies are available.
    // (Emitting only the directly-referenced autoDetectedVars can fail when those vars
    // themselves depend on other vars that haven't been emitted yet.)
    for (const v of savedRulesVars) {
      if (!v.name) continue;
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
    // Period
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
    // Schedule with subset of columns
    lines.push('sched = schedule(p, {');
    colsToTest.forEach((col, idx) => {
      const comma = idx < colsToTest.length - 1 ? ',' : '';
      lines.push(`    "${col.name}": "${col.formula}"${comma}`);
    });
    const contextPairs = autoDetectedVars.map(v => `"${v}": ${v}`);
    if (contextPairs.length > 0) {
      lines.push(`}, {${contextPairs.join(', ')}})`);
    } else {
      lines.push('})');
    }
    lines.push('print(sched)');
    const dslCode = lines.join('\n');
    const today = new Date().toISOString().split('T')[0];
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: dslCode, posting_date: today }),
    });
    const data = await response.json();
    if (response.ok && data.success) {
      const out = (data.print_outputs || []).map(p => String(p)).join('\n') || 'Executed successfully (no output)';
      return { success: true, output: out };
    } else {
      return { success: false, error: data.error || data.detail || 'Execution failed' };
    }
  }, [columns, autoDetectedVars, savedRulesVars, periodType, periodCount, periodCountSource, periodCountField, periodCountFormula, startDateSource, startDateField, startDateFormula, startDate, endDateSource, endDateField, endDateFormula, endDate, frequency, convention]);

  const generatedCode = useMemo(() => {
    const lines = [];
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push(`## ${(scheduleName || 'SCHEDULE').toUpperCase()}`);
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push('');

    // Auto-detected dependency variables from saved rules
    const emittedVars = [];
    for (const varName of autoDetectedVars) {
      const savedVar = savedRulesVars.find(v => v.name === varName);
      if (savedVar) {
        if (savedVar.source === 'value') {
          lines.push(`${savedVar.name} = ${savedVar.value || 0}`);
        } else if (savedVar.source === 'event_field') {
          lines.push(`${savedVar.name} = ${savedVar.eventField}`);
        } else if (savedVar.source === 'formula') {
          lines.push(`${savedVar.name} = ${savedVar.formula || 0}`);
        } else if (savedVar.source === 'collect') {
          lines.push(`${savedVar.name} = ${savedVar.collectType || 'collect'}(${savedVar.eventField})`);
        }
        emittedVars.push(varName);
      }
    }
    if (emittedVars.length > 0) lines.push('');

    // Period definition
    if (periodType === 'number') {
      // Number-based period: period(count)
      const countExpr = periodCountSource === 'field' && periodCountField ? periodCountField
        : periodCountSource === 'formula' && periodCountFormula ? periodCountFormula
        : (periodCount || 12);
      lines.push('## Schedule Period');
      lines.push(`p = period(${countExpr})`);
    } else {
      // Date-based period
      const startExpr = startDateSource === 'field' && startDateField ? startDateField
        : startDateSource === 'formula' && startDateFormula ? startDateFormula
        : `"${startDate || '2026-01-01'}"`;
      const endExpr = endDateSource === 'field' && endDateField ? endDateField
        : endDateSource === 'formula' && endDateFormula ? endDateFormula
        : `"${endDate || '2026-12-31'}"`;
      lines.push('## Schedule Period');
      let periodCall = `p = period(${startExpr}, ${endExpr}, "${frequency}"`;
      if (convention) {
        periodCall += `, "${convention}"`;
      }
      periodCall += ')';
      lines.push(periodCall);
    }
    lines.push('');

    // Schedule definition
    lines.push('## Schedule Columns');
    lines.push('sched = schedule(p, {');
    const validCols = columns.filter(c => c.name && c.formula);
    validCols.forEach((col, idx) => {
      const comma = idx < validCols.length - 1 ? ',' : '';
      lines.push(`    "${col.name}": "${col.formula}"${comma}`);
    });

    // Context object — auto-wired from detected variable references
    const contextPairs = autoDetectedVars.map(v => `"${v}": ${v}`);
    if (contextPairs.length > 0) {
      lines.push(`}, {${contextPairs.join(', ')}})`);
    } else {
      lines.push('})');
    }

    lines.push('');
    lines.push('## Display Results');
    lines.push('print(sched)');

    // Totals for numeric columns
    const numericCols = validCols.filter(c => c.name !== 'date' && c.formula !== 'period_date' && c.formula !== 'period_number');
    if (numericCols.length > 0) {
      lines.push('');
      lines.push('## Summary Totals');
      numericCols.forEach(col => {
        lines.push(`print("Total ${col.name}:", schedule_sum(sched, "${col.name}"))`);
      });
    }

    // Extract first/last values
    const targetCol = extractColumn || (numericCols.length > 0 ? numericCols[numericCols.length - 1].name : '');
    if (extractFirst && targetCol) {
      lines.push('');
      lines.push(`first_${targetCol} = schedule_first(sched, "${targetCol}")`);
      lines.push(`print("First ${targetCol}:", first_${targetCol})`);
    }
    if (extractLast && targetCol) {
      if (!extractFirst) lines.push('');
      lines.push(`last_${targetCol} = schedule_last(sched, "${targetCol}")`);
      lines.push(`print("Last ${targetCol}:", last_${targetCol})`);
    }

    // Schedule Sum
    if (enableSum && sumColumn && sumVarName) {
      lines.push('');
      lines.push(`${sumVarName} = schedule_sum(sched, "${sumColumn}")`);
      lines.push(`print("${sumVarName}:", ${sumVarName})`);
    }

    // Schedule Column
    if (enableCol && colColumn && colVarName) {
      lines.push('');
      lines.push(`${colVarName} = schedule_column(sched, "${colColumn}")`);
      lines.push(`print("${colVarName}:", ${colVarName})`);
    }

    // Schedule Filter: schedule_filter(sched, matchCol, matchValue, returnCol)
    if (enableFilter && filterVarName && filterMatchCol && filterMatchValue && filterReturnCol) {
      lines.push('');
      lines.push(`${filterVarName} = schedule_filter(sched, "${filterMatchCol}", ${filterMatchValue}, "${filterReturnCol}")`);
      lines.push(`print("${filterVarName}:", ${filterVarName})`);
    }

    // Create transaction from output variable
    if (createTxn && txnType && txnAmountCol) {
      lines.push('');
      lines.push('## Create Transaction');
      lines.push(`createTransaction(postingdate, postingdate, "${txnType}", ${txnAmountCol})`);
    }

    return lines.join('\n');
  }, [scheduleName, periodType, startDate, startDateSource, startDateField, startDateFormula, endDate, endDateSource, endDateField, endDateFormula, periodCount, periodCountSource, periodCountField, periodCountFormula, frequency, convention, columns, autoDetectedVars, savedRulesVars, createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn, enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName, enableFilter, filterVarName, filterMatchCol, filterMatchValue, filterReturnCol]);

  // Build schedule base lines (vars + period + schedule() call) — shared by test functions
  const buildScheduleBaseLines = useCallback(() => {
    const lines = [];
    for (const v of savedRulesVars) {
      if (!v.name) continue;
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
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
    const ctxPairs = autoDetectedVars.map(v => `"${v}": ${v}`);
    if (ctxPairs.length > 0) lines.push(`}, {${ctxPairs.join(', ')}})`);
    else lines.push('})' );
    return lines;
  }, [savedRulesVars, periodType, periodCount, periodCountSource, periodCountField, periodCountFormula,
      startDateSource, startDateField, startDateFormula, startDate,
      endDateSource, endDateField, endDateFormula, endDate,
      frequency, convention, columns, autoDetectedVars]);

  // Run only the schedule-definition code and return parsed rows for the preview table
  const testSchedulePreview = useCallback(async () => {
    setSchedulePreviewTesting(true);
    setSchedulePreviewData(null);
    setSchedulePreviewError(null);
    try {
      const validCols = columns.filter(c => c.name && c.formula);
      if (validCols.length === 0) { setSchedulePreviewError('No valid columns defined yet.'); return; }
      const lines = buildScheduleBaseLines();
      lines.push('print(sched)');
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: lines.join('\n'), posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success && data.print_outputs?.length > 0) {
        try {
          // Each instrument emits its own print output — take the first instrument's schedule
          let parsed = JSON.parse(data.print_outputs[0]);
          // Normalise nested array-of-arrays (multi-instrument packed into one JSON)
          if (Array.isArray(parsed) && Array.isArray(parsed[0])) parsed = parsed[0];
          else if (Array.isArray(parsed) && parsed[0]?.schedule) parsed = parsed[0].schedule;
          if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === 'object' && !Array.isArray(parsed[0])) {
            setSchedulePreviewData(parsed);
          } else {
            setSchedulePreviewError('Schedule ran but returned no rows.');
          }
        } catch {
          setSchedulePreviewError('Ran successfully but output was not parseable JSON.');
        }
      } else {
        setSchedulePreviewError(data.error || data.detail || 'Execution failed');
      }
    } catch (err) {
      setSchedulePreviewError(err.message || 'Network error');
    } finally {
      setSchedulePreviewTesting(false);
    }
  }, [buildScheduleBaseLines, columns]);

  // Test a specific output option in isolation
  const testOutputOption = useCallback(async (optType) => {
    setOutputTests(prev => ({ ...prev, [optType]: { testing: true, result: null, error: null } }));
    const setResult = (result, error) =>
      setOutputTests(prev => ({ ...prev, [optType]: { testing: false, result, error } }));
    try {
      const lines = buildScheduleBaseLines();
      switch (optType) {
        case 'first':
          if (!extractColumn) { setResult(null, 'Select a column first.'); return; }
          lines.push(`first_${extractColumn} = schedule_first(sched, "${extractColumn}")`);
          lines.push(`print("first_${extractColumn}:", first_${extractColumn})`);
          break;
        case 'last':
          if (!extractColumn) { setResult(null, 'Select a column first.'); return; }
          lines.push(`last_${extractColumn} = schedule_last(sched, "${extractColumn}")`);
          lines.push(`print("last_${extractColumn}:", last_${extractColumn})`);
          break;
        case 'sum':
          if (!sumVarName || !sumColumn) { setResult(null, 'Fill in variable name and column.'); return; }
          lines.push(`${sumVarName} = schedule_sum(sched, "${sumColumn}")`);
          lines.push(`print("${sumVarName}:", ${sumVarName})`);
          break;
        case 'col':
          if (!colVarName || !colColumn) { setResult(null, 'Fill in variable name and column.'); return; }
          lines.push(`${colVarName} = schedule_column(sched, "${colColumn}")`);
          lines.push(`print("${colVarName}:", ${colVarName})`);
          break;
        case 'filter':
          if (!filterVarName || !filterMatchCol || !filterMatchValue || !filterReturnCol) {
            setResult(null, 'Fill in all filter fields.'); return;
          }
          lines.push(`${filterVarName} = schedule_filter(sched, "${filterMatchCol}", ${filterMatchValue}, "${filterReturnCol}")`);
          lines.push(`print("${filterVarName}:", ${filterVarName})`);
          break;
        default: return;
      }
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: lines.join('\n'), posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const out = (data.print_outputs || []).map(p => String(p)).join('\n') || 'Ran (no output)';
        setResult(out, null);
      } else {
        setResult(null, data.error || data.detail || 'Execution failed');
      }
    } catch (err) {
      setResult(null, err.message);
    }
  }, [buildScheduleBaseLines, extractColumn, sumVarName, sumColumn, colVarName, colColumn,
      filterVarName, filterMatchCol, filterMatchValue, filterReturnCol]);

  const handleSave = useCallback(async () => {
    if (!scheduleName.trim()) {
      setValidationMsg('Schedule Name is required and not populated.');
      return;
    }
    if (schedulePriority === '' || schedulePriority === null || schedulePriority === undefined) {
      setValidationMsg('Priority is required and not populated.');
      return;
    }
    setSaving(true);
    setSaveResult(null);
    try {
      const payload = {
        id: scheduleId,
        name: scheduleName.trim(),
        priority: Number(schedulePriority),
        generatedCode,
        config: {
          periodType, startDate, startDateSource, startDateField, startDateFormula,
          endDate, endDateSource, endDateField, endDateFormula,
          periodCount, periodCountSource, periodCountField, periodCountFormula, frequency, convention, columns,
          createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn,
          enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName,
          enableFilter, filterVarName, filterMatchCol, filterMatchValue, filterReturnCol,
        },
      };
      const response = await fetch(`${API}/saved-schedules`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setScheduleId(data.id);
        setSaveResult({ success: true, output: data.message || 'Schedule saved successfully.' });
        if (onSave) onSave();
      } else {
        const errMsg = data.detail || data.error || 'Save failed';
        setSaveResult({ success: false, error: typeof errMsg === 'string' ? errMsg : JSON.stringify(errMsg) });
      }
    } catch (err) {
      setSaveResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setSaving(false);
    }
  }, [scheduleName, schedulePriority, scheduleId, generatedCode, periodType, startDate, startDateSource, startDateField, startDateFormula, endDate, endDateSource, endDateField, endDateFormula, periodCount, periodCountSource, periodCountField, periodCountFormula, frequency, convention, columns, createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn, enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName, enableFilter, filterVarName, filterMatchCol, filterMatchValue, filterReturnCol, onSave]);

  // Compute a simple mock preview of what the schedule table would look like
  const previewHeaders = useMemo(() => columns.filter(c => c.name).map(c => c.name), [columns]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%' }}>
      {/* Header */}
      <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', bgcolor: 'white', flexShrink: 0 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 0.5 }}>
          <TableIcon size={20} color="#5B5FED" />
          <Typography variant="h5">Schedule Builder</Typography>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Build amortization and time-based schedules visually
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {/* Schedule Name & Priority */}
        <Box sx={{ display: 'flex', gap: 1.5, mb: 2 }}>
          <TextField size="small" label="Schedule Name *" value={scheduleName}
            onChange={(e) => setScheduleName(e.target.value)}
            placeholder="e.g., Loan Amortization Schedule"
            sx={{ flex: 1 }} />
          <TextField size="small" label="Priority *" value={schedulePriority}
            onChange={(e) => { const v = e.target.value; if (v === '' || /^\d+$/.test(v)) setSchedulePriority(v === '' ? '' : Number(v)); }}
            placeholder="e.g., 2"
            type="number"
            inputProps={{ min: 0, step: 1 }}
            sx={{ width: 140 }} />
        </Box>

        {/* Time Period */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
          <Calendar size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
          Time Period
        </Typography>

        {/* Period Type Toggle */}
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
                  onChange={(e) => setPeriodCountField(e.target.value)} notched
                  displayEmpty
                  renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                  {allEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                </Select>
              </FormControl>
            )}
            {periodCountSource === 'formula' && (
              <Box sx={{ maxWidth: 400 }}>
                <FormulaBar
                  value={periodCountFormula}
                  onChange={setPeriodCountFormula}
                  events={events}
                  label="Period Count Formula"
                  placeholder='e.g., multiply(years, 12)'
                />
              </Box>
            )}
          </Box>
        ) : (
          <>
            <Box sx={{ display: 'flex', gap: 1.5, mb: 1.5 }}>
              {/* Start Date */}
              <Box sx={{ flex: 1 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>Start Date</Typography>
                <Box sx={{ display: 'flex', gap: 0.5, mb: 0.5 }}>
                  <ToggleButtonGroup size="small" exclusive value={startDateSource}
                    onChange={(e, v) => { if (v) setStartDateSource(v); }}
                    sx={{ '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1, py: 0.25 } }}>
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
                    <Select value={startDateField}
                      onChange={(e) => setStartDateField(e.target.value)}
                      displayEmpty
                      renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {startDateSource === 'formula' && (
                  <FormulaBar
                    value={startDateFormula}
                    onChange={setStartDateFormula}
                    events={events}
                    placeholder='e.g., add_months(effectivedate, 12)'
                  />
                )}
              </Box>

              {/* End Date */}
              <Box sx={{ flex: 1 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>End Date</Typography>
                <Box sx={{ display: 'flex', gap: 0.5, mb: 0.5 }}>
                  <ToggleButtonGroup size="small" exclusive value={endDateSource}
                    onChange={(e, v) => { if (v) setEndDateSource(v); }}
                    sx={{ '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.6875rem', px: 1, py: 0.25 } }}>
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
                    <Select value={endDateField}
                      onChange={(e) => setEndDateField(e.target.value)}
                      displayEmpty
                      renderValue={(val) => val || <em style={{ color: '#999' }}>Select field...</em>}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {endDateSource === 'formula' && (
                  <FormulaBar
                    value={endDateFormula}
                    onChange={setEndDateFormula}
                    events={events}
                    placeholder='e.g., add_months(effectivedate, 60)'
                  />
                )}
              </Box>
            </Box>
          </>
        )}

        <Box sx={{ display: 'flex', gap: 1.5, mb: 2.5 }}>
          {periodType === 'date' && (
            <FormControl size="small" sx={{ minWidth: 140 }}>
              <InputLabel>Frequency</InputLabel>
              <Select value={frequency} label="Frequency"
                onChange={(e) => setFrequency(e.target.value)}>
                {FREQUENCY_OPTIONS.map(f => (
                  <MenuItem key={f.value} value={f.value}>{f.label}</MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          {periodType === 'date' && (
            <FormControl size="small" sx={{ minWidth: 160 }}>
              <InputLabel>Day Count Convention</InputLabel>
              <Select value={convention} label="Day Count Convention"
                onChange={(e) => setConvention(e.target.value)}>
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

        {/* Column Definitions */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="body2" fontWeight={600}>
            Schedule Columns ({columns.length})
          </Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={addColumn}>
            Custom Column
          </Button>
        </Box>

        {columns.map((col, idx) => (
          <ColumnCard key={idx} column={col} index={idx} events={events}
            variables={[...new Set([...autoDetectedVars, ...savedRulesVarNames])]}
            onUpdate={updateColumn} onRemove={removeColumn}
            onMoveUp={() => moveColumn(idx, -1)} onMoveDown={() => moveColumn(idx, 1)}
            isFirst={idx === 0} isLast={idx === columns.length - 1}
            onTest={testColumn} />
        ))}

        {/* Table Preview */}
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
                    onClick={testSchedulePreview}
                    disabled={schedulePreviewTesting}
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
                    {(schedulePreviewData ? Object.keys(schedulePreviewData[0] || {}) : previewHeaders).map(h => (
                      <TableCell key={h} sx={{ fontWeight: 600, fontSize: '0.75rem', bgcolor: '#F8F9FA', whiteSpace: 'nowrap' }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schedulePreviewData ? (
                    schedulePreviewData.slice(0, 20).map((row, rowIdx) => (
                      <TableRow key={rowIdx} hover sx={{ '&:last-child td': { borderBottom: 0 } }}>
                        <TableCell sx={{ fontSize: '0.75rem', color: '#6C757D' }}>{rowIdx + 1}</TableCell>
                        {Object.values(row).map((val, ci) => (
                          <TableCell key={ci} sx={{ fontSize: '0.75rem',
                            fontFamily: typeof val === 'number' ? 'monospace' : 'inherit',
                            fontWeight: typeof val === 'number' ? 500 : 400 }}>
                            {typeof val === 'number'
                              ? (Number.isInteger(val) ? val.toLocaleString() : val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 }))
                              : String(val ?? '—')}
                          </TableCell>
                        ))}
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

        {/* Output Options */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1.5 }}>Output Options</Typography>

        {/* Extract first/last */}
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap', mb: (extractFirst || extractLast || enableSum || enableCol || enableFilter) ? 1 : 0 }}>
              <FormControlLabel
                control={<Switch checked={extractFirst} onChange={(e) => setExtractFirst(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule First</Typography>}
              />
              <FormControlLabel
                control={<Switch checked={extractLast} onChange={(e) => setExtractLast(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Last</Typography>}
              />
              <FormControlLabel
                control={<Switch checked={enableSum} onChange={(e) => setEnableSum(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Sum</Typography>}
              />
              <FormControlLabel
                control={<Switch checked={enableCol} onChange={(e) => setEnableCol(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Column</Typography>}
              />
              <FormControlLabel
                control={<Switch checked={enableFilter} onChange={(e) => setEnableFilter(e.target.checked)} size="small" />}
                label={<Typography variant="body2">Schedule Filter</Typography>}
              />
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
            {extractColumn && (extractFirst || extractLast) && (
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 1 }}>
                {extractFirst && (
                  <Box sx={{ flex: 1, minWidth: 160 }}>
                    <Button size="small" variant="outlined"
                      startIcon={outputTests.first?.testing ? <CircularProgress size={12} /> : <Play size={12} />}
                      onClick={() => testOutputOption('first')} disabled={outputTests.first?.testing}
                      sx={{ fontSize: '0.7rem', py: 0.25, borderColor: '#4CAF50', color: '#4CAF50', '&:hover': { borderColor: '#388E3C', bgcolor: '#E8F5E9' } }}>
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
                      sx={{ fontSize: '0.7rem', py: 0.25, borderColor: '#4CAF50', color: '#4CAF50', '&:hover': { borderColor: '#388E3C', bgcolor: '#E8F5E9' } }}>
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
                    <Select value={sumColumn} label="Sum Column"
                      onChange={(e) => setSumColumn(e.target.value)}>
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
                    <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap' }}>{outputTests.sum.result}</Typography>
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
                    <Select value={colColumn} label="Column"
                      onChange={(e) => setColColumn(e.target.value)}>
                      {columns.filter(c => c.name).map(c => (
                        <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                      ))}
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
                    <Select value={filterMatchCol} label="Match Column"
                      onChange={(e) => setFilterMatchCol(e.target.value)}>
                      {columns.filter(c => c.name).map(c => (
                        <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  <Autocomplete
                    freeSolo
                    size="small"
                    options={filterValueOptions}
                    groupBy={(opt) => opt.group}
                    getOptionLabel={(opt) => (typeof opt === 'string' ? opt : opt.label)}
                    value={filterMatchValue || ''}
                    onChange={(_, newVal) => {
                      if (newVal === null) setFilterMatchValue('');
                      else setFilterMatchValue(typeof newVal === 'string' ? newVal : newVal.label);
                    }}
                    onInputChange={(_, val, reason) => {
                      if (reason === 'input') setFilterMatchValue(val);
                    }}
                    sx={{ flex: 1 }}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Match Value"
                        placeholder="e.g., postingdate"
                        helperText='Variable, event field, or quoted string'
                        InputProps={{ ...params.InputProps, sx: { fontFamily: 'monospace', fontSize: '0.8125rem' } }}
                      />
                    )}
                  />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Return Column</InputLabel>
                    <Select value={filterReturnCol} label="Return Column"
                      onChange={(e) => setFilterReturnCol(e.target.value)}>
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
                <Typography variant="caption" color="text.secondary" sx={{ pl: 0.5 }}>
                  Generates: <code style={{fontFamily:'monospace'}}>{`${filterVarName || 'result'} = schedule_filter(sched, "${filterMatchCol || 'col'}", ${filterMatchValue || 'value'}, "${filterReturnCol || 'col'}")`}</code>
                </Typography>
                {outputTests.filter?.result && (
                  <Alert severity="success" sx={{ mt: 0.5, py: 0, '& .MuiAlert-message': { py: 0.5 } }}
                    onClose={() => setOutputTests(p => ({ ...p, filter: { ...p.filter, result: null } }))}>
                    <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap' }}>{outputTests.filter.result}</Typography>
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

        {/* Create transaction from output variables */}
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <Typography variant="body2">Create transaction from output</Typography>
              <Switch checked={createTxn} onChange={(e) => {
                const turning_on = e.target.checked;
                if (turning_on) {
                  // Check if any output variable is defined
                  const hasOutputVars = (extractFirst && extractColumn) || (extractLast && extractColumn) || (enableSum && sumVarName && sumColumn) || (enableCol && colVarName && colColumn) || (enableFilter && filterVarName && filterMatchCol && filterMatchValue && filterReturnCol);
                  if (!hasOutputVars) {
                    setValidationMsg('Please define at least one output variable (Schedule First, Last, Sum, Column, or Filter) before creating a transaction.');
                    return;
                  }
                }
                setCreateTxn(turning_on);
              }} size="small" />
            </Box>
            {createTxn && (() => {
              // Collect all defined output variables
              const outputVars = [];
              if (extractFirst && extractColumn) outputVars.push({ name: `first_${extractColumn}`, label: `Schedule First (first_${extractColumn})` });
              if (extractLast && extractColumn) outputVars.push({ name: `last_${extractColumn}`, label: `Schedule Last (last_${extractColumn})` });
              if (enableSum && sumVarName && sumColumn) outputVars.push({ name: sumVarName, label: `Schedule Sum (${sumVarName})` });
              if (enableCol && colVarName && colColumn) outputVars.push({ name: colVarName, label: `Schedule Column (${colVarName})` });
              if (enableFilter && filterVarName && filterMatchCol && filterMatchValue && filterReturnCol) outputVars.push({ name: filterVarName, label: `Schedule Filter → ${filterReturnCol} (${filterVarName})` });
              if (outputVars.length === 0) return (
                <Alert severity="warning" sx={{ mt: 1, fontSize: '0.8125rem' }}>
                  No output variables defined. Enable and configure at least one output option above (Schedule First, Last, Sum, Column, or Filter).
                </Alert>
              );
              return (
                <Box sx={{ mt: 1, display: 'flex', gap: 1 }}>
                  <TextField size="small" label="Transaction Type" value={txnType}
                    onChange={(e) => setTxnType(e.target.value)} sx={{ flex: 1 }}
                    placeholder="e.g., Interest Accrual" />
                  <FormControl size="small" sx={{ flex: 1 }}>
                    <InputLabel>Amount Variable</InputLabel>
                    <Select value={txnAmountCol} label="Amount Variable"
                      onChange={(e) => setTxnAmountCol(e.target.value)}>
                      {outputVars.map(v => (
                        <MenuItem key={v.name} value={v.name}>{v.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Box>
              );
            })()}
          </CardContent>
        </Card>

        <Divider sx={{ my: 2 }} />

        {/* Code Preview (Progressive Disclosure — Rec 8) */}
        <FormControlLabel
          control={<Switch checked={showCode} onChange={(e) => setShowCode(e.target.checked)} size="small" />}
          label={<Typography variant="body2" fontWeight={500}>Show generated logic</Typography>}
        />
        {showCode && (
          <Paper variant="outlined" sx={{ mt: 1, p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 300, overflow: 'auto' }}>
            <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
              {generatedCode}
            </pre>
          </Paper>
        )}

        {/* Save Result */}
        {saveResult && (
          <Alert severity={saveResult.success ? 'success' : 'error'} sx={{ mt: 2, '& .MuiAlert-message': { width: '100%' } }}
            onClose={() => setSaveResult(null)}>
            <Typography variant="body2">{saveResult.success ? saveResult.output : saveResult.error}</Typography>
          </Alert>
        )}
      </Box>

      {/* Validation Dialog */}
      <Dialog open={!!validationMsg} onClose={() => setValidationMsg('')}>
        <DialogTitle>Missing Required Field</DialogTitle>
        <DialogContent>
          <DialogContentText>{validationMsg}</DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setValidationMsg('')} autoFocus>OK</Button>
        </DialogActions>
      </Dialog>

      {/* Action Bar */}
      <Box sx={{ p: 2, borderTop: '1px solid #E9ECEF', bgcolor: 'white', display: 'flex', gap: 1, justifyContent: 'flex-end', flexShrink: 0 }}>
        {onClose && <Button onClick={onClose} color="inherit">Cancel</Button>}
        <Button variant="outlined" onClick={handleSave} disabled={saving}
          startIcon={saving ? <CircularProgress size={16} /> : <Save size={16} />}
          sx={{ borderColor: '#1976D2', color: '#1976D2', '&:hover': { borderColor: '#1565C0', bgcolor: '#E3F2FD' } }}>
          {saving ? 'Saving...' : 'Save Schedule'}
        </Button>
      </Box>
    </Box>
  );
};

export default ScheduleBuilder;
