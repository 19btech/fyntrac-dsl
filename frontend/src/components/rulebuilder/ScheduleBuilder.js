import React, { useState, useMemo, useCallback } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  ToggleButtonGroup, ToggleButton, CircularProgress,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, GripVertical, Play, Code, Eye, Calendar,
  Table as TableIcon, Hash, Columns, BarChart3, RefreshCw, FlaskConical, Save,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";

const FREQUENCY_OPTIONS = [
  { value: 'M', label: 'Monthly', description: '12 periods per year' },
  { value: 'Q', label: 'Quarterly', description: '4 periods per year' },
  { value: 'S', label: 'Semi-Annual', description: '2 periods per year' },
  { value: 'A', label: 'Annual', description: '1 period per year' },
];

const COLUMN_PRESETS = [
  { name: 'date', formula: 'period_date', category: 'Date', description: 'Period date' },
  { name: 'period_num', formula: 'period_number', category: 'Date', description: 'Period index (1, 2, 3...)' },
  { name: 'opening_bal', formula: "lag('closing_bal', 1, principal)", category: 'Balance', description: 'Opening balance from prior closing' },
  { name: 'interest', formula: "divide(multiply(opening_bal, annual_rate), 12)", category: 'Interest', description: 'Monthly interest amount' },
  { name: 'principal_pmt', formula: "subtract(payment, interest)", category: 'Payment', description: 'Principal portion of payment' },
  { name: 'closing_bal', formula: "subtract(opening_bal, principal_pmt)", category: 'Balance', description: 'Ending balance' },
  { name: 'depreciation', formula: "divide(subtract(cost, salvage), life)", category: 'Depreciation', description: 'Periodic depreciation' },
  { name: 'accumulated_dep', formula: "add(lag('accumulated_dep', 1, 0), depreciation)", category: 'Depreciation', description: 'Cumulative depreciation' },
  { name: 'book_value', formula: "subtract(cost, accumulated_dep)", category: 'Depreciation', description: 'Net book value' },
  { name: 'revenue', formula: "divide(total_revenue, num_periods)", category: 'Revenue', description: 'Recognized revenue for period' },
  { name: 'deferred_rev', formula: "subtract(lag('deferred_rev', 1, total_revenue), revenue)", category: 'Revenue', description: 'Remaining deferred revenue' },
  { name: 'days_in_period', formula: "days_in_current_period", category: 'Date', description: 'Days in the current period' },
];

const ColumnCard = ({ column, index, events, variables, onUpdate, onRemove, onMoveUp, onMoveDown, isFirst, isLast }) => {
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
          </Box>

          <IconButton size="small" onClick={() => onRemove(index)} sx={{ color: '#F44336', mt: 0.5 }}>
            <Trash2 size={14} />
          </IconButton>
        </Box>
      </CardContent>
    </Card>
  );
};

/**
 * ScheduleBuilder — Visual drag-and-drop schedule column builder.
 * Builds schedule() DSL code using a column palette and formula bars.
 */
const ScheduleBuilder = ({ events, dslFunctions, onClose, onSave }) => {
  const [scheduleName, setScheduleName] = useState('');
  const [schedulePriority, setSchedulePriority] = useState('');
  const [scheduleId, setScheduleId] = useState(null);
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState(null);
  // Period type: 'date' (date-based) or 'number' (count-based)
  const [periodType, setPeriodType] = useState('date');
  // Date-based: start/end source = 'value' | 'field' | 'formula'
  const [startDate, setStartDate] = useState('');
  const [startDateSource, setStartDateSource] = useState('value');
  const [startDateField, setStartDateField] = useState('');
  const [startDateFormula, setStartDateFormula] = useState('');
  const [endDate, setEndDate] = useState('');
  const [endDateSource, setEndDateSource] = useState('value');
  const [endDateField, setEndDateField] = useState('');
  const [endDateFormula, setEndDateFormula] = useState('');
  // Number-based: just a count of periods
  const [periodCount, setPeriodCount] = useState('12');
  const [frequency, setFrequency] = useState('M');
  const [convention, setConvention] = useState('');
  const [columns, setColumns] = useState([
    { name: 'date', formula: 'period_date' },
  ]);
  const [contextVars, setContextVars] = useState([
    { name: '', value: '' },
  ]);
  const [showPresets, setShowPresets] = useState(true);
  const [showCode, setShowCode] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [createTxn, setCreateTxn] = useState(false);
  const [txnType, setTxnType] = useState('');
  const [txnAmountCol, setTxnAmountCol] = useState('');
  const [extractFirst, setExtractFirst] = useState(false);
  const [extractLast, setExtractLast] = useState(false);
  const [extractColumn, setExtractColumn] = useState('');
  // Schedule Sum toggle
  const [enableSum, setEnableSum] = useState(false);
  const [sumColumn, setSumColumn] = useState('');
  const [sumVarName, setSumVarName] = useState('');
  // Schedule Column toggle
  const [enableCol, setEnableCol] = useState(false);
  const [colColumn, setColColumn] = useState('');
  const [colVarName, setColVarName] = useState('');
  // Schedule Filter toggle
  const [enableFilter, setEnableFilter] = useState(false);
  const [filterCondition, setFilterCondition] = useState('');
  const [filterVarName, setFilterVarName] = useState('');

  const dateEventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.filter(f => f.datatype === 'date' || f.name.includes('date')).forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  const addColumn = useCallback((preset) => {
    if (preset) {
      // Check if already added
      if (columns.some(c => c.name === preset.name)) return;
      setColumns(prev => [...prev, { name: preset.name, formula: preset.formula }]);
    } else {
      setColumns(prev => [...prev, { name: '', formula: '' }]);
    }
  }, [columns]);

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

  const addContextVar = useCallback(() => {
    setContextVars(prev => [...prev, { name: '', value: '' }]);
  }, []);

  const updateContextVar = useCallback((index, field, value) => {
    setContextVars(prev => prev.map((v, i) => i === index ? { ...v, [field]: value } : v));
  }, []);

  const removeContextVar = useCallback((index) => {
    setContextVars(prev => prev.filter((_, i) => i !== index));
  }, []);

  const usedPresets = useMemo(() => new Set(columns.map(c => c.name)), [columns]);

  const generatedCode = useMemo(() => {
    const lines = [];
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push(`## ${(scheduleName || 'SCHEDULE').toUpperCase()}`);
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push('');

    // Context variables
    const validVars = contextVars.filter(v => v.name && v.value);
    validVars.forEach(v => {
      lines.push(`${v.name} = ${v.value}`);
    });
    if (validVars.length > 0) lines.push('');

    // Period definition
    if (periodType === 'number') {
      // Number-based period: period(count)
      lines.push('## Schedule Period');
      lines.push(`p = period(${periodCount || 12})`);
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

    // Context object
    const contextPairs = validVars.map(v => `"${v.name}": ${v.name}`);
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

    // Schedule Filter
    if (enableFilter && filterCondition && filterVarName) {
      lines.push('');
      lines.push(`${filterVarName} = schedule_filter(sched, "${filterCondition}")`);
      lines.push(`print(${filterVarName})`);
    }

    // Create transaction from schedule results
    if (createTxn && txnType && txnAmountCol) {
      lines.push('');
      lines.push('## Create Transaction');
      lines.push(`total_amount = schedule_sum(sched, "${txnAmountCol}")`);
      lines.push(`createTransaction(postingdate, postingdate, "${txnType}", total_amount)`);
    }

    return lines.join('\n');
  }, [scheduleName, periodType, startDate, startDateSource, startDateField, startDateFormula, endDate, endDateSource, endDateField, endDateFormula, periodCount, frequency, convention, columns, contextVars, createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn, enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName, enableFilter, filterCondition, filterVarName]);

  const handleTest = useCallback(async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: generatedCode, posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const outputs = [];
        if (data.print_outputs?.length > 0) outputs.push(...data.print_outputs.map(p => String(p)));
        if (data.transactions?.length > 0) outputs.push(`Generated ${data.transactions.length} transaction(s)`);
        setTestResult({ success: true, output: outputs.join('\n') || 'Executed successfully (no output)' });
      } else {
        const errMsg = data.error || (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail)) || 'Execution failed';
        setTestResult({ success: false, error: errMsg });
      }
    } catch (err) {
      setTestResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setTesting(false);
    }
  }, [generatedCode]);

  const handleSave = useCallback(async () => {
    if (!scheduleName.trim()) {
      setSaveResult({ success: false, error: 'Please enter a schedule name before saving.' });
      return;
    }
    if (schedulePriority === '' || schedulePriority === null || schedulePriority === undefined) {
      setSaveResult({ success: false, error: 'Please enter a schedule priority before saving.' });
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
          periodCount, frequency, convention, columns, contextVars,
          createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn,
          enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName, enableFilter, filterCondition, filterVarName,
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
  }, [scheduleName, schedulePriority, scheduleId, generatedCode, periodType, startDate, startDateSource, startDateField, startDateFormula, endDate, endDateSource, endDateField, endDateFormula, periodCount, frequency, convention, columns, contextVars, createTxn, txnType, txnAmountCol, extractFirst, extractLast, extractColumn, enableSum, sumColumn, sumVarName, enableCol, colColumn, colVarName, enableFilter, filterCondition, filterVarName, onSave]);

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
            required
            error={!scheduleName.trim()}
            helperText={!scheduleName.trim() ? 'Required' : ''}
            sx={{ flex: 1 }} />
          <TextField size="small" label="Schedule Priority *" value={schedulePriority}
            onChange={(e) => { const v = e.target.value; if (v === '' || /^\d+$/.test(v)) setSchedulePriority(v === '' ? '' : Number(v)); }}
            placeholder="e.g., 2"
            type="number"
            required
            error={schedulePriority === '' || schedulePriority === null || schedulePriority === undefined}
            helperText={schedulePriority === '' || schedulePriority === null || schedulePriority === undefined ? 'Required' : ''}
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
          <Box sx={{ display: 'flex', gap: 1.5, mb: 2.5 }}>
            <TextField size="small" label="Number of Periods" type="number" value={periodCount}
              onChange={(e) => setPeriodCount(e.target.value)} sx={{ flex: '0 0 180px' }}
              placeholder="e.g., 12" helperText="e.g., 12 for monthly, 60 for 5 years" />
          </Box>
        ) : (
          <>
            <Box sx={{ display: 'flex', gap: 1.5, mb: 1.5 }}>
              {/* Start Date */}
              <Box sx={{ flex: 1 }}>
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
                  <TextField size="small" fullWidth label="Start Date" type="date" value={startDate}
                    onChange={(e) => setStartDate(e.target.value)} InputLabelProps={{ shrink: true }} />
                )}
                {startDateSource === 'field' && (
                  <FormControl size="small" fullWidth>
                    <InputLabel>Start Date Field</InputLabel>
                    <Select value={startDateField} label="Start Date Field"
                      onChange={(e) => setStartDateField(e.target.value)}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {startDateSource === 'formula' && (
                  <FormulaBar
                    value={startDateFormula}
                    onChange={setStartDateFormula}
                    events={events}
                    label="Start Date Formula"
                    placeholder='e.g., add_months(effectivedate, 12)'
                  />
                )}
              </Box>

              {/* End Date */}
              <Box sx={{ flex: 1 }}>
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
                  <TextField size="small" fullWidth label="End Date" type="date" value={endDate}
                    onChange={(e) => setEndDate(e.target.value)} InputLabelProps={{ shrink: true }} />
                )}
                {endDateSource === 'field' && (
                  <FormControl size="small" fullWidth>
                    <InputLabel>End Date Field</InputLabel>
                    <Select value={endDateField} label="End Date Field"
                      onChange={(e) => setEndDateField(e.target.value)}>
                      {dateEventFields.map(f => <MenuItem key={f} value={f}>{f}</MenuItem>)}
                    </Select>
                  </FormControl>
                )}
                {endDateSource === 'formula' && (
                  <FormulaBar
                    value={endDateFormula}
                    onChange={setEndDateFormula}
                    events={events}
                    label="End Date Formula"
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

        {/* Context Variables */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="body2" fontWeight={600}>
            <Hash size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
            Input Parameters
          </Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={addContextVar}>Add</Button>
        </Box>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mb: 2 }}>
          {contextVars.map((v, idx) => (
            <Box key={idx} sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
              <TextField size="small" label="Name" value={v.name}
                onChange={(e) => updateContextVar(idx, 'name', e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                sx={{ flex: '0 0 160px' }} placeholder="e.g., principal" />
              <TextField size="small" label="Value" value={v.value} fullWidth
                onChange={(e) => updateContextVar(idx, 'value', e.target.value)}
                placeholder="e.g., 100000" />
              <IconButton size="small" onClick={() => removeContextVar(idx)} sx={{ color: '#F44336' }}>
                <Trash2 size={14} />
              </IconButton>
            </Box>
          ))}
        </Box>

        <Divider sx={{ mb: 2 }} />

        {/* Column Presets */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="body2" fontWeight={600}>
            <Columns size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
            Quick Add Columns
          </Typography>
          <Button size="small" onClick={() => setShowPresets(!showPresets)} color="inherit">
            {showPresets ? 'Hide' : 'Show'}
          </Button>
        </Box>
        {showPresets && (
          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mb: 2 }}>
            {COLUMN_PRESETS.map((preset) => (
              <Tooltip key={preset.name} title={`${preset.description} — ${preset.formula}`} arrow>
                <Chip
                  label={preset.name}
                  size="small"
                  variant={usedPresets.has(preset.name) ? 'filled' : 'outlined'}
                  color={usedPresets.has(preset.name) ? 'primary' : 'default'}
                  disabled={usedPresets.has(preset.name)}
                  onClick={() => addColumn(preset)}
                  sx={{ cursor: 'pointer', fontSize: '0.75rem' }}
                />
              </Tooltip>
            ))}
          </Box>
        )}

        {/* Column Definitions */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="body2" fontWeight={600}>
            Schedule Columns ({columns.length})
          </Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={() => addColumn(null)}>
            Custom Column
          </Button>
        </Box>

        {columns.map((col, idx) => (
          <ColumnCard key={idx} column={col} index={idx} events={events}
            variables={contextVars.filter(v => v.name).map(v => v.name)}
            onUpdate={updateColumn} onRemove={removeColumn}
            onMoveUp={() => moveColumn(idx, -1)} onMoveDown={() => moveColumn(idx, 1)}
            isFirst={idx === 0} isLast={idx === columns.length - 1} />
        ))}

        {/* Table Preview */}
        {previewHeaders.length > 0 && (
          <>
            <Divider sx={{ my: 2 }} />
            <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
              <BarChart3 size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
              Table Preview (structure)
            </Typography>
            <TableContainer component={Paper} variant="outlined" sx={{ mb: 2 }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: '#F8F9FA' }}>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem' }}>#</TableCell>
                    {previewHeaders.map(h => (
                      <TableCell key={h} sx={{ fontWeight: 600, fontSize: '0.75rem' }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {[1, 2, 3].map(row => (
                    <TableRow key={row} sx={{ '&:last-child td': { borderBottom: 0 } }}>
                      <TableCell sx={{ fontSize: '0.75rem', color: '#6C757D' }}>{row}</TableCell>
                      {previewHeaders.map(h => (
                        <TableCell key={h} sx={{ fontSize: '0.75rem', color: '#ADB5BD', fontStyle: 'italic' }}>
                          {h === 'date' || h.includes('date') ? '2026-01-31' : '...'}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
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
              <FormControl size="small" fullWidth sx={{ mb: 1 }}>
                <InputLabel>Column to extract (first/last)</InputLabel>
                <Select value={extractColumn} label="Column to extract (first/last)"
                  onChange={(e) => setExtractColumn(e.target.value)}>
                  {columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number').map(c => (
                    <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            )}
            {enableSum && (
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
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
              </Box>
            )}
            {enableCol && (
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
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
              </Box>
            )}
            {enableFilter && (
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
                <TextField size="small" label="Variable Name" value={filterVarName}
                  onChange={(e) => setFilterVarName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                  placeholder="e.g., positive_rows" sx={{ flex: 1 }} />
                <TextField size="small" label="Filter Condition" value={filterCondition}
                  onChange={(e) => setFilterCondition(e.target.value)}
                  placeholder='e.g., gt(balance, 0)' sx={{ flex: 1 }}
                  InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.8125rem' } }} />
              </Box>
            )}
          </CardContent>
        </Card>

        {/* Create transaction */}
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <Typography variant="body2">Create transaction from schedule total</Typography>
              <Switch checked={createTxn} onChange={(e) => setCreateTxn(e.target.checked)} size="small" />
            </Box>
            {createTxn && (
              <Box sx={{ mt: 1, display: 'flex', gap: 1 }}>
                <TextField size="small" label="Transaction Type" value={txnType}
                  onChange={(e) => setTxnType(e.target.value)} sx={{ flex: 1 }}
                  placeholder="e.g., Interest Accrual" />
                <FormControl size="small" sx={{ flex: 1 }}>
                  <InputLabel>Amount Column</InputLabel>
                  <Select value={txnAmountCol} label="Amount Column"
                    onChange={(e) => setTxnAmountCol(e.target.value)}>
                    {columns.filter(c => c.name && c.formula !== 'period_date' && c.formula !== 'period_number').map(c => (
                      <MenuItem key={c.name} value={c.name}>{c.name}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
            )}
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

        {/* Test Results */}
        {testResult && (
          <Alert severity={testResult.success ? 'success' : 'error'} sx={{ mt: 2, '& .MuiAlert-message': { width: '100%' } }}
            onClose={() => setTestResult(null)}>
            {testResult.success ? (
              <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', whiteSpace: 'pre-wrap', maxHeight: 200, overflow: 'auto' }}>
                {testResult.output}
              </pre>
            ) : (
              <Typography variant="body2">{testResult.error}</Typography>
            )}
          </Alert>
        )}
        {/* Save Result */}
        {saveResult && (
          <Alert severity={saveResult.success ? 'success' : 'error'} sx={{ mt: 2, '& .MuiAlert-message': { width: '100%' } }}
            onClose={() => setSaveResult(null)}>
            <Typography variant="body2">{saveResult.success ? saveResult.output : saveResult.error}</Typography>
          </Alert>
        )}
      </Box>

      {/* Action Bar */}
      <Box sx={{ p: 2, borderTop: '1px solid #E9ECEF', bgcolor: 'white', display: 'flex', gap: 1, justifyContent: 'flex-end', flexShrink: 0 }}>
        {onClose && <Button onClick={onClose} color="inherit">Cancel</Button>}
        <Button variant="outlined" onClick={handleTest} disabled={testing}
          startIcon={testing ? <CircularProgress size={16} /> : <FlaskConical size={16} />}
          sx={{ borderColor: '#4CAF50', color: '#4CAF50', '&:hover': { borderColor: '#388E3C', bgcolor: '#E8F5E9' } }}>
          {testing ? 'Testing...' : 'Test'}
        </Button>
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
