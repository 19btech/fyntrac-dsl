import React, { useState, useMemo, useCallback, useEffect, useRef } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, CircularProgress, Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
  Menu, Autocomplete,
} from "@mui/material";
import {
  Plus, Trash2, Play, Code, Save, X,
  Calculator, GitBranch, Repeat, GripVertical, Edit3, ChevronDown, Calendar, Copy, Receipt,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";
import ScheduleStepModal from "./ScheduleStepModal";
import CustomCodeStepModal from "./CustomCodeStepModal";
import TestResultCard from "./TestResultCard";

// Step-level testing always runs against the EARLIEST loaded posting date so
// behaviour is deterministic across the whole UI. The /event-data/posting-dates
// endpoint already returns dates sorted ascending, so we just take index 0.
// Falls back to "today" only when no event data has been loaded.
const fetchEarliestPostingDate = async () => {
  try {
    const res = await fetch(`${API}/event-data/posting-dates`);
    if (res.ok) {
      const data = await res.json();
      const dates = data?.posting_dates || [];
      if (dates.length > 0) return dates[0];
    }
  } catch { /* ignore — fall through to today */ }
  return new Date().toISOString().split('T')[0];
};

// ─── Step type metadata ────────────────────────────────────────────────
const STEP_TYPE_META = {
  calc:        { label: 'Calculation', color: '#5B5FED', icon: Calculator },
  condition:   { label: 'Condition',   color: '#FF9800', icon: GitBranch },
  iteration:   { label: 'Iteration',   color: '#00BCD4', icon: Repeat },
  schedule:    { label: 'Schedule',    color: '#9C27B0', icon: Calendar },
  custom_code: { label: 'CustomCode',  color: '#607D8B', icon: Code },
};

// ─── Identifier-aware test print ───────────────────────────────────────
// When the rule references events, the backend runs the DSL once per
// (instrument, subinstrument) at the chosen posting date. To make the
// per-step test results meaningful, we tag each print with the row's
// instrument + sub-instrument identifier so the UI can render a table.
const _hasEventRefs = (code) => /\b[A-Z][A-Z0-9_]*\.[a-zA-Z_]\w*/.test(code || '');
const _testPrintLine = (varName, codeSoFar) =>
  _hasEventRefs(codeSoFar)
    ? `print("__TEST_ROW__|" + str(instrumentid) + "|" + str(subinstrumentid) + "| ${varName} =", ${varName})`
    : `print("${varName} =", ${varName})`;


// ─── Helper: build DSL line for a single calc variable ─────────────────
const buildCalcLine = (v) => {
  if (v.source === 'value')       return `${v.name} = ${v.value || 0}`;
  if (v.source === 'event_field') return `${v.name} = ${v.eventField}`;
  if (v.source === 'formula')     return `${v.name} = ${v.formula || 0}`;
  if (v.source === 'collect')     return `${v.name} = ${v.collectType || 'collect'}(${v.eventField})`;
  return null;
};

// ─── Helper: build nested condition expression ─────────────────────────
const buildConditionExpr = (conditions, elseFormula) => {
  const valid = conditions.filter(c => c.condition);
  if (valid.length === 0) return elseFormula || '0';
  let nested = elseFormula || '0';
  for (let i = valid.length - 1; i >= 0; i--) {
    const c = valid[i];
    // If this condition has nested sub-conditions, build those as the "then"
    const thenPart = (c.nestedConditions?.length > 0)
      ? buildConditionExpr(c.nestedConditions, c.nestedElse || c.thenFormula || '0')
      : (c.thenFormula || '0');
    nested = `if(${c.condition}, ${thenPart}, ${nested})`;
  }
  return nested;
};

// ─── Helper: build iteration lines for a step ──────────────────────────
const buildIterationLines = (iters, availableVarNames) => {
  const lines = [];
  const iterResultVars = [];
  for (const iter of iters) {
    const available = [...availableVarNames, ...iterResultVars];
    const exprIds = new Set((iter.expression || '').match(/[a-zA-Z_][a-zA-Z0-9_]*/g) || []);
    if (iter.sourceArray && /^[a-zA-Z_]\w*$/.test(iter.sourceArray)) exprIds.add(iter.sourceArray);
    if (iter.secondArray && /^[a-zA-Z_]\w*$/.test(iter.secondArray)) exprIds.add(iter.secondArray);
    const ctx = available.filter(v => exprIds.has(v));
    const ctxStr = ctx.length ? `, {${ctx.map(v => `"${v}": ${v}`).join(', ')}}` : '';
    if (iter.type === 'apply_each') {
      lines.push(`${iter.resultVar} = apply_each(${iter.sourceArray}, "${iter.expression}"${ctxStr})`);
    } else if (iter.type === 'apply_each_paired') {
      lines.push(`${iter.resultVar} = apply_each(${iter.sourceArray}, ${iter.secondArray || '[]'}, "${iter.expression}"${ctxStr})`);
    } else if (iter.type === 'map_array') {
      lines.push(`${iter.resultVar} = map_array(${iter.sourceArray}, "${iter.varName}", "${iter.expression}"${ctxStr})`);
    } else {
      lines.push(`${iter.resultVar} = for_each(${iter.sourceArray}, ${iter.secondArray || '[]'}, "${iter.varName}", "${iter.secondVar}", "${iter.expression}")`);
    }
    iterResultVars.push(iter.resultVar);
  }
  return lines;
};


// ═══════════════════════════════════════════════════════════════════════
// CalcForm — form fields for a Calculation step (used inside StepModal)
// ═══════════════════════════════════════════════════════════════════════
const CalcForm = ({ step, onChange, events, definedVarNames }) => {
  const eventFields = useMemo(() => {
    if (!events?.length) return [];
    const r = [];
    events.forEach(ev => {
      ['postingdate', 'effectivedate', 'subinstrumentid'].forEach(sf => r.push(`${ev.event_name}.${sf}`));
      ev.fields.forEach(f => r.push(`${ev.event_name}.${f.name}`));
    });
    return r;
  }, [events]);

  return (
    <>
      <FormControl size="small" fullWidth sx={{ mb: 2 }}>
        <InputLabel>Source</InputLabel>
        <Select value={step.source || 'formula'} label="Source"
          onChange={(e) => onChange({ ...step, source: e.target.value })}>
          <MenuItem value="formula">Formula</MenuItem>
          <MenuItem value="value">Fixed Value</MenuItem>
          <MenuItem value="event_field">Event Field</MenuItem>
          <MenuItem value="collect">Collect from Events</MenuItem>
        </Select>
      </FormControl>

      {step.source === 'formula' && (
        <FormulaBar value={step.formula || ''} onChange={(val) => onChange({ ...step, formula: val })}
          events={events} variables={definedVarNames}
          label="Formula" placeholder="e.g., multiply(principal, rate)" />
      )}
      {step.source === 'value' && (
        <TextField size="small" fullWidth label="Value" value={step.value || ''}
          onChange={(e) => onChange({ ...step, value: e.target.value })}
          placeholder='e.g., 100000 or "2026-01-01"' />
      )}
      {step.source === 'event_field' && (
        <FormControl fullWidth size="small">
          <InputLabel>Event Field</InputLabel>
          <Select value={step.eventField || ''} label="Event Field"
            onChange={(e) => onChange({ ...step, eventField: e.target.value })}>
            <MenuItem value="" disabled><em>Select event field...</em></MenuItem>
            {eventFields.map(ef => <MenuItem key={ef} value={ef}>{ef}</MenuItem>)}
          </Select>
        </FormControl>
      )}
      {step.source === 'collect' && (
        <Box sx={{ display: 'flex', gap: 1 }}>
          <FormControl size="small" sx={{ minWidth: 180 }}>
            <InputLabel>Collect Type</InputLabel>
            <Select value={step.collectType || 'collect'} label="Collect Type"
              onChange={(e) => onChange({ ...step, collectType: e.target.value })}>
              <MenuItem value="collect">collect (by posting date)</MenuItem>
              <MenuItem value="collect_by_instrument">collect_by_instrument</MenuItem>
              <MenuItem value="collect_all">collect_all</MenuItem>
              <MenuItem value="collect_by_subinstrument">collect_by_subinstrument</MenuItem>
              <MenuItem value="collect_subinstrumentids">collect_subinstrumentids</MenuItem>
            </Select>
          </FormControl>
          <FormControl fullWidth size="small">
            <InputLabel>Event Field</InputLabel>
            <Select value={step.eventField || ''} label="Event Field"
              onChange={(e) => onChange({ ...step, eventField: e.target.value })}>
              <MenuItem value="" disabled><em>Select...</em></MenuItem>
              {eventFields.map(ef => <MenuItem key={ef} value={ef}>{ef}</MenuItem>)}
            </Select>
          </FormControl>
        </Box>
      )}
    </>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// ConditionForm — form fields for a Condition step (used inside StepModal)
// Supports nested conditions within each branch.
// ═══════════════════════════════════════════════════════════════════════
const ConditionBranch = ({ cond, index, events, definedVarNames, onChange, onRemove }) => {
  const [showNested, setShowNested] = useState(!!(cond.nestedConditions?.length > 0));

  return (
    <Card sx={{ mb: 1, borderLeft: '3px solid #FF9800' }}>
      <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <Typography variant="caption" fontWeight={600} color="text.secondary">
            {index === 0 ? 'IF' : 'ELSE IF'}
          </Typography>
          <IconButton size="small" onClick={onRemove} sx={{ color: '#F44336' }}><Trash2 size={14} /></IconButton>
        </Box>
        <Box sx={{ mb: 1 }}>
          <FormulaBar value={cond.condition || ''} onChange={(val) => onChange({ ...cond, condition: val })}
            events={events} variables={definedVarNames}
            label="Condition" placeholder="e.g., gt(balance, 0)" />
        </Box>

        {!showNested ? (
          <>
            <FormulaBar value={cond.thenFormula || ''} onChange={(val) => onChange({ ...cond, thenFormula: val })}
              events={events} variables={definedVarNames}
              label="Then (result)" placeholder="e.g., multiply(balance, rate)" />
            <Button size="small" sx={{ mt: 0.5, fontSize: '0.7rem' }}
              onClick={() => {
                setShowNested(true);
                onChange({ ...cond, nestedConditions: cond.nestedConditions?.length ? cond.nestedConditions : [{ condition: '', thenFormula: '' }], nestedElse: cond.nestedElse || '' });
              }}>
              + Add Nested Condition
            </Button>
          </>
        ) : (
          <Box sx={{ ml: 2, mt: 1, borderLeft: '2px solid #E0E0E0', pl: 1.5 }}>
            <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>
              Nested Conditions (inside THEN)
            </Typography>
            {(cond.nestedConditions || []).map((nc, ni) => (
              <Card key={ni} sx={{ mb: 1, borderLeft: '2px solid #FFC107' }}>
                <CardContent sx={{ p: 1, '&:last-child': { pb: 1 } }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
                    <Typography variant="caption" fontWeight={600} color="text.secondary">
                      {ni === 0 ? 'IF' : 'ELSE IF'}
                    </Typography>
                    <IconButton size="small" onClick={() => {
                      const updated = [...(cond.nestedConditions || [])];
                      updated.splice(ni, 1);
                      if (updated.length === 0) { setShowNested(false); onChange({ ...cond, nestedConditions: [], nestedElse: '' }); }
                      else onChange({ ...cond, nestedConditions: updated });
                    }} sx={{ color: '#F44336' }}><Trash2 size={12} /></IconButton>
                  </Box>
                  <FormulaBar value={nc.condition || ''} onChange={(val) => {
                    const updated = [...(cond.nestedConditions || [])];
                    updated[ni] = { ...updated[ni], condition: val };
                    onChange({ ...cond, nestedConditions: updated });
                  }} events={events} variables={definedVarNames}
                    label="Condition" placeholder="e.g., gt(term, 24)" />
                  <Box sx={{ mt: 0.5 }}>
                    <FormulaBar value={nc.thenFormula || ''} onChange={(val) => {
                      const updated = [...(cond.nestedConditions || [])];
                      updated[ni] = { ...updated[ni], thenFormula: val };
                      onChange({ ...cond, nestedConditions: updated });
                    }} events={events} variables={definedVarNames}
                      label="Then" placeholder="e.g., multiply(rate, 0.90)" />
                  </Box>
                </CardContent>
              </Card>
            ))}
            <FormulaBar value={cond.nestedElse || ''} onChange={(val) => onChange({ ...cond, nestedElse: val })}
              events={events} variables={definedVarNames}
              label="Nested ELSE (default)" placeholder="e.g., rate" />
            <Box sx={{ display: 'flex', gap: 1, mt: 0.5 }}>
              <Button size="small" sx={{ fontSize: '0.7rem' }}
                onClick={() => onChange({ ...cond, nestedConditions: [...(cond.nestedConditions || []), { condition: '', thenFormula: '' }] })}>
                + Add Nested Branch
              </Button>
              <Button size="small" color="inherit" sx={{ fontSize: '0.7rem' }}
                onClick={() => { setShowNested(false); onChange({ ...cond, nestedConditions: [], nestedElse: '' }); }}>
                Remove Nesting
              </Button>
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

const ConditionForm = ({ step, onChange, events, definedVarNames }) => {
  const conditions = step.conditions || [{ condition: '', thenFormula: '' }];
  const updateCond = (i, updated) => {
    const arr = [...conditions];
    arr[i] = updated;
    onChange({ ...step, conditions: arr });
  };
  const removeCond = (i) => {
    const arr = conditions.filter((_, j) => j !== i);
    onChange({ ...step, conditions: arr.length ? arr : [{ condition: '', thenFormula: '' }] });
  };

  return (
    <>
      {conditions.map((cond, i) => (
        <ConditionBranch key={i} cond={cond} index={i} events={events} definedVarNames={definedVarNames}
          onChange={(u) => updateCond(i, u)} onRemove={() => removeCond(i)} />
      ))}
      <Card sx={{ mb: 1, borderLeft: '3px solid #9E9E9E' }}>
        <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
          <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>ELSE (default)</Typography>
          <FormulaBar value={step.elseFormula || ''} onChange={(val) => onChange({ ...step, elseFormula: val })}
            events={events} variables={definedVarNames}
            label="Default value" placeholder="e.g., 0" />
        </CardContent>
      </Card>
      <Button size="small" startIcon={<Plus size={14} />}
        onClick={() => onChange({ ...step, conditions: [...conditions, { condition: '', thenFormula: '' }] })}>
        Add Condition
      </Button>
    </>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// IterationForm — form fields for an Iteration step (used inside StepModal)
// ═══════════════════════════════════════════════════════════════════════
const IterationForm = ({ step, onChange, events, definedVarNames }) => {
  const rawIterations = step.iterations || [{ type: 'apply_each', sourceArray: '', varName: 'each', expression: '', resultVar: step.name || 'mapped_result', secondArray: '', secondVar: 'second' }];
  // Always keep the last iteration's resultVar in sync with the top-level step name
  const iterations = rawIterations.map((it, i) =>
    i === rawIterations.length - 1 ? { ...it, resultVar: step.name || it.resultVar } : it
  );

  const updateIter = (idx, field, value) => {
    const arr = [...iterations];
    arr[idx] = { ...arr[idx], [field]: value };
    onChange({ ...step, iterations: arr });
  };
  const removeIter = (idx) => {
    const arr = iterations.filter((_, i) => i !== idx);
    onChange({ ...step, iterations: arr.length ? arr : [{ type: 'apply_each', sourceArray: '', varName: 'each', expression: '', resultVar: step.name || 'mapped_result', secondArray: '', secondVar: 'second' }] });
  };

  const varOptions = [...new Set([
    ...definedVarNames,
    ...iterations.filter(it => it.resultVar).map(it => it.resultVar),
  ])];

  return (
    <>
      {iterations.map((iter, idx) => (
        <Card key={idx} variant="outlined" sx={{ mb: 1.5, bgcolor: '#FAFAFA' }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="caption" fontWeight={600} color="text.secondary">
                {iterations.length > 1 ? `Iteration Step ${idx + 1}` : 'Iteration'}
              </Typography>
              {iterations.length > 1 && (
                <IconButton size="small" onClick={() => removeIter(idx)} sx={{ color: '#999' }}><Trash2 size={14} /></IconButton>
              )}
            </Box>
            <Box sx={{ display: 'flex', gap: 1, mb: 1.5 }}>
              <FormControl size="small" sx={{ minWidth: 240 }}>
                <InputLabel>Mode</InputLabel>
                <Select value={iter.type} label="Mode" onChange={(e) => updateIter(idx, 'type', e.target.value)}>
                  <MenuItem value="apply_each">Each Item — apply formula to every item</MenuItem>
                  <MenuItem value="apply_each_paired">Paired Items — process two arrays together</MenuItem>
                </Select>
              </FormControl>
              {idx < iterations.length - 1 && (
                <TextField size="small" label="Variable Name" value={iter.resultVar}
                  onChange={(e) => updateIter(idx, 'resultVar', e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
                  sx={{ flex: '0 0 150px' }} />
              )}
            </Box>
            <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
              <FormControl size="small" fullWidth>
                <InputLabel>Source Array</InputLabel>
                <Select value={iter.sourceArray} label="Source Array"
                  onChange={(e) => updateIter(idx, 'sourceArray', e.target.value)}
                  sx={{ fontFamily: 'monospace' }}>
                  {varOptions.map(name => <MenuItem key={name} value={name} sx={{ fontFamily: 'monospace' }}>{name}</MenuItem>)}
                </Select>
              </FormControl>
            </Box>
            {iter.type === 'apply_each_paired' && (
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
                <FormControl size="small" fullWidth>
                  <InputLabel>Second Array</InputLabel>
                  <Select value={iter.secondArray} label="Second Array"
                    onChange={(e) => updateIter(idx, 'secondArray', e.target.value)}
                    sx={{ fontFamily: 'monospace' }}>
                    {varOptions.map(name => <MenuItem key={name} value={name} sx={{ fontFamily: 'monospace' }}>{name}</MenuItem>)}
                  </Select>
                </FormControl>
              </Box>
            )}
            {(iter.type === 'apply_each' || iter.type === 'apply_each_paired') && (
              <Alert severity="info" sx={{ mb: 1, py: 0.25 }}>
                <Typography variant="caption">
                  {iter.type === 'apply_each'
                    ? "Use 'each' for the current item, 'index' for position, 'count' for total"
                    : "Use 'first' and 'second' for items from each array, 'index' for position"}
                </Typography>
              </Alert>
            )}
            <FormulaBar value={iter.expression} onChange={(val) => updateIter(idx, 'expression', val)}
              events={events} variables={varOptions}
              label={iter.type === 'apply_each' ? "Formula (use 'each' for current item)" : iter.type === 'apply_each_paired' ? "Formula (use 'first' and 'second')" : "Expression"}
              placeholder={iter.type === 'apply_each' ? 'e.g., multiply(each, 1.1)' : iter.type === 'apply_each_paired' ? 'e.g., multiply(first, second)' : 'e.g., multiply(item, 1.1)'} />
          </CardContent>
        </Card>
      ))}
      <Button size="small" startIcon={<Plus size={14} />}
        onClick={() => onChange({ ...step, iterations: [...iterations, { type: 'apply_each', sourceArray: '', varName: 'each', expression: '', resultVar: 'result_' + (iterations.length + 1), secondArray: '', secondVar: 'second' }] })}>
        Add Chained Iteration
      </Button>
    </>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// StepModal — Dialog for creating / editing a step
// ═══════════════════════════════════════════════════════════════════════
const StepModal = ({ open, step, stepType, onClose, onSaveStep, events, definedVarNames, onTest, generatedCode }) => {
  const [local, setLocal] = useState(step || {});
  const [testResult, setTestResult] = useState(null);
  const [testing, setTesting] = useState(false);
  const [showCode, setShowCode] = useState(false);
  const [localInlineComment, setLocalInlineComment] = useState(step?.inlineComment || false);
  const [localCommentText, setLocalCommentText] = useState(step?.commentText || '');
  const [localPrintResult, setLocalPrintResult] = useState(step?.printResult !== undefined ? step.printResult : true);

  // Reset local state when step changes (open new modal)
  useEffect(() => {
    if (open) {
      setLocal(step || {});
      setTestResult(null);
      setShowCode(false);
      setLocalInlineComment(step?.inlineComment || false);
      setLocalCommentText(step?.commentText || '');
      setLocalPrintResult(step?.printResult !== undefined ? step.printResult : true);
    }
  }, [open, step]);

  const title = step?.name ? `Edit Step: ${step.name}` :
    stepType === 'calc' ? 'Add Calculation Step' :
    stepType === 'condition' ? 'Add Condition Step' :
    'Add Iteration Step';

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const result = await onTest(local);
      setTestResult(result);
    } catch (e) {
      setTestResult({ success: false, error: e.message });
    } finally {
      setTesting(false);
    }
  };

  const handleSave = () => {
    if (!local.name) return;
    onSaveStep({ ...local, stepType: local.stepType || stepType, inlineComment: localInlineComment, commentText: localCommentText, printResult: localPrintResult });
    onClose();
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth
      PaperProps={{ sx: { maxHeight: '85vh' } }}>
      <DialogTitle sx={{ pb: 1, borderBottom: '1px solid #E9ECEF' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1 }}>
          {React.createElement(STEP_TYPE_META[local.stepType || stepType]?.icon || Calculator, { size: 20, color: STEP_TYPE_META[local.stepType || stepType]?.color })}
          <Typography variant="h6" sx={{ flex: 1 }}>{title}</Typography>
          <IconButton size="small" onClick={onClose} sx={{ color: '#6C757D' }}><X size={18} /></IconButton>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 1, overflow: 'auto' }}>
        <TextField size="small" fullWidth label="Variable Name *" value={local.name || ''}
          onChange={(e) => setLocal(prev => ({ ...prev, name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') }))}
          placeholder="e.g., monthly_payment"
          sx={{ mb: 2, mt: 1 }} />

        {(local.stepType || stepType) === 'calc' && (
          <CalcForm step={local} onChange={setLocal} events={events} definedVarNames={definedVarNames} />
        )}
        {(local.stepType || stepType) === 'condition' && (
          <ConditionForm step={local} onChange={setLocal} events={events} definedVarNames={definedVarNames} />
        )}
        {(local.stepType || stepType) === 'iteration' && (
          <IterationForm step={local} onChange={setLocal} events={events} definedVarNames={definedVarNames} />
        )}

        {testResult && (
          <TestResultCard
            success={testResult.success}
            output={testResult.output}
            error={testResult.error}
            variableName={testResult.variableName || local?.name}
            onClose={() => setTestResult(null)}
            sx={{ mt: 2 }}
          />
        )}

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
        {showCode && generatedCode && (
          <Paper variant="outlined" sx={{ mt: 1, p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 200, overflow: 'auto' }}>
            <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
              {generatedCode}
            </pre>
          </Paper>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleSave} disabled={!local.name} variant="contained"
          startIcon={<Save size={14} />}>
          Save Step
        </Button>
      </DialogActions>
    </Dialog>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// TransactionModal — Dialog for creating / editing a transaction
// Groups fields into clear sections (Identity, Amount, Dates, Sub-Instrument)
// with full-width inputs and proper labels. Test runs in-modal via TestResultCard.
// ═══════════════════════════════════════════════════════════════════════
const TXN_COLOR = '#0288D1';

const TransactionModal = ({ open, txn, onClose, onSaveTxn, onTest, amountOptions, subIdOptions, eventFieldOptions }) => {
  const [local, setLocal] = useState(txn || {});
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);

  useEffect(() => {
    if (open) {
      setLocal(txn || { type: '', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '1.0' });
      setTestResult(null);
    }
  }, [open, txn]);

  const update = (field, val) => setLocal(prev => ({ ...prev, [field]: val }));

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const result = await onTest(local);
      setTestResult(result);
    } catch (e) {
      setTestResult({ success: false, error: e.message });
    } finally {
      setTesting(false);
    }
  };

  const handleSave = () => {
    if (!local.type) return;
    onSaveTxn(local);
    onClose();
  };

  const title = txn?.type ? `Edit Transaction: ${txn.type}` : 'Define Transaction';
  const SectionLabel = ({ children }) => (
    <Typography variant="caption" fontWeight={700} sx={{ color: '#5F6B7A', textTransform: 'uppercase', letterSpacing: 0.5, display: 'block', mb: 0.75 }}>
      {children}
    </Typography>
  );

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth
      PaperProps={{ sx: { maxHeight: '85vh' } }}>
      <DialogTitle sx={{ pb: 1, borderBottom: '1px solid #E9ECEF' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1 }}>
          <Receipt size={20} color={TXN_COLOR} />
          <Typography variant="h6" sx={{ flex: 1 }}>{title}</Typography>
          <IconButton size="small" onClick={onClose} sx={{ color: '#6C757D' }}><X size={18} /></IconButton>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 2, overflow: 'auto' }}>
        {/* ── Identity ── */}
        <SectionLabel>Transaction Identity</SectionLabel>
        <TextField
          size="small" fullWidth required
          label="Transaction Type *"
          value={local.type || ''}
          onChange={(e) => update('type', e.target.value)}
          placeholder="e.g., Calculation Result, Interest Accrual"
          helperText="A short label that identifies this transaction in the ledger."
          sx={{ mb: 2 }}
        />

        <Divider sx={{ my: 1.5 }} />

        {/* ── Amount ── */}
        <SectionLabel>Amount</SectionLabel>
        <Autocomplete
          size="small"
          fullWidth
          freeSolo
          options={[...(amountOptions || []), ...(eventFieldOptions || [])]}
          value={local.amount || ''}
          onInputChange={(_, val) => update('amount', val)}
          onChange={(_, val) => update('amount', val || '')}
          renderInput={(params) => (
            <TextField {...params}
              label="Amount Variable or Expression"
              placeholder="e.g., interest_amount"
              helperText="Pick a variable defined by a step above, or type any expression."
              inputProps={{ ...params.inputProps, style: { fontFamily: 'monospace', fontSize: '0.8125rem' } }}
            />
          )}
          renderOption={(props, option) => (
            <li {...props} style={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{option}</li>
          )}
          noOptionsText={
            <Typography variant="caption" color="text.secondary">
              No variables found. Add a step above first.
            </Typography>
          }
          sx={{ mb: 2 }}
        />

        <Divider sx={{ my: 1.5 }} />

        {/* ── Dates ── */}
        <SectionLabel>Dates</SectionLabel>
        <Box sx={{ display: 'flex', gap: 1.5, mb: 2 }}>
          <FormControl size="small" fullWidth>
            <InputLabel shrink>Posting Date</InputLabel>
            <Select
              label="Posting Date"
              notched
              value={local.postingDate || ''}
              onChange={(e) => update('postingDate', e.target.value)}
              displayEmpty
              renderValue={(val) => val || <em style={{ color: '#999' }}>postingdate</em>}
            >
              <MenuItem value="postingdate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>postingdate</MenuItem>
              <MenuItem value="effectivedate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>effectivedate</MenuItem>
            </Select>
          </FormControl>
          <FormControl size="small" fullWidth>
            <InputLabel shrink>Effective Date</InputLabel>
            <Select
              label="Effective Date"
              notched
              value={local.effectiveDate || ''}
              onChange={(e) => update('effectiveDate', e.target.value)}
              displayEmpty
              renderValue={(val) => val || <em style={{ color: '#999' }}>same as posting</em>}
            >
              <MenuItem value="postingdate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>postingdate</MenuItem>
              <MenuItem value="effectivedate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>effectivedate</MenuItem>
            </Select>
          </FormControl>
        </Box>

        <Divider sx={{ my: 1.5 }} />

        {/* ── Sub-Instrument ── */}
        <SectionLabel>Sub-Instrument</SectionLabel>
        <Autocomplete
          size="small"
          fullWidth
          freeSolo
          options={subIdOptions || []}
          value={local.subInstrumentId || ''}
          onInputChange={(_, val) => update('subInstrumentId', val)}
          onChange={(_, val) => update('subInstrumentId', val || '')}
          renderInput={(params) => (
            <TextField {...params}
              label="Sub-Instrument ID"
              placeholder="default (1.0)"
              helperText="Use a defined sub-instrument list, or leave the default."
              inputProps={{ ...params.inputProps, style: { fontFamily: 'monospace', fontSize: '0.8125rem' } }}
            />
          )}
          renderOption={(props, option) => (
            <li {...props} style={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{option}</li>
          )}
        />

        {testResult && (
          <TestResultCard
            success={testResult.success}
            output={testResult.output}
            error={testResult.error}
            variableName={testResult.variableName || (local.type ? `${local.type} transaction` : 'transaction')}
            onClose={() => setTestResult(null)}
            sx={{ mt: 2 }}
          />
        )}
      </DialogContent>
      <DialogActions sx={{ px: 2, py: 1.5, borderTop: '1px solid #E9ECEF' }}>
        <Button onClick={handleTest} disabled={testing || !local.type}
          startIcon={testing ? <CircularProgress size={14} /> : <Play size={14} />}
          sx={{ color: '#4CAF50' }}>
          {testing ? 'Testing…' : 'Test'}
        </Button>
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose} startIcon={<X size={14} />}>Cancel</Button>
        <Button onClick={handleSave} variant="contained" disabled={!local.type}
          startIcon={<Save size={14} />}
          sx={{ bgcolor: TXN_COLOR, '&:hover': { bgcolor: '#0277BD' } }}>
          Save Transaction
        </Button>
      </DialogActions>
    </Dialog>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// AccountingRuleBuilder — main component
// The main screen shows a flat list of steps. Clicking "Add Step" opens
// a modal for calc / condition / iteration. Steps are draggable.
// ═══════════════════════════════════════════════════════════════════════
const AccountingRuleBuilder = ({ events, dslFunctions, onClose, onSave, initialData }) => {
  // ── Rule-level state ──
  const [ruleName, setRuleName] = useState(initialData?.name || '');
  const [rulePriority, setRulePriority] = useState(initialData?.priority ?? '');
  const [ruleId, setRuleId] = useState(initialData?.id || null);
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState(null);
  const [validationMsg, setValidationMsg] = useState('');
  const [showCode, setShowCode] = useState(false);

  // ── Output options ──
  // Transactions are first-class; the legacy `createTransaction` boolean is kept
  // for backward compat only — the UI now treats `transactions.length > 0` as truth.
  const [outputs, setOutputs] = useState(
    initialData?.outputs?.printResult !== undefined ? initialData.outputs :
    { printResult: true, createTransaction: false, transactions: [] }
  );
  const [inlineComment, setInlineComment] = useState(initialData?.inlineComment || false);
  const [commentText, setCommentText] = useState(initialData?.commentText || '');

  // ── Unified steps array ──
  // Each step: { name, stepType: 'calc'|'condition'|'iteration', source, formula, value, eventField, collectType, conditions, elseFormula, iterations }
  const [steps, setSteps] = useState(() => {
    if (initialData) return convertInitialDataToSteps(initialData);
    return [];
  });

  // ── Saved rules variables (for FormulaBar hints and code generation) ──
  const [savedRulesVarNames, setSavedRulesVarNames] = useState([]);
  const [savedRulesVars, setSavedRulesVars] = useState([]);
  const [savedRulesRaw, setSavedRulesRaw] = useState([]);
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API}/saved-rules`);
        if (!res.ok) return;
        const data = await res.json();
        const rules = (Array.isArray(data) ? data : (data.rules || []))
          .sort((a, b) => (a.priority ?? Infinity) - (b.priority ?? Infinity));
        setSavedRulesRaw(rules);
        const names = new Set();
        const allVars = [];
        rules.forEach(r => {
          // Legacy calc variables
          (r.variables || []).forEach(v => {
            if (v.name && !names.has(v.name)) { names.add(v.name); allVars.push(v); }
          });
          // All step types from unified steps format (condition, iteration, schedule outputVars)
          (r.steps || []).forEach(step => {
            if (step.stepType === 'calc' && step.name && !names.has(step.name)) {
              names.add(step.name);
              allVars.push({ name: step.name, source: step.source || 'formula', formula: step.formula || '', value: step.value || '', eventField: step.eventField || '', collectType: step.collectType || 'collect' });
            } else if (step.stepType === 'condition' && step.name && !names.has(step.name)) {
              names.add(step.name);
              const expr = buildConditionExpr(step.conditions || [], step.elseFormula);
              allVars.push({ name: step.name, source: 'formula', formula: expr, value: '', eventField: '', collectType: 'collect' });
            } else if (step.stepType === 'iteration') {
              (step.iterations || []).forEach(iter => {
                const rv = iter.resultVar;
                if (rv && !names.has(rv)) {
                  names.add(rv);
                  // Store the raw iterData so buildIterationLines can reconstruct the correct DSL line
                  allVars.push({ name: rv, source: 'formula', formula: '', iterData: { ...iter }, _isIterResult: true });
                }
              });
            } else if (step.stepType === 'schedule') {
              (step.outputVars || []).forEach(ov => {
                if (ov.name && !names.has(ov.name)) {
                  names.add(ov.name);
                  const codeLine = (r.generatedCode || '').split('\n').find(l => l.trim().startsWith(ov.name + ' ='));
                  const formula = codeLine ? codeLine.trim().replace(new RegExp('^' + ov.name + '\\s*=\\s*'), '') : '0';
                  // Mark as schedule output — formula references the schedule variable (defined in another
                  // rule's steps section), so it cannot be safely emitted as a standalone dep line.
                  allVars.push({ name: ov.name, source: 'formula', formula, value: '', eventField: '', collectType: 'collect', _isScheduleOutput: true });
                }
              });
            }
          });
          // Legacy iteration result vars (old-format rules without unified steps)
          if (r.ruleType === 'iteration') {
            const iters = r.iterations?.length ? r.iterations : (r.iterConfig?.type ? [r.iterConfig] : []);
            for (const iter of iters) {
              const rv = iter.resultVar;
              if (rv && !names.has(rv)) {
                names.add(rv);
                allVars.push({ name: rv, source: 'formula', formula: '', iterData: { ...iter }, _isIterResult: true });
              }
            }
          }
        });
        setSavedRulesVarNames([...names]);
        setSavedRulesVars(allVars);
      } catch { /* ignore */ }
    })();
  }, []);

  // ── Modal state ──
  const [modalOpen, setModalOpen] = useState(false);
  const [modalStepType, setModalStepType] = useState('calc');
  const [editingStepIndex, setEditingStepIndex] = useState(null); // null = adding new
  const [modalStep, setModalStep] = useState(null);

  // ── Add Step dropdown ──
  const [addMenuAnchor, setAddMenuAnchor] = useState(null);

  // ── Per-step inline test results ──
  const [stepTestResults, setStepTestResults] = useState({});
  const [stepTesting, setStepTesting] = useState({});

  // ── Per-transaction inline test results ──
  const [txnTestResults, setTxnTestResults] = useState({});
  const [txnTesting, setTxnTesting] = useState({});

  // ── Transaction modal state ──
  const [txnModalOpen, setTxnModalOpen] = useState(false);
  const [editingTxnIndex, setEditingTxnIndex] = useState(null);

  // ── Drag state (steps + transactions share separate refs) ──
  const dragItem = useRef(null);
  const dragOverItem = useRef(null);
  const txnDragItem = useRef(null);
  const txnDragOverItem = useRef(null);

  // ── Derived: all variable names defined so far (steps + saved rules) ──
  const allDefinedVarNames = useMemo(() => {
    const names = new Set(savedRulesVarNames);
    for (const s of steps) {
      if (s.name) names.add(s.name);
      if (s.stepType === 'iteration') {
        (s.iterations || []).forEach(it => { if (it.resultVar) names.add(it.resultVar); });
      }
      if (s.stepType === 'schedule') {
        (s.outputVars || []).forEach(ov => { if (ov.name) names.add(ov.name); });
      }
    }
    return [...names];
  }, [steps, savedRulesVarNames]);

  // ── Convert initial data from old format to unified steps ──
  // This reads the legacy separate arrays and merges into one steps list.

  // ── Build DSL lines for code prior to current-rule (from saved rules) ──
  const buildPriorCodeLines = useCallback((currentStepNames) => {
    const lines = [];
    const emittedSaved = new Set();
    const deferredIterResults = [];
    const deferredDependsOnIter = [];

    // Pre-compute iteration-result variable names so we can detect dependencies
    const iterResultVarNames = new Set(
      savedRulesVars.filter(v => v._isIterResult).map(v => v.name)
    );

    // Pass 1: regular (non-iteration-result) vars that don't depend on iter results
    for (const v of savedRulesVars) {
      if (!v.name || currentStepNames.has(v.name) || emittedSaved.has(v.name)) continue;
      if (v._isIterResult) {
        // Always defer to Pass 2 so their dependencies (regular vars) are emitted first
        deferredIterResults.push(v);
        continue;
      }
      // Schedule outputVars (e.g. recognition_results) reference the schedule variable
      // from another rule's steps section — emitting them as standalone calc lines would
      // reference an undefined name. Skip them here; the full prior-rule code is injected
      // via savedRulesRaw in testStep/testStepFromModal.
      if (v._isScheduleOutput) continue;
      // If this regular var's formula references an iter-result variable, defer to Pass 3
      const refText = (v.formula || '') + ' ' + (v.value || '');
      const dependsOnIter = [...iterResultVarNames].some(n => new RegExp(`\\b${n}\\b`).test(refText));
      if (dependsOnIter) {
        deferredDependsOnIter.push(v);
        continue;
      }
      emittedSaved.add(v.name);
      const line = buildCalcLine(v);
      if (line) lines.push(line);
    }
    // Pass 2: iteration result vars (depend on regular vars from Pass 1)
    for (const v of deferredIterResults) {
      if (emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.iterData) {
        // Use buildIterationLines with already-emitted vars as available context
        const available = [...emittedSaved];
        const iterLines = buildIterationLines([v.iterData], available);
        iterLines.forEach(l => lines.push(l));
      } else {
        const line = buildCalcLine(v);
        if (line) lines.push(line);
      }
    }
    // Pass 3: regular vars that depended on iter results (now safe to emit)
    for (const v of deferredDependsOnIter) {
      if (emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      const line = buildCalcLine(v);
      if (line) lines.push(line);
    }
    return lines;
  }, [savedRulesVars, steps]);

  // ── Test a step (builds code for all saved rules + all steps up to this one) ──
  const buildScheduleStepLines = useCallback((s) => {
    const lines = [];
    const sc = s.scheduleConfig || {};
    if (sc.periodType === 'number') {
      const countExpr = sc.periodCountSource === 'field' && sc.periodCountField ? sc.periodCountField
        : sc.periodCountSource === 'formula' && sc.periodCountFormula ? sc.periodCountFormula
        : (sc.periodCount || 12);
      lines.push(`p = period(${countExpr})`);
    } else {
      const startExpr = sc.startDateSource === 'field' && sc.startDateField ? sc.startDateField
        : sc.startDateSource === 'formula' && sc.startDateFormula ? sc.startDateFormula
        : `"${sc.startDate || '2026-01-01'}"`;
      const endExpr = sc.endDateSource === 'field' && sc.endDateField ? sc.endDateField
        : sc.endDateSource === 'formula' && sc.endDateFormula ? sc.endDateFormula
        : `"${sc.endDate || '2026-12-31'}"`;
      let periodCall = `p = period(${startExpr}, ${endExpr}, "${sc.frequency || 'M'}"`;
      if (sc.convention) periodCall += `, "${sc.convention}"`;
      periodCall += ')';
      lines.push(periodCall);
    }
    const validCols = (sc.columns || []).filter(c => c.name && c.formula);
    lines.push(`${s.name} = schedule(p, {`);
    validCols.forEach((col, idx) => {
      const comma = idx < validCols.length - 1 ? ',' : '';
      lines.push(`    "${col.name}": "${col.formula}"${comma}`);
    });
    const ctxVars = sc.contextVars || [];
    if (ctxVars.length > 0) {
      const ctxPairs = ctxVars.map(v => `"${v}": ${v}`).join(', ');
      lines.push(`}, {${ctxPairs}})`);
    } else {
      lines.push('})');
    }
    for (const o of (s.outputVars || [])) {
      if (o.type === 'first') lines.push(`${o.name} = schedule_first(${s.name}, "${o.column}")`);
      else if (o.type === 'last') lines.push(`${o.name} = schedule_last(${s.name}, "${o.column}")`);
      else if (o.type === 'sum') lines.push(`${o.name} = schedule_sum(${s.name}, "${o.column}")`);
      else if (o.type === 'column') lines.push(`${o.name} = schedule_column(${s.name}, "${o.column}")`);
      else if (o.type === 'filter') lines.push(`${o.name} = schedule_filter(${s.name}, "${o.matchCol}", ${o.matchValue}, "${o.column}")`);
    }
    return lines;
  }, []);

  // Helper: strip the deps section from a rule's generatedCode (same logic as handlePlayAll / backend)
  const _stripDepsSection = useCallback((code) => {
    const out = []; let skip = false;
    for (const line of (code || '').split('\n')) {
      const t = line.trim();
      if (t === '## Dependencies from saved rules') { skip = true; out.push(line); continue; }
      if (skip && t.startsWith('## ') && !t.startsWith('## ═')) { skip = false; }
      if (!skip) out.push(line);
    }
    return out.join('\n');
  }, []);

  // Helper: strip the "## Create Transactions" section from a rule's generatedCode.
  // Prior rules' createTransaction calls must NOT execute when testing a step in
  // isolation — they would emit real transactions and (worse) trigger validation
  // errors like "Length of 'amount' must equal number of subInstrumentIds" which
  // mask the actual step-under-test result.
  const _stripCreateTxnSection = useCallback((code) => {
    const out = []; let skip = false;
    for (const line of (code || '').split('\n')) {
      const t = line.trim();
      if (t === '## Create Transactions') { skip = true; continue; }
      if (skip && t.startsWith('## ') && !t.startsWith('## ═')) { skip = false; }
      if (skip) continue;
      out.push(line);
    }
    return out.join('\n');
  }, []);

  // Build full prior-rules code using savedRulesRaw (strip-deps on all but first),
  // excluding the rule currently being edited so it doesn't re-define its own vars.
  // Only includes rules that should run BEFORE this one (priority strictly less than
  // the current rule's priority). Including later rules pulls in dependencies that
  // weren't yet computed and produces "variable not defined" errors when testing
  // a step in isolation.
  //
  // We strip the "## Dependencies from saved rules" section from EVERY prior rule:
  // those deps are just a preview of variables that come from OTHER rules and they
  // can reference variables (e.g., `Schedule`) that won't be defined yet at that
  // point in the combined script. The prior rules' own `## Steps` sections will
  // re-define their outputs, so nothing useful is lost by stripping.
  const _buildPriorRulesCode = useCallback((excludeId) => {
    const currentPriority = rulePriority === '' || rulePriority == null
      ? Infinity
      : Number(rulePriority);
    const priorRules = (savedRulesRaw || [])
      .filter(r => !excludeId || r.id !== excludeId)
      .filter(r => {
        const p = r.priority == null ? Infinity : Number(r.priority);
        return p < currentPriority;
      })
      .sort((a, b) => {
        const pa = a.priority == null ? Infinity : Number(a.priority);
        const pb = b.priority == null ? Infinity : Number(b.priority);
        return pa - pb;
      });
    return priorRules
      .map(r => _stripCreateTxnSection(_stripDepsSection(r.generatedCode || '')))
      .filter(Boolean)
      .join('\n\n')
      // Drop any print() lines from prior rules so their output never leaks into
      // print_outputs — the tested step's own print is the only thing that should
      // appear in the result panel.
      .split('\n')
      .filter(l => {
        const t = l.trim();
        return !(t.startsWith('print(') || t.startsWith('print ('));
      })
      .join('\n');
  }, [savedRulesRaw, _stripDepsSection, _stripCreateTxnSection, rulePriority]);

  const testStep = useCallback(async (step, stepIndex) => {
    const lines = [];
    const targetIndex = stepIndex !== undefined ? stepIndex : steps.length - 1;

    // Inject full prior-rules code (strip-deps). This correctly handles schedule steps
    // whose outputVars reference the schedule variable from the prior rule's context.
    const priorCode = _buildPriorRulesCode(ruleId || null);
    if (priorCode) lines.push(priorCode);

    const definedVars = [];
    for (let i = 0; i <= targetIndex; i++) {
      const s = steps[i];
      if (!s.name) continue;
      if (s.stepType === 'calc') {
        const line = buildCalcLine(s);
        if (line) { lines.push(line); definedVars.push(s.name); }
      } else if (s.stepType === 'condition') {
        const expr = buildConditionExpr(s.conditions || [], s.elseFormula);
        lines.push(`${s.name} = ${expr}`);
        definedVars.push(s.name);
      } else if (s.stepType === 'iteration') {
        const allAvailable = [...new Set([...definedVars, ...savedRulesVarNames])];
        const iterLines = buildIterationLines(s.iterations || [], allAvailable);
        lines.push(...iterLines);
        (s.iterations || []).forEach(it => { if (it.resultVar) definedVars.push(it.resultVar); });
      } else if (s.stepType === 'schedule') {
        lines.push(...buildScheduleStepLines(s));
        definedVars.push(s.name);
        (s.outputVars || []).forEach(ov => definedVars.push(ov.name));
      } else if (s.stepType === 'custom_code') {
        if (s.customCode) lines.push(s.customCode);
      }
    }

    const targetStep = stepIndex !== undefined ? steps[stepIndex] : step;
    let variableName = '';
    const _codeSoFar = lines.join('\n');
    if (targetStep) {
      if (targetStep.stepType === 'iteration') {
        const lastIter = (targetStep.iterations || [])[(targetStep.iterations || []).length - 1];
        if (lastIter?.resultVar) {
          variableName = lastIter.resultVar;
          lines.push(_testPrintLine(lastIter.resultVar, _codeSoFar));
        }
      } else if (targetStep.stepType === 'schedule') {
        // Per-step play tests the schedule itself only — outputVars (sum/filter/etc.)
        // are tested individually from inside the Schedule modal.
        variableName = targetStep.name;
        lines.push(_testPrintLine(targetStep.name, _codeSoFar));
      } else if (targetStep.stepType === 'custom_code') {
        // custom code runs as-is, no extra print needed
      } else if (targetStep.name) {
        variableName = targetStep.name;
        lines.push(_testPrintLine(targetStep.name, _codeSoFar));
      }
    }

    const dslCode = lines.join('\n');
    const postingDate = await fetchEarliestPostingDate();
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: dslCode, posting_date: postingDate }),
    });
    const data = await response.json();
    if (response.ok && data.success) {
      const prints = data.print_outputs || [];
      // Join ALL prints so the card can group __TEST_ROW__ rows by (instrument, subinstrument).
      const out = prints.map(String).join('\n');
      return { success: true, output: out, variableName };
    } else {
      return { success: false, error: data.error || (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail)) || 'Execution failed', variableName };
    }
  }, [steps, savedRulesVarNames, ruleId, _buildPriorRulesCode, buildScheduleStepLines]);

  // Test for modal (builds code up to end + this new step)
  const testStepFromModal = useCallback(async (localStep) => {
    const lines = [];

    // Inject full prior-rules code (strip-deps). Correctly handles schedule outputVars
    // that reference the schedule variable defined inside a prior rule's steps section.
    const priorCode = _buildPriorRulesCode(ruleId || null);
    if (priorCode) lines.push(priorCode);

    // Emit ALL existing steps (since this step depends on them)
    const definedVars = [];
    for (const s of steps) {
      if (!s.name) continue;
      // If editing, skip the original version of the step being edited
      if (editingStepIndex !== null && s === steps[editingStepIndex]) continue;
      if (s.stepType === 'calc') {
        const line = buildCalcLine(s);
        if (line) { lines.push(line); definedVars.push(s.name); }
      } else if (s.stepType === 'condition') {
        lines.push(`${s.name} = ${buildConditionExpr(s.conditions || [], s.elseFormula)}`);
        definedVars.push(s.name);
      } else if (s.stepType === 'iteration') {
        const allAvailable = [...new Set([...definedVars, ...savedRulesVarNames])];
        lines.push(...buildIterationLines(s.iterations || [], allAvailable));
        (s.iterations || []).forEach(it => { if (it.resultVar) definedVars.push(it.resultVar); });
      } else if (s.stepType === 'schedule') {
        lines.push(...buildScheduleStepLines(s));
        definedVars.push(s.name);
        (s.outputVars || []).forEach(ov => definedVars.push(ov.name));
      } else if (s.stepType === 'custom_code') {
        if (s.customCode) lines.push(s.customCode);
      }
    }

    // Now emit the step being tested
    let variableName = '';
    const _codeSoFar2 = lines.join('\n');
    if (localStep.stepType === 'calc') {
      const line = buildCalcLine(localStep);
      if (line) lines.push(line);
      if (localStep.name) {
        variableName = localStep.name;
        lines.push(_testPrintLine(localStep.name, _codeSoFar2));
      }
    } else if (localStep.stepType === 'condition') {
      const expr = buildConditionExpr(localStep.conditions || [], localStep.elseFormula);
      lines.push(`${localStep.name} = ${expr}`);
      if (localStep.name) {
        variableName = localStep.name;
        lines.push(_testPrintLine(localStep.name, _codeSoFar2));
      }
    } else if (localStep.stepType === 'iteration') {
      const allAvailable = [...new Set([...definedVars, ...savedRulesVarNames])];
      lines.push(...buildIterationLines(localStep.iterations || [], allAvailable));
      const lastIter = (localStep.iterations || [])[(localStep.iterations || []).length - 1];
      if (lastIter?.resultVar) {
        variableName = lastIter.resultVar;
        lines.push(_testPrintLine(lastIter.resultVar, _codeSoFar2));
      }
    } else if (localStep.stepType === 'schedule') {
      lines.push(...buildScheduleStepLines(localStep));
      // Only print the schedule itself (single tested variable). Output variables
      // (sum/filter/etc.) have their own play buttons inside the modal.
      variableName = localStep.name;
      lines.push(_testPrintLine(localStep.name, _codeSoFar2));
    } else if (localStep.stepType === 'custom_code') {
      if (localStep.customCode) lines.push(localStep.customCode);
    }

    const dslCode = lines.join('\n');
    const postingDate = await fetchEarliestPostingDate();
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: dslCode, posting_date: postingDate }),
    });
    const data = await response.json();
    if (response.ok && data.success) {
      const prints = data.print_outputs || [];
      const out = prints.map(String).join('\n');
      return { success: true, output: out, variableName };
    } else {
      return { success: false, error: data.error || data.detail || 'Failed', variableName };
    }
  }, [steps, savedRulesVarNames, editingStepIndex, ruleId, _buildPriorRulesCode, buildScheduleStepLines]);

  // ── Generated code ──
  const generatedCode = useMemo(() => {
    const lines = [];
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push(`## ${(ruleName || 'CUSTOM CALCULATION').toUpperCase()}`);
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push('');

    // Collect current step names
    const currentStepNames = new Set(steps.filter(s => s.name).map(s => s.name));
    for (const s of steps) {
      if (s.stepType === 'iteration') {
        (s.iterations || []).forEach(it => { if (it.resultVar) currentStepNames.add(it.resultVar); });
      }
      if (s.stepType === 'schedule') {
        // Must exclude outputVars too — they depend on the schedule variable (defined in steps),
        // so emitting them in the deps section would reference the schedule before it's assigned.
        (s.outputVars || []).forEach(ov => { if (ov.name) currentStepNames.add(ov.name); });
      }
    }

    // Prior-rules dependencies
    const emittedSaved = new Set();
    const depLines = [];

    const deferredIterDeps = [];
    const deferredDepsDependOnIter = [];

    // Pre-compute iteration-result variable names
    const iterResultVarNames = new Set(
      savedRulesVars.filter(v => v._isIterResult).map(v => v.name)
    );

    for (const v of savedRulesVars) {
      if (!v.name || currentStepNames.has(v.name) || emittedSaved.has(v.name)) continue;
      if (v._isIterResult) {
        // Always defer to pass 2 so regular var dependencies are emitted first
        deferredIterDeps.push(v);
        continue;
      }
      // Defer regular vars that reference iter-result vars to pass 3
      const refText = (v.formula || '') + ' ' + (v.value || '');
      const dependsOnIter = [...iterResultVarNames].some(n => new RegExp(`\\b${n}\\b`).test(refText));
      if (dependsOnIter) {
        deferredDepsDependOnIter.push(v);
        continue;
      }
      emittedSaved.add(v.name);
      const line = buildCalcLine(v);
      if (line) depLines.push(line);
    }
    for (const v of deferredIterDeps) {
      if (emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.iterData) {
        const available = [...emittedSaved];
        const iterLines = buildIterationLines([v.iterData], available);
        iterLines.forEach(l => depLines.push(l));
      } else {
        const line = buildCalcLine(v);
        if (line) depLines.push(line);
      }
    }
    // Pass 3: regular vars that depended on iter results
    for (const v of deferredDepsDependOnIter) {
      if (emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      const line = buildCalcLine(v);
      if (line) depLines.push(line);
    }
    if (depLines.length > 0) {
      lines.push('## Dependencies from saved rules');
      lines.push(...depLines);
      lines.push('');
    }

    // Add a Steps section header so strip-deps terminates correctly when this rule
    // is used as a non-first rule in combined code (calc steps have no own header).
    if (steps.length > 0) {
      lines.push('## Steps');
    }

    // Emit each step
    const definedVars = [];
    for (const s of steps) {
      if (!s.name && s.stepType === 'calc') continue;

      // Per-step inline comment
      if (s.inlineComment && s.commentText?.trim()) {
        s.commentText.trim().split('\n').forEach(l => lines.push(`## ${l}`));
      }

      if (s.stepType === 'calc') {
        const line = buildCalcLine(s);
        if (line) { lines.push(line); definedVars.push(s.name); }
        if (s.printResult && s.name) lines.push(`print("${s.name} =", ${s.name})`);
      } else if (s.stepType === 'condition') {
        lines.push('## Conditional Logic');
        const expr = buildConditionExpr(s.conditions || [], s.elseFormula);
        lines.push(`${s.name} = ${expr}`);
        definedVars.push(s.name);
        if (s.printResult && s.name) lines.push(`print("${s.name} =", ${s.name})`);
        lines.push('');
      } else if (s.stepType === 'iteration') {
        lines.push('## Iteration');
        const allCtxVars = [...new Set([...definedVars, ...depLines.map(l => l.split(' = ')[0]).filter(Boolean)])];
        const iterLines = buildIterationLines(s.iterations || [], allCtxVars);
        lines.push(...iterLines);
        (s.iterations || []).forEach(it => { if (it.resultVar) definedVars.push(it.resultVar); });
        if (s.printResult) {
          const lastIter = (s.iterations || [])[(s.iterations || []).length - 1];
          if (lastIter?.resultVar) lines.push(`print("${lastIter.resultVar} =", ${lastIter.resultVar})`);
        }
        lines.push('');
      } else if (s.stepType === 'schedule') {
        const sc = s.scheduleConfig || {};
        lines.push('## Schedule');
        // Period definition
        if (sc.periodType === 'number') {
          const countExpr = sc.periodCountSource === 'field' && sc.periodCountField ? sc.periodCountField
            : sc.periodCountSource === 'formula' && sc.periodCountFormula ? sc.periodCountFormula
            : (sc.periodCount || 12);
          lines.push(`p = period(${countExpr})`);
        } else {
          const startExpr = sc.startDateSource === 'field' && sc.startDateField ? sc.startDateField
            : sc.startDateSource === 'formula' && sc.startDateFormula ? sc.startDateFormula
            : `"${sc.startDate || '2026-01-01'}"`;
          const endExpr = sc.endDateSource === 'field' && sc.endDateField ? sc.endDateField
            : sc.endDateSource === 'formula' && sc.endDateFormula ? sc.endDateFormula
            : `"${sc.endDate || '2026-12-31'}"`;
          let periodCall = `p = period(${startExpr}, ${endExpr}, "${sc.frequency || 'M'}"`;
          if (sc.convention) periodCall += `, "${sc.convention}"`;
          periodCall += ')';
          lines.push(periodCall);
        }
        // Schedule call
        const validCols = (sc.columns || []).filter(c => c.name && c.formula);
        lines.push(`${s.name} = schedule(p, {`);
        validCols.forEach((col, idx) => {
          const comma = idx < validCols.length - 1 ? ',' : '';
          lines.push(`    "${col.name}": "${col.formula}"${comma}`);
        });
        const ctxVars = sc.contextVars || [];
        if (ctxVars.length > 0) {
          const ctxPairs = ctxVars.map(v => `"${v}": ${v}`).join(', ');
          lines.push(`}, {${ctxPairs}})`);
        } else {
          lines.push('})');  
        }
        lines.push(`print(${s.name})`);
        definedVars.push(s.name);
        // Output variables
        const ov = s.outputVars || [];
        for (const o of ov) {
          if (o.type === 'first') {
            lines.push(`${o.name} = schedule_first(${s.name}, "${o.column}")`);
          } else if (o.type === 'last') {
            lines.push(`${o.name} = schedule_last(${s.name}, "${o.column}")`);
          } else if (o.type === 'sum') {
            lines.push(`${o.name} = schedule_sum(${s.name}, "${o.column}")`);
          } else if (o.type === 'column') {
            lines.push(`${o.name} = schedule_column(${s.name}, "${o.column}")`);
          } else if (o.type === 'filter') {
            lines.push(`${o.name} = schedule_filter(${s.name}, "${o.matchCol}", ${o.matchValue}, "${o.column}")`);
          }
          definedVars.push(o.name);
        }
        lines.push('');
      } else if (s.stepType === 'custom_code') {
        lines.push('## Custom Code');
        if (s.customCode) lines.push(s.customCode);
        lines.push('');
      }
    }

    // Transactions — emit any defined transactions (toggle removed; presence implies intent)
    if ((outputs.transactions || []).some(t => t && t.type)) {
      lines.push('');
      lines.push('## Create Transactions');
      for (const txn of outputs.transactions) {
        if (!txn.type) continue;
        const amt = txn.amount || definedVars[definedVars.length - 1] || '0';
        const pd = txn.postingDate || 'postingdate';
        const ed = txn.effectiveDate || pd;
        const sid = txn.subInstrumentId || '';
        if (sid) lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt}, ${sid})`);
        else lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt})`);
      }
    }

    return lines.join('\n');
  }, [ruleName, steps, outputs, savedRulesVars]);

  // Determine the ruleType for backward-compatible saving
  const effectiveRuleType = useMemo(() => {
    const types = new Set(steps.map(s => s.stepType));
    if (types.has('schedule')) return 'schedule';
    if (types.has('custom_code')) return 'custom_code';
    if (types.has('iteration')) return 'iteration';
    if (types.has('condition')) return 'conditional';
    return 'simple_calc';
  }, [steps]);

  const resetForm = useCallback(() => {
    setRuleName('');
    setRulePriority('');
    setRuleId(null);
    setSteps([]);
    setOutputs({ printResult: true, createTransaction: false, transactions: [] });
    setInlineComment(false);
    setCommentText('');
    setShowCode(false);
    setSaveResult(null);
  }, []);

  // ── Save ──
  const handleSave = useCallback(async () => {
    if (!ruleName.trim()) { setValidationMsg('Rule Name is required.'); return; }
    if (rulePriority === '' || rulePriority === null || rulePriority === undefined) { setValidationMsg('Priority is required.'); return; }
    const emptySteps = steps.filter(s => !s.name);
    if (emptySteps.length > 0) { setValidationMsg('All steps must have a variable name.'); return; }

    setSaving(true);
    setSaveResult(null);

    // Convert unified steps back to the legacy format for backward compatibility
    const variables = steps.filter(s => s.stepType === 'calc').map(s => ({
      name: s.name, source: s.source || 'formula', formula: s.formula || '', value: s.value || '',
      eventField: s.eventField || '', collectType: s.collectType || 'collect',
    }));
    const condStep = steps.find(s => s.stepType === 'condition');
    const conditions = condStep?.conditions || [];
    const elseFormula = condStep?.elseFormula || '';
    const conditionResultVar = condStep?.name || 'result';
    const iterStep = steps.find(s => s.stepType === 'iteration');
    const iterations = iterStep?.iterations || [];

    try {
      const payload = {
        id: ruleId,
        name: ruleName.trim(),
        priority: Number(rulePriority),
        ruleType: effectiveRuleType,
        variables,
        conditions,
        elseFormula,
        conditionResultVar,
        iterations,
        iterConfig: iterations[0] || {},
        outputs,
        inlineComment,
        commentText,
        customCode: '',
        generatedCode,
        // Also save unified steps for future loading
        steps,
      };
      const response = await fetch(`${API}/saved-rules`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setRuleId(data.id);
        setSaveResult({ success: true, output: data.message || 'Rule saved successfully.' });
        if (onSave) onSave();
        setTimeout(() => resetForm(), 1500);
      } else {
        const errMsg = data.detail || data.error || 'Save failed';
        setSaveResult({ success: false, error: typeof errMsg === 'string' ? errMsg : JSON.stringify(errMsg) });
      }
    } catch (err) {
      setSaveResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setSaving(false);
    }
  }, [ruleName, rulePriority, ruleId, effectiveRuleType, steps, outputs, inlineComment, commentText, generatedCode, onSave, resetForm]);

  // ── Step CRUD ──
  const openAddStep = (type) => {
    setEditingStepIndex(null);
    setModalStepType(type);
    const defaults = type === 'calc'
      ? { name: '', stepType: 'calc', source: 'formula', formula: '', value: '', eventField: '', collectType: 'collect' }
      : type === 'condition'
      ? { name: '', stepType: 'condition', conditions: [{ condition: '', thenFormula: '' }], elseFormula: '' }
      : type === 'iteration'
      ? { name: '', stepType: 'iteration', iterations: [{ type: 'apply_each', sourceArray: '', varName: 'each', expression: '', resultVar: '', secondArray: '', secondVar: 'second' }] }
      : type === 'schedule'
      ? { name: '', stepType: 'schedule', scheduleConfig: {}, outputVars: [] }
      : type === 'custom_code'
      ? { name: '', stepType: 'custom_code', customCode: '' }
      : { name: '', stepType: 'calc', source: 'formula', formula: '', value: '', eventField: '', collectType: 'collect' };
    setModalStep(defaults);
    setModalOpen(true);
    setAddMenuAnchor(null);
  };

  const openEditStep = (index) => {
    setEditingStepIndex(index);
    setModalStepType(steps[index].stepType);
    setModalStep({ ...steps[index] });
    setModalOpen(true);
  };

  const saveStepFromModal = (step) => {
    if (editingStepIndex !== null) {
      setSteps(prev => prev.map((s, i) => i === editingStepIndex ? step : s));
    } else {
      setSteps(prev => [...prev, step]);
    }
    setStepTestResults({});
  };

  const removeStep = (index) => {
    setSteps(prev => prev.filter((_, i) => i !== index));
    setStepTestResults({});
  };

  const duplicateStep = (index) => {
    const original = steps[index];
    const baseName = original.name || 'step';
    const existingNames = new Set(steps.map(s => s.name));
    let newName = `${baseName}_copy`;
    let counter = 2;
    while (existingNames.has(newName)) {
      newName = `${baseName}_copy${counter++}`;
    }
    const copy = { ...JSON.parse(JSON.stringify(original)), name: newName };
    setSteps(prev => {
      const arr = [...prev];
      arr.splice(index + 1, 0, copy);
      return arr;
    });
    setStepTestResults({});
  };

  // ── Inline test for transactions ──
  const handleTransactionTest = useCallback(async (txnIdx) => {
    const txn = outputs.transactions[txnIdx];
    if (!txn.type) return;
    setTxnTesting(prev => ({ ...prev, [txnIdx]: true }));
    setTxnTestResults(prev => ({ ...prev, [txnIdx]: null }));
    try {
      const lines = [];
      const priorCode = _buildPriorRulesCode(ruleId || null);
      if (priorCode) lines.push(priorCode);
      const definedVars = [];
      for (const s of steps) {
        if (!s.name) continue;
        if (s.stepType === 'calc') {
          const line = buildCalcLine(s);
          if (line) { lines.push(line); definedVars.push(s.name); }
        } else if (s.stepType === 'condition') {
          lines.push(`${s.name} = ${buildConditionExpr(s.conditions || [], s.elseFormula)}`);
          definedVars.push(s.name);
        } else if (s.stepType === 'iteration') {
          const allAvailable = [...new Set([...definedVars, ...savedRulesVarNames])];
          lines.push(...buildIterationLines(s.iterations || [], allAvailable));
          (s.iterations || []).forEach(it => { if (it.resultVar) definedVars.push(it.resultVar); });
        } else if (s.stepType === 'schedule') {
          lines.push(...buildScheduleStepLines(s));
          definedVars.push(s.name);
          (s.outputVars || []).forEach(ov => definedVars.push(ov.name));
        } else if (s.stepType === 'custom_code') {
          if (s.customCode) lines.push(s.customCode);
        }
      }
      const amt = txn.amount || definedVars[definedVars.length - 1] || '0';
      const pd = txn.postingDate || 'postingdate';
      const ed = txn.effectiveDate || pd;
      const sid = txn.subInstrumentId || '';
      if (sid) lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt}, ${sid})`);
      else lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt})`);
      const dslCode = lines.join('\n');
      const postingDate = await fetchEarliestPostingDate();
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: dslCode, posting_date: postingDate }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const txns = data.transactions || [];
        // Only the LAST transaction is the one we just emitted (prior rules'
        // createTransaction sections are stripped, but defensive slicing here too).
        const justThis = txns.length > 0 ? [txns[txns.length - 1]] : [];
        // Render a compact one-row table by serialising as a Python-style list of dicts;
        // TestResultCard will parse and table-ify it.
        const out = justThis.length > 0
          ? `transaction = ${JSON.stringify(justThis)}`
          : 'transaction = []';
        const variableName = txn.type ? `${txn.type} transaction` : 'transaction';
        setTxnTestResults(prev => ({ ...prev, [txnIdx]: { success: true, output: out, variableName } }));
      } else {
        setTxnTestResults(prev => ({ ...prev, [txnIdx]: { success: false, error: data.error || 'Execution failed', variableName: txn.type ? `${txn.type} transaction` : 'transaction' } }));
      }
    } catch (e) {
      setTxnTestResults(prev => ({ ...prev, [txnIdx]: { success: false, error: e.message, variableName: txn.type ? `${txn.type} transaction` : 'transaction' } }));
    } finally {
      setTxnTesting(prev => ({ ...prev, [txnIdx]: false }));
    }
  }, [outputs.transactions, steps, ruleId, _buildPriorRulesCode, buildScheduleStepLines, savedRulesVarNames]);

  // ── Inline test (play button on the step row) ──
  const handleInlineTest = useCallback(async (index) => {
    setStepTesting(prev => ({ ...prev, [index]: true }));
    setStepTestResults(prev => ({ ...prev, [index]: null }));
    try {
      const result = await testStep(steps[index], index);
      setStepTestResults(prev => ({ ...prev, [index]: result }));
    } catch (e) {
      setStepTestResults(prev => ({ ...prev, [index]: { success: false, error: e.message } }));
    } finally {
      setStepTesting(prev => ({ ...prev, [index]: false }));
    }
  }, [steps, testStep]);

  // ── Drag and drop ──
  const handleDragStart = (idx) => { dragItem.current = idx; };
  const handleDragOver = (e, idx) => { e.preventDefault(); dragOverItem.current = idx; };
  const handleDrop = (e) => {
    e.preventDefault();
    const from = dragItem.current;
    const to = dragOverItem.current;
    if (from === null || to === null || from === to) return;
    setSteps(prev => {
      const arr = [...prev];
      const [moved] = arr.splice(from, 1);
      arr.splice(to, 0, moved);
      return arr;
    });
    dragItem.current = null;
    dragOverItem.current = null;
    setStepTestResults({});
  };

  // ── Transaction CRUD (modal-driven, step-style) ──
  const openAddTransaction = useCallback(() => {
    setEditingTxnIndex(null);
    setTxnModalOpen(true);
  }, []);
  const openEditTransaction = useCallback((i) => {
    setEditingTxnIndex(i);
    setTxnModalOpen(true);
  }, []);
  const saveTransactionFromModal = useCallback((txn) => {
    setOutputs(prev => {
      const list = [...(prev.transactions || [])];
      if (editingTxnIndex !== null && editingTxnIndex >= 0 && editingTxnIndex < list.length) {
        list[editingTxnIndex] = txn;
      } else {
        list.push(txn);
      }
      return { ...prev, transactions: list, createTransaction: list.length > 0 };
    });
    setTxnTestResults({});
  }, [editingTxnIndex]);
  const removeTransaction = useCallback((i) => {
    setOutputs(prev => {
      const list = (prev.transactions || []).filter((_, j) => j !== i);
      return { ...prev, transactions: list, createTransaction: list.length > 0 };
    });
    setTxnTestResults({});
  }, []);
  const duplicateTransaction = useCallback((i) => {
    setOutputs(prev => {
      const list = [...(prev.transactions || [])];
      if (i < 0 || i >= list.length) return prev;
      const baseType = list[i].type || 'Transaction';
      const existingTypes = new Set(list.map(t => t.type));
      let newType = `${baseType} (copy)`;
      let counter = 2;
      while (existingTypes.has(newType)) newType = `${baseType} (copy ${counter++})`;
      const copy = { ...JSON.parse(JSON.stringify(list[i])), type: newType };
      list.splice(i + 1, 0, copy);
      return { ...prev, transactions: list, createTransaction: list.length > 0 };
    });
    setTxnTestResults({});
  }, []);

  // ── Transaction drag & drop ──
  const handleTxnDragStart = (idx) => { txnDragItem.current = idx; };
  const handleTxnDragOver = (e, idx) => { e.preventDefault(); txnDragOverItem.current = idx; };
  const handleTxnDrop = (e) => {
    e.preventDefault();
    const from = txnDragItem.current;
    const to = txnDragOverItem.current;
    if (from === null || to === null || from === to) return;
    setOutputs(prev => {
      const list = [...(prev.transactions || [])];
      const [moved] = list.splice(from, 1);
      list.splice(to, 0, moved);
      return { ...prev, transactions: list };
    });
    txnDragItem.current = null;
    txnDragOverItem.current = null;
    setTxnTestResults({});
  };

  // ── Test a transaction passed by the modal (independent of saved index) ──
  const testTransactionDraft = useCallback(async (txn) => {
    if (!txn?.type) return { success: false, error: 'Transaction type is required' };
    const lines = [];
    const priorCode = _buildPriorRulesCode(ruleId || null);
    if (priorCode) lines.push(priorCode);
    const definedVars = [];
    for (const s of steps) {
      if (!s.name) continue;
      if (s.stepType === 'calc') {
        const line = buildCalcLine(s);
        if (line) { lines.push(line); definedVars.push(s.name); }
      } else if (s.stepType === 'condition') {
        lines.push(`${s.name} = ${buildConditionExpr(s.conditions || [], s.elseFormula)}`);
        definedVars.push(s.name);
      } else if (s.stepType === 'iteration') {
        const allAvailable = [...new Set([...definedVars, ...savedRulesVarNames])];
        lines.push(...buildIterationLines(s.iterations || [], allAvailable));
        (s.iterations || []).forEach(it => { if (it.resultVar) definedVars.push(it.resultVar); });
      } else if (s.stepType === 'schedule') {
        lines.push(...buildScheduleStepLines(s));
        definedVars.push(s.name);
        (s.outputVars || []).forEach(ov => definedVars.push(ov.name));
      } else if (s.stepType === 'custom_code') {
        if (s.customCode) lines.push(s.customCode);
      }
    }
    const amt = txn.amount || definedVars[definedVars.length - 1] || '0';
    const pd = txn.postingDate || 'postingdate';
    const ed = txn.effectiveDate || pd;
    const sid = txn.subInstrumentId || '';
    if (sid) lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt}, ${sid})`);
    else lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt})`);
    const dslCode = lines.join('\n');
    const postingDate = await fetchEarliestPostingDate();
    const response = await fetch(`${API}/dsl/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dsl_code: dslCode, posting_date: postingDate }),
    });
    const data = await response.json();
    const variableName = `${txn.type} transaction`;
    if (response.ok && data.success) {
      const txns = data.transactions || [];
      const justThis = txns.length > 0 ? [txns[txns.length - 1]] : [];
      const out = justThis.length > 0 ? `transaction = ${JSON.stringify(justThis)}` : 'transaction = []';
      return { success: true, output: out, variableName };
    }
    return { success: false, error: data.error || 'Execution failed', variableName };
  }, [steps, ruleId, _buildPriorRulesCode, buildScheduleStepLines, savedRulesVarNames]);

  // ── Get step display name ──
  const getStepLabel = (s) => {
    if (s.stepType === 'iteration') {
      const last = (s.iterations || [])[(s.iterations || []).length - 1];
      return last?.resultVar || s.name || '(unnamed)';
    }
    return s.name || '(unnamed)';
  };

  const getStepDescription = (s) => {
    if (s.stepType === 'calc') {
      const labels = { formula: 'Formula', value: 'Fixed Value', event_field: 'Event Field', collect: 'Collect' };
      return labels[s.source] || 'Calculation';
    }
    if (s.stepType === 'condition') {
      const count = (s.conditions || []).filter(c => c.condition).length;
      return `${count} condition${count !== 1 ? 's' : ''}`;
    }
    if (s.stepType === 'iteration') {
      const count = (s.iterations || []).length;
      return `${count} iteration${count !== 1 ? 's' : ''}`;
    }
    if (s.stepType === 'schedule') {
      const colCount = (s.scheduleConfig?.columns || []).filter(c => c.name).length;
      return `${colCount} column${colCount !== 1 ? 's' : ''}`;
    }
    if (s.stepType === 'custom_code') {
      return 'Custom DSL';
    }
    return '';
  };

  // ═════════════════════════════════════════════════════════════════════
  // RENDER
  // ═════════════════════════════════════════════════════════════════════
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%' }}>
      {/* Header */}
      <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', bgcolor: 'white', flexShrink: 0 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
          <Calculator size={20} color="#5B5FED" />
          <Typography variant="h5">Rule Builder</Typography>
          <Box sx={{ flex: 1 }} />
          <Tooltip title="New Rule">
            <IconButton size="small" onClick={resetForm} sx={{ color: '#5B5FED' }}>
              <Plus size={18} />
            </IconButton>
          </Tooltip>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Build calculation logic using forms — no coding required
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {/* Rule Name & Priority */}
        <Box sx={{ display: 'flex', gap: 1.5, mb: 2 }}>
          <TextField size="small" label="Rule Name *" value={ruleName}
            onChange={(e) => setRuleName(e.target.value)}
            placeholder="e.g., Monthly Interest Accrual" sx={{ flex: 1 }} />
          <TextField size="small" label="Priority *" value={rulePriority}
            onChange={(e) => { const v = e.target.value; if (v === '' || /^\d+$/.test(v)) setRulePriority(v === '' ? '' : Number(v)); }}
            placeholder="e.g., 1" type="number" inputProps={{ min: 0, step: 1 }} sx={{ width: 140 }} />
        </Box>

        {/* ── Steps List ── */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <Typography variant="body2" fontWeight={600}>Steps</Typography>
          <Box>
            <Button size="small" startIcon={<Plus size={14} />} endIcon={<ChevronDown size={12} />}
              onClick={(e) => setAddMenuAnchor(e.currentTarget)}>
              Add Step
            </Button>
            <Menu anchorEl={addMenuAnchor} open={!!addMenuAnchor} onClose={() => setAddMenuAnchor(null)}>
              <MenuItem onClick={() => openAddStep('calc')}>
                <Calculator size={16} style={{ marginRight: 8 }} color="#5B5FED" /> Calculation
              </MenuItem>
              <MenuItem onClick={() => openAddStep('condition')}>
                <GitBranch size={16} style={{ marginRight: 8 }} color="#FF9800" /> Condition
              </MenuItem>
              <MenuItem onClick={() => openAddStep('iteration')}>
                <Repeat size={16} style={{ marginRight: 8 }} color="#00BCD4" /> Iteration
              </MenuItem>
              <MenuItem onClick={() => openAddStep('schedule')}>
                <Calendar size={16} style={{ marginRight: 8 }} color="#9C27B0" /> Schedule
              </MenuItem>
              <MenuItem onClick={() => openAddStep('custom_code')}>
                <Code size={16} style={{ marginRight: 8 }} color="#607D8B" /> Custom Code
              </MenuItem>
            </Menu>
          </Box>
        </Box>

        {steps.length === 0 && (
          <Box sx={{ textAlign: 'center', py: 4, color: 'text.secondary', border: '2px dashed #E9ECEF', borderRadius: 2, mb: 2 }}>
            <Calculator size={32} style={{ margin: '0 auto 8px', opacity: 0.3 }} />
            <Typography variant="body2">No steps yet. Click <strong>+ Add Step</strong> to begin.</Typography>
          </Box>
        )}

        {steps.map((step, idx) => {
          const meta = STEP_TYPE_META[step.stepType] || STEP_TYPE_META.calc;
          const Icon = meta.icon;
          const tr = stepTestResults[idx];
          return (
            <Card key={idx}
              draggable
              onDragStart={() => handleDragStart(idx)}
              onDragOver={(e) => handleDragOver(e, idx)}
              onDrop={handleDrop}
              sx={{
                mb: 1, borderLeft: `3px solid ${meta.color}`,
                transition: 'all 0.15s',
                '&:hover': { boxShadow: `0 2px 8px ${meta.color}1F` },
              }}>
              <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box sx={{ cursor: 'grab', display: 'flex', alignItems: 'center', flexShrink: 0 }}
                    onMouseDown={(e) => e.stopPropagation()}>
                    <GripVertical size={16} color="#ADB5BD" />
                  </Box>
                  <Chip size="small" label={idx + 1}
                    sx={{ fontSize: '0.6875rem', height: 20, minWidth: 24, bgcolor: '#F0F0F0', fontWeight: 600 }} />
                  <Icon size={16} color={meta.color} />
                  <Typography variant="body2" fontWeight={600} sx={{ flex: 1 }} noWrap>
                    {getStepLabel(step)}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ mr: 1 }}>
                    {getStepDescription(step)}
                  </Typography>
                  <Chip size="small" label={meta.label}
                    sx={{ fontSize: '0.625rem', height: 18, bgcolor: `${meta.color}18`, color: meta.color, fontWeight: 600 }} />
                  {step.stepType !== 'custom_code' && (
                    <Tooltip title="Test up to this step">
                      <IconButton size="small" onClick={() => handleInlineTest(idx)}
                        disabled={!!stepTesting[idx]} sx={{ color: '#4CAF50' }}>
                        {stepTesting[idx] ? <CircularProgress size={14} /> : <Play size={14} />}
                      </IconButton>
                    </Tooltip>
                  )}
                  <Tooltip title="Duplicate step">
                    <IconButton size="small" onClick={() => duplicateStep(idx)} sx={{ color: '#607D8B' }}>
                      <Copy size={14} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Edit step">
                    <IconButton size="small" onClick={() => openEditStep(idx)} sx={{ color: meta.color }}>
                      <Edit3 size={14} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete step">
                    <IconButton size="small" onClick={() => removeStep(idx)} sx={{ color: '#F44336' }}>
                      <Trash2 size={14} />
                    </IconButton>
                  </Tooltip>
                </Box>
                {tr && (
                  <TestResultCard
                    success={tr.success}
                    output={tr.output}
                    error={tr.error}
                    variableName={tr.variableName || step?.name}
                    onClose={() => setStepTestResults(prev => ({ ...prev, [idx]: null }))}
                  />
                )}
              </CardContent>
            </Card>
          );
        })}

        <Divider sx={{ my: 2 }} />

        {/* ── Transactions ── (parallel to Steps) */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Receipt size={16} color={TXN_COLOR} />
            <Typography variant="body2" fontWeight={600}>Transactions</Typography>
            {(outputs.transactions || []).length > 0 && (
              <Chip size="small" label={(outputs.transactions || []).length}
                sx={{ height: 18, fontSize: '0.6875rem', bgcolor: `${TXN_COLOR}1A`, color: TXN_COLOR, fontWeight: 700 }} />
            )}
          </Box>
          <Button size="small" startIcon={<Plus size={14} />} onClick={openAddTransaction}
            sx={{ color: TXN_COLOR }}>
            Add Transaction
          </Button>
        </Box>

        {(outputs.transactions || []).length === 0 && (
          <Box sx={{ textAlign: 'center', py: 3, color: '#ADB5BD', border: '2px dashed #DEE2E6', borderRadius: 1, mb: 1.5 }}>
            <Receipt size={32} style={{ margin: '0 auto 8px', opacity: 0.3 }} />
            <Typography variant="body2">No transactions yet. Click <strong>+ Add Transaction</strong> to define one.</Typography>
          </Box>
        )}

        {(outputs.transactions || []).map((txn, idx) => {
          const tr = txnTestResults[idx];
          const labelText = txn.type || '(unnamed)';
          const desc = txn.amount ? `amount: ${txn.amount}` : 'no amount';
          return (
            <Card key={idx}
              draggable
              onDragStart={() => handleTxnDragStart(idx)}
              onDragOver={(e) => handleTxnDragOver(e, idx)}
              onDrop={handleTxnDrop}
              sx={{
                mb: 1, borderLeft: `3px solid ${TXN_COLOR}`,
                transition: 'all 0.15s',
                '&:hover': { boxShadow: `0 2px 8px ${TXN_COLOR}1F` },
              }}>
              <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box sx={{ cursor: 'grab', display: 'flex', alignItems: 'center', flexShrink: 0 }}
                    onMouseDown={(e) => e.stopPropagation()}>
                    <GripVertical size={16} color="#ADB5BD" />
                  </Box>
                  <Chip size="small" label={idx + 1}
                    sx={{ fontSize: '0.6875rem', height: 20, minWidth: 24, bgcolor: '#F0F0F0', fontWeight: 600 }} />
                  <Receipt size={16} color={TXN_COLOR} />
                  <Typography variant="body2" fontWeight={600} sx={{ flex: 1 }} noWrap>
                    {labelText}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ mr: 1 }}>
                    {desc}
                  </Typography>
                  <Chip size="small" label="Transaction"
                    sx={{ fontSize: '0.625rem', height: 18, bgcolor: `${TXN_COLOR}18`, color: TXN_COLOR, fontWeight: 600 }} />
                  <Tooltip title="Test this transaction">
                    <span>
                      <IconButton size="small" onClick={() => handleTransactionTest(idx)}
                        disabled={!!txnTesting[idx] || !txn.type} sx={{ color: '#4CAF50' }}>
                        {txnTesting[idx] ? <CircularProgress size={14} /> : <Play size={14} />}
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Duplicate transaction">
                    <IconButton size="small" onClick={() => duplicateTransaction(idx)} sx={{ color: '#607D8B' }}>
                      <Copy size={14} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Edit transaction">
                    <IconButton size="small" onClick={() => openEditTransaction(idx)} sx={{ color: TXN_COLOR }}>
                      <Edit3 size={14} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete transaction">
                    <IconButton size="small" onClick={() => removeTransaction(idx)} sx={{ color: '#F44336' }}>
                      <Trash2 size={14} />
                    </IconButton>
                  </Tooltip>
                </Box>
                {tr && (
                  <TestResultCard
                    success={tr.success}
                    output={tr.output}
                    error={tr.error}
                    variableName={tr.variableName || (txn.type ? `${txn.type} transaction` : 'transaction')}
                    onClose={() => setTxnTestResults(prev => ({ ...prev, [idx]: null }))}
                  />
                )}
              </CardContent>
            </Card>
          );
        })}

        {/* Save Result */}
        {saveResult && (
          <Alert severity={saveResult.success ? 'success' : 'error'} sx={{ mt: 2 }}
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

      {/* Step Modal — conditionally render based on step type */}
      {modalStepType === 'schedule' ? (
        <ScheduleStepModal
          open={modalOpen}
          step={modalStep}
          onClose={() => setModalOpen(false)}
          onSaveStep={saveStepFromModal}
          events={events}
          dslFunctions={dslFunctions}
          definedVarNames={allDefinedVarNames}
          freshPriorCode={(() => {
            // Build prior-rules code by concatenating saved rules' generatedCode in priority
            // order. Apply the SAME rules as testStep / _buildPriorRulesCode:
            //   1. Only include rules with priority < current rule's priority
            //   2. Exclude the rule currently being edited (so it doesn't redefine its own vars)
            //   3. Strip "## Dependencies from saved rules" section from EVERY rule
            //      (those refer to vars from OTHER rules and may not be defined yet —
            //       e.g., `Schedule` referenced by Stage1 deps when editing Stage 2's schedule)
            //   4. Strip "## Create Transactions" section from EVERY rule (avoid emitting
            //      transactions / triggering length-validation errors during step tests)
            //   5. Drop print() / createTransaction() lines as a final safety net
            const stripSection = (code, header) => {
              const out = []; let skip = false;
              for (const line of code.split('\n')) {
                const t = line.trim();
                if (t === header) { skip = true; continue; }
                if (skip && t.startsWith('## ') && !t.startsWith('## \u2550')) { skip = false; }
                if (skip) continue;
                out.push(line);
              }
              return out.join('\n');
            };
            const currentPriority = rulePriority === '' || rulePriority == null
              ? Infinity : Number(rulePriority);
            return savedRulesRaw
              .filter(r => !ruleId || r.id !== ruleId)
              .filter(r => {
                const p = r.priority == null ? Infinity : Number(r.priority);
                return p < currentPriority;
              })
              .sort((a, b) => {
                const pa = a.priority == null ? Infinity : Number(a.priority);
                const pb = b.priority == null ? Infinity : Number(b.priority);
                return pa - pb;
              })
              .map(r => stripSection(stripSection(r.generatedCode || '', '## Dependencies from saved rules'), '## Create Transactions'))
              .filter(Boolean).join('\n\n')
              .split('\n')
              .filter(l => { const t = l.trim(); return t && !t.startsWith('print(') && !t.startsWith('print (') && !t.startsWith('createTransaction('); })
              .join('\n');
          })()}
          currentRulePreStepCode={(() => {
            // Emit code for all steps before the one being edited so that variables
            // like start_dates / end_dates are available when testing the schedule.
            const preLines = [];
            const definedSoFar = [];
            for (const s of steps) {
              if (editingStepIndex !== null && s === steps[editingStepIndex]) break;
              if (!s.name && s.stepType !== 'custom_code') continue;
              if (s.stepType === 'calc') {
                const line = buildCalcLine(s);
                if (line) { preLines.push(line); definedSoFar.push(s.name); }
              } else if (s.stepType === 'condition') {
                const expr = buildConditionExpr(s.conditions || [], s.elseFormula);
                preLines.push(`${s.name} = ${expr}`);
                definedSoFar.push(s.name);
              } else if (s.stepType === 'iteration') {
                const allAvail = [...new Set([...definedSoFar, ...savedRulesVarNames])];
                preLines.push(...buildIterationLines(s.iterations || [], allAvail));
                (s.iterations || []).forEach(it => { if (it.resultVar) definedSoFar.push(it.resultVar); });
              } else if (s.stepType === 'schedule') {
                preLines.push(...buildScheduleStepLines(s));
                definedSoFar.push(s.name);
                (s.outputVars || []).forEach(ov => definedSoFar.push(ov.name));
              } else if (s.stepType === 'custom_code') {
                if (s.customCode) preLines.push(s.customCode);
              }
            }
            return preLines.join('\n');
          })()}
        />
      ) : modalStepType === 'custom_code' ? (
        <CustomCodeStepModal
          open={modalOpen}
          step={modalStep}
          onClose={() => setModalOpen(false)}
          onSaveStep={saveStepFromModal}
          events={events}
          dslFunctions={dslFunctions}
        />
      ) : (
        <StepModal
          open={modalOpen}
          step={modalStep}
          stepType={modalStepType}
          onClose={() => setModalOpen(false)}
          onSaveStep={saveStepFromModal}
          events={events}
          definedVarNames={allDefinedVarNames}
          onTest={testStepFromModal}
          generatedCode={generatedCode}
        />
      )}

      {/* Transaction Modal */}
      <TransactionModal
        open={txnModalOpen}
        txn={editingTxnIndex !== null ? (outputs.transactions || [])[editingTxnIndex] : null}
        onClose={() => { setTxnModalOpen(false); setEditingTxnIndex(null); }}
        onSaveTxn={saveTransactionFromModal}
        onTest={testTransactionDraft}
        amountOptions={(() => {
          const varNames = [];
          steps.forEach(s => {
            if (s.stepType === 'schedule') {
              (s.outputVars || []).forEach(ov => { if (ov.name) varNames.push(ov.name); });
            } else if (s.stepType === 'iteration') {
              (s.iterations || []).forEach(it => { if (it.resultVar) varNames.push(it.resultVar); });
              if (s.name) varNames.push(s.name);
            } else if (s.name) {
              varNames.push(s.name);
            }
          });
          return [...new Set([...varNames, ...savedRulesVarNames])];
        })()}
        eventFieldOptions={events?.flatMap(ev => [
          ...['postingdate', 'effectivedate'].map(sf => `${ev.event_name}.${sf}`),
          ...ev.fields.map(f => `${ev.event_name}.${f.name}`),
        ]) || []}
        subIdOptions={(() => {
          const isSubIdStep = (s) => {
            if (!s.name) return false;
            if (s.source === 'collect' && s.collectType === 'collect_subinstrumentids') return true;
            if (s.source === 'event_field' && s.eventField?.toLowerCase().endsWith('.subinstrumentid')) return true;
            if (s.source === 'collect' && s.eventField?.toLowerCase().endsWith('.subinstrumentid')) return true;
            return false;
          };
          return [...new Set([
            ...steps.filter(isSubIdStep).map(s => s.name),
            ...savedRulesVars.filter(isSubIdStep).map(s => s.name),
          ].filter(Boolean))];
        })()}
      />

      {/* Action Bar */}
      <Box sx={{ p: 2, borderTop: '1px solid #E9ECEF', bgcolor: 'white', display: 'flex', gap: 1, justifyContent: 'flex-end', flexShrink: 0 }}>
        {onClose && <Button onClick={onClose} color="inherit">Cancel</Button>}
        <Button variant="outlined" onClick={handleSave} disabled={saving}
          startIcon={saving ? <CircularProgress size={16} /> : <Save size={16} />}
          sx={{ borderColor: '#1976D2', color: '#1976D2', '&:hover': { borderColor: '#1565C0', bgcolor: '#E3F2FD' } }}>
          {saving ? 'Saving...' : 'Save Rule'}
        </Button>
      </Box>
    </Box>
  );
};


// ═══════════════════════════════════════════════════════════════════════
// Convert old-format initialData to new unified steps array
// ═══════════════════════════════════════════════════════════════════════
function convertInitialDataToSteps(data) {
  // If the rule was already saved with the new 'steps' array, use it directly
  if (data.steps?.length > 0) return data.steps;

  const steps = [];

  // Convert legacy variables to calc steps
  if (data.variables?.length > 0) {
    for (const v of data.variables) {
      if (!v.name) continue;
      steps.push({
        name: v.name,
        stepType: 'calc',
        source: v.source || 'formula',
        formula: v.formula || '',
        value: v.value || '',
        eventField: v.eventField || '',
        collectType: v.collectType || 'collect',
      });
    }
  }

  // Convert legacy conditions to a condition step
  if (data.ruleType === 'conditional' && data.conditions?.length > 0) {
    steps.push({
      name: data.conditionResultVar || 'result',
      stepType: 'condition',
      conditions: data.conditions,
      elseFormula: data.elseFormula || '',
    });
  }

  // Convert legacy iterations to an iteration step
  if (data.ruleType === 'iteration') {
    const iters = data.iterations?.length ? data.iterations : (data.iterConfig?.type ? [data.iterConfig] : []);
    if (iters.length > 0) {
      const lastName = iters[iters.length - 1]?.resultVar || 'mapped_result';
      steps.push({
        name: lastName,
        stepType: 'iteration',
        iterations: iters,
      });
    }
  }

  // Fallback for legacy schedule rules without steps: create a custom_code step
  if (data.ruleType === 'schedule' && steps.length === 0 && data.generatedCode) {
    steps.push({
      name: 'schedule_code',
      stepType: 'custom_code',
      customCode: data.generatedCode,
    });
  }

  return steps;
}


export default AccountingRuleBuilder;
