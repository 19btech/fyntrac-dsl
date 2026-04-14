import React, { useState, useMemo, useCallback, useEffect } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, CircularProgress, Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, Play, Code, Eye, Save, Sparkles,
  Calculator, Hash, Type, List, ToggleLeft, GitBranch, Repeat, FlaskConical,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";

const RULE_TYPES = [
  { value: 'simple_calc', label: 'Simple Calculation', description: 'Compute values using formulas', icon: Calculator },
  { value: 'conditional', label: 'Conditional Logic', description: 'Apply different formulas based on conditions', icon: GitBranch },
  { value: 'iteration', label: 'Iteration / Loop', description: 'Process arrays or collections with for_each / map', icon: Repeat },
  { value: 'custom_code', label: 'Custom Code', description: 'Write raw DSL code directly in the rule', icon: Code },
];

const VariableRow = ({ variable, index, events, definedVarNames, onUpdate, onRemove, onMoveUp, onMoveDown, isFirst, isLast, onTest }) => {
  const [varTesting, setVarTesting] = useState(false);
  const [varTestResult, setVarTestResult] = useState(null);

  const eventFields = useMemo(() => {
    if (!events || events.length === 0) return [];
    const result = [];
    events.forEach((event) => {
      ['postingdate', 'effectivedate', 'subinstrumentid'].forEach(sf => {
        result.push(`${event.event_name}.${sf}`);
      });
      event.fields.forEach(f => result.push(`${event.event_name}.${f.name}`));
    });
    return result;
  }, [events]);

  return (
    <Card sx={{ mb: 1.5, borderLeft: '3px solid #5B5FED' }}>
      <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
        <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'flex-start' }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
              <TextField
                size="small" label="Variable Name *" value={variable.name}
                onChange={(e) => onUpdate(index, { ...variable, name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') })}
                sx={{ flex: 1 }}
                placeholder="e.g., monthly_payment"
              />
              <FormControl size="small" sx={{ minWidth: 150 }}>
                <InputLabel>Source</InputLabel>
                <Select value={variable.source || 'formula'} label="Source"
                  onChange={(e) => onUpdate(index, { ...variable, source: e.target.value })}>
                  <MenuItem value="formula">Formula</MenuItem>
                  <MenuItem value="value">Fixed Value</MenuItem>
                  <MenuItem value="event_field">Event Field</MenuItem>
                  <MenuItem value="collect">Collect from Events</MenuItem>
                </Select>
              </FormControl>
            </Box>

            {variable.source === 'formula' && (
              <FormulaBar
                value={variable.formula || ''}
                onChange={(val) => onUpdate(index, { ...variable, formula: val })}
                events={events}
                variables={definedVarNames}
                label="Formula"
                placeholder="e.g., multiply(principal, rate)"
              />
            )}

            {variable.source === 'value' && (
              <TextField size="small" fullWidth label="Value" value={variable.value || ''}
                onChange={(e) => onUpdate(index, { ...variable, value: e.target.value })}
                placeholder="e.g., 100000 or &quot;2026-01-01&quot;" />
            )}

            {variable.source === 'event_field' && (
              <FormControl fullWidth size="small">
                <InputLabel>Event Field</InputLabel>
                <Select value={variable.eventField || ''} label="Event Field"
                  onChange={(e) => onUpdate(index, { ...variable, eventField: e.target.value })}>
                  <MenuItem value="" disabled><em>Select event field...</em></MenuItem>
                  {eventFields.map(ef => <MenuItem key={ef} value={ef}>{ef}</MenuItem>)}
                </Select>
              </FormControl>
            )}

            {variable.source === 'collect' && (
              <Box sx={{ display: 'flex', gap: 1 }}>
                <FormControl size="small" sx={{ minWidth: 180 }}>
                  <InputLabel>Collect Type</InputLabel>
                  <Select value={variable.collectType || 'collect'} label="Collect Type"
                    onChange={(e) => onUpdate(index, { ...variable, collectType: e.target.value })}>
                    <MenuItem value="collect">collect (by posting date)</MenuItem>
                    <MenuItem value="collect_by_instrument">collect_by_instrument</MenuItem>
                    <MenuItem value="collect_all">collect_all</MenuItem>
                    <MenuItem value="collect_by_subinstrument">collect_by_subinstrument</MenuItem>
                    <MenuItem value="collect_subinstrumentids">collect_subinstrumentids</MenuItem>
                  </Select>
                </FormControl>
                <FormControl fullWidth size="small">
                  <InputLabel>Event Field</InputLabel>
                  <Select value={variable.eventField || ''} label="Event Field"
                    onChange={(e) => onUpdate(index, { ...variable, eventField: e.target.value })}>
                    <MenuItem value="" disabled><em>Select...</em></MenuItem>
                    {eventFields.map(ef => <MenuItem key={ef} value={ef}>{ef}</MenuItem>)}
                  </Select>
                </FormControl>
              </Box>
            )}
          </Box>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.25, pt: 0.5 }}>
            <Tooltip title="Test up to this step">
              <IconButton size="small" onClick={async () => {
                if (!variable.name || !onTest) return;
                setVarTesting(true);
                setVarTestResult(null);
                try {
                  const result = await onTest(index);
                  setVarTestResult(result);
                } catch (e) {
                  setVarTestResult({ success: false, error: e.message });
                } finally {
                  setVarTesting(false);
                }
              }} disabled={varTesting || !variable.name} sx={{ color: '#4CAF50' }}>
                {varTesting ? <CircularProgress size={14} /> : <Play size={14} />}
              </IconButton>
            </Tooltip>
            <IconButton size="small" onClick={() => onMoveUp(index)} disabled={isFirst}><ArrowUp size={14} /></IconButton>
            <IconButton size="small" onClick={() => onMoveDown(index)} disabled={isLast}><ArrowDown size={14} /></IconButton>
            <IconButton size="small" onClick={() => onRemove(index)} sx={{ color: '#F44336' }}><Trash2 size={14} /></IconButton>
          </Box>
        </Box>
        {varTestResult && (
          <Alert severity={varTestResult.success ? 'success' : 'error'} sx={{ mt: 1, '& .MuiAlert-message': { width: '100%' } }}
            onClose={() => setVarTestResult(null)}>
            {varTestResult.success ? (
              <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap' }}>
                {varTestResult.output}
              </Typography>
            ) : (
              <Typography variant="body2">{varTestResult.error}</Typography>
            )}
          </Alert>
        )}
      </CardContent>
    </Card>
  );
};

const ConditionRow = ({ condition, index, events, definedVarNames, onUpdate, onRemove }) => (
  <Card sx={{ mb: 1, borderLeft: '3px solid #FF9800' }}>
    <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
      <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>
        {index === 0 ? 'IF' : 'ELSE IF'}
      </Typography>
      <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
        <Box sx={{ flex: 1 }}>
          <FormulaBar
            value={condition.condition || ''}
            onChange={(val) => onUpdate(index, { ...condition, condition: val })}
            events={events}
            variables={definedVarNames}
            label="Condition"
            placeholder='e.g., gt(balance, 0)'
          />
        </Box>
        <IconButton size="small" onClick={() => onRemove(index)} sx={{ color: '#F44336' }}><Trash2 size={14} /></IconButton>
      </Box>
      <FormulaBar
        value={condition.thenFormula || ''}
        onChange={(val) => onUpdate(index, { ...condition, thenFormula: val })}
        events={events}
        variables={definedVarNames}
        label="Then (result)"
        placeholder="e.g., multiply(balance, rate)"
      />
    </CardContent>
  </Card>
);

/**
 * AccountingRuleBuilder — Form-based rule builder for accounting calculations.
 * Supports: simple calculations, conditional logic, iteration, collect.
 */
const AccountingRuleBuilder = ({ events, dslFunctions, onClose, onSave, initialData }) => {
  const [ruleType, setRuleType] = useState(initialData?.ruleType || 'simple_calc');
  const [ruleName, setRuleName] = useState(initialData?.name || '');
  const [rulePriority, setRulePriority] = useState(initialData?.priority ?? '');
  const [ruleId, setRuleId] = useState(initialData?.id || null);

  // Fetch all saved-rules for FormulaBar hints and per-variable testing
  const [savedRulesVarNames, setSavedRulesVarNames] = useState([]);
  const [savedRulesVars, setSavedRulesVars] = useState([]); // full variable objects from all saved rules
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
  const [variables, setVariables] = useState(
    initialData?.variables?.length ? initialData.variables :
    [{ name: '', source: 'value', value: '', formula: '', eventField: '', collectType: 'collect' }]
  );

  // Conditional config
  const [conditions, setConditions] = useState(
    initialData?.conditions?.length ? initialData.conditions :
    [{ condition: '', thenFormula: '' }]
  );
  const [elseFormula, setElseFormula] = useState(initialData?.elseFormula || '');
  const [conditionResultVar, setConditionResultVar] = useState(initialData?.conditionResultVar || 'result');

  // Iteration config
  const [iterConfig, setIterConfig] = useState(
    initialData?.iterConfig?.type ? initialData.iterConfig :
    {
      type: 'map_array',
      sourceArray: '', varName: 'item', expression: '',
      resultVar: 'mapped_result',
      secondArray: '', secondVar: 'amount',
    }
  );

  // Outputs
  const [outputs, setOutputs] = useState(
    initialData?.outputs?.printResult !== undefined ? initialData.outputs :
    {
      printResult: true,
      createTransaction: false,
      transactions: [{ type: 'Calculation Result', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }],
    }
  );
  const [inlineComment, setInlineComment] = useState(initialData?.inlineComment || false);
  const [commentText, setCommentText] = useState(initialData?.commentText || '');
  const [customCode, setCustomCode] = useState(initialData?.customCode || '');
  const [customCodeTesting, setCustomCodeTesting] = useState(false);
  const [customCodeOutput, setCustomCodeOutput] = useState('');
  const [saving, setSaving] = useState(false);
  const [showCode, setShowCode] = useState(false);
  const [saveResult, setSaveResult] = useState(null); // { success, output, error }
  const [validationMsg, setValidationMsg] = useState('');

  // Variable CRUD
  const addVariable = useCallback(() => {
    setVariables(prev => [...prev, { name: '', source: 'formula', value: '', formula: '', eventField: '', collectType: 'collect' }]);
  }, []);
  const updateVariable = useCallback((index, updated) => {
    setVariables(prev => prev.map((v, i) => i === index ? updated : v));
  }, []);
  const removeVariable = useCallback((index) => {
    setVariables(prev => prev.filter((_, i) => i !== index));
  }, []);
  const moveVariable = useCallback((index, direction) => {
    setVariables(prev => {
      const arr = [...prev];
      const target = index + direction;
      if (target < 0 || target >= arr.length) return arr;
      [arr[index], arr[target]] = [arr[target], arr[index]];
      return arr;
    });
  }, []);

  // Test a single variable (generates code for saved-rule vars + current vars up to this index, then runs it)
  const testVariable = useCallback(async (varIndex) => {
    const varsToTest = variables.slice(0, varIndex + 1);
    const currentVarNames = new Set(varsToTest.filter(v => v.name).map(v => v.name));
    const lines = [];

    // First, emit definitions from saved rules (skip any that the current rule redefines)
    const emittedSaved = new Set();
    for (const v of savedRulesVars) {
      if (!v.name || currentVarNames.has(v.name) || emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.source === 'value') {
        lines.push(`${v.name} = ${v.value || 0}`);
      } else if (v.source === 'event_field') {
        lines.push(`${v.name} = ${v.eventField}`);
      } else if (v.source === 'formula') {
        lines.push(`${v.name} = ${v.formula || 0}`);
      } else if (v.source === 'collect') {
        lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
      }
    }

    // Then emit current rule variables up to varIndex
    for (const v of varsToTest) {
      if (!v.name) continue;
      if (v.source === 'value') {
        lines.push(`${v.name} = ${v.value || 0}`);
      } else if (v.source === 'event_field') {
        lines.push(`${v.name} = ${v.eventField}`);
      } else if (v.source === 'formula') {
        lines.push(`${v.name} = ${v.formula || 0}`);
      } else if (v.source === 'collect') {
        lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
      }
    }
    // Print each variable's value
    for (const v of varsToTest) {
      if (v.name) lines.push(`print("${v.name} =", ${v.name})`);
    }
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
      const errMsg = data.error || (typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail)) || 'Execution failed';
      return { success: false, error: errMsg };
    }
  }, [variables, savedRulesVars]);

  // Condition CRUD
  const addCondition = useCallback(() => setConditions(prev => [...prev, { condition: '', thenFormula: '' }]), []);
  const updateCondition = useCallback((i, u) => setConditions(prev => prev.map((c, j) => j === i ? u : c)), []);
  const removeCondition = useCallback((i) => setConditions(prev => prev.filter((_, j) => j !== i)), []);

  // Test Conditional Logic — run all variables + conditional to see result
  const [condTesting, setCondTesting] = useState(false);
  const [condTestResult, setCondTestResult] = useState(null);
  const testConditional = useCallback(async () => {
    setCondTesting(true);
    setCondTestResult(null);
    const lines = [];
    // Saved rule dependencies
    const currentVarNames = new Set(variables.filter(v => v.name).map(v => v.name));
    const emittedSaved = new Set();
    for (const v of savedRulesVars) {
      if (!v.name || currentVarNames.has(v.name) || emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
    // Current variables
    for (const v of variables) {
      if (!v.name) continue;
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
    // Conditional
    const validConds = conditions.filter(c => c.condition && c.thenFormula);
    if (validConds.length === 1) {
      lines.push(`${conditionResultVar} = iif(${validConds[0].condition}, ${validConds[0].thenFormula}, ${elseFormula || 0})`);
    } else if (validConds.length > 1) {
      let nested = elseFormula || '0';
      for (let i = validConds.length - 1; i >= 0; i--) {
        nested = `iif(${validConds[i].condition}, ${validConds[i].thenFormula}, ${nested})`;
      }
      lines.push(`${conditionResultVar} = ${nested}`);
    }
    lines.push(`print("${conditionResultVar} =", ${conditionResultVar})`);
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: lines.join('\n'), posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setCondTestResult({ success: true, output: (data.print_outputs || []).join('\n') || 'OK' });
      } else {
        setCondTestResult({ success: false, error: data.error || data.detail || 'Failed' });
      }
    } catch (e) {
      setCondTestResult({ success: false, error: e.message });
    } finally {
      setCondTesting(false);
    }
  }, [variables, savedRulesVars, conditions, conditionResultVar, elseFormula]);

  // Test Iteration — run all variables + iteration to see result
  const [iterTesting, setIterTesting] = useState(false);
  const [iterTestResult, setIterTestResult] = useState(null);
  const testIteration = useCallback(async () => {
    setIterTesting(true);
    setIterTestResult(null);
    const lines = [];
    // Saved rule dependencies
    const currentVarNames = new Set(variables.filter(v => v.name).map(v => v.name));
    const emittedSaved = new Set();
    for (const v of savedRulesVars) {
      if (!v.name || currentVarNames.has(v.name) || emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
    // Current variables
    const definedVars = [];
    for (const v of variables) {
      if (!v.name) continue;
      definedVars.push(v.name);
      if (v.source === 'value') lines.push(`${v.name} = ${v.value || 0}`);
      else if (v.source === 'event_field') lines.push(`${v.name} = ${v.eventField}`);
      else if (v.source === 'formula') lines.push(`${v.name} = ${v.formula || 0}`);
      else if (v.source === 'collect') lines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
    }
    // Iteration
    if (iterConfig.type === 'map_array') {
      lines.push(`${iterConfig.resultVar} = map_array(${iterConfig.sourceArray}, "${iterConfig.varName}", "${iterConfig.expression}"${definedVars.length ? `, {${definedVars.map(v => `"${v}": ${v}`).join(', ')}}` : ''})`);
    } else {
      lines.push(`${iterConfig.resultVar} = for_each(${iterConfig.sourceArray}, ${iterConfig.secondArray || '[]'}, "${iterConfig.varName}", "${iterConfig.secondVar}", "${iterConfig.expression}")`);
    }
    lines.push(`print("${iterConfig.resultVar} =", ${iterConfig.resultVar})`);
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: lines.join('\n'), posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setIterTestResult({ success: true, output: (data.print_outputs || []).join('\n') || 'OK' });
      } else {
        setIterTestResult({ success: false, error: data.error || data.detail || 'Failed' });
      }
    } catch (e) {
      setIterTestResult({ success: false, error: e.message });
    } finally {
      setIterTesting(false);
    }
  }, [variables, savedRulesVars, iterConfig]);

  // Transaction CRUD
  const addTransaction = useCallback(() => {
    setOutputs(prev => ({ ...prev, transactions: [...prev.transactions, { type: '', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }] }));
  }, []);
  const updateTransaction = useCallback((i, field, val) => {
    setOutputs(prev => ({
      ...prev,
      transactions: prev.transactions.map((t, j) => j === i ? { ...t, [field]: val } : t),
    }));
  }, []);
  const removeTransaction = useCallback((i) => {
    setOutputs(prev => ({ ...prev, transactions: prev.transactions.filter((_, j) => j !== i) }));
  }, []);

  const generatedCode = useMemo(() => {
    // Custom code rule — just return the raw code as-is
    if (ruleType === 'custom_code') {
      return customCode;
    }

    const lines = [];
    if (inlineComment && commentText.trim()) {
      commentText.trim().split('\n').forEach(l => lines.push(`## ${l}`));
      lines.push('');
    }
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push(`## ${(ruleName || 'CUSTOM CALCULATION').toUpperCase()}`);
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push('');

    // Collect current rule's variable names to avoid duplicating from saved rules
    const currentVarNames = new Set(variables.filter(v => v.name).map(v => v.name));

    // Prepend dependency variables from saved rules (skip any redefined in current rule)
    const emittedSaved = new Set();
    const depLines = [];
    for (const v of savedRulesVars) {
      if (!v.name || currentVarNames.has(v.name) || emittedSaved.has(v.name)) continue;
      emittedSaved.add(v.name);
      if (v.source === 'value') {
        depLines.push(`${v.name} = ${v.value || 0}`);
      } else if (v.source === 'event_field') {
        depLines.push(`${v.name} = ${v.eventField}`);
      } else if (v.source === 'formula') {
        depLines.push(`${v.name} = ${v.formula || 0}`);
      } else if (v.source === 'collect') {
        depLines.push(`${v.name} = ${v.collectType || 'collect'}(${v.eventField})`);
      }
    }
    if (depLines.length > 0) {
      lines.push('## Dependencies from saved rules');
      lines.push(...depLines);
      lines.push('');
    }

    // Emit variable definitions
    const definedVars = [];
    for (const v of variables) {
      if (!v.name) continue;
      definedVars.push(v.name);
      if (v.source === 'value') {
        lines.push(`${v.name} = ${v.value || 0}`);
      } else if (v.source === 'event_field') {
        lines.push(`${v.name} = ${v.eventField}`);
      } else if (v.source === 'formula') {
        lines.push(`${v.name} = ${v.formula || 0}`);
      } else if (v.source === 'collect') {
        const ct = v.collectType || 'collect';
        lines.push(`${v.name} = ${ct}(${v.eventField})`);
      }
    }
    if (definedVars.length) lines.push('');

    // ── Conditional ──
    if (ruleType === 'conditional') {
      lines.push('## Conditional Logic');
      const validConds = conditions.filter(c => c.condition && c.thenFormula);
      if (validConds.length === 1) {
        lines.push(`${conditionResultVar} = iif(${validConds[0].condition}, ${validConds[0].thenFormula}, ${elseFormula || 0})`);
      } else if (validConds.length > 1) {
        // Nested iif for multiple conditions
        let nested = elseFormula || '0';
        for (let i = validConds.length - 1; i >= 0; i--) {
          nested = `iif(${validConds[i].condition}, ${validConds[i].thenFormula}, ${nested})`;
        }
        lines.push(`${conditionResultVar} = ${nested}`);
      }
      lines.push('');
    }

    // ── Iteration ──
    if (ruleType === 'iteration') {
      lines.push('## Iteration');
      if (iterConfig.type === 'map_array') {
        lines.push(`${iterConfig.resultVar} = map_array(${iterConfig.sourceArray}, "${iterConfig.varName}", "${iterConfig.expression}"${definedVars.length ? `, {${definedVars.map(v => `"${v}": ${v}`).join(', ')}}` : ''})`);
      } else {
        // for_each with paired arrays
        lines.push(`${iterConfig.resultVar} = for_each(${iterConfig.sourceArray}, ${iterConfig.secondArray || '[]'}, "${iterConfig.varName}", "${iterConfig.secondVar}", "${iterConfig.expression}")`);
      }
      lines.push('');
    }

    // ── Print results ──
    if (outputs.printResult && ruleType === 'simple_calc') {
      const lastVar = definedVars[definedVars.length - 1];
      if (lastVar) lines.push(`print("Result:", ${lastVar})`);
    }
    if (outputs.printResult && ruleType === 'conditional') {
      lines.push(`print("Result:", ${conditionResultVar})`);
    }
    if (outputs.printResult && ruleType === 'iteration') {
      lines.push(`print("Result:", ${iterConfig.resultVar})`);
    }

    // ── Transactions ──
    if (outputs.createTransaction) {
      lines.push('');
      lines.push('## Create Transactions');
      for (const txn of outputs.transactions) {
        if (!txn.type) continue;
        const amt = txn.amount || definedVars[definedVars.length - 1] || (ruleType === 'conditional' ? conditionResultVar : '0');
        const pd = txn.postingDate || 'postingdate';
        const ed = txn.effectiveDate || pd;
        const sid = txn.subInstrumentId || '';
        if (sid) {
          lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt}, ${sid})`);
        } else {
          lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt})`);
        }
      }
    }

    return lines.join('\n');
  }, [ruleName, ruleType, variables, outputs, conditions, elseFormula, conditionResultVar, iterConfig, inlineComment, commentText, savedRulesVars, customCode]);

  const handleSave = useCallback(async () => {
    if (!ruleName.trim()) {
      setValidationMsg('Rule Name is required and not populated.');
      return;
    }
    if (rulePriority === '' || rulePriority === null || rulePriority === undefined) {
      setValidationMsg('Priority is required and not populated.');
      return;
    }
    const emptyVars = variables.filter(v => !v.name);
    if (ruleType !== 'custom_code' && emptyVars.length > 0) {
      setValidationMsg('Variable Name is required and not populated for one or more calculation steps.');
      return;
    }
    if (ruleType === 'custom_code' && !customCode.trim()) {
      setValidationMsg('Custom code is required. Please write some DSL code.');
      return;
    }
    setSaving(true);
    setSaveResult(null);
    try {
      const payload = {
        id: ruleId,
        name: ruleName.trim(),
        priority: rulePriority === '' ? null : Number(rulePriority),
        ruleType,
        variables: ruleType === 'custom_code' ? [] : variables,
        conditions,
        elseFormula,
        conditionResultVar,
        iterConfig,
        outputs,
        inlineComment,
        commentText,
        customCode: ruleType === 'custom_code' ? customCode : '',
        generatedCode,
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
        // Clear the form after successful save
        setTimeout(() => {
          resetForm();
        }, 1500);
      } else {
        const errMsg = data.detail || data.error || 'Save failed';
        setSaveResult({ success: false, error: typeof errMsg === 'string' ? errMsg : JSON.stringify(errMsg) });
      }
    } catch (err) {
      setSaveResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setSaving(false);
    }
  }, [ruleName, rulePriority, ruleId, ruleType, variables, conditions, elseFormula, conditionResultVar, iterConfig, outputs, inlineComment, commentText, generatedCode, onSave]);

  const resetForm = useCallback(() => {
    setRuleName('');
    setRulePriority('');
    setRuleId(null);
    setRuleType('simple_calc');
    setVariables([{ name: '', source: 'value', value: '', formula: '', eventField: '', collectType: 'collect' }]);
    setConditions([{ condition: '', thenFormula: '' }]);
    setElseFormula('');
    setConditionResultVar('result');
    setIterConfig({ type: 'map_array', sourceArray: '', varName: 'item', expression: '', resultVar: 'mapped_result', secondArray: '', secondVar: 'amount' });
    setOutputs({ printResult: true, createTransaction: false, transactions: [{ type: 'Calculation Result', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }] });
    setInlineComment(false);
    setCommentText('');
    setCustomCode('');
    setShowCode(false);
    setSaveResult(null);
  }, []);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%' }}>
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
            placeholder="e.g., Monthly Interest Accrual"
            sx={{ flex: 1 }} />
          <TextField size="small" label="Priority *" value={rulePriority}
            onChange={(e) => { const v = e.target.value; if (v === '' || /^\d+$/.test(v)) setRulePriority(v === '' ? '' : Number(v)); }}
            placeholder="e.g., 1"
            type="number"
            inputProps={{ min: 0, step: 1 }}
            sx={{ width: 140 }} />
        </Box>

        {/* Rule Type */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>Calculation Type</Typography>
        <Box sx={{ display: 'flex', gap: 1, mb: 2.5, flexWrap: 'wrap' }}>
          {RULE_TYPES.map((rt) => (
            <Card key={rt.value} onClick={() => setRuleType(rt.value)}
              sx={{
                flex: '1 1 170px', cursor: 'pointer', p: 1.5,
                border: ruleType === rt.value ? '2px solid #5B5FED' : '1px solid #E9ECEF',
                bgcolor: ruleType === rt.value ? '#EEF0FE' : '#FFFFFF',
              }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                <rt.icon size={16} color={ruleType === rt.value ? '#5B5FED' : '#6C757D'} />
                <Typography variant="body2" fontWeight={600}>{rt.label}</Typography>
              </Box>
              <Typography variant="caption" color="text.secondary">{rt.description}</Typography>
            </Card>
          ))}
        </Box>

        <Divider sx={{ mb: 2 }} />

        {/* ── Custom Code Editor ── */}
        {ruleType === 'custom_code' && (
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
              <Code size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
              DSL Code
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
              Write raw DSL code. This will be saved as a rule and combined with other rules in priority order.
            </Typography>
            <Paper variant="outlined" sx={{ bgcolor: '#0D1117', borderRadius: 1.5, overflow: 'hidden' }}>
              <textarea
                value={customCode}
                onChange={(e) => setCustomCode(e.target.value)}
                placeholder="## Write DSL code here&#10;loan_amount = LoanEvent.principal&#10;rate = divide(LoanEvent.rate, 12)&#10;payment = pmt(rate, 12, loan_amount)&#10;print(payment)"
                style={{
                  width: '100%', minHeight: 220, padding: 16,
                  fontFamily: 'monospace', fontSize: '0.8125rem', lineHeight: 1.6,
                  color: '#E6EDF3', backgroundColor: 'transparent',
                  border: 'none', outline: 'none', resize: 'vertical',
                  tabSize: 4,
                }}
                spellCheck={false}
              />
            </Paper>

            {/* Mini Console for testing custom code */}
            <Box sx={{ mt: 1.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary">Console</Typography>
                <Box sx={{ display: 'flex', gap: 0.5 }}>
                  <Button size="small" variant="outlined"
                    onClick={() => setCustomCodeOutput('')}
                    disabled={!customCodeOutput}
                    sx={{ fontSize: '0.7rem', minHeight: 24, px: 1, py: 0, color: '#8B949E', borderColor: '#30363D', '&:hover': { borderColor: '#8B949E' } }}>
                    Clear
                  </Button>
                  <Button size="small" variant="contained"
                    startIcon={customCodeTesting ? <CircularProgress size={12} color="inherit" /> : <Play size={12} />}
                    disabled={customCodeTesting || !customCode.trim()}
                    onClick={async () => {
                      setCustomCodeTesting(true);
                      setCustomCodeOutput('');
                      try {
                        const today = new Date().toISOString().split('T')[0];
                        const response = await fetch(`${API}/dsl/run`, {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({ dsl_code: customCode, posting_date: today }),
                        });
                        const data = await response.json();
                        if (response.ok && data.success) {
                          const out = (data.print_outputs || []).map(String).join('\n') || 'Executed successfully (no output)';
                          setCustomCodeOutput(out);
                        } else {
                          setCustomCodeOutput('ERROR: ' + (data.error || data.detail || 'Execution failed'));
                        }
                      } catch (e) {
                        setCustomCodeOutput('ERROR: ' + (e.message || 'Network error'));
                      } finally {
                        setCustomCodeTesting(false);
                      }
                    }}
                    sx={{ fontSize: '0.7rem', minHeight: 24, px: 1.5, py: 0, bgcolor: '#4CAF50', '&:hover': { bgcolor: '#388E3C' } }}>
                    Run
                  </Button>
                </Box>
              </Box>
              <Paper variant="outlined" sx={{ bgcolor: '#161B22', borderRadius: 1, minHeight: 60, maxHeight: 180, overflow: 'auto', p: 1.5 }}>
                <Typography component="pre" variant="body2" sx={{
                  fontFamily: 'monospace', fontSize: '0.75rem', lineHeight: 1.5, whiteSpace: 'pre-wrap',
                  color: customCodeOutput.startsWith('ERROR:') ? '#F85149' : '#7EE787', m: 0,
                }}>
                  {customCodeOutput || <span style={{ color: '#484F58' }}>Click Run to test your code...</span>}
                </Typography>
              </Paper>
            </Box>
          </Box>
        )}

        {/* ── Variables / Parameters (not for custom_code) ── */}
        {ruleType !== 'custom_code' && (
          <>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <Typography variant="body2" fontWeight={600}>
            Calculation Steps
          </Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={addVariable}>Add Step</Button>
        </Box>

        {variables.map((variable, idx) => (
          <VariableRow key={idx} variable={variable} index={idx} events={events}
            definedVarNames={[...new Set([...variables.filter(v => v.name).map(v => v.name), ...savedRulesVarNames])]}
            onUpdate={updateVariable} onRemove={removeVariable}
            onMoveUp={() => moveVariable(idx, -1)} onMoveDown={() => moveVariable(idx, 1)}
            isFirst={idx === 0} isLast={idx === variables.length - 1}
            onTest={testVariable} />
        ))}
          </>
        )}

        {/* ── Conditional Logic Section ── */}
        {ruleType === 'conditional' && (
          <>
            <Divider sx={{ my: 2 }} />
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="body2" fontWeight={600}>
                <GitBranch size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
                Conditions (IF / ELSE IF / ELSE)
              </Typography>
              <Box sx={{ display: 'flex', gap: 0.5 }}>
                <Tooltip title="Test conditional logic">
                  <IconButton size="small" onClick={testConditional} disabled={condTesting} sx={{ color: '#4CAF50' }}>
                    {condTesting ? <CircularProgress size={14} /> : <Play size={14} />}
                  </IconButton>
                </Tooltip>
                <Button size="small" startIcon={<Plus size={14} />} onClick={addCondition}>Add Condition</Button>
              </Box>
            </Box>
            <TextField size="small" label="Result Variable Name" value={conditionResultVar}
              onChange={(e) => setConditionResultVar(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
              sx={{ mb: 1.5 }} placeholder="result" />

            {conditions.map((cond, idx) => (
              <ConditionRow key={idx} condition={cond} index={idx} events={events}
                definedVarNames={[...new Set([...variables.filter(v => v.name).map(v => v.name), ...savedRulesVarNames])]}
                onUpdate={updateCondition} onRemove={removeCondition} />
            ))}

            <Card sx={{ mb: 1, borderLeft: '3px solid #9E9E9E' }}>
              <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>ELSE (default)</Typography>
                <FormulaBar
                  value={elseFormula}
                  onChange={setElseFormula}
                  events={events}
                  variables={[...new Set([...variables.filter(v => v.name).map(v => v.name), ...savedRulesVarNames])]}
                  label="Default value"
                  placeholder="e.g., 0"
                />
              </CardContent>
            </Card>
            {condTestResult && (
              <Alert severity={condTestResult.success ? 'success' : 'error'} sx={{ mt: 1, '& .MuiAlert-message': { width: '100%' } }}
                onClose={() => setCondTestResult(null)}>
                {condTestResult.success ? (
                  <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap' }}>
                    {condTestResult.output}
                  </Typography>
                ) : (
                  <Typography variant="body2">{condTestResult.error}</Typography>
                )}
              </Alert>
            )}
          </>
        )}

        {/* ── Iteration Section ── */}
        {ruleType === 'iteration' && (
          <>
            <Divider sx={{ my: 2 }} />
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="body2" fontWeight={600}>
                <Repeat size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
                Iteration Configuration
              </Typography>
              <Tooltip title="Test iteration logic">
                <IconButton size="small" onClick={testIteration} disabled={iterTesting} sx={{ color: '#4CAF50' }}>
                  {iterTesting ? <CircularProgress size={14} /> : <Play size={14} />}
                </IconButton>
              </Tooltip>
            </Box>
            <Box sx={{ display: 'flex', gap: 1, mb: 1.5 }}>
              <FormControl size="small" sx={{ minWidth: 200 }}>
                <InputLabel>Iteration Type</InputLabel>
                <Select value={iterConfig.type} label="Iteration Type"
                  onChange={(e) => setIterConfig(prev => ({ ...prev, type: e.target.value }))}>
                  <MenuItem value="map_array">map_array — Transform each element</MenuItem>
                  <MenuItem value="for_each">for_each — Process paired arrays</MenuItem>
                </Select>
              </FormControl>
              <TextField size="small" label="Result Variable" value={iterConfig.resultVar}
                onChange={(e) => setIterConfig(prev => ({ ...prev, resultVar: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') }))}
                sx={{ flex: '0 0 150px' }} />
            </Box>
            <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
              <TextField size="small" fullWidth label="Source Array" value={iterConfig.sourceArray}
                onChange={(e) => setIterConfig(prev => ({ ...prev, sourceArray: e.target.value }))}
                placeholder="e.g., amounts or collect(Event.amount)"
                sx={{ '& .MuiOutlinedInput-root': { fontFamily: 'monospace' } }} />
              <TextField size="small" label="Element Variable" value={iterConfig.varName}
                onChange={(e) => setIterConfig(prev => ({ ...prev, varName: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') }))}
                sx={{ flex: '0 0 150px' }} />
            </Box>
            {iterConfig.type === 'for_each' && (
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
                <TextField size="small" fullWidth label="Second Array" value={iterConfig.secondArray}
                  onChange={(e) => setIterConfig(prev => ({ ...prev, secondArray: e.target.value }))}
                  placeholder="e.g., dates"
                  sx={{ '& .MuiOutlinedInput-root': { fontFamily: 'monospace' } }} />
                <TextField size="small" label="Second Variable" value={iterConfig.secondVar}
                  onChange={(e) => setIterConfig(prev => ({ ...prev, secondVar: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') }))}
                  sx={{ flex: '0 0 150px' }} />
              </Box>
            )}
            <FormulaBar
              value={iterConfig.expression}
              onChange={(val) => setIterConfig(prev => ({ ...prev, expression: val }))}
              events={events}
              variables={[...new Set([...variables.filter(v => v.name).map(v => v.name), ...savedRulesVarNames])]}
              label="Expression (applied to each element)"
              placeholder='e.g., multiply(item, 1.1)'
            />
            {iterTestResult && (
              <Alert severity={iterTestResult.success ? 'success' : 'error'} sx={{ mt: 1, '& .MuiAlert-message': { width: '100%' } }}
                onClose={() => setIterTestResult(null)}>
                {iterTestResult.success ? (
                  <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem" sx={{ whiteSpace: 'pre-wrap' }}>
                    {iterTestResult.output}
                  </Typography>
                ) : (
                  <Typography variant="body2">{iterTestResult.error}</Typography>
                )}
              </Alert>
            )}
          </>
        )}

        {ruleType !== 'custom_code' && (<>
        <Divider sx={{ my: 2 }} />

        {/* ── Output Options ── */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>Output Options</Typography>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: inlineComment ? 1 : 0 }}>
              <Typography variant="body2">Inline comment</Typography>
              <Switch checked={inlineComment} onChange={(e) => setInlineComment(e.target.checked)} size="small" />
            </Box>
            {inlineComment && (
              <TextField
                size="small" fullWidth multiline minRows={2} maxRows={4}
                label="Description"
                placeholder="Describe what this rule does — will appear as ## comment above the rule"
                value={commentText}
                onChange={(e) => setCommentText(e.target.value)}
              />
            )}
          </CardContent>
        </Card>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 }, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="body2">Print Results for Preview</Typography>
            <Switch checked={outputs.printResult} onChange={(e) => setOutputs(p => ({ ...p, printResult: e.target.checked }))} size="small" />
          </CardContent>
        </Card>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: outputs.createTransaction ? 1 : 0 }}>
              <Typography variant="body2">Create transactions</Typography>
              <Switch checked={outputs.createTransaction} onChange={(e) => setOutputs(p => ({ ...p, createTransaction: e.target.checked }))} size="small" />
            </Box>
            {outputs.createTransaction && (
              <>
                {outputs.transactions.map((txn, idx) => {
                  const allVarNames = [...new Set([...variables.filter(v => v.name).map(v => v.name), ...savedRulesVarNames])];
                  const dateVarNames = allVarNames.filter(v => v.toLowerCase().includes('date'));
                  const eventFieldOptions = events?.flatMap(ev => [
                    ...['postingdate', 'effectivedate'].map(sf => `${ev.event_name}.${sf}`),
                    ...ev.fields.map(f => `${ev.event_name}.${f.name}`),
                  ]) || [];
                  const dateEventFields = events?.flatMap(ev => [
                    ...['postingdate', 'effectivedate'].map(sf => `${ev.event_name}.${sf}`),
                    ...ev.fields.filter(f => f.datatype === 'date' || f.name.includes('date')).map(f => `${ev.event_name}.${f.name}`),
                  ]) || [];

                  return (
                  <Card key={idx} variant="outlined" sx={{ p: 1, mb: 1, bgcolor: '#FAFAFA' }}>
                    <Box sx={{ display: 'flex', gap: 1, mb: 0.5, alignItems: 'flex-end' }}>
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.25, display: 'block' }}>Transaction Type</Typography>
                        <TextField size="small" fullWidth value={txn.type}
                          placeholder="e.g., Calculation Result"
                          onChange={(e) => updateTransaction(idx, 'type', e.target.value)} />
                      </Box>
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.25, display: 'block' }}>Amount</Typography>
                        <FormControl size="small" fullWidth>
                          <Select
                            value={txn.amount || ''}
                            onChange={(e) => updateTransaction(idx, 'amount', e.target.value)}
                            displayEmpty
                            renderValue={(val) => val || <em style={{ color: '#999' }}>Select amount...</em>}
                          >
                          {allVarNames.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#5B5FED' }}>— Calculated Variables —</MenuItem>
                          )}
                          {allVarNames.map(v => (
                            <MenuItem key={`var-${v}`} value={v} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{v}</MenuItem>
                          ))}
                          {eventFieldOptions.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#FF9800' }}>— Event Fields —</MenuItem>
                          )}
                          {eventFieldOptions.map(ef => (
                            <MenuItem key={`ef-${ef}`} value={ef} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{ef}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                      </Box>
                      {outputs.transactions.length > 1 && (
                        <IconButton size="small" onClick={() => removeTransaction(idx)} sx={{ color: '#F44336', alignSelf: 'center' }}><Trash2 size={12} /></IconButton>
                      )}
                    </Box>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.25, display: 'block' }}>Posting Date</Typography>
                        <FormControl size="small" fullWidth>
                          <Select
                            value={txn.postingDate || ''}
                            onChange={(e) => updateTransaction(idx, 'postingDate', e.target.value)}
                            displayEmpty
                            renderValue={(val) => val || <em style={{ color: '#999' }}>postingdate</em>}
                          >
                          <MenuItem value="postingdate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>postingdate</MenuItem>
                          <MenuItem value="effectivedate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>effectivedate</MenuItem>
                          {allVarNames.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#5B5FED' }}>— Variables —</MenuItem>
                          )}
                          {allVarNames.map(v => (
                            <MenuItem key={`dv-${v}`} value={v} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{v}</MenuItem>
                          ))}
                          {dateEventFields.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#FF9800' }}>— Event Fields —</MenuItem>
                          )}
                          {dateEventFields.map(ef => (
                            <MenuItem key={`def-${ef}`} value={ef} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{ef}</MenuItem>
                          ))}
                          </Select>
                        </FormControl>
                      </Box>
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.25, display: 'block' }}>Effective Date</Typography>
                        <FormControl size="small" fullWidth>
                          <Select
                            value={txn.effectiveDate || ''}
                            onChange={(e) => updateTransaction(idx, 'effectiveDate', e.target.value)}
                            displayEmpty
                            renderValue={(val) => val || <em style={{ color: '#999' }}>same as posting</em>}
                          >
                          <MenuItem value="postingdate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>postingdate</MenuItem>
                          <MenuItem value="effectivedate" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>effectivedate</MenuItem>
                          {allVarNames.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#5B5FED' }}>— Variables —</MenuItem>
                          )}
                          {allVarNames.map(v => (
                            <MenuItem key={`edv-${v}`} value={v} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{v}</MenuItem>
                          ))}
                          {dateEventFields.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#FF9800' }}>— Event Fields —</MenuItem>
                          )}
                          {dateEventFields.map(ef => (
                            <MenuItem key={`edef-${ef}`} value={ef} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{ef}</MenuItem>
                          ))}
                          </Select>
                        </FormControl>
                      </Box>
                    </Box>
                    <Box sx={{ display: 'flex', gap: 1, mt: 0.5 }}>
                      <Box sx={{ flex: 1 }}>
                        <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.25, display: 'block' }}>Sub Instrument ID</Typography>
                        <FormControl size="small" fullWidth>
                          <Select
                            value={txn.subInstrumentId || ''}
                            onChange={(e) => updateTransaction(idx, 'subInstrumentId', e.target.value)}
                            displayEmpty
                            renderValue={(val) => val || <em style={{ color: '#999' }}>default (1)</em>}
                          >
                          <MenuItem value="" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}><em>default (1)</em></MenuItem>
                          {allVarNames.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#5B5FED' }}>— Variables —</MenuItem>
                          )}
                          {allVarNames.map(v => (
                            <MenuItem key={`sid-${v}`} value={v} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{v}</MenuItem>
                          ))}
                          {eventFieldOptions.length > 0 && (
                            <MenuItem disabled sx={{ fontSize: '0.75rem', fontWeight: 600, color: '#FF9800' }}>— Event Fields —</MenuItem>
                          )}
                          {eventFieldOptions.map(ef => (
                            <MenuItem key={`sidef-${ef}`} value={ef} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{ef}</MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                      </Box>
                    </Box>
                  </Card>
                  );
                })}
                <Button size="small" startIcon={<Plus size={14} />} onClick={addTransaction}>Add Transaction</Button>
              </>
            )}
          </CardContent>
        </Card>
        </>)}

        {/* Code Preview */}
        <Divider sx={{ my: 2 }} />
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
            {saveResult.success ? (
              <Typography variant="body2">{saveResult.output}</Typography>
            ) : (
              <Typography variant="body2">{saveResult.error}</Typography>
            )}
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
          {saving ? 'Saving...' : 'Save Rule'}
        </Button>
      </Box>
    </Box>
  );
};

export default AccountingRuleBuilder;
