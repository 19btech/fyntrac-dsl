import React, { useState, useMemo, useCallback } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, IconButton,
  Tooltip, Divider, Select, FormControl, InputLabel, Paper, Switch, FormControlLabel,
  Alert, CircularProgress,
} from "@mui/material";
import {
  Plus, Trash2, ArrowUp, ArrowDown, Play, Code, Eye, Save, Sparkles,
  Calculator, Database, Hash, Type, List, ToggleLeft, GitBranch, Repeat, FlaskConical,
} from "lucide-react";
import { API } from "../../config";
import FormulaBar from "./FormulaBar";

const RULE_TYPES = [
  { value: 'simple_calc', label: 'Simple Calculation', description: 'Compute values using formulas', icon: Calculator },
  { value: 'conditional', label: 'Conditional Logic', description: 'Apply different formulas based on conditions', icon: GitBranch },
  { value: 'iteration', label: 'Iteration / Loop', description: 'Process arrays or collections with for_each / map', icon: Repeat },
  { value: 'collect', label: 'Collect & Aggregate', description: 'Collect event data and aggregate across instruments', icon: Database },
];

const VariableRow = ({ variable, index, events, onUpdate, onRemove, onMoveUp, onMoveDown, isFirst, isLast }) => {
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
                size="small" label="Variable Name" value={variable.name}
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
            <IconButton size="small" onClick={() => onMoveUp(index)} disabled={isFirst}><ArrowUp size={14} /></IconButton>
            <IconButton size="small" onClick={() => onMoveDown(index)} disabled={isLast}><ArrowDown size={14} /></IconButton>
            <IconButton size="small" onClick={() => onRemove(index)} sx={{ color: '#F44336' }}><Trash2 size={14} /></IconButton>
          </Box>
        </Box>
      </CardContent>
    </Card>
  );
};

const ConditionRow = ({ condition, index, events, onUpdate, onRemove }) => (
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
const AccountingRuleBuilder = ({ events, dslFunctions, onGenerate, onClose }) => {
  const [ruleType, setRuleType] = useState('simple_calc');
  const [ruleName, setRuleName] = useState('');
  const [variables, setVariables] = useState([
    { name: '', source: 'value', value: '', formula: '', eventField: '', collectType: 'collect' },
  ]);

  // Conditional config
  const [conditions, setConditions] = useState([
    { condition: '', thenFormula: '' },
  ]);
  const [elseFormula, setElseFormula] = useState('');
  const [conditionResultVar, setConditionResultVar] = useState('result');

  // Iteration config
  const [iterConfig, setIterConfig] = useState({
    type: 'map_array', // map_array | for_each
    sourceArray: '', varName: 'item', expression: '',
    resultVar: 'mapped_result',
    // for for_each paired:
    secondArray: '', secondVar: 'amount',
  });

  // Outputs
  const [outputs, setOutputs] = useState({
    printResult: true,
    createTransaction: false,
    transactions: [{ type: 'Calculation Result', amount: '', postingDate: '', effectiveDate: '' }],
  });
  const [showCode, setShowCode] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null); // { success, output, error }

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

  // Condition CRUD
  const addCondition = useCallback(() => setConditions(prev => [...prev, { condition: '', thenFormula: '' }]), []);
  const updateCondition = useCallback((i, u) => setConditions(prev => prev.map((c, j) => j === i ? u : c)), []);
  const removeCondition = useCallback((i) => setConditions(prev => prev.filter((_, j) => j !== i)), []);

  // Transaction CRUD
  const addTransaction = useCallback(() => {
    setOutputs(prev => ({ ...prev, transactions: [...prev.transactions, { type: '', amount: '', postingDate: '', effectiveDate: '' }] }));
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
    const lines = [];
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push(`## ${(ruleName || 'CUSTOM CALCULATION').toUpperCase()}`);
    lines.push('## ═══════════════════════════════════════════════════════════════');
    lines.push('');

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

    // ── Collect ──
    if (ruleType === 'collect') {
      // Already handled in variable definitions — add aggregate print
      const collectVars = variables.filter(v => v.source === 'collect' && v.name);
      if (collectVars.length > 0) {
        lines.push('## Aggregate Results');
        collectVars.forEach(cv => {
          lines.push(`print("Count of ${cv.name}:", count(${cv.name}))`);
          lines.push(`print("Sum of ${cv.name}:", sum(${cv.name}))`);
          lines.push(`print("Avg of ${cv.name}:", avg(${cv.name}))`);
        });
        lines.push('');
      }
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
        lines.push(`createTransaction(${pd}, ${ed}, "${txn.type}", ${amt})`);
      }
    }

    return lines.join('\n');
  }, [ruleName, ruleType, variables, outputs, conditions, elseFormula, conditionResultVar, iterConfig]);

  const handleTest = useCallback(async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: generatedCode, posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        const outputs = [];
        if (data.print_outputs?.length > 0) outputs.push(...data.print_outputs.map(p => String(p)));
        if (data.transactions?.length > 0) outputs.push(`Generated ${data.transactions.length} transaction(s)`);
        setTestResult({ success: true, output: outputs.join('\n') || 'Executed successfully (no output)', transactions: data.transactions || [] });
      } else {
        setTestResult({ success: false, error: data.error || data.detail || 'Execution failed' });
      }
    } catch (err) {
      setTestResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setTesting(false);
    }
  }, [generatedCode]);

  const handleApply = useCallback(() => {
    onGenerate(generatedCode);
  }, [generatedCode, onGenerate]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', bgcolor: 'white' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
          <Calculator size={20} color="#5B5FED" />
          <Typography variant="h5">Rule Builder</Typography>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Build calculation logic using forms — no coding required
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {/* Rule Name */}
        <TextField size="small" fullWidth label="Rule Name" value={ruleName}
          onChange={(e) => setRuleName(e.target.value)} sx={{ mb: 2 }}
          placeholder="e.g., Monthly Interest Accrual" />

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

        {/* Variables / Parameters */}
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <Typography variant="body2" fontWeight={600}>
            {ruleType === 'collect' ? 'Data Collection' : 'Calculation Steps'}
          </Typography>
          <Button size="small" startIcon={<Plus size={14} />} onClick={addVariable}>Add Step</Button>
        </Box>

        {variables.map((variable, idx) => (
          <VariableRow key={idx} variable={variable} index={idx} events={events}
            onUpdate={updateVariable} onRemove={removeVariable}
            onMoveUp={() => moveVariable(idx, -1)} onMoveDown={() => moveVariable(idx, 1)}
            isFirst={idx === 0} isLast={idx === variables.length - 1} />
        ))}

        {/* ── Conditional Logic Section ── */}
        {ruleType === 'conditional' && (
          <>
            <Divider sx={{ my: 2 }} />
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="body2" fontWeight={600}>
                <GitBranch size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
                Conditions (IF / ELSE IF / ELSE)
              </Typography>
              <Button size="small" startIcon={<Plus size={14} />} onClick={addCondition}>Add Condition</Button>
            </Box>
            <TextField size="small" label="Result Variable Name" value={conditionResultVar}
              onChange={(e) => setConditionResultVar(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
              sx={{ mb: 1.5 }} placeholder="result" />

            {conditions.map((cond, idx) => (
              <ConditionRow key={idx} condition={cond} index={idx} events={events}
                onUpdate={updateCondition} onRemove={removeCondition} />
            ))}

            <Card sx={{ mb: 1, borderLeft: '3px solid #9E9E9E' }}>
              <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>ELSE (default)</Typography>
                <FormulaBar
                  value={elseFormula}
                  onChange={setElseFormula}
                  events={events}
                  label="Default value"
                  placeholder="e.g., 0"
                />
              </CardContent>
            </Card>
          </>
        )}

        {/* ── Iteration Section ── */}
        {ruleType === 'iteration' && (
          <>
            <Divider sx={{ my: 2 }} />
            <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
              <Repeat size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
              Iteration Configuration
            </Typography>
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
              label="Expression (applied to each element)"
              placeholder='e.g., multiply(item, 1.1)'
            />
          </>
        )}

        <Divider sx={{ my: 2 }} />

        {/* ── Output Options ── */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>Output Options</Typography>
        <Card sx={{ mb: 1 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 }, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="body2">Print result to console</Typography>
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
                {outputs.transactions.map((txn, idx) => (
                  <Card key={idx} variant="outlined" sx={{ p: 1, mb: 1, bgcolor: '#FAFAFA' }}>
                    <Box sx={{ display: 'flex', gap: 1, mb: 0.5, alignItems: 'center' }}>
                      <TextField size="small" label="Transaction Type" value={txn.type} sx={{ flex: 1 }}
                        onChange={(e) => updateTransaction(idx, 'type', e.target.value)} />
                      <TextField size="small" label="Amount (variable)" value={txn.amount} sx={{ flex: 1 }}
                        onChange={(e) => updateTransaction(idx, 'amount', e.target.value)}
                        placeholder="e.g., interest" />
                      {outputs.transactions.length > 1 && (
                        <IconButton size="small" onClick={() => removeTransaction(idx)} sx={{ color: '#F44336' }}><Trash2 size={12} /></IconButton>
                      )}
                    </Box>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <TextField size="small" label="Posting Date" value={txn.postingDate} sx={{ flex: 1 }}
                        onChange={(e) => updateTransaction(idx, 'postingDate', e.target.value)}
                        placeholder="postingdate" />
                      <TextField size="small" label="Effective Date" value={txn.effectiveDate} sx={{ flex: 1 }}
                        onChange={(e) => updateTransaction(idx, 'effectiveDate', e.target.value)}
                        placeholder="same as posting" />
                    </Box>
                  </Card>
                ))}
                <Button size="small" startIcon={<Plus size={14} />} onClick={addTransaction}>Add Transaction</Button>
              </>
            )}
          </CardContent>
        </Card>

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
      </Box>

      {/* Action Bar */}
      <Box sx={{ p: 2, borderTop: '1px solid #E9ECEF', bgcolor: 'white', display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
        {onClose && <Button onClick={onClose} color="inherit">Cancel</Button>}
        <Button variant="outlined" onClick={handleTest} disabled={testing}
          startIcon={testing ? <CircularProgress size={16} /> : <FlaskConical size={16} />}
          sx={{ borderColor: '#4CAF50', color: '#4CAF50', '&:hover': { borderColor: '#388E3C', bgcolor: '#E8F5E9' } }}>
          {testing ? 'Testing...' : 'Test'}
        </Button>
        <Button variant="contained" onClick={handleApply} startIcon={<Play size={16} />}>
          Load into Editor
        </Button>
      </Box>
    </Box>
  );
};

export default AccountingRuleBuilder;
