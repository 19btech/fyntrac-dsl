import React, { useState, useCallback, useRef } from "react";
import {
  Box, Typography, Button, TextField, Paper, CircularProgress, Alert,
  Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
} from "@mui/material";
import { Code, Play, Save } from "lucide-react";
import Editor from "@monaco-editor/react";
import { API } from "../../config";

/**
 * CustomCodeEditor — Standalone DSL code editor for "Custom Code" rules.
 * Supports write / run / save as a saved-rule with ruleType='custom_code'.
 */
const CustomCodeEditor = ({ events, dslFunctions, onSave, initialData }) => {
  const [ruleName, setRuleName] = useState(initialData?.name || '');
  const [rulePriority, setRulePriority] = useState(initialData?.priority ?? '');
  const [ruleId, setRuleId] = useState(initialData?.id || null);
  const [customCode, setCustomCode] = useState(initialData?.customCode || initialData?.generatedCode || '');
  const [testing, setTesting] = useState(false);
  const [output, setOutput] = useState('');
  const [saving, setSaving] = useState(false);
  const [saveResult, setSaveResult] = useState(null);
  const [validationMsg, setValidationMsg] = useState('');
  const completionDisposerRef = useRef(null);
  const EDITOR_HEIGHT = 400;

  const handleRun = useCallback(async () => {
    if (!customCode.trim()) return;
    setTesting(true);
    setOutput('');
    try {
      const today = new Date().toISOString().split('T')[0];
      const response = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dsl_code: customCode, posting_date: today }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setOutput((data.print_outputs || []).map(String).join('\n') || 'Executed successfully (no output)');
      } else {
        setOutput('ERROR: ' + (data.error || data.detail || 'Execution failed'));
      }
    } catch (e) {
      setOutput('ERROR: ' + (e.message || 'Network error'));
    } finally {
      setTesting(false);
    }
  }, [customCode]);

  const handleSave = useCallback(async () => {
    if (!ruleName.trim()) {
      setValidationMsg('Rule Name is required.');
      return;
    }
    if (rulePriority === '' || rulePriority === null || rulePriority === undefined) {
      setValidationMsg('Priority is required.');
      return;
    }
    if (!customCode.trim()) {
      setValidationMsg('Custom code is required. Please write some DSL code.');
      return;
    }
    setSaving(true);
    setSaveResult(null);
    try {
      const payload = {
        id: ruleId,
        name: ruleName.trim(),
        priority: Number(rulePriority),
        ruleType: 'custom_code',
        variables: [],
        conditions: [],
        elseFormula: '',
        conditionResultVar: 'result',
        iterations: [],
        iterConfig: {},
        outputs: { printResult: false, createTransaction: false, transactions: [] },
        inlineComment: false,
        commentText: '',
        customCode,
        generatedCode: customCode,
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
      } else {
        setSaveResult({ success: false, error: typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data.error || 'Save failed') });
      }
    } catch (err) {
      setSaveResult({ success: false, error: err.message || 'Network error' });
    } finally {
      setSaving(false);
    }
  }, [ruleName, rulePriority, ruleId, customCode, onSave]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%' }}>
      {/* Header */}
      <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', bgcolor: 'white', flexShrink: 0 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1 }}>
          <Code size={20} color="#9C27B0" />
          <Typography variant="h5">Custom Code</Typography>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Write raw DSL code directly — full control over the logic
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {/* Name & Priority */}
        <Box sx={{ display: 'flex', gap: 1.5, mb: 2 }}>
          <TextField size="small" label="Rule Name *" value={ruleName}
            onChange={(e) => setRuleName(e.target.value)}
            placeholder="e.g., Custom Interest Logic"
            sx={{ flex: 1 }} />
          <TextField size="small" label="Priority *" value={rulePriority}
            onChange={(e) => { const v = e.target.value; if (v === '' || /^\d+$/.test(v)) setRulePriority(v === '' ? '' : Number(v)); }}
            placeholder="e.g., 1" type="number" inputProps={{ min: 0, step: 1 }}
            sx={{ width: 140 }} />
        </Box>

        {/* Code Editor */}
        <Typography variant="body2" fontWeight={600} sx={{ mb: 1 }}>
          <Code size={14} style={{ display: 'inline', marginRight: 6, verticalAlign: 'middle' }} />
          DSL Code
        </Typography>
        <Paper variant="outlined" sx={{ bgcolor: '#0D1117', borderRadius: 1.5, overflow: 'hidden' }}>
          <Editor
            height={EDITOR_HEIGHT}
            language="python"
            theme="vs-dark"
            value={customCode}
            onChange={(val) => setCustomCode(val || '')}
            options={{
              minimap: { enabled: false },
              fontSize: 13,
              lineNumbers: 'on',
              scrollBeyondLastLine: false,
              wordWrap: 'on',
              tabSize: 4,
              automaticLayout: true,
              suggestOnTriggerCharacters: true,
              quickSuggestions: true,
              padding: { top: 8 },
              renderLineHighlight: 'line',
              cursorBlinking: 'blink',
              fixedOverflowWidgets: true,
            }}
            beforeMount={(monaco) => {
              if (completionDisposerRef.current) {
                completionDisposerRef.current.dispose();
                completionDisposerRef.current = null;
              }
              completionDisposerRef.current = monaco.languages.registerCompletionItemProvider('python', {
                provideCompletionItems: (model) => {
                  const suggestions = [];
                  const seen = new Set();

                  (dslFunctions || []).forEach(func => {
                    if (seen.has(func.name)) return;
                    seen.add(func.name);
                    suggestions.push({
                      label: func.name,
                      kind: monaco.languages.CompletionItemKind.Function,
                      insertText: `${func.name}()`,
                      detail: func.params || '',
                      documentation: func.description || '',
                    });
                  });

                  const helpers = [
                    { name: 'lag', params: "col, offset, default", desc: 'Get previous row value in schedule' },
                    { name: 'schedule', params: 'period_def, columns, context?', desc: 'Generate a schedule' },
                    { name: 'schedule_sum', params: 'sched, col', desc: 'Sum a schedule column' },
                    { name: 'schedule_first', params: 'sched, col', desc: 'First value of schedule column' },
                    { name: 'schedule_last', params: 'sched, col', desc: 'Last value of schedule column' },
                    { name: 'schedule_filter', params: 'sched, date_col, target_date, value_col', desc: 'Filter schedule rows' },
                    { name: 'period', params: 'start, end, freq, convention?', desc: 'Create a period definition' },
                    { name: 'print', params: 'value', desc: 'Print value to console' },
                    { name: 'collect', params: 'EVENT.field', desc: 'Collect values for current instrument/postingdate' },
                    { name: 'collect_by_instrument', params: 'EVENT.field', desc: 'Collect values grouped by instrument' },
                    { name: 'collect_all', params: 'EVENT.field', desc: 'Collect all values' },
                    { name: 'collect_subinstrumentids', params: '', desc: 'Collect sub-instrument IDs' },
                    { name: 'for_each', params: 'dates_arr, amounts_arr, date_var, amount_var, expression', desc: 'Iterate paired arrays' },
                    { name: 'map_array', params: 'array, var_name, expression, context?', desc: 'Transform array elements' },
                    { name: 'sum_vals', params: 'array', desc: 'Sum numeric values in array' },
                    { name: 'createTransaction', params: 'posting_date, effective_date, type, amount, subinstrumentid?', desc: 'Create transaction' },
                  ];
                  helpers.forEach(h => {
                    if (seen.has(h.name)) return;
                    seen.add(h.name);
                    suggestions.push({
                      label: h.name,
                      kind: monaco.languages.CompletionItemKind.Function,
                      insertText: `${h.name}()`,
                      detail: h.params,
                      documentation: h.desc,
                    });
                  });

                  (events || []).forEach(event => {
                    ['postingdate', 'effectivedate', 'subinstrumentid'].forEach(sf => {
                      const full = `${event.event_name}.${sf}`;
                      if (!seen.has(full)) {
                        seen.add(full);
                        suggestions.push({ label: full, kind: monaco.languages.CompletionItemKind.Field, insertText: full, detail: '(date)', documentation: `Field from ${event.event_name}` });
                      }
                    });
                    (event.fields || []).forEach(field => {
                      const full = `${event.event_name}.${field.name}`;
                      if (!seen.has(full)) {
                        seen.add(full);
                        suggestions.push({ label: full, kind: monaco.languages.CompletionItemKind.Field, insertText: full, detail: `(${field.datatype})`, documentation: `Event field from ${event.event_name}` });
                      }
                    });
                  });

                  try {
                    const code = model.getValue();
                    const assignRegex = /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=.*$/gm;
                    let m;
                    while ((m = assignRegex.exec(code)) !== null) {
                      if (!seen.has(m[1])) {
                        seen.add(m[1]);
                        suggestions.push({ label: m[1], kind: monaco.languages.CompletionItemKind.Variable, insertText: m[1], detail: 'Local variable' });
                      }
                    }
                  } catch (_) { /* ignore */ }

                  return { suggestions };
                },
              });
            }}
          />
        </Paper>

        {/* Console */}
        <Box sx={{ mt: 1.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
            <Typography variant="caption" fontWeight={600} color="text.secondary">Console</Typography>
            <Box sx={{ display: 'flex', gap: 0.5 }}>
              <Button size="small" variant="outlined"
                onClick={() => setOutput('')} disabled={!output}
                sx={{ fontSize: '0.7rem', minHeight: 24, px: 1, py: 0, color: '#8B949E', borderColor: '#30363D', '&:hover': { borderColor: '#8B949E' } }}>
                Clear
              </Button>
              <Button size="small" variant="contained"
                startIcon={testing ? <CircularProgress size={12} color="inherit" /> : <Play size={12} />}
                disabled={testing || !customCode.trim()}
                onClick={handleRun}
                sx={{ fontSize: '0.7rem', minHeight: 24, px: 1.5, py: 0, bgcolor: '#4CAF50', '&:hover': { bgcolor: '#388E3C' } }}>
                Run
              </Button>
            </Box>
          </Box>
          <Paper variant="outlined" sx={{ bgcolor: '#161B22', borderRadius: 1, height: 180, overflow: 'auto', p: 1.5 }}>
            <Typography component="pre" variant="body2" sx={{
              fontFamily: 'monospace', fontSize: '0.75rem', lineHeight: 1.5, whiteSpace: 'pre-wrap',
              color: output.startsWith('ERROR:') ? '#F85149' : '#7EE787', m: 0,
            }}>
              {output || <span style={{ color: '#484F58' }}>Click Run to test your code...</span>}
            </Typography>
          </Paper>
        </Box>

        {/* Save Result */}
        {saveResult && (
          <Alert severity={saveResult.success ? 'success' : 'error'} sx={{ mt: 2 }}
            onClose={() => setSaveResult(null)}>
            {saveResult.success ? saveResult.output : saveResult.error}
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
        <Button variant="outlined" onClick={handleSave} disabled={saving}
          startIcon={saving ? <CircularProgress size={16} /> : <Save size={16} />}
          sx={{ borderColor: '#1976D2', color: '#1976D2', '&:hover': { borderColor: '#1565C0', bgcolor: '#E3F2FD' } }}>
          {saving ? 'Saving...' : 'Save Rule'}
        </Button>
      </Box>
    </Box>
  );
};

export default CustomCodeEditor;
