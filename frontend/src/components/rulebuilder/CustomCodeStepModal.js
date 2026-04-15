import React, { useState, useCallback, useRef, useEffect } from "react";
import {
  Box, Typography, Button, TextField, Paper, CircularProgress, Alert, IconButton,
  Dialog, DialogTitle, DialogContent, DialogActions,
} from "@mui/material";
import { Code, Play, Save, X } from "lucide-react";
import Editor from "@monaco-editor/react";
import { API } from "../../config";

/**
 * CustomCodeStepModal — Full-screen modal for writing raw DSL code
 * as a step inside the Rule Builder. No rule name/priority/save — those
 * belong to the parent rule.
 */
const CustomCodeStepModal = ({ open, step, onClose, onSaveStep, events, dslFunctions }) => {
  const [stepName, setStepName] = useState(step?.name || '');
  const [customCode, setCustomCode] = useState(step?.customCode || '');
  const [testing, setTesting] = useState(false);
  const [output, setOutput] = useState('');
  const completionDisposerRef = useRef(null);
  const EDITOR_HEIGHT = 400;

  // Reset when step changes
  useEffect(() => {
    if (!open) return;
    setStepName(step?.name || '');
    setCustomCode(step?.customCode || '');
    setOutput('');
  }, [open, step]);

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

  const handleSave = () => {
    if (!stepName) return;
    onSaveStep({
      name: stepName,
      stepType: 'custom_code',
      customCode,
    });
    onClose();
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth
      PaperProps={{ sx: { maxHeight: '90vh', height: '90vh' } }}>
      <DialogTitle sx={{ pb: 1, borderBottom: '1px solid #E9ECEF' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1 }}>
          <Code size={20} color="#607D8B" />
          <Typography variant="h6" sx={{ flex: 1 }}>{step?.name ? `Edit Custom Code Step: ${step.name}` : 'Add Custom Code Step'}</Typography>
          <IconButton size="small" onClick={onClose} sx={{ color: '#6C757D' }}><X size={18} /></IconButton>
        </Box>
      </DialogTitle>
      <DialogContent sx={{ pt: 2, overflow: 'auto' }}>
        {/* Step Name */}
        <TextField size="small" fullWidth label="Variable Name *" value={stepName}
          onChange={(e) => setStepName(e.target.value.replace(/[^a-zA-Z0-9_]/g, ''))}
          placeholder="e.g., custom_logic" sx={{ mb: 2, mt: 1 }} />

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
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={onClose} color="inherit">Cancel</Button>
        <Button onClick={handleSave} disabled={!stepName} variant="contained" startIcon={<Save size={14} />}>
          Save Step
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default CustomCodeStepModal;
