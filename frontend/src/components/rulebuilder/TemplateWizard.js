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
  Download,
} from "lucide-react";
import ACCOUNTING_TEMPLATES from "./AccountingTemplates";
import { API } from "../../config";

/**
 * Parse generated DSL code into Rule Builder-compatible variables and outputs.
 * Converts assignment lines → variables, print → outputs, createTransaction → outputs.transactions.
 */
function parseDSLToRuleVariables(code) {
  const lines = code.split('\n');
  const variables = [];
  const transactions = [];
  let hasCreateTxn = false;
  let i = 0;

  while (i < lines.length) {
    const line = lines[i].trim();

    // Skip empty lines, comments, print statements
    if (!line || line.startsWith('##') || line.startsWith('print(') || line.startsWith('print (')) {
      i++;
      continue;
    }

    // Handle createTransaction — parse for outputs
    if (line.startsWith('createTransaction(')) {
      hasCreateTxn = true;
      const inner = line.slice('createTransaction('.length, -1);
      const args = [];
      let depth = 0, current = '';
      for (const ch of inner) {
        if (ch === '(' || ch === '[' || ch === '{') depth++;
        if (ch === ')' || ch === ']' || ch === '}') depth--;
        if (ch === ',' && depth === 0) { args.push(current.trim()); current = ''; }
        else { current += ch; }
      }
      if (current.trim()) args.push(current.trim());
      transactions.push({
        type: (args[2] || '').replace(/^"|"$/g, ''),
        amount: args[3] || '',
        postingDate: args[0] || '',
        effectiveDate: args[1] || '',
        subInstrumentId: args[4] || '',
      });
      i++;
      continue;
    }

    // Handle assignments (possibly multi-line)
    const assignMatch = line.match(/^([a-zA-Z_]\w*)\s*=\s*(.*)/);
    if (assignMatch) {
      const name = assignMatch[1];
      let rhs = assignMatch[2];

      // Track bracket/paren depth for multi-line
      let depth = 0;
      for (const ch of rhs) { if ('({['.includes(ch)) depth++; if (')}]'.includes(ch)) depth--; }
      while (depth > 0 && i + 1 < lines.length) {
        i++;
        rhs += '\n' + lines[i];
        for (const ch of lines[i]) { if ('({['.includes(ch)) depth++; if (')}]'.includes(ch)) depth--; }
      }
      rhs = rhs.trim();

      // Detect collect functions
      const collectMatch = rhs.match(/^(collect_by_instrument|collect_all|collect_by_subinstrument|collect_subinstrumentids|collect)\(([^)]*)\)$/);
      if (collectMatch) {
        variables.push({ name, source: 'collect', collectType: collectMatch[1], eventField: collectMatch[2] || '', value: '', formula: '' });
        i++; continue;
      }

      // Detect plain number
      if (/^-?\d+(\.\d+)?$/.test(rhs)) {
        variables.push({ name, source: 'value', value: rhs, formula: '', eventField: '', collectType: 'collect' });
        i++; continue;
      }

      // Detect quoted string
      if (/^"[^"]*"$/.test(rhs)) {
        variables.push({ name, source: 'value', value: rhs, formula: '', eventField: '', collectType: 'collect' });
        i++; continue;
      }

      // Detect event field reference (EventName.field_name)
      if (/^[A-Z][a-zA-Z0-9]*\.[a-zA-Z_]\w*$/.test(rhs)) {
        variables.push({ name, source: 'event_field', eventField: rhs, value: '', formula: '', collectType: 'collect' });
        i++; continue;
      }

      // Everything else is a formula
      variables.push({ name, source: 'formula', formula: rhs, value: '', eventField: '', collectType: 'collect' });
      i++; continue;
    }

    i++;
  }

  const outputs = {
    printResult: true,
    createTransaction: hasCreateTxn,
    transactions: transactions.length > 0 ? transactions
      : [{ type: 'Calculation Result', amount: '', postingDate: '', effectiveDate: '', subInstrumentId: '' }],
  };

  return { variables, outputs };
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
    const parsed = parseDSLToRuleVariables(code);
    onGenerate(code, { rules: [{
      name: template.title,
      ruleType: 'simple_calc',
      variables: parsed.variables,
      outputs: parsed.outputs,
      generatedCode: code,
    }] });
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
    onApply(combinedCode, { rules: filteredRules });
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

/**
/**
 * TemplateLibrary — Browse standard and user-created accounting templates.
 */
const TemplateLibrary = ({ events, onLoadTemplate, onClose }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [activeTemplate, setActiveTemplate] = useState(null);
  const [activeUserTemplate, setActiveUserTemplate] = useState(null);
  const [userTemplates, setUserTemplates] = useState([]);
  const [loadingUser, setLoadingUser] = useState(true);
  const [deletingId, setDeletingId] = useState(null);
  const [section, setSection] = useState('standard'); // 'standard' | 'user'

  // Fetch user templates
  useEffect(() => {
    (async () => {
      setLoadingUser(true);
      try {
        const res = await fetch(`${API}/user-templates`);
        const data = await res.json();
        setUserTemplates(Array.isArray(data) ? data : []);
      } catch { /* ignore */ }
      finally { setLoadingUser(false); }
    })();
  }, []);

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

  return (
    <Dialog open={true} onClose={onClose} maxWidth="lg" fullWidth PaperProps={{ sx: { height: '85vh' } }}>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <BookOpen size={24} color="#5B5FED" />
          <Box sx={{ flex: 1 }}>
            <Typography variant="h4">Accounting Templates</Typography>
            <Typography variant="body2" color="text.secondary">
              Pre-built and user-created calculation templates
            </Typography>
          </Box>
          <IconButton onClick={onClose} sx={{ alignSelf: 'flex-start' }}>
            <X size={20} />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ display: 'flex', flexDirection: 'column', p: 3 }}>
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
                        <Box sx={{ flex: 1 }}>
                          <Typography variant="h6" sx={{ mb: 0.25 }}>{template.title}</Typography>
                          <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.4 }}>
                            {template.description}
                          </Typography>
                        </Box>
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
      </DialogContent>
    </Dialog>
  );
};

export default TemplateLibrary;
