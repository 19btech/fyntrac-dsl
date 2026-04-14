import React, { useState, useMemo, useCallback } from "react";
import {
  Box, Typography, Card, CardContent, Button, TextField, MenuItem, Chip, Stepper, Step,
  StepLabel, FormControlLabel, Checkbox, Switch, Alert, IconButton, Tooltip, Divider,
  Dialog, DialogTitle, DialogContent, DialogActions, Paper, InputAdornment, Select, FormControl,
  InputLabel,
} from "@mui/material";
import {
  BookOpen, Search, ArrowRight, ArrowLeft, Play, Code, Eye, CheckCircle2,
  TrendingUp, TrendingDown, DollarSign, Percent, Receipt, Calculator, Building,
  Sparkles, Copy, Settings2, X,
} from "lucide-react";
import ACCOUNTING_TEMPLATES from "./AccountingTemplates";

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
    onGenerate(generatedCode || template.generateDSL(config));
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
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Set the parameters for your calculation. You can enter values directly or reference fields from your uploaded event data.
              </Typography>
              {template.fields.map((field) => (
                <FieldInput
                  key={field.key} field={field} events={events}
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

/**
 * TemplateLibrary — Browse and configure accounting template wizards.
 */
const TemplateLibrary = ({ events, onLoadTemplate, onClose }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [activeTemplate, setActiveTemplate] = useState(null);

  const categories = useMemo(() => {
    return ['All', ...new Set(ACCOUNTING_TEMPLATES.map(t => t.category))];
  }, []);

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

  const handleGenerate = useCallback((code) => {
    onLoadTemplate(code);
    setActiveTemplate(null);
    onClose();
  }, [onLoadTemplate, onClose]);

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
              Pre-built calculation templates — configure and generate without writing code
            </Typography>
          </Box>
          <IconButton onClick={onClose} sx={{ alignSelf: 'flex-start' }}>
            <X size={20} />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ display: 'flex', flexDirection: 'column', p: 3 }}>
        <Box sx={{ mb: 2 }}>
          <TextField
            placeholder="Search templates by name, description, or standard..."
            value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)}
            fullWidth size="small"
            InputProps={{ startAdornment: <InputAdornment position="start"><Search size={16} color="#6C757D" /></InputAdornment> }}
            sx={{ mb: 1.5 }}
          />
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75 }}>
            {categories.map((cat) => (
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
          </Box>
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default TemplateLibrary;
