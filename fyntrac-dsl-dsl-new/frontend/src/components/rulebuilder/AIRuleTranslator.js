import React, { useState, useCallback } from "react";
import {
  Box, Typography, TextField, Button, Card, CardContent, Chip, Alert,
  CircularProgress, Paper, Switch, FormControlLabel, Divider,
} from "@mui/material";
import { Sparkles, Play, Wand2, ArrowRight, RefreshCw, Code, Eye, EyeOff } from "lucide-react";
import { API } from "../../config";

const SUGGESTION_PROMPTS = [
  "Calculate monthly loan payment for a $500K mortgage at 4.5% over 30 years",
  "Depreciate an asset worth $120,000 using straight-line over 10 years with $5,000 salvage",
  "Generate an amortization schedule for a $200K loan at 6% annual interest, monthly for 5 years",
  "Recognize revenue for a $50,000 contract over 12 months starting January 2026",
  "Accrue interest on a $1M deposit at 3.75% annual rate, quarterly periods",
  "Calculate net present value of cash flows [$10K, $20K, $30K, $40K] at 8% discount rate",
  "Build a lease amortization schedule with right-of-use asset and lease liability under ASC 842",
  "Collect all amounts from events and sum by instrument, then create transactions for each",
  "Generate depreciation schedule using double declining balance for a $80K asset over 5 years",
  "Calculate fee amortization under FAS 91 for a $15K origination fee over 60 months",
];

/**
 * AIRuleTranslator — Takes natural language descriptions and generates
 * DSL code through the AI backend, with a preview step before inserting.
 */
const AIRuleTranslator = ({ events, dslFunctions, onGenerate, selectedModel }) => {
  const [description, setDescription] = useState('');
  const [loading, setLoading] = useState(false);
  const [generatedCode, setGeneratedCode] = useState(null);
  const [error, setError] = useState(null);
  const [showCode, setShowCode] = useState(true);
  const [sessionId, setSessionId] = useState(null);

  const handleGenerate = useCallback(async () => {
    if (!description.trim()) return;
    setLoading(true);
    setError(null);
    setGeneratedCode(null);

    try {
      // Build comprehensive context for the AI
      const eventContext = events?.map(e => ({
        name: e.event_name,
        fields: e.fields?.map(f => `${f.name} (${f.datatype})`) || [],
      })) || [];

      // Include ALL available DSL functions, grouped by category
      const functionList = dslFunctions?.map(f => {
        const params = f.params || '';
        return `${f.name}(${params})`;
      }) || [];

      const prompt = `You are a DSL code generator for the Fyntrac accounting calculation platform.

ALL AVAILABLE DSL FUNCTIONS:
${functionList.join('\n')}

AVAILABLE EVENT DATA:
${eventContext.length > 0 ? eventContext.map(e => `Event "${e.name}" with fields: ${e.fields.join(', ')}\n  System fields: ${e.name}.postingdate, ${e.name}.effectivedate, ${e.name}.subinstrumentid`).join('\n') : 'No events loaded — use fixed values for standalone calculations.'}

KEY DSL PATTERNS — Use these as references:

1. SCHEDULE PATTERN (amortization, depreciation, revenue recognition):
   p = period("2026-01-01", "2030-12-31", "M")
   sched = schedule(p, {
       "date": "period_date",
       "opening_bal": "lag('closing_bal', 1, principal)",
       "interest": "divide(multiply(opening_bal, annual_rate), 12)",
       "payment_amount": "pmt(annual_rate, num_periods, principal)",
       "principal_pmt": "subtract(payment_amount, interest)",
       "closing_bal": "subtract(opening_bal, principal_pmt)"
   }, {"principal": principal, "annual_rate": annual_rate, "num_periods": num_periods})
   print(sched)

2. PERIOD CONVENTIONS: period(start, end, freq, convention)
   Frequencies: "M" monthly, "Q" quarterly, "S" semi-annual, "A" annual
   Conventions: "ACT/360", "ACT/365", "30/360", "ACT/ACT"

3. SCHEDULE EXTRACTION:
   schedule_sum(sched, "column") — total of a column
   schedule_first(sched, "column") — first period value
   schedule_last(sched, "column") — last period value
   schedule_filter(sched, "condition") — filter rows

4. TRANSACTION PATTERN:
   createTransaction(postingdate, effectivedate, transactiontype, amount)

5. COLLECT PATTERN (read event data scoped to current instrument):
   amounts = collect_by_instrument(EVENT.amount)
   dates = collect_by_instrument(EVENT.postingdate)

6. ITERATION PATTERN:
   doubled = apply_each(arr, "multiply(each, 2)")
   total = sum_vals(doubled)
   for_each(dates, amounts, "d", "a", "createTransaction(d, d, \\"Type\\", a)")

7. GENERATE_SCHEDULES (multi-instrument from event data):
   generate_schedules(EVENT, period_def, columns, context)

8. CONDITIONAL:
   result = if(condition, true_value, false_value)

IMPORTANT RULES:
- Use ONLY the DSL functions listed above — do NOT use Python standard library
- Use lag('column', offset, default) inside schedule columns to reference previous rows
- Use print() to display results
- Add comments with ## for section headers
- Variable names should be descriptive (snake_case)
- Always print the schedule and summary totals
- For event field references, use EVENT_NAME.field_name syntax

Generate clean, working DSL code for this request:
"${description}"

Return ONLY the DSL code, no explanations or markdown fences.`;

      const response = await fetch(`${API}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: prompt,
          model: selectedModel || undefined,
          ...(sessionId ? { session_id: sessionId } : {}),
        }),
      });

      if (!response.ok) {
        const errData = await response.json().catch(() => null);
        throw new Error(errData?.detail || errData?.error_message || `API error: ${response.status}`);
      }

      const data = await response.json();
      
      // Check for provider-level errors
      if (data.error_type || data.error_message) {
        throw new Error(data.error_message || 'AI provider error — check AI Agent Setup in Settings.');
      }

      let code = data.response || data.message || '';
      if (data.session_id) setSessionId(data.session_id);

      // Strip markdown fences if present
      code = code.replace(/^```(?:python|dsl)?\n?/gm, '').replace(/```\s*$/gm, '').trim();
      setGeneratedCode(code);
    } catch (err) {
      setError(err.message || 'Failed to generate code. Check AI provider configuration.');
    } finally {
      setLoading(false);
    }
  }, [description, events, dslFunctions, selectedModel, sessionId]);

  const handleApply = useCallback(() => {
    if (generatedCode) {
      onGenerate(generatedCode);
    }
  }, [generatedCode, onGenerate]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Header */}
      <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', bgcolor: 'white' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 0.5 }}>
          <Sparkles size={20} color="#5B5FED" />
          <Typography variant="h5">AI Rule Generator</Typography>
        </Box>
        <Typography variant="body2" color="text.secondary">
          Describe what you want to calculate in plain English
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {/* Description input */}
        <TextField
          fullWidth multiline rows={3}
          label="Describe your calculation"
          placeholder="e.g., Generate a loan amortization schedule for a $500,000 mortgage at 4.5% annual rate over 30 years with monthly payments"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          sx={{ mb: 2 }}
        />

        {/* Quick suggestions */}
        <Typography variant="caption" fontWeight={600} color="text.secondary" gutterBottom display="block">
          Try one of these:
        </Typography>
        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mb: 2 }}>
          {SUGGESTION_PROMPTS.map((prompt, idx) => (
            <Chip key={idx} label={prompt} size="small" variant="outlined"
              onClick={() => setDescription(prompt)}
              sx={{ fontSize: '0.6875rem', cursor: 'pointer', maxWidth: '100%',
                '&:hover': { bgcolor: '#EEF0FE', borderColor: '#5B5FED' },
              }} />
          ))}
        </Box>

        <Button
          variant="contained" fullWidth onClick={handleGenerate}
          disabled={!description.trim() || loading}
          startIcon={loading ? <CircularProgress size={16} color="inherit" /> : <Wand2 size={16} />}
          sx={{ mb: 2 }}
        >
          {loading ? 'Generating...' : 'Generate Calculation Logic'}
        </Button>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Generated code preview */}
        {generatedCode && (
          <Card sx={{ border: '1px solid #5B5FED' }}>
            <CardContent sx={{ p: 0 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', p: 1.5, pb: 0 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Code size={16} color="#5B5FED" />
                  <Typography variant="body2" fontWeight={600}>Generated Logic</Typography>
                </Box>
                <Box sx={{ display: 'flex', gap: 0.5 }}>
                  <Button size="small" startIcon={<RefreshCw size={14} />} onClick={handleGenerate} color="inherit">
                    Regenerate
                  </Button>
                  <FormControlLabel
                    control={<Switch checked={showCode} onChange={(e) => setShowCode(e.target.checked)} size="small" />}
                    label={<Typography variant="caption">{showCode ? 'Hide Code' : 'Show Code'}</Typography>}
                    sx={{ mr: 0 }}
                  />
                </Box>
              </Box>

              {showCode && (
                <Paper variant="outlined" sx={{ m: 1.5, mt: 1, p: 2, bgcolor: '#0D1117', borderRadius: 2, maxHeight: 350, overflow: 'auto' }}>
                  <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.8125rem', color: '#E6EDF3', whiteSpace: 'pre-wrap' }}>
                    {generatedCode}
                  </pre>
                </Paper>
              )}

              <Divider />
              <Box sx={{ p: 1.5, display: 'flex', gap: 1 }}>
                <Button variant="contained" onClick={handleApply} startIcon={<Play size={16} />} fullWidth>
                  Load into Editor
                </Button>
              </Box>
            </CardContent>
          </Card>
        )}
      </Box>
    </Box>
  );
};

export default AIRuleTranslator;
