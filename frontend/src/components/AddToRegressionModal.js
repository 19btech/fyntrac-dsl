import React, { useState, useEffect, useCallback } from "react";
import {
  Box, Typography, Button, TextField, MenuItem, Chip, Alert, Slide,
  Dialog, DialogTitle, DialogContent, DialogActions, CircularProgress,
  LinearProgress, Divider,
} from "@mui/material";
import { ShieldCheck, AlertTriangle } from "lucide-react";
import { API } from "../config";
import ModalHeader from "./ModalHeader";

/**
 * Capture the currently-loaded dataset plus a template as a regression case.
 *
 * Capture *runs* the book — a baseline only means something alongside the
 * transactions it produced — so this dialog is a short-lived progress surface
 * as well as a form. If any posting date fails during that run the backend
 * refuses the capture and we surface the choice explicitly, because a baseline
 * built on a failed run silently turns that failure into the expected result.
 */
const AddToRegressionModal = ({ open, onClose, onCaptured }) => {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [templateId, setTemplateId] = useState('');
  const [tolerance, setTolerance] = useState('0.01');
  const [templates, setTemplates] = useState([]);
  const [summary, setSummary] = useState(null);
  const [capturing, setCapturing] = useState(false);
  const [result, setResult] = useState(null);
  const [errorState, setErrorState] = useState(null);

  const today = new Date().toISOString().slice(0, 10);

  // Load the template list and a preview of what would be captured, so the
  // user can see the size of the snapshot before committing to it.
  useEffect(() => {
    if (!open) return;
    setResult(null);
    setErrorState(null);

    (async () => {
      try {
        const res = await fetch(`${API}/user-templates`);
        const data = await res.json();
        const list = Array.isArray(data) ? data : [];
        setTemplates(list);
        // Default to whichever template the Rule Manager currently has loaded.
        let loaded = null;
        try { loaded = localStorage.getItem('savedRulesTemplateId'); } catch { /* ignore */ }
        const match = list.find(t => t.id === loaded);
        setTemplateId(match ? match.id : '');
        setName(prev => prev || `${match ? match.name : 'Workspace rules'} — ${today}`);
      } catch {
        setTemplates([]);
      }

      try {
        const [eventsRes, datesRes] = await Promise.all([
          fetch(`${API}/event-data`),
          fetch(`${API}/event-data/posting-dates`),
        ]);
        const events = await eventsRes.json();
        const dates = await datesRes.json();
        const rows = (Array.isArray(events) ? events : [])
          .reduce((sum, e) => sum + (e.row_count || 0), 0);
        setSummary({
          eventCount: Array.isArray(events) ? events.length : 0,
          rowCount: rows,
          dateCount: (dates?.posting_dates || []).length,
        });
      } catch {
        setSummary(null);
      }
    })();
  }, [open, today]);

  const capture = useCallback(async (allowErrors) => {
    setCapturing(true);
    setErrorState(null);
    setResult(null);
    try {
      const res = await fetch(`${API}/regression/cases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name.trim(),
          description: description.trim(),
          template_id: templateId || null,
          amount_tolerance: parseFloat(tolerance) || 0.01,
          allow_errors: !!allowErrors,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        // A 409 carrying can_force is the "some dates failed" case: offer the
        // override rather than making the user guess what went wrong.
        const detail = data?.detail;
        if (res.status === 409 && detail && typeof detail === 'object' && detail.can_force) {
          setErrorState({ forceable: true, message: detail.message, errors: detail.errors || [] });
        } else {
          setErrorState({
            forceable: false,
            message: typeof detail === 'string' ? detail : (detail?.message || 'Capture failed.'),
            errors: [],
          });
        }
        return;
      }
      setResult(data);
      if (onCaptured) onCaptured(data);
    } catch (err) {
      setErrorState({ forceable: false, message: err.message || 'Network error', errors: [] });
    } finally {
      setCapturing(false);
    }
  }, [name, description, templateId, tolerance, onCaptured]);

  const close = () => {
    if (capturing) return;
    setName(''); setDescription(''); setResult(null); setErrorState(null);
    onClose();
  };

  return (
    <Dialog open={open} onClose={close} maxWidth="sm" fullWidth
      TransitionComponent={Slide} TransitionProps={{ direction: 'up' }}
      PaperProps={{ sx: { borderRadius: 4, boxShadow: '0 32px 64px rgba(0,0,0,0.14)', overflow: 'hidden', border: '1px solid', borderColor: 'divider' } }}>
      <DialogTitle sx={{ p: 0 }}>
        <ModalHeader badge="REGRESSION" title="Add to Regression" onClose={close} />
      </DialogTitle>
      <DialogContent>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Freezes the dataset loaded right now together with the selected rules, runs
          every posting date for every instrument, and saves the resulting transactions
          as the expected result.
        </Typography>

        {summary && (
          <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 2 }}>
            <Chip size="small" label={`${summary.eventCount} event${summary.eventCount !== 1 ? 's' : ''}`} />
            <Chip size="small" label={`${summary.rowCount.toLocaleString()} rows`} />
            <Chip size="small" label={`${summary.dateCount} posting date${summary.dateCount !== 1 ? 's' : ''}`} />
          </Box>
        )}

        <TextField
          autoFocus fullWidth size="small" label="Case name"
          value={name} onChange={(e) => setName(e.target.value)}
          disabled={capturing || !!result} sx={{ mb: 2 }}
        />
        <TextField
          fullWidth size="small" label="Description" multiline rows={2}
          placeholder="What is this case protecting?"
          value={description} onChange={(e) => setDescription(e.target.value)}
          disabled={capturing || !!result} sx={{ mb: 2 }}
        />
        <TextField
          select fullWidth size="small" label="Rules to capture"
          value={templateId} onChange={(e) => setTemplateId(e.target.value)}
          disabled={capturing || !!result} sx={{ mb: 2 }}
          helperText="The case remembers this choice as its default, but any template can be run against it later."
        >
          <MenuItem value="">Current workspace rules</MenuItem>
          {templates.map(t => <MenuItem key={t.id} value={t.id}>{t.name}</MenuItem>)}
        </TextField>
        <TextField
          fullWidth size="small" label="Amount tolerance" type="number"
          inputProps={{ step: '0.01', min: '0' }}
          value={tolerance} onChange={(e) => setTolerance(e.target.value)}
          disabled={capturing || !!result}
          helperText="Amounts within this much of the baseline count as unchanged."
        />

        {capturing && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Running every posting date…
            </Typography>
            <LinearProgress sx={{ mt: 0.5, borderRadius: 1 }} />
          </Box>
        )}

        {errorState && (
          <Alert
            severity={errorState.forceable ? 'warning' : 'error'}
            icon={errorState.forceable ? <AlertTriangle size={18} /> : undefined}
            sx={{ mt: 2 }}
          >
            <Typography variant="body2" sx={{ fontWeight: 500 }}>{errorState.message}</Typography>
            {errorState.errors.length > 0 && (
              <Box component="ul" sx={{ pl: 2, mt: 0.5, mb: 0, fontSize: '0.75rem' }}>
                {errorState.errors.slice(0, 5).map((e, i) => (
                  <li key={i}>{e.posting_date || 'run'}: {String(e.error).slice(0, 160)}</li>
                ))}
              </Box>
            )}
            {errorState.forceable && (
              <Button size="small" color="warning" variant="outlined" sx={{ mt: 1 }}
                onClick={() => capture(true)} disabled={capturing}>
                Capture anyway
              </Button>
            )}
          </Alert>
        )}

        {result && (
          <Alert severity="success" sx={{ mt: 2 }}>
            <Typography variant="body2" sx={{ fontWeight: 500 }}>{result.message}</Typography>
            {result.timing?.total_ms != null && (
              <Typography variant="caption" sx={{ display: 'block', mt: 0.5 }}>
                Took {(result.timing.total_ms / 1000).toFixed(1)}s
                {result.timing.slowest_date_ms != null && result.timing.slowest_date && (
                  <> — slowest posting date {result.timing.slowest_date} at{' '}
                  {(result.timing.slowest_date_ms / 1000).toFixed(1)}s</>
                )}
              </Typography>
            )}
            {result.nondeterminism_warnings?.length > 0 && (
              <>
                <Divider sx={{ my: 1 }} />
                <Typography variant="caption" sx={{ display: 'block' }}>
                  Heads up — these rules use {result.nondeterminism_warnings.join(', ')},
                  which can produce a different answer on every run. This case may report
                  differences that are not real regressions.
                </Typography>
              </>
            )}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={close} color="inherit" disabled={capturing}>
          {result ? 'Done' : 'Cancel'}
        </Button>
        {!result && (
          <Button
            variant="contained"
            onClick={() => capture(false)}
            disabled={capturing || !name.trim()}
            startIcon={capturing ? <CircularProgress size={14} color="inherit" /> : <ShieldCheck size={16} />}
          >
            {capturing ? 'Capturing…' : 'Capture Baseline'}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default AddToRegressionModal;
