import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import {
  Box, Typography, Button, IconButton, Chip, TextField, MenuItem, Alert, Slide,
  Dialog, DialogTitle, DialogContent, DialogActions, CircularProgress, Divider,
  Tabs, Tab, Tooltip, Checkbox, LinearProgress, InputAdornment,
} from "@mui/material";
import {
  ShieldCheck, Play, Trash2, Search as SearchIcon, Download, RefreshCw,
  Check, AlertTriangle, Camera, RotateCcw, Activity, Square,
} from "lucide-react";
import DataTable from "./DataTable";
import { API } from "../config";
import ModalHeader from "./ModalHeader";

// Bounded by the MIT DataGrid's 100-row page cap; this is also the server's
// page size, so a huge diff costs more requests but never a giant payload.
const DIFF_PAGE_SIZE = 100;

// One colour per outcome, used for the status pill and the diff rows so the
// two always agree at a glance.
const STATUS_META = {
  passed:  { label: 'PASS',   color: '#2E7D32', bg: '#E8F5E9' },
  failed:  { label: 'FAIL',   color: '#C62828', bg: '#FFEBEE' },
  error:   { label: 'ERROR',  color: '#E65100', bg: '#FFF3E0' },
  running: { label: 'RUNNING',color: '#1565C0', bg: '#E3F2FD' },
  cancelled: { label: 'STOPPED', color: '#6C757D', bg: '#F1F3F5' },
};

const DIFF_META = {
  MISSING: { color: '#C62828', bg: '#FFEBEE', label: 'Missing' },
  ADDED:   { color: '#2E7D32', bg: '#E8F5E9', label: 'Added' },
  CHANGED: { color: '#E65100', bg: '#FFF3E0', label: 'Changed' },
};

const money = (v) => (v === null || v === undefined)
  ? '—'
  : Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

const ago = (iso) => {
  if (!iso) return 'never run';
  const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 60) return 'just now';
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
};

const clock = (ms) => {
  if (ms === null || ms === undefined) return null;
  const total = Math.max(0, Math.round(ms / 1000));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return m ? `${m}m ${String(s).padStart(2, '0')}s` : `${s}s`;
};

/**
 * Live progress for a running batch.
 *
 * Percentage comes from the backend, which weights each case equally and adds
 * the fraction of posting dates the current case has executed — so a
 * single-case run still moves instead of sitting at zero until it finishes.
 * The remaining-time figure is an extrapolation from elapsed time, so it is
 * labelled as an estimate and suppressed until there is enough signal.
 */
const RunProgress = ({ batch }) => {
  // Progress switches source mid-run — an estimate from the previous run's
  // duration until enough posting dates have finished to measure this one.
  // The two can disagree, and a bar that jumps backwards reads as a bug, so
  // the displayed value only ever climbs. Reset when a new batch starts.
  const peak = useRef({ id: null, pct: 0 });
  const p = (batch && batch.progress) || {};
  const done = batch && batch.status === 'complete';
  const raw = done ? 100 : (p.percent || 0);
  if (batch && peak.current.id !== batch.batch_id) {
    peak.current = { id: batch.batch_id, pct: raw };
  } else if (raw > peak.current.pct) {
    peak.current.pct = raw;
  }

  if (!batch) return null;
  const pct = done ? 100 : peak.current.pct;
  const eta = clock(p.eta_ms);
  const elapsed = clock(p.elapsed_ms);
  const etaLabel = eta
    ? `~${eta} left${p.source === 'history' ? ' (from last run)' : ' (est.)'}`
    : 'estimating…';

  return (
    <Box sx={{ px: 2.5, py: 1, bgcolor: '#F8F9FA',
               borderBottom: '1px solid', borderColor: 'divider' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 1 }}>
        <Typography variant="caption" color="text.secondary" noWrap sx={{ minWidth: 0 }}>
          {done ? 'Finished'
                : (batch.cancel_requested
                    ? 'Stopping after this posting date…'
                    : (batch.current_case || 'Starting…'))}
          {!done && p.cases_total > 1 && ` · case ${Math.min(p.cases_done + 1, p.cases_total)} of ${p.cases_total}`}
          {!done && p.date_total > 0 && ` · date ${p.date_index + 1} of ${p.date_total}`}
        </Typography>
        <Typography variant="caption" sx={{ fontWeight: 700, color: '#5B5FED', flexShrink: 0 }}>
          {pct.toFixed(0)}%
        </Typography>
      </Box>
      <LinearProgress
        variant="determinate"
        value={pct}
        sx={{ mt: 0.5, borderRadius: 1, height: 6 }}
      />
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 0.4 }}>
        <Typography variant="caption" color="text.disabled">
          {elapsed ? `elapsed ${elapsed}` : ''}
        </Typography>
        <Typography variant="caption" color="text.disabled">
          {done ? '' : etaLabel}
        </Typography>
      </Box>
    </Box>
  );
};

const StatusPill = ({ status, counts }) => {
  const meta = STATUS_META[status];
  if (!meta) {
    return <Chip size="small" label="NEVER RUN" sx={{ height: 20, fontSize: '0.65rem', fontWeight: 600, bgcolor: '#F1F3F5', color: '#6C757D' }} />;
  }
  const diffs = counts?.differences;
  return (
    <Chip
      size="small"
      label={status === 'failed' && diffs ? `FAIL · ${diffs}` : meta.label}
      sx={{ height: 20, fontSize: '0.65rem', fontWeight: 600, bgcolor: meta.bg, color: meta.color }}
    />
  );
};

/**
 * The regression suite: every saved case, what it expects, and how the latest
 * run compared.
 *
 * Master/detail — cases on the left, the selected case's overview, differences,
 * versions and run history on the right. Diff tables page server-side because a
 * failing case across a full book can run to tens of thousands of rows, which
 * no browser wants in one payload.
 */
/* Column definitions for the modal's tables. Kept together so the five grids
 * stay visually consistent with each other and with the rest of the app. */
const DATASET_COLUMNS = [
  { field: 'event_name', headerName: 'Event', flex: 1, minWidth: 160,
    cellClassName: 'cell-strong' },
  {
    field: 'event_type', headerName: 'Type', width: 120,
    renderCell: (params) => (
      <Chip size="small" label={params.value} sx={{ height: 18, fontSize: '0.65rem' }} />
    ),
  },
  {
    field: 'row_count', headerName: 'Rows', type: 'number', width: 110,
    align: 'right', headerAlign: 'right', cellClassName: 'cell-num',
    valueFormatter: (value) => (value || 0).toLocaleString(),
  },
];

const DIFF_COLUMNS = [
  {
    field: 'status', headerName: 'Status', width: 110, sortable: false,
    renderCell: (params) => {
      const meta = DIFF_META[params.value] || {};
      return (
        <Chip size="small" label={meta.label || params.value}
          sx={{ height: 18, fontSize: '0.62rem', fontWeight: 700,
                color: meta.color, bgcolor: 'white',
                border: `1px solid ${meta.color}33` }} />
      );
    },
  },
  { field: 'instrumentid', headerName: 'Instrument', flex: 1, minWidth: 120,
    cellClassName: 'cell-mono', sortable: false },
  { field: 'subinstrumentid', headerName: 'Sub', width: 70, sortable: false },
  { field: 'postingdate', headerName: 'Posting date', width: 120, sortable: false },
  { field: 'effectivedate', headerName: 'Effective date', width: 120, sortable: false },
  { field: 'transactiontype', headerName: 'Transaction type', flex: 1, minWidth: 140,
    sortable: false },
  {
    field: 'expected_amount', headerName: 'Expected', type: 'number', width: 120,
    align: 'right', headerAlign: 'right', cellClassName: 'cell-num', sortable: false,
    valueFormatter: (value) => money(value),
  },
  {
    field: 'actual_amount', headerName: 'Actual', type: 'number', width: 120,
    align: 'right', headerAlign: 'right', cellClassName: 'cell-num', sortable: false,
    valueFormatter: (value) => money(value),
  },
  {
    field: 'delta', headerName: 'Delta', type: 'number', width: 120,
    align: 'right', headerAlign: 'right', sortable: false,
    renderCell: (params) => {
      const meta = DIFF_META[params.row.status] || {};
      const v = params.value;
      return (
        <Box component="span" sx={{ fontFamily: 'ui-monospace, monospace',
                                    fontWeight: 600, color: meta.color }}>
          {v === null || v === undefined ? '—' : `${v > 0 ? '+' : ''}${money(v)}`}
        </Box>
      );
    },
  },
];

const PROFILE_COLUMNS = [
  { field: 'function', headerName: 'Function', flex: 1, minWidth: 280,
    cellClassName: 'cell-mono' },
  {
    field: 'calls', headerName: 'Calls', type: 'number', width: 120,
    align: 'right', headerAlign: 'right',
    valueFormatter: (value) => (value || 0).toLocaleString(),
  },
  {
    field: 'self_ms', headerName: 'Self', type: 'number', width: 100,
    align: 'right', headerAlign: 'right', cellClassName: 'cell-num',
    valueFormatter: (value) => `${(value / 1000).toFixed(1)}s`,
  },
  {
    field: 'total_ms', headerName: 'Total', type: 'number', width: 100,
    align: 'right', headerAlign: 'right',
    valueFormatter: (value) => `${(value / 1000).toFixed(1)}s`,
  },
];

const RegressionModal = ({ open, onClose }) => {
  const [cases, setCases] = useState([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');
  const [selectedId, setSelectedId] = useState(null);
  const [checked, setChecked] = useState([]);
  const [tab, setTab] = useState(0);

  const [templates, setTemplates] = useState([]);
  const [runTemplateId, setRunTemplateId] = useState('__origin__');
  // Off by default: profiling roughly doubles a run, so it is a
  // deliberate diagnostic rather than a tax on every execution.
  const [profileRun, setProfileRun] = useState(false);

  const [batch, setBatch] = useState(null);
  const pollRef = useRef(null);

  const [activeRun, setActiveRun] = useState(null);
  const [diff, setDiff] = useState(null);
  const [diffFilter, setDiffFilter] = useState('');
  const [diffPage, setDiffPage] = useState(0);
  const [diffLoading, setDiffLoading] = useState(false);

  const [versions, setVersions] = useState([]);
  const [runs, setRuns] = useState([]);
  const [busy, setBusy] = useState(null);
  const [banner, setBanner] = useState(null);
  const [confirm, setConfirm] = useState(null);

  const selected = useMemo(
    () => cases.find(c => c.id === selectedId) || null,
    [cases, selectedId]);

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return cases;
    return cases.filter(c =>
      (c.name || '').toLowerCase().includes(needle) ||
      (c.description || '').toLowerCase().includes(needle) ||
      (c.source_template_name || '').toLowerCase().includes(needle));
  }, [cases, search]);

  const loadCases = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API}/regression/cases`);
      const data = await res.json();
      const list = Array.isArray(data) ? data : [];
      setCases(list);
      setSelectedId(prev => (prev && list.some(c => c.id === prev)) ? prev : (list[0]?.id || null));
    } catch (err) {
      setBanner({ severity: 'error', message: `Could not load regression cases: ${err.message}` });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    loadCases();
    fetch(`${API}/user-templates`)
      .then(r => r.json())
      .then(d => setTemplates(Array.isArray(d) ? d : []))
      .catch(() => setTemplates([]));
  }, [open, loadCases]);

  // Stop polling when the dialog goes away, or a closed modal keeps hitting
  // the backend for a batch nobody is watching.
  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current); }, []);
  useEffect(() => {
    if (!open && pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  }, [open]);

  // ── selection-driven loads ────────────────────────────────────────────
  const loadDetail = useCallback(async (caseId) => {
    if (!caseId) { setVersions([]); setRuns([]); setActiveRun(null); setDiff(null); return; }
    try {
      const [vRes, rRes] = await Promise.all([
        fetch(`${API}/regression/cases/${caseId}/versions`),
        fetch(`${API}/regression/runs?case_id=${caseId}`),
      ]);
      const vData = await vRes.json();
      const rData = await rRes.json();
      setVersions(vData?.versions || []);
      const runList = Array.isArray(rData) ? rData : [];
      setRuns(runList);
      setActiveRun(runList[0] || null);
    } catch {
      setVersions([]); setRuns([]);
    }
  }, []);

  useEffect(() => { loadDetail(selectedId); setDiff(null); setDiffPage(0); }, [selectedId, loadDetail]);

  const loadDiff = useCallback(async (runId, page, filter) => {
    if (!runId) { setDiff(null); return; }
    // The previous rows stay on screen until the new ones arrive; blanking
    // them first is what produced the visible blink between pages.
    setDiffLoading(true);
    try {
      const params = new URLSearchParams({
        limit: String(DIFF_PAGE_SIZE),
        offset: String(page * DIFF_PAGE_SIZE),
      });
      if (filter) params.set('status', filter);
      const res = await fetch(`${API}/regression/runs/${runId}/diff?${params}`);
      setDiff(await res.json());
    } catch {
      setDiff(null);
    } finally {
      setDiffLoading(false);
    }
  }, []);

  // Keyed on the run ID rather than the run object: loadCases/loadDetail hand
  // back freshly-built objects, and depending on their identity refetched the
  // same diff on every poll, which is what made the table flicker.
  const activeRunId = activeRun?.run_id || null;
  // The most recent run that was actually profiled. Keyed off the whole list
  // so a later unprofiled run cannot hide it.
  const profiledRun = useMemo(
    () => runs.find(r => r?.timing?.profile?.length > 0) || null, [runs]);
  useEffect(() => {
    if (tab === 1 && activeRunId) loadDiff(activeRunId, diffPage, diffFilter);
  }, [tab, activeRunId, diffPage, diffFilter, loadDiff]);

  // ── running ───────────────────────────────────────────────────────────
  const startRun = useCallback(async (caseIds) => {
    setBanner(null);
    const usePinned = runTemplateId === '__pinned__';
    const body = {
      case_ids: caseIds,
      template_id: (runTemplateId === '__origin__' || usePinned) ? null : runTemplateId,
      use_pinned_code: usePinned,
      profile: profileRun,
    };
    try {
      const res = await fetch(`${API}/regression/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) {
        setBanner({ severity: 'error', message: data?.detail || 'Could not start the run.' });
        return;
      }
      setStopping(false);
      setBatch({ ...data, status: 'running', results: [], current_index: 0 });
      // The backend runs the batch in the background; poll rather than hold
      // the request open for what can be minutes of execution.
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const pRes = await fetch(`${API}/regression/batches/${data.batch_id}`);
          const pData = await pRes.json();
          setBatch(pData);
          if (pData.status === 'complete') {
            clearInterval(pollRef.current);
            pollRef.current = null;
            await loadCases();
            await loadDetail(selectedId);
            setTab(1);
            setBanner({
              severity: pData.failed || pData.errored ? 'warning' : 'success',
              message: `${pData.passed} passed · ${pData.failed} failed · ${pData.errored} errored`,
            });
          }
        } catch { /* keep polling */ }
      }, 1200);
    } catch (err) {
      setBanner({ severity: 'error', message: err.message });
    }
  }, [runTemplateId, profileRun, loadCases, loadDetail, selectedId]);

  // ── mutations ─────────────────────────────────────────────────────────
  const post = useCallback(async (url, body, label) => {
    setBusy(label);
    setBanner(null);
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
      });
      const data = await res.json();
      if (!res.ok) {
        const detail = data?.detail;
        setBanner({ severity: 'error', message: typeof detail === 'string' ? detail : (detail?.message || 'Request failed.') });
        return null;
      }
      setBanner({ severity: 'success', message: data.message });
      await loadCases();
      await loadDetail(selectedId);
      return data;
    } catch (err) {
      setBanner({ severity: 'error', message: err.message });
      return null;
    } finally {
      setBusy(null);
    }
  }, [loadCases, loadDetail, selectedId]);

  const [stopping, setStopping] = useState(false);

  const stopRun = useCallback(async () => {
    if (!batch?.batch_id) return;
    setStopping(true);
    try {
      const res = await fetch(`${API}/regression/batches/${batch.batch_id}/cancel`, {
        method: 'POST',
      });
      const data = await res.json();
      setBanner({ severity: 'info', message: data.message || 'Stopping…' });
    } catch (err) {
      setBanner({ severity: 'error', message: err.message });
    }
  }, [batch]);

  const del = useCallback(async (url, label) => {
    setBusy(label);
    setBanner(null);
    try {
      const res = await fetch(url, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) {
        setBanner({ severity: 'error', message: data?.detail || 'Request failed.' });
        return null;
      }
      setBanner({ severity: 'success', message: data.message });
      await loadCases();
      await loadDetail(selectedId);
      return data;
    } catch (err) {
      setBanner({ severity: 'error', message: err.message });
      return null;
    } finally {
      setBusy(null);
    }
  }, [loadCases, loadDetail, selectedId]);

  const deleteCase = useCallback(async (caseId) => {
    setBusy('delete');
    try {
      const res = await fetch(`${API}/regression/cases/${caseId}`, { method: 'DELETE' });
      const data = await res.json();
      if (!res.ok) { setBanner({ severity: 'error', message: data?.detail || 'Delete failed.' }); return; }
      setBanner({ severity: 'success', message: data.message });
      setSelectedId(null);
      await loadCases();
    } finally {
      setBusy(null);
    }
  }, [loadCases]);

  /* Versions and runs carry row actions, so their columns are built here
   * where the handlers live rather than as module constants. */
  const versionColumns = useMemo(() => [
    {
      field: 'version', headerName: 'Version', width: 130, sortable: false,
      renderCell: (params) => {
        const isActive = params.value === selected?.active_version;
        return (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Chip size="small" label={`v${params.value}`}
              sx={{ height: 19, fontSize: '0.68rem', fontWeight: 700,
                    bgcolor: isActive ? '#5B5FED' : '#F1F3F5',
                    color: isActive ? 'white' : '#6C757D' }} />
            {isActive && <Typography variant="caption" sx={{ color: '#5B5FED' }}>active</Typography>}
          </Box>
        );
      },
    },
    {
      field: 'created_at', headerName: 'Captured', flex: 1, minWidth: 160,
      valueFormatter: (value) => (value ? new Date(value).toLocaleString() : '—'),
    },
    {
      field: 'expected_count', headerName: 'Expected txns', type: 'number', width: 130,
      align: 'right', headerAlign: 'right',
      valueFormatter: (value) => (value || 0).toLocaleString(),
    },
    {
      field: 'template', headerName: 'Template', flex: 1, minWidth: 140,
      valueGetter: (value, row) => row.template_snapshot?.name || '—',
    },
    {
      field: 'note', headerName: 'Note', flex: 1.4, minWidth: 180,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <span>{params.value}</span>
          {params.row.captured_with_errors && (
            <Tooltip title="Captured while some posting dates were failing">
              <AlertTriangle size={13} style={{ color: '#E65100' }} />
            </Tooltip>
          )}
        </Box>
      ),
    },
    {
      field: 'actions', headerName: '', width: 60, sortable: false, align: 'right',
      renderCell: (params) => (params.row.version === selected?.active_version ? null : (
        <Tooltip title="Make this the active baseline (copied forward as a new version)">
          <span>
            <IconButton size="small" disabled={!!busy}
              onClick={(e) => {
                e.stopPropagation();
                setConfirm({
                  title: `Restore v${params.row.version}`,
                  body: `Copy v${params.row.version}'s expected results forward as a new version and make it the active baseline. Nothing in history is overwritten.`,
                  confirmLabel: 'Restore',
                  action: () => post(`${API}/regression/cases/${selected.id}/versions/${params.row.version}/activate`, { note: '' }, 'activate'),
                });
              }}>
              <RotateCcw size={14} />
            </IconButton>
          </span>
        </Tooltip>
      )),
    },
  ], [selected, busy, post]);

  const runColumns = useMemo(() => [
    {
      field: 'started_at', headerName: 'When', flex: 1.2, minWidth: 170,
      valueFormatter: (value) => (value ? new Date(value).toLocaleString() : '—'),
    },
    {
      field: 'status', headerName: 'Status', width: 110, sortable: false,
      renderCell: (params) => <StatusPill status={params.value} counts={params.row.counts} />,
    },
    {
      field: 'version_compared', headerName: 'Baseline', width: 90,
      valueFormatter: (value) => `v${value}`,
    },
    {
      field: 'template', headerName: 'Template', flex: 1, minWidth: 150, sortable: false,
      renderCell: (params) => (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <span>{params.row.template_used?.name || '—'}</span>
          {params.row.template_used?.pinned && (
            <Chip size="small" label="pinned" sx={{ height: 16, fontSize: '0.6rem' }} />
          )}
        </Box>
      ),
    },
    {
      field: 'differences', headerName: 'Differences', width: 110, align: 'right',
      headerAlign: 'right', sortable: false,
      valueGetter: (value, row) => row.counts?.differences ?? null,
      renderCell: (params) => (params.value ?? '—'),
    },
    {
      field: 'duration_ms', headerName: 'Duration', flex: 1, minWidth: 170, sortable: false,
      renderCell: (params) => (
        <Box sx={{ py: 0.5 }}>
          <div>{params.value ? `${(params.value / 1000).toFixed(1)}s` : '—'}</div>
          {params.row.timing?.slowest_date_ms != null && (
            <Typography variant="caption" sx={{ display: 'block', color: 'text.disabled' }}>
              worst date {params.row.timing.slowest_date}: {(params.row.timing.slowest_date_ms / 1000).toFixed(1)}s
            </Typography>
          )}
          {params.row.timing?.execute_ms != null && (
            <Typography variant="caption" sx={{ display: 'block', color: 'text.disabled' }}>
              rules {(params.row.timing.execute_ms / 1000).toFixed(1)}s
              {params.row.timing.evals > 0 && ` · ${params.row.timing.evals.toLocaleString()} evals`}
            </Typography>
          )}
        </Box>
      ),
    },
    {
      field: 'accepted', headerName: '', width: 150, sortable: false,
      renderCell: (params) => (params.row.accepted_as_version ? (
        <Chip size="small" label={`accepted → v${params.row.accepted_as_version}`}
          sx={{ height: 18, fontSize: '0.62rem', bgcolor: '#E8F5E9', color: '#2E7D32' }} />
      ) : null),
    },
    {
      field: 'actions', headerName: '', width: 60, sortable: false, align: 'right',
      renderCell: (params) => (
        <Tooltip title="Delete this run">
          <span>
            <IconButton size="small" disabled={!!busy}
              onClick={(e) => {
                // The row itself opens the diff; deleting must not navigate.
                e.stopPropagation();
                setConfirm({
                  title: 'Delete run',
                  body: `Delete the run from ${params.row.started_at ? new Date(params.row.started_at).toLocaleString() : 'this case'}`
                    + ' and its differences? The baseline is unaffected.',
                  confirmLabel: 'Delete',
                  danger: true,
                  action: () => del(`${API}/regression/runs/${params.row.run_id}`, 'delete-run'),
                });
              }}>
              <Trash2 size={13} />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ], [busy, del]);

  const batchRunning = batch && batch.status === 'running';
  const summaryCounts = diff?.counts || activeRun?.counts || {};

  return (
    <Dialog open={open} onClose={onClose} maxWidth="xl" fullWidth
      TransitionComponent={Slide} TransitionProps={{ direction: 'up' }}
      PaperProps={{ sx: { borderRadius: 4, height: '88vh', overflow: 'hidden', border: '1px solid', borderColor: 'divider' } }}>
      <DialogTitle sx={{ p: 0 }}>
        <ModalHeader
          badge="REGRESSION" BadgeIcon={ShieldCheck}
          title="Regression Suite"
          subtitle="Replay frozen datasets and compare every transaction against the saved baseline"
          onClose={onClose}
        />
      </DialogTitle>

      <DialogContent sx={{ p: 0, display: 'flex', overflow: 'hidden' }}>
        {/* ── Case list ─────────────────────────────────────────────── */}
        <Box sx={{ width: 340, flexShrink: 0, borderRight: '1px solid', borderColor: 'divider', display: 'flex', flexDirection: 'column' }}>
          <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
            <TextField
              fullWidth size="small" placeholder="Search cases…"
              value={search} onChange={(e) => setSearch(e.target.value)}
              InputProps={{ startAdornment: <InputAdornment position="start"><SearchIcon size={14} /></InputAdornment> }}
              sx={{ mb: 1 }}
            />
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button
                fullWidth size="small" variant="contained"
                startIcon={batchRunning ? <CircularProgress size={13} color="inherit" /> : <Play size={14} />}
                disabled={batchRunning || cases.length === 0}
                onClick={() => startRun(checked.length ? checked : [])}
                data-testid="regression-run-all"
              >
                {batchRunning ? 'Running…' : (checked.length ? `Run ${checked.length}` : 'Run All')}
              </Button>
              <Tooltip title="Refresh">
                <span>
                  <IconButton size="small" onClick={loadCases} disabled={loading || batchRunning}>
                    <RefreshCw size={15} />
                  </IconButton>
                </span>
              </Tooltip>
            </Box>
          </Box>


          <Box sx={{ flex: 1, overflow: 'auto' }}>
            {loading && <Box sx={{ p: 3, textAlign: 'center' }}><CircularProgress size={22} /></Box>}
            {!loading && filtered.length === 0 && (
              <Box sx={{ p: 4, textAlign: 'center', color: 'text.secondary' }}>
                <ShieldCheck size={36} style={{ opacity: 0.25, marginBottom: 10 }} />
                <Typography variant="body2" fontWeight={500}>No regression cases yet</Typography>
                <Typography variant="caption">
                  Load a dataset, then use “Add to Regression” in the Rule Manager.
                </Typography>
              </Box>
            )}
            {filtered.map(c => {
              const last = c.last_run;
              const isSelected = c.id === selectedId;
              return (
                <Box
                  key={c.id}
                  onClick={() => { setSelectedId(c.id); setTab(0); }}
                  sx={{
                    display: 'flex', gap: 1, alignItems: 'flex-start', px: 1.5, py: 1.25,
                    borderBottom: '1px solid', borderColor: 'divider', cursor: 'pointer',
                    bgcolor: isSelected ? 'rgba(91,95,237,0.07)' : 'transparent',
                    borderLeft: '3px solid',
                    borderLeftColor: isSelected ? '#5B5FED' : 'transparent',
                    '&:hover': { bgcolor: isSelected ? 'rgba(91,95,237,0.09)' : '#F8F9FA' },
                  }}
                >
                  <Checkbox
                    size="small" sx={{ p: 0.25, mt: 0.1 }}
                    checked={checked.includes(c.id)}
                    onClick={(e) => e.stopPropagation()}
                    onChange={(e) => setChecked(prev =>
                      e.target.checked ? [...prev, c.id] : prev.filter(id => id !== c.id))}
                  />
                  <Box sx={{ minWidth: 0, flex: 1 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', gap: 1 }}>
                      <Typography variant="body2" fontWeight={600} noWrap>{c.name}</Typography>
                      <StatusPill status={last?.status} counts={last?.counts} />
                    </Box>
                    <Typography variant="caption" color="text.secondary" noWrap sx={{ display: 'block' }}>
                      {c.source_template_name || 'Workspace rules'} · v{c.active_version} ·{' '}
                      {(c.expected_count || 0).toLocaleString()} txns
                    </Typography>
                    <Typography variant="caption" color="text.disabled">
                      {ago(last?.started_at)}
                    </Typography>
                  </Box>
                </Box>
              );
            })}
          </Box>
        </Box>

        {/* ── Detail ────────────────────────────────────────────────── */}
        <Box sx={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
          {batchRunning && <RunProgress batch={batch} />}
          {banner && (
            <Alert severity={banner.severity} sx={{ m: 2, mb: 0, py: 0.25 }}
              onClose={() => setBanner(null)}>
              <Typography variant="caption">{banner.message}</Typography>
            </Alert>
          )}
          {!selected && (
            <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'text.secondary' }}>
              <Typography variant="body2">Select a case to see its baseline and differences.</Typography>
            </Box>
          )}

          {selected && (
            <>
              <Box sx={{ px: 2.5, pt: 2, pb: 0 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
                  <Box sx={{ minWidth: 0 }}>
                    <Typography variant="h6" fontWeight={600} noWrap>{selected.name}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      {selected.description || 'No description'}
                    </Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexShrink: 0 }}>
                    <TextField
                      select size="small" value={runTemplateId}
                      onChange={(e) => setRunTemplateId(e.target.value)}
                      sx={{ minWidth: 230 }}
                      disabled={batchRunning}
                    >
                      <MenuItem value="__origin__">
                        Current “{selected.source_template_name || 'Workspace rules'}”
                      </MenuItem>
                      <MenuItem value="__pinned__">Pinned baseline code</MenuItem>
                      {templates.length > 0 && (
                        <MenuItem disabled sx={{ fontSize: '0.7rem', opacity: 0.7 }}>
                          Other templates
                        </MenuItem>
                      )}
                      {templates.map(t => <MenuItem key={t.id} value={t.id}>{t.name}</MenuItem>)}
                    </TextField>
                    <Tooltip title={profileRun
                      ? 'Profiling on — the next run reports where its time goes (about 2x slower)'
                      : 'Profile the next run to see which functions cost the time'}>
                      <span>
                        <IconButton
                          size="small"
                          onClick={() => setProfileRun(v => !v)}
                          disabled={batchRunning}
                          data-testid="regression-profile-toggle"
                          sx={{
                            color: profileRun ? '#fff' : '#6C757D',
                            bgcolor: profileRun ? '#5B5FED' : 'transparent',
                            border: '1px solid',
                            borderColor: profileRun ? '#5B5FED' : '#CED4DA',
                            borderRadius: 1,
                            '&:hover': { bgcolor: profileRun ? '#4A4EDB' : '#F1F3F5' },
                          }}
                        >
                          <Activity size={14} />
                        </IconButton>
                      </span>
                    </Tooltip>
                    {batchRunning ? (
                      <Button
                        variant="outlined" size="small" color="error"
                        startIcon={stopping
                          ? <CircularProgress size={13} color="inherit" />
                          : <Square size={12} />}
                        disabled={stopping}
                        onClick={stopRun}
                        data-testid="regression-stop"
                      >
                        {stopping ? 'Stopping…' : 'Stop'}
                      </Button>
                    ) : (
                      <Button
                        variant="contained" size="small"
                        startIcon={<Play size={14} />}
                        onClick={() => startRun([selected.id])}
                        data-testid="regression-run-case"
                      >
                        Run
                      </Button>
                    )}
                  </Box>
                </Box>

                {selected.nondeterminism_warnings?.length > 0 && (
                  <Alert severity="warning" icon={<AlertTriangle size={17} />} sx={{ mt: 1.5, py: 0.25 }}>
                    <Typography variant="caption">
                      These rules use {selected.nondeterminism_warnings.join(', ')} — results may
                      differ between runs for reasons that are not regressions.
                    </Typography>
                  </Alert>
                )}
                <Tabs value={tab} onChange={(e, v) => setTab(v)} sx={{ mt: 1, minHeight: 38 }}>
                  <Tab label="Overview" sx={{ textTransform: 'none', minHeight: 38, fontSize: '0.8rem' }} />
                  <Tab label="Differences" sx={{ textTransform: 'none', minHeight: 38, fontSize: '0.8rem' }} />
                  <Tab label={`Versions (${versions.length})`} sx={{ textTransform: 'none', minHeight: 38, fontSize: '0.8rem' }} />
                  <Tab label={`Runs (${runs.length})`} sx={{ textTransform: 'none', minHeight: 38, fontSize: '0.8rem' }} />
                </Tabs>
              </Box>
              <Divider />

              <Box sx={{ flex: 1, overflow: 'auto', px: 2.5, py: 2 }}>
                {/* Overview */}
                {tab === 0 && (
                  <Box>
                    <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 2 }}>
                      <Chip size="small" label={`Baseline v${selected.active_version}`} sx={{ bgcolor: '#EEF0FF', color: '#3F43C7', fontWeight: 600 }} />
                      <Chip size="small" label={`${(selected.expected_count || 0).toLocaleString()} expected txns`} />
                      <Chip size="small" label={`${(selected.dataset_summary?.total_rows || 0).toLocaleString()} data rows`} />
                      <Chip size="small" label={`${selected.dataset_summary?.posting_date_count || 0} posting dates`} />
                      <Chip size="small" label={`${selected.dataset_summary?.instrument_count || 0} instruments`} />
                      <Chip size="small" label={`± ${selected.amount_tolerance} tolerance`} variant="outlined" />
                    </Box>

                    <Typography variant="subtitle2" fontWeight={600} sx={{ mb: 1 }}>Frozen dataset</Typography>
                    <Box sx={{ mb: 3 }}>
                      <DataTable
                        rows={selected.dataset_summary?.events || []}
                        columns={DATASET_COLUMNS}
                        autoHeight
                        getRowId={(r) => r.event_name}
                        emptyLabel="No events in this snapshot"
                      />
                    </Box>

                    <Typography variant="subtitle2" fontWeight={600} sx={{ mb: 1 }}>Actions</Typography>
                    <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                      <Button
                        size="small" variant="outlined" startIcon={<Camera size={14} />}
                        disabled={!!busy}
                        onClick={() => setConfirm({
                          title: 'Recapture dataset',
                          body: `Snapshot the dataset currently loaded in the app and make it the new baseline for “${selected.name}”. The existing v${selected.active_version} is kept in history.`,
                          confirmLabel: 'Recapture',
                          action: () => post(`${API}/regression/cases/${selected.id}/recapture`, { note: '' }, 'recapture'),
                        })}
                      >
                        Recapture from current data
                      </Button>
                      <Button
                        size="small" variant="outlined" color="error" startIcon={<Trash2 size={14} />}
                        disabled={!!busy}
                        onClick={() => setConfirm({
                          title: 'Delete case',
                          body: `Delete “${selected.name}” along with its dataset snapshot, every baseline version and all run history. This cannot be undone.`,
                          confirmLabel: 'Delete',
                          danger: true,
                          action: () => deleteCase(selected.id),
                        })}
                      >
                        Delete case
                      </Button>
                    </Box>
                  </Box>
                )}

                {/* Differences */}
                {tab === 1 && (
                  <Box>
                    {!activeRun && (
                      <Box sx={{ py: 6, textAlign: 'center', color: 'text.secondary' }}>
                        <Play size={34} style={{ opacity: 0.25, marginBottom: 10 }} />
                        <Typography variant="body2">This case has not been run yet.</Typography>
                      </Box>
                    )}

                    {activeRun && (
                      <>
                        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 2, mb: 1.5, flexWrap: 'wrap' }}>
                          <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap', alignItems: 'center' }}>
                            <StatusPill status={activeRun.status} counts={activeRun.counts} />
                            <Typography variant="caption" color="text.secondary">
                              v{activeRun.version_compared} vs “{activeRun.template_used?.name || '—'}” · {ago(activeRun.started_at)}
                            </Typography>
                          </Box>
                          <Box sx={{ display: 'flex', gap: 1 }}>
                            <Button
                              size="small" variant="outlined" startIcon={<Download size={14} />}
                              href={`${API}/regression/runs/${activeRun.run_id}/export`}
                            >
                              Export
                            </Button>
                            <Button
                              size="small" variant="contained" color="warning" startIcon={<Check size={14} />}
                              disabled={!!busy || !!activeRun.accepted_as_version
                                || activeRun.status === 'error' || activeRun.status === 'passed'}
                              onClick={() => setConfirm({
                                title: 'Replace expected results',
                                body: `Save this run's output as the new expected baseline for “${selected.name}”. `
                                  + `${summaryCounts.missing || 0} missing, ${summaryCounts.added || 0} added and `
                                  + `${summaryCounts.changed || 0} changed transaction(s) will become the expected result. `
                                  + `v${selected.active_version} stays in history.`,
                                confirmLabel: `Save as v${(selected.active_version || 1) + 1}`,
                                action: () => post(`${API}/regression/runs/${activeRun.run_id}/accept`, { note: '' }, 'accept'),
                              })}
                            >
                              {activeRun.accepted_as_version
                                ? `Accepted as v${activeRun.accepted_as_version}`
                                : 'Accept new results'}
                            </Button>
                          </Box>
                        </Box>

                        <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', px: 1.5, py: 1, mb: 1.5, bgcolor: '#F8F9FA', borderRadius: 1.5 }}>
                          {[
                            ['Expected', summaryCounts.expected_total],
                            ['Actual', summaryCounts.actual_total],
                            ['Matched', summaryCounts.matched],
                            ['Missing', summaryCounts.missing],
                            ['Added', summaryCounts.added],
                            ['Changed', summaryCounts.changed],
                          ].map(([label, value]) => (
                            <Box key={label}>
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>{label}</Typography>
                              <Typography variant="body2" fontWeight={600}>{(value || 0).toLocaleString()}</Typography>
                            </Box>
                          ))}
                        </Box>

                        {activeRun.errors?.length > 0 && (
                          <Alert severity="error" sx={{ mb: 1.5 }}>
                            <Typography variant="caption" fontWeight={600}>
                              {activeRun.errors.length} execution error(s)
                            </Typography>
                            <Box component="ul" sx={{ pl: 2, m: 0, fontSize: '0.72rem' }}>
                              {activeRun.errors.slice(0, 5).map((e, i) => (
                                <li key={i}>{e.posting_date || 'run'}: {String(e.error).slice(0, 200)}</li>
                              ))}
                            </Box>
                          </Alert>
                        )}

                        <Box sx={{ display: 'flex', gap: 0.75, mb: 1 }}>
                          {['', 'MISSING', 'ADDED', 'CHANGED'].map(f => (
                            <Chip
                              key={f || 'all'}
                              size="small"
                              label={f ? DIFF_META[f].label : 'All'}
                              onClick={() => { setDiffFilter(f); setDiffPage(0); }}
                              variant={diffFilter === f ? 'filled' : 'outlined'}
                              sx={diffFilter === f && f
                                ? { bgcolor: DIFF_META[f].bg, color: DIFF_META[f].color, fontWeight: 600 }
                                : undefined}
                            />
                          ))}
                        </Box>

                        {/* Fixed height whether or not it is running, so a
                            refetch cannot shift the table underneath it. */}
                        <Box sx={{ height: 4, mb: 1 }}>
                          {diffLoading && <LinearProgress sx={{ height: 4, borderRadius: 1 }} />}
                        </Box>

                        {diff && diff.total === 0 && (
                          <Box sx={{ py: 5, textAlign: 'center', color: 'text.secondary' }}>
                            <Check size={34} style={{ opacity: 0.3, marginBottom: 8 }} />
                            <Typography variant="body2">
                              {diffFilter ? 'No differences of this kind.' : 'Every transaction matched the baseline.'}
                            </Typography>
                          </Box>
                        )}

                        {diff && diff.total > 0 && (
                          <>
                            <Box sx={{ height: 420 }}>
                              <DataTable
                                rows={diff.rows || []}
                                columns={DIFF_COLUMNS}
                                getRowId={(r) => `${r.instrumentid}-${r.subinstrumentid}-${r.postingdate}-${r.transactiontype}-${r.status}-${r.expected_amount}-${r.actual_amount}`}
                                getRowClassName={(params) => `diff-${params.row.status}`}
                                disableColumnFilter
                                disableColumnMenu
                                /* The server pages this: a failing case can run
                                   to tens of thousands of differences, so the
                                   grid is told the true count and asks for one
                                   page at a time. */
                                paginationMode="server"
                                rowCount={diff.total}
                                paginationModel={{ page: diffPage, pageSize: DIFF_PAGE_SIZE }}
                                onPaginationModelChange={(m) => setDiffPage(m.page)}
                                pageSizeOptions={[DIFF_PAGE_SIZE]}
                                emptyLabel="No differences"
                                sx={{
                                  '& .diff-MISSING': { bgcolor: DIFF_META.MISSING.bg },
                                  '& .diff-ADDED': { bgcolor: DIFF_META.ADDED.bg },
                                  '& .diff-CHANGED': { bgcolor: DIFF_META.CHANGED.bg },
                                }}
                              />
                            </Box>
                          </>
                        )}
                      </>
                    )}
                  </Box>
                )}

                {/* Versions */}
                {tab === 2 && (
                  <DataTable
                    rows={versions}
                    columns={versionColumns}
                    autoHeight
                    getRowId={(v) => v.version}
                    getRowClassName={(params) =>
                      params.row.version === selected.active_version ? 'row-active' : ''}
                    emptyLabel="No versions"
                    sx={{ '& .row-active': { bgcolor: 'rgba(91,95,237,0.06)' } }}
                  />
                )}

                {/* Runs */}
                {tab === 3 && (
                  <>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                    <Typography variant="caption" color="text.secondary">
                      {runs.length} run{runs.length !== 1 ? 's' : ''} recorded — history only,
                      clearing it never changes the baseline.
                    </Typography>
                    <Button
                      size="small" color="error" startIcon={<Trash2 size={13} />}
                      disabled={!!busy || runs.length === 0}
                      onClick={() => setConfirm({
                        title: 'Clear run history',
                        body: `Delete all ${runs.length} run(s) recorded for “${selected.name}”, `
                          + 'including their differences. The case, its dataset and every '
                          + 'baseline version are kept, so the next run still compares '
                          + 'against the same expected results.',
                        confirmLabel: 'Clear history',
                        danger: true,
                        action: () => del(`${API}/regression/cases/${selected.id}/runs`, 'clear-runs'),
                      })}
                    >
                      Clear history
                    </Button>
                  </Box>
                  {profiledRun && (
                    <Box sx={{ mb: 3, p: 2, border: '1px solid', borderColor: '#5B5FED33',
                               borderRadius: 2, bgcolor: 'rgba(91,95,237,0.03)' }}>
                      <Typography variant="subtitle2" fontWeight={600} sx={{ mb: 0.5 }}>
                        Where the time went
                      </Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                        Heaviest functions in the profiled run from{' '}
                        {profiledRun.started_at ? new Date(profiledRun.started_at).toLocaleString() : ''},
                        by time spent inside each one.
                      </Typography>
                      <DataTable
                        rows={profiledRun.timing.profile}
                        columns={PROFILE_COLUMNS}
                        autoHeight
                        emptyLabel="No profile captured"
                      />
                    </Box>
                  )}

                  <DataTable
                    rows={runs}
                    columns={runColumns}
                    autoHeight
                    getRowId={(r) => r.run_id}
                    onRowClick={(params) => {
                      setActiveRun(params.row); setDiffPage(0); setTab(1);
                    }}
                    getRowClassName={(params) =>
                      params.row.run_id === activeRun?.run_id ? 'row-active' : ''}
                    emptyLabel="No runs recorded"
                    sx={{
                      '& .MuiDataGrid-row': { cursor: 'pointer' },
                      '& .row-active': { bgcolor: 'rgba(91,95,237,0.06)' },
                    }}
                  />

                  </>
                )}
              </Box>
            </>
          )}
        </Box>
      </DialogContent>

      {/* Shared confirm step — every destructive or baseline-moving action
          restates exactly what it is about to do before doing it. */}
      <Dialog open={!!confirm} onClose={() => setConfirm(null)} maxWidth="xs" fullWidth>
        <DialogTitle sx={{ p: 0 }}>
          <ModalHeader badge="CONFIRM" title={confirm?.title || ''} onClose={() => setConfirm(null)} />
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" sx={{ mt: 1 }}>{confirm?.body}</Typography>
        </DialogContent>
        <DialogActions>
          <Button color="inherit" onClick={() => setConfirm(null)}>Cancel</Button>
          <Button
            variant="contained"
            color={confirm?.danger ? 'error' : 'primary'}
            disabled={!!busy}
            startIcon={busy ? <CircularProgress size={14} color="inherit" /> : null}
            onClick={async () => { const fn = confirm?.action; setConfirm(null); if (fn) await fn(); }}
          >
            {confirm?.confirmLabel || 'Confirm'}
          </Button>
        </DialogActions>
      </Dialog>
    </Dialog>
  );
};

export default RegressionModal;
