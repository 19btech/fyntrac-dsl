import React, { useState, useEffect, useMemo, useCallback, useRef } from "react";
import {
  Box, Typography, Button, Menu, MenuItem, CircularProgress, Alert, Chip,
  Stack, Tooltip, LinearProgress,
} from "@mui/material";
import { useGridApiRef, gridFilteredSortedRowEntriesSelector } from "@mui/x-data-grid";
import DataTable, { numericAwareComparator } from "./DataTable";
import {
  Receipt, Download, RotateCcw, FileDown, ChevronDown, Play,
} from "lucide-react";
import axios from "axios";
import { API } from "../config";

/* Brand tokens — mirrors LivePreview.js so the report matches the app. */
const C = {
  brand: '#5B5FED', brandDark: '#4A4ED0', brandSoft: '#EEF0FE',
  ink: '#14213D', body: '#495057', muted: '#6C757D',
  border: '#E9ECEF', surface: '#FFFFFF', bg: '#F6F7FB',
  success: '#10B981', successSoft: '#E7F8F1', successInk: '#065F46',
  danger: '#DC2626', dangerSoft: '#FEE2E2', dangerInk: '#991B1B',
  zebra: '#FAFBFF',
};

const fmtAmount = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
};

/* Column order as specified: dates, then identity, then amount last. Amount is
 * typed as a number so the column menu offers numeric comparisons rather than
 * text matching. */
const COLUMNS = [
  { field: 'postingdate', headerName: 'Posting Date', minWidth: 130, flex: 1 },
  { field: 'effectivedate', headerName: 'Effective Date', minWidth: 130, flex: 1 },
  {
    field: 'instrumentid', headerName: 'Instrument ID', minWidth: 140, flex: 1.2,
    cellClassName: 'cell-strong',
  },
  {
    field: 'subinstrumentid', headerName: 'Sub-Instrument', minWidth: 130, flex: 0.8,
    sortComparator: numericAwareComparator,
  },
  {
    field: 'transactiontype', headerName: 'Transaction Type', minWidth: 160, flex: 1.2,
    renderCell: (params) => (params.value
      ? <Chip size="small" label={params.value}
          sx={{ height: 18, fontSize: '0.65rem', bgcolor: C.surface,
                border: `1px solid ${C.border}`, color: C.body }} />
      : '—'),
  },
  {
    field: 'amount', headerName: 'Amount', type: 'number',
    minWidth: 130, flex: 0.9, align: 'right', headerAlign: 'right',
    valueFormatter: (value) => fmtAmount(value),
    cellClassName: (params) => (Number(params.value) < 0 ? 'cell-num neg' : 'cell-num'),
  },
];

const EXPORT_KEYS = COLUMNS.map(c => c.field);

const toCSV = (rows, keys) => {
  if (!rows || !rows.length) return '';
  const escape = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [keys.join(','), ...rows.map(r => keys.map(k => escape(r[k])).join(','))].join('\n');
};

const downloadBlob = (data, filename, mime = 'text/csv') => {
  const blob = new Blob([data], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
};

export default function TransactionReport() {
  const apiRef = useGridApiRef();
  const [rows, setRows] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [exportAnchor, setExportAnchor] = useState(null);
  const [running, setRunning] = useState(false);
  const [runResult, setRunResult] = useState(null);
  /* What the grid is currently showing, after its own filters and sorting. The
   * summary total and both exports follow it, so what you export is what you
   * can see. */
  const [visible, setVisible] = useState([]);
  const visibleKey = useRef('');

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const res = await axios.get(`${API}/transaction-reports`, { params: { limit: 20000 } });
      const data = res.data?.transactions || [];
      // The grid needs a stable id, and the report has no natural key: the same
      // instrument, date and type can legitimately repeat.
      setRows(data.map((r, i) => ({ ...r, id: i })));
      setMeta(res.data || null);
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || 'Failed to load transactions');
      setRows([]); setMeta(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  /* Run the whole book: every instrument, every posting date, using the rules
   * currently in the workspace. No template argument — the server combines the
   * saved rules by priority, which is exactly the template last loaded into the
   * Rule Manager. `replace` clears the previous report first, so a second press
   * does not double every row. */
  const runAll = useCallback(async () => {
    setRunning(true); setError(null); setRunResult(null);
    try {
      const res = await axios.post(`${API}/transaction-reports/run`, null,
        { params: { replace: true } });
      setRunResult(res.data || null);
      await load();
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || 'Run failed');
    } finally {
      setRunning(false);
    }
  }, [load]);

  /* The grid owns filtering and sorting, so the visible set is read back from
   * it rather than recomputed here. onStateChange fires on every interaction,
   * so this only re-renders when the visible rows actually differ. */
  const syncVisible = useCallback(() => {
    if (!apiRef.current?.getRow) return;
    let entries;
    try {
      entries = gridFilteredSortedRowEntriesSelector(apiRef);
    } catch {
      return;
    }
    const models = entries.map(e => e.model);
    const key = `${models.length}:${models[0]?.id ?? ''}:${models[models.length - 1]?.id ?? ''}`;
    if (key === visibleKey.current) return;
    visibleKey.current = key;
    setVisible(models);
  }, [apiRef]);

  useEffect(() => { visibleKey.current = ''; syncVisible(); }, [rows, syncVisible]);

  const total = useMemo(
    () => visible.reduce((s, r) => s + (Number(r.amount) || 0), 0), [visible]);

  const isFiltered = visible.length !== rows.length;
  const exportSuffix = () => (isFiltered ? 'filtered' : 'all');

  const exportCsv = () => {
    downloadBlob(toCSV(visible, EXPORT_KEYS), `transaction-report-${exportSuffix()}.csv`);
    setExportAnchor(null);
  };
  const exportJson = () => {
    downloadBlob(JSON.stringify({
      generated_at: new Date().toISOString(),
      filtered: isFiltered,
      total_amount: total,
      row_count: visible.length,
      transactions: visible.map(r => Object.fromEntries(
        EXPORT_KEYS.map(k => [k, r[k]]))),
    }, null, 2), `transaction-report-${exportSuffix()}.json`, 'application/json');
    setExportAnchor(null);
  };

  return (
    <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0, bgcolor: C.bg }}>
      {/* Header */}
      <Box sx={{
        px: 2, py: 1.25, bgcolor: C.brandSoft, borderBottom: `1px solid #D6D8FE`,
        display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap',
      }}>
        <Receipt size={16} color={C.brand} />
        <Typography variant="body2" sx={{ fontWeight: 600, color: C.ink }}>
          Transaction Report
        </Typography>
        <Typography variant="caption" sx={{ color: C.muted, flex: 1, minWidth: 180 }}>
          Every transaction across all periods. Use a column's menu to sort,
          filter or hide it.
        </Typography>

        <Tooltip title="Run the loaded rules over every instrument, on every posting date in the current dataset">
          <span>
            <Button size="small" variant="contained" onClick={runAll} disabled={running || loading}
              startIcon={running ? <CircularProgress size={12} color="inherit" /> : <Play size={13} />}
              sx={{ textTransform: 'none', fontSize: '0.75rem', bgcolor: C.success,
                '&:hover': { bgcolor: '#0E9F6E' } }}>
              {running ? 'Running…' : 'Play'}
            </Button>
          </span>
        </Tooltip>

        <Button size="small" variant="outlined" onClick={load} disabled={loading || running}
          startIcon={loading ? <CircularProgress size={12} color="inherit" /> : <RotateCcw size={13} />}
          sx={{ textTransform: 'none', fontSize: '0.75rem', borderColor: C.brand, color: C.brand }}>
          {loading ? 'Loading…' : 'Refresh'}
        </Button>

        <Button size="small" variant="contained" disabled={!visible.length}
          onClick={(e) => setExportAnchor(e.currentTarget)}
          startIcon={<Download size={13} />} endIcon={<ChevronDown size={13} />}
          sx={{ textTransform: 'none', fontSize: '0.75rem', bgcolor: C.brand,
            '&:hover': { bgcolor: C.brandDark } }}>
          Export
        </Button>
        <Menu anchorEl={exportAnchor} open={Boolean(exportAnchor)} onClose={() => setExportAnchor(null)}>
          <MenuItem onClick={exportCsv} sx={{ fontSize: '0.8rem', gap: 1 }}>
            <FileDown size={14} /> Export as CSV
          </MenuItem>
          <MenuItem onClick={exportJson} sx={{ fontSize: '0.8rem', gap: 1 }}>
            <FileDown size={14} /> Export as JSON
          </MenuItem>
        </Menu>
      </Box>

      {(loading || running) && <LinearProgress sx={{ height: 2 }} />}

      {/* What the last Play produced. The source is stated rather than chosen:
          the report always runs the rules currently in the workspace. */}
      {runResult && !error && (
        <Alert severity={runResult.dates_failed ? 'warning' : 'success'}
          onClose={() => setRunResult(null)}
          sx={{ mx: 2, mt: 1, py: 0.25 }}>
          <Typography variant="caption">
            {runResult.message}
            {runResult.rule_names?.length > 0 && (
              <> — using {runResult.rule_names.length} rule
              {runResult.rule_names.length !== 1 ? 's' : ''}: {runResult.rule_names.join(', ')}</>
            )}
          </Typography>
          {runResult.errors?.length > 0 && (
            <Box component="ul" sx={{ pl: 2, m: 0, fontSize: '0.72rem' }}>
              {runResult.errors.slice(0, 5).map((e, i) => (
                <li key={i}>{e.posting_date || 'run'}: {String(e.error).slice(0, 200)}</li>
              ))}
            </Box>
          )}
        </Alert>
      )}

      {/* Summary strip — follows the grid's current filters. */}
      {meta && !error && (
        <Stack direction="row" spacing={1} sx={{ px: 2, py: 1, flexWrap: 'wrap', gap: 0.75 }}>
          <Chip size="small"
            label={isFiltered
              ? `${visible.length.toLocaleString()} of ${rows.length.toLocaleString()} transactions`
              : `${rows.length.toLocaleString()} transactions`}
            sx={{ bgcolor: C.brandSoft, color: C.brand, fontWeight: 600 }} />
          <Chip size="small" label={`${meta.summary?.instrument_count ?? 0} instruments`}
            sx={{ bgcolor: C.surface, color: C.body, border: `1px solid ${C.border}` }} />
          <Chip size="small" label={`${meta.summary?.run_count ?? 0} runs`}
            sx={{ bgcolor: C.surface, color: C.body, border: `1px solid ${C.border}` }} />
          <Chip size="small" label={`Total ${fmtAmount(total)}`}
            sx={{ bgcolor: total < 0 ? C.dangerSoft : C.successSoft,
                  color: total < 0 ? C.dangerInk : C.successInk, fontWeight: 700 }} />
        </Stack>
      )}

      {error && <Alert severity="error" sx={{ mx: 2, mb: 1 }}>{error}</Alert>}

      {meta?.truncated && (
        <Alert severity="info" sx={{ mx: 2, mb: 1, fontSize: '0.75rem' }}>
          Showing the first {meta.returned.toLocaleString()} of {meta.total.toLocaleString()} transactions.
        </Alert>
      )}

      {/* Grid */}
      <Box sx={{ flex: 1, minHeight: 0, px: 2, pb: 2 }}>
        <DataTable
          apiRef={apiRef}
          rows={rows}
          columns={COLUMNS}
          loading={loading || running}
          onStateChange={syncVisible}
          emptyLabel={rows.length
            ? 'No transactions match these filters'
            : 'No transactions yet — press Play to run the loaded rules'}
        />
      </Box>
    </Box>
  );
}
