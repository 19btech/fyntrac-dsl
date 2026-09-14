import React, { useState, useEffect, useMemo, useCallback } from "react";
import {
  Box, Typography, Table, TableBody, TableCell, TableContainer, TableHead,
  TableRow, TableFooter, TableSortLabel, Paper, Button, Menu, MenuItem,
  CircularProgress, Alert, Chip, Stack, TextField, MenuItem as SelectItem,
  Tooltip, IconButton, LinearProgress,
} from "@mui/material";
import {
  Receipt, Download, RotateCcw, FileDown, ChevronDown, Filter, X,
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

/* The canonical report order. instrumentid appeared twice in the spec; the
 * second occurrence is redundant once subinstrumentid follows it. */
const DEFAULT_SORT = [
  'instrumentid', 'postingdate', 'effectivedate', 'subinstrumentid', 'amount',
];

const COLUMNS = [
  { key: 'instrumentid',    label: 'Instrument ID',   align: 'left'  },
  { key: 'subinstrumentid', label: 'Sub-Instrument',  align: 'left',  numericAware: true },
  { key: 'postingdate',     label: 'Posting Date',    align: 'left'  },
  { key: 'effectivedate',   label: 'Effective Date',  align: 'left'  },
  { key: 'transactiontype', label: 'Transaction Type', align: 'left' },
  { key: 'amount',          label: 'Amount',          align: 'right', numeric: true },
  { key: 'template_name',   label: 'Rule / Template', align: 'left'  },
];

const fmtAmount = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
};

const slugify = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');

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

/* Numeric-aware comparator so sub-instrument 10 sorts after 9, not after 1. */
const cmp = (a, b, col) => {
  const av = a[col.key], bv = b[col.key];
  if (col.numeric) return (Number(av) || 0) - (Number(bv) || 0);
  if (col.numericAware) {
    const an = Number(av), bn = Number(bv);
    const aNum = Number.isFinite(an), bNum = Number.isFinite(bn);
    if (aNum && bNum) return an - bn;
    if (aNum !== bNum) return aNum ? -1 : 1;
  }
  return String(av ?? '').localeCompare(String(bv ?? ''));
};

export default function TransactionReport() {
  const [rows, setRows] = useState([]);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sortBy, setSortBy] = useState(null);          // null = canonical order
  const [sortDir, setSortDir] = useState('asc');
  const [instrument, setInstrument] = useState('');
  const [template, setTemplate] = useState('');
  const [exportAnchor, setExportAnchor] = useState(null);

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const params = { limit: 20000 };
      if (instrument) params.instrumentid = instrument;
      if (template) params.template_name = template;
      const res = await axios.get(`${API}/transaction-reports`, { params });
      setRows(res.data?.transactions || []);
      setMeta(res.data || null);
    } catch (e) {
      setError(e?.response?.data?.detail || e.message || 'Failed to load transactions');
      setRows([]); setMeta(null);
    } finally {
      setLoading(false);
    }
  }, [instrument, template]);

  useEffect(() => { load(); }, [load]);

  /* The server already returns canonical order; only re-sort when the user
   * clicks a header, so the default view is the report order as specified. */
  const sorted = useMemo(() => {
    if (!sortBy) return rows;
    const col = COLUMNS.find(c => c.key === sortBy);
    if (!col) return rows;
    const out = [...rows].sort((a, b) => cmp(a, b, col));
    return sortDir === 'desc' ? out.reverse() : out;
  }, [rows, sortBy, sortDir]);

  const total = useMemo(
    () => sorted.reduce((s, r) => s + (Number(r.amount) || 0), 0), [sorted]);

  const handleSort = (key) => {
    if (sortBy === key) {
      if (sortDir === 'asc') setSortDir('desc');
      else { setSortBy(null); setSortDir('asc'); }   // third click → canonical
    } else { setSortBy(key); setSortDir('asc'); }
  };

  const exportCsv = () => {
    downloadBlob(toCSV(sorted, COLUMNS.map(c => c.key)),
      `transaction-report-${slugify(instrument) || 'all'}.csv`);
    setExportAnchor(null);
  };
  const exportJson = () => {
    downloadBlob(JSON.stringify({ generated_at: new Date().toISOString(),
      filters: { instrument, template }, total_amount: total,
      row_count: sorted.length, transactions: sorted }, null, 2),
      `transaction-report-${slugify(instrument) || 'all'}.json`, 'application/json');
    setExportAnchor(null);
  };

  const hasFilters = Boolean(instrument || template);

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
          Every transaction across all periods, by instrument and sub-instrument.
        </Typography>

        <TextField
          select size="small" label="Instrument" value={instrument}
          onChange={(e) => setInstrument(e.target.value)}
          sx={{ minWidth: 160, '& .MuiInputBase-root': { fontSize: '0.75rem' } }}
        >
          <SelectItem value="">All instruments</SelectItem>
          {(meta?.filters?.instruments || []).map(i => (
            <SelectItem key={i} value={i}>{i}</SelectItem>
          ))}
        </TextField>

        <TextField
          select size="small" label="Rule / Template" value={template}
          onChange={(e) => setTemplate(e.target.value)}
          sx={{ minWidth: 170, '& .MuiInputBase-root': { fontSize: '0.75rem' } }}
        >
          <SelectItem value="">All rules</SelectItem>
          {(meta?.filters?.templates || []).map(t => (
            <SelectItem key={t} value={t}>{t}</SelectItem>
          ))}
        </TextField>

        {hasFilters && (
          <Tooltip title="Clear filters">
            <IconButton size="small" onClick={() => { setInstrument(''); setTemplate(''); }}>
              <X size={14} />
            </IconButton>
          </Tooltip>
        )}

        <Button size="small" variant="outlined" onClick={load} disabled={loading}
          startIcon={loading ? <CircularProgress size={12} color="inherit" /> : <RotateCcw size={13} />}
          sx={{ textTransform: 'none', fontSize: '0.75rem', borderColor: C.brand, color: C.brand }}>
          {loading ? 'Loading…' : 'Refresh'}
        </Button>

        <Button size="small" variant="contained" disabled={!sorted.length}
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

      {loading && <LinearProgress sx={{ height: 2 }} />}

      {/* Summary strip */}
      {meta && !error && (
        <Stack direction="row" spacing={1} sx={{ px: 2, py: 1, flexWrap: 'wrap', gap: 0.75 }}>
          <Chip size="small" label={`${sorted.length.toLocaleString()} transactions`}
            sx={{ bgcolor: C.brandSoft, color: C.brand, fontWeight: 600 }} />
          <Chip size="small" label={`${meta.summary?.instrument_count ?? 0} instruments`}
            sx={{ bgcolor: C.surface, color: C.body, border: `1px solid ${C.border}` }} />
          <Chip size="small" label={`${meta.summary?.run_count ?? 0} runs`}
            sx={{ bgcolor: C.surface, color: C.body, border: `1px solid ${C.border}` }} />
          <Chip size="small" label={`Total ${fmtAmount(total)}`}
            sx={{ bgcolor: total < 0 ? C.dangerSoft : C.successSoft,
                  color: total < 0 ? C.dangerInk : C.successInk, fontWeight: 700 }} />
          {sortBy && (
            <Chip size="small" icon={<Filter size={12} />} onDelete={() => setSortBy(null)}
              label={`sorted by ${sortBy} ${sortDir}`}
              sx={{ bgcolor: C.surface, border: `1px solid ${C.border}` }} />
          )}
        </Stack>
      )}

      {error && <Alert severity="error" sx={{ mx: 2, mb: 1 }}>{error}</Alert>}

      {meta?.truncated && (
        <Alert severity="info" sx={{ mx: 2, mb: 1, fontSize: '0.75rem' }}>
          Showing the first {meta.returned.toLocaleString()} of {meta.total.toLocaleString()} transactions.
          Filter by instrument to narrow the report.
        </Alert>
      )}

      {/* Table */}
      <Box sx={{ flex: 1, minHeight: 0, px: 2, pb: 2 }}>
        {!loading && !sorted.length && !error ? (
          <Box sx={{ py: 6, textAlign: 'center', color: C.muted }}>
            <Receipt size={28} />
            <Typography variant="body2" sx={{ mt: 1, fontWeight: 600, color: C.body }}>
              No transactions yet
            </Typography>
            <Typography variant="caption">
              Execute a template to generate transactions, then refresh this report.
            </Typography>
          </Box>
        ) : (
          <TableContainer component={Paper} elevation={0}
            sx={{ height: '100%', border: `1px solid ${C.border}`, borderRadius: 1.5 }}>
            <Table stickyHeader size="small" sx={{ '& td, & th': { fontSize: '0.75rem' } }}>
              <TableHead>
                <TableRow>
                  {COLUMNS.map(col => (
                    <TableCell key={col.key} align={col.align}
                      sx={{ fontWeight: 700, color: C.ink, bgcolor: C.brandSoft,
                            whiteSpace: 'nowrap', borderBottom: `2px solid #D6D8FE` }}>
                      <TableSortLabel
                        active={sortBy === col.key}
                        direction={sortBy === col.key ? sortDir : 'asc'}
                        onClick={() => handleSort(col.key)}
                      >
                        {col.label}
                      </TableSortLabel>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {sorted.map((r, i) => (
                  <TableRow key={`${r.instrumentid}-${r.subinstrumentid}-${r.postingdate}-${r.template_name}-${i}`}
                    sx={{ '&:nth-of-type(odd)': { bgcolor: C.zebra },
                          '&:hover': { bgcolor: C.brandSoft } }}>
                    <TableCell sx={{ fontWeight: 600, color: C.ink, whiteSpace: 'nowrap' }}>
                      {r.instrumentid || '—'}
                    </TableCell>
                    <TableCell sx={{ color: C.body }}>{r.subinstrumentid || '—'}</TableCell>
                    <TableCell sx={{ color: C.body, whiteSpace: 'nowrap' }}>{r.postingdate || '—'}</TableCell>
                    <TableCell sx={{ color: C.body, whiteSpace: 'nowrap' }}>{r.effectivedate || '—'}</TableCell>
                    <TableCell>
                      {r.transactiontype
                        ? <Chip size="small" label={r.transactiontype}
                            sx={{ height: 18, fontSize: '0.65rem', bgcolor: C.surface,
                                  border: `1px solid ${C.border}`, color: C.body }} />
                        : '—'}
                    </TableCell>
                    <TableCell align="right" sx={{
                      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
                      fontWeight: 600,
                      color: Number(r.amount) < 0 ? C.danger : C.ink, whiteSpace: 'nowrap',
                    }}>
                      {fmtAmount(r.amount)}
                    </TableCell>
                    <TableCell sx={{ color: C.muted }}>{r.template_name || '—'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
              {sorted.length > 0 && (
                <TableFooter>
                  <TableRow>
                    <TableCell colSpan={5} sx={{ fontWeight: 700, color: C.ink,
                      bgcolor: C.surface, borderTop: `2px solid ${C.border}`, position: 'sticky', bottom: 0 }}>
                      Total — {sorted.length.toLocaleString()} transactions
                    </TableCell>
                    <TableCell align="right" sx={{
                      fontWeight: 800, bgcolor: C.surface, borderTop: `2px solid ${C.border}`,
                      position: 'sticky', bottom: 0,
                      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
                      color: total < 0 ? C.danger : C.ink,
                    }}>
                      {fmtAmount(total)}
                    </TableCell>
                    <TableCell sx={{ bgcolor: C.surface, borderTop: `2px solid ${C.border}`,
                      position: 'sticky', bottom: 0 }} />
                  </TableRow>
                </TableFooter>
              )}
            </Table>
          </TableContainer>
        )}
      </Box>
    </Box>
  );
}

export { DEFAULT_SORT, COLUMNS, toCSV };
