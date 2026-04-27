import React, { useMemo, useRef, useCallback, useState, useEffect } from "react";
import {
  Box, Typography, Card, CardContent, Chip, Alert, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, TableFooter, Tooltip,
  IconButton, CircularProgress, Tabs, Tab, Autocomplete, TextField, Button,
  Menu, MenuItem, ToggleButton, ToggleButtonGroup, Stack, Snackbar,
  TableSortLabel, Fade,
} from "@mui/material";
import {
  Eye, AlertTriangle, CheckCircle2, FileText, DollarSign, Calendar, TrendingUp,
  TrendingDown, Download, ChevronDown, Copy, FileDown, RotateCcw, Filter,
  Layers, ListChecks, LayoutDashboard, Info, XCircle,
} from "lucide-react";
import html2pdf from "html2pdf.js";

/* ──────────────────────────────────────────────────────────────────────────
 * Brand tokens
 * ──────────────────────────────────────────────────────────────────────── */
const C = {
  brand: '#5B5FED',
  brandDark: '#4A4ED0',
  brandSoft: '#EEF0FE',
  ink: '#14213D',
  body: '#495057',
  muted: '#6C757D',
  border: '#E9ECEF',
  surface: '#FFFFFF',
  bg: '#F6F7FB',
  success: '#10B981',
  successSoft: '#E7F8F1',
  successInk: '#065F46',
  danger: '#DC2626',
  dangerSoft: '#FEE2E2',
  dangerInk: '#991B1B',
  warn: '#F59E0B',
  warnSoft: '#FEF3C7',
  warnInk: '#92400E',
  info: '#3B82F6',
  infoSoft: '#DBEAFE',
  infoInk: '#1E40AF',
};

/* ──────────────────────────────────────────────────────────────────────────
 * Helpers
 * ──────────────────────────────────────────────────────────────────────── */
const formatNumber = (val) => {
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'number') {
    if (Number.isInteger(val)) return val.toLocaleString();
    return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  }
  return String(val);
};

const formatCompact = (val) => {
  if (typeof val !== 'number') return formatNumber(val);
  return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const slugify = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');

const toCSV = (rows) => {
  if (!rows || !rows.length) return '';
  const keys = [...new Set(rows.flatMap(r => Object.keys(r)))].filter(k => !String(k).startsWith('_'));
  const escape = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const header = keys.join(',');
  const body = rows.map(r => keys.map(k => escape(r[k])).join(',')).join('\n');
  return `${header}\n${body}`;
};

const downloadBlob = (data, filename, mime = 'text/csv') => {
  const blob = new Blob([data], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
};

/* ──────────────────────────────────────────────────────────────────────────
 * Tiny presentational pieces
 * ──────────────────────────────────────────────────────────────────────── */
const StatPill = ({ tone = 'neutral', children, icon: Icon }) => {
  const palette = {
    neutral: { bg: C.brandSoft, fg: C.brand },
    success: { bg: C.successSoft, fg: C.successInk },
    danger:  { bg: C.dangerSoft, fg: C.dangerInk },
    warn:    { bg: C.warnSoft, fg: C.warnInk },
    info:    { bg: C.infoSoft, fg: C.infoInk },
  }[tone] || { bg: C.brandSoft, fg: C.brand };
  return (
    <Box sx={{
      display: 'inline-flex', alignItems: 'center', gap: 0.5, px: 0.875, py: 0.25,
      borderRadius: 999, bgcolor: palette.bg, color: palette.fg,
      fontSize: '0.6875rem', fontWeight: 600, lineHeight: 1.4,
    }}>
      {Icon && <Icon size={11} />}
      {children}
    </Box>
  );
};

const Sparkline = ({ values = [], color = C.brand, width = 80, height = 22 }) => {
  if (!values || values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const pts = values.map((v, i) => `${(i * step).toFixed(1)},${(height - ((v - min) / range) * height).toFixed(1)}`).join(' ');
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <polyline fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" points={pts} />
    </svg>
  );
};

/* ──────────────────────────────────────────────────────────────────────────
 * Hero KPI cards
 * ──────────────────────────────────────────────────────────────────────── */
const HeroKpis = ({ transactions, schedules, selectedDates = [], dateOptions = [] }) => {
  const kpi = useMemo(() => {
    const debits = transactions.filter(t => (Number(t.amount) || 0) > 0);
    const credits = transactions.filter(t => (Number(t.amount) || 0) < 0);
    const debitTotal = debits.reduce((s, t) => s + Math.abs(Number(t.amount) || 0), 0);
    const creditTotal = credits.reduce((s, t) => s + Math.abs(Number(t.amount) || 0), 0);
    const net = transactions.reduce((s, t) => s + (Number(t.amount) || 0), 0);
    const types = [...new Set(transactions.map(t => t.transactiontype).filter(Boolean))];
    const totalPeriods = schedules.reduce((s, sc) => s + (Array.isArray(sc) ? sc.length : 0), 0);
    return { debits, credits, debitTotal, creditTotal, net, types, totalPeriods };
  }, [transactions, schedules]);

  // Date display — strictly reflects the *currently visible* posting date.
  // When the user has selected exactly one date, show it. When the filtered
  // result narrows down to a single date by itself, show that. Otherwise
  // prompt the user to select one — we never list all dates here.
  const dateDisplay = useMemo(() => {
    // Posting dates actually present in the visible (filtered) data
    const visible = new Set();
    for (const t of transactions || []) {
      const v = String(t?.postingdate ?? '').trim();
      if (v) visible.add(v);
    }
    for (const sched of schedules || []) {
      if (Array.isArray(sched)) {
        for (const r of sched) {
          const v = String(r?._postingdate ?? '').trim();
          if (v) visible.add(v);
        }
      }
    }
    const visibleArr = [...visible].sort();

    // 1) Explicit single selection wins
    if (selectedDates && selectedDates.length === 1) {
      return { value: selectedDates[0], sub: 'Selected posting date' };
    }
    // 2) Data resolves to exactly one date (even without an explicit pick)
    if (visibleArr.length === 1) {
      return { value: visibleArr[0], sub: 'Posting date' };
    }
    // 3) Multiple dates — ask user to pick one, no enumeration
    if (visibleArr.length > 1) {
      return {
        value: 'Select a date',
        sub: `${visibleArr.length} posting dates available`,
      };
    }
    // 4) Nothing
    return { value: '—', sub: 'No posting dates' };
  }, [selectedDates, transactions, schedules]);

  const drCrTotal = kpi.debitTotal + kpi.creditTotal || 1;
  const drPct = (kpi.debitTotal / drCrTotal) * 100;

  const cards = [
    {
      key: 'net',
      label: 'Net Amount',
      value: formatCompact(kpi.net),
      sub: `${kpi.debits.length} debit · ${kpi.credits.length} credit`,
      icon: DollarSign,
      tone: kpi.net >= 0 ? 'success' : 'danger',
      extra: transactions.length > 0 ? (
        <Box sx={{ mt: 1, height: 4, borderRadius: 2, bgcolor: C.dangerSoft, overflow: 'hidden', display: 'flex' }}>
          <Box sx={{ width: `${drPct}%`, bgcolor: C.success }} />
          <Box sx={{ width: `${100 - drPct}%`, bgcolor: C.danger }} />
        </Box>
      ) : null,
    },
    {
      key: 'txns',
      label: 'Transactions',
      value: transactions.length.toLocaleString(),
      sub: kpi.types.length ? `${kpi.types.length} type${kpi.types.length === 1 ? '' : 's'}` : 'No transactions',
      icon: FileText,
      tone: 'info',
    },
    {
      key: 'date',
      label: 'Posting Date',
      value: dateDisplay.value,
      sub: dateDisplay.sub,
      icon: Calendar,
      tone: 'neutral',
      mono: /^\d/.test(String(dateDisplay.value || '')),
    },
  ];

  const palette = {
    success: { bg: C.successSoft, ic: C.success, ink: C.successInk },
    danger:  { bg: C.dangerSoft, ic: C.danger, ink: C.dangerInk },
    info:    { bg: C.infoSoft, ic: C.info, ink: C.infoInk },
    neutral: { bg: C.brandSoft, ic: C.brand, ink: C.brand },
  };

  return (
    <Box sx={{
      display: 'grid', gap: 1.5, mb: 2,
      gridTemplateColumns: { xs: '1fr', sm: 'repeat(3, 1fr)' },
    }}>
      {cards.map(c => {
        const p = palette[c.tone];
        return (
          <Card key={c.key} variant="outlined" sx={{ borderColor: C.border, borderRadius: 2, transition: 'box-shadow .15s', '&:hover': { boxShadow: '0 2px 8px rgba(20,33,61,0.06)' } }}>
            <CardContent sx={{ p: 1.75, '&:last-child': { pb: 1.75 } }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.75 }}>
                <Box sx={{
                  width: 28, height: 28, borderRadius: 1.5, display: 'flex',
                  alignItems: 'center', justifyContent: 'center', bgcolor: p.bg, color: p.ic,
                }}>
                  <c.icon size={15} />
                </Box>
                <Typography variant="caption" sx={{ color: C.muted, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.4, fontSize: '0.6875rem' }}>
                  {c.label}
                </Typography>
              </Box>
              <Typography sx={{
                color: C.ink, fontWeight: 700, lineHeight: 1.15,
                fontSize: c.mono === false ? '1.25rem' : '1.5rem',
                fontFamily: c.mono === false ? 'inherit' : 'monospace',
              }}>
                {c.value}
              </Typography>
              <Typography variant="caption" sx={{ color: C.muted, display: 'block', mt: 0.25 }}>
                {c.sub}
              </Typography>
              {c.extra}
            </CardContent>
          </Card>
        );
      })}
    </Box>
  );
};

/* ──────────────────────────────────────────────────────────────────────────
 * Schedule card with totals, sparkline, copy/CSV
 * ──────────────────────────────────────────────────────────────────────── */
const ScheduleCard = ({ data, title, density, onCopy, defaultMaxRows = 12 }) => {
  const [showAll, setShowAll] = useState(false);
  const allKeys = useMemo(() => (
    [...new Set((data || []).flatMap(o => Object.keys(o)))].filter(k => !String(k).startsWith('_'))
  ), [data]);

  const numericKeys = useMemo(() => (
    allKeys.filter(k => (data || []).some(r => typeof r[k] === 'number'))
  ), [allKeys, data]);

  const totals = useMemo(() => {
    const sums = {};
    for (const k of numericKeys) {
      sums[k] = (data || []).reduce((s, r) => s + (typeof r[k] === 'number' ? r[k] : 0), 0);
    }
    return sums;
  }, [data, numericKeys]);

  const sparkValues = useMemo(() => {
    const k = numericKeys[0];
    if (!k) return [];
    return (data || []).map(r => (typeof r[k] === 'number' ? r[k] : 0));
  }, [data, numericKeys]);

  if (!data || !Array.isArray(data) || data.length === 0) return null;

  const rows = showAll ? data : data.slice(0, defaultMaxRows);
  const hasMore = data.length > defaultMaxRows;
  const cellPy = density === 'compact' ? 0.25 : 0.625;

  const handleCopy = async () => {
    try {
      const csv = toCSV(data);
      await navigator.clipboard.writeText(csv);
      onCopy?.(`Copied ${title} (${data.length} rows) to clipboard`);
    } catch { /* ignore */ }
  };
  const handleCsv = () => {
    downloadBlob(toCSV(data), `${slugify(title) || 'schedule'}.csv`);
  };

  return (
    <Card variant="outlined" sx={{ borderColor: C.border, borderRadius: 2, mb: 1.5 }}>
      <Box sx={{
        display: 'flex', alignItems: 'center', gap: 1, px: 1.5, py: 1,
        borderBottom: `1px solid ${C.border}`, bgcolor: '#FAFBFD',
      }}>
        <Calendar size={14} color={C.brand} />
        <Typography variant="body2" fontWeight={700} color={C.ink} sx={{ flex: 1 }}>{title}</Typography>
        <StatPill>{data.length} periods</StatPill>
        {sparkValues.length > 1 && (
          <Box sx={{ display: { xs: 'none', md: 'block' }, opacity: 0.85 }}>
            <Sparkline values={sparkValues} />
          </Box>
        )}
        <Tooltip title="Copy CSV to clipboard">
          <IconButton size="small" onClick={handleCopy} sx={{ color: C.muted, '&:hover': { color: C.brand } }}>
            <Copy size={14} />
          </IconButton>
        </Tooltip>
        <Tooltip title="Download CSV">
          <IconButton size="small" onClick={handleCsv} sx={{ color: C.muted, '&:hover': { color: C.brand } }}>
            <FileDown size={14} />
          </IconButton>
        </Tooltip>
      </Box>
      <TableContainer sx={{ maxHeight: 420 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', color: C.muted, position: 'sticky', left: 0, zIndex: 3, minWidth: 36 }}>#</TableCell>
              {allKeys.map(k => (
                <TableCell key={k} align={numericKeys.includes(k) ? 'right' : 'left'}
                  sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', color: C.body, whiteSpace: 'nowrap' }}>
                  {k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map((row, i) => (
              <TableRow key={i} hover>
                <TableCell sx={{ fontSize: '0.75rem', py: cellPy, color: C.muted, position: 'sticky', left: 0, bgcolor: '#fff', zIndex: 1 }}>{i + 1}</TableCell>
                {allKeys.map(k => {
                  const v = row[k];
                  const isNum = typeof v === 'number';
                  return (
                    <TableCell key={k} align={isNum ? 'right' : 'left'}
                      sx={{
                        fontSize: '0.75rem', py: cellPy, whiteSpace: 'nowrap',
                        color: isNum ? C.ink : C.body,
                        fontWeight: isNum ? 500 : 400,
                        fontFamily: isNum ? 'monospace' : 'inherit',
                      }}>
                      {formatNumber(v)}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
          </TableBody>
          {numericKeys.length > 0 && (
            <TableFooter>
              <TableRow sx={{ '& td': { borderTop: `2px solid ${C.border}`, bgcolor: '#FAFBFD', position: 'sticky', bottom: 0 } }}>
                <TableCell sx={{ fontSize: '0.75rem', fontWeight: 700, color: C.ink }}>Total</TableCell>
                {allKeys.map(k => (
                  <TableCell key={k} align={numericKeys.includes(k) ? 'right' : 'left'}
                    sx={{ fontSize: '0.75rem', fontWeight: 700, color: C.ink, fontFamily: numericKeys.includes(k) ? 'monospace' : 'inherit' }}>
                    {numericKeys.includes(k) ? formatNumber(totals[k]) : ''}
                  </TableCell>
                ))}
              </TableRow>
            </TableFooter>
          )}
        </Table>
      </TableContainer>
      {hasMore && (
        <Box sx={{ px: 1.5, py: 0.75, borderTop: `1px solid ${C.border}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="caption" color={C.muted}>
            {showAll ? `Showing all ${data.length} periods` : `Showing ${defaultMaxRows} of ${data.length} periods`}
          </Typography>
          <Button size="small" onClick={() => setShowAll(s => !s)} sx={{ textTransform: 'none', fontSize: '0.75rem', color: C.brand }}>
            {showAll ? 'Show less' : `Show all (${data.length})`}
          </Button>
        </Box>
      )}
    </Card>
  );
};

/* ──────────────────────────────────────────────────────────────────────────
 * Transactions table with sort, totals, running balance, DR/CR
 * ──────────────────────────────────────────────────────────────────────── */
const TransactionsCard = ({ transactions, density, onCopy }) => {
  const [orderBy, setOrderBy] = useState('postingdate');
  const [order, setOrder] = useState('asc');

  const sorted = useMemo(() => {
    const arr = [...transactions];
    arr.sort((a, b) => {
      const av = a[orderBy]; const bv = b[orderBy];
      if (av === bv) return 0;
      if (av === undefined || av === null) return 1;
      if (bv === undefined || bv === null) return -1;
      if (typeof av === 'number' && typeof bv === 'number') return order === 'asc' ? av - bv : bv - av;
      return order === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [transactions, orderBy, order]);

  // Running balance respects current sort order
  const withBalance = useMemo(() => {
    let bal = 0;
    return sorted.map(t => {
      bal += Number(t.amount) || 0;
      return { ...t, _runningBalance: bal };
    });
  }, [sorted]);

  const totals = useMemo(() => {
    const debit = transactions.filter(t => (Number(t.amount) || 0) > 0).reduce((s, t) => s + Math.abs(Number(t.amount) || 0), 0);
    const credit = transactions.filter(t => (Number(t.amount) || 0) < 0).reduce((s, t) => s + Math.abs(Number(t.amount) || 0), 0);
    const net = transactions.reduce((s, t) => s + (Number(t.amount) || 0), 0);
    return { debit, credit, net };
  }, [transactions]);

  const handleSort = (col) => {
    if (orderBy === col) setOrder(o => (o === 'asc' ? 'desc' : 'asc'));
    else { setOrderBy(col); setOrder('asc'); }
  };

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(toCSV(transactions));
      onCopy?.(`Copied ${transactions.length} transactions to clipboard`);
    } catch { /* ignore */ }
  };
  const handleCsv = () => downloadBlob(toCSV(transactions), 'transactions.csv');

  if (!transactions || transactions.length === 0) return null;

  const cellPy = density === 'compact' ? 0.25 : 0.625;
  const head = (col, label, align = 'left') => (
    <TableCell align={align} sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', color: C.body, whiteSpace: 'nowrap' }}>
      <TableSortLabel active={orderBy === col} direction={orderBy === col ? order : 'asc'} onClick={() => handleSort(col)}>
        {label}
      </TableSortLabel>
    </TableCell>
  );

  return (
    <Card variant="outlined" sx={{ borderColor: C.border, borderRadius: 2, mb: 1.5 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 1.5, py: 1, borderBottom: `1px solid ${C.border}`, bgcolor: '#FAFBFD' }}>
        <FileText size={14} color={C.brand} />
        <Typography variant="body2" fontWeight={700} color={C.ink} sx={{ flex: 1 }}>Transactions</Typography>
        <StatPill tone="success">{transactions.length} entries</StatPill>
        <Tooltip title="Copy CSV to clipboard">
          <IconButton size="small" onClick={handleCopy} sx={{ color: C.muted, '&:hover': { color: C.brand } }}>
            <Copy size={14} />
          </IconButton>
        </Tooltip>
        <Tooltip title="Download CSV">
          <IconButton size="small" onClick={handleCsv} sx={{ color: C.muted, '&:hover': { color: C.brand } }}>
            <FileDown size={14} />
          </IconButton>
        </Tooltip>
      </Box>
      <TableContainer sx={{ maxHeight: 480 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              {head('postingdate', 'Posting Date')}
              {head('effectivedate', 'Effective Date')}
              {head('transactiontype', 'Type')}
              {head('instrumentid', 'Instrument')}
              {head('subinstrumentid', 'Sub')}
              {head('amount', 'Amount', 'right')}
              <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', color: C.body }}>Running</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {withBalance.map((t, i) => {
              const amt = Number(t.amount) || 0;
              const isDr = amt > 0;
              const isCr = amt < 0;
              return (
                <TableRow key={i} hover>
                  <TableCell sx={{ fontSize: '0.75rem', py: cellPy }}>{t.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: cellPy }}>{t.effectivedate || t.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: cellPy }}>
                    <Chip label={t.transactiontype || 'Unknown'} size="small"
                      sx={{ fontSize: '0.6875rem', height: 18, bgcolor: C.brandSoft, color: C.brand }} />
                  </TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: cellPy, color: C.muted, fontFamily: 'monospace' }}>{t.instrumentid || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: cellPy, color: C.muted, fontFamily: 'monospace' }}>{t.subinstrumentid || '1'}</TableCell>
                  <TableCell align="right" sx={{ fontSize: '0.75rem', py: cellPy, fontFamily: 'monospace', fontWeight: 600 }}>
                    <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, color: isDr ? C.successInk : isCr ? C.dangerInk : C.ink }}>
                      {isDr && <TrendingUp size={12} />}
                      {isCr && <TrendingDown size={12} />}
                      {formatNumber(Math.abs(amt))}
                    </Box>
                  </TableCell>
                  <TableCell align="right" sx={{ fontSize: '0.75rem', py: cellPy, fontFamily: 'monospace', color: C.muted }}>
                    {formatNumber(t._runningBalance)}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
          <TableFooter>
            <TableRow sx={{ '& td': { borderTop: `2px solid ${C.border}`, bgcolor: '#FAFBFD', position: 'sticky', bottom: 0 } }}>
              <TableCell colSpan={5} sx={{ fontSize: '0.75rem', fontWeight: 700, color: C.ink }}>
                Totals · DR {formatNumber(totals.debit)} · CR {formatNumber(totals.credit)}
              </TableCell>
              <TableCell align="right" sx={{ fontSize: '0.75rem', fontWeight: 700, fontFamily: 'monospace', color: C.ink }}>
                {formatNumber(totals.net)}
              </TableCell>
              <TableCell />
            </TableRow>
          </TableFooter>
        </Table>
      </TableContainer>
    </Card>
  );
};

/* ──────────────────────────────────────────────────────────────────────────
 * Checks tab — grouped warnings
 * ──────────────────────────────────────────────────────────────────────── */
const ChecksPanel = ({ warnings }) => {
  const [onlyErrors, setOnlyErrors] = useState(false);
  const groups = useMemo(() => {
    const g = { error: [], warning: [], info: [], success: [] };
    for (const w of warnings || []) {
      const sev = w.severity || 'info';
      (g[sev] || g.info).push(w);
    }
    return g;
  }, [warnings]);

  const groupConfig = [
    { key: 'error', label: 'Errors', icon: XCircle, tone: 'danger' },
    { key: 'warning', label: 'Warnings', icon: AlertTriangle, tone: 'warn' },
    { key: 'info', label: 'Information', icon: Info, tone: 'info' },
    { key: 'success', label: 'Success', icon: CheckCircle2, tone: 'success' },
  ];

  if (!warnings || warnings.length === 0) {
    return (
      <Box sx={{ textAlign: 'center', py: 5, color: C.muted }}>
        <CheckCircle2 size={32} color={C.success} />
        <Typography variant="body2" sx={{ mt: 1 }}>No checks to report.</Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', mb: 1 }}>
        <ToggleButtonGroup size="small" exclusive value={onlyErrors ? 'errors' : 'all'}
          onChange={(_, v) => v && setOnlyErrors(v === 'errors')}>
          <ToggleButton value="all" sx={{ textTransform: 'none', fontSize: '0.75rem', px: 1.25, py: 0.25 }}>All</ToggleButton>
          <ToggleButton value="errors" sx={{ textTransform: 'none', fontSize: '0.75rem', px: 1.25, py: 0.25 }}>Errors only</ToggleButton>
        </ToggleButtonGroup>
      </Box>
      {groupConfig.map(g => {
        if (onlyErrors && g.key !== 'error') return null;
        const items = groups[g.key] || [];
        if (!items.length) return null;
        return (
          <Box key={g.key} sx={{ mb: 1.5 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
              <g.icon size={14} color={g.tone === 'danger' ? C.danger : g.tone === 'warn' ? C.warn : g.tone === 'success' ? C.success : C.info} />
              <Typography variant="body2" fontWeight={700} color={C.ink}>{g.label}</Typography>
              <StatPill tone={g.tone}>{items.length}</StatPill>
            </Box>
            <Stack spacing={0.75}>
              {items.map((w, i) => (
                <Alert key={i} severity={g.key === 'success' ? 'success' : g.key} variant="outlined"
                  sx={{ fontSize: '0.8125rem', '& .MuiAlert-message': { fontSize: '0.8125rem' }, borderRadius: 1.5 }}>
                  {w.message}
                </Alert>
              ))}
            </Stack>
          </Box>
        );
      })}
    </Box>
  );
};

/* ──────────────────────────────────────────────────────────────────────────
 * Empty state
 * ──────────────────────────────────────────────────────────────────────── */
const EmptyState = () => (
  <Card variant="outlined" sx={{ borderColor: C.border, borderRadius: 3, p: 4, textAlign: 'center', bgcolor: '#fff' }}>
    <Box sx={{
      width: 56, height: 56, borderRadius: '50%', mx: 'auto', mb: 2,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      bgcolor: C.brandSoft, color: C.brand,
    }}>
      <Eye size={26} />
    </Box>
    <Typography variant="subtitle1" fontWeight={700} color={C.ink} sx={{ mb: 0.5 }}>
      Nothing to preview yet
    </Typography>
    <Typography variant="body2" color={C.muted} sx={{ mb: 2.5, maxWidth: 360, mx: 'auto' }}>
      Run a calculation to see a business‑readable view of what your DSL produced.
    </Typography>
    <Stack spacing={1} sx={{ maxWidth: 320, mx: 'auto', textAlign: 'left' }}>
      {[
        'A summary of generated transactions',
        'Each schedule as a table with totals',
        'Validation checks that flag suspicious values',
      ].map((t, i) => (
        <Box key={i} sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
          <Box sx={{ mt: 0.25, width: 16, height: 16, borderRadius: '50%', bgcolor: C.brandSoft, color: C.brand, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700 }}>
            {i + 1}
          </Box>
          <Typography variant="body2" color={C.body}>{t}</Typography>
        </Box>
      ))}
    </Stack>
  </Card>
);

/* ──────────────────────────────────────────────────────────────────────────
 * Main component
 * ──────────────────────────────────────────────────────────────────────── */
const useFacetOptions = (transactions, schedules, txnKey, schedKey) => useMemo(() => {
  const set = new Set();
  for (const t of (transactions || [])) {
    const v = String(t?.[txnKey] ?? '').trim();
    if (v) set.add(v);
  }
  for (const sched of (schedules || [])) {
    if (Array.isArray(sched)) {
      for (const row of sched) {
        const v = String(row?.[schedKey] ?? '').trim();
        if (v) set.add(v);
      }
    }
  }
  return [...set].sort();
}, [transactions, schedules, txnKey, schedKey]);

/**
 * LivePreview — Business-meaningful preview of DSL execution results.
 * Tabs over Overview / Schedules / Transactions / Checks, with sticky filters,
 * KPI hero strip, and per-table copy/CSV/PDF export.
 */
const LivePreview = ({
  consoleOutput = [],
  transactions = [],
  schedules = [],
  warnings = [],
  visible = true,
  templateName = '',
}) => {
  const contentRef = useRef(null);
  const [exporting, setExporting] = useState(false);
  const [exportAnchor, setExportAnchor] = useState(null);
  const [tab, setTab] = useState('overview');
  const [density, setDensity] = useState('comfortable');
  const [selectedInstruments, setSelectedInstruments] = useState([]);
  const [selectedDates, setSelectedDates] = useState([]);
  const [toast, setToast] = useState('');

  // Extract schedules from `print` console output
  const extractedSchedules = useMemo(() => {
    if (schedules && schedules.length > 0) return schedules;
    const result = [];
    for (const log of consoleOutput) {
      if (log.type !== 'print') continue;
      try {
        const parsed = JSON.parse(log.message);
        if (Array.isArray(parsed)) {
          if (parsed.every(x => x && typeof x === 'object' && 'schedule' in x)) {
            parsed.forEach(r => { if (Array.isArray(r.schedule)) result.push(r.schedule); });
          } else if (parsed.every(item => Array.isArray(item) && item.length > 0 && typeof item[0] === 'object')) {
            parsed.forEach(s => result.push(s));
          } else if (parsed.length > 0 && typeof parsed[0] === 'object' && !Array.isArray(parsed[0])) {
            result.push(parsed);
          }
        }
      } catch { /* not JSON */ }
    }
    return result;
  }, [consoleOutput, schedules]);

  // Validation derived from data
  const computedWarnings = useMemo(() => {
    const w = [...(warnings || [])];
    if (transactions.length > 0) {
      const zero = transactions.filter(t => (Number(t.amount) || 0) === 0);
      if (zero.length > 0) {
        w.push({ message: `${zero.length} transaction(s) have a zero amount — is that expected?`, severity: 'warning' });
      }
      const dates = [...new Set(transactions.map(t => t.postingdate))];
      if (dates.length > 1) {
        w.push({ message: `Transactions span ${dates.length} different posting dates.`, severity: 'info' });
      }
    }
    for (const sched of extractedSchedules) {
      if (Array.isArray(sched) && sched.length) {
        const numCols = Object.keys(sched[0] || {}).filter(k => typeof sched[0][k] === 'number');
        for (const col of numCols) {
          const zeros = sched.filter(r => r[col] === 0).length;
          if (zeros > sched.length * 0.5 && zeros > 2) {
            w.push({ message: `Schedule column "${col}" has ${zeros} zero-value periods out of ${sched.length} — check your formula.`, severity: 'warning' });
          }
        }
      }
    }
    if (transactions.length > 0) {
      w.push({ message: `Execution produced ${transactions.length} transaction(s) successfully.`, severity: 'success' });
    }
    return w;
  }, [transactions, extractedSchedules, warnings]);

  const errorCount = computedWarnings.filter(w => w.severity === 'error').length;
  const hasContent = extractedSchedules.length > 0 || transactions.length > 0;

  // Facet options
  const instrumentOptions = useFacetOptions(transactions, extractedSchedules, 'instrumentid', '_instrumentid');
  const dateOptions = useFacetOptions(transactions, extractedSchedules, 'postingdate', '_postingdate');

  // Drop stale selections — only update state if the filtered list actually
  // differs, otherwise we create a new array reference every render and
  // trigger an infinite update loop.
  useEffect(() => {
    setSelectedInstruments(prev => {
      const next = prev.filter(v => instrumentOptions.includes(v));
      return next.length === prev.length ? prev : next;
    });
  }, [instrumentOptions]);
  useEffect(() => {
    setSelectedDates(prev => {
      const next = prev.filter(v => dateOptions.includes(v));
      return next.length === prev.length ? prev : next;
    });
  }, [dateOptions]);

  const filtersActive = selectedInstruments.length > 0 || selectedDates.length > 0;

  // Apply filters
  const filteredTransactions = useMemo(() => (
    (transactions || []).filter(t => {
      if (selectedInstruments.length && !selectedInstruments.includes(String(t.instrumentid ?? ''))) return false;
      if (selectedDates.length && !selectedDates.includes(String(t.postingdate ?? ''))) return false;
      return true;
    })
  ), [transactions, selectedInstruments, selectedDates]);

  const filteredSchedules = useMemo(() => {
    if (!filtersActive) return extractedSchedules;
    return extractedSchedules.filter(sched => {
      if (!Array.isArray(sched) || sched.length === 0) return false;
      return sched.some(row => {
        if (selectedInstruments.length) {
          if (row?._instrumentid === undefined) return false;
          if (!selectedInstruments.includes(String(row._instrumentid))) return false;
        }
        if (selectedDates.length) {
          if (row?._postingdate === undefined) return false;
          if (!selectedDates.includes(String(row._postingdate))) return false;
        }
        return true;
      });
    });
  }, [extractedSchedules, selectedInstruments, selectedDates, filtersActive]);

  const handleResetFilters = () => {
    setSelectedInstruments([]);
    setSelectedDates([]);
  };

  // Export menu
  const openExport = (e) => setExportAnchor(e.currentTarget);
  const closeExport = () => setExportAnchor(null);

  const handleExportPDF = useCallback(async () => {
    closeExport();
    if (!contentRef.current) return;
    setExporting(true);
    try {
      const slug = slugify(templateName || 'business-preview');
      await html2pdf().set({
        margin: [12, 10, 12, 10],
        filename: `${slug}-preview.pdf`,
        image: { type: 'jpeg', quality: 0.98 },
        html2canvas: { scale: 2, useCORS: true, logging: false },
        jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' },
        pagebreak: { mode: ['avoid-all', 'css', 'legacy'] },
      }).from(contentRef.current).save();
    } catch (err) {
      console.error('PDF export failed:', err);
    } finally { setExporting(false); }
  }, [templateName]);

  const handleExportCSV = useCallback(() => {
    closeExport();
    if (filteredTransactions.length) {
      downloadBlob(toCSV(filteredTransactions), `${slugify(templateName) || 'transactions'}.csv`);
    }
    filteredSchedules.forEach((sc, i) => {
      downloadBlob(toCSV(sc), `${slugify(templateName) || 'schedule'}-${i + 1}.csv`);
    });
  }, [filteredTransactions, filteredSchedules, templateName]);

  const handleExportJSON = useCallback(() => {
    closeExport();
    const payload = {
      templateName, generatedAt: new Date().toISOString(),
      transactions: filteredTransactions, schedules: filteredSchedules,
      warnings: computedWarnings,
    };
    downloadBlob(JSON.stringify(payload, null, 2),
      `${slugify(templateName) || 'business-preview'}.json`, 'application/json');
  }, [templateName, filteredTransactions, filteredSchedules, computedWarnings]);

  if (!visible) return null;

  const totalTxns = filteredTransactions.length;
  const totalScheds = filteredSchedules.length;

  return (
    <Box sx={{ bgcolor: C.bg, height: '100%', overflowY: 'auto' }} data-testid="live-preview">
      {/* ── Header ─────────────────────────────────────────── */}
      <Box sx={{
        position: 'sticky', top: 0, zIndex: 10, bgcolor: C.surface,
        borderBottom: `1px solid ${C.border}`, px: 2, py: 1.25,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25 }}>
          <Box sx={{
            width: 32, height: 32, borderRadius: 1.5, display: 'flex',
            alignItems: 'center', justifyContent: 'center',
            bgcolor: C.brandSoft, color: C.brand,
          }}>
            <Eye size={17} />
          </Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="subtitle1" fontWeight={700} color={C.ink} sx={{ lineHeight: 1.2, fontSize: '0.9375rem' }}>
              Business Preview
            </Typography>
            {hasContent && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mt: 0.25, flexWrap: 'wrap' }}>
                <Typography variant="caption" color={C.muted}>
                  Generated {new Date().toLocaleString()}
                </Typography>
                <Box component="span" sx={{ color: C.border }}>·</Box>
                <Typography variant="caption" color={C.muted}>
                  {totalTxns} txn{totalTxns === 1 ? '' : 's'}
                </Typography>
                <Box component="span" sx={{ color: C.border }}>·</Box>
                <Typography variant="caption" color={C.muted}>
                  {totalScheds} schedule{totalScheds === 1 ? '' : 's'}
                </Typography>
                {errorCount > 0 && (
                  <>
                    <Box component="span" sx={{ color: C.border }}>·</Box>
                    <StatPill tone="danger" icon={XCircle}>{errorCount} error{errorCount === 1 ? '' : 's'}</StatPill>
                  </>
                )}
              </Box>
            )}
          </Box>

          <ToggleButtonGroup size="small" exclusive value={density}
            onChange={(_, v) => v && setDensity(v)} sx={{ display: { xs: 'none', sm: 'flex' } }}>
            <ToggleButton value="comfortable" sx={{ textTransform: 'none', fontSize: '0.75rem', px: 1, py: 0.25 }}>
              Comfortable
            </ToggleButton>
            <ToggleButton value="compact" sx={{ textTransform: 'none', fontSize: '0.75rem', px: 1, py: 0.25 }}>
              Compact
            </ToggleButton>
          </ToggleButtonGroup>

          {hasContent && (
            <>
              <Tooltip title="Export">
                <span>
                  <IconButton
                    size="small"
                    onClick={openExport}
                    disabled={exporting}
                    sx={{ color: C.brand, '&:hover': { bgcolor: C.brandSoft } }}
                  >
                    {exporting ? <CircularProgress size={16} /> : <Download size={16} />}
                  </IconButton>
                </span>
              </Tooltip>
              <Menu anchorEl={exportAnchor} open={Boolean(exportAnchor)} onClose={closeExport}
                anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                transformOrigin={{ vertical: 'top', horizontal: 'right' }}>
                <MenuItem onClick={handleExportPDF} sx={{ fontSize: '0.8125rem', gap: 1 }}>
                  <FileText size={14} /> PDF
                </MenuItem>
                <MenuItem onClick={handleExportCSV} sx={{ fontSize: '0.8125rem', gap: 1 }}>
                  <FileDown size={14} /> CSV (per table)
                </MenuItem>
                <MenuItem onClick={handleExportJSON} sx={{ fontSize: '0.8125rem', gap: 1 }}>
                  <FileDown size={14} /> JSON
                </MenuItem>
              </Menu>
            </>
          )}
        </Box>
      </Box>

      {/* ── Error banner ──────────────────────────────────── */}
      {errorCount > 0 && (
        <Fade in>
          <Alert
            severity="error"
            icon={<XCircle size={18} />}
            action={<Button size="small" color="inherit" onClick={() => setTab('checks')} sx={{ textTransform: 'none' }}>Review</Button>}
            sx={{ borderRadius: 0, alignItems: 'center' }}
          >
            <strong>{errorCount}</strong> {errorCount === 1 ? 'error' : 'errors'} found in this run.
          </Alert>
        </Fade>
      )}

      <Box sx={{ p: 2 }}>
        {/* ── Empty state ────────────────────────────────── */}
        {!hasContent && computedWarnings.filter(w => w.severity !== 'success').length === 0 && (
          <EmptyState />
        )}

        {(hasContent || computedWarnings.filter(w => w.severity !== 'success').length > 0) && (
          <Box ref={contentRef}>
            {/* Hero KPIs */}
            <HeroKpis
              transactions={filteredTransactions}
              schedules={filteredSchedules}
              selectedDates={selectedDates}
              dateOptions={dateOptions}
            />

            {/* Filter bar */}
            {(instrumentOptions.length > 1 || dateOptions.length > 1) && (
              <Card variant="outlined" sx={{ borderColor: C.border, borderRadius: 2, mb: 2, p: 1.25 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <Filter size={13} color={C.muted} />
                  <Typography variant="caption" sx={{ color: C.muted, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 0.4 }}>
                    Filters
                  </Typography>
                  <Box sx={{ flex: 1 }} />
                  <Typography variant="caption" color={C.muted}>
                    Showing {totalTxns} of {transactions.length} txns · {totalScheds} of {extractedSchedules.length} schedules
                  </Typography>
                  {filtersActive && (
                    <Button size="small" startIcon={<RotateCcw size={12} />} onClick={handleResetFilters}
                      sx={{ textTransform: 'none', fontSize: '0.75rem', color: C.muted }}>
                      Reset
                    </Button>
                  )}
                </Box>
                <Box sx={{ display: 'grid', gap: 1, gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' } }}>
                  {instrumentOptions.length > 1 && (
                    <Autocomplete
                      multiple size="small" options={instrumentOptions}
                      value={selectedInstruments}
                      onChange={(_, v) => setSelectedInstruments(v)}
                      renderInput={(p) => <TextField {...p} label="Instrument" placeholder="All" />}
                      ChipProps={{ size: 'small', sx: { fontFamily: 'monospace' } }}
                    />
                  )}
                  {dateOptions.length > 1 && (
                    <Autocomplete
                      multiple size="small" options={dateOptions}
                      value={selectedDates}
                      onChange={(_, v) => setSelectedDates(v)}
                      renderInput={(p) => <TextField {...p} label="Posting Date" placeholder="All" />}
                      ChipProps={{ size: 'small', sx: { fontFamily: 'monospace' } }}
                    />
                  )}
                </Box>
              </Card>
            )}

            {/* Tabs */}
            <Tabs
              value={tab}
              onChange={(_, v) => setTab(v)}
              sx={{
                minHeight: 36, mb: 2,
                '& .MuiTab-root': { minHeight: 36, textTransform: 'none', fontSize: '0.8125rem', fontWeight: 600, color: C.muted },
                '& .Mui-selected': { color: `${C.brand} !important` },
                '& .MuiTabs-indicator': { backgroundColor: C.brand, height: 2 },
                borderBottom: `1px solid ${C.border}`,
              }}
            >
              <Tab value="overview" iconPosition="start"
                icon={<LayoutDashboard size={14} style={{ marginRight: 6 }} />}
                label="Overview" />
              <Tab value="schedules" iconPosition="start"
                icon={<Layers size={14} style={{ marginRight: 6 }} />}
                label={`Schedules · ${filteredSchedules.length}`} />
              <Tab value="transactions" iconPosition="start"
                icon={<FileText size={14} style={{ marginRight: 6 }} />}
                label={`Transactions · ${filteredTransactions.length}`} />
              <Tab value="checks" iconPosition="start"
                icon={<ListChecks size={14} style={{ marginRight: 6 }} />}
                label={`Checks · ${computedWarnings.length}`} />
            </Tabs>

            {/* Tab panels */}
            {tab === 'overview' && (
              <Box>
                {filteredSchedules.length > 0 && (
                  <ScheduleCard
                    title={filteredSchedules.length > 1 ? 'Schedule 1 (preview)' : 'Schedule'}
                    data={filteredSchedules[0]}
                    density={density}
                    onCopy={setToast}
                    defaultMaxRows={6}
                  />
                )}
                {filteredTransactions.length > 0 && (
                  <TransactionsCard
                    transactions={filteredTransactions.slice(0, 8)}
                    density={density}
                    onCopy={setToast}
                  />
                )}
                {filteredSchedules.length === 0 && filteredTransactions.length === 0 && (
                  <Typography variant="body2" color={C.muted} sx={{ p: 2, textAlign: 'center' }}>
                    No data matches the current filters.
                  </Typography>
                )}
                {(filteredSchedules.length > 1 || filteredTransactions.length > 8) && (
                  <Box sx={{ display: 'flex', justifyContent: 'center', mt: 1 }}>
                    <Button size="small" onClick={() => setTab(filteredSchedules.length > 1 ? 'schedules' : 'transactions')}
                      sx={{ textTransform: 'none', color: C.brand, fontSize: '0.8125rem' }}>
                      View full {filteredSchedules.length > 1 ? 'schedules' : 'transactions'} →
                    </Button>
                  </Box>
                )}
              </Box>
            )}

            {tab === 'schedules' && (
              <Box>
                {filteredSchedules.length === 0 ? (
                  <Typography variant="body2" color={C.muted} sx={{ p: 2, textAlign: 'center' }}>
                    No schedules to display.
                  </Typography>
                ) : (
                  filteredSchedules.map((sched, idx) => (
                    <ScheduleCard
                      key={idx}
                      title={filteredSchedules.length > 1 ? `Schedule ${idx + 1}` : 'Schedule'}
                      data={sched}
                      density={density}
                      onCopy={setToast}
                    />
                  ))
                )}
              </Box>
            )}

            {tab === 'transactions' && (
              filteredTransactions.length === 0 ? (
                <Typography variant="body2" color={C.muted} sx={{ p: 2, textAlign: 'center' }}>
                  No transactions to display.
                </Typography>
              ) : (
                <TransactionsCard transactions={filteredTransactions} density={density} onCopy={setToast} />
              )
            )}

            {tab === 'checks' && <ChecksPanel warnings={computedWarnings} />}
          </Box>
        )}
      </Box>

      <Snackbar
        open={Boolean(toast)}
        onClose={() => setToast('')}
        autoHideDuration={2200}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        message={toast}
      />
    </Box>
  );
};

export default LivePreview;
