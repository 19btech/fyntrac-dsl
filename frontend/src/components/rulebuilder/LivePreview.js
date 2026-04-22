import React, { useMemo, useRef, useCallback, useState } from "react";
import {
  Box, Typography, Card, CardContent, Chip, Alert, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, Paper, Tooltip, IconButton,
  CircularProgress, Collapse,
} from "@mui/material";
import { Eye, AlertTriangle, CheckCircle2, FileText, DollarSign, Calendar, TrendingUp, Download, ChevronDown, ChevronRight } from "lucide-react";
import html2pdf from "html2pdf.js";

const formatNumber = (val) => {
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'number') {
    if (Number.isInteger(val)) return val.toLocaleString();
    return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  }
  return String(val);
};

const SchedulePreview = ({ data, title, maxRows = 6 }) => {
  if (!data || !Array.isArray(data) || data.length === 0) return null;
  // Hide internal tagging columns (e.g. _instrumentid, _postingdate) used
  // only for Business Preview filtering — they are not part of the
  // user's schedule output.
  const allKeys = [...new Set(data.flatMap(obj => Object.keys(obj)))]
    .filter(k => !String(k).startsWith('_'));
  const displayRows = data.slice(0, maxRows);
  const hasMore = data.length > maxRows;

  return (
    <Box sx={{ mb: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Calendar size={14} color="#5B5FED" />
        <Typography variant="body2" fontWeight={600} color="text.primary">
          {title || 'Schedule Preview'}
        </Typography>
        <Chip label={`${data.length} periods`} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#EEF0FE', color: '#5B5FED' }} />
      </Box>
      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 2, maxHeight: 300 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', minWidth: 32 }}>#</TableCell>
              {allKeys.map((key) => (
                <TableCell key={key} sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA', whiteSpace: 'nowrap' }}>
                  {key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {displayRows.map((row, idx) => (
              <TableRow key={idx} hover>
                <TableCell sx={{ fontSize: '0.75rem', py: 0.5, color: '#6C757D' }}>{idx + 1}</TableCell>
                {allKeys.map((key) => (
                  <TableCell key={key} sx={{
                    fontSize: '0.75rem', py: 0.5, whiteSpace: 'nowrap',
                    color: typeof row[key] === 'number' ? '#14213D' : '#495057',
                    fontWeight: typeof row[key] === 'number' ? 500 : 400,
                    fontFamily: typeof row[key] === 'number' ? 'monospace' : 'inherit',
                  }}>
                    {formatNumber(row[key])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {hasMore && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Showing {maxRows} of {data.length} periods
        </Typography>
      )}
    </Box>
  );
};

const TransactionPreview = ({ transactions }) => {
  if (!transactions || transactions.length === 0) return null;

  return (
    <Box sx={{ mb: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <FileText size={14} color="#5B5FED" />
        <Typography variant="body2" fontWeight={600} color="text.primary">Transactions</Typography>
        <Chip label={`${transactions.length} entries`} size="small" sx={{ fontSize: '0.6875rem', height: 20, bgcolor: '#D4EDDA', color: '#155724' }} />
      </Box>
      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 2, maxHeight: 300 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Posting Date</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Effective Date</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Type</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Instrument</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Sub-Instrument</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }} align="right">Amount</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {transactions.map((txn, idx) => {
              const amount = typeof txn.amount === 'number' ? txn.amount : parseFloat(txn.amount) || 0;
              return (
                <TableRow key={idx} hover>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>{txn.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>{txn.effectivedate || txn.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>
                    <Chip label={txn.transactiontype || 'Unknown'} size="small"
                      sx={{ fontSize: '0.6875rem', height: 18, bgcolor: '#EEF0FE', color: '#5B5FED' }} />
                  </TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5, color: '#6C757D' }}>{txn.instrumentid || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5, color: '#6C757D' }}>{txn.subinstrumentid || '1'}</TableCell>
                  <TableCell align="right" sx={{ fontSize: '0.75rem', py: 0.5, fontFamily: 'monospace', fontWeight: 500, color: '#14213D' }}>
                    {formatNumber(amount)}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

const CollapsibleSection = ({ title, icon: Icon, iconColor, count, countColor, countBg, children, defaultOpen = true }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Box sx={{ mb: 2 }}>
      <Box
        onClick={() => setOpen(o => !o)}
        sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: open ? 1 : 0, cursor: 'pointer', userSelect: 'none',
          p: 0.5, borderRadius: 1, '&:hover': { bgcolor: '#F0F0F0' } }}>
        {open ? <ChevronDown size={14} color="#6C757D" /> : <ChevronRight size={14} color="#6C757D" />}
        {Icon && <Icon size={14} color={iconColor || '#5B5FED'} />}
        <Typography variant="body2" fontWeight={600} color="text.primary">{title}</Typography>
        {count !== undefined && (
          <Chip label={count} size="small"
            sx={{ fontSize: '0.6875rem', height: 20, bgcolor: countBg || '#EEF0FE', color: countColor || '#5B5FED' }} />
        )}
      </Box>
      <Collapse in={open}>{children}</Collapse>
    </Box>
  );
};

const ValidationWarnings = ({ warnings }) => {
  if (!warnings || warnings.length === 0) return null;
  const errors = warnings.filter(w => w.severity === 'error');
  const others = warnings.filter(w => w.severity !== 'error');
  return (
    <>
      {errors.length > 0 && (
        <CollapsibleSection title="Errors" icon={AlertTriangle} iconColor="#D32F2F"
          count={errors.length} countBg="#FFEBEE" countColor="#D32F2F" defaultOpen={true}>
          {errors.map((w, idx) => (
            <Alert key={idx} severity="error" sx={{ mb: 0.75, fontSize: '0.8125rem', '& .MuiAlert-message': { fontSize: '0.8125rem' } }}>
              {w.message}
            </Alert>
          ))}
        </CollapsibleSection>
      )}
      {others.length > 0 && (
        <CollapsibleSection title="Warnings & Info" icon={AlertTriangle} iconColor="#F57C00"
          count={others.length} countBg="#FFF3E0" countColor="#E65100" defaultOpen={false}>
          {others.map((w, idx) => (
            <Alert key={idx} severity={w.severity || 'warning'}
              sx={{ mb: 0.75, fontSize: '0.8125rem', '& .MuiAlert-message': { fontSize: '0.8125rem' } }}
              icon={w.severity === 'success' ? <CheckCircle2 size={16} /> : <AlertTriangle size={16} />}>
              {w.message}
            </Alert>
          ))}
        </CollapsibleSection>
      )}
    </>
  );
};

const SummaryMetrics = ({ transactions, schedules }) => {
  const metrics = useMemo(() => {
    const result = [];
    if (transactions && transactions.length > 0) {
      const totalAmount = transactions.reduce((s, t) => s + Math.abs(t.amount || 0), 0);
      const types = [...new Set(transactions.map(t => t.transactiontype))];
      result.push({ label: 'Total Amount', value: formatNumber(totalAmount), icon: DollarSign, color: '#155724' });
      result.push({ label: 'Transaction Count', value: transactions.length.toLocaleString(), icon: FileText, color: '#5B5FED' });
      result.push({ label: 'Transaction Types', value: types.join(', '), icon: TrendingUp, color: '#5B5FED' });
    }
    if (schedules && schedules.length > 0) {
      const totalPeriods = schedules.reduce((s, sch) => s + (Array.isArray(sch) ? sch.length : 0), 0);
      result.push({ label: 'Total Periods', value: totalPeriods.toLocaleString(), icon: Calendar, color: '#5B5FED' });
    }
    return result;
  }, [transactions, schedules]);

  if (metrics.length === 0) return null;

  return (
    <Box sx={{ display: 'flex', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
      {metrics.map((m, idx) => (
        <Card key={idx} sx={{ flex: '1 1 auto', minWidth: 120 }}>
          <CardContent sx={{ p: 1.5, '&:last-child': { pb: 1.5 } }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.5 }}>
              <m.icon size={12} color={m.color} />
              <Typography variant="caption" color="text.secondary">{m.label}</Typography>
            </Box>
            <Typography variant="body2" fontWeight={600} color="text.primary" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>
              {m.value}
            </Typography>
          </CardContent>
        </Card>
      ))}
    </Box>
  );
};

/**
 * LivePreview — Shows business-meaningful preview of DSL execution results.
 * Renders schedule tables, journal entries, validation warnings, and summary metrics.
 *
 * Props:
 *   consoleOutput: array of { message, type, timestamp } from console
 *   transactions: array of transaction objects from last execution
 *   schedules: array of schedule data arrays
 *   warnings: array of { message, severity } validation warnings
 *   visible: boolean
 */
const LivePreview = ({ consoleOutput = [], transactions = [], schedules = [], warnings = [], visible = true, templateName = '' }) => {
  const contentRef = useRef(null);
  const [exporting, setExporting] = useState(false);
  const [selectedInstrument, setSelectedInstrument] = useState(null);
  const [selectedPostingDate, setSelectedPostingDate] = useState(null);
  // Extract schedule data from console print outputs
  const extractedSchedules = useMemo(() => {
    if (schedules && schedules.length > 0) return schedules;
    const result = [];
    for (const log of consoleOutput) {
      if (log.type !== 'print') continue;
      try {
        const parsed = JSON.parse(log.message);
        if (Array.isArray(parsed)) {
          // Array of schedule result objects
          if (parsed.every(x => x && typeof x === 'object' && 'schedule' in x)) {
            parsed.forEach(r => { if (Array.isArray(r.schedule)) result.push(r.schedule); });
          }
          // Array of schedule arrays
          else if (parsed.every(item => Array.isArray(item) && item.length > 0 && typeof item[0] === 'object')) {
            parsed.forEach(s => result.push(s));
          }
          // Single schedule (array of objects)
          else if (parsed.length > 0 && typeof parsed[0] === 'object' && !Array.isArray(parsed[0])) {
            result.push(parsed);
          }
        }
      } catch { /* not JSON */ }
    }
    return result;
  }, [consoleOutput, schedules]);

  // Generate validation warnings from data
  const computedWarnings = useMemo(() => {
    const w = [...(warnings || [])];
    if (transactions.length > 0) {
      const zeroAmountTxns = transactions.filter(t => (t.amount || 0) === 0);
      if (zeroAmountTxns.length > 0) {
        w.push({ message: `${zeroAmountTxns.length} transaction(s) have a zero amount — is that expected?`, severity: 'warning' });
      }
      const uniqueDates = [...new Set(transactions.map(t => t.postingdate))];
      if (uniqueDates.length > 1) {
        w.push({ message: `Transactions span ${uniqueDates.length} different posting dates.`, severity: 'info' });
      }
    }
    for (const sched of extractedSchedules) {
      if (Array.isArray(sched)) {
        const numCols = Object.keys(sched[0] || {}).filter(k => typeof sched[0][k] === 'number');
        for (const col of numCols) {
          const zeroPeriods = sched.filter(row => row[col] === 0).length;
          if (zeroPeriods > sched.length * 0.5 && zeroPeriods > 2) {
            w.push({ message: `Schedule column "${col}" has ${zeroPeriods} zero-value periods out of ${sched.length} — check your formula.`, severity: 'warning' });
          }
        }
      }
    }
    if (transactions.length > 0) {
      w.push({ message: `Execution produced ${transactions.length} transaction(s) successfully.`, severity: 'success' });
    }
    return w;
  }, [transactions, extractedSchedules, warnings]);

  const hasContent = extractedSchedules.length > 0 || transactions.length > 0;

  // Derive unique instrument IDs from transactions AND schedules
  const instrumentOptions = useMemo(() => {
    const set = new Set();
    for (const t of (transactions || [])) {
      const v = String(t.instrumentid ?? '').trim();
      if (v) set.add(v);
    }
    for (const sched of extractedSchedules) {
      if (Array.isArray(sched)) {
        for (const row of sched) {
          const v = String(row?._instrumentid ?? '').trim();
          if (v) set.add(v);
        }
      }
    }
    return [...set].sort();
  }, [transactions, extractedSchedules]);

  // Derive unique posting dates from transactions AND schedules
  const postingDateOptions = useMemo(() => {
    const set = new Set();
    for (const t of (transactions || [])) {
      const v = String(t.postingdate ?? '').trim();
      if (v) set.add(v);
    }
    for (const sched of extractedSchedules) {
      if (Array.isArray(sched)) {
        for (const row of sched) {
          const v = String(row?._postingdate ?? '').trim();
          if (v) set.add(v);
        }
      }
    }
    return [...set].sort();
  }, [transactions, extractedSchedules]);

  // Resolve current selections (drop if no longer valid)
  const resolvedInstrument = instrumentOptions.includes(selectedInstrument) ? selectedInstrument : null;
  const resolvedPostingDate = postingDateOptions.includes(selectedPostingDate) ? selectedPostingDate : null;

  // Apply both filters to transactions (AND semantics)
  const filteredTransactions = useMemo(() => {
    return (transactions || []).filter(t => {
      if (resolvedInstrument && String(t.instrumentid ?? '') !== resolvedInstrument) return false;
      if (resolvedPostingDate && String(t.postingdate ?? '') !== resolvedPostingDate) return false;
      return true;
    });
  }, [transactions, resolvedInstrument, resolvedPostingDate]);

  // Apply both filters to schedules: a schedule is shown only if at least one
  // tagged row matches BOTH selected filters. Strict semantics — untagged rows
  // never satisfy an active filter, so legacy/uncategorized schedules are
  // hidden once a selection is made.
  const filteredSchedules = useMemo(() => {
    if (!resolvedInstrument && !resolvedPostingDate) return extractedSchedules;
    return extractedSchedules.filter(sched => {
      if (!Array.isArray(sched) || sched.length === 0) return false;
      return sched.some(row => {
        if (resolvedInstrument) {
          if (row?._instrumentid === undefined) return false;
          if (String(row._instrumentid) !== resolvedInstrument) return false;
        }
        if (resolvedPostingDate) {
          if (row?._postingdate === undefined) return false;
          if (String(row._postingdate) !== resolvedPostingDate) return false;
        }
        return true;
      });
    });
  }, [extractedSchedules, resolvedInstrument, resolvedPostingDate]);

  const handleExportPDF = useCallback(async () => {
    if (!contentRef.current) return;
    setExporting(true);
    try {
      const slug = (templateName || 'business-preview').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '');
      const filename = `${slug}-preview.pdf`;
      const element = contentRef.current;
      await html2pdf().set({
        margin: [12, 10, 12, 10],
        filename,
        image: { type: 'jpeg', quality: 0.98 },
        html2canvas: { scale: 2, useCORS: true, logging: false },
        jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' },
        pagebreak: { mode: ['avoid-all', 'css', 'legacy'] },
      }).from(element).save();
    } catch (err) {
      console.error('PDF export failed:', err);
    } finally {
      setExporting(false);
    }
  }, [templateName]);

  if (!visible) return null;

  return (
    <Box sx={{ p: 2, bgcolor: '#F8F9FA', height: '100%', overflowY: 'auto' }} data-testid="live-preview">
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Eye size={18} color="#5B5FED" />
        <Typography variant="h6" sx={{ fontSize: '0.9375rem', flex: 1 }}>Business Preview</Typography>
        {hasContent && (
          <Tooltip title="Download as PDF">
            <IconButton size="small" onClick={handleExportPDF} disabled={exporting}
              sx={{ color: '#5B5FED', '&:hover': { bgcolor: '#EEF0FE' } }}>
              {exporting ? <CircularProgress size={16} /> : <Download size={16} />}
            </IconButton>
          </Tooltip>
        )}
      </Box>

      <Box ref={contentRef}>
      {templateName && hasContent && (
        <Box sx={{ mb: 2, pb: 1, borderBottom: '1px solid #E9ECEF' }} className="pdf-header">
          <Typography variant="subtitle1" fontWeight={700} color="text.primary">{templateName}</Typography>
          <Typography variant="caption" color="text.secondary">
            Generated {new Date().toLocaleDateString()}
          </Typography>
        </Box>
      )}

      {/* Instrument filter */}
      {instrumentOptions.length > 1 && (
        <Box sx={{ mb: 2 }}>
          <Typography variant="caption" color="text.secondary" sx={{ mb: 0.75, display: 'block', fontWeight: 600 }}>
            Filter by Instrument
          </Typography>
          <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap' }}>
            <Chip
              label="All"
              size="small"
              onClick={() => setSelectedInstrument(null)}
              sx={{
                fontSize: '0.75rem', height: 24, cursor: 'pointer',
                bgcolor: !resolvedInstrument ? '#5B5FED' : '#F0F0F0',
                color: !resolvedInstrument ? '#fff' : '#495057',
                fontWeight: !resolvedInstrument ? 600 : 400,
                '&:hover': { bgcolor: !resolvedInstrument ? '#4A4ED0' : '#E0E0E0' },
              }}
            />
            {instrumentOptions.map(opt => (
              <Chip
                key={opt}
                label={opt}
                size="small"
                onClick={() => setSelectedInstrument(opt === resolvedInstrument ? null : opt)}
                sx={{
                  fontSize: '0.75rem', height: 24, cursor: 'pointer', fontFamily: 'monospace',
                  bgcolor: resolvedInstrument === opt ? '#5B5FED' : '#F0F0F0',
                  color: resolvedInstrument === opt ? '#fff' : '#495057',
                  fontWeight: resolvedInstrument === opt ? 600 : 400,
                  '&:hover': { bgcolor: resolvedInstrument === opt ? '#4A4ED0' : '#E0E0E0' },
                }}
              />
            ))}
          </Box>
        </Box>
      )}

      {/* Posting date filter */}
      {postingDateOptions.length > 1 && (
        <Box sx={{ mb: 2 }}>
          <Typography variant="caption" color="text.secondary" sx={{ mb: 0.75, display: 'block', fontWeight: 600 }}>
            Filter by Posting Date
          </Typography>
          <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap' }}>
            <Chip
              label="All"
              size="small"
              onClick={() => setSelectedPostingDate(null)}
              sx={{
                fontSize: '0.75rem', height: 24, cursor: 'pointer',
                bgcolor: !resolvedPostingDate ? '#5B5FED' : '#F0F0F0',
                color: !resolvedPostingDate ? '#fff' : '#495057',
                fontWeight: !resolvedPostingDate ? 600 : 400,
                '&:hover': { bgcolor: !resolvedPostingDate ? '#4A4ED0' : '#E0E0E0' },
              }}
            />
            {postingDateOptions.map(opt => (
              <Chip
                key={opt}
                label={opt}
                size="small"
                onClick={() => setSelectedPostingDate(opt === resolvedPostingDate ? null : opt)}
                sx={{
                  fontSize: '0.75rem', height: 24, cursor: 'pointer', fontFamily: 'monospace',
                  bgcolor: resolvedPostingDate === opt ? '#5B5FED' : '#F0F0F0',
                  color: resolvedPostingDate === opt ? '#fff' : '#495057',
                  fontWeight: resolvedPostingDate === opt ? 600 : 400,
                  '&:hover': { bgcolor: resolvedPostingDate === opt ? '#4A4ED0' : '#E0E0E0' },
                }}
              />
            ))}
          </Box>
        </Box>
      )}

      {!hasContent && computedWarnings.length === 0 && (
        <Card sx={{ textAlign: 'center', py: 4 }}>
          <CardContent>
            <Eye size={36} color="#CED4DA" style={{ marginBottom: 12 }} />
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              Run your calculation to see a business preview here
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Schedule tables, transactions, and validation checks will appear automatically
            </Typography>
          </CardContent>
        </Card>
      )}

      {(hasContent || computedWarnings.length > 0) && (
        <>
          <SummaryMetrics transactions={filteredTransactions} schedules={filteredSchedules} />
          <ValidationWarnings warnings={computedWarnings} />
          {filteredSchedules.map((sched, idx) => (
            <SchedulePreview key={idx} data={sched} title={filteredSchedules.length > 1 ? `Schedule ${idx + 1}` : 'Schedule'} maxRows={999} />
          ))}
          <TransactionPreview transactions={filteredTransactions} />
        </>
      )}
      </Box>
    </Box>
  );
};

export default LivePreview;
