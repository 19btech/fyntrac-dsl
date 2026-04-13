import React, { useMemo } from "react";
import {
  Box, Typography, Card, CardContent, Chip, Alert, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, Paper, Divider, Tooltip, IconButton,
} from "@mui/material";
import { Eye, AlertTriangle, CheckCircle2, FileText, DollarSign, Calendar, TrendingUp } from "lucide-react";

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
  const allKeys = [...new Set(data.flatMap(obj => Object.keys(obj)))];
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
      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Posting Date</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Effective Date</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Type</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }}>Instrument</TableCell>
              <TableCell sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, bgcolor: '#F8F9FA' }} align="right">Amount</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {transactions.slice(0, 10).map((txn, idx) => {
              const amount = typeof txn.amount === 'number' ? txn.amount : parseFloat(txn.amount) || 0;
              return (
                <TableRow key={idx} hover>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>{txn.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>{txn.effectivedate || txn.postingdate || '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5 }}>
                    <Chip label={txn.transactiontype || 'Unknown'} size="small"
                      sx={{ fontSize: '0.6875rem', height: 18, bgcolor: '#EEF0FE', color: '#5B5FED' }} />
                  </TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', py: 0.5, color: '#6C757D' }}>
                    {txn.instrumentid || '—'}{txn.subinstrumentid && txn.subinstrumentid !== '1' ? ` / ${txn.subinstrumentid}` : ''}
                  </TableCell>
                  <TableCell align="right" sx={{ fontSize: '0.75rem', py: 0.5, fontFamily: 'monospace', fontWeight: 500, color: '#14213D' }}>
                    {formatNumber(amount)}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
      {transactions.length > 10 && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Showing 10 of {transactions.length} transactions
        </Typography>
      )}
    </Box>
  );
};

const ValidationWarnings = ({ warnings }) => {
  if (!warnings || warnings.length === 0) return null;
  return (
    <Box sx={{ mb: 2 }}>
      {warnings.map((w, idx) => (
        <Alert key={idx} severity={w.severity || 'warning'} sx={{ mb: 1, fontSize: '0.8125rem', '& .MuiAlert-message': { fontSize: '0.8125rem' } }}
          icon={w.severity === 'success' ? <CheckCircle2 size={16} /> : <AlertTriangle size={16} />}>
          {w.message}
        </Alert>
      ))}
    </Box>
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
const LivePreview = ({ consoleOutput = [], transactions = [], schedules = [], warnings = [], visible = true }) => {
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

  if (!visible) return null;

  return (
    <Box sx={{ p: 2, bgcolor: '#F8F9FA', height: '100%', overflowY: 'auto' }} data-testid="live-preview">
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Eye size={18} color="#5B5FED" />
        <Typography variant="h6" sx={{ fontSize: '0.9375rem' }}>Business Preview</Typography>
      </Box>

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
          <SummaryMetrics transactions={transactions} schedules={extractedSchedules} />
          <ValidationWarnings warnings={computedWarnings} />
          {extractedSchedules.map((sched, idx) => (
            <SchedulePreview key={idx} data={sched} title={extractedSchedules.length > 1 ? `Schedule ${idx + 1}` : 'Schedule'} />
          ))}
          <TransactionPreview transactions={transactions} />
        </>
      )}
    </Box>
  );
};

export default LivePreview;
