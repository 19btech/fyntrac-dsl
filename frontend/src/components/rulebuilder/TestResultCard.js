import React, { useMemo } from "react";
import {
  Box, Typography, Card, IconButton, Chip,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
} from "@mui/material";
import { CheckCircle2, XCircle, AlertTriangle, X } from "lucide-react";

/**
 * Parses a single print line of the form `name = <repr>` or `name: <repr>`
 * into { label, raw }. If no label separator is found, raw is the whole text.
 */
function splitLabelAndValue(line) {
  if (line == null) return { label: null, raw: '' };
  const text = String(line);
  const eq = text.indexOf(' = ');
  if (eq > 0) return { label: text.slice(0, eq).trim(), raw: text.slice(eq + 3) };
  const colon = text.indexOf(': ');
  if (colon > 0 && colon < 40) return { label: text.slice(0, colon).trim(), raw: text.slice(colon + 2) };
  return { label: null, raw: text };
}

/**
 * Try to parse a Python repr value into a JS value:
 *   - numbers, booleans (True/False), None
 *   - lists of dicts (Python single-quote dict repr → JSON)
 *   - quoted strings
 * Returns the parsed value or `undefined` if parsing fails.
 */
function tryParsePythonRepr(raw) {
  if (raw == null) return undefined;
  const trimmed = String(raw).trim();
  if (trimmed === '') return undefined;
  if (trimmed === 'True') return true;
  if (trimmed === 'False') return false;
  if (trimmed === 'None' || trimmed === 'null') return null;
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
  // Quoted string
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
    return trimmed.slice(1, -1);
  }
  // List or dict — try JSON first, then convert single → double quotes
  if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
    try { return JSON.parse(trimmed); } catch { /* fall through */ }
    try {
      const json = trimmed
        .replace(/\bTrue\b/g, 'true')
        .replace(/\bFalse\b/g, 'false')
        .replace(/\bNone\b/g, 'null')
        .replace(/'/g, '"');
      return JSON.parse(json);
    } catch { /* fall through */ }
  }
  return undefined;
}

function formatScalar(v) {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toLocaleString(undefined, { maximumFractionDigits: 6 });
  }
  if (typeof v === 'boolean') return v ? 'True' : 'False';
  return String(v);
}

/** Compact one-line preview of any parsed value, suitable for a table cell. */
function formatPreview(value, raw, max = 80) {
  if (value === undefined || value === null) {
    const s = (raw || '').replace(/\s+/g, ' ').trim();
    if (!s) return '—';
    return s.length > max ? s.slice(0, max - 1) + '…' : s;
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return '[ ]';
    const items = value.slice(0, 6).map(v => {
      if (v === null || v === undefined) return '—';
      if (typeof v === 'object') return JSON.stringify(v);
      return formatScalar(v);
    });
    let out = '[' + items.join(', ');
    if (value.length > items.length) out += `, … +${value.length - items.length}`;
    out += ']';
    return out.length > max ? out.slice(0, max - 1) + '…' : out;
  }
  if (typeof value === 'object') {
    const s = JSON.stringify(value);
    return s.length > max ? s.slice(0, max - 1) + '…' : s;
  }
  return formatScalar(value);
}

/**
 * Friendly translation of common backend / DSL error messages.
 */
function humanizeError(err) {
  if (!err) return { message: 'Something went wrong while running this step.', detail: null };
  const text = typeof err === 'string' ? err : (err.message || String(err));

  // Pull out an offending expression if the backend put one in quotes.
  const exprMatch = text.match(/'([^']{2,})'/);
  const expr = exprMatch ? exprMatch[1] : null;

  if (/not defined/i.test(text)) {
    return { message: `A variable used here isn't defined yet.`, detail: text, expr };
  }
  if (/division by zero/i.test(text)) {
    return { message: `This calculation tried to divide by zero.`, detail: text, expr };
  }
  if (/length.*must equal/i.test(text)) {
    return { message: `The number of values doesn't match the expected list length.`, detail: text, expr };
  }
  if (/syntax/i.test(text)) {
    return { message: `The expression has a syntax problem.`, detail: text, expr };
  }
  if (/type/i.test(text) && /unsupported|cannot|expected/i.test(text)) {
    return { message: `The value type isn't compatible with this operation.`, detail: text, expr };
  }
  return { message: text, detail: null, expr };
}

const COLOR = {
  ok: '#2E7D32', okBg: '#E8F5E9',
  err: '#C62828', errBg: '#FFEBEE',
  num: '#1565C0', numBg: '#E3F2FD',
  neutral: '#37474F', neutralBg: '#F5F7FA',
  warn: '#EF6C00',
};

function ScalarValue({ value }) {
  if (value === true) {
    return (
      <Chip size="small" icon={<CheckCircle2 size={14} />} label="TRUE"
        sx={{ bgcolor: COLOR.okBg, color: COLOR.ok, fontWeight: 700, '& .MuiChip-icon': { color: COLOR.ok } }} />
    );
  }
  if (value === false) {
    return (
      <Chip size="small" icon={<XCircle size={14} />} label="FALSE"
        sx={{ bgcolor: COLOR.errBg, color: COLOR.err, fontWeight: 700, '& .MuiChip-icon': { color: COLOR.err } }} />
    );
  }
  const isNumber = typeof value === 'number';
  return (
    <Typography
      variant="h6"
      sx={{
        fontFamily: isNumber ? 'monospace' : 'inherit',
        fontWeight: 700,
        color: isNumber ? COLOR.num : COLOR.neutral,
        lineHeight: 1.2,
        wordBreak: 'break-word',
      }}>
      {formatScalar(value)}
    </Typography>
  );
}

function PerInstrumentTable({ rows, valueLabel }) {
  const visible = rows.slice(0, 100);
  return (
    <Box>
      <TableContainer sx={{ border: '1px solid #E0E4EA', borderRadius: 1, maxHeight: 360 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 700, bgcolor: '#F5F7FA', width: 40 }}>#</TableCell>
              <TableCell sx={{ fontWeight: 700, bgcolor: '#F5F7FA' }}>Instrument</TableCell>
              <TableCell sx={{ fontWeight: 700, bgcolor: '#F5F7FA' }}>{valueLabel}</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {visible.map((r, idx) => (
              <TableRow key={idx} hover>
                <TableCell sx={{ color: '#90A4AE', fontFamily: 'monospace' }}>{idx + 1}</TableCell>
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{r.instrument || '—'}</TableCell>
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8125rem', wordBreak: 'break-word' }}>
                  {formatPreview(r.value, r.raw)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {rows.length > visible.length && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Showing first {visible.length} of {rows.length} rows
        </Typography>
      )}
    </Box>
  );
}

function RowsTable({ rows }) {
  // Determine columns from the first object row; otherwise treat as 1-column list.
  const isObjectList = rows.length > 0 && typeof rows[0] === 'object' && rows[0] !== null && !Array.isArray(rows[0]);
  const columns = isObjectList
    ? Array.from(new Set(rows.flatMap(r => Object.keys(r || {}))))
    : ['value'];
  const visible = rows.slice(0, 50);
  return (
    <Box>
      <TableContainer sx={{ border: '1px solid #E0E4EA', borderRadius: 1, maxHeight: 320 }}>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 700, bgcolor: '#F5F7FA', width: 40 }}>#</TableCell>
              {columns.map(c => (
                <TableCell key={c} sx={{ fontWeight: 700, bgcolor: '#F5F7FA' }}>{c}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {visible.map((row, idx) => (
              <TableRow key={idx} hover>
                <TableCell sx={{ color: '#90A4AE', fontFamily: 'monospace' }}>{idx + 1}</TableCell>
                {columns.map(c => {
                  const v = isObjectList ? row?.[c] : row;
                  return (
                    <TableCell key={c} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>
                      {v === null || v === undefined ? '—' : formatScalar(v)}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {rows.length > visible.length && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Showing first {visible.length} of {rows.length} rows
        </Typography>
      )}
    </Box>
  );
}

/**
 * TestResultCard — clean, type-aware result display for variable-level tests.
 *
 * Props:
 *   success      boolean
 *   output       string (raw print line, e.g. "foo = 5" or just the value)
 *   error        string (when !success)
 *   variableName string (header label; falls back to label parsed from output)
 *   onClose      optional callback to dismiss the card
 *   sx           optional MUI sx for outer Card
 */
export default function TestResultCard({ success, output, error, variableName, onClose, sx }) {
  // Per-instrument rows produced by `__TEST_ROW__|inst| name = value` markers.
  const perInstrumentRows = useMemo(() => {
    if (!success) return null;
    const text = String(output || '');
    if (!text.includes('__TEST_ROW__')) return null;
    const lines = text.split('\n');
    // A marker line begins with "__TEST_ROW__|<inst>| <name> ="; the value
    // may span multiple following lines until the next marker.
    const rows = [];
    let current = null;
    for (const line of lines) {
      const m = line.match(/^__TEST_ROW__\|([^|]*)\|\s*(.*)$/);
      if (m) {
        if (current) rows.push(current);
        const [, inst, rest] = m;
        const { label, raw } = splitLabelAndValue(rest);
        current = { instrument: inst.trim(), label, rawLines: [raw] };
      } else if (current) {
        current.rawLines.push(line);
      }
    }
    if (current) rows.push(current);
    if (rows.length === 0) return null;
    return rows.map(r => {
      const raw = r.rawLines.join('\n').trim();
      const value = tryParsePythonRepr(raw);
      return { instrument: r.instrument, label: r.label, raw, value };
    });
  }, [success, output]);

  const parsed = useMemo(() => {
    if (!success) return null;
    if (perInstrumentRows) return null; // handled separately
    const text = String(output || '');
    const allLines = text.split('\n').map(l => l.trimEnd());
    // A single print can span many lines (Python pretty-prints lists/dicts):
    //   product_names = [
    //     'a', 'b'
    //   ]
    // Find the last line that looks like the START of a print ("name = ..." or
    // "name: ..." with name being an identifier) and concatenate everything
    // from there to the end. That is the value we actually want to render.
    const isStartLine = (l) => /^[A-Za-z_][A-Za-z0-9_]*\s*(=|:)\s/.test(l);
    let startIdx = -1;
    for (let i = allLines.length - 1; i >= 0; i--) {
      if (isStartLine(allLines[i])) { startIdx = i; break; }
    }
    let block;
    if (startIdx >= 0) {
      block = allLines.slice(startIdx).join('\n').trim();
    } else {
      // No "name = value" pattern — treat the whole thing as the raw value.
      block = allLines.filter(Boolean).join('\n').trim();
    }
    const { label, raw } = splitLabelAndValue(block);
    const value = tryParsePythonRepr(raw);
    return { label, raw, value };
  }, [success, output, perInstrumentRows]);

  const headerLabel = variableName || parsed?.label || perInstrumentRows?.[0]?.label || (success ? 'Result' : 'Error');

  if (!success) {
    const { message, detail, expr } = humanizeError(error);
    return (
      <Card variant="outlined" sx={{ borderColor: COLOR.err, bgcolor: COLOR.errBg, p: 1.25, mt: 1, ...sx }}>
        <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
          <AlertTriangle size={18} color={COLOR.err} style={{ marginTop: 2, flexShrink: 0 }} />
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.25 }}>
              <Typography variant="body2" fontWeight={700} sx={{ color: COLOR.err }}>
                {headerLabel}
              </Typography>
              <Chip size="small" label="Failed" sx={{ height: 18, fontSize: '0.6875rem', bgcolor: COLOR.err, color: 'white', fontWeight: 700 }} />
            </Box>
            <Typography variant="body2" sx={{ color: '#3E2723' }}>{message}</Typography>
            {expr && (
              <Box sx={{ mt: 0.5, p: 0.5, bgcolor: 'white', border: '1px solid #FFCDD2', borderRadius: 0.5, display: 'inline-block' }}>
                <Typography variant="caption" fontFamily="monospace" sx={{ color: COLOR.err, fontWeight: 600 }}>
                  {expr}
                </Typography>
              </Box>
            )}
            {detail && detail !== message && (
              <Typography variant="caption" sx={{ display: 'block', mt: 0.5, color: '#6D4C41', fontStyle: 'italic' }}>
                {detail}
              </Typography>
            )}
          </Box>
          {onClose && (
            <IconButton size="small" onClick={onClose} sx={{ color: COLOR.err, ml: 0.5 }}>
              <X size={14} />
            </IconButton>
          )}
        </Box>
      </Card>
    );
  }

  const value = parsed?.value;
  const isList = Array.isArray(value);
  const isEmpty = parsed?.raw === '' || parsed?.raw == null;

  return (
    <Card variant="outlined" sx={{ borderColor: '#C8E6C9', bgcolor: 'white', p: 1.25, mt: 1, ...sx }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.75 }}>
        <CheckCircle2 size={16} color={COLOR.ok} style={{ flexShrink: 0 }} />
        <Typography variant="body2" fontWeight={700} sx={{ color: COLOR.ok, flex: 1 }}>
          {headerLabel}
        </Typography>
        {perInstrumentRows ? (
          <Chip size="small" label={`${perInstrumentRows.length} ${perInstrumentRows.length === 1 ? 'row' : 'rows'}`}
            sx={{ height: 18, fontSize: '0.6875rem', bgcolor: COLOR.numBg, color: COLOR.num, fontWeight: 600 }} />
        ) : isList && (
          <Chip size="small" label={`${value.length} ${value.length === 1 ? 'row' : 'rows'}`}
            sx={{ height: 18, fontSize: '0.6875rem', bgcolor: COLOR.numBg, color: COLOR.num, fontWeight: 600 }} />
        )}
        {onClose && (
          <IconButton size="small" onClick={onClose} sx={{ color: '#90A4AE' }}>
            <X size={14} />
          </IconButton>
        )}
      </Box>

      {perInstrumentRows ? (
        <PerInstrumentTable rows={perInstrumentRows} valueLabel={perInstrumentRows[0]?.label || 'value'} />
      ) : isEmpty ? (
        <Typography variant="body2" color="text.secondary" fontStyle="italic">
          Ran successfully (no value produced)
        </Typography>
      ) : isList ? (
        value.length === 0
          ? <Typography variant="body2" color="text.secondary" fontStyle="italic">Empty list</Typography>
          : <RowsTable rows={value} />
      ) : value !== undefined ? (
        <ScalarValue value={value} />
      ) : (
        // Fallback: couldn't parse — show the raw value in monospace, no JSON braces emphasized.
        <Typography variant="body2" fontFamily="monospace" fontSize="0.8125rem"
          sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', color: COLOR.neutral }}>
          {parsed?.raw}
        </Typography>
      )}
    </Card>
  );
}
