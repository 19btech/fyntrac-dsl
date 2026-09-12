import React, { useState } from "react";
import { Copy, Check } from "lucide-react";

/**
 * MarkdownLite — a small, dependency-free Markdown renderer tuned for a
 * finance/accounting assistant. Supports headings, bold/italic/inline-code,
 * bullet & numbered lists, GitHub-style tables (with numeric-column
 * right-alignment + thousands separators and total-row emphasis), fenced code
 * blocks (copy button + language label + light DSL highlighting), callout
 * blockquotes (Note/Warning/Tip/Important), clickable links, and paragraphs.
 * Deliberately minimal (no raw HTML passthrough) so model output is safe.
 */

// ── Number parsing / formatting ────────────────────────────────────────────
// Parse a cell/inline token into a number without losing precision. Handles
// $ prefixes, thousands commas, trailing %, and parenthesised negatives.
function parseNumeric(raw) {
  if (raw == null) return null;
  let s = String(raw).trim();
  if (!s) return null;
  let neg = false;
  if (/^\(.*\)$/.test(s)) { neg = true; s = s.slice(1, -1).trim(); }
  const hasDollar = s.includes("$");
  const hasPct = /%\s*$/.test(s);
  let body = s.replace(/[$,%\s]/g, "");
  if (body.startsWith("-")) { neg = true; body = body.slice(1); }
  else if (body.startsWith("+")) { body = body.slice(1); }
  if (!/^\d+(\.\d+)?$/.test(body)) return null;
  const value = parseFloat(body) * (neg ? -1 : 1);
  if (!isFinite(value)) return null;
  const decimals = (body.split(".")[1] || "").length;
  return { value, hasDollar, hasPct, decimals };
}

// Add thousands separators; preserve the original decimal count (no rounding),
// but ensure at least 2 decimals for explicit currency.
function formatNumeric(n) {
  const { value, hasDollar, hasPct, decimals } = n;
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  const minDec = hasDollar ? Math.max(decimals, 2) : decimals;
  const maxDec = Math.max(minDec, decimals);
  const str = abs.toLocaleString("en-US", {
    minimumFractionDigits: minDec,
    maximumFractionDigits: maxDec,
  });
  return `${sign}${hasDollar ? "$" : ""}${str}${hasPct ? "%" : ""}`;
}

// Shared amount formatter (used by ResultCard too). Thousands separators,
// up to 4 decimals, optional $ prefix. Non-numeric input returns as-is.
export function formatAmount(v, { currency = false } = {}) {
  const num = typeof v === "number" ? v : parseNumeric(v)?.value;
  if (num == null || !isFinite(num)) return String(v == null ? "" : v);
  const sign = num < 0 ? "-" : "";
  const abs = Math.abs(num);
  return `${sign}${currency ? "$" : ""}${abs.toLocaleString("en-US", {
    minimumFractionDigits: currency ? 2 : 0,
    maximumFractionDigits: 4,
  })}`;
}

// ── Inline rendering: code, links, bold, italic, $-amounts ─────────────────
function renderInline(text, keyPrefix = "") {
  const out = [];
  const re = /(`[^`]+`)|(\[[^\]]+\]\([^)]+\))|(\*\*[^*]+\*\*)|(\*[^*]+\*)|(https?:\/\/[^\s)]+)|(\$\s?-?[\d,]+(?:\.\d+)?)/g;
  let last = 0;
  let m;
  let i = 0;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) out.push(text.slice(last, m.index));
    const tok = m[0];
    const k = `${keyPrefix}-${i++}`;
    if (tok.startsWith("`")) {
      out.push(
        <code key={k} style={{
          background: "rgba(99,102,241,0.10)", borderRadius: 4, padding: "1px 5px",
          fontSize: "0.86em", fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
        }}>{tok.slice(1, -1)}</code>
      );
    } else if (tok.startsWith("[")) {
      const lm = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(tok);
      out.push(
        <a key={k} href={lm[2]} target="_blank" rel="noopener noreferrer"
           style={{ color: "#4f46e5", textDecoration: "underline" }}>{lm[1]}</a>
      );
    } else if (tok.startsWith("**")) {
      out.push(<strong key={k}>{tok.slice(2, -2)}</strong>);
    } else if (tok.startsWith("*")) {
      out.push(<em key={k}>{tok.slice(1, -1)}</em>);
    } else if (tok.startsWith("http")) {
      out.push(
        <a key={k} href={tok} target="_blank" rel="noopener noreferrer"
           style={{ color: "#4f46e5", textDecoration: "underline", wordBreak: "break-all" }}>{tok}</a>
      );
    } else {
      // $-prefixed amount
      const n = parseNumeric(tok);
      out.push(n ? <span key={k}>{formatNumeric({ ...n, hasDollar: true })}</span> : tok);
    }
    last = m.index + tok.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

// ── Code block with copy + language label + light DSL highlighting ─────────
const DSL_KEYWORDS = new Set([
  "if", "and", "or", "not", "multiply", "divide", "add", "subtract", "power",
  "round", "round_val", "abs", "ceil", "floor", "min", "max", "sum", "avg",
  "lag", "schedule", "schedule_sum", "schedule_last", "schedule_first",
  "lookup", "coalesce", "is_null", "switch", "concat", "pmt", "pv", "fv",
  "npv", "irr", "xnpv", "xirr", "rate", "nper", "add_months", "add_days",
  "end_of_month", "day_count_fraction", "days_between", "months_between",
  "collect_by_instrument", "collect_all", "for_each", "prev", "createTransaction",
]);

function highlightCode(code) {
  const nodes = [];
  const re = /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)/g;
  let last = 0;
  let m;
  let i = 0;
  while ((m = re.exec(code)) !== null) {
    if (m.index > last) nodes.push(code.slice(last, m.index));
    const tok = m[0];
    const k = `c${i++}`;
    if (m[1]) nodes.push(<span key={k} style={{ color: "#16a34a" }}>{tok}</span>);
    else if (m[2]) nodes.push(<span key={k} style={{ color: "#d97706" }}>{tok}</span>);
    else if (DSL_KEYWORDS.has(tok)) nodes.push(<span key={k} style={{ color: "#4f46e5", fontWeight: 600 }}>{tok}</span>);
    else nodes.push(tok);
    last = m.index + tok.length;
  }
  if (last < code.length) nodes.push(code.slice(last));
  return nodes;
}

function CodeBlock({ code, lang }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    try {
      navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch { /* ignore */ }
  };
  return (
    <div style={{ position: "relative", margin: "8px 0" }}>
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        background: "rgba(15,23,42,0.06)", borderRadius: "8px 8px 0 0",
        padding: "3px 8px 3px 12px", fontSize: 10.5, color: "#64748b",
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      }}>
        <span>{lang || "code"}</span>
        <button onClick={copy} title="Copy" style={{
          display: "flex", alignItems: "center", gap: 4, border: 0,
          background: "transparent", cursor: "pointer", color: "#64748b",
          fontSize: 10.5, padding: "2px 4px",
        }}>
          {copied ? <Check size={11} /> : <Copy size={11} />}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre style={{
        background: "rgba(15,23,42,0.05)", borderRadius: "0 0 8px 8px",
        padding: "10px 12px", overflowX: "auto", fontSize: 12, margin: 0,
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      }}>
        <code>{highlightCode(code)}</code>
      </pre>
    </div>
  );
}

// ── Callout blockquotes ────────────────────────────────────────────────────
const CALLOUTS = {
  note: { border: "#3b82f6", bg: "rgba(59,130,246,0.08)", label: "Note" },
  info: { border: "#3b82f6", bg: "rgba(59,130,246,0.08)", label: "Info" },
  tip: { border: "#10b981", bg: "rgba(16,185,129,0.08)", label: "Tip" },
  warning: { border: "#f59e0b", bg: "rgba(245,158,11,0.10)", label: "Warning" },
  important: { border: "#ef4444", bg: "rgba(239,68,68,0.08)", label: "Important" },
  caution: { border: "#ef4444", bg: "rgba(239,68,68,0.08)", label: "Caution" },
};

const splitRow = (line) =>
  line.trim().replace(/^\||\|$/g, "").split("|").map(c => c.trim());

export default function MarkdownLite({ text, style }) {
  if (!text) return null;
  const lines = String(text).replace(/\r\n/g, "\n").split("\n");
  const blocks = [];
  let i = 0;
  let key = 0;
  const listItemStyle = { margin: "2px 0", lineHeight: 1.55 };

  while (i < lines.length) {
    const line = lines[i];

    // Fenced code block
    if (line.trim().startsWith("```")) {
      const lang = line.trim().slice(3).trim();
      const buf = [];
      i++;
      while (i < lines.length && !lines[i].trim().startsWith("```")) { buf.push(lines[i]); i++; }
      i++;
      blocks.push(<CodeBlock key={key++} code={buf.join("\n")} lang={lang} />);
      continue;
    }

    // Blockquote / callout group
    if (/^\s*>\s?/.test(line)) {
      const buf = [];
      while (i < lines.length && /^\s*>\s?/.test(lines[i])) {
        buf.push(lines[i].replace(/^\s*>\s?/, ""));
        i++;
      }
      const inner = buf.join("\n").trim();
      const cm = /^\*\*(note|info|tip|warning|important|caution)\s*:?\*\*\s*:?\s*/i.exec(inner)
        || /^(note|info|tip|warning|important|caution)\s*:\s*/i.exec(inner);
      const kind = cm ? cm[1].toLowerCase() : "note";
      const style2 = CALLOUTS[kind] || CALLOUTS.note;
      const body = cm ? inner.slice(cm[0].length) : inner;
      blocks.push(
        <div key={key++} style={{
          borderLeft: `3px solid ${style2.border}`, background: style2.bg,
          borderRadius: 6, padding: "8px 12px", margin: "8px 0", fontSize: 12.5,
        }}>
          <div style={{ fontWeight: 700, color: style2.border, marginBottom: 2, fontSize: 12 }}>
            {style2.label}
          </div>
          <div style={{ lineHeight: 1.5 }}>{renderInline(body, `cq${key}`)}</div>
        </div>
      );
      continue;
    }

    // Table
    if (line.includes("|") && i + 1 < lines.length &&
        /^\s*\|?[\s:-]+\|[\s:|-]*$/.test(lines[i + 1])) {
      const header = splitRow(line);
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim()) {
        rows.push(splitRow(lines[i]));
        i++;
      }
      // Determine numeric columns: >= 60% of non-empty body cells parse.
      const numericCol = header.map((_, ci) => {
        let numeric = 0, total = 0;
        for (const r of rows) {
          const v = (r[ci] || "").trim();
          if (!v) continue;
          total++;
          if (parseNumeric(v)) numeric++;
        }
        return total > 0 && numeric / total >= 0.6;
      });
      const isTotalRow = (r) => /^(total|net|sum|grand\s*total|subtotal)\b/i.test((r[0] || "").trim());
      blocks.push(
        <div key={key++} style={{ overflowX: "auto", margin: "8px 0" }}>
          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 12.5 }}>
            <thead>
              <tr>
                {header.map((h, hi) => (
                  <th key={hi} style={{
                    textAlign: numericCol[hi] ? "right" : "left", padding: "6px 10px",
                    borderBottom: "2px solid rgba(99,102,241,0.35)", fontWeight: 700,
                    whiteSpace: "nowrap",
                  }}>{renderInline(h, `th${key}-${hi}`)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => {
                const totalRow = isTotalRow(r);
                return (
                  <tr key={ri} style={{
                    background: totalRow ? "rgba(99,102,241,0.10)"
                      : ri % 2 ? "rgba(99,102,241,0.04)" : "transparent",
                    fontWeight: totalRow ? 700 : 400,
                  }}>
                    {header.map((_, ci) => {
                      const raw = r[ci] || "";
                      const n = numericCol[ci] ? parseNumeric(raw) : null;
                      return (
                        <td key={ci} style={{
                          padding: "5px 10px", borderBottom: "1px solid rgba(0,0,0,0.06)",
                          verticalAlign: "top", textAlign: numericCol[ci] ? "right" : "left",
                          fontVariantNumeric: numericCol[ci] ? "tabular-nums" : "normal",
                          whiteSpace: numericCol[ci] ? "nowrap" : "normal",
                        }}>
                          {n ? formatNumeric(n) : renderInline(raw, `td${key}-${ri}-${ci}`)}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      );
      continue;
    }

    // Headings
    const h = line.match(/^(#{1,4})\s+(.*)/);
    if (h) {
      const level = h[1].length;
      const sizes = { 1: 17, 2: 15.5, 3: 14, 4: 13 };
      blocks.push(
        <div key={key++} style={{
          fontWeight: 700, fontSize: sizes[level] || 13, margin: "10px 0 4px", lineHeight: 1.3,
        }}>{renderInline(h[2], `h${key}`)}</div>
      );
      i++;
      continue;
    }

    // Bulleted list
    if (/^\s*[-•*]\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*[-•*]\s+/.test(lines[i])) {
        const indent = (lines[i].match(/^\s*/)[0] || "").length;
        items.push({ indent, text: lines[i].replace(/^\s*[-•*]\s+/, "") });
        i++;
      }
      blocks.push(
        <ul key={key++} style={{ margin: "4px 0", paddingLeft: 20 }}>
          {items.map((it, ii) => (
            <li key={ii} style={{ ...listItemStyle, marginLeft: it.indent >= 2 ? 16 : 0 }}>
              {renderInline(it.text, `li${key}-${ii}`)}
            </li>
          ))}
        </ul>
      );
      continue;
    }

    // Numbered list
    if (/^\s*\d+\.\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^\s*\d+\.\s+/, ""));
        i++;
      }
      blocks.push(
        <ol key={key++} style={{ margin: "4px 0", paddingLeft: 22 }}>
          {items.map((it, ii) => (
            <li key={ii} style={listItemStyle}>{renderInline(it, `ol${key}-${ii}`)}</li>
          ))}
        </ol>
      );
      continue;
    }

    if (!line.trim()) { i++; continue; }

    // Paragraph
    const para = [];
    while (i < lines.length && lines[i].trim()
      && !/^\s*[-•*]\s+/.test(lines[i])
      && !/^\s*\d+\.\s+/.test(lines[i])
      && !/^#{1,4}\s+/.test(lines[i])
      && !/^\s*>\s?/.test(lines[i])
      && !lines[i].trim().startsWith("```")
      && !(lines[i].includes("|") && i + 1 < lines.length && /^\s*\|?[\s:-]+\|[\s:|-]*$/.test(lines[i + 1]))) {
      para.push(lines[i]);
      i++;
    }
    blocks.push(
      <p key={key++} style={{ margin: "4px 0", lineHeight: 1.55 }}>
        {para.map((pl, pi) => (
          <React.Fragment key={pi}>
            {pi > 0 && <br />}
            {renderInline(pl, `p${key}-${pi}`)}
          </React.Fragment>
        ))}
      </p>
    );
  }

  return <div style={{ fontSize: 13, ...style }}>{blocks}</div>;
}
