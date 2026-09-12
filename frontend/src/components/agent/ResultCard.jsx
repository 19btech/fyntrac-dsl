import React from "react";
import { CheckCircle2, XCircle, AlertTriangle, TrendingUp } from "lucide-react";
import { formatAmount } from "./MarkdownLite";

/**
 * ResultCard — renders the results of key agent tools (dry-run, reconciliation,
 * readiness check) as a compact, scannable card instead of raw JSON. Returns
 * null for anything it doesn't recognise so the caller can fall back to JSON.
 */

const Tile = ({ label, value, tone }) => (
  <div style={{
    flex: "1 1 auto", minWidth: 78, padding: "6px 10px", borderRadius: 8,
    background: "rgba(99,102,241,0.06)",
    border: "1px solid rgba(99,102,241,0.12)",
  }}>
    <div style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.3 }}>{label}</div>
    <div style={{
      fontSize: 15, fontWeight: 700, fontVariantNumeric: "tabular-nums",
      color: tone === "bad" ? "#dc2626" : tone === "good" ? "#16a34a" : "#1e293b",
    }}>{value}</div>
  </div>
);

const MiniTable = ({ head, rows }) => (
  <div style={{ overflowX: "auto", marginTop: 6 }}>
    <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 11.5 }}>
      <thead>
        <tr>
          {head.map((h, i) => (
            <th key={i} style={{
              textAlign: i === 0 ? "left" : "right", padding: "3px 8px",
              borderBottom: "1px solid rgba(99,102,241,0.3)", color: "#475569", fontWeight: 600,
            }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, ri) => (
          <tr key={ri}>
            {r.map((c, ci) => (
              <td key={ci} style={{
                textAlign: ci === 0 ? "left" : "right", padding: "3px 8px",
                borderBottom: "1px solid rgba(0,0,0,0.05)",
                fontVariantNumeric: ci === 0 ? "normal" : "tabular-nums",
                whiteSpace: ci === 0 ? "normal" : "nowrap",
              }}>{c}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

const Warnings = ({ items }) => (
  !items || !items.length ? null : (
    <div style={{
      marginTop: 6, borderLeft: "3px solid #f59e0b", background: "rgba(245,158,11,0.10)",
      borderRadius: 6, padding: "6px 10px",
    }}>
      {items.slice(0, 4).map((w, i) => (
        <div key={i} style={{ display: "flex", gap: 5, fontSize: 11.5, color: "#92600a", marginBottom: i < items.length - 1 ? 3 : 0 }}>
          <AlertTriangle size={12} style={{ flexShrink: 0, marginTop: 1 }} /> <span>{w}</span>
        </div>
      ))}
    </div>
  )
);

const Wrap = ({ children }) => (
  <div style={{ padding: "8px 2px" }}>{children}</div>
);

const tileRow = { display: "flex", flexWrap: "wrap", gap: 6 };

export default function ResultCard({ name, result }) {
  if (!result || typeof result !== "object") return null;

  // dry_run_rule wraps the payload under `result`; dry_run_template is flat.
  const dry = name === "dry_run_rule" ? result.result : result;

  if ((name === "dry_run_template" || name === "dry_run_rule") && dry &&
      (dry.transaction_count != null || dry.by_transaction_type)) {
    const byType = dry.by_transaction_type || {};
    const typeRows = Object.entries(byType).map(([t, v]) => [
      t, String(v.count ?? ""), formatAmount(v.total, { currency: true }),
    ]);
    return (
      <Wrap>
        <div style={tileRow}>
          <Tile label="Transactions" value={formatAmount(dry.transaction_count)} />
          <Tile label="Total amount" value={formatAmount(dry.total_amount, { currency: true })} />
          {dry.row_count_input != null && <Tile label="Input rows" value={formatAmount(dry.row_count_input)} />}
        </div>
        {typeRows.length > 0 && <MiniTable head={["Transaction type", "Count", "Total"]} rows={typeRows} />}
        <Warnings items={dry.sanity_warnings} />
      </Wrap>
    );
  }

  if (name === "reconcile_workbook_outputs" && result.status) {
    const ok = result.status === "reconciled";
    const noVals = result.status === "no_cached_values";
    return (
      <Wrap>
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
          {ok ? <CheckCircle2 size={15} color="#16a34a" />
            : <XCircle size={15} color="#dc2626" />}
          <strong style={{ fontSize: 13, color: ok ? "#16a34a" : "#dc2626" }}>
            {ok ? "Reconciled" : noVals ? "No computed values in workbook" : "Differences found"}
          </strong>
        </div>
        {!noVals && (
          <div style={tileRow}>
            <Tile label="Matched" value={`${result.matched ?? 0}/${result.compared ?? 0}`}
                  tone={ok ? "good" : "bad"} />
            <Tile label="Match rate" value={`${Math.round((result.match_rate ?? 0) * 100)}%`}
                  tone={ok ? "good" : undefined} />
            <Tile label="Tolerance" value={formatAmount(result.tolerance, { currency: true })} />
            {result.max_abs_diff != null &&
              <Tile label="Max diff" value={formatAmount(result.max_abs_diff, { currency: true })} />}
          </div>
        )}
        {Array.isArray(result.mismatches) && result.mismatches.length > 0 && (
          <MiniTable
            head={["Instrument", "Type", "Expected", "Actual", "Diff"]}
            rows={result.mismatches.slice(0, 8).map(m => [
              m.instrumentid, m.transactiontype,
              formatAmount(m.expected, { currency: true }),
              formatAmount(m.actual, { currency: true }),
              formatAmount(m.diff, { currency: true }),
            ])}
          />
        )}
        {result.detail && <Warnings items={[result.detail]} />}
      </Wrap>
    );
  }

  if (name === "verify_rule_complete" && Array.isArray(result.checklist)) {
    return (
      <Wrap>
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
          {result.overall_ready ? <CheckCircle2 size={15} color="#16a34a" />
            : <AlertTriangle size={15} color="#f59e0b" />}
          <strong style={{ fontSize: 13, color: result.overall_ready ? "#16a34a" : "#92600a" }}>
            {result.overall_ready ? "Ready" : "Not ready yet"}
          </strong>
        </div>
        {result.checklist.map((c, i) => (
          <div key={i} style={{ display: "flex", gap: 6, fontSize: 11.5, marginBottom: 2, alignItems: "flex-start" }}>
            {c.ok ? <CheckCircle2 size={12} color="#16a34a" style={{ flexShrink: 0, marginTop: 1 }} />
              : <XCircle size={12} color="#dc2626" style={{ flexShrink: 0, marginTop: 1 }} />}
            <span style={{ color: "#475569" }}>
              <strong>{String(c.check || "").replace(/_/g, " ")}</strong>
              {c.detail ? ` — ${c.detail}` : ""}
            </span>
          </div>
        ))}
      </Wrap>
    );
  }

  return null;
}

export const RESULT_CARD_TOOLS = new Set([
  "dry_run_template", "dry_run_rule", "reconcile_workbook_outputs", "verify_rule_complete",
]);
