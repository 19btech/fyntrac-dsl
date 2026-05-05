import React, { useMemo, useState } from "react";
import {
  Box,
  Card,
  CardContent,
  Tabs,
  Tab,
  TextField,
  Chip,
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  TableContainer,
  Typography,
  InputAdornment,
  alpha,
} from "@mui/material";
import {
  Search,
  Database,
  Receipt,
  Inbox,
  AlertTriangle,
  AlertCircle,
  CheckCircle2,
} from "lucide-react";

// Datatypes accepted by the backend's /events/upload endpoint.
const KNOWN_DATATYPES = new Set([
  "string",
  "date",
  "boolean",
  "decimal",
  "integer",
  "int",
]);

const VALID_EVENT_TYPES = new Set(["activity", "reference"]);
const VALID_EVENT_TABLES = new Set(["standard", "custom"]);

const TABLE_SX = {
  "& thead th": {
    position: "sticky",
    top: 0,
    zIndex: 1,
    bgcolor: "#FAFBFC",
    color: "#495057",
    fontWeight: 600,
    fontSize: "0.75rem",
    letterSpacing: "0.04em",
    textTransform: "uppercase",
    borderBottom: "1px solid #E9ECEF",
    py: 1.25,
    fontFamily: "inherit",
  },
  "& tbody td": {
    fontSize: "0.8125rem",
    color: "#212529",
    borderBottom: "1px solid #F1F3F5",
    py: 1.25,
    fontFamily: "inherit",
  },
  "& tbody tr": {
    transition: "background-color 120ms ease",
  },
  "& tbody tr:hover": {
    bgcolor: (theme) => alpha(theme.palette.primary.main, 0.04),
  },
  "& tbody tr:last-of-type td": { borderBottom: 0 },
};

const EmptyState = ({ icon: Icon, title, hint, tone = "neutral" }) => {
  const palette =
    tone === "success"
      ? { bg: "#E8F5E9", fg: "#388E3C" }
      : { bg: "#F1F3F5", fg: "#ADB5BD" };
  return (
    <Box
      sx={{
        py: 6,
        px: 3,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: 1.5,
        color: "#6C757D",
      }}
    >
      <Box
        sx={{
          width: 56,
          height: 56,
          borderRadius: "50%",
          bgcolor: palette.bg,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: palette.fg,
        }}
      >
        <Icon size={26} />
      </Box>
      <Typography variant="subtitle2" sx={{ color: "#495057", fontWeight: 600 }}>
        {title}
      </Typography>
      <Typography
        variant="caption"
        sx={{ color: "#6C757D", textAlign: "center", maxWidth: 360 }}
      >
        {hint}
      </Typography>
    </Box>
  );
};

const Toolbar = ({ count, query, onQueryChange, label, countLabel }) => (
  <Box
    sx={{
      px: 2,
      py: 1.25,
      display: "flex",
      alignItems: "center",
      gap: 1.5,
      borderBottom: "1px solid #F1F3F5",
      bgcolor: "#FFFFFF",
    }}
  >
    <TextField
      size="small"
      value={query}
      onChange={(e) => onQueryChange(e.target.value)}
      placeholder={`Search ${label.toLowerCase()}…`}
      variant="outlined"
      InputProps={{
        startAdornment: (
          <InputAdornment position="start">
            <Search size={14} color="#ADB5BD" />
          </InputAdornment>
        ),
        sx: {
          fontSize: "0.8125rem",
          "& fieldset": { borderColor: "#E9ECEF" },
          "&:hover fieldset": { borderColor: "#D0D5DD" },
          "&.Mui-focused fieldset": { borderColor: "#5B5FED" },
        },
      }}
      sx={{ flex: 1, maxWidth: 360 }}
    />
    <Box sx={{ flex: 1 }} />
    <Chip
      size="small"
      label={`${count} ${countLabel || (count === 1 ? "row" : "rows")}`}
      sx={{
        bgcolor: "#EEF0FE",
        color: "#5B5FED",
        fontWeight: 600,
        fontSize: "0.7rem",
        height: 22,
        borderRadius: 1,
      }}
    />
  </Box>
);

// Client-side validation of the loaded events + transactions. Surfaces issues
// the upload may have silently accepted (or that the user should fix before
// re-uploading). Each issue: { severity, scope, target, message }.
const validateData = (events, transactions) => {
  const issues = [];

  for (const ev of events || []) {
    const evName = ev.event_name || "(unnamed event)";
    const fields = ev.fields || [];
    const evType = String(ev.eventType || ev.event_type || "activity").toLowerCase();
    const evTable = String(ev.eventTable || ev.event_table || "standard").toLowerCase();

    if (!ev.event_name) {
      issues.push({
        severity: "error",
        scope: "Event",
        target: "—",
        message: "Event has no name.",
      });
    }
    if (!fields.length) {
      issues.push({
        severity: "warning",
        scope: "Event",
        target: evName,
        message: "No fields defined for this event.",
      });
    }
    if (evType && !VALID_EVENT_TYPES.has(evType)) {
      issues.push({
        severity: "error",
        scope: "Event",
        target: evName,
        message: `Unknown event type "${evType}". Expected: activity or reference.`,
      });
    }
    if (evTable && !VALID_EVENT_TABLES.has(evTable)) {
      issues.push({
        severity: "error",
        scope: "Event",
        target: evName,
        message: `Unknown event table "${evTable}". Expected: standard or custom.`,
      });
    }
    if (evTable === "standard" && evType !== "activity") {
      issues.push({
        severity: "error",
        scope: "Event",
        target: evName,
        message: `Standard event table requires eventType "activity", got "${evType}".`,
      });
    }

    const seenFields = new Map();
    for (const f of fields) {
      const fname = (f && f.name) || "(unnamed field)";
      const dt = String((f && f.datatype) || "").toLowerCase();
      if (!f || !f.name) {
        issues.push({
          severity: "error",
          scope: "Field",
          target: `${evName}.—`,
          message: "Field has no name.",
        });
      }
      if (!dt) {
        issues.push({
          severity: "error",
          scope: "Field",
          target: `${evName}.${fname}`,
          message: "Missing datatype.",
        });
      } else if (!KNOWN_DATATYPES.has(dt)) {
        issues.push({
          severity: "warning",
          scope: "Field",
          target: `${evName}.${fname}`,
          message: `Unrecognised datatype "${f.datatype}". Expected one of: string, date, boolean, decimal, integer.`,
        });
      }
      if (f && f.name) {
        const lower = String(f.name).toLowerCase();
        if (seenFields.has(lower)) {
          issues.push({
            severity: "error",
            scope: "Field",
            target: `${evName}.${f.name}`,
            message: `Duplicate field name in event "${evName}".`,
          });
        } else {
          seenFields.set(lower, true);
        }
      }
    }
  }

  // Cross-event: duplicate event names
  const seenEvents = new Map();
  for (const ev of events || []) {
    const lower = String(ev.event_name || "").toLowerCase();
    if (!lower) continue;
    if (seenEvents.has(lower)) {
      issues.push({
        severity: "error",
        scope: "Event",
        target: ev.event_name,
        message: `Duplicate event name "${ev.event_name}".`,
      });
    } else {
      seenEvents.set(lower, true);
    }
  }

  // Transactions
  const seenTxns = new Map();
  for (const t of transactions || []) {
    const name =
      typeof t === "string" ? t : t && (t.transactiontype || t.name || "");
    if (!name || !String(name).trim()) {
      issues.push({
        severity: "error",
        scope: "Transaction",
        target: "—",
        message: "Transaction type is blank.",
      });
      continue;
    }
    const lower = String(name).toLowerCase();
    if (seenTxns.has(lower)) {
      issues.push({
        severity: "warning",
        scope: "Transaction",
        target: String(name),
        message: `Duplicate transaction type "${name}".`,
      });
    } else {
      seenTxns.set(lower, true);
    }
    if (/\s/.test(String(name))) {
      issues.push({
        severity: "warning",
        scope: "Transaction",
        target: String(name),
        message:
          "Transaction type contains whitespace — rules reference it by exact name.",
      });
    }
  }

  return issues;
};

const DataPreviewPanel = ({ events = [], transactions = [] }) => {
  const [tab, setTab] = useState(0);
  const [eventQuery, setEventQuery] = useState("");
  const [txnQuery, setTxnQuery] = useState("");
  const [issueQuery, setIssueQuery] = useState("");

  // Flatten events into one row per (event, field). Show the RAW datatype
  // exactly as it was uploaded — no remapping ("integer" stays "integer", not
  // "whole number"). Unknown datatypes are flagged in the Issues tab and
  // get a warning-tinted chip in the table.
  const eventRows = useMemo(() => {
    const rows = [];
    for (const ev of events || []) {
      const fields = ev.fields || [];
      if (fields.length === 0) {
        rows.push({
          eventName: ev.event_name,
          eventType: ev.eventType || ev.event_type || "—",
          eventTable: ev.eventTable || ev.event_table || "—",
          fieldName: "—",
          dataType: "—",
          dataTypeKnown: true,
        });
        continue;
      }
      for (const f of fields) {
        const dt = f.datatype || "";
        rows.push({
          eventName: ev.event_name,
          eventType: ev.eventType || ev.event_type || "—",
          eventTable: ev.eventTable || ev.event_table || "—",
          fieldName: f.name,
          dataType: dt || "—",
          dataTypeKnown: !dt || KNOWN_DATATYPES.has(String(dt).toLowerCase()),
        });
      }
    }
    return rows;
  }, [events]);

  const filteredEvents = useMemo(() => {
    const q = eventQuery.trim().toLowerCase();
    if (!q) return eventRows;
    return eventRows.filter((r) =>
      [r.eventName, r.eventType, r.eventTable, r.fieldName, r.dataType]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q))
    );
  }, [eventRows, eventQuery]);

  const txnRows = useMemo(() => {
    return (transactions || []).map((t) =>
      typeof t === "string" ? { transactiontype: t } : t
    );
  }, [transactions]);

  const filteredTxns = useMemo(() => {
    const q = txnQuery.trim().toLowerCase();
    if (!q) return txnRows;
    return txnRows.filter((r) =>
      String(r.transactiontype || r.name || "").toLowerCase().includes(q)
    );
  }, [txnRows, txnQuery]);

  const issues = useMemo(
    () => validateData(events, transactions),
    [events, transactions]
  );

  const filteredIssues = useMemo(() => {
    const q = issueQuery.trim().toLowerCase();
    if (!q) return issues;
    return issues.filter((it) =>
      [it.scope, it.target, it.message, it.severity]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q))
    );
  }, [issues, issueQuery]);

  const errorCount = issues.filter((i) => i.severity === "error").length;
  const warningCount = issues.filter((i) => i.severity === "warning").length;

  let issueBadgeLabel = null;
  let issueBadgeColor = null;
  if (errorCount > 0) {
    issueBadgeLabel = `${errorCount} ${errorCount === 1 ? "error" : "errors"}${
      warningCount ? ` · ${warningCount} warn` : ""
    }`;
    issueBadgeColor = { bg: "#FDECEA", fg: "#D32F2F" };
  } else if (warningCount > 0) {
    issueBadgeLabel = `${warningCount} ${
      warningCount === 1 ? "warning" : "warnings"
    }`;
    issueBadgeColor = { bg: "#FFF8E1", fg: "#B26A00" };
  }

  const issuesTabIcon =
    errorCount > 0
      ? AlertCircle
      : warningCount > 0
      ? AlertTriangle
      : CheckCircle2;
  const IssuesTabIconComp = issuesTabIcon;

  return (
    <Card sx={{ mt: 3, overflow: "hidden", border: "1px solid #E9ECEF" }}>
      <CardContent sx={{ p: 0, "&:last-child": { pb: 0 } }}>
        <Box
          sx={{
            borderBottom: "1px solid #F1F3F5",
            px: 2,
            bgcolor: "#FFFFFF",
          }}
        >
          <Tabs
            value={tab}
            onChange={(_, v) => setTab(v)}
            sx={{
              minHeight: 44,
              "& .MuiTab-root": {
                minHeight: 44,
                textTransform: "none",
                fontSize: "0.8125rem",
                fontWeight: 600,
                color: "#6C757D",
                gap: 0.75,
                fontFamily: "inherit",
              },
              "& .Mui-selected": { color: "#5B5FED" },
              "& .MuiTabs-indicator": { backgroundColor: "#5B5FED", height: 2 },
            }}
          >
            <Tab
              icon={<Database size={14} />}
              iconPosition="start"
              label={`Event Definitions${
                (events || []).length ? ` (${events.length})` : ""
              }`}
              data-testid="data-preview-events-tab"
            />
            <Tab
              icon={<Receipt size={14} />}
              iconPosition="start"
              label={`Transactions${
                (transactions || []).length ? ` (${transactions.length})` : ""
              }`}
              data-testid="data-preview-txns-tab"
            />
            <Tab
              icon={<IssuesTabIconComp size={14} />}
              iconPosition="start"
              label={
                <Box
                  component="span"
                  sx={{
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 0.75,
                  }}
                >
                  Issues
                  {issueBadgeLabel && (
                    <Chip
                      size="small"
                      label={issueBadgeLabel}
                      sx={{
                        bgcolor: issueBadgeColor.bg,
                        color: issueBadgeColor.fg,
                        fontWeight: 600,
                        fontSize: "0.65rem",
                        height: 18,
                        borderRadius: 0.75,
                      }}
                    />
                  )}
                </Box>
              }
              data-testid="data-preview-issues-tab"
            />
          </Tabs>
        </Box>

        {tab === 0 && (
          <Box>
            <Toolbar
              count={filteredEvents.length}
              query={eventQuery}
              onQueryChange={setEventQuery}
              label="event fields"
            />
            {filteredEvents.length === 0 ? (
              <EmptyState
                icon={Inbox}
                title={eventQuery ? "No matches" : "No event definitions yet"}
                hint={
                  eventQuery
                    ? "Try a different search term."
                    : "Upload a Reference Data file (.xlsx) to see your events here."
                }
              />
            ) : (
              <TableContainer sx={{ maxHeight: 360 }}>
                <Table
                  size="small"
                  stickyHeader
                  sx={TABLE_SX}
                  data-testid="event-defs-table"
                >
                  <TableHead>
                    <TableRow>
                      <TableCell>Event</TableCell>
                      <TableCell>Field</TableCell>
                      <TableCell>Data Type</TableCell>
                      <TableCell>Event Type</TableCell>
                      <TableCell>Table</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredEvents.map((r, i) => (
                      <TableRow key={`${r.eventName}-${r.fieldName}-${i}`}>
                        <TableCell
                          sx={{ fontWeight: 600, color: "#14213d" }}
                        >
                          {r.eventName}
                        </TableCell>
                        <TableCell>{r.fieldName}</TableCell>
                        <TableCell>
                          <Chip
                            size="small"
                            label={r.dataType}
                            sx={{
                              height: 20,
                              fontSize: "0.7rem",
                              bgcolor: r.dataTypeKnown
                                ? "#F1F3F5"
                                : "#FFF8E1",
                              color: r.dataTypeKnown ? "#495057" : "#B26A00",
                              border: r.dataTypeKnown
                                ? "1px solid transparent"
                                : "1px solid #FFE082",
                              borderRadius: 0.75,
                              fontWeight: 500,
                              fontFamily: "inherit",
                            }}
                          />
                        </TableCell>
                        <TableCell sx={{ color: "#6C757D" }}>
                          {r.eventType}
                        </TableCell>
                        <TableCell sx={{ color: "#6C757D" }}>
                          {r.eventTable}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Box>
        )}

        {tab === 1 && (
          <Box>
            <Toolbar
              count={filteredTxns.length}
              query={txnQuery}
              onQueryChange={setTxnQuery}
              label="transaction types"
            />
            {filteredTxns.length === 0 ? (
              <EmptyState
                icon={Inbox}
                title={txnQuery ? "No matches" : "No transaction types yet"}
                hint={
                  txnQuery
                    ? "Try a different search term."
                    : "Add a `transactions` sheet to your Reference Data file with a `transactiontype` column."
                }
              />
            ) : (
              <TableContainer sx={{ maxHeight: 360 }}>
                <Table
                  size="small"
                  stickyHeader
                  sx={TABLE_SX}
                  data-testid="transactions-table"
                >
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ width: 56 }}>#</TableCell>
                      <TableCell>Transaction Type</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredTxns.map((r, i) => (
                      <TableRow key={`${r.transactiontype}-${i}`}>
                        <TableCell sx={{ color: "#ADB5BD", width: 56 }}>
                          {i + 1}
                        </TableCell>
                        <TableCell sx={{ color: "#14213d", fontWeight: 500 }}>
                          {r.transactiontype || r.name}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Box>
        )}

        {tab === 2 && (
          <Box>
            <Toolbar
              count={filteredIssues.length}
              query={issueQuery}
              onQueryChange={setIssueQuery}
              label="issues"
              countLabel={filteredIssues.length === 1 ? "issue" : "issues"}
            />
            {filteredIssues.length === 0 ? (
              issues.length === 0 ? (
                <EmptyState
                  icon={CheckCircle2}
                  tone="success"
                  title="All clear"
                  hint="No validation issues found in the uploaded reference data."
                />
              ) : (
                <EmptyState
                  icon={Inbox}
                  title="No matches"
                  hint="Try a different search term."
                />
              )
            ) : (
              <TableContainer sx={{ maxHeight: 360 }}>
                <Table
                  size="small"
                  stickyHeader
                  sx={TABLE_SX}
                  data-testid="issues-table"
                >
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ width: 100 }}>Severity</TableCell>
                      <TableCell sx={{ width: 120 }}>Scope</TableCell>
                      <TableCell sx={{ width: 240 }}>Target</TableCell>
                      <TableCell>Message</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredIssues.map((it, i) => {
                      const isErr = it.severity === "error";
                      return (
                        <TableRow key={`${it.scope}-${it.target}-${i}`}>
                          <TableCell>
                            <Chip
                              size="small"
                              icon={
                                isErr ? (
                                  <AlertCircle size={12} />
                                ) : (
                                  <AlertTriangle size={12} />
                                )
                              }
                              label={isErr ? "Error" : "Warning"}
                              sx={{
                                height: 22,
                                fontSize: "0.7rem",
                                fontWeight: 600,
                                borderRadius: 0.75,
                                bgcolor: isErr ? "#FDECEA" : "#FFF8E1",
                                color: isErr ? "#D32F2F" : "#B26A00",
                                fontFamily: "inherit",
                                "& .MuiChip-icon": {
                                  color: isErr ? "#D32F2F" : "#B26A00",
                                  ml: "6px",
                                },
                              }}
                            />
                          </TableCell>
                          <TableCell sx={{ color: "#495057", fontWeight: 500 }}>
                            {it.scope}
                          </TableCell>
                          <TableCell
                            sx={{ color: "#14213d", fontWeight: 500 }}
                          >
                            {it.target}
                          </TableCell>
                          <TableCell sx={{ color: "#495057" }}>
                            {it.message}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

export default DataPreviewPanel;
