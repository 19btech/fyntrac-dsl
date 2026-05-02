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
import { Search, Database, Receipt, Inbox } from "lucide-react";

const formatDataType = (dt) => {
  const map = {
    string: "text",
    decimal: "number",
    integer: "whole number",
    int: "whole number",
    boolean: "yes/no",
    bool: "yes/no",
    date: "date",
  };
  return map[(dt || "").toLowerCase()] || dt || "—";
};

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
  },
  "& tbody td": {
    fontSize: "0.8125rem",
    color: "#212529",
    borderBottom: "1px solid #F1F3F5",
    py: 1.25,
  },
  "& tbody tr": {
    transition: "background-color 120ms ease",
  },
  "& tbody tr:hover": {
    bgcolor: (theme) => alpha(theme.palette.primary.main, 0.04),
  },
  "& tbody tr:last-of-type td": { borderBottom: 0 },
};

const EmptyState = ({ icon: Icon, title, hint }) => (
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
        bgcolor: "#F1F3F5",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#ADB5BD",
      }}
    >
      <Icon size={26} />
    </Box>
    <Typography variant="subtitle2" sx={{ color: "#495057", fontWeight: 600 }}>
      {title}
    </Typography>
    <Typography variant="caption" sx={{ color: "#6C757D", textAlign: "center", maxWidth: 320 }}>
      {hint}
    </Typography>
  </Box>
);

const Toolbar = ({ count, query, onQueryChange, label }) => (
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
      label={`${count} ${count === 1 ? "row" : "rows"}`}
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

const DataPreviewPanel = ({ events = [], transactions = [] }) => {
  const [tab, setTab] = useState(0);
  const [eventQuery, setEventQuery] = useState("");
  const [txnQuery, setTxnQuery] = useState("");

  // Flatten events into one row per (event, field)
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
        });
        continue;
      }
      for (const f of fields) {
        rows.push({
          eventName: ev.event_name,
          eventType: ev.eventType || ev.event_type || "—",
          eventTable: ev.eventTable || ev.event_table || "—",
          fieldName: f.name,
          dataType: formatDataType(f.datatype),
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

  return (
    <Card sx={{ mt: 3, overflow: "hidden", border: "1px solid #E9ECEF" }}>
      <CardContent sx={{ p: 0, "&:last-child": { pb: 0 } }}>
        <Box sx={{ borderBottom: "1px solid #F1F3F5", px: 2, bgcolor: "#FFFFFF" }}>
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
              },
              "& .Mui-selected": { color: "#5B5FED" },
              "& .MuiTabs-indicator": { backgroundColor: "#5B5FED", height: 2 },
            }}
          >
            <Tab
              icon={<Database size={14} />}
              iconPosition="start"
              label={`Event Definitions${(events || []).length ? ` (${events.length})` : ""}`}
              data-testid="data-preview-events-tab"
            />
            <Tab
              icon={<Receipt size={14} />}
              iconPosition="start"
              label={`Transactions${(transactions || []).length ? ` (${transactions.length})` : ""}`}
              data-testid="data-preview-txns-tab"
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
                <Table size="small" stickyHeader sx={TABLE_SX} data-testid="event-defs-table">
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
                        <TableCell sx={{ fontWeight: 600, color: "#14213d" }}>{r.eventName}</TableCell>
                        <TableCell>{r.fieldName}</TableCell>
                        <TableCell>
                          <Chip
                            size="small"
                            label={r.dataType}
                            sx={{
                              height: 20,
                              fontSize: "0.7rem",
                              bgcolor: "#F1F3F5",
                              color: "#495057",
                              borderRadius: 0.75,
                            }}
                          />
                        </TableCell>
                        <TableCell sx={{ color: "#6C757D" }}>{r.eventType}</TableCell>
                        <TableCell sx={{ color: "#6C757D" }}>{r.eventTable}</TableCell>
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
                <Table size="small" stickyHeader sx={TABLE_SX} data-testid="transactions-table">
                  <TableHead>
                    <TableRow>
                      <TableCell>#</TableCell>
                      <TableCell>Transaction Type</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredTxns.map((r, i) => (
                      <TableRow key={`${r.transactiontype}-${i}`}>
                        <TableCell sx={{ color: "#ADB5BD", width: 56 }}>{i + 1}</TableCell>
                        <TableCell sx={{ fontFamily: "monospace", fontSize: "0.8125rem", color: "#14213d" }}>
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
      </CardContent>
    </Card>
  );
};

export default DataPreviewPanel;
