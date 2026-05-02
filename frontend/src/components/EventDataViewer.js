import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import * as XLSX from "xlsx";
import { useToast } from "./ToastProvider";
import { X, Database, Download } from "lucide-react";
import { Button, IconButton, Chip, Box, Typography, Table, TableHead, TableBody, TableRow, TableCell, Card, Tabs, Tab, Alert } from '@mui/material';
import { API } from '../config';

const EventDataViewer = ({ onClose }) => {
  const [eventDataSummary, setEventDataSummary] = useState([]);
  const [selectedEvent, setSelectedEvent] = useState(null);
  const [eventData, setEventData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [leftTab, setLeftTab] = useState(0); // 0 = Events, 1 = Errors
  const [uploadErrors, setUploadErrors] = useState([]);
  const [instrumentWarning, setInstrumentWarning] = useState(null); // array of instrument ids or null
  const toast = useToast();

  const loadEventDataSummary = useCallback(async () => {
    try {
      // Clear prior viewer state before loading summary
      setSelectedEvent(null);
      setEventData(null);
      setLeftTab(0);
      const response = await axios.get(`${API}/event-data`);
      setEventDataSummary(response.data);
    } catch (error) {
      console.error("Error loading event data summary:", error);
    }
  }, []);

  const loadEventData = useCallback(async (eventName) => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/event-data/${eventName}`);
      setEventData(response.data);
      setSelectedEvent(eventName);
    } catch (error) {
      toast.error("Failed to load event data");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    loadEventDataSummary();
    // load persisted upload errors
    try {
      const raw = localStorage.getItem('lastEventDataUploadErrors');
      if (raw) setUploadErrors(JSON.parse(raw));
    } catch (e) {}
    // load instrument warning from last JSON import
    try {
      const raw = localStorage.getItem('importSelectedInstruments');
      if (raw) setInstrumentWarning(JSON.parse(raw));
    } catch (e) {}
    const uploadErrorsHandler = (e) => {
      try {
        const detail = e?.detail || JSON.parse(localStorage.getItem('lastEventDataUploadErrors') || '[]');
        setUploadErrors(detail || []);
      } catch (err) {}
    };

    const clearViewerHandler = () => {
      try {
        setEventDataSummary([]);
        setSelectedEvent(null);
        setEventData(null);
        setUploadErrors([]);
        setInstrumentWarning(null);
        setLeftTab(0);
        localStorage.removeItem('importSelectedInstruments');
      } catch (err) {}
    };

    const refreshHandler = () => {
      loadEventDataSummary();
      // Re-read instrument warning in case a new import just completed
      try {
        const raw = localStorage.getItem('importSelectedInstruments');
        setInstrumentWarning(raw ? JSON.parse(raw) : null);
      } catch (e) {}
    };

    window.addEventListener('dsl-upload-errors', uploadErrorsHandler);
    window.addEventListener('dsl-clear-event-viewer', clearViewerHandler);
    window.addEventListener('dsl-event-data-refresh', refreshHandler);
    return () => {
      window.removeEventListener('dsl-upload-errors', uploadErrorsHandler);
      window.removeEventListener('dsl-clear-event-viewer', clearViewerHandler);
      window.removeEventListener('dsl-event-data-refresh', refreshHandler);
    };
  }, [loadEventDataSummary]);

  // When summary is loaded and there's no selection, auto-select the first event
  useEffect(() => {
    if (eventDataSummary && eventDataSummary.length > 0 && !selectedEvent) {
      loadEventData(eventDataSummary[0].event_name);
    }
  }, [eventDataSummary, loadEventData, selectedEvent]);

  const [exporting, setExporting] = useState(false);

  // Excel sheet names: max 31 chars, cannot contain : \ / ? * [ ]
  const sanitizeSheetName = (name, used) => {
    let safe = String(name || "Sheet").replace(/[:\\\/\?\*\[\]]/g, "_");
    if (safe.length > 31) safe = safe.slice(0, 31);
    if (!safe.trim()) safe = "Sheet";
    let candidate = safe;
    let n = 2;
    while (used.has(candidate.toLowerCase())) {
      const suffix = `_${n}`;
      candidate = safe.slice(0, 31 - suffix.length) + suffix;
      n += 1;
    }
    used.add(candidate.toLowerCase());
    return candidate;
  };

  // Coerce extended-JSON style objects into plain scalars so they render
  // sensibly in Excel cells.
  const flattenCellForExcel = (value) => {
    if (value === null || value === undefined) return "";
    if (typeof value === "object") {
      if (value.$date) return typeof value.$date === "string" ? value.$date : String(value.$date);
      if (value.$oid) return value.$oid;
      if (value.$numberDecimal) return Number(value.$numberDecimal);
      if (value.$numberLong) return Number(value.$numberLong);
      return JSON.stringify(value);
    }
    return value;
  };

  const exportAllEventsToExcel = async () => {
    if (!eventDataSummary || eventDataSummary.length === 0) {
      toast.error("No event data to export");
      return;
    }
    setExporting(true);
    try {
      const wb = XLSX.utils.book_new();
      const usedSheetNames = new Set();
      let exportedCount = 0;

      for (const summary of eventDataSummary) {
        const eventName = summary.event_name;
        let rows = [];
        try {
          const resp = await axios.get(`${API}/event-data/${eventName}`);
          rows = (resp.data && resp.data.data_rows) || [];
        } catch (err) {
          // Skip events that fail to load but keep going on the rest.
          continue;
        }

        // Build a stable column order: union of keys across all rows in order
        // of first appearance.
        const headers = [];
        const seen = new Set();
        for (const r of rows) {
          for (const k of Object.keys(r || {})) {
            if (!seen.has(k)) {
              seen.add(k);
              headers.push(k);
            }
          }
        }

        const flatRows = rows.map((r) => {
          const out = {};
          for (const h of headers) out[h] = flattenCellForExcel(r ? r[h] : "");
          return out;
        });

        const ws =
          headers.length > 0
            ? XLSX.utils.json_to_sheet(flatRows, { header: headers })
            : XLSX.utils.aoa_to_sheet([["(no rows)"]]);

        const sheetName = sanitizeSheetName(eventName, usedSheetNames);
        XLSX.utils.book_append_sheet(wb, ws, sheetName);
        exportedCount += 1;
      }

      if (exportedCount === 0) {
        toast.error("No event data could be exported");
        return;
      }

      const stamp = new Date().toISOString().slice(0, 10);
      XLSX.writeFile(wb, `event_data_${stamp}.xlsx`);
      toast.success(`Exported ${exportedCount} event${exportedCount > 1 ? "s" : ""} to Excel`);
    } catch (err) {
      console.error(err);
      toast.error("Failed to export event data");
    } finally {
      setExporting(false);
    }
  };

  const getColumnHeaders = () => {
    if (!eventData || !eventData.data_rows || eventData.data_rows.length === 0) return [];
    // Union all keys across every row so columns present only in later rows are not missed.
    const seen = new Set();
    const headers = [];
    for (const row of eventData.data_rows) {
      for (const key of Object.keys(row)) {
        if (!seen.has(key)) {
          seen.add(key);
          headers.push(key);
        }
      }
    }
    return headers;
  };

  // Safely convert any cell value to a renderable string.
  // Handles MongoDB extended JSON objects like {$oid: "..."} and {$date: "..."}.
  const renderCellValue = (value) => {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') {
      if (value.$date) return typeof value.$date === 'string' ? value.$date : String(value.$date);
      if (value.$oid) return value.$oid;
      if (value.$numberDecimal) return value.$numberDecimal;
      if (value.$numberLong) return value.$numberLong;
      return JSON.stringify(value);
    }
    return String(value);
  };

  return (
    <Box 
      sx={{ 
        position: 'fixed', 
        inset: 0, 
        bgcolor: 'rgba(0,0,0,0.5)', 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        zIndex: 1300 
      }} 
      data-testid="event-data-viewer"
    >
      <Card 
        sx={{ 
          width: '95vw', 
          maxWidth: 1200, 
          height: '85vh', 
          display: 'flex', 
          flexDirection: 'column',
          borderRadius: 2
        }}
      >
        {/* Header */}
        <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid #E9ECEF', display: 'flex', alignItems: 'center', justifyContent: 'space-between', bgcolor: '#F8F9FA' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Database size={24} color="#5B5FED" />
            <Box>
              <Typography variant="h4">Event Data Viewer</Typography>
              <Typography variant="body2" color="text.secondary">
                View uploaded event data by event type
              </Typography>
            </Box>
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Button
              variant="outlined"
              size="small"
              onClick={exportAllEventsToExcel}
              startIcon={<Download size={16} />}
              disabled={exporting || eventDataSummary.length === 0}
              data-testid="export-events-excel"
            >
              {exporting ? 'Exporting…' : 'Export Excel'}
            </Button>
            <IconButton onClick={onClose} data-testid="close-data-viewer">
              <X size={20} />
            </IconButton>
          </Box>
        </Box>

        {/* Instrument scope warning banner (from JSON import) */}
        {instrumentWarning && instrumentWarning.length > 0 && (
          <Box sx={{ px: 2, pt: 1.5, pb: 0.5 }}>
            <Alert
              severity="warning"
              onClose={() => setInstrumentWarning(null)}
              sx={{ fontSize: '0.8125rem' }}
            >
              This import has been loaded for {instrumentWarning.length} instrument{instrumentWarning.length > 1 ? 's' : ''} only. Data has been randomly selected for{' '}
              <strong>{instrumentWarning.join(' and ')}</strong>.
              All other instruments from the source JSON have been excluded.
            </Alert>
          </Box>
        )}

        <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          {/* Left Panel - Event List */}
          <Box sx={{ width: 280, borderRight: '1px solid #E9ECEF', display: 'flex', flexDirection: 'column' }}>
            <Box sx={{ p: 0, borderBottom: '1px solid #E9ECEF' }}>
              <Tabs value={leftTab} onChange={(e, v) => setLeftTab(v)} variant="fullWidth">
                <Tab label={`Events (${eventDataSummary.length})`} />
                <Tab label={`Errors (${uploadErrors.length})`} />
              </Tabs>
            </Box>

            <Box sx={{ flex: 1, overflowY: 'auto', p: 1.5 }}>
              {leftTab === 0 && (
                eventDataSummary.length === 0 ? (
                  <Box sx={{ p: 2, textAlign: 'center' }}>
                    <Database size={32} color="#CED4DA" style={{ marginBottom: 8 }} />
                    <Typography variant="body2" color="text.secondary">No event data uploaded yet</Typography>
                  </Box>
                ) : (
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    {eventDataSummary.map((item) => (
                      <Card
                        key={item.event_name}
                        sx={{
                          p: 1.5,
                          cursor: 'pointer',
                          bgcolor: selectedEvent === item.event_name ? '#EEF0FE' : 'transparent',
                          border: '1px solid',
                          borderColor: selectedEvent === item.event_name ? '#5B5FED' : '#E9ECEF',
                          '&:hover': {
                            bgcolor: selectedEvent === item.event_name ? '#EEF0FE' : '#F8F9FA',
                          },
                        }}
                        onClick={() => { setLeftTab(0); loadEventData(item.event_name); }}
                        data-testid={`event-data-item-${item.event_name}`}
                      >
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Typography variant="body2" sx={{ fontWeight: 600, color: selectedEvent === item.event_name ? '#5B5FED' : '#212529' }}>
                            {item.event_name}
                          </Typography>
                          <Chip
                            label={`${item.row_count} rows`}
                            size="small"
                            sx={{ 
                              fontSize: '0.6875rem',
                              height: 20,
                              bgcolor: selectedEvent === item.event_name ? '#5B5FED' : '#D4EDDA',
                              color: selectedEvent === item.event_name ? '#FFFFFF' : '#155724',
                            }}
                          />
                        </Box>
                      </Card>
                    ))}
                  </Box>
                )
              )}

              {/* When Errors tab is selected we intentionally do not render anything in the left panel */}
            </Box>
          </Box>

          {/* Right Panel - Data Table */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            {leftTab === 1 ? (
              // Show errors table when Errors tab selected
              <>
                <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', display: 'flex', alignItems: 'center', justifyContent: 'space-between', bgcolor: '#F8F9FA' }}>
                  <Box>
                    <Typography variant="h6">Upload Errors</Typography>
                    <Typography variant="caption" color="text.secondary">{uploadErrors.length} error(s)</Typography>
                  </Box>
                </Box>
                <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
                  {uploadErrors.length > 0 ? (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell sx={{ fontWeight: 600, bgcolor: '#F8F9FA' }}>Error Type</TableCell>
                          <TableCell sx={{ fontWeight: 600, bgcolor: '#F8F9FA' }}>Message</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {uploadErrors.map((err, i) => (
                          <TableRow key={i} hover>
                            <TableCell sx={{ fontFamily: 'monospace' }}>{err.ErrorType || 'FileLoad'}</TableCell>
                            <TableCell>{err.message}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  ) : (
                    <Box sx={{ textAlign: 'center', py: 6 }}>
                      <Typography variant="body2" color="text.secondary">No upload errors recorded</Typography>
                    </Box>
                  )}
                </Box>
              </>
            ) : !selectedEvent ? (
              <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', bgcolor: '#F8F9FA' }}>
                <Box sx={{ textAlign: 'center' }}>
                  <Database size={64} color="#CED4DA" style={{ marginBottom: 16 }} />
                  <Typography variant="h5" sx={{ mb: 1 }}>Select an Event</Typography>
                  <Typography variant="body2" color="text.secondary">
                    Click on an event to view its data
                  </Typography>
                </Box>
              </Box>
            ) : loading ? (
              <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Box className="animate-spin w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full"></Box>
              </Box>
            ) : (
              <>
                {/* Data Header */}
                <Box sx={{ p: 2, borderBottom: '1px solid #E9ECEF', display: 'flex', alignItems: 'center', justifyContent: 'space-between', bgcolor: '#F8F9FA' }}>
                  <Box>
                    <Typography variant="h6">{selectedEvent}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      {eventData?.data_rows?.length || 0} rows × {getColumnHeaders().length} columns
                    </Typography>
                  </Box>
                </Box>

                {/* Data Table */}
                <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
                  {eventData?.data_rows?.length > 0 ? (
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell sx={{ fontWeight: 600, bgcolor: '#F8F9FA' }}>#</TableCell>
                          {getColumnHeaders().map((header, idx) => (
                            <TableCell key={idx} sx={{ fontWeight: 600, bgcolor: '#F8F9FA', whiteSpace: 'nowrap' }}>
                              {header}
                            </TableCell>
                          ))}
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {eventData.data_rows.map((row, rowIdx) => (
                          <TableRow key={rowIdx} hover>
                            <TableCell sx={{ color: '#6C757D', fontFamily: 'monospace', fontSize: '0.8125rem' }}>
                              {rowIdx + 1}
                            </TableCell>
                            {getColumnHeaders().map((header, colIdx) => (
                              <TableCell key={colIdx} sx={{ fontFamily: 'monospace', fontSize: '0.8125rem', whiteSpace: 'nowrap' }}>
                                {renderCellValue(row[header])}
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  ) : (
                    <Box sx={{ textAlign: 'center', py: 6 }}>
                      <Typography variant="body2" color="text.secondary">
                        No data rows found for this event
                      </Typography>
                    </Box>
                  )}
                </Box>
              </>
            )}
          </Box>
        </Box>
      </Card>
    </Box>
  );
};

export default EventDataViewer;
