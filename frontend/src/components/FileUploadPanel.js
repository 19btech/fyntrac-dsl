import React, { useState, useEffect } from "react";
import axios from "axios";
import { useToast } from "./ToastProvider";
import { Upload, FileText, FileSpreadsheet, Download, CheckCircle, Eye, X, Info, Sparkles } from "lucide-react";
import { Button, Card, CardContent, Box, Typography, LinearProgress, IconButton, Tooltip } from '@mui/material';
import { API } from '../config';
import DataPreviewPanel from './DataPreviewPanel';

const FileUploadPanel = ({ onUploadSuccess, events, transactions = [], addConsoleLog, selectedEvent, onViewEvent, onGenerateSample }) => {
  const [eventFile, setEventFile] = useState(null);
  const [excelDataFile, setExcelDataFile] = useState(null);
  const [uploadedEventFileName, setUploadedEventFileName] = useState('');
  const [uploadedExcelFileName, setUploadedExcelFileName] = useState('');
  const [lastFailedUploadFile, setLastFailedUploadFile] = useState('');
  const [lastUploadStatus, setLastUploadStatus] = useState(null); // 'success' | 'warning' | 'error'

  // Restore persisted uploaded filenames so they survive hard refresh
  useEffect(() => {
    try {
      const ev = localStorage.getItem('uploadedEventFileName');
      const ex = localStorage.getItem('uploadedExcelFileName');
      const failed = localStorage.getItem('lastEventDataUploadFailedFile');
      const status = localStorage.getItem('lastEventDataUploadStatus');
      const lastUploadFile = localStorage.getItem('lastEventDataUploadFileName');
      if (ev) setUploadedEventFileName(ev);
      if (ex) setUploadedExcelFileName(ex);
      if (failed) setLastFailedUploadFile(failed);
      if (status) setLastUploadStatus(status);
      if (lastUploadFile) {
        // if previous status was success or warning, ensure uploadedExcelFileName reflects last uploaded
        if (!ex) setUploadedExcelFileName(lastUploadFile);
      }
    } catch (e) {
      // ignore storage errors
    }
  }, []);

  // Listen for global clear event to reset displayed/persisted filenames
  useEffect(() => {
    const handler = () => {
      try {
        setUploadedEventFileName('');
        setUploadedExcelFileName('');
        localStorage.removeItem('uploadedEventFileName');
        localStorage.removeItem('uploadedExcelFileName');
      } catch (e) {
        // ignore
      }
    };
    window.addEventListener('dsl-clear-uploaded-files', handler);
    return () => window.removeEventListener('dsl-clear-uploaded-files', handler);
  }, []);
  const [uploading, setUploading] = useState(false);
  const [eventDataSummary, setEventDataSummary] = useState([]);
  const [uploadErrors, setUploadErrors] = useState([]);
  const hasEvents = events && events.length > 0;
  const toast = useToast();

  useEffect(() => {
    const loadSummary = async () => {
      try {
        const resp = await axios.get(`${API}/event-data`);
        setEventDataSummary(resp.data || []);
      } catch (e) {
        // ignore
      }
    };
    loadSummary();

    // load persisted upload errors and listen for updates
    const loadErrors = () => {
      try {
        const raw = localStorage.getItem('lastEventDataUploadErrors');
        const arr = raw ? JSON.parse(raw) : [];
        setUploadErrors(arr || []);
      } catch (e) {
        setUploadErrors([]);
      }
    };
    loadErrors();
    const uploadErrorsHandler = (e) => {
      try {
        const detail = e?.detail || JSON.parse(localStorage.getItem('lastEventDataUploadErrors') || '[]');
        setUploadErrors(detail || []);
      } catch (err) {
        setUploadErrors([]);
      }
    };

    const refreshHandler = () => {
      try { loadSummary(); } catch (e) {}
    };

    const clearViewerHandler = () => {
      try {
        setUploadedEventFileName('');
        setUploadedExcelFileName('');
        setLastFailedUploadFile('');
        setLastUploadStatus(null);
        localStorage.removeItem('uploadedEventFileName');
        localStorage.removeItem('uploadedExcelFileName');
        localStorage.removeItem('lastEventDataUploadFailedFile');
        localStorage.removeItem('lastEventDataUploadStatus');
        localStorage.removeItem('lastEventDataUploadFileName');
        localStorage.removeItem('lastEventDataUploadErrors');
      } catch (e) {}
    };

    const eventDefLoadedHandler = (e) => {
      try {
        const filename = e?.detail?.filename || 'ReferenceData.xlsx';
        setUploadedEventFileName(filename);
        localStorage.setItem('uploadedEventFileName', filename);
      } catch (err) {}
    };

    const eventDataImportedHandler = (e) => {
      try {
        const filename = e?.detail?.filename || 'ActivityData.xlsx';
        setUploadedExcelFileName(filename);
        localStorage.setItem('uploadedExcelFileName', filename);
      } catch (err) {}
    };

    window.addEventListener('dsl-refresh-event-data', refreshHandler);
    window.addEventListener('dsl-clear-event-viewer', clearViewerHandler);
    window.addEventListener('dsl-upload-errors', uploadErrorsHandler);
    window.addEventListener('dsl-event-def-loaded', eventDefLoadedHandler);
    window.addEventListener('dsl-event-data-imported', eventDataImportedHandler);
    return () => {
      window.removeEventListener('dsl-refresh-event-data', refreshHandler);
      window.removeEventListener('dsl-clear-event-viewer', clearViewerHandler);
      window.removeEventListener('dsl-upload-errors', uploadErrorsHandler);
      window.removeEventListener('dsl-event-def-loaded', eventDefLoadedHandler);
      window.removeEventListener('dsl-event-data-imported', eventDataImportedHandler);
    };
  }, [events]);

  const handleUploadEvents = async () => {
    if (!eventFile) {
      toast.error("Please select an event definitions file");
      return;
    }

    // Clear prior upload errors before starting a new upload
    try {
      localStorage.removeItem('lastEventDataUploadErrors');
      try { window.dispatchEvent(new CustomEvent('dsl-upload-errors', { detail: [] })); } catch(e) {}
    } catch(e) {}

    const formData = new FormData();
    formData.append("file", eventFile);

    try {
      setUploading(true);
      addConsoleLog("Uploading event definitions...", "info");
      const response = await axios.post(`${API}/events/upload`, formData);
      toast.success(response.data.message);
      addConsoleLog(`✓ ${response.data.message}`, "success");
      // Persist filename for display after upload completes
      if (eventFile && eventFile.name) {
        setUploadedEventFileName(eventFile.name);
        try { localStorage.setItem('uploadedEventFileName', eventFile.name); } catch (e) {}
      }
      setEventFile(null);
      onUploadSuccess();
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message;
      toast.error("Failed to upload events");
      addConsoleLog(`✗ Error: ${errorMsg}`, "error");
    } finally {
      setUploading(false);
    }
  };

  const handleUploadExcelData = async () => {
    if (!excelDataFile) {
      toast.error("Please select an Excel file");
      return;
    }

    // Clear prior upload errors before starting a new upload
    try {
      localStorage.removeItem('lastEventDataUploadErrors');
      try { window.dispatchEvent(new CustomEvent('dsl-upload-errors', { detail: [] })); } catch(e) {}
    } catch(e) {}

    const formData = new FormData();
    formData.append("file", excelDataFile);

    try {
      setUploading(true);
      addConsoleLog("Uploading Excel event data...", "info");
      const response = await axios.post(`${API}/event-data/upload-excel`, formData);
      
      const { uploaded_events, errors } = response.data;
      
      if (uploaded_events && uploaded_events.length > 0) {
        uploaded_events.forEach(item => {
          addConsoleLog(`✓ ${item.event_name}: ${item.rows_uploaded} rows uploaded`, "success");
        });
        toast.success(`Uploaded data for ${uploaded_events.length} event(s)`);
        // clear last failed marker on success
        try { localStorage.removeItem('lastEventDataUploadFailedFile'); } catch(e) {}
        setLastFailedUploadFile('');
      }
      
      if (errors && errors.length > 0) {
        // classify each message as Warning or Error
        const structured = errors.map(m => {
          const msg = (m && m.message) ? String(m.message) : String(m);
          const isWarning = /no data rows found/i.test(msg);
          const type = isWarning ? 'Warning' : 'Error';
          addConsoleLog(`${isWarning ? '⚠' : '✗'} ${msg}`, isWarning ? "warning" : "error");
          return { ErrorType: type, message: msg };
        });
        // Persist structured upload errors for viewer
        try {
          localStorage.setItem('lastEventDataUploadErrors', JSON.stringify(structured));
          try { window.dispatchEvent(new CustomEvent('dsl-upload-errors', { detail: structured })); } catch(e) {}
        } catch(e) {}

        // overall status: error if any Error, otherwise warning
        const overall = structured.some(s => s.ErrorType === 'Error') ? 'error' : 'warning';
        setLastUploadStatus(overall);
        try { localStorage.setItem('lastEventDataUploadStatus', overall); } catch(e) {}

        if (overall === 'error') {
          // mark this filename as failed so viewer can highlight it
          try {
            if (excelDataFile && excelDataFile.name) {
              localStorage.setItem('lastEventDataUploadFailedFile', excelDataFile.name);
              setLastFailedUploadFile(excelDataFile.name);
            }
          } catch(e) {}
        } else {
          // warning only - clear failed marker so filename displays as green
          try { localStorage.removeItem('lastEventDataUploadFailedFile'); } catch(e) {}
          setLastFailedUploadFile('');
        }

        // also persist last uploaded filename for reference
        try {
          if (excelDataFile && excelDataFile.name) {
            localStorage.setItem('lastEventDataUploadFileName', excelDataFile.name);
          }
        } catch(e) {}
      }
      
      // Persist filename for display after upload completes
      if (excelDataFile && excelDataFile.name) {
        setUploadedExcelFileName(excelDataFile.name);
        try { localStorage.setItem('uploadedExcelFileName', excelDataFile.name); } catch (e) {}
      }
      setExcelDataFile(null);
      onUploadSuccess();
    } catch (error) {
      const detailMsg = error.response?.data?.detail || error.message;
      toast.error("Failed to upload Excel data");
      addConsoleLog(`✗ Error: ${detailMsg}`, "error");
      // mark this filename as failed so viewer can highlight it
      try {
        if (excelDataFile && excelDataFile.name) {
          localStorage.setItem('lastEventDataUploadFailedFile', excelDataFile.name);
          setLastFailedUploadFile(excelDataFile.name);
          try { localStorage.setItem('lastEventDataUploadFileName', excelDataFile.name); } catch(e) {}
        }
        setLastUploadStatus('error');
        try { localStorage.setItem('lastEventDataUploadStatus', 'error'); } catch(e) {}
      } catch(e) {}
      // Persist structured upload error for viewer
      try {
        const structured = [{ ErrorType: 'Error', message: String(detailMsg) }];
        localStorage.setItem('lastEventDataUploadErrors', JSON.stringify(structured));
        try { window.dispatchEvent(new CustomEvent('dsl-upload-errors', { detail: structured })); } catch(e) {}
      } catch(e) {}
    } finally {
      setUploading(false);
    }
  };

  const handleDownloadEvents = async () => {
    try {
      addConsoleLog("Downloading reference data...", "info");
      const response = await axios.get(`${API}/events/download`, { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([response.data], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'reference_data.xlsx');
      document.body.appendChild(link);
      link.click();
      link.remove();
      toast.success("Reference data downloaded!");
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message;
      toast.error("Failed to download reference data");
      addConsoleLog(`✗ Error: ${errorMsg}`, "error");
    }
  };

  return (
    <Box sx={{ p: 3, bgcolor: '#F8F9FA', minHeight: '100%' }} data-testid="file-upload-panel">
      <Box sx={{ mb: 3 }}>
        <Typography variant="h3" sx={{ mb: 0.5 }}>Upload Data Files</Typography>
        <Typography variant="body2" color="text.secondary">Upload reference data (.xlsx) and event data (Excel)</Typography>
      </Box>

      {uploading && (
        <Box sx={{ mb: 3 }}>
          <LinearProgress sx={{ borderRadius: 1 }} />
        </Box>
      )}

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' }, gap: 3 }}>
        {/* Event Definitions */}
        <Card>
          <CardContent sx={{ p: 3 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2.5 }}>
              <Box>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <FileText size={20} color="#5B5FED" />
                  <Typography variant="h5">Reference Data File</Typography>
                  <Tooltip
                    arrow
                    placement="right"
                    componentsProps={{
                      tooltip: {
                        sx: {
                          bgcolor: '#1A1D23',
                          color: '#F8F9FA',
                          maxWidth: 460,
                          p: 2,
                          fontSize: '0.78rem',
                          lineHeight: 1.55,
                          borderRadius: 1.5,
                          boxShadow: 4,
                          '& code': {
                            bgcolor: 'rgba(255,255,255,0.08)',
                            px: 0.5,
                            py: 0.1,
                            borderRadius: 0.5,
                            fontSize: '0.72rem',
                          },
                          '& strong': { color: '#A5B4FC' },
                          '& em': { color: '#F8F9FA', fontStyle: 'normal' },
                        },
                      },
                      arrow: { sx: { color: '#1A1D23' } },
                    }}
                    title={(
                      <Box>
                        <Typography variant="caption" sx={{ display: 'block', fontWeight: 700, mb: 1, color: '#A5B4FC', textTransform: 'uppercase', letterSpacing: '0.06em', fontSize: '0.68rem' }}>
                          Upload Instructions
                        </Typography>
                        <Box component="ul" sx={{ m: 0, pl: 2, '& li': { mb: 0.75 } }}>
                          <li><strong>Reference Data File (Excel):</strong> Two sheets — <em>events</em> (columns: EventName, EventField, DataType, EventType, EventTable) and <em>transactions</em> (column: transactiontype, no spaces e.g. <code>InterestAccrual</code>)</li>
                          <li><strong>Event Table:</strong> <code>standard</code> (always a transaction event) or <code>custom</code> (transaction event or reference table)</li>
                          <li><strong>Event Data (Excel):</strong> Sheet name must match event name</li>
                          <li><strong>Required Columns (transaction events):</strong> PostingDate, EffectiveDate, InstrumentId + event fields</li>
                          <li><strong>Reference table events (custom):</strong> Tenant-level data — no PostingDate, EffectiveDate, or InstrumentId needed</li>
                          <li><strong>Financial Formulas:</strong> 100+ built-in financial calculation formulas are available</li>
                        </Box>
                      </Box>
                    )}
                  >
                    <IconButton size="small" sx={{ p: 0.25, color: '#5B5FED' }} aria-label="Upload instructions" data-testid="reference-data-info">
                      <Info size={15} />
                    </IconButton>
                  </Tooltip>
                </Box>
                <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.6 }}>
                  Upload .xlsx file with events and transaction types
                </Typography>
              </Box>
              <Tooltip title="Download">
                <span>
                  <Button
                    size="small"
                    onClick={handleDownloadEvents}
                    disabled={!hasEvents}
                    sx={{ minWidth: 'auto', p: 1 }}
                    data-testid="download-events-button"
                    aria-label="Download event definitions"
                  >
                    <Download size={16} />
                  </Button>
                </span>
              </Tooltip>
            </Box>
            <Box sx={{ mb: 2 }}>
              <input
                type="file"
                accept=".xlsx"
                onChange={(e) => setEventFile(e.target.files[0])}
                style={{ display: 'none' }}
                id="event-file-input"
                data-testid="event-file-input"
              />
              <label htmlFor="event-file-input">
                <Button
                  component="span"
                  variant="outlined"
                  fullWidth
                  size="small"
                  sx={{ justifyContent: 'flex-start', py: 1.5, textAlign: 'left' }}
                >
                  {eventFile ? eventFile.name : 'Choose .xlsx file...'}
                </Button>
              </label>
              {uploadedEventFileName && (
                <Typography variant="caption" color="success.main" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 1 }}>
                  <CheckCircle size={12} />
                  {uploadedEventFileName === 'event.csv' ? 'ReferenceData.xlsx' : uploadedEventFileName}
                </Typography>
              )}
            </Box>
            <Button 
              onClick={handleUploadEvents} 
              disabled={!eventFile || uploading}
              variant="contained"
              fullWidth
              size="small"
              startIcon={<Upload size={16} />}
              data-testid="upload-events-button"
            >
              Upload Reference File
            </Button>
          </CardContent>
        </Card>

        {/* Event Data - Excel */}
        <Card>
          <CardContent sx={{ p: 3 }}>
            <Box sx={{ mb: 2.5 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1, justifyContent: 'space-between' }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <FileSpreadsheet size={20} color="#4CAF50" />
                  <Typography variant="h5">Event Data (Excel)</Typography>
                </Box>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  {(() => {
                    const hasDefs = Array.isArray(events) && events.length > 0;
                    const hasData = Array.isArray(eventDataSummary)
                      && eventDataSummary.some(it => (it.row_count || 0) > 0);
                    const canGenerate = hasDefs && !hasData;
                    const tooltip = !hasDefs
                      ? 'Load event definitions first'
                      : hasData
                        ? 'Event data is already loaded'
                        : 'Ask the AI agent to generate sample event data';
                    const handleClick = () => {
                      if (!canGenerate || typeof onGenerateSample !== 'function') return;
                      const eventNames = (events || []).map(e => e.event_name).filter(Boolean);
                      const txnNames = (transactions || []).map(t => t.name || t.transaction_name).filter(Boolean);

                      // Detect domain from event names to suggest the right instrument ID prefix
                      const allNames = eventNames.join(' ').toLowerCase();
                      let instPrefix = 'INST';
                      if (/fas91|origination_fee|loan_fee|amort/.test(allNames)) instPrefix = 'LN';
                      else if (/ecl|ifrs9|credit_risk|impairment|provision/.test(allNames)) instPrefix = 'ECL';
                      else if (/lease|ifrs16|asc842|rou/.test(allNames)) instPrefix = 'LEASE';
                      else if (/depreciation|fixed_asset|ias16|property/.test(allNames)) instPrefix = 'FA';
                      else if (/revenue|ifrs15|asc606|contract/.test(allNames)) instPrefix = 'CONT';
                      else if (/bond|security|sbo|fair_value|mtm/.test(allNames)) instPrefix = 'BOND';

                      const inst1 = `${instPrefix}-001`;
                      const inst2 = `${instPrefix}-002`;

                      // 6 monthly posting dates ending at the most recent month-end
                      const today = new Date();
                      const monthEnds = [];
                      for (let m = 5; m >= 0; m--) {
                        const d = new Date(today.getFullYear(), today.getMonth() - m + 1, 0);
                        monthEnds.push(d.toISOString().slice(0, 10));
                      }

                      const msg = [
                        `Generate production-quality, accounting-standards-coherent sample event data for exactly 2 instruments and load it into the system.`,
                        ``,
                        `Event definitions: ${eventNames.length ? eventNames.join(', ') : '(use whatever is loaded)'}.`,
                        txnNames.length ? `Transaction types: ${txnNames.join(', ')}.` : '',
                        ``,
                        `MANDATORY REQUIREMENTS — read carefully:`,
                        `1. Use instrument IDs: "${inst1}" and "${inst2}".`,
                        `2. Use these 6 monthly posting dates: ${monthEnds.join(', ')}.`,
                        `   This shows time-series evolution (amortising balances, accumulating depreciation, etc.).`,
                        `3. Data must be INTERNALLY CONSISTENT per instrument across all 6 dates:`,
                        `   - Loan/FAS91: outstanding_balance must decline each month as principal amortises.`,
                        `     origination_fee must be 0.5–2.5% of loan_amount. eir_rate > note_rate.`,
                        `     origination_date < each posting_date < maturity_date.`,
                        `   - IFRS 9/ECL: ecl = pd × lgd × ead. Stage 1 pd < 2%, Stage 2 pd 2–15%, Stage 3 pd > 20%.`,
                        `     days_past_due matches stage: S1 0–29, S2 30–89, S3 90+.`,
                        `   - IFRS 16 Lease: rou_asset decreases each month. lease_liability decreases via annuity`,
                        `     amortisation. lease_payment is fixed. ibr/discount_rate is an annual rate 3–9%.`,
                        `   - IAS 16 Fixed Assets: accumulated_depreciation increases by annual_charge/12 each month.`,
                        `     nbv = acquisition_cost − accumulated_depreciation.`,
                        `   - IFRS 15 Revenue: recognized_revenue increases monthly, deferred_revenue decreases.`,
                        `   - Securities/SBO: market_value fluctuates realistically around face_value.`,
                        `     accrued_interest resets at coupon payment dates.`,
                        `4. All rates must be in DECIMAL form: 5% = 0.05, NOT 5.`,
                        `5. Amounts must be realistic: loans $50k–$500k, leases $20k–$400k, bonds $1k–$1M.`,
                        `6. Call generate_sample_event_data once per event with all 6 posting dates.`,
                        `7. After loading, confirm with a table showing: instrument, event, key fields, and their`,
                        `   values at the first and last posting date to prove time-series coherence.`,
                      ].filter(Boolean).join('\n');
                      onGenerateSample(msg);
                    };
                    return (
                      <Tooltip title={tooltip}>
                        <span>
                          <Button
                            variant="contained"
                            size="small"
                            disableElevation
                            onClick={handleClick}
                            disabled={!canGenerate}
                            startIcon={<Sparkles size={14} />}
                            data-testid="generate-sample-event-data"
                            sx={{
                              textTransform: 'none',
                              borderRadius: '999px',
                              fontWeight: 600,
                              fontSize: '0.75rem',
                              px: 1.5,
                              py: 0.25,
                              minHeight: 28,
                              bgcolor: '#F3E8FF',
                              color: '#7C3AED',
                              '&:hover': { bgcolor: '#E9D5FF', boxShadow: 'none' },
                              '&.Mui-disabled': { bgcolor: '#F3F4F6', color: '#9CA3AF' },
                            }}
                          >
                            Generate Sample
                          </Button>
                        </span>
                      </Tooltip>
                    );
                  })()}
                  <Tooltip title="View data">
                    <span>
                      <IconButton
                        size="small"
                        onClick={() => { if (typeof onViewEvent === 'function' && selectedEvent) onViewEvent(selectedEvent); }}
                        disabled={!(selectedEvent && (eventDataSummary.some(it => it.event_name === selectedEvent && (it.row_count || 0) > 0) || uploadErrors.length > 0))}
                        data-testid="view-event-data-button"
                        sx={{ color: '#5B5FED' }}
                        aria-label="View event data"
                      >
                        <Eye size={16} />
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>
              </Box>
              <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.6 }}>
                Excel file with event data (each sheet = one event)
              </Typography>
            </Box>
            <Box sx={{ mb: 2 }}>
              <input
                type="file"
                accept=".xlsx,.xls"
                onChange={(e) => setExcelDataFile(e.target.files[0])}
                style={{ display: 'none' }}
                id="excel-data-file-input"
                data-testid="excel-data-file-input"
              />
              <label htmlFor="excel-data-file-input">
                <Button
                  component="span"
                  variant="outlined"
                  fullWidth
                  size="small"
                  sx={{ justifyContent: 'flex-start', py: 1.5, textAlign: 'left' }}
                >
                  {excelDataFile ? excelDataFile.name : 'Choose Excel file...'}
                </Button>
              </label>
            </Box>
            {excelDataFile && (
              <Typography variant="caption" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 1, color: lastFailedUploadFile === excelDataFile.name ? 'error.main' : 'success.main' }}>
                {lastFailedUploadFile === excelDataFile.name ? <X size={12} /> : <CheckCircle size={12} />}
                {excelDataFile.name}
              </Typography>
            )}
            {uploadedExcelFileName && !excelDataFile && (
              <Typography variant="caption" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 1, color: lastFailedUploadFile === uploadedExcelFileName ? 'error.main' : 'success.main' }}>
                {lastFailedUploadFile === uploadedExcelFileName ? <X size={12} /> : <CheckCircle size={12} />}
                {uploadedExcelFileName}
              </Typography>
            )}
            <Button 
              onClick={handleUploadExcelData} 
              disabled={!excelDataFile || uploading}
              variant="contained"
              fullWidth
              size="small"
              startIcon={<Upload size={16} />}
              data-testid="upload-excel-data-button"
              sx={{
                bgcolor: '#4CAF50',
                '&:hover': { bgcolor: '#388E3C' },
              }}
            >
              Upload Excel Data
            </Button>
          </CardContent>
        </Card>
      </Box>

      {/* Live data preview */}
      <DataPreviewPanel events={events} transactions={transactions} />
    </Box>
  );
};

export default FileUploadPanel;