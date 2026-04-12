import React, { useState, useEffect } from "react";
import axios from "axios";
import { useToast } from "./ToastProvider";
import { Upload, FileText, FileSpreadsheet, Download, CheckCircle, Eye, X } from "lucide-react";
import { Button, Card, CardContent, Box, Typography, LinearProgress, IconButton, Tooltip } from '@mui/material';

const API = '/api';

const FileUploadPanel = ({ onUploadSuccess, events, addConsoleLog, selectedEvent, onViewEvent }) => {
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

    window.addEventListener('dsl-refresh-event-data', refreshHandler);
    window.addEventListener('dsl-clear-event-viewer', clearViewerHandler);
    window.addEventListener('dsl-upload-errors', uploadErrorsHandler);
    return () => {
      window.removeEventListener('dsl-refresh-event-data', refreshHandler);
      window.removeEventListener('dsl-clear-event-viewer', clearViewerHandler);
      window.removeEventListener('dsl-upload-errors', uploadErrorsHandler);
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
      addConsoleLog("Downloading event definitions...", "info");
      const response = await axios.get(`${API}/events/download`, { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'event_definitions.csv');
      document.body.appendChild(link);
      link.click();
      link.remove();
      toast.success("Event definitions downloaded!");
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message;
      toast.error("Failed to download events");
      addConsoleLog(`✗ Error: ${errorMsg}`, "error");
    }
  };

  return (
    <Box sx={{ p: 3, bgcolor: '#F8F9FA', minHeight: '100%' }} data-testid="file-upload-panel">
      <Box sx={{ mb: 3 }}>
        <Typography variant="h3" sx={{ mb: 0.5 }}>Upload Data Files</Typography>
        <Typography variant="body2" color="text.secondary">Upload event definitions (CSV) and event data (Excel)</Typography>
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
                  <Typography variant="h5">Event Definitions</Typography>
                </Box>
                <Typography variant="body2" color="text.secondary" sx={{ lineHeight: 1.6 }}>
                  Upload CSV file with event schemas
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
                accept=".csv"
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
                  {eventFile ? eventFile.name : 'Choose CSV file...'}
                </Button>
              </label>
              {uploadedEventFileName && (
                <Typography variant="caption" color="success.main" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 1 }}>
                  <CheckCircle size={12} />
                  {uploadedEventFileName}
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
              Upload Events
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

      {/* Instructions */}
      <Card sx={{ mt: 3, bgcolor: '#EEF0FE', border: '1px solid #D4D6FA' }}>
        <CardContent sx={{ p: 3 }}>
          <Typography variant="h6" sx={{ mb: 1.5, color: '#5B5FED' }}>Upload Instructions</Typography>
            <Box component="ul" sx={{ m: 0, pl: 2.5, '& li': { mb: 1, fontSize: '0.8125rem', color: '#495057', lineHeight: 1.6 } }}>
            <li><strong>Event Definitions (CSV):</strong> Columns: EventName, EventField, DataType, EventType, EventTable</li>
            <li><strong>EventTable:</strong> <code>standard</code> (always activity) or <code>custom</code> (activity or reference)</li>
            <li><strong>Event Data (Excel):</strong> Sheet name must match event name</li>
            <li><strong>Required Columns (activity):</strong> PostingDate, EffectiveDate, InstrumentId + event fields</li>
            <li><strong>Reference events (custom):</strong> Tenant-level data — no PostingDate, EffectiveDate, or InstrumentId needed</li>
            <li><strong>DSL Functions:</strong> 100+ pre-loaded financial functions available</li>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
};

export default FileUploadPanel;