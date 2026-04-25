import React, { useState, useRef } from "react";
import axios from "axios";
import {
  Box, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  Typography, CircularProgress, Alert,
} from "@mui/material";
import { Upload, FileJson, CheckCircle2 } from "lucide-react";
import { API } from "../config";

const ImportEventsModal = ({ open, onClose, onSuccess }) => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);       // full failure message (string)
  const [warnings, setWarnings] = useState([]);   // partial failure messages
  const [result, setResult] = useState(null);     // successful transform result
  const fileInputRef = useRef(null);

  const reset = () => {
    setSelectedFile(null);
    setError(null);
    setWarnings([]);
    setResult(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const handleFileChange = (e) => {
    const file = e.target.files[0] || null;
    setSelectedFile(file);
    setError(null);
    setWarnings([]);
    setResult(null);
  };

  const handleUpload = async () => {
    if (!selectedFile) {
      setError("Please select a JSON file before uploading.");
      return;
    }
    if (!selectedFile.name.toLowerCase().endsWith(".json")) {
      setError("Only .json files are accepted.");
      return;
    }

    setUploading(true);
    setError(null);
    setWarnings([]);
    setResult(null);

    try {
      const formData = new FormData();
      formData.append("file", selectedFile);
      const { data } = await axios.post(`${API}/import-events/transform`, formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      setUploading(false);

      // Collect any partial failure warnings
      const warns = [];
      if (data.event_definitions?.success === false) {
        warns.push(`Event definitions could not be created: ${data.event_definitions.error}`);
      }
      if (data.event_data?.success === false) {
        warns.push(`Event data could not be loaded: ${data.event_data.error}`);
      }
      setWarnings(warns);
      setResult(data);

      // Persist instrument selection so EventDataViewer can show the warning banner
      if (data.selected_instruments && data.selected_instruments.length > 0) {
        localStorage.setItem('importSelectedInstruments', JSON.stringify(data.selected_instruments));
      } else {
        localStorage.removeItem('importSelectedInstruments');
      }

      // If everything succeeded, close and notify parent immediately
      if (warns.length === 0) {
        reset();
        onSuccess(data);
      }
      // If there are partial warnings, stay open so the user can read them,
      // but still notify the parent to refresh what did succeed.
      else {
        onSuccess(data);
      }
    } catch (err) {
      setUploading(false);
      const detail =
        err?.response?.data?.detail ||
        err?.message ||
        "An unexpected error occurred. Please check the file and try again.";
      setError(typeof detail === "string" ? detail : JSON.stringify(detail));
    }
  };

  const handleCancel = () => {
    if (uploading) return;
    reset();
    onClose();
  };

  const allSuccess = result && warnings.length === 0;

  return (
    <Dialog
      open={open}
      onClose={uploading ? undefined : handleCancel}
      maxWidth="xs"
      fullWidth
      PaperProps={{ sx: { borderRadius: 2 } }}
    >
      <DialogTitle sx={{ fontWeight: 700, fontSize: "1rem", pb: 1 }}>
        Import Events
      </DialogTitle>

      <DialogContent sx={{ pt: 1 }}>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Select a <strong>.json</strong> file that contains an array of event objects to import.
          The system will load both event definitions and event data automatically.
        </Typography>

        {/* File picker — hide once we have a result to show */}
        {!result && (
          <Box
            sx={{
              border: "2px dashed",
              borderColor: selectedFile ? "#5B5FED" : "#D4D6FA",
              borderRadius: 2,
              p: 2.5,
              textAlign: "center",
              cursor: "pointer",
              bgcolor: selectedFile ? "#EEF0FE" : "#FAFAFA",
              transition: "all 0.15s ease",
              "&:hover": { borderColor: "#5B5FED", bgcolor: "#EEF0FE" },
            }}
            onClick={() => !uploading && fileInputRef.current && fileInputRef.current.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".json"
              style={{ display: "none" }}
              onChange={handleFileChange}
            />
            {selectedFile ? (
              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 1 }}>
                <FileJson size={20} color="#5B5FED" />
                <Typography variant="body2" sx={{ fontWeight: 600, color: "#5B5FED" }}>
                  {selectedFile.name}
                </Typography>
              </Box>
            ) : (
              <Box>
                <Upload size={24} color="#9DA3AE" style={{ marginBottom: 4 }} />
                <Typography variant="body2" color="text.secondary">
                  Click to select a file
                </Typography>
              </Box>
            )}
          </Box>
        )}

        {/* Success summary with partial warnings */}
        {result && warnings.length > 0 && (
          <Box>
            <Alert severity="warning" sx={{ mb: 1.5, fontSize: "0.8125rem" }}>
              Import completed with issues:
            </Alert>
            {warnings.map((w, i) => (
              <Alert key={i} severity="error" sx={{ mb: 1, fontSize: "0.8125rem" }}>{w}</Alert>
            ))}
            {result.event_definitions?.success && (
              <Alert severity="success" sx={{ mb: 1, fontSize: "0.8125rem" }}>
                Event definitions loaded: {result.event_definitions.names?.join(", ")}
              </Alert>
            )}
            {result.event_data?.success && (
              <Alert severity="success" sx={{ mb: 1, fontSize: "0.8125rem" }}>
                Event data loaded: {result.event_data.total_rows} rows across {Object.keys(result.event_data.by_event || {}).length} event type(s)
              </Alert>
            )}
          </Box>
        )}

        {/* Full error */}
        {error && (
          <Alert severity="error" sx={{ mt: 2, fontSize: "0.8125rem" }}>
            {error}
          </Alert>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2.5, gap: 1 }}>
        <Button
          onClick={handleCancel}
          disabled={uploading}
          variant="outlined"
          size="small"
          sx={{ borderColor: "#CED4DA", color: "#495057", "&:hover": { borderColor: "#ADB5BD" } }}
        >
          {result ? "Close" : "Cancel"}
        </Button>
        {!result && (
          <Button
            onClick={handleUpload}
            disabled={uploading || !selectedFile}
            variant="contained"
            size="small"
            startIcon={uploading ? <CircularProgress size={14} color="inherit" /> : <Upload size={14} />}
            sx={{
              bgcolor: "#14213d",
              "&:hover": { bgcolor: "#1D3557" },
              "&:disabled": { bgcolor: "#ADB5BD" },
            }}
          >
            {uploading ? "Importing…" : "Upload"}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default ImportEventsModal;
