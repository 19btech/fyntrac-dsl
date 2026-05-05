import React, { useState, useRef } from "react";
import axios from "axios";
import {
  Box, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  Typography, CircularProgress, Alert, Divider,
} from "@mui/material";
import { Upload, FileJson, CheckCircle2 } from "lucide-react";
import { API } from "../config";

const SLOTS = [
  {
    key: "transactions",
    label: "Transactions",
    accept: ".json",
    Icon: FileJson,
    endpoint: "/import/transactions",
    hint: "JSON array of transaction-type objects.",
    summary: (r) => `${r.count} transaction type(s) loaded.`,
  },
  {
    key: "event_configurations",
    label: "Event Configurations",
    accept: ".json",
    Icon: FileJson,
    endpoint: "/import/event-configurations",
    hint: "JSON array of EventConfiguration objects → Event Definitions.",
    summary: (r) => `${r.count} event definition(s) loaded: ${(r.names || []).join(", ")}`,
  },
];

const ImportEventsModal = ({ open, onClose, onSuccess }) => {
  const [files, setFiles] = useState({});
  const [results, setResults] = useState({});
  const [errors, setErrors] = useState({});
  const [busy, setBusy] = useState({});
  const inputRefs = useRef({});

  const reset = () => {
    setFiles({});
    setResults({});
    setErrors({});
    setBusy({});
    Object.values(inputRefs.current).forEach((el) => { if (el) el.value = ""; });
  };

  const handleClose = () => {
    if (Object.values(busy).some(Boolean)) return;
    reset();
    onClose();
  };

  const handlePick = (key, file) => {
    setFiles((s) => ({ ...s, [key]: file || null }));
    setErrors((s) => ({ ...s, [key]: null }));
    setResults((s) => ({ ...s, [key]: null }));
  };

  const upload = async (slot) => {
    const file = files[slot.key];
    if (!file) {
      setErrors((s) => ({ ...s, [slot.key]: "Pick a file first." }));
      return;
    }
    const accepted = slot.accept.split(",").map((s) => s.trim().toLowerCase());
    const ok = accepted.some((ext) => file.name.toLowerCase().endsWith(ext));
    if (!ok) {
      setErrors((s) => ({ ...s, [slot.key]: `Only ${slot.accept} files are accepted.` }));
      return;
    }
    setBusy((s) => ({ ...s, [slot.key]: true }));
    setErrors((s) => ({ ...s, [slot.key]: null }));
    try {
      const fd = new FormData();
      fd.append("file", file);
      const { data } = await axios.post(`${API}${slot.endpoint}`, fd, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setResults((s) => ({ ...s, [slot.key]: data }));
      onSuccess && onSuccess({ slot: slot.key, data });
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || "Upload failed.";
      setErrors((s) => ({
        ...s,
        [slot.key]: typeof detail === "string" ? detail : JSON.stringify(detail),
      }));
    } finally {
      setBusy((s) => ({ ...s, [slot.key]: false }));
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth
            PaperProps={{ sx: { borderRadius: 2 } }}>
      <DialogTitle sx={{ fontWeight: 700, fontSize: "1rem", pb: 1 }}>
        Import
      </DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Upload each file independently. Each upload replaces only its
          corresponding data (transactions / event definitions / event data).
        </Typography>
        {SLOTS.map((slot, idx) => {
          const file = files[slot.key];
          const result = results[slot.key];
          const error = errors[slot.key];
          const isBusy = !!busy[slot.key];
          const Icon = slot.Icon;
          return (
            <Box key={slot.key}>
              {idx > 0 && <Divider sx={{ my: 2 }} />}
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 0.75 }}>
                <Icon size={16} color="#5B5FED" />
                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                  {slot.label}
                </Typography>
                {result && <CheckCircle2 size={14} color="#2E7D32" />}
              </Box>
              <Typography variant="caption" color="text.secondary"
                          sx={{ display: "block", mb: 1 }}>
                {slot.hint}
              </Typography>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Box
                  sx={{
                    flex: 1,
                    border: "1px dashed",
                    borderColor: file ? "#5B5FED" : "#D4D6FA",
                    borderRadius: 1,
                    px: 1.5, py: 1,
                    cursor: isBusy ? "default" : "pointer",
                    bgcolor: file ? "#EEF0FE" : "#FAFAFA",
                    fontSize: "0.8125rem",
                    color: file ? "#5B5FED" : "#6C757D",
                    overflow: "hidden",
                    whiteSpace: "nowrap",
                    textOverflow: "ellipsis",
                  }}
                  onClick={() => !isBusy && inputRefs.current[slot.key]?.click()}
                >
                  {file ? file.name : `Choose a ${slot.accept} file…`}
                </Box>
                <input
                  ref={(el) => { inputRefs.current[slot.key] = el; }}
                  type="file"
                  accept={slot.accept}
                  style={{ display: "none" }}
                  onChange={(e) => handlePick(slot.key, e.target.files?.[0] || null)}
                />
                <Button
                  onClick={() => upload(slot)}
                  disabled={isBusy || !file}
                  variant="contained"
                  size="small"
                  startIcon={isBusy
                    ? <CircularProgress size={12} color="inherit" />
                    : <Upload size={12} />}
                  sx={{
                    bgcolor: "#14213d",
                    "&:hover": { bgcolor: "#1D3557" },
                    "&:disabled": { bgcolor: "#ADB5BD" },
                  }}
                >
                  {isBusy ? "Uploading…" : "Upload"}
                </Button>
              </Box>
              {result && (
                <Alert severity="success" sx={{ mt: 1, fontSize: "0.75rem", py: 0.5 }}>
                  {slot.summary(result)}
                </Alert>
              )}
              {error && (
                <Alert severity="error" sx={{ mt: 1, fontSize: "0.75rem", py: 0.5 }}>
                  {error}
                </Alert>
              )}
            </Box>
          );
        })}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2.5, gap: 1 }}>
        <Button
          onClick={handleClose}
          disabled={Object.values(busy).some(Boolean)}
          variant="outlined"
          size="small"
          sx={{ borderColor: "#CED4DA", color: "#495057",
                "&:hover": { borderColor: "#ADB5BD" } }}
        >
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ImportEventsModal;
