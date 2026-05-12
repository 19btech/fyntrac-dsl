import React, { useState, useRef } from "react";
import axios from "axios";
import {
  Box, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  Typography, CircularProgress, Alert, Divider, Slide,
} from "@mui/material";
import { Upload, FileJson, CheckCircle2, CloudDownload } from "lucide-react";
import { API } from "../config";
import ModalHeader from "./ModalHeader";
import { useToast } from "./ToastProvider";

const SLOTS = [
  {
    key: "transactions",
    label: "Transactions",
    accept: ".json",
    Icon: FileJson,
    endpoint: "/import/transactions",
    hint: "JSON array of transaction-type objects.",
    summary: (r) => `${r.count} transaction name(s) loaded.`,
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
  const toast = useToast();
  const [files, setFiles] = useState({});
  const [results, setResults] = useState({});
  const [errors, setErrors] = useState({});
  const [busy, setBusy] = useState({});
  const [isPulling, setIsPulling] = useState(false);
  const inputRefs = useRef({});

  const reset = () => {
    setFiles({});
    setResults({});
    setErrors({});
    setBusy({});
    setIsPulling(false);
    Object.values(inputRefs.current).forEach((el) => { if (el) el.value = ""; });
  };

  const handleClose = () => {
    if (Object.values(busy).some(Boolean) || isPulling) return;
    reset();
    onClose();
  };

  const handlePick = (key, file) => {
    setFiles((s) => ({ ...s, [key]: file || null }));
    setErrors((s) => ({ ...s, [key]: null }));
    setResults((s) => ({ ...s, [key]: null }));
  };

  const handlePullFromDataloader = async () => {
    setIsPulling(true);
    let txCount = 0;
    let evCount = 0;

    // ── Step 1: Pull Transaction Definitions ──────────────────────────────
    try {
      const txRes = await axios.get('/api/dataloader/transaction/get/all');
      const txData = txRes.data;
      console.log('[Import] Transactions fetched from dataloader:', txData?.length, 'items');

      if (Array.isArray(txData) && txData.length > 0) {
        const txBlob = new Blob([JSON.stringify(txData)], { type: 'application/json' });
        const txFile = new File([txBlob], 'transactions.json', { type: 'application/json' });
        const txFd = new FormData();
        txFd.append('file', txFile);
        const txUploadRes = await axios.post(`${API}/import/transactions`, txFd);
        txCount = txUploadRes.data?.count || 0;
        console.log('[Import] Transactions imported into DSL:', txCount);
      } else {
        console.warn('[Import] No transactions returned from dataloader');
      }
    } catch (err) {
      console.error('[Import] Transaction pull failed:', err?.response?.data || err.message);
      toast.error('Failed to pull transactions: ' + (err?.response?.data?.detail || err?.message || 'Unknown error'));
    }

    // ── Step 2: Pull Event Configurations ─────────────────────────────────
    try {
      const evRes = await axios.get('/api/dataloader/fyntrac/event-configurations/all');
      const evData = evRes.data;
      console.log('[Import] Event configs fetched from dataloader:', evData?.length, 'items');

      if (Array.isArray(evData) && evData.length > 0) {
        const evBlob = new Blob([JSON.stringify(evData)], { type: 'application/json' });
        const evFile = new File([evBlob], 'event-configurations.json', { type: 'application/json' });
        const evFd = new FormData();
        evFd.append('file', evFile);
        const evUploadRes = await axios.post(`${API}/import/event-configurations`, evFd);
        evCount = evUploadRes.data?.count || 0;
        console.log('[Import] Event configs imported into DSL:', evCount);
      } else {
        console.warn('[Import] No event configurations returned from dataloader');
      }
    } catch (err) {
      console.error('[Import] Event config pull failed:', err?.response?.data || err.message);
      toast.error('Failed to pull event configs: ' + (err?.response?.data?.detail || err?.message || 'Unknown error'));
    }

    // ── Summary ───────────────────────────────────────────────────────────
    if (txCount > 0 || evCount > 0) {
      toast.success('Data loaded successfully from Dataloader');
      localStorage.setItem('uploadedEventFileName', 'EventConfigurations.json');
      window.dispatchEvent(new CustomEvent('dsl-event-def-loaded', { detail: { filename: 'EventConfigurations.json' } }));
      window.dispatchEvent(new CustomEvent('dsl-transaction-defs-changed'));
      if (onSuccess) onSuccess({ slot: 'combined', data: { count: evCount + txCount } });
      handleClose();
    } else {
      toast.info('Data is not available on backend service');
    }

    setIsPulling(false);
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
      toast.success(slot.summary(data));
      onSuccess && onSuccess({ slot: slot.key, data });
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || "Upload failed.";
      setErrors((s) => ({
        ...s,
        [slot.key]: typeof detail === "string" ? detail : JSON.stringify(detail),
      }));
      toast.error(typeof detail === "string" ? detail : JSON.stringify(detail));
    } finally {
      setBusy((s) => ({ ...s, [slot.key]: false }));
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth
            TransitionComponent={Slide}
            TransitionProps={{ direction: 'up' }}
            PaperProps={{ sx: { borderRadius: 4, boxShadow: '0 32px 64px rgba(0,0,0,0.14)', overflow: 'hidden', border: '1px solid', borderColor: 'divider' } }}>
      <DialogTitle sx={{ p: 0 }}>
        <ModalHeader badge="DATA IMPORT" title="Import" onClose={handleClose} />
      </DialogTitle>
      <DialogContent sx={{ pt: 1 }}>
        <Box sx={{
          mb: 2.5,
          p: 2,
          borderRadius: 2,
          background: "linear-gradient(135deg, #F5F7FF 0%, #E8EDFF 100%)",
          border: "1px solid #D3DCFF",
          display: "flex",
          flexDirection: { xs: "column", sm: "row" },
          alignItems: "center",
          justifyContent: "space-between",
          gap: 2,
        }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="body2" sx={{ fontWeight: 700, color: "#1A237E" }}>
              Sync with Dataloader
            </Typography>
            <Typography variant="caption" sx={{ display: "block", color: "#3949AB", mt: 0.5 }}>
              Automatically pull transaction names and event configurations directly from the connected Fyntrac Gateway service without files.
            </Typography>
          </Box>
          <Button
            onClick={handlePullFromDataloader}
            disabled={isPulling || Object.values(busy).some(Boolean)}
            variant="contained"
            size="small"
            startIcon={isPulling ? <CircularProgress size={14} color="inherit" /> : <CloudDownload size={15} />}
            sx={{
              bgcolor: "#3F51B5",
              whiteSpace: "nowrap",
              "&:hover": { bgcolor: "#303F9F" },
              "&:disabled": { bgcolor: "#C5CAE9", color: "#7986CB" },
              boxShadow: "0 2px 8px rgba(63, 81, 181, 0.25)",
              fontWeight: 600,
              textTransform: "none",
            }}
          >
            {isPulling ? "Syncing..." : "Pull Direct"}
          </Button>
        </Box>

        <Divider sx={{ my: 2, '&::before, &::after': { borderTopStyle: 'dashed' } }}>
          <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600, px: 1 }}>
            OR UPLOAD FILES
          </Typography>
        </Divider>

        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Upload each file independently. Each upload replaces only its
          corresponding data (transactions / event definitions).
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
