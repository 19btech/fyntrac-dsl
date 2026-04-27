import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import {
  Box, Select, MenuItem, Typography, Tooltip, CircularProgress,
} from "@mui/material";
import { Bot } from "lucide-react";
import { API } from "../config";

const ModelSelector = ({ onModelChange, refreshKey, sx }) => {
  const [status, setStatus] = useState(null); // { configured, provider, selected_model, available_models }
  const [loading, setLoading] = useState(true);
  const [model, setModel] = useState("");

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/ai/provider/status`);
      setStatus(response.data);
      const savedModel = response.data.selected_model || "";
      setModel(savedModel);
      if (onModelChange) onModelChange(savedModel);
    } catch {
      setStatus(null);
    } finally {
      setLoading(false);
    }
  }, [onModelChange]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus, refreshKey]);

  const handleChange = (e) => {
    setModel(e.target.value);
    if (onModelChange) onModelChange(e.target.value);
  };

  if (loading) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", gap: 1, ...sx }}>
        <CircularProgress size={14} />
        <Typography variant="caption" color="text.secondary">Loading...</Typography>
      </Box>
    );
  }

  if (!status?.configured) {
    return (
      <Tooltip title="Configure an AI provider in Settings → AI Agent Setup">
        <Box sx={{ display: "flex", alignItems: "center", gap: 1, opacity: 0.5, ...sx }}>
          <Bot size={14} />
          <Typography variant="caption" color="text.secondary">No AI provider</Typography>
        </Box>
      </Tooltip>
    );
  }

  const models = status.available_models || [];

  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1, ...sx }}>
      <Bot size={14} />
      <Select
        size="small"
        value={model}
        onChange={handleChange}
        variant="standard"
        disableUnderline
        sx={{
          fontSize: "0.75rem",
          "& .MuiSelect-select": { py: 0.25 },
        }}
      >
        {models.map((m) => (
          <MenuItem key={m.id} value={m.id} sx={{ fontSize: "0.75rem" }}>
            {m.name}
          </MenuItem>
        ))}
      </Select>
    </Box>
  );
};

export default ModelSelector;
