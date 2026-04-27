import React, { useState } from "react";
import {
  Dialog, DialogTitle, DialogContent, DialogActions,
  Button, Box, Typography, FormControl, InputLabel, Select, MenuItem,
} from "@mui/material";
import { CalendarDays } from "lucide-react";

/**
 * Modal presented when the user clicks Run on the Console and multiple posting dates
 * are present in the loaded event data.  The user selects one date from a dropdown.
 */
const PostingDateModal = ({ open, postingDates, onConfirm, onCancel }) => {
  const [selected, setSelected] = useState('');

  // Reset selection when modal opens
  React.useEffect(() => {
    if (open) setSelected('');
  }, [open]);

  const handleConfirm = () => {
    if (!selected) return;
    onConfirm(selected);
  };

  return (
    <Dialog open={open} onClose={onCancel} maxWidth="xs" fullWidth>
      <DialogTitle>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <CalendarDays size={20} color="#5B5FED" />
          <Typography variant="h6" component="span">
            Select Posting Date
          </Typography>
        </Box>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
          Multiple posting dates found. Choose one to run the console against.
        </Typography>
      </DialogTitle>

      <DialogContent sx={{ pt: 3, pb: 1, overflow: 'visible' }}>
        <FormControl fullWidth size="small">
          <InputLabel id="posting-date-label">Posting Date</InputLabel>
          <Select
            labelId="posting-date-label"
            value={selected}
            label="Posting Date"
            onChange={(e) => setSelected(e.target.value)}
            sx={{ fontFamily: "monospace" }}
          >
            {(postingDates || []).map((date) => (
              <MenuItem key={date} value={date} sx={{ fontFamily: "monospace" }}>
                {date}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </DialogContent>

      <DialogActions sx={{ px: 2, py: 1.5 }}>
        <Button onClick={onCancel} variant="outlined" color="inherit" size="small">
          Cancel
        </Button>
        <Button
          onClick={handleConfirm}
          variant="contained"
          size="small"
          disabled={!selected}
          sx={{ bgcolor: "#14213D", "&:hover": { bgcolor: "#1D3557" } }}
        >
          Run
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default PostingDateModal;

