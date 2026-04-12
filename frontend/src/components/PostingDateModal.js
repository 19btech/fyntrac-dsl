import React, { useState } from "react";
import {
  Dialog, DialogTitle, DialogContent, DialogActions,
  Button, Box, Typography, List, ListItemButton, ListItemText,
} from "@mui/material";
import { CalendarDays } from "lucide-react";

/**
 * Modal presented when the user clicks Run on the Console and multiple posting dates
 * are present in the loaded event data.  The user selects exactly one date and confirms.
 */
const PostingDateModal = ({ open, postingDates, onConfirm, onCancel }) => {
  const [selected, setSelected] = useState(null);

  // Reset selection when modal opens
  React.useEffect(() => {
    if (open) setSelected(null);
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

      <DialogContent dividers sx={{ p: 0 }}>
        <List disablePadding>
          {(postingDates || []).map((date) => (
            <ListItemButton
              key={date}
              selected={selected === date}
              onClick={() => setSelected(date)}
              sx={{
                "&.Mui-selected": {
                  bgcolor: "#EEF0FE",
                  "&:hover": { bgcolor: "#E0E3FD" },
                },
              }}
            >
              <ListItemText
                primary={date}
                primaryTypographyProps={{
                  sx: {
                    fontFamily: "monospace",
                    fontWeight: selected === date ? 700 : 400,
                    color: selected === date ? "#5B5FED" : "inherit",
                  },
                }}
              />
            </ListItemButton>
          ))}
        </List>
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
