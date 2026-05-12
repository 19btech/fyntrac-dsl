import React from "react";
import { Box, Typography, IconButton, Chip, Tooltip } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { X } from "lucide-react";

/**
 * Standard Fyntrac modal header — matches the Fyntrac app design system.
 *
 * Props:
 *   badge      – uppercase label for the category chip  (e.g. "TRANSACTION")
 *   badgeColor – hex color for the chip accent  (default #3f51b5)
 *   title      – main heading string or ReactNode
 *   onClose    – close handler (omit to hide the close button)
 */
const ModalHeader = ({ badge, title, onClose, badgeColor = "#3f51b5" }) => (
  <Box
    sx={{
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      px: 3,
      pt: 3,
      pb: 2.5,
      background:
        "linear-gradient(135deg, rgba(30,64,175,0.05) 0%, rgba(99,102,241,0.04) 100%)",
      borderBottom: "1px solid",
      borderColor: "divider",
    }}
  >
    {/* Left: logo + badge + title */}
    <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
      <img
        src={process.env.PUBLIC_URL + "/fyntrac9.png"}
        alt="Fyntrac"
        style={{ width: 72, height: "auto" }}
        onError={(e) => {
          e.currentTarget.onerror = null;
          e.currentTarget.src = process.env.PUBLIC_URL + "/logo.png";
        }}
      />
      <Box>
        {badge && (
          <Chip
            label={badge}
            size="small"
            sx={{
              height: 18,
              fontSize: "0.6rem",
              fontWeight: 700,
              letterSpacing: 0.8,
              textTransform: "uppercase",
              bgcolor: alpha(badgeColor, 0.1),
              color: badgeColor,
              mb: 0.5,
              borderRadius: 1,
            }}
          />
        )}
        <Typography
          variant="h6"
          fontWeight={700}
          sx={{ lineHeight: 1.2, color: "text.primary" }}
        >
          {title}
        </Typography>
      </Box>
    </Box>

    {/* Right: close button */}
    {onClose && (
      <Tooltip title="Close" placement="left">
        <IconButton
          onClick={onClose}
          size="small"
          sx={{
            color: "text.secondary",
            bgcolor: "action.hover",
            borderRadius: 2,
            "&:hover": { bgcolor: "error.50", color: "error.main" },
          }}
        >
          <X size={18} />
        </IconButton>
      </Tooltip>
    )}
  </Box>
);

export default ModalHeader;
