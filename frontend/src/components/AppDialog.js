import React from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Button,
  TextField,
  IconButton,
  Box,
} from "@mui/material";
import { X } from "lucide-react";

/**
 * Shared reusable dialog component.
 *
 * Props:
 *  open           – boolean
 *  onClose        – () => void
 *  title          – string
 *  message        – string  (plain text body)
 *  children       – ReactNode (use instead of message for custom content)
 *  actions        – [{ label, onClick, variant?, color?, disabled?, autoFocus? }]
 *  disableBackdropClick – boolean (default false)
 *  maxWidth       – Dialog maxWidth prop (default "xs")
 */
const AppDialog = ({
  open,
  onClose,
  title,
  message,
  children,
  actions = [],
  disableBackdropClick = false,
  maxWidth = "xs",
}) => {
  const handleClose = (event, reason) => {
    if (disableBackdropClick && reason === "backdropClick") return;
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth={maxWidth}
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 2,
          boxShadow: "0 8px 32px rgba(0,0,0,0.12)",
        },
      }}
    >
      <DialogTitle
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          pb: 1,
          fontSize: "1rem",
          fontWeight: 600,
        }}
      >
        {title}
        <IconButton size="small" onClick={onClose} sx={{ color: "#6C757D" }}>
          <X size={18} />
        </IconButton>
      </DialogTitle>

      <DialogContent sx={{ pt: 0.5 }}>
        {message && (
          <DialogContentText sx={{ fontSize: "0.875rem", color: "#495057" }}>
            {message}
          </DialogContentText>
        )}
        {children}
      </DialogContent>

      {actions.length > 0 && (
        <DialogActions sx={{ px: 3, pb: 2 }}>
          {actions.map(
            (
              {
                label,
                onClick,
                variant = "text",
                color = "primary",
                disabled = false,
                autoFocus = false,
              },
              idx
            ) => (
              <Button
                key={idx}
                onClick={onClick}
                variant={variant}
                color={color}
                disabled={disabled}
                autoFocus={autoFocus}
                size="small"
                sx={{ textTransform: "none", fontWeight: 500 }}
              >
                {label}
              </Button>
            )
          )}
        </DialogActions>
      )}
    </Dialog>
  );
};

/**
 * Hook that returns helpers for confirm / prompt dialogs so components
 * can open them imperatively while still rendering MUI Dialogs.
 *
 * Usage:
 *   const { confirmProps, openConfirm, promptProps, openPrompt } = useAppDialog();
 *   ...
 *   openConfirm({ title, message, confirmLabel, confirmColor, onConfirm })
 *   openPrompt({ title, message, label, defaultValue, onSubmit })
 *   ...
 *   <AppDialog {...confirmProps} />
 *   <AppDialog {...promptProps} />
 */
export const useAppDialog = () => {
  // ----- confirm dialog state -----
  const [confirmState, setConfirmState] = React.useState({
    open: false,
    title: "",
    message: "",
    confirmLabel: "Confirm",
    confirmColor: "primary",
    confirmVariant: "contained",
    onConfirm: null,
  });

  const openConfirm = ({
    title,
    message,
    confirmLabel = "Confirm",
    confirmColor = "primary",
    confirmVariant = "contained",
    onConfirm,
  }) => {
    setConfirmState({
      open: true,
      title,
      message,
      confirmLabel,
      confirmColor,
      confirmVariant,
      onConfirm,
    });
  };

  const closeConfirm = () =>
    setConfirmState((s) => ({ ...s, open: false }));

  const confirmProps = {
    open: confirmState.open,
    onClose: closeConfirm,
    title: confirmState.title,
    message: confirmState.message,
    actions: [
      { label: "Cancel", onClick: closeConfirm, variant: "text" },
      {
        label: confirmState.confirmLabel,
        onClick: () => {
          closeConfirm();
          confirmState.onConfirm?.();
        },
        variant: confirmState.confirmVariant,
        color: confirmState.confirmColor,
        autoFocus: true,
      },
    ],
  };

  // ----- prompt dialog state -----
  const [promptState, setPromptState] = React.useState({
    open: false,
    title: "",
    message: "",
    label: "",
    defaultValue: "",
    value: "",
    onSubmit: null,
  });

  const openPrompt = ({
    title,
    message = "",
    label = "",
    defaultValue = "",
    onSubmit,
  }) => {
    setPromptState({
      open: true,
      title,
      message,
      label,
      defaultValue,
      value: defaultValue,
      onSubmit,
    });
  };

  const closePrompt = () =>
    setPromptState((s) => ({ ...s, open: false }));

  const promptProps = {
    open: promptState.open,
    onClose: closePrompt,
    title: promptState.title,
    message: promptState.message || undefined,
    children: (
      <TextField
        autoFocus
        fullWidth
        size="small"
        label={promptState.label}
        value={promptState.value}
        onChange={(e) =>
          setPromptState((s) => ({ ...s, value: e.target.value }))
        }
        onKeyDown={(e) => {
          if (e.key === "Enter" && promptState.value.trim()) {
            closePrompt();
            promptState.onSubmit?.(promptState.value.trim());
          }
        }}
        sx={{ mt: 1 }}
      />
    ),
    actions: [
      { label: "Cancel", onClick: closePrompt, variant: "text" },
      {
        label: "OK",
        onClick: () => {
          const v = promptState.value.trim();
          if (!v) return;
          closePrompt();
          promptState.onSubmit?.(v);
        },
        variant: "contained",
        autoFocus: false,
      },
    ],
  };

  return { confirmProps, openConfirm, promptProps, openPrompt };
};

export default AppDialog;
