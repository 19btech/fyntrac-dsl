import React, { createContext, useContext, useState, useCallback } from 'react';
import { Snackbar, Alert } from '@mui/material';

const ToastContext = createContext(null);

export const useToast = () => {
  const context = useContext(ToastContext);
  if (!context) {
    throw new Error('useToast must be used within ToastProvider');
  }
  return context;
};

const TOAST_STYLES = {
  success: { bgcolor: '#DCFCE7', color: '#166534', iconColor: '#16A34A', borderColor: '#16A34A' },
  error:   { bgcolor: '#FEE2E2', color: '#991B1B', iconColor: '#DC2626', borderColor: '#DC2626' },
  warning: { bgcolor: '#FEF9C3', color: '#854D0E', iconColor: '#D97706', borderColor: '#D97706' },
  info:    { bgcolor: '#DBEAFE', color: '#1E40AF', iconColor: '#3B82F6', borderColor: '#3B82F6' },
};

export const ToastProvider = ({ children }) => {
  const [toasts, setToasts] = useState([]);

  const showToast = useCallback((message, severity = 'info') => {
    const id = Date.now() + Math.random();
    setToasts(prev => [...prev, { id, message, severity, open: true }]);
  }, []);

  const toast = {
    success: (message) => showToast(message, 'success'),
    error: (message) => showToast(message, 'error'),
    info: (message) => showToast(message, 'info'),
    warning: (message) => showToast(message, 'warning'),
  };

  // Start the close (exit) transition; the toast is removed only after it
  // finishes animating out (see TransitionProps.onExited below).
  const handleClose = (id, reason) => {
    if (reason === 'clickaway') return;  // don't dismiss on outside click
    setToasts(prev => prev.map(t => (t.id === id ? { ...t, open: false } : t)));
  };

  const handleExited = (id) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  };

  return (
    <ToastContext.Provider value={toast}>
      {children}
      {toasts.map(({ id, message, severity, open }, index) => {
        const styles = TOAST_STYLES[severity] || TOAST_STYLES.info;
        return (
          <Snackbar
            key={id}
            open={open}
            autoHideDuration={4500}
            onClose={(e, reason) => handleClose(id, reason)}
            TransitionProps={{ onExited: () => handleExited(id) }}
            anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
            // Stack multiple toasts vertically so they never overlap.
            sx={{ top: `${72 + index * 64}px !important` }}
          >
            <Alert
              onClose={() => handleClose(id)}
              severity={severity}
              sx={{
                width: '100%',
                minWidth: 280,
                bgcolor: styles.bgcolor,
                color: styles.color,
                borderLeft: `4px solid ${styles.borderColor}`,
                borderRadius: 2,
                boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
                '& .MuiAlert-icon': { color: styles.iconColor },
                '& .MuiAlert-action': { color: styles.color },
                '& .MuiAlert-message': { fontWeight: 500 },
              }}
            >
              {message}
            </Alert>
          </Snackbar>
        );
      })}
    </ToastContext.Provider>
  );
};
