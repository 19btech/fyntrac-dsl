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

  const handleClose = (id) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  };

  return (
    <ToastContext.Provider value={toast}>
      {children}
      {toasts.map(({ id, message, severity, open }) => {
        const styles = TOAST_STYLES[severity] || TOAST_STYLES.info;
        return (
          <Snackbar
            key={id}
            open={open}
            autoHideDuration={4500}
            onClose={() => handleClose(id)}
            anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
            sx={{ top: '72px !important' }}
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

// Export a replacement for sonner's toast for compatibility
export const toast = {
  success: (message) => {
    // Will be overridden by context
  },
  error: (message) => {
    console.error('Toast (fallback):', message);
  },
  info: (message) => {
    console.info('Toast (fallback):', message);
  },
  warning: (message) => {
    console.warn('Toast (fallback):', message);
  },
};
