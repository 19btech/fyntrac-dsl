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
      {toasts.map(({ id, message, severity, open }) => (
        <Snackbar
          key={id}
          open={open}
          autoHideDuration={4000}
          onClose={() => handleClose(id)}
          anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
        >
          <Alert
            onClose={() => handleClose(id)}
            severity={severity}
            variant="filled"
            sx={{ width: '100%' }}
          >
            {message}
          </Alert>
        </Snackbar>
      ))}
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
