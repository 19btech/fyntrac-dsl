import { createTheme } from '@mui/material/styles';

// Fyntrac Design System Theme
// Colors and styles matching the primary application
const fyntracTheme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#5B5FED', // Fyntrac primary purple/indigo
      light: '#7E82F1',
      dark: '#4346C8',
      contrastText: '#FFFFFF',
    },
    secondary: {
      main: '#6C757D', // Gray for secondary actions
      light: '#ADB5BD',
      dark: '#495057',
      contrastText: '#FFFFFF',
    },
    success: {
      main: '#4CAF50', // Green for success states
      light: '#66BB6A',
      dark: '#388E3C',
      contrastText: '#FFFFFF',
    },
    error: {
      main: '#F44336',
      light: '#E57373',
      dark: '#D32F2F',
    },
    warning: {
      main: '#FFC107',
      light: '#FFD54F',
      dark: '#FFA000',
    },
    info: {
      main: '#2196F3',
      light: '#64B5F6',
      dark: '#1976D2',
    },
    background: {
      default: '#F8F9FA', // Light gray background
      paper: '#FFFFFF',
    },
    text: {
      primary: '#212529', // Dark text
      secondary: '#6C757D', // Medium gray text
      disabled: '#ADB5BD',
    },
    divider: '#CED4DA', // Light gray dividers
    grey: {
      50: '#F8F9FA',
      100: '#F1F3F5',
      200: '#E9ECEF',
      300: '#DEE2E6',
      400: '#CED4DA',
      500: '#ADB5BD',
      600: '#6C757D',
      700: '#495057',
      800: '#343A40',
      900: '#212529',
    },
  },
  typography: {
    fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif",
    h1: {
      fontSize: '2rem', // 32px
      fontWeight: 700,
      color: '#212529',
      letterSpacing: '-0.01em',
    },
    h2: {
      fontSize: '1.5rem', // 24px
      fontWeight: 600,
      color: '#212529',
    },
    h3: {
      fontSize: '1.25rem', // 20px
      fontWeight: 600,
      color: '#212529',
    },
    h4: {
      fontSize: '1.125rem', // 18px
      fontWeight: 600,
      color: '#212529',
    },
    h5: {
      fontSize: '1rem', // 16px
      fontWeight: 600,
      color: '#212529',
    },
    h6: {
      fontSize: '0.875rem', // 14px
      fontWeight: 600,
      color: '#212529',
    },
    body1: {
      fontSize: '0.875rem', // 14px
      color: '#495057',
      lineHeight: 1.6,
    },
    body2: {
      fontSize: '0.8125rem', // 13px
      color: '#6C757D',
      lineHeight: 1.5,
    },
    button: {
      textTransform: 'none', // No uppercase
      fontWeight: 500,
      fontSize: '0.875rem',
    },
    caption: {
      fontSize: '0.75rem', // 12px
      color: '#6C757D',
    },
  },
  shape: {
    borderRadius: 8, // Consistent rounded corners
  },
  shadows: [
    'none',
    '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
    '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
    '0 2px 4px 0 rgba(0, 0, 0, 0.08)',
    '0 4px 6px -1px rgba(0, 0, 0, 0.08)',
    '0 6px 10px -2px rgba(0, 0, 0, 0.08)',
    '0 8px 16px -4px rgba(0, 0, 0, 0.08)',
    ...Array(18).fill('0 8px 16px -4px rgba(0, 0, 0, 0.08)'),
  ],
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          scrollbarColor: '#ADB5BD #F8F9FA',
          '&::-webkit-scrollbar, & *::-webkit-scrollbar': {
            width: 8,
            height: 8,
          },
          '&::-webkit-scrollbar-thumb, & *::-webkit-scrollbar-thumb': {
            borderRadius: 8,
            backgroundColor: '#ADB5BD',
          },
          '&::-webkit-scrollbar-track, & *::-webkit-scrollbar-track': {
            borderRadius: 8,
            backgroundColor: '#F8F9FA',
          },
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 10,
          padding: '10px 20px',
          fontSize: '0.875rem',
          fontWeight: 600,
          boxShadow: 'none',
          textTransform: 'none',
          transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
          position: 'relative',
          overflow: 'hidden',
          '&::before': {
            content: '""',
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background: 'linear-gradient(180deg, rgba(255,255,255,0.15) 0%, rgba(255,255,255,0) 100%)',
            opacity: 0,
            transition: 'opacity 0.2s ease',
          },
          '&:hover::before': {
            opacity: 1,
          },
          '&:active': {
            transform: 'scale(0.98)',
          },
        },
        sizeSmall: {
          padding: '6px 14px',
          fontSize: '0.8125rem',
          borderRadius: 8,
        },
        sizeLarge: {
          padding: '12px 28px',
          fontSize: '0.9375rem',
          borderRadius: 12,
        },
        contained: {
          background: '#14213D',
          color: '#FFFFFF',
          boxShadow: '0 2px 8px rgba(20, 33, 61, 0.25)',
          '&:hover': {
            background: '#1D3557',
            boxShadow: '0 4px 16px rgba(20, 33, 61, 0.35)',
            transform: 'translateY(-1px)',
          },
        },
        outlined: {
          borderColor: '#E9ECEF',
          borderWidth: '1.5px',
          color: '#495057',
          background: 'rgba(255,255,255,0.8)',
          backdropFilter: 'blur(8px)',
          '&:hover': {
            borderColor: '#5B5FED',
            backgroundColor: 'rgba(91, 95, 237, 0.04)',
            color: '#5B5FED',
            transform: 'translateY(-1px)',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.08)',
          },
        },
        text: {
          '&:hover': {
            backgroundColor: 'rgba(91, 95, 237, 0.08)',
            color: '#5B5FED',
          },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: 16,
          boxShadow: '0 1px 3px rgba(0, 0, 0, 0.04), 0 4px 12px rgba(0, 0, 0, 0.04)',
          border: '1px solid rgba(233, 236, 239, 0.8)',
          backgroundColor: 'rgba(255, 255, 255, 0.95)',
          backdropFilter: 'blur(20px)',
          transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
          '&:hover': {
            boxShadow: '0 4px 20px rgba(0, 0, 0, 0.08), 0 8px 32px rgba(91, 95, 237, 0.08)',
            transform: 'translateY(-2px)',
            borderColor: 'rgba(91, 95, 237, 0.2)',
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          borderRadius: 8,
          backgroundImage: 'none',
        },
        outlined: {
          border: '1px solid #E9ECEF',
        },
        elevation1: {
          boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
        },
        elevation2: {
          boxShadow: '0 2px 4px 0 rgba(0, 0, 0, 0.08)',
        },
      },
    },
    MuiTextField: {
      styleOverrides: {
        root: {
          '& .MuiOutlinedInput-root': {
            borderRadius: 10,
            backgroundColor: 'rgba(255, 255, 255, 0.9)',
            transition: 'all 0.2s ease',
            '& fieldset': {
              borderColor: '#E9ECEF',
              borderWidth: '1.5px',
              transition: 'all 0.2s ease',
            },
            '&:hover fieldset': {
              borderColor: '#ADB5BD',
            },
            '&.Mui-focused': {
              backgroundColor: '#FFFFFF',
              boxShadow: '0 0 0 4px rgba(91, 95, 237, 0.1)',
            },
            '&.Mui-focused fieldset': {
              borderColor: '#5B5FED',
              borderWidth: 2,
            },
          },
          '& .MuiInputBase-input': {
            fontSize: '0.875rem',
            padding: '12px 16px',
          },
        },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontSize: '0.875rem',
          fontWeight: 500,
          minHeight: 48,
          color: '#6C757D',
          transition: 'all 0.2s ease',
          position: 'relative',
          '&.Mui-selected': {
            color: '#5B5FED',
            fontWeight: 600,
          },
          '&:hover': {
            color: '#5B5FED',
            backgroundColor: 'rgba(91, 95, 237, 0.04)',
          },
        },
      },
    },
    MuiTabs: {
      styleOverrides: {
        root: {
          borderBottom: '1px solid #E9ECEF',
        },
        indicator: {
          backgroundColor: '#5B5FED',
          height: 3,
          borderRadius: '3px 3px 0 0',
          transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        },
      },
    },
    MuiDialog: {
      styleOverrides: {
        paper: {
          borderRadius: 20,
          boxShadow: '0 24px 48px -12px rgba(0, 0, 0, 0.18)',
          border: '1px solid rgba(233, 236, 239, 0.5)',
          backdropFilter: 'blur(20px)',
        },
      },
    },
    MuiIconButton: {
      styleOverrides: {
        root: {
          transition: 'all 0.2s ease',
          '&:hover': {
            transform: 'scale(1.1)',
            backgroundColor: 'rgba(91, 95, 237, 0.08)',
          },
          '&:active': {
            transform: 'scale(0.95)',
          },
        },
      },
    },
    MuiDialogTitle: {
      styleOverrides: {
        root: {
          padding: '20px 24px',
          fontSize: '1.25rem',
          fontWeight: 600,
          color: '#212529',
          borderBottom: '1px solid #E9ECEF',
        },
      },
    },
    MuiDialogContent: {
      styleOverrides: {
        root: {
          padding: '24px',
        },
      },
    },
    MuiDivider: {
      styleOverrides: {
        root: {
          borderColor: '#E9ECEF',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 8,
          fontSize: '0.75rem',
          fontWeight: 600,
          height: 26,
          transition: 'all 0.2s ease',
          '&:hover': {
            transform: 'scale(1.02)',
          },
        },
        colorSuccess: {
          backgroundColor: '#D4EDDA',
          color: '#155724',
        },
        colorPrimary: {
          background: 'linear-gradient(135deg, #EEF0FE 0%, #E0E2FD 100%)',
          color: '#5B5FED',
        },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: {
          '& .MuiTableCell-head': {
            backgroundColor: '#F8F9FA',
            color: '#495057',
            fontWeight: 600,
            fontSize: '0.8125rem',
            textTransform: 'uppercase',
            letterSpacing: '0.5px',
            padding: '12px 16px',
            borderBottom: '2px solid #DEE2E6',
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          fontSize: '0.875rem',
          padding: '12px 16px',
          borderBottom: '1px solid #E9ECEF',
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:hover': {
            backgroundColor: '#F8F9FA',
          },
        },
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          backgroundColor: '#212529',
          fontSize: '0.75rem',
          padding: '6px 12px',
          borderRadius: 6,
        },
      },
    },
    MuiSnackbar: {
      styleOverrides: {
        root: {
          '& .MuiAlert-root': {
            borderRadius: 8,
            fontSize: '0.875rem',
          },
        },
      },
    },
    MuiListItemButton: {
      styleOverrides: {
        root: {
          borderRadius: 10,
          marginBottom: 4,
          transition: 'all 0.2s cubic-bezier(0.4, 0, 0.2, 1)',
          '&.Mui-selected': {
            backgroundColor: '#EEF0FE',
            color: '#5B5FED',
            boxShadow: 'inset 3px 0 0 #5B5FED',
            '&:hover': {
              backgroundColor: '#E0E2FD',
            },
            '& .MuiListItemIcon-root': {
              color: '#5B5FED',
            },
          },
          '&:hover': {
            backgroundColor: '#F8F9FA',
            transform: 'translateX(4px)',
          },
        },
      },
    },
    MuiListItemIcon: {
      styleOverrides: {
        root: {
          minWidth: 40,
          color: '#6C757D',
        },
      },
    },
  },
});

export default fyntracTheme;
