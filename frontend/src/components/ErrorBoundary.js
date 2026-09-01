import React from "react";
import { Box, Typography, Button, Paper } from "@mui/material";
import { AlertTriangle, RefreshCw } from "lucide-react";

/**
 * App-level error boundary. Catches render-time errors anywhere below it and
 * shows a recoverable fallback instead of a blank white screen.
 *
 * React requires error boundaries to be class components.
 */
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    // Surface to the console for diagnostics; a real deployment can forward
    // this to an error-reporting service here.
    // eslint-disable-next-line no-console
    console.error("Unhandled UI error:", error, errorInfo);
  }

  handleReload = () => {
    window.location.reload();
  };

  handleReset = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (!this.state.hasError) return this.props.children;

    return (
      <Box
        sx={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          bgcolor: "background.default",
          p: 3,
        }}
      >
        <Paper
          elevation={0}
          sx={{
            maxWidth: 440,
            width: "100%",
            textAlign: "center",
            p: 4,
            borderRadius: 4,
            border: "1px solid",
            borderColor: "divider",
          }}
        >
          <Box
            sx={{
              width: 56,
              height: 56,
              borderRadius: "50%",
              bgcolor: "error.50",
              color: "error.main",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              mx: "auto",
              mb: 2,
            }}
          >
            <AlertTriangle size={28} />
          </Box>
          <Typography variant="h5" sx={{ mb: 1 }}>
            Something went wrong
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            The screen hit an unexpected error. Your data is safe — try again, or
            reload the app.
          </Typography>
          {this.state.error?.message && (
            <Typography
              variant="caption"
              component="pre"
              sx={{
                display: "block",
                textAlign: "left",
                bgcolor: "grey.100",
                color: "text.secondary",
                borderRadius: 2,
                p: 1.5,
                mb: 3,
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                fontFamily: "monospace",
              }}
            >
              {String(this.state.error.message).slice(0, 300)}
            </Typography>
          )}
          <Box sx={{ display: "flex", gap: 1.5, justifyContent: "center" }}>
            <Button variant="outlined" onClick={this.handleReset}>
              Try again
            </Button>
            <Button
              variant="contained"
              startIcon={<RefreshCw size={16} />}
              onClick={this.handleReload}
            >
              Reload app
            </Button>
          </Box>
        </Paper>
      </Box>
    );
  }
}

export default ErrorBoundary;
