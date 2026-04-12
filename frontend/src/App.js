import React from "react";
import "@/App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import theme from './theme/theme';
import { ToastProvider } from './components/ToastProvider';
import Dashboard from "./pages/Dashboard";

function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <ToastProvider>
        <div className="App">
          <BrowserRouter>
            <Routes>
              <Route path="/" element={<Dashboard />} />
            </Routes>
          </BrowserRouter>
        </div>
      </ToastProvider>
    </ThemeProvider>
  );
}

export default App;
