// Global error handlers — use textContent to prevent XSS
window.addEventListener('error', (event) => {
  console.error('Global error:', event.error?.message);
});

window.addEventListener('unhandledrejection', (event) => {
  console.error('Unhandled rejection:', event.reason);
});

import React from "react";
import ReactDOM from "react-dom/client";
import axios from "axios";
import "@/index.css";
import App from "@/App";

// ── Token Bootstrap ─────────────────────────────────────────────────────────
// When DSL Studio is opened from fyntrac-web, the ID token is passed as a
// ?token=<jwt> URL parameter. We read it once, store it in sessionStorage
// (so it survives page refreshes within this tab), then strip it from the URL
// so it doesn't leak in the browser history.
(function bootstrapToken() {
  const params = new URLSearchParams(window.location.search);
  const urlToken = params.get('token');
  const urlTenant = params.get('tenant');
  
  if (urlToken) {
    sessionStorage.setItem('dsl_auth_token', urlToken);
    console.info('[DSL Studio] Auth token received and stored.');
    params.delete('token');
  }
  
  if (urlTenant) {
    sessionStorage.setItem('dsl_tenant', urlTenant);
    console.info('[DSL Studio] Tenant received and stored.');
    params.delete('tenant');
  }

  // Also remove firstName if it exists to keep URL clean
  if (params.has('firstName')) {
    params.delete('firstName');
  }

  if (urlToken || urlTenant) {
    const cleanUrl = window.location.pathname + (params.toString() ? '?' + params.toString() : '');
    window.history.replaceState({}, document.title, cleanUrl);
  }
})();

// ── Axios defaults ───────────────────────────────────────────────────────────
// Inject the Bearer token and X-Tenant into every request if available in sessionStorage
axios.interceptors.request.use((config) => {
  const token = sessionStorage.getItem('dsl_auth_token');
  const tenant = sessionStorage.getItem('dsl_tenant');
  
  config.headers = config.headers || {};
  
  if (token) {
    config.headers['Authorization'] = `Bearer ${token}`;
  }
  
  if (tenant) {
    config.headers['X-Tenant'] = tenant;
  }
  
  return config;
});

// Response interceptor: log detailed error info
axios.interceptors.response.use(
  response => response,
  error => {
    console.error('Axios Error Interceptor:', {
      url: error.config?.url,
      method: error.config?.method,
      status: error.response?.status,
      data: error.response?.data,
      message: error.message
    });
    return Promise.reject(error);
  }
);

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element not found!");
}

const root = ReactDOM.createRoot(rootElement);
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
