const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  console.log('[Proxy] Setting up /api -> http://localhost:8000 proxy (keeping /api path)');
  
  app.use(
    '/api',
    createProxyMiddleware({
      target: 'http://localhost:8000',
      changeOrigin: true,
      logLevel: 'debug',
      // Ensure the /api prefix is preserved when proxying to backend.
      // Some dev-server integrations may strip the prefix by default; explicitly
      // rewrite ^/api to /api (no-op) so the backend receives the expected path.
      pathRewrite: {
        '^/api': '/api'
      },
      onProxyReq: (proxyReq, req, res) => {
        console.log('[Proxy] Forwarding:', req.method, req.path);
      },
      onError: (err, req, res) => {
        console.error('[Proxy] Error:', err.message);
        res.status(500).json({ error: 'Backend connection failed: ' + err.message });
      },
    })
  );
};


