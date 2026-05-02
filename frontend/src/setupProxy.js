const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  console.log('[Proxy] Setting up /api -> http://localhost:8000 proxy (keeping /api path)');
  
  app.use(
    '/api',
    createProxyMiddleware({
      target: 'http://localhost:8000',
      changeOrigin: true,
      logLevel: 'debug',
      // Stream responses immediately (critical for SSE / agent runs).
      // selfHandleResponse:false (the default) lets node-http-proxy pipe
      // chunks straight through. We additionally disable timeouts and
      // buffering on the proxied response.
      proxyTimeout: 0,
      timeout: 0,
      buffer: false,
      // Ensure the /api prefix is preserved when proxying to backend.
      pathRewrite: {
        '^/api': '/api'
      },
      onProxyReq: (proxyReq, req, res) => {
        console.log('[Proxy] Forwarding:', req.method, req.path);
      },
      onProxyRes: (proxyRes, req, res) => {
        const ct = proxyRes.headers['content-type'] || '';
        if (ct.includes('text/event-stream')) {
          // Disable any compression / buffering that webpack-dev-server
          // or the host platform might otherwise apply to SSE responses.
          proxyRes.headers['cache-control'] = 'no-cache, no-transform';
          proxyRes.headers['x-accel-buffering'] = 'no';
          delete proxyRes.headers['content-length'];
        }
      },
      onError: (err, req, res) => {
        console.error('[Proxy] Error:', err.message);
        res.status(500).json({ error: 'Backend connection failed: ' + err.message });
      },
    })
  );
};


