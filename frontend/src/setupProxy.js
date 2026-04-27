const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  console.log('[Proxy] Setting up /api/** -> http://localhost:8585 (gateway)');

  // Mount at ROOT — NOT at '/api' — so Express does NOT strip the prefix.
  // The pathFilter ensures only /api/** requests are proxied.
  // The full path (including /api) is forwarded to the gateway unchanged.
  app.use(
    createProxyMiddleware({
      target: 'http://localhost:8585',
      changeOrigin: true,
      pathFilter: '/api/**',   // http-proxy-middleware v3 filter syntax
      on: {
        proxyReq: (proxyReq, req) => {
          console.log('[Proxy] Forwarding:', req.method, req.path, '→', 'http://localhost:8585' + req.path);
        },
        error: (err, req, res) => {
          console.error('[Proxy] Error:', err.message);
          if (!res.headersSent) {
            res.status(502).json({ error: 'Gateway unreachable: ' + err.message });
          }
        },
      },
    })
  );
};
