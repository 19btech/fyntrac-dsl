const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  // ── /api/dsl/** → backend directly (localhost:8000) ───────────────────
  // The DSL backend runs natively when using ./startup.sh, so we bypass
  // the gateway for these requests.
  console.log('[Proxy] Setting up /api/dsl/** -> http://localhost:8000 (backend direct)');
  app.use(
    '/api/dsl',
    createProxyMiddleware({
      target: 'http://localhost:8000',
      changeOrigin: true,
      proxyTimeout: 0,
      timeout: 0,
      buffer: false,
      on: {
        proxyReq: (proxyReq, req) => {
          console.log('[Proxy] DSL:', req.method, req.originalUrl, '→', 'http://localhost:8000' + req.originalUrl);
        },
        proxyRes: (proxyRes, req, res) => {
          const ct = proxyRes.headers['content-type'] || '';
          if (ct.includes('text/event-stream')) {
            proxyRes.headers['cache-control'] = 'no-cache, no-transform';
            proxyRes.headers['x-accel-buffering'] = 'no';
            delete proxyRes.headers['content-length'];
          }
        },
        error: (err, req, res) => {
          console.error('[Proxy] DSL Error:', err.message);
          if (!res.headersSent) {
            res.status(502).json({ error: 'Backend unreachable: ' + err.message });
          }
        },
      },
    })
  );

  // ── /api/** (everything else) → gateway (localhost:8585) ──────────────
  // Other services (dataloader, reporting, etc.) are reached via the gateway.
  console.log('[Proxy] Setting up /api/** -> http://localhost:8585 (gateway)');
  app.use(
    '/api',
    createProxyMiddleware({
      target: 'http://localhost:8585',
      changeOrigin: true,
      proxyTimeout: 0,
      timeout: 0,
      buffer: false,
      on: {
        proxyReq: (proxyReq, req) => {
          console.log('[Proxy] Gateway:', req.method, req.originalUrl, '→', 'http://localhost:8585' + req.originalUrl);
        },
        error: (err, req, res) => {
          console.error('[Proxy] Gateway Error:', err.message);
          if (!res.headersSent) {
            res.status(502).json({ error: 'Gateway unreachable: ' + err.message });
          }
        },
      },
    })
  );
};
