'use strict';
require('dotenv/config');
const express        = require('express');
const { runMigration } = require('./lib/migrate');

const syncRouter       = require('./routes/sync');
const duplicatesRouter = require('./routes/duplicates');
const metaRouter       = require('./routes/meta');
const quotaRouter      = require('./routes/quota');

const PORT = Number(process.env.WORKER4_PORT ?? 3100);

async function start() {
  await runMigration();

  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => res.json({
    status: 'ok', service: 'worker4', version: '1.0.0',
  }));

  app.use('/sync',       syncRouter);
  app.use('/duplicates', duplicatesRouter);
  app.use('/meta',       metaRouter);
  app.use('/quota',      quotaRouter);

  app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

  app.listen(PORT, () => {
    console.log(`[worker4] HTTP server listening on :${PORT}`);
    console.log(`  POST   /sync/pull`);
    console.log(`  GET    /sync/status`);
    console.log(`  GET    /duplicates`);
    console.log(`  GET    /duplicates/search?q=<term>`);
    console.log(`  POST   /duplicates/resolve`);
    console.log(`  GET    /meta/search?filename=<>&title=<>&tag=<>`);
    console.log(`  GET    /meta/:videoId`);
    console.log(`  PATCH  /meta/:videoId`);
    console.log(`  PATCH  /meta/:videoId/youtube`);
    console.log(`  GET    /quota`);
  });
}

start().catch(err => {
  console.error('[worker4] Fatal startup error:', err);
  process.exit(1);
});
