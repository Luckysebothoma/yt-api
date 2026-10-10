'use strict';
require('dotenv/config');
const { Worker }          = require('bullmq');
const express             = require('express');
const { redisConnection, getRedis } = require('./lib/redis');
const { db }              = require('./lib/db');
const { runMigration }    = require('./lib/migrate');
const { handleBatch }     = require('./jobs/uploadBatch');
const { handleSingle }    = require('./jobs/uploadSingle');
const { Logger }          = require('./lib/logger');
const streamRouter        = require('./routes/stream');
const { workerHeartbeat } = require('./lib/heartbeat');
const { recoverOrphans }  = require('./lib/registry');
const {
  setShutdownHandler, requestShutdown, isShuttingDown,
} = require('./lib/shutdown');

const log  = new Logger('worker3');

// Fail fast if a job handler is missing, instead of booting and failing every queued job.
for (const [name, fn] of Object.entries({ handleBatch, handleSingle })) {
  if (typeof fn !== 'function') {
    console.error(`[worker3] FATAL: ${name} is not a function — check src/jobs exports. Refusing to start.`);
    process.exit(1);
  }
}
const PORT = Number(process.env.STREAM_PORT ?? 3099);

// Max time to wait for the in-flight upload before giving up. NOTE: docker
// swarm sends SIGKILL after the service's stop_grace_period (default 10s), so
// set stop_grace_period in docker-compose to at least this long.
const SHUTDOWN_TIMEOUT_MS = Number(process.env.SHUTDOWN_TIMEOUT_MS ?? 15 * 60 * 1000);

async function start() {
  await runMigration();

  // Sync any uploads that reached YouTube but never reached Postgres
  // (parked in Redis by a previous run) BEFORE accepting new jobs.
  try {
    const n = await recoverOrphans(log);
    if (n) await log.info(`Startup: recovered ${n} orphaned upload(s)`);
  } catch (err) {
    await log.warn(`Startup orphan recovery failed: ${err.message}`);
  }

  // ── BullMQ Worker ──────────────────────────────────────────────────────────
  const worker = new Worker('upload-queue', async (job) => {
    await log.info(`Processing job ${job.id} type=${job.name}`);
    if (job.name === 'upload-batch')  return handleBatch(job);
    if (job.name === 'upload-single') return handleSingle(job);
    throw new Error(`Unknown job type: ${job.name}`);
  }, {
    connection:  redisConnection,
    concurrency: 1,
  });

  worker.on('completed', (job) => log.info(`Job completed: ${job.id}`));
  worker.on('failed',    (job, err) => log.error(`Job ${job?.id} failed: ${err.message}`));
  worker.on('error',     (err) => console.error('[worker3] BullMQ worker error:', err.message));

  // heartbeat loop
  const heartbeatTimer = setInterval(() => workerHeartbeat('worker3'), 30 * 1000);
  await workerHeartbeat('worker3'); // initial heartbeat

  // ── Express HTTP server ────────────────────────────────────────────────────
  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => {
    if (isShuttingDown()) return res.status(503).json({ status: 'shutting_down', service: 'worker3' });
    res.json({ status: 'ok', service: 'worker3', version: '4.0.0' });
  });

  app.use('/stream', streamRouter);
  app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

  const server = app.listen(PORT, () => {
    log.info(`HTTP server listening on :${PORT}`);
    log.info(`  GET  /stream/allpaginator?page=1&limit=50`);
    log.info(`  GET  /stream/video/:videoId`);
    log.info(`  GET  /stream/watchlater`);
    log.info(`  DELETE /stream/watchlater/:videoId`);
    log.info(`  POST /stream/watchlater/retry`);
  });

  // ── Graceful shutdown ──────────────────────────────────────────────────────
  setShutdownHandler(async (reason, code) => {
    console.log(`[worker3] Shutting down (${reason}) — letting the current job finish…`);

    const force = setTimeout(() => {
      console.error(`[worker3] Shutdown timed out after ${SHUTDOWN_TIMEOUT_MS}ms — forcing exit`);
      process.exit(code || 1);
    }, SHUTDOWN_TIMEOUT_MS);
    force.unref();

    clearInterval(heartbeatTimer);

    // Stop accepting new HTTP connections; let in-flight streams finish.
    const serverClosed = new Promise((resolve) => server.close(() => resolve()));
    if (typeof server.closeIdleConnections === 'function') server.closeIdleConnections();

    try {
      // close() stops fetching new jobs and waits for the active one.
      await worker.close();
      console.log('[worker3] BullMQ worker closed');
    } catch (err) {
      console.error('[worker3] Error closing worker:', err.message);
    }

    await serverClosed;

    try { await db.end(); } catch (err) { console.error('[worker3] db.end failed:', err.message); }
    try { await getRedis().quit(); } catch { /* already closed */ }

    console.log('[worker3] Shutdown complete');
    process.exit(code);
  });

  for (const sig of ['SIGTERM', 'SIGINT']) {
    process.on(sig, () => {
      if (isShuttingDown()) {
        console.error(`[worker3] Second ${sig} — forcing exit`);
        process.exit(1);
      }
      requestShutdown(sig, 0);
    });
  }

  await log.info('worker3 v4 ready — registry tier: Redis → PG → YouTube API | Stream + Watch-Later active');
}

process.on('unhandledRejection', (reason) => {
  console.error('[worker3] Unhandled rejection:', reason);
});

start().catch((err) => {
  console.error('[worker3] Fatal startup error:', err);
  process.exit(1);
});