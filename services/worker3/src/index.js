'use strict';
require('dotenv/config');
const { Worker }          = require('bullmq');
const express             = require('express');
const { redisConnection } = require('./lib/redis');
const { runMigration }    = require('./lib/migrate');
const { handleBatch }     = require('./jobs/uploadBatch');
const { handleSingle }    = require('./jobs/uploadSingle');
const { Logger }          = require('./lib/logger');
const streamRouter        = require('./routes/stream');
const { workerHeartbeat }  = require('./lib/heartbeat');
const { check_if_dir_is_accessible } = require('./lib/registry');
const log  = new Logger('worker3');
const PORT = Number(process.env.STREAM_PORT ?? 3099);

async function start() {
  await runMigration();

  // Fail fast if the media directory isn't mounted/readable — better to
  // crash before accepting jobs than to accept them and fail mid-batch.
//  const groupedDir = process.env.GROUPED_DIR ?? '/mnt/grouped';
 // const dirAccessible = await check_if_dir_is_accessible(groupedDir);
//  if (!dirAccessible) {
//    throw new Error(`Directory not accessible: ${groupedDir}`);
//  }

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

  // heartbeat loop
  setInterval(() => workerHeartbeat('worker3'), 30 * 1000);
  await workerHeartbeat('worker3'); // initial heartbeat
  
  // ── Express HTTP server ────────────────────────────────────────────────────
  const app = express();
  app.use(express.json());

  // Health check
  app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'worker3', version: '4.0.0' }));

  // Stream routes
  app.use('/stream', streamRouter);

  // 404 fallback
  app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

  app.listen(PORT, () => {
    log.info(`HTTP server listening on :${PORT}`);
    log.info(`  GET  /stream/allpaginator?page=1&limit=50`);
    log.info(`  GET  /stream/video/:videoId`);
    log.info(`  GET  /stream/watchlater`);
    log.info(`  DELETE /stream/watchlater/:videoId`);
    log.info(`  POST /stream/watchlater/retry`);
  });

  await log.info('worker3 v4 ready — registry tier: Redis → PG → YouTube API | Stream + Watch-Later active');
}

start().catch(err => {
  console.error('[worker3] Fatal startup error:', err);
  process.exit(1);
});
