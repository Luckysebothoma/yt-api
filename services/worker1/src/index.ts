import 'dotenv/config';
import { Worker } from 'bullmq';
import { bullMQConnectionOptions } from './lib/redis';  // ← changed
import { redisConnectionWithRetry } from './lib/redis';
import { handleArrange } from './jobs/arrangeJob';
import { Logger } from './lib/logger';
import { workerHeartbeat } from './lib/heartbeat';

const log = new Logger('worker1');

async function main() {
  const worker = new Worker('arrange-queue', async (job) => {
    await log.info(`Processing job ${job.id} type=${job.name}`);
    if (job.name === 'arrange') return handleArrange(job);
    throw new Error(`Unknown job type: ${job.name}`);
  }, {
    connection: bullMQConnectionOptions,
    concurrency: 2,
    lockDuration:   120_000,  // 2 min lock — renewer fires at 30s so this is very safe
    lockRenewTime:   30_000,  // BullMQ's built-in renewal cadence (lockDuration / 4 is the rule of thumb)
    stalledInterval: 30_000,  // how often BullMQ checks for stalled jobs
    maxStalledCount: 1,       // allow one stall recovery before marking the job failed
  });

  worker.on('completed', (job) =>
    console.log(`[worker1] Job ${job.id} completed`));
  worker.on('failed', (job, err) =>
    console.error(`[worker1] Job ${job?.id} failed:`, err.message));

  // ✅ Surface lock renewal failures so they're visible in logs
  worker.on('error', (err) =>
    console.error(`[worker1] Worker error:`, err.message));

  await workerHeartbeat('worker1');
  setInterval(() => workerHeartbeat('worker1'), 30_000);

  console.log('[worker1] Listening on arrange-queue');
}

main().catch((err) => {
  console.error('[worker1] Fatal startup error:', err);
  process.exit(1);
});