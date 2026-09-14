import 'dotenv/config';
import { Worker } from 'bullmq';
import { redisConnectionWithRetry } from './lib/redis';
import { handleListFiles }       from './jobs/listFilesJob';
import { handleGetYTVideos }     from './jobs/getYouTubeVideos';
import { handleGetLinks }        from './jobs/getLinksJob';
import { handleYouTubeTest }     from './jobs/testYouTubeConnection';
import { Logger }                from './lib/logger';
import { workerHeartbeat }       from './lib/heartbeat';

const log = new Logger('worker2');

async function main() {
  const worker = new Worker('view-queue', async (job) => {
    await log.info(`Processing job ${job.id} type=${job.name}`);
    if (job.name === 'yt-test')    return handleYouTubeTest(job);
    if (job.name === 'list-files') return handleListFiles(job);
    if (job.name === 'yt-list')    return handleGetYTVideos(job);
    if (job.name === 'get-links')  return handleGetLinks(job);
    throw new Error(`Unknown job type: ${job.name}`);
  }, {
    connection: redisConnectionWithRetry,
    concurrency: 3,
  });

  worker.on('completed', (job) => console.log(`[worker2] Job ${job.id} completed`));
  worker.on('failed',    (job, err) => console.error(`[worker2] Job ${job?.id} failed:`, err.message));

  await workerHeartbeat('worker2');
  setInterval(() => workerHeartbeat('worker2'), 30_000);

  console.log('[worker2] Listening on view-queue');
}

main().catch((err) => {
  console.error('[worker2] Fatal startup error:', err);
  process.exit(1);
});