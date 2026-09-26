import { Hono } from 'hono';
import { viewQueue } from '../queues/index';
import { Logger } from '../lib/logger';

export const viewRouter = new Hono();
const log = new Logger('api:view');

// GET /view/files — list grouped files from filesystem
viewRouter.get('/files', async (c) => {
  const job = await viewQueue.add('list-files', { mediaPath: process.env.MEDIA_PATH });
  await log.info(`Enqueued list-files job ${job.id}`);
  return c.json({ jobId: job.id, status: 'queued' });
});

// GET /view/yt-videos — fetch all videos from YouTube channel
viewRouter.get('/yt-videos', async (c) => {
  const job = await viewQueue.add('yt-list', {});
  await log.info(`Enqueued yt-list job ${job.id}`);
  return c.json({ jobId: job.id, status: 'queued' });
});

// GET /view/links — get all stored YouTube URLs from DB
viewRouter.get('/links', async (c) => {
  const job = await viewQueue.add(
    'get-links',
    { limit: c.req.query('limit'), offset: c.req.query('offset') },
    {
      removeOnComplete: { age: 600, count: 50 },
      removeOnFail:     { age: 3600, count: 50 },
    },
  );
  await log.info(`Enqueued get-links job ${job.id}`);
  return c.json({ jobId: job.id, status: 'queued' });
});


// GET /view/job/:id — poll job result
viewRouter.get('/job/:id', async (c) => {
  const { Job } = await import('bullmq');
  const { redisConnection } = await import('../lib/redis');
  const job = await Job.fromId(viewQueue, c.req.param('id'));
  if (!job) return c.json({ error: 'Job not found' }, 404);
  const state  = await job.getState();
  const result = job.returnvalue;
  return c.json({ jobId: job.id, state, result });
});
