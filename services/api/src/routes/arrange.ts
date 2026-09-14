import { Hono } from 'hono';
import { arrangeQueue } from '../queues/index';
import { Logger } from '../lib/logger';

export const arrangeRouter = new Hono();
const log = new Logger('api:arrange');

// POST /arrange — group media files by filename pattern
// Body: { targetDir?: string, dryRun?: boolean, mediaType?: "images"|"videos"|"both" }
arrangeRouter.post('/', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const payload = {
    targetDir: body.targetDir ?? process.env.MEDIA_PATH,
    dryRun:    body.dryRun    ?? false,
    mediaType: body.mediaType ?? 'both',   // images | videos | both
  };
  const job = await arrangeQueue.add('arrange', payload);
  await log.info(`Enqueued arrange job ${job.id} for ${payload.targetDir}`);
  return c.json({ jobId: job.id, status: 'queued' });
});

// GET /arrange/job/:id
arrangeRouter.get('/job/:id', async (c) => {
  const { Job } = await import('bullmq');
  const job = await Job.fromId(arrangeQueue, c.req.param('id'));
  if (!job) return c.json({ error: 'Job not found' }, 404);
  return c.json({ jobId: job.id, state: await job.getState(), result: job.returnvalue });
});
