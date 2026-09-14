import { Hono } from 'hono';
import { uploadQueue } from '../queues/index';
import { Logger } from '../lib/logger';

export const uploadRouter = new Hono();
const log = new Logger('api:upload');

// POST /upload/batch — enqueue up to 15 videos for batch upload
// Body: { groupedDir: string } — path to grouped folder from worker1 output
uploadRouter.post('/batch', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const job = await uploadQueue.add('upload-batch', {
    groupedDir: body.groupedDir,
    batchSize:  15,               // YouTube Studio hard limit
    visibility: 'unlisted',
    madeForKids: false, 
  });
  await log.info(`Enqueued upload-batch job ${job.id}`);
  return c.json({ jobId: job.id, status: 'queued' });
});

// POST /upload/single — upload one video file
// Body: { filePath: string, title?: string }
uploadRouter.post('/single', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const job = await uploadQueue.add('upload-single', {
    filePath:    body.filePath,
    title:       body.title ?? '',
    visibility:  'unlisted',
    madeForKids: false,
  });
  await log.info(`Enqueued upload-single job ${job.id}`);
  return c.json({ jobId: job.id, status: 'queued' });
});

// GET /upload/job/:id
uploadRouter.get('/job/:id', async (c) => {
  const { Job } = await import('bullmq');
  const job = await Job.fromId(uploadQueue, c.req.param('id'));
  if (!job) return c.json({ error: 'Job not found' }, 404);
  return c.json({ jobId: job.id, state: await job.getState(), result: job.returnvalue });
});
