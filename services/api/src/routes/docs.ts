import { Hono } from 'hono';

export const docsRouter = new Hono();

// GET /docs — live endpoint map
docsRouter.get('/', (c) => c.json({
  service: 'youtube-media-stack API',
  endpoints: [
    { method: 'GET',  path: '/health',          description: 'Health check' },
    { method: 'GET',  path: '/docs',            description: 'This endpoint map' },
    { method: 'GET',  path: '/view/files',      worker: 'worker2', description: 'List grouped media from filesystem' },
    { method: 'GET',  path: '/view/yt-videos',  worker: 'worker2', description: 'Fetch all videos from YouTube channel via API' },
    { method: 'GET',  path: '/view/links',      worker: 'worker2', description: 'Get all stored YouTube URLs from PostgreSQL' },
    { method: 'GET',  path: '/view/job/:id',    worker: 'worker2', description: 'Poll view job result by id' },
    { method: 'POST', path: '/arrange',         worker: 'worker1', description: 'Group media by filename pattern. Body: { targetDir, dryRun, mediaType }' },
    { method: 'GET',  path: '/arrange/job/:id', worker: 'worker1', description: 'Poll arrange job result by id' },
    { method: 'POST', path: '/upload/batch',    worker: 'worker3', description: 'Batch upload up to 15 videos. Body: { groupedDir }' },
    { method: 'POST', path: '/upload/single',   worker: 'worker3', description: 'Single video upload. Body: { filePath, title }' },
    { method: 'GET',  path: '/upload/job/:id',  worker: 'worker3', description: 'Poll upload job result by id' },
  ],
}));
