import { Queue } from 'bullmq';
import { redisConnection } from '../lib/redis';

// One queue per worker domain
export const arrangeQueue = new Queue('arrange-queue', { connection: redisConnection });
export const viewQueue    = new Queue('view-queue',    { connection: redisConnection });
export const uploadQueue  = new Queue('upload-queue',  { connection: redisConnection });
