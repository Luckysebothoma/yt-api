import 'dotenv/config';
import { redisConnectionWithRetry } from './redis.js';
import Redis from 'ioredis';

let client = null;

function getClient() {
  if (!client) client = new Redis(redisConnectionWithRetry);
  return client;
}

export async function workerHeartbeat(workerName) {
  try {
    await getClient().set(
      `heartbeat:${workerName}`,
      new Date().toISOString(),
      'EX', 60,
    );
  } catch (err) {
    console.warn(`[heartbeat] Failed to write for ${workerName}: ${err.message}`);
  }
}