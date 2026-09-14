import 'dotenv/config';
import { redisConnectionWithRetry } from './redis';
import Redis from 'ioredis';

let client: Redis | null = null;

function getClient(): Redis {
  if (!client) client = new Redis(redisConnectionWithRetry);
  return client;
}

export async function workerHeartbeat(workerName: string): Promise<void> {
  try {
    await getClient().set(
      `heartbeat:${workerName}`,
      new Date().toISOString(),
      'EX', 60,
    );
  } catch (err: any) {
    console.warn(`[heartbeat] Failed to write for ${workerName}: ${err.message}`);
  }
}
