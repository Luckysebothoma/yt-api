// lib/heartbeat.ts
import 'dotenv/config';
import { redis } from './redis';

export async function workerHeartbeat(workerName: string): Promise<void> {
  try {
    await redis.set(
      `heartbeat:${workerName}`,
      new Date().toISOString(),
      'EX', 60,
    );
  } catch (err: any) {
    console.warn(`[heartbeat] Failed to write for ${workerName}: ${err.message}`);
  }
}