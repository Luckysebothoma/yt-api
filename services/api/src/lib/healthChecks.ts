// services/api/src/lib/healthChecks.ts
// Standalone infra checks used by the periodic poller in index.ts.
// Each function is independently callable and returns a consistent { ok, ... } shape.

import 'dotenv/config';
import { Queue } from 'bullmq';
import type { Redis } from 'ioredis';
import { Pool }  from 'pg';
import { redisConnectionWithRetry } from './redis';

// ── Re-export YouTube check so index.ts only needs one import source ──────────
export { checkYouTube } from './ytHealth';
// 12.70 src/lib/healthChecks.ts(23,18): error TS2339: Property 'ping' does not exist on type 'IRedisClient'.
// ── Redis ─────────────────────────────────────────────────────────────────────
export async function checkRedis(): Promise<{
  ok: boolean;
  latencyMs?: number;
  error?: string;
}> {
  const start = Date.now();
  const q     = new Queue('__ping__', { connection: redisConnectionWithRetry });
  try {
    const client = (await q.client) as unknown as Redis;
    await client.ping();
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err: any) {
    return { ok: false, error: err.message };
  } finally {
    await q.close();
  }
}

// ── Postgres ──────────────────────────────────────────────────────────────────
export async function checkPostgres(): Promise<{
  ok: boolean;
  latencyMs?: number;
  version?: string;
  error?: string;
}> {
  const missing = ['POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']
    .filter((k) => !process.env[k]);

  if (missing.length) {
    return { ok: false, error: `Missing env vars: ${missing.join(', ')}` };
  }

  const pool = new Pool({
    host:                    process.env.POSTGRES_HOST,
    port:                    Number(process.env.POSTGRES_PORT ?? 5432),
    database:                process.env.POSTGRES_DB,
    user:                    process.env.POSTGRES_USER,
    password:                process.env.POSTGRES_PASSWORD,
    connectionTimeoutMillis: 3000,
  });

  const start = Date.now();
  try {
    const res     = await pool.query('SELECT version()');
    const version = (res.rows[0]?.version as string)
      ?.split(' ')
      .slice(0, 2)
      .join(' ');
    return { ok: true, latencyMs: Date.now() - start, version };
  } catch (err: any) {
    return { ok: false, error: err.message };
  } finally {
    await pool.end();
  }
}