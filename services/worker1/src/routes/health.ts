// services/api/src/routes/health.ts
import 'dotenv/config';
import { Hono }       from 'hono';
import { Queue }      from 'bullmq';
import { Pool }       from 'pg';
import { checkYouTube, YTHealthResult } from '../lib/ytHealth';
import { bullMQConnectionOptions } from '../lib/redis';  // ← changed

export const healthRouter = new Hono();

// ─── Helpers ─────────────────────────────────────────────────────────────────
async function checkRedis(): Promise<{ ok: boolean; latencyMs?: number; error?: string }> {
  const start = Date.now();
  const q = new Queue('__health-ping__', { connection: bullMQConnectionOptions });  // ← changed
  try {
    await q.client;
    const latencyMs = Date.now() - start;
    return { ok: true, latencyMs };
  } catch (err: any) {
    return { ok: false, error: err.message };
  } finally {
    await q.close();
  }
}

async function checkPostgres(): Promise<{ ok: boolean; latencyMs?: number; version?: string; error?: string }> {
  const pool = new Pool({
    host:     process.env.POSTGRES_HOST,
    port:     Number(process.env.POSTGRES_PORT ?? 5432),
    database: process.env.POSTGRES_DB,
    user:     process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    connectionTimeoutMillis: 3000,
  });
  const start = Date.now();
  try {
    const res       = await pool.query('SELECT version()');
    const latencyMs = Date.now() - start;
    const version   = (res.rows[0]?.version as string)?.split(' ').slice(0, 2).join(' ');
    return { ok: true, latencyMs, version };
  } catch (err: any) {
    return { ok: false, error: err.message };
  } finally {
    await pool.end();
  }
}

function queueSummary(name: string) {
  return async (): Promise<{ queue: string; ok: boolean; waiting?: number; active?: number; failed?: number; error?: string }> => {
    const q = new Queue(name, { connection: bullMQConnectionOptions });  // ← changed
    try {
      const [waiting, active, failed] = await Promise.all([
        q.getWaitingCount(),
        q.getActiveCount(),
        q.getFailedCount(),
      ]);
      return { queue: name, ok: true, waiting, active, failed };
    } catch (err: any) {
      return { queue: name, ok: false, error: err.message };
    } finally {
      await q.close();
    }
  };
}

// ─── GET /health — liveness only ─────────────────────────────────────────────
healthRouter.get('/', (c) =>
  c.json({ ok: true, service: 'api', ts: new Date().toISOString() })
);

// ─── GET /health/full — deep readiness check ─────────────────────────────────
healthRouter.get('/full', async (c) => {
  const [redis, postgres, youtube, arrangeQ, viewQ, uploadQ] = await Promise.allSettled([
    checkRedis(),
    checkPostgres(),
    checkYouTube(),
    queueSummary('arrange-queue')(),
    queueSummary('view-queue')(),
    queueSummary('upload-queue')(),
  ]);

  const get = <T>(r: PromiseSettledResult<T>): T | { ok: false; error: string } =>
    r.status === 'fulfilled'
      ? r.value
      : { ok: false, error: (r as PromiseRejectedResult).reason?.message ?? 'unknown' };

  const r = {
    redis:    get(redis),
    postgres: get(postgres),
    youtube:  get(youtube) as YTHealthResult,
    queues: {
      arrange: get(arrangeQ),
      view:    get(viewQ),
      upload:  get(uploadQ),
    },
  };

  const allOk =
    (r.redis    as any).ok &&
    (r.postgres as any).ok &&
    r.youtube.ok &&
    (r.queues.arrange as any).ok &&
    (r.queues.view    as any).ok &&
    (r.queues.upload  as any).ok;

  return c.json(
    { ok: allOk, checkedAt: new Date().toISOString(), ...r },
    allOk ? 200 : 503,
  );
});