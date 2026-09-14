// services/api/src/lib/health.ts
// Workers have NO HTTP server — health is read through Redis queue stats + heartbeat keys.

import { Hono } from 'hono';
import { Queue } from 'bullmq';
import { redisConnectionWithRetry } from './redis';

const health = new Hono();

const WORKER_QUEUES: Record<string, string> = {
  worker1: 'arrange-queue',
  worker2: 'view-queue',
  worker3: 'upload-queue',
};

async function getHeartbeat(name: string): Promise<{ lastSeen: string | null; stale: boolean }> {
  const q = new Queue('__heartbeats__', { connection: redisConnectionWithRetry });
  try {
    const client = await q.client;
    const val    = await client.get(`heartbeat:${name}`);
    const stale  = val
      ? (Date.now() - new Date(val).getTime()) > 90_000
      : true;
    return { lastSeen: val, stale };
  } finally {
    await q.close();
  }
}

async function getQueueStats(queueName: string): Promise<{
  ok: boolean;
  waiting?: number;
  active?: number;
  completed?: number;
  failed?: number;
  delayed?: number;
  lastJobAt?: string | null;
  error?: string;
}> {
  const q = new Queue(queueName, { connection: redisConnectionWithRetry });
  try {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      q.getWaitingCount(),
      q.getActiveCount(),
      q.getCompletedCount(),
      q.getFailedCount(),
      q.getDelayedCount(),
    ]);
    const lastDone  = await q.getJobs(['completed'], 0, 0);
    const lastJobAt = lastDone[0]?.finishedOn
      ? new Date(lastDone[0].finishedOn).toISOString()
      : null;
    return { ok: true, waiting, active, completed, failed, delayed, lastJobAt };
  } catch (err: any) {
    return { ok: false, error: err.message };
  } finally {
    await q.close();
  }
}

function isWorkerOk(
  heartbeat: { stale: boolean },
  stats: { ok: boolean; lastJobAt?: string | null },
): boolean {
  if (!stats.ok) return false;
  const recentJob = stats.lastJobAt
    ? (Date.now() - new Date(stats.lastJobAt).getTime()) < 300_000
    : false;
  return !heartbeat.stale || recentJob;
}

// ── GET /health/self ──────────────────────────────────────────────────────────
health.get('/self', (c) =>
  c.json({
    ok:        true,
    service:   'api',
    uptime:    process.uptime(),
    timestamp: new Date().toISOString(),
  })
);

// ── GET /health/:worker ───────────────────────────────────────────────────────
health.get('/:worker', async (c) => {
  const name      = c.req.param('worker');
  const queueName = WORKER_QUEUES[name];

  if (!queueName) {
    return c.json(
      { ok: false, error: `Unknown worker "${name}". Valid: ${Object.keys(WORKER_QUEUES).join(', ')}` },
      400,
    );
  }

  const [heartbeat, stats] = await Promise.all([
    getHeartbeat(name),
    getQueueStats(queueName),
  ]);

  const ok = isWorkerOk(heartbeat, stats);

  return c.json(
    { ok, worker: name, queue: queueName, heartbeat, stats, checkedAt: new Date().toISOString() },
    ok ? 200 : 503,
  );
});

// ── GET /health ── aggregate ──────────────────────────────────────────────────
health.get('/', async (c) => {
  const workerResults = await Promise.all(
    Object.entries(WORKER_QUEUES).map(async ([name, queueName]) => {
      const [heartbeat, stats] = await Promise.all([
        getHeartbeat(name),
        getQueueStats(queueName),
      ]);
      return {
        worker:    name,
        queue:     queueName,
        ok:        isWorkerOk(heartbeat, stats),
        heartbeat,
        stats,
      };
    })
  );

  const allOk = workerResults.every((w) => w.ok);

  return c.json(
    {
      ok:      allOk,
      status:  allOk ? 'ok' : 'degraded',
      self:    { ok: true, service: 'api', uptime: process.uptime() },
      workers: workerResults,
      checkedAt: new Date().toISOString(),
    },
    allOk ? 200 : 503,
  );
});

export { health as healthRouter };