// lib/redis.ts
import 'dotenv/config';
import IORedis from 'ioredis';

const rawHost = process.env.REDIS_HOST;
const rawPort = process.env.REDIS_PORT;
const host    = rawHost?.trim();
const port    = Number(rawPort ?? 6379);

console.log('[redis] ENV CHECK →', {
  REDIS_HOST: rawHost ?? '❌ NOT SET',
  REDIS_PORT: rawPort ?? '⚠️ DEFAULT (6379)',
});

if (!host) {
  console.error('[redis] FATAL: REDIS_HOST missing');
  process.exit(1);
}
if (isNaN(port) || port < 1 || port > 65535) {
  console.error(`[redis] FATAL: REDIS_PORT invalid → "${rawPort}"`);
  process.exit(1);
}
if (host === 'localhost' || host === '127.0.0.1') {
  console.warn('[redis] WARNING: localhost will fail inside Docker');
}
// Add this export — BullMQ will use its own ioredis with these options
export const bullMQConnectionOptions = {
  host,
  port,
  family: 4,
  maxRetriesPerRequest: null,
  enableReadyCheck: false, // BullMQ recommends false
} as const;

export const redis = new IORedis({
  host,
  port,
  family: 4,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  lazyConnect: false,
  retryStrategy(times: number) {
    const delay = Math.min(times * 1000, 15000);
    console.warn(`[redis] 🔁 Retry #${times} → reconnecting in ${delay}ms (${host}:${port})`);
    return delay;
  },
});

// ─── Aliases ──────────────────────────────────────────────────────────────────
// health.ts uses `redisConnection`, heartbeat.ts uses `redisConnectionWithRetry`
// Both point to the same shared instance — one connection, no duplication.
export const redisConnection         = redis;
export const redisConnectionWithRetry = redis;

// ─── Startup gate ─────────────────────────────────────────────────────────────
export async function ensureRedisReady(timeoutMs = 15000) {
  const timer = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Redis connect timeout')), timeoutMs)
  );
  try {
    await Promise.race([redis.connect(), timer]);
    await redis.ping();
    console.log(`[redis] ✅ Connected → ${host}:${port}`);
  } catch (err: any) {
    console.error('[redis] ❌ FATAL: cannot connect to Redis:', err.message);
    process.exit(1);
  }
}

// ─── Runtime visibility ───────────────────────────────────────────────────────
redis.on('error',       (err) => console.error('[redis] ⚠️  runtime error:', err.message));
redis.on('reconnecting',()    => console.warn ('[redis] 🔁 reconnecting...'));
redis.on('end',         ()    => console.error('[redis] 🔌 connection closed'));