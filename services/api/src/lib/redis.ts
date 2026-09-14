import 'dotenv/config';

/**
 * 🔍 Read & normalize environment variables
 */
const rawHost = process.env.REDIS_HOST;
const rawPort = process.env.REDIS_PORT;

const host = rawHost?.trim();
const port = Number(rawPort ?? 6379);

/**
 * 🧪 Environment Debug (VERY IMPORTANT)
 */
console.log('[redis] ENV CHECK →', {
  REDIS_HOST: rawHost ?? '❌ NOT SET',
  REDIS_PORT: rawPort ?? '⚠️ DEFAULT (6379)',
});

/**
 * 🚨 Hard validation
 */
if (!host) {
  console.error('[redis] FATAL: REDIS_HOST is NOT set or empty');
  process.exit(1);
}

if (isNaN(port) || port < 1 || port > 65535) {
  console.error(`[redis] FATAL: REDIS_PORT is INVALID → "${rawPort}"`);
  process.exit(1);
}

/**
 * ⚠️ Dangerous configs detection (DevOps safety)
 */
if (host === 'localhost' || host === '127.0.0.1') {
  console.warn(
    '[redis] WARNING: REDIS_HOST is set to localhost/127.0.0.1 — this will FAIL inside Docker containers'
  );
}

/**
 * 🚀 Base connection (simple usage)
 */
export const redisConnection = {
  host,
  port,
  family: 4, // 🔥 Force IPv4 (prevents Node resolving ::1 issues)
};

/**
 * 🔁 Production-ready BullMQ connection
 */
export const redisConnectionWithRetry = {
  host,
  port,
  family: 4, // 🔥 Critical for Docker environments

  maxRetriesPerRequest: null, // ✅ Required by BullMQ
  enableReadyCheck: true,     // ✅ Wait until Redis is actually ready
  lazyConnect: true,          // ✅ Prevent crash if Redis not ready yet

  retryStrategy: (times: number) => {
    const delay = Math.min(times * 500, 5000);
    console.warn(
      `[redis] 🔁 Retry #${times} → reconnecting in ${delay}ms (${host}:${port})`
    );
    return delay;
  },

  reconnectOnError: (err: Error) => {
    console.error(`[redis] ⚠️ reconnectOnError → ${err.message}`);
    return true;
  },
};

/**
 * 📡 Final connection intent log
 */
console.log(
  `[redis] 🚀 Attempting connection → ${host}:${port} (IPv4 enforced)`
);