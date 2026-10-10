'use strict';
require('dotenv/config');

const redisConnection = {
  host: process.env.REDIS_HOST ?? 'redis',
  port: Number(process.env.REDIS_PORT ?? 6379),
  connectTimeout: Number(process.env.REDIS_CONNECT_TIMEOUT_MS ?? 20000),
  keepAlive: 10000,
};

// Used by BullMQ (src/index.js). Retries forever with capped backoff instead of
// surfacing a bare "connect ETIMEDOUT". maxRetriesPerRequest MUST be null for
// BullMQ workers (blocking commands).
const redisConnectionWithRetry = {
  ...redisConnection,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  retryStrategy(times) {
    const delay = Math.min(times * 200, 5000);
    console.warn(`[redis] Connection failed. Retrying in ${delay}ms... (attempt ${times})`);
    return delay;
  },
};

let _redis = null;
function getRedis() {
  if (!_redis) {
    const Redis = require('ioredis');
    _redis = new Redis({
      ...redisConnection,
      retryStrategy: redisConnectionWithRetry.retryStrategy,
    });
    _redis.on('error', (err) => console.error('[redis]', err.message));
  }
  return _redis;
}

module.exports = { redisConnection, redisConnectionWithRetry, getRedis };