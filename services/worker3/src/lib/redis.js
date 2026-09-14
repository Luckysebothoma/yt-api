'use strict';
require('dotenv/config');

const redisConnection = {
  host: process.env.REDIS_HOST ?? 'redis',
  port: Number(process.env.REDIS_PORT ?? 6379),
};


// redisConnectionWithRetry
const redisConnectionWithRetry = {
  ...redisConnection,
  retryStrategy(times) {
    const delay = Math.min(times * 100, 2000);
    console.warn(`[redis] Connection failed. Retrying in ${delay}ms... (attempt ${times})`);
    return delay;
  },
};



let _redis = null;
function getRedis() {
  if (!_redis) {
    const Redis = require('ioredis');
    _redis = new Redis(redisConnection);
    _redis.on('error', (err) => console.error('[redis]', err.message));
  }
  return _redis;
}

module.exports = { redisConnection, redisConnectionWithRetry, getRedis };
