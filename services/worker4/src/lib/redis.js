'use strict';
require('dotenv/config');

let _redis = null;
function getRedis() {
  if (!_redis) {
    const Redis = require('ioredis');
    _redis = new Redis({
      host: process.env.REDIS_HOST ?? 'redis',
      port: Number(process.env.REDIS_PORT ?? 6379),
    });
    _redis.on('error', (err) => console.error('[redis]', err.message));
  }
  return _redis;
}

module.exports = { getRedis };
