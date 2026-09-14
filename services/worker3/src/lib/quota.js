'use strict';
const { getRedis } = require('./redis');

const QUOTA_DAILY_LIMIT = Number(process.env.QUOTA_DAILY_LIMIT ?? 10000);


function quotaKey() {
  return `yt:quota:${new Date().toISOString().slice(0, 10)}`;
}

async function getQuotaUsed() {
  const val = await getRedis().get(quotaKey());
  return val ? parseInt(val, 10) : 0;
}

async function incrementQuota(units = 1) {
  const r   = getRedis();
  const key = quotaKey();
  await r.incrby(key, units);
  const now      = new Date();
  const midnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 1));
  await r.expire(key, Math.floor((midnight - now) / 1000));
}

async function isQuotaExhausted() {
  return (await getQuotaUsed()) >= QUOTA_DAILY_LIMIT;
}

// Returns epoch-ms when quota resets (next day 08:00 UTC)
function quotaResetsAt() {
  const d = new Date();
  d.setUTCHours(8, 0, 0, 0);
  if (d <= new Date()) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}

module.exports = { getQuotaUsed, incrementQuota, isQuotaExhausted, QUOTA_DAILY_LIMIT, quotaResetsAt };
