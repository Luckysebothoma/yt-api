'use strict';
const { getRedis } = require('./redis');

const QUOTA_DAILY_LIMIT = Number(process.env.QUOTA_DAILY_LIMIT ?? 10000);

// Costs for operations this service performs
const QUOTA_COSTS = {
  'channels.list':       1,
  'playlistItems.list':  1,
  'videos.list':         1,
  'videos.update':       50,
};

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
  const midnight = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 1
  ));
  await r.expire(key, Math.floor((midnight - now) / 1000));
}

async function isQuotaExhausted() {
  return (await getQuotaUsed()) >= QUOTA_DAILY_LIMIT;
}

async function wouldExceedQuota(units) {
  return (await getQuotaUsed()) + units > QUOTA_DAILY_LIMIT;
}

function quotaResetsAt() {
  const d = new Date();
  d.setUTCHours(8, 0, 0, 0);
  if (d <= new Date()) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}

function quotaSummary(used) {
  return {
    used,
    limit:      QUOTA_DAILY_LIMIT,
    remaining:  Math.max(0, QUOTA_DAILY_LIMIT - used),
    exhausted:  used >= QUOTA_DAILY_LIMIT,
    resets_at:  quotaResetsAt().toISOString(),
    costs:      QUOTA_COSTS,
  };
}

module.exports = {
  getQuotaUsed, incrementQuota, isQuotaExhausted, wouldExceedQuota,
  quotaResetsAt, quotaSummary, QUOTA_DAILY_LIMIT, QUOTA_COSTS,
};
