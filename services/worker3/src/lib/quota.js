'use strict';
const { getRedis } = require('./redis');

const QUOTA_DAILY_LIMIT = Number(process.env.QUOTA_DAILY_LIMIT ?? 10000);

// YouTube Data API quota resets at midnight Pacific time (07:00 UTC during
// PDT, 08:00 UTC during PST). The counter MUST be keyed by the Pacific date,
// otherwise it resets 7-8h before YouTube does.
const QUOTA_TZ = 'America/Los_Angeles';

const ptFmt = new Intl.DateTimeFormat('en-CA', {
  timeZone: QUOTA_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
}); // → YYYY-MM-DD
const ptDay = (d = new Date()) => ptFmt.format(d);

const quotaKey = () => `yt:quota:${ptDay()}`;

async function getQuotaUsed() {
  const val = await getRedis().get(quotaKey());
  return val ? parseInt(val, 10) : 0;
}

async function incrementQuota(units = 1) {
  const r   = getRedis();
  const key = quotaKey();
  await r.incrby(key, units);
  await r.expire(key, 48 * 3600); // key is date-scoped, so a generous TTL is safe
}

async function isQuotaExhausted() {
  return (await getQuotaUsed()) >= QUOTA_DAILY_LIMIT;
}

// Next Pacific midnight (DST-aware). Returns a Date.
function quotaResetsAt() {
  const today = ptDay();
  const t = new Date();
  t.setUTCSeconds(0, 0);
  for (let i = 0; i < 26 * 60; i++) {
    t.setUTCMinutes(t.getUTCMinutes() + 1);
    if (ptDay(t) !== today) return t;
  }
  return t;
}

module.exports = { getQuotaUsed, incrementQuota, isQuotaExhausted, QUOTA_DAILY_LIMIT, quotaResetsAt };