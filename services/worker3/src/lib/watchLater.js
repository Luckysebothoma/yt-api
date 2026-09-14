'use strict';
const { getRedis } = require('./redis');
const { db }       = require('./db');

const WL_KEY = 'yt:watchlater';

/**
 * Enqueue a video into Watch-Later.
 * reason: 'quota_exhausted' | 'stream_unavailable' | 'api_error'
 */
async function enqueue(videoId, { title, filename, playlistId, reason = 'quota_exhausted' } = {}) {
  const score = Date.now();

  // Redis sorted-set — fast lookup and ordering
  await getRedis().zadd(WL_KEY, score, videoId);

  // PG — survives Redis flush / restart
  await db.query(
    `INSERT INTO yt_watch_later (video_id, title, filename, playlist_id, reason, queued_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (video_id) DO UPDATE
       SET reason      = EXCLUDED.reason,
           retry_count = yt_watch_later.retry_count + 1,
           last_retry  = NOW(),
           resolved    = FALSE`,
    [videoId, title ?? null, filename ?? null, playlistId ?? null, reason]
  );

  console.log(`[watchLater] enqueued ${videoId} reason=${reason}`);
}

/**
 * List all unresolved watch-later items, ordered by queued_at ascending.
 * @param {number} limit
 * @param {number} offset
 */
async function list(limit = 50, offset = 0) {
  const { rows } = await db.query(
    `SELECT id, video_id, title, filename, playlist_id, reason,
            queued_at, retry_count, last_retry
     FROM yt_watch_later
     WHERE resolved = FALSE
     ORDER BY queued_at ASC
     LIMIT $1 OFFSET $2`,
    [limit, offset]
  );
  return rows;
}

/**
 * Count unresolved items.
 */
async function count() {
  const { rows } = await db.query(
    `SELECT COUNT(*)::int AS n FROM yt_watch_later WHERE resolved = FALSE`
  );
  return rows[0].n;
}

/**
 * Mark a single video as resolved.
 */
async function resolve(videoId) {
  await getRedis().zrem(WL_KEY, videoId);
  await db.query(
    `UPDATE yt_watch_later
     SET resolved = TRUE, resolved_at = NOW()
     WHERE video_id = $1`,
    [videoId]
  );
}

/**
 * Remove a video entirely from the queue (user-initiated delete).
 */
async function remove(videoId) {
  await getRedis().zrem(WL_KEY, videoId);
  await db.query(`DELETE FROM yt_watch_later WHERE video_id = $1`, [videoId]);
}

/**
 * Return all unresolved video IDs from Redis (fast path).
 */
async function listFromRedis() {
  return getRedis().zrange(WL_KEY, 0, -1);
}

module.exports = { enqueue, list, count, resolve, remove, listFromRedis };
