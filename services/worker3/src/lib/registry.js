'use strict';
require('dotenv/config');

/**
 * Registry Service
 * ================
 * Single source of truth for "has this video/file already been uploaded?".
 *
 * Backed by a three-tier lookup for the full video-ID set (cheap → expensive):
 *   1. Redis        — hot cache, short TTL, near-zero cost
 *   2. Postgres      — durable, always available, no YouTube quota cost
 *   3. YouTube API   — authoritative, but burns daily quota, used to refresh PG
 *
 * Filename-level duplicate checks (used to stop a re-upload of the same file
 * before it happens) always go straight to Postgres — it's indexed, cheap,
 * and needs to be correct on every call, not eventually-consistent like the
 * Redis-cached video-ID set.
 */

const fs          = require('fs');
const { google }  = require('googleapis');
const { getRedis } = require('./redis');
const { db }       = require('./db');
const { ytAuth }   = require('./ytAuth');
const {
  incrementQuota, getQuotaUsed, isQuotaExhausted, QUOTA_DAILY_LIMIT,
} = require('./quota');

// ── Config ────────────────────────────────────────────────────────────────

const REGISTRY_CACHE_TTL_SECS    = Number(process.env.REGISTRY_CACHE_TTL_SECS    ?? 3600);
const REGISTRY_PG_SYNC_MAX_AGE_H = Number(process.env.REGISTRY_PG_SYNC_MAX_AGE_H ?? 24);
const PLAYLIST_ID_ENV            = process.env.YT_UPLOADS_PLAYLIST_ID ?? null;

const redisKeyFor = (playlistId) => `yt:registry:${playlistId}`;

// ── Tier 1: Redis ────────────────────────────────────────────────────────

async function redisLoad(playlistId) {
  const raw = await getRedis().get(redisKeyFor(playlistId));
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    // Corrupt cache entry — treat as a miss rather than throwing, so a bad
    // write can't take down every upload job until the TTL expires.
    return null;
  }
}

async function redisWarm(playlistId, videoIds) {
  await getRedis().setex(
    redisKeyFor(playlistId),
    REGISTRY_CACHE_TTL_SECS,
    JSON.stringify(videoIds),
  );
}

/** Add a single video ID to the warm cache without a full reload/rewrite. */
async function redisAppend(playlistId, videoId) {
  try {
    const redis = getRedis();
    const key   = redisKeyFor(playlistId);
    const raw   = await redis.get(key);

    if (!raw) {
      // Nothing cached yet — nothing to append to, next syncRegistry() call
      // will build the cache from Postgres/YouTube and pick this row up.
      return;
    }

    const ids = JSON.parse(raw);
    if (!ids.includes(videoId)) {
      ids.push(videoId);
      await redis.setex(key, REGISTRY_CACHE_TTL_SECS, JSON.stringify(ids));
    }
  } catch (err) {
    // Cache-write failures must never fail an upload job — Postgres is the
    // durable source of truth, Redis is only an accelerator.
    console.warn(`[registry] Redis append failed for ${videoId}: ${err.message}`);
  }
}

// ── Tier 2: Postgres ─────────────────────────────────────────────────────

async function pgLoad(playlistId) {
  const { rows } = await db.query(
    'SELECT video_id FROM yt_registry WHERE playlist_id = $1',
    [playlistId],
  );
  return rows.map((r) => r.video_id);
}

async function pgUpsertBatch(playlistId, items) {
  // items: array of { videoId, title?, filename? }  OR plain string IDs
  const normalised = items.map((i) =>
    (typeof i === 'string' ? { videoId: i, title: null, filename: null } : i));
  if (!normalised.length) return;

  const ids       = normalised.map((i) => i.videoId);
  const titles    = normalised.map((i) => i.title ?? null);
  const filenames = normalised.map((i) => i.filename ?? null);

  await db.query(
    `INSERT INTO yt_registry (video_id, playlist_id, title, filename, synced_at)
     SELECT unnest($1::text[]), $2, unnest($3::text[]), unnest($4::text[]), NOW()
     ON CONFLICT (video_id) DO UPDATE
       SET synced_at = NOW(),
           title     = COALESCE(EXCLUDED.title, yt_registry.title),
           filename  = COALESCE(EXCLUDED.filename, yt_registry.filename)`,
    [ids, playlistId, titles, filenames],
  );
}

async function pgGetMeta(playlistId) {
  const { rows } = await db.query(
    'SELECT last_full_sync, total_videos FROM yt_registry_meta WHERE playlist_id = $1',
    [playlistId],
  );
  return rows[0] ?? null;
}

async function pgSetMeta(playlistId, total) {
  await db.query(
    `INSERT INTO yt_registry_meta (playlist_id, last_full_sync, total_videos, updated_at)
     VALUES ($1, NOW(), $2, NOW())
     ON CONFLICT (playlist_id) DO UPDATE
       SET last_full_sync = NOW(), total_videos = $2, updated_at = NOW()`,
    [playlistId, total],
  );
}

async function pgNeedsRefresh(playlistId) {
  const meta = await pgGetMeta(playlistId);
  if (!meta || meta.total_videos === 0) return true;
  const ageHours = (Date.now() - new Date(meta.last_full_sync).getTime()) / 3_600_000;
  return ageHours > REGISTRY_PG_SYNC_MAX_AGE_H;
}

/**
 * Filename-level duplicate check. Case-insensitive to stay consistent with
 * worker4's /duplicates endpoint, which groups on LOWER(filename) — using
 * different casing rules in the two services would let a duplicate slip
 * past one and get flagged by the other.
 *
 * Callers MUST await this BEFORE uploading to YouTube. Checking afterwards
 * means the file has already been streamed and billed against quota by the
 * time you find out it should have been skipped.
 */
async function check_if_filename_exists_in_registry(playlistId, filename) {
  try {
    const { rows } = await db.query(
      'SELECT video_id FROM yt_registry WHERE playlist_id = $1 AND LOWER(filename) = LOWER($2)',
      [playlistId, filename],
    );
    return rows.length > 0;
  } catch (err) {
    console.error('[registry] Error checking filename existence:', err);
    throw err;
  }
}

// ── Tier 3: YouTube API ──────────────────────────────────────────────────

async function resolvePlaylistId(log) {
  if (PLAYLIST_ID_ENV) return PLAYLIST_ID_ENV;

  const cached = await getRedis().get('yt:uploads_playlist_id');
  if (cached) return cached;

  const yt  = google.youtube({ version: 'v3', auth: ytAuth() });
  const res = await yt.channels.list({ part: ['contentDetails'], mine: true });
  await incrementQuota(1);

  const id = res.data.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  if (!id) throw new Error('Could not resolve uploads playlist ID from YouTube API');

  await getRedis().setex('yt:uploads_playlist_id', 86400, id);
  if (log) await log.info(`[registry] Resolved uploads playlist: ${id}`);
  return id;
}

async function syncFromYouTube(playlistId, jobId, log) {
  await log.info(`[registry] job=${jobId} tier=YouTube — starting paginated sync`);
  const yt     = google.youtube({ version: 'v3', auth: ytAuth() });
  const allIds = [];
  let pageToken;
  let pageNum = 0;

  do {
    if (await isQuotaExhausted()) {
      await log.warn(`[registry] job=${jobId} quota hit mid-sync after page ${pageNum}`);
      break;
    }

    const res = await yt.playlistItems.list({
      part: ['contentDetails', 'snippet'], playlistId, maxResults: 50, pageToken,
    });
    await incrementQuota(1);
    pageNum += 1;

    const items = (res.data.items ?? [])
      .map((i) => ({
        videoId:  i.contentDetails?.videoId,
        title:    i.snippet?.title ?? null,
        filename: null,
      }))
      .filter((i) => i.videoId);

    allIds.push(...items.map((i) => i.videoId));
    await pgUpsertBatch(playlistId, items);

    pageToken = res.data.nextPageToken ?? undefined;
    console.log(`[registry] job=${jobId} page ${pageNum} synced — ${items.length} items, total: ${allIds.length}`);
  } while (pageToken);

  await pgSetMeta(playlistId, allIds.length);
  await log.info(`[registry] job=${jobId} YouTube sync complete — ${allIds.length} videos`);
  return allIds;
}

/**
 * Resolve the full set of known video IDs for the uploads playlist, using
 * the cheapest tier that has fresh-enough data: Redis → Postgres → YouTube.
 * Returns a Set<string> of video IDs.
 */
async function syncRegistry(jobId, log, { forceSync = false } = {}) {
  const quotaUsed = await getQuotaUsed();
  const quotaLeft = Math.max(0, QUOTA_DAILY_LIMIT - quotaUsed);
  await log.info(`[registry] job=${jobId} quota=${quotaUsed}/${QUOTA_DAILY_LIMIT} left=${quotaLeft}`);

  const playlistId = await resolvePlaylistId(log);

  // Tier 1: Redis
  if (!forceSync) {
    const redisIds = await redisLoad(playlistId);
    if (redisIds) {
      await log.info(`[registry] job=${jobId} tier=Redis hit — ${redisIds.length} videos`);
      return new Set(redisIds);
    }
    await log.info(`[registry] job=${jobId} tier=Redis miss — checking PG`);
  }

  // Tier 2: Postgres
  const pgIds       = await pgLoad(playlistId);
  const needRefresh = forceSync || await pgNeedsRefresh(playlistId);

  if (pgIds.length > 0 && !needRefresh) {
    await log.info(`[registry] job=${jobId} tier=PG hit — ${pgIds.length} videos`);
    await redisWarm(playlistId, pgIds);
    return new Set(pgIds);
  }

  await log.info(pgIds.length > 0
    ? `[registry] job=${jobId} tier=PG stale — refreshing from YouTube`
    : `[registry] job=${jobId} tier=PG miss — syncing from YouTube`);

  // Tier 3: YouTube API
  if (quotaUsed >= QUOTA_DAILY_LIMIT) {
    if (pgIds.length > 0) {
      await log.warn(`[registry] job=${jobId} quota exhausted — falling back to stale PG`);
      await redisWarm(playlistId, pgIds);
      return new Set(pgIds);
    }
    const resetsAt = new Date();
    resetsAt.setUTCHours(8, 0, 0, 0);
    if (resetsAt <= new Date()) resetsAt.setUTCDate(resetsAt.getUTCDate() + 1);
    await log.error(`[registry] job=${jobId} quota exhausted and PG empty — resets ${resetsAt.toISOString()}`);
    throw new Error('QUOTA_EXHAUSTED');
  }

  const ytIds  = await syncFromYouTube(playlistId, jobId, log);
  const merged = [...new Set([...pgIds, ...ytIds])];
  await redisWarm(playlistId, merged);
  await log.info(`[registry] job=${jobId} registry ready — ${merged.length} total videos`);
  return new Set(merged);
}

// ── Filesystem guard ─────────────────────────────────────────────────────

/**
 * Confirms the media directory the workers read from is actually mounted
 * and readable. Called once at startup so a missing bind-mount fails fast
 * instead of surfacing as a confusing mid-batch ENOENT.
 */
async function check_if_dir_is_accessible(dir) {
  try {
    console.log(`[registry] Checking if directory is accessible: ${dir}`);
    await fs.promises.access(dir, fs.constants.R_OK);
    console.log(`[registry] Directory is accessible: ${dir}`);
    return true;
  } catch {
    console.log(`[registry] Directory is not accessible: ${dir}`);
    return false;
  }
}

module.exports = {
  syncRegistry,
  pgUpsertBatch,
  pgSetMeta,
  pgGetMeta,
  resolvePlaylistId,
  check_if_filename_exists_in_registry,
  check_if_dir_is_accessible,
  redisAppend,
};
