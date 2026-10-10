'use strict';
require('dotenv/config');

/**
 * Upload batch job + Registry service (single file, CommonJS)
 * ===========================================================
 * Exports: handleBatch  (BullMQ job "upload-batch", used by src/index.js)
 *          + the registry functions (same API as lib/registry.js)
 *
 * NOTE: must stay CommonJS. Do NOT mix `import` and `require`, and do NOT use
 * `export default` — index.js does `const { handleBatch } = require(...)`,
 * which returns `undefined` for an ES-module default export.
 *
 * Registry: three-tier lookup for the video-ID set (Redis -> Postgres -> YouTube API).
 * Filename duplicate checks go to Postgres (plus the Redis orphan queue).
 * Orphaned uploads (on YouTube, Postgres write failed) are parked in the Redis
 * hash `yt:orphaned_uploads` and replayed by recoverOrphans() at the start of
 * every job and every worker startup.
 */

const fs          = require('fs');
const path        = require('path');
const { google }  = require('googleapis');
const { getRedis } = require('../lib/redis');
const { db }       = require('../lib/db');
const { ytAuth }   = require('../lib/ytAuth');
const { Logger }   = require('../lib/logger');
const { uploadVideo } = require('./uploadSingle');
const {
  incrementQuota, getQuotaUsed, isQuotaExhausted, QUOTA_DAILY_LIMIT, quotaResetsAt,
} = require('../lib/quota');

// ── Config ────────────────────────────────────────────────────────────────

const REGISTRY_CACHE_TTL_SECS    = Number(process.env.REGISTRY_CACHE_TTL_SECS    ?? 3600);
const REGISTRY_PG_SYNC_MAX_AGE_H = Number(process.env.REGISTRY_PG_SYNC_MAX_AGE_H ?? 24);
const PLAYLIST_ID_ENV            = process.env.YT_UPLOADS_PLAYLIST_ID ?? null;
const ORPHAN_KEY                 = 'yt:orphaned_uploads';

const redisKeyFor = (playlistId) => `yt:registry:${playlistId}`;

// ── Tier 1: Redis ────────────────────────────────────────────────────────

async function redisLoad(playlistId) {
  const raw = await getRedis().get(redisKeyFor(playlistId));
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    // Corrupt cache entry — treat as a miss rather than throwing.
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

    if (!raw) return; // nothing cached — next syncRegistry() rebuilds from PG/YouTube

    const ids = JSON.parse(raw);
    if (!ids.includes(videoId)) {
      ids.push(videoId);
      await redis.setex(key, REGISTRY_CACHE_TTL_SECS, JSON.stringify(ids));
    }
  } catch (err) {
    // Cache-write failures must never fail an upload job.
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

  // De-dupe by videoId: a repeated ID inside one statement makes Postgres
  // throw "ON CONFLICT DO UPDATE command cannot affect row a second time".
  const unique = [...new Map(normalised.map((i) => [i.videoId, i])).values()];

  const ids       = unique.map((i) => i.videoId);
  const titles    = unique.map((i) => i.title ?? null);
  const filenames = unique.map((i) => i.filename ?? null);

  await db.query(
    `INSERT INTO yt_registry (video_id, playlist_id, title, filename, synced_at)
     SELECT unnest($1::text[]), $2::text, unnest($3::text[]), unnest($4::text[]), NOW()
     ON CONFLICT (video_id) DO UPDATE
       SET synced_at = NOW(),
           title     = COALESCE(EXCLUDED.title, yt_registry.title),
           filename  = COALESCE(EXCLUDED.filename, yt_registry.filename)`,
    [ids, playlistId, titles, filenames],
  );
}

/**
 * Durable record of a completed upload: `videos` + `yt_registry` in ONE
 * transaction. Idempotent (ON CONFLICT), so it is safe to replay.
 */
async function persistUpload({ videoId, url, title, filename, groupedName, playlistId }) {
  const client = await db.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `INSERT INTO videos (yt_url, yt_video_id, title, grouped_name)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (yt_video_id) DO NOTHING`,
      [url, videoId, title || filename, groupedName],
    );

    await client.query(
      `INSERT INTO yt_registry (video_id, playlist_id, title, filename, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (video_id) DO UPDATE
         SET synced_at = NOW(),
             title     = EXCLUDED.title,
             filename  = EXCLUDED.filename`,
      [videoId, playlistId, title || filename, filename],
    );

    await client.query('COMMIT');
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch { /* connection may be gone */ }
    throw e;
  } finally {
    client.release();
  }
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

// ── Orphaned uploads (uploaded to YouTube, Postgres write failed) ─────────

/** Park an upload record in Redis. Returns true if it was stored. */
async function queueOrphan(record) {
  try {
    await getRedis().hset(ORPHAN_KEY, record.videoId, JSON.stringify(record));
    return true;
  } catch (err) {
    // Last resort: both PG and Redis are down. Make it impossible to miss.
    console.error(
      `[registry] CRITICAL: could not queue orphan upload — recover manually: ${JSON.stringify(record)} (${err.message})`,
    );
    return false;
  }
}

async function listOrphans() {
  const all = await getRedis().hgetall(ORPHAN_KEY);
  const out = [];
  for (const [videoId, raw] of Object.entries(all)) {
    try {
      out.push(JSON.parse(raw));
    } catch {
      console.warn(`[registry] Corrupt orphan entry for ${videoId} — leaving in ${ORPHAN_KEY}`);
    }
  }
  return out;
}

/**
 * Replay parked uploads into Postgres. Call at worker startup and at the
 * start of every upload job, BEFORE any dedup check or upload.
 * Never throws: failures leave the entry queued for the next attempt.
 */
async function recoverOrphans(log) {
  let orphans;
  try {
    orphans = await listOrphans();
  } catch (err) {
    console.warn(`[registry] Could not read orphan queue: ${err.message}`);
    return 0;
  }
  if (!orphans.length) return 0;

  await log.warn(`[registry] ${orphans.length} orphaned upload(s) found — syncing to Postgres first`);

  let recovered = 0;
  for (const o of orphans) {
    try {
      await persistUpload(o);
      await getRedis().hdel(ORPHAN_KEY, o.videoId);
      await redisAppend(o.playlistId, o.videoId);
      recovered += 1;
      await log.info(`[registry] Recovered orphan ${o.videoId} (${o.filename})`);
    } catch (err) {
      await log.warn(`[registry] Orphan ${o.videoId} still not recoverable: ${err.message}`);
    }
  }
  return recovered;
}

async function orphanHasFilename(playlistId, filename) {
  try {
    const needle = String(filename).toLowerCase();
    const orphans = await listOrphans();
    return orphans.some(
      (o) => o.playlistId === playlistId && String(o.filename ?? '').toLowerCase() === needle,
    );
  } catch {
    return false;
  }
}

/**
 * All known filenames (lower-cased) for a playlist, incl. pending orphans.
 * One query up front lets the batch job skip already-uploaded files in memory
 * instead of hitting Postgres once per file.
 */
async function pgLoadFilenames(playlistId) {
  const { rows } = await db.query(
    'SELECT LOWER(filename) AS f FROM yt_registry WHERE playlist_id = $1 AND filename IS NOT NULL',
    [playlistId],
  );
  const set = new Set(rows.map((r) => r.f));
  try {
    for (const o of await listOrphans()) {
      if (o.playlistId === playlistId && o.filename) set.add(String(o.filename).toLowerCase());
    }
  } catch { /* orphan queue unreadable — PG set is still valid */ }
  return set;
}

/**
 * Filename-level duplicate check. Case-insensitive to stay consistent with
 * worker4's /duplicates endpoint, which groups on LOWER(filename).
 * Also consults the orphan queue so a file whose PG write is still pending
 * is not uploaded a second time.
 *
 * Callers MUST await this BEFORE uploading to YouTube.
 */
async function check_if_filename_exists_in_registry(playlistId, filename) {
  try {
    const { rows } = await db.query(
      'SELECT video_id FROM yt_registry WHERE playlist_id = $1 AND LOWER(filename) = LOWER($2)',
      [playlistId, filename],
    );
    if (rows.length > 0) return true;
    return await orphanHasFilename(playlistId, filename);
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
  let pageNum  = 0;
  let complete = true;

  do {
    if (await isQuotaExhausted()) {
      complete = false;
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

  if (complete) {
    await pgSetMeta(playlistId, allIds.length);
    await log.info(`[registry] job=${jobId} YouTube sync complete — ${allIds.length} videos`);
  } else {
    // Do NOT stamp last_full_sync on a partial sync — that would make PG look
    // fresh for 24h while videos are missing.
    await log.warn(`[registry] job=${jobId} partial sync (${allIds.length} videos) — NOT marking PG as fresh`);
  }
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
    const resetsAt = quotaResetsAt();
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
 * and readable.
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

// ── Batch job ─────────────────────────────────────────────────────────────

const VIDEO_EXT = new Set(['.mp4', '.mkv', '.mov', '.avi', '.flv', '.wmv']);

// Where videos live inside the container. Override with UPLOAD_DIR in .env.
const UPLOAD_BASE_DIR  = process.env.UPLOAD_DIR ?? '/media/grouped';
const SKIP_DIR_NAMES   = new Set(['skipped', 'uploaded']);   // legacy sub-dirs, never re-scan
const MAX_SCAN_DEPTH   = 6;

/** All video files under `dir` (recursive, sorted, hidden + legacy dirs skipped). */
function listVideoFiles(dir, depth = 0) {
  let entries;
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return []; }
  const out = [];
  for (const e of entries) {
    if (e.name.startsWith('.')) continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      if (depth < MAX_SCAN_DEPTH && !SKIP_DIR_NAMES.has(e.name)) out.push(...listVideoFiles(full, depth + 1));
    } else if (e.isFile() && VIDEO_EXT.has(path.extname(e.name).toLowerCase())) {
      out.push(full);
    }
  }
  return out.sort();
}

async function handleBatch(job) {
  const log = new Logger('worker3:batch', job);
  const { groupedDir, batchSize = 15, forceRegistrySync = false } = job.data;

  await log.info(`━━ Batch upload start ━━ dir=${groupedDir} batchSize=${batchSize}`);

  if (!process.env.YT_CLIENT_ID || !process.env.YT_REFRESH_TOKEN) {
    await log.error('Missing YT credentials in environment');
    throw new Error('Missing YT credentials');
  }

  // Don't depend on the job's folder: old jobs name folders that no longer exist.
  // Use the job's dir if it is still there, otherwise upload whatever is under the base dir.
  const pid = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';
  let scanDir = UPLOAD_BASE_DIR;
  if (groupedDir && fs.existsSync(groupedDir)) {
    scanDir = groupedDir;
  } else if (groupedDir) {
    await log.warn(`groupedDir not found: ${groupedDir} — scanning ${UPLOAD_BASE_DIR} instead`);
  }
  if (!fs.existsSync(scanDir)) {
    await log.error(`Upload dir not found: ${scanDir}`);
    throw new Error(`Upload dir not found: ${scanDir}`);
  }

  // Replay uploads that reached YouTube but not Postgres, BEFORE any dedup check.
  await recoverOrphans(log);

  // Drop already-uploaded files BEFORE applying batchSize. Files are not moved after
  // upload, so taking "the first N" would stall once those N are done.
  const knownFilenames = await pgLoadFilenames(pid);
  const allFiles   = listVideoFiles(scanDir);
  const pending    = allFiles.filter(f => !knownFilenames.has(path.basename(f).toLowerCase()));
  const videoFiles = pending.slice(0, batchSize);
  await log.info(`Scanned ${scanDir}: ${allFiles.length} video(s), ${allFiles.length - pending.length} already uploaded, ${pending.length} pending`);

  await log.info(`Videos to upload: ${videoFiles.length}`);
  if (!videoFiles.length) {
    return { results: [], skipped: allFiles.length - pending.length, uploaded: 0 };
  }

  const readable = videoFiles.filter(f => {
    try { fs.accessSync(f, fs.constants.R_OK); return true; } catch { return false; }
  });

  await log.info('Resolving registry (Redis → PG → YouTube API)...');
  let registry;
  try {
    registry = await syncRegistry(job.id, log, { forceSync: forceRegistrySync });
  } catch (err) {
    if (err.message === 'QUOTA_EXHAUSTED') {
      return { results: [], skipped: videoFiles.length, uploaded: 0, abortReason: 'quota_exhausted' };
    }
    throw err;
  }

  const results  = [];
  let uploaded   = 0;
  let skipped    = 0;

  for (const file of readable) {
    if (await isQuotaExhausted()) {
      await log.error(`Quota exhausted mid-batch (${await getQuotaUsed()}/${QUOTA_DAILY_LIMIT}) — stopping`);
      results.push({ file, status: 'skipped', reason: 'quota_exhausted' });
      skipped++;
      continue;
    }

    try {
      const filename = path.basename(file);

      // Check the registry BEFORE uploading. Checking afterwards (the old
      // behaviour) meant the file had already been streamed to YouTube and
      // written into yt_registry by the time we found out it was a
      // duplicate — burning quota and creating a second video on the
      // channel for something we were about to skip anyway.
      const filenameExists = await check_if_filename_exists_in_registry(pid, filename);

      if (filenameExists) {
        await log.warn(`Filename ${filename} already exists in registry — skipping upload (no YouTube call made)`);
        // move and tag the file as skipped to prevent re-processing in future batches
        const skippedDir = path.join(path.dirname(file), 'skipped');
        if (!fs.existsSync(skippedDir)) {
          // DO nothing now
//          fs.mkdirSync(skippedDir);
        }
        const destPath = path.join(skippedDir, `${path.basename(file, path.extname(file))}_skipped${path.extname(file)}`);
          // DO not rename now 
        //      fs.renameSync(file, destPath);
        console.log(`Moved ${file} → ${destPath}`);

        results.push({ file, status: 'skipped', reason: 'filename_exists' });
        skipped++;
        continue;
      }

      const r = await uploadVideo(file, path.basename(file, path.extname(file)), log);
      const videoId = r.videoId;

      // 1. In-memory registry
      registry.add(videoId);

      // 2. PG already written by uploadVideo — but ensure filename is stored.
      //    If uploadVideo parked it as an orphan (PG down) skip this; recoverOrphans() replays it.
      if (!r.orphaned) {
        await pgUpsertBatch(pid, [{ videoId, title: r.title ?? filename, filename }]);
      } else {
        await log.warn(`${filename} (${videoId}) is on YouTube but parked in the orphan queue — will sync to Postgres later`);
      }

      // move file to "uploaded" subdir
      // tag filename_<videoId> with videoId to prevent collisions and for easier tracing
      // add videoId to filename to prevent collisions and for easier tracing
      // hide it, so app wont process it again if the batch is re-run

        try{

          const uploadedDir = path.join(path.dirname(file), 'uploaded');
        if (!fs.existsSync(uploadedDir)) {
          // DO nothing now
//// Do no
//           fs.mkdirSync(uploadedDir);
        }
        const destPath = path.join(uploadedDir, `${path.basename(file, path.extname(file))}_${videoId}${path.extname(file)}`);
     
        //  fs.renameSync(file, destPath);
        console.log(`Moved ${file} → ${destPath}`);

        //hide the file so that it is not processed again if the batch is re-run
        //fs.chmodSync(destPath, 0o000);
        console.log(`Hid ${destPath}`);
        }catch(err){
          await log.error(`Failed to move file ${file} after upload: ${err.message}`);
          return { results: [], skipped: videoFiles.length, uploaded: 0, abortReason: 'file_move_failed' };

        }

        


      // 3. Incremental Redis update — delegated to the registry service so
      //    there's one place that knows the cache key shape and TTL.
      if (!r.orphaned) await redisAppend(pid, videoId);

      results.push({ file, ...r, status: 'ok' });
      uploaded++;

        } catch (err) {
    console.error(err);
    console.error(err.stack);
    results.push({ file, status: 'error', error: err.message });
      /*if (
        err.message.includes('exceeded the number of videos') ||
        err.message.includes('The request cannot be completed because you have exceeded')
      ) {
        await log.error('Quota exhausted during upload — stopping batch');
        break;
      }

      */

          // We now will check why its catch but we will need to shut it down
          // for any error app need to shutdown, swarm will restart and new process will be sent
          if (
            err.message.includes('exceeded the number of videos') ||
            err.message.includes('The request cannot be completed because you have exceeded') ||
            err.message.includes('Quota exceeded')
          ) {
            await log.error('Quota exhausted during upload — stopping batch');
            // shutdown the process so that swarm will restart it and new process will be sent
            process.exit(1);
          } else {
            await log.error(`Error uploading ${file}: ${err.message}`);
          }


    }
  }

  await log.info(`Batch complete: ${uploaded} uploaded, ${skipped} skipped, ${results.filter(r => r.status === 'error').length} errors`);
  return { results, uploaded, skipped };
}

module.exports = {
  handleBatch,
  syncRegistry,
  pgUpsertBatch,
  pgSetMeta,
  pgGetMeta,
  persistUpload,
  queueOrphan,
  recoverOrphans,
  pgLoadFilenames,
  resolvePlaylistId,
  check_if_filename_exists_in_registry,
  check_if_dir_is_accessible,
  redisAppend,
};