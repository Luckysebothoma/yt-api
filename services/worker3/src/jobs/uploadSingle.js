'use strict';
require('dotenv/config');
const fs         = require('fs');
const path       = require('path');
const { google } = require('googleapis');
const { ytAuth } = require('../lib/ytAuth');
const { incrementQuota } = require('../lib/quota');
const {
  check_if_filename_exists_in_registry,
  persistUpload,
  queueOrphan,
  recoverOrphans,
} = require('../lib/registry');

const DB_WRITE_ATTEMPTS = Number(process.env.DB_WRITE_ATTEMPTS ?? 3);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Upload one file to YouTube and record it.
 *
 * Once YouTube has accepted the video this function NEVER throws because of a
 * bookkeeping failure — throwing would make the caller/queue retry and create
 * a duplicate video. Instead the DB write is retried, and if it still fails
 * the record is parked in Redis (yt:orphaned_uploads) and replayed on the
 * next job / startup. The return value carries `orphaned: true` in that case.
 */
async function uploadVideo(filePath, title, log) {
  const yt  = google.youtube({ version: 'v3', auth: ytAuth() });
  const prv = process.env.PRIVACY_STATUS ?? 'unlisted';

  const filename   = path.basename(filePath);
  const groupedName = path.basename(path.dirname(filePath));
  const playlistId = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';

  // ── 1. Upload (stream is ALWAYS destroyed, success or failure) ───────────
  const stream = fs.createReadStream(filePath);
  let res;
  try {
    res = await yt.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: { title: title || filename, description: '' },
        status:  { privacyStatus: prv, selfDeclaredMadeForKids: false },
      },
      media: { body: stream },
    });
  } finally {
    if (!stream.destroyed) stream.destroy();
  }

  const videoId = res.data.id;
  const url     = `https://youtu.be/${videoId}`;
  await log.info(`Uploaded ${filename} → ${url}`);

  // Quota bookkeeping must not be able to lose an already-uploaded video.
  try {
    await incrementQuota(1600);
  } catch (err) {
    await log.warn(`Quota counter update failed after upload of ${videoId}: ${err.message}`);
  }

  // ── 2. Persist (retry, then park in Redis if PG is unavailable) ──────────
  const record = {
    videoId, url, filename, groupedName, playlistId,
    title: title || filename,
    uploadedAt: new Date().toISOString(),
  };

  let lastErr;
  for (let attempt = 1; attempt <= DB_WRITE_ATTEMPTS; attempt++) {
    try {
      await persistUpload(record);
      return { url, videoId, filename, title: record.title, orphaned: false };
    } catch (err) {
      lastErr = err;
      await log.warn(`DB write for ${videoId} failed (attempt ${attempt}/${DB_WRITE_ATTEMPTS}): ${err.message}`);
      if (attempt < DB_WRITE_ATTEMPTS) await sleep(500 * attempt);
    }
  }

  const queued = await queueOrphan(record);
  await log.error(
    queued
      ? `Video ${videoId} (${filename}) is on YouTube but NOT in Postgres — queued in Redis, will be synced before the next upload/startup. Cause: ${lastErr.message}`
      : `Video ${videoId} (${filename}) is on YouTube but NOT in Postgres AND Redis queue failed — manual recovery needed: ${JSON.stringify(record)}`,
  );
  return { url, videoId, filename, title: record.title, orphaned: true, orphanQueued: queued };
}

async function handleSingle(job) {
  const { Logger } = require('../lib/logger');
  const log = new Logger('worker3:single', job);
  const { filePath, title } = job.data;
  await log.info(`Single upload: ${filePath}`);

  // Replay any parked uploads FIRST so the dedup check below sees them.
  await recoverOrphans(log);

  // Dedup guard: check the registry BEFORE calling YouTube.
  const filename     = path.basename(filePath);
  const playlistId   = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';
  const alreadyExists = await check_if_filename_exists_in_registry(playlistId, filename);

  if (alreadyExists) {
    await log.warn(`Filename ${filename} already exists in registry — skipping upload (no YouTube call made)`);
    return { status: 'skipped', reason: 'filename_exists', filename };
  }

  return uploadVideo(filePath, title, log);
}

module.exports = { uploadVideo, handleSingle };