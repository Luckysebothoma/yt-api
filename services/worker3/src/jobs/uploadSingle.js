'use strict';
require('dotenv/config');
const fs         = require('fs');
const path       = require('path');
const { google } = require('googleapis');
const { db }     = require('../lib/db');
const { ytAuth } = require('../lib/ytAuth');
const { incrementQuota } = require('../lib/quota');
const { check_if_filename_exists_in_registry } = require('../lib/registry');

async function uploadVideo(filePath, title, log) {
  const yt  = google.youtube({ version: 'v3', auth: ytAuth() });
  const prv = process.env.PRIVACY_STATUS ?? 'unlisted';
  const res = await yt.videos.insert({
    part: ['snippet', 'status'],
    requestBody: {
      snippet: { title: title || path.basename(filePath), description: '' },
      status:  { privacyStatus: prv, selfDeclaredMadeForKids: false },
    },
    media: { body: fs.createReadStream(filePath) },
  });

  await incrementQuota(1600);

  const videoId     = res.data.id;
  const url         = `https://youtu.be/${videoId}`;
  const groupedName = path.basename(path.dirname(filePath));
  const filename    = path.basename(filePath);
  const playlistId  = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';

  await log.info(`Uploaded ${filename} → ${url}`);

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `INSERT INTO videos (yt_url, yt_video_id, title, grouped_name)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (yt_video_id) DO NOTHING`,
      [url, videoId, title || filename, groupedName]
    );

    await client.query(
      `INSERT INTO yt_registry (video_id, playlist_id, title, filename, synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (video_id) DO UPDATE
         SET synced_at = NOW(),
             title     = EXCLUDED.title,
             filename  = EXCLUDED.filename`,
      [videoId, playlistId, title || filename, filename]
    );

    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  return { url, videoId, filename };
}

async function handleSingle(job) {
  const { Logger } = require('../lib/logger');
  const log = new Logger('worker3:single', job);
  const { filePath, title } = job.data;
  await log.info(`Single upload: ${filePath}`);

  // Same dedup guard as the batch path: check the registry BEFORE calling
  // YouTube. Previously this route had no dedup check at all, so hitting
  // /upload/single with an already-uploaded filename would create a
  // duplicate video on the channel every time.
  const filename    = path.basename(filePath);
  const playlistId  = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';
  const alreadyExists = await check_if_filename_exists_in_registry(playlistId, filename);

  if (alreadyExists) {
    await log.warn(`Filename ${filename} already exists in registry — skipping upload (no YouTube call made)`);
    return { status: 'skipped', reason: 'filename_exists', filename };
  }

  return uploadVideo(filePath, title, log);
}

module.exports = { uploadVideo, handleSingle };
