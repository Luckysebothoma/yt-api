'use strict';
require('dotenv/config');
const express    = require('express');
const { google } = require('googleapis');
const { db }     = require('../lib/db');
const { ytAuth } = require('../lib/ytAuth');
const {
  getQuotaUsed, incrementQuota, isQuotaExhausted,
  wouldExceedQuota, quotaSummary, QUOTA_COSTS,
} = require('../lib/quota');

const router = express.Router();

function addQuotaHeaders(res, used) {
  const s = quotaSummary(used);
  res.set('X-Quota-Used',      String(s.used));
  res.set('X-Quota-Remaining', String(s.remaining));
  res.set('X-Quota-Resets-At', s.resets_at);
}

// ─── GET /meta/search ─────────────────────────────────────────────────────────
// Must be defined BEFORE /meta/:videoId to avoid 'search' being caught as an id
router.get('/search', async (req, res) => {
  const { filename, title, tag } = req.query;

  if (!filename && !title && !tag) {
    return res.status(400).json({ error: 'Provide at least one of: filename, title, tag' });
  }

  const conditions = [];
  const params     = [];

  if (filename) {
    params.push(`%${filename}%`);
    conditions.push(`r.filename ILIKE $${params.length}`);
  }
  if (title) {
    params.push(`%${title}%`);
    conditions.push(`(r.title ILIKE $${params.length} OR m.custom_title ILIKE $${params.length})`);
  }
  if (tag) {
    params.push(tag);
    conditions.push(`$${params.length} = ANY(m.tags)`);
  }

  const where = conditions.join(' AND ');

  try {
    const { rows } = await db.query(
      `SELECT
         r.video_id, r.title, r.filename, r.playlist_id, r.privacy,
         r.description, r.duration, r.canonical, r.added_at, r.synced_at,
         m.notes, m.tags, m.local_filename, m.local_path,
         m.custom_title, m.custom_description, m.pending_yt_push,
         m.last_local_edit, m.last_yt_push
       FROM yt_registry r
       LEFT JOIN yt_video_meta m ON m.video_id = r.video_id
       WHERE ${where}
       ORDER BY LOWER(COALESCE(r.filename, r.title, r.video_id)) ASC
       LIMIT 200`,
      params
    );

    res.json({
      count:      rows.length,
      quota_cost: 0,
      results:    rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /meta/:videoId ───────────────────────────────────────────────────────
router.get('/:videoId', async (req, res) => {
  try {
    const { rows } = await db.query(
      `SELECT
         r.video_id, r.title, r.filename, r.playlist_id, r.privacy,
         r.description, r.duration, r.canonical, r.added_at, r.synced_at,
         r.yt_updated_at,
         m.notes, m.tags, m.local_filename, m.local_path,
         m.custom_title, m.custom_description, m.pending_yt_push,
         m.last_local_edit, m.last_yt_push
       FROM yt_registry r
       LEFT JOIN yt_video_meta m ON m.video_id = r.video_id
       WHERE r.video_id = $1`,
      [req.params.videoId]
    );

    if (!rows.length) {
      return res.status(404).json({ error: 'Video not found in local registry' });
    }

    res.json({ ...rows[0], quota_cost: 0 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── PATCH /meta/:videoId ─────────────────────────────────────────────────────
// Local-only update. Accepted fields:
//   filename, title, description, privacy (local flag only)
//   notes, tags (array), local_filename, local_path,
//   custom_title, custom_description
//
// If title or description changes, sets pending_yt_push=TRUE as a reminder
// that the YouTube record is now out of sync.
router.patch('/:videoId', async (req, res) => {
  const { videoId }   = req.params;
  const {
    filename, title, description, privacy,
    notes, tags, local_filename, local_path,
    custom_title, custom_description,
  } = req.body ?? {};

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    // Check video exists
    const { rows: check } = await client.query(
      `SELECT video_id FROM yt_registry WHERE video_id = $1`, [videoId]
    );
    if (!check.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Video not found in local registry' });
    }

    // Update yt_registry fields
    const regUpdates = [];
    const regVals    = [videoId];
    const push = (col, val) => { regVals.push(val); regUpdates.push(`${col} = $${regVals.length}`); };

    if (filename    !== undefined) push('filename',    filename);
    if (title       !== undefined) push('title',       title);
    if (description !== undefined) push('description', description);
    if (privacy     !== undefined) push('privacy',     privacy);

    if (regUpdates.length) {
      await client.query(
        `UPDATE yt_registry SET ${regUpdates.join(', ')} WHERE video_id = $1`,
        regVals
      );
    }

    // Determine if a push flag should be set
    const needsPush = title !== undefined || description !== undefined || privacy !== undefined;

    // Upsert yt_video_meta
    await client.query(
      `INSERT INTO yt_video_meta
         (video_id, notes, tags, local_filename, local_path,
          custom_title, custom_description, pending_yt_push, last_local_edit)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
       ON CONFLICT (video_id) DO UPDATE
         SET notes              = COALESCE($2, yt_video_meta.notes),
             tags               = COALESCE($3, yt_video_meta.tags),
             local_filename     = COALESCE($4, yt_video_meta.local_filename),
             local_path         = COALESCE($5, yt_video_meta.local_path),
             custom_title       = COALESCE($6, yt_video_meta.custom_title),
             custom_description = COALESCE($7, yt_video_meta.custom_description),
             pending_yt_push    = CASE WHEN $8 THEN TRUE ELSE yt_video_meta.pending_yt_push END,
             last_local_edit    = NOW()`,
      [
        videoId,
        notes            ?? null,
        tags             ? tags : null,
        local_filename   ?? null,
        local_path       ?? null,
        custom_title     ?? null,
        custom_description ?? null,
        needsPush,
      ]
    );

    await client.query('COMMIT');

    res.json({
      status:         'updated',
      video_id:       videoId,
      pending_yt_push: needsPush || undefined,
      quota_cost:     0,
      message:        needsPush
        ? 'Local metadata updated. Use PATCH /meta/:id/youtube to push to YouTube.'
        : 'Local metadata updated.',
    });
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// ─── PATCH /meta/:videoId/youtube ────────────────────────────────────────────
// Push title, description, and/or privacyStatus to YouTube.
// Quota cost: 50 units (videos.update).
// Accepted body fields: title, description, privacyStatus
router.patch('/:videoId/youtube', async (req, res) => {
  const { videoId } = req.params;
  const { title, description, privacyStatus } = req.body ?? {};

  if (!title && !description && !privacyStatus) {
    return res.status(400).json({
      error: 'Provide at least one of: title, description, privacyStatus',
    });
  }

  const quotaUsed = await getQuotaUsed();
  addQuotaHeaders(res, quotaUsed);

  if (await isQuotaExhausted()) {
    return res.status(429).json({
      error:   'Quota exhausted — cannot push to YouTube',
      quota:   quotaSummary(quotaUsed),
    });
  }

  if (await wouldExceedQuota(QUOTA_COSTS['videos.update'])) {
    return res.status(429).json({
      error: `This operation costs ${QUOTA_COSTS['videos.update']} units but only ` +
             `${Math.max(0, 10000 - quotaUsed)} remain today`,
      quota: quotaSummary(quotaUsed),
    });
  }

  // Fetch current data from local registry to fill in unchanged fields
  const { rows: local } = await db.query(
    `SELECT title, description, privacy FROM yt_registry WHERE video_id = $1`,
    [videoId]
  );
  if (!local.length) {
    return res.status(404).json({ error: 'Video not found in local registry' });
  }
  const current = local[0];

  try {
    const yt = google.youtube({ version: 'v3', auth: ytAuth() });

    // videos.update requires the full snippet + status objects
    const requestBody = {
      id: videoId,
      snippet: {
        title:       title       ?? current.title       ?? videoId,
        description: description ?? current.description ?? '',
        // categoryId is required by the API; 22 = "People & Blogs" (safe default)
        categoryId: '22',
      },
      status: {
        privacyStatus: privacyStatus ?? current.privacy ?? 'unlisted',
      },
    };

    await yt.videos.update({ part: ['snippet', 'status'], requestBody });
    await incrementQuota(QUOTA_COSTS['videos.update']);

    // Sync local registry with what was just pushed
    await db.query(
      `UPDATE yt_registry
       SET title         = $2,
           description   = $3,
           privacy       = $4,
           yt_updated_at = NOW()
       WHERE video_id = $1`,
      [
        videoId,
        title       ?? current.title,
        description ?? current.description,
        privacyStatus ?? current.privacy,
      ]
    );

    // Clear the pending push flag
    await db.query(
      `UPDATE yt_video_meta
       SET pending_yt_push = FALSE, last_yt_push = NOW()
       WHERE video_id = $1`,
      [videoId]
    );

    const finalQuota = await getQuotaUsed();
    addQuotaHeaders(res, finalQuota);

    res.json({
      status:    'pushed',
      video_id:  videoId,
      pushed:    { title, description, privacyStatus },
      quota:     quotaSummary(finalQuota),
    });

  } catch (err) {
    console.error(`[meta/youtube] ${videoId}`, err);
    res.status(500).json({
      error: err.message,
      quota: quotaSummary(await getQuotaUsed()),
    });
  }
});

module.exports = router;
