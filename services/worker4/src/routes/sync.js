'use strict';
require('dotenv/config');
const express  = require('express');
const { google } = require('googleapis');
const { db }   = require('../lib/db');
const { ytAuth } = require('../lib/ytAuth');
const { resolvePlaylistId } = require('../lib/playlistResolver');
const {
  getQuotaUsed, incrementQuota, isQuotaExhausted,
  wouldExceedQuota, quotaSummary, QUOTA_COSTS,
} = require('../lib/quota');

const router = express.Router();

// ─── helpers ─────────────────────────────────────────────────────────────────

function addQuotaHeaders(res, used) {
  const s = quotaSummary(used);
  res.set('X-Quota-Used',      String(s.used));
  res.set('X-Quota-Remaining', String(s.remaining));
  res.set('X-Quota-Resets-At', s.resets_at);
}

/**
 * Upsert a batch of playlist items into yt_registry.
 * Only inserts new rows; does NOT overwrite existing title/filename/description
 * unless ?force=true was requested (to protect local edits).
 */
async function upsertBatch(items, force = false) {
  if (!items.length) return;

  const ids          = items.map(i => i.videoId);
  const playlistIds  = items.map(i => i.playlistId);
  const titles       = items.map(i => i.title);
  const descriptions = items.map(i => i.description);
  const privacies    = items.map(i => i.privacy ?? 'unlisted');
  const durations    = items.map(i => i.duration ?? null);

  if (force) {
    // Full overwrite of YT-sourced fields
    await db.query(
      `INSERT INTO yt_registry
         (video_id, playlist_id, title, description, privacy, duration, synced_at)
       SELECT
         unnest($1::text[]),
         unnest($2::text[]),
         unnest($3::text[]),
         unnest($4::text[]),
         unnest($5::text[]),
         unnest($6::text[]),
         NOW()
       ON CONFLICT (video_id) DO UPDATE
         SET title       = EXCLUDED.title,
             description = EXCLUDED.description,
             privacy     = EXCLUDED.privacy,
             duration    = EXCLUDED.duration,
             synced_at   = NOW()`,
      [ids, playlistIds, titles, descriptions, privacies, durations]
    );
  } else {
    // Insert-only for new videos; do not overwrite local edits on existing rows
    await db.query(
      `INSERT INTO yt_registry
         (video_id, playlist_id, title, description, privacy, duration, synced_at)
       SELECT
         unnest($1::text[]),
         unnest($2::text[]),
         unnest($3::text[]),
         unnest($4::text[]),
         unnest($5::text[]),
         unnest($6::text[]),
         NOW()
       ON CONFLICT (video_id) DO UPDATE
         SET synced_at = NOW()`,
      [ids, playlistIds, titles, descriptions, privacies, durations]
    );
  }
}

// ─── POST /sync/pull ──────────────────────────────────────────────────────────
router.post('/pull', async (req, res) => {
  const force         = req.query.force === 'true';
  const resumeToken   = req.body?.page_token ?? req.query.page_token ?? undefined;
  const maxPages      = Math.min(500, parseInt(req.query.max_pages ?? '500', 10));

  let quotaUsed = await getQuotaUsed();
  addQuotaHeaders(res, quotaUsed);

  if (await isQuotaExhausted()) {
    return res.status(429).json({
      status:    'quota_exhausted',
      ...quotaSummary(quotaUsed),
      message:   'Cannot sync — daily quota exhausted. Try again after resets_at.',
    });
  }

  let playlistId;
  try {
    playlistId = req.query.playlist_id || await resolvePlaylistId();
  } catch (err) {
    return res.status(500).json({ error: `Could not resolve playlist: ${err.message}` });
  }

  const yt        = google.youtube({ version: 'v3', auth: ytAuth() });
  let   pageToken = resumeToken;
  let   pageNum   = 0;
  let   inserted  = 0;
  let   seen      = 0;
  let   nextPageToken = null;

  try {
    do {
      // Guard: stop if next page would exhaust quota
      if (await wouldExceedQuota(QUOTA_COSTS['playlistItems.list'])) {
        console.warn(`[sync/pull] Quota ceiling reached before page ${pageNum + 1} — suspending`);
        nextPageToken = pageToken ?? null;
        break;
      }

      const resp = await yt.playlistItems.list({
        part:        ['snippet', 'contentDetails'],
        playlistId,
        maxResults:  50,
        pageToken,
      });
      await incrementQuota(QUOTA_COSTS['playlistItems.list']);
      quotaUsed = await getQuotaUsed();
      pageNum++;

      const rawItems = resp.data.items ?? [];
      seen += rawItems.length;

      const batch = rawItems
        .map(item => ({
          videoId:     item.contentDetails?.videoId,
          playlistId,
          title:       item.snippet?.title ?? null,
          description: item.snippet?.description ?? null,
          privacy:     item.status?.privacyStatus ?? 'unlisted',
          duration:    null, // playlistItems.list doesn't include duration
        }))
        .filter(i => i.videoId && i.videoId !== 'Deleted video' && i.videoId !== 'Private video');

      // Check which videoIds are new (not yet in registry)
      const batchIds = batch.map(b => b.videoId);
      const { rows: existing } = await db.query(
        `SELECT video_id FROM yt_registry WHERE video_id = ANY($1::text[])`,
        [batchIds]
      );
      const existingSet = new Set(existing.map(r => r.video_id));
      const newItems    = batch.filter(b => !existingSet.has(b.videoId));

      await upsertBatch(force ? batch : newItems, force);
      inserted += force ? batch.length : newItems.length;

      console.log(
        `[sync/pull] page=${pageNum} seen=${rawItems.length} ` +
        `new=${newItems.length} quota=${quotaUsed}`
      );

      pageToken     = resp.data.nextPageToken ?? undefined;
      nextPageToken = pageToken ?? null;

    } while (pageToken && pageNum < maxPages);

    // Update meta
    await db.query(
      `INSERT INTO yt_registry_meta (playlist_id, last_full_sync, total_videos, updated_at)
       VALUES ($1, NOW(), (SELECT COUNT(*) FROM yt_registry WHERE playlist_id = $1), NOW())
       ON CONFLICT (playlist_id) DO UPDATE
         SET last_full_sync = NOW(),
             total_videos   = (SELECT COUNT(*) FROM yt_registry WHERE playlist_id = $1),
             updated_at     = NOW()`,
      [playlistId]
    );

    addQuotaHeaders(res, quotaUsed);
    res.json({
      status:          nextPageToken ? 'partial' : 'complete',
      playlist_id:     playlistId,
      pages_fetched:   pageNum,
      videos_seen:     seen,
      videos_inserted: inserted,
      resume_token:    nextPageToken,  // non-null means quota cut the sync short
      quota:           quotaSummary(quotaUsed),
      message: nextPageToken
        ? `Sync paused at page ${pageNum} due to quota. Resume with page_token.`
        : `Sync complete — ${inserted} new videos added from ${seen} seen.`,
    });

  } catch (err) {
    console.error('[sync/pull]', err);
    res.status(500).json({ error: err.message, quota: quotaSummary(await getQuotaUsed()) });
  }
});

// ─── GET /sync/status ─────────────────────────────────────────────────────────
router.get('/status', async (req, res) => {
  const skipDrift = req.query.skip_drift === 'true';

  try {
    const quotaUsed = await getQuotaUsed();
    addQuotaHeaders(res, quotaUsed);

    const { rows: [counts] } = await db.query(
      `SELECT COUNT(*)::int AS total,
              MAX(synced_at) AS last_synced
       FROM yt_registry`
    );

    const { rows: metaRows } = await db.query(
      `SELECT playlist_id, last_full_sync, total_videos
       FROM yt_registry_meta
       ORDER BY updated_at DESC
       LIMIT 5`
    );

    let drift = null;
    if (!skipDrift && !(await isQuotaExhausted())) {
      try {
        const playlistId = await resolvePlaylistId();
        const yt = google.youtube({ version: 'v3', auth: ytAuth() });
        const resp = await yt.playlistItems.list({
          part: ['id'], playlistId, maxResults: 1,
        });
        await incrementQuota(QUOTA_COSTS['playlistItems.list']);

        const ytTotal = resp.data.pageInfo?.totalResults ?? null;
        drift = {
          local_count:    counts.total,
          youtube_count:  ytTotal,
          difference:     ytTotal !== null ? ytTotal - counts.total : null,
          in_sync:        ytTotal !== null ? ytTotal === counts.total : null,
        };
      } catch (e) {
        drift = { error: e.message };
      }
    }

    res.json({
      registry: {
        total_videos:  counts.total,
        last_synced:   counts.last_synced,
        playlists:     metaRows,
      },
      drift: skipDrift ? 'skipped (pass skip_drift=false to check)' : drift,
      quota: quotaSummary(await getQuotaUsed()),
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
