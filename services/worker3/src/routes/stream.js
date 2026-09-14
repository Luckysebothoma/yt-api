'use strict';
require('dotenv/config');
const express  = require('express');
const { db }   = require('../lib/db');
const wl       = require('../lib/watchLater');
const { resolveStream }    = require('../lib/streamProxy');
const { isQuotaExhausted, getQuotaUsed, QUOTA_DAILY_LIMIT, quotaResetsAt } = require('../lib/quota');

const router = express.Router();

// ─── helpers ─────────────────────────────────────────────────────────────────
function parsePage(query) {
  const page   = Math.max(1, parseInt(query.page  ?? '1',  10));
  const limit  = Math.min(200, Math.max(1, parseInt(query.limit ?? '50', 10)));
  const offset = (page - 1) * limit;
  return { page, limit, offset };
}

function quotaHeaders(res, used) {
  res.set('X-Quota-Used',      String(used));
  res.set('X-Quota-Limit',     String(QUOTA_DAILY_LIMIT));
  res.set('X-Quota-Resets-At', quotaResetsAt().toISOString());
}

// ─── GET /stream/allpaginator ─────────────────────────────────────────────────
// Returns paginated video list sorted by filename (or title).
// Does NOT call YouTube API — reads from PG registry only.
// Each video entry has a /stream/video/:id href the client can follow.
router.get('/allpaginator', async (req, res) => {
  try {
    const { page, limit, offset } = parsePage(req.query);
    const sort = req.query.sort === 'title' ? 'title' : 'file_name';

    const { rows: videos } = await db.query(
      `SELECT yt_video_id AS video_id,
              COALESCE(file_name, title, yt_video_id) AS display_name,
              title,
              file_name,
              synced_at
       FROM yt_video_registry
       ORDER BY LOWER(COALESCE(file_name, title, yt_video_id)) ASC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    const { rows: [{ n: total }] } = await db.query(
      `SELECT COUNT(*)::int AS n FROM yt_video_registry`
    );

    const quotaUsed    = await getQuotaUsed();
    const quotaFull    = quotaUsed >= QUOTA_DAILY_LIMIT;
    const wlCount      = await wl.count();

    quotaHeaders(res, quotaUsed);

    res.json({
      page,
      limit,
      total,
      pages:         Math.ceil(total / limit),
      quota_exhausted: quotaFull,
      quota_used:    quotaUsed,
      quota_limit:   QUOTA_DAILY_LIMIT,
      quota_resets_at: quotaResetsAt().toISOString(),
      watch_later_queued: wlCount,
      sort,
      videos: videos.map(v => ({
        video_id:     v.video_id,
        display_name: v.display_name,
        title:        v.title,
        filename:     v.filename,
        playlist_id:  v.playlist_id,
        synced_at:    v.synced_at,
        stream_href:  `http://192.168.0.140:3099/stream/video/${v.video_id}`,
        watch_url:    `https://www.youtube.com/watch?v=${v.video_id}`,
      })),
    });
  } catch (err) {
    console.error('[stream/allpaginator]', err);
    res.status(500).json({ error: err.message });
  }
});

router.get('/all/yt-video-registry/allpaginator', async (req, res) => {
  try {
    const sort = req.query.sort === 'title' ? 'title' : 'file_name';
    const { rows: videos } = await db.query(
      `SELECT yt_video_id AS video_id,
              COALESCE(file_name, title, yt_video_id) AS display_name,
              title,
              file_name,
              synced_at
       FROM yt_video_registry
       ORDER BY LOWER(COALESCE(file_name, title, yt_video_id)) ASC`
    );
    const total = videos.length;
    const quotaUsed    = await getQuotaUsed();
    const quotaFull    = quotaUsed >= QUOTA_DAILY_LIMIT;
    const wlCount      = await wl.count();
    quotaHeaders(res, quotaUsed);
    res.json({
      total,
      quota_exhausted: quotaFull,
      quota_used:    quotaUsed,
      quota_limit:   QUOTA_DAILY_LIMIT,
      quota_resets_at: quotaResetsAt().toISOString(),
      watch_later_queued: wlCount,
      sort,
      videos: videos.map(v => ({
        video_id:     v.video_id,
        display_name: v.display_name,
        title:        v.title,
        filename:     v.file_name,
        playlist_id:  v.playlist_id,
        synced_at:    v.synced_at,
        stream_href:  `http://192.168.0.140:3099/stream/video/${v.video_id}`,
        watch_url:    `https://www.youtube.com/watch?v=${v.video_id}`,
      })),
    });
  } catch (err) {
    console.error('[stream/allpaginator]', err);
    res.status(500).json({ error: err.message });
  }
});
router.get('/all/yt-registry/allpaginator', async (req, res) => {
  try {
    const sort = req.query.sort === 'title' ? 'title' : 'filename';
    const { rows: videos } = await db.query(
      `SELECT video_id,
              COALESCE(filename, video_id) AS display_name,
              filename,
              playlist_id,
              synced_at
       FROM yt_registry
       ORDER BY LOWER(COALESCE(filename, video_id)) ASC`
    );
    const total = videos.length;
    const quotaUsed    = await getQuotaUsed();
    const quotaFull    = quotaUsed >= QUOTA_DAILY_LIMIT;
    const wlCount      = await wl.count();
    quotaHeaders(res, quotaUsed);
    res.json({
      total,
      quota_exhausted: quotaFull,
      quota_used:      quotaUsed,
      quota_limit:     QUOTA_DAILY_LIMIT,
      quota_resets_at: quotaResetsAt().toISOString(),
      watch_later_queued: wlCount,
      sort,
      videos: videos.map(v => ({
        video_id:     v.video_id,
        display_name: v.display_name,
        filename:     v.filename,
        playlist_id:  v.playlist_id,
        synced_at:    v.synced_at,
        stream_href:  `http://192.168.0.140:3099/stream/video/${v.video_id}`,
        watch_url:    `https://www.youtube.com/watch?v=${v.video_id}`,
      })),
    });
  } catch (err) {
    console.error('[stream/allpaginator]', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /stream/video/:videoId ───────────────────────────────────────────────
// Resolves and streams (or redirects to) a single video.
// On quota exhaustion or stream unavailability: enqueue to watch-later → 202.
router.get('/video/:videoId', async (req, res) => {
  const { videoId } = req.params;
  const quotaUsed   = await getQuotaUsed();
  quotaHeaders(res, quotaUsed);

  // Fast-path: quota already gone before we even try
  if (await isQuotaExhausted()) {
    // Fetch metadata from PG so we can label the queue entry properly
    const { rows } = await db.query(
      `SELECT title, file_name, playlist_id FROM yt_video_registry WHERE yt_video_id = $1`,
      [videoId]
    );
    const meta = rows[0] ?? {};
    await wl.enqueue(videoId, {
      title:      meta.title,
      filename:   meta.filename,
      playlistId: meta.playlist_id,
      reason:     'quota_exhausted',
    });

    return res.status(202).json({
      status:      'watch_later',
      video_id:    videoId,
      reason:      'quota_exhausted',
      resets_at:   quotaResetsAt().toISOString(),
      watch_url:   `https://www.youtube.com/watch?v=${videoId}`,
      message:     'Quota exhausted — video queued to Watch Later',
    });
  }

  try {
    const stream = await resolveStream(videoId);

    if (stream.mode === 'redirect') {
      // Redirect client to YouTube watch page (most common path without yt-dlp)
      return res.redirect(302, stream.url);
    }

    if (stream.mode === 'proxy') {
      // Proxy the byte stream from the resolved direct URL
      const { fetch } = require('undici');
      const upstream  = await fetch(stream.url, {
        headers: { Range: req.headers.range ?? 'bytes=0-' },
      });

      res.status(upstream.status);
      res.set('Content-Type',        upstream.headers.get('content-type') ?? 'video/mp4');
      res.set('Content-Length',      upstream.headers.get('content-length') ?? '');
      res.set('Accept-Ranges',       'bytes');
      res.set('Content-Range',       upstream.headers.get('content-range') ?? '');
      res.set('Content-Disposition', `inline; filename="${stream.title}.mp4"`);

      upstream.body.pipeTo(
        new WritableStream({
          write(chunk) { res.write(chunk); },
          close()      { res.end(); },
          abort(err)   { res.destroy(err); },
        })
      );
      return;
    }
  } catch (err) {
    if (err.code === 'QUOTA_EXHAUSTED' || err.code === 'STREAM_UNAVAILABLE') {
      const { rows } = await db.query(
        `SELECT title, file_name, playlist_id FROM yt_video_registry WHERE yt_video_id = $1`,
        [videoId]
      );
      const meta = rows[0] ?? {};
      const reason = err.code === 'QUOTA_EXHAUSTED' ? 'quota_exhausted' : 'stream_unavailable';

      await wl.enqueue(videoId, {
        title:      meta.title,
        filename:   meta.file_name,
        playlistId: meta.playlist_id,
        reason,
      });

      return res.status(202).json({
        status:    'watch_later',
        video_id:  videoId,
        reason,
        resets_at: err.code === 'QUOTA_EXHAUSTED' ? quotaResetsAt().toISOString() : null,
        watch_url: `https://www.youtube.com/watch?v=${videoId}`,
        message:   'Video queued to Watch Later',
      });
    }

    console.error(`[stream/video] ${videoId}`, err);
    return res.status(500).json({ error: err.message });
  }
});

// ─── GET /stream/watchlater ───────────────────────────────────────────────────
router.get('/watchlater', async (req, res) => {
  try {
    const { page, limit, offset } = parsePage(req.query);
    const items = await wl.list(limit, offset);
    const total = await wl.count();
    const quotaUsed = await getQuotaUsed();
    quotaHeaders(res, quotaUsed);

    res.json({
      page,
      limit,
      total,
      pages: Math.ceil(total / limit),
      quota_exhausted: quotaUsed >= QUOTA_DAILY_LIMIT,
      quota_resets_at: quotaResetsAt().toISOString(),
      items: items.map(i => ({
        ...i,
        watch_url:   `https://www.youtube.com/watch?v=${i.video_id}`,
        stream_href: `http://192.168.0.140:3099/stream/video/${i.video_id}`,
      })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── DELETE /stream/watchlater/:videoId ──────────────────────────────────────
router.delete('/watchlater/:videoId', async (req, res) => {
  try {
    await wl.remove(req.params.videoId);
    res.json({ status: 'removed', video_id: req.params.videoId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /stream/watchlater/retry ───────────────────────────────────────────
// Attempts to resolve all queued watch-later items.
// Stops as soon as quota is exhausted again.
// Returns a summary of what was resolved vs re-queued.
router.post('/watchlater/retry', async (req, res) => {
  try {
    const quotaUsed = await getQuotaUsed();
    quotaHeaders(res, quotaUsed);

    if (await isQuotaExhausted()) {
      return res.status(429).json({
        status:    'quota_exhausted',
        resets_at: quotaResetsAt().toISOString(),
        message:   'Cannot retry — quota still exhausted',
      });
    }

    const items    = await wl.list(200, 0);
    const resolved = [];
    const failed   = [];

    for (const item of items) {
      if (await isQuotaExhausted()) {
        failed.push({ video_id: item.video_id, reason: 'quota_exhausted_mid_retry' });
        continue;
      }
      try {
        await resolveStream(item.video_id); // just verify it's streamable
        await wl.resolve(item.video_id);
        resolved.push(item.video_id);
      } catch (err) {
        failed.push({ video_id: item.video_id, reason: err.code ?? err.message });
      }
    }

    res.json({
      status:   'done',
      resolved: resolved.length,
      failed:   failed.length,
      details:  { resolved, failed },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
