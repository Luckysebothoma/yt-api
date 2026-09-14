'use strict';
require('dotenv/config');
const { google }           = require('googleapis');
const { ytAuth }           = require('./ytAuth');
const { incrementQuota, isQuotaExhausted } = require('./quota');

const YTDLP_ENDPOINT = process.env.YT_DLP_ENDPOINT ?? null;

/**
 * resolveStream(videoId)
 * Returns { mode, url, videoId, title, duration }
 *
 * mode:
 *   'redirect'  — client should GET the watch URL (no bandwidth on our side)
 *   'proxy'     — direct streamable URL via yt-dlp sidecar
 */
async function resolveStream(videoId) {
  if (await isQuotaExhausted()) {
    throw Object.assign(new Error('QUOTA_EXHAUSTED'), { code: 'QUOTA_EXHAUSTED' });
  }

  const yt  = google.youtube({ version: 'v3', auth: ytAuth() });
  const res = await yt.videos.list({
    part: ['snippet', 'contentDetails', 'status'],
    id:   [videoId],
  });
  await incrementQuota(1);

  const item = res.data.items?.[0];
  if (!item) {
    throw Object.assign(new Error('STREAM_UNAVAILABLE'), { code: 'STREAM_UNAVAILABLE' });
  }

  const privacyStatus = item.status?.privacyStatus;
  if (privacyStatus === 'private') {
    throw Object.assign(
      new Error(`Video ${videoId} is private`),
      { code: 'STREAM_UNAVAILABLE' }
    );
  }

  const title    = item.snippet?.title ?? videoId;
  const duration = item.contentDetails?.duration ?? null;
  const watchUrl = `https://www.youtube.com/watch?v=${videoId}`;

  // If a yt-dlp sidecar is configured, fetch a direct streamable URL from it
  if (YTDLP_ENDPOINT) {
    try {
      const { fetch } = require('undici');
      const dlRes     = await fetch(`${YTDLP_ENDPOINT}/url?id=${videoId}`);
      if (dlRes.ok) {
        const body = await dlRes.json();
        if (body.url) {
          return { mode: 'proxy', url: body.url, videoId, title, duration };
        }
      }
    } catch (e) {
      console.warn(`[streamProxy] yt-dlp sidecar error for ${videoId}: ${e.message} — falling back to redirect`);
    }
  }

  return { mode: 'redirect', url: watchUrl, videoId, title, duration };
}

module.exports = { resolveStream };
