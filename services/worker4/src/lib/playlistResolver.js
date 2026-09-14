'use strict';
require('dotenv/config');
const { google }   = require('googleapis');
const { ytAuth }   = require('./ytAuth');
const { getRedis } = require('./redis');
const { incrementQuota } = require('./quota');

const PLAYLIST_ID_ENV = process.env.YT_UPLOADS_PLAYLIST_ID ?? null;

async function resolvePlaylistId() {
  if (PLAYLIST_ID_ENV) return PLAYLIST_ID_ENV;

  const cached = await getRedis().get('yt:uploads_playlist_id');
  if (cached) return cached;

  const yt  = google.youtube({ version: 'v3', auth: ytAuth() });
  const res = await yt.channels.list({ part: ['contentDetails'], mine: true });
  await incrementQuota(1); // channels.list = 1 unit

  const id = res.data.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  if (!id) throw new Error('Could not resolve uploads playlist ID');

  await getRedis().setex('yt:uploads_playlist_id', 86400, id);
  return id;
}

module.exports = { resolvePlaylistId };
