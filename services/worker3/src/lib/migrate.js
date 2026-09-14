'use strict';
const { db } = require('./db');

const MIGRATION = `
  -- Core registry
  CREATE TABLE IF NOT EXISTS yt_registry (
    video_id     TEXT        PRIMARY KEY,
    playlist_id  TEXT        NOT NULL,
    title        TEXT,
    filename     TEXT,
    added_at     TIMESTAMPTZ DEFAULT NOW(),
    synced_at    TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE INDEX IF NOT EXISTS idx_yt_registry_playlist
    ON yt_registry (playlist_id);
  CREATE INDEX IF NOT EXISTS idx_yt_registry_filename
    ON yt_registry (filename);

  -- Patch: add filename if table existed before this column was introduced
  ALTER TABLE yt_registry
    ADD COLUMN IF NOT EXISTS filename TEXT;

  CREATE TABLE IF NOT EXISTS yt_registry_meta (
    playlist_id    TEXT        PRIMARY KEY,
    last_full_sync TIMESTAMPTZ,
    total_videos   INTEGER     DEFAULT 0,
    updated_at     TIMESTAMPTZ DEFAULT NOW()
  );

  -- Watch-Later
  CREATE TABLE IF NOT EXISTS yt_watch_later (
    id           SERIAL      PRIMARY KEY,
    video_id     TEXT        NOT NULL UNIQUE,
    title        TEXT,
    filename     TEXT,
    playlist_id  TEXT,
    reason       TEXT        NOT NULL DEFAULT 'quota_exhausted'
                 CHECK (reason IN ('quota_exhausted', 'unavailable', 'private', 'other')),
    queued_at    TIMESTAMPTZ DEFAULT NOW(),
    retry_count  INT         DEFAULT 0,
    last_retry   TIMESTAMPTZ,
    resolved     BOOLEAN     DEFAULT FALSE,
    resolved_at  TIMESTAMPTZ,
    CONSTRAINT chk_resolved_at CHECK (
      (resolved = FALSE AND resolved_at IS NULL) OR
      (resolved = TRUE  AND resolved_at IS NOT NULL)
    )
  );

  -- Patch: add filename if table existed before this column was introduced
  ALTER TABLE yt_watch_later
    ADD COLUMN IF NOT EXISTS filename TEXT;

  CREATE INDEX IF NOT EXISTS idx_yt_watch_later_resolved
    ON yt_watch_later (resolved);
  CREATE INDEX IF NOT EXISTS idx_yt_watch_later_video_id
    ON yt_watch_later (video_id);
`;

async function runMigration() {
  // Every statement in MIGRATION uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS,
  // so it's safe (and necessary) to actually run this on every startup rather
  // than just logging that it happened. Without this, yt_registry /
  // yt_watch_later only exist if someone created them by hand.
  await db.query(MIGRATION);
  console.log('[migrate] yt_registry + yt_watch_later tables ready');
}

module.exports = { runMigration };
