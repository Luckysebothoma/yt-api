'use strict';
const { db } = require('./db');

const MIGRATION = `
BEGIN;
  -- Core registry
  CREATE TABLE IF NOT EXISTS yt_registry (
    video_id     TEXT        PRIMARY KEY,
    playlist_id  TEXT        NOT NULL,
    title        TEXT,
    filename     TEXT,
    added_at     TIMESTAMPTZ DEFAULT NOW(),
    synced_at    TIMESTAMPTZ DEFAULT NOW()
  );

  -- Patch FIRST: add filename if the table existed before this column was
  -- introduced. Must run before any index that references the column.
  ALTER TABLE yt_registry
    ADD COLUMN IF NOT EXISTS filename TEXT;

  CREATE INDEX IF NOT EXISTS idx_yt_registry_playlist
    ON yt_registry (playlist_id);
  CREATE INDEX IF NOT EXISTS idx_yt_registry_filename
    ON yt_registry (filename);
  -- Dedup lookups use LOWER(filename); a plain index can't serve them.
  CREATE INDEX IF NOT EXISTS idx_yt_registry_playlist_lower_filename
    ON yt_registry (playlist_id, LOWER(filename));

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



CREATE TABLE IF NOT EXISTS public.videos (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    yt_url       TEXT NOT NULL,
    yt_video_id  TEXT NOT NULL,
    title        TEXT,
    grouped_name TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Required for ON CONFLICT (yt_video_id) DO NOTHING
CREATE UNIQUE INDEX IF NOT EXISTS idx_videos_yt_video_id
    ON public.videos (yt_video_id);

-- Useful for retrieving videos by group
CREATE INDEX IF NOT EXISTS idx_videos_grouped_name
    ON public.videos (grouped_name);

-- Useful for listing recently inserted videos
CREATE INDEX IF NOT EXISTS idx_videos_created_at
    ON public.videos (created_at DESC);

COMMIT;

`;

async function runMigration() {
  // Every statement uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS, so it is
  // safe to run on every startup.
  await db.query(MIGRATION);
  console.log('[migrate] yt_registry + yt_watch_later tables ready');
}

module.exports = { runMigration };
