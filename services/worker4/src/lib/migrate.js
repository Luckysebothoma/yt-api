'use strict';
const { db } = require('./db');

const MIGRATION = `
  -- yt_registry already exists (created by worker3).
  -- Add columns worker4 needs if they aren't there yet.
  ALTER TABLE yt_registry
    ADD COLUMN IF NOT EXISTS description   TEXT,
    ADD COLUMN IF NOT EXISTS privacy       TEXT DEFAULT 'unlisted',
    ADD COLUMN IF NOT EXISTS duration      TEXT,
    ADD COLUMN IF NOT EXISTS yt_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS canonical     BOOLEAN DEFAULT TRUE;

  -- Extended local-only metadata — never pushes to YouTube automatically.
  -- Push is an explicit PATCH /meta/:id/youtube operation.
  CREATE TABLE IF NOT EXISTS yt_video_meta (
    video_id          TEXT        PRIMARY KEY REFERENCES yt_registry(video_id) ON DELETE CASCADE,
    notes             TEXT,
    tags              TEXT[]      DEFAULT '{}',
    local_filename    TEXT,
    local_path        TEXT,
    custom_title      TEXT,
    custom_description TEXT,
    pending_yt_push   BOOLEAN     DEFAULT FALSE,
    last_local_edit   TIMESTAMPTZ DEFAULT NOW(),
    last_yt_push      TIMESTAMPTZ
  );

  -- Index for duplicate detection queries
  CREATE INDEX IF NOT EXISTS idx_yt_registry_filename_lower
    ON yt_registry (LOWER(COALESCE(filename, '')));

  CREATE INDEX IF NOT EXISTS idx_yt_registry_title_lower
    ON yt_registry (LOWER(COALESCE(title, '')));

  -- Index for tag search
  CREATE INDEX IF NOT EXISTS idx_yt_video_meta_tags
    ON yt_video_meta USING GIN (tags);
`;

async function runMigration() {
  await db.query(MIGRATION);
  console.log('[migrate] worker4 schema ready');
}

module.exports = { runMigration };
