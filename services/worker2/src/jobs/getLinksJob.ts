import { Job } from 'bullmq';
import { db } from '../lib/db';
import { Logger } from '../lib/logger';

const DEFAULT_LIMIT = 5000;
const MAX_LIMIT     = 10000;

const toInt = (v: unknown, fallback: number) => {
  const n = parseInt(String(v ?? ''), 10);
  return Number.isFinite(n) ? n : fallback;
};

// One row per duplicate group (oldest kept). Same key as worker4 "basis=both":
// lower(filename) → lower(title) → video_id (so key-less rows never collapse together).
// Rows worker4 marked canonical=false are skipped; if the column doesn't exist
// yet, to_jsonb(r)->>'canonical' is NULL and everything counts as canonical.
const UNIQUE_LINKS_SQL = `
  WITH keyed AS (
    SELECT video_id, title, filename, added_at,
           COALESCE(NULLIF(LOWER(COALESCE(NULLIF(filename,''), title, '')), ''), video_id) AS dup_key
      FROM yt_registry r
     WHERE COALESCE((to_jsonb(r) ->> 'canonical')::boolean, TRUE)
  ),
  uniq AS (
    SELECT DISTINCT ON (dup_key)
           video_id, title, filename, added_at AS uploaded_at
      FROM keyed
     ORDER BY dup_key, added_at ASC NULLS LAST, video_id
  )
  SELECT video_id, title, filename, uploaded_at,
         'https://www.youtube.com/watch?v=' || video_id AS watch_url,
         (COUNT(*) OVER ())::int             AS unique_total,
         (SELECT COUNT(*)::int FROM keyed)   AS eligible_total
    FROM uniq
   ORDER BY uploaded_at DESC NULLS LAST, video_id
   LIMIT $1 OFFSET $2`;

export async function handleGetLinks(job: Job): Promise<object> {
  const log    = new Logger('worker2:get-links', job);
  const limit  = Math.min(MAX_LIMIT, Math.max(1, toInt(job.data?.limit, DEFAULT_LIMIT)));
  const offset = Math.max(0, toInt(job.data?.offset, 0));

  await log.info(`Fetching de-duplicated links (limit=${limit} offset=${offset})`);
  const t0 = Date.now();
  const { rows } = await db.query(UNIQUE_LINKS_SQL, [limit, offset]);

  const uniqueTotal  = rows[0]?.unique_total   ?? 0;
  const eligibleTotal = rows[0]?.eligible_total ?? 0;
  const links = rows.map(({ unique_total, eligible_total, ...link }) => link);

  await log.info(`Returned ${links.length}/${uniqueTotal} unique links in ${Date.now() - t0}ms`);
  return {
    links,
    returned:          links.length,
    unique_total:      uniqueTotal,
    duplicates_hidden: Math.max(0, eligibleTotal - uniqueTotal),
    limit,
    offset,
    has_more:          offset + links.length < uniqueTotal,
  };
}
