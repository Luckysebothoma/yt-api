'use strict';
const express = require('express');
const { db }  = require('../lib/db');

const router = express.Router();

function parsePage(q) {
  const page  = Math.max(1, parseInt(q.page  ?? '1',  10));
  const limit = Math.min(200, Math.max(1, parseInt(q.limit ?? '50', 10)));
  return { page, limit, offset: (page - 1) * limit };
}

/**
 * Core duplicate query.
 * Returns rows grouped by normalised key (lower filename or lower title).
 * Only returns keys where COUNT > 1 (actual duplicates).
 *
 * basis: 'filename' | 'title' | 'both'
 * filter: optional ILIKE substring
 */
async function fetchDuplicateGroups(basis = 'both', filter = null, limit = 50, offset = 0) {
  // Build the grouping expression
  let groupExpr;
  if (basis === 'filename') {
    groupExpr = `LOWER(COALESCE(filename, ''))`;
  } else if (basis === 'title') {
    groupExpr = `LOWER(COALESCE(title, ''))`;
  } else {
    // 'both': group by filename when available, fall back to title
    groupExpr = `LOWER(COALESCE(NULLIF(filename,''), title, ''))`;
  }

  const filterClause = filter
    ? `AND (filename ILIKE $3 OR title ILIKE $3)`
    : '';
  const params = filter
    ? [limit, offset, `%${filter}%`]
    : [limit, offset];

  const { rows } = await db.query(
    `WITH dup_keys AS (
       SELECT ${groupExpr} AS dup_key
       FROM   yt_registry
       WHERE  ${groupExpr} != ''
       ${filterClause}
       GROUP  BY dup_key
       HAVING COUNT(*) > 1
     )
     SELECT
       dk.dup_key,
       COUNT(r.video_id)::int                     AS count,
       JSON_AGG(
         JSON_BUILD_OBJECT(
           'video_id',    r.video_id,
           'title',       r.title,
           'filename',    r.filename,
           'playlist_id', r.playlist_id,
           'privacy',     r.privacy,
           'added_at',    r.added_at,
           'synced_at',   r.synced_at,
           'canonical',   r.canonical
         ) ORDER BY r.added_at ASC
       )                                           AS videos
     FROM   dup_keys dk
     JOIN   yt_registry r
       ON   ${groupExpr} = dk.dup_key
     ${filterClause}
     GROUP  BY dk.dup_key
     ORDER  BY count DESC, dk.dup_key ASC
     LIMIT  $1 OFFSET $2`,
    params
  );

  // Total count (for pagination)
  const countParams = filter ? [`%${filter}%`] : [];
  const countFilter = filter ? `AND (filename ILIKE $1 OR title ILIKE $1)` : '';
  const { rows: [{ n: total }] } = await db.query(
    `SELECT COUNT(*)::int AS n
     FROM (
       SELECT ${groupExpr} AS dup_key
       FROM   yt_registry
       WHERE  ${groupExpr} != ''
       ${countFilter}
       GROUP  BY dup_key
       HAVING COUNT(*) > 1
     ) sub`,
    countParams
  );

  return { rows, total };
}

// ─── GET /duplicates ──────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const { page, limit, offset } = parsePage(req.query);
    const basis  = ['filename', 'title', 'both'].includes(req.query.basis)
      ? req.query.basis : 'both';

    const { rows, total } = await fetchDuplicateGroups(basis, null, limit, offset);

    res.json({
      page,
      limit,
      total_groups: total,
      pages:        Math.ceil(total / limit),
      basis,
      quota_cost:   0,
      groups:       rows,
    });
  } catch (err) {
    console.error('[duplicates]', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /duplicates/search ───────────────────────────────────────────────────
router.get('/search', async (req, res) => {
  const q = (req.query.q ?? '').trim();
  if (!q) return res.status(400).json({ error: 'q param is required' });

  try {
    const { page, limit, offset } = parsePage(req.query);
    const basis = ['filename', 'title', 'both'].includes(req.query.basis)
      ? req.query.basis : 'both';

    const { rows, total } = await fetchDuplicateGroups(basis, q, limit, offset);

    res.json({
      page,
      limit,
      total_groups: total,
      pages:        Math.ceil(total / limit),
      query:        q,
      basis,
      quota_cost:   0,
      groups:       rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /duplicates/resolve ─────────────────────────────────────────────────
// Body: { canonical_video_id: string, duplicate_video_ids: string[] }
// Sets canonical=TRUE on the chosen video, FALSE on the listed duplicates.
// This is a local-only bookkeeping operation — nothing is deleted or pushed.
router.post('/resolve', async (req, res) => {
  const { canonical_video_id, duplicate_video_ids } = req.body ?? {};

  if (!canonical_video_id) {
    return res.status(400).json({ error: 'canonical_video_id is required' });
  }
  if (!Array.isArray(duplicate_video_ids) || duplicate_video_ids.length === 0) {
    return res.status(400).json({ error: 'duplicate_video_ids[] is required' });
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    // Mark the canonical
    await client.query(
      `UPDATE yt_registry SET canonical = TRUE WHERE video_id = $1`,
      [canonical_video_id]
    );

    // Mark the duplicates as non-canonical
    await client.query(
      `UPDATE yt_registry SET canonical = FALSE WHERE video_id = ANY($1::text[])`,
      [duplicate_video_ids]
    );

    // Persist a resolution note on the canonical's meta record
    await client.query(
      `INSERT INTO yt_video_meta (video_id, notes, last_local_edit)
       VALUES ($1, $2, NOW())
       ON CONFLICT (video_id) DO UPDATE
         SET notes           = EXCLUDED.notes,
             last_local_edit = NOW()`,
      [
        canonical_video_id,
        `Canonical of duplicate group. Non-canonical: ${duplicate_video_ids.join(', ')}`,
      ]
    );

    await client.query('COMMIT');

    res.json({
      status:              'resolved',
      canonical_video_id,
      marked_non_canonical: duplicate_video_ids,
      quota_cost:           0,
      message: 'Canonical flag updated locally. Use PATCH /meta/:id/youtube to push privacy changes.',
    });
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

module.exports = router;
