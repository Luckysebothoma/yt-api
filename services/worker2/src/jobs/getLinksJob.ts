import { Job } from 'bullmq';
import { db } from '../lib/db';
import { Logger } from '../lib/logger';

export async function handleGetLinks(job: Job): Promise<object> {
  const log = new Logger('worker2:get-links', job);
  await log.info('Fetching YouTube links from DB');

  const { rows } = await db.query(
    'SELECT id, yt_url, yt_video_id, title, grouped_name, uploaded_at FROM videos ORDER BY uploaded_at DESC'
  );

  await log.info(`Returned ${rows.length} links`);
  return { links: rows };
}
