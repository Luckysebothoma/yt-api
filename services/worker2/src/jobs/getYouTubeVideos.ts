import { Job } from 'bullmq';
import { google } from 'googleapis';
import { Logger } from '../lib/logger';

export async function handleGetYTVideos(job: Job): Promise<object> {
  const log = new Logger('worker2:yt-list', job);
  await log.info('Fetching YouTube video list');

  const auth = new google.auth.OAuth2(
    process.env.YT_CLIENT_ID,
    process.env.YT_CLIENT_SECRET,
    process.env.YT_REDIRECT_URI,
  );
  auth.setCredentials({ refresh_token: process.env.YT_REFRESH_TOKEN });

  const yt      = google.youtube({ version: 'v3', auth });
  const res     = await yt.search.list({ part: ['snippet'], forMine: true, type: ['video'], maxResults: 50 });
  const videos  = (res.data.items ?? []).map(item => ({
    id:       item.id?.videoId,
    title:    item.snippet?.title,
    url:      `https://youtu.be/${item.id?.videoId}`,
    published: item.snippet?.publishedAt,
  }));

  await log.info(`Fetched ${videos.length} videos from YouTube`);
  return { videos };
}
