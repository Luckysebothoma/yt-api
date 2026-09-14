import { Job } from 'bullmq';
import { google } from 'googleapis';
import { Logger } from '../lib/logger';

export async function handleYouTubeTest(job: Job): Promise<object> {
  const log = new Logger('worker2:yt-test', job);

  await log.info('Testing YouTube OAuth connection...');

  const auth = new google.auth.OAuth2(
    process.env.YT_CLIENT_ID,
    process.env.YT_CLIENT_SECRET,
    process.env.YT_REDIRECT_URI,
  );

  auth.setCredentials({
    refresh_token: process.env.YT_REFRESH_TOKEN,
  });

  const yt = google.youtube({ version: 'v3', auth });

  try {
    // Lightweight API call (no upload, just identity check)
    const res = await yt.channels.list({
      part: ['snippet'],
      mine: true,
      maxResults: 1,
    });

    const channel = res.data.items?.[0];

    if (!channel) {
      throw new Error('No channel returned — invalid auth');
    }

    await log.info(`Connected to channel: ${channel.snippet?.title}`);

    return {
      status: 'ok',
      channelId: channel.id,
      channelTitle: channel.snippet?.title,
    };
  } catch (err: any) {
    await log.error(`YouTube connection failed: ${err.message}`);
    throw err;
  }
}
