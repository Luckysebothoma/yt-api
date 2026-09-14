import { google, youtube_v3 } from 'googleapis';

export class YouTubeClient {
  private yt: youtube_v3.Youtube;

  constructor() {
    const auth = new google.auth.OAuth2(
      process.env.YT_CLIENT_ID,
      process.env.YT_CLIENT_SECRET,
      process.env.YT_REDIRECT_URI,
    );

    auth.setCredentials({
      refresh_token: process.env.YT_REFRESH_TOKEN,
    });

    this.yt = google.youtube({
      version: 'v3',
      auth,
    });
  }

  // ─────────────────────────────────────────────
  // BASIC CONNECTION TEST (IMPORTANT)
  // ─────────────────────────────────────────────
  async testConnection() {
    const res = await this.yt.channels.list({
      part: ['snippet', 'statistics'],
      mine: true,
      maxResults: 1,
    });

    const channel = res.data.items?.[0];

    if (!channel) {
      throw new Error('YouTube auth failed: no channel returned');
    }

    return {
      status: 'ok',
      channelId: channel.id,
      title: channel.snippet?.title,
      stats: channel.statistics,
    };
  }

  // ─────────────────────────────────────────────
  // LIST VIDEOS (reuse everywhere)
  // ─────────────────────────────────────────────
  async listMyVideos(maxResults = 50) {
    const res = await this.yt.search.list({
      part: ['snippet'],
      forMine: true,
      type: ['video'],
      maxResults,
    });

    return (res.data.items ?? []).map(v => ({
      id: v.id?.videoId,
      title: v.snippet?.title,
      url: `https://youtu.be/${v.id?.videoId}`,
    }));
  }

  // ─────────────────────────────────────────────
  // UPLOAD VIDEO (shared for worker3)
  // ─────────────────────────────────────────────
  async uploadVideo(params: {
    fileStream: any;
    title: string;
    description?: string;
  }) {
    const res = await this.yt.videos.insert({
      part: ['snippet', 'status'],
      requestBody: {
        snippet: {
          title: params.title,
          description: params.description ?? '',
        },
        status: {
          privacyStatus: 'unlisted',
          selfDeclaredMadeForKids: false,
        },
      },
      media: {
        body: params.fileStream,
      },
    });

    return {
      videoId: res.data.id,
      url: `https://youtu.be/${res.data.id}`,
    };
  }
}
