import { Hono } from "hono";
import { google } from "googleapis";

const youtube = new Hono();

// ─── OAuth2 Client ────────────────────────────────────────────────────────────

function getOAuthClient() {
  const client = new google.auth.OAuth2(
    process.env.YT_CLIENT_ID,
    process.env.YT_CLIENT_SECRET,
    "https://developers.google.com/oauthplayground"
  );
  client.setCredentials({ refresh_token: process.env.YT_REFRESH_TOKEN });
  return client;
}

function getYouTube() {
  return google.youtube({ version: "v3", auth: getOAuthClient() });
}

// ─── Channel ──────────────────────────────────────────────────────────────────

// GET /youtube/channel
youtube.get("/channel", async (c) => {
  const yt = getYouTube();
  const res = await yt.channels.list({
    part: ["snippet", "statistics", "contentDetails"],
    mine: true,
  });
  return c.json(res.data.items?.[0] ?? {});
});

// ─── Videos ───────────────────────────────────────────────────────────────────

// GET /youtube/videos?maxResults=10&pageToken=xxx
youtube.get("/videos", async (c) => {
  const yt = getYouTube();
  const maxResults = Number(c.req.query("maxResults") ?? 10);
  const pageToken = c.req.query("pageToken");

  const res = await yt.search.list({
    part: ["snippet"],
    forMine: true,
    type: ["video"],
    maxResults,
    ...(pageToken && { pageToken }),
  });

  return c.json({
    nextPageToken: res.data.nextPageToken,
    items: res.data.items,
  });
});

// GET /youtube/videos/:id
youtube.get("/videos/:id", async (c) => {
  const yt = getYouTube();
  const id = c.req.param("id");

  const res = await yt.videos.list({
    part: ["snippet", "statistics", "status", "contentDetails"],
    id: [id],
  });

  return c.json(res.data.items?.[0] ?? {});
});

// POST /youtube/videos/upload
// body: { title, description, tags[], categoryId, privacyStatus, videoBase64, mimeType }
youtube.post("/videos/upload", async (c) => {
  const yt = getYouTube();
  const body = await c.req.json();

  const { title, description, tags, categoryId, privacyStatus, videoBase64, mimeType } = body;

  const buffer = Buffer.from(videoBase64, "base64");
  const { Readable } = await import("stream");
  const stream = Readable.from(buffer);

  const res = await yt.videos.insert({
    part: ["snippet", "status"],
    requestBody: {
      snippet: { title, description, tags, categoryId: categoryId ?? "22" },
      status: { privacyStatus: privacyStatus ?? "private" },
    },
    media: { mimeType: mimeType ?? "video/mp4", body: stream },
  });

  return c.json(res.data, 201);
});

// PATCH /youtube/videos/:id
// body: { title?, description?, tags?, privacyStatus? }
youtube.patch("/videos/:id", async (c) => {
  const yt = getYouTube();
  const id = c.req.param("id");
  const body = await c.req.json();

  const res = await yt.videos.update({
    part: ["snippet", "status"],
    requestBody: {
      id,
      snippet: {
        title: body.title,
        description: body.description,
        tags: body.tags,
        categoryId: body.categoryId ?? "22",
      },
      status: { privacyStatus: body.privacyStatus },
    },
  });

  return c.json(res.data);
});

// DELETE /youtube/videos/:id
youtube.delete("/videos/:id", async (c) => {
  const yt = getYouTube();
  const id = c.req.param("id");
  await yt.videos.delete({ id });
  return c.json({ deleted: id });
});

// ─── Thumbnails ───────────────────────────────────────────────────────────────

// POST /youtube/videos/:id/thumbnail
// body: { imageBase64, mimeType }
youtube.post("/videos/:id/thumbnail", async (c) => {
  const yt = getYouTube();
  const id = c.req.param("id");
  const { imageBase64, mimeType } = await c.req.json();

  const buffer = Buffer.from(imageBase64, "base64");
  const { Readable } = await import("stream");

  const res = await yt.thumbnails.set({
    videoId: id,
    media: { mimeType: mimeType ?? "image/jpeg", body: Readable.from(buffer) },
  });

  return c.json(res.data);
});

// ─── Playlists ────────────────────────────────────────────────────────────────

// GET /youtube/playlists
youtube.get("/playlists", async (c) => {
  const yt = getYouTube();
  const res = await yt.playlists.list({
    part: ["snippet", "contentDetails"],
    mine: true,
    maxResults: 50,
  });
  return c.json(res.data.items);
});

// POST /youtube/playlists
// body: { title, description, privacyStatus }
youtube.post("/playlists", async (c) => {
  const yt = getYouTube();
  const { title, description, privacyStatus } = await c.req.json();

  const res = await yt.playlists.insert({
    part: ["snippet", "status"],
    requestBody: {
      snippet: { title, description },
      status: { privacyStatus: privacyStatus ?? "public" },
    },
  });

  return c.json(res.data, 201);
});

// POST /youtube/playlists/:playlistId/items
// body: { videoId, position? }
youtube.post("/playlists/:playlistId/items", async (c) => {
  const yt = getYouTube();
  const playlistId = c.req.param("playlistId");
  const { videoId, position } = await c.req.json();

  const res = await yt.playlistItems.insert({
    part: ["snippet"],
    requestBody: {
      snippet: {
        playlistId,
        resourceId: { kind: "youtube#video", videoId },
        ...(position !== undefined && { position }),
      },
    },
  });

  return c.json(res.data, 201);
});

// DELETE /youtube/playlists/:id
youtube.delete("/playlists/:id", async (c) => {
  const yt = getYouTube();
  await yt.playlists.delete({ id: c.req.param("id") });
  return c.json({ deleted: c.req.param("id") });
});

// ─── Comments ─────────────────────────────────────────────────────────────────

// GET /youtube/videos/:id/comments?maxResults=20
youtube.get("/videos/:id/comments", async (c) => {
  const yt = getYouTube();
  const res = await yt.commentThreads.list({
    part: ["snippet"],
    videoId: c.req.param("id"),
    maxResults: Number(c.req.query("maxResults") ?? 20),
  });
  return c.json(res.data.items);
});

// POST /youtube/videos/:id/comments
// body: { text }
youtube.post("/videos/:id/comments", async (c) => {
  const yt = getYouTube();
  const { text } = await c.req.json();

  const res = await yt.commentThreads.insert({
    part: ["snippet"],
    requestBody: {
      snippet: {
        videoId: c.req.param("id"),
        topLevelComment: { snippet: { textOriginal: text } },
      },
    },
  });

  return c.json(res.data, 201);
});

// ─── Analytics (basic) ────────────────────────────────────────────────────────

// GET /youtube/analytics?startDate=2024-01-01&endDate=2024-12-31&metrics=views,likes
youtube.get("/analytics", async (c) => {
  const auth = getOAuthClient();
  const ytAnalytics = google.youtubeAnalytics({ version: "v2", auth });

  const startDate = c.req.query("startDate") ?? "2024-01-01";
  const endDate = c.req.query("endDate") ?? new Date().toISOString().split("T")[0];
  const metrics = c.req.query("metrics") ?? "views,likes,dislikes,comments,shares";

  const res = await ytAnalytics.reports.query({
    ids: "channel==MINE",
    startDate,
    endDate,
    metrics,
    dimensions: "day",
    sort: "day",
  });

  return c.json(res.data);
});

export default youtube;