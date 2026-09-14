// services/worker2/src/lib/ytHealth.ts
// Isolated YouTube API connectivity checker.
// Run standalone:  npx tsx src/lib/ytHealth.ts
// Or import checkYouTube() for use in health endpoints / startup guards.

import 'dotenv/config';
import { google } from 'googleapis';

export interface YTHealthResult {
  ok:          boolean;
  authed:      boolean;
  channelId?:  string;
  channel?:    string;
  quota?:      string;
  error?:      string;
  checkedAt:   string;
}

function buildAuth() {
  const missing: string[] = [];
  if (!process.env.YT_CLIENT_ID)     missing.push('YT_CLIENT_ID');
  if (!process.env.YT_CLIENT_SECRET) missing.push('YT_CLIENT_SECRET');
  if (!process.env.YT_REDIRECT_URI)  missing.push('YT_REDIRECT_URI');
  if (!process.env.YT_REFRESH_TOKEN) missing.push('YT_REFRESH_TOKEN');

  if (missing.length > 0) {
    throw new Error(`Missing env vars: ${missing.join(', ')}`);
  }

  const auth = new google.auth.OAuth2(
    process.env.YT_CLIENT_ID,
    process.env.YT_CLIENT_SECRET,
    process.env.YT_REDIRECT_URI,
  );
  auth.setCredentials({ refresh_token: process.env.YT_REFRESH_TOKEN });
  return auth;
}

// ─── Main check ──────────────────────────────────────────────────────────────
// Performs 3 checks in order:
//   1. Env vars present
//   2. Token exchange (OAuth2 refresh)
//   3. channels.list — confirms quota + identity
export async function checkYouTube(): Promise<YTHealthResult> {
  const checkedAt = new Date().toISOString();

  // 1. Env validation
  let auth: ReturnType<typeof buildAuth>;
  try {
    auth = buildAuth();
  } catch (err: any) {
    return { ok: false, authed: false, error: err.message, checkedAt };
  }

  // 2. Token refresh — confirms client_id / secret / refresh_token are valid
  try {
    await auth.getAccessToken();
  } catch (err: any) {
    return {
      ok:     false,
      authed: false,
      error:  `OAuth token refresh failed: ${err.message}`,
      checkedAt,
    };
  }

  // 3. API call — channels.list(mine) confirms quota + scope
  try {
    const yt  = google.youtube({ version: 'v3', auth });
    const res = await yt.channels.list({ part: ['snippet'], mine: true });
    const ch  = res.data.items?.[0];

    if (!ch) {
      return {
        ok:     false,
        authed: true,
        error:  'Token valid but no channel found — check OAuth scope includes youtube.readonly',
        checkedAt,
      };
    }

    return {
      ok:        true,
      authed:    true,
      channelId: ch.id ?? undefined,
      channel:   ch.snippet?.title ?? undefined,
      quota:     'ok',
      checkedAt,
    };
  } catch (err: any) {
    // Distinguish quota exhaustion from other API errors
    const isQuota = err?.code === 403 &&
      JSON.stringify(err).toLowerCase().includes('quotaexceeded');

    return {
      ok:     false,
      authed: true,
      quota:  isQuota ? 'exhausted' : 'unknown',
      error:  isQuota
        ? 'YouTube API quota exhausted — resets at midnight Pacific'
        : `API call failed: ${err.message}`,
      checkedAt,
    };
  }
}

// ─── Standalone runner ────────────────────────────────────────────────────────
async function main() {

  if (!process.env.YT_REFRESH_TOKEN?.startsWith("1//")) {
  throw new Error("Invalid refresh token format");
}
  console.log('[ YouTube Health Check ]');
  console.log('────────────────────────');
  const result = await checkYouTube();

  const lines: [string, string][] = [
    ['Status',    result.ok      ? '✔ OK'     : '✘ FAILED'],
    ['Authed',    result.authed  ? '✔ yes'    : '✘ no'],
    ['Channel',   result.channel  ?? '—'],
    ['ChannelId', result.channelId ?? '—'],
    ['Quota',     result.quota    ?? '—'],
    ['Error',     result.error    ?? '—'],
    ['CheckedAt', result.checkedAt],
  ];

  for (const [k, v] of lines) {
    console.log(`  ${k.padEnd(12)}: ${v}`);
  }

  console.log('────────────────────────');
  process.exit(result.ok ? 0 : 1);
}

// Only runs when executed directly — safe to import in other modules
if (require.main === module) main();
