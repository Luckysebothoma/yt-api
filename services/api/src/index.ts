import 'dotenv/config';
import { serve }         from '@hono/node-server';
import { Hono }          from 'hono';
import { viewRouter }    from './routes/view';
import { arrangeRouter } from './routes/arrange';
import { uploadRouter }  from './routes/upload';
import { docsRouter }    from './routes/docs';
import { healthRouter }  from './lib/health';
import { checkRedis, checkPostgres, checkYouTube } from './lib/healthChecks';
import { Logger }        from './lib/logger';
import youtube from './routes/youtube'; 

const app = new Hono();
const log = new Logger('api');

app.route("/youtube", youtube);
app.route('/view',    viewRouter);
app.route('/arrange', arrangeRouter);
app.route('/upload',  uploadRouter);
app.route('/docs',    docsRouter);
app.route('/health',  healthRouter);

// ─── Periodic health poll ─────────────────────────────────────────────────────
// Runs every 60s so logs in Redis always reflect current infra state.
// Does NOT block startup — fires independently in the background.

const POLL_INTERVAL_MS = 60_000;

async function runHealthPoll() {
  const [redis, postgres, youtube] = await Promise.allSettled([
    checkRedis(),
    checkPostgres(),
    checkYouTube(),
  ]);

  const unwrap = (r: PromiseSettledResult<any>) =>
    r.status === 'fulfilled' ? r.value : { ok: false, error: (r as any).reason?.message };

  const r = {
    redis:    unwrap(redis),
    postgres: unwrap(postgres),
    youtube:  unwrap(youtube),
  };

  const allOk = r.redis.ok && r.postgres.ok && r.youtube.ok;

  if (allOk) {
    await log.info(`[health-poll] All systems OK — redis=${r.redis.latencyMs}ms pg=${r.postgres.latencyMs}ms yt=authed`);
  } else {
    if (!r.redis.ok)    await log.error(`[health-poll] Redis DEGRADED: ${r.redis.error}`);
    if (!r.postgres.ok) await log.error(`[health-poll] Postgres DEGRADED: ${r.postgres.error}`);
    if (!r.youtube.ok)  await log.warn(`[health-poll] YouTube DEGRADED: ${r.youtube.error}`);
  }
}

function startHealthPoller() {
  // First poll 10s after boot — gives Redis/Postgres time to be ready
  setTimeout(async () => {
    await runHealthPoll();
    setInterval(runHealthPoll, POLL_INTERVAL_MS);
  }, 10_000);
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
serve({ fetch: app.fetch, port: 3000 }, () => {
  console.log('[API] Listening on :3000');
  startHealthPoller();
});