import { Job } from 'bullmq';
import fs from 'fs/promises';
import { createReadStream, existsSync } from 'fs';
import path from 'path';
import crypto from 'crypto';
import { Logger } from '../lib/logger';

interface ArrangePayload {
  targetDir: string;
  dryRun: boolean;
  mediaType: 'images' | 'videos' | 'both';
}

const VIDEO_EXT = new Set(['.mp4', '.mkv', '.mov', '.avi', '.flv', '.wmv']);
const IMAGE_EXT = new Set(['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']);

// How often to renew the lock (ms). Must be less than lockDuration in your Worker config.
// If your Worker is configured with lockDuration: 30_000, renew every 15s.
const LOCK_RENEW_INTERVAL_MS = 15_000;

async function getFastHash(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5');
    const stream = createReadStream(filePath, { start: 0, end: 5 * 1024 * 1024 });
    stream.on('data', chunk => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

function extractBaseName(fname: string): string {
  const noExt = fname.replace(/\.[^.]+$/, '');
  const m = noExt.match(/^(\d{8}_\d{6})/);
  return m ? m[1] : (noExt.replace(/_[^_]+$/, '') || 'undated');
}

export async function handleArrange(job: Job): Promise<object> {
  const { targetDir, dryRun, mediaType } = job.data as ArrangePayload;
  const log = new Logger('worker1:arrange', job);
  const stats = { moved: 0, duplicates: 0, errors: 0 };
  const seenHashes = new Set<string>();

  if (!existsSync(targetDir)) throw new Error(`Path does not exist: ${targetDir}`);

  // ✅ FIX 1: Time-based lock renewal running in parallel with the file work.
  // This is more reliable than counting files, because a single 10GB move
  // can stall for minutes with no file-count ticks.
  let lockRenewActive = true;
  const lockRenewer = (async () => {
    while (lockRenewActive) {
      await new Promise(resolve => setTimeout(resolve, LOCK_RENEW_INTERVAL_MS));
      if (!lockRenewActive) break;
      try {
        await job.extendLock(job.token!, LOCK_RENEW_INTERVAL_MS * 4);
        await log.info(`Lock renewed (moved so far: ${stats.moved})`);
      } catch (err) {
        // Log but don't throw — job may still complete successfully
        await log.error(`Lock renewal failed (non-fatal): ${err}`);
      }
    }
  })();

  async function processEntry(currentPath: string) {
    const entries = await fs.readdir(currentPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        if (entry.name === 'grouped') continue;
        await processEntry(fullPath);
        continue;
      }

      const ext = path.extname(entry.name).toLowerCase();
      const isVideo = VIDEO_EXT.has(ext);
      const isImage = IMAGE_EXT.has(ext);
      if (mediaType === 'videos' && !isVideo) continue;
      if (mediaType === 'images' && !isImage) continue;
      if (mediaType === 'both' && !isVideo && !isImage) continue;

      try {
        const hash = await getFastHash(fullPath);
        const isDup = seenHashes.has(hash);
        seenHashes.add(hash);

        const baseName = extractBaseName(entry.name);
        const targetFolder = isDup
          ? path.join(targetDir, 'grouped', baseName, '.duplicates')
          : path.join(targetDir, 'grouped', baseName);
        const destPath = path.join(targetFolder, entry.name);

        // ✅ FIX 2: fs.rename() fails silently when src and dst are on different
        // filesystems/mount points (EXDEV error). Fall back to copy+delete for
        // cross-device moves, which is common when /media is a separate mount.
        if (!dryRun) {
          await fs.mkdir(targetFolder, { recursive: true });
          try {
            await fs.rename(fullPath, destPath);
          } catch (err: any) {
            if (err.code === 'EXDEV') {
              // Cross-device: stream copy then delete original
              await streamCopy(fullPath, destPath);
              await fs.unlink(fullPath);
            } else {
              throw err;
            }
          }
        }

        stats.moved++;
        if (isDup) stats.duplicates++;

        // Progress update every 5 files (cosmetic only — lock renewal is handled above)
        if (stats.moved % 5 === 0) {
          await job.updateProgress({ count: stats.moved, lastFile: entry.name });
        }

      } catch (err) {
        stats.errors++;
        await log.error(`Failed to process ${entry.name}: ${err}`);
      }
    }
  }

  try {
    await log.info(`Starting arrange: dir=${targetDir} dryRun=${dryRun} type=${mediaType}`);
    await processEntry(targetDir);
  } finally {
    // ✅ Always stop the lock renewer, even if processEntry threw
    lockRenewActive = false;
    await lockRenewer;
  }

  return { summary: 'Success', ...stats, dryRun };
}

// Streams a file from src to dst. Safe for large files — no full RAM load.
async function streamCopy(src: string, dst: string): Promise<void> {
  const { createReadStream, createWriteStream } = await import('fs');
  await new Promise<void>((resolve, reject) => {
    const rd = createReadStream(src);
    const wr = createWriteStream(dst);
    rd.on('error', reject);
    wr.on('error', reject);
    wr.on('finish', resolve);
    rd.pipe(wr);
  });
}