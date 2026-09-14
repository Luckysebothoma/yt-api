import { Job } from 'bullmq';
import fs from 'fs';
import path from 'path';
import { Logger } from '../lib/logger';

export async function handleListFiles(job: Job): Promise<object> {
  const log = new Logger('worker2:list-files', job);
  const mediaPath = job.data.mediaPath ?? process.env.MEDIA_PATH ?? '/media';
  await log.info(`Listing files in ${mediaPath}`);

  if (!fs.existsSync(mediaPath)) {
    await log.warn(`Media path not found: ${mediaPath}`);
    return { files: [], mediaPath };
  }

  function walk(dir: string, depth = 0): object[] {
    return fs.readdirSync(dir, { withFileTypes: true }).map(e => ({
      name:     e.name,
      type:     e.isDirectory() ? 'dir' : 'file',
      path:     path.join(dir, e.name),
      children: e.isDirectory() && depth < 3 ? walk(path.join(dir, e.name), depth + 1) : undefined,
    }));
  }

  const tree = walk(mediaPath);
  await log.info(`Listed ${tree.length} top-level entries`);
  return { mediaPath, tree };
}
