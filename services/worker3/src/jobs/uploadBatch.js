'use strict';
require('dotenv/config');
const fs   = require('fs');
const path = require('path');
const { Logger }       = require('../lib/logger');
const { uploadVideo }  = require('./uploadSingle');
const { syncRegistry, pgUpsertBatch, check_if_filename_exists_in_registry, redisAppend } = require('../lib/registry');
const { isQuotaExhausted, getQuotaUsed, QUOTA_DAILY_LIMIT } = require('../lib/quota');

const VIDEO_EXT = new Set(['.mp4', '.mkv', '.mov', '.avi', '.flv', '.wmv']);

async function handleBatch(job) {
  const log = new Logger('worker3:batch', job);
  const { groupedDir, batchSize = 15, forceRegistrySync = false } = job.data;

  await log.info(`━━ Batch upload start ━━ dir=${groupedDir} batchSize=${batchSize}`);

  if (!process.env.YT_CLIENT_ID || !process.env.YT_REFRESH_TOKEN) {
    await log.error('Missing YT credentials in environment');
    throw new Error('Missing YT credentials');
  }

  if (!fs.existsSync(groupedDir)) {
    await log.error(`groupedDir not found: ${groupedDir}`);
    throw new Error(`groupedDir not found: ${groupedDir}`);
  }

  const entries    = fs.readdirSync(groupedDir, { withFileTypes: true });
  const videoFiles = entries
    .filter(e => e.isFile() && VIDEO_EXT.has(path.extname(e.name).toLowerCase()))
    .map(e => path.join(groupedDir, e.name))
    .slice(0, batchSize);

  await log.info(`Videos to upload: ${videoFiles.length}`);
  if (!videoFiles.length) {
    return { results: [], skipped: 0, uploaded: 0 };
  }

  const readable = videoFiles.filter(f => {
    try { fs.accessSync(f, fs.constants.R_OK); return true; } catch { return false; }
  });

  await log.info('Resolving registry (Redis → PG → YouTube API)...');
  let registry;
  try {
    registry = await syncRegistry(job.id, log, { forceSync: forceRegistrySync });
  } catch (err) {
    if (err.message === 'QUOTA_EXHAUSTED') {
      return { results: [], skipped: videoFiles.length, uploaded: 0, abortReason: 'quota_exhausted' };
    }
    throw err;
  }

  const pid      = process.env.YT_UPLOADS_PLAYLIST_ID ?? 'unknown';
  const results  = [];
  let uploaded   = 0;
  let skipped    = 0;

  for (const file of readable) {
    if (await isQuotaExhausted()) {
      await log.error(`Quota exhausted mid-batch (${await getQuotaUsed()}/${QUOTA_DAILY_LIMIT}) — stopping`);
      results.push({ file, status: 'skipped', reason: 'quota_exhausted' });
      skipped++;
      continue;
    }

    try {
      const filename = path.basename(file);

      // Check the registry BEFORE uploading. Checking afterwards (the old
      // behaviour) meant the file had already been streamed to YouTube and
      // written into yt_registry by the time we found out it was a
      // duplicate — burning quota and creating a second video on the
      // channel for something we were about to skip anyway.
      const filenameExists = await check_if_filename_exists_in_registry(pid, filename);

      if (filenameExists) {
        await log.warn(`Filename ${filename} already exists in registry — skipping upload (no YouTube call made)`);
        // move and tag the file as skipped to prevent re-processing in future batches
        const skippedDir = path.join(groupedDir, 'skipped');
        if (!fs.existsSync(skippedDir)) {
          // DO nothing now
//          fs.mkdirSync(skippedDir);
        }
        const destPath = path.join(skippedDir, `${path.basename(file, path.extname(file))}_skipped${path.extname(file)}`);
          // DO not rename now 
        //      fs.renameSync(file, destPath);
        console.log(`Moved ${file} → ${destPath}`);

        results.push({ file, status: 'skipped', reason: 'filename_exists' });
        skipped++;
        continue;
      }

      const r = await uploadVideo(file, path.basename(file, path.extname(file)), log);
      const videoId = r.videoId;

      // 1. In-memory registry
      registry.add(videoId);

      // 2. PG already written by uploadVideo — but ensure filename is stored
      await pgUpsertBatch(pid, [{ videoId, title: r.title ?? filename, filename }]);

      // move file to "uploaded" subdir
      // tag filename_<videoId> with videoId to prevent collisions and for easier tracing
      // add videoId to filename to prevent collisions and for easier tracing
      // hide it, so app wont process it again if the batch is re-run

        try{

          const uploadedDir = path.join(groupedDir, 'uploaded');
        if (!fs.existsSync(uploadedDir)) {
          // DO nothing now
//// Do no
//           fs.mkdirSync(uploadedDir);
        }
        const destPath = path.join(uploadedDir, `${path.basename(file, path.extname(file))}_${videoId}${path.extname(file)}`);
     
        //  fs.renameSync(file, destPath);
        console.log(`Moved ${file} → ${destPath}`);

        //hide the file so that it is not processed again if the batch is re-run
        //fs.chmodSync(destPath, 0o000);
        console.log(`Hid ${destPath}`);
        }catch(err){
          await log.error(`Failed to move file ${file} after upload: ${err.message}`);
          return { results: [], skipped: videoFiles.length, uploaded: 0, abortReason: 'file_move_failed' };

        }

        


      // 3. Incremental Redis update — delegated to the registry service so
      //    there's one place that knows the cache key shape and TTL.
      await redisAppend(pid, videoId);

      results.push({ file, ...r, status: 'ok' });
      uploaded++;

        } catch (err) {
    console.error(err);
    console.error(err.stack);
    results.push({ file, status: 'error', error: err.message });
      /*if (
        err.message.includes('exceeded the number of videos') ||
        err.message.includes('The request cannot be completed because you have exceeded')
      ) {
        await log.error('Quota exhausted during upload — stopping batch');
        break;
      }

      */

          // We now will check why its catch but we will need to shut it down
          // for any error app need to shutdown, swarm will restart and new process will be sent
          if (
            err.message.includes('exceeded the number of videos') ||
            err.message.includes('The request cannot be completed because you have exceeded') ||
            err.message.includes('Quota exceeded')
          ) {
            await log.error('Quota exhausted during upload — stopping batch');
            // shutdown the process so that swarm will restart it and new process will be sent
            process.exit(1);
          } else {
            await log.error(`Error uploading ${file}: ${err.message}`);
          }


    }
  }

  await log.info(`Batch complete: ${uploaded} uploaded, ${skipped} skipped, ${results.filter(r => r.status === 'error').length} errors`);
  return { results, uploaded, skipped };
}

module.exports = { handleBatch };
