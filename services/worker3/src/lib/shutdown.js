'use strict';

/**
 * Tiny shutdown coordinator (NEW FILE).
 *
 * - index.js registers the real shutdown routine via setShutdownHandler().
 * - Anything (signal handlers, the batch job on quota exhaustion) calls
 *   requestShutdown(); the handler runs once.
 * - Job code polls isShuttingDown() between files so it stops starting new
 *   uploads while the in-flight one finishes.
 */

let handler   = null;
let requested = false;

function setShutdownHandler(fn) {
  handler = fn;
}

function isShuttingDown() {
  return requested;
}

function requestShutdown(reason, code = 0) {
  if (requested) return;
  requested = true;
  if (handler) {
    // Run on next tick so callers (e.g. a running job) are never blocked or
    // deadlocked waiting on the shutdown routine that waits on them.
    setImmediate(() => handler(reason, code));
  } else {
    process.exit(code);
  }
}

module.exports = { setShutdownHandler, isShuttingDown, requestShutdown };
