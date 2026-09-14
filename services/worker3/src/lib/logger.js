'use strict';

class Logger {
  constructor(service, job) {
    this.service = service;
    this.job     = job ?? null;
  }

  _fmt(level, msg) {
    return `[${new Date().toISOString()}] [${this.service}] [${level}] ${msg}`;
  }

  async info(msg)  { const l = this._fmt('INFO',  msg); console.log(l);   if (this.job) await this.job.log(l); }
  async warn(msg)  { const l = this._fmt('WARN',  msg); console.warn(l);  if (this.job) await this.job.log(l); }
  async error(msg) { const l = this._fmt('ERROR', msg); console.error(l); if (this.job) await this.job.log(l); }
}

module.exports = { Logger };
