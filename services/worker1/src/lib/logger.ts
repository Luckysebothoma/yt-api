import { Job } from 'bullmq';

// Logs go to BullMQ job log (Redis) — never to disk.
// Outside a job context (API), logs go to stdout only.
export class Logger {
  private job?: Job;
  private service: string;

  constructor(service: string, job?: Job) {
    this.service = service;
    this.job = job;
  }

  private fmt(level: string, msg: string): string {
    return `[${new Date().toISOString()}] [${this.service}] [${level}] ${msg}`;
  }

  async info(msg: string) {
    const line = this.fmt('INFO', msg);
    console.log(line);
    if (this.job) await this.job.log(line);
  }

  async warn(msg: string) {
    const line = this.fmt('WARN', msg);
    console.warn(line);
    if (this.job) await this.job.log(line);
  }

  async error(msg: string) {
    const line = this.fmt('ERROR', msg);
    console.error(line);
    if (this.job) await this.job.log(line);
  }
}
