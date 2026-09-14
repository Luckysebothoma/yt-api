import { Pool } from 'pg';
import 'dotenv/config';

function requireEnv(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;

  if (value === undefined || value === null || value.toString().trim() === '') {
    throw new Error(`Missing or empty environment variable: ${name}`);
  }

  return value.toString().trim();
}

function requireNumberEnv(name: string, fallback?: number): number {
  const raw = process.env[name];

  const value =
    raw === undefined || raw.trim() === ''
      ? fallback
      : Number(raw);

  if (value === undefined || value === null || Number.isNaN(value)) {
    throw new Error(`Invalid or missing numeric environment variable: ${name}`);
  }

  return value;
}

export const db = new Pool({
  host: requireEnv('POSTGRES_HOST'),
  port: requireNumberEnv('POSTGRES_PORT', 5432),
  database: requireEnv('POSTGRES_DB'),
  user: requireEnv('POSTGRES_USER'),
  password: requireEnv('POSTGRES_PASSWORD'),
});