export const db = new Pool({
  host:     process.env.POSTGRES_HOST,
  port:     Number(process.env.POSTGRES_PORT ?? 5432),
  database: process.env.POSTGRES_DB,
  user:     process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  max: 5,
  statement_timeout: 20_000,
  idleTimeoutMillis: 30_000,
});
