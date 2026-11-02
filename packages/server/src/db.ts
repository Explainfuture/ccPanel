import pg from 'pg';
export const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 4, connectionTimeoutMillis: 5000, idleTimeoutMillis: 10000, statement_timeout: 5000 });
pool.on('error', () => console.error(JSON.stringify({ code: 'database_pool_error' })));
export const query = <T extends pg.QueryResultRow = pg.QueryResultRow>(text: string, values: unknown[] = []) => pool.query<T>(text, values);
