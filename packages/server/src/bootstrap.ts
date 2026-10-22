import { randomUUID } from 'node:crypto';
import { pool } from './db';
import { passwordHash } from './security';
const email = process.env.ADMIN_EMAIL;
const password = process.env.ADMIN_PASSWORD;
if (!email || !password || password.length < 12) throw new Error('Set ADMIN_EMAIL and ADMIN_PASSWORD (at least 12 characters)');
const c = await pool.connect();
try {
  await c.query('BEGIN');
  await c.query('SELECT pg_advisory_xact_lock(169202611)');
  const existing = await c.query('SELECT 1 FROM users LIMIT 1');
  if (existing.rowCount) throw new Error('Bootstrap is one-time only; users already exist');
  const user = randomUUID(), tenant = randomUUID();
  await c.query('INSERT INTO users VALUES($1,$2,$3)', [user,email.toLowerCase(),passwordHash(password)]);
  await c.query('INSERT INTO tenants VALUES($1,$2)', [tenant,process.env.COMPANY_NAME || '我的公司']);
  await c.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')", [tenant,user]);
  await c.query('COMMIT');
  console.log('Administrator and company created; no credentials printed');
} catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); await pool.end(); }
