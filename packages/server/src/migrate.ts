import { pool } from './db';
export async function migrate() {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query("SELECT pg_advisory_xact_lock(169202610)");
    await c.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations(version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS tenants(id uuid PRIMARY KEY, name text NOT NULL);
      CREATE TABLE IF NOT EXISTS users(id uuid PRIMARY KEY, email text UNIQUE NOT NULL, password_hash text NOT NULL);
      CREATE TABLE IF NOT EXISTS tenant_memberships(tenant_id uuid REFERENCES tenants(id), user_id uuid REFERENCES users(id), role text NOT NULL CHECK(role IN ('admin','analyst','viewer')), PRIMARY KEY(tenant_id,user_id));
      CREATE TABLE IF NOT EXISTS sessions(token_hash text PRIMARY KEY,user_id uuid NOT NULL REFERENCES users(id),csrf_hash text NOT NULL, expires_at timestamptz NOT NULL);
      CREATE TABLE IF NOT EXISTS projects(id uuid PRIMARY KEY,tenant_id uuid NOT NULL REFERENCES tenants(id),name text NOT NULL,timezone text NOT NULL DEFAULT 'Asia/Shanghai',origins text[] NOT NULL DEFAULT '{}',schema_mode text NOT NULL DEFAULT 'loose' CHECK(schema_mode IN ('loose','strict')),created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(tenant_id,id));
      CREATE TABLE IF NOT EXISTS write_keys(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,key_hash text UNIQUE NOT NULL,label text NOT NULL,revoked_at timestamptz,FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS event_schemas(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_name text NOT NULL,required_properties jsonb NOT NULL DEFAULT '{}',version integer NOT NULL DEFAULT 1,PRIMARY KEY(project_id,event_name),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS event_dedup(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,payload_hash text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS events(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,event_name text NOT NULL,distinct_id text NOT NULL,anonymous_id text,event_time timestamptz NOT NULL,received_at timestamptz NOT NULL,properties jsonb NOT NULL,context jsonb NOT NULL,schema_version integer NOT NULL,PRIMARY KEY(project_id,event_time,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id)) PARTITION BY RANGE(event_time);
      CREATE TABLE IF NOT EXISTS identity_links(tenant_id uuid NOT NULL,project_id uuid NOT NULL,anonymous_id text NOT NULL,user_id text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,anonymous_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS aggregation_jobs(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS audit_logs(id bigserial PRIMARY KEY,tenant_id uuid NOT NULL REFERENCES tenants(id),user_id uuid REFERENCES users(id),action text NOT NULL,details jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS ingest_metrics(tenant_id uuid NOT NULL,project_id uuid NOT NULL,metric text NOT NULL,value bigint NOT NULL DEFAULT 0,PRIMARY KEY(project_id,metric),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS dlq_records(id text PRIMARY KEY,tenant_id uuid,project_id uuid,reason text NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS reports(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,name text NOT NULL,spec jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS dashboards(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,name text NOT NULL,report_ids uuid[] NOT NULL DEFAULT '{}',updated_at timestamptz NOT NULL DEFAULT now(),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS events_scope_time ON events(tenant_id,project_id,event_time DESC);
      CREATE INDEX IF NOT EXISTS events_user_time ON events(tenant_id,project_id,distinct_id,event_time DESC);
      INSERT INTO schema_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(2) ON CONFLICT DO NOTHING;
    `);
    const now = new Date();
    for (let i = -2; i <= 2; i++) {
      const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1));
      const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i + 1, 1));
      const name = 'events_' + start.toISOString().slice(0, 7).replace('-', '_');
      await c.query(`CREATE TABLE IF NOT EXISTS ${name} PARTITION OF events FOR VALUES FROM ('${start.toISOString()}') TO ('${end.toISOString()}')`);
    }
    await c.query('COMMIT');
  } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
}
if (process.argv[1]?.includes('migrate')) {
  migrate().then(() => console.log('Database migrations complete')).finally(() => pool.end());
}
