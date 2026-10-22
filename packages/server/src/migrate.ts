import { pool } from './db';
export async function migrate() {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query("SELECT pg_advisory_xact_lock(169202610)");
    const integrityExists=!!(await c.query("SELECT to_regclass('public.ingest_integrity_gate') AS name")).rows[0].name;
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
      CREATE TABLE IF NOT EXISTS query_jobs(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES users(id),spec jsonb NOT NULL,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','succeeded','failed','cancelled')),result jsonb,error text,created_at timestamptz NOT NULL DEFAULT now(),started_at timestamptz,finished_at timestamptz,expires_at timestamptz NOT NULL DEFAULT now()+interval '24 hours',FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS query_jobs_queue ON query_jobs(created_at) WHERE status IN ('queued','running');
      CREATE TABLE IF NOT EXISTS deletion_jobs(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES users(id),scope text NOT NULL CHECK(scope IN ('user','project')),subject_hash text,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed')),requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),blocked_before timestamptz NOT NULL DEFAULT clock_timestamp()+interval '5 minutes',stream_expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '45 days',removed_events bigint NOT NULL DEFAULT 0,error text,finished_at timestamptz,FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS deletion_barriers(tenant_id uuid NOT NULL,project_id uuid NOT NULL,identity_hash text NOT NULL,deletion_job_id uuid NOT NULL REFERENCES deletion_jobs(id),requested_at timestamptz NOT NULL,blocked_before timestamptz NOT NULL,PRIMARY KEY(project_id,identity_hash),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS deletion_jobs_queue ON deletion_jobs(requested_at) WHERE status IN ('queued','running');
      CREATE INDEX IF NOT EXISTS deletion_barriers_job ON deletion_barriers(project_id,deletion_job_id,identity_hash);
      ALTER TABLE projects ADD COLUMN IF NOT EXISTS privacy_epoch integer NOT NULL DEFAULT 0;
      ALTER TABLE query_jobs ADD COLUMN IF NOT EXISTS privacy_epoch integer NOT NULL DEFAULT 0;
      ALTER TABLE deletion_jobs ALTER COLUMN user_id DROP NOT NULL;
      ALTER TABLE event_dedup ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'stored' CHECK(status IN ('stored','failed'));
      CREATE TABLE IF NOT EXISTS replay_jobs(id uuid NOT NULL,tenant_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES users(id),request_hash text NOT NULL,reason text NOT NULL,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed')),error text,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,PRIMARY KEY(project_id,id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS replay_items(tenant_id uuid NOT NULL,project_id uuid NOT NULL,job_id uuid NOT NULL,record_id text NOT NULL,event_id uuid NOT NULL,payload_hash text NOT NULL,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','published','stored','duplicate','suppressed','rejected','failed')),attempts integer NOT NULL DEFAULT 0,error text,topic text,partition integer,message_offset text,outcome_dlq_id text,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(project_id,job_id,record_id),FOREIGN KEY(project_id,job_id) REFERENCES replay_jobs(project_id,id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS replay_jobs_queue ON replay_jobs(next_attempt_at,checked_at) WHERE status IN ('queued','running');
      CREATE INDEX IF NOT EXISTS replay_items_source ON replay_items(project_id,record_id,updated_at DESC);
      CREATE TABLE IF NOT EXISTS recovery_gate(id boolean PRIMARY KEY DEFAULT true CHECK(id),blocked boolean NOT NULL DEFAULT true,ledger_exported_at timestamptz,updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
      INSERT INTO recovery_gate(id) VALUES(true) ON CONFLICT DO NOTHING;
      CREATE TABLE IF NOT EXISTS ingest_integrity_gate(id boolean PRIMARY KEY DEFAULT true CHECK(id),blocked boolean NOT NULL DEFAULT false,reason text,detected_at timestamptz,details jsonb NOT NULL DEFAULT '{}',CHECK((NOT blocked AND reason IS NULL) OR (blocked AND reason IS NOT NULL AND reason IN ('retained_offset_gap','consumer_offset_unknown'))));
      INSERT INTO ingest_integrity_gate(id,blocked,reason,detected_at) VALUES(true,${integrityExists?'true':'false'},${integrityExists?"'consumer_offset_unknown'":'NULL'},${integrityExists?'clock_timestamp()':'NULL'}) ON CONFLICT DO NOTHING;
      CREATE TABLE IF NOT EXISTS consumer_streams(topic text NOT NULL,group_id text NOT NULL,partition_count integer NOT NULL CHECK(partition_count BETWEEN 1 AND 1024),initialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(topic,group_id));
      CREATE TABLE IF NOT EXISTS consumer_progress(topic text NOT NULL,group_id text NOT NULL,partition_id integer NOT NULL CHECK(partition_id>=0),next_offset bigint NOT NULL CHECK(next_offset>=0),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(topic,group_id,partition_id),FOREIGN KEY(topic,group_id) REFERENCES consumer_streams(topic,group_id));
      CREATE TABLE IF NOT EXISTS consumer_progress_audit(id bigserial PRIMARY KEY,topic text NOT NULL,group_id text NOT NULL,action text NOT NULL,reason text NOT NULL,details jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
      CREATE TABLE IF NOT EXISTS recovery_import_scopes(ledger_hash text NOT NULL,tenant_id uuid NOT NULL,project_id uuid NOT NULL,PRIMARY KEY(ledger_hash,tenant_id,project_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      ALTER TABLE events ADD COLUMN IF NOT EXISTS registered boolean NOT NULL DEFAULT false;
      ALTER TABLE events ADD COLUMN IF NOT EXISTS project_schema_version integer NOT NULL DEFAULT 0;
      CREATE OR REPLACE VIEW visible_events WITH (security_barrier=true) AS
        SELECT e.* FROM events e
        LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id
        WHERE NOT EXISTS (
          SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=e.tenant_id AND b.project_id=e.project_id AND (
            b.identity_hash='*' OR (
              b.identity_hash IN (
                encode(sha256(convert_to(e.tenant_id::text||':'||e.project_id::text||':'||e.distinct_id,'UTF8')),'hex'),
                encode(sha256(convert_to(e.tenant_id::text||':'||e.project_id::text||':'||e.anonymous_id,'UTF8')),'hex'),
                encode(sha256(convert_to(e.tenant_id::text||':'||e.project_id::text||':'||l.user_id,'UTF8')),'hex')
              ) AND (e.event_time<=b.blocked_before OR e.received_at<=b.requested_at)
            )
          )
        );
      CREATE INDEX IF NOT EXISTS events_scope_time ON events(tenant_id,project_id,event_time DESC);
      CREATE INDEX IF NOT EXISTS events_user_time ON events(tenant_id,project_id,distinct_id,event_time DESC);
      INSERT INTO schema_migrations(version) VALUES(1) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(2) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(3) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(4) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(5) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(6) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(7) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(8) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(9) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(10) ON CONFLICT DO NOTHING;
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
