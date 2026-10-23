import { pool } from './db';
export const schemaVersion=20;
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
      CREATE TABLE IF NOT EXISTS tenant_invitations(id uuid PRIMARY KEY,tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,email text NOT NULL CHECK(email=lower(email) AND length(email) BETWEEN 3 AND 254),role text NOT NULL CHECK(role IN ('admin','analyst','viewer')),token_hash text UNIQUE NOT NULL CHECK(token_hash ~ '^[a-f0-9]{64}$'),created_by uuid REFERENCES users(id) ON DELETE SET NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '7 days',accepted_by uuid REFERENCES users(id) ON DELETE SET NULL,accepted_at timestamptz,revoked_at timestamptz,CHECK(expires_at>created_at),CHECK(accepted_at IS NULL OR revoked_at IS NULL),CHECK(accepted_at IS NOT NULL OR accepted_by IS NULL));
      CREATE UNIQUE INDEX IF NOT EXISTS tenant_invitations_pending_email ON tenant_invitations(tenant_id,email) WHERE accepted_at IS NULL AND revoked_at IS NULL;
      CREATE TABLE IF NOT EXISTS platform_operators(user_id uuid PRIMARY KEY REFERENCES users(id),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),revoked_at timestamptz);
      CREATE TABLE IF NOT EXISTS company_provision_requests(id uuid PRIMARY KEY,operator_id uuid NOT NULL REFERENCES platform_operators(user_id),tenant_id uuid NOT NULL REFERENCES tenants(id),owner_id uuid NOT NULL REFERENCES users(id),request_hash text NOT NULL CHECK(request_hash ~ '^[a-f0-9]{64}$'),created_at timestamptz NOT NULL DEFAULT clock_timestamp());
      CREATE TABLE IF NOT EXISTS platform_audit(id bigserial PRIMARY KEY,operator_id uuid NOT NULL REFERENCES platform_operators(user_id),tenant_id uuid REFERENCES tenants(id),action text NOT NULL,details jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT clock_timestamp());
      CREATE TABLE IF NOT EXISTS sessions(token_hash text PRIMARY KEY,user_id uuid NOT NULL REFERENCES users(id),csrf_hash text NOT NULL, expires_at timestamptz NOT NULL);
      CREATE TABLE IF NOT EXISTS projects(id uuid PRIMARY KEY,tenant_id uuid NOT NULL REFERENCES tenants(id),name text NOT NULL,timezone text NOT NULL DEFAULT 'Asia/Shanghai',origins text[] NOT NULL DEFAULT '{}',schema_mode text NOT NULL DEFAULT 'loose' CHECK(schema_mode IN ('loose','strict')),created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(tenant_id,id));
      ALTER TABLE projects ADD COLUMN IF NOT EXISTS retention_days smallint NOT NULL DEFAULT 30 CHECK(retention_days BETWEEN 1 AND 30);
      ALTER TABLE projects ADD COLUMN IF NOT EXISTS retention_version integer NOT NULL DEFAULT 1 CHECK(retention_version>0);
      ALTER TABLE projects ADD COLUMN IF NOT EXISTS retention_floor timestamptz;
      CREATE OR REPLACE FUNCTION project_retention_cutoff(days integer,floor timestamptz) RETURNS timestamptz LANGUAGE sql STABLE PARALLEL SAFE AS 'SELECT GREATEST(COALESCE(floor,''-infinity''::timestamptz),CURRENT_TIMESTAMP-days*interval ''1 day'')';
      CREATE TABLE IF NOT EXISTS write_keys(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,key_hash text UNIQUE NOT NULL,label text NOT NULL,revoked_at timestamptz,FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS event_schemas(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_name text NOT NULL,required_properties jsonb NOT NULL DEFAULT '{}',version integer NOT NULL DEFAULT 1,PRIMARY KEY(project_id,event_name),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS event_dedup(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,payload_hash text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS events(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,event_name text NOT NULL,distinct_id text NOT NULL,anonymous_id text,event_time timestamptz NOT NULL,received_at timestamptz NOT NULL,properties jsonb NOT NULL,context jsonb NOT NULL,schema_version integer NOT NULL,PRIMARY KEY(project_id,event_time,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id)) PARTITION BY RANGE(event_time);
      CREATE TABLE IF NOT EXISTS identity_links(tenant_id uuid NOT NULL,project_id uuid NOT NULL,anonymous_id text NOT NULL,user_id text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,anonymous_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS aggregation_jobs(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_id uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(project_id,event_id),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      ALTER TABLE aggregation_jobs ADD COLUMN IF NOT EXISTS processed_at timestamptz;
      ALTER TABLE aggregation_jobs ADD COLUMN IF NOT EXISTS processed_epoch integer;
      ALTER TABLE aggregation_jobs ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT clock_timestamp();
      ALTER TABLE aggregation_jobs ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0);
      ALTER TABLE aggregation_jobs ADD COLUMN IF NOT EXISTS last_error_code text;
      CREATE INDEX IF NOT EXISTS aggregation_pending ON aggregation_jobs(available_at,created_at) WHERE processed_at IS NULL;
      CREATE INDEX IF NOT EXISTS aggregation_epoch ON aggregation_jobs(project_id,processed_epoch,available_at);
      CREATE TABLE IF NOT EXISTS event_count_buckets(tenant_id uuid NOT NULL,project_id uuid NOT NULL,event_name text NOT NULL,bucket_start timestamptz NOT NULL,event_count bigint NOT NULL CHECK(event_count>=0),privacy_epoch integer NOT NULL,computed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(project_id,event_name,bucket_start),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id) ON DELETE CASCADE,CHECK(bucket_start=date_trunc('hour',bucket_start AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'));
      CREATE TABLE IF NOT EXISTS audit_logs(id bigserial PRIMARY KEY,tenant_id uuid NOT NULL REFERENCES tenants(id),user_id uuid REFERENCES users(id),action text NOT NULL,details jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS ingest_metrics(tenant_id uuid NOT NULL,project_id uuid NOT NULL,metric text NOT NULL,value bigint NOT NULL DEFAULT 0,PRIMARY KEY(project_id,metric),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS dlq_records(id text PRIMARY KEY,tenant_id uuid,project_id uuid,reason text NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS reports(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,name text NOT NULL,spec jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS dashboards(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,name text NOT NULL,report_ids uuid[] NOT NULL DEFAULT '{}',updated_at timestamptz NOT NULL DEFAULT now(),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      ALTER TABLE reports ADD COLUMN IF NOT EXISTS timezone text;
      ALTER TABLE reports ADD COLUMN IF NOT EXISTS version integer NOT NULL DEFAULT 1 CHECK(version>0);
      ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS columns smallint NOT NULL DEFAULT 1 CHECK(columns IN (1,2));
      ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS range_days integer CHECK(range_days IN (1,7,30));
      ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS filters jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(filters)='array' AND jsonb_array_length(filters)<=20);
      ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS version integer NOT NULL DEFAULT 1 CHECK(version>0);
      CREATE TABLE IF NOT EXISTS query_jobs(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES users(id),spec jsonb NOT NULL,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','succeeded','failed','cancelled')),result jsonb,error text,created_at timestamptz NOT NULL DEFAULT now(),started_at timestamptz,finished_at timestamptz,expires_at timestamptz NOT NULL DEFAULT now()+interval '24 hours',FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS query_jobs_queue ON query_jobs(created_at) WHERE status IN ('queued','running');
      CREATE TABLE IF NOT EXISTS deletion_jobs(id uuid PRIMARY KEY,tenant_id uuid NOT NULL,project_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES users(id),scope text NOT NULL CHECK(scope IN ('user','project')),subject_hash text,status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed')),requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),blocked_before timestamptz NOT NULL DEFAULT clock_timestamp()+interval '5 minutes',stream_expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '45 days',removed_events bigint NOT NULL DEFAULT 0,error text,finished_at timestamptz,FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE TABLE IF NOT EXISTS deletion_barriers(tenant_id uuid NOT NULL,project_id uuid NOT NULL,identity_hash text NOT NULL,deletion_job_id uuid NOT NULL REFERENCES deletion_jobs(id),requested_at timestamptz NOT NULL,blocked_before timestamptz NOT NULL,PRIMARY KEY(project_id,identity_hash),FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id));
      CREATE INDEX IF NOT EXISTS deletion_jobs_queue ON deletion_jobs(requested_at) WHERE status IN ('queued','running');
      CREATE INDEX IF NOT EXISTS deletion_barriers_job ON deletion_barriers(project_id,deletion_job_id,identity_hash);
      ALTER TABLE projects ADD COLUMN IF NOT EXISTS privacy_epoch integer NOT NULL DEFAULT 0;
      ALTER TABLE query_jobs ADD COLUMN IF NOT EXISTS privacy_epoch integer NOT NULL DEFAULT 0;
      ALTER TABLE query_jobs ADD COLUMN IF NOT EXISTS timezone text;
      ALTER TABLE query_jobs ADD COLUMN IF NOT EXISTS report_id uuid;
      ALTER TABLE query_jobs ADD COLUMN IF NOT EXISTS report_version integer CHECK(report_version>0) CHECK((report_id IS NULL AND report_version IS NULL) OR (report_id IS NOT NULL AND report_version IS NOT NULL AND report_version>0 AND timezone IS NOT NULL));
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
        JOIN projects p ON p.tenant_id=e.tenant_id AND p.id=e.project_id
        LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id
        WHERE e.event_time>=project_retention_cutoff(p.retention_days,p.retention_floor) AND NOT EXISTS (
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
      CREATE OR REPLACE VIEW visible_event_count_buckets WITH (security_barrier=true) AS
        SELECT b.* FROM event_count_buckets b JOIN projects p ON p.tenant_id=b.tenant_id AND p.id=b.project_id AND p.privacy_epoch=b.privacy_epoch
        WHERE b.bucket_start>=project_retention_cutoff(p.retention_days,p.retention_floor) AND NOT EXISTS(SELECT 1 FROM deletion_barriers d WHERE d.tenant_id=b.tenant_id AND d.project_id=b.project_id AND d.identity_hash='*');
      CREATE INDEX IF NOT EXISTS events_retention_time ON events(event_time);
      CREATE INDEX IF NOT EXISTS dedup_retention_time ON event_dedup(created_at);
      CREATE INDEX IF NOT EXISTS buckets_retention_time ON event_count_buckets(bucket_start);
      CREATE INDEX IF NOT EXISTS replay_items_event ON replay_items(project_id,event_id);
      CREATE INDEX IF NOT EXISTS dlq_retention_time ON dlq_records(created_at);
      CREATE INDEX IF NOT EXISTS events_scope_time ON events(tenant_id,project_id,event_time DESC);
      CREATE INDEX IF NOT EXISTS events_user_time ON events(tenant_id,project_id,distinct_id,event_time DESC);
      CREATE INDEX IF NOT EXISTS events_scope_event ON events(tenant_id,project_id,event_id);
      CREATE TABLE IF NOT EXISTS operational_samples(
        source text NOT NULL CHECK(source IN ('api','worker','host')),instance_id uuid NOT NULL,
        observed_at timestamptz NOT NULL,started_at timestamptz NOT NULL,
        received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        payload_hash text NOT NULL CHECK(payload_hash ~ '^[a-f0-9]{64}$'),snapshot jsonb NOT NULL,
        PRIMARY KEY(source,instance_id,observed_at),CHECK(observed_at>=started_at),
        CHECK(jsonb_typeof(snapshot)='object' AND octet_length(snapshot::text)<=16384));
      CREATE INDEX IF NOT EXISTS operational_samples_time ON operational_samples(observed_at,source,instance_id);
      CREATE TABLE IF NOT EXISTS operational_alert_states(
        rule text PRIMARY KEY CHECK(rule IN ('api_sampling','worker_sampling','api_server_errors','worker_failures')),
        state text NOT NULL CHECK(state IN ('clear','firing','unknown')),reason text NOT NULL CHECK(length(reason)<=64),
        evaluated_at timestamptz NOT NULL,state_since timestamptz NOT NULL,revision bigint NOT NULL CHECK(revision>0),
        signal jsonb NOT NULL CHECK(jsonb_typeof(signal)='object' AND octet_length(signal::text)<=2048 AND signal->>'rule'=rule AND signal->>'state'=state),
        CHECK(state_since<=evaluated_at));
      CREATE TABLE IF NOT EXISTS operational_alert_history(
        id bigserial PRIMARY KEY,rule text NOT NULL CHECK(rule IN ('api_sampling','worker_sampling','api_server_errors','worker_failures')),
        state text NOT NULL CHECK(state IN ('clear','firing','unknown')),reason text NOT NULL CHECK(length(reason)<=64),
        evaluated_at timestamptz NOT NULL,state_since timestamptz NOT NULL,revision bigint NOT NULL CHECK(revision>0),
        signal jsonb NOT NULL CHECK(jsonb_typeof(signal)='object' AND octet_length(signal::text)<=2048 AND signal->>'rule'=rule AND signal->>'state'=state),
        UNIQUE(rule,revision),CHECK(state_since<=evaluated_at));
      CREATE INDEX IF NOT EXISTS operational_alert_history_time ON operational_alert_history(evaluated_at,id);
      INSERT INTO schema_migrations(version) VALUES(19) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(20) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(18) ON CONFLICT DO NOTHING;
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
      INSERT INTO schema_migrations(version) VALUES(11) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(12) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(13) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(14) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(15) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(16) ON CONFLICT DO NOTHING;
      INSERT INTO schema_migrations(version) VALUES(17) ON CONFLICT DO NOTHING;
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
