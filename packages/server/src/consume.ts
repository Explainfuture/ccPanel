import { canonical, type StoredEnvelope } from '../../schema/src/index';
import { pool } from './db';
import { digest } from './security';
export class EventConflict extends Error { constructor(public reason: string) { super(reason); } }
export async function persist(record: StoredEnvelope): Promise<'inserted' | 'duplicate'> {
  const c = await pool.connect(); const e = record.event;
  try {
    await c.query('BEGIN');
    const exists = await c.query('SELECT 1 FROM projects WHERE tenant_id=$1 AND id=$2', [record.tenant_id,record.project_id]);
    if (!exists.rowCount) throw new EventConflict('scope_mismatch');
    const hash = digest(canonical(e));
    const registered = await c.query('INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING event_id', [record.tenant_id,record.project_id,e.event_id,hash]);
    if (!registered.rowCount) {
      const old = await c.query('SELECT payload_hash FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3', [record.tenant_id,record.project_id,e.event_id]);
      if (old.rows[0]?.payload_hash !== hash) throw new EventConflict('payload_conflict');
      await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'duplicates',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1", [record.tenant_id,record.project_id]);
      await c.query('COMMIT'); return 'duplicate';
    }
    if (e.event_name === '$identify') {
      if (!e.anonymous_id) throw new EventConflict('missing_anonymous_id');
      await c.query('INSERT INTO identity_links(tenant_id,project_id,anonymous_id,user_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING', [record.tenant_id,record.project_id,e.anonymous_id,e.distinct_id]);
      const link = await c.query('SELECT user_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=$3', [record.tenant_id,record.project_id,e.anonymous_id]);
      if (link.rows[0]?.user_id !== e.distinct_id) throw new EventConflict('identity_conflict');
    }
    await c.query('INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,anonymous_id,event_time,received_at,properties,context,schema_version,registered,project_schema_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)', [record.tenant_id,record.project_id,e.event_id,e.event_name,e.distinct_id,e.anonymous_id || null,e.event_time,record.received_at,JSON.stringify(e.properties),JSON.stringify(e.context),e.schema_version,record.validation?.registered||false,record.validation?.version||0]);
    await c.query('INSERT INTO aggregation_jobs(tenant_id,project_id,event_id) VALUES($1,$2,$3)', [record.tenant_id,record.project_id,e.event_id]);
    await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'inserted',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1", [record.tenant_id,record.project_id]);
    await c.query('COMMIT'); return 'inserted';
  } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
}
