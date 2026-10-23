import { canonical, type StoredEnvelope } from '../../schema/src/index';
import { pool } from './db';
import { digest } from './security';
import { lockProjectPrivacy,suppressedByDeletion } from './privacy';
export class EventConflict extends Error { constructor(public reason: string) { super(reason); } }
export function storageFailureReason(error:unknown){
  const code=(error as {code?:unknown})?.code;
  return typeof code==='string'&&(/^22[0-9A-Z]{3}$/.test(code)||code==='23514')?'invalid_storage_value':undefined;
}
export async function persist(record: StoredEnvelope): Promise<'inserted' | 'duplicate' | 'suppressed'> {
  const c = await pool.connect(); const e = record.event;
  try {
    await c.query('BEGIN');
    await lockProjectPrivacy(c,record.project_id,e.event_name==='$identify');
    const exists = await c.query('SELECT $3::timestamptz<project_retention_cutoff(retention_days,retention_floor) AS retention_expired FROM projects WHERE tenant_id=$1 AND id=$2', [record.tenant_id,record.project_id,e.event_time]);
    if (!exists.rowCount) throw new EventConflict('scope_mismatch');
    if(e.event_name==='$identify'&&!exists.rows[0].retention_expired){
      if(!e.anonymous_id)throw new EventConflict('missing_anonymous_id');
      const oldLink=await c.query('SELECT user_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=$3',[record.tenant_id,record.project_id,e.anonymous_id]);
      if(oldLink.rows[0]&&oldLink.rows[0].user_id!==e.distinct_id)throw new EventConflict('identity_conflict');
    }
    if(await suppressedByDeletion(c,record)){
      await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'deletion_suppressed',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1",[record.tenant_id,record.project_id]);
      await c.query('COMMIT');return 'suppressed';
    }
    const hash = digest(canonical(e));
    const registered = await c.query('INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING event_id', [record.tenant_id,record.project_id,e.event_id,hash]);
    if (!registered.rowCount) {
      const old = await c.query('SELECT payload_hash,status FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 FOR UPDATE', [record.tenant_id,record.project_id,e.event_id]);
      if (old.rows[0]?.payload_hash !== hash) throw new EventConflict('payload_conflict');
      if(old.rows[0].status==='failed'){
        // Retrying a repaired processor must write the original event once.
        // This transition rolls back together with any failed raw/aggregate write.
        await c.query("UPDATE event_dedup SET status='stored' WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3",[record.tenant_id,record.project_id,e.event_id]);
      }else{
        await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'duplicates',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1", [record.tenant_id,record.project_id]);
        await c.query('COMMIT'); return 'duplicate';
      }
    }
    if(exists.rows[0].retention_expired){
      await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'retention_suppressed',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1",[record.tenant_id,record.project_id]);
      await c.query('COMMIT');return 'suppressed';
    }
    if (e.event_name === '$identify') {
      if (!e.anonymous_id) throw new EventConflict('missing_anonymous_id');
      await c.query('INSERT INTO identity_links(tenant_id,project_id,anonymous_id,user_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING', [record.tenant_id,record.project_id,e.anonymous_id,e.distinct_id]);
    }
    await c.query('INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,anonymous_id,event_time,received_at,properties,context,schema_version,registered,project_schema_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)', [record.tenant_id,record.project_id,e.event_id,e.event_name,e.distinct_id,e.anonymous_id || null,e.event_time,record.received_at,JSON.stringify(e.properties),JSON.stringify(e.context),e.schema_version,record.validation?.registered||false,record.validation?.version||0]);
    await c.query('INSERT INTO aggregation_jobs(tenant_id,project_id,event_id) VALUES($1,$2,$3)', [record.tenant_id,record.project_id,e.event_id]);
    await c.query("INSERT INTO ingest_metrics VALUES($1,$2,'inserted',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1", [record.tenant_id,record.project_id]);
    await c.query('COMMIT'); return 'inserted';
  } catch (e) { await c.query('ROLLBACK');const reason=storageFailureReason(e);if(reason)throw new EventConflict(reason);throw e; } finally { c.release(); }
}
