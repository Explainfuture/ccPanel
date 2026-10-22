import type {PoolClient} from 'pg';
import {pool} from './db';
import {lockProjectPrivacy} from './privacy';

// Physical housekeeping never changes Kafka checkpoints or deletion barriers.
// A fixed transaction timestamp makes the expiry boundary consistent per batch.
export async function retainNext(db:PoolClient):Promise<'empty'|'completed'> {
  await db.query('BEGIN');
  try {
    await db.query("SET LOCAL statement_timeout='3s'");
    await db.query("SET LOCAL lock_timeout='200ms'");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202635) AS held')).rows[0].held){await db.query('ROLLBACK');return 'empty';}
    const scope=(await db.query(`SELECT tenant_id,project_id FROM (
      (SELECT tenant_id,project_id FROM events WHERE event_time<CURRENT_TIMESTAMP-interval '30 days' ORDER BY event_time LIMIT 1)
      UNION ALL
      (SELECT d.tenant_id,d.project_id FROM event_dedup d WHERE d.created_at<CURRENT_TIMESTAMP-interval '45 days'
        AND NOT EXISTS(SELECT 1 FROM events e WHERE e.tenant_id=d.tenant_id AND e.project_id=d.project_id AND e.event_id=d.event_id)
        AND NOT EXISTS(SELECT 1 FROM aggregation_jobs a WHERE a.project_id=d.project_id AND a.event_id=d.event_id)
        AND NOT EXISTS(SELECT 1 FROM replay_items i JOIN replay_jobs j ON j.project_id=i.project_id AND j.id=i.job_id WHERE i.project_id=d.project_id AND i.event_id=d.event_id AND j.status IN ('queued','running'))
        ORDER BY d.created_at LIMIT 1)
      UNION ALL
      (SELECT tenant_id,project_id FROM event_count_buckets WHERE bucket_start+interval '1 hour'<=CURRENT_TIMESTAMP-interval '30 days' ORDER BY bucket_start LIMIT 1)
    ) candidates LIMIT 1`)).rows[0];
    if(!scope){await db.query('COMMIT');return 'empty';}
    // Match persist/aggregation/deletion lock order, before touching task rows.
    await lockProjectPrivacy(db,scope.project_id,true);
    const args=[scope.tenant_id,scope.project_id];
    const removed=(await db.query(`WITH ids AS MATERIALIZED (
      SELECT event_id,event_time FROM events WHERE tenant_id=$1 AND project_id=$2 AND event_time<CURRENT_TIMESTAMP-interval '30 days' ORDER BY event_time,event_id LIMIT 100
    ), tasks AS (
      DELETE FROM aggregation_jobs a USING ids WHERE a.tenant_id=$1 AND a.project_id=$2 AND a.event_id=ids.event_id RETURNING a.event_id
    ), raw AS (
      DELETE FROM events e USING ids WHERE e.tenant_id=$1 AND e.project_id=$2 AND e.event_id=ids.event_id AND e.event_time=ids.event_time RETURNING e.event_id
    ) SELECT (SELECT count(*)::int FROM raw) AS raw,(SELECT count(*)::int FROM tasks) AS tasks`,args)).rows[0];
    const dedup=await db.query(`DELETE FROM event_dedup d USING (
      SELECT d.event_id FROM event_dedup d WHERE d.tenant_id=$1 AND d.project_id=$2 AND d.created_at<CURRENT_TIMESTAMP-interval '45 days'
        AND NOT EXISTS(SELECT 1 FROM events e WHERE e.tenant_id=d.tenant_id AND e.project_id=d.project_id AND e.event_id=d.event_id)
        AND NOT EXISTS(SELECT 1 FROM aggregation_jobs a WHERE a.project_id=d.project_id AND a.event_id=d.event_id)
        AND NOT EXISTS(SELECT 1 FROM replay_items i JOIN replay_jobs j ON j.project_id=i.project_id AND j.id=i.job_id WHERE i.project_id=d.project_id AND i.event_id=d.event_id AND j.status IN ('queued','running'))
      ORDER BY d.created_at,d.event_id LIMIT 100
    ) ids WHERE d.tenant_id=$1 AND d.project_id=$2 AND d.event_id=ids.event_id`,args);
    const buckets=await db.query(`DELETE FROM event_count_buckets b USING (
      SELECT event_name,bucket_start FROM event_count_buckets WHERE tenant_id=$1 AND project_id=$2 AND bucket_start+interval '1 hour'<=CURRENT_TIMESTAMP-interval '30 days' ORDER BY bucket_start,event_name LIMIT 100
    ) ids WHERE b.tenant_id=$1 AND b.project_id=$2 AND b.event_name=ids.event_name AND b.bucket_start=ids.bucket_start`,args);
    for(const [metric,count] of [['retention_raw_expired',removed.raw],['retention_tasks_expired',removed.tasks],['retention_dedup_expired',dedup.rowCount??0],['retention_buckets_expired',buckets.rowCount??0]] as const){
      if(count)await db.query('INSERT INTO ingest_metrics(tenant_id,project_id,metric,value) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+EXCLUDED.value',[...args,metric,count]);
    }
    await db.query('COMMIT');return 'completed';
  }catch(error){await db.query('ROLLBACK');throw error;}
}

export function startRetention(){
  // Enable only after the upgrade's original-data reconciliation is frozen.
  if(process.env.RETENTION_ENABLED!=='true')return()=>{};
  let stopped=false,busy=false;
  const timer=setInterval(async()=>{
    if(stopped||busy)return;busy=true;let db:PoolClient|undefined;
    try{db=await pool.connect();await retainNext(db);}
    catch{console.error(JSON.stringify({code:'retention_retry'}));}
    finally{db?.release();busy=false;}
  },5000);
  timer.unref();return()=>{stopped=true;clearInterval(timer);};
}
