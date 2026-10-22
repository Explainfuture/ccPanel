import type {PoolClient} from 'pg';
import {pool} from './db';
import {lockProjectPrivacy} from './privacy';

// One bounded runner per database. Completion markers remain for dedup/lifecycle
// reconciliation; a privacy epoch change automatically makes them pending again.
export async function aggregateNext(db:PoolClient):Promise<'empty'|'completed'|'retry'> {
  let candidate:{tenant_id:string;project_id:string;event_id:string}|undefined;
  try{
    await db.query('BEGIN');
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202632) AS held')).rows[0].held){await db.query('ROLLBACK');return 'empty';}
    candidate=(await db.query(`SELECT a.tenant_id,a.project_id,a.event_id FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id
      WHERE a.available_at<=clock_timestamp() AND (a.processed_at IS NULL OR a.processed_epoch IS DISTINCT FROM p.privacy_epoch) ORDER BY a.created_at,a.project_id,a.event_id LIMIT 1`)).rows[0];
    if(!candidate){await db.query('COMMIT');return 'empty';}
    // Same order as deletion/persist: privacy lock BEFORE any task row lock.
    await lockProjectPrivacy(db,candidate.project_id);
    const values=[candidate.tenant_id,candidate.project_id,candidate.event_id];
    const job=(await db.query(`SELECT a.event_id,p.privacy_epoch,EXISTS(SELECT 1 FROM deletion_barriers d WHERE d.tenant_id=a.tenant_id AND d.project_id=a.project_id AND d.identity_hash='*') AS deleted
      FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id WHERE a.tenant_id=$1 AND a.project_id=$2 AND a.event_id=$3 FOR UPDATE OF a SKIP LOCKED`,values)).rows[0];
    if(!job){await db.query('COMMIT');return 'empty';}
    const raw=(await db.query("SELECT event_name,date_trunc('hour',event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS bucket_start FROM events WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 LIMIT 1",values)).rows[0];
    if(!raw)throw Object.assign(new Error('aggregate_source_missing'),{code:'AG001'});
    if(raw.event_name==='$identify'||job.deleted){
      await db.query('UPDATE aggregation_jobs SET processed_at=clock_timestamp(),processed_epoch=$4,last_error_code=NULL WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[...values,job.privacy_epoch]);
    }else{
      // ONE statement snapshot for counted raw rows and the task IDs completed.
      // A later raw+task COMMIT is not accidentally marked completed without
      // being counted, and recomputation replaces (never adds to) the bucket.
      await db.query(`WITH counted AS MATERIALIZED (
        SELECT count(*)::bigint AS value FROM visible_events WHERE tenant_id=$1 AND project_id=$2 AND event_name=$3 AND event_time>=$4 AND event_time<$4::timestamptz+interval '1 hour'
      ), ids AS MATERIALIZED (
        SELECT a.event_id FROM aggregation_jobs a JOIN events e ON e.tenant_id=a.tenant_id AND e.project_id=a.project_id AND e.event_id=a.event_id
        WHERE a.tenant_id=$1 AND a.project_id=$2 AND e.event_name=$3 AND e.event_time>=$4 AND e.event_time<$4::timestamptz+interval '1 hour'
          AND (a.processed_at IS NULL OR a.processed_epoch IS DISTINCT FROM $5::integer) ORDER BY a.created_at,a.event_id LIMIT 500
      ), bucket AS (
        INSERT INTO event_count_buckets(tenant_id,project_id,event_name,bucket_start,event_count,privacy_epoch)
        SELECT $1,$2,$3,$4,value,$5 FROM counted WHERE value>0 ON CONFLICT(project_id,event_name,bucket_start)
        DO UPDATE SET event_count=EXCLUDED.event_count,privacy_epoch=EXCLUDED.privacy_epoch,computed_at=clock_timestamp() RETURNING project_id
      ), cleaned AS (
        DELETE FROM event_count_buckets b USING counted c WHERE c.value=0 AND b.tenant_id=$1 AND b.project_id=$2 AND b.event_name=$3 AND b.bucket_start=$4 RETURNING b.project_id
      ), applied AS (SELECT count(*) FROM (SELECT project_id FROM bucket UNION ALL SELECT project_id FROM cleaned) changes)
      UPDATE aggregation_jobs a SET processed_at=clock_timestamp(),processed_epoch=$5,last_error_code=NULL
        FROM ids,applied WHERE a.tenant_id=$1 AND a.project_id=$2 AND a.event_id=ids.event_id`,[candidate.tenant_id,candidate.project_id,raw.event_name,raw.bucket_start,job.privacy_epoch]);
    }
    await db.query('COMMIT');return 'completed';
  }catch(error){
    await db.query('ROLLBACK');
    if(candidate){
      const raw=(error as {code?:unknown}).code,code=typeof raw==='string'&&/^[0-9A-Z]{5}$/.test(raw)?raw:'aggregation_unavailable';
      await db.query("UPDATE aggregation_jobs SET attempts=LEAST(attempts+1,1000),last_error_code=$4,available_at=clock_timestamp()+interval '30 seconds' WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 AND (processed_at IS NULL OR processed_epoch IS DISTINCT FROM (SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2))",[candidate.tenant_id,candidate.project_id,candidate.event_id,code]);
    }
    console.error(JSON.stringify({code:'aggregation_retry'}));return 'retry';
  }
}

export function startAggregations(){
  let busy=false,stopped=false,nextTick=0;
  const tick=async()=>{
    if(busy||stopped||Date.now()<nextTick)return;busy=true;let db:PoolClient|undefined;
    try{db=await pool.connect();const result=await aggregateNext(db);nextTick=Date.now()+(result==='retry'?30000:result==='empty'?5000:0);}
    catch{nextTick=Date.now()+30000;console.error(JSON.stringify({code:'aggregation_probe_failed'}));}
    finally{db?.release();busy=false;}
  };
  const timer=setInterval(()=>void tick(),1000);timer.unref();void tick();
  return()=>{stopped=true;clearInterval(timer);};
}
