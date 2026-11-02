import type {PoolClient} from 'pg';
import {parseOperationalSnapshot,type OperationalSnapshot} from './operational-snapshot';
export const operationalHistoryLimit=50000;
export async function storeOperationalSnapshot(db:PoolClient,input:OperationalSnapshot){
  const value=parseOperationalSnapshot(input);
  await db.query('BEGIN');
  try{
    await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','1000ms',true)");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202637) AS held')).rows[0].held){await db.query('ROLLBACK');return{status:'busy' as const,expired_deleted:0,capacity_deleted:0};}
    const key=[value.source,value.instance_id,value.observed_at];
    const prior=(await db.query('SELECT payload_hash FROM operational_samples WHERE source=$1 AND instance_id=$2 AND observed_at=$3',key)).rows[0];
    if(prior){await db.query('COMMIT');return{status:prior.payload_hash===value.payload_hash?'duplicate' as const:'conflict' as const,expired_deleted:0,capacity_deleted:0};}
    const anchor=(await db.query('SELECT started_at FROM operational_samples WHERE source=$1 AND instance_id=$2 LIMIT 1',key.slice(0,2))).rows[0];
    if(anchor&&anchor.started_at.toISOString()!==value.started_at.toISOString()){await db.query('COMMIT');return{status:'conflict' as const,expired_deleted:0,capacity_deleted:0};}
    const expired=await db.query("DELETE FROM operational_samples WHERE (source,instance_id,observed_at) IN (SELECT source,instance_id,observed_at FROM operational_samples WHERE observed_at<CURRENT_TIMESTAMP-interval '7 days' ORDER BY observed_at,source,instance_id LIMIT 100)");
    const count=Number((await db.query('SELECT count(*)::int AS n FROM operational_samples')).rows[0].n);
    const remove=Math.min(100,Math.max(0,count-operationalHistoryLimit+1));
    const trimmed=remove?await db.query('DELETE FROM operational_samples WHERE (source,instance_id,observed_at) IN (SELECT source,instance_id,observed_at FROM operational_samples ORDER BY observed_at,source,instance_id LIMIT $1)',[remove]):{rowCount:0};
    if(count-(trimmed.rowCount??0)>=operationalHistoryLimit){await db.query('COMMIT');return{status:'full' as const,expired_deleted:expired.rowCount??0,capacity_deleted:trimmed.rowCount??0};}
    await db.query('INSERT INTO operational_samples(source,instance_id,observed_at,started_at,payload_hash,snapshot) VALUES($1,$2,$3,$4,$5,$6)',[...key,value.started_at,value.payload_hash,JSON.stringify(value.snapshot)]);
    await db.query('COMMIT');return{status:'stored' as const,expired_deleted:expired.rowCount??0,capacity_deleted:trimmed.rowCount??0};
  }catch(error){await db.query('ROLLBACK');throw error;}
}
