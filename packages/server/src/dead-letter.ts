import { canonical,storageSafeText,validateEvent,type StoredEnvelope } from '../../schema/src/index';
import { pool } from './db';
import { digest } from './security';
import { identityHash,lockProjectPrivacy } from './privacy';

const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export interface DeadLetter {id:string;reason:string;source:{topic:string;partition:number;offset:string};tenant_id:string|null;project_id:string|null;identity_hashes:string[];raw_base64:string|null;processor_version:2}
// Kafka has already acknowledged the immutable dead letter. Reserve the first
// valid event hash and index that acknowledgement in one PG transaction before
// the consumer is allowed to advance. Failed registrations are retryable, not
// successful event writes; persist() promotes them only with its raw-write TX.
export async function indexDeadLetter(dead:DeadLetter,record:StoredEnvelope|undefined){
  const db=await pool.connect();
  try{
    await db.query('BEGIN');
    const tenant=dead.tenant_id,project=dead.project_id;
    if(tenant&&project&&uuid.test(tenant)&&uuid.test(project)){
      await lockProjectPrivacy(db,project);
      const identities=[record?.event?.distinct_id,record?.event?.anonymous_id].filter((s):s is string=>typeof s==='string'&&s.length>0&&s.length<=256&&storageSafeText(s));
      const hashes=identities.map(s=>identityHash(tenant,project,s));
      const deleted=await db.query("SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=$1 AND b.project_id=$2 AND (b.identity_hash='*' OR b.identity_hash=ANY($3::text[]) OR b.identity_hash IN (SELECT encode(sha256(convert_to($1::uuid::text||':'||$2::uuid::text||':'||user_id,'UTF8')),'hex') FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=ANY($4::text[]))) LIMIT 1",[tenant,project,hashes,identities]);
      if(deleted.rowCount){
        await db.query("INSERT INTO ingest_metrics VALUES($1,$2,'deletion_suppressed',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1",[tenant,project]);
        await db.query('COMMIT');return;
      }
      if(record&&Number.isFinite(Date.parse(record.received_at))&&!validateEvent(record.event,Date.now(),false)){
        await db.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash,status) SELECT $1,$2,$3,$4,'failed' FROM projects WHERE tenant_id=$1 AND id=$2 ON CONFLICT DO NOTHING",[tenant,project,record.event.event_id,digest(canonical(record.event))]);
      }
    }
    await db.query('INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',[dead.id,dead.tenant_id,dead.project_id,dead.reason,JSON.stringify(dead)]);
    await db.query('COMMIT');
  }catch(e){await db.query('ROLLBACK');throw e;}finally{db.release();}
}
