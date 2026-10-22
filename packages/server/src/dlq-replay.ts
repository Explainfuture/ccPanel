import type { Hono } from 'hono';
import type { PoolClient } from 'pg';
import type { Env } from '../../../apps/api/src/app';
import type { StoredEnvelope } from '../../schema/src/index';
import { canonical } from '../../schema/src/index';
import { pool,query } from './db';
import { digest } from './security';
import { identityHash,lockProjectPrivacy } from './privacy';
import { parseReplayRequest,readReplayPayload,replayCandidate,ReplayRejected } from './dlq-replay-policy';
import { IngestionPaused } from './ingest-admission';

const prefix='/api/tenants/:tenant/projects/:project/dlq/replays';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
async function deleted(db:PoolClient,r:StoredEnvelope){
  const ids=[r.event.distinct_id,...(r.event.anonymous_id?[r.event.anonymous_id]:[])];
  const hashes=ids.map(id=>identityHash(r.tenant_id,r.project_id,id));
  return !!(await db.query("SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=$1 AND b.project_id=$2 AND (b.identity_hash='*' OR ((b.identity_hash=ANY($3::text[]) OR b.identity_hash IN (SELECT encode(sha256(convert_to($1::uuid::text||':'||$2::uuid::text||':'||user_id,'UTF8')),'hex') FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=ANY($4::text[]))) AND ($5::timestamptz<=b.blocked_before OR $6::timestamptz<=b.requested_at))) LIMIT 1",[r.tenant_id,r.project_id,hashes,ids,r.event.event_time,r.received_at])).rowCount;
}
async function audit(db:PoolClient,job:{tenant_id:string;project_id:string;id:string;user_id:string},action:string,details:Record<string,unknown>){
  await db.query('INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)',[job.tenant_id,job.user_id,action,JSON.stringify({project_id:job.project_id,replay_job_id:job.id,...details})]);
}
export function registerReplayRoutes(app:Hono<Env>){
  app.use(prefix,async(c,next)=>{if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);await next();});
  app.use(prefix+'/*',async(c,next)=>{if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);await next();});
  app.post(prefix,async c=>{
    let body;try{body=parseReplayRequest(await c.req.json());}catch{return c.json({error:'invalid_replay_request'},400);}
    const tenant=c.get('tenantId'),project=c.req.param('project')!,hash=digest(JSON.stringify(body)),db=await pool.connect();
    try{
      await db.query('BEGIN');
      await db.query('SELECT pg_advisory_xact_lock(169202619)');
      const prior=await db.query('SELECT id,status,request_hash FROM replay_jobs WHERE tenant_id=$1 AND project_id=$2 AND id=$3',[tenant,project,body.request_id]);
      if(prior.rowCount){await db.query('COMMIT');return prior.rows[0].request_hash===hash?c.json({id:body.request_id,status:prior.rows[0].status},200):c.json({error:'replay_request_conflict'},409);}
      const counts=(await db.query("SELECT count(*)::int AS total,count(*) FILTER(WHERE project_id=$1)::int AS project FROM replay_jobs WHERE status IN ('queued','running')",[project])).rows[0];
      if(counts.total>=100||counts.project>=5){await db.query('ROLLBACK');return c.json({error:'replay_queue_full'},429);}
      await lockProjectPrivacy(db,project);
      const rows=await db.query("SELECT id,reason,payload FROM dlq_records WHERE tenant_id=$1 AND project_id=$2 AND id=ANY($3::text[]) AND created_at>=CURRENT_TIMESTAMP-interval '14 days' ORDER BY id",[tenant,project,body.record_ids]);
      if(rows.rowCount!==body.record_ids.length){await db.query('ROLLBACK');return c.json({error:'not_found'},404);}
      const candidates=[];
      for(const row of rows.rows){
        const input={tenant,project,payload:row.payload,reason:row.reason},decoded=readReplayPayload(input);
        const registration=(await db.query('SELECT payload_hash,created_at FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 FOR SHARE',[tenant,project,decoded.record.event.event_id])).rows[0]??null;
        const candidate=replayCandidate({...input,registration});
        if(await deleted(db,candidate.record))throw new ReplayRejected('deleted_scope');
        candidates.push({id:row.id,event:candidate.record.event.event_id,hash:digest(canonical(candidate.record.event))});
      }
      await db.query('INSERT INTO replay_jobs(id,tenant_id,project_id,user_id,request_hash,reason) VALUES($1,$2,$3,$4,$5,$6)',[body.request_id,tenant,project,c.get('userId'),hash,body.reason]);
      for(const item of candidates)await db.query('INSERT INTO replay_items(tenant_id,project_id,job_id,record_id,event_id,payload_hash) VALUES($1,$2,$3,$4,$5,$6)',[tenant,project,body.request_id,item.id,item.event,item.hash]);
      await audit(db,{tenant_id:tenant,project_id:project,id:body.request_id,user_id:c.get('userId')},'dlq_replay_requested',{reason:body.reason,count:candidates.length,record_ids:body.record_ids});
      await db.query('COMMIT');return c.json({id:body.request_id,status:'queued',count:candidates.length},202);
    }catch(e){await db.query('ROLLBACK');if(e instanceof ReplayRejected)return c.json({error:e.code},409);throw e;}finally{db.release();}
  });
  app.get(prefix,async c=>c.json({jobs:(await query('SELECT id,user_id,reason,status,error,created_at,finished_at FROM replay_jobs WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at DESC LIMIT 50',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.get(prefix+'/:job',async c=>{
    const id=c.req.param('job')!;if(!uuid.test(id))return c.json({error:'not_found'},404);
    const scope=[c.get('tenantId'),c.req.param('project'),id],job=await query('SELECT id,user_id,reason,status,error,created_at,finished_at FROM replay_jobs WHERE tenant_id=$1 AND project_id=$2 AND id=$3',scope);
    if(!job.rowCount)return c.json({error:'not_found'},404);
    return c.json({...job.rows[0],items:(await query('SELECT record_id,status,attempts,error,outcome_dlq_id,updated_at FROM replay_items WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 ORDER BY record_id',scope)).rows});
  });
}

export interface ReplayReceipt {topic:string;partition:number;offset:string}
export type ReplayPublisher=(raw:Buffer,key:string)=>Promise<ReplayReceipt>;
// One bounded item per transaction; Kafka acknowledgement precedes the durable
// receipt. A crash in between republishes the same ID and cannot double count.
// No raw payload is copied into the task or its audit trail.
export async function replayTick(publish:ReplayPublisher){
  const db=await pool.connect();
  try{
    await db.query('BEGIN');
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202618) AS held')).rows[0].held){await db.query('ROLLBACK');return;}
    const job=(await db.query("SELECT * FROM replay_jobs WHERE status IN ('queued','running') AND next_attempt_at<=clock_timestamp() ORDER BY checked_at,id LIMIT 1 FOR UPDATE")).rows[0];
    if(!job){await db.query('COMMIT');return;}
    const scope=[job.tenant_id,job.project_id,job.id];
    await lockProjectPrivacy(db,job.project_id);
    await db.query("UPDATE replay_jobs SET status='running',checked_at=clock_timestamp(),next_attempt_at=clock_timestamp()+interval '2 seconds' WHERE tenant_id=$1 AND project_id=$2 AND id=$3",scope);
    const item=(await db.query("SELECT * FROM replay_items WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND status IN ('queued','published') ORDER BY updated_at,record_id LIMIT 1 FOR UPDATE",scope)).rows[0];
    if(item){
      const set=async(status:string,error:string|null=null,outcome:string|null=null)=>{await db.query('UPDATE replay_items SET status=$5,error=$6,outcome_dlq_id=$7,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND record_id=$4',[...scope,item.record_id,status,error,outcome]);};
      const registration=(await db.query('SELECT status,payload_hash,created_at FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 FOR SHARE',[job.tenant_id,job.project_id,item.event_id])).rows[0]??null;
      const row=(await db.query('SELECT reason,payload FROM dlq_records WHERE tenant_id=$1 AND project_id=$2 AND id=$3',[job.tenant_id,job.project_id,item.record_id])).rows[0];
      if(row?.payload?.redacted===true){await set('suppressed','deleted_scope');}
      else if(!row){await set(item.status==='published'?'failed':'rejected',item.status==='published'?'outcome_unresolved':'source_unavailable');}
      else{
        try{
          const candidate=replayCandidate({tenant:job.tenant_id,project:job.project_id,payload:row.payload,reason:row.reason,registration});
          if(digest(canonical(candidate.record.event))!==item.payload_hash)throw new ReplayRejected('manual_review_required');
          if(await deleted(db,candidate.record)){await set('suppressed','deleted_scope');}
          else if(registration?.status==='stored'){await set(item.status==='published'?'stored':'duplicate');}
          else if(item.status==='published'){
            const outcome=digest(`${item.topic}:${item.partition}:${item.message_offset}`);
            const rejected=await db.query('SELECT 1 FROM dlq_records WHERE id=$1 AND tenant_id=$2 AND project_id=$3',[outcome,job.tenant_id,job.project_id]);
            if(rejected.rowCount)await set('failed','consumer_rejected',outcome);
            else await db.query('UPDATE replay_items SET updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND record_id=$4',[...scope,item.record_id]);
          }else{
            const role=await db.query("SELECT 1 FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 AND role='admin'",[job.tenant_id,job.user_id]);
            if(!role.rowCount)await set('rejected','admin_permission_revoked');
            else{
              try{
                const receipt=await publish(candidate.raw,candidate.record.project_id+':'+candidate.record.event.distinct_id);
                if(!receipt||typeof receipt.topic!=='string'||!/^[A-Za-z0-9._-]{1,249}$/.test(receipt.topic)||['.','..'].includes(receipt.topic)||!Number.isSafeInteger(receipt.partition)||receipt.partition<0||!/^\d+$/.test(receipt.offset))throw new Error('invalid_publish_receipt');
                await db.query("UPDATE replay_items SET status='published',attempts=attempts+1,error=NULL,topic=$5,partition=$6,message_offset=$7,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND record_id=$4",[...scope,item.record_id,receipt.topic,receipt.partition,receipt.offset]);
                await audit(db,job,'dlq_replay_published',{record_id:item.record_id,topic:receipt.topic,partition:receipt.partition,offset:receipt.offset});
              }catch(e){
                if(e instanceof IngestionPaused){
                  await db.query("UPDATE replay_items SET error='ingestion_paused',updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND record_id=$4",[...scope,item.record_id]);
                  await db.query("UPDATE replay_jobs SET next_attempt_at=clock_timestamp()+($4::int*interval '1 second') WHERE tenant_id=$1 AND project_id=$2 AND id=$3",[...scope,e.retryAfterSeconds]);
                }else{
                // SQL failures abort the transaction and the outer rollback
                // preserves the queued item; never fake a successful receipt.
                if((e as {code?:string}).code&&/^[0-9A-Z]{5}$/.test((e as {code:string}).code))throw e;
                await db.query("UPDATE replay_items SET attempts=attempts+1,error='publish_unconfirmed',updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3 AND record_id=$4",[...scope,item.record_id]);
                await db.query("UPDATE replay_jobs SET next_attempt_at=clock_timestamp()+($4::int*interval '1 second') WHERE tenant_id=$1 AND project_id=$2 AND id=$3",[...scope,Math.min(60,2**Math.min(item.attempts+1,6))]);
                }
              }
            }
          }
        }catch(e){if(e instanceof ReplayRejected)await set(item.status==='published'?'failed':'rejected',item.status==='published'?'outcome_unresolved':e.code);else throw e;}
      }
    }
    const counts=(await db.query("SELECT count(*) FILTER(WHERE status IN ('queued','published'))::int AS pending,count(*) FILTER(WHERE status IN ('failed','rejected'))::int AS failures,count(*) FILTER(WHERE status='stored')::int AS stored,count(*) FILTER(WHERE status='duplicate')::int AS duplicates,count(*) FILTER(WHERE status='suppressed')::int AS suppressed FROM replay_items WHERE tenant_id=$1 AND project_id=$2 AND job_id=$3",scope)).rows[0];
    if(counts.pending===0){
      await db.query("UPDATE replay_jobs SET status=$4,finished_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND id=$3",[...scope,counts.failures?'failed':'completed']);
      await audit(db,job,'dlq_replay_finished',counts);
    }
    await db.query('COMMIT');
  }catch(e){await db.query('ROLLBACK');throw e;}finally{db.release();}
}
export function startReplayJobs(publish:ReplayPublisher){
  let busy=false,stopped=false;
  const timer=setInterval(()=>{if(busy||stopped)return;busy=true;void replayTick(publish).catch(()=>console.error(JSON.stringify({code:'replay_runner_error'}))).finally(()=>{busy=false;});},2000);
  timer.unref();return()=>{stopped=true;clearInterval(timer);};
}
