// Real PG + in-process authenticated HTTP; publisher is deliberately injected
// to inspect ack/unknown/crash boundaries. This is not a live Kafka assertion.
import assert from 'node:assert/strict';
import { randomUUID,randomBytes } from 'node:crypto';
import { createApp } from '../apps/api/src/app';
import { pool } from '../packages/server/src/db';
import { digest } from '../packages/server/src/security';
import { indexDeadLetter,type DeadLetter } from '../packages/server/src/dead-letter';
import { persist } from '../packages/server/src/consume';
import { replayTick,type ReplayPublisher } from '../packages/server/src/dlq-replay';
import { identityHash } from '../packages/server/src/privacy';
import type { StoredEnvelope } from '../packages/schema/src/index';
const app=createApp({ready:()=>true,publish:async()=>{throw new Error('unexpected normal ingestion');}});
const origin=process.env.PUBLIC_ORIGIN!;
try{
  const database=(await pool.query('SELECT current_database() AS name')).rows[0].name;
  assert.match(database,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  async function fixture(role='admin',company?:{tenant:string;project:string}){
    const tenant=company?.tenant??randomUUID(),project=company?.project??randomUUID(),user=randomUUID(),session=randomBytes(24).toString('hex'),csrf=randomBytes(24).toString('hex');
    if(!company){await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'REPLAY INTEGRATION TEMP']);await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'REPLAY INTEGRATION TEMP']);}
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[user,`replay-${user}@integration.invalid`,'test-only-unused-hash']);
    await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[tenant,user,role]);
    await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(session),user,digest(csrf)]);
    return {tenant,project,user,headers:{cookie:`ccpanel_session=${session}`,origin,'content-type':'application/json','x-csrf-token':csrf},root:`/api/tenants/${tenant}/projects/${project}`};
  }
  const a=await fixture(),b=await fixture(),viewer=await fixture('viewer',a),analyst=await fixture('analyst',a);
  const api=(f:typeof a,path:string,body?:unknown,method=body===undefined?'GET':'POST')=>app.request(f.root+path,{method,headers:f.headers,...(body===undefined?{}:{body:JSON.stringify(body)})});
  async function source(f=a){
    const record:StoredEnvelope={tenant_id:f.tenant,project_id:f.project,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'repaired_event',distinct_id:randomUUID(),event_time:new Date().toISOString(),properties:{test:true},context:{},schema_version:1}};
    const raw=Buffer.from(JSON.stringify(record,null,2));
    const dead:DeadLetter={id:digest(randomUUID()),reason:'invalid_storage_value',source:{topic:'isolated-fixture',partition:0,offset:'0'},tenant_id:f.tenant,project_id:f.project,identity_hashes:[identityHash(f.tenant,f.project,record.event.distinct_id)],raw_base64:raw.toString('base64'),processor_version:2};
    await indexDeadLetter(dead,record);return {record,raw,dead};
  }
  const body=(id:string,request_id=randomUUID())=>({request_id,record_ids:[id],reason:'Retry after processor repair'});
  const tick=async(id:string,publish:ReplayPublisher)=>{await pool.query("UPDATE replay_jobs SET next_attempt_at=clock_timestamp()-interval '1 minute',checked_at='2000-01-01T00:00:00Z' WHERE project_id=$1 AND id=$2",[a.project,id]);await replayTick(publish);};
  const forbiddenPublish:ReplayPublisher=async()=>{throw new Error('unexpected_publish');};
  const first=await source(),other=await source(b),request=body(first.dead.id),path='/dlq/replays';
  for(const f of [viewer,analyst]){assert.equal((await api(f,path,request)).status,403);assert.equal((await api(f,path)).status,403);assert.equal((await api(f,path+'/'+request.request_id)).status,403);}
  assert.equal((await app.request(a.root+path,{headers:b.headers})).status,404,'foreign company hidden');
  assert.equal((await api(b,path,body(first.dead.id))).status,404,'foreign DLQ hidden');
  assert.equal((await api(a,path,body(other.dead.id))).status,404);
  assert.equal((await api(a,path,request)).status,202);
  assert.equal((await api(a,path,request)).status,200);
  assert.equal((await api(a,path,{...request,reason:'Changed reason for the same request'})).status,409);
  assert.equal((await api(b,path+'/'+request.request_id)).status,404,'foreign task hidden');
  await tick(request.request_id,async()=>{throw new Error('simulated acknowledgement timeout');});
  let state=await(await api(a,path+'/'+request.request_id)).json();
  assert.equal(state.status,'running');assert.equal(state.items[0].status,'queued');assert.equal(state.items[0].error,'publish_unconfirmed');
  let sent=0;
  await tick(request.request_id,async(raw)=>{sent++;assert.equal(raw.equals(first.raw),true);return {topic:'isolated-replay',partition:0,offset:'10'};});
  state=await(await api(a,path+'/'+request.request_id)).json();
  assert.equal(state.status,'running');assert.equal(state.items[0].status,'published');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM events')).rows[0].n,0,'Kafka ack is not raw-write success');
  await persist(first.record);
  await tick(request.request_id,forbiddenPublish);
  state=await(await api(a,path+'/'+request.request_id)).json();assert.equal(state.status,'completed');assert.equal(state.items[0].status,'stored');
  assert.equal((await api(a,path,request)).status,200);await tick(request.request_id,forbiddenPublish);assert.equal(sent,1);
  const duplicate=body(first.dead.id);assert.equal((await api(a,path,duplicate)).status,202);await tick(duplicate.request_id,forbiddenPublish);
  assert.equal((await(await api(a,path+'/'+duplicate.request_id)).json()).items[0].status,'duplicate');
  const cross=body(other.dead.id,request.request_id);assert.equal((await api(b,path,cross)).status,202,'request UUID is scoped per project');
  // Finish B independently so it cannot mask A scheduling assertions.
  await persist(other.record);await replayTick(forbiddenPublish);
  const revoked=await source(),revokedJob=body(revoked.dead.id);assert.equal((await api(a,path,revokedJob)).status,202);
  await pool.query("UPDATE tenant_memberships SET role='analyst' WHERE tenant_id=$1 AND user_id=$2",[a.tenant,a.user]);
  await tick(revokedJob.request_id,forbiddenPublish);
  assert.equal((await pool.query('SELECT error FROM replay_items WHERE project_id=$1 AND job_id=$2',[a.project,revokedJob.request_id])).rows[0].error,'admin_permission_revoked');
  await pool.query("UPDATE tenant_memberships SET role='admin' WHERE tenant_id=$1 AND user_id=$2",[a.tenant,a.user]);
  const suppressed=await source(),suppressedJob=body(suppressed.dead.id);assert.equal((await api(a,path,suppressedJob)).status,202);
  const deletion=randomUUID();await pool.query("INSERT INTO deletion_jobs(id,tenant_id,project_id,scope,subject_hash) VALUES($1,$2,$3,'user',$4)",[deletion,a.tenant,a.project,identityHash(a.tenant,a.project,suppressed.record.event.distinct_id)]);
  await pool.query('INSERT INTO deletion_barriers VALUES($1,$2,$3,$4,clock_timestamp(),clock_timestamp())',[a.tenant,a.project,identityHash(a.tenant,a.project,suppressed.record.event.distinct_id),deletion]);
  await tick(suppressedJob.request_id,forbiddenPublish);
  assert.equal((await(await api(a,path+'/'+suppressedJob.request_id)).json()).items[0].status,'suppressed');
  assert.equal((await api(a,path,body(suppressed.dead.id))).status,409,'barriers checked again when enqueuing');
  const poison=await source(),poisonJob=body(poison.dead.id);assert.equal((await api(a,path,poisonJob)).status,202);
  await tick(poisonJob.request_id,async()=>({topic:'isolated-replay',partition:0,offset:'42'}));
  const outcome=digest('isolated-replay:0:42');await indexDeadLetter({...poison.dead,id:outcome,source:{topic:'isolated-replay',partition:0,offset:'42'}},poison.record);
  await tick(poisonJob.request_id,forbiddenPublish);state=await(await api(a,path+'/'+poisonJob.request_id)).json();assert.equal(state.status,'failed');assert.equal(state.items[0].outcome_dlq_id,outcome);
  const crash=await source(),crashJob=body(crash.dead.id);assert.equal((await api(a,path,crashJob)).status,202);
  await tick(crashJob.request_id,async(raw)=>{assert.equal(raw.equals(crash.raw),true);throw new Error('simulated ack succeeded but receipt was lost');});
  await persist(crash.record); // Consumer completes after the unknown ack transaction ends.
  await tick(crashJob.request_id,forbiddenPublish);assert.equal((await(await api(a,path+'/'+crashJob.request_id)).json()).items[0].status,'duplicate');
  const missing=await source(),missingJob=body(missing.dead.id);assert.equal((await api(a,path,missingJob)).status,202);
  await pool.query('DELETE FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[a.tenant,a.project,missing.record.event.event_id]);await tick(missingJob.request_id,forbiddenPublish);
  assert.equal((await(await api(a,path+'/'+missingJob.request_id)).json()).items[0].error,'dedup_registration_missing');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE tenant_id=$1',[a.tenant])).rows[0].n,2);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM audit_logs WHERE tenant_id=$1 AND action='dlq_replay_requested' AND details->>'replay_job_id'=$2",[a.tenant,request.request_id])).rows[0].n,1);
  assert.equal((await pool.query('SELECT 1 FROM dlq_records WHERE id=$1',[first.dead.id])).rowCount,1,'replay never deletes its source');
  console.log(JSON.stringify({passed:true,role_and_company_isolation:true,request_idempotency:true,unconfirmed_retry:true,ack_not_raw_success:true,original_bytes:true,duplicate_once:true,permission_revocation:true,deletion_barrier:true,consumer_rejection:true,unknown_ack_recovery:true,missing_registration_rejected:true,source_preserved:true}));
}finally{await pool.end();}
