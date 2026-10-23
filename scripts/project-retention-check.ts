// Disposable PostgreSQL only. Synthetic historical records bypass SDK's 7-day TTL.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {changeProjectRetention,readProjectRetention} from '../packages/server/src/project-retention';
import {persist,EventConflict} from '../packages/server/src/consume';
import {retainNext} from '../packages/server/src/retention';
import {lockProjectPrivacy} from '../packages/server/src/privacy';
import {trends} from '../packages/server/src/trends';
import {createApp} from '../apps/api/src/app';
import {digest,token} from '../packages/server/src/security';
import {canonical} from '../packages/schema/src/index';
import {replayTick} from '../packages/server/src/dlq-replay';
import {withQuerySnapshot,lockQuerySnapshot} from '../packages/server/src/query-snapshot';
import type {StoredEnvelope} from '../packages/schema/src/index';
import type {TrendSpec} from '../packages/server/src/query-spec';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),foreign=randomUUID(),project=randomUUID(),other=randomUUID(),admin=randomUUID(),viewer=randomUUID();
  for(const id of[tenant,foreign])await pool.query("INSERT INTO tenants VALUES($1,'RETENTION POLICY TEMP')",[id]);
  for(const [id,t]of[[project,tenant],[other,foreign]])await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'RETENTION POLICY TEMP')",[id,t]);
  for(const [id,role]of[[admin,'admin'],[viewer,'viewer']]){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`retention-${id}@integration.invalid`,'unused']);
    await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[tenant,id,role]);
  }
  const update=(days:number,version:number,actor=admin,p=project)=>changeProjectRetention(tenant,p,actor,{raw_days:days,expected_version:version});
  const state=async()=>(await pool.query('SELECT retention_days,retention_version,retention_floor,privacy_epoch FROM projects WHERE id=$1',[project])).rows[0];
  const audits=async()=>Number((await pool.query("SELECT count(*) AS n FROM audit_logs WHERE action='retention_policy_changed'")).rows[0].n);
  const count=async(table:string,p=project)=>Number((await pool.query(`SELECT count(*) AS n FROM ${table} WHERE project_id=$1`,[p])).rows[0].n);
  const record=(age:number,p=project,t=tenant):StoredEnvelope=>({tenant_id:t,project_id:p,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'view',distinct_id:randomUUID(),event_time:new Date(Date.now()-age*86400000).toISOString(),properties:{},context:{},schema_version:1}});
  for(const r of[record(10),record(0.01),record(10,other,foreign)])assert.equal(await persist(r),'inserted');
  assert.equal(await count('visible_events'),2);
  await assert.rejects(update(7,1,viewer),{status:403});
  await assert.rejects(update(7,1,admin,other),{status:404});
  assert.equal((await update(30,1)).changed,false);assert.equal(await audits(),0);
  const blocked=await pool.connect();
  try{await blocked.query('BEGIN');await lockProjectPrivacy(blocked,project,true);await assert.rejects(update(7,1),{status:409,message:'retention_policy_busy'});}finally{await blocked.query('ROLLBACK');blocked.release();}
  assert.equal((await state()).retention_version,1);
  assert.equal((await update(7,1)).changed,true);
  const shortened=await state();assert.equal(shortened.retention_days,7);assert.equal(shortened.privacy_epoch,1);
  assert.equal(await count('events'),2);assert.equal(await count('visible_events'),1);assert.equal(await count('visible_events',other),1);
  await assert.rejects(update(30,1),{status:409,message:'retention_policy_conflict'});
  const result=await trends(tenant,project,{kind:'trend',from:new Date(Date.now()-20*86400000).toISOString(),to:new Date().toISOString(),metric:'count',filters:[],group_by:[]});
  assert.equal(result.summary.events,1);assert.equal(result.data_coverage.expired_overlap,true);
  assert.ok(Date.parse(result.data_coverage.retained_from)>=shortened.retention_floor.getTime());
  await update(30,2);assert.equal(await count('visible_events'),1,'extension cannot resurrect uncollected raw rows');
  const expired=record(10);assert.equal(await persist(expired),'suppressed');assert.equal(await persist(expired),'duplicate');
  await assert.rejects(persist({...expired,event:{...expired.event,properties:{changed:true}}}),error=>error instanceof EventConflict&&error.reason==='payload_conflict');
  const identify=record(10);identify.event.event_name='$identify';identify.event.anonymous_id='expired-anonymous';
  assert.equal(await persist(identify),'suppressed');assert.equal(await count('identity_links'),0);assert.equal(await count('aggregation_jobs'),2);assert.equal(await count('event_dedup'),4);
  // A failed audit must roll back policy, floor, epoch and version together.
  const before=await state(),beforeAudits=await audits();
  await pool.query("CREATE FUNCTION fail_retention_audit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.action='retention_policy_changed' THEN RAISE EXCEPTION 'synthetic audit failure'; END IF; RETURN NEW; END$$; CREATE TRIGGER fail_retention_audit BEFORE INSERT ON audit_logs FOR EACH ROW EXECUTE FUNCTION fail_retention_audit()");
  try{await assert.rejects(update(1,3),/synthetic audit failure/);}finally{await pool.query('DROP TRIGGER fail_retention_audit ON audit_logs; DROP FUNCTION fail_retention_audit()');}
  assert.deepEqual(await state(),before);assert.equal(await audits(),beforeAudits);
  await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[tenant,admin]);
  await assert.rejects(update(1,3),{status:403});assert.deepEqual(await state(),before);
  const db=await pool.connect();try{assert.equal(await retainNext(db),'completed');assert.equal(await retainNext(db),'empty');}finally{db.release();}
  assert.equal(await count('events'),1);assert.equal(await count('aggregation_jobs'),1);assert.equal(await count('event_dedup'),4);assert.equal(await count('events',other),1);
  await pool.query("UPDATE tenant_memberships SET role='admin' WHERE tenant_id=$1 AND user_id=$2",[tenant,admin]);
  const secret=token(),csrf=token(),key=token(),origin=process.env.PUBLIC_ORIGIN!;
  await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(secret),admin,digest(csrf)]);
  await pool.query("INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,'synthetic retention check')",[randomUUID(),tenant,project,digest(key)]);
  let published=0;
  // Explicit synthetic open admission; no broker/disk sampler runs in this suite.
  const app=createApp({ready:()=>true,admissionStatus:()=>({allowed:true,reason:null,integrityReason:null,retryAfterSeconds:0,totalLagMessages:'0',maxLagMessages:'0',remainingMessages:256,reservedMessages:0,unknownMessages:0,pendingRequests:0,reservationCount:0}),publish:async records=>{published+=records.length;}});
  const root=`/api/tenants/${tenant}/projects/${project}`,headers={cookie:`ccpanel_session=${secret}`,origin,'content-type':'application/json','x-csrf-token':csrf};
  const replay=async(r:StoredEnvelope)=>{
    const id=digest(randomUUID());
    await pool.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash,status) VALUES($1,$2,$3,$4,'failed')",[tenant,project,r.event.event_id,digest(canonical(r.event))]);
    await pool.query("INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES($1,$2,$3,'invalid_storage_value',$4)",[id,tenant,project,JSON.stringify({processor_version:2,raw_base64:Buffer.from(JSON.stringify(r)).toString('base64')})]);
    return app.request(root+'/dlq/replays',{method:'POST',headers,body:JSON.stringify({request_id:randomUUID(),record_ids:[id],reason:'Retry synthetic repair'})});
  };
  // Admit before policy change, then recheck the queued job at publication.
  const queued=await replay(record(2));assert.equal(queued.status,202);const queuedId=(await queued.json()).id;
  const oldQuery=randomUUID(),spec:TrendSpec={kind:'trend',from:new Date(Date.now()-3*86400000).toISOString(),to:new Date().toISOString(),metric:'count',filters:[],group_by:[]};
  // Synthetic saved output: this checks epoch invalidation, not query execution.
  await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,result,privacy_epoch,finished_at) SELECT $1,$2,$3,$4,$5,'succeeded',$6,privacy_epoch,clock_timestamp() FROM projects WHERE id=$3",[oldQuery,tenant,project,admin,JSON.stringify(spec),JSON.stringify({summary:{events:1}})]);
  const download=await app.request(root+`/query-jobs/${oldQuery}/download`,{headers});assert.equal(download.status,200);await download.text();
  await update(1,3);
  assert.equal((await app.request(root+`/query-jobs/${oldQuery}`,{headers})).status,404);
  assert.equal((await app.request(root+`/query-jobs/${oldQuery}/download`,{headers})).status,404);
  const rejected=await replay(record(2));assert.equal(rejected.status,409);assert.equal((await rejected.json()).error,'project_retention_expired');
  await replayTick(async()=>{throw new Error('expired replay must not publish');});
  const item=(await pool.query('SELECT status,error FROM replay_items WHERE job_id=$1',[queuedId])).rows[0];
  assert.equal(item.status,'rejected');assert.equal(item.error,'project_retention_expired');
  const recent=record(0.01).event,old=record(2).event;
  const ingest=(events:typeof recent[])=>app.request('/v1/batch',{method:'POST',body:JSON.stringify({write_key:key,schema_version:1,events})});
  const invalid=await ingest([recent,old]);assert.equal(invalid.status,400);const invalidBody=await invalid.json();
  assert.deepEqual(invalidBody.errors,[{index:1,code:'project_retention_expired'}]);assert.equal(published,0,'batch admission is all or nothing');
  const accepted=await ingest([recent]);assert.equal(accepted.status,202);assert.equal(published,1);
  await assert.rejects(withQuerySnapshot(tenant,other,async()=>{throw new Error('foreign callback must not run');}),{status:404});
  let snapshotCount=0;
  await withQuerySnapshot(tenant,project,async run=>{
    const before=(await run('SELECT count(*)::int AS n,CURRENT_TIMESTAMP AS at FROM visible_events WHERE tenant_id=$1 AND project_id=$2',[tenant,project])).rows[0];snapshotCount=before.n;
    await assert.rejects(update(2,4),{status:409,message:'retention_policy_busy'});
    assert.equal(await persist(record(0.001)),'inserted','normal intake can proceed during a shared query');
    const after=(await run('SELECT count(*)::int AS n,CURRENT_TIMESTAMP AS at FROM visible_events WHERE tenant_id=$1 AND project_id=$2',[tenant,project])).rows[0];
    assert.equal(after.n,before.n,'one snapshot excludes events inserted between subqueries');assert.equal(after.at.getTime(),before.at.getTime());
  });
  const fresh=await trends(tenant,project,spec);assert.equal(fresh.summary.events,snapshotCount+1);
  // Deterministic race: a policy commits after snapshot creation but before
  // shared lock acquisition. Row locking must reject the outdated snapshot.
  const raced=await pool.connect();
  try{
    await raced.query('BEGIN ISOLATION LEVEL REPEATABLE READ');await raced.query('SELECT 1');
    await update(2,4);
    await assert.rejects(lockQuerySnapshot(raced,tenant,project),(error:unknown)=>(error as {code:string}).code==='40001');
  }finally{await raced.query('ROLLBACK');raced.release();}
  await assert.rejects(withQuerySnapshot(tenant,project,async()=>{throw new Error('synthetic read failure');}),/synthetic read failure/);
  await update(3,5); // Prior rollback released both locks and connection.
  const busy=await pool.connect();
  try{
    await busy.query('BEGIN');await lockProjectPrivacy(busy,project,true);
    for(const [path,input]of[['/query',spec],['/behavior',{kind:'funnel',from:spec.from,to:spec.to,steps:['view','finish'],window_hours:24}]] as const){
      const response=await app.request(root+path,{method:'POST',headers,body:JSON.stringify(input)});assert.equal(response.status,429);assert.equal(response.headers.get('retry-after'),'1');assert.equal((await response.json()).error,'query_busy');
    }
  }finally{await busy.query('ROLLBACK');busy.release();}
  await assert.rejects(withQuerySnapshot(tenant,project,async run=>{await run('SELECT pg_sleep(6)');}),{status:504,message:'query_timeout'});
  await update(4,6); // SQL timeout also rolls back and releases the shared lock.
  const viewerSecret=token(),viewerCsrf=token();
  await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(viewerSecret),viewer,digest(viewerCsrf)]);
  const viewerHeaders={...headers,cookie:`ccpanel_session=${viewerSecret}`,'x-csrf-token':viewerCsrf};
  const policyApi=(body?:unknown,h=headers,path=root+'/retention')=>app.request(path,{method:body===undefined?'GET':'PATCH',headers:h,...(body===undefined?{}:{body:JSON.stringify(body)})});
  const readable=await policyApi(undefined,viewerHeaders);assert.equal(readable.status,200);assert.equal(readable.headers.get('cache-control'),'no-store');
  const policy=await readable.json();assert.equal(policy.policy.raw_days,4);assert.equal(policy.policy.version,7);assert.equal(policy.limits.dedup_days,45);assert.equal(policy.limits.dlq_days,14);assert.ok(Date.parse(policy.policy.effective_from)<=Date.parse(policy.observed_at));
  assert.equal((await app.request(root+'/retention')).status,401);
  assert.equal((await policyApi({raw_days:3,expected_version:7},{...headers,'x-csrf-token':'invalid'})).status,403);
  assert.equal((await policyApi({raw_days:3,expected_version:7},{...headers,origin:'https://foreign.invalid'})).status,403);
  assert.equal((await policyApi({raw_days:3,expected_version:7},viewerHeaders)).status,403);
  assert.equal((await policyApi(undefined,headers,`/api/tenants/${tenant}/projects/${other}/retention`)).status,404);
  assert.equal((await policyApi(undefined,headers,`/api/tenants/${foreign}/projects/${other}/retention`)).status,404);
  assert.equal((await policyApi(undefined,headers,root+'/retention?tenant_id='+foreign)).status,400);
  for(const body of[{raw_days:31,expected_version:7},{raw_days:1.5,expected_version:7},{raw_days:3,expected_version:7,tenant_id:foreign}])assert.equal((await policyApi(body)).status,400);
  assert.equal((await app.request(root+'/retention',{method:'PATCH',headers,body:'{'})).status,400);
  assert.equal((await app.request(root+'/retention',{method:'PATCH',headers,body:'x'.repeat(1025)})).status,413);
  const unchanged=await policyApi({raw_days:4,expected_version:7});assert.equal(unchanged.status,200);assert.equal((await unchanged.json()).changed,false);
  const auditBeforeRace=await audits();
  const outcomes=await Promise.all([policyApi({raw_days:3,expected_version:7}),policyApi({raw_days:2,expected_version:7})]);assert.deepEqual(outcomes.map(r=>r.status).sort(),[200,409]);
  assert.equal((await state()).retention_version,8);assert.equal(await audits(),auditBeforeRace+1);
  const current=await state();
  for(let i=0;i<3;i++)assert.equal((await policyApi({raw_days:current.retention_days,expected_version:8})).status,200);
  const limited=await policyApi({raw_days:1,expected_version:8});assert.equal(limited.status,429);assert.equal(limited.headers.get('retry-after'),'60');assert.equal((await state()).retention_version,8);
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[tenant,viewer]);
  assert.equal((await policyApi(undefined,viewerHeaders)).status,404);
  await assert.rejects(readProjectRetention(tenant,project,viewer),{status:404});
  await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[tenant,admin]);
  assert.equal((await policyApi({raw_days:1,expected_version:8})).status,403);assert.equal((await state()).retention_version,8);
  console.log(JSON.stringify({retention_settings_http:true,company_member_read:true,admin_only_write:true,csrf_origin_checked:true,scope_hidden:true,strict_input_and_size:true,concurrent_version_single_audit:true,removed_member_denied:true,revoked_admin_denied:true}));
  console.log(JSON.stringify({query_snapshot:true,repeatable_rows_and_clock:true,ingestion_during_read:true,policy_write_busy_during_read:true,post_snapshot_policy_commit_rejected:true,foreign_snapshot_hidden:true,sync_trend_and_behavior_busy_http:true,error_and_timeout_release_locks:true}));
  console.log(JSON.stringify({passed:true,schema21:true,fresh_authorization:true,foreign_scope_hidden:true,no_op_no_audit:true,version_conflict:true,privacy_lock_busy:true,logical_expiry_immediate:true,extension_no_resurrection:true,expired_consumer_dedup:true,expired_identify_no_link:true,audit_atomic_rollback:true,physical_expiry_keeps_45_day_dedup:true,replay_admission_and_queued_recheck:true,http_batch_all_or_nothing:true,old_result_and_download_hidden:true}));
}finally{await pool.end();}
