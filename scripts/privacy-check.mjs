// Destructive checks are confined to newly created, explicitly labelled companies.
import pg from 'pg';
import { Kafka,logLevel } from 'kafkajs';
import { randomUUID,randomBytes,scryptSync,createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:3}),origin=process.env.PUBLIC_ORIGIN,base=process.env.TEST_API||'http://api:3000';
const producer=new Kafka({clientId:'privacy-acceptance',brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING}).producer({idempotent:true,maxInFlightRequests:1});
const fixtures=[];
async function fixture(role='admin'){
  const f={tenant:randomUUID(),project:randomUUID(),user:randomUUID(),password:randomBytes(24).toString('hex'),key:randomBytes(24).toString('hex')};f.email=`privacy-${f.user}@integration.invalid`;fixtures.push(f);
  const salt=randomBytes(16).toString('hex');
  await pool.query('INSERT INTO tenants VALUES($1,$2)',[f.tenant,'PRIVACY INTEGRATION TEMP']);
  await pool.query('INSERT INTO users VALUES($1,$2,$3)',[f.user,f.email,salt+':'+scryptSync(f.password,salt,64).toString('hex')]);
  await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[f.tenant,f.user,role]);
  await pool.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[f.project,f.tenant,'PRIVACY INTEGRATION TEMP',[origin]]);
  await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),f.tenant,f.project,createHash('sha256').update(f.key).digest('hex'),'privacy-test']);
  const r=await fetch(base+'/api/auth/login',{method:'POST',headers:{origin,'content-type':'application/json'},body:JSON.stringify({email:f.email,password:f.password})});assert.equal(r.status,200);const b=await r.json();
  f.headers={cookie:r.headers.get('set-cookie').split(';')[0],origin,'content-type':'application/json','x-csrf-token':b.csrf_token};f.root=`${base}/api/tenants/${f.tenant}/projects/${f.project}`;return f;
}
async function waitFor(fn,label){const deadline=Date.now()+30000;while(Date.now()<deadline){if(await fn())return;await new Promise(r=>setTimeout(r,250));}throw new Error('Timeout: '+label);}
async function api(f,path,body,method='POST'){return fetch(f.root+path,{method,headers:f.headers,...(body===undefined?{}:{body:JSON.stringify(body)})});}
async function send(f,event){const r=await fetch(base+'/v1/batch',{method:'POST',headers:{origin,'content-type':'text/plain'},body:JSON.stringify({write_key:f.key,schema_version:1,events:[event]})});assert.equal(r.status,202);}
async function completed(f,id){let value;await waitFor(async()=>{const r=await api(f,'/deletions',undefined,'GET');assert.equal(r.status,200);value=(await r.json()).jobs.find(j=>j.id===id);assert.notEqual(value?.status,'failed',value?.error);return value?.status==='completed';},'deletion cleanup');return value;}
try{
  await producer.connect();const a=await fixture(),b=await fixture(),viewer=await fixture('viewer'),anonymous=randomUUID(),lateAlias=randomUUID(),old=new Date(Date.now()-86400000).toISOString();
  const event=(name,user,extra={})=>({event_id:randomUUID(),event_name:name,distinct_id:user,event_time:old,properties:{test:true},context:{source:'privacy-acceptance'},schema_version:1,...extra});
  const journey=[event('privacy_start',anonymous),event('$identify','victim',{anonymous_id:anonymous}),event('privacy_purchase','victim',{anonymous_id:anonymous}),event('privacy_late',lateAlias),event('privacy_safe','unrelated')];
  for(const item of journey)await send(a,item);await send(b,{...journey[0],distinct_id:'victim'});
  await waitFor(async()=>Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[a.tenant])).rows[0].count)===5,'privacy fixture ingestion');
  const spec={kind:'trend',from:new Date(Date.now()-2*86400000).toISOString(),to:new Date().toISOString(),metric:'count'};
  const queued=await api(a,'/query-jobs',spec);assert.equal(queued.status,202);const oldJob=(await queued.json()).id;
  await waitFor(async()=>{const r=await(await api(a,'/query-jobs/'+oldJob,undefined,'GET')).json();assert.notEqual(r.status,'failed',r.error);return r.status==='succeeded';},'old cached result');
  const request={scope:'user',distinct_id:'victim',confirmation:'victim',reason:'Synthetic privacy acceptance'};
  assert.equal((await api(viewer,'/deletions',request)).status,403,'viewer cannot delete');
  assert.equal((await fetch(`${base}/api/tenants/${b.tenant}/projects/${b.project}/deletions`,{method:'POST',headers:a.headers,body:JSON.stringify(request)})).status,404,'cross-company deletion hidden');
  assert.equal((await api(a,'/deletions',{...request,confirmation:'wrong'})).status,400,'explicit confirmation required');
  const guard=await pool.connect();let deletionId;
  try{
    await guard.query('SELECT pg_advisory_lock(169202614),pg_advisory_lock(169202612)');
    const pending=await api(a,'/query-jobs',spec);assert.equal(pending.status,202);const pendingJob=(await pending.json()).id;
    const r=await api(a,'/deletions',request);assert.equal(r.status,202);deletionId=(await r.json()).id;
    assert.equal(Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[a.tenant])).rows[0].count),5,'raw cleanup intentionally held');
    const visible=await(await api(a,'/events',undefined,'GET')).json();assert.equal(visible.events.length,2,'barrier hides linked history before raw deletion');
    for(const id of [oldJob,pendingJob])assert.equal((await api(a,'/query-jobs/'+id,undefined,'GET')).status,404,'old query snapshots invalidated');
    assert.equal((await(await api(b,'/events',undefined,'GET')).json()).events.length,1,'same user in another company remains visible');
  }finally{await guard.query('SELECT pg_advisory_unlock(169202614),pg_advisory_unlock(169202612)');guard.release();}
  const done=await completed(a,deletionId);assert.equal(Number(done.removed_events),3);assert.equal(done.stream_status,'pending-stream-expiry');
  assert.equal(Number((await pool.query('SELECT count(*) FROM aggregation_jobs WHERE tenant_id=$1 AND event_id=ANY($2::uuid[])',[a.tenant,journey.slice(0,3).map(e=>e.event_id)])).rows[0].count),0,'aggregate contributions removed');
  assert.equal(Number((await pool.query('SELECT count(*) FROM event_dedup WHERE tenant_id=$1 AND event_id=ANY($2::uuid[])',[a.tenant,journey.slice(0,3).map(e=>e.event_id)])).rows[0].count),3,'dedup retained through replay window');
  const before=new Date(Date.now()-60000).toISOString(),lateIdentify=event('$identify','victim',{anonymous_id:lateAlias}),poison={...journey[2],event_id:'not-a-uuid'};
  await producer.send({topic:'ccpanel.events',acks:-1,messages:[journey[0],journey[2],lateIdentify,poison].map(e=>({key:a.project+':'+e.distinct_id,value:JSON.stringify({tenant_id:a.tenant,project_id:a.project,received_at:before,event:e})}))});
  await waitFor(async()=>Number((await pool.query("SELECT value FROM ingest_metrics WHERE tenant_id=$1 AND project_id=$2 AND metric='deletion_suppressed'",[a.tenant,a.project])).rows[0]?.value||0)>=4,'replay and poison suppressed');
  await waitFor(async()=>Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[a.tenant])).rows[0].count)===1,'late anonymous history removed');
  assert.equal(Number((await pool.query('SELECT count(*) FROM dlq_records WHERE tenant_id=$1',[a.tenant])).rows[0].count),0,'suppressed poison not copied into DLQ');
  await completed(a,deletionId);
  const projectRequest={scope:'project',confirmation:'PRIVACY INTEGRATION TEMP',reason:'Remove synthetic project'};
  assert.equal((await api(a,'/deletions',{...projectRequest,confirmation:'wrong'})).status,400);
  const removeProject=await api(a,'/deletions',projectRequest);assert.equal(removeProject.status,202);await completed(a,(await removeProject.json()).id);
  assert.equal((await api(a,'/keys',{})).status,409,'deleted project cannot regain a write key');
  const retry=await fetch(base+'/v1/batch',{method:'POST',headers:{origin,'content-type':'text/plain'},body:JSON.stringify({write_key:a.key,schema_version:1,events:[journey[4]]})});assert.equal(retry.status,401,'project write key revoked');
  assert.equal(Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[a.tenant])).rows[0].count),0);
  assert.equal(Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[b.tenant])).rows[0].count),1);
  console.log('PASS: confirmed scoped deletion, immediate visibility barrier, stale query invalidation, transactional cleanup, stable-ID and late-alias replay suppression, poison suppression, project revocation and tenant isolation');
}finally{
  await producer.disconnect();
  for(const f of fixtures){
    for(const table of ['deletion_barriers','deletion_jobs','query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects'])await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
    await pool.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await pool.query('DELETE FROM users WHERE id=$1',[f.user]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);
  }
  await pool.end();
}
