// Run inside the API network with DATABASE_URL; uses isolated, labelled test companies.
import pg from 'pg';
import { randomUUID,randomBytes,scryptSync,createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:2});
const base=process.env.TEST_API||'http://api:3000';
const origin=process.env.PUBLIC_ORIGIN;
const hash=v=>createHash('sha256').update(v).digest('hex');
const password=randomBytes(24).toString('hex');
const salt=randomBytes(16).toString('hex');
const passwordHash=salt+':'+scryptSync(password,salt,64).toString('hex');
const fixtures=[];
async function fixture(role='admin') {
  const tenant=randomUUID(),project=randomUUID(),user=randomUUID(),key='integration_'+randomBytes(24).toString('hex'),email=`test-${user}@integration.invalid`;
  await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'INTEGRATION TEMP']);
  await pool.query('INSERT INTO users VALUES($1,$2,$3)',[user,email,passwordHash]);
  await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[tenant,user,role]);
  await pool.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[project,tenant,'INTEGRATION TEMP',[origin]]);
  await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,hash(key),'integration']);
  const f={tenant,project,user,key,email};fixtures.push(f);return f;
}
async function login(f) {
  const r=await fetch(base+'/api/auth/login',{method:'POST',headers:{origin,'content-type':'application/json'},body:JSON.stringify({email:f.email,password})});
  assert.equal(r.status,200,'login');const b=await r.json();return {cookie:r.headers.get('set-cookie').split(';')[0],origin,'x-csrf-token':b.csrf_token};
}
async function send(f,event,extra={}) {
  return fetch(base+'/v1/batch',{method:'POST',headers:{origin,'content-type':'text/plain'},body:JSON.stringify({write_key:f.key,schema_version:1,events:[event],...extra})});
}
async function waitFor(fn,label) {
  const end=Date.now()+25000;
  while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,250));}
  throw new Error('Timed out: '+label);
}
try {
  const a=await fixture(),b=await fixture(),viewer=await fixture('viewer');
  const h=await login(a),v=await login(viewer);
  const root=`/api/tenants/${a.tenant}/projects/${a.project}`;
  assert.equal((await fetch(base+`/api/tenants/${b.tenant}/projects`,{headers:h})).status,404,'other company hidden');
  assert.equal((await fetch(base+`/api/tenants/${a.tenant}/projects/${b.project}/events`,{headers:h})).status,404,'forged project blocked');
  assert.equal((await fetch(base+`/api/tenants/${viewer.tenant}/projects`,{method:'POST',headers:{...v,'content-type':'application/json'},body:JSON.stringify({name:'forbidden',origins:[]})})).status,403,'viewer cannot create');
  const e={event_id:randomUUID(),event_name:'integration_click',distinct_id:'same-user',event_time:new Date().toISOString(),properties:{button:'buy'},context:{source:'integration'},schema_version:1};
  assert.equal((await send(a,e,{tenant_id:b.tenant})).status,400,'tenant forgery blocked');
  for(const f of [a,a,b]) {
    const r=await send(f,e);assert.equal(r.status,202,'Kafka ack');assert.deepEqual((await r.json()).accepted_event_ids,[e.event_id]);
  }
  await waitFor(async()=>Number((await pool.query('SELECT count(*) FROM events WHERE event_id=$1 AND project_id=ANY($2::uuid[])',[e.event_id,[a.project,b.project]])).rows[0].count)===2,'dedup and separate companies');
  assert.equal(Number((await pool.query('SELECT count(*) FROM aggregation_jobs WHERE event_id=$1',[e.event_id])).rows[0].count),2,'one aggregate task per project');
  const list=await fetch(base+root+'/events',{headers:h});assert.equal(list.status,200);assert.equal((await list.json()).events.length,1);
  const trend=await fetch(base+root+'/trends',{headers:h});assert.equal(trend.status,200);const stats=await trend.json();assert.equal(stats.summary.events,1);assert.equal(stats.summary.users,1);assert.equal(stats.series.reduce((sum,row)=>sum+row.events,0),1);
  assert.equal((await send(a,{...e,properties:{button:'changed'}})).status,202);
  await waitFor(async()=>(await pool.query("SELECT 1 FROM dlq_records WHERE tenant_id=$1 AND reason='payload_conflict'",[a.tenant])).rowCount===1,'conflict DLQ');
  const original=await pool.query('SELECT properties FROM events WHERE project_id=$1 AND event_id=$2',[a.project,e.event_id]);assert.equal(original.rows[0].properties.button,'buy');
  const measured=[{amount:5},{amount:15,tag:null},{amount:'20',tag:'web'},{amount:null,tag:'app'},{}].map(properties=>({...e,event_id:randomUUID(),event_name:'measure',properties}));
  for(const item of measured)assert.equal((await send(a,item)).status,202);
  await waitFor(async()=>Number((await pool.query("SELECT count(*) FROM events WHERE project_id=$1 AND event_name='measure'",[a.project])).rows[0].count)===5,'query fixtures');
  const spec={kind:'trend',from:new Date(Date.now()-86400000).toISOString(),to:new Date(Date.now()+1000).toISOString(),metric:'avg',property:'amount',filters:[],group_by:['tag']};
  const queried=await fetch(base+root+'/query',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify(spec)});assert.equal(queried.status,200);const result=await queried.json();assert.equal(result.summary.value,10);assert.equal(result.summary.excluded,4);assert.equal(result.groups.length,4);
  const filtered=await fetch(base+root+'/query',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify({...spec,metric:'count',group_by:[],filters:[{key:'tag',op:'neq',value:null}]})});assert.equal(filtered.status,200);assert.equal((await filtered.json()).summary.value,2,'missing/null excluded from neq');
  const saved=await fetch(base+root+'/reports',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify({name:'INTEGRATION AVG',spec})});assert.equal(saved.status,201);const reportId=(await saved.json()).id;
  assert.equal((await fetch(base+root+'/dashboards',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify({name:'INTEGRATION BOARD',report_ids:[reportId]})})).status,201);
  const start=Date.now()-3*86400000,anonymous=randomUUID();
  const journey=[
    {...e,event_id:randomUUID(),event_name:'journey_start',distinct_id:anonymous,anonymous_id:anonymous,event_time:new Date(start).toISOString()},
    {...e,event_id:randomUUID(),event_name:'$identify',distinct_id:'identified-user',anonymous_id:anonymous,event_time:new Date(start+1000).toISOString()},
    {...e,event_id:randomUUID(),event_name:'journey_buy',distinct_id:'identified-user',anonymous_id:anonymous,event_time:new Date(start+3600000).toISOString()},
    {...e,event_id:randomUUID(),event_name:'journey_return',distinct_id:'identified-user',anonymous_id:anonymous,event_time:new Date(start+86400000).toISOString()},
  ];
  for(const item of journey)assert.equal((await send(a,item)).status,202);
  await waitFor(async()=>Number((await pool.query("SELECT count(*) FROM events WHERE project_id=$1 AND event_name=ANY($2::text[])",[a.project,journey.map(j=>j.event_name)])).rows[0].count)===4,'identity and journey fixtures');
  const behavior={from:new Date(start-1000).toISOString(),to:new Date(start+12*3600000).toISOString(),filters:[]};
  const runBehavior=async extra=>{const r=await fetch(base+root+'/behavior',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify({...behavior,...extra})});assert.equal(r.status,200);return r.json();};
  const funnel=await runBehavior({kind:'funnel',steps:['journey_start','journey_buy'],window_hours:24});assert.deepEqual(funnel.steps.map(s=>s.users),[1,1]);
  const retention=await runBehavior({kind:'retention',start_event:'journey_start',return_event:'journey_return',days:3});assert.equal(retention.cohorts[0].size,1);assert.equal(retention.cohorts[0].retention[1].users,1);assert.equal(retention.cohorts[0].retention[3].users,null);
  const segment=await runBehavior({kind:'cohort',event_name:'journey_buy',count_op:'gte',count:1});assert.equal(segment.total,1);assert.equal(segment.users[0].user,'identified-user');
  const timeline=await fetch(base+root+'/users/events?'+new URLSearchParams({distinct_id:anonymous}),{headers:h});assert.equal(timeline.status,200);const history=await timeline.json();assert.equal(history.canonical_id,'identified-user');assert.equal(history.events.length,4);
  const queued=await fetch(base+root+'/query-jobs',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify(spec)});assert.equal(queued.status,202);const job=(await queued.json()).id;
  let completed;
  await waitFor(async()=>{const r=await fetch(base+root+'/query-jobs/'+job,{headers:h});assert.equal(r.status,200);completed=await r.json();assert.notEqual(completed.status,'failed',completed.error);return completed.status==='succeeded';},'async trend job');
  assert.equal(completed.result.summary.value,10,'async result matches synchronous result');
  assert.equal((await fetch(base+`/api/tenants/${b.tenant}/projects/${b.project}/query-jobs/${job}`,{headers:h})).status,404,'cross-company job hidden');
  const behaviorJob=await fetch(base+root+'/query-jobs',{method:'POST',headers:{...h,'content-type':'application/json'},body:JSON.stringify({...behavior,kind:'funnel',steps:['journey_start','journey_buy'],window_hours:24})});assert.equal(behaviorJob.status,202);const funnelJob=(await behaviorJob.json()).id;
  await waitFor(async()=>{const result=await(await fetch(base+root+'/query-jobs/'+funnelJob,{headers:h})).json();assert.notEqual(result.status,'failed',result.error);if(result.status!=='succeeded')return false;assert.deepEqual(result.result.steps.map(s=>s.users),[1,1]);return true;},'async behavior job');
  assert.equal((await fetch(base+root+'/query-jobs/'+funnelJob,{method:'DELETE',headers:h})).status,200,'cancel discards result');
  const cancelled=await(await fetch(base+root+'/query-jobs/'+funnelJob,{headers:h})).json();assert.equal(cancelled.status,'cancelled');assert.equal(cancelled.result,null);
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'viewer')",[b.tenant,a.user]);
  assert.equal((await fetch(base+`/api/tenants/${b.tenant}/projects`,{headers:h})).status,200,'same account can read joined company');
  assert.equal((await fetch(base+`/api/tenants/${b.tenant}/projects/${b.project}/keys`,{method:'POST',headers:{...h,'content-type':'application/json'},body:'{}'})).status,403,'role scoped to current company');
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[a.tenant,a.user]);
  assert.equal((await fetch(base+root+'/events',{headers:h})).status,404,'revocation immediately enforced');
  assert.equal((await fetch(base+root+'/query-jobs/'+job,{headers:h})).status,404,'revoked member cannot read old task result');
  console.log('PASS: Kafka/dedup/DLQ, tenant isolation and role switching, typed aggregates/filters, saved reports/dashboards, identity-linked funnel/retention/cohort, and revocation');
} finally {
  await pool.query('DELETE FROM tenant_memberships WHERE user_id=ANY($1::uuid[])',[fixtures.map(f=>f.user)]);
  for(const f of fixtures) {
    for(const table of ['query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','projects','tenant_memberships']) await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
    await pool.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await pool.query('DELETE FROM users WHERE id=$1',[f.user]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);
  }
  await pool.end();
}
