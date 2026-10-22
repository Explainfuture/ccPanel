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
  assert.equal((await send(a,{...e,properties:{button:'changed'}})).status,202);
  await waitFor(async()=>(await pool.query("SELECT 1 FROM dlq_records WHERE tenant_id=$1 AND reason='payload_conflict'",[a.tenant])).rowCount===1,'conflict DLQ');
  const original=await pool.query('SELECT properties FROM events WHERE project_id=$1 AND event_id=$2',[a.project,e.event_id]);assert.equal(original.rows[0].properties.button,'buy');
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[a.tenant,a.user]);
  assert.equal((await fetch(base+root+'/events',{headers:h})).status,404,'revocation immediately enforced');
  console.log('PASS: real Kafka ack, duplicate dedup, aggregate uniqueness, cross-company isolation, viewer restrictions, payload conflict DLQ, and membership revocation');
} finally {
  for(const f of fixtures) {
    for(const table of ['dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','projects','tenant_memberships']) await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
    await pool.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await pool.query('DELETE FROM users WHERE id=$1',[f.user]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);
  }
  await pool.end();
}
