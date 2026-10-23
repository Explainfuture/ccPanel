// Disposable PG plus actual authenticated HTTP. No Kafka/network probe is added.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {createApp} from '../apps/api/src/app';
import {digest,token} from '../packages/server/src/security';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),foreign=randomUUID(),project=randomUUID(),other=randomUUID(),operator=randomUUID(),ordinary=randomUUID(),viewer=randomUUID(),foreignUser=randomUUID(),origin=process.env.PUBLIC_ORIGIN!;
  for(const id of[tenant,foreign])await pool.query("INSERT INTO tenants VALUES($1,'PRESSURE TEMP')",[id]);
  for(const [id,t]of[[project,tenant],[other,foreign]])await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'PRESSURE TEMP')",[id,t]);
  const headers=new Map<string,Record<string,string>>();
  for(const [id,t,role,isOperator]of[[operator,tenant,'admin',true],[ordinary,tenant,'admin',false],[viewer,tenant,'viewer',true],[foreignUser,foreign,'admin',true]] as const){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`pressure-${id}@integration.invalid`,'unused']);await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[t,id,role]);
    if(isOperator)await pool.query('INSERT INTO platform_operators(user_id) VALUES($1)',[id]);
    const secret=token();await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(secret),id,digest(token())]);headers.set(id,{cookie:'ccpanel_session='+secret});
  }
  let calls=0,fail=false;
  const deps={ready:()=>true,publish:async()=>{throw Error('unexpected_publish');},pressureAlerts:()=>{
    calls++;if(fail)throw new Error('private_failure_detail');const now=Date.now();
    return evaluatePressureAlerts({startedAt:now,partitions:[{partition:0,low:'0',high:'9007199254740993',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind,sampledAt:now,totalBytes:'21474836480',availableBytes:'2147483648',totalInodes:'100000',availableInodes:'50000'}))});
  }};
  let app=createApp(deps);const root=`/api/tenants/${tenant}/projects/${project}`,path=root+'/ingest-status';
  const get=(id=operator,target=path)=>app.request(target,{headers:headers.get(id)});
  assert.equal((await app.request(path)).status,401);
  for(const id of[ordinary,viewer,foreignUser])assert.equal((await get(id)).status,id===foreignUser?404:403);
  assert.equal(calls,0);assert.equal((await get(operator,root.replace(project,other)+'/ingest-status')).status,404);
  assert.equal((await get(operator,path+'?tenant_id='+foreign)).status,400);
  const response=await get();assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');const value=await response.json();
  assert.equal(value.pressure_alerts.signals[0].max_lag_messages,'9007199254740993');assert.deepEqual(value.pressure_alerts.signals.map((r:{state:string})=>r.state),['firing','clear','firing']);assert.equal(calls,1);
  fail=true;const failed=await get();assert.equal(failed.status,503);assert.deepEqual(await failed.json(),{error:'temporarily_unavailable'});fail=false;
  await pool.query('UPDATE platform_operators SET revoked_at=clock_timestamp() WHERE user_id=$1',[operator]);const before=calls;assert.equal((await get()).status,403);assert.equal(calls,before);
  await pool.query('UPDATE platform_operators SET revoked_at=NULL WHERE user_id=$1',[operator]);
  await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE user_id=$1",[operator]);assert.equal((await get()).status,403);assert.equal(calls,before);
  await pool.query('DELETE FROM tenant_memberships WHERE user_id=$1',[operator]);assert.equal((await get()).status,404);assert.equal(calls,before);
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,operator]);app=createApp(deps);
  const budgetStart=calls;for(let i=0;i<12;i++)assert.equal((await get()).status,200);const limited=await get();assert.equal(limited.status,429);assert.equal(limited.headers.get('retry-after'),'5');assert.equal(calls,budgetStart+12);
  assert.equal((await get(operator,root+'/metrics')).status,200,'platform read budget does not block project counters');
  console.log(JSON.stringify({pressure_alerts_http:true,operator_and_company_admin_required:true,foreign_scope_hidden:true,callback_after_authorization_only:true,precise_large_counters:true,independent_kafka_disk_signals:true,no_store:true,error_no_payload:true,revoked_grants_and_membership_denied:true,read_budget_12:true,business_read_unaffected:true}));
}finally{await pool.end();}
