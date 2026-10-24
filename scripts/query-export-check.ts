// Disposable PG only. Completed result rows below are synthetic export fixtures,
// not proof of query execution (covered by report-edit-check.ts).
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {token,digest} from '../packages/server/src/security';
import {createApp} from '../apps/api/src/app';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),foreignTenant=randomUUID(),project=randomUUID(),foreignProject=randomUUID(),actor=randomUUID(),creator=randomUUID(),viewer=randomUUID(),foreign=randomUUID();
  for(const id of[tenant,foreignTenant])await pool.query('INSERT INTO tenants VALUES($1,$2)',[id,'EXPORT TEMP']);
  for(const [id,t]of[[project,tenant],[foreignProject,foreignTenant]])await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[id,t,'EXPORT TEMP']);
  const headers=new Map<string,Record<string,string>>(),origin=process.env.PUBLIC_ORIGIN!;
  for(const [id,t,role]of[[actor,tenant,'admin'],[creator,tenant,'analyst'],[viewer,tenant,'viewer'],[foreign,foreignTenant,'admin']]){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`export-${id}@integration.invalid`,'unused']);await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[t,id,role]);
    const secret=token(),csrf=digest(secret+':csrf');await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(secret),id,digest(csrf)]);headers.set(id,{cookie:'ccpanel_session='+secret,origin,'x-csrf-token':csrf,'content-type':'application/json'});
  }
  const spec={kind:'trend',from:new Date(Date.now()-86400000).toISOString(),to:new Date().toISOString(),metric:'count',filters:[],group_by:[]},result={spec,summary:{value:2,events:2,users:1,excluded:0},series:[],groups:[],other:null,groups_truncated:false,computed_at:new Date().toISOString(),synthetic_text:'=1+2',numeric_string:'20'};
  const good=randomUUID(),pending=randomUUID(),large=randomUUID(),expired=randomUUID();
  for(const [id,status,output]of[[good,'succeeded',result],[pending,'queued',null],[large,'succeeded',{text:'x'.repeat(1024*1024+1)}],[expired,'succeeded',result]])await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,result,privacy_epoch,finished_at) SELECT $1,$2,$3,$4,$5,$6,$7,privacy_epoch,clock_timestamp() FROM projects WHERE id=$3",[id,tenant,project,creator,JSON.stringify(spec),status,output===null?null:JSON.stringify(output)]);
  await pool.query("UPDATE query_jobs SET created_at=now()-interval '2 days',expires_at=now()-interval '1 day' WHERE id=$1",[expired]);
  const root=`/api/tenants/${tenant}/projects/${project}/query-jobs`,other=`/api/tenants/${foreignTenant}/projects/${foreignProject}/query-jobs`;
  const makeApp=()=>createApp({ready:()=>true,publish:async()=>{throw Error('unexpected_publish');}});let app=makeApp();
  const req=(user:string,id=good,suffix='?format=json',base=root)=>app.request(base+'/'+id+'/download'+suffix,{headers:headers.get(user)});
  const json=await req(actor);assert.equal(json.status,200);assert.equal(json.headers.get('cache-control'),'no-store');assert.match(json.headers.get('content-disposition')!,new RegExp(good+'\\.json'));
  const text=await json.text();assert.equal(Number(json.headers.get('content-length')),Buffer.byteLength(text));const document=JSON.parse(text);assert.equal(document.tenant_id,tenant);assert.equal(document.project_id,project);assert.equal(document.result.numeric_string,'20');assert.equal(document.result.synthetic_text,'=1+2');
  const csv=await req(creator,good,'?format=csv');assert.equal(csv.status,200);const bytes=Buffer.from(await csv.arrayBuffer());assert.equal(bytes.subarray(0,3).toString('hex'),'efbbbf');assert.ok(bytes.toString('utf8').includes('"string","\t=1+2"'));assert.equal(Number(csv.headers.get('content-length')),bytes.length);
  const audit=(await pool.query("SELECT details FROM audit_logs WHERE tenant_id=$1 AND action='query_export_prepared'",[tenant])).rows;assert.equal(audit.length,2);assert.ok(audit.every(r=>Object.keys(r.details).sort().join(',')==='bytes,format,job_id,project_id'));
  for(const [user,id,suffix,base,status]of[[viewer,good,'',root,403],[foreign,good,'',other,404],[actor,randomUUID(),'',root,404],[actor,pending,'',root,409],[actor,large,'',root,413],[actor,expired,'',root,404],[actor,good,'?format=exe',root,400],[actor,good,'?format=csv&format=json',root,400],[actor,good,'?tenant_id=other',root,400]] as const){const response=await req(user,id,suffix,base);assert.equal(response.status,status);assert.equal(response.headers.has('content-disposition'),false);await response.text();}
  app=makeApp();
  const unread=await req(actor);assert.equal(unread.status,200);const concurrent=await req(actor);assert.equal(concurrent.status,429);assert.equal(concurrent.headers.get('retry-after'),'5');await concurrent.text();await unread.body!.cancel();
  const recovered=await req(actor);assert.equal(recovered.status,200);await recovered.text();
  const outgoing=new EventEmitter() as EventEmitter&{destroy:()=>void};outgoing.destroy=()=>{outgoing.emit('close');};
  const adapter=await app.request(root+'/'+good+'/download',{headers:headers.get(actor)},{outgoing});assert.equal(adapter.status,200);await adapter.text();
  assert.equal((await req(actor)).status,429,'stream EOF alone must not release Node adapter quota');outgoing.emit('finish');assert.equal(outgoing.listenerCount('finish')+outgoing.listenerCount('close'),0);
  const afterFlush=await req(actor);assert.equal(afterFlush.status,200);await afterFlush.text();
  app=makeApp();
  const changing=await pool.connect();let changed=false;
  try{
    await changing.query('BEGIN');await changing.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[tenant,actor]);
    const waiting=req(actor);let blocked=false;const deadline=Date.now()+1500;
    while(Date.now()<deadline){const row=(await pool.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT role FROM tenant_memberships%'")).rows[0];if(row.n){blocked=true;break;}await new Promise(r=>setTimeout(r,10));}
    assert.ok(blocked,'export reaches fresh membership read behind an uncommitted role change');await changing.query('COMMIT');changed=true;
    assert.equal((await waiting).status,403,'middleware role is stale; fresh locked role read must win');
  }finally{if(!changed)await changing.query('ROLLBACK');changing.release();}
  await pool.query("UPDATE tenant_memberships SET role='admin' WHERE tenant_id=$1 AND user_id=$2",[tenant,actor]);
  const locked=await pool.connect();try{await locked.query('BEGIN');await locked.query('SELECT pg_advisory_xact_lock(169202634)');const busy=await req(actor);assert.equal(busy.status,429);await busy.text();}finally{await locked.query('ROLLBACK');locked.release();}
  app=makeApp();for(let i=0;i<10;i++){const response=await req(actor);assert.equal(response.status,200);await response.text();}assert.equal((await req(actor)).status,429);
  app=makeApp();await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[tenant,actor]);assert.equal((await req(actor)).status,403);await pool.query("UPDATE tenant_memberships SET role='admin' WHERE tenant_id=$1 AND user_id=$2",[tenant,actor]);
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[tenant,creator]);assert.equal((await req(actor)).status,404);assert.equal((await app.request(root+'/'+good,{headers:headers.get(actor)})).status,404);assert.equal((await(await app.request(root,{headers:headers.get(actor)})).json()).jobs.length,0);
  // A new active creator can make a new result; never lower the privacy epoch.
  const current=randomUUID();await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,result,privacy_epoch) SELECT $1,$2,$3,$4,$5,'succeeded',$6,privacy_epoch FROM projects WHERE id=$3",[current,tenant,project,actor,JSON.stringify(spec),JSON.stringify(result)]);
  await pool.query('UPDATE projects SET privacy_epoch=privacy_epoch+1 WHERE id=$1',[project]);assert.equal((await req(actor,current)).status,404);
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[tenant,actor]);assert.equal((await req(actor,current)).status,404);
  console.log(JSON.stringify({passed:true,json_raw_types:true,csv_text_prefix:true,scope_role_privacy_expiry_checks:true,pending_and_oversized_denied:true,headers_and_audit:true,one_stream_budget:true,cancel_releases_stream:true,global_advisory_budget:true,rate_limit:true,revoked_creator_hidden_on_all_reads:true,removed_reader_denied:true,node_adapter_finish_budget:true,membership_change_during_request_rechecked:true}));
}finally{await pool.end();}
