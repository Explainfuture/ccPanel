// Isolated PG and authenticated Hono HTTP; no Kafka or external notification.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {createApp} from '../apps/api/src/app';
import {digest,token} from '../packages/server/src/security';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const a=randomUUID(),b=randomUUID(),pa=randomUUID(),pb=randomUUID(),operator=randomUUID(),ordinary=randomUUID(),viewer=randomUUID(),foreign=randomUUID();
  for(const id of[a,b])await pool.query("INSERT INTO tenants VALUES($1,'DLQ HEALTH TEMP')",[id]);
  for(const[id,t]of[[pa,a],[pb,b]])await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'DLQ HEALTH TEMP')",[id,t]);
  const headers=new Map<string,Record<string,string>>();
  for(const[id,t,role,op]of[[operator,a,'admin',true],[ordinary,a,'admin',false],[viewer,a,'viewer',true],[foreign,b,'admin',true]] as const){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`dlq-health-${id}@integration.invalid`,'unused']);await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[t,id,role]);
    if(op)await pool.query('INSERT INTO platform_operators(user_id) VALUES($1)',[id]);
    const session=token();await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(session),id,digest(token())]);headers.set(id,{cookie:'ccpanel_session='+session});
  }
  const deps={ready:()=>true,publish:async()=>{throw Error('unexpected_publish');}};
  let app=createApp(deps);const root=`/api/tenants/${a}/projects/${pa}`,other=`/api/tenants/${b}/projects/${pb}`,path=root+'/dlq-health';
  const get=(id=operator,target=path)=>app.request(target,{headers:headers.get(id)});
  // Prove denied readers never reach the cross-company index query.
  await pool.query('ALTER TABLE dlq_records RENAME TO dlq_records_unavailable');
  try{
    assert.equal((await app.request(path)).status,401);
    for(const id of[ordinary,viewer,foreign])assert.equal((await get(id)).status,id===foreign?404:403);
    assert.equal((await get(operator,other+'/dlq-health')).status,404);
    const failed=await get();assert.equal(failed.status,503);assert.deepEqual(await failed.json(),{error:'temporarily_unavailable'});
  }finally{await pool.query('ALTER TABLE dlq_records_unavailable RENAME TO dlq_records');}
  assert.equal((await get(operator,path+'?tenant_id='+b)).status,400);
  let response=await get();assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');let value=await response.json();assert.deepEqual(value.signals.map((s:{state:string})=>s.state),['clear','clear']);
  const source=async(id:string,t:string|null,p:string|null,age:number,redacted=false)=>pool.query("INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload,created_at) VALUES($1,$2,$3,'private-reason',$4,clock_timestamp()-$5::int*interval '1 day')",[id,t,p,JSON.stringify({secret:'private-dlq-payload',redacted}),age]);
  const receipt=async(id:string,t:string,p:string,status:string)=>{
    const job=randomUUID(),user=t===a?operator:foreign;
    await pool.query("INSERT INTO replay_jobs(id,tenant_id,project_id,user_id,request_hash,reason) VALUES($1,$2,$3,$4,'fixture','private-replay-reason')",[job,t,p,user]);
    await pool.query("INSERT INTO replay_items(tenant_id,project_id,job_id,record_id,event_id,payload_hash,status) VALUES($1,$2,$3,$4,$5,'private-payload-hash',$6)",[t,p,job,id,randomUUID(),status]);
  };
  await source('pending',a,pa,1);
  for(const status of['rejected','failed']){await source(status,a,pa,1);await receipt(status,a,pa,status);}
  await source('unscoped-expired',null,null,15);await receipt('unscoped-expired',b,pb,'stored');
  await source('queued-old',a,pa,13);await receipt('queued-old',a,pa,'queued');
  await source('published',b,pb,2);await receipt('published',b,pb,'published');
  await source('stored',a,pa,3);await receipt('stored',a,pa,'stored');
  await source('duplicate',b,pb,4);await receipt('duplicate',b,pb,'duplicate');
  await source('suppressed',a,pa,5);await receipt('suppressed',a,pa,'suppressed');
  await source('redacted',b,pb,6,true);
  await source('wrong-scope',a,pa,1);await receipt('wrong-scope',b,pb,'stored');
  await source('suppression-priority',a,pa,1);for(const s of['queued','stored','suppressed'])await receipt('suppression-priority',a,pa,s);
  await source('success-priority',b,pb,2);for(const s of['queued','duplicate'])await receipt('success-priority',b,pb,s);
  await source('replaying-after-failure',a,pa,1);for(const s of['failed','queued'])await receipt('replaying-after-failure',a,pa,s);
  await source('old-resolved',b,pb,15);await receipt('old-resolved',b,pb,'stored');
  const fingerprint=async()=>(await pool.query("SELECT (SELECT bit_xor(hashtextextended(row_to_json(d)::text,0))::text FROM dlq_records d) AS sources,(SELECT bit_xor(hashtextextended(row_to_json(i)::text,0))::text FROM replay_items i) AS receipts")).rows[0];
  const before=await fingerprint();app=createApp(deps);response=await get();assert.equal(response.status,200);const text=await response.text();value=JSON.parse(text);
  assert.deepEqual(value.categories,{unresolved:'5',replaying:'3',resolved:'4',suppressed:'3'});
  assert.equal(value.signals[0].pending_records,'8');assert.equal(value.signals[0].state,'firing');assert.equal(value.signals[1].reason,'retention_window_exceeded');assert.ok(BigInt(value.signals[1].oldest_pending_age_seconds)>=15n*86400n);
  for(const privateValue of[a,b,pa,pb,'private-dlq-payload','private-reason','private-payload-hash','wrong-scope'])assert.ok(!text.includes(privateValue),'aggregate response must not expose company/record/payload fields');
  assert.deepEqual(await fingerprint(),before,'health reads must not change DLQ or replay receipts');
  await source('future',b,pb,-1);value=await (await get()).json();assert.equal(value.categories,null);assert.ok(value.signals.every((s:{state:string;reason:string})=>s.state==='unknown'&&s.reason==='future_record'));await pool.query("DELETE FROM dlq_records WHERE id='future'");
  await pool.query("UPDATE dlq_records SET payload=jsonb_set(payload,'{redacted}','true'::jsonb)");value=await (await get()).json();assert.equal(value.categories.suppressed,'15');assert.ok(value.signals.every((s:{state:string;reason:string})=>s.state==='clear'&&s.reason==='no_retained_pending'));
  await pool.query('UPDATE platform_operators SET revoked_at=clock_timestamp() WHERE user_id=$1',[operator]);assert.equal((await get()).status,403);
  await pool.query('UPDATE platform_operators SET revoked_at=NULL WHERE user_id=$1',[operator]);await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE user_id=$1",[operator]);assert.equal((await get()).status,403);
  await pool.query('DELETE FROM tenant_memberships WHERE user_id=$1',[operator]);assert.equal((await get()).status,404);
  for(const t of[a,b])await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[t,operator]);app=createApp(deps);
  for(let i=0;i<6;i++){assert.equal((await get()).status,200);assert.equal((await get(operator,other+'/operational-alerts')).status,200);}
  response=await get();assert.equal(response.status,429);assert.equal(response.headers.get('retry-after'),'5');assert.equal((await get(operator,root+'/metrics')).status,200);
  console.log(JSON.stringify({passed:true,dlq_health_http:true,dual_role_and_latest_grants:true,denied_before_index_query:true,foreign_scope_hidden:true,all_receipt_states_classified:true,receipt_precedence:true,wrong_scope_receipt_ignored:true,unscoped_and_expired_retained_pending:true,no_payload_or_company_identifiers:true,read_only_fingerprints:true,future_record_unknown:true,shared_history_budget:true,business_read_unaffected:true}));
}finally{await pool.end();}
