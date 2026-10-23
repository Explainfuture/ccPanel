// Real PG/Hono metadata and query checks in a disposable database only.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {token,digest} from '../packages/server/src/security';
import {createApp} from '../apps/api/src/app';
import {startQueryJobs} from '../packages/server/src/query-jobs';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {HostResourceStatus} from '../packages/server/src/host-resource-status';
let stopJobs:(()=>void)|undefined;
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const a=randomUUID(),b=randomUUID(),project=randomUUID(),otherProject=randomUUID(),admin=randomUUID(),analyst=randomUUID(),viewer=randomUUID(),foreign=randomUUID();
  for(const tenant of [a,b])await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'REPORT EDIT TEMP']);
  await pool.query("INSERT INTO projects(id,tenant_id,name,timezone) VALUES($1,$2,'REPORT EDIT TEMP','UTC'),($3,$4,'REPORT EDIT TEMP','UTC')",[project,a,otherProject,b]);
  const headers=new Map<string,Record<string,string>>(),origin=process.env.PUBLIC_ORIGIN!;
  for(const [id,tenant,role]of[[admin,a,'admin'],[analyst,a,'analyst'],[viewer,a,'viewer'],[foreign,b,'admin']]){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`report-${id}@integration.invalid`,'fixture-only']);await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[tenant,id,role]);
    const secret=token(),csrf=digest(secret+':csrf');await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(secret),id,digest(csrf)]);headers.set(id,{cookie:'ccpanel_session='+secret,origin,'x-csrf-token':csrf,'content-type':'application/json'});
  }
  const intake=new IntakeMetrics();intake.observe({status:202,events:2,durationMs:9});let intakeReads=0,resourceReads=0;const resourceStatus=new HostResourceStatus();
  const app=createApp({maxIntakeInFlight:2,ready:()=>true,intakeStatus:()=>{intakeReads++;return intake.snapshot();},hostResourceStatus:()=>{resourceReads++;return resourceStatus.snapshot();},publish:async()=>{throw new Error('unexpected_publish');}}),root=`/api/tenants/${a}/projects/${project}`,other=`/api/tenants/${b}/projects/${otherProject}`;
  const req=(user:string,path:string,body?:unknown,method=body?'POST':'GET')=>app.request(path,{method,headers:headers.get(user),...(body?{body:JSON.stringify(body)}:{})});
  assert.equal((await req(admin,root+'/ingest-status')).status,403);assert.equal(intakeReads,0);assert.equal(resourceReads,0);assert.equal((await (await req(admin,'/api/me')).json()).platform_operator,false);
  assert.equal((await req(admin,root+'/operational-history')).status,403);
  await pool.query('INSERT INTO platform_operators(user_id) VALUES($1)',[admin]);
  const historySnapshot=intake.snapshot();
  // Synthetic SQL timestamps intentionally differ at microsecond precision to
  // exercise lossless DB keyset boundaries, not producer timestamp fidelity.
  await pool.query("INSERT INTO operational_samples(source,instance_id,started_at,observed_at,payload_hash,snapshot) SELECT 'api',$1,clock_timestamp()-interval '1 hour',date_trunc('second',CURRENT_TIMESTAMP)-interval '1 second'+n*interval '1 microsecond',$2,$3 FROM generate_series(1,53) n",[historySnapshot.instance_id,'0'.repeat(64),JSON.stringify(historySnapshot)]);
  const history=await req(admin,root+'/operational-history?source=api');assert.equal(history.status,200);assert.equal(history.headers.get('cache-control'),'no-store');const historyBody=await history.json();assert.equal(historyBody.samples.length,50);assert.equal(historyBody.scope,'platform_aggregate');assert.ok(historyBody.next_cursor);
  const nextHistory=await(await req(admin,root+'/operational-history?source=api&cursor='+historyBody.next_cursor)).json();assert.equal(nextHistory.samples.length,3);assert.equal(nextHistory.next_cursor,null);
  assert.equal((await(await req(admin,root+'/operational-history?source=worker')).json()).samples.length,0);
  for(const suffix of ['?limit=100000','?source=api&source=worker','?cursor=broken'])assert.equal((await req(admin,root+'/operational-history'+suffix)).status,400);
  for(const user of [viewer,analyst])assert.equal((await req(user,root+'/operational-history')).status,403);
  assert.equal((await req(foreign,root+'/operational-history')).status,404);assert.equal((await app.request(root+'/operational-history')).status,401);
  assert.equal((await (await req(admin,'/api/me')).json()).platform_operator,true);
  const telemetry=await req(admin,root+'/ingest-status');assert.equal(telemetry.status,200);const telemetryBody=await telemetry.json();assert.equal(telemetryBody.intake.scope,'api_instance_aggregate');assert.equal(telemetryBody.host_resources.scope,'host_aggregate');assert.equal(telemetryBody.host_resources.state,'unavailable');assert.equal(resourceReads,1);assert.equal(telemetryBody.intake.responses[0].known_events,'2');
  assert.deepEqual(telemetryBody.processing,{in_flight:0,max_in_flight:2,at_capacity:false});
  for(const actor of [viewer,analyst])assert.equal((await req(actor,root+'/ingest-status')).status,403);
  assert.equal((await req(foreign,root+'/ingest-status')).status,404);assert.equal((await app.request(root+'/ingest-status')).status,401);assert.equal(intakeReads,1);
  for(const actor of [viewer,analyst])assert.equal((await req(actor,root+'/dlq-summary')).status,403);
  assert.equal((await req(foreign,root+'/dlq-summary')).status,404);assert.equal((await app.request(root+'/dlq-summary')).status,401);
  const sources=['unresolved','replaying','resolved','suppressed','expired'];
  for(const name of sources)await pool.query(`INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload,created_at) VALUES($1,$2,$3,'fixture',$4,CURRENT_TIMESTAMP-($5::int*interval '1 day'))`,[name,a,project,JSON.stringify(name==='suppressed'?{redacted:true}:{}),name==='expired'?15:2]);
  await pool.query("INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES('foreign',$1,$2,'fixture','{}')",[b,otherProject]);
  for(const [name,status]of [['replaying','published'],['resolved','stored'],['resolved','failed'],['resolved','published'],['suppressed','stored'],['unresolved','failed']]){
    const job=randomUUID();await pool.query("INSERT INTO replay_jobs(id,tenant_id,project_id,user_id,request_hash,reason) VALUES($1,$2,$3,$4,'fixture','fixture')",[job,a,project,admin]);
    await pool.query("INSERT INTO replay_items(tenant_id,project_id,job_id,record_id,event_id,payload_hash,status) VALUES($1,$2,$3,$4,$5,'fixture',$6)",[a,project,job,name,randomUUID(),status]);
  }
  const dlqResponse=await req(admin,root+'/dlq-summary');assert.equal(dlqResponse.status,200);const dlq=await dlqResponse.json();assert.equal(dlq.scope,'project');assert.equal(dlq.categories.length,4);
  for(const category of dlq.categories){assert.equal(category.count,'1');assert.ok(Number(category.oldest_age_seconds)>=172800);assert.ok(Number(category.oldest_age_seconds)<172900);}
  assert.ok(!JSON.stringify(dlq).includes('fixture'));assert.ok(!JSON.stringify(dlq).includes(otherProject));
  await pool.query('DELETE FROM replay_items WHERE project_id=$1',[project]);await pool.query('DELETE FROM replay_jobs WHERE project_id=$1',[project]);await pool.query('DELETE FROM dlq_records WHERE project_id=ANY($1::uuid[])',[[project,otherProject]]);
  const emptyDlq=await (await req(admin,root+'/dlq-summary')).json();for(const category of emptyDlq.categories){assert.equal(category.count,'0');assert.equal(category.oldest_age_seconds,null);assert.equal(category.oldest_created_at,null);}
  const statusIds:string[]=[];
  for(const status of ['queued','running','succeeded','failed','cancelled']){
    const id=randomUUID();statusIds.push(id);
    await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,privacy_epoch,created_at,started_at,finished_at) SELECT $1,$2,$3,$4,'{}',$5,privacy_epoch,CURRENT_TIMESTAMP-interval '10 seconds',CASE WHEN $5='queued' THEN NULL ELSE CURRENT_TIMESTAMP-interval '5 seconds' END,CASE WHEN $5 IN ('succeeded','failed','cancelled') THEN CURRENT_TIMESTAMP-interval '2 seconds' ELSE NULL END FROM projects WHERE id=$3",[id,a,project,admin,status]);
  }
  for(const [owner,expired,epoch]of [[foreign,false,0],[admin,true,0],[admin,false,99]] as const){
    const id=randomUUID();statusIds.push(id);await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,privacy_epoch,expires_at) VALUES($1,$2,$3,$4,'{}','queued',$5,CURRENT_TIMESTAMP+($6::int*interval '1 hour'))",[id,a,project,owner,epoch,expired?-1:1]);
  }
  const foreignStatus=randomUUID();statusIds.push(foreignStatus);await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status) VALUES($1,$2,$3,$4,'{}','queued')",[foreignStatus,b,otherProject,foreign]);
  for(const actor of [admin,analyst,viewer]){
    const response=await req(actor,root+'/query-status');assert.equal(response.status,200);const summary=await response.json();assert.equal(summary.states.length,5);
    for(const row of summary.states){assert.equal(row.count,'1');assert.ok(Number(row.oldest_age_seconds)>=10);if(row.status==='running'){assert.ok(Number(row.oldest_running_seconds)>=5);}else assert.equal(row.oldest_running_seconds,null);if(['succeeded','failed','cancelled'].includes(row.status)){assert.equal(row.duration_samples,'1');assert.equal(Number(row.max_completed_duration_ms),3000);}else{assert.equal(row.duration_samples,'0');assert.equal(row.max_completed_duration_ms,null);}}
  }
  assert.equal((await req(foreign,root+'/query-status')).status,404);assert.equal((await app.request(root+'/query-status')).status,401);
  await pool.query('DELETE FROM query_jobs WHERE id=ANY($1::uuid[])',[statusIds]);
  const emptyStatus=await (await req(admin,root+'/query-status')).json();for(const row of emptyStatus.states){assert.equal(row.count,'0');assert.equal(row.oldest_age_seconds,null);}
  for(const id of [a,b,project,otherProject])assert.ok(!JSON.stringify(telemetryBody).includes(id));
  await pool.query('UPDATE platform_operators SET revoked_at=CURRENT_TIMESTAMP WHERE user_id=$1',[admin]);assert.equal((await req(admin,root+'/ingest-status')).status,403);assert.equal(intakeReads,1);assert.equal(resourceReads,1);assert.equal((await (await req(admin,'/api/me')).json()).platform_operator,false);
  assert.equal((await req(admin,root+'/operational-history')).status,403);
  await pool.query('DELETE FROM platform_operators WHERE user_id=$1',[admin]);
  const day=Math.floor(Date.now()/86400000)*86400000,first=day-86400000-1800000;
  const range={from:new Date(day-3*86400000).toISOString(),to:new Date(day).toISOString()};
  const trend={kind:'trend',...range,event_name:'view',metric:'count',filters:[],group_by:[]};
  for(const [name,time]of[['view',first],['return',first+3600000]] as const)await persist({tenant_id:a,project_id:project,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:name,distinct_id:'same-user',event_time:new Date(time).toISOString(),properties:{plan:'paid'},context:{},schema_version:1}});
  const created=await req(analyst,root+'/reports',{name:'原报表',spec:trend});assert.equal(created.status,201);const report=await created.json();assert.equal(report.timezone,'UTC');assert.equal(report.version,1);
  assert.equal((await req(viewer,root+'/reports',{name:'forbidden',spec:trend})).status,403);
  const updates=await Promise.all(['编辑A','编辑B'].map(name=>req(analyst,root+'/reports/'+report.id,{name,spec:trend,expected_version:1},'PATCH')));assert.deepEqual(updates.map(r=>r.status).sort(),[200,409]);
  await pool.query("UPDATE projects SET timezone='Asia/Shanghai' WHERE id=$1",[project]);
  const saved=await req(viewer,root+'/reports/'+report.id+'/run',{});assert.equal(saved.status,200);const snapshot=await saved.json();assert.equal(snapshot.saved_report.timezone,'UTC');assert.equal(snapshot.series[0].day,new Date(first).toISOString().slice(0,10));
  const current=await req(viewer,root+'/query',trend);assert.equal(current.status,200);assert.notEqual((await current.json()).series[0].day,snapshot.series[0].day);
  const expiredRange={from:new Date(Date.now()-30.5*86400000).toISOString(),to:new Date(Date.now()-86400000).toISOString()};
  for(const [path,spec]of [['/query',{...trend,...expiredRange}],['/behavior',{kind:'funnel',...expiredRange,steps:['view','return'],window_hours:24,filters:[]}] ] as const){
    const response=await req(viewer,root+path,spec);assert.equal(response.status,200);const value=await response.json();assert.equal(value.data_coverage.expired_overlap,true);assert.equal(value.data_coverage.incomplete,true);
  }
  const behaviorIds:string[]=[];
  for(const spec of [{kind:'funnel',...range,steps:['view','return'],window_hours:24,filters:[]},{kind:'retention',...range,start_event:'view',return_event:'return',days:1,filters:[]},{kind:'cohort',...range,event_name:'view',count_op:'gte',count:1,filters:[]}]){
    const result=await req(analyst,root+'/reports',{name:spec.kind,spec,timezone:'UTC'});assert.equal(result.status,201);const row=await result.json();behaviorIds.push(row.id);
    const run=await req(viewer,root+'/reports/'+row.id+'/run',{});assert.equal(run.status,200);const value=await run.json();assert.equal(value.saved_report.timezone,'UTC');
    if(spec.kind==='retention'){assert.equal(value.timezone,'UTC');assert.equal(value.cohorts[0].retention[1].users,1);}
    if(spec.kind==='funnel')assert.equal(value.steps[1].users,1);
  }
  const legacy=randomUUID();await pool.query('INSERT INTO reports(id,tenant_id,project_id,name,spec) VALUES($1,$2,$3,$4,$5)',[legacy,a,project,'legacy',JSON.stringify(trend)]);
  assert.equal((await req(viewer,root+'/reports/'+legacy+'/run',{})).status,409);
  assert.equal((await req(analyst,root+'/reports/'+legacy,{name:'recorded',spec:trend,timezone:'UTC',expected_version:1},'PATCH')).status,200);
  const boardResponse=await req(analyst,root+'/dashboards',{name:'board',report_ids:[report.id,behaviorIds[0]],columns:2,range_days:7,filters:[{key:'plan',op:'eq',value:'paid'}]});assert.equal(boardResponse.status,201);const board=await boardResponse.json();
  assert.equal(board.columns,2);assert.equal(board.range_days,7);assert.equal(board.filters[0].value,'paid');
  const planResponse=await req(viewer,root+'/dashboards/'+board.id+'/run',{expected_version:1,filters:[{key:'extra',op:'not_exists'}]});assert.equal(planResponse.status,200);const plan=await planResponse.json();
  assert.equal(plan.dashboard.version,1);assert.equal(plan.dashboard.columns,2);assert.deepEqual(plan.cards.map((r:{report_id:string})=>r.report_id),[report.id,behaviorIds[0]]);
  assert.ok(plan.cards.every((r:{overrides:{from:string;to:string;filters:unknown[]}})=>r.overrides.to===plan.planned_at&&Date.parse(r.overrides.to)-Date.parse(r.overrides.from)===7*86400000&&r.overrides.filters.length===2));
  assert.equal((await req(viewer,root+'/reports/'+plan.cards[0].report_id+'/run',{...plan.cards[0].overrides,expected_version:plan.cards[0].expected_version})).status,200);
  assert.ok(plan.cards.every((card:{name:unknown})=>typeof card.name==='string'));
  const filteredPlan=await(await req(viewer,root+'/dashboards/'+board.id+'/run',{expected_version:1,filters:[{key:'extra',op:'exists'}]})).json();
  const filteredResult=await req(viewer,root+'/reports/'+filteredPlan.cards[0].report_id+'/run',{...filteredPlan.cards[0].overrides,expected_version:filteredPlan.cards[0].expected_version});assert.equal(filteredResult.status,200);assert.equal((await filteredResult.json()).summary.value,0);
  const ownRange=await req(viewer,root+'/dashboards/'+board.id+'/run',{expected_version:1,range_days:null});assert.equal(ownRange.status,200);assert.equal((await ownRange.json()).cards[0].overrides.from,undefined);
  assert.equal((await req(viewer,root+'/dashboards/'+board.id+'/run',{expected_version:99})).status,409);
  assert.equal((await req(foreign,other+'/dashboards/'+board.id+'/run',{expected_version:1})).status,404);
  assert.equal((await req(viewer,root+'/dashboards/'+board.id+'/run',{expected_version:1,filters:Array.from({length:20},()=>({key:'x',op:'exists'}))})).status,400);
  assert.equal((await req(viewer,root+'/dashboards/'+board.id,{name:'no',report_ids:[],expected_version:1},'PATCH')).status,403);
  const otherReport=await req(foreign,other+'/reports',{name:'foreign',spec:trend});const otherId=(await otherReport.json()).id;
  assert.equal((await req(analyst,root+'/dashboards',{name:'bad-scope',report_ids:[otherId]})).status,404);
  assert.equal((await req(analyst,root+'/reports/'+otherId,{name:'bad',spec:trend,expected_version:1},'PATCH')).status,404);
  assert.equal((await req(admin,root+'/reports/'+report.id,undefined,'DELETE')).status,200);
  const boards=await req(viewer,root+'/dashboards');const updated=(await boards.json()).dashboards.find((d:{id:string})=>d.id===board.id);assert.deepEqual(updated.report_ids,[behaviorIds[0]]);assert.equal(updated.version,2);assert.equal(updated.columns,2);assert.equal(updated.range_days,7);assert.equal(updated.filters[0].value,'paid');
  assert.equal((await req(analyst,root+'/dashboards/'+board.id,{name:'stale',report_ids:[],expected_version:1},'PATCH')).status,409);
  assert.equal((await req(analyst,root+'/dashboards/'+board.id,{name:'reordered',report_ids:[behaviorIds[2],behaviorIds[0]],expected_version:2},'PATCH')).status,200);
  const preserved=(await pool.query('SELECT columns,range_days,filters FROM dashboards WHERE id=$1',[board.id])).rows[0];assert.equal(preserved.columns,2);assert.equal(preserved.range_days,7);assert.equal(preserved.filters[0].value,'paid');
  const cleared=await req(analyst,root+'/dashboards/'+board.id,{name:'clear view',report_ids:[],expected_version:3,columns:1,range_days:null,filters:[]},'PATCH');assert.equal(cleared.status,200);const clear=await cleared.json();assert.equal(clear.columns,1);assert.equal(clear.range_days,null);assert.deepEqual(clear.filters,[]);
  const raced=await req(analyst,root+'/reports',{name:'race',spec:trend});const racedId=(await raced.json()).id;
  const race=await Promise.all([req(analyst,root+'/dashboards',{name:'race',report_ids:[racedId]}),req(admin,root+'/reports/'+racedId,undefined,'DELETE')]);
  if(race[0]!.status===201){const id=(await race[0]!.json()).id;assert.deepEqual((await pool.query('SELECT report_ids FROM dashboards WHERE id=$1',[id])).rows[0].report_ids,[]);}else assert.equal(race[0]!.status,404);
  const enqueue=await req(viewer,root+'/query-jobs',{report_id:behaviorIds[1],expected_version:1});assert.equal(enqueue.status,202);const queued=await enqueue.json();
  const stored=(await pool.query('SELECT timezone,report_version FROM query_jobs WHERE id=$1',[queued.id])).rows[0];assert.equal(stored.timezone,'UTC');assert.equal(stored.report_version,1);
  await pool.query("UPDATE projects SET timezone='Pacific/Auckland' WHERE id=$1",[project]);
  const retention={kind:'retention',...range,start_event:'view',return_event:'return',days:1,filters:[]};
  assert.equal((await req(analyst,root+'/reports/'+behaviorIds[1],{name:'edited after queue',spec:retention,timezone:'Asia/Shanghai',expected_version:1},'PATCH')).status,200);
  assert.equal((await req(viewer,root+'/query-jobs',{report_id:behaviorIds[1],expected_version:1})).status,409);
  assert.equal((await req(viewer,root+'/query-jobs',{report_id:otherId,expected_version:1})).status,404);
  const raw=await req(viewer,root+'/query-jobs',trend);assert.equal(raw.status,202);const rawJob=await raw.json();
  const revoked=await req(analyst,root+'/query-jobs',{report_id:behaviorIds[1],expected_version:2});assert.equal(revoked.status,202);const revokedJob=await revoked.json();
  const cancelled=await req(viewer,root+'/query-jobs',trend);assert.equal(cancelled.status,202);const cancelledJob=await cancelled.json();assert.equal((await req(viewer,root+'/query-jobs/'+cancelledJob.id,undefined,'DELETE')).status,200);
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[a,analyst]);assert.equal((await req(analyst,root+'/reports')).status,404);
  stopJobs=startQueryJobs();
  const deadline=Date.now()+30000;let done=false;
  while(Date.now()<deadline){const rows=(await pool.query('SELECT id,status FROM query_jobs')).rows;if(rows.every(r=>!['queued','running'].includes(r.status))){done=true;break;}await new Promise(r=>setTimeout(r,100));}assert.ok(done,'query jobs completed');
  const output=await req(viewer,root+'/query-jobs/'+queued.id);assert.equal(output.status,200);const finished=await output.json();assert.equal(finished.status,'succeeded');assert.equal(finished.result.saved_report.version,1);assert.equal(finished.result.saved_report.timezone,'UTC');assert.equal(finished.result.timezone,'UTC');assert.equal(finished.result.cohorts[0].retention[1].users,1);
  const anonymous=(await pool.query('SELECT status,result,timezone,report_id FROM query_jobs WHERE id=$1',[rawJob.id])).rows[0];assert.equal(anonymous.status,'succeeded');assert.equal(anonymous.report_id,null);assert.equal(anonymous.timezone,null);assert.equal(anonymous.result.saved_report,undefined);
  const rejected=(await pool.query('SELECT status,error,result FROM query_jobs WHERE id=$1',[revokedJob.id])).rows[0];assert.equal(rejected.status,'failed');assert.equal(rejected.error,'membership_revoked');assert.equal(rejected.result,null);
  assert.equal((await req(analyst,root+'/query-jobs/'+revokedJob.id)).status,404);
  assert.equal((await pool.query('SELECT status,result FROM query_jobs WHERE id=$1',[cancelledJob.id])).rows[0].status,'cancelled');
  console.log(JSON.stringify({passed:true,concurrent_edit_conflict:true,saved_timezone_pinned:true,retention_timezone_pinned:true,all_analysis_kinds_saved:true,legacy_timezone_not_guessed:true,viewer_read_only:true,cross_company_not_found:true,delete_repairs_dashboard:true,dashboard_revision_conflict:true,create_delete_reference_race_safe:true,removed_member_denied:true,queued_report_snapshot_pinned:true,queued_revision_conflict:true,anonymous_query_compatible:true,revoked_job_failed:true,cancelled_job_not_executed:true,dashboard_layout_and_filters:true,dashboard_snapshot_one_range:true,dashboard_stale_or_foreign_denied:true,legacy_update_preserves_settings:true,explicit_view_clear:true}));
}finally{stopJobs?.();await pool.end();}
