// Real PG/Hono metadata and query checks in a disposable database only.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {token,digest} from '../packages/server/src/security';
import {createApp} from '../apps/api/src/app';
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
  const app=createApp({ready:()=>true,publish:async()=>{throw new Error('unexpected_publish');}}),root=`/api/tenants/${a}/projects/${project}`,other=`/api/tenants/${b}/projects/${otherProject}`;
  const req=(user:string,path:string,body?:unknown,method=body?'POST':'GET')=>app.request(path,{method,headers:headers.get(user),...(body?{body:JSON.stringify(body)}:{})});
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
  const boardResponse=await req(analyst,root+'/dashboards',{name:'board',report_ids:[report.id,behaviorIds[0]]});assert.equal(boardResponse.status,201);const board=await boardResponse.json();
  assert.equal((await req(viewer,root+'/dashboards/'+board.id,{name:'no',report_ids:[],expected_version:1},'PATCH')).status,403);
  const otherReport=await req(foreign,other+'/reports',{name:'foreign',spec:trend});const otherId=(await otherReport.json()).id;
  assert.equal((await req(analyst,root+'/dashboards',{name:'bad-scope',report_ids:[otherId]})).status,404);
  assert.equal((await req(analyst,root+'/reports/'+otherId,{name:'bad',spec:trend,expected_version:1},'PATCH')).status,404);
  assert.equal((await req(admin,root+'/reports/'+report.id,undefined,'DELETE')).status,200);
  const boards=await req(viewer,root+'/dashboards');const updated=(await boards.json()).dashboards.find((d:{id:string})=>d.id===board.id);assert.deepEqual(updated.report_ids,[behaviorIds[0]]);assert.equal(updated.version,2);
  assert.equal((await req(analyst,root+'/dashboards/'+board.id,{name:'stale',report_ids:[],expected_version:1},'PATCH')).status,409);
  assert.equal((await req(analyst,root+'/dashboards/'+board.id,{name:'reordered',report_ids:[behaviorIds[2],behaviorIds[0]],expected_version:2},'PATCH')).status,200);
  const raced=await req(analyst,root+'/reports',{name:'race',spec:trend});const racedId=(await raced.json()).id;
  const race=await Promise.all([req(analyst,root+'/dashboards',{name:'race',report_ids:[racedId]}),req(admin,root+'/reports/'+racedId,undefined,'DELETE')]);
  if(race[0]!.status===201){const id=(await race[0]!.json()).id;assert.deepEqual((await pool.query('SELECT report_ids FROM dashboards WHERE id=$1',[id])).rows[0].report_ids,[]);}else assert.equal(race[0]!.status,404);
  await pool.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[a,analyst]);assert.equal((await req(analyst,root+'/reports')).status,404);
  console.log(JSON.stringify({passed:true,concurrent_edit_conflict:true,saved_timezone_pinned:true,retention_timezone_pinned:true,all_analysis_kinds_saved:true,legacy_timezone_not_guessed:true,viewer_read_only:true,cross_company_not_found:true,delete_repairs_dashboard:true,dashboard_revision_conflict:true,create_delete_reference_race_safe:true,removed_member_denied:true}));
}finally{await pool.end();}
