import {randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,query} from './db';
import {ReportError,reportId,reportDraft,reportRun,dashboardDraft,dashboardRunReference} from './report-spec';
export async function editReports<T>(tenant:string,project:string,actor:string,run:(db:PoolClient)=>Promise<T>){
  const db=await pool.connect();try{
    await db.query('BEGIN');
    const membership=(await db.query('SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 FOR SHARE',[tenant,actor])).rows[0];
    if(!membership)throw new ReportError(404,'not_found');if(membership.role==='viewer')throw new ReportError(403,'forbidden');
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',['ccpanel:reports:'+project.toLowerCase()]);
    if(!(await db.query('SELECT 1 FROM projects WHERE tenant_id=$1 AND id=$2',[tenant,project])).rowCount)throw new ReportError(404,'not_found');
    const result=await run(db);await db.query('COMMIT');return result;
  }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
export async function saveReport(tenant:string,project:string,actor:string,input:unknown,id?:string){
  if(id&&!reportId(id))throw new ReportError(404,'not_found');const draft=reportDraft(input,!!id);
  return editReports(tenant,project,actor,async db=>{
    const previous=id?(await db.query('SELECT version,timezone FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=$3 FOR UPDATE',[tenant,project,id])).rows[0]:null;
    if(id&&!previous)throw new ReportError(404,'not_found');if(previous&&previous.version!==draft.expected_version)throw new ReportError(409,'report_conflict');
    const projectRow=(await db.query('SELECT timezone FROM projects WHERE tenant_id=$1 AND id=$2 FOR SHARE',[tenant,project])).rows[0];
    const timezone=draft.timezone??previous?.timezone??projectRow.timezone;
    if(!(await db.query('SELECT 1 FROM pg_timezone_names WHERE name=$1',[timezone])).rowCount)throw new ReportError(400,'invalid_timezone');
    const key=id??randomUUID(),values=[tenant,project,key,draft.name,JSON.stringify(draft.spec),timezone];
    const saved=(await db.query(id?'UPDATE reports SET name=$4,spec=$5,timezone=$6,version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND id=$3 RETURNING id,name,spec,timezone,version,updated_at':'INSERT INTO reports(tenant_id,project_id,id,name,spec,timezone) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,name,spec,timezone,version,updated_at',values)).rows[0];
    await db.query('INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)',[tenant,actor,id?'report_updated':'report_created',JSON.stringify({project_id:project,report_id:key,version:saved.version})]);return saved;
  });
}
export async function removeReport(tenant:string,project:string,actor:string,id:string){
  if(!reportId(id))throw new ReportError(404,'not_found');
  return editReports(tenant,project,actor,async db=>{
    if(!(await db.query('DELETE FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=$3 RETURNING id',[tenant,project,id])).rowCount)throw new ReportError(404,'not_found');
    await db.query('UPDATE dashboards SET report_ids=array_remove(report_ids,$3::uuid),version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND $3::uuid=ANY(report_ids)',[tenant,project,id]);
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'report_deleted',$3)",[tenant,actor,JSON.stringify({project_id:project,report_id:id})]);return{ok:true};
  });
}
export async function savedReportRun(tenant:string,project:string,id:string,input:unknown,run:typeof query=query,lock=false){
  if(!reportId(id))throw new ReportError(404,'not_found');
  const row=(await run('SELECT id,name,spec,version,timezone FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=$3'+(lock?' FOR SHARE':''),[tenant,project,id])).rows[0];
  if(!row)throw new ReportError(404,'not_found');if(!row.timezone)throw new ReportError(409,'report_timezone_unrecorded');
  const parsed=reportRun(row.spec,input);if(parsed.expected_version!==undefined&&parsed.expected_version!==row.version)throw new ReportError(409,'report_conflict');
  return{spec:parsed.spec,timezone:row.timezone as string,report:{id:row.id,name:row.name,version:row.version,timezone:row.timezone}};
}
export async function saveDashboard(tenant:string,project:string,actor:string,input:unknown,id?:string){
  if(id&&!reportId(id))throw new ReportError(404,'not_found');const draft=dashboardDraft(input,!!id);
  return editReports(tenant,project,actor,async db=>{
    const old=id?(await db.query('SELECT version,columns,range_days,filters FROM dashboards WHERE tenant_id=$1 AND project_id=$2 AND id=$3 FOR UPDATE',[tenant,project,id])).rows[0]:null;
    if(id&&!old)throw new ReportError(404,'not_found');if(old&&old.version!==draft.expected_version)throw new ReportError(409,'dashboard_conflict');
    if((await db.query('SELECT id FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=ANY($3::uuid[])',[tenant,project,draft.report_ids])).rowCount!==draft.report_ids.length)throw new ReportError(404,'not_found');
    const key=id??randomUUID(),values=[tenant,project,key,draft.name,draft.report_ids,draft.columns??old?.columns??1,draft.range_days===undefined?(old?.range_days??null):draft.range_days,JSON.stringify(draft.filters??old?.filters??[])];
    const saved=(await db.query(id?'UPDATE dashboards SET name=$4,report_ids=$5,columns=$6,range_days=$7,filters=$8,version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND project_id=$2 AND id=$3 RETURNING id,name,report_ids,columns,range_days,filters,version,updated_at':'INSERT INTO dashboards(tenant_id,project_id,id,name,report_ids,columns,range_days,filters) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id,name,report_ids,columns,range_days,filters,version,updated_at',values)).rows[0];
    await db.query('INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)',[tenant,actor,id?'dashboard_updated':'dashboard_created',JSON.stringify({project_id:project,dashboard_id:key,version:saved.version})]);return saved;
  });
}
export async function removeDashboard(tenant:string,project:string,actor:string,id:string){
  if(!reportId(id))throw new ReportError(404,'not_found');return editReports(tenant,project,actor,async db=>{
    if(!(await db.query('DELETE FROM dashboards WHERE tenant_id=$1 AND project_id=$2 AND id=$3 RETURNING id',[tenant,project,id])).rowCount)throw new ReportError(404,'not_found');
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'dashboard_deleted',$3)",[tenant,actor,JSON.stringify({project_id:project,dashboard_id:id})]);return{ok:true};
  });
}

// One MVCC metadata snapshot fixes card order and revisions; data queries still
// run individually through the existing scope, timezone and budget guards.
export async function savedDashboardRun(tenant:string,project:string,id:string,input:unknown){
  if(!reportId(id))throw new ReportError(404,'not_found');const requested=dashboardRunReference(input);
  const board=(await query(`SELECT b.id,b.name,b.version,b.columns,b.range_days,b.filters,
    coalesce(jsonb_agg(jsonb_build_object('report_id',picked.id,'name',r.name,'expected_version',r.version) ORDER BY picked.position) FILTER(WHERE picked.id IS NOT NULL),'[]') AS cards
    FROM dashboards b LEFT JOIN LATERAL unnest(b.report_ids) WITH ORDINALITY picked(id,position) ON true
    LEFT JOIN reports r ON r.tenant_id=b.tenant_id AND r.project_id=b.project_id AND r.id=picked.id
    WHERE b.tenant_id=$1 AND b.project_id=$2 AND b.id=$3 GROUP BY b.id`,[tenant,project,id])).rows[0];
  if(!board)throw new ReportError(404,'not_found');if(board.version!==requested.expected_version)throw new ReportError(409,'dashboard_conflict');
  const days=requested.range_days===undefined?board.range_days:requested.range_days,now=new Date();
  const filters=[...board.filters,...(requested.filters??[])];if(filters.length>20)throw new ReportError(400,'too_many_filters');
  const overrides={filters,...(days?{from:new Date(now.getTime()-days*86400000).toISOString(),to:now.toISOString()}:{})};
  return{dashboard:{id:board.id,name:board.name,version:board.version,columns:board.columns},planned_at:now.toISOString(),cards:board.cards.map((card:{report_id:string;name:string|null;expected_version:number|null})=>card.expected_version===null?{report_id:card.report_id,error:'not_found'}:{...card,overrides})};
}
