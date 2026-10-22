import type { Hono } from 'hono';
import { query } from './db';
import { parseTrend,InvalidQuery } from './query-spec';
import { trends } from './trends';
import type { Env } from '../../../apps/api/src/app';
import { projectQueryFlights as flights } from './query-budget';
import {saveReport,removeReport,savedReportRun,saveDashboard,removeDashboard} from './reports';
import {behaviorQuery,QueryBudgetExceeded} from './behavior-routes';
export function registerReportRoutes(app:Hono<Env>){
  const prefix='/api/tenants/:tenant/projects/:project';
  app.post(prefix+'/query',async c=>{
    const project=c.req.param('project')!.toLowerCase();
    if(flights.has(project))return c.json({error:'query_busy'},429);
    let spec;
    try{spec=parseTrend(await c.req.json());}catch(e){return c.json({error:e instanceof InvalidQuery?e.message:'invalid_query'},400);}
    if(flights.has(project))return c.json({error:'query_busy'},429);
    flights.add(project);
    try{return c.json(await trends(c.get('tenantId'),project,spec));}
    catch(e){if((e as {code?:string}).code==='57014')return c.json({error:'query_timeout',async_available:true},504);throw e;}
    finally{flights.delete(project);}
  });
  app.get(prefix+'/reports',async c=>c.json({reports:(await query('SELECT id,name,spec,timezone,version,updated_at FROM reports WHERE tenant_id=$1 AND project_id=$2 ORDER BY updated_at DESC',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.post(prefix+'/reports',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await saveReport(c.get('tenantId'),c.req.param('project')!,c.get('userId'),await c.req.json().catch(()=>null)),201);
  });
  app.patch(prefix+'/reports/:report',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await saveReport(c.get('tenantId'),c.req.param('project')!,c.get('userId'),await c.req.json().catch(()=>null),c.req.param('report')));
  });
  app.post(prefix+'/reports/:report/run',async c=>{
    const project=c.req.param('project')!.toLowerCase();if(flights.has(project))return c.json({error:'query_busy'},429);
    const saved=await savedReportRun(c.get('tenantId'),project,c.req.param('report')!,await c.req.json().catch(()=>null));
    if(flights.has(project))return c.json({error:'query_busy'},429);
    flights.add(project);
    try{const result=saved.spec.kind==='trend'?await trends(c.get('tenantId'),project,saved.spec,query,saved.timezone):await behaviorQuery(c.get('tenantId'),project,saved.spec,query,false,saved.timezone);return c.json({...result,saved_report:saved.report});}
    catch(error){if(error instanceof QueryBudgetExceeded)return c.json({error:error.message,async_available:true,report_id:saved.report.id,expected_version:saved.report.version},422);if((error as {code?:string}).code==='57014')return c.json({error:'query_timeout',async_available:true,report_id:saved.report.id,expected_version:saved.report.version},504);throw error;}
    finally{flights.delete(project);}
  });
  app.delete(prefix+'/reports/:report',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await removeReport(c.get('tenantId'),c.req.param('project')!,c.get('userId'),c.req.param('report')!));
  });
  app.get(prefix+'/dashboards',async c=>c.json({dashboards:(await query('SELECT id,name,report_ids,version,updated_at FROM dashboards WHERE tenant_id=$1 AND project_id=$2 ORDER BY updated_at DESC',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.post(prefix+'/dashboards',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await saveDashboard(c.get('tenantId'),c.req.param('project')!,c.get('userId'),await c.req.json().catch(()=>null)),201);
  });
  app.patch(prefix+'/dashboards/:dashboard',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await saveDashboard(c.get('tenantId'),c.req.param('project')!,c.get('userId'),await c.req.json().catch(()=>null),c.req.param('dashboard')));
  });
  app.delete(prefix+'/dashboards/:dashboard',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    return c.json(await removeDashboard(c.get('tenantId'),c.req.param('project')!,c.get('userId'),c.req.param('dashboard')!));
  });
}
