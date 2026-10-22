import type { Hono } from 'hono';
import { randomUUID } from 'node:crypto';
import { query } from './db';
import { parseTrend,InvalidQuery } from './query-spec';
import { trends } from './trends';
import type { Env } from '../../../apps/api/src/app';
import { projectQueryFlights as flights } from './query-budget';
export function registerReportRoutes(app:Hono<Env>){
  const prefix='/api/tenants/:tenant/projects/:project';
  app.post(prefix+'/query',async c=>{
    const project=c.req.param('project')!.toLowerCase();
    if(flights.has(project))return c.json({error:'query_busy'},429);
    let spec;
    try{spec=parseTrend(await c.req.json());}catch(e){return c.json({error:e instanceof InvalidQuery?e.message:'invalid_query'},400);}
    flights.add(project);
    try{return c.json(await trends(c.get('tenantId'),project,spec));}
    catch(e){if((e as {code?:string}).code==='57014')return c.json({error:'query_timeout',async_available:true},504);throw e;}
    finally{flights.delete(project);}
  });
  app.get(prefix+'/reports',async c=>c.json({reports:(await query('SELECT id,name,spec,updated_at FROM reports WHERE tenant_id=$1 AND project_id=$2 ORDER BY updated_at DESC',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.post(prefix+'/reports',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    let spec,name:string;
    try{const b=await c.req.json();spec=parseTrend(b.spec);name=b.name;if(typeof name!=='string'||!name.trim()||name.length>128)throw new InvalidQuery('invalid_report_name');}catch(e){return c.json({error:e instanceof InvalidQuery?e.message:'invalid_report'},400);}
    const id=randomUUID();
    await query('INSERT INTO reports(id,tenant_id,project_id,name,spec) VALUES($1,$2,$3,$4,$5)',[id,c.get('tenantId'),c.req.param('project'),name,JSON.stringify(spec)]);
    return c.json({id},201);
  });
  app.delete(prefix+'/reports/:report',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    const id=c.req.param('report')!;if(!/^[0-9a-f-]{36}$/i.test(id))return c.json({error:'not_found'},404);
    const result=await query('DELETE FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=$3 RETURNING id',[c.get('tenantId'),c.req.param('project'),id]);
    return result.rowCount?c.json({ok:true}):c.json({error:'not_found'},404);
  });
  app.get(prefix+'/dashboards',async c=>c.json({dashboards:(await query('SELECT id,name,report_ids,updated_at FROM dashboards WHERE tenant_id=$1 AND project_id=$2 ORDER BY updated_at DESC',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.post(prefix+'/dashboards',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    const b=await c.req.json();
    if(typeof b.name!=='string'||!b.name.trim()||b.name.length>128||!Array.isArray(b.report_ids)||b.report_ids.length>50||new Set(b.report_ids).size!==b.report_ids.length||!b.report_ids.every((x:unknown)=>typeof x==='string'&&/^[0-9a-f-]{36}$/i.test(x)))return c.json({error:'invalid_dashboard'},400);
    const found=await query('SELECT id FROM reports WHERE tenant_id=$1 AND project_id=$2 AND id=ANY($3::uuid[])',[c.get('tenantId'),c.req.param('project'),b.report_ids]);
    if(found.rowCount!==b.report_ids.length)return c.json({error:'not_found'},404);
    const id=randomUUID();await query('INSERT INTO dashboards(id,tenant_id,project_id,name,report_ids) VALUES($1,$2,$3,$4,$5)',[id,c.get('tenantId'),c.req.param('project'),b.name,b.report_ids]);return c.json({id},201);
  });
}
