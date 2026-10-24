import type{Hono}from'hono';
import type{Env}from'../../../apps/api/src/app';
import{pool,query}from'./db';
import{propertyRules,typesDefinition}from'./project-schema';
const prefix='/api/tenants/:tenant/projects/:project';
export function registerSchemaRoutes(app:Hono<Env>){
  app.get(prefix+'/schemas',async c=>c.json({schemas:(await query('SELECT event_name,required_properties,version FROM event_schemas WHERE tenant_id=$1 AND project_id=$2 ORDER BY event_name',[c.get('tenantId'),c.req.param('project')])).rows}));
  app.get(prefix+'/schemas/types',async c=>{
    const rows=await query('SELECT event_name,required_properties FROM event_schemas WHERE tenant_id=$1 AND project_id=$2 ORDER BY event_name',[c.get('tenantId'),c.req.param('project')]);
    c.header('Content-Type','text/plain; charset=utf-8');c.header('Content-Disposition','attachment; filename="project-events.d.ts"');
    return c.body(typesDefinition(rows.rows as Parameters<typeof typesDefinition>[0]));
  });
  app.put(prefix+'/schemas/:event',async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    const event=c.req.param('event')!;if(!event.trim()||event.length>128||event.startsWith('$'))return c.json({error:'invalid_event_name'},400);
    let rules;try{rules=propertyRules((await c.req.json()).required_properties);}catch{return c.json({error:'invalid_property_rules'},400);}
    const client=await pool.connect();
    try{
      await client.query('BEGIN');
      const result=await client.query('INSERT INTO event_schemas(tenant_id,project_id,event_name,required_properties) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,event_name) DO UPDATE SET required_properties=excluded.required_properties,version=event_schemas.version+1 RETURNING version',[c.get('tenantId'),c.req.param('project'),event,JSON.stringify(rules)]);
      await client.query('INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)',[c.get('tenantId'),c.get('userId'),'event_schema_updated',JSON.stringify({project_id:c.req.param('project'),event_name:event,version:result.rows[0].version})]);
      await client.query('COMMIT');return c.json({version:result.rows[0].version});
    }catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}
  });
  app.put(prefix+'/settings',async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    const b=await c.req.json();
    if(!['loose','strict'].includes(b.schema_mode)||typeof b.timezone!=='string'||b.timezone.length>100||!Array.isArray(b.origins)||b.origins.length>20||!b.origins.every((o:unknown)=>{try{return typeof o==='string'&&new URL(o).origin===o&&/^https?:/.test(o);}catch{return false;}}))return c.json({error:'invalid_settings'},400);
    try{new Intl.DateTimeFormat('en-US',{timeZone:b.timezone});}catch{return c.json({error:'invalid_timezone'},400);}
    await query('UPDATE projects SET timezone=$1,origins=$2,schema_mode=$3 WHERE tenant_id=$4 AND id=$5',[b.timezone,b.origins,b.schema_mode,c.get('tenantId'),c.req.param('project')]);return c.json({ok:true});
  });
}
