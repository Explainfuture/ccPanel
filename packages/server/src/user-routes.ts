import type{Hono}from'hono';
import type{Env}from'../../../apps/api/src/app';
import{query}from'./db';
export function registerUserRoutes(app:Hono<Env>){
  app.get('/api/tenants/:tenant/projects/:project/users/events',async c=>{
    const input=c.req.query('distinct_id'),page=Number(c.req.query('page')||0);
    if(!input||input.length>256||!Number.isInteger(page)||page<0||page>1000)return c.json({error:'invalid_user_query'},400);
    const tenant=c.get('tenantId'),project=c.req.param('project');
    const linked=await query('SELECT user_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=$3',[tenant,project,input]);
    const canonical=linked.rows[0]?.user_id||input;
    const rows=await query(`SELECT e.event_id,e.event_name,e.distinct_id,e.event_time,e.received_at,e.properties,e.context,e.registered,e.project_schema_version FROM visible_events e LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id WHERE e.tenant_id=$1 AND e.project_id=$2 AND COALESCE(l.user_id,e.distinct_id)=$3 AND e.event_time>=now()-interval '30 days' ORDER BY e.event_time DESC,e.event_id LIMIT 50 OFFSET $4`,[tenant,project,canonical,page*50]);
    return c.json({canonical_id:canonical,events:rows.rows,page,computed_at:new Date().toISOString()});
  });
}
