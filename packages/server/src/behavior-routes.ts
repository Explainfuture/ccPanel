import type{Hono}from'hono';
import type{Env}from'../../../apps/api/src/app';
import{parseBehavior,funnel,retention,cohort,type BehaviorEvent}from'./behavior';
import{query}from'./db';
import{InvalidQuery}from'./query-spec';
import{projectQueryFlights as flights}from'./query-budget';
export function registerBehaviorRoutes(app:Hono<Env>){
  app.post('/api/tenants/:tenant/projects/:project/behavior',async c=>{
    let spec;try{spec=parseBehavior(await c.req.json());}catch(e){return c.json({error:e instanceof InvalidQuery?e.message:'invalid_query'},400);}
    const project=c.req.param('project')!;
    if(flights.has(project))return c.json({error:'query_busy'},429);
    flights.add(project);
    try{
      const now=Date.now(),from=spec.kind==='cohort'?new Date(now-30*86400000).toISOString():spec.from;
      const to=new Date(Math.min(now,Date.parse(spec.to)+(spec.kind==='funnel'?spec.window_hours!*3600000:spec.kind==='retention'?(spec.days!+1)*86400000:0))).toISOString();
      const values:unknown[]=[c.get('tenantId'),project,from,to];
      let predicate='';if(spec.kind!=='cohort'){values.push(spec.kind==='funnel'?spec.steps:[spec.start_event,spec.return_event]);predicate=' AND e.event_name=ANY($5::text[])';}
      const where=`e.tenant_id=$1 AND e.project_id=$2 AND e.event_time >= $3 AND e.event_time < $4 AND e.event_name<>'$identify' ${predicate}`;
      const budget=await query(`SELECT count(*)::int AS count,COALESCE(sum(octet_length(e.properties::text)+1024),0)::float8 AS bytes FROM events e WHERE ${where}`,values);
      if(budget.rows[0].count>5000||budget.rows[0].bytes>4*1024*1024)return c.json({error:'query_requires_narrower_range',limit:5000},422);
      const rows=await query(`SELECT e.event_id AS id,COALESCE(l.user_id,e.distinct_id) AS "user",e.event_name AS name,e.event_time AS time,e.properties FROM events e LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id WHERE ${where} ORDER BY e.event_time,e.event_id LIMIT 5001`,values);
      if(rows.rows.length>5000||Buffer.byteLength(JSON.stringify(rows.rows))>8*1024*1024)return c.json({error:'query_requires_narrower_range',limit:5000},422);
      const events=rows.rows.map(r=>({...r,time:new Date(r.time).getTime()})) as BehaviorEvent[];
      const projectRow=await query('SELECT timezone FROM projects WHERE tenant_id=$1 AND id=$2',[c.get('tenantId'),project]);
      const result=spec.kind==='funnel'?funnel(events,spec,now):spec.kind==='retention'?retention(events,spec,projectRow.rows[0].timezone,now):cohort(events,spec);
      return c.json({...result,spec,computed_at:new Date().toISOString(),source_events:events.length,source_limit:5000});
    }finally{flights.delete(project);}
  });
}
