import { query } from './db';
import { compileFilters,type TrendSpec } from './query-spec';
import {dataCoverage} from './data-coverage';
export async function trends(tenantId:string,projectId:string,spec:TrendSpec,run:typeof query=query,timezone?:string){
  const values:unknown[]=[tenantId,projectId,spec.from,spec.to];
  const predicates=[`e.tenant_id=$1`,`e.project_id=$2`,`e.event_time >= $3`,`e.event_time < $4`,`e.event_name<>'$identify'`];
  if(spec.event_name){values.push(spec.event_name);predicates.push(`e.event_name=$${values.length}`);}
  const filters=compileFilters(spec.filters,values);if(filters)predicates.push(filters);
  let metric='count(*)::float8',excluded='0::int';
  if(spec.metric==='users')metric='count(DISTINCT COALESCE(l.user_id,e.distinct_id))::float8';
  if(spec.metric==='sum'||spec.metric==='avg'){
    values.push(spec.property);const param='$'+values.length;
    const numeric=`CASE WHEN jsonb_typeof(e.properties->${param})='number' THEN (e.properties->>${param})::numeric END`;
    metric=`${spec.metric}(${numeric})::float8`;excluded=`count(*) FILTER(WHERE jsonb_typeof(e.properties->${param}) IS DISTINCT FROM 'number')::int`;
  }
  const joins=`FROM visible_events e JOIN projects p ON p.id=e.project_id AND p.tenant_id=e.tenant_id LEFT JOIN identity_links l ON l.project_id=e.project_id AND l.tenant_id=e.tenant_id AND l.anonymous_id=e.distinct_id WHERE ${predicates.join(' AND ')}`;
  const grouping=spec.group_by.map(k=>{values.push(k);const param='$'+values.length;return`jsonb_build_object('present',e.properties ? ${param},'value',e.properties->${param})`;});
  const groups=grouping.length?`jsonb_build_array(${grouping.join(',')})`:`'[]'::jsonb`;
  const base=values.slice(0,values.length-grouping.length);
  const seriesValues=timezone?[...base,timezone]:base,zone=timezone?`$${base.length+1}::text`:'p.timezone';
  const [summary,series,byGroup]=await Promise.all([
    run(`SELECT ${metric} AS value,${excluded} AS excluded,count(*)::int AS events,count(DISTINCT COALESCE(l.user_id,e.distinct_id))::int AS users,max(e.received_at) AS latest_received_at ${joins}`,base),
    run(`SELECT to_char(date_trunc('day',e.event_time AT TIME ZONE ${zone}),'YYYY-MM-DD') AS day,${metric} AS value,${excluded} AS excluded ${joins} GROUP BY 1 ORDER BY 1`,seriesValues),
    run(`SELECT ${groups} AS dimensions,${metric} AS value,${excluded} AS excluded ${joins} GROUP BY 1 ORDER BY value DESC NULLS LAST LIMIT 21`,values),
  ]);
  let other:unknown=null;
  if(byGroup.rows.length>20){
    const excluding=values.length+1;
    const result=await run(`SELECT ${metric} AS value,${excluded} AS excluded ${joins} AND NOT (${groups}=ANY($${excluding}::jsonb[]))`,[...values,byGroup.rows.slice(0,20).map(r=>JSON.stringify(r.dimensions))]);
    other=result.rows[0];
  }
  return{spec,summary:summary.rows[0],series:series.rows,groups:byGroup.rows.slice(0,20),other,groups_truncated:byGroup.rows.length>20,computed_at:new Date().toISOString(),data_coverage:dataCoverage(spec)};
}
