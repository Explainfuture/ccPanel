import type { Json } from '../../schema/src/index';
export type Filter={key:string;op:'eq'|'neq'|'in'|'exists'|'not_exists'|'gt'|'gte'|'lt'|'lte';value?:Json};
export interface TrendSpec{kind:'trend';from:string;to:string;event_name?:string;metric:'count'|'users'|'sum'|'avg';property?:string;filters:Filter[];group_by:string[]}
export class InvalidQuery extends Error{}
export function parseTrend(input:unknown,now=Date.now()):TrendSpec{
  if(!input||typeof input!=='object'||Array.isArray(input))throw new InvalidQuery('invalid_query');
  const b=input as Record<string,unknown>;
  if(Object.keys(b).some(k=>!['kind','from','to','event_name','metric','property','filters','group_by'].includes(k)))throw new InvalidQuery('unknown_query_field');
  const from=typeof b.from==='string'?Date.parse(b.from):NaN,to=typeof b.to==='string'?Date.parse(b.to):NaN;
  if(b.kind!=='trend'||!Number.isFinite(from)||!Number.isFinite(to)||from>=to||to-from>30*86400000||from<now-31*86400000||to>now+60000)throw new InvalidQuery('invalid_time_range');
  const metric=b.metric??'count';if(!['count','users','sum','avg'].includes(String(metric)))throw new InvalidQuery('invalid_metric');
  const key=(v:unknown)=>typeof v==='string'&&v.length>0&&v.length<=128;
  if(b.event_name!==undefined&&!key(b.event_name))throw new InvalidQuery('invalid_event_name');
  if(['sum','avg'].includes(String(metric))&&!key(b.property))throw new InvalidQuery('numeric_property_required');
  if(b.property!==undefined&&!key(b.property))throw new InvalidQuery('invalid_property');
  const filters=b.filters??[],groups=b.group_by??[];
  if(!Array.isArray(filters)||filters.length>20||!Array.isArray(groups)||groups.length>2||!groups.every(key))throw new InvalidQuery('query_limits_exceeded');
  const scalar=(v:unknown)=>v===null||typeof v==='string'||typeof v==='boolean'||(typeof v==='number'&&Number.isFinite(v));
  for(const f of filters){
    if(!f||typeof f!=='object'||Array.isArray(f)||Object.keys(f).some(k=>!['key','op','value'].includes(k))||!key(f.key)||!['eq','neq','in','exists','not_exists','gt','gte','lt','lte'].includes(f.op))throw new InvalidQuery('invalid_filter');
    if(['gt','gte','lt','lte'].includes(f.op)&&(typeof f.value!=='number'||!Number.isFinite(f.value)))throw new InvalidQuery('numeric_filter_required');
    if(['eq','neq'].includes(f.op)&&!scalar(f.value))throw new InvalidQuery('scalar_filter_required');
    if(f.op==='in'&&(!Array.isArray(f.value)||!f.value.length||f.value.length>100||!f.value.every(scalar)))throw new InvalidQuery('invalid_set_filter');
  }
  return{kind:'trend',from:new Date(from).toISOString(),to:new Date(to).toISOString(),metric:metric as TrendSpec['metric'],event_name:b.event_name as string|undefined,property:b.property as string|undefined,filters,group_by:[...new Set(groups)] as string[]};
}
export function compileFilters(filters:Filter[],values:unknown[]){
  const bind=(v:unknown)=>{values.push(v);return'$'+values.length;};
  return filters.map(f=>{
    const k=bind(f.key),p='e.properties';
    if(f.op==='exists')return`${p} ? ${k}`;
    if(f.op==='not_exists')return`NOT (${p} ? ${k})`;
    if(f.op==='eq'||f.op==='neq'){const val=bind(JSON.stringify(f.value));return`(${p} ? ${k} AND ${p}->${k} ${f.op==='eq'?'=':'<>'} ${val}::jsonb)`;}
    if(f.op==='in'){const parts=(f.value as Json[]).map(v=>`${p}->${k}=${bind(JSON.stringify(v))}::jsonb`);return`(${parts.join(' OR ')})`;}
    const op={gt:'>',gte:'>=',lt:'<',lte:'<='}[f.op];
    return`(CASE WHEN jsonb_typeof(${p}->${k})='number' THEN (${p}->>${k})::numeric END ${op} ${bind(f.value)}::numeric)`;
  }).map(s=>'('+s+')').join(' AND ');
}
