import {expect,it} from 'vitest';
import {reportDraft,reportRun,dashboardDraft,reportReference,dashboardRunReference} from '../packages/server/src/report-spec';
import {trends} from '../packages/server/src/trends';
import {query} from '../packages/server/src/db';
const range=()=>({from:new Date(Date.now()-86400000).toISOString(),to:new Date().toISOString()});
const trend=()=>({kind:'trend',...range(),metric:'count',filters:[],group_by:[]});
it('saves shared analysis kinds with an explicit timezone and requires edit revision',()=>{
  const funnel={kind:'funnel',...range(),steps:['view','pay'],window_hours:24,filters:[]};
  expect(reportDraft({name:'漏斗',spec:funnel,timezone:'UTC'}).spec.kind).toBe('funnel');
  expect(reportDraft({name:'趋势',spec:trend(),timezone:'Asia/Shanghai',expected_version:2},true).expected_version).toBe(2);
  expect(()=>reportDraft({name:'趋势',spec:trend()},true)).toThrow('expected_version_required');
});
it('rejects forged scope, invalid zones, storage-unsafe names and malformed report runs',()=>{
  for(const value of [{name:'A',spec:trend(),tenant_id:'other'},{name:'a\0b',spec:trend()},{name:'A',spec:trend(),timezone:'not/a/timezone'}])expect(()=>reportDraft(value)).toThrow();
  expect(()=>reportRun(trend(),{tenant_id:'other'})).toThrow('invalid_report_run');
  expect(()=>reportRun(trend(),{from:new Date().toISOString()})).toThrow('invalid_time_range');
  expect(()=>reportRun(null,{})).toThrow('invalid_report');
});
it('keeps saved filters when applying dashboard filters and validates their combined limit',()=>{
  const spec={...trend(),filters:[{key:'plan',op:'eq',value:'paid'}]};
  const applied=reportRun(spec,{filters:[{key:'country',op:'eq',value:'CN'}],expected_version:1});
  expect(applied.spec.filters.map(f=>f.key)).toEqual(['plan','country']);expect(applied.expected_version).toBe(1);
  expect(()=>reportRun(spec,{filters:Array.from({length:20},()=>({key:'extra',op:'exists'}))})).toThrow('query_limits_exceeded');
});
it('normalizes dashboard references and rejects duplicate UUID casing or foreign fields',()=>{
  const id='aaaaaaaa-1111-4111-8111-111111111111';expect(dashboardDraft({name:'board',report_ids:[id.toUpperCase()]}).report_ids).toEqual([id]);
  expect(()=>dashboardDraft({name:'board',report_ids:[id,id.toUpperCase()]})).toThrow('invalid_dashboard');
  expect(()=>dashboardDraft({name:'board',report_ids:[id],project_id:'other'})).toThrow('invalid_dashboard');
});
it('binds a saved timezone only to the daily-series query without adding unused parameters',async()=>{
  const calls:{sql:string;values:unknown[]}[]=[];
  const run=(async(sql:string,values:unknown[])=>{calls.push({sql,values});return{rows:[]};}) as unknown as typeof query;
  await trends('company','project',{kind:'trend',...range(),metric:'sum',property:'price',filters:[{key:'plan',op:'eq',value:'paid'}],group_by:['country']},run,'UTC');
  expect(calls).toHaveLength(3);
  for(const call of calls){const parameters=[...call.sql.matchAll(/\$(\d+)/g)].map(m=>Number(m[1]));expect(Math.max(...parameters)).toBe(call.values.length);}
  const series=calls.find(c=>c.sql.includes('date_trunc'))!;expect(series.values.at(-1)).toBe('UTC');expect(series.sql).not.toContain("TIME ZONE 'UTC'");expect(calls.filter(c=>c.values.includes('UTC'))).toHaveLength(1);
});
it('pins an async saved-report reference without accepting caller timezone, scope or nested revision',()=>{
  const id='aaaaaaaa-1111-4111-8111-111111111111';expect(reportReference({report_id:id.toUpperCase(),expected_version:3,overrides:{filters:[]}})).toEqual({report_id:id,expected_version:3,overrides:{filters:[]}});
  for(const value of [{report_id:id},{report_id:id,expected_version:3,timezone:'UTC'},{report_id:id,expected_version:3,tenant_id:'other'},{report_id:id,expected_version:3,overrides:{expected_version:1}},{report_id:id,expected_version:3,overrides:[]}])expect(()=>reportReference(value)).toThrow();
});

it('bounds dashboard layouts, ranges and typed filters without guessing missing updates',()=>{
  const draft=dashboardDraft({name:'board',report_ids:[],columns:2,range_days:7,filters:[{key:'code',op:'eq',value:'20'}]});
  expect(draft.columns).toBe(2);expect(draft.range_days).toBe(7);expect(draft.filters?.[0]?.value).toBe('20');
  expect(dashboardDraft({name:'old client',report_ids:[],expected_version:2},true).columns).toBeUndefined();
  expect(dashboardDraft({name:'clear',report_ids:[],range_days:null,filters:[]}).range_days).toBeNull();
  for(const field of [{columns:0},{columns:3},{columns:'2'},{range_days:'7'},{range_days:2},{filters:null},{filters:{}},{filters:Array.from({length:21},()=>({key:'plan',op:'exists'}))}])expect(()=>dashboardDraft({name:'bad',report_ids:[],...field})).toThrow();
});
it('accepts viewer dashboard run overrides only with a revision and bounded fields',()=>{
  expect(dashboardRunReference({expected_version:4,range_days:null,filters:[]})).toMatchObject({expected_version:4,range_days:null,filters:[]});
  for(const body of [{},{expected_version:0},{expected_version:4,tenant_id:'other'},{expected_version:4,columns:2},{expected_version:4,report_ids:[]},{expected_version:4,filters:[{key:'price',op:'gt',value:'20'}]}])expect(()=>dashboardRunReference(body)).toThrow();
});
