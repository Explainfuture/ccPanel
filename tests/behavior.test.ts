import{expect,it}from'vitest';
import{calendarDay,funnel,retention,cohort,matches,parseBehavior,type BehaviorEvent,type BehaviorSpec}from'../packages/server/src/behavior';
const t=Date.parse('2026-10-20T00:00:00Z');
const e=(user:string,name:string,hours:number,properties:BehaviorEvent['properties']={}):BehaviorEvent=>({id:crypto.randomUUID(),user,name,time:t+hours*3600000,properties});
const spec=(extra:Partial<BehaviorSpec>):BehaviorSpec=>({kind:'funnel',from:new Date(t).toISOString(),to:new Date(t+48*3600000).toISOString(),steps:['view','buy'],window_hours:24,filters:[],...extra});
it('counts users once with strict ordering and retries later starting events inside a valid window',()=>{
  const rows=[e('A','view',0),e('A','view',30),e('A','buy',31),e('B','view',2),e('B','buy',2),e('C','buy',1),e('D','view',3),e('D','unrelated',4),e('D','buy',5)];
  const r=funnel(rows,spec({}),t+100*3600000);expect(r.steps.map(s=>s.users)).toEqual([3,2]);expect(r.steps[1]!.conversion).toBe(2/3);
});
it('uses the successful sequence first-step property and marks unfinished observation windows',()=>{
  const rows=[e('A','view',0,{channel:'old'}),e('A','view',30,{channel:'new'}),e('A','buy',31),e('B','view',47,{channel:null})];
  const r=funnel(rows,spec({group:'channel'}),t+48*3600000);expect(r.groups.some(g=>g.dimension?.value==='new')).toBe(true);expect(r.groups.some(g=>g.dimension?.value==='old')).toBe(false);expect(r.incomplete_windows).toBe(true);
});
it('retention uses natural calendar days and leaves immature cells null',()=>{
  const rows=[e('A','start',15.5),e('A','return',16.5),e('A','return',17),e('B','start',15.8),e('B','return',40)];
  const r=retention(rows,spec({kind:'retention',start_event:'start',return_event:'return',days:3}),'Asia/Shanghai',t+45*3600000);
  expect(r.cohorts[0]!.day).toBe('2026-10-20');expect(r.cohorts[0]!.size).toBe(2);expect(r.cohorts[0]!.retention[1]!.users).toBe(1);expect(r.cohorts[0]!.retention[2]!.users).toBeNull();
});
it('calendar day arithmetic is correct across a daylight-saving change',()=>{
  const a=Date.parse('2026-03-08T05:30:00Z'),b=Date.parse('2026-03-09T04:30:00Z');expect(b-a).toBe(23*3600000);expect(calendarDay(b,'America/New_York')-calendarDay(a,'America/New_York')).toBe(1);
});
it('never-did cohorts retain users whose other events exist outside the rule interval',()=>{
  const rows=[e('A','view',-10),e('B','view',1),e('B','buy',2),e('C','buy',3,{amount:'10'})];
  const r=cohort(rows,spec({kind:'cohort',event_name:'buy',count_op:'eq',count:0,filters:[{key:'amount',op:'gt',value:5}]}));expect(r.base_users).toBe(3);expect(r.total).toBe(3);
  expect(matches({},[{key:'x',op:'neq',value:null}])).toBe(false);expect(matches({x:null},[{key:'x',op:'exists'}])).toBe(true);
});
it('rejects unbounded or malformed behavior requests',()=>{
  const now=Date.now(),from=new Date(now-86400000).toISOString(),to=new Date(now).toISOString();expect(()=>parseBehavior({kind:'funnel',from,to,steps:['a'],window_hours:24})).toThrow('invalid_funnel');expect(()=>parseBehavior({kind:'cohort',from,to,event_name:'a',count_op:'eq',count:-1})).toThrow('invalid_cohort');
});
