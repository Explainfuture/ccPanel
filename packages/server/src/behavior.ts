import { parseTrend,InvalidQuery,type Filter } from './query-spec';
import type{Json}from'../../schema/src/index';
export type BehaviorSpec={kind:'funnel'|'retention'|'cohort';from:string;to:string;filters:Filter[];steps?:string[];window_hours?:number;group?:string;start_event?:string;return_event?:string;days?:number;event_name?:string;count_op?:'gte'|'eq'|'lte';count?:number};
export interface BehaviorEvent{id:string;user:string;name:string;time:number;properties:Record<string,Json>}
export function parseBehavior(input:unknown,now=Date.now()):BehaviorSpec{
  if(!input||typeof input!=='object'||Array.isArray(input))throw new InvalidQuery('invalid_query');
  const b=input as Record<string,unknown>,common=['kind','from','to','filters'];
  const fields=b.kind==='funnel'?['steps','window_hours','group']:b.kind==='retention'?['start_event','return_event','days']:b.kind==='cohort'?['event_name','count_op','count']:[];
  if(!fields.length||Object.keys(b).some(k=>![...common,...fields].includes(k)))throw new InvalidQuery('invalid_behavior_query');
  const normalized=parseTrend({kind:'trend',from:b.from,to:b.to,filters:b.filters},now);
  const name=(v:unknown)=>typeof v==='string'&&!!v.trim()&&v.length<=128;
  if(b.kind==='funnel'&&(!Array.isArray(b.steps)||b.steps.length<2||b.steps.length>5||!b.steps.every(name)||!Number.isFinite(b.window_hours)||Number(b.window_hours)<1||Number(b.window_hours)>168||(b.group!==undefined&&!name(b.group))))throw new InvalidQuery('invalid_funnel');
  if(b.kind==='retention'&&(!name(b.start_event)||!name(b.return_event)||!Number.isInteger(b.days)||Number(b.days)<1||Number(b.days)>30))throw new InvalidQuery('invalid_retention');
  if(b.kind==='cohort'&&(!name(b.event_name)||!['gte','eq','lte'].includes(String(b.count_op))||!Number.isInteger(b.count)||Number(b.count)<0||Number(b.count)>1000000))throw new InvalidQuery('invalid_cohort');
  return{...b,from:normalized.from,to:normalized.to,filters:normalized.filters} as BehaviorSpec;
}
export function matches(properties:Record<string,Json>,filters:Filter[]){
  return filters.every(f=>{
    const has=Object.hasOwn(properties,f.key),v=properties[f.key];
    switch(f.op){
      case'exists':return has;
      case'not_exists':return!has;
      case'eq':return has&&v===f.value;
      case'neq':return has&&v!==f.value;
      case'in':return has&&(f.value as Json[]).includes(v!);
      case'gt':return typeof v==='number'&&v>Number(f.value);
      case'gte':return typeof v==='number'&&v>=Number(f.value);
      case'lt':return typeof v==='number'&&v<Number(f.value);
      case'lte':return typeof v==='number'&&v<=Number(f.value);
    }
  });
}
const byUser=(events:BehaviorEvent[])=>{
  const map=new Map<string,BehaviorEvent[]>();for(const e of events){const rows=map.get(e.user)||[];rows.push(e);map.set(e.user,rows);}
  for(const rows of map.values())rows.sort((a,b)=>a.time-b.time||a.id.localeCompare(b.id));return map;
};
const dimension=(e:BehaviorEvent,key?:string)=>key?{present:Object.hasOwn(e.properties,key),value:e.properties[key]??null}:null;
export function funnel(events:BehaviorEvent[],spec:BehaviorSpec,now=Date.now()){
  const steps=spec.steps!,window=spec.window_hours!*3600000,from=Date.parse(spec.from),to=Date.parse(spec.to);
  const counts=steps.map(()=>0),grouped=new Map<string,{dimension:ReturnType<typeof dimension>;users:number[]}>();let incomplete=false;
  for(const rows of byUser(events.filter(e=>matches(e.properties,spec.filters))).values()){
    const names=new Map<string,BehaviorEvent[]>();for(const e of rows){const list=names.get(e.name)||[];list.push(e);names.set(e.name,list);}
    let best:BehaviorEvent[]=[];
    for(const first of names.get(steps[0]!)||[]){
      if(first.time<from||first.time>=to)continue;
      if(first.time+window>now)incomplete=true;
      const sequence=[first];let previous=first.time;
      for(let i=1;i<steps.length;i++){
        const candidates=names.get(steps[i]!)||[];let lo=0,hi=candidates.length;
        while(lo<hi){const mid=(lo+hi)>>>1;if(candidates[mid]!.time<=previous)lo=mid+1;else hi=mid;}
        const next=candidates[lo];if(!next||next.time>first.time+window||next.time>now)break;
        sequence.push(next);previous=next.time;
      }
      if(sequence.length>best.length||(sequence.length===best.length&&sequence.at(-1)!.time<best.at(-1)!.time))best=sequence;
    }
    if(!best.length)continue;
    for(let i=0;i<best.length;i++)counts[i]!++;
    if(spec.group){const d=dimension(best[0]!,spec.group),key=JSON.stringify(d);const g=grouped.get(key)||{dimension:d,users:steps.map(()=>0)};for(let i=0;i<best.length;i++)g.users[i]!++;grouped.set(key,g);}
  }
  const groups=[...grouped.values()].sort((a,b)=>b.users[0]!-a.users[0]!);
  const other=groups.length>20?{users:steps.map((_,i)=>groups.slice(20).reduce((n,g)=>n+g.users[i]!,0))}:null;
  return{kind:'funnel' as const,steps:steps.map((name,i)=>({name,users:counts[i]!,conversion:counts[0]?counts[i]!/counts[0]:null})),groups:groups.slice(0,20),other,incomplete_windows:incomplete};
}
const dayFormatters=new Map<string,Intl.DateTimeFormat>();
export function calendarDay(time:number,timezone:string){
  let formatter=dayFormatters.get(timezone);
  if(!formatter){if(dayFormatters.size>=16)dayFormatters.clear();formatter=new Intl.DateTimeFormat('en-US',{timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit'});dayFormatters.set(timezone,formatter);}
  const p=formatter.formatToParts(new Date(time));
  const part=(type:string)=>Number(p.find(x=>x.type===type)!.value);
  return Math.floor(Date.UTC(part('year'),part('month')-1,part('day'))/86400000);
}
export function retention(events:BehaviorEvent[],spec:BehaviorSpec,timezone:string,now=Date.now()){
  const from=Date.parse(spec.from),to=Date.parse(spec.to),today=calendarDay(now,timezone),cohorts=new Map<number,{size:number;returns:Set<string>[]} >();
  for(const[user,rows]of byUser(events.filter(e=>matches(e.properties,spec.filters)))){
    const first=rows.find(e=>e.name===spec.start_event&&e.time>=from&&e.time<to);if(!first)continue;
    const day=calendarDay(first.time,timezone),cohort=cohorts.get(day)||{size:0,returns:Array.from({length:spec.days!+1},()=>new Set<string>())};cohort.size++;
    for(const e of rows){if(e.name!==spec.return_event||e.time<first.time||e.time>now)continue;const n=calendarDay(e.time,timezone)-day;if(n>=0&&n<=spec.days!)cohort.returns[n]!.add(user);}
    cohorts.set(day,cohort);
  }
  return{kind:'retention' as const,timezone,cohorts:[...cohorts].sort(([a],[b])=>a-b).map(([day,c])=>({day:new Date(day*86400000).toISOString().slice(0,10),size:c.size,retention:c.returns.map((users,n)=>({day:n,mature:day+n<today,users:day+n<today?users.size:null,rate:day+n<today?users.size/c.size:null}))})),observed_at:new Date(now).toISOString()};
}
export function cohort(events:BehaviorEvent[],spec:BehaviorSpec){
  const counts=new Map<string,number>(),from=Date.parse(spec.from),to=Date.parse(spec.to);
  for(const e of events){if(!counts.has(e.user))counts.set(e.user,0);if(e.time>=from&&e.time<to&&e.name===spec.event_name&&matches(e.properties,spec.filters))counts.set(e.user,counts.get(e.user)!+1);}
  const users=[...counts].filter(([,count])=>spec.count_op==='gte'?count>=spec.count!:spec.count_op==='lte'?count<=spec.count!:count===spec.count!).map(([user,count])=>({user,count}));
  return{kind:'cohort' as const,total:users.length,base_users:counts.size,users:users.slice(0,100),users_truncated:users.length>100};
}
