import {parseOperationalSnapshot} from './operational-snapshot';
type State='clear'|'firing'|'unknown';
export type OperationalAlert={rule:'api_sampling'|'worker_sampling'|'api_server_errors'|'worker_failures';state:State;reason:string;observed_at:string|null;interval_ms:number|null;increase:string|null};
const freshnessMs=90000;
// Expected topology: at least one API and one Worker reporting every 30s.
// Evaluates the newest instance per source; not a replica inventory or SLO.
export function evaluateOperationalAlerts(input:{api:unknown[];worker:unknown[]},now=Date.now()):OperationalAlert[]{
  if(!Number.isFinite(now))throw new Error('invalid_alert_time');
  const result:OperationalAlert[]=[];
  for(const source of ['api','worker'] as const){
    let samples:ReturnType<typeof parseOperationalSnapshot>[];
    const row=(kind:'sampling'|'failures',state:State,reason:string,at:string|null=null,interval:number|null=null,increase:string|null=null)=>result.push({rule:kind==='sampling'?`${source}_sampling`:source==='api'?'api_server_errors':'worker_failures',state,reason,observed_at:at,interval_ms:interval,increase});
    try{
      if(!Array.isArray(input[source])||input[source].length>2)throw 0;
      samples=input[source].map(value=>parseOperationalSnapshot(value,now)).sort((a,b)=>b.observed_at.getTime()-a.observed_at.getTime());
      if(samples.some(value=>value.source!==source))throw 0;
    }catch{row('sampling','unknown','invalid_snapshot');row('failures','unknown','invalid_snapshot');continue;}
    const latest=samples[0],prior=samples[1];
    if(!latest){row('sampling','firing','missing_sample');row('failures','unknown','missing_sample');continue;}
    const at=latest.observed_at.toISOString(),age=now-latest.observed_at.getTime();
    if(age<0){row('sampling','unknown','future_sample',at);row('failures','unknown','future_sample',at);continue;}
    if(age>freshnessMs){row('sampling','firing','stale_sample',at);row('failures','unknown','stale_sample',at);continue;}
    row('sampling','clear','fresh_sample',at);
    if(!prior||prior.instance_id!==latest.instance_id||prior.started_at.getTime()!==latest.started_at.getTime()){row('failures','unknown','baseline_required',at);continue;}
    const interval=latest.observed_at.getTime()-prior.observed_at.getTime();
    if(interval<=0||interval>freshnessMs){row('failures','unknown',interval<=0?'duplicate_time':'sample_gap',at);continue;}
    const counters=(value:typeof latest)=>{
      const snapshot=value.snapshot;
      // Compare each fixed label separately: a reset in one must not be hidden
      // by growth in another. BigInt preserves counters above 2^53.
      return 'responses' in snapshot?[500,503].map(status=>BigInt(snapshot.responses.find(r=>r.status===String(status))!.batches)):
        ['persist','dlq_publish','dlq_index','kafka_checkpoint'].map(name=>BigInt(snapshot.operations.find(operation=>operation.operation===name)!.outcomes.find(outcome=>outcome.outcome==='failed')!.count));
    };
    const before=counters(prior),after=counters(latest);
    if(after.some((value,index)=>value<before[index]!)){row('failures','unknown','counter_reset',at,interval);continue;}
    const increase=after.reduce((sum,value,index)=>sum+value-before[index]!,0n);
    row('failures',increase>0n?'firing':'clear',increase>0n?'failure_increase':'no_failure_increase',at,interval,increase.toString());
  }
  return result;
}
