import {canonical,type Json} from '../../schema/src/index';
import {digest} from './security';
import type {IntakeMetrics} from './intake-metrics';
import type {WorkerMetrics} from './worker-metrics';
export type OperationalSnapshot=ReturnType<IntakeMetrics['snapshot']>|ReturnType<WorkerMetrics['snapshot']>;
const bounds=[1,5,10,25,50,100,250,500,1000,2500,5000,10000,30000,null];
const statuses=['202','400','401','403','413','429','500','503','other'];
const operations=['persist','dlq_publish','dlq_index','kafka_checkpoint'];
const outcomes=['inserted','duplicate','suppressed','conflict','failed','completed'];
const invalid=():never=>{throw new Error('invalid_operational_snapshot');};
const record=(value:unknown):value is Record<string,unknown>=>!!value&&typeof value==='object'&&!Array.isArray(value);
function fields(value:unknown,names:string[]):asserts value is Record<string,unknown>{if(!record(value)||Object.keys(value).length!==names.length||!names.every(n=>Object.hasOwn(value,n)))invalid();}
function counter(value:unknown){if(typeof value!=='string'||!/^(0|[1-9]\d{0,39})$/.test(value))invalid();}
function rows(value:unknown,keys:string[],key:string):Record<string,unknown>[]{
  if(!Array.isArray(value)||value.length!==keys.length||value.some(v=>!record(v)||typeof v[key]!=='string'||!keys.includes(v[key] as string))||new Set(value.map(v=>v[key])).size!==keys.length)return invalid();return value;
}
function latency(value:unknown){
  if(!Array.isArray(value)||value.length!==bounds.length)return invalid();
  for(let i=0;i<bounds.length;i++){fields(value[i],['upper_bound_ms','count']);if(value[i].upper_bound_ms!==bounds[i])invalid();counter(value[i].count);}
}
function timestamp(value:unknown):number{
  if(typeof value!=='string')return invalid();const at=Date.parse(value);if(!Number.isFinite(at)||new Date(at).toISOString()!==value)return invalid();return at;
}
// Internal producers only. No HTTP route accepts snapshots from clients.
export function parseOperationalSnapshot(input:unknown,now=Date.now()){
  let text:string;
  try{text=JSON.stringify(input);}catch{return invalid();}
  if(typeof text!=='string'||Buffer.byteLength(text)>12288)return invalid();
  const body=JSON.parse(text) as Record<string,unknown>;
  if(!record(body))return invalid();
  const source=body.scope==='api_instance_aggregate'?'api':body.scope==='worker_instance_aggregate'?'worker':invalid();
  fields(body,['scope','instance_id','started_at','observed_at','counter_semantics','bucket_semantics',...(source==='api'?['responses','accepted_processing_unknown','accepted_processing_ms']:['operations'])]);
  if(typeof body.instance_id!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(body.instance_id)||body.counter_semantics!=='cumulative_since_process_start'||body.bucket_semantics!=='non_cumulative')return invalid();
  const started=timestamp(body.started_at),observed=timestamp(body.observed_at);
  if(!Number.isFinite(now)||started>observed||observed>now+5000||observed<now-7*86400000)return invalid();
  if(source==='api'){
    for(const row of rows(body.responses,statuses,'status')){fields(row,['status','batches','known_events','unknown_event_batches']);counter(row.batches);counter(row.known_events);counter(row.unknown_event_batches);}
    counter(body.accepted_processing_unknown);latency(body.accepted_processing_ms);
  }else{
    for(const row of rows(body.operations,operations,'operation')){
      fields(row,['operation','in_flight','outcomes','duration_unknown','duration_ms']);if(typeof row.in_flight!=='number'||!Number.isSafeInteger(row.in_flight)||row.in_flight<0)return invalid();
      for(const outcome of rows(row.outcomes,outcomes,'outcome')){fields(outcome,['outcome','count']);counter(outcome.count);}
      counter(row.duration_unknown);latency(row.duration_ms);
    }
  }
  const payload_hash=digest(canonical(body as Json));
  return {source,instance_id:body.instance_id,started_at:new Date(started),observed_at:new Date(observed),payload_hash,snapshot:body as unknown as OperationalSnapshot};
}
