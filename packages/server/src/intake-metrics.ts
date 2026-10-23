import {randomUUID} from 'node:crypto';
import {limits} from '../../schema/src/index';
export interface IntakeObservation{status:number;events:number|null;durationMs:number}
const bounds=[1,5,10,25,50,100,250,500,1000,2500,5000,10000,30000] as const;
const statuses=[202,400,401,403,413,429,500,503] as const;
export class IntakeMetrics{
  private readonly instance=randomUUID();
  private readonly started=new Date().toISOString();
  private readonly rows=new Map<string,{batches:bigint;knownEvents:bigint;unknownEventBatches:bigint}>([...statuses.map(s=>String(s)),'other'].map(s=>[s,{batches:0n,knownEvents:0n,unknownEventBatches:0n}]));
  private readonly latency=Array.from({length:bounds.length+1},()=>0n);
  private unknownLatency=0n;
  observe(row:IntakeObservation){
    const value=this.rows.get(statuses.includes(row.status as typeof statuses[number])?String(row.status):'other')!;
    value.batches++;
    if(row.events===null)value.unknownEventBatches++;else if(Number.isSafeInteger(row.events)&&row.events>=0&&row.events<=limits.serverEvents)value.knownEvents+=BigInt(row.events);else value.unknownEventBatches++;
    if(row.status===202){if(Number.isFinite(row.durationMs)&&row.durationMs>=0){const at=bounds.findIndex(b=>row.durationMs<=b);this.latency[at<0?bounds.length:at]++;}else this.unknownLatency++;}
  }
  snapshot(){return {scope:'api_instance_aggregate' as const,instance_id:this.instance,started_at:this.started,observed_at:new Date().toISOString(),counter_semantics:'cumulative_since_process_start' as const,bucket_semantics:'non_cumulative' as const,
    responses:[...this.rows].map(([status,v])=>({status,batches:v.batches.toString(),known_events:v.knownEvents.toString(),unknown_event_batches:v.unknownEventBatches.toString()})),
    accepted_processing_unknown:this.unknownLatency.toString(),accepted_processing_ms:this.latency.map((count,i)=>({upper_bound_ms:i<bounds.length?bounds[i]:null,count:count.toString()}))};}
}
export function startIntakeMetricLog(metrics:IntakeMetrics){
  const emit=()=>{try{console.log(JSON.stringify({code:'intake_metrics',...metrics.snapshot()}));}catch{/* Telemetry never changes ingestion acknowledgement. */}};
  const timer=setInterval(emit,10000);timer.unref();
  return()=>{clearInterval(timer);emit();};
}
