import {randomUUID} from 'node:crypto';
const operations=['persist','dlq_publish','dlq_index','kafka_checkpoint'] as const;
type Operation=typeof operations[number];
const outcomes=['inserted','duplicate','suppressed','conflict','failed','completed'] as const;
type Outcome=typeof outcomes[number];
const bounds=[1,5,10,25,50,100,250,500,1000,2500,5000,10000,30000];
export class WorkerMetrics{
  private readonly instance=randomUUID();private readonly started=new Date().toISOString();
  private readonly rows=operations.map(operation=>({operation,inFlight:0,counts:outcomes.map(()=>0n),latency:bounds.map(()=>0n).concat(0n),unknown:0n}));
  async measure<T>(operation:Operation,work:()=>Promise<T>,classify:(value:T)=>Outcome=()=> 'completed',failure:(error:unknown)=>Outcome=()=> 'failed'):Promise<T>{
    const row=this.rows.find(r=>r.operation===operation)!;const start=performance.now();row.inFlight++;
    const record=(outcome:Outcome)=>{try{row.counts[outcomes.indexOf(outcome)]++;}catch{/* Telemetry cannot change processing. */}};
    try{const value=await work();try{record(classify(value));}catch{record('completed');}return value;}
    catch(error){try{record(failure(error));}catch{record('failed');}throw error;}
    finally{row.inFlight--;const elapsed=performance.now()-start;if(Number.isFinite(elapsed)&&elapsed>=0){const at=bounds.findIndex(b=>elapsed<=b);row.latency[at<0?bounds.length:at]++;}else row.unknown++;}
  }
  snapshot(){return {scope:'worker_instance_aggregate',instance_id:this.instance,started_at:this.started,observed_at:new Date().toISOString(),counter_semantics:'cumulative_since_process_start',bucket_semantics:'non_cumulative',operations:this.rows.map(r=>({operation:r.operation,in_flight:r.inFlight,outcomes:outcomes.map((outcome,i)=>({outcome,count:r.counts[i].toString()})),duration_unknown:r.unknown.toString(),duration_ms:r.latency.map((count,i)=>({upper_bound_ms:bounds[i]??null,count:count.toString()}))}))};}
}
export function startWorkerMetricLog(metrics:WorkerMetrics){
  const emit=()=>{try{console.log(JSON.stringify({code:'worker_metrics',...metrics.snapshot()}));}catch{/* No acknowledgement depends on logging. */}};
  const timer=setInterval(emit,10000);timer.unref();return()=>{clearInterval(timer);emit();};
}
