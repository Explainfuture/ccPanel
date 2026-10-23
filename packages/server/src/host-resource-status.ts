import {parseHostTelemetry,type HostTelemetry} from './host-telemetry';
import {isDeepStrictEqual} from 'node:util';
type RateState='baseline_required'|'ready'|'sample_gap'|'counter_reset';
export class HostResourceStatus{
  private current:HostTelemetry|null=null;
  private reason:'unavailable'|'invalid'|'stale'='unavailable';
  private rates:{interval_ms:number;started_at:number;finished_at:number;cpu_idle_percent:number;cpu_user_percent:number;cpu_system_percent:number;cpu_iowait_percent:number;cpu_steal_percent:number;swap_in_bytes_per_second:string;swap_out_bytes_per_second:string}|null=null;
  private rateState:RateState='baseline_required';
  constructor(private clock:()=>number=Date.now){}
  failed(){this.current=null;this.rates=null;this.reason='unavailable';this.rateState='baseline_required';}
  observe(input:unknown){
    let value:HostTelemetry;
    try{value=parseHostTelemetry(input);}catch{this.current=null;this.rates=null;this.reason='invalid';this.rateState='baseline_required';return;}
    const age=this.clock()-value.sampledAt;
    if(age<0||age>30000){this.current=null;this.rates=null;this.reason='stale';this.rateState='baseline_required';return;}
    const old=this.current;
    if(old&&value.sampledAt<old.sampledAt){this.current=null;this.rates=null;this.reason='invalid';this.rateState='baseline_required';return;}
    if(old&&value.sampledAt===old.sampledAt){
      if(!isDeepStrictEqual(value,old)){this.current=null;this.rates=null;this.reason='invalid';this.rateState='baseline_required';}
      return;
    }
    this.current=structuredClone(value);this.rates=null;this.rateState='baseline_required';
    if(!old)return;
    const interval=value.sampledAt-old.sampledAt;
    if(interval>30000){this.rateState='sample_gap';return;}
    const a=old.host,b=value.host,keys=['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks','totalTicks'] as const;
    if(a.pageBytes!==b.pageBytes||a.cpu.ticksPerSecond!==b.cpu.ticksPerSecond||keys.some(k=>BigInt(b.cpu[k])<BigInt(a.cpu[k]))||BigInt(b.swapInPages)<BigInt(a.swapInPages)||BigInt(b.swapOutPages)<BigInt(a.swapOutPages)){this.rateState='counter_reset';return;}
    const total=BigInt(b.cpu.totalTicks)-BigInt(a.cpu.totalTicks);
    if(total===0n){this.rateState='counter_reset';return;}
    const percent=(key:typeof keys[number])=>Number((BigInt(b.cpu[key])-BigInt(a.cpu[key]))*10000n/total)/100;
    const rate=(key:'swapInPages'|'swapOutPages')=>((BigInt(b[key])-BigInt(a[key]))*BigInt(b.pageBytes)*1000n/BigInt(interval)).toString();
    this.rateState='ready';this.rates={interval_ms:interval,started_at:old.sampledAt,finished_at:value.sampledAt,cpu_idle_percent:percent('idleTicks'),cpu_user_percent:percent('userTicks'),cpu_system_percent:percent('systemTicks'),cpu_iowait_percent:percent('ioWaitTicks'),cpu_steal_percent:percent('stealTicks'),swap_in_bytes_per_second:rate('swapInPages'),swap_out_bytes_per_second:rate('swapOutPages')};
  }
  snapshot(){
    const now=this.clock(),current=this.current,age=current?now-current.sampledAt:null;
    const state=current&&age!==null&&age>=0&&age<=30000?'ready':current?'stale':this.reason;
    const value=state==='ready'?current:null;
    return {scope:'host_aggregate' as const,state,observed_at:now,sampled_at:value?.sampledAt??null,age_ms:value?age:null,
      gauges:value?{memory_bytes:{...value.host.memoryBytes},swap_used_bytes:(BigInt(value.host.memoryBytes.swapTotal)-BigInt(value.host.memoryBytes.swapFree)).toString(),load_average:[...value.host.loadAverage],disks:value.disks.map(d=>({kind:d.kind,total_bytes:d.totalBytes,available_bytes:d.availableBytes,available_percent:Number(BigInt(d.availableBytes)*10000n/BigInt(d.totalBytes))/100,total_inodes:d.totalInodes,available_inodes:d.availableInodes,available_inode_percent:BigInt(d.totalInodes)>0n?Number(BigInt(d.availableInodes)*10000n/BigInt(d.totalInodes))/100:null}))}:null,
      rate_state:value?this.rateState:null,rates:value&&this.rates?{...this.rates}:null};
  }
}
