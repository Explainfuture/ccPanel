import {defaultPressurePolicy,type PressurePolicy} from './ingest-pressure';
export type PressureSignal={rule:'kafka_backlog'|'ingest_integrity'|'storage_headroom';state:'clear'|'firing'|'unknown';reason:string;observed_at:string|null;expires_at:string|null;max_lag_messages:string|null;total_lag_messages:string|null};
export type PressureAlerts=ReturnType<typeof evaluatePressureAlerts>;
type Integrity='retained_offset_gap'|'consumer_offset_unknown';
const record=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
const exact=(v:unknown,names:string[])=>record(v)&&Object.keys(v).length===names.length&&names.every(k=>Object.hasOwn(v,k));
const maxInt64=9223372036854775807n;
function integer(v:unknown){if(typeof v!=='string'||!/^(0|[1-9]\d{0,18})$/.test(v)||BigInt(v)>maxInt64)throw 0;return BigInt(v);}
function time(v:unknown){if(typeof v!=='number'||!Number.isSafeInteger(v)||v<0||!Number.isFinite(new Date(v).getTime()))throw 0;return v;}
// Read-only diagnostics. Never changes admission decisions, reservations or probes.
export function evaluatePressureAlerts(input:unknown,policy:Readonly<PressurePolicy>=defaultPressurePolicy,now=Date.now(),context:{unavailable?:unknown;integrityReason?:Integrity|null}={}){
  if(!Number.isSafeInteger(now)||now<0||!Number.isFinite(new Date(now).getTime()))throw new Error('invalid_pressure_alert_time');
  if(!exact(policy,['maxLagMessages','maxLagAgeMs','maxProbeAgeMs','maxDiskAgeMs','minFreeBytes','minFreePercent','minFreeInodes'])||Object.values(policy).some(v=>!Number.isSafeInteger(v)||v<0)||policy.maxLagMessages<=0||policy.maxProbeAgeMs<=0||policy.maxDiskAgeMs<=0||policy.minFreePercent<=0||policy.minFreePercent>100)throw new Error('invalid_pressure_alert_policy');
  if(context.integrityReason!==undefined&&context.integrityReason!==null&&!['retained_offset_gap','consumer_offset_unknown'].includes(context.integrityReason))throw new Error('invalid_pressure_alert_context');
  const unknown=(rule:PressureSignal['rule'],reason:string):PressureSignal=>({rule,state:'unknown',reason,observed_at:null,expires_at:null,max_lag_messages:null,total_lag_messages:null});
  let kafka=unknown('kafka_backlog','missing_snapshot'),integrity=unknown('ingest_integrity','missing_snapshot'),storage=unknown('storage_headroom','missing_snapshot');
  const unavailable=context.unavailable!==undefined&&context.unavailable!==null;
  if(input!==null&&input!==undefined){
    kafka=unknown('kafka_backlog','invalid_snapshot');integrity=unknown('ingest_integrity','invalid_snapshot');storage=unknown('storage_headroom','invalid_snapshot');
    if(exact(input,['startedAt','partitions','disks'])){
      const snapshot=input as Record<string,unknown>;
      try{
        const started=time(snapshot.startedAt);
        if(started>now){kafka=unknown('kafka_backlog','future_sample');integrity=unknown('ingest_integrity','future_sample');}
        else if(unavailable){kafka=unknown('kafka_backlog','probe_unavailable');integrity=unknown('ingest_integrity','probe_unavailable');}
        else if(now-started>policy.maxProbeAgeMs){kafka=unknown('kafka_backlog','stale_sample');integrity=unknown('ingest_integrity','stale_sample');}
        else{
          const partitions=snapshot.partitions;if(!Array.isArray(partitions)||partitions.length<1||partitions.length>1024)throw 0;
          let max=0n,total=0n,old=false,gap=false,missing=false;const seen=new Set<number>();
          for(const p of partitions){
            if(!exact(p,['partition','low','high','committed','ageBoundary'])||!Number.isSafeInteger(p.partition)||p.partition<0||seen.has(p.partition))throw 0;
            seen.add(p.partition);const low=integer(p.low),high=integer(p.high),boundary=integer(p.ageBoundary),committed=p.committed===null?0n:integer(p.committed);
            if(low>high||boundary<low||boundary>high||committed>high)throw 0;
            missing ||=p.committed===null&&low>0n;gap ||=committed<low;
            const lag=high-committed;max=lag>max?lag:max;total+=lag;old ||=lag>0n&&committed<boundary;
          }
          const evidence={observed_at:new Date(started).toISOString(),expires_at:new Date(started+policy.maxProbeAgeMs).toISOString(),max_lag_messages:null,total_lag_messages:null};
          const issue:Integrity|null=missing?'consumer_offset_unknown':gap?'retained_offset_gap':null;
          integrity={rule:'ingest_integrity',state:issue?'firing':'clear',reason:issue??'offsets_consistent',...evidence};
          kafka=issue?{rule:'kafka_backlog',state:'unknown',reason:issue,...evidence}:{rule:'kafka_backlog',state:max>=BigInt(policy.maxLagMessages)||old?'firing':'clear',reason:max>=BigInt(policy.maxLagMessages)?'consumer_lag_high':old?'consumer_lag_old':'lag_within_budget',...evidence,max_lag_messages:max.toString(),total_lag_messages:total.toString()};
        }
      }catch{kafka=unknown('kafka_backlog','invalid_snapshot');integrity=unknown('ingest_integrity','invalid_snapshot');}
      try{
        const disks=snapshot.disks;if(!Array.isArray(disks)||disks.length!==2)throw 0;
        const seen=new Set<string>();let oldest=now,lowBytes=false,lowInodes=false,stale=false,future=false;
        for(const d of disks){
          if(!exact(d,['kind','sampledAt','totalBytes','availableBytes','totalInodes','availableInodes'])||!['kafka','postgres'].includes(d.kind)||seen.has(d.kind))throw 0;
          seen.add(d.kind);const at=time(d.sampledAt),total=integer(d.totalBytes),free=integer(d.availableBytes),files=integer(d.totalInodes),freeFiles=integer(d.availableInodes);
          if(total===0n||files===0n||free>total||freeFiles>files)throw 0;
          oldest=Math.min(oldest,at);future ||=at>now;stale ||=now-at>policy.maxDiskAgeMs;
          const percentage=(total*BigInt(policy.minFreePercent)+99n)/100n,reserve=percentage>BigInt(policy.minFreeBytes)?percentage:BigInt(policy.minFreeBytes);
          lowBytes ||=free<=reserve;lowInodes ||=freeFiles<=BigInt(policy.minFreeInodes);
        }
        storage=future?unknown('storage_headroom','future_sample'):unavailable?unknown('storage_headroom','probe_unavailable'):stale?unknown('storage_headroom','stale_sample'):{rule:'storage_headroom',state:lowBytes||lowInodes?'firing':'clear',reason:lowBytes?'disk_space_low':lowInodes?'disk_inodes_low':'headroom_available',observed_at:new Date(oldest).toISOString(),expires_at:new Date(oldest+policy.maxDiskAgeMs).toISOString(),max_lag_messages:null,total_lag_messages:null};
      }catch{storage=unknown('storage_headroom','invalid_snapshot');}
    }
  }
  // A latched integrity pause cannot be declared healthy merely because the
  // latest probe is missing, failed or now reports offsets inside the range.
  if(context.integrityReason)integrity={...unknown('ingest_integrity',context.integrityReason),state:'firing',observed_at:new Date(now).toISOString(),expires_at:new Date(now+policy.maxProbeAgeMs).toISOString()};
  return{scope:'platform_pressure' as const,computed_at:new Date(now).toISOString(),signals:[kafka,integrity,storage],policy:{max_lag_messages:String(policy.maxLagMessages),max_lag_age_ms:policy.maxLagAgeMs,max_probe_age_ms:policy.maxProbeAgeMs,max_disk_age_ms:policy.maxDiskAgeMs,min_free_bytes:String(policy.minFreeBytes),min_free_percent:policy.minFreePercent,min_free_inodes:String(policy.minFreeInodes)}};
}
