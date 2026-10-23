import {canonical,type Json} from '../../schema/src/index';
import {digest} from './security';
import {evaluatePressureAlerts,type PressureAlerts} from './pressure-alerts';
export type PressureObservation={instance_id:string;started_at:string;assessment:PressureAlerts};
const fail=():never=>{throw new Error('invalid_pressure_observation');};
function fields(v:unknown,keys:string[]):asserts v is Record<string,unknown>{if(!v||typeof v!=='object'||Array.isArray(v)||Object.keys(v).length!==keys.length||!keys.every(k=>Object.hasOwn(v,k)))fail();}
function time(v:unknown){if(typeof v!=='string'||!Number.isFinite(Date.parse(v))||new Date(v).toISOString()!==v)return fail();return Date.parse(v);}
function integer(v:unknown){if(typeof v!=='string'||!/^(0|[1-9]\d{0,39})$/.test(v))return fail();return BigInt(v);}
const base=['missing_snapshot','invalid_snapshot','future_sample','probe_unavailable','stale_sample'];
const reasons:Record<string,Record<string,string[]>>={
 kafka_backlog:{clear:['lag_within_budget'],firing:['consumer_lag_high','consumer_lag_old'],unknown:[...base,'consumer_offset_unknown','retained_offset_gap']},
 ingest_integrity:{clear:['offsets_consistent'],firing:['consumer_offset_unknown','retained_offset_gap'],unknown:base},
 storage_headroom:{clear:['headroom_available'],firing:['disk_space_low','disk_inodes_low'],unknown:base},
};
export function parsePressureObservation(input:unknown,now=Date.now()){
  let text:string|undefined;try{text=JSON.stringify(input);}catch{return fail();}
  if(typeof text!=='string'||Buffer.byteLength(text)>4096||!Number.isSafeInteger(now)||!Number.isFinite(new Date(now).getTime()))return fail();
  const value=JSON.parse(text) as PressureObservation;fields(value,['instance_id','started_at','assessment']);
  if(typeof value.instance_id!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.instance_id))return fail();
  const started=time(value.started_at),a=value.assessment;fields(a,['scope','computed_at','signals','policy']);
  const assessed=time(a.computed_at);if(started>assessed||assessed>now||assessed<now-7*86400000||a.scope!=='platform_pressure')return fail();
  const p=a.policy;fields(p,['max_lag_messages','max_lag_age_ms','max_probe_age_ms','max_disk_age_ms','min_free_bytes','min_free_percent','min_free_inodes']);
  const lag=integer(p.max_lag_messages),bytes=integer(p.min_free_bytes),inodes=integer(p.min_free_inodes);
  if([lag,bytes,inodes].some(v=>v>BigInt(Number.MAX_SAFE_INTEGER)))return fail();
  try{evaluatePressureAlerts(null,{maxLagMessages:Number(lag),maxLagAgeMs:p.max_lag_age_ms,maxProbeAgeMs:p.max_probe_age_ms,maxDiskAgeMs:p.max_disk_age_ms,minFreeBytes:Number(bytes),minFreePercent:p.min_free_percent,minFreeInodes:Number(inodes)},now);}catch{return fail();}
  if(!Array.isArray(a.signals)||a.signals.length!==3)return fail();const seen=new Set<string>();
  for(const r of a.signals){
    fields(r,['rule','state','reason','observed_at','expires_at','max_lag_messages','total_lag_messages']);
    if(!Object.hasOwn(reasons,r.rule)||!reasons[r.rule]![r.state]?.includes(r.reason)||seen.has(r.rule))return fail();seen.add(r.rule);
    if(r.observed_at===null||r.expires_at===null){if(r.observed_at!==null||r.expires_at!==null||r.state!=='unknown'||!base.includes(r.reason))return fail();}
    else{
      const observed=time(r.observed_at),expires=time(r.expires_at),ttl=r.rule==='storage_headroom'?p.max_disk_age_ms:p.max_probe_age_ms;
      if(observed>assessed||expires<assessed||expires-observed!==ttl)return fail();
    }
    if(r.rule==='kafka_backlog'&&r.state!=='unknown'){
      const max=integer(r.max_lag_messages),total=integer(r.total_lag_messages);if(max>9223372036854775807n||total<max||total>max*1024n)return fail();
      if(r.state==='clear'&&max>=lag||r.reason==='consumer_lag_high'&&max<lag||r.reason==='consumer_lag_old'&&(max===0n||max>=lag))return fail();
    }else if(r.max_lag_messages!==null||r.total_lag_messages!==null)return fail();
  }
  const order=['kafka_backlog','ingest_integrity','storage_headroom'];a.signals.sort((a,b)=>order.indexOf(a.rule)-order.indexOf(b.rule));
  return{value,started:new Date(started),assessed:new Date(assessed),payload_hash:digest(canonical(value as unknown as Json)),policy_hash:digest(canonical(p as unknown as Json))};
}
