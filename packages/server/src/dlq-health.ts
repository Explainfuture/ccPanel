export const dlqHealthPolicy=Object.freeze({source_retention_days:14,retention_warning_days:12,freshness_ms:90000} as const);
const categories=['unresolved','replaying','resolved','suppressed'] as const;
type Counts=Record<typeof categories[number],string>;
export type DlqHealthSignal={rule:'dlq_pending'|'dlq_retention_risk';state:'clear'|'firing'|'unknown';reason:string;pending_records:string|null;oldest_pending_at:string|null;oldest_pending_age_seconds:string|null;observed_at:string|null;expires_at:string|null};
export type DlqHealth={scope:'platform_dlq_index';computed_at:string;policy:typeof dlqHealthPolicy;categories:Counts|null;signals:DlqHealthSignal[]};
const record=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
const timestamp=(v:unknown):v is string=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v;
function counter(v:unknown){if(typeof v!=='string'||!/^(0|[1-9]\d{0,18})$/.test(v)||BigInt(v)>9223372036854775807n)throw 0;return BigInt(v);}
export function evaluateDlqHealth(input:unknown,now=Date.now()):DlqHealth{
  if(!Number.isSafeInteger(now)||!Number.isFinite(new Date(now).getTime()))throw new Error('invalid_dlq_health_time');
  const output=(counts:Counts|null,signals:DlqHealthSignal[]):DlqHealth=>({scope:'platform_dlq_index',computed_at:new Date(now).toISOString(),policy:dlqHealthPolicy,categories:counts,signals});
  const unknown=(reason:string)=>output(null,(['dlq_pending','dlq_retention_risk'] as const).map(rule=>({rule,state:'unknown',reason,pending_records:null,oldest_pending_at:null,oldest_pending_age_seconds:null,observed_at:null,expires_at:null})));
  if(input===null||input===undefined)return unknown('missing_snapshot');
  try{
    if(!record(input)||Object.keys(input).length!==4||!['observed_at','counts','oldest_pending_at','future_records'].every(k=>Object.hasOwn(input,k))||!timestamp(input.observed_at)||!record(input.counts)||Object.keys(input.counts).length!==4||!categories.every(k=>Object.hasOwn(input.counts as object,k)))throw 0;
    const counts=input.counts,pending=counter(counts.unresolved)+counter(counts.replaying),total=categories.reduce((sum,key)=>sum+counter(counts[key]),0n),future=counter(input.future_records);
    if(total>9223372036854775807n||future>total||(pending===0n?input.oldest_pending_at!==null:!timestamp(input.oldest_pending_at)))throw 0;
    const observed=Date.parse(input.observed_at);
    if(input.oldest_pending_at!==null&&Date.parse(input.oldest_pending_at as string)>observed&&future===0n)throw 0;
    if(observed>now)return unknown('future_sample');
    if(now-observed>dlqHealthPolicy.freshness_ms)return unknown('stale_sample');
    if(future>0n)return unknown('future_record');
    const oldest=input.oldest_pending_at as string|null,age=oldest===null?null:now-Date.parse(oldest),risk=age!==null&&age>=dlqHealthPolicy.retention_warning_days*86400000;
    const evidence={pending_records:pending.toString(),oldest_pending_at:oldest,oldest_pending_age_seconds:age===null?null:Math.floor(age/1000).toString(),observed_at:input.observed_at,expires_at:new Date(observed+dlqHealthPolicy.freshness_ms).toISOString()};
    return output(Object.fromEntries(categories.map(k=>[k,counts[k]])) as Counts,[
      {rule:'dlq_pending',state:pending>0n?'firing':'clear',reason:pending>0n?'retained_pending':'no_retained_pending',...evidence},
      {rule:'dlq_retention_risk',state:risk?'firing':'clear',reason:risk?(age!==null&&age>=14*86400000?'retention_window_exceeded':'retention_window_near'):pending===0n?'no_retained_pending':'below_retention_warning',...evidence},
    ]);
  }catch{return unknown('invalid_snapshot');}
}
