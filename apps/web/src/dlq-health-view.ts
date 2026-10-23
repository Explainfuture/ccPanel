import {evaluateDlqHealth,dlqHealthPolicy,type DlqHealth} from '../../../packages/server/src/dlq-health';
const record=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
const timestamp=(v:unknown):v is string=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v;
const rules=['dlq_pending','dlq_retention_risk'];
export function parseDlqHealth(input:unknown):DlqHealth|null{
  try{
    if(!record(input)||Object.keys(input).length!==5||input.scope!=='platform_dlq_index'||!timestamp(input.computed_at)||!record(input.policy)||Object.keys(input.policy).length!==3||!Object.entries(dlqHealthPolicy).every(([k,v])=>(input.policy as Record<string,unknown>)[k]===v)||!Array.isArray(input.signals)||input.signals.length!==2||!input.signals.every(record))return null;
    const signals=input.signals;
    if(new Set(signals.map(s=>s.rule)).size!==2||!rules.every(rule=>signals.some(s=>s.rule===rule)))return null;
    if(input.categories===null){
      if(!signals.every(s=>Object.keys(s).length===8&&s.state==='unknown'&&typeof s.reason==='string'&&['missing_snapshot','invalid_snapshot','future_sample','stale_sample','future_record'].includes(s.reason)&&['pending_records','oldest_pending_at','oldest_pending_age_seconds','observed_at','expires_at'].every(k=>s[k]===null))||signals[0].reason!==signals[1].reason)return null;
    }else{
      const first=signals[0],computed=evaluateDlqHealth({observed_at:first.observed_at,counts:input.categories,oldest_pending_at:first.oldest_pending_at,future_records:'0'},Date.parse(input.computed_at));
      if(computed.categories===null||!computed.signals.every(expected=>{const actual=signals.find(s=>s.rule===expected.rule)!;return Object.keys(actual).length===8&&Object.entries(expected).every(([key,value])=>actual[key]===value);}))return null;
    }
    return input as unknown as DlqHealth;
  }catch{return null;}
}
