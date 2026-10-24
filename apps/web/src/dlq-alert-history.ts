import {evaluateDlqHealth,type DlqHealth,type DlqHealthSignal} from '../../../packages/server/src/dlq-health';
import {parseDlqHealthAssessment} from '../../../packages/server/src/dlq-health-observation';
export type DlqHistoryRow={rule:DlqHealthSignal['rule'];state_at_assessment:DlqHealthSignal['state'];reason:string;assessed_at:string;state_since:string;revision:string;source_observed_at:string|null;source_expires_at:string|null;signal:DlqHealthSignal;policy:DlqHealth['policy'];categories:DlqHealth['categories']};
export type DlqAlertHistory={scope:'platform_dlq_index';computed_at:string;retention_days:14;transition_limit:50;persistence_state:'fresh'|'expired'|'invalid'|'unavailable';states:(DlqHistoryRow&{current_state:DlqHealthSignal['state']})[];transitions:(DlqHistoryRow&{id:string;change_kind:string})[];discarded_transitions:number};
const rules=['dlq_pending','dlq_retention_risk'] as const;
const record=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
const timestamp=(v:unknown):v is string=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v;
const integer=(v:unknown):v is string=>typeof v==='string'&&/^[1-9]\d{0,18}$/.test(v)&&BigInt(v)<=9223372036854775807n;
const state=(v:unknown):v is DlqHealthSignal['state']=>typeof v==='string'&&['clear','firing','unknown'].includes(v);
function row(value:unknown,computed:string):value is DlqHistoryRow&Record<string,unknown>{
  if(!record(value)||!rules.includes(value.rule as typeof rules[number])||!state(value.state_at_assessment)||typeof value.reason!=='string'||!timestamp(value.assessed_at)||!timestamp(value.state_since)||value.state_since>value.assessed_at||value.assessed_at>computed||!integer(value.revision)||!record(value.signal))return false;
  const s=value.signal,at=value.assessed_at;
  const frame=value.categories===null?{scope:'platform_dlq_index',computed_at:at,policy:value.policy,categories:null,signals:rules.map(rule=>({...s,rule}))}:{...evaluateDlqHealth({observed_at:s.observed_at,counts:value.categories,oldest_pending_at:s.oldest_pending_at,future_records:'0'},Date.parse(at)),policy:value.policy};
  const parsed=parseDlqHealthAssessment(frame),signal=parsed?.signals.find(r=>r.rule===value.rule);
  return !!signal&&!(value.categories!==null&&parsed?.categories===null)&&Object.keys(s).length===8&&Object.entries(signal).every(([k,v])=>s[k]===v)&&signal.state===value.state_at_assessment&&signal.reason===value.reason&&signal.observed_at===value.source_observed_at&&signal.expires_at===value.source_expires_at;
}
export function parseDlqAlertHistory(input:unknown):DlqAlertHistory|null{
  try{
    if(!record(input)||Object.keys(input).length!==8||input.scope!=='platform_dlq_index'||!timestamp(input.computed_at)||input.retention_days!==14||input.transition_limit!==50||!['fresh','expired','invalid','unavailable'].includes(String(input.persistence_state))||!Array.isArray(input.states)||input.states.length>2||!Array.isArray(input.transitions)||input.transitions.length>50||!Number.isInteger(input.discarded_transitions)||Number(input.discarded_transitions)<0||input.transitions.length+Number(input.discarded_transitions)>50)return null;
    const at=input.computed_at,states=input.states;
    if(!states.every(r=>row(r,at)&&Object.keys(r).length===12)||new Set(states.map(r=>r.rule)).size!==states.length)return null;
    if(['fresh','expired'].includes(String(input.persistence_state))&&states.length!==2||['invalid','unavailable'].includes(String(input.persistence_state))&&states.length!==0)return null;
    if(states.length&&states.some(r=>r.assessed_at!==states[0]!.assessed_at||r.source_observed_at!==states[0]!.source_observed_at||r.source_expires_at!==states[0]!.source_expires_at||JSON.stringify(r.categories)!==JSON.stringify(states[0]!.categories)))return null;
    const expired=states.some(r=>r.source_expires_at===null||r.source_expires_at<at);
    if(input.persistence_state==='fresh'&&expired||input.persistence_state==='expired'&&!expired)return null;
    for(const r of states){if(!state(r.current_state)||r.current_state!=='unknown'&&(r.current_state!==r.state_at_assessment||r.source_expires_at===null||r.source_expires_at<at))return null;}
    if(!input.transitions.every(r=>row(r,at)&&Object.keys(r).length===13&&integer(r.id)&&typeof r.change_kind==='string'&&['initial','state','reason','policy','gap'].includes(r.change_kind))||new Set(input.transitions.map(r=>r.id)).size!==input.transitions.length)return null;
    return input as unknown as DlqAlertHistory;
  }catch{return null;}
}
