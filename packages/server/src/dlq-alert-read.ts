import type {PoolClient} from 'pg';
import {canonical,type Json} from '../../schema/src/index';
import {digest} from './security';
import {withOperationalRead} from './operational-history-read';
import {evaluateDlqHealth} from './dlq-health';
import {parseDlqHealthAssessment} from './dlq-health-observation';
type Row={rule:string;state_at_assessment:string;reason:string;assessed_at:string;state_since:string;revision:string;source_observed_at:string|null;source_expires_at:string|null;policy_hash:string;payload_hash:string;evidence:{signal:Record<string,unknown>;policy:unknown;categories:unknown};id?:string;change_kind?:string};
const rules=['dlq_pending','dlq_retention_risk'] as const;
const iso=(value:string)=>new Date(value).toISOString();
const integer=(value:unknown):value is string=>typeof value==='string'&&/^[1-9]\d{0,18}$/.test(value)&&BigInt(value)<=9223372036854775807n;
// Decode only fixed aggregate evidence. Do not spread raw DB JSON into responses.
function decode(row:Row,now:number){
  try{
    if(!rules.includes(row.rule as typeof rules[number])||!integer(row.revision)||Date.parse(row.assessed_at)>now||Date.parse(row.state_since)>Date.parse(row.assessed_at))return null;
    const s=row.evidence.signal,at=iso(row.assessed_at);
    const assessment=row.evidence.categories===null?
      {scope:'platform_dlq_index',computed_at:at,policy:row.evidence.policy,categories:null,signals:rules.map(rule=>({...s,rule}))}:
      {...evaluateDlqHealth({observed_at:s.observed_at,counts:row.evidence.categories,oldest_pending_at:s.oldest_pending_at,future_records:'0'},Date.parse(at)),policy:row.evidence.policy};
    const valid=parseDlqHealthAssessment(assessment);if(!valid)return null;
    const signal=valid.signals.find(value=>value.rule===row.rule)!;
    if(Object.keys(s).length!==8||!Object.entries(signal).every(([key,value])=>s[key]===value)||signal.state!==row.state_at_assessment||signal.reason!==row.reason||signal.observed_at!==(row.source_observed_at===null?null:iso(row.source_observed_at))||signal.expires_at!==(row.source_expires_at===null?null:iso(row.source_expires_at))||digest(canonical(valid as unknown as Json))!==row.payload_hash||digest(canonical(valid.policy as unknown as Json))!==row.policy_hash)return null;
    return{rule:signal.rule,state_at_assessment:signal.state,reason:signal.reason,assessed_at:at,state_since:iso(row.state_since),revision:row.revision,source_observed_at:signal.observed_at,source_expires_at:signal.expires_at,signal,policy:valid.policy,categories:valid.categories};
  }catch{return null;}
}
export async function queryDlqAlertHistory(db:PoolClient){
  const data=(await db.query(`SELECT
    COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY rule) FROM
      (SELECT rule,state_at_assessment,reason,assessed_at,state_since,revision::text,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence FROM dlq_alert_states) s),'[]'::jsonb) AS states,
    COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY assessed_at DESC,id::bigint DESC) FROM
      (SELECT id::text,rule,state_at_assessment,reason,assessed_at,state_since,revision::text,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,change_kind FROM dlq_alert_history WHERE assessed_at>=CURRENT_TIMESTAMP-interval '14 days' AND assessed_at<=CURRENT_TIMESTAMP ORDER BY assessed_at DESC,dlq_alert_history.id DESC LIMIT 50) h),'[]'::jsonb) AS transitions`)).rows[0];
  const now=(await db.query('SELECT clock_timestamp() AS at')).rows[0].at as Date,states=data.states as Row[],history=data.transitions as Row[];
  const decoded=states.map(row=>decode(row,now.getTime()));
  const coherent=states.length===2&&new Set(states.map(r=>r.rule)).size===2&&decoded.every(r=>r!==null)&&states.every(r=>r.assessed_at===states[0]!.assessed_at&&r.policy_hash===states[0]!.policy_hash&&r.payload_hash===states[0]!.payload_hash);
  const visible=coherent?decoded.filter(r=>r!==null):[];
  const transitions=history.flatMap(row=>{const valid=decode(row,now.getTime());return valid&&integer(row.id)&&['initial','state','reason','policy','gap'].includes(row.change_kind??'')?[{...valid,id:row.id!,change_kind:row.change_kind!}]:[];});
  return{scope:'platform_dlq_index' as const,computed_at:now.toISOString(),retention_days:14,transition_limit:50,
    persistence_state:!states.length?'unavailable':!coherent?'invalid':visible.some(s=>s.source_expires_at===null||Date.parse(s.source_expires_at)<now.getTime())?'expired':'fresh',
    states:visible.map(s=>({...s,current_state:s.source_expires_at!==null&&Date.parse(s.source_expires_at)>=now.getTime()?s.state_at_assessment:'unknown'})),
    transitions,discarded_transitions:history.length-transitions.length};
}
export function readDlqAlertHistory(user:string,tenant:string,project:string){return withOperationalRead(user,tenant,project,queryDlqAlertHistory);}
