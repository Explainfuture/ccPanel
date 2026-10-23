import type {PressureAlerts,PressureSignal} from '../../../packages/server/src/pressure-alerts';
import {pressureLabels,validPressureAlerts} from './PressureSignalsPanel';
export type PressureRow={rule:PressureSignal['rule'];state_at_assessment:PressureSignal['state'];reason:string;instance_id:string;started_at:string;assessed_at:string;recorded_at:string;state_since:string;revision:string;signal_expires_at:string|null;signal:PressureSignal;policy:PressureAlerts['policy']};
export type PressureHistory={scope:'platform_pressure';computed_at:string;retention_days:14;transition_limit:50;persistence_state:'fresh'|'expired'|'invalid'|'unavailable';states:(PressureRow&{current_state:PressureSignal['state']})[];transitions:(PressureRow&{id:string;change_kind:string})[]};
const timestamp=(v:unknown):v is string=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v;
const state=(v:unknown):v is PressureSignal['state']=>typeof v==='string'&&['clear','firing','unknown'].includes(v);
const integer=(v:unknown):v is string=>typeof v==='string'&&/^[1-9]\d{0,18}$/.test(v)&&BigInt(v)<=9223372036854775807n;
const object=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
function row(v:unknown):v is PressureRow&Record<string,unknown>{
  if(!object(v)||typeof v.rule!=='string'||!Object.hasOwn(pressureLabels,v.rule)||!state(v.state_at_assessment)||typeof v.reason!=='string'||typeof v.instance_id!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v.instance_id)||!integer(v.revision))return false;
  if(!timestamp(v.started_at)||!timestamp(v.assessed_at)||!timestamp(v.recorded_at)||!timestamp(v.state_since)||(v.signal_expires_at!==null&&!timestamp(v.signal_expires_at)))return false;
  if(v.started_at>v.assessed_at||v.assessed_at>v.recorded_at||v.state_since>v.assessed_at||!object(v.signal)||v.signal.rule!==v.rule||v.signal.state!==v.state_at_assessment||v.signal.reason!==v.reason||v.signal.expires_at!==v.signal_expires_at)return false;
  const signals=Object.keys(pressureLabels).map(rule=>rule===v.rule?v.signal:{rule,state:'unknown',reason:'missing_snapshot',observed_at:null,expires_at:null,max_lag_messages:null,total_lag_messages:null});
  return validPressureAlerts({scope:'platform_pressure',computed_at:v.assessed_at,signals,policy:v.policy} as PressureAlerts);
}
export function parsePressureHistory(input:unknown):PressureHistory|null{
  try{
    if(!object(input)||input.scope!=='platform_pressure'||!timestamp(input.computed_at)||input.retention_days!==14||input.transition_limit!==50||!['fresh','expired','invalid','unavailable'].includes(String(input.persistence_state))||!Array.isArray(input.states)||input.states.length>3||!Array.isArray(input.transitions)||input.transitions.length>50)return null;
    const states=input.states;if(!states.every(row)||new Set(states.map(r=>r.rule)).size!==states.length)return null;
    if(['fresh','expired'].includes(String(input.persistence_state))&&states.length!==3)return null;
    if(['fresh','expired'].includes(String(input.persistence_state))&&states.some(r=>r.instance_id!==states[0]!.instance_id||r.started_at!==states[0]!.started_at||r.assessed_at!==states[0]!.assessed_at||r.recorded_at!==states[0]!.recorded_at||JSON.stringify(r.policy)!==JSON.stringify(states[0]!.policy)))return null;
    if(input.persistence_state==='unavailable'&&states.length)return null;
    for(const r of states){if(!state(r.current_state)||r.recorded_at>input.computed_at)return null;
      if(r.current_state!=='unknown'&&(input.persistence_state==='invalid'||input.persistence_state==='unavailable'||r.current_state!==r.state_at_assessment||r.signal_expires_at===null||r.signal_expires_at<input.computed_at))return null;
    }
    if(!input.transitions.every(v=>row(v)&&integer(v.id)&&typeof v.change_kind==='string'&&['initial','state','reason','policy','source','gap'].includes(v.change_kind)))return null;
    if(new Set(input.transitions.map(v=>v.id)).size!==input.transitions.length)return null;
    return input as unknown as PressureHistory;
  }catch{return null;}
}
