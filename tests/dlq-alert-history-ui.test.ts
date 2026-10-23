import {expect,it} from 'vitest';
import {evaluateDlqHealth} from '../packages/server/src/dlq-health';
import {parseDlqAlertHistory} from '../apps/web/src/dlq-alert-history';
const now=Date.UTC(2026,9,23),iso=(v:number)=>new Date(v).toISOString();
function fixture(){
  const health=evaluateDlqHealth({observed_at:iso(now),counts:{unresolved:'9007199254740993',replaying:'0',resolved:'0',suppressed:'0'},oldest_pending_at:iso(now-13*86400000),future_records:'0'},now);
  const rows=health.signals.map(signal=>({rule:signal.rule,state_at_assessment:signal.state,reason:signal.reason,assessed_at:iso(now),state_since:iso(now),revision:'9007199254740993',source_observed_at:signal.observed_at,source_expires_at:signal.expires_at,signal,policy:health.policy,categories:health.categories}));
  return{scope:'platform_dlq_index',computed_at:iso(now+1),retention_days:14,transition_limit:50,persistence_state:'fresh',states:rows.map(r=>({...r,current_state:r.state_at_assessment})),transitions:rows.map((r,i)=>({...r,id:String(i+1),change_kind:'initial'})),discarded_transitions:0};
}
it('preserves exact revisions and historical firing when the current source has expired',()=>{
  const value=fixture();expect(parseDlqAlertHistory(value)?.states[0]?.revision).toBe('9007199254740993');expect(parseDlqAlertHistory(value)?.states[0]?.signal.pending_records).toBe('9007199254740993');
  value.computed_at=iso(now+90001);value.persistence_state='expired';value.states.forEach(s=>s.current_state='unknown');expect(parseDlqAlertHistory(value)?.states.every(s=>s.state_at_assessment==='firing')).toBe(true);
  value.discarded_transitions=1;expect(parseDlqAlertHistory(value)?.discarded_transitions).toBe(1);
  expect(parseDlqAlertHistory({...value,states:[],persistence_state:'invalid'})?.transitions.length).toBe(2);
});
it('refuses forged current health, partial/coherence errors, malformed history and extra fields',()=>{
  const stale=fixture();stale.computed_at=iso(now+90001);expect(parseDlqAlertHistory(stale)).toBeNull();
  const partial=fixture();partial.states.pop();expect(parseDlqAlertHistory(partial)).toBeNull();
  const duplicate=fixture();duplicate.states[1]=duplicate.states[0]!;expect(parseDlqAlertHistory(duplicate)).toBeNull();
  const revision=fixture();revision.states[0]!.revision='9223372036854775808';expect(parseDlqAlertHistory(revision)).toBeNull();
  const wrong=fixture();wrong.states[0]!.signal.pending_records='0';expect(parseDlqAlertHistory(wrong)).toBeNull();
  const ids=fixture();ids.transitions[1]!.id=ids.transitions[0]!.id;expect(parseDlqAlertHistory(ids)).toBeNull();
  const extra=fixture();Object.assign(extra.states[0]!,{secret:'unexpected'});expect(parseDlqAlertHistory(extra)).toBeNull();
  const oversized=fixture();oversized.transitions=Array.from({length:51},()=>oversized.transitions[0]!);expect(parseDlqAlertHistory(oversized)).toBeNull();
  expect(parseDlqAlertHistory({...fixture(),discarded_transitions:49})).toBeNull();expect(parseDlqAlertHistory(null)).toBeNull();
  const badCounts=evaluateDlqHealth({},now),rows=fixture().states;
  expect(parseDlqAlertHistory({...fixture(),persistence_state:'expired',states:badCounts.signals.map((signal,i)=>({...rows[i],state_at_assessment:signal.state,current_state:'unknown',reason:signal.reason,source_observed_at:null,source_expires_at:null,signal,categories:{secret:'must-not-accept'}})),transitions:[]})).toBeNull();
});
