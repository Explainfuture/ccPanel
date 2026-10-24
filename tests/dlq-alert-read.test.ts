import {expect,it} from 'vitest';
import type {PoolClient} from 'pg';
import {canonical,type Json} from '../packages/schema/src/index';
import {digest} from '../packages/server/src/security';
import {evaluateDlqHealth} from '../packages/server/src/dlq-health';
import {queryDlqAlertHistory} from '../packages/server/src/dlq-alert-read';
const at=Date.UTC(2026,9,23);
function fixture(unknown=false){const value=evaluateDlqHealth(unknown?null:{observed_at:new Date(at).toISOString(),counts:{unresolved:'9007199254740993',replaying:'0',resolved:'0',suppressed:'0'},oldest_pending_at:new Date(at-13*86400000).toISOString(),future_records:'0'},at);return value.signals.map(signal=>({rule:signal.rule,state_at_assessment:signal.state,reason:signal.reason,assessed_at:value.computed_at,state_since:value.computed_at,revision:'9007199254740993',source_observed_at:signal.observed_at,source_expires_at:signal.expires_at,policy_hash:digest(canonical(value.policy as unknown as Json)),payload_hash:digest(canonical(value as unknown as Json)),evidence:{signal,policy:value.policy,categories:value.categories}}));}
async function query(states:unknown[],now:number,transitions:unknown[]=[]){let calls=0;const db={query:async()=>({rows:[calls++===0?{states,transitions}:{at:new Date(now)}]})} as unknown as PoolClient;return queryDlqAlertHistory(db);}
it('retains historical firing and exact revisions while source freshness expires independently',async()=>{
  const rows=fixture(),live=await query(rows,at+90000);expect(live.persistence_state).toBe('fresh');expect(live.states.every(s=>s.current_state==='firing')).toBe(true);expect(live.states[0]!.revision).toBe('9007199254740993');
  const expired=await query(rows,at+90001);expect(expired.persistence_state).toBe('expired');expect(expired.states.every(s=>s.current_state==='unknown'&&s.state_at_assessment==='firing')).toBe(true);
  expect((await query(fixture(true),at)).states.every(s=>s.current_state==='unknown')).toBe(true);
});
it('hides invalid latest evidence and reports discarded malformed history without spreading DB JSON',async()=>{
  expect((await query([],at)).persistence_state).toBe('unavailable');
  const rows=fixture();for(const invalid of[rows.slice(0,1),rows.map((r,i)=>i?r:{...r,payload_hash:'0'.repeat(64)}),rows.map(r=>({...r,revision:'9223372036854775808'}))]){const result=await query(invalid,at);expect(result.persistence_state).toBe('invalid');expect(result.states).toEqual([]);}
  expect((await query(rows,at-1)).persistence_state).toBe('invalid');
  const injected={...rows[0],id:'1',change_kind:'state',evidence:{...rows[0]!.evidence,signal:{...rows[0]!.evidence.signal,secret:'must-not-leak'}}};const result=await query(rows,at,[injected]);expect(result.discarded_transitions).toBe(1);expect(result.transitions).toEqual([]);expect(JSON.stringify(result)).not.toContain('must-not-leak');
});
