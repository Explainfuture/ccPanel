import {expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {parsePressureHistory} from '../apps/web/src/pressure-history';
const at=Date.UTC(2026,9,23),iso=(v:number)=>new Date(v).toISOString();
function fixture(){const instance=randomUUID(),assessment=evaluatePressureAlerts({startedAt:at,partitions:[{partition:0,low:'0',high:'9007199254740993',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind,sampledAt:at,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))},undefined,at);
  const rows=assessment.signals.map(signal=>({rule:signal.rule,state_at_assessment:signal.state,reason:signal.reason,instance_id:instance,started_at:iso(at-1000),assessed_at:iso(at),recorded_at:iso(at+1),state_since:iso(at),revision:'9007199254740993',signal_expires_at:signal.expires_at,signal,policy:assessment.policy,current_state:signal.state}));
  return{scope:'platform_pressure',computed_at:iso(at+2),retention_days:14,transition_limit:50,persistence_state:'fresh',states:rows,transitions:rows.map((r,i)=>({...r,id:String(i+1),change_kind:'initial'}))};
}
it('retains exact counters/revisions and accepts historical firing with expired current evidence',()=>{
  const value=fixture(),fresh=parsePressureHistory(value);expect(fresh?.states[0]?.revision).toBe('9007199254740993');expect(fresh?.states[0]?.signal.max_lag_messages).toBe('9007199254740993');
  value.persistence_state='expired';value.computed_at=iso(at+60000);value.states.forEach(r=>r.current_state='unknown');expect(parsePressureHistory(value)?.states[0]?.state_at_assessment).toBe('firing');
});
it('refuses malformed shape, duplicate rules/ids, oversized history and forged current health',()=>{
  const bad=fixture();bad.states[1]=bad.states[0]!;expect(parsePressureHistory(bad)).toBeNull();
  const partial=fixture();partial.states.pop();expect(parsePressureHistory(partial)).toBeNull();
  const mixed=fixture();mixed.states[0]!.instance_id=randomUUID();expect(parsePressureHistory(mixed)).toBeNull();
  const ids=fixture();ids.transitions[1]!.id=ids.transitions[0]!.id;expect(parsePressureHistory(ids)).toBeNull();
  const oversized=fixture();oversized.transitions=Array.from({length:51},()=>oversized.transitions[0]!);expect(parsePressureHistory(oversized)).toBeNull();
  const expired=fixture();expired.computed_at=iso(at+60000);expect(parsePressureHistory(expired)).toBeNull();
  const invalid=fixture();invalid.persistence_state='invalid';expect(parsePressureHistory(invalid)).toBeNull();
  const precise=fixture();precise.states[0]!.revision='9223372036854775808';expect(parsePressureHistory(precise)).toBeNull();
  expect(parsePressureHistory(null)).toBeNull();expect(parsePressureHistory({states:[]})).toBeNull();
});
