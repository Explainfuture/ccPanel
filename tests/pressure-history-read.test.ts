import {expect,it} from 'vitest';
import type {PoolClient} from 'pg';
import {randomUUID} from 'node:crypto';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {parsePressureObservation} from '../packages/server/src/pressure-observation';
import {queryPressureHistory} from '../packages/server/src/pressure-history-read';
const at=Date.UTC(2026,9,23);
function fixture(){
  const observation={instance_id:randomUUID(),started_at:new Date(at-60000).toISOString(),assessment:evaluatePressureAlerts({startedAt:at,partitions:[{partition:0,low:'0',high:'0',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind,sampledAt:at,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))},undefined,at)};
  const parsed=parsePressureObservation(observation,at);
  return observation.assessment.signals.map(signal=>({rule:signal.rule,state_at_assessment:signal.state,reason:signal.reason,instance_id:observation.instance_id,started_at:observation.started_at,assessed_at:observation.assessment.computed_at,recorded_at:new Date(at+1).toISOString(),state_since:observation.assessment.computed_at,revision:'9007199254740993',signal_expires_at:signal.expires_at,policy_hash:parsed.policy_hash,payload_hash:parsed.payload_hash,evidence:{signal,policy:observation.assessment.policy}}));
}
async function query(states:unknown[],now:number){let calls=0;const db={query:async()=>({rows:[calls++===0?{states,transitions:[]}:{at:new Date(now)}]})} as unknown as PoolClient;const result=await queryPressureHistory(db);expect(calls).toBe(2);return result;}
it('keeps independent source expiry and historical state, with exact string revisions',async()=>{
  const rows=fixture(),boundary=await query(rows,at+15000);expect(boundary.persistence_state).toBe('fresh');expect(boundary.states.every(s=>s.current_state==='clear')).toBe(true);expect(boundary.states[0]!.revision).toBe('9007199254740993');
  const expired=await query(rows,at+15001);expect(expired.persistence_state).toBe('expired');expect(expired.states.find(s=>s.rule==='kafka_backlog')!.current_state).toBe('unknown');expect(expired.states.find(s=>s.rule==='storage_headroom')!.current_state).toBe('clear');expect(expired.states.every(s=>s.state_at_assessment==='clear')).toBe(true);
  const stopped=await query(rows,at+8*86400000);expect(stopped.persistence_state).toBe('expired');expect(stopped.states.every(s=>s.current_state==='unknown')).toBe(true);
});
it('treats absent, incomplete, mismatched and future evidence conservatively',async()=>{
  expect((await query([],at)).persistence_state).toBe('unavailable');
  const rows=fixture();
  for(const malformed of[rows.slice(0,2),rows.map((r,i)=>i? r:{...r,payload_hash:'0'.repeat(64)}),rows.map((r,i)=>i?r:{...r,evidence:{...r.evidence,policy:{...r.evidence.policy,max_lag_messages:'999'}}})]){
    const result=await query(malformed,at+100);expect(result.persistence_state).toBe('invalid');expect(result.states.every(s=>s.current_state==='unknown')).toBe(true);
  }
  expect((await query(rows,at)).persistence_state,'a future recorded_at cannot appear fresh').toBe('invalid');
});
