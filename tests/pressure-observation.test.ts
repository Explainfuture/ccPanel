import {expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {parsePressureObservation} from '../packages/server/src/pressure-observation';
const now=Date.UTC(2026,9,23);
const fixture=()=>({instance_id:randomUUID(),started_at:new Date(now-60000).toISOString(),assessment:evaluatePressureAlerts({startedAt:now,partitions:[{partition:0,low:'0',high:'9223372036854775807',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind,sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))},undefined,now)});
it('normalizes fixed signal order and hashes canonical observations without mutating input',()=>{
  const value=fixture(),first=parsePressureObservation(value,now);value.assessment.signals.reverse();const second=parsePressureObservation(value,now);
  expect(first.payload_hash).toBe(second.payload_hash);expect(first.policy_hash).toBe(second.policy_hash);expect(value.assessment.signals[0]!.rule).toBe('storage_headroom');expect(second.value.assessment.signals[0]!.max_lag_messages).toBe('9223372036854775807');
});
it('retains source expiry instead of replacing it with the later recording time',()=>{
  const value=fixture();expect(parsePressureObservation(value,now+120000).value.assessment.signals[0]!.expires_at).toBe(new Date(now+15000).toISOString());
  expect(()=>parsePressureObservation(value,now-1)).toThrow();expect(()=>parsePressureObservation(value,now+7*86400000+1)).toThrow();
});
it('rejects extra fields, reused rules, missing evidence and forged normality or precision',()=>{
  const value=fixture();expect(()=>parsePressureObservation({...value,secret:'private'},now)).toThrow();
  const duplicate=fixture();duplicate.assessment.signals[1]={...duplicate.assessment.signals[0]!};expect(()=>parsePressureObservation(duplicate,now)).toThrow();
  for(const patch of[{state:'clear',reason:'lag_within_budget'},{max_lag_messages:'01'},{max_lag_messages:'9223372036854775808'},{expires_at:null},{expires_at:new Date(now+15001).toISOString()},{reason:'arbitrary_label'}]){
    const invalid=fixture();Object.assign(invalid.assessment.signals[0]!,patch);expect(()=>parsePressureObservation(invalid,now)).toThrow('invalid_pressure_observation');
  }
  expect(()=>parsePressureObservation({...value,assessment:{...value.assessment,padding:'x'.repeat(4096)}},now)).toThrow();
});
it('allows unknown evidence and a cached sample captured before metrics initialization',()=>{
  const unknown={...fixture(),assessment:evaluatePressureAlerts(null,undefined,now)};expect(parsePressureObservation(unknown,now).value.assessment.signals.every(r=>r.state==='unknown')).toBe(true);
  const cached=fixture();cached.started_at=new Date(now).toISOString();for(const r of cached.assessment.signals){r.observed_at=new Date(now-1000).toISOString();r.expires_at=new Date(now-1000+(r.rule==='storage_headroom'?30000:15000)).toISOString();}expect(parsePressureObservation(cached,now).assessed.getTime()).toBe(now);
});
