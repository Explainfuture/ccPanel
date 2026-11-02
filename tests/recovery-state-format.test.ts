import {it,expect} from 'vitest';
import {parseRecoverySourceState} from '../packages/server/src/recovery-state-format';
const now=Date.parse('2026-11-02T00:10:00.000Z'),at='2026-11-02T00:09:00.000Z';
const state=()=>({version:1,status:'exported_unverified',source:{id:'source-001',database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'9223372036854775807',streams:[{topic:'events',group_id:'worker',partition_count:2,initialized_at:at,positions:[{partition:0,next_offset:'9007199254740993',updated_at:at},{partition:1,next_offset:'9223372036854775807',updated_at:at}]}]},kafka_observation_verified:false,latest_state_verified:false});
it('preserves exact PG offset/audit text and keeps all trust assertions false',()=>{
  const result=parseRecoverySourceState(state(),{now});
  expect(result.integrity.streams[0]!.positions[0]!.next_offset).toBe('9007199254740993');expect(result.integrity.audit_high_water).toBe('9223372036854775807');
  expect(result.source.authenticated).toBe(false);expect(result.latest_state_verified).toBe(false);expect(result.kafka_observation_verified).toBe(false);
});
it('accepts a described active integrity incident and a retained last-detection time',()=>{
  const active:any=state();active.integrity.blocked=true;active.integrity.reason='retained_offset_gap';active.integrity.detected_at=at;active.integrity.details={partition:0,expected:'9007199254740993',observed:['a',1,true,null]};
  expect(parseRecoverySourceState(active,{now}).integrity.blocked).toBe(true);
  active.integrity.blocked=false;active.integrity.reason=null;expect(parseRecoverySourceState(active,{now}).integrity.detected_at).toBe(at);
});
it('rejects stale/future state and requires the configured freshness bound',()=>{
  expect(()=>parseRecoverySourceState(state(),{now:now+600000})).toThrow('state_not_fresh');
  expect(()=>parseRecoverySourceState(state(),{now:now-400000})).toThrow('state_not_fresh');
  expect(()=>parseRecoverySourceState(state(),{now,maxAgeSeconds:59})).toThrow('state_not_fresh');
  expect(parseRecoverySourceState(state(),{now,maxAgeSeconds:60}).exported_at).toBe(at);
});
it.each([0,3600,NaN,1.5])('rejects invalid freshness option %s',maxAgeSeconds=>{expect(()=>parseRecoverySourceState(state(),{now,maxAgeSeconds})).toThrow('state_format_options_invalid');});
it('rejects legacy deletion format, timestamp disagreement and unknown/promotion fields',()=>{
  const changes=[(s:any)=>{delete s.deletion.retention;s.deletion.schema_version=1;},(s:any)=>s.deletion.exported_at='2026-11-02T00:08:00.000Z',(s:any)=>s.extra=true,(s:any)=>s.source.authenticated=true,(s:any)=>s.latest_state_verified=true,(s:any)=>s.kafka_observation_verified=true,(s:any)=>s.source.schema_version=23,(s:any)=>s.integrity.extra=true];
  for(const change of changes){const value:any=state();change(value);expect(()=>parseRecoverySourceState(value,{now})).toThrow('invalid_recovery_state');}
});
it('requires complete unique contiguous partition descriptions without numeric offsets',()=>{
  const changes=[(s:any)=>s.integrity.streams[0].positions.pop(),(s:any)=>s.integrity.streams[0].positions[1].partition=0,(s:any)=>s.integrity.streams[0].positions[1].partition=2,(s:any)=>s.integrity.streams.push(s.integrity.streams[0]),(s:any)=>s.integrity.streams[0].positions[0].next_offset=9007199254740992,(s:any)=>s.integrity.streams[0].positions[0].next_offset='01',(s:any)=>s.integrity.audit_high_water='9223372036854775808'];
  for(const change of changes){const value:any=state();change(value);expect(()=>parseRecoverySourceState(value,{now})).toThrow('invalid_recovery_state');}
});
it('rejects inconsistent gate state, invalid JSON details and oversized detail data',()=>{
  const changes=[(s:any)=>s.integrity.reason='retained_offset_gap',(s:any)=>s.integrity.blocked=true,(s:any)=>s.integrity.details=[],(s:any)=>s.integrity.details={n:Infinity},(s:any)=>s.integrity.details={bad:undefined},(s:any)=>s.integrity.details={huge:'x'.repeat(65536)},(s:any)=>{s.integrity.details.self=s.integrity.details;}];
  for(const change of changes){const value:any=state();change(value);expect(()=>parseRecoverySourceState(value,{now})).toThrow('invalid_recovery_state');}
});
it('bounds source identifiers and allows an empty PG stream description without claiming broker completeness',()=>{
  const value:any=state();value.integrity.streams=[];expect(parseRecoverySourceState(value,{now}).integrity.streams).toEqual([]);
  for(const field of ['id','database']){const bad:any=state();bad.source[field]='\u0000';expect(()=>parseRecoverySourceState(bad,{now})).toThrow('invalid_recovery_state');}
});
