import {it,expect} from 'vitest';
import {assertRecoveryStateContinuity} from '../packages/server/src/recovery-state-continuity';
import {parseRecoverySourceState,type RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {retentionCutoff} from '../packages/server/src/retention-policy';

const at='2026-11-02T00:00:00.000Z',later='2026-11-02T00:05:00.000Z';
const tenant='00000000-0000-4000-8000-000000000001',project='00000000-0000-4000-8000-000000000002',other='00000000-0000-4000-8000-000000000003';
const job='00000000-0000-4000-8000-000000000004',nextJob='00000000-0000-4000-8000-000000000005';
const policy=(id=project,raw_days=7,version=2,floor:string|null='2026-10-24T00:00:00.000Z',time=at)=>({tenant_id:tenant,project_id:id,raw_days,version,floor,effective_from:retentionCutoff({raw_days,version,floor},Date.parse(time))});
function fixture():RecoverySourceState{
  const requested='2026-11-01T00:00:00.000Z',expires='2027-01-01T00:00:00.000Z';
  return parseRecoverySourceState({version:1,status:'exported_unverified',source:{id:'source-001',database:'source_fixture',schema_version:22,authenticated:false},exported_at:at,
    deletion:{schema_version:2,exported_at:at,projects:[{tenant_id:tenant,id:project,privacy_epoch:2}],jobs:[{id:job,tenant_id:tenant,project_id:project,user_id:null,scope:'project',subject_hash:null,requested_at:requested,blocked_before:requested,stream_expires_at:expires}],barriers:[{tenant_id:tenant,project_id:project,identity_hash:'*',deletion_job_id:job,requested_at:requested,blocked_before:requested}],retention:[policy()]},
    integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'9007199254740993',streams:[{topic:'events',group_id:'worker',partition_count:1,initialized_at:at,positions:[{partition:0,next_offset:'9007199254740993',updated_at:at}]}]},kafka_observation_verified:false,latest_state_verified:false},{now:Date.parse(at)});
}
function advance(value:RecoverySourceState,time=later):RecoverySourceState{
  const next=structuredClone(value);next.exported_at=time;next.deletion.exported_at=time;
  for(const row of next.deletion.retention)row.effective_from=retentionCutoff(row,Date.parse(time));
  return next;
}
function rejected(change:(next:RecoverySourceState)=>void,code:string){
  const before=fixture(),next=advance(before);change(next);expect(()=>assertRecoveryStateContinuity(before,next)).toThrow(code);
}

it('accepts later descriptions without mutating or promoting their trust flags',()=>{
  const before=fixture(),next=advance(before),snapshot=JSON.stringify([before,next]);
  next.integrity.streams[0]!.positions[0]!.next_offset='9007199254740994';
  const original=JSON.stringify([before,next]);assertRecoveryStateContinuity(before,next);
  expect(JSON.stringify([before,next])).toBe(original);expect(original).not.toBe(snapshot);
  expect(next.source.authenticated).toBe(false);expect(next.latest_state_verified).toBe(false);
});
it('accepts added projects, streams and partitions while retaining prior offsets exactly',()=>{
  const before=fixture(),next=advance(before);
  next.deletion.retention.push(policy(other,30,1,null,later));
  next.integrity.streams[0]!.partition_count=2;next.integrity.streams[0]!.positions.push({partition:1,next_offset:'0',updated_at:later});
  next.integrity.streams.push({topic:'new-events',group_id:'new-worker',partition_count:1,initialized_at:later,positions:[{partition:0,next_offset:'0',updated_at:later}]});
  assertRecoveryStateContinuity(before,next);expect(next.integrity.streams[0]!.positions[0]!.next_offset).toBe('9007199254740993');
});
it('accepts a retention extension only when it retains the previous effective boundary',()=>{
  const before=fixture(),next=advance(before);
  next.deletion.retention[0]=policy(project,30,3,before.deletion.retention[0]!.effective_from,later);
  assertRecoveryStateContinuity(before,next);
  next.deletion.retention[0]=policy(project,30,3,before.deletion.retention[0]!.floor,later);
  expect(()=>assertRecoveryStateContinuity(before,next)).toThrow('state_retention_boundary_regressed');
});
it('allows repeated deletion to supersede the old job while retaining protection',()=>{
  const before=fixture(),next=advance(before),requested='2026-11-02T00:01:00.000Z';
  next.deletion.jobs=[{...next.deletion.jobs[0]!,id:nextJob,requested_at:requested,blocked_before:requested,stream_expires_at:'2027-01-02T00:00:00.000Z'}];
  next.deletion.barriers[0]={...next.deletion.barriers[0]!,deletion_job_id:nextJob,requested_at:requested,blocked_before:requested};
  next.deletion.projects[0]!.privacy_epoch=3;assertRecoveryStateContinuity(before,next);
});
it('requires audit advancement for gate changes, while details-only changes remain allowed',()=>{
  const before=fixture(),next=advance(before);next.integrity.details={diagnostic:'reordered or refreshed metadata'};
  assertRecoveryStateContinuity(before,next);
  next.integrity.blocked=true;next.integrity.reason='retained_offset_gap';next.integrity.detected_at=later;
  expect(()=>assertRecoveryStateContinuity(before,next)).toThrow('state_gate_audit_missing');
  next.integrity.audit_high_water='9007199254740994';assertRecoveryStateContinuity(before,next);
  const resumed=advance(next,'2026-11-02T00:06:00.000Z');resumed.integrity.blocked=false;resumed.integrity.reason=null;
  expect(()=>assertRecoveryStateContinuity(next,resumed)).toThrow('state_gate_audit_missing');
  resumed.integrity.audit_high_water='9007199254740995';assertRecoveryStateContinuity(next,resumed);
});
it('rejects source changes, temporal regression and audit regression',()=>{
  rejected(s=>s.source.id='different','state_source_changed');rejected(s=>s.source.database='other','state_source_changed');
  rejected(s=>{const time='2026-11-01T23:59:00.000Z';s.exported_at=time;s.deletion.exported_at=time;for(const p of s.deletion.retention)p.effective_from=retentionCutoff(p,Date.parse(time));},'state_time_regressed');
  rejected(s=>s.integrity.audit_high_water='9007199254740992','state_audit_regressed');
});
it('rejects disappearing projects, policy rollback and same-version policy disagreement',()=>{
  rejected(s=>{s.deletion.projects=[];s.deletion.jobs=[];s.deletion.barriers=[];s.deletion.retention=[];},'state_project_missing');
  rejected(s=>{s.deletion.retention[0]!.version=1;},'state_policy_regressed');
  rejected(s=>{const row=s.deletion.retention[0]!;row.raw_days=6;row.effective_from=retentionCutoff(row,Date.parse(later));},'state_policy_regressed');
});
it('rejects a cleared/backward floor and an earlier effective boundary',()=>{
  rejected(s=>{const p=s.deletion.retention[0]!;p.floor=null;p.effective_from=retentionCutoff(p,Date.parse(later));},'state_retention_boundary_regressed');
  rejected(s=>{const p=s.deletion.retention[0]!;p.floor='2026-10-23T00:00:00.000Z';p.effective_from=retentionCutoff(p,Date.parse(later));},'state_retention_boundary_regressed');
  rejected(s=>{const p=s.deletion.retention[0]!;p.raw_days=30;p.version++;p.effective_from=retentionCutoff(p,Date.parse(later));},'state_retention_boundary_regressed');
});
it('rejects missing barriers, earlier boundaries/expiry and privacy epoch rollback',()=>{
  rejected(s=>{s.deletion.projects=[];s.deletion.jobs=[];s.deletion.barriers=[];},'state_privacy_epoch_regressed');
  // Keep another valid barrier in the project so disappearance reaches the
  // barrier comparison rather than violating the ledger project inventory.
  const before=fixture(),next=advance(before),userJob='00000000-0000-4000-8000-000000000006';
  next.deletion.jobs=[{...next.deletion.jobs[0]!,id:userJob,scope:'user',subject_hash:'a'.repeat(64)}];next.deletion.barriers=[{...next.deletion.barriers[0]!,identity_hash:'a'.repeat(64),deletion_job_id:userJob}];
  expect(()=>assertRecoveryStateContinuity(before,next)).toThrow('state_barrier_missing');
  rejected(s=>{const b=s.deletion.barriers[0]!,j=s.deletion.jobs[0]!;b.requested_at='2026-10-31T00:00:00.000Z';b.blocked_before=b.requested_at;j.requested_at=b.requested_at;j.blocked_before=b.blocked_before;},'state_barrier_regressed');
  rejected(s=>{s.deletion.jobs[0]!.stream_expires_at='2026-12-31T00:00:00.000Z';},'state_barrier_regressed');
  rejected(s=>{s.deletion.projects[0]!.privacy_epoch=1;},'state_privacy_epoch_regressed');
});
it('rejects missing/reinitialized streams and offset regression beyond JS integer precision',()=>{
  rejected(s=>{s.integrity.streams=[];},'state_stream_regressed');
  rejected(s=>{s.integrity.streams[0]!.initialized_at=later;},'state_stream_reset');
  rejected(s=>{s.integrity.streams[0]!.positions[0]!.next_offset='9007199254740992';},'state_offset_regressed');
  const before=fixture();before.integrity.streams[0]!.partition_count=2;before.integrity.streams[0]!.positions.push({partition:1,next_offset:'4',updated_at:at});
  const next=advance(before);next.integrity.streams[0]!.partition_count=1;next.integrity.streams[0]!.positions.pop();
  expect(()=>assertRecoveryStateContinuity(before,next)).toThrow('state_stream_regressed');
});
it('rejects malformed descriptions instead of comparing partially validated input',()=>{
  rejected(s=>{s.integrity.streams[0]!.positions=[];},'state_continuity_invalid');
  rejected(s=>{(s as any).latest_state_verified=true;},'state_continuity_invalid');
});
