import {expect,it} from 'vitest';
import {parseRetentionUpdate,planRetentionChange,retentionCutoff,RetentionPolicyError} from '../packages/server/src/retention-policy';
const day=86400000,now=Date.parse('2026-10-23T00:00:00.000Z');
it('shortens immediately and does not restore logically expired data when later extended',()=>{
  const first=planRetentionChange({raw_days:30,version:1,floor:null},{raw_days:7,expected_version:1},now);
  expect(first.next.floor).toBe('2026-10-16T00:00:00.000Z');expect(first.invalidate_query_results).toBe(true);
  const later=now+5*day,extended=planRetentionChange(first.next,{raw_days:30,expected_version:2},later);
  expect(extended.next.floor).toBe('2026-10-21T00:00:00.000Z');expect(extended.next_cutoff).toBe(retentionCutoff(first.next,later));
  expect(retentionCutoff(extended.next,later+40*day)).toBe(new Date(later+10*day).toISOString());
});
it('preserves no-op versions, caller values, and conservative floors after clock rollback',()=>{
  const current={raw_days:7,version:9,floor:new Date(now+day).toISOString()};const value=planRetentionChange(current,{raw_days:7,expected_version:9},now);expect(value.next).toEqual(current);expect(value.changed).toBe(false);expect(value.invalidate_query_results).toBe(false);value.next.raw_days=1;expect(current.raw_days).toBe(7);
  const changed=planRetentionChange(current,{raw_days:30,expected_version:9},now);expect(changed.next_cutoff).toBe(current.floor);
});
it('never moves the effective cutoff backwards across all bounded old/new policies and forward times',()=>{
  for(let before=1;before<=30;before++)for(let after=1;after<=30;after++){
    const policy={raw_days:before,version:1,floor:new Date(now-20*day).toISOString()},plan=planRetentionChange(policy,{raw_days:after,expected_version:1},now);
    let prior=Date.parse(retentionCutoff(policy,now));expect(Date.parse(plan.next_cutoff)).toBeGreaterThanOrEqual(prior);
    for(const elapsed of [0,1,7,30,60,90]){const next=Date.parse(retentionCutoff(plan.next,now+elapsed*day));expect(next).toBeGreaterThanOrEqual(prior);prior=next;}
  }
});
it('rejects scope/safety overrides, invalid boundaries and stale or exhausted revisions',()=>{
  for(const body of [{raw_days:0,expected_version:1},{raw_days:31,expected_version:1},{raw_days:7.5,expected_version:1},{raw_days:'7',expected_version:1},{raw_days:7,expected_version:0},{raw_days:7,expected_version:1,dedup_days:1},{raw_days:7,expected_version:1,tenant_id:'other'}])expect(()=>parseRetentionUpdate(body)).toThrow('invalid_retention_policy');
  try{planRetentionChange({raw_days:30,version:2,floor:null},{raw_days:7,expected_version:1},now);throw new Error('expected conflict');}catch(error){expect(error).toBeInstanceOf(RetentionPolicyError);expect((error as RetentionPolicyError).status).toBe(409);}
  expect(()=>planRetentionChange({raw_days:30,version:2147483647,floor:null},{raw_days:7,expected_version:2147483647},now)).toThrow('retention_policy_version_exhausted');
  expect(()=>retentionCutoff({raw_days:7,version:1,floor:'2026-02-30T00:00:00.000Z'},now)).toThrow('invalid_retention_floor');
});
