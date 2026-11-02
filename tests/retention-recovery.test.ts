import {expect,it} from 'vitest';
import {parseDeletionLedger} from '../packages/server/src/deletion-ledger-format';
import {recoveredRetention,type RetentionLedgerEntry} from '../packages/server/src/retention-recovery';
const now=Date.UTC(2026,9,23),day=86400000,at=(days:number)=>new Date(now-days*day).toISOString();
const row=():RetentionLedgerEntry=>({tenant_id:crypto.randomUUID(),project_id:crypto.randomUUID(),raw_days:7,version:2,floor:at(14),effective_from:at(7)});
const ledger=()=>({schema_version:2,exported_at:at(0),projects:[],jobs:[],barriers:[],retention:[row()]});
it('accepts policy-only projects and requires a canonical source cutoff',()=>{
  const value=ledger();expect(parseDeletionLedger(value,now).schema_version).toBe(2);
  for(const patch of[{raw_days:31},{version:0},{floor:at(6)},{effective_from:at(8)},{effective_from:new Date(now-7*day).toUTCString()},{secret:'forbidden'}]){
    expect(()=>parseDeletionLedger({...value,retention:[{...value.retention[0],...patch}]},now)).toThrow('invalid_deletion_ledger');
  }
  expect(()=>parseDeletionLedger({...value,retention:[value.retention[0],value.retention[0]]},now)).toThrow();
  expect(()=>parseDeletionLedger({...value,retention:Array(10001).fill(value.retention[0])},now)).toThrow();
  expect(()=>parseDeletionLedger(value,now+3600001)).toThrow('deletion_ledger_not_fresh');
});
it('restores the latest longer policy without advancing an obsolete short policy',()=>{
  const source={...row(),raw_days:30,version:3,floor:at(14),effective_from:at(14)};
  const result=recoveredRetention({raw_days:7,version:2,floor:at(30)},source,now);
  expect(result).toEqual({raw_days:30,version:3,floor:at(14)});
  expect(source.floor).toBe(at(14));
});
it('preserves stricter floors and advances only the latest rolling policy',()=>{
  expect(recoveredRetention({raw_days:30,version:1,floor:null},row(),now+300000).floor).toBe(new Date(now+300000-7*day).toISOString());
  expect(recoveredRetention({raw_days:7,version:2,floor:at(2)},row(),now).floor).toBe(at(2));
  const future={...row(),floor:at(-1),effective_from:at(-1)};
  expect(recoveredRetention({raw_days:30,version:1,floor:null},future,now).floor).toBe(at(-1));
});
it('refuses stale versions and equal-version policy conflicts',()=>{
  expect(()=>recoveredRetention({raw_days:7,version:3,floor:null},row(),now)).toThrow('retention_ledger_version_stale');
  expect(()=>recoveredRetention({raw_days:30,version:2,floor:null},row(),now)).toThrow('retention_ledger_policy_conflict');
});
