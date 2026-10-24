import {retentionCutoff,type RetentionPolicy} from './retention-policy';
export type RetentionLedgerEntry=RetentionPolicy&{tenant_id:string;project_id:string;effective_from:string};
// The backup's shorter policy may have been replaced before recovery. Never
// advance that obsolete rolling cutoff to now; use the latest source policy.
export function recoveredRetention(current:RetentionPolicy,source:RetentionLedgerEntry,now:number){
  retentionCutoff(current,now);retentionCutoff(source,now);
  if(source.version<current.version)throw new Error('retention_ledger_version_stale');
  if(source.version===current.version&&source.raw_days!==current.raw_days)throw new Error('retention_ledger_policy_conflict');
  const effective=Date.parse(source.effective_from);
  if(!Number.isFinite(effective))throw new Error('invalid_retention_recovery_boundary');
  return{raw_days:source.raw_days,version:source.version,floor:new Date(Math.max(effective,Date.parse(retentionCutoff(source,now)),current.floor===null?-Infinity:Date.parse(current.floor))).toISOString()};
}
