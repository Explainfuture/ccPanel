// Pure transition contract used by the internal writer; no public write route yet.
const day=86400000;
export const retentionLimits={raw_min_days:1,raw_max_days:30,dedup_days:45,dlq_days:14,sdk_queue_days:7} as const;
export type RetentionPolicy={raw_days:number;version:number;floor:string|null};
export class RetentionPolicyError extends Error{constructor(readonly status:400|409,message:string){super(message);}}
export function parseRetentionUpdate(input:unknown){
  if(!input||typeof input!=='object'||Array.isArray(input))throw new RetentionPolicyError(400,'invalid_retention_policy');
  const value=input as Record<string,unknown>;
  if(Object.keys(value).length!==2||!Object.hasOwn(value,'raw_days')||!Object.hasOwn(value,'expected_version')||!Number.isInteger(value.raw_days)||Number(value.raw_days)<1||Number(value.raw_days)>30||!Number.isInteger(value.expected_version)||Number(value.expected_version)<1||Number(value.expected_version)>2147483647)throw new RetentionPolicyError(400,'invalid_retention_policy');
  return{raw_days:value.raw_days as number,expected_version:value.expected_version as number};
}
function validate(policy:RetentionPolicy,now:number){
  if(!Number.isSafeInteger(now)||!Number.isFinite(new Date(now).getTime())||!Number.isFinite(new Date(now-30*day).getTime()))throw new RetentionPolicyError(400,'invalid_retention_clock');
  parseRetentionUpdate({raw_days:policy.raw_days,expected_version:policy.version});
  if(policy.floor!==null&&(typeof policy.floor!=='string'||!Number.isFinite(Date.parse(policy.floor))||new Date(policy.floor).toISOString()!==policy.floor))throw new RetentionPolicyError(400,'invalid_retention_floor');
}
export function retentionCutoff(policy:RetentionPolicy,now:number){
  validate(policy,now);
  return new Date(Math.max(policy.floor===null?-Infinity:Date.parse(policy.floor),now-policy.raw_days*day)).toISOString();
}
export function planRetentionChange(current:RetentionPolicy,input:unknown,now:number){
  validate(current,now);const update=parseRetentionUpdate(input);
  if(update.expected_version!==current.version)throw new RetentionPolicyError(409,'retention_policy_conflict');
  const oldCutoff=retentionCutoff(current,now),changed=update.raw_days!==current.raw_days;
  if(changed&&current.version===2147483647)throw new RetentionPolicyError(409,'retention_policy_version_exhausted');
  const next:RetentionPolicy=changed?{
    raw_days:update.raw_days,version:current.version+1,
    // Capture the OLD rolling cutoff even on an increase. Physical cleanup may
    // lag, so retaining only a previous floor could reveal already expired rows.
    floor:new Date(Math.max(Date.parse(oldCutoff),now-update.raw_days*day)).toISOString(),
  }:{raw_days:current.raw_days,version:current.version,floor:current.floor};
  return{changed,current_cutoff:oldCutoff,next_cutoff:retentionCutoff(next,now),next,invalidate_query_results:changed};
}
