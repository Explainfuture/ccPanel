import {parseRecoverySourceState,type RecoverySourceState} from './recovery-state-format';

export class RecoveryStateContinuityError extends Error{
  constructor(public code:string){super(code);this.name='RecoveryStateContinuityError';}
}
const fail=(code:string):never=>{throw new RecoveryStateContinuityError(code);};
const scope=(tenant:string,project:string)=>tenant+':'+project;
const streamKey=(topic:string,group:string)=>topic+'\0'+group;

// This checks ordered, already authenticated descriptions against current PG
// writer invariants. It neither authenticates a source nor authorizes import,
// integrity-gate clearance or checkpoint advancement in a restored database.
export function assertRecoveryStateContinuity(previous:RecoverySourceState,next:RecoverySourceState):void{
  let before:RecoverySourceState,after:RecoverySourceState;
  try{
    // Prefix records may be historical. The chain verifier separately checks
    // actual-clock freshness of its independently anchored terminal record.
    before=parseRecoverySourceState(previous,{now:Date.parse(previous.exported_at)});
    after=parseRecoverySourceState(next,{now:Date.parse(next.exported_at)});
  }catch{fail('state_continuity_invalid');}
  if(before!.source.id!==after!.source.id||before!.source.database!==after!.source.database||before!.source.schema_version!==after!.source.schema_version)fail('state_source_changed');
  if(Date.parse(after!.exported_at)<Date.parse(before!.exported_at))fail('state_time_regressed');
  const oldAudit=BigInt(before!.integrity.audit_high_water),newAudit=BigInt(after!.integrity.audit_high_water);
  if(newAudit<oldAudit)fail('state_audit_regressed');
  if((before!.integrity.blocked!==after!.integrity.blocked||before!.integrity.reason!==after!.integrity.reason||before!.integrity.detected_at!==after!.integrity.detected_at)&&newAudit===oldAudit)fail('state_gate_audit_missing');

  const retention=new Map(after!.deletion.retention.map(row=>[scope(row.tenant_id,row.project_id),row]));
  for(const old of before!.deletion.retention){
    const current=retention.get(scope(old.tenant_id,old.project_id));if(!current)fail('state_project_missing');
    if(current!.version<old.version||(current!.version===old.version&&current!.raw_days!==old.raw_days))fail('state_policy_regressed');
    if((old.floor!==null&&(current!.floor===null||Date.parse(current!.floor)<Date.parse(old.floor)))||Date.parse(current!.effective_from)<Date.parse(old.effective_from))fail('state_retention_boundary_regressed');
  }
  const projects=new Map(after!.deletion.projects.map(row=>[scope(row.tenant_id,row.id),row]));
  for(const old of before!.deletion.projects){
    const current=projects.get(scope(old.tenant_id,old.id));
    if(!current||current.privacy_epoch<old.privacy_epoch)fail('state_privacy_epoch_regressed');
  }
  const barriers=new Map(after!.deletion.barriers.map(row=>[scope(row.tenant_id,row.project_id)+':'+row.identity_hash,row]));
  const oldJobs=new Map(before!.deletion.jobs.map(row=>[row.id,row])),newJobs=new Map(after!.deletion.jobs.map(row=>[row.id,row]));
  for(const old of before!.deletion.barriers){
    const current=barriers.get(scope(old.tenant_id,old.project_id)+':'+old.identity_hash);if(!current)fail('state_barrier_missing');
    const oldJob=oldJobs.get(old.deletion_job_id)!,newJob=newJobs.get(current!.deletion_job_id)!;
    // A repeated deletion legitimately replaces the barrier's job reference.
    // Its old unreferenced job need not remain in the exported current ledger.
    if(Date.parse(current!.requested_at)<Date.parse(old.requested_at)||Date.parse(current!.blocked_before)<Date.parse(old.blocked_before)||Date.parse(newJob.stream_expires_at)<Date.parse(oldJob.stream_expires_at))fail('state_barrier_regressed');
  }

  const streams=new Map(after!.integrity.streams.map(row=>[streamKey(row.topic,row.group_id),row]));
  for(const old of before!.integrity.streams){
    const current=streams.get(streamKey(old.topic,old.group_id));if(!current||current.partition_count<old.partition_count)fail('state_stream_regressed');
    if(current!.initialized_at!==old.initialized_at)fail('state_stream_reset');
    const positions=new Map(current!.positions.map(row=>[row.partition,row.next_offset]));
    for(const position of old.positions){
      const offset=positions.get(position.partition);
      if(offset===undefined||BigInt(offset)<BigInt(position.next_offset))fail('state_offset_regressed');
    }
  }
}
