// Actual CI PostgreSQL source snapshot, not a restored or production database.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID,generateKeyPairSync,createHash} from 'node:crypto';
import {isAbsolute} from 'node:path';
import pg from 'pg';
import {exportRecoverySourceState} from '../packages/server/src/recovery-state-export';
import {readBackupFingerprints} from '../packages/server/src/backup-snapshot';
import {recoveryLock} from '../packages/server/src/recovery-gate';
import {signRecoveryStateRecord,verifyRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import {verifyRecoveryStateChain} from '../packages/server/src/recovery-state-chain';
import {parseRecoverySourceState} from '../packages/server/src/recovery-state-format';
import {RecoveryStateContinuityError} from '../packages/server/src/recovery-state-continuity';
import {retentionCutoff} from '../packages/server/src/retention-policy';

assert.equal(process.env.GITHUB_ACTIONS,'true');
const source=new URL(process.env.DATABASE_URL!);
assert.equal(source.protocol,'postgresql:');assert.equal(source.username,'ccpanel_ci');
assert.ok(['127.0.0.1','localhost'].includes(source.hostname));
assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const connectionString=source.href,sourceDatabase=decodeURIComponent(source.pathname.slice(1));
const options={connectionString,sourceId:'ci-source',expectedSchema:22,timeoutMs:15000,maxAgeSeconds:600};
const observer=new pg.Client({connectionString,statement_timeout:5000,options:'-c timezone=UTC'});
const controller=new pg.Client({connectionString,statement_timeout:5000,options:'-c timezone=UTC'});
const writer=new pg.Client({connectionString,statement_timeout:2000,options:'-c timezone=UTC'});
let tableLocked=false,recoveryLocked=false,pending:ReturnType<typeof exportRecoverySourceState>|undefined;
const delay=(ms:number)=>new Promise<void>(done=>setTimeout(done,ms));

async function waitForExportBarrier(controllerPid:number){
  const deadline=performance.now()+7000;
  while(performance.now()<deadline){
    const rows=(await observer.query(`SELECT pid,query FROM pg_stat_activity
      WHERE datname=$1 AND application_name='ccpanel_backup_state_export'
        AND wait_event_type='Lock' AND $2::int=ANY(pg_blocking_pids(pid))`,[sourceDatabase,controllerPid])).rows;
    if(rows.length){assert.equal(rows.length,1);assert.match(rows[0].query,/consumer_streams/);return;}
    await delay(20);
  }
  throw Error('state_snapshot_relation_barrier_not_observed');
}
async function cli(){
  const path=process.env.CCPANEL_STATE_EXPORT_CLI!;assert.ok(isAbsolute(path));
  return new Promise<{code:number|null;stdout:string}>((resolve,reject)=>{
    const child=spawn(process.execPath,[path,'ci-source','22'],{env:{...process.env,CCPANEL_BACKUP_DATABASE_URL:connectionString},stdio:['ignore','pipe','pipe']});
    const chunks:Buffer[]=[];let bytes=0,failed=false;
    child.stdout.on('data',(chunk:Buffer)=>{bytes+=chunk.length;if(bytes>16*1024*1024){failed=true;child.kill('SIGKILL');}else chunks.push(chunk);});
    child.stderr.resume();
    const timer=setTimeout(()=>{failed=true;child.kill('SIGKILL');},30000);
    child.once('error',()=>{clearTimeout(timer);reject(Error('state_export_cli_spawn_failed'));});
    child.once('close',code=>{clearTimeout(timer);if(failed)reject(Error('state_export_cli_fixture_failed'));else resolve({code,stdout:Buffer.concat(chunks).toString('utf8')});});
  });
}

try{
  await observer.connect();await controller.connect();await writer.connect();
  assert.equal((await observer.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  assert.equal((await observer.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false);
  assert.equal((await observer.query('SELECT count(*)::int AS n FROM consumer_progress_audit')).rows[0].n,0);
  const tenant=randomUUID(),foreign=randomUUID(),project=randomUUID(),other=randomUUID(),job=randomUUID();
  for(const id of [tenant,foreign])await writer.query("INSERT INTO tenants(id,name) VALUES($1,'STATE SOURCE FIXTURE')",[id]);
  await writer.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'STATE SOURCE FIXTURE'),($3,$4,'STATE SOURCE FIXTURE')",[project,tenant,other,foreign]);
  await writer.query("INSERT INTO consumer_streams(topic,group_id,partition_count) VALUES('state_fixture','state-fixture',1)");
  await writer.query("INSERT INTO consumer_progress(topic,group_id,partition_id,next_offset) VALUES('state_fixture','state-fixture',0,9007199254740993)");
  await controller.query('BEGIN');await controller.query('LOCK TABLE consumer_streams IN ACCESS EXCLUSIVE MODE');tableLocked=true;
  const controllerPid=(await controller.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  pending=exportRecoverySourceState(options);void pending.catch(()=>{});
  await waitForExportBarrier(controllerPid);
  await writer.query('BEGIN');
  try{
    await writer.query('UPDATE projects SET retention_days=7,retention_version=2,privacy_epoch=privacy_epoch+1 WHERE id=$1',[project]);
    await writer.query("INSERT INTO deletion_jobs(id,tenant_id,project_id,scope) VALUES($1,$2,$3,'project')",[job,tenant,project]);
    await writer.query("INSERT INTO deletion_barriers(tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before) SELECT tenant_id,project_id,'*',id,requested_at,blocked_before FROM deletion_jobs WHERE id=$1",[job]);
    await writer.query("UPDATE ingest_integrity_gate SET blocked=true,reason='retained_offset_gap',detected_at=clock_timestamp(),details='{\"kind\":\"state_snapshot_fixture\"}'::jsonb WHERE id=true");
    await writer.query("INSERT INTO consumer_progress_audit(topic,group_id,action,reason,details) VALUES('state_fixture','state-fixture','paused','retained_offset_gap','{\"kind\":\"state_snapshot_fixture\"}')");
    // Updating only the offset leaves the FK key unchanged; stmt timeout bounds
    // any unexpected lock dependency rather than hanging behind our barrier.
    await writer.query("UPDATE consumer_progress SET next_offset=9007199254740994,updated_at=clock_timestamp() WHERE topic='state_fixture' AND group_id='state-fixture' AND partition_id=0");
    await writer.query('COMMIT');
  }catch(error){await writer.query('ROLLBACK');throw error;}
  await controller.query('ROLLBACK');tableLocked=false;
  const old=await pending;pending=undefined;
  assert.equal(old.source.id,'ci-source');assert.equal(old.source.database,sourceDatabase);assert.equal(old.source.schema_version,22);assert.equal(old.source.authenticated,false);
  assert.equal(old.status,'exported_unverified');assert.equal(old.latest_state_verified,false);assert.equal(old.kafka_observation_verified,false);
  assert.equal(old.deletion.schema_version,2);assert.ok('retention' in old.deletion);
  assert.equal(old.deletion.retention.length,2);assert.equal(old.deletion.projects.length,0);assert.equal(old.deletion.jobs.length,0);assert.equal(old.deletion.barriers.length,0);
  assert.ok(old.deletion.retention.every(row=>row.raw_days===30&&row.version===1));
  assert.equal(old.deletion.exported_at,old.exported_at);assert.equal(old.integrity.blocked,false);assert.equal(old.integrity.reason,null);assert.equal(old.integrity.audit_high_water,'0');
  assert.equal(old.integrity.streams.length,1);assert.equal(old.integrity.streams[0].positions[0].next_offset,'9007199254740993');
  const before=await readBackupFingerprints(writer),gateBefore=(await writer.query('SELECT blocked,updated_at::text FROM recovery_gate WHERE id=true')).rows;
  const fresh=await exportRecoverySourceState(options);
  assert.deepEqual(await readBackupFingerprints(writer),before,'state exporter must not mutate source rows');
  assert.deepEqual((await writer.query('SELECT blocked,updated_at::text FROM recovery_gate WHERE id=true')).rows,gateBefore);
  assert.equal(fresh.deletion.schema_version,2);assert.ok('retention' in fresh.deletion);
  assert.equal(fresh.deletion.retention.length,2);assert.equal(fresh.deletion.projects.length,1);assert.equal(fresh.deletion.jobs.length,1);assert.equal(fresh.deletion.barriers.length,1);
  assert.equal(fresh.deletion.retention.find(row=>row.project_id===project)?.raw_days,7);assert.equal(fresh.deletion.retention.find(row=>row.project_id===project)?.version,2);
  assert.equal(fresh.deletion.retention.find(row=>row.project_id===other)?.raw_days,30);
  assert.equal(fresh.integrity.blocked,true);assert.equal(fresh.integrity.reason,'retained_offset_gap');assert.equal(fresh.integrity.details.kind,'state_snapshot_fixture');
  assert.ok(BigInt(fresh.integrity.audit_high_water)>0n);assert.equal(fresh.integrity.streams[0].positions[0].next_offset,'9007199254740994');
  assert.equal(fresh.deletion.exported_at,fresh.exported_at);assert.equal(fresh.latest_state_verified,false);assert.equal(fresh.source.authenticated,false);
  // Bind the actual old/fresh PG exports to an ephemeral signer and an anchor
  // held outside the records. This association hash is a fixture label, NOT an
  // actual backup-manifest proof; the archive roundtrip is a different suite.
  const signer=generateKeyPairSync('ed25519'),signingNow=Math.max(Date.now(),Date.parse(fresh.exported_at)+1);
  const backup={id:'pg-state-binding-fixture',manifest_sha256:createHash('sha256').update('PG STATE SIGNATURE FIXTURE ONLY').digest('hex')};
  const signedOld=signRecoveryStateRecord({state:old,backup,previous:null,privateKey:signer.privateKey,now:signingNow});
  const signedFresh=signRecoveryStateRecord({state:fresh,backup,previous:signedOld.anchor,privateKey:signer.privateKey,now:signingNow});
  const externallyHeldTip=structuredClone(signedFresh.anchor);
  const chainOptions={publicKey:signer.publicKey,expectedSourceId:'ci-source',trustedTip:externallyHeldTip,now:signingNow};
  const verified=verifyRecoveryStateChain({...chainOptions,records:[signedOld.record,signedFresh.record]});
  assert.equal(verified.recordsVerified,2);assert.equal(verified.signaturesVerified,true);assert.equal(verified.anchorMatched,true);assert.equal(verified.semanticContinuityChecked,true);
  assert.equal(verified.state.deletion.barriers.length,1);assert.equal(verified.state.integrity.blocked,true);assert.equal(verified.state.integrity.streams[0].positions[0].next_offset,'9007199254740994');
  assert.equal(verified.sourceEndpointVerified,false);assert.equal(verified.anchorCustodyVerified,false);assert.equal(verified.latestStateVerified,false);assert.equal(verified.state.latest_state_verified,false);
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedOld.record]}));
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedFresh.record,signedOld.record]}));
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedFresh.record]}));
  const tamperedPayload=Buffer.from(signedFresh.record.payload,'base64');tamperedPayload[tamperedPayload.length-1]^=1;
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedOld.record,{...signedFresh.record,payload:tamperedPayload.toString('base64')}]}));
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedOld.record,signedFresh.record],publicKey:generateKeyPairSync('ed25519').publicKey}));
  // A newly signed and correctly chained record can still regress source data.
  // Keep it structurally valid/fresh so the semantic continuity check must act.
  const regressed=structuredClone(old),regressedAt=Date.parse(fresh.exported_at)+1;
  regressed.exported_at=new Date(regressedAt).toISOString();regressed.deletion.exported_at=regressed.exported_at;
  regressed.deletion.retention=regressed.deletion.retention.map(row=>({...row,effective_from:retentionCutoff(row,regressedAt)}));
  parseRecoverySourceState(regressed,{now:signingNow});
  const signedRegression=signRecoveryStateRecord({state:regressed,backup,previous:signedFresh.anchor,privateKey:signer.privateKey,now:signingNow});
  assert.equal(verifyRecoveryStateRecord({record:signedRegression.record,publicKey:signer.publicKey,expectedSourceId:'ci-source',now:signingNow}).signatureVerified,true);
  assert.throws(()=>verifyRecoveryStateChain({...chainOptions,records:[signedOld.record,signedFresh.record,signedRegression.record],trustedTip:signedRegression.anchor}),error=>error instanceof RecoveryStateContinuityError&&error.code!=='state_continuity_invalid');
  assert.deepEqual(await readBackupFingerprints(writer),before,'signing/verification must not mutate source rows');
  assert.deepEqual((await writer.query('SELECT blocked,updated_at::text FROM recovery_gate WHERE id=true')).rows,gateBefore,'chain verification must not open or change the recovery gate');
  let actualCli=false;
  if(process.env.CCPANEL_STATE_EXPORT_CLI){
    const result=await cli();assert.equal(result.code,2,'exported state does not become trusted latest state');
    const exported=JSON.parse(result.stdout);assert.equal(exported.status,'exported_unverified');assert.equal(exported.source.authenticated,false);assert.equal(exported.latest_state_verified,false);
    assert.equal(exported.integrity.blocked,true);assert.equal(exported.integrity.streams[0].positions[0].next_offset,'9007199254740994');assert.equal(exported.deletion.retention.length,2);
    assert.ok(!result.stdout.includes(source.password)&&!result.stdout.includes(connectionString));actualCli=true;
  }
  await writer.query('UPDATE recovery_gate SET blocked=true WHERE id=true');
  await assert.rejects(exportRecoverySourceState(options),/state_export_source_not_ready/);
  await writer.query('UPDATE recovery_gate SET blocked=false WHERE id=true');
  await controller.query('SELECT pg_advisory_lock($1)',[recoveryLock]);recoveryLocked=true;
  await assert.rejects(exportRecoverySourceState(options),/state_export_lock_busy/);
  await controller.query('SELECT pg_advisory_unlock($1)',[recoveryLock]);recoveryLocked=false;
  await assert.rejects(exportRecoverySourceState({...options,expectedSchema:999}),/state_export_schema_mismatch/);
  const beforeTimeout=await readBackupFingerprints(writer);
  await controller.query('BEGIN');await controller.query('LOCK TABLE consumer_streams IN ACCESS EXCLUSIVE MODE');tableLocked=true;
  pending=exportRecoverySourceState({...options,timeoutMs:3000});void pending.catch(()=>{});
  await waitForExportBarrier(controllerPid);
  // Keep the table lock held after the local deadline result. Closing the
  // local socket is not evidence that PG has already observed the disconnect;
  // releasing this lock would incorrectly cause the server-side cleanup.
  await assert.rejects(pending,/state_export_timeout/);pending=undefined;
  const cleanupStarted=performance.now(),cleanupDeadline=cleanupStarted+2000;
  while(true){
    // Observer uses autocommit, so each query obtains a fresh statistics view.
    const count=(await observer.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=$1 AND application_name='ccpanel_backup_state_export'",[sourceDatabase])).rows[0].n;
    if(count===0)break;
    assert.ok(performance.now()<cleanupDeadline,'state_export_backend_cleanup_not_observed');
    await delay(20);
  }
  const backendCleanupAfterLocalReturnMs=Math.round(performance.now()-cleanupStarted);
  await controller.query('ROLLBACK');tableLocked=false;
  assert.deepEqual(await readBackupFingerprints(writer),beforeTimeout);
  for(const key of [169202610,recoveryLock]){
    assert.equal((await controller.query('SELECT pg_try_advisory_lock($1) AS locked',[key])).rows[0].locked,true);
    await controller.query('SELECT pg_advisory_unlock($1)',[key]);
  }
  assert.equal((await observer.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=$1 AND application_name='ccpanel_backup_state_export'",[sourceDatabase])).rows[0].n,0);
  console.log(JSON.stringify({passed:true,actual_pg_source_snapshot:true,relation_lock_barrier_observed:true,concurrent_policy_deletion_incident_progress_committed:true,first_export_consistent_old_snapshot:true,fresh_export_contains_complete_v2_and_integrity:true,offset_beyond_js_safe_integer_preserved:true,source_rows_unchanged_by_export:true,actual_pg_states_signed:true,externally_supplied_tip_matched:true,replay_reorder_missing_genesis_rejected:true,tampered_record_and_wrong_signer_rejected:true,correct_signature_with_regressed_state_rejected:true,chain_does_not_change_source_or_recovery_gate:true,ephemeral_signer_key_in_memory_only:true,backup_manifest_binding_is_fixture:true,source_endpoint_verified:false,anchor_custody_verified:false,closed_source_gate_rejected:true,offline_recovery_lock_rejected:true,schema_mismatch_rejected:true,deadline_interrupted_actual_locked_query:true,local_return_does_not_imply_backend_gone:true,server_cleanup_observed_separately:true,backend_cleanup_after_local_return_ms:backendCleanupAfterLocalReturnMs,deadline_source_rows_unchanged:true,leases_released:true,exporter_sessions_remaining:0,actual_state_cli:actualCli,source_authenticated:false,latest_state_verified:false,project_policies:2,deletion_barriers:1,streams:1}));
}finally{
  if(tableLocked)await controller.query('ROLLBACK');
  if(pending)await pending.catch(()=>{});
  if(recoveryLocked)await controller.query('SELECT pg_advisory_unlock($1)',[recoveryLock]);
  await Promise.allSettled([observer.end(),controller.end(),writer.end()]);
}
