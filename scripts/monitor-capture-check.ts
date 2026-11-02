// Actual CI PG snapshots, cap pruning, precise timestamps and cleanup. Never
// accepts a production source and never uses a synthetic capture/SQL adapter.
import assert from 'node:assert/strict';
import {spawn,spawnSync} from 'node:child_process';
import {randomUUID,createHash} from 'node:crypto';
import {mkdtemp,readFile,readdir,lstat,unlink,rmdir,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,isAbsolute} from 'node:path';
import pg from 'pg';
import {captureMonitorPreservation} from '../packages/server/src/monitor-preservation-capture';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts} from '../packages/server/src/operational-alert-store';
import {storePressureObservation} from '../packages/server/src/pressure-history-store';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {evaluateAndStoreDlqAlerts} from '../packages/server/src/dlq-alert-store';

assert.equal(process.env.GITHUB_ACTIONS,'true');
const source=new URL(process.env.DATABASE_URL!);assert.equal(source.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(source.hostname));assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const observer=new pg.Client({connectionString:source.href,statement_timeout:5000,options:'-c timezone=UTC'}),writer=new pg.Client({connectionString:source.href,statement_timeout:10000,options:'-c timezone=UTC'}),blocker=new pg.Client({connectionString:source.href,statement_timeout:5000,options:'-c timezone=UTC'});
const nativeWriter=writer as unknown as pg.PoolClient;
const folder=await mkdtemp(join(tmpdir(),'ccpanel-monitor-capture-'));
const options={connectionString:source.href,sourceLabel:'ci-monitor-cap',expectedSchema:22,timeoutMs:60000};
const tables=['operational_samples','operational_alert_history','pressure_signal_history','dlq_alert_history'] as const;
let relationLocked=false,pending:Promise<unknown>|undefined;
const delay=(ms:number)=>new Promise<void>(done=>setTimeout(done,ms));
async function until(label:string,check:()=>Promise<boolean>,ms=5000){const end=performance.now()+ms;while(performance.now()<end){if(await check())return;await delay(20);}throw Error(label);}
async function barrier(){
  const pid=(await blocker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
  await until('actual_capture_relation_barrier_not_observed',async()=>!!(await observer.query("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='ccpanel_monitor_preservation_capture' AND wait_event_type='Lock' AND $1::int=ANY(pg_blocking_pids(pid)) AND query LIKE 'SELECT blocked FROM recovery_gate%'",[pid])).rowCount);
}
async function sessionsGone(label='capture_server_session_not_gone'){await until(label,async()=>(await observer.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=current_database() AND application_name='ccpanel_monitor_preservation_capture'")).rows[0].n===0,3000);}
async function leasesReleased(){for(const lock of [169202610,169202617]){assert.equal((await observer.query('SELECT pg_try_advisory_lock($1) AS held',[lock])).rows[0].held,true);await observer.query('SELECT pg_advisory_unlock($1)',[lock]);}}
async function fingerprint(){const values=[];for(const {tablename:table}of(await observer.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows){assert.match(table,/^[a-z_][a-z0-9_]*$/);values.push({table,...(await observer.query('SELECT count(*)::text AS n,coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)::text AS hash FROM '+table+' t')).rows[0]});}return values;}
async function snapshot(name:string,label=options.sourceLabel){const output=join(folder,name+'.jsonl'),before=await fingerprint(),result=await captureMonitorPreservation({...options,sourceLabel:label,output});assert.deepEqual(await fingerprint(),before);assert.equal(result.sha256,createHash('sha256').update(await readFile(output)).digest('hex'));return{output,result};}
async function parsed(file:string){const raw=await readFile(file),lines=raw.toString('utf8').trimEnd().split('\n'),values=lines.map(line=>JSON.parse(line)),footer=values.at(-1);assert.equal(footer.kind,'footer');assert.equal(footer.prefix_sha256,createHash('sha256').update(lines.slice(0,-1).join('\n')+'\n').digest('hex'));assert.equal(values[0].kind,'header');assert.equal(values.length-2,footer.rows);return{header:values[0],rows:values.slice(1,-1),footer};}
function compare(before:string,after:string,name:string){const report=join(folder,name+'.json'),result=spawnSync(process.env.PYTHON??'python3',['scripts/monitor-preservation.py','verify',before,after,report],{encoding:'utf8',timeout:30000,maxBuffer:1024*1024});assert.equal(result.status,0,'actual capture policy verifier failed');const value=JSON.parse(result.stdout);assert.equal(value.policy_required_originals_preserved,true);return value;}
async function cli(output:string){
  const executable=process.env.CCPANEL_MONITOR_CAPTURE_CLI!;assert.ok(isAbsolute(executable));
  return new Promise<{code:number|null;stdout:string;stderr:string}>((resolve,reject)=>{
    const child=spawn(process.execPath,[executable,options.sourceLabel,output],{env:{...process.env,CCPANEL_MONITOR_DATABASE_URL:source.href,CCPANEL_MONITOR_CAPTURE_TIMEOUT_MS:'60000'},stdio:['ignore','pipe','pipe']});let stdout='',stderr='';const watchdog=setTimeout(()=>child.kill('SIGKILL'),70000);
    child.stdout.on('data',data=>{stdout+=data;if(stdout.length>65536)child.kill('SIGKILL');});child.stderr.on('data',data=>{stderr+=data;if(stderr.length>65536)child.kill('SIGKILL');});child.once('error',reject);child.once('close',code=>{clearTimeout(watchdog);resolve({code,stdout,stderr});});
  });
}
try{
  await observer.connect();await writer.connect();await blocker.connect();
  assert.equal((await observer.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const metrics=new IntakeMetrics();await storeOperationalSnapshot(nativeWriter,metrics.snapshot());await evaluateAndStoreOperationalAlerts(nativeWriter);
  const pressure=()=>{const at=Date.now()-1000;return{instance_id:randomUUID(),started_at:new Date(at-1000).toISOString(),assessment:evaluatePressureAlerts(null,undefined,at)};};
  await storePressureObservation(nativeWriter,pressure());await evaluateAndStoreDlqAlerts(nativeWriter);
  await writer.query('DELETE FROM operational_samples');
  const ids=[randomUUID(),randomUUID()];
  await writer.query(`INSERT INTO operational_samples(source,instance_id,started_at,observed_at,payload_hash,snapshot)
    SELECT CASE n%3 WHEN 0 THEN 'api' WHEN 1 THEN 'host' ELSE 'worker' END,
    CASE WHEN n%6<3 THEN $1::uuid ELSE $2::uuid END,CURRENT_TIMESTAMP-interval '2 days',
    CURRENT_TIMESTAMP-interval '1 day'+((n/6)*interval '1 microsecond'),repeat('0',64),'{}'::jsonb FROM generate_series(1,50000) n`,ids);
  await writer.query('DELETE FROM operational_alert_history;DELETE FROM pressure_signal_history;DELETE FROM dlq_alert_history');
  await writer.query(`INSERT INTO operational_alert_history(id,rule,state,reason,evaluated_at,state_since,revision,signal)
    SELECT 9007199254740993::bigint+n,rule,state,reason,evaluated_at+n*interval '1 microsecond',state_since,100000+n,signal FROM operational_alert_states CROSS JOIN generate_series(1,5000) n WHERE rule='api_sampling'`);
  await writer.query(`INSERT INTO pressure_signal_history(id,rule,state_at_assessment,reason,instance_id,started_at,assessed_at,recorded_at,state_since,revision,signal_expires_at,policy_hash,payload_hash,evidence,change_kind)
    SELECT 9007199254740993::bigint+n,rule,state_at_assessment,reason,instance_id,started_at,assessed_at+n*interval '1 microsecond',recorded_at+interval '1 second',state_since,100000+n,signal_expires_at,policy_hash,payload_hash,evidence,'initial' FROM pressure_signal_states CROSS JOIN generate_series(1,5000) n WHERE rule='kafka_backlog'`);
  await writer.query(`INSERT INTO dlq_alert_history(id,rule,state_at_assessment,reason,assessed_at,state_since,revision,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,change_kind)
    SELECT 9007199254740993::bigint+n,rule,state_at_assessment,reason,assessed_at+n*interval '1 microsecond',state_since,100000+n,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,'initial' FROM dlq_alert_states CROSS JOIN generate_series(1,5000) n WHERE rule='dlq_pending'`);
  const independent=await observer.query(`SELECT source,instance_id::text AS id,((extract(epoch FROM observed_at)*1000000)::bigint)::text AS policy_us,row_to_json(t)::text AS body FROM operational_samples t ORDER BY observed_at,source,instance_id OFFSET 249 LIMIT 3`);
  assert.ok(independent.rows.some(row=>BigInt(row.policy_us)%1000n!==0n));
  await blocker.query('BEGIN');await blocker.query('LOCK TABLE recovery_gate IN ACCESS EXCLUSIVE MODE');relationLocked=true;
  const beforeFile=join(folder,'cap-before.jsonl');pending=captureMonitorPreservation({...options,output:beforeFile});void pending.catch(()=>{});await barrier();
  const sampleOne=await storeOperationalSnapshot(nativeWriter,metrics.snapshot());await delay(5);metrics.observe({status:500,events:0,durationMs:1});const sampleTwo=await storeOperationalSnapshot(nativeWriter,metrics.snapshot());
  assert.equal(sampleOne.capacity_deleted,1);assert.equal(sampleTwo.capacity_deleted,1);
  const alertChange=await evaluateAndStoreOperationalAlerts(nativeWriter);assert.ok(alertChange.transitions>0);
  const pressureChange=await storePressureObservation(nativeWriter,pressure());assert.equal(pressureChange.transitions,3);
  await writer.query("INSERT INTO dlq_records(id,reason,payload) VALUES(repeat('a',64),'synthetic_monitor_fixture','{}')");const dlqChange=await evaluateAndStoreDlqAlerts(nativeWriter);assert.ok(dlqChange.transitions>0);
  await blocker.query('ROLLBACK');relationLocked=false;const oldReceipt=await pending as Awaited<ReturnType<typeof captureMonitorPreservation>>;pending=undefined;
  assert.deepEqual(oldReceipt.counts,{operational_samples:50000,operational_alert_history:5000,pressure_signal_history:5000,dlq_alert_history:5000});
  const before=await parsed(beforeFile);assert.equal(before.rows.length,65000);
  for(const row of independent.rows){const captured=before.rows.find(item=>item.table==='operational_samples'&&item.pk[0]===row.source&&item.pk[1]===row.id&&item.policy_us===row.policy_us);assert.ok(captured);assert.equal(captured.row_sha256,createHash('sha256').update(row.body,'utf8').digest('hex'));}
  assert.ok(before.rows.filter(item=>item.table!=='operational_samples').every(item=>BigInt(item.pk[0])>9007199254740991n));
  const fresh=await snapshot('cap-after'),after=await parsed(fresh.output);assert.deepEqual(fresh.result.counts,oldReceipt.counts);assert.notEqual(fresh.result.sha256,oldReceipt.sha256);
  assert.equal(after.rows.filter(item=>item.table==='operational_samples'&&item.pk[1]===metrics.snapshot().instance_id).length,2);
  assert.equal(after.rows.filter(item=>item.table!=='operational_samples'&&BigInt(item.pk[0])<=9007199254740991n).length,alertChange.transitions+pressureChange.transitions+dlqChange.transitions);
  assert.deepEqual(before.header.database,after.header.database);assert.equal(before.header.postmaster_started_us,after.header.postmaster_started_us);compare(beforeFile,fresh.output,'cap-policy');
  const originalBytes=await readFile(fresh.output);await assert.rejects(captureMonitorPreservation({...options,output:fresh.output}),/monitor_capture_output_exists/);assert.deepEqual(await readFile(fresh.output),originalBytes);
  // A second, small scenario proves the real bounded age prune leaves an old
  // remainder; the endpoint verifier must not demand all expired rows disappear.
  for(const table of tables)await writer.query('DELETE FROM '+table);
  await writer.query("INSERT INTO operational_samples(source,instance_id,started_at,observed_at,payload_hash,snapshot) SELECT 'api',$1,CURRENT_TIMESTAMP-interval '9 days',CURRENT_TIMESTAMP-interval '8 days'+n*interval '1 microsecond',repeat('0',64),'{}' FROM generate_series(1,101) n",[randomUUID()]);
  const expiryBefore=await snapshot('expiry-before','ci-monitor-expiry');const prune=await storeOperationalSnapshot(nativeWriter,new IntakeMetrics().snapshot());assert.equal(prune.expired_deleted,100);
  const expiryAfter=await snapshot('expiry-after','ci-monitor-expiry');assert.equal(expiryAfter.result.counts.operational_samples,2);compare(expiryBefore.output,expiryAfter.output,'expiry-policy');
  const cliFile=join(folder,'actual-cli.jsonl'),cliBefore=await fingerprint(),actualCli=await cli(cliFile);assert.equal(actualCli.code,0);assert.equal(actualCli.stderr,'');const cliReceipt=JSON.parse(actualCli.stdout);assert.equal(cliReceipt.sha256,createHash('sha256').update(await readFile(cliFile)).digest('hex'));assert.equal(cliReceipt.sourceAuthenticated,false);assert.equal(cliReceipt.causalPruneVerified,false);assert.equal(cliReceipt.continuousAvailabilityVerified,false);assert.deepEqual(await fingerprint(),cliBefore);
  const unsafeUri=new URL(source);unsafeUri.searchParams.set('application_name','unsafe_override');const noFile=join(folder,'uri-rejected.jsonl');await assert.rejects(captureMonitorPreservation({...options,connectionString:unsafeUri.href,output:noFile}),/monitor_capture_connection_invalid/);await assert.rejects(lstat(noFile),{code:'ENOENT'});
  for(const lock of [169202610,169202617]){await blocker.query('SELECT pg_advisory_lock($1)',[lock]);try{await assert.rejects(captureMonitorPreservation({...options,output:join(folder,'locked-'+lock+'.jsonl')}),/monitor_capture_lock_busy/);}finally{await blocker.query('SELECT pg_advisory_unlock($1)',[lock]);}await sessionsGone();}
  await writer.query('INSERT INTO schema_migrations(version) VALUES(999)');try{await assert.rejects(captureMonitorPreservation({...options,output:join(folder,'schema-rejected.jsonl')}),/monitor_capture_schema_mismatch/);}finally{await writer.query('DELETE FROM schema_migrations WHERE version=999');}
  const unchanged=await fingerprint();await blocker.query('BEGIN');await blocker.query('LOCK TABLE recovery_gate IN ACCESS EXCLUSIVE MODE');relationLocked=true;
  await assert.rejects(captureMonitorPreservation({...options,output:join(folder,'timeout-partial.jsonl'),timeoutMs:200}),/monitor_capture_timeout/);await sessionsGone('capture_timeout_server_session_not_gone');await leasesReleased();await blocker.query('ROLLBACK');relationLocked=false;assert.deepEqual(await fingerprint(),unchanged);
  await blocker.query('BEGIN');await blocker.query('LOCK TABLE recovery_gate IN ACCESS EXCLUSIVE MODE');relationLocked=true;
  const controller=new AbortController();pending=captureMonitorPreservation({...options,output:join(folder,'abort-partial.jsonl'),signal:controller.signal});void pending.catch(()=>{});await barrier();const abortAt=performance.now();controller.abort();await assert.rejects(pending,/monitor_capture_aborted/);pending=undefined;await sessionsGone('capture_abort_server_session_not_gone');const abortBackendCleanupMs=Math.ceil(performance.now()-abortAt);await leasesReleased();await blocker.query('ROLLBACK');relationLocked=false;assert.deepEqual(await fingerprint(),unchanged);
  const partialReport=join(folder,'must-not-create.json'),partial=spawnSync(process.env.PYTHON??'python3',['scripts/monitor-preservation.py','verify',join(folder,'timeout-partial.jsonl'),cliFile,partialReport],{encoding:'utf8',timeout:5000});assert.equal(partial.status,1);await assert.rejects(lstat(partialReport),{code:'ENOENT'});
  await sessionsGone();await leasesReleased();
  assert.equal((await observer.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  console.log(JSON.stringify({passed:true,actual_pg_monitor_capture:true,same_read_only_rr_snapshot:true,actual_relation_barrier:true,concurrent_real_store_and_prune_committed:true,operational_cap:50000,history_caps:[5000,5000,5000],captured_old_rows:65000,microsecond_keyset_and_int64_ids_preserved:true,independent_node_full_row_hash_checked:true,actual_bounded_expiry_deleted:100,expired_remainder_retained:true,policy_required_originals_verified:true,actual_cli:true,uri_override_schema_locks_refused:true,actual_locked_query_timeout_and_abort:true,client_connection_check_interval_ms:100,abort_backend_cleanup_ms:abortBackendCleanupMs,partial_footer_rejected:true,server_cleanup_observed_separately:true,sessions_remaining:0,shared_leases_released:true,leases_released_while_relation_lock_held:true,capture_source_unchanged:true,source_authenticated:false,causal_prune_verified:false,continuous_availability_verified:false,full_capacity_verified:false}));
}finally{
  if(relationLocked)await blocker.query('ROLLBACK').catch(()=>{});await pending?.catch(()=>{});
  await blocker.end();await writer.end();await observer.end();
  assert.equal(dirname(folder),resolve(tmpdir()));for(const name of await readdir(folder)){const file=join(folder,name);assert.equal(dirname(file),folder);assert.ok((await lstat(file)).isFile());await unlink(file);}await rmdir(folder);
}
