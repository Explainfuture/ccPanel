// Actual isolated CI PostgreSQL and Kafka. Controller/client documents are
// synthetic fixtures, explicitly not a native client or a capacity acceptance.
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,readdir,copyFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename,isAbsolute} from 'node:path';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
import {runCapacityWindowDb,type WindowDbConfig,type BrokerOffsetDiagnostic,inspectWindowFixture,WindowDbError,brokerOffsetDiagnostic} from './capacity-window-db';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {withConsumerProgress} from '../packages/server/src/consumer-progress';
import {aggregateNext} from '../packages/server/src/aggregations';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {WorkerMetrics} from '../packages/server/src/worker-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts} from '../packages/server/src/operational-alert-store';
import {evaluateAndStoreDlqAlerts} from '../packages/server/src/dlq-alert-store';
import {storePressureObservation} from '../packages/server/src/pressure-history-store';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {defaultPressurePolicy} from '../packages/server/src/ingest-pressure';

// Exercise the public logger with native KafkaJS errors without connecting.
// These only validate redacted diagnostics, not actual broker readiness.
{
  const {Kafka,logLevel,KafkaJSProtocolError,KafkaJSNumberOfRetriesExceeded,KafkaJSGroupCoordinatorNotFound}=kafkaPackage;
  const protocol=new KafkaJSProtocolError(Object.assign(Error('private-diagnostic-text'),{code:15,type:'COORDINATOR_NOT_AVAILABLE',retriable:true}));
  const wrapped=new KafkaJSNumberOfRetriesExceeded(protocol,{retryCount:0,retryTime:1});
  let observed:unknown;
  const logger=new Kafka({clientId:'diagnostic-only',brokers:['127.0.0.1:1'],logLevel:logLevel.DEBUG,logCreator:()=>({log})=>{observed=log.error;}}).logger();
  logger.debug('diagnostic-only',{error:wrapped});assert.equal(observed,wrapped);
  assert.deepEqual(brokerOffsetDiagnostic(observed),{phase:'broker_offsets',protocol_code:15,protocol_type:'coordinator_unavailable',error_class:'retries_exhausted'});
  // KafkaJS 2.2.4's declaration omits the runtime constructor's message arg.
  assert.deepEqual(brokerOffsetDiagnostic(Reflect.construct(KafkaJSGroupCoordinatorNotFound,['private-diagnostic-text'])),{phase:'broker_offsets',protocol_code:null,protocol_type:'unclassified',error_class:'coordinator_not_found'});
  assert.equal(brokerOffsetDiagnostic(Error('private-diagnostic-text')).error_class,'other');
  assert.equal(brokerOffsetDiagnostic({code:'15'}).protocol_code,null);
  assert.equal(JSON.stringify(brokerOffsetDiagnostic(observed)).includes('private-diagnostic-text'),false);
}

assert.equal(process.env.GITHUB_ACTIONS,'true');
const url=new URL(process.env.DATABASE_URL!);assert.equal(url.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(url.hostname));assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const brokers=process.env.CCPANEL_WINDOW_KAFKA_BROKERS!.split(',');assert.ok(brokers.length>0&&brokers.every(value=>/^(127\.0\.0\.1|localhost):[0-9]{1,5}$/.test(value)));
const executable=process.env.CCPANEL_WINDOW_DB_CLI!;assert.ok(isAbsolute(executable));
const invalidCli=spawnSync(process.execPath,[executable],{encoding:'utf8',timeout:10000,maxBuffer:65536});assert.equal(invalidCli.status,1);assert.equal(invalidCli.stdout,'');assert.deepEqual(JSON.parse(invalidCli.stderr),{code:'window_db_command_invalid'});
const hash=(raw:Uint8Array|string)=>createHash('sha256').update(raw).digest('hex'),json=async(path:string,value:unknown)=>writeFile(path,JSON.stringify(value)+'\n',{mode:0o600,flag:'wx'});
const root=await mkdtemp(join(tmpdir(),'ccpanel-window225-')),state=join(root,'evidence'),product=join(root,'product'),tools=join(root,'client');await mkdir(state,{mode:0o700});await mkdir(tools,{mode:0o700});
const nonce=randomBytes(8).toString('hex'),namespace={events:`ccpanel.drill.window225.${nonce}.events`,dlq:`ccpanel.drill.window225.${nonce}.dlq`,group:`ccpanel.drill.window225.${nonce}.writer`};
let observeWarmup=true,warmupErrors:BrokerOffsetDiagnostic[]=[];
const {Kafka,logLevel}=kafkaPackage,kafka=new Kafka({clientId:'ccpanel-window225-ci',brokers,logLevel:logLevel.DEBUG,logCreator:()=>({log})=>{if(observeWarmup&&warmupErrors.length<16){const diagnostic=brokerOffsetDiagnostic(log.error);if(diagnostic.protocol_code!==null)warmupErrors.push(diagnostic);}},retry:{retries:0},connectionTimeout:3000,requestTimeout:5000}),admin=kafka.admin(),producer=kafka.producer({idempotent:true,maxInFlightRequests:1,retry:{retries:3}}),consumer=kafka.consumer({groupId:namespace.group,sessionTimeout:10000});
const db=new pg.Client({connectionString:url.href,statement_timeout:10000,options:'-c timezone=UTC'}),blocker=new pg.Client({connectionString:url.href,statement_timeout:10000,options:'-c timezone=UTC'});
let topics=false,producerConnected=false,consumerConnected=false,consumerFailure=false,fixture:any;
const configurationSha256=hash('isolated-synthetic-source-configuration'),config:WindowDbConfig={connectionString:url.href,brokers,namespace,origin:'https://ci.ccpanel.invalid',expectedSchema:22,durability:{replicationFactor:1,minInSyncReplicas:1},pressure:{...defaultPressurePolicy},productRoot:product,toolsDirectory:tools,operatorFile:executable,configurationSha256};
const intake=new IntakeMetrics(),worker=new WorkerMetrics();
async function monitoring(){
  const writer=await pool.connect();try{await storeOperationalSnapshot(writer,intake.snapshot());await storeOperationalSnapshot(writer,worker.snapshot());await evaluateAndStoreOperationalAlerts(writer);const now=Date.now()-1000;await storePressureObservation(writer,{instance_id:intake.snapshot().instance_id,started_at:intake.snapshot().started_at,assessment:evaluatePressureAlerts(null,undefined,Math.max(now,Date.parse(intake.snapshot().started_at)))});await evaluateAndStoreDlqAlerts(writer);}finally{writer.release();}
}
async function until(check:()=>Promise<boolean>,label:string,ms=70000){const end=Date.now()+ms;while(Date.now()<end){assert.equal(consumerFailure,false,'real consumer failed');if(await check())return;await new Promise(done=>setTimeout(done,50));}throw Error(label);}
async function run(command:Parameters<typeof runCapacityWindowDb>[0]['command'],directory=state,sequence?:string){await monitoring();return runCapacityWindowDb({command,directory,...(sequence===undefined?{}:{sequence})},config);}
async function clone(label:string){const path=join(root,label);await mkdir(path,{mode:0o700});for(const file of await readdir(state))if(file.endsWith('.json'))await copyFile(join(state,file),join(path,file));return path;}
async function change(path:string,name:string,mutate:(value:any)=>void){const value=JSON.parse(await readFile(join(path,name),'utf8'));mutate(value);await writeFile(join(path,name),JSON.stringify(value)+'\n',{mode:0o600});}
async function retained(){assert.equal((await db.query('SELECT count(*)::int AS n FROM projects WHERE tenant_id=$1 AND id=$2',[fixture.tenant,fixture.project])).rows[0].n,1);}

try{
  await db.connect();await blocker.connect();await admin.connect();assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  await admin.createTopics({waitForLeaders:true,topics:[{topic:namespace.events,numPartitions:2,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'},{name:'unclean.leader.election.enable',value:'false'}]},{topic:namespace.dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'},{name:'unclean.leader.election.enable',value:'false'}]}]});topics=true;
  // A fresh real broker lazily initializes __consumer_offsets on the first
  // FindCoordinator. Observe that readiness explicitly in CI setup; do not
  // initialize/advance offsets, publish data, or retry an operator command.
  const warmupUntil=Date.now()+30000;let warmupAttempts=0;
  while(true){
    warmupAttempts++;warmupErrors=[];
    let values;
    try{values=await admin.fetchOffsets({groupId:namespace.group,topics:[namespace.events],resolveOffsets:false});}
    catch(error){const diagnostic=brokerOffsetDiagnostic(error),observed=[...warmupErrors];
      // KafkaJS 2.2.4 withBroker discards a failed coordinator lookup's cause.
      // Its public logger still exposes the actual protocol Error. Never infer
      // code15 merely from the replacement GroupCoordinatorNotFound class.
      const effective=diagnostic.protocol_code!==null?diagnostic:observed.find(row=>![14,15,16].includes(row.protocol_code??-1))??observed[0]??diagnostic;
      if((diagnostic.protocol_code===null&&diagnostic.error_class!=='coordinator_not_found')||![14,15,16].includes(effective.protocol_code??-1)||observed.some(row=>![14,15,16].includes(row.protocol_code??-1))||Date.now()>=warmupUntil)throw Object.assign(Error('actual_broker_readonly_warmup_failed'),{diagnostic,observed_protocols:observed});
      console.log(JSON.stringify({kind:'capacity_window_broker_readonly_warmup',attempt:warmupAttempts,ready:false,diagnostic,observed_protocols:observed}));await new Promise(done=>setTimeout(done,200));continue;}
    assert.equal(values.length,1);assert.equal(values[0]!.partitions.length,2);assert.ok(values[0]!.partitions.every(p=>p.offset==='-1'));observeWarmup=false;kafka.logger().setLogLevel(logLevel.NOTHING);console.log(JSON.stringify({kind:'capacity_window_broker_readonly_warmup',attempt:warmupAttempts,ready:true}));break;
  }
  await db.query('INSERT INTO consumer_streams(topic,group_id,partition_count) VALUES($1,$2,2)',[namespace.events,namespace.group]);await db.query('INSERT INTO consumer_progress(topic,group_id,partition_id,next_offset) VALUES($1,$2,0,0),($1,$2,1,0)',[namespace.events,namespace.group]);
  const products:Record<string,string>={};for(const [path,key]of [['dist/api.mjs','api_sha256'],['dist/worker.mjs','worker_sha256'],['dist/web/sdk/ccpanel.js','sdk_sha256'],['infra/compose.yaml','compose_sha256']]){const target=join(product,path!);await mkdir(dirname(target),{recursive:true,mode:0o700});const raw='synthetic-native-test-asset:'+path;await writeFile(target,raw,{mode:0o600});products[key!]=hash(raw);}
  const artifactNames=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'],artifactHashes:Record<string,string>={};for(const file of artifactNames){await copyFile(join('scripts',file),join(tools,file));artifactHashes[file]=hash(await readFile(join(tools,file)));}
  const binding={version:1,kind:'capacity_window_binding',run_id:randomBytes(16).toString('hex'),planned_events:1800,product:{release:'m4-polling-014',source_commit:'b9b213f2313151c1e21be30e924a9af3a1f84a82',schema:22,...products},containers:Object.fromEntries(['api','worker','postgres','kafka'].map(name=>[name,{id:hash(name),started_at:new Date().toISOString(),restart_count:0,image_id:'sha256:'+hash('image:'+name)}])),tools:{db_operator_sha256:hash(await readFile(executable)),generator_sha256:artifactHashes[artifactNames[0]!],evidence_sha256:artifactHashes[artifactNames[1]!],lease_sha256:artifactHashes[artifactNames[2]!],reader_sha256:artifactHashes[artifactNames[3]!],sampler_sha256:hash('synthetic-sampler'),controller_sha256:hash('synthetic-controller')},namespace,configuration_sha256:configurationSha256};
  await json(join(state,'binding.json'),binding);
  assert.equal((await run('baseline'))?.status,'baseline_recorded');assert.equal((await run('status',state,'1'))?.status,'status_recorded');
  await assert.rejects(run('status',state,'1'),error=>error instanceof WindowDbError&&error.code==='window_db_status_sequence_invalid');
  assert.equal((await run('create'))?.status,'created');fixture=JSON.parse(await readFile(join(state,'fixture.json'),'utf8'));assert.equal(fixture.run_id,binding.run_id);assert.equal((await db.query('SELECT count(*)::int AS n FROM write_keys WHERE id=$1 AND revoked_at IS NULL',[fixture.write_key_id])).rows[0].n,1);
  const beforeRevokeGate=(await db.query('SELECT blocked,reason,detected_at::text,details FROM ingest_integrity_gate WHERE id=true')).rows[0];
  await db.query("UPDATE ingest_integrity_gate SET blocked=true,reason='consumer_offset_unknown' WHERE id=true");
  assert.equal((await runCapacityWindowDb({command:'revoke',directory:state},config))?.status,'revoked');
  await db.query('UPDATE ingest_integrity_gate SET blocked=$1,reason=$2,detected_at=$3::timestamptz,details=$4 WHERE id=true',[beforeRevokeGate.blocked,beforeRevokeGate.reason,beforeRevokeGate.detected_at,JSON.stringify(beforeRevokeGate.details)]);
  const revokeBytes=await readFile(join(state,'key-revoked.json'));assert.equal((await run('revoke'))?.status,'revoked');assert.deepEqual(await readFile(join(state,'key-revoked.json')),revokeBytes);

  const now=new Date().toISOString(),ids=Array.from({length:1800},()=>randomUUID()),events=ids.map((event_id,index)=>({tenant_id:fixture.tenant,project_id:fixture.project,received_at:now,event:{event_id,event_name:'capacity_public_probe',distinct_id:'probe-'+index%20,event_time:now,properties:{test:true,padding:'x'.repeat(32)},context:{source:'isolated-ci'},schema_version:1}}));
  const records=[...events,...events.filter((_,index)=>index>0&&index%50===0)];assert.equal(records.length,1835);
  await producer.connect();producerConnected=true;await consumer.connect();consumerConnected=true;await consumer.subscribe({topic:namespace.events,fromBeginning:true});consumer.on(consumer.events.CRASH,()=>{consumerFailure=true;});
  await consumer.run({autoCommit:false,eachBatchAutoResolve:false,partitionsConsumedConcurrently:2,eachBatch:async({batch,resolveOffset,heartbeat})=>{for(const message of batch.messages){await withConsumerProgress({topic:namespace.events,group:namespace.group},batch.partition,message.offset,async()=>{await persist(JSON.parse(message.value!.toString()));return true;});resolveOffset(message.offset);await consumer.commitOffsets([{topic:namespace.events,partition:batch.partition,offset:(BigInt(message.offset)+1n).toString()}]);await heartbeat();}}});
  for(let offset=0;offset<records.length;offset+=100){await producer.send({topic:namespace.events,acks:-1,messages:records.slice(offset,offset+100).map(record=>({key:record.project_id+':'+record.event.distinct_id,value:JSON.stringify(record)}))});if(offset===0)assert.equal((await run('status',state,'2'))?.status,'status_recorded','status permits legitimate in-flight lag');}
  await until(async()=>{const highs=await admin.fetchTopicOffsets(namespace.events),commits=(await admin.fetchOffsets({groupId:namespace.group,topics:[namespace.events],resolveOffsets:false}))[0]!.partitions,progress=(await db.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2',[namespace.events,namespace.group])).rows;return highs.every(p=>progress.find(v=>v.partition_id===p.partition)?.next_offset===p.high&&(commits.find(v=>v.partition===p.partition)?.offset===p.high||(p.high==='0'&&commits.find(v=>v.partition===p.partition)?.offset==='-1')));},'actual consumer did not drain');
  await consumer.disconnect();consumerConnected=false;
  const aggregate=await pool.connect();try{while((await aggregateNext(aggregate))==='completed'){};}finally{aggregate.release();}
  const proof=await inspectWindowFixture(db,fixture,ids);assert.equal(proof.complete,true);assert.equal(proof.raw,'1800');
  const original=(await db.query('SELECT event_time::text AS at FROM events WHERE project_id=$1 AND event_id=$2',[fixture.project,ids[0]])).rows[0];
  const moved=(await db.query("UPDATE events SET event_id=$3,event_time=event_time+interval '1 microsecond' WHERE project_id=$1 AND event_id=$2 RETURNING event_time::text AS at",[fixture.project,ids[0],ids[1]])).rows[0];
  try{await assert.rejects(inspectWindowFixture(db,fixture,ids),error=>error instanceof WindowDbError&&error.code==='window_db_event_set_mismatch');}finally{assert.equal((await db.query('UPDATE events SET event_id=$2,event_time=$4::timestamptz WHERE project_id=$1 AND event_id=$3 AND event_time=$5::timestamptz',[fixture.project,ids[0],ids[1],original.at,moved.at])).rowCount,1);}
  for(const epoch of [-1,null]){await db.query('UPDATE aggregation_jobs SET processed_epoch=$3 WHERE project_id=$1 AND event_id=$2',[fixture.project,ids[0],epoch]);assert.equal((await inspectWindowFixture(db,fixture,ids)).complete,false);const writer=await pool.connect();try{assert.equal(await aggregateNext(writer),'completed');}finally{writer.release();}assert.equal((await inspectWindowFixture(db,fixture,ids)).complete,true);}

  const start=new Date(Date.now()-1800000).toISOString(),finish=new Date().toISOString(),bytes=records.map(record=>Buffer.byteLength(JSON.stringify({write_key:fixture.key,schema_version:1,events:[record.event]})));
  const control={mode:'stdin_monotonic_lease',run_id:binding.run_id,lease_ms:5000},result={tenant:fixture.tenant,project:fixture.project,started_at:start,completed_at:finish,duration_seconds:1800,generation_seconds:1799,max_generation_lag_ms:0,target_eps:1,generated:1800,acknowledged:1800,duplicate_requests:35,backlog:0,not_yet_visible:0,ingest_p95_ms:1,observed_visible_p95_ms:1,query_p95_ms:1,errors:[],passed:true,event_ids:ids,workload:{ingest_attempts:1835,ingest_accepted_requests:1835,ingest_failed_requests:0,ingest_attempted_events:1835,ingest_body_bytes_total:bytes.reduce((sum,value)=>sum+value,0),ingest_body_bytes_min:Math.min(...bytes),ingest_body_bytes_max:Math.max(...bytes),query_opportunities:180,query_attempts:180,query_successes:180,query_failures:0,query_skipped_busy:0,visibility_requests:1800,ingest_body_bytes_mean:bytes.reduce((sum,value)=>sum+value,0)/bytes.length,query_schedule_coverage:true},termination:{state:'completed',reason:null,planned_events:1800},confirmed_event_ids:ids,artifacts:artifactHashes,control};
  const manifest={version:1,writer_pid:process.pid,artifacts:artifactHashes,control,status:'completed',termination_reason:null,tenant:fixture.tenant,project:fixture.project,planned_events:1800,started_at:start,event_ids:ids,confirmed_event_ids:ids,completed_at:finish};
  await json(join(state,'client-result.json'),result);await json(join(state,'client-manifest.json'),manifest);
  const resultRaw=await readFile(join(state,'client-result.json')),manifestRaw=await readFile(join(state,'client-manifest.json'));
  await json(join(state,'client-terminal.json'),{version:1,kind:'capacity_client_terminal',run_id:binding.run_id,binding_sha256:hash(await readFile(join(state,'binding.json'))),pid:process.pid,close:{code:0,signal:null,watchdog_fired:false},observed_at:new Date().toISOString(),result:{sha256:hash(resultRaw),bytes:resultRaw.length},manifest:{sha256:hash(manifestRaw),bytes:manifestRaw.length},artifacts:artifactHashes,control});
  assert.equal((await run('verify'))?.status,'verified');
  const badTerminal=await clone('bad-terminal');await change(badTerminal,'client-terminal.json',value=>{value.close.watchdog_fired=true;});assert.equal((await run('cleanup',badTerminal))?.status,'cleanup_deferred');await retained();
  const bare=await clone('bare-boolean');await writeFile(join(bare,'verified.json'),JSON.stringify({verified:true})+'\n',{mode:0o600});assert.equal((await run('cleanup',bare))?.status,'cleanup_deferred');await retained();
  const extra=await clone('unknown-terminal-field');await change(extra,'client-terminal.json',value=>{value.safe_to_cleanup=true;});assert.equal((await run('cleanup',extra))?.status,'cleanup_deferred');await retained();
  const changedSource=await clone('changed-baseline');await change(changedSource,'baseline.json',value=>{value.observation.state.database.oid='1';});assert.equal((await run('cleanup',changedSource))?.status,'cleanup_deferred');await retained();
  const locked=await clone('locked-progress');await blocker.query('BEGIN');await blocker.query('SELECT partition_id FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id FOR UPDATE',[namespace.events,namespace.group]);try{assert.equal((await run('cleanup',locked))?.status,'cleanup_deferred');await retained();}finally{await blocker.query('ROLLBACK');}
  const resources=await clone('unexpected-resource');await db.query('INSERT INTO reports(id,tenant_id,project_id,name,spec) VALUES($1,$2,$3,$4,$5)',[randomUUID(),fixture.tenant,fixture.project,'synthetic-unexpected','{}']);try{assert.equal((await run('cleanup',resources))?.status,'cleanup_deferred');await retained();}finally{await db.query('DELETE FROM reports WHERE tenant_id=$1',[fixture.tenant]);}
  const failed=await clone('failed-registration');await db.query("UPDATE event_dedup SET status='failed' WHERE project_id=$1 AND event_id=$2",[fixture.project,ids[0]]);try{assert.equal((await run('cleanup',failed))?.status,'cleanup_deferred');await retained();}finally{await db.query("UPDATE event_dedup SET status='stored' WHERE project_id=$1 AND event_id=$2",[fixture.project,ids[0]]);}
  assert.equal((await run('cleanup'))?.status,'cleaned');assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);fixture=undefined;
  const cli=spawnSync(process.execPath,[executable,'status',state,'3'],{encoding:'utf8',timeout:20000,maxBuffer:65536,env:{...process.env,DATABASE_URL:url.href,KAFKA_BROKERS:brokers.join(','),KAFKA_EVENTS_TOPIC:namespace.events,KAFKA_DLQ_TOPIC:namespace.dlq,KAFKA_GROUP_ID:namespace.group,PUBLIC_ORIGIN:config.origin,CCPANEL_WINDOW_CONFIGURATION_SHA256:configurationSha256,CCPANEL_WINDOW_PRODUCT_ROOT:product,CCPANEL_WINDOW_TOOL_ROOT:tools}});assert.equal(cli.status,0,'actual CLI status failed');assert.equal(cli.stderr,'');assert.equal(JSON.parse(cli.stdout).status,'status_recorded');
  assert.equal((await admin.fetchTopicOffsets(namespace.events)).reduce((sum,p)=>sum+BigInt(p.high),0n),1835n);assert.equal((await admin.fetchTopicOffsets(namespace.dlq)).reduce((sum,p)=>sum+BigInt(p.high),0n),0n);
  await until(async()=>(await db.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname=current_database() AND application_name='ccpanel_window225_db'")).rows[0].n===0,'operator PG session outlived bounded cleanup',3000);
  for(const lock of [169202610,169202617]){assert.equal((await db.query('SELECT pg_try_advisory_lock($1) AS held',[lock])).rows[0].held,true);await db.query('SELECT pg_advisory_unlock($1)',[lock]);}
  console.log(JSON.stringify({passed:true,actual_pg_and_kafka:true,all_six_commands:true,actual_cli:true,actual_broker_records:1835,actual_exact_unique_events:1800,actual_producer_consumer_checkpoint:true,actual_aggregate_current_epoch:true,revoke_while_gate_closed:true,revoke_repeat_no_overwrite:true,invalid_terminal_unknown_fields_bare_boolean_and_changed_source_refused:true,checkpoint_lock_contention_deferred:true,unexpected_resources_and_failed_registration_retained:true,transaction_cleanup_owned_only:true,synthetic_controller_client_documents:true,native_client_terminal_observed:false,operator_96m_resource_limit_verified:false,vps_accessed:false,capacity_verified:false}));
}finally{
  await blocker.query('ROLLBACK').catch(()=>{});if(consumerConnected)await consumer.disconnect().catch(()=>{});if(producerConnected)await producer.disconnect().catch(()=>{});
  if(topics){await admin.deleteGroups([namespace.group]).catch(()=>{});await admin.deleteTopics({topics:[namespace.events,namespace.dlq],timeout:10000}).catch(()=>{});}await admin.disconnect().catch(()=>{});
  if(fixture){const cleanup=spawnSync(process.execPath,['scripts/load-fixture.mjs','cleanup'],{input:JSON.stringify(fixture),encoding:'utf8',timeout:20000,maxBuffer:65536});assert.equal(cleanup.status,0,'isolated fixture cleanup failed');}
  await blocker.end();await db.end();await pool.end();assert.equal(dirname(resolve(root)),resolve(tmpdir()));assert.ok(basename(root).startsWith('ccpanel-window225-'));await rm(root,{recursive:true,force:true});
}
