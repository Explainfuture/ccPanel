// Guarded A14 drill: actual deployed API/Worker, public Node SDK and two real
// broker partitions. Only a new isolated topic's size limit is changed.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {readFile,writeFile,open,lstat} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel,ConfigResourceTypes,Partitioners}=kafkaPackage;

assert.equal(process.env.CCPANEL_ISOLATED_PARTIAL_PRODUCE_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.partialproduce\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin(),nativeFetch=globalThis.fetch;
const outboxPath='/drill-evidence/sdk-outbox.json',manifestPath='/drill-evidence/event-manifest.json';
const baseConfig=[{name:'min.insync.replicas',value:'1'},{name:'unclean.leader.election.enable',value:'false'},{name:'retention.ms',value:'259200000'},{name:'retention.bytes',value:'134217728'}];
const dlqConfig=[{name:'min.insync.replicas',value:'1'},{name:'unclean.leader.election.enable',value:'false'},{name:'retention.ms',value:'1209600000'},{name:'retention.bytes',value:'67108864'}];
let child,childExit,childKind,ready=false,admissionAllowed=false,diagnostics='',brokerRejections=0,topicsCreated=false,sdk,summary;
let lockReleased=false,cleanupConfirmed=false,forcedStops=0;
const attempts=[];
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const live=()=>child&&child.exitCode===null&&child.signalCode===null;
async function until(label,check,ms=45000){
  const deadline=Date.now()+ms;
  do{if(child&&!live())throw Error('isolated_'+childKind+'_exited');if(await check())return;await sleep(300);}while(Date.now()<deadline);
  throw Error('Timed out: '+label);
}
function start(kind){
  assert.equal(child,undefined);childKind=kind;ready=false;admissionAllowed=false;
  child=spawn(process.execPath,['--max-old-space-size=40','dist/'+kind+'.mjs'],{env:{...process.env,PORT:'3198',HOST:'127.0.0.1'},stdio:['ignore','pipe','pipe']});
  const current=child;
  childExit=new Promise((resolve,reject)=>{current.once('error',reject);current.once('exit',(code,signal)=>resolve({code,signal}));});
  for(const stream of [current.stdout,current.stderr]){
    let remainder='';stream.on('data',chunk=>{
      diagnostics=(diagnostics+chunk.toString()).slice(-32768);remainder=(remainder+chunk.toString()).slice(-65536);
      const lines=remainder.split('\n');remainder=lines.pop();
      for(const line of lines)try{
        const value=JSON.parse(line);
        if(value.code===kind+'_ready')ready=true;
        if(kind==='api'&&value.code==='ingest_admission_changed')admissionAllowed=value.allowed===true;
        if(kind==='api'&&value.logger==='kafkajs'&&value.message?.includes('Response Produce(')&&/message.*(large|size)|record.*large|MESSAGE_TOO_LARGE/i.test(String(value.error??'')))brokerRejections++;
      }catch{/* Other child log lines remain only in bounded private evidence. */}
    });
  }
}
async function stop(){
  if(!child)return;
  const current=child,pending=childExit;
  if(live())current.kill('SIGTERM');
  const timer=setTimeout(()=>{if(current.exitCode===null&&current.signalCode===null){forcedStops++;current.kill('SIGKILL');}},6000);
  try{await pending;}finally{clearTimeout(timer);child=undefined;childKind=undefined;childExit=undefined;}
}
const progress=async()=>(await db.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id',[topic,group])).rows;
const gate=async()=>(await db.query('SELECT blocked,reason,detected_at::text,details FROM ingest_integrity_gate WHERE id=true')).rows[0];
const counts=async()=>(await db.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM event_dedup) AS dedup,(SELECT count(*)::int FROM aggregation_jobs) AS jobs,(SELECT count(*)::int FROM dlq_records) AS dlq')).rows[0];
const highs=async()=>(await admin.fetchTopicOffsets(topic)).map(p=>({partition:p.partition,high:p.high})).sort((a,b)=>a.partition-b.partition);
const commits=async()=>(await admin.fetchOffsets({groupId:group,topics:[topic],resolveOffsets:false}))[0].partitions.map(p=>({partition:p.partition,offset:p.offset})).sort((a,b)=>a.partition-b.partition);
async function observedConfig(max){
  const wanted=[...baseConfig,{name:'max.message.bytes',value:String(max)}];
  const result=(await admin.describeConfigs({resources:[{type:ConfigResourceTypes.TOPIC,name:topic,configNames:wanted.map(c=>c.name)}]})).resources[0];
  assert.equal(result.errorCode,0);assert.equal(result.resourceName,topic);
  for(const entry of wanted){const entries=result.configEntries.filter(c=>c.configName===entry.name);assert.equal(entries.length,1);assert.equal(entries[0].configValue,entry.value);}
  return Object.fromEntries(wanted.map(c=>[c.name,c.value]));
}
async function cleanupKafka(){
  if((await admin.listGroups()).groups.some(g=>g.groupId===group)){
    await until('isolated group empty',async()=>!(await admin.describeGroups([group])).groups[0]?.members.length,40000);
    await admin.deleteGroups([group]);
  }
  const existing=await admin.listTopics(),owned=[topic,dlq].filter(t=>existing.includes(t));
  if(owned.length)await admin.deleteTopics({topics:owned,timeout:20000});
  await until('isolated topics removed',async()=>{const names=await admin.listTopics();return !names.includes(topic)&&!names.includes(dlq);},25000);
  assert.ok(!(await admin.listGroups()).groups.some(g=>g.groupId===group));cleanupConfirmed=true;
}
if(process.argv[2]==='cleanup'){
  try{await admin.connect();await cleanupKafka();console.log(JSON.stringify({isolated_topics_and_group_removed:true}));}finally{await admin.disconnect();}
  process.exit(0);
}
try{
  const expectedSdkHash=process.env.CCPANEL_PARTIAL_SDK_SHA256;assert.match(expectedSdkHash??'',/^[a-f0-9]{64}$/);
  assert.equal(createHash('sha256').update(await readFile('/drill-sdk/index.mjs')).digest('hex'),expectedSdkHash);
  const {createNodeAnalytics}=await import('/drill-sdk/index.mjs');
  await db.connect();await admin.connect();
  assert.equal((await db.query('SELECT max(version) AS version FROM schema_migrations')).rows[0].version,22);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM consumer_streams')).rows[0].n,0);
  assert.equal((await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false);
  for(const path of [outboxPath,outboxPath+'.lock',manifestPath])await assert.rejects(lstat(path),{code:'ENOENT'});
  const existing=await admin.listTopics();assert.ok(!existing.includes(topic)&&!existing.includes(dlq));topicsCreated=true;
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[...baseConfig,{name:'max.message.bytes',value:'1536'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:dlqConfig}]});
  const faultConfig=await observedConfig(1536);
  const metadata=(await admin.fetchTopicMetadata({topics:[topic]})).topics[0];assert.equal(metadata.partitions.length,2);assert.ok(metadata.partitions.every(p=>p.partitionErrorCode===0&&p.leader>=0));
  const tenant=randomUUID(),project=randomUUID(),writeKey=randomBytes(24).toString('hex');
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED PARTIAL PRODUCE']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED PARTIAL PRODUCE']);
  await db.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(writeKey).digest('hex'),'TEMP']);
  const partitioner=Partitioners.DefaultPartitioner(),identities=new Map();
  for(let n=0;n<1000&&identities.size<2;n++){
    const identity='partial-produce-'+n,partition=partitioner({topic,partitionMetadata:metadata.partitions,message:{key:project+':'+identity}});
    if(!identities.has(partition))identities.set(partition,identity);
  }
  assert.equal(identities.size,2);const smallPartition=0,largePartition=1;
  start('worker');await until('initial Worker checkpoints',async()=>ready&&(await progress()).length===2);
  assert.deepEqual(await progress(),[{partition_id:0,next_offset:'0'},{partition_id:1,next_offset:'0'}]);await stop();
  const initialGate=await gate();assert.equal(initialGate.blocked,false);assert.deepEqual(await counts(),{raw:0,dedup:0,jobs:0,dlq:0});
  assert.deepEqual(await highs(),[{partition:0,high:'0'},{partition:1,high:'0'}]);
  start('api');await until('actual API ready and admission allowed',()=>ready&&admissionAllowed);const apiPid=child.pid;
  sdk=createNodeAnalytics({endpoint:'http://127.0.0.1:3198/v1/batch',projectId:project,writeKey,consent:true,outboxPath,flushInterval:60000,fetch:async(endpoint,init)=>{
    const sentAt=Date.now(),body=String(init.body),response=await nativeFetch(endpoint,init);let responseBody;
    try{responseBody=await response.clone().json();}catch{/* The unchanged response still reaches the SDK. */}
    attempts.push({sentAt,respondedAt:Date.now(),status:response.status,body,responseBody});return response;
  }});
  await sdk.flush();assert.equal(attempts.length,0);
  const small=await sdk.trackFor(identities.get(smallPartition),'partial_produce_small',{synthetic:true});
  const large=await sdk.trackFor(identities.get(largePartition),'partial_produce_large',{synthetic:true,padding:'x'.repeat(4096)});
  assert.equal(small.status,'queued-persistent');assert.equal(large.status,'queued-persistent');assert.notEqual(small.event_id,large.event_id);assert.equal(attempts.length,0);
  const originalOutbox=JSON.parse(await readFile(outboxPath,'utf8'));assert.equal(originalOutbox.version,1);assert.equal(originalOutbox.rows.length,2);
  const originalEvents=originalOutbox.rows.map(r=>r.event),originalIds=originalEvents.map(e=>e.event_id);
  assert.deepEqual(originalIds,[small.event_id,large.event_id]);assert.equal((await sdk.getQueueStatus()).persistent_count,2);
  const manifestBytes=Buffer.from(JSON.stringify({version:1,scenario:'partial_produce',events:originalEvents,routing:[{event_id:small.event_id,partition:smallPartition},{event_id:large.event_id,partition:largePartition}]})+'\n');
  const manifest=await open(manifestPath,'wx',0o600);try{await manifest.writeFile(manifestBytes);await manifest.sync();}finally{await manifest.close();}
  const evidenceDirectory=await open('/drill-evidence','r');try{await evidenceDirectory.sync();}finally{await evidenceDirectory.close();}
  const manifestHash=createHash('sha256').update(manifestBytes).digest('hex');assert.deepEqual(await readFile(manifestPath),manifestBytes);
  const first=await sdk.flush();assert.equal(first.confirmed,0);assert.equal(first.pending,2);
  assert.equal(attempts.length,1);assert.equal(attempts[0].status,503);assert.deepEqual(attempts[0].responseBody,{error:'temporarily_unavailable'});
  const originalBody=attempts[0].body;assert.deepEqual(JSON.parse(originalBody).events,originalEvents);
  assert.deepEqual(JSON.parse(await readFile(outboxPath,'utf8')).rows.map(r=>r.event),originalEvents);assert.equal((await sdk.getQueueStatus()).persistent_count,2);
  await until('real Produce size rejection',()=>brokerRejections>0,10000);
  const partialHigh=await highs();assert.deepEqual(partialHigh,[{partition:smallPartition,high:'1'},{partition:largePartition,high:'0'}]);
  assert.deepEqual(await counts(),{raw:0,dedup:0,jobs:0,dlq:0});assert.deepEqual(await progress(),[{partition_id:0,next_offset:'0'},{partition_id:1,next_offset:'0'}]);
  assert.ok((await commits()).every(p=>['-1','0'].includes(p.offset)));assert.deepEqual(await gate(),initialGate);
  await sleep(1000);assert.equal(attempts.length,1);assert.deepEqual(await highs(),partialHigh);assert.equal(child.pid,apiPid);assert.ok(live());
  // alterConfigs replaces topic overrides, so explicitly retain every initial
  // ISR/election/retention override while restoring only this random topic's cap.
  await admin.alterConfigs({validateOnly:false,resources:[{type:ConfigResourceTypes.TOPIC,name:topic,configEntries:[...baseConfig,{name:'max.message.bytes',value:'1048588'}]}]});
  const restoredConfig=await observedConfig(1048588);
  await until('same SDK automatically retries complete batch',async()=>attempts.some(a=>a.status===202)&&(await sdk.getQueueStatus()).count===0,90000);
  assert.equal(attempts.length,2);assert.equal(attempts[1].status,202);assert.ok(attempts[1].body===originalBody,'retry payload changed');assert.deepEqual(attempts[1].responseBody,{accepted_event_ids:originalIds});
  assert.equal(child.pid,apiPid);assert.ok(live());assert.deepEqual(JSON.parse(await readFile(outboxPath,'utf8')).rows,[]);
  const retryHigh=await highs();assert.ok(['1','2'].includes(retryHigh.find(p=>p.partition===smallPartition).high));assert.equal(retryHigh.find(p=>p.partition===largePartition).high,'1');
  await sdk.shutdown();sdk=undefined;await assert.rejects(lstat(outboxPath+'.lock'),{code:'ENOENT'});lockReleased=true;
  const duplicate=await nativeFetch('http://127.0.0.1:3198/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body:originalBody,signal:AbortSignal.timeout(10000)});
  assert.equal(duplicate.status,202);assert.deepEqual(await duplicate.json(),{accepted_event_ids:originalIds});assert.equal(child.pid,apiPid);assert.ok(live());
  const finalHigh=await highs();for(const row of finalHigh)assert.equal(BigInt(row.high)-BigInt(retryHigh.find(p=>p.partition===row.partition).high),1n);
  const expectedDuplicates=finalHigh.reduce((n,p)=>n+Number(p.high),0)-2;assert.ok(expectedDuplicates>=2&&expectedDuplicates<=3);
  await stop();start('worker');
  await until('PG advances to actual broker highs',async()=>{const rows=await progress();return ready&&rows.length===2&&rows.every(p=>p.next_offset===finalHigh.find(h=>h.partition===p.partition_id).high);},60000);
  await until('Kafka commits actual PG checkpoints',async()=>{const rows=await commits();return rows.length===2&&rows.every(p=>p.offset===finalHigh.find(h=>h.partition===p.partition).high);});
  assert.deepEqual(await counts(),{raw:2,dedup:2,jobs:2,dlq:0});
  for(const table of ['events','event_dedup','aggregation_jobs'])assert.deepEqual((await db.query('SELECT event_id::text FROM '+table+' ORDER BY event_id')).rows.map(r=>r.event_id),[...originalIds].sort());
  const raw=(await db.query('SELECT event_id::text,event_name,distinct_id,anonymous_id,event_time,properties,context,schema_version FROM events ORDER BY event_id')).rows;
  for(const row of raw){const event=originalEvents.find(e=>e.event_id===row.event_id);assert.equal(row.event_name,event.event_name);assert.equal(row.distinct_id,event.distinct_id);assert.equal(row.anonymous_id,event.anonymous_id??null);assert.equal(row.event_time.toISOString(),event.event_time);assert.deepEqual(row.properties,event.properties);assert.deepEqual(row.context,event.context);assert.equal(row.schema_version,event.schema_version);}
  await until('two exact aggregate contributions',async()=>{
    const jobs=(await db.query('SELECT event_id::text FROM aggregation_jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL ORDER BY event_id')).rows;
    const buckets=(await db.query('SELECT event_name,event_count::text AS count FROM visible_event_count_buckets WHERE project_id=$1 ORDER BY event_name',[project])).rows;
    return JSON.stringify(jobs.map(r=>r.event_id))===JSON.stringify([...originalIds].sort())&&JSON.stringify(buckets)===JSON.stringify([{event_name:'partial_produce_large',count:'1'},{event_name:'partial_produce_small',count:'1'}]);
  });
  const duplicates=Number((await db.query("SELECT coalesce(sum(value),0)::text AS n FROM ingest_metrics WHERE project_id=$1 AND metric='duplicates'",[project])).rows[0].n);assert.equal(duplicates,expectedDuplicates);
  assert.deepEqual(await gate(),initialGate);assert.ok((await admin.fetchTopicOffsets(dlq)).every(p=>p.high==='0'));assert.deepEqual(await highs(),finalHigh);
  assert.deepEqual(await readFile(manifestPath),manifestBytes);assert.equal(createHash('sha256').update(await readFile(manifestPath)).digest('hex'),manifestHash);
  assert.deepEqual(JSON.parse(await readFile(outboxPath,'utf8')).rows,[]);await stop();
  summary={passed:true,current_deployed_api_worker:true,public_node_sdk:true,sdk_sha256:expectedSdkHash,queue_mode:'persistent',first_batch_events:2,distinct_partition_keys:true,real_topic_size_fault:true,fault_config:faultConfig,restored_config:restoredConfig,broker_produce_size_rejections:brokerRejections,first_request_partial_broker_highs:partialHigh,first_http_status:503,first_http_error:'temporarily_unavailable',whole_batch_remained_in_outbox:true,pg_rows_during_fault:0,same_api_pid_through_recovery:true,automatic_same_sdk_retry:true,sdk_http_statuses:attempts.map(a=>a.status),retry_payload_identical:true,acknowledged_original_event_ids:originalIds,outbox_empty_after_ack:true,explicit_duplicate_http_status:202,explicit_duplicate_records:2,retry_broker_highs:retryHigh,final_broker_highs:finalHigh,duplicates_derived_from_actual_kafka:true,duplicates,raw:2,dedup:2,jobs:2,processed:2,aggregate_bucket_rows:2,aggregate_contributions:2,dlq:0,integrity_gate_unchanged:true,event_manifest_sha256:manifestHash,request_payload_sha256:createHash('sha256').update(originalBody).digest('hex')};
}catch(error){
  await writeFile('/drill-evidence/processes.log',diagnostics+'\n'+JSON.stringify({brokerRejections,attempt_statuses:attempts.map(a=>a.status),forcedStops}),{mode:0o600}).catch(()=>{});throw error;
}finally{
  try{if(sdk){try{await sdk.shutdown();}finally{sdk=undefined;}}}finally{
    await stop();try{if(topicsCreated)await cleanupKafka();}finally{await admin.disconnect();await db.end();}
  }
}
assert.ok(summary);assert.equal(forcedStops,0);assert.ok(lockReleased&&cleanupConfirmed);
console.log(JSON.stringify({...summary,sdk_lock_released:true,children_stopped_gracefully:true,isolated_topics_and_group_removed:true}));
