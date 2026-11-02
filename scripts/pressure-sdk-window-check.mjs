// Synthetic disk telemetry only inside a fresh isolated stream/DB, via the guarded wrapper.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {writeFile,readFile,rename} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel}=kafkaPackage;
assert.equal(process.env.CCPANEL_ISOLATED_PRESSURE_SDK_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.pressuresdk\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin();let child,exit,ready=false,diagnostics='',topicsCreated=false;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(label,check,ms=40000){const end=Date.now()+ms;do{if(await check())return;await sleep(300);}while(Date.now()<end);throw Error('Timed out: '+label);}
function start(kind){
  assert.ok(!child);ready=false;
  child=spawn(process.execPath,['--max-old-space-size=40',`dist/${kind}.mjs`],{env:{...process.env,PORT:'3198',HOST:'127.0.0.1',...(kind==='api'?{HOST_TELEMETRY_PATH:'/drill-evidence/pressure.json'}:{})},stdio:['ignore','pipe','pipe']});
  exit=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
  for(const stream of [child.stdout,child.stderr]){let remainder='';stream.on('data',chunk=>{
    diagnostics=(diagnostics+chunk.toString()).slice(-32768);remainder+=chunk;const parts=remainder.split('\n');remainder=parts.pop();
    for(const line of parts){try{if(JSON.parse(line).code===kind+'_ready')ready=true;}catch{}}
  });}
}
async function stop(){
  if(!child)return;
  if(child.exitCode===null&&child.signalCode===null)child.kill('SIGTERM');
  const timer=setTimeout(()=>child?.kill('SIGKILL'),6000);
  try{await exit;}finally{clearTimeout(timer);child=undefined;}
}
const progress=async()=>(await db.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id',[topic,group])).rows;
const gate=async()=>(await db.query('SELECT blocked,reason,detected_at::text,details FROM ingest_integrity_gate WHERE id=true')).rows[0];
const counts=async()=>(await db.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM event_dedup) AS dedup,(SELECT count(*)::int FROM aggregation_jobs) AS jobs,(SELECT count(*)::int FROM dlq_records) AS dlq')).rows[0];
async function cleanupKafka(){
  if((await admin.listGroups()).groups.some(g=>g.groupId===group)){
    await until('isolated group empty',async()=>!(await admin.describeGroups([group])).groups[0]?.members.length);
    await admin.deleteGroups([group]);
  }
  const existing=await admin.listTopics(),owned=[topic,dlq].filter(t=>existing.includes(t));
  if(owned.length)await admin.deleteTopics({topics:owned,timeout:20000});
  await until('isolated topics removed',async()=>{const names=await admin.listTopics();return !names.includes(topic)&&!names.includes(dlq);});
  assert.ok(!(await admin.listGroups()).groups.some(g=>g.groupId===group));
}
if(process.argv[2]==='cleanup'){
  try{await admin.connect();await cleanupKafka();console.log(JSON.stringify({isolated_topics_and_group_removed:true}));}finally{await admin.disconnect();}
  process.exit(0);
}
let sdk,telemetryTimer,telemetryError,telemetryWrite=Promise.resolve(),lowDisk=true;
async function publishTelemetry(){
  const telemetry=JSON.parse(await readFile('/telemetry/host.json','utf8'));
  assert.ok(Date.now()-telemetry.sampledAt<30000,'real source telemetry is stale');
  assert.equal(telemetry.disks.length,2);
  // Only the private API reads this synthetic override. Never fill real disks.
  if(lowDisk)for(const disk of telemetry.disks)disk.availableBytes='0';
  const temporary='/drill-evidence/pressure-'+randomUUID()+'.tmp';
  await writeFile(temporary,JSON.stringify(telemetry),{mode:0o600});await rename(temporary,'/drill-evidence/pressure.json');
}
try{
  const {Analytics}=await import('../dist/web/sdk/ccpanel.js');
  await db.connect();await admin.connect();
  assert.equal((await db.query('SELECT max(version) AS version FROM schema_migrations')).rows[0].version,22);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM consumer_streams')).rows[0].n,0);
  assert.equal((await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false);
  const existing=await admin.listTopics();assert.ok(!existing.includes(topic)&&!existing.includes(dlq));topicsCreated=true;
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1},{topic:dlq,numPartitions:1,replicationFactor:1}]});
  const tenant=randomUUID(),project=randomUUID(),key=randomBytes(24).toString('hex');
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED PRESSURE SDK']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED PRESSURE SDK']);
  await db.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(key).digest('hex'),'TEMP']);
  start('worker');await until('initial Worker progress',async()=>ready&&(await progress()).length===2);await stop();
  const initialGate=await gate();assert.equal(initialGate.blocked,false);
  await publishTelemetry();
  telemetryTimer=setInterval(()=>{telemetryWrite=telemetryWrite.then(publishTelemetry).catch(error=>{telemetryError=error;});},1000);
  start('api');await until('pressure API ready',async()=>ready);
  const attempts=[];let firstComplete=false,retryMs=0,originalBody;
  sdk=new Analytics({endpoint:'http://127.0.0.1:3198/v1/batch',projectId:project,writeKey:key,consent:true,flushInterval:1000,fetch:async(endpoint,init)=>{
    const sentAt=Date.now(),body=String(init.body);const response=await fetch(endpoint,init);
    const respondedAt=Date.now();attempts.push({sentAt,respondedAt,status:response.status,body});
    if(attempts.length===1){
      assert.equal(response.status,503);assert.equal((await response.clone().json()).reason,'disk_space_low');
      retryMs=Number(response.headers.get('Retry-After'))*1000;assert.equal(retryMs,30000);originalBody=body;firstComplete=true;
    }
    return response;
  }});
  await sdk.flush();const tracked=await sdk.track('pressure_sdk_current',{synthetic:true});assert.equal(tracked.status,'queued-memory');
  await sdk.flush();await until('real disk pressure503 observed',async()=>firstComplete);
  assert.equal(attempts.length,1);assert.equal((await sdk.getQueueStatus()).count,1);
  assert.deepEqual(await counts(),{raw:0,dedup:0,jobs:0,dlq:0});
  assert.ok((await admin.fetchTopicOffsets(topic)).every(p=>p.high==='0'));
  await sleep(3000);await sdk.flush();assert.equal(attempts.length,1,'manual flush must also respect Retry-After');
  assert.equal((await sdk.getQueueStatus()).count,1);assert.deepEqual(await gate(),initialGate);
  lowDisk=false;telemetryWrite=telemetryWrite.then(publishTelemetry);await telemetryWrite;
  await until('automatic original SDK retry accepted',async()=>attempts.some(a=>a.status===202)&&(await sdk.getQueueStatus()).count===0,75000);
  assert.equal(attempts.length,2);assert.equal(attempts[1].status,202);assert.equal(attempts[1].body,originalBody);
  const elapsed=attempts[1].sentAt-attempts[0].respondedAt;assert.ok(elapsed>=retryMs-100);
  const event=JSON.parse(originalBody).events[0];assert.equal(event.event_id,tracked.event_id);
  sdk.destroy();sdk=undefined;
  const duplicate=await fetch('http://127.0.0.1:3198/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body:originalBody,signal:AbortSignal.timeout(5000)});
  assert.equal(duplicate.status,202);assert.deepEqual((await duplicate.json()).accepted_event_ids,[event.event_id]);
  await stop();clearInterval(telemetryTimer);telemetryTimer=undefined;await telemetryWrite;if(telemetryError)throw telemetryError;
  start('worker');await until('Worker processes original and exact duplicate',async()=>{
    const positions=await progress();return positions.reduce((n,p)=>n+Number(p.next_offset),0)===2;
  });
  await until('two records committed to Kafka',async()=>{
    const commits=(await admin.fetchOffsets({groupId:group,topics:[topic],resolveOffsets:false}))[0].partitions;
    return commits.some(p=>p.offset==='2');
  });
  assert.deepEqual(await counts(),{raw:1,dedup:1,jobs:1,dlq:0});
  assert.deepEqual((await db.query('SELECT event_id::text FROM events')).rows,[{event_id:event.event_id}]);
  assert.equal((await db.query("SELECT value::text FROM ingest_metrics WHERE project_id=$1 AND metric='duplicates'",[project])).rows[0].value,'1');
  await until('one exact aggregate contribution',async()=>{
    const row=(await db.query('SELECT (SELECT count(*)::int FROM aggregation_jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL) AS processed,(SELECT coalesce(sum(event_count),0)::int FROM visible_event_count_buckets) AS contributions')).rows[0];
    return row.processed===1&&row.contributions===1;
  });
  assert.deepEqual(await gate(),initialGate);assert.ok((await admin.fetchTopicOffsets(dlq)).every(p=>p.high==='0'));
  assert.equal((await admin.fetchTopicOffsets(topic)).reduce((n,p)=>n+Number(p.high),0),2);
  console.log(JSON.stringify({passed:true,current_deployed_sdk_api_worker:true,queue_mode:'memory',synthetic_disk_telemetry:true,real_disk_filled:false,http_statuses:attempts.map(a=>a.status),retry_after_ms:retryMs,retry_elapsed_ms:elapsed,forced_flush_respected_retry_after:true,complete_payload_identical:true,original_event_id:true,queue_empty_after_ack:true,duplicate_acknowledged:true,counts:await counts(),duplicates:1,processed:1,aggregate_contributions:1,integrity_gate_unchanged:true}));
}catch(error){await writeFile('/drill-evidence/processes.log',diagnostics,{mode:0o600}).catch(()=>{});throw error;
}finally{
  sdk?.destroy();if(telemetryTimer)clearInterval(telemetryTimer);await telemetryWrite.catch(()=>{});await stop();
  try{if(topicsCreated)await cleanupKafka();}finally{await admin.disconnect();await db.end();}
}
