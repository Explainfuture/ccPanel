// Destructive only inside a fresh isolated stream/DB, via the guarded wrapper.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel}=kafkaPackage;
assert.equal(process.env.CCPANEL_ISOLATED_RETAINED_GAP_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.retainedgap\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin();let child,exit,ready=false,diagnostics='',topicsCreated=false;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(label,check,ms=40000){const end=Date.now()+ms;do{if(await check())return;await sleep(300);}while(Date.now()<end);throw Error('Timed out: '+label);}
function start(kind){
  assert.ok(!child);ready=false;
  child=spawn(process.execPath,['--max-old-space-size=40',`dist/${kind}.mjs`],{env:{...process.env,PORT:'3198',HOST:'127.0.0.1'},stdio:['ignore','pipe','pipe']});
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
try{
  await db.connect();await admin.connect();
  assert.equal((await db.query('SELECT max(version) AS version FROM schema_migrations')).rows[0].version,22);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM consumer_streams')).rows[0].n,0);
  assert.equal((await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false);
  const existing=await admin.listTopics();assert.ok(!existing.includes(topic)&&!existing.includes(dlq));
  topicsCreated=true;
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1},{topic:dlq,numPartitions:1,replicationFactor:1}]});
  const tenant=randomUUID(),project=randomUUID(),key=randomBytes(24).toString('hex');
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED RETAINED GAP']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED RETAINED GAP']);
  await db.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(key).digest('hex'),'TEMP']);
  start('worker');await until('initial Worker progress',async()=>ready&&(await progress()).length===2);
  await stop();
  assert.deepEqual(await progress(),[{partition_id:0,next_offset:'0'},{partition_id:1,next_offset:'0'}]);
  start('api');await until('initial API ready',async()=>ready);
  const event={event_id:randomUUID(),event_name:'retained_gap_synthetic',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true},context:{},schema_version:1};
  const body=JSON.stringify({write_key:key,project_id:project,schema_version:1,events:[event]});
  const post=()=>fetch('http://127.0.0.1:3198/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body,signal:AbortSignal.timeout(5000)});
  const accepted=await post();assert.equal(accepted.status,202);
  assert.deepEqual((await accepted.json()).accepted_event_ids,[event.event_id]);
  await stop();
  assert.deepEqual(await counts(),{raw:0,dedup:0,jobs:0,dlq:0});
  const source=await admin.fetchTopicOffsets(topic),lost=source.filter(p=>p.high==='1');
  assert.equal(lost.length,1);assert.ok(source.every(p=>p.low==='0'&&['0','1'].includes(p.high)));
  assert.equal((await admin.describeGroups([group])).groups[0]?.state,'Empty');
  await admin.deleteTopicRecords({topic,partitions:[{partition:lost[0].partition,offset:'1'}]});
  await until('broker lower bound advanced',async()=>(await admin.fetchTopicOffsets(topic)).some(p=>p.partition===lost[0].partition&&p.low==='1'));
  await admin.setOffsets({groupId:group,topic,partitions:source.map(p=>({partition:p.partition,offset:p.high}))});
  const broker=async()=>(await admin.fetchOffsets({groupId:group,topics:[topic],resolveOffsets:false}))[0].partitions;
  const commits=await broker();assert.ok(source.every(p=>commits.some(c=>c.partition===p.partition&&c.offset===p.high)));
  // Startup must discover the real retained gap even though broker lag is zero.
  start('worker');await until('Worker exits on lost acknowledged record',async()=>child.exitCode!==null||child.signalCode!==null);
  const failed=await exit;assert.notEqual(failed.code,0);assert.equal(failed.signal,null);await stop();
  const incident=await gate();assert.equal(incident.blocked,true);assert.equal(incident.reason,'retained_offset_gap');
  assert.equal(incident.details.expected,'0');assert.equal(incident.details.low,'1');assert.equal(incident.details.partition,lost[0].partition);
  const verify=async()=>{
    const response=await post();assert.equal(response.status,503);assert.equal((await response.json()).reason,'retained_offset_gap');assert.ok(Number(response.headers.get('Retry-After'))>0);
    assert.deepEqual(await gate(),incident);
    assert.equal((await db.query("SELECT count(*)::int AS n FROM consumer_progress_audit WHERE action='paused'")).rows[0].n,1);
    assert.deepEqual(await progress(),[{partition_id:0,next_offset:'0'},{partition_id:1,next_offset:'0'}]);
    assert.deepEqual(await counts(),{raw:0,dedup:0,jobs:0,dlq:0});
    assert.deepEqual(await broker(),commits);
    const now=await admin.fetchTopicOffsets(topic);
    for(const p of now){assert.equal(p.high,source.find(v=>v.partition===p.partition).high);assert.equal(p.low,p.partition===lost[0].partition?'1':'0');}
    assert.ok((await admin.fetchTopicOffsets(dlq)).every(p=>p.high==='0'));
  };
  start('api');await until('blocked API ready',async()=>ready);await verify();await sleep(1200);await verify();await stop();
  start('worker');await until('restarted Worker still exits',async()=>child.exitCode!==null||child.signalCode!==null);
  const restarted=await exit;assert.notEqual(restarted.code,0);assert.equal(restarted.signal,null);await stop();
  start('api');await until('restarted API ready',async()=>ready);await verify();
  console.log(JSON.stringify({passed:true,actual_current_api_worker:true,http_acknowledged_original_id:true,deleted_isolated_acknowledged_records:1,broker_lag_after_reset:0,worker_startups_refused:2,api_503_checks:3,restart_preserved_incident:true,paused_incidents:1,pg_progress:[0,0],counts:await counts(),automatic_gap_acknowledgement:false}));
}catch(error){await writeFile('/drill-evidence/processes.log',diagnostics,{mode:0o600}).catch(()=>{});throw error;
}finally{
  await stop();
  try{if(topicsCreated)await cleanupKafka();}finally{await admin.disconnect();await db.end();}
}
