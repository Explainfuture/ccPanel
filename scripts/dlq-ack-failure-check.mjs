// Run only via the guarded remote controller, against an empty temporary DB.
// This uses the actual deployed Worker and broker; no producer mocks.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID,createHash} from 'node:crypto';
import {once} from 'node:events';
import {writeFile} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel,ConfigResourceTypes}=kafkaPackage;

assert.equal(process.env.CCPANEL_ISOLATED_DLQ_ACK_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.dlqack\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin(),producer=kafka.producer();let worker,workerExit,lines='',metrics=[],brokerRejections=0,ready=false,topicsCreated=false;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(label,check,ms=45000){const end=Date.now()+ms;do{if(await check())return;await sleep(500);}while(Date.now()<end);throw Error('Timed out: '+label);}
async function stopWorker(){if(!worker)return;const child=worker;worker=undefined;if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),6000);try{await workerExit;}finally{clearTimeout(timer);}}}
function startWorker(){
  ready=false;worker=spawn(process.execPath,['--max-old-space-size=40','dist/worker.mjs'],{env:process.env,stdio:['ignore','pipe','pipe']});
  workerExit=once(worker,'exit');
  const listen=stream=>{let remainder='';stream.on('data',chunk=>{remainder+=chunk;const parts=remainder.split('\n');remainder=parts.pop();for(const line of parts){try{const value=JSON.parse(line);if(value.code==='worker_ready')ready=true;if(value.code==='worker_metrics'){metrics.push(value);metrics=metrics.slice(-12);}if(value.logger==='kafkajs'&&value.message?.includes('Response Produce(')&&/message.*(large|size)|record.*large|MESSAGE_TOO_LARGE/i.test(value.error??''))brokerRejections++;}catch{}}});};
  listen(worker.stdout);listen(worker.stderr);
  // Bounded private diagnostics only. Never print broker payloads/credentials.
  worker.stderr.on('data',chunk=>{lines=(lines+chunk.toString()).slice(-16384);});
}
async function progress(){return (await db.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id',[topic,group])).rows;}
async function snapshot(){
  const rows=await progress(),committed=(await admin.fetchOffsets({groupId:group,topics:[topic]}))[0].partitions;
  const source=await admin.fetchTopicOffsets(topic),dead=await admin.fetchTopicOffsets(dlq);
  const counts=(await db.query('SELECT (SELECT count(*)::text FROM events) AS raw,(SELECT count(*)::text FROM event_dedup) AS dedup,(SELECT count(*)::text FROM dlq_records) AS dlq')).rows[0];
  return {rows,committed:committed.map(p=>({partition:p.partition,offset:p.offset})).sort((a,b)=>a.partition-b.partition),source:source.map(p=>({partition:p.partition,high:p.high})).sort((a,b)=>a.partition-b.partition),dead:dead.map(p=>({partition:p.partition,high:p.high})),counts};
}
function blocked(s){assert.deepEqual(s.rows,[{partition_id:0,next_offset:'0'},{partition_id:1,next_offset:'0'}]);assert.deepEqual(s.source,[{partition:0,high:'2'},{partition:1,high:'0'}]);assert.equal(s.dead[0].high,'0');assert.deepEqual(s.counts,{raw:'0',dedup:'0',dlq:'0'});assert.ok(s.committed.every(p=>['-1','0'].includes(p.offset)));}
if(process.argv[2]==='cleanup'){
  try{
    await admin.connect();
    const groups=await admin.listGroups();
    if(groups.groups.some(g=>g.groupId===group)){
      await until('test consumer group has no active members',async()=>!(await admin.describeGroups([group])).groups[0]?.members.length,40000);
      await admin.deleteGroups([group]);
    }
    const topics=await admin.listTopics(),owned=[topic,dlq].filter(t=>topics.includes(t));
    if(owned.length)await admin.deleteTopics({topics:owned,timeout:20000});
    await until('isolated topics removed',async()=>{const names=await admin.listTopics();return !names.includes(topic)&&!names.includes(dlq);},20000);
    assert.ok(!(await admin.listGroups()).groups.some(g=>g.groupId===group));
    console.log(JSON.stringify({isolated_topics_and_group_removed:true}));
  }finally{await admin.disconnect();}
  process.exit(0);
}
try{
  await db.connect();await admin.connect();await producer.connect();
  assert.equal((await db.query('SELECT max(version) AS version FROM schema_migrations')).rows[0].version,22);
  assert.equal((await db.query('SELECT count(*)::text AS count FROM tenants')).rows[0].count,'0');
  assert.equal((await db.query('SELECT count(*)::text AS count FROM consumer_streams')).rows[0].count,'0');
  assert.equal((await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false);
  const existing=await admin.listTopics();assert.ok(!existing.includes(topic)&&!existing.includes(dlq));
  topicsCreated=true;
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'},{name:'max.message.bytes',value:'128'}]}]});
  const config=(await admin.describeConfigs({resources:[{type:ConfigResourceTypes.TOPIC,name:dlq,configNames:['max.message.bytes']}]})).resources[0];
  assert.equal(config.configEntries.find(c=>c.configName==='max.message.bytes').configValue,'128');
  const tenant=randomUUID(),project=randomUUID();
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED DLQ ACK CHECK']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED DLQ ACK CHECK']);
  startWorker();await until('Worker ready and initial PG checkpoints',async()=>ready&&(await progress()).length===2);
  const event={event_id:randomUUID(),event_name:'dlq_ack_boundary',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true},context:{source:'isolated-dlq-ack'},schema_version:1};
  const envelope=event=>({tenant_id:tenant,project_id:project,received_at:new Date().toISOString(),event});
  await producer.send({topic,acks:-1,messages:[{partition:0,key:project,value:JSON.stringify(envelope({...event,event_id:'invalid-id'}))},{partition:0,key:project,value:JSON.stringify(envelope(event))}]});
  await until('actual broker rejection of DLQ record',()=>brokerRejections>0);
  await until('Worker halts without skipping rejected DLQ',()=>worker.exitCode!==null&&worker.exitCode!==0);
  const before=await snapshot();blocked(before);await sleep(2000);const second=await snapshot();blocked(second);
  assert.deepEqual(second.rows,before.rows);assert.deepEqual(second.committed,before.committed);
  // Reap the halted process before changing the fault. Recovery starts a new
  // Worker, so this does not claim same-process retry or process availability.
  await stopWorker();
  await admin.alterConfigs({validateOnly:false,resources:[{type:ConfigResourceTypes.TOPIC,name:dlq,configEntries:[{name:'min.insync.replicas',value:'1'},{name:'max.message.bytes',value:'1048588'}]}]});
  metrics=[];startWorker();
  await until('recovery through poison and following valid event',async()=>{const rows=await progress();return rows.find(r=>r.partition_id===0)?.next_offset==='2';},60000);
  await until('Kafka commit after PG checkpoint',async()=>{const s=await snapshot();return s.committed.find(r=>r.partition===0)?.offset==='2';});
  const after=await snapshot();assert.deepEqual(after.counts,{raw:'1',dedup:'1',dlq:'1'});assert.equal(after.dead[0].high,'1');
  const dead=(await db.query("SELECT id,reason,payload->'source'->>'topic' AS source_topic,(payload->'source'->>'partition')::int AS source_partition,payload->'source'->>'offset' AS source_offset FROM dlq_records")).rows[0];
  assert.equal(dead.reason,'invalid_event_id');assert.equal(dead.source_topic,topic);assert.equal(dead.source_partition,0);assert.equal(dead.source_offset,'0');assert.equal(dead.id,createHash('sha256').update(`${topic}:0:0`).digest('hex'));
  assert.equal((await db.query('SELECT event_id::text FROM events')).rows[0].event_id,event.event_id);
  assert.equal((await db.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
  console.log(JSON.stringify({passed:true,fault:'dlq_max_message_bytes_128',real_worker:true,broker_dlq_rejections:brokerRejections,dlq_publish_unconfirmed_observed:true,blocked_snapshots:2,source_messages:2,pg_checkpoint_during_fault:'0',kafka_commit_during_fault:before.committed,following_valid_event_blocked:true,recovery:'new_worker_after_dlq_size_limit_restored',after,exact_source_and_event_ids:true}));
}catch(error){
  await writeFile('/drill-evidence/worker.log',lines+'\n'+JSON.stringify({ready,brokerRejections,metrics}),{mode:0o600}).catch(()=>{});
  throw error;
}finally{
  await stopWorker();
  try{await producer.disconnect();}finally{
    try{if(topicsCreated){try{await admin.deleteGroups([group]);}catch{}await admin.deleteTopics({topics:[topic,dlq],timeout:20000});}}finally{await admin.disconnect();await db.end();}
  }
}
