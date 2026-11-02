// Run only via the guarded remote controller, against an empty temporary DB.
// This uses the actual deployed Worker and broker; no producer mocks.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import {writeFile,readFile} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel}=kafkaPackage;

assert.equal(process.env.CCPANEL_ISOLATED_CHECKPOINT_KILL_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.checkpointkill\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const workerUrl=new URL(url);workerUrl.hostname='127.0.0.1';workerUrl.port='15432';workerUrl.searchParams.set('sslmode','disable');
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin(),producer=kafka.producer();let proxy,proxyExit,worker,workerExit,lines='',metrics=[],databaseRetries=0,ready=false,topicsCreated=false;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(label,check,ms=45000){const end=Date.now()+ms;do{if(await check())return;await sleep(500);}while(Date.now()<end);throw Error('Timed out: '+label);}
async function stopWorker(){if(!worker)return;const child=worker;worker=undefined;if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),6000);try{await workerExit;}finally{clearTimeout(timer);}}}
function startWorker(){
  ready=false;worker=spawn(process.execPath,['--max-old-space-size=40','dist/worker.mjs'],{env:{...process.env,DATABASE_URL:workerUrl.toString()},stdio:['ignore','pipe','pipe']});
  workerExit=once(worker,'exit');
  const listen=stream=>{let remainder='';stream.on('data',chunk=>{remainder+=chunk;const parts=remainder.split('\n');remainder=parts.pop();for(const line of parts){try{const value=JSON.parse(line);if(value.code==='worker_ready')ready=true;if(value.code==='worker_metrics'){metrics.push(value);metrics=metrics.slice(-12);}if(value.code==='database_write_retry')databaseRetries++;}catch{}}});};
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
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'}]}]});
  const tenant=randomUUID(),project=randomUUID();
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED CHECKPOINT KILL CHECK']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED CHECKPOINT KILL CHECK']);
  const exists=async path=>{try{return await readFile(path,'utf8');}catch(e){if(e.code==='ENOENT')return null;throw e;}};
  assert.equal(await exists('/control/server-committed'),null,'stale proxy marker');
  proxy=spawn(process.execPath,['--max-old-space-size=16','scripts/checkpoint-commit-proxy.mjs'],{env:process.env,stdio:['ignore','ignore','pipe']});proxyExit=once(proxy,'exit');proxy.stderr.on('data',chunk=>{lines=(lines+chunk.toString()).slice(-16384);});
  await until('actual proxy listener ready',async()=>!!(await exists('/control/proxy-ready')));
  startWorker();await until('current Worker ready and PG progress initialized',async()=>ready&&(await progress()).length===2);
  const event={event_id:randomUUID(),event_name:'checkpoint_commit_kill',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true},context:{source:'isolated-checkpoint-kill'},schema_version:1},following={...event,event_id:randomUUID()};
  const envelope=event=>({tenant_id:tenant,project_id:project,received_at:new Date().toISOString(),event});
  await producer.send({topic,acks:-1,messages:[event,following].map(e=>({partition:0,key:project,value:JSON.stringify(envelope(e))}))});
  await until('proxy received actual PG checkpoint COMMIT acknowledgement',async()=>!!(await exists('/control/server-committed')));
  assert.deepEqual(JSON.parse(await exists('/control/server-committed')),{checkpoint_commit_confirmed:true,response_withheld:true});
  const blocked=await snapshot();
  assert.deepEqual(blocked.rows,[{partition_id:0,next_offset:'1'},{partition_id:1,next_offset:'0'}]);assert.ok(blocked.committed.every(p=>['-1','0'].includes(p.offset)));assert.deepEqual(blocked.counts,{raw:'1',dedup:'1',dlq:'0'});assert.equal(blocked.source.find(p=>p.partition===0).high,'2');assert.equal(blocked.dead[0].high,'0');assert.equal((await db.query('SELECT count(*)::int AS n FROM aggregation_jobs')).rows[0].n,1);
  await sleep(1000);const second=await snapshot();assert.deepEqual(second.rows,blocked.rows);assert.deepEqual(second.committed,blocked.committed);assert.deepEqual(second.counts,blocked.counts);
  assert.equal(worker.exitCode,null);assert.equal(proxy.exitCode,null);const oldPid=worker.pid;assert.equal(worker.kill('SIGKILL'),true);const result=await workerExit;assert.equal(result[1],'SIGKILL');await stopWorker();
  const afterKill=await snapshot();assert.deepEqual(afterKill.rows,blocked.rows);assert.deepEqual(afterKill.committed,blocked.committed);assert.deepEqual(afterKill.counts,blocked.counts);
  // Proxy withholds exactly once. Its next Worker connection is forwarded.
  startWorker();assert.notEqual(worker.pid,oldPid);await until('new current Worker recovers from durable PG checkpoint',async()=>ready&&(await progress()).find(p=>p.partition_id===0)?.next_offset==='2',60000);
  await until('Kafka reaches recovered checkpoint',async()=>(await snapshot()).committed.find(p=>p.partition===0)?.offset==='2');
  const final=await snapshot();assert.deepEqual(final.counts,{raw:'2',dedup:'2',dlq:'0'});assert.equal(final.dead[0].high,'0');assert.equal((await db.query('SELECT count(*)::int AS n FROM aggregation_jobs')).rows[0].n,2);
  assert.deepEqual((await db.query('SELECT event_id::text FROM events ORDER BY event_id')).rows.map(r=>r.event_id),[event.event_id,following.event_id].sort());
  await until('two exact aggregate contributions',async()=>{const r=(await db.query('SELECT (SELECT count(*)::int FROM aggregation_jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL) AS processed,(SELECT coalesce(sum(event_count),0)::int FROM visible_event_count_buckets) AS contributions')).rows[0];return r.processed===2&&r.contributions===2;});
  const duplicates=Number((await db.query("SELECT coalesce(sum(value),0)::text AS n FROM ingest_metrics WHERE metric='duplicates'")).rows[0].n);assert.equal(duplicates,1);assert.equal((await db.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
  console.log(JSON.stringify({passed:true,real_current_worker:true,server_checkpoint_commit_confirmed:true,commit_response_withheld:true,durable_pg_before_kill:'1',kafka_before_kill:blocked.committed,actual_signal:'SIGKILL',new_worker_pid:true,raw:2,dedup:2,jobs:2,processed:2,aggregate_contributions:2,duplicates,dlq:0,final}));
}catch(error){
  await writeFile('/drill-evidence/worker.log',lines+'\n'+JSON.stringify({ready,databaseRetries,metrics}),{mode:0o600}).catch(()=>{});
  throw error;
}finally{
  await stopWorker();if(proxy&&proxy.exitCode===null&&proxy.signalCode===null){proxy.kill('SIGTERM');const timer=setTimeout(()=>proxy.kill('SIGKILL'),6000);try{await proxyExit;}finally{clearTimeout(timer);}}
  try{await producer.disconnect();}finally{
    try{if(topicsCreated){try{await admin.deleteGroups([group]);}catch{}await admin.deleteTopics({topics:[topic,dlq],timeout:20000});}}finally{await admin.disconnect();await db.end();}
  }
}
