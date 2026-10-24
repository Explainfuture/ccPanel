// Run only via the guarded remote controller, against an empty temporary DB.
// This uses the actual deployed Worker and broker; no producer mocks.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import {writeFile} from 'node:fs/promises';
import pg from 'pg';
import kafkaPackage from 'kafkajs';
const {Kafka,logLevel}=kafkaPackage;

assert.equal(process.env.CCPANEL_ISOLATED_PG_RETRY_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.pgretry\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin(),producer=kafka.producer();let worker,workerExit,lines='',metrics=[],databaseRetries=0,ready=false,topicsCreated=false;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(label,check,ms=45000){const end=Date.now()+ms;do{if(await check())return;await sleep(500);}while(Date.now()<end);throw Error('Timed out: '+label);}
async function stopWorker(){if(!worker)return;const child=worker;worker=undefined;if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),6000);try{await workerExit;}finally{clearTimeout(timer);}}}
function startWorker(){
  ready=false;worker=spawn(process.execPath,['--max-old-space-size=40','dist/worker.mjs'],{env:process.env,stdio:['ignore','pipe','pipe']});
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
  await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'min.insync.replicas',value:'1'}]}]});
  const tenant=randomUUID(),project=randomUUID();
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED PG RETRY CHECK']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED PG RETRY CHECK']);
  const event={event_id:randomUUID(),event_name:'transaction_retry_boundary',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true},context:{source:'isolated-pg-retry'},schema_version:1},following={...event,event_id:randomUUID()};
  await db.query(`CREATE TABLE retry_control(event_id uuid PRIMARY KEY,enabled boolean NOT NULL);CREATE SEQUENCE retry_attempts;
    CREATE FUNCTION fail_aggregation_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF EXISTS(SELECT 1 FROM retry_control WHERE event_id=NEW.event_id AND enabled) THEN
        PERFORM nextval('retry_attempts');RAISE EXCEPTION 'isolated transient transaction fault' USING ERRCODE='40001';
      END IF;RETURN NEW;END $$;
    CREATE TRIGGER retry_boundary BEFORE INSERT ON aggregation_jobs FOR EACH ROW EXECUTE FUNCTION fail_aggregation_insert();`);
  await db.query('INSERT INTO retry_control VALUES($1,true)',[event.event_id]);
  const attempts=async()=>{const r=(await db.query('SELECT last_value::int AS value,is_called FROM retry_attempts')).rows[0];return r.is_called?r.value:0;};
  const jobs=async()=>Number((await db.query('SELECT count(*)::int AS count FROM aggregation_jobs')).rows[0].count);
  startWorker();await until('Worker ready and initial PG checkpoints',async()=>ready&&(await progress()).length===2);const originalPid=worker.pid;
  const alive=()=>worker.pid===originalPid&&worker.exitCode===null&&worker.signalCode===null;
  const lease=async()=>{
    const rows=(await db.query("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid=0 AND objid=169202617 AND objsubid=1 AND mode='ShareLock' AND granted")).rows;
    assert.equal(rows.length,1,'actual shared recovery lease missing');return rows[0].pid;
  };
  const leaseBackend=await lease();
  const envelope=event=>({tenant_id:tenant,project_id:project,received_at:new Date().toISOString(),event});
  await producer.send({topic,acks:-1,messages:[event,following].map(e=>({partition:0,key:project,value:JSON.stringify(envelope(e))}))});
  await until('actual retrying trigger after raw and dedup writes',async()=>databaseRetries>=2&&(await attempts())>=2);
  assert.ok(alive(),'Worker replaced or exited during temporary transaction fault');
  const before=await snapshot();blocked(before);assert.equal(await lease(),leaseBackend);assert.equal(await jobs(),0);await sleep(2200);const second=await snapshot();blocked(second);assert.equal(await jobs(),0);assert.ok(alive());assert.equal(await lease(),leaseBackend);assert.deepEqual(second.committed,before.committed);
  const failureAttempts=await attempts();assert.ok(failureAttempts>=2);
  await db.query('UPDATE retry_control SET enabled=false');
  await until('same Worker recovers both ordered events',async()=>alive()&&(await progress()).find(r=>r.partition_id===0)?.next_offset==='2',60000);
  await until('Kafka commit after PG checkpoint',async()=>(await snapshot()).committed.find(r=>r.partition===0)?.offset==='2');
  const after=await snapshot();assert.deepEqual(after.counts,{raw:'2',dedup:'2',dlq:'0'});assert.equal(after.dead[0].high,'0');assert.equal(await jobs(),2);
  const ids=(await db.query('SELECT event_id::text FROM events ORDER BY event_id')).rows.map(r=>r.event_id);assert.deepEqual(ids,[event.event_id,following.event_id].sort());
  await until('exactly two completed aggregate contributions',async()=>{const r=(await db.query('SELECT (SELECT count(*)::int FROM aggregation_jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL) AS processed,(SELECT coalesce(sum(event_count),0)::int FROM visible_event_count_buckets) AS contributions')).rows[0];return r.processed===2&&r.contributions===2;});
  assert.ok(alive());assert.equal(await lease(),leaseBackend);assert.equal((await db.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
  console.log(JSON.stringify({passed:true,fault:'SQLSTATE40001_before_aggregation_insert',real_worker:true,same_worker_pid:true,same_shared_recovery_lease_backend:true,process_restarts:0,transaction_failure_attempts:failureAttempts,database_retry_logs:databaseRetries,blocked_snapshots:2,raw_dedup_jobs_during_fault:0,following_event_blocked:true,pg_checkpoint_during_fault:'0',kafka_commit_during_fault:before.committed,recovery:'same_worker_after_trigger_switch_disabled',after,exact_event_ids:true,processed:2,aggregate_contributions:2}));
}catch(error){
  await writeFile('/drill-evidence/worker.log',lines+'\n'+JSON.stringify({ready,databaseRetries,metrics}),{mode:0o600}).catch(()=>{});
  throw error;
}finally{
  await stopWorker();
  try{await producer.disconnect();}finally{
    try{if(topicsCreated){try{await admin.deleteGroups([group]);}catch{}await admin.deleteTopics({topics:[topic,dlq],timeout:20000});}}finally{await admin.disconnect();await db.end();}
  }
}
