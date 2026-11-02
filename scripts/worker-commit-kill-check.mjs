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

assert.equal(process.env.CCPANEL_ISOLATED_COMMIT_KILL_CHECK,'true');
const url=new URL(process.env.DATABASE_URL);
assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const topic=process.env.KAFKA_EVENTS_TOPIC,dlq=process.env.KAFKA_DLQ_TOPIC,group=process.env.KAFKA_GROUP_ID;
assert.match(topic??'',/^ccpanel\.drill\.commitkill\.[a-f0-9]{16}\.events$/);
const prefix=topic.slice(0,-7);assert.equal(dlq,prefix+'.dlq');assert.equal(group,prefix+'.writer');
const db=new pg.Client({connectionString:url.toString(),statement_timeout:5000});
const kafka=new Kafka({clientId:prefix,brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}});
const admin=kafka.admin(),producer=kafka.producer();let holder,worker,workerExit,lines='',metrics=[],databaseRetries=0,ready=false,topicsCreated=false;
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
  await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,'ISOLATED COMMIT KILL CHECK']);
  await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[project,tenant,'ISOLATED COMMIT KILL CHECK']);
  holder=new pg.Client({connectionString:url.toString()});await holder.connect();const lock=169202632;
  await db.query(`CREATE TABLE crash_control(phase text,event_id uuid,checkpoint_next bigint,enabled boolean NOT NULL);
    CREATE FUNCTION commit_boundary_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF TG_TABLE_NAME='aggregation_jobs' THEN
        IF EXISTS(SELECT 1 FROM crash_control WHERE phase='before_raw_commit' AND enabled AND event_id=NEW.event_id) THEN PERFORM pg_advisory_xact_lock(169202632);END IF;
      ELSIF TG_TABLE_NAME='consumer_progress' THEN
        IF EXISTS(SELECT 1 FROM crash_control WHERE phase='after_raw_commit' AND enabled AND checkpoint_next=NEW.next_offset) AND NEW.partition_id=0 THEN PERFORM pg_advisory_xact_lock(169202632);END IF;
      END IF;RETURN NEW;END $$;
    CREATE TRIGGER raw_commit_gate BEFORE INSERT ON aggregation_jobs FOR EACH ROW EXECUTE FUNCTION commit_boundary_gate();
    CREATE TRIGGER progress_commit_gate BEFORE UPDATE ON consumer_progress FOR EACH ROW EXECUTE FUNCTION commit_boundary_gate();`);
  const waiters=async()=>(await db.query("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid=0 AND objid=$1 AND objsubid=1 AND NOT granted",[lock])).rows;
  const jobCount=async()=>Number((await db.query('SELECT count(*)::int AS n FROM aggregation_jobs')).rows[0].n);
  const expected=[],phases=[];let previous=0;
  startWorker();await until('actual Worker ready',async()=>ready&&(await progress()).length===2);
  for(const phase of ['before_raw_commit','after_raw_commit']){
    const event={event_id:randomUUID(),event_name:'commit_kill_probe',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true,phase},context:{source:'isolated-commit-kill'},schema_version:1},following={...event,event_id:randomUUID()};expected.push(event.event_id,following.event_id);
    await db.query('DELETE FROM crash_control');await db.query('INSERT INTO crash_control VALUES($1,$2,$3,true)',[phase,event.event_id,previous+1]);await holder.query('SELECT pg_advisory_lock($1)',[lock]);
    const envelope=event=>({tenant_id:tenant,project_id:project,received_at:new Date().toISOString(),event});
    await producer.send({topic,acks:-1,messages:[event,following].map(e=>({partition:0,key:project,value:JSON.stringify(envelope(e))}))});
    await until('actual native PG lock wait at '+phase,async()=>(await waiters()).length===1);
    const blocked=await snapshot(),visible=previous+(phase==='after_raw_commit'?1:0);
    assert.equal(blocked.rows.find(r=>r.partition_id===0).next_offset,String(previous));assert.ok(blocked.committed.find(r=>r.partition===0).offset===(previous?String(previous):'-1')||(!previous&&blocked.committed.find(r=>r.partition===0).offset==='0'));
    assert.deepEqual(blocked.counts,{raw:String(visible),dedup:String(visible),dlq:'0'});assert.equal(await jobCount(),visible);assert.equal(blocked.source.find(r=>r.partition===0).high,String(previous+2));
    const waiting=await waiters();assert.equal(waiting.length,1,'target left the native fault boundary before kill');const killedPid=worker.pid;
    assert.equal(worker.kill('SIGKILL'),true);const exit=await workerExit;assert.equal(exit[1],'SIGKILL');await stopWorker();
    await until('PG rollback after process death',async()=>(await waiters()).length===0);
    const afterKill=await snapshot();assert.deepEqual(afterKill.counts,blocked.counts);assert.equal(await jobCount(),visible);assert.equal(afterKill.rows.find(r=>r.partition_id===0).next_offset,String(previous));
    await db.query('UPDATE crash_control SET enabled=false');await holder.query('SELECT pg_advisory_unlock($1)',[lock]);
    startWorker();assert.notEqual(worker.pid,killedPid);await until('new Worker replays both events',async()=>ready&&(await progress()).find(r=>r.partition_id===0)?.next_offset===String(previous+2),60000);
    await until('Kafka acknowledges recovered PG progress',async()=>(await snapshot()).committed.find(r=>r.partition===0)?.offset===String(previous+2));
    const recovered=await snapshot();assert.deepEqual(recovered.counts,{raw:String(previous+2),dedup:String(previous+2),dlq:'0'});assert.equal(await jobCount(),previous+2);assert.equal(recovered.dead[0].high,'0');
    const ids=(await db.query('SELECT event_id::text FROM events ORDER BY event_id')).rows.map(r=>r.event_id);assert.deepEqual(ids,[...expected].sort());
    await until('exact aggregate contributions after replay',async()=>{const r=(await db.query('SELECT (SELECT count(*)::int FROM aggregation_jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL) AS processed,(SELECT coalesce(sum(event_count),0)::int FROM visible_event_count_buckets) AS contributions')).rows[0];return r.processed===previous+2&&r.contributions===previous+2;});
    phases.push({phase,actual_signal:'SIGKILL',native_pg_wait_observed:true,pg_checkpoint_before_kill:String(previous),raw_before_and_after_kill:visible,recovered_raw:previous+2});previous+=2;
  }
  const duplicates=Number((await db.query("SELECT coalesce(sum(value),0)::text AS n FROM ingest_metrics WHERE metric='duplicates'")).rows[0].n);assert.equal(duplicates,1,'committed raw event must replay exactly once as duplicate');
  assert.equal((await db.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
  console.log(JSON.stringify({passed:true,real_current_worker:true,kills:2,source_messages:4,phases,raw:4,dedup:4,jobs:4,processed:4,aggregate_contributions:4,duplicates:1,dlq:0,final:await snapshot()}));
}catch(error){
  await writeFile('/drill-evidence/worker.log',lines+'\n'+JSON.stringify({ready,databaseRetries,metrics}),{mode:0o600}).catch(()=>{});
  throw error;
}finally{
  await stopWorker();if(holder)await holder.end();
  try{await producer.disconnect();}finally{
    try{if(topicsCreated){try{await admin.deleteGroups([group]);}catch{}await admin.deleteTopics({topics:[topic,dlq],timeout:20000});}}finally{await admin.disconnect();await db.end();}
  }
}
