// Fresh isolated PG, private topics/group, actual staged API/Worker and Kafka.
import pg from 'pg';
import { Kafka,logLevel } from 'kafkajs';
import { randomUUID,randomBytes,createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1}),mode=process.argv[2],prefix=process.env.CCPANEL_REPLAY_PREFIX;
assert.match(prefix||'',/^ccpanel\.restore\.[a-f0-9]{12}$/);
const topic=prefix+'.events',dlq=prefix+'.dlq',group=prefix+'.writer';
const kafka=new Kafka({clientId:'live-replay-test',brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING});
const hash=s=>createHash('sha256').update(s).digest('hex');
async function waitFor(fn,label){const end=Date.now()+60000;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,300));}throw new Error('live_replay_timeout:'+label);}
async function send(raw,key){const p=kafka.producer();await p.connect();try{await p.send({topic,acks:-1,messages:[{key,value:raw}]});}finally{await p.disconnect();}}
try{
  const database=(await pool.query('SELECT current_database() AS name')).rows[0].name;
  assert.match(database,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  if(mode==='create'){
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0,'fresh isolated database only');
    const f={tenant:randomUUID(),project:randomUUID(),user:randomUUID(),token:randomBytes(24).toString('hex'),csrf:randomBytes(24).toString('hex'),request_id:randomUUID()};
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[f.tenant,'DLQ LIVE TEMP']);await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[f.project,f.tenant,'DLQ LIVE TEMP']);
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[f.user,`live-replay-${f.user}@integration.invalid`,'unused-test-hash']);
    await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[f.tenant,f.user]);
    await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[hash(f.token),f.user,hash(f.csrf)]);
    await pool.query("ALTER TABLE events ADD CONSTRAINT live_replay_failure CHECK(event_name<>'dlq_live_repair')");
    f.record={tenant_id:f.tenant,project_id:f.project,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'dlq_live_repair',distinct_id:'synthetic-live-replay',event_time:new Date().toISOString(),properties:{test:true},context:{source:'isolated-live-replay'},schema_version:1}};
    f.raw_base64=Buffer.from(JSON.stringify(f.record,null,2)).toString('base64');
    console.log(JSON.stringify(f));
  }else if(mode==='cleanup'){
    const admin=kafka.admin();await admin.connect();try{
      for(let i=0;i<80;i++){try{await admin.deleteGroups([group]);break;}catch(e){if(i===79)throw e;await new Promise(r=>setTimeout(r,500));}}
      await admin.deleteTopics({topics:[topic,dlq]});console.log('Isolated live replay topics/group removed');
    }finally{await admin.disconnect();}
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    assert.equal((await pool.query("SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE t.id=$1 AND p.id=$2 AND t.name='DLQ LIVE TEMP' AND p.name='DLQ LIVE TEMP'",[f.tenant,f.project])).rowCount,1);
    if(mode==='prepare'){
      const admin=kafka.admin();await admin.connect();try{await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[{name:'retention.ms',value:'600000'},{name:'retention.bytes',value:'1048576'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'retention.ms',value:'600000'},{name:'retention.bytes',value:'1048576'}]}]});}finally{await admin.disconnect();}
    }else if(mode==='verify'){
      const base=process.env.TEST_API,root=`${base}/api/tenants/${f.tenant}/projects/${f.project}`,headers={cookie:`ccpanel_session=${f.token}`,origin:process.env.PUBLIC_ORIGIN,'content-type':'application/json','x-csrf-token':f.csrf};
      const api=(path,body)=>fetch(root+path,{method:body?'POST':'GET',headers,...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(10000)});
      await waitFor(async()=>{try{return(await fetch(base+'/ready',{signal:AbortSignal.timeout(3000)})).status===200;}catch{return false;}},'API ready');
      const raw=Buffer.from(f.raw_base64,'base64');await send(raw,f.project+':'+f.record.event.distinct_id);
      await waitFor(async()=>(await pool.query('SELECT 1 FROM dlq_records WHERE tenant_id=$1',[f.tenant])).rowCount===1,'initial dead letter');
      const source=(await pool.query('SELECT * FROM dlq_records WHERE tenant_id=$1',[f.tenant])).rows[0];assert.equal(source.reason,'invalid_storage_value');assert.equal(source.payload.raw_base64,f.raw_base64);
      assert.equal((await pool.query('SELECT status FROM event_dedup WHERE project_id=$1 AND event_id=$2',[f.project,f.record.event.event_id])).rows[0].status,'failed');
      await pool.query('ALTER TABLE events DROP CONSTRAINT live_replay_failure');
      const request={request_id:f.request_id,record_ids:[source.id],reason:'Synthetic live processor repair'};
      assert.equal((await api('/dlq/replays',request)).status,202);
      async function completed(id,expected){let job;await waitFor(async()=>{const r=await api('/dlq/replays/'+id);assert.equal(r.status,200);job=await r.json();if(job.status==='failed'&&expected!=='failed')assert.fail(JSON.stringify(job.items.map(i=>({status:i.status,error:i.error}))));return job.status===expected;},'task '+expected);return job;}
      const done=await completed(request.request_id,'completed');assert.equal(done.items[0].status,'stored');
      assert.equal((await api('/dlq/replays',request)).status,200);
      const duplicate={...request,request_id:randomUUID()};assert.equal((await api('/dlq/replays',duplicate)).status,202);assert.equal((await completed(duplicate.request_id,'completed')).items[0].status,'duplicate');
      const counts=(await pool.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM aggregation_jobs) AS aggregate,(SELECT count(*)::int FROM event_dedup WHERE status=$1) AS stored',[ 'stored' ])).rows[0];assert.deepEqual(counts,{raw:1,aggregate:1,stored:1});
      assert.equal((await pool.query('SELECT payload FROM dlq_records WHERE id=$1',[source.id])).rows[0].payload.raw_base64,f.raw_base64);
      // A second, still-broken original must produce a linked failure outcome,
      // never a fabricated successful replay just because Kafka accepted it.
      await pool.query("ALTER TABLE events ADD CONSTRAINT live_replay_failure CHECK(properties->>'stage' IS DISTINCT FROM 'second')");
      const bad={...f.record,event:{...f.record.event,event_id:randomUUID(),properties:{test:true,stage:'second'}}};await send(Buffer.from(JSON.stringify(bad)),f.project+':'+bad.event.distinct_id);
      let badSource;await waitFor(async()=>{badSource=(await pool.query("SELECT * FROM dlq_records WHERE tenant_id=$1 AND id<>$2 ORDER BY created_at LIMIT 1",[f.tenant,source.id])).rows[0];return !!badSource;},'second original DLQ');
      const failed={request_id:randomUUID(),record_ids:[badSource.id],reason:'Synthetic unchanged processor failure'};assert.equal((await api('/dlq/replays',failed)).status,202);
      const rejected=await completed(failed.request_id,'failed');assert.equal(rejected.items[0].error,'consumer_rejected');assert.match(rejected.items[0].outcome_dlq_id,/^[a-f0-9]{64}$/);
      assert.equal((await pool.query('SELECT 1 FROM dlq_records WHERE id=$1 AND tenant_id=$2',[rejected.items[0].outcome_dlq_id,f.tenant])).rowCount,1);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM events')).rows[0].n,1);
      const audits=(await pool.query("SELECT action,count(*)::int AS n FROM audit_logs WHERE tenant_id=$1 AND action LIKE 'dlq_replay_%' GROUP BY action",[f.tenant])).rows;
      const values=Object.fromEntries(audits.map(r=>[r.action,r.n]));assert.equal(values.dlq_replay_requested,3);assert.equal(values.dlq_replay_published,2);assert.equal(values.dlq_replay_finished,3);
      let progressVerified=false;
      if((await pool.query("SELECT to_regclass('consumer_progress') AS relation")).rows[0].relation){
        const admin=kafka.admin();await admin.connect();try{
          await waitFor(async()=>{
            const commits=(await admin.fetchOffsets({groupId:group,topics:[topic],resolveOffsets:false})).find(t=>t.topic===topic)?.partitions;
            const highs=await admin.fetchTopicOffsets(topic);
            const rows=(await pool.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2',[topic,group])).rows;
            return highs.length===2&&rows.length===2&&highs.every(p=>rows.some(r=>r.partition_id===p.partition&&r.next_offset===p.high)&&commits?.some(c=>c.partition===p.partition&&(c.offset===p.high||(p.high==='0'&&c.offset==='-1'))));
          },'PG checkpoints and Kafka commits caught up');
          assert.equal((await pool.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
          progressVerified=true;
        }finally{await admin.disconnect();}
      }
      console.log(JSON.stringify({passed:true,live_api_worker_kafka:true,first_failure_registered:true,raw_bytes_preserved:true,repaired_stored_once:true,duplicate_task_no_extra_write:true,request_idempotency:true,source_preserved:true,unrepaired_outcome_linked:true,progress_verified:progressVerified,audit_counts:values,raw_events:1,aggregation_jobs:1}));
    }else throw new Error('invalid_live_replay_mode');
  }
}finally{await pool.end();}
