import pg from 'pg';
import assert from 'node:assert/strict';
import { randomUUID,randomBytes,scryptSync,createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { Kafka,logLevel } from 'kafkajs';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1}),mode=process.argv[2],origin=process.env.PUBLIC_ORIGIN,base=process.env.TEST_API||'http://api:3000';
const label='PRIVACY RESTORE TEMP';
async function account(tenant){const id=randomUUID(),email=`privacy-restore-${id}@integration.invalid`,password=randomBytes(24).toString('hex'),salt=randomBytes(16).toString('hex');await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,email,salt+':'+scryptSync(password,salt,64).toString('hex')]);await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,id]);return{id,email,password};}
async function login(user){const r=await fetch(base+'/api/auth/login',{method:'POST',headers:{origin,'content-type':'application/json'},body:JSON.stringify({email:user.email,password:user.password})});assert.equal(r.status,200);const b=await r.json();return{cookie:r.headers.get('set-cookie').split(';')[0],origin,'content-type':'application/json','x-csrf-token':b.csrf_token};}
async function waitFor(fn){const end=Date.now()+30000;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,250));}throw new Error('privacy_restore_fixture_timeout');}
async function replay(mode,f){
  const prefix=process.env.CCPANEL_REPLAY_PREFIX;assert.match(prefix||'',/^ccpanel\.restore\.[a-f0-9]{12}$/);
  const topic=prefix+'.events',dlq=prefix+'.dlq',group=prefix+'.writer';
  const kafka=new Kafka({clientId:'restore-fixture',brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING}),admin=kafka.admin();await admin.connect();
  try{
    if(mode==='replay-prepare'){
      await admin.createTopics({waitForLeaders:true,topics:[{topic,numPartitions:2,replicationFactor:1,configEntries:[{name:'retention.ms',value:'600000'},{name:'retention.bytes',value:'1048576'}]},{topic:dlq,numPartitions:1,replicationFactor:1,configEntries:[{name:'retention.ms',value:'600000'},{name:'retention.bytes',value:'1048576'}]}]});
      const producer=kafka.producer();await producer.connect();try{
        const records=[...f.events,{...f.events[2],event_id:'invalid-restored-id'}].map(event=>({tenant_id:f.tenant,project_id:f.project,received_at:event.event_time,event}));
        const messages=records.map(record=>({key:f.project+':'+record.event.distinct_id,value:JSON.stringify(record)}));
        if(f.dead_raw_base64)messages.push({key:f.project+':restore-victim',value:Buffer.from(f.dead_raw_base64,'base64')});
        await producer.send({topic,acks:-1,messages});
      }finally{await producer.disconnect();}
      console.log('Original/retry/poison and historical DLQ records prepared on isolated recovery topics');
    }else if(mode==='replay-verify'){
      let observed;
      await waitFor(async()=>{
        const metrics=await pool.query("SELECT metric,value FROM ingest_metrics WHERE tenant_id=$1 AND project_id=$2",[f.tenant,f.project]);const values=Object.fromEntries(metrics.rows.map(r=>[r.metric,Number(r.value)]));observed=values;
        if((values.deletion_suppressed||0)<(f.dead_raw_base64?5:4)||(values.duplicates||0)<1)return false;
        const high=await admin.fetchTopicOffsets(topic),offsets=(await admin.fetchOffsets({groupId:group,topics:[topic]}))[0].partitions;
        return high.every(p=>{const committed=BigInt(offsets.find(r=>r.partition===p.partition)?.offset??'-1');return committed===BigInt(p.high)||(BigInt(p.high)===0n&&committed<0n);});
      });
      assert.equal(Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[f.tenant])).rows[0].count),1);
      const dead=await pool.query('SELECT payload FROM dlq_records WHERE tenant_id=$1',[f.tenant]);assert.equal(dead.rowCount,f.dead_raw_base64?1:0);if(dead.rowCount)assert.equal(dead.rows[0].payload.redacted,true);
      console.log(JSON.stringify({recovery_replay_passed:true,suppressed:observed.deletion_suppressed,duplicates:observed.duplicates,group_offsets_caught_up:true,raw_events:1}));
    }else if(mode==='replay-cleanup'){
      let done=false;
      for(let n=0;n<80&&!done;n++){try{await admin.deleteGroups([group]);done=true;}catch(e){if(n===79)throw e;await new Promise(r=>setTimeout(r,500));}}
      await admin.deleteTopics({topics:[topic,dlq]});console.log('Isolated replay topics and group removed');
    }else throw new Error('unknown_replay_mode');
  }finally{await admin.disconnect();}
}
try{
  if(mode==='create'){
    const tenant=randomUUID(),project=randomUUID(),key=randomBytes(24).toString('hex'),anonymous=randomUUID();
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,label]);const user=await account(tenant);
    await pool.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[project,tenant,label,[origin]]);
    await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(key).digest('hex'),'restore-fixture']);
    const event=(name,identity,extra={})=>({event_id:randomUUID(),event_name:name,distinct_id:identity,event_time:new Date(Date.now()-86400000).toISOString(),properties:{test:true},context:{source:'privacy-restore-drill'},schema_version:1,...extra});
    const events=[event('restore_anon',anonymous),event('$identify','restore-victim',{anonymous_id:anonymous}),event('restore_purchase','restore-victim',{anonymous_id:anonymous}),event('restore_safe','unrelated')];
    const sent=await fetch(base+'/v1/batch',{method:'POST',headers:{origin,'content-type':'text/plain'},body:JSON.stringify({write_key:key,schema_version:1,events})});assert.equal(sent.status,202);
    await waitFor(async()=>Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1 AND project_id=$2',[tenant,project])).rows[0].count)===4);
    const conflict={...events[2],properties:{test:true,conflict:true}};
    const conflicting=await fetch(base+'/v1/batch',{method:'POST',headers:{origin,'content-type':'text/plain'},body:JSON.stringify({write_key:key,schema_version:1,events:[conflict]})});assert.equal(conflicting.status,202);
    await waitFor(async()=>(await pool.query("SELECT 1 FROM dlq_records WHERE tenant_id=$1 AND project_id=$2 AND reason='payload_conflict'",[tenant,project])).rowCount===1);
    const dead=(await pool.query("SELECT payload FROM dlq_records WHERE tenant_id=$1 AND project_id=$2 AND reason='payload_conflict'",[tenant,project])).rows[0].payload;assert.equal(typeof dead.raw_base64,'string');
    // Only synthetic fixture metadata; passwords and keys never leave memory.
    console.log(JSON.stringify({tenant,project,user:{id:user.id},events,dead_raw_base64:dead.raw_base64}));
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    assert.equal((await pool.query('SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE t.id=$1 AND p.id=$2 AND t.name=$3 AND p.name=$3',[f.tenant,f.project,label])).rowCount,1,'isolated fixture scope');
    if(mode.startsWith('replay-')){
      const dbName=(await pool.query('SELECT current_database() AS name')).rows[0].name;assert.match(dbName,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
      await replay(mode,f);
    }else if(mode==='delete'){
      // This actor is created AFTER the backup and intentionally absent there.
      const actor=await account(f.tenant),headers=await login(actor);
      const root=`${base}/api/tenants/${f.tenant}/projects/${f.project}`;
      const r=await fetch(root+'/deletions',{method:'POST',headers,body:JSON.stringify({scope:'user',distinct_id:'restore-victim',confirmation:'restore-victim',reason:'Controlled old backup recovery drill'})});assert.equal(r.status,202);
      const id=(await r.json()).id;
      await waitFor(async()=>{const result=await(await fetch(root+'/deletions',{headers})).json();const job=result.jobs.find(j=>j.id===id);assert.notEqual(job?.status,'failed',job?.error);return job?.status==='completed';});
      assert.equal(Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[f.tenant])).rows[0].count),1);console.log(JSON.stringify({deleted_events:3,actor_created_after_backup:true}));
    }else if(mode==='before'||mode==='after'){
      const dbName=(await pool.query('SELECT current_database() AS name')).rows[0].name;assert.match(dbName,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
      const raw=Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[f.tenant])).rows[0].count);
      assert.equal(raw,mode==='before'?4:1);
      if(mode==='before')assert.equal((await pool.query('SELECT 1 FROM recovery_gate WHERE blocked=false')).rowCount,0,'restored DB is closed by default');
      else{
        assert.equal((await pool.query('SELECT 1 FROM recovery_gate WHERE blocked=false')).rowCount,1);
        const jobs=await pool.query("SELECT id,user_id,status,removed_events FROM deletion_jobs WHERE tenant_id=$1 AND project_id=$2 AND scope='user'",[f.tenant,f.project]);assert.equal(jobs.rowCount,1);assert.equal(jobs.rows[0].user_id,null);assert.equal(jobs.rows[0].status,'completed');assert.equal(Number(jobs.rows[0].removed_events),3);
        assert.equal(Number((await pool.query('SELECT count(*) FROM aggregation_jobs WHERE tenant_id=$1',[f.tenant])).rows[0].count),1);
        assert.equal(Number((await pool.query('SELECT count(*) FROM event_dedup WHERE tenant_id=$1',[f.tenant])).rows[0].count),4);
        const audit=await pool.query("SELECT details->>'source_actor_id' AS actor FROM audit_logs WHERE tenant_id=$1 AND action='deletion_restored_from_ledger' AND details->>'deletion_job_id'=$2",[f.tenant,jobs.rows[0].id]);assert.equal(audit.rowCount,1);assert.notEqual(audit.rows[0].actor,f.user.id);assert.equal((await pool.query('SELECT 1 FROM users WHERE id=$1',[audit.rows[0].actor])).rowCount,0);
        assert.equal((await pool.query('SELECT 1 FROM events WHERE tenant_id=$1 AND event_id=ANY($2::uuid[])',[f.tenant,f.events.slice(0,3).map(e=>e.event_id)])).rowCount,0);
        if(f.dead_raw_base64){const oldDead=await pool.query('SELECT payload FROM dlq_records WHERE tenant_id=$1',[f.tenant]);assert.equal(oldDead.rowCount,1);assert.equal(oldDead.rows[0].payload.redacted,true);assert.equal(oldDead.rows[0].payload.raw_base64,undefined);}
      }
      console.log(JSON.stringify({phase:mode,raw_events:raw,passed:true}));
    }else if(mode==='cleanup'){
      const members=(await pool.query('SELECT user_id FROM tenant_memberships WHERE tenant_id=$1',[f.tenant])).rows.map(r=>r.user_id);
      const safe=await pool.query("SELECT id FROM users WHERE id=ANY($1::uuid[]) AND email LIKE 'privacy-restore-%@integration.invalid'",[members]);assert.equal(safe.rowCount,members.length,'only temporary fixture accounts');
      assert.equal((await pool.query('SELECT 1 FROM tenant_memberships WHERE user_id=ANY($1::uuid[]) AND tenant_id<>$2',[members,f.tenant])).rowCount,0,'no shared real accounts');
      for(const table of ['replay_items','replay_jobs','recovery_import_scopes','deletion_barriers','deletion_jobs','query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects']){
        if(['replay_items','replay_jobs','recovery_import_scopes'].includes(table)&&!(await pool.query('SELECT to_regclass($1) AS relation',['public.'+table])).rows[0].relation)continue;
        await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
      }
      await pool.query('DELETE FROM sessions WHERE user_id=ANY($1::uuid[])',[members]);await pool.query('DELETE FROM users WHERE id=ANY($1::uuid[])',[members]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);console.log('Privacy restore fixture cleaned');
    }else throw new Error('unknown_fixture_mode');
  }
}finally{await pool.end();}
