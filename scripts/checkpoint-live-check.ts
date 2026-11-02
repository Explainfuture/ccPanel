import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {existsSync,readFileSync,writeFileSync} from 'node:fs';
import {pool} from '../packages/server/src/db';
import {kafka,eventsTopic,dlqTopic,consumerGroup} from '../packages/server/src/kafka';
const mode=process.argv[2],prefix=process.env.CCPANEL_REPLAY_PREFIX!;
assert.match(prefix||'',/^ccpanel\.restore\.[a-f0-9]{12}$/);
assert.equal(eventsTopic,prefix+'.events');assert.equal(dlqTopic,prefix+'.dlq');assert.equal(consumerGroup,prefix+'.writer');
async function until(fn:()=>Promise<boolean>,label:string){const end=Date.now()+90000;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,100));}throw new Error('checkpoint_test_timeout:'+label);}
const admin=kafka.admin();
try{
  if(mode==='worker-env'){
    assert.match(process.env.CCPANEL_PROXY_HOST||'',/^ccpanel-checkpoint-proxy-[a-f0-9]{8}$/);
    const url=new URL(process.env.DATABASE_URL!);assert.match(url.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
    url.hostname=process.env.CCPANEL_PROXY_HOST!;url.port='15432';url.searchParams.set('sslmode','disable');
    // Caller redirects into a mode-600 env file. Never send this to logs.
    console.log('DATABASE_URL='+url.toString());
  }else if(mode==='verify'){
    const database=(await pool.query('SELECT current_database() AS name')).rows[0].name;
    assert.match(database,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
    const f=JSON.parse(readFileSync('/control/fixture.json','utf8')),base=process.env.TEST_API!;
    assert.equal((await pool.query("SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1 AND t.id=$2 AND t.name='PRESSURE LIVE TEMP'",[f.project,f.tenant])).rowCount,1);
    await admin.connect();
    await until(async()=>{try{return(await fetch(base+'/ready',{signal:AbortSignal.timeout(3000)})).ok&&(await pool.query('SELECT count(*)::int AS n FROM consumer_progress WHERE topic=$1 AND group_id=$2 AND next_offset=0',[eventsTopic,consumerGroup])).rows[0].n===2;}catch{return false;}},'initial progress');
    const event={event_id:randomUUID(),event_name:'checkpoint_commit_synthetic',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{test:true},context:{},schema_version:1};
    const response=await fetch(base+'/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body:JSON.stringify({write_key:f.key,project_id:f.project,schema_version:1,events:[event]}),signal:AbortSignal.timeout(10000)});assert.equal(response.status,202);
    await until(async()=>existsSync('/control/server-committed'),'real server COMMIT response withheld');
    const counts=async()=>(await pool.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM aggregation_jobs) AS aggregate')).rows[0];
    assert.deepEqual(await counts(),{raw:1,aggregate:1});
    const progress=(await pool.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2',[eventsTopic,consumerGroup])).rows;
    assert.equal(progress.reduce((n,p)=>n+Number(p.next_offset),0),1,'PG checkpoint actually committed');
    const highs=await admin.fetchTopicOffsets(eventsTopic),active=highs.find(p=>p.high==='1');assert.ok(active);assert.equal(highs.reduce((n,p)=>n+Number(p.high),0),1);
    const commits=(await admin.fetchOffsets({groupId:consumerGroup,topics:[eventsTopic],resolveOffsets:false}))[0]!.partitions;
    assert.ok(commits.every(c=>c.offset==='-1'||c.offset==='0'),'Kafka offset NOT committed');
    writeFileSync('/control/verified-blocked','ready',{mode:0o600});
    await until(async()=>existsSync('/control/killed'),'external SIGKILL');
    assert.deepEqual(await counts(),{raw:1,aggregate:1});
    await until(async()=>{
      const values=(await admin.fetchOffsets({groupId:consumerGroup,topics:[eventsTopic],resolveOffsets:false}))[0]!.partitions;
      return values.some(p=>p.partition===active.partition&&p.offset==='1');
    },'restarted worker committed replay');
    assert.deepEqual(await counts(),{raw:1,aggregate:1});
    assert.ok(Number((await pool.query("SELECT value FROM ingest_metrics WHERE project_id=$1 AND metric='duplicates'",[f.project])).rows[0]?.value)>=1);
    assert.equal((await pool.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
    console.log(JSON.stringify({passed:true,actual_worker_sigkill:true,server_checkpoint_commit_before_kill:true,kafka_commit_before_kill:false,restarted_replay_duplicate:true,raw_events:1,aggregation_jobs:1,final_kafka_offset:'1'}));
  }else throw new Error('invalid_checkpoint_test_mode');
}finally{await admin.disconnect();await pool.end();}
