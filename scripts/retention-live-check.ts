// Deliberate loss ONLY inside a fresh, guarded disposable Kafka/PG namespace.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFileSync,writeFileSync} from 'node:fs';
import {pool} from '../packages/server/src/db';
import {kafka,eventsTopic,dlqTopic,consumerGroup} from '../packages/server/src/kafka';
const mode=process.argv[2],base=process.env.TEST_API!,prefix=process.env.CCPANEL_REPLAY_PREFIX!;
assert.match(prefix||'',/^ccpanel\.restore\.[a-f0-9]{12}$/);
assert.equal(eventsTopic,prefix+'.events');assert.equal(dlqTopic,prefix+'.dlq');assert.equal(consumerGroup,prefix+'.writer');
const f=JSON.parse(readFileSync('/control/fixture.json','utf8'));
const admin=kafka.admin();
async function until(fn:()=>Promise<boolean>,label:string){const end=Date.now()+30000;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,300));}throw new Error('retention_test_timeout:'+label);}
const batch=()=>({write_key:f.key,project_id:f.project,schema_version:1,events:[{event_id:randomUUID(),event_name:'retention_gap_synthetic',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{test:true},context:{},schema_version:1}]});
const post=()=>fetch(base+'/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body:JSON.stringify(batch()),signal:AbortSignal.timeout(5000)});
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query("SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1 AND t.id=$2 AND t.name='PRESSURE LIVE TEMP'",[f.project,f.tenant])).rowCount,1);
  await admin.connect();
  if(mode==='initialize'){
    await until(async()=>{try{return(await fetch(base+'/ready',{signal:AbortSignal.timeout(3000)})).ok&&(await pool.query('SELECT count(*)::int AS n FROM consumer_progress WHERE topic=$1 AND group_id=$2 AND next_offset=0',[eventsTopic,consumerGroup])).rows[0].n===2;}catch{return false;}},'fresh worker progress');
    assert.ok((await admin.fetchTopicOffsets(eventsTopic)).every(p=>p.high==='0'&&p.low==='0'));
  }else if(mode==='enqueue'){
    assert.equal((await post()).status,202,'synthetic record actually acknowledged before the fault');
    const highs=await admin.fetchTopicOffsets(eventsTopic);
    assert.equal(highs.filter(p=>p.high==='1').length,1);assert.ok(highs.every(p=>p.high==='0'||p.high==='1'));
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events')).rows[0].n,0);
  }else if(mode==='remove'){
    // Wrapper has stopped BOTH private services. Refuse an active group.
    const group=(await admin.describeGroups([consumerGroup])).groups[0];assert.equal(group?.state,'Empty');
    const highs=await admin.fetchTopicOffsets(eventsTopic),lost=highs.filter(p=>p.high==='1');
    assert.equal(lost.length,1);assert.ok(highs.every(p=>p.low==='0'));
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM consumer_progress WHERE topic=$1 AND group_id=$2 AND next_offset=0',[eventsTopic,consumerGroup])).rows[0].n,2);
    await admin.deleteTopicRecords({topic:eventsTopic,partitions:lost.map(p=>({partition:p.partition,offset:p.high}))});
    await until(async()=>(await admin.fetchTopicOffsets(eventsTopic)).some(p=>p.partition===lost[0]!.partition&&p.low==='1'),'actual retained lower bound');
    // Simulate the broker-side reset already having hidden the lag.
    await admin.setOffsets({groupId:consumerGroup,topic:eventsTopic,partitions:highs.map(p=>({partition:p.partition,offset:p.high}))});
    const commits=(await admin.fetchOffsets({groupId:consumerGroup,topics:[eventsTopic],resolveOffsets:false}))[0]!.partitions;
    assert.ok(highs.every(p=>commits.some(c=>c.partition===p.partition&&c.offset===p.high)));
    writeFileSync('/control/gap.json',JSON.stringify({partition:lost[0]!.partition,highs}),{mode:0o600});
    console.log(JSON.stringify({fault_injected:true,synthetic_accepted_records_removed:1,broker_lag_after_reset:0,database_progress_unchanged:true}));
  }else if(mode==='verify'||mode==='verify-restart'){
    const gap=JSON.parse(readFileSync('/control/gap.json','utf8'));
    let response:Response|undefined;
    await until(async()=>{try{response=await post();return response.status===503&&(await response.clone().json()).reason==='retained_offset_gap';}catch{return false;}},'persistent gap refusal');
    assert.ok(Number(response!.headers.get('Retry-After'))>0);
    const control=(await pool.query('SELECT blocked,reason,detected_at::text,details FROM ingest_integrity_gate WHERE id=true')).rows[0];
    assert.equal(control.blocked,true);assert.equal(control.reason,'retained_offset_gap');assert.equal(control.details.expected,'0');
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM consumer_progress_audit WHERE action='paused'")).rows[0].n,1);
    if(mode==='verify')writeFileSync('/control/gap.json',JSON.stringify({...gap,detected_at:control.detected_at}),{mode:0o600});
    else assert.equal(control.detected_at,gap.detected_at,'restart cannot acknowledge or replace the incident');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events')).rows[0].n,0);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM aggregation_jobs')).rows[0].n,0);
    assert.equal((await pool.query('SELECT next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 AND partition_id=$3',[eventsTopic,consumerGroup,gap.partition])).rows[0].next_offset,'0');
    const highs=await admin.fetchTopicOffsets(eventsTopic);assert.deepEqual(highs,gap.highs.map((p:{partition:number;high:string;low:string;offset:string})=>({...p,low:p.partition===gap.partition?'1':p.low})));
    console.log(JSON.stringify({passed:true,mode,actual_retention_gap:true,kafka_commit_already_reset:true,http_503:true,raw_events:0,aggregation_jobs:0,pg_next_offset:'0',incident_count:1,restart_preserved_incident:mode==='verify-restart'}));
  }else throw new Error('invalid_retention_mode');
}finally{await admin.disconnect();await pool.end();}
