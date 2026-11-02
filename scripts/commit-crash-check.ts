// Pause actual private Worker SQL with temporary PG triggers, then require
// the external controller to SIGKILL that container. No production test hooks.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {existsSync,readFileSync,writeFileSync} from 'node:fs';
import type {PoolClient} from 'pg';
import {pool} from '../packages/server/src/db';
import {kafka,eventsTopic,dlqTopic,consumerGroup} from '../packages/server/src/kafka';
const phase=process.argv[2],first=phase==='before_raw';assert.ok(first||phase==='after_raw');
const ordinal=first?1:2,previous=ordinal-1,lock=169202630,base=process.env.TEST_API!,prefix=process.env.CCPANEL_REPLAY_PREFIX!;
assert.match(prefix||'',/^ccpanel\.restore\.[a-f0-9]{12}$/);
assert.equal(eventsTopic,prefix+'.events');assert.equal(dlqTopic,prefix+'.dlq');assert.equal(consumerGroup,prefix+'.writer');
const fixture=JSON.parse(readFileSync('/control/fixture.json','utf8')),admin=kafka.admin();
const file=(name:string)=>`/control/${phase}.${name}`;
async function until(fn:()=>Promise<boolean>,label:string,ms=90000){const end=Date.now()+ms;while(Date.now()<end){if(await fn())return;await new Promise(r=>setTimeout(r,100));}throw new Error('commit_crash_timeout:'+label);}
async function counts(){return(await pool.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM aggregation_jobs) AS aggregate')).rows[0];}
let holder:PoolClient|undefined;
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query("SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1 AND t.id=$2 AND t.name='PRESSURE LIVE TEMP'",[fixture.project,fixture.tenant])).rowCount,1);
  await admin.connect();
  await until(async()=>{try{return(await fetch(base+'/ready',{signal:AbortSignal.timeout(3000)})).ok&&(await pool.query('SELECT count(*)::int AS n FROM consumer_progress WHERE topic=$1 AND group_id=$2',[eventsTopic,consumerGroup])).rows[0].n===2;}catch{return false;}},'services ready');
  assert.deepEqual(await counts(),{raw:previous,aggregate:previous});
  const highs=await admin.fetchTopicOffsets(eventsTopic);
  assert.equal(highs.reduce((n,p)=>n+Number(p.high),0),previous);
  holder=await pool.connect();await holder.query('SELECT pg_advisory_lock($1,$2)',[lock,ordinal]);
  const table=first?'events':'consumer_progress';
  await pool.query(`CREATE FUNCTION crash_fixture_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(${lock},${ordinal}); RETURN NEW; END $$`);
  await pool.query(`CREATE TRIGGER crash_fixture_wait BEFORE ${first?'INSERT':'UPDATE'} ON ${table} FOR EACH ROW EXECUTE FUNCTION crash_fixture_wait()`);
  const event={event_id:randomUUID(),event_name:'commit_crash_synthetic',distinct_id:'same-synthetic-user',event_time:new Date().toISOString(),properties:{phase},context:{},schema_version:1};
  const response=await fetch(base+'/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body:JSON.stringify({write_key:fixture.key,project_id:fixture.project,schema_version:1,events:[event]}),signal:AbortSignal.timeout(10000)});
  assert.equal(response.status,202);
  await until(async()=>(await pool.query('SELECT 1 FROM pg_locks WHERE locktype=$1 AND classid=$2 AND objid=$3 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())',['advisory',lock,ordinal])).rowCount!>0,'worker SQL blocked',10000);
  assert.deepEqual(await counts(),{raw:first?previous:ordinal,aggregate:first?previous:ordinal});
  const progress=(await pool.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id',[eventsTopic,consumerGroup])).rows;
  assert.equal(progress.reduce((n,p)=>n+Number(p.next_offset),0),previous);
  writeFileSync(file('blocked'),JSON.stringify({phase,raw_committed:!first,checkpoint_committed:false}),{mode:0o600});
  await until(async()=>existsSync(file('killed')),'external SIGKILL',10000);
  await holder.query('SELECT pg_advisory_unlock($1,$2)',[lock,ordinal]);holder.release();holder=undefined;
  // DDL waits for the terminated Worker's transaction to release its table lock.
  await pool.query(`DROP TRIGGER crash_fixture_wait ON ${table}`);
  await pool.query('DROP FUNCTION crash_fixture_wait()');
  assert.deepEqual(await counts(),{raw:first?previous:ordinal,aggregate:first?previous:ordinal});
  assert.deepEqual((await pool.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id',[eventsTopic,consumerGroup])).rows,progress);
  writeFileSync(file('resume'),'ready',{mode:0o600});
  await until(async()=>{
    const positions=await admin.fetchTopicOffsets(eventsTopic),commits=(await admin.fetchOffsets({groupId:consumerGroup,topics:[eventsTopic],resolveOffsets:false}))[0]!.partitions;
    const rows=(await pool.query('SELECT partition_id,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2',[eventsTopic,consumerGroup])).rows;
    return positions.reduce((n,p)=>n+Number(p.high),0)===ordinal&&positions.every(p=>rows.some(r=>r.partition_id===p.partition&&r.next_offset===p.high)&&commits.some(c=>c.partition===p.partition&&(c.offset===p.high||(p.high==='0'&&c.offset==='-1'))));
  },'restarted worker caught up');
  assert.deepEqual(await counts(),{raw:ordinal,aggregate:ordinal});
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM event_dedup WHERE project_id=$1 AND event_id=$2 AND status=$3',[fixture.project,event.event_id,'stored'])).rows[0].n,1);
  if(!first)assert.ok(Number((await pool.query("SELECT value FROM ingest_metrics WHERE project_id=$1 AND metric='duplicates'",[fixture.project])).rows[0]?.value)>=1);
  assert.equal((await pool.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
  console.log(JSON.stringify({passed:true,phase,actual_worker_sigkill:true,raw_committed_before_kill:!first,checkpoint_committed_before_kill:false,raw_events:ordinal,aggregation_jobs:ordinal,pg_and_kafka_caught_up:true,replay_duplicate_observed:!first}));
}finally{if(holder){await holder.query('SELECT pg_advisory_unlock_all()');holder.release();}await admin.disconnect();await pool.end();}
