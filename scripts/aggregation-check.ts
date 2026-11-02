// Run only in a disposable freshly migrated PG database, after capacity tests.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {aggregateNext} from '../packages/server/src/aggregations';
import {deleteBatch} from '../packages/server/src/deletion-service';
import {identityHash,lockProjectPrivacy} from '../packages/server/src/privacy';
import type {StoredEnvelope} from '../packages/schema/src/index';
const db=await pool.connect();
try{
  assert.match((await db.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await db.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const a={tenant_id:randomUUID(),project_id:randomUUID()},b={tenant_id:randomUUID(),project_id:randomUUID()};
  for(const scope of [a,b]){
    await db.query('INSERT INTO tenants VALUES($1,$2)',[scope.tenant_id,'AGGREGATION TEMP']);
    await db.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[scope.project_id,scope.tenant_id,'AGGREGATION TEMP']);
  }
  const hour=Math.floor(Date.now()/3600000)*3600000-2*3600000;
  const record=(scope:typeof a,user:string):StoredEnvelope=>({...scope,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'aggregate_fixture',distinct_id:user,event_time:new Date(hour+600000).toISOString(),properties:{test:true},context:{},schema_version:1}});
  const value=async(scope:typeof a,view='visible_event_count_buckets')=>Number((await db.query(`SELECT coalesce(sum(event_count),0)::text AS n FROM ${view} WHERE tenant_id=$1 AND project_id=$2`,[scope.tenant_id,scope.project_id])).rows[0].n);
  const first=record(a,'subject-a'),second=record(a,'subject-b');
  await persist(first);await persist(second);assert.equal(await persist(first),'duplicate');
  assert.equal(await aggregateNext(db),'completed');assert.equal(await value(a),2);
  assert.equal(await aggregateNext(db),'empty');assert.equal(await value(a),2);
  await persist(record(a,'subject-b'));assert.equal(await aggregateNext(db),'completed');assert.equal(await value(a),3,'late event replaces count instead of adding the bucket again');
  await persist({...first,...b});assert.equal(await aggregateNext(db),'completed');assert.equal(await value(b),1,'same event ID in another company is independent');
  await persist(record(a,'subject-b'));
  await db.query('ALTER TABLE event_count_buckets ADD CONSTRAINT fixture_aggregate_failure CHECK(event_count<=3)');
  assert.equal(await aggregateNext(db),'retry');assert.equal(await value(a),3);
  const failed=(await db.query('SELECT processed_at,attempts,last_error_code FROM aggregation_jobs WHERE tenant_id=$1 AND processed_at IS NULL',[a.tenant_id])).rows;
  assert.equal(failed.length,1);assert.equal(failed[0].attempts,1);assert.equal(failed[0].last_error_code,'23514');
  await db.query('ALTER TABLE event_count_buckets DROP CONSTRAINT fixture_aggregate_failure');
  await db.query('UPDATE aggregation_jobs SET available_at=clock_timestamp() WHERE tenant_id=$1',[a.tenant_id]);
  assert.equal(await aggregateNext(db),'completed');assert.equal(await value(a),4);
  async function barrier(scope:typeof a,subject:string){
    const id=randomUUID();await db.query('BEGIN');await lockProjectPrivacy(db,scope.project_id,true);
    await db.query('INSERT INTO deletion_jobs(id,tenant_id,project_id,scope,subject_hash) VALUES($1,$2,$3,$4,$5)',[id,scope.tenant_id,scope.project_id,subject==='*'?'project':'user',subject==='*'?null:identityHash(scope.tenant_id,scope.project_id,subject)]);
    await db.query("INSERT INTO deletion_barriers SELECT tenant_id,project_id,$2,id,requested_at,blocked_before FROM deletion_jobs WHERE id=$1",[id,subject==='*'?'*':identityHash(scope.tenant_id,scope.project_id,subject)]);
    await db.query('UPDATE projects SET privacy_epoch=privacy_epoch+1 WHERE tenant_id=$1 AND id=$2',[scope.tenant_id,scope.project_id]);await db.query('COMMIT');
    return {id,...scope};
  }
  async function erase(job:{id:string;tenant_id:string;project_id:string}){
    for(let i=0;i<20;i++){await db.query('BEGIN');await lockProjectPrivacy(db,job.project_id,true);const complete=await deleteBatch(db,job);await db.query('COMMIT');if(complete)return;}
    assert.fail('bounded deletion did not finish');
  }
  const deletedA=await barrier(a,'subject-a');assert.equal(await value(a),0,'epoch hides old aggregate before physical cleanup');assert.equal(await value(b),1);
  assert.equal(await aggregateNext(db),'completed');assert.equal(await value(a),3,'rebuild reads privacy-filtered raw rows');await erase(deletedA);assert.equal(await value(a),3);
  const deletedB=await barrier(a,'subject-b');assert.equal(await aggregateNext(db),'completed');assert.equal(await value(a,'event_count_buckets'),0,'empty buckets are physically removed');await erase(deletedB);
  const deletedProject=await barrier(b,'*');assert.equal(await value(b),0);assert.equal(await aggregateNext(db),'completed');await erase(deletedProject);assert.equal(await value(b,'event_count_buckets'),0);
  await db.query('INSERT INTO aggregation_jobs(tenant_id,project_id,event_id) VALUES($1,$2,$3)',[a.tenant_id,a.project_id,randomUUID()]);
  assert.equal(await aggregateNext(db),'retry');assert.equal((await db.query("SELECT count(*)::int AS n FROM aggregation_jobs WHERE tenant_id=$1 AND processed_at IS NULL AND last_error_code='AG001'",[a.tenant_id])).rows[0].n,1);
  console.log(JSON.stringify({passed:true,duplicate_count_once:true,late_event_recomputed:true,tenant_isolation:true,bucket_and_completion_rollback:true,privacy_epoch_hides_stale:true,deleted_users_excluded:true,zero_and_project_buckets_removed:true,missing_raw_retained_for_retry:true}));
}finally{await db.query('ROLLBACK').catch(()=>{});db.release();await pool.end();}
