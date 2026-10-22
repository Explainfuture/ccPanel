// Real SQL checks: isolated freshly migrated database only, never live data.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {retainNext,expireDeadLetters} from '../packages/server/src/retention';
import {lockProjectPrivacy} from '../packages/server/src/privacy';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),other=randomUUID(),project=randomUUID(),otherProject=randomUUID();
  await pool.query("INSERT INTO tenants VALUES($1,'RETENTION TEMP'),($2,'RETENTION TEMP')",[tenant,other]);
  await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'RETENTION TEMP'),($3,$4,'RETENTION TEMP')",[project,tenant,otherProject,other]);
  const ids:string[]=[];
  for(let i=0;i<103;i++){
    const id=randomUUID();ids.push(id);
    const time=i<101?"CURRENT_TIMESTAMP-interval '31 days'":"CURRENT_TIMESTAMP-interval '29 days'";
    await pool.query(`INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,event_time,received_at,properties,context,schema_version) VALUES($1,$2,$3,'view','user',${time},now(),'{}','{}',1)`,[tenant,project,id]);
    await pool.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash,created_at) VALUES($1,$2,$3,'fixture',CURRENT_TIMESTAMP-interval '46 days')",[tenant,project,id]);
    await pool.query('INSERT INTO aggregation_jobs(tenant_id,project_id,event_id) VALUES($1,$2,$3)',[tenant,project,id]);
  }
  const recentDedup=randomUUID(),expiredDedup=randomUUID();
  await pool.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash,created_at) VALUES($1,$2,$3,'fixture',CURRENT_TIMESTAMP-interval '44 days'),($1,$2,$4,'fixture',CURRENT_TIMESTAMP-interval '46 days')",[tenant,project,recentDedup,expiredDedup]);
  const foreignId=randomUUID();
  await pool.query("INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,event_time,received_at,properties,context,schema_version) VALUES($1,$2,$3,'view','user',CURRENT_TIMESTAMP-interval '29 days',now(),'{}','{}',1)",[other,otherProject,foreignId]);
  await pool.query("INSERT INTO event_count_buckets(tenant_id,project_id,event_name,bucket_start,event_count,privacy_epoch) VALUES($1,$2,'view',date_trunc('hour',CURRENT_TIMESTAMP-interval '31 days'),101,0)",[tenant,project]);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_events WHERE project_id=$1',[project])).rows[0].n,2);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_event_count_buckets WHERE project_id=$1',[project])).rows[0].n,0);
  const db=await pool.connect(),blocking=await pool.connect();
  try{
    await blocking.query('BEGIN');await lockProjectPrivacy(blocking,project,true);
    await assert.rejects(retainNext(db),(error:unknown)=>(error as {code:string}).code==='55P03');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE project_id=$1',[project])).rows[0].n,103);
    await blocking.query('ROLLBACK');
    assert.equal(await retainNext(db),'completed');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE project_id=$1',[project])).rows[0].n,3);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM aggregation_jobs WHERE project_id=$1',[project])).rows[0].n,3);
    for(let i=0;i<5;i++)await retainNext(db);
    assert.equal(await retainNext(db),'empty');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE project_id=$1',[project])).rows[0].n,2);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM aggregation_jobs WHERE project_id=$1',[project])).rows[0].n,2);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM event_dedup WHERE project_id=$1',[project])).rows[0].n,3);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM event_dedup WHERE project_id=$1 AND event_id=$2',[project,recentDedup])).rows[0].n,1);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE project_id=$1 AND event_id=$2',[otherProject,foreignId])).rows[0].n,1);
    assert.equal((await pool.query('SELECT value::int FROM ingest_metrics WHERE project_id=$1 AND metric=\'retention_raw_expired\'',[project])).rows[0].value,101);
    const actor=randomUUID(),job=randomUUID(),replayEvent=randomUUID();
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[actor,`retention-${actor}@integration.invalid`,'fixture-only']);
    await pool.query("INSERT INTO replay_jobs(id,tenant_id,project_id,user_id,request_hash,reason) VALUES($1,$2,$3,$4,'fixture','retention check')",[job,tenant,project,actor]);
    await pool.query("INSERT INTO replay_items(tenant_id,project_id,job_id,record_id,event_id,payload_hash) VALUES($1,$2,$3,'retention-active',$4,'fixture')",[tenant,project,job,replayEvent]);
    await pool.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash,created_at) VALUES($1,$2,$3,'fixture',CURRENT_TIMESTAMP-interval '46 days')",[tenant,project,replayEvent]);
    for(const [id,age,scoped]of[['retention-expired',16,true],['retention-active',16,true],['retention-fresh',13,true],['retention-unscoped',17,false]] as const){
      await pool.query("INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload,created_at) VALUES($1,$2,$3,'fixture','{}',CURRENT_TIMESTAMP-$4::int*interval '1 day')",[id,scoped?tenant:null,scoped?project:null,age]);
    }
    let count=0;for(let i=0;i<5;i++)count+=await expireDeadLetters(db);assert.equal(count,2);
    assert.deepEqual((await pool.query('SELECT id FROM dlq_records ORDER BY id')).rows.map(r=>r.id),['retention-active','retention-fresh']);
    assert.equal(await retainNext(db),'empty');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM event_dedup WHERE project_id=$1 AND event_id=$2',[project,replayEvent])).rows[0].n,1);
    await pool.query("UPDATE replay_jobs SET status='completed',finished_at=now() WHERE id=$1 AND project_id=$2",[job,project]);
    assert.equal(await expireDeadLetters(db),1);assert.equal(await retainNext(db),'completed');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM event_dedup WHERE project_id=$1 AND event_id=$2',[project,replayEvent])).rows[0].n,0);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM replay_items WHERE project_id=$1 AND job_id=$2',[project,job])).rows[0].n,1);
  }finally{await blocking.query('ROLLBACK');blocking.release();db.release();}
  console.log(JSON.stringify({passed:true,logical_expiry:true,raw_batch_limit_100:true,tasks_atomic_cleanup:true,dedup_45_days_and_raw_guard:true,project_lock_contention_rolls_back:true,foreign_scope_preserved:true,idempotent_empty:true,dlq_14_days:true,unscoped_dlq_expired:true,active_replay_preserves_dlq_and_dedup:true,terminal_replay_releases_expiry:true,replay_audit_retained:true}));
}finally{await pool.end();}
