// Disposable PostgreSQL only. Synthetic historical records bypass SDK's 7-day TTL.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {changeProjectRetention} from '../packages/server/src/project-retention';
import {persist,EventConflict} from '../packages/server/src/consume';
import {retainNext} from '../packages/server/src/retention';
import {lockProjectPrivacy} from '../packages/server/src/privacy';
import {trends} from '../packages/server/src/trends';
import type {StoredEnvelope} from '../packages/schema/src/index';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),foreign=randomUUID(),project=randomUUID(),other=randomUUID(),admin=randomUUID(),viewer=randomUUID();
  for(const id of[tenant,foreign])await pool.query("INSERT INTO tenants VALUES($1,'RETENTION POLICY TEMP')",[id]);
  for(const [id,t]of[[project,tenant],[other,foreign]])await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'RETENTION POLICY TEMP')",[id,t]);
  for(const [id,role]of[[admin,'admin'],[viewer,'viewer']]){
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,`retention-${id}@integration.invalid`,'unused']);
    await pool.query('INSERT INTO tenant_memberships VALUES($1,$2,$3)',[tenant,id,role]);
  }
  const update=(days:number,version:number,actor=admin,p=project)=>changeProjectRetention(tenant,p,actor,{raw_days:days,expected_version:version});
  const state=async()=>(await pool.query('SELECT retention_days,retention_version,retention_floor,privacy_epoch FROM projects WHERE id=$1',[project])).rows[0];
  const audits=async()=>Number((await pool.query("SELECT count(*) AS n FROM audit_logs WHERE action='retention_policy_changed'")).rows[0].n);
  const count=async(table:string,p=project)=>Number((await pool.query(`SELECT count(*) AS n FROM ${table} WHERE project_id=$1`,[p])).rows[0].n);
  const record=(age:number,p=project,t=tenant):StoredEnvelope=>({tenant_id:t,project_id:p,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'view',distinct_id:randomUUID(),event_time:new Date(Date.now()-age*86400000).toISOString(),properties:{},context:{},schema_version:1}});
  for(const r of[record(10),record(0.01),record(10,other,foreign)])assert.equal(await persist(r),'inserted');
  assert.equal(await count('visible_events'),2);
  await assert.rejects(update(7,1,viewer),{status:403});
  await assert.rejects(update(7,1,admin,other),{status:404});
  assert.equal((await update(30,1)).changed,false);assert.equal(await audits(),0);
  const blocked=await pool.connect();
  try{await blocked.query('BEGIN');await lockProjectPrivacy(blocked,project,true);await assert.rejects(update(7,1),{status:409,message:'retention_policy_busy'});}finally{await blocked.query('ROLLBACK');blocked.release();}
  assert.equal((await state()).retention_version,1);
  assert.equal((await update(7,1)).changed,true);
  const shortened=await state();assert.equal(shortened.retention_days,7);assert.equal(shortened.privacy_epoch,1);
  assert.equal(await count('events'),2);assert.equal(await count('visible_events'),1);assert.equal(await count('visible_events',other),1);
  await assert.rejects(update(30,1),{status:409,message:'retention_policy_conflict'});
  const result=await trends(tenant,project,{kind:'trend',from:new Date(Date.now()-20*86400000).toISOString(),to:new Date().toISOString(),metric:'count',filters:[],group_by:[]});
  assert.equal(result.summary.events,1);assert.equal(result.data_coverage.expired_overlap,true);
  assert.ok(Date.parse(result.data_coverage.retained_from)>=shortened.retention_floor.getTime());
  await update(30,2);assert.equal(await count('visible_events'),1,'extension cannot resurrect uncollected raw rows');
  const expired=record(10);assert.equal(await persist(expired),'suppressed');assert.equal(await persist(expired),'duplicate');
  await assert.rejects(persist({...expired,event:{...expired.event,properties:{changed:true}}}),error=>error instanceof EventConflict&&error.reason==='payload_conflict');
  const identify=record(10);identify.event.event_name='$identify';identify.event.anonymous_id='expired-anonymous';
  assert.equal(await persist(identify),'suppressed');assert.equal(await count('identity_links'),0);assert.equal(await count('aggregation_jobs'),2);assert.equal(await count('event_dedup'),4);
  // A failed audit must roll back policy, floor, epoch and version together.
  const before=await state(),beforeAudits=await audits();
  await pool.query("CREATE FUNCTION fail_retention_audit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.action='retention_policy_changed' THEN RAISE EXCEPTION 'synthetic audit failure'; END IF; RETURN NEW; END$$; CREATE TRIGGER fail_retention_audit BEFORE INSERT ON audit_logs FOR EACH ROW EXECUTE FUNCTION fail_retention_audit()");
  try{await assert.rejects(update(1,3),/synthetic audit failure/);}finally{await pool.query('DROP TRIGGER fail_retention_audit ON audit_logs; DROP FUNCTION fail_retention_audit()');}
  assert.deepEqual(await state(),before);assert.equal(await audits(),beforeAudits);
  await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[tenant,admin]);
  await assert.rejects(update(1,3),{status:403});assert.deepEqual(await state(),before);
  const db=await pool.connect();try{assert.equal(await retainNext(db),'completed');assert.equal(await retainNext(db),'empty');}finally{db.release();}
  assert.equal(await count('events'),1);assert.equal(await count('aggregation_jobs'),1);assert.equal(await count('event_dedup'),4);assert.equal(await count('events',other),1);
  console.log(JSON.stringify({passed:true,schema20:true,fresh_authorization:true,foreign_scope_hidden:true,no_op_no_audit:true,version_conflict:true,privacy_lock_busy:true,logical_expiry_immediate:true,extension_no_resurrection:true,expired_consumer_dedup:true,expired_identify_no_link:true,audit_atomic_rollback:true,physical_expiry_keeps_45_day_dedup:true}));
}finally{await pool.end();}
