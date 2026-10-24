// Real CLI subprocesses and PostgreSQL; isolated DB only, never live data.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {changeProjectRetention} from '../packages/server/src/project-retention';
import {holdRecoveryLease} from '../packages/server/src/recovery-gate';
import {persist} from '../packages/server/src/consume';
import {identityHash} from '../packages/server/src/privacy';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const cli=(mode:string,input?:unknown)=>{
    const result=spawnSync(process.execPath,[fileURLToPath(new URL('deletion-ledger.mjs',import.meta.url)),mode],{env:{...process.env,CCPANEL_OFFLINE_RESTORE:'true'},input:input===undefined?'':JSON.stringify(input),encoding:'utf8',timeout:15000,maxBuffer:9*1024*1024});
    if(result.error)throw result.error;return{status:result.status,stdout:result.stdout,stderr:result.stderr};
  };
  const apply=(input:unknown)=>{const result=cli('apply',input);assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout);};
  const denied=async(input:unknown,code:string)=>{const result=cli('apply',input);assert.notEqual(result.status,0);assert.equal(JSON.parse(result.stderr.trim()).error,code);assert.equal((await pool.query('SELECT blocked FROM recovery_gate')).rows[0].blocked,true);};
  const tenant=randomUUID(),foreign=randomUUID(),project=randomUUID(),other=randomUUID(),actor=randomUUID(),job=randomUUID();
  for(const id of[tenant,foreign])await pool.query("INSERT INTO tenants VALUES($1,'RECOVERY TEMP')",[id]);
  for(const [id,t]of[[project,tenant],[other,foreign]])await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'RECOVERY TEMP')",[id,t]);
  await pool.query('INSERT INTO users VALUES($1,$2,$3)',[actor,`recovery-${actor}@integration.invalid`,'unused']);await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,actor]);
  const event=(age:number,p=project,t=tenant)=>({tenant_id:t,project_id:p,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'view',distinct_id:'keep-user',event_time:new Date(Date.now()-age*86400000).toISOString(),properties:{},context:{},schema_version:1 as const}});
  for(const r of[event(10),event(0.01),event(10,other,foreign)])assert.equal(await persist(r),'inserted');
  await changeProjectRetention(tenant,project,actor,{raw_days:1,expected_version:1});
  const hash=identityHash(tenant,project,'deleted-user');
  await pool.query("INSERT INTO deletion_jobs(id,tenant_id,project_id,user_id,scope,subject_hash) VALUES($1,$2,$3,$4,'user',$5)",[job,tenant,project,actor,hash]);
  await pool.query('INSERT INTO deletion_barriers(tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before) SELECT tenant_id,project_id,$2,id,requested_at,blocked_before FROM deletion_jobs WHERE id=$1',[job,hash]);
  await pool.query('UPDATE projects SET privacy_epoch=privacy_epoch+1 WHERE id=$1',[project]);
  const exported=cli('export');assert.equal(exported.status,0,exported.stderr);const ledger=JSON.parse(exported.stdout);
  assert.equal(ledger.schema_version,2);assert.equal(ledger.retention.length,2);assert.equal(ledger.projects.length,1);assert.equal(ledger.barriers.length,1);
  // Emulate the old backup's metadata and missing deletion state in this DB.
  await pool.query('DELETE FROM deletion_barriers');await pool.query('DELETE FROM deletion_jobs');
  await pool.query('UPDATE projects SET retention_days=30,retention_version=1,retention_floor=NULL,privacy_epoch=0 WHERE id=$1',[project]);
  const oldQuery=randomUUID();await pool.query("INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,status,result,privacy_epoch) VALUES($1,$2,$3,$4,'{}','succeeded','{\"events\":2}',0)",[oldQuery,tenant,project,actor]);
  const policy=async()=>(await pool.query('SELECT retention_days,retention_version,retention_floor,privacy_epoch FROM projects WHERE id=$1',[project])).rows[0],original=await policy();
  await denied({schema_version:1,exported_at:ledger.exported_at,projects:ledger.projects,jobs:ledger.jobs,barriers:ledger.barriers},'retention_ledger_required');assert.deepEqual(await policy(),original);
  await denied({...ledger,retention:ledger.retention.filter((p:{project_id:string})=>p.project_id===project)},'restore_scope_metadata_missing');
  await denied({...ledger,retention:ledger.retention.map((p:{project_id:string;tenant_id:string})=>p.project_id===other?{...p,tenant_id:tenant}:p)},'restore_scope_metadata_missing');
  await pool.query('UPDATE projects SET retention_version=3 WHERE id=$1',[project]);await denied(ledger,'retention_ledger_version_stale');await pool.query('UPDATE projects SET retention_version=1 WHERE id=$1',[project]);
  await pool.query("CREATE FUNCTION fail_retention_import() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.action='retention_ledger_applied' THEN RAISE EXCEPTION 'synthetic import failure'; END IF; RETURN NEW; END$$; CREATE TRIGGER fail_retention_import BEFORE INSERT ON audit_logs FOR EACH ROW EXECUTE FUNCTION fail_retention_import()");
  try{const failed=cli('apply',ledger);assert.notEqual(failed.status,0);assert.equal((await pool.query('SELECT blocked FROM recovery_gate')).rows[0].blocked,true);assert.deepEqual(await policy(),original);}finally{await pool.query('DROP TRIGGER fail_retention_import ON audit_logs; DROP FUNCTION fail_retention_import()');}
  assert.equal(apply(ledger).gate,'open');
  assert.equal((await policy()).retention_days,1);assert.equal((await policy()).retention_version,2);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_events WHERE project_id=$1',[project])).rows[0].n,1);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE project_id=$1',[project])).rows[0].n,2);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_events WHERE project_id=$1',[other])).rows[0].n,1);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM deletion_barriers')).rows[0].n,1);
  assert.equal((await pool.query('SELECT status,result FROM query_jobs WHERE id=$1',[oldQuery])).rows[0].result,null);
  const imported=await policy(),auditCount=(await pool.query("SELECT count(*)::int AS n FROM audit_logs WHERE action='retention_ledger_applied'")).rows[0].n;assert.equal(auditCount,2);
  apply(ledger);assert.deepEqual(await policy(),imported);assert.equal((await pool.query("SELECT count(*)::int AS n FROM audit_logs WHERE action='retention_ledger_applied'")).rows[0].n,auditCount);
  const release=await holdRecoveryLease();try{const online=cli('apply',ledger);assert.notEqual(online.status,0);assert.equal(JSON.parse(online.stderr.trim()).error,'services_still_online');assert.equal((await pool.query('SELECT blocked FROM recovery_gate')).rows[0].blocked,false);}finally{await release();}
  // Separate legitimate-history fixture: source extended a seven-day policy
  // a week ago. Its 14-day floor must retain an event now ten days old.
  const extended=randomUUID();await pool.query("INSERT INTO projects(id,tenant_id,name,retention_days,retention_version,retention_floor) VALUES($1,$2,'EXTENDED TEMP',30,3,CURRENT_TIMESTAMP-interval '14 days')",[extended,tenant]);
  assert.equal(await persist(event(10,extended)),'inserted');
  const latest=cli('export');assert.equal(latest.status,0,latest.stderr);
  await pool.query("UPDATE projects SET retention_days=7,retention_version=2,retention_floor=CURRENT_TIMESTAMP-interval '30 days' WHERE id=$1",[extended]);
  apply(JSON.parse(latest.stdout));
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_events WHERE project_id=$1',[extended])).rows[0].n,1,'obsolete seven-day policy must not be advanced during restore');
  assert.equal((await pool.query('SELECT retention_days FROM projects WHERE id=$1',[extended])).rows[0].retention_days,30);
  console.log(JSON.stringify({retention_recovery:true,actual_cli:true,complete_inventory_required:true,legacy_ledger_refused_on_schema20:true,cross_company_metadata_refused:true,stale_version_refused:true,failed_audit_rolls_back_policy:true,failure_gate_closed:true,deleted_barrier_restored:true,expired_raw_stays_hidden:true,foreign_data_retained:true,old_result_cleared:true,retry_idempotent:true,live_lease_blocks_apply:true,obsolete_short_policy_not_advanced:true}));
}finally{await pool.end();}
