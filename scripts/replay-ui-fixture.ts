// Explicit synthetic UI-only source records; actual DLQ creation is covered by
// dlq-live-check.mjs. Never alters constraints or seeds an existing company.
import assert from 'node:assert/strict';
import { randomUUID,randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { pool } from '../packages/server/src/db';
import { digest } from '../packages/server/src/security';
import { indexDeadLetter,type DeadLetter } from '../packages/server/src/dead-letter';
import { identityHash } from '../packages/server/src/privacy';
import type { StoredEnvelope } from '../packages/schema/src/index';
const mode=process.argv[2],label='DLQ UI TEMP';
try{
  if(mode==='create'){
    assert.equal((await pool.query('SELECT 1 FROM tenants WHERE name=$1',[label])).rowCount,0,'clean prior UI fixture first');
    const f={tenant:randomUUID(),project:randomUUID(),user:randomUUID(),token:randomBytes(24).toString('hex'),csrf:randomBytes(24).toString('hex'),records:[] as {id:string;kind:string;event_id:string}[]};
    f.csrf=digest(f.token+':csrf');
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[f.tenant,label]);
    await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[f.project,f.tenant,'DLQ 页面验收临时项目']);
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[f.user,`replay-ui-${f.user}@integration.invalid`,'unused-test-hash']);
    await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[f.tenant,f.user]);
    await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(f.token),f.user,digest(f.csrf)]);
    let first:StoredEnvelope|undefined;
    for(const kind of ['success','unknown','conflict']){
      const record:StoredEnvelope=kind==='conflict'?{...first!,event:{...first!.event,properties:{test:true,conflict:true}}}:{tenant_id:f.tenant,project_id:f.project,received_at:new Date().toISOString(),event:{event_id:randomUUID(),event_name:'dlq_ui_fixture',distinct_id:'synthetic-'+kind,event_time:new Date().toISOString(),properties:{test:true},context:{source:'explicit-ui-fixture'},schema_version:1}};
      if(kind==='success')first=record;
      const dead:DeadLetter={id:digest(randomUUID()),reason:kind==='conflict'?'payload_conflict':'invalid_storage_value',tenant_id:f.tenant,project_id:f.project,identity_hashes:[identityHash(f.tenant,f.project,record.event.distinct_id)],source:{topic:'explicit-ui-fixture',partition:0,offset:'0'},raw_base64:Buffer.from(JSON.stringify(record)).toString('base64'),processor_version:2};
      await indexDeadLetter(dead,record);f.records.push({id:dead.id,kind,event_id:record.event.event_id});
    }
    // Redirect stdout into a 600 file. The short-lived session must never be
    // printed in terminal or browser snapshots; remove the file after cleanup.
    console.log(JSON.stringify(f));
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    assert.equal((await pool.query('SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE t.id=$1 AND p.id=$2 AND t.name=$3',[f.tenant,f.project,label])).rowCount,1);
    assert.equal((await pool.query("SELECT 1 FROM users WHERE id=$1 AND email LIKE 'replay-ui-%@integration.invalid'",[f.user])).rowCount,1);
    assert.equal((await pool.query('SELECT 1 FROM tenant_memberships WHERE user_id=$1 AND tenant_id<>$2',[f.user,f.tenant])).rowCount,0);
    if(mode==='verify'){
      const jobs=await pool.query('SELECT id,status FROM replay_jobs WHERE tenant_id=$1',[f.tenant]);assert.equal(jobs.rowCount,2);assert.ok(jobs.rows.every(j=>j.status==='completed'));
      const items=await pool.query('SELECT record_id,status,attempts FROM replay_items WHERE tenant_id=$1',[f.tenant]);assert.equal(items.rowCount,2);assert.ok(items.rows.every(i=>i.status==='stored'&&i.attempts===1));
      for(const kind of ['success','unknown'])assert.equal(items.rows.filter(i=>i.record_id===f.records.find((r:{kind:string})=>r.kind===kind).id).length,1,'lost response must reuse the task id');
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM events WHERE tenant_id=$1',[f.tenant])).rows[0].n,2);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM aggregation_jobs WHERE tenant_id=$1',[f.tenant])).rows[0].n,2);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM dlq_records WHERE tenant_id=$1',[f.tenant])).rows[0].n,3);
      assert.equal((await pool.query("SELECT count(*)::int AS n FROM audit_logs WHERE tenant_id=$1 AND action='dlq_replay_requested'",[f.tenant])).rows[0].n,2);
      console.log(JSON.stringify({passed:true,ui_tasks:2,unique_raw:2,aggregation_jobs:2,source_records_retained:3,lost_response_single_task:true}));
    }else if(mode==='empty'){
      assert.equal((await pool.query("SELECT 1 FROM projects WHERE tenant_id=$1 AND name='DLQ UI EMPTY'",[f.tenant])).rowCount,0);
      await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[randomUUID(),f.tenant,'DLQ UI EMPTY']);
      console.log('Empty UI fixture project created');
    }else if(mode==='viewer'){
      await pool.query("UPDATE tenant_memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",[f.tenant,f.user]);
      console.log('Synthetic account changed to viewer');
    }else if(mode==='cleanup'){
      const db=await pool.connect();try{
        await db.query('BEGIN');
        for(const table of ['replay_items','replay_jobs','recovery_import_scopes','deletion_barriers','deletion_jobs','query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects'])await db.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
        await db.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await db.query('DELETE FROM users WHERE id=$1',[f.user]);await db.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);
        await db.query('COMMIT');console.log('Synthetic UI fixture cleaned');
      }catch(e){await db.query('ROLLBACK');throw e;}finally{db.release();}
    }else throw new Error('invalid_ui_fixture_mode');
  }
}finally{await pool.end();}
