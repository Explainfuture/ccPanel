// Operator-only fixture lifecycle. Create output contains credentials: redirect
// to a mode-600 file, transfer over SSH, and never include it in logs or Git.
import pg from 'pg';
import { randomUUID,randomBytes,scryptSync,createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
try{
  const mode=process.argv[2];
  if(mode==='create'){
    await pool.query('BEGIN');
    const tenant=randomUUID(),project=randomUUID(),user=randomUUID(),key=randomBytes(24).toString('hex'),password=randomBytes(24).toString('hex'),salt=randomBytes(16).toString('hex'),email=`public-load-${user}@integration.invalid`;
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'CAPACITY EXTERNAL TEMP']);
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[user,email,salt+':'+scryptSync(password,salt,64).toString('hex')]);
    await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,user]);
    await pool.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[project,tenant,'CAPACITY EXTERNAL TEMP',[process.env.PUBLIC_ORIGIN]]);
    await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(key).digest('hex'),'capacity-external']);
    await pool.query('COMMIT');
    console.log(JSON.stringify({tenant,project,user,key,email,password,origin:process.env.PUBLIC_ORIGIN,created_at:new Date().toISOString()}));
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    const owned=await pool.query("SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id JOIN tenant_memberships m ON m.tenant_id=t.id JOIN users u ON u.id=m.user_id WHERE t.id=$1 AND p.id=$2 AND m.user_id=$3 AND u.email=$4 AND u.email LIKE 'public-load-%@integration.invalid' AND t.name='CAPACITY EXTERNAL TEMP'",[f.tenant,f.project,f.user,f.email]);assert.equal(owned.rowCount,1,'expected isolated capacity fixture and its own user');
    if(mode==='verify'){
      const result=JSON.parse(await readFile(process.argv[3],'utf8'));
      assert.equal(result.tenant,f.tenant);assert.equal(result.project,f.project);
      assert.ok(Number.isInteger(result.generated)&&result.generated>0&&result.generated<=3600,'bounded generated count');
      assert.ok(Array.isArray(result.event_ids)&&result.event_ids.length===result.generated&&result.event_ids.every(id=>typeof id==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)),'valid exact event set');
      assert.equal(new Set(result.event_ids.map(id=>id.toLowerCase())).size,result.generated);
      const db=await pool.connect();let snapshot,aggregates;
      try{
        const deadline=Date.now()+15000;
        while(true){
          await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
          try{
            await db.query("SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='200ms'");
            const ownedSnapshot=await db.query("SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id JOIN tenant_memberships m ON m.tenant_id=t.id JOIN users u ON u.id=m.user_id WHERE t.id=$1 AND p.id=$2 AND m.user_id=$3 AND u.email=$4 AND u.email LIKE 'public-load-%@integration.invalid' AND t.name='CAPACITY EXTERNAL TEMP'",[f.tenant,f.project,f.user,f.email]);
            assert.equal(ownedSnapshot.rowCount,1,'expected isolated capacity fixture and its own user');
            const bucketSchema=(await db.query("SELECT to_regclass('public.event_count_buckets') IS NOT NULL AS buckets,to_regclass('public.visible_event_count_buckets') IS NOT NULL AS visible")).rows[0];
            assert.equal(bucketSchema.buckets&&bucketSchema.visible,true,'expected aggregate bucket schema');
            snapshot=(await db.query(`WITH expected AS (SELECT unnest($3::uuid[]) AS id),
              raw AS MATERIALIZED (SELECT event_id AS id FROM events WHERE tenant_id=$1 AND project_id=$2),
              registered AS MATERIALIZED (SELECT event_id AS id,status FROM event_dedup WHERE tenant_id=$1 AND project_id=$2),
              jobs AS MATERIALIZED (
                SELECT a.event_id AS id,a.processed_at,a.last_error_code,a.processed_epoch,p.privacy_epoch
                FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id
                WHERE a.tenant_id=$1 AND a.project_id=$2)
              SELECT (SELECT count(*)::int FROM raw) AS events,
                (SELECT count(*)::int FROM registered) AS dedup,(SELECT count(*)::int FROM jobs) AS aggregate_jobs,
                (SELECT count(*)::int FROM dlq_records WHERE tenant_id=$1 AND project_id=$2) AS dlq,
                (SELECT count(*)::int FROM registered WHERE id=ANY($3::uuid[])) AS unique_ids_matched,
                (SELECT count(*)::int FROM raw WHERE id=ANY($3::uuid[])) AS raw_ids_matched,
                (SELECT count(DISTINCT id)::int FROM raw) AS raw_unique_ids,
                (SELECT count(*)::int FROM jobs WHERE id=ANY($3::uuid[])) AS job_ids_matched,
                NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM raw)
                  UNION ALL (SELECT id FROM raw EXCEPT SELECT id FROM expected)) AS raw_set_exact,
                NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM registered)
                  UNION ALL (SELECT id FROM registered EXCEPT SELECT id FROM expected)) AS dedup_set_exact,
                NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM jobs)
                  UNION ALL (SELECT id FROM jobs EXCEPT SELECT id FROM expected)) AS job_set_exact,
                (SELECT count(*)::int FROM registered WHERE status<>'stored') AS failed_registrations,
                (SELECT count(*)::int FROM jobs WHERE processed_at IS NOT NULL
                  AND processed_epoch IS DISTINCT FROM privacy_epoch) AS completed_wrong_epoch,
                (SELECT count(*)::int FROM jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL
                  AND processed_epoch IS NOT DISTINCT FROM privacy_epoch) AS processed,
                (SELECT coalesce(sum(event_count),0)::text FROM visible_event_count_buckets WHERE tenant_id=$1 AND project_id=$2) AS bucket_events`,[f.tenant,f.project,result.event_ids])).rows[0];
            for(const name of ['events','dedup','aggregate_jobs'])assert.equal(snapshot[name],result.generated,name);
            assert.equal(snapshot.dlq,0);assert.equal(snapshot.unique_ids_matched,result.generated);
            assert.equal(snapshot.raw_ids_matched,result.generated,'exact raw event IDs');assert.equal(snapshot.raw_unique_ids,result.generated,'exact raw event IDs');assert.equal(snapshot.raw_set_exact,true,'exact raw event IDs');
            assert.equal(snapshot.dedup_set_exact,true,'exact dedup event IDs');assert.equal(snapshot.job_ids_matched,result.generated,'exact aggregation job IDs');assert.equal(snapshot.job_set_exact,true,'exact aggregation job IDs');
            assert.equal(snapshot.failed_registrations,0,'event registrations are stored');
            aggregates={processed:snapshot.processed,bucket_events:snapshot.bucket_events};
            // Current-epoch jobs and their buckets commit together. Work not
            // yet complete at the current epoch can still be recomputed.
            if(aggregates.processed===result.generated)assert.equal(Number(aggregates.bucket_events),result.generated,'visible bucket count reconciled');
          }finally{await db.query('ROLLBACK');}
          if(aggregates.processed===result.generated)break;
          if(Date.now()>=deadline){assert.equal(snapshot.completed_wrong_epoch,0,'completed aggregation epoch matches project');assert.equal(aggregates.processed,result.generated,'active aggregate jobs drained');}
          // Each retry needs a new snapshot to see the real worker's COMMIT.
          await new Promise(resolve=>setTimeout(resolve,500));
        }
      }finally{db.release();}
      const event_ids_sha256=createHash('sha256').update(JSON.stringify(result.event_ids.map(id=>id.toLowerCase()).sort())).digest('hex');
      const {unique_ids_matched,raw_ids_matched,raw_unique_ids,job_ids_matched,events,dedup,aggregate_jobs,dlq}=snapshot;
      console.log(JSON.stringify({passed:true,tenant:f.tenant,project:f.project,event_ids_sha256,unique_ids_matched,raw_ids_matched,raw_unique_ids,job_ids_matched,events,dedup,aggregate_jobs,dlq,exact_id_sets:true,dedup_stored:true,aggregates}));
    }else if(mode==='cleanup'){
      const buckets=(await pool.query("SELECT to_regclass('public.event_count_buckets') AS name")).rows[0].name;
      if(buckets)await pool.query('DELETE FROM event_count_buckets WHERE tenant_id=$1',[f.tenant]);
      for(const table of ['replay_items','replay_jobs','query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects'])await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
      await pool.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await pool.query('DELETE FROM users WHERE id=$1',[f.user]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);console.log('Isolated capacity fixture cleaned');
    }else throw new Error('Expected create, verify, or cleanup');
  }
}finally{await pool.end();}
