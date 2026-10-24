// CI creates this DB using the actual pinned schema19 migration source.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {migrate} from '../packages/server/src/migrate';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts} from '../packages/server/src/operational-alert-store';
import {changeProjectRetention} from '../packages/server/src/project-retention';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,19);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const tenant=randomUUID(),project=randomUUID(),actor=randomUUID();
  await pool.query("INSERT INTO tenants VALUES($1,'UPGRADE TEMP')",[tenant]);
  await pool.query('INSERT INTO users VALUES($1,$2,$3)',[actor,`upgrade-${actor}@integration.invalid`,'unused']);
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,actor]);
  await pool.query("INSERT INTO projects(id,tenant_id,name,timezone,privacy_epoch) VALUES($1,$2,'UPGRADE TEMP','UTC',4)",[project,tenant]);
  for(const age of[1,10,31]){
    const id=randomUUID();
    await pool.query("INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,event_time,received_at,properties,context,schema_version) VALUES($1,$2,$3,'view','fixture',CURRENT_TIMESTAMP-$4::int*interval '1 day',clock_timestamp(),'{\"price\":20}','{}',1)",[tenant,project,id,age]);
    await pool.query("INSERT INTO event_dedup(tenant_id,project_id,event_id,payload_hash) VALUES($1,$2,$3,'original-hash')",[tenant,project,id]);
    await pool.query('INSERT INTO aggregation_jobs(tenant_id,project_id,event_id) VALUES($1,$2,$3)',[tenant,project,id]);
  }
  await pool.query("INSERT INTO event_count_buckets(tenant_id,project_id,event_name,bucket_start,event_count,privacy_epoch) VALUES($1,$2,'view',date_trunc('hour',CURRENT_TIMESTAMP-interval '1 day'),1,4)",[tenant,project]);
  await pool.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'upgrade_fixture','{\"preserve\":true}')",[tenant,actor]);
  await pool.query("INSERT INTO consumer_streams(topic,group_id,partition_count) VALUES('upgrade-fixture','upgrade-fixture',2)");
  await pool.query("INSERT INTO consumer_progress(topic,group_id,partition_id,next_offset) VALUES('upgrade-fixture','upgrade-fixture',0,9007199254740993),('upgrade-fixture','upgrade-fixture',1,42)");
  const db=await pool.connect();try{await storeOperationalSnapshot(db,new IntakeMetrics().snapshot());await evaluateAndStoreOperationalAlerts(db);}finally{db.release();}
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM operational_samples')).rows[0].n,1);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM operational_alert_states')).rows[0].n,4);
  const projections=(await pool.query(`SELECT table_name,string_agg(quote_ident(column_name),',' ORDER BY ordinal_position) AS columns FROM information_schema.columns c WHERE table_schema='public' AND table_name<>'schema_migrations' AND EXISTS(SELECT 1 FROM pg_tables p WHERE p.schemaname=c.table_schema AND p.tablename=c.table_name) GROUP BY table_name ORDER BY table_name`)).rows;
  const fingerprints=async()=>{
    const rows=[];
    for(const p of projections){assert.match(p.table_name,/^[a-z_0-9]+$/);rows.push({table:p.table_name,...(await pool.query(`SELECT count(*)::text AS count,COALESCE(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)::text AS hash FROM (SELECT ${p.columns} FROM public."${p.table_name}") t`)).rows[0]});}
    return rows;
  };
  const original=await fingerprints(),visible=Number((await pool.query('SELECT count(*) AS n FROM visible_events')).rows[0].n);assert.equal(visible,2);
  await pool.query("CREATE FUNCTION fail_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.version=22 THEN RAISE EXCEPTION 'synthetic migration failure'; END IF; RETURN NEW; END$$; CREATE TRIGGER fail_upgrade BEFORE INSERT ON schema_migrations FOR EACH ROW EXECUTE FUNCTION fail_upgrade()");
  try{
    await assert.rejects(migrate(),/synthetic migration failure/);
    assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,19);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM information_schema.columns WHERE table_schema='public' AND table_name='projects' AND column_name IN ('retention_days','retention_version','retention_floor')")).rows[0].n,0);
    assert.deepEqual(await fingerprints(),original);
  }finally{await pool.query('DROP TRIGGER fail_upgrade ON schema_migrations; DROP FUNCTION fail_upgrade()');}
  await migrate();assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,22);
  assert.deepEqual(await fingerprints(),original);
  assert.deepEqual((await pool.query('SELECT retention_days,retention_version,retention_floor,privacy_epoch FROM projects WHERE id=$1',[project])).rows[0],{retention_days:30,retention_version:1,retention_floor:null,privacy_epoch:4});
  assert.equal(Number((await pool.query('SELECT count(*) AS n FROM visible_events')).rows[0].n),visible);
  await migrate();assert.deepEqual(await fingerprints(),original);
  await changeProjectRetention(tenant,project,actor,{raw_days:7,expected_version:1});
  const policySql='SELECT retention_days,retention_version,retention_floor,privacy_epoch FROM projects WHERE id=$1';
  const configured=(await pool.query(policySql,[project])).rows[0],configuredRows=await fingerprints();
  await migrate();assert.deepEqual((await pool.query(policySql,[project])).rows[0],configured);assert.deepEqual(await fingerprints(),configuredRows);
  console.log(JSON.stringify({schema19_to_22:true,legacy_source:'1bd729e2bb44d1fbb75ef160bf6a2dab2bc263f3',original_relations:projections.length,original_columns_and_rows_preserved:true,migration_failure_atomic:true,defaults_30_days_version1_no_floor:true,privacy_epoch_unchanged:true,visibility_unchanged:true,large_offsets_and_monitor_rows_preserved:true,idempotent_migration:true,configured_schema20_policy_preserved:true}));
}finally{await pool.end();}
