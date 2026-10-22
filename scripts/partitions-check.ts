import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {maintainPartitions,monthPartition} from '../packages/server/src/partitions';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const now=new Date((await pool.query('SELECT CURRENT_TIMESTAMP AS now')).rows[0].now),future=monthPartition(now,2),old=monthPartition(now,-4),occupied=monthPartition(now,-3),unknown=monthPartition(now,-5);
  await pool.query(`DROP TABLE public.${future.name}`);
  for(const plan of [old,occupied,unknown])await pool.query(`CREATE TABLE public.${plan===unknown?'events_unmanaged_archive':plan.name} PARTITION OF public.events FOR VALUES FROM ('${plan.from}') TO ('${plan.to}')`);
  const tenant=randomUUID(),project=randomUUID(),event=randomUUID();await pool.query("INSERT INTO tenants VALUES($1,'PARTITIONS TEMP')",[tenant]);await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,'PARTITIONS TEMP')",[project,tenant]);
  await pool.query("INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,event_time,received_at,properties,context,schema_version) VALUES($1,$2,$3,'view','user',$4,now(),'{}','{}',1)",[tenant,project,event,occupied.from]);
  const db=await pool.connect(),block=await pool.connect();
  try{
    await block.query('BEGIN');await block.query('SELECT pg_advisory_xact_lock(169202610)');assert.equal(await maintainPartitions(db),'busy');await block.query('ROLLBACK');
    await block.query('BEGIN');await block.query('LOCK TABLE ONLY public.events IN ROW EXCLUSIVE MODE');
    await assert.rejects(maintainPartitions(db),(error:unknown)=>(error as {code:string}).code==='55P03');await block.query('ROLLBACK');
    assert.equal(await maintainPartitions(db),'completed');
    assert.equal((await pool.query('SELECT to_regclass($1) AS name',[future.name])).rows[0].name,future.name);
    assert.equal((await pool.query('SELECT to_regclass($1) AS name',[old.name])).rows[0].name,null);
    assert.equal((await pool.query(`SELECT event_id FROM public.${occupied.name}`)).rows[0].event_id,event);
    assert.equal((await pool.query("SELECT to_regclass('public.events_unmanaged_archive') AS name")).rows[0].name,'events_unmanaged_archive');
    assert.equal(await maintainPartitions(db),'completed');
  }finally{await block.query('ROLLBACK');block.release();db.release();}
  console.log(JSON.stringify({passed:true,future_partition_created:true,migration_lock_skips:true,writer_lock_nowait_rolls_back:true,empty_old_partition_reclaimed:true,nonempty_old_partition_retained:true,unknown_partition_retained:true,idempotent:true}));
}finally{await pool.end();}
