import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {WorkerMetrics} from '../packages/server/src/worker-metrics';
import {storeOperationalSnapshot,operationalHistoryLimit} from '../packages/server/src/operational-history';
import {startOperationalCollection} from '../packages/server/src/operational-collection';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,22);
  const db=await pool.connect(),blocker=await pool.connect();
  const count=async()=>Number((await pool.query('SELECT count(*)::int AS n FROM operational_samples')).rows[0].n);
  try{
    const snapshot=new IntakeMetrics().snapshot();snapshot.responses[0]!.batches='9007199254740993';
    assert.equal((await storeOperationalSnapshot(db,snapshot)).status,'stored');assert.equal((await storeOperationalSnapshot(db,snapshot)).status,'duplicate');assert.equal(await count(),1);
    const conflict=structuredClone(snapshot);conflict.responses[0]!.batches='7';assert.equal((await storeOperationalSnapshot(db,conflict)).status,'conflict');
    const restarted=structuredClone(snapshot);restarted.observed_at=new Date(Date.parse(snapshot.observed_at)+1).toISOString();restarted.started_at=new Date(Date.parse(snapshot.started_at)-1).toISOString();assert.equal((await storeOperationalSnapshot(db,restarted)).status,'conflict','retained instance start must not change');
    assert.equal((await pool.query("SELECT snapshot->'responses'->0->>'batches' AS n FROM operational_samples")).rows[0].n,'9007199254740993');
    await blocker.query('BEGIN');await blocker.query('SELECT pg_advisory_xact_lock(169202637)');assert.equal((await storeOperationalSnapshot(db,new WorkerMetrics().snapshot())).status,'busy');await blocker.query('ROLLBACK');assert.equal(await count(),1);
    const previousConfig=process.env.OPERATIONAL_COLLECTION_ENABLED;process.env.OPERATIONAL_COLLECTION_ENABLED='false';let disabledReads=0;
    try{const stopDisabled=startOperationalCollection(()=>{disabledReads++;return new IntakeMetrics().snapshot();},()=>{disabledReads++;return evaluatePressureAlerts(null);});await new Promise(resolve=>setTimeout(resolve,31000));await stopDisabled();assert.equal(disabledReads,0);assert.equal(await count(),1);assert.equal((await pool.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='ccpanel_operational_history' AND datname=current_database()")).rows[0].n,0);}
    finally{if(previousConfig===undefined)delete process.env.OPERATIONAL_COLLECTION_ENABLED;else process.env.OPERATIONAL_COLLECTION_ENABLED=previousConfig;}
    await pool.query("INSERT INTO operational_samples(source,instance_id,started_at,observed_at,payload_hash,snapshot) VALUES('api',$1,CURRENT_TIMESTAMP-interval '9 days',CURRENT_TIMESTAMP-interval '8 days',$2,'{}')",[randomUUID(),'0'.repeat(64)]);
    assert.equal((await storeOperationalSnapshot(db,new WorkerMetrics().snapshot())).expired_deleted,1);assert.equal(await count(),2);
    await pool.query('DELETE FROM operational_samples');
    await pool.query("INSERT INTO operational_samples(source,instance_id,started_at,observed_at,payload_hash,snapshot) SELECT 'api',$1,CURRENT_TIMESTAMP-interval '1 day',CURRENT_TIMESTAMP-(n*interval '1 second'),$2,'{}' FROM generate_series(1,$3) n",[randomUUID(),'0'.repeat(64),operationalHistoryLimit]);
    const before=(await pool.query('SELECT min(observed_at) AS oldest FROM operational_samples')).rows[0].oldest.toISOString();
    await pool.query("CREATE FUNCTION fail_operational_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic_history_write_failure'; END $$; CREATE TRIGGER operational_fail BEFORE INSERT ON operational_samples FOR EACH ROW EXECUTE FUNCTION fail_operational_insert()");
    await assert.rejects(storeOperationalSnapshot(db,new IntakeMetrics().snapshot()),/synthetic_history_write_failure/);
    assert.equal(await count(),operationalHistoryLimit);assert.equal((await pool.query('SELECT min(observed_at) AS oldest FROM operational_samples')).rows[0].oldest.toISOString(),before);
    await pool.query('DROP TRIGGER operational_fail ON operational_samples;DROP FUNCTION fail_operational_insert()');
    const latest=new IntakeMetrics().snapshot(),capped=await storeOperationalSnapshot(db,latest);assert.equal(capped.status,'stored');assert.equal(capped.capacity_deleted,1);assert.equal(await count(),operationalHistoryLimit);assert.equal((await pool.query('SELECT 1 FROM operational_samples WHERE instance_id=$1',[latest.instance_id])).rowCount,1);
    const invalid=new IntakeMetrics().snapshot() as unknown as Record<string,unknown>;invalid.secret='must-not-store';await assert.rejects(storeOperationalSnapshot(db,invalid as never),/invalid_operational_snapshot/);assert.equal(await count(),operationalHistoryLimit);
    let pressureReads=0,workerPressureReads=0,apiSnapshots=0;
    const collected=new WorkerMetrics(),instance=collected.snapshot().instance_id;
    const stop=startOperationalCollection(()=>collected.snapshot(),()=>{workerPressureReads++;return evaluatePressureAlerts(null);});
    try{
      const deadline=Date.now()+40000;let stored=false;
      while(Date.now()<deadline){if((await pool.query('SELECT 1 FROM operational_samples WHERE instance_id=$1',[instance])).rowCount){stored=true;break;}await new Promise(resolve=>setTimeout(resolve,250));}
      assert.equal(stored,true,'real worker collector must persist its scheduled snapshot');
    }finally{await stop();}
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM dlq_alert_states')).rows[0].n,0,'worker never evaluates global DLQ alerts');
    // Contention is explicitly tested above. Two aligned try-lock timers need
    // not take turns fairly; verify each real scheduler in its own window.
    const api=new IntakeMetrics();
    const stopApi=startOperationalCollection(()=>{apiSnapshots++;return api.snapshot();},()=>{pressureReads++;return evaluatePressureAlerts(null);});
    try{
      const deadline=Date.now()+40000;let stored=false;
      while(Date.now()<deadline){
        const samples=(await pool.query('SELECT count(DISTINCT instance_id)::int AS n FROM operational_samples WHERE instance_id=ANY($1::uuid[])',[[instance,api.snapshot().instance_id]])).rows[0].n;
        const pressure=(await pool.query('SELECT count(*)::int AS n FROM pressure_signal_states WHERE instance_id=$1',[api.snapshot().instance_id])).rows[0].n;
        if(samples===2&&pressure===3){stored=true;break;}await new Promise(resolve=>setTimeout(resolve,250));
      }
      assert.equal(stored,true,'real API collector must persist its snapshot and pressure evidence');
    }finally{await stopApi();}
    assert.equal(pressureReads,apiSnapshots,'pressure is captured once per existing API snapshot');assert.equal(apiSnapshots,1);
    assert.equal(workerPressureReads,0,'worker never owns pressure observations');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM pressure_signal_states WHERE instance_id=$1',[api.snapshot().instance_id])).rows[0].n,3);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM pressure_signal_history')).rows[0].n,3);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM operational_alert_states')).rows[0].n,4,'scheduled collector must persist fixed alert states before draining');
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM dlq_alert_states')).rows[0].n,2);assert.equal((await pool.query('SELECT count(*)::int AS n FROM dlq_alert_history')).rows[0].n,2);
    assert.equal(await count(),operationalHistoryLimit);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='ccpanel_operational_history' AND datname=current_database()")).rows[0].n,0,'collector pool must close');
    console.log(JSON.stringify({operational_history:true,schema22:true,precise_counters:true,idempotent:true,immutable_conflict:true,lock_contention_skips:true,expiry_prunes:true,global_cap:operationalHistoryLimit,failed_insert_restores_evicted_row:true,invalid_payload_no_write:true,scheduled_collection:true,scheduled_api_pressure:true,pressure_callback_once:true,disabled_pressure_not_read:true,collector_pool_closed:true}));
  }finally{await blocker.query('ROLLBACK');blocker.release();db.release();}
}finally{await pool.end();}
