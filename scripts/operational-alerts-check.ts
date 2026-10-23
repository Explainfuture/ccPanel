import assert from 'node:assert/strict';
import {pool} from '../packages/server/src/db';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts,operationalAlertHistoryLimit} from '../packages/server/src/operational-alert-store';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,20);
  const db=await pool.connect(),blocker=await pool.connect();
  const states=async()=>(await pool.query('SELECT rule,state,reason,evaluated_at,state_since,revision::text FROM operational_alert_states ORDER BY rule')).rows;
  const count=async()=>Number((await pool.query('SELECT count(*)::int AS n FROM operational_alert_history')).rows[0].n);
  try{
    assert.equal((await evaluateAndStoreOperationalAlerts(db)).transitions,4);assert.equal((await states()).length,4);assert.equal(await count(),4);
    const first=await states();await new Promise(resolve=>setTimeout(resolve,5));assert.equal((await evaluateAndStoreOperationalAlerts(db)).transitions,0);assert.equal(await count(),4);assert.deepEqual((await states()).map(row=>[row.rule,row.state_since,row.revision]),first.map(row=>[row.rule,row.state_since,row.revision]));
    await blocker.query('BEGIN');await blocker.query('SELECT pg_advisory_xact_lock(169202638)');assert.equal((await evaluateAndStoreOperationalAlerts(db)).status,'busy');await blocker.query('ROLLBACK');
    await pool.query("UPDATE operational_alert_states SET evaluated_at=clock_timestamp()+interval '1 hour'");assert.equal((await evaluateAndStoreOperationalAlerts(db)).status,'stale');assert.equal(await count(),4);await pool.query('UPDATE operational_alert_states SET evaluated_at=state_since');
    const latest=new IntakeMetrics().snapshot(),now=Date.now();latest.started_at=new Date(now-60000).toISOString();latest.observed_at=new Date(now-1000).toISOString();const prior=structuredClone(latest);prior.observed_at=new Date(now-30000).toISOString();
    await storeOperationalSnapshot(db,prior);await storeOperationalSnapshot(db,latest);
    assert.equal((await evaluateAndStoreOperationalAlerts(db)).transitions,2);assert.equal((await states()).find(row=>row.rule==='api_server_errors')!.state,'clear');
    const failed=structuredClone(latest);failed.observed_at=new Date(now).toISOString();failed.responses.find(row=>row.status==='500')!.batches='1';await storeOperationalSnapshot(db,failed);assert.equal((await evaluateAndStoreOperationalAlerts(db)).transitions,1);assert.equal((await states()).find(row=>row.rule==='api_server_errors')!.state,'firing');
    await pool.query('DELETE FROM operational_alert_history');
    await pool.query("INSERT INTO operational_alert_history(rule,state,reason,evaluated_at,state_since,revision,signal) SELECT rule,state,reason,clock_timestamp()-interval '1 day',state_since-interval '2 days',10000+n,signal FROM operational_alert_states CROSS JOIN generate_series(1,$1) n WHERE rule='api_sampling'",[operationalAlertHistoryLimit]);
    const oldest=(await pool.query('SELECT min(id)::text AS n FROM operational_alert_history')).rows[0].n;
    await pool.query('DELETE FROM operational_samples');const before=await states();
    await pool.query("CREATE FUNCTION fail_alert_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic_alert_history_failure'; END $$;CREATE TRIGGER alert_history_fail BEFORE INSERT ON operational_alert_history FOR EACH ROW EXECUTE FUNCTION fail_alert_history()");
    await assert.rejects(evaluateAndStoreOperationalAlerts(db),/synthetic_alert_history_failure/);assert.equal(await count(),operationalAlertHistoryLimit);assert.equal((await pool.query('SELECT min(id)::text AS n FROM operational_alert_history')).rows[0].n,oldest);assert.deepEqual(await states(),before);
    await pool.query('DROP TRIGGER alert_history_fail ON operational_alert_history;DROP FUNCTION fail_alert_history()');assert.equal((await evaluateAndStoreOperationalAlerts(db)).transitions,2);assert.equal(await count(),operationalAlertHistoryLimit);assert.equal((await states()).find(row=>row.rule==='api_server_errors')!.state,'unknown','loss of evidence must not resolve to clear');
    await pool.query("UPDATE operational_alert_history SET evaluated_at=clock_timestamp()-interval '15 days',state_since=clock_timestamp()-interval '16 days'");await new Promise(resolve=>setTimeout(resolve,5));await evaluateAndStoreOperationalAlerts(db);assert.equal(await count(),operationalAlertHistoryLimit-100,'expiry cleanup remains bounded');
    console.log(JSON.stringify({passed:true,schema20:true,fixed_states:4,stable_revision:true,busy_skip:true,stale_write_rejected:true,clear_firing_unknown:true,history_limit:operationalAlertHistoryLimit,failed_history_rolls_back_state_and_eviction:true,bounded_expiry:true}));
  }finally{await blocker.query('ROLLBACK');blocker.release();db.release();}
}finally{await pool.end();}
