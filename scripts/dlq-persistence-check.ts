// Harness bootstraps the actual deployed schema21 source, not a reverse migration.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {migrate} from '../packages/server/src/migrate';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts} from '../packages/server/src/operational-alert-store';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {storePressureObservation} from '../packages/server/src/pressure-history-store';
import {evaluateAndStoreDlqAlerts,dlqAlertHistoryLimit} from '../packages/server/src/dlq-alert-store';
import {evaluateDlqHealth} from '../packages/server/src/dlq-health';
import {canonical,type Json} from '../packages/schema/src/index';
import {digest} from '../packages/server/src/security';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,21);
  const tenant=randomUUID(),project=randomUUID(),actor=randomUUID();
  await pool.query("INSERT INTO tenants VALUES($1,'DLQ PERSIST TEMP')",[tenant]);await pool.query('INSERT INTO users VALUES($1,$2,$3)',[actor,`dlq-persist-${actor}@integration.invalid`,'unused']);await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,actor]);
  await pool.query("INSERT INTO projects(id,tenant_id,name,retention_days,retention_version,retention_floor,privacy_epoch) VALUES($1,$2,'DLQ PERSIST TEMP',7,4,clock_timestamp()-interval '7 days',9)",[project,tenant]);
  for(const age of[1,10])await pool.query("INSERT INTO events(tenant_id,project_id,event_id,event_name,distinct_id,event_time,received_at,properties,context,schema_version) VALUES($1,$2,$3,'persist','fixture',clock_timestamp()-$4::int*interval '1 day',clock_timestamp(),'{}','{}',1)",[tenant,project,randomUUID(),age]);
  await pool.query("INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES('original',$1,$2,'fixture','{\"redacted\":true,\"secret\":\"old-source\"}')",[tenant,project]);
  const db=await pool.connect(),blocker=await pool.connect();
  try{
    const snapshot=new IntakeMetrics().snapshot();await storeOperationalSnapshot(db,snapshot);await evaluateAndStoreOperationalAlerts(db);await storePressureObservation(db,{instance_id:snapshot.instance_id,started_at:snapshot.started_at,assessment:evaluatePressureAlerts(null)});
    const projections=(await pool.query(`SELECT table_name,string_agg(quote_ident(column_name),',' ORDER BY ordinal_position) AS columns FROM information_schema.columns c WHERE table_schema='public' AND table_name<>'schema_migrations' AND EXISTS(SELECT 1 FROM pg_tables p WHERE p.schemaname=c.table_schema AND p.tablename=c.table_name) GROUP BY table_name ORDER BY table_name`)).rows;
    const fingerprint=async()=>{const out=[];for(const p of projections){assert.match(p.table_name,/^[a-z_0-9]+$/);out.push({table:p.table_name,...(await pool.query(`SELECT count(*)::text AS n,COALESCE(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)::text AS hash FROM (SELECT ${p.columns} FROM public."${p.table_name}") t`)).rows[0]});}return out;};
    const original=await fingerprint();assert.equal((await pool.query('SELECT count(*)::int AS n FROM visible_events')).rows[0].n,1);
    await pool.query("CREATE FUNCTION fail_dlq_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.version=22 THEN RAISE EXCEPTION 'dlq_upgrade_failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER dlq_upgrade_fail BEFORE INSERT ON schema_migrations FOR EACH ROW EXECUTE FUNCTION fail_dlq_upgrade()");
    await assert.rejects(migrate(),/dlq_upgrade_failure/);assert.equal((await pool.query('SELECT max(version) AS n FROM schema_migrations')).rows[0].n,21);assert.equal((await pool.query("SELECT to_regclass('dlq_alert_states') AS name")).rows[0].name,null);assert.deepEqual(await fingerprint(),original);
    await pool.query('DROP TRIGGER dlq_upgrade_fail ON schema_migrations; DROP FUNCTION fail_dlq_upgrade()');await migrate();await migrate();assert.deepEqual(await fingerprint(),original);
    const states=async()=>(await pool.query('SELECT * FROM dlq_alert_states ORDER BY rule')).rows;
    const history=async()=>(await pool.query('SELECT count(*)::int AS n,min(id)::text AS oldest,COALESCE(bit_xor(hashtextextended(row_to_json(h)::text,0)),0)::text AS hash FROM dlq_alert_history h')).rows[0];
    const tick=async()=>{await db.query('SELECT pg_sleep(0.003)');return evaluateAndStoreDlqAlerts(db);};
    assert.equal((await tick()).transitions,2);const first=await states();assert.ok(first.every(r=>r.state_at_assessment==='clear'));assert.equal((await history()).n,2);
    assert.equal((await tick()).transitions,0);assert.deepEqual((await states()).map(r=>[r.revision,r.state_since]),first.map(r=>[r.revision,r.state_since]));
    await pool.query("INSERT INTO dlq_records(id,reason,payload) VALUES('pending','fixture','{}')");assert.equal((await tick()).transitions,2);assert.equal((await states())[0].state_at_assessment,'firing');
    await pool.query("UPDATE dlq_records SET created_at=clock_timestamp()-interval '13 days' WHERE id='pending'");assert.equal((await tick()).transitions,1);const firingSince=(await states())[1].state_since;
    await pool.query("UPDATE dlq_records SET created_at=clock_timestamp()-interval '15 days' WHERE id='pending'");assert.equal((await tick()).transitions,1);assert.equal((await states())[1].reason,'retention_window_exceeded');assert.deepEqual((await states())[1].state_since,firingSince);
    await pool.query("INSERT INTO dlq_records(id,reason,payload,created_at) VALUES('future','fixture','{}',clock_timestamp()+interval '1 day')");assert.equal((await tick()).transitions,2);const unknown=await states();assert.ok(unknown.every(r=>r.state_at_assessment==='unknown'&&r.source_expires_at===null));assert.equal((await tick()).transitions,0);assert.deepEqual((await states()).map(r=>r.revision),unknown.map(r=>r.revision));
    await pool.query("DELETE FROM dlq_records WHERE id IN ('future','pending')");assert.equal((await tick()).transitions,2);
    // Valid old evidence simulates the observation gap without sleeping 90s.
    const past=Date.now()-120000,assessment=evaluateDlqHealth({observed_at:new Date(past).toISOString(),counts:{unresolved:'0',replaying:'0',resolved:'0',suppressed:'1'},oldest_pending_at:null,future_records:'0'},past),hash=digest(canonical(assessment as unknown as Json));
    for(const s of assessment.signals)await pool.query('UPDATE dlq_alert_states SET assessed_at=$1,state_since=$1,source_observed_at=$2,source_expires_at=$3,payload_hash=$4,evidence=$5 WHERE rule=$6',[assessment.computed_at,s.observed_at,s.expires_at,hash,JSON.stringify({signal:s,policy:assessment.policy,categories:assessment.categories}),s.rule]);
    assert.equal((await tick()).transitions,2);assert.equal((await pool.query("SELECT count(*)::int AS n FROM dlq_alert_history WHERE change_kind='gap'")).rows[0].n,2);
    await pool.query("UPDATE dlq_alert_states SET policy_hash=repeat('0',64)");assert.equal((await tick()).transitions,2);assert.equal((await pool.query("SELECT count(*)::int AS n FROM dlq_alert_history WHERE change_kind='policy'")).rows[0].n,2);
    await blocker.query('BEGIN');await blocker.query('SELECT pg_advisory_xact_lock(169202640)');const beforeBusy=await states();assert.equal((await tick()).status,'busy');assert.deepEqual(await states(),beforeBusy);await blocker.query('ROLLBACK');
    await pool.query("UPDATE dlq_alert_states SET assessed_at=statement_timestamp()+interval '1 second'");assert.equal((await tick()).status,'stale');await pool.query('UPDATE dlq_alert_states SET assessed_at=$1',[beforeBusy[0].assessed_at]);
    const missing=(await states())[0];await pool.query('DELETE FROM dlq_alert_states WHERE rule=$1',[missing.rule]);const incomplete=await history();assert.equal((await tick()).status,'incomplete');assert.deepEqual(await history(),incomplete);await pool.query('INSERT INTO dlq_alert_states SELECT * FROM jsonb_populate_record(NULL::dlq_alert_states,$1)',[JSON.stringify(missing)]);
    await assert.rejects(pool.query("UPDATE dlq_alert_states SET evidence='{}'"));await assert.rejects(pool.query("UPDATE dlq_alert_states SET evidence=evidence #- '{signal,observed_at}'"));
    await pool.query('DELETE FROM dlq_alert_history');
    const seed=`INSERT INTO dlq_alert_history(rule,state_at_assessment,reason,assessed_at,state_since,revision,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,change_kind) SELECT rule,state_at_assessment,reason,assessed_at,state_since,$1::bigint+n,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,'initial' FROM dlq_alert_states CROSS JOIN generate_series(1,$2::int) n WHERE rule='dlq_pending'`;
    await pool.query(seed,[1000,dlqAlertHistoryLimit]);await pool.query("UPDATE dlq_alert_states SET revision=6000 WHERE rule='dlq_pending'");const beforeFull=await states(),full=await history();
    await pool.query("INSERT INTO dlq_records(id,reason,payload) VALUES('capacity-trigger','fixture','{}')");await pool.query("CREATE FUNCTION fail_dlq_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'dlq_history_failure'; END $$; CREATE TRIGGER dlq_history_fail BEFORE INSERT ON dlq_alert_history FOR EACH ROW EXECUTE FUNCTION fail_dlq_history()");
    await assert.rejects(tick(),/dlq_history_failure/);assert.deepEqual(await states(),beforeFull);assert.deepEqual(await history(),full);
    await pool.query('DROP TRIGGER dlq_history_fail ON dlq_alert_history; DROP FUNCTION fail_dlq_history()');assert.equal((await tick()).transitions,2);assert.equal((await history()).n,dlqAlertHistoryLimit);
    const capacityRevision=(await states())[0].revision;await pool.query(seed,[10000,201]);await pool.query("UPDATE dlq_alert_states SET revision=10201 WHERE rule='dlq_pending'");const tooFull=await history(),tooFullStates=await states();assert.equal((await tick()).status,'full');assert.deepEqual(await history(),tooFull);assert.deepEqual(await states(),tooFullStates);
    await pool.query('DELETE FROM dlq_alert_history WHERE revision>10000');await pool.query("UPDATE dlq_alert_states SET revision=$1 WHERE rule='dlq_pending'",[capacityRevision]);
    await pool.query(`UPDATE dlq_alert_history SET assessed_at=assessed_at-interval '15 days',state_since=state_since-interval '15 days',source_observed_at=source_observed_at-interval '15 days',source_expires_at=source_expires_at-interval '15 days',evidence=jsonb_set(jsonb_set(jsonb_set(evidence,'{signal,observed_at}',to_jsonb(to_char(source_observed_at-interval '15 days','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))),'{signal,expires_at}',to_jsonb(to_char(source_expires_at-interval '15 days','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))),'{signal,oldest_pending_at}',COALESCE(to_jsonb(to_char((evidence->'signal'->>'oldest_pending_at')::timestamptz-interval '15 days','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),'null'::jsonb))`);
    assert.equal((await tick()).transitions,0);assert.equal((await history()).n,dlqAlertHistoryLimit-100);
    await pool.query("UPDATE dlq_alert_states SET revision=9223372036854775807 WHERE rule='dlq_pending'");await pool.query("DELETE FROM dlq_records WHERE id='capacity-trigger'");const overflow=await states(),overflowHistory=await history();assert.equal((await tick()).status,'revision_exhausted');assert.deepEqual(await states(),overflow);assert.deepEqual(await history(),overflowHistory);
    assert.deepEqual(await fingerprint(),original);
    console.log(JSON.stringify({passed:true,schema21_to_22:true,migration_failure_atomic:true,original_fields_and_custom_policy_preserved:true,fixed_states:2,stable_revision:true,clear_firing_reason_unknown:true,gap_policy_transitions:true,unknown_no_gap_spam:true,busy_stale_incomplete_refused:true,evidence_constraints:true,history_limit:dlqAlertHistoryLimit,failed_history_rolls_back_state_and_eviction:true,oversized_refused:true,bounded_expiry_100:true,revision_overflow_refused:true}));
  }finally{await blocker.query('ROLLBACK');blocker.release();db.release();}
}finally{await pool.end();}
