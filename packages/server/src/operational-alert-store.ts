import type {PoolClient} from 'pg';
import {queryOperationalAlerts} from './operational-alert-query';
export const operationalAlertHistoryLimit=5000;
export async function evaluateAndStoreOperationalAlerts(db:PoolClient){
  await db.query('BEGIN');
  try{
    await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','1000ms',true)");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202638) AS held')).rows[0].held){await db.query('ROLLBACK');return{status:'busy' as const,transitions:0};}
    const value=await queryOperationalAlerts(db),at=new Date(value.computed_at);
    const previous=(await db.query('SELECT rule,state,reason,evaluated_at,state_since,revision::text FROM operational_alert_states')).rows;
    if(previous.some(row=>row.evaluated_at.getTime()>=at.getTime())){await db.query('ROLLBACK');return{status:'stale' as const,transitions:0};}
    const changes=value.rules.filter(signal=>{const prior=previous.find(row=>row.rule===signal.rule);return !prior||prior.state!==signal.state||prior.reason!==signal.reason;});
    await db.query("DELETE FROM operational_alert_history WHERE id IN (SELECT id FROM operational_alert_history WHERE evaluated_at<CURRENT_TIMESTAMP-interval '14 days' ORDER BY evaluated_at,id LIMIT 100)");
    const count=Number((await db.query('SELECT count(*)::int AS n FROM operational_alert_history')).rows[0].n);
    const remove=Math.min(100,Math.max(0,count+changes.length-operationalAlertHistoryLimit));
    if(remove)await db.query('DELETE FROM operational_alert_history WHERE id IN (SELECT id FROM operational_alert_history ORDER BY evaluated_at,id LIMIT $1)',[remove]);
    if(count-remove+changes.length>operationalAlertHistoryLimit){await db.query('ROLLBACK');return{status:'full' as const,transitions:0};}
    for(const signal of value.rules){
      const prior=previous.find(row=>row.rule===signal.rule),changed=changes.includes(signal),revision=prior?BigInt(prior.revision)+(changed?1n:0n):1n;
      const since=prior&&prior.state===signal.state?prior.state_since:at;
      const fields=[signal.rule,signal.state,signal.reason,at,since,revision.toString(),JSON.stringify(signal)];
      await db.query('INSERT INTO operational_alert_states(rule,state,reason,evaluated_at,state_since,revision,signal) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(rule) DO UPDATE SET state=EXCLUDED.state,reason=EXCLUDED.reason,evaluated_at=EXCLUDED.evaluated_at,state_since=EXCLUDED.state_since,revision=EXCLUDED.revision,signal=EXCLUDED.signal',fields);
      if(changed)await db.query('INSERT INTO operational_alert_history(rule,state,reason,evaluated_at,state_since,revision,signal) VALUES($1,$2,$3,$4,$5,$6,$7)',fields);
    }
    await db.query('COMMIT');return{status:'stored' as const,transitions:changes.length};
  }catch(error){await db.query('ROLLBACK');throw error;}
}
