import type {PoolClient} from 'pg';
import {canonical,type Json} from '../../schema/src/index';
import {digest} from './security';
import {queryDlqHealth} from './dlq-health-query';
import {parseDlqHealthAssessment} from './dlq-health-observation';
export const dlqAlertHistoryLimit=5000;
// Read and write under the same global lock: different API instances assess PG
// directly, so there is no caller-supplied snapshot or process ordering claim.
export async function evaluateAndStoreDlqAlerts(db:PoolClient){
  await db.query('BEGIN');
  try{
    await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','1000ms',true)");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202640) AS held')).rows[0].held){await db.query('ROLLBACK');return{status:'busy' as const,transitions:0};}
    const value=parseDlqHealthAssessment(await queryDlqHealth(db));if(!value)throw new Error('invalid_dlq_health_observation');
    const at=new Date(value.computed_at),hash=digest(canonical(value as unknown as Json)),policyHash=digest(canonical(value.policy as unknown as Json));
    const previous=(await db.query('SELECT * FROM dlq_alert_states ORDER BY rule')).rows;
    const stop=async(status:'incomplete'|'stale'|'duplicate'|'conflict'|'revision_exhausted'|'full')=>{await db.query('ROLLBACK');return{status,transitions:0};};
    if(previous.length!==0&&previous.length!==2)return await stop('incomplete');
    if(previous.length){
      if(previous.some(r=>r.assessed_at.getTime()!==previous[0].assessed_at.getTime()||r.policy_hash!==previous[0].policy_hash||r.payload_hash!==previous[0].payload_hash))return await stop('incomplete');
      if(previous[0].assessed_at>at)return await stop('stale');
      if(previous[0].assessed_at.getTime()===at.getTime())return await stop(previous[0].payload_hash===hash?'duplicate':'conflict');
    }
    const rows=value.signals.map(signal=>{
      const old=previous.find(r=>r.rule===signal.rule),policy=!!old&&old.policy_hash!==policyHash,gap=!!old&&old.source_expires_at!==null&&old.source_expires_at<at;
      const kind=!old?'initial':old.state_at_assessment!==signal.state?'state':old.reason!==signal.reason?'reason':policy?'policy':gap?'gap':null;
      return{signal,kind,since:old&&old.state_at_assessment===signal.state&&!policy&&!gap?old.state_since:at,revision:old?BigInt(old.revision)+(kind?1n:0n):1n};
    });
    if(rows.some(r=>r.revision>9223372036854775807n))return await stop('revision_exhausted');
    await db.query("DELETE FROM dlq_alert_history WHERE id IN (SELECT id FROM dlq_alert_history WHERE assessed_at<CURRENT_TIMESTAMP-interval '14 days' ORDER BY assessed_at,id LIMIT 100)");
    const count=Number((await db.query('SELECT count(*)::int AS n FROM dlq_alert_history')).rows[0].n),transitions=rows.filter(r=>r.kind).length,remove=Math.min(100,Math.max(0,count+transitions-dlqAlertHistoryLimit));
    if(remove)await db.query('DELETE FROM dlq_alert_history WHERE id IN (SELECT id FROM dlq_alert_history ORDER BY assessed_at,id LIMIT $1)',[remove]);
    if(count-remove+transitions>dlqAlertHistoryLimit)return await stop('full');
    for(const row of rows){
      const s=row.signal,args=[s.rule,s.state,s.reason,at,row.since,row.revision.toString(),s.observed_at,s.expires_at,policyHash,hash,JSON.stringify({signal:s,policy:value.policy,categories:value.categories})];
      await db.query('INSERT INTO dlq_alert_states(rule,state_at_assessment,reason,assessed_at,state_since,revision,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(rule) DO UPDATE SET state_at_assessment=EXCLUDED.state_at_assessment,reason=EXCLUDED.reason,assessed_at=EXCLUDED.assessed_at,state_since=EXCLUDED.state_since,revision=EXCLUDED.revision,source_observed_at=EXCLUDED.source_observed_at,source_expires_at=EXCLUDED.source_expires_at,policy_hash=EXCLUDED.policy_hash,payload_hash=EXCLUDED.payload_hash,evidence=EXCLUDED.evidence',args);
      if(row.kind)await db.query('INSERT INTO dlq_alert_history(rule,state_at_assessment,reason,assessed_at,state_since,revision,source_observed_at,source_expires_at,policy_hash,payload_hash,evidence,change_kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)',[...args,row.kind]);
    }
    await db.query('COMMIT');return{status:'stored' as const,transitions};
  }catch(error){await db.query('ROLLBACK');throw error;}
}
