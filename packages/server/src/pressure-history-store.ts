import type {PoolClient} from 'pg';
import {parsePressureObservation} from './pressure-observation';
export const pressureHistoryLimit=5000;
export async function storePressureObservation(db:PoolClient,input:unknown){
  // Reject malformed input before taking a pool transaction or pruning data.
  const value=parsePressureObservation(input);
  await db.query('BEGIN');
  try{
    await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','1000ms',true)");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202639) AS held')).rows[0].held){await db.query('ROLLBACK');return{status:'busy' as const,transitions:0};}
    const at=(await db.query('SELECT clock_timestamp() AS at')).rows[0].at as Date;
    if(value.assessed>at){await db.query('ROLLBACK');return{status:'future' as const,transitions:0};}
    const prior=(await db.query('SELECT * FROM pressure_signal_states ORDER BY rule')).rows;
    const stop=async(status:'conflict'|'superseded'|'stale'|'incomplete'|'duplicate'|'full'|'revision_exhausted')=>{await db.query('ROLLBACK');return{status,transitions:0};};
    if(prior.length!==0&&prior.length!==3)return await stop('incomplete');
    if(prior.length){
      if(prior.some(r=>r.instance_id!==prior[0].instance_id||r.started_at.getTime()!==prior[0].started_at.getTime()||r.assessed_at.getTime()!==prior[0].assessed_at.getTime()||r.payload_hash!==prior[0].payload_hash||r.policy_hash!==prior[0].policy_hash||r.recorded_at.getTime()!==prior[0].recorded_at.getTime()))return await stop('incomplete');
      const previous=prior[0];
      if(previous.instance_id===value.value.instance_id&&previous.started_at.getTime()!==value.started.getTime())return await stop('conflict');
      if(previous.instance_id!==value.value.instance_id&&previous.started_at.getTime()===value.started.getTime())return await stop('conflict');
      if(value.started<previous.started_at)return await stop('superseded');
      if(value.assessed<previous.assessed_at)return await stop('stale');
      if(value.assessed.getTime()===previous.assessed_at.getTime())return await stop(previous.instance_id===value.value.instance_id&&previous.payload_hash===value.payload_hash?'duplicate':'conflict');
    }
    const changes=value.value.assessment.signals.map(signal=>{
      const old=prior.find(r=>r.rule===signal.rule),source=!!old&&old.instance_id!==value.value.instance_id,policy=!!old&&old.policy_hash!==value.policy_hash;
      const gap=!!old&&old.state_at_assessment!=='unknown'&&old.signal_expires_at!==null&&old.signal_expires_at.getTime()<value.assessed.getTime();
      const kind=!old?'initial':old.state_at_assessment!==signal.state?'state':old.reason!==signal.reason?'reason':policy?'policy':source?'source':gap?'gap':null;
      const since=old&&old.state_at_assessment===signal.state&&!policy&&!source&&!gap?old.state_since:value.assessed;
      const revision=old?BigInt(old.revision)+(kind?1n:0n):1n;
      return{signal,kind,since,revision};
    });
    if(changes.some(r=>r.revision>9223372036854775807n))return await stop('revision_exhausted');
    await db.query("DELETE FROM pressure_signal_history WHERE id IN (SELECT id FROM pressure_signal_history WHERE assessed_at<CURRENT_TIMESTAMP-interval '14 days' ORDER BY assessed_at,id LIMIT 100)");
    const count=Number((await db.query('SELECT count(*)::int AS n FROM pressure_signal_history')).rows[0].n),transitionCount=changes.filter(r=>r.kind).length;
    const remove=Math.min(100,Math.max(0,count+transitionCount-pressureHistoryLimit));
    if(remove)await db.query('DELETE FROM pressure_signal_history WHERE id IN (SELECT id FROM pressure_signal_history ORDER BY assessed_at,id LIMIT $1)',[remove]);
    if(count-remove+transitionCount>pressureHistoryLimit)return await stop('full');
    for(const row of changes){
      const s=row.signal,args=[s.rule,s.state,s.reason,value.value.instance_id,value.started,value.assessed,at,row.since,row.revision.toString(),s.expires_at,value.policy_hash,value.payload_hash,JSON.stringify({signal:s,policy:value.value.assessment.policy})];
      await db.query('INSERT INTO pressure_signal_states(rule,state_at_assessment,reason,instance_id,started_at,assessed_at,recorded_at,state_since,revision,signal_expires_at,policy_hash,payload_hash,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(rule) DO UPDATE SET state_at_assessment=EXCLUDED.state_at_assessment,reason=EXCLUDED.reason,instance_id=EXCLUDED.instance_id,started_at=EXCLUDED.started_at,assessed_at=EXCLUDED.assessed_at,recorded_at=EXCLUDED.recorded_at,state_since=EXCLUDED.state_since,revision=EXCLUDED.revision,signal_expires_at=EXCLUDED.signal_expires_at,policy_hash=EXCLUDED.policy_hash,payload_hash=EXCLUDED.payload_hash,evidence=EXCLUDED.evidence',args);
      if(row.kind)await db.query('INSERT INTO pressure_signal_history(rule,state_at_assessment,reason,instance_id,started_at,assessed_at,recorded_at,state_since,revision,signal_expires_at,policy_hash,payload_hash,evidence,change_kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)',[...args,row.kind]);
    }
    await db.query('COMMIT');return{status:'stored' as const,transitions:transitionCount};
  }catch(error){await db.query('ROLLBACK');throw error;}
}
