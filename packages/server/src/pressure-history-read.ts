import type {PoolClient} from 'pg';
import {withOperationalRead} from './operational-history-read';
import {parsePressureObservation} from './pressure-observation';

// One SQL snapshot keeps states/history coherent while the collector commits.
export async function queryPressureHistory(db:PoolClient){
  const data=(await db.query(`SELECT
    COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY rule) FROM
      (SELECT rule,state_at_assessment,reason,instance_id,started_at,assessed_at,recorded_at,state_since,revision::text,signal_expires_at,policy_hash,payload_hash,evidence FROM pressure_signal_states) s),'[]'::jsonb) AS states,
    COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY assessed_at DESC,id::bigint DESC) FROM
      (SELECT id::text,rule,state_at_assessment,reason,instance_id,started_at,assessed_at,recorded_at,state_since,revision::text,signal_expires_at,policy_hash,payload_hash,evidence,change_kind
       FROM pressure_signal_history WHERE assessed_at>=CURRENT_TIMESTAMP-interval '14 days' AND assessed_at<=CURRENT_TIMESTAMP ORDER BY assessed_at DESC,pressure_signal_history.id DESC LIMIT 50) h),'[]'::jsonb) AS transitions`)).rows[0];
  const now=(await db.query('SELECT clock_timestamp() AS at')).rows[0].at as Date;
  type Row={rule:string;state_at_assessment:string;reason:string;instance_id:string;started_at:string;assessed_at:string;recorded_at:string;state_since:string;revision:string;signal_expires_at:string|null;policy_hash:string;payload_hash:string;evidence:{signal:unknown;policy:unknown};id?:string;change_kind?:string};
  const states=data.states as Row[],transitions=data.transitions as Row[];
  const iso=(v:string)=>new Date(v).toISOString();
  let valid=states.length===3;
  try{
    if(!valid)throw 0;const first=states[0]!;
    if(states.some(s=>s.instance_id!==first.instance_id||s.started_at!==first.started_at||s.assessed_at!==first.assessed_at||s.recorded_at!==first.recorded_at||s.payload_hash!==first.payload_hash||s.policy_hash!==first.policy_hash||JSON.stringify(s.evidence.policy)!==JSON.stringify(first.evidence.policy)||Date.parse(s.recorded_at)>now.getTime()))throw 0;
    // The parser's seven-day admission window belongs to writes. Stored evidence
    // can remain after collection stops; its source expiry still makes it unknown.
    if(Date.parse(first.assessed_at)>now.getTime())throw 0;
    const parsed=parsePressureObservation({instance_id:first.instance_id,started_at:iso(first.started_at),assessment:{scope:'platform_pressure',computed_at:iso(first.assessed_at),signals:states.map(s=>s.evidence.signal),policy:first.evidence.policy}},Date.parse(first.assessed_at));
    if(parsed.payload_hash!==first.payload_hash||parsed.policy_hash!==first.policy_hash)throw 0;
  }catch{valid=false;}
  const present=(s:Row)=>({...(s.id?{id:s.id,change_kind:s.change_kind}:{}),rule:s.rule,state_at_assessment:s.state_at_assessment,reason:s.reason,instance_id:s.instance_id,started_at:iso(s.started_at),assessed_at:iso(s.assessed_at),recorded_at:iso(s.recorded_at),state_since:iso(s.state_since),revision:String(s.revision),signal_expires_at:s.signal_expires_at===null?null:iso(s.signal_expires_at),signal:s.evidence.signal,policy:s.evidence.policy});
  return{scope:'platform_pressure' as const,computed_at:now.toISOString(),retention_days:14,transition_limit:50,
    persistence_state:!states.length?'unavailable':!valid?'invalid':states.some(s=>s.signal_expires_at===null||Date.parse(s.signal_expires_at)<now.getTime())?'expired':'fresh',
    states:states.map(s=>({...present(s),current_state:valid&&s.signal_expires_at!==null&&Date.parse(s.signal_expires_at)>=now.getTime()?s.state_at_assessment:'unknown'})),
    transitions:transitions.map(present)};
}
export async function readPressureHistory(user:string,tenant:string,project:string){return withOperationalRead(user,tenant,project,queryPressureHistory);}
