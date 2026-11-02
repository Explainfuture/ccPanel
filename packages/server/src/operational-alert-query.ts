import type {PoolClient} from 'pg';
import {evaluateOperationalAlerts} from './operational-alerts';
export async function queryOperationalAlerts(db:PoolClient){
  const rows=(await db.query(`SELECT s.source,p.snapshot FROM (VALUES ('api'),('worker')) AS s(source)
    CROSS JOIN LATERAL (SELECT instance_id FROM operational_samples WHERE source=s.source AND observed_at>=CURRENT_TIMESTAMP-interval '7 days' AND observed_at<=CURRENT_TIMESTAMP+interval '5 seconds' ORDER BY observed_at DESC,instance_id DESC LIMIT 1) latest
    CROSS JOIN LATERAL (SELECT snapshot FROM operational_samples WHERE source=s.source AND instance_id=latest.instance_id AND observed_at>=CURRENT_TIMESTAMP-interval '7 days' AND observed_at<=CURRENT_TIMESTAMP+interval '5 seconds' ORDER BY observed_at DESC LIMIT 2) p`)).rows;
  const now=(await db.query('SELECT clock_timestamp() AS at')).rows[0].at as Date;
  return{computed_at:now.toISOString(),rules:evaluateOperationalAlerts({api:rows.filter(r=>r.source==='api').map(r=>r.snapshot),worker:rows.filter(r=>r.source==='worker').map(r=>r.snapshot)},now.getTime())};
}
