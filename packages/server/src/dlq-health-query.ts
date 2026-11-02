import type {PoolClient} from 'pg';
import {evaluateDlqHealth} from './dlq-health';
export async function queryDlqHealth(db:PoolClient){
  // All physically retained index rows, including unscoped or expired rows.
  // Matching receipts must share both company and project. No payload leaves PG.
  const row=(await db.query(`WITH classified AS (
    SELECT d.created_at,CASE
      WHEN d.payload->>'redacted'='true' OR EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=d.tenant_id AND i.project_id=d.project_id AND i.record_id=d.id AND i.status='suppressed') THEN 'suppressed'
      WHEN EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=d.tenant_id AND i.project_id=d.project_id AND i.record_id=d.id AND i.status IN ('stored','duplicate')) THEN 'resolved'
      WHEN EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=d.tenant_id AND i.project_id=d.project_id AND i.record_id=d.id AND i.status IN ('queued','published')) THEN 'replaying'
      ELSE 'unresolved' END AS category
    FROM dlq_records d
  ) SELECT statement_timestamp() AS observed_at,
    jsonb_build_object('unresolved',(count(*) FILTER(WHERE category='unresolved'))::text,'replaying',(count(*) FILTER(WHERE category='replaying'))::text,'resolved',(count(*) FILTER(WHERE category='resolved'))::text,'suppressed',(count(*) FILTER(WHERE category='suppressed'))::text) AS counts,
    min(created_at) FILTER(WHERE category IN ('unresolved','replaying')) AS oldest_pending_at,
    (count(*) FILTER(WHERE created_at>statement_timestamp()))::text AS future_records FROM classified`)).rows[0];
  const now=(await db.query('SELECT clock_timestamp() AS at')).rows[0].at as Date;
  return evaluateDlqHealth({...row,observed_at:row.observed_at.toISOString(),oldest_pending_at:row.oldest_pending_at?.toISOString()??null},now.getTime());
}
