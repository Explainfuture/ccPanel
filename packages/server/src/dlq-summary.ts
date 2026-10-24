import {query} from './db';
// A retained source is not resolved merely because a replay was requested or failed.
export async function readDeadLetterSummary(tenant:string,project:string){
  const result=await query(`WITH classified AS (
    SELECT d.created_at,CASE
      WHEN d.payload->>'redacted'='true' OR EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=$1 AND i.project_id=$2 AND i.record_id=d.id AND i.status='suppressed') THEN 'suppressed'
      WHEN EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=$1 AND i.project_id=$2 AND i.record_id=d.id AND i.status IN ('stored','duplicate')) THEN 'resolved'
      WHEN EXISTS(SELECT 1 FROM replay_items i WHERE i.tenant_id=$1 AND i.project_id=$2 AND i.record_id=d.id AND i.status IN ('queued','published')) THEN 'replaying'
      ELSE 'unresolved' END AS category
    FROM dlq_records d WHERE d.tenant_id=$1 AND d.project_id=$2 AND d.created_at>=CURRENT_TIMESTAMP-interval '14 days'
  ), categories(category) AS (VALUES('unresolved'),('replaying'),('resolved'),('suppressed'))
  SELECT c.category,count(d.created_at)::text AS count,min(d.created_at) AS oldest_created_at,
    CASE WHEN min(d.created_at) IS NULL THEN NULL ELSE greatest(0,floor(extract(epoch FROM CURRENT_TIMESTAMP-min(d.created_at))))::text END AS oldest_age_seconds,
    CURRENT_TIMESTAMP AS observed_at FROM categories c LEFT JOIN classified d USING(category) GROUP BY c.category ORDER BY c.category`,[tenant,project]);
  return {scope:'project',retention_days:14,semantics:'retained_source_replay_receipts',categories:result.rows};
}
