import {query} from './db';
export async function readQueryStatus(tenant:string,project:string){
  const rows=await query(`WITH visible AS (
    SELECT q.* FROM query_jobs q JOIN projects p ON p.tenant_id=q.tenant_id AND p.id=q.project_id
    WHERE q.tenant_id=$1 AND q.project_id=$2 AND q.expires_at>CURRENT_TIMESTAMP AND q.privacy_epoch=p.privacy_epoch
      AND EXISTS(SELECT 1 FROM tenant_memberships m WHERE m.tenant_id=q.tenant_id AND m.user_id=q.user_id)
  ), states(status) AS (VALUES('queued'),('running'),('succeeded'),('failed'),('cancelled'))
  SELECT s.status,count(q.id)::text AS count,min(q.created_at) AS oldest_created_at,
    CASE WHEN min(q.created_at) IS NULL THEN NULL ELSE greatest(0,floor(extract(epoch FROM CURRENT_TIMESTAMP-min(q.created_at))))::text END AS oldest_age_seconds,
    CASE WHEN s.status='running' AND min(q.started_at) IS NOT NULL THEN greatest(0,floor(extract(epoch FROM CURRENT_TIMESTAMP-min(q.started_at))))::text ELSE NULL END AS oldest_running_seconds,
    count(q.id) FILTER(WHERE q.started_at IS NOT NULL AND q.finished_at>=q.started_at)::text AS duration_samples,
    max(extract(epoch FROM q.finished_at-q.started_at)*1000) FILTER(WHERE q.started_at IS NOT NULL AND q.finished_at>=q.started_at)::text AS max_completed_duration_ms,
    CURRENT_TIMESTAMP AS observed_at
  FROM states s LEFT JOIN visible q USING(status) GROUP BY s.status ORDER BY s.status`,[tenant,project]);
  return {scope:'project_visible_jobs',semantics:'unexpired_current_privacy_epoch_and_member',states:rows.rows};
}
