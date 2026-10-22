import type { PoolClient } from "pg";
import { digest } from "./security";
import type { StoredEnvelope } from "../../schema/src/index";

export const identityHash = (
  tenant: string,
  project: string,
  identity: string,
) => digest(tenant + ":" + project + ":" + identity);
export const identityHashSql = (alias: string, column = "distinct_id") =>
  `encode(sha256(convert_to(${alias}.tenant_id::text||':'||${alias}.project_id::text||':'||${alias}.${column},'UTF8')),'hex')`;
export async function lockProjectPrivacy(
  db: PoolClient,
  project: string,
  exclusive = false,
) {
  await db.query(
    `SELECT ${exclusive ? "pg_advisory_xact_lock" : "pg_advisory_xact_lock_shared"}(hashtextextended($1,0))`,
    ["ccpanel:privacy:" + project],
  );
}

// Run inside the same transaction and project privacy lock as the raw write.
// A late identify can reveal a previously unknown anonymous alias: propagate
// the original cutoff and reopen cleanup before allowing that association.
export async function suppressedByDeletion(
  db: PoolClient,
  record: StoredEnvelope,
) {
  const e = record.event;
  if (e.event_name === "$identify" && e.anonymous_id) {
    const userHash = identityHash(
        record.tenant_id,
        record.project_id,
        e.distinct_id,
      ),
      anonymousHash = identityHash(
        record.tenant_id,
        record.project_id,
        e.anonymous_id,
      );
    const inherited = await db.query(
      "SELECT * FROM deletion_barriers WHERE tenant_id=$1 AND project_id=$2 AND identity_hash=$3",
      [record.tenant_id, record.project_id, userHash],
    );
    const barrier = inherited.rows[0];
    if (barrier && anonymousHash !== userHash) {
      const changed = await db.query(
        "INSERT INTO deletion_barriers(tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(project_id,identity_hash) DO UPDATE SET deletion_job_id=EXCLUDED.deletion_job_id,requested_at=GREATEST(deletion_barriers.requested_at,EXCLUDED.requested_at),blocked_before=GREATEST(deletion_barriers.blocked_before,EXCLUDED.blocked_before) WHERE deletion_barriers.blocked_before<EXCLUDED.blocked_before OR deletion_barriers.requested_at<EXCLUDED.requested_at RETURNING identity_hash",
        [
          record.tenant_id,
          record.project_id,
          anonymousHash,
          barrier.deletion_job_id,
          barrier.requested_at,
          barrier.blocked_before,
        ],
      );
      // The caller holds the exclusive project lock for identify. A cleanup
      // transaction cannot finish against an incomplete alias set concurrently.
      if (changed.rowCount) {
        await db.query(
          "UPDATE deletion_jobs SET status='queued',finished_at=NULL,error=NULL,stream_expires_at=GREATEST(stream_expires_at,clock_timestamp()+interval '45 days') WHERE tenant_id=$1 AND project_id=$2 AND id=$3",
          [record.tenant_id, record.project_id, barrier.deletion_job_id],
        );
        await db.query(
          "UPDATE projects SET privacy_epoch=privacy_epoch+1 WHERE tenant_id=$1 AND id=$2",
          [record.tenant_id, record.project_id],
        );
        await db.query(
          "UPDATE query_jobs SET status='cancelled',result=NULL,finished_at=now() WHERE tenant_id=$1 AND project_id=$2",
          [record.tenant_id, record.project_id],
        );
        await db.query('INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,$2,$3)',[record.tenant_id,'deletion_alias_discovered',JSON.stringify({project_id:record.project_id,deletion_job_id:barrier.deletion_job_id,identity_hash:anonymousHash})]);
      }
    }
  }
  const hashes = [
    identityHash(record.tenant_id, record.project_id, e.distinct_id),
  ];
  if (e.anonymous_id)
    hashes.push(
      identityHash(record.tenant_id, record.project_id, e.anonymous_id),
    );
  const link = await db.query(
    "SELECT user_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=$3",
    [record.tenant_id, record.project_id, e.distinct_id],
  );
  if (link.rows[0])
    hashes.push(
      identityHash(record.tenant_id, record.project_id, link.rows[0].user_id),
    );
  const found = await db.query(
    "SELECT 1 FROM deletion_barriers WHERE tenant_id=$1 AND project_id=$2 AND (identity_hash='*' OR (identity_hash=ANY($3::text[]) AND ($4::timestamptz<=blocked_before OR $5::timestamptz<=requested_at))) LIMIT 1",
    [
      record.tenant_id,
      record.project_id,
      hashes,
      e.event_time,
      record.received_at,
    ],
  );
  return !!found.rowCount;
}
