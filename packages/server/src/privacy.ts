import type { PoolClient } from "pg";
import { pool } from "./db";
import { digest } from "./security";
import type { StoredEnvelope } from "../../schema/src/index";

export const identityHash = (
  tenant: string,
  project: string,
  identity: string,
) => digest(tenant.toLowerCase() + ":" + project.toLowerCase() + ":" + identity);
export const identityHashSql = (alias: string, column = "distinct_id") =>
  `encode(sha256(convert_to(${alias}.tenant_id::text||':'||${alias}.project_id::text||':'||${alias}.${column},'UTF8')),'hex')`;
export const eventBarrierPredicate = (event = "e", link = "l", barrier = "b") =>
  `(${barrier}.identity_hash='*' OR (${barrier}.identity_hash IN (${identityHashSql(event)},${identityHashSql(event, "anonymous_id")},${identityHashSql(link, "user_id")}) AND (${event}.event_time<=${barrier}.blocked_before OR ${event}.received_at<=${barrier}.requested_at)))`;
export async function lockProjectPrivacy(
  db: PoolClient,
  project: string,
  exclusive = false,
) {
  await db.query(
    `SELECT ${exclusive ? "pg_advisory_xact_lock" : "pg_advisory_xact_lock_shared"}(hashtextextended($1,0))`,
    ["ccpanel:privacy:" + project.toLowerCase()],
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
        await db.query(
          "INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,$2,$3)",
          [
            record.tenant_id,
            "deletion_alias_discovered",
            JSON.stringify({
              project_id: record.project_id,
              deletion_job_id: barrier.deletion_job_id,
              identity_hash: anonymousHash,
            }),
          ],
        );
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

// Poison payloads bypass persist(), but must not create a fresh DLQ copy of a
// known deleted identity/project. Invalid unscoped JSON cannot be attributed;
// it remains subject to the restricted global DLQ retention boundary.
export async function discardDeletedDeadLetter(input: unknown) {
  if (!input || typeof input !== "object") return false;
  const r = input as Record<string, unknown>,
    uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (
    typeof r.tenant_id !== "string" ||
    typeof r.project_id !== "string" ||
    !uuid.test(r.tenant_id) ||
    !uuid.test(r.project_id)
  )
    return false;
  const e =
    r.event && typeof r.event === "object"
      ? (r.event as Record<string, unknown>)
      : {};
  const identities = [e.distinct_id, e.anonymous_id].filter(
    (v): v is string =>
      typeof v === "string" && v.length > 0 && v.length <= 256,
  );
  const hashes = identities.map((id) =>
    identityHash(r.tenant_id as string, r.project_id as string, id),
  );
  const db = await pool.connect();
  try {
    await db.query("BEGIN");
    await lockProjectPrivacy(db, r.project_id);
    const found = await db.query(
      "SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=$1 AND b.project_id=$2 AND (b.identity_hash='*' OR b.identity_hash=ANY($3::text[]) OR b.identity_hash IN (SELECT encode(sha256(convert_to($1::text||':'||$2::text||':'||user_id,'UTF8')),'hex') FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=ANY($4::text[]))) LIMIT 1",
      [r.tenant_id, r.project_id, hashes, identities],
    );
    if (found.rowCount)
      await db.query(
        "INSERT INTO ingest_metrics VALUES($1,$2,'deletion_suppressed',1) ON CONFLICT(project_id,metric) DO UPDATE SET value=ingest_metrics.value+1",
        [r.tenant_id, r.project_id],
      );
    await db.query("COMMIT");
    return !!found.rowCount;
  } catch (e) {
    await db.query("ROLLBACK");
    throw e;
  } finally {
    db.release();
  }
}
