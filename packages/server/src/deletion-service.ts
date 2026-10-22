import { randomUUID } from "node:crypto";
import type { Hono, MiddlewareHandler } from "hono";
import type { PoolClient } from "pg";
import type { Env } from "../../../apps/api/src/app";
import { pool, query } from "./db";
import {
  identityHash,
  identityHashSql,
  eventBarrierPredicate,
  lockProjectPrivacy,
} from "./privacy";

export function deletionRequest(input: unknown) {
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw new Error("invalid_deletion");
  const b = input as Record<string, unknown>;
  if (
    Object.keys(b).some(
      (k) => !["scope", "distinct_id", "confirmation", "reason"].includes(k),
    ) ||
    typeof b.scope !== "string" ||
    !["user", "project"].includes(b.scope) ||
    typeof b.reason !== "string" ||
    b.reason.trim().length < 5 ||
    b.reason.length > 512 ||
    typeof b.confirmation !== "string"
  )
    throw new Error("invalid_deletion");
  if (
    b.scope === "user" &&
    (typeof b.distinct_id !== "string" ||
      !b.distinct_id ||
      b.distinct_id.length > 256 ||
      b.confirmation !== b.distinct_id)
  )
    throw new Error("invalid_confirmation");
  if (b.scope === "project" && b.distinct_id !== undefined)
    throw new Error("invalid_deletion");
  return b as {
    scope: "user" | "project";
    distinct_id?: string;
    confirmation: string;
    reason: string;
  };
}
const prefix = "/api/tenants/:tenant/projects/:project/deletions";
const validId = (s: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
export function registerDeletions(app: Hono<Env>) {
  const admin: MiddlewareHandler<Env> = async (c, next) => {
    if (c.get("role") !== "admin") return c.json({ error: "forbidden" }, 403);
    await next();
  };
  app.use(prefix, admin);
  app.use(prefix + "/*", admin);
  app.post(prefix, async (c) => {
    let body;
    try {
      body = deletionRequest(await c.req.json());
    } catch (e) {
      return c.json({ error: (e as Error).message }, 400);
    }
    const tenant = c.get("tenantId"),
      project = c.req.param("project")!,
      db = await pool.connect();
    try {
      await db.query("BEGIN");
      await db.query("SELECT pg_advisory_xact_lock(169202615)");
      await lockProjectPrivacy(db, project, true);
      const projectRow = await db.query(
        "SELECT name FROM projects WHERE tenant_id=$1 AND id=$2",
        [tenant, project],
      );
      if (
        body.scope === "project" &&
        body.confirmation !== projectRow.rows[0].name
      ) {
        await db.query("ROLLBACK");
        return c.json({ error: "invalid_confirmation" }, 400);
      }
      const active = await db.query(
        "SELECT id FROM deletion_jobs WHERE tenant_id=$1 AND project_id=$2 AND status IN ('queued','running') LIMIT 1",
        [tenant, project],
      );
      if (active.rowCount) {
        await db.query("ROLLBACK");
        return c.json({ error: "deletion_busy" }, 409);
      }
      const global = await db.query(
        "SELECT count(*)::int AS count FROM deletion_jobs WHERE status IN ('queued','running')",
      );
      if (global.rows[0].count >= 100) {
        await db.query("ROLLBACK");
        return c.json({ error: "deletion_queue_full" }, 429);
      }
      const link =
        body.scope === "user"
          ? await db.query(
              "SELECT user_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND anonymous_id=$3",
              [tenant, project, body.distinct_id],
            )
          : { rows: [] };
      const canonical = link.rows[0]?.user_id || body.distinct_id,
        hash =
          body.scope === "user"
            ? identityHash(tenant, project, canonical!)
            : null,
        id = randomUUID();
      const job = await db.query(
        "INSERT INTO deletion_jobs(id,tenant_id,project_id,user_id,scope,subject_hash) VALUES($1,$2,$3,$4,$5,$6) RETURNING requested_at,blocked_before,stream_expires_at",
        [id, tenant, project, c.get("userId"), body.scope, hash],
      );
      const times = job.rows[0];
      if (body.scope === "project") {
        await db.query(
          "INSERT INTO deletion_barriers VALUES($1,$2,'*',$3,$4,$5) ON CONFLICT(project_id,identity_hash) DO UPDATE SET deletion_job_id=EXCLUDED.deletion_job_id,requested_at=EXCLUDED.requested_at,blocked_before=EXCLUDED.blocked_before",
          [tenant, project, id, times.requested_at, times.blocked_before],
        );
        await db.query(
          "UPDATE write_keys SET revoked_at=COALESCE(revoked_at,now()) WHERE tenant_id=$1 AND project_id=$2",
          [tenant, project],
        );
      } else {
        await db.query(
          `INSERT INTO deletion_barriers(tenant_id,project_id,identity_hash,deletion_job_id,requested_at,blocked_before)
          SELECT $1,$2,encode(sha256(convert_to($1::text||':'||$2::text||':'||identity,'UTF8')),'hex'),$3,$4,$5 FROM (
            SELECT $6::text AS identity UNION SELECT $7::text
            UNION SELECT anonymous_id FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND user_id=$6
            UNION SELECT anonymous_id FROM events WHERE tenant_id=$1 AND project_id=$2 AND distinct_id=$6 AND anonymous_id IS NOT NULL
          ) aliases WHERE identity IS NOT NULL
          ON CONFLICT(project_id,identity_hash) DO UPDATE SET deletion_job_id=EXCLUDED.deletion_job_id,requested_at=GREATEST(deletion_barriers.requested_at,EXCLUDED.requested_at),blocked_before=GREATEST(deletion_barriers.blocked_before,EXCLUDED.blocked_before)`,
          [
            tenant,
            project,
            id,
            times.requested_at,
            times.blocked_before,
            canonical,
            body.distinct_id,
          ],
        );
      }
      await db.query(
        "UPDATE projects SET privacy_epoch=privacy_epoch+1 WHERE tenant_id=$1 AND id=$2",
        [tenant, project],
      );
      await db.query(
        "UPDATE query_jobs SET status='cancelled',result=NULL,finished_at=now() WHERE tenant_id=$1 AND project_id=$2",
        [tenant, project],
      );
      await db.query(
        "INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)",
        [
          tenant,
          c.get("userId"),
          "deletion_requested",
          JSON.stringify({
            project_id: project,
            deletion_job_id: id,
            scope: body.scope,
            subject_hash: hash,
            reason: body.reason,
          }),
        ],
      );
      await db.query("COMMIT");
      return c.json(
        {
          id,
          status: "queued",
          ...times,
          stream_status: "pending-stream-expiry",
        },
        202,
      );
    } catch (e) {
      await db.query("ROLLBACK");
      throw e;
    } finally {
      db.release();
    }
  });
  app.get(prefix, async (c) =>
    c.json({
      jobs: (
        await query(
          "SELECT id,scope,left(subject_hash,12) AS subject_reference,status,removed_events,error,requested_at,blocked_before,finished_at,stream_expires_at,CASE WHEN clock_timestamp()<stream_expires_at THEN 'pending-stream-expiry' ELSE 'retention-window-elapsed' END AS stream_status FROM deletion_jobs WHERE tenant_id=$1 AND project_id=$2 ORDER BY requested_at DESC LIMIT 50",
          [c.get("tenantId"), c.req.param("project")],
        )
      ).rows,
      backup_status:
        "retention-managed; current deletion ledger must be applied on restore",
    }),
  );
  app.post(prefix + "/:job/retry", async (c) => {
    const id = c.req.param("job")!;
    if (!validId(id)) return c.json({ error: "not_found" }, 404);
    const db = await pool.connect();
    try {
      await db.query("BEGIN");
      await db.query("SELECT pg_advisory_xact_lock(169202615)");
      const count = await db.query(
        "SELECT count(*)::int AS count FROM deletion_jobs WHERE status IN ('queued','running')",
      );
      if (count.rows[0].count >= 100) {
        await db.query("ROLLBACK");
        return c.json({ error: "deletion_queue_full" }, 429);
      }
      const active = await db.query(
        "SELECT 1 FROM deletion_jobs WHERE tenant_id=$1 AND project_id=$2 AND status IN ('queued','running') LIMIT 1",
        [c.get("tenantId"), c.req.param("project")],
      );
      if (active.rowCount) {
        await db.query("ROLLBACK");
        return c.json({ error: "deletion_busy" }, 409);
      }
      const changed = await db.query(
        "UPDATE deletion_jobs SET status='queued',error=NULL WHERE tenant_id=$1 AND project_id=$2 AND id=$3 AND status='failed' RETURNING id",
        [c.get("tenantId"), c.req.param("project"), id],
      );
      if (!changed.rowCount) {
        await db.query("ROLLBACK");
        return c.json({ error: "not_found" }, 404);
      }
      await db.query(
        "INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)",
        [
          c.get("tenantId"),
          c.get("userId"),
          "deletion_retried",
          JSON.stringify({
            project_id: c.req.param("project"),
            deletion_job_id: id,
          }),
        ],
      );
      await db.query("COMMIT");
      return c.json({ ok: true });
    } catch (e) {
      await db.query("ROLLBACK");
      throw e;
    } finally {
      db.release();
    }
  });
}

export async function deleteBatch(
  db: PoolClient,
  job: { id: string; tenant_id: string; project_id: string },
) {
  const values = [job.tenant_id, job.project_id, job.id];
  const removed = await db.query(
    `WITH doomed AS MATERIALIZED (
    SELECT e.tableoid AS relation,e.ctid AS row_ref,e.event_id FROM events e
    LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id
    WHERE e.tenant_id=$1 AND e.project_id=$2 AND EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=e.tenant_id AND b.project_id=e.project_id AND b.deletion_job_id=$3 AND ${eventBarrierPredicate()}) LIMIT 500
  ), tasks AS (DELETE FROM aggregation_jobs a USING doomed d WHERE a.tenant_id=$1 AND a.project_id=$2 AND a.event_id=d.event_id),
  gone AS (DELETE FROM events e USING doomed d WHERE e.tenant_id=$1 AND e.project_id=$2 AND e.tableoid=d.relation AND e.ctid=d.row_ref RETURNING e.event_id)
  SELECT count(*)::int AS count FROM gone`,
    values,
  );
  if (removed.rows[0].count) {
    await db.query(
      "UPDATE deletion_jobs SET removed_events=removed_events+$2 WHERE id=$1 AND tenant_id=$3 AND project_id=$4",
      [job.id, removed.rows[0].count, job.tenant_id, job.project_id],
    );
    return false;
  }
  const links = await db.query(
    `DELETE FROM identity_links WHERE tenant_id=$1 AND project_id=$2 AND ctid IN (SELECT l.ctid FROM identity_links l WHERE l.tenant_id=$1 AND l.project_id=$2 AND EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=l.tenant_id AND b.project_id=l.project_id AND b.deletion_job_id=$3 AND (b.identity_hash='*' OR (b.identity_hash IN (${identityHashSql("l", "anonymous_id")},${identityHashSql("l", "user_id")}) AND l.created_at<=b.blocked_before))) LIMIT 500) RETURNING anonymous_id`,
    values,
  );
  if (links.rowCount) return false;
  const deadHash = (field: string) =>
    `encode(sha256(convert_to(d.tenant_id::text||':'||d.project_id::text||':'||(d.payload->'record'->'event'->>'${field}'),'UTF8')),'hex')`;
  const redacted = await db.query(
    `UPDATE dlq_records SET payload=jsonb_build_object('redacted',true,'deletion_job_id',$3::text) WHERE tenant_id=$1 AND project_id=$2 AND id IN (SELECT d.id FROM dlq_records d WHERE d.tenant_id=$1 AND d.project_id=$2 AND NOT (d.payload ? 'redacted') AND EXISTS(SELECT 1 FROM deletion_barriers b WHERE b.tenant_id=d.tenant_id AND b.project_id=d.project_id AND b.deletion_job_id=$3::uuid AND (b.identity_hash='*' OR b.identity_hash IN (${deadHash("distinct_id")},${deadHash("anonymous_id")}))) LIMIT 500) RETURNING id`,
    values,
  );
  return !redacted.rowCount;
}
export function startDeletions() {
  let busy = false,
    stopped = false;
  const tick = async () => {
    if (busy || stopped) return;
    busy = true;
    let db: PoolClient | undefined,
      locked = false,
      id: string | undefined,
      scope:{tenant_id:string;project_id:string}|undefined;
    try {
      db = await pool.connect();
      locked = (
        await db.query("SELECT pg_try_advisory_lock(169202614) AS locked")
      ).rows[0].locked;
      if (!locked) return;
      const next = await db.query(
        "SELECT id,tenant_id,project_id FROM deletion_jobs WHERE status IN ('queued','running') ORDER BY requested_at LIMIT 1",
      );
      const job = next.rows[0];
      if (!job) return;
      id = job.id;
      scope=job;
      await db.query("BEGIN");
      await lockProjectPrivacy(db, job.project_id, true);
      await db.query("UPDATE deletion_jobs SET status='running' WHERE id=$1 AND tenant_id=$2 AND project_id=$3", [
        id,job.tenant_id,job.project_id,
      ]);
      const complete = await deleteBatch(db, job);
      if (complete) {
        await db.query(
          "UPDATE deletion_jobs SET status='completed',finished_at=clock_timestamp(),error=NULL WHERE id=$1 AND tenant_id=$2 AND project_id=$3",
          [id,job.tenant_id,job.project_id],
        );
        await db.query(
          "INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,$2,$3)",
          [
            job.tenant_id,
            "deletion_database_completed",
            JSON.stringify({ project_id: job.project_id, deletion_job_id: id }),
          ],
        );
      }
      await db.query("COMMIT");
    } catch (e) {
      if (db) {
        await db.query("ROLLBACK").catch(() => {});
        if (id&&scope)
          await db
            .query(
              "UPDATE deletion_jobs SET status='failed',error=$2 WHERE id=$1 AND tenant_id=$3 AND project_id=$4",
              [
                id,
                (e as { code?: string }).code === "57014"
                  ? "deletion_timeout"
                  : "deletion_failed",
                scope.tenant_id,scope.project_id,
              ],
            )
            .catch(() => {});
      }
      console.error(JSON.stringify({ code: "deletion_batch_failed" }));
    } finally {
      if (db) {
        if (locked)
          try {
            await db.query("SELECT pg_advisory_unlock(169202614)");
          } catch {
            db.release(true);
            db = undefined;
          }
        db?.release();
      }
      busy = false;
    }
  };
  const timer = setInterval(() => void tick(), 2000);
  timer.unref();
  return () => {
    stopped = true;
    clearInterval(timer);
  };
}
