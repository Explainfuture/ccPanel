import { randomUUID } from "node:crypto";
import type { Hono } from "hono";
import type { PoolClient, QueryResultRow } from "pg";
import type { Env } from "../../../apps/api/src/app";
import { pool, query } from "./db";
import { parseTrend, InvalidQuery } from "./query-spec";
import { parseBehavior } from "./behavior";
import { trends } from "./trends";
import { behaviorQuery, QueryBudgetExceeded } from "./behavior-routes";
import { projectQueryFlights } from "./query-budget";

export function parseQuerySpec(input: unknown) {
  return (input as { kind?: unknown })?.kind === "trend"
    ? parseTrend(input)
    : parseBehavior(input);
}
const validId = (id: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id);
export function registerQueryJobs(app: Hono<Env>) {
  const prefix = "/api/tenants/:tenant/projects/:project/query-jobs";
  app.post(prefix, async (c) => {
    let spec;
    try {
      spec = parseQuerySpec(await c.req.json());
    } catch (e) {
      return c.json(
        { error: e instanceof InvalidQuery ? e.message : "invalid_query" },
        400,
      );
    }
    const db = await pool.connect();
    try {
      await db.query("BEGIN");
      // Serialize enqueue limits across API instances; the queue is deliberately small.
      await db.query("SELECT pg_advisory_xact_lock(169202611)");
      const counts = await db.query(
        "SELECT count(*)::int AS total,count(*) FILTER(WHERE project_id=$1)::int AS project FROM query_jobs WHERE status IN ('queued','running')",
        [c.req.param("project")],
      );
      if (counts.rows[0].total >= 100 || counts.rows[0].project >= 5) {
        await db.query("ROLLBACK");
        return c.json({ error: "query_queue_full" }, 429);
      }
      const id = randomUUID();
      await db.query(
        "INSERT INTO query_jobs(id,tenant_id,project_id,user_id,spec,privacy_epoch) SELECT $1,$2,$3,$4,$5,p.privacy_epoch FROM projects p WHERE p.tenant_id=$2 AND p.id=$3",
        [
          id,
          c.get("tenantId"),
          c.req.param("project"),
          c.get("userId"),
          JSON.stringify(spec),
        ],
      );
      await db.query("COMMIT");
      return c.json({ id, status: "queued", expires_in_seconds: 86400 }, 202);
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
          "SELECT id,spec->>'kind' AS kind,status,error,created_at,started_at,finished_at,expires_at FROM query_jobs WHERE tenant_id=$1 AND project_id=$2 AND expires_at>now() AND privacy_epoch=(SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2) ORDER BY created_at DESC LIMIT 50",
          [c.get("tenantId"), c.req.param("project")],
        )
      ).rows,
    }),
  );
  app.get(prefix + "/:job", async (c) => {
    const id = c.req.param("job")!;
    if (!validId(id)) return c.json({ error: "not_found" }, 404);
    const found = await query(
      "SELECT id,status,spec,result,error,created_at,started_at,finished_at,expires_at FROM query_jobs WHERE tenant_id=$1 AND project_id=$2 AND id=$3 AND expires_at>now() AND privacy_epoch=(SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2)",
      [c.get("tenantId"), c.req.param("project"), id],
    );
    return found.rowCount
      ? c.json(found.rows[0])
      : c.json({ error: "not_found" }, 404);
  });
  app.delete(prefix + "/:job", async (c) => {
    const id = c.req.param("job")!;
    if (!validId(id)) return c.json({ error: "not_found" }, 404);
    const found = await query(
      "UPDATE query_jobs SET status='cancelled',result=NULL,finished_at=now() WHERE tenant_id=$1 AND project_id=$2 AND id=$3 AND (user_id=$4 OR $5='admin') AND expires_at>now() RETURNING id",
      [
        c.get("tenantId"),
        c.req.param("project"),
        id,
        c.get("userId"),
        c.get("role"),
      ],
    );
    return found.rowCount
      ? c.json({ ok: true })
      : c.json({ error: "not_found" }, 404);
  });
}

// One global runner, protected by a connection-held PG advisory lock. Read-only
// jobs can be requeued safely after a process crash; no Redis or extra service.
export function startQueryJobs() {
  let busy = false,
    stopped = false;
  const tick = async () => {
    if (busy || stopped) return;
    busy = true;
    let db: PoolClient | undefined,
      locked = false,
      project: string | undefined;
    try {
      db = await pool.connect();
      locked = (
        await db.query("SELECT pg_try_advisory_lock(169202612) AS locked")
      ).rows[0].locked;
      if (!locked) return;
      await db.query("DELETE FROM query_jobs WHERE expires_at<=now()");
      await db.query(
        "UPDATE query_jobs SET status='queued',started_at=NULL WHERE status='running'",
      );
      const next = await db.query(
        "SELECT * FROM query_jobs WHERE status='queued' AND expires_at>now() ORDER BY created_at LIMIT 1",
      );
      const job = next.rows[0];
      if (!job || projectQueryFlights.has(job.project_id)) return;
      project = job.project_id;
      projectQueryFlights.add(project!);
      const epoch=await db.query('SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2',[job.tenant_id,job.project_id]);
      if(epoch.rows[0]?.privacy_epoch!==job.privacy_epoch){
        await db.query("UPDATE query_jobs SET status='cancelled',result=NULL,error='privacy_changed',finished_at=now() WHERE id=$1",[job.id]);return;
      }
      const allowed = await db.query(
        "SELECT 1 FROM tenant_memberships m JOIN projects p ON p.tenant_id=m.tenant_id WHERE m.tenant_id=$1 AND m.user_id=$2 AND p.id=$3",
        [job.tenant_id, job.user_id, job.project_id],
      );
      if (!allowed.rowCount) {
        await db.query(
          "UPDATE query_jobs SET status='failed',error='membership_revoked',finished_at=now() WHERE id=$1 AND status='queued'",
          [job.id],
        );
        return;
      }
      const claimed = await db.query(
        "UPDATE query_jobs SET status='running',started_at=now() WHERE id=$1 AND status='queued' RETURNING id",
        [job.id],
      );
      if (!claimed.rowCount) return;
      try {
        const spec = parseQuerySpec(job.spec),
          deadline = Date.now() + 30000;
        await db.query("BEGIN");
        // Serialize on this connection and allocate the remaining total deadline
        // to each statement, rather than allowing 30 seconds per subquery.
        let tail: Promise<unknown> = Promise.resolve();
        const run = <T extends QueryResultRow = QueryResultRow>(
          sql: string,
          values: unknown[] = [],
        ) => {
          const result = tail.then(async () => {
            const remaining = deadline - Date.now();
            if (remaining <= 0) throw new Error("query_timeout");
            await db!.query("SELECT set_config('statement_timeout',$1,true)", [
              String(remaining),
            ]);
            return db!.query<T>(sql, values);
          });
          tail = result;
          return result;
        };
        const result =
          spec.kind === "trend"
            ? await trends(job.tenant_id, job.project_id, spec, run)
            : await behaviorQuery(
                job.tenant_id,
                job.project_id,
                spec,
                run,
                true,
              );
        if (Date.now() > deadline) throw new Error("query_timeout");
        const encoded = JSON.stringify(result);
        if (Buffer.byteLength(encoded) > 2 * 1024 * 1024)
          throw new QueryBudgetExceeded("query_result_too_large");
        await db.query("COMMIT");
        await db.query(
          "UPDATE query_jobs SET status='succeeded',result=$2,finished_at=now() WHERE id=$1 AND status='running' AND privacy_epoch=(SELECT privacy_epoch FROM projects WHERE tenant_id=$3 AND id=query_jobs.project_id) AND EXISTS(SELECT 1 FROM tenant_memberships WHERE tenant_id=$3 AND user_id=$4)",
          [job.id, encoded, job.tenant_id, job.user_id],
        );
        await db.query(
          "UPDATE query_jobs SET status='failed',result=NULL,error=CASE WHEN privacy_epoch<>(SELECT privacy_epoch FROM projects WHERE tenant_id=query_jobs.tenant_id AND id=query_jobs.project_id) THEN 'privacy_changed' ELSE 'membership_revoked' END,finished_at=now() WHERE id=$1 AND status='running'",
          [job.id],
        );
      } catch (e) {
        await db.query("ROLLBACK");
        const code =
          e instanceof QueryBudgetExceeded
            ? e.message
            : e instanceof InvalidQuery
              ? "query_expired_or_invalid"
              : (e as { code?: string }).code === "57014" ||
                  (e as Error).message === "query_timeout"
                ? "query_timeout"
                : "query_failed";
        await db.query(
          "UPDATE query_jobs SET status='failed',error=$2,finished_at=now() WHERE id=$1 AND status='running'",
          [job.id, code],
        );
      }
    } catch {
      console.error(JSON.stringify({ code: "query_job_runner_error" }));
    } finally {
      if (project) projectQueryFlights.delete(project);
      if (db) {
        if (locked)
          try {
            await db.query("SELECT pg_advisory_unlock(169202612)");
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
