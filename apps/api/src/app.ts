import { Hono, type Context } from "hono";
import { getCookie, setCookie, deleteCookie } from "hono/cookie";
import { bodyLimit } from "hono/body-limit";
import { registerReportRoutes } from '../../../packages/server/src/report-routes';
import {ReportError} from '../../../packages/server/src/report-spec';
import {InvalidQuery} from '../../../packages/server/src/query-spec';
import { registerBehaviorRoutes } from '../../../packages/server/src/behavior-routes';
import { registerSchemaRoutes } from '../../../packages/server/src/schema-routes';
import { registerUserRoutes } from '../../../packages/server/src/user-routes';
import {registerQueryExports} from '../../../packages/server/src/query-export';
import { registerQueryJobs } from '../../../packages/server/src/query-jobs';
import { registerDeletions } from '../../../packages/server/src/deletion-service';
import { registerReplayRoutes } from '../../../packages/server/src/dlq-replay';
import { registerInvitationAcceptance,registerCompanyInvitations,lockMembership,membershipAdmin } from '../../../packages/server/src/invitations';
import { IngestionPaused,type AdmissionStatus } from '../../../packages/server/src/ingest-admission';
import { lockProjectPrivacy } from '../../../packages/server/src/privacy';
import { checkRequired } from '../../../packages/server/src/project-schema';
import { projectQueryFlights as queryFlights } from '../../../packages/server/src/query-budget';
import { randomUUID } from "node:crypto";
import { pool, query } from "../../../packages/server/src/db";
import {
  digest,
  token,
  passwordHash,
  passwordMatches,
} from "../../../packages/server/src/security";
import {
  limits,
  validateEvent,
  type AnalyticsEvent,
  type StoredEnvelope,
} from "../../../packages/schema/src/index";

type Role = "admin" | "analyst" | "viewer";
export type Env = { Variables: { userId: string; tenantId: string; role: Role } };
export interface AppDeps {
  publish: (records: StoredEnvelope[]) => Promise<void>;
  ready: () => boolean;
  admissionStatus?:()=>AdmissionStatus;
}
const validId = (s: string | undefined) =>
  typeof s === "string" && /^[0-9a-f]{8}-[0-9a-f-]{27}$/i.test(s);
const allowed = (c: Context<Env>, roles: Role[]) =>
  roles.includes(c.get("role"));
export function createApp(deps: AppDeps) {
  const app = new Hono<Env>();
  const buckets = new Map<string, { count: number; expires: number }>();
  function rate(key: string, max: number, interval: number) {
    const now = Date.now();
    if (buckets.size > 10000)
      for (const [k, v] of buckets) if (v.expires < now) buckets.delete(k);
    const old = buckets.get(key);
    const b =
      old && old.expires > now ? old : { count: 0, expires: now + interval };
    if (!old && buckets.size >= 10000) return false;
    b.count++;
    buckets.set(key, b);
    return b.count <= max;
  }
  app.onError((e, c) => {
    if(e instanceof ReportError)return c.json({error:e.message},e.status);
    if(e instanceof InvalidQuery)return c.json({error:e.message},400);
    if(e instanceof IngestionPaused){c.header('Retry-After',String(e.retryAfterSeconds));return c.json({error:'ingestion_paused',reason:e.reason},503);}
    console.error(JSON.stringify({ code: "request_failed", type: e.name }));
    return c.json({ error: "temporarily_unavailable" }, 503);
  });
  app.use("*", async (c, next) => {
    c.header("X-Content-Type-Options", "nosniff");
    c.header("Cache-Control", "no-store");
    await next();
  });
  app.get("/health", (c) =>
    c.json({ status: "ok", service: "ccpanel", stage: "development" }),
  );
  app.get("/ready", async (c) => {
    await query("SELECT 1");
    const ready=deps.ready()&&!!deps.admissionStatus?.().allowed;
    return c.json(
      { status: ready ? "ready" : "unavailable" },
      ready ? 200 : 503,
    );
  });
  app.use('/v1/batch',async(c,next)=>{
    // Public, cookie-free ingestion errors must be readable by cross-origin SDKs.
    c.header('Access-Control-Allow-Origin','*');
    c.header('Access-Control-Expose-Headers','Retry-After');
    await next();
  });
  app.options("/v1/batch", (c) => {
    c.header("Access-Control-Allow-Origin", "*");
    c.header("Access-Control-Allow-Methods", "POST");
    c.header("Access-Control-Allow-Headers", "content-type");
    return c.body(null, 204);
  });
  app.post(
    "/v1/batch",
    bodyLimit({
      maxSize: limits.requestBytes,
      onError: (c) => c.json({ error: "batch_too_large" }, 413),
    }),
    async (c) => {
      let body: any;
      try {
        body = JSON.parse(await c.req.text());
      } catch {
        return c.json({ error: "invalid_json" }, 400);
      }
      if (
        !body ||
        typeof body.write_key !== "string" ||
        body.write_key.length > 256
      )
        return c.json({ error: "invalid_key" }, 401);
      const found = await query(
        "SELECT k.tenant_id,k.project_id,p.origins,p.schema_mode FROM write_keys k JOIN projects p ON p.id=k.project_id AND p.tenant_id=k.tenant_id WHERE k.key_hash=$1 AND k.revoked_at IS NULL",
        [digest(body.write_key)],
      );
      const key = found.rows[0];
      if (!key) return c.json({ error: "invalid_key" }, 401);
      const origin = c.req.header("origin");
      if (origin && !(key.origins as string[]).includes(origin))
        return c.json({ error: "origin_not_allowed" }, 403);
      if (origin) {
        c.header("Access-Control-Allow-Origin", origin);
        c.header("Vary", "Origin");
      }
      if (
        body.tenant_id !== undefined ||
        (body.project_id !== undefined && body.project_id !== key.project_id)
      )
        return c.json({ error: "scope_mismatch" }, 400);
      if (
        body.schema_version !== 1 ||
        !Array.isArray(body.events) ||
        !body.events.length ||
        body.events.length > limits.serverEvents
      )
        return c.json({ error: "invalid_batch" }, 400);
      const schemas = await query(
        "SELECT event_name,required_properties,version FROM event_schemas WHERE tenant_id=$1 AND project_id=$2",
        [key.tenant_id, key.project_id],
      );
      const schemaMap = new Map(schemas.rows.map(s => [s.event_name,s]));
      const errors: { index: number; code: string }[] = [];
      body.events.forEach((event: unknown, index: number) => {
        let code = validateEvent(event);
        if (!code) {
          const e = event as AnalyticsEvent,
            schema = schemaMap.get(e.event_name);
          if (
            key.schema_mode === "strict" &&
            !schema &&
            e.event_name !== "$identify"
          )
            code = "unknown_event";
          if (schema) code = checkRequired(e,schema.required_properties);
        }
        if (code) errors.push({ index, code });
      });
      if (errors.length)
        return c.json({ error: "invalid_events", errors }, 400);
      if (!rate("ingest:" + key.tenant_id, 1200, 60000)) {
        c.header("Retry-After", "60");
        return c.json({ error: "rate_limited" }, 429);
      }
      if (!deps.ready()) {c.header('Retry-After','5');return c.json({ error: "kafka_unavailable" }, 503);}
      const admission=deps.admissionStatus?.();
      if(!admission?.allowed)throw new IngestionPaused(admission?.reason??'pressure_probe_unavailable',admission?.retryAfterSeconds||5);
      const received_at = new Date().toISOString();
      await deps.publish(
        body.events.map((event: AnalyticsEvent) => ({
          tenant_id: key.tenant_id,
          project_id: key.project_id,
          event,
          received_at,
          validation: {registered:event.event_name==='$identify'||schemaMap.has(event.event_name),version:event.event_name==='$identify'?1:(schemaMap.get(event.event_name)?.version||0)},
        })),
      );
      return c.json(
        {
          accepted_event_ids: body.events.map(
            (e: AnalyticsEvent) => e.event_id,
          ),
        },
        202,
      );
    },
  );
  app.use(
    "/api/*",
    bodyLimit({
      maxSize: 65536,
      onError: (c) => c.json({ error: "too_large" }, 413),
    }),
  );
  app.post("/api/auth/login", async (c) => {
    if (c.req.header("origin") !== process.env.PUBLIC_ORIGIN)
      return c.json({ error: "origin_not_allowed" }, 403);
    const ip = c.req.header("x-real-ip") || "local";
    if (!rate("login:" + ip, 10, 60000))
      return c.json({ error: "rate_limited" }, 429);
    const b = await c.req.json().catch(() => null);
    if (
      !b ||
      typeof b.email !== "string" ||
      typeof b.password !== "string" ||
      b.password.length > 1024
    )
      return c.json({ error: "invalid_credentials" }, 401);
    const found = await query(
      "SELECT id,password_hash FROM users WHERE email=$1",
      [b.email.toLowerCase()],
    );
    const u = found.rows[0];
    // Run scrypt for unknown users too; never expose account existence.
    const matches = passwordMatches(
      b.password,
      u?.password_hash || "00000000000000000000000000000000:" + "0".repeat(128),
    );
    if (!u || !matches) return c.json({ error: "invalid_credentials" }, 401);
    const session = token(),
      csrf = digest(session + ":csrf");
    await query(
      "INSERT INTO sessions VALUES($1,$2,$3,now()+interval '12 hours')",
      [digest(session), u.id, digest(csrf)],
    );
    setCookie(c, "ccpanel_session", session, {
      httpOnly: true,
      secure: process.env.COOKIE_SECURE !== "false",
      sameSite: "Strict",
      path: "/",
      maxAge: 43200,
    });
    return c.json({ csrf_token: csrf });
  });
  app.use("/api/*", async (c, next) => {
    const session = getCookie(c, "ccpanel_session");
    if (!session) return c.json({ error: "unauthenticated" }, 401);
    const r = await query(
      "SELECT user_id,csrf_hash FROM sessions WHERE token_hash=$1 AND expires_at>now()",
      [digest(session)],
    );
    if (!r.rows[0]) return c.json({ error: "unauthenticated" }, 401);
    if (
      !["GET", "HEAD"].includes(c.req.method) &&
      (c.req.header("origin") !== process.env.PUBLIC_ORIGIN ||
        digest(c.req.header("x-csrf-token") || "") !== r.rows[0].csrf_hash)
    )
      return c.json({ error: "csrf_failed" }, 403);
    c.set("userId", r.rows[0].user_id);
    await next();
  });
  app.get("/api/me", async (c) => {
    const u = await query("SELECT id,email FROM users WHERE id=$1", [
      c.get("userId"),
    ]);
    const t = await query(
      "SELECT t.id,t.name,m.role FROM tenant_memberships m JOIN tenants t ON t.id=m.tenant_id WHERE m.user_id=$1 ORDER BY t.name",
      [c.get("userId")],
    );
    return c.json({
      user: u.rows[0],
      tenants: t.rows,
      csrf_token: digest(getCookie(c, "ccpanel_session")! + ":csrf"),
    });
  });
  registerInvitationAcceptance(app,rate);
  app.post("/api/auth/logout", async (c) => {
    await query("DELETE FROM sessions WHERE token_hash=$1", [
      digest(getCookie(c, "ccpanel_session")!),
    ]);
    deleteCookie(c, "ccpanel_session", { path: "/" });
    return c.json({ ok: true });
  });
  app.use("/api/tenants/:tenant/*", async (c, next) => {
    const tenant = c.req.param("tenant");
    if (!validId(tenant)) return c.json({ error: "not_found" }, 404);
    const r = await query(
      "SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2",
      [tenant, c.get("userId")],
    );
    if (!r.rows[0]) return c.json({ error: "not_found" }, 404);
    c.set("tenantId", tenant);
    c.set("role", r.rows[0].role);
    await next();
  });
  app.get("/api/tenants/:tenant/projects", async (c) =>
    c.json({
      projects: (
        await query(
          "SELECT id,name,timezone,origins,schema_mode FROM projects WHERE tenant_id=$1 ORDER BY created_at",
          [c.get("tenantId")],
        )
      ).rows,
    }),
  );
  registerCompanyInvitations(app,rate);
  app.post("/api/tenants/:tenant/projects", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    const b = await c.req.json();
    if (
      typeof b.name !== "string" ||
      !b.name.trim() ||
      b.name.length > 128 ||
      !Array.isArray(b.origins) ||
      b.origins.length > 20 ||
      !b.origins.every((o: unknown) => {
        try {
          return (
            typeof o === "string" &&
            new URL(o).origin === o &&
            /^https?:/.test(o)
          );
        } catch {
          return false;
        }
      })
    )
      return c.json({ error: "invalid_project" }, 400);
    const id = randomUUID();
    await query(
      "INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)",
      [id, c.get("tenantId"), b.name, b.origins],
    );
    return c.json({ id }, 201);
  });
  app.get("/api/tenants/:tenant/members", async (c) =>
    c.json({
      members: (
        await query(
          "SELECT u.id,u.email,m.role FROM tenant_memberships m JOIN users u ON u.id=m.user_id WHERE m.tenant_id=$1",
          [c.get("tenantId")],
        )
      ).rows,
    }),
  );
  app.post("/api/tenants/:tenant/members", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    const b = await c.req.json();
    if (
      typeof b.email !== "string" ||
      b.email.length > 254 ||
      !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(b.email) ||
      typeof b.password !== "string" ||
      b.password.length < 12 ||
      b.password.length > 1024 ||
      !["admin", "analyst", "viewer"].includes(b.role)
    )
      return c.json({ error: "invalid_member" }, 400);
    const existing = await query("SELECT id FROM users WHERE email=$1", [
      b.email.toLowerCase(),
    ]);
    // Existing accounts join through their own authenticated invitation acceptance.
    if (existing.rowCount)
      return c.json({ error: "account_requires_invitation_acceptance" }, 409);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await lockMembership(client,c.get('tenantId'));
      if(!await membershipAdmin(client,c.get('tenantId'),c.get('userId'))){await client.query('ROLLBACK');return c.json({error:'forbidden'},403);}
      const id = randomUUID();
      await client.query("INSERT INTO users VALUES($1,$2,$3)", [
        id,
        b.email.toLowerCase(),
        passwordHash(b.password),
      ]);
      await client.query("INSERT INTO tenant_memberships VALUES($1,$2,$3)", [
        c.get("tenantId"),
        id,
        b.role,
      ]);
      await client.query(
        "INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,$3,$4)",
        [
          c.get("tenantId"),
          c.get("userId"),
          "member_created",
          JSON.stringify({ user_id: id, role: b.role }),
        ],
      );
      await client.query("COMMIT");
      return c.json({ id }, 201);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  });
  app.delete("/api/tenants/:tenant/members/:user", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    if (!validId(c.req.param("user")))
      return c.json({ error: "not_found" }, 404);
    if (c.req.param("user") === c.get("userId"))
      return c.json({ error: "cannot_remove_self" }, 400);
    const db=await pool.connect();
    try{
      await db.query('BEGIN');await lockMembership(db,c.get('tenantId'));
      if(!await membershipAdmin(db,c.get('tenantId'),c.get('userId'))){await db.query('ROLLBACK');return c.json({error:'forbidden'},403);}
      await db.query('DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[c.get('tenantId'),c.req.param('user')]);
      await db.query('UPDATE tenant_invitations SET revoked_at=clock_timestamp() WHERE tenant_id=$1 AND accepted_at IS NULL AND revoked_at IS NULL AND (created_by=$2 OR email=(SELECT email FROM users WHERE id=$2))',[c.get('tenantId'),c.req.param('user')]);
      await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'member_removed',$3)",[c.get('tenantId'),c.get('userId'),JSON.stringify({user_id:c.req.param('user')})]);
      await db.query('COMMIT');return c.json({ok:true});
    }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
  });
  app.use("/api/tenants/:tenant/projects/:project/*", async (c, next) => {
    const id = c.req.param("project");
    if (!validId(id)) return c.json({ error: "not_found" }, 404);
    if (
      !(
        await query("SELECT 1 FROM projects WHERE tenant_id=$1 AND id=$2", [
          c.get("tenantId"),
          id,
        ])
      ).rowCount
    )
      return c.json({ error: "not_found" }, 404);
    await next();
  });
  const prefix = "/api/tenants/:tenant/projects/:project";
  app.get(prefix + "/trends", async (c) => {
    const project = c.req.param("project")!.toLowerCase();
    if (queryFlights.has(project)) return c.json({ error: "query_busy" }, 429);
    const to = c.req.query("to") ? Date.parse(c.req.query("to")!) : Date.now();
    const from = c.req.query("from")
      ? Date.parse(c.req.query("from")!)
      : to - 7 * 86400000;
    if (
      !Number.isFinite(to) ||
      !Number.isFinite(from) ||
      from >= to ||
      to - from > 30 * 86400000 ||
      from < Date.now() - 31 * 86400000 ||
      to > Date.now() + 60000
    )
      return c.json({ error: "invalid_time_range" }, 400);
    const name = c.req.query("event_name") || null;
    queryFlights.add(project);
    try {
      const params = [
        c.get("tenantId"),
        project,
        new Date(from).toISOString(),
        new Date(to).toISOString(),
        name,
      ];
      const [summary, series, names] = await Promise.all([
        query(
          `SELECT count(*)::int AS events,count(DISTINCT COALESCE(l.user_id,e.distinct_id))::int AS users,max(e.received_at) AS latest_received_at FROM visible_events e LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id WHERE e.tenant_id=$1 AND e.project_id=$2 AND e.event_time >= $3 AND e.event_time < $4 AND ($5::text IS NULL OR e.event_name=$5) AND e.event_name<>'$identify'`,
          params,
        ),
        query(
          `SELECT to_char(date_trunc('day',e.event_time AT TIME ZONE p.timezone),'YYYY-MM-DD') AS day,count(*)::int AS events,count(DISTINCT COALESCE(l.user_id,e.distinct_id))::int AS users FROM visible_events e JOIN projects p ON p.tenant_id=e.tenant_id AND p.id=e.project_id LEFT JOIN identity_links l ON l.tenant_id=e.tenant_id AND l.project_id=e.project_id AND l.anonymous_id=e.distinct_id WHERE e.tenant_id=$1 AND e.project_id=$2 AND e.event_time >= $3 AND e.event_time < $4 AND ($5::text IS NULL OR e.event_name=$5) AND e.event_name<>'$identify' GROUP BY 1 ORDER BY 1`,
          params,
        ),
        query(
          `SELECT event_name,count(*)::int AS count FROM visible_events WHERE tenant_id=$1 AND project_id=$2 AND event_time >= $3 AND event_time < $4 AND event_name<>'$identify' GROUP BY event_name ORDER BY count(*) DESC LIMIT 20`,
          params.slice(0, 4),
        ),
      ]);
      return c.json({
        summary: summary.rows[0],
        series: series.rows,
        top_events: names.rows,
        from: new Date(from).toISOString(),
        to: new Date(to).toISOString(),
        computed_at: new Date().toISOString(),
      });
    } finally {
      queryFlights.delete(project);
    }
  });
  app.get(prefix + "/keys", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    return c.json({
      keys: (
        await query(
          "SELECT id,label,revoked_at FROM write_keys WHERE tenant_id=$1 AND project_id=$2",
          [c.get("tenantId"), c.req.param("project")],
        )
      ).rows,
    });
  });
  app.post(prefix + "/keys", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    const key = "ccp_" + token(),
      id = randomUUID();
    const db=await pool.connect();
    try{
    await db.query('BEGIN');await lockProjectPrivacy(db,c.req.param('project')!);
    const inserted=await db.query(
      "INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) SELECT $1,$2,$3,$4,$5 WHERE NOT EXISTS(SELECT 1 FROM deletion_barriers WHERE tenant_id=$2 AND project_id=$3 AND identity_hash='*') RETURNING id",
      [
        id,
        c.get("tenantId"),
        c.req.param("project"),
        digest(key),
        "SDK 写入凭证",
      ],
    );
    await db.query('COMMIT');if(!inserted.rowCount)return c.json({error:'project_deleted'},409);
    }catch(e){await db.query('ROLLBACK');throw e;}finally{db.release();}
    return c.json({ id, write_key: key }, 201);
  });
  app.delete(prefix + "/keys/:key", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    if (!validId(c.req.param("key")))
      return c.json({ error: "not_found" }, 404);
    await query(
      "UPDATE write_keys SET revoked_at=now() WHERE tenant_id=$1 AND project_id=$2 AND id=$3",
      [c.get("tenantId"), c.req.param("project"), c.req.param("key")],
    );
    return c.json({ ok: true });
  });
  app.get(prefix + "/events", async (c) => {
    const name = c.req.query("event_name") || null,
      user = c.req.query("distinct_id") || null;
    const page = Math.max(0, Math.min(Number(c.req.query("page")) || 0, 1000));
    const rows = await query(
        `SELECT event_id,event_name,distinct_id,event_time,received_at,properties,context,registered,project_schema_version FROM visible_events WHERE tenant_id=$1 AND project_id=$2 AND event_time>=now()-interval '30 days' AND ($3::text IS NULL OR event_name=$3) AND ($4::text IS NULL OR distinct_id=$4) ORDER BY event_time DESC,event_id LIMIT 50 OFFSET $5`,
      [c.get("tenantId"), c.req.param("project"), name, user, page * 50],
    );
    return c.json({
      events: rows.rows,
      page,
      computed_at: new Date().toISOString(),
    });
  });
  app.get(prefix + "/metrics", async (c) =>
    c.json({
      metrics: (
        await query(
          "SELECT metric,value FROM ingest_metrics WHERE tenant_id=$1 AND project_id=$2",
          [c.get("tenantId"), c.req.param("project")],
        )
      ).rows,
    }),
  );
  app.get(prefix+'/ingest-status',c=>{
    if(!allowed(c,['admin']))return c.json({error:'forbidden'},403);
    return c.json({scope:'platform_aggregate',computed_at:new Date().toISOString(),admission:deps.admissionStatus?.()??null});
  });
  app.get(prefix + "/dlq", async (c) => {
    if (!allowed(c, ["admin"])) return c.json({ error: "forbidden" }, 403);
    return c.json({
      records: (
        await query(
          "SELECT id,reason,created_at FROM dlq_records WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at DESC LIMIT 50",
          [c.get("tenantId"), c.req.param("project")],
        )
      ).rows,
    });
  });
  registerReportRoutes(app);
  registerBehaviorRoutes(app);
  registerSchemaRoutes(app);
  registerUserRoutes(app);
  registerQueryJobs(app);
  registerQueryExports(app,rate);
  registerDeletions(app);
  registerReplayRoutes(app);
  return app;
}
