// Controlled 1 EPS acceptance probe. Uses a temporary company; never real customer events.
import pg from "pg";
import { randomUUID, randomBytes, scryptSync, createHash } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 1,
});
const seconds = Number(process.argv[2] || 1800);
if (!Number.isInteger(seconds) || seconds < 10 || seconds > 3600)
  throw new Error("duration must be 10..3600 seconds");
const origin = process.env.PUBLIC_ORIGIN,
  base = process.env.TEST_API || "http://127.0.0.1:3000";
const tenant = randomUUID(),
  project = randomUUID(),
  user = randomUUID(),
  key = randomBytes(24).toString("hex"),
  password = randomBytes(24).toString("hex"),
  email = `load-${user}@integration.invalid`,
  salt = randomBytes(16).toString("hex");
const hash = (v) => createHash("sha256").update(v).digest("hex");
const ingest = [],
  visible = [],
  queries = [],
  pending = new Map(),
  backlog = [],
  errors = [];
let generated = 0,
  acknowledged = 0,
  retries = 0,
  cookie = "";
const start = Date.now(),
  p95 = (a) => {
    const sorted = [...a].sort((a, b) => a - b);
    return sorted.length
      ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))]
      : null;
  };
async function observe() {
  if (!pending.size) return;
  const ids = [...pending.keys()].slice(0, 100);
  const found = await pool.query(
    "SELECT event_id FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=ANY($3::uuid[])",
    [tenant, project, ids],
  );
  const now = Date.now();
  for (const r of found.rows) {
    visible.push(now - pending.get(r.event_id));
    pending.delete(r.event_id);
  }
}
async function post(events) {
  const before = performance.now();
  try {
    const r = await fetch(base + "/v1/batch", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: JSON.stringify({ write_key: key, schema_version: 1, events }),
      signal: AbortSignal.timeout(12000),
    });
    if (r.status !== 202) throw new Error("ingest_" + r.status);
    const b = await r.json();
    if (b.accepted_event_ids.length !== events.length)
      throw new Error("invalid_ack");
    ingest.push(performance.now() - before);
    return true;
  } catch (e) {
    errors.push(String(e.message));
    return false;
  }
}
try {
  await pool.query("INSERT INTO tenants VALUES($1,$2)", [
    tenant,
    "CAPACITY TEST TEMP",
  ]);
  await pool.query("INSERT INTO users VALUES($1,$2,$3)", [
    user,
    email,
    salt + ":" + scryptSync(password, salt, 64).toString("hex"),
  ]);
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')", [
    tenant,
    user,
  ]);
  await pool.query("INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)", [
    project,
    tenant,
    "CAPACITY TEST TEMP",
  ]);
  await pool.query(
    "INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)",
    [randomUUID(), tenant, project, hash(key), "capacity-test"],
  );
  const login = await fetch(base + "/api/auth/login", {
    method: "POST",
    headers: { origin, "content-type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (login.status !== 200) throw new Error("test_login_failed");
  cookie = login.headers.get("set-cookie").split(";")[0];
  console.log(
    "Starting " +
      seconds +
      " second probe: 1 new event/second, trend query every 10 seconds, duplicate retry every 50 events",
  );
  for (let n = 0; n < seconds; n++) {
    const delay = start + n * 1000 - Date.now();
    if (delay > 0) await new Promise((r) => setTimeout(r, delay));
    const event = {
      event_id: randomUUID(),
      event_name: "capacity_probe",
      distinct_id: "probe-" + (n % 20),
      event_time: new Date().toISOString(),
      properties: { padding: "x".repeat(720), test: true },
      context: { source: "capacity-probe" },
      schema_version: 1,
    };
    generated++;
    pending.set(event.event_id, Date.now());
    backlog.push(event);
    const batch = backlog.slice(0, 20);
    if (await post(batch)) {
      acknowledged += batch.length;
      backlog.splice(0, batch.length);
    }
    if (n > 0 && n % 50 === 0) {
      retries++;
      await post([event]);
    }
    await observe();
    if (n % 10 === 0) {
      const before = performance.now();
      const r = await fetch(
        `${base}/api/tenants/${tenant}/projects/${project}/trends`,
        { headers: { cookie } },
      );
      if (r.status !== 200) errors.push("query_" + r.status);
      else queries.push(performance.now() - before);
    }
    if (n % 60 === 0) {
      const report = {
        elapsed_seconds: Math.round((Date.now() - start) / 1000),
        generated,
        acknowledged,
        backlog: backlog.length,
        not_yet_visible: pending.size,
        ingest_p95_ms: p95(ingest),
        visible_p95_ms: p95(visible),
        query_p95_ms: p95(queries),
        error_count: errors.length,
      };
      await writeFile(
        "/tmp/ccpanel-load-progress.json",
        JSON.stringify(report),
        { mode: 0o600 },
      );
      console.log(JSON.stringify(report));
    }
  }
  const deadline = Date.now() + 30000;
  while ((backlog.length || pending.size) && Date.now() < deadline) {
    if (backlog.length) {
      const b = backlog.slice(0, 20);
      if (await post(b)) {
        acknowledged += b.length;
        backlog.splice(0, b.length);
      }
    }
    await observe();
    await new Promise((r) => setTimeout(r, 250));
  }
  const counts = await pool.query(
    "SELECT (SELECT count(*)::int FROM events WHERE tenant_id=$1) AS events,(SELECT count(*)::int FROM event_dedup WHERE tenant_id=$1) AS dedup,(SELECT count(*)::int FROM aggregation_jobs WHERE tenant_id=$1) AS aggregate_jobs,(SELECT count(*)::int FROM dlq_records WHERE tenant_id=$1) AS dlq",
    [tenant],
  );
  const result = {
    duration_seconds: Math.round((Date.now() - start) / 1000),
    target_eps: 1,
    generated,
    acknowledged,
    duplicate_requests: retries,
    backlog: backlog.length,
    not_yet_visible: pending.size,
    counts: counts.rows[0],
    ingest_p95_ms: p95(ingest),
    observed_visible_p95_ms: p95(visible),
    query_p95_ms: p95(queries),
    errors,
    passed:
      generated === seconds &&
      counts.rows[0].events === seconds &&
      counts.rows[0].dedup === seconds &&
      counts.rows[0].aggregate_jobs === seconds &&
      !counts.rows[0].dlq &&
      !pending.size &&
      !backlog.length &&
      !errors.length &&
      p95(ingest) < 1000 &&
      p95(visible) < 10000,
  };
  await writeFile(
    "/tmp/ccpanel-load-result.json",
    JSON.stringify(result, null, 2),
    { mode: 0o600 },
  );
  console.log(JSON.stringify(result));
  if (!result.passed) process.exitCode = 1;
} finally {
  for (const table of [
    "dlq_records",
    "aggregation_jobs",
    "ingest_metrics",
    "events",
    "event_dedup",
    "write_keys",
    "projects",
    "tenant_memberships",
  ])
    await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`, [tenant]);
  await pool.query("DELETE FROM sessions WHERE user_id=$1", [user]);
  await pool.query("DELETE FROM users WHERE id=$1", [user]);
  await pool.query("DELETE FROM tenants WHERE id=$1", [tenant]);
  await pool.end();
}
