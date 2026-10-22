export interface LedgerJob {
  id: string;
  tenant_id: string;
  project_id: string;
  user_id: string | null;
  scope: "user" | "project";
  subject_hash: string | null;
  requested_at: string;
  blocked_before: string;
  stream_expires_at: string;
}
export interface LedgerBarrier {
  tenant_id: string;
  project_id: string;
  identity_hash: string;
  deletion_job_id: string;
  requested_at: string;
  blocked_before: string;
}
export interface DeletionLedger {
  schema_version: 1;
  exported_at: string;
  projects: { tenant_id: string; id: string; privacy_epoch: number }[];
  jobs: LedgerJob[];
  barriers: LedgerBarrier[];
}
const uuid = (v: unknown): v is string =>
  typeof v === "string" &&
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
const hash = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{64}$/.test(v);
const time = (v: unknown): v is string =>
  typeof v === "string" && v.endsWith("Z") && Number.isFinite(Date.parse(v));
const fields = (v: unknown, keys: string[]) =>
  !!v &&
  typeof v === "object" &&
  !Array.isArray(v) &&
  Object.keys(v).length === keys.length &&
  Object.keys(v).every((k) => keys.includes(k));
export function parseDeletionLedger(
  input: unknown,
  now = Date.now(),
): DeletionLedger {
  const fail = (): never => {
    throw new Error("invalid_deletion_ledger");
  };
  if (
    !fields(input, [
      "schema_version",
      "exported_at",
      "projects",
      "jobs",
      "barriers",
    ])
  )
    fail();
  const b = input as DeletionLedger;
  if (
    b.schema_version !== 1 ||
    !time(b.exported_at) ||
    Date.parse(b.exported_at) > now + 300000
  )
    fail();
  if (now - Date.parse(b.exported_at) > 3600000)
    throw new Error("deletion_ledger_not_fresh");
  for (const rows of [b.projects, b.jobs, b.barriers])
    if (!Array.isArray(rows) || rows.length > 10000) fail();
  const scopes = new Set<string>();
  for (const p of b.projects) {
    if (
      !fields(p, ["tenant_id", "id", "privacy_epoch"]) ||
      !uuid(p.tenant_id) ||
      !uuid(p.id) ||
      !Number.isInteger(p.privacy_epoch) ||
      p.privacy_epoch < 0
    )
      fail();
    const key = p.tenant_id + ":" + p.id;
    if (scopes.has(key)) fail();
    scopes.add(key);
  }
  const jobs = new Map<string, LedgerJob>();
  for (const j of b.jobs) {
    if (
      !fields(j, [
        "id",
        "tenant_id",
        "project_id",
        "user_id",
        "scope",
        "subject_hash",
        "requested_at",
        "blocked_before",
        "stream_expires_at",
      ]) ||
      !uuid(j.id) ||
      !uuid(j.tenant_id) ||
      !uuid(j.project_id) ||
      (j.user_id !== null && !uuid(j.user_id)) ||
      !scopes.has(j.tenant_id + ":" + j.project_id) ||
      !["user", "project"].includes(j.scope) ||
      (j.scope === "user" ? !hash(j.subject_hash) : j.subject_hash !== null) ||
      !time(j.requested_at) ||
      !time(j.blocked_before) ||
      !time(j.stream_expires_at) ||
      Date.parse(j.blocked_before) < Date.parse(j.requested_at) ||
      Date.parse(j.stream_expires_at) < Date.parse(j.blocked_before) ||
      jobs.has(j.id)
    )
      fail();
    jobs.set(j.id, j);
  }
  const identities = new Set<string>(),referencedJobs=new Set<string>(),referencedScopes=new Set<string>();
  for (const barrier of b.barriers) {
    if (
      !fields(barrier, [
        "tenant_id",
        "project_id",
        "identity_hash",
        "deletion_job_id",
        "requested_at",
        "blocked_before",
      ]) ||
      !uuid(barrier.tenant_id) ||
      !uuid(barrier.project_id) ||
      !uuid(barrier.deletion_job_id) ||
      !time(barrier.requested_at) ||
      !time(barrier.blocked_before) ||
      Date.parse(barrier.blocked_before) < Date.parse(barrier.requested_at)
    )
      fail();
    const job = jobs.get(barrier.deletion_job_id);
    if (
      !job ||
      job.tenant_id !== barrier.tenant_id ||
      job.project_id !== barrier.project_id ||
      (job.scope === "project"
        ? barrier.identity_hash !== "*"
        : !hash(barrier.identity_hash))
    )
      fail();
    const key = barrier.project_id + ":" + barrier.identity_hash;
    if (identities.has(key)) fail();
    identities.add(key);
    referencedJobs.add(barrier.deletion_job_id);referencedScopes.add(barrier.tenant_id+':'+barrier.project_id);
  }
  for (const job of jobs.values())
    if (!referencedJobs.has(job.id)) fail();
  for (const project of b.projects)
    if (!referencedScopes.has(project.tenant_id+':'+project.id))
      fail();
  return b;
}
