import { expect, it } from "vitest";
import {
  parseDeletionLedger,
  type DeletionLedger,
} from "../packages/server/src/deletion-ledger-format";
const now = Date.now(),
  tenant = crypto.randomUUID(),
  project = crypto.randomUUID(),
  id = crypto.randomUUID(),
  hash = "a".repeat(64),
  at = new Date(now).toISOString();
const ledger = (): DeletionLedger => ({
  schema_version: 1,
  exported_at: at,
  projects: [{ tenant_id: tenant, id: project, privacy_epoch: 2 }],
  jobs: [
    {
      id,
      tenant_id: tenant,
      project_id: project,
      user_id: null,
      scope: "user",
      subject_hash: hash,
      requested_at: at,
      blocked_before: new Date(now + 300000).toISOString(),
      stream_expires_at: new Date(now + 45 * 86400000).toISOString(),
    },
  ],
  barriers: [
    {
      tenant_id: tenant,
      project_id: project,
      identity_hash: hash,
      deletion_job_id: id,
      requested_at: at,
      blocked_before: new Date(now + 300000).toISOString(),
    },
  ],
});
it("accepts a complete scoped ledger including an actor absent from the old backup", () => {
  expect(parseDeletionLedger(ledger(), now).jobs[0]!.user_id).toBeNull();
});
it('requires every deletion scope to have policy metadata in version2',()=>{
  const base=ledger(),value={...base,schema_version:2,retention:[]};
  expect(()=>parseDeletionLedger(value,now)).toThrow();
  const policy={tenant_id:tenant,project_id:project,raw_days:30,version:1,floor:null,effective_from:new Date(now-30*86400000).toISOString()};
  expect(parseDeletionLedger({...value,retention:[policy]},now).schema_version).toBe(2);
  expect(()=>parseDeletionLedger({...value,retention:[{...policy,tenant_id:crypto.randomUUID()}]},now)).toThrow();
});
it("rejects cross-company reference changes, duplicate identities and incomplete graphs", () => {
  const forged = ledger();
  forged.barriers[0]!.tenant_id = crypto.randomUUID();
  expect(() => parseDeletionLedger(forged, now)).toThrow(
    "invalid_deletion_ledger",
  );
  const duplicate = ledger();
  duplicate.barriers.push({ ...duplicate.barriers[0]! });
  expect(() => parseDeletionLedger(duplicate, now)).toThrow();
  const incomplete = ledger();
  incomplete.barriers = [];
  expect(() => parseDeletionLedger(incomplete, now)).toThrow();
  const wildcard = ledger();
  wildcard.barriers[0]!.identity_hash = "*";
  expect(() => parseDeletionLedger(wildcard, now)).toThrow();
});
it("rejects stale and unknown-format ledgers without treating them as an empty deletion list", () => {
  expect(() => parseDeletionLedger(ledger(), now + 3600001)).toThrow(
    "deletion_ledger_not_fresh",
  );
  expect(() =>
    parseDeletionLedger({ ...ledger(), schema_version: 2 }, now),
  ).toThrow();
  expect(() =>
    parseDeletionLedger({ ...ledger(), secret: "not part of the format" }, now),
  ).toThrow();
});
