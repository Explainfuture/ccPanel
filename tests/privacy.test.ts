import { expect, it, vi } from "vitest";
import type { PoolClient } from "pg";
import {
  identityHash,
  suppressedByDeletion,
} from "../packages/server/src/privacy";
import type { StoredEnvelope } from "../packages/schema/src/index";
const record: StoredEnvelope = {
  tenant_id: crypto.randomUUID(),
  project_id: crypto.randomUUID(),
  received_at: new Date().toISOString(),
  event: {
    event_id: crypto.randomUUID(),
    event_name: "click",
    distinct_id: "same-user",
    event_time: new Date().toISOString(),
    properties: {},
    context: {},
    schema_version: 1,
  },
};
it("namespaces identity lookup hashes by company and project", () => {
  const a = identityHash(record.tenant_id, record.project_id, "same-user");
  expect(a).toMatch(/^[a-f0-9]{64}$/);
  expect(a).not.toBe(
    identityHash(crypto.randomUUID(), record.project_id, "same-user"),
  );
  expect(a).not.toBe(
    identityHash(record.tenant_id, crypto.randomUUID(), "same-user"),
  );
});
it("checks current canonical identity and original event/acceptance times", async () => {
  const query = vi
    .fn()
    .mockResolvedValueOnce({ rows: [{ user_id: "canonical-user" }] })
    .mockResolvedValueOnce({ rowCount: 1 });
  expect(
    await suppressedByDeletion({ query } as unknown as PoolClient, record),
  ).toBe(true);
  const values = query.mock.calls[1]![1];
  expect(values.slice(0, 2)).toEqual([record.tenant_id, record.project_id]);
  expect(values[2]).toContain(
    identityHash(record.tenant_id, record.project_id, "canonical-user"),
  );
  expect(values.slice(3)).toEqual([
    record.event.event_time,
    record.received_at,
  ]);
});
it("an already-covered identify retry does not reopen cleanup or invalidate newer query results", async () => {
  const identify = {
    ...record,
    event: {
      ...record.event,
      event_name: "$identify",
      anonymous_id: "old-anonymous",
    },
  };
  const query = vi
    .fn()
    .mockResolvedValueOnce({
      rows: [
        {
          deletion_job_id: crypto.randomUUID(),
          requested_at: record.received_at,
          blocked_before: record.received_at,
        },
      ],
    })
    .mockResolvedValueOnce({ rowCount: 0 })
    .mockResolvedValueOnce({ rows: [] })
    .mockResolvedValueOnce({ rowCount: 1 });
  expect(
    await suppressedByDeletion({ query } as unknown as PoolClient, identify),
  ).toBe(true);
  expect(
    query.mock.calls.some(([sql]) => String(sql).startsWith("UPDATE")),
  ).toBe(false);
});
