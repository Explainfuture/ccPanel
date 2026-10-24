import { expect, it, vi } from "vitest";
import { parseQuerySpec } from "../packages/server/src/query-jobs";
import {
  behaviorQuery,
  QueryBudgetExceeded,
} from "../packages/server/src/behavior-routes";
import type { query } from "../packages/server/src/db";

const range = {
  from: new Date(Date.now() - 86400000).toISOString(),
  to: new Date().toISOString(),
};
it("queued queries reject scope injection and arbitrary SQL", () => {
  expect(() =>
    parseQuerySpec({ ...range, kind: "trend", tenant_id: "other" }),
  ).toThrow();
  expect(() =>
    parseQuerySpec({ ...range, kind: "sql", sql: "SELECT * FROM events" }),
  ).toThrow();
});
it("behavior queries reject oversized data before loading event bodies", async () => {
  const run = vi.fn().mockResolvedValue({ rows: [{ count: 10001, bytes: 1 }] });
  const spec = parseQuerySpec({
    ...range,
    kind: "funnel",
    steps: ["start", "finish"],
    window_hours: 24,
  });
  if (spec.kind === "trend") throw new Error("wrong fixture");
  await expect(
    behaviorQuery(
      "company",
      "project",
      spec,
      run as unknown as typeof query,
      true,
    ),
  ).rejects.toBeInstanceOf(QueryBudgetExceeded);
  expect(run).toHaveBeenCalledTimes(1);
  expect(run.mock.calls[0]![1].slice(0, 2)).toEqual(["company", "project"]);
});
