import { expect, it } from "vitest";
import { deletionRequest } from "../packages/server/src/deletion-service";
it("requires an explicit matching user confirmation and rejects injected scope fields", () => {
  const request = {
    scope: "user",
    distinct_id: "customer-1",
    confirmation: "customer-1",
    reason: "Customer privacy request",
  };
  expect(deletionRequest(request).scope).toBe("user");
  expect(() =>
    deletionRequest({ ...request, confirmation: "customer-2" }),
  ).toThrow("invalid_confirmation");
  expect(() => deletionRequest({ ...request, tenant_id: "other" })).toThrow(
    "invalid_deletion",
  );
  expect(() => deletionRequest({ ...request, scope: ["user"] })).toThrow(
    "invalid_deletion",
  );
});
it("requires a meaningful audit reason and keeps project deletion separate from user deletion", () => {
  expect(() =>
    deletionRequest({
      scope: "project",
      confirmation: "Example",
      reason: "   ",
    }),
  ).toThrow();
  expect(() =>
    deletionRequest({
      scope: "project",
      confirmation: "Example",
      reason: "Remove test project",
      distinct_id: "customer-1",
    }),
  ).toThrow();
  expect(
    deletionRequest({
      scope: "project",
      confirmation: "Example",
      reason: "Remove test project",
    }).scope,
  ).toBe("project");
});
