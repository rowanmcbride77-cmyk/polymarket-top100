import test from "node:test";
import assert from "node:assert/strict";
import { fetchTokenResolutionIndex } from "./resolution-data.js";

test("joins token payout to condition-level finality and timestamp", async () => {
  const calls = [];
  const request = async (path, params) => {
    calls.push({ path, params });
    if (path === "/v2/tokens") return { data: [
      { tokenId: "yes-token", conditionId: "condition-1", resolved: true, finalPrice: 1 },
      { tokenId: "no-token", conditionId: "condition-1", resolved: true, finalPrice: 0 }
    ] };
    return { data: [{ conditionId: "condition-1", status: "resolved", resolvedAt: "2026-11-17T12:00:00Z" }] };
  };
  const index = await fetchTokenResolutionIndex(["condition-1"], { request });
  assert.equal(index.get("yes-token").resolved, true);
  assert.equal(index.get("yes-token").finalPrice, 1);
  assert.equal(index.get("no-token").finalPrice, 0);
  assert.equal(index.get("yes-token").resolvedAt, "2026-11-17T12:00:00Z");
  assert.equal(calls.length, 2);
});

test("does not mark token final when condition resolution is not final", async () => {
  const request = async path => path === "/v2/tokens"
    ? { data: [{ tokenId: "t", conditionId: "c", resolved: true, finalPrice: 1 }] }
    : { data: [{ conditionId: "c", status: "disputed", resolvedAt: null }] };
  const index = await fetchTokenResolutionIndex(["c"], { request });
  assert.equal(index.get("t").resolved, false);
});

test("chunks token and resolution queries at their documented limits", async () => {
  const calls = [];
  const conditions = Array.from({ length: 21 }, (_, i) => `c${i}`);
  const request = async (path, params) => {
    calls.push({ path, count: params.condition.split(",").length });
    return { data: [] };
  };
  await fetchTokenResolutionIndex(conditions, { request });
  assert.deepEqual(calls.filter(x => x.path === "/v2/tokens").map(x => x.count), [10, 10, 1]);
  assert.deepEqual(calls.filter(x => x.path === "/v2/resolutions").map(x => x.count), [20, 1]);
});
