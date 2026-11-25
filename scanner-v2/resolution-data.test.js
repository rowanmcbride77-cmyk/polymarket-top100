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

test("uses 50-token lookup batches and 20-condition resolution batches", async () => {
  const calls = [];
  const conditions = Array.from({ length: 21 }, (_, i) => `c${i}`);
  const tokenIds = Array.from({ length: 101 }, (_, i) => `t${i}`);
  const request = async (path, params) => {
    calls.push({ path, count: (params.token_id ?? params.condition).split(",").length, key: params.token_id ? "token_id" : "condition" });
    return { data: [] };
  };
  await fetchTokenResolutionIndex(conditions, { tokenIds, request });
  assert.deepEqual(calls.filter(x => x.path === "/v2/tokens").map(x => x.count), [50, 50, 1]);
  assert.ok(calls.filter(x => x.path === "/v2/tokens").every(x => x.key === "token_id"));
  assert.deepEqual(calls.filter(x => x.path === "/v2/resolutions").map(x => x.count), [20, 1]);
});


test("fetches token and resolution batches concurrently", async () => {
  let active = 0;
  let maximumActive = 0;
  const conditions = Array.from({ length: 41 }, (_, i) => `c${i}`);
  const request = async () => {
    active++;
    maximumActive = Math.max(maximumActive, active);
    await new Promise(resolve => setTimeout(resolve, 5));
    active--;
    return { data: [] };
  };
  await fetchTokenResolutionIndex(conditions, { request });
  assert.ok(maximumActive > 1, "metadata batches should overlap");
});

test("handles large token-resolution responses without argument-spread overflow", async () => {
  const conditions = Array.from({ length: 2000 }, (_, i) => `c${i}`);
  const request = async (path, params) => {
    const batch = (params.token_id ?? params.condition).split(",");
    if (path === "/v2/tokens") {
      return {
        data: batch.map(tokenId => ({
          tokenId,
          conditionId: "c0",
          resolved: true,
          finalPrice: 1
        }))
      };
    }
    return {
      data: batch.map(conditionId => ({
        conditionId,
        status: "resolved",
        resolvedAt: "2026-11-17T12:00:00Z"
      }))
    };
  };
  const tokenIds = Array.from({ length: 200000 }, (_, i) => "t" + i);
  const index = await fetchTokenResolutionIndex(conditions, { tokenIds, request });
  assert.equal(index.size, 200000);
  assert.equal(index.get("t999").resolved, true);
  assert.equal(index.get("t199999").resolved, true);
});
