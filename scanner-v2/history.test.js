import test from "node:test";
import assert from "node:assert/strict";
import { reconstructHeldAtResolution } from "./history.js";

const resolutionIndex = new Map([
  ["yes", { tokenId: "yes", conditionId: "c1", resolved: true, finalPrice: 1, resolvedAt: "2026-11-10T00:00:00Z" }],
  ["no", { tokenId: "no", conditionId: "c1", resolved: true, finalPrice: 0, resolvedAt: "2026-11-10T00:00:00Z" }],
  ["other", { tokenId: "other", conditionId: "c2", resolved: true, finalPrice: 0, resolvedAt: "2026-11-11T00:00:00Z" }]
]);

const event = (type, token, condition, timestamp, size, side = "") =>
  ({ type, token_id: token, condition_id: condition, timestamp, size, side });

test("counts only tokens held at resolution; later sells do not erase a valid result", () => {
  const activity = [
    event("TRADE", "yes", "c1", 1794000000, 10, "BUY"),
    event("TRADE", "yes", "c1", 1795000000, 10, "SELL")
  ];
  const result = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  assert.equal(result.positions.length, 1);
  assert.equal(result.positions[0].condition_id, "c1");
  assert.equal(result.complete, true);
});

test("excludes early exits made before resolution", () => {
  const activity = [
    event("TRADE", "yes", "c1", 1794000000, 10, "BUY"),
    event("TRADE", "yes", "c1", 1794500000, 10, "SELL")
  ];
  const result = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  assert.equal(result.positions.length, 0);
});

test("excludes hedged markets where both outcomes are held at resolution", () => {
  const activity = [
    event("TRADE", "yes", "c1", 1794000000, 10, "BUY"),
    event("TRADE", "no", "c1", 1794000000, 10, "BUY")
  ];
  const result = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  assert.equal(result.positions[0].status, "HEDGED_AT_RESOLUTION");
});

test("marks unsupported and combo events incomplete", () => {
  const activity = [
    event("TRADE", "yes", "c1", 1794000000, 10, "BUY"),
    { ...event("CONVERSION", "yes", "c1", 1794000001, 1), is_combo: true }
  ];
  const result = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  assert.equal(result.complete, false);
  assert.equal(result.diagnostics.unsupportedEvents, 1);
});

test("a position acquired exactly at resolution is not counted", () => {
  const activity = [
    event("TRADE", "yes", "c1", Date.parse("2026-11-10T00:00:00Z") / 1000, 10, "BUY")
  ];
  const result = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  assert.equal(result.positions.length, 0);
});

import { getWalletSettlementRecord } from "./history.js";

test("wallet audit uses recent settlement candidates and reconstructs pre-resolution holdings", async () => {
  const calls = [];
  const nowMs = Date.parse("2026-11-18T12:00:00Z");
  const result = await getWalletSettlementRecord("0xabc", {
    nowMs,
    fetchAllPages: async (path, params) => {
      calls.push({ path, params });
      if (path === "/v2/activity" && params.type === "REDEEM") {
        return [{ condition_id: "c1", token_id: "t1", type: "REDEEM", timestamp: 1795000000, size: 10 }];
      }
      if (path === "/v2/activity" && params.condition) {
        return [event("TRADE", "t1", "c1", 1794000000, 10, "BUY")];
      }
      return [];
    },
    fetchResolutionIndex: async () => new Map([
      ["t1", { tokenId: "t1", conditionId: "c1", resolved: true, finalPrice: 1, resolvedAt: "2026-11-17T00:00:00Z" }]
    ])
  });
  assert.equal(result.wins, 1);
  assert.equal(result.resolvedMarkets, 1);
  assert.equal(result.complete, true);
  assert.ok(calls.some(call => call.path === "/v2/activity" && call.params.start === 1 && call.params.condition === "c1"));
});

test("wallet audit does not count a trader who sold before settlement", async () => {
  const nowMs = Date.parse("2026-11-18T12:00:00Z");
  const result = await getWalletSettlementRecord("0xabc", {
    nowMs,
    fetchAllPages: async (path, params) => {
      if (path === "/v2/positions" && params.status === "CLOSED") {
        return [{ condition_id: "c1", token_id: "t1", last_event_at: 1794500000 }];
      }
      if (path === "/v2/activity" && params.condition) return [
        event("TRADE", "t1", "c1", 1794000000, 10, "BUY"),
        event("TRADE", "t1", "c1", 1794500000, 10, "SELL")
      ];
      return [];
    },
    fetchResolutionIndex: async () => new Map([
      ["t1", { tokenId: "t1", conditionId: "c1", resolved: true, finalPrice: 1, resolvedAt: "2026-11-17T00:00:00Z" }]
    ])
  });
  assert.equal(result.wins, 0);
  assert.equal(result.resolvedMarkets, 0);
  assert.equal(result.complete, true);
});
