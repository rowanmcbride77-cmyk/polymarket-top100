import test from "node:test";
import assert from "node:assert/strict";
import { buildSettlementRecord, classifySettledPosition, qualifiesForTop100 } from "./settlement.js";

const nowMs = Date.parse("2026-11-18T12:00:00.000Z");
const position = (condition, token, status = "REDEEMABLE") => ({
  condition_id: condition, token_id: token, status, realized_pnl: 9999
});
const resolution = (condition, token, payout, resolvedAt = "2026-11-17T12:00:00.000Z") => ({
  conditionId: condition, tokenId: token, resolved: true, finalPrice: payout, resolvedAt
});

test("counts only verified binary token settlements", () => {
  const record = buildSettlementRecord(
    [position("c1", "t1"), position("c2", "t2")],
    [resolution("c1", "t1", 1), resolution("c2", "t2", 0)],
    { nowMs, historyComplete: true }
  );
  assert.equal(record.wins, 1);
  assert.equal(record.losses, 1);
  assert.equal(record.resolvedMarkets, 2);
  assert.equal(record.winRate, 50);
});

test("excludes hedged-at-resolution conditions from directional accuracy", () => {\n  const result = classifySettledPosition(position("c1", "t1", "HEDGED_AT_RESOLUTION"), resolution("c1", "t1", 1));\n  assert.equal(result.status, "excluded");\n});\n\ntest("never counts a profitable early exit as a settlement win", () => {
  const result = classifySettledPosition(position("c1", "t1", "CLOSED"), resolution("c1", "t1", 1));
  assert.equal(result.status, "excluded");
  assert.equal(result.reason, "closed-position-may-be-early-exit");
});

test("does not count unresolved tokens or missing resolution metadata", () => {
  const record = buildSettlementRecord(
    [position("c1", "t1"), position("c2", "t2")],
    [{ ...resolution("c1", "t1", 1), resolved: false }],
    { nowMs }
  );
  assert.equal(record.resolvedMarkets, 0);
  assert.equal(record.diagnostics.pendingExcluded, 1);
  assert.equal(record.diagnostics.unverifiedExcluded, 1);
  assert.equal(record.complete, false);
});

test("excludes results outside the rolling window", () => {
  const record = buildSettlementRecord(
    [position("c1", "t1")],
    [resolution("c1", "t1", 1, "2026-10-01T12:00:00.000Z")],
    { nowMs, windowDays: 30, historyComplete: true }
  );
  assert.equal(record.resolvedMarkets, 0);
  assert.equal(record.diagnostics.outsideWindowExcluded, 1);
});

test("requires 90 percent and at least 20 verified settlements", () => {
  assert.equal(qualifiesForTop100({ complete: true, winRate: 90, resolvedMarkets: 20 }), true);
  assert.equal(qualifiesForTop100({ complete: true, winRate: 89.99, resolvedMarkets: 100 }), false);
  assert.equal(qualifiesForTop100({ complete: true, winRate: 100, resolvedMarkets: 19 }), false);
  assert.equal(qualifiesForTop100({ complete: false, winRate: 100, resolvedMarkets: 100 }), false);
});
