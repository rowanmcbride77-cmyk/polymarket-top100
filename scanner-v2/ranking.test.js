import test from "node:test";
import assert from "node:assert/strict";
import { buildQualifiedLeaderboard, createSettlementRecordCache } from "./ranking.js";

const passing = (winRate = 90, resolvedMarkets = 20, complete = true) => ({
  wins: Math.round(resolvedMarkets * winRate / 100),
  losses: resolvedMarkets - Math.round(resolvedMarkets * winRate / 100),
  winRate, resolvedMarkets, complete
});

test("ranks only qualified wallets by selected-period P&L", async () => {
  const result = await buildQualifiedLeaderboard([
    { wallet: "low-pnl", pnl: 100 },
    { wallet: "high-pnl", pnl: 500 },
    { wallet: "bad-record", pnl: 900 }
  ], {
    period: "WEEK", auditLimit: 10,
    getRecord: async wallet => wallet === "bad-record" ? passing(89, 100) : passing(95, 25),
    getPeriodPnl: async wallet => ({ complete: true, value: wallet === "high-pnl" ? 500 : wallet === "low-pnl" ? 100 : 900 })
  });
  assert.deepEqual(result.qualified.map(row => row.wallet), ["high-pnl", "low-pnl"]);
  assert.equal(result.qualified[0].rank, 1);
  assert.equal(result.rejectedCount, 1);
});

test("incomplete history never qualifies, even with 100 percent accuracy", async () => {
  const result = await buildQualifiedLeaderboard([{ wallet: "x", pnl: 10 }], {
    getRecord: async () => passing(100, 100, false),
    getPeriodPnl: async () => ({ complete: true, value: 20 })
  });
  assert.equal(result.qualifiedCount, 0);
  assert.equal(result.diagnostics.incompleteHistories, 1);
});

test("does not audit beyond configured limit", async () => {
  const seen = [];
  const candidates = Array.from({ length: 10 }, (_, i) => ({ wallet: String(i), pnl: 10 - i }));
  const result = await buildQualifiedLeaderboard(candidates, {
    auditLimit: 3, getRecord: async wallet => { seen.push(wallet); return passing(); },
    getPeriodPnl: async () => ({ complete: true, value: 1 })
  });
  assert.equal(seen.length, 3);
  assert.equal(result.candidatesAudited, 3);
});

test("reuses the same complete 30-day wallet audit across weekly and monthly rankings", async () => {
  const calls = [];
  const cache = createSettlementRecordCache(async (wallet, options) => {
    calls.push({ wallet, options });
    return passing(95, 40);
  });
  const options = { windowDays: 30, nowMs: Date.parse("2026-11-19T22:00:00Z") };
  const [weekly, monthly] = await Promise.all([
    cache("0xABC", options),
    cache("0xabc", options)
  ]);
  assert.equal(calls.length, 1);
  assert.equal(weekly, monthly);
  assert.deepEqual(cache.stats(), { uniqueRecords: 1, cacheHits: 1 });
});

test("does not reuse a settlement audit from a different scoring window", async () => {
  let calls = 0;
  const cache = createSettlementRecordCache(async () => {
    calls++;
    return passing(95, 40);
  });
  await cache("0xabc", { windowDays: 30, nowMs: 123 });
  await cache("0xabc", { windowDays: 7, nowMs: 123 });
  assert.equal(calls, 2);
});
