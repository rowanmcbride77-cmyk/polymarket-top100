import test from "node:test";
import assert from "node:assert/strict";
import { buildConsensusSignals, classifyMarket, verifyRecentBuys } from "./consensus.js";

const wallet = n => "0x" + String(n).padStart(40, "0");
const baseMarket = {
  conditionId: "c1", question: "Will BTC be above $100k?",
  category: "Crypto", active: true, closed: false,
  endDate: "2026-11-18T23:00:00Z"
};
const tokenResolution = new Map([
  ["t1", { tokenId: "t1", conditionId: "c1", resolved: false, finalPrice: null }],
  ["t2", { tokenId: "t2", conditionId: "c1", resolved: false, finalPrice: null }]
]);
const pos = (walletId, token = "t1", outcome = "Yes") => ({
  wallet: walletId,
  trader: { wallet: walletId, name: "Trader", rank: 1, selectedPeriodPnl: 100 },
  position: {
    condition_id: "c1", token_id: token, outcome, status: "OPEN",
    current_size: 20, current_value: 12, entry_cost_usdc: 10
  }
});

test("classifies only supported sports, esports and crypto markets", () => {
  assert.equal(classifyMarket({ category: "Politics", question: "Bitcoin election poll" }), "excluded");
  assert.equal(classifyMarket({ category: "Sports", question: "NBA over 22.5 points" }), "sports");
  assert.equal(classifyMarket({ category: "Esports", question: "League of Legends winner" }), "esports");
  assert.equal(classifyMarket({ category: "Crypto", question: "Bitcoin above $100k" }), "crypto");
});

test("requires recent same-token BUY activity and minimum entry cost", async () => {
  const rows = [pos(wallet(1))];
  const verified = await verifyRecentBuys(rows, {
    nowMs: Date.parse("2026-11-18T12:00:00Z"),
    fetchAllPages: async () => [{ condition_id: "c1", token_id: "t1", usdc_size: 15 }]
  });
  assert.equal(verified.size, 1);
  assert.equal(verified.values().next().value.entryCostUsd, 10);
});

test("groups only same outcome token held by multiple qualified wallets", async () => {
  const rows = [pos(wallet(1)), pos(wallet(2)), pos(wallet(3), "t2")];
  const result = await buildConsensusSignals(
    rows.map(row => row.trader),
    {
      nowMs: Date.parse("2026-11-18T12:00:00Z"),
      fetchQualifiedOpenPositions: undefined,
      fetchAllPages: async (_path, params) => {
        if (params.status === "OPEN") return rows.filter(row => row.wallet === params.user).map(row => row.position);
        return [{ condition_id: "c1", token_id: "t1", usdc_size: 12 }];
      },
      fetchGammaMarkets: async () => [baseMarket],
      fetchTokenResolutionIndex: async () => tokenResolution
    }
  );
  assert.equal(result.signals.length, 1);
  assert.equal(result.signals[0].qualifiedWalletCount, 2);
  assert.equal(result.signals[0].outcome, "Yes");
});

test("suppresses a condition when both outcome sides independently reach consensus", async () => {
  const rows = [
    pos(wallet(1), "t1", "Yes"),
    pos(wallet(2), "t1", "Yes"),
    pos(wallet(3), "t2", "No"),
    pos(wallet(4), "t2", "No")
  ];
  const result = await buildConsensusSignals(
    rows.map(row => row.trader),
    {
      nowMs: Date.parse("2026-11-18T12:00:00Z"),
      fetchAllPages: async (_path, params) => {
        if (params.status === "OPEN") return rows.filter(row => row.wallet === params.user).map(row => row.position);
        return [
          { condition_id: "c1", token_id: "t1", usdc_size: 12 },
          { condition_id: "c1", token_id: "t2", usdc_size: 12 }
        ];
      },
      fetchGammaMarkets: async () => [baseMarket],
      fetchTokenResolutionIndex: async () => tokenResolution
    }
  );
  assert.equal(result.signals.length, 0);
  assert.equal(result.diagnostics.excludedOppositeSideConditions, 1);
});


test("reports positions excluded because they lack a qualifying recent BUY", async () => {
  const rows = [pos(wallet(9))];
  const result = await buildConsensusSignals(
    rows.map(row => row.trader),
    {
      nowMs: Date.parse("2026-11-18T12:00:00Z"),
      fetchAllPages: async (_path, params) => {
        if (params.status === "OPEN") return rows.filter(row => row.wallet === params.user).map(row => row.position);
        return [];
      },
      fetchGammaMarkets: async () => [baseMarket],
      fetchTokenResolutionIndex: async () => tokenResolution
    }
  );
  assert.equal(result.signals.length, 0);
  assert.equal(result.diagnostics.excludedNoRecentBuy, 1);
  assert.equal(result.diagnostics.excludedMissingSide, 0);
});
