import test from "node:test";
import assert from "node:assert/strict";
import { candidatesForPeriod, discoverWallets } from "./discovery.js";
import { fetchAllPages as paginate } from "./data-api.js";

const wallet = n => "0x" + String(n).padStart(40, "0");

test("deduplicates wallets across categories and preserves period-specific P&L", async () => {
  const request = async (_path, params) => {
    const pnl = params.time_period === "week" ? 12 : 120;
    return [
      { proxyWallet: wallet(1), userName: "Trader", pnl, volume_usdc: 500, rank: 1 },
      { proxy_wallet: wallet(2), pnl: pnl - 1, volume_usdc: 300, rank: 2 }
    ];
  };
  const result = await discoverWallets({
    fetchAllPages: request,
    categories: ["sports", "crypto"],
    periods: ["WEEK", "MONTH"],
    sorts: ["PNL"],
    totalLimit: 10,
    categoryQuotas: { sports: 10, crypto: 10 }
  });
  assert.equal(result.candidates.length, 2);
  assert.equal(result.diagnostics.uniqueWallets, 2);
  assert.equal(result.candidates[0].pnlByPeriod.WEEK, 12);
  assert.equal(result.candidates[0].pnlByPeriod.MONTH, 120);
  assert.equal(candidatesForPeriod(result, "WEEK")[0].pnl, 12);
  assert.equal(candidatesForPeriod(result, "MONTH")[0].pnl, 120);
});

test("stops a leaderboard cursor walk when the category's unique-wallet quota is met", async () => {
  let calls = 0;
  const pages = [
    {
      data: [
        { proxyWallet: wallet(11), pnl: 100 },
        { proxyWallet: wallet(12), pnl: 90 }
      ],
      pagination: { has_more: true, next_cursor: "next-page" }
    },
    {
      data: [{ proxyWallet: wallet(13), pnl: 80 }],
      pagination: { has_more: false, next_cursor: null }
    }
  ];
  const result = await discoverWallets({
    fetchAllPages: (path, params, options) => paginate(path, params, {
      ...options,
      request: async () => {
        calls++;
        return pages.shift();
      }
    }),
    categories: ["sports"], periods: ["WEEK"], sorts: ["PNL"],
    totalLimit: 2, categoryQuotas: { sports: 2 }, pageSize: 2
  });
  assert.equal(result.candidates.length, 2);
  assert.equal(calls, 1);
});

test("rejects malformed wallet identifiers and zero address", async () => {
  const result = await discoverWallets({
    fetchAllPages: async () => [
      { proxyWallet: "not-a-wallet", pnl: 100 },
      { proxyWallet: "0x0000000000000000000000000000000000000000", pnl: 99 },
      { proxyWallet: wallet(3), pnl: 98 }
    ],
    categories: ["sports"], periods: ["WEEK"], sorts: ["PNL"],
    totalLimit: 10, categoryQuotas: { sports: 10 }
  });
  assert.equal(result.candidates.length, 1);
  assert.equal(result.diagnostics.invalidWalletRows, 2);
});

test("only candidates with selected-period P&L enter that period ranking", async () => {
  const result = await discoverWallets({
    fetchAllPages: async (_path, params) => params.time_period === "week"
      ? [{ proxyWallet: wallet(4), pnl: 10 }]
      : [{ proxyWallet: wallet(5), pnl: 20 }],
    categories: ["sports"], periods: ["WEEK", "MONTH"], sorts: ["PNL"],
    totalLimit: 10, categoryQuotas: { sports: 10 }
  });
  assert.deepEqual(candidatesForPeriod(result, "WEEK").map(x => x.wallet), [wallet(4)]);
  assert.deepEqual(candidatesForPeriod(result, "MONTH").map(x => x.wallet), [wallet(5)]);
});


test("splits the wallet budget across weekly and monthly streams", async () => {
  const calls = [];
  const result = await discoverWallets({
    fetchAllPages: async (_path, params) => {
      calls.push(params.time_period);
      const id = params.time_period === "week" ? 21 : 22;
      return [{ proxyWallet: wallet(id), pnl: id, rank: 1 }];
    },
    categories: ["sports"], periods: ["WEEK", "MONTH"], sorts: ["PNL"],
    totalLimit: 2, categoryQuotas: { sports: 2 }
  });
  assert.deepEqual(calls, ["week", "month"]);
  assert.deepEqual(candidatesForPeriod(result, "WEEK").map(row => row.wallet), [wallet(21)]);
  assert.deepEqual(candidatesForPeriod(result, "MONTH").map(row => row.wallet), [wallet(22)]);
});
