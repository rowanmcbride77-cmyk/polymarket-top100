import test from "node:test";
import assert from "node:assert/strict";
import { getSelectedPeriodPnl } from "./pnl.js";

test("calculates weekly P&L from cumulative realized P&L points", async () => {
  let paramsSeen;
  const result = await getSelectedPeriodPnl("0xabc", "WEEK", {
    request: async (_path, params) => {
      paramsSeen = params;
      return { data: { fidelity: "1d", points: [
        { timestamp: 100, realized_pnl: 25 },
        { timestamp: 200, realized_pnl: 80 }
      ] } };
    }
  });
  assert.equal(paramsSeen.interval, "1w");
  assert.equal(paramsSeen.fidelity, "1d");
  assert.equal(result.value, 55);
  assert.equal(result.complete, true);
});

test("calculates monthly P&L and refuses missing values", async () => {
  const monthly = await getSelectedPeriodPnl("0xabc", "MONTH", {
    request: async (_path, params) => {
      assert.equal(params.interval, "1m");
      return { data: { points: [
        { timestamp: 100, realized_pnl: -10 },
        { timestamp: 200, realized_pnl: 15 }
      ] } };
    }
  });
  assert.equal(monthly.value, 25);

  const missing = await getSelectedPeriodPnl("0xabc", "MONTH", {
    request: async () => ({ data: { points: [
      { timestamp: 100, realized_pnl: null },
      { timestamp: 200, realized_pnl: 15 }
    ] } })
  });
  assert.equal(missing.complete, false);
  assert.equal(missing.value, null);
});
