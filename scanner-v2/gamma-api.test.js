import test from "node:test";
import assert from "node:assert/strict";
import { fetchGammaMarkets, gammaJson } from "./gamma-api.js";

test("fetches Gamma markets in 100-condition batches", async () => {
  const calls = [];
  const markets = await fetchGammaMarkets(Array.from({ length: 201 }, (_, i) => "c" + i), {
    gammaJson: async (path, params) => { calls.push({ path, params }); return [{ conditionId: params.condition_ids }]; }
  });
  assert.equal(calls.length, 3);
  assert.deepEqual(calls.map(call => call.params.condition_ids.split(",").length), [100, 100, 1]);
  assert.equal(markets.length, 3);
});

test("Gamma request retries 429 and returns JSON", async () => {
  let calls = 0;
  const result = await gammaJson("/markets", {}, {
    attempts: 2,
    fetchImpl: async () => {
      calls++;
      if (calls === 1) return { status: 429, ok: false, headers: { get: () => "0" }, text: async () => "busy" };
      return { status: 200, ok: true, json: async () => [{ id: 1 }] };
    }
  });
  assert.equal(calls, 2);
  assert.equal(result[0].id, 1);
});
