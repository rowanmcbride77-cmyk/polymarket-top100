import test from "node:test";
import assert from "node:assert/strict";
import { fetchAllPages, requestJson } from "./data-api.js";

test("follows opaque cursors and preserves every page filter", async () => {
  const calls = [];
  const pages = [
    { data: [{ id: 1 }], pagination: { has_more: true, next_cursor: "opaque-A" } },
    { data: [{ id: 2 }], pagination: { has_more: false, next_cursor: null } }
  ];
  const rows = await fetchAllPages("/v2/activity", { user: "0xabc", type: "REDEEM" }, {
    request: async (path, params) => { calls.push({ path, params }); return pages.shift(); }
  });
  assert.deepEqual(rows.map(row => row.id), [1, 2]);
  assert.equal(calls[1].params.user, "0xabc");
  assert.equal(calls[1].params.type, "REDEEM");
  assert.equal(calls[1].params.cursor, "opaque-A");
});

test("refuses to call a truncated cursor walk complete", async () => {
  await assert.rejects(
    fetchAllPages("/v2/activity", {}, {
      request: async () => ({ data: [{ id: 1 }], pagination: { has_more: true, next_cursor: null } })
    }),
    /has_more without next_cursor/
  );
});

test("rejects repeated cursors instead of looping", async () => {
  let call = 0;
  await assert.rejects(
    fetchAllPages("/v2/activity", {}, {
      request: async () => {
        call++;
        return { data: [{ id: call }], pagination: { has_more: true, next_cursor: "same" } };
      }
    }),
    /Repeated cursor/
  );
});

test("retries rate limits using Retry-After", async () => {
  let calls = 0;
  const delays = [];
  const fetchImpl = async () => {
    calls++;
    if (calls === 1) return {
      status: 429, ok: false, headers: { get: () => "0.25" },
      text: async () => "rate limited"
    };
    return { status: 200, ok: true, json: async () => ({ data: [{ ok: true }] }) };
  };
  const result = await requestJson("/v2/tokens", {}, {
    fetchImpl, sleepImpl: async ms => delays.push(ms), attempts: 2
  });
  assert.equal(result.data[0].ok, true);
  assert.deepEqual(delays, [250]);
  assert.equal(calls, 2);
});
