import { requestJson } from "./data-api.js";

const chunks = (items, size) => {
  const result = [];
  for (let i = 0; i < items.length; i += size) result.push(items.slice(i, i + size));
  return result;
};

const key = value => String(value ?? "").trim().toLowerCase();

async function mapLimit(items, limit, worker) {
  const output = new Array(items.length);
  let next = 0;
  async function run() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await worker(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return output;
}

/**
 * Join Polymarket's token payout facts to condition-level finality.
 * /v2/tokens proves the outcome token's 0/1 payout; /v2/resolutions proves
 * the condition reached final resolution and supplies the actual resolvedAt.
 */
export async function fetchTokenResolutionIndex(conditionIds, options = {}) {
  const request = options.request ?? requestJson;
  const uniqueConditions = [...new Set((conditionIds ?? []).map(key).filter(Boolean))];
  const uniqueTokenIds = [...new Set((options.tokenIds ?? []).map(key).filter(Boolean))];
  const tokenRows = [];
  const resolutionRows = [];

  const concurrency = options.resolutionBatchConcurrency ?? 4;
  // We only need payout facts for tokens actually held by these wallets.
  // /v2/tokens accepts up to 50 token_id values per request, versus 10 conditions
  // when expanding every token in each market.
  if (uniqueTokenIds.length) {
    await mapLimit(chunks(uniqueTokenIds, 50), concurrency, async batch => {
      const payload = await request("/v2/tokens", { token_id: batch.join(",") }, options);
      if (!Array.isArray(payload.data)) throw new Error("Expected token data[] from /v2/tokens");
      for (const row of payload.data) tokenRows.push(row);
    });
  } else {
    // Backwards-compatible condition expansion for callers without token IDs.
    await mapLimit(chunks(uniqueConditions, 10), concurrency, async batch => {
      const payload = await request("/v2/tokens", { condition: batch.join(",") }, options);
      if (!Array.isArray(payload.data)) throw new Error("Expected token data[] from /v2/tokens");
      for (const row of payload.data) tokenRows.push(row);
    });
  }

  await mapLimit(chunks(uniqueConditions, 20), concurrency, async batch => {
    const payload = await request("/v2/resolutions", { condition: batch.join(",") }, options);
    if (!Array.isArray(payload.data)) throw new Error("Expected resolution data[] from /v2/resolutions");
    for (const row of payload.data) resolutionRows.push(row);
  });

  const finalityByCondition = new Map(
    resolutionRows.map(row => [key(row.conditionId ?? row.condition_id), row])
  );
  const index = new Map();

  for (const token of tokenRows) {
    const tokenId = key(token.tokenId ?? token.token_id);
    const conditionId = key(token.conditionId ?? token.condition_id);
    if (!tokenId || !conditionId) continue;
    const finality = finalityByCondition.get(conditionId);
    index.set(tokenId, {
      tokenId,
      conditionId,
      resolved: token.resolved === true && String(finality?.status ?? "").toLowerCase() === "resolved",
      finalPrice: token.finalPrice ?? token.final_price ?? null,
      resolvedAt: finality?.resolvedAt ?? finality?.resolved_at ?? finality?.lastUpdateTimestamp ?? finality?.last_update_timestamp ?? null,
      resolutionStatus: finality?.status ?? null
    });
  }

  return index;
}
