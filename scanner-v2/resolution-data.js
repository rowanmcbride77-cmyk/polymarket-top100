import { requestJson } from "./data-api.js";

const chunks = (items, size) => {
  const result = [];
  for (let i = 0; i < items.length; i += size) result.push(items.slice(i, i + size));
  return result;
};

const key = value => String(value ?? "").trim().toLowerCase();

/**
 * Join Polymarket's token payout facts to condition-level finality.
 * /v2/tokens proves the outcome token's 0/1 payout; /v2/resolutions proves
 * the condition reached final resolution and supplies the actual resolvedAt.
 */
export async function fetchTokenResolutionIndex(conditionIds, options = {}) {
  const request = options.request ?? requestJson;
  const uniqueConditions = [...new Set((conditionIds ?? []).map(key).filter(Boolean))];
  const tokenRows = [];
  const resolutionRows = [];

  for (const batch of chunks(uniqueConditions, 10)) {
    const payload = await request("/v2/tokens", { condition: batch.join(",") }, options);
    if (!Array.isArray(payload.data)) throw new Error("Expected token data[] from /v2/tokens");
    tokenRows.push(...payload.data);
  }

  for (const batch of chunks(uniqueConditions, 20)) {
    const payload = await request("/v2/resolutions", { condition: batch.join(",") }, options);
    if (!Array.isArray(payload.data)) throw new Error("Expected resolution data[] from /v2/resolutions");
    resolutionRows.push(...payload.data);
  }

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
      resolvedAt: finality?.resolvedAt ?? finality?.resolved_at ?? null,
      resolutionStatus: finality?.status ?? null
    });
  }

  return index;
}
