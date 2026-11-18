import { fetchAllPages } from "./data-api.js";
import { fetchTokenResolutionIndex } from "./resolution-data.js";
import { buildSettlementRecord } from "./settlement.js";

const SUPPORTED_TYPES = "TRADE,SPLIT,MERGE,REDEEM";
const key = value => String(value ?? "").trim().toLowerCase();
const num = value => {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};
const timestampMs = value => {
  const n = num(value);
  if (n !== null) return n < 100_000_000_000 ? n * 1000 : n;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};

/**
 * Reconstructs token balances immediately before each market's resolution.
 * Only explicit TRADE/SPLIT/MERGE/REDEEM events are supported. Unknown or
 * malformed events make the history incomplete instead of silently creating
 * an inflated accuracy record.
 */
export function reconstructHeldAtResolution(activity, resolutionIndex, options = {}) {
  const resolutions = [...(resolutionIndex?.values?.() ?? [])];
  const resolutionByCondition = new Map();
  const tokenToCondition = new Map();

  for (const resolution of resolutions) {
    const conditionId = key(resolution.conditionId);
    const tokenId = key(resolution.tokenId);
    const resolvedAtMs = timestampMs(resolution.resolvedAt);
    if (!conditionId || !tokenId) continue;
    tokenToCondition.set(tokenId, conditionId);
    if (resolution.resolved === true && (resolution.finalPrice === 0 || resolution.finalPrice === 1) &&
        resolvedAtMs !== null) {
      if (!resolutionByCondition.has(conditionId)) resolutionByCondition.set(conditionId, {
        resolvedAtMs,
        tokens: []
      });
      resolutionByCondition.get(conditionId).tokens.push(resolution);
    }
  }

  const events = [];
  let complete = options.historyComplete === true;
  const diagnostics = {
    activityRows: (activity ?? []).length,
    unsupportedEvents: 0,
    malformedEvents: 0,
    comboEvents: 0,
    ignoredAfterResolution: 0
  };

  for (const row of activity ?? []) {
    const type = String(row.type ?? "").toUpperCase();
    if (!["TRADE", "SPLIT", "MERGE", "REDEEM"].includes(type)) {
      diagnostics.unsupportedEvents++;
      complete = false;
      continue;
    }
    if (row.is_combo === true) {
      diagnostics.comboEvents++;
      complete = false;
      continue;
    }
    const tokenId = key(row.token_id ?? row.tokenId);
    const conditionId = key(row.condition_id ?? row.conditionId);
    const at = timestampMs(row.timestamp ?? row.block_timestamp);
    const size = num(row.size);
    if (!tokenId || !conditionId || at === null || size === null || size < 0) {
      diagnostics.malformedEvents++;
      complete = false;
      continue;
    }
    const side = String(row.side ?? "").toUpperCase();
    let delta;
    if (type === "TRADE") {
      if (side === "BUY") delta = size;
      else if (side === "SELL") delta = -size;
      else {
        diagnostics.malformedEvents++;
        complete = false;
        continue;
      }
    } else if (type === "SPLIT") {
      delta = size;
    } else if (type === "MERGE" || type === "REDEEM") {
      delta = -size;
    }
    events.push({ tokenId, conditionId, at, delta, type });
  }

  events.sort((a, b) => a.at - b.at);
  const results = [];
  const balances = new Map();
  let eventIndex = 0;

  const targetConditions = [...resolutionByCondition.entries()]
    .sort((a, b) => a[1].resolvedAtMs - b[1].resolvedAtMs);

  for (const [conditionId, resolution] of targetConditions) {
    while (eventIndex < events.length && events[eventIndex].at < resolution.resolvedAtMs) {
      const event = events[eventIndex++];
      const balanceKey = event.tokenId;
      const next = (balances.get(balanceKey) ?? 0) + event.delta;
      if (next < -0.000001) {
        complete = false;
        diagnostics.malformedEvents++;
      }
      balances.set(balanceKey, Math.max(0, next));
    }

    const heldTokens = resolution.tokens.filter(token => (balances.get(key(token.tokenId)) ?? 0) > 0.000001);
    if (heldTokens.length === 1) {
      const token = heldTokens[0];
      results.push({
        condition_id: conditionId,
        token_id: token.tokenId,
        status: "HELD_AT_RESOLUTION"
      });
    } else if (heldTokens.length > 1) {
      // Holding both sides at settlement is a hedge, not a clean directional call.
      results.push({ condition_id: conditionId, status: "HEDGED_AT_RESOLUTION" });
    }
  }

  return { positions: results, complete, diagnostics };
}

/**
 * Complete all-time activity is required to reconstruct pre-window inventory:
 * a token bought before the 30-day scoring window may still be held at a
 * resolution that occurred inside the window.
 */
export async function getWalletSettlementRecord(wallet, options = {}) {
  const nowMs = Number(options.nowMs ?? Date.now());
  const nowSeconds = Math.floor(nowMs / 1000);
  const activityFetcher = options.fetchAllPages ?? fetchAllPages;
  const activity = await activityFetcher("/v2/activity", {
    user: wallet,
    type: SUPPORTED_TYPES,
    start: 1,
    end: nowSeconds,
    sort_by: "TIMESTAMP",
    sort_direction: "ASC",
    limit: 1000
  }, { maxPages: options.maxActivityPages ?? 5000 });

  const conditionIds = [...new Set(activity.map(row => key(row.condition_id ?? row.conditionId)).filter(Boolean))];
  const resolutionIndex = await (options.fetchResolutionIndex ?? fetchTokenResolutionIndex)(
    conditionIds,
    options
  );

  const reconstructed = reconstructHeldAtResolution(activity, resolutionIndex, { historyComplete: true });
  const record = buildSettlementRecord(
    reconstructed.positions,
    [...resolutionIndex.values()],
    {
      nowMs,
      windowDays: options.windowDays ?? 30,
      historyComplete: reconstructed.complete
    }
  );

  return {
    ...record,
    historySource: "Complete Data API v2 activity replay + token finalPrice + condition resolvedAt",
    diagnostics: { ...record.diagnostics, ...reconstructed.diagnostics, activityConditions: conditionIds.length }
  };
}
