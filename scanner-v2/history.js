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
  const windowDays = options.windowDays ?? 30;
  const cutoffSeconds = Math.floor((nowMs - windowDays * 24 * 60 * 60 * 1000) / 1000);
  const fetchPages = options.fetchAllPages ?? fetchAllPages;

  // Discover only conditions that could have settled in the scoring window:
  // recent redemptions, recent closes, and currently-held redeemable positions.
  const [redeems, recentClosed, redeemable, redeemableLost] = await Promise.all([
    fetchPages("/v2/activity", {
      user: wallet, type: "REDEEM", start: cutoffSeconds, end: nowSeconds,
      sort_by: "TIMESTAMP", sort_direction: "ASC", limit: 1000
    }, { maxPages: options.maxActivityPages ?? 100 }),
    fetchPages("/v2/positions", {
      user: wallet, status: "CLOSED", start: cutoffSeconds, end: nowSeconds,
      sort_by: "TIMESTAMP", sort_direction: "ASC", limit: 1000
    }, { maxPages: options.maxPositionPages ?? 100 }),
    fetchPages("/v2/positions", {
      user: wallet, status: "REDEEMABLE", sort_by: "TIMESTAMP",
      sort_direction: "ASC", limit: 1000
    }, { maxPages: options.maxPositionPages ?? 100 }),
    fetchPages("/v2/positions", {
      user: wallet, status: "REDEEMABLE_LOST", sort_by: "TIMESTAMP",
      sort_direction: "ASC", limit: 1000
    }, { maxPages: options.maxPositionPages ?? 100 })
  ]);

  const candidateConditions = [...new Set([
    ...redeems, ...recentClosed, ...redeemable, ...redeemableLost
  ].map(row => key(row.condition_id ?? row.conditionId)).filter(Boolean))];

  if (!candidateConditions.length) {
    return {
      wins: 0, losses: 0, resolvedMarkets: 0, winRate: null,
      windowDays, complete: true,
      basis: "Verified token settlement payout only; positions reconstructed at resolution; early exits excluded.",
      diagnostics: {
        positionsSeen: 0, closedExcluded: 0, pendingExcluded: 0,
        unverifiedExcluded: 0, outsideWindowExcluded: 0,
        conflictingConditionsExcluded: 0, activityRows: 0,
        unsupportedEvents: 0, malformedEvents: 0, comboEvents: 0,
        ignoredAfterResolution: 0, candidateConditions: 0
      }
    };
  }

  // A wallet may have bought a token months before settlement. Fetch the
  // complete event history only for candidate conditions, not every market
  // the wallet has ever touched.
  const conditionActivity = [];
  for (let i = 0; i < candidateConditions.length; i += 20) {
    const batch = candidateConditions.slice(i, i + 20);
    const rows = await fetchPages("/v2/activity", {
      user: wallet,
      condition: batch.join(","),
      type: SUPPORTED_TYPES,
      start: 1,
      end: nowSeconds,
      sort_by: "TIMESTAMP",
      sort_direction: "ASC",
      limit: 1000
    }, { maxPages: options.maxConditionActivityPages ?? 200 });
    conditionActivity.push(...rows);
  }

  const resolutionIndex = await (options.fetchResolutionIndex ?? fetchTokenResolutionIndex)(
    candidateConditions,
    options
  );
  const reconstructed = reconstructHeldAtResolution(
    conditionActivity,
    resolutionIndex,
    { historyComplete: true }
  );
  const record = buildSettlementRecord(
    reconstructed.positions,
    [...resolutionIndex.values()],
    { nowMs, windowDays, historyComplete: reconstructed.complete }
  );

  return {
    ...record,
    historySource: "Candidate-condition activity replay + verified token payout + condition resolution timestamp",
    diagnostics: {
      ...record.diagnostics,
      ...reconstructed.diagnostics,
      candidateConditions: candidateConditions.length,
      discoveryRedeems: redeems.length,
      discoveryRecentClosed: recentClosed.length,
      discoveryRedeemable: redeemable.length,
      discoveryRedeemableLost: redeemableLost.length,
      conditionActivityRows: conditionActivity.length
    }
  };
}
