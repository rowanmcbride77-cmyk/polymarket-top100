/**
 * Settlement-only record primitives for the clean scanner engine.
 *
 * Important: CLOSED positions are exits, not proof of market settlement.
 * A result is countable only when the token's resolution metadata explicitly
 * says it resolved and supplies an exact binary final payout (0 or 1).
 */

export const DEFAULT_QUALIFICATION = Object.freeze({
  minimumWinRate: 90,
  minimumResolvedMarkets: 10,
  windowDays: 30
});

const numberOrNull = value => {
  if (value === null || value === undefined || value === "") return null;
  const valueAsNumber = Number(value);
  return Number.isFinite(valueAsNumber) ? valueAsNumber : null;
};

const keyOf = value => String(value ?? "").trim().toLowerCase();

export function classifySettledPosition(position, tokenResolution) {
  if (!position || !tokenResolution) return { status: "unverified", reason: "missing-position-or-resolution" };

  // A closed position may have been sold at any point. Its realized P&L is
  // deliberately never used as evidence of a settlement result.
  if (String(position.status ?? "").toUpperCase() === "HEDGED_AT_RESOLUTION") {
    return { status: "excluded", reason: "hedged-at-resolution" };
  }
  if (String(position.status ?? "").toUpperCase() === "CLOSED") {
    return { status: "excluded", reason: "closed-position-may-be-early-exit" };
  }

  if (tokenResolution.resolved !== true) {
    return { status: "pending", reason: "token-not-resolved" };
  }

  const payout = numberOrNull(tokenResolution.finalPrice);
  if (payout === null) return { status: "unverified", reason: "missing-final-payout" };
  if (payout !== 0 && payout !== 1) {
    return { status: "unverified", reason: "non-binary-payout-requires-separate-policy" };
  }

  const positionCondition = keyOf(position.condition_id ?? position.conditionId);
  const tokenCondition = keyOf(tokenResolution.conditionId ?? tokenResolution.condition_id);
  const positionToken = keyOf(position.token_id ?? position.tokenId ?? position.asset);
  const resolutionToken = keyOf(tokenResolution.tokenId ?? tokenResolution.token_id);

  if (!positionCondition || !tokenCondition || positionCondition !== tokenCondition) {
    return { status: "unverified", reason: "condition-mismatch" };
  }
  if (!positionToken || !resolutionToken || positionToken !== resolutionToken) {
    return { status: "unverified", reason: "token-mismatch" };
  }

  return {
    status: "settled",
    result: payout === 1 ? "win" : "loss",
    conditionId: positionCondition,
    tokenId: positionToken,
    resolvedAt: tokenResolution.resolvedAt ?? tokenResolution.resolved_at ?? null,
    finalPrice: payout
  };
}

/**
 * Produces one result per condition. If the wallet has conflicting outcome
 * tokens for one condition, the condition is excluded rather than guessed.
 */
export function buildSettlementRecord(positions, resolutions, options = {}) {
  const nowMs = Number(options.nowMs ?? Date.now());
  const windowDays = Number(options.windowDays ?? DEFAULT_QUALIFICATION.windowDays);
  const cutoffMs = nowMs - windowDays * 24 * 60 * 60 * 1000;
  const resolutionByToken = new Map();

  for (const resolution of resolutions ?? []) {
    const tokenId = keyOf(resolution.tokenId ?? resolution.token_id);
    if (tokenId) resolutionByToken.set(tokenId, resolution);
  }

  const byCondition = new Map();
  const diagnostics = {
    positionsSeen: (positions ?? []).length,
    closedExcluded: 0,
    pendingExcluded: 0,
    unverifiedExcluded: 0,
    outsideWindowExcluded: 0,
    conflictingConditionsExcluded: 0
  };

  for (const position of positions ?? []) {
    const tokenId = keyOf(position.token_id ?? position.tokenId ?? position.asset);
    const resolution = resolutionByToken.get(tokenId);
    const classified = classifySettledPosition(position, resolution);

    if (classified.status === "excluded") { diagnostics.closedExcluded++; continue; }
    if (classified.status === "pending") { diagnostics.pendingExcluded++; continue; }
    if (classified.status !== "settled") { diagnostics.unverifiedExcluded++; continue; }

    const resolvedAt = classified.resolvedAt ? Date.parse(classified.resolvedAt) : NaN;
    if (!Number.isFinite(resolvedAt)) { diagnostics.unverifiedExcluded++; continue; }
    if (resolvedAt < cutoffMs || resolvedAt > nowMs) { diagnostics.outsideWindowExcluded++; continue; }

    const prior = byCondition.get(classified.conditionId);
    if (!prior) {
      byCondition.set(classified.conditionId, classified);
    } else if (prior.result !== classified.result || prior.tokenId !== classified.tokenId) {
      byCondition.set(classified.conditionId, { status: "conflict" });
    }
  }

  let wins = 0;
  let losses = 0;
  for (const result of byCondition.values()) {
    if (result.status === "conflict") { diagnostics.conflictingConditionsExcluded++; continue; }
    if (result.result === "win") wins++;
    if (result.result === "loss") losses++;
  }

  const resolvedMarkets = wins + losses;
  const winRate = resolvedMarkets ? wins / resolvedMarkets * 100 : null;
  return {
    wins,
    losses,
    resolvedMarkets,
    winRate,
    windowDays,
    cutoff: new Date(cutoffMs).toISOString(),
    complete: options.historyComplete === true && diagnostics.unverifiedExcluded === 0,
    basis: "Verified token settlement payout only; CLOSED/early-exited positions excluded; one unambiguous result per condition.",
    diagnostics
  };
}

export function qualifiesForTop100(record, rules = DEFAULT_QUALIFICATION) {
  return Boolean(
    record &&
    record.complete === true &&
    Number.isFinite(record.winRate) &&
    record.winRate >= rules.minimumWinRate &&
    Number.isInteger(record.resolvedMarkets) &&
    record.resolvedMarkets >= rules.minimumResolvedMarkets
  );
}
