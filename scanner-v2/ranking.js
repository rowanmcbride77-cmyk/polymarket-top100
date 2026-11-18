import { getWalletSettlementRecord } from "./history.js";
import { qualifiesForTop100, DEFAULT_QUALIFICATION } from "./settlement.js";
import { getSelectedPeriodPnl } from "./pnl.js";

async function mapLimit(items, limit, worker) {
  const output = new Array(items.length);
  let next = 0;
  async function run() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      try { output[index] = await worker(items[index], index); }
      catch (error) { output[index] = { error: String(error?.message ?? error) }; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return output;
}

/**
 * Audits the highest selected-period P&L candidates, then ranks only complete,
 * verified settlement passers. Failed/incomplete wallet audits never qualify.
 */
export async function buildQualifiedLeaderboard(candidates, options = {}) {
  const period = String(options.period ?? "WEEK").toUpperCase();
  const auditLimit = options.auditLimit ?? 1000;
  const concurrency = options.concurrency ?? 6;
  const rules = { ...DEFAULT_QUALIFICATION, ...(options.rules ?? {}) };
  const rankedPool = [...(candidates ?? [])]
    .filter(row => row.wallet && Number.isFinite(Number(row.pnl)))
    .sort((a, b) => Number(b.pnl) - Number(a.pnl))
    .slice(0, auditLimit);

  const audit = await mapLimit(rankedPool, concurrency, async trader => {
    const [record, pnl] = await Promise.all([
      (options.getRecord ?? getWalletSettlementRecord)(trader.wallet, options),
      (options.getPeriodPnl ?? getSelectedPeriodPnl)(trader.wallet, period, options)
    ]);
    return { trader, record, pnl };
  });

  const qualified = [];
  const rejected = [];
  for (let i = 0; i < audit.length; i++) {
    const result = audit[i];
    if (!result || result.error) {
      rejected.push({
        wallet: rankedPool[i].wallet,
        reason: "history-fetch-failed",
        error: result?.error ?? "unknown"
      });
      continue;
    }
    const { trader, record } = result;
    if (qualifiesForTop100(record, rules)) {
      qualified.push({
        ...trader,
        ...record,
        period,
        selectedPeriodPnl: Number(pnl.value),
        periodPnlBasis: pnl.basis,
        periodPnlPoints: pnl.points
      });
    } else {
      rejected.push({
        wallet: trader.wallet,
        reason: !record.complete ? "incomplete-settlement-history" :
          record.resolvedMarkets < rules.minimumResolvedMarkets ? "below-minimum-resolved-markets" :
          record.winRate < rules.minimumWinRate ? "below-minimum-settlement-accuracy" :
          "not-qualified",
        winRate: record.winRate,
        resolvedMarkets: record.resolvedMarkets,
        complete: record.complete
      });
    }
  }

  qualified.sort((a, b) =>
    b.selectedPeriodPnl - a.selectedPeriodPnl ||
    b.winRate - a.winRate ||
    b.resolvedMarkets - a.resolvedMarkets
  );

  return {
    period,
    rules,
    candidatesAudited: rankedPool.length,
    qualifiedCount: qualified.length,
    qualified: qualified.slice(0, 100).map((row, index) => ({ ...row, rank: index + 1 })),
    rejectedCount: rejected.length,
    diagnostics: {
      auditFailures: rejected.filter(row => row.reason === "history-fetch-failed").length,
      missingPeriodPnl: rejected.filter(row => row.reason === "missing-period-pnl").length,
      incompleteHistories: rejected.filter(row => row.reason === "incomplete-settlement-history").length,
      belowAccuracy: rejected.filter(row => row.reason === "below-minimum-settlement-accuracy").length,
      belowMarketMinimum: rejected.filter(row => row.reason === "below-minimum-resolved-markets").length
    }
  };
}
