import fs from "node:fs/promises";
import { discoverWallets, candidatesForPeriod } from "./discovery.js";
import { buildQualifiedLeaderboard } from "./ranking.js";
import { buildConsensusSignals } from "./consensus.js";
import { getWalletSettlementRecord } from "./history.js";

const startedAt = new Date().toISOString();
const discoveryLimit = Number(process.env.DISCOVERY_LIMIT ?? 20000);
const auditLimit = Number(process.env.QUALIFICATION_AUDIT_LIMIT ?? 1000);
const auditConcurrency = Number(process.env.AUDIT_CONCURRENCY ?? 4);

if (!Number.isInteger(discoveryLimit) || discoveryLimit < 100 || discoveryLimit > 20000) {
  throw new Error("DISCOVERY_LIMIT must be an integer between 100 and 20000.");
}
if (!Number.isInteger(auditLimit) || auditLimit < 100 || auditLimit > 2000) {
  throw new Error("QUALIFICATION_AUDIT_LIMIT must be an integer between 100 and 2000.");
}
if (!Number.isInteger(auditConcurrency) || auditConcurrency < 1 || auditConcurrency > 12) {
  throw new Error("AUDIT_CONCURRENCY must be an integer between 1 and 12.");
}

const discovery = await discoverWallets({
  totalLimit: discoveryLimit,
  maxPagesPerStream: Number(process.env.DISCOVERY_MAX_PAGES ?? 1000),
  categories: ["sports", "esports", "crypto", "overall"],
  periods: ["WEEK", "MONTH"],
  sorts: ["PNL", "VOLUME"]
});

// WEEK and MONTH often contain the same wallets. Reuse each wallet's exact
// settlement audit across both leaderboards instead of fetching its full
// history twice. Cache promises so concurrent period jobs share in-flight work.
const settlementCache = new Map();
async function getSharedSettlementRecord(wallet, options) {
  const key = String(wallet).trim().toLowerCase();
  if (!settlementCache.has(key)) {
    settlementCache.set(key, getWalletSettlementRecord(wallet, options));
  }
  return settlementCache.get(key);
}

const qualificationOptions = period => ({
  period,
  auditLimit,
  concurrency: auditConcurrency,
  windowDays: 30,
  getRecord: getSharedSettlementRecord,
  rules: { minimumWinRate: 90, minimumResolvedMarkets: 20, windowDays: 30 }
});

const [weekRanking, monthRanking] = await Promise.all([
  buildQualifiedLeaderboard(candidatesForPeriod(discovery, "WEEK"), qualificationOptions("WEEK")),
  buildQualifiedLeaderboard(candidatesForPeriod(discovery, "MONTH"), qualificationOptions("MONTH"))
]);
const rankings = { WEEK: weekRanking, MONTH: monthRanking };
const outputs = {};

for (const period of ["WEEK", "MONTH"]) {
  const ranking = rankings[period];
  const leaders = ranking.qualified.map(trader => ({
    ...trader,
    pnl: trader.selectedPeriodPnl,
    qualified: true,
    recordComplete: true,
    settlementRecordComplete: true,
    settlementWins: trader.wins,
    settlementLosses: trader.losses,
    recordBasis: "Settlement-only: positions exited before actual market resolution are excluded from wins, losses, win rates, and qualification. Incomplete settlement histories are withheld."
  }));

  const consensusResult = await buildConsensusSignals(leaders, {
    horizonHours: 24,
    entryWindowMinutes: 60,
    minimumEntryUsd: 10,
    minimumHolders: 2,
    strongHolders: 8
  });
  const leadersByWallet = new Map(leaders.map(row => [row.wallet.toLowerCase(), row]));
  const consensus = consensusResult.signals.map(signal => ({
    ...signal,
    signalTier: signal.qualifiedWalletCount >= 8 ? "Strong Signals" : "Watchlist",
    marketMetadataVerified: true,
    marketClosed: false,
    sameSide: signal.qualifiedWalletCount,
    totalEntry: signal.totalEntryUsd,
    traders: signal.traders.map(trader => {
      const source = leadersByWallet.get(trader.wallet.toLowerCase());
      return {
        ...trader,
        qualified: true,
        recordComplete: true,
        settlementRecordComplete: true,
        wins: source?.wins ?? null,
        losses: source?.losses ?? null,
        settlementWins: source?.wins ?? null,
        settlementLosses: source?.losses ?? null,
        resolvedMarkets: source?.resolvedMarkets ?? null,
        winRate: source?.winRate ?? null,
        recordBasis: source?.recordBasis ?? "Settlement-only; early exits excluded.",
        side: signal.outcome
      };
    })
  }));

  outputs[period] = {
    period: period.toLowerCase(),
    leaders,
    discovery: discovery.diagnostics,
    qualification: ranking,
    consensus,
    consensusDiagnostics: consensusResult.diagnostics
  };
}

const finishedAt = new Date().toISOString();
for (const period of ["WEEK", "MONTH"]) {
  const output = outputs[period];
  const prefix = period.toLowerCase();
  const base = {
    generatedAt: finishedAt,
    period: prefix,
    leaders: output.leaders,
    discovery: output.discovery,
    qualification: output.qualification,
    scannerVersion: "v3"
  };
  const consensus = {
    generatedAt: finishedAt,
    period: prefix,
    minimumResolvedMarkets: 20,
    minimumWinRate: 90,
    entryWindowMinutes: 60,
    minimumEntryUsd: 10,
    consensus: output.consensus,
    diagnostics: output.consensusDiagnostics,
    scannerVersion: "v3"
  };
  await fs.writeFile(${JSON.stringify("week.json")}.replace("week", prefix), JSON.stringify(base, null, 2) + "\n");
  await fs.writeFile(`consensus-${prefix}.json`, JSON.stringify(consensus, null, 2) + "\n");
}

const status = {
  startedAt,
  finishedAt,
  generatedAt: finishedAt,
  ok: true,
  errors: [],
  scannerVersion: "v3",
  discoveredWallets: discovery.candidates.length,
  discovery: discovery.diagnostics,
  uniqueSettlementAudits: settlementCache.size,
  weekLeaders: outputs.WEEK.leaders.length,
  monthLeaders: outputs.MONTH.leaders.length,
  weekConsensusSignals: outputs.WEEK.consensus.length,
  monthConsensusSignals: outputs.MONTH.consensus.length,
  weekCandidatesAudited: weekRanking.candidatesAudited,
  monthCandidatesAudited: monthRanking.candidatesAudited,
  weekQualification: weekRanking.diagnostics,
  monthQualification: monthRanking.diagnostics
};
await fs.writeFile("status.json", JSON.stringify(status, null, 2) + "\n");
