import fs from "node:fs/promises";
import { discoverWallets, candidatesForPeriod } from "./discovery.js";
import { buildQualifiedLeaderboard } from "./ranking.js";
import { buildConsensusSignals } from "./consensus.js";

const startedAt = new Date().toISOString();
const discovery = await discoverWallets({
  totalLimit: Number(process.env.DISCOVERY_LIMIT ?? 20000),
  maxPagesPerStream: Number(process.env.DISCOVERY_MAX_PAGES ?? 200),
  categories: ["sports", "esports", "crypto", "overall"],
  periods: ["WEEK", "MONTH"],
  sorts: ["PNL", "VOLUME"]
});

const outputs = {};
for (const period of ["WEEK", "MONTH"]) {
  const candidates = candidatesForPeriod(discovery, period);
  const ranking = await buildQualifiedLeaderboard(candidates, {
    period,
    auditLimit: Number(process.env.QUALIFICATION_AUDIT_LIMIT ?? 1000),
    concurrency: Number(process.env.AUDIT_CONCURRENCY ?? 6),
    windowDays: 30,
    rules: { minimumWinRate: 90, minimumResolvedMarkets: 20, windowDays: 30 }
  });
  outputs[period] = ranking;
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
  const consensus = consensusResult.signals.map(signal => ({
    ...signal,
    signalTier: signal.qualifiedWalletCount >= 8 ? "Strong Signals" : "Watchlist",
    marketMetadataVerified: true,
    marketClosed: false,
    sameSide: signal.qualifiedWalletCount,
    totalEntry: signal.totalEntryUsd,
    traders: signal.traders.map(trader => {
      const source = leaders.find(row => row.wallet.toLowerCase() === trader.wallet.toLowerCase());
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
  const payload = {
    generatedAt: new Date().toISOString(),
    period: period.toLowerCase(),
    leaders,
    discovery: discovery.diagnostics,
    qualification: ranking,
    scannerVersion: "v2"
  };
  const consensusPayload = {
    generatedAt: payload.generatedAt,
    period: period.toLowerCase(),
    minimumResolvedMarkets: 20,
    minimumWinRate: 90,
    entryWindowMinutes: 60,
    minimumEntryUsd: 10,
    consensus,
    diagnostics: consensusResult.diagnostics,
    scannerVersion: "v2"
  };
  const prefix = period.toLowerCase();
  await fs.writeFile(`${prefix}.json`, JSON.stringify(payload, null, 2) + "\n");
  await fs.writeFile(`consensus-${prefix}.json`, JSON.stringify(consensusPayload, null, 2) + "\n");
}

const finishedAt = new Date().toISOString();
const status = {
  startedAt, finishedAt, ok: true, errors: [],
  scannerVersion: "v2",
  discoveredWallets: discovery.candidates.length,
  discovery: discovery.diagnostics,
  weekLeaders: outputs.WEEK.qualified.length,
  monthLeaders: outputs.MONTH.qualified.length,
  weekCandidatesAudited: outputs.WEEK.candidatesAudited,
  monthCandidatesAudited: outputs.MONTH.candidatesAudited,
  weekQualification: outputs.WEEK.diagnostics,
  monthQualification: outputs.MONTH.diagnostics
};
await fs.writeFile("status.json", JSON.stringify(status, null, 2) + "\n");
