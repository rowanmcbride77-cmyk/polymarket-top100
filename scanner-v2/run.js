import fs from "node:fs/promises";
import { fetchAllPages } from "./data-api.js";

const startedAt = new Date().toISOString();

async function getTop100(period) {
  const rows = await fetchAllPages("/v2/leaderboard", {
    category: "overall",
    time_period: period,
    sort_by: "PNL",
    limit: 50
  }, {
    maxPages: 2,
    stopWhen: (_page, allRows) => allRows.length >= 100
  });

  const seen = new Set();
  return rows
    .map((row) => {
      const wallet = String(row.proxy_wallet ?? row.proxyWallet ?? row.user_id ?? row.userId ?? "").toLowerCase();
      const pnl = Number(row.pnl ?? row.profit);
      const volume = Number(row.volume_usdc ?? row.volumeUsd ?? row.volume_usd ?? row.volume);
      return {
        wallet,
        name: row.user_name ?? row.userName ?? row.username ?? row.name ?? "Polymarket Trader",
        rank: Number(row.rank) || null,
        pnl: Number.isFinite(pnl) ? pnl : null,
        selectedPeriodPnl: Number.isFinite(pnl) ? pnl : null,
        volume: Number.isFinite(volume) ? volume : null,
        period,
        source: "Polymarket public leaderboard"
      };
    })
    .filter((row) => /^0x[a-f0-9]{40}$/.test(row.wallet) && row.pnl !== null && !seen.has(row.wallet) && seen.add(row.wallet))
    .sort((a, b) => b.pnl - a.pnl)
    .slice(0, 100)
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

const [weekLeaders, monthLeaders] = await Promise.all([
  getTop100("week"),
  getTop100("month")
]);

if (!weekLeaders.length || !monthLeaders.length) {
  throw new Error("Polymarket returned an empty Top 100 leaderboard; refusing to publish empty data.");
}

const finishedAt = new Date().toISOString();
for (const [period, leaders] of [["week", weekLeaders], ["month", monthLeaders]]) {
  await fs.writeFile(`${period}.json`, JSON.stringify({
    generatedAt: finishedAt,
    period,
    source: "Polymarket public Data API v2 leaderboard",
    leaders,
    scannerVersion: "simple-top100-v1"
  }, null, 2) + "\\n");

  await fs.writeFile(`consensus-${period}.json`, JSON.stringify({
    generatedAt: finishedAt,
    period,
    source: "Consensus scan paused during simple leaderboard restart",
    consensus: [],
    diagnostics: { signals: 0 },
    scannerVersion: "simple-top100-v1"
  }, null, 2) + "\\n");
}

await fs.writeFile("status.json", JSON.stringify({
  ok: true,
  scannerVersion: "simple-top100-v1",
  startedAt,
  finishedAt,
  generatedAt: finishedAt,
  source: "Polymarket public Data API v2 leaderboard",
  weekLeaders: weekLeaders.length,
  monthLeaders: monthLeaders.length,
  weekConsensusSignals: 0,
  monthConsensusSignals: 0
}, null, 2) + "\\n");

console.log(`Published-ready Polymarket leaderboard data: week=${weekLeaders.length}, month=${monthLeaders.length}.`);
