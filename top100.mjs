import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const OUTPUTS = [
  { period: "week", file: "top100-week.json" },
  { period: "month", file: "top100-month.json" }
];

function rowsOf(payload) {
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload)) return payload;
  return [];
}

async function getJson(url) {
  const response = await fetch(url, {
    headers: { accept: "application/json", "user-agent": "PolymarketTop100/2.0" },
    signal: AbortSignal.timeout(20000)
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Polymarket API ${response.status}: ${body.slice(0, 300)}`);
  }
  return response.json();
}

function walletOf(row) {
  return String(row.proxy_wallet ?? row.proxyWallet ?? row.user ?? row.address ?? "").toLowerCase();
}

async function fetchTop100(period) {
  const leaders = [];
  let cursor = null;
  const seenCursors = new Set();
  const seenWallets = new Set();

  while (leaders.length < 100) {
    const params = new URLSearchParams({
      category: "overall",
      time_period: period,
      sort_by: "PNL",
      limit: "50"
    });
    if (cursor) params.set("cursor", cursor);

    const payload = await getJson(`${API}/v2/leaderboard?${params}`);
    const page = rowsOf(payload);
    if (!page.length) break;

    for (const row of page) {
      const wallet = walletOf(row);
      if (!/^0x[a-f0-9]{40}$/.test(wallet) || seenWallets.has(wallet)) continue;
      const pnl = Number(row.pnl ?? row.profit ?? row.rank_pnl);
      if (!Number.isFinite(pnl)) continue;
      seenWallets.add(wallet);
      leaders.push({
        rank: leaders.length + 1,
        wallet,
        name: row.user_name || row.username || row.name || row.pseudonym || wallet.slice(0, 8) + "…" + wallet.slice(-4),
        pnl,
        volume: Number.isFinite(Number(row.volume_usdc ?? row.volumeUsd)) ? Number(row.volume_usdc ?? row.volumeUsd) : null,
        wins: null,
        losses: null,
        winRate: null,
        resolvedMarkets: null,
        recordVerified: false,
        settlementRecordComplete: false,
        recordBasis: "Settlement-only audit is deliberately not inferred from closed-position P&L. Early exits are not counted as wins or losses."
      });
      if (leaders.length === 100) break;
    }

    const next = payload?.pagination?.next_cursor ?? null;
    if (!next || seenCursors.has(String(next))) break;
    seenCursors.add(String(next));
    cursor = next;
  }

  if (leaders.length !== 100) {
    throw new Error(`${period}: expected 100 unique leaderboard traders; received ${leaders.length}. Refusing to publish a partial Top 100.`);
  }
  return leaders;
}

for (const output of OUTPUTS) {
  console.log(`Fetching canonical ${output.period} P&L leaderboard…`);
  const leaders = await fetchTop100(output.period);
  const document = {
    generatedAt: new Date().toISOString(),
    period: output.period,
    source: "Polymarket Data API v2 /leaderboard (overall, PNL)",
    leaders
  };
  await fs.writeFile(output.file, JSON.stringify(document, null, 2) + "\n");
  console.log(`Wrote ${output.file}: ${leaders.length} unique traders.`);
}

console.log("PASS: both canonical Top 100 leaderboards contain exactly 100 unique wallets.");
