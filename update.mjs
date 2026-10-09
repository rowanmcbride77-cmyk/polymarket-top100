
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "week" },
  { key: "month", api: "month" }
];

const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const TRADE_LIMIT = 50;
const MAX_CONCURRENCY = 5;
const REQUEST_DELAY_MS = 150;
const MIN_RESOLVED_FOR_RANKING = 10;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketTop100/1.0"
        },
        signal: AbortSignal.timeout(25000)
      });

      if (response.status === 429 || response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after"));
        await sleep(
          Number.isFinite(retryAfter) && retryAfter > 0
            ? Math.min(retryAfter * 1000, 15000)
            : 1000 * (attempt + 1)
        );
        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 300)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await sleep(500 * (attempt + 1));
      }
    }
  }

  throw lastError || new Error("Request failed");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  return [];
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
    row?.user_id ??
    row?.address ??
    row?.wallet ??
    ""
  ).toLowerCase();
}

function normalizeLeader(row, fallbackRank) {
  const wallet = walletOf(row);

  return {
    rank: numberOrNull(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: numberOrNull(row.pnl),
    volume: numberOrNull(row.vol ?? row.volume),
    // Never infer a record from volume, trade counts, or P&L.
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getLeaderboard(period) {
  const combined = [];
  const seen = new Set();

  // The v1 leaderboard supports limit 50 and offset pagination.
  // Use documented uppercase timePeriod values for v1.
  for (let offset = 0; offset < LEADER_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api.toUpperCase(),
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const rows = asArray(
      await getJson(`${API}/v1/leaderboard?${params.toString()}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const leader = normalizeLeader(row, combined.length + 1);
      if (!leader.wallet || seen.has(leader.wallet)) continue;
      seen.add(leader.wallet);
      combined.push(leader);
    }

    if (rows.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return combined.slice(0, LEADER_COUNT).map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT)
  });

  try {
    const response = await getJson(`${API}/trades?${params.toString()}`);
    const rows = asArray(response);

    return rows
      .filter(row => String(row.side || "").toUpperCase() === "BUY")
      .map(row => ({
        title:
          row.title ||
          row.question ||
          row.eventSlug ||
          "Unknown market",
        side: "BUY",
        outcome: row.outcome || row.outcome_name || "Unknown",
        amount: numberOrNull(
          row.usdcSize ?? row.amount ?? row.size
        ),
        buyTime: numberOrNull(
          row.timestamp ?? row.createdAt ?? row.time
        ),
        conditionId: row.conditionId || row.condition_id || null,
        transactionHash: row.transactionHash || row.transaction_hash || null
      }))
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    console.error(`Recent buys failed for ${wallet}: ${error.message}`);
    return [];
  }
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;
      results[index] = await callback(items[index], index);
      await sleep(REQUEST_DELAY_MS);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(limit, items.length) },
      () => worker()
    )
  );

  return results;
}

function buildConsensus(leaders, traderActiveTrades) {
  const markets = new Map();

  for (const leader of leaders) {
    const trades = traderActiveTrades[leader.wallet] || [];

    for (const trade of trades) {
      const condition = String(trade.conditionId || "").toLowerCase();
      const title = String(trade.title || "Unknown market").trim();
      const side = String(trade.outcome || "Unknown").trim();

      // Without a condition ID, titles alone can accidentally combine
      // unrelated markets. Skip them rather than inventing a match.
      if (!condition || side === "Unknown") continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title,
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          oldestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }

      const market = markets.get(key);
      if (market.walletsSeen.has(leader.wallet)) continue;

      market.walletsSeen.add(leader.wallet);
      market.sameSide++;
      market.traders.push({
        name: leader.name,
        wallet: leader.wallet,
        rank: leader.rank,
        wins: leader.wins,
        losses: leader.losses,
        winRate: leader.winRate,
        pnl: leader.pnl,
        volume: leader.volume,
        recordVerified: leader.recordVerified,
        side,
        amount: numberOrNull(trade.amount),
        buyTime: numberOrNull(trade.buyTime)
      });

      const amount = numberOrNull(trade.amount);
      if (amount !== null) market.totalEntry += amount;

      const time = numberOrNull(trade.buyTime);
      if (time !== null) {
        market.newestBuy =
          market.newestBuy === null
            ? time
            : Math.max(market.newestBuy, time);

        market.oldestBuy =
          market.oldestBuy === null
            ? time
            : Math.min(market.oldestBuy, time);
      }
    }
  }

  return [...markets.values()]
    .filter(market => market.sameSide >= 2)
    .map(market => {
      const { walletsSeen, ...result } = market;
      return result;
    })
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

function sortByVerifiedRecord(leaders) {
  const qualified = [];
  const unqualified = [];

  for (const leader of leaders) {
    const wins = numberOrNull(leader.wins);
    const losses = numberOrNull(leader.losses);
    const total = numberOrNull(leader.resolvedMarkets);

    if (
      leader.recordVerified === true &&
      wins !== null &&
      losses !== null &&
      total !== null &&
      total >= MIN_RESOLVED_FOR_RANKING &&
      wins >= 0 &&
      losses >= 0 &&
      wins + losses >= MIN_RESOLVED_FOR_RANKING
    ) {
      qualified.push({
        ...leader,
        winRate: (wins / (wins + losses)) * 100
      });
    } else {
      unqualified.push({
        ...leader,
        wins: null,
        losses: null,
        winRate: null,
        recordVerified: false
      });
    }
  }

  qualified.sort((a, b) =>
    b.winRate - a.winRate ||
    b.wins - a.wins ||
    (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
  );

  // Until reliable records are supplied, unqualified traders retain
  // their original P&L leaderboard order and display N/A for records.
  return [...qualified, ...unqualified].map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function buildPeriod(period) {
  console.log(`Loading ${period.key} leaderboard...`);

  const leaders = await getLeaderboard(period);
  console.log(`${period.key}: retrieved ${leaders.length} leaders`);

  if (!leaders.length) {
    throw new Error(
      `The ${period.key} leaderboard returned no traders. Existing JSON will not be overwritten.`
    );
  }

  console.log(`Loading recent BUY trades for ${leaders.length} traders...`);

  const tradeLists = await mapLimit(
    leaders,
    MAX_CONCURRENCY,
    async leader => [leader.wallet, await getRecentTrades(leader.wallet)]
  );

  const traderActiveTrades = Object.fromEntries(tradeLists);
  const consensus = buildConsensus(leaders, traderActiveTrades);

  // The current public endpoints used here do not establish a verified
  // win-loss record for each trader. Preserve N/A instead of fabricating one.
  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis:
      "Official Polymarket leaderboard P&L; verified win-loss records unavailable",
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: false,
    leaders,
    traderActiveTrades,
    consensus
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename} (${leaders.length} leaders, ${consensus.length} consensus markets)`
  );
}

async function main() {
  for (const period of PERIODS) {
    await buildPeriod(period);
  }

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
