
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const TRADE_LIMIT = 50;
const MAX_CONCURRENCY = 4;
const REQUEST_DELAY_MS = 250;
const MIN_RESOLVED_FOR_RANKING = 10;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/2.0"
        },
        signal: AbortSignal.timeout(25000)
      });

      if (!response.ok) {
        const body = await response.text();
        const error = new Error(
          `HTTP ${response.status}: ${body.slice(0, 250)}`
        );

        if (
          response.status !== 429 &&
          response.status < 500
        ) {
          throw error;
        }

        const retryAfter = Number(
          response.headers.get("retry-after")
        );

        await sleep(
          retryAfter > 0
            ? Math.min(retryAfter * 1000, 20000)
            : 1000 * (attempt + 1)
        );

        throw error;
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (attempt < attempts - 1) {
        console.warn(
          `Request attempt ${attempt + 1} failed: ${error.message}`
        );
        await sleep(750 * (attempt + 1));
      }
    }
  }

  throw lastError || new Error("Request failed");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  if (Array.isArray(value?.data?.data)) return value.data.data;
  return [];
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
    row?.proxy_wallet ??
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
      "Unknown trader",
    pnl: numberOrNull(row.pnl),
    volume: numberOrNull(row.vol ?? row.volume),

    // Do not fabricate a trader's resolved win/loss record.
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getLeaderboard(period) {
  const leaders = [];
  const seen = new Set();

  for (
    let offset = 0;
    offset < LEADER_COUNT;
    offset += PAGE_SIZE
  ) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const url =
      `${API}/v1/leaderboard?${params.toString()}`;

    const rows = asArray(await getJson(url));

    if (!rows.length) {
      if (offset === 0) {
        throw new Error(
          `${period.key} leaderboard returned no data.`
        );
      }
      break;
    }

    for (const row of rows) {
      const leader = normalizeLeader(
        row,
        leaders.length + 1
      );

      if (!leader.wallet || seen.has(leader.wallet)) {
        continue;
      }

      seen.add(leader.wallet);
      leaders.push(leader);
    }

    if (rows.length < PAGE_SIZE) break;

    await sleep(REQUEST_DELAY_MS);
  }

  return leaders.slice(0, LEADER_COUNT).map(
    (leader, index) => ({
      ...leader,
      rank: index + 1
    })
  );
}

function getTradeTimestamp(row) {
  const raw =
    row.timestamp ??
    row.createdAt ??
    row.created_at ??
    row.time;

  const numeric = numberOrNull(raw);

  if (numeric !== null) {
    // Convert millisecond timestamps to seconds.
    return numeric > 100000000000
      ? Math.floor(numeric / 1000)
      : numeric;
  }

  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    if (Number.isFinite(parsed)) {
      return Math.floor(parsed / 1000);
    }
  }

  return null;
}

function getTradeAmount(row) {
  // Explicit USD fields take priority.
  const explicitUsd = numberOrNull(
    row.usdcSize ??
    row.usdc_size ??
    row.amount_usdc ??
    row.cashAmount
  );

  if (explicitUsd !== null) return explicitUsd;

  // Polymarket trade "size" is normally shares.
  // USD value = shares multiplied by execution price.
  const size = numberOrNull(row.size);
  const price = numberOrNull(row.price);

  if (size !== null && price !== null) {
    return size * price;
  }

  // Do not treat raw share size as dollars.
  return null;
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT)
  });

  try {
    const response = await getJson(
      `${API}/trades?${params.toString()}`
    );

    return asArray(response)
      .filter(
        row =>
          String(row.side || "").toUpperCase() === "BUY"
      )
      .map(row => ({
        title:
          row.title ||
          row.question ||
          row.market ||
          row.eventSlug ||
          "Unknown market",

        side: "BUY",

        outcome:
          row.outcome ||
          row.outcome_name ||
          row.outcomeName ||
          "Unknown",

        amount: getTradeAmount(row),

        buyTime: getTradeTimestamp(row),

        conditionId:
          row.conditionId ||
          row.condition_id ||
          null,

        transactionHash:
          row.transactionHash ||
          row.transaction_hash ||
          null
      }))
      .filter(trade => trade.conditionId)
      .sort(
        (a, b) => (b.buyTime || 0) - (a.buyTime || 0)
      );
  } catch (error) {
    console.error(
      `Recent buys failed for ${wallet}: ${error.message}`
    );
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
    const trades =
      traderActiveTrades[leader.wallet] || [];

    for (const trade of trades) {
      const condition = String(
        trade.conditionId || ""
      ).toLowerCase();

      const title = String(
        trade.title || "Unknown market"
      ).trim();

      const outcome = String(
        trade.outcome || "Unknown"
      ).trim();

      if (!condition || outcome === "Unknown") continue;

      // Consensus requires the same market AND same outcome.
      const key = `${condition}|${outcome.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title,
          side: outcome,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          oldestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }

      const market = markets.get(key);

      // Count each trader only once for a market/outcome.
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
        side: outcome,
        amount: numberOrNull(trade.amount),
        buyTime: numberOrNull(trade.buyTime)
      });

      const amount = numberOrNull(trade.amount);

      if (amount !== null) {
        market.totalEntry += amount;
      }

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
    .sort(
      (a, b) =>
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
    const resolved = numberOrNull(
      leader.resolvedMarkets
    );

    if (
      leader.recordVerified === true &&
      wins !== null &&
      losses !== null &&
      resolved !== null &&
      resolved >= MIN_RESOLVED_FOR_RANKING &&
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

  qualified.sort(
    (a, b) =>
      b.winRate - a.winRate ||
      b.wins - a.wins ||
      (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
  );

  return [...qualified, ...unqualified].map(
    (leader, index) => ({
      ...leader,
      rank: index + 1
    })
  );
}

async function buildPeriod(period) {
  console.log(
    `\n========== ${period.key.toUpperCase()} ==========`
  );

  const leaders = await getLeaderboard(period);

  if (!leaders.length) {
    throw new Error(
      `${period.key} leaderboard returned no traders.`
    );
  }

  console.log(
    `Retrieved ${leaders.length} leaderboard traders.`
  );

  console.log("Fetching recent BUY trades...");

  const tradeLists = await mapLimit(
    leaders,
    MAX_CONCURRENCY,
    async leader => [
      leader.wallet,
      await getRecentTrades(leader.wallet)
    ]
  );

  const traderActiveTrades =
    Object.fromEntries(tradeLists);

  const totalBuys = Object.values(
    traderActiveTrades
  ).reduce((sum, trades) => sum + trades.length, 0);

  const consensus = buildConsensus(
    leaders,
    traderActiveTrades
  );

  const rankedLeaders = sortByVerifiedRecord(leaders);

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,

    rankingBasis:
      "Official Polymarket P&L leaderboard; win/loss records are N/A until independently verified.",

    minResolvedMarketsForRecordRanking:
      MIN_RESOLVED_FOR_RANKING,

    recordRankingEnabled: false,

    leaders: rankedLeaders,
    traderActiveTrades,
    consensus,

    diagnostics: {
      leaderCount: leaders.length,
      tradersWithBuys: Object.values(
        traderActiveTrades
      ).filter(trades => trades.length > 0).length,
      totalBuyTrades: totalBuys,
      consensusMarkets: consensus.length
    }
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(
    tempFilename,
    JSON.stringify(output),
    "utf8"
  );

  await fs.rename(tempFilename, filename);

  console.log(`Wrote ${filename}`);
  console.log(`  Traders: ${leaders.length}`);
  console.log(`  BUY trades: ${totalBuys}`);
  console.log(`  Consensus markets: ${consensus.length}`);
}

async function main() {
  for (const period of PERIODS) {
    await buildPeriod(period);
  }

  console.log(
    "\nUpdate finished. Both week.json and month.json were generated."
  );
}

main().catch(error => {
  console.error("\nPolymarket update failed:", error);
  process.exitCode = 1;
});
