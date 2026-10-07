import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const PERIODS = {
  week: 7 * 24 * 60 * 60 * 1000,
  month: 30 * 24 * 60 * 60 * 1000,
  threeMonth: 90 * 24 * 60 * 60 * 1000
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJson(url, tries = 4) {
  let lastError;

  for (let attempt = 1; attempt <= tries; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 20000);

      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Consensus-Scanner/1.0"
        }
      });

      clearTimeout(timer);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (attempt < tries) {
        await sleep(800 * attempt);
      }
    }
  }

  throw lastError;
}

async function fetchLeaderboard(timePeriod, orderBy, limit = 1000) {
  const rows = [];

  for (let offset = 0; offset < limit; offset += 50) {
    const url =
      `${BASE}/v1/leaderboard` +
      `?category=OVERALL` +
      `&timePeriod=${encodeURIComponent(timePeriod)}` +
      `&orderBy=${encodeURIComponent(orderBy)}` +
      `&limit=50` +
      `&offset=${offset}`;

    const data = await getJson(url);

    if (!Array.isArray(data) || data.length === 0) {
      break;
    }

    rows.push(...data);

    if (data.length < 50) {
      break;
    }
  }

  return rows;
}

async function buildCandidateUniverse() {
  console.log("Loading Polymarket leaderboard candidates...");

  const lists = await Promise.all([
    fetchLeaderboard("WEEK", "PNL"),
    fetchLeaderboard("WEEK", "VOL"),
    fetchLeaderboard("MONTH", "PNL"),
    fetchLeaderboard("MONTH", "VOL"),
    fetchLeaderboard("ALL", "PNL"),
    fetchLeaderboard("ALL", "VOL")
  ]);

  const map = new Map();

  for (const list of lists) {
    for (const trader of list) {
      const address =
        trader.proxyWallet ||
        trader.address ||
        trader.user ||
        trader.wallet;

      if (!address) continue;

      const existing = map.get(address);

      map.set(address, {
        address,
        name:
          trader.userName ||
          trader.username ||
          trader.name ||
          existing?.name ||
          "Unknown"
      });
    }
  }

  return [...map.values()];
}

async function fetchClosedPositions(address) {
  const positions = [];
  let cursor = null;

  for (let page = 0; page < 100; page++) {
    let url =
      `${BASE}/v2/positions` +
      `?user=${encodeURIComponent(address)}` +
      `&status=CLOSED` +
      `&sortBy=TIMESTAMP` +
      `&sortDirection=DESC`;

    if (cursor) {
      url += `&cursor=${encodeURIComponent(cursor)}`;
    }

    const result = await getJson(url);

    const rows = Array.isArray(result)
      ? result
      : Array.isArray(result?.data)
        ? result.data
        : [];

    positions.push(...rows);

    const nextCursor =
      result?.pagination?.next_cursor ||
      result?.next_cursor ||
      null;

    if (!nextCursor || rows.length === 0) {
      break;
    }

    cursor = nextCursor;

    if (positions.length >= 5000) {
      break;
    }
  }

  return positions;
}

function getTimestamp(position) {
  const values = [
    position.timestamp,
    position.closed_at,
    position.close_timestamp,
    position.last_event_at
  ];

  for (const value of values) {
    if (value === undefined || value === null) continue;

    const number = Number(value);

    if (Number.isFinite(number)) {
      return number > 100000000000
        ? number
        : number * 1000;
    }

    const date = new Date(value).getTime();

    if (Number.isFinite(date)) {
      return date;
    }
  }

  return 0;
}

function getRealizedPnl(position) {
  const values = [
    position.realizedPnl,
    position.realized_pnl,
    position.realizedPnL,
    position.cashPnl,
    position.cash_pnl
  ];

  for (const value of values) {
    const number = Number(value);

    if (Number.isFinite(number)) {
      return number;
    }
  }

  return 0;
}

function getMarketId(position) {
  return (
    position.conditionId ||
    position.condition_id ||
    position.market ||
    position.condition ||
    ""
  );
}

function calculateStats(positions, cutoff) {
  const relevant = positions.filter(position => {
    const timestamp = getTimestamp(position);
    return timestamp >= cutoff;
  });

  let wins = 0;
  let losses = 0;
  let breakeven = 0;
  let pnl = 0;

  const markets = new Set();

  for (const position of relevant) {
    const realizedPnl = getRealizedPnl(position);

    pnl += realizedPnl;

    const marketId = getMarketId(position);

    if (marketId) {
      markets.add(marketId);
    }

    if (realizedPnl > 0.000001) {
      wins++;
    } else if (realizedPnl < -0.000001) {
      losses++;
    } else {
      breakeven++;
    }
  }

  const decided = wins + losses;

  return {
    wins,
    losses,
    breakeven,
    markets: markets.size,
    pnl,
    winRate: decided > 0
      ? (wins / decided) * 100
      : null
  };
}

function rankingSort(a, b) {
  const winRateA = a.winRate ?? -1;
  const winRateB = b.winRate ?? -1;

  if (winRateB !== winRateA) {
    return winRateB - winRateA;
  }

  if (b.wins !== a.wins) {
    return b.wins - a.wins;
  }

  const decidedA = a.wins + a.losses;
  const decidedB = b.wins + b.losses;

  if (decidedB !== decidedA) {
    return decidedB - decidedA;
  }

  return b.pnl - a.pnl;
}

async function processCandidates(candidates) {
  const results = [];
  const queue = [...candidates];

  let completed = 0;

  async function worker() {
    while (queue.length > 0) {
      const trader = queue.shift();

      try {
        const positions = await fetchClosedPositions(trader.address);

        const now = Date.now();

        results.push({
          address: trader.address,
          name: trader.name,

          week: calculateStats(
            positions,
            now - PERIODS.week
          ),

          month: calculateStats(
            positions,
            now - PERIODS.month
          ),

          threeMonth: calculateStats(
            positions,
            now - PERIODS.threeMonth
          )
        });
      } catch (error) {
        console.log(
          `Failed ${trader.name}: ${error.message}`
        );
      }

      completed++;

      if (completed % 25 === 0) {
        console.log(
          `Processed ${completed}/${candidates.length}`
        );
      }
    }
  }

  const workers = [];

  for (let i = 0; i < 12; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  return results;
}

function buildLeaderboard(results, period) {
  const qualified = results
    .filter(trader => trader[period])
    .filter(trader => {
      const stats = trader[period];
      return stats.wins + stats.losses >= 10;
    })
    .sort((a, b) =>
      rankingSort(a[period], b[period])
    );

  return qualified
    .slice(0, 100)
    .map((trader, index) => {
      const stats = trader[period];

      return {
        rank: index + 1,
        name: trader.name,
        pnl: Number(stats.pnl.toFixed(2)),
        wins: stats.wins,
        losses: stats.losses,
        breakeven: stats.breakeven,
        markets: stats.markets,
        winRate: Number(stats.winRate.toFixed(2))
      };
    });
}

async function fetchOpenPositions(address) {
  const positions = [];
  let cursor = null;

  for (let page = 0; page < 20; page++) {
    let url =
      `${BASE}/v2/positions` +
      `?user=${encodeURIComponent(address)}` +
      `&status=OPEN`;

    if (cursor) {
      url += `&cursor=${encodeURIComponent(cursor)}`;
    }

    const result = await getJson(url);

    const rows = Array.isArray(result)
      ? result
      : Array.isArray(result?.data)
        ? result.data
        : [];

    positions.push(...rows);

    const nextCursor =
      result?.pagination?.next_cursor ||
      result?.next_cursor ||
      null;

    if (!nextCursor || rows.length === 0) {
      break;
    }

    cursor = nextCursor;

    if (positions.length >= 500) {
      break;
    }
  }

  return positions;
}

async function fetchRecentBuys(address, sinceMs) {
  const start = Math.floor(sinceMs / 1000);

  const url =
    `${BASE}/trades` +
    `?user=${encodeURIComponent(address)}` +
    `&side=BUY` +
    `&start=${start}` +
    `&limit=500`;

  try {
    const result = await getJson(url);

    return Array.isArray(result)
      ? result
      : Array.isArray(result?.data)
        ? result.data
        : [];
  } catch {
    return [];
  }
}

function getOutcome(position) {
  return String(
    position.outcome ||
    position.outcome_name ||
    position.outcomeName ||
    ""
  ).toUpperCase();
}

function getAmount(position) {
  const direct = [
    position.initialValue,
    position.initial_value,
    position.currentValue,
    position.current_value
  ];

  for (const value of direct) {
    const number = Number(value);

    if (Number.isFinite(number) && number > 0) {
      return number;
    }
  }

  const size = Number(
    position.size ||
    position.current_size ||
    0
  );

  const price = Number(
    position.avgPrice ||
    position.avg_price ||
    0
  );

  if (size > 0 && price > 0) {
    return size * price;
  }

  return 0;
}

function tradeTime(trade) {
  const value =
    trade.timestamp ||
    trade.time ||
    trade.created_at ||
    trade.createdAt;

  const number = Number(value);

  if (!Number.isFinite(number)) {
    return 0;
  }

  return number > 100000000000
    ? number
    : number * 1000;
}

async function buildConsensus(candidates) {
  const universe = candidates.slice(0, 1000);

  const since = Date.now() - 48 * 60 * 60 * 1000;

  const groups = new Map();

  const queue = [...universe];

  let completed = 0;

  async function worker() {
    while (queue.length > 0) {
      const trader = queue.shift();

      try {
        const [
          positions,
          buys
        ] = await Promise.all([
          fetchOpenPositions(trader.address),
          fetchRecentBuys(
            trader.address,
            since
          )
        ]);

        for (const position of positions) {
          const marketId = getMarketId(position);
          const side = getOutcome(position);

          if (
            !marketId ||
            (side !== "YES" && side !== "NO")
          ) {
            continue;
          }

          const matchingBuys = buys.filter(trade => {

            const tradeMarket =
              trade.conditionId ||
              trade.condition_id ||
              trade.market ||
              "";

            const tradeOutcome =
              String(
                trade.outcome ||
                trade.outcome_name ||
                trade.outcomeName ||
                ""
              ).toUpperCase();

            return (
              String(tradeMarket) ===
              String(marketId) &&
              tradeOutcome === side
            );
          });

          if (!matchingBuys.length) {
            continue;
          }

          const times = matchingBuys
            .map(tradeTime)
            .filter(Boolean)
            .sort((a, b) => a - b);

          if (!times.length) {
            continue;
          }

          let group = groups.get(marketId);

          if (!group) {
            group = {
              market: marketId,
              title:
                position.title ||
                position.market_title ||
                position.marketTitle ||
                position.question ||
                "Unknown market",
              YES: new Map(),
              NO: new Map()
            };

            groups.set(marketId, group);
          }

          const map = group[side];

          if (!map.has(trader.address)) {
            map.set(trader.address, {
              name: trader.name,
              side,
              amount: getAmount(position),
              firstBuy: times[0]
            });
          }
        }
      } catch {}

      completed++;

      if (completed % 50 === 0) {
        console.log(
          `Consensus processed ${completed}/${universe.length}`
        );
      }
    }
  }

  const workers = [];

  for (let i = 0; i < 15; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  const output = [];

  for (const group of groups.values()) {
    for (const side of ["YES", "NO"]) {
      const traders = [...group[side].values()];

      if (traders.length < 2) {
        continue;
      }

      const opposite =
        side === "YES"
          ? group.NO.size
          : group.YES.size;

      if (opposite > 0) {
        continue;
      }

      traders.sort(
        (a, b) => b.amount - a.amount
      );

      const totalEntry =
        traders.reduce(
          (sum, trader) =>
            sum + Number(trader.amount || 0),
          0
        );

      const firstBuy =
        Math.min(
          ...traders.map(
            trader => trader.firstBuy
          )
        );

      output.push({
        market: group.market,
        title: group.title,
        side,
        sameSide: traders.length,
        oppositeSide: 0,
        totalEntry:
          Number(totalEntry.toFixed(2)),
        firstBuy:
          new Date(firstBuy).toISOString(),
        holdLabel:
          "Bought within 48h",

        traders:
          traders.map(trader => ({
            name: trader.name,
            side: trader.side,
            amount:
              Number(trader.amount.toFixed(2)),
            firstBuy:
              new Date(
                trader.firstBuy
              ).toISOString()
          }))
      });
    }
  }

  output.sort((a, b) => {

    if (b.sameSide !== a.sameSide) {
      return b.sameSide - a.sameSide;
    }

    return b.totalEntry - a.totalEntry;
  });

  return output.slice(0, 500);
}

function writeJson(filename, data) {
  fs.writeFileSync(
    path.join(DATA_DIR, filename),
    JSON.stringify(data, null, 2)
  );
}

async function main() {

  fs.mkdirSync(DATA_DIR, {
    recursive: true
  });

  console.log(
    "Building Polymarket candidate universe..."
  );

  const candidates =
    await buildCandidateUniverse();

  console.log(
    `Found ${candidates.length} candidate wallets`
  );

  console.log(
    "Calculating closed-position win/loss records..."
  );

  const results =
    await processCandidates(candidates);

  console.log(
    "Building active consensus..."
  );

  const consensus =
    await buildConsensus(candidates);

  const generatedAt =
    new Date().toISOString();

  const week =
    buildLeaderboard(
      results,
      "week"
    );

  const month =
    buildLeaderboard(
      results,
      "month"
    );

  const threeMonth =
    buildLeaderboard(
      results,
      "threeMonth"
    );

  const base = {
    generatedAt,

    consensus,

    rankingMethod:
      "Win rate first, then wins, then decided markets, then P&L",

    minimumDecidedMarkets: 10
  };

  writeJson(
    "week.json",
    {
      ...base,
      period: "week",
      periodLabel: "1 Week",
      leaders: week
    }
  );

  writeJson(
    "month.json",
    {
      ...base,
      period: "month",
      periodLabel: "1 Month",
      leaders: month
    }
  );

  writeJson(
    "threeMonth.json",
    {
      ...base,
      period: "threeMonth",
      periodLabel: "3 Months",
      leaders: threeMonth
    }
  );

  writeJson(
    "status.json",
    {
      ok: true,
      generatedAt,
      candidateWallets:
        candidates.length,
      walletsProcessed:
        results.length,
      weekLeaders:
        week.length,
      monthLeaders:
        month.length,
      threeMonthLeaders:
        threeMonth.length,
      consensusMarkets:
        consensus.length
    }
  );

  console.log("================================");
  console.log("POLYMARKET UPDATE COMPLETE");
  console.log("================================");
  console.log(
    `Week leaders: ${week.length}`
  );
  console.log(
    `Month leaders: ${month.length}`
  );
  console.log(
    `3-month leaders: ${threeMonth.length}`
  );
  console.log(
    `Consensus markets: ${consensus.length}`
  );
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
