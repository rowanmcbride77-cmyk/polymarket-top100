
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const OUT = "data";
const CANDIDATES = 1000;
const LEADERS = 100;
const PAGE = 50;
const MIN_RESOLVED = 10;

const RECORD_WORKERS = 20;
const ACTIVE_WORKERS = 20;
const TRADE_WORKERS = 15;
const TIMEOUT_MS = 10000;
const ATTEMPTS = 3;

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" },
  { key: "threeMonth", days: 90, api: "MONTH" }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const recordCache = new Map();
const activeCache = new Map();
const tradeCache = new Map();

async function getJson(url) {
  let lastError;

  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/3.0"
        },
        signal: AbortSignal.timeout(TIMEOUT_MS)
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt + 1 < ATTEMPTS) {
        await sleep(300 * (attempt + 1));
      }
    }
  }

  throw lastError || new Error("Request failed");
}

function arr(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  return [];
}

function num(value) {
  if (value === null || value === undefined || value === "") return null;
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function ms(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const result = Number(value);
    if (!Number.isFinite(result)) return null;
    return result < 1e12 ? result * 1000 : result;
  }

  const result = Date.parse(value);
  return Number.isFinite(result) ? result : null;
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

function normalize(row, rank) {
  const wallet = walletOf(row);

  return {
    rank: num(row.rank) ?? rank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: num(row.pnl),
    volume: num(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;

      try {
        results[index] = await fn(items[index], index);
      } catch (error) {
        results[index] = { __error: error.message };
      }
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

async function candidates(period) {
  const results = [];
  const seen = new Set();

  for (let offset = 0; offset < CANDIDATES; offset += PAGE) {
    const query = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE),
      offset: String(offset)
    });

    const rows = arr(
      await getJson(`${API}/v1/leaderboard?${query}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalize(row, results.length + 1);

      if (trader.wallet && !seen.has(trader.wallet)) {
        seen.add(trader.wallet);
        results.push(trader);
      }
    }

    if (rows.length < PAGE) break;
    await sleep(75);
  }

  return results.slice(0, CANDIDATES);
}

/*
 * IMPORTANT:
 * A closed-position current price of 1 or 0 is NOT sufficient
 * evidence that a trader won or lost that market.
 *
 * Until verified settlement/redemption data is available,
 * the record stays unverified instead of inventing W/L results.
 */
async function getRecord() {
  return {
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function attachRecords(list, period) {
  let done = 0;

  return mapLimit(list, RECORD_WORKERS, async trader => {
    const record = await getRecord(trader.wallet, period);
    done++;

    if (done % 50 === 0 || done === list.length) {
      console.log(`${period.key} records ${done}/${list.length}`);
    }

    return { ...trader, ...record };
  });
}

function rank(list) {
  const qualified = trader =>
    trader.recordVerified === true &&
    Number.isFinite(trader.wins) &&
    Number.isFinite(trader.losses) &&
    trader.resolvedMarkets >= MIN_RESOLVED;

  return [...list]
    .sort((a, b) =>
      Number(qualified(b)) - Number(qualified(a)) ||
      (b.winRate ?? -1) - (a.winRate ?? -1) ||
      (b.wins ?? -1) - (a.wins ?? -1) ||
      (b.resolvedMarkets ?? -1) - (a.resolvedMarkets ?? -1) ||
      (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity) ||
      (b.volume ?? -Infinity) - (a.volume ?? -Infinity)
    )
    .map((trader, index) => ({ ...trader, rank: index + 1 }));
}

function isActive(position, now) {
  const size = num(
    position.size ??
    position.current_size ??
    position.currentSize
  );

  if (
    size === null ||
    size <= 0 ||
    position.closed === true ||
    position.redeemable === true ||
    position.mergeable === true
  ) {
    return false;
  }

  const end = ms(
    position.endDate ??
    position.end_date ??
    position.endDateIso
  );

  return end === null || end >= now;
}

async function getActive(wallet, now) {
  if (!activeCache.has(wallet)) {
    const promise = (async () => {
      const query = new URLSearchParams({
        user: wallet,
        limit: "500",
        offset: "0"
      });

      return arr(
        await getJson(`${API}/positions?${query}`)
      ).filter(position => isActive(position, now));
    })();

    activeCache.set(wallet, promise);
    promise.catch(() => activeCache.delete(wallet));
  }

  return activeCache.get(wallet);
}

async function attachActive(list, now) {
  const unique = [
    ...new Map(
      list
        .filter(trader => trader?.wallet)
        .map(trader => [trader.wallet, trader])
    ).values()
  ];

  let done = 0;

  const pairs = await mapLimit(
    unique,
    ACTIVE_WORKERS,
    async trader => {
      let positions = [];

      try {
        positions = await getActive(trader.wallet, now);
      } catch (error) {
        console.log(`Active positions skipped for ${trader.name}: ${error.message}`);
      }

      done++;

      if (done % 50 === 0 || done === unique.length) {
        console.log(`Active positions ${done}/${unique.length}`);
      }

      return [trader.wallet, positions];
    }
  );

  return Object.fromEntries(
    pairs.filter(Array.isArray)
  );
}

async function getTrades(wallet) {
  if (!tradeCache.has(wallet)) {
    const promise = (async () => {
      const query = new URLSearchParams({
        user: wallet,
        limit: "50"
      });

      return arr(
        await getJson(`${API}/trades?${query}`)
      )
        .filter(row => String(row.side || "").toUpperCase() === "BUY")
        .map(row => ({
          title: row.title || row.question || row.eventSlug || "Unknown market",
          side: "BUY",
          outcome: row.outcome || row.outcome_name || "Unknown",
          amount: num(row.usdcSize ?? row.amount ?? row.size),
          buyTime: ms(row.timestamp ?? row.createdAt ?? row.time),
          conditionId: row.conditionId || row.condition_id || null
        }))
        .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
    })();

    tradeCache.set(wallet, promise);
    promise.catch(() => tradeCache.delete(wallet));
  }

  return tradeCache.get(wallet);
}

function consensus(ranked, active) {
  const markets = new Map();

  /*
   * Use the weekly top-1,000 candidates, with a verified record
   * and at least 10 resolved markets. Never merge opposite sides.
   */
  for (const trader of ranked.slice(0, CANDIDATES)) {
    if (
      !trader.recordVerified ||
      trader.resolvedMarkets < MIN_RESOLVED
    ) continue;

    for (const position of active[trader.wallet] || []) {
      const condition = String(
        position.conditionId ?? position.condition_id ?? ""
      ).toLowerCase();

      const side = String(
        position.outcome ?? position.outcome_name ?? ""
      ).trim();

      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: position.title || position.question || "Unknown market",
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

      if (market.walletsSeen.has(trader.wallet)) continue;
      market.walletsSeen.add(trader.wallet);

      const amount = num(
        position.currentValue ??
        position.current_value ??
        position.initialValue ??
        position.initial_value
      );

      const buyTime = ms(
        position.lastEventAt ??
        position.last_event_at
      );

      market.sameSide++;

      market.traders.push({
        name: trader.name,
        wallet: trader.wallet,
        rank: trader.rank,
        wins: trader.wins,
        losses: trader.losses,
        winRate: trader.winRate,
        resolvedMarkets: trader.resolvedMarkets,
        pnl: trader.pnl,
        volume: trader.volume,
        recordVerified: trader.recordVerified,
        side,
        amount,
        buyTime
      });

      if (amount !== null) market.totalEntry += amount;

      if (buyTime !== null) {
        market.newestBuy =
          market.newestBuy === null
            ? buyTime
            : Math.max(market.newestBuy, buyTime);

        market.oldestBuy =
          market.oldestBuy === null
            ? buyTime
            : Math.min(market.oldestBuy, buyTime);
      }
    }
  }

  return [...markets.values()]
    .filter(market => market.sameSide >= 2)
    .map(({ walletsSeen, ...market }) => market)
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function writePeriod(period, ranked, active, now) {
  const leaders = ranked.slice(0, LEADERS);

  const tradePairs = await mapLimit(
    leaders,
    TRADE_WORKERS,
    async trader => {
      try {
        return [trader.wallet, await getTrades(trader.wallet)];
      } catch (error) {
        console.log(`Trades skipped for ${trader.name}: ${error.message}`);
        return [trader.wallet, []];
      }
    }
  );

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    rankingBasis:
      `Polymarket ${period.api} P&L leaderboard candidate pool; ` +
      `${period.days}-day target period`,
    recordBasis:
      "W/L withheld until verified settlement/redemption outcomes are available",
    candidateCount: ranked.length,
    minResolvedMarketsForRecordRanking: MIN_RESOLVED,
    recordRankingEnabled: false,
    leaders,
    traderActiveTrades: Object.fromEntries(
      tradePairs.filter(Array.isArray)
    ),
    traderActivePositions: active,
    consensus: period.key === "week" ? consensus(ranked, active) : []
  };

  await fs.mkdir(OUT, { recursive: true });

  const file = `${OUT}/${period.key}.json`;
  const temporary = `${file}.tmp`;

  await fs.writeFile(temporary, JSON.stringify(output), "utf8");
  await fs.rename(temporary, file);

  console.log(`Wrote ${file}: ${leaders.length} leaders`);
}

async function main() {
  const now = Date.now();

  console.log("Loading weekly leaderboard candidates...");
  const week = await candidates(PERIODS[0]);

  console.log("Loading monthly leaderboard candidates...");
  const month = await candidates(PERIODS[1]);

  console.log(
    `Loaded ${week.length} weekly and ${month.length} monthly candidates.`
  );

  /*
   * The 3-month board cannot be called a verified 90-day W/L ranking
   * by simply reusing the monthly leaderboard. Keep its period explicit.
   */
  const [weekRanked, monthRanked, threeMonthRanked] = await Promise.all([
    attachRecords(week, PERIODS[0]).then(rank),
    attachRecords(month, PERIODS[1]).then(rank),
    attachRecords(month, PERIODS[2]).then(rank)
  ]);

  console.log("Loading active positions for the weekly top 1,000 and monthly top 100...");

  const active = await attachActive(
    [
      ...weekRanked.slice(0, CANDIDATES),
      ...monthRanked.slice(0, LEADERS)
    ],
    now
  );

  await Promise.all([
    writePeriod(PERIODS[0], weekRanked, active, now),
    writePeriod(PERIODS[1], monthRanked, active, now),
    writePeriod(PERIODS[2], threeMonthRanked, active, now)
  ]);

  await fs.writeFile(
    `${OUT}/status.json`,
    JSON.stringify({
      updatedAt: new Date(now).toISOString(),
      success: true,
      weeklyCandidates: week.length,
      monthlyCandidates: month.length,
      activeTradersChecked: Object.keys(active).length,
      warning: "W/L records are intentionally unverified until settlement outcomes are reliably sourced."
    }, null, 2),
    "utf8"
  );

  console.log("Polymarket scanner update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
