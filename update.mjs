
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const OUT = "data";
const CANDIDATES = 1000;
const LEADERS = 100;
const PAGE = 50;
const MIN_RESOLVED = 10;

const RECORD_WORKERS = 24;
const ACTIVE_WORKERS = 30;
const TRADE_WORKERS = 20;
const TIMEOUT_MS = 8000;
const ATTEMPTS = 2;
const DELAY_MS = 25;

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" },
  { key: "threeMonth", days: 90, api: "MONTH" }
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

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
          "User-Agent": "PolymarketConsensusScanner/2.0"
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
        await sleep(150 * (attempt + 1));
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
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function ms(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (
    typeof value === "number" ||
    /^\d+(\.\d+)?$/.test(String(value))
  ) {
    const n = Number(value);
    return Number.isFinite(n)
      ? n < 1e12 ? n * 1000 : n
      : null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
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

function normalize(row, fallbackRank) {
  const wallet = walletOf(row);

  return {
    rank: num(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      (wallet
        ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}`
        : "Unknown"),
    pnl: num(row.pnl),
    volume: num(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getCandidates(period) {
  const traders = [];
  const seen = new Set();

  for (
    let offset = 0;
    offset < CANDIDATES;
    offset += PAGE
  ) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE),
      offset: String(offset)
    });

    const rows = arr(
      await getJson(
        `${API}/v1/leaderboard?${params}`
      )
    );

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalize(
        row,
        traders.length + 1
      );

      if (
        trader.wallet &&
        !seen.has(trader.wallet)
      ) {
        seen.add(trader.wallet);
        traders.push(trader);
      }
    }

    if (rows.length < PAGE) break;
    await sleep(DELAY_MS);
  }

  return traders.slice(0, CANDIDATES);
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;

      if (index >= items.length) return;

      try {
        results[index] = await callback(
          items[index],
          index
        );
      } catch (error) {
        results[index] = {
          __error: error
        };
      }

      await sleep(DELAY_MS);
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(limit, items.length)
      },
      () => worker()
    )
  );

  return results;
}

async function getClosedPositions(wallet, historyStart) {
  const positions = [];
  const seen = new Set();

  // Limit history requests to three pages per trader.
  for (let offset = 0; offset < 150; offset += PAGE) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(PAGE),
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = arr(
      await getJson(
        `${API}/closed-positions?${params}`
      )
    );

    if (!rows.length) break;

    let foundOlder = false;

    for (const position of rows) {
      const timestamp = ms(
        position.timestamp ??
        position.closedAt ??
        position.closed_at
      );

      if (timestamp === null) continue;

      if (timestamp < historyStart) {
        foundOlder = true;
        continue;
      }

      const condition = String(
        position.conditionId ??
        position.condition_id ??
        ""
      ).toLowerCase();

      const asset = String(
        position.asset ??
        position.token_id ??
        position.tokenId ??
        position.outcomeIndex ??
        position.outcome_index ??
        ""
      ).toLowerCase();

      if (!condition || !asset) continue;

      const key = `${condition}|${asset}`;

      if (seen.has(key)) continue;
      seen.add(key);

      positions.push({
        condition,
        asset,
        timestamp,
        price: num(
          position.curPrice ??
          position.currentPrice ??
          position.current_price
        ),
        title:
          position.title ||
          position.question ||
          "Unknown market",
        outcome: position.outcome || "Unknown"
      });
    }

    if (foundOlder || rows.length < PAGE) break;
  }

  return positions;
}

function calculateRecord(positions, period, now) {
  const start = now - period.days * 86400000;
  let wins = 0;
  let losses = 0;

  for (const position of positions) {
    if (
      position.timestamp < start ||
      position.timestamp > now
    ) {
      continue;
    }

    if (position.price === 1) wins++;
    else if (position.price === 0) losses++;
  }

  const resolvedMarkets = wins + losses;

  return {
    wins,
    losses,
    resolvedMarkets,
    winRate: resolvedMarkets
      ? (wins / resolvedMarkets) * 100
      : null,
    recordVerified: true
  };
}

async function getRecord(wallet, historyStart) {
  if (!recordCache.has(wallet)) {
    const request = getClosedPositions(
      wallet,
      historyStart
    );

    recordCache.set(wallet, request);

    request.catch(() => {
      recordCache.delete(wallet);
    });
  }

  return recordCache.get(wallet);
}

async function attachRecords(traders, period, now) {
  let completed = 0;

  return mapLimit(
    traders,
    RECORD_WORKERS,
    async trader => {
      try {
        const positions = await getRecord(
          trader.wallet,
          now - 90 * 86400000
        );

        const record = calculateRecord(
          positions,
          period,
          now
        );

        completed++;

        if (
          completed % 25 === 0 ||
          completed === traders.length
        ) {
          console.log(
            `${period.key} records ${completed}/${traders.length}`
          );
        }

        return {
          ...trader,
          ...record
        };
      } catch (error) {
        completed++;

        if (
          completed % 25 === 0 ||
          completed === traders.length
        ) {
          console.log(
            `${period.key} records ${completed}/${traders.length} (some requests failed)`
          );
        }

        return {
          ...trader,
          wins: null,
          losses: null,
          winRate: null,
          resolvedMarkets: null,
          recordVerified: false
        };
      }
    }
  );
}

function rankTraders(traders) {
  const qualifies = trader =>
    trader.recordVerified &&
    Number.isFinite(trader.wins) &&
    Number.isFinite(trader.losses) &&
    trader.resolvedMarkets >= MIN_RESOLVED;

  return [...traders]
    .sort((a, b) =>
      Number(qualifies(b)) - Number(qualifies(a)) ||
      (b.winRate ?? -1) - (a.winRate ?? -1) ||
      (b.wins ?? -1) - (a.wins ?? -1) ||
      (b.resolvedMarkets ?? -1) -
        (a.resolvedMarkets ?? -1) ||
      (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
    )
    .map((trader, index) => ({
      ...trader,
      rank: index + 1
    }));
}

function isActivePosition(position, now) {
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

  const endDate = ms(
    position.endDate ??
    position.end_date ??
    position.endDateIso
  );

  return endDate === null || endDate >= now;
}

async function getActivePositions(wallet, now) {
  if (!activeCache.has(wallet)) {
    const request = (async () => {
      const params = new URLSearchParams({
        user: wallet,
        limit: "500",
        offset: "0"
      });

      return arr(
        await getJson(
          `${API}/positions?${params}`
        )
      ).filter(position =>
        isActivePosition(position, now)
      );
    })();

    activeCache.set(wallet, request);

    request.catch(() => {
      activeCache.delete(wallet);
    });
  }

  return activeCache.get(wallet);
}

async function attachActivePositions(traders, now) {
  const unique = [
    ...new Map(
      traders
        .filter(trader => trader?.wallet)
        .map(trader => [trader.wallet, trader])
    ).values()
  ];

  let completed = 0;

  const pairs = await mapLimit(
    unique,
    ACTIVE_WORKERS,
    async trader => {
      let positions = [];

      try {
        positions = await getActivePositions(
          trader.wallet,
          now
        );
      } catch (error) {
        console.log(
          `Active request skipped (${trader.name}): ${error.message}`
        );
      }

      completed++;

      if (
        completed % 50 === 0 ||
        completed === unique.length
      ) {
        console.log(
          `Active positions ${completed}/${unique.length}`
        );
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
    const request = (async () => {
      const params = new URLSearchParams({
        user: wallet,
        limit: "50"
      });

      return arr(
        await getJson(
          `${API}/trades?${params}`
        )
      )
        .filter(
          trade =>
            String(trade.side || "").toUpperCase() === "BUY"
        )
        .map(trade => ({
          title:
            trade.title ||
            trade.question ||
            trade.eventSlug ||
            "Unknown market",
          side: "BUY",
          outcome:
            trade.outcome ||
            trade.outcome_name ||
            "Unknown",
          amount: num(
            trade.usdcSize ??
            trade.amount ??
            trade.size
          ),
          buyTime: ms(
            trade.timestamp ??
            trade.createdAt ??
            trade.time
          ),
          conditionId:
            trade.conditionId ||
            trade.condition_id ||
            null
        }))
        .sort(
          (a, b) =>
            (b.buyTime || 0) - (a.buyTime || 0)
        );
    })();

    tradeCache.set(wallet, request);

    request.catch(() => {
      tradeCache.delete(wallet);
    });
  }

  return tradeCache.get(wallet);
}

function buildConsensus(ranked, activeMap) {
  const markets = new Map();

  // Consensus is based on the weekly top 1,000 only.
  for (const trader of ranked.slice(0, CANDIDATES)) {
    if (
      !trader.recordVerified ||
      trader.resolvedMarkets < MIN_RESOLVED
    ) {
      continue;
    }

    for (
      const position of
      activeMap[trader.wallet] || []
    ) {
      const condition = String(
        position.conditionId ??
        position.condition_id ??
        ""
      ).toLowerCase();

      const side = String(
        position.outcome ??
        position.outcome_name ??
        ""
      ).trim();

      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title:
            position.title ||
            position.question ||
            "Unknown market",
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

      if (market.walletsSeen.has(trader.wallet)) {
        continue;
      }

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

      if (amount !== null) {
        market.totalEntry += amount;
      }

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
    .sort(
      (a, b) =>
        b.sameSide - a.sameSide ||
        b.totalEntry - a.totalEntry ||
        (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function writePeriod(period, ranked, activeMap, now) {
  const leaders = ranked.slice(0, LEADERS);

  const tradePairs = await mapLimit(
    leaders,
    TRADE_WORKERS,
    async trader => {
      try {
        return [
          trader.wallet,
          await getTrades(trader.wallet)
        ];
      } catch (error) {
        console.log(
          `Trades skipped (${trader.name}): ${error.message}`
        );

        return [trader.wallet, []];
      }
    }
  );

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    rankingBasis:
      `Resolved win-loss record over the last ${period.days} days, within the top ${CANDIDATES} P&L-ranked candidate traders`,
    recordBasis:
      "Closed positions with confirmed settlement price 1 (win) or 0 (loss)",
    candidateCount: ranked.length,
    minResolvedMarketsForRecordRanking: MIN_RESOLVED,
    recordRankingEnabled: true,
    leaders,
    traderActiveTrades: Object.fromEntries(
      tradePairs.filter(Array.isArray)
    ),
    traderActivePositions: activeMap,
    consensus:
      period.key === "week"
        ? buildConsensus(ranked, activeMap)
        : []
  };

  await fs.mkdir(OUT, { recursive: true });

  const file = `${OUT}/${period.key}.json`;
  const temp = `${file}.tmp`;

  await fs.writeFile(
    temp,
    JSON.stringify(output),
    "utf8"
  );

  await fs.rename(temp, file);

  console.log(
    `Wrote ${file}: ${leaders.length} leaders`
  );
}

async function main() {
  const now = Date.now();

  console.log("Loading weekly candidates...");
  const weekly = await getCandidates(PERIODS[0]);
  console.log(
    `Weekly candidates loaded: ${weekly.length}`
  );

  console.log("Loading monthly candidates...");
  const monthly = await getCandidates(PERIODS[1]);
  console.log(
    `Monthly candidates loaded: ${monthly.length}`
  );

  const uniqueCount = new Set(
    [...weekly, ...monthly].map(t => t.wallet)
  ).size;

  console.log(
    `Checking ${uniqueCount} unique traders. Records first; active positions later.`
  );

  // First calculate rankings. Do not load active positions for
  // all 1,500 traders before the rankings are available.
  const [weeklyRanked, monthlyRanked, threeMonthRanked] =
    await Promise.all([
      attachRecords(weekly, PERIODS[0], now)
        .then(rankTraders),
      attachRecords(monthly, PERIODS[1], now)
        .then(rankTraders),
      attachRecords(monthly, PERIODS[2], now)
        .then(rankTraders)
    ]);

  console.log(
    "Record rankings complete. Fetching active positions for weekly top 1,000 and monthly top 100."
  );

  const activeMap = await attachActivePositions(
    [
      ...weeklyRanked.slice(0, CANDIDATES),
      ...monthlyRanked.slice(0, LEADERS)
    ],
    now
  );

  await Promise.all([
    writePeriod(
      PERIODS[0],
      weeklyRanked,
      activeMap,
      now
    ),
    writePeriod(
      PERIODS[1],
      monthlyRanked,
      activeMap,
      now
    ),
    writePeriod(
      PERIODS[2],
      threeMonthRanked,
      activeMap,
      now
    )
  ]);

  await fs.writeFile(
    `${OUT}/status.json`,
    JSON.stringify(
      {
        updatedAt: new Date(now).toISOString(),
        success: true,
        weeklyCandidates: weekly.length,
        monthlyCandidates: monthly.length,
        activeTradersChecked:
          Object.keys(activeMap).length
      },
      null,
      2
    ),
    "utf8"
  );

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
