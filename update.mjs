
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

const diagnostics = {
  apiErrors: [],
  tradeErrors: 0,
  positionErrors: 0
};

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

function logApiError(type, wallet, error) {
  diagnostics.apiErrors.push({
    type,
    wallet,
    message: String(error?.message || error).slice(0, 500)
  });

  if (type === "trades") diagnostics.tradeErrors++;
  if (type === "positions") diagnostics.positionErrors++;

  console.error(`[${type.toUpperCase()} ERROR] ${wallet}: ${error.message}`);
}

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/3.0"
        },
        signal: AbortSignal.timeout(25000)
      });

      if (!response.ok) {
        const body = await response.text();
        const error = new Error(
          `HTTP ${response.status}: ${body.slice(0, 300)}`
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

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
    row?.proxy_wallet ??
    row?.user_id ??
    row?.address ??
    row?.wallet ??
    ""
  ).trim().toLowerCase();
}

function normalizeLeader(row, fallbackRank) {
  return {
    rank: numberOrNull(row.rank) ?? fallbackRank,
    wallet: walletOf(row),
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      "Unknown trader",
    pnl: numberOrNull(row.pnl),
    volume: numberOrNull(row.vol ?? row.volume),
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

  for (let offset = 0; offset < LEADER_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const url = `${API}/v1/leaderboard?${params}`;

    const rows = asArray(await getJson(url));

    if (!rows.length) {
      if (offset === 0) {
        throw new Error(`${period.key} leaderboard returned no data.`);
      }
      break;
    }

    for (const row of rows) {
      const leader = normalizeLeader(row, leaders.length + 1);

      if (!leader.wallet || seen.has(leader.wallet)) continue;

      seen.add(leader.wallet);
      leaders.push(leader);
    }

    if (rows.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return leaders.slice(0, LEADER_COUNT).map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

function timestampOf(row) {
  const raw =
    row?.timestamp ??
    row?.createdAt ??
    row?.created_at ??
    row?.time;

  const numeric = numberOrNull(raw);

  if (numeric !== null) {
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

function amountOf(row) {
  const explicit = numberOrNull(
    row?.usdcSize ??
    row?.usdc_size ??
    row?.amount_usdc ??
    row?.cashAmount
  );

  if (explicit !== null) return explicit;

  const size = numberOrNull(row?.size);
  const price = numberOrNull(row?.price);

  // Trade size is shares; shares × execution price = approximate USD.
  if (size !== null && price !== null) return size * price;

  return null;
}

function normalizeTrade(row) {
  const conditionId =
    row?.conditionId ??
    row?.condition_id ??
    row?.condition ??
    null;

  const outcome =
    row?.outcome ??
    row?.outcome_name ??
    row?.outcomeName ??
    null;

  const wallet = walletOf(row);

  return {
    title:
      row?.title ||
      row?.question ||
      row?.market ||
      row?.eventSlug ||
      row?.event_slug ||
      "Unknown market",
    side: String(row?.side || "").toUpperCase(),
    outcome: outcome || "Unknown",
    amount: amountOf(row),
    buyTime: timestampOf(row),
    conditionId: conditionId ? String(conditionId).toLowerCase() : null,
    transactionHash:
      row?.transactionHash ??
      row?.transaction_hash ??
      null,
    wallet
  };
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

  const urls = [
    `${API}/trades?${new URLSearchParams({
      user: wallet,
      limit: String(TRADE_LIMIT)
    })}`,
    `${API}/v2/trades?${new URLSearchParams({
      user: wallet,
      limit: String(TRADE_LIMIT),
      side: "BUY"
    })}`
  ];

  let lastError = null;

  for (const url of urls) {
    try {
      const response = await getJson(url);
      const rows = asArray(response);

      // An empty response is valid, but try the other feed before
      // deciding that this wallet has no recent trades.
      if (!rows.length) continue;

      return rows
        .map(normalizeTrade)
        .filter(trade =>
          trade.side === "BUY" &&
          trade.conditionId &&
          trade.outcome !== "Unknown"
        )
        .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
    } catch (error) {
      lastError = error;
    }
  }

  if (lastError) logApiError("trades", wallet, lastError);

  return [];
}

async function getOpenPositions(wallet) {
  if (!wallet) return [];

  const urls = [
    `${API}/positions?${new URLSearchParams({
      user: wallet,
      limit: "50"
    })}`,
    `${API}/v2/positions?${new URLSearchParams({
      user: wallet,
      status: "OPEN",
      limit: "50"
    })}`
  ];

  let lastError = null;

  for (const url of urls) {
    try {
      const response = await getJson(url);
      const rows = asArray(response);

      if (!rows.length) continue;

      return rows.map(row => ({
        title:
          row.title ||
          row.market ||
          row.question ||
          "Unknown market",
        conditionId:
          row.conditionId ??
          row.condition_id ??
          null,
        outcome:
          row.outcome ??
          row.outcome_name ??
          "Unknown",
        size: numberOrNull(
          row.size ??
          row.current_size ??
          row.tokens
        ),
        currentValue: numberOrNull(
          row.currentValue ??
          row.current_value ??
          row.currentValueUsdc
        ),
        initialValue: numberOrNull(
          row.initialValue ??
          row.initial_value ??
          row.entry_cost_usdc
        ),
        pnl: numberOrNull(
          row.cashPnl ??
          row.cash_pnl ??
          row.unrealized_pnl ??
          row.pnl
        ),
        endDate: row.endDate ?? row.end_date ?? null
      }));
    } catch (error) {
      lastError = error;
    }
  }

  if (lastError) logApiError("positions", wallet, lastError);

  return [];
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;

      try {
        results[index] = await callback(items[index], index);
      } catch (error) {
        console.error(`Worker failed: ${error.message}`);
        results[index] = null;
      }

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
      const outcome = String(trade.outcome || "").trim();

      if (!condition || !outcome || outcome === "Unknown") continue;

      const key = `${condition}|${outcome.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: trade.title || "Unknown market",
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

      // One vote per trader per market/outcome.
      if (market.walletsSeen.has(leader.wallet)) continue;
      market.walletsSeen.add(leader.wallet);

      market.sameSide++;

      const amount = numberOrNull(trade.amount);
      const buyTime = numberOrNull(trade.buyTime);

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

function sortByVerifiedRecord(leaders) {
  const qualified = [];
  const unqualified = [];

  for (const leader of leaders) {
    const wins = numberOrNull(leader.wins);
    const losses = numberOrNull(leader.losses);
    const resolved = numberOrNull(leader.resolvedMarkets);

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
        resolvedMarkets: null,
        recordVerified: false
      });
    }
  }

  qualified.sort((a, b) =>
    b.winRate - a.winRate ||
    b.wins - a.wins ||
    (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
  );

  return [...qualified, ...unqualified].map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function buildPeriod(period) {
  console.log(`\n========== ${period.key.toUpperCase()} ==========`);

  const leaders = await getLeaderboard(period);

  if (!leaders.length) {
    throw new Error(`${period.key} leaderboard returned no traders.`);
  }

  console.log(`Retrieved ${leaders.length} leaderboard traders.`);
  console.log("Fetching recent BUY trades and open positions...");

  const results = await mapLimit(
    leaders,
    MAX_CONCURRENCY,
    async leader => {
      const [trades, positions] = await Promise.all([
        getRecentTrades(leader.wallet),
        getOpenPositions(leader.wallet)
      ]);

      return {
        wallet: leader.wallet,
        trades,
        positions
      };
    }
  );

  const traderActiveTrades = {};
  const traderActivePositions = {};

  for (const result of results.filter(Boolean)) {
    traderActiveTrades[result.wallet] = result.trades;
    traderActivePositions[result.wallet] = result.positions;
  }

  const totalBuys = Object.values(traderActiveTrades)
    .reduce((sum, trades) => sum + trades.length, 0);

  const totalPositions = Object.values(traderActivePositions)
    .reduce((sum, positions) => sum + positions.length, 0);

  const consensus = buildConsensus(leaders, traderActiveTrades);
  const rankedLeaders = sortByVerifiedRecord(leaders);

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis:
      "Official Polymarket P&L leaderboard. Win/loss records remain unavailable until independently verified.",
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: false,
    leaders: rankedLeaders,
    traderActiveTrades,
    traderActivePositions,
    consensus,
    diagnostics: {
      leaderCount: leaders.length,
      tradersWithBuys: Object.values(traderActiveTrades)
        .filter(trades => trades.length > 0).length,
      totalBuyTrades: totalBuys,
      tradersWithPositions: Object.values(traderActivePositions)
        .filter(positions => positions.length > 0).length,
      totalPositions,
      consensusMarkets: consensus.length,
      tradeErrors: diagnostics.tradeErrors,
      positionErrors: diagnostics.positionErrors,
      apiErrors: diagnostics.apiErrors.slice(-100)
    }
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(`Wrote ${filename}`);
  console.log(`  Traders: ${leaders.length}`);
  console.log(`  BUY trades: ${totalBuys}`);
  console.log(`  Open positions: ${totalPositions}`);
  console.log(`  Consensus markets: ${consensus.length}`);
  console.log(`  Trade API errors: ${diagnostics.tradeErrors}`);
  console.log(`  Position API errors: ${diagnostics.positionErrors}`);
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
