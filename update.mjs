
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const LEADER_PAGE_SIZE = 50;
const ACTIVE_PAGE_SIZE = 500;
const TRADE_LIMIT = 50;
const REQUEST_DELAY_MS = 50;
const ACTIVE_CONCURRENCY = 8;
const TRADE_CONCURRENCY = 8;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const activeCache = new Map();

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketTop100/1.0"
        },
        signal: AbortSignal.timeout(20000)
      });

      if (response.status === 429 || response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after"));
        await sleep(
          Number.isFinite(retryAfter) && retryAfter > 0
            ? Math.min(retryAfter * 1000, 10000)
            : 500 * (attempt + 1)
        );
        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) await sleep(250 * (attempt + 1));
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

function num(value) {
  if (value === null || value === undefined || value === "") return null;
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const n = Number(value);
    return Number.isFinite(n) ? (n < 100000000000 ? n * 1000 : n) : null;
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

function normalizeLeader(row, rank) {
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

    // Do not infer wins or losses from current prices or closed positions.
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getCandidateLeaderboard(period) {
  const candidates = [];
  const seen = new Set();

  for (let offset = 0; offset < CANDIDATE_COUNT; offset += LEADER_PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(LEADER_PAGE_SIZE),
      offset: String(offset)
    });

    const rows = asArray(
      await getJson(`${API}/v1/leaderboard?${params}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeLeader(row, candidates.length + 1);
      if (!trader.wallet || seen.has(trader.wallet)) continue;

      seen.add(trader.wallet);
      candidates.push(trader);
    }

    if (rows.length < LEADER_PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return candidates.slice(0, CANDIDATE_COUNT);
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

/*
 * The legacy closed-positions endpoint does not reliably provide
 * a verified resolution outcome and resolution timestamp for each
 * weekly/monthly result. Therefore, records remain unavailable
 * until the updater uses a verified resolution-event source.
 */
function unavailableRecord() {
  return {
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

function rankByPnl(candidates) {
  return [...candidates]
    .sort((a, b) => (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity))
    .slice(0, CANDIDATE_COUNT)
    .map((trader, index) => ({
      ...trader,
      ...unavailableRecord(),
      rank: index + 1
    }));
}

function isActivePosition(position) {
  const size = num(
    position.size ??
    position.current_size ??
    position.currentSize
  );

  if (size === null || size <= 0) return false;

  const status = String(position.status || "").toUpperCase();

  if (
    position.closed === true ||
    position.redeemable === true ||
    position.mergeable === true ||
    status === "REDEEMABLE" ||
    status === "REDEEMABLE_LOST" ||
    status === "CLOSED" ||
    status === "MERGEABLE"
  ) {
    return false;
  }

  return true;
}

async function getActivePositions(wallet) {
  if (activeCache.has(wallet)) return activeCache.get(wallet);

  const promise = (async () => {
    const positions = [];
    let cursor = null;
    let pages = 0;

    do {
      const params = new URLSearchParams({
        user: wallet,
        limit: String(ACTIVE_PAGE_SIZE),
        status: "OPEN",
        sort_by: "CURRENT_VALUE",
        sort_direction: "DESC"
      });

      if (cursor) params.set("cursor", cursor);

      const response = await getJson(`${API}/v2/positions?${params}`);
      const rows = asArray(response);

      positions.push(...rows.filter(isActivePosition));

      cursor = response?.pagination?.next_cursor ?? null;
      pages++;

      if (pages >= 10) {
        console.warn(`Stopped position pagination after 10 pages for ${wallet}`);
        break;
      }
    } while (cursor);

    return positions;
  })();

  activeCache.set(wallet, promise);

  try {
    return await promise;
  } catch (error) {
    activeCache.delete(wallet);
    throw error;
  }
}

async function attachActivePositions(candidates) {
  const rows = await mapLimit(
    candidates,
    ACTIVE_CONCURRENCY,
    async trader => {
      try {
        const positions = await getActivePositions(trader.wallet);
        console.log(`Loaded active positions: ${trader.name}`);
        return [trader.wallet, positions];
      } catch (error) {
        console.error(`Positions failed for ${trader.name}: ${error.message}`);
        return [trader.wallet, []];
      }
    }
  );

  return Object.fromEntries(rows);
}

async function getRecentTrades(wallet) {
  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT)
  });

  try {
    const rows = asArray(await getJson(`${API}/trades?${params}`));

    return rows
      .filter(row => String(row.side || "").toUpperCase() === "BUY")
      .map(row => ({
        title: row.title || row.question || row.eventSlug || "Unknown market",
        side: "BUY",
        outcome: row.outcome || row.outcome_name || "Unknown",
        amount: num(row.usdcSize ?? row.amount),
        buyTime: timestampMs(row.timestamp ?? row.createdAt ?? row.time),
        conditionId: row.conditionId || row.condition_id || null,
        transactionHash: row.transactionHash || row.transaction_hash || null
      }))
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    console.error(`Recent buys failed for ${wallet}: ${error.message}`);
    return [];
  }
}

function buildConsensus(rankedCandidates, activePositions) {
  const markets = new Map();

  for (const trader of rankedCandidates) {
    const positions = activePositions[trader.wallet] || [];

    for (const position of positions) {
      const condition = String(
        position.conditionId ?? position.condition_id ?? ""
      ).toLowerCase();

      const side = String(
        position.outcome ?? position.outcome_name ?? ""
      ).trim();

      if (!condition || !side) continue;

      // Market AND outcome are both required: YES and NO never combine.
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

      const lastEvent = timestampMs(
        position.lastEventAt ?? position.last_event_at
      );

      market.sameSide++;

      market.traders.push({
        name: trader.name,
        wallet: trader.wallet,
        rank: trader.rank,
        wins: null,
        losses: null,
        winRate: null,
        resolvedMarkets: null,
        recordVerified: false,
        pnl: trader.pnl,
        volume: trader.volume,
        side,
        amount,
        buyTime: lastEvent
      });

      if (amount !== null) market.totalEntry += amount;

      if (lastEvent !== null) {
        market.newestBuy = market.newestBuy === null
          ? lastEvent
          : Math.max(market.newestBuy, lastEvent);

        market.oldestBuy = market.oldestBuy === null
          ? lastEvent
          : Math.min(market.oldestBuy, lastEvent);
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

async function buildPeriod(period, candidates, activePositions, nowMs) {
  const ranked = rankByPnl(candidates);
  const leaders = ranked.slice(0, LEADER_COUNT);

  const tradeLists = await mapLimit(
    leaders,
    TRADE_CONCURRENCY,
    async trader => [
      trader.wallet,
      await getRecentTrades(trader.wallet)
    ]
  );

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      `Polymarket ${period.api} P&L leaderboard. Verified period-specific win/loss ranking is currently unavailable.`,
    recordBasis:
      "N/A: the available data does not reliably prove each outcome and its resolution timestamp.",
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: false,
    recordWarning:
      "Win/loss records are intentionally unavailable rather than inferred from current prices or closed-position status.",
    leaders,
    traderActiveTrades: Object.fromEntries(tradeLists),
    traderActivePositions: activePositions,
    consensus: buildConsensus(ranked, activePositions)
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${leaders.length} traders, ${output.consensus.length} consensus markets`
  );
}

async function main() {
  const nowMs = Date.now();

  console.log("Loading weekly leaderboard...");
  const weekCandidates = await getCandidateLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Loading monthly leaderboard...");
  const monthCandidates = await getCandidateLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const unique = new Map();

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!unique.has(trader.wallet)) unique.set(trader.wallet, trader);
  }

  console.log(`Loading positions for ${unique.size} traders...`);
  const activePositions = await attachActivePositions([...unique.values()]);

  await buildPeriod(PERIODS[0], weekCandidates, activePositions, nowMs);
  await buildPeriod(PERIODS[1], monthCandidates, activePositions, nowMs);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
