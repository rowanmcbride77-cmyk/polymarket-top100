
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "WEEK", days: 7 },
  { key: "month", api: "MONTH", days: 30 }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_LIMIT = 500;
const TRADE_LIMIT = 50;
const MIN_RESOLVED_FOR_RANKING = 10;
const CONCURRENCY = 6;
const DELAY_MS = 200;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/1.0"
        },
        signal: AbortSignal.timeout(20000)
      });

      if (response.status === 429 || response.status >= 500) {
        const retry = Number(response.headers.get("retry-after"));
        await sleep(
          retry > 0 ? Math.min(retry * 1000, 15000) : 1000 * (attempt + 1)
        );
        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) await sleep(500 * (attempt + 1));
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
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const n = Number(value);
    return n < 100000000000 ? n * 1000 : n;
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

function normalizeLeader(row, index) {
  const wallet = walletOf(row);

  return {
    rank: num(row.rank) ?? index + 1,
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

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      results[index] = await callback(items[index], index);
      await sleep(DELAY_MS);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker())
  );

  return results;
}

async function getCandidates(period) {
  const candidates = [];
  const seen = new Set();

  for (let offset = 0; offset < CANDIDATE_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const rows = asArray(await getJson(`${API}/v1/leaderboard?${params}`));
    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeLeader(row, candidates.length);
      if (!trader.wallet || seen.has(trader.wallet)) continue;
      seen.add(trader.wallet);
      candidates.push(trader);
    }

    if (rows.length < PAGE_SIZE) break;
  }

  return candidates.slice(0, CANDIDATE_COUNT);
}

/*
 * Closed-position price data alone is not sufficient to verify
 * each resolution time and outcome reliably. Do not invent records.
 */
async function attachRecords(candidates) {
  return candidates.map(trader => ({
    ...trader,
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  }));
}

/*
 * Until verified win/loss records are available, order by P&L.
 * This is explicitly labelled in the JSON output.
 */
function rankCandidates(candidates) {
  return [...candidates]
    .sort((a, b) => (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity))
    .map((trader, index) => ({ ...trader, rank: index + 1 }));
}

function isActivePosition(position, nowMs) {
  const size = num(
    position.size ?? position.current_size ?? position.currentSize
  );

  if (size === null || size <= 0) return false;
  if (position.closed === true) return false;
  if (position.redeemable === true) return false;
  if (position.mergeable === true) return false;

  const endDate = position.endDate ?? position.end_date ?? position.endDateIso;
  if (endDate) {
    const endMs = timestampMs(endDate);
    if (endMs !== null && endMs < nowMs) return false;
  }

  return true;
}

async function getActivePositions(wallet, nowMs) {
  const positions = [];

  // V2 positions use cursor pagination.
  let cursor = null;
  let pages = 0;

  do {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(POSITION_LIMIT),
      status: "OPEN",
      sort_by: "CURRENT_VALUE",
      sort_direction: "DESC"
    });

    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${params}`);
    const rows = asArray(response);

    positions.push(...rows.filter(p => isActivePosition(p, nowMs)));

    cursor = response?.pagination?.next_cursor ?? null;
    pages++;

    if (!rows.length) break;
  } while (cursor && pages < 10);

  return positions;
}

async function attachActivePositions(candidates, nowMs) {
  const rows = await mapLimit(candidates, CONCURRENCY, async trader => {
    try {
      const positions = await getActivePositions(trader.wallet, nowMs);
      console.log(`Positions loaded: ${trader.name} (${positions.length})`);
      return [trader.wallet, positions];
    } catch (error) {
      console.error(`Positions failed for ${trader.name}: ${error.message}`);
      return [trader.wallet, []];
    }
  });

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
        amount: num(row.usdcSize ?? row.amount ?? row.size),
        buyTime: timestampMs(row.timestamp ?? row.createdAt ?? row.time),
        conditionId: row.conditionId || row.condition_id || null
      }))
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    console.error(`Trades failed for ${wallet}: ${error.message}`);
    return [];
  }
}

function buildConsensus(candidates, activePositions) {
  const markets = new Map();

  for (const trader of candidates) {
    for (const position of activePositions[trader.wallet] || []) {
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
      market.sameSide++;

      const amount = num(
        position.currentValue ??
        position.current_value ??
        position.initialValue ??
        position.initial_value
      );

      const buyTime = timestampMs(
        position.lastEventAt ?? position.last_event_at
      );

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
          market.newestBuy === null ? buyTime : Math.max(market.newestBuy, buyTime);
        market.oldestBuy =
          market.oldestBuy === null ? buyTime : Math.min(market.oldestBuy, buyTime);
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
  const withRecords = await attachRecords(candidates);
  const ranked = rankCandidates(withRecords);
  const leaders = ranked.slice(0, LEADER_COUNT);

  const tradeRows = await mapLimit(leaders, CONCURRENCY, async trader => [
    trader.wallet,
    await getRecentTrades(trader.wallet)
  ]);

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      `P&L leaderboard for ${period.key}; verified period-specific win/loss ranking is unavailable`,
    recordBasis:
      "N/A: verified resolution outcomes and timestamps are not available from the current record source",
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: false,
    recordWarning:
      "Win/loss records are unavailable rather than inferred from open positions or current prices.",
    leaders,
    traderActiveTrades: Object.fromEntries(tradeRows),
    traderActivePositions: activePositions,
    consensus: buildConsensus(ranked, activePositions)
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${leaders.length} leaders, ${output.consensus.length} consensus markets`
  );
}

async function main() {
  const nowMs = Date.now();

  console.log("Loading weekly leaderboard...");
  const weekCandidates = await getCandidates(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Loading monthly leaderboard...");
  const monthCandidates = await getCandidates(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const unique = new Map();
  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!unique.has(trader.wallet)) unique.set(trader.wallet, trader);
  }

  console.log(`Loading positions for ${unique.size} unique traders...`);
  const activePositions = await attachActivePositions([...unique.values()], nowMs);

  await buildPeriod(PERIODS[0], weekCandidates, activePositions, nowMs);
  await buildPeriod(PERIODS[1], monthCandidates, activePositions, nowMs);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
