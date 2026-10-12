
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const LEADER_PAGE_SIZE = 50;
const ACTIVE_PAGE_SIZE = 100;
const MAX_ACTIVE_PAGES = 2;
const MAX_POSITIONS_PER_TRADER = 30;
const MAX_RECENT_BUYS_PER_TRADER = 10;
const MAX_CONSENSUS_MARKETS = 200;
const MAX_CONSENSUS_MEMBERS = 100;
const REQUEST_DELAY_MS = 80;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const activeCache = new Map();
const tradeCache = new Map();

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/2.0"
        },
        signal: AbortSignal.timeout(20000)
      });

      if (!response.ok) {
        if (response.status === 429 || response.status >= 500) {
          await sleep(500 * (attempt + 1));
          throw new Error(`HTTP ${response.status}`);
        }

        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) await sleep(300 * (attempt + 1));
    }
  }

  throw lastError || new Error("API request failed");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  return [];
}

function n(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return null;
    return parsed < 100000000000 ? parsed * 1000 : parsed;
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

function normalizeLeader(row, fallbackRank) {
  const wallet = walletOf(row);

  return {
    rank: n(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      "Trader",
    pnl: n(row.pnl),
    volume: n(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false,
    activePositions: []
  };
}

async function getCandidateLeaderboard(period) {
  const candidates = [];
  const seen = new Set();

  for (
    let offset = 0;
    offset < CANDIDATE_COUNT;
    offset += LEADER_PAGE_SIZE
  ) {
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

      if (candidates.length >= CANDIDATE_COUNT) break;
    }

    if (rows.length < LEADER_PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);

    if (candidates.length >= CANDIDATE_COUNT) break;
  }

  return candidates.slice(0, CANDIDATE_COUNT);
}

async function mapLimit(items, limit, callback) {
  const output = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await callback(items[index], index);
      await sleep(REQUEST_DELAY_MS);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(limit, items.length) },
      () => worker()
    )
  );

  return output;
}

function isActivePosition(position) {
  const size = n(position.size ?? position.current_size ?? position.currentSize);

  if (size !== null && size <= 0) return false;

  if (
    position.closed === true ||
    position.redeemable === true ||
    position.status === "REDEEMABLE" ||
    position.status === "REDEEMABLE_LOST" ||
    position.mergeable === true
  ) {
    return false;
  }

  return true;
}

function compactPosition(position) {
  return {
    conditionId: position.conditionId ?? position.condition_id ?? null,
    title: position.title || position.question || "Unknown market",
    outcome: position.outcome || position.outcome_name || null,
    size: n(position.size ?? position.current_size),
    currentValue: n(position.currentValue ?? position.current_value),
    initialValue: n(position.initialValue ?? position.initial_value),
    lastEventAt: timestampMs(
      position.lastEventAt ?? position.last_event_at
    ),
    endDate: position.endDate ?? position.end_date ?? null
  };
}

async function getActivePositions(wallet) {
  if (activeCache.has(wallet)) return activeCache.get(wallet);

  const promise = (async () => {
    const collected = [];
    let cursor = null;

    for (let page = 0; page < MAX_ACTIVE_PAGES; page++) {
      const params = new URLSearchParams({
        user: wallet,
        limit: String(ACTIVE_PAGE_SIZE),
        sort_by: "CURRENT_VALUE",
        sort_direction: "DESC"
      });

      if (cursor) params.set("cursor", cursor);

      const response = await getJson(`${API}/v2/positions?${params}`);
      const rows = asArray(response);

      collected.push(
        ...rows.filter(isActivePosition).slice(
          0,
          MAX_POSITIONS_PER_TRADER - collected.length
        )
      );

      cursor = response?.pagination?.next_cursor ?? null;

      if (
        !cursor ||
        collected.length >= MAX_POSITIONS_PER_TRADER ||
        rows.length === 0
      ) {
        break;
      }
    }

    return collected
      .slice(0, MAX_POSITIONS_PER_TRADER)
      .map(compactPosition);
  })();

  activeCache.set(wallet, promise);

  try {
    return await promise;
  } catch (error) {
    activeCache.delete(wallet);
    console.error(`Positions failed for ${wallet}: ${error.message}`);
    return [];
  }
}

async function attachActivePositions(candidates) {
  let done = 0;

  const pairs = await mapLimit(candidates, 8, async trader => {
    const positions = await getActivePositions(trader.wallet);
    done++;

    if (done % 50 === 0 || done === candidates.length) {
      console.log(`Active positions: ${done}/${candidates.length}`);
    }

    return [trader.wallet, positions];
  });

  return Object.fromEntries(pairs);
}

async function getRecentBuys(wallet) {
  if (tradeCache.has(wallet)) return tradeCache.get(wallet);

  const promise = (async () => {
    const params = new URLSearchParams({
      user: wallet,
      limit: "50"
    });

    try {
      const rows = asArray(await getJson(`${API}/trades?${params}`));

      return rows
        .filter(row => String(row.side || "").toUpperCase() === "BUY")
        .map(row => ({
          title: row.title || row.question || "Unknown market",
          side: "BUY",
          outcome: row.outcome || row.outcome_name || "Unknown",
          amount: n(row.usdcSize ?? row.amount ?? row.size),
          timestamp: timestampMs(row.timestamp ?? row.createdAt ?? row.time),
          conditionId: row.conditionId || row.condition_id || null
        }))
        .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
        .slice(0, MAX_RECENT_BUYS_PER_TRADER);
    } catch (error) {
      console.error(`Trades failed for ${wallet}: ${error.message}`);
      return [];
    }
  })();

  tradeCache.set(wallet, promise);
  return promise;
}

function buildConsensus(candidates, positionsByWallet) {
  const groups = new Map();

  for (const trader of candidates) {
    const positions = positionsByWallet[trader.wallet] || [];

    for (const position of positions) {
      const condition = String(position.conditionId || "").toLowerCase();
      const side = String(position.outcome || "").trim();

      if (!condition || !side) continue;

      // The condition and outcome together prevent Yes and No from mixing.
      const key = `${condition}|${side.toLowerCase()}`;

      if (!groups.has(key)) {
        groups.set(key, {
          conditionId: condition,
          title: position.title,
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }

      const market = groups.get(key);
      if (market.walletsSeen.has(trader.wallet)) continue;

      market.walletsSeen.add(trader.wallet);
      market.sameSide++;

      const amount = n(position.currentValue ?? position.initialValue) ?? 0;
      const buyTime = timestampMs(position.lastEventAt);

      market.totalEntry += amount;
      if (buyTime !== null) {
        market.newestBuy = market.newestBuy === null
          ? buyTime
          : Math.max(market.newestBuy, buyTime);
      }

      if (market.traders.length < MAX_CONSENSUS_MEMBERS) {
        market.traders.push({
          name: trader.name,
          wallet: trader.wallet,
          rank: trader.rank,
          pnl: trader.pnl,
          volume: trader.volume,
          wins: null,
          losses: null,
          winRate: null,
          recordVerified: false,
          side,
          amount,
          buyTime
        });
      }
    }
  }

  return [...groups.values()]
    .filter(market => market.sameSide >= 2)
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    )
    .slice(0, MAX_CONSENSUS_MARKETS)
    .map(market => {
      const { walletsSeen, ...clean } = market;
      return clean;
    });
}

function compactLeader(trader, positions) {
  return {
    rank: trader.rank,
    name: trader.name,
    wallet: trader.wallet,
    pnl: trader.pnl,
    volume: trader.volume,
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false,
    activePositions: (positions || []).slice(0, MAX_POSITIONS_PER_TRADER)
  };
}

async function buildPeriod(period, candidates, positionsByWallet) {
  const ranked = [...candidates]
    .sort((a, b) => (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity))
    .map((trader, index) => ({ ...trader, rank: index + 1 }));

  const leaders = ranked
    .slice(0, LEADER_COUNT)
    .map(trader => compactLeader(
      trader,
      positionsByWallet[trader.wallet] || []
    ));

  // Consensus comes from up to the top 1,000 candidates, but stores only
  // a limited number of markets and member records.
  const consensus = buildConsensus(ranked, positionsByWallet);

  // Recent buys are collected for the top 100 only.
  const recentPairs = await mapLimit(leaders, 8, async trader => {
    const buys = await getRecentBuys(trader.wallet);

    return buys.map(buy => ({
      ...buy,
      traderName: trader.name,
      wallet: trader.wallet,
      rank: trader.rank
    }));
  });

  const recentBuys = recentPairs
    .flat()
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
    .slice(0, 500);

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis: "Period P&L leaderboard; verified win-loss ranking is not available",
    recordBasis: "Verified resolved-market records are unavailable; no records are inferred",
    candidateCount: candidates.length,
    recordRankingEnabled: false,
    recordWarning: "Win-loss records are N/A until verified resolution data is available.",
    leaders,
    recentBuys,
    consensus
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;
  const json = JSON.stringify(output);

  // Fail safely instead of accidentally committing huge files.
  const sizeMB = Buffer.byteLength(json, "utf8") / (1024 * 1024);
  if (sizeMB > 5) {
    throw new Error(
      `${filename} is ${sizeMB.toFixed(2)} MB. Refusing to write oversized output.`
    );
  }

  await fs.writeFile(tempFilename, json, "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${sizeMB.toFixed(2)} MB, ` +
    `${leaders.length} leaders, ${recentBuys.length} buys, ` +
    `${consensus.length} consensus markets`
  );
}

async function main() {
  console.log("Loading weekly candidates...");
  const weekCandidates = await getCandidateLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Loading monthly candidates...");
  const monthCandidates = await getCandidateLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const combined = new Map();

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!combined.has(trader.wallet)) combined.set(trader.wallet, trader);
  }

  const uniqueCandidates = [...combined.values()];
  console.log(`Loading limited positions for ${uniqueCandidates.length} traders...`);

  const positionsByWallet = await attachActivePositions(uniqueCandidates);

  await buildPeriod(PERIODS[0], weekCandidates, positionsByWallet);
  await buildPeriod(PERIODS[1], monthCandidates, positionsByWallet);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
