
import fs from "node:fs/promises";
import path from "node:path";

const API = "https://data-api.polymarket.com";
const CACHE_FILE = "api-cache.json";
const CACHE_TTL = 6 * 60 * 60 * 1000;
const MAX_FILE_SIZE = 5 * 1024 * 1024;

const CONFIG = {
  candidateCount: 1000,
  leaderboardPageSize: 50,
  leaderCount: 100,
  requestDelayMs: 350,
  concurrency: 2,
  maxRetries: 6,
  positionPages: 3,
  positionPageSize: 500,
  closedPositionPages: 3,
  closedPositionPageSize: 50,
  tradeLimit: 30,
  minResolvedMarkets: 10
};

const PERIODS = [
  { key: "week", apiPeriod: "WEEK", days: 7 },
  { key: "month", apiPeriod: "MONTH", days: 30 }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function number(value) {
  if (value === null || value === undefined || value === "") return null;
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const result = Number(value);
    return result < 1e12 ? result * 1000 : result;
  }

  const result = Date.parse(value);
  return Number.isFinite(result) ? result : null;
}

function rowsOf(response) {
  if (Array.isArray(response)) return response;
  if (Array.isArray(response?.data)) return response.data;
  if (Array.isArray(response?.results)) return response.results;
  return [];
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
    row?.proxy_wallet ??
    row?.address ??
    row?.wallet ??
    ""
  ).toLowerCase();
}

let cache = { version: 1, entries: {} };
let requestQueue = Promise.resolve();
let lastRequestAt = 0;

async function loadCache() {
  try {
    const raw = await fs.readFile(CACHE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed?.entries && typeof parsed.entries === "object") {
      cache = parsed;
    }
  } catch {
    console.log("No reusable API cache found; starting a fresh cache.");
  }
}

async function saveCache() {
  const now = Date.now();

  for (const [key, item] of Object.entries(cache.entries)) {
    if (!item || now - item.savedAt > 7 * 24 * 60 * 60 * 1000) {
      delete cache.entries[key];
    }
  }

  const raw = JSON.stringify(cache);

  if (Buffer.byteLength(raw, "utf8") <= MAX_FILE_SIZE) {
    const temp = `${CACHE_FILE}.tmp`;
    await fs.writeFile(temp, raw, "utf8");
    await fs.rename(temp, CACHE_FILE);
  } else {
    console.warn("Cache exceeded 5 MB; leaving the previous cache intact.");
  }
}

function cached(key, ttl = CACHE_TTL) {
  const entry = cache.entries[key];
  if (!entry || Date.now() - entry.savedAt > ttl) return undefined;
  return entry.value;
}

function putCache(key, value) {
  cache.entries[key] = { savedAt: Date.now(), value };
}

async function requestJson(url) {
  // Serialize outgoing requests so multiple workers cannot create request bursts.
  const run = requestQueue.then(async () => {
    const wait = CONFIG.requestDelayMs - (Date.now() - lastRequestAt);
    if (wait > 0) await sleep(wait);
    lastRequestAt = Date.now();

    let lastError;

    for (let attempt = 0; attempt < CONFIG.maxRetries; attempt++) {
      try {
        const response = await fetch(url, {
          headers: {
            Accept: "application/json",
            "User-Agent": "PolymarketConsensusScanner/3.0"
          },
          signal: AbortSignal.timeout(30000)
        });

        if (response.ok) return await response.json();

        const body = await response.text().catch(() => "");
        const status = response.status;

        if (status !== 429 && status !== 500 && status !== 502 &&
            status !== 503 && status !== 504) {
          throw new Error(`HTTP ${status}: ${body.slice(0, 200)}`);
        }

        const retryAfter = Number(response.headers.get("retry-after"));
        const exponential = Math.min(60000, 2000 * (2 ** attempt));
        const waitMs = retryAfter > 0
          ? Math.min(120000, retryAfter * 1000)
          : exponential;

        lastError = new Error(`HTTP ${status}`);
        console.warn(
          `${lastError.message}; retry ${attempt + 1}/${CONFIG.maxRetries} in ${waitMs}ms`
        );
        await sleep(waitMs);
      } catch (error) {
        lastError = error;

        if (/^HTTP 4(?!29)/.test(String(error.message))) {
          throw error;
        }

        if (attempt < CONFIG.maxRetries - 1) {
          await sleep(Math.min(60000, 2000 * (2 ** attempt)));
        }
      }
    }

    throw lastError || new Error("API request failed after retries");
  });

  requestQueue = run.catch(() => {});
  return run;
}

async function getCachedJson(key, url, ttl = CACHE_TTL) {
  const hit = cached(key, ttl);
  if (hit !== undefined) return hit;

  const result = await requestJson(url);
  putCache(key, result);
  return result;
}

async function mapLimit(items, concurrency, fn) {
  const result = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor++;
      if (index >= items.length) return;
      result[index] = await fn(items[index], index);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(concurrency, items.length) },
      () => worker()
    )
  );

  return result;
}

function normalizeTrader(row, index) {
  const wallet = walletOf(row);

  return {
    rank: number(row.rank) ?? index + 1,
    wallet,
    name: row.userName || row.user_name || row.username ||
      row.name || row.pseudonym ||
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: number(row.pnl),
    volume: number(row.vol ?? row.volume),
    wins: null,
    losses: null,
    resolvedMarkets: null,
    winRate: null,
    recordVerified: false
  };
}

async function getLeaderboard(period) {
  const candidates = new Map();

  for (
    let offset = 0;
    offset < CONFIG.candidateCount;
    offset += CONFIG.leaderboardPageSize
  ) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.apiPeriod,
      orderBy: "PNL",
      limit: String(CONFIG.leaderboardPageSize),
      offset: String(offset)
    });

    const response = await requestJson(
      `${API}/v1/leaderboard?${params}`
    );

    const rows = rowsOf(response);
    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeTrader(row, candidates.size);
      if (trader.wallet) candidates.set(trader.wallet, trader);
    }

    if (rows.length < CONFIG.leaderboardPageSize) break;
  }

  return [...candidates.values()].slice(0, CONFIG.candidateCount);
}

async function getPositions(user) {
  const key = `positions:${user}`;
  const hit = cached(key);
  if (hit !== undefined) return hit;

  const output = [];
  let cursor = null;

  for (let page = 0; page < CONFIG.positionPages; page++) {
    const params = new URLSearchParams({
      user,
      limit: String(CONFIG.positionPageSize),
      status: "OPEN",
      sort_by: "CURRENT_VALUE",
      sort_direction: "DESC",
      filter_type: "CASH",
      filter_amount: "1"
    });

    if (cursor) params.set("cursor", cursor);

    const response = await requestJson(`${API}/v2/positions?${params}`);
    output.push(...rowsOf(response));

    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor) break;
  }

  const now = Date.now();
  const active = output.filter(position => {
    const size = number(position.current_size ?? position.size);
    const end = timestamp(position.end_date);

    return size !== null &&
      size > 0 &&
      position.redeemable !== true &&
      position.mergeable !== true &&
      position.status !== "REDEEMABLE_LOST" &&
      !(end !== null && end < now);
  });

  putCache(key, active);
  return active;
}

async function getClosedPositions(user) {
  const key = `closed:${user}`;
  const hit = cached(key);
  if (hit !== undefined) return hit;

  const output = [];

  for (let page = 0; page < CONFIG.closedPositionPages; page++) {
    const params = new URLSearchParams({
      user,
      limit: String(CONFIG.closedPositionPageSize),
      offset: String(page * CONFIG.closedPositionPageSize),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const response = await requestJson(
      `${API}/closed-positions?${params}`
    );

    const rows = rowsOf(response);
    output.push(...rows);

    if (rows.length < CONFIG.closedPositionPageSize) break;
  }

  putCache(key, output);
  return output;
}

function calculateRecord(positions, period, now) {
  const start = now - period.days * 86400000;
  const seen = new Set();
  let wins = 0;
  let losses = 0;

  for (const position of positions) {
    const time = timestamp(
      position.timestamp ??
      position.closedAt ??
      position.closed_at
    );

    if (time === null || time < start || time > now) continue;

    const price = number(
      position.curPrice ??
      position.cur_price ??
      position.settlementPrice
    );

    if (price === null) continue;
    if (price < 0.99 && price > 0.01) continue;

    const condition = String(
      position.conditionId ?? position.condition_id ?? ""
    );
    const outcome = String(
      position.outcomeIndex ??
      position.outcome_index ??
      position.outcome ??
      ""
    );

    if (!condition) continue;

    const key = `${condition}|${outcome}`;
    if (seen.has(key)) continue;
    seen.add(key);

    if (price >= 0.99) wins++;
    else losses++;
  }

  const total = wins + losses;

  return {
    wins,
    losses,
    resolvedMarkets: total,
    winRate: total ? (wins / total) * 100 : null,
    recordVerified: total > 0
  };
}

async function attachRecords(traders, period, now) {
  return mapLimit(traders, CONFIG.concurrency, async trader => {
    const positions = await getClosedPositions(trader.wallet);
    return {
      ...trader,
      ...calculateRecord(positions, period, now)
    };
  });
}

function rankTraders(traders) {
  return [...traders]
    .sort((a, b) => {
      const aq = a.recordVerified &&
        a.resolvedMarkets >= CONFIG.minResolvedMarkets;
      const bq = b.recordVerified &&
        b.resolvedMarkets >= CONFIG.minResolvedMarkets;

      if (aq !== bq) return aq ? -1 : 1;

      if (aq && bq) {
        const rateDiff = (b.winRate ?? -1) - (a.winRate ?? -1);
        if (rateDiff) return rateDiff;

        const winsDiff = (b.wins ?? 0) - (a.wins ?? 0);
        if (winsDiff) return winsDiff;
      }

      return (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity);
    })
    .map((trader, index) => ({ ...trader, rank: index + 1 }));
}

async function getTrades(user) {
  const key = `trades:${user}`;
  const hit = cached(key);
  if (hit !== undefined) return hit;

  const params = new URLSearchParams({
    user,
    limit: String(CONFIG.tradeLimit)
  });

  const response = await requestJson(`${API}/trades?${params}`);

  const trades = rowsOf(response)
    .filter(trade => String(trade.side || "").toUpperCase() === "BUY")
    .map(trade => ({
      title: trade.title || trade.question || "Unknown market",
      side: "BUY",
      outcome: trade.outcome || trade.outcome_name || "Unknown",
      amount: number(trade.usdcSize ?? trade.amount ?? trade.size),
      buyTime: timestamp(trade.timestamp ?? trade.createdAt ?? trade.time),
      conditionId: trade.conditionId || trade.condition_id || null
    }))
    .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));

  putCache(key, trades);
  return trades;
}

function buildConsensus(traders, activeByWallet) {
  const marketMap = new Map();

  for (const trader of traders) {
    for (const position of activeByWallet[trader.wallet] || []) {
      const condition = String(
        position.condition_id ?? position.conditionId ?? ""
      ).toLowerCase();

      const side = String(
        position.outcome ?? position.outcome_name ?? ""
      ).trim();

      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!marketMap.has(key)) {
        marketMap.set(key, {
          conditionId: condition,
          title: position.title || position.name || "Unknown market",
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          _seen: new Set()
        });
      }

      const market = marketMap.get(key);
      if (market._seen.has(trader.wallet)) continue;
      market._seen.add(trader.wallet);

      market.sameSide++;

      const amount = number(
        position.current_value ??
        position.currentValue ??
        position.entry_cost_usdc ??
        position.initialValue
      );

      const buyTime = timestamp(
        position.last_event_at ??
        position.lastEventAt ??
        position.first_entry_at
      );

      if (amount !== null) market.totalEntry += amount;

      if (buyTime !== null) {
        market.newestBuy = Math.max(market.newestBuy || 0, buyTime);
      }

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
    }
  }

  return [...marketMap.values()]
    .filter(market => market.sameSide >= 2)
    .map(market => {
      delete market._seen;
      return market;
    })
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function buildPeriod(period, candidates, activeByWallet, now) {
  console.log(`Building ${period.key} data...`);

  const withRecords = await attachRecords(candidates, period, now);
  const ranked = rankTraders(withRecords);
  const leaders = ranked.slice(0, CONFIG.leaderCount);

  const tradePairs = await mapLimit(
    leaders,
    CONFIG.concurrency,
    async trader => [trader.wallet, await getTrades(trader.wallet)]
  );

  const traderActiveTrades = Object.fromEntries(tradePairs);

  const consensus = buildConsensus(ranked, activeByWallet);

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: CONFIG.minResolvedMarkets,
    recordRankingEnabled: true,
    recordWarning:
      "Closed-position settlement-like prices are an estimate, not a complete ledger of all resolved trades.",
    leaders,
    traderActiveTrades,
    traderActivePositions: activeByWallet,
    consensus
  };

  const serialized = JSON.stringify(output);
  const bytes = Buffer.byteLength(serialized, "utf8");

  if (bytes > MAX_FILE_SIZE) {
    throw new Error(
      `${period.key}.json is ${(bytes / 1024 / 1024).toFixed(2)} MB; limit is 5 MB.`
    );
  }

  const temp = `${period.key}.json.tmp`;
  await fs.writeFile(temp, serialized, "utf8");
  await fs.rename(temp, `${period.key}.json`);

  console.log(
    `${period.key}.json saved: ${leaders.length} leaders, ` +
    `${consensus.length} consensus markets, ${(bytes / 1024).toFixed(0)} KB`
  );
}

async function main() {
  const now = Date.now();
  await loadCache();

  console.log("Fetching weekly candidate leaderboard...");
  const weekCandidates = await getLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Fetching monthly candidate leaderboard...");
  const monthCandidates = await getLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const combined = new Map();

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (trader.wallet) combined.set(trader.wallet, trader);
  }

  const allTraders = [...combined.values()];
  const activeByWallet = {};

  console.log(`Fetching active positions for ${allTraders.length} traders...`);

  await mapLimit(allTraders, CONFIG.concurrency, async trader => {
    activeByWallet[trader.wallet] = await getPositions(trader.wallet);
  });

  await buildPeriod(PERIODS[0], weekCandidates, activeByWallet, now);
  await buildPeriod(PERIODS[1], monthCandidates, activeByWallet, now);

  await saveCache();

  console.log("Polymarket tracker update completed successfully.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
