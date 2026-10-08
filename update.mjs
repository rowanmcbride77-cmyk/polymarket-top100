import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const DATA = path.join(ROOT, "data");
const API = "https://data-api.polymarket.com";

const HOURS = 12;
const TARGET = 10000;
const MIN_DECIDED = 10;
const NOW = () => Math.floor(Date.now() / 1000);
const sleep = ms => new Promise(r => setTimeout(r, ms));

function n(...values) {
  for (const v of values) {
    if (v === undefined || v === null || v === "") continue;
    const x = Number(v);
    if (Number.isFinite(x)) return x;
  }
  return 0;
}

function addr(r) {
  return String(
    r.proxy_wallet || r.proxyWallet || r.user ||
    r.address || r.wallet || ""
  ).toLowerCase();
}

function traderName(r) {
  return r.user_name || r.userName || r.name ||
    r.username || r.pseudonym || "Unknown";
}

function condition(r) {
  return String(r.condition_id || r.conditionId || r.condition || "");
}

function token(r) {
  return String(r.token_id || r.tokenId || r.asset || "");
}

function outcome(r) {
  return String(
    r.outcome || r.outcome_name || r.outcomeName || ""
  ).trim().toUpperCase();
}

function timestamp(r) {
  const value = r.timestamp ?? r.last_event_at ?? r.lastEventAt;
  if (value === undefined || value === null) return 0;

  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    return numeric > 1e12 ? Math.floor(numeric / 1000) : Math.floor(numeric);
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
}

function dollars(r) {
  const direct = r.usdc_size ?? r.usdcSize;
  if (direct !== undefined && direct !== null) {
    const value = Number(direct);
    if (Number.isFinite(value)) return value;
  }
  return n(r.size, r.amount) * n(r.price);
}

function marketTitle(r) {
  return r.title || r.question || r.market_title ||
    r.marketTitle || r.name || "Unknown market";
}

async function getJSON(url, attempts = 6) {
  let lastError;

  for (let i = 0; i < attempts; i++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Consensus-Scanner"
        },
        signal: AbortSignal.timeout(30000)
      });

      if (response.status === 429 || response.status === 503) {
        const retry = Number(response.headers.get("retry-after"));
        await sleep(Number.isFinite(retry) && retry > 0
          ? retry * 1000
          : 1500 * (i + 1));
        continue;
      }

      if (!response.ok) {
        const body = await response.text();
        throw new Error(
          `HTTP ${response.status}: ${body.slice(0, 250)}`
        );
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (i < attempts - 1) await sleep(800 * (i + 1));
    }
  }

  throw lastError;
}

function rowsFrom(result) {
  if (Array.isArray(result)) return result;
  if (Array.isArray(result?.data)) return result.data;
  return [];
}

async function cursorPages(endpoint, params, maxPages = 30) {
  const output = [];
  let cursor = "";

  for (let page = 0; page < maxPages; page++) {
    const query = new URLSearchParams();

    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== "") {
        query.set(key, String(value));
      }
    }

    if (cursor) query.set("cursor", cursor);

    const result = await getJSON(`${API}${endpoint}?${query}`);
    output.push(...rowsFrom(result));

    const pagination = result?.pagination;
    if (!pagination?.has_more || !pagination?.next_cursor) break;

    cursor = pagination.next_cursor;
    await sleep(50);
  }

  return output;
}

async function mapLimit(items, concurrency, fn) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;

      try {
        results[index] = await fn(items[index], index);
      } catch (error) {
        console.error(error?.message || error);
        results[index] = null;
      }
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(concurrency, items.length) },
      () => worker()
    )
  );

  return results;
}

/* ---------------------------------------------------------
   WEEKLY LEADERBOARD CANDIDATES
   --------------------------------------------------------- */

async function leaderboard(sortBy) {
  return cursorPages(
    "/v2/leaderboard",
    {
      category: "OVERALL",
      timePeriod: "WEEK",
      sortBy,
      limit: 1000
    },
    20
  );
}

async function discoverCandidates() {
  console.log("Collecting weekly leaderboard candidates...");

  const [pnlRows, volumeRows] = await Promise.all([
    leaderboard("PNL"),
    leaderboard("VOLUME")
  ]);

  const candidates = new Map();

  for (const row of [...pnlRows, ...volumeRows]) {
    const address = addr(row);
    if (!address) continue;

    const previous = candidates.get(address);

    if (!previous) {
      candidates.set(address, {
        address,
        name: traderName(row),
        pnl: n(row.pnl),
        volume: n(row.volume, row.vol)
      });
    } else {
      if (previous.name === "Unknown") previous.name = traderName(row);
      previous.pnl = Math.max(previous.pnl, n(row.pnl));
      previous.volume = Math.max(previous.volume, n(row.volume, row.vol));
    }
  }

  return [...candidates.values()];
}

/* ---------------------------------------------------------
   CLOSED POSITIONS — USE THE DEDICATED ENDPOINT
   --------------------------------------------------------- */

async function getClosedPositions(address) {
  const output = [];
  const pageSize = 50;
  const maxPages = 20;

  for (let page = 0; page < maxPages; page++) {
    const query = new URLSearchParams({
      user: address,
      limit: String(pageSize),
      offset: String(page * pageSize),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const result = await getJSON(
      `${API}/closed-positions?${query}`
    );

    const batch = rowsFrom(result);
    if (!batch.length) break;

    output.push(...batch);

    if (batch.length < pageSize) break;

    const oldest = Math.min(
      ...batch.map(timestamp).filter(Boolean)
    );

    const cutoff = NOW() - 90 * 86400;
    if (oldest && oldest < cutoff) break;

    await sleep(50);
  }

  return output;
}

function recordFromClosedPositions(positions, periodDays = 7) {
  const cutoff = NOW() - periodDays * 86400;
  const pnlByMarket = new Map();

  for (const position of positions) {
    const when = timestamp(position);

    // Only count positions resolved/closed in the selected period.
    if (!when || when < cutoff) continue;

    const market = condition(position);
    if (!market) continue;

    const pnl = n(position.realized_pnl, position.realizedPnl);

    pnlByMarket.set(
      market,
      (pnlByMarket.get(market) || 0) + pnl
    );
  }

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  for (const value of pnlByMarket.values()) {
    pnl += value;

    if (value > 0.000001) wins++;
    else if (value < -0.000001) losses++;
  }

  const decided = wins + losses;
  if (decided < MIN_DECIDED) return null;

  return {
    wins,
    losses,
    decided,
    pnl,
    winRate: wins / decided * 100
  };
}

async function buildWeekly(candidates) {
  let finished = 0;

  console.log(`Calculating weekly records for ${candidates.length} candidates...`);

  const results = await mapLimit(candidates, 12, async candidate => {
    const closed = await getClosedPositions(candidate.address);
    const record = recordFromClosedPositions(closed, 7);

    finished++;
    if (finished % 100 === 0) {
      console.log(`Weekly records: ${finished}/${candidates.length}`);
    }

    if (!record) return null;

    return {
      ...candidate,
      ...record
    };
  });

  const qualified = results.filter(Boolean);

  qualified.sort((a, b) =>
    b.winRate - a.winRate ||
    b.wins - a.wins ||
    b.decided - a.decided ||
    b.pnl - a.pnl
  );

  return qualified.slice(0, TARGET).map((row, i) => ({
    ...row,
    rank: i + 1
  }));
}

/* ---------------------------------------------------------
   RECENT GLOBAL BUY TRADES
   --------------------------------------------------------- */

async function getRecentGlobalBuys() {
  const cutoff = NOW() - HOURS * 3600;

  const trades = await cursorPages(
    "/v2/trades",
    { limit: 1000 },
    100
  );

  return trades.filter(trade =>
    String(trade.side || "").toUpperCase() === "BUY" &&
    timestamp(trade) >= cutoff
  );
}

/* ---------------------------------------------------------
   OPEN MARKET HOLDERS
   --------------------------------------------------------- */

async function getMarketPositions(conditionId) {
  return cursorPages(
    "/v2/positions",
    {
      condition: conditionId,
      status: "OPEN",
      limit: 1000
    },
    10
  );
}

function normalizedSide(position) {
  const label = outcome(position);
  if (label === "YES" || label === "NO") return label;

  const index = n(position.outcome_index, position.outcomeIndex);
  if (index === 0) return "YES";
  if (index === 1) return "NO";

  return "";
}

async function buildRecentActive(globalTrades, weekly) {
  const weeklyByWallet = new Map(
    weekly.map(trader => [trader.address.toLowerCase(), trader])
  );

  const candidates = new Map();

  for (const trade of globalTrades) {
    const address = addr(trade);
    const market = condition(trade);
    const tokenId = token(trade);

    if (!address || !market || !weeklyByWallet.has(address)) continue;

    const key = `${market}|${address}|${tokenId || outcome(trade)}`;

    if (!candidates.has(key)) {
      candidates.set(key, {
        address,
        market,
        token: tokenId,
        outcome: outcome(trade),
        title: marketTitle(trade),
        amount: 0,
        buyTime: 0
      });
    }

    const item = candidates.get(key);
    item.amount += dollars(trade);

    if (timestamp(trade) > item.buyTime) {
      item.buyTime = timestamp(trade);
      item.title = marketTitle(trade);
    }
  }

  const candidateRows = [...candidates.values()];
  const marketIds = [...new Set(candidateRows.map(row => row.market))];

  console.log(`Recent BUYs from weekly traders: ${candidateRows.length}`);
  console.log(`Checking ${marketIds.length} markets for current open positions...`);

  const marketResults = await mapLimit(marketIds, 8, async marketId => ({
    marketId,
    positions: await getMarketPositions(marketId)
  }));

  const holdersByMarket = new Map();

  for (const result of marketResults.filter(Boolean)) {
    const holders = new Map();

    for (const position of result.positions) {
      const address = addr(position);
      if (!address) continue;

      const side = normalizedSide(position);
      const tokenId = token(position);

      if (side) holders.set(`${address}|${side}`, position);
      if (tokenId) holders.set(`${address}|TOKEN:${tokenId}`, position);
    }

    holdersByMarket.set(result.marketId, holders);
  }

  const active = [];

  for (const item of candidateRows) {
    const holders = holdersByMarket.get(item.market);
    if (!holders) continue;

    let position = null;

    if (item.token) {
      position = holders.get(
        `${item.address}|TOKEN:${item.token}`
      );
    }

    if (!position && item.outcome) {
      position = holders.get(
        `${item.address}|${item.outcome}`
      );
    }

    if (!position) continue;

    const currentSize = n(position.current_size, position.currentSize);
    if (currentSize <= 0) continue;

    const trader = weeklyByWallet.get(item.address);
    const side = normalizedSide(position) || item.outcome;

    if (!trader || !side) continue;

    active.push({
      trader,
      market: item.market,
      title: position.title || item.title,
      side,
      amount: Number(item.amount.toFixed(2)),
      buyTime: new Date(item.buyTime * 1000).toISOString(),
      timestamp: item.buyTime
    });
  }

  return active;
}

/* ---------------------------------------------------------
   CONSENSUS: SAME MARKET, SAME SIDE
   --------------------------------------------------------- */

function buildConsensus(active) {
  const markets = new Map();

  for (const item of active) {
    if (item.side !== "YES" && item.side !== "NO") continue;

    if (!markets.has(item.market)) {
      markets.set(item.market, {
        title: item.title,
        YES: new Map(),
        NO: new Map()
      });
    }

    const group = markets.get(item.market);
    const trader = item.trader;

    const detail = {
      name: trader.name,
      rank: trader.rank,
      wins: trader.wins,
      losses: trader.losses,
      decided: trader.decided,
      winRate: Number(trader.winRate.toFixed(2)),
      pnl: Number(trader.pnl.toFixed(2)),
      side: item.side,
      amount: item.amount,
      buyTime: item.buyTime,
      timestamp: item.timestamp
    };

    const wallet = trader.address.toLowerCase();
    const existing = group[item.side].get(wallet);

    if (!existing || item.timestamp > existing.timestamp) {
      group[item.side].set(wallet, detail);
    }
  }

  const output = [];

  for (const group of markets.values()) {
    for (const side of ["YES", "NO"]) {
      const traders = [...group[side].values()];

      if (traders.length < 2) continue;

      // Exclude a market if qualifying traders recently bought both sides.
      const opposite = side === "YES" ? "NO" : "YES";
      if (group[opposite].size > 0) continue;

      traders.sort((a, b) => a.rank - b.rank);

      const totalEntry = traders.reduce((sum, t) => sum + t.amount, 0);
      const times = traders.map(t => t.timestamp);

      output.push({
        title: group.title,
        side,
        sameSide: traders.length,
        totalEntry: Number(totalEntry.toFixed(2)),
        newestBuy: new Date(Math.max(...times) * 1000).toISOString(),
        oldestBuy: new Date(Math.min(...times) * 1000).toISOString(),
        traders: traders.map(({ timestamp, ...trader }) => trader)
      });
    }
  }

  output.sort((a, b) =>
    b.sameSide - a.sameSide ||
    b.totalEntry - a.totalEntry
  );

  return output;
}

/* ---------------------------------------------------------
   FILE OUTPUT
   --------------------------------------------------------- */

function writeJSON(filename, value) {
  fs.writeFileSync(
    path.join(DATA, filename),
    JSON.stringify(value, null, 2)
  );
}

function readJSON(filename) {
  try {
    return JSON.parse(
      fs.readFileSync(path.join(DATA, filename), "utf8")
    );
  } catch {
    return null;
  }
}

/* ---------------------------------------------------------
   MAIN
   --------------------------------------------------------- */

async function main() {
  fs.mkdirSync(DATA, { recursive: true });

  console.log("==========================================");
  console.log("POLYMARKET 12-HOUR CONSENSUS SCANNER");
  console.log("==========================================");

  const candidates = await discoverCandidates();
  console.log(`Candidate wallets: ${candidates.length}`);

  const weekly = await buildWeekly(candidates);
  console.log(`Qualified weekly traders: ${weekly.length}`);

  const recentBuys = await getRecentGlobalBuys();
  console.log(`BUY trades in the last ${HOURS} hours: ${recentBuys.length}`);

  const active = await buildRecentActive(recentBuys, weekly);
  const consensus = buildConsensus(active);

  const leaders = weekly.slice(0, 100).map(trader => ({
    rank: trader.rank,
    name: trader.name,
    wallet: trader.address,
    winRate: Number(trader.winRate.toFixed(2)),
    wins: trader.wins,
    losses: trader.losses,
    markets: trader.decided,
    pnl: Number(trader.pnl.toFixed(2)),
    volume: trader.volume
  }));

  const traderActiveTrades = {};

  for (const item of active) {
    if (item.trader.rank > 100) continue;

    if (!traderActiveTrades[item.trader.address]) {
      traderActiveTrades[item.trader.address] = [];
    }

    traderActiveTrades[item.trader.address].push({
      title: item.title,
      market: item.market,
      side: item.side,
      amount: item.amount,
      buyTime: item.buyTime,
      timestamp: item.timestamp
    });
  }

  for (const rows of Object.values(traderActiveTrades)) {
    rows.sort((a, b) => b.timestamp - a.timestamp);
  }

  const generatedAt = new Date().toISOString();

  writeJSON("week.json", {
    generatedAt,
    period: "week",
    periodLabel: "1 Week",
    leaders,
    traderActiveTrades,
    consensus,
    activeWindowHours: HOURS,
    activeTradeWindow: "12 hours",
    activeTraderUniverseTarget: TARGET,
    activeTraderUniverseSize: weekly.length,
    minimumDecidedMarkets: MIN_DECIDED,
    rankingMethod: "Win rate → wins → decided markets → P&L"
  });

  // Preserve the separately generated month and three-month leaderboards.
  for (const filename of ["month.json", "threeMonth.json"]) {
    const previous = readJSON(filename);

    if (previous) {
      writeJSON(filename, {
        ...previous,
        generatedAt,
        consensus,
        activeWindowHours: HOURS,
        activeTradeWindow: "12 hours",
        activeTraderUniverseTarget: TARGET,
        activeTraderUniverseSize: weekly.length
      });
    }
  }

  writeJSON("status.json", {
    ok: true,
    generatedAt,
    weeklyCandidates: candidates.length,
    weeklyQualified: weekly.length,
    weeklyTarget: TARGET,
    activeWindowHours: HOURS,
    recentBuyTrades: recentBuys.length,
    activeTradersWithPositions: new Set(
      active.map(item => item.trader.address.toLowerCase())
    ).size,
    activePositions: active.length,
    consensusMarkets: consensus.length
  });

  console.log("------------------------------------------");
  console.log(`Weekly qualified traders: ${weekly.length}`);
  console.log(`Recent BUY trades: ${recentBuys.length}`);
  console.log(`Verified active positions: ${active.length}`);
  console.log(`Consensus markets: ${consensus.length}`);
  console.log("UPDATE COMPLETE");
}

main().catch(error => {
  console.error("FATAL ERROR:", error);
  process.exit(1);
});
