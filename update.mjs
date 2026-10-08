import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const DATA = path.join(ROOT, "data");
const API = "https://data-api.polymarket.com";

const ACTIVE_HOURS = 12;
const MIN_DECIDED = 10;
const TARGET = 10000;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const now = () => Math.floor(Date.now() / 1000);

function errorText(error) {
  if (error == null) return "Unknown error";
  if (typeof error === "string") return error;
  if (typeof error.message === "string") return error.message;
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

function number(...values) {
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

function address(row) {
  return String(
    row?.proxyWallet || row?.proxy_wallet ||
    row?.address || row?.wallet || row?.user || ""
  ).toLowerCase();
}

function displayName(row) {
  return row?.userName || row?.user_name ||
    row?.name || row?.username || "Unknown";
}

function condition(row) {
  return String(
    row?.conditionId || row?.condition_id ||
    row?.market || row?.condition || ""
  );
}

function token(row) {
  return String(row?.asset || row?.token_id || row?.tokenId || "");
}

function outcome(row) {
  return String(
    row?.outcome || row?.outcome_name || row?.outcomeName || ""
  ).trim().toUpperCase();
}

function epoch(row) {
  const value = row?.timestamp ?? row?.createdAt ??
    row?.created_at ?? row?.last_event_at;

  if (value === undefined || value === null) return 0;

  const n = Number(value);
  if (Number.isFinite(n) && n > 0) {
    return n > 1e12 ? Math.floor(n / 1000) : Math.floor(n);
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
}

function tradeAmount(row) {
  const direct = row?.usdc_size ?? row?.usdcSize;
  if (direct !== undefined && direct !== null) {
    const value = Number(direct);
    if (Number.isFinite(value)) return value;
  }
  return number(row?.size, row?.amount) * number(row?.price);
}

function title(row) {
  return row?.title || row?.question || row?.market_title ||
    row?.marketTitle || "Unknown market";
}

async function request(url, retries = 5) {
  let lastError = "Unknown request error";

  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(30000)
      });

      if (response.status === 429 || response.status === 503) {
        await sleep(1500 * (i + 1));
        continue;
      }

      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status}: ${(await response.text()).slice(0, 250)}`
        );
      }

      return await response.json();
    } catch (error) {
      lastError = errorText(error);
      if (i < retries - 1) await sleep(800 * (i + 1));
    }
  }

  throw new Error(lastError);
}

function extractRows(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data?.leaderboard)) return data.leaderboard;
  return [];
}

async function limited(items, concurrency, fn) {
  const results = new Array(items.length);
  let cursor = 0;
  let failures = 0;

  async function worker() {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;

      try {
        results[i] = await fn(items[i], i);
      } catch (error) {
        failures++;
        console.error(
          `Worker failed for item ${i + 1}/${items.length}: ${errorText(error)}`
        );
        results[i] = null;
      }
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(concurrency, items.length) },
      () => worker()
    )
  );

  if (failures) {
    console.log(`Worker failures handled: ${failures}`);
  }

  return results;
}

/* -------------------- LEADERBOARD -------------------- */

async function getLeaderboard(orderBy) {
  const all = [];

  for (let offset = 0; offset <= 950; offset += 50) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: "WEEK",
      orderBy,
      limit: "50",
      offset: String(offset)
    });

    const result = await request(`${API}/v1/leaderboard?${params}`);
    const batch = extractRows(result);
    all.push(...batch);

    if (batch.length < 50) break;
    await sleep(100);
  }

  return all;
}

async function discoverTraders() {
  console.log("Loading weekly leaderboard candidates...");

  const [pnlRows, volumeRows] = await Promise.all([
    getLeaderboard("PNL"),
    getLeaderboard("VOL")
  ]);

  const map = new Map();

  for (const row of [...pnlRows, ...volumeRows]) {
    const wallet = address(row);
    if (!wallet) continue;

    if (!map.has(wallet)) {
      map.set(wallet, {
        address: wallet,
        name: displayName(row),
        pnl: number(row.pnl),
        volume: number(row.vol, row.volume),
        sourceRank: number(row.rank)
      });
    } else {
      const old = map.get(wallet);
      old.pnl = Math.max(old.pnl, number(row.pnl));
      old.volume = Math.max(old.volume, number(row.vol, row.volume));
      if (old.name === "Unknown") old.name = displayName(row);
    }
  }

  return [...map.values()];
}

/* -------------------- WIN/LOSS RECORDS -------------------- */

async function getClosedPositions(wallet) {
  const results = [];
  const pageSize = 50;

  for (let offset = 0; offset < 5000; offset += pageSize) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(pageSize),
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = extractRows(
      await request(`${API}/closed-positions?${params}`)
    );

    if (!rows.length) break;
    results.push(...rows);

    if (rows.length < pageSize) break;
    await sleep(50);
  }

  return results;
}

function calculateRecord(positions) {
  const pnlByMarket = new Map();

  for (const row of positions) {
    const market = condition(row);
    if (!market) continue;

    const pnl = number(
      row.realizedPnl,
      row.realized_pnl,
      row.pnl
    );

    pnlByMarket.set(
      market,
      (pnlByMarket.get(market) || 0) + pnl
    );
  }

  let wins = 0;
  let losses = 0;

  for (const pnl of pnlByMarket.values()) {
    if (pnl > 0.000001) wins++;
    else if (pnl < -0.000001) losses++;
  }

  const decided = wins + losses;

  return {
    wins,
    losses,
    decided,
    winRate: decided ? wins / decided * 100 : null,
    qualified: decided >= MIN_DECIDED
  };
}

async function enrichRecords(candidates) {
  console.log(`Checking records for ${candidates.length} candidates...`);

  let finished = 0;

  const records = await limited(candidates, 10, async trader => {
    const positions = await getClosedPositions(trader.address);
    const record = calculateRecord(positions);

    finished++;

    if (finished % 50 === 0) {
      console.log(`Record checks: ${finished}/${candidates.length}`);
    }

    return { ...trader, ...record };
  });

  return records.filter(Boolean);
}

function rankQualified(records) {
  const qualified = records.filter(row => row.qualified);

  qualified.sort((a, b) =>
    (b.winRate ?? -1) - (a.winRate ?? -1) ||
    b.wins - a.wins ||
    b.decided - a.decided ||
    b.pnl - a.pnl
  );

  return qualified.slice(0, TARGET).map((row, i) => ({
    ...row,
    rank: i + 1
  }));
}

/* -------------------- RECENT TRADES -------------------- */

async function getRecentBuys() {
  const cutoff = now() - ACTIVE_HOURS * 3600;
  const trades = [];

  for (let offset = 0; offset < 10000; offset += 500) {
    const params = new URLSearchParams({
      limit: "500",
      offset: String(offset),
      side: "BUY",
      start: String(cutoff),
      end: String(now()),
      takerOnly: "false"
    });

    const batch = extractRows(
      await request(`${API}/trades?${params}`)
    );

    if (!batch.length) break;

    trades.push(...batch.filter(row =>
      String(row.side || "").toUpperCase() === "BUY" &&
      epoch(row) >= cutoff
    ));

    if (batch.length < 500) break;
    await sleep(75);
  }

  return trades;
}

/* -------------------- OPEN POSITIONS -------------------- */

async function getMarketPositions(market) {
  const params = new URLSearchParams({
    market,
    limit: "500",
    offset: "0",
    sortBy: "TOTAL_PNL",
    sortDirection: "DESC"
  });

  const result = await request(`${API}/v1/market-positions?${params}`);

  if (Array.isArray(result)) {
    return result.flatMap(group =>
      Array.isArray(group?.positions) ? group.positions : []
    );
  }

  return extractRows(result);
}

function positionSide(row) {
  const label = outcome(row);
  if (label === "YES" || label === "NO") return label;

  const index = row?.outcomeIndex ?? row?.outcome_index;
  if (Number(index) === 0) return "YES";
  if (Number(index) === 1) return "NO";

  return "";
}

async function buildActivePositions(trades, ranked) {
  const byWallet = new Map(
    ranked.map(trader => [trader.address, trader])
  );

  const grouped = new Map();
  const cutoff = now() - ACTIVE_HOURS * 3600;

  for (const trade of trades) {
    const wallet = address(trade);
    const market = condition(trade);
    const tokenId = token(trade);
    const side = outcome(trade);
    const time = epoch(trade);

    if (!wallet || !market || !byWallet.has(wallet) || time < cutoff) continue;

    const key = `${wallet}|${market}|${tokenId || side}`;

    if (!grouped.has(key)) {
      grouped.set(key, {
        wallet,
        market,
        token: tokenId,
        side,
        title: title(trade),
        amount: 0,
        buyTime: 0
      });
    }

    const item = grouped.get(key);
    item.amount += tradeAmount(trade);

    if (time > item.buyTime) {
      item.buyTime = time;
      item.title = title(trade);
    }
  }

  const candidates = [...grouped.values()];
  const marketIds = [...new Set(candidates.map(row => row.market))];

  console.log(`Recent BUY groups: ${candidates.length}`);
  console.log(`Checking ${marketIds.length} markets for open positions...`);

  const positionResults = await limited(marketIds, 8, async market => ({
    market,
    positions: await getMarketPositions(market)
  }));

  const holderMaps = new Map();

  for (const result of positionResults.filter(Boolean)) {
    const holders = new Map();

    for (const position of result.positions) {
      const wallet = address(position);
      if (!wallet) continue;

      const tokenId = token(position);
      const side = positionSide(position);
      const size = number(
        position.size,
        position.currentSize,
        position.current_size
      );

      if (size <= 0) continue;
      if (tokenId) holders.set(`${wallet}|TOKEN:${tokenId}`, position);
      if (side) holders.set(`${wallet}|SIDE:${side}`, position);
    }

    holderMaps.set(result.market, holders);
  }

  const active = [];

  for (const item of candidates) {
    const holders = holderMaps.get(item.market);
    if (!holders) continue;

    let position = item.token
      ? holders.get(`${item.wallet}|TOKEN:${item.token}`)
      : null;

    if (!position && item.side) {
      position = holders.get(`${item.wallet}|SIDE:${item.side}`);
    }

    if (!position) continue;

    const trader = byWallet.get(item.wallet);
    const side = positionSide(position) || item.side;
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

/* -------------------- CONSENSUS -------------------- */

function buildConsensus(active) {
  const markets = new Map();

  for (const row of active) {
    if (row.side !== "YES" && row.side !== "NO") continue;

    if (!markets.has(row.market)) {
      markets.set(row.market, {
        title: row.title,
        YES: new Map(),
        NO: new Map()
      });
    }

    const group = markets.get(row.market);
    const trader = row.trader;

    group[row.side].set(trader.address, {
      name: trader.name,
      rank: trader.rank,
      wins: trader.wins,
      losses: trader.losses,
      decided: trader.decided,
      winRate: trader.winRate,
      pnl: trader.pnl,
      side: row.side,
      amount: row.amount,
      buyTime: row.buyTime,
      timestamp: row.timestamp
    });
  }

  const result = [];

  for (const group of markets.values()) {
    for (const side of ["YES", "NO"]) {
      const traders = [...group[side].values()];
      const opposite = side === "YES" ? "NO" : "YES";

      if (traders.length < 2 || group[opposite].size > 0) continue;

      traders.sort((a, b) => a.rank - b.rank);

      const times = traders.map(row => row.timestamp);
      const totalEntry = traders.reduce((sum, row) => sum + row.amount, 0);

      result.push({
        title: group.title,
        side,
        sameSide: traders.length,
        totalEntry: Number(totalEntry.toFixed(2)),
        newestBuy: new Date(Math.max(...times) * 1000).toISOString(),
        oldestBuy: new Date(Math.min(...times) * 1000).toISOString(),
        traders: traders.map(({ timestamp, ...row }) => row)
      });
    }
  }

  return result.sort((a, b) =>
    b.sameSide - a.sameSide || b.totalEntry - a.totalEntry
  );
}

/* -------------------- OUTPUT -------------------- */

function writeJSON(filename, data) {
  fs.writeFileSync(
    path.join(DATA, filename),
    JSON.stringify(data, null, 2)
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

async function main() {
  fs.mkdirSync(DATA, { recursive: true });

  console.log("==========================================");
  console.log("POLYMARKET 12-HOUR CONSENSUS SCANNER");
  console.log("==========================================");

  const candidates = await discoverTraders();
  console.log(`Leaderboard candidates: ${candidates.length}`);

  const records = await enrichRecords(candidates);
  const ranked = rankQualified(records);

  console.log(`Qualified traders: ${ranked.length}`);

  if (ranked.length === 0) {
    throw new Error(
      "No traders met the minimum 10 decided markets. Existing data was preserved."
    );
  }

  const recentBuys = await getRecentBuys();
  console.log(`Recent BUY trades: ${recentBuys.length}`);

  const active = await buildActivePositions(recentBuys, ranked);
  const consensus = buildConsensus(active);

  const leaders = ranked.slice(0, 100).map(row => ({
    rank: row.rank,
    name: row.name,
    wallet: row.address,
    winRate: Number(row.winRate.toFixed(2)),
    wins: row.wins,
    losses: row.losses,
    markets: row.decided,
    pnl: Number(row.pnl.toFixed(2)),
    volume: row.volume
  }));

  const traderActiveTrades = {};

  for (const row of active) {
    if (row.trader.rank > 100) continue;

    const wallet = row.trader.address;
    if (!traderActiveTrades[wallet]) traderActiveTrades[wallet] = [];

    traderActiveTrades[wallet].push({
      title: row.title,
      market: row.market,
      side: row.side,
      amount: row.amount,
      buyTime: row.buyTime,
      timestamp: row.timestamp
    });
  }

  const generatedAt = new Date().toISOString();

  writeJSON("week.json", {
    generatedAt,
    period: "week",
    periodLabel: "1 Week",
    leaders,
    traderActiveTrades,
    consensus,
    activeWindowHours: ACTIVE_HOURS,
    activeTradeWindow: "12 hours",
    activeTraderUniverseTarget: TARGET,
    activeTraderUniverseSize: ranked.length,
    minimumDecidedMarkets: MIN_DECIDED,
    rankingMethod: "Win rate → wins → decided markets → P&L"
  });

  for (const filename of ["month.json", "threeMonth.json"]) {
    const previous = readJSON(filename);
    if (!previous) continue;

    writeJSON(filename, {
      ...previous,
      generatedAt,
      consensus,
      activeWindowHours: ACTIVE_HOURS,
      activeTradeWindow: "12 hours"
    });
  }

  writeJSON("status.json", {
    ok: true,
    generatedAt,
    leaderboardCandidates: candidates.length,
    recordsRetrieved: records.length,
    weeklyQualified: ranked.length,
    weeklyTarget: TARGET,
    recentBuyTrades: recentBuys.length,
    activePositions: active.length,
    activeTradersWithPositions: new Set(
      active.map(row => row.trader.address)
    ).size,
    consensusMarkets: consensus.length
  });

  console.log("------------------------------------------");
  console.log(`Leaderboard candidates: ${candidates.length}`);
  console.log(`Qualified weekly traders: ${ranked.length}`);
  console.log(`Verified active positions: ${active.length}`);
  console.log(`Consensus markets: ${consensus.length}`);
  console.log("UPDATE COMPLETE");
}

main().catch(error => {
  console.error("FATAL ERROR:", errorText(error));
  process.exit(1);
});
