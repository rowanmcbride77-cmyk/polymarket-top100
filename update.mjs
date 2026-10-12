
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const CACHE_FILE = "api-cache.json";
const BATCH_SIZE = 30;
const REQUEST_GAP_MS = 900;
const MAX_RETRIES = 5;
const MAX_JSON_BYTES = 5 * 1024 * 1024;
const LEADER_COUNT = 100;
const CANDIDATE_COUNT = 1000;
const PAGE_SIZE = 50;
const PERIODS = [
  { key: "week", api: "WEEK", days: 7 },
  { key: "month", api: "MONTH", days: 30 }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let lastRequest = 0;
let cache = {
  version: 1,
  nextIndex: 0,
  wallets: {},
  records: {}
};

function num(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function timeMs(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" || /^\d+(\.\d+)?$/.test(String(v))) {
    const n = Number(v);
    return n < 1e12 ? n * 1000 : n;
  }
  const n = Date.parse(v);
  return Number.isFinite(n) ? n : null;
}

function rowsOf(response) {
  if (Array.isArray(response)) return response;
  if (Array.isArray(response?.data)) return response.data;
  return [];
}

function walletOf(row) {
  return String(
    row?.proxyWallet ?? row?.proxy_wallet ?? row?.address ?? row?.wallet ?? ""
  ).toLowerCase();
}

async function loadCache() {
  try {
    const parsed = JSON.parse(await fs.readFile(CACHE_FILE, "utf8"));
    if (parsed && parsed.wallets && parsed.records) {
      cache = { ...cache, ...parsed };
    }
    console.log(`Loaded cache for ${Object.keys(cache.wallets).length} wallets.`);
  } catch {
    console.log("No cache found; starting a new cache.");
  }
}

async function saveCache() {
  const raw = JSON.stringify(cache);
  if (Buffer.byteLength(raw, "utf8") > MAX_JSON_BYTES) {
    throw new Error("api-cache.json exceeded 5 MB; refusing to overwrite it.");
  }
  await fs.writeFile(`${CACHE_FILE}.tmp`, raw, "utf8");
  await fs.rename(`${CACHE_FILE}.tmp`, CACHE_FILE);
}

async function getJson(url) {
  let lastError;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const gap = REQUEST_GAP_MS - (Date.now() - lastRequest);
    if (gap > 0) await sleep(gap);
    lastRequest = Date.now();

    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/4.0"
        },
        signal: AbortSignal.timeout(30000)
      });

      if (response.ok) return await response.json();

      const body = await response.text().catch(() => "");
      const error = new Error(
        `HTTP ${response.status}: ${body.slice(0, 180)}`
      );

      if (![429, 500, 502, 503, 504].includes(response.status)) {
        throw error;
      }

      const retryHeader = Number(response.headers.get("retry-after"));
      const wait = retryHeader > 0
        ? Math.min(120000, retryHeader * 1000)
        : Math.min(60000, 3000 * (2 ** attempt));

      lastError = error;
      console.warn(
        `${error.message}; retry ${attempt + 1}/${MAX_RETRIES} after ${wait}ms`
      );
      await sleep(wait);
    } catch (error) {
      if (/^HTTP (?!429|500|502|503|504)/.test(error.message)) {
        throw error;
      }

      lastError = error;
      if (attempt < MAX_RETRIES - 1) {
        await sleep(Math.min(60000, 3000 * (2 ** attempt)));
      }
    }
  }

  throw lastError || new Error("Request failed after retries");
}

async function getLeaderboard(period) {
  const traders = new Map();

  for (let offset = 0; offset < CANDIDATE_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const response = await getJson(`${API}/v1/leaderboard?${params}`);
    const rows = rowsOf(response);
    if (!rows.length) break;

    for (const row of rows) {
      const wallet = walletOf(row);
      if (!wallet) continue;

      if (!traders.has(wallet)) {
        traders.set(wallet, {
          wallet,
          name: row.userName || row.user_name || row.username ||
            row.name || row.pseudonym ||
            `${wallet.slice(0, 8)}…${wallet.slice(-4)}`,
          pnl: num(row.pnl),
          volume: num(row.vol ?? row.volume)
        });
      }
    }

    if (rows.length < PAGE_SIZE) break;
  }

  return [...traders.values()].slice(0, CANDIDATE_COUNT);
}

async function getPositions(wallet) {
  const positions = [];
  let cursor = null;

  for (let page = 0; page < 3; page++) {
    const params = new URLSearchParams({
      user: wallet,
      limit: "500",
      status: "OPEN",
      sort_by: "CURRENT_VALUE",
      sort_direction: "DESC"
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${params}`);
    positions.push(...rowsOf(response));
    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor) break;
  }

  const now = Date.now();

  return positions.filter(p => {
    const size = num(p.current_size ?? p.size);
    const end = timeMs(p.end_date);
    return size !== null && size > 0 &&
      p.redeemable !== true &&
      p.mergeable !== true &&
      !(end !== null && end < now);
  }).map(p => ({
    condition_id: p.condition_id ?? p.conditionId ?? null,
    title: p.title ?? p.name ?? "Unknown market",
    outcome: p.outcome ?? p.outcome_name ?? null,
    current_size: num(p.current_size ?? p.size),
    current_value: num(p.current_value ?? p.currentValue),
    entry_cost_usdc: num(p.entry_cost_usdc ?? p.initialValue),
    last_event_at: timeMs(p.last_event_at ?? p.lastEventAt),
    end_date: p.end_date ?? null
  }));
}

async function getTrades(wallet) {
  const params = new URLSearchParams({ user: wallet, limit: "30" });
  const response = await getJson(`${API}/trades?${params}`);

  return rowsOf(response)
    .filter(t => String(t.side || "").toUpperCase() === "BUY")
    .map(t => ({
      title: t.title || t.question || "Unknown market",
      side: "BUY",
      outcome: t.outcome || t.outcome_name || "Unknown",
      amount: num(t.usdcSize ?? t.amount ?? t.size),
      buyTime: timeMs(t.timestamp ?? t.createdAt ?? t.time),
      conditionId: t.conditionId ?? t.condition_id ?? null
    }))
    .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
}

function buildConsensus(traders, positionsByWallet) {
  const markets = new Map();

  for (const trader of traders) {
    for (const p of positionsByWallet[trader.wallet] || []) {
      const condition = String(p.condition_id || "").toLowerCase();
      const side = String(p.outcome || "").trim();
      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: p.title || "Unknown market",
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          _wallets: new Set()
        });
      }

      const m = markets.get(key);
      if (m._wallets.has(trader.wallet)) continue;
      m._wallets.add(trader.wallet);
      m.sameSide++;

      m.totalEntry += p.current_value ?? p.entry_cost_usdc ?? 0;
      if (p.last_event_at !== null) {
        m.newestBuy = Math.max(m.newestBuy || 0, p.last_event_at);
      }

      m.traders.push({
        name: trader.name,
        wallet: trader.wallet,
        pnl: trader.pnl,
        volume: trader.volume,
        wins: null,
        losses: null,
        winRate: null,
        resolvedMarkets: null,
        recordVerified: false,
        side,
        amount: p.current_value ?? p.entry_cost_usdc ?? null,
        buyTime: p.last_event_at
      });
    }
  }

  return [...markets.values()]
    .filter(m => m.sameSide >= 2)
    .map(m => {
      delete m._wallets;
      return m;
    })
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function updateRotatingPositions(allTraders) {
  const wallets = allTraders.map(t => t.wallet);
  const count = wallets.length;
  if (!count) return;

  const start = cache.nextIndex % count;
  const batch = [];

  for (let i = 0; i < Math.min(BATCH_SIZE, count); i++) {
    batch.push(wallets[(start + i) % count]);
  }

  console.log(`Updating ${batch.length} positions; queue starts at ${start}.`);

  for (const wallet of batch) {
    try {
      cache.wallets[wallet] = {
        savedAt: Date.now(),
        positions: await getPositions(wallet)
      };
    } catch (error) {
      console.error(`Positions failed for ${wallet}: ${error.message}`);
      // Preserve the last successful positions for this wallet.
      if (!cache.wallets[wallet]) {
        cache.wallets[wallet] = { savedAt: 0, positions: [] };
      }
    }
  }

  cache.nextIndex = (start + batch.length) % count;
  await saveCache();
}

function makeLeaders(traders) {
  return traders.slice(0, LEADER_COUNT).map((t, i) => ({
    rank: i + 1,
    wallet: t.wallet,
    name: t.name,
    pnl: t.pnl,
    volume: t.volume,
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  }));
}

async function writePeriod(period, candidates, positionsByWallet, now) {
  const leaders = makeLeaders(candidates);
  const tradeMap = {};

  for (const trader of leaders) {
    try {
      tradeMap[trader.wallet] = await getTrades(trader.wallet);
    } catch (error) {
      console.warn(`Recent trades unavailable for ${trader.name}: ${error.message}`);
      tradeMap[trader.wallet] = [];
    }
  }

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    candidateCount: candidates.length,
    recordRankingEnabled: false,
    recordWarning:
      "Win-loss records are intentionally marked unavailable until resolved outcomes can be verified reliably.",
    leaders,
    traderActiveTrades: tradeMap,
    traderActivePositions: positionsByWallet,
    consensus: buildConsensus(candidates, positionsByWallet)
  };

  const raw = JSON.stringify(output);
  const size = Buffer.byteLength(raw, "utf8");
  if (size > MAX_JSON_BYTES) {
    throw new Error(`${period.key}.json exceeds 5 MB; refusing to publish.`);
  }

  const temp = `${period.key}.json.tmp`;
  await fs.writeFile(temp, raw, "utf8");
  await fs.rename(temp, `${period.key}.json`);
  console.log(
    `${period.key}.json written: ${leaders.length} leaders, ` +
    `${output.consensus.length} consensus markets, ${(size / 1024).toFixed(0)} KB`
  );
}

async function main() {
  await loadCache();

  console.log("Fetching weekly candidate leaderboard...");
  const weekCandidates = await getLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Fetching monthly candidate leaderboard...");
  const monthCandidates = await getLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const combined = new Map();
  for (const t of [...weekCandidates, ...monthCandidates]) {
    combined.set(t.wallet, t);
  }
  const allTraders = [...combined.values()];

  await updateRotatingPositions(allTraders);

  const positionsByWallet = {};
  for (const [wallet, value] of Object.entries(cache.wallets)) {
    positionsByWallet[wallet] = value.positions || [];
  }

  // Build both outputs in memory first. Do not overwrite website data if
  // the essential leaderboards or position batch failed completely.
  const now = Date.now();
  await writePeriod(PERIODS[0], weekCandidates, positionsByWallet, now);
  await writePeriod(PERIODS[1], monthCandidates, positionsByWallet, now);

  await saveCache();
  console.log("Update completed.");
}

async function loadCache() {
  try {
    const parsed = JSON.parse(await fs.readFile(CACHE_FILE, "utf8"));
    if (parsed?.wallets) cache = { ...cache, ...parsed };
    console.log(`Loaded ${Object.keys(cache.wallets).length} cached wallets.`);
  } catch {
    console.log("No reusable API cache found; starting fresh.");
  }
}

main().catch(error => {
  console.error("Update failed:", error);
  process.exitCode = 1;
});
