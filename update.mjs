
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const CONFIG = {
  candidates: 1000,
  leaders: 100,
  leaderboardPageSize: 50,
  closedPageSize: 50,
  activePageSize: 500,
  tradeLimit: 50,
  minResolved: 10,
  recordConcurrency: 8,
  activeConcurrency: 8,
  tradeConcurrency: 8,
  maxClosedPagesPerTrader: 2,
  requestTimeoutMs: 12000,
  retries: 2
};

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const recordCache = new Map();

function arr(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.data)) return data.data;
  if (Array.isArray(data?.results)) return data.results;
  return [];
}

function num(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function timeMs(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const n = Number(value);
    return Number.isFinite(n) ? (n < 1e11 ? n * 1000 : n) : null;
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

async function getJson(url) {
  let lastError;

  for (let attempt = 0; attempt <= CONFIG.retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(),
      CONFIG.requestTimeoutMs
    );

    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketTop100/2.0"
        },
        signal: controller.signal
      });

      if (response.status === 429 || response.status >= 500) {
        const error = new Error(`HTTP ${response.status}`);
        error.retryable = true;
        throw error;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 160)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (attempt >= CONFIG.retries) break;

      await sleep(
        error.retryable
          ? 700 * (attempt + 1)
          : 250 * (attempt + 1)
      );
    } finally {
      clearTimeout(timer);
    }
  }

  throw lastError || new Error("Request failed");
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;

      try {
        results[index] = await callback(items[index], index);
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

/* ---------------- LEADERBOARD ---------------- */

async function getCandidates(period) {
  const traders = [];
  const seen = new Set();

  for (
    let offset = 0;
    offset < CONFIG.candidates;
    offset += CONFIG.leaderboardPageSize
  ) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(CONFIG.leaderboardPageSize),
      offset: String(offset)
    });

    const rows = arr(
      await getJson(`${API}/v1/leaderboard?${params}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const wallet = walletOf(row);
      if (!wallet || seen.has(wallet)) continue;
      seen.add(wallet);

      traders.push({
        rank: traders.length + 1,
        wallet,
        name:
          row.userName ||
          row.user_name ||
          row.username ||
          row.name ||
          row.pseudonym ||
          `${wallet.slice(0, 8)}…${wallet.slice(-4)}`,
        pnl: num(row.pnl),
        volume: num(row.vol ?? row.volume),
        wins: null,
        losses: null,
        winRate: null,
        resolvedMarkets: null,
        recordVerified: false
      });
    }

    console.log(`${period.key}: ${traders.length}/${CONFIG.candidates} candidates`);

    if (rows.length < CONFIG.leaderboardPageSize) break;
    await sleep(100);
  }

  return traders.slice(0, CONFIG.candidates);
}

/* ---------------- CLOSED POSITIONS ---------------- */

/*
 * IMPORTANT:
 * The closed-positions endpoint may not expose the actual
 * settlement time or a reliable settled outcome for every row.
 * We never label a record verified unless the row contains
 * the required evidence. We also cap pages to prevent runaway scans.
 */

function getPositionTime(position) {
  return timeMs(
    position.resolutionTimestamp ??
    position.resolution_timestamp ??
    position.resolvedAt ??
    position.resolved_at ??
    position.timestamp ??
    position.closedAt ??
    position.closed_at
  );
}

function getSettlementPrice(position) {
  const candidates = [
    position.settlementPrice,
    position.settlement_price,
    position.payout,
    position.curPrice,
    position.currentPrice,
    position.current_price
  ];

  for (const value of candidates) {
    const n = num(value);
    if (n === 0 || n === 1) return n;
  }

  return null;
}

async function getRecentClosedPositions(wallet, startMs) {
  const positions = [];
  const seen = new Set();

  for (
    let page = 0;
    page < CONFIG.maxClosedPagesPerTrader;
    page++
  ) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(CONFIG.closedPageSize),
      offset: String(page * CONFIG.closedPageSize),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = arr(
      await getJson(`${API}/closed-positions?${params}`)
    );

    if (!rows.length) break;

    let foundOlderTimestamp = false;
    let missingTimestamps = 0;

    for (const p of rows) {
      const timestamp = getPositionTime(p);

      if (timestamp === null) {
        missingTimestamps++;
        continue;
      }

      if (timestamp < startMs) {
        foundOlderTimestamp = true;
        continue;
      }

      const condition = String(
        p.conditionId ?? p.condition_id ?? ""
      ).toLowerCase();

      const asset = String(
        p.asset ??
        p.token_id ??
        p.tokenId ??
        p.outcomeIndex ??
        p.outcome_index ??
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
        settlementPrice: getSettlementPrice(p),
        title: p.title || p.question || "Unknown market",
        outcome: p.outcome || "Unknown"
      });
    }

    /*
     * Critical safety stop:
     * if timestamps are missing, do not keep paging indefinitely.
     */
    if (
      foundOlderTimestamp ||
      missingTimestamps > rows.length / 2 ||
      rows.length < CONFIG.closedPageSize
    ) {
      break;
    }
  }

  return positions;
}

function calculateRecord(positions, period, nowMs) {
  const startMs = nowMs - period.days * 86400000;
  let wins = 0;
  let losses = 0;
  let datedSettlements = 0;

  for (const p of positions) {
    if (p.timestamp < startMs || p.timestamp > nowMs) continue;

    if (p.settlementPrice === 1) {
      wins++;
      datedSettlements++;
    } else if (p.settlementPrice === 0) {
      losses++;
      datedSettlements++;
    }
  }

  const resolvedMarkets = wins + losses;

  return {
    wins,
    losses,
    resolvedMarkets,
    winRate: resolvedMarkets
      ? (wins / resolvedMarkets) * 100
      : null,

    /*
     * Do not falsely label inferred or incomplete data as verified.
     * The public endpoint's timestamps are not guaranteed to be
     * actual resolution timestamps.
     */
    recordVerified: false,
    recordBasis:
      "Closed-position snapshot; actual settlement timestamp not guaranteed",
    datedSettlements
  };
}

async function getCachedPositions(wallet, monthStartMs) {
  if (recordCache.has(wallet)) return recordCache.get(wallet);

  const promise = getRecentClosedPositions(wallet, monthStartMs);
  recordCache.set(wallet, promise);

  try {
    return await promise;
  } catch (error) {
    recordCache.delete(wallet);
    throw error;
  }
}

async function attachRecords(traders, period, monthStartMs, nowMs) {
  let completed = 0;

  const result = await mapLimit(
    traders,
    CONFIG.recordConcurrency,
    async trader => {
      try {
        const positions = await getCachedPositions(
          trader.wallet,
          monthStartMs
        );

        const record = calculateRecord(positions, period, nowMs);
        completed++;

        if (completed % 25 === 0 || completed === traders.length) {
          console.log(`${period.key}: checked ${completed}/${traders.length}`);
        }

        return { ...trader, ...record };
      } catch (error) {
        completed++;
        console.error(`Record lookup failed for ${trader.name}: ${error.message}`);

        return {
          ...trader,
          wins: null,
          losses: null,
          winRate: null,
          resolvedMarkets: null,
          recordVerified: false,
          recordError: error.message
        };
      }
    }
  );

  return result.filter(Boolean);
}

/* ---------------- ACTIVE POSITIONS ---------------- */

function isActive(p, nowMs) {
  const size = num(p.size ?? p.current_size ?? p.currentSize);
  if (size === null || size <= 0) return false;

  if (p.closed === true || p.redeemable === true || p.mergeable === true) {
    return false;
  }

  const end = timeMs(p.endDate ?? p.end_date ?? p.endDateIso);
  if (end !== null && end < nowMs) return false;

  return true;
}

async function getActivePositions(wallet, nowMs) {
  const positions = [];

  /*
   * Active-position scan is bounded as well.
   * Most traders return fewer than one page.
   */
  for (let offset = 0; offset <= 1000; offset += CONFIG.activePageSize) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(CONFIG.activePageSize),
      offset: String(offset)
    });

    const rows = arr(
      await getJson(`${API}/positions?${params}`)
    );

    if (!rows.length) break;
    positions.push(...rows.filter(p => isActive(p, nowMs)));

    if (rows.length < CONFIG.activePageSize) break;
  }

  return positions;
}

async function attachActivePositions(traders, nowMs) {
  const rows = await mapLimit(
    traders,
    CONFIG.activeConcurrency,
    async trader => {
      try {
        return [
          trader.wallet,
          await getActivePositions(trader.wallet, nowMs)
        ];
      } catch (error) {
        console.error(`Active positions failed for ${trader.name}: ${error.message}`);
        return [trader.wallet, []];
      }
    }
  );

  return Object.fromEntries(
    rows.filter(row => Array.isArray(row) && row.length === 2)
  );
}

/* ---------------- RECENT BUYS ---------------- */

async function getRecentTrades(wallet) {
  const params = new URLSearchParams({
    user: wallet,
    limit: String(CONFIG.tradeLimit)
  });

  try {
    const rows = arr(
      await getJson(`${API}/trades?${params}`)
    );

    return rows
      .filter(row => String(row.side || "").toUpperCase() === "BUY")
      .map(row => ({
        title: row.title || row.question || row.eventSlug || "Unknown market",
        side: "BUY",
        outcome: row.outcome || row.outcome_name || "Unknown",
        amount: num(row.usdcSize ?? row.amount ?? row.size),
        buyTime: timeMs(row.timestamp ?? row.createdAt ?? row.time),
        conditionId: row.conditionId || row.condition_id || null,
        transactionHash: row.transactionHash || row.transaction_hash || null
      }))
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    console.error(`Recent buys failed for ${wallet}: ${error.message}`);
    return [];
  }
}

/* ---------------- SAME-SIDE CONSENSUS ---------------- */

function buildConsensus(traders, activePositions) {
  const markets = new Map();

  for (const trader of traders) {
    const positions = activePositions[trader.wallet] || [];

    for (const p of positions) {
      const condition = String(
        p.conditionId ?? p.condition_id ?? ""
      ).toLowerCase();

      const side = String(
        p.outcome ?? p.outcome_name ?? ""
      ).trim();

      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: p.title || p.question || "Unknown market",
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }

      const market = markets.get(key);
      if (market.walletsSeen.has(trader.wallet)) continue;
      market.walletsSeen.add(trader.wallet);

      const amount = num(
        p.currentValue ??
        p.current_value ??
        p.initialValue ??
        p.initial_value
      );

      const buyTime = timeMs(p.lastEventAt ?? p.last_event_at);

      market.sameSide++;
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
        recordVerified: trader.recordVerified,
        side,
        amount,
        buyTime
      });
    }
  }

  return [...markets.values()]
    .filter(m => m.sameSide >= 2)
    .map(({ walletsSeen, ...m }) => m)
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

/* ---------------- BUILD OUTPUT ---------------- */

function rankTraders(traders) {
  /*
   * Unverified records are not promoted above verified records.
   * Because the public endpoint doesn't guarantee settlement times,
   * this updater deliberately does not pretend it has verified records.
   */
  return [...traders]
    .sort((a, b) =>
      (b.recordVerified === true) - (a.recordVerified === true) ||
      (b.winRate ?? -1) - (a.winRate ?? -1) ||
      (b.resolvedMarkets ?? -1) - (a.resolvedMarkets ?? -1) ||
      (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
    )
    .map((trader, index) => ({ ...trader, rank: index + 1 }));
}

async function buildPeriod(period, candidates, activePositions, nowMs, monthStartMs) {
  console.log(`Calculating ${period.key} records...`);

  const withRecords = await attachRecords(
    candidates,
    period,
    monthStartMs,
    nowMs
  );

  const ranked = rankTraders(withRecords);
  const leaders = ranked.slice(0, CONFIG.leaders);

  const tradeRows = await mapLimit(
    leaders,
    CONFIG.tradeConcurrency,
    async trader => [
      trader.wallet,
      await getRecentTrades(trader.wallet)
    ]
  );

  const traderActiveTrades = Object.fromEntries(
    tradeRows.filter(row => Array.isArray(row) && row.length === 2)
  );

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      "Public leaderboard candidates; closed-position history is incomplete and not a verified period record",
    recordBasis:
      "Closed-position endpoint; settlement timestamps are not guaranteed",
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: CONFIG.minResolved,
    recordRankingEnabled: false,
    leaders,
    traderActiveTrades,
    traderActivePositions: activePositions,
    consensus: buildConsensus(ranked, activePositions)
  };

  const filename = `${period.key}.json`;
  const temp = `${filename}.tmp`;

  await fs.writeFile(temp, JSON.stringify(output), "utf8");
  await fs.rename(temp, filename);

  console.log(`Wrote ${filename}: ${leaders.length} leaders`);
}

async function main() {
  const nowMs = Date.now();
  const monthStartMs = nowMs - 30 * 86400000;

  console.log("Loading weekly leaderboard candidates...");
  const weekCandidates = await getCandidates(PERIODS[0]);

  console.log("Loading monthly leaderboard candidates...");
  const monthCandidates = await getCandidates(PERIODS[1]);

  const uniqueMap = new Map();

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!uniqueMap.has(trader.wallet)) {
      uniqueMap.set(trader.wallet, trader);
    }
  }

  const uniqueCandidates = [...uniqueMap.values()];

  /*
   * Only fetch active positions for candidates that can appear
   * in the displayed top 100 for either period. This avoids
   * scanning thousands of traders' active positions unnecessarily.
   */
  const activeWallets = new Map();

  for (const trader of [...weekCandidates.slice(0, 100), ...monthCandidates.slice(0, 100)]) {
    activeWallets.set(trader.wallet, trader);
  }

  console.log(
    `Loading active positions for ${activeWallets.size} displayed candidates...`
  );

  const activePositions = await attachActivePositions(
    [...activeWallets.values()],
    nowMs
  );

  await buildPeriod(
    PERIODS[0],
    weekCandidates,
    activePositions,
    nowMs,
    monthStartMs
  );

  await buildPeriod(
    PERIODS[1],
    monthCandidates,
    activePositions,
    nowMs,
    monthStartMs
  );

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
