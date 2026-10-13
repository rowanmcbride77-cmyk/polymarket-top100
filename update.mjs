
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const CONSENSUS_LIMIT = 50;
const TRADERS_PER_MARKET = 15;
const TRADE_LIMIT = 10;
const MAX_JSON_BYTES = 5 * 1024 * 1024;
const CONCURRENCY = 6;
const DELAY_MS = 150;

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
        const retryAfter = Number(response.headers.get("retry-after"));
        await sleep(
          retryAfter > 0
            ? Math.min(retryAfter * 1000, 15000)
            : 1000 * (attempt + 1)
        );
        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`
        );
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await sleep(500 * (attempt + 1));
      }
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

async function getResolvedRecord(wallet, period, nowMs) {
  if (!wallet) return null;

  // Closed-position rows have a settlement price and settlement timestamp.
  // Count only settled positions whose settlement timestamp falls in this
  // leaderboard's window; do not substitute an all-time record.
  const days = period.key === "week" ? 7 : 30;
  const startMs = nowMs - days * 24 * 60 * 60 * 1000;
  const endSeconds = Math.floor(nowMs / 1000);
  const startSeconds = Math.floor(startMs / 1000);
  const seen = new Set();
  let wins = 0;
  let losses = 0;

  for (let offset = 0; offset < 1000; offset += 50) {
    const params = new URLSearchParams({
      user: wallet,
      limit: "50",
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });
    const rows = asArray(await getJson(`${API}/closed-positions?${params}`));
    if (!rows.length) break;

    let passedWindow = false;
    for (const row of rows) {
      const timestamp = timestampMs(row.timestamp);
      if (timestamp === null) continue;
      if (timestamp < startMs) {
        passedWindow = true;
        continue;
      }
      if (timestamp > nowMs) continue;

      const price = num(row.curPrice ?? row.cur_price);
      if (price === null) continue;
      // One closed-position outcome is one win/loss; avoid duplicate API rows.
      const key = String(row.asset ?? "") || `${row.conditionId ?? row.condition_id ?? ""}|${row.outcomeIndex ?? row.outcome_index ?? row.outcome ?? ""}`;
      if (seen.has(key)) continue;
      seen.add(key);

      if (price >= 0.99) wins++;
      else if (price <= 0.01) losses++;
    }

    if (passedWindow || rows.length < 50) break;
  }

  const resolvedMarkets = wins + losses;
  return {
    wins,
    losses,
    winRate: resolvedMarkets ? (wins / resolvedMarkets) * 100 : null,
    resolvedMarkets,
    recordVerified: true,
    recordBasis: `Closed positions settled within the last ${days} days`
  };
}

async function attachRecords(traders, period, nowMs) {
  const rows = await mapLimit(traders, CONCURRENCY, async trader => {
    try {
      const record = await getResolvedRecord(trader.wallet, period, nowMs);
      return record ? [trader.wallet, record] : null;
    } catch (error) {
      console.error(`Record lookup failed for ${trader.name}: ${error.message}`);
      return null;
    }
  });
  return Object.fromEntries(rows.filter(Boolean));
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
        console.error(`Item failed: ${error.message}`);
        results[index] = null;
      }

      await sleep(DELAY_MS);
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

    const rows = asArray(
      await getJson(`${API}/v1/leaderboard?${params}`)
    );

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

function rankCandidates(candidates) {
  return [...candidates]
    .sort((a, b) => (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity))
    .map((trader, index) => ({ ...trader, rank: index + 1 }));
}

function isActivePosition(position, nowMs) {
  const size = num(
    position.size ??
    position.current_size ??
    position.currentSize
  );

  if (size === null || size <= 0) return false;
  if (position.closed === true) return false;
  if (position.redeemable === true) return false;
  if (position.mergeable === true) return false;

  const endDate =
    position.endDate ??
    position.end_date ??
    position.endDateIso;

  if (endDate) {
    const endMs = timestampMs(endDate);
    if (endMs !== null && endMs < nowMs) return false;
  }

  return true;
}

function compactPosition(position) {
  return {
    conditionId:
      position.conditionId ??
      position.condition_id ??
      null,
    title: position.title || position.question || "Unknown market",
    outcome: position.outcome ?? position.outcome_name ?? null,
    size: num(position.size ?? position.current_size ?? position.currentSize),
    currentValue: num(position.currentValue ?? position.current_value),
    initialValue: num(position.initialValue ?? position.initial_value),
    lastEventAt: timestampMs(
      position.lastEventAt ?? position.last_event_at
    ),
    endDate:
      position.endDate ??
      position.end_date ??
      position.endDateIso ??
      null
  };
}

async function getActivePositions(wallet, nowMs) {
  const params = new URLSearchParams({
    user: wallet,
    limit: String(POSITION_FETCH_LIMIT),
    status: "OPEN",
    sort_by: "CURRENT_VALUE",
    sort_direction: "DESC"
  });

  const response = await getJson(`${API}/v2/positions?${params}`);
  const rows = asArray(response);

  return rows
    .filter(position => isActivePosition(position, nowMs))
    .sort(
      (a, b) =>
        (num(b.currentValue ?? b.current_value) ?? 0) -
        (num(a.currentValue ?? a.current_value) ?? 0)
    )
    .slice(0, POSITIONS_PER_TRADER)
    .map(compactPosition);
}

async function attachActivePositions(candidates, nowMs) {
  const rows = await mapLimit(
    candidates,
    CONCURRENCY,
    async trader => {
      try {
        const positions = await getActivePositions(trader.wallet, nowMs);
        console.log(`Positions: ${trader.name} (${positions.length})`);
        return [trader.wallet, positions];
      } catch (error) {
        console.error(`Positions failed for ${trader.name}: ${error.message}`);
        return [trader.wallet, []];
      }
    }
  );

  return Object.fromEntries(rows.filter(Boolean));
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
      .slice(0, TRADE_LIMIT)
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

function buildConsensus(candidates, activePositions, records = {}) {
  const markets = new Map();

  for (const trader of candidates) {
    for (const position of activePositions[trader.wallet] || []) {
      const conditionId = String(
        position.conditionId ?? position.condition_id ?? ""
      ).toLowerCase();

      const side = String(
        position.outcome ?? position.outcome_name ?? ""
      ).trim();

      if (!conditionId || !side) continue;

      const key = `${conditionId}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId,
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

      const amount = num(position.currentValue ?? position.initialValue);
      const buyTime = timestampMs(position.lastEventAt);

      if (market.traders.length < TRADERS_PER_MARKET) {
        market.traders.push({
          wallet: trader.wallet,
          name: trader.name,
          rank: trader.rank,
          pnl: trader.pnl,
          volume: trader.volume,
          wins: records[trader.wallet]?.wins ?? null,
          losses: records[trader.wallet]?.losses ?? null,
          winRate: records[trader.wallet]?.winRate ?? null,
          resolvedMarkets: records[trader.wallet]?.resolvedMarkets ?? null,
          side,
          amount,
          buyTime
        });
      }

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
    .sort(
      (a, b) =>
        b.sameSide - a.sameSide ||
        b.totalEntry - a.totalEntry ||
        (b.newestBuy || 0) - (a.newestBuy || 0)
    )
    .slice(0, CONSENSUS_LIMIT);
}

function makeOutput(period, candidates, activePositions, tradeRows, nowMs, consensusCandidates, records) {
  const ranked = rankCandidates(candidates);
  const leaders = ranked.slice(0, LEADER_COUNT).map(trader => ({ ...trader, ...(records[trader.wallet] || {}) }));

  const leaderPositions = Object.fromEntries(
    leaders.map(trader => [
      trader.wallet,
      activePositions[trader.wallet] || []
    ])
  );

  return {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      `Leaderboard ordered by ${period.key} P&L, with resolved-market record data`,
    recordBasis: `Win-loss record counts closed positions settled within the selected ${period.key} window`,
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: true,
    recordWarning:
      "Records count settled closed positions within the selected period; they may differ from Polymarket profile all-time statistics.",
    leaders,
    traderActiveTrades: Object.fromEntries(tradeRows.filter(Boolean)),
    traderActivePositions: leaderPositions,
    consensus: buildConsensus(consensusCandidates, activePositions, records)
  };
}

async function writeJsonUnderLimit(filename, output) {
  const compact = () => JSON.stringify(output);

  let json = compact();
  let bytes = Buffer.byteLength(json, "utf8");

  // Progressively trim large sections until the actual serialized
  // file is below the workflow's 5 MB limit.
  while (bytes > MAX_JSON_BYTES) {
    if (output.consensus.length > 0) {
      output.consensus = output.consensus.slice(
        0,
        Math.floor(output.consensus.length / 2)
      );
    } else {
      for (const wallet of Object.keys(output.traderActivePositions)) {
        output.traderActivePositions[wallet] =
          (output.traderActivePositions[wallet] || []).slice(0, 3);
      }

      for (const wallet of Object.keys(output.traderActiveTrades)) {
        output.traderActiveTrades[wallet] =
          (output.traderActiveTrades[wallet] || []).slice(0, 3);
      }

      const noPositions = Object.values(output.traderActivePositions)
        .every(positions => positions.length === 0);

      const noTrades = Object.values(output.traderActiveTrades)
        .every(trades => trades.length === 0);

      if (noPositions && noTrades) {
        throw new Error(
          `${filename} remains ${(bytes / 1024 / 1024).toFixed(2)} MB after trimming positions, trades, and consensus. Check whether another workflow step is generating this file.`
        );
      }
    }

    json = compact();
    bytes = Buffer.byteLength(json, "utf8");
  }

  const tempFilename = `${filename}.tmp`;
  await fs.writeFile(tempFilename, json, "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${(bytes / 1024 / 1024).toFixed(2)} MB`
  );
}

async function buildPeriod(period, candidates, activePositions, nowMs, consensusCandidates) {
  const ranked = rankCandidates(candidates);
  const leaders = ranked.slice(0, LEADER_COUNT);
  console.log(`Loading resolved records for ${leaders.length} ${period.key} leaders...`);
  const records = await attachRecords(leaders, period, nowMs);

  const tradeRows = await mapLimit(
    leaders,
    CONCURRENCY,
    async trader => [
      trader.wallet,
      await getRecentTrades(trader.wallet)
    ]
  );

  const output = makeOutput(
    period,
    ranked,
    activePositions,
    tradeRows,
    nowMs,
    consensusCandidates,
    records
  );

  await writeJsonUnderLimit(`${period.key}.json`, output);

  console.log(
    `${period.key}: ${leaders.length} leaders, ${output.consensus.length} consensus markets`
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
  const activePositions = await attachActivePositions(
    [...unique.values()],
    nowMs
  );

  await buildPeriod(PERIODS[0], weekCandidates, activePositions, nowMs, rankCandidates(weekCandidates));
  await buildPeriod(PERIODS[1], monthCandidates, activePositions, nowMs, rankCandidates(weekCandidates));

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
