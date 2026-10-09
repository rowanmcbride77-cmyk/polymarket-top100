
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const LEADER_PAGE_SIZE = 50;
const CLOSED_PAGE_SIZE = 50;
const ACTIVE_PAGE_SIZE = 500;
const TRADE_LIMIT = 50;
const MIN_RESOLVED_FOR_RANKING = 10;

const RECORD_CONCURRENCY = 12;
const ACTIVE_CONCURRENCY = 12;
const TRADE_CONCURRENCY = 10;
const REQUEST_DELAY_MS = 50;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const recordCache = new Map();
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
        const body = await response.text();
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (attempt < attempts - 1) {
        await sleep(250 * (attempt + 1));
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

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    return number < 100000000000 ? number * 1000 : number;
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
    rank: numberOrNull(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: numberOrNull(row.pnl),
    volume: numberOrNull(row.vol ?? row.volume),
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
 * Pull only recent closed-position history.
 *
 * We stop paging when the returned history has passed
 * the rolling 30-day window. This avoids scanning years
 * of all-time history for every trader.
 */
async function getRecentClosedPositions(wallet, monthStartMs) {
  const positions = [];
  const seen = new Set();

  for (let offset = 0; offset <= 100000; offset += CLOSED_PAGE_SIZE) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(CLOSED_PAGE_SIZE),
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = asArray(
      await getJson(`${API}/closed-positions?${params}`)
    );

    if (!rows.length) break;

    let reachedOlderHistory = false;

    for (const position of rows) {
      const time = timestampMs(
        position.timestamp ??
        position.closedAt ??
        position.closed_at
      );

      if (time !== null && time < monthStartMs) {
        reachedOlderHistory = true;
        continue;
      }

      if (time === null) continue;

      const condition = String(
        position.conditionId ?? position.condition_id ?? ""
      ).toLowerCase();

      const asset = String(
        position.asset ??
        position.token_id ??
        position.tokenId ??
        position.outcomeIndex ??
        position.outcome_index ??
        ""
      ).toLowerCase();

      if (!condition || !asset) continue;

      const key = `${condition}|${asset}`;

      if (seen.has(key)) continue;
      seen.add(key);

      positions.push({
        condition,
        asset,
        timestamp: time,
        price: numberOrNull(
          position.curPrice ??
          position.currentPrice ??
          position.current_price
        ),
        realizedPnl: numberOrNull(
          position.realizedPnl ??
          position.realized_pnl
        ),
        title: position.title || position.question || "Unknown market",
        outcome: position.outcome || "Unknown"
      });
    }

    if (reachedOlderHistory || rows.length < CLOSED_PAGE_SIZE) break;
  }

  return positions;
}

/*
 * Count only records with an explicit settlement price:
 * 1 = winning outcome
 * 0 = losing outcome
 *
 * Do not infer a win or loss from trading volume or P&L.
 */
function calculatePeriodRecord(positions, period, nowMs) {
  const startMs = nowMs - period.days * 24 * 60 * 60 * 1000;

  let wins = 0;
  let losses = 0;

  for (const position of positions) {
    if (position.timestamp < startMs || position.timestamp > nowMs) {
      continue;
    }

    if (position.price === 1) {
      wins++;
    } else if (position.price === 0) {
      losses++;
    }
  }

  const resolvedMarkets = wins + losses;

  return {
    wins,
    losses,
    resolvedMarkets,
    winRate: resolvedMarkets > 0
      ? (wins / resolvedMarkets) * 100
      : null,
    recordVerified: true
  };
}

async function getRecord(wallet, monthStartMs) {
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

async function attachRecords(candidates, period, monthStartMs, nowMs) {
  let completed = 0;

  return mapLimit(
    candidates,
    RECORD_CONCURRENCY,
    async trader => {
      try {
        const positions = await getRecord(trader.wallet, monthStartMs);
        const record = calculatePeriodRecord(positions, period, nowMs);

        completed++;

        console.log(
          `${period.key} records ${completed}/${candidates.length}: ` +
          `${trader.name} ${record.wins}-${record.losses}`
        );

        return { ...trader, ...record };
      } catch (error) {
        completed++;

        console.error(
          `${period.key} record failed for ${trader.name}: ${error.message}`
        );

        return {
          ...trader,
          wins: null,
          losses: null,
          winRate: null,
          resolvedMarkets: null,
          recordVerified: false
        };
      }
    }
  );
}

/*
 * Rank by period-specific record, not P&L.
 * Traders with at least 10 resolved positions qualify first.
 * Records that could not be retrieved are never shown as 0-0.
 */
function rankByRecord(candidates) {
  const qualified = candidates.filter(trader =>
    trader.recordVerified === true &&
    Number.isFinite(trader.wins) &&
    Number.isFinite(trader.losses) &&
    trader.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING
  );

  const unqualified = candidates.filter(trader =>
    !(
      trader.recordVerified === true &&
      Number.isFinite(trader.wins) &&
      Number.isFinite(trader.losses) &&
      trader.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING
    )
  );

  const compareRecords = (a, b) =>
    (b.winRate ?? -1) - (a.winRate ?? -1) ||
    (b.wins ?? -1) - (a.wins ?? -1) ||
    (b.resolvedMarkets ?? -1) - (a.resolvedMarkets ?? -1) ||
    (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity);

  qualified.sort(compareRecords);

  unqualified.sort((a, b) => {
    if (a.recordVerified !== b.recordVerified) {
      return a.recordVerified ? -1 : 1;
    }

    return compareRecords(a, b);
  });

  return [...qualified, ...unqualified].map((trader, index) => ({
    ...trader,
    rank: index + 1
  }));
}

function isActivePosition(position, nowMs) {
  const size = numberOrNull(
    position.size ??
    position.current_size ??
    position.currentSize
  );

  if (size === null || size <= 0) return false;

  if (
    position.closed === true ||
    position.redeemable === true ||
    position.mergeable === true
  ) {
    return false;
  }

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

async function getActivePositions(wallet, nowMs) {
  if (activeCache.has(wallet)) return activeCache.get(wallet);

  const promise = (async () => {
    const positions = [];

    for (let offset = 0; offset <= 10000; offset += ACTIVE_PAGE_SIZE) {
      const params = new URLSearchParams({
        user: wallet,
        limit: String(ACTIVE_PAGE_SIZE),
        offset: String(offset)
      });

      const rows = asArray(
        await getJson(`${API}/positions?${params}`)
      );

      if (!rows.length) break;

      positions.push(
        ...rows.filter(position => isActivePosition(position, nowMs))
      );

      if (rows.length < ACTIVE_PAGE_SIZE) break;
    }

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

async function attachActivePositions(candidates, nowMs) {
  let completed = 0;

  const rows = await mapLimit(
    candidates,
    ACTIVE_CONCURRENCY,
    async trader => {
      try {
        const positions = await getActivePositions(trader.wallet, nowMs);
        completed++;

        console.log(
          `Active positions ${completed}/${candidates.length}: ${trader.name}`
        );

        return [trader.wallet, positions];
      } catch (error) {
        completed++;

        console.error(
          `Active positions failed for ${trader.name}: ${error.message}`
        );

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
    const rows = asArray(
      await getJson(`${API}/trades?${params}`)
    );

    return rows
      .filter(row => String(row.side || "").toUpperCase() === "BUY")
      .map(row => ({
        title: row.title || row.question || row.eventSlug || "Unknown market",
        side: "BUY",
        outcome: row.outcome || row.outcome_name || "Unknown",
        amount: numberOrNull(row.usdcSize ?? row.amount ?? row.size),
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

  /*
   * Consensus is formed from the top 1,000 record-ranked
   * candidates' current positions, not merely their recent BUY trades.
   */
  for (const trader of rankedCandidates) {
    if (
      trader.recordVerified !== true ||
      trader.resolvedMarkets < MIN_RESOLVED_FOR_RANKING
    ) {
      continue;
    }

    const positions = activePositions[trader.wallet] || [];

    for (const position of positions) {
      const condition = String(
        position.conditionId ??
        position.condition_id ??
        ""
      ).toLowerCase();

      const side = String(
        position.outcome ??
        position.outcome_name ??
        ""
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
        amount: numberOrNull(
          position.currentValue ??
          position.current_value ??
          position.initialValue ??
          position.initial_value
        ),
        buyTime: timestampMs(
          position.lastEventAt ??
          position.last_event_at
        )
      });

      const amount = numberOrNull(
        position.currentValue ??
        position.current_value ??
        position.initialValue ??
        position.initial_value
      );

      if (amount !== null) market.totalEntry += amount;

      const time = timestampMs(
        position.lastEventAt ??
        position.last_event_at
      );

      if (time !== null) {
        market.newestBuy =
          market.newestBuy === null
            ? time
            : Math.max(market.newestBuy, time);

        market.oldestBuy =
          market.oldestBuy === null
            ? time
            : Math.min(market.oldestBuy, time);
      }
    }
  }

  return [...markets.values()]
    .filter(market => market.sameSide >= 2)
    .map(market => {
      const { walletsSeen, ...result } = market;
      return result;
    })
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function buildPeriod(
  period,
  candidates,
  activePositions,
  nowMs
) {
  console.log(`Ranking ${period.key} by resolved win-loss record...`);

  const withRecords = await attachRecords(
    candidates,
    period,
    nowMs - 30 * 24 * 60 * 60 * 1000,
    nowMs
  );

  const ranked = rankByRecord(withRecords);
  const leaders = ranked.slice(0, LEADER_COUNT);

  const recordQualifiedCount = ranked.filter(trader =>
    trader.recordVerified === true &&
    trader.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING
  ).length;

  console.log(
    `${period.key}: ${recordQualifiedCount} traders qualify with ` +
    `${MIN_RESOLVED_FOR_RANKING}+ resolved positions`
  );

  const tradeLists = await mapLimit(
    leaders,
    TRADE_CONCURRENCY,
    async trader => [
      trader.wallet,
      await getRecentTrades(trader.wallet)
    ]
  );

  const traderActiveTrades = Object.fromEntries(tradeLists);

  const consensus = buildConsensus(ranked, activePositions);

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      `Resolved win-loss record over the last ${period.days} days, ` +
      `within the top ${CANDIDATE_COUNT} P&L-ranked candidate traders`,
    recordBasis:
      "Closed positions with confirmed settlement price 1 (win) or 0 (loss)",
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: true,
    leaders,
    traderActiveTrades,
    traderActivePositions: activePositions,
    consensus
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${leaders.length} leaders, ` +
    `${consensus.length} consensus markets`
  );
}

async function main() {
  const nowMs = Date.now();
  const monthStartMs = nowMs - 30 * 24 * 60 * 60 * 1000;

  console.log("Loading weekly P&L candidate pool...");
  const weekCandidates = await getCandidateLeaderboard(PERIODS[0]);
  console.log(`week: retrieved ${weekCandidates.length} candidates`);

  console.log("Loading monthly P&L candidate pool...");
  const monthCandidates = await getCandidateLeaderboard(PERIODS[1]);
  console.log(`month: retrieved ${monthCandidates.length} candidates`);

  const candidateWallets = new Map();

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!candidateWallets.has(trader.wallet)) {
      candidateWallets.set(trader.wallet, trader);
    }
  }

  const uniqueCandidates = [...candidateWallets.values()];

  console.log(
    `Checking recent records for ${uniqueCandidates.length} unique traders...`
  );

  /*
   * Fetch active positions once per unique wallet and reuse
   * the results for both period JSON files.
   */
  console.log("Loading current active positions for candidate traders...");

  const activePositions = await attachActivePositions(
    uniqueCandidates,
    nowMs
  );

  /*
   * Build both period rankings. Recent closed-position history
   * is cached, so a wallet appearing in both pools is not scanned twice.
   */
  await buildPeriod(
    PERIODS[0],
    weekCandidates,
    activePositions,
    nowMs
  );

  await buildPeriod(
    PERIODS[1],
    monthCandidates,
    activePositions,
    nowMs
  );

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
