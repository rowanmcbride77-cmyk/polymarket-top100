
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "week" },
  { key: "month", api: "month" }
];

const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_PAGE_SIZE = 500;
const TRADE_LIMIT = 50;
const MAX_CONCURRENCY = 5;
const POSITION_CONCURRENCY = 3;
const REQUEST_DELAY_MS = 100;
const MIN_RESOLVED_FOR_RANKING = 10;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

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
            : 750 * (attempt + 1)
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
        await sleep(300 * (attempt + 1));
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

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
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

async function getLeaderboard(period) {
  const combined = [];
  const seen = new Set();

  for (let offset = 0; offset < LEADER_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api.toUpperCase(),
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const rows = asArray(
      await getJson(`${API}/v1/leaderboard?${params}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const leader = normalizeLeader(row, combined.length + 1);

      if (!leader.wallet || seen.has(leader.wallet)) continue;

      seen.add(leader.wallet);
      combined.push(leader);
    }

    if (rows.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  // Preserve the order returned by Polymarket's P&L leaderboard.
  return combined.slice(0, LEADER_COUNT).map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

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
        title:
          row.title ||
          row.question ||
          row.eventSlug ||
          "Unknown market",
        side: "BUY",
        outcome: row.outcome || row.outcome_name || "Unknown",
        amount: numberOrNull(
          row.usdcSize ?? row.amount ?? row.size
        ),
        buyTime: numberOrNull(
          row.timestamp ?? row.createdAt ?? row.time
        ),
        conditionId: row.conditionId || row.condition_id || null,
        transactionHash: row.transactionHash || row.transaction_hash || null
      }))
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    console.error(`Recent buys failed for ${wallet}: ${error.message}`);
    return [];
  }
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
 * Read all pages for one trader and one position status.
 * The v2 API uses next_cursor rather than offset pagination.
 */
async function getPositionPages(wallet, status) {
  const positions = [];
  let cursor = null;
  const seenCursors = new Set();

  while (true) {
    const params = new URLSearchParams({
      user: wallet,
      status,
      limit: String(POSITION_PAGE_SIZE),
      sort_by: "TIMESTAMP",
      sort_direction: "DESC"
    });

    if (cursor) params.set("cursor", cursor);

    const result = await getJson(
      `${API}/v2/positions?${params}`
    );

    const rows = asArray(result);
    positions.push(...rows);

    const nextCursor =
      result?.pagination?.next_cursor ??
      result?.pagination?.nextCursor ??
      null;

    if (
      !nextCursor ||
      !result?.pagination?.has_more ||
      seenCursors.has(nextCursor)
    ) {
      break;
    }

    seenCursors.add(nextCursor);
    cursor = nextCursor;
  }

  return positions;
}

function getPositionPrice(position) {
  return numberOrNull(
    position.current_price ??
    position.currentPrice ??
    position.curPrice ??
    position.cur_price
  );
}

function getConditionId(position) {
  return String(
    position.condition_id ??
    position.conditionId ??
    ""
  ).toLowerCase();
}

/*
 * Records are counted by distinct market condition, not by raw trade count.
 *
 * A confirmed winning outcome has settlement price 1.
 * A confirmed losing outcome has settlement price 0.
 *
 * Positions that do not have one of those exact prices are not counted.
 */
function calculateRecord(closedPositions, redeemablePositions, losingPositions) {
  const markets = new Map();

  function addPosition(position, forcedResult = null) {
    const condition = getConditionId(position);
    if (!condition) return;

    let result = forcedResult;

    if (result === null) {
      const price = getPositionPrice(position);

      if (price === 1) result = "win";
      else if (price === 0) result = "loss";
      else return;
    }

    if (!markets.has(condition)) {
      markets.set(condition, new Set());
    }

    markets.get(condition).add(result);
  }

  for (const position of closedPositions) {
    addPosition(position);
  }

  // These API statuses specifically represent settled positions that
  // remain held. Their status confirms the settlement result.
  for (const position of redeemablePositions) {
    addPosition(position, "win");
  }

  for (const position of losingPositions) {
    addPosition(position, "loss");
  }

  let wins = 0;
  let losses = 0;

  for (const outcomes of markets.values()) {
    // Count each market once. If the trader held both winning and
    // losing outcomes, count it as a win rather than double-counting it.
    if (outcomes.has("win")) wins++;
    else if (outcomes.has("loss")) losses++;
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

async function getTraderRecord(wallet) {
  if (!wallet) {
    throw new Error("Missing trader wallet");
  }

  /*
   * CLOSED includes exited positions, while REDEEMABLE and
   * REDEEMABLE_LOST cover settled winning and losing positions
   * that are still held.
   */
  const [closed, redeemable, redeemableLost] = await Promise.all([
    getPositionPages(wallet, "CLOSED"),
    getPositionPages(wallet, "REDEEMABLE"),
    getPositionPages(wallet, "REDEEMABLE_LOST")
  ]);

  return calculateRecord(closed, redeemable, redeemableLost);
}

async function attachTraderRecords(leaders) {
  let completed = 0;

  const updated = await mapLimit(
    leaders,
    POSITION_CONCURRENCY,
    async leader => {
      try {
        const record = await getTraderRecord(leader.wallet);

        completed++;

        console.log(
          `Records ${completed}/${leaders.length}: ${leader.name} ` +
          `${record.wins}-${record.losses} ` +
          `(${record.resolvedMarkets} resolved markets)`
        );

        return { ...leader, ...record };
      } catch (error) {
        completed++;

        console.error(
          `Record lookup failed ${completed}/${leaders.length}: ` +
          `${leader.name}: ${error.message}`
        );

        return {
          ...leader,
          wins: null,
          losses: null,
          winRate: null,
          resolvedMarkets: null,
          recordVerified: false
        };
      }
    }
  );

  return updated;
}

function buildConsensus(leaders, traderActiveTrades) {
  const markets = new Map();

  for (const leader of leaders) {
    const trades = traderActiveTrades[leader.wallet] || [];

    for (const trade of trades) {
      const condition = String(trade.conditionId || "").toLowerCase();
      const title = String(trade.title || "Unknown market").trim();
      const side = String(trade.outcome || "Unknown").trim();

      if (!condition || side === "Unknown") continue;

      const key = `${condition}|${side.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title,
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

      if (market.walletsSeen.has(leader.wallet)) continue;

      market.walletsSeen.add(leader.wallet);
      market.sameSide++;

      market.traders.push({
        name: leader.name,
        wallet: leader.wallet,
        rank: leader.rank,
        wins: leader.wins,
        losses: leader.losses,
        winRate: leader.winRate,
        pnl: leader.pnl,
        volume: leader.volume,
        recordVerified: leader.recordVerified,
        side,
        amount: numberOrNull(trade.amount),
        buyTime: numberOrNull(trade.buyTime)
      });

      const amount = numberOrNull(trade.amount);
      if (amount !== null) market.totalEntry += amount;

      const time = numberOrNull(trade.buyTime);

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

async function buildPeriod(period) {
  console.log(`Loading ${period.key} leaderboard...`);

  const leaders = await getLeaderboard(period);

  console.log(`${period.key}: retrieved ${leaders.length} leaders`);

  if (!leaders.length) {
    throw new Error(
      `The ${period.key} leaderboard returned no traders. ` +
      "Existing JSON will not be overwritten."
    );
  }

  console.log(
    `Loading resolved win-loss records for ${leaders.length} traders...`
  );

  const leadersWithRecords = await attachTraderRecords(leaders);

  console.log(
    `Loading recent BUY trades for ${leaders.length} traders...`
  );

  const tradeLists = await mapLimit(
    leadersWithRecords,
    MAX_CONCURRENCY,
    async leader => [
      leader.wallet,
      await getRecentTrades(leader.wallet)
    ]
  );

  const traderActiveTrades = Object.fromEntries(tradeLists);
  const consensus = buildConsensus(leadersWithRecords, traderActiveTrades);

  const verifiedCount = leadersWithRecords.filter(
    leader => leader.recordVerified
  ).length;

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis: "Official Polymarket leaderboard P&L",
    recordBasis:
      "Distinct markets with confirmed 0 or 1 settlement outcomes; " +
      "unverified records remain unavailable",
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: false,
    leaders: leadersWithRecords,
    traderActiveTrades,
    consensus
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${leadersWithRecords.length} leaders, ` +
    `${verifiedCount} record lookups completed, ` +
    `${consensus.length} consensus markets`
  );
}

async function main() {
  for (const period of PERIODS) {
    await buildPeriod(period);
  }

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
