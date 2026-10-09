
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "week" },
  { key: "month", api: "month" }
];

const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const TRADE_LIMIT = 50;
const CLOSED_POSITION_PAGE_SIZE = 50;
const REDEEMABLE_POSITION_PAGE_SIZE = 500;
const MAX_CONCURRENCY = 5;
const REQUEST_DELAY_MS = 150;
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
        signal: AbortSignal.timeout(25000)
      });

      if (response.status === 429 || response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after"));

        await sleep(
          Number.isFinite(retryAfter) && retryAfter > 0
            ? Math.min(retryAfter * 1000, 15000)
            : 1000 * (attempt + 1)
        );

        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 300)}`);
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

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
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

    // These are populated from position data below.
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
      await getJson(`${API}/v1/leaderboard?${params.toString()}`)
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

  return combined.slice(0, LEADER_COUNT).map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

/*
 * A position is counted as a win only when its settlement price is 1,
 * and as a loss only when its settlement price is 0.
 *
 * Other prices are not treated as settled results.
 */
function settledResult(row) {
  const raw =
    row?.curPrice ??
    row?.currentPrice ??
    row?.current_price;

  const price = numberOrNull(raw);

  if (price === 1) return "win";
  if (price === 0) return "loss";

  return null;
}

/*
 * Identify the same position across the closed-position and redeemable
 * endpoints so it is not counted twice.
 */
function positionKey(row) {
  const condition = String(
    row?.conditionId ??
    row?.condition_id ??
    ""
  ).toLowerCase();

  const asset = String(
    row?.asset ??
    row?.tokenId ??
    row?.token_id ??
    ""
  );

  const outcomeIndex = row?.outcomeIndex ?? row?.outcome_index;
  const outcome = String(row?.outcome ?? "").toLowerCase();

  if (!condition) return "";

  const identity = asset || (outcomeIndex ?? outcome);

  if (identity === "") return "";

  return `${condition}|${identity}`;
}

/*
 * Fetch closed positions with offset pagination.
 * Polymarket's closed-positions endpoint supports up to 50 rows per page.
 */
async function getClosedPositions(wallet) {
  const positions = [];

  for (
    let offset = 0;
    offset <= 100000;
    offset += CLOSED_POSITION_PAGE_SIZE
  ) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(CLOSED_POSITION_PAGE_SIZE),
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = asArray(
      await getJson(`${API}/closed-positions?${params.toString()}`)
    );

    positions.push(...rows);

    if (rows.length < CLOSED_POSITION_PAGE_SIZE) break;

    await sleep(REQUEST_DELAY_MS);
  }

  return positions;
}

/*
 * Fetch redeemable positions as well. This catches settled winning
 * positions that a trader still holds instead of having closed.
 */
async function getRedeemablePositions(wallet) {
  const positions = [];

  for (
    let offset = 0;
    offset <= 10000;
    offset += REDEEMABLE_POSITION_PAGE_SIZE
  ) {
    const params = new URLSearchParams({
      user: wallet,
      redeemable: "true",
      limit: String(REDEEMABLE_POSITION_PAGE_SIZE),
      offset: String(offset)
    });

    const rows = asArray(
      await getJson(`${API}/positions?${params.toString()}`)
    );

    positions.push(...rows);

    if (rows.length < REDEEMABLE_POSITION_PAGE_SIZE) break;

    await sleep(REQUEST_DELAY_MS);
  }

  return positions;
}

/*
 * Combine the two sources and deduplicate each position.
 *
 * A successful lookup with no settled positions produces a verified 0-0.
 * A failed lookup is handled separately and remains unverified.
 */
async function getTraderRecord(wallet) {
  if (!wallet) {
    throw new Error("Missing wallet address");
  }

  const [closed, redeemable] = await Promise.all([
    getClosedPositions(wallet),
    getRedeemablePositions(wallet)
  ]);

  const unique = new Map();

  for (const row of [...closed, ...redeemable]) {
    const result = settledResult(row);
    const key = positionKey(row);

    if (!key || !result) continue;

    unique.set(key, { key, result });
  }

  let wins = 0;
  let losses = 0;

  for (const position of unique.values()) {
    if (position.result === "win") {
      wins++;
    } else if (position.result === "loss") {
      losses++;
    }
  }

  const resolvedMarkets = wins + losses;

  return {
    wins,
    losses,
    resolvedMarkets,
    winRate:
      resolvedMarkets > 0
        ? (wins / resolvedMarkets) * 100
        : null,
    recordVerified: true
  };
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

async function attachTraderRecords(leaders) {
  let completed = 0;

  return mapLimit(leaders, MAX_CONCURRENCY, async leader => {
    try {
      const record = await getTraderRecord(leader.wallet);

      completed++;

      console.log(
        `Records ${completed}/${leaders.length}: ` +
        `${leader.name} ${record.wins}-${record.losses}`
      );

      return {
        ...leader,
        ...record
      };
    } catch (error) {
      completed++;

      console.error(
        `Record lookup failed for ${leader.wallet}: ${error.message}`
      );

      return {
        ...leader,
        wins: null,
        losses: null,
        winRate: null,
        resolvedMarkets: null,
        recordVerified: false,
        recordError: error.message
      };
    }
  });
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT)
  });

  try {
    const response = await getJson(
      `${API}/trades?${params.toString()}`
    );

    const rows = asArray(response);

    return rows
      .filter(
        row => String(row.side || "").toUpperCase() === "BUY"
      )
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
        transactionHash:
          row.transactionHash ||
          row.transaction_hash ||
          null
      }))
      .sort(
        (a, b) => (b.buyTime || 0) - (a.buyTime || 0)
      );
  } catch (error) {
    console.error(
      `Recent buys failed for ${wallet}: ${error.message}`
    );

    return [];
  }
}

function buildConsensus(leaders, traderActiveTrades) {
  const markets = new Map();

  for (const leader of leaders) {
    const trades = traderActiveTrades[leader.wallet] || [];

    for (const trade of trades) {
      const condition = String(
        trade.conditionId || ""
      ).toLowerCase();

      const title = String(
        trade.title || "Unknown market"
      ).trim();

      const side = String(
        trade.outcome || "Unknown"
      ).trim();

      // Do not combine unrelated markets based on titles alone.
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

      if (amount !== null) {
        market.totalEntry += amount;
      }

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
    .sort(
      (a, b) =>
        b.sameSide - a.sameSide ||
        b.totalEntry - a.totalEntry ||
        (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

/*
 * Traders with at least 10 resolved positions qualify for win-rate ranking.
 * Everyone else keeps their original P&L order, but any successfully
 * retrieved record remains visible on the site.
 */
function sortByVerifiedRecord(leaders) {
  const qualified = [];
  const unqualified = [];

  for (const leader of leaders) {
    const wins = numberOrNull(leader.wins);
    const losses = numberOrNull(leader.losses);
    const total = numberOrNull(leader.resolvedMarkets);

    if (
      leader.recordVerified === true &&
      wins !== null &&
      losses !== null &&
      total !== null &&
      total >= MIN_RESOLVED_FOR_RANKING &&
      wins >= 0 &&
      losses >= 0 &&
      wins + losses >= MIN_RESOLVED_FOR_RANKING
    ) {
      qualified.push({
        ...leader,
        winRate: (wins / (wins + losses)) * 100
      });
    } else {
      // The threshold affects eligibility for ranking, not visibility
      // of an otherwise verified record.
      unqualified.push({ ...leader });
    }
  }

  qualified.sort(
    (a, b) =>
      b.winRate - a.winRate ||
      b.wins - a.wins ||
      (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
  );

  return [...qualified, ...unqualified].map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function buildPeriod(period) {
  console.log(`Loading ${period.key} leaderboard...`);

  let leaders = await getLeaderboard(period);

  console.log(
    `${period.key}: retrieved ${leaders.length} leaders`
  );

  if (!leaders.length) {
    throw new Error(
      `The ${period.key} leaderboard returned no traders. ` +
      `Existing JSON will not be overwritten.`
    );
  }

  console.log(
    `Loading resolved win-loss records for ${leaders.length} traders...`
  );

  leaders = await attachTraderRecords(leaders);
  leaders = sortByVerifiedRecord(leaders);

  console.log(
    `Loading recent BUY trades for ${leaders.length} traders...`
  );

  const tradeLists = await mapLimit(
    leaders,
    MAX_CONCURRENCY,
    async leader => [
      leader.wallet,
      await getRecentTrades(leader.wallet)
    ]
  );

  const traderActiveTrades = Object.fromEntries(tradeLists);

  const consensus = buildConsensus(
    leaders,
    traderActiveTrades
  );

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis:
      "Resolved-position win rate for traders with at least " +
      "10 settled positions; remaining traders follow the official " +
      "P&L leaderboard order",
    minResolvedMarketsForRecordRanking:
      MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: true,
    leaders,
    traderActiveTrades,
    consensus
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(
    tempFilename,
    JSON.stringify(output),
    "utf8"
  );

  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename} (${leaders.length} leaders, ` +
    `${consensus.length} consensus markets)`
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
