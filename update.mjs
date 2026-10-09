
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const OUT = process.env.OUTPUT_DIR || ".";
const TOP_N = 100;
const TRADE_LIMIT = 50;
const CONCURRENCY = 5;

const PERIODS = [
  { key: "week", apiPeriod: "WEEK" },
  { key: "month", apiPeriod: "MONTH" },
  { key: "quarter", apiPeriod: "THREE_MONTH" },
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getJson(url, attempts = 5) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          accept: "application/json",
          "user-agent": "polymarket-top100-github-actions/1.0",
        },
        signal: AbortSignal.timeout(30000),
      });

      if (response.status === 429 || response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after") || 0);
        await sleep(Math.max(retryAfter * 1000, 1000 * (attempt + 1)));
        continue;
      }

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 250)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await sleep(1000 * (attempt + 1));
      }
    }
  }

  throw lastError || new Error(`Request failed: ${url}`);
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function firstDefined(...values) {
  return values.find((value) => value !== null && value !== undefined);
}

function walletOf(row) {
  return String(
    firstDefined(row.proxyWallet, row.proxy_wallet, row.wallet, row.address, "")
  ).toLowerCase();
}

function normalizeLeader(row, index) {
  const wallet = walletOf(row);
  const pnl = numberOrNull(firstDefined(row.pnl, row.profit, row.PnL));

  return {
    rank: Number(row.rank) || index + 1,
    wallet,
    name:
      firstDefined(row.userName, row.username, row.name, row.pseudonym) ||
      `${wallet.slice(0, 6)}…${wallet.slice(-4)}`,
    profileImage: firstDefined(row.profileImage, row.profile_image, null),
    pnl,
    volume: numberOrNull(row.vol ?? row.volume),
    // The leaderboard endpoint does not establish resolved win/loss counts.
    wins: null,
    losses: null,
    decided: null,
    winRate: null,
  };
}

async function getLeaderboard(period) {
  const params = new URLSearchParams({
    category: "OVERALL",
    timePeriod: period.apiPeriod,
    orderBy: "PNL",
    limit: String(TOP_N),
    offset: "0",
  });

  const result = await getJson(`${API}/v1/leaderboard?${params}`);
  const rows = Array.isArray(result) ? result : result?.data;

  if (!Array.isArray(rows)) {
    throw new Error(
      `Unexpected leaderboard response for ${period.key}: ${JSON.stringify(result).slice(0, 300)}`
    );
  }

  return rows
    .map(normalizeLeader)
    .filter((leader) => leader.wallet.startsWith("0x") && leader.wallet.length === 42)
    .slice(0, TOP_N)
    .map((leader, index) => ({ ...leader, rank: index + 1 }));
}

async function getRecentTrades(wallet) {
  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT),
  });

  const result = await getJson(`${API}/trades?${params}`);
  const rows = Array.isArray(result) ? result : result?.data;

  if (!Array.isArray(rows)) return [];

  return rows
    .filter((trade) => {
      const action = String(trade.side || trade.action || "").toUpperCase();
      return action === "BUY";
    })
    .map((trade) => {
      const price = numberOrNull(trade.price);
      const size = numberOrNull(trade.size ?? trade.amount);
      const timestamp = numberOrNull(trade.timestamp ?? trade.time);

      return {
        title: firstDefined(trade.title, trade.market, trade.question, "Unknown market"),
        conditionId: firstDefined(
          trade.conditionId,
          trade.condition_id,
          trade.marketId,
          trade.market_id,
          null
        ),
        slug: firstDefined(trade.slug, trade.eventSlug, trade.event_slug, null),
        outcome: firstDefined(trade.outcome, trade.outcomeName, trade.outcome_name, null),
        side: "BUY",
        price,
        size,
        amount: price !== null && size !== null ? price * size : null,
        buyTime: timestamp,
        timestamp,
        transactionHash: firstDefined(
          trade.transactionHash,
          trade.transaction_hash,
          trade.id,
          null
        ),
      };
    })
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
}

async function mapLimit(items, limit, callback) {
  const output = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await callback(items[index], index);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker())
  );

  return output;
}

function buildConsensus(leaders, tradesByWallet) {
  const groups = new Map();
  const rankByWallet = new Map(leaders.map((leader) => [leader.wallet, leader]));

  for (const leader of leaders) {
    const trades = tradesByWallet[leader.wallet] || [];

    for (const trade of trades) {
      const marketId = trade.conditionId || trade.slug || trade.title;
      const outcome = String(trade.outcome || "").trim().toLowerCase();

      if (!marketId || !outcome) continue;

      const key = `${marketId}::${outcome}`;
      if (!groups.has(key)) {
        groups.set(key, {
          key,
          title: trade.title,
          conditionId: trade.conditionId,
          slug: trade.slug,
          side: trade.outcome,
          traders: new Map(),
          totalEntry: 0,
          newestBuy: 0,
          oldestBuy: Infinity,
        });
      }

      const group = groups.get(key);

      // One contribution per trader per market/outcome.
      if (group.traders.has(leader.wallet)) continue;

      group.traders.set(leader.wallet, {
        name: leader.name,
        rank: leader.rank,
        wins: null,
        losses: null,
        decided: null,
        winRate: null,
        pnl: leader.pnl,
        side: trade.outcome,
        amount: trade.amount,
        buyTime: trade.buyTime,
      });

      if (trade.amount !== null) group.totalEntry += trade.amount;
      group.newestBuy = Math.max(group.newestBuy, trade.timestamp || 0);
      group.oldestBuy = Math.min(group.oldestBuy, trade.timestamp || Infinity);
    }
  }

  return [...groups.values()]
    .filter((group) => group.traders.size >= 2)
    .map((group) => ({
      title: group.title,
      conditionId: group.conditionId,
      slug: group.slug,
      side: group.side,
      sameSide: group.traders.size,
      totalEntry: group.totalEntry,
      newestBuy: group.newestBuy || null,
      oldestBuy: Number.isFinite(group.oldestBuy) ? group.oldestBuy : null,
      traders: [...group.traders.values()].sort((a, b) => a.rank - b.rank),
    }))
    .sort((a, b) => b.sameSide - a.sameSide || b.newestBuy - a.newestBuy);
}

async function buildPeriod(period) {
  console.log(`Loading ${period.key} leaderboard...`);
  const leaders = await getLeaderboard(period);

  console.log(`${period.key}: received ${leaders.length} leaders`);

  const tradesByWallet = {};
  const results = await mapLimit(leaders, CONCURRENCY, async (leader, index) => {
    try {
      const trades = await getRecentTrades(leader.wallet);
      console.log(
        `${period.key}: trades ${index + 1}/${leaders.length} (${leader.name})`
      );
      return [leader.wallet, trades];
    } catch (error) {
      console.warn(`Trade lookup failed for ${leader.wallet}: ${error.message}`);
      return [leader.wallet, []];
    }
  });

  for (const [wallet, trades] of results) {
    tradesByWallet[wallet] = trades;
  }

  const traderActiveTrades = {};
  for (const leader of leaders) {
    traderActiveTrades[leader.wallet] = tradesByWallet[leader.wallet] || [];
  }

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    leaders,
    traderActiveTrades,
    consensus: buildConsensus(leaders, tradesByWallet),
  };

  await fs.mkdir(OUT, { recursive: true });
  await fs.writeFile(
    `${OUT}/${period.key}.json`,
    JSON.stringify(output, null, 2) + "\n"
  );

  console.log(
    `Wrote ${period.key}.json (${leaders.length} leaders, ${output.consensus.length} consensus markets)`
  );
}

async function main() {
  for (const period of PERIODS) {
    await buildPeriod(period);
  }
  console.log("All period files generated.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
