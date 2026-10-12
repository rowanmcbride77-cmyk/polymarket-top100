
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const LEADER_COUNT = 100;
const CONSENSUS_COUNT = 250;
const PAGE_SIZE = 50;
const CONCURRENCY = 8;
const MIN_RESOLVED_FOR_RANKING = 10;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function request(path, params = {}) {
  const url = new URL(`${API}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(url, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(20000),
      });

      if (response.ok) return await response.json();

      if (![429, 500, 502, 503, 504].includes(response.status)) {
        throw new Error(`${response.status} ${url.pathname}`);
      }

      await sleep(500 * (attempt + 1));
    } catch (error) {
      if (attempt === 2) throw error;
      await sleep(500 * (attempt + 1));
    }
  }

  throw new Error(`Request failed: ${url.pathname}`);
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  if (Array.isArray(value?.data?.data)) return value.data.data;
  return [];
}

async function mapLimit(items, limit, callback) {
  const output = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;

      try {
        output[index] = await callback(items[index], index);
      } catch (error) {
        console.warn("Skipped one lookup:", error.message);
        output[index] = null;
      }
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(limit, items.length) },
      () => worker()
    )
  );

  return output;
}

function number(value) {
  const result = Number(value);
  return Number.isFinite(result) ? result : 0;
}

function walletOf(item) {
  return String(
    item?.proxyWallet ??
    item?.proxy_wallet ??
    item?.user ??
    item?.wallet ??
    item?.address ??
    ""
  ).toLowerCase();
}

function normalizeLeader(item, rank) {
  const wallet = walletOf(item);
  const name =
    item?.userName ??
    item?.username ??
    item?.name ??
    item?.pseudonym ??
    (wallet ? `${wallet.slice(0, 6)}...${wallet.slice(-4)}` : "Unknown");

  return {
    rank,
    wallet,
    name,
    pnl: number(item?.pnl ?? item?.profit),
    volume: number(item?.vol ?? item?.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false,
  };
}

async function getLeaderboard(period) {
  const rows = [];

  for (let offset = 0; offset < LEADER_COUNT; offset += PAGE_SIZE) {
    const result = await request("/v1/leaderboard", {
      category: "OVERALL",
      timePeriod: period,
      orderBy: "PNL",
      limit: PAGE_SIZE,
      offset,
    });

    const page = asArray(result);
    rows.push(...page);

    if (page.length < PAGE_SIZE) break;
  }

  const unique = new Map();

  for (const item of rows) {
    const wallet = walletOf(item);
    if (wallet && !unique.has(wallet)) {
      unique.set(wallet, item);
    }
  }

  return [...unique.values()]
    .slice(0, LEADER_COUNT)
    .map((item, index) => normalizeLeader(item, index + 1));
}

function tradeAmount(item) {
  const direct = number(
    item?.usdcSize ??
    item?.usdc_size ??
    item?.amount ??
    item?.value
  );

  if (direct > 0) return direct;

  return number(item?.size) * number(item?.price);
}

function normalizeTrade(item, leader) {
  const timestamp = number(
    item?.timestamp ??
    item?.createdAt ??
    item?.created_at ??
    item?.time
  );

  const rawSide = String(item?.side ?? "").toUpperCase();

  return {
    wallet: leader.wallet,
    name: leader.name,
    rank: leader.rank,
    side: rawSide,
    outcome: item?.outcome ?? item?.outcomeName ?? "Unknown",
    title:
      item?.title ??
      item?.marketTitle ??
      item?.market ??
      "Unknown market",
    amount: tradeAmount(item),
    buyTime: timestamp,
    conditionId: String(
      item?.conditionId ??
      item?.condition_id ??
      item?.marketId ??
      ""
    ),
  };
}

async function getRecentTrades(leader) {
  if (!leader.wallet) return [];

  let result;

  try {
    result = await request("/v2/trades", {
      user: leader.wallet,
      limit: 50,
    });
  } catch {
    result = await request("/trades", {
      user: leader.wallet,
      limit: 50,
    });
  }

  return asArray(result)
    .map((item) => normalizeTrade(item, leader))
    .filter((trade) => trade.side === "BUY" && trade.conditionId)
    .sort((a, b) => b.buyTime - a.buyTime);
}

async function getClosedRecord(leader) {
  if (!leader.wallet) return leader;

  try {
    const result = await request("/v2/positions", {
      user: leader.wallet,
      status: "CLOSED",
      limit: 1000,
    });

    const positions = asArray(result);
    let wins = 0;
    let losses = 0;

    for (const position of positions) {
      const pnl = Number(
        position?.realized_pnl ??
        position?.realizedPnl ??
        position?.cashPnl
      );

      if (!Number.isFinite(pnl) || pnl === 0) continue;
      if (pnl > 0) wins++;
      else losses++;
    }

    const resolvedMarkets = wins + losses;

    return {
      ...leader,
      wins,
      losses,
      winRate: resolvedMarkets
        ? (wins / resolvedMarkets) * 100
        : null,
      resolvedMarkets,
      recordVerified: resolvedMarkets > 0,
    };
  } catch (error) {
    console.warn(
      `Record unavailable for ${leader.name}: ${error.message}`
    );
    return leader;
  }
}

function sortByRecord(leaders) {
  return [...leaders].sort((a, b) => {
    const aQualified =
      a.recordVerified &&
      a.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING;
    const bQualified =
      b.recordVerified &&
      b.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING;

    if (aQualified !== bQualified) return aQualified ? -1 : 1;

    if (aQualified && bQualified) {
      const aRatio = a.losses === 0 ? a.wins : a.wins / a.losses;
      const bRatio = b.losses === 0 ? b.wins : b.wins / b.losses;

      if (bRatio !== aRatio) return bRatio - aRatio;
      if (b.winRate !== a.winRate) return b.winRate - a.winRate;
    }

    return b.pnl - a.pnl;
  }).map((leader, index) => ({ ...leader, rank: index + 1 }));
}

function buildConsensus(leaders, tradesByWallet) {
  const groups = new Map();

  for (const leader of leaders) {
    const trades = tradesByWallet[leader.wallet] ?? [];
    const seenForTrader = new Set();

    for (const trade of trades) {
      if (trade.side !== "BUY" || !trade.conditionId) continue;

      const outcome = String(trade.outcome ?? "Unknown").toLowerCase();
      const key = `${trade.conditionId}|${outcome}`;

      if (seenForTrader.has(key)) continue;
      seenForTrader.add(key);

      if (!groups.has(key)) {
        groups.set(key, {
          conditionId: trade.conditionId,
          title: trade.title,
          side: trade.outcome,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: 0,
          traders: [],
          traderWallets: new Set(),
        });
      }

      const group = groups.get(key);
      if (group.traderWallets.has(leader.wallet)) continue;

      group.traderWallets.add(leader.wallet);
      group.sameSide++;
      group.totalEntry += trade.amount;
      group.newestBuy = Math.max(group.newestBuy, trade.buyTime);

      group.traders.push({
        name: leader.name,
        wallet: leader.wallet,
        rank: leader.rank,
        wins: leader.wins,
        losses: leader.losses,
        winRate: leader.winRate,
        pnl: leader.pnl,
        amount: trade.amount,
        buyTime: trade.buyTime,
      });
    }
  }

  return [...groups.values()]
    .filter((group) => group.sameSide >= 2)
    .map(({ traderWallets, traders, ...group }) => ({
      ...group,
      traders: traders.sort((a, b) => a.rank - b.rank),
    }))
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      b.newestBuy - a.newestBuy
    )
    .slice(0, 100);
}

async function buildPeriod(period, makeConsensus = false) {
  console.log(`Loading ${period} leaderboard...`);

  const initialLeaders = await getLeaderboard(period);

  if (!initialLeaders.length) {
    throw new Error(`The ${period} leaderboard returned no traders.`);
  }

  // Closed records are only requested for the displayed top 100.
  console.log(`Checking records for ${initialLeaders.length} traders...`);

  const checkedLeaders = await mapLimit(
    initialLeaders,
    CONCURRENCY,
    (leader) => getClosedRecord(leader)
  );

  const ranked = sortByRecord(
    checkedLeaders.filter(Boolean)
  );

  const leaders = ranked.slice(0, LEADER_COUNT);

  // Consensus uses recent buys from at most 250 weekly leaders.
  const tradeLeaders = makeConsensus
    ? ranked.slice(0, CONSENSUS_COUNT)
    : leaders;

  console.log(
    `Loading recent trades for ${tradeLeaders.length} traders...`
  );

  const tradeResults = await mapLimit(
    tradeLeaders,
    CONCURRENCY,
    async (leader) => {
      await sleep(100);
      return {
        wallet: leader.wallet,
        trades: await getRecentTrades(leader),
      };
    }
  );

  const tradesByWallet = {};

  for (const item of tradeResults.filter(Boolean)) {
    tradesByWallet[item.wallet] = item.trades;
  }

  const traderActiveTrades = {};

  for (const leader of leaders) {
    traderActiveTrades[leader.wallet] =
      (tradesByWallet[leader.wallet] ?? []).slice(0, 30);
  }

  const consensus = makeConsensus
    ? buildConsensus(tradeLeaders, tradesByWallet)
    : [];

  return {
    updatedAt: new Date().toISOString(),
    period,
    recordRankingEnabled: true,
    minResolvedForRanking: MIN_RESOLVED_FOR_RANKING,
    leaders,
    consensus,
    traderActiveTrades,
  };
}

async function main() {
  const status = {
    startedAt: new Date().toISOString(),
    ok: false,
    errors: [],
  };

  try {
    const week = await buildPeriod("WEEK", true);
    await fs.writeFile("week.json", JSON.stringify(week, null, 2));

    const month = await buildPeriod("MONTH", false);
    await fs.writeFile("month.json", JSON.stringify(month, null, 2));

    status.ok = true;
    status.finishedAt = new Date().toISOString();
    status.weekLeaders = week.leaders.length;
    status.monthLeaders = month.leaders.length;
    status.consensusMarkets = week.consensus.length;

    await fs.writeFile("status.json", JSON.stringify(status, null, 2));

    console.log(
      `Done. Week: ${week.leaders.length}; ` +
      `month: ${month.leaders.length}; ` +
      `consensus: ${week.consensus.length}.`
    );
  } catch (error) {
    status.errors.push(error.message);
    status.finishedAt = new Date().toISOString();
    await fs.writeFile("status.json", JSON.stringify(status, null, 2));
    console.error(error);
    process.exitCode = 1;
  }
}

main();
