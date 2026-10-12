
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const LEADER_COUNT = 1000;
const PAGE_SIZE = 50;
const TRADE_LIMIT = 50;
const MAX_CONCURRENCY = 4;
const REQUEST_DELAY_MS = 200;
const MIN_RESOLVED_FOR_RANKING = 10;

const diagnostics = {
  apiErrors: [],
  tradeErrors: 0,
  positionErrors: 0,
  recordErrors: 0
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function logApiError(type, wallet, error) {
  diagnostics.apiErrors.push({
    type,
    wallet,
    message: String(error?.message || error).slice(0, 400)
  });

  if (diagnostics.apiErrors.length > 200) {
    diagnostics.apiErrors.shift();
  }

  if (type === "trades") diagnostics.tradeErrors++;
  if (type === "positions") diagnostics.positionErrors++;
  if (type === "records") diagnostics.recordErrors++;

  console.error(`[${type}] ${wallet}: ${error.message}`);
}

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/4.0"
        },
        signal: AbortSignal.timeout(25000)
      });

      if (!response.ok) {
        const body = await response.text();
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 250)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (attempt < attempts - 1) {
        console.warn(`Retry ${attempt + 1}: ${error.message}`);
        await sleep(750 * (attempt + 1));
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
  ).trim().toLowerCase();
}

function normalizeLeader(row, rank) {
  return {
    rank,
    wallet: walletOf(row),
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      "Unknown trader",
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
  const leaders = [];
  const seen = new Set();

  for (let offset = 0; offset < LEADER_COUNT; offset += PAGE_SIZE) {
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

    if (!rows.length) {
      if (offset === 0) {
        throw new Error(`${period.key} leaderboard returned no data`);
      }
      break;
    }

    for (const row of rows) {
      const leader = normalizeLeader(row, leaders.length + 1);

      if (!leader.wallet || seen.has(leader.wallet)) continue;

      seen.add(leader.wallet);
      leaders.push(leader);
    }

    if (rows.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return leaders.slice(0, LEADER_COUNT).map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

function timestampOf(row) {
  const raw = row?.timestamp ?? row?.createdAt ?? row?.created_at ?? row?.time;
  const numeric = numberOrNull(raw);

  if (numeric !== null) {
    return numeric > 100000000000 ? Math.floor(numeric / 1000) : numeric;
  }

  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }

  return null;
}

function amountOf(row) {
  const explicit = numberOrNull(
    row?.usdcSize ??
    row?.usdc_size ??
    row?.amount_usdc ??
    row?.cashAmount
  );

  if (explicit !== null) return explicit;

  const size = numberOrNull(row?.size);
  const price = numberOrNull(row?.price);

  return size !== null && price !== null ? size * price : null;
}

function normalizeTrade(row) {
  return {
    title:
      row?.title ||
      row?.question ||
      row?.market ||
      row?.eventSlug ||
      row?.event_slug ||
      "Unknown market",
    side: String(row?.side || "").toUpperCase(),
    outcome:
      row?.outcome ??
      row?.outcome_name ??
      row?.outcomeName ??
      "Unknown",
    amount: amountOf(row),
    buyTime: timestampOf(row),
    conditionId: String(
      row?.conditionId ??
      row?.condition_id ??
      row?.condition ??
      ""
    ).toLowerCase() || null,
    transactionHash: row?.transactionHash ?? row?.transaction_hash ?? null
  };
}

async function getRecentTrades(wallet) {
  if (!wallet) return [];

  const params = new URLSearchParams({
    user: wallet,
    limit: String(TRADE_LIMIT),
    side: "BUY"
  });

  try {
    const response = await getJson(`${API}/v2/trades?${params}`);

    return asArray(response)
      .map(normalizeTrade)
      .filter(t => t.side === "BUY")
      .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
  } catch (error) {
    logApiError("trades", wallet, error);
    return [];
  }
}

async function getOpenPositions(wallet) {
  if (!wallet) return [];

  const params = new URLSearchParams({
    user: wallet,
    status: "OPEN",
    limit: "50"
  });

  try {
    const response = await getJson(`${API}/v2/positions?${params}`);

    return asArray(response).map(row => ({
      title: row.title || row.market || row.question || "Unknown market",
      conditionId: row.condition_id ?? row.conditionId ?? null,
      outcome: row.outcome ?? "Unknown",
      size: numberOrNull(row.current_size ?? row.size),
      currentValue: numberOrNull(row.current_value ?? row.currentValue),
      initialValue: numberOrNull(row.entry_cost_usdc ?? row.initialValue),
      pnl: numberOrNull(row.unrealized_pnl ?? row.cashPnl ?? row.pnl),
      endDate: row.end_date ?? row.endDate ?? null
    }));
  } catch (error) {
    logApiError("positions", wallet, error);
    return [];
  }
}

/*
 * Count closed positions with positive realized P&L as wins and
 * negative realized P&L as losses. This is an all-time realized-position
 * record, not a weekly/monthly market-resolution record.
 */
async function getClosedRecord(wallet) {
  if (!wallet) return null;

  try {
    const records = [];
    let cursor = null;
    let pages = 0;

    do {
      const params = new URLSearchParams({
        user: wallet,
        status: "CLOSED",
        limit: "1000",
        sort_by: "REALIZED_PNL",
        sort_direction: "DESC"
      });

      if (cursor) params.set("cursor", cursor);

      const response = await getJson(`${API}/v2/positions?${params}`);
      const rows = asArray(response);

      records.push(...rows);

      cursor = response?.pagination?.next_cursor ?? null;
      pages++;

      if (!cursor || rows.length === 0 || pages >= 10) break;
      await sleep(REQUEST_DELAY_MS);
    } while (cursor);

    let wins = 0;
    let losses = 0;

    for (const row of records) {
      const pnl = numberOrNull(row.realized_pnl ?? row.realizedPnl);

      if (pnl === null || Math.abs(pnl) < 0.000001) continue;
      if (pnl > 0) wins++;
      else losses++;
    }

    return {
      wins,
      losses,
      resolvedMarkets: wins + losses,
      winRate: wins + losses ? 100 * wins / (wins + losses) : null,
      recordVerified: true,
      recordBasis: "all_time_closed_positions_realized_pnl"
    };
  } catch (error) {
    logApiError("records", wallet, error);
    return null;
  }
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;

      try {
        results[index] = await callback(items[index], index);
      } catch (error) {
        console.error(`Worker failed: ${error.message}`);
        results[index] = null;
      }

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

function buildConsensus(leaders, traderActiveTrades) {
  const markets = new Map();

  for (const leader of leaders) {
    for (const trade of traderActiveTrades[leader.wallet] || []) {
      const condition = String(trade.conditionId || "").toLowerCase();
      const outcome = String(trade.outcome || "").trim();

      if (!condition || !outcome || outcome === "Unknown") continue;

      const key = `${condition}|${outcome.toLowerCase()}`;

      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: trade.title || "Unknown market",
          side: outcome,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }

      const market = markets.get(key);
      if (market.walletsSeen.has(leader.wallet)) continue;

      market.walletsSeen.add(leader.wallet);
      market.sameSide++;

      const amount = numberOrNull(trade.amount);
      const buyTime = numberOrNull(trade.buyTime);

      market.traders.push({
        name: leader.name,
        wallet: leader.wallet,
        rank: leader.rank,
        wins: leader.wins,
        losses: leader.losses,
        winRate: leader.winRate,
        resolvedMarkets: leader.resolvedMarkets,
        recordVerified: leader.recordVerified,
        pnl: leader.pnl,
        volume: leader.volume,
        side: outcome,
        amount,
        buyTime
      });

      if (amount !== null) market.totalEntry += amount;

      if (buyTime !== null) {
        market.newestBuy = market.newestBuy === null
          ? buyTime
          : Math.max(market.newestBuy, buyTime);
      }
    }
  }

  return [...markets.values()]
    .filter(m => m.sameSide >= 2)
    .map(({ walletsSeen, ...market }) => market)
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

function rankLeaders(leaders) {
  const qualified = [];
  const unqualified = [];

  for (const leader of leaders) {
    const r = leader.record;
    delete leader.record;

    if (
      r?.recordVerified &&
      r.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING
    ) {
      qualified.push({ ...leader, ...r });
    } else {
      unqualified.push({
        ...leader,
        ...(r || {}),
        wins: r?.wins ?? null,
        losses: r?.losses ?? null,
        winRate: r?.winRate ?? null,
        resolvedMarkets: r?.resolvedMarkets ?? null,
        recordVerified: r?.recordVerified ?? false
      });
    }
  }

  qualified.sort((a, b) =>
    (b.winRate ?? -1) - (a.winRate ?? -1) ||
    (b.wins ?? 0) - (a.wins ?? 0) ||
    (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)
  );

  return [...qualified, ...unqualified].map((leader, index) => ({
    ...leader,
    rank: index + 1
  }));
}

async function buildPeriod(period) {
  console.log(`\n========== ${period.key.toUpperCase()} ==========`);

  const leaders = await getLeaderboard(period);
  if (!leaders.length) throw new Error(`${period.key} leaderboard is empty`);

  console.log(`Retrieved ${leaders.length} leaderboard candidates.`);

  const results = await mapLimit(leaders, MAX_CONCURRENCY, async leader => {
    const [trades, positions, record] = await Promise.all([
      getRecentTrades(leader.wallet),
      getOpenPositions(leader.wallet),
      getClosedRecord(leader.wallet)
    ]);

    return {
      wallet: leader.wallet,
      trades,
      positions,
      record
    };
  });

  const traderActiveTrades = {};
  const traderActivePositions = {};
  const recordByWallet = {};

  for (const result of results.filter(Boolean)) {
    traderActiveTrades[result.wallet] = result.trades;
    traderActivePositions[result.wallet] = result.positions;
    recordByWallet[result.wallet] = result.record;
  }

  const enrichedLeaders = leaders.map(leader => ({
    ...leader,
    record: recordByWallet[leader.wallet] || null
  }));

  const rankedLeaders = rankLeaders(enrichedLeaders);

  const totalBuys = Object.values(traderActiveTrades)
    .reduce((sum, trades) => sum + trades.length, 0);

  const totalPositions = Object.values(traderActivePositions)
    .reduce((sum, positions) => sum + positions.length, 0);

  const consensus = buildConsensus(rankedLeaders, traderActiveTrades);

  const output = {
    generatedAt: new Date().toISOString(),
    period: period.key,
    rankingBasis:
      "Leaderboard candidates ordered by all-time realized closed-position record where available, then other candidates.",
    recordBasis: "All-time realized P&L of closed positions; not weekly/monthly resolved-market results.",
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    recordRankingEnabled: true,
    leaders: rankedLeaders,
    traderActiveTrades,
    traderActivePositions,
    consensus,
    diagnostics: {
      leaderCount: leaders.length,
      tradersWithRecords: Object.values(recordByWallet)
        .filter(r => r?.recordVerified).length,
      tradersWithBuys: Object.values(traderActiveTrades)
        .filter(trades => trades.length > 0).length,
      totalBuyTrades: totalBuys,
      tradersWithPositions: Object.values(traderActivePositions)
        .filter(positions => positions.length > 0).length,
      totalPositions,
      consensusMarkets: consensus.length,
      tradeErrors: diagnostics.tradeErrors,
      positionErrors: diagnostics.positionErrors,
      recordErrors: diagnostics.recordErrors,
      apiErrors: diagnostics.apiErrors.slice(-100)
    }
  };

  const filename = `${period.key}.json`;
  const tempFilename = `${filename}.tmp`;

  await fs.writeFile(tempFilename, JSON.stringify(output), "utf8");
  await fs.rename(tempFilename, filename);

  console.log(`Wrote ${filename}`);
  console.log(`  Leader candidates: ${leaders.length}`);
  console.log(`  Traders with records: ${output.diagnostics.tradersWithRecords}`);
  console.log(`  BUY trades: ${totalBuys}`);
  console.log(`  Open positions: ${totalPositions}`);
  console.log(`  Consensus markets: ${consensus.length}`);
  console.log(`  API errors: ${diagnostics.apiErrors.length}`);
}

async function main() {
  for (const period of PERIODS) {
    diagnostics.apiErrors = [];
    diagnostics.tradeErrors = 0;
    diagnostics.positionErrors = 0;
    diagnostics.recordErrors = 0;

    await buildPeriod(period);
  }

  console.log("\nUpdate finished.");
}

main().catch(error => {
  console.error("\nPolymarket update failed:", error);
  process.exitCode = 1;
});
