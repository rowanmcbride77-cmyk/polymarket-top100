
import fs from "node:fs/promises";
import path from "node:path";

const API = "https://data-api.polymarket.com";
const OUT = "data";

const CANDIDATES = 1000;
const TOP = 100;
const MIN_RESOLVED = 10;
const PAGE = 50;
const ACTIVE_PAGE = 500;
const CONCURRENCY = 8;
const DELAY = 100;

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" },
  { key: "threeMonth", days: 90, api: "MONTH" }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function arr(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
  return [];
}

function n(value) {
  if (value === null || value === undefined || value === "") return null;
  const valueNumber = Number(value);
  return Number.isFinite(valueNumber) ? valueNumber : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const v = Number(value);
    return v < 1e12 ? v * 1000 : v;
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

function normalize(row, index) {
  const wallet = walletOf(row);
  const name = String(
    row?.userName ??
    row?.user_name ??
    row?.username ??
    row?.name ??
    row?.pseudonym ??
    (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-5)}` : "Unknown")
  );

  return {
    rank: index + 1,
    wallet,
    name,
    pnl: n(row?.pnl ?? row?.PNL ?? row?.profitLoss) ?? 0,
    volume: n(row?.vol ?? row?.volume ?? row?.total_volume) ?? 0,
    wins: null,
    losses: null,
    resolved: 0,
    winRate: null,
    roi: null,
    recordVerified: false
  };
}

async function getJSON(url, attempts = 5) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(25000)
      });

      if (response.status === 429 || response.status >= 500) {
        throw new Error(`Polymarket HTTP ${response.status}`);
      }

      if (!response.ok) {
        throw new Error(`Polymarket HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await sleep(Math.min(1000 * (attempt + 1), 5000));
      }
    }
  }

  throw lastError || new Error("Polymarket request failed");
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
        results[index] = { error: error.message || String(error) };
      }

      await sleep(DELAY);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, worker)
  );

  return results;
}

async function getCandidates(period) {
  const result = [];
  const seen = new Set();

  for (let offset = 0; offset < CANDIDATES; offset += PAGE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period,
      orderBy: "PNL",
      limit: String(PAGE),
      offset: String(offset)
    });

    const rows = arr(
      await getJSON(`${API}/v1/leaderboard?${params}`)
    );

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalize(row, result.length);

      if (!trader.wallet || seen.has(trader.wallet)) continue;

      seen.add(trader.wallet);
      result.push(trader);
    }

    if (rows.length < PAGE) break;
  }

  return result.slice(0, CANDIDATES);
}

async function getClosedPositions(wallet, days, now) {
  const cutoff = now - days * 86400000;
  const result = [];
  const seen = new Set();
  const pageSize = 50;

  for (let offset = 0; offset < 100000; offset += pageSize) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(pageSize),
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = arr(
      await getJSON(`${API}/closed-positions?${params}`)
    );

    if (!rows.length) break;

    let reachedOlder = false;

    for (const p of rows) {
      const time = timestamp(
        p.timestamp ?? p.closedAt ?? p.closed_at
      );

      if (time === null) continue;

      if (time < cutoff) {
        reachedOlder = true;
        continue;
      }

      if (time > now) continue;

      const condition = String(
        p.conditionId ?? p.condition_id ?? p.market ?? ""
      ).toLowerCase();

      const asset = String(
        p.asset ?? p.token_id ?? p.tokenId ?? p.outcome ?? ""
      ).toLowerCase();

      const key = `${condition}|${asset}`;

      if (!condition || seen.has(key)) continue;
      seen.add(key);

      result.push({
        conditionId: condition,
        asset,
        title: p.title || p.question || "Unknown market",
        outcome: p.outcome || p.outcome_name || "Unknown",
        timestamp: time,
        realizedPnl: n(p.realizedPnl ?? p.realized_pnl ?? p.cashPnl),
        cost: n(p.totalBought ?? p.total_bought ?? p.total_cost_usdc) ?? 0
      });
    }

    if (reachedOlder || rows.length < pageSize) break;
  }

  return result;
}

function recordFromPositions(positions) {
  let wins = 0;
  let losses = 0;
  let pnl = 0;
  let invested = 0;

  for (const p of positions) {
    if (p.realizedPnl === null) continue;

    pnl += p.realizedPnl;
    invested += p.cost;

    if (p.realizedPnl > 0) wins++;
    else if (p.realizedPnl < 0) losses++;
  }

  const resolved = wins + losses;

  return {
    wins,
    losses,
    resolved,
    winRate: resolved ? wins / resolved * 100 : null,
    pnl,
    invested,
    roi: invested > 0 ? pnl / invested * 100 : null,
    recordVerified: true
  };
}

function rankTraders(rows) {
  const qualified = rows.filter(
    t => t.recordVerified && t.resolved >= MIN_RESOLVED
  );

  const other = rows.filter(
    t => !(t.recordVerified && t.resolved >= MIN_RESOLVED)
  );

  const compare = (a, b) =>
    (b.winRate ?? -1) - (a.winRate ?? -1) ||
    b.wins - a.wins ||
    b.resolved - a.resolved ||
    b.pnl - a.pnl;

  qualified.sort(compare);

  other.sort((a, b) => {
    if (a.recordVerified !== b.recordVerified) {
      return a.recordVerified ? -1 : 1;
    }

    return compare(a, b);
  });

  return [...qualified, ...other].map((t, index) => ({
    ...t,
    rank: index + 1
  }));
}

function isActive(p) {
  const size = n(p.size ?? p.current_size ?? p.currentSize) ?? 0;

  if (size <= 0 || p.redeemable === true || p.closed === true) {
    return false;
  }

  return true;
}

async function getActivePositions(wallet) {
  const result = [];
  const seen = new Set();

  for (let offset = 0; offset < 10000; offset += ACTIVE_PAGE) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(ACTIVE_PAGE),
      offset: String(offset)
    });

    const rows = arr(
      await getJSON(`${API}/positions?${params}`)
    );

    if (!rows.length) break;

    for (const p of rows) {
      if (!isActive(p)) continue;

      const condition = String(
        p.conditionId ?? p.condition_id ?? ""
      ).toLowerCase();

      const outcome = String(
        p.outcome ?? p.outcome_name ?? ""
      ).trim();

      if (!condition || !outcome) continue;

      const key = `${condition}|${outcome.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);

      result.push({
        conditionId: condition,
        title: p.title || p.question || p.market || "Unknown market",
        outcome,
        size: n(p.size ?? p.current_size) ?? 0,
        value: n(p.currentValue ?? p.current_value ?? p.value) ?? 0,
        pnl: n(p.cashPnl ?? p.cash_pnl ?? p.unrealized_pnl ?? p.total_pnl) ?? 0,
        lastEventAt: timestamp(
          p.lastEventAt ?? p.last_event_at ?? p.timestamp
        ),
        avgPrice: n(p.avgPrice ?? p.avg_price),
        endDate: p.endDate ?? p.end_date ?? null
      });
    }

    if (rows.length < ACTIVE_PAGE) break;
  }

  return result;
}

function buildConsensus(ranked, activeByWallet, now) {
  const groups = new Map();

  for (const trader of ranked.slice(0, CANDIDATES)) {
    if (!trader.recordVerified || trader.resolved < MIN_RESOLVED) continue;

    const positions = activeByWallet[trader.wallet] || [];

    for (const p of positions) {
      const side = p.outcome.trim();
      const key = `${p.conditionId}|${side.toLowerCase()}`;

      if (!groups.has(key)) {
        groups.set(key, {
          conditionId: p.conditionId,
          title: p.title,
          side,
          traders: [],
          newestBuy: null,
          totalValue: 0
        });
      }

      const group = groups.get(key);

      if (group.traders.some(t => t.wallet === trader.wallet)) continue;

      const buyTime = p.lastEventAt;

      group.traders.push({
        rank: trader.rank,
        name: trader.name,
        wallet: trader.wallet,
        wins: trader.wins,
        losses: trader.losses,
        resolved: trader.resolved,
        winRate: trader.winRate,
        pnl: trader.pnl,
        volume: trader.volume,
        amount: p.value,
        buyTime,
        outcome: side
      });

      group.totalValue += p.value;

      if (buyTime !== null) {
        group.newestBuy = group.newestBuy === null
          ? buyTime
          : Math.max(group.newestBuy, buyTime);
      }
    }
  }

  return [...groups.values()]
    .filter(g => g.traders.length >= 2)
    .map(g => ({
      ...g,
      walletCount: g.traders.length,
      hoursSinceNewestBuy: g.newestBuy === null
        ? null
        : Math.max(0, (now - g.newestBuy) / 3600000)
    }))
    .sort((a, b) =>
      b.walletCount - a.walletCount ||
      b.totalValue - a.totalValue
    );
}

async function main() {
  const now = Date.now();
  await fs.mkdir(OUT, { recursive: true });

  console.log("Loading weekly candidates...");
  const weekCandidates = await getCandidates("WEEK");

  console.log("Loading monthly candidates...");
  const monthCandidates = await getCandidates("MONTH");

  const pool = new Map();

  for (const t of [...weekCandidates, ...monthCandidates]) {
    if (t.wallet && !pool.has(t.wallet)) pool.set(t.wallet, t);
  }

  const candidates = [...pool.values()];
  console.log(`Checking ${candidates.length} unique traders.`);

  const recordCache = new Map();
  const activeResults = await mapLimit(
    candidates,
    CONCURRENCY,
    async t => {
      const [active, closed30] = await Promise.all([
        getActivePositions(t.wallet),
        getClosedPositions(t.wallet, 30, now)
      ]);

      recordCache.set(t.wallet, closed30);

      return [t.wallet, active];
    }
  );

  const activeByWallet = Object.fromEntries(
    activeResults.filter(x => Array.isArray(x) && x.length === 2)
  );

  for (const period of PERIODS) {
    const source = period.key === "week"
      ? weekCandidates
      : monthCandidates;

    const rows = await mapLimit(source, CONCURRENCY, async t => {
      try {
        let closed = recordCache.get(t.wallet);

        if (period.days > 30) {
          closed = await getClosedPositions(t.wallet, period.days, now);
        }

        if (period.days === 7) {
          const cutoff = now - 7 * 86400000;
          closed = closed.filter(p => p.timestamp >= cutoff);
        }

        const record = recordFromPositions(closed);

        return { ...t, ...record };
      } catch (error) {
        console.error(`Record failed: ${t.name}: ${error.message}`);

        return {
          ...t,
          wins: null,
          losses: null,
          resolved: 0,
          winRate: null,
          roi: null,
          recordVerified: false
        };
      }
    });

    const ranked = rankTraders(rows);
    const leaders = ranked.slice(0, TOP);
    const consensus = buildConsensus(ranked, activeByWallet, now);

    const output = {
      generatedAt: new Date(now).toISOString(),
      period: period.key,
      candidateCount: source.length,
      minResolvedMarketsForRanking: MIN_RESOLVED,
      rankingBasis: "Verified closed-position realized P&L record; candidates selected from Polymarket P&L leaderboard",
      leaders,
      traderActivePositions: Object.fromEntries(
        leaders.map(t => [t.wallet, activeByWallet[t.wallet] || []])
      ),
      consensus
    };

    const file = path.join(OUT, `${period.key}.json`);
    const temp = `${file}.tmp`;

    await fs.writeFile(temp, JSON.stringify(output), "utf8");
    await fs.rename(temp, file);

    console.log(
      `Wrote ${file}: ${leaders.length} traders; ${consensus.length} consensus markets`
    );
  }

  await fs.writeFile(
    path.join(OUT, "status.json"),
    JSON.stringify({
      ok: true,
      generatedAt: new Date(now).toISOString(),
      candidates: candidates.length
    }),
    "utf8"
  );

  console.log("Update complete.");
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
