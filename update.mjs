
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "WEEK", days: 7 },
  { key: "month", api: "MONTH", days: 30 }
];

const CANDIDATES = 1000;
const LEADERS = 100;
const PAGE = 50;
const POSITION_LIMIT = 1000;
const TRADE_LIMIT = 50;
const MIN_RECORD = 10;
const DELAY = 100;

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getJson(url, attempts = 4) {
  let last;
  for (let i = 0; i < attempts; i++) {
    try {
      const response = await fetch(url, {
        headers: { Accept: "application/json", "User-Agent": "PolymarketConsensusScanner/2.0" },
        signal: AbortSignal.timeout(25000)
      });

      if (response.status === 429 || response.status >= 500) {
        await sleep(800 * (i + 1));
        throw new Error(`HTTP ${response.status}`);
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      return await response.json();
    } catch (e) {
      last = e;
      if (i < attempts - 1) await sleep(500 * (i + 1));
    }
  }
  throw last;
}

function arr(x) {
  if (Array.isArray(x)) return x;
  if (Array.isArray(x?.data)) return x.data;
  if (Array.isArray(x?.results)) return x.results;
  return [];
}

function n(x) {
  if (x === null || x === undefined || x === "") return null;
  const v = Number(x);
  return Number.isFinite(v) ? v : null;
}

function ms(x) {
  if (x === null || x === undefined || x === "") return null;
  if (typeof x === "number" || /^\d+(\.\d+)?$/.test(String(x))) {
    const v = Number(x);
    return Number.isFinite(v) ? (v < 1e12 ? v * 1000 : v) : null;
  }
  const v = Date.parse(x);
  return Number.isFinite(v) ? v : null;
}

function wallet(row) {
  return String(
    row?.proxyWallet ?? row?.proxy_wallet ?? row?.address ??
    row?.wallet ?? row?.user_id ?? ""
  ).toLowerCase();
}

function normalize(row, fallbackRank) {
  const w = wallet(row);
  return {
    rank: n(row.rank) ?? fallbackRank,
    wallet: w,
    name: row.userName || row.user_name || row.username || row.name ||
      row.pseudonym || (w ? `${w.slice(0, 8)}…${w.slice(-4)}` : "Unknown"),
    pnl: n(row.pnl),
    volume: n(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getLeaderboard(period) {
  const out = [];
  const seen = new Set();

  for (let offset = 0; offset < CANDIDATES; offset += PAGE) {
    const p = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE),
      offset: String(offset)
    });

    const rows = arr(await getJson(`${API}/v1/leaderboard?${p}`));
    if (!rows.length) break;

    for (const row of rows) {
      const t = normalize(row, out.length + 1);
      if (!t.wallet || seen.has(t.wallet)) continue;
      seen.add(t.wallet);
      out.push(t);
    }

    if (rows.length < PAGE) break;
    await sleep(DELAY);
  }

  return out.slice(0, CANDIDATES);
}

async function mapLimit(items, limit, fn) {
  const result = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      try {
        result[i] = await fn(items[i], i);
      } catch (e) {
        console.error("Item failed:", e.message);
        result[i] = null;
      }
      await sleep(DELAY);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker())
  );
  return result;
}

/*
 * /closed-positions returns exited positions. Only rows with a binary
 * settlement price near 0 or 1 are eligible for win/loss counting.
 */
async function getClosedPositions(user) {
  const output = [];

  for (let offset = 0; offset < 1000; offset += 50) {
    const p = new URLSearchParams({
      user,
      limit: "50",
      offset: String(offset),
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    });

    const rows = arr(await getJson(`${API}/closed-positions?${p}`));
    if (!rows.length) break;

    output.push(...rows);

    if (rows.length < 50) break;
    if (output.length >= 1000) break;
    await sleep(DELAY);
  }

  return output;
}

function calculateRecord(positions, period, now) {
  const start = now - period.days * 86400000;
  const seen = new Set();
  let wins = 0;
  let losses = 0;

  for (const p of positions) {
    const time = ms(p.timestamp ?? p.closedAt ?? p.closed_at);
    if (time === null || time < start || time > now) continue;

    const price = n(p.curPrice ?? p.cur_price ?? p.settlementPrice);
    if (price === null) continue;

    // Do not count trades closed by selling at an intermediate price.
    if (price >= 0.99) {
      // resolved winning outcome
    } else if (price <= 0.01) {
      // resolved losing outcome
    } else {
      continue;
    }

    const condition = String(p.conditionId ?? p.condition_id ?? "");
    const outcome = String(p.outcomeIndex ?? p.outcome_index ?? p.outcome ?? "");
    const key = `${condition}|${outcome}`;
    if (!condition || seen.has(key)) continue;
    seen.add(key);

    if (price >= 0.99) wins++;
    else losses++;
  }

  const total = wins + losses;
  return {
    wins,
    losses,
    resolvedMarkets: total,
    winRate: total ? wins / total * 100 : null,
    recordVerified: total > 0
  };
}

async function attachRecords(candidates, period, now) {
  return mapLimit(candidates, 8, async trader => {
    const positions = await getClosedPositions(trader.wallet);
    return { ...trader, ...calculateRecord(positions, period, now) };
  }).then(rows => rows.filter(Boolean));
}

function rankByRecord(candidates) {
  return [...candidates].sort((a, b) => {
    const aQualified = a.recordVerified && a.resolvedMarkets >= MIN_RECORD;
    const bQualified = b.recordVerified && b.resolvedMarkets >= MIN_RECORD;
    if (aQualified !== bQualified) return aQualified ? -1 : 1;

    if (aQualified && bQualified) {
      const rateDiff = (b.winRate ?? -1) - (a.winRate ?? -1);
      if (rateDiff) return rateDiff;
      const winDiff = (b.wins ?? 0) - (a.wins ?? 0);
      if (winDiff) return winDiff;
    }

    return (b.pnl ?? -Infinity) - (a.pnl ?? -Infinity);
  }).map((t, i) => ({ ...t, rank: i + 1 }));
}

function activePosition(p, now) {
  const size = n(p.current_size ?? p.size ?? p.currentSize);
  if (size === null || size <= 0) return false;

  if (p.redeemable === true || p.mergeable === true ||
      p.status === "REDEEMABLE" || p.status === "REDEEMABLE_LOST") return false;

  const end = ms(p.end_date ?? p.endDate);
  if (end !== null && end < now) return false;
  return true;
}

async function getActivePositions(user, now) {
  const all = [];
  let cursor = null;

  for (let page = 0; page < 5; page++) {
    const p = new URLSearchParams({
      user,
      limit: String(POSITION_LIMIT),
      status: "OPEN",
      sort_by: "CURRENT_VALUE",
      sort_direction: "DESC",
      filter_type: "CASH",
      filter_amount: "1"
    });
    if (cursor) p.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${p}`);
    const rows = arr(response);
    all.push(...rows.filter(x => activePosition(x, now)));

    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor || !rows.length) break;
    await sleep(DELAY);
  }

  return all;
}

async function getTrades(user) {
  const p = new URLSearchParams({ user, limit: String(TRADE_LIMIT) });
  const rows = arr(await getJson(`${API}/trades?${p}`));

  return rows
    .filter(x => String(x.side || "").toUpperCase() === "BUY")
    .map(x => ({
      title: x.title || x.question || x.eventSlug || "Unknown market",
      side: "BUY",
      outcome: x.outcome || x.outcome_name || "Unknown",
      amount: n(x.usdcSize ?? x.amount ?? x.size),
      buyTime: ms(x.timestamp ?? x.createdAt ?? x.time),
      conditionId: x.conditionId || x.condition_id || null
    }))
    .sort((a, b) => (b.buyTime || 0) - (a.buyTime || 0));
}

function buildConsensus(candidates, activeByWallet) {
  const markets = new Map();

  for (const t of candidates) {
    for (const p of activeByWallet[t.wallet] || []) {
      const condition = String(p.condition_id ?? p.conditionId ?? "").toLowerCase();
      const side = String(p.outcome ?? p.outcome_name ?? "").trim();
      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;
      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: p.title || p.name || p.question || "Unknown market",
          side,
          sameSide: 0,
          totalEntry: 0,
          newestBuy: null,
          traders: [],
          seen: new Set()
        });
      }

      const m = markets.get(key);
      if (m.seen.has(t.wallet)) continue;
      m.seen.add(t.wallet);
      m.sameSide++;

      const amount = n(
        p.current_value ?? p.currentValue ??
        p.entry_cost_usdc ?? p.initialValue ?? p.initial_value
      );
      const buyTime = ms(p.last_event_at ?? p.lastEventAt ?? p.first_entry_at);

      if (amount !== null) m.totalEntry += amount;
      if (buyTime !== null) m.newestBuy = Math.max(m.newestBuy || 0, buyTime);

      m.traders.push({
        name: t.name,
        wallet: t.wallet,
        rank: t.rank,
        wins: t.wins,
        losses: t.losses,
        winRate: t.winRate,
        resolvedMarkets: t.resolvedMarkets,
        pnl: t.pnl,
        volume: t.volume,
        recordVerified: t.recordVerified,
        side,
        amount,
        buyTime
      });
    }
  }

  return [...markets.values()]
    .filter(m => m.sameSide >= 2)
    .map(m => {
      delete m.seen;
      // The UI needs only a compact member list, not all 1,000 full profiles.
      m.traders = m.traders.slice(0, 100);
      return m;
    })
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalEntry - a.totalEntry ||
      (b.newestBuy || 0) - (a.newestBuy || 0)
    );
}

async function buildPeriod(period, rawCandidates, activeByWallet, now) {
  console.log(`Building ${period.key} records...`);

  const withRecords = await attachRecords(rawCandidates, period, now);
  const ranked = rankByRecord(withRecords);
  const leaders = ranked.slice(0, LEADERS);

  const tradeLists = await mapLimit(leaders, 8, async t => [
    t.wallet, await getTrades(t.wallet)
  ]);
  const traderActiveTrades = Object.fromEntries(tradeLists.filter(Boolean));

  // Consensus must be calculated from the entire weekly candidate pool.
  const consensusPool = period.key === "week"
    ? ranked
    : ranked;

  const consensus = buildConsensus(consensusPool, activeByWallet);

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    rankingBasis: `Verified resolved win/loss records where available; P&L fallback for traders without ${MIN_RECORD}+ verified results`,
    recordBasis: "Closed-position settlement prices and timestamps from Polymarket Data API",
    candidateCount: rawCandidates.length,
    minResolvedMarketsForRecordRanking: MIN_RECORD,
    recordRankingEnabled: true,
    recordWarning: "Only binary settlement prices near 0 or 1 are counted as resolved outcomes.",
    leaders,
    traderActiveTrades,
    traderActivePositions: activeByWallet,
    consensus
  };

  const file = `${period.key}.json`;
  const temp = `${file}.tmp`;
  const serialized = JSON.stringify(output);

  if (Buffer.byteLength(serialized, "utf8") > 5 * 1024 * 1024) {
    throw new Error(`${file} exceeds the 5 MB safety limit.`);
  }

  await fs.writeFile(temp, serialized, "utf8");
  await fs.rename(temp, file);
  console.log(`Wrote ${file}: ${leaders.length} leaders, ${consensus.length} consensus markets`);
}

async function main() {
  const now = Date.now();

  console.log("Fetching weekly candidate leaderboard...");
  const weekCandidates = await getLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates: ${weekCandidates.length}`);

  console.log("Fetching monthly candidate leaderboard...");
  const monthCandidates = await getLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates: ${monthCandidates.length}`);

  const combined = new Map();
  for (const t of [...weekCandidates, ...monthCandidates]) {
    if (t.wallet && !combined.has(t.wallet)) combined.set(t.wallet, t);
  }

  const allCandidates = [...combined.values()];
  console.log(`Fetching active positions for ${allCandidates.length} traders...`);

  const pairs = await mapLimit(allCandidates, 8, async t => [
    t.wallet,
    await getActivePositions(t.wallet, now)
  ]);
  const activeByWallet = Object.fromEntries(pairs.filter(Boolean));

  await buildPeriod(PERIODS[0], weekCandidates, activeByWallet, now);
  await buildPeriod(PERIODS[1], monthCandidates, activeByWallet, now);

  console.log("Polymarket tracker update completed.");
}

main().catch(e => {
  console.error("Polymarket update failed:", e);
  process.exitCode = 1;
});
