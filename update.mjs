
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const PERIODS = [
  { key: "week", days: 7, api: "WEEK" },
  { key: "month", days: 30, api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_PAGE_SIZE = 500;
const COMBO_PAGE_SIZE = 200;
const MIN_RESOLVED_FOR_RANKING = 10;
const MIN_COMBO_LEGS = 2;
const MIN_COMBO_WIN_RATE = 60;
const MIN_COMBO_RESOLVED = 20;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJson(url, attempts = 5) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/2.0"
        },
        signal: AbortSignal.timeout(20000)
      });

      if (response.status === 429 || response.status === 503 ||
          response.status >= 500) {
        const retryAfter = Number(response.headers.get("retry-after"));
        const delay = Number.isFinite(retryAfter) && retryAfter > 0
          ? Math.min(retryAfter * 1000, 15000)
          : Math.min(500 * 2 ** attempt, 10000);

        throw Object.assign(new Error(`HTTP ${response.status}`), {
          retryable: true,
          delay
        });
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt === attempts - 1) break;
      await sleep(error.delay ?? Math.min(300 * 2 ** attempt, 5000));
    }
  }

  throw lastError ?? new Error("Request failed");
}

function arr(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.results)) return value.results;
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
    return n < 1e11 ? n * 1000 : n;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function walletOf(row) {
  return String(
    row?.proxyWallet ?? row?.proxy_wallet ?? row?.user ?? row?.address ?? ""
  ).toLowerCase();
}

function normalizeLeader(row, fallbackRank) {
  const wallet = walletOf(row);
  return {
    rank: num(row.rank) ?? fallbackRank,
    wallet,
    name: row.userName || row.user_name || row.username ||
      row.name || row.pseudonym ||
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: num(row.pnl),
    volume: num(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false,
    recordError: null
  };
}

async function getLeaderboard(period) {
  const candidates = [];
  const seen = new Set();

  // Public v1 leaderboard: retrieve the documented first 1,000
  // P&L-ranked candidates. This does NOT claim to cover 10,000 traders.
  for (let offset = 0; offset < CANDIDATE_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    const rows = arr(await getJson(`${API}/v1/leaderboard?${params}`));
    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeLeader(row, candidates.length + 1);
      if (!trader.wallet || seen.has(trader.wallet)) continue;
      seen.add(trader.wallet);
      candidates.push(trader);
    }

    if (rows.length < PAGE_SIZE) break;
    await sleep(100);
  }

  return candidates.slice(0, CANDIDATE_COUNT);
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

async function getClosedPositions(wallet) {
  const output = [];
  let cursor = null;
  const seen = new Set();

  for (let page = 0; page < 100; page++) {
    const params = new URLSearchParams({
      user: wallet,
      status: "CLOSED",
      sort_by: "TIMESTAMP",
      sort_direction: "DESC",
      limit: String(POSITION_PAGE_SIZE)
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${params}`);
    const rows = arr(response);

    for (const p of rows) {
      const condition = String(p.condition_id ?? "").toLowerCase();
      const outcome = String(p.outcome_index ?? p.outcome ?? "");
      const key = `${condition}|${outcome}`;

      if (!condition || seen.has(key)) continue;
      seen.add(key);

      output.push({
        condition,
        outcome,
        title: p.title ?? p.name ?? "Unknown market",
        outcomeLabel: p.outcome ?? "Unknown",
        timestamp: timeMs(p.last_event_at),
        realizedPnl: num(p.realized_pnl),
        entryCost: num(p.total_cost_usdc ?? p.entry_cost_usdc),
        status: String(p.status ?? "CLOSED").toUpperCase()
      });
    }

    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor || !rows.length) break;
    await sleep(75);
  }

  return output;
}

function recordForPeriod(positions, period, now) {
  const start = now - period.days * 86400000;
  let wins = 0;
  let losses = 0;
  let realizedPnl = 0;
  let volume = 0;

  for (const p of positions) {
    if (p.timestamp === null || p.timestamp < start || p.timestamp > now) {
      continue;
    }

    // Only classify records with known realized P&L.
    // Zero-P&L outcomes are excluded from wins/losses.
    if (p.realizedPnl === null) continue;
    if (p.realizedPnl > 0) wins++;
    if (p.realizedPnl < 0) losses++;

    realizedPnl += p.realizedPnl;
    volume += p.entryCost ?? 0;
  }

  const total = wins + losses;
  return {
    wins,
    losses,
    resolvedMarkets: total,
    winRate: total ? wins / total * 100 : null,
    recordVerified: true,
    periodRealizedPnl: realizedPnl,
    periodResolvedVolume: volume
  };
}

async function attachRecords(candidates, period, now) {
  let completed = 0;

  return mapLimit(candidates, 8, async trader => {
    try {
      const positions = await getClosedPositions(trader.wallet);
      const record = recordForPeriod(positions, period, now);
      completed++;

      if (completed % 25 === 0) {
        console.log(`${period.key}: checked records for ${completed}/${candidates.length}`);
      }

      return { ...trader, ...record, recordError: null };
    } catch (error) {
      completed++;
      console.error(`Record request failed for ${trader.name}: ${error.message}`);

      return {
        ...trader,
        wins: null,
        losses: null,
        resolvedMarkets: null,
        winRate: null,
        recordVerified: false,
        recordError: error.message
      };
    }
  });
}

function rankTraders(candidates) {
  const qualified = candidates.filter(t =>
    t.recordVerified &&
    t.resolvedMarkets >= MIN_RESOLVED_FOR_RANKING &&
    t.winRate !== null
  );

  qualified.sort((a, b) =>
    (b.winRate - a.winRate) ||
    (b.resolvedMarkets - a.resolvedMarkets) ||
    ((b.pnl ?? -Infinity) - (a.pnl ?? -Infinity))
  );

  const other = candidates
    .filter(t => !qualified.includes(t))
    .sort((a, b) => {
      if (a.recordVerified !== b.recordVerified) {
        return a.recordVerified ? -1 : 1;
      }
      return (b.resolvedMarkets ?? -1) - (a.resolvedMarkets ?? -1);
    });

  return [...qualified, ...other].map((t, i) => ({ ...t, rank: i + 1 }));
}

function isActive(p, now) {
  const size = num(p.current_size ?? p.size ?? p.currentSize);
  if (size === null || size <= 0) return false;
  if (p.redeemable === true || p.mergeable === true) return false;

  const end = timeMs(p.end_date ?? p.endDate);
  if (end !== null && end < now) return false;
  return true;
}

async function getActivePositions(wallet) {
  const output = [];
  let cursor = null;

  for (let page = 0; page < 20; page++) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(POSITION_PAGE_SIZE),
      status: "OPEN"
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${params}`);
    const rows = arr(response);
    output.push(...rows.filter(p => isActive(p, Date.now())));

    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor || !rows.length) break;
  }

  return output;
}

async function getComboPositions(wallet) {
  const output = [];
  let cursor = null;

  for (let page = 0; page < 10; page++) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(COMBO_PAGE_SIZE),
      sort_by: "FIRST_ENTRY",
      sort_direction: "DESC"
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions/combos?${params}`);
    const rows = arr(response);
    output.push(...rows);

    cursor = response?.pagination?.next_cursor ?? null;
    if (!cursor || !rows.length) break;
  }

  return output;
}

function normalizeCombo(combo, trader) {
  const status = String(combo.status ?? "").toUpperCase();
  const legs = Array.isArray(combo.legs) ? combo.legs : [];

  return {
    traderName: trader.name,
    traderRank: trader.rank,
    wallet: trader.wallet,
    wins: trader.wins,
    losses: trader.losses,
    winRate: trader.winRate,
    resolvedMarkets: trader.resolvedMarkets,
    comboId: combo.combo_condition_id ?? combo.combo_position_id ?? null,
    status,
    entryTime: timeMs(combo.first_entry_at),
    updatedAt: timeMs(combo.updated_at),
    entryCost: num(combo.entry_cost_usdc),
    legsTotal: num(combo.legs_total) ?? legs.length,
    legsResolved: num(combo.legs_resolved) ?? 0,
    legs: legs.map(leg => ({
      market: leg.market?.question ?? leg.market?.title ?? "Unknown market",
      event: leg.market?.event?.event_title ?? "",
      outcome: leg.market?.outcome ?? leg.leg_outcome_label ?? "Unknown",
      category: leg.market?.category ?? "",
      status: leg.leg_status ?? ""
    }))
  };
}

function isSportsCombo(combo) {
  const text = combo.legs.map(leg =>
    `${leg.category} ${leg.event} ${leg.market}`
  ).join(" ").toLowerCase();

  return /\b(nfl|nba|mlb|nhl|ncaa|football|basketball|baseball|hockey|soccer|sports|ufc|tennis)\b/.test(text);
}

async function buildComboPicks(ranked) {
  // This scans combo positions only for the candidate leaderboard pool.
  // A trader is eligible only if their record is actually available and
  // meets the thresholds below.
  const eligible = ranked.filter(t =>
    t.recordVerified &&
    t.resolvedMarkets >= MIN_COMBO_RESOLVED &&
    t.winRate >= MIN_COMBO_WIN_RATE
  );

  let completed = 0;
  const all = [];

  await mapLimit(eligible, 6, async trader => {
    try {
      const combos = await getComboPositions(trader.wallet);

      for (const combo of combos) {
        const normalized = normalizeCombo(combo, trader);
        if (normalized.legsTotal < MIN_COMBO_LEGS) continue;
        if (!isSportsCombo(normalized)) continue;
        if (!normalized.entryTime) continue;

        // Only recent combos; no invented parlays from adjacent trades.
        if (Date.now() - normalized.entryTime > 7 * 86400000) continue;

        all.push(normalized);
      }
    } catch (error) {
      console.error(`Combo scan failed for ${trader.name}: ${error.message}`);
    }

    completed++;
    if (completed % 25 === 0) {
      console.log(`Combo scan: ${completed}/${eligible.length} qualified traders`);
    }
  });

  const seen = new Set();
  return all
    .sort((a, b) => (b.entryTime ?? 0) - (a.entryTime ?? 0))
    .filter(combo => {
      const key = `${combo.wallet}|${combo.comboId}`;
      if (!combo.comboId || seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, 100);
}

async function attachActivePositions(traders) {
  const output = {};
  let completed = 0;
  let failures = 0;

  // Fetch active positions for the 100 displayed traders only.
  // This avoids the previous all-candidate scan that caused cancellations.
  await mapLimit(traders, 6, async trader => {
    try {
      output[trader.wallet] = await getActivePositions(trader.wallet);
    } catch (error) {
      failures++;
      output[trader.wallet] = null;
      console.error(`Active positions failed for ${trader.name}: ${error.message}`);
    }

    completed++;
    if (completed % 20 === 0) {
      console.log(`Active positions: ${completed}/${traders.length}`);
    }
  });

  return { positions: output, failures };
}

function buildConsensus(ranked, activePositions) {
  const markets = new Map();

  for (const trader of ranked) {
    if (!trader.recordVerified ||
        trader.resolvedMarkets < MIN_RESOLVED_FOR_RANKING) continue;

    const positions = activePositions[trader.wallet];
    if (!Array.isArray(positions)) continue;

    for (const p of positions) {
      const condition = String(p.condition_id ?? "").toLowerCase();
      const side = String(p.outcome ?? "").trim();
      if (!condition || !side) continue;

      const key = `${condition}|${side.toLowerCase()}`;
      if (!markets.has(key)) {
        markets.set(key, {
          conditionId: condition,
          title: p.title ?? p.name ?? "Unknown market",
          side,
          sameSide: 0,
          totalValue: 0,
          traders: [],
          seen: new Set(),
          newestActivity: null
        });
      }

      const market = markets.get(key);
      if (market.seen.has(trader.wallet)) continue;
      market.seen.add(trader.wallet);
      market.sameSide++;

      const value = num(p.current_value);
      const activity = timeMs(p.last_event_at);

      market.totalValue += value ?? 0;
      if (activity !== null) {
        market.newestActivity = Math.max(market.newestActivity ?? 0, activity);
      }

      market.traders.push({
        name: trader.name,
        rank: trader.rank,
        wins: trader.wins,
        losses: trader.losses,
        winRate: trader.winRate,
        resolvedMarkets: trader.resolvedMarkets,
        pnl: trader.pnl,
        volume: trader.volume,
        side,
        amount: value,
        lastActivity: activity
      });
    }
  }

  return [...markets.values()]
    .filter(m => m.sameSide >= 2)
    .map(({ seen, ...m }) => m)
    .sort((a, b) =>
      b.sameSide - a.sameSide ||
      b.totalValue - a.totalValue ||
      (b.newestActivity ?? 0) - (a.newestActivity ?? 0)
    );
}

async function writeJson(filename, data) {
  const temp = `${filename}.tmp`;
  await fs.writeFile(temp, JSON.stringify(data), "utf8");
  await fs.rename(temp, filename);
}

async function buildPeriod(period, candidates, now) {
  console.log(`Calculating ${period.key} resolved records...`);
  const withRecords = await attachRecords(candidates, period, now);
  const ranked = rankTraders(withRecords);
  const leaders = ranked.slice(0, LEADER_COUNT);

  console.log(`Loading active positions for ${period.key} top 100...`);
  const activeResult = await attachActivePositions(leaders);
  const consensus = buildConsensus(ranked, activeResult.positions);

  console.log(`Scanning actual combo positions for ${period.key}...`);
  const comboPicks = await buildComboPicks(ranked);

  const output = {
    generatedAt: new Date(now).toISOString(),
    period: period.key,
    rankingBasis: `Resolved position record over the last ${period.days} days`,
    recordBasis: "Realized P&L on positions closed in the selected period; zero-P&L outcomes excluded",
    candidateCount: candidates.length,
    candidateLimitNote: "This public leaderboard scan covers at most 1,000 P&L-ranked candidates, not the top 10,000.",
    minResolvedMarketsForRecordRanking: MIN_RESOLVED_FOR_RANKING,
    leaders,
    traderActivePositions: activeResult.positions,
    activePositionFailures: activeResult.failures,
    consensus,
    comboPicks,
    comboPickCriteria: {
      minResolvedMarkets: MIN_COMBO_RESOLVED,
      minWinRatePercent: MIN_COMBO_WIN_RATE,
      recentDays: 7,
      source: "Polymarket combo-position endpoint"
    }
  };

  await writeJson(`${period.key}.json`, output);
  console.log(`Wrote ${period.key}.json: ${leaders.length} leaders, ${consensus.length} consensus markets, ${comboPicks.length} combo picks`);
}

async function main() {
  const now = Date.now();

  for (const period of PERIODS) {
    console.log(`Loading ${period.key} leaderboard candidates...`);
    const candidates = await getLeaderboard(period);
    console.log(`${period.key}: ${candidates.length} candidates retrieved`);

    if (!candidates.length) {
      throw new Error(`No ${period.key} leaderboard candidates returned; refusing to overwrite data.`);
    }

    await buildPeriod(period, candidates, now);
  }

  console.log("Polymarket scanner update completed.");
}

main().catch(error => {
  console.error("Polymarket scanner update failed:", error);
  process.exitCode = 1;
});
