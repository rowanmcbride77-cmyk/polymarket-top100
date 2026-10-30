
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

// Discover and use a broad pool for overlap detection. Expensive per-wallet history
// verification remains separately bounded so the scheduled workflow can finish.
const CANDIDATE_COUNT = 5000;
const PROCESS_CANDIDATE_COUNT = 5000;
const RECORD_SCAN_CANDIDATE_COUNT = 1000;
const LEADERBOARD_PAGE_COUNT = 1000;
const MIN_CONSENSUS_WIN_RATE = 90;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const RANKING_RECORD_LIMIT = 1000;
const LEADER_RECORD_LIMIT = RANKING_RECORD_LIMIT;
const CONSENSUS_LIMIT = 100;
const CONSENSUS_MARKET_SCAN_LIMIT = 3000;
// Keep upcoming markets in the data so the UI can filter 24h / 3d / 7d accurately.
const CONSENSUS_WINDOW_HOURS = 720; // 30-day upcoming-market window to avoid dropping nearly all shared positions
const CONSENSUS_HOLDERS_PER_MARKET = 1500;
const MIN_CONSENSUS_POSITION_VALUE = 1;
const CONSENSUS_ENTRY_WINDOW_MINUTES = 1440;
const MIN_TRADER_RESOLVED_MARKETS = 5;
const TRADERS_PER_MARKET = 500;
const TRADE_LIMIT = 10;
const MAX_JSON_BYTES = 5 * 1024 * 1024;
const CONCURRENCY = 6;
const DELAY_MS = 150;
let lastConsensusDiagnostics = null;

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

async function getResolvedRecord(wallet, period, nowMs, maxPositions = RANKING_RECORD_LIMIT) {
  if (!wallet) return null;

  // WEEK and MONTH records must be scoped to the selected time window.
  // Previously this scanned all historical closed positions, making all-time
  // totals look like recent-period trade counts.
  const periodDays = String(period?.api || period || "").toUpperCase() === "MONTH" ? 30 : 7;
  const cutoffMs = nowMs - periodDays * 24 * 60 * 60 * 1000;
  const seen = new Set();
  let wins = 0;
  let losses = 0;
  let realizedPnlTotal = 0;
  let sportsWins = 0;
  let sportsLosses = 0;
  let scannedPositions = 0;
  let recordComplete = false;
  let cursor = null;
  const pageLimit = maxPositions === null ? 1000 : Math.min(1000, Math.max(1, maxPositions));

  while (true) {
    const params = new URLSearchParams({
      user: wallet,
      limit: String(pageLimit),
      status: "CLOSED",
      sort_by: "TIMESTAMP",
      sort_direction: "DESC",
      start: String(Math.floor(cutoffMs / 1000))
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/positions?${params}`);
    const rows = asArray(response);
    const pagination = response?.pagination ?? {};
    if (!rows.length) {
      recordComplete = true;
      break;
    }

    let reachedPeriodCutoff = false;
    for (const row of rows) {
      if (maxPositions !== null && scannedPositions >= maxPositions) break;
      const eventMs = timestampMs(row.last_event_at ?? row.lastEventAt ?? row.timestamp ?? row.updated_at ?? row.updatedAt);
      // API results are sorted newest-first, so once we pass the period boundary
      // there are no more in-window closed positions on subsequent pages.
      if (eventMs !== null && eventMs < cutoffMs) {
        reachedPeriodCutoff = true;
        break;
      }
      if (eventMs === null) continue;
      scannedPositions++;
      const pnl = num(row.realized_pnl ?? row.realizedPnl ?? row.cashPnl ?? row.cash_pnl);
      if (pnl === null || Math.abs(pnl) < 0.000001) continue;
      const asset = String(row.token_id ?? row.tokenId ?? row.asset ?? "");
      const condition = String(row.condition_id ?? row.conditionId ?? row.market ?? "");
      const outcome = String(row.outcome_index ?? row.outcomeIndex ?? row.outcome ?? "");
      const key = asset || `${condition}|${outcome}`;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      realizedPnlTotal += pnl;
      if (pnl > 0) wins++; else losses++;
      if (isSportsMarket(row)) {
        if (pnl > 0) sportsWins++; else sportsLosses++;
      }
    }

    if (reachedPeriodCutoff) {
      recordComplete = true;
      break;
    }
    if (maxPositions !== null && scannedPositions >= maxPositions) {
      recordComplete = false;
      break;
    }
    cursor = pagination.next_cursor || null;
    if (pagination.has_more !== true || !cursor) {
      recordComplete = true;
      break;
    }
  }

  const resolvedMarkets = wins + losses;
  // A capped sample is not a complete record. Never publish its 1,000–0
  // counts or 100% rate as if they were a trader's true record.
  const publishableRecord = recordComplete && resolvedMarkets > 0;
  return {
    wins: publishableRecord ? wins : null,
    losses: publishableRecord ? losses : null,
    realizedPnlTotal: publishableRecord ? realizedPnlTotal : null,
    sportsWins: recordComplete ? sportsWins : null,
    sportsLosses: recordComplete ? sportsLosses : null,
    sportsResolvedMarkets: recordComplete ? sportsWins + sportsLosses : null,
    sportsWinRate: recordComplete && (sportsWins + sportsLosses) ? (sportsWins / (sportsWins + sportsLosses)) * 100 : null,
    winRate: publishableRecord ? (wins / resolvedMarkets) * 100 : null,
    resolvedMarkets: publishableRecord ? resolvedMarkets : null,
    recordVerified: publishableRecord,
    scannedPositions,
    recordComplete,
    recordLimit: maxPositions,
    recordBasis: recordComplete
      ? `Closed-position sample from the last ${periodDays} days scanned; positive/negative realized P&L proxy, not verified market settlement`
      : `INCOMPLETE sample of up to ${maxPositions} closed positions from the last ${periodDays} days; positive/negative realized P&L proxy, not verified market settlement`
  };
}
async function attachRecords(traders, period, nowMs, maxPositions = RANKING_RECORD_LIMIT, cache = null) {
  const rows = await mapLimit(traders, CONCURRENCY, async trader => {
    try {
      let recordPromise;
      if (cache && cache.has(trader.wallet)) {
        recordPromise = cache.get(trader.wallet);
      } else {
        recordPromise = getResolvedRecord(trader.wallet, period, nowMs, maxPositions);
        if (cache) cache.set(trader.wallet, recordPromise);
      }
      const record = await recordPromise;
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
  // Build a broader discovery pool from public trader leaderboards across
  // categories and time windows. Each individual leaderboard is capped at
  // offset 1,000, so no single board can provide 5,000 wallets.
  const byWallet = new Map();
  const categories = [
    "OVERALL", "SPORTS", "POLITICS", "CRYPTO", "ESPORTS", "CULTURE",
    "MENTIONS", "WEATHER", "ECONOMICS", "TECH", "FINANCE"  ];
  const timePeriods = [...new Set([period.api, "DAY", "WEEK", "MONTH", "ALL"])];
  const target = CANDIDATE_COUNT;

  discovery:
  for (const timePeriod of timePeriods) {
    for (const category of categories) {
      for (const orderBy of ["PNL", "VOL"]) {
        for (let offset = 0; offset <= LEADERBOARD_PAGE_COUNT; offset += PAGE_SIZE) {
          const params = new URLSearchParams({
            category,
            timePeriod,
            orderBy,
            limit: String(PAGE_SIZE),
            offset: String(offset)
          });
          const rows = asArray(await getJson(`${API}/v1/leaderboard?${params}`));
          if (!rows.length) break;
          for (let i = 0; i < rows.length; i++) {
            const trader = normalizeLeader(rows[i], offset + i + 1);
            if (!trader.wallet) continue;
            const existing = byWallet.get(trader.wallet);
            if (existing) {
              existing.pnl = trader.pnl ?? existing.pnl;
              existing.volume = trader.volume ?? existing.volume;
              if ((!existing.name || existing.name === "Unknown") && trader.name) existing.name = trader.name;
            } else {
              byWallet.set(trader.wallet, trader);
            }
          }
          if (byWallet.size >= target) break discovery;
          if (rows.length < PAGE_SIZE || offset >= LEADERBOARD_PAGE_COUNT) break;
        }
      }
    }
  }

  const candidates = [...byWallet.values()];
  console.log(`${period.key}: discovered ${candidates.length} unique traders across public leaderboard categories and periods (target ${target})`);
  if (candidates.length < target) {
    throw new Error(
      `${period.key}: only discovered ${candidates.length} unique trader wallets; required ${target}. Refusing to publish a successful update with an undersized candidate pool.`
    );
  }
  return candidates.slice(0, target);
}

function rankSportsCandidates(candidates, records = {}) {
  const rows = candidates.map(trader => ({ ...trader, ...(records[trader.wallet] || {}) }));
  return rows.filter(trader => (trader.sportsResolvedMarkets || 0) >= 5 && trader.sportsWinRate !== null)
    .sort((a, b) => (b.sportsWinRate || 0) - (a.sportsWinRate || 0) || (b.sportsResolvedMarkets || 0) - (a.sportsResolvedMarkets || 0))
    .map((trader, index) => ({ ...trader, sportsRank: index + 1 }));
}

function rankCandidates(candidates, records = {}) {
  return [...candidates]
    .map(trader => {
      // Wallets are addresses, so join records case-insensitively to avoid
      // accidentally treating a scanned trader as unscanned due to casing.
      const walletKey = String(trader.wallet || "").toLowerCase();
      const recordKey = Object.keys(records).find(key => String(key).toLowerCase() === walletKey);
      const record = (recordKey ? records[recordKey] : null) || {};
      const merged = { ...trader, ...record };
      const wins = Number(record.wins);
      const losses = Number(record.losses);
      const resolved = Number(record.resolvedMarkets);
      const validCounts = Number.isFinite(wins) && wins >= 0 &&
        Number.isFinite(losses) && losses >= 0 && wins + losses > 0 &&
        Number.isFinite(resolved) && resolved === wins + losses;
      // Recompute the displayed/ranked rate from the exact W-L counts instead
      // of trusting a potentially stale or inconsistent cached winRate field.
      if (record.recordComplete === true && validCounts) {
        merged.winRate = (wins / (wins + losses)) * 100;
        merged.resolvedMarkets = wins + losses;
      }
      // When the closed-position scan is complete, use the sum of those same
      // period-scoped realized P&L rows so the displayed dollars agree with W-L.
      if (record.recordComplete === true && Number.isFinite(record.realizedPnlTotal)) {
        merged.pnl = record.realizedPnlTotal;
        merged.pnlBasis = "Sum of unique closed-position realized P&L in selected period";
      } else {
        merged.pnlBasis = "Polymarket leaderboard period P&L; closed-position history incomplete";
      }
      return merged;
    })
    .sort((a, b) => {
      // A trader is eligible for record ranking only with a complete sample,
      // consistent W-L counts, and the minimum number of classifiable closes.
      const aCountsValid = Number.isFinite(Number(a.wins)) && Number(a.wins) >= 0 &&
        Number.isFinite(Number(a.losses)) && Number(a.losses) >= 0 &&
        Number(a.resolvedMarkets) === Number(a.wins) + Number(a.losses) &&
        Number(a.resolvedMarkets) > 0;
      const bCountsValid = Number.isFinite(Number(b.wins)) && Number(b.wins) >= 0 &&
        Number.isFinite(Number(b.losses)) && Number(b.losses) >= 0 &&
        Number(b.resolvedMarkets) === Number(b.wins) + Number(b.losses) &&
        Number(b.resolvedMarkets) > 0;
      const aComplete = a.recordComplete === true && aCountsValid &&
        Number(a.resolvedMarkets) >= MIN_TRADER_RESOLVED_MARKETS;
      const bComplete = b.recordComplete === true && bCountsValid &&
        Number(b.resolvedMarkets) >= MIN_TRADER_RESOLVED_MARKETS;
      if (aComplete !== bComplete) return aComplete ? -1 : 1;
      if (aComplete && bComplete) {
        const aRate = Number(a.wins) / (Number(a.wins) + Number(a.losses));
        const bRate = Number(b.wins) / (Number(b.wins) + Number(b.losses));
        return (bRate - aRate) ||
          (Number(b.resolvedMarkets) - Number(a.resolvedMarkets)) ||
          ((Number(b.pnl) || 0) - (Number(a.pnl) || 0)) ||
          ((Number(b.volume) || 0) - (Number(a.volume) || 0));
      }
      return ((Number(b.pnl) || 0) - (Number(a.pnl) || 0)) ||
        ((Number(b.volume) || 0) - (Number(a.volume) || 0));
    })
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
  // Trust explicit API lifecycle status over inferred end dates.
  const status = String(position.status ?? "").toUpperCase();
  if (status && status !== "OPEN" && status !== "ACTIVE") return false;
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
    initialValue: num(position.entry_cost_usdc ?? position.total_cost_usdc ?? position.initialValue ?? position.initial_value ?? position.initial_value_usd) ?? (() => { const avg = num(position.avgPrice ?? position.avg_price ?? position.averagePrice ?? position.average_price); const size = num(position.size ?? position.current_size ?? position.currentSize); return avg !== null && size !== null && avg >= 0 && size > 0 ? avg * size : null; })(),
    realizedPnl: num(position.realizedPnl ?? position.realized_pnl ?? position.cashPnl ?? position.cash_pnl ?? position.total_pnl ?? position.totalPnl),
    lastEventAt: timestampMs(
      position.lastEventAt ?? position.last_event_at
    ),
    firstEntryAt: timestampMs(position.firstEntryAt ?? position.first_entry_at),
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

async function getPublicProfileName(wallet) {
  try {
    const params = new URLSearchParams({ address: wallet });
    const response = await getJson(GAMMA_API + "/public-profile?" + params);
    const profile = response?.data ?? response;
    const name = profile?.name || profile?.pseudonym || profile?.username || profile?.userName;
    return name ? String(name).trim() : null;
  } catch {
    return null;
  }
}

async function getMarketConsensus(market, knownNames) {
  const conditionId = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
  if (!conditionId) return [];

  const params = new URLSearchParams({
    condition: conditionId,
    limit: String(CONSENSUS_HOLDERS_PER_MARKET),
    status: "OPEN",
    sort_by: "CURRENT_VALUE",
    sort_direction: "DESC",
    filter_type: "CASH",
    filter_amount: String(MIN_CONSENSUS_POSITION_VALUE)
  });

  let response;
  try {
    response = await getJson(API + "/v2/positions?" + params);
  } catch (error) {
    console.error("Consensus holders failed for " + conditionId + ": " + error.message);
    return [];
  }

  const rows = asArray(response);
  const grouped = new Map();

  for (const position of rows) {
    if (position.redeemable === true || position.mergeable === true) continue;

    const wallet = String(position.proxy_wallet ?? position.proxyWallet ?? position.user ?? "").toLowerCase();
    const side = String(position.outcome ?? position.outcome_name ?? "").trim();
    const value = num(position.current_value ?? position.currentValue);
    if (!wallet || !side || value === null || value < MIN_CONSENSUS_POSITION_VALUE) continue;

    // last_event_at reflects the most recent activity; first_entry_at can be
    // months old even when a trader recently added to an open position.
    const lastEventAt = timestampMs(position.last_event_at ?? position.lastEventAt ?? position.updated_at ?? position.updatedAt ?? position.first_entry_at ?? position.firstEntryAt);
    if (lastEventAt === null || lastEventAt < Date.now() - CONSENSUS_ENTRY_WINDOW_MINUTES * 60 * 1000) continue;

    const key = side.toLowerCase();
    if (!grouped.has(key)) {
      grouped.set(key, {
        conditionId,
        title: position.title || position.name || market.question || market.title || "Unknown market",
        endDate: marketEndDate(market),
        side,
        sameSide: 0,
        totalValue: 0,
        totalEntry: 0,
        entryValueKnownCount: 0,
        newestBuy: null,
        oldestBuy: null,
        traders: [],        walletsSeen: new Set()
      });
    }

    const group = grouped.get(key);
    if (group.walletsSeen.has(wallet)) continue;
    group.walletsSeen.add(wallet);
    group.sameSide++;
    const currentValue = num(position.currentValue ?? position.current_value ?? value) ?? value;
    const avgPrice = num(position.avgPrice ?? position.avg_price ?? position.averagePrice ?? position.average_price);
    const shares = num(position.size ?? position.shares ?? position.balance);
    // Derive cost basis from average entry price × shares first. Some API
    // initialValue fields have incompatible semantics and can inflate totals.
    const entryValue = (avgPrice !== null && shares !== null && avgPrice >= 0 && shares >= 0)
      ? avgPrice * shares
      : null;
    group.totalValue += currentValue;
    if (entryValue !== null) { group.totalEntry += entryValue; group.entryValueKnownCount++; }
    if (lastEventAt !== null) {
      group.newestBuy = group.newestBuy === null ? lastEventAt : Math.max(group.newestBuy, lastEventAt);
      group.oldestBuy = group.oldestBuy === null ? lastEventAt : Math.min(group.oldestBuy, lastEventAt);
    }

    if (group.traders.length < TRADERS_PER_MARKET) {
      group.traders.push({
        wallet,
        name: knownNames.get(wallet)?.name || position.name || position.userName || null,
        rank: knownNames.get(wallet)?.rank ?? null,
        pnl: knownNames.get(wallet)?.pnl ?? null,
        volume: knownNames.get(wallet)?.volume ?? null,
        side,
        amount: currentValue,
        currentValue,
        entryValue,
        avgPrice,
        currentPrice: num(position.curPrice ?? position.currentPrice ?? position.outcomePrice ?? position.outcome_price),
        shares,
        isStillHolding: true,
        positionStatus: "OPEN",
        buyTime: lastEventAt
      });
    }
  }

  const qualifyingGroups = [...grouped.values()]
    .filter(group => group.sameSide >= 2);
  if (qualifyingGroups.length === 0) console.log("No same-side group had 2+ holders with position activity in the last " + CONSENSUS_ENTRY_WINDOW_MINUTES + " minutes for market " + conditionId);
  return qualifyingGroups
    .map(({ walletsSeen, ...group }) => ({ ...group, totalEntry: group.entryValueKnownCount === group.sameSide ? group.totalEntry : null, traders: group.traders.map(trader => ({ ...trader, isStillHolding: true, positionStatus: "OPEN" })) }));
}

function marketEndDate(market) {
  return market.endDate ?? market.end_date ?? market.endDateIso ?? market.endDateISO ??
    market.end_date_iso ?? market.resolveTime ?? market.resolve_time ??
    market.gameStartTime ?? market.game_start_time ?? market.eventStartTime ?? market.event_start_time ?? null;
}

function isSportsMarket(market) {
  const tags = Array.isArray(market.tags)
    ? market.tags.map(tag => typeof tag === "string" ? tag : (tag.label || tag.slug || tag.name || ""))
    : [];
  const events = Array.isArray(market.events) ? market.events : [];
  const eventText = events.flatMap(event => [
    event.title, event.slug, event.category, event.sport, event.seriesSlug,
    ...(Array.isArray(event.tags) ? event.tags.map(tag => typeof tag === "string" ? tag : (tag.label || tag.slug || tag.name || "")) : [])
  ]);
  // Gamma sports markets commonly expose gameId/gameStartTime/sportsMarketType.
  if (market.gameId || market.game_id || market.gameStartTime || market.game_start_time ||
      market.sportsMarketType || market.sports_market_type) return true;

  const metadata = [
    market.category, market.sport, market.sports, market.sportsMarketType,
    market.sports_market_type, market.eventTitle, market.event_title,
    market.slug, market.question, market.title, market.description,
    market.groupItemTitle, ...tags, ...eventText
  ].filter(Boolean).join(" ").toLowerCase();
  const keywords = [
    "sport", "spread:", "moneyline", "over/under", "o/u", "nfl", "nba", "wnba", "mlb", "nhl", "ncaa", "ncaaf", "ncaab",
    "college football", "college basketball", "football", "basketball", "baseball",
    "hockey", "soccer", "mls", "premier league", "champions league", "world cup",
    "tennis", "atp", "wta", "golf", "pga", "lpga", "formula 1", "f1", "nascar",
    "indycar", "motogp", "ufc", "mma", "boxing", "cricket", "rugby", "volleyball",
    "lacrosse", "esports", "e-sports", "super bowl", "stanley cup", "world series",
    "march madness", "liga", "bundesliga", "serie a", "la liga", "ligue 1"
  ];
  return keywords.some(keyword => metadata.includes(keyword));
}

function isResolvingSoon(market, nowMs) {
  const rawEndDate = marketEndDate(market);
  let endMs = timestampMs(rawEndDate);
  if (typeof rawEndDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(rawEndDate)) {
    endMs = Date.parse(rawEndDate + "T23:59:59.999Z");
  }
  // This is a practical upcoming-game window, not a claim that Gamma endDate
  // is the exact resolution deadline; on sports markets it often represents kickoff.
  return endMs !== null && endMs > nowMs &&
    endMs <= nowMs + CONSENSUS_WINDOW_HOURS * 60 * 60 * 1000;
}

function hasExpiredEndDate(market, nowMs) {
  const rawEndDate = marketEndDate(market);
  if (rawEndDate === null || rawEndDate === undefined || rawEndDate === "") return false;
  let endMs = timestampMs(rawEndDate);
  // Treat date-only values as valid through the end of that UTC calendar day.
  if (typeof rawEndDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(rawEndDate)) {
    endMs = Date.parse(rawEndDate + "T23:59:59.999Z");
  }
  return endMs !== null && endMs < nowMs;
}

async function getBroadConsensusMarkets(knownTraders, nowMs, activePositions) {
  // Group positions directly from the Data API. Page beyond the first 100
  // holdings so lower-value active markets are not silently omitted.
  const MAX_POSITION_PAGES_PER_TRADER = 2; // 200 open positions per wallet; avoid spending most of the run on deep low-value pagination.
  const consensusPositions = await mapLimit(knownTraders, CONCURRENCY, async trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    if (!wallet) return [wallet, []];
    try {
      const all = [];
      const seen = new Set();
      let cursor = null;
      let pages = 0;
      let hasMore = false;
      while (pages < MAX_POSITION_PAGES_PER_TRADER) {
        const params = new URLSearchParams({
          user: wallet,
          limit: "100",
          status: "OPEN",
          sort_by: "CURRENT_VALUE",
          sort_direction: "DESC"
        });
        if (cursor) params.set("cursor", cursor);
        const response = await getJson(API + "/v2/positions?" + params);
        const rows = asArray(response);
        const pagination = response?.pagination ?? {};
        pages++;
        for (const row of rows) {
          const compact = compactPosition(row);
          const key = String(compact.conditionId || "") + "|" + String(compact.outcome || "").toLowerCase();
          if (!compact.conditionId || !compact.outcome || seen.has(key)) continue;
          seen.add(key);
          all.push(compact);
        }
        cursor = pagination.next_cursor || null;
        hasMore = pagination.has_more === true && Boolean(cursor);
        if (!hasMore || !rows.length) break;
      }
      return [wallet, all, hasMore, pages];
    } catch (error) {
      console.warn("Consensus positions fetch failed for " + wallet + ": " + error.message);
      return [wallet, Array.isArray(activePositions?.[wallet]) ? activePositions[wallet] : [], false, 0];
    }
  });

  const allPositionsByWallet = new Map(consensusPositions.map(row => [row[0], row[1]]));
  const traderByWallet = new Map(knownTraders.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  const groups = new Map();
  let openPositionsSeen = 0;
  let positionsWithConditionAndSide = 0;
  let walletsWithMorePositions = 0;
  let positionPagesFetched = 0;

  for (const [wallet, positions, hasMore, pages] of consensusPositions) {
    positionPagesFetched += pages || 0;
    if (hasMore) walletsWithMorePositions++;
    const trader = traderByWallet.get(wallet);
    if (!trader) continue;
    for (const position of positions) {
      openPositionsSeen++;
      const conditionId = String(position.conditionId ?? position.condition_id ?? "").toLowerCase();
      const side = String(position.outcome ?? position.outcome_name ?? "").trim();
      if (!conditionId || !side) continue;
      const currentValue = num(position.currentValue ?? position.current_value) ?? 0;
      const size = num(position.size ?? position.current_size ?? position.currentSize) ?? 0;
      if (currentValue <= 0 && size <= 0) continue;

      positionsWithConditionAndSide++;
      const key = conditionId + "|" + side.toLowerCase();
      if (!groups.has(key)) {
        groups.set(key, {
          conditionId,
          title: position.title || position.question || "Unknown market",
          endDate: position.endDate ?? position.end_date ?? null,
          side,
          sameSide: 0,
          totalValue: 0,
          totalEntry: 0,
          newestBuy: null,
          oldestBuy: null,
          traders: [],
          walletsSeen: new Set()
        });
      }
      const group = groups.get(key);
      if (group.walletsSeen.has(wallet)) continue;
      group.walletsSeen.add(wallet);
      const invested = num(position.initialValue ?? position.initial_value) ?? currentValue;
      const lastEventAt = timestampMs(position.lastEventAt ?? position.last_event_at ?? position.firstEntryAt ?? position.first_entry_at);
      group.sameSide++;
      group.totalValue += currentValue;
      group.totalEntry += invested;
      if (lastEventAt !== null) {
        group.newestBuy = group.newestBuy === null ? lastEventAt : Math.max(group.newestBuy, lastEventAt);
        group.oldestBuy = group.oldestBuy === null ? lastEventAt : Math.min(group.oldestBuy, lastEventAt);
      }
      group.traders.push({
        wallet,
        name: trader.name || null,
        rank: trader.rank ?? null,
        pnl: trader.pnl ?? null,
        volume: trader.volume ?? null,
        side,
        amount: invested,
        currentValue,
        buyTime: lastEventAt
      });
    }
  }

  const holderGroups = [...groups.values()]
    .filter(group => group.sameSide >= 2)
    .map(({ walletsSeen, ...group }) => group)
    // Do not truncate the raw groups before checking the weekly 90% filter.
    // Otherwise a few popular but ineligible markets can crowd out valid ones.
    .sort((a, b) => b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0));

  lastConsensusDiagnostics = {
    candidateWallets: knownTraders.length,
    positionPagesFetched,
    openPositionsSeen,
    positionsWithConditionAndSide,
    walletsWithMorePositions,
    sameSideGroups: holderGroups.length,
    pageCapPerWallet: MAX_POSITION_PAGES_PER_TRADER,
    note: "walletsWithMorePositions indicates candidate wallets whose open positions exceed the scanned pages"
  };
  console.log("Consensus scan diagnostics: " + JSON.stringify(lastConsensusDiagnostics));
  const activePositionsByWallet = Object.fromEntries(consensusPositions.map(([wallet, positions]) => [wallet, positions]));
  return {holderGroups, activePositionsByWallet};
}

function buildConsensus(holderGroups, candidates, records = {}) {
  const candidateMap = new Map(candidates.map(trader => [
    String(trader.wallet || "").toLowerCase(), trader
  ]));

  // First qualify each side independently. Never combine different outcomes
  // for the same condition/market into one consensus count.
  const qualifiedGroups = holderGroups
    .filter(group => {
      const nowMs = Date.now();
      // Include upcoming markets; the old negated isResolvingSoon() check
      // accidentally removed markets resolving within 24 hours.
      const end = marketEndDate(group);
      const hasEnd = end !== null && end !== undefined && end !== "";
      return !hasExpiredEndDate(group, nowMs) &&
        (!hasEnd || isResolvingSoon(group, nowMs)) &&
        (num(group.totalValue) ?? 0) > 0;
    })
    .map(group => {
      const traders = group.traders.flatMap(holder => {
        const wallet = String(holder.wallet || "").toLowerCase();
        const candidate = candidateMap.get(wallet);
        if (!candidate) return [];
        // The weekly consensus pool must use the SAME period-scoped win/loss
        // record shown in the weekly leaderboard. Do not substitute lifetime
        // settlement counts here; that mismatch allowed 48% lifetime/other
        // period records to appear in the weekly consensus list.
        const record = records[wallet] || records[candidate.wallet];
        // No raw leaderboard/lifetime fallback: without the selected period's
        // closed-position record, the trader cannot qualify for consensus.
        if (!record || record.recordComplete !== true) return [];
        const wins = num(record.wins);
        const losses = num(record.losses);
        const periodClosedMarkets = wins !== null && losses !== null ? wins + losses : null;
        const resolvedMarkets = num(record.resolvedMarkets) ?? periodClosedMarkets;
        const winRate = wins !== null && losses !== null && wins + losses > 0
          ? wins / (wins + losses) * 100
          : num(record.winRate);
        const earlyExits = num(record.earlyExitPositions);
        const settlementWins = num(record.settlementWins) ?? 0;
        const settlementLosses = num(record.settlementLosses) ?? 0;
        const settlementResolved = settlementWins + settlementLosses;
        const observedClassified = settlementResolved + (earlyExits ?? 0);
        const earlyExitRate = observedClassified > 0 && earlyExits !== null
          ? earlyExits / observedClassified * 100
          : null;
        // The requested 90% threshold is the profitable-close rate.
        // Early exits are tracked and displayed separately, never treated as wins.
        if (periodClosedMarkets === null ||
            periodClosedMarkets < MIN_TRADER_RESOLVED_MARKETS ||
            winRate === null ||
            winRate < MIN_CONSENSUS_WIN_RATE) return [];
        return [{
          ...holder,
          wallet,
          name: holder.name || candidate.name || "Trader",
          rank: candidate.sportsRank ?? candidate.rank ?? null,
          overallRank: candidate.rank ?? null,
          pnl: candidate.pnl ?? holder.pnl ?? null,
          volume: candidate.volume ?? holder.volume ?? null,
          wins,
          losses,
          winRate,
          resolvedMarkets,
          settlementWins,
          settlementLosses,
          earlyExitPositions: earlyExits,
          earlyExitRate,
          settlementResolvedMarkets: settlementResolved,
          settlementRecordComplete: record.settlementRecordComplete === true,
          recordComplete: record.recordComplete !== false,
          recordBasis: "At least 10 period-scoped closed positions and at least 90% profitable-close rate. Early exits are counted and displayed separately; early exits are not automatically treated as wins."
        }];
      });
      const totalValue = traders.reduce((sum, trader) => sum + (num(trader.amount) ?? 0), 0);      return { ...group, rawSameSide: group.sameSide, traders, sameSide: traders.length, totalValue };
    })
    .filter(group => group.sameSide >= 2);

  // Strict one-sided consensus: if qualified traders hold more than one
  // outcome in the same condition/market, suppress the entire market. This
  // prevents a 9-vs-1 split (or a 3-vs-3 sports split) from appearing as a
  // one-sided signal. A market is shown only when exactly one outcome has
  // at least two qualified traders in the scanned candidate pool.
  const outcomesByCondition = new Map();
  for (const group of qualifiedGroups) {
    const conditionId = String(group.conditionId || "").toLowerCase();
    if (!conditionId) continue;
    if (!outcomesByCondition.has(conditionId)) outcomesByCondition.set(conditionId, new Set());
    outcomesByCondition.get(conditionId).add(String(group.side || "").trim().toLowerCase());
  }

  return qualifiedGroups
    .filter(group => {
      const conditionId = String(group.conditionId || "").toLowerCase();
      const outcomes = outcomesByCondition.get(conditionId);
      if (!outcomes || outcomes.size <= 1) return true;
      const sameConditionGroups = qualifiedGroups.filter(item => String(item.conditionId || "").toLowerCase() === conditionId);
      // A lone opposing holder should not erase a valid 2+ trader consensus.
      return sameConditionGroups.filter(item => item.sameSide >= 2).length <= 1;
    })
    .sort((a, b) => b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0))
    .slice(0, CONSENSUS_LIMIT);
}

function parseArrayField(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string" || !value.trim()) return [];
  try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; }
  catch { return []; }
}
function timeMs(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  const ms = Number.isFinite(n) ? (n < 1e12 ? n * 1000 : n) : Date.parse(String(value));
  return Number.isFinite(ms) && ms > 0 ? ms : null;
}
async function getSettlementStats(wallet, period, nowMs, limit = 250) {
  // A CLOSED position means the wallet exited that position; it does not
  // mean the market settled. Include held positions too, because the API's
  // OPEN cohort contains settled winners that have not been redeemed and
  // REDEEMABLE_LOST exposes settled losing positions still held by the wallet.
  async function fetchPositionCohort(status, maxPages) {
    const rows = [];
    let cursor = null;
    let complete = true;
    let pagesFetched = 0;
    do {
      const params = new URLSearchParams({
        user: wallet,
        limit: String(limit),
        status,
        sort_by: "TIMESTAMP",
        sort_direction: "DESC"
      });
      if (cursor) params.set("cursor", cursor);
      const response = await getJson(`${API}/v2/positions?${params}`);
      const page = asArray(response);
      rows.push(...page.map(row => ({...row, _scanStatus: status})));
      const pagination = response?.pagination ?? {};
      pagesFetched++;
      const nextCursor = pagination.next_cursor ?? pagination.nextCursor ?? null;
      if (pagination.has_more !== true) {
        cursor = null;
        break;
      }
      if (!nextCursor || pagesFetched >= maxPages) {
        complete = false;
        cursor = null;
        break;
      }
      cursor = String(nextCursor);
    } while (cursor);
    return {rows, complete};
  }

  // CLOSED can be much larger than a wallet's currently held book, so it gets
  // the existing five-page cap. Held winner/loss cohorts usually fit in one
  // page; if they do not, the record is explicitly marked incomplete.
  const cohorts = await Promise.all([
    fetchPositionCohort("CLOSED", 5),
    fetchPositionCohort("OPEN", 2),
    fetchPositionCohort("REDEEMABLE_LOST", 2)
  ]);
  const allPagesComplete = cohorts.every(cohort => cohort.complete);
  const uniqueRows = new Map();
  const priority = status => status === "CLOSED" ? 0 : 1;
  for (const cohort of cohorts) {
    for (const row of cohort.rows) {
      const condition = String(row.condition_id ?? row.conditionId ?? "").toLowerCase();
      const rawIndex = row.outcome_index ?? row.outcomeIndex;
      const outcome = String(rawIndex ?? row.outcome ?? "").trim().toLowerCase();
      const key = condition ? condition + "|" + outcome : "";
      if (!key) continue;
      const prior = uniqueRows.get(key);
      if (!prior || priority(row._scanStatus) > priority(prior._scanStatus)) uniqueRows.set(key, row);
    }
  }

  const positions = [...uniqueRows.values()];
  const conditions = [...new Set(positions
    .map(row => String(row.condition_id ?? row.conditionId ?? "").toLowerCase())
    .filter(Boolean))];
  const conditionChunks = [];
  for (let i = 0; i < conditions.length; i += 40) conditionChunks.push(conditions.slice(i, i + 40));
  const marketPages = await mapLimit(conditionChunks, CONCURRENCY, async ids => {
    const q = new URLSearchParams({condition_ids: ids.join(",")});
    try {
      return {rows:asArray(await getJson(`${GAMMA_API}/markets?${q.toString()}`)), ok:true};
    } catch (error) {
      console.warn("Settlement market metadata lookup failed:", error.message);
      return {rows:[], ok:false};
    }
  });
  const markets = new Map();
  for (const page of marketPages) for (const market of page.rows) {
    const condition = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
    if (condition) markets.set(condition, market);
  }

  let settlementWins = 0, settlementLosses = 0, earlyExitMarkets = 0;
  let classified = 0, earlyExitPositions = 0, unclassified = 0;
  const earlyKeys = new Set();
  for (const position of positions) {
    const condition = String(position.condition_id ?? position.conditionId ?? "").toLowerCase();
    const market = markets.get(condition);
    const scanStatus = String(position._scanStatus || position.status || "").toUpperCase();
    const lastEventAt = timeMs(position.last_event_at ?? position.lastEventAt ?? position.timestamp ?? position.updated_at ?? position.updatedAt);
    if (!market) { unclassified++; continue; }

    // An active-market holding is neither a win nor a loss. A closed position
    // on a market that is still open is an early exit and must not count as a win.
    if (market.closed !== true) {
      if (scanStatus === "CLOSED") {
        if (lastEventAt === null) { unclassified++; continue; }
        classified++;
        earlyExitPositions++;
        const rawIndex = position.outcome_index ?? position.outcomeIndex ?? position.outcome;
        earlyKeys.add(condition + "|" + String(rawIndex ?? "unknown"));
      }
      continue;
    }

    const outcomes = parseArrayField(market.outcomes);
    const prices = parseArrayField(market.outcomePrices).map(num);
    const winnerIndex = prices.findIndex(price => price !== null && price >= 0.999);
    const resolvedAt = timeMs(market.closedTime ?? market.closed_time ?? market.resolvedAt ?? market.resolved_at);
    if (winnerIndex < 0 || resolvedAt === null) { unclassified++; continue; }

    const rawIndex = position.outcome_index ?? position.outcomeIndex;
    let positionIndex = rawIndex === null || rawIndex === undefined ? -1 : Number(rawIndex);
    if (!Number.isInteger(positionIndex) || positionIndex < 0) {
      const outcome = String(position.outcome ?? "").trim().toLowerCase();
      positionIndex = outcomes.findIndex(label => String(label).trim().toLowerCase() === outcome);
    }
    if (positionIndex < 0 || positionIndex >= outcomes.length) { unclassified++; continue; }

    // If the trader exited before resolution, the P&L of that exit is not a
    // settlement result. Current held positions and exits at/after resolution
    // are classified by the actual resolved outcome.
    if (scanStatus === "CLOSED" && lastEventAt !== null && lastEventAt < resolvedAt) {
      classified++;
      earlyExitPositions++;
      earlyKeys.add(condition + "|" + positionIndex);
      continue;
    }
    if (scanStatus === "CLOSED" && lastEventAt === null) { unclassified++; continue; }

    classified++;
    if (positionIndex === winnerIndex) settlementWins++;
    else settlementLosses++;
  }

  const metadataComplete = marketPages.every(page => page.ok);
  const settlementRecordComplete = allPagesComplete && metadataComplete && unclassified === 0;
  return {
    settlementWins,
    settlementLosses,
    earlyExitMarkets: earlyKeys.size,
    earlyExitPositions,
    settlementRecordComplete,
    settlementPositionsChecked: positions.length,
    settlementPositionsClassified: classified,
    settlementPositionsUnclassified: unclassified,
    settlementRecordBasis: "Verified market outcome at resolution; early exits excluded from settled wins/losses",
    settlementRecordCoverage: settlementRecordComplete ? "complete" : "partial"
  };
}
const settlementStatsCache = new Map();
async function attachSettlementStats(traders, period, nowMs) {
  return await mapLimit(traders, 4, async trader => {
    try {
      const wallet = String(trader.wallet || "").toLowerCase();
      let pending = settlementStatsCache.get(wallet);
      if (!pending) {
        pending = getSettlementStats(wallet, period, nowMs, 250);
        settlementStatsCache.set(wallet, pending);
      }
      const stats = await pending;
      return {...trader, ...stats};
    } catch (error) {
      console.warn("Settlement record unavailable for " + trader.wallet + ":", error.message);
      return {...trader, settlementWins: null, settlementLosses: null, earlyExitMarkets: null, earlyExitPositions: null, settlementRecordComplete: false, settlementPositionsChecked: 0};
    }
  });
}

function makeOutput(period, ranked, sportsRanked, activePositions, tradeRows, nowMs, consensusCandidates, records, leaderRecords, consensusGroups, settlementRecords) {
  // Preserve the exact period-scoped record used to rank this list.
  // Full-history enrichment must never overwrite weekly/monthly wins, losses,
  // win rate, or resolved-market count, otherwise the displayed numbers no
  // longer match the sort order.
  // Keep the general leaderboard populated; the public data cannot prove
  // that traders never exit early, so don't use a 100% profitable-close
  // filter as if it were a verified hold-to-settlement guarantee.
  const leaders = ranked.slice(0, LEADER_COUNT).map(trader => {
    const lifetime = leaderRecords[trader.wallet] || {};
    return {
      ...lifetime,
      ...trader,
      lifetimeWins: lifetime.wins ?? null,
      lifetimeLosses: lifetime.losses ?? null,
      lifetimeWinRate: lifetime.winRate ?? null,
      lifetimeResolvedMarkets: lifetime.resolvedMarkets ?? null,
      wins: trader.wins,
      losses: trader.losses,
      winRate: trader.winRate,
      resolvedMarkets: trader.resolvedMarkets,
      recordBasis: trader.recordBasis
    };
  });

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
      `General leaderboard ranks the discovered candidate set using the existing period ranking. Closed-position profitability is a realized-P&L proxy, not verified settlement accuracy and not proof that a trader never exits early. Candidate discovery is limited to the discovered public leaderboard universe.`,
    recordBasis: "IMPORTANT: complete profitable/unprofitable close counts are inferred from realized P&L, not verified market prediction wins/losses. If a history scan hits its cap, counts and rate are hidden rather than shown as a misleading 1,000–0 sample. Incomplete-record traders fall back to Polymarket period P&L/volume ranking.",
    candidateCount: ranked.length,
    sportsLeaders: sportsRanked.slice(0, LEADER_COUNT),
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: true,
    recordWarning:
      `Consensus groups open positions by exact market condition ID and exact outcome. Candidate coverage is limited to the discovered public leaderboard pool and the paginated position rows scanned. Closed-position profitability figures are P&L-based proxies, not verified settlement records.`,
    leaders,
    // Combo discovery pool is intentionally broad: combo traders can be profitable
    // despite low win rates or small sample sizes. Rank and display their actual combo positions.
    comboCandidates: ranked.map(trader => ({
      wallet: trader.wallet,
      name: trader.name || trader.username || "",
      rank: trader.rank ?? null,
      pnl: trader.pnl ?? trader.profit ?? null,
      volume: trader.volume ?? trader.vol ?? null,
      wins: trader.wins ?? null,
      losses: trader.losses ?? null,
      winRate: trader.winRate ?? null,
      resolvedMarkets: trader.resolvedMarkets ?? null,
      recordComplete: trader.recordComplete === true,
      recordBasis: trader.recordBasis || "Period market record unavailable"
    })),
    traderActiveTrades: Object.fromEntries(tradeRows.filter(Boolean)),
    traderActivePositions: leaderPositions,
    consensus: buildConsensus(consensusGroups, consensusCandidates || ranked, Object.fromEntries([...new Set([...Object.keys(records || {}), ...Object.keys(settlementRecords || {})])].map(wallet => [wallet, { ...(records?.[wallet] || {}), ...(settlementRecords?.[wallet] || {}), wins: records?.[wallet]?.wins ?? settlementRecords?.[wallet]?.wins, losses: records?.[wallet]?.losses ?? settlementRecords?.[wallet]?.losses }])) )
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

async function buildPeriod(period, discoveredCandidates, activePositions, nowMs, consensusCandidates, fullRecordCache, consensusGroups) {
  // Rank the leaderboard using a bounded top-wallet history sample, but verify
  // every wallet actually present in a discovered consensus market. Previously
  // only the first 1,000 leaderboard wallets received period records, so valid
  // consensus holders outside that arbitrary slice were silently discarded.
  const candidates = discoveredCandidates.slice(0, PROCESS_CANDIDATE_COUNT);
  const rankingRecordCandidates = candidates.slice(0, RECORD_SCAN_CANDIDATE_COUNT);
  const candidateByWallet = new Map(candidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  const consensusWallets = new Set(
    consensusGroups.flatMap(group => (Array.isArray(group.traders) ? group.traders : [])
      .map(holder => String(holder.wallet || "").toLowerCase())
      .filter(wallet => candidateByWallet.has(wallet)))
  );
  const recordCandidateMap = new Map(rankingRecordCandidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  for (const wallet of consensusWallets) {
    if (!recordCandidateMap.has(wallet)) recordCandidateMap.set(wallet, candidateByWallet.get(wallet));
  }
  const recordCandidates = [...recordCandidateMap.values()];
  console.log(`Discovered ${discoveredCandidates.length} ${period.key} candidates; scanning active positions across ${candidates.length} wallets, leaderboard records for ${rankingRecordCandidates.length}, and consensus-holder records for ${consensusWallets.size} (unique record scan: ${recordCandidates.length}).`);
  const records = await attachRecords(recordCandidates, period, nowMs, RANKING_RECORD_LIMIT);
  console.log(`${period.key}: verifying settlement outcomes for the top ${rankingRecordCandidates.length} wallets...`);
  const ranked = rankCandidates(candidates, records);
  const sportsRanked = rankSportsCandidates(candidates, records);
  const leaders = ranked.slice(0, LEADER_COUNT);
  // Detailed settlement verification is only needed for the visible Top 100.
  // Scanning 1,000 wallets across three API cohorts twice per period delayed the entire publish.
  const settlementRows = await attachSettlementStats(leaders, period, nowMs);
  const settlementRecords = Object.fromEntries(settlementRows.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  console.log(`${period.key}: consensus traders require at least ${MIN_TRADER_RESOLVED_MARKETS} closed positions and ${MIN_CONSENSUS_WIN_RATE}%+ profitable-close rate`);
  // Reuse the period-scoped record sample used for ranking; avoid a second history scan.
  const leaderRecords = records;
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
    sportsRanked,
    activePositions,
    tradeRows,
    nowMs,
    consensusCandidates,
    records,
    leaderRecords,
    consensusGroups,
    settlementRecords
  );
  // Enrich only the visible Top 100 with verified settlement outcomes and a
  // separate count of positions whose last event preceded market resolution.
  output.leaders = await attachSettlementStats(output.leaders, period, nowMs);

  await writeJsonUnderLimit(`${period.key}.json`, output);

  // Keep consensus data independent from the Top 100 period tabs. These
  // files reflect the actual fetched candidate pool; label the exact discovered pool size and the smaller settlement-verified sample separately.
  await writeJsonUnderLimit(`consensus-${period.key}.json`, {
    generatedAt: new Date(nowMs).toISOString(),
    pool: period.key,
    qualificationWindowDays: period.key === "month" ? 30 : 7,
    minimumWinRate: MIN_CONSENSUS_WIN_RATE,
    minimumResolvedMarkets: MIN_TRADER_RESOLVED_MARKETS,
    qualificationBasis: period.key === "week"
      ? "Every displayed consensus trader must have at least 5 closed markets in the last 7 days and a period win rate of at least 90%, using the same period-scoped record as the weekly leaderboard. Lifetime settlement counts do not qualify a trader."
      : "Every displayed consensus trader must have at least 10 closed markets in the last 30 days and a period win rate of at least 90%, using the same period-scoped record as the monthly leaderboard. Lifetime settlement counts do not qualify a trader.",
    minimumSameSideTraders: 2,
    entryWindowMinutes: CONSENSUS_ENTRY_WINDOW_MINUTES,
    candidateCount: ranked.length,
    discoveredCandidateCount: discoveredCandidates.length,
    settlementRecordCandidateCount: rankingRecordCandidates.length,
    consensusHolderRecordCandidateCount: consensusWallets.size,
    totalRecordCandidateCount: recordCandidates.length,
    scanDiagnostics: lastConsensusDiagnostics,
    candidatePoolBasis: "Unique traders collected from Polymarket public Data API v1 overall PnL and volume leaderboards, paginating each board through its supported offset ceiling (0–1,000), targeting 5,000 wallets. This is a broad leaderboard-derived sample, not all platform traders. Consensus market overlap counts open positions of at least $1 and retains same-outcome holders; upcoming markets within 30 days are eligible.",
    resolutionWindowHours: 168,
    entryActivityWindowHours: null,
    recordBasis: "Consensus traders are filtered by the selected period's closed-market profitable-close record (90%+ and at least 5 closed markets for weekly; 10 for monthly). Verified settlement data is retained separately and never substituted for period metrics.",
    consensus: output.consensus
  });

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

  // Use the full discovered pool for overlap detection; only the detailed
  // period-history scan is bounded separately.
  const positionCandidates = [
    ...weekCandidates.slice(0, PROCESS_CANDIDATE_COUNT),
    ...monthCandidates.slice(0, PROCESS_CANDIDATE_COUNT)
  ];
  for (const trader of positionCandidates) {
    if (!unique.has(trader.wallet)) unique.set(trader.wallet, trader);
  }

  console.log(`Loading positions for ${unique.size} unique traders...`);
  const fullRecordCache = new Map();
  const consensusCandidatePool = [...unique.values()];
  const consensusScan = await getBroadConsensusMarkets(consensusCandidatePool, nowMs, {});
  const activePositions = consensusScan.activePositionsByWallet;
  const consensusGroups = consensusScan.holderGroups;
  console.log("Broad consensus groups: " + consensusGroups.length);
  // Consensus members must come from the actual record-ranked candidates for
  // the selected period, not merely anyone in the raw P&L candidate fetch.
  // IMPORTANT: consensus membership must use the full discovered candidate pool,
  // not the tiny record-filtered leaderboard. The latter can make valid overlaps
  // vanish even when the position scan found them.
  await buildPeriod(PERIODS[0], weekCandidates, activePositions, nowMs, consensusCandidatePool, fullRecordCache, consensusGroups);
  await buildPeriod(PERIODS[1], monthCandidates, activePositions, nowMs, consensusCandidatePool, fullRecordCache, consensusGroups);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});