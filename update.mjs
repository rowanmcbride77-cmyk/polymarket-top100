
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const RANKING_RECORD_LIMIT = 1000;
const LEADER_RECORD_LIMIT = RANKING_RECORD_LIMIT;
const CONSENSUS_LIMIT = 50;
const CONSENSUS_MARKET_SCAN_LIMIT = 1000;
const CONSENSUS_WINDOW_HOURS = 24;
const CONSENSUS_HOLDERS_PER_MARKET = 1000;
const MIN_CONSENSUS_POSITION_VALUE = 5;
const CONSENSUS_ENTRY_WINDOW_MINUTES = 24 * 60;
const MIN_TRADER_RESOLVED_MARKETS = 20;
const TRADERS_PER_MARKET = 100;
const TRADE_LIMIT = 10;
const MAX_JSON_BYTES = 5 * 1024 * 1024;
const CONCURRENCY = 6;
const DELAY_MS = 150;

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
      recordComplete = pagination.has_more !== true && !pagination.next_cursor;
      break;
    }
    cursor = pagination.next_cursor || null;
    if (pagination.has_more !== true || !cursor) {
      recordComplete = true;
      break;
    }
  }

  const resolvedMarkets = wins + losses;
  return {
    wins, losses,
    sportsWins, sportsLosses,
    sportsResolvedMarkets: sportsWins + sportsLosses,
    sportsWinRate: (sportsWins + sportsLosses) ? (sportsWins / (sportsWins + sportsLosses)) * 100 : null,
    winRate: resolvedMarkets ? (wins / resolvedMarkets) * 100 : null,
    resolvedMarkets,
    recordVerified: resolvedMarkets > 0,
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

async function getCandidates(period, category = "OVERALL") {
  // P&L and volume leaderboards discover candidates only; final rank is record-based.
  // Polymarket does not provide a native win/loss leaderboard.
  const candidates = [];
  const byWallet = new Map();

  for (const orderBy of ["PNL", "VOL"]) {
    for (let offset = 0; offset < CANDIDATE_COUNT; offset += PAGE_SIZE) {
      const params = new URLSearchParams({
        category, timePeriod: period.api, orderBy,
        limit: String(PAGE_SIZE), offset: String(offset)
      });
      const rows = asArray(await getJson(`${API}/v1/leaderboard?${params}`));
      if (!rows.length) break;
      for (const row of rows) {
        const trader = normalizeLeader(row, candidates.length);
        if (!trader.wallet) continue;
        const existing = byWallet.get(trader.wallet);
        if (existing) {
          existing.pnl = trader.pnl ?? existing.pnl;
          existing.volume = trader.volume ?? existing.volume;
          if (!existing.name || existing.name === "Unknown") existing.name = trader.name;
          continue;
        }
        byWallet.set(trader.wallet, trader);
        candidates.push(trader);
      }
      if (rows.length < PAGE_SIZE) break;
    }
  }
  console.log(`${period.key}: discovered ${candidates.length} unique candidates from P&L + volume leaderboards; final rank is record-based`);
  return candidates;
}

function rankSportsCandidates(candidates, records = {}) {
  const rows = candidates.map(trader => ({ ...trader, ...(records[trader.wallet] || {}) }));
  return rows.filter(trader => (trader.sportsResolvedMarkets || 0) >= 5 && trader.sportsWinRate !== null)
    .sort((a, b) => (b.sportsWinRate || 0) - (a.sportsWinRate || 0) || (b.sportsResolvedMarkets || 0) - (a.sportsResolvedMarkets || 0))
    .map((trader, index) => ({ ...trader, sportsRank: index + 1 }));
}

function rankCandidates(candidates, records = {}) {
  return [...candidates]
    .map(trader => ({ ...trader, ...(records[trader.wallet] || {}) }))
    .filter(trader => trader.recordVerified && (trader.resolvedMarkets ?? 0) >= 10 && trader.winRate !== null)
    .sort((a, b) =>
      (b.winRate - a.winRate) ||
      ((b.resolvedMarkets ?? 0) - (a.resolvedMarkets ?? 0)) ||
      ((b.pnl ?? -Infinity) - (a.pnl ?? -Infinity)) ||
      ((b.volume ?? -Infinity) - (a.volume ?? -Infinity))
    )
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
    initialValue: num(position.entry_cost_usdc ?? position.total_cost_usdc ?? position.initialValue ?? position.initial_value ?? position.initial_value_usd),
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
        newestBuy: null,
        oldestBuy: null,
        traders: [],
        walletsSeen: new Set()
      });
    }

    const group = grouped.get(key);
    if (group.walletsSeen.has(wallet)) continue;
    group.walletsSeen.add(wallet);
    group.sameSide++;
    group.totalValue += value;
    group.totalEntry += value;
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
        amount: value,
        buyTime: lastEventAt
      });
    }
  }

  const qualifyingGroups = [...grouped.values()]
    .filter(group => group.sameSide >= 2);
  if (qualifyingGroups.length === 0) console.log("No same-side group had 2+ holders with position activity in the last " + CONSENSUS_ENTRY_WINDOW_MINUTES + " minutes for market " + conditionId);
  return qualifyingGroups
    .map(({ walletsSeen, ...group }) => group);
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

async function getBroadConsensusMarkets(knownTraders, nowMs, activePositions) {
  // The profile payload is intentionally capped to ten positions per trader.
  // That is too small for consensus discovery, so fetch the full first page
  // (up to 100) independently for each candidate wallet.
  const consensusPositions = await mapLimit(knownTraders, CONCURRENCY, async trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    if (!wallet) return [wallet, []];
    try {
      const params = new URLSearchParams({
        user: wallet,
        limit: "100",
        status: "OPEN",
        sort_by: "CURRENT_VALUE",
        sort_direction: "DESC"
      });
      const response = await getJson(API + "/v2/positions?" + params);
      return [wallet, asArray(response).map(compactPosition)];
    } catch (error) {
      console.warn("Consensus positions fetch failed for " + wallet + ": " + error.message);
      return [wallet, Array.isArray(activePositions?.[wallet]) ? activePositions[wallet] : []];
    }
  });
  const allPositionsByWallet = new Map(consensusPositions);
  // Gamma currently caps market-list responses at 100 rows per page even
  // when a larger limit is requested. Asking for 500 caused the loop to stop
  // after its first 100-market response, missing almost every candidate market.
  const pageSize = 100;
  // Gamma offset pagination rejects offsets beyond 2000; stay within its supported range.
  const maxPages = 20;
  const marketsById = new Map();
  for (let page = 0; page < maxPages; page++) {
    const params = new URLSearchParams({
      active: "true",
      closed: "false",
      limit: String(pageSize),
      offset: String(page * pageSize),
      order: "volume",
      ascending: "false"
    });
    const rows = asArray(await getJson(GAMMA_API + "/markets?" + params));
    console.log("Consensus market discovery page " + (page + 1) + ": " + rows.length + " markets");
    for (const market of rows) {
      const id = market.conditionId ?? market.condition_id;
      if (id) marketsById.set(String(id).toLowerCase(), market);
    }
    if (rows.length < pageSize) break;
  }

  const knownByWallet = new Map(knownTraders.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  const groups = new Map();
  let openPositionsSeen = 0;
  let positionsMatchedToMarket = 0;
  let positionsInWindow = 0;

  for (const trader of knownTraders) {
    const wallet = String(trader.wallet || "").toLowerCase();
    if (!wallet) continue;
    const positions = allPositionsByWallet.get(wallet) || (Array.isArray(activePositions?.[wallet]) ? activePositions[wallet] : []);
    for (const position of positions) {
      openPositionsSeen++;
      const conditionId = String(position.conditionId ?? position.condition_id ?? "").toLowerCase();
      if (!conditionId) continue;
      const market = marketsById.get(conditionId);
      if (!market || market.closed === true || market.active === false) continue;
      positionsMatchedToMarket++;
      if (!isResolvingSoon(market, nowMs)) continue;
      positionsInWindow++;

      const side = String(position.outcome ?? position.outcome_name ?? "").trim();
      if (!side) continue;
      const key = conditionId + "|" + side.toLowerCase();
      if (!groups.has(key)) {
        groups.set(key, {
          conditionId,
          title: position.title || market.question || market.title || "Unknown market",
          endDate: marketEndDate(market),
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
      const currentValue = num(position.currentValue ?? position.current_value) ?? 0;
      const invested = num(position.entry_cost_usdc ?? position.total_cost_usdc ?? position.initialValue ?? position.initial_value) ?? currentValue;
      const lastEventAt = timestampMs(position.lastEventAt ?? position.last_event_at ?? position.firstEntryAt ?? position.first_entry_at);
      group.sameSide++;
      group.totalValue += currentValue;
      group.totalEntry += invested;
      if (lastEventAt !== null) {
        group.newestBuy = group.newestBuy === null ? lastEventAt : Math.max(group.newestBuy, lastEventAt);
        group.oldestBuy = group.oldestBuy === null ? lastEventAt : Math.min(group.oldestBuy, lastEventAt);
      }
      if (group.traders.length < TRADERS_PER_MARKET) {
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
  }

  const holderGroups = [...groups.values()]
    .filter(group => group.sameSide >= 2)
    .map(({ walletsSeen, ...group }) => group)
    .sort((a, b) => b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0))
    .slice(0, CONSENSUS_LIMIT);

  console.log("Consensus position matching: " + JSON.stringify({
    discoveredMarkets: marketsById.size,
    candidateWallets: knownTraders.length,
    openPositionsSeen,
    positionsMatchedToMarket,
    positionsIn24hWindow: positionsInWindow,
    sameSideGroups: holderGroups.length
  }));
  return holderGroups;
}

function buildConsensus(holderGroups, candidates, records = {}) {
  const candidateMap = new Map(candidates.map(trader => [
    String(trader.wallet || "").toLowerCase(), trader
  ]));
  return holderGroups.map(group => {
    const traders = group.traders.flatMap(holder => {
      const wallet = String(holder.wallet || "").toLowerCase();
      const candidate = candidateMap.get(wallet);
      if (!candidate) return [];
      const record = records[wallet] || records[candidate.wallet] || candidate;
      // A consensus member must have a usable resolved-position sample in the
      // same selected period; never label all-time history as recent activity.
      // The candidate leaderboard already applies its minimum-history rules.
      // Requiring 20 positions resolved in just the last 7/30 days made valid
      // high-history traders disappear from consensus almost universally.
      const lifetimeResolved = num(candidate.resolvedMarkets ?? candidate.totalResolvedMarkets ?? candidate.closedMarkets);
      if (lifetimeResolved !== null && lifetimeResolved < MIN_TRADER_RESOLVED_MARKETS) return [];
      return [{
        ...holder,
        wallet,
        name: holder.name || candidate.name || "Trader",
        rank: candidate.sportsRank ?? candidate.rank ?? null,
        overallRank: candidate.rank ?? null,
        pnl: candidate.pnl ?? holder.pnl ?? null,
        volume: candidate.volume ?? holder.volume ?? null,
        wins: record.wins ?? null,
        losses: record.losses ?? null,
        winRate: record.winRate ?? null,
        resolvedMarkets: record.resolvedMarkets ?? null,
        sportsWins: record.sportsWins ?? null,
        sportsLosses: record.sportsLosses ?? null,
        sportsWinRate: record.sportsWinRate ?? null,
        sportsResolvedMarkets: record.sportsResolvedMarkets ?? null
      }];
    });
    const totalValue = traders.reduce((sum, trader) => sum + (num(trader.amount) ?? 0), 0);
    return { ...group, traders, sameSide: traders.length, totalValue };
  }).filter(group => group.sameSide >= 2)
    .sort((a, b) => b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0))
    .slice(0, CONSENSUS_LIMIT);
}
function makeOutput(period, ranked, sportsRanked, activePositions, tradeRows, nowMs, consensusCandidates, records, leaderRecords, consensusGroups) {
  // Preserve the exact period-scoped record used to rank this list.
  // Full-history enrichment must never overwrite weekly/monthly wins, losses,
  // win rate, or resolved-market count, otherwise the displayed numbers no
  // longer match the sort order.
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
      `Ranked by profitable closed-position rate (realized P&L proxy, NOT verified prediction/settlement accuracy) in the selected ${period.key === "week" ? "7-day" : "30-day"} period, then sample size, period P&L, and volume as tie-breakers. Candidate discovery combines up to ${CANDIDATE_COUNT} entries from each Polymarket P&L and volume leaderboard; because Polymarket does not expose a native win/loss leaderboard, this is the top record-ranked set within that discovered candidate universe, not a claim to cover every trader on the platform.`,
    recordBasis: "IMPORTANT: these are profitable/unprofitable closed-position counts inferred from realized P&L, not verified market prediction wins/losses. A high profitable-close rate can coexist with negative period P&L because position sizes, open positions, and timing differ.",
    candidateCount: ranked.length,
    sportsLeaders: sportsRanked.slice(0, LEADER_COUNT),
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: true,
    recordWarning:
      `Consensus includes same-side open positions in markets with a listed end date within 24 hours. Closed-position profitability figures are P&L-based proxies, not verified settlement records.`,
    leaders,
    traderActiveTrades: Object.fromEntries(tradeRows.filter(Boolean)),
    traderActivePositions: leaderPositions,
    consensus: buildConsensus(consensusGroups, (consensusCandidates || ranked).slice(0, CANDIDATE_COUNT), records)
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

async function buildPeriod(period, candidates, activePositions, nowMs, consensusCandidates, fullRecordCache, consensusGroups) {
  console.log(`Loading sample closed-position records for ${candidates.length} ${period.key} candidates...`);
  const records = await attachRecords(candidates, period, nowMs, RANKING_RECORD_LIMIT);
  const ranked = rankCandidates(candidates, records);
  const sportsRanked = rankSportsCandidates(candidates, records);
  const leaders = ranked.slice(0, LEADER_COUNT);
  console.log(`${period.key}: ${leaders.length} traders qualify with at least 10 classifiable closed positions`);
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
    consensusGroups
  );

  await writeJsonUnderLimit(`${period.key}.json`, output);

  // Keep consensus data independent from the Top 100 period tabs. These
  // files reflect the actual fetched candidate pool; do not label them as
  // 10,000-trader pools until 10,000 candidates are fetched and validated.
  await writeJsonUnderLimit(`consensus-${period.key}.json`, {
    generatedAt: new Date(nowMs).toISOString(),
    pool: period.key,
    candidateCount: Math.min(CANDIDATE_COUNT, ranked.length),
    candidatePoolBasis: "Consensus is restricted to the record-ranked candidate pool. Ranking uses up to 1,000 recent closed-position rows per trader (P&L-based profitability proxy, not settlement-verified); candidate discovery combines up to 1,000 P&L-ranked and 1,000 volume-ranked leaderboard entries, so this is not platform-wide exhaustive.",
    resolutionWindowHours: CONSENSUS_WINDOW_HOURS,
    entryActivityWindowHours: CONSENSUS_ENTRY_WINDOW_MINUTES / 60,
    recordBasis: "Win-loss is not settlement-verified; current trader records are based on realized P&L on closed positions.",
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

  for (const trader of [...weekCandidates, ...monthCandidates]) {
    if (!unique.has(trader.wallet)) unique.set(trader.wallet, trader);
  }

  console.log(`Loading positions for ${unique.size} unique traders...`);
  const activePositions = await attachActivePositions(
    [...unique.values()],
    nowMs
  );

  const fullRecordCache = new Map();
  const consensusCandidatePool = [...unique.values()];
  const consensusGroups = await getBroadConsensusMarkets(consensusCandidatePool, nowMs, activePositions);
  console.log("Broad consensus groups: " + consensusGroups.length);
  // Consensus members must come from the actual record-ranked candidates for
  // the selected period, not merely anyone in the raw P&L candidate fetch.
  await buildPeriod(PERIODS[0], weekCandidates, activePositions, nowMs, null, fullRecordCache, consensusGroups);
  await buildPeriod(PERIODS[1], monthCandidates, activePositions, nowMs, null, fullRecordCache, consensusGroups);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
