
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const GAMMA_API = "https://gamma-api.polymarket.com";
const PERIODS = [
  { key: "week", api: "WEEK" },
  { key: "month", api: "MONTH" }
];

// Discover and use a broad pool for overlap detection. Expensive per-wallet history
// verification remains separately bounded so the scheduled workflow can finish.
const MIN_CONSENSUS_WIN_RATE = 90;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const MAX_CANDIDATES_PER_PERIOD = 5000;
const MAX_LEADERBOARD_PAGES_PER_STREAM = 20;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const RANKING_RECORD_LIMIT = 1000;
// Bound closed-position history requests. Records that hit this cap are marked
// incomplete and withheld rather than presented as complete win/loss stats.
const CLOSED_POSITION_SCAN_LIMIT = 250;
// Discover shared holdings across the full 5,000-wallet candidate pool.
// Four position pages per wallet keeps the scan bounded while widening coverage.
const CONSENSUS_WALLET_LIMIT = 5000;
const MAX_CONSENSUS_POSITION_PAGES_PER_WALLET = 4;
const LEADER_RECORD_LIMIT = RANKING_RECORD_LIMIT;
// Keep upcoming markets in the data so the UI can filter 24h / 3d / 7d accurately.
const CONSENSUS_WINDOW_HOURS = 168; // Include active markets ending within 7 days; exclude long-dated markets years away
const CONSENSUS_HOLDERS_PER_MARKET = 1500;
const MIN_CONSENSUS_POSITION_VALUE = 1;
const MIN_CONSENSUS_ENTRY_VALUE = 0; // No minimum original entry size.
const CONSENSUS_ENTRY_WINDOW_MINUTES = 60;
const MIN_TRADER_RESOLVED_MARKETS = 10;
const MIN_CONSENSUS_CLOSED_MARKETS = 0; // Only a complete 30-day record and 90%+ rate; no extra trade-count floor.
const CONSENSUS_RECORD_PERIOD = {key: "month", api: "MONTH"};
const TRADERS_PER_MARKET = 500;
const TRADE_LIMIT = 10;
const MAX_JSON_BYTES = 20 * 1024 * 1024;
const CONCURRENCY = 10;
const DELAY_MS = 50;
// Bound discovery to four pages (400 positions) per wallet so scheduled scans finish predictably.
let lastConsensusDiagnostics = null;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// One shared request gate for every Data API call. Ten concurrent workers
// previously produced bursts that exceeded the positions endpoint's allowance.
const API_MIN_INTERVAL_MS = 125;
let nextApiRequestAt = 0;
let apiPauseUntil = 0;

async function waitForApiSlot() {
  while (true) {
    const now = Date.now();
    const waitMs = Math.max(nextApiRequestAt, apiPauseUntil) - now;
    if (waitMs > 0) {
      await sleep(waitMs);
      continue;
    }
    nextApiRequestAt = Date.now() + API_MIN_INTERVAL_MS;
    return;
  }
}

function retryAfterMs(response, attempt) {
  const header = response.headers.get("retry-after");
  if (header) {
    const seconds = Number(header);
    if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
    const dateMs = Date.parse(header);
    if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  }
  return Math.min(30000, 1500 * (2 ** attempt));
}

async function getJson(url, attempts = 6) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      await waitForApiSlot();
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/1.0"
        },
        signal: AbortSignal.timeout(30000)
      });

      if (response.status === 429 || response.status === 503 || response.status >= 500) {
        const delayMs = retryAfterMs(response, attempt);
        // A rate-limit response pauses all workers, not just the worker that
        // received it, so queued requests do not immediately trigger more 429s.
        apiPauseUntil = Math.max(apiPauseUntil, Date.now() + delayMs);
        throw new Error(`HTTP ${response.status} (retry after ${Math.ceil(delayMs / 1000)}s)`);
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
        const backoffMs = Math.min(30000, 750 * (2 ** attempt));
        await sleep(backoffMs);
      }
    }
  }

  throw lastError || new Error("Request failed");
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (Array.isArray(value?.data)) return value.data;
  if (Array.isArray(value?.data?.leaderboard)) return value.data.leaderboard;
  if (Array.isArray(value?.leaderboard)) return value.leaderboard;
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
    row?.userId ??
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
    // v2's bare volume is shares; only publish a dollar figure when the API
    // explicitly supplies a USDC-denominated field.
    volume: num(row.volume_usdc ?? row.volumeUsdc ?? row.vol ?? null),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

async function getResolvedRecord(wallet, period, nowMs, maxPositions = RANKING_RECORD_LIMIT) {
  if (!wallet) return null;

  // Consensus eligibility uses the selected rolling 30-day period. Capped or incomplete samples are withheld.
  const periodKey = String(period?.api || period?.key || period || "").toUpperCase();
  const allTime = periodKey === "ALL" || periodKey === "ALL_TIME" || periodKey === "ALLTIME";
  const periodDays = periodKey === "MONTH" ? 30 : 7;
  const cutoffMs = allTime ? 0 : nowMs - periodDays * 24 * 60 * 60 * 1000;
  const seenPositions = new Set();
  const marketPnl = new Map();
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
      sort_direction: "DESC"
    });
    if (!allTime) {
      params.set("start", String(Math.floor(cutoffMs / 1000)));
      params.set("end", String(Math.floor(nowMs / 1000)));
    }
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
      if (!allTime && eventMs !== null && eventMs < cutoffMs) {
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
      if (!key || seenPositions.has(key)) continue;
      seenPositions.add(key);
      realizedPnlTotal += pnl;
      // Count one result per market, not one result per outcome-token row.
      // Both outcome positions in the same condition are netted together.
      const marketKey = condition || key;
      marketPnl.set(marketKey, (marketPnl.get(marketKey) || 0) + pnl);
      if (isSportsMarket(row)) {
        if (pnl > 0) sportsWins++; else sportsLosses++;
      }
    }

    if (reachedPeriodCutoff) {
      recordComplete = true;
      break;
    }
    if (maxPositions !== null && scannedPositions >= maxPositions) {
      recordComplete = pagination.has_more !== true;
      break;
    }
    cursor = pagination.next_cursor || null;
    if (pagination.has_more !== true || !cursor) {
      recordComplete = true;
      break;
    }
  }

  for (const marketPnlValue of marketPnl.values()) {
    if (marketPnlValue > 0.000001) wins++;
    else if (marketPnlValue < -0.000001) losses++;
  }
  const resolvedMarkets = wins + losses;
  // A capped sample is not a complete record. Never publish its partial
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
      ? `Complete closed-position scan from the all available history; one profitable/unprofitable result per market using net realized P&L. This is a close-profitability proxy, not verified settlement accuracy; early exits count.`
      : `INCOMPLETE sample of closed positions across all available history; market-level profitable/unprofitable counts are withheld. This is a close-profitability proxy, not verified settlement accuracy.`
  };
}
async function attachRecords(traders, period, nowMs, maxPositions = RANKING_RECORD_LIMIT, cache = null) {
  const rows = await mapLimit(traders, CONCURRENCY, async trader => {
    try {
      let recordPromise;
      const cacheKey = String(period?.key || period?.api || period || "week") + "|" + String(maxPositions ?? "unbounded") + "|" + String(trader.wallet || "").toLowerCase();
      if (cache && cache.has(cacheKey)) {
        recordPromise = cache.get(cacheKey);
      } else {
        recordPromise = getResolvedRecord(trader.wallet, period, nowMs, maxPositions);
        if (cache) cache.set(cacheKey, recordPromise);
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
  const byWallet = new Map();
  const categories = ["overall","sports","politics","crypto","esports","culture","mentions","weather","economics","tech","finance"];
  const timePeriods = [...new Set([period.api,"DAY","WEEK","MONTH","ALL"])];
  let pagesFetched = 0;
  for (const category of categories) {
    if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD) break;
    for (const timePeriod of timePeriods) {
      if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD) break;
      for (const orderBy of ["PNL","VOLUME"]) {
        if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD) break;
        let cursor = null, page = 0;
        const seenCursors = new Set();
        while (page < MAX_LEADERBOARD_PAGES_PER_STREAM && byWallet.size < MAX_CANDIDATES_PER_PERIOD) {
          const params = new URLSearchParams({category,time_period:timePeriod.toLowerCase(),sort_by:orderBy,limit:String(PAGE_SIZE)});
          if (cursor) params.set("cursor",cursor);
          const response = await getJson(`${API}/v2/leaderboard?${params}`);
          const rows = asArray(response);
          pagesFetched++;
          if (!rows.length) break;
          for (let i=0;i<rows.length;i++) {
            const trader=normalizeLeader(rows[i],page*PAGE_SIZE+i+1);
            if(!trader.wallet) continue;
            const wallet=String(trader.wallet).toLowerCase();
            if(timePeriod!==period.api){trader.pnl=null;trader.volume=null;}
            const existing=byWallet.get(wallet);
            if(existing){
              if((existing.pnl===null||existing.pnl===undefined)&&trader.pnl!==null)existing.pnl=trader.pnl;
              if((existing.volume===null||existing.volume===undefined)&&trader.volume!==null)existing.volume=trader.volume;
              if((!existing.name||existing.name==="Unknown")&&trader.name)existing.name=trader.name;
            } else byWallet.set(wallet,trader);
          }
          const nextCursor=response?.pagination?.next_cursor??null;
          page++;
          if(!nextCursor||response?.pagination?.has_more===false)break;
          const next=String(nextCursor);
          if(seenCursors.has(next)) {
            console.warn(`Leaderboard cursor repeated for ${category}/${timePeriod}/${orderBy}; ending this cursor stream safely.`);
            break;
          }
          seenCursors.add(next);
          cursor=next;
        }
      }
    }
    console.log(`Candidate discovery: ${category} complete; ${byWallet.size.toLocaleString()} unique wallets accumulated.`);
  }
  const candidates=[...byWallet.values()];
  console.log(`${period.key}: discovered ${candidates.length.toLocaleString()} unique traders across ${pagesFetched.toLocaleString()} public leaderboard pages; no 10,000-wallet cap.`);
  if(!candidates.length)throw new Error(`${period.key}: public leaderboard discovery returned no trader wallets.`);
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
      // Keep P&L from Polymarket's explicit WEEK/MONTH leaderboard period.
      // Position.realized_pnl is cumulative for that position, so summing it
      // for positions with a recent last_event_at can incorrectly look like
      // lifetime P&L rather than the selected period's P&L.
      merged.pnlBasis = `Polymarket Data API v2 ${String(record.periodKey || "").toLowerCase() || "selected"} leaderboard P&L`;
      return merged;
    })
    .sort((a, b) => {
      // A trader is eligible for record ranking only with a complete sample,
      // consistent W-L counts, and the minimum number of classifiable closes.
      // The primary leaderboard promise is period profit, not a huge
      // profitable-close count. Sort by the explicit WEEK/MONTH leaderboard
      // P&L first; use volume as a secondary signal and the close proxy only
      // as a later tie-breaker. This prevents 964-0 / $2k rows outranking
      // traders who actually made substantially more in the selected period.
      const aPnl = Number(a.pnl);
      const bPnl = Number(b.pnl);
      const aHasPnl = a.pnl !== null && a.pnl !== undefined && a.pnl !== "" && Number.isFinite(aPnl);
      const bHasPnl = b.pnl !== null && b.pnl !== undefined && b.pnl !== "" && Number.isFinite(bPnl);
      if (aHasPnl !== bHasPnl) return aHasPnl ? -1 : 1;
      if (aHasPnl && bHasPnl && aPnl !== bPnl) return bPnl - aPnl;
      const aVolume = Number(a.volume), bVolume = Number(b.volume);
      if (Number.isFinite(aVolume) && Number.isFinite(bVolume) && aVolume !== bVolume) return bVolume - aVolume;
      const aCountsValid = Number.isFinite(Number(a.wins)) && Number(a.wins) >= 0 &&
        Number.isFinite(Number(a.losses)) && Number(a.losses) >= 0 &&
        Number(a.resolvedMarkets) === Number(a.wins) + Number(a.losses) &&
        Number(a.resolvedMarkets) > 0;
      const bCountsValid = Number.isFinite(Number(b.wins)) && Number(b.wins) >= 0 &&
        Number.isFinite(Number(b.losses)) && Number(b.losses) >= 0 &&
        Number(b.resolvedMarkets) === Number(b.wins) + Number(b.losses) &&
        Number(b.resolvedMarkets) > 0;
      const aComplete = a.recordComplete === true && aCountsValid && Number(a.resolvedMarkets) >= MIN_TRADER_RESOLVED_MARKETS;
      const bComplete = b.recordComplete === true && bCountsValid && Number(b.resolvedMarkets) >= MIN_TRADER_RESOLVED_MARKETS;
      if (aComplete !== bComplete) return aComplete ? -1 : 1;
      return (Number(b.resolvedMarkets) || 0) - (Number(a.resolvedMarkets) || 0);
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
    let endMs = timestampMs(endDate);
    if (typeof endDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      endMs = Date.parse(endDate + "T23:59:59.999Z");
    }
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
    clobTokenId: position.asset ?? position.token_id ?? position.tokenId ?? position.clobTokenId ?? null,
    title: position.title || position.question || "Unknown market",
    slug: position.slug ?? null,
    eventSlug: position.event_slug ?? position.eventSlug ?? null,
    eventTitle: position.event_title ?? position.eventTitle ?? null,
    eventStartTime: position.event_start_time ?? position.eventStartTime ?? null,
    category: position.category ?? null,
    subcategory: position.subcategory ?? null,
    sport: position.sport ?? null,
    sportsMarketType: position.sports_market_type ?? position.sportsMarketType ?? null,
    gameId: position.game_id ?? position.gameId ?? null,
    gameStartTime: position.game_start_time ?? position.gameStartTime ?? null,
    tags: Array.isArray(position.tags) ? position.tags : [],
    outcome: position.outcome ?? position.outcome_name ?? null,
    size: num(position.current_size ?? position.currentSize ?? position.size),
    avgPrice: num(position.avg_price ?? position.avgPrice ?? position.averagePrice ?? position.average_price),
    currentPrice: num(position.current_price ?? position.currentPrice ?? position.curPrice ?? position.cur_price),
    currentValue: num(position.current_value ?? position.currentValue) ??
      (() => {
        const shares = num(position.current_size ?? position.currentSize ?? position.size);
        const price = num(position.current_price ?? position.currentPrice ?? position.curPrice ?? position.cur_price);
        return shares !== null && price !== null && shares >= 0 && price >= 0 ? shares * price : null;
      })(),
    initialValue: num(position.entry_cost_usdc ?? position.total_cost_usdc ?? position.initialValue ?? position.initial_value ?? position.initial_value_usd) ??
      (() => {
        const avg = num(position.avg_price ?? position.avgPrice ?? position.averagePrice ?? position.average_price);
        const shares = num(position.total_size ?? position.totalSize ?? position.current_size ?? position.currentSize ?? position.size);
        return avg !== null && shares !== null && avg >= 0 && shares > 0 ? avg * shares : null;
      })(),
    realizedPnl: num(position.realizedPnl ?? position.realized_pnl ?? position.cashPnl ?? position.cash_pnl ?? position.total_pnl ?? position.totalPnl),
    lastEventAt: timestampMs(
      position.lastEventAt ?? position.last_event_at
    ),
    firstEntryAt: timestampMs(position.firstEntryAt ?? position.first_entry_at),
    endDate:
      position.endDate ??
      position.end_date ??
      position.endDateIso ??
      null,
    status: position.status ?? null,
    closed: position.closed === true,
    redeemable: position.redeemable === true,
    mergeable: position.mergeable === true
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
    const rows = asArray(await getJson(`${API}/v2/trades?${params}`));

    return rows
      .filter(row => String(row.side || "").toUpperCase() === "BUY")
      .slice(0, TRADE_LIMIT)
      .map(row => {
        const price = num(row.price);
        const shares = num(row.size);
        return {
          title: row.title || row.question || row.event_slug || row.eventSlug || "Unknown market",
          side: "BUY",
          outcome: row.outcome || row.outcome_name || "Unknown",
          amount: num(row.usdc_size ?? row.usdcSize ?? row.amount) ?? (price !== null && shares !== null ? price * shares : null),
          buyTime: timestampMs(row.timestamp ?? row.created_at ?? row.createdAt ?? row.time),
          conditionId: row.condition_id || row.conditionId || null
        };
      })
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

    // An open, non-redeemable position is active even if the original entry
    // was made earlier. Do not require a recent add-on trade to count it.
    const lastEventAt = timestampMs(position.last_event_at ?? position.lastEventAt ?? position.updated_at ?? position.updatedAt ?? position.first_entry_at ?? position.firstEntryAt);

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
    const shares = num(position.size ?? position.current_size ?? position.currentSize ?? position.shares ?? position.balance);
    const directAvgPrice = num(position.avgPrice ?? position.avg_price ?? position.averagePrice ?? position.average_price);
    const explicitEntryCost = num(position.entry_cost_usdc ?? position.total_cost_usdc);
    const avgPrice = directAvgPrice ?? (
      explicitEntryCost !== null && shares !== null && shares > 0
        ? explicitEntryCost / shares
        : null
    );
    // Only use an explicit USDC cost basis when the API omits average price.
    const entryValue = (avgPrice !== null && shares !== null && avgPrice > 0 && shares > 0)
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
  if (qualifyingGroups.length === 0) console.log("No same-side group had 2+ active holders for market " + conditionId);
  return qualifyingGroups
    .map(({ walletsSeen, ...group }) => ({ ...group, totalEntry: group.entryValueKnownCount === group.sameSide ? group.totalEntry : null, traders: group.traders.map(trader => ({ ...trader, isStillHolding: true, positionStatus: "OPEN" })) }));
}

function marketEndDate(market) {
  // Do not mistake event/game start dates for a market resolution deadline.
  // Only explicit market end/resolution fields are valid for the 24-hour filter.
  const direct = market.endDate ?? market.end_date ?? market.endDateIso ?? market.endDateISO ??
    market.end_date_iso ?? market.resolveTime ?? market.resolve_time ?? market.endTime ?? market.end_time ?? null;
  if (direct) return direct;
  const events = Array.isArray(market.events) ? market.events : [];
  for (const event of events) {
    const eventDate = event?.endDate ?? event?.end_date ?? event?.endDateIso ??
      event?.resolveTime ?? event?.resolve_time ?? event?.endTime ?? event?.end_time ?? null;
    if (eventDate) return eventDate;
  }
  return null;
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

function isLiveConsensusPosition(position) {
  const size = num(position.size ?? position.current_size ?? position.currentSize);
  const currentValue = num(position.current_value ?? position.currentValue);
  if (size === null || size <= 0) return false;
  if (currentValue === null || currentValue < MIN_CONSENSUS_POSITION_VALUE) return false;
  if (position.closed === true || position.redeemable === true || position.mergeable === true) return false;
  const status = String(position.status ?? "").toUpperCase();
  return !status || status === "OPEN" || status === "ACTIVE";
}

async function getBroadConsensusMarkets(knownTraders, nowMs, activePositions) {
  // Discover open holdings from a broad leaderboard-derived wallet pool, with a hard page cap.
  const consensusPositions = await mapLimit(knownTraders, CONCURRENCY, async trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    if (!wallet) return [wallet, []];
    try {
      const all = [];
      const seen = new Set();
      let cursor = null;
      let pages = 0;
      let hasMore = false;
      const seenCursors = new Set();
      while (pages < MAX_CONSENSUS_POSITION_PAGES_PER_WALLET) {
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
        const cursorKey = String(cursor);
        if (seenCursors.has(cursorKey)) {
          console.warn("Consensus positions cursor repeated for " + wallet + "; stopping this wallet to avoid an infinite loop.");
          break;
        }
        seenCursors.add(cursorKey);
      }
      return [wallet, all, hasMore, pages];
    } catch (error) {
      console.error("Consensus positions fetch failed for " + wallet + ": " + error.message);
      // Mark the wallet as failed. The scan must not publish a partial consensus
      // dataset as if every wallet had been checked successfully.
      return [wallet, Array.isArray(activePositions?.[wallet]) ? activePositions[wallet] : [], false, 0, true];
    }
  });

  const failedWallets = consensusPositions.filter(row => row?.[4] === true);
  if (failedWallets.length) {
    throw new Error(
      `Consensus scan aborted: positions could not be fetched for ${failedWallets.length} wallets. Existing published data will be preserved rather than replaced with incomplete results.`
    );
  }

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
      // OPEN is a superset in Data API v2 and includes redeemable settled
      // winners. Consensus must only count genuinely active, non-dust holdings.
      if (!isLiveConsensusPosition(position)) continue;
      openPositionsSeen++;
      const conditionId = String(position.conditionId ?? position.condition_id ?? "").toLowerCase();
      const side = String(position.outcome ?? position.outcome_name ?? "").trim();
      if (!conditionId || !side) continue;
      const currentValue = num(position.currentValue ?? position.current_value) ?? 0;
      const size = num(position.size ?? position.current_size ?? position.currentSize) ?? 0;
      const invested = num(position.initialValue ?? position.initial_value) ?? 0;
      // Count every genuine active holding, regardless of original entry size or whether entry cost is available.
      if (currentValue <= 0 && size <= 0) continue;

      positionsWithConditionAndSide++;
      const key = conditionId + "|" + side.toLowerCase();
      if (!groups.has(key)) {
        groups.set(key, {
          conditionId,
          clobTokenIds: [],
          title: position.title || position.question || "Unknown market",
          slug: position.slug ?? null,
          eventSlug: position.eventSlug ?? null,
          eventTitle: position.eventTitle ?? null,
          category: position.category ?? null,
          subcategory: position.subcategory ?? null,
          sport: position.sport ?? null,
          sportsMarketType: position.sportsMarketType ?? null,
          gameId: position.gameId ?? null,
          gameStartTime: position.gameStartTime ?? null,
          tags: position.tags ?? [],
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
      if (position.clobTokenId && !group.clobTokenIds.includes(String(position.clobTokenId))) group.clobTokenIds.push(String(position.clobTokenId));
      if (group.walletsSeen.has(wallet)) continue;
      group.walletsSeen.add(wallet);
      const lastEventAt = timestampMs(position.lastEventAt ?? position.last_event_at ?? position.firstEntryAt ?? position.first_entry_at);
      // Position activity is a discovery hint only. Exact BUY prints are verified
      // against the market trade feed before a signal can be published.
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
        entryValue: invested,
        currentValue,
        buyTime: lastEventAt
      });
    }
  }

  // A market is only a directional consensus when 2+ traders agree on
  // exactly one outcome. If both YES and NO have 2+ holders, the market is
  // contradictory and must be excluded entirely (not shown as two rows).
  const qualifyingSidesByCondition = new Map();
  for (const group of groups.values()) {
    if (group.sameSide < 2) continue;
    qualifyingSidesByCondition.set(
      group.conditionId,
      (qualifyingSidesByCondition.get(group.conditionId) || 0) + 1
    );
  }
  const holderGroups = [...groups.values()]
    .filter(group => group.sameSide >= 2)
    .filter(group => qualifyingSidesByCondition.get(group.conditionId) === 1)
    .map(({ walletsSeen, ...group }) => group)
    .sort((a, b) => b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0));

  lastConsensusDiagnostics = {
    candidateWallets: knownTraders.length,
    positionPagesFetched,
    openPositionsSeen,
    positionsWithConditionAndSide,
    walletsWithMorePositions,
    sameSideGroups: holderGroups.length,
    pageCapPerWallet: MAX_CONSENSUS_POSITION_PAGES_PER_WALLET,
    note: "Open-position discovery scans up to 4 pages (400 positions) for each of up to 5,000 wallets. Any positive active holding counts; no minimum entry size or recent BUY requirement. Published groups require 2+ same-side holders and a complete 30-day record at 90%+."
  };
  console.log("Consensus scan diagnostics: " + JSON.stringify(lastConsensusDiagnostics));
  const activePositionsByWallet = Object.fromEntries(consensusPositions.map(([wallet, positions]) => [wallet, positions]));
  return {holderGroups, activePositionsByWallet};
}

function classifyMarketCategory(market, group = {}) {
  const tags = Array.isArray(market?.tags) ? market.tags.map(tag => typeof tag === "string" ? tag : (tag.label || tag.slug || tag.name || "")) : [];
  const events = Array.isArray(market?.events) ? market.events : [];
  const eventText = events.flatMap(event => [event.title, event.slug, event.category, event.sport, event.seriesSlug]);
  const raw = [market?.category, market?.subcategory, market?.sport, market?.sportsMarketType, market?.sports_market_type, market?.seriesSlug, market?.slug, market?.eventSlug, market?.event_slug, market?.eventTitle, market?.event_title, market?.question, market?.title, market?.description, group?.category, group?.subcategory, group?.sport, group?.sportsMarketType, group?.eventSlug, group?.eventTitle, group?.slug, group?.title, ...tags, ...(group?.tags || []), ...eventText].filter(Boolean).join(" ").toLowerCase();
  // Classify esports before sports: the substring "esports" must never
  // be swallowed by the general sports bucket.
  if (/\b(esports|e-sports|league of legends|lol|dota\s*2|counter.?strike|cs2|valorant|overwatch|rocket league)\b/.test(raw)) return "esports";
  const sportsSlug = /\b(nba|wnba|nfl|mlb|nhl|ncaab|ncaaf|ncaa|mls|epl|ucl|ufc|atp|wta|pga|f1|nascar|cfl|liga|bundesliga|laliga|serie-a|ligue-1|cricket|rugby|tennis|golf|boxing|mma)\b/.test(raw);
  // Many active sports positions omit an explicit league/category field.
  // Their market question or slug still contains the participating team.
  const sportsTeam = /\b(lakers|celtics|warriors|knicks|nets|bulls|heat|bucks|sixers|76ers|cavaliers|cavs|clippers|suns|nuggets|mavericks|mavs|rockets|spurs|grizzlies|thunder|timberwolves|pelicans|kings|trail blazers|blazers|jazz|hawks|magic|hornets|pistons|raptors|wizards|chiefs|eagles|cowboys|49ers|niners|ravens|bills|bengals|steelers|packers|lions|vikings|bears|commanders|giants|jets|patriots|dolphins|broncos|chargers|raiders|seahawks|rams|cardinals|falcons|panthers|saints|buccaneers|bucs|titans|colts|texans|jaguars|browns|orioles|yankees|red sox|blue jays|rays|astros|mariners|rangers|dodgers|padres|giants|mets|phillies|braves|cubs|cardinals|brewers|twins|guardians|tigers|royals|athletics|angels|white sox|reds|pirates|marlins|nationals|rockies|maple leafs|bruins|rangers|penguins|capitals|lightning|panthers|oilers|canucks|flames|avalanche|golden knights|stars|wild|jets|predators|hurricanes|islanders|devils|sabres|red wings|senators|ducks|sharks|kings|kraken|blues|blackhawks|canadiens|flyers|blue jackets)\b/.test(raw);
  if (market?.gameId || market?.game_id || market?.gameStartTime || market?.game_start_time ||
      group?.gameId || group?.gameStartTime || group?.sportsMarketType || sportsSlug || sportsTeam ||
      (/\b(moneyline|spread|over\/under|point spread|total points|win the game|wins the match|versus)\b/.test(raw) &&
       /\b(nba|wnba|nfl|mlb|nhl|ncaa|football|basketball|baseball|hockey|soccer|tennis|golf|ufc|mma|team|match|game)\b/.test(raw))) return "sports";
  if (/\b(politic|election|president|congress|senate|democrat|republican|parliament|governor|vote)\b/.test(raw)) return "politics";
  if (/\b(crypto|bitcoin|btc|ethereum|eth|solana|sol|dogecoin|token|defi)\b/.test(raw)) return "crypto";
  if (/\b(econom|federal reserve|inflation|gdp|interest rate|recession|jobs report)\b/.test(raw)) return "economics";
  if (/\b(tech|technology|artificial intelligence|apple|google|microsoft|nvidia)\b/.test(raw)) return "tech";
  if (/\b(weather|temperature|rainfall|snowfall|hurricane)\b/.test(raw)) return "weather";
  if (/\b(entertainment|celebrity|movie|music|culture|grammy|oscar)\b/.test(raw)) return "culture";
  return "other";
}
async function enrichConsensusMarkets(holderGroups, nowMs) {
  // Position records can carry stale/missing end dates. Resolve condition IDs
  // through Gamma before applying the 30-day UI window; keep undated active
  // markets visible rather than silently deleting the entire consensus panel.
  // Enrich every discovered same-side group; do not cap the market list
  // or let a popular category crowd other categories out.
  // Copy Signals are based on positions that are currently open, not on
  // whether the trader happened to add to the position in the last hour.
  const scan = holderGroups;
  console.log("Consensus enrichment: checking " + scan.length + " shared open-position groups.");
  // Look markets up by outcome token ID first. Position rows expose the
  // token as `asset`; this avoids empty condition-ID lookups when the
  // position endpoint uses a different condition identifier representation.
  const tokenIds = [...new Set(scan.flatMap(group => Array.isArray(group.clobTokenIds) ? group.clobTokenIds : []).map(String).filter(Boolean))];
  const conditionIds = [...new Set(scan.map(group => String(group.conditionId || "").toLowerCase()).filter(Boolean))];
  const lookupItems = [
    ...tokenIds.map(id => ({key: "clob_token_ids", id})),
    ...conditionIds.map(id => ({key: "condition_ids", id}))
  ];
  const chunks = [];
  for (let i = 0; i < lookupItems.length; i += 40) chunks.push(lookupItems.slice(i, i + 40));

  const pages = await mapLimit(chunks, CONCURRENCY, async items => {
    const params = new URLSearchParams();
    for (const item of items) params.append(item.key, item.id);
    try {
      return {rows: asArray(await getJson(`${GAMMA_API}/markets?${params}`)), ok: true};
    } catch (error) {
      console.warn("Consensus market metadata lookup failed:", error.message);
      return {rows: [], ok: false};
    }
  });

  const marketByCondition = new Map();
  for (const page of pages) for (const market of page.rows) {
    const id = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
    if (id) marketByCondition.set(id, market);
  }

  let closedRemoved = 0;
  let outsideWindowRemoved = 0;
  let unknownDateKept = 0;
  let staleDateCleared = 0;
  const enriched = [];
  for (const group of scan) {
    const market = marketByCondition.get(String(group.conditionId || "").toLowerCase());
    // Fail closed: a stale position row is not proof that a market is still
    // tradable. Require Gamma metadata and an explicit open status.
    if (!market) {
      unknownDateKept++;
      continue;
    }
    const isClosed = market.closed === true || String(market.closed).toLowerCase() === "true";
    const isExplicitlyOpen = market.closed === false || String(market.closed).toLowerCase() === "false";
    if (isClosed) {
      closedRemoved++;
      continue;
    }
    if (!isExplicitlyOpen || market.active === false || String(market.active).toLowerCase() === "false") {
      unknownDateKept++;
      continue;
    }

    let endDate = marketEndDate(market);
    let endMs = timestampMs(endDate);
    if (typeof endDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      endMs = Date.parse(endDate + "T23:59:59.999Z");
    }

    // Include active markets ending within the next 7 days, matching the dashboard filter.
    // Exclude overdue, undated, and long-dated markets rather than relabeling
    // them as active opportunities or substituting an event start date.
    if (endMs === null) {
      unknownDateKept++;
      continue;
    }
    if (endMs <= nowMs) {
      staleDateCleared++;
      continue;
    }
    if (endMs > nowMs + CONSENSUS_WINDOW_HOURS * 60 * 60 * 1000) {
      outsideWindowRemoved++;
      continue;
    }

    enriched.push({
      ...group,
      title: market?.question || market?.title || group.title,
      slug: market?.slug || group.slug || null,
      category: classifyMarketCategory(market || {}, group),
      endDate,
      marketClosed: false,
      marketMetadataVerified: true
    });
  }

  console.log("Consensus Gamma enrichment: " + JSON.stringify({
    scannedGroups: scan.length,
    conditionIds: conditionIds.length,
    tokenIds: tokenIds.length,
    metadataPages: pages.length,
    metadataReturned: marketByCondition.size,
    closedRemoved,
    outsideWindowRemoved,
    staleDateCleared,
    unknownDateKept,
    eligibleGroups: enriched.length
  }));
  // These groups were built from live OPEN positions; historical performance
  // is checked separately for every holder before a signal is published.
  return enriched;
}

function buildConsensus(holderGroups, candidates, records = {}, minimumResolvedMarkets = MIN_CONSENSUS_CLOSED_MARKETS) {
  const candidateMap = new Map(candidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  const qualifiedGroups = [];
  let rawGroupsWithTwoHolders = 0;
  let groupsWithTwoQualified = 0;
  const qualificationDiagnostics = {
    holdersEvaluated: 0,
    incompleteRecord: 0,
    fewerThanMinimumCloses: 0,
    belowMinimumWinRate: 0,
    qualifiedHolders: 0,
    recordSamples: []
  };

  for (const group of holderGroups) {
    const uniqueHolders = [...new Map(
      (Array.isArray(group.traders) ? group.traders : [])
        .filter(holder => String(holder.wallet || "").trim())
        .map(holder => [String(holder.wallet).trim().toLowerCase(), holder])
    ).values()];
    if (uniqueHolders.length < 2) continue;
    rawGroupsWithTwoHolders++;

    const traders = uniqueHolders.map(holder => {
      const wallet = String(holder.wallet || "").toLowerCase();
      const candidate = candidateMap.get(wallet) || {};
      const record = records[wallet] || records[candidate.wallet] || {};
      const wins = num(record.wins);
      const losses = num(record.losses);
      const resolvedMarkets = wins !== null && losses !== null
        ? wins + losses
        : num(record.resolvedMarkets);
      const winRate = wins !== null && losses !== null && resolvedMarkets > 0
        ? wins / resolvedMarkets * 100
        : num(record.winRate);
      // Require a complete rolling-30-day profitable-close record at 90%+.
      // No extra minimum number of closed markets is imposed.
      const entryPrice = num(holder.avgPrice);
      const entryShares = num(holder.size ?? holder.shares);
      const entryCost = num(holder.entryValue);
      const hasVerifiedEntryPrice =
        entryPrice !== null && entryPrice > 0 && entryPrice <= 1 &&
        entryShares !== null && entryShares > 0 &&
        entryCost !== null && entryCost > 0;
      const isQualified =
        record.recordComplete === true &&
        resolvedMarkets !== null &&
        resolvedMarkets >= minimumResolvedMarkets &&
        winRate !== null &&
        winRate >= MIN_CONSENSUS_WIN_RATE &&
        hasVerifiedEntryPrice;

      qualificationDiagnostics.holdersEvaluated++;
      if (record.recordComplete !== true) qualificationDiagnostics.incompleteRecord++;
      if (resolvedMarkets === null || resolvedMarkets < minimumResolvedMarkets) qualificationDiagnostics.fewerThanMinimumCloses++;
      if (winRate === null || winRate < MIN_CONSENSUS_WIN_RATE) qualificationDiagnostics.belowMinimumWinRate++;
      if (!hasVerifiedEntryPrice) qualificationDiagnostics.missingEntryPrice = (qualificationDiagnostics.missingEntryPrice || 0) + 1;
      if (isQualified) qualificationDiagnostics.qualifiedHolders++;
      qualificationDiagnostics.recordSamples.push({
        resolvedMarkets,
        winRate: winRate === null ? null : Math.round(winRate * 100) / 100,
        recordComplete: record.recordComplete === true,
        scannedPositions: num(record.scannedPositions)
      });

      return {
        ...holder,
        wallet,
        name: holder.name || candidate.name || "Trader",
        rank: candidate.sportsRank ?? candidate.rank ?? holder.rank ?? null,
        overallRank: candidate.rank ?? null,
        pnl: candidate.pnl ?? holder.pnl ?? null,
        volume: candidate.volume ?? holder.volume ?? null,
        wins,
        losses,
        winRate,
        resolvedMarkets,
        earlyExitPositions: num(record.earlyExitPositions),
        recordComplete: record.recordComplete === true,
        qualified: isQualified,
        recordBasis: record.recordComplete === true
          ? "Rolling 30-day realized profitable-close proxy; displayed as context, not a guarantee"
          : "Rolling 30-day record incomplete or unavailable; historical performance is unknown"
      };
    }).filter(trader => trader.qualified);

    // A consensus market is valid when at least two distinct holders
    // independently have a complete 30-day record and a 90%+ win rate.
    if (traders.length < 2) continue;
    groupsWithTwoQualified++;
    qualifiedGroups.push({
      ...group,
      traders,
      sameSide: traders.length,
      qualifiedTraders: traders.length,
      // Combined Value is the current mark-to-market value, not historical entry cost.
      totalValue: traders.reduce((sum, trader) => sum + (num(trader.currentValue) ?? 0), 0),
      totalEntry: traders.every(trader => {
        const entry = num(trader.entryValue);
        return entry !== null && entry > 0;
      }) ? traders.reduce((sum, trader) => sum + num(trader.entryValue), 0) : null
    });
  }

  const result = qualifiedGroups.map(group => {
    const avgRate = group.traders.reduce((sum,t) => sum + (num(t.winRate) || 0),0) / group.traders.length;
    const avgSample = group.traders.reduce((sum,t) => sum + Math.min(100, num(t.resolvedMarkets) || 0),0) / group.traders.length;
    // Heuristic strength score (not a win probability): 50% breadth,
    // 30% average profitable-close rate, 20% record sample size.
    const breadthScore = Math.min(group.sameSide,20) / 20 * 50;
    const accuracyScore = avgRate / 100 * 30;
    const sampleScore = Math.min(avgSample,100) / 100 * 20;
    const consensusScore = Math.round(breadthScore + accuracyScore + sampleScore);
    return {...group, consensusScore, signalTier: group.sameSide >= 10 ? "Strong consensus" : group.sameSide >= 5 ? "High consensus" : "2–4 traders"};
  }).sort((a, b) => b.consensusScore - a.consensusScore || b.sameSide - a.sameSide || b.totalValue - a.totalValue || (b.newestBuy || 0) - (a.newestBuy || 0));
  console.log("Consensus holder qualification diagnostics:", JSON.stringify(qualificationDiagnostics));
  console.log("Consensus qualification:", JSON.stringify({
    rawGroupsWithTwoHolders,
    groupsWithTwoQualified,
    publishedGroups: result.length,
    minimumWinRate: MIN_CONSENSUS_WIN_RATE,
    minimumResolvedMarkets,
    qualificationWindowDays: 30
  }));
  return result;
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
  const periodDays = String(period?.api || period || "").toUpperCase() === "MONTH" ? 30 : 7;
  const periodCutoffMs = nowMs - periodDays * 24 * 60 * 60 * 1000;
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
    fetchPositionCohort("CLOSED", 2),
    fetchPositionCohort("OPEN", 1),
    fetchPositionCohort("REDEEMABLE_LOST", 1)
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
    const q = new URLSearchParams();
    for (const id of ids) q.append("condition_ids", id);
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
    if (market.closed !== true && String(market.closed).toLowerCase() !== "true") {
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
    if (resolvedAt < periodCutoffMs || resolvedAt > nowMs) continue;

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
async function getPeriodVolume(wallet, period, nowMs) {
  const periodDays = String(period?.api || period || "").toUpperCase() === "MONTH" ? 30 : 7;
  const cutoffMs = nowMs - periodDays * 24 * 60 * 60 * 1000;
  const params = new URLSearchParams({
    user: wallet,
    start: String(Math.floor(cutoffMs / 1000)),
    end: String(Math.floor(nowMs / 1000))
  });
  const response = await getJson(`${API}/v2/user-volume?${params}`);
  const data = response?.data ?? response ?? {};
  return {
    volume: num(data.volume_usdc),
    volumeShares: num(data.volume),
    volumeTradeCount: num(data.trade_count),
    volumeBasis: `Polymarket Data API v2 user-volume volume_usdc over the last ${periodDays} UTC days`
  };
}

async function attachPeriodVolumes(traders, period, nowMs) {
  return await mapLimit(traders, CONCURRENCY, async trader => {
    try {
      const stats = await getPeriodVolume(String(trader.wallet || "").toLowerCase(), period, nowMs);
      return { ...trader, ...stats };
    } catch (error) {
      console.warn("Period USD volume unavailable for " + trader.wallet + ":", error.message);
      return { ...trader, volume: null, volumeShares: null, volumeTradeCount: null, volumeBasis: "Unavailable from Data API v2 user-volume" };
    }
  });
}

async function attachSettlementStats(traders, period, nowMs) {
  return await mapLimit(traders, 4, async trader => {
    try {
      const wallet = String(trader.wallet || "").toLowerCase();
      const cacheKey = String(period?.key || period?.api || period || "week") + "|" + wallet;
      let pending = settlementStatsCache.get(cacheKey);
      if (!pending) {
        pending = getSettlementStats(wallet, period, nowMs, 100);
        settlementStatsCache.set(cacheKey, pending);
      }
      const stats = await pending;
      return {...trader, ...stats};
    } catch (error) {
      console.warn("Settlement record unavailable for " + trader.wallet + ":", error.message);
      return {...trader, settlementWins: null, settlementLosses: null, earlyExitMarkets: null, earlyExitPositions: null, settlementRecordComplete: false, settlementPositionsChecked: 0};
    }
  });
}

function makeOutput(period, ranked, sportsRanked, activePositions, tradeRows, nowMs, consensusCandidates, records, leaderRecords, consensusGroups, settlementRecords, consensusRecords) {
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
    recordBasis: "Leaderboard ranking uses selected-period Polymarket leaderboard P&L. Profitable/unprofitable close counts are a period-scoped realized-P&L proxy, not verified settlement wins/losses; incomplete samples are withheld.",
    candidateCount: ranked.length,
    sportsLeaders: sportsRanked.slice(0, LEADER_COUNT),
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: true,
    recordWarning:
      `Leaderboard records use verified market settlement outcomes when complete; incomplete samples are withheld. Consensus qualification remains based on the selected-period profitable-close proxy and the configured 90% threshold.`,
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
    consensus: buildConsensus(consensusGroups, consensusCandidates || ranked, consensusRecords || {}, MIN_CONSENSUS_CLOSED_MARKETS)
  };
}

async function writeJsonUnderLimit(filename, output) {
  const hasLeaders = Array.isArray(output?.leaders) && output.leaders.length > 0;
  const hasConsensus = Array.isArray(output?.consensus) && output.consensus.length > 0;
  const allowEmptyConsensus = String(filename).startsWith("consensus-");
  if (!output || (!hasLeaders && !hasConsensus && !allowEmptyConsensus)) {
    throw new Error(`${filename}: refusing to write empty or missing leaderboard/consensus data`);
  }
  const compact = () => JSON.stringify(output);

  let json = compact();
  let bytes = Buffer.byteLength(json, "utf8");

  // Preserve every consensus market. Trim only optional leader detail if
  // needed; if the full consensus itself exceeds the limit, fail loudly
  // rather than silently publishing an incomplete market list.
  while (bytes > MAX_JSON_BYTES) {
    if (output.traderActivePositions && output.traderActiveTrades) {
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
    } else {
      throw new Error(`${filename} remains over the size limit after trimming all available sections`);
    }

    json = compact();
    bytes = Buffer.byteLength(json, "utf8");
  }

  const parsedCheck = JSON.parse(json);
  const parsedHasLeaders = Array.isArray(parsedCheck.leaders) && parsedCheck.leaders.length > 0;
  const parsedHasConsensus = Array.isArray(parsedCheck.consensus) && parsedCheck.consensus.length > 0;
  if (!parsedHasLeaders && !parsedHasConsensus && !allowEmptyConsensus) {
    throw new Error(`${filename}: serialized output failed the non-empty data check`);
  }
  const tempFilename = `${filename}.tmp`;
  await fs.writeFile(tempFilename, json, "utf8");
  await fs.rename(tempFilename, filename);

  console.log(
    `Wrote ${filename}: ${(bytes / 1024 / 1024).toFixed(2)} MB`
  );
}

async function buildPeriod(period, discoveredCandidates, activePositions, nowMs, consensusCandidates, fullRecordCache, consensusGroups) {
  // Keep the general leaderboard history scan bounded, but separately
  // verify every wallet that actually appears in a live, time-qualified
  // consensus group. This applies equally to weekly and monthly records.
  const candidates = discoveredCandidates;
  const rankingRecordCandidates = candidates.slice(0, RANKING_RECORD_LIMIT);
  const candidateByWallet = new Map(
    (Array.isArray(consensusCandidates) ? consensusCandidates : [])
      .map(trader => [String(trader.wallet || "").toLowerCase(), trader])
  );
  const consensusWallets = new Set(
    consensusGroups.flatMap(group => (Array.isArray(group.traders) ? group.traders : [])
      .map(holder => String(holder.wallet || "").toLowerCase())
      .filter(Boolean))
  );
  const recordCandidateMap = new Map(
    rankingRecordCandidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader])
  );
  for (const wallet of consensusWallets) {
    if (!recordCandidateMap.has(wallet)) {
      recordCandidateMap.set(wallet, candidateByWallet.get(wallet) || {wallet});
    }
  }
  const recordCandidates = [...recordCandidateMap.values()];
  console.log(`Discovered ${discoveredCandidates.length} ${period.key} candidates; scanning active positions across ${candidates.length} wallets, leaderboard records for ${rankingRecordCandidates.length}, and consensus-holder records for ${consensusWallets.size} (unique record scan: ${recordCandidates.length}).`);
  const records = await attachRecords(recordCandidates, period, nowMs, CLOSED_POSITION_SCAN_LIMIT, fullRecordCache);
  // Consensus holder records are scanned across the complete rolling 30-day window; leaderboard records remain separately capped.
  const consensusRecordCandidates = [...consensusWallets].map(wallet =>
    candidateByWallet.get(wallet) || {wallet}
  );
  const consensusRecords = await attachRecords(
    consensusRecordCandidates,
    CONSENSUS_RECORD_PERIOD,
    nowMs,
    null,
    fullRecordCache
  );
  console.log(period.key + ": loaded full available rolling 30-day closed-position history for " +
    consensusRecordCandidates.length + " consensus holders; history is displayed, not used as a publication gate.");
  console.log(`${period.key}: verifying settlement outcomes for the top ${rankingRecordCandidates.length} wallets...`);
  const ranked = rankCandidates(candidates, records);
  const sportsRanked = rankSportsCandidates(candidates, records);
  const leaders = ranked.slice(0, LEADER_COUNT);
  // Detailed settlement verification is only needed for the visible Top 100.
  // Scanning 1,000 wallets across three API cohorts twice per period delayed the entire publish.
  // Keep W-L based on the selected-period closed-position scan; don't replace
  // it with a second, lifetime-shaped settlement cohort scan.
  const settlementRecords = {};
  const consensusMinimumResolved = MIN_CONSENSUS_CLOSED_MARKETS;
  console.log(`${period.key}: Copy Signals require 2+ distinct same-side open holders, each with a complete rolling-30-day record and a 90%+ profitable-close rate; no extra trade-count minimum.`);
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
    settlementRecords,
    consensusRecords
  );

  // Count every discovered weekly wallet, not only Top 100 traders or shared-market holders.
  // Reuse cached records so each wallet is queried only once in this period.
  let qualificationCount = null;
  if (period.key === "week") {
    const allCandidateRecords = records;
    const completeRecordRows = Object.entries(allCandidateRecords)
      .filter(([, record]) => record?.recordComplete === true);
    const qualifies = (record, minimumTrades) => {
      const wins = Number(record.wins);
      const losses = Number(record.losses);
      const resolved = Number(record.resolvedMarkets);
      const rate = Number(record.winRate);
      return Number.isFinite(wins) && Number.isFinite(losses) &&
        Number.isFinite(resolved) && resolved === wins + losses &&
        resolved >= minimumTrades &&
        Number.isFinite(rate) && rate >= MIN_CONSENSUS_WIN_RATE;
    };
    const qualifiedWallets = completeRecordRows
      .filter(([, record]) => qualifies(record, MIN_TRADER_RESOLVED_MARKETS))
      .map(([wallet]) => wallet.toLowerCase());
    const qualifiedAtLeastFiveTrades = completeRecordRows
      .filter(([, record]) => qualifies(record, 5))
      .map(([wallet]) => wallet.toLowerCase());
    qualificationCount = {
      qualifiedTraderCount: qualifiedWallets.length,
      qualifiedTraderCountAtLeastFiveTrades: qualifiedAtLeastFiveTrades.length,
      minimumTradesForRequestedCount: 5,
      candidateWalletsScanned: recordCandidates.length,
      recordsReturned: Object.keys(allCandidateRecords).length,
      completeRecords: completeRecordRows.length,
      incompleteOrUnavailableRecords: recordCandidates.length - completeRecordRows.length,
      minimumWinRate: MIN_CONSENSUS_WIN_RATE,
      minimumResolvedMarkets: MIN_TRADER_RESOLVED_MARKETS,
      windowDays: 7,
      basis: "Unique wallets from the discovered public leaderboard candidate pool; complete closed-position records within the last 7 days; one net profitable/unprofitable result per market. Early exits count as closes. This is a sampled candidate-pool count, not a census of every Polymarket wallet."
    };
    output.qualificationCount = qualificationCount;
    console.log("Weekly all-candidate qualification count: " + JSON.stringify(qualificationCount));
  }
  // The public position API does not provide a complete, bounded settlement
  // history for every wallet. Keep the period-scoped close-profitability
  // counts computed by rankCandidates; never relabel them as settled W-L.
  // This avoids a second, slow settlement scan that mostly returns
  // incomplete records and hides every trader record.
  output.leaders = await attachPeriodVolumes(output.leaders, period, nowMs);
  output.leaders = output.leaders.map(trader => ({
    ...trader,
    recordVerified: false,
    pnlBasis: `Polymarket v2 leaderboard P&L for selected ${period.key} period`,
    recordBasis: trader.recordComplete === true
      ? `Complete selected-period closed-position scan; one profitable/unprofitable result per market using net realized P&L. This is a close-profitability statistic, not verified market-settlement W-L; early exits count.`
      : `Incomplete selected-period closed-position scan; profitable/unprofitable counts and rate withheld.`
  }));

  // Keep consensus independent of the Top 100 JSON size guard.
  const builtConsensusRows = Array.isArray(output.consensus) ? output.consensus : [];
  // Publish the independently qualified consensus groups; never discard them.
  const consensusRows = builtConsensusRows;
  console.log(`${period.key}: publishing ${consensusRows.length} qualified consensus markets.`);
  await writeJsonUnderLimit(`${period.key}.json`, output);

  // Keep consensus data independent from the Top 100 period tabs. These
  // files reflect the actual fetched candidate pool; label the exact discovered pool size and the smaller settlement-verified sample separately.
  await writeJsonUnderLimit(`consensus-${period.key}.json`, {
    generatedAt: new Date(nowMs).toISOString(),
    pool: period.key,
    qualificationWindowDays: 30,
    minimumWinRate: MIN_CONSENSUS_WIN_RATE,
    minimumResolvedMarkets: MIN_CONSENSUS_CLOSED_MARKETS,
    qualificationBasis: "A market is published when at least 2 distinct wallets are confirmed holding the same side in the live OPEN positions endpoint, and each has a complete rolling-30-day record with a 90%+ profitable-close rate; no extra trade-count minimum. No recent BUY is required.",
    minimumSameSideTraders: 2,
    candidateCount: ranked.length,
    discoveredCandidateCount: discoveredCandidates.length,
    settlementRecordCandidateCount: rankingRecordCandidates.length,
    consensusHolderRecordCandidateCount: consensusWallets.size,
    totalRecordCandidateCount: recordCandidates.length,
    scanDiagnostics: lastConsensusDiagnostics,
    candidatePoolBasis: "All unique wallets exposed by available public Data API v2 leaderboard cursors across overall, sports, politics, crypto, esports, culture, mentions, weather, economics, tech, and finance categories and DAY/WEEK/MONTH/ALL periods, sorted by PnL and volume. The public API does not expose a literal all-wallet directory, so this is the broadest discoverable leaderboard pool rather than a guaranteed census. Every available open-position page is scanned; final consensus includes only same-outcome groups with at least 2 independently qualified traders.",
    resolutionWindowHours: CONSENSUS_WINDOW_HOURS,
    recordBasis: "Active markets with at least 2 distinct same-outcome holders, each independently passing a complete rolling-30-day 90%+ profitable-close rate; no extra trade-count minimum. Current holdings are confirmed by the live OPEN positions endpoint. This is a research signal, not a proven profitable strategy.",
    qualificationCount,
    consensus: consensusRows
  });

  console.log(
    `${period.key}: ${leaders.length} leaders, ${consensusRows.length} consensus markets`
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
    ...weekCandidates,
    ...monthCandidates
  ];
  for (const trader of positionCandidates) {
    if (!unique.has(trader.wallet)) unique.set(trader.wallet, trader);
  }

  console.log(`Loading positions for ${unique.size} unique traders...`);
  const fullRecordCache = new Map();
  const consensusCandidatePool = [...unique.values()].slice(0, CONSENSUS_WALLET_LIMIT);
  console.log(`Consensus scan bounded to ${consensusCandidatePool.length.toLocaleString()} unique wallets per update to keep the scheduled job within runtime limits.`);
  const consensusScan = await getBroadConsensusMarkets(consensusCandidatePool, nowMs, {});
  const activePositions = consensusScan.activePositionsByWallet;
  const consensusGroups = await enrichConsensusMarkets(consensusScan.holderGroups, nowMs);
  console.log("Broad consensus groups after Gamma enrichment: " + consensusGroups.length);
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