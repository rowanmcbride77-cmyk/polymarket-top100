
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
// [run-scanner] Publish up to 1,000 ranked candidates so the UI can filter before selecting its Top 100.
// [run-scanner] Publish only multi-trader consensus where every holder passes the strict quality gate.
// [run-scanner] Expand candidate discovery and live-position overlap scan to 15,000 wallets.
// [run-scanner] Prioritize sports, crypto and esports while retaining broad category coverage.
const LEADER_COUNT = 1000;
const PAGE_SIZE = 50;
const MAX_CANDIDATES_PER_PERIOD = 20000;
const CATEGORY_CANDIDATE_QUOTAS = { sports: 10000, crypto: 3000, esports: 2000, overall: 2000, politics: 2000, culture: 1200, mentions: 1200, weather: 1200, economics: 1200, tech: 1200, finance: 700 };
const MAX_LEADERBOARD_PAGES_PER_STREAM = 30;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const RANKING_RECORD_LIMIT = 1000;
// Bound closed-position history requests. Records that hit this cap are marked
// incomplete and withheld rather than presented as complete win/loss stats.
const CLOSED_POSITION_SCAN_LIMIT = 1000;
// Keep the full 15,000-wallet live-position scan; optimize request throughput rather than shrinking coverage.
const CONSENSUS_WALLET_LIMIT = 10000; // Verify records for the highest-capital 10k unique consensus holders to keep the 20k discovery scan inside the workflow budget.
const MAX_CONSENSUS_POSITION_PAGES_PER_WALLET = 2;
const MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET = 5;
const LEADER_RECORD_LIMIT = RANKING_RECORD_LIMIT;
// Keep upcoming markets in the data so the UI can filter 24h / 3d / 7d accurately.
const CONSENSUS_WINDOW_HOURS = 48; // Sports markets may remain eligible through the estimated finish of games starting within 48 hours.
const CONSENSUS_HOLDERS_PER_MARKET = 1000;
const MIN_CONSENSUS_DISCOVERY_VALUE = 3; // Discovery feeds Developing/Watchlist; Strong Signals retain the $10 gate.
const MIN_CONSENSUS_POSITION_VALUE = 10;
const MIN_CONSENSUS_ENTRY_VALUE = 10; // Exclude tiny/dust entries when verified cost basis is available.
const CONSENSUS_ENTRY_WINDOW_MINUTES = 90;
const MIN_TRADER_RESOLVED_MARKETS = 10;
const MIN_CONSENSUS_CLOSED_MARKETS = 10; // Require at least 10 resolved markets in the complete rolling 7-day record.
const CONSENSUS_RECORD_PERIOD = {key: "week", api: "WEEK"};
const TRADERS_PER_MARKET = 500;
const TRADE_LIMIT = 10;
const MAX_JSON_BYTES = 20 * 1024 * 1024;
const CONCURRENCY = 20;
const DELAY_MS = 0;
// Sports-category wallets receive deeper position pagination; Gamma sports metadata drives a balanced market-first sweep across major leagues and props.
let lastConsensusDiagnostics = null;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Shared request pacing: 20 requests/second while allowing concurrent responses.
// Retry-After globally pauses workers if the API pushes back.
const API_MIN_INTERVAL_MS = 50;
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

  // Consensus eligibility uses the selected rolling 7-day period. Capped or incomplete samples are withheld.
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
      ? `Complete rolling-period closed-position scan; one profitable/unprofitable result per market using net realized P&L. This is a close-profitability proxy, not verified settlement accuracy; early exits count.`
      : `INCOMPLETE rolling-period sample of closed positions; market-level profitable/unprofitable counts are withheld. This is a close-profitability proxy, not verified settlement accuracy.`
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
  const categories = ["sports","crypto","esports","overall","politics","culture","mentions","weather","economics","tech","finance"];
  const timePeriods = [...new Set([period.api,"WEEK","ALL"])];
  let pagesFetched = 0;
  for (const category of categories) {
    if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD) break;
    const categoryStartSize = byWallet.size;
    const categoryQuota = CATEGORY_CANDIDATE_QUOTAS[category] ?? 500;
    for (const timePeriod of timePeriods) {
      if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD || byWallet.size - categoryStartSize >= categoryQuota) break;
      for (const orderBy of ["PNL","VOLUME"]) {
        if (byWallet.size >= MAX_CANDIDATES_PER_PERIOD || byWallet.size - categoryStartSize >= categoryQuota) break;
        let cursor = null, page = 0;
        const seenCursors = new Set();
        while (page < MAX_LEADERBOARD_PAGES_PER_STREAM && byWallet.size < MAX_CANDIDATES_PER_PERIOD && byWallet.size - categoryStartSize < categoryQuota) {
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
            } else {
              trader.sourceCategories = [category];
              byWallet.set(wallet,trader);
            }
            if (existing && !Array.isArray(existing.sourceCategories)) existing.sourceCategories = [];
            if (existing && !existing.sourceCategories.includes(category)) existing.sourceCategories.push(category);
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
  console.log(`${period.key}: discovered ${candidates.length.toLocaleString()} unique traders across ${pagesFetched.toLocaleString()} public leaderboard pages; discovery cap is ${MAX_CANDIDATES_PER_PERIOD.toLocaleString()} wallets.`);
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
    filter_amount: String(MIN_CONSENSUS_DISCOVERY_VALUE)
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
    if (!isLiveConsensusPosition(position)) continue;

    const wallet = String(position.proxy_wallet ?? position.proxyWallet ?? position.user ?? "").toLowerCase();
    const side = String(position.outcome ?? position.outcome_name ?? "").trim();
    const value = num(position.current_value ?? position.currentValue);
    if (!wallet || !side || value === null || value < MIN_CONSENSUS_DISCOVERY_VALUE) continue;

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
  // Return the explicit Gamma market/event end field. For sports, Gamma's
  // endDate can be the scheduled kickoff rather than the end of play.
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

function consensusTiming(market) {
  const rawEnd = marketEndDate(market);
  if (!isSportsMarket(market)) return {endDate: rawEnd, endTimeBasis: "market_end"};
  const events = Array.isArray(market.events) ? market.events : [];
  const starts = [
    market.gameStartTime, market.game_start_time, market.eventStartTime,
    market.event_start_time, market.startDate, market.start_date,
    ...events.flatMap(event => [
      event?.gameStartTime, event?.game_start_time, event?.eventStartTime,
      event?.event_start_time, event?.startDate, event?.start_date, event?.startTime
    ])
  ];
  let kickoffMs = null;
  for (const value of starts) {
    const parsed = timestampMs(value);
    if (parsed !== null) { kickoffMs = parsed; break; }
  }
  // In the observed NFL/NBA-style Gamma records, endDate is the scheduled
  // kickoff. When a separate start field is unavailable, use that timestamp
  // as kickoff and allow four hours for play to finish. This keeps live games
  // visible after kickoff without treating them as already resolved.
  if (kickoffMs === null) kickoffMs = timestampMs(rawEnd);
  if (kickoffMs === null) return {endDate: rawEnd, endTimeBasis: "market_end"};
  const estimatedFinish = new Date(kickoffMs + 4 * 60 * 60 * 1000).toISOString();
  return {
    endDate: estimatedFinish,
    gameStartTime: new Date(kickoffMs).toISOString(),
    endTimeBasis: "estimated_game_finish"
  };
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
  const playerProp = /\b(over|under)\s+\d+(?:\.\d+)?\s+(?:points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|steals?|blocks?|yards?|receptions?|touchdowns?|strikeouts?|hits?|runs?|goals?|shots?|saves?|aces?)\b/i.test(metadata) || /\b\d+(?:\.\d+)?\+\s+(?:points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|yards?|receptions?|touchdowns?|strikeouts?|hits?|goals?)\b/i.test(metadata);
  return playerProp || keywords.some(keyword => metadata.includes(keyword));
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
  if (currentValue === null || currentValue < MIN_CONSENSUS_DISCOVERY_VALUE) return false;
  if (position.closed === true || position.redeemable === true || position.mergeable === true) return false;
  const status = String(position.status ?? "").toUpperCase();
  return !status || status === "OPEN" || status === "ACTIVE";
}

async function getBroadConsensusMarkets(knownTraders, nowMs, activePositions) {
  // Discover open holdings from a broad leaderboard-derived wallet pool, with a hard page cap.
  const consensusPositions = await mapLimit(knownTraders, CONCURRENCY, async trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    if (!wallet) return [wallet, []];
    const isSportsCandidate = Array.isArray(trader.sourceCategories) && trader.sourceCategories.includes("sports");
    const walletPageCap = isSportsCandidate ? MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET : MAX_CONSENSUS_POSITION_PAGES_PER_WALLET;
    try {
      const all = [];
      const seen = new Set();
      let cursor = null;
      let pages = 0;
      let hasMore = false;
      const seenCursors = new Set();
      while (pages < walletPageCap) {
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
      return [wallet, all, hasMore, pages, false, isSportsCandidate];
    } catch (error) {
      console.error("Consensus positions fetch failed for " + wallet + ": " + error.message);
      // Mark the wallet as failed. The scan must not publish a partial consensus
      // dataset as if every wallet had been checked successfully.
      return [wallet, Array.isArray(activePositions?.[wallet]) ? activePositions[wallet] : [], false, 0, true, isSportsCandidate];
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
  let sportsWalletsScanned = 0;
  let sportsPositionPagesFetched = 0;

  for (const [wallet, positions, hasMore, pages, failed, isSportsCandidate] of consensusPositions) {
    if (isSportsCandidate) { sportsWalletsScanned++; sportsPositionPagesFetched += pages || 0; }
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
      const rawInvested = num(position.initialValue ?? position.initial_value);
      const avgPrice = num(position.avgPrice ?? position.avg_price ?? position.averagePrice ?? position.average_price);
      const invested = rawInvested !== null && rawInvested > 0
        ? rawInvested
        : (avgPrice !== null && avgPrice > 0 && avgPrice <= 1 && size > 0 ? avgPrice * size : null);
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
      if (invested !== null && invested > 0) group.totalEntry += invested;
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
        amount: invested > 0 ? invested : null,
        entryValue: invested > 0 ? invested : null,
        avgPrice: position.avgPrice !== null && position.avgPrice > 0 && position.avgPrice <= 1
          ? position.avgPrice
          : null,
        shares: size > 0 ? size : null,
        size: size > 0 ? size : null,
        currentPrice: position.currentPrice ?? null,
        currentValue,
        buyTime: lastEventAt
      });
    }
  }

  // Evaluate each outcome independently. A Yes consensus is not cancelled
  // by traders holding No; if both sides independently have 2+ holders, show
  // both directional groups transparently rather than silently dropping both.
  const holderGroups = [...groups.values()]
    .filter(group => group.sameSide >= 2)
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
    allWalletPositionPageCap: MAX_CONSENSUS_POSITION_PAGES_PER_WALLET, 
    nbaSportsWalletTarget: CATEGORY_CANDIDATE_QUOTAS.sports,
    nbaSportsPositionPageCap: MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET,
    sportsWalletsScanned,
    sportsPositionPagesFetched,
    sportsPageCapPerWallet: MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET,
    note: `Consensus discovery scans up to ${MAX_CONSENSUS_POSITION_PAGES_PER_WALLET * 100} positions for every selected wallet and up to ${MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET * 100} for sports-category wallets, within the 15,000-wallet cap. This deliberately scans beyond the first 100 highest-value positions for non-sports wallets so player props held by cross-category traders are not missed. Active holdings worth $3+ are eligible for Developing/Watchlist; Strong Signals retain the $10 entry/current-value gate. Exact market condition and outcome are grouped separately. Sports may remain eligible for 48 hours; other categories use 24 hours.`
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
  const playerProp = /\b(over|under)\s+\d+(?:\.\d+)?\s+(?:points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|steals?|blocks?|yards?|receptions?|touchdowns?|strikeouts?|hits?|runs?|goals?|shots?|saves?|aces?)\b/i.test(raw) || /\b\d+(?:\.\d+)?\+\s+(?:points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|yards?|receptions?|touchdowns?|strikeouts?|hits?|goals?)\b/i.test(raw);
  // Many active sports positions omit an explicit league/category field.
  // Their market question or slug still contains the participating team.
  const sportsTeam = /\b(lakers|celtics|warriors|knicks|nets|bulls|heat|bucks|sixers|76ers|cavaliers|cavs|clippers|suns|nuggets|mavericks|mavs|rockets|spurs|grizzlies|thunder|timberwolves|pelicans|kings|trail blazers|blazers|jazz|hawks|magic|hornets|pistons|raptors|wizards|chiefs|eagles|cowboys|49ers|niners|ravens|bills|bengals|steelers|packers|lions|vikings|bears|commanders|giants|jets|patriots|dolphins|broncos|chargers|raiders|seahawks|rams|cardinals|falcons|panthers|saints|buccaneers|bucs|titans|colts|texans|jaguars|browns|orioles|yankees|red sox|blue jays|rays|astros|mariners|rangers|dodgers|padres|giants|mets|phillies|braves|cubs|cardinals|brewers|twins|guardians|tigers|royals|athletics|angels|white sox|reds|pirates|marlins|nationals|rockies|maple leafs|bruins|rangers|penguins|capitals|lightning|panthers|oilers|canucks|flames|avalanche|golden knights|stars|wild|jets|predators|hurricanes|islanders|devils|sabres|red wings|senators|ducks|sharks|kings|kraken|blues|blackhawks|canadiens|flyers|blue jackets)\b/.test(raw);
  if (market?.gameId || market?.game_id || market?.gameStartTime || market?.game_start_time ||
      group?.gameId || group?.gameStartTime || group?.sportsMarketType || sportsSlug || sportsTeam || playerProp ||
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
async function getDirectSportsConsensusGroups(nowMs, knownNames = new Map()) {
  // Discover sports from Gamma metadata instead of hard-coding NBA tag 745.
  let metadata = [];
  try { metadata = asArray(await getJson(`${GAMMA_API}/sports`)); }
  catch (error) { console.warn("Gamma sports metadata unavailable:", error.message); return []; }

  const families = [
    ["NFL", /^(nfl|american.?football|football)$/i],
    ["NBA", /^(nba|basketball)$/i], ["WNBA", /^wnba$/i],
    ["MLB", /^(mlb|baseball)$/i], ["NHL", /^(nhl|hockey|ice.?hockey)$/i],
    ["NCAAF", /^(ncaaf|cfb|college.?football)$/i],
    ["NCAAB", /^(ncaab|cbb|college.?basketball)$/i],
    ["Soccer", /(soccer|epl|premier|champions|liga|bundesliga|serie.?a|la.?liga|ligue)/i],
    ["Tennis", /(tennis|atp|wta)/i], ["MMA", /(ufc|mma|boxing)/i],
    ["Golf", /(golf|pga|lpga)/i], ["Cricket", /cricket/i],
    ["Rugby", /rugby/i], ["Esports", /(esport|counter.?strike|dota|league.?of.?legends|valorant)/i]
  ];
  const tagsByFamily = new Map();
  for (const row of metadata) {
    const label = String(row.sport || row.league || row.name || "").trim();
    const family = families.find(([, pattern]) => pattern.test(label));
    if (!family) continue;
    const rawTags = Array.isArray(row.tags) ? row.tags : String(row.tags || "").split(",");
    const ids = rawTags.map(value => String(typeof value === "object" ? (value.id ?? value.tag_id ?? "") : value).trim())
      .filter(value => /^\d+$/.test(value));
    if (!tagsByFamily.has(family[0])) tagsByFamily.set(family[0], new Set());
    for (const id of ids) tagsByFamily.get(family[0]).add(id);
  }

  const marketByCondition = new Map(), familyCounts = {};
  // Bound each league's sweep so one large NBA slate cannot crowd out other sports.
  for (const [family, tagSet] of tagsByFamily) {
    let familyMarkets = 0;
    for (const tagId of [...tagSet].slice(0, 4)) {
      const params = new URLSearchParams({tag_id:tagId,active:"true",closed:"false",limit:"1000",order:"endDate",ascending:"true"});
      try {
        const rows = asArray(await getJson(`${GAMMA_API}/markets?${params}`));
        for (const raw of rows) {
          const market = {...raw,tags:[...(Array.isArray(raw.tags)?raw.tags:[]),family]};
          const conditionId = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
          if (!conditionId || market.closed === true || String(market.closed).toLowerCase() === "true" ||
              market.active === false || String(market.active).toLowerCase() === "false" || !isSportsMarket(market)) continue;
          const timing = consensusTiming(market);
          let endMs = timestampMs(timing.endDate);
          if (typeof timing.endDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(timing.endDate)) endMs = Date.parse(timing.endDate + "T23:59:59.999Z");
          if (endMs === null || endMs <= nowMs || endMs > nowMs + CONSENSUS_WINDOW_HOURS * 60 * 60 * 1000) continue;
          if (!marketByCondition.has(conditionId)) {
            marketByCondition.set(conditionId,{...market,conditionId,category:"sports",sport:family,
              title:market.question || market.title || `${family} market`,endDate:timing.endDate,
              gameStartTime:timing.gameStartTime || market.gameStartTime || market.game_start_time || null});
            familyMarkets++;
          }
          if (familyMarkets >= 35) break;
        }
      } catch (error) { console.warn(`Direct ${family} discovery failed for tag ${tagId}:`,error.message); }
      if (familyMarkets >= 35) break;
    }
    familyCounts[family] = familyMarkets;
  }

  const eligible = [...marketByCondition.values()];
  const groupsByMarket = await mapLimit(eligible,12,market =>
    getMarketConsensus(market,knownNames).then(groups => groups.map(group => ({
      ...group,title:market.title,slug:market.slug || null,
      eventSlug:market.eventSlug || market.event_slug || null,
      eventTitle:market.eventTitle || market.event_title || null,
      category:"sports",sport:market.sport,
      sportsMarketType:market.sportsMarketType || market.sports_market_type || null,
      gameStartTime:market.gameStartTime || null,endDate:market.endDate,
      marketClosed:false,marketMetadataVerified:true
    })))
  );
  const found = groupsByMarket.flat();
  console.log("Direct multi-sport market discovery:",JSON.stringify({
    sportFamilies:Object.keys(familyCounts),eligibleMarketsBySport:familyCounts,
    eligibleMarkets:eligible.length,sameSideConsensusGroups:found.length,
    playerPropGroups:found.filter(group => /\b(points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|steals?|blocks?|pra|yards?|receptions?|touchdowns?|strikeouts?|hits?|goals?)\b/i.test(String(group.title || ""))).length
  }));
  return found;
}

async function enrichConsensusMarkets(holderGroups, nowMs) {
  // Position records can carry stale/missing end dates. Resolve condition IDs
  // through Gamma before applying the strict 24-hour resolution window; keep undated active
  // markets visible rather than silently deleting the entire consensus panel.
  // Enrich every discovered same-side group; do not cap the market list
  // or let a popular category crowd other categories out.
  // Consensus Markets are based on positions that are currently open, not on
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

    const timing = consensusTiming(market);
    const endDate = timing.endDate;
    let endMs = timestampMs(endDate);
    if (typeof endDate === "string" && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      endMs = Date.parse(endDate + "T23:59:59.999Z");
    }

    // For sports, endDate is the estimated finish (kickoff + 4 hours) when
    // Gamma exposes kickoff in its endDate field. Other categories use the
    // explicit market end/resolution timestamp. Keep the same strict 24h cap.
    if (endMs === null) {
      unknownDateKept++;
      continue;
    }
    if (endMs <= nowMs) {
      staleDateCleared++;
      continue;
    }
    const marketWindowHours = isSportsMarket(market || group) ? CONSENSUS_WINDOW_HOURS : 24;
    if (endMs > nowMs + marketWindowHours * 60 * 60 * 1000) {
      outsideWindowRemoved++;
      continue;
    }

    const category = classifyMarketCategory(market || {}, group);
    // Keep every verified category. Category filtering belongs in presentation,
    // never in discovery; the 24-hour window and trader-quality gates remain strict.

    enriched.push({
      ...group,
      title: market?.question || market?.title || group.title,
      slug: market?.slug || group.slug || null,
      category,
      endDate,
      ...(timing.gameStartTime ? {gameStartTime: timing.gameStartTime} : {}),
      endTimeBasis: timing.endTimeBasis,
      marketClosed: false,
      marketMetadataVerified: true
    });
  }

  // Expand each discovered, upcoming market from the market side as well as the
  // leaderboard-wallet side. This finds holders outside the 15k sampled wallet pool
  // and prevents consensus counts from being limited to leaderboard candidates.
  const marketLookup = new Map();
  const knownNames = new Map();
  for (const group of enriched) {
    const condition = String(group.conditionId || "").toLowerCase();
    if (!condition) continue;
    if (!marketLookup.has(condition)) {
      marketLookup.set(condition, {
        conditionId: condition,
        question: group.title,
        title: group.title,
        endDate: group.endDate,
        events: group.gameStartTime ? [{startDate: group.gameStartTime}] : [],
        category: group.category,
        sport: group.sport,
        sportsMarketType: group.sportsMarketType,
        tags: group.tags || []
      });
    }
    for (const trader of group.traders || []) {
      const wallet = String(trader.wallet || "").toLowerCase();
      if (wallet && !knownNames.has(wallet)) knownNames.set(wallet, trader);
    }
  }
  const marketHolderRows = await mapLimit(
    [...marketLookup.values()],
    10,
    async market => {
      try {
        return await getMarketConsensus(market, knownNames);
      } catch (error) {
        console.warn("Market-first holder expansion failed for " + market.conditionId + ": " + error.message);
        return [];
      }
    }
  );
  const expandedGroups = new Map(
    enriched.map(group => [String(group.conditionId || "").toLowerCase() + "|" + String(group.side || "").toLowerCase(), group])
  );
  for (const directGroup of marketHolderRows.flat()) {
    const condition = String(directGroup.conditionId || "").toLowerCase();
    const key = condition + "|" + String(directGroup.side || "").toLowerCase();
    const source = marketLookup.get(condition);
    const prior = expandedGroups.get(key);
    const traders = new Map();
    for (const trader of [...(prior?.traders || []), ...(directGroup.traders || [])]) {
      const wallet = String(trader.wallet || "").toLowerCase();
      if (wallet && !traders.has(wallet)) traders.set(wallet, trader);
    }
    const mergedTraders = [...traders.values()].slice(0, TRADERS_PER_MARKET);
    if (mergedTraders.length < 2) continue;
    expandedGroups.set(key, {
      ...(source || {}),
      ...(prior || {}),
      ...directGroup,
      category: prior?.category || source?.category || classifyMarketCategory(source || {}, directGroup),
      title: prior?.title || source?.title || directGroup.title,
      endDate: prior?.endDate || source?.endDate || directGroup.endDate,
      marketClosed: false,
      marketMetadataVerified: true,
      traders: mergedTraders,
      sameSide: mergedTraders.length,
      totalValue: mergedTraders.reduce((sum, trader) => sum + (num(trader.currentValue) || 0), 0),
      newestBuy: mergedTraders.reduce((latest, trader) => Math.max(latest, num(trader.buyTime) || 0), 0) || null,
      oldestBuy: mergedTraders.reduce((earliest, trader) => {
        const time = num(trader.buyTime);
        return time === null ? earliest : (earliest === null ? time : Math.min(earliest, time));
      }, null)
    });
  }
  const fullyExpanded = [...expandedGroups.values()];
  const directSportsGroups = await getDirectSportsConsensusGroups(nowMs, knownNames);
  for (const directGroup of directSportsGroups) {
    const condition = String(directGroup.conditionId || "").toLowerCase();
    const key = condition + "|" + String(directGroup.side || "").toLowerCase();
    const prior = expandedGroups.get(key);
    const merged = new Map();
    for (const trader of [...(prior?.traders || []), ...(directGroup.traders || [])]) {
      const wallet = String(trader.wallet || "").toLowerCase();
      if (wallet && !merged.has(wallet)) merged.set(wallet, trader);
    }
    const traders = [...merged.values()].slice(0, TRADERS_PER_MARKET);
    if (traders.length < 2) continue;
    expandedGroups.set(key, {
      ...(prior || {}),
      ...directGroup,
      category: "sports",
      sport: directGroup.sport || "sports",
      marketClosed: false,
      marketMetadataVerified: true,
      traders,
      sameSide: traders.length,
      totalValue: traders.reduce((sum, trader) => sum + (num(trader.currentValue) || 0), 0)
    });
  }
  const allExpanded = [...expandedGroups.values()];
  console.log("Market-first holder expansion: " + JSON.stringify({
    eligibleMarkets: marketLookup.size,
    holderLookups: marketHolderRows.length,
    expandedSameSideGroups: allExpanded.length - enriched.length,
    directSportsGroups: directSportsGroups.length
  }));
  enriched.splice(0, enriched.length, ...allExpanded);

  const categoryCounts = enriched.reduce((counts, group) => { counts[group.category] = (counts[group.category] || 0) + 1; return counts; }, {});
  console.log("Consensus Gamma enrichment: " + JSON.stringify({
    scannedGroups: scan.length,
    categoryCounts,
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
  const strongGroups = [], developingGroups = [], watchlistGroups = [];
  const diagnostics = {
    rawGroupsWithTwoHolders: 0, holdersEvaluated: 0, incompleteRecord: 0,
    fewerThanMinimumCloses: 0, belowMinimumWinRate: 0, nonPositiveWeeklyPnl: 0,
    missingVerifiedEntry: 0, strongHolders: 0, developingHolders: 0
  };

  for (const group of holderGroups) {
    const uniqueHolders = [...new Map(
      (Array.isArray(group.traders) ? group.traders : [])
        .filter(holder => String(holder.wallet || "").trim())
        .map(holder => [String(holder.wallet).trim().toLowerCase(), holder])
    ).values()];
    if (uniqueHolders.length < 2) continue;
    diagnostics.rawGroupsWithTwoHolders++;

    const holders = uniqueHolders.map(holder => {
      const wallet = String(holder.wallet || "").toLowerCase();
      const candidate = candidateMap.get(wallet) || {};
      const record = records[wallet] || records[candidate.wallet] || {};
      const wins = num(record.wins), losses = num(record.losses);
      const resolvedMarkets = wins !== null && losses !== null ? wins + losses : num(record.resolvedMarkets);
      const winRate = wins !== null && losses !== null && resolvedMarkets > 0
        ? wins / resolvedMarkets * 100 : num(record.winRate);
      const entryPrice = num(holder.avgPrice), entryShares = num(holder.size ?? holder.shares);
      const entryCost = num(holder.entryValue), currentValue = num(holder.currentValue);
      const hasVerifiedEntryPrice = entryPrice !== null && entryPrice > 0 && entryPrice <= 1 &&
        entryShares !== null && entryShares > 0 && entryCost !== null && entryCost > 0;
      const hasStrongEntry = hasVerifiedEntryPrice && entryCost >= MIN_CONSENSUS_ENTRY_VALUE &&
        currentValue !== null && currentValue >= MIN_CONSENSUS_POSITION_VALUE;
      const hasDevelopingEntry = currentValue !== null && currentValue >= MIN_CONSENSUS_DISCOVERY_VALUE;
      const periodPnl = num(candidate.pnl ?? holder.pnl);
      const complete = record.recordComplete === true;
      const strongQualified = periodPnl !== null && periodPnl > 0 && complete &&
        resolvedMarkets !== null && resolvedMarkets >= minimumResolvedMarkets &&
        winRate !== null && winRate >= MIN_CONSENSUS_WIN_RATE && hasStrongEntry;
      const developingQualified = periodPnl !== null && periodPnl > 0 && complete &&
        resolvedMarkets !== null && resolvedMarkets >= 5 && winRate !== null &&
        winRate >= 75 && hasDevelopingEntry;

      diagnostics.holdersEvaluated++;
      if (!complete) diagnostics.incompleteRecord++;
      if (resolvedMarkets === null || resolvedMarkets < minimumResolvedMarkets) diagnostics.fewerThanMinimumCloses++;
      if (winRate === null || winRate < MIN_CONSENSUS_WIN_RATE) diagnostics.belowMinimumWinRate++;
      if (periodPnl === null || periodPnl <= 0) diagnostics.nonPositiveWeeklyPnl++;
      if (!hasStrongEntry) diagnostics.missingVerifiedEntry++;
      if (strongQualified) diagnostics.strongHolders++;
      if (developingQualified) diagnostics.developingHolders++;

      return {
        ...holder, wallet, name: holder.name || candidate.name || "Trader",
        rank: candidate.sportsRank ?? candidate.rank ?? holder.rank ?? null,
        overallRank: candidate.rank ?? null, pnl: periodPnl,
        volume: candidate.volume ?? holder.volume ?? null, wins, losses, winRate,
        resolvedMarkets, recordComplete: complete, qualified: strongQualified,
        strongQualified, developingQualified, entryVerified: hasVerifiedEntryPrice,
        recordStatus: !complete ? "Incomplete / unavailable" :
          (resolvedMarkets === null || resolvedMarkets < minimumResolvedMarkets ? "Fewer than 10 closes" :
            (winRate !== null && winRate < MIN_CONSENSUS_WIN_RATE ? "Below 90% strong-signal threshold" : "Meets historical record threshold")),
        recordBasis: complete
          ? "Rolling 7-day realized profitable-close proxy; early exits count and this is not verified settlement accuracy"
          : "Rolling 7-day record incomplete or unavailable"
      };
    });

    const strongHolders = holders.filter(holder => holder.strongQualified);
    const developingHolders = holders.filter(holder => holder.developingQualified);
    if (strongHolders.length < 2) continue;
    const tier = "Strong Signals";
    const displayTraders = strongHolders;
    const tierReason = "Every listed trader independently passes the complete 7-day record, 10+ closes, 90%+ profitable-close rate, positive weekly P&L, and verified $10+ position gate.";

    const knownRates = displayTraders.map(trader => num(trader.winRate)).filter(rate => rate !== null);
    const avgRate = knownRates.length ? knownRates.reduce((sum, rate) => sum + rate, 0) / knownRates.length : 0;
    const entries = displayTraders.map(trader => num(trader.entryValue)).filter(value => value !== null && value > 0);
    const totalEntry = entries.length ? entries.reduce((sum, value) => sum + value, 0) : null;
    const totalValue = displayTraders.reduce((sum, trader) => sum + (num(trader.currentValue) ?? 0), 0);
    const breadthScore = Math.min(displayTraders.length, 20) / 20 * 55;
    const capitalScore = Math.min(Math.log10(Math.max(1, totalEntry || 0) + 1) / Math.log10(10001), 1) * 25;
    const qualityScore = knownRates.length ? avgRate / 100 * 10 : 0;
    const result = {
      ...group, traders: displayTraders, sameSide: displayTraders.length,
      qualifiedTraders: strongHolders.length, developingTraders: developingHolders.length,
      strongHolderCount: strongHolders.length, signalTier: tier, tierReason,
      consensusScore: Math.round(breadthScore + capitalScore + qualityScore),
      totalValue, totalEntry
    };
    if (tier === "Strong Signals") strongGroups.push(result);
    else if (tier === "Developing Signals") developingGroups.push(result);
    else watchlistGroups.push(result);
  }

  const sortGroups = (a, b) =>
    (b.consensusScore ?? 0) - (a.consensusScore ?? 0) ||
    b.sameSide - a.sameSide ||
    (b.totalValue ?? 0) - (a.totalValue ?? 0) ||
    (b.newestBuy || 0) - (a.newestBuy || 0);
  strongGroups.sort(sortGroups);
  developingGroups.sort(sortGroups);
  watchlistGroups.sort(sortGroups);

  const perTierLimit = 1500;
  const result = strongGroups.slice(0, perTierLimit);
  console.log("Consensus tier diagnostics:", JSON.stringify({
    ...diagnostics,
    strongMarkets: strongGroups.length, developingMarkets: developingGroups.length,
    watchlistMarkets: watchlistGroups.length,
    publishedStrong: Math.min(strongGroups.length, perTierLimit),
    publishedDeveloping: Math.min(developingGroups.length, perTierLimit),
    publishedWatchlist: Math.min(watchlistGroups.length, perTierLimit),
    minimumStrongWinRate: MIN_CONSENSUS_WIN_RATE,
    minimumStrongResolvedMarkets: minimumResolvedMarkets,
    developingWinRate: 75, developingMinimumResolvedMarkets: 5,
    discoveryPositionValue: MIN_CONSENSUS_DISCOVERY_VALUE
  }));
  // Never publish both sides of the same exact market condition as copy signals.
  // If at least two distinct wallets support two or more different outcomes,
  // treat the condition as conflicted and suppress every side for that condition.
  const outcomesByCondition = new Map();
  for (const market of result) {
    const condition = String(market.conditionId || "").trim().toLowerCase();
    const outcome = String(market.side || "").trim().toLowerCase();
    if (!condition || !outcome) continue;
    if (!outcomesByCondition.has(condition)) outcomesByCondition.set(condition, new Map());
    const byOutcome = outcomesByCondition.get(condition);
    byOutcome.set(outcome, Math.max(byOutcome.get(outcome) || 0, Number(market.sameSide) || 0));
  }
  const conflictedConditions = new Set(
    [...outcomesByCondition.entries()]
      .filter(([, outcomes]) => [...outcomes.values()].filter(count => count >= 2).length >= 2)
      .map(([condition]) => condition)
  );
  const filteredResult = result.filter(market =>
    !conflictedConditions.has(String(market.conditionId || "").trim().toLowerCase())
  );
  console.log("Conflicting outcome filter:", JSON.stringify({
    conflictedConditions: conflictedConditions.size,
    suppressedSignals: result.length - filteredResult.length
  }));
  return filteredResult;
}
// [run-scanner] Conflict suppression and $3 holder discovery are part of this build.
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
  // Historical records enrich signals but no longer gate publication. Bound
  // this optional enrichment so broad overlap discovery does not time out.
  const walletCapital = new Map();
  for (const group of consensusGroups) {
    for (const holder of (Array.isArray(group.traders) ? group.traders : [])) {
      const wallet = String(holder.wallet || "").toLowerCase();
      if (!wallet) continue;
      walletCapital.set(wallet, (walletCapital.get(wallet) || 0) + (num(holder.entryValue) ?? 0));
    }
  }
  const consensusRecordWallets = [...consensusWallets]
    .sort((a,b) => (walletCapital.get(b) || 0) - (walletCapital.get(a) || 0))
    .slice(0, CONSENSUS_WALLET_LIMIT);
  // Keep the selected-period ranking record scan at its fixed 1,000-wallet
  // budget. Consensus-specific historical context is fetched separately below.
  const recordCandidates = rankingRecordCandidates;
  console.log(`Discovered ${discoveredCandidates.length} ${period.key} candidates; scanning active positions across ${candidates.length} wallets, leaderboard records for ${rankingRecordCandidates.length}, and consensus-holder records for ${consensusWallets.size} (unique record scan: ${recordCandidates.length}).`);
  const records = await attachRecords(recordCandidates, period, nowMs, CLOSED_POSITION_SCAN_LIMIT, fullRecordCache);
  // Consensus holder records are scanned across the complete rolling 7-day window; leaderboard records remain separately capped.
  const consensusRecordCandidates = consensusRecordWallets.map(wallet =>
    candidateByWallet.get(wallet) || {wallet}
  );
  const consensusRecords = await attachRecords(
    consensusRecordCandidates,
    CONSENSUS_RECORD_PERIOD,
    nowMs,
    CLOSED_POSITION_SCAN_LIMIT,
    fullRecordCache
  );
  console.log(period.key + ": scanned up to " + CLOSED_POSITION_SCAN_LIMIT + " closed positions per consensus holder in the rolling 7-day window; capped/incomplete records are withheld.");
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
  console.log(`${period.key}: Strong Signals require 2+ same-side holders each with a complete 7-day record, 10+ closes, >=90% profitable-close rate, positive weekly P&L, and verified $10+ entry/current value. Developing and Watchlist tiers preserve broader 2+ holder overlap.`);
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
    qualificationWindowDays: 7,
    minimumWinRate: MIN_CONSENSUS_WIN_RATE,
    minimumResolvedMarkets: MIN_CONSENSUS_CLOSED_MARKETS,
    qualificationBasis: "Signals are grouped by exact Polymarket condition and outcome, including individual player props. Strong Signals preserve the strict 90%+ complete rolling 7-day profitable-close proxy, 10+ closes, positive weekly P&L, and verified $10+ entry/current value for every displayed holder. Developing Signals and Watchlist retain 2+ distinct same-side holders with their record status shown; these tiers are not represented as 90% signals.",
    minimumSameSideTraders: 2,
    candidateCount: ranked.length,
    discoveredCandidateCount: discoveredCandidates.length,
    settlementRecordCandidateCount: rankingRecordCandidates.length,
    consensusHolderRecordCandidateCount: consensusWallets.size,
    totalRecordCandidateCount: recordCandidates.length,
    scanDiagnostics: lastConsensusDiagnostics,
    candidatePoolBasis: "Up to 20,000 unique wallets selected from public Data API v2 leaderboard discovery across sports, crypto, esports, politics, culture, mentions, weather, economics, tech, finance, and overall categories. The public API does not expose a literal all-wallet directory. The scan checks up to 300 open positions for sports-category wallets and 100 for other wallets. Discovery includes active positions worth $3+; the Strong Signals tier keeps its separate $10+ entry/current-value requirement.",
    resolutionWindowHours: CONSENSUS_WINDOW_HOURS,
    recordBasis: "Strong Signals: every listed trader independently passes the complete rolling 7-day profitable-close proxy with at least 10 closes, 90%+ rate, positive weekly P&L, and verified $10+ entry/current value. Developing Signals: at least one holder has 5+ closes and 75%+ rate with positive weekly P&L. Watchlist: 2+ same-side active holders without that developing qualification. Player props are grouped by exact market and outcome. This is a research signal, not a proven profitable strategy.",
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
  // Select a balanced cross-category sample instead of taking the first N wallets,
  // which would overrepresent sports because discovery intentionally starts there.
  const categoryOrder = ["sports","crypto","esports","overall","politics","culture","mentions","weather","economics","tech","finance"]; // Sports is intentionally prioritized, with direct league discovery below preserving cross-sport coverage.
  const categoryBuckets = new Map(categoryOrder.map(category => [
    category,
    [...unique.values()].filter(trader => Array.isArray(trader.sourceCategories) && trader.sourceCategories.includes(category))
  ]));
  const consensusCandidatePool = [];
  const selectedConsensusWallets = new Set();
  for (let offset = 0; consensusCandidatePool.length < CONSENSUS_WALLET_LIMIT; offset++) {
    let addedThisRound = false;
    for (const category of categoryOrder) {
      const trader = categoryBuckets.get(category)?.[offset];
      if (!trader) continue;
      const wallet = String(trader.wallet || "").toLowerCase();
      if (!wallet || selectedConsensusWallets.has(wallet)) continue;
      selectedConsensusWallets.add(wallet);
      consensusCandidatePool.push(trader);
      addedThisRound = true;
      if (consensusCandidatePool.length >= CONSENSUS_WALLET_LIMIT) break;
    }
    if (!addedThisRound) break;
  }
  // Fill any remaining slots from the full candidate pool if category metadata is absent.
  if (consensusCandidatePool.length < CONSENSUS_WALLET_LIMIT) {
    for (const trader of unique.values()) {
      const wallet = String(trader.wallet || "").toLowerCase();
      if (!wallet || selectedConsensusWallets.has(wallet)) continue;
      selectedConsensusWallets.add(wallet);
      consensusCandidatePool.push(trader);
      if (consensusCandidatePool.length >= CONSENSUS_WALLET_LIMIT) break;
    }
  }
  console.log(`Consensus scan bounded to ${consensusCandidatePool.length.toLocaleString()} balanced wallets across sports, crypto, esports, politics and other categories; leaderboard discovery still covers up to ${unique.size.toLocaleString()} unique wallets.`);
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