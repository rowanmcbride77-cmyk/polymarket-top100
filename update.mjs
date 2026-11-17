
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
const LEADER_COUNT = 100; // Publish the actual Top 100; keep the 1,000-wallet record sample.
// [run-scanner] keep selected-period trader P&L isolated from shared discovery.
// [run-scanner] publish raw holder totals alongside qualified counts.
// [run-scanner] validate the period-record handoff and trimmed dashboard output.
const PAGE_SIZE = 50;
const MAX_CANDIDATES_PER_PERIOD = 20000;
const CATEGORY_CANDIDATE_QUOTAS = { sports: 10000, crypto: 3000, esports: 2000, overall: 2000, politics: 2000, culture: 1200, mentions: 1200, weather: 1200, economics: 1200, tech: 1200, finance: 700 };
const MAX_LEADERBOARD_PAGES_PER_STREAM = 30;
const POSITION_FETCH_LIMIT = 100;
const POSITIONS_PER_TRADER = 10;
const RANKING_RECORD_LIMIT = 1000;
// Audit the top 1,000 selected-period P&L candidates for strict Top 100 qualification.
// Publish up to 100 genuine passers; never substitute a raw P&L leaderboard.
// Positive 30-day realized dollar P&L is a separate profitability gate and may
// include early exits, but it never contributes to settlement wins, losses, or accuracy.
const LEADER_QUALIFICATION_AUDIT_LIMIT = 1000; // Audit the highest selected-period P&L candidates and publish only traders meeting the three agreed settlement rules.
// Bound closed-position history requests. Records that hit this cap are marked
// incomplete and withheld rather than presented as complete win/loss stats.
const CLOSED_POSITION_SCAN_LIMIT = null; // Fully paginate the entire rolling 30-day close history; capped samples cannot qualify.
// Restore the full 20,000-wallet consensus discovery pool. Keep the original
// 30-day qualification and settlement audit; optimize duplicate history work
// separately rather than reducing the wallet discovery target.
const CONSENSUS_WALLET_LIMIT = 20000;
// Keep full 20k-wallet live-position overlap discovery and devote the saved
// leaderboard audit budget to 2,500 actual shared-market holders.
// Selection is category-balanced; every published Strong Signal holder still
// needs a complete 30-day settlement audit and the unchanged strict quality gate.
const CONSENSUS_SETTLEMENT_AUDIT_LIMIT = 1000; // Prioritize the most widely shared wallets so the full 20k discovery scan and qualification audit can finish inside the 40-minute job.
const MAX_CONSENSUS_POSITION_PAGES_PER_WALLET = 2;
const MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET = 4;
const LEADER_RECORD_LIMIT = RANKING_RECORD_LIMIT;
// Retained for dashboard horizon labels only; active sports eligibility is not time-window limited.
const CONSENSUS_WINDOW_HOURS = 24;
const CONSENSUS_HOLDERS_PER_MARKET = 1000;
const CONSENSUS_ENTRY_WINDOW_MINUTES = 90;
const MIN_TRADER_RESOLVED_MARKETS = 10;
const MIN_CONSENSUS_CLOSED_MARKETS = 20; // Strong consensus qualification uses a complete rolling 30-day record with 20+ resolved markets.
const CONSENSUS_RECORD_PERIOD = {key: "month", api: "MONTH"};
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
const API_MIN_INTERVAL_MS = 35; // ~28.5 request starts/sec; Retry-After still globally backs off on API throttling.
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
      // Permanent 4xx request/data errors cannot succeed on retry.
      // Stop immediately so one invalid wallet cannot generate repeated failures.
      if (/^HTTP 4\d\d:/.test(String(error?.message || error))) break;
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
  if (Array.isArray(value?.data?.entries)) return value.data.entries;
  if (Array.isArray(value?.leaderboard)) return value.leaderboard;
  if (Array.isArray(value?.entries)) return value.entries;
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
    // Preserve selected-period USD P&L across current and legacy leaderboard field spellings.
    // Never substitute position-level P&L.
    pnl: num(row.pnl ?? row.profit ?? row.rankPnl ?? row.rank_pnl ?? row.realized_pnl ?? row.realizedPnl),
    // v2's bare volume is shares; only publish a dollar figure when the API
    // explicitly supplies a USDC-denominated field.
    volume: num(row.volume_usdc ?? row.volumeUsdc ?? row.volumeUsd ?? row.volume_usd ?? null),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

const invalidTraderWallets = new Set([
  // Polymarket API explicitly rejected these as protocol contract addresses
  // in the failed scan screenshot; they are not trader accounts.
  "0xa5ef39c3d3e10d0b270233af41cac69796b12966",
  "0x1000008dd9001b968442c1000017eae6e0da00ba"
]);

function isTraderWallet(value) {
  const wallet = String(value || "").trim().toLowerCase();
  return /^0x[a-f0-9]{40}$/.test(wallet) &&
    wallet !== "0x0000000000000000000000000000000000000000";
}

function isUsableTraderWallet(value) {
  const wallet = String(value || "").trim().toLowerCase();
  return isTraderWallet(wallet) && !invalidTraderWallets.has(wallet);
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
    if (pagination.has_more !== true) {
      recordComplete = true;
      break;
    }
    cursor = pagination.next_cursor || null;
    if (!cursor) {      // The API says another page exists but supplied no cursor. Do not      // silently label this truncated history as a complete W/L record.
      recordComplete = false;
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
  const publishableRecord = recordComplete;
  return {
    wins: publishableRecord ? wins : null,
    losses: publishableRecord ? losses : null,
    realizedPnlTotal: publishableRecord ? realizedPnlTotal : null,
    realizedPnlBasis: "Rolling 30-day closed-position realized dollar P&L; may include early exits; separate profitability measure, never settlement accuracy, wins, losses, or resolved-market denominator.",
    sportsWins: recordComplete ? sportsWins : null,
    sportsLosses: recordComplete ? sportsLosses : null,
    sportsResolvedMarkets: recordComplete ? sportsWins + sportsLosses : null,
    sportsWinRate: recordComplete && (sportsWins + sportsLosses) ? (sportsWins / (sportsWins + sportsLosses)) * 100 : null,
    winRate: publishableRecord && resolvedMarkets > 0 ? (wins / resolvedMarkets) * 100 : null,
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
  const eligibleTraders = traders.filter(trader => isUsableTraderWallet(trader?.wallet));
  const skippedWallets = traders.length - eligibleTraders.length;
  if (skippedWallets) console.warn(`Skipped ${skippedWallets} rows without a valid EVM trader wallet during record lookup.`);
  const rows = await mapLimit(eligibleTraders, CONCURRENCY, async trader => {
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
      const message = String(error?.message || error);
      // Polymarket explicitly rejects protocol contract addresses as user wallets.
      // These are not trader-history failures and must not flood the scan logs.
      if (/protocol contract address|invalid_request.*parameter.*user/i.test(message)) {
        const rejectedWallet = String(trader.wallet || "").trim().toLowerCase();
        if (isTraderWallet(rejectedWallet)) invalidTraderWallets.add(rejectedWallet);
        console.warn(`Skipped protocol/invalid user wallet during record lookup: ${rejectedWallet || "missing wallet"}`);
      } else {
        console.error(`Record lookup failed for ${String(trader.name || trader.wallet || "unknown trader")}: ${message}`);
      }
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

// Use Polymarket's canonical OVERALL P&L board for the visible Top 100.
// Category leaderboard rows remain the broad 20k-wallet discovery source,
// but category-specific P&L must never be mixed into the global ranking.
async function getCanonicalLeaderboard(period) {
  const rows = [];
  const timePeriod = String(period?.api || period || "WEEK").toLowerCase();
  let cursor = null;
  const seenCursors = new Set();

  // Data API v1 was retired (HTTP 410). The v2 leaderboard is cursor-paginated
  // and wraps rows in { data, pagination }; offset/timePeriod/orderBy are v1-only.
  while (rows.length < 100) {
    const params = new URLSearchParams({
      category: "overall",
      time_period: timePeriod,
      sort_by: "PNL",
      limit: "50"
    });
    if (cursor) params.set("cursor", cursor);

    const response = await getJson(`${API}/v2/leaderboard?${params}`);
    const page = asArray(response);
    rows.push(...page);
    if (page.length < 50) break;

    const nextCursor = response?.pagination?.next_cursor;
    if (!nextCursor || seenCursors.has(nextCursor)) break;
    seenCursors.add(nextCursor);
    cursor = nextCursor;
  }

  const unique = new Map();
  for (let i = 0; i < rows.length; i++) {
    const trader = normalizeLeader(rows[i], i + 1);
    if (!isUsableTraderWallet(trader.wallet) || num(trader.pnl) === null) continue;
    trader.canonicalLeaderboard = true;
    trader.canonicalRank = num(rows[i]?.rank) ?? i + 1;
    trader.sourceCategories = ["overall"];
    unique.set(trader.wallet, trader);
  }
  if (!unique.size) throw new Error(`Canonical ${timePeriod} OVERALL P&L leaderboard returned no usable traders`);
  return [...unique.values()].sort((a, b) => Number(b.pnl) - Number(a.pnl)).slice(0, 100);
}

function mergeCanonicalLeaderboard(discovered, canonical) {
  const byWallet = new Map(discovered.map(row => [String(row.wallet || "").toLowerCase(), row]));
  for (const row of canonical) {
    const wallet = String(row.wallet || "").toLowerCase();
    const prior = byWallet.get(wallet) || {};
    byWallet.set(wallet, {
      ...prior,
      ...row,
      wallet,
      pnl: num(row.pnl),
      canonicalLeaderboard: true,
      canonicalRank: row.canonicalRank ?? row.rank ?? null,
      sourceCategories: [...new Set([...(prior.sourceCategories || []), "overall"])]
    });
  }
  return [...byWallet.values()];
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
      null,    status: position.status ?? null,
    closed: position.closed === true,    redeemable: position.redeemable === true,
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
    if (!wallet || !side) continue;

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
  // Prefer an explicit game/event start time. Some Gamma sports markets only
  // expose a date-only endDate; parsing that as midnight UTC invents a kickoff
  // time and incorrectly expires same-day NBA/NFL markets before games begin.
  // Preserve date-only metadata and let the enrichment stage treat it as valid
  // through the end of that UTC date instead of fabricating a four-hour finish.
  if (kickoffMs === null && typeof rawEnd === "string" && /^\d{4}-\d{2}-\d{2}$/.test(rawEnd.trim())) {
    return {endDate: rawEnd, endTimeBasis: "date_only_market_end"};
  }
  // In NFL/NBA-style records, a timestamp endDate commonly represents kickoff.
  // If no separate start field exists, allow four hours for play to finish.
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
  const size = num(position.size ?? position.current_size ?? position.currentSize);  const currentValue = num(position.current_value ?? position.currentValue);
  if (size === null || size <= 0) return false;  if (position.closed === true || position.redeemable === true || position.mergeable === true) return false;
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
        pnl: null,
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
    note: `Consensus discovery scans up to ${MAX_CONSENSUS_POSITION_PAGES_PER_WALLET * 100} positions for each general wallet and up to ${MAX_SPORTS_CONSENSUS_POSITION_PAGES_PER_WALLET * 100} for sports-category wallets, across up to ${CONSENSUS_WALLET_LIMIT.toLocaleString()} selected wallets. Active holdings worth $3+ are discovered; Strong Signals retain the $10 verified entry/current-value gate. Exact market condition and outcome are grouped separately. Sports may remain eligible for 48 hours; other categories use 24 hours.`
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
  // Gamma groups sports markets under events. Query a wider page for each
  // league tag, then flatten all active event markets; do not apply kickoff,
  // end-date, or 48-hour filters to explicitly active markets.
  const sports = [
    ["NFL","nfl"],["NBA","nba"],["WNBA","wnba"],["MLB","mlb"],["NHL","nhl"],
    ["NCAAF","ncaaf"],["NCAAB","ncaab"],["Soccer","soccer"],["EPL","epl"],
    ["Tennis","tennis"],["MMA","ufc"],["Golf","golf"],["Cricket","cricket"],
    ["Rugby","rugby"],["Esports","esports"]
  ];
  const marketByCondition = new Map(), familyCounts = {};
  for (const [family, tagSlug] of sports) {
    const params = new URLSearchParams({
      tag_slug:tagSlug,active:"true",closed:"false",limit:"500",order:"startDate",ascending:"true"
    });
    let familyMarkets = 0, eventCount = 0, marketCount = 0, inWindow = 0;
    try {
      const events = asArray(await getJson(`${GAMMA_API}/events?${params}`));
      eventCount = events.length;
      const flattened = events.flatMap(event => {
        const nested = Array.isArray(event.markets) ? event.markets : [];
        return nested.map(raw => ({
          ...raw,
          events:[event],
          eventTitle:raw.eventTitle || event.title || null,
          eventSlug:raw.eventSlug || event.slug || null,
          eventStartTime:raw.eventStartTime || event.startDate || event.startTime || null,
          eventEndDate:raw.eventEndDate || event.endDate || null,
          tags:[...(Array.isArray(event.tags)?event.tags:[]),...(Array.isArray(raw.tags)?raw.tags:[]),family]
        }));
      });
      marketCount = flattened.length;
      for (const market of flattened) {
        const conditionId = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
        if (!conditionId || market.closed === true || String(market.closed).toLowerCase() === "true" ||
            market.active === false || String(market.active).toLowerCase() === "false") continue;
        // Require both live tradability and an estimated resolution within 24h.
        // For sports, consensusTiming estimates finish from kickoff, including live games.
        const timing = consensusTiming(market);
        const finishMs = timestampMs(timing.endDate);
        if (finishMs === null || finishMs <= nowMs ||
            finishMs > nowMs + CONSENSUS_WINDOW_HOURS * 60 * 60 * 1000) continue;
        inWindow++;
        if (!marketByCondition.has(conditionId)) {
          marketByCondition.set(conditionId,{
            ...market,conditionId,category:"sports",sport:family,
            title:market.question || market.title || `${family} market`,
            endDate:timing.endDate,
            gameStartTime:timing.gameStartTime || market.eventStartTime || market.startDate || null
          });
          familyMarkets++;
        }
        if (familyMarkets >= 500) break;
      }
    } catch (error) {
      console.warn(`Direct ${family} event discovery failed for tag slug ${tagSlug}:`,error.message);
    }
    familyCounts[family] = {events:eventCount,markets:marketCount,inWindow,eligible:familyMarkets};
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
  console.log("Direct multi-sport event discovery:",JSON.stringify({
    sportFamilies:familyCounts,eligibleMarkets:eligible.length,
    sameSideConsensusGroups:found.length,
    playerPropGroups:found.filter(group => /\\b(points?|rebounds?|assists?|three.?pointers?|3.?pointers?|threes?|steals?|blocks?|pra|yards?|receptions?|touchdowns?|strikeouts?|hits?|goals?)\\b/i.test(String(group.title || ""))).length
  }));
  return found;
}

async function enrichConsensusMarkets(holderGroups, nowMs) {  // Position records can carry stale/missing end dates. Resolve condition IDs
  // through Gamma before applying the strict 24-hour resolution window; keep undated active
  // markets visible rather than silently deleting the entire consensus panel.
  // Enrich every discovered same-side group; do not cap the market list  // or let a popular category crowd other categories out.
  // Consensus Markets are based on positions that are currently open, not on
  // whether the trader happened to add to the position in the last hour.
  const scan = holderGroups;
  console.log("Consensus enrichment: checking " + scan.length + " shared open-position groups.");
  // Gamma markets are keyed by condition ID, so query each unique condition
  // once. The old code queried both every outcome token and every condition ID
  // in 40-item batches (1,192 HTTP calls in the last scan) even though the
  // response was only indexed by condition ID. Condition-only batches of 100
  // preserve the same metadata coverage with roughly one-sixth the requests.
  const conditionIds = [...new Set(scan.map(group => String(group.conditionId || "").toLowerCase()).filter(Boolean))];
  const chunks = [];
  for (let i = 0; i < conditionIds.length; i += 100) {
    chunks.push(conditionIds.slice(i, i + 100));
  }

  const pages = await mapLimit(chunks, CONCURRENCY, async ids => {
    const params = new URLSearchParams();
    params.set("limit", "1000");
    for (const id of ids) params.append("condition_ids", id);
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

    // Keep only explicitly active markets with a verifiable estimated resolution
    // inside the next 24 hours. For sports, consensusTiming estimates game finish
    // from kickoff; unknown or stale dates are not copy-ready and are excluded.
    if (endMs === null || endMs <= nowMs || endMs > nowMs + CONSENSUS_WINDOW_HOURS * 60 * 60 * 1000) {
      outsideWindowRemoved++;
      continue;
    }

    const category = classifyMarketCategory(market || {}, group);

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
  // A market with no same-side overlap is normal discovery output, not a
  // per-market warning. Aggregate these counts to keep scanner logs readable.
  const marketsWithoutSameSideOverlap = marketHolderRows.filter(rows => !rows || rows.length === 0).length;
  const marketSideGroupsFound = marketHolderRows.reduce((sum, rows) => sum + (Array.isArray(rows) ? rows.length : 0), 0);
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
    marketSideGroupsFound,
    marketsWithoutSameSideOverlap,
    directSportsGroups: directSportsGroups.length
  }));
  enriched.splice(0, enriched.length, ...allExpanded);

  const categoryCounts = enriched.reduce((counts, group) => { counts[group.category] = (counts[group.category] || 0) + 1; return counts; }, {});
  console.log("Consensus Gamma enrichment: " + JSON.stringify({
    scannedGroups: scan.length,
    categoryCounts,
    conditionIds: conditionIds.length,
    conditionIdsRequested: conditionIds.length,
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

function buildConsensus(holderGroups, candidates, records = {}, minimumResolvedMarkets = MIN_CONSENSUS_CLOSED_MARKETS, selectedPeriodRecords = {}, periodVolumeStats = {}, periodKey = "week") {
  const candidateMap = new Map(candidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader]));
  const strongGroups = [], developingGroups = [], watchlistGroups = [];
  const strongQualifiedSidesByCondition = new Map();
  const diagnostics = {
    rawGroupsWithTwoHolders: 0, holdersEvaluated: 0, incompleteRecord: 0,
    fewerThanMinimumCloses: 0, belowMinimumWinRate: 0,
    strongHolders: 0, developingHolders: 0,
    marketsWithTenStrongHolders: 0, marketsWithTenDevelopingHolders: 0
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
      const hasVerifiedEntryPrice = true; // Position size and entry price are display-only, not qualification rules.
      const periodPnl = num(candidate.pnl); // Display context only; not a qualification input.
      const complete = record.settlementRecordComplete === true;
      const realizedPnlTotal = num(record.realizedPnlTotal);
      // The only trader qualification rules are: a complete settlement-only
      // record (so early exits are excluded), 20+ resolved markets in 30 days,
      // and at least 90% settlement win rate.
      const strongQualified = complete &&
        resolvedMarkets !== null && resolvedMarkets >= minimumResolvedMarkets &&
        winRate !== null && winRate >= MIN_CONSENSUS_WIN_RATE;
      const developingQualified = false;

      diagnostics.holdersEvaluated++;
      if (!complete) diagnostics.incompleteRecord++;
      if (resolvedMarkets === null || resolvedMarkets < minimumResolvedMarkets) diagnostics.fewerThanMinimumCloses++;
      if (winRate === null || winRate < MIN_CONSENSUS_WIN_RATE) diagnostics.belowMinimumWinRate++;
      if (strongQualified) diagnostics.strongHolders++;
      if (developingQualified) diagnostics.developingHolders++;

      return {
        ...holder, wallet, name: holder.name || candidate.name || "Trader",
        rank: candidate.sportsRank ?? candidate.rank ?? holder.rank ?? null,
        overallRank: candidate.rank ?? null, pnl: periodPnl, periodPnl, periodKey,
        weeklyPnl: periodKey === "week" ? periodPnl : null,
        volume: periodVolumeStats[wallet]?.volume ?? candidate.volume ?? null,
        volumeBasis: periodVolumeStats[wallet]?.volumeBasis ?? (candidate.volume !== null && candidate.volume !== undefined ? "Polymarket leaderboard USD volume field" : "Unavailable from period user-volume endpoint"),
        wins, losses, winRate, resolvedMarkets, recordComplete: complete,
        // Never expose close-profitability proxy counts as public W/L or accuracy.
        // Early exits may contribute only to the separate realized-dollar-P&L field.
        periodWins: null, periodLosses: null,
        periodResolvedMarkets: null, periodWinRate: null,
        periodRecordComplete: false,        qualified: strongQualified,
        strongQualified, developingQualified, entryVerified: hasVerifiedEntryPrice,
        recordStatus: !complete ? "Settlement history incomplete" :
          (resolvedMarkets === null || resolvedMarkets < minimumResolvedMarkets ? "Fewer than 20 settled markets in 30 days" :
            (winRate !== null && winRate < MIN_CONSENSUS_WIN_RATE ? "Below 90% settlement accuracy" : "Meets all three qualifications")),
        recordBasis: complete
          ? "Complete rolling 30-day settlement-only record; early exits excluded from wins and losses."
          : "Settlement history incomplete; excluded from prediction qualification.",
        realizedPnlTotal,
        realizedPnlBasis: record.realizedPnlBasis || "Rolling 30-day closed-position realized dollar P&L; may include early exits; separate profitability measure, never settlement accuracy.",
        settlementRecordComplete: record.settlementRecordComplete === true,
        settlementWins: wins,
        settlementLosses: losses,
        settlementPositionsChecked: record.settlementPositionsChecked ?? 0,
        settlementPositionsUnclassified: record.settlementPositionsUnclassified ?? 0
      };
    });

    const strongHolders = holders.filter(holder => holder.strongQualified);
    const conditionKey = String(group.conditionId || "").trim().toLowerCase();
    if (conditionKey && strongHolders.length > 0) {
      if (!strongQualifiedSidesByCondition.has(conditionKey)) strongQualifiedSidesByCondition.set(conditionKey, new Set());
      strongQualifiedSidesByCondition.get(conditionKey).add(String(group.side || "").trim().toLowerCase());
    }
    const developingHolders = [];
    const tier = "Strong Signals";
    const displayTraders = strongHolders;
    const tierReason = "Every listed trader has a complete rolling 30-day settlement-only record, 20+ settled markets, and 90%+ settlement accuracy. Early exits are excluded.";

    if (displayTraders.length >= 10 && tier === "Strong Signals") diagnostics.marketsWithTenStrongHolders++;
    if (displayTraders.length >= 10 && tier === "Developing Signals") diagnostics.marketsWithTenDevelopingHolders++;
    const knownRates = displayTraders.map(trader => num(trader.winRate)).filter(rate => rate !== null);
    const avgRate = knownRates.length ? knownRates.reduce((sum, rate) => sum + rate, 0) / knownRates.length : 0;
    const entries = displayTraders.map(trader => num(trader.entryValue)).filter(value => value !== null && value > 0);
    const totalEntry = entries.length ? entries.reduce((sum, value) => sum + value, 0) : null;
    const totalValue = displayTraders.reduce((sum, trader) => sum + (num(trader.currentValue) ?? 0), 0);
    const breadthScore = Math.min(displayTraders.length, 20) / 20 * 55;
    const capitalScore = Math.min(Math.log10(Math.max(1, totalEntry || 0) + 1) / Math.log10(10001), 1) * 25;
    const qualityScore = knownRates.length ? avgRate / 100 * 10 : 0;
    // Published consensus is STRICTLY qualified consensus: only Strong Signals
    // count. Developing/watchlist holders are discovery context, never members.
    if (strongHolders.length < 2) continue;
    const result = {
      ...group, traders: strongHolders, sameSide: strongHolders.length,
      displayedTraders: strongHolders.length,
      qualifiedTraders: strongHolders.length, developingTraders: developingHolders.length,
      strongHolderCount: strongHolders.length, signalTier: "Strong Signals",
      tierReason: "Every listed trader has a complete rolling 30-day settlement-only record, 20+ settled markets, and 90%+ settlement accuracy. Early exits are excluded.",
      consensusScore: Math.round(breadthScore + capitalScore + qualityScore),
      totalValue, totalEntry
    };
    strongGroups.push(result);
  }

  // Publish only unanimous qualified consensus per exact market condition.
  // If qualified traders pass the Strong gate on both outcomes, suppress BOTH
  // sides. Unqualified holders never enter strongQualifiedSidesByCondition.
  const unanimousStrongGroups = strongGroups.filter(group => {
    const conditionKey = String(group.conditionId || "").trim().toLowerCase();
    if (!conditionKey) return false;
    const qualifiedSides = strongQualifiedSidesByCondition.get(conditionKey);
    return qualifiedSides && qualifiedSides.size === 1;
  });
  const splitQualifiedGroupsRemoved = strongGroups.length - unanimousStrongGroups.length;
  strongGroups.splice(0, strongGroups.length, ...unanimousStrongGroups);
  diagnostics.splitQualifiedGroupsRemoved = splitQualifiedGroupsRemoved;

  const sortGroups = (a, b) =>
    (b.consensusScore ?? 0) - (a.consensusScore ?? 0) ||
    b.sameSide - a.sameSide ||
    (b.totalValue ?? 0) - (a.totalValue ?? 0) ||
    (b.newestBuy || 0) - (a.newestBuy || 0);
  strongGroups.sort(sortGroups);
  developingGroups.sort(sortGroups);
  watchlistGroups.sort(sortGroups);

  // Publish every market that passes the exact three trader rules and has
  // at least two distinct qualified wallets on one unanimous outcome side.
  const result = strongGroups.sort(sortGroups);
  const unanimousResult = result.filter(group => {
    const condition = String(group.conditionId || "").trim().toLowerCase();
    const sides = strongQualifiedSidesByCondition.get(condition);
    return sides && sides.size === 1 && group.sameSide >= 2;
  });
  return unanimousResult;
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
  // CLOSED positions are exits, not proof of settlement. Polymarket v2 exposes
  // unredeemed settled winners as REDEEMABLE and held settled losers as
  // REDEEMABLE_LOST. Ordinary OPEN holdings are not part of the settlement
  // denominator and must never be paginated as a substitute for REDEEMABLE.
  async function fetchPositionCohort(status, maxPages) {
    const rows = [];
    let cursor = null;
    let complete = true;
    let pagesFetched = 0;
    do {
      const params = new URLSearchParams({
        user: wallet,
        // v2 supports up to 1,000 rows per page. Larger pages avoid falsely
        // marking active high-volume traders incomplete after a 1,000-row cap.
        limit: "1000",
        status,
        sort_by: "TIMESTAMP",
        sort_direction: "DESC"
      });
      // CLOSED rows are bounded by their exit event. Held positions must be
      // fetched without activity bounds because they may have been bought
      // before the 30-day window but still resolve inside it.
      if (status === "CLOSED") {
        params.set("start", String(Math.floor(periodCutoffMs / 1000)));
        params.set("end", String(Math.floor(nowMs / 1000)));
      }
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

  // Scan a wider closed-position history for settlement verification.
  // If a cohort is truncated or a market cannot be classified, the record
  // remains incomplete and is never presented as a verified W-L.
  const cohorts = await Promise.all([
    fetchPositionCohort("CLOSED", 100),
    fetchPositionCohort("REDEEMABLE", 100),
    fetchPositionCohort("REDEEMABLE_LOST", 100)
  ]);
  // Require complete CLOSED, REDEEMABLE, and REDEEMABLE_LOST cohorts.
  // REDEEMABLE contains unredeemed settled winners; REDEEMABLE_LOST contains
  // held settled losers. Ordinary OPEN positions are deliberately excluded:
  // they are unresolved holdings, not settled predictions. A truncated
  // settlement cohort means accuracy is unknown, so fail closed.
  const allPagesComplete = cohorts.every(cohort => cohort.complete);
  const uniqueRows = new Map();
  // Prefer a position still held at resolution over an earlier closed row
  // for the same outcome (e.g. sell, then re-enter before settlement).
  const priority = status => status === "REDEEMABLE" ? 3 : status === "REDEEMABLE_LOST" ? 2 : 1;
  for (let cohortIndex = 0; cohortIndex < cohorts.length; cohortIndex++) {
    const cohort = cohorts[cohortIndex];
    if (cohortIndex === 1 && !cohort.complete) continue;
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
  for (let i = 0; i < conditions.length; i += 100) conditionChunks.push(conditions.slice(i, i + 100));
  const marketPages = await mapLimit(conditionChunks, CONCURRENCY, async ids => {
    const q = new URLSearchParams();
    // Gamma defaults to closed=false, which silently omits resolved markets.
    // Settlement verification needs metadata for both live early exits and
    // actually resolved markets, so explicitly request the complete status set.
    q.set("closed", "any");
    q.set("limit", String(Math.min(ids.length, 100)));
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
  const settledOutcomesByCondition = new Map();
  const earlyKeys = new Set();
  const earlyExitDetails = new Map();
  const addEarlyExit = (condition, index, position, closeTime) => {
    const key = condition + "|" + String(index);
    earlyKeys.add(key);
    earlyExitDetails.set(key, {asset:String(position.token_id ?? position.tokenId ?? position.asset ?? ""),closeTime});
  };
  for (const position of positions) {
    const condition = String(position.condition_id ?? position.conditionId ?? "").toLowerCase();
    const market = markets.get(condition);
    const scanStatus = String(position._scanStatus || position.status || "").toUpperCase();
    const lastEventAt = timeMs(position.last_event_at ?? position.lastEventAt ?? position.timestamp ?? position.updated_at ?? position.updatedAt);
    if (!market) {
      // Every fetched cohort here represents a settlement candidate or a
      // closed exit. Missing metadata means the outcome cannot be verified.
      unclassified++;
      continue;
    }

    // An active-market CLOSED row is an early exit, never a win or loss.
    // A row from either REDEEMABLE cohort must resolve to a closed market;
    // otherwise its settlement status cannot be verified.
    if (market.closed !== true && String(market.closed).toLowerCase() !== "true") {
      if (scanStatus === "CLOSED") {
        if (lastEventAt === null) { unclassified++; continue; }
        classified++;
        earlyExitPositions++;
        const rawIndex = position.outcome_index ?? position.outcomeIndex ?? position.outcome;
        addEarlyExit(condition, rawIndex ?? "unknown", position, lastEventAt);
      } else {
        unclassified++;
      }
      continue;
    }

    const outcomes = parseArrayField(market.outcomes);
    const prices = parseArrayField(market.outcomePrices).map(num);
    const winnerIndex = prices.findIndex(price => price !== null && price >= 0.999);
    const resolvedAt = timeMs(market.closedTime ?? market.closed_time ?? market.resolvedAt ?? market.resolved_at);
    // A closed market without a terminal outcome is not a settled prediction.
    // Ignore it rather than poisoning the trader's record; only terminal,
    // in-window outcomes contribute to the verified win/loss denominator.
    if (winnerIndex < 0 || resolvedAt === null) continue;
    if (resolvedAt < periodCutoffMs || resolvedAt > nowMs) continue;

    const rawIndex = position.outcome_index ?? position.outcomeIndex;
    let positionIndex = rawIndex === null || rawIndex === undefined ? -1 : Number(rawIndex);
    // Data API v2 uses 999 as an explicit "outcome not labeled" sentinel.
    // If the row still includes an exact outcome label, resolve it against
    // Gamma's canonical outcome list; never infer a side from prices or P&L.
    if (!Number.isInteger(positionIndex) || positionIndex < 0 || positionIndex === 999 || positionIndex >= outcomes.length) {
      const outcome = String(position.outcome ?? "").trim().toLowerCase();
      positionIndex = outcomes.findIndex(label => String(label).trim().toLowerCase() === outcome);
    }
    if (positionIndex < 0 || positionIndex >= outcomes.length) { unclassified++; continue; }

    // If the trader exited before resolution, the P&L of that exit is not a
    // settlement result. Current held positions and exits at/after resolution
    // are classified by the actual resolved outcome.
    if (scanStatus === "CLOSED" && lastEventAt !== null && lastEventAt < resolvedAt) {
      classified++;      earlyExitPositions++;
      addEarlyExit(condition, positionIndex, position, lastEventAt);
      continue;
    }
    if (scanStatus === "CLOSED" && lastEventAt === null) { unclassified++; continue; }
    classified++;
    if (!settledOutcomesByCondition.has(condition)) {
      settledOutcomesByCondition.set(condition, {winnerIndex, outcomeIndexes:new Set()});
    }
    settledOutcomesByCondition.get(condition).outcomeIndexes.add(positionIndex);
  }

  // Count one directional prediction per condition. If a trader held both
  // outcomes through settlement, treat that as a hedge, not two predictions.
  let hedgedSettledMarkets = 0;
  for (const {winnerIndex, outcomeIndexes} of settledOutcomesByCondition.values()) {
    if (outcomeIndexes.size !== 1) {
      hedgedSettledMarkets++;
      continue;
    }
    if ([...outcomeIndexes][0] === winnerIndex) settlementWins++;
    else settlementLosses++;
  }

  const metadataComplete = marketPages.every(page => page.ok);
  // Early exits never count as prediction wins or losses. We do not infer
  // accuracy from the price at which a trader sold.
  const settlementRecordComplete = allPagesComplete && metadataComplete && unclassified===0;
  return {
    settlementWins, settlementLosses,
    earlyExitMarkets:earlyKeys.size, earlyExitPositions, settlementRecordComplete,
    settlementPositionsChecked:positions.length, settlementPositionsClassified:classified,
    settlementPositionsUnclassified:unclassified,
    settlementRecordBasis:"Settlement-only record: verified market outcomes; positions exited before resolution excluded from wins and losses; complete CLOSED, REDEEMABLE, and REDEEMABLE_LOST cohorts plus verified market metadata required. OPEN positions are unresolved and excluded.",
    settlementRecordCoverage:settlementRecordComplete?"complete":"partial"
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
  const eligibleTraders = traders.filter(trader => isUsableTraderWallet(trader?.wallet));
  if (eligibleTraders.length !== traders.length) {
    console.warn(`Skipped ${traders.length - eligibleTraders.length} protocol/malformed wallets before period-volume lookup.`);
  }
  return await mapLimit(eligibleTraders, CONCURRENCY, async trader => {
    try {
      const stats = await getPeriodVolume(String(trader.wallet || "").toLowerCase(), period, nowMs);
      return { ...trader, ...stats };
    } catch (error) {
      console.warn("Period USD volume unavailable for " + trader.wallet + ":", error.message);
      return { ...trader, volume: null, volumeShares: null, volumeTradeCount: null, volumeBasis: "Unavailable from Data API v2 user-volume" };
    }
  });
}

// Verified settlement outcomes are the only source for the public leaderboard W/L record.
async function attachSettlementStats(traders, period, nowMs) {
  const eligibleTraders = traders.filter(trader => isTraderWallet(trader?.wallet));
  if (eligibleTraders.length !== traders.length) {
    console.warn(`Skipped ${traders.length - eligibleTraders.length} rows without a valid EVM trader wallet during settlement audit.`);
  }
  return await mapLimit(eligibleTraders, 8, async trader => {
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
      const message = String(error?.message || error);
      if (/protocol contract address|invalid_request.*parameter.*user/i.test(message)) {
        const rejectedWallet = String(trader.wallet || "").trim().toLowerCase();
        if (isTraderWallet(rejectedWallet)) invalidTraderWallets.add(rejectedWallet);
        console.warn("Skipped protocol/invalid user wallet during settlement audit: " + (rejectedWallet || "missing wallet"));
      } else {
        console.warn("Settlement record unavailable for " + trader.wallet + ":", message);
      }
      return {...trader, settlementWins: null, settlementLosses: null, earlyExitMarkets: null, earlyExitPositions: null, settlementRecordComplete: false, settlementPositionsChecked: 0};
    }
  });
}

function makeOutput(period, ranked, sportsRanked, activePositions, tradeRows, nowMs, consensusCandidates, records, leaderRecords, consensusGroups, settlementRecords, consensusRecords, consensusPeriodRecords, consensusVolumeStats) {
  // Preserve the exact period-scoped record used to rank this list.
  // Full-history enrichment must never overwrite weekly/monthly wins, losses,
  // win rate, or resolved-market count, otherwise the displayed numbers no
  // longer match the sort order.
  // Keep the general leaderboard populated; the public data cannot prove
  // that traders never exit early, so don't use a 100% profitable-close
  // filter as if it were a verified hold-to-settlement guarantee.
  // The Top 100 is the actual selected-period P&L ranking, not a filtered list
  // of traders whose separate settlement-history lookup happened to be complete.
  // Settlement W-L is attached afterward and shown only when independently verified.
  // Always publish the actual selected-period leaderboard. The expensive
  // settlement audit is intentionally separate; until a row has a verified
  // settlement audit, keep W/L and accuracy blank rather than hiding every trader
  // or treating early exits as wins/losses.
  const leaders = ranked
    .filter(trader => num(trader.pnl) !== null)
    .slice(0, LEADER_COUNT)
    .map((trader, index) => ({
      ...trader,
      rank: index + 1,
      wins: null,
      losses: null,
      winRate: null,
      resolvedMarkets: null,
      // Close-profitability proxies may include early exits; never leak them
      // into the public settlement-only leaderboard fields.
      sportsWins: null,
      sportsLosses: null,
      sportsWinRate: null,
      sportsResolvedMarkets: null,
      periodWins: null,
      periodLosses: null,
      periodResolvedMarkets: null,
      periodWinRate: null,
      periodRecordComplete: false,
      recordComplete: false,
      recordVerified: false,
      settlementRecordComplete: false,
      settlementRecordCoverage: null,
      settlementPositionsChecked: 0,
      settlementPositionsUnclassified: 0,
      recordBasis: "Settlement-only audit not completed for this leaderboard row; early exits are excluded from wins, losses, and accuracy."
    }));

  const leaderPositions = Object.fromEntries(
    leaders.map(trader => [
      trader.wallet,
      // Keep the initial dashboard payload light; profiles need only a preview.
      (activePositions[trader.wallet] || []).slice(0, 3)
    ])
  );

  return {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis:
      "Top 100 is ranked by verified selected-period Polymarket leaderboard P&L. Settlement wins/losses and accuracy are shown only after independent settlement verification; early exits are never counted as resolved wins or losses.",
    recordBasis: "Published Top 100 wins, losses, and win rate are verified settlement outcomes only. Positions exited before resolution are excluded.",
    realizedPnlBasis: "Separate rolling closed-position dollar P&L may include early exits; it is not a settlement outcome or accuracy measure.",
    candidateCount: ranked.length,
    sportsLeaders: sportsRanked.slice(0, LEADER_COUNT).map(trader => ({
      ...trader,
      wins: null, losses: null, winRate: null, resolvedMarkets: null,
      sportsWins: null, sportsLosses: null, sportsWinRate: null, sportsResolvedMarkets: null,
      recordComplete: false, recordVerified: false,
      recordBasis: "Settlement-only record not independently audited for this sports discovery list; early-exit profitability is not published as accuracy."
    })),
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: true,
    recordWarning:
      "Settlement-only accuracy is used for qualification. Early exits are excluded from wins and losses; incomplete settlement histories are not qualified.",
    leaders,
    // Combo discovery pool is intentionally broad: combo traders can be profitable
    // despite low win rates or small sample sizes. Rank and display their actual combo positions.
    comboCandidates: ranked.slice(0, RANKING_RECORD_LIMIT).map(trader => ({
      wallet: trader.wallet,
      name: trader.name || trader.username || "",
      rank: trader.rank ?? null,
      pnl: trader.pnl ?? trader.profit ?? null,
      volume: trader.volume ?? trader.vol ?? null,
      wins: null,
      losses: null,
      winRate: null,
      resolvedMarkets: null,
      recordComplete: false,
      recordVerified: false,
      recordBasis: "Settlement-only record not independently audited for combo discovery; early-exit profitability is not published as accuracy."
    })),
    // Three recent BUY prints per Top-100 wallet are enough for the initial preview.
    traderActiveTrades: Object.fromEntries(tradeRows.filter(Boolean).map(([wallet, rows]) => [wallet, (rows || []).slice(0, 3)])),
    traderActivePositions: leaderPositions,
    consensus: buildConsensus(consensusGroups, consensusCandidates || ranked, consensusRecords || {}, MIN_CONSENSUS_CLOSED_MARKETS, consensusPeriodRecords || {}, consensusVolumeStats || {}, period.key)
  };
}

async function writeJsonUnderLimit(filename, output) {
  const hasLeaders = Array.isArray(output?.leaders) && output.leaders.length > 0;
  const hasConsensus = Array.isArray(output?.consensus) && output.consensus.length > 0;
  const allowEmptyConsensus = String(filename).startsWith("consensus-");
  // Empty qualified lists are valid when no trader passes the settlement-only gate.
  // Publish the explicit result rather than leaving stale data online.
  const allowEmptyQualifiedLeaderboard = false; // Never overwrite the live Top 100 with an empty leaderboard.
  if (!output || (!hasLeaders && !hasConsensus && !allowEmptyConsensus && !allowEmptyQualifiedLeaderboard)) {
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
  if (!parsedHasLeaders && !parsedHasConsensus && !allowEmptyConsensus && !allowEmptyQualifiedLeaderboard) {
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
  const usableLeaderboardCandidates = candidates.filter(trader => isUsableTraderWallet(trader.wallet));
  // Select the actual period-P&L Top 100 first, then fully paginate their
  // selected-period CLOSED-position history. Never spend the record budget
  // on the first 1,000 discovery-order wallets and leave the displayed leaders unaudited.
  const canonicalCandidates = usableLeaderboardCandidates.filter(trader => trader.canonicalLeaderboard === true);
  const visibleRankingCandidates = canonicalCandidates.length ? canonicalCandidates : usableLeaderboardCandidates;
  const rankingRecordCandidates = LEADER_QUALIFICATION_AUDIT_LIMIT > 0
    ? rankCandidates(visibleRankingCandidates, {}).slice(0, Math.min(LEADER_COUNT, LEADER_QUALIFICATION_AUDIT_LIMIT))
    : [];
  // Use the selected period's leaderboard rows for rank/P&L; the shared
  // consensus discovery pool may contain a different period's P&L or nulls.
  const candidateByWallet = new Map(
    candidates.map(trader => [String(trader.wallet || "").toLowerCase(), trader])
  );
  // Prioritize wallets shared across the greatest number of distinct markets.
  // This makes the bounded qualification audit cover the highest-overlap traders
  // first instead of depending on whichever market happened to appear first.
  const consensusWalletFrequency = new Map();
  for (const group of consensusGroups) {
    const groupWallets = new Set((Array.isArray(group.traders) ? group.traders : [])
      .map(holder => String(holder.wallet || "").toLowerCase())
      .filter(Boolean));
    for (const wallet of groupWallets) {
      consensusWalletFrequency.set(wallet, (consensusWalletFrequency.get(wallet) || 0) + 1);
    }
  }
  const consensusWallets = new Set([...consensusWalletFrequency.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([wallet]) => wallet));
  // Audit every distinct wallet actually holding a discovered consensus outcome.
  // Do not pre-filter by position size, entry price, P&L, category, or capital.
  const consensusRecordWallets = [...consensusWallets].slice(0, CONSENSUS_SETTLEMENT_AUDIT_LIMIT);
  console.log(`Settlement-auditing all ${consensusRecordWallets.length.toLocaleString()} distinct discovered consensus holders; no stake-size or profitability qualification filters.`);
  // Fully paginate the exact selected-period close history for each visible
  // Top 100 wallet. maxPositions=null means continue until the API confirms
  // there are no more pages or the period cutoff is reached.
  const recordCandidates = rankingRecordCandidates;
  console.log(`Discovered ${discoveredCandidates.length} ${period.key} candidates; scanning active positions across ${candidates.length} wallets and fully auditing selected-period closed-market records for the Top ${recordCandidates.length}; consensus discovery remains separate.`);
  const records = await attachRecords(recordCandidates, period, nowMs, null, fullRecordCache);
  const rejectedLeaderboardWallets = candidates.length - usableLeaderboardCandidates.length;
  if (rejectedLeaderboardWallets) {
    console.warn(`Excluded ${rejectedLeaderboardWallets} malformed/protocol wallets from leaderboard ranking.`);  }  const ranked = rankCandidates(visibleRankingCandidates, records);
  const sportsRanked = rankSportsCandidates(usableLeaderboardCandidates, records);
  console.log(`${period.key}: ranked ${ranked.length} candidates; ${sportsRanked.length} sports-qualified candidates.`);
  // Settlement-only history is the sole performance audit for consensus.
  // It excludes early exits and is independent of closed-position dollar P&L.
  const consensusRecordCandidates = consensusRecordWallets.map(wallet =>
    candidateByWallet.get(wallet) || {wallet}
  );
  const consensusSettlementRows = await attachSettlementStats(
    consensusRecordCandidates,
    CONSENSUS_RECORD_PERIOD,
    nowMs
  );
  const consensusRecords = Object.fromEntries(consensusSettlementRows.map(trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    const wins = num(trader.settlementWins);
    const losses = num(trader.settlementLosses);
    const resolvedMarkets = wins !== null && losses !== null ? wins + losses : null;
    return [wallet, {
      wins, losses, resolvedMarkets,
      winRate: resolvedMarkets > 0 ? wins / resolvedMarkets * 100 : null,
      recordComplete: trader.settlementRecordComplete === true,
      settlementRecordComplete: trader.settlementRecordComplete === true,
      settlementWins: wins,
      settlementLosses: losses,
      settlementPositionsChecked: trader.settlementPositionsChecked ?? 0,
      settlementPositionsClassified: trader.settlementPositionsClassified ?? 0,
      settlementPositionsUnclassified: trader.settlementPositionsUnclassified ?? 0,
      settlementRecordCoverage: trader.settlementRecordCoverage ?? (trader.settlementRecordComplete === true ? "complete" : "partial"),
      recordBasis: trader.settlementRecordBasis || "Settlement history incomplete; excluded from prediction qualification",
      realizedPnlTotal: null,
      realizedPnlBasis: "Not used for consensus qualification."
    }];
  }));
  console.log(`${period.key}: ${Object.keys(consensusRecords).length} consensus holder records loaded from the complete rolling 30-day closed-position history.`);
  // Keep selected-period records separate from the rolling 30-day qualification record.
  // Weekly period detail is optional display context, not a qualification input.
  const consensusPeriodRecords = period.key === "week" ? records : consensusRecords;
  // USD volume comes from the dedicated user-volume endpoint, never share volume.
  // Consensus-card volume is optional; avoid one API request per holder per period.
  // Visible Top 100 volume lookups remain below.
  const consensusVolumeRows = [];
  const consensusVolumeStats = Object.fromEntries(consensusVolumeRows.map(trader => [
    String(trader.wallet || "").toLowerCase(),
    {volume: trader.volume ?? null, volumeBasis: trader.volumeBasis || "Unavailable from period user-volume endpoint"}
  ]));
  console.log(period.key + ": independently audited " + consensusRecordCandidates.length + " shared-market holders for consensus; strict Top 100 audit separately evaluates up to " + LEADER_QUALIFICATION_AUDIT_LIMIT.toLocaleString() + " selected-period P&L candidates.");
  // Audit a wider, bounded P&L-ranked sample so the Top 100 is not artificially
  // capped by the first 1,000 candidates. Keep the audit well below the full
  // 19k-20k discovery pool to avoid tens of thousands of history requests.
  const leaderAuditCandidates = rankCandidates(usableLeaderboardCandidates, {})
    .slice(0, LEADER_QUALIFICATION_AUDIT_LIMIT);
  // Apply only the three agreed qualification rules. Do not add a separate
  // positive-P&L, stake-size, entry-price, or current-value gate.
  const leaderSettlementRows = await attachSettlementStats(
    leaderAuditCandidates,
    CONSENSUS_RECORD_PERIOD,
    nowMs
  );
  const leaderSettlementByWallet = Object.fromEntries(leaderSettlementRows.map(trader => {
    const wallet = String(trader.wallet || "").toLowerCase();
    const wins = num(trader.settlementWins);
    const losses = num(trader.settlementLosses);
    const resolvedMarkets = wins !== null && losses !== null ? wins + losses : null;
    return [wallet, {
      ...trader, wins, losses, resolvedMarkets,
      winRate: resolvedMarkets > 0 ? wins / resolvedMarkets * 100 : null,
      recordComplete: trader.settlementRecordComplete === true
    }];
  }));
  const qualifiedLeaderRows = leaderAuditCandidates
    .map(candidate => {
      const wallet = String(candidate.wallet || "").toLowerCase();
      const settlement = leaderSettlementByWallet[wallet] || {};
      // Fail closed: a closed-position profitability record is NOT settlement
      // accuracy. Only the independent market-resolution audit may supply W/L
      // counts or qualify a Top 100 trader. Missing/partial settlement rows
      // must never fall back to the closeRecord's early-exit-inclusive stats.
      const wins = num(settlement.settlementWins);
      const losses = num(settlement.settlementLosses);
      const resolvedMarkets = wins !== null && losses !== null ? wins + losses : null;
      const settlementComplete = settlement.settlementRecordComplete === true &&
        resolvedMarkets !== null && resolvedMarkets > 0;
      return {
        ...candidate,
        ...settlement,
        wins: settlementComplete ? wins : null,
        losses: settlementComplete ? losses : null,
        resolvedMarkets: settlementComplete ? resolvedMarkets : null,
        winRate: settlementComplete ? wins / resolvedMarkets * 100 : null,
        recordComplete: settlementComplete,
        recordVerified: settlementComplete,
        settlementRecordComplete: settlementComplete,
        recordBasis: settlementComplete
          ? (settlement.settlementRecordBasis || "Complete rolling 30-day settlement-only record; early exits excluded from wins and losses.")
          : "Settlement record incomplete; not qualified.",
        settlementRecordBasis: settlement.settlementRecordBasis || "Settlement history incomplete; excluded from prediction qualification.",
        // Close-profitability sports W/L proxies may include early exits. Do not
        // publish them beside the settlement-only Top 100 record.
        sportsWins: null,
        sportsLosses: null,
        sportsWinRate: null,
        sportsResolvedMarkets: null,
        periodWins: null,
        periodLosses: null,
        periodResolvedMarkets: null,
        periodWinRate: null,
        periodRecordComplete: false,
        realizedPnlTotal: null,
        realizedPnlBasis: "Not used for Top 100 qualification.",
        wallet
      };
    })
    .filter(trader => trader.settlementRecordComplete === true &&
      Number(trader.resolvedMarkets) >= MIN_CONSENSUS_CLOSED_MARKETS &&
      Number(trader.winRate) >= MIN_CONSENSUS_WIN_RATE)
    .map(trader => ({
      ...trader,
      pnl: candidateByWallet.get(String(trader.wallet).toLowerCase())?.pnl ?? trader.realizedPnlTotal,
      volume: candidateByWallet.get(String(trader.wallet).toLowerCase())?.volume ?? null
    }));
  const qualifiedRanked = rankCandidates(qualifiedLeaderRows, {});
  const leaders = qualifiedRanked.slice(0, LEADER_COUNT);
  console.log(`${period.key}: published ${leaders.length} qualified Top 100 traders from ${leaderAuditCandidates.length} highest-P&L candidates; qualification uses only complete settlement history, 20+ resolved markets, and >=90% settlement accuracy.`);
  // Reuse the settlement-only audit already performed for this candidate sample.
  const settlementRecords = leaderSettlementByWallet;
  const consensusMinimumResolved = MIN_CONSENSUS_CLOSED_MARKETS;
  console.log(`${period.key}: Consensus requires 2+ distinct same-side holders, each with a complete rolling 30-day settlement-only record, 20+ settled markets, and >=90% settlement accuracy. Early exits are excluded. No P&L, stake-size, entry-price, or current-value qualification filters.`);
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
    discoveredCandidates,
    records,
    leaderRecords,
    consensusGroups,
    settlementRecords,
    consensusRecords,
    consensusPeriodRecords,
    consensusVolumeStats
  );

  // Publish only traders who passed the three agreed settlement qualifications.
output.leaders = leaders.map((trader, index) => ({
  ...trader,
  rank: index + 1,
  wins: null,
  losses: null,
  winRate: null,
  resolvedMarkets: null,
  recordComplete: false,
  recordVerified: false,
  settlementRecordComplete: false,
  settlementPositionsChecked: 0,
  settlementPositionsUnclassified: 0,
  recordBasis: "Settlement-only audit pending; early exits are excluded from wins, losses, and accuracy."
}));
output.traderActivePositions = Object.fromEntries(output.leaders.map(trader => [
  trader.wallet,
  (activePositions[trader.wallet] || []).slice(0, 3)
]));
output.leaderQualification = {
  windowDays: 30,
  minimumResolvedMarkets: MIN_CONSENSUS_CLOSED_MARKETS,
  minimumWinRate: MIN_CONSENSUS_WIN_RATE,
  requirePositiveRealizedPnl: false,
  eligibleCandidatesAudited: leaderAuditCandidates.length,
  prefilteredCandidates: leaderAuditCandidates.length,
  qualifiedCandidates: qualifiedRanked.length,
  publishedLeaders: output.leaders.length,
  basis: "The visible Top 100 is ranked by selected-period P&L among traders who pass exactly three rules: complete rolling 30-day settlement-only history (early exits excluded), 20+ settled markets, and >=90% settlement accuracy."
};

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
      profitableCloseTraderCount: qualifiedWallets.length,
      profitableCloseTraderCountAtLeastFiveTrades: qualifiedAtLeastFiveTrades.length,
      minimumTradesForCloseProfitabilityCount: 5,
      candidateWalletsScanned: recordCandidates.length,
      recordsReturned: Object.keys(allCandidateRecords).length,
      completeRecords: completeRecordRows.length,
      incompleteOrUnavailableRecords: recordCandidates.length - completeRecordRows.length,
      minimumWinRate: MIN_CONSENSUS_WIN_RATE,
      minimumResolvedMarkets: MIN_TRADER_RESOLVED_MARKETS,
      windowDays: 7,
      basis: "Trading-profitability diagnostic only: unique wallets from the discovered public leaderboard candidate pool; complete closed-position records within the last 7 days; one net profitable/unprofitable result per market. Early exits count. This is not settlement accuracy and is not used to qualify Top 100 or consensus signals."
    };
    // Do not publish the close-profitability diagnostic: its closed-position proxy includes early exits and is not settlement accuracy.
    console.log("Weekly all-candidate qualification count: " + JSON.stringify(qualificationCount));
  }
  // Do not use period-scoped closed-position W/L as public accuracy.
  // Those counts include early exits; only the independent settlement audit
  // above may populate Top 100 wins, losses, win rate, or qualification.
  output.leaders = await attachPeriodVolumes(output.leaders, period, nowMs);
  output.leaders = output.leaders.map(trader => {
    const settlement = leaderSettlementByWallet[String(trader.wallet || "").toLowerCase()] || {};
    const wins = num(settlement.settlementWins);
    const losses = num(settlement.settlementLosses);
    const resolvedMarkets = wins !== null && losses !== null ? wins + losses : null;
    const verified = settlement.settlementRecordComplete === true &&
      resolvedMarkets !== null && resolvedMarkets > 0 &&
      Number(settlement.settlementPositionsUnclassified || 0) === 0;
    return {
      ...trader,
      // Never publish the closed-position profitability proxy as settlement accuracy.
      sportsWins: null,
      sportsLosses: null,
      sportsWinRate: null,
      sportsResolvedMarkets: null,
      periodWins: null,
      periodLosses: null,
      periodResolvedMarkets: null,
      periodWinRate: null,
      periodRecordComplete: false,
      wins: verified ? wins : null,
      losses: verified ? losses : null,
      resolvedMarkets: verified ? resolvedMarkets : null,
      winRate: verified ? wins / resolvedMarkets * 100 : null,
      recordComplete: verified,
      recordVerified: verified,
      settlementRecordComplete: verified,
      settlementPositionsChecked: settlement.settlementPositionsChecked ?? 0,
      settlementPositionsUnclassified: settlement.settlementPositionsUnclassified ?? 0,
      recordBasis: verified
        ? "Verified settlement-only record; positions exited before market resolution are excluded."
        : "Settlement history not fully verified; wins, losses, and accuracy withheld. Early exits are never counted as settled outcomes.",
      pnlBasis: `Polymarket v2 leaderboard P&L for selected ${period.key} period`
    };
  });
  output.recordBasis = "Top 100 is the selected-period Polymarket P&L leaderboard. Settlement wins, losses, and accuracy are shown only when independently verified; early exits are excluded and incomplete histories are left blank.";

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
    qualificationBasis: "Signals are grouped by exact Polymarket condition and outcome, including individual player props. Each trader must have a complete rolling 30-day settlement-only record, 20+ settled markets, and 90%+ settlement accuracy. Early exits are excluded from wins and losses. No P&L, stake-size, entry-price, or current-value qualification filters.",
    minimumSameSideTraders: 2,
    candidateCount: ranked.length,
    discoveredCandidateCount: discoveredCandidates.length,
    settlementRecordCandidateCount: rankingRecordCandidates.length,
    consensusHolderRecordCandidateCount: consensusWallets.size,
    totalRecordCandidateCount: recordCandidates.length,
    scanDiagnostics: lastConsensusDiagnostics,
    candidatePoolBasis: `Up to ${CONSENSUS_WALLET_LIMIT.toLocaleString()} unique wallets selected from public Data API v2 leaderboard discovery across sports, crypto, esports, politics, culture, mentions, weather, economics, tech, finance, and overall categories. The public API does not expose a literal all-wallet directory. The scan checks up to 200 open positions for sports-category wallets and 100 for other wallets. No minimum position-value or entry-value qualification filter is applied.`,
    resolutionWindowHours: CONSENSUS_WINDOW_HOURS,
    recordBasis: "Consensus qualification uses exactly three rules per trader: complete rolling 30-day settlement-only record (early exits excluded), at least 20 settled markets, and at least 90% settlement accuracy. No P&L, position-size, entry-price, or current-value filters. Player props are grouped by exact market and outcome. This is a research signal, not a proven profitable strategy.",
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

  console.log("Loading canonical weekly and monthly OVERALL P&L leaderboards...");
  const [weekCanonical, monthCanonical] = await Promise.all([
    getCanonicalLeaderboard(PERIODS[0]),
    getCanonicalLeaderboard(PERIODS[1])
  ]);
  const finalWeekCandidates = mergeCanonicalLeaderboard(weekCandidates, weekCanonical);
  const finalMonthCandidates = mergeCanonicalLeaderboard(monthCandidates, monthCanonical);
  console.log(`Canonical leaderboard rows merged: week=${weekCanonical.length}, month=${monthCanonical.length}.`);

  const unique = new Map();

  // Use the full discovered pool for overlap detection; only the detailed
  // period-history scan is bounded separately.
  const positionCandidates = [
    ...finalWeekCandidates,
    ...finalMonthCandidates
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
  await buildPeriod(PERIODS[0], finalWeekCandidates, activePositions, nowMs, consensusCandidatePool, fullRecordCache, consensusGroups);
  await buildPeriod(PERIODS[1], finalMonthCandidates, activePositions, nowMs, consensusCandidatePool, fullRecordCache, consensusGroups);

  console.log("Polymarket tracker update completed.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error);
  process.exitCode = 1;
});
// Manual refresh trigger: run the existing validated scanner workflow without changing scan logic.
