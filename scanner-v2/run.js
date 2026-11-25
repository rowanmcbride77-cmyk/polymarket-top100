import fs from "node:fs/promises";
import { buildConsensusSignals } from "./consensus.js";
import { projectSettlementPosition } from "./position-projection.js";
import { buildSettlementRecord } from "./settlement.js";
import { fetchTokenResolutionIndex } from "./resolution-data.js";

const DATA = "https://data-api.polymarket.com";
const startedAt = new Date().toISOString();
const WINDOW_DAYS = 30;
const MIN_MARKETS = 10;
const MIN_WIN_RATE = 90;
const CANDIDATES_PER_PERIOD = 1000;
const POSITION_PAGE_SIZE = 1000;
const MAX_POSITION_PAGES = 5;
const CONCURRENCY = 12;
const AUDIT_BATCH_SIZE = 20;
const REQUEST_GAP_MS = 52; // ~19.2 requests/sec, below the public API's 20 req/s ceiling
let nextRequestAt = 0;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function requestSlot() {
  const now = Date.now();
  const at = Math.max(now, nextRequestAt);
  nextRequestAt = at + REQUEST_GAP_MS;
  if (at > now) await sleep(at - now);
}
function retryDelay(response, attempt) {
  const h = response.headers.get("retry-after");
  if (h) {
    const n = Number(h);
    if (Number.isFinite(n)) return Math.max(0, n * 1000);
    const d = Date.parse(h);
    if (Number.isFinite(d)) return Math.max(0, d - Date.now());
  }
  return Math.min(15000, 700 * (2 ** attempt));
}
async function getJson(url) {
  let last;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      await requestSlot();
      const response = await fetch(url, {
        headers: { Accept: "application/json", "User-Agent": "PolymarketSettlementScanner/2.0" },
        signal: AbortSignal.timeout(25000)
      });
      if (response.status === 429 || response.status === 503 || response.status >= 500) {
        const wait = retryDelay(response, attempt);
        await sleep(wait);
        throw new Error("Retryable API response HTTP " + response.status);
      }
      if (!response.ok) throw new Error("HTTP " + response.status + ": " + (await response.text()).slice(0, 200));
      return await response.json();
    } catch (error) {
      last = error;
      if (/^HTTP 4\d\d:/.test(String(error.message))) break;
      if (attempt < 4 && !String(error.message).startsWith("Retryable")) await sleep(Math.min(10000, 500 * (2 ** attempt)));
    }
  }
  throw last || new Error("API request failed");
}

async function requestDataApi(path, params = {}) {
  const url = new URL(path, DATA);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
  }
  // Reuse the scanner's global request pacing and retry policy for the
  // settlement-resolution endpoints as well.
  return getJson(url.toString());
}
function rowsOf(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}
function n(v) {
  if (v === null || v === undefined || v === "") return null;
  const x = Number(v);
  return Number.isFinite(x) ? x : null;
}
async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  await Promise.all(Array.from({length: Math.min(limit, items.length)}, async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i], i);
    }
  }));
  return out;
}

async function getLeaderboard(period) {
  const all = [];
  let cursor = null;
  const seen = new Set();
  for (let page = 0; page < 20 && all.length < CANDIDATES_PER_PERIOD; page++) {
    const q = new URLSearchParams({
      category: "overall", time_period: period, sort_by: "PNL", limit: "50"
    });
    if (cursor) q.set("cursor", cursor);
    const payload = await getJson(DATA + "/v2/leaderboard?" + q);
    const pageRows = rowsOf(payload);
    all.push(...pageRows);
    const next = payload?.pagination?.next_cursor;
    if (payload?.pagination?.has_more !== true || !next || seen.has(next)) break;
    seen.add(next);
    cursor = next;
  }
  const unique = new Map();
  for (const [index, row] of all.entries()) {
    const wallet = String(row.proxy_wallet ?? row.proxyWallet ?? row.user_id ?? row.userId ?? "").toLowerCase();
    const pnl = n(row.pnl ?? row.profit);
    if (!/^0x[a-f0-9]{40}$/.test(wallet) || pnl === null || unique.has(wallet)) continue;
    unique.set(wallet, {
      wallet,
      leaderboardRank: index + 1,
      name: row.user_name ?? row.userName ?? row.username ?? row.name ?? "Polymarket Trader",
      pnl,
      selectedPeriodPnl: pnl,
      volume: n(row.volume_usdc ?? row.volumeUsd ?? row.volume_usd),
      source: "Polymarket public Data API v2 leaderboard",
      period
    });
  }
  return [...unique.values()].sort((a,b) => b.pnl - a.pnl).slice(0, CANDIDATES_PER_PERIOD);
}

// Pull only the three lifecycle cohorts that can prove a settled prediction.
// CLOSED is bounded to the last 60 days; redeemable cohorts are unbounded because
// a position may have been entered earlier and still resolve inside the window.
async function fetchCohort(wallet, status, cutoffSec, nowSec) {
  const out = [];
  let cursor = null;
  let complete = true;
  let exhausted = false;
  let pages = 0;
  while (pages < MAX_POSITION_PAGES) {
    const q = new URLSearchParams({
      user: wallet, limit: String(POSITION_PAGE_SIZE), status,
      sort_by: "TIMESTAMP", sort_direction: "DESC"
    });
    if (status === "CLOSED") {
      q.set("start", String(cutoffSec));
      q.set("end", String(nowSec));
    }
    if (cursor) q.set("cursor", cursor);
    const payload = await getJson(DATA + "/v2/positions?" + q);
    const pageRows = rowsOf(payload);
    out.push(...pageRows.map(row => projectSettlementPosition(row, status)));
    pages++;
    const pagination = payload?.pagination ?? {};
    if (pagination.has_more !== true) { exhausted = true; break; }
    const next = pagination.next_cursor;
    if (!next) { complete = false; break; }
    cursor = String(next);
  }
  if (!exhausted) {
    // Hitting the page cap or receiving a malformed cursor is incomplete.
    // Fail closed rather than presenting a partial record as accurate.
    complete = false;
  }
  return { rows: out, complete, pages };
}

async function auditWallet(wallet, cutoffSec, nowSec) {
  const cohorts = await Promise.all([
    fetchCohort(wallet, "CLOSED", cutoffSec, nowSec),
    fetchCohort(wallet, "REDEEMABLE", cutoffSec, nowSec),
    fetchCohort(wallet, "REDEEMABLE_LOST", cutoffSec, nowSec)
  ]);
  const byPosition = new Map();
  let invalidRows = 0;
  const priority = { CLOSED: 1, REDEEMABLE_LOST: 2, REDEEMABLE: 3 };
  for (const cohort of cohorts) for (const row of cohort.rows) {
    const condition = String(row.condition_id ?? row.conditionId ?? "").toLowerCase();
    const raw = row.outcome_index ?? row.outcomeIndex ?? row.outcome ?? "";
    if (!condition) { invalidRows++; continue; }
    const token = String(row.token_id ?? row.tokenId ?? "").toLowerCase();
    const key = condition + "|" + (token || String(raw).toLowerCase());
    const prior = byPosition.get(key);
    if (!prior || priority[row._cohort] > priority[prior._cohort]) byPosition.set(key, row);
  }
  return {
    wallet, positions: [...byPosition.values()],
    complete: cohorts.every(c => c.complete) && invalidRows === 0,
    invalidRows,
    pages: cohorts.reduce((s,c) => s + c.pages, 0),
    rows: cohorts.reduce((s,c) => s + c.rows.length, 0)
  };
}

const [weekCandidates, monthCandidates] = await Promise.all([
  getLeaderboard("week"), getLeaderboard("month")
]);
if (!weekCandidates.length || !monthCandidates.length) throw new Error("Polymarket returned an empty leaderboard.");

const candidateByWallet = new Map();
for (const row of [...weekCandidates, ...monthCandidates]) {
  const prior = candidateByWallet.get(row.wallet) || {};
  candidateByWallet.set(row.wallet, {
    ...prior,
    ...row,
    bestLeaderboardRank: Math.min(prior.bestLeaderboardRank ?? Infinity, row.leaderboardRank ?? Infinity)
  });
}
const candidates = [...candidateByWallet.values()]
  .sort((a, b) => a.bestLeaderboardRank - b.bestLeaderboardRank || b.pnl - a.pnl)
  .slice(0, 1000);
if (candidates.length !== 1000) {
  throw new Error(`Expected 1,000 unique Polymarket leaderboard wallets; received ${candidates.length}. Refusing to publish a smaller sample.`);
}
const selectedWallets = new Set(candidates.map(row => row.wallet));
const selectedWeekCandidates = weekCandidates.filter(row => selectedWallets.has(row.wallet));
const selectedMonthCandidates = monthCandidates.filter(row => selectedWallets.has(row.wallet));
const cutoffMs = Date.now() - WINDOW_DAYS * 86400000;
const nowMs = Date.now();
const cutoffSec = Math.floor(cutoffMs / 1000);
const nowSec = Math.floor(nowMs / 1000);
console.log("Settlement audit: " + candidates.length + " unique wallets selected from the top 1,000 weekly and top 1,000 monthly Polymarket P&L leaderboards by best leaderboard rank; rolling 30-day window; max " + MAX_POSITION_PAGES + " pages per lifecycle cohort.");
console.log("Qualification pool: weekly=" + selectedWeekCandidates.length + ", monthly=" + selectedMonthCandidates.length + "; audit batches=" + Math.ceil(candidates.length / AUDIT_BATCH_SIZE) + ".");

// Settlement W/L come only from /v2/tokens finalPrice joined to /v2/resolutions status/resolvedAt.\n// Process wallets in bounded batches. Keeping every wallet's raw position history
// and the complete settlement-resolution map alive together exceeded the hosted runner's
// ~4 GB Node heap. Each batch is fully classified before its raw rows are released.
const records = new Map();
let completeAudits = 0;
let incompleteAudits = 0;
let positionsChecked = 0;
let positionPages = 0;
let resolutionConditionLookups = 0;

for (let offset = 0; offset < candidates.length; offset += AUDIT_BATCH_SIZE) {
  const batch = candidates.slice(offset, offset + AUDIT_BATCH_SIZE);
  console.log("Auditing wallet batch " + (Math.floor(offset / AUDIT_BATCH_SIZE) + 1) +
    "/" + Math.ceil(candidates.length / AUDIT_BATCH_SIZE) + " (" + batch.length + " wallets).");
  const audits = await mapLimit(batch, CONCURRENCY, async trader => {
    try { return await auditWallet(trader.wallet, cutoffSec, nowSec); }
    catch (error) {
      console.warn("Audit failed for " + trader.wallet + ": " + error.message);
      return { wallet: trader.wallet, positions: [], complete: false, pages: 0, rows: 0 };
    }
  });
  const allConditions = new Set();
  for (const audit of audits) {
    positionsChecked += audit.rows;
    positionPages += audit.pages;
    for (const position of audit.positions) {
      const condition = String(position.condition_id ?? position.conditionId ?? "").toLowerCase();
      if (condition) allConditions.add(condition);
    }
  }
  const resolutionIndex = await fetchTokenResolutionIndex([...allConditions], {
    request: requestDataApi,
    resolutionBatchConcurrency: 4
  });
  resolutionConditionLookups += allConditions.size;
  for (const audit of audits) {
    const record = buildSettlementRecord(audit.positions, resolutionIndex.values(), {
      nowMs,
      windowDays: WINDOW_DAYS,
      historyComplete: audit.complete
    });
    const diagnostics = record.diagnostics || {};
    const complete = record.complete === true;
    if (complete) completeAudits++;
    else incompleteAudits++;
    records.set(audit.wallet, {
      wins: complete ? record.wins : null,
      losses: complete ? record.losses : null,
      settlementWins: complete ? record.wins : null,
      settlementLosses: complete ? record.losses : null,
      resolvedMarkets: complete ? record.resolvedMarkets : null,
      winRate: complete ? record.winRate : null,
      recordComplete: complete,
      settlementRecordComplete: complete,
      earlyExitPositions: Number(diagnostics.closedExcluded || 0),
      hedgedMarkets: Number(diagnostics.conflictingConditionsExcluded || 0),
      positionsChecked: audit.rows,
      positionsPages: audit.pages,
      unclassifiedPositions: Number(diagnostics.unverifiedExcluded || 0),
      pendingPositions: Number(diagnostics.pendingExcluded || 0),
      outsideWindowPositions: Number(diagnostics.outsideWindowExcluded || 0),
      recordBasis: record.basis
    });
    // Drop the large per-wallet position arrays as soon as classification is done.
    audit.positions = [];
  }
  // Drop batch-scoped arrays/maps before requesting the next batch.
}

function qualify(candidatesForPeriod) {
  return candidatesForPeriod
    .map(row => ({ ...row, ...(records.get(row.wallet) || {}) }))
    .filter(row => row.recordComplete === true &&
      row.settlementRecordComplete === true &&
      Number.isFinite(Number(row.wins)) &&
      Number.isFinite(Number(row.losses)) &&
      Number(row.resolvedMarkets) === Number(row.wins) + Number(row.losses) &&
      Number(row.resolvedMarkets) >= MIN_MARKETS &&
      Number.isFinite(Number(row.winRate)) &&
      Number(row.winRate) >= MIN_WIN_RATE &&
      String(row.recordBasis || "").toLowerCase().includes("settlement-only") &&
      String(row.recordBasis || "").toLowerCase().includes("excluded"))
    .sort((a,b) => b.pnl - a.pnl)
    .slice(0, 100)
    .map((row, index) => ({ ...row, rank: index + 1, period: row.period }));
}
const weekLeaders = qualify(selectedWeekCandidates);
const monthLeaders = qualify(selectedMonthCandidates);
if (!weekLeaders.length || !monthLeaders.length) {
  const summary = {
    ok: false, scannerVersion: "settlement-v2", startedAt,
    generatedAt: new Date().toISOString(),
    candidates: candidates.length,
    completeAudits,
    incompleteAudits,
    qualifiedWeek: weekLeaders.length,
    qualifiedMonth: monthLeaders.length,
    reason: "No qualified leaderboard was published. Incomplete histories are withheld."
  };
  await fs.writeFile("settlement-audit-status.json", JSON.stringify(summary, null, 2) + "\n");
  throw new Error("Settlement qualification did not produce a non-empty Top 100 for both periods; previous published files preserved.");
}

const [weekConsensus, monthConsensus] = await Promise.all([
  buildConsensusSignals(weekLeaders, { minimumHolders: 2, strongHolders: 8, horizonHours: 24, minimumEntryUsd: 10 }),
  buildConsensusSignals(monthLeaders, { minimumHolders: 2, strongHolders: 8, horizonHours: 24, minimumEntryUsd: 10 })
]);

const finishedAt = new Date().toISOString();
for (const [period, leaders, consensusResult] of [["week", weekLeaders, weekConsensus], ["month", monthLeaders, monthConsensus]]) {
  await fs.writeFile(period + ".json", JSON.stringify({
    generatedAt: finishedAt, period,
    source: "Polymarket Data API v2 leaderboard + verified token payouts and condition resolution finality",
    leaders, scannerVersion: "settlement-v2",
    qualification: { windowDays: WINDOW_DAYS, minimumResolvedMarkets: MIN_MARKETS, minimumWinRate: MIN_WIN_RATE, earlyExitsIncluded: false, settlementBasis: "Verified token payout and condition resolution time; pre-resolution exits excluded." }
  }, null, 2) + "\n");
  await fs.writeFile("consensus-" + period + ".json", JSON.stringify({
    generatedAt: finishedAt, period,
    source: "Open Polymarket positions held by settlement-qualified leaderboard traders; same-token agreement; recent BUY activity verified; active sports, esports and crypto markets only.",
    consensus: consensusResult.signals,
    diagnostics: consensusResult.diagnostics,
    qualificationWindowDays: WINDOW_DAYS,
    minimumResolvedMarkets: MIN_MARKETS,
    qualification: {
      windowDays: WINDOW_DAYS,
      minimumResolvedMarkets: MIN_MARKETS,
      minimumWinRate: MIN_WIN_RATE,
      minimumHolders: 2,
      strongHolders: 8,
      horizonHours: 24,
      minimumEntryUsd: 10
    },
    scannerVersion: "settlement-v2"
  }, null, 2) + "\n");
}
const status = {
  ok: true, scannerVersion: "settlement-v2", startedAt, finishedAt, generatedAt: finishedAt,
  source: "Polymarket public Data API v2 leaderboard + /v2/tokens final payouts + /v2/resolutions finality",
  windowDays: WINDOW_DAYS, minimumResolvedMarkets: MIN_MARKETS, minimumWinRate: MIN_WIN_RATE,
  candidates: candidates.length, completeAudits,
  incompleteAudits,
  positionsChecked,
  positionPages,
  resolutionConditionLookups,
  weekCandidates: weekCandidates.length, monthCandidates: monthCandidates.length,
  weekQualified: weekLeaders.length, monthQualified: monthLeaders.length,
  weekConsensusSignals: weekConsensus.signals.length,
  monthConsensusSignals: monthConsensus.signals.length,
  consensusBuiltFromQualifiedTradersOnly: true,
  earlyExitsExcluded: true,
  earlyExitRealizedPnlPublished: false,
  settlementVerification: "Exact binary token finalPrice joined to condition status=resolved and resolvedAt from Polymarket Data API v2"
};
await fs.writeFile("status.json", JSON.stringify(status, null, 2) + "\n");
await fs.writeFile("settlement-audit-status.json", JSON.stringify(status, null, 2) + "\n");
console.log("Published settlement-qualified Top 100: week=" + weekLeaders.length + ", month=" + monthLeaders.length + ".");
