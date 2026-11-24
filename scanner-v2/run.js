import fs from "node:fs/promises";

const DATA = "https://data-api.polymarket.com";
const GAMMA = "https://gamma-api.polymarket.com";
const startedAt = new Date().toISOString();
const WINDOW_DAYS = 60;
const MIN_MARKETS = 10;
const MIN_WIN_RATE = 90;
const CANDIDATES_PER_PERIOD = 500;
const POSITION_PAGE_SIZE = 1000;
const MAX_POSITION_PAGES = 5;
const CONCURRENCY = 18;
const REQUEST_GAP_MS = 55; // stay below the public API's documented 20 req/s ceiling
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
function ms(v) {
  if (v === null || v === undefined || v === "") return null;
  const x = Number(v);
  const out = Number.isFinite(x) ? (x < 1e12 ? x * 1000 : x) : Date.parse(String(v));
  return Number.isFinite(out) ? out : null;
}
function arrayField(v) {
  if (Array.isArray(v)) return v;
  if (typeof v !== "string") return [];
  try { const parsed = JSON.parse(v); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
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
  for (let page = 0; page < 10 && all.length < CANDIDATES_PER_PERIOD; page++) {
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
  for (const row of all) {
    const wallet = String(row.proxy_wallet ?? row.proxyWallet ?? row.user_id ?? row.userId ?? "").toLowerCase();
    const pnl = n(row.pnl ?? row.profit);
    if (!/^0x[a-f0-9]{40}$/.test(wallet) || pnl === null || unique.has(wallet)) continue;
    unique.set(wallet, {
      wallet,
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
// CLOSED is bounded to the last 30 days; redeemable cohorts are unbounded because
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
    out.push(...pageRows.map(row => ({...row, _cohort: status})));
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

async function fetchMarketMetadata(conditionIds) {
  const unique = [...new Set(conditionIds)].filter(Boolean);
  const chunks = [];
  for (let i = 0; i < unique.length; i += 100) chunks.push(unique.slice(i, i + 100));
  const results = await mapLimit(chunks, 8, async ids => {
    const q = new URLSearchParams({ closed: "true", limit: String(ids.length) });
    for (const id of ids) q.append("condition_ids", id);
    const closed = rowsOf(await getJson(GAMMA + "/markets?" + q));
    const qOpen = new URLSearchParams({ closed: "false", limit: String(ids.length) });
    for (const id of ids) qOpen.append("condition_ids", id);
    const open = rowsOf(await getJson(GAMMA + "/markets?" + qOpen));
    return [...closed, ...open];
  });
  const map = new Map();
  for (const market of results.flat()) {
    const id = String(market.conditionId ?? market.condition_id ?? "").toLowerCase();
    if (id) map.set(id, market);
  }
  return map;
}

function classifyAudit(audit, markets, cutoffMs, nowMs) {
  let wins = 0, losses = 0, earlyExitPositions = 0, unclassified = audit.invalidRows || 0;
  const settled = new Map();
  for (const position of audit.positions) {
    const condition = String(position.condition_id ?? position.conditionId ?? "").toLowerCase();
    const market = markets.get(condition);
    const cohort = String(position._cohort || "").toUpperCase();
    const lastEvent = ms(position.last_event_at ?? position.lastEventAt ?? position.timestamp ?? position.updated_at ?? position.updatedAt);
    if (!market) { unclassified++; continue; }
    const isClosed = market.closed === true || String(market.closed).toLowerCase() === "true";
    if (!isClosed) {
      if (cohort === "CLOSED" && lastEvent !== null) { earlyExitPositions++; continue; }
      unclassified++;
      continue;
    }
    const outcomes = arrayField(market.outcomes);
    const prices = arrayField(market.outcomePrices).map(n);
    const winner = prices.findIndex(price => price !== null && price >= 0.999);
    const resolvedAt = ms(market.closedTime ?? market.closed_time ?? market.resolvedAt ?? market.resolved_at);
    if (winner < 0 || resolvedAt === null) { unclassified++; continue; }
    if (resolvedAt < cutoffMs || resolvedAt > nowMs) continue;

    let index = Number(position.outcome_index ?? position.outcomeIndex);
    if (!Number.isInteger(index) || index < 0 || index === 999 || index >= outcomes.length) {
      const label = String(position.outcome ?? "").trim().toLowerCase();
      index = outcomes.findIndex(x => String(x).trim().toLowerCase() === label);
    }
    if (index < 0 || index >= outcomes.length) { unclassified++; continue; }

    // Include early exits when the market later resolves inside the scoring
    // window: score the trader's selected side against the final market outcome.
    // This measures directional market accuracy, not the trader's realized P&L.
    if (cohort === "CLOSED" && (lastEvent === null || lastEvent < resolvedAt)) {
      earlyExitPositions++;
    }
    if (!settled.has(condition)) settled.set(condition, { winner, indexes: new Set() });
    settled.get(condition).indexes.add(index);
  }
  let hedged = 0;
  for (const item of settled.values()) {
    if (item.indexes.size !== 1) { hedged++; continue; }
    if ([...item.indexes][0] === item.winner) wins++;
    else losses++;
  }
  const resolvedMarkets = wins + losses;
  const complete = audit.complete && unclassified === 0;
  return {
    wins: complete ? wins : null,
    losses: complete ? losses : null,
    resolvedMarkets: complete ? resolvedMarkets : null,
    winRate: complete && resolvedMarkets ? wins / resolvedMarkets * 100 : null,
    recordComplete: complete,
    settlementRecordComplete: complete,
    earlyExitPositions,
    hedgedMarkets: hedged,
    positionsChecked: audit.rows,
    positionsPages: audit.pages,
    unclassifiedPositions: unclassified,
    recordBasis: "Verified Gamma final outcomes; early-exited positions included when the market later resolves inside the window; this is directional accuracy, not realized close P&L; hedged markets excluded."
  };
}

const [weekCandidates, monthCandidates] = await Promise.all([
  getLeaderboard("week"), getLeaderboard("month")
]);
if (!weekCandidates.length || !monthCandidates.length) throw new Error("Polymarket returned an empty leaderboard.");

const candidateByWallet = new Map();
for (const row of [...weekCandidates, ...monthCandidates]) {
  const prior = candidateByWallet.get(row.wallet) || {};
  candidateByWallet.set(row.wallet, { ...prior, ...row });
}
const candidates = [...candidateByWallet.values()];
const cutoffMs = Date.now() - WINDOW_DAYS * 86400000;
const nowMs = Date.now();
const cutoffSec = Math.floor(cutoffMs / 1000);
const nowSec = Math.floor(nowMs / 1000);
console.log("Settlement audit: " + candidates.length + " unique candidates; rolling 60-day window; max " + MAX_POSITION_PAGES + " pages per lifecycle cohort.");

const audits = await mapLimit(candidates, CONCURRENCY, async trader => {
  try { return await auditWallet(trader.wallet, cutoffSec, nowSec); }
  catch (error) {
    console.warn("Audit failed for " + trader.wallet + ": " + error.message);
    return { wallet: trader.wallet, positions: [], complete: false, pages: 0, rows: 0 };
  }
});
const allConditions = audits.flatMap(a => a.positions.map(p => String(p.condition_id ?? p.conditionId ?? "").toLowerCase()));
const marketMetadata = await fetchMarketMetadata(allConditions);
const records = new Map(audits.map(a => [
  a.wallet, classifyAudit(a, marketMetadata, cutoffMs, nowMs)
]));

function qualify(candidatesForPeriod) {
  return candidatesForPeriod
    .map(row => ({ ...row, ...(records.get(row.wallet) || {}) }))
    .filter(row => row.recordComplete === true &&
      Number(row.resolvedMarkets) >= MIN_MARKETS &&
      Number(row.winRate) >= MIN_WIN_RATE)
    .sort((a,b) => b.pnl - a.pnl)
    .slice(0, 100)
    .map((row, index) => ({ ...row, rank: index + 1, period: row.period }));
}
const weekLeaders = qualify(weekCandidates);
const monthLeaders = qualify(monthCandidates);
if (!weekLeaders.length || !monthLeaders.length) {
  const summary = {
    ok: false, scannerVersion: "settlement-v2", startedAt,
    generatedAt: new Date().toISOString(),
    candidates: candidates.length,
    completeAudits: audits.filter(a => a.complete).length,
    qualifiedWeek: weekLeaders.length,
    qualifiedMonth: monthLeaders.length,
    reason: "No qualified leaderboard was published. Incomplete histories are withheld."
  };
  await fs.writeFile("settlement-audit-status.json", JSON.stringify(summary, null, 2) + "\n");
  throw new Error("Settlement qualification did not produce a non-empty Top 100 for both periods; previous published files preserved.");
}

const finishedAt = new Date().toISOString();
for (const [period, leaders] of [["week", weekLeaders], ["month", monthLeaders]]) {
  await fs.writeFile(period + ".json", JSON.stringify({
    generatedAt: finishedAt, period,
    source: "Polymarket Data API v2 leaderboard + verified Gamma settlement outcomes",
    leaders, scannerVersion: "settlement-v2",
    qualification: { windowDays: WINDOW_DAYS, minimumResolvedMarkets: MIN_MARKETS, minimumWinRate: MIN_WIN_RATE, earlyExitsIncluded: true }
  }, null, 2) + "\n");
  await fs.writeFile("consensus-" + period + ".json", JSON.stringify({
    generatedAt: finishedAt, period,
    source: "Consensus scan is intentionally separate from settlement-qualified leaderboard",
    consensus: [], diagnostics: { signals: 0 },
    scannerVersion: "settlement-v2"
  }, null, 2) + "\n");
}
const status = {
  ok: true, scannerVersion: "settlement-v2", startedAt, finishedAt, generatedAt: finishedAt,
  source: "Polymarket public Data API v2 leaderboard + Gamma resolved market metadata",
  windowDays: WINDOW_DAYS, minimumResolvedMarkets: MIN_MARKETS, minimumWinRate: MIN_WIN_RATE,
  candidates: candidates.length, completeAudits: audits.filter(a => a.complete).length,
  incompleteAudits: audits.filter(a => !a.complete).length,
  positionsChecked: audits.reduce((s,a) => s + a.rows, 0),
  positionPages: audits.reduce((s,a) => s + a.pages, 0),
  marketMetadataConditions: allConditions.length,
  weekCandidates: weekCandidates.length, monthCandidates: monthCandidates.length,
  weekQualified: weekLeaders.length, monthQualified: monthLeaders.length,
  earlyExitsExcluded: false
};
await fs.writeFile("status.json", JSON.stringify(status, null, 2) + "\n");
await fs.writeFile("settlement-audit-status.json", JSON.stringify(status, null, 2) + "\n");
console.log("Published settlement-qualified Top 100: week=" + weekLeaders.length + ", month=" + monthLeaders.length + ".");
