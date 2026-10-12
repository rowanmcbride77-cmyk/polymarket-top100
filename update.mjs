
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";

const CANDIDATE_COUNT = 1000;
const LEADER_COUNT = 100;
const PAGE_SIZE = 50;
const COMBO_PAGE_SIZE = 100;
const COMBO_WALLETS = 1000;
const COMBO_CONCURRENCY = 10;
const REQUEST_TIMEOUT_MS = 12000;
const REQUEST_ATTEMPTS = 3;
const REQUEST_DELAY_MS = 60;

const PERIODS = [
  { key: "week", api: "WEEK", days: 7 },
  { key: "month", api: "MONTH", days: 30 }
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function asArray(value) {
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

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const n = Number(value);
    return n < 1e12 ? n * 1000 : n;
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

function normalizeLeader(row, fallbackRank) {
  const wallet = walletOf(row);

  return {
    rank: num(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ||
      row.user_name ||
      row.username ||
      row.name ||
      row.pseudonym ||
      "Unknown trader",
    pnl: num(row.pnl),
    volume: num(row.vol ?? row.volume),
    wins: null,
    losses: null,
    winRate: null,
    resolvedMarkets: null,
    recordVerified: false
  };
}

/*
 * Every network request has a timeout and bounded retries.
 * A failed request cannot hang the workflow indefinitely.
 */
async function getJson(url, attempts = REQUEST_ATTEMPTS) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "PolymarketConsensusScanner/2.0"
        },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
      });

      if (!response.ok) {
        const body = await response.text().catch(() => "");
        const error = new Error(
          `HTTP ${response.status} ${url}: ${body.slice(0, 160)}`
        );

        if (
          response.status !== 429 &&
          response.status < 500
        ) {
          throw Object.assign(error, { permanent: true });
        }

        const retryAfter = Number(response.headers.get("retry-after"));
        await sleep(
          Number.isFinite(retryAfter) && retryAfter > 0
            ? Math.min(retryAfter * 1000, 5000)
            : attempt * 500
        );

        throw error;
      }

      return await response.json();
    } catch (error) {
      lastError = error;

      if (error.permanent || attempt === attempts) break;

      console.warn(
        `Request attempt ${attempt}/${attempts} failed: ${error.message}`
      );

      await sleep(attempt * 400);
    }
  }

  throw lastError || new Error("Request failed");
}

/*
 * Leaderboard requests use the stable v1 endpoint with offset
 * pagination. If one leaderboard period fails, the script exits
 * with a useful error instead of silently producing fake data.
 */
async function getCandidateLeaderboard(period) {
  const candidates = [];
  const seen = new Set();

  console.log(`Loading ${period.key} Polymarket leaderboard...`);

  for (let offset = 0; offset < CANDIDATE_COUNT; offset += PAGE_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: period.api,
      orderBy: "PNL",
      limit: String(PAGE_SIZE),
      offset: String(offset)
    });

    let rows;

    try {
      rows = asArray(
        await getJson(`${API}/v1/leaderboard?${params}`)
      );
    } catch (error) {
      throw new Error(
        `Could not load ${period.key} leaderboard at offset ${offset}: ` +
        `${error.message}`
      );
    }

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeLeader(row, candidates.length + 1);
      if (!trader.wallet || seen.has(trader.wallet)) continue;
      seen.add(trader.wallet);
      candidates.push(trader);
    }

    console.log(
      `${period.key} leaderboard: ${candidates.length}/${CANDIDATE_COUNT}`
    );

    if (rows.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return candidates.slice(0, CANDIDATE_COUNT);
}

async function mapLimit(items, limit, callback) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;

      try {
        results[index] = await callback(items[index], index);
      } catch (error) {
        results[index] = null;
        console.error(`Item ${index + 1} failed: ${error.message}`);
      }

      await sleep(REQUEST_DELAY_MS);
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

/*
 * Fetch the combo positions exposed by Polymarket's data API.
 * These are actual combo-position records, not inferred matches
 * between separate ordinary market positions.
 */
async function getComboPositions(wallet) {
  const all = [];
  const seen = new Set();

  // One page per wallet keeps the workflow bounded.
  const params = new URLSearchParams({
    user: wallet,
    limit: String(COMBO_PAGE_SIZE),
    sortBy: "UPDATED",
    sortDirection: "DESC"
  });

  const payload = await getJson(
    `${API}/v2/positions/combos?${params}`
  );

  for (const row of asArray(payload)) {
    const comboId = String(
      row.combo_condition_id ??
      row.comboConditionId ??
      row.conditionId ??
      row.condition_id ??
      ""
    );

    if (!comboId) continue;

    const legs = Array.isArray(row.legs) ? row.legs : [];
    const status = String(row.status ?? "").toUpperCase();

    const entryTime = timestampMs(
      row.first_entry_at ??
      row.firstEntryAt ??
      row.created_at ??
      row.createdAt
    );

    const updatedTime = timestampMs(
      row.updated_at ??
      row.updatedAt ??
      row.last_event_at ??
      row.lastEventAt
    );

    const key = `${wallet}|${comboId}`;
    if (seen.has(key)) continue;
    seen.add(key);

    all.push({
      comboId,
      title:
        row.title ??
        row.question ??
        row.name ??
        legs.map(leg => leg.title ?? leg.question ?? "").filter(Boolean).join(" + ") ??
        "Combo market",
      status: status || "OPEN",
      legs,
      entryTime,
      updatedTime,
      size: num(row.size ?? row.current_size ?? row.currentSize),
      currentValue: num(
        row.current_value ??
        row.currentValue ??
        row.value
      ),
      initialValue: num(
        row.initial_value ??
        row.initialValue ??
        row.cost
      ),
      wallet,
      traderName: "",
      traderRank: null
    });
  }

  return all;
}

function buildComboConsensus(walletResults) {
  const groups = new Map();

  for (const result of walletResults) {
    if (!result) continue;

    const { trader, combos } = result;

    for (const combo of combos) {
      const key = combo.comboId;

      if (!groups.has(key)) {
        groups.set(key, {
          comboId: combo.comboId,
          title: combo.title,
          status: combo.status,
          legs: combo.legs,
          entryTime: combo.entryTime,
          updatedTime: combo.updatedTime,
          traderCount: 0,
          totalValue: 0,
          traders: [],
          seenWallets: new Set()
        });
      }

      const group = groups.get(key);
      if (group.seenWallets.has(trader.wallet)) continue;

      group.seenWallets.add(trader.wallet);
      group.traderCount++;

      const value = combo.currentValue ?? combo.initialValue;
      if (value !== null) group.totalValue += value;

      group.entryTime =
        group.entryTime === null
          ? combo.entryTime
          : combo.entryTime === null
            ? group.entryTime
            : Math.min(group.entryTime, combo.entryTime);

      group.updatedTime =
        group.updatedTime === null
          ? combo.updatedTime
          : combo.updatedTime === null
            ? group.updatedTime
            : Math.max(group.updatedTime, combo.updatedTime);

      group.traders.push({
        name: trader.name,
        rank: trader.rank,
        amount: value,
        entryTime: combo.entryTime,
        updatedTime: combo.updatedTime
      });
    }
  }

  return [...groups.values()]
    .map(({ seenWallets, ...group }) => group)
    .sort((a, b) =>
      b.traderCount - a.traderCount ||
      b.totalValue - a.totalValue ||
      (b.updatedTime ?? 0) - (a.updatedTime ?? 0)
    );
}

async function writeJsonAtomic(filename, data) {
  const temp = `${filename}.tmp`;
  await fs.writeFile(temp, JSON.stringify(data, null, 2), "utf8");
  await fs.rename(temp, filename);
}

async function buildPeriod(period, candidates, nowMs) {
  /*
   * Do not invent win/loss records from leaderboard P&L.
   * This file explicitly identifies the ranking as P&L-ranked.
   */
  const leaders = candidates.slice(0, LEADER_COUNT).map((trader, i) => ({
    ...trader,
    rank: i + 1
  }));

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    period: period.key,
    rankingBasis: `Polymarket ${period.key} P&L leaderboard`,
    recordBasis: "Win/loss records are not verified by this leaderboard request",
    candidateCount: candidates.length,
    minResolvedMarketsForRecordRanking: 10,
    recordRankingEnabled: false,
    leaders,
    traderActiveTrades: {},
    traderActivePositions: {},
    consensus: []
  };

  await writeJsonAtomic(`${period.key}.json`, output);
  console.log(`Wrote ${period.key}.json (${leaders.length} leaders)`);
}

async function buildCombos(weekCandidates, nowMs) {
  const traders = weekCandidates.slice(0, COMBO_WALLETS);

  console.log(
    `Loading real combo positions for ${traders.length} weekly leaderboard traders...`
  );

  let completed = 0;

  const results = await mapLimit(
    traders,
    COMBO_CONCURRENCY,
    async trader => {
      try {
        const combos = await getComboPositions(trader.wallet);
        completed++;

        if (completed % 25 === 0 || completed === traders.length) {
          console.log(`Combo positions: ${completed}/${traders.length}`);
        }

        return { trader, combos };
      } catch (error) {
        completed++;

        console.warn(
          `Combo lookup failed for ${trader.name}: ${error.message}`
        );

        return { trader, combos: [] };
      }
    }
  );

  const consensus = buildComboConsensus(results);

  const output = {
    generatedAt: new Date(nowMs).toISOString(),
    source: "Polymarket Data API /v2/positions/combos",
    candidateCount: traders.length,
    comboCount: consensus.length,
    minimumTraders: 1,
    combos: consensus
  };

  await writeJsonAtomic("combos.json", output);

  console.log(
    `Wrote combos.json (${consensus.length} combo positions/groups)`
  );
}

async function main() {
  const nowMs = Date.now();

  console.log("Starting Polymarket tracker update...");

  // Load both pools before writing output files.
  const weekCandidates = await getCandidateLeaderboard(PERIODS[0]);
  console.log(`Weekly candidates retrieved: ${weekCandidates.length}`);

  const monthCandidates = await getCandidateLeaderboard(PERIODS[1]);
  console.log(`Monthly candidates retrieved: ${monthCandidates.length}`);

  if (!weekCandidates.length) {
    throw new Error("Weekly leaderboard returned no traders.");
  }

  if (!monthCandidates.length) {
    throw new Error("Monthly leaderboard returned no traders.");
  }

  await buildPeriod(PERIODS[0], weekCandidates, nowMs);
  await buildPeriod(PERIODS[1], monthCandidates, nowMs);
  await buildCombos(weekCandidates, nowMs);

  console.log("Polymarket tracker update completed successfully.");
}

main().catch(error => {
  console.error("Polymarket update failed:", error.stack || error.message || error);
  process.exitCode = 1;
});
