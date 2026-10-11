
import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const LEADERBOARD_SIZE = 1000;
const LEADERBOARD_PAGE_SIZE = 100;
const COMBO_PAGE_SIZE = 100;
const COMBO_PAGES_PER_TRADER = 2;
const CONCURRENCY = 8;
const REQUEST_DELAY_MS = 100;
const MIN_SHARED_TRADERS = 2;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function asArray(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.data)) return body.data;
  if (Array.isArray(body?.results)) return body.results;
  return [];
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function timestampMs(value) {
  if (value === null || value === undefined || value === "") return null;

  if (typeof value === "number" || /^\d+(\.\d+)?$/.test(String(value))) {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return n < 100000000000 ? n * 1000 : n;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function getJson(url, attempts = 4) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Combo-Scanner/3.0"
        },
        signal: AbortSignal.timeout(15000)
      });

      if (response.status === 429 || response.status === 503) {
        const retryAfter = Number(response.headers.get("retry-after"));
        const delay = Number.isFinite(retryAfter) && retryAfter > 0
          ? Math.min(retryAfter * 1000, 15000)
          : 750 * (attempt + 1);

        await sleep(delay);
        throw new Error(`HTTP ${response.status}`);
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
      }

      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) await sleep(300 * (attempt + 1));
    }
  }

  throw lastError || new Error("Request failed");
}

function walletOf(row) {
  return String(
    row?.proxyWallet ??
    row?.proxy_wallet ??
    row?.user?.proxyWallet ??
    row?.address ??
    row?.wallet ??
    ""
  ).toLowerCase();
}

function normalizeLeader(row, fallbackRank) {
  const wallet = walletOf(row);

  return {
    rank: numberOrNull(row.rank) ?? fallbackRank,
    wallet,
    name:
      row.userName ??
      row.user_name ??
      row.username ??
      row.name ??
      row.pseudonym ??
      (wallet ? `${wallet.slice(0, 8)}…${wallet.slice(-4)}` : "Unknown"),
    pnl: numberOrNull(row.pnl),
    volume: numberOrNull(row.vol ?? row.volume)
  };
}

async function getLeaderboard() {
  const traders = [];
  const seen = new Set();
  let cursor = null;

  while (traders.length < LEADERBOARD_SIZE) {
    const params = new URLSearchParams({
      category: "OVERALL",
      timePeriod: "WEEK",
      orderBy: "PNL",
      limit: String(LEADERBOARD_PAGE_SIZE)
    });

    if (cursor) {
      params.set("cursor", cursor);
    }

    const body = await getJson(`${API}/v2/leaderboard?${params}`);
    const rows = asArray(body);

    if (!rows.length) break;

    for (const row of rows) {
      const trader = normalizeLeader(row, traders.length + 1);

      if (!trader.wallet || seen.has(trader.wallet)) continue;

      seen.add(trader.wallet);
      traders.push(trader);

      if (traders.length >= LEADERBOARD_SIZE) break;
    }

    cursor = body?.pagination?.next_cursor ?? null;

    if (!cursor || rows.length < LEADERBOARD_PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return traders.slice(0, LEADERBOARD_SIZE);
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
 * Fetch genuine combo positions for one wallet.
 * This endpoint returns the actual combo and its legs;
 * individual-market trades are never stitched together into fake combos.
 */
async function getComboPositions(trader) {
  const combos = [];
  let cursor = null;

  for (let page = 0; page < COMBO_PAGES_PER_TRADER; page++) {
    const params = new URLSearchParams({
      user: trader.wallet,
      limit: String(COMBO_PAGE_SIZE),
      sortBy: "UPDATED",
      sortDirection: "DESC"
    });

    if (cursor) params.set("cursor", cursor);

    const body = await getJson(`${API}/v2/positions/combos?${params}`);
    const rows = asArray(body);

    if (!rows.length) break;

    for (const combo of rows) {
      const comboId = String(combo.combo_condition_id ?? "").toLowerCase();
      const legs = Array.isArray(combo.legs) ? combo.legs : [];

      if (!comboId || legs.length === 0) continue;

      combos.push({
        comboId,
        comboPositionId: combo.combo_position_id ?? null,
        status: combo.status ?? "UNKNOWN",
        outcome: combo.outcome_label ?? null,
        firstEntryAt: timestampMs(combo.first_entry_at),
        updatedAt: timestampMs(combo.updated_at),
        entryCost: numberOrNull(combo.entry_cost_usdc),
        currentSize: numberOrNull(combo.current_size),
        currentValue: numberOrNull(combo.current_value_usdc),
        realizedPayout: numberOrNull(combo.realized_payout_usdc),
        redeemable: combo.redeemable === true,
        legs: legs.map(leg => ({
          title:
            leg.market?.question ??
            leg.market?.title ??
            leg.market?.event?.event_title ??
            "Unknown market",
          eventTitle: leg.market?.event?.event_title ?? null,
          outcome: leg.market?.outcome ?? leg.leg_outcome_label ?? "Unknown",
          conditionId: leg.leg_condition_id ?? null,
          status: leg.leg_status ?? "UNKNOWN",
          resolvedAt: timestampMs(leg.leg_resolved_at),
          eventSlug: leg.market?.event?.event_slug ?? null,
          marketSlug: leg.market?.slug ?? null
        })),
        trader: {
          rank: trader.rank,
          name: trader.name,
          wallet: trader.wallet,
          pnl: trader.pnl,
          volume: trader.volume
        }
      });
    }

    cursor = body?.pagination?.next_cursor ?? null;
    if (!cursor) break;
  }

  return combos;
}

function comboIsOpen(combo) {
  const status = String(combo.status || "").toUpperCase();

  return (
    status === "OPEN" ||
    status === "PARTIAL" ||
    status === "REDEEMABLE"
  );
}

function groupComboPicks(rows) {
  const groups = new Map();

  for (const combo of rows) {
    /*
     * A combo condition ID identifies the real combo contract.
     * Group only by that ID, never by similar-looking leg titles.
     */
    const key = combo.comboId;

    if (!groups.has(key)) {
      groups.set(key, {
        comboId: key,
        status: combo.status,
        outcome: combo.outcome,
        firstEntryAt: combo.firstEntryAt,
        updatedAt: combo.updatedAt,
        legs: combo.legs,
        traders: [],
        traderWallets: new Set(),
        combinedEntryCost: 0,
        combinedCurrentValue: 0,
        entryCostKnown: false,
        currentValueKnown: false
      });
    }

    const group = groups.get(key);

    if (group.traderWallets.has(combo.trader.wallet)) continue;

    group.traderWallets.add(combo.trader.wallet);

    group.traders.push({
      ...combo.trader,
      status: combo.status,
      outcome: combo.outcome,
      entryCost: combo.entryCost,
      currentValue: combo.currentValue,
      firstEntryAt: combo.firstEntryAt,
      updatedAt: combo.updatedAt,
      redeemable: combo.redeemable
    });

    if (combo.entryCost !== null) {
      group.combinedEntryCost += combo.entryCost;
      group.entryCostKnown = true;
    }

    if (combo.currentValue !== null) {
      group.combinedCurrentValue += combo.currentValue;
      group.currentValueKnown = true;
    }

    if (combo.firstEntryAt !== null) {
      group.firstEntryAt = group.firstEntryAt === null
        ? combo.firstEntryAt
        : Math.min(group.firstEntryAt, combo.firstEntryAt);
    }

    if (combo.updatedAt !== null) {
      group.updatedAt = group.updatedAt === null
        ? combo.updatedAt
        : Math.max(group.updatedAt, combo.updatedAt);
    }
  }

  return [...groups.values()]
    .map(group => {
      const traders = group.traders.sort(
        (a, b) => (a.rank ?? Infinity) - (b.rank ?? Infinity)
      );

      return {
        comboId: group.comboId,
        status: group.status,
        outcome: group.outcome,
        isOpen: traders.some(t => comboIsOpen(t)),
        firstEntryAt: group.firstEntryAt,
        updatedAt: group.updatedAt,
        legs: group.legs,
        legCount: group.legs.length,
        traderCount: traders.length,
        traders,
        combinedEntryCost: group.entryCostKnown
          ? group.combinedEntryCost
          : null,
        combinedCurrentValue: group.currentValueKnown
          ? group.combinedCurrentValue
          : null
      };
    })
    .filter(group => group.traderCount >= MIN_SHARED_TRADERS)
    .sort((a, b) =>
      Number(b.isOpen) - Number(a.isOpen) ||
      b.traderCount - a.traderCount ||
      (b.updatedAt ?? 0) - (a.updatedAt ?? 0) ||
      (b.combinedEntryCost ?? 0) - (a.combinedEntryCost ?? 0)
    );
}

async function writeJsonAtomic(filename, value) {
  const temporary = `${filename}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(value, null, 2), "utf8");
  await fs.rename(temporary, filename);
}

async function main() {
  const generatedAt = new Date().toISOString();

  console.log("Loading weekly Polymarket leaderboard...");
  const traders = await getLeaderboard();

  if (!traders.length) {
    throw new Error("Leaderboard returned no traders; no files were overwritten.");
  }

  console.log(`Loaded ${traders.length} weekly leaderboard candidates.`);
  console.log("PRIORITY: scanning actual combo positions before any record calculations.");

  let completed = 0;

  const comboResults = await mapLimit(
    traders,
    CONCURRENCY,
    async trader => {
      const combos = await getComboPositions(trader);
      completed++;

      if (completed % 25 === 0 || completed === traders.length) {
        console.log(`Combo scan: ${completed}/${traders.length}`);
      }

      return combos;
    }
  );

  const allCombos = comboResults.flatMap(result => result ?? []);
  const comboPicks = groupComboPicks(allCombos);

  const comboOutput = {
    generatedAt,
    source: "Polymarket Data API v2 /v2/positions/combos",
    rankingPool: "Weekly P&L leaderboard",
    candidateCount: traders.length,
    walletsScanned: completed,
    comboPositionsFound: allCombos.length,
    comboGroupsFound: comboPicks.length,
    minSharedTraders: MIN_SHARED_TRADERS,
    comboPicks
  };

  /*
   * Save combos immediately. The combo dashboard can load this file
   * even if a later optional task fails.
   */
  await writeJsonAtomic("combos.json", comboOutput);

  console.log(
    `SAVED combos.json: ${allCombos.length} actual combo positions; ` +
    `${comboPicks.length} combos shared by ${MIN_SHARED_TRADERS}+ traders.`
  );

  /*
   * Lightweight leaderboard output for the dashboard.
   * Win-loss values remain N/A rather than being fabricated.
   */
  const leaders = traders.slice(0, 100).map((trader, index) => ({
    ...trader,
    rank: index + 1,
    wins: null,
    losses: null,
    resolvedMarkets: null,
    winRate: null,
    recordVerified: false
  }));

  const basicOutput = {
    generatedAt,
    period: "week",
    rankingBasis: "Official Polymarket weekly P&L leaderboard",
    recordRankingEnabled: false,
    leaders,
    consensus: [],
    comboFile: "combos.json"
  };

  await writeJsonAtomic("week.json", basicOutput);
  await writeJsonAtomic("month.json", {
    ...basicOutput,
    period: "month",
    rankingBasis: "Weekly P&L leaderboard used as candidate pool; monthly win-loss records not calculated"
  });

  console.log("Finished. Combo data was saved first.");
}

main().catch(error => {
  console.error("Polymarket updater failed:", error);
  process.exitCode = 1;
});
