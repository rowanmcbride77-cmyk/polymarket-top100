import { fetchAllPages } from "./data-api.js";
import { fetchTokenResolutionIndex } from "./resolution-data.js";
import { fetchGammaMarkets } from "./gamma-api.js";

const key = value => String(value ?? "").trim().toLowerCase();
const num = value => {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};
const asMs = value => {
  if (value === null || value === undefined || value === "") return null;
  const n = num(value);
  if (n !== null) return n < 100_000_000_000 ? n * 1000 : n;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};
function isSettlementQualified(trader) {
  const wins = num(trader?.settlementWins ?? trader?.wins);
  const losses = num(trader?.settlementLosses ?? trader?.losses);
  const resolved = num(trader?.resolvedMarkets);
  const rate = num(trader?.winRate);
  const basis = String(trader?.recordBasis ?? "").toLowerCase();
  return trader?.recordComplete === true &&
    trader?.settlementRecordComplete === true &&
    wins !== null && losses !== null && resolved !== null &&
    resolved === wins + losses && resolved >= 10 &&
    rate !== null && rate >= 90 &&
    Math.abs(rate - wins / resolved * 100) <= 1e-6 &&
    basis.includes("settlement-only") && basis.includes("excluded");
}

const chunks = (values, size) => {
  const result = [];
  for (let i = 0; i < values.length; i += size) result.push(values.slice(i, i + size));
  return result;
};

function marketText(market) {
  const tags = Array.isArray(market.tags) ? market.tags.map(tag =>
    typeof tag === "string" ? tag : tag?.label ?? tag?.slug ?? tag?.name ?? "") : [];
  const events = Array.isArray(market.events) ? market.events : [];
  const eventText = events.flatMap(event => [
    event.title, event.slug, event.category, event.sport,
    ...(Array.isArray(event.tags) ? event.tags.map(tag =>
      typeof tag === "string" ? tag : tag?.label ?? tag?.slug ?? tag?.name ?? "") : [])
  ]);
  return [
    market.category, market.subcategory, market.sport, market.seriesSlug,
    market.slug, market.question, market.title, market.description,
    ...tags, ...eventText
  ].filter(Boolean).join(" ").toLowerCase();
}

export function classifyMarket(market) {
  const raw = marketText(market);
  if (/\b(politic|election|president|congress|senate|democrat|republican|parliament|governor|vote)\b/.test(raw)) return "excluded";
  if (/\b(esports|e-sports|league of legends|dota\s*2|counter.?strike|cs2|valorant|overwatch|rocket league)\b/.test(raw)) return "esports";
  if (/\b(crypto|bitcoin|btc|ethereum|eth|solana|sol|dogecoin|defi|xrp|bnb|avax)\b/.test(raw)) return "crypto";
  if (
    String(market.category ?? "").toLowerCase() === "sports" ||
    market.gameId || market.game_id || market.gameStartTime || market.game_start_time ||
    /\b(nba|wnba|nfl|mlb|nhl|ncaab|ncaaf|ncaa|mls|epl|ufc|atp|wta|pga|f1|nascar|tennis|golf|boxing|mma|football|basketball|baseball|hockey|soccer|cricket|rugby)\b/.test(raw) ||
    /\b(over|under)\s+\d+(?:\.\d+)?\s+(points?|rebounds?|assists?|yards?|receptions?|touchdowns?|strikeouts?|hits?|goals?)\b/.test(raw)
  ) return "sports";
  return "excluded";
}

function marketEndMs(market, category) {
  const events = Array.isArray(market.events) ? market.events : [];
  const start = market.eventStartTime ?? market.event_start_time ?? market.startDate ??
    market.start_date ?? events[0]?.startDate ?? events[0]?.startTime ?? null;
  const end = market.endDate ?? market.end_date ?? market.endDateIso ??
    market.resolveTime ?? market.resolve_time ?? events[0]?.endDate ?? null;
  if (category === "sports" && start) {
    const startMs = asMs(start);
    if (startMs !== null) return startMs + 4 * 60 * 60 * 1000;
  }
  if (typeof end === "string" && /^\d{4}-\d{2}-\d{2}$/.test(end)) {
    return Date.parse(end + "T23:59:59.999Z");
  }
  return asMs(end);
}

function isLivePosition(position) {
  const status = String(position.status ?? "").toUpperCase();
  const size = num(position.current_size ?? position.currentSize ?? position.size);
  return size !== null && size > 0.000001 &&
    (!status || status === "OPEN" || status === "ACTIVE") &&
    position.closed !== true && position.redeemable !== true && position.mergeable !== true;
}

async function mapLimit(items, limit, worker) {
  const output = new Array(items.length);
  let next = 0;
  async function run() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await worker(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return output;
}

export async function fetchQualifiedOpenPositions(qualifiedTraders, options = {}) {
  const fetchPages = options.fetchAllPages ?? fetchAllPages;
  const concurrency = options.positionConcurrency ?? 8;
  const results = await mapLimit(qualifiedTraders, concurrency, async trader => {
    const rows = await fetchPages("/v2/positions", {
      user: trader.wallet,
      status: "OPEN",
      sort_by: "CURRENT_VALUE",
      sort_direction: "DESC",
      limit: 1000
    }, { maxPages: options.maxPositionPages ?? 20 });
    return { wallet: key(trader.wallet), trader, positions: rows.filter(isLivePosition) };
  });
  return results;
}

export async function verifyRecentBuys(positionRows, options = {}) {
  const fetchPages = options.fetchAllPages ?? fetchAllPages;
  const nowMs = Number(options.nowMs ?? Date.now());
  const windowMinutes = options.entryWindowMinutes ?? 60;
  const cutoff = Math.floor((nowMs - windowMinutes * 60_000) / 1000);
  const byWallet = new Map();
  for (const row of positionRows) {
    if (!byWallet.has(row.wallet)) byWallet.set(row.wallet, []);
    byWallet.get(row.wallet).push(row);
  }

  // Verify recent BUY activity for a small number of wallets concurrently.
  // Serial wallet-by-wallet pagination made consensus verification a major
  // avoidable bottleneck on larger qualified leaderboards.
  const walletResults = await mapLimit([...byWallet.entries()], options.activityConcurrency ?? 4, async ([wallet, walletRows]) => {
    const walletVerified = [];
    const conditions = [...new Set(walletRows.map(row => key(row.position.condition_id ?? row.position.conditionId)).filter(Boolean))];
    const buyTotals = new Map();
    for (const batch of chunks(conditions, 20)) {
      const activity = await fetchPages("/v2/activity", {
        user: wallet,
        condition: batch.join(","),
        type: "TRADE",
        side: "BUY",
        start: cutoff,
        end: Math.floor(nowMs / 1000),
        sort_by: "TIMESTAMP",
        sort_direction: "DESC",
        limit: 1000
      }, { maxPages: options.maxActivityPagesPerWallet ?? 20 });
      for (const row of activity) {
        const condition = key(row.condition_id ?? row.conditionId);
        const token = key(row.token_id ?? row.tokenId);
        const amount = num(row.usdc_size) ?? (() => {
          const price = num(row.price), size = num(row.size);
          return price !== null && size !== null ? price * size : null;
        })();
        if (!condition || !token || amount === null || amount <= 0) continue;
        const mapKey = wallet + "|" + condition + "|" + token;
        buyTotals.set(mapKey, (buyTotals.get(mapKey) ?? 0) + amount);
      }
    }
    for (const row of walletRows) {
      const condition = key(row.position.condition_id ?? row.position.conditionId);
      const token = key(row.position.token_id ?? row.position.tokenId ?? row.position.asset);
      const mapKey = wallet + "|" + condition + "|" + token;
      const entryCost = num(row.position.entry_cost_usdc ?? row.position.total_cost_usdc) ??
        (() => {
          const average = num(row.position.avg_price ?? row.position.avgPrice);
          const shares = num(row.position.current_size ?? row.position.currentSize ?? row.position.size);
          return average !== null && shares !== null ? average * shares : null;
        })();
      if ((buyTotals.get(mapKey) ?? 0) > 0 && entryCost !== null && entryCost >= (options.minimumEntryUsd ?? 10)) {
        walletVerified.push([mapKey, {
          recentBuyUsd: buyTotals.get(mapKey),
          entryCostUsd: entryCost
        }]);
      }
    }
    return walletVerified;
  });
  return new Map(walletResults.flat());
}

export async function buildConsensusSignals(qualifiedTraders, options = {}) {
  const nowMs = Number(options.nowMs ?? Date.now());
  const horizonHours = options.horizonHours ?? 24;
  const minimumHolders = options.minimumHolders ?? 2;
  const strongHolders = options.strongHolders ?? 8;

  const trustedTraders = qualifiedTraders.filter(isSettlementQualified);
  const walletRows = await fetchQualifiedOpenPositions(trustedTraders, options);
  const allPositions = walletRows.flatMap(row => row.positions.map(position => ({ wallet: row.wallet, trader: row.trader, position })));
  const conditionIds = [...new Set(allPositions.map(row => key(row.position.condition_id ?? row.position.conditionId)).filter(Boolean))];
  if (!conditionIds.length) return { signals: [], diagnostics: { qualifiedWallets: trustedTraders.length, openPositions: 0, conditions: 0, signals: 0 } };

  const [markets, resolutions, recentBuys] = await Promise.all([
    (options.fetchGammaMarkets ?? fetchGammaMarkets)(conditionIds, options),
    (options.fetchTokenResolutionIndex ?? fetchTokenResolutionIndex)(conditionIds, options),
    verifyRecentBuys(allPositions, { ...options, nowMs })
  ]);

  const marketByCondition = new Map(markets.map(market => [
    key(market.conditionId ?? market.condition_id), market
  ]));
  const groups = new Map();
  let excludedResolved = 0;
  let excludedCategory = 0;
  let excludedHorizon = 0;
  let excludedUnverifiedToken = 0;
  let excludedClosedMarket = 0;

  for (const row of allPositions) {
    const position = row.position;
    const condition = key(position.condition_id ?? position.conditionId);
    const token = key(position.token_id ?? position.tokenId ?? position.asset);
    const market = marketByCondition.get(condition);
    const resolution = resolutions.get(token);
    if (!market || !resolution || resolution.conditionId !== condition) {
      excludedUnverifiedToken++;
      continue;
    }
    const marketClosed = market.closed === true || String(market.closed).toLowerCase() === "true";
    if (marketClosed) { excludedClosedMarket++; continue; }
    if (resolution.resolved === true) { excludedResolved++; continue; }
    const category = classifyMarket(market);
    if (category === "excluded") { excludedCategory++; continue; }
    const endMs = marketEndMs(market, category);
    if (endMs === null || endMs <= nowMs || endMs > nowMs + horizonHours * 60 * 60 * 1000) {
      excludedHorizon++;
      continue;
    }
    const verifiedBuy = recentBuys.get(row.wallet + "|" + condition + "|" + token);
    if (!verifiedBuy) continue;

    const side = String(position.outcome ?? position.outcome_name ?? "").trim();
    if (!side) continue;
    const groupKey = condition + "|" + token;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        conditionId: condition,
        tokenId: token,
        title: market.question ?? market.title ?? position.title ?? "Unknown market",
        slug: market.slug ?? position.slug ?? null,
        category,
        sport: market.sport ?? null,
        endDate: new Date(endMs).toISOString(),
        side,
        wallets: new Map(),
        totalCurrentValueUsd: 0,
        totalEntryUsd: 0
      });
    }
    const group = groups.get(groupKey);
    if (group.wallets.has(row.wallet)) continue;
    const currentValue = num(position.current_value ?? position.currentValue) ?? 0;
    const wins = num(row.trader.settlementWins ?? row.trader.wins);
    const losses = num(row.trader.settlementLosses ?? row.trader.losses);
    const resolvedMarkets = num(row.trader.resolvedMarkets);
    const winRate = num(row.trader.winRate);
    const recordBasis = String(row.trader.recordBasis ?? "");
    const recordComplete = row.trader.recordComplete === true;
    const settlementRecordComplete = row.trader.settlementRecordComplete === true;
    group.wallets.set(row.wallet, {
      wallet: row.wallet,
      name: row.trader.name ?? position.name ?? null,
      rank: row.trader.rank ?? null,
      selectedPeriodPnl: row.trader.selectedPeriodPnl ?? row.trader.pnl ?? null,
      currentValueUsd: currentValue,
      entryCostUsd: verifiedBuy.entryCostUsd,
      recentBuyUsd: verifiedBuy.recentBuyUsd,
      side: group.side,
      wins,
      losses,
      settlementWins: wins,
      settlementLosses: losses,
      resolvedMarkets,
      winRate,
      recordComplete,
      settlementRecordComplete,
      qualified: isSettlementQualified(row.trader),
      recordBasis,
      earlyExitPositions: Number(row.trader.earlyExitPositions ?? 0)
    });
    group.totalCurrentValueUsd += currentValue;
    group.totalEntryUsd += verifiedBuy.entryCostUsd;
  }

  // A condition is directional only when exactly one outcome token has
  // enough qualified wallets. If both YES and NO reach the holder threshold,
  // suppress the entire condition rather than publishing two opposing signals.
  const qualifiedTokensByCondition = new Map();
  for (const group of groups.values()) {
    if (group.wallets.size < minimumHolders) continue;
    const condition = key(group.conditionId);
    if (!qualifiedTokensByCondition.has(condition)) qualifiedTokensByCondition.set(condition, new Set());
    qualifiedTokensByCondition.get(condition).add(key(group.tokenId));
  }
  const conflictedConditions = new Set(
    [...qualifiedTokensByCondition.entries()]
      .filter(([, tokens]) => tokens.size !== 1)
      .map(([condition]) => condition)
  );

  const signals = [...groups.values()]
    .filter(group => group.wallets.size >= minimumHolders && !conflictedConditions.has(key(group.conditionId)))
    .map(group => {
      const traders = [...group.wallets.values()].sort((a, b) => b.currentValueUsd - a.currentValueUsd);
      return {
        conditionId: group.conditionId,
        tokenId: group.tokenId,
        title: group.title,
        slug: group.slug,
        category: group.category,
        sport: group.sport,
        endDate: group.endDate,
        outcome: group.side,
        side: group.side,
        marketId: group.conditionId,
        marketMetadataVerified: true,
        marketClosed: false,
        sameSide: traders.length,
        traderCount: traders.length,
        qualifiedWalletCount: traders.length,
        signalStrength: traders.length >= strongHolders ? "strong" : "watch",
        signalTier: traders.length >= strongHolders ? "Strong Signals" : "Watchlist",
        totalCurrentValueUsd: group.totalCurrentValueUsd,
        totalEntryUsd: group.totalEntryUsd,
        totalValue: group.totalCurrentValueUsd,
        totalEntry: group.totalEntryUsd,
        traders
      };
    })
    .sort((a, b) => b.qualifiedWalletCount - a.qualifiedWalletCount || b.totalCurrentValueUsd - a.totalCurrentValueUsd);

  return {
    signals,
    diagnostics: {
      qualifiedWallets: qualifiedTraders.length,
      openPositions: allPositions.length,
      conditions: conditionIds.length,
      verifiedRecentBuys: recentBuys.size,
      signals: signals.length,
      excludedResolved,
      excludedCategory,
      excludedHorizon,
      excludedUnverifiedToken,
      excludedClosedMarket,
      excludedOppositeSideConditions: conflictedConditions.size
    }
  };
}
