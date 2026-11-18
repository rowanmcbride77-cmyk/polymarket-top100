import { fetchAllPages } from "./data-api.js";

const WALLET_RE = /^0x[a-f0-9]{40}$/i;
const DEFAULT_CATEGORIES = ["sports", "esports", "crypto", "overall"];
const DEFAULT_PERIODS = ["WEEK", "MONTH"];
const DEFAULT_SORTS = ["PNL", "VOLUME"];

function walletOf(row) {
  const wallet = String(
    row?.proxyWallet ?? row?.proxy_wallet ?? row?.user_id ??
    row?.userId ?? row?.address ?? row?.wallet ?? ""
  ).trim().toLowerCase();
  return WALLET_RE.test(wallet) && wallet !== "0x0000000000000000000000000000000000000000"
    ? wallet : null;
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function nameOf(row, wallet) {
  return row?.userName ?? row?.user_name ?? row?.username ?? row?.name ??
    row?.pseudonym ?? `${wallet.slice(0, 8)}…${wallet.slice(-4)}`;
}

/**
 * Build a category-balanced discovery pool using only the requested period's
 * leaderboard rows. P&L from WEEK and MONTH is never mixed.
 */
export async function discoverWallets(options = {}) {
  const requestAllPages = options.fetchAllPages ?? fetchAllPages;
  const categories = options.categories ?? DEFAULT_CATEGORIES;
  const periods = options.periods ?? DEFAULT_PERIODS;
  const sorts = options.sorts ?? DEFAULT_SORTS;
  const totalLimit = options.totalLimit ?? 20_000;
  const pageSize = options.pageSize ?? 50;
  const categoryQuotas = options.categoryQuotas ?? {
    sports: 10_000, esports: 3_000, crypto: 4_000, overall: 3_000
  };
  const wallets = new Map();
  const diagnostics = {
    pagesFetched: 0,
    rowsFetched: 0,
    invalidWalletRows: 0,
    categoryCounts: {},
    streams: []
  };

  for (const category of categories) {
    if (wallets.size >= totalLimit) break;
    const categoryStart = new Set(wallets.keys());
    const discoveredInCategory = new Set();
    const quota = categoryQuotas[category] ?? Math.ceil(totalLimit / categories.length);

    for (const period of periods) {
      for (const sortBy of sorts) {
        if (wallets.size >= totalLimit) break;
        const categoryCount = [...wallets.keys()].filter(wallet => !categoryStart.has(wallet)).length;
        if (categoryCount >= quota) break;

        const rows = await requestAllPages("/v2/leaderboard", {
          category,
          time_period: period.toLowerCase(),
          sort_by: sortBy,
          limit: pageSize
        }, {
          maxPages: options.maxPagesPerStream ?? 1000,
          stopWhen: pageRows => {
            for (const row of pageRows) {
              const wallet = walletOf(row);
              if (wallet && !categoryStart.has(wallet)) discoveredInCategory.add(wallet);
            }
            return discoveredInCategory.size >= quota ||
              categoryStart.size + discoveredInCategory.size >= totalLimit;
          }
        });

        // fetchAllPages returns all rows but not per-page metadata; each stream
        // is counted explicitly for diagnostics using a conservative page bound.
        diagnostics.rowsFetched += rows.length;
        diagnostics.pagesFetched += Math.ceil(rows.length / pageSize);
        diagnostics.streams.push({ category, period, sortBy, rows: rows.length });

        for (const row of rows) {
          const wallet = walletOf(row);
          if (!wallet) { diagnostics.invalidWalletRows++; continue; }

          let trader = wallets.get(wallet);
          if (!trader) {
            trader = {
              wallet,
              name: nameOf(row, wallet),
              pnlByPeriod: {},
              volumeByPeriod: {},
              sourceCategories: [],
              sourceStreams: [],
              ranksByPeriod: {}
            };
            wallets.set(wallet, trader);
          }
          if (!trader.sourceCategories.includes(category)) trader.sourceCategories.push(category);
          const stream = `${category}:${period}:${sortBy}`;
          if (!trader.sourceStreams.includes(stream)) trader.sourceStreams.push(stream);

          const pnl = numberOrNull(row.pnl ?? row.profit ?? row.rankPnl ?? row.rank_pnl ??
            row.realized_pnl ?? row.realizedPnl);
          const volumeUsdc = numberOrNull(row.volume_usdc ?? row.volumeUsdc ??
            row.volumeUsd ?? row.volume_usd);
          if (sortBy === "PNL" && pnl !== null) {
            const current = trader.pnlByPeriod[period];
            if (current === undefined || pnl > current) trader.pnlByPeriod[period] = pnl;
            const rank = numberOrNull(row.rank);
            if (rank !== null && (trader.ranksByPeriod[period] === undefined ||
                rank < trader.ranksByPeriod[period])) trader.ranksByPeriod[period] = rank;
          }
          if (volumeUsdc !== null) {
            const current = trader.volumeByPeriod[period];
            if (current === undefined || volumeUsdc > current) trader.volumeByPeriod[period] = volumeUsdc;
          }
        }

        const afterStreamCount = [...wallets.keys()].filter(wallet => !categoryStart.has(wallet)).length;
        if (afterStreamCount >= quota) break;
      }
    }
    diagnostics.categoryCounts[category] =
      [...wallets.keys()].filter(wallet => !categoryStart.has(wallet)).length;
  }

  const candidates = [...wallets.values()].slice(0, totalLimit);
  if (!candidates.length) throw new Error("Wallet discovery returned zero valid EVM trader wallets.");
  return {
    candidates,
    diagnostics: {
      ...diagnostics,
      uniqueWallets: candidates.length,
      totalLimit,
      categories,
      periods,
      sorts
    }
  };
}

export function candidatesForPeriod(discovery, period) {
  const key = String(period).toUpperCase();
  return (discovery?.candidates ?? [])
    .filter(row => Number.isFinite(row.pnlByPeriod?.[key]))
    .map(row => ({
      ...row,
      pnl: row.pnlByPeriod[key],
      volume: row.volumeByPeriod?.[key] ?? null,
      period: key,
      rank: row.ranksByPeriod?.[key] ?? null
    }))
    .sort((a, b) => b.pnl - a.pnl);
}
