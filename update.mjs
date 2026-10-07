const fs = require("fs");
const path = require("path");

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(process.cwd(), "data");

const PERIODS = {
  week: 7 * 24 * 60 * 60 * 1000,
  month: 30 * 24 * 60 * 60 * 1000,
  threeMonth: 90 * 24 * 60 * 60 * 1000
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getJson(url, tries = 4) {
  let lastError;

  for (let attempt = 1; attempt <= tries; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 20000);

      const res = await fetch(url, {
        signal: controller.signal,
        headers: {
          "Accept": "application/json",
          "User-Agent": "Polymarket-Consensus-Scanner/1.0"
        }
      });

      clearTimeout(timer);

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      return await res.json();
    } catch (err) {
      lastError = err;
      if (attempt < tries) {
        await sleep(800 * attempt);
      }
    }
  }

  throw lastError;
}

async function fetchLeaderboard(timePeriod, orderBy, limit = 50) {
  const rows = [];

  for (let offset = 0; offset <= 1000 && rows.length < limit; offset += 50) {
    const url =
      `${BASE}/v1/leaderboard` +
      `?category=OVERALL` +
      `&timePeriod=${encodeURIComponent(timePeriod)}` +
      `&orderBy=${encodeURIComponent(orderBy)}` +
      `&limit=50` +
      `&offset=${offset}`;

    const data = await getJson(url);

    if (!Array.isArray(data) || !data.length) break;

    rows.push(...data);

    if (data.length < 50) break;
  }

  return rows;
}

async function buildCandidateUniverse() {
  const sets = await Promise.all([
    fetchLeaderboard("WEEK", "PNL", 1000),
    fetchLeaderboard("WEEK", "VOL", 1000),
    fetchLeaderboard("MONTH", "PNL", 1000),
    fetchLeaderboard("MONTH", "VOL", 1000),
    fetchLeaderboard("ALL", "PNL", 1000),
    fetchLeaderboard("ALL", "VOL", 1000)
  ]);

  const map = new Map();

  for (const list of sets) {
    for (const x of list) {
      const address =
        x.proxyWallet ||
        x.address ||
        x.user ||
        x.wallet;

      if (!address) continue;

      const existing = map.get(address);

      map.set(address, {
        address,
        name:
          x.userName ||
          x.username ||
          x.name ||
          existing?.name ||
          "Unknown",
        profile:
          x.profile ||
          existing?.profile ||
          null
      });
    }
  }

  return [...map.values()];
}

async function fetchClosedPositions(address) {
  const all = [];
  let cursor = null;

  for (let page = 0; page < 100; page++) {
    let url =
      `${BASE}/v2/positions` +
      `?user=${encodeURIComponent(address)}` +
      `&status=CLOSED` +
      `&sort_by=TIMESTAMP` +
      `&sort_direction=DESC`;

    if (cursor) {
      url += `&cursor=${encodeURIComponent(cursor)}`;
    }

    const result = await getJson(url);

    const rows = Array.isArray(result)
      ? result
      : Array.isArray(result?.data)
        ? result.data
        : [];

    all.push(...rows);

    const next =
      result?.pagination?.next_cursor ??
      result?.next_cursor ??
      null;

    if (!next || !rows.length) break;

    cursor = next;

    if (all.length >= 5000) break;
  }

  return all;
}

function timestampOfPosition(p) {
  const candidates = [
    p.last_event_at,
    p.timestamp,
    p.closed_at,
    p.close_timestamp
  ];

  for (const value of candidates) {
    if (value == null) continue;

    const n = Number(value);

    if (Number.isFinite(n)) {
      return n > 100000000000 ? n : n * 1000;
    }

    const d = new Date(value).getTime();

    if (Number.isFinite(d)) return d;
  }

  return 0;
}

function pnlOfPosition(p) {
  const candidates = [
    p.realized_pnl,
    p.realizedPnl,
    p.realizedPnL,
    p.cash_pnl,
    p.cashPnl,
    p.total_pnl,
    p.totalPnl
  ];

  for (const value of candidates) {
    const n = Number(value);

    if (Number.isFinite(n)) return n;
  }

  return 0;
}

function titleOfPosition(p) {
  return (
    p.title ||
    p.market_title ||
    p.marketTitle ||
    p.question ||
    "Unknown market"
  );
}

function conditionOfPosition(p) {
  return (
    p.condition_id ||
    p.conditionId ||
    p.market ||
    p.condition ||
    ""
  );
}

function outcomeOfPosition(p) {
  return (
    p.outcome ||
    p.outcome_name ||
    p.outcomeName ||
    ""
  );
}

function calculateStats(closedPositions, cutoff) {
  const relevant = closedPositions.filter(p => {
    const ts = timestampOfPosition(p);
    return ts >= cutoff;
  });

  let wins = 0;
  let losses = 0;
  let breakeven = 0;
  let pnl = 0;
  let volume = 0;

  const markets = new Set();

  for (const p of relevant) {
    const value = pnlOfPosition(p);

    pnl += value;

    const volumeCandidates = [
      p.initial_value,
      p.initialValue,
      p.entry_value,
      p.entryValue,
      p.cash_pnl != null ? Math.abs(Number(p.cash_pnl)) : 0
    ];

    for (const v of volumeCandidates) {
      const n = Number(v);
      if (Number.isFinite(n) && n > 0) {
        volume += n;
        break;
      }
    }

    const market = conditionOfPosition(p);

    if (market) markets.add(market);

    if (value > 0.000001) {
      wins++;
    } else if (value < -0.000001) {
      losses++;
    } else {
      breakeven++;
    }
  }

  const decided = wins + losses;

  if (decided === 0) {
    return {
      wins: 0,
      losses: 0,
      breakeven,
      markets: markets.size,
      winRate: null,
      pnl,
      volume
    };
  }

  return {
    wins,
    losses,
    breakeven,
    markets: markets.size,
    winRate: wins / decided * 100,
    pnl,
    volume
  };
}

function rankingSort(a, b) {
  // PRIMARY: win rate
  if ((b.winRate ?? -1) !== (a.winRate ?? -1)) {
    return (b.winRate ?? -1) - (a.winRate ?? -1);
  }

  // SECONDARY: number of wins
  if (b.wins !== a.wins) {
    return b.wins - a.wins;
  }

  // THIRD: total decided markets
  const aDecided = a.wins + a.losses;
  const bDecided = b.wins + b.losses;

  if (bDecided !== aDecided) {
    return bDecided - aDecided;
  }

  // FOURTH: P&L
  return b.pnl - a.pnl;
}

function displayName(x) {
  return (
    x.name ||
    x.username ||
    x.userName ||
    "Unknown"
  );
}

async function processCandidates(candidates) {
  const results = [];
  let completed = 0;

  const queue = [...candidates];

  async function worker() {
    while (queue.length) {
      const candidate = queue.shift();

      try {
        const closed = await fetchClosedPositions(candidate.address);

        const now = Date.now();

        const week = calculateStats(
          closed,
          now - PERIODS.week
        );

        const month = calculateStats(
          closed,
          now - PERIODS.month
        );

        const threeMonth = calculateStats(
          closed,
          now - PERIODS.threeMonth
        );

        results.push({
          address: candidate.address,
          name: displayName(candidate),
          week,
          month,
          threeMonth
        });
      } catch (err) {
        results.push({
          address: candidate.address,
          name: displayName(candidate),
          week: null,
          month: null,
          threeMonth: null,
          error: String(err)
        });
      }

      completed++;

      if (completed % 25 === 0) {
        console.log(`Processed ${completed}/${candidates.length}`);
      }
    }
  }

  const workers = [];

  for (let i = 0; i < 12; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  return results;
}

function buildLeaderboard(results, period) {
  const valid = results
    .filter(x => x[period])
    .filter(x => {
      const s = x[period];
      return s.wins + s.losses >= 10;
    })
    .sort((a, b) => rankingSort(
      {
        ...a[period],
        name: a.name
      },
      {
        ...b[period],
        name: b.name
      }
    ));

  return valid.slice(0, 100).map((x, i) => ({
    rank: i + 1,
    name: x.name,
    pnl: Number(x[period].pnl.toFixed(2)),
    volume: Number(x[period].volume.toFixed(2)),
    wins: x[period].wins,
    losses: x[period].losses,
    breakeven: x[period].breakeven,
    markets: x[period].markets,
    winRate: Number(x[period].winRate.toFixed(2))
  }));
}

async function fetchOpenPositions(address) {
  const all = [];
  let cursor = null;

  for (let page = 0; page < 20; page++) {
    let url =
      `${BASE}/v2/positions` +
      `?user=${encodeURIComponent(address)}` +
      `&status=OPEN` +
      `&sort_by=CURRENT_VALUE` +
      `&sort_direction=DESC`;

    if (cursor) {
      url += `&cursor=${encodeURIComponent(cursor)}`;
    }

    const result = await getJson(url);

    const rows = Array.isArray(result)
      ? result
      : Array.isArray(result?.data)
        ? result.data
        : [];

    all.push(...rows);

    const next =
      result?.pagination?.next_cursor ??
      result?.next_cursor ??
      null;

    if (!next || !rows.length) break;

    cursor = next;

    if (all.length >= 500) break;
  }

  return all;
}

async function fetchRecentBuys(address, sinceMs) {
  const start = Math.floor(sinceMs / 1000);

  const url =
    `${BASE}/trades` +
    `?user=${encodeURIComponent(address)}` +
    `&side=BUY` +
    `&start=${start}` +
    `&limit=500` +
    `&takerOnly=false`;

  try {
    const data = await getJson(url);
    return Array.isArray(data)
      ? data
      : Array.isArray(data?.data)
        ? data.data
        : [];
  } catch {
    return [];
  }
}

function marketKey(position) {
  return (
    position.condition_id ||
    position.conditionId ||
    position.market ||
    position.condition ||
    position.event_id ||
    position.eventId ||
    ""
  );
}

function positionSide(position) {
  const outcome = String(
    position.outcome ||
    position.outcome_name ||
    position.outcomeName ||
    ""
  ).toLowerCase();

  if (outcome === "yes") return "YES";
  if (outcome === "no") return "NO";

  return outcome.toUpperCase();
}

function positionAmount(position) {
  const candidates = [
    position.initial_value,
    position.initialValue,
    position.current_value,
    position.currentValue,
    position.cash_pnl,
    position.cashPnl
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }

  const size = Number(position.size || position.current_size || 0);
  const avg = Number(position.avg_price || position.avgPrice || 0);

  if (size > 0 && avg > 0) {
    return size * avg;
  }

  return 0;
}

function tradeTimestamp(t) {
  const n = Number(
    t.timestamp ||
    t.time ||
    t.created_at ||
    t.createdAt ||
    0
  );

  if (!Number.isFinite(n) || n <= 0) return 0;

  return n > 100000000000 ? n : n * 1000;
}

async function buildConsensus(candidates) {
  const since = Date.now() - 48 * 60 * 60 * 1000;

  // Use the first 1000 weekly candidates for consensus.
  const universe = candidates.slice(0, 1000);

  const groups = new Map();

  let completed = 0;
  const queue = [...universe];

  async function worker() {
    while (queue.length) {
      const trader = queue.shift();

      try {
        const [positions, trades] = await Promise.all([
          fetchOpenPositions(trader.address),
          fetchRecentBuys(trader.address, since)
        ]);

        const recentBuys = trades.filter(
          t => tradeTimestamp(t) >= since
        );

        for (const p of positions) {
          const key = marketKey(p);
          const side = positionSide(p);

          if (!key || !side) continue;

          const title = titleOfPosition(p);

          const buys = recentBuys.filter(t => {
            const tradeMarket =
              t.conditionId ||
              t.condition_id ||
              t.market ||
              "";

            const tradeSide = String(
              t.outcome ||
              t.outcome_name ||
              t.outcomeName ||
              ""
            ).toUpperCase();

            return (
              String(tradeMarket) === String(key) &&
              (
                tradeSide === side ||
                tradeSide === side.toLowerCase()
              )
            );
          });

          if (!buys.length) continue;

          const firstBuy = buys
            .map(tradeTimestamp)
            .filter(Boolean)
            .sort((a, b) => a - b)[0];

          if (!firstBuy) continue;

          let group = groups.get(key);

          if (!group) {
            group = {
              market: key,
              title,
              sides: {
                YES: new Map(),
                NO: new Map()
              }
            };

            groups.set(key, group);
          }

          if (!group.sides[side]) continue;

          if (!group.sides[side].has(trader.address)) {
            group.sides[side].set(trader.address, {
              address: trader.address,
              name: trader.name,
              amount: positionAmount(p),
              firstBuy,
              side
            });
          }
        }
      } catch {}

      completed++;

      if (completed % 50 === 0) {
        console.log(`Consensus processed ${completed}/${universe.length}`);
      }
    }
  }

  const workers = [];

  for (let i = 0; i < 15; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  const output = [];

  for (const group of groups.values()) {
    for (const side of ["YES", "NO"]) {
      const members = [...group.sides[side].values()];

      if (members.length < 2) continue;

      const opposite =
        side === "YES"
          ? [...group.sides.NO.values()]
          : [...group.sides.YES.values()];

      // Only show pure same-side consensus.
      if (opposite.length > 0) continue;

      members.sort((a, b) => b.amount - a.amount);

      const totalEntry = members.reduce(
        (sum, x) => sum + Number(x.amount || 0),
        0
      );

      const firstBuy = Math.min(
        ...members.map(x => x.firstBuy)
      );

      output.push({
        market: group.market,
        title: group.title,
        side,
        sameSide: members.length,
        oppositeSide: 0,
        totalEntry: Number(totalEntry.toFixed(2)),
        firstBuy: new Date(firstBuy).toISOString(),
        holdLabel: "Bought within 48h",
        traders: members.map(x => ({
          name: x.name,
          side: x.side,
          amount: Number(x.amount.toFixed(2)),
          firstBuy: new Date(x.firstBuy).toISOString()
        }))
      });
    }
  }

  output.sort((a, b) => {
    if (b.sameSide !== a.sameSide) {
      return b.sameSide - a.sameSide;
    }

    return b.totalEntry - a.totalEntry;
  });

  return output.slice(0, 500);
}

function writeJson(filename, data) {
  fs.writeFileSync(
    path.join(DATA_DIR, filename),
    JSON.stringify(data, null, 2)
  );
}

async function main() {
  fs.mkdirSync(DATA_DIR, { recursive: true });

  console.log("Building candidate universe...");
  const candidates = await buildCandidateUniverse();

  console.log(`Candidate wallets: ${candidates.length}`);

  console.log("Calculating closed-position records...");
  const results = await processCandidates(candidates);

  console.log("Building consensus...");
  const consensus = await buildConsensus(candidates);

  const generatedAt = new Date().toISOString();

  const week = buildLeaderboard(results, "week");
  const month = buildLeaderboard(results, "month");
  const threeMonth = buildLeaderboard(results, "threeMonth");

  const base = {
    generatedAt,
    consensus,
    rankingMethod:
      "Win rate first, then wins, then decided markets, then P&L",
    minimumDecidedMarkets: 10
  };

  writeJson("week.json", {
    ...base,
    period: "week",
    periodLabel: "1 Week",
    leaders: week
  });

  writeJson("month.json", {
    ...base,
    period: "month",
    periodLabel: "1 Month",
    leaders: month
  });

  writeJson("threeMonth.json", {
    ...base,
    period: "threeMonth",
    periodLabel: "3 Months",
    leaders: threeMonth
  });

  writeJson("status.json", {
    ok: true,
    generatedAt,
    candidateWallets: candidates.length,
    walletsProcessed: results.length,
    weekLeaders: week.length,
    monthLeaders: month.length,
    threeMonthLeaders: threeMonth.length,
    consensusMarkets: consensus.length
  });

  console.log("DONE");
  console.log(`Week leaders: ${week.length}`);
  console.log(`Month leaders: ${month.length}`);
  console.log(`3-month leaders: ${threeMonth.length}`);
  console.log(`Consensus markets: ${consensus.length}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
