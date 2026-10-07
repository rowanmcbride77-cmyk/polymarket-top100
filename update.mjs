import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function api(url, attempts = 5) {
  let last;

  for (let i = 0; i < attempts; i++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 15000);

      const r = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Scanner/1.0"
        }
      });

      clearTimeout(timer);

      if (r.status === 429) {
        await sleep(1000 * (i + 1));
        continue;
      }

      if (!r.ok) {
        throw new Error(`HTTP ${r.status}`);
      }

      return await r.json();
    } catch (e) {
      last = e;
      if (i < attempts - 1) {
        await sleep(500 * (i + 1));
      }
    }
  }

  throw last;
}

async function leaderboard() {
  const out = [];

  for (let offset = 0; offset < 1000; offset += 50) {
    const url =
      `${BASE}/v1/leaderboard` +
      `?category=OVERALL` +
      `&timePeriod=WEEK` +
      `&orderBy=PNL` +
      `&limit=50` +
      `&offset=${offset}`;

    const data = await api(url);

    if (!Array.isArray(data) || !data.length) break;

    out.push(...data);

    if (data.length < 50) break;

    await sleep(100);
  }

  return out;
}

function addressOf(x) {
  return x.proxyWallet || x.address || x.user || x.wallet || "";
}

function nameOf(x) {
  return x.userName || x.username || x.name || "Unknown";
}

function timeOf(x) {
  const values = [
    x.timestamp,
    x.last_event_at,
    x.closed_at,
    x.close_timestamp
  ];

  for (const v of values) {
    if (v === undefined || v === null) continue;

    const n = Number(v);

    if (Number.isFinite(n) && n > 0) {
      return n > 100000000000 ? n : n * 1000;
    }

    const d = new Date(v).getTime();

    if (Number.isFinite(d)) return d;
  }

  return 0;
}

function pnlOf(x) {
  for (const v of [
    x.realizedPnl,
    x.realized_pnl,
    x.realizedPnL,
    x.cashPnl,
    x.cash_pnl
  ]) {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }

  return 0;
}

function marketOf(x) {
  return (
    x.conditionId ||
    x.condition_id ||
    x.market ||
    x.condition ||
    ""
  );
}

function titleOf(x) {
  return (
    x.title ||
    x.market_title ||
    x.marketTitle ||
    x.question ||
    "Unknown market"
  );
}

function sideOf(x) {
  return String(
    x.outcome ||
    x.outcome_name ||
    x.outcomeName ||
    ""
  ).toUpperCase();
}

function amountOf(x) {
  for (const v of [
    x.initialValue,
    x.initial_value,
    x.currentValue,
    x.current_value
  ]) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }

  const size = Number(
    x.size ||
    x.current_size ||
    0
  );

  const price = Number(
    x.avgPrice ||
    x.avg_price ||
    0
  );

  if (size > 0 && price > 0) {
    return size * price;
  }

  return 0;
}

async function positions(address, status) {
  const url =
    `${BASE}/v2/positions` +
    `?user=${encodeURIComponent(address)}` +
    `&status=${status}` +
    `&limit=500`;

  const data = await api(url);

  if (Array.isArray(data)) return data;

  if (Array.isArray(data?.data)) {
    return data.data;
  }

  return [];
}

function stats(rows, cutoff) {
  let wins = 0;
  let losses = 0;
  let pnl = 0;

  const markets = new Set();

  for (const row of rows) {
    if (timeOf(row) < cutoff) continue;

    const p = pnlOf(row);

    pnl += p;

    const market = marketOf(row);

    if (market) markets.add(market);

    if (p > 0.000001) wins++;
    else if (p < -0.000001) losses++;
  }

  const decided = wins + losses;

  return {
    wins,
    losses,
    markets: markets.size,
    pnl,
    winRate: decided
      ? wins / decided * 100
      : null
  };
}

function compare(a, b) {
  const aw = a.winRate ?? -1;
  const bw = b.winRate ?? -1;

  if (bw !== aw) return bw - aw;

  if (b.wins !== a.wins) {
    return b.wins - a.wins;
  }

  const ad = a.wins + a.losses;
  const bd = b.wins + b.losses;

  if (bd !== ad) return bd - ad;

  return b.pnl - a.pnl;
}

async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;

      if (index >= items.length) return;

      try {
        results[index] = await fn(items[index], index);
      } catch {
        results[index] = null;
      }
    }
  }

  const workers = [];

  for (let i = 0; i < limit; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  return results;
}

async function buildTraderStats(candidates) {
  let finished = 0;

  const results = await mapLimit(
    candidates,
    20,
    async trader => {
      const closed = await positions(
        trader.address,
        "CLOSED"
      );

      finished++;

      if (finished % 25 === 0) {
        console.log(
          `Closed positions: ${finished}/${candidates.length}`
        );
      }

      const now = Date.now();

      return {
        address: trader.address,
        name: trader.name,

        week: stats(
          closed,
          now - 7 * 86400000
        ),

        month: stats(
          closed,
          now - 30 * 86400000
        ),

        threeMonth: stats(
          closed,
          now - 90 * 86400000
        )
      };
    }
  );

  return results.filter(Boolean);
}

function makeLeaders(all, period) {
  return all
    .filter(x => {
      const s = x[period];

      return (
        s &&
        s.wins + s.losses >= 10 &&
        s.winRate !== null
      );
    })
    .sort((a, b) =>
      compare(a[period], b[period])
    )
    .slice(0, 100)
    .map((x, i) => {
      const s = x[period];

      return {
        rank: i + 1,
        name: x.name,
        winRate: Number(s.winRate.toFixed(2)),
        wins: s.wins,
        losses: s.losses,
        markets: s.wins + s.losses,
        pnl: Number(s.pnl.toFixed(2))
      };
    });
}

async function buildConsensus(candidates) {
  console.log(
    "Loading active positions for weekly top 1,000..."
  );

  let finished = 0;

  const rows = await mapLimit(
    candidates.slice(0, 1000),
    20,
    async trader => {
      const open = await positions(
        trader.address,
        "OPEN"
      );

      finished++;

      if (finished % 25 === 0) {
        console.log(
          `Open positions: ${finished}/${Math.min(1000, candidates.length)}`
        );
      }

      return {
        trader,
        open
      };
    }
  );

  const markets = new Map();

  for (const result of rows.filter(Boolean)) {
    const trader = result.trader;

    for (const p of result.open) {
      const market = marketOf(p);
      const side = sideOf(p);

      if (
        !market ||
        (side !== "YES" && side !== "NO")
      ) {
        continue;
      }

      const timestamp = timeOf(p);

      // Only positions observed within the last 48 hours.
      if (
        timestamp &&
        Date.now() - timestamp > 48 * 86400000
      ) {
        continue;
      }

      if (!markets.has(market)) {
        markets.set(market, {
          title: titleOf(p),
          YES: new Map(),
          NO: new Map()
        });
      }

      const group = markets.get(market);

      if (!group[side].has(trader.address)) {
        group[side].set(
          trader.address,
          {
            name: trader.name,
            side,
            amount: amountOf(p),
            firstBuy: timestamp
              ? new Date(timestamp).toISOString()
              : null
          }
        );
      }
    }
  }

  const output = [];

  for (const group of markets.values()) {
    for (const side of ["YES", "NO"]) {
      const same = [
        ...group[side].values()
      ];

      const opposite =
        side === "YES"
          ? group.NO.size
          : group.YES.size;

      if (same.length < 2) continue;
      if (opposite > 0) continue;

      same.sort(
        (a, b) => b.amount - a.amount
      );

      const total = same.reduce(
        (sum, x) => sum + x.amount,
        0
      );

      const times = same
        .map(x => x.firstBuy)
        .filter(Boolean)
        .map(x => new Date(x).getTime());

      output.push({
        title: group.title,
        side,
        sameSide: same.length,
        oppositeSide: 0,
        totalEntry: Number(total.toFixed(2)),
        firstBuy: times.length
          ? new Date(Math.min(...times)).toISOString()
          : null,
        holdLabel: "Active · recent",
        traders: same.map(x => ({
          name: x.name,
          side: x.side,
          amount: Number(x.amount.toFixed(2)),
          firstBuy: x.firstBuy
        }))
      });
    }
  }

  return output
    .sort((a, b) => {
      if (b.sameSide !== a.sameSide) {
        return b.sameSide - a.sameSide;
      }

      return b.totalEntry - a.totalEntry;
    })
    .slice(0, 500);
}

function write(name, value) {
  fs.writeFileSync(
    path.join(DATA_DIR, name),
    JSON.stringify(value, null, 2)
  );
}

async function main() {
  fs.mkdirSync(DATA_DIR, {
    recursive: true
  });

  console.log("Loading weekly Polymarket leaderboard...");

  const raw = await leaderboard();

  const candidateMap = new Map();

  for (const x of raw) {
    const address = addressOf(x);

    if (!address) continue;

    if (!candidateMap.has(address)) {
      candidateMap.set(address, {
        address,
        name: nameOf(x)
      });
    }
  }

  const candidates = [
    ...candidateMap.values()
  ];

  console.log(
    `Candidate traders: ${candidates.length}`
  );

  const traderStats =
    await buildTraderStats(candidates);

  console.log(
    "Building win-rate leaderboards..."
  );

  const week =
    makeLeaders(traderStats, "week");

  const month =
    makeLeaders(traderStats, "month");

  const threeMonth =
    makeLeaders(traderStats, "threeMonth");

  const consensus =
    await buildConsensus(candidates);

  const generatedAt =
    new Date().toISOString();

  const base = {
    generatedAt,
    consensus,
    rankingMethod:
      "Win rate → wins → decided markets → P&L",
    minimumDecidedMarkets: 10
  };

  write("week.json", {
    ...base,
    period: "week",
    periodLabel: "1 Week",
    leaders: week
  });

  write("month.json", {
    ...base,
    period: "month",
    periodLabel: "1 Month",
    leaders: month
  });

  write("threeMonth.json", {
    ...base,
    period: "threeMonth",
    periodLabel: "3 Months",
    leaders: threeMonth
  });

  write("status.json", {
    ok: true,
    generatedAt,
    candidateWallets: candidates.length,
    walletsProcessed: traderStats.length,
    weekLeaders: week.length,
    monthLeaders: month.length,
    threeMonthLeaders: threeMonth.length,
    consensusMarkets: consensus.length
  });

  console.log("");
  console.log("================================");
  console.log("UPDATE COMPLETE");
  console.log("================================");
  console.log(`Candidates: ${candidates.length}`);
  console.log(`Processed: ${traderStats.length}`);
  console.log(`Week leaders: ${week.length}`);
  console.log(`Month leaders: ${month.length}`);
  console.log(`3-month leaders: ${threeMonth.length}`);
  console.log(`Consensus markets: ${consensus.length}`);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
