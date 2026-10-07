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

      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Scanner/1.0"
        }
      });

      clearTimeout(timer);

      if (response.status === 429) {
        await sleep(1000 * (i + 1));
        continue;
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();

    } catch (error) {
      last = error;

      if (i < attempts - 1) {
        await sleep(500 * (i + 1));
      }
    }
  }

  throw last;
}


/* =========================================================
   BASIC HELPERS
========================================================= */

function addressOf(x) {
  return (
    x.proxyWallet ||
    x.address ||
    x.user ||
    x.wallet ||
    ""
  );
}

function nameOf(x) {
  return (
    x.userName ||
    x.username ||
    x.name ||
    "Unknown"
  );
}

function timeOf(x) {
  const values = [
    x.timestamp,
    x.last_event_at,
    x.closed_at,
    x.close_timestamp
  ];

  for (const value of values) {
    if (
      value === undefined ||
      value === null
    ) {
      continue;
    }

    const n = Number(value);

    if (
      Number.isFinite(n) &&
      n > 0
    ) {
      return n > 100000000000
        ? n
        : n * 1000;
    }

    const d =
      new Date(value).getTime();

    if (Number.isFinite(d)) {
      return d;
    }
  }

  return 0;
}

function pnlOf(x) {
  const values = [
    x.realizedPnl,
    x.realized_pnl,
    x.realizedPnL,
    x.cashPnl,
    x.cash_pnl
  ];

  for (const value of values) {
    const n = Number(value);

    if (Number.isFinite(n)) {
      return n;
    }
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

  for (const value of [
    x.initialValue,
    x.initial_value,
    x.currentValue,
    x.current_value
  ]) {

    const n = Number(value);

    if (
      Number.isFinite(n) &&
      n > 0
    ) {
      return n;
    }
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

  if (
    size > 0 &&
    price > 0
  ) {
    return size * price;
  }

  return 0;
}


/* =========================================================
   LEADERBOARD CANDIDATES
========================================================= */

async function leaderboard() {

  const output = [];

  for (
    let offset = 0;
    offset < 1000;
    offset += 50
  ) {

    const url =
      `${BASE}/v1/leaderboard` +
      `?category=OVERALL` +
      `&timePeriod=WEEK` +
      `&orderBy=PNL` +
      `&limit=50` +
      `&offset=${offset}`;

    const data =
      await api(url);

    if (
      !Array.isArray(data) ||
      !data.length
    ) {
      break;
    }

    output.push(...data);

    if (
      data.length < 50
    ) {
      break;
    }

    await sleep(100);
  }

  return output;
}


/* =========================================================
   CLOSED POSITIONS
========================================================= */

async function positions(
  address,
  status
) {

  const url =
    `${BASE}/v2/positions` +
    `?user=${encodeURIComponent(address)}` +
    `&status=${status}` +
    `&limit=500`;

  const data =
    await api(url);

  if (Array.isArray(data)) {
    return data;
  }

  if (
    Array.isArray(data?.data)
  ) {
    return data.data;
  }

  return [];
}


/* =========================================================
   STATISTICS
========================================================= */

function stats(
  rows,
  cutoff
) {

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  const markets =
    new Set();

  for (const row of rows) {

    const timestamp =
      timeOf(row);

    if (
      timestamp &&
      timestamp < cutoff
    ) {
      continue;
    }

    const p =
      pnlOf(row);

    pnl += p;

    const market =
      marketOf(row);

    if (market) {
      markets.add(market);
    }

    if (
      p > 0.000001
    ) {
      wins++;

    } else if (
      p < -0.000001
    ) {
      losses++;
    }
  }

  const decided =
    wins + losses;

  return {

    wins,

    losses,

    markets:
      markets.size,

    pnl,

    winRate:
      decided
        ? wins / decided * 100
        : null
  };
}


/* =========================================================
   WIN-RATE RANKING
========================================================= */

function compare(
  a,
  b
) {

  const aw =
    a.winRate ?? -1;

  const bw =
    b.winRate ?? -1;

  /* 1. Win percentage */

  if (
    bw !== aw
  ) {
    return bw - aw;
  }

  /* 2. Wins */

  if (
    b.wins !== a.wins
  ) {
    return b.wins - a.wins;
  }

  /* 3. Number of decided markets */

  const ad =
    a.wins + a.losses;

  const bd =
    b.wins + b.losses;

  if (
    bd !== ad
  ) {
    return bd - ad;
  }

  /* 4. P&L */

  return b.pnl - a.pnl;
}


/* =========================================================
   CONCURRENT REQUESTS
========================================================= */

async function mapLimit(
  items,
  limit,
  fn
) {

  const results =
    new Array(items.length);

  let next = 0;

  async function worker() {

    while (true) {

      const index =
        next++;

      if (
        index >= items.length
      ) {
        return;
      }

      try {

        results[index] =
          await fn(
            items[index],
            index
          );

      } catch {

        results[index] =
          null;
      }
    }
  }

  const workers = [];

  for (
    let i = 0;
    i < limit;
    i++
  ) {
    workers.push(
      worker()
    );
  }

  await Promise.all(
    workers
  );

  return results;
}


/* =========================================================
   BUILD TRADER STATISTICS
========================================================= */

async function buildTraderStats(
  candidates
) {

  let finished = 0;

  const results =
    await mapLimit(
      candidates,
      20,
      async trader => {

        const closed =
          await positions(
            trader.address,
            "CLOSED"
          );

        finished++;

        if (
          finished % 25 === 0
        ) {

          console.log(
            `Closed positions: ${finished}/${candidates.length}`
          );
        }

        const now =
          Date.now();

        return {

          address:
            trader.address,

          name:
            trader.name,

          week:
            stats(
              closed,
              now -
                7 * 86400000
            ),

          month:
            stats(
              closed,
              now -
                30 * 86400000
            ),

          threeMonth:
            stats(
              closed,
              now -
                90 * 86400000
            )
        };
      }
    );

  return results.filter(
    Boolean
  );
}


/* =========================================================
   PRIMARY TOP 100
   DO NOT CHANGE THE TOP-100 LOGIC
========================================================= */

function makeLeaders(
  all,
  period
) {

  return all

    .filter(item => {

      const s =
        item[period];

      return (
        s &&
        s.wins +
          s.losses >=
          10 &&
        s.winRate !== null
      );
    })

    .sort(
      (a, b) =>
        compare(
          a[period],
          b[period]
        )
    )

    .slice(0, 100)

    .map(
      (item, index) => {

        const s =
          item[period];

        return {

          rank:
            index + 1,

          name:
            item.name,

          /* Internal only.
             The frontend does not
             display this wallet. */

          wallet:
            item.address,

          winRate:
            Number(
              s.winRate.toFixed(2)
            ),

          wins:
            s.wins,

          losses:
            s.losses,

          markets:
            s.wins +
            s.losses,

          pnl:
            Number(
              s.pnl.toFixed(2)
            )
        };
      }
    );
}


/* =========================================================
   SORT WEEKLY TRADERS BY WIN RATE
   THIS IS WHAT THE BOTTOM USES
========================================================= */

function weeklyTop1000(
  all
) {

  return all

    .filter(item => {

      const s =
        item.week;

      return (
        s &&
        s.wins +
          s.losses >=
          10 &&
        s.winRate !== null
      );
    })

    .sort(
      (a, b) =>
        compare(
          a.week,
          b.week
        )
    )

    .slice(
      0,
      1000
    );
}


/* =========================================================
   ACTIVE POSITIONS
========================================================= */

async function loadOpenPositions(
  traders
) {

  let finished = 0;

  const rows =
    await mapLimit(
      traders,
      20,
      async trader => {

        const open =
          await positions(
            trader.address,
            "OPEN"
          );

        finished++;

        if (
          finished % 25 === 0
        ) {

          console.log(
            `Open positions: ${finished}/${traders.length}`
          );
        }

        return {
          trader,
          open
        };
      }
    );

  return rows.filter(
    Boolean
  );
}


/* =========================================================
   TOP-100 INDIVIDUAL ACTIVE TRADES
========================================================= */

function buildTopTraderActiveTrades(
  rows
) {

  const result = {};

  for (
    const row of rows
  ) {

    const trader =
      row.trader;

    const trades = [];

    for (
      const position of row.open
    ) {

      const market =
        marketOf(position);

      const side =
        sideOf(position);

      if (!market) {
        continue;
      }

      const timestamp =
        timeOf(position);

      trades.push({

        title:
          titleOf(position),

        market,

        side:
          side || "UNKNOWN",

        amount:
          Number(
            amountOf(position)
              .toFixed(2)
          ),

        shares:
          Number(
            (
              position.size ||
              position.current_size ||
              0
            )
          ),

        buyTime:
          timestamp
            ? new Date(
                timestamp
              ).toISOString()
            : null,

        timestamp,

        currentValue:
          Number(
            (
              position.currentValue ||
              position.current_value ||
              0
            ).toFixed(2)
          ),

        pnl:
          Number(
            pnlOf(position)
              .toFixed(2)
          )
      });
    }

    trades.sort(
      (a, b) =>
        (b.timestamp || 0) -
        (a.timestamp || 0)
    );

    result[
      trader.address
    ] = trades;
  }

  return result;
}


/* =========================================================
   BOTTOM CONSENSUS
========================================================= */

function buildConsensus(
  rows
) {

  const markets =
    new Map();

  const cutoff24 =
    Date.now() -
    24 * 86400000;

  const cutoff72 =
    Date.now() -
    72 * 86400000;

  for (
    const row of rows
  ) {

    const trader =
      row.trader;

    for (
      const position of row.open
    ) {

      const market =
        marketOf(position);

      const side =
        sideOf(position);

      if (
        !market ||
        (
          side !== "YES" &&
          side !== "NO"
        )
      ) {
        continue;
      }

      const timestamp =
        timeOf(position);

      /*
       * Store the position even if
       * it is older than 72 hours.
       *
       * The frontend can then switch
       * between 24H and 72H without
       * another GitHub Action run.
       */

      if (
        !markets.has(market)
      ) {

        markets.set(
          market,
          {
            title:
              titleOf(position),

            YES:
              new Map(),

            NO:
              new Map()
          }
        );
      }

      const group =
        markets.get(
          market
        );

      /*
       * One position per trader per
       * market side.
       */

      const existing =
        group[side].get(
          trader.address
        );

      const trade = {

        name:
          trader.name,

        wallet:
          trader.address,

        side,

        amount:
          Number(
            amountOf(position)
              .toFixed(2)
          ),

        firstBuy:
          timestamp
            ? new Date(
                timestamp
              ).toISOString()
            : null,

        timestamp
      };

      /*
       * Keep the newest position
       * record if duplicates occur.
       */

      if (
        !existing ||
        timestamp >
          existing.timestamp
      ) {

        group[side].set(
          trader.address,
          trade
        );
      }
    }
  }

  const output = [];

  for (
    const group of markets.values()
  ) {

    for (
      const side of [
        "YES",
        "NO"
      ]
    ) {

      const same =
        [
          ...group[side].values()
        ];

      const opposite =
        side === "YES"
          ? group.NO.size
          : group.YES.size;

      /*
       * Same-side only.
       *
       * Never mix YES and NO.
       */

      if (
        same.length < 2
      ) {
        continue;
      }

      /*
       * If both sides have traders,
       * this is not pure consensus.
       */

      if (
        opposite > 0
      ) {
        continue;
      }

      same.sort(
        (a, b) =>
          b.amount -
          a.amount
      );

      const total =
        same.reduce(
          (sum, trader) =>
            sum +
            trader.amount,
          0
        );

      const times =
        same
          .map(
            trader =>
              trader.timestamp
          )
          .filter(Boolean);

      const newest =
        times.length
          ? Math.max(...times)
          : 0;

      const oldest =
        times.length
          ? Math.min(...times)
          : 0;

      output.push({

        title:
          group.title,

        side,

        sameSide:
          same.length,

        oppositeSide:
          0,

        totalEntry:
          Number(
            total.toFixed(2)
          ),

        firstBuy:
          oldest
            ? new Date(
                oldest
              ).toISOString()
            : null,

        newestBuy:
          newest
            ? new Date(
                newest
              ).toISOString()
            : null,

        /*
         * Frontend can filter:
         * 24 hours
         * 72 hours
         */

        active24h:
          same.filter(
            trader =>
              trader.timestamp >=
              cutoff24
          ).length,

        active72h:
          same.filter(
            trader =>
              trader.timestamp >=
              cutoff72
          ).length,

        traders:
          same.map(
            trader => ({
              name:
                trader.name,

              side:
                trader.side,

              amount:
                trader.amount,

              firstBuy:
                trader.firstBuy
            })
          )
      });
    }
  }

  return output
    .sort(
      (a, b) => {

        if (
          b.sameSide !==
          a.sameSide
        ) {
          return (
            b.sameSide -
            a.sameSide
          );
        }

        return (
          b.totalEntry -
          a.totalEntry
        );
      }
    )
    .slice(
      0,
      500
    );
}


/* =========================================================
   WRITE FILE
========================================================= */

function write(
  name,
  value
) {

  fs.writeFileSync(
    path.join(
      DATA_DIR,
      name
    ),
    JSON.stringify(
      value,
      null,
      2
    )
  );
}


/* =========================================================
   MAIN
========================================================= */

async function main() {

  fs.mkdirSync(
    DATA_DIR,
    {
      recursive: true
    }
  );

  console.log(
    "Loading weekly Polymarket leaderboard..."
  );

  const raw =
    await leaderboard();

  const candidateMap =
    new Map();

  for (
    const item of raw
  ) {

    const address =
      addressOf(item);

    if (!address) {
      continue;
    }

    if (
      !candidateMap.has(
        address
      )
    ) {

      candidateMap.set(
        address,
        {
          address,
          name:
            nameOf(item)
        }
      );
    }
  }

  const candidates =
    [
      ...candidateMap.values()
    ];

  console.log(
    `Candidate traders: ${candidates.length}`
  );

  /*
   * Calculate W/L for every
   * candidate first.
   */

  const traderStats =
    await buildTraderStats(
      candidates
    );

  console.log(
    "Building Top 100..."
  );

  const week =
    makeLeaders(
      traderStats,
      "week"
    );

  const month =
    makeLeaders(
      traderStats,
      "month"
    );

  const threeMonth =
    makeLeaders(
      traderStats,
      "threeMonth"
    );

  /*
   * THIS IS THE IMPORTANT FIX:
   *
   * Sort the calculated weekly
   * W/L records first.
   *
   * Do NOT use the original
   * P&L leaderboard order.
   */

  const top1000 =
    weeklyTop1000(
      traderStats
    );

  console.log(
    `Weekly win-rate Top 1,000: ${top1000.length}`
  );

  /*
   * Load open positions ONLY for
   * the actual weekly win-rate
   * Top 1,000.
   */

  const openRows =
    await loadOpenPositions(
      top1000
    );

  /*
   * Top-100 expandable active
   * trader data.
   */

  const top100Addresses =
    new Set(
      week.map(
        trader =>
          trader.wallet
      )
    );

  const top100Rows =
    openRows.filter(
      row =>
        top100Addresses.has(
          row.trader.address
        )
    );

  const traderActiveTrades =
    buildTopTraderActiveTrades(
      top100Rows
    );

  /*
   * Bottom consensus is now
   * built ONLY from the weekly
   * win-rate Top 1,000.
   */

  const consensus =
    buildConsensus(
      openRows
    );

  const generatedAt =
    new Date().toISOString();

  const base = {

    generatedAt,

    consensus,

    rankingMethod:
      "Win rate → wins → decided markets → P&L",

    minimumDecidedMarkets:
      10,

    activeTraderUniverse:
      "Weekly win-rate Top 1,000",

    activeTraderUniverseSize:
      top1000.length,

    activeTradeWindows:
      [
        "24h",
        "72h"
      ],

    traderActiveTrades
  };


  /* =======================================================
     WEEK
  ======================================================= */

  write(
    "week.json",
    {
      ...base,

      period:
        "week",

      periodLabel:
        "1 Week",

      leaders:
        week
    }
  );


  /* =======================================================
     MONTH
  ======================================================= */

  write(
    "month.json",
    {
      ...base,

      period:
        "month",

      periodLabel:
        "1 Month",

      leaders:
        month
    }
  );


  /* =======================================================
     THREE MONTH
  ======================================================= */

  write(
    "threeMonth.json",
    {
      ...base,

      period:
        "threeMonth",

      periodLabel:
        "3 Months",

      leaders:
        threeMonth
    }
  );


  /* =======================================================
     STATUS
  ======================================================= */

  write(
    "status.json",
    {

      ok:
        true,

      generatedAt,

      candidateWallets:
        candidates.length,

      walletsProcessed:
        traderStats.length,

      weekLeaders:
        week.length,

      monthLeaders:
        month.length,

      threeMonthLeaders:
        threeMonth.length,

      weeklyWinRateTop1000:
        top1000.length,

      activePositionsScanned:
        openRows.reduce(
          (sum, row) =>
            sum +
            row.open.length,
          0
        ),

      consensusMarkets:
        consensus.length
    }
  );


  console.log("");
  console.log(
    "================================"
  );
  console.log(
    "UPDATE COMPLETE"
  );
  console.log(
    "================================"
  );

  console.log(
    `Candidates: ${candidates.length}`
  );

  console.log(
    `Processed: ${traderStats.length}`
  );

  console.log(
    `Top 100: ${week.length}`
  );

  console.log(
    `Weekly W/L Top 1,000: ${top1000.length}`
  );

  console.log(
    `Active positions: ${
      openRows.reduce(
        (sum, row) =>
          sum +
          row.open.length,
        0
      )
    }`
  );

  console.log(
    `Consensus markets: ${consensus.length}`
  );
}


main().catch(
  error => {
    console.error(
      error
    );

    process.exit(1);
  }
);
