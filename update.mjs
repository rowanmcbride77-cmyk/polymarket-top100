import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const MAX_ACTIVE_TRADERS = 10000;
const ACTIVE_HOURS = 12;
const MIN_DECIDED = 10;

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function api(url, attempts = 7) {
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const controller = new AbortController();

      const timer = setTimeout(
        () => controller.abort(),
        20000
      );

      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
          "User-Agent": "Polymarket-Consensus-Scanner/4.0"
        }
      });

      clearTimeout(timer);

      if (response.status === 429 || response.status === 503) {
        const retry =
          Number(response.headers.get("retry-after")) || 2;

        await sleep(Math.max(1500, retry * 1000));
        continue;
      }

      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status}: ${url}`
        );
      }

      return await response.json();

    } catch (error) {
      lastError = error;

      if (attempt < attempts - 1) {
        await sleep(1000 * (attempt + 1));
      }
    }
  }

  throw lastError;
}


/* =========================================================
   HELPERS
   ========================================================= */

function num(...values) {
  for (const value of values) {
    const n = Number(value);

    if (Number.isFinite(n)) {
      return n;
    }
  }

  return 0;
}

function walletOf(x) {
  return (
    x.proxy_wallet ||
    x.proxyWallet ||
    x.address ||
    x.user ||
    x.wallet ||
    ""
  );
}

function nameOf(x) {
  return (
    x.user_name ||
    x.userName ||
    x.username ||
    x.name ||
    "Unknown"
  );
}

function marketOf(x) {
  return (
    x.condition_id ||
    x.conditionId ||
    x.condition ||
    x.market ||
    ""
  );
}

function titleOf(x) {
  return (
    x.title ||
    x.market_title ||
    x.marketTitle ||
    x.question ||
    x.name ||
    "Unknown market"
  );
}

function outcomeOf(x) {
  return String(
    x.outcome ||
    x.outcome_name ||
    x.outcomeName ||
    ""
  ).toUpperCase();
}

function sideOf(x) {
  return String(
    x.side ||
    ""
  ).toUpperCase();
}

function timeOf(x) {
  const values = [
    x.timestamp,
    x.last_event_at,
    x.lastEventAt,
    x.time
  ];

  for (const value of values) {
    if (
      value === undefined ||
      value === null ||
      value === ""
    ) {
      continue;
    }

    const n = Number(value);

    if (Number.isFinite(n) && n > 0) {
      return n > 100000000000
        ? n
        : n * 1000;
    }

    const d = new Date(value).getTime();

    if (Number.isFinite(d)) {
      return d;
    }
  }

  return 0;
}


/*
  IMPORTANT:
  This is actual trade dollars.

  We prefer an explicit USDC field if Polymarket
  supplies one. Otherwise:

      shares × price

  gives the cash amount of the fill.
*/

function tradeDollars(x) {

  const explicit = [
    x.usdc_size,
    x.usdcSize,
    x.cash_value,
    x.cashValue
  ];

  for (const value of explicit) {
    const n = Number(value);

    if (
      Number.isFinite(n) &&
      n >= 0
    ) {
      return n;
    }
  }

  const size =
    num(
      x.size,
      x.amount
    );

  const price =
    num(x.price);

  if (
    size > 0 &&
    price > 0
  ) {
    return size * price;
  }

  return 0;
}


/* =========================================================
   CURSOR PAGINATION
   ========================================================= */

async function fetchPaged(
  endpoint,
  params,
  maxPages = 100
) {

  const result = [];

  let cursor = null;

  for (
    let page = 0;
    page < maxPages;
    page++
  ) {

    const query =
      new URLSearchParams();

    for (
      const [key, value]
      of Object.entries(params)
    ) {
      if (
        value !== undefined &&
        value !== null &&
        value !== ""
      ) {
        query.set(
          key,
          String(value)
        );
      }
    }

    if (cursor) {
      query.set(
        "cursor",
        cursor
      );
    }

    const response =
      await api(
        `${BASE}${endpoint}?${query.toString()}`
      );

    const rows =
      Array.isArray(response)
        ? response
        : Array.isArray(response?.data)
          ? response.data
          : [];

    result.push(...rows);

    const pagination =
      response?.pagination;

    const next =
      pagination?.next_cursor ||
      null;

    if (
      !pagination?.has_more ||
      !next ||
      !rows.length
    ) {
      break;
    }

    cursor = next;

    await sleep(80);
  }

  return result;
}


/* =========================================================
   DISCOVER ACTIVE TRADERS
   =========================================================

   We intentionally use the actual trade feed.

   We collect BUY/SELL traders who have traded
   during the last 12 hours, then calculate their
   weekly results.

   This lets us get far beyond the official
   leaderboard's 1,000-row ceiling.
*/

async function discoverRecentTraders() {

  const now =
    Math.floor(Date.now() / 1000);

  const start =
    now -
    ACTIVE_HOURS * 3600;

  console.log(
    `Discovering traders active in last ${ACTIVE_HOURS} hours...`
  );

  const trades =
    await fetchPaged(
      "/v2/trades",
      {
        start,
        end: now,
        limit: 1000
      },
      200
    );

  const traders =
    new Map();

  for (const trade of trades) {

    const wallet =
      walletOf(trade);

    if (!wallet) {
      continue;
    }

    if (!traders.has(wallet)) {

      traders.set(
        wallet,
        {
          address: wallet,
          name: nameOf(trade)
        }
      );
    }
  }

  console.log(
    `Recent active wallets discovered: ${traders.size}`
  );

  return [
    ...traders.values()
  ];
}


/* =========================================================
   CLOSED POSITIONS / WEEKLY RESULTS
   ========================================================= */

async function closedPositions(
  wallet
) {

  const now =
    Math.floor(Date.now() / 1000);

  const start =
    now -
    7 * 86400;

  return await fetchPaged(
    "/v2/positions",
    {
      user: wallet,
      status: "CLOSED",
      start,
      end: now,
      limit: 1000,
      sort_by: "TIMESTAMP",
      sort_direction: "DESC"
    },
    50
  );
}


/*
  A closed position is treated as:

      realized_pnl > 0 = win
      realized_pnl < 0 = loss

  This is the same basic win/loss logic used
  by your existing Top 100.
*/

function weeklyStats(
  rows
) {

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  const markets =
    new Set();

  for (const row of rows) {

    const p =
      num(
        row.realized_pnl,
        row.realizedPnl,
        row.cash_pnl,
        row.cashPnl
      );

    pnl += p;

    const market =
      marketOf(row);

    if (market) {
      markets.add(market);
    }

    if (p > 0.000001) {
      wins++;
    }

    if (p < -0.000001) {
      losses++;
    }
  }

  const decided =
    wins + losses;

  return {
    wins,
    losses,
    markets: markets.size,
    pnl,
    winRate:
      decided >= MIN_DECIDED
        ? wins / decided * 100
        : null
  };
}


/* =========================================================
   CONCURRENCY
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

      } catch (error) {

        console.error(
          `Trader error: ${error?.message || error}`
        );

        results[index] = null;
      }
    }
  }

  const workers = [];

  for (
    let i = 0;
    i < limit;
    i++
  ) {
    workers.push(worker());
  }

  await Promise.all(workers);

  return results;
}


/* =========================================================
   BUILD WEEKLY WIN-RATE RANKING
   ========================================================= */

async function buildWeeklyUniverse(
  candidates
) {

  let completed = 0;

  const rows =
    await mapLimit(
      candidates,
      25,
      async trader => {

        const closed =
          await closedPositions(
            trader.address
          );

        completed++;

        if (
          completed % 100 === 0
        ) {
          console.log(
            `Weekly stats: ${completed}/${candidates.length}`
          );
        }

        const stats =
          weeklyStats(closed);

        if (
          stats.winRate === null
        ) {
          return null;
        }

        return {
          ...trader,
          ...stats
        };
      }
    );

  const valid =
    rows.filter(Boolean);

  valid.sort(
    (a,b) => {

      if (
        b.winRate !==
        a.winRate
      ) {
        return (
          b.winRate -
          a.winRate
        );
      }

      if (
        b.wins !==
        a.wins
      ) {
        return (
          b.wins -
          a.wins
        );
      }

      const ad =
        a.wins +
        a.losses;

      const bd =
        b.wins +
        b.losses;

      if (
        bd !== ad
      ) {
        return bd - ad;
      }

      return b.pnl - a.pnl;
    }
  );

  return valid
    .slice(
      0,
      MAX_ACTIVE_TRADERS
    )
    .map(
      (trader,index) => ({
        ...trader,
        weeklyRank:
          index + 1
      })
    );
}


/* =========================================================
   RECENT ACTUAL TRADES FOR TOP 10K
   ========================================================= */

async function recentUserTrades(
  wallet
) {

  const now =
    Math.floor(
      Date.now() / 1000
    );

  const start =
    now -
    ACTIVE_HOURS * 3600;

  return await fetchPaged(
    "/v2/trades",
    {
      user: wallet,
      start,
      end: now,
      limit: 1000
    },
    20
  );
}


/* =========================================================
   CURRENT OPEN POSITIONS
   ========================================================= */

async function openPositions(
  wallet
) {

  return await fetchPaged(
    "/v2/positions",
    {
      user: wallet,
      status: "OPEN",
      limit: 1000,
      sort_by: "TIMESTAMP",
      sort_direction: "DESC"
    },
    20
  );
}


/* =========================================================
   BUILD ACTIVE TRADER DATA
   ========================================================= */

function buildActiveData(
  trader,
  open,
  trades
) {

  /*
    Group actual BUY trades by
    market + token/outcome.

    We want the actual recent buy,
    not the position's last_event_at.
  */

  const buys =
    new Map();

  for (const trade of trades) {

    if (
      sideOf(trade) !==
      "BUY"
    ) {
      continue;
    }

    const market =
      marketOf(trade);

    const outcome =
      outcomeOf(trade);

    const timestamp =
      timeOf(trade);

    if (
      !market ||
      !outcome ||
      !timestamp
    ) {
      continue;
    }

    const key =
      `${market}|${outcome}`;

    const existing =
      buys.get(key);

    if (
      !existing ||
      timestamp >
      existing.timestamp
    ) {

      buys.set(
        key,
        {
          market,
          outcome,
          timestamp,
          dollars:
            tradeDollars(trade),
          title:
            titleOf(trade)
        }
      );
    }
  }


  const positions =
    [];


  for (const position of open) {

    const market =
      marketOf(position);

    const outcome =
      outcomeOf(position);

    if (
      !market ||
      !outcome
    ) {
      continue;
    }

    const key =
      `${market}|${outcome}`;

    const buy =
      buys.get(key);

    if (!buy) {
      continue;
    }

    const age =
      Date.now() -
      buy.timestamp;

    if (
      age >
      ACTIVE_HOURS * 3600000
    ) {
      continue;
    }

    positions.push({

      title:
        titleOf(position) ||
        buy.title,

      market,

      side:
        outcome,

      amount:
        Number(
          buy.dollars.toFixed(2)
        ),

      shares:
        Number(
          position.current_size ||
          position.size ||
          0
        ),

      buyTime:
        new Date(
          buy.timestamp
        ).toISOString(),

      timestamp:
        buy.timestamp
    });
  }


  positions.sort(
    (a,b) =>
      b.timestamp -
      a.timestamp
  );


  return positions;
}


/* =========================================================
   BUILD CONSENSUS
   ========================================================= */

function buildConsensus(
  rows
) {

  const markets =
    new Map();

  const cutoff =
    Date.now() -
    ACTIVE_HOURS * 3600000;


  for (const row of rows) {

    const trader =
      row.trader;

    for (
      const position
      of row.positions
    ) {

      const market =
        position.market;

      const side =
        position.side;

      if (
        !market ||
        (
          side !== "YES" &&
          side !== "NO"
        )
      ) {
        continue;
      }

      if (
        position.timestamp <
        cutoff
      ) {
        continue;
      }

      if (
        !markets.has(market)
      ) {

        markets.set(
          market,
          {
            title:
              position.title,

            YES:
              new Map(),

            NO:
              new Map()
          }
        );
      }

      const group =
        markets.get(market);

      group[side].set(
        trader.address,
        {
          name:
            trader.name,

          rank:
            trader.weeklyRank,

          winRate:
            trader.winRate,

          wins:
            trader.wins,

          losses:
            trader.losses,

          pnl:
            trader.pnl,

          side,

          amount:
            position.amount,

          buyTime:
            position.buyTime,

          timestamp:
            position.timestamp
        }
      );
    }
  }


  const output = [];


  for (
    const group
    of markets.values()
  ) {

    for (
      const side
      of ["YES","NO"]
    ) {

      const traders =
        [
          ...group[side].values()
        ];

      if (
        traders.length <
        2
      ) {
        continue;
      }

      /*
        If there are people on the
        opposite side, don't call it
        consensus.
      */

      if (
        group[
          side === "YES"
            ? "NO"
            : "YES"
        ].size > 0
      ) {
        continue;
      }


      traders.sort(
        (a,b) =>
          a.rank -
          b.rank
      );


      const total =
        traders.reduce(
          (sum,t) =>
            sum + t.amount,
          0
        );


      const timestamps =
        traders.map(
          t => t.timestamp
        );


      const newest =
        Math.max(
          ...timestamps
        );

      const oldest =
        Math.min(
          ...timestamps
        );


      output.push({

        title:
          group.title,

        side,

        sameSide:
          traders.length,

        totalEntry:
          Number(
            total.toFixed(2)
          ),

        firstBuy:
          new Date(
            oldest
          ).toISOString(),

        newestBuy:
          new Date(
            newest
          ).toISOString(),

        active12h:
          traders.length,

        holdLabel:
          `${traders.length} traders bought within 12h`,

        traders:
          traders.map(
            t => ({

              name:
                t.name,

              rank:
                t.rank,

              rankLabel:
                `#${t.rank} / ${MAX_ACTIVE_TRADERS}`,

              winRate:
                Number(
                  t.winRate.toFixed(2)
                ),

              wins:
                t.wins,

              losses:
                t.losses,

              pnl:
                Number(
                  t.pnl.toFixed(2)
                ),

              side:
                t.side,

              amount:
                Number(
                  t.amount.toFixed(2)
                ),

              firstBuy:
                t.buyTime
            })
          )
      });
    }
  }


  return output
    .sort(
      (a,b) => {

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
   TOP 100
   =========================================================

   We preserve your existing ranking method,
   but now use the same weekly universe where
   possible.
*/

function makeTop100(
  universe,
  period
) {

  if (
    period === "week"
  ) {

    return universe
      .slice(
        0,
        100
      )
      .map(
        (x,index) => ({

          rank:
            index + 1,

          name:
            x.name,

          wallet:
            x.address,

          winRate:
            Number(
              x.winRate.toFixed(2)
            ),

          wins:
            x.wins,

          losses:
            x.losses,

          markets:
            x.wins +
            x.losses,

          pnl:
            Number(
              x.pnl.toFixed(2)
            )
        })
      );
  }

  return [];
}


/* =========================================================
   WRITE
   ========================================================= */

function write(
  filename,
  value
) {

  fs.writeFileSync(
    path.join(
      DATA_DIR,
      filename
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
      recursive:true
    }
  );


  console.log("");
  console.log(
    "========================================"
  );
  console.log(
    "POLYMARKET ACTIVE CONSENSUS SCANNER"
  );
  console.log(
    "========================================"
  );


  /*
    Step 1:
    Discover people who actually traded
    recently.
  */

  const candidates =
    await discoverRecentTraders();


  if (
    !candidates.length
  ) {
    throw new Error(
      "No recent traders were discovered."
    );
  }


  /*
    Step 2:
    Calculate weekly win-rate.
  */

  console.log("");
  console.log(
    "Calculating weekly win/loss..."
  );


  const universe =
    await buildWeeklyUniverse(
      candidates
    );


  console.log(
    `Qualified weekly traders: ${universe.length}`
  );


  /*
    Step 3:
    Get actual recent trades and open
    positions for the ranked universe.
  */

  console.log("");
  console.log(
    `Scanning recent trades for ${universe.length} traders...`
  );


  let processed = 0;


  const activeRows =
    await mapLimit(
      universe,
      20,
      async trader => {

        const [
          trades,
          open
        ] =
          await Promise.all([
            recentUserTrades(
              trader.address
            ),

            openPositions(
              trader.address
            )
          ]);


        const positions =
          buildActiveData(
            trader,
            open,
            trades
          );


        processed++;


        if (
          processed % 100 === 0
        ) {
          console.log(
            `Active scan: ${processed}/${universe.length}`
          );
        }


        return {
          trader,
          positions
        };
      }
    );


  const valid =
    activeRows.filter(Boolean);


  /*
    Step 4:
    Consensus.
  */

  console.log("");
  console.log(
    "Building 12-hour consensus..."
  );


  const consensus =
    buildConsensus(
      valid
    );


  /*
    Step 5:
    Top 100.
  */

  const top100 =
    makeTop100(
      universe,
      "week"
    );


  /*
    For compatibility with your existing
    three tabs, keep the existing files.

    The weekly data is now the new accurate
    active-trader universe.

    Month / 3-month leaderboards remain
    populated from the prior files if they
    already exist.
  */

  let oldMonth = [];
  let oldThreeMonth = [];

  try {
    oldMonth =
      JSON.parse(
        fs.readFileSync(
          path.join(
            DATA_DIR,
            "month.json"
          ),
          "utf8"
        )
      ).leaders || [];
  } catch {}

  try {
    oldThreeMonth =
      JSON.parse(
        fs.readFileSync(
          path.join(
            DATA_DIR,
            "threeMonth.json"
          ),
          "utf8"
        )
      ).leaders || [];
  } catch {}


  const generatedAt =
    new Date().toISOString();


  const traderActiveTrades = {};

  for (
    const row
    of valid
  ) {

    traderActiveTrades[
      row.trader.address
    ] =
      row.positions;
  }


  const base = {

    generatedAt,

    consensus,

    rankingMethod:
      "Win rate → wins → decided markets → P&L",

    minimumDecidedMarkets:
      MIN_DECIDED,

    activeTraderUniverse:
      "Weekly win-rate traders discovered from recent Polymarket activity",

    activeTraderUniverseSize:
      universe.length,

    activeTraderUniverseTarget:
      MAX_ACTIVE_TRADERS,

    activeTradeWindow:
      "12 hours",

    traderActiveTrades

  };


  write(
    "week.json",
    {
      ...base,

      period:
        "week",

      periodLabel:
        "1 Week",

      leaders:
        top100
    }
  );


  write(
    "month.json",
    {
      ...base,

      period:
        "month",

      periodLabel:
        "1 Month",

      leaders:
        oldMonth
    }
  );


  write(
    "threeMonth.json",
    {
      ...base,

      period:
        "threeMonth",

      periodLabel:
        "3 Months",

      leaders:
        oldThreeMonth
    }
  );


  write(
    "status.json",
    {

      ok:true,

      generatedAt,

      recentTraderCandidates:
        candidates.length,

      weeklyQualifiedTraders:
        universe.length,

      weeklyTarget:
        MAX_ACTIVE_TRADERS,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradersProcessed:
        valid.length,

      activeTraderPositions:
        valid.reduce(
          (sum,row) =>
            sum +
            row.positions.length,
          0
        ),

      consensusMarkets:
        consensus.length,

      weekLeaders:
        top100.length,

      monthLeaders:
        oldMonth.length,

      threeMonthLeaders:
        oldThreeMonth.length
    }
  );


  console.log("");
  console.log(
    "========================================"
  );
  console.log(
    "UPDATE COMPLETE"
  );
  console.log(
    "========================================"
  );

  console.log(
    `Recent candidates: ${candidates.length}`
  );

  console.log(
    `Weekly qualified: ${universe.length}`
  );

  console.log(
    `Active window: ${ACTIVE_HOURS} hours`
  );

  console.log(
    `Consensus markets: ${consensus.length}`
  );
}


main().catch(
  error => {

    console.error(
      "FATAL ERROR:",
      error
    );

    process.exit(1);
  }
);
