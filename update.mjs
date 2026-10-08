import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function api(url, attempts = 6) {
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
          "User-Agent": "Polymarket-Consensus-Scanner/2.0"
        }
      });

      clearTimeout(timer);

      if (response.status === 429 || response.status === 503) {
        const retryAfter =
          Number(response.headers.get("retry-after")) || 2;

        await sleep(
          Math.max(1000, retryAfter * 1000)
        );

        continue;
      }

      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status} from ${url}`
        );
      }

      return await response.json();

    } catch (error) {
      lastError = error;

      if (attempt < attempts - 1) {
        await sleep(
          1000 * (attempt + 1)
        );
      }
    }
  }

  throw lastError;
}


/* =========================================================
   BASIC HELPERS
   ========================================================= */

function addressOf(x) {
  return (
    x.proxyWallet ||
    x.proxy_wallet ||
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
    x.user_name ||
    x.name ||
    "Unknown"
  );
}

function numberOf(...values) {
  for (const value of values) {
    const n = Number(value);

    if (Number.isFinite(n)) {
      return n;
    }
  }

  return 0;
}

function timestampOf(x) {
  const values = [
    x.timestamp,
    x.last_event_at,
    x.lastEventAt,
    x.closed_at,
    x.close_timestamp
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

    if (
      Number.isFinite(n) &&
      n > 0
    ) {
      return n > 100000000000
        ? n
        : n * 1000;
    }

    const parsed =
      new Date(value).getTime();

    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return 0;
}

function conditionOf(x) {
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

function tradeValueOf(x) {

  const explicit = [
    x.usdcSize,
    x.usdc_size,
    x.cashValue,
    x.cash_value
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
    Number(
      x.size ||
      x.amount ||
      0
    );

  const price =
    Number(
      x.price ||
      0
    );

  if (
    Number.isFinite(size) &&
    Number.isFinite(price)
  ) {
    return size * price;
  }

  return 0;
}

function positionValueOf(x) {

  const values = [
    x.currentValue,
    x.current_value,
    x.initialValue,
    x.initial_value,
    x.entryCost,
    x.entry_cost,
    x.entry_cost_usdc
  ];

  for (const value of values) {

    const n = Number(value);

    if (
      Number.isFinite(n) &&
      n >= 0
    ) {
      return n;
    }
  }

  const size =
    Number(
      x.size ||
      x.current_size ||
      0
    );

  const price =
    Number(
      x.avgPrice ||
      x.avg_price ||
      x.price ||
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
   PAGINATION
   ========================================================= */

async function pagedV2(
  pathName,
  params,
  maxPages = 100
) {
  const output = [];

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

    const result =
      await api(
        `${BASE}${pathName}?${query.toString()}`
      );

    const rows =
      Array.isArray(result)
        ? result
        : Array.isArray(result?.data)
          ? result.data
          : [];

    output.push(...rows);

    const next =
      result?.pagination?.next_cursor ||
      null;

    const hasMore =
      Boolean(
        result?.pagination?.has_more
      );

    if (
      !hasMore ||
      !next ||
      !rows.length
    ) {
      break;
    }

    cursor = next;

    await sleep(100);
  }

  return output;
}


/* =========================================================
   LEADERBOARD
   ========================================================= */

async function leaderboard() {

  const output = [];

  for (
    let offset = 0;
    offset <= 1000;
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
   POSITIONS
   ========================================================= */

async function positions(
  address,
  status
) {

  return await pagedV2(
    "/v2/positions",
    {
      user: address,
      status,
      limit: 1000,
      sortBy: "TIMESTAMP",
      sortDirection: "DESC"
    },
    50
  );
}


/* =========================================================
   CLOSED-TRADE STATS
   ========================================================= */

function pnlOf(x) {

  return numberOf(
    x.realizedPnl,
    x.realized_pnl,
    x.realizedPnL,
    x.cashPnl,
    x.cash_pnl,
    x.pnl
  );
}

function stats(
  rows,
  cutoff
) {

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  const markets =
    new Set();

  for (
    const row of rows
  ) {

    const timestamp =
      timestampOf(row);

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
      conditionOf(row);

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
    markets: markets.size,
    pnl,
    winRate:
      decided
        ? wins / decided * 100
        : null
  };
}


function compare(
  a,
  b
) {

  const aw =
    a.winRate ?? -1;

  const bw =
    b.winRate ?? -1;

  if (bw !== aw) {
    return bw - aw;
  }

  if (b.wins !== a.wins) {
    return b.wins - a.wins;
  }

  const ad =
    a.wins + a.losses;

  const bd =
    b.wins + b.losses;

  if (bd !== ad) {
    return bd - ad;
  }

  return b.pnl - a.pnl;
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
          "Worker error:",
          error?.message || error
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
   BUILD TRADER STATS
   ========================================================= */

async function buildTraderStats(
  candidates
) {

  let finished = 0;

  const results =
    await mapLimit(
      candidates,
      15,
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

  return results.filter(Boolean);
}


/* =========================================================
   TOP 100
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
      (a,b) =>
        compare(
          a[period],
          b[period]
        )
    )
    .slice(0,100)
    .map(
      (item,index) => {

        const s =
          item[period];

        return {

          rank:
            index + 1,

          name:
            item.name,

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
   WEEKLY TOP 1000
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
      (a,b) =>
        compare(
          a.week,
          b.week
        )
    )
    .slice(0,1000);
}


/* =========================================================
   ACTUAL TRADES
   ========================================================= */

async function recentTrades(
  address
) {

  const now =
    Math.floor(
      Date.now() / 1000
    );

  /*
    We need enough history to find the
    latest BUY for currently-held positions.

    The v2 user trade feed supports time
    bounds and cursor pagination.
  */

  const start =
    now -
    90 * 86400;

  return await pagedV2(
    "/v2/trades",
    {
      user: address,
      start,
      end: now,
      limit: 500
    },
    30
  );
}


/* =========================================================
   MATCH OPEN POSITIONS TO ACTUAL BUY TRADES
   ========================================================= */

function buildActiveTrades(
  open,
  trades,
  trader
) {

  const byMarketSide =
    new Map();

  /*
    Build the latest BUY for every
    market + outcome combination.
  */

  for (
    const trade of trades
  ) {

    const side =
      sideOf(trade);

    if (
      side !== "BUY"
    ) {
      continue;
    }

    const market =
      conditionOf(trade);

    const outcome =
      outcomeOf(trade);

    if (
      !market ||
      !outcome
    ) {
      continue;
    }

    const key =
      `${market}|${outcome}`;

    const timestamp =
      timestampOf(trade);

    if (!timestamp) {
      continue;
    }

    const existing =
      byMarketSide.get(key);

    if (
      !existing ||
      timestamp >
      existing.timestamp
    ) {

      byMarketSide.set(
        key,
        {
          market,
          outcome,
          timestamp,
          tradeValue:
            tradeValueOf(trade),
          title:
            titleOf(trade)
        }
      );
    }
  }


  const result = [];


  for (
    const position of open
  ) {

    const market =
      conditionOf(position);

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
      byMarketSide.get(key);

    if (!buy) {
      continue;
    }

    /*
      Only include positions for which
      the actual latest BUY is recent
      enough for this scanner.
    */

    const ageHours =
      (
        Date.now() -
        buy.timestamp
      ) /
      3600000;

    if (
      ageHours > 72
    ) {
      continue;
    }

    result.push({

      title:
        titleOf(position) ||
        buy.title,

      market,

      side:
        outcome,

      amount:
        Number(
          positionValueOf(position)
            .toFixed(2)
        ),

      shares:
        Number(
          position.size ||
          position.current_size ||
          0
        ),

      buyTime:
        new Date(
          buy.timestamp
        ).toISOString(),

      timestamp:
        buy.timestamp,

      currentValue:
        Number(
          positionValueOf(position)
            .toFixed(2)
        ),

      pnl:
        Number(
          pnlOf(position)
            .toFixed(2)
        )
    });
  }


  result.sort(
    (a,b) =>
      b.timestamp -
      a.timestamp
  );


  return result;
}


/* =========================================================
   CONSENSUS
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

    const trades =
      row.trades;

    const open =
      row.open;


    /*
      Latest actual BUY per
      market + outcome.
    */

    const latestBuys =
      new Map();


    for (
      const trade of trades
    ) {

      if (
        sideOf(trade) !==
        "BUY"
      ) {
        continue;
      }

      const market =
        conditionOf(trade);

      const outcome =
        outcomeOf(trade);

      const timestamp =
        timestampOf(trade);

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
        latestBuys.get(key);

      if (
        !existing ||
        timestamp >
        existing.timestamp
      ) {

        latestBuys.set(
          key,
          {
            timestamp,

            amount:
              tradeValueOf(trade),

            title:
              titleOf(trade),

            market,

            outcome
          }
        );
      }
    }


    /*
      Only count an actual BUY if the
      trader still has that position OPEN.
    */

    for (
      const position of open
    ) {

      const market =
        conditionOf(position);

      const outcome =
        outcomeOf(position);

      if (
        !market ||
        (
          outcome !== "YES" &&
          outcome !== "NO"
        )
      ) {
        continue;
      }

      const key =
        `${market}|${outcome}`;

      const buy =
        latestBuys.get(key);

      if (!buy) {
        continue;
      }

      /*
        Ignore positions whose latest
        BUY is older than 72 hours.

        This prevents old positions such
        as 2028 markets from dominating
        the active scanner.
      */

      if (
        buy.timestamp <
        cutoff72
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
              titleOf(position) ||
              buy.title,

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


      group[outcome].set(
        trader.address,
        {
          name:
            trader.name,

          wallet:
            trader.address,

          side:
            outcome,

          amount:
            Number(
              positionValueOf(position)
                .toFixed(2)
            ),

          firstBuy:
            new Date(
              buy.timestamp
            ).toISOString(),

          timestamp:
            buy.timestamp
        }
      );
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

      if (
        same.length < 2
      ) {
        continue;
      }


      /*
        Never mix YES and NO.
      */

      same.sort(
        (a,b) =>
          b.amount -
          a.amount
      );


      const total =
        same.reduce(
          (sum,trader) =>
            sum +
            trader.amount,
          0
        );


      const timestamps =
        same
          .map(
            trader =>
              trader.timestamp
          )
          .filter(Boolean);


      const newest =
        timestamps.length
          ? Math.max(...timestamps)
          : 0;


      const oldest =
        timestamps.length
          ? Math.min(...timestamps)
          : 0;


      const active24 =
        same.filter(
          trader =>
            trader.timestamp >=
            cutoff24
        ).length;


      const active72 =
        same.filter(
          trader =>
            trader.timestamp >=
            cutoff72
        ).length;


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

        active24h:
          active24,

        active72h:
          active72,

        holdLabel:
          active24 > 0
            ? `${active24} bought in 24h`
            : `${active72} bought in 72h`,

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
      (a,b) => {

        if (
          b.active24h !==
          a.active24h
        ) {
          return (
            b.active24h -
            a.active24h
          );
        }

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
    .slice(0,500);
}


/* =========================================================
   TOP-100 ACTIVE TRADES
   ========================================================= */

function buildTopTraderActiveTrades(
  rows
) {

  const result = {};

  for (
    const row of rows
  ) {

    result[
      row.trader.address
    ] =
      buildActiveTrades(
        row.open,
        row.trades,
        row.trader
      );
  }

  return result;
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


  console.log(
    "Loading Polymarket leaderboard..."
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
      !candidateMap.has(address)
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


  const top1000 =
    weeklyTop1000(
      traderStats
    );


  console.log(
    `Weekly win-rate Top 1,000: ${top1000.length}`
  );


  /*
    Load current positions and actual
    trade histories for the weekly
    win-rate universe.
  */

  let finished =
    0;


  const openRows =
    await mapLimit(
      top1000,
      12,
      async trader => {

        const open =
          await positions(
            trader.address,
            "OPEN"
          );


        const trades =
          await recentTrades(
            trader.address
          );


        finished++;


        if (
          finished % 25 === 0
        ) {

          console.log(
            `Active traders processed: ${finished}/${top1000.length}`
          );
        }


        return {

          trader,

          open,

          trades
        };
      }
    );


  const validOpenRows =
    openRows.filter(Boolean);


  const top100Addresses =
    new Set(
      week.map(
        trader =>
          trader.wallet
      )
    );


  const top100Rows =
    validOpenRows.filter(
      row =>
        top100Addresses.has(
          row.trader.address
        )
    );


  console.log(
    "Building actual active trade data..."
  );


  const traderActiveTrades =
    buildTopTraderActiveTrades(
      top100Rows
    );


  console.log(
    "Building consensus..."
  );


  const consensus =
    buildConsensus(
      validOpenRows
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


  write(
    "status.json",
    {

      ok:true,

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

      activeTradersProcessed:
        validOpenRows.length,

      activePositionsScanned:
        validOpenRows.reduce(
          (sum,row) =>
            sum +
            row.open.length,
          0
        ),

      actualTradesScanned:
        validOpenRows.reduce(
          (sum,row) =>
            sum +
            row.trades.length,
          0
        ),

      top100ActiveTraders:
        Object.keys(
          traderActiveTrades
        ).length,

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
    `Active traders processed: ${validOpenRows.length}`
  );

  console.log(
    `Actual trades scanned: ${
      validOpenRows.reduce(
        (sum,row) =>
          sum +
          row.trades.length,
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
      "FATAL:",
      error
    );

    process.exit(1);
  }
);
