import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const TARGET_WEEKLY_TRADERS = 10000;
const CANDIDATE_TARGET = 12000;

const ACTIVE_HOURS = 12;
const MIN_DECIDED = 10;

const WEEKLY_CACHE_HOURS = 2;

const REQUEST_CONCURRENCY = 20;

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));


/* =========================================================
   BASIC HELPERS
   ========================================================= */

function nowSeconds() {
  return Math.floor(Date.now() / 1000);
}

function weekKey() {
  const d = new Date();

  const first =
    new Date(
      d.getFullYear(),
      0,
      1
    );

  const days =
    Math.floor(
      (
        d -
        first
      ) /
      86400000
    );

  return (
    d.getFullYear() +
    "-W" +
    String(
      Math.floor(
        (
          days +
          first.getDay() +
          6
        ) /
        7
      )
    ).padStart(2, "0")
  );
}

function number(...values) {

  for (const value of values) {

    if (
      value === null ||
      value === undefined ||
      value === ""
    ) {
      continue;
    }

    const n = Number(value);

    if (Number.isFinite(n)) {
      return n;
    }
  }

  return 0;
}

function walletOf(row) {

  return (
    row.proxy_wallet ||
    row.proxyWallet ||
    row.user_id ||
    row.userId ||
    ""
  );
}

function nameOf(row) {

  return (
    row.user_name ||
    row.userName ||
    row.name ||
    "Unknown"
  );
}

function conditionOf(row) {

  return (
    row.condition_id ||
    row.conditionId ||
    row.condition ||
    ""
  );
}

function tokenOf(row) {

  return (
    row.token_id ||
    row.tokenId ||
    ""
  );
}

function outcomeOf(row) {

  return String(
    row.outcome ||
    row.outcome_name ||
    row.outcomeName ||
    ""
  ).toUpperCase();
}

function sideOf(row) {

  return String(
    row.side ||
    ""
  ).toUpperCase();
}

function timestampOf(row) {

  const value =
    row.timestamp ||
    row.time ||
    row.last_event_at ||
    row.lastEventAt;

  const n = Number(value);

  if (
    Number.isFinite(n) &&
    n > 0
  ) {
    return n > 100000000000
      ? Math.floor(n / 1000)
      : n;
  }

  const parsed =
    Date.parse(value);

  if (
    Number.isFinite(parsed)
  ) {
    return Math.floor(
      parsed / 1000
    );
  }

  return 0;
}

function dollarsOfTrade(row) {

  const explicit =
    row.usdc_size ??
    row.usdcSize ??
    row.cash_value ??
    row.cashValue;

  if (
    explicit !== null &&
    explicit !== undefined
  ) {

    const n =
      Number(explicit);

    if (
      Number.isFinite(n)
    ) {
      return n;
    }
  }

  const size =
    number(
      row.size,
      row.amount
    );

  const price =
    number(row.price);

  return size * price;
}


/* =========================================================
   API
   ========================================================= */

async function api(
  url,
  attempts = 8
) {

  let lastError;

  for (
    let attempt = 0;
    attempt < attempts;
    attempt++
  ) {

    try {

      const controller =
        new AbortController();

      const timer =
        setTimeout(
          () => controller.abort(),
          30000
        );

      const response =
        await fetch(
          url,
          {
            signal:
              controller.signal,

            headers:{
              Accept:
                "application/json",

              "User-Agent":
                "Polymarket-Top10000-Consensus/1.0"
            }
          }
        );

      clearTimeout(timer);

      if (
        response.status === 429 ||
        response.status === 503
      ) {

        const retry =
          Number(
            response.headers.get(
              "retry-after"
            )
          );

        await sleep(
          Number.isFinite(retry)
            ? retry * 1000
            : 2500 +
              attempt * 1500
        );

        continue;
      }

      if (!response.ok) {

        const text =
          await response.text();

        throw new Error(
          `HTTP ${response.status}: ${text.slice(0,300)}`
        );
      }

      return await response.json();

    } catch (error) {

      lastError = error;

      if (
        attempt <
        attempts - 1
      ) {

        await sleep(
          1000 +
          attempt * 1500
        );
      }
    }
  }

  throw lastError;
}


/* =========================================================
   CURSOR PAGINATION
   ========================================================= */

async function paged(
  endpoint,
  params = {},
  options = {}
) {

  const {
    maxPages = 100,
    cursorOnly = false
  } = options;

  const output = [];

  let cursor = null;

  for (
    let page = 0;
    page < maxPages;
    page++
  ) {

    const query =
      new URLSearchParams();

    if (!cursorOnly) {

      for (
        const [key,value]
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
    }

    if (cursor) {

      query.set(
        "cursor",
        cursor
      );
    }

    const result =
      await api(
        `${BASE}${endpoint}?${query.toString()}`
      );

    const rows =
      Array.isArray(result)
        ? result
        : Array.isArray(result?.data)
          ? result.data
          : [];

    output.push(
      ...rows
    );

    const pagination =
      result?.pagination;

    if (
      !pagination?.has_more ||
      !pagination?.next_cursor
    ) {
      break;
    }

    cursor =
      pagination.next_cursor;

    await sleep(75);
  }

  return output;
}


/* =========================================================
   LEADERBOARD CANDIDATES
   ========================================================= */

async function leaderboardCandidates(
  sortBy
) {

  console.log(
    `Collecting weekly ${sortBy} leaderboard...`
  );

  const rows =
    await paged(
      "/v2/leaderboard",
      {
        time_period:
          "week",

        category:
          "overall",

        sort_by:
          sortBy,

        limit:
          1000
      },
      {
        maxPages: 20,
        cursorOnly:
          false
      }
    );

  return rows
    .map(row => {

      const wallet =
        walletOf(row);

      if (!wallet) {
        return null;
      }

      return {
        address:
          wallet,

        name:
          nameOf(row),

        leaderboardPnl:
          number(row.pnl),

        leaderboardVolume:
          number(row.volume)
      };

    })
    .filter(Boolean);
}


/* =========================================================
   BUILD CANDIDATE UNIVERSE
   ========================================================= */

async function discoverCandidates() {

  const [
    pnlRows,
    volumeRows
  ] =
    await Promise.all([
      leaderboardCandidates("PNL"),
      leaderboardCandidates("VOLUME")
    ]);

  const map =
    new Map();

  for (
    const row
    of [
      ...pnlRows,
      ...volumeRows
    ]
  ) {

    const existing =
      map.get(
        row.address
      );

    if (!existing) {

      map.set(
        row.address,
        row
      );

      continue;
    }

    if (
      row.name &&
      existing.name === "Unknown"
    ) {
      existing.name =
        row.name;
    }

    existing.leaderboardPnl =
      Math.max(
        existing.leaderboardPnl,
        row.leaderboardPnl
      );

    existing.leaderboardVolume =
      Math.max(
        existing.leaderboardVolume,
        row.leaderboardVolume
      );
  }

  const candidates =
    [
      ...map.values()
    ];

  candidates.sort(
    (a,b) =>
      (
        b.leaderboardPnl -
        a.leaderboardPnl
      )
  );

  return candidates.slice(
    0,
    CANDIDATE_TARGET
  );
}


/* =========================================================
   WEEKLY CLOSED POSITIONS
   ========================================================= */

async function weeklyClosedPositions(
  wallet
) {

  const now =
    nowSeconds();

  const start =
    now -
    7 * 86400;

  return paged(
    "/v2/positions",
    {
      user:
        wallet,

      status:
        "CLOSED",

      start,

      end:
        now,

      limit:
        1000,

      sort_by:
        "TIMESTAMP",

      sort_direction:
        "DESC"
    },
    {
      maxPages:
        20,

      cursorOnly:
        false
    }
  );
}


/* =========================================================
   CALCULATE WEEKLY WIN/LOSS
   ========================================================= */

function calculateWeeklyStats(
  positions
) {

  /*
    Group by condition.

    This prevents a market from being
    counted twice if the API returns
    multiple position rows for it.
  */

  const markets =
    new Map();

  for (
    const position
    of positions
  ) {

    const condition =
      conditionOf(position);

    if (!condition) {
      continue;
    }

    const pnl =
      number(
        position.realized_pnl,
        position.realizedPnl
      );

    markets.set(
      condition,
      (
        markets.get(condition) ||
        0
      ) + pnl
    );
  }

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  for (
    const marketPnl
    of markets.values()
  ) {

    pnl += marketPnl;

    if (
      marketPnl >
      0.000001
    ) {
      wins++;
    } else if (
      marketPnl <
      -0.000001
    ) {
      losses++;
    }
  }

  const decided =
    wins +
    losses;

  if (
    decided <
    MIN_DECIDED
  ) {

    return {
      wins,
      losses,
      decided,
      pnl,
      winRate:null
    };
  }

  return {
    wins,
    losses,
    decided,
    pnl,
    winRate:
      wins /
      decided *
      100
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
    new Array(
      items.length
    );

  let nextIndex = 0;

  async function worker() {

    while (true) {

      const index =
        nextIndex++;

      if (
        index >=
        items.length
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
          error?.message ||
          error
        );

        results[index] =
          null;
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length:
          Math.min(
            limit,
            items.length
          )
      },
      () => worker()
    )
  );

  return results;
}


/* =========================================================
   BUILD WEEKLY RANKING
   ========================================================= */

async function buildWeeklyRanking(
  candidates
) {

  let completed = 0;

  const rows =
    await mapLimit(
      candidates,
      REQUEST_CONCURRENCY,
      async candidate => {

        const positions =
          await weeklyClosedPositions(
            candidate.address
          );

        const stats =
          calculateWeeklyStats(
            positions
          );

        completed++;

        if (
          completed % 100 === 0
        ) {

          console.log(
            `Weekly calculation: ${completed}/${candidates.length}`
          );
        }

        if (
          stats.winRate === null
        ) {
          return null;
        }

        return {

          address:
            candidate.address,

          name:
            candidate.name,

          wins:
            stats.wins,

          losses:
            stats.losses,

          decided:
            stats.decided,

          pnl:
            stats.pnl,

          winRate:
            stats.winRate,

          leaderboardPnl:
            candidate.leaderboardPnl,

          leaderboardVolume:
            candidate.leaderboardVolume
        };
      }
    );

  const valid =
    rows.filter(Boolean);


  /*
    EXACT RANKING REQUESTED:

      1. Win rate
      2. Wins
      3. Decided markets
      4. P&L
  */

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

      if (
        b.decided !==
        a.decided
      ) {

        return (
          b.decided -
          a.decided
        );
      }

      return (
        b.pnl -
        a.pnl
      );
    }
  );


  return valid
    .slice(
      0,
      TARGET_WEEKLY_TRADERS
    )
    .map(
      (row,index) => ({

        ...row,

        weeklyRank:
          index + 1
      })
    );
}


/* =========================================================
   CACHE
   ========================================================= */

function cachePath() {

  return path.join(
    DATA_DIR,
    "weekly-universe.json"
  );
}

function loadWeeklyCache() {

  try {

    const raw =
      fs.readFileSync(
        cachePath(),
        "utf8"
      );

    const cache =
      JSON.parse(raw);

    if (
      cache.weekKey !==
      weekKey()
    ) {

      return null;
    }

    const age =
      Date.now() -
      new Date(
        cache.generatedAt
      ).getTime();

    if (
      age >
      WEEKLY_CACHE_HOURS *
      3600000
    ) {

      return null;
    }

    if (
      !Array.isArray(
        cache.traders
      )
    ) {

      return null;
    }

    return cache.traders;

  } catch {

    return null;
  }
}

function saveWeeklyCache(
  traders
) {

  fs.writeFileSync(
    cachePath(),
    JSON.stringify(
      {
        generatedAt:
          new Date().toISOString(),

        weekKey:
          weekKey(),

        target:
          TARGET_WEEKLY_TRADERS,

        traders
      },
      null,
      2
    )
  );
}


/* =========================================================
   GLOBAL RECENT TRADES
   ========================================================= */

async function recentGlobalTrades() {

  const cutoff =
    nowSeconds() -
    ACTIVE_HOURS *
    3600;

  const output = [];

  let cursor = null;

  for (
    let page = 0;
    page < 100;
    page++
  ) {

    const query =
      new URLSearchParams();

    query.set(
      "limit",
      "1000"
    );

    if (cursor) {

      query.set(
        "cursor",
        cursor
      );
    }

    const result =
      await api(
        `${BASE}/v2/trades?${query.toString()}`
      );

    const rows =
      Array.isArray(result?.data)
        ? result.data
        : [];

    if (!rows.length) {
      break;
    }

    let oldest =
      Number.MAX_SAFE_INTEGER;

    for (
      const row
      of rows
    ) {

      const timestamp =
        timestampOf(row);

      if (
        timestamp
      ) {

        oldest =
          Math.min(
            oldest,
            timestamp
          );
      }

      if (
        timestamp >=
        cutoff
      ) {

        output.push(
          row
        );
      }
    }

    /*
      Global feed is newest-first.

      Once an entire page is older
      than the 12-hour window, stop.
    */

    if (
      oldest <
      cutoff
    ) {
      break;
    }

    const pagination =
      result?.pagination;

    if (
      !pagination?.has_more ||
      !pagination?.next_cursor
    ) {
      break;
    }

    cursor =
      pagination.next_cursor;

    await sleep(100);
  }

  return output;
}


/* =========================================================
   MARKET HOLDERS
   ========================================================= */

async function marketPositions(
  condition
) {

  return paged(
    "/v2/positions",
    {
      condition,
      status:
        "OPEN",
      limit:
        1000,
      sort_by:
        "CURRENT_VALUE",
      sort_direction:
        "DESC"
    },
    {
      maxPages:
        10,
      cursorOnly:
        false
    }
  );
}


/* =========================================================
   BUILD ACTIVE CONSENSUS
   ========================================================= */

async function buildActiveConsensus(
  weeklyTraders
) {

  const rankMap =
    new Map();

  for (
    const trader
    of weeklyTraders
  ) {

    rankMap.set(
      trader.address.toLowerCase(),
      trader
    );
  }


  const recent =
    await recentGlobalTrades();


  /*
    Only BUYs matter.
  */

  const buys =
    new Map();

  for (
    const trade
    of recent
  ) {

    if (
      sideOf(trade) !==
      "BUY"
    ) {
      continue;
    }

    const wallet =
      walletOf(trade);

    if (!wallet) {
      continue;
    }

    const trader =
      rankMap.get(
        wallet.toLowerCase()
      );

    if (!trader) {
      continue;
    }

    const condition =
      conditionOf(trade);

    const token =
      tokenOf(trade);

    const outcome =
      outcomeOf(trade);

    const timestamp =
      timestampOf(trade);

    if (
      !condition ||
      !timestamp
    ) {
      continue;
    }

    const key =
      [
        condition,
        token ||
        outcome,
        wallet.toLowerCase()
      ].join("|");


    const amount =
      dollarsOfTrade(trade);


    const existing =
      buys.get(key);


    if (!existing) {

      buys.set(
        key,
        {
          wallet,
          trader,
          condition,
          token,
          outcome,
          timestamp,
          amount,
          title:
            trade.title ||
            trade.market_title ||
            trade.marketTitle ||
            "Unknown market"
        }
      );

    } else {

      /*
        Sum all BUY dollars for the
        same trader / market / token
        during the 12-hour window.
      */

      existing.amount +=
        amount;

      if (
        timestamp >
        existing.timestamp
      ) {

        existing.timestamp =
          timestamp;
      }

      if (
        trade.title ||
        trade.market_title
      ) {

        existing.title =
          trade.title ||
          trade.market_title;
      }
    }
  }


  /*
    Group by market so we only request
    holder data once per market.
  */

  const conditions =
    [
      ...new Set(
        [
          ...buys.values()
        ].map(
          x => x.condition
        )
      )
    ];


  console.log(
    `12-hour qualifying market count: ${conditions.length}`
  );


  const holderResults =
    await mapLimit(
      conditions,
      15,
      async condition => {

        const positions =
          await marketPositions(
            condition
          );

        return {
          condition,
          positions
        };
      }
    );


  const holderMap =
    new Map();


  for (
    const result
    of holderResults
  ) {

    if (!result) {
      continue;
    }

    const tokenWallets =
      new Set();

    for (
      const position
      of result.positions
    ) {

      const wallet =
        walletOf(position);

      const token =
        tokenOf(position);

      const size =
        number(
          position.current_size,
          position.currentSize
        );

      if (
        wallet &&
        token &&
        size > 0
      ) {

        tokenWallets.add(
          `${token}|${wallet.toLowerCase()}`
        );
      }
    }

    holderMap.set(
      result.condition,
      tokenWallets
    );
  }


  /*
    Build market/side consensus.
  */

  const markets =
    new Map();


  for (
    const buy
    of buys.values()
  ) {

    const holders =
      holderMap.get(
        buy.condition
      );

    if (!holders) {
      continue;
    }

    /*
      Must still have a live position.
    */

    if (
      buy.token &&
      !holders.has(
        `${buy.token}|${buy.wallet.toLowerCase()}`
      )
    ) {
      continue;
    }


    const side =
      buy.outcome === "YES" ||
      buy.outcome === "NO"
        ? buy.outcome
        : "";


    if (!side) {
      continue;
    }


    if (
      !markets.has(
        buy.condition
      )
    ) {

      markets.set(
        buy.condition,
        {
          title:
            buy.title,

          YES:
            new Map(),

          NO:
            new Map()
        }
      );
    }


    const market =
      markets.get(
        buy.condition
      );


    const trader =
      buy.trader;


    market[side].set(
      trader.address.toLowerCase(),
      {
        name:
          trader.name,

        rank:
          trader.weeklyRank,

        wins:
          trader.wins,

        losses:
          trader.losses,

        decided:
          trader.decided,

        winRate:
          trader.winRate,

        pnl:
          trader.pnl,

        amount:
          buy.amount,

        side,

        buyTime:
          new Date(
            buy.timestamp * 1000
          ).toISOString(),

        timestamp:
          buy.timestamp
      }
    );
  }


  const consensus = [];


  for (
    const market
    of markets.values()
  ) {

    for (
      const side
      of ["YES","NO"]
    ) {

      const traders =
        [
          ...market[side].values()
        ];


      /*
        At least two strong weekly
        traders must be on the same side.
      */

      if (
        traders.length <
        2
      ) {
        continue;
      }


      /*
        Opposing side means it is not
        the clean consensus you're after.
      */

      const opposite =
        side === "YES"
          ? "NO"
          : "YES";


      if (
        market[opposite].size >
        0
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
            sum +
            t.amount,
          0
        );


      const newest =
        Math.max(
          ...traders.map(
            t => t.timestamp
          )
        );


      const oldest =
        Math.min(
          ...traders.map(
            t => t.timestamp
          )
        );


      consensus.push({

        title:
          market.title,

        side,

        sameSide:
          traders.length,

        totalEntry:
          Number(
            total.toFixed(2)
          ),

        newestBuy:
          new Date(
            newest * 1000
          ).toISOString(),

        oldestBuy:
          new Date(
            oldest * 1000
          ).toISOString(),

        active12h:
          traders.length,

        traders:
          traders.map(
            trader => ({

              name:
                trader.name,

              rank:
                trader.rank,

              wins:
                trader.wins,

              losses:
                trader.losses,

              decided:
                trader.decided,

              winRate:
                Number(
                  trader.winRate.toFixed(2)
                ),

              pnl:
                Number(
                  trader.pnl.toFixed(2)
                ),

              side:
                trader.side,

              amount:
                Number(
                  trader.amount.toFixed(2)
                ),

              buyTime:
                trader.buyTime
            })
          )
      });
    }
  }


  consensus.sort(
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
  );


  return consensus.slice(
    0,
    1000
  );
}


/* =========================================================
   ACTIVE TRADES FOR TOP 100
   ========================================================= */

async function buildTop100ActiveTrades(
  leaders
) {

  const result = {};


  /*
    Use the same global 12-hour feed,
    rather than making 100 separate
    trade requests.
  */

  const recent =
    await recentGlobalTrades();


  const leaderMap =
    new Map();


  for (
    const leader
    of leaders
  ) {

    if (
      leader.wallet
    ) {

      leaderMap.set(
        leader.wallet.toLowerCase(),
        leader.wallet
      );
    }
  }


  const grouped =
    new Map();


  for (
    const trade
    of recent
  ) {

    if (
      sideOf(trade) !==
      "BUY"
    ) {
      continue;
    }

    const wallet =
      walletOf(trade);

    if (
      !leaderMap.has(
        wallet.toLowerCase()
      )
    ) {
      continue;
    }

    const condition =
      conditionOf(trade);

    const token =
      tokenOf(trade);

    if (
      !condition
    ) {
      continue;
    }

    const key =
      [
        wallet.toLowerCase(),
        condition,
        token
      ].join("|");


    const timestamp =
      timestampOf(trade);


    if (!grouped.has(key)) {

      grouped.set(
        key,
        {
          wallet,
          condition,
          token,
          title:
            trade.title ||
            trade.market_title ||
            "Unknown market",

          side:
            outcomeOf(trade),

          amount:
            dollarsOfTrade(trade),

          timestamp
        }
      );

    } else {

      const existing =
        grouped.get(key);

      existing.amount +=
        dollarsOfTrade(trade);

      if (
        timestamp >
        existing.timestamp
      ) {

        existing.timestamp =
          timestamp;
      }
    }
  }


  const conditions =
    [
      ...new Set(
        [
          ...grouped.values()
        ].map(
          x => x.condition
        )
      )
    ];


  const holderResults =
    await mapLimit(
      conditions,
      15,
      async condition => {

        const positions =
          await marketPositions(
            condition
          );

        return {
          condition,
          positions
        };
      }
    );


  const holders =
    new Map();


  for (
    const resultRow
    of holderResults
  ) {

    if (!resultRow) {
      continue;
    }

    const set =
      new Set();

    for (
      const position
      of resultRow.positions
    ) {

      const wallet =
        walletOf(position);

      const token =
        tokenOf(position);

      const size =
        number(
          position.current_size,
          position.currentSize
        );

      if (
        wallet &&
        token &&
        size > 0
      ) {

        set.add(
          `${token}|${wallet.toLowerCase()}`
        );
      }
    }

    holders.set(
      resultRow.condition,
      set
    );
  }


  for (
    const row
    of grouped.values()
  ) {

    const holderSet =
      holders.get(
        row.condition
      );

    if (!holderSet) {
      continue;
    }

    if (
      row.token &&
      !holderSet.has(
        `${row.token}|${row.wallet.toLowerCase()}`
      )
    ) {
      continue;
    }


    if (
      !result[row.wallet]
    ) {
      result[row.wallet] = [];
    }


    result[row.wallet].push({

      title:
        row.title,

      market:
        row.condition,

      side:
        row.side,

      amount:
        Number(
          row.amount.toFixed(2)
        ),

      buyTime:
        new Date(
          row.timestamp * 1000
        ).toISOString(),

      timestamp:
        row.timestamp
    });
  }


  for (
    const wallet
    of Object.keys(result)
  ) {

    result[wallet].sort(
      (a,b) =>
        b.timestamp -
        a.timestamp
    );
  }


  return result;
}


/* =========================================================
   READ OLD FILE
   ========================================================= */

function readJson(
  filename
) {

  try {

    return JSON.parse(
      fs.readFileSync(
        path.join(
          DATA_DIR,
          filename
        ),
        "utf8"
      )
    );

  } catch {

    return null;
  }
}


/* =========================================================
   WRITE JSON
   ========================================================= */

function writeJson(
  filename,
  data
) {

  fs.writeFileSync(
    path.join(
      DATA_DIR,
      filename
    ),
    JSON.stringify(
      data,
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
    "======================================"
  );
  console.log(
    "POLYMARKET 10,000 CONSENSUS SCANNER"
  );
  console.log(
    "======================================"
  );


  /* -------------------------------------
     WEEKLY RANKING
     ------------------------------------- */

  let weekly =
    loadWeeklyCache();


  if (weekly) {

    console.log(
      `Using weekly cache: ${weekly.length} traders`
    );

  } else {

    console.log(
      "Weekly cache expired/missing."
    );

    const candidates =
      await discoverCandidates();

    console.log(
      `Candidates found: ${candidates.length}`
    );

    weekly =
      await buildWeeklyRanking(
        candidates
      );

    console.log(
      `Qualified weekly traders: ${weekly.length}`
    );

    saveWeeklyCache(
      weekly
    );
  }


  /* -------------------------------------
     TOP 100

     Preserve your existing Top 100 if
     it already exists, so this change
     does not wreck the part that was
     already working.
     ------------------------------------- */

  const oldWeek =
    readJson(
      "week.json"
    );


  let leaders =
    oldWeek?.leaders;


  if (
    !Array.isArray(leaders) ||
    leaders.length === 0
  ) {

    leaders =
      weekly
        .slice(0,100)
        .map(
          (trader,index) => ({

            rank:
              index + 1,

            name:
              trader.name,

            wallet:
              trader.address,

            winRate:
              Number(
                trader.winRate.toFixed(2)
              ),

            wins:
              trader.wins,

            losses:
              trader.losses,

            markets:
              trader.decided,

            pnl:
              Number(
                trader.pnl.toFixed(2)
              ),

            volume:
              trader.leaderboardVolume
          })
        );
  }


  /* -------------------------------------
     12-HOUR ACTIVE TOP-100 POSITIONS
     ------------------------------------- */

  console.log(
    "Building Top 100 active trades..."
  );


  const traderActiveTrades =
    await buildTop100ActiveTrades(
      leaders
    );


  /* -------------------------------------
     12-HOUR CONSENSUS
     ------------------------------------- */

  console.log(
    "Building 12-hour consensus..."
  );


  const consensus =
    await buildActiveConsensus(
      weekly
    );


  /* -------------------------------------
     PRESERVE MONTH / 3 MONTH
     ------------------------------------- */

  const oldMonth =
    readJson(
      "month.json"
    );

  const oldThreeMonth =
    readJson(
      "threeMonth.json"
    );


  const generatedAt =
    new Date().toISOString();


  const common = {

    generatedAt,

    activeWindowHours:
      ACTIVE_HOURS,

    activeTradeWindow:
      "12 hours",

    activeTraderUniverseTarget:
      TARGET_WEEKLY_TRADERS,

    activeTraderUniverseSize:
      weekly.length,

    minimumDecidedMarkets:
      MIN_DECIDED,

    rankingMethod:
      "Win rate → wins → decided markets → P&L",

    consensus
  };


  writeJson(
    "week.json",
    {
      ...common,

      period:
        "week",

      periodLabel:
        "1 Week",

      leaders,

      traderActiveTrades
    }
  );


  writeJson(
    "month.json",
    {
      ...(oldMonth || {}),

      generatedAt,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradeWindow:
        "12 hours",

      activeTraderUniverseTarget:
        TARGET_WEEKLY_TRADERS,

      consensus
    }
  );


  writeJson(
    "threeMonth.json",
    {
      ...(oldThreeMonth || {}),

      generatedAt,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradeWindow:
        "12 hours",

      activeTraderUniverseTarget:
        TARGET_WEEKLY_TRADERS,

      consensus
    }
  );


  writeJson(
    "status.json",
    {

      ok:
        true,

      generatedAt,

      weeklyQualifiedTraders:
        weekly.length,

      weeklyTarget:
        TARGET_WEEKLY_TRADERS,

      activeWindowHours:
        ACTIVE_HOURS,

      top100:
        leaders.length,

      consensusMarkets:
        consensus.length,

      cacheHours:
        WEEKLY_CACHE_HOURS
    }
  );


  console.log("");
  console.log(
    "======================================"
  );
  console.log(
    "DONE"
  );
  console.log(
    "======================================"
  );

  console.log(
    `Weekly traders: ${weekly.length}`
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
      "FATAL:",
      error
    );

    process.exit(1);
  }
);
