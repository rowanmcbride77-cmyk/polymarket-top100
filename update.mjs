import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const BASE = "https://data-api.polymarket.com";
const DATA_DIR = path.join(__dirname, "data");

const ACTIVE_HOURS = 12;
const TARGET_TRADERS = 10000;
const MIN_DECIDED = 10;

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));


/* =========================================================
   HELPERS
   ========================================================= */

function num(...values) {
  for (const value of values) {
    if (
      value !== undefined &&
      value !== null &&
      value !== ""
    ) {
      const n = Number(value);

      if (Number.isFinite(n)) {
        return n;
      }
    }
  }

  return 0;
}


function wallet(row) {
  return (
    row.proxy_wallet ||
    row.proxyWallet ||
    row.user ||
    ""
  );
}


function name(row) {
  return (
    row.user_name ||
    row.userName ||
    row.name ||
    row.username ||
    "Unknown"
  );
}


function condition(row) {
  return (
    row.condition_id ||
    row.conditionId ||
    row.condition ||
    ""
  );
}


function token(row) {
  return (
    row.token_id ||
    row.tokenId ||
    ""
  );
}


function outcome(row) {
  return String(
    row.outcome ||
    row.outcome_name ||
    row.outcomeName ||
    ""
  ).toUpperCase();
}


function side(row) {
  return String(
    row.side ||
    ""
  ).toUpperCase();
}


function timestamp(row) {
  const raw =
    row.timestamp ??
    row.time ??
    row.last_event_at ??
    row.lastEventAt;

  const n = Number(raw);

  if (
    Number.isFinite(n) &&
    n > 0
  ) {
    return n > 100000000000
      ? Math.floor(n / 1000)
      : Math.floor(n);
  }

  const parsed =
    Date.parse(raw);

  if (Number.isFinite(parsed)) {
    return Math.floor(parsed / 1000);
  }

  return 0;
}


function tradeDollars(row) {
  const direct =
    row.usdc_size ??
    row.usdcSize;

  if (
    direct !== undefined &&
    direct !== null
  ) {
    const n = Number(direct);

    if (Number.isFinite(n)) {
      return n;
    }
  }

  const size =
    num(
      row.size,
      row.amount
    );

  const price =
    num(row.price);

  return size * price;
}


function title(row) {
  return (
    row.title ||
    row.market_title ||
    row.marketTitle ||
    row.question ||
    row.name ||
    "Unknown market"
  );
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

      const timeout =
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

            headers: {
              Accept:
                "application/json",

              "User-Agent":
                "Polymarket-Consensus-Scanner"
            }
          }
        );

      clearTimeout(timeout);

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
            : 2500
        );

        continue;
      }

      if (!response.ok) {
        const body =
          await response.text();

        throw new Error(
          `HTTP ${response.status}: ${body.slice(0,300)}`
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
          attempt * 1200
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
  params,
  maxPages = 100
) {
  const rows = [];

  let cursor = null;

  for (
    let page = 0;
    page < maxPages;
    page++
  ) {
    const query =
      new URLSearchParams();

    for (
      const [key,value]
      of Object.entries(params || {})
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
        `${BASE}${endpoint}?${query.toString()}`
      );

    const pageRows =
      Array.isArray(result)
        ? result
        : Array.isArray(result?.data)
          ? result.data
          : [];

    rows.push(
      ...pageRows
    );

    const next =
      result?.pagination?.next_cursor;

    if (
      !result?.pagination?.has_more ||
      !next
    ) {
      break;
    }

    cursor = next;

    await sleep(75);
  }

  return rows;
}


/* =========================================================
   WEEKLY LEADERBOARD CANDIDATES
   ========================================================= */

async function getLeaderboard(
  sortBy
) {
  return paged(
    "/v2/leaderboard",
    {
      category:
        "OVERALL",

      timePeriod:
        "WEEK",

      sortBy:
        sortBy,

      limit:
        1000
    },
    20
  );
}


async function discoverCandidates() {
  console.log(
    "Collecting weekly leaderboard candidates..."
  );

  const [
    pnl,
    volume
  ] =
    await Promise.all([
      getLeaderboard("PNL"),
      getLeaderboard("VOLUME")
    ]);

  const map =
    new Map();

  for (
    const row
    of [...pnl,...volume]
  ) {
    const address =
      wallet(row);

    if (!address) {
      continue;
    }

    const key =
      address.toLowerCase();

    const existing =
      map.get(key);

    if (!existing) {
      map.set(
        key,
        {
          address,

          name:
            name(row),

          pnl:
            num(row.pnl),

          volume:
            num(
              row.volume,
              row.vol
            )
        }
      );

    } else {
      existing.pnl =
        Math.max(
          existing.pnl,
          num(row.pnl)
        );

      existing.volume =
        Math.max(
          existing.volume,
          num(
            row.volume,
            row.vol
          )
        );

      if (
        existing.name ===
        "Unknown"
      ) {
        existing.name =
          name(row);
      }
    }
  }

  return [
    ...map.values()
  ];
}


/* =========================================================
   WEEKLY CLOSED POSITIONS
   ========================================================= */

async function getWeeklyClosed(
  address
) {
  const now =
    Math.floor(
      Date.now() / 1000
    );

  const start =
    now -
    7 * 86400;

  return paged(
    "/v2/positions",
    {
      user:
        address,

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
    30
  );
}


/* =========================================================
   WEEKLY RECORD
   ========================================================= */

function calculateRecord(
  positions
) {
  const marketPnl =
    new Map();

  for (
    const position
    of positions
  ) {
    const market =
      condition(position);

    if (!market) {
      continue;
    }

    const pnl =
      num(
        position.realized_pnl,
        position.realizedPnl
      );

    marketPnl.set(
      market,
      (
        marketPnl.get(market) ||
        0
      ) + pnl
    );
  }

  let wins = 0;
  let losses = 0;
  let pnl = 0;

  for (
    const value
    of marketPnl.values()
  ) {
    pnl += value;

    if (
      value > 0.000001
    ) {
      wins++;

    } else if (
      value < -0.000001
    ) {
      losses++;
    }
  }

  const decided =
    wins + losses;

  if (
    decided <
    MIN_DECIDED
  ) {
    return null;
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

  let next = 0;

  async function worker() {
    while (true) {
      const index =
        next++;

      if (
        index >=
        items.length
      ) {
        return;
      }

      try {
        results[index] =
          await fn(
            items[index]
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
      worker
    )
  );

  return results;
}


/* =========================================================
   WEEKLY TOP 10,000
   ========================================================= */

async function buildWeekly(
  candidates
) {
  let finished = 0;

  const results =
    await mapLimit(
      candidates,
      15,
      async candidate => {
        const closed =
          await getWeeklyClosed(
            candidate.address
          );

        const record =
          calculateRecord(
            closed
          );

        finished++;

        if (
          finished % 100 === 0
        ) {
          console.log(
            `Weekly records: ${finished}/${candidates.length}`
          );
        }

        if (!record) {
          return null;
        }

        return {
          address:
            candidate.address,

          name:
            candidate.name,

          wins:
            record.wins,

          losses:
            record.losses,

          decided:
            record.decided,

          pnl:
            record.pnl,

          winRate:
            record.winRate,

          volume:
            candidate.volume
        };
      }
    );

  const valid =
    results.filter(Boolean);

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
      TARGET_TRADERS
    )
    .map(
      (row,index) => ({
        ...row,

        rank:
          index + 1
      })
    );
}


/* =========================================================
   USER RECENT TRADES
   ========================================================= */

async function getRecentTrades(
  address
) {
  const now =
    Math.floor(
      Date.now() / 1000
    );

  const start =
    now -
    ACTIVE_HOURS * 3600;

  return paged(
    "/v2/trades",
    {
      user:
        address,

      start,

      end:
        now,

      limit:
        1000
    },
    50
  );
}


/* =========================================================
   USER OPEN POSITIONS
   ========================================================= */

async function getOpenPositions(
  address
) {
  return paged(
    "/v2/positions",
    {
      user:
        address,

      status:
        "OPEN",

      limit:
        1000,

      sort_by:
        "CURRENT_VALUE",

      sort_direction:
        "DESC"
    },
    30
  );
}


/* =========================================================
   ACTIVE POSITIONS FOR TRADER
   ========================================================= */

function activeForTrader(
  trader,
  trades,
  positions
) {
  const cutoff =
    Math.floor(
      Date.now() / 1000
    ) -
    ACTIVE_HOURS * 3600;

  const openByToken =
    new Map();

  const openByOutcome =
    new Map();

  for (
    const position
    of positions
  ) {
    const conditionId =
      condition(position);

    const tokenId =
      token(position);

    const out =
      outcome(position);

    if (
      conditionId &&
      tokenId
    ) {
      openByToken.set(
        `${conditionId}|${tokenId}`,
        position
      );
    }

    if (
      conditionId &&
      out
    ) {
      openByOutcome.set(
        `${conditionId}|${out}`,
        position
      );
    }
  }

  const buys =
    new Map();

  for (
    const trade
    of trades
  ) {
    if (
      side(trade) !==
      "BUY"
    ) {
      continue;
    }

    const when =
      timestamp(trade);

    if (
      !when ||
      when <
      cutoff
    ) {
      continue;
    }

    const conditionId =
      condition(trade);

    const tokenId =
      token(trade);

    const out =
      outcome(trade);

    if (!conditionId) {
      continue;
    }

    const key =
      tokenId
        ? `${conditionId}|${tokenId}`
        : `${conditionId}|${out}`;

    if (!buys.has(key)) {
      buys.set(
        key,
        {
          title:
            title(trade),

          condition:
            conditionId,

          token:
            tokenId,

          outcome:
            out,

          amount:
            tradeDollars(trade),

          timestamp:
            when
        }
      );

    } else {
      const existing =
        buys.get(key);

      existing.amount +=
        tradeDollars(trade);

      if (
        when >
        existing.timestamp
      ) {
        existing.timestamp =
          when;

        existing.title =
          title(trade);
      }
    }
  }

  const active = [];

  for (
    const buy
    of buys.values()
  ) {
    let position = null;

    if (
      buy.token
    ) {
      position =
        openByToken.get(
          `${buy.condition}|${buy.token}`
        );
    }

    if (
      !position &&
      buy.outcome
    ) {
      position =
        openByOutcome.get(
          `${buy.condition}|${buy.outcome}`
        );
    }

    if (!position) {
      continue;
    }

    const currentSize =
      num(
        position.current_size,
        position.currentSize
      );

    if (
      currentSize <= 0
    ) {
      continue;
    }

    let amount =
      buy.amount;

    if (
      amount <= 0
    ) {
      amount =
        num(
          position.initial_value,
          position.initialValue,
          position.cash_pnl
        );
    }

    active.push({
      title:
        position.title ||
        buy.title,

      market:
        buy.condition,

      side:
        position.outcome ||
        buy.outcome,

      amount:
        Number(
          amount.toFixed(2)
        ),

      buyTime:
        new Date(
          buy.timestamp * 1000
        ).toISOString(),

      timestamp:
        buy.timestamp
    });
  }

  active.sort(
    (a,b) =>
      b.timestamp -
      a.timestamp
  );

  return active;
}


/* =========================================================
   ACTIVE DATA
   ========================================================= */

async function buildActiveData(
  weekly
) {
  console.log(
    `Scanning ${weekly.length} weekly traders for 12-hour BUYs...`
  );

  let done = 0;

  const rows =
    await mapLimit(
      weekly,
      15,
      async trader => {
        const [
          trades,
          positions
        ] =
          await Promise.all([
            getRecentTrades(
              trader.address
            ),

            getOpenPositions(
              trader.address
            )
          ]);

        const active =
          activeForTrader(
            trader,
            trades,
            positions
          );

        done++;

        if (
          done % 100 === 0
        ) {
          console.log(
            `Active scan: ${done}/${weekly.length}`
          );
        }

        return {
          trader,
          active
        };
      }
    );

  return rows.filter(Boolean);
}


/* =========================================================
   CONSENSUS
   ========================================================= */

function buildConsensus(
  rows
) {
  const markets =
    new Map();

  for (
    const row
    of rows
  ) {
    for (
      const position
      of row.active
    ) {
      const market =
        position.market;

      const sideValue =
        String(
          position.side ||
          ""
        ).toUpperCase();

      if (
        !market ||
        (
          sideValue !== "YES" &&
          sideValue !== "NO"
        )
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

      group[
        sideValue
      ].set(
        row.trader.address.toLowerCase(),
        {
          name:
            row.trader.name,

          rank:
            row.trader.rank,

          wins:
            row.trader.wins,

          losses:
            row.trader.losses,

          decided:
            row.trader.decided,

          winRate:
            row.trader.winRate,

          pnl:
            row.trader.pnl,

          side:
            sideValue,

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
      const sideValue
      of ["YES","NO"]
    ) {
      const traders =
        [
          ...group[
            sideValue
          ].values()
        ];

      if (
        traders.length < 2
      ) {
        continue;
      }

      const opposite =
        sideValue === "YES"
          ? "NO"
          : "YES";

      if (
        group[opposite].size > 0
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

      output.push({
        title:
          group.title,

        side:
          sideValue,

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

        traders:
          traders.map(
            t => ({
              name:
                t.name,

              rank:
                t.rank,

              wins:
                t.wins,

              losses:
                t.losses,

              decided:
                t.decided,

              winRate:
                Number(
                  t.winRate.toFixed(2)
                ),

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

              buyTime:
                t.buyTime
            })
          )
      });
    }
  }

  output.sort(
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

  return output;
}


/* =========================================================
   TOP 100 ACTIVE MAP
   ========================================================= */

function activeMapForTop100(
  rows
) {
  const result = {};

  for (
    const row
    of rows
  ) {
    result[
      row.trader.address
    ] =
      row.active;
  }

  return result;
}


/* =========================================================
   WRITE FILE
   ========================================================= */

function write(
  file,
  data
) {
  fs.writeFileSync(
    path.join(
      DATA_DIR,
      file
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
      recursive:
        true
    }
  );

  console.log("");
  console.log(
    "=========================================="
  );
  console.log(
    "POLYMARKET 12-HOUR CONSENSUS SCANNER"
  );
  console.log(
    "=========================================="
  );

  const candidates =
    await discoverCandidates();

  console.log(
    `Candidate wallets: ${candidates.length}`
  );

  const weekly =
    await buildWeekly(
      candidates
    );

  console.log(
    `Qualified weekly traders: ${weekly.length}`
  );

  const activeRows =
    await buildActiveData(
      weekly
    );

  const consensus =
    buildConsensus(
      activeRows
    );

  const leaders =
    weekly
      .slice(
        0,
        100
      )
      .map(
        trader => ({
          rank:
            trader.rank,

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
            trader.volume
        })
      );

  const activeMap =
    activeMapForTop100(
      activeRows.filter(
        row =>
          row.trader.rank <=
          100
      )
    );

  const generatedAt =
    new Date().toISOString();

  let month = {};
  let threeMonth = {};

  try {
    month =
      JSON.parse(
        fs.readFileSync(
          path.join(
            DATA_DIR,
            "month.json"
          ),
          "utf8"
        )
      );
  } catch {}

  try {
    threeMonth =
      JSON.parse(
        fs.readFileSync(
          path.join(
            DATA_DIR,
            "threeMonth.json"
          ),
          "utf8"
        )
      );
  } catch {}

  write(
    "week.json",
    {
      generatedAt,

      period:
        "week",

      periodLabel:
        "1 Week",

      leaders,

      traderActiveTrades:
        activeMap,

      consensus,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradeWindow:
        "12 hours",

      activeTraderUniverseTarget:
        TARGET_TRADERS,

      activeTraderUniverseSize:
        weekly.length,

      minimumDecidedMarkets:
        MIN_DECIDED,

      rankingMethod:
        "Win rate → wins → decided markets → P&L"
    }
  );

  write(
    "month.json",
    {
      ...month,

      generatedAt,

      consensus,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradeWindow:
        "12 hours",

      activeTraderUniverseTarget:
        TARGET_TRADERS,

      activeTraderUniverseSize:
        weekly.length
    }
  );

  write(
    "threeMonth.json",
    {
      ...threeMonth,

      generatedAt,

      consensus,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradeWindow:
        "12 hours",

      activeTraderUniverseTarget:
        TARGET_TRADERS,

      activeTraderUniverseSize:
        weekly.length
    }
  );

  write(
    "status.json",
    {
      ok:
        true,

      generatedAt,

      weeklyCandidates:
        candidates.length,

      weeklyQualified:
        weekly.length,

      weeklyTarget:
        TARGET_TRADERS,

      activeWindowHours:
        ACTIVE_HOURS,

      activeTradersWithPositions:
        activeRows.filter(
          row =>
            row.active.length > 0
        ).length,

      activePositions:
        activeRows.reduce(
          (
            total,
            row
          ) =>
            total +
            row.active.length,
          0
        ),

      consensusMarkets:
        consensus.length
    }
  );

  console.log("");
  console.log(
    "=========================================="
  );
  console.log(
    "UPDATE COMPLETE"
  );
  console.log(
    "=========================================="
  );

  console.log(
    `Weekly qualified: ${weekly.length}`
  );

  console.log(
    `Active traders: ${
      activeRows.filter(
        row =>
          row.active.length > 0
      ).length
    }`
  );

  console.log(
    `Active positions: ${
      activeRows.reduce(
        (n,row) =>
          n +
          row.active.length,
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
      "FATAL ERROR:",
      error
    );

    process.exit(1);
  }
);
