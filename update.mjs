import fs from 'node:fs/promises';

const API = 'https://data-api.polymarket.com';
const now = Math.floor(Date.now() / 1000);
const DAY = 86400;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function get(path, params = {}, tries = 3) {
  const url = new URL(API + path);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  }

  for (let attempt = 0; attempt < tries; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'user-agent': 'polymarket-consensus-scanner/4.0'
        }
      });

      clearTimeout(timeout);

      if (response.ok) {
        return await response.json();
      }

      if (
        (response.status === 429 || response.status >= 500) &&
        attempt < tries - 1
      ) {
        await sleep(Math.min(2000 * (attempt + 1), 8000));
        continue;
      }

      throw new Error(
        `${response.status} ${await response.text()}`
      );
    } catch (error) {
      clearTimeout(timeout);

      if (attempt === tries - 1) {
        throw error;
      }

      await sleep(1500 * (attempt + 1));
    }
  }

  throw new Error('Request failed');
}

function getUser(x) {
  return x?.proxyWallet || x?.address || x?.user || null;
}

function getName(x) {
  return (
    x?.userName ||
    x?.username ||
    x?.pseudonym ||
    x?.name ||
    'Unknown'
  );
}

/* =========================================================
   LEADERBOARD
========================================================= */

async function leaderboard(period, wanted = 100) {
  const results = [];

  for (let offset = 0; offset < wanted; offset += 50) {
    const page = await get('/v1/leaderboard', {
      category: 'OVERALL',
      timePeriod: period,
      orderBy: 'PNL',
      limit: 50,
      offset
    });

    if (!Array.isArray(page) || page.length === 0) {
      break;
    }

    results.push(...page);

    if (page.length < 50) {
      break;
    }

    await sleep(100);
  }

  return results.slice(0, wanted);
}

/* =========================================================
   WIN / LOSS
========================================================= */

async function getClosedStats(user, start) {
  let wins = 0;
  let losses = 0;
  let cursor = null;

  for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
    const params = {
      user,
      status: 'CLOSED',
      limit: 500,
      start,
      sortBy: 'TIMESTAMP',
      sortDirection: 'DESC'
    };

    if (cursor) {
      params.cursor = cursor;
    }

    let page;

    try {
      page = await get('/v2/positions', params);
    } catch {
      break;
    }

    const positions = Array.isArray(page?.data)
      ? page.data
      : [];

    if (!positions.length) {
      break;
    }

    for (const position of positions) {
      const pnl = Number(
        position.realized_pnl ??
        position.realizedPnl ??
        0
      );

      if (pnl > 0) {
        wins++;
      } else if (pnl < 0) {
        losses++;
      }
    }

    if (
      !page?.pagination?.has_more ||
      !page?.pagination?.next_cursor
    ) {
      break;
    }

    cursor = page.pagination.next_cursor;

    await sleep(50);
  }

  return {
    wins,
    losses,
    winRate:
      wins + losses > 0
        ? (wins / (wins + losses)) * 100
        : null
  };
}

/* =========================================================
   TOP 100 LEADERS
========================================================= */

async function buildLeaders(period, label) {
  console.log(`Loading ${label} leaderboard...`);

  const source = await leaderboard(period, 100);

  const start =
    period === 'WEEK'
      ? now - 7 * DAY
      : now - 30 * DAY;

  const results = [];

  for (let i = 0; i < source.length; i += 10) {
    const batch = source.slice(i, i + 10);

    const batchResults = await Promise.all(
      batch.map(async trader => {
        const user = getUser(trader);

        let stats = {
          wins: 0,
          losses: 0,
          winRate: null
        };

        if (user) {
          stats = await getClosedStats(
            user,
            start
          );
        }

        return {
          name: getName(trader),
          pnl: Number(trader.pnl || 0),
          volume: Number(
            trader.vol ??
            trader.volume ??
            0
          ),
          wins: stats.wins,
          losses: stats.losses,
          winRate: stats.winRate
        };
      })
    );

    results.push(...batchResults);

    console.log(
      `${label}: ${Math.min(
        i + 10,
        source.length
      )}/${source.length}`
    );
  }

  return {
    periodLabel: label,
    generatedAt: new Date().toISOString(),

    leaders: results.map((trader, index) => ({
      rank: index + 1,
      ...trader
    }))
  };
}

/* =========================================================
   TOP 1000 WEEKLY TRADERS
========================================================= */

async function getTop1000Weekly() {
  console.log(
    'Loading top 1,000 weekly traders...'
  );

  const traders = [];

  for (let offset = 0; offset < 1000; offset += 50) {
    const page = await get('/v1/leaderboard', {
      category: 'OVERALL',
      timePeriod: 'WEEK',
      orderBy: 'PNL',
      limit: 50,
      offset
    });

    if (!Array.isArray(page) || !page.length) {
      break;
    }

    traders.push(...page);

    console.log(
      `Weekly universe: ${Math.min(
        traders.length,
        1000
      )}/1000`
    );

    if (page.length < 50) {
      break;
    }

    await sleep(100);
  }

  return traders.slice(0, 1000);
}

/* =========================================================
   OPEN POSITIONS
========================================================= */

async function getOpenPositions(traders) {
  const results = [];

  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;

      if (index >= traders.length) {
        return;
      }

      const trader = traders[index];
      const user = getUser(trader);

      if (!user) {
        continue;
      }

      try {
        const data = await get('/v2/positions', {
          user,
          status: 'OPEN',
          limit: 500,
          sortBy: 'TIMESTAMP',
          sortDirection: 'DESC'
        });

        results.push({
          user,
          positions: Array.isArray(data?.data)
            ? data.data
            : []
        });
      } catch (error) {
        console.log(
          `Open position failed ${index + 1}/${
            traders.length
          }`
        );
      }

      await sleep(50);
    }
  }

  const workerCount = Math.min(
    15,
    traders.length
  );

  await Promise.all(
    Array.from(
      { length: workerCount },
      () => worker()
    )
  );

  return results;
}

/* =========================================================
   RECENT TRADES
========================================================= */

async function getRecentTrades(users) {
  const results = [];

  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex++;

      if (index >= users.length) {
        return;
      }

      const user = users[index];

      try {
        const data = await get('/v2/trades', {
          user,
          start: now - 48 * 60 * 60,
          end: now,
          limit: 500,
          sortDirection: 'DESC'
        });

        results.push({
          user,
          trades: Array.isArray(data?.data)
            ? data.data
            : []
        });
      } catch {
        console.log(
          `Trade lookup failed ${index + 1}/${
            users.length
          }`
        );
      }

      await sleep(50);
    }
  }

  const workerCount = Math.min(
    15,
    users.length
  );

  await Promise.all(
    Array.from(
      { length: workerCount },
      () => worker()
    )
  );

  return results;
}

/* =========================================================
   TIME LABEL
========================================================= */

function ageLabel(timestamp) {
  if (!timestamp) {
    return 'Recent';
  }

  const hours =
    (Date.now() -
      Number(timestamp) * 1000) /
    3600000;

  if (hours < 1) {
    return `${Math.max(
      1,
      Math.round(hours * 60)
    )}m ago`;
  }

  if (hours < 24) {
    return `${Math.round(hours)}h ago`;
  }

  return `${Math.round(
    hours / 24
  )}d ago`;
}

/* =========================================================
   CONSENSUS
========================================================= */

async function buildConsensus() {
  const traders = await getTop1000Weekly();

  console.log(
    `Scanning open positions for ${
      traders.length
    } traders...`
  );

  const bundles =
    await getOpenPositions(traders);

  const markets = new Map();

  for (const bundle of bundles) {
    for (const position of bundle.positions) {
      if (position.archived) {
        continue;
      }

      const currentSize = Number(
        position.current_size ??
        position.size ??
        0
      );

      if (currentSize <= 0) {
        continue;
      }

      const outcome = String(
        position.outcome || ''
      ).toLowerCase();

      if (
        outcome !== 'yes' &&
        outcome !== 'no'
      ) {
        continue;
      }

      const conditionId =
        position.condition_id ||
        position.conditionId;

      if (!conditionId) {
        continue;
      }

      if (!markets.has(conditionId)) {
        markets.set(conditionId, {
          conditionId,
          title:
            position.title ||
            position.name ||
            'Untitled',
          endDate:
            position.end_date ||
            position.endDate ||
            null,
          yes: new Map(),
          no: new Map()
        });
      }

      const market =
        markets.get(conditionId);

      const entry = Number(
        position.entry_cost_usdc ??
        position.total_cost_usdc ??
        position.cash_value ??
        0
      );

      market[outcome].set(
        bundle.user,
        {
          user: bundle.user,
          entry,
          position
        }
      );
    }
  }

  console.log(
    `Active markets discovered: ${
      markets.size
    }`
  );

  /*
    Only request recent trades from traders who
    actually hold an active position.
  */
  const activeUsers = new Set();

  for (const market of markets.values()) {
    for (const holder of market.yes.values()) {
      activeUsers.add(holder.user);
    }

    for (const holder of market.no.values()) {
      activeUsers.add(holder.user);
    }
  }

  console.log(
    `Checking recent activity for ${
      activeUsers.size
    } active traders...`
  );

  const tradeBundles =
    await getRecentTrades(
      [...activeUsers]
    );

  /*
    wallet|condition|side
    -> earliest BUY
  */
  const buys = new Map();

  for (const bundle of tradeBundles) {
    for (const trade of bundle.trades) {
      if (
        String(trade.side || '').toUpperCase() !==
        'BUY'
      ) {
        continue;
      }

      const outcome = String(
        trade.outcome || ''
      ).toLowerCase();

      if (
        outcome !== 'yes' &&
        outcome !== 'no'
      ) {
        continue;
      }

      const conditionId =
        trade.condition_id ||
        trade.conditionId;

      if (!conditionId) {
        continue;
      }

      const timestamp = Number(
        trade.timestamp ||
        trade.block_timestamp ||
        0
      );

      if (!timestamp) {
        continue;
      }

      const key =
        `${bundle.user}|${conditionId}|${outcome}`;

      const existing =
        buys.get(key);

      if (
        !existing ||
        timestamp < existing.timestamp
      ) {
        buys.set(key, {
          timestamp,
          amount: Number(
            trade.usdc_size ??
            trade.usdcSize ??
            (
              Number(trade.size || 0) *
              Number(trade.price || 0)
            )
          )
        });
      }
    }
  }

  const rows = [];

  for (const market of markets.values()) {
    const yes = market.yes;
    const no = market.no;

    let side;
    let group;
    let opposite;

    /*
      Same-side consensus ONLY.

      YES 10 / NO 0 -> valid
      YES 0 / NO 10 -> valid

      YES 10 / NO 1 -> rejected
      YES 5 / NO 5 -> rejected
    */

    if (
      yes.size >= 2 &&
      no.size === 0
    ) {
      side = 'YES';
      group = yes;
      opposite = no;
    } else if (
      no.size >= 2 &&
      yes.size === 0
    ) {
      side = 'NO';
      group = no;
      opposite = yes;
    } else {
      continue;
    }

    let firstBuy = Infinity;
    let totalEntry = 0;

    const tradersForMarket = [];

    for (const holder of group.values()) {
      totalEntry += Number(
        holder.entry || 0
      );

      const key =
        `${holder.user}|${market.conditionId}|${side.toLowerCase()}`;

      const buy = buys.get(key);

      if (buy?.timestamp) {
        firstBuy = Math.min(
          firstBuy,
          buy.timestamp
        );
      }

      tradersForMarket.push({
        amount: Number(
          holder.entry || 0
        ),
        buyTime: buy?.timestamp
          ? new Date(
              buy.timestamp * 1000
            ).toISOString()
          : null
      });
    }

    /*
      We need an observed BUY timestamp.
    */
    if (!Number.isFinite(firstBuy)) {
      continue;
    }

    /*
      Only recent consensus.
    */
    const ageHours =
      (now - firstBuy) / 3600;

    if (ageHours > 48) {
      continue;
    }

    /*
      Ignore markets ending more than
      90 days in the future.
    */
    if (market.endDate) {
      const end =
        new Date(
          market.endDate
        ).getTime();

      if (Number.isFinite(end)) {
        const daysToEnd =
          (end - Date.now()) /
          86400000;

        if (daysToEnd > 90) {
          continue;
        }
      }
    }

    rows.push({
      title: market.title,
      side,
      sameSide: group.size,
      oppositeSide: opposite.size,
      totalEntry,
      firstBuy:
        new Date(
          firstBuy * 1000
        ).toISOString(),
      holdLabel:
        ageLabel(firstBuy),
      traders: tradersForMarket
    });
  }

  rows.sort(
    (a, b) =>
      b.sameSide - a.sameSide ||
      new Date(b.firstBuy) -
        new Date(a.firstBuy) ||
      b.totalEntry -
        a.totalEntry
  );

  console.log(
    `Qualifying consensus markets: ${
      rows.length
    }`
  );

  return rows.slice(0, 1000);
}

/* =========================================================
   THREE MONTHS
========================================================= */

async function buildThreeMonth() {
  const source =
    await leaderboard('ALL', 100);

  return {
    periodLabel: '3 Months',
    generatedAt:
      new Date().toISOString(),

    leaders: source.map(
      (trader, index) => ({
        rank: index + 1,
        name: getName(trader),
        pnl: Number(
          trader.pnl || 0
        ),
        volume: Number(
          trader.vol ??
          trader.volume ??
          0
        ),
        wins: Number(
          trader.wins || 0
        ),
        losses: Number(
          trader.losses || 0
        ),
        winRate:
          Number(trader.wins || 0) +
            Number(trader.losses || 0) >
          0
            ? 100 *
              Number(trader.wins || 0) /
              (
                Number(trader.wins || 0) +
                Number(trader.losses || 0)
              )
            : null
      })
    )
  };
}

/* =========================================================
   MAIN
========================================================= */

async function main() {
  await fs.mkdir(
    'data',
    { recursive: true }
  );

  console.log(
    '================================'
  );

  console.log(
    'POLYMARKET SCANNER STARTING'
  );

  console.log(
    '================================'
  );

  /*
    Weekly top 100 + win/loss
  */
  const week =
    await buildLeaders(
      'WEEK',
      '1 Week'
    );

  /*
    Consensus is calculated once
    and shared between periods.
  */
  const consensus =
    await buildConsensus();

  await fs.writeFile(
    'data/week.json',
    JSON.stringify(
      {
        ...week,
        consensus
      },
      null,
      2
    )
  );

  /*
    Monthly top 100 + win/loss
  */
  const month =
    await buildLeaders(
      'MONTH',
      '1 Month'
    );

  await fs.writeFile(
    'data/month.json',
    JSON.stringify(
      {
        ...month,
        consensus
      },
      null,
      2
    )
  );

  /*
    Three-month leaderboard
  */
  const threeMonth =
    await buildThreeMonth();

  await fs.writeFile(
    'data/threeMonth.json',
    JSON.stringify(
      {
        ...threeMonth,
        consensus
      },
      null,
      2
    )
  );

  /*
    Status
  */
  await fs.writeFile(
    'data/status.json',
    JSON.stringify(
      {
        ok: true,
        generatedAt:
          new Date().toISOString(),
        weeklyTraders:
          week.leaders.length,
        monthlyTraders:
          month.leaders.length,
        threeMonthTraders:
          threeMonth.leaders.length,
        consensusMarkets:
          consensus.length
      },
      null,
      2
    )
  );

  console.log(
    '================================'
  );

  console.log(
    'POLYMARKET SCANNER COMPLETE'
  );

  console.log(
    `Weekly traders: ${
      week.leaders.length
    }`
  );

  console.log(
    `Monthly traders: ${
      month.leaders.length
    }`
  );

  console.log(
    `Consensus markets: ${
      consensus.length
    }`
  );

  console.log(
    '================================'
  );
}

main().catch(
  async error => {
    console.error(
      'SCANNER FAILED:',
      error
    );

    await fs.writeFile(
      'data/status.json',
      JSON.stringify(
        {
          ok: false,
          error:
            error?.message ||
            String(error),
          generatedAt:
            new Date().toISOString()
        },
        null,
        2
      )
    );

    process.exit(1);
  }
);
