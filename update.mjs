import fs from 'node:fs/promises';

const API = 'https://data-api.polymarket.com';
const now = Math.floor(Date.now() / 1000);
const DAY = 86400;

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function get(path, params = {}, tries = 3) {
  const url = new URL(API + path);

  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') {
      url.searchParams.set(k, String(v));
    }
  }

  for (let attempt = 0; attempt < tries; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
      const r = await fetch(url, {
        signal: controller.signal,
        headers: {
          'user-agent': 'polymarket-consensus-scanner/3.0'
        }
      });

      clearTimeout(timeout);

      if (r.ok) return await r.json();

      if ((r.status === 429 || r.status >= 500) && attempt < tries - 1) {
        await sleep(Math.min(2000 * (attempt + 1), 8000));
        continue;
      }

      throw new Error(`${r.status} ${await r.text()}`);
    } catch (e) {
      clearTimeout(timeout);

      if (attempt === tries - 1) throw e;

      await sleep(1500 * (attempt + 1));
    }
  }
}

function address(x) {
  return x.proxyWallet || x.address || x.user;
}

function name(x) {
  return x.userName || x.username || x.pseudonym || x.name || 'Unknown';
}

/*
  Polymarket returns leaderboard pages of up to 50.
  We request two pages so the site actually receives 100 traders.
*/
async function leaderboard(period, count = 100) {
  const results = [];

  for (let offset = 0; offset < count; offset += 50) {
    const page = await get('/v1/leaderboard', {
      category: 'OVERALL',
      timePeriod: period,
      orderBy: 'PNL',
      limit: 50,
      offset
    });

    if (!Array.isArray(page) || page.length === 0) break;

    results.push(...page);

    if (page.length < 50) break;

    await sleep(100);
  }

  return results.slice(0, count);
}

/*
  Calculate wins/losses from closed positions.

  A resolved position with positive realized PNL = win.
  A resolved position with negative realized PNL = loss.

  Pushes are ignored.
*/
async function closedStats(user, start) {
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

    if (cursor) params.cursor = cursor;

    let page;

    try {
      page = await get('/v2/positions', params);
    } catch {
      break;
    }

    const rows = Array.isArray(page?.data) ? page.data : [];

    if (!rows.length) break;

    for (const p of rows) {
      const pnl = Number(
        p.realized_pnl ??
        p.realizedPnl ??
        0
      );

      if (pnl > 0) wins++;
      else if (pnl < 0) losses++;
    }

    if (
      !page.pagination?.has_more ||
      !page.pagination?.next_cursor
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
      wins + losses
        ? (wins / (wins + losses)) * 100
        : null
  };
}

/*
  Build the actual 100-trader leaderboard.
*/
async function buildLeaders(period, label) {
  console.log(`Loading ${label}...`);

  const source = await leaderboard(period, 100);

  const start =
    period === 'WEEK'
      ? now - 7 * DAY
      : now - 30 * DAY;

  const results = [];

  /*
    Ten concurrent traders at a time keeps the Action reasonably fast
    without hammering the API.
  */
  for (let i = 0; i < source.length; i += 10) {
    const batch = source.slice(i, i + 10);

    const stats = await Promise.all(
      batch.map(async trader => {
        const user = address(trader);

        const s = user
          ? await closedStats(user, start)
          : { wins: 0, losses: 0, winRate: null };

        return {
          rank: 0,
          name: name(trader),
          pnl: Number(trader.pnl || 0),
          volume: Number(
            trader.vol ??
            trader.volume ??
            0
          ),
          wins: s.wins,
          losses: s.losses,
          winRate: s.winRate
        };
      })
    );

    results.push(...stats);

    console.log(
      `${label}: ${Math.min(i + 10, source.length)}/${source.length}`
    );
  }

  return {
    periodLabel: label,
    generatedAt: new Date().toISOString(),
    leaders: results.map((x, i) => ({
      ...x,
      rank: i + 1
    }))
  };
}

/*
  Get the top 1,000 weekly traders for the consensus universe.
*/
async function weeklyTraderUniverse() {
  console.log('Loading top 1,000 weekly traders...');

  const results = [];

  for (let offset = 0; offset < 1000; offset += 50) {
    const page = await get('/v1/leaderboard', {
      category: 'OVERALL',
      timePeriod: 'WEEK',
      orderBy: 'PNL',
      limit: 50,
      offset
    });

    if (!Array.isArray(page) || !page.length) break;

    results.push(...page);

    console.log(
      `Weekly traders: ${Math.min(results.length, 1000)}/1000`
    );

    if (page.length < 50) break;

    await sleep(100);
  }

  return results.slice(0, 1000);
}

/*
  Fetch open positions for the 1,000 weekly traders.
*/
async function fetchOpenPositions(users) {
  const results = [];
  let index = 0;

  async function worker() {
    while (true) {
      const i = index++;

      if (i >= users.length) return;

      const trader = users[i];
      const user = address(trader);

      if (!user) continue;

      try {
        const j = await get('/v2/positions', {
          user,
          status: 'OPEN',
          limit: 500,
          sortBy: 'TIMESTAMP',
          sortDirection: 'DESC'
        });

        results.push({
          trader,
          user,
          positions: Array.isArray(j?.data)
            ? j.data
            : []
        });
      } catch {
        console.log(`Position failed: ${i + 1}/1000`);
      }

      await sleep(40);
    }
  }

  const workers = Math.min(15, users.length);

  await Promise.all(
    Array.from(
      { length: workers },
      () => worker()
    )
  );

  return results;
}

/*
  First discover active markets from positions.
  Then only request recent trades for traders who are
  actually holding one of those markets.
*/
async function recentTrades(users) {
  const results = [];
  let index = 0;

  async function worker() {
    while (true) {
      const i = index++;

      if (i >= users.length) return;

      const user = users[i];

      try {
        const j = await get('/v2/trades', {
          user,
          start: now - 48 * 60 * 60,
          end: now,
          limit: 500,
          sortDirection: 'DESC'
        });

        results.push({
          user,
          trades: Array.isArray(j?.data)
            ? j.data
            : []
        });
      } catch {
        console.log(`Trade failed: ${i + 1}/${users.length}`);
      }

      await sleep(40);
    }
  }

  const workers = Math.min(15, users.length);

  await Promise.all(
    Array.from(
      { length: workers },
      () => worker()
    )
  );

  return results;
}

function ageLabel(timestamp) {
  if (!timestamp) return 'Recent';

  const hours =
    (Date.now() - Number(timestamp) * 1000) /
    3600000;

  if (hours < 1) {
    return `${Math.max(1, Math.round(hours * 60))}m ago`;
  }

  if (hours < 24) {
    return `${Math.round(hours)}h ago`;
  }

  return `${Math.round(hours / 24)}d ago`;
}

async function consensus() {
  const traders = await weeklyTraderUniverse();

  console.log(
    `Scanning open positions for ${traders.length} traders...`
  );

  const bundles = await fetchOpenPositions(traders);

  const markets = new Map();

  for (const bundle of bundles) {
    for (const p of bundle.positions) {
      if (p.archived) continue;

      const size = Number(
        p.current_size ??
        p.size ??
        0
      );

      if (size <= 0) continue;

      const side = String(
        p.outcome || ''
      ).toLowerCase();

      if (side !== 'yes' && side !== 'no') continue;

      const conditionId =
        p.condition_id ||
        p.conditionId;

      if (!conditionId) continue;

      if (!markets.has(conditionId)) {
        markets.set(conditionId, {
          conditionId,
          title:
            p.title ||
            pquestion?.title ||
            p.name ||
            'Untitled',
          endDate:
            p.end_date ||
            p.endDate ||
            null,
          yes: new Map(),
          no: new Map()
        });
      }

      const market = markets.get(conditionId);

      const user = bundle.user;

      const entry = Number(
        p.entry_cost_usdc ??
        p.total_cost_usdc ??
        p.cash_value ??
        0
      );

      market[side].set(user, {
        user,
        entry,
        position: p
      });
    }
  }

  console.log(
    `Active markets discovered: ${markets.size}`
  );

  /*
    Only traders actually holding an active market.
  */
  const activeUsers = new Set();

  for (const market of markets.values()) {
    for (const x of market.yes.values()) {
      activeUsers.add(x.user);
    }

    for (const x of market.no.values()) {
      activeUsers.add(x.user);
    }
  }

  const trades = await recentTrades(
    [...activeUsers]
  );

  /*
    wallet|condition|side -> earliest recent buy
  */
  const buys = new Map();

  for (const bundle of trades) {
    for (const t of bundle.trades) {
      if (
        String(t.side || '').toUpperCase() !==
        'BUY'
      ) {
        continue;
      }

      const side = String(
        t.outcome || ''
      ).toLowerCase();

      if (side !== 'yes' && side !== 'no') continue;

      const conditionId =
        t.condition_id ||
        t.conditionId;

      if (!conditionId) continue;

      const timestamp = Number(
        t.timestamp ||
        t.block_timestamp ||
        0
      );

      if (!timestamp) continue;

      const key =
        `${bundle.user}|${conditionId}|${side}`;

      const old = buys.get(key);

      if (!old || timestamp < old.timestamp) {
        buys.set(key, {
          timestamp,
          amount: Number(
            t.usdc_size ??
            t.usdcSize ??
            (
              Number(t.size || 0) *
              Number(t.price || 0)
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

    /*
      TRUE consensus:
        YES >= 2 and NO = 0
      OR
        NO >= 2 and YES = 0
    */
    let side;
    let group;

    if (yes.size >= 2 && no.size === 0) {
      side = 'YES';
      group = yes;
    } else if (no.size >= 2 && yes.size === 0) {
      side = 'NO';
      group = no;
    } else {
      continue;
    }

    let firstBuy = Infinity;
    let totalEntry = 0;

    const holders = [];

    for (const holder of group.values()) {
      totalEntry += holder.entry;

      const key =
        `${holder.user}|${market.conditionId}|${side.toLowerCase()}`;

      const buy = buys.get(key);

      if (buy?.timestamp) {
        firstBuy = Math.min(
          firstBuy,
          buy.timestamp
        );
      }

      holders.push({
        amount: holder.entry,
        buyTime: buy?.timestamp
          ? new Date(
              buy.timestamp * 1000
            ).toISOString()
          : null
      });
    }

    /*
      If we don't have a recent trade timestamp,
      don't pretend we know when the position was bought.
    */
    if (!Number.isFinite(firstBuy)) continue;

    const ageHours =
      (now - firstBuy) / 3600;

    if (ageHours > 48) continue;

    /*
      Ignore markets ending more than 90 days away.
    */
    if (market.endDate) {
      const end = new Date(
        market.endDate
      ).getTime();

      if (Number.isFinite(end)) {
        const days =
          (end - Date.now()) / 86400000;

        if (days > 90) continue;
      }
    }

    rows.push({
      title: market.title,
      side,
      sameSide: group.size,
      oppositeSide: side === 'YES'
        ? no.size
        : yes.size,
      totalEntry,
      firstBuy:
        new Date(
          firstBuy * 1000
        ).toISOString(),
      holdLabel: ageLabel(firstBuy),
      traders: holders
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
    `Qualifying consensus markets: ${rows.length}`
  );

  return rows.slice(0, 1000);
}

/*
  3-month leaderboard.

  We use the official ALL leaderboard directly.
  This keeps the scheduled Action fast and reliable.
*/
async function build3m() {
  const source = await leaderboard('ALL', 100);

  return {
    periodLabel: '3 Months',
    generatedAt: new Date().toISOString(),
    leaders: source.map((x, i) => ({
      rank: i + 1,
      name: name(x),
      pnl: Number(x.pnl || 0),
      volume: Number(
        x.vol ??
        x.volume ??
        0
      ),
      wins: Number(x.wins || 0),
      losses: Number(x.losses || 0),
      winRate:
        Number(x.wins || 0) +
        Number(x.losses || 0)
          ? 100 *
            Number(x.wins || 0) /
            (
              Number(x.wins || 0) +
              Number(x.losses || 0)
            )
          : null
    }))
  };
}

async function main() {
  await fs.mkdir('data', {
    recursive: true
  });

  console.log('Starting scanner...');

  const week = await buildLeaders(
    'WEEK',
    '1 Week'
  );

  const consensusResults =
    await consensus();

  await fs.writeFile(
    'data/week.json',
    JSON.stringify(
      {
        ...week,
        consensus: consensusResults
      },
      null,
      2
    )
  );

  const month = await buildLeaders(
    'MONTH',
    '1 Month'
  );

  await fs.writeFile(
    'data/month.json',
    JSON.stringify(
      {
        ...month,
        consensus: consensusResults
      },
      null,
      2
    )
  );

  const three = await build3m();

  await fs.writeFile(
    'data/threeMonth.json',
    JSON.stringify(
      {
        ...three,
        consensus: consensusResults
      },
      null,
      2
    )
  );

  await fs.writeFile(
    'data/status.json',
    JSON.stringify(
      {
        ok: true,
        generatedAt:
          new Date().toISOString(),
        consensusMarkets:
          consensusResults.length,
        weeklyTraders:
          week.leaders.length
      },
      null,
      2
    )
  );

  console.log('==============================');
  console.log('SCAN COMPLETE');
  console.log(
    `Weekly traders: ${week.leaders.length}`
  );
  console.log(
    `Consensus markets: ${consensusResults.length}`
  );
  console.log('==============================');
}

main().catch(async error => {
  console.error(
    'SCANNER FAILED:',
    error
  );

  await fs.writeFile(
    'data/status.json',
    JSON.stringify(
      {
        ok: false,
        error: error.message,
        generatedAt:
          new Date().toISOString()
      },
      null,
      2
    )
  );

  process.exit(1);
});
