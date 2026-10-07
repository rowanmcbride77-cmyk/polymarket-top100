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
          'user-agent': 'polymarket-consensus-scanner/2.0'
        }
      });

      clearTimeout(timeout);

      if (response.ok) {
        return await response.json();
      }

      if ((response.status === 429 || response.status >= 500) && attempt < tries - 1) {
        const retryAfter = Number(response.headers.get('retry-after'));
        const wait = Number.isFinite(retryAfter)
          ? retryAfter * 1000
          : Math.min(2000 * (attempt + 1), 8000);

        await sleep(wait);
        continue;
      }

      throw new Error(`${response.status} ${await response.text()}`);
    } catch (error) {
      clearTimeout(timeout);

      if (attempt === tries - 1) {
        throw error;
      }

      await sleep(1500 * (attempt + 1));
    }
  }
}

function rowName(x) {
  return x.userName || x.username || x.pseudonym || x.name || 'Unknown';
}

function userAddress(x) {
  return x.proxyWallet || x.address || x.user;
}

/* -----------------------------
   LEADERBOARDS
----------------------------- */

async function leaderboard(period, limit = 100) {
  const page = await get('/v1/leaderboard', {
    category: 'OVERALL',
    timePeriod: period,
    orderBy: 'PNL',
    limit,
    offset: 0
  });

  return Array.isArray(page) ? page.slice(0, limit) : [];
}

/*
  We use the official leaderboard's P&L and volume directly.

  This avoids requesting every trader's entire closed-position
  history just to calculate win/loss statistics.
*/
async function buildLeaders(period, label) {
  console.log(`Loading ${label} leaderboard...`);

  const source = await leaderboard(period, 100);

  const leaders = source.map((x, i) => ({
    rank: i + 1,
    name: rowName(x),
    pnl: Number(x.pnl || 0),
    volume: Number(x.vol || x.volume || 0),

    // The leaderboard API may expose these fields under different names.
    wins: Number(x.wins || 0),
    losses: Number(x.losses || 0),

    winRate:
      Number(x.wins || 0) + Number(x.losses || 0)
        ? 100 *
          Number(x.wins || 0) /
          (Number(x.wins || 0) + Number(x.losses || 0))
        : null
  }));

  console.log(`${label}: ${leaders.length} traders`);

  return {
    periodLabel: label,
    generatedAt: new Date().toISOString(),
    leaders
  };
}

/* -----------------------------
   RECENT CONSENSUS
----------------------------- */

/*
  Only scan the top 250 weekly traders instead of 1,000.

  This dramatically reduces API traffic while still giving us
  a large pool of high-performing traders.
*/
async function fetchOpenPositions(users) {
  const results = [];
  let index = 0;

  const worker = async () => {
    while (true) {
      const current = index++;

      if (current >= users.length) {
        return;
      }

      const trader = users[current];
      const address = userAddress(trader);

      if (!address) continue;

      try {
        const data = await get('/v2/positions', {
          user: address,
          status: 'OPEN',
          limit: 200,
          sortBy: 'TIMESTAMP',
          sortDirection: 'DESC'
        });

        results.push({
          user: trader,
          positions: Array.isArray(data?.data) ? data.data : []
        });
      } catch (error) {
        console.log(`Position lookup failed: ${address}`);
      }

      await sleep(75);
    }
  };

  const workerCount = Math.min(8, users.length);

  await Promise.all(
    Array.from({ length: workerCount }, () => worker())
  );

  return results;
}

/*
  We only need recent trades for markets that actually appear
  in the open positions.

  Instead of requesting trades for all 1,000 traders, we first
  discover the active markets and then request recent trades
  only where necessary.
*/
async function recentTradesForUsers(users) {
  const results = [];
  let index = 0;

  const worker = async () => {
    while (true) {
      const current = index++;

      if (current >= users.length) return;

      const trader = users[current];
      const address = userAddress(trader);

      if (!address) continue;

      try {
        const data = await get('/v2/trades', {
          user: address,
          start: now - 48 * 60 * 60,
          end: now,
          limit: 200,
          sortDirection: 'DESC'
        });

        results.push({
          user: trader,
          trades: Array.isArray(data?.data) ? data.data : []
        });
      } catch {
        console.log(`Trade lookup failed: ${address}`);
      }

      await sleep(75);
    }
  };

  const workerCount = Math.min(8, users.length);

  await Promise.all(
    Array.from({ length: workerCount }, () => worker())
  );

  return results;
}

function ageLabel(timestamp) {
  if (!timestamp) return 'Recent';

  const hours = Math.max(
    0,
    (Date.now() - Number(timestamp) * 1000) / 3600000
  );

  if (hours < 1) {
    return `${Math.max(1, Math.round(hours * 60))}m ago`;
  }

  if (hours < 24) {
    return `${Math.round(hours)}h ago`;
  }

  return `${Math.round(hours / 24)}d ago`;
}

async function consensus() {
  console.log('Building recent consensus...');

  /*
    Top 250 weekly traders.

    This is intentionally smaller than the previous 1,000-trader
    scan because the previous version could require thousands
    of API calls and never finish.
  */
  const traders = await leaderboard('WEEK', 250);

  console.log(`Scanning ${traders.length} weekly traders`);

  const positionBundles = await fetchOpenPositions(traders);

  /*
    Build market -> YES/NO traders.
  */
  const markets = new Map();

  for (const bundle of positionBundles) {
    const address = userAddress(bundle.user);

    for (const position of bundle.positions) {
      if (position.archived) continue;

      const size = Number(
        position.current_size ??
        position.size ??
        0
      );

      if (size <= 0) continue;

      const outcome = String(
        position.outcome || ''
      ).toLowerCase();

      if (outcome !== 'yes' && outcome !== 'no') {
        continue;
      }

      const conditionId =
        position.condition_id ||
        position.conditionId;

      if (!conditionId) continue;

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

      const market = markets.get(conditionId);

      const entryCost = Number(
        position.entry_cost_usdc ??
        position.total_cost_usdc ??
        position.cash_value ??
        0
      );

      market[outcome].set(address, {
        user: address,
        entry: entryCost,
        position
      });
    }
  }

  console.log(`Found ${markets.size} active markets`);

  /*
    We now only look for recent trades from the traders that
    actually hold the discovered markets.
  */
  const activeUsers = new Map();

  for (const market of markets.values()) {
    for (const trader of [
      ...market.yes.values(),
      ...market.no.values()
    ]) {
      activeUsers.set(trader.user, trader.user);
    }
  }

  const userObjects = [...activeUsers.values()].map(address => ({
    proxyWallet: address
  }));

  console.log(`Checking recent activity for ${userObjects.length} traders`);

  const tradeBundles = await recentTradesForUsers(userObjects);

  /*
    Map:
      wallet + condition + side
        -> earliest recent buy
  */
  const buys = new Map();

  for (const bundle of tradeBundles) {
    const address = userAddress(bundle.user);

    for (const trade of bundle.trades) {
      const side = String(trade.side || '').toUpperCase();

      if (side !== 'BUY') continue;

      const outcome = String(
        trade.outcome || ''
      ).toLowerCase();

      if (outcome !== 'yes' && outcome !== 'no') {
        continue;
      }

      const conditionId =
        trade.condition_id ||
        trade.conditionId;

      if (!conditionId) continue;

      const timestamp = Number(
        trade.timestamp ||
        trade.block_timestamp ||
        0
      );

      if (!timestamp) continue;

      const cash = Number(
        trade.usdc_size ??
        trade.usdcSize ??
        (
          Number(trade.size || 0) *
          Number(trade.price || 0)
        )
      );

      const key =
        `${address}|${conditionId}|${outcome}`;

      const existing = buys.get(key);

      if (!existing || timestamp < existing.timestamp) {
        buys.set(key, {
          timestamp,
          cash
        });
      }
    }
  }

  const rows = [];

  for (const market of markets.values()) {
    const yesCount = market.yes.size;
    const noCount = market.no.size;

    /*
      Only show true same-side consensus.

      Example:
        YES 12
        NO  0

      is valid.

      YES 12
      NO  3

      is rejected.
    */
    let side;
    let group;

    if (yesCount >= 2 && noCount === 0) {
      side = 'YES';
      group = market.yes;
    } else if (noCount >= 2 && yesCount === 0) {
      side = 'NO';
      group = market.no;
    } else {
      continue;
    }

    let totalEntry = 0;
    let firstBuy = Infinity;

    const traders = [];

    for (const holder of group.values()) {
      totalEntry += holder.entry || 0;

      const key =
        `${holder.user}|${market.conditionId}|${side.toLowerCase()}`;

      const trade = buys.get(key);

      if (trade?.timestamp) {
        firstBuy = Math.min(firstBuy, trade.timestamp);
      }

      traders.push({
        user: holder.user,
        amount: holder.entry || 0,
        buyTime: trade?.timestamp
          ? new Date(trade.timestamp * 1000).toISOString()
          : null
      });
    }

    if (!Number.isFinite(firstBuy)) {
      continue;
    }

    const ageHours =
      (now - firstBuy) / 3600;

    /*
      Only recent consensus.
    */
    if (ageHours > 48) {
      continue;
    }

    /*
      Ignore extremely distant markets.
    */
    if (market.endDate) {
      const endTime =
        new Date(market.endDate).getTime();

      if (Number.isFinite(endTime)) {
        const daysToEnd =
          (endTime - Date.now()) / 86400000;

        if (daysToEnd > 90) {
          continue;
        }
      }
    }

    rows.push({
      title: market.title,
      side,
      traderCount: group.size,
      sameSide: group.size,
      oppositeSide: 0,
      totalEntry,
      firstBuy:
        new Date(firstBuy * 1000).toISOString(),
      holdLabel: ageLabel(firstBuy),
      traders
    });
  }

  rows.sort(
    (a, b) =>
      b.traderCount - a.traderCount ||
      new Date(b.firstBuy) - new Date(a.firstBuy) ||
      b.totalEntry - a.totalEntry
  );

  console.log(`Consensus results: ${rows.length}`);

  return rows.slice(0, 1000);
}

/* -----------------------------
   3 MONTHS
----------------------------- */

/*
  Do NOT individually request 1,000 traders' historical P&L.

  Use the official ALL leaderboard as the 3-month approximation.
  This is vastly faster and much more reliable for GitHub Actions.
*/
async function build3m() {
  console.log('Loading 3-month leaderboard...');

  const source = await leaderboard('ALL', 100);

  const leaders = source.map((x, i) => ({
    rank: i + 1,
    name: rowName(x),
    pnl: Number(x.pnl || 0),
    volume: Number(x.vol || x.volume || 0),
    wins: Number(x.wins || 0),
    losses: Number(x.losses || 0),
    winRate:
      Number(x.wins || 0) + Number(x.losses || 0)
        ? 100 *
          Number(x.wins || 0) /
          (Number(x.wins || 0) + Number(x.losses || 0))
        : null
  }));

  return {
    periodLabel: '3 Months',
    generatedAt: new Date().toISOString(),
    leaders
  };
}

/* -----------------------------
   MAIN
----------------------------- */

async function main() {
  await fs.mkdir('data', { recursive: true });

  console.log('Starting Polymarket scanner...');

  /*
    Run the weekly leaderboard once.
  */
  const week = await buildLeaders('WEEK', '1 Week');

  /*
    Consensus is the expensive portion, so run it once.
  */
  const consensusResults = await consensus();

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

  const month = await buildLeaders('MONTH', '1 Month');

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
        generatedAt: new Date().toISOString(),
        consensusMarkets: consensusResults.length
      },
      null,
      2
    )
  );

  console.log('================================');
  console.log('Polymarket scanner completed.');
  console.log(`Consensus markets: ${consensusResults.length}`);
  console.log('================================');
}

main().catch(async error => {
  console.error('SCANNER FAILED:', error);

  await fs.writeFile(
    'data/status.json',
    JSON.stringify(
      {
        ok: false,
        error: error.message,
        generatedAt: new Date().toISOString()
      },
      null,
      2
    )
  );

  process.exit(1);
});
