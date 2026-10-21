import fs from "node:fs/promises";

const API = "https://data-api.polymarket.com";
const GAMMA = "https://gamma-api.polymarket.com";
const FILE = "forward-test.json";
const now = Date.now();
const iso = n => n ? new Date(n).toISOString() : null;
const num = x => x == null || x === "" || !Number.isFinite(Number(x)) ? null : Number(x);
const arr = x => Array.isArray(x) ? x : [];
const truthy = x => x === true || x === 1 || String(x).toLowerCase() === "true" || String(x).toLowerCase() === "1";
async function get(url) {
  const r = await fetch(url, {headers:{Accept:"application/json","User-Agent":"PolymarketForwardTest/1.0"},signal:AbortSignal.timeout(20000)});
  if (!r.ok) throw new Error("HTTP "+r.status+" for "+url);
  return r.json();
}
async function readJson(path, fallback) { try { return JSON.parse(await fs.readFile(path,"utf8")); } catch { return fallback; } }
function category(m) {
  const s = [m.title,m.question,m.slug,m.category,...arr(m.tags).map(t=>typeof t==="string"?t:t.label||t.slug)].filter(Boolean).join(" ").toLowerCase();
  if (/nfl|nba|wnba|mlb|nhl|soccer|football|basketball|tennis|ufc|esports|sport|hockey|baseball/.test(s)) return "sports";
  if (/bitcoin|btc|ethereum|crypto|solana|eth\b/.test(s)) return "crypto";
  if (/election|president|congress|senate|politic|trump|democrat|republican/.test(s)) return "politics";
  return "other";
}
async function marketFor(condition) {
  const data = await get(GAMMA+"/markets?condition_ids="+encodeURIComponent(condition));
  const m = arr(data)[0];
  if (!m) return null;
  const parse = v => { try { return Array.isArray(v) ? v : JSON.parse(v); } catch { return []; } };
  return {...m, outcomes:parse(m.outcomes), outcomePrices:parse(m.outcomePrices), clobTokenIds:parse(m.clobTokenIds)};
}
const old = await readJson(FILE,{generatedAt:null,methodology:"Forward paper test at the first observed market price; hypothetical $10 stake; no fees/slippage unless added later.",signals:[]});
const signals = Array.isArray(old.signals) ? old.signals : [];
const byKey = new Map(signals.map(s=>[s.key,s]));
const sources = [];
for (const file of ["consensus-week.json","consensus-month.json"]) {
  try { const d=await readJson(file,null); for (const g of arr(d?.consensus)) sources.push({...g,pool:file.includes("week")?"week":"month"}); } catch {}
}
const seen = new Set();
const freshCutoff = now - 60 * 60 * 1000;
const orderedSources = sources.sort((a,b)=>(num(b.newestBuy)||0)-(num(a.newestBuy)||0));
for (const g of orderedSources) {
  const condition = String(g.conditionId||g.condition_id||"").toLowerCase();
  const side = String(g.side||"").trim();
  const allTraders = arr(g.traders);
  const recentTraders = allTraders.filter(t => { const ts=num(t.buyTime); return ts!==null && ts>=freshCutoff && ts<=now; });
  const wallets = [...new Set(allTraders.map(t=>String(t.wallet||"").toLowerCase()).filter(Boolean))];
  const recentWallets = [...new Set(recentTraders.map(t=>String(t.wallet||"").toLowerCase()).filter(Boolean))];
  if (!condition || !side) continue;
  const key=condition+"|"+side.toLowerCase();
  let row=byKey.get(key);
  if (!row && recentWallets.length<2) continue;
  seen.add(key);
  try {
    const m=await marketFor(condition);
    if (!m) continue;
    const outcomes=arr(m.outcomes).map(String);
    const index=outcomes.findIndex(o=>o.toLowerCase()===side.toLowerCase());
    const prices=arr(m.outcomePrices).map(num);
    const entryPrice=index>=0?prices[index]:null;
    if (!row) {
      row={key,conditionId:condition,title:g.title||m.question||m.title||"Untitled market",side,category:category({...m,title:g.title||m.question}),firstSeenAt:iso(now),entryPrice,entryPriceSource:"Market outcome price when at least two qualified traders had recent position activity",stake:10,shares:entryPrice>0&&entryPrice<1?10/entryPrice:null,consensusCount:recentWallets.length,traderWallets:wallets,pool:g.pool,consensusEntryValue:num(g.totalEntry),consensusCurrentValue:num(g.totalValue),recentTraderEntryCount:recentWallets.length,firstSignalPriceAvailable:entryPrice>0&&entryPrice<1,status:entryPrice>0&&entryPrice<1?"PENDING":"ENTRY_UNAVAILABLE",resolvedAt:null,settlement:null,paperPnl:null,exitObservations:[]};
      signals.push(row); byKey.set(key,row);
    } else {
      row.lastSeenAt=iso(now); row.latestConsensusCount=recentWallets.length;
      row.consensusEntryValue=num(g.totalEntry); row.consensusCurrentValue=num(g.totalValue); row.recentTraderEntryCount=recentWallets.length;
      row.traderWallets=[...new Set([...(row.traderWallets||[]),...wallets])];
    }
    row.currentPrice = index >= 0 ? prices[index] : null;
    row.currentValue = row.shares != null && row.currentPrice != null ? row.shares * row.currentPrice : null;
    row.unrealizedPaperPnl = row.currentValue != null ? row.currentValue - row.stake : null;
    row.lastPriceCheckAt = iso(now);
    const isClosed=truthy(m.closed) || m.active === false || String(m.active).toLowerCase() === "false";
    const isResolved=truthy(m.resolved) || ["resolved","finalized"].includes(String(m.status||"").toLowerCase()) || String(m.umaResolutionStatus||"").toLowerCase()==="resolved";
    row.marketDiagnostics={closed:truthy(m.closed),active:m.active??null,resolved:truthy(m.resolved),status:m.status??null,umaResolutionStatus:m.umaResolutionStatus??null,endDate:m.endDate??null,closedTime:m.closedTime??null,conditionId:condition};
    row.resolutionCheckReason=(isClosed||isResolved)?((index>=0 && prices[index]!=null && (prices[index]>=0.99 || prices[index]<=0.01))?"closed_with_terminal_price":"closed_but_price_not_terminal"):"market_not_closed_or_resolved";
    if ((isClosed||isResolved) && row.status==="PENDING" && row.firstSignalPriceAvailable) {
      const pricesNow=arr(m.outcomePrices).map(num);
      const win=index>=0 && pricesNow[index]!==null && pricesNow[index]>=0.99;
      const lose=index>=0 && pricesNow[index]!==null && pricesNow[index]<=0.01;
      if (win||lose) {
        row.status="RESOLVED"; row.settlement=win?"WIN":"LOSS"; row.resolvedAt=iso(now);
        row.paperPnl=win?(row.shares*1-row.stake):-row.stake;
      }
    }
    // Track current observable positions and recent buy/sell activity for the consensus wallets.
    for (const wallet of wallets.slice(0,4)) {
      try {
        const u=await get(API+"/trades?user="+encodeURIComponent(wallet)+"&limit=100");
        const trades=arr(u).filter(t=>String(t.conditionId||t.condition_id||"").toLowerCase()===condition).slice(0,100);
        const latest=trades.map(t=>({side:String(t.side||"").toUpperCase(),outcome:t.outcome||null,price:num(t.price),size:num(t.size),timestamp:num(t.timestamp),wallet})).filter(t=>t.side==="BUY"||t.side==="SELL").sort((a,b)=>(b.timestamp||0)-(a.timestamp||0)).slice(0,10);
        for (const t of latest) {
          const id=[wallet,t.side,t.timestamp,t.price,t.size].join("|");
          if (!row.exitObservations.some(x=>x.id===id)) row.exitObservations.push({id,...t,observedAt:iso(now)});
        }
      } catch (e) { console.warn("trade history fetch failed",wallet,e.message); }
    }
    row.exitObservations=row.exitObservations.slice(-300);
  } catch(e) { console.warn("market snapshot failed",condition,e.message); }
}
// Continue checking every pending signal even after it falls off the current consensus list.
for (const row of signals.filter(s=>s.status==="PENDING" && s.firstSignalPriceAvailable)) {
  try {
    const m=await marketFor(row.conditionId);
    if (!m) continue;
    const outcomes=arr(m.outcomes).map(String);
    const index=outcomes.findIndex(o=>o.toLowerCase()===String(row.side).toLowerCase());
    const prices=arr(m.outcomePrices).map(num);
    const price=index>=0?prices[index]:null;
    const closed=truthy(m.closed) || m.active===false || String(m.active).toLowerCase()==="false" || truthy(m.resolved) || ["resolved","finalized"].includes(String(m.status||"").toLowerCase()) || String(m.umaResolutionStatus||"").toLowerCase()==="resolved";
    row.marketDiagnostics={closed:truthy(m.closed),active:m.active??null,resolved:truthy(m.resolved),status:m.status??null,umaResolutionStatus:m.umaResolutionStatus??null,endDate:m.endDate??null,closedTime:m.closedTime??null,conditionId:row.conditionId};
    row.resolutionCheckReason=closed?((price!=null && (price>=0.99 || price<=0.01))?"closed_with_terminal_price":"closed_but_price_not_terminal"):"market_not_closed_or_resolved";
    row.currentPrice = price;
    row.currentValue = row.shares != null && price != null ? row.shares * price : null;
    row.unrealizedPaperPnl = row.currentValue != null ? row.currentValue - row.stake : null;
    row.lastPriceCheckAt = iso(now);
    if (!closed || price===null) continue;
    if (price>=0.99 || price<=0.01) {
      const won=price>=0.99;
      row.status="RESOLVED"; row.settlement=won?"WIN":"LOSS"; row.resolvedAt=iso(now);
      row.finalOutcomePrice=price;
      row.paperPnl=won?(Number(row.shares)*1-Number(row.stake)):-Number(row.stake);
    }
  } catch(e) { console.warn("pending signal check failed",row.conditionId,e.message); }
}
const resolved=signals.filter(s=>s.status==="RESOLVED");
const wins=resolved.filter(s=>s.settlement==="WIN").length;
const losses=resolved.filter(s=>s.settlement==="LOSS").length;
let streak=0,worst=0;
for (const s of resolved.sort((a,b)=>Date.parse(a.resolvedAt)-Date.parse(b.resolvedAt))) { if(s.settlement==="LOSS"){streak++;worst=Math.max(worst,streak)} else streak=0; }
const valid=signals.filter(s=>s.firstSignalPriceAvailable);
const freshSignals=signals.filter(s=>Date.parse(s.firstSeenAt)>=now-24*60*60*1000).length;
const pending=valid.filter(s=>s.status==="PENDING").length;
const pnl=resolved.reduce((n,s)=>n+(num(s.paperPnl)||0),0);
const byCategory={};
for(const c of ["sports","crypto","politics","other"]) { const rows=resolved.filter(s=>s.category===c); byCategory[c]={resolved:rows.length,wins:rows.filter(s=>s.settlement==="WIN").length,losses:rows.filter(s=>s.settlement==="LOSS").length,paperPnl:rows.reduce((n,s)=>n+(num(s.paperPnl)||0),0)}; }
const marked=valid.filter(s=>s.currentPrice!=null);
const consensusEntryValue=signals.filter(s=>s.status==="PENDING").reduce((n,s)=>n+(num(s.consensusEntryValue)||0),0);
const consensusCurrentValue=signals.filter(s=>s.status==="PENDING").reduce((n,s)=>n+(num(s.consensusCurrentValue)||0),0);
const notChecked=valid.filter(s=>!s.resolutionCheckReason).length;
const openMarkets=valid.filter(s=>s.status==="PENDING"&&s.resolutionCheckReason==="market_not_closed_or_resolved").length;
const closedAwaitingPrice=valid.filter(s=>s.status==="PENDING"&&s.resolutionCheckReason==="closed_but_price_not_terminal").length;
const terminalAwaitingSettlement=valid.filter(s=>s.status==="PENDING"&&s.resolutionCheckReason==="closed_with_terminal_price").length;
const sellObservations=signals.reduce((n,s)=>n+(s.exitObservations||[]).filter(t=>t.side==="SELL").length,0);
const output={generatedAt:iso(now),methodology:"Forward copy test records a signal only when at least two qualified consensus traders show recent position activity within 60 minutes. Entry is the market outcome price observed by this tracker, not the traders’ historical execution price. Consensus invested/current holding values are reported from open-position snapshots and are not the paper test’s P&L. Settlement requires an explicitly closed/resolved market and terminal outcome price. SELL prints are observations, not proof of full exits; partial exits and fees/slippage are not modeled.",metrics:{signals:signals.length,entryPriced:valid.length,freshSignals,pending,resolved:resolved.length,wins,losses,openMarkets,closedAwaitingPrice,terminalAwaitingSettlement,notChecked,markedSignals:marked.length,consensusEntryValue,consensusCurrentValue,sellObservations,winRate:resolved.length?wins/resolved.length*100:null,paperPnl:pnl,worstConsecutiveLosses:worst,byCategory},signals:signals.sort((a,b)=>Date.parse(b.firstSeenAt)-Date.parse(a.firstSeenAt)).slice(0,1500)};
await fs.writeFile(FILE,JSON.stringify(output,null,2)+"\n");
console.log("Forward test: "+valid.length+" entry-priced signals; "+resolved.length+" resolved; "+wins+" wins / "+losses+" losses; paper P&L $"+pnl.toFixed(2));
