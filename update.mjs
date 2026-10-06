import fs from 'node:fs/promises';
const API='https://data-api.polymarket.com';
const now=Math.floor(Date.now()/1000);
const DAY=86400;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function get(path, params={}, tries=5){
  const u=new URL(API+path); for(const [k,v] of Object.entries(params)) if(v!==undefined&&v!==null&&v!=='') u.searchParams.set(k,String(v));
  for(let i=0;i<tries;i++){
    const r=await fetch(u,{headers:{'user-agent':'polymarket-consensus-scanner/1.0'}});
    if(r.ok)return r.json();
    if((r.status===429||r.status>=500)&&i<tries-1){const ra=Number(r.headers.get('retry-after'));await sleep((Number.isFinite(ra)?ra:Math.min(2**i*2,20))*1000);continue}
    throw new Error(`${r.status} ${await r.text()}`);
  }
}
async function leaderboard(period){
  const out=[];
  for(let offset=0;offset<1000;offset+=50){
    const page=await get('/v1/leaderboard',{category:'OVERALL',timePeriod:period,orderBy:'PNL',limit:50,offset});
    if(!Array.isArray(page)||!page.length)break; out.push(...page);
    if(page.length<50)break;
    await sleep(100);
  }
  return out.slice(0,1000);
}
function rowName(x){return x.userName||x.username||x.pseudonym||x.name||'Unknown'}
async function closedStats(user,start){
  let wins=0,losses=0;
  let cursor=null;
  for(;;){
    const page=await get('/v2/positions',{user,status:'CLOSED',limit:50,start,sortBy:'TIMESTAMP',sortDirection:'DESC',...(cursor?{cursor}:{})}).catch(()=>null);
    if(!page?.data?.length)break;
    for(const p of page.data){if(Number(p.realized_pnl)>0)wins++; else if(Number(p.realized_pnl)<0)losses++}
    if(!page.pagination?.has_more||!page.pagination?.next_cursor)break;
    cursor=page.pagination.next_cursor;
    await sleep(70);
  }
  return {wins,losses};
}
async function volume(user,start){const x=await get('/v2/user-volume',{user,start,end:now});return Number(x?.data?.volume_usdc||0)}
async function pnl3m(user){
  const x=await get('/v2/user-pnl',{user,interval:'all',fidelity:'1d'});const pts=x?.data?.points||[];if(!pts.length)return 0;
  const cutoff=now-90*DAY;let before=pts[0],latest=pts[pts.length-1];for(const p of pts){if(Number(p.timestamp)<=cutoff)before=p;if(Number(p.timestamp)<=now)latest=p}
  const val=p=>Number(p?.economic_pnl ?? p?.realized_pnl ?? 0);return val(latest)-val(before);
}
async function buildLeaders(period,label){
  const source=await leaderboard(period);const start=period==='WEEK'?now-7*DAY:now-30*DAY;
  const top=source.slice(0,100);const out=[];
  for(let i=0;i<top.length;i+=8){
    const batch=top.slice(i,i+8);const vals=await Promise.all(batch.map(async x=>{const user=x.proxyWallet||x.address||x.user;const s=await closedStats(user,start);return {...x,name:rowName(x),pnl:Number(x.pnl||0),volume:Number(x.vol||x.volume||0),wins:s.wins,losses:s.losses,winRate:(s.wins+s.losses)?100*s.wins/(s.wins+s.losses):null}}));out.push(...vals);await sleep(150)}
  return {periodLabel:label,generatedAt:new Date().toISOString(),leaders:out.map((x,i)=>({rank:i+1,name:x.name,pnl:x.pnl,volume:x.volume,wins:x.wins,losses:x.losses,winRate:x.winRate}))};
}
async function fetchPositions(users){
  const all=[];const queue=[...users];let active=0;let idx=0;
  async function worker(){while(idx<queue.length){const x=queue[idx++];try{const j=await get('/v2/positions',{user:x.proxyWallet||x.address, status:'OPEN', limit:500, sortBy:'TIMESTAMP',sortDirection:'DESC'});all.push({user:x,positions:j.data||[]})}catch(e){console.error('position',x.proxyWallet,e.message)}}}
  await Promise.all(Array.from({length:10},worker));return all;
}
async function activityFirstBuys(users){
  const map=new Map();
  let idx=0;async function worker(){while(idx<users.length){const u=users[idx++];try{const j=await get('/v2/trades',{user:u.proxyWallet||u.address,start:now-7*DAY,end:now,limit:500,sortDirection:'ASC'});for(const t of j.data||[]){if(String(t.side).toUpperCase()!=='BUY')continue;const key=`${t.condition_id||t.conditionId}|${String(t.outcome||'').toLowerCase()}`;const ts=Number(t.timestamp||t.block_timestamp||0);const cash=Number(t.usdc_size??t.usdcSize??(Number(t.size||0)*Number(t.price||0)));const old=map.get(`${u.proxyWallet||u.address}|${key}`);if(!old||ts<old.ts)map.set(`${u.proxyWallet||u.address}|${key}`,{ts,cash})}}catch(e){console.error('trade',u.proxyWallet,e.message)}}}
  await Promise.all(Array.from({length:10},worker));return map;
}
async function consensus(){
  const traders=await leaderboard('WEEK');const universe=traders.slice(0,1000);const positions=await fetchPositions(universe);const buys=await activityFirstBuys(universe);const markets=new Map();
  for(const bundle of positions){for(const p of bundle.positions){if(p.archived||Number(p.current_size||0)<=0)continue;const out=String(p.outcome||'').toLowerCase();if(out!=='yes'&&out!=='no')continue;const key=p.condition_id||p.conditionId;if(!key)continue;let m=markets.get(key);if(!m){m={conditionId:key,title:p.title||p.name||'Untitled',yes:new Map(),no:new Map(),endDate:p.end_date||null}};const side=out==='yes'?'yes':'no';const user=bundle.user.proxyWallet||bundle.user.address;const entry=Number(p.entry_cost_usdc??p.total_cost_usdc??0);const k=`${user}`;m[side].set(k,{user,entry,position:p});markets.set(key,m)}}
  const rows=[];for(const m of markets.values()){const same=m.yes.size,opp=m.no.size;if(!same||opp>0)continue;let total=0,first=Infinity;for(const x of m.yes.values()){total+=x.entry;const b=buys.get(`${x.user}|${m.conditionId}|yes`);if(b?.ts)first=Math.min(first,b.ts)}const firstBuy=Number.isFinite(first)?new Date(first*1000).toISOString():null;const ageHours=firstBuy?(Date.now()-new Date(firstBuy).getTime())/36e5:9999;if(ageHours>48)continue;
    if(m.endDate){const daysToEnd=(new Date(m.endDate).getTime()-Date.now())/86400000;if(daysToEnd>90)continue;}rows.push({title:m.title,sameSide:same,oppositeSide:opp,totalEntry:total,firstBuy,holdLabel:firstBuy?age(firstBuy):'Recent'})}
  rows.sort((a,b)=>b.sameSide-a.sameSide||new Date(b.firstBuy||0)-new Date(a.firstBuy||0)||b.totalEntry-a.totalEntry);return rows.slice(0,1000);
}
function age(s){const h=Math.max(0,(Date.now()-new Date(s).getTime())/36e5);if(h<1)return Math.round(h*60)+'m ago';if(h<24)return Math.round(h)+'h ago';return Math.round(h/24)+'d ago'}
async function build3m(){
  const source=await leaderboard('ALL');const candidates=source.slice(0,1000);const rows=[];
  for(let i=0;i<candidates.length;i+=12){const batch=candidates.slice(i,i+12);const vals=await Promise.all(batch.map(async x=>{const user=x.proxyWallet||x.address;const [p,v]=await Promise.all([pnl3m(user),volume(user,now-90*DAY)]);return {name:rowName(x),pnl:p,volume:v,user}}));rows.push(...vals);console.log(`3m metrics ${Math.min(i+12,candidates.length)}/${candidates.length}`);await sleep(100)}
  rows.sort((a,b)=>b.pnl-a.pnl);const top=rows.slice(0,100);
  for(let i=0;i<top.length;i+=10){const batch=top.slice(i,i+10);const stats=await Promise.all(batch.map(x=>closedStats(x.user,now-90*DAY)));batch.forEach((x,j)=>{x.wins=stats[j].wins;x.losses=stats[j].losses;x.winRate=(x.wins+x.losses)?100*x.wins/(x.wins+x.losses):null});await sleep(100)}
  return {periodLabel:'3 Months · calculated from official cumulative PnL series',generatedAt:new Date().toISOString(),leaders:top.map((x,i)=>({rank:i+1,name:x.name,pnl:x.pnl,volume:x.volume,wins:x.wins,losses:x.losses,winRate:(x.wins+x.losses)?100*x.wins/(x.wins+x.losses):null}))};
}
async function main(){await fs.mkdir('data',{recursive:true});const week=await buildLeaders('WEEK','1 Week');await fs.writeFile('data/week.json',JSON.stringify({...week,consensus:await consensus()},null,2));const month=await buildLeaders('MONTH','1 Month');await fs.writeFile('data/month.json',JSON.stringify({...month,consensus:week.consensus},null,2));const three=await build3m();await fs.writeFile('data/threeMonth.json',JSON.stringify({...three,consensus:week.consensus},null,2));await fs.writeFile('data/status.json',JSON.stringify({ok:true,generatedAt:new Date().toISOString()},null,2));}
main().catch(async e=>{console.error(e);await fs.writeFile('data/status.json',JSON.stringify({ok:false,error:e.message,generatedAt:new Date().toISOString()},null,2));process.exit(1)});
