import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const SYMBOL = "XAUUSDT";
const OUTPUT_SYMBOL = "XAUUSD";
const STREAM_URL = `wss://fstream.binance.com/market/ws/${SYMBOL.toLowerCase()}@aggTrade`;

const LOTS = Number(process.env.FOOTPRINT_LOTS || 0.05);
const SL_DISTANCE = Number(process.env.FOOTPRINT_SL_DISTANCE || 1.00);
const RR = Number(process.env.FOOTPRINT_RR || 1.25);
const EXECUTION_ENABLED = String(process.env.FOOTPRINT_EXECUTION_ENABLED || "true").toLowerCase() === "true";

const state = {
  connected: false, reconnects: 0, ticks: 0,
  lastTradeAt: null, lastPrice: null, lastQuantity: null, lastSide: null, lastError: null,
  totalBuyVolume: 0, totalSellVolume: 0, totalDelta: 0,
  candle: { minute:null, open:null, high:null, low:null, close:null, buyVolume:0, sellVolume:0, delta:0, trades:0 },
  levels: new Map(), recentTrades: [], lastExecutionSignalId: null
};
let ws = null, reconnectTimer = null;

function resetCandle(minute, price) {
  state.candle = { minute, open:price, high:price, low:price, close:price, buyVolume:0, sellVolume:0, delta:0, trades:0 };
  state.levels.clear();
}
function ensureCandle(timestamp, price) {
  const minute = Math.floor(timestamp / 60000);
  if (state.candle.minute === null || state.candle.minute !== minute) resetCandle(minute, price);
}
function addLevel(price, side, qty) {
  const key = price.toFixed(2);
  let x = state.levels.get(key);
  if (!x) { x={price,buyVolume:0,sellVolume:0,delta:0,trades:0}; state.levels.set(key,x); }
  if (side === "BUY") { x.buyVolume += qty; x.delta += qty; }
  else { x.sellVolume += qty; x.delta -= qty; }
  x.trades += 1;
}
function processTrade(data) {
  const price=Number(data.p), qty=Number(data.q), timestamp=Number(data.T);
  if (!Number.isFinite(price)||!Number.isFinite(qty)) return;
  const time=Number.isFinite(timestamp)?timestamp:Date.now();
  const side=data.m===true?"SELL":"BUY";
  ensureCandle(time,price);
  const c=state.candle;
  c.high=Math.max(c.high,price); c.low=Math.min(c.low,price); c.close=price; c.trades+=1;
  if(side==="BUY"){c.buyVolume+=qty;state.totalBuyVolume+=qty;state.totalDelta+=qty;}
  else{c.sellVolume+=qty;state.totalSellVolume+=qty;state.totalDelta-=qty;}
  c.delta=c.buyVolume-c.sellVolume; addLevel(price,side,qty);
  state.ticks+=1; state.lastTradeAt=new Date(time).toISOString(); state.lastPrice=price; state.lastQuantity=qty; state.lastSide=side; state.lastError=null;
  state.recentTrades.push({time:state.lastTradeAt,price,quantity:qty,side});
  if(state.recentTrades.length>100) state.recentTrades.shift();
}
function scheduleReconnect(){ if(reconnectTimer)return; state.reconnects+=1; const delay=Math.min(30000,2000*state.reconnects); reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect();},delay); }
function connect(){
  try{if(ws)ws.close();}catch{}
  state.connected=false; ws=new WebSocket(STREAM_URL);
  ws.on("open",()=>{state.connected=true;state.reconnects=0;state.lastError=null;console.log("BINANCE STREAM CONNECTED");});
  ws.on("message",raw=>{try{const msg=JSON.parse(raw.toString()); if(msg?.e==="aggTrade"&&msg.s===SYMBOL)processTrade(msg); else if(msg?.code)state.lastError=`Binance ${msg.code}: ${msg.msg||"unknown error"}`;}catch(e){state.lastError=`JSON parse error: ${e.message}`;}});
  ws.on("error",e=>{state.lastError=`WebSocket error: ${e.message}`;});
  ws.on("close",()=>{state.connected=false;scheduleReconnect();});
}
function candleOutput(){const c=state.candle;return{open:c.open,high:c.high,low:c.low,close:c.close,buyVolume:Number(c.buyVolume.toFixed(8)),sellVolume:Number(c.sellVolume.toFixed(8)),delta:Number(c.delta.toFixed(8)),trades:c.trades};}
function footprintOutput(){return Array.from(state.levels.values()).sort((a,b)=>b.price-a.price).map(x=>({price:x.price,buyVolume:Number(x.buyVolume.toFixed(8)),sellVolume:Number(x.sellVolume.toFixed(8)),delta:Number(x.delta.toFixed(8)),trades:x.trades}));}
function signalOutput(){
  const c=state.candle,total=c.buyVolume+c.sellVolume;
  if(!c.trades||total<=0)return{signal:"WAIT",setup:"NO_DATA",reason:"Waiting for live footprint data"};
  const deltaPercent=(c.delta/total)*100;
  if(c.close<c.open&&deltaPercent>=20)return{signal:"SELL",setup:"BEARISH_DELTA_DIVERGENCE",deltaPercent:Number(deltaPercent.toFixed(2)),reason:"Red candle with positive aggressive-buy delta."};
  if(c.close>c.open&&deltaPercent<=-20)return{signal:"BUY",setup:"BULLISH_DELTA_DIVERGENCE",deltaPercent:Number(deltaPercent.toFixed(2)),reason:"Green candle with negative aggressive-sell delta."};
  return{signal:"WAIT",setup:"NONE",deltaPercent:Number(deltaPercent.toFixed(2)),reason:"No configured divergence condition."};
}
function buildExecutionSignal(){
  const s=signalOutput(), price=Number(state.lastPrice);
  if(!Number.isFinite(price))return{success:true,instrument:OUTPUT_SYMBOL,signal:"WAIT",setup:"NO_DATA",executable:false,dryRun:!EXECUTION_ENABLED,autoTrade:EXECUTION_ENABLED,lots:LOTS,levels:null,reason:"No live price yet."};
  if(s.signal!=="BUY"&&s.signal!=="SELL")return{success:true,instrument:OUTPUT_SYMBOL,sourceInstrument:SYMBOL,generatedAt:new Date().toISOString(),signal:"WAIT",setup:s.setup,executable:false,dryRun:!EXECUTION_ENABLED,autoTrade:EXECUTION_ENABLED,lots:LOTS,priceSource:"Binance Futures XAUUSDT aggTrade",currentPrice:price,levels:null,diagnostic:s};
  const entry=price;
  const risk=SL_DISTANCE;
  const sl=s.signal==="BUY"?entry-risk:entry+risk;
  const tp=s.signal==="BUY"?entry+risk*RR:entry-risk*RR;
  const signalId=`${s.signal}_${Math.floor(Date.now()/1000)}`;
  return{success:true,instrument:OUTPUT_SYMBOL,sourceInstrument:SYMBOL,generatedAt:new Date().toISOString(),signalId,signal:s.signal,setup:s.setup,executable:EXECUTION_ENABLED,dryRun:!EXECUTION_ENABLED,autoTrade:EXECUTION_ENABLED,lots:LOTS,priceSource:"Binance Futures XAUUSDT aggTrade",currentPrice:price,levels:{entry:Number(entry.toFixed(2)),stopLoss:Number(sl.toFixed(2)),takeProfit:{TP1:Number(tp.toFixed(2))}},diagnostic:s};
}

app.get("/",(req,res)=>res.json({success:true,service:"XAU Footprint Order Flow Execution Engine",instrument:OUTPUT_SYMBOL,sourceInstrument:SYMBOL,dataSource:"Binance Futures public aggTrade",executionEnabled:EXECUTION_ENABLED,lotSize:LOTS,slDistance:SL_DISTANCE,rr:RR,endpoints:["/health","/status","/footprint","/signal","/execution-signal"]}));
app.get("/health",(req,res)=>res.json({success:true,status:"ok",websocketConnected:state.connected,ticks:state.ticks,lastTradeAt:state.lastTradeAt,lastError:state.lastError,executionEnabled:EXECUTION_ENABLED}));
app.get("/status",(req,res)=>res.json({success:true,instrument:OUTPUT_SYMBOL,websocket:{connected:state.connected,endpoint:STREAM_URL,reconnects:state.reconnects},ticks:state.ticks,lastTradeAt:state.lastTradeAt,lastPrice:state.lastPrice,lastQuantity:state.lastQuantity,lastSide:state.lastSide,lastError:state.lastError,executionEnabled:EXECUTION_ENABLED,lots:LOTS,serverTime:new Date().toISOString()}));
app.get("/footprint",(req,res)=>{const c=candleOutput(),total=c.buyVolume+c.sellVolume;res.json({success:true,symbol:SYMBOL,generatedAt:new Date().toISOString(),dataSource:"Binance Futures public aggTrade",dataType:"Live exchange trade stream",websocket:{connected:state.connected,reconnects:state.reconnects},ticks:state.ticks,lastTradeAt:state.lastTradeAt,lastPrice:state.lastPrice,lastQuantity:state.lastQuantity,lastSide:state.lastSide,lastError:state.lastError,totalBuyVolume:Number(state.totalBuyVolume.toFixed(8)),totalSellVolume:Number(state.totalSellVolume.toFixed(8)),totalDelta:Number(state.totalDelta.toFixed(8)),candle:c,summary:{buyVolume:c.buyVolume,sellVolume:c.sellVolume,delta:c.delta,deltaPercent:total?Number(((c.delta/total)*100).toFixed(2)):0,totalTrades:c.trades},footprint:footprintOutput(),recentTrades:state.recentTrades.slice(-20)});});
app.get("/signal",(req,res)=>res.json({success:true,instrument:OUTPUT_SYMBOL,generatedAt:new Date().toISOString(),dataReady:state.ticks>0,websocketConnected:state.connected,ticks:state.ticks,signal:signalOutput(),candle:candleOutput(),autoTrade:EXECUTION_ENABLED}));
app.get("/execution-signal",(req,res)=>res.json(buildExecutionSignal()));

app.listen(PORT,()=>{console.log(`XAU Footprint Execution Engine listening on port ${PORT}`);console.log(`STREAM=${STREAM_URL}`);console.log(`EXECUTION_ENABLED=${EXECUTION_ENABLED} LOTS=${LOTS} SL_DISTANCE=${SL_DISTANCE} RR=${RR}`);connect();});
