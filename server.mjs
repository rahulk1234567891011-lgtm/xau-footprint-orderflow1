import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const SYMBOL = "XAUUSDT";
const STREAM_URL = `wss://fstream.binance.com/market/ws/${SYMBOL.toLowerCase()}@aggTrade`;

const state = {
  connected: false,
  reconnects: 0,
  ticks: 0,
  lastTradeAt: null,
  lastPrice: null,
  lastQuantity: null,
  lastSide: null,
  lastError: null,
  totalBuyVolume: 0,
  totalSellVolume: 0,
  totalDelta: 0,
  candle: { minute: null, open: null, high: null, low: null, close: null, buyVolume: 0, sellVolume: 0, delta: 0, trades: 0 },
  levels: new Map(),
  recentTrades: []
};

let ws = null;
let reconnectTimer = null;

function resetCandle(minute, price) {
  state.candle = { minute, open: price, high: price, low: price, close: price, buyVolume: 0, sellVolume: 0, delta: 0, trades: 0 };
  state.levels.clear();
}

function ensureCandle(timestamp, price) {
  const minute = Math.floor(timestamp / 60000);
  if (state.candle.minute === null || state.candle.minute !== minute) resetCandle(minute, price);
}

function addLevel(price, side, qty) {
  const key = price.toFixed(2);
  let level = state.levels.get(key);
  if (!level) {
    level = { price, buyVolume: 0, sellVolume: 0, delta: 0, trades: 0 };
    state.levels.set(key, level);
  }
  if (side === "BUY") {
    level.buyVolume += qty;
    level.delta += qty;
  } else {
    level.sellVolume += qty;
    level.delta -= qty;
  }
  level.trades += 1;
}

function processTrade(data) {
  const price = Number(data.p);
  const qty = Number(data.q);
  const timestamp = Number(data.T);
  if (!Number.isFinite(price) || !Number.isFinite(qty)) return;

  const time = Number.isFinite(timestamp) ? timestamp : Date.now();
  const side = data.m === true ? "SELL" : "BUY";
  ensureCandle(time, price);

  const c = state.candle;
  c.high = Math.max(c.high, price);
  c.low = Math.min(c.low, price);
  c.close = price;
  c.trades += 1;

  if (side === "BUY") {
    c.buyVolume += qty;
    state.totalBuyVolume += qty;
    state.totalDelta += qty;
  } else {
    c.sellVolume += qty;
    state.totalSellVolume += qty;
    state.totalDelta -= qty;
  }

  c.delta = c.buyVolume - c.sellVolume;
  addLevel(price, side, qty);
  state.ticks += 1;
  state.lastTradeAt = new Date(time).toISOString();
  state.lastPrice = price;
  state.lastQuantity = qty;
  state.lastSide = side;
  state.lastError = null;
  state.recentTrades.push({ time: state.lastTradeAt, price, quantity: qty, side });
  if (state.recentTrades.length > 100) state.recentTrades.shift();
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  state.reconnects += 1;
  const delay = Math.min(30000, 2000 * state.reconnects);
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
}

function connect() {
  try { if (ws) ws.close(); } catch {}
  state.connected = false;
  console.log(`Connecting Binance stream: ${STREAM_URL}`);
  ws = new WebSocket(STREAM_URL);

  ws.on("open", () => {
    state.connected = true;
    state.lastError = null;
    console.log("BINANCE STREAM CONNECTED");
  });

  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg?.e === "aggTrade" && msg.s === SYMBOL) processTrade(msg);
      else if (msg?.code) {
        state.lastError = `Binance ${msg.code}: ${msg.msg || "unknown error"}`;
        console.error(state.lastError);
      }
    } catch (err) {
      state.lastError = `JSON parse error: ${err.message}`;
      console.error(state.lastError);
    }
  });

  ws.on("error", err => {
    state.lastError = `WebSocket error: ${err.message}`;
    console.error(state.lastError);
  });

  ws.on("close", (code, reason) => {
    state.connected = false;
    console.log(`BINANCE STREAM CLOSED code=${code} reason=${reason?.toString() || ""}`);
    scheduleReconnect();
  });
}

function candleOutput() {
  const c = state.candle;
  return {
    open: c.open,
    high: c.high,
    low: c.low,
    close: c.close,
    buyVolume: Number(c.buyVolume.toFixed(8)),
    sellVolume: Number(c.sellVolume.toFixed(8)),
    delta: Number(c.delta.toFixed(8)),
    trades: c.trades
  };
}

function footprintOutput() {
  return Array.from(state.levels.values()).sort((a,b) => b.price-a.price).map(x => ({
    price: x.price,
    buyVolume: Number(x.buyVolume.toFixed(8)),
    sellVolume: Number(x.sellVolume.toFixed(8)),
    delta: Number(x.delta.toFixed(8)),
    trades: x.trades
  }));
}

function signalOutput() {
  const c = state.candle;
  const total = c.buyVolume + c.sellVolume;
  if (!c.trades || total <= 0) return { signal: "WAIT", setup: "NO_DATA", reason: "Waiting for live footprint data" };
  const deltaPercent = (c.delta / total) * 100;
  if (c.close < c.open && deltaPercent >= 20) return { signal: "SELL", setup: "BEARISH_DELTA_DIVERGENCE", deltaPercent: Number(deltaPercent.toFixed(2)), reason: "Red candle with positive delta." };
  if (c.close > c.open && deltaPercent <= -20) return { signal: "BUY", setup: "BULLISH_DELTA_DIVERGENCE", deltaPercent: Number(deltaPercent.toFixed(2)), reason: "Green candle with negative delta." };
  return { signal: "WAIT", setup: "NONE", deltaPercent: Number(deltaPercent.toFixed(2)), reason: "No configured divergence condition." };
}

app.get("/", (req,res) => res.json({ success:true, service:"XAU Footprint Order Flow Engine", instrument:SYMBOL, dataSource:"Binance Futures public aggTrade", stream:STREAM_URL, endpoints:["/health","/status","/footprint","/signal"] }));
app.get("/health", (req,res) => res.json({ success:true, status:"ok", websocketConnected:state.connected, ticks:state.ticks, lastTradeAt:state.lastTradeAt, lastError:state.lastError }));
app.get("/status", (req,res) => res.json({ success:true, instrument:SYMBOL, websocket:{connected:state.connected,reconnects:state.reconnects,endpoint:STREAM_URL}, ticks:state.ticks, lastTradeAt:state.lastTradeAt, lastPrice:state.lastPrice, lastQuantity:state.lastQuantity, lastSide:state.lastSide, lastError:state.lastError, serverTime:new Date().toISOString() }));
app.get("/footprint", (req,res) => {
  const c = candleOutput();
  const total = c.buyVolume + c.sellVolume;
  res.json({ success:true, symbol:SYMBOL, generatedAt:new Date().toISOString(), dataSource:"Binance Futures public aggTrade", dataType:"Live exchange trade stream", websocket:{connected:state.connected,reconnects:state.reconnects}, ticks:state.ticks, lastTradeAt:state.lastTradeAt, lastPrice:state.lastPrice, lastQuantity:state.lastQuantity, lastSide:state.lastSide, lastError:state.lastError, totalBuyVolume:Number(state.totalBuyVolume.toFixed(8)), totalSellVolume:Number(state.totalSellVolume.toFixed(8)), totalDelta:Number(state.totalDelta.toFixed(8)), candle:c, summary:{buyVolume:c.buyVolume,sellVolume:c.sellVolume,delta:c.delta,deltaPercent:total?Number(((c.delta/total)*100).toFixed(2)):0,totalTrades:c.trades}, footprint:footprintOutput(), recentTrades:state.recentTrades.slice(-20) });
});
app.get("/signal", (req,res) => res.json({ success:true, instrument:SYMBOL, generatedAt:new Date().toISOString(), dataReady:state.ticks>0, websocketConnected:state.connected, ticks:state.ticks, signal:signalOutput(), candle:candleOutput(), autoTrade:false }));

app.listen(PORT, () => {
  console.log(`XAU Footprint Order Flow Engine listening on port ${PORT}`);
  console.log(`STREAM=${STREAM_URL}`);
  connect();
});
