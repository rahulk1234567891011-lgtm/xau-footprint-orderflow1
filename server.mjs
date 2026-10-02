import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = process.env.PORT || 3000;
const SYMBOL = "XAUUSDT";
const BINANCE_WS_URL = "wss://fstream.binance.com/market/ws/xauusdt@aggTrade";

let binanceWs = null;
let reconnectTimer = null;
let reconnectCount = 0;

const state = {
  service: "XAU Footprint Order Flow Engine",
  instrument: SYMBOL,
  dataSource: "Binance Futures public aggTrade stream",
  dataType: "Live exchange trade stream",
  websocketConnected: false,
  ticks: 0,
  lastTradeAt: null,
  lastPrice: null,
  lastQuantity: null,
  lastSide: null,
  totalBuyVolume: 0,
  totalSellVolume: 0,
  totalDelta: 0,
  currentCandle: {
    minute: null,
    open: null,
    high: null,
    low: null,
    close: null,
    buyVolume: 0,
    sellVolume: 0,
    delta: 0,
    trades: 0
  },
  priceLevels: new Map(),
  recentTrades: [],
  lastSignal: null,
  lastError: null
};

function resetCandle(minute, price) {
  state.currentCandle = {
    minute,
    open: price,
    high: price,
    low: price,
    close: price,
    buyVolume: 0,
    sellVolume: 0,
    delta: 0,
    trades: 0
  };
  state.priceLevels.clear();
}

function ensureCandle(timestamp, price) {
  const minute = Math.floor(timestamp / 60000);
  if (state.currentCandle.minute === null) {
    resetCandle(minute, price);
    return;
  }
  if (minute !== state.currentCandle.minute) resetCandle(minute, price);
}

function addPriceLevel(price, side, quantity) {
  const key = price.toFixed(2);
  if (!state.priceLevels.has(key)) {
    state.priceLevels.set(key, {
      price,
      buyVolume: 0,
      sellVolume: 0,
      delta: 0,
      trades: 0
    });
  }
  const level = state.priceLevels.get(key);
  if (side === "BUY") {
    level.buyVolume += quantity;
    level.delta += quantity;
  } else {
    level.sellVolume += quantity;
    level.delta -= quantity;
  }
  level.trades += 1;
}

function processTrade(data) {
  const price = Number(data.p);
  const quantity = Number(data.q);
  const timestamp = Number(data.T || Date.now());
  if (!Number.isFinite(price) || !Number.isFinite(quantity)) return;

  // m=true: buyer is maker, so seller was aggressive -> SELL.
  // m=false: buyer is taker, so buyer was aggressive -> BUY.
  const side = data.m === true ? "SELL" : "BUY";
  ensureCandle(timestamp, price);

  const candle = state.currentCandle;
  candle.high = Math.max(candle.high, price);
  candle.low = Math.min(candle.low, price);
  candle.close = price;
  candle.trades += 1;

  if (side === "BUY") {
    candle.buyVolume += quantity;
    state.totalBuyVolume += quantity;
    state.totalDelta += quantity;
  } else {
    candle.sellVolume += quantity;
    state.totalSellVolume += quantity;
    state.totalDelta -= quantity;
  }

  candle.delta = candle.buyVolume - candle.sellVolume;
  addPriceLevel(price, side, quantity);

  state.ticks += 1;
  state.lastTradeAt = new Date(timestamp).toISOString();
  state.lastPrice = price;
  state.lastQuantity = quantity;
  state.lastSide = side;
  state.lastError = null;

  state.recentTrades.push({
    time: state.lastTradeAt,
    price,
    quantity,
    side
  });
  if (state.recentTrades.length > 100) state.recentTrades.shift();
}

function calculateSignal() {
  const c = state.currentCandle;
  if (c.open === null || c.close === null || c.trades < 1) {
    return { signal: "WAIT", reason: "Waiting for live trade data" };
  }

  const totalVolume = c.buyVolume + c.sellVolume;
  if (totalVolume <= 0) return { signal: "WAIT", reason: "No volume available" };

  const deltaPercent = (c.delta / totalVolume) * 100;
  const redCandle = c.close < c.open;
  const greenCandle = c.close > c.open;

  if (redCandle && deltaPercent >= 20) {
    return {
      signal: "SELL",
      setup: "BEARISH_DELTA_DIVERGENCE",
      confidence: "DIAGNOSTIC",
      candle: "RED",
      deltaPercent: Number(deltaPercent.toFixed(2)),
      reason: "Red candle with strongly positive delta: possible buyer absorption."
    };
  }

  if (greenCandle && deltaPercent <= -20) {
    return {
      signal: "BUY",
      setup: "BULLISH_DELTA_DIVERGENCE",
      confidence: "DIAGNOSTIC",
      candle: "GREEN",
      deltaPercent: Number(deltaPercent.toFixed(2)),
      reason: "Green candle with strongly negative delta: possible seller absorption."
    };
  }

  return {
    signal: "WAIT",
    setup: "NONE",
    deltaPercent: Number(deltaPercent.toFixed(2)),
    reason: "No configured delta-divergence setup."
  };
}

function connectBinance() {
  if (binanceWs) {
    try { binanceWs.removeAllListeners(); binanceWs.close(); } catch {}
  }
  state.websocketConnected = false;
  console.log("Connecting to Binance:", BINANCE_WS_URL);
  binanceWs = new WebSocket(BINANCE_WS_URL);

  binanceWs.on("open", () => {
    state.websocketConnected = true;
    state.lastError = null;
    reconnectCount = 0;
    console.log("BINANCE WEBSOCKET CONNECTED");
  });

  binanceWs.on("message", (raw) => {
    try {
      const message = JSON.parse(raw.toString());
      if (message && message.e === "aggTrade" && message.s === SYMBOL) processTrade(message);
    } catch (error) {
      state.lastError = `Message parse error: ${error.message}`;
      console.error(state.lastError);
    }
  });

  binanceWs.on("error", (error) => {
    state.lastError = `WebSocket error: ${error.message}`;
    console.error(state.lastError);
  });

  binanceWs.on("close", (code, reason) => {
    state.websocketConnected = false;
    console.log("BINANCE WEBSOCKET CLOSED:", code, reason ? reason.toString() : "");
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectCount += 1;
  const delay = Math.min(30000, 2000 * reconnectCount);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectBinance();
  }, delay);
}

function getCandleOutput() {
  const c = state.currentCandle;
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

function getFootprintLevels() {
  return Array.from(state.priceLevels.values())
    .sort((a, b) => b.price - a.price)
    .map(level => ({
      price: level.price,
      buyVolume: Number(level.buyVolume.toFixed(8)),
      sellVolume: Number(level.sellVolume.toFixed(8)),
      delta: Number(level.delta.toFixed(8)),
      trades: level.trades
    }));
}

app.get("/", (req, res) => {
  res.json({
    success: true,
    service: state.service,
    instrument: SYMBOL,
    engine: "Live Binance Futures trade-flow footprint",
    dataSource: state.dataSource,
    websocket: "Binance Futures public aggTrade",
    websocketEndpoint: BINANCE_WS_URL,
    endpoints: ["/health", "/status", "/footprint", "/signal"]
  });
});

app.get("/health", (req, res) => {
  res.json({
    success: true,
    status: "ok",
    websocketConnected: state.websocketConnected,
    ticks: state.ticks,
    lastTradeAt: state.lastTradeAt,
    lastError: state.lastError
  });
});

app.get("/status", (req, res) => {
  res.json({
    success: true,
    service: state.service,
    instrument: SYMBOL,
    websocket: {
      connected: state.websocketConnected,
      endpoint: BINANCE_WS_URL,
      reconnects: reconnectCount
    },
    ticks: state.ticks,
    lastTradeAt: state.lastTradeAt,
    lastPrice: state.lastPrice,
    lastQuantity: state.lastQuantity,
    lastSide: state.lastSide,
    lastError: state.lastError,
    time: new Date().toISOString()
  });
});

app.get("/footprint", (req, res) => {
  const candle = getCandleOutput();
  const totalVolume = candle.buyVolume + candle.sellVolume;
  const deltaPercent = totalVolume > 0 ? (candle.delta / totalVolume) * 100 : 0;

  res.json({
    success: true,
    service: state.service,
    symbol: SYMBOL,
    generatedAt: new Date().toISOString(),
    dataSource: state.dataSource,
    dataType: state.dataType,
    websocket: { connected: state.websocketConnected, reconnects: reconnectCount },
    ticks: state.ticks,
    lastTradeAt: state.lastTradeAt,
    lastPrice: state.lastPrice,
    lastQuantity: state.lastQuantity,
    lastSide: state.lastSide,
    totalBuyVolume: Number(state.totalBuyVolume.toFixed(8)),
    totalSellVolume: Number(state.totalSellVolume.toFixed(8)),
    totalDelta: Number(state.totalDelta.toFixed(8)),
    candle,
    footprint: getFootprintLevels(),
    summary: {
      buyVolume: candle.buyVolume,
      sellVolume: candle.sellVolume,
      delta: candle.delta,
      deltaPercent: Number(deltaPercent.toFixed(2)),
      totalTrades: candle.trades
    },
    recentTrades: state.recentTrades.slice(-20),
    lastError: state.lastError
  });
});

app.get("/signal", (req, res) => {
  const signal = calculateSignal();
  state.lastSignal = signal;
  res.json({
    success: true,
    instrument: SYMBOL,
    generatedAt: new Date().toISOString(),
    dataReady: state.ticks > 0,
    websocketConnected: state.websocketConnected,
    ticks: state.ticks,
    signal: signal.signal,
    setup: signal.setup || "NONE",
    candle: getCandleOutput(),
    diagnostic: signal,
    autoTrade: false,
    note: "Diagnostic footprint signal only. No automatic order execution is connected."
  });
});

app.listen(PORT, () => {
  console.log(`XAU Footprint Order Flow Engine listening on port ${PORT}`);
  console.log(`Instrument: ${SYMBOL}`);
  console.log(`Binance stream: ${BINANCE_WS_URL}`);
  connectBinance();
});
