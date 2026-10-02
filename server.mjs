import express from "express";
import WebSocket from "ws";

const app = express();
const PORT = process.env.PORT || 3000;
const SYMBOL = "XAUUSDT";
const BINANCE_WS_URL = "wss://fstream.binance.com/market/ws/xauusdt@aggTrade";

// Execution hand-off configuration.
// The server never places a broker order by itself. It exposes a deterministic
// signal endpoint for Liquid Chart / an execution bridge to consume.
const DRY_RUN = String(process.env.DRY_RUN ?? "true").toLowerCase() === "true";
const EXECUTION_SECRET = process.env.EXECUTION_SECRET || "";
const LOTS = Number(process.env.LOTS || 0.05);
const SL_BUFFER = Number(process.env.SL_BUFFER || 0.20);
const TP_RR = Number(process.env.TP_RR || 1.25);
const MIN_DELTA_PERCENT = Number(process.env.MIN_DELTA_PERCENT || 20);
const ABSORPTION_RATIO = Number(process.env.ABSORPTION_RATIO || 1.20);


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
    return { signal: "WAIT", setup: "NO_DATA", reason: "Waiting for live trade data" };
  }

  const totalVolume = c.buyVolume + c.sellVolume;
  if (totalVolume <= 0) return { signal: "WAIT", setup: "NO_VOLUME", reason: "No live volume available" };

  const deltaPercent = (c.delta / totalVolume) * 100;
  const range = Math.max(c.high - c.low, 0.00001);
  const body = Math.abs(c.close - c.open);
  const upperWick = c.high - Math.max(c.open, c.close);
  const lowerWick = Math.min(c.open, c.close) - c.low;
  const red = c.close < c.open;
  const green = c.close > c.open;

  // Wick requirement: the rejection wick must be material relative to candle range.
  const upperWickPct = (upperWick / range) * 100;
  const lowerWickPct = (lowerWick / range) * 100;
  const upperWickOk = upperWick > body * 0.50 && upperWickPct >= 15;
  const lowerWickOk = lowerWick > body * 0.50 && lowerWickPct >= 15;

  const levels = getFootprintLevels();
  const levelCount = levels.length;
  const topCount = Math.max(1, Math.ceil(levelCount * 0.25));
  const bottomCount = Math.max(1, Math.ceil(levelCount * 0.25));
  const topLevels = levels.slice(0, topCount);
  const bottomLevels = levels.slice(Math.max(0, levelCount - bottomCount));

  const topBuy = topLevels.reduce((n, x) => n + x.buyVolume, 0);
  const topSell = topLevels.reduce((n, x) => n + x.sellVolume, 0);
  const bottomBuy = bottomLevels.reduce((n, x) => n + x.buyVolume, 0);
  const bottomSell = bottomLevels.reduce((n, x) => n + x.sellVolume, 0);

  // Price-level absorption proxy: aggressive flow is concentrated at the rejected extreme.
  const bearishAbsorption = topBuy > 0 && topBuy >= topSell * ABSORPTION_RATIO;
  const bullishAbsorption = bottomSell > 0 && bottomSell >= bottomBuy * ABSORPTION_RATIO;

  const bearishDelta = deltaPercent >= MIN_DELTA_PERCENT;
  const bullishDelta = deltaPercent <= -MIN_DELTA_PERCENT;

  if (red && upperWickOk && bearishDelta && bearishAbsorption) {
    const entry = c.close;
    const stopLoss = c.high + SL_BUFFER;
    const risk = Math.max(stopLoss - entry, 0.01);
    const takeProfit = entry - risk * TP_RR;
    return {
      signal: "SELL",
      setup: "FOOTPRINT_BUYER_ABSORPTION",
      confidence: "CONFIRMED_RULESET",
      candle: "RED",
      deltaPercent: Number(deltaPercent.toFixed(2)),
      upperWickPct: Number(upperWickPct.toFixed(2)),
      absorption: { topBuy: Number(topBuy.toFixed(6)), topSell: Number(topSell.toFixed(6)), ratio: Number((topBuy / Math.max(topSell, 0.000001)).toFixed(2)) },
      levels: { entry, stopLoss: Number(stopLoss.toFixed(2)), takeProfit: Number(takeProfit.toFixed(2)), risk: Number(risk.toFixed(2)), rr: TP_RR },
      reason: "Red candle + material upper wick + positive delta + buy-flow concentration at the upper footprint levels."
    };
  }

  if (green && lowerWickOk && bullishDelta && bullishAbsorption) {
    const entry = c.close;
    const stopLoss = Math.max(c.low - SL_BUFFER, 0.01);
    const risk = Math.max(entry - stopLoss, 0.01);
    const takeProfit = entry + risk * TP_RR;
    return {
      signal: "BUY",
      setup: "FOOTPRINT_SELLER_ABSORPTION",
      confidence: "CONFIRMED_RULESET",
      candle: "GREEN",
      deltaPercent: Number(deltaPercent.toFixed(2)),
      lowerWickPct: Number(lowerWickPct.toFixed(2)),
      absorption: { bottomBuy: Number(bottomBuy.toFixed(6)), bottomSell: Number(bottomSell.toFixed(6)), ratio: Number((bottomSell / Math.max(bottomBuy, 0.000001)).toFixed(2)) },
      levels: { entry, stopLoss: Number(stopLoss.toFixed(2)), takeProfit: Number(takeProfit.toFixed(2)), risk: Number(risk.toFixed(2)), rr: TP_RR },
      reason: "Green candle + material lower wick + negative delta + sell-flow concentration at the lower footprint levels."
    };
  }

  return {
    signal: "WAIT",
    setup: "NONE",
    confidence: "NO_SETUP",
    deltaPercent: Number(deltaPercent.toFixed(2)),
    upperWickPct: Number(upperWickPct.toFixed(2)),
    lowerWickPct: Number(lowerWickPct.toFixed(2)),
    absorption: { bearish: bearishAbsorption, bullish: bullishAbsorption },
    reason: "All required footprint conditions are not aligned."
  };
}

function authorized(req) {
  if (!EXECUTION_SECRET) return true;
  return String(req.headers["x-execution-secret"] || "") === EXECUTION_SECRET;
}

function buildExecutionSignal() {
  const diagnostic = calculateSignal();
  const executable = diagnostic.signal === "BUY" || diagnostic.signal === "SELL";
  const id = `${diagnostic.signal}_${state.currentCandle.minute ?? "NA"}`;
  return {
    success: true,
    instrument: "XAUUSD",
    sourceInstrument: SYMBOL,
    generatedAt: new Date().toISOString(),
    signalId: id,
    signal: diagnostic.signal,
    setup: diagnostic.setup,
    executable,
    dryRun: DRY_RUN,
    autoTrade: false,
    lots: Number.isFinite(LOTS) && LOTS > 0 ? LOTS : 0.05,
    priceSource: "Binance Futures XAUUSDT aggTrade",
    currentPrice: state.lastPrice,
    levels: diagnostic.levels || null,
    diagnostic
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
    endpoints: ["/health", "/status", "/footprint", "/signal", "/execution-signal"]
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
    note: "Diagnostic only. Use /execution-signal for the Liquid Chart execution hand-off."
  });
});

app.get("/execution-signal", (req, res) => {
  if (!authorized(req)) {
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  const result = buildExecutionSignal();
  state.lastSignal = result.diagnostic;
  res.json(result);
});

app.listen(PORT, () => {
  console.log(`XAU Footprint Order Flow Engine listening on port ${PORT}`);
  console.log(`Instrument: ${SYMBOL}`);
  console.log(`Binance stream: ${BINANCE_WS_URL}`);
  connectBinance();
});
