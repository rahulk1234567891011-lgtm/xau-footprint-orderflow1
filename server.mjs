import http from "node:http";
import WebSocket from "ws";

const PORT = Number(process.env.PORT || 10000);

const SYMBOL = "XAUUSDT";
const WS_URL = "wss://fstream.binance.com/ws/xauusdt@aggTrade";

const MAX_TRADES = 5000;
const MAX_PRICE_LEVELS = 1000;

const state = {
  startedAt: new Date().toISOString(),

  connected: false,
  reconnects: 0,

  symbol: SYMBOL,

  ticks: 0,
  lastTradeAt: null,
  lastPrice: null,
  lastQuantity: null,
  lastSide: null,

  buyVolume: 0,
  sellVolume: 0,
  delta: 0,

  currentMinute: null,

  candle: {
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

  lastError: null
};

let ws = null;
let reconnectTimer = null;

function nowISO() {
  return new Date().toISOString();
}

function resetCandle(minute) {
  state.currentMinute = minute;

  state.candle = {
    open: null,
    high: null,
    low: null,
    close: null,

    buyVolume: 0,
    sellVolume: 0,
    delta: 0,

    trades: 0
  };
}

function getMinute(timestamp) {
  return Math.floor(timestamp / 60000);
}

function classifyTrade(data) {
  /*
    Binance aggTrade:
      p = price
      q = quantity
      T = trade time
      m = buyer is maker

    If buyer is maker:
      aggressive seller hit the bid
      => SELL

    If buyer is NOT maker:
      aggressive buyer lifted the ask
      => BUY
  */

  return data.m === true ? "SELL" : "BUY";
}

function processTrade(data) {
  const price = Number(data.p);
  const quantity = Number(data.q);
  const timestamp = Number(data.T);

  if (
    !Number.isFinite(price) ||
    !Number.isFinite(quantity) ||
    quantity <= 0 ||
    !Number.isFinite(timestamp)
  ) {
    return;
  }

  const side = classifyTrade(data);

  const minute = getMinute(timestamp);

  if (state.currentMinute === null) {
    resetCandle(minute);
  }

  if (minute !== state.currentMinute) {
    resetCandle(minute);
  }

  state.ticks += 1;

  state.lastTradeAt = new Date(timestamp).toISOString();
  state.lastPrice = price;
  state.lastQuantity = quantity;
  state.lastSide = side;

  if (state.candle.open === null) {
    state.candle.open = price;
  }

  state.candle.high =
    state.candle.high === null
      ? price
      : Math.max(state.candle.high, price);

  state.candle.low =
    state.candle.low === null
      ? price
      : Math.min(state.candle.low, price);

  state.candle.close = price;
  state.candle.trades += 1;

  if (side === "BUY") {
    state.buyVolume += quantity;
    state.candle.buyVolume += quantity;
  } else {
    state.sellVolume += quantity;
    state.candle.sellVolume += quantity;
  }

  state.candle.delta =
    state.candle.buyVolume - state.candle.sellVolume;

  state.delta =
    state.buyVolume - state.sellVolume;

  /*
    Price-level footprint

    BUY = aggressive buyers
    SELL = aggressive sellers
  */

  const key = price.toFixed(2);

  let level = state.priceLevels.get(key);

  if (!level) {
    level = {
      price,
      buyVolume: 0,
      sellVolume: 0,
      delta: 0,
      trades: 0
    };

    state.priceLevels.set(key, level);
  }

  if (side === "BUY") {
    level.buyVolume += quantity;
  } else {
    level.sellVolume += quantity;
  }

  level.delta =
    level.buyVolume - level.sellVolume;

  level.trades += 1;

  /*
    Prevent unlimited memory growth.
  */

  if (state.priceLevels.size > MAX_PRICE_LEVELS) {
    const firstKey = state.priceLevels.keys().next().value;

    if (firstKey !== undefined) {
      state.priceLevels.delete(firstKey);
    }
  }

  state.recentTrades.unshift({
    time: new Date(timestamp).toISOString(),
    price,
    quantity,
    side
  });

  if (state.recentTrades.length > MAX_TRADES) {
    state.recentTrades.length = MAX_TRADES;
  }
}

function getFootprint() {
  const levels = Array.from(state.priceLevels.values())
    .sort((a, b) => b.price - a.price)
    .map(level => ({
      price: level.price,
      buyVolume: Number(level.buyVolume.toFixed(6)),
      sellVolume: Number(level.sellVolume.toFixed(6)),
      delta: Number(level.delta.toFixed(6)),
      trades: level.trades
    }));

  let poc = null;
  let maxVolume = -Infinity;

  for (const level of levels) {
    const total =
      level.buyVolume +
      level.sellVolume;

    if (total > maxVolume) {
      maxVolume = total;
      poc = level.price;
    }
  }

  const totalBuy = state.candle.buyVolume;
  const totalSell = state.candle.sellVolume;

  const total =
    totalBuy + totalSell;

  const delta =
    totalBuy - totalSell;

  const deltaPercent =
    total > 0
      ? (delta / total) * 100
      : 0;

  return {
    symbol: SYMBOL,

    generatedAt: nowISO(),

    candle: {
      ...state.candle,

      buyVolume: Number(
        state.candle.buyVolume.toFixed(6)
      ),

      sellVolume: Number(
        state.candle.sellVolume.toFixed(6)
      ),

      delta: Number(
        state.candle.delta.toFixed(6)
      )
    },

    footprint: levels,

    summary: {
      buyVolume: Number(
        totalBuy.toFixed(6)
      ),

      sellVolume: Number(
        totalSell.toFixed(6)
      ),

      delta: Number(
        delta.toFixed(6)
      ),

      deltaPercent: Number(
        deltaPercent.toFixed(2)
      ),

      poc,

      totalTrades: state.candle.trades
    }
  };
}

function getSignal() {
  const candle = state.candle;

  if (
    candle.open === null ||
    candle.close === null ||
    candle.trades < 5
  ) {
    return {
      signal: "WAIT",
      reason: "Not enough current candle data"
    };
  }

  const delta = candle.delta;

  const total =
    candle.buyVolume +
    candle.sellVolume;

  if (total <= 0) {
    return {
      signal: "WAIT",
      reason: "No volume"
    };
  }

  const deltaPercent =
    (delta / total) * 100;

  /*
    IMPORTANT:

    This is only a diagnostic footprint
    signal for now.

    It is NOT an 80-85% claim and
    NOT an automatic trading signal.
  */

  if (
    candle.close < candle.open &&
    deltaPercent >= 20
  ) {
    return {
      signal: "BEARISH_DELTA_DIVERGENCE",
      bias: "SELL",
      candle: "RED",
      delta: Number(delta.toFixed(6)),
      deltaPercent: Number(deltaPercent.toFixed(2)),
      reason:
        "Bearish candle with strongly positive delta"
    };
  }

  if (
    candle.close > candle.open &&
    deltaPercent <= -20
  ) {
    return {
      signal: "BULLISH_DELTA_DIVERGENCE",
      bias: "BUY",
      candle: "GREEN",
      delta: Number(delta.toFixed(6)),
      deltaPercent: Number(deltaPercent.toFixed(2)),
      reason:
        "Bullish candle with strongly negative delta"
    };
  }

  return {
    signal: "WAIT",
    bias: "NEUTRAL",

    candle:
      candle.close > candle.open
        ? "GREEN"
        : candle.close < candle.open
          ? "RED"
          : "DOJI",

    delta: Number(delta.toFixed(6)),

    deltaPercent:
      Number(deltaPercent.toFixed(2)),

    reason:
      "No confirmed delta-divergence setup"
  };
}

function getStatus() {
  return {
    success: true,

    service:
      "XAU Footprint Order Flow Engine",

    symbol: SYMBOL,

    dataSource:
      "Binance Futures public aggTrade stream",

    dataType:
      "Live exchange trade stream",

    websocket: {
      connected: state.connected,
      reconnects: state.reconnects
    },

    ticks: state.ticks,

    lastTradeAt:
      state.lastTradeAt,

    lastPrice:
      state.lastPrice,

    lastQuantity:
      state.lastQuantity,

    lastSide:
      state.lastSide,

    totalBuyVolume:
      Number(state.buyVolume.toFixed(6)),

    totalSellVolume:
      Number(state.sellVolume.toFixed(6)),

    totalDelta:
      Number(state.delta.toFixed(6)),

    currentCandle:
      state.candle,

    lastError:
      state.lastError
  };
}

function sendJSON(res, data, statusCode = 200) {
  const body = JSON.stringify(
    data,
    null,
    2
  );

  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*"
  });

  res.end(body);
}

function startWebSocket() {
  if (ws) {
    try {
      ws.close();
    } catch {}
  }

  state.connected = false;

  console.log(
    `[${nowISO()}] Connecting: ${WS_URL}`
  );

  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    state.connected = true;
    state.lastError = null;

    console.log(
      `[${nowISO()}] XAUUSDT WebSocket CONNECTED`
    );
  });

  ws.on("message", raw => {
    try {
      const data = JSON.parse(
        raw.toString()
      );

      if (data.e === "aggTrade") {
        processTrade(data);
      }
    } catch (error) {
      state.lastError =
        error?.message ||
        String(error);
    }
  });

  ws.on("error", error => {
    state.lastError =
      error?.message ||
      String(error);

    console.error(
      `[${nowISO()}] WebSocket error:`,
      state.lastError
    );
  });

  ws.on("close", () => {
    state.connected = false;
    state.reconnects += 1;

    console.log(
      `[${nowISO()}] WebSocket disconnected`
    );

    scheduleReconnect();
  });
}

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;

    startWebSocket();
  }, 5000);
}

const server = http.createServer(
  (req, res) => {
    const url = new URL(
      req.url,
      `http://${req.headers.host}`
    );

    if (url.pathname === "/") {
      return sendJSON(res, {
        success: true,

        service:
          "XAU Footprint Order Flow Engine",

        instrument: SYMBOL,

        engine:
          "Live Binance Futures trade-flow footprint",

        endpoints: [
          "/health",
          "/status",
          "/footprint",
          "/signal"
        ],

        websocket:
          "Binance Futures public aggTrade"
      });
    }

    if (url.pathname === "/health") {
      return sendJSON(res, {
        success: true,
        status: "running",
        websocket: state.connected,
        instrument: SYMBOL,
        time: nowISO()
      });
    }

    if (url.pathname === "/status") {
      return sendJSON(
        res,
        getStatus()
      );
    }

    if (url.pathname === "/footprint") {
      return sendJSON(
        res,
        getFootprint()
      );
    }

    if (url.pathname === "/signal") {
      return sendJSON(
        res,
        {
          success: true,
          instrument: SYMBOL,
          generatedAt: nowISO(),
          ...getSignal()
        }
      );
    }

    return sendJSON(
      res,
      {
        success: false,
        error: "Endpoint not found"
      },
      404
    );
  }
);

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `[${nowISO()}] Server listening on port ${PORT}`
    );

    startWebSocket();
  }
);
