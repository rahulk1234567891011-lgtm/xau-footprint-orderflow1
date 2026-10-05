// ============================================================
// XAUUSD 78.31% STRATEGY — RENDER SIGNAL SERVER
// ============================================================
// ONLY the recovered 78% strategy.
// Instrument : XAUUSD
// Execution  : 5M
// Context    : 1H + 15M
//
// Strategy:
// Trendline -> Breakout -> Strong Breakout -> MTF Confirmation
// -> Retest / Continuation -> BUY/SELL signal
//
// RR  : 1.50
// ATR : 14
// Minimum signal score : 7
//
// NO Ready8
// NO Goldara
// NO Quick Scalping
// NO Strong Engine
// NO additional strategy
// ============================================================

import express from "express";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 10000);
const SYMBOL = "XAUUSD";

const CONFIG = {
  executionTF: "5M",
  contextTF: ["1H", "15M"],
  rr: 1.50,
  atrPeriod: 14,
  minScore: 7
};

let latestSignal = {
  status: "WAITING",
  direction: null,
  score: 0,
  instrument: SYMBOL
};

// ------------------------------------------------------------
// BASIC HELPERS
// ------------------------------------------------------------

function last(a) {
  return a && a.length ? a[a.length - 1] : null;
}

function body(c) {
  return Math.abs(Number(c.close) - Number(c.open));
}

function range(c) {
  return Math.max(
    Number(c.high) - Number(c.low),
    0.00000001
  );
}

// ------------------------------------------------------------
// ATR 14
// ------------------------------------------------------------

function atr(candles, period = 14) {

  if (!candles || candles.length < period + 1)
    return null;

  const tr = [];

  for (let i = 1; i < candles.length; i++) {

    const c = candles[i];
    const p = candles[i - 1];

    tr.push(
      Math.max(
        Number(c.high) - Number(c.low),
        Math.abs(Number(c.high) - Number(p.close)),
        Math.abs(Number(c.low) - Number(p.close))
      )
    );
  }

  const x = tr.slice(-period);

  return x.reduce((a, b) => a + b, 0) / x.length;
}

// ------------------------------------------------------------
// EMA
// ------------------------------------------------------------

function ema(candles, period) {

  if (!candles || candles.length < period)
    return null;

  let e = Number(candles[0].close);
  const k = 2 / (period + 1);

  for (let i = 1; i < candles.length; i++) {

    e =
      Number(candles[i].close) * k +
      e * (1 - k);
  }

  return e;
}

// ------------------------------------------------------------
// RSI
// ------------------------------------------------------------

function rsi(candles, period = 14) {

  if (!candles || candles.length < period + 1)
    return null;

  let gain = 0;
  let loss = 0;

  for (
    let i = candles.length - period;
    i < candles.length;
    i++
  ) {

    const d =
      Number(candles[i].close) -
      Number(candles[i - 1].close);

    if (d >= 0)
      gain += d;
    else
      loss -= d;
  }

  if (loss === 0)
    return 100;

  const rs =
    (gain / period) /
    (loss / period);

  return 100 - 100 / (1 + rs);
}

// ------------------------------------------------------------
// SWINGS
// ------------------------------------------------------------

function isSwingHigh(candles, i, w = 2) {

  if (
    i < w ||
    i >= candles.length - w
  )
    return false;

  for (let j = 1; j <= w; j++) {

    if (
      Number(candles[i].high) <=
        Number(candles[i - j].high) ||
      Number(candles[i].high) <=
        Number(candles[i + j].high)
    )
      return false;
  }

  return true;
}

function isSwingLow(candles, i, w = 2) {

  if (
    i < w ||
    i >= candles.length - w
  )
    return false;

  for (let j = 1; j <= w; j++) {

    if (
      Number(candles[i].low) >=
        Number(candles[i - j].low) ||
      Number(candles[i].low) >=
        Number(candles[i + j].low)
    )
      return false;
  }

  return true;
}

function getSwings(candles) {

  const highs = [];
  const lows = [];

  for (
    let i = 2;
    i < candles.length - 2;
    i++
  ) {

    if (isSwingHigh(candles, i))
      highs.push({
        index: i,
        price: Number(candles[i].high)
      });

    if (isSwingLow(candles, i))
      lows.push({
        index: i,
        price: Number(candles[i].low)
      });
  }

  return { highs, lows };
}

// ------------------------------------------------------------
// TRENDLINE BREAKOUT
// ------------------------------------------------------------

function trendlineBreak(candles, direction) {

  if (!candles || candles.length < 30)
    return {
      confirmed: false,
      strength: "Weak"
    };

  const swings = getSwings(candles);
  const current = last(candles);

  const a = atr(candles, 14);

  if (!current || !a)
    return {
      confirmed: false,
      strength: "Weak"
    };

  const tolerance = a * 0.08;

  // ----------------------------------------------------------
  // BUY
  // Descending swing highs -> upside breakout
  // ----------------------------------------------------------

  if (
    direction === "BUY" &&
    swings.highs.length >= 2
  ) {

    const h1 =
      swings.highs[swings.highs.length - 2];

    const h2 =
      swings.highs[swings.highs.length - 1];

    if (h2.price < h1.price) {

      const level = h2.price;

      if (
        Number(current.close) >
        level + tolerance * 0.15
      ) {

        const br = body(current) / range(current);

        const closeLocation =
          (Number(current.close) -
            Number(current.low)) /
          range(current);

        const strong =
          (
            br >= 0.55 &&
            closeLocation >= 0.65
          ) ||
          (
            range(current) >= a * 1.10 &&
            br >= 0.60
          );

        return {
          confirmed: true,
          strength: strong ? "Strong" : "Valid",
          level
        };
      }
    }
  }

  // ----------------------------------------------------------
  // SELL
  // Rising swing lows -> downside breakout
  // ----------------------------------------------------------

  if (
    direction === "SELL" &&
    swings.lows.length >= 2
  ) {

    const l1 =
      swings.lows[swings.lows.length - 2];

    const l2 =
      swings.lows[swings.lows.length - 1];

    if (l2.price > l1.price) {

      const level = l2.price;

      if (
        Number(current.close) <
        level - tolerance * 0.15
      ) {

        const br = body(current) / range(current);

        const closeLocation =
          (Number(current.high) -
            Number(current.close)) /
          range(current);

        const strong =
          (
            br >= 0.55 &&
            closeLocation >= 0.65
          ) ||
          (
            range(current) >= a * 1.10 &&
            br >= 0.60
          );

        return {
          confirmed: true,
          strength: strong ? "Strong" : "Valid",
          level
        };
      }
    }
  }

  return {
    confirmed: false,
    strength: "Weak"
  };
}

// ------------------------------------------------------------
// MTF STRUCTURE
// ------------------------------------------------------------

function structure(candles, direction) {

  const c = last(candles);

  const e20 = ema(candles, 20);
  const e50 = ema(candles, 50);

  if (!c || e20 == null || e50 == null)
    return false;

  if (direction === "BUY") {

    return (
      e20 > e50 &&
      Number(c.close) > e20
    );
  }

  return (
    e20 < e50 &&
    Number(c.close) < e20
  );
}

// ------------------------------------------------------------
// MTF MOMENTUM
// ------------------------------------------------------------

function momentum(candles, direction) {

  const r = rsi(candles, 14);

  if (r == null)
    return false;

  if (direction === "BUY")
    return r > 50;

  return r < 50;
}

// ------------------------------------------------------------
// RETEST
// ------------------------------------------------------------

function retest(candles, level, direction) {

  if (!level || candles.length < 4) {

    return {
      occurred: false,
      held: false
    };
  }

  const recent =
    candles.slice(-4);

  let touched = false;
  let held = false;

  for (const c of recent) {

    if (direction === "BUY") {

      if (
        Number(c.low) <= level &&
        Number(c.close) > level
      ) {

        touched = true;
        held = true;
      }
    }

    if (direction === "SELL") {

      if (
        Number(c.high) >= level &&
        Number(c.close) < level
      ) {

        touched = true;
        held = true;
      }
    }
  }

  return {
    occurred: touched,
    held
  };
}

// ------------------------------------------------------------
// COMPLETE 78% STRATEGY
// ------------------------------------------------------------

function analyze(
  candles1H,
  candles15M,
  candles5M
) {

  const result = {};

  for (const direction of ["BUY", "SELL"]) {

    const t1 =
      trendlineBreak(
        candles1H,
        direction
      );

    const t15 =
      trendlineBreak(
        candles15M,
        direction
      );

    const t5 =
      trendlineBreak(
        candles5M,
        direction
      );

    const s1 =
      structure(
        candles1H,
        direction
      );

    const s15 =
      structure(
        candles15M,
        direction
      );

    const s5 =
      structure(
        candles5M,
        direction
      );

    const m1 =
      momentum(
        candles1H,
        direction
      );

    const m15 =
      momentum(
        candles15M,
        direction
      );

    const m5 =
      momentum(
        candles5M,
        direction
      );

    let score = 0;

    // 1H trendline
    if (t1.confirmed)
      score += 1;

    if (
      t1.confirmed &&
      t1.strength !== "Weak"
    )
      score += 2;

    if (t1.strength === "Strong")
      score += 1;

    // 15M trendline
    if (
      t15.confirmed &&
      t15.strength !== "Weak"
    )
      score += 2;

    // 5M execution confirmation
    if (s5 && m5)
      score += 2;

    // Retest / continuation
    const rt =
      retest(
        candles1H,
        t1.level,
        direction
      );

    if (
      rt.occurred &&
      rt.held
    )
      score += 1;

    result[direction] = {
      score,
      trendline1H: t1,
      trendline15M: t15,
      trendline5M: t5,
      structure1H: s1,
      structure15M: s15,
      structure5M: s5,
      momentum1H: m1,
      momentum15M: m15,
      momentum5M: m5,
      retest: rt
    };
  }

  let direction = null;

  if (
    result.BUY.score >= CONFIG.minScore &&
    result.BUY.score >
      result.SELL.score
  ) {

    direction = "BUY";
  }

  if (
    result.SELL.score >= CONFIG.minScore &&
    result.SELL.score >
      result.BUY.score
  ) {

    direction = "SELL";
  }

  const c = last(candles5M);
  const a = atr(candles5M, 14);

  if (
    !direction ||
    !c ||
    !a
  ) {

    return {
      status: "WAITING",
      direction: null,
      score: 0,
      instrument: SYMBOL,
      timeframe: "5M"
    };
  }

  const entry =
    Number(c.close);

  const stopLoss =
    direction === "BUY"
      ? entry - a
      : entry + a;

  const takeProfit =
    direction === "BUY"
      ? entry + a * CONFIG.rr
      : entry - a * CONFIG.rr;

  return {
    status: "CONFIRMED",
    direction,
    score: result[direction].score,
    instrument: SYMBOL,
    timeframe: "5M",
    entry,
    stopLoss,
    takeProfit,
    rr: CONFIG.rr,
    details: result[direction]
  };
}

// ------------------------------------------------------------
// API
// ------------------------------------------------------------

app.get("/", (req, res) => {

  res.json({
    bot: "XAUUSD 78.31% Strategy",
    instrument: "XAUUSD",
    execution: "5M",
    context: ["1H", "15M"],
    strategy:
      "Trendline Breakout + MTF Confirmation + Retest/Continuation",
    status: latestSignal.status,
    signal: latestSignal
  });
});

app.post("/analyze", (req, res) => {

  try {

    const result =
      analyze(
        req.body.candles1H || [],
        req.body.candles15M || [],
        req.body.candles5M || []
      );

    latestSignal = result;

    res.json(result);

  } catch (error) {

    res.status(500).json({
      status: "ERROR",
      error: error.message
    });
  }
});

app.get("/signal", (req, res) => {

  res.json(latestSignal);
});

app.listen(PORT, () => {

  console.log(
    `XAUUSD 78.31% strategy running on ${PORT}`
  );
});
