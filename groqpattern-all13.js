"use strict";

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env") });
const { getGroqUniversalRunModel } = require("./schemas/groqUniversalRun");

let duckdb = null;
try { duckdb = require("duckdb"); } catch (_) {}
let mongoose = null;
try { mongoose = require("mongoose"); } catch (_) {}

const DATA_DIR = path.join(__dirname, "data");
const DUCKDB_FILE = path.join(DATA_DIR, "backtesting.duckdb");
const RESULTS_DIR = path.join(__dirname, "groq-all13-results");
const SUMMARY_FILE = path.join(__dirname, "groq-all13-summary.json");

const GROQ_API_KEY = process.env.GROQ_API_KEY || "";
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.3-70b-versatile";
const GROQ_URL = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_ENABLED = ["1", "true", "yes", "on"].includes(String(process.env.GROQ_ENABLED ?? "true").trim().toLowerCase());
const GROQ_TIMEOUT = Number(process.env.GROQ_TIMEOUT_MS || 120_000);
const GROQ_MAX_RETRY = Number(process.env.GROQ_MAX_RETRY || 4);
const MONGO_URI = process.env.MONGO_URI || "";

const SYMBOLS = (process.env.BACKTEST_SYMBOLS || "").trim()
  ? process.env.BACKTEST_SYMBOLS.split(",").map((s) => s.trim()).filter(Boolean)
  : ["R_100", "R_75", "R_50", "R_25", "R_10", "1HZ100V", "1HZ90V", "1HZ75V", "1HZ50V", "1HZ30V", "1HZ25V", "1HZ15V", "1HZ10V"];

const TIMEFRAMES = [1, 2, 3, 5, 10, 15];
const MAX_ROUNDS = 10;

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function dbAll(db, sql) {
  return new Promise((resolve, reject) => db.all(sql, (err, rows) => (err ? reject(err) : resolve(rows || []))));
}

async function resolveCandleTable(db) {
  const pref = String(process.env.OLLAMA_CANDLE_TABLE || "").trim().toLowerCase();
  if (pref === "candles" || pref === "candles_raw") return pref;
  try { if (Number((await dbAll(db, "SELECT COUNT(*) AS c FROM candles_raw"))[0]?.c) > 0) return "candles_raw"; } catch (_) {}
  try { if (Number((await dbAll(db, "SELECT COUNT(*) AS c FROM candles"))[0]?.c) > 0) return "candles"; } catch (_) {}
  return "candles_raw";
}

async function loadRawCandles(db, symbol, tableName) {
  const safe = String(symbol).replace(/'/g, "''");
  const rows = await dbAll(db, `SELECT time, open, high, low, close FROM ${tableName} WHERE symbol='${safe}' ORDER BY time ASC`);
  return rows.map((r) => ({
    time: new Date(r.time),
    open: Number(r.open),
    high: Number(r.high),
    low: Number(r.low),
    close: Number(r.close),
  }));
}

function resample(candles, minutes) {
  if (minutes <= 1) return candles;
  const out = [];
  for (let i = 0; i < candles.length; i += minutes) {
    const g = candles.slice(i, i + minutes);
    if (!g.length) break;
    out.push({
      time: g[0].time,
      open: g[0].open,
      high: Math.max(...g.map((c) => c.high)),
      low: Math.min(...g.map((c) => c.low)),
      close: g[g.length - 1].close,
    });
  }
  return out;
}

function simulateReversal(colors, minStreak) {
  let wins = 0, losses = 0, streak = 0, maxLoss = 0, run = 1;
  for (let i = 1; i < colors.length - 1; i++) {
    run = colors[i] === colors[i - 1] ? run + 1 : 1;
    if (run >= minStreak) {
      const betGreen = colors[i] === "R";
      const nextGreen = colors[i + 1] === "G";
      const win = betGreen === nextGreen;
      if (win) { wins++; streak = 0; } else { losses++; streak++; maxLoss = Math.max(maxLoss, streak); }
    }
  }
  const total = wins + losses;
  return { winRate: total > 0 ? (wins / total * 100).toFixed(1) : "0", maxLossStreak: maxLoss, trades: total };
}

function computeTimeframeStats(candles, tf) {
  const n = candles.length;
  if (n < 10) return null;
  let green = 0, red = 0;
  const colors = candles.map((c) => (c.close >= c.open ? "G" : "R"));
  colors.forEach((c) => (c === "G" ? green++ : red++));

  let GG = 0, GR = 0, RG = 0, RR = 0;
  for (let i = 1; i < n; i++) {
    const p = colors[i - 1], c = colors[i];
    if (p === "G" && c === "G") GG++;
    else if (p === "G" && c === "R") GR++;
    else if (p === "R" && c === "G") RG++;
    else RR++;
  }
  const t = n - 1;
  return {
    tf,
    bars: n,
    greenPct: (green / n * 100).toFixed(2),
    redPct: (red / n * 100).toFixed(2),
    transitions: { GG: (GG / t * 100).toFixed(2), GR: (GR / t * 100).toFixed(2), RG: (RG / t * 100).toFixed(2), RR: (RR / t * 100).toFixed(2) },
    strategies: {
      reversal_after_2: simulateReversal(colors, 2),
      reversal_after_3: simulateReversal(colors, 3),
      reversal_after_4: simulateReversal(colors, 4),
    },
  };
}

function computeCrossStats(rawCandles, higherTF) {
  const higherBars = resample(rawCandles, higherTF);
  const higherColors = new Map();
  for (const bar of higherBars) higherColors.set(bar.time.getTime(), bar.close >= bar.open ? "G" : "R");
  function getHigherColor(time) {
    const t = time.getTime();
    const barStart = Math.floor(t / (higherTF * 60_000)) * (higherTF * 60_000);
    return higherColors.get(barStart) || null;
  }
  let rrThenG = 0, rrThenR = 0, ggThenR = 0, ggThenG = 0;
  for (let i = 2; i < rawCandles.length - 1; i++) {
    const hColor = getHigherColor(rawCandles[i].time);
    if (!hColor) continue;
    const c0 = rawCandles[i - 2].close >= rawCandles[i - 2].open ? "G" : "R";
    const c1 = rawCandles[i - 1].close >= rawCandles[i - 1].open ? "G" : "R";
    const c2 = rawCandles[i].close >= rawCandles[i].open ? "G" : "R";
    if (hColor === "G" && c0 === "R" && c1 === "R") c2 === "G" ? rrThenG++ : rrThenR++;
    if (hColor === "G" && c0 === "G" && c1 === "G") c2 === "R" ? ggThenR++ : ggThenG++;
  }
  const rrTotal = rrThenG + rrThenR;
  const ggTotal = ggThenG + ggThenR;
  return {
    higherTF,
    rr: { green: rrThenG, red: rrThenR, winRate: rrTotal > 0 ? (rrThenG / rrTotal * 100).toFixed(1) : "0", samples: rrTotal },
    gg: { green: ggThenG, red: ggThenR, reversalRate: ggTotal > 0 ? (ggThenR / ggTotal * 100).toFixed(1) : "0", samples: ggTotal },
  };
}

function normalizeDirection(text) {
  const v = String(text || "").toUpperCase();
  const hasBuy = /\bBUY\b/.test(v);
  const hasSell = /\bSELL\b/.test(v);
  if (hasBuy && hasSell) return "";
  if (hasBuy) return "BUY";
  if (hasSell) return "SELL";
  return "";
}

function parsePatterns(response) {
  const patterns = [];
  if (!response) return patterns;
  const normalized = response
    .replace(/\*\*PATTERN\s+(\d+)[:\*]*/gi, "### PATTERN $1:")
    .replace(/^#{1,4}\s*PATTERN\s+(\d+)[:\s]*/gim, "### PATTERN $1:");
  const blocks = normalized.split(/### PATTERN \d+:/i);
  for (let i = 1; i < blocks.length; i++) {
    const lines = blocks[i].split("\n").map((l) => l.replace(/\*\*/g, "").replace(/^\s*[-*]\s*/, "").trim());
    const p = { name: "", entry_condition: "", exit_condition: "", direction: "", timeframe_context: "", higher_tf_context: "", why_reliable: "" };
    for (const line of lines) if (line && !p.name) { p.name = line; break; }
    for (const line of lines) {
      const low = line.toLowerCase();
      if (low.startsWith("entry condition:")) p.entry_condition = line.split(":").slice(1).join(":").trim();
      if (low.startsWith("exit condition:")) p.exit_condition = line.split(":").slice(1).join(":").trim();
      if (low.startsWith("direction:")) p.direction = normalizeDirection(line.split(":").slice(1).join(":").trim());
      if (low.startsWith("timeframe context:")) p.timeframe_context = line.split(":").slice(1).join(":").trim();
      if (low.startsWith("higher tf context:")) p.higher_tf_context = line.split(":").slice(1).join(":").trim().toUpperCase();
      if (low.startsWith("why reliable:")) p.why_reliable = line.split(":").slice(1).join(":").trim();
    }
    if (!p.direction) p.direction = normalizeDirection(p.entry_condition);
    if (!p.higher_tf_context) p.higher_tf_context = "PREVIOUS_CLOSED";
    if (!p.name) p.name = `Pattern_${i}`;
    if (p.entry_condition && (p.direction === "BUY" || p.direction === "SELL")) patterns.push(p);
  }
  return patterns;
}

function patternKey(p) {
  return `${String(p.direction || "").toUpperCase()}|${String(p.higher_tf_context || "").toUpperCase()}|${String(p.entry_condition || "").toUpperCase().replace(/\s+/g, " ").trim()}`;
}

function createRoundState() {
  const dist = {};
  for (let r = 1; r <= MAX_ROUNDS; r++) dist[`R${r}`] = { wins: 0, losses: 0 };
  return { round: 1, wins: 0, losses: 0, final_losses: 0, round_distribution: dist };
}

function classifyCandleType(c) {
  const high = Number(c?.high);
  const low = Number(c?.low);
  const open = Number(c?.open);
  const close = Number(c?.close);
  const range = high - low;
  if (!Number.isFinite(range) || range <= 0) return "unknown";
  const body = Math.abs(close - open);
  const bodyRatio = body / range;
  const upperWick = high - Math.max(open, close);
  const lowerWick = Math.min(open, close) - low;
  if (bodyRatio <= 0.1) return "doji";
  if (bodyRatio <= 0.35 && upperWick >= body * 1.2 && upperWick > lowerWick) return "spinning_top";
  if (bodyRatio <= 0.35 && lowerWick >= body * 1.2 && lowerWick > upperWick) return "spinning_bottom";
  return "normal";
}

function checkColorEntry(rawCandles, idx, pattern, higherBars) {
  if (idx < 5) return false;
  const cond = pattern.entry_condition.toUpperCase();
  const dir = pattern.direction;
  const seqMatch = cond.match(/LAST\s+(\d+)\s+(?:RAW\s*)?1M\s+CANDLES?\s+(?:ARE|IS)\s+(GREEN|RED)/);
  if (!seqMatch) {
    const c0 = rawCandles[idx - 1].close >= rawCandles[idx - 1].open ? "G" : "R";
    const c1 = rawCandles[idx - 2].close >= rawCandles[idx - 2].open ? "G" : "R";
    if (c0 === c1) return (c0 === "G" && dir === "SELL") || (c0 === "R" && dir === "BUY");
    return false;
  }
  const N = Number(seqMatch[1]);
  const reqColor = seqMatch[2] === "GREEN" ? "G" : "R";
  for (let k = 1; k <= N; k++) {
    const c = rawCandles[idx - k];
    if (!c) return false;
    const color = c.close >= c.open ? "G" : "R";
    if (color !== reqColor) return false;
  }
  const tfMatch = cond.match(/(\d+)M\s+IS\s+(GREEN|RED)/);
  if (tfMatch && higherBars) {
    const tfMin = Number(tfMatch[1]);
    const reqHColor = tfMatch[2] === "GREEN" ? "G" : "R";
    const bars = higherBars[tfMin];
    if (bars) {
      const t = rawCandles[idx].time.getTime();
      const barMs = tfMin * 60_000;
      const barStart = Math.floor(t / barMs) * barMs;
      const currentBar = bars.find((b) => b.time.getTime() === barStart);
      const prevBar = bars.find((b) => b.time.getTime() === (barStart - barMs));
      const mode = String(pattern.higher_tf_context || "PREVIOUS_CLOSED").toUpperCase();
      if (mode === "CURRENT_FORMING") {
        if (currentBar) {
          const hColor = currentBar.close >= currentBar.open ? "G" : "R";
          if (hColor !== reqHColor) return false;
        } else return false;
      } else if (mode === "BOTH") {
        if (!currentBar || !prevBar) return false;
        const c1 = currentBar.close >= currentBar.open ? "G" : "R";
        const c2 = prevBar.close >= prevBar.open ? "G" : "R";
        if (c1 !== reqHColor || c2 !== reqHColor) return false;
      } else {
        if (prevBar) {
          const hColor = prevBar.close >= prevBar.open ? "G" : "R";
          if (hColor !== reqHColor) return false;
        } else return false;
      }
    }
  }
  return true;
}

function isPatternReasonable(pattern, side) {
  if (!pattern || !pattern.entry_condition) return { ok: false, reason: "missing_entry_condition" };
  if (pattern.direction !== side) return { ok: false, reason: "wrong_side_direction" };
  const mode = String(pattern.higher_tf_context || "").toUpperCase();
  if (!["PREVIOUS_CLOSED", "CURRENT_FORMING", "BOTH"].includes(mode)) return { ok: false, reason: "invalid_higher_tf_context" };
  const m = String(pattern.entry_condition).toUpperCase().match(
    /IF\s+(\d+)M\s+IS\s+(GREEN|RED)\s+AND\s+LAST\s+(\d+)\s+RAW\s+1M\s+CANDLES\s+ARE\s+(GREEN|RED)\s+THEN\s+(BUY|SELL)/
  );
  if (!m) return { ok: false, reason: "entry_not_strict_format" };
  const tfMin = Number(m[1]);
  const tfColor = m[2];
  const n = Number(m[3]);
  const seqColor = m[4];
  const thenDir = m[5];
  if (thenDir !== side) return { ok: false, reason: "then_direction_mismatch" };
  if (!Number.isFinite(tfMin) || tfMin < 2 || tfMin > 15) return { ok: false, reason: "unsupported_tf" };
  if (!Number.isFinite(n) || n < 1 || n > 5) return { ok: false, reason: "invalid_sequence_length" };
  // Conservative logic check for currently forming TF bar.
  if (mode === "CURRENT_FORMING" && n >= tfMin && tfColor !== seqColor) {
    return { ok: false, reason: "likely_logical_conflict_current_forming" };
  }
  return { ok: true };
}

function testPattern(rawCandles, pattern, higherBars) {
  const rs = createRoundState();
  let trades = 0;
  const indecision = {
    total_losses: 0,
    buy_side_losses: { doji: 0, spinning_bottom: 0 },
    sell_side_losses: { doji: 0, spinning_top: 0 },
  };
  for (let i = 5; i < rawCandles.length - 1; i++) {
    if (!checkColorEntry(rawCandles, i, pattern, higherBars)) continue;
    const next = rawCandles[i + 1];
    const candleType = classifyCandleType(next);
    const nextG = next.close >= next.open;
    const forcedLossByIndecision =
      (pattern.direction === "BUY" && (candleType === "doji" || candleType === "spinning_bottom")) ||
      (pattern.direction === "SELL" && (candleType === "doji" || candleType === "spinning_top"));
    const win = !forcedLossByIndecision && (
      (pattern.direction === "BUY" && nextG) || (pattern.direction === "SELL" && !nextG)
    );
    const key = `R${rs.round}`;
    if (win) { rs.wins++; rs.round_distribution[key].wins++; rs.round = 1; }
    else {
      rs.losses++; rs.round_distribution[key].losses++;
      if (rs.round >= MAX_ROUNDS) { rs.final_losses++; rs.round = 1; } else rs.round++;
      if (forcedLossByIndecision) {
        indecision.total_losses++;
        if (pattern.direction === "BUY") {
          if (candleType === "doji") indecision.buy_side_losses.doji++;
          if (candleType === "spinning_bottom") indecision.buy_side_losses.spinning_bottom++;
        } else if (pattern.direction === "SELL") {
          if (candleType === "doji") indecision.sell_side_losses.doji++;
          if (candleType === "spinning_top") indecision.sell_side_losses.spinning_top++;
        }
      }
    }
    trades++;
  }
  return {
    total_trades: trades,
    wins: rs.wins,
    losses: rs.losses,
    final_losses: rs.final_losses,
    never_hit_10_losses: rs.final_losses === 0,
    round_distribution: rs.round_distribution,
    indecision_losses: indecision,
  };
}

function buildDualSidePatternPrompt(symbolBundles, successfulPatterns, failedPatterns) {
  const lines = [];
  lines.push("You are a trading analyst. I am providing 6-month statistics for 13 symbols.");
  lines.push("Generate exactly TWO new candidate universal patterns in one response:");
  lines.push(" - one BUY pattern");
  lines.push(" - one SELL pattern");
  lines.push("Use only color-based OHLC rules with explicit BUY or SELL.");
  lines.push("The pattern must be robust across all symbols, not just one.");
  lines.push("");
  lines.push(`SYMBOL_COUNT: ${symbolBundles.length}`);
  lines.push("");
  for (const b of symbolBundles) {
    lines.push(`## SYMBOL ${b.symbol} (candles=${b.rawCount})`);
    for (const tf of [2, 3, 5]) {
      const s = b.tfStats.find((x) => x && x.tf === tf);
      if (!s) continue;
      lines.push(`TF ${tf}m: G=${s.greenPct}% R=${s.redPct}% | rev2(win=${s.strategies.reversal_after_2.winRate}% maxLoss=${s.strategies.reversal_after_2.maxLossStreak}) rev3(win=${s.strategies.reversal_after_3.winRate}% maxLoss=${s.strategies.reversal_after_3.maxLossStreak}) rev4(win=${s.strategies.reversal_after_4.winRate}% maxLoss=${s.strategies.reversal_after_4.maxLossStreak})`);
    }
    for (const cs of b.crossStats.filter((x) => [2, 3, 5].includes(x.higherTF))) {
      lines.push(`Cross ${cs.higherTF}m: (G + RR -> nextG=${cs.rr.green}, nextR=${cs.rr.red}, buyWin=${cs.rr.winRate}%, n=${cs.rr.samples}) | (G + GG -> nextG=${cs.gg.green}, nextR=${cs.gg.red}, sellRev=${cs.gg.reversalRate}%, n=${cs.gg.samples})`);
    }
    lines.push("");
  }

  if (successfulPatterns.length) {
    lines.push("ALREADY_SUCCESSFUL_PATTERNS (DO NOT REPEAT):");
    for (const s of successfulPatterns) {
      lines.push(`- ${s.pattern.direction} | ${s.pattern.entry_condition}`);
    }
    lines.push("");
  }

  if (failedPatterns.length) {
    lines.push("FAILED_PATTERNS (DO NOT REPEAT):");
    for (const f of failedPatterns.slice(-20)) {
      lines.push(`- ${f.pattern.direction} | ${f.pattern.entry_condition} | failed_at=${f.failed_symbol} | tried=${f.tried_symbols.length}`);
    }
    lines.push("");
  }

  lines.push("TASK:");
  lines.push("Return exactly TWO NEW candidate patterns that are different from all successful/failed patterns above.");
  lines.push("Pattern 1 direction must be BUY.");
  lines.push("Pattern 2 direction must be SELL.");
  lines.push("Prefer simple conditions likely to generalize across all 13 symbols.");
  lines.push("");
  lines.push("STRICT OUTPUT:");
  lines.push("### PATTERN 1: [Short Name]");
  lines.push("Entry Condition: IF [2m/3m/5m/10m/15m] is [GREEN/RED] AND last [N] raw 1m candles are [GREEN/RED] THEN BUY");
  lines.push("Exit Condition: WHEN next 1m candle closes");
  lines.push("Direction: BUY");
  lines.push("Higher TF Context: PREVIOUS_CLOSED or CURRENT_FORMING or BOTH");
  lines.push("Timeframe Context: [which higher TF]");
  lines.push("Why Reliable: [short reason from stats]");
  lines.push("");
  lines.push("### PATTERN 2: [Short Name]");
  lines.push("Entry Condition: IF [2m/3m/5m/10m/15m] is [GREEN/RED] AND last [N] raw 1m candles are [GREEN/RED] THEN SELL");
  lines.push("Exit Condition: WHEN next 1m candle closes");
  lines.push("Direction: SELL");
  lines.push("Higher TF Context: PREVIOUS_CLOSED or CURRENT_FORMING or BOTH");
  lines.push("Timeframe Context: [which higher TF]");
  lines.push("Why Reliable: [short reason from stats]");
  lines.push("Do NOT output ambiguous or contradictory logic.");
  return lines.join("\n");
}

async function callGROQ(prompt, label) {
  if (!GROQ_ENABLED) return null;
  if (!GROQ_API_KEY) throw new Error("GROQ_API_KEY not set in .env");
  console.log(`  [GROQ] Sending unified request (${label})...`);
  console.log(`  [GROQ] Prompt size: ${prompt.length.toLocaleString()} chars (~${Math.round(prompt.length / 4)} tokens)`);
  for (let attempt = 1; attempt <= GROQ_MAX_RETRY; attempt++) {
    const controller = new AbortController();
    const startedAt = Date.now();
    const timer = setTimeout(() => controller.abort(), GROQ_TIMEOUT);
    const heartbeat = setInterval(() => {
      const sec = Math.floor((Date.now() - startedAt) / 1000);
      console.log(`  [GROQ] ${label} in progress... ${sec}s elapsed (attempt ${attempt}/${GROQ_MAX_RETRY})`);
    }, 20000);
    try {
      const res = await fetch(GROQ_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${GROQ_API_KEY}` },
        signal: controller.signal,
        body: JSON.stringify({ model: GROQ_MODEL, messages: [{ role: "user", content: prompt }], temperature: 0.2, max_tokens: 1400 }),
      });
      clearTimeout(timer);
      clearInterval(heartbeat);
      if (res.status === 429) {
        const wait = Number(res.headers.get("retry-after") || 60) + 10;
        console.log(`  [GROQ] Rate limited. Waiting ${wait}s...`);
        await sleep(wait * 1000);
        continue;
      }
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`GROQ HTTP ${res.status}: ${txt.slice(0, 200)}`);
      }
      const data = await res.json();
      const text = data?.choices?.[0]?.message?.content || "";
      const pTok = data?.usage?.prompt_tokens ?? "?";
      const oTok = data?.usage?.completion_tokens ?? "?";
      console.log(`  [GROQ] Done | prompt tokens: ${pTok} | output tokens: ${oTok}`);
      return { text, promptTokens: pTok, outputTokens: oTok };
    } catch (err) {
      clearTimeout(timer);
      clearInterval(heartbeat);
      console.error(`  [GROQ] Attempt ${attempt}/${GROQ_MAX_RETRY} failed: ${err.message}`);
      if (attempt < GROQ_MAX_RETRY) await sleep(attempt * 10_000);
      else throw err;
    }
  }
  return null;
}

async function run() {
  if (!duckdb) throw new Error("duckdb not installed - run: npm i duckdb");
  if (!fs.existsSync(DUCKDB_FILE)) throw new Error(`DuckDB not found: ${DUCKDB_FILE}`);
  if (!fs.existsSync(RESULTS_DIR)) fs.mkdirSync(RESULTS_DIR, { recursive: true });

  const db = new duckdb.Database(DUCKDB_FILE);
  const candleTable = await resolveCandleTable(db);
  console.log(`Analyzing all symbols in one pass. Candle table: ${candleTable}`);

  const symbolBundles = [];
  for (const symbol of SYMBOLS) {
    const raw = await loadRawCandles(db, symbol, candleTable);
    if (raw.length < 200) continue;
    const resampled = {};
    for (const tf of TIMEFRAMES) resampled[tf] = resample(raw, tf);
    const higherBarsMap = {};
    for (const tf of TIMEFRAMES.filter((tf) => tf > 1)) higherBarsMap[tf] = resampled[tf];
    const tfStats = TIMEFRAMES.map((tf) => computeTimeframeStats(resampled[tf], tf)).filter(Boolean);
    const crossStats = TIMEFRAMES.filter((tf) => tf > 1).map((tf) => computeCrossStats(raw, tf));
    symbolBundles.push({ symbol, raw, rawCount: raw.length, resampled, higherBarsMap, tfStats, crossStats });
    console.log(`  ${symbol}: candles=${raw.length} stats_ready`);
  }

  const MAX_CYCLES = Math.max(1, Number(process.env.GROQ_UNIVERSAL_MAX_ROUNDS || 30));
  const TARGET_SUCCESS_TOTAL = Math.max(1, Number(process.env.GROQ_UNIVERSAL_TARGET_SUCCESS || 3));
  const MIN_ACTIVE_PER_SIDE = Math.max(1, Number(process.env.GROQ_UNIVERSAL_MIN_ACTIVE_PER_SIDE || 1));

  const successfulPatterns = []; // passed all symbols
  const failedPatterns = []; // dropped at first failure
  const activePatterns = []; // still being validated across symbols
  const testedKeys = new Set(); // avoid duplicates
  const attempts = [];
  let tokenUsage = { prompt: 0, output: 0 };
  let generationId = 0;

  function createRowForSymbol(stats, symbol) {
    return {
      symbol,
      total_trades: stats.total_trades,
      wins: stats.wins,
      losses: stats.losses,
      final_losses: stats.final_losses,
      never_hit_10_losses: stats.never_hit_10_losses,
      indecision_losses: stats.indecision_losses,
    };
  }

  function passStats(stats) {
    return stats.total_trades > 0 && stats.never_hit_10_losses;
  }

  async function generateCandidatePair(processedSymbolCount) {
    generationId++;
    const label = `gen-pair-${generationId}`;
    const prompt = buildDualSidePatternPrompt(symbolBundles, successfulPatterns, failedPatterns);
    fs.writeFileSync(path.join(RESULTS_DIR, `${label}-prompt.txt`), prompt);
    const groqResult = await callGROQ(prompt, label);
    if (!groqResult?.text) {
      attempts.push({ label, status: "no_response" });
      return false;
    }
    tokenUsage.prompt += Number(groqResult.promptTokens) || 0;
    tokenUsage.output += Number(groqResult.outputTokens) || 0;
    fs.writeFileSync(path.join(RESULTS_DIR, `${label}-response.txt`), groqResult.text);

    const parsed = parsePatterns(groqResult.text);
    let activatedAny = false;

    for (const side of ["BUY", "SELL"]) {
      const candidate = parsed.find((p) => {
        if (p.direction !== side) return false;
        const k = patternKey(p);
        if (testedKeys.has(k)) return false;
        const chk = isPatternReasonable(p, side);
        return chk.ok;
      });
      if (!candidate) {
        attempts.push({ label, side, status: "no_new_candidate_for_side" });
        console.log(`  No new ${side} candidate found in pair.`);
        continue;
      }

      const key = patternKey(candidate);
      testedKeys.add(key);

      const perSymbol = [];
      let failedAt = null;
      for (let i = 0; i < processedSymbolCount; i++) {
        const b = symbolBundles[i];
        const stats = testPattern(b.raw, candidate, b.higherBarsMap);
        const row = createRowForSymbol(stats, b.symbol);
        perSymbol.push(row);
        if (!passStats(stats)) {
          failedAt = b.symbol;
          break;
        }
      }

      if (failedAt) {
        failedPatterns.push({
          pattern: candidate,
          pattern_key: key,
          side,
          failed_symbol: failedAt,
          tried_symbols: perSymbol.map((x) => x.symbol),
          per_symbol: perSymbol,
          dropped_reason: "failed_on_backfill",
        });
        attempts.push({ label, side, status: "failed_backfill", failed_symbol: failedAt, pattern_key: key });
        console.log(`  ${side} candidate dropped during backfill at ${failedAt}.`);
        continue;
      }

      activePatterns.push({
        id: `${label}-${side}`,
        side,
        pattern: candidate,
        pattern_key: key,
        tried_symbols: perSymbol.map((x) => x.symbol),
        per_symbol: perSymbol,
      });
      attempts.push({ label, side, status: "activated", pattern_key: key, backfilled: processedSymbolCount });
      console.log(`  Activated ${side} candidate: ${candidate.entry_condition}`);
      activatedAny = true;
    }

    return activatedAny;
  }

  async function ensureActiveForSide(side, processedSymbolCount) {
    let guard = 0;
    while (activePatterns.filter((p) => p.side === side).length < MIN_ACTIVE_PER_SIDE && guard < 5) {
      guard++;
      const ok = await generateCandidatePair(processedSymbolCount);
      if (!ok) break;
    }
  }

  for (let cycle = 1; cycle <= MAX_CYCLES; cycle++) {
    if (successfulPatterns.length >= TARGET_SUCCESS_TOTAL) break;
    console.log(`\n[CYCLE ${cycle}/${MAX_CYCLES}] success=${successfulPatterns.length}/${TARGET_SUCCESS_TOTAL} active=${activePatterns.length}`);

    // Ensure at least one active pattern per side before testing this symbol.
    const processedSymbolCount = 0;
    await ensureActiveForSide("BUY", processedSymbolCount);
    await ensureActiveForSide("SELL", processedSymbolCount);

    for (let symbolIdx = 0; symbolIdx < symbolBundles.length; symbolIdx++) {
      const b = symbolBundles[symbolIdx];
      console.log(`\n  [SYMBOL ${symbolIdx + 1}/${symbolBundles.length}] ${b.symbol}`);

      // Ensure side pools for this point in the walk.
      await ensureActiveForSide("BUY", symbolIdx);
      await ensureActiveForSide("SELL", symbolIdx);

      const activeNow = [...activePatterns];
      if (!activeNow.length) {
        console.log("    No active patterns available; moving to next symbol.");
        continue;
      }

      for (const ap of activeNow) {
        // Pattern might already be removed by earlier failure in this symbol.
        const stillActive = activePatterns.find((x) => x.id === ap.id);
        if (!stillActive) continue;
        // Only test if this symbol has not been tested yet for this pattern.
        if (stillActive.tried_symbols.includes(b.symbol)) continue;

        const stats = testPattern(b.raw, stillActive.pattern, b.higherBarsMap);
        const row = createRowForSymbol(stats, b.symbol);
        stillActive.tried_symbols.push(b.symbol);
        stillActive.per_symbol.push(row);

        const pass = passStats(stats);
        console.log(`    [${stillActive.side}] ${stillActive.pattern.name || "Pattern"} -> ${pass ? "PASS" : "FAIL"} | trades=${stats.total_trades} final_losses=${stats.final_losses}`);

        if (!pass) {
          failedPatterns.push({
            pattern: stillActive.pattern,
            pattern_key: stillActive.pattern_key,
            side: stillActive.side,
            failed_symbol: b.symbol,
            tried_symbols: [...stillActive.tried_symbols],
            per_symbol: [...stillActive.per_symbol],
            dropped_reason: "failed_on_symbol_walk",
          });
          attempts.push({ cycle, status: "failed", side: stillActive.side, failed_symbol: b.symbol, pattern_key: stillActive.pattern_key });
          const idx = activePatterns.findIndex((x) => x.id === stillActive.id);
          if (idx >= 0) activePatterns.splice(idx, 1);
          continue;
        }

        if (stillActive.tried_symbols.length >= symbolBundles.length) {
          successfulPatterns.push({
            pattern: stillActive.pattern,
            pattern_key: stillActive.pattern_key,
            side: stillActive.side,
            per_symbol: [...stillActive.per_symbol],
          });
          attempts.push({ cycle, status: "success", side: stillActive.side, pattern_key: stillActive.pattern_key });
          console.log(`    Pattern completed all ${symbolBundles.length} symbols.`);
          const idx = activePatterns.findIndex((x) => x.id === stillActive.id);
          if (idx >= 0) activePatterns.splice(idx, 1);
        }
      }
    }

    fs.writeFileSync(path.join(RESULTS_DIR, "search-state.json"), JSON.stringify({
      timestamp: new Date().toISOString(),
      cycles_run: cycle,
      max_cycles: MAX_CYCLES,
      target_success_total: TARGET_SUCCESS_TOTAL,
      min_active_per_side: MIN_ACTIVE_PER_SIDE,
      successful_count: successfulPatterns.length,
      failed_count: failedPatterns.length,
      active_count: activePatterns.length,
      successful_patterns: successfulPatterns,
      failed_patterns: failedPatterns,
      active_patterns: activePatterns.map((p) => ({
        id: p.id, side: p.side, pattern_key: p.pattern_key, tried_symbols: p.tried_symbols,
      })),
      attempts,
      token_usage: tokenUsage,
    }, null, 2));

    if (!activePatterns.length && successfulPatterns.length >= TARGET_SUCCESS_TOTAL) break;
  }

  const finalPerSymbol = [];
  for (const b of symbolBundles) {
    const rows = [];
    for (const sp of successfulPatterns) {
      const hit = sp.per_symbol.find((x) => x.symbol === b.symbol);
      if (!hit) continue;
      rows.push({
        symbol: b.symbol,
        pattern_name: sp.pattern.name,
        entry_condition: sp.pattern.entry_condition,
        exit_condition: sp.pattern.exit_condition,
        direction: sp.pattern.direction,
        timeframe_context: sp.pattern.timeframe_context,
        description: sp.pattern.why_reliable,
        ...hit,
      });
    }
    finalPerSymbol.push(...rows);
    fs.writeFileSync(path.join(RESULTS_DIR, `${b.symbol}-results.json`), JSON.stringify({
      symbol: b.symbol,
      timestamp: new Date().toISOString(),
      universal_patterns_survived: successfulPatterns.length,
      results: rows,
    }, null, 2));
  }

  const summary = {
    timestamp: new Date().toISOString(),
    workflow: "GROQ_UNIVERSAL_ALL13_SIDE_SEPARATED_WALK",
    symbols_analyzed: symbolBundles.map((b) => b.symbol),
    symbols_count: symbolBundles.length,
    cycles_max: MAX_CYCLES,
    target_success_patterns_total: TARGET_SUCCESS_TOTAL,
    min_active_per_side: MIN_ACTIVE_PER_SIDE,
    successful_patterns_count: successfulPatterns.length,
    failed_patterns_count: failedPatterns.length,
    successful_patterns: successfulPatterns,
    failed_patterns: failedPatterns,
    active_patterns_left: activePatterns,
    attempts,
    token_usage: tokenUsage,
    final_results: finalPerSymbol,
  };
  fs.writeFileSync(SUMMARY_FILE, JSON.stringify(summary, null, 2));

  if (mongoose && MONGO_URI) {
    try {
      await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 7000, socketTimeoutMS: 20000 });
      const GroqUniversalRun = getGroqUniversalRunModel(mongoose);
      await GroqUniversalRun.create(summary);
      console.log("Saved summary to MongoDB.");
      await mongoose.connection.close();
    } catch (err) {
      console.error("MongoDB save failed (local files still saved):", err.message);
      try { await mongoose.connection.close(); } catch (_) {}
    }
  } else {
    console.log("MongoDB save skipped (mongoose or MONGO_URI not configured).");
  }

  try { db.close(); } catch (_) {}
  console.log(`Complete. Summary: ${SUMMARY_FILE}`);
}

run().catch((err) => {
  console.error("Fatal:", err.message || err);
  process.exitCode = 1;
});
