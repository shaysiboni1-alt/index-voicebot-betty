// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline + SSOT)
// Baseline preserved (audio queue, VAD, debounce gates).
// Adds:
// A) STT transcript logs (from Realtime events) controlled by ENV
// B) Assistant text logs controlled by ENV (includes audio_transcript)
// C) POST /admin/reload-sheets (force SSOT reload) for Apps Script
// D) SSOT prewarm + cache-first on call open (NO BLOCKING on open path)
// E) MEMORY_DB auto-migration + safe prewarm + runtime lookup/upsert/name-save
// F) BARGE-IN (response.cancel) with noise-resistant gating (MIN_MS + COOLDOWN)
// G) Name Gate helper (reject obvious non-names / noise)
// H) Closing enforcement in code when caller says bye/thanks/end (never opens new topics after closing)

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

// pg is optional. Only used when DB URL exists.
let Pg;
try {
  Pg = require("pg");
} catch {
  Pg = null;
}

const PORT = process.env.PORT || 10000;

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim(); // optional
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

const MB_BARGEIN_ENABLED = String(process.env.MB_BARGEIN_ENABLED || "").toLowerCase() === "true";
const MB_BARGEIN_MIN_MS = Number(process.env.MB_BARGEIN_MIN_MS || 250);
const MB_BARGEIN_COOLDOWN_MS = Number(process.env.MB_BARGEIN_COOLDOWN_MS || 600);

const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SA_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

const MB_DEBUG = String(process.env.MB_DEBUG || "").toLowerCase() === "true";
const MB_LOG_TRANSCRIPTS =
  String(process.env.MB_LOG_TRANSCRIPTS || "").toLowerCase() === "true";
const MB_LOG_ASSISTANT_TEXT =
  String(process.env.MB_LOG_ASSISTANT_TEXT || "").toLowerCase() === "true";

// DB URL (Render: DATABASE_URL)
const MEMORY_DB_URL = (process.env.MEMORY_DB_URL || process.env.DATABASE_URL || "").trim();

if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

function nowIso() {
  return new Date().toISOString();
}

function getGreetingBucketAndText() {
  let hour = 0;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: TIME_ZONE,
      hour12: false,
      hour: "2-digit",
    }).formatToParts(new Date());
    hour = Number(parts.find((p) => p.type === "hour")?.value || "0");
  } catch {
    hour = new Date().getHours();
  }
  if (hour >= 5 && hour < 12) return { bucket: "morning", text: "בוקר טוב" };
  if (hour >= 12 && hour < 17) return { bucket: "afternoon", text: "צהריים טובים" };
  if (hour >= 17 && hour < 22) return { bucket: "evening", text: "ערב טוב" };
  return { bucket: "night", text: "לילה טוב" };
}

function injectVars(text, vars) {
  let out = String(text || "");
  for (const [k, v] of Object.entries(vars || {})) {
    out = out.replaceAll(`{${k}}`, String(v ?? ""));
  }
  return out;
}

function normalizeE164(s) {
  const t = String(s || "").trim();
  if (!t) return null;
  // Twilio will usually send +972...
  if (t.startsWith("+") && t.length >= 8) return t;
  // Try to salvage digits
  const digits = t.replace(/[^\d+]/g, "");
  if (digits.startsWith("+") && digits.length >= 8) return digits;
  return null;
}

/* ================== MEMORY DB (safe) ================== */
const memoryDb = {
  enabled: false,
  ready: false,
  error: null,
  pool: null,
  last_ok_at: null,
};

function memoryDbEnabled() {
  return !!(MEMORY_DB_URL && Pg);
}

async function initMemoryDbIfNeeded() {
  memoryDb.enabled = memoryDbEnabled();

  if (!memoryDb.enabled) {
    memoryDb.ready = false;
    if (MEMORY_DB_URL && !Pg) memoryDb.error = "pg module is not available (missing dependency).";
    return memoryDb;
  }

  if (memoryDb.pool) return memoryDb;

  try {
    const { Pool } = Pg;
    memoryDb.pool = new Pool({
      connectionString: MEMORY_DB_URL,
      ssl: MEMORY_DB_URL.includes("render.com") ? { rejectUnauthorized: false } : undefined,
      max: 2,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 10_000,
    });

    await memoryDb.pool.query("SELECT 1");

    // IMPORTANT: We align on caller_id_e164 (TEXT) as canonical key.
    // Create table if missing, then add any missing columns (handles older broken schemas).
    await memoryDb.pool.query(`
      CREATE TABLE IF NOT EXISTS caller_memory (
        caller_id_e164 TEXT PRIMARY KEY,
        name TEXT,
        call_count INTEGER DEFAULT 0,
        last_call_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    const alters = [
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS caller_id_e164 TEXT;`,
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS name TEXT;`,
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS call_count INTEGER DEFAULT 0;`,
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS last_call_at TIMESTAMPTZ;`,
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();`,
      `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`,
    ];

    for (const sql of alters) {
      await memoryDb.pool.query(sql);
    }

    // If table existed without PK on caller_id_e164, we won't try to rewrite PK (dangerous).
    // But we DO ensure the column exists and we use it for lookups going forward.

    await memoryDb.pool.query(
      `CREATE INDEX IF NOT EXISTS idx_caller_memory_last_call_at ON caller_memory (last_call_at);`
    );

    memoryDb.ready = true;
    memoryDb.error = null;
    memoryDb.last_ok_at = nowIso();

    if (MB_DEBUG) console.log("[MEMORY_DB] ready");
  } catch (e) {
    memoryDb.ready = false;
    memoryDb.error = e && (e.message || String(e));
    console.error("[MEMORY_DB] init failed", memoryDb.error);
  }

  return memoryDb;
}

async function memoryLookup(callerE164) {
  try {
    if (!callerE164) return null;
    await initMemoryDbIfNeeded();
    if (!memoryDb.ready || !memoryDb.pool) return null;

    const res = await memoryDb.pool.query(
      `SELECT caller_id_e164, name, call_count, last_call_at FROM caller_memory WHERE caller_id_e164 = $1 LIMIT 1`,
      [callerE164]
    );
    return res.rows?.[0] || null;
  } catch (e) {
    console.error("[MEMORY] lookup failed", e && (e.message || String(e)));
    return null;
  }
}

async function memoryUpsertCall(callerE164) {
  try {
    if (!callerE164) return;
    await initMemoryDbIfNeeded();
    if (!memoryDb.ready || !memoryDb.pool) return;

    await memoryDb.pool.query(
      `
      INSERT INTO caller_memory (caller_id_e164, call_count, last_call_at, created_at, updated_at)
      VALUES ($1, 1, NOW(), NOW(), NOW())
      ON CONFLICT (caller_id_e164)
      DO UPDATE SET
        call_count = COALESCE(caller_memory.call_count, 0) + 1,
        last_call_at = NOW(),
        updated_at = NOW()
      `,
      [callerE164]
    );

    memoryDb.last_ok_at = nowIso();
  } catch (e) {
    console.error("[MEMORY] upsert_call failed", e && (e.message || String(e)));
  }
}

async function memorySaveName(callerE164, name) {
  try {
    if (!callerE164) return;
    const n = String(name || "").trim();
    if (!n) return;

    await initMemoryDbIfNeeded();
    if (!memoryDb.ready || !memoryDb.pool) return;

    await memoryDb.pool.query(
      `
      INSERT INTO caller_memory (caller_id_e164, name, call_count, last_call_at, created_at, updated_at)
      VALUES ($1, $2, 1, NOW(), NOW(), NOW())
      ON CONFLICT (caller_id_e164)
      DO UPDATE SET
        name = EXCLUDED.name,
        updated_at = NOW()
      `,
      [callerE164, n]
    );

    memoryDb.last_ok_at = nowIso();
  } catch (e) {
    console.error("[MEMORY] name_saved failed", e && (e.message || String(e)));
  }
}

// Prewarm memory DB at startup (never blocks boot)
(async () => {
  const t0 = Date.now();
  try {
    await initMemoryDbIfNeeded();
  } finally {
    if (MB_DEBUG) {
      console.log("[MEMORY_DB] prewarm", {
        enabled: memoryDb.enabled,
        ready: memoryDb.ready,
        error: memoryDb.error,
        ms: Date.now() - t0,
      });
    }
  }
})();

/* ================== SSOT ================== */
const SSOT_TTL_MS = 60_000;

const ssot = {
  enabled: !!(GSHEET_ID && GOOGLE_SA_B64),
  loaded_at: null,
  error: null,
  data: {
    settings: {},
    prompts: {},
    intents: [],
    intent_suggestions: [],
  },
  _expires: 0,
};

async function loadSSOT(force = false) {
  if (!ssot.enabled) return ssot;

  const now = Date.now();
  if (!force && now < ssot._expires && ssot.loaded_at) return ssot;

  try {
    const creds = JSON.parse(Buffer.from(GOOGLE_SA_B64, "base64").toString("utf8"));

    const auth = new google.auth.JWT(
      creds.client_email,
      null,
      creds.private_key,
      ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    );

    const sheets = google.sheets({ version: "v4", auth });

    async function read(range) {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: GSHEET_ID,
        range,
      });
      return res.data.values || [];
    }

    const settingsRows = await read("SETTINGS!A:B");
    const settings = {};
    settingsRows.slice(1).forEach(([k, v]) => {
      if (k) settings[String(k).trim()] = v ?? "";
    });

    const promptRows = await read("PROMPTS!A:B");
    const prompts = {};
    promptRows.slice(1).forEach(([id, content]) => {
      if (id) prompts[String(id).trim()] = content ?? "";
    });

    const intentRows = await read("INTENTS!A:D");
    const intents = intentRows.slice(1).map((r) => ({
      intent: r[0],
      priority: Number(r[1] || 0),
      trigger_type: r[2],
      triggers_he: r[3],
    }));

    const suggRows = await read("INTENT_SUGGESTIONS!A:F");
    const intent_suggestions = suggRows.slice(1).map((r) => ({
      phrase_he: r[0],
      detected_intent: r[1],
      occurrences: r[2],
      last_seen_at: r[3],
      approved: r[4],
      notes: r[5],
    }));

    ssot.data = { settings, prompts, intents, intent_suggestions };
    ssot.loaded_at = nowIso();
    ssot.error = null;
    ssot._expires = Date.now() + SSOT_TTL_MS;

    if (MB_DEBUG) {
      console.log("[SSOT] loaded", {
        settings_keys: Object.keys(settings).length,
        prompts_keys: Object.keys(prompts).length,
        intents: intents.length,
        intent_suggestions: intent_suggestions.length,
      });
    }
  } catch (e) {
    ssot.error = e && (e.message || String(e));
    console.error("[SSOT] load failed", ssot.error);
  }

  return ssot;
}

function getSSOTCacheFast() {
  const now = Date.now();
  const hasData =
    Object.keys(ssot.data.settings || {}).length > 0 && Object.keys(ssot.data.prompts || {}).length > 0;

  const stale = !ssot.loaded_at || now >= ssot._expires;

  // If we have data, DO NOT block the call path: use cache immediately.
  // If stale, refresh in background.
  if (hasData) {
    if (stale) {
      if (MB_DEBUG) console.log("[SSOT] stale cache -> background refresh");
      loadSSOT(true).catch(() => {});
    }
    return { ok: true, stale, ssot };
  }

  return { ok: false, stale: true, ssot };
}

// Prewarm SSOT at startup (non-blocking)
(async () => {
  if (!ssot.enabled) return;
  try {
    const t0 = Date.now();
    await loadSSOT(false);
    if (MB_DEBUG) console.log("[SSOT] prewarm done in", Date.now() - t0, "ms");
  } catch (e) {
    console.error("[SSOT] prewarm failed", e && (e.message || e));
  }
})();

function buildSettingsContext(settings) {
  const lines = Object.entries(settings || {}).map(([k, v]) => `${k}=${String(v ?? "")}`);
  return lines.join("\n");
}

/* ================== Name heuristics ================== */
const NAME_REJECT_WORDS = new Set([
  "היי","שלום","כן","לא","רגע","שנייה","שניה","אוקיי","אוקי","המ","אמ","אה","טוב","ביי","להתראות","תודה",
  "מה","למה","איפה","מתי","איך","כמה","מי","בסדר","תשמע","תקשיבי","תקשיבו","בסדר גמור"
]);

function looksLikeName(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  const cleaned = t.replace(/[^\u0590-\u05FFa-zA-Z\s'-]/g, "").trim(); // allow heb/eng letters
  if (!cleaned) return false;
  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length > 3) return false;
  if (cleaned.length > 22) return false;
  if (/\d/.test(t)) return false;

  const lower = cleaned.toLowerCase();
  // reject if contains question words / fillers
  for (const w of words) {
    const ww = w.toLowerCase();
    if (NAME_REJECT_WORDS.has(ww)) return false;
  }
  // if it's single very common preposition-like "בבית/במשחקים" etc -> reject
  if (words.length === 1) {
    const w = words[0];
    if (w.startsWith("ב") && w.length >= 4) return false; // crude but effective against "בבית/במשחקים"
  }
  return true;
}

/* ================== Closing enforcement ================== */
function isCallerWantsToEnd(stt) {
  const t = String(stt || "").trim();
  if (!t) return false;
  return /(ביי|להתראות|סיימנו|זהו|תודה ביי|יאללה ביי|נדבר|סגרנו)/.test(t);
}

/* ================== APP ================== */
const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  await loadSSOT(false);
  await initMemoryDbIfNeeded();

  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
    ssot: {
      enabled: ssot.enabled,
      loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys: Object.keys(ssot.data.settings || {}).length,
      prompts_keys: Object.keys(ssot.data.prompts || {}).length,
      intents: ssot.data.intents.length,
      intent_suggestions: ssot.data.intent_suggestions.length,
    },
    memory_db: {
      enabled: memoryDb.enabled,
      ready: memoryDb.ready,
      error: memoryDb.error,
      last_ok_at: memoryDb.last_ok_at,
    },
    model: OPENAI_REALTIME_MODEL,
  });
});

// REQUIRED endpoint for Apps Script
app.post("/admin/reload-sheets", async (req, res) => {
  const started = nowIso();
  await loadSSOT(true);

  const settings_keys = Object.keys(ssot.data.settings || {}).length;
  const prompt_ids = Object.keys(ssot.data.prompts || {});
  const intents = ssot.data.intents?.length || 0;
  const intent_suggestions = ssot.data.intent_suggestions?.length || 0;

  if (ssot.error) {
    return res.status(500).json({
      ok: false,
      reloaded_at: started,
      sheets_loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys,
      prompt_ids,
      intents,
      intent_suggestions,
    });
  }

  return res.json({
    ok: true,
    reloaded_at: started,
    sheets_loaded_at: ssot.loaded_at,
    settings_keys,
    prompt_ids,
    intents,
    intent_suggestions,
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/* ================== WS ================== */
wss.on("connection", (twilioWs) => {
  if (MB_DEBUG) console.log("[WS] connection established");

  let streamSid = null;
  let callSid = null;
  let callerE164 = null;

  const connT0 = Date.now();
  let twilioStartAt = null;

  let openaiReady = false;
  let sessionConfigured = false;
  let pendingCreate = false;
  let responseInFlight = false;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  // BARGE-IN state
  let speechActive = false;
  let speechStartedAt = 0;
  let lastBargeinAt = 0;
  let bargeinTimer = null;

  // Audio queue
  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  // Transcript aggregation
  let sttBuf = "";
  let asstBuf = "";
  let asstAudioTranscriptBuf = "";

  // Name gate state
  let expectingName = false;
  let capturedName = null;

  // Closing state
  let closingForced = false;

  function logSTTLine(line) {
    if (!MB_LOG_TRANSCRIPTS) return;
    const t = String(line || "").trim();
    if (!t) return;
    console.log(`[STT] ${t}`);
  }

  function logAsstDelta(delta) {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const d = String(delta || "");
    if (!d) return;
    asstBuf += d;
    process.stdout.write(d);
  }

  function flushAsstLine() {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const t = String(asstBuf || "").trim();
    if (t) process.stdout.write("\n");
    asstBuf = "";
  }

  function logAsstAudioTranscriptDelta(delta) {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const d = String(delta || "");
    if (!d) return;
    asstAudioTranscriptBuf += d;
    process.stdout.write(d);
  }

  function flushAsstAudioTranscriptLine() {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const t = String(asstAudioTranscriptBuf || "").trim();
    if (t) process.stdout.write("\n");
    asstAudioTranscriptBuf = "";
  }

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(obj));
      return true;
    } catch (e) {
      console.error("[OPENAI] send failed", e && (e.message || e));
      return false;
    }
  }

  function flushAudioQueue(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (!openaiReady || !sessionConfigured) return;

    while (audioQueue.length) {
      const payload = audioQueue.shift();
      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }
  }

  function maybeCreateResponse(openaiWs, reason) {
    const now = Date.now();
    if (responseInFlight) return;

    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
      pendingCreate = true;
      return;
    }

    // KEEP baseline values
    const DEBOUNCE_MS = 350;
    const MIN_FRAMES = 4;

    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    // If we already forced closing, we don't let the model roam:
    if (closingForced) {
      safeSend(openaiWs, {
        type: "response.create",
        response: {
          modalities: ["audio", "text"],
          instructions: "ענו במשפט קצר ומנומס לסיום השיחה בלבד: תודה ולהתראות.",
        },
      });
      responseInFlight = true;
      if (MB_DEBUG) console.log("[CLOSING] forced");
      return;
    }

    safeSend(openaiWs, { type: "response.create" });
    pendingCreate = false;
    responseInFlight = true;
    if (MB_DEBUG) console.log("[TURN] response.create", reason || "speech_stopped");
  }

  function cancelAssistant(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    safeSend(openaiWs, { type: "response.cancel" });
    if (MB_DEBUG) console.log("[BARGEIN] response.cancel");
  }

  function onSpeechStarted(openaiWs) {
    speechActive = true;
    speechStartedAt = Date.now();

    if (!MB_BARGEIN_ENABLED) return;
    if (!responseInFlight) return;

    const now = Date.now();
    if (now - lastBargeinAt < MB_BARGEIN_COOLDOWN_MS) return;

    // Noise-resistant gating:
    // only cancel if speech is still active after MIN_MS (filters short bursts)
    if (bargeinTimer) clearTimeout(bargeinTimer);
    bargeinTimer = setTimeout(() => {
      if (!speechActive) return;
      const now2 = Date.now();
      if (!responseInFlight) return;
      if (now2 - lastBargeinAt < MB_BARGEIN_COOLDOWN_MS) return;

      lastBargeinAt = now2;
      cancelAssistant(openaiWs);
    }, Math.max(0, MB_BARGEIN_MIN_MS));
  }

  function onSpeechStopped() {
    speechActive = false;
    if (bargeinTimer) {
      clearTimeout(bargeinTimer);
      bargeinTimer = null;
    }
  }

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openaiWs.on("open", async () => {
    if (MB_DEBUG) console.log("[OPENAI] ws open");
    openaiReady = true;

    // FAST PATH: do not block on SSOT load if cache exists
    const cache = getSSOTCacheFast();
    if (!cache.ok) {
      // first-ever load: we must load once (no choice)
      await loadSSOT(true);
    }

    const { settings, prompts } = ssot.data;
    const g = getGreetingBucketAndText();

    // Memory lookup (non-blocking: if fails, proceed normally)
    let memoryRow = null;
    if (callerE164) {
      memoryRow = await memoryLookup(callerE164);
      if (memoryRow && memoryRow.name) {
        if (MB_DEBUG) console.log("[MEMORY] hit", { caller: callerE164, name: memoryRow.name });
      } else {
        if (MB_DEBUG) console.log("[MEMORY] miss", { caller: callerE164 });
      }
      // upsert call count/last_call_at regardless
      memoryUpsertCall(callerE164).catch(() => {});
    }

    // OPENING
    const openingTemplate = settings.OPENING_SCRIPT || "";
    const returningTemplate = settings.OPENING_SCRIPT_RETURNING || "";

    let opening = "";
    if (memoryRow && memoryRow.name) {
      // If returning template exists use it; else use approved default text.
      if (String(returningTemplate || "").trim()) {
        opening = injectVars(returningTemplate, {
          GREETING: g.text,
          BOT_NAME: settings.BOT_NAME,
          BUSINESS_NAME: settings.BUSINESS_NAME,
          CALLER_NAME: memoryRow.name,
        });
      } else {
        opening = `${g.text}, ${memoryRow.name}, נעים לשמוע ממך שוב. איך נוכל לעזור?`;
      }
    } else {
      opening = injectVars(openingTemplate, {
        GREETING: g.text,
        BOT_NAME: settings.BOT_NAME,
        BUSINESS_NAME: settings.BUSINESS_NAME,
      });
    }

    const settingsContext = buildSettingsContext(settings);

    const master = injectVars(prompts.MASTER_PROMPT || "", settings);
    const guard = injectVars(prompts.GUARDRAILS_PROMPT || "", settings);
    const kb = injectVars(prompts.KB_PROMPT || "", settings);
    const leadCapture = injectVars(prompts.LEAD_CAPTURE_PROMPT || "", settings);

    const hardNoHallucinationLayer = [
      "חוק על: SETTINGS_CONTEXT הוא מקור האמת היחיד לפרטי המשרד והערכים העסקיים.",
      "אסור להשתמש בשום מידע שלא מופיע ב-SETTINGS_CONTEXT.",
      "אם נשאלת שאלה שאין לה ערך מפורש ב-SETTINGS_CONTEXT - השתמשי ב-NO_DATA_MESSAGE מתוך SETTINGS ואז חזרי לשיחה.",
      "OPENING_SCRIPT: כאשר משתמשים בו - יש לומר מילה במילה ללא שינוי.",
      "אין לטעון שמרגריטה עזבה/אינה עובדת; מותר רק לומר שהיא לא זמינה כרגע.",
    ].join(" ");

    const instructions = [
      master,
      guard,
      kb,
      leadCapture,
      hardNoHallucinationLayer,
      "SETTINGS_CONTEXT (Key=Value):\n" + settingsContext,
    ].join("\n\n");

    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      instructions,
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        silence_duration_ms: MB_VAD_SILENCE_MS,
        prefix_padding_ms: MB_VAD_PREFIX_MS,
      },
      max_response_output_tokens: "inf",
    };

    if (MB_TRANSCRIPTION_MODEL) {
      session.input_audio_transcription = {
        model: MB_TRANSCRIPTION_MODEL,
        language: MB_TRANSCRIPTION_LANGUAGE,
      };
    }

    safeSend(openaiWs, { type: "session.update", session });
    sessionConfigured = true;

    // Opening: speak immediately (no extra pre-say)
    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions:
          "דברי עכשיו את הטקסט הבא ללא תוספות לפני/אחרי, באותו ניסוח בדיוק:\n" + opening,
      },
    });

    if (MB_DEBUG) {
      const tSinceConn = Date.now() - connT0;
      const tSinceStart = twilioStartAt ? Date.now() - twilioStartAt : null;
      console.log("[LATENCY] opening response.create sent", {
        ms_since_ws_connection: tSinceConn,
        ms_since_twilio_start: tSinceStart,
      });
    }

    responseInFlight = true;
    flushAudioQueue(openaiWs);
    if (pendingCreate) maybeCreateResponse(openaiWs, "pending_after_open");
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // STT transcription (defensive)
    if (MB_LOG_TRANSCRIPTS) {
      if (msg && typeof msg.type === "string" && msg.type.toLowerCase().includes("transcription")) {
        if (typeof msg.delta === "string") sttBuf += msg.delta;

        if (typeof msg.text === "string") {
          logSTTLine(msg.text);
          sttBuf = "";
        }
        if (typeof msg.transcript === "string") {
          logSTTLine(msg.transcript);
          sttBuf = "";
        }

        if (
          msg.type.toLowerCase().includes("done") ||
          msg.type.toLowerCase().includes("completed") ||
          msg.type.toLowerCase().includes("result")
        ) {
          if (sttBuf.trim()) logSTTLine(sttBuf);
          sttBuf = "";
        }
      }
    }

    // Assistant text logs
    if (MB_LOG_ASSISTANT_TEXT) {
      if (msg.type === "response.output_text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.output_text.done" || msg.type === "response.text.done") {
        flushAsstLine();
      }

      if (msg.type === "response.audio_transcript.delta" && typeof msg.delta === "string") {
        logAsstAudioTranscriptDelta(msg.delta);
      }
      if (msg.type === "response.audio_transcript.done") {
        flushAsstAudioTranscriptLine();
      }

      if (msg.type === "response.completed") {
        flushAsstLine();
        flushAsstAudioTranscriptLine();
      }
    }

    // BARGE-IN hooks (if available)
    if (msg.type === "input_audio_buffer.speech_started") {
      onSpeechStarted(openaiWs);
      return;
    }
    if (msg.type === "input_audio_buffer.speech_stopped") {
      onSpeechStopped();
      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    // When model finished speaking, allow next user turn
    if (msg.type === "response.audio.done" || msg.type === "response.completed") {
      responseInFlight = false;
      return;
    }

    // AUDIO OUT (DO NOT TOUCH)
    if (msg.type === "response.audio.delta" && streamSid) {
      try {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload: msg.delta },
          })
        );
      } catch (e) {
        console.error("[TWILIO] send media failed", e && (e.message || e));
      }
    }
  });

  openaiWs.on("error", (e) => {
    responseInFlight = false;
    console.error("[OPENAI] ws error", e && (e.message || e));
  });

  openaiWs.on("close", (code, reason) => {
    responseInFlight = false;
    if (MB_DEBUG) console.log("[OPENAI] ws closed", { code, reason: String(reason || "") });
  });

  // Track last user STT for name/closing decisions (simple: rely on MB_LOG_TRANSCRIPTS events above isn't enough)
  // We'll infer from the raw transcription "text" event by capturing it here:
  let lastUserUtterance = "";

  // We can capture user utterance if transcription sends msg.text/msg.transcript,
  // but we already parse those above without storing. Add a lightweight store:
  function captureUserUtteranceFromTranscriptionEvent(msg) {
    if (!msg || typeof msg.type !== "string") return;
    if (!msg.type.toLowerCase().includes("transcription")) return;

    const t = (typeof msg.text === "string" && msg.text) || (typeof msg.transcript === "string" && msg.transcript) || "";
    if (t.trim()) lastUserUtterance = t.trim();
  }

  // Patch capture inside openaiWs message pipeline by re-parsing last seen msg:
  // (We don't want to restructure too much; just do it here on next tick via twilio audio stop? Not needed.)
  // Instead: capture lastUserUtterance on speech_stopped by using sttBuf fallback (best effort).

  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;
      twilioStartAt = Date.now();

      const callerRaw = msg.start?.customParameters?.caller || msg.start?.caller || null;
      callerE164 = normalizeE164(callerRaw);

      if (MB_DEBUG) {
        console.log("[WS] start", {
          accountSid: msg.start?.accountSid,
          streamSid,
          callSid,
          tracks: msg.start?.tracks,
          mediaFormat: msg.start?.mediaFormat,
          customParameters: msg.start?.customParameters || {},
        });
      }
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;

      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
        audioQueue.push(payload);
        if (audioQueue.length > MAX_QUEUE_FRAMES) {
          audioQueue.splice(0, audioQueue.length - MAX_QUEUE_FRAMES);
        }
        return;
      }

      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
      if (MB_DEBUG) console.log("[WS] stop", { callSid: msg.stop?.callSid || callSid });
      try {
        if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_stop");
      } catch {}
      return;
    }
  });

  // Extra: if we have transcription text arriving (best effort), do simple state actions.
  // Because OpenAI events vary, we add a second listener that tries to store utterances.
  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    captureUserUtteranceFromTranscriptionEvent(msg);

    // Detect caller wants to end (code-level closing lock)
    if (typeof lastUserUtterance === "string" && lastUserUtterance) {
      if (isCallerWantsToEnd(lastUserUtterance)) {
        closingForced = true;
      }
    }

    // Name capture helper (when the model is asking for name, we help enforce it safely)
    // We activate expectingName when we hear the assistant ask for a name in audio_transcript (best-effort).
    if (msg.type === "response.audio_transcript.delta" && typeof msg.delta === "string") {
      const d = msg.delta;
      if (/מה השם|מה שמך|איך אפשר לפנות|שם בבקשה/.test(d)) {
        expectingName = true;
        if (MB_DEBUG) console.log("[NAME] expecting_name");
      }
    }

    // On speech_stopped, if expectingName and we have a recent user utterance, validate it as a name
    if (msg.type === "input_audio_buffer.speech_stopped") {
      const u = String(lastUserUtterance || "").trim();
      if (expectingName && u) {
        if (looksLikeName(u)) {
          capturedName = u;
          expectingName = false;
          if (MB_DEBUG) console.log("[NAME] captured", { name: capturedName });

          // Save to memory
          if (callerE164) memorySaveName(callerE164, capturedName).catch(() => {});
        } else {
          // reject and force a short re-ask (one question)
          expectingName = true;
          if (MB_DEBUG) console.log("[NAME] rejected", { utterance: u });

          if (openaiWs.readyState === WebSocket.OPEN) {
            safeSend(openaiWs, {
              type: "response.create",
              response: {
                modalities: ["audio", "text"],
                instructions: "עני בעברית בלשון רבים, במשפט אחד בלבד: נשמח לדעת איך אפשר לפנות אליכם — מה השם בבקשה?",
              },
            });
            responseInFlight = true;
          }
        }
      }
    }
  });

  twilioWs.on("close", () => {
    if (MB_DEBUG) console.log("[WS] closed");
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_close");
    } catch {}
  });

  twilioWs.on("error", (e) => {
    console.error("[TWILIO] ws error", e && (e.message || e));
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1011, "twilio_error");
    } catch {}
  });
});

server.listen(PORT, () => {
  const base = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: ${base}/health`);
});
