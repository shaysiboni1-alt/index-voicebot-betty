// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline + SSOT)
// Baseline preserved (audio queue, VAD, debounce gates).
// Adds ONLY:
// A) STT transcript logs (from Realtime events) controlled by ENV
// B) Assistant text logs controlled by ENV  (NOW includes audio_transcript)
// C) POST /admin/reload-sheets (force SSOT reload) for Apps Script
// D) SSOT prewarm + cache-first on call open to reduce opening latency (safe fallback to force reload)
// E) MEMORY DB (Postgres) OPTIONAL + safe (no DB -> no crash)
// F) Admin auth token (Bearer) for /admin/*
// G) POST /admin/purge-memory (monthly delete from app)

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

// OPTIONAL DB (safe)
let Pool = null;
try {
  ({ Pool } = require("pg"));
} catch {
  // If pg not installed, we keep running (but Render will crash only if code path requires it).
  // We'll still guard all DB usage behind availability checks.
  Pool = null;
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

const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SA_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

// LOG FLAGS
const MB_DEBUG = String(process.env.MB_DEBUG || "").toLowerCase() === "true";
const MB_LOG_TRANSCRIPTS =
  String(process.env.MB_LOG_TRANSCRIPTS || "").toLowerCase() === "true";
const MB_LOG_ASSISTANT_TEXT =
  String(process.env.MB_LOG_ASSISTANT_TEXT || "").toLowerCase() === "true";

// ADMIN TOKEN (Bearer)
const MB_ADMIN_TOKEN = (process.env.MB_ADMIN_TOKEN || "").trim();

// MEMORY DB (Postgres on Render)
const DATABASE_URL = (process.env.DATABASE_URL || "").trim();
const MB_MEMORY_ENABLED =
  String(process.env.MB_MEMORY_ENABLED || "true").toLowerCase() === "true";

// Memory behavior (locked by your choices)
const MB_MEMORY_USE_REPEAT_GREETING =
  String(process.env.MB_MEMORY_USE_REPEAT_GREETING || "true").toLowerCase() === "true";
// Format you approved:
// "{GREETING}, שי, נעים לשמוע ממך שוב. איך נוכל לעזור?"
const MB_MEMORY_REPEAT_TEMPLATE =
  (process.env.MB_MEMORY_REPEAT_TEMPLATE ||
    "{GREETING}, {NAME}, נעים לשמוע ממך שוב. איך נוכל לעזור?").trim();

// Monthly purge default: 30 days
const MB_MEMORY_RETENTION_DAYS = Number(process.env.MB_MEMORY_RETENTION_DAYS || 30);

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

/* ================== ADMIN AUTH ================== */
function requireAdmin(req, res, next) {
  // If no token set, keep endpoints open (dev convenience) – but recommended to set token in Render.
  if (!MB_ADMIN_TOKEN) return next();
  const auth = String(req.headers.authorization || "");
  if (!auth.startsWith("Bearer ")) return res.status(401).json({ ok: false, error: "missing_bearer" });
  const token = auth.slice("Bearer ".length).trim();
  if (token !== MB_ADMIN_TOKEN) return res.status(403).json({ ok: false, error: "forbidden" });
  return next();
}

/* ================== MEMORY DB (OPTIONAL, SAFE) ================== */
let dbPool = null;
let dbReady = false;
let dbError = null;

function normalizeCallerIdToE164(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  // Twilio usually provides +972...
  if (s.startsWith("+") && s.length >= 8) return s;
  // If already digits, attempt best-effort:
  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return null;
  if (digits.startsWith("972")) return "+" + digits;
  if (digits.startsWith("0") && digits.length >= 9) return "+972" + digits.slice(1);
  return "+" + digits;
}

async function initDbIfNeeded() {
  if (!MB_MEMORY_ENABLED) return { enabled: false, ready: false, error: null };
  if (!DATABASE_URL) return { enabled: false, ready: false, error: null };
  if (!Pool) {
    dbError = "pg_module_missing";
    return { enabled: true, ready: false, error: dbError };
  }
  if (dbReady && dbPool) return { enabled: true, ready: true, error: null };

  try {
    dbPool = new Pool({
      connectionString: DATABASE_URL,
      ssl: DATABASE_URL.includes("render.com") ? { rejectUnauthorized: false } : undefined,
      max: 5,
    });

    // create table (idempotent)
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS caller_memory (
        caller_id_e164 TEXT PRIMARY KEY,
        name TEXT,
        last_intent TEXT,
        last_call_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    // index for cleanup
    await dbPool.query(`
      CREATE INDEX IF NOT EXISTS idx_caller_memory_last_call_at
      ON caller_memory (last_call_at);
    `);

    dbReady = true;
    dbError = null;
    if (MB_DEBUG) console.log("[MEMORY_DB] ready");
    return { enabled: true, ready: true, error: null };
  } catch (e) {
    dbReady = false;
    dbError = e && (e.message || String(e));
    console.error("[MEMORY_DB] init failed", dbError);
    return { enabled: true, ready: false, error: dbError };
  }
}

async function getCallerMemory(callerE164) {
  try {
    if (!MB_MEMORY_ENABLED || !dbReady || !dbPool || !callerE164) return null;
    const r = await dbPool.query(
      `SELECT caller_id_e164, name, last_intent, last_call_at FROM caller_memory WHERE caller_id_e164 = $1 LIMIT 1`,
      [callerE164]
    );
    return r.rows?.[0] || null;
  } catch (e) {
    if (MB_DEBUG) console.error("[MEMORY_DB] get failed", e && (e.message || e));
    return null;
  }
}

async function upsertCallerMemory({ callerE164, name, lastIntent }) {
  try {
    if (!MB_MEMORY_ENABLED || !dbReady || !dbPool || !callerE164) return false;
    await dbPool.query(
      `
      INSERT INTO caller_memory (caller_id_e164, name, last_intent, last_call_at, updated_at)
      VALUES ($1, $2, $3, NOW(), NOW())
      ON CONFLICT (caller_id_e164)
      DO UPDATE SET
        name = COALESCE(EXCLUDED.name, caller_memory.name),
        last_intent = COALESCE(EXCLUDED.last_intent, caller_memory.last_intent),
        last_call_at = NOW(),
        updated_at = NOW()
      `,
      [callerE164, name || null, lastIntent || null]
    );
    return true;
  } catch (e) {
    if (MB_DEBUG) console.error("[MEMORY_DB] upsert failed", e && (e.message || e));
    return false;
  }
}

async function purgeMemoryOlderThan(days) {
  try {
    if (!MB_MEMORY_ENABLED || !dbReady || !dbPool) return { ok: false, deleted: 0, error: "memory_disabled_or_db_not_ready" };
    const d = Number(days || 30);
    const r = await dbPool.query(
      `DELETE FROM caller_memory WHERE last_call_at IS NOT NULL AND last_call_at < NOW() - ($1 || ' days')::interval`,
      [String(d)]
    );
    return { ok: true, deleted: r.rowCount || 0, error: null };
  } catch (e) {
    return { ok: false, deleted: 0, error: e && (e.message || String(e)) };
  }
}

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

// PREWARM SSOT at server startup (non-blocking)
(async () => {
  try {
    if (ssot.enabled) {
      const t0 = Date.now();
      await loadSSOT(false);
      if (MB_DEBUG) console.log("[SSOT] prewarm done in", Date.now() - t0, "ms");
    }
  } catch (e) {
    console.error("[SSOT] prewarm failed", e && (e.message || e));
  }
})();

// PREWARM DB at server startup (non-blocking)
(async () => {
  try {
    const t0 = Date.now();
    const r = await initDbIfNeeded();
    if (MB_DEBUG) console.log("[MEMORY_DB] prewarm", { ...r, ms: Date.now() - t0 });
  } catch (e) {
    // already logged
  }
})();

function injectVars(text, vars) {
  let out = String(text || "");
  for (const [k, v] of Object.entries(vars || {})) {
    out = out.replaceAll(`{${k}}`, String(v ?? ""));
  }
  return out;
}

function buildSettingsContext(settings) {
  const lines = Object.entries(settings || {}).map(([k, v]) => `${k}=${String(v ?? "")}`);
  return lines.join("\n");
}

/* ================== APP ================== */
const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  await loadSSOT(false);
  const db = await initDbIfNeeded();

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
    memory_db: db,
    model: OPENAI_REALTIME_MODEL,
  });
});

// ✅ REQUIRED endpoint for Apps Script (protected if token set)
app.post("/admin/reload-sheets", requireAdmin, async (req, res) => {
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

// ✅ Monthly purge from app (protected if token set)
app.post("/admin/purge-memory", requireAdmin, async (req, res) => {
  await initDbIfNeeded();
  const days = Number(req.body?.days || MB_MEMORY_RETENTION_DAYS || 30);
  const r = await purgeMemoryOlderThan(days);
  return res.status(r.ok ? 200 : 500).json({
    ok: r.ok,
    deleted: r.deleted,
    retention_days: days,
    error: r.error,
    ts: nowIso(),
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/* ================== WS ================== */
wss.on("connection", (twilioWs) => {
  if (MB_DEBUG) console.log("[WS] connection established");

  let streamSid = null;
  let callSid = null;

  // caller info for memory
  let callerRaw = null;
  let callerE164 = null;
  let rememberedName = null;

  // capture name from early STT (minimal, safe)
  let awaitingName = true; // opening asks for name
  let capturedName = null;

  // for latency measurement
  const connT0 = Date.now();
  let twilioStartAt = null;

  // OpenAI session readiness gates
  let openaiReady = false;
  let sessionConfigured = false;
  let pendingCreate = false;
  let responseInFlight = false;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  // Queue Twilio audio until OpenAI WS is ready
  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  // Transcript aggregation
  let sttBuf = "";
  let asstBuf = "";
  let asstAudioTranscriptBuf = "";

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

    // KEEP exactly your baseline values
    const DEBOUNCE_MS = 350;
    const MIN_FRAMES = 4;

    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    safeSend(openaiWs, { type: "response.create" });
    pendingCreate = false;
    responseInFlight = true;
    if (MB_DEBUG) console.log("[TURN] response.create", reason || "speech_stopped");
  }

  function looksLikeName(utterance) {
    const t = String(utterance || "").trim();
    if (!t) return null;
    // remove common fluff
    let s = t
      .replace(/[.,!?]/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    // If user says "קוראים לי X" / "אני X"
    s = s
      .replace(/^קוראים לי\s+/i, "")
      .replace(/^אני\s+/i, "")
      .replace(/^זה\s+/i, "")
      .replace(/^השם שלי\s+/i, "")
      .trim();

    // reject digits
    if (/\d/.test(s)) return null;

    // take first 1-2 words
    const parts = s.split(" ").filter(Boolean);
    if (parts.length === 0) return null;
    const cand = parts.slice(0, 2).join(" ").trim();

    // length sanity
    if (cand.length < 2 || cand.length > 40) return null;

    // must be letters (he/en) + spaces + hyphen
    if (!/^[A-Za-z\u0590-\u05FF\s\-׳"״]+$/.test(cand)) return null;

    return cand;
  }

  async function handleUserUtteranceForMemory(text) {
    if (!awaitingName) return;
    if (capturedName) return;

    const name = looksLikeName(text);
    if (!name) return;

    capturedName = name;
    awaitingName = false;

    if (MB_DEBUG) console.log("[MEMORY] captured name", { callerE164, name });

    await initDbIfNeeded();
    if (callerE164) {
      await upsertCallerMemory({ callerE164, name, lastIntent: null });
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

    // Cache-first SSOT for low latency
    await loadSSOT(false);

    // SAFE fallback: if cache is empty or error, force reload
    const cachedSettingsKeys = Object.keys(ssot.data.settings || {}).length;
    const cachedPromptsKeys = Object.keys(ssot.data.prompts || {}).length;
    if (ssot.error || cachedSettingsKeys === 0 || cachedPromptsKeys === 0) {
      if (MB_DEBUG) console.log("[SSOT] cache empty/error -> force reload");
      await loadSSOT(true);
    }

    const { settings, prompts } = ssot.data;
    const g = getGreetingBucketAndText();

    // MEMORY: load caller name if exists
    await initDbIfNeeded();
    if (callerE164) {
      const mem = await getCallerMemory(callerE164);
      rememberedName = mem?.name || null;
      if (MB_DEBUG && rememberedName) console.log("[MEMORY] recognized caller", { callerE164, name: rememberedName });
    }

    // OPENING: from SETTINGS, but if rememberedName -> use repeat template you approved
    let opening = "";
    if (MB_MEMORY_ENABLED && rememberedName && MB_MEMORY_USE_REPEAT_GREETING) {
      opening = injectVars(MB_MEMORY_REPEAT_TEMPLATE, {
        GREETING: g.text,
        NAME: rememberedName,
      });
    } else {
      const openingTemplate = settings.OPENING_SCRIPT || "";
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
      "OPENING_SCRIPT ו-CLOSING_SCRIPT: כאשר משתמשים בהם - יש לומר מילה במילה ללא שינוי.",
      "שם פרטי בלבד מספיק; אין להתעקש על שם משפחה.",
      "דברי בלשון רבים כברירת מחדל, אלא אם המתקשר מבקש אחרת.",
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

    // Correct Realtime STT field name
    if (MB_TRANSCRIPTION_MODEL) {
      session.input_audio_transcription = {
        model: MB_TRANSCRIPTION_MODEL,
        language: MB_TRANSCRIPTION_LANGUAGE,
      };
    }

    safeSend(openaiWs, { type: "session.update", session });
    sessionConfigured = true;

    // VERBATIM opening: force speak exactly opening
    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions:
          "דברי עכשיו את הטקסט הבא מילה במילה, ללא שינוי כלל, בלי להוסיף כלום לפני או אחרי:\n" +
          opening,
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

  openaiWs.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // ======== LOGGING (defensive) + MEMORY name capture ========
    if (MB_LOG_TRANSCRIPTS) {
      if (msg && typeof msg.type === "string" && msg.type.toLowerCase().includes("transcription")) {
        if (typeof msg.delta === "string") sttBuf += msg.delta;

        if (typeof msg.text === "string") {
          logSTTLine(msg.text);
          await handleUserUtteranceForMemory(msg.text);
          sttBuf = "";
        }
        if (typeof msg.transcript === "string") {
          logSTTLine(msg.transcript);
          await handleUserUtteranceForMemory(msg.transcript);
          sttBuf = "";
        }

        if (
          msg.type.toLowerCase().includes("done") ||
          msg.type.toLowerCase().includes("completed") ||
          msg.type.toLowerCase().includes("result")
        ) {
          if (sttBuf.trim()) {
            logSTTLine(sttBuf);
            await handleUserUtteranceForMemory(sttBuf);
          }
          sttBuf = "";
        }
      }
    }
    // If transcripts logging is OFF, we still want memory capture when possible.
    // So we also listen to transcription events and try to capture without printing.
    if (!MB_LOG_TRANSCRIPTS) {
      if (msg && typeof msg.type === "string" && msg.type.toLowerCase().includes("transcription")) {
        const t = typeof msg.text === "string" ? msg.text : (typeof msg.transcript === "string" ? msg.transcript : null);
        if (t) await handleUserUtteranceForMemory(t);
      }
    }

    if (MB_LOG_ASSISTANT_TEXT) {
      // TEXT STREAM
      if (msg.type === "response.output_text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.output_text.done" || msg.type === "response.text.done") {
        flushAsstLine();
      }

      // AUDIO TRANSCRIPT
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
    // ================================================================

    if (msg.type === "input_audio_buffer.speech_stopped") {
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

      // Caller for memory (from Twilio customParameters)
      const cp = msg.start?.customParameters || {};
      callerRaw = cp.caller || cp.From || null;
      callerE164 = normalizeCallerIdToE164(callerRaw);

      if (MB_DEBUG) {
        console.log("[WS] start", {
          accountSid: msg.start?.accountSid,
          streamSid,
          callSid,
          tracks: msg.start?.tracks,
          mediaFormat: msg.start?.mediaFormat,
          customParameters: cp,
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
