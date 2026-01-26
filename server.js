// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// BASELINE: user's server_fixed.js (tested)
// ADDITIONS (minimal, safe):
// - SSOT load from Google Sheets (SETTINGS + PROMPTS + INTENTS + INTENT_SUGGESTIONS) with TTL cache
// - Use PROMPTS (if present) to build instructions; fallback to baseline hardcoded instructions
// - Use SETTINGS.OPENING_SCRIPT (if present) with {GREETING}; fallback to baseline opening
// - Buffer outbound audio until streamSid exists, and ALSO derive streamSid from msg.streamSid on media/stop
// - Fix: mark responseInFlight for initial response.create to avoid duplicate create errors

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

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

// SSOT ENV
const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();
const SSOT_CACHE_TTL_MS = Number(process.env.SSOT_CACHE_TTL_MS || 60_000);

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

// ===== SSOT (Google Sheets) =====
let ssotCache = {
  loadedAt: 0,
  settings: {},
  prompts: {},
  intents: [],
  intentSuggestions: [],
  ok: false,
  error: null,
};

function getSheetsClient() {
  const json = Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8");
  const creds = JSON.parse(json);
  const auth = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  );
  return google.sheets({ version: "v4", auth });
}

async function readRange(sheets, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GSHEET_ID,
    range,
  });
  return res.data.values || [];
}

async function loadSSOT(force = false) {
  // Do NOT crash voicebot if SSOT env is missing; fallback to baseline behavior
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) {
    ssotCache = {
      ...ssotCache,
      ok: false,
      error: "SSOT env missing (GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64)",
      loadedAt: Date.now(),
    };
    return ssotCache;
  }

  const now = Date.now();
  if (!force && ssotCache.ok && now - ssotCache.loadedAt < SSOT_CACHE_TTL_MS) return ssotCache;

  try {
    const sheets = getSheetsClient();

    const settingsRows = await readRange(sheets, "SETTINGS!A:B");
    const settings = {};
    settingsRows.slice(1).forEach(([key, value]) => {
      if (key) settings[String(key).trim()] = (value || "").toString().trim();
    });

    const promptsRows = await readRange(sheets, "PROMPTS!A:B");
    const prompts = {};
    promptsRows.slice(1).forEach(([id, content]) => {
      if (id) prompts[String(id).trim()] = (content || "").toString().trim();
    });

    const intentsRows = await readRange(sheets, "INTENTS!A:E");
    const intents = intentsRows.slice(1).map((row) => ({
      intent: row[0],
      priority: Number(row[1] || 0),
      trigger_type: row[2],
      triggers_he: row[3],
      notes: row[4],
    }));

    const intentSuggestionsRows = await readRange(sheets, "INTENT_SUGGESTIONS!A:F");
    const intentSuggestions = intentSuggestionsRows.slice(1).map((row) => ({
      phrase_he: row[0],
      detected_intent: row[1],
      occurrences: row[2],
      last_seen_at: row[3],
      approved: row[4],
      notes: row[5],
    }));

    ssotCache = {
      loadedAt: now,
      settings,
      prompts,
      intents,
      intentSuggestions,
      ok: true,
      error: null,
    };

    console.log("[SSOT] loaded", {
      settings_count: Object.keys(settings).length,
      prompts_count: Object.keys(prompts).length,
      intents_count: intents.length,
    });

    return ssotCache;
  } catch (e) {
    ssotCache = {
      ...ssotCache,
      ok: false,
      error: e && (e.message || String(e)),
      loadedAt: Date.now(),
    };
    console.error("[SSOT] load failed", ssotCache.error);
    return ssotCache;
  }
}

function buildInstructionsFromSSOT(ssot) {
  // Baseline fallback (exactly as your tested code)
  const fallback = [
    "את בטי, בוטית קולית נשית, אנרגטית, שמחה ושנונה.",
    "מדברת עברית טבעית עם סלנג עדין, בלי אימוג׳ים ובלי סימנים מוקראים.",
    "כל תגובה קצרה וממוקדת, ובכל תגובה שאלה אחת בלבד.",
    "אם הלקוח מתחיל באנגלית או מבקש לעבור לאנגלית, עברי לאנגלית.",
    "אל תמציאי מידע משרדי; תמסרי מידע רק אם נשאלת עליו במפורש ובהמשך נמשוך אותו מה-SETTINGS.",
  ].join(" ");

  if (!ssot || !ssot.ok) return fallback;

  const { settings, prompts } = ssot;
  const merged = [prompts.MASTER_PROMPT, prompts.GUARDRAILS_PROMPT, prompts.KB_PROMPT]
    .filter(Boolean)
    .join(" ")
    .trim();

  if (!merged) return fallback;

  return merged
    .replace(/{BUSINESS_NAME}/g, settings.BUSINESS_NAME || "")
    .replace(/{BOT_NAME}/g, settings.BOT_NAME || "");
}

function buildOpeningFromSSOT(ssot) {
  const g = getGreetingBucketAndText();
  const fallback = `${g.text}, אני בטי הבוטית, העוזרת של מרגריטה. איך אפשר לעזור?`;

  if (!ssot || !ssot.ok) return fallback;
  const { settings } = ssot;
  const tpl = (settings.OPENING_SCRIPT || "").trim();
  if (!tpl) return fallback;

  return tpl.replace("{GREETING}", g.text);
}

const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  const ssot = await loadSSOT();
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
    stt_enabled: !!MB_TRANSCRIPTION_MODEL,
    model: OPENAI_REALTIME_MODEL,
    voice: OPENAI_VOICE,
    ssot: {
      ok: ssot.ok,
      loaded_at: ssot.loadedAt,
      error: ssot.error,
      settings_count: ssot.ok ? Object.keys(ssot.settings).length : 0,
      prompts_count: ssot.ok ? Object.keys(ssot.prompts).length : 0,
      intents_count: ssot.ok ? ssot.intents.length : 0,
    },
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", async (twilioWs) => {
  console.log("[WS] connection established");

  const ssot = await loadSSOT();

  let streamSid = null;
  let callSid = null;

  // OpenAI session readiness gates
  let openaiReady = false; // WS open
  let sessionConfigured = false; // after we send session.update
  let pendingCreate = false; // if we got speech_stopped before ready
  let responseInFlight = false; // prevent duplicate response.create
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;
  let lastUserAudioAt = 0;

  // Queue Twilio audio until OpenAI WS is ready
  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400; // safety cap

  // Buffer outbound audio until we know streamSid
  const outAudioQueue = [];
  const MAX_OUT_QUEUE_FRAMES = 400;

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
      lastUserAudioAt = Date.now();
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }
  }

  function flushOutAudioToTwilio() {
    if (!streamSid) return;
    while (outAudioQueue.length) {
      const payload = outAudioQueue.shift();
      try {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload },
          })
        );
      } catch (e) {
        console.error("[TWILIO] send media failed (flush)", e && (e.message || e));
        return;
      }
    }
  }

  function maybeCreateResponse(openaiWs, reason) {
    const now = Date.now();
    if (responseInFlight) return;

    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
      pendingCreate = true;
      return;
    }

    const DEBOUNCE_MS = 1200;
    const MIN_FRAMES = 8;

    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    safeSend(openaiWs, { type: "response.create" });
    pendingCreate = false;
    responseInFlight = true;
    console.log("[TURN] response.create", reason || "speech_stopped");
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

  openaiWs.on("open", () => {
    console.log("[OPENAI] ws open");
    openaiReady = true;

    const instructions = buildInstructionsFromSSOT(ssot);

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

    const opening = buildOpeningFromSSOT(ssot);

    safeSend(openaiWs, {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: opening }],
      },
    });

    // mark in-flight for initial response to avoid duplicate response.create
    lastResponseCreateAt = Date.now();
    userFramesSinceLastCreate = 0;
    safeSend(openaiWs, { type: "response.create" });
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

    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    if (msg.type === "response.audio.done" || msg.type === "response.completed") {
      responseInFlight = false;
      return;
    }

    if (msg.type === "error") {
      console.error("[OPENAI] error event", msg);
      responseInFlight = false;
      return;
    }

    if (msg.type === "response.audio.delta") {
      const payload = msg.delta;
      if (!payload) return;

      // If streamSid isn't known yet, buffer (do NOT drop)
      if (!streamSid) {
        outAudioQueue.push(payload);
        if (outAudioQueue.length > MAX_OUT_QUEUE_FRAMES) {
          outAudioQueue.splice(0, outAudioQueue.length - MAX_OUT_QUEUE_FRAMES);
        }
        return;
      }

      try {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload },
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
    console.log("[OPENAI] ws closed", { code, reason: String(reason || "") });
  });

  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Always log Twilio event types (minimal)
    if (msg.event) {
      if (msg.event !== "media") {
        console.log("[TWILIO] event", msg.event);
      }
    }

    // IMPORTANT: Derive streamSid even if "start" doesn't arrive
    if (!streamSid && msg.streamSid) {
      streamSid = msg.streamSid;
      console.log("[WS] streamSid derived from msg.streamSid", streamSid);
      flushOutAudioToTwilio();
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || streamSid || null;
      callSid = msg.start?.callSid || null;
      console.log("[WS] start", {
        accountSid: msg.start?.accountSid,
        streamSid,
        callSid,
        tracks: msg.start?.tracks,
        mediaFormat: msg.start?.mediaFormat,
        customParameters: msg.start?.customParameters || {},
      });

      flushOutAudioToTwilio();
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;

      // Derive streamSid from media if needed
      if (!streamSid && msg.streamSid) {
        streamSid = msg.streamSid;
        console.log("[WS] streamSid derived from media", streamSid);
        flushOutAudioToTwilio();
      }

      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
        audioQueue.push(payload);
        if (audioQueue.length > MAX_QUEUE_FRAMES) {
          audioQueue.splice(0, audioQueue.length - MAX_QUEUE_FRAMES);
        }
        return;
      }

      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
      // Derive streamSid from stop if needed
      if (!streamSid && msg.streamSid) {
        streamSid = msg.streamSid;
        console.log("[WS] streamSid derived from stop", streamSid);
        flushOutAudioToTwilio();
      }

      console.log("[WS] stop", { callSid: msg.stop?.callSid || callSid });
      try {
        if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_stop");
      } catch {}
      return;
    }
  });

  twilioWs.on("close", () => {
    console.log("[WS] closed");
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
