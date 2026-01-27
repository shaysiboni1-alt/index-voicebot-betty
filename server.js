// server.js
// Index VoiceBot – Betty (Realtime Voice Bot)
// Stage: SSOT (Google Sheets) READ-ONLY integration
//
// Notes:
// - Keeps existing OpenAI Realtime streaming flow.
// - Adds SSOT loader (SETTINGS/PROMPTS/INTENTS/INTENT_SUGGESTIONS) via Google Service Account (read-only).
// - No writes to Sheets in this stage.

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const PORT = process.env.PORT || 10000;

// ---- ENV (Runtime) ----
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const PROVIDER_MODE = (process.env.PROVIDER_MODE || "openai").trim(); // openai|gemini|hybrid

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

// Optional STT settings for OpenAI Realtime
const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim(); // optional
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

// VAD tuning
const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

// SSOT (Google Sheets)
const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();
const SSOT_REFRESH_MS = Number(process.env.SSOT_REFRESH_MS || 60_000);

// Basic sanity
if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

// ---- Helpers ----
function nowIso() {
  return new Date().toISOString();
}

function safeJsonParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function decodeServiceAccountJson() {
  if (!GOOGLE_SERVICE_ACCOUNT_JSON_B64) return null;
  const raw = Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8");
  const obj = safeJsonParse(raw);
  return obj && obj.client_email && obj.private_key ? obj : null;
}

function getGreetingBucketAndText() {
  let hour = 0;
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: TIME_ZONE,
      hour12: false,
      hour: "2-digit",
    }).formatToParts(new Date());
    const hh = parts.find((p) => p.type === "hour")?.value;
    hour = Number(hh || 0);
  } catch {
    hour = new Date().getHours();
  }

  let bucket = "morning";
  let text = "בוקר טוב";
  if (hour >= 12 && hour < 17) {
    bucket = "afternoon";
    text = "צהריים טובים";
  } else if (hour >= 17 && hour < 22) {
    bucket = "evening";
    text = "ערב טוב";
  } else if (hour >= 22 || hour < 5) {
    bucket = "night";
    text = "לילה טוב";
  }
  return { bucket, text };
}

// ---- SSOT State ----
const ssot = {
  enabled: Boolean(GSHEET_ID && GOOGLE_SERVICE_ACCOUNT_JSON_B64),
  loaded_at: null,
  error: null,
  settings: {},               // key->value
  prompts: {},                // prompt_id->content_he
  intents: [],                // rows
  intent_suggestions: [],     // rows
};

async function ssotAuthClient() {
  const sa = decodeServiceAccountJson();
  if (!sa) throw new Error("Invalid GOOGLE_SERVICE_ACCOUNT_JSON_B64 (expected base64 JSON with client_email/private_key)");
  const scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"];
  const jwt = new google.auth.JWT(sa.client_email, null, sa.private_key, scopes);
  await jwt.authorize();
  return jwt;
}

function rowsToKeyValue(rows) {
  const out = {};
  for (const r of rows || []) {
    const k = (r?.[0] || "").toString().trim();
    const v = (r?.[1] || "").toString().trim();
    if (!k || k.toLowerCase() === "key") continue;
    out[k] = v;
  }
  return out;
}

async function loadSsotOnce() {
  if (!ssot.enabled) return;

  const auth = await ssotAuthClient();
  const sheets = google.sheets({ version: "v4", auth });

  const ranges = [
    "SETTINGS!A:B",
    "PROMPTS!A:B",
    "INTENTS!A:E",
    "INTENT_SUGGESTIONS!A:F",
  ];

  const res = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: GSHEET_ID,
    ranges,
    majorDimension: "ROWS",
  });

  const valueRanges = res.data.valueRanges || [];
  const byRange = {};
  for (const vr of valueRanges) {
    byRange[vr.range] = vr.values || [];
  }

  // SETTINGS
  const settingsRows = byRange["SETTINGS!A:B"] || byRange["SETTINGS!A:B"] || [];
  ssot.settings = rowsToKeyValue(settingsRows);

  // PROMPTS
  const promptsRows = byRange["PROMPTS!A:B"] || [];
  const prompts = {};
  for (const r of promptsRows || []) {
    const id = (r?.[0] || "").toString().trim();
    const content = (r?.[1] || "").toString();
    if (!id || id.toLowerCase() === "prompt_id") continue;
    prompts[id] = content;
  }
  ssot.prompts = prompts;

  // INTENTS
  const intentsRows = byRange["INTENTS!A:E"] || [];
  const intents = [];
  for (const r of intentsRows || []) {
    const intent = (r?.[0] || "").toString().trim();
    if (!intent || intent.toLowerCase() === "intent") continue;
    intents.push({
      intent,
      priority: Number(r?.[1] || 0),
      trigger_type: (r?.[2] || "").toString().trim(),
      triggers_he: (r?.[3] || "").toString().trim(),
      notes: (r?.[4] || "").toString().trim(),
    });
  }
  ssot.intents = intents;

  // INTENT_SUGGESTIONS (read-only for now)
  const sugRows = byRange["INTENT_SUGGESTIONS!A:F"] || [];
  const suggestions = [];
  for (const r of sugRows || []) {
    const phrase_he = (r?.[0] || "").toString().trim();
    if (!phrase_he || phrase_he.toLowerCase() === "phrase_he") continue;
    suggestions.push({
      phrase_he,
      detected_intent: (r?.[1] || "").toString().trim(),
      occurrences: Number(r?.[2] || 0),
      last_seen_at: (r?.[3] || "").toString().trim(),
      approved: (r?.[4] || "").toString().trim(),
      notes: (r?.[5] || "").toString().trim(),
    });
  }
  ssot.intent_suggestions = suggestions;

  ssot.loaded_at = nowIso();
  ssot.error = null;
  console.log("[SSOT] loaded", {
    loaded_at: ssot.loaded_at,
    settings_keys: Object.keys(ssot.settings || {}).length,
    prompts_keys: Object.keys(ssot.prompts || {}).length,
    intents: ssot.intents.length,
    intent_suggestions: ssot.intent_suggestions.length,
  });
}

async function startSsotLoop() {
  if (!ssot.enabled) {
    console.log("[SSOT] disabled (missing GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64)");
    return;
  }
  try {
    await loadSsotOnce();
  } catch (e) {
    ssot.error = e?.message || String(e);
    console.error("[SSOT] load failed:", ssot.error);
  }
  setInterval(async () => {
    try {
      await loadSsotOnce();
    } catch (e) {
      ssot.error = e?.message || String(e);
      console.error("[SSOT] refresh failed:", ssot.error);
    }
  }, SSOT_REFRESH_MS);
}

// ---- Express ----
const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: PROVIDER_MODE,
    ssot: {
      enabled: ssot.enabled,
      loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys: Object.keys(ssot.settings || {}).length,
      prompts_keys: Object.keys(ssot.prompts || {}).length,
      intents: ssot.intents.length,
      intent_suggestions: ssot.intent_suggestions.length,
    },
  });
});

// ---- HTTP + WS (Twilio Media Streams -> OpenAI Realtime) ----
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// OpenAI Realtime WS URL
const OPENAI_REALTIME_URL = "wss://api.openai.com/v1/realtime?model=" + encodeURIComponent(OPENAI_REALTIME_MODEL);

// Turn gating: never call response.create before OpenAI WS is OPEN
function safeSend(ws, obj, label) {
  try {
    if (ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify(obj));
    if (label) console.log("[SEND]", label);
    return true;
  } catch {
    return false;
  }
}

function buildSessionUpdate() {
  // Pull dynamic scripts from SSOT if present (fallbacks are safe)
  const opening = (ssot.settings.OPENING_SCRIPT || "").trim();
  const noData = (ssot.settings.NO_DATA_MESSAGE || "אין לי מידע זמין כרגע.").trim();
  const master = (ssot.prompts.MASTER_PROMPT || "").trim();

  // We will only inject small context placeholders; runtime logic uses full prompts later stages.
  const systemPreamble = [
    master,
    "אם אין לך הוראות אחרות, תפעלי לפי SSOT בלבד.",
    `אם חסר מידע: אמרי בקצרה: "${noData}".`,
    opening ? `פתיח לשימוש: "${opening}"` : "",
  ].filter(Boolean).join(" ");

  const session = {
    type: "session.update",
    session: {
      // voice
      voice: OPENAI_VOICE,
      // turn detection
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        prefix_padding_ms: MB_VAD_PREFIX_MS,
        silence_duration_ms: MB_VAD_SILENCE_MS,
      },
      // modalities
      modalities: ["text", "audio"],
      // language bias via system text
      instructions: systemPreamble,
    },
  };

  // Realtime STT
  if (MB_TRANSCRIPTION_MODEL) {
    session.session.input_audio_transcription = {
      model: MB_TRANSCRIPTION_MODEL,
      language: MB_TRANSCRIPTION_LANGUAGE || "he",
    };
  }

  return session;
}

wss.on("connection", (twilioWs) => {
  console.log("[WS] connection established");

  let callSid = "";
  let streamSid = "";
  let caller = "";
  let called = "";
  let source = "";
  let startedAt = nowIso();

  // OpenAI WS state
  let openaiWs = null;
  let openaiOpen = false;
  const pending = []; // queue of JSON-serializable messages to send after open

  function enqueueOrSend(obj, label) {
    if (openaiWs && openaiOpen) {
      safeSend(openaiWs, obj, label);
    } else {
      pending.push({ obj, label });
    }
  }

  function flushPending() {
    if (!openaiWs || !openaiOpen) return;
    while (pending.length) {
      const { obj, label } = pending.shift();
      safeSend(openaiWs, obj, label);
    }
  }

  function connectOpenAI() {
    openaiWs = new WebSocket(OPENAI_REALTIME_URL, {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    });

    openaiWs.on("open", () => {
      openaiOpen = true;
      console.log("[OPENAI] ws open");
      // 1) session.update
      enqueueOrSend(buildSessionUpdate(), "session.update");
      // 2) Start a first response only after open (queue-safe)
      enqueueOrSend({ type: "response.create" }, "response.create (boot)");
      flushPending();
    });

    openaiWs.on("message", (data) => {
      let msg = null;
      try {
        msg = JSON.parse(data.toString("utf8"));
      } catch {
        return;
      }

      // Audio out -> Twilio
      if (msg.type === "response.audio.delta" && msg.delta) {
        try {
          twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: msg.delta } }));
        } catch {}
      }

      // Basic debug for turn events
      if (msg.type === "response.create" || msg.type?.includes("speech_")) {
        console.log("[TURN]", msg.type);
      }
    });

    openaiWs.on("close", (e) => {
      openaiOpen = false;
      console.log("[OPENAI] ws closed", { code: e?.code, reason: e?.reason || "" });
    });

    openaiWs.on("error", (e) => {
      console.log("[OPENAI] ws error", e?.message || e);
    });
  }

  connectOpenAI();

  twilioWs.on("message", (msg) => {
    let data = null;
    try {
      data = JSON.parse(msg.toString("utf8"));
    } catch {
      return;
    }

    if (data.event === "start") {
      streamSid = data.start?.streamSid || "";
      callSid = data.start?.callSid || data.start?.customParameters?.callSid || "";
      caller = data.start?.customParameters?.caller || "";
      called = data.start?.customParameters?.called || "";
      source = data.start?.customParameters?.source || "";
      startedAt = nowIso();

      console.log("[WS] start", {
        streamSid,
        callSid,
        tracks: data.start?.tracks,
        mediaFormat: data.start?.mediaFormat,
        customParameters: data.start?.customParameters,
      });

      // For future stages: use SSOT OPENING_SCRIPT with greeting bucket. For now, Realtime handles via instructions.
      return;
    }

    if (data.event === "media") {
      // Forward inbound audio to OpenAI
      const payload = data.media?.payload;
      if (!payload) return;

      enqueueOrSend({ type: "input_audio_buffer.append", audio: payload }, "input_audio_buffer.append");
      // Let server_vad decide when to respond (OpenAI handles)
      flushPending();
      return;
    }

    if (data.event === "stop") {
      console.log("[WS] stop", { callSid });
      try {
        if (openaiWs) openaiWs.close(1000, "twilio_stop");
      } catch {}
      try {
        twilioWs.close();
      } catch {}
      return;
    }
  });

  twilioWs.on("close", () => {
    console.log("[WS] closed");
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_ws_closed");
    } catch {}
  });

  twilioWs.on("error", (e) => {
    console.log("[WS] error", e?.message || e);
  });
});

server.listen(PORT, () => {
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: https://index-voicebot-betty.onrender.com/health`);
  startSsotLoop().catch((e) => console.error("[SSOT] loop failed", e?.message || e));
});
