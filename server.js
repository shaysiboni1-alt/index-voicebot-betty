// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline)
// Stage: SSOT Runtime Integration (Google Sheets)
// Includes: SETTINGS + PROMPTS + INTENTS loading with cache, dynamic opening & instructions
// Excludes (by design): Gemini, Hybrid, Webhooks FINAL/ABANDONED, Recording, Lead Gate

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const PORT = process.env.PORT || 10000;

// ===== ENV =====
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 =
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim();
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

// ===== Guards =====
if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}
if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) {
  console.error("[FATAL] Missing GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64");
  process.exit(1);
}

// ===== Utils =====
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
const SSOT_CACHE_TTL_MS = 60 * 1000; // 1 minute
let ssotCache = {
  loadedAt: 0,
  settings: {},
  prompts: {},
  intents: [],
  intentSuggestions: [],
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

async function loadSSOT(force = false) {
  const now = Date.now();
  if (!force && now - ssotCache.loadedAt < SSOT_CACHE_TTL_MS) {
    return ssotCache;
  }

  const sheets = getSheetsClient();

  async function readRange(range) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: GSHEET_ID,
      range,
    });
    return res.data.values || [];
  }

  // SETTINGS
  const settingsRows = await readRange("SETTINGS!A:B");
  const settings = {};
  settingsRows.slice(1).forEach(([key, value]) => {
    if (key) settings[key.trim()] = (value || "").toString().trim();
  });

  // PROMPTS
  const promptsRows = await readRange("PROMPTS!A:B");
  const prompts = {};
  promptsRows.slice(1).forEach(([id, content]) => {
    if (id) prompts[id.trim()] = (content || "").toString().trim();
  });

  // INTENTS
  const intentsRows = await readRange("INTENTS!A:E");
  const intents = intentsRows.slice(1).map((row) => ({
    intent: row[0],
    priority: Number(row[1] || 0),
    trigger_type: row[2],
    triggers_he: row[3],
    notes: row[4],
  }));

  // INTENT_SUGGESTIONS (read only for now)
  const intentSuggestionsRows = await readRange("INTENT_SUGGESTIONS!A:F");
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
  };

  console.log("[SSOT] loaded", {
    settings_count: Object.keys(settings).length,
    prompts_count: Object.keys(prompts).length,
    intents_count: intents.length,
  });

  return ssotCache;
}

// ===== Express =====
const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  await loadSSOT();
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
    model: OPENAI_REALTIME_MODEL,
    ssot_loaded_at: ssotCache.loadedAt,
  });
});

// ===== WebSocket Server =====
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", async (twilioWs) => {
  console.log("[WS] connection established");

  await loadSSOT();

  let streamSid = null;
  let callSid = null;

  let openaiReady = false;
  let sessionConfigured = false;
  let responseInFlight = false;

  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(obj));
      return true;
    } catch {
      return false;
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

  openaiWs.on("open", () => {
    openaiReady = true;

    const { settings, prompts } = ssotCache;

    const systemInstructions = [
      prompts.MASTER_PROMPT || "",
      prompts.GUARDRAILS_PROMPT || "",
      prompts.KB_PROMPT || "",
    ]
      .join(" ")
      .replace(/{BUSINESS_NAME}/g, settings.BUSINESS_NAME || "")
      .replace(/{BOT_NAME}/g, settings.BOT_NAME || "");

    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      instructions: systemInstructions,
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

    const g = getGreetingBucketAndText();
    const openingTemplate = settings.OPENING_SCRIPT || "{GREETING}";
    const opening = openingTemplate.replace("{GREETING}", g.text);

    safeSend(openaiWs, {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: opening }],
      },
    });
    safeSend(openaiWs, { type: "response.create" });

    while (audioQueue.length) {
      safeSend(openaiWs, {
        type: "input_audio_buffer.append",
        audio: audioQueue.shift(),
      });
    }
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "response.audio.delta" && streamSid) {
      twilioWs.send(
        JSON.stringify({
          event: "media",
          streamSid,
          media: { payload: msg.delta },
        })
      );
    }

    if (msg.type === "response.completed") {
      responseInFlight = false;
    }
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
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;

      if (!openaiReady || !sessionConfigured) {
        audioQueue.push(payload);
        if (audioQueue.length > MAX_QUEUE_FRAMES) {
          audioQueue.splice(0, audioQueue.length - MAX_QUEUE_FRAMES);
        }
        return;
      }
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }

    if (msg.event === "stop") {
      if (openaiWs.readyState === WebSocket.OPEN) {
        openaiWs.close(1000, "twilio_stop");
      }
    }
  });
});

server.listen(PORT, () => {
  const base = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: ${base}/health`);
});
