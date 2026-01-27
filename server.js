// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline + SSOT)
// Baseline preserved. Added: SSOT loader + prompt injection + faster response create.

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const PORT = process.env.PORT || 10000;

/* ================== ENV ================== */
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim();
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

/* ================== UTILS ================== */
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

/* ================== SSOT LOADER ================== */
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

async function loadSSOT() {
  if (!ssot.enabled) return ssot;

  const now = Date.now();
  if (now < ssot._expires && ssot.loaded_at) return ssot;

  try {
    const creds = JSON.parse(
      Buffer.from(GOOGLE_SA_B64, "base64").toString("utf8")
    );

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

    // SETTINGS
    const settingsRows = await read("SETTINGS!A:B");
    const settings = {};
    settingsRows.slice(1).forEach(([k, v]) => {
      if (k) settings[k] = v ?? "";
    });

    // PROMPTS
    const promptRows = await read("PROMPTS!A:B");
    const prompts = {};
    promptRows.slice(1).forEach(([id, content]) => {
      if (id) prompts[id] = content ?? "";
    });

    // INTENTS
    const intentRows = await read("INTENTS!A:D");
    const intents = intentRows.slice(1).map((r) => ({
      intent: r[0],
      priority: Number(r[1] || 0),
      trigger_type: r[2],
      triggers_he: r[3],
    }));

    // INTENT_SUGGESTIONS
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
    ssot._expires = now + SSOT_TTL_MS;

    console.log("[SSOT] loaded", {
      settings_keys: Object.keys(settings).length,
      prompts_keys: Object.keys(prompts).length,
      intents: intents.length,
      intent_suggestions: intent_suggestions.length,
    });
  } catch (e) {
    ssot.error = e.message || String(e);
    console.error("[SSOT] load failed", ssot.error);
  }

  return ssot;
}

function injectVars(text, vars) {
  let out = text;
  Object.entries(vars).forEach(([k, v]) => {
    out = out.replaceAll(`{${k}}`, v ?? "");
  });
  return out;
}

/* ================== APP ================== */
const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  await loadSSOT();
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
    model: OPENAI_REALTIME_MODEL,
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/* ================== WS ================== */
wss.on("connection", async (twilioWs) => {
  console.log("[WS] connection established");
  await loadSSOT();

  let streamSid = null;
  let callSid = null;

  let openaiReady = false;
  let sessionConfigured = false;
  let responseInFlight = false;

  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify(obj));
    return true;
  }

  function maybeCreateResponse(openaiWs) {
    if (responseInFlight) return;
    const now = Date.now();
    if (now - lastResponseCreateAt < 300) return; // latency reduced
    if (userFramesSinceLastCreate < 4) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;
    responseInFlight = true;
    safeSend(openaiWs, { type: "response.create" });
  }

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(
      OPENAI_REALTIME_MODEL
    )}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openaiWs.on("open", () => {
    openaiReady = true;

    const { settings, prompts } = ssot.data;
    const greeting = getGreetingBucketAndText();

    const openingScriptRaw = settings.OPENING_SCRIPT || "";
    const opening = injectVars(openingScriptRaw, {
      GREETING: greeting.text,
      BOT_NAME: settings.BOT_NAME,
      BUSINESS_NAME: settings.BUSINESS_NAME,
    });

    const instructions = [
      injectVars(prompts.MASTER_PROMPT || "", settings),
      injectVars(prompts.GUARDRAILS_PROMPT || "", settings),
      injectVars(prompts.KB_PROMPT || "", settings),
      injectVars(prompts.LEAD_CAPTURE_PROMPT || "", settings),
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

    safeSend(openaiWs, {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: opening }],
      },
    });
    safeSend(openaiWs, { type: "response.create" });
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse(openaiWs);
      return;
    }

    if (msg.type === "response.completed") {
      responseInFlight = false;
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

      userFramesSinceLastCreate++;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }

    if (msg.event === "stop") {
      if (openaiWs.readyState === WebSocket.OPEN) openaiWs.close();
    }
  });
});

server.listen(PORT, () => {
  const base = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: ${base}/health`);
});
