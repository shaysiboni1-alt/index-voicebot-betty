// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline)
// Stage: SSOT Runtime Integration (Google Sheets) + Fix audio output race + Turn-taking
// Fixes:
// 1) Buffer outbound audio until Twilio streamSid exists (prevents "no audio at all").
// 2) Re-add server_vad speech_stopped -> response.create with debounce/min-audio gating.
// 3) Add basic OpenAI event logging for easier debugging.
//
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
const OPENAI_VOICE = (process.env.OPENAI_VOICE || "alloy").trim();

const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 =
  (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

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
  if (!force && now - ssotCache.loadedAt < SSOT_CACHE_TTL_MS) return ssotCache;

  const sheets = getSheetsClient();

  async function readRange(range) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: GSHEET_ID,
      range,
    });
    return res.data.values || [];
  }

  // SETTINGS (A:B)
  const settingsRows = await readRange("SETTINGS!A:B");
  const settings = {};
  settingsRows.slice(1).forEach(([key, value]) => {
    if (key) settings[key.trim()] = (value || "").toString().trim();
  });

  // PROMPTS (A:B)
  const promptsRows = await readRange("PROMPTS!A:B");
  const prompts = {};
  promptsRows.slice(1).forEach(([id, content]) => {
    if (id) prompts[id.trim()] = (content || "").toString().trim();
  });

  // INTENTS (A:E)
  const intentsRows = await readRange("INTENTS!A:E");
  const intents = intentsRows.slice(1).map((row) => ({
    intent: row[0],
    priority: Number(row[1] || 0),
    trigger_type: row[2],
    triggers_he: row[3],
    notes: row[4],
  }));

  // INTENT_SUGGESTIONS (A:F) read-only for now
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
    voice: OPENAI_VOICE,
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

  // OpenAI readiness gates
  let openaiReady = false;
  let sessionConfigured = false;

  // Turn-taking
  let pendingCreate = false;
  let responseInFlight = false;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  // Buffer inbound audio until OpenAI ready
  const inAudioQueue = [];
  const MAX_IN_QUEUE_FRAMES = 400;

  // Buffer outbound audio until Twilio streamSid exists (FIX for "no audio at all")
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

  function flushInAudio(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (!openaiReady || !sessionConfigured) return;
    while (inAudioQueue.length) {
      const payload = inAudioQueue.shift();
      userFramesSinceLastCreate += 1;
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

    // Debounce + minimum-audio gate
    const DEBOUNCE_MS = 1200;
    const MIN_FRAMES = 8; // ~160ms

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

    // Correct Realtime STT field name:
    if (MB_TRANSCRIPTION_MODEL) {
      session.input_audio_transcription = {
        model: MB_TRANSCRIPTION_MODEL,
        language: MB_TRANSCRIPTION_LANGUAGE,
      };
    }

    safeSend(openaiWs, { type: "session.update", session });
    sessionConfigured = true;

    // Opening (from SSOT)
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

    // Force first response (greeting)
    safeSend(openaiWs, { type: "response.create" });
    responseInFlight = true;

    flushInAudio(openaiWs);

    if (pendingCreate) {
      // If user already spoke before OpenAI ready, try to answer
      maybeCreateResponse(openaiWs, "pending_after_open");
    }
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Useful debug for silent failures (do not spam deltas)
    if (
      msg.type &&
      msg.type !== "response.audio.delta" &&
      msg.type !== "input_audio_buffer.append" &&
      msg.type !== "input_audio_buffer.committed"
    ) {
      if (msg.type === "error") {
        console.error("[OPENAI] error event", msg);
      } else if (msg.type === "session.updated") {
        console.log("[OPENAI] session.updated");
      } else if (msg.type === "response.created") {
        // quiet
      } else {
        // keep it light
        // console.log("[OPENAI] event", msg.type);
      }
    }

    // Turn boundary (server_vad)
    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    // Audio out
    if (msg.type === "response.audio.delta") {
      const payload = msg.delta;
      if (!payload) return;

      // If Twilio streamSid not ready yet, buffer outbound audio
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
      return;
    }

    // Response completed gates next turn
    if (msg.type === "response.audio.done" || msg.type === "response.completed") {
      responseInFlight = false;
      return;
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

  // ===== Twilio WS inbound =====
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

      console.log("[WS] start", {
        streamSid,
        callSid,
        mediaFormat: msg.start?.mediaFormat,
        customParameters: msg.start?.customParameters || {},
      });

      // Flush any audio that was produced before streamSid was known
      flushOutAudioToTwilio();
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;

      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
        inAudioQueue.push(payload);
        if (inAudioQueue.length > MAX_IN_QUEUE_FRAMES) {
          inAudioQueue.splice(0, inAudioQueue.length - MAX_IN_QUEUE_FRAMES);
        }
        return;
      }

      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
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
