// server.js
// Index Betty – Realtime VoiceBot (OpenAI + SSOT)
// ADDITIONS ONLY:
// 1) STT transcript logging
// 2) Assistant text logging
// 3) Slight latency improvement
// 4) +20% audio volume (safe, optional)

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

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim();
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

// LOGGING FLAGS
const MB_DEBUG = process.env.MB_DEBUG === "true";
const MB_LOG_TRANSCRIPTS = process.env.MB_LOG_TRANSCRIPTS === "true";
const MB_LOG_ASSISTANT_TEXT = process.env.MB_LOG_ASSISTANT_TEXT === "true";

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SA_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

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

/* ================== SSOT (unchanged) ================== */
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

  const settings = {};
  (await read("SETTINGS!A:B")).slice(1).forEach(([k, v]) => {
    if (k) settings[k] = v ?? "";
  });

  const prompts = {};
  (await read("PROMPTS!A:B")).slice(1).forEach(([k, v]) => {
    if (k) prompts[k] = v ?? "";
  });

  const intents = (await read("INTENTS!A:D")).slice(1).map((r) => ({
    intent: r[0],
    priority: Number(r[1] || 0),
    trigger_type: r[2],
    triggers_he: r[3],
  }));

  ssot.data = { settings, prompts, intents, intent_suggestions: [] };
  ssot.loaded_at = nowIso();
  ssot._expires = now + SSOT_TTL_MS;

  if (MB_DEBUG) {
    console.log("[SSOT] loaded", {
      settings: Object.keys(settings).length,
      prompts: Object.keys(prompts).length,
      intents: intents.length,
    });
  }

  return ssot;
}

function injectVars(text, vars) {
  let out = String(text || "");
  Object.entries(vars || {}).forEach(([k, v]) => {
    out = out.replaceAll(`{${k}}`, String(v ?? ""));
  });
  return out;
}

/* ================== APP ================== */
const app = express();
app.get("/health", async (req, res) => {
  await loadSSOT();
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/* ================== WS ================== */
wss.on("connection", async (twilioWs) => {
  await loadSSOT(true);

  let streamSid = null;
  let openaiReady = false;
  let sessionConfigured = false;
  let responseInFlight = false;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  function maybeCreateResponse() {
    const now = Date.now();
    if (responseInFlight) return;
    if (now - lastResponseCreateAt < 250) return; // LATENCY TUNE
    if (userFramesSinceLastCreate < 3) return;    // LATENCY TUNE

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;
    responseInFlight = true;

    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: {
          audio: { volume: 1.2 }, // +20% volume (safe)
        },
      })
    );
  }

  openaiWs.on("open", () => {
    openaiReady = true;

    const { settings, prompts } = ssot.data;
    const greeting = getGreetingBucketAndText();

    const opening = injectVars(settings.OPENING_SCRIPT, {
      GREETING: greeting.text,
      BOT_NAME: settings.BOT_NAME,
      BUSINESS_NAME: settings.BUSINESS_NAME,
    });

    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["audio", "text"],
          voice: OPENAI_VOICE,
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          instructions: [
            injectVars(prompts.MASTER_PROMPT, settings),
            injectVars(prompts.GUARDRAILS_PROMPT, settings),
            injectVars(prompts.KB_PROMPT, settings),
            injectVars(prompts.LEAD_CAPTURE_PROMPT, settings),
          ].join("\n\n"),
          turn_detection: {
            type: "server_vad",
            threshold: MB_VAD_THRESHOLD,
            silence_duration_ms: MB_VAD_SILENCE_MS,
            prefix_padding_ms: MB_VAD_PREFIX_MS,
          },
          ...(MB_TRANSCRIPTION_MODEL && {
            input_audio_transcription: {
              model: MB_TRANSCRIPTION_MODEL,
              language: MB_TRANSCRIPTION_LANGUAGE,
            },
          }),
        },
      })
    );

    sessionConfigured = true;

    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: {
          instructions:
            "אמרי מילה במילה ללא שינוי:\n" + opening,
          audio: { volume: 1.2 },
        },
      })
    );

    responseInFlight = true;
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // LOGGING ADDITION — STT
    if (MB_LOG_TRANSCRIPTS && msg.type === "input_audio_transcription.result") {
      console.log("[STT]", msg.text);
    }

    // LOGGING ADDITION — Assistant text
    if (MB_LOG_ASSISTANT_TEXT && msg.type === "response.output_text.delta") {
      process.stdout.write(msg.delta);
    }

    if (msg.type === "response.completed") {
      responseInFlight = false;
      if (MB_LOG_ASSISTANT_TEXT) process.stdout.write("\n");
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse();
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
      streamSid = msg.start?.streamSid;
      return;
    }

    if (msg.event === "media") {
      userFramesSinceLastCreate++;
      if (openaiReady && sessionConfigured) {
        openaiWs.send(
          JSON.stringify({
            type: "input_audio_buffer.append",
            audio: msg.media.payload,
          })
        );
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`==> Service live on port ${PORT}`);
});
