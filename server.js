// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// Twilio Media Streams <-> OpenAI Realtime
// Stage: Dynamic greeting + minimal instructions + immediate opening speech
// No Sheets, No Leads, No Webhooks (yet)

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

function envNumber(name, def) {
  const raw = process.env[name];
  if (!raw) return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}

function safeStr(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function getTimeParts(timeZone) {
  try {
    const now = new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
    }).formatToParts(now);
    const hour = Number(parts.find((p) => p.type === "hour")?.value || "0");
    const minute = Number(parts.find((p) => p.type === "minute")?.value || "0");
    return { hour, minute };
  } catch (_) {
    return { hour: new Date().getHours(), minute: new Date().getMinutes() };
  }
}

function getGreetingBucket(timeZone) {
  const { hour } = getTimeParts(timeZone);
  if (hour >= 5 && hour < 12) return "morning";
  if (hour >= 12 && hour < 17) return "afternoon";
  if (hour >= 17 && hour < 22) return "evening";
  return "night";
}

function getGreetingHe(bucket) {
  if (bucket === "morning") return "בוקר טוב";
  if (bucket === "afternoon") return "צהריים טובים";
  if (bucket === "evening") return "ערב טוב";
  return "לילה טוב";
}

function nowIso() {
  return new Date().toISOString();
}

// -----------------------------
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 10000);

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

// VAD / turn settings (safe defaults, can be ENV later)
const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 200);

// -----------------------------
// Express
// -----------------------------
const app = express();
app.use(express.json());

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
  });
});

app.get("/", (req, res) => res.status(200).send("OK"));

// -----------------------------
// HTTP + WS Server
// -----------------------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// -----------------------------
// OpenAI helpers
// -----------------------------
function sendSessionUpdate(openAiWs, { instructions, voice, vad }) {
  if (openAiWs.readyState !== WebSocket.OPEN) return;

  openAiWs.send(
    JSON.stringify({
      type: "session.update",
      session: {
        model: OPENAI_REALTIME_MODEL,
        modalities: ["audio", "text"],
        voice: voice || OPENAI_VOICE,
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        turn_detection: {
          type: "server_vad",
          threshold: vad.threshold,
          silence_duration_ms: vad.silence_duration_ms,
          prefix_padding_ms: vad.prefix_padding_ms,
        },
        max_response_output_tokens: "inf",
        instructions: instructions || "",
      },
    })
  );
}

function sendBotSpeak(openAiWs, text) {
  if (openAiWs.readyState !== WebSocket.OPEN) return;

  // Create a user message (instruction to speak) then trigger response.
  openAiWs.send(
    JSON.stringify({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: safeStr(text) }],
      },
    })
  );
  openAiWs.send(JSON.stringify({ type: "response.create" }));
}

// -----------------------------
// Twilio <-> OpenAI bridge
// -----------------------------
wss.on("connection", (twilioWs) => {
  console.log("[WS] connection established");

  if (!OPENAI_API_KEY) {
    console.error("[FATAL] Missing OPENAI_API_KEY");
    try {
      twilioWs.close();
    } catch (_) {}
    return;
  }

  let streamSid = null;
  let callSid = null;

  // OpenAI Realtime WS
  const openAiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  let openAiReady = false;

  openAiWs.on("open", () => {
    openAiReady = true;

    const bucket = getGreetingBucket(TIME_ZONE);
    const greetingHe = getGreetingHe(bucket);

    // Minimal, locked behavior: short, energetic, one question only.
    // Name is NOT forced as the only first question anymore; we allow small talk,
    // then one question to understand intent, and gate name later (in future stages).
    const instructions = [
      'את בטי, בוטית קולית נשית עם קול אנושי, ברור ונעים, העוזרת הווירטואלית של מרגריטה ממשרד אינדקס חשבונאות וייעוץ מס.',
      'דברי בעברית טבעית וקלילה עם סלנג עדין, בטון שירותי, אנרגטי, שמח ושנון, בלי אימוג׳ים ובלי סימנים שמוקראים.',
      'כל תגובה קצרה וממוקדת, ובכל תגובה מותרת שאלה אחת בלבד.',
      'אם הלקוח פותח בסמול־טוק או מתחכם, תעני קצר ואז תחזרי לשאלה אחת שמקדמת את השיחה.',
      'אל תמציאי פרטים; אם אין לך מידע ודאי, תגידי בקצרה שאין לך מידע ותמשיכי לשאלה שמקדמת את השיחה.',
      'אם הלקוח מבקש לעבור לאנגלית או מתחיל לדבר באנגלית, תעני באנגלית פשוטה וברורה.',
    ].join(" ");

    const vad = {
      threshold: MB_VAD_THRESHOLD,
      silence_duration_ms: MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS,
      prefix_padding_ms: MB_VAD_PREFIX_MS,
    };

    sendSessionUpdate(openAiWs, { instructions, vad, voice: OPENAI_VOICE });

    // Immediate opening speech (dynamic greeting).
    // One question only.
    const opening = `${greetingHe}, אני בטי הבוטית, העוזרת של מרגריטה. איך אפשר לעזור?`;
    sendBotSpeak(openAiWs, opening);
  });

  openAiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString("utf8"));
    } catch (_) {
      return;
    }

    if (msg.type === "response.audio.delta") {
      const b64 = msg.delta;
      if (!b64 || !streamSid) return;
      if (twilioWs.readyState !== WebSocket.OPEN) return;

      twilioWs.send(
        JSON.stringify({
          event: "media",
          streamSid,
          media: { payload: b64 },
        })
      );
      return;
    }

    if (msg.type === "error") {
      console.error("[OPENAI][error]", msg);
      return;
    }
  });

  openAiWs.on("close", () => {
    if (twilioWs.readyState === WebSocket.OPEN) {
      try {
        twilioWs.close();
      } catch (_) {}
    }
  });

  openAiWs.on("error", (err) => {
    console.error("[OPENAI][ws_error]", err && (err.message || err));
    if (twilioWs.readyState === WebSocket.OPEN) {
      try {
        twilioWs.close();
      } catch (_) {}
    }
  });

  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString("utf8"));
    } catch (_) {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;
      console.log("[WS] start", msg.start || {});
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;
      if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

      openAiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
      return;
    }

    if (msg.event === "stop") {
      console.log("[WS] stop", { callSid: callSid || msg.stop?.callSid || null });
      try {
        if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close(1000, "twilio_stop");
      } catch (_) {}
      return;
    }
  });

  twilioWs.on("close", (code, reason) => {
    console.log("[WS] closed", { code, reason: safeStr(reason) });
    try {
      if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close(1000, "twilio_closed");
    } catch (_) {}
  });

  twilioWs.on("error", (err) => {
    console.error("[WS][error]", err && (err.message || err));
    try {
      if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close(1000, "twilio_error");
    } catch (_) {}
  });
});

// -----------------------------
// Start
// -----------------------------
server.listen(PORT, () => {
  console.log("==> Your service is live");
  console.log("==> Available at your primary URL", process.env.RENDER_EXTERNAL_URL || "");
  console.log("[BOOT]", {
    port: PORT,
    provider_mode: "openai",
    time_zone: TIME_ZONE,
    model: OPENAI_REALTIME_MODEL,
    voice: OPENAI_VOICE,
  });
});
