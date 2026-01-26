// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// Stage: Turn handling + STT
// Twilio Media Streams <-> OpenAI Realtime

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || null;
const MB_TRANSCRIPTION_LANGUAGE = process.env.MB_TRANSCRIPTION_LANGUAGE || "he";

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

if (!OPENAI_API_KEY) {
  console.error("Missing OPENAI_API_KEY");
  process.exit(1);
}

function nowIso() {
  return new Date().toISOString();
}

function greetingByTime() {
  const hour = Number(
    new Intl.DateTimeFormat("en-US", {
      timeZone: TIME_ZONE,
      hour12: false,
      hour: "2-digit",
    }).format(new Date())
  );
  if (hour >= 5 && hour < 12) return "בוקר טוב";
  if (hour >= 12 && hour < 17) return "צהריים טובים";
  if (hour >= 17 && hour < 22) return "ערב טוב";
  return "לילה טוב";
}

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

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs) => {
  let streamSid = null;

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openaiWs.on("open", () => {
    const instructions = [
      "את בטי, בוטית קולית נשית, אנרגטית, שמחה ושנונה.",
      "מדברת עברית טבעית עם סלנג עדין, בלי אימוג׳ים ובלי סימנים מוקראים.",
      "כל תגובה קצרה, ובכל תגובה שאלה אחת בלבד.",
      "אם יש סמול־טוק — עני קצר וחזרי לשאלה שמקדמת את השיחה.",
      "אם הלקוח מבקש לעבור לאנגלית — עברי לאנגלית.",
    ].join(" ");

    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
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
          transcription:
            MB_TRANSCRIPTION_MODEL
              ? {
                  model: MB_TRANSCRIPTION_MODEL,
                  language: MB_TRANSCRIPTION_LANGUAGE,
                }
              : undefined,
        },
      })
    );

    const opening = `${greetingByTime()}, אני בטי הבוטית, העוזרת של מרגריטה. איך אפשר לעזור?`;

    openaiWs.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: opening }],
        },
      })
    );
    openaiWs.send(JSON.stringify({ type: "response.create" }));
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // כשהלקוח סיים לדבר – הבוט עונה
    if (msg.type === "input_audio_buffer.speech_stopped") {
      openaiWs.send(JSON.stringify({ type: "response.create" }));
      return;
    }

    // Audio חזרה ל-Twilio
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
      streamSid = msg.start.streamSid;
      return;
    }

    if (msg.event === "media") {
      openaiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        })
      );
    }

    if (msg.event === "stop") {
      openaiWs.close();
    }
  });

  twilioWs.on("close", () => {
    openaiWs.close();
  });
});

server.listen(PORT, () => {
  console.log("Service live on port", PORT);
});
