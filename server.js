// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// Twilio Media Streams <-> OpenAI Realtime
// No Sheets, No Leads, No Prompts – Voice loop only

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

if (!OPENAI_API_KEY) {
  console.error("Missing OPENAI_API_KEY");
  process.exit(1);
}

const app = express();
app.use(express.json());

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: new Date().toISOString(),
    provider_mode: "openai",
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs) => {
  console.log("[WS] connection established");

  let streamSid = null;
  let callSid = null;

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
    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["audio"],
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          voice: "alloy",
        },
      })
    );
  });

  openaiWs.on("message", (msg) => {
    const data = JSON.parse(msg.toString());

    if (data.type === "response.audio.delta" && streamSid) {
      twilioWs.send(
        JSON.stringify({
          event: "media",
          streamSid,
          media: { payload: data.delta },
        })
      );
    }
  });

  twilioWs.on("message", (msg) => {
    const data = JSON.parse(msg.toString());

    if (data.event === "start") {
      streamSid = data.start.streamSid;
      callSid = data.start.callSid;
      console.log("[WS] start", { streamSid, callSid });
    }

    if (data.event === "media") {
      if (openaiWs.readyState === WebSocket.OPEN) {
        openaiWs.send(
          JSON.stringify({
            type: "input_audio_buffer.append",
            audio: data.media.payload,
          })
        );
      }
    }

    if (data.event === "stop") {
      console.log("[WS] stop", { callSid });
      try {
        openaiWs.close();
      } catch {}
    }
  });

  twilioWs.on("close", () => {
    console.log("[WS] closed");
    try {
      openaiWs.close();
    } catch {}
  });
});

server.listen(PORT, () => {
  console.log(`==> Service live on port ${PORT}`);
});
