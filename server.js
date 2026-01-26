
const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

// ---- In-memory call state ----
const calls = new Map();

function nowISO() {
  return new Date().toISOString();
}

// ---- Health ----
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowISO(),
    call_log_webhook_configured: !!process.env.CALL_LOG_WEBHOOK_URL,
    provider_mode: process.env.PROVIDER_MODE || null,
  });
});

// ---- HTTP server ----
const server = http.createServer(app);

// ---- WebSocket server (Twilio Media Streams) ----
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (ws) => {
  console.log("[WS] connection established");

  ws.on("message", async (msg) => {
    let data;
    try {
      data = JSON.parse(msg.toString());
    } catch {
      return;
    }

    if (data.event === "start") {
      const { callSid, streamSid, customParameters } = data.start || {};
      calls.set(callSid, {
        callSid,
        streamSid,
        started_at: nowISO(),
        media_frames: 0,
        customParameters: customParameters || {},
      });
      console.log("[WS] start", data.start);
    }

    if (data.event === "media") {
      const callSid = data.streamSid && [...calls.values()].find(c => c.streamSid === data.streamSid)?.callSid;
      if (callSid && calls.has(callSid)) {
        calls.get(callSid).media_frames += 1;
      }
    }

    if (data.event === "stop") {
      const { callSid } = data.stop || {};
      const state = calls.get(callSid);
      console.log("[WS] stop", data.stop);

      if (state && process.env.CALL_LOG_WEBHOOK_URL) {
        try {
          console.log("[CALL_LOG] sending...", { callSid });
          await fetch(process.env.CALL_LOG_WEBHOOK_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              event: "CALL_LOG",
              callSid,
              started_at: state.started_at,
              ended_at: nowISO(),
              media_frames: state.media_frames,
              provider_mode: process.env.PROVIDER_MODE || null,
              customParameters: state.customParameters || {},
            }),
          });
          console.log("[CALL_LOG] delivered");
        } catch (e) {
          console.log("[CALL_LOG] failed", e && (e.message || e));
        }
      }

      calls.delete(callSid);
    }
  });

  ws.on("close", (code, reason) => {
    console.log("[WS] closed", { code, reason: reason && reason.toString() });
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
