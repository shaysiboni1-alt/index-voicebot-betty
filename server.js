// server.js
// Minimal WebSocket stub for Twilio Media Streams (no AI, no Sheets, no webhooks yet)

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const app = express();

// Health endpoint
app.get("/health", (req, res) => {
  res.status(200).json({ ok: true, service: "index-betty-voicebot", ts: new Date().toISOString() });
});

// Twilio will connect to this as WebSocket: /twilio-media-stream
const server = http.createServer(app);

const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (ws, req) => {
  const remote = req.socket && req.socket.remoteAddress ? req.socket.remoteAddress : "unknown";
  console.log("[WS] connection established from", remote);

  ws.on("message", (data) => {
    // Twilio sends JSON frames: start / media / mark / stop
    // At this stage we just parse and log minimal info.
    try {
      const msg = JSON.parse(data.toString("utf8"));
      const evt = msg.event || "unknown";

      if (evt === "start") {
        const start = msg.start || {};
        console.log("[WS] start", {
          streamSid: start.streamSid,
          callSid: start.callSid,
          tracks: start.tracks,
          customParameters: start.customParameters || {},
        });
      } else if (evt === "media") {
        // Too chatty to log every frame; keep it silent
      } else if (evt === "stop") {
        console.log("[WS] stop", msg.stop || {});
      } else {
        // mark / dtmf / etc.
        // console.log("[WS] event", evt);
      }
    } catch (e) {
      console.log("[WS] non-json message length=", data ? data.length : 0);
    }
  });

  ws.on("close", (code, reason) => {
    console.log("[WS] closed", { code, reason: reason ? reason.toString() : "" });
  });

  ws.on("error", (err) => {
    console.log("[WS] error", err && (err.message || err));
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
  console.log(`WS: ws://localhost:${PORT}/twilio-media-stream`);
});
