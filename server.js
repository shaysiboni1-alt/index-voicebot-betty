/**
 * Index Betty VoiceBot – Server Skeleton
 * Stage 1: Infrastructure only (NO AI, NO intents, NO lead logic)
 *
 * Includes:
 * - GET /health
 * - POST /twilio-voice (optional TwiML entry)
 * - WS /twilio-media-stream (Twilio Media Streams)
 * - Call lifecycle handling (start / stop)
 * - CALL_LOG webhook on call end
 */

import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import fetch from "node-fetch";
import bodyParser from "body-parser";

const PORT = process.env.PORT || 10000;
const CALL_LOG_WEBHOOK = process.env.CALL_LOG_WEBHOOK_URL || null;

const app = express();
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

/* =========================
   Health
========================= */
app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: new Date().toISOString(),
  });
});

/* =========================
   Optional Twilio Voice Entry
========================= */
app.post("/twilio-voice", (req, res) => {
  const wsUrl =
    process.env.TWILIO_STREAM_WS_URL ||
    "wss://index-voicebot-betty.onrender.com/twilio-media-stream";

  const twiml = `
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
      <Parameter name="callSid" value="${req.body.CallSid || ""}" />
      <Parameter name="source" value="Index Betty Voice AI" />
    </Stream>
  </Connect>
</Response>`.trim();

  res.type("text/xml").send(twiml);
});

/* =========================
   HTTP + WS Server
========================= */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/twilio-media-stream" });

/**
 * In-memory call sessions
 * callSid => { startedAt, caller, called }
 */
const calls = new Map();

wss.on("connection", (ws) => {
  console.log("[WS] connection established");

  ws.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      const { callSid, customParameters } = msg.start;

      calls.set(callSid, {
        callSid,
        startedAt: Date.now(),
        caller: customParameters?.caller || null,
        called: customParameters?.called || null,
        source: customParameters?.source || null,
      });

      console.log("[WS] start", msg.start);
    }

    if (msg.event === "stop") {
      const callSid = msg.stop.callSid;
      const session = calls.get(callSid);

      console.log("[WS] stop", msg.stop);

      if (session) {
        const endedAt = Date.now();
        const durationSec = Math.max(
          0,
          Math.round((endedAt - session.startedAt) / 1000)
        );

        if (CALL_LOG_WEBHOOK) {
          try {
            await fetch(CALL_LOG_WEBHOOK, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                event: "CALL_LOG",
                callSid,
                caller: session.caller,
                called: session.called,
                started_at: new Date(session.startedAt).toISOString(),
                ended_at: new Date(endedAt).toISOString(),
                duration_sec: durationSec,
                source: session.source,
              }),
            });
          } catch (e) {
            console.log("Failed sending CALL_LOG:", e?.message || e);
          }
        }

        calls.delete(callSid);
      }
    }
  });

  ws.on("close", (code, reason) => {
    console.log("[WS] closed", { code, reason: reason?.toString() });
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
