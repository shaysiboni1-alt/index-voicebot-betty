/**
 * Index Betty VoiceBot – Server Skeleton (CommonJS)
 * Stage 1: Infrastructure only (NO AI, NO intents, NO lead logic)
 *
 * Includes:
 * - GET /health
 * - POST /twilio-voice (optional TwiML entry)
 * - WS /twilio-media-stream (Twilio Media Streams)
 * - Call lifecycle handling (start / stop)
 * - CALL_LOG webhook on call end
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;
const CALL_LOG_WEBHOOK = process.env.CALL_LOG_WEBHOOK_URL || null;

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

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
   Optional Twilio Voice Entry (TwiML)
========================= */
app.post("/twilio-voice", (req, res) => {
  const wsUrl =
    process.env.TWILIO_STREAM_WS_URL ||
    "wss://index-voicebot-betty.onrender.com/twilio-media-stream";

  const from = req.body.From || "";
  const to = req.body.To || "";
  const callSid = req.body.CallSid || "";

  const twiml = `
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${from}" />
      <Parameter name="called" value="${to}" />
      <Parameter name="callSid" value="${callSid}" />
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
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/**
 * In-memory call sessions
 * callSid => { startedAt, caller, called, source }
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

    if (msg.event === "start" && msg.start) {
      const { callSid, customParameters } = msg.start;

      calls.set(callSid, {
        callSid,
        startedAt: Date.now(),
        caller: customParameters?.caller || null,
        called: customParameters?.called || null,
        source: customParameters?.source || null,
      });

      console.log("[WS] start", msg.start);
      return;
    }

    if (msg.event === "stop" && msg.stop) {
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
            console.log("Failed sending CALL_LOG:", e && (e.message || e));
          }
        }

        calls.delete(callSid);
      }
      return;
    }
  });

  ws.on("close", (code, reason) => {
    console.log("[WS] closed", { code, reason: reason?.toString() });
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
