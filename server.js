// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline + SSOT)
// Baseline preserved.
// Adds:
// E) Caller Memory (Postgres): returning caller recognition + monthly purge

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const { Pool } = require("pg");

/* ================== ENV ================== */

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

const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SA_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

const MB_DEBUG = String(process.env.MB_DEBUG || "").toLowerCase() === "true";
const MB_LOG_TRANSCRIPTS = String(process.env.MB_LOG_TRANSCRIPTS || "").toLowerCase() === "true";
const MB_LOG_ASSISTANT_TEXT = String(process.env.MB_LOG_ASSISTANT_TEXT || "").toLowerCase() === "true";

// Memory
const MB_MEMORY_ENABLED = String(process.env.MB_MEMORY_ENABLED || "").toLowerCase() === "true";
const MB_MEMORY_RETENTION_DAYS = Number(process.env.MB_MEMORY_RETENTION_DAYS || 30);
const MB_RETURNING_CALL_TEXT = process.env.MB_RETURNING_CALL_TEXT || "";

// DB
const DATABASE_URL = process.env.DATABASE_URL || "";

if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

/* ================== TIME ================== */

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

/* ================== DB (Memory) ================== */

let db = null;

if (MB_MEMORY_ENABLED && DATABASE_URL) {
  db = new Pool({ connectionString: DATABASE_URL });
}

async function initMemoryTable() {
  if (!db) return;
  await db.query(`
    CREATE TABLE IF NOT EXISTS caller_memory (
      id SERIAL PRIMARY KEY,
      caller_key TEXT UNIQUE NOT NULL,
      name TEXT,
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}

async function findCallerName(callerKey) {
  if (!db || !callerKey) return null;
  const r = await db.query(
    `SELECT name FROM caller_memory WHERE caller_key=$1 LIMIT 1`,
    [callerKey]
  );
  return r.rows[0]?.name || null;
}

async function upsertCallerName(callerKey, name) {
  if (!db || !callerKey || !name) return;
  await db.query(
    `
    INSERT INTO caller_memory (caller_key, name, last_seen_at)
    VALUES ($1,$2,now())
    ON CONFLICT (caller_key)
    DO UPDATE SET name=EXCLUDED.name, last_seen_at=now()
    `,
    [callerKey, name]
  );
}

async function purgeOldMemory() {
  if (!db) return;
  await db.query(
    `DELETE FROM caller_memory WHERE last_seen_at < now() - ($1 || ' days')::interval`,
    [MB_MEMORY_RETENTION_DAYS]
  );
}

/* ================== SSOT (UNCHANGED) ================== */

const SSOT_TTL_MS = 60_000;

const ssot = {
  enabled: !!(GSHEET_ID && GOOGLE_SA_B64),
  loaded_at: null,
  error: null,
  data: { settings: {}, prompts: {}, intents: [], intent_suggestions: [] },
  _expires: 0,
};

async function loadSSOT(force = false) {
  if (!ssot.enabled) return ssot;
  const now = Date.now();
  if (!force && now < ssot._expires && ssot.loaded_at) return ssot;

  try {
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

    const settingsRows = await read("SETTINGS!A:B");
    const settings = {};
    settingsRows.slice(1).forEach(([k, v]) => {
      if (k) settings[String(k).trim()] = v ?? "";
    });

    const promptRows = await read("PROMPTS!A:B");
    const prompts = {};
    promptRows.slice(1).forEach(([id, content]) => {
      if (id) prompts[String(id).trim()] = content ?? "";
    });

    ssot.data = { settings, prompts, intents: [], intent_suggestions: [] };
    ssot.loaded_at = nowIso();
    ssot.error = null;
    ssot._expires = Date.now() + SSOT_TTL_MS;
  } catch (e) {
    ssot.error = e.message || String(e);
  }
  return ssot;
}

/* ================== APP ================== */

const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  await loadSSOT(false);
  res.json({ ok: true, service: "index-betty-voicebot", ts: nowIso() });
});

app.post("/admin/reload-sheets", async (req, res) => {
  await loadSSOT(true);
  res.json({ ok: !ssot.error, error: ssot.error });
});

/* ================== WS ================== */

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs) => {
  let streamSid = null;
  let callerKey = null;
  let capturedName = null;

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openaiWs.on("open", async () => {
    await loadSSOT(false);
    const { settings, prompts } = ssot.data;

    const greeting = getGreetingBucketAndText();

    let openingText = settings.OPENING_SCRIPT || "";

    if (MB_MEMORY_ENABLED && callerKey) {
      const knownName = await findCallerName(callerKey);
      if (knownName && MB_RETURNING_CALL_TEXT) {
        openingText = MB_RETURNING_CALL_TEXT
          .replace("{GREETING}", greeting.text)
          .replace("{NAME}", knownName);
      }
    }

    const instructions = [
      prompts.MASTER_PROMPT,
      prompts.GUARDRAILS_PROMPT,
      prompts.KB_PROMPT,
      prompts.LEAD_CAPTURE_PROMPT,
    ].join("\n\n");

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
        },
      })
    );

    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: {
          modalities: ["audio", "text"],
          instructions: openingText,
        },
      })
    );
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "response.completed" && msg.output?.name) {
      capturedName = msg.output.name;
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

  twilioWs.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      const caller =
        msg.start.customParameters?.caller ||
        msg.start.customParameters?.caller_id ||
        null;
      callerKey = caller;
      return;
    }

    if (msg.event === "stop") {
      if (MB_MEMORY_ENABLED && callerKey && capturedName) {
        await upsertCallerName(callerKey, capturedName);
      }
      openaiWs.close();
    }
  });
});

/* ================== START ================== */

(async () => {
  if (MB_MEMORY_ENABLED && db) {
    await initMemoryTable();
    setInterval(purgeOldMemory, 1000 * 60 * 60 * 24 * 30);
  }
})();

server.listen(PORT, () => {
  console.log(`==> Service live on port ${PORT}`);
});
