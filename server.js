/**
 * Index VoiceBot – Betty
 * Runtime: Node (CommonJS), Express, WebSocket
 * - /health: service status + SSOT load summary
 * - /twilio-media-stream: Twilio Media Streams ingress (mulaw 8k)
 *
 * SSOT (READ ONLY):
 * - GSHEET_ID
 * - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64 of service account JSON)
 *
 * Tabs expected:
 * - SETTINGS (key,value)
 * - PROMPTS (prompt_id,content_he)
 * - INTENTS (intent,priority,trigger_type,triggers_he,notes)
 * - INTENT_SUGGESTIONS (phrase_he,detected_intent,occurrences,last_seen_at,approved,notes)  // read only for now
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const SERVICE_NAME = process.env.SERVICE_NAME || "index-betty-voicebot";
const PORT = process.env.PORT || 10000;

const MB_DEBUG = String(process.env.MB_DEBUG || "false").toLowerCase() === "true";

function nowIso() {
  return new Date().toISOString();
}

function safeString(v) {
  if (v === undefined || v === null) return "";
  return String(v).trim();
}

function decodeServiceAccountJson() {
  const b64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64;
  if (!b64) return null;
  try {
    const raw = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

/** -------------------------
 * SSOT loader (READ ONLY)
 * ------------------------*/
const ssot = {
  enabled: false,
  loaded_at: null,
  error: null,
  settings: {},
  prompts: {},
  intents: [],
  intent_suggestions: [],
  counts: {
    settings_keys: 0,
    prompts_keys: 0,
    intents: 0,
    intent_suggestions: 0,
  },
};

async function loadSSOT() {
  const spreadsheetId = safeString(process.env.GSHEET_ID);
  const sa = decodeServiceAccountJson();

  ssot.enabled = Boolean(spreadsheetId && sa);
  ssot.loaded_at = null;
  ssot.error = null;
  ssot.settings = {};
  ssot.prompts = {};
  ssot.intents = [];
  ssot.intent_suggestions = [];
  ssot.counts = { settings_keys: 0, prompts_keys: 0, intents: 0, intent_suggestions: 0 };

  if (!ssot.enabled) {
    ssot.error = "SSOT disabled: missing GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64";
    return;
  }

  try {
    const auth = new google.auth.JWT({
      email: sa.client_email,
      key: sa.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    });

    const sheets = google.sheets({ version: "v4", auth });

    async function getValues(tab) {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${tab}!A1:Z`,
      });
      return Array.isArray(res.data.values) ? res.data.values : [];
    }

    // SETTINGS
    const settingsRows = await getValues("SETTINGS");
    for (let i = 1; i < settingsRows.length; i++) {
      const row = settingsRows[i] || [];
      const k = safeString(row[0]);
      const v = safeString(row[1]);
      if (!k) continue;
      if (!v) continue;
      ssot.settings[k] = v;
    }

    // PROMPTS
    const promptsRows = await getValues("PROMPTS");
    for (let i = 1; i < promptsRows.length; i++) {
      const row = promptsRows[i] || [];
      const promptId = safeString(row[0]);
      const contentHe = safeString(row[1]);
      if (!promptId) continue;
      if (!contentHe) continue;
      ssot.prompts[promptId] = contentHe;
    }

    // INTENTS
    const intentsRows = await getValues("INTENTS");
    for (let i = 1; i < intentsRows.length; i++) {
      const row = intentsRows[i] || [];
      const intent = safeString(row[0]);
      if (!intent) continue;

      const priority = Number(safeString(row[1]) || "0");
      const trigger_type = safeString(row[2]);
      const triggers_he_raw = safeString(row[3]);
      const notes = safeString(row[4]);

      const triggers_he = triggers_he_raw
        ? triggers_he_raw.split(",").map(s => safeString(s)).filter(Boolean)
        : [];

      ssot.intents.push({ intent, priority, trigger_type, triggers_he, notes });
    }

    // INTENT_SUGGESTIONS (read-only)
    const suggRows = await getValues("INTENT_SUGGESTIONS");
    for (let i = 1; i < suggRows.length; i++) {
      const row = suggRows[i] || [];
      const phrase_he = safeString(row[0]);
      if (!phrase_he) continue;
      ssot.intent_suggestions.push({
        phrase_he,
        detected_intent: safeString(row[1]),
        occurrences: Number(safeString(row[2]) || "0"),
        last_seen_at: safeString(row[3]),
        approved: safeString(row[4]),
        notes: safeString(row[5]),
      });
    }

    ssot.counts.settings_keys = Object.keys(ssot.settings).length;
    ssot.counts.prompts_keys = Object.keys(ssot.prompts).length;
    ssot.counts.intents = ssot.intents.length;
    ssot.counts.intent_suggestions = ssot.intent_suggestions.length;

    ssot.loaded_at = nowIso();
    ssot.error = null;
  } catch (e) {
    ssot.loaded_at = nowIso();
    ssot.error = e && (e.message || String(e)) || "unknown error";
  }
}

/** Auto-load on startup */
loadSSOT().catch(() => {});

/** Optional: periodic refresh */
const SSOT_REFRESH_SEC = Number(process.env.SSOT_REFRESH_SEC || "300");
if (SSOT_REFRESH_SEC > 0) {
  setInterval(() => {
    loadSSOT().catch(() => {});
  }, SSOT_REFRESH_SEC * 1000);
}

/** -------------------------
 * HTTP server
 * ------------------------*/
const app = express();
app.use(express.json({ limit: "2mb" }));

app.get("/health", (req, res) => {
  const payload = {
    ok: true,
    service: SERVICE_NAME,
    ts: nowIso(),
    provider_mode: safeString(process.env.PROVIDER_MODE || "openai"),
    ssot: {
      enabled: ssot.enabled,
      loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys: ssot.counts.settings_keys,
      prompts_keys: ssot.counts.prompts_keys,
      intents: ssot.counts.intents,
      intent_suggestions: ssot.counts.intent_suggestions,
    },
  };

  // Debug helper (never include secrets)
  if (MB_DEBUG) {
    payload.ssot.sample = {
      settings_keys: Object.keys(ssot.settings).slice(0, 10),
      prompts_keys: Object.keys(ssot.prompts).slice(0, 10),
      intents: ssot.intents.slice(0, 5).map(x => x.intent),
    };
  }

  res.json(payload);
});

/** Manual SSOT refresh (read-only) */
app.post("/ssot/refresh", async (req, res) => {
  await loadSSOT();
  res.json({
    ok: true,
    ts: nowIso(),
    ssot: {
      enabled: ssot.enabled,
      loaded_at: ssot.loaded_at,
      error: ssot.error,
      ...ssot.counts,
    },
  });
});

/** -------------------------
 * WebSocket server placeholder
 * (kept minimal; SSOT scope only)
 * ------------------------*/
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (ws) => {
  if (MB_DEBUG) console.log("[WS] connection established");

  ws.on("message", () => {
    // SSOT task: no realtime routing here. Your existing voice runtime can live in this endpoint.
  });

  ws.on("close", () => {
    if (MB_DEBUG) console.log("[WS] closed");
  });

  ws.on("error", (err) => {
    console.log("[WS] error", err && (err.message || err));
  });
});

server.listen(PORT, () => {
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: https://${process.env.RENDER_EXTERNAL_HOSTNAME || "YOUR_RENDER_HOST"}/health`);
});
