// server.js
// Index Betty – Realtime VoiceBot (OpenAI baseline + SSOT)
// Baseline preserved (audio queue, VAD, debounce gates).
// Adds / locks:
// 1) Webhook ENV mapping (fixed):
//    - CALL_LOG_WEBHOOK_URL   -> CALL_LOG
//    - ABANDONED_WEBHOOK_URL  -> ABANDONED
//    - FINAL_WEBHOOK_URL      -> FINAL + PARTIAL
// 2) Lead decision (single decision point): FINAL / PARTIAL / ABANDONED
//    - FINAL: name + message (+ callback_to_number if callback_requested)
//    - PARTIAL: has name but missing message OR callback requested missing number
//    - ABANDONED: not FINAL/PARTIAL and no name
// 3) INFO payload enrichment: info_request_text, info_answer_text, info_topics, notes_internal
// 4) Twilio recording public link for FINAL/PARTIAL/ABANDONED via proxy endpoint:
//    - GET /recordings/:recordingSid.mp3  (streams from Twilio with Basic Auth)
// 5) Noise solution option (keep BOTH options in code; behavior controlled by ENV):
//    - MB_HALF_DUPLEX=true  (strict turn-taking: drop user audio while bot speaking)
//    - MB_BARGEIN_ENABLED=true (+ knobs) cancels assistant on user speech (barge-in)
//      NOTE: If MB_HALF_DUPLEX=true, barge-in is disabled (half-duplex wins).
//
// NEW (added, without touching existing logic):
// 6) Recording v2 (GilSport-style) OPTIONAL: start Twilio recording + callback registry + wait
//    - POST /twilio-recording-callback (Twilio RecordingStatusCallback)
//    - Best-effort wait (up to 12s) before sending FINAL/PARTIAL/ABANDONED
//    - Falls back to legacy "fetchLatestRecordingSid" if callback path not enabled/missing
// 7) OPTIONAL post-call LLM parsing enrichment (in addition, not instead):
//    - Adds parsedLeadCollection (+ parsedLeadCollection_error) into webhook payloads
//    - Does NOT affect deterministic decision FINAL/PARTIAL/ABANDONED

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const { createMemoryDb } = require("./memoryDb");

const PORT = process.env.PORT || 10000;

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

/* ================== ENV helpers (P0) ================== */
function envBool(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return defaultValue;
  const normalized = String(raw).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "n", "off"].includes(normalized)) return false;
  return defaultValue;
}

function envNum(name, defaultValue) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return defaultValue;
  const n = Number(raw);
  return Number.isFinite(n) ? n : defaultValue;
}

function safeStr(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

/* ================== Optional LLM parsing (additive) ================== */
const MB_ENABLE_LLM_PARSING = envBool("MB_ENABLE_LLM_PARSING", false);
const MB_LEAD_PARSING_MODEL = (process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini").trim();

/* ================== Optional Recording v2 (GilSport-style) ================== */
const MB_ENABLE_RECORDING = envBool("MB_ENABLE_RECORDING", false);

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim(); // optional
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);

const MB_BARGEIN_ENABLED = envBool("MB_BARGEIN_ENABLED", false);
const MB_BARGEIN_MIN_MS = envNum("MB_BARGEIN_MIN_MS", 250);
const MB_BARGEIN_COOLDOWN_MS = envNum("MB_BARGEIN_COOLDOWN_MS", 600);
const MB_BARGEIN_AUDIO_DROP_MS = envNum("MB_BARGEIN_AUDIO_DROP_MS", 0);

// Strict turn-taking mode (noise hardening): when true, we drop user audio while assistant is speaking
const MB_HALF_DUPLEX = envBool("MB_HALF_DUPLEX", false);

const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SA_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

const MB_DEBUG = envBool("MB_DEBUG", false);
// If STT enabled, default log transcripts to true unless explicitly disabled
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", !!MB_TRANSCRIPTION_MODEL);
const MB_LOG_ASSISTANT_TEXT = envBool("MB_LOG_ASSISTANT_TEXT", false);

const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", false);

const MB_NAME_CAPTURE_EARLY_WINDOW_SEC = envNum("MB_NAME_CAPTURE_EARLY_WINDOW_SEC", 30);
const MB_NAME_CAPTURE_EARLY_UTTERANCES = envNum("MB_NAME_CAPTURE_EARLY_UTTERANCES", 2);
const MB_NAME_EXPECTING_TTL_MS = envNum("MB_NAME_EXPECTING_TTL_MS", 12000);

// Webhook URLs (fixed mapping)
const CALL_LOG_WEBHOOK_URL = (process.env.CALL_LOG_WEBHOOK_URL || "").trim(); // CALL_LOG
const ABANDONED_WEBHOOK_URL = (process.env.ABANDONED_WEBHOOK_URL || "").trim(); // ABANDONED
const FINAL_WEBHOOK_URL = (process.env.FINAL_WEBHOOK_URL || "").trim(); // FINAL + PARTIAL

// Twilio for recording fetch/start/callback proxy
const TWILIO_ACCOUNT_SID = (process.env.TWILIO_ACCOUNT_SID || "").trim();
const TWILIO_AUTH_TOKEN = (process.env.TWILIO_AUTH_TOKEN || "").trim();
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || process.env.RENDER_EXTERNAL_URL || "").trim();

// DB URL (Render: DATABASE_URL)
const MEMORY_DB_URL = (process.env.MEMORY_DB_URL || process.env.DATABASE_URL || "").trim();

if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function baseUrlNoSlash(u) {
  return String(u || "").replace(/\/+$/, "");
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

function injectVars(text, vars) {
  let out = String(text || "");
  for (const [k, v] of Object.entries(vars || {})) {
    out = out.replaceAll(`{${k}}`, String(v ?? ""));
  }
  return out;
}

function getClosingLineFromSsot() {
  const settings = ssot.data?.settings || {};
  const closing = safeStr(settings?.CLOSING_SCRIPT || "");
  if (!closing) return "";
  return injectVars(closing, settings);
}

function buildSettingsContext(settings) {
  const lines = Object.entries(settings || {}).map(([k, v]) => `${k}=${String(v ?? "")}`);
  return lines.join("\n");
}

/* ================== Memory DB (module) ================== */
const memory = createMemoryDb({ url: MEMORY_DB_URL, debug: MB_DEBUG });

// Prewarm memory DB at startup (never blocks boot)
(async () => {
  const t0 = Date.now();
  try {
    await memory.init();
  } finally {
    if (MB_DEBUG) {
      console.log("[MEMORY_DB] prewarm", {
        enabled: memory.state.enabled,
        ready: memory.state.ready,
        error: memory.state.error,
        ms: Date.now() - t0,
      });
    }
  }
})();

/* ================== SSOT ================== */
const SSOT_TTL_MS = 60_000;

// P1: required SSOT keys (non-secret)
const REQUIRED_SETTINGS_KEYS = [
  "BUSINESS_NAME",
  "BOT_NAME",
  "DEFAULT_LANGUAGE",
  "MAIN_PHONE",
  "BUSINESS_EMAIL",
  "BUSINESS_ADDRESS",
  "WORKING_HOURS",
  "OPENING_SCRIPT",
  "OPENING_SCRIPT_RETURNING",
  "NO_DATA_MESSAGE",
  "CLOSING_SCRIPT",
];

const REQUIRED_PROMPTS = ["MASTER_PROMPT", "GUARDRAILS_PROMPT", "KB_PROMPT", "LEAD_CAPTURE_PROMPT"];

const ssot = {
  enabled: !!(GSHEET_ID && GOOGLE_SA_B64),
  loaded_at: null,
  error: null,
  data: { settings: {}, prompts: {}, intents: [], intent_suggestions: [] },
  missing_settings_keys: [],
  missing_prompt_keys: [],
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

    const intentRows = await read("INTENTS!A:D");
    const intents = intentRows.slice(1).map((r) => ({
      intent: r[0],
      priority: Number(r[1] || 0),
      trigger_type: r[2],
      triggers_he: r[3],
    }));

    const suggRows = await read("INTENT_SUGGESTIONS!A:F");
    const intent_suggestions = suggRows.slice(1).map((r) => ({
      phrase_he: r[0],
      detected_intent: r[1],
      occurrences: r[2],
      last_seen_at: r[3],
      approved: r[4],
      notes: r[5],
    }));

    const missingSettings = REQUIRED_SETTINGS_KEYS.filter((k) => !settings[k]);
    const missingPrompts = REQUIRED_PROMPTS.filter((k) => !prompts[k]);

    ssot.data = { settings, prompts, intents, intent_suggestions };
    ssot.loaded_at = nowIso();
    ssot.missing_settings_keys = missingSettings;
    ssot.missing_prompt_keys = missingPrompts;

    ssot.error = null;
    if (missingSettings.length || missingPrompts.length) {
      const parts = [];
      if (missingSettings.length) parts.push(`Missing SETTINGS keys: ${missingSettings.join(", ")}`);
      if (missingPrompts.length) parts.push(`Missing PROMPTS keys: ${missingPrompts.join(", ")}`);
      ssot.error = parts.join(" | ");
    }

    ssot._expires = Date.now() + SSOT_TTL_MS;

    if (MB_DEBUG) {
      console.log("[SSOT] loaded", {
        settings_keys: Object.keys(settings).length,
        prompts_keys: Object.keys(prompts).length,
        intents: intents.length,
        intent_suggestions: intent_suggestions.length,
        missing_settings: missingSettings.length,
        missing_prompts: missingPrompts.length,
      });
    }
  } catch (e) {
    ssot.error = e && (e.message || String(e));
    console.error("[SSOT] load failed", ssot.error);
  }

  return ssot;
}

function getSSOTCacheFast() {
  const now = Date.now();
  const hasData =
    Object.keys(ssot.data.settings || {}).length > 0 &&
    Object.keys(ssot.data.prompts || {}).length > 0;

  const stale = !ssot.loaded_at || now >= ssot._expires;

  if (hasData) {
    if (stale) {
      if (MB_DEBUG) console.log("[SSOT] stale cache -> background refresh");
      loadSSOT(true).catch(() => {});
    }
    return { ok: true, stale, ssot };
  }
  return { ok: false, stale: true, ssot };
}

// Prewarm SSOT at startup (non-blocking)
(async () => {
  if (!ssot.enabled) return;
  try {
    const t0 = Date.now();
    await loadSSOT(false);
    if (MB_DEBUG) console.log("[SSOT] prewarm done in", Date.now() - t0, "ms");
  } catch (e) {
    console.error("[SSOT] prewarm failed", e && (e.message || e));
  }
})();

/* ================== Name heuristics ================== */
const NAME_REJECT_WORDS = new Set([
  "היי",
  "שלום",
  "כן",
  "לא",
  "רגע",
  "שנייה",
  "שניה",
  "אוקיי",
  "אוקי",
  "המ",
  "אמ",
  "אה",
  "טוב",
  "ביי",
  "להתראות",
  "תודה",
  "מה",
  "למה",
  "איפה",
  "מתי",
  "איך",
  "כמה",
  "מי",
  "בסדר",
  "תשמע",
  "תקשיבי",
  "תקשיבו",
  "בסדר גמור",
]);

function looksLikeName(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  const cleaned = t.replace(/[^\u0590-\u05FFa-zA-Z\s'-]/g, "").trim();
  if (!cleaned) return false;

  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length > 3) return false;
  if (cleaned.length > 22) return false;
  if (/\d/.test(t)) return false;

  for (const w of words) {
    const ww = w.toLowerCase();
    if (NAME_REJECT_WORDS.has(ww)) return false;
  }
  if (words.length === 1) {
    const w = words[0];
    if (w.startsWith("ב") && w.length >= 4) return false;
  }
  return true;
}

function extractNameFromSelfIntro(text) {
  const t = String(text || "").trim();
  if (!t) return null;
  const match = t.match(/\b(קוראים לי|שמי|אני|מדבר|מדברת)\s+([^\n\r,.!?]{1,22})/);
  if (!match) return null;
  const prefix = match[1];
  let name = String(match[2] || "").trim();
  name = name.replace(/^[\s"'“”‘’׳״\-–—.,!?]+|[\s"'“”‘’׳״\-–—.,!?]+$/g, "");
  name = name.replace(/["'“”‘’׳״]/g, "").trim();
  if (!name) return null;
  if (name.length > 22) return null;
  if (prefix === "אני" || prefix === "מדבר" || prefix === "מדברת") {
    const words = name.split(/\s+/).filter(Boolean);
    if (words.length < 1 || words.length > 2) return null;
    const firstWord = (words[0] || "").toLowerCase();
    if (
      /(רוצה|צריך|צריכה|מבקש|מבקשת|מתקשר|מתקשרת|מחפש|מחפשת|מבקשים|מבקשות|לגבי|על|בקשר|שאלה|בעיה|זה|זאת|התקשרתי|פונה|מדבר|מדברת)/.test(
        firstWord
      )
    ) {
      return null;
    }
    if (prefix === "אני") {
      if (/^ש/.test(firstWord) || /^(שאני|ש)/.test(firstWord)) return null;
    }
  }
  if (!looksLikeName(name)) return null;
  return name;
}

function shouldAttemptEarlyNameCapture(capturedName, connStartedAtMs, userUtteranceCount) {
  if (capturedName) return false;
  const withinTime = Date.now() - connStartedAtMs <= MB_NAME_CAPTURE_EARLY_WINDOW_SEC * 1000;
  const withinUtterances = userUtteranceCount <= MB_NAME_CAPTURE_EARLY_UTTERANCES;
  return withinTime && withinUtterances;
}

/* ================== Closing enforcement ================== */
function isCallerWantsToEnd(stt) {
  const t = String(stt || "").trim();
  if (!t) return false;
  return /(ביי|להתראות|סיימנו|זהו|תודה ביי|יאללה ביי|נדבר|סגרנו)/.test(t);
}

/* ================== Callback detection helpers ================== */
function isYes(text) {
  const t = String(text || "").trim();
  return /^(כן|כן בבקשה|כן תודה|בטח|נכון|כן כן)$/i.test(t);
}
function isNo(text) {
  const t = String(text || "").trim();
  return /^(לא|לא תודה|ממש לא|לא לא)$/i.test(t);
}
function extractILPhoneDigits(text) {
  const t = String(text || "");
  const digits = t.replace(/[^\d]/g, "");
  if (digits.length === 9 || digits.length === 10) return digits;
  return null;
}

function normalizePhoneLocal(input) {
  const raw = safeStr(input);
  if (!raw) return null;
  const digits = raw.replace(/[^\d]/g, "");
  if (!digits) return null;
  if (digits.startsWith("972") && digits.length >= 11) {
    const local = digits.slice(3);
    if (local.length === 9) return `0${local}`;
    if (local.length === 10 && local.startsWith("0")) return local;
  }
  if (digits.length === 10 && digits.startsWith("0")) return digits;
  if (digits.length === 9) return `0${digits}`;
  return null;
}
function isCallbackRequested(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  return /(תחזרו|תחזור|חזרה|שיחזרו|טלפון חוזר|תתקשרו|שיחה חוזרת|שיחזר אליי|לחזור אלי|לחזור אליי)/.test(
    t
  );
}

/* ================== INFO detection (deterministic) ================== */
function isInfoRequest(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  return /(שעות|שעות פעילות|עד מתי|מתי פתוח|מתי פתוחים|כתובת|איפה אתם|מיקום|טלפון|מספר טלפון|איך מתקשרים|מייל|אימייל|דוא"ל|email)/i.test(
    t
  );
}

function detectInfoTopics(text) {
  const t = String(text || "").toLowerCase();
  const topics = [];

  if (/(שעות|שעות פעילות|פתוח|פתוחים|סגור|סגורים)/.test(t)) topics.push("WORKING_HOURS");
  if (/(כתובת|איפה אתם|מיקום)/.test(t)) topics.push("BUSINESS_ADDRESS");
  if (/(טלפון|מספר טלפון|איך מתקשרים|מספר)/.test(t)) topics.push("MAIN_PHONE");
  if (/(מייל|אימייל|דוא"ל|email)/.test(t)) topics.push("BUSINESS_EMAIL");

  return Array.from(new Set(topics));
}

/* ================== Webhook sender + https logs ================== */
async function postJson(url, payload, meta = {}) {
  if (!url) return { ok: false, status: null };

  const started = Date.now();
  const tag = meta.tag || "WEBHOOK";

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (MB_DEBUG) {
      const ms = Date.now() - started;
      let bodyText = "";
      try {
        bodyText = await res.text();
      } catch {}
      const sample = String(bodyText || "").slice(0, 600);
      console.log(`[${tag}] POST`, {
        url,
        status: res.status,
        ok: res.ok,
        ms,
        resp_sample: sample || null,
      });
    }

    return { ok: res.ok, status: res.status };
  } catch (e) {
    console.error(`[${tag}] post failed`, { url, err: e && (e.message || String(e)) });
    return { ok: false, status: null };
  }
}

/* ================== Twilio recording helpers (proxy + legacy + v2 start/callback) ================== */
function twilioAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;
  const token = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
  return `Basic ${token}`;
}

function getPublicOrigin() {
  try {
    if (!PUBLIC_BASE_URL) return "";
    const u = new URL(PUBLIC_BASE_URL);
    return `${u.protocol}//${u.host}`;
  } catch {
    return "";
  }
}

// ---- v2 registry (GilSport-style) ----
const RECORDINGS = new Map(); // callSid -> { callSid, recordingSid, recordingUrl, updatedAt }

function setRecordingForCall(callSid, { recordingSid, recordingUrl } = {}) {
  const cs = safeStr(callSid);
  if (!cs) return;
  const sid = safeStr(recordingSid);
  const url = safeStr(recordingUrl);
  const cur = RECORDINGS.get(cs) || { callSid: cs, recordingSid: "", recordingUrl: "", updatedAt: 0 };
  if (sid) cur.recordingSid = sid;
  if (url) cur.recordingUrl = url;
  cur.updatedAt = Date.now();
  RECORDINGS.set(cs, cur);
}

function getRecordingForCall(callSid) {
  const cs = safeStr(callSid);
  if (!cs) return { callSid: "", recordingSid: "", recordingUrl: "", updatedAt: 0 };
  return RECORDINGS.get(cs) || { callSid: cs, recordingSid: "", recordingUrl: "", updatedAt: 0 };
}

async function waitForRecording(callSid, timeoutMs = 12000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = getRecordingForCall(callSid);
    if (r.recordingSid || r.recordingUrl) return r;
    await sleep(250);
  }
  return getRecordingForCall(callSid);
}

async function startRecordingIfEnabled(callSid, tagForLog = "recording") {
  if (!MB_ENABLE_RECORDING) return { ok: false, reason: "recording_disabled" };
  if (!callSid) return { ok: false, reason: "missing_callSid" };
  const auth = twilioAuthHeader();
  if (!auth) return { ok: false, reason: "twilio_auth_missing" };
  if (!PUBLIC_BASE_URL) return { ok: false, reason: "public_base_url_missing" };

  const cbUrl = `${getPublicOrigin()}/twilio-recording-callback`;
  if (!cbUrl.startsWith("http")) return { ok: false, reason: "callback_url_invalid" };

  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
    TWILIO_ACCOUNT_SID
  )}/Calls/${encodeURIComponent(callSid)}/Recordings.json`;

  const body = new URLSearchParams({
    RecordingStatusCallback: cbUrl,
    RecordingStatusCallbackMethod: "POST",
    RecordingChannels: "dual",
  });

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: auth,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    });

    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (MB_DEBUG) console.error(`[${tagForLog}] recording start failed`, { status: res.status, body: json });
      return { ok: false, reason: "recording_start_failed", status: res.status };
    }

    const sid = safeStr(json?.sid || "");
    if (sid) setRecordingForCall(callSid, { recordingSid: sid });
    if (MB_DEBUG) console.log(`[${tagForLog}] recording started`, { callSid, recordingSid: sid || null });
    return { ok: true, reason: "recording_started", sid: sid || null };
  } catch (e) {
    if (MB_DEBUG) console.error(`[${tagForLog}] recording start error`, e && (e.message || String(e)));
    return { ok: false, reason: "recording_start_error" };
  }
}

// ---- legacy (list latest recording) ----
async function fetchLatestRecordingSid(callSid) {
  if (!callSid) return null;
  const auth = twilioAuthHeader();
  if (!auth) return null;

  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
    TWILIO_ACCOUNT_SID
  )}/Calls/${encodeURIComponent(callSid)}/Recordings.json?PageSize=50`;

  try {
    const res = await fetch(url, { headers: { Authorization: auth } });
    if (!res.ok) return null;
    const json = await res.json();
    const recs = Array.isArray(json.recordings) ? json.recordings : [];
    if (!recs.length) return null;

    // pick newest by date_created if available
    recs.sort((a, b) => {
      const da = Date.parse(a.date_created || "") || 0;
      const db = Date.parse(b.date_created || "") || 0;
      return db - da;
    });

    const sid = recs[0]?.sid || null;
    return sid;
  } catch {
    return null;
  }
}

/* ================== fetchWithTimeout (for additive LLM parsing) ================== */
async function fetchWithTimeout(url, options = {}, timeoutMs = 6500) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

/* ================== Additive LLM parsing helpers ================== */
function normalizeKeyLoose(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_'"״׳]/g, "");
}

function coerceLeadFields(obj) {
  const out = {};
  const entries = Object.entries(obj || {}).map(([k, v]) => [normalizeKeyLoose(k), v]);
  const loose = Object.fromEntries(entries);

  const getLoose = (names) => {
    for (const n of names) {
      const key = normalizeKeyLoose(n);
      if (loose[key] !== undefined && loose[key] !== null && String(loose[key]).trim() !== "") return loose[key];
    }
    return null;
  };

  out.is_lead = obj && typeof obj.is_lead === "boolean" ? obj.is_lead : !!getLoose(["is_lead", "islead"]);
  out.intent = getLoose(["intent"]) || "unknown";
  out.full_name = getLoose(["full_name", "fullname", "name"]) || null;
  out.phone_number = getLoose(["phone_number", "phonenumber", "phone"]) || null;
  out.prefers_caller_id = getLoose(["prefers_caller_id", "preferscallerid"]) ?? null;

  out.brand = getLoose(["brand"]) || null;
  out.model = getLoose(["model"]) || null;
  out.message_for = getLoose(["message_for", "messagefor", "recipient", "target"]) || null;
  out.reason = getLoose(["reason", "issue"]) || null;
  out.notes = getLoose(["notes"]) || null;

  // Normalize types
  if (typeof out.prefers_caller_id === "string") {
    const s = out.prefers_caller_id.toLowerCase();
    out.prefers_caller_id = /^(true|1|yes|y|כן|נכון)$/i.test(s);
  }
  if (out.phone_number != null) out.phone_number = String(out.phone_number).trim() || null;
  if (out.full_name != null) out.full_name = String(out.full_name).trim() || null;
  if (out.reason != null) out.reason = String(out.reason).trim() || null;
  if (out.notes != null) out.notes = String(out.notes).trim() || null;
  if (out.brand != null) out.brand = String(out.brand).trim() || null;
  if (out.model != null) out.model = String(out.model).trim() || null;
  if (out.message_for != null) out.message_for = String(out.message_for).trim() || null;

  return out;
}

async function extractLeadFromConversationLLM({ conversationLog, botName, businessName, callerE164, calledE164 }) {
  if (!MB_ENABLE_LLM_PARSING) return { ok: false, skipped: true, lead: null, error: null };
  if (!OPENAI_API_KEY) return { ok: false, skipped: true, lead: null, error: "missing OPENAI_API_KEY" };
  if (!Array.isArray(conversationLog) || conversationLog.length === 0)
    return { ok: false, skipped: true, lead: null, error: "empty_conversation" };

  const transcript = conversationLog
    .map((m) => `${m.from === "user" ? "לקוח" : botName || "הבוט"}: ${String(m.text || "").trim()}`)
    .filter((x) => x.trim().length > 0)
    .join("\n");

  const systemPrompt = `
You extract a structured lead object from a phone call transcript.
Return ONLY valid JSON (no extra text).
Schema keys MUST be exactly:
{"is_lead":boolean,"intent":"sales"|"support"|"delivery"|"message"|"info"|"unknown","full_name":string|null,"phone_number":string|null,"prefers_caller_id":boolean|null,"brand":string|null,"model":string|null,"message_for":string|null,"reason":string|null,"notes":string|null}

Rules:
- reason and notes MUST be in Hebrew, professional and clear.
- If info is missing, use null (do NOT write "לא נמסר..." or similar).
- Do NOT invent phone numbers.
- If the caller asked only for office info (hours/address/phone/email), intent may be "info".
JSON only.
`.trim();

  const userPrompt = `
Business: ${businessName || ""}
Bot: ${botName || ""}
CallerE164: ${callerE164 || ""}
CalledE164: ${calledE164 || ""}

Transcript:
${transcript}
`.trim();

  try {
    const res = await fetchWithTimeout(
      "https://api.openai.com/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: MB_LEAD_PARSING_MODEL,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt },
          ],
        }),
      },
      9000
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      return { ok: false, skipped: false, lead: null, error: `llm_http_${res.status}:${txt.slice(0, 300)}` };
    }

    const data = await res.json().catch(() => null);
    const raw = data?.choices?.[0]?.message?.content || "";
    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { ok: false, skipped: false, lead: null, error: "llm_invalid_json" };
    }
    const lead = coerceLeadFields(parsed || {});
    return { ok: true, skipped: false, lead, error: null };
  } catch (e) {
    return { ok: false, skipped: false, lead: null, error: String(e?.message || e) };
  }
}

/* ================== APP ================== */
const app = express();

// IMPORTANT: Twilio callbacks are urlencoded by default
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// Twilio RecordingStatusCallback endpoint (GilSport-style)
app.post("/twilio-recording-callback", (req, res) => {
  try {
    const callSid = safeStr(req.body?.CallSid || req.body?.callSid || "");
    const recordingSid = safeStr(req.body?.RecordingSid || req.body?.recordingSid || "");
    const recordingUrl = safeStr(req.body?.RecordingUrl || req.body?.recordingUrl || "");
    if (callSid) {
      setRecordingForCall(callSid, { recordingSid, recordingUrl });
      if (MB_DEBUG) {
        console.log("[RECORDING_CALLBACK]", { callSid, recordingSid: recordingSid || null, hasUrl: !!recordingUrl });
      }
    }
  } catch {
    // ignore
  }
  return res.status(200).send("OK");
});

// Public proxy for Twilio recordings (keeps Twilio creds private)
app.get("/recordings/:recordingSid.mp3", async (req, res) => {
  const recordingSid = String(req.params.recordingSid || "").trim();
  const auth = twilioAuthHeader();

  if (!recordingSid) return res.status(400).send("missing recordingSid");
  if (!auth) return res.status(500).send("recording proxy not configured");

  const mediaUrl = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
    TWILIO_ACCOUNT_SID
  )}/Recordings/${encodeURIComponent(recordingSid)}.mp3`;

  try {
    const r = await fetch(mediaUrl, { headers: { Authorization: auth } });
    if (!r.ok) return res.status(404).send("recording not found");

    res.setHeader("content-type", "audio/mpeg");
    res.setHeader("cache-control", "public, max-age=31536000, immutable");

    // Stream
    const buf = Buffer.from(await r.arrayBuffer());
    return res.status(200).send(buf);
  } catch (e) {
    return res.status(502).send("recording fetch failed");
  }
});

app.get("/health", async (req, res) => {
  await loadSSOT(false);
  await memory.init();

  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
    ssot: {
      enabled: ssot.enabled,
      loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys: Object.keys(ssot.data.settings || {}).length,
      prompts_keys: Object.keys(ssot.data.prompts || {}).length,
      missing_settings_keys: ssot.missing_settings_keys || [],
      missing_prompt_keys: ssot.missing_prompt_keys || [],
      intents: ssot.data.intents.length,
      intent_suggestions: ssot.data.intent_suggestions.length,
    },
    memory_db: {
      enabled: memory.state.enabled,
      ready: memory.state.ready,
      error: memory.state.error,
      last_ok_at: memory.state.last_ok_at,
    },
    model: OPENAI_REALTIME_MODEL,
    recordings_proxy: {
      enabled: !!(TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN && PUBLIC_BASE_URL),
      public_base_url: baseUrlNoSlash(PUBLIC_BASE_URL) || null,
      v2_recording_enabled: MB_ENABLE_RECORDING,
      v2_callback_endpoint: `${getPublicOrigin()}/twilio-recording-callback` || null,
    },
    noise_options: {
      half_duplex: MB_HALF_DUPLEX,
      bargein_enabled: MB_BARGEIN_ENABLED,
      bargein_min_ms: MB_BARGEIN_MIN_MS,
      bargein_cooldown_ms: MB_BARGEIN_COOLDOWN_MS,
      bargein_audio_drop_ms: MB_BARGEIN_AUDIO_DROP_MS,
    },
    transcription: {
      enabled: !!MB_TRANSCRIPTION_MODEL,
      log_transcripts: MB_LOG_TRANSCRIPTS,
      model: MB_TRANSCRIPTION_MODEL || null,
      language: MB_TRANSCRIPTION_LANGUAGE || null,
    },
    llm_parsing: {
      enabled: MB_ENABLE_LLM_PARSING,
      model: MB_LEAD_PARSING_MODEL || null,
    },
  });
});

app.post("/admin/reload-sheets", async (req, res) => {
  const started = nowIso();
  await loadSSOT(true);

  const settings_keys = Object.keys(ssot.data.settings || {}).length;
  const prompt_ids = Object.keys(ssot.data.prompts || {});
  const intents = ssot.data.intents?.length || 0;
  const intent_suggestions = ssot.data.intent_suggestions?.length || 0;
  const missing_settings_keys = ssot.missing_settings_keys || [];
  const missing_prompt_keys = ssot.missing_prompt_keys || [];

  if (ssot.error) {
    return res.status(500).json({
      ok: false,
      reloaded_at: started,
      sheets_loaded_at: ssot.loaded_at,
      error: ssot.error,
      settings_keys,
      prompt_ids,
      missing_settings_keys,
      missing_prompt_keys,
      intents,
      intent_suggestions,
    });
  }

  return res.json({
    ok: true,
    reloaded_at: started,
    sheets_loaded_at: ssot.loaded_at,
    settings_keys,
    prompt_ids,
    missing_settings_keys,
    missing_prompt_keys,
    intents,
    intent_suggestions,
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

/* ================== WS ================== */
wss.on("connection", (twilioWs) => {
  if (MB_DEBUG) console.log("[WS] connection established");

  let streamSid = null;
  let callSid = null;
  let callerE164 = null;
  let calledE164 = null;

  const connT0 = Date.now();
  const connStartedAtMs = connT0;
  let twilioStartAt = null;

  let openaiReady = false;
  let sessionConfigured = false;
  let pendingCreate = false;
  let responseInFlight = false;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  // Assistant speaking (for half-duplex)
  let assistantSpeaking = false;

  // BARGE-IN state + audio drop window
  let speechActive = false;
  let lastBargeinAt = 0;
  let bargeinTimer = null;
  let dropAudioUntilTs = 0;

  // Audio queue
  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  // Transcript aggregation
  let sttBuf = "";
  let asstBuf = "";
  let asstAudioTranscriptBuf = "";

  // Additive conversation log for post-call LLM parsing (does NOT affect flow)
  const conversationLog = []; // {from:"user"|"bot", text:string}
  let botTranscriptLineBuf = ""; // accumulate assistant audio transcript into a "line" to push on done/completed

  // User utterance tracking
  let lastUserUtterance = "";

  // Name/message/callback gates (deterministic)
  let expectingName = false;
  let expectingMessage = false;
  let expectingCallbackConfirm = false;
  let expectingCallbackNumber = false;

  let capturedName = null;
  let userUtteranceCount = 0;
  let assistantAskedNameAt = 0;
  let nameCapturedFrom = null; // "caller_stt" | null
  let capturedMessage = null;
  let callbackRequested = false;
  let callbackToNumber = null; // digits or e164
  let closingForced = false;
  let callEnding = false;
  let callEnded = false;
  let closingResponsePending = false;
  let responseCancelSent = false;

  // INFO tracking (locked)
  let infoRequested = false; // user asked office info
  let infoProvided = false; // assistant answered office info (meaningfully)
  let infoRequestText = null;
  let infoTopics = [];
  let infoAnswerCaptureActive = false;
  let infoAnswerText = "";
  let infoAnswerCharsMax = 1200;

  // Recording tracking
  let recordingSid = null;
  let recordingPublicUrl = null;
  let recordingResolved = false;

  // Additive LLM parsing tracking
  let parsedLeadCollection = null;
  let parsedLeadCollectionError = null;
  let parsedLeadAttempted = false;

  // Lead/webhook bookkeeping
  const callStartedAtIso = nowIso();
  let callEndedAtIso = null;
  let greetingBucket = getGreetingBucketAndText().bucket;

  let sentCallLog = false;
  let sentFinal = false;
  let sentAbandoned = false;
  let sentPartial = false;
  let enrichedFromLlm = false;

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(obj));
      return true;
    } catch (e) {
      console.error("[OPENAI] send failed", e && (e.message || e));
      return false;
    }
  }

  function flushAudioQueue(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (callEnding || callEnded) return;
    if (!openaiReady || !sessionConfigured) return;

    while (audioQueue.length) {
      const payload = audioQueue.shift();
      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }
  }

  function maybeCreateResponse(openaiWs, reason) {
    const now = Date.now();
    if (responseInFlight || callEnding || callEnded) return;

    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
      pendingCreate = true;
      return;
    }

    const DEBOUNCE_MS = 350;
    const MIN_FRAMES = 4;
    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    safeSend(openaiWs, { type: "response.create" });
    pendingCreate = false;
    responseInFlight = true;
    assistantSpeaking = true;
    if (MB_DEBUG) console.log("[TURN] response.create", reason || "speech_stopped");
  }

  function finalizeCallAfterClosing(reason, openaiWs, twilioWs) {
    if (callEnded) return;
    callEnded = true;
    callEndedAtIso = callEndedAtIso || nowIso();
    if (MB_DEBUG) console.log("[CALL_END] finalize", { reason });

    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, reason || "call_end");
    } catch {}
    try {
      if (twilioWs && twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch {}
  }

  function beginCallEnding(reason, openaiWs, twilioWs, { sendClosing = true } = {}) {
    if (callEnding || callEnded) return;
    callEnding = true;
    callEndedAtIso = callEndedAtIso || nowIso();

    if (!responseCancelSent && openaiWs && openaiWs.readyState === WebSocket.OPEN) {
      responseCancelSent = true;
      try {
        safeSend(openaiWs, { type: "response.cancel" });
      } catch {}
      responseInFlight = false;
      assistantSpeaking = false;
    }

    if (!sendClosing) {
      finalizeCallAfterClosing(reason, openaiWs, twilioWs);
      return;
    }

    const closingLine = getClosingLineFromSsot();
    if (!closingLine || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
      finalizeCallAfterClosing(reason, openaiWs, twilioWs);
      return;
    }

    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions:
          "דברי עכשיו את הטקסט הבא ללא תוספות לפני/אחרי, באותו ניסוח בדיוק:\n" + closingLine,
      },
    });
    closingResponsePending = true;
    responseInFlight = true;
    assistantSpeaking = true;
    if (MB_DEBUG) console.log("[CLOSING] response.create", { reason });
  }

  function cancelAssistant(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    safeSend(openaiWs, { type: "response.cancel" });

    if (MB_BARGEIN_AUDIO_DROP_MS > 0) {
      dropAudioUntilTs = Math.max(dropAudioUntilTs, Date.now() + MB_BARGEIN_AUDIO_DROP_MS);
      if (MB_DEBUG) console.log("[BARGEIN] audio_drop", { ms: MB_BARGEIN_AUDIO_DROP_MS });
    }

    if (MB_DEBUG) console.log("[BARGEIN] response.cancel");
  }

  function onSpeechStarted(openaiWs) {
    speechActive = true;

    // Half-duplex: never barge-in / cancel
    if (MB_HALF_DUPLEX) return;

    if (!MB_BARGEIN_ENABLED) return;
    if (!responseInFlight) return;

    const now = Date.now();
    if (now - lastBargeinAt < MB_BARGEIN_COOLDOWN_MS) return;

    if (bargeinTimer) clearTimeout(bargeinTimer);
    bargeinTimer = setTimeout(() => {
      if (!speechActive) return;
      const now2 = Date.now();
      if (!responseInFlight) return;
      if (now2 - lastBargeinAt < MB_BARGEIN_COOLDOWN_MS) return;

      lastBargeinAt = now2;
      cancelAssistant(openaiWs);
    }, Math.max(0, MB_BARGEIN_MIN_MS));
  }

  function onSpeechStopped() {
    speechActive = false;
    if (bargeinTimer) {
      clearTimeout(bargeinTimer);
      bargeinTimer = null;
    }
  }

  function logSTTLine(line) {
    if (!MB_LOG_TRANSCRIPTS) return;
    const t = String(line || "").trim();
    if (!t) return;
    console.log(`[STT] ${t}`);
  }

  function logAsstDelta(delta) {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const d = String(delta || "");
    if (!d) return;
    asstBuf += d;
    process.stdout.write(d);
  }

  function flushAsstLine() {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const t = String(asstBuf || "").trim();
    if (t) process.stdout.write("\n");
    asstBuf = "";
  }

  function logAsstAudioTranscriptDelta(delta) {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const d = String(delta || "");
    if (!d) return;
    asstAudioTranscriptBuf += d;
    process.stdout.write(d);
  }

  function flushAsstAudioTranscriptLine() {
    if (!MB_LOG_ASSISTANT_TEXT) return;
    const t = String(asstAudioTranscriptBuf || "").trim();
    if (t) process.stdout.write("\n");
    asstAudioTranscriptBuf = "";
  }

  async function ensureParsedLeadCollection() {
    if (parsedLeadAttempted) return;
    parsedLeadAttempted = true;

    const { settings } = ssot.data || {};
    const businessName = settings?.BUSINESS_NAME || null;
    const botName = settings?.BOT_NAME || null;

    const r = await extractLeadFromConversationLLM({
      conversationLog,
      botName,
      businessName,
      callerE164,
      calledE164,
    });

    if (r && r.ok && r.lead) {
      parsedLeadCollection = r.lead;
      parsedLeadCollectionError = null;
    } else {
      parsedLeadCollection = null;
      parsedLeadCollectionError = r?.error || null;
    }
  }

  function computeRecordingPublicUrlFromSid(sid) {
    if (!sid) return null;
    const base = baseUrlNoSlash(PUBLIC_BASE_URL);
    if (!base) return null;
    return `${base}/recordings/${sid}.mp3`;
  }

  async function ensureRecordingResolved(reason) {
    if (recordingResolved) return;
    recordingResolved = true; // prevent concurrent storms

    if (!callSid) return;
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !PUBLIC_BASE_URL) return;

    // v2 path: if enabled, wait for callback to arrive (best effort)
    if (MB_ENABLE_RECORDING) {
      const rec = await waitForRecording(callSid, 12000);
      const sid = safeStr(rec?.recordingSid || "");
      if (sid) {
        recordingSid = sid;
        recordingPublicUrl = computeRecordingPublicUrlFromSid(sid);
        if (MB_DEBUG) console.log("[RECORDING_V2] resolved", { reason, callSid, recordingSid: sid });
        return;
      }
      // If callback didn't arrive in time, we fall back to legacy lookup below
      if (MB_DEBUG) console.log("[RECORDING_V2] not found within wait window, falling back", { reason, callSid });
    }

    // Legacy: Twilio recording may appear with a small delay after hangup. Retry a few times.
    const attempts = 3;
    const delays = [350, 700, 1200];

    for (let i = 0; i < attempts; i++) {
      const sid = await fetchLatestRecordingSid(callSid);
      if (sid) {
        recordingSid = sid;
        recordingPublicUrl = computeRecordingPublicUrlFromSid(sid);
        if (MB_DEBUG) console.log("[RECORDING] resolved", { reason, recordingSid: sid });
        return;
      }
      await sleep(delays[i] || 700);
    }

    if (MB_DEBUG) console.log("[RECORDING] not found after retries", { reason });
  }

  function buildNotesInternalForDecision(decision) {
    const parts = [];
    parts.push(`decision=${decision}`);

    if (infoRequestText) parts.push(`info_asked="${String(infoRequestText).slice(0, 220)}"`);
    if (infoTopics.length) parts.push(`info_topics=${infoTopics.join(",")}`);

    const ans = String(infoAnswerText || "").trim();
    if (ans) parts.push(`info_answer="${ans.slice(0, 420)}"`);
    else if (infoRequested) parts.push("info_answer=empty");

    // recording note (debug-friendly)
    if (
      !recordingPublicUrl &&
      (decision === "FINAL" || decision === "PARTIAL" || decision === "ABANDONED")
    ) {
      parts.push("recording_public_url=missing");
    }

    // additive parsing note (debug-friendly only)
    if (MB_ENABLE_LLM_PARSING) {
      if (!parsedLeadCollection && parsedLeadCollectionError) {
        parts.push(`llm_parsing_error="${String(parsedLeadCollectionError).slice(0, 180)}"`);
      }
    }

    return parts.join(" | ");
  }

  function buildBasePayload() {
    const ended = callEndedAtIso || nowIso();
    const started = callStartedAtIso;

    const durationSec = (() => {
      try {
        return Math.max(0, Math.round((new Date(ended) - new Date(started)) / 1000));
      } catch {
        return null;
      }
    })();

    return {
      provider_mode: "openai",
      callSid: callSid || null,
      streamSid: streamSid || null,
      call_id: callSid || null,
      started_at: started,
      ended_at: ended,
      duration_sec: Number.isFinite(durationSec) ? durationSec : null,
      caller_id_e164: callerE164 || null,
      caller_id_raw: callerE164 || null,
      called: calledE164 || null,
      greeting_time_bucket: greetingBucket || null,
      name: capturedName || null,
      message: capturedMessage || null,
      callback_requested: !!callbackRequested,
      callback_to_number: callbackToNumber || null,

      // INFO enrichment
      info_request_text: infoRequestText || null,
      info_answer_text: String(infoAnswerText || "").trim() || null,
      info_topics: Array.isArray(infoTopics) && infoTopics.length ? infoTopics : null,

      // internal note
      notes_internal: null,

      recording_provider: "TWILIO",
      recording_public_url: recordingPublicUrl || null,

      // Additive: post-call parsing enrichment (does not affect decision)
      parsedLeadCollection: parsedLeadCollection || null,
      parsedLeadCollection_error: parsedLeadCollectionError || null,
    };
  }

  function isLeadComplete() {
    if (!capturedName) return false;
    if (!capturedMessage) return false;
    if (callbackRequested && !callbackToNumber) return false;
    return true;
  }

  function isPartialLead() {
    // PARTIAL = יש name אבל חסר message, או שהתבקשה חזרה וחסר callback_to_number.
    if (!capturedName) return false;
    if (!capturedMessage) return true;
    if (callbackRequested && !callbackToNumber) return true;
    return false;
  }

  async function sendCallLogOnce() {
    if (sentCallLog) return;
    if (!CALL_LOG_WEBHOOK_URL) return;
    if (MB_FINAL_WEBHOOK_ONLY) return;

    sentCallLog = true;
    const payload = { event_type: "CALL_LOG", lead_type: "CALL_LOG", ...buildBasePayload() };
    // notes_internal for call log is optional; keep null to avoid noise
    await postJson(CALL_LOG_WEBHOOK_URL, payload, { tag: "CALL_LOG" });
  }

  async function sendFinalOnce() {
    if (sentFinal) return;
    if (!FINAL_WEBHOOK_URL) return;
    sentFinal = true;

    const payload = { event_type: "FINAL", lead_type: "FINAL", lead_decision: "FINAL", ...buildBasePayload() };
    payload.notes_internal = buildNotesInternalForDecision("FINAL");
    await postJson(FINAL_WEBHOOK_URL, payload, { tag: "FINAL" });
  }

  async function sendPartialOnce() {
    if (sentPartial) return;
    if (!FINAL_WEBHOOK_URL) return;
    sentPartial = true;

    const payload = { event_type: "PARTIAL", lead_type: "PARTIAL", lead_decision: "PARTIAL", ...buildBasePayload() };
    payload.notes_internal = buildNotesInternalForDecision("PARTIAL");
    await postJson(FINAL_WEBHOOK_URL, payload, { tag: "PARTIAL" });
  }

  async function sendAbandonedOnce() {
    if (sentAbandoned) return;
    if (!ABANDONED_WEBHOOK_URL) return;
    sentAbandoned = true;

    const payload = {
      event_type: "ABANDONED",
      lead_type: "ABANDONED",
      lead_decision: "ABANDONED",
      ...buildBasePayload(),
    };
    payload.notes_internal = buildNotesInternalForDecision("ABANDONED");
    await postJson(ABANDONED_WEBHOOK_URL, payload, { tag: "ABANDONED" });
  }

  function applyLlmEnrichmentForDecision() {
    enrichedFromLlm = false;
    if (!parsedLeadCollection) return;

    if (!capturedName && parsedLeadCollection.full_name) {
      capturedName = parsedLeadCollection.full_name;
      enrichedFromLlm = true;
    }

    if (
      !capturedMessage &&
      parsedLeadCollection.intent === "message" &&
      (parsedLeadCollection.reason || parsedLeadCollection.notes)
    ) {
      capturedMessage = parsedLeadCollection.reason || parsedLeadCollection.notes;
      enrichedFromLlm = true;
    }

    if (callbackRequested && !callbackToNumber && parsedLeadCollection.phone_number) {
      const normalized = normalizePhoneLocal(parsedLeadCollection.phone_number);
      if (normalized) {
        callbackToNumber = normalized;
        enrichedFromLlm = true;
      }
    }

    if (callbackToNumber) {
      callbackToNumber = normalizePhoneLocal(callbackToNumber) || callbackToNumber;
    }
  }

  async function decideAndSendOnEnd(reason) {
    // Single decision point (prevents contradictions)
    if (sentFinal || sentPartial || sentAbandoned) return;

    // Additive: parse lead collection once (does NOT affect decision)
    if (MB_ENABLE_LLM_PARSING) {
      await ensureParsedLeadCollection();
      applyLlmEnrichmentForDecision();
    }

    // Resolve recording before final/partial/abandoned (best effort)
    await ensureRecordingResolved(reason);

    let decision = "NONE";

    if (isLeadComplete()) {
      decision = "FINAL";
      await sendFinalOnce();
    } else if (isPartialLead()) {
      decision = "PARTIAL";
      await sendPartialOnce();
    } else if (!capturedName) {
      decision = "ABANDONED";
      await sendAbandonedOnce();
    } else {
      // fallback safety: name exists but nothing else -> PARTIAL
      decision = "PARTIAL_FALLBACK";
      await sendPartialOnce();
    }

    if (MB_DEBUG) {
      console.log("[LEAD_DECISION]", {
        reason: reason || null,
        decision,
        name: !!capturedName,
        message: !!capturedMessage,
        callback_requested: !!callbackRequested,
        callback_to_number: !!callbackToNumber,
        infoRequested,
        infoProvided,
        recording_public_url: recordingPublicUrl || null,
        llm_parsing_enabled: MB_ENABLE_LLM_PARSING,
        llm_parsing_ok: !!parsedLeadCollection,
        enriched_from_llm: enrichedFromLlm,
      });
    }
  }

  function captureUserUtteranceFromTranscriptionEvent(msg) {
    if (!msg || typeof msg.type !== "string") return;
    if (!msg.type.toLowerCase().includes("transcription")) return;

    const t =
      (typeof msg.text === "string" && msg.text) ||
      (typeof msg.transcript === "string" && msg.transcript) ||
      "";
    if (t.trim()) lastUserUtterance = t.trim();
  }

  function maybeAppendUserToConversationLog(text) {
    const t = String(text || "").trim();
    if (!t) return;
    // Keep it minimal; do NOT include raw audio or metadata.
    conversationLog.push({ from: "user", text: t });
  }

  function maybeAppendBotToConversationLog(text) {
    const t = String(text || "").trim();
    if (!t) return;
    conversationLog.push({ from: "bot", text: t });
  }

  function applyDeterministicGatesFromUserUtterance(u) {
    const text = String(u || "").trim();
    if (!text) return;

    userUtteranceCount += 1;

    if (expectingName && assistantAskedNameAt) {
      if (Date.now() - assistantAskedNameAt > MB_NAME_EXPECTING_TTL_MS) {
        expectingName = false;
        assistantAskedNameAt = 0;
        if (MB_DEBUG) console.log("[NAME] expecting_name_expired");
      }
    }

    if (!capturedName) {
      const introName = extractNameFromSelfIntro(text);
      if (introName) {
        capturedName = introName;
        nameCapturedFrom = "caller_stt";
        expectingName = false;
        assistantAskedNameAt = 0;
        if (callerE164) memory.saveName(callerE164, capturedName, MB_DEBUG).catch(() => {});
        if (MB_DEBUG) console.log("[NAME] captured self_intro", { name: capturedName });
        return;
      }
    }

    if (expectingName && !capturedName) {
      if (looksLikeName(text)) {
        capturedName = text;
        nameCapturedFrom = "caller_stt";
        expectingName = false;
        assistantAskedNameAt = 0;
        if (MB_DEBUG) console.log("[NAME] captured", { name: capturedName });
        if (callerE164) memory.saveName(callerE164, capturedName, MB_DEBUG).catch(() => {});
        return;
      }
      if (MB_DEBUG) console.log("[NAME] rejected", { utterance: text });
    }

    if (!capturedName && shouldAttemptEarlyNameCapture(capturedName, connStartedAtMs, userUtteranceCount)) {
      if (looksLikeName(text)) {
        capturedName = text;
        nameCapturedFrom = "caller_stt";
        expectingName = false;
        assistantAskedNameAt = 0;
        if (MB_DEBUG) console.log("[NAME] captured early", { name: capturedName });
        if (callerE164) memory.saveName(callerE164, capturedName, MB_DEBUG).catch(() => {});
        return;
      }
    }

    if (isCallerWantsToEnd(text)) closingForced = true;

    // INFO request detection (locked)
    if (!infoRequested && isInfoRequest(text)) {
      infoRequested = true;
      infoRequestText = text;
      infoTopics = detectInfoTopics(text);
      if (MB_DEBUG) console.log("[INFO] requested=true (from user)", { infoTopics });
    }

    if (!callbackRequested && isCallbackRequested(text)) {
      callbackRequested = true;
      if (MB_DEBUG) console.log("[GATE] callback_requested=true (from user)");
    }

    if (expectingMessage && !capturedMessage) {
      const cleaned = text.replace(/\s+/g, " ").trim();
      if (cleaned.length >= 2) {
        capturedMessage = cleaned;
        expectingMessage = false;
        if (MB_DEBUG) console.log("[GATE] message_captured", { message: capturedMessage });
      }
      return;
    }

    if (expectingCallbackConfirm) {
      if (isYes(text)) {
        expectingCallbackConfirm = false;
        callbackToNumber = normalizePhoneLocal(callerE164) || null;
        if (MB_DEBUG) console.log("[GATE] callback_confirmed_callerid", { callbackToNumber });
      } else if (isNo(text)) {
        expectingCallbackConfirm = false;
        expectingCallbackNumber = true;
        if (MB_DEBUG) console.log("[GATE] callback_need_new_number");
      }
      return;
    }

    if (expectingCallbackNumber && !callbackToNumber) {
      const digits = extractILPhoneDigits(text);
      if (digits) {
        callbackToNumber = normalizePhoneLocal(digits) || digits;
        expectingCallbackNumber = false;
        if (MB_DEBUG) console.log("[GATE] callback_number_captured", { callbackToNumber });
      }
      return;
    }
  }

  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openaiWs.on("open", async () => {
    if (MB_DEBUG) console.log("[OPENAI] ws open");
    openaiReady = true;
    if (callEnding || callEnded) {
      try {
        openaiWs.close(1000, "call_already_ended");
      } catch {}
      return;
    }

    const cache = getSSOTCacheFast();
    if (!cache.ok) {
      await loadSSOT(true);
    }

    const { settings, prompts } = ssot.data;
    const g = getGreetingBucketAndText();
    greetingBucket = g.bucket;

    // Memory lookup
    let memoryRow = null;
    if (callerE164) {
      try {
        memoryRow = await memory.lookup(callerE164);
      } catch (e) {
        memoryRow = null;
        if (MB_DEBUG) console.log("[MEMORY] lookup failed", { err: e && (e.message || String(e)) });
      }

      if (memoryRow && memoryRow.name) {
        if (MB_DEBUG) console.log("[MEMORY] hit", { caller: callerE164, name: memoryRow.name });
      } else {
        if (MB_DEBUG) console.log("[MEMORY] miss", { caller: callerE164 });
      }
      memory.upsertCall(callerE164).catch(() => {});
    }

    // OPENING
    const openingTemplate = settings.OPENING_SCRIPT || "";
    const returningTemplate = settings.OPENING_SCRIPT_RETURNING || "";

    let opening = "";
    if (memoryRow && memoryRow.name) {
      capturedName = memoryRow.name; // returning caller prefill
      if (String(returningTemplate || "").trim()) {
        opening = injectVars(returningTemplate, {
          GREETING: g.text,
          BOT_NAME: settings.BOT_NAME,
          BUSINESS_NAME: settings.BUSINESS_NAME,
          CALLER_NAME: memoryRow.name,
        });
      } else {
        opening = `${g.text}, ${memoryRow.name}, נעים לשמוע ממך שוב. איך נוכל לעזור?`;
      }
    } else {
      opening = injectVars(openingTemplate, {
        GREETING: g.text,
        BOT_NAME: settings.BOT_NAME,
        BUSINESS_NAME: settings.BUSINESS_NAME,
      });
    }

    const settingsContext = buildSettingsContext(settings);

    const master = injectVars(prompts.MASTER_PROMPT || "", settings);
    const guard = injectVars(prompts.GUARDRAILS_PROMPT || "", settings);
    const kb = injectVars(prompts.KB_PROMPT || "", settings);
    const leadCapture = injectVars(prompts.LEAD_CAPTURE_PROMPT || "", settings);

    const hardNoHallucinationLayer = [
      "חוק על: SETTINGS_CONTEXT הוא מקור האמת היחיד לפרטי המשרד והערכים העסקיים.",
      "אסור להשתמש בשום מידע שלא מופיע ב-SETTINGS_CONTEXT.",
      "אם נשאלת שאלה שאין לה ערך מפורש ב-SETTINGS_CONTEXT - השתמשי ב-NO_DATA_MESSAGE מתוך SETTINGS ואז חזרי לשיחה.",
      "OPENING_SCRIPT: כאשר משתמשים בו - יש לומר מילה במילה ללא שינוי.",
      "אין לטעון שמרגריטה עזבה/אינה עובדת; מותר רק לומר שהיא לא זמינה כרגע.",
    ].join(" ");

    const instructions = [
      master,
      guard,
      kb,
      leadCapture,
      hardNoHallucinationLayer,
      "SETTINGS_CONTEXT (Key=Value):\n" + settingsContext,
    ].join("\n\n");

    const session = {
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
      max_response_output_tokens: "inf",
    };

    if (MB_TRANSCRIPTION_MODEL) {
      session.input_audio_transcription = {
        model: MB_TRANSCRIPTION_MODEL,
        language: MB_TRANSCRIPTION_LANGUAGE,
      };
    }

    safeSend(openaiWs, { type: "session.update", session });
    sessionConfigured = true;

    // CALL_LOG
    sendCallLogOnce().catch(() => {});

    // Opening: speak immediately (no extra pre-say)
    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: "דברי עכשיו את הטקסט הבא ללא תוספות לפני/אחרי, באותו ניסוח בדיוק:\n" + opening,
      },
    });

    if (MB_DEBUG) {
      const tSinceConn = Date.now() - connT0;
      const tSinceStart = twilioStartAt ? Date.now() - twilioStartAt : null;
      console.log("[LATENCY] opening response.create sent", {
        ms_since_ws_connection: tSinceConn,
        ms_since_twilio_start: tSinceStart,
      });
    }

    responseInFlight = true;
    assistantSpeaking = true;
    flushAudioQueue(openaiWs);
    if (pendingCreate) maybeCreateResponse(openaiWs, "pending_after_open");
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // capture user utterance
    captureUserUtteranceFromTranscriptionEvent(msg);

    // STT logs + keep lastUserUtterance fresh
    if (MB_LOG_TRANSCRIPTS) {
      if (msg && typeof msg.type === "string" && msg.type.toLowerCase().includes("transcription")) {
        if (typeof msg.delta === "string") sttBuf += msg.delta;

        if (typeof msg.text === "string") {
          logSTTLine(msg.text);
          sttBuf = "";
        }
        if (typeof msg.transcript === "string") {
          logSTTLine(msg.transcript);
          sttBuf = "";
        }

        if (
          msg.type.toLowerCase().includes("done") ||
          msg.type.toLowerCase().includes("completed") ||
          msg.type.toLowerCase().includes("result")
        ) {
          if (sttBuf.trim()) logSTTLine(sttBuf);
          sttBuf = "";
        }
      }
    }

    // BARGE-IN hooks
    if (msg.type === "input_audio_buffer.speech_started") {
      onSpeechStarted(openaiWs);
      return;
    }
    if (msg.type === "input_audio_buffer.speech_stopped") {
      onSpeechStopped();

      if (lastUserUtterance) {
        // Additive: keep a minimal user transcript line
        maybeAppendUserToConversationLog(lastUserUtterance);
        applyDeterministicGatesFromUserUtterance(lastUserUtterance);
      }

      if (closingForced) {
        beginCallEnding("closing_forced", openaiWs, twilioWs, { sendClosing: true });
        decideAndSendOnEnd("closing_forced").catch(() => {});
        return;
      }

      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    // Assistant text logs + deterministic gate detection + INFO capture
    // NOTE: regardless of logging, we always capture assistant audio transcript to conversationLog
    if (msg.type === "response.audio_transcript.delta" && typeof msg.delta === "string") {
      botTranscriptLineBuf += msg.delta;

      if (MB_LOG_ASSISTANT_TEXT) {
        logAsstAudioTranscriptDelta(msg.delta);
      }

      const d = msg.delta;

      // INFO answer capture: first assistant answer after infoRequested
      if (infoRequested && !infoProvided) {
        // Start capturing the assistant answer for INFO (only first answer block)
        infoProvided = true;
        infoAnswerCaptureActive = true;
        if (MB_DEBUG) console.log("[INFO] provided=true (assistant started answering)");
      }
      if (infoAnswerCaptureActive) {
        if (infoAnswerText.length < infoAnswerCharsMax) {
          infoAnswerText += d;
        }
      }

      if (/מה הנושא|מה תרצה שאעביר|מה תרצו שאעביר|מה למסור/.test(d)) {
        expectingMessage = true;
        if (MB_DEBUG) console.log("[GATE] expecting_message");
      }

      if (/נוח לחזור למספר שממנו התקשר/.test(d)) {
        callbackRequested = true;
        expectingCallbackConfirm = true;
        if (MB_DEBUG) console.log("[GATE] expecting_callback_confirm");
      }

      if (/מה המספר|מספר טלפון|מספר לחזרה/.test(d)) {
        if (callbackRequested && !callbackToNumber) {
          expectingCallbackNumber = true;
          if (MB_DEBUG) console.log("[GATE] expecting_callback_number");
        }
      }
    }

    if (MB_LOG_ASSISTANT_TEXT) {
      if (msg.type === "response.output_text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.text.delta" && typeof msg.delta === "string") {
        logAsstDelta(msg.delta);
      }
      if (msg.type === "response.output_text.done" || msg.type === "response.text.done") {
        flushAsstLine();
      }
    }

    if (msg.type === "response.audio_transcript.done") {
      if (MB_LOG_ASSISTANT_TEXT) flushAsstAudioTranscriptLine();

      // Additive: push assistant transcript "line"
      const line = String(botTranscriptLineBuf || "").trim();
      if (line) maybeAppendBotToConversationLog(line);
      botTranscriptLineBuf = "";

      if (
        !capturedName &&
        /\b(מה השם|מה שמך|איך אפשר לפנות|שם בבקשה|אפשר שם|עם מי אני מדבר|עם מי אני מדברת)\b/.test(line)
      ) {
        expectingName = true;
        assistantAskedNameAt = Date.now();
        if (MB_DEBUG) console.log("[NAME] expecting_name");
      }

      // Stop INFO capture after the first assistant answer block ends
      if (infoAnswerCaptureActive) infoAnswerCaptureActive = false;
    }

    if (msg.type === "response.completed") {
      if (MB_LOG_ASSISTANT_TEXT) {
        flushAsstLine();
        flushAsstAudioTranscriptLine();
      }

      // Additive: if we didn't get audio_transcript.done (edge), flush any pending bot transcript
      const line = String(botTranscriptLineBuf || "").trim();
      if (line) maybeAppendBotToConversationLog(line);
      botTranscriptLineBuf = "";

      if (
        !capturedName &&
        /\b(מה השם|מה שמך|איך אפשר לפנות|שם בבקשה|אפשר שם|עם מי אני מדבר|עם מי אני מדברת)\b/.test(line)
      ) {
        expectingName = true;
        assistantAskedNameAt = Date.now();
        if (MB_DEBUG) console.log("[NAME] expecting_name");
      }

      if (infoAnswerCaptureActive) infoAnswerCaptureActive = false;
    }

    if (msg.type === "response.audio.done" || msg.type === "response.completed") {
      responseInFlight = false;
      assistantSpeaking = false;
      if (closingResponsePending) {
        closingResponsePending = false;
        finalizeCallAfterClosing("closing_audio_done", openaiWs, twilioWs);
      }
      return;
    }

    // AUDIO OUT (DO NOT TOUCH) – but allow drop window after barge-in cancel
    if (msg.type === "response.audio.delta" && streamSid) {
      assistantSpeaking = true;

      if (callEnded) return;
      if (callEnding && !closingResponsePending) return;
      if (dropAudioUntilTs && Date.now() < dropAudioUntilTs) return;

      try {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload: msg.delta },
          })
        );
      } catch (e) {
        console.error("[TWILIO] send media failed", e && (e.message || e));
      }
    }
  });

  openaiWs.on("error", (e) => {
    responseInFlight = false;
    assistantSpeaking = false;
    console.error("[OPENAI] ws error", e && (e.message || e));
  });

  openaiWs.on("close", (code, reason) => {
    responseInFlight = false;
    assistantSpeaking = false;
    if (MB_DEBUG) console.log("[OPENAI] ws closed", { code, reason: String(reason || "") });
  });

  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;
      twilioStartAt = Date.now();

      const callerRaw = msg.start?.customParameters?.caller || msg.start?.caller || null;
      const calledRaw = msg.start?.customParameters?.called || msg.start?.called || null;

      callerE164 = memory.normalizeE164(callerRaw);
      calledE164 = memory.normalizeE164(calledRaw);

      if (MB_DEBUG) {
        console.log("[WS] start", {
          accountSid: msg.start?.accountSid,
          streamSid,
          callSid,
          tracks: msg.start?.tracks,
          mediaFormat: msg.start?.mediaFormat,
          customParameters: msg.start?.customParameters || {},
        });
      }

      // NEW (v2 recording): start recording at call start (best effort; does not change flow)
      if (MB_ENABLE_RECORDING && callSid) {
        startRecordingIfEnabled(callSid, "RECORDING_V2_START").catch(() => {});
      }

      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;
      if (callEnding || callEnded) return;

      // Half-duplex: drop user audio while assistant is speaking
      if (MB_HALF_DUPLEX && assistantSpeaking) {
        return;
      }

      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
        audioQueue.push(payload);
        if (audioQueue.length > MAX_QUEUE_FRAMES) {
          audioQueue.splice(0, audioQueue.length - MAX_QUEUE_FRAMES);
        }
        return;
      }

      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
      callEndedAtIso = nowIso();
      beginCallEnding("twilio_stop", openaiWs, twilioWs, { sendClosing: false });

      if (MB_DEBUG) console.log("[WS] stop", { callSid: msg.stop?.callSid || callSid });

      decideAndSendOnEnd("twilio_stop").catch(() => {});
      return;
    }
  });

  // P0: also decide on WS close (in case stop is missed)
  twilioWs.on("close", () => {
    if (MB_DEBUG) console.log("[WS] closed");
    if (!callEndedAtIso) {
      callEndedAtIso = nowIso();
      decideAndSendOnEnd("twilio_close").catch(() => {});
    }
    beginCallEnding("twilio_close", openaiWs, twilioWs, { sendClosing: false });
  });

  twilioWs.on("error", (e) => {
    console.error("[TWILIO] ws error", e && (e.message || e));
    beginCallEnding("twilio_error", openaiWs, twilioWs, { sendClosing: false });
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1011, "twilio_error");
    } catch {}
  });
});

server.listen(PORT, () => {
  const base = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
  console.log(`==> Service live on port ${PORT}`);
  console.log(`==> Health: ${base}/health`);
});
