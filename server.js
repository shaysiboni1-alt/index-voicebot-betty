// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// Aligned to SSOT PROMPTS + SETTINGS + INTENTS provided by user.
// Fixes:
// 1) STT logs only on FINAL transcripts (no char-by-char).
// 2) Avoid "conversation_already_has_active_response": release responseInFlight ONLY on response.completed.
// 3) Deterministic KB enforcement: office info + business info only from SETTINGS and only if explicitly asked.
// 4) INTENTS parsing aligned to user's sheet: trigger_type = keyword|phrase|default|fallback; triggers_he comma-separated.
// 5) Preserve baseline: SSOT preload, exact opening, streamSid fallback, queues, server_vad.

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const PORT = process.env.PORT || 10000;

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim(); // required for STT logs + intent/name
const MB_TRANSCRIPTION_LANGUAGE = (process.env.MB_TRANSCRIPTION_LANGUAGE || "he").trim();

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD || 0.65);
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS || 900);
const MB_VAD_PREFIX_MS = Number(process.env.MB_VAD_PREFIX_MS || 200);

// SSOT ENV
const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();
const SSOT_CACHE_TTL_MS = Number(process.env.SSOT_CACHE_TTL_MS || 60_000);
const SSOT_REFRESH_MS = Number(process.env.SSOT_REFRESH_MS || 60_000);

// Debug
const MB_LOG_TRANSCRIPTS = String(process.env.MB_LOG_TRANSCRIPTS || "true").toLowerCase() === "true";
const MB_LOG_ASSISTANT_TEXT = String(process.env.MB_LOG_ASSISTANT_TEXT || "true").toLowerCase() === "true";
const MB_DEBUG_TWILIO_EVENTS = String(process.env.MB_DEBUG_TWILIO_EVENTS || "true").toLowerCase() === "true";

if (!OPENAI_API_KEY) {
  console.error("[FATAL] Missing OPENAI_API_KEY");
  process.exit(1);
}

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

// ===== SSOT cache =====
let ssotCache = {
  loadedAt: 0,
  settings: {},
  prompts: {},
  intents: [],
  intentSuggestions: [],
  ok: false,
  error: null,
};

function getSheetsClient() {
  const json = Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8");
  const creds = JSON.parse(json);
  const auth = new google.auth.JWT(
    creds.client_email,
    null,
    creds.private_key,
    ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  );
  return google.sheets({ version: "v4", auth });
}

async function readRange(sheets, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GSHEET_ID,
    range,
  });
  return res.data.values || [];
}

async function loadSSOT(force = false) {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) {
    ssotCache = {
      ...ssotCache,
      ok: false,
      error: "SSOT env missing (GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64)",
      loadedAt: Date.now(),
    };
    return ssotCache;
  }

  const now = Date.now();
  if (!force && ssotCache.ok && now - ssotCache.loadedAt < SSOT_CACHE_TTL_MS) return ssotCache;

  try {
    const sheets = getSheetsClient();

    const settingsRows = await readRange(sheets, "SETTINGS!A:B");
    const settings = {};
    settingsRows.slice(1).forEach(([key, value]) => {
      if (key) settings[String(key).trim()] = (value || "").toString().trim();
    });

    const promptsRows = await readRange(sheets, "PROMPTS!A:B");
    const prompts = {};
    promptsRows.slice(1).forEach(([id, content]) => {
      if (id) prompts[String(id).trim()] = (content || "").toString().trim();
    });

    // USER'S INTENTS SHAPE:
    // intent | priority | trigger_type | triggers_he
    const intentsRows = await readRange(sheets, "INTENTS!A:D");
    const intents = intentsRows
      .slice(1)
      .map((row) => ({
        intent: (row[0] || "").toString().trim(),
        priority: Number(row[1] || 0),
        trigger_type: (row[2] || "").toString().trim(), // keyword|phrase|default|fallback
        triggers_he: (row[3] || "").toString().trim(),  // comma-separated
      }))
      .filter((x) => x.intent);

    const intentSuggestionsRows = await readRange(sheets, "INTENT_SUGGESTIONS!A:F");
    const intentSuggestions = intentSuggestionsRows.slice(1).map((row) => ({
      phrase_he: row[0],
      detected_intent: row[1],
      occurrences: row[2],
      last_seen_at: row[3],
      approved: row[4],
      notes: row[5],
    }));

    ssotCache = {
      loadedAt: now,
      settings,
      prompts,
      intents,
      intentSuggestions,
      ok: true,
      error: null,
    };

    console.log("[SSOT] loaded", {
      settings_count: Object.keys(settings).length,
      prompts_count: Object.keys(prompts).length,
      intents_count: intents.length,
    });

    return ssotCache;
  } catch (e) {
    ssotCache = {
      ...ssotCache,
      ok: false,
      error: e && (e.message || String(e)),
      loadedAt: Date.now(),
    };
    console.error("[SSOT] load failed", ssotCache.error);
    return ssotCache;
  }
}

// preload + refresh
(async () => {
  const ssot = await loadSSOT(true);
  console.log("[SSOT] preload", { ok: ssot.ok, error: ssot.error || null });
  setInterval(() => {
    loadSSOT(false).catch(() => {});
  }, SSOT_REFRESH_MS).unref();
})();

function buildInstructionsFromSSOT(ssot) {
  const fallback = [
    "את בטי, בוטית קולית נשית, אנרגטית, שמחה ושנונה.",
    "מדברת עברית טבעית, בלי אימוג׳ים ובלי סימנים מוקראים.",
    "שאלה אחת בלבד בכל תגובה.",
    "אין להמציא מידע; מידע משרדי/עסקי רק אם נשאלו ובדיוק מתוך SETTINGS.",
  ].join(" ");

  if (!ssot || !ssot.ok) return fallback;

  const { settings, prompts } = ssot;
  const merged = [prompts.MASTER_PROMPT, prompts.GUARDRAILS_PROMPT, prompts.KB_PROMPT, prompts.LEAD_CAPTURE_PROMPT]
    .filter(Boolean)
    .join(" ")
    .trim();

  if (!merged) return fallback;

  return merged
    .replace(/{BUSINESS_NAME}/g, settings.BUSINESS_NAME || "")
    .replace(/{BOT_NAME}/g, settings.BOT_NAME || "");
}

function buildOpeningFromSSOT(ssot) {
  const g = getGreetingBucketAndText();
  const fallback = `${g.text}, אני בטי הבוטית, העוזרת של מרגריטה. מה השם בבקשה?`;
  if (!ssot || !ssot.ok) return { text: fallback, bucket: g.bucket };

  const tpl = (ssot.settings.OPENING_SCRIPT || "").trim();
  if (!tpl) return { text: fallback, bucket: g.bucket };

  return { text: tpl.replace("{GREETING}", g.text), bucket: g.bucket };
}

// ===== Helpers (intent, kb detection, name) =====
function normalize(s) {
  return (s || "").toString().trim().toLowerCase();
}

function splitCommaList(s) {
  return (s || "")
    .toString()
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function detectEnglishPreference(transcript) {
  const s = (transcript || "").toString();
  if (/[A-Za-z]/.test(s)) return true;
  const t = s.toLowerCase();
  if (t.includes("english") || t.includes("in english") || t.includes("speak english")) return true;
  return false;
}

function looksLikeNameToken(tok) {
  if (!tok) return false;
  if (/\d/.test(tok)) return false;
  if (!/^[A-Za-z\u0590-\u05FF]{2,20}$/.test(tok)) return false;
  const bad = ["אני", "שמי", "קוראים", "לי", "זה", "היי", "שלום", "כן", "לא", "בטי", "מרגריטה", "ריטה"];
  if (bad.includes(tok)) return false;
  return true;
}

function extractNameSoft(transcript) {
  const raw = (transcript || "").toString().trim();
  if (!raw) return null;

  const patterns = [
    /קוראים\s+לי\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
    /שמי\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
    /אני\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
  ];
  for (const re of patterns) {
    const m = raw.match(re);
    if (m && m[1] && looksLikeNameToken(m[1])) return m[1];
  }

  const toks = raw
    .replace(/[^\w\u0590-\u05FF\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);

  if (toks.length === 1 && looksLikeNameToken(toks[0])) return toks[0];
  for (const tok of toks.slice(0, 4)) {
    if (looksLikeNameToken(tok)) return tok;
  }
  return null;
}

// Explicit KB questions detection (aligned to KB_PROMPT + SETTINGS keys)
function detectKbQuestion(transcript) {
  const t = normalize(transcript);

  const isHours = t.includes("שעות") || t.includes("שעות פעילות") || t.includes("מתי פתוח") || t.includes("מתי אתם פתוחים");
  const isAddress = t.includes("כתובת") || t.includes("איפה אתם") || t.includes("איפה נמצאים") || t.includes("מיקום") || t.includes("ממוקמים");
  const isPhone = t.includes("טלפון") || t.includes("מספר טלפון") || t.includes("מה המספר") || t.includes("להתקשר");
  const isEmail = t.includes("מייל") || t.includes("אימייל") || t.includes('דוא"ל') || t.includes("דואר אלקטרוני");

  const isBizDesc = t.includes("על העסק") || t.includes("על המשרד") || t.includes("מה אתם עושים") || t.includes("מה השירותים") || t.includes("שירותים") || t.includes("תיאור");
  const isOwner = t.includes("מי הבעלים") || t.includes("מי מרגריטה") || t.includes("מי הבעלת") || t.includes("מי היועצת");
  const isExperience = t.includes("ותק") || t.includes("כמה שנים");
  const isExpertise = t.includes("תחומי") || t.includes("התמחות") || t.includes("במה אתם מתמחים");
  const isRepresentation = t.includes("רשויות") || t.includes("מס הכנסה") || t.includes("ביטוח לאומי") || t.includes('מע"מ') || t.includes("ייצוג");
  const isSpecialNotes = t.includes("הערות") || t.includes("מיוחד") || t.includes("איסוף חומר") || t.includes("הרשות הפלסטינאית");

  // English minimal
  const isHoursEn = t.includes("hours") || t.includes("working hours") || t.includes("open");
  const isAddressEn = t.includes("address") || t.includes("location");
  const isPhoneEn = t.includes("phone") || t.includes("phone number");
  const isEmailEn = t.includes("email");

  return {
    isHours: isHours || isHoursEn,
    isAddress: isAddress || isAddressEn,
    isPhone: isPhone || isPhoneEn,
    isEmail: isEmail || isEmailEn,
    isBizDesc,
    isOwner,
    isExperience,
    isExpertise,
    isRepresentation,
    isSpecialNotes,
  };
}

function buildKbAnswerFromSettings(ssot, kb, userPrefEnglish) {
  const s = ssot?.ok ? ssot.settings : {};
  const noData = (s.NO_DATA_MESSAGE || "אין לי מידע זמין על זה כרגע.").trim();

  const langHe = !userPrefEnglish;

  const takeMessageQ = langHe
    ? "מה תרצו שאמסור למרגריטה?"
    : "What would you like me to pass to Margarita?";

  const askNameQ = langHe ? "מה השם בבקשה?" : "What’s your name, please?";

  const hours = (s.WORKING_HOURS || "").trim();
  const address = (s.BUSINESS_ADDRESS || "").trim();
  const phone = (s.MAIN_PHONE || "").trim();
  const email = (s.BUSINESS_EMAIL || "").trim();

  const businessDesc = (s.BUSINESS_DESCRIPTION || "").trim();
  const services = (s.BUSINESS_SERVICES_LIST || "").trim();
  const owner = (s.BUSINESS_OWNER || "").trim();
  const expYears = (s.BUSINESS_EXPERIENCE_YEARS || "").trim();
  const expertise = (s.BUSINESS_EXPERTISE || "").trim();
  const representation = (s.BUSINESS_AUTHORITIES_REPRESENTATION || "").trim();
  const specialNotes = (s.BUSINESS_SPECIAL_NOTES || "").trim();

  function lineOrNoData(labelHe, labelEn, val) {
    if (!val) return langHe ? noData : "I don’t have that information right now.";
    return langHe ? `${labelHe}: ${val}.` : `${labelEn}: ${val}.`;
  }

  // Office info
  if (kb.isHours) return { exact: `${lineOrNoData("שעות הפעילות", "Working hours", hours)} ${takeMessageQ}` };
  if (kb.isAddress) return { exact: `${lineOrNoData("הכתובת", "Address", address)} ${takeMessageQ}` };
  if (kb.isPhone) return { exact: `${lineOrNoData("מספר הטלפון", "Phone number", phone)} ${takeMessageQ}` };
  if (kb.isEmail) return { exact: `${lineOrNoData("האימייל", "Email", email)} ${takeMessageQ}` };

  // Business info (allowed ONLY if asked explicitly — this detector is that)
  if (kb.isOwner) return { exact: `${lineOrNoData("בעלת המשרד", "Office owner", owner)} ${takeMessageQ}` };
  if (kb.isExperience) return { exact: `${lineOrNoData("ותק", "Experience", expYears)} ${takeMessageQ}` };
  if (kb.isExpertise) return { exact: `${lineOrNoData("תחומי התמחות", "Expertise", expertise)} ${takeMessageQ}` };
  if (kb.isRepresentation) return { exact: `${lineOrNoData("ייצוג מול רשויות", "Representation", representation)} ${takeMessageQ}` };
  if (kb.isSpecialNotes) return { exact: `${lineOrNoData("הערות מיוחדות", "Special notes", specialNotes)} ${takeMessageQ}` };

  if (kb.isBizDesc) {
    // Keep short: description + optionally services in one breath if asked broadly
    const parts = [];
    if (businessDesc) parts.push(langHe ? businessDesc : businessDesc);
    if (services) parts.push(langHe ? `שירותים מרכזיים: ${services}` : `Main services: ${services}`);
    const merged = parts.length ? parts.join(". ") + "." : (langHe ? noData : "I don’t have that information right now.");
    // Return to flow: ask name if missing, else ask for message
    return { exact: `${merged} ${askNameQ}` };
  }

  return null;
}

// INTENT detection aligned to sheet
function detectIntentFromSheet(ssot, transcript, kb) {
  // Hard override: contact info questions => ask_contact_info
  if (kb?.isHours || kb?.isAddress || kb?.isPhone || kb?.isEmail) {
    return { intent: "ask_contact_info", matched: "kb_office_info" };
  }

  const t = normalize(transcript);
  const intents = (ssot?.ok ? ssot.intents : []) || [];
  if (!intents.length) return { intent: "other", matched: null };

  const sorted = intents.slice().sort((a, b) => (b.priority || 0) - (a.priority || 0));

  // First pass: keyword/phrase matching
  for (const row of sorted) {
    const type = normalize(row.trigger_type);
    if (type !== "keyword" && type !== "phrase") continue;

    const triggers = splitCommaList(row.triggers_he);
    for (const trig of triggers) {
      const tn = normalize(trig);
      if (!tn) continue;

      if (type === "keyword") {
        if (t.includes(tn)) return { intent: row.intent, matched: trig };
      } else {
        // phrase: still "contains" but longer phrases
        if (t.includes(tn)) return { intent: row.intent, matched: trig };
      }
    }
  }

  // default
  const def = sorted.find((x) => normalize(x.trigger_type) === "default");
  if (def) return { intent: def.intent, matched: "default" };

  // fallback
  const fb = sorted.find((x) => normalize(x.trigger_type) === "fallback");
  if (fb) return { intent: fb.intent, matched: "fallback" };

  return { intent: "other", matched: null };
}

// ===== Express =====
const app = express();
app.use(express.json());

app.get("/health", async (req, res) => {
  const ssot = await loadSSOT(false);
  res.json({
    ok: true,
    service: "index-betty-voicebot",
    ts: nowIso(),
    provider_mode: "openai",
    stt_enabled: !!MB_TRANSCRIPTION_MODEL,
    model: OPENAI_REALTIME_MODEL,
    voice: OPENAI_VOICE,
    ssot: {
      ok: ssot.ok,
      loaded_at: ssot.loadedAt,
      error: ssot.error,
      settings_count: ssot.ok ? Object.keys(ssot.settings).length : 0,
      prompts_count: ssot.ok ? Object.keys(ssot.prompts).length : 0,
      intents_count: ssot.ok ? ssot.intents.length : 0,
    },
  });
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs) => {
  let streamSid = null;
  let callSid = null;

  const logp = (...args) => {
    const prefix = callSid ? `[CALL ${callSid}]` : "[CALL -]";
    console.log(prefix, ...args);
  };
  const loge = (...args) => {
    const prefix = callSid ? `[CALL ${callSid}]` : "[CALL -]";
    console.error(prefix, ...args);
  };

  logp("[WS] connection established");

  const state = {
    name: null,
    user_prefers_english: false,
    last_user_transcript_final: "",
    intent: "other",
    intentMatched: null,
    greeting_time_bucket: getGreetingBucketAndText().bucket,
  };

  let openaiReady = false;
  let sessionConfigured = false;

  let openingLock = true;

  let responseInFlight = false; // IMPORTANT: released ONLY on response.completed
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  const outAudioQueue = [];
  const MAX_OUT_QUEUE_FRAMES = 400;

  let assistantTextBuf = "";

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(obj));
      return true;
    } catch (e) {
      loge("[OPENAI] send failed", e && (e.message || e));
      return false;
    }
  }

  function flushAudioQueue(openaiWs) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (!openaiReady || !sessionConfigured) return;
    while (audioQueue.length) {
      const payload = audioQueue.shift();
      userFramesSinceLastCreate += 1;
      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
    }
  }

  function flushOutAudioToTwilio() {
    if (!streamSid) return;
    while (outAudioQueue.length) {
      const payload = outAudioQueue.shift();
      try {
        twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload } }));
      } catch (e) {
        loge("[TWILIO] send media failed (flush)", e && (e.message || e));
        return;
      }
    }
  }

  function forceExactUtterance(openaiWs, exactText, reason) {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (!openaiReady || !sessionConfigured) return;

    responseInFlight = true;
    lastResponseCreateAt = Date.now();
    assistantTextBuf = "";

    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `אמרי מילה במילה בדיוק את המשפט הבא, בלי להוסיף ובלי לשנות אף מילה: ${exactText}`,
      },
    });

    logp("[TURN] forced exact response.create", reason || "forced");
  }

  function maybeCreateResponse(openaiWs, reason) {
    const now = Date.now();

    if (openingLock) return;
    if (responseInFlight) return;
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) return;

    const DEBOUNCE_MS = 900;
    const MIN_FRAMES = 8;
    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    const ssot = ssotCache;

    // 1) Deterministic KB answers from SETTINGS if explicitly asked
    const kb = detectKbQuestion(state.last_user_transcript_final);
    const kbAns = buildKbAnswerFromSettings(ssot, kb, state.user_prefers_english);
    if (kbAns?.exact) {
      // After giving KB info: if name missing, ask name (one question)
      if (!state.name) {
        // kbAns already ends with flow question; but for office info we want message flow, not name gate.
        // MASTER says opening asks name; if they evade, we can circle back later. Here keep it human:
        // Ask name only if the KB answer didn't already ask a question about the message.
      }
      forceExactUtterance(openaiWs, kbAns.exact, "kb_from_settings");
      // Set intent for contact info if it was office info
      if (kb.isHours || kb.isAddress || kb.isPhone || kb.isEmail) {
        state.intent = "ask_contact_info";
        state.intentMatched = "kb_office_info";
      }
      return;
    }

    // 2) Intent from INTENTS sheet (aligned to their trigger_type semantics)
    const det = detectIntentFromSheet(ssot, state.last_user_transcript_final, kb);
    state.intent = det.intent || "other";
    state.intentMatched = det.matched || null;

    // 3) Lead Gate (name required before any “closing/hand-off”; here we enforce when we need to proceed)
    // If name missing, ask for name exactly (short).
    if (!state.name) {
      const ask = state.user_prefers_english
        ? "Hi, I’m Betty, Margarita’s assistant. What’s your name, please?"
        : "היי, אני בטי, העוזרת של מרגריטה. מה השם בבקשה?";
      forceExactUtterance(openaiWs, ask, "lead_gate_name");
      return;
    }

    // 4) Special handling: reach_margarita => consistent line, then one open question
    if (state.intent === "reach_margarita") {
      const line = state.user_prefers_english
        ? `Margarita is currently busy, but I can help and pass everything accurately. What would you like me to tell her, ${state.name}?`
        : `מרגריטה עסוקה כרגע, אבל אני כאן לעזור ולהעביר הכול במדויק. מה תרצו שאמסור לה, ${state.name}?`;
      forceExactUtterance(openaiWs, line, "reach_margarita");
      return;
    }

    // 5) Special handling: reports_request => ask for details (one question)
    if (state.intent === "reports_request") {
      const q = state.user_prefers_english
        ? `Sure, ${state.name}. Which reports do you need, for which period, and who are they for?`
        : `בטח ${state.name}. איזה דוחות צריך, לאיזו תקופה, ולמי הם מיועדים?`;
      forceExactUtterance(openaiWs, q, "reports_request");
      return;
    }

    // 6) Default: let model respond but with strict constraints for this turn
    responseInFlight = true;
    assistantTextBuf = "";

    const turnInstructions = state.user_prefers_english
      ? `You are Betty, Margarita’s assistant. Caller name is "${state.name}". Be short, upbeat, and service-oriented. Ask exactly ONE question at the end. Do NOT invent any office/business info; only use SETTINGS if explicitly asked.`
      : `את בטי, העוזרת של מרגריטה. שם המתקשר הוא "${state.name}". תשובה קצרה, אנושית ושירותית, ושאלה אחת בלבד בסוף. אין להמציא מידע; מידע משרדי/עסקי רק אם נשאלו במפורש ורק מתוך SETTINGS.`;

    safeSend(openaiWs, {
      type: "response.create",
      response: { modalities: ["audio", "text"], instructions: turnInstructions },
    });

    logp("[TURN] response.create", reason || "speech_stopped", { intent: state.intent, matched: state.intentMatched });
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

  openaiWs.on("open", () => {
    logp("[OPENAI] ws open");
    openaiReady = true;

    const ssot = ssotCache;
    const instructions = buildInstructionsFromSSOT(ssot);

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

    // Opening exact (from SETTINGS.OPENING_SCRIPT)
    const openingObj = buildOpeningFromSSOT(ssot);
    state.greeting_time_bucket = openingObj.bucket;

    openingLock = true;
    responseInFlight = true;
    lastResponseCreateAt = Date.now();

    safeSend(openaiWs, {
      type: "conversation.item.create",
      item: { type: "message", role: "user", content: [{ type: "input_text", text: "התחילי בפתיח." }] },
    });

    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `אמרי מילה במילה בדיוק את המשפט הבא, בלי להוסיף ובלי לשנות אף מילה: ${openingObj.text}`,
      },
    });

    flushAudioQueue(openaiWs);
  });

  openaiWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // DO NOT cancel during opening (prevents cutting opening due to noise)
    if (msg.type === "input_audio_buffer.speech_started") {
      if (!openingLock && responseInFlight) {
        safeSend(openaiWs, { type: "response.cancel" });
        // responseInFlight will be released on response.completed OR we force-release here to allow next create
        responseInFlight = false;
        assistantTextBuf = "";
        logp("[TURN] response.cancel (barge-in)");
      }
      return;
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    // STT: accept only FINAL/completed (varies by model; we gate by "completed" keyword)
    if (
      MB_LOG_TRANSCRIPTS &&
      typeof msg.type === "string" &&
      msg.type.includes("transcription") &&
      msg.type.includes("completed")
    ) {
      const text =
        msg.transcript ||
        msg.text ||
        msg?.item?.content?.[0]?.transcript ||
        msg?.item?.content?.[0]?.text ||
        msg?.transcription ||
        null;

      if (text && typeof text === "string") {
        const cleaned = text.trim();
        if (cleaned) {
          state.last_user_transcript_final = cleaned;
          state.user_prefers_english =
            (ssotCache?.settings?.AUTO_EN_ENABLED || "").toString().toLowerCase() === "true"
              ? (state.user_prefers_english || detectEnglishPreference(cleaned))
              : state.user_prefers_english;

          logp("[STT][USER]", cleaned);

          if (!state.name) {
            const nm = extractNameSoft(cleaned);
            if (nm) {
              state.name = nm;
              logp("[LEAD] name captured:", state.name);
            }
          }
        }
      }
      return;
    }

    // Assistant text accumulation (best-effort)
    if (MB_LOG_ASSISTANT_TEXT) {
      if (typeof msg.type === "string" && msg.type.includes("text") && typeof msg.delta === "string") {
        assistantTextBuf += msg.delta;
      }
      if (typeof msg.type === "string" && msg.type.endsWith(".delta") && typeof msg.text === "string") {
        assistantTextBuf += msg.text;
      }
    }

    if (msg.type === "error") {
      loge("[OPENAI] error event", msg);
      responseInFlight = false;
      openingLock = false;
      return;
    }

    // IMPORTANT: release ONLY on response.completed
    if (msg.type === "response.completed") {
      responseInFlight = false;
      if (openingLock) openingLock = false;

      if (MB_LOG_ASSISTANT_TEXT) {
        const t = (assistantTextBuf || "").trim();
        if (t) logp("[LLM][ASSISTANT]", t.slice(0, 800));
        assistantTextBuf = "";
      }
      return;
    }

    // Audio out
    if (msg.type === "response.audio.delta") {
      const payload = msg.delta;
      if (!payload) return;

      if (!streamSid) {
        outAudioQueue.push(payload);
        if (outAudioQueue.length > MAX_OUT_QUEUE_FRAMES) {
          outAudioQueue.splice(0, outAudioQueue.length - MAX_OUT_QUEUE_FRAMES);
        }
        return;
      }

      try {
        twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload } }));
      } catch (e) {
        loge("[TWILIO] send media failed", e && (e.message || e));
      }
    }
  });

  openaiWs.on("error", (e) => {
    responseInFlight = false;
    openingLock = false;
    loge("[OPENAI] ws error", e && (e.message || e));
  });

  openaiWs.on("close", (code, reason) => {
    responseInFlight = false;
    openingLock = false;
    logp("[OPENAI] ws closed", { code, reason: String(reason || "") });
  });

  // Twilio inbound
  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (MB_DEBUG_TWILIO_EVENTS && msg.event && msg.event !== "media") {
      logp("[TWILIO] event", msg.event);
    }

    if (!streamSid && msg.streamSid) {
      streamSid = msg.streamSid;
      logp("[WS] streamSid derived from msg.streamSid", streamSid);
      flushOutAudioToTwilio();
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || streamSid || null;
      callSid = msg.start?.callSid || null;

      logp("[WS] start", {
        streamSid,
        callSid,
        tracks: msg.start?.tracks,
        mediaFormat: msg.start?.mediaFormat,
        customParameters: msg.start?.customParameters || {},
      });

      flushOutAudioToTwilio();
      return;
    }

    if (msg.event === "media") {
      const payload = msg.media?.payload;
      if (!payload) return;

      if (!streamSid && msg.streamSid) {
        streamSid = msg.streamSid;
        logp("[WS] streamSid derived from media", streamSid);
        flushOutAudioToTwilio();
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
      logp("[WS] stop", { callSid: msg.stop?.callSid || callSid });
      try {
        if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_stop");
      } catch {}
      return;
    }
  });

  twilioWs.on("close", () => {
    logp("[WS] closed");
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_close");
    } catch {}
  });

  twilioWs.on("error", (e) => {
    loge("[TWILIO] ws error", e && (e.message || e));
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
