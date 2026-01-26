// server.js
// Index Betty – Realtime VoiceBot (OpenAI only)
// LOCKED baseline preserved: SSOT preload, exact opening, queues, streamSid fallback, VAD turn-taking.
// Added in this stage:
// - STT transcription logging (user) + assistant text logging
// - Intent detection from SSOT INTENTS
// - Lead Gate: name required (soft validation), never ask again after captured
// - Turn orchestration via response.create.response.instructions per turn

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

const MB_TRANSCRIPTION_MODEL = (process.env.MB_TRANSCRIPTION_MODEL || "").trim(); // required for STT logs
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

// ===== SSOT (Google Sheets) =====
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
  // Do NOT crash voicebot if SSOT env missing; fallback to baseline hardcoded behavior
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
  if (!force && ssotCache.ok && now - ssotCache.loadedAt < SSOT_CACHE_TTL_MS) {
    return ssotCache;
  }

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

    const intentsRows = await readRange(sheets, "INTENTS!A:E");
    const intents = intentsRows
      .slice(1)
      .map((row) => ({
        intent: (row[0] || "").toString().trim(),
        priority: Number(row[1] || 0),
        trigger_type: (row[2] || "").toString().trim(),
        triggers_he: (row[3] || "").toString().trim(),
        notes: (row[4] || "").toString().trim(),
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

function buildInstructionsFromSSOT(ssot) {
  // Baseline fallback (kept)
  const fallback = [
    "את בטי, בוטית קולית נשית, אנרגטית, שמחה ושנונה.",
    "מדברת עברית טבעית עם סלנג עדין, בלי אימוג׳ים ובלי סימנים מוקראים.",
    "כל תגובה קצרה וממוקדת, ובכל תגובה שאלה אחת בלבד.",
    "אם הלקוח מתחיל באנגלית או מבקש לעבור לאנגלית, עברי לאנגלית.",
    "אל תמציאי מידע משרדי; תמסרי מידע רק אם נשאלת עליו במפורש ובהמשך נמשוך אותו מה-SETTINGS.",
  ].join(" ");

  if (!ssot || !ssot.ok) return fallback;

  const { settings, prompts } = ssot;
  const merged = [prompts.MASTER_PROMPT, prompts.GUARDRAILS_PROMPT, prompts.KB_PROMPT]
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
  const fallback = `${g.text}, אני בטי הבוטית, העוזרת של מרגריטה. איך אפשר לעזור?`;

  if (!ssot || !ssot.ok) return { text: fallback, bucket: g.bucket };
  const { settings } = ssot;

  const tpl = (settings.OPENING_SCRIPT || "").trim();
  if (!tpl) return { text: fallback, bucket: g.bucket };

  return { text: tpl.replace("{GREETING}", g.text), bucket: g.bucket };
}

// ===== Intent + Name helpers =====
function normalizeForMatch(s) {
  return (s || "")
    .toString()
    .replace(/[.,!?;:"'(){}\[\]<>]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function splitTriggers(triggers) {
  // supports: comma, newline, | separators
  const raw = (triggers || "").toString();
  return raw
    .split(/\r?\n|,|\|/g)
    .map((t) => t.trim())
    .filter(Boolean);
}

function detectIntentFromSSOT(ssot, transcript) {
  const t = normalizeForMatch(transcript);
  if (!t) return { intent: "other", matched: null };

  const intents = (ssot?.ok ? ssot.intents : []) || [];
  if (!intents.length) return { intent: "other", matched: null };

  // sort by priority desc
  const sorted = intents.slice().sort((a, b) => (b.priority || 0) - (a.priority || 0));

  for (const row of sorted) {
    const type = (row.trigger_type || "contains").toLowerCase();
    const trigList = splitTriggers(row.triggers_he);

    for (const trig of trigList) {
      const trigN = normalizeForMatch(trig);
      if (!trigN) continue;

      if (type === "regex") {
        try {
          const re = new RegExp(trig, "i");
          if (re.test(transcript)) return { intent: row.intent, matched: trig };
        } catch {
          // ignore invalid regex
        }
      } else if (type === "starts_with") {
        if (t.startsWith(trigN)) return { intent: row.intent, matched: trig };
      } else {
        // contains (default)
        if (t.includes(trigN)) return { intent: row.intent, matched: trig };
      }
    }
  }

  return { intent: "other", matched: null };
}

function looksLikeNameToken(tok) {
  if (!tok) return false;
  if (/\d/.test(tok)) return false;
  // Allow Hebrew/English letters, avoid long phrases
  if (!/^[A-Za-z\u0590-\u05FF]{2,20}$/.test(tok)) return false;
  const bad = ["אני", "שמי", "קוראים", "לי", "זה", "היי", "שלום", "כן", "לא", "בטי", "מרגריטה"];
  if (bad.includes(tok)) return false;
  return true;
}

function extractNameSoft(transcript) {
  const raw = (transcript || "").toString().trim();
  if (!raw) return null;

  // Patterns:
  // "קוראים לי X", "שמי X", "אני X"
  const patterns = [
    /קוראים\s+לי\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
    /שמי\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
    /אני\s+([A-Za-z\u0590-\u05FF]{2,20})/i,
  ];
  for (const re of patterns) {
    const m = raw.match(re);
    if (m && m[1] && looksLikeNameToken(m[1])) return m[1];
  }

  // Single/first token fallback
  const toks = raw
    .replace(/[^\w\u0590-\u05FF\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);

  if (toks.length === 1 && looksLikeNameToken(toks[0])) return toks[0];

  // First reasonable token
  for (const tok of toks.slice(0, 4)) {
    if (looksLikeNameToken(tok)) return tok;
  }

  return null;
}

function detectEnglishPreference(transcript) {
  const s = (transcript || "").toString();
  if (/[A-Za-z]/.test(s)) return true;
  const t = s.toLowerCase();
  if (t.includes("english") || t.includes("speak english") || t.includes("in english")) return true;
  return false;
}

// ===== Preload SSOT on boot + periodic refresh =====
(async () => {
  const ssot = await loadSSOT(true);
  console.log("[SSOT] preload", { ok: ssot.ok, error: ssot.error || null });
  setInterval(() => {
    loadSSOT(false).catch(() => {});
  }, SSOT_REFRESH_MS).unref();
})();

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
  console.log("[WS] connection established");

  let streamSid = null;
  let callSid = null;

  // OpenAI session readiness gates
  let openaiReady = false;
  let sessionConfigured = false;

  // Turn-taking
  let pendingCreate = false;
  let responseInFlight = false;
  let openingLock = true;
  let lastResponseCreateAt = 0;
  let userFramesSinceLastCreate = 0;

  // Conversation state (in-memory for this call)
  const state = {
    name: null,
    nameAskedCount: 0,
    intent: "other",
    intentMatched: null,
    asked_for_margarita: false,
    callback_requested: false,
    contact_info_requested: false,
    reports_details: "",
    message: "",
    user_prefers_english: false,
    greeting_time_bucket: getGreetingBucketAndText().bucket,
    last_user_transcript: "",
  };

  // Queue Twilio audio until OpenAI WS is ready
  const audioQueue = [];
  const MAX_QUEUE_FRAMES = 400;

  // Outbound audio buffer until streamSid known
  const outAudioQueue = [];
  const MAX_OUT_QUEUE_FRAMES = 400;

  // Assistant text accumulation for logs
  let assistantTextBuf = "";

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
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload },
          })
        );
      } catch (e) {
        console.error("[TWILIO] send media failed (flush)", e && (e.message || e));
        return;
      }
    }
  }

  function buildTurnInstructions(ssot) {
    // Use SSOT prompts as base system policy (already in session.instructions),
    // and inject state as "local" constraints for THIS response.
    const lang = state.user_prefers_english ? "en" : "he";
    const nameKnown = !!state.name;

    // Minimal deterministic business logic for this stage
    const leadGate = nameKnown
      ? (lang === "he"
          ? `השם של המתקשר כבר ידוע: "${state.name}". אל תשאלי שוב על השם.`
          : `Caller name is known: "${state.name}". Do NOT ask again for the name.`)
      : (lang === "he"
          ? `חובה לאסוף שם (שם פרטי מספיק). אם אין שם עדיין, בקשי שם בצורה קצרה וברורה.`
          : `You must collect the caller's name (first name is enough). If missing, ask for the name briefly.`);

    const intentLine =
      lang === "he"
        ? `הכוונה הנוכחית: ${state.intent} (matched: ${state.intentMatched || "none"}).`
        : `Current intent: ${state.intent} (matched: ${state.intentMatched || "none"}).`;

    const styleLine =
      lang === "he"
        ? `סגנון: קצר, אנושי, אנרגטי, שאלה אחת בלבד בסוף. בלי אימוג'ים.`
        : `Style: short, human, upbeat. Exactly one question at the end. No emojis.`;

    const taskLine =
      lang === "he"
        ? `מטרה: להבין מה הלקוח צריך, ואם צריך—לקחת הודעה למרגריטה.`
        : `Goal: understand what the caller needs and, if needed, take a message for Margarita.`;

    // Avoid hallucinating office info: already in GUARDRAILS/KB, keep reminder
    const kbReminder =
      lang === "he"
        ? `מידע משרדי (שעות/כתובת/טלפון/מייל) מוסרים רק אם נשאלים במפורש ורק מ-SETTINGS.`
        : `Office info (hours/address/phone/email) only if explicitly asked and only from SETTINGS.`;

    return [leadGate, intentLine, taskLine, kbReminder, styleLine].join(" ");
  }

  function maybeCreateResponse(openaiWs, reason) {
    const now = Date.now();

    if (openingLock) return;
    if (responseInFlight) return;

    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
      pendingCreate = true;
      return;
    }

    const DEBOUNCE_MS = 1200;
    const MIN_FRAMES = 8;

    if (now - lastResponseCreateAt < DEBOUNCE_MS) return;
    if (userFramesSinceLastCreate < MIN_FRAMES) return;

    lastResponseCreateAt = now;
    userFramesSinceLastCreate = 0;

    // Per-turn instructions for orchestration
    const ssot = ssotCache;
    const turnInstructions = buildTurnInstructions(ssot);

    assistantTextBuf = "";
    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: turnInstructions,
      },
    });

    pendingCreate = false;
    responseInFlight = true;
    console.log("[TURN] response.create", reason || "speech_stopped");
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
    console.log("[OPENAI] ws open");
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

    // Enable STT (needed for transcript logs + intent/name)
    if (MB_TRANSCRIPTION_MODEL) {
      session.input_audio_transcription = {
        model: MB_TRANSCRIPTION_MODEL,
        language: MB_TRANSCRIPTION_LANGUAGE,
      };
    }

    safeSend(openaiWs, { type: "session.update", session });
    sessionConfigured = true;

    // Opening (exact, no paraphrase)
    const openingObj = buildOpeningFromSSOT(ssot);
    const openingText = openingObj.text;

    openingLock = true;
    responseInFlight = true;
    lastResponseCreateAt = Date.now();
    userFramesSinceLastCreate = 0;

    // Context item (light)
    safeSend(openaiWs, {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "התחילי בפתיח." }],
      },
    });

    safeSend(openaiWs, {
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `אמרי מילה במילה בדיוק את המשפט הבא, בלי להוסיף ובלי לשנות אף מילה: ${openingText}`,
      },
    });

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

    // Turn boundary
    if (msg.type === "input_audio_buffer.speech_stopped") {
      maybeCreateResponse(openaiWs, "speech_stopped");
      return;
    }

    // Handle STT events (best-effort across event variants)
    // We extract text from common shapes and update state.last_user_transcript.
    if (MB_LOG_TRANSCRIPTS && typeof msg.type === "string" && msg.type.includes("transcription")) {
      const text =
        msg.transcript ||
        msg.text ||
        msg?.item?.content?.[0]?.transcript ||
        msg?.item?.content?.[0]?.text ||
        msg?.delta ||
        null;

      if (text && typeof text === "string") {
        // Keep only meaningful, non-empty
        const cleaned = text.trim();
        if (cleaned) {
          state.last_user_transcript = cleaned;
          state.user_prefers_english = state.user_prefers_english || detectEnglishPreference(cleaned);

          // Update name (Lead Gate)
          if (!state.name) {
            const maybeName = extractNameSoft(cleaned);
            if (maybeName) {
              state.name = maybeName;
              console.log("[LEAD] name captured:", state.name);
            }
          }

          // Update intent
          const det = detectIntentFromSSOT(ssotCache, cleaned);
          state.intent = det.intent || "other";
          state.intentMatched = det.matched || null;

          // Minimal flags
          if (state.intent === "reach_margarita") state.asked_for_margarita = true;
          if (state.intent === "callback_request") state.callback_requested = true;
          if (state.intent === "ask_contact_info") state.contact_info_requested = true;
          if (state.intent === "reports_request") state.reports_details = cleaned;

          // message capture (for now keep last meaningful user statement)
          state.message = cleaned;

          console.log("[STT][USER]", cleaned);
        }
      }
    }

    // Assistant text accumulation (best-effort)
    if (MB_LOG_ASSISTANT_TEXT && typeof msg.type === "string" && msg.type.includes("text") && msg.delta) {
      if (typeof msg.delta === "string") assistantTextBuf += msg.delta;
    }
    if (MB_LOG_ASSISTANT_TEXT && typeof msg.type === "string" && msg.type.endsWith(".delta") && msg.text) {
      if (typeof msg.text === "string") assistantTextBuf += msg.text;
    }

    // OpenAI error
    if (msg.type === "error") {
      console.error("[OPENAI] error event", msg);
      responseInFlight = false;
      openingLock = false;
      return;
    }

    // Response done => unlock
    if (msg.type === "response.audio.done" || msg.type === "response.completed") {
      responseInFlight = false;

      if (openingLock) {
        openingLock = false;
        // After opening, if still no name, we will let next user turn trigger name gate.
      }

      if (MB_LOG_ASSISTANT_TEXT) {
        const t = (assistantTextBuf || "").trim();
        if (t) console.log("[LLM][ASSISTANT]", t.slice(0, 800));
        assistantTextBuf = "";
      }

      if (pendingCreate) {
        pendingCreate = false;
        maybeCreateResponse(openaiWs, "pending_after_completed");
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
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload },
          })
        );
      } catch (e) {
        console.error("[TWILIO] send media failed", e && (e.message || e));
      }
    }
  });

  openaiWs.on("error", (e) => {
    responseInFlight = false;
    openingLock = false;
    console.error("[OPENAI] ws error", e && (e.message || e));
  });

  openaiWs.on("close", (code, reason) => {
    responseInFlight = false;
    openingLock = false;
    console.log("[OPENAI] ws closed", { code, reason: String(reason || "") });
  });

  // ===== Twilio WS inbound =====
  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.event && msg.event !== "media") {
      console.log("[TWILIO] event", msg.event);
    }

    // Fallback: derive streamSid from top-level msg.streamSid
    if (!streamSid && msg.streamSid) {
      streamSid = msg.streamSid;
      console.log("[WS] streamSid derived from msg.streamSid", streamSid);
      flushOutAudioToTwilio();
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || streamSid || null;
      callSid = msg.start?.callSid || null;
      console.log("[WS] start", {
        accountSid: msg.start?.accountSid,
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

      // Derive streamSid from media if needed
      if (!streamSid && msg.streamSid) {
        streamSid = msg.streamSid;
        console.log("[WS] streamSid derived from media", streamSid);
        flushOutAudioToTwilio();
      }

      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN || !openaiReady || !sessionConfigured) {
        audioQueue.push(payload);
        if (audioQueue.length > MAX_QUEUE_FRAMES) {
          audioQueue.splice(0, audioQueue.length - MAX_QUEUE_FRAMES);
        }
        return;
      }

      // Count audio frames for gating
      userFramesSinceLastCreate += 1;

      safeSend(openaiWs, { type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
      if (!streamSid && msg.streamSid) {
        streamSid = msg.streamSid;
        console.log("[WS] streamSid derived from stop", streamSid);
        flushOutAudioToTwilio();
      }

      console.log("[WS] stop", { callSid: msg.stop?.callSid || callSid });
      try {
        if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_stop");
      } catch {}
      return;
    }
  });

  twilioWs.on("close", () => {
    console.log("[WS] closed");
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close(1000, "twilio_close");
    } catch {}
  });

  twilioWs.on("error", (e) => {
    console.error("[TWILIO] ws error", e && (e.message || e));
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
