// memoryDb.js
// Index Betty – Memory DB module
// Fixes the "caller_key" issue by treating caller_key as the canonical PK.
// Auto-migrates safely and supports existing/broken schemas.

let Pg;
try {
  Pg = require("pg");
} catch {
  Pg = null;
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeE164(s) {
  const t = String(s || "").trim();
  if (!t) return null;
  if (t.startsWith("+") && t.length >= 8) return t;
  const digits = t.replace(/[^\d+]/g, "");
  if (digits.startsWith("+") && digits.length >= 8) return digits;
  return null;
}

function createMemoryDb({ url, debug }) {
  const state = {
    enabled: false,
    ready: false,
    error: null,
    last_ok_at: null,
    pool: null,
    schema: {
      hasCallerKey: false,
      hasCallerIdE164: false,
      hasName: false,
      hasCallCount: false,
      hasLastCallAt: false,
    },
  };

  function enabled() {
    return !!(url && Pg);
  }

  async function query(sql, params) {
    return state.pool.query(sql, params);
  }

  async function detectColumns() {
    const res = await query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'caller_memory'
      `,
      []
    );
    const cols = new Set((res.rows || []).map((r) => r.column_name));

    state.schema.hasCallerKey = cols.has("caller_key");
    state.schema.hasCallerIdE164 = cols.has("caller_id_e164");
    state.schema.hasName = cols.has("name");
    state.schema.hasCallCount = cols.has("call_count");
    state.schema.hasLastCallAt = cols.has("last_call_at");
  }

  async function ensureSchema() {
    // Create the table in the NEW canonical shape:
    // caller_key is the PK (matches what your DB currently enforces).
    await query(`
      CREATE TABLE IF NOT EXISTS caller_memory (
        caller_key TEXT PRIMARY KEY,
        caller_id_e164 TEXT,
        name TEXT,
        call_count INTEGER DEFAULT 0,
        last_call_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    await detectColumns();

    // Add missing columns safely (works even if the table existed)
    const alters = [];

    if (!state.schema.hasCallerKey) alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS caller_key TEXT;`);
    if (!state.schema.hasCallerIdE164)
      alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS caller_id_e164 TEXT;`);
    if (!state.schema.hasName) alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS name TEXT;`);
    if (!state.schema.hasCallCount)
      alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS call_count INTEGER DEFAULT 0;`);
    if (!state.schema.hasLastCallAt)
      alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS last_call_at TIMESTAMPTZ;`);

    alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();`);
    alters.push(`ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`);

    for (const sql of alters) {
      await query(sql);
    }

    // Helpful indexes (safe)
    await query(`CREATE INDEX IF NOT EXISTS idx_caller_memory_last_call_at ON caller_memory (last_call_at);`);
    await query(`CREATE INDEX IF NOT EXISTS idx_caller_memory_caller_id_e164 ON caller_memory (caller_id_e164);`);

    // If some old rows had caller_id_e164 but caller_key is null, backfill caller_key
    // (This is the exact error you hit: null value in caller_key violates not-null constraint)
    await query(`
      UPDATE caller_memory
      SET caller_key = COALESCE(caller_key, caller_id_e164),
          updated_at = NOW()
      WHERE caller_key IS NULL AND caller_id_e164 IS NOT NULL;
    `);

    state.last_ok_at = nowIso();
  }

  async function init() {
    state.enabled = enabled();

    if (!state.enabled) {
      state.ready = false;
      state.error = url && !Pg ? "pg module is not available (missing dependency)." : null;
      return state;
    }

    if (state.pool) return state;

    try {
      const { Pool } = Pg;
      state.pool = new Pool({
        connectionString: url,
        ssl: url.includes("render.com") ? { rejectUnauthorized: false } : undefined,
        max: 2,
        idleTimeoutMillis: 30_000,
        connectionTimeoutMillis: 10_000,
      });

      await state.pool.query("SELECT 1");
      await ensureSchema();

      state.ready = true;
      state.error = null;
      if (debug) console.log("[MEMORY_DB] ready");
    } catch (e) {
      state.ready = false;
      state.error = e && (e.message || String(e));
      console.error("[MEMORY_DB] init failed", state.error);
    }

    return state;
  }

  function canonicalKeyFromCaller(callerE164) {
    const e164 = normalizeE164(callerE164);
    if (!e164) return null;
    return e164; // caller_key == e164
  }

  async function lookup(callerE164) {
    try {
      const key = canonicalKeyFromCaller(callerE164);
      if (!key) return null;
      await init();
      if (!state.ready || !state.pool) return null;

      const res = await query(
        `
        SELECT caller_key, caller_id_e164, name, call_count, last_call_at
        FROM caller_memory
        WHERE caller_key = $1
        LIMIT 1
        `,
        [key]
      );

      return res.rows?.[0] || null;
    } catch (e) {
      console.error("[MEMORY] lookup failed", e && (e.message || String(e)));
      return null;
    }
  }

  async function upsertCall(callerE164) {
    try {
      const key = canonicalKeyFromCaller(callerE164);
      if (!key) return;
      await init();
      if (!state.ready || !state.pool) return;

      await query(
        `
        INSERT INTO caller_memory (caller_key, caller_id_e164, call_count, last_call_at, created_at, updated_at)
        VALUES ($1, $1, 1, NOW(), NOW(), NOW())
        ON CONFLICT (caller_key)
        DO UPDATE SET
          caller_id_e164 = COALESCE(caller_memory.caller_id_e164, EXCLUDED.caller_id_e164),
          call_count = COALESCE(caller_memory.call_count, 0) + 1,
          last_call_at = NOW(),
          updated_at = NOW()
        `,
        [key]
      );

      state.last_ok_at = nowIso();
      if (debug) console.log("[MEMORY] upsert_call ok", { caller: key });
    } catch (e) {
      console.error("[MEMORY] upsert_call failed", e && (e.message || String(e)));
    }
  }

  async function saveName(callerE164, name) {
    try {
      const key = canonicalKeyFromCaller(callerE164);
      const n = String(name || "").trim();
      if (!key || !n) return;

      await init();
      if (!state.ready || !state.pool) return;

      await query(
        `
        INSERT INTO caller_memory (caller_key, caller_id_e164, name, call_count, last_call_at, created_at, updated_at)
        VALUES ($1, $1, $2, 1, NOW(), NOW(), NOW())
        ON CONFLICT (caller_key)
        DO UPDATE SET
          name = EXCLUDED.name,
          caller_id_e164 = COALESCE(caller_memory.caller_id_e164, EXCLUDED.caller_id_e164),
          updated_at = NOW()
        `,
        [key, n]
      );

      state.last_ok_at = nowIso();
      if (debug) console.log("[MEMORY] name_saved ok", { caller: key, name: n });
    } catch (e) {
      console.error("[MEMORY] name_saved failed", e && (e.message || String(e)));
    }
  }

  return {
    state,
    init,
    lookup,
    upsertCall,
    saveName,
    normalizeE164,
  };
}

module.exports = { createMemoryDb };
