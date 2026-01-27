// memoryDb.js
// Index Betty – Memory DB module (Postgres)
// Goals:
// - Safe init + auto-migration
// - Fix broken schemas: missing column, missing unique constraint for ON CONFLICT
// - Provide: init(), lookup(), upsertCall(), saveName()

let Pg;
try {
  Pg = require("pg");
} catch {
  Pg = null;
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
    pool: null,
    last_ok_at: null,
  };

  function enabled() {
    return !!(url && Pg);
  }

  async function init() {
    state.enabled = enabled();

    if (!state.enabled) {
      state.ready = false;
      if (url && !Pg) state.error = "pg module is not available (missing dependency).";
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

      // Create table if missing (correct schema)
      await state.pool.query(`
        CREATE TABLE IF NOT EXISTS caller_memory (
          caller_id_e164 TEXT PRIMARY KEY,
          name TEXT,
          call_count INTEGER DEFAULT 0,
          last_call_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);

      // Ensure columns exist (older broken schema safety)
      const alters = [
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS caller_id_e164 TEXT;`,
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS name TEXT;`,
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS call_count INTEGER DEFAULT 0;`,
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS last_call_at TIMESTAMPTZ;`,
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();`,
        `ALTER TABLE caller_memory ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`,
      ];
      for (const sql of alters) {
        await state.pool.query(sql);
      }

      // ✅ Fix "ON CONFLICT ... no unique constraint":
      // Some existing DBs may have the table without PK/unique on caller_id_e164.
      // We create a UNIQUE index if missing (safe even if PK already exists).
      await state.pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS ux_caller_memory_caller_id_e164
        ON caller_memory (caller_id_e164);
      `);

      await state.pool.query(`
        CREATE INDEX IF NOT EXISTS idx_caller_memory_last_call_at
        ON caller_memory (last_call_at);
      `);

      // Optional: if any rows have NULL caller_id_e164 due to past bad schema, clean them.
      await state.pool.query(`DELETE FROM caller_memory WHERE caller_id_e164 IS NULL OR caller_id_e164 = '';`);

      state.ready = true;
      state.error = null;
      state.last_ok_at = nowIso();
      if (debug) console.log("[MEMORY_DB] ready");
    } catch (e) {
      state.ready = false;
      state.error = e && (e.message || String(e));
      console.error("[MEMORY_DB] init failed", state.error);
    }

    return state;
  }

  async function lookup(callerE164) {
    try {
      const c = normalizeE164(callerE164);
      if (!c) return null;

      await init();
      if (!state.ready || !state.pool) return null;

      const res = await state.pool.query(
        `SELECT caller_id_e164, name, call_count, last_call_at FROM caller_memory WHERE caller_id_e164 = $1 LIMIT 1`,
        [c]
      );
      return res.rows?.[0] || null;
    } catch (e) {
      console.error("[MEMORY] lookup failed", e && (e.message || String(e)));
      return null;
    }
  }

  async function upsertCall(callerE164) {
    try {
      const c = normalizeE164(callerE164);
      if (!c) return;

      await init();
      if (!state.ready || !state.pool) return;

      await state.pool.query(
        `
        INSERT INTO caller_memory (caller_id_e164, call_count, last_call_at, created_at, updated_at)
        VALUES ($1, 1, NOW(), NOW(), NOW())
        ON CONFLICT (caller_id_e164)
        DO UPDATE SET
          call_count = COALESCE(caller_memory.call_count, 0) + 1,
          last_call_at = NOW(),
          updated_at = NOW()
        `,
        [c]
      );

      state.last_ok_at = nowIso();
    } catch (e) {
      console.error("[MEMORY] upsert_call failed", e && (e.message || String(e)));
    }
  }

  async function saveName(callerE164, name, debug) {
    try {
      const c = normalizeE164(callerE164);
      if (!c) return;

      const n = String(name || "").trim();
      if (!n) return;

      await init();
      if (!state.ready || !state.pool) return;

      await state.pool.query(
        `
        INSERT INTO caller_memory (caller_id_e164, name, call_count, last_call_at, created_at, updated_at)
        VALUES ($1, $2, 1, NOW(), NOW(), NOW())
        ON CONFLICT (caller_id_e164)
        DO UPDATE SET
          name = EXCLUDED.name,
          updated_at = NOW()
        `,
        [c, n]
      );

      state.last_ok_at = nowIso();
      if (debug) console.log("[MEMORY] name_saved", { caller: c, name: n });
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
