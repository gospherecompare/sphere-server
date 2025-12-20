require("dotenv").config();
const { Pool } = require("pg");

const connectionString = process.env.DATABASE_URL;

// AWS RDS usually REQUIRES SSL
const useSsl =
  process.env.PGSSLMODE === "require" || process.env.NODE_ENV === "production";

// Timeouts (good defaults for cloud DBs)
const connectionTimeoutMillis = Number(process.env.PG_CONN_TIMEOUT_MS) || 30000; // 30s
const idleTimeoutMillis = Number(process.env.PG_IDLE_TIMEOUT_MS) || 60000; // 60s

const pool = new Pool({
  connectionString,
  ssl: useSsl
    ? {
        rejectUnauthorized: false, // required for AWS RDS
      }
    : false,
  connectionTimeoutMillis,
  idleTimeoutMillis,
  max: Number(process.env.PG_POOL_MAX) || 10, // IMPORTANT for RDS
});

// Prevent crashing on idle errors
// Log pool errors but do not exit the process immediately — allow retries
pool.on("error", (err) => {
  console.error("Unexpected PostgreSQL pool error", err);
});

// Helper to wait for DB connection with retries (used at startup)
async function waitForConnection(retries = 5, delayMs = 5000) {
  let lastErr = null;
  for (let i = 0; i < retries; i++) {
    try {
      const r = await pool.query("SELECT 1");
      return r.rows[0];
    } catch (err) {
      lastErr = err;
      console.warn(
        `DB connection attempt ${i + 1} failed:`,
        err.message || err
      );
      await new Promise((res) => setTimeout(res, delayMs));
    }
  }
  throw lastErr || new Error("Failed to connect to DB");
}

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool,
  connect: () => pool.connect(),
  testConnection: async () => {
    const res = await pool.query("SELECT NOW()");
    return res.rows[0];
  },
  waitForConnection,
};
