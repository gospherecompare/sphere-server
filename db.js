const { Pool } = require("pg");
require("dotenv").config();

const parseBool = (value, defaultValue = false) => {
  if (value === undefined || value === null || value === "") return defaultValue;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
};

const sslEnabled = parseBool(process.env.DB_SSL, true);
const sslRejectUnauthorized = parseBool(
  process.env.DB_SSL_REJECT_UNAUTHORIZED,
  true
);
const sslCa =
  process.env.DB_SSL_CA && process.env.DB_SSL_CA.replace(/\\n/g, "\n");

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT, 10) || 5432,
  // pool sizing and timeouts can be tuned via env
  max: parseInt(process.env.DB_POOL_MAX, 10) || 10,
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT_MS, 10) || 30000,
  connectionTimeoutMillis:
    parseInt(process.env.DB_CONN_TIMEOUT_MS, 10) || 10000,
  // enable SSL by default (set DB_SSL=false to disable)
  ssl: sslEnabled
    ? {
        rejectUnauthorized: sslRejectUnauthorized,
        ...(sslCa ? { ca: sslCa } : {}),
      }
    : false,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle PostgreSQL client", err);
});

async function waitForConnection(retries = 5, delayMs = 5000) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try {
      await pool.query("SELECT 1");
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw lastErr || new Error("Unable to connect to DB");
}

const db = {
  query: (text, params) => pool.query(text, params),
  connect: () => pool.connect(),
  waitForConnection,
};

module.exports = { pool, db };
