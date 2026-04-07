"use strict";

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { db, pool } = require("../db");
const {
  recomputeSmartphoneCompetitorAnalysis,
} = require("../utils/competitorAnalysis");

async function main() {
  await db.waitForConnection(
    Number(process.env.DB_CONN_RETRIES) || 5,
    Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000,
  );

  const limitRaw = Number(process.env.COMPETITOR_ANALYSIS_LIMIT);
  const limit = Number.isFinite(limitRaw)
    ? Math.min(10, Math.max(1, Math.floor(limitRaw)))
    : 3;

  const result = await recomputeSmartphoneCompetitorAnalysis(db, { limit });
  console.log(
    JSON.stringify(
      {
        ok: true,
        limit,
        result,
      },
      null,
      2,
    ),
  );
}

main()
  .then(async () => {
    await pool.end();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("Recompute competitors failed:", err);
    try {
      await pool.end();
    } catch (_) {
      // ignore
    }
    process.exit(1);
  });
