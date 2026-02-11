"use strict";

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { db, pool } = require("../db");
const {
  recomputeProductDynamicScoreSmartphones,
} = require("../utils/hookScore");

async function main() {
  await db.waitForConnection(
    Number(process.env.DB_CONN_RETRIES) || 5,
    Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000,
  );

  const result = await recomputeProductDynamicScoreSmartphones(db);
  console.log(JSON.stringify(result, null, 2));
}

main()
  .then(async () => {
    await pool.end();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("Recompute Hook Scores failed:", err);
    try {
      await pool.end();
    } catch (_) {
      // ignore
    }
    process.exit(1);
  });

