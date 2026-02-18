"use strict";

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { db, pool } = require("../db");
const {
  recomputeProductDynamicScoreSmartphones,
  recomputeProductDynamicScoreLaptops,
  recomputeProductDynamicScoreTVs,
} = require("../utils/hookScore");

async function main() {
  await db.waitForConnection(
    Number(process.env.DB_CONN_RETRIES) || 5,
    Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000,
  );

  const smartphones = await recomputeProductDynamicScoreSmartphones(db);
  const laptops = await recomputeProductDynamicScoreLaptops(db);
  const tvs = await recomputeProductDynamicScoreTVs(db);
  console.log(
    JSON.stringify(
      {
        ok: true,
        updated:
          (smartphones.updated || 0) +
          (laptops.updated || 0) +
          (tvs.updated || 0),
        results: {
          smartphones,
          laptops,
          tvs,
        },
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
    console.error("Recompute Hook Scores failed:", err);
    try {
      await pool.end();
    } catch (_) {
      // ignore
    }
    process.exit(1);
  });
