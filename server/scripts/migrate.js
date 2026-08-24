const fs = require("fs");
const path = require("path");
const { db } = require("../db");

const migrationPath = path.join(
  __dirname,
  "..",
  "migrations",
  "ai_generated_content.sql",
);
const sql = fs.readFileSync(migrationPath, "utf-8");

(async () => {
  try {
    console.log("Executing ai_generated_content migration...");
    await db.query(sql);
    console.log("✅ Migration completed successfully!");
    process.exit(0);
  } catch (err) {
    console.error("❌ Migration failed:", err.message);
    process.exit(1);
  }
})();
