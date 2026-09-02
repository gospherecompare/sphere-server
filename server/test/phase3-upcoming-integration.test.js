/**
 * Optional PostgreSQL-backed Phase 3 upcoming-feed validation.
 * Run with RUN_PHASE3_DB_INTEGRATION=true and the server DB environment set.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const integrationEnabled = process.env.RUN_PHASE3_DB_INTEGRATION === "true";

test(
  "Phase 3: upcoming feed filters only by canonical launch stage",
  { skip: !integrationEnabled },
  async () => {
    const { pool } = require("../db");
    const app = require("../index");
    const server = await new Promise((resolve) => {
      const listener = app.listen(0, () => resolve(listener));
    });

    try {
      const response = await fetch(
        `http://127.0.0.1:${server.address().port}/api/public/upcoming/smartphones?limit=80`,
      );
      assert.equal(response.status, 200);

      const body = await response.json();
      const items = body.upcoming || body.smartphones || [];
      assert.ok(Array.isArray(items));

      for (const item of items) {
        assert.ok(item.lifecycle, "upcoming item must include lifecycle");
        assert.ok(
          ["rumored", "announced", "upcoming"].includes(
            item.lifecycle.launch.stage,
          ),
          `unexpected launch stage: ${item.lifecycle.launch.stage}`,
        );
        assert.equal(item.lifecycle.render.type, "upcoming");
        assert.equal(item.launch_status, item.lifecycle.launch.stage);
      }
    } finally {
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
      await pool.end();
    }
  },
);
