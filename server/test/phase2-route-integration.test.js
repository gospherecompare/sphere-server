/**
 * Optional PostgreSQL-backed Phase 2 route consistency test.
 * Run with RUN_PHASE2_DB_INTEGRATION=true and the server DB environment set.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const integrationEnabled = process.env.RUN_PHASE2_DB_INTEGRATION === "true";

test(
  "Phase 2: compare resolve lifecycle matches smartphone detail lifecycle",
  { skip: !integrationEnabled },
  async () => {
    const { db, pool } = require("../db");
    const app = require("../index");
    const server = await new Promise((resolve) => {
      const listener = app.listen(0, () => resolve(listener));
    });

    try {
      const productResult = await db.query(`
        SELECT
          p.id,
          p.name,
          COUNT(DISTINCT pv.id)::int AS variant_count,
          COUNT(DISTINCT vsp.store_name)::int AS store_count
        FROM products p
        INNER JOIN smartphones s ON s.product_id = p.id
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        INNER JOIN product_variants pv ON pv.product_id = p.id
        INNER JOIN variant_store_prices vsp ON vsp.variant_id = pv.id
        WHERE p.product_type = 'smartphone'
        GROUP BY p.id, p.name
        HAVING COUNT(DISTINCT pv.id) >= 2
           AND COUNT(DISTINCT vsp.store_name) >= 2
        ORDER BY p.id DESC
        LIMIT 2
      `);

      assert.ok(
        productResult.rows.length >= 2,
        "integration fixture requires two published smartphones with two variants and two stores",
      );

      const [leftProduct, rightProduct] = productResult.rows;
      const slugify = (value) =>
        String(value || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/(^-|-$)/g, "");
      const baseUrl = `http://127.0.0.1:${server.address().port}`;

      const detailResponse = await fetch(
        `${baseUrl}/api/smartphone/${leftProduct.id}`,
      );
      assert.equal(detailResponse.status, 200);
      const detailBody = await detailResponse.json();

      const compareResponse = await fetch(
        `${baseUrl}/api/public/compare/resolve?left=${slugify(leftProduct.name)}&right=${slugify(rightProduct.name)}&type=smartphone`,
      );
      assert.equal(compareResponse.status, 200);
      const compareBody = await compareResponse.json();

      assert.equal(compareBody.matched, true);
      assert.deepEqual(compareBody.left.lifecycle, {
        ...detailBody.data.lifecycle,
      });
    } finally {
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
      await pool.end();
    }
  },
);
