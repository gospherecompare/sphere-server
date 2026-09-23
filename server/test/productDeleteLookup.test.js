const test = require("node:test");
const assert = require("node:assert/strict");

const {
  resolveProductIdByEntityOrProductId,
} = require("../utils/productDeleteLookup");

test("resolveProductIdByEntityOrProductId accepts either product_id or table id", async () => {
  const client = {
    async query(sql, params) {
      if (sql.includes("WHERE product_id = $1")) {
        if (params[0] === 99) {
          return { rows: [] };
        }
        return { rows: [{ product_id: 42 }] };
      }
      if (sql.includes("WHERE id = $1")) {
        if (params[0] === 99) {
          return { rows: [{ product_id: 42 }] };
        }
        return { rows: [] };
      }
      return { rows: [] };
    },
  };

  const productId = await resolveProductIdByEntityOrProductId(client, {
    tableName: "tvs",
    targetId: 99,
  });

  assert.equal(productId, 42);
});

test("resolveProductIdByEntityOrProductId returns null when no matching record exists", async () => {
  const client = {
    async query() {
      return { rows: [] };
    },
  };

  const productId = await resolveProductIdByEntityOrProductId(client, {
    tableName: "tvs",
    targetId: 404,
  });

  assert.equal(productId, null);
});
