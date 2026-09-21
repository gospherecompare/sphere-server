"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  toCanonicalTvPayload,
  validateTvImageUrl,
  createTvCatalogRecord,
} = require("../services/tvCatalogService");

const input = {
  product_name: "LG OLED C4",
  brand_name: "LG",
  model: "OLED55C4PSA",
  category: "OLED TV",
  images_json: [{ url: "https://www.lg.com/content/dam/tv.jpg" }],
  display_json: { panel_type: "OLED" },
  variants: [
    {
      variant_key: "55-inch",
      base_price: 100000,
      images: [{ url: "https://www.lg.com/content/dam/variant.jpg" }],
      store_prices: [{ store_name: "Amazon", price: null, store_url: "https://amazon.in/tv" }],
    },
    {
      variant_key: "55-inch",
      base_price: 100000,
      store_prices: [{ store_name: "Flipkart", price: 99000, url: "https://flipkart.com/tv" }],
    },
  ],
};

test("canonical payload normalizes official images and deduplicates variants", () => {
  const payload = toCanonicalTvPayload(input, { imageDomains: ["lg.com"] });
  assert.deepEqual(payload.images_json, ["https://www.lg.com/content/dam/tv.jpg"]);
  assert.equal(payload.variants.length, 1);
  assert.equal(payload.variants[0].store_prices.length, 2);
  assert.deepEqual(payload.variants[0].images, ["https://www.lg.com/content/dam/variant.jpg"]);
  assert.equal(payload.variants[0].store_prices[0].price, null);
  assert.deepEqual(payload.display_json, undefined);
  assert.deepEqual(payload.sections.display_json, { panel_type: "OLED" });
});

test("image validator keeps official URL and rejects non-HTTPS or untrusted hosts", () => {
  assert.equal(validateTvImageUrl("https://www.lg.com/tv.jpg", ["lg.com"]), "https://www.lg.com/tv.jpg");
  assert.throws(() => validateTvImageUrl("http://www.lg.com/tv.jpg", ["lg.com"]), /HTTPS/);
  assert.throws(() => validateTvImageUrl("https://cdn.example.com/tv.jpg", ["lg.com"]), /approved official/);
});

test("create persistence uses shared tables and preserves null store prices", async () => {
  const queries = [];
  const client = { query: async (sql, params) => {
    queries.push({ sql, params });
    if (sql.includes("INSERT INTO products")) return { rows: [{ id: 42 }] };
    if (sql.includes("INSERT INTO product_variants")) return { rows: [{ id: 7 }] };
    return { rows: [] };
  } };
  await createTvCatalogRecord(client, input, {
    resolveBrandId: async () => 3,
    imageDomains: ["lg.com"],
  });
  assert.ok(queries.some((query) => query.sql.includes("INSERT INTO tvs")));
  assert.ok(queries.some((query) => query.sql.includes("INSERT INTO product_images")));
  assert.ok(queries.some((query) => query.sql.includes("INSERT INTO product_variants")));
  const storeQuery = queries.find((query) => query.sql.includes("INSERT INTO variant_store_prices"));
  assert.equal(storeQuery.params[2], null);
  assert.ok(queries.some((query) => query.sql.includes("INSERT INTO product_publish")));
});
