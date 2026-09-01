const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizeSmartphonePayload,
  flattenVariantStoreRows,
} = require("../schemas/smartphonePayload");

test("normalizeSmartphonePayload accepts canonical create/update payload and legacy aliases", () => {
  const payload = {
    product_name: "Pixel 9",
    brand_name: "Google",
    model: "Pixel 9",
    images: ["https://img/1.jpg", "https://img/2.jpg"],
    variants: [
      {
        ram: "8 GB",
        storage: "256 GB",
        base_price: 49999,
        stores: [
          {
            store_name: "Amazon",
            price: 49999,
            url: "https://amazon.in/pixel9",
          },
          {
            store: "Flipkart",
            price: 50999,
            link: "https://flipkart.com/pixel9",
          },
        ],
      },
    ],
    sensors: ["Accelerometer", "Gyroscope"],
    build_design: { material: "Glass" },
    connectivity: { wifi: "Wi‑Fi 7" },
  };

  const normalized = normalizeSmartphonePayload(payload);

  assert.equal(normalized.product_name, "Pixel 9");
  assert.equal(normalized.variants.length, 1);
  assert.equal(normalized.variants[0].stores.length, 2);
  assert.equal(normalized.variants[0].stores[0].store_name, "Amazon");
  assert.equal(normalized.variants[0].stores[1].store_name, "Flipkart");
  assert.equal(
    normalized.variants[0].stores[1].url,
    "https://flipkart.com/pixel9",
  );
  assert.deepEqual(normalized.sensors, ["Accelerometer", "Gyroscope"]);
  assert.deepEqual(normalized.images, [
    "https://img/1.jpg",
    "https://img/2.jpg",
  ]);
  assert.deepEqual(normalized.build_design, { material: "Glass" });
  assert.deepEqual(normalized.connectivity, { wifi: "Wi‑Fi 7" });
});

test("normalizeSmartphonePayload accepts legacy variants_json and store_prices aliases", () => {
  const payload = {
    product_name: "OnePlus 13",
    brand_name: "OnePlus",
    model: "OnePlus 13",
    variants_json: [
      {
        ram: "12 GB",
        storage: "256 GB",
        store_prices: [
          { store: "Amazon", price: 59999, url: "https://amazon.in/oneplus13" },
        ],
      },
    ],
  };

  const normalized = normalizeSmartphonePayload(payload);

  assert.equal(normalized.variants.length, 1);
  assert.equal(normalized.variants[0].stores.length, 1);
  assert.equal(normalized.variants[0].stores[0].store_name, "Amazon");
  assert.equal(
    normalized.variants[0].stores[0].url,
    "https://amazon.in/oneplus13",
  );
  assert.equal(normalized.variant_store_prices.length, 1);
  assert.equal(normalized.variant_store_prices[0].store_name, "Amazon");
  assert.equal(
    normalized.variant_store_prices[0].url,
    "https://amazon.in/oneplus13",
  );
});

test("flattenVariantStoreRows preserves nested stores for update payloads", () => {
  const payload = {
    launch_status_override: "upcoming",
    variants: [
      {
        id: 42,
        ram: "8 GB",
        storage: "128 GB",
        stores: [
          {
            store_name: "Flipkart",
            price: 19999,
            url: "https://flipkart.com/phone",
            offer_text: "Starting at",
          },
        ],
      },
    ],
  };

  const rows = flattenVariantStoreRows(payload);

  assert.equal(rows.length, 1);
  assert.equal(rows[0].variant_id, 42);
  assert.equal(rows[0].store_name, "Flipkart");
  assert.equal(rows[0].price, 19999);
  assert.equal(rows[0].url, "https://flipkart.com/phone");
  assert.equal(rows[0].offer_text, "Starting at");
  assert.equal(rows[0].sale_start_date, null);
});
