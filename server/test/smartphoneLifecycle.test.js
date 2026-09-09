const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createCanonicalSmartphoneResponse,
  resolveCanonicalLaunchStage,
} = require("../lifecycle/smartphoneLifecycle");

test("launch date controls launch status independently of sale date", () => {
  const device = {
    launch_date: "2026-09-04",
    store_prices: [{ price: 49999, sale_date: "2026-09-10" }],
  };

  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-03"), "upcoming");
  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-09"), "released");
});

test("sale date does not release a product without a launch date", () => {
  const device = {
    store_prices: [{ sale_date: "2026-09-01" }],
  };

  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-11"), "upcoming");
});

test("uses a standalone product sale date without requiring a store", () => {
  const device = {
    sale_start_date: "2026-09-10",
    launch_status_override: "released",
  };

  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-09"), "released");
  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-10"), "released");
});

test("renders one canonical sale date when product and store dates repeat", () => {
  const lifecycle = createCanonicalSmartphoneResponse(
    {
      sale_start_date: "2026-09-10",
      variants: [
        {
          sale_date: "2026-09-10",
          stores: [{ price: 39999, sale_start_date: "2026-09-10" }],
        },
      ],
    },
    "2026-09-09",
  );

  assert.equal(lifecycle.launch.stage, "upcoming");
  assert.equal(lifecycle.sale.stage, "sale_scheduled");
  assert.equal(lifecycle.sale.start_date, "2026-09-10");
});

test("keeps a launched product scheduled for the upcoming sale route", () => {
  const lifecycle = createCanonicalSmartphoneResponse(
    {
      launch_date: "2026-09-03",
      sale_start_date: "2026-09-10",
    },
    "2026-09-09",
  );

  assert.equal(lifecycle.launch.stage, "released");
  assert.equal(lifecycle.sale.stage, "sale_scheduled");
  assert.equal(lifecycle.render.type, "upcoming");
  assert.equal(lifecycle.render.display_status, "Upcoming");
});

test("variant sale dates do not override launch status", () => {
  const device = {
    variants: [{ stores: [{ price: 39999, sale_date: "2026-09-10" }] }],
    launch_status_override: "released",
  };

  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-09"), "released");
  assert.equal(resolveCanonicalLaunchStage(device, "2026-09-11"), "released");
});

test("reports the submitted payload as released and on sale on its sale date", () => {
  const lifecycle = createCanonicalSmartphoneResponse(
    {
      launch_status_override: "released",
      variants: [
        {
          stores: [
            {
              price: 29999,
              sale_start_date: "2026-09-09",
              url: "https://www.flipkart.com/p/poco-x8-5g/p/abc123",
            },
          ],
        },
      ],
    },
    "2026-09-09",
  );

  assert.equal(lifecycle.launch.stage, "released");
  assert.equal(lifecycle.sale.stage, "on_sale");
  assert.equal(lifecycle.store.stage, "live");
  assert.equal(lifecycle.render.type, "released");
  assert.equal(lifecycle.render.display_status, "Released");
});

test("matches the three India-date lifecycle examples", () => {
  const todayIndia = "2026-09-09";
  const cases = [
    {
      name: "vivo T5 5G",
      device: {
        launch_date: "2026-09-02",
        sale_start_date: "2026-09-09",
        variants: [
          {
            stores: [
              {
                price: 29999,
                url: "https://www.flipkart.com/p/vivo-t5-5g",
                sale_start_date: "2026-09-09",
                status: "upcoming",
              },
            ],
          },
        ],
      },
      expected: ["released", "on_sale", "live"],
    },
    {
      name: "Redmi 17 5G",
      device: {
        launch_date: "2026-09-03",
        sale_start_date: "2026-09-10",
        variants: [
          {
            stores: [
              {
                price: 23999,
                url: null,
                sale_start_date: "2026-09-10",
                status: "upcoming",
              },
            ],
          },
        ],
      },
      expected: ["released", "sale_scheduled", "prebooking"],
    },
    {
      name: "Lava Virat V1 5G",
      device: {
        launch_date: "2026-07-24",
        sale_start_date: "2026-07-31",
        variants: [
          {
            stores: [
              {
                price: 12999,
                url: "https://www.flipkart.com/p/lava-virat-v1-5g",
                sale_start_date: "2026-07-31",
                status: "upcoming",
              },
            ],
          },
        ],
      },
      expected: ["released", "on_sale", "live"],
    },
  ];

  for (const example of cases) {
    const lifecycle = createCanonicalSmartphoneResponse(
      example.device,
      todayIndia,
    );
    assert.deepEqual(
      [
        lifecycle.launch.stage,
        lifecycle.sale.stage,
        lifecycle.store.stage,
      ],
      example.expected,
      example.name,
    );
  }
});
