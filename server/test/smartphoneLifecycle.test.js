/**
 * PHASE 1 VALIDATION: CANONICAL SMARTPHONE LIFECYCLE RESOLVER
 *
 * Test Location: server/test/smartphoneLifecycle.test.js
 * Execution: npm test
 *
 * This test validates the canonical resolver in isolation (no server startup).
 * All 8 edge cases from Phase 0D must pass before proceeding to Phase 2.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  resolveCanonicalLaunchStage,
  resolveCanonicalSaleStage,
  resolveCanonicalStoreStage,
  createCanonicalSmartphoneResponse,
} = require("../lifecycle/smartphoneLifecycle");

// ============================================================================
// TEST DATA: Realistic JSON from Phase 0D
// ============================================================================

const todayIndia = "2026-09-02";

// Case A: No variants
const deviceA = {
  id: 1001,
  name: "Future Product A",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [],
  store_prices: [],
};

// Case B: Null price
const deviceB = {
  id: 1002,
  name: "Future Product B",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [{ base_price: null, store_prices: [] }],
  store_prices: [],
};

// Case C: Generic URL
const deviceC = {
  id: 1003,
  name: "Future Product C",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [
    {
      base_price: null,
      store_prices: [
        {
          store_name: "Flipkart",
          url: "https://www.flipkart.com/",
          price: null,
        },
      ],
    },
  ],
  store_prices: [
    {
      store_name: "Flipkart",
      url: "https://www.flipkart.com/",
      price: null,
    },
  ],
};

// Case D: Preorder
const deviceD = {
  id: 1004,
  name: "Future Product D",
  launch_date: "2026-09-04",
  sale_start_date: "2026-09-03",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [
    {
      base_price: 25000,
      store_prices: [
        {
          store_name: "Amazon",
          url: "https://www.amazon.in/product/abc123",
          price: 25000,
        },
      ],
    },
  ],
  store_prices: [
    {
      store_name: "Amazon",
      url: "https://www.amazon.in/product/abc123",
      price: 25000,
      sale_start_date: "2026-09-03",
    },
  ],
};

// Case E: Past launch + Future sale
const deviceE = {
  id: 1005,
  name: "Released Product E",
  launch_date: "2026-08-25",
  sale_start_date: "2026-09-10",
  launch_status_mode: "auto",
  launch_status_override: "released",
  variants: [
    {
      base_price: 30000,
      store_prices: [],
    },
  ],
  store_prices: [],
};

// Case F: Released + Store
const deviceF = {
  id: 1006,
  name: "Released Product F",
  launch_date: "2026-08-15",
  launch_status_mode: "auto",
  launch_status_override: "released",
  variants: [
    {
      base_price: 35000,
      store_prices: [
        {
          store_name: "Amazon",
          url: "https://www.amazon.in/product/def456",
          price: 35000,
        },
      ],
    },
  ],
  store_prices: [
    {
      store_name: "Amazon",
      url: "https://www.amazon.in/product/def456",
      price: 35000,
    },
  ],
};

// Case G: Editorial status wins regardless of mode/date
const deviceG = {
  id: 1007,
  name: "Future Product G",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: "released",
  variants: [],
  store_prices: [],
};

// Case H: Manual mode (override persists)
const deviceH = {
  id: 1008,
  name: "Future Product H",
  launch_date: "2026-09-04",
  launch_status_mode: "manual",
  launch_status_override: "upcoming",
  variants: [],
  store_prices: [],
};

// ============================================================================
// TESTS
// ============================================================================

test("Case A: No variants", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceA, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be upcoming",
  );
  assert.equal(
    canonical.render.type,
    "upcoming",
    "render.type should be upcoming",
  );
  assert.equal(canonical.allow_compare, false, "allow_compare should be false");
  assert.equal(canonical.launch.mode, "auto", "mode should be auto");
});

test("Case B: Null price", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceB, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be upcoming",
  );
  assert.equal(
    canonical.sale.stage,
    "sale_tbd",
    "sale.stage should be sale_tbd",
  );
  assert.equal(canonical.store.stage, "none", "store.stage should be none");
});

test("Case C: Generic URL (CRITICAL BUG FIX)", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceC, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be upcoming (generic URL doesn't override launch date)",
  );
  assert.equal(
    canonical.render.type,
    "upcoming",
    "render.type should be upcoming",
  );
  assert.equal(
    canonical.store.stage,
    "none",
    "store.stage should be none (store name alone without price/URL doesn't count)",
  );
  assert.equal(
    canonical.sale.stage,
    "sale_tbd",
    "sale.stage should be sale_tbd (no sale date, no valid price signal)",
  );
});

test("Case D: Preorder", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceD, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be upcoming",
  );
  assert.equal(
    canonical.sale.stage,
    "sale_scheduled",
    "sale.stage should be sale_scheduled",
  );
  assert.equal(
    canonical.store.stage,
    "prebooking",
    "store.stage should be prebooking",
  );
});

test("Case E: Past launch + future sale (CRITICAL BUG FIX)", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceE, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "released",
    "launch.stage should be released (launch date is past)",
  );
  assert.equal(
    canonical.sale.stage,
    "sale_scheduled",
    "sale.stage should be sale_scheduled (sale is independent, future date)",
  );
  assert.equal(
    canonical.render.type,
    "released",
    "render.type should be released (based on launch only)",
  );
  assert.equal(
    canonical.allow_compare,
    true,
    "allow_compare should be true (released products can compare)",
  );
});

test("Case F: Released + store", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceF, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "released",
    "launch.stage should be released",
  );
  assert.equal(
    canonical.sale.stage,
    "on_sale",
    "sale.stage should be on_sale (inferred from store price, no explicit sale date)",
  );
  assert.equal(canonical.store.stage, "live", "store.stage should be live");
  assert.equal(canonical.allow_compare, true, "allow_compare should be true");
});

test("Store signal: Priced affiliate product links are live stores", () => {
  const canonical = createCanonicalSmartphoneResponse(
    {
      launch_status_override: "released",
      variants: [
        {
          store_prices: [
            {
              store_name: "Amazon",
              price: 51999,
              url: "https://amzn.to/4rSEEfn",
            },
          ],
        },
      ],
    },
    todayIndia,
  );

  assert.equal(canonical.store.stage, "live");
  assert.equal(canonical.sale.stage, "on_sale");
});

test("Case G: Editorial status controls launch regardless of date", () => {
  const canonical = createCanonicalSmartphoneResponse(deviceG, todayIndia);

  assert.equal(
    canonical.launch.stage,
    "released",
    "launch.stage should be released (editorial status wins over date)",
  );
  assert.equal(canonical.launch.mode, "auto", "mode should be auto");
});

test("Case H: Manual mode (override persists)", () => {
  // Sub-test 1: Before launch date
  let canonical = createCanonicalSmartphoneResponse(deviceH, "2026-09-02");
  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "Sep 2: launch.stage should be upcoming (manual override)",
  );

  // Sub-test 2: On launch date
  canonical = createCanonicalSmartphoneResponse(deviceH, "2026-09-04");
  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "Sep 4: launch.stage should still be upcoming (manual override persists)",
  );

  // Sub-test 3: After launch date
  canonical = createCanonicalSmartphoneResponse(deviceH, "2026-09-10");
  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "Sep 10: launch.stage should still be upcoming (manual override permanent)",
  );

  assert.equal(canonical.launch.mode, "manual", "mode should be manual");
});

// ============================================================================
// CONSISTENCY TEST: Same input → same output
// ============================================================================

test("Consistency: Same input always produces same output", () => {
  const result1 = createCanonicalSmartphoneResponse(deviceA, todayIndia);
  const result2 = createCanonicalSmartphoneResponse(deviceA, todayIndia);

  assert.deepEqual(
    result1,
    result2,
    "Running same resolver twice with same inputs should produce identical results",
  );
});

// ============================================================================
// DIMENSION INDEPENDENCE TEST
// ============================================================================

test("Dimension Independence: Launch and sale are independent", () => {
  // Example 1: Upcoming launch + preorder sale
  const deviceUpcomingPreorder = {
    launch_date: "2026-09-04",
    sale_start_date: "2026-09-03",
    variants: [
      {
        base_price: 25000,
        store_prices: [{ url: "https://example.com/p/123", price: 25000 }],
      },
    ],
  };

  const canonical1 = createCanonicalSmartphoneResponse(
    deviceUpcomingPreorder,
    "2026-09-02",
  );
  assert.equal(canonical1.launch.stage, "upcoming", "launch can be upcoming");
  assert.equal(
    canonical1.sale.stage,
    "sale_scheduled",
    "while sale is scheduled",
  );
  assert.equal(canonical1.store.stage, "prebooking", "and store is prebooking");

  // Example 2: Released launch + future sale
  const deviceReleasedFutureSale = {
    launch_date: "2026-08-25",
    launch_status_override: "released",
    sale_start_date: "2026-09-10",
    variants: [],
  };

  const canonical2 = createCanonicalSmartphoneResponse(
    deviceReleasedFutureSale,
    "2026-09-02",
  );
  assert.equal(canonical2.launch.stage, "released", "launch can be released");
  assert.equal(
    canonical2.sale.stage,
    "sale_scheduled",
    "while sale is scheduled",
  );
  assert.equal(
    canonical2.store.stage,
    "none",
    "and store is none (independently)",
  );
});

// ============================================================================
// PRECEDENCE TEST: Manual vs Auto
// ============================================================================

test("Editorial status: Upcoming persists after launch date", () => {
  const device = {
    launch_date: "2026-08-15", // Past
    launch_status_mode: "auto",
    launch_status_override: "upcoming",
  };

  const canonical = createCanonicalSmartphoneResponse(device, "2026-09-10");
  assert.equal(
    canonical.launch.stage,
    "upcoming",
    "Upcoming status should win even when date is past",
  );
});

test("Editorial status: Released requires explicit status", () => {
  const device = {
    launch_date: "2026-08-15", // Past
    launch_status_mode: "auto",
    launch_status_override: "released",
  };

  const canonical = createCanonicalSmartphoneResponse(device, "2026-09-02");
  assert.equal(
    canonical.launch.stage,
    "released",
    "Released status should be explicit",
  );
});

test("Editorial status: Launch date today does not release product", () => {
  const canonical = createCanonicalSmartphoneResponse(
    { launch_date: todayIndia, launch_status_override: "upcoming" },
    todayIndia,
  );
  assert.equal(canonical.launch.stage, "upcoming");
  assert.equal(canonical.render.type, "upcoming");
});

test("Render: All upcoming launch stages render as upcoming", () => {
  for (const launchStage of ["rumored", "announced", "upcoming"]) {
    const canonical = createCanonicalSmartphoneResponse(
      {
        launch_status_mode: "manual",
        launch_status_override: launchStage,
      },
      todayIndia,
    );

    assert.equal(canonical.launch.stage, launchStage);
    assert.equal(canonical.render.type, "upcoming");
    assert.equal(canonical.render.display_status, "Upcoming");
  }
});

test("Compare-shaped product: Variants and stores produce complete lifecycle", () => {
  const canonical = createCanonicalSmartphoneResponse(
    {
      launch_date: "2026-08-15",
      launch_status_override: "released",
      sale_start_date: "2026-08-20",
      variants: [
        {
          base_price: 29999,
          store_prices: [
            {
              store_name: "Amazon",
              price: 29999,
              url: "https://www.amazon.in/product/abc123",
              sale_start_date: "2026-08-20",
            },
          ],
        },
      ],
    },
    todayIndia,
  );

  assert.equal(canonical.launch.stage, "released");
  assert.equal(canonical.sale.stage, "on_sale");
  assert.equal(canonical.store.stage, "live");
});

// ============================================================================
// SPEC SCORE PERMISSION TEST
// ============================================================================

test("Spec Score: Upcoming products never receive algorithmic scores", () => {
  // Upcoming product → score unavailable regardless of spec completeness
  const upcomingEligible = createCanonicalSmartphoneResponse(
    deviceA,
    todayIndia,
    true,
  );
  assert.equal(upcomingEligible.allow_spec_score, false);

  // Upcoming product with ineligible specs → cannot show score
  const upcomingIneligible = createCanonicalSmartphoneResponse(
    deviceA,
    todayIndia,
    false,
  );
  assert.equal(
    upcomingIneligible.allow_spec_score,
    false,
    "Upcoming with ineligible specs → allow_spec_score false",
  );

  // Released product always can show score (if eligible)
  const releasedEligible = createCanonicalSmartphoneResponse(
    deviceF,
    todayIndia,
    true,
  );
  assert.equal(
    releasedEligible.allow_spec_score,
    true,
    "Released with eligible specs → allow_spec_score true",
  );
});

// ============================================================================
// STORE INDEPENDENCE TEST
// ============================================================================

test("Store Stage: Independent from sale date (not inferred from it)", () => {
  // Future sale date should NOT automatically make store=prebooking
  // Store state should be based on store data, not sale date
  const device = {
    launch_date: "2026-09-04",
    sale_start_date: "2026-09-10", // Future sale
    // But no actual store entries
    variants: [],
    store_prices: [],
  };

  const canonical = createCanonicalSmartphoneResponse(device, "2026-09-02");
  assert.equal(
    canonical.sale.stage,
    "sale_scheduled",
    "sale.stage should be sale_scheduled (because sale_start_date is future)",
  );
  assert.equal(
    canonical.store.stage,
    "none",
    "store.stage should be none (no stores, even though sale is future)",
  );
});

test("Market status derives strictly from launch stage and ignores store/live signals", () => {
  const releasedWithStorePending = createCanonicalSmartphoneResponse(
    {
      launch_status_override: "released",
      variants: [
        {
          store_prices: [
            {
              store_name: "Amazon",
              price: 31999,
              url: "https://www.amazon.in/product/abc123",
            },
          ],
        },
      ],
    },
    todayIndia,
  );

  assert.equal(releasedWithStorePending.launch.stage, "released");
  assert.equal(releasedWithStorePending.market_status, "Released");
  assert.equal(releasedWithStorePending.render.type, "released");

  const upcomingWithLiveStore = createCanonicalSmartphoneResponse(
    {
      launch_status_override: "upcoming",
      variants: [
        {
          store_prices: [
            {
              store_name: "Flipkart",
              price: 34999,
              url: "https://www.flipkart.com/product/xyz",
            },
          ],
        },
      ],
    },
    todayIndia,
  );

  assert.equal(upcomingWithLiveStore.launch.stage, "upcoming");
  assert.equal(upcomingWithLiveStore.market_status, "Upcoming");
  assert.equal(upcomingWithLiveStore.render.type, "upcoming");
});
