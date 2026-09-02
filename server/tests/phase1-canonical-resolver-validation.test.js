/**
 * PHASE 1 CANONICAL RESOLVER VALIDATION TEST
 *
 * Test the canonical resolver in isolation before wiring into API endpoints.
 * This validates the exact precedence rules and state calculations.
 *
 * DO NOT proceed to Phase 2 until all 8 cases pass with expected output.
 */

const {
  resolveCanonicalLaunchStage,
  resolveCanonicalSaleStage,
  resolveCanonicalStoreStage,
  resolveCanonicalRenderType,
  resolveCanonicalPermissions,
  createCanonicalSmartphoneResponse,
} = require("../index.js");

// Test helpers
const ISO_DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

const assertEquals = (actual, expected, testName) => {
  const pass = JSON.stringify(actual) === JSON.stringify(expected);
  const status = pass ? "✓ PASS" : "✗ FAIL";
  console.log(`  ${status}: ${testName}`);
  if (!pass) {
    console.log(`    Expected: ${JSON.stringify(expected)}`);
    console.log(`    Actual:   ${JSON.stringify(actual)}`);
  }
  return pass;
};

// ============================================================================
// TEST DATA: Realistic JSON structures from Phase 0D
// ============================================================================

/**
 * Case A: No variants
 * Description: Future launch, no variants, null prices
 * Date: 2026-09-02 (2 days before launch)
 */
const caseA_NoVariants = {
  id: 1001,
  name: "Future Product A",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [],
  store_prices: [],
};

/**
 * Case B: Null price
 * Description: Future launch, null price allowed, preorder scenario
 * Date: 2026-09-02
 */
const caseB_NullPrice = {
  id: 1002,
  name: "Future Product B",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [{ base_price: null, store_prices: [] }],
  store_prices: [],
};

/**
 * Case C: Generic URL (THE BUG CASE)
 * Description: Future launch + generic Flipkart homepage URL
 * Server returns "available" because generic URL triggers hasSmartphonePurchaseSignal
 * This should now return "upcoming"
 * Date: 2026-09-02
 */
const caseC_GenericUrl = {
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
          url: "https://www.flipkart.com/", // Generic homepage URL
          price: null,
        },
      ],
    },
  ],
  store_prices: [
    {
      store_name: "Flipkart",
      url: "https://www.flipkart.com/", // Generic URL
      price: null,
    },
  ],
};

/**
 * Case D: Preorder
 * Description: Future launch + preorder URL + future sale date
 * Date: 2026-09-02
 */
const caseD_Preorder = {
  id: 1004,
  name: "Future Product D",
  launch_date: "2026-09-04",
  sale_start_date: "2026-09-03", // Preorder starts tomorrow
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [
    {
      base_price: 25000,
      store_prices: [
        {
          store_name: "Amazon",
          url: "https://www.amazon.in/s?k=product&ref=sr_pg_1", // Product search
          price: 25000,
        },
      ],
    },
  ],
  store_prices: [
    {
      store_name: "Amazon",
      url: "https://www.amazon.in/product/abc123", // Product URL
      price: 25000,
      sale_start_date: "2026-09-03",
    },
  ],
};

/**
 * Case E: Past launch + Future sale (THE BUG CASE)
 * Description: Past launch (Aug 25) + Future sale (Sep 10)
 * Server returns "upcoming" because it checks sale_date BEFORE launch_date
 * Should return "released" (launch date is authoritative for launch stage)
 * Date: 2026-09-02
 */
const caseE_PastLaunchFutureSale = {
  id: 1005,
  name: "Released Product E",
  launch_date: "2026-08-25", // Past launch
  sale_start_date: "2026-09-10", // Future sale date
  launch_status_mode: "auto",
  launch_status_override: null,
  variants: [
    {
      base_price: 30000,
      store_prices: [],
    },
  ],
  store_prices: [],
};

/**
 * Case F: Released + Store
 * Description: Past launch + live store with URL and price
 * Date: 2026-09-02
 */
const caseF_ReleasedStore = {
  id: 1006,
  name: "Released Product F",
  launch_date: "2026-08-15", // Past launch
  launch_status_mode: "auto",
  launch_status_override: null,
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

/**
 * Case G: Override (Auto mode, override present but not used)
 * Description: Future launch + override field + auto mode
 * Override should not be used in auto mode
 * Date: 2026-09-02
 */
const caseG_Override = {
  id: 1007,
  name: "Future Product G",
  launch_date: "2026-09-04",
  launch_status_mode: "auto",
  launch_status_override: "released", // Override ignored in auto mode
  variants: [],
  store_prices: [],
};

/**
 * Case H: Manual Mode (CRITICAL)
 * Description: Manual mode + upcoming override + future launch date
 * Override should be used because mode=manual, date is ignored
 * Product should stay "upcoming" even after launch date passes
 * Date: 2026-09-02 (but test what happens on Sep 3 and Sep 4)
 */
const caseH_ManualMode = {
  id: 1008,
  name: "Future Product H",
  launch_date: "2026-09-04",
  launch_status_mode: "manual",
  launch_status_override: "upcoming", // Override is authoritative
  variants: [],
  store_prices: [],
};

// ============================================================================
// TESTS
// ============================================================================

console.log("\n");
console.log("═".repeat(80));
console.log("PHASE 1: CANONICAL RESOLVER VALIDATION");
console.log("═".repeat(80));
console.log("\nTest Date: 2026-09-02 (referenced as 'today' in all cases)");
console.log("\n");

let totalTests = 0;
let passedTests = 0;

// Helper to track test results
const test = (name, fn) => {
  console.log(`\n${name}`);
  const result = fn();
  totalTests++;
  if (result) passedTests++;
  return result;
};

// ─────────────────────────────────────────────────────────────────────────────
// CASE A: No Variants
// ─────────────────────────────────────────────────────────────────────────────

test("CASE A: No variants", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseA_NoVariants,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseA_NoVariants.name)}`);
  console.log(`  Launch date: ${caseA_NoVariants.launch_date}`);
  console.log(`  Mode: ${caseA_NoVariants.launch_status_mode}`);

  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be 'upcoming' (date is future)",
  );

  const pass2 = assertEquals(
    canonical.render.type,
    "upcoming",
    "render.type should be 'upcoming'",
  );

  const pass3 = assertEquals(
    canonical.allow_compare,
    false,
    "allow_compare should be false for upcoming",
  );

  return pass1 && pass2 && pass3;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE B: Null Price
// ─────────────────────────────────────────────────────────────────────────────

test("CASE B: Null price", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseB_NullPrice,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseB_NullPrice.name)}`);
  console.log(`  Launch date: ${caseB_NullPrice.launch_date}`);
  console.log(`  Variant price: null`);

  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be 'upcoming' (null price doesn't affect)",
  );

  const pass2 = assertEquals(
    canonical.sale.stage,
    "sale_tbd",
    "sale.stage should be 'sale_tbd' (no sale info)",
  );

  const pass3 = assertEquals(
    canonical.store.stage,
    "none",
    "store.stage should be 'none' (no stores)",
  );

  return pass1 && pass2 && pass3;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE C: Generic URL (Critical Bug Fix)
// ─────────────────────────────────────────────────────────────────────────────

test("CASE C: Generic Flipkart URL (CRITICAL BUG FIX)", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseC_GenericUrl,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseC_GenericUrl.name)}`);
  console.log(`  Launch date: ${caseC_GenericUrl.launch_date}`);
  console.log(`  Store URL: ${caseC_GenericUrl.store_prices[0].url}`);
  console.log(`  Store price: null`);
  console.log(
    `  OLD BUG: Server would return 'available' (generic URL triggers store signal)`,
  );
  console.log(
    `  EXPECTED: 'upcoming' (date-based, generic URL ignored in launch calc)`,
  );

  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be 'upcoming' (date checked first, not store signal)",
  );

  const pass2 = assertEquals(
    canonical.render.type,
    "upcoming",
    "render.type should be 'upcoming'",
  );

  const pass3 = assertEquals(
    canonical.store.stage,
    "none",
    "store.stage should be 'none' (generic URL with no price isn't a store signal)",
  );

  return pass1 && pass2 && pass3;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE D: Preorder
// ─────────────────────────────────────────────────────────────────────────────

test("CASE D: Preorder (future launch + future sale)", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseD_Preorder,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseD_Preorder.name)}`);
  console.log(`  Launch date: ${caseD_Preorder.launch_date}`);
  console.log(`  Sale date: ${caseD_Preorder.sale_start_date}`);

  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be 'upcoming' (launch is future)",
  );

  const pass2 = assertEquals(
    canonical.sale.stage,
    "sale_scheduled",
    "sale.stage should be 'sale_scheduled' (sale is future)",
  );

  const pass3 = assertEquals(
    canonical.store.stage,
    "prebooking",
    "store.stage should be 'prebooking' (sale is future)",
  );

  return pass1 && pass2 && pass3;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE E: Past Launch + Future Sale (Critical Bug Fix)
// ─────────────────────────────────────────────────────────────────────────────

test("CASE E: Past launch + future sale (CRITICAL BUG FIX)", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseE_PastLaunchFutureSale,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseE_PastLaunchFutureSale.name)}`);
  console.log(`  Launch date: ${caseE_PastLaunchFutureSale.launch_date}`);
  console.log(`  Sale date: ${caseE_PastLaunchFutureSale.sale_start_date}`);
  console.log(
    `  OLD BUG: Both server and frontend return 'upcoming' (check sale before launch)`,
  );
  console.log(
    `  EXPECTED: 'released' (launch is past, sale doesn't override launch state)`,
  );

  const pass1 = assertEquals(
    canonical.launch.stage,
    "released",
    "launch.stage should be 'released' (launch date is past)",
  );

  const pass2 = assertEquals(
    canonical.sale.stage,
    "sale_scheduled",
    "sale.stage should be 'sale_scheduled' (sale is future, independent from launch)",
  );

  const pass3 = assertEquals(
    canonical.render.type,
    "released",
    "render.type should be 'released' (based on launch only)",
  );

  const pass4 = assertEquals(
    canonical.allow_compare,
    true,
    "allow_compare should be true (launched products can compare)",
  );

  return pass1 && pass2 && pass3 && pass4;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE F: Released + Store
// ─────────────────────────────────────────────────────────────────────────────

test("CASE F: Released + live store", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseF_ReleasedStore,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseF_ReleasedStore.name)}`);
  console.log(`  Launch date: ${caseF_ReleasedStore.launch_date}`);
  console.log(`  Store: ${caseF_ReleasedStore.store_prices[0].store_name}`);

  const pass1 = assertEquals(
    canonical.launch.stage,
    "released",
    "launch.stage should be 'released' (launch date is past)",
  );

  const pass2 = assertEquals(
    canonical.sale.stage,
    "on_sale",
    "sale.stage should be 'on_sale' (sale started, no future date)",
  );

  const pass3 = assertEquals(
    canonical.store.stage,
    "live",
    "store.stage should be 'live' (store has URL and price)",
  );

  const pass4 = assertEquals(
    canonical.allow_compare,
    true,
    "allow_compare should be true",
  );

  return pass1 && pass2 && pass3 && pass4;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE G: Override (Auto Mode)
// ─────────────────────────────────────────────────────────────────────────────

test("CASE G: Override present but auto mode (date should win)", () => {
  const todayIndia = "2026-09-02";
  const canonical = createCanonicalSmartphoneResponse(
    caseG_Override,
    todayIndia,
  );

  console.log(`  Input: ${JSON.stringify(caseG_Override.name)}`);
  console.log(`  Mode: ${caseG_Override.launch_status_mode}`);
  console.log(`  Override field: ${caseG_Override.launch_status_override}`);
  console.log(`  Launch date: ${caseG_Override.launch_date}`);
  console.log(`  NOTE: In auto mode, date takes precedence over override`);

  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "launch.stage should be 'upcoming' (date is future, overrides the override)",
  );

  const pass2 = assertEquals(
    canonical.launch.mode,
    "auto",
    "launch.mode should be 'auto'",
  );

  return pass1 && pass2;
});

// ─────────────────────────────────────────────────────────────────────────────
// CASE H: Manual Mode (CRITICAL)
// ─────────────────────────────────────────────────────────────────────────────

test("CASE H: Manual mode (override is immutable)", () => {
  console.log(`  Input: ${JSON.stringify(caseH_ManualMode.name)}`);
  console.log(`  Mode: ${caseH_ManualMode.launch_status_mode}`);
  console.log(`  Override: ${caseH_ManualMode.launch_status_override}`);
  console.log(`  Launch date: ${caseH_ManualMode.launch_date}`);

  // Sub-test 1: Before launch date (Sep 2)
  let todayIndia = "2026-09-02";
  let canonical = createCanonicalSmartphoneResponse(
    caseH_ManualMode,
    todayIndia,
  );

  console.log(`\n  Sub-test Sep 2 (before launch):`);
  const pass1 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "  Sep 2: launch.stage should be 'upcoming' (manual override)",
  );

  // Sub-test 2: On launch date (Sep 4)
  todayIndia = "2026-09-04";
  canonical = createCanonicalSmartphoneResponse(caseH_ManualMode, todayIndia);

  console.log(`\n  Sub-test Sep 4 (on launch date):`);
  const pass2 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "  Sep 4: launch.stage should STILL be 'upcoming' (manual override persists)",
  );

  // Sub-test 3: After launch date (Sep 10)
  todayIndia = "2026-09-10";
  canonical = createCanonicalSmartphoneResponse(caseH_ManualMode, todayIndia);

  console.log(`\n  Sub-test Sep 10 (after launch):`);
  const pass3 = assertEquals(
    canonical.launch.stage,
    "upcoming",
    "  Sep 10: launch.stage should STILL be 'upcoming' (manual override permanent until admin changes)",
  );

  const pass4 = assertEquals(
    canonical.launch.mode,
    "manual",
    "launch.mode should be 'manual'",
  );

  return pass1 && pass2 && pass3 && pass4;
});

// ============================================================================
// SUMMARY
// ============================================================================

console.log("\n");
console.log("═".repeat(80));
console.log("VALIDATION SUMMARY");
console.log("═".repeat(80));
console.log(`\nTotal Tests: ${totalTests}`);
console.log(`Passed: ${passedTests}/${totalTests}`);
console.log(`Failed: ${totalTests - passedTests}/${totalTests}`);

if (passedTests === totalTests) {
  console.log("\n✓ ALL TESTS PASSED");
  console.log("\nCanonical resolver is production-safe.");
  console.log("Ready to proceed to Phase 2: Wire resolver into API endpoints.");
  process.exit(0);
} else {
  console.log("\n✗ SOME TESTS FAILED");
  console.log("\nDo NOT proceed to Phase 2 until all tests pass.");
  console.log("The canonical resolver must be verified first.");
  process.exit(1);
}
