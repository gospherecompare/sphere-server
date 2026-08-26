/**
 * JSON Validation Test
 * Validates the iQOO Z11 5G smartphone JSON against:
 * 1. Server API expectations
 * 2. Client-side normalization logic
 */

const testJsonPayload = {
  "name": "iQOO Z11 5G",
  "brand_name": "iQOO",
  "model": "Z11 5G",
  "category": "mid-range",
  "launch_date": "2026-08-28",
  "official_preorder_url": "https://www.iqoo.com/in/products/z11-5g",
  "launch_status_override": "upcoming",

  "colors": [
    "Glacier Blue",
    "Cosmic Black"
  ],

  "build_design": {
    "width": "76.18 mm",
    "height": "163.73 mm",
    "weight": "213 g",
    "thickness": "8.25 mm",
    "back_cover_material": "Plastic composite sheet",
    "fingerprint_sensor_type": "In-display optical fingerprint sensor"
  },

  "display": {
    "size": "6.83 inch",
    "panel": "AMOLED",
    "resolution": "2800 x 1260",
    "refresh_rate": "144Hz",
    "protection": "Panda Glass",
    "pixel_density": "449 PPI",
    "brightness": "2000 nits HBM",
    "touch_sampling_rate": null,
    "hdr_support": null,
    "pwm_dimming": null,
    "dc_dimming": null,
    "color_gamut": "P3",
    "touch_screen": "Capacitive multi-touch"
  },

  "performance": {
    "processor": "Qualcomm Snapdragon 7s Gen 4",
    "gpu": null,
    "ai_processor": null,
    "process_technology": "4nm",
    "cpu": [
      "1 x 2.7GHz",
      "3 x 2.4GHz",
      "4 x 1.8GHz"
    ]
  },

  "camera": {
    "rear_camera": {
      "main_camera": {
        "resolution": "50 MP",
        "sensor": "Sony IMX882",
        "aperture": "f/1.79",
        "ois": null
      },
      "ultrawide_camera": null,
      "telephoto_camera": null,
      "macro_camera": null,
      "depth_camera": {
        "resolution": "2 MP",
        "aperture": "f/2.4"
      },
      "flash": "Rear flash"
    },
    "front_camera": {
      "resolution": "32 MP",
      "aperture": "f/2.0",
      "features": [
        "Night",
        "Portrait",
        "Photo",
        "Video"
      ]
    }
  },

  "battery": {
    "capacity": "9020 mAh",
    "battery_type": "Li-ion",
    "charging_power": "90W",
    "reverse_charging": null,
    "bypass_charging": null,
    "usb_port": "USB Type-C",
    "pd_support": null
  },

  "connectivity": {
    "usb": "USB Type-C",
    "wifi": "Wi-Fi 6",
    "bluetooth": "Bluetooth 5.2",
    "nfc": "Supported",
    "otg": "Supported",
    "fm_radio": "Not supported"
  },

  "network": {
    "network_type": "5G+5G Dual SIM Dual Standby",
    "sim_slot": "2 Nano SIMs",
    "dual_active": "Supported",
    "esim": null,
    "frequency_band": {
      "2g_gsm": "850/900/1800MHz",
      "3g_wcdma": "B1/B5/B6/B8/B19",
      "4g_fdd_lte": "B1/B3/B5/B7/B8/B19/B20/B28/B71",
      "4g_tdd_lte": "B38/B40/B41/B42/B43/B48",
      "5g": "n1/n3/n5/n7/n8/n20/n28/n38/n40/n41/n71/n77/n78"
    }
  },

  "operating_system": {
    "os": "OriginOS 6",
    "android_version": "Android 16"
  },

  "sensors": {
    "accelerometer": "Supported",
    "ambient_light_sensor": "Supported",
    "gyroscope": "Supported",
    "proximity_sensor": "Supported",
    "electronic_compass": "Supported",
    "infrared_sensor": "Infrared blaster",
    "fingerprint_sensor": "In-display optical"
  },

  "images": [],

  "variants": [
    {
      "ram": "8 GB",
      "storage": "256 GB",
      "base_price": 24999,

      "stores": [
        {
          "store_name": "Amazon",
          "region": "India",
          "price": 24999,
          "currency": "INR",
          "status": "available",
          "url": "https://www.amazon.in/example-iqoo-z11-8gb-256gb",
          "offer_text": "Bank offer available",
          "notes": "Online exclusive"
        },
        {
          "store_name": "Flipkart",
          "region": "India",
          "price": 24999,
          "currency": "INR",
          "status": "available",
          "url": "https://www.flipkart.com/example-iqoo-z11-8gb-256gb",
          "offer_text": null,
          "notes": null
        }
      ]
    },

    {
      "ram": "12 GB",
      "storage": "256 GB",
      "base_price": 27999,

      "stores": [
        {
          "store_name": "Amazon",
          "region": "India",
          "price": 27999,
          "currency": "INR",
          "status": "available",
          "url": "https://www.amazon.in/example-iqoo-z11-12gb-256gb",
          "offer_text": "Limited-time offer",
          "notes": null
        },
        {
          "store_name": "Flipkart",
          "region": "India",
          "price": 27999,
          "currency": "INR",
          "status": "available",
          "url": "https://www.flipkart.com/example-iqoo-z11-12gb-256gb",
          "offer_text": null,
          "notes": null
        }
      ]
    }
  ],

  "country_of_origin": "India",

  "box_contents": [
    "Phone",
    "USB Cable",
    "Charger",
    "SIM Eject Tool",
    "Protective Case",
    "Warranty Card"
  ],

  "published": true
};

// ============================================================
// SERVER-SIDE VALIDATION
// ============================================================

function validateServerExpectations() {
  console.log("\n📋 SERVER-SIDE VALIDATION");
  console.log("=" .repeat(60));
  
  const issues = [];
  
  // Check 1: Variants array
  if (!Array.isArray(testJsonPayload.variants)) {
    issues.push("❌ variants must be an array");
  }
  
  testJsonPayload.variants.forEach((variant, idx) => {
    // Check if stores or store_prices exists
    const hasStores = Array.isArray(variant.stores);
    const hasStorePrices = Array.isArray(variant.store_prices);
    
    if (!hasStores && !hasStorePrices) {
      issues.push(`❌ Variant ${idx}: Missing both 'stores' and 'store_prices' arrays`);
    } else {
      const storeField = hasStores ? "stores" : "store_prices";
      console.log(`✅ Variant ${idx}: Found '${storeField}' array`);
    }
    
    // Check store records
    const storeList = variant.stores || variant.store_prices || [];
    storeList.forEach((store, storeIdx) => {
      // Required: store_name and url
      if (!store.store_name) {
        issues.push(`❌ Variant ${idx}, Store ${storeIdx}: Missing 'store_name'`);
      } else {
        console.log(`  ✅ Store ${storeIdx}: store_name="${store.store_name}"`);
      }
      
      if (!store.url) {
        issues.push(`⚠️  Variant ${idx}, Store ${storeIdx}: Missing 'url' (will be skipped by server)`);
      } else {
        console.log(`  ✅ Store ${storeIdx}: url present`);
      }
      
      if (store.price !== null && store.price !== undefined) {
        console.log(`  ✅ Store ${storeIdx}: price=${store.price}`);
      } else {
        console.log(`  ⚠️  Store ${storeIdx}: price is null`);
      }
    });
  });
  
  return issues;
}

// ============================================================
// CLIENT-SIDE NORMALIZATION TEST
// ============================================================

function testClientNormalization() {
  console.log("\n📊 CLIENT-SIDE NORMALIZATION TEST");
  console.log("=" .repeat(60));
  
  const errors = [];
  
  // Simulate client-side normalizeVariant logic
  testJsonPayload.variants.forEach((variant, variantIdx) => {
    console.log(`\nVariant ${variantIdx}: ${variant.ram} + ${variant.storage}`);
    
    // Step 1: Source detection (mimics normalizeVariant)
    const storesSource = Array.isArray(variant?.stores)
      ? variant.stores
      : Array.isArray(variant?.store_prices)
        ? variant.store_prices
        : [];
    
    if (storesSource.length === 0) {
      console.log("  ⚠️  No stores/store_prices found");
      return;
    }
    
    console.log(`  📦 Found ${storesSource.length} store(s)`);
    
    // Step 2: Normalize each store
    storesSource.forEach((store, storeIdx) => {
      // Simulate normalizeStore logic
      const normalized = {
        store_name: store.store_name || store.store || store.storeName,
        price: store.price ?? store.current_price ?? store.sale_price ?? null,
        url: store.url || store.link || store.affiliate_link || store.affiliateUrl,
        offer_text: store.offer_text || store.offerText,
        sale_start_date: store.sale_start_date || store.sale_date || store.saleStartDate,
      };
      
      console.log(`    Store ${storeIdx}: ${normalized.store_name}`);
      if (normalized.url) {
        console.log(`      ✅ URL: ${normalized.url.substring(0, 40)}...`);
      } else {
        console.log(`      ❌ URL: MISSING`);
        errors.push(`Variant ${variantIdx}, Store ${storeIdx}: No URL`);
      }
      if (normalized.price !== null) {
        console.log(`      ✅ Price: ₹${normalized.price.toLocaleString('en-IN')}`);
      } else {
        console.log(`      ⚠️  Price: null`);
      }
      if (normalized.offer_text) {
        console.log(`      ℹ️  Offer: ${normalized.offer_text}`);
      }
    });
  });
  
  return errors;
}

// ============================================================
// DATABASE INSERTION SIMULATION
// ============================================================

function simulateDatabaseInsertion() {
  console.log("\n💾 DATABASE INSERTION SIMULATION");
  console.log("=" .repeat(60));
  
  const insertedRecords = [];
  
  testJsonPayload.variants.forEach((variant, variantIdx) => {
    const storeList = variant.stores || variant.store_prices || [];
    
    storeList.forEach((store) => {
      // Simulate the server's field normalization
      const storeName = 
        store?.store_name ||
        store?.store ||
        store?.storeName ||
        store?.display_store_name ||
        null;

      const price =
        store?.price ??
        store?.current_price ??
        store?.sale_price ??
        null;

      const url =
        store?.url ||
        store?.link ||
        store?.affiliate_link ||
        store?.affiliateUrl ||
        null;
      
      // Server validation: skip if missing store_name or url
      if (!storeName || !url) {
        console.log(`⏭️  SKIPPED: ${variant.ram}/${variant.storage} - Missing required fields`);
        return;
      }
      
      insertedRecords.push({
        variant_key: `${variant.ram}_${variant.storage}`,
        store_name: storeName,
        price,
        url,
        offer_text: store?.offer_text || store?.offerText || null,
      });
      
      console.log(`✅ INSERT: ${storeName} - ${variant.ram}/${variant.storage} @ ₹${price || 'NULL'}`);
    });
  });
  
  console.log(`\n📊 Total records to insert: ${insertedRecords.length}`);
  return insertedRecords;
}

// ============================================================
// JSON SCHEMA VALIDATION
// ============================================================

function validateJsonSchema() {
  console.log("\n🔍 JSON SCHEMA VALIDATION");
  console.log("=" .repeat(60));
  
  const checks = {
    "Product name": testJsonPayload.name ? "✅" : "❌",
    "Brand": testJsonPayload.brand_name ? "✅" : "❌",
    "Model": testJsonPayload.model ? "✅" : "❌",
    "Launch date": testJsonPayload.launch_date ? "✅" : "❌",
    "Colors array": Array.isArray(testJsonPayload.colors) ? "✅" : "❌",
    "Build design": typeof testJsonPayload.build_design === 'object' ? "✅" : "❌",
    "Display specs": typeof testJsonPayload.display === 'object' ? "✅" : "❌",
    "Performance specs": typeof testJsonPayload.performance === 'object' ? "✅" : "❌",
    "Camera specs": typeof testJsonPayload.camera === 'object' ? "✅" : "❌",
    "Battery specs": typeof testJsonPayload.battery === 'object' ? "✅" : "❌",
    "Connectivity specs": typeof testJsonPayload.connectivity === 'object' ? "✅" : "❌",
    "Network specs": typeof testJsonPayload.network === 'object' ? "✅" : "❌",
    "Sensors": typeof testJsonPayload.sensors === 'object' ? "✅" : "❌",
    "Variants array": Array.isArray(testJsonPayload.variants) ? "✅" : "❌",
  };
  
  Object.entries(checks).forEach(([field, status]) => {
    console.log(`${status} ${field}`);
  });
  
  return Object.values(checks).every(v => v === "✅");
}

// ============================================================
// MAIN TEST EXECUTION
// ============================================================

function runAllTests() {
  console.log("\n" + "🧪 COMPREHENSIVE JSON VALIDATION TEST SUITE".padEnd(60, "="));
  console.log(`Generated: ${new Date().toISOString()}`);
  
  // Test 1: Schema validation
  const schemaValid = validateJsonSchema();
  
  // Test 2: Server expectations
  const serverIssues = validateServerExpectations();
  
  // Test 3: Client normalization
  const clientErrors = testClientNormalization();
  
  // Test 4: Database insertion simulation
  const dbRecords = simulateDatabaseInsertion();
  
  // Summary
  console.log("\n" + "=" .repeat(60));
  console.log("📈 TEST SUMMARY");
  console.log("=" .repeat(60));
  console.log(`✅ JSON Structure: ${schemaValid ? 'VALID' : 'INVALID'}`);
  console.log(`✅ Server Expectations: ${serverIssues.length === 0 ? 'PASS' : `FAIL (${serverIssues.length} issues)`}`);
  console.log(`✅ Client Normalization: ${clientErrors.length === 0 ? 'PASS' : `FAIL (${clientErrors.length} errors)`}`);
  console.log(`✅ Database Records: ${dbRecords.length} records ready for insertion`);
  
  if (serverIssues.length > 0) {
    console.log("\n⚠️  Server Issues:");
    serverIssues.forEach(issue => console.log(`   ${issue}`));
  }
  
  if (clientErrors.length > 0) {
    console.log("\n❌ Client Errors:");
    clientErrors.forEach(error => console.log(`   ${error}`));
  }
  
  console.log("\n" + "=" .repeat(60));
  
  // Overall result
  const allTestsPass = schemaValid && serverIssues.length === 0 && clientErrors.length === 0;
  console.log(`\n🎯 OVERALL RESULT: ${allTestsPass ? '✅ PASS - JSON is ready for API submission' : '❌ FAIL - Please fix issues above'}\n`);
  
  return {
    schemaValid,
    serverIssues,
    clientErrors,
    dbRecords,
    allTestsPass,
  };
}

// Execute tests
const results = runAllTests();

// Export for Node.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    testJsonPayload,
    validateServerExpectations,
    testClientNormalization,
    simulateDatabaseInsertion,
    validateJsonSchema,
    runAllTests,
    results,
  };
}
