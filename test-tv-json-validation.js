/**
 * TV JSON Validation Test
 * Validates Sony BRAVIA 5 TV JSON against:
 * 1. Server API expectations
 * 2. Client-side normalization logic
 */

const testJsonPayload = {
  "product_name": "Sony BRAVIA 5 TV",
  "brand_name": "Sony",
  "brand_id": 3,
  "category": "4K TV",
  "model": "K-55XR50",

  "basic_info_json": {
    "brand": "Sony",
    "model_number": "K-55XR50",
    "year": 2024,
    "availability": "In Stock"
  },

  "display_json": {
    "screen_size": "55 inch OLED",
    "resolution": "3840 x 2160 (4K)",
    "panel_type": "OLED",
    "refresh_rate": "120Hz",
    "brightness": "200 nits"
  },

  "video_engine_json": {
    "processor": "XR 200",
    "hdr_support": "HDR10, Dolby Vision",
    "upscaling": "4K Upscaling"
  },

  "audio_json": {
    "speakers": "2.2ch 20W",
    "dolby_atmos": "Yes",
    "sound_quality": "Dolby Atmos"
  },

  "smart_tv_json": {
    "os": "Google TV",
    "connectivity": "Wi-Fi 6E, Bluetooth 5.2"
  },

  "ports_json": {
    "hdmi": "4x HDMI 2.1",
    "usb": "2x USB 3.0",
    "optical": "Yes"
  },

  "connectivity_json": {
    "wifi": "Wi-Fi 6E",
    "bluetooth": "5.2",
    "ethernet": "Gigabit"
  },

  "images_json": [
    "https://example.com/sony-bravia-5-55.jpg"
  ],

  "variants": [
    {
      "variant_key": "55inch",
      "screen_size": "55 inch",
      "screen_size_value": 55,
      "model_number": "K-55XR50",
      "base_price": 134990,

      "stores": [
        {
          "store_name": "Sony India Official",
          "region": "India",
          "price": 134990,
          "currency": "INR",
          "status": "available",
          "url": "https://www.sony.co.in/bravia/products/bravia-5-xr50",
          "offer_text": null
        },
        {
          "store_name": "Amazon India",
          "region": "India",
          "price": 134990,
          "currency": "INR",
          "status": "available",
          "url": "https://www.amazon.in/dp/B0F7X7WM8N",
          "offer_text": "No EMI available"
        },
        {
          "store_name": "Flipkart",
          "region": "India",
          "price": 139990,
          "currency": "INR",
          "status": "available",
          "url": "https://www.flipkart.com/sony-bravia-5-55/p/itm123abc",
          "offer_text": "Extra 5% off with card"
        }
      ]
    },

    {
      "variant_key": "65inch",
      "screen_size": "65 inch",
      "screen_size_value": 65,
      "model_number": "K-65XR50",
      "base_price": 199990,

      "store_prices": [
        {
          "store": "Sony India Official",
          "price": 199990,
          "url": "https://www.sony.co.in/bravia/products/bravia-5-xr50-65"
        },
        {
          "store": "Amazon India",
          "price": 199990,
          "url": "https://www.amazon.in/dp/B0F7X7WM8N-65"
        },
        {
          "store": "Flipkart",
          "price": 209990,
          "url": "https://www.flipkart.com/sony-bravia-5-65/p/itm123def"
        }
      ]
    },

    {
      "variant_key": "75inch",
      "screen_size": "75 inch",
      "screen_size_value": 75,
      "model_number": "K-75XR50",
      "base_price": 299990,

      "stores": [
        {
          "store_name": "Croma",
          "price": 299990,
          "link": "https://www.croma.com/sony-bravia-5-75/p/265789",
          "offer_text": "Free installation"
        },
        {
          "store_name": "Reliance Digital",
          "price": 309990,
          "affiliate_url": "https://www.reliancedigital.in/sony-bravia-5-75"
        }
      ]
    },

    {
      "variant_key": "85inch",
      "screen_size": "85 inch",
      "screen_size_value": 85,
      "model_number": "K-85XR50",
      "base_price": 449990,

      "store_prices": [
        {
          "store_name": "Croma",
          "current_price": 449990,
          "url": "https://www.croma.com/sony-bravia-5-85/p/265790",
          "delivery_info": "Free delivery + installation"
        },
        {
          "store_name": "Reliance Digital",
          "sale_price": 459990,
          "url": "https://www.reliancedigital.in/sony-bravia-5-85",
          "delivery_info": "Same day delivery available"
        }
      ]
    }
  ],

  "published": false
};

// ============================================================
// SERVER-SIDE VALIDATION
// ============================================================

function validateServerExpectations() {
  console.log("\n📺 SERVER-SIDE VALIDATION (TV)");
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
      console.log(`✅ Variant ${idx} (${variant.screen_size}): Found '${storeField}' array`);
    }
    
    // Check store records
    const storeList = variant.stores || variant.store_prices || [];
    storeList.forEach((store, storeIdx) => {
      // Check store name
      const storeName = store.store_name || store.store;
      if (!storeName) {
        issues.push(`❌ Variant ${idx}, Store ${storeIdx}: Missing 'store_name' or 'store'`);
      } else {
        console.log(`  ✅ Store ${storeIdx}: ${storeName}`);
      }
      
      // Check URL (multiple aliases)
      const url = store.url || store.link || store.affiliate_url || store.affiliateUrl;
      if (!url) {
        issues.push(`⚠️  Variant ${idx}, Store ${storeIdx}: Missing URL (link, affiliate_url, url)`);
      } else {
        console.log(`     ✅ URL: ${url.substring(0, 40)}...`);
      }
      
      // Check price (multiple aliases)
      const price = store.price ?? store.current_price ?? store.sale_price;
      if (price !== null && price !== undefined) {
        console.log(`     ✅ Price: ₹${price.toLocaleString('en-IN')}`);
      } else {
        console.log(`     ⚠️  Price: null`);
      }
    });
  });
  
  return issues;
}

// ============================================================
// CLIENT-SIDE NORMALIZATION TEST
// ============================================================

function testClientNormalization() {
  console.log("\n📊 CLIENT-SIDE NORMALIZATION TEST (TV)");
  console.log("=" .repeat(60));
  
  const errors = [];
  
  testJsonPayload.variants.forEach((variant, variantIdx) => {
    console.log(`\nVariant ${variantIdx}: ${variant.screen_size}`);
    
    // Step 1: Source detection
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
      const normalized = {
        store_name: store.store_name || store.store || store.storeName,
        price: store.price ?? store.current_price ?? store.sale_price ?? null,
        url: store.url || store.link || store.affiliate_url || store.affiliateUrl,
        offer_text: store.offer_text || store.offerText || store.offer_text,
        delivery_info: store.delivery_info || store.deliveryInfo,
      };
      
      console.log(`    Store ${storeIdx}: ${normalized.store_name}`);
      if (normalized.url) {
        console.log(`      ✅ URL: ${normalized.url.substring(0, 50)}...`);
      } else {
        console.log(`      ❌ URL: MISSING`);
        errors.push(`Variant ${variantIdx}, Store ${storeIdx}: No URL`);
      }
      if (normalized.price !== null) {
        console.log(`      ✅ Price: ₹${normalized.price.toLocaleString('en-IN')}`);
      }
      if (normalized.offer_text) {
        console.log(`      ℹ️  Offer: ${normalized.offer_text}`);
      }
      if (normalized.delivery_info) {
        console.log(`      🚚 Delivery: ${normalized.delivery_info}`);
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
      // Simulate server normalization
      const storeName = 
        store?.store_name ||
        store?.store ||
        store?.storeName ||
        null;

      const price =
        store?.price ??
        store?.current_price ??
        store?.sale_price ??
        null;

      const url =
        store?.url ||
        store?.link ||
        store?.affiliate_url ||
        store?.affiliateUrl ||
        null;
      
      // Server validation: skip if missing store_name or url
      if (!storeName || !url) {
        console.log(`⏭️  SKIPPED: ${variant.screen_size} - Missing required fields`);
        return;
      }
      
      insertedRecords.push({
        variant_key: variant.variant_key,
        screen_size: variant.screen_size,
        store_name: storeName,
        price,
        url,
        offer_text: store?.offer_text || store?.offerText || null,
        delivery_info: store?.delivery_info || store?.deliveryInfo || null,
      });
      
      console.log(`✅ INSERT: ${storeName} - ${variant.screen_size} @ ₹${price || 'NULL'}`);
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
    "Product name": testJsonPayload.product_name ? "✅" : "❌",
    "Brand": testJsonPayload.brand_name ? "✅" : "❌",
    "Brand ID": testJsonPayload.brand_id ? "✅" : "❌",
    "Model": testJsonPayload.model ? "✅" : "❌",
    "Display specs": typeof testJsonPayload.display_json === 'object' ? "✅" : "❌",
    "Video engine": typeof testJsonPayload.video_engine_json === 'object' ? "✅" : "❌",
    "Audio specs": typeof testJsonPayload.audio_json === 'object' ? "✅" : "❌",
    "Smart TV specs": typeof testJsonPayload.smart_tv_json === 'object' ? "✅" : "❌",
    "Ports specs": typeof testJsonPayload.ports_json === 'object' ? "✅" : "❌",
    "Connectivity": typeof testJsonPayload.connectivity_json === 'object' ? "✅" : "❌",
    "Images array": Array.isArray(testJsonPayload.images_json) ? "✅" : "❌",
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
  console.log("\n" + "🧪 TV JSON VALIDATION TEST SUITE".padEnd(60, "="));
  console.log(`Generated: ${new Date().toISOString()}`);
  console.log(`Product: Sony BRAVIA 5 TV`);
  
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
