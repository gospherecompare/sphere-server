/**
 * Sony BRAVIA 5 TV JSON Validation & API Test
 * Tests against server API expectations and client normalization
 */

const testJsonPayload = {
  "product_name": "Sony BRAVIA 5 55-inch 4K Mini LED Smart Google TV",
  "brand_name": "Sony",
  "category": "4K Smart Mini LED TV",
  "model": "K-55XR50",
  "publish": true,

  "key_specs_json": {
    "smart_tv": true,
    "panel_type": "Mini LED",
    "resolution": "3840 x 2160 (4K UHD)",
    "hdr_support": ["Dolby Vision", "HDR10", "HLG"],
    "screen_size": "55 inch",
    "audio_output": "40W",
    "gaming_ready": true,
    "refresh_rate": "120Hz",
    "operating_system": "Google TV",
    "imax_enhanced": true
  },

  "basic_info_json": {
    "launch_year": 2025,
    "model_number": "K-55XR50",
    "model_series": "BRAVIA 5 (XR50)"
  },

  "display_json": {
    "panel_type": "Mini LED",
    "resolution": "3840 x 2160",
    "refresh_rate": "120Hz",
    "hdr_formats": ["Dolby Vision", "HDR10"],
    "imax_enhanced": true
  },

  "video_engine_json": {
    "processor": "XR Processor (AI Cognitive Processor)",
    "upscaling": "XR Clear Image (AI 4K Upscaler)",
    "colour_processing": "XR Triluminos Pro"
  },

  "audio_json": {
    "dolby_atmos": true,
    "output_power": "40W",
    "audio_channels": "2.2ch"
  },

  "smart_tv_json": {
    "operating_system": "Google TV",
    "google_assistant": true,
    "apple_airplay2": true,
    "supported_apps": ["Netflix", "Prime Video", "YouTube"]
  },

  "gaming_json": {
    "gaming_mode": true,
    "vrr": true,
    "allm": true,
    "4k_at_120fps": true
  },

  "ports_json": {
    "hdmi": 4,
    "hdmi_version": "HDMI 2.1",
    "ethernet": true,
    "optical_audio": true
  },

  "connectivity_json": {
    "wifi": true,
    "wifi_standard": "Wi-Fi 6",
    "bluetooth": "5.3",
    "apple_airplay2": true
  },

  "power_json": {
    "power_consumption": "112W",
    "standby_power": "0.5W"
  },

  "physical_json": {
    "dimensions_with_stand_inside": "122.8 x 73.9 x 21.0 cm",
    "weight": "18.8 kg",
    "vesa_mount": "300 x 300 mm"
  },

  "product_details_json": {
    "launch_year": 2025,
    "model_series": "BRAVIA 5 XR50 Series",
    "country_of_origin": "India",
    "color": "Black"
  },

  "in_the_box_json": {
    "items": ["TV Unit", "Table Top Stand", "Voice Remote Control", "2 AA Batteries"]
  },

  "warranty_json": {
    "service_type": "On-site",
    "product_warranty": "1 Year Comprehensive Warranty"
  },

  "images_json": [],

  "variants": [
    {
      "variant_key": "55inch",
      "screen_size": "55 inch",
      "screen_size_cm": "138.8 cm",
      "screen_size_value": 55,
      "model_number": "K-55XR50",
      "base_price": 134990,
      "images": [],

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
          "offer_text": null
        },
        {
          "store_name": "Flipkart",
          "region": "India",
          "price": 183900,
          "currency": "INR",
          "status": "available",
          "url": "https://www.flipkart.com/sony-bravia-5-138-8-cm-55-inch-ultra-hd-4k-mini-led-smart-google-tv-2025/p/itmf517f779cdc5e",
          "offer_text": null
        }
      ]
    },

    {
      "variant_key": "65inch",
      "screen_size": "65 inch",
      "screen_size_cm": "163.9 cm",
      "screen_size_value": 65,
      "model_number": "K-65XR50",
      "base_price": 139990,
      "images": [],

      "stores": [
        {
          "store_name": "Amazon India",
          "region": "India",
          "price": 139990,
          "currency": "INR",
          "status": "available",
          "url": "https://www.amazon.in/dp/B0F7X7KL2P",
          "offer_text": null
        },
        {
          "store_name": "Flipkart",
          "region": "India",
          "price": 141990,
          "currency": "INR",
          "status": "available",
          "url": "https://www.flipkart.com/example-sony-bravia-65-xr50",
          "offer_text": null
        }
      ]
    }
  ]
};

// ============================================================
// VALIDATION TESTS
// ============================================================

console.log("\n" + "🧪 SONY BRAVIA 5 TV - COMPREHENSIVE TEST SUITE".padEnd(60, "="));
console.log(`Product: ${testJsonPayload.product_name}`);
console.log(`Generated: ${new Date().toISOString()}\n`);

let totalTests = 0;
let passedTests = 0;

function test(name, condition) {
  totalTests++;
  if (condition) {
    console.log(`✅ ${name}`);
    passedTests++;
  } else {
    console.log(`❌ ${name}`);
  }
}

// Test 1: Schema Validation
console.log("\n1️⃣  SCHEMA VALIDATION");
console.log("-".repeat(60));
test("Product name present", !!testJsonPayload.product_name);
test("Brand name present", !!testJsonPayload.brand_name);
test("Category present", !!testJsonPayload.category);
test("Model present", !!testJsonPayload.model);
test("Display specs present", !!testJsonPayload.display_json);
test("Video engine present", !!testJsonPayload.video_engine_json);
test("Audio specs present", !!testJsonPayload.audio_json);
test("Smart TV specs present", !!testJsonPayload.smart_tv_json);
test("Gaming specs present", !!testJsonPayload.gaming_json);
test("Ports present", !!testJsonPayload.ports_json);
test("Connectivity present", !!testJsonPayload.connectivity_json);
test("Power specs present", !!testJsonPayload.power_json);
test("Physical specs present", !!testJsonPayload.physical_json);
test("Warranty info present", !!testJsonPayload.warranty_json);
test("Variants array present", Array.isArray(testJsonPayload.variants));

// Test 2: Variants Structure
console.log("\n2️⃣  VARIANTS STRUCTURE");
console.log("-".repeat(60));
testJsonPayload.variants.forEach((variant, idx) => {
  test(`Variant ${idx} has variant_key`, !!variant.variant_key);
  test(`Variant ${idx} has screen_size`, !!variant.screen_size);
  test(`Variant ${idx} has base_price`, variant.base_price > 0);
  test(`Variant ${idx} has stores array`, Array.isArray(variant.stores));
  
  const stores = variant.stores || [];
  test(`Variant ${idx} has ${stores.length} store(s)`, stores.length > 0);
});

// Test 3: Server-Side Normalization
console.log("\n3️⃣  SERVER-SIDE FIELD NORMALIZATION");
console.log("-".repeat(60));

let totalStores = 0;
let validStores = 0;

testJsonPayload.variants.forEach((variant, variantIdx) => {
  const stores = variant.stores || [];
  stores.forEach((store, storeIdx) => {
    totalStores++;
    
    // Check store_name (accepts 'store_name' or 'store')
    const storeName = store.store_name || store.store;
    if (storeName) {
      validStores++;
      console.log(`✅ V${variantIdx}.S${storeIdx}: ${storeName}`);
    } else {
      console.log(`❌ V${variantIdx}.S${storeIdx}: Missing store_name`);
    }
    
    // Check URL (accepts 'url', 'link', 'affiliate_url', 'affiliateUrl')
    const url = store.url || store.link || store.affiliate_url || store.affiliateUrl;
    if (!url) {
      console.log(`   ❌ Missing URL`);
    } else {
      console.log(`   ✅ URL: ${url.substring(0, 45)}...`);
    }
    
    // Check Price (accepts 'price', 'current_price', 'sale_price')
    const price = store.price ?? store.current_price ?? store.sale_price;
    if (price) {
      console.log(`   ✅ Price: ₹${price.toLocaleString('en-IN')}`);
    }
  });
});

console.log(`\nStore Validation: ${validStores}/${totalStores} stores have valid names`);

// Test 4: Database Simulation
console.log("\n4️⃣  DATABASE INSERTION SIMULATION");
console.log("-".repeat(60));

const dbRecords = [];
testJsonPayload.variants.forEach((variant) => {
  const stores = variant.stores || [];
  stores.forEach((store) => {
    const storeName = store.store_name || store.store;
    const price = store.price ?? store.current_price ?? store.sale_price;
    const url = store.url || store.link || store.affiliate_url || store.affiliateUrl;
    
    if (storeName && url) {
      dbRecords.push({
        variant: variant.screen_size,
        store: storeName,
        price,
        url
      });
      console.log(`✅ INSERT: ${storeName} - ${variant.screen_size} @ ₹${price}`);
    }
  });
});

console.log(`\nTotal records for database: ${dbRecords.length}`);

// Test 5: Client-Side Compatibility
console.log("\n5️⃣  CLIENT-SIDE NORMALIZATION");
console.log("-".repeat(60));

let clientErrors = 0;
testJsonPayload.variants.forEach((variant) => {
  // Client expects: variant.stores array with store_name, price, url
  const storesSource = Array.isArray(variant?.stores)
    ? variant.stores
    : Array.isArray(variant?.store_prices)
      ? variant.store_prices
      : [];
  
  if (storesSource.length > 0) {
    test(`Variant stores normalize correctly`, true);
  } else {
    console.log(`❌ No stores found in variant`);
    clientErrors++;
  }
});

// Summary
console.log("\n" + "=".repeat(60));
console.log("📊 FINAL SUMMARY");
console.log("=".repeat(60));
console.log(`Total Tests: ${totalTests}`);
console.log(`Passed: ${passedTests}`);
console.log(`Failed: ${totalTests - passedTests}`);
console.log(`Pass Rate: ${((passedTests / totalTests) * 100).toFixed(1)}%`);
console.log(`Database Records: ${dbRecords.length}`);
console.log(`Store Validation: ${validStores}/${totalStores} valid`);

const isReady = passedTests === totalTests && validStores === totalStores;
console.log(`\n🎯 Status: ${isReady ? '✅ READY FOR API SUBMISSION' : '⚠️  NEEDS FIXES'}\n`);

// Export
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    testJsonPayload,
    isReady,
    dbRecords,
    stats: {
      totalTests,
      passedTests,
      totalStores,
      validStores
    }
  };
}
