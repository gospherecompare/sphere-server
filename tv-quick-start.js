#!/usr/bin/env node

/**
 * 🚀 Sony BRAVIA 5 TV - Quick Start Testing Script
 * 
 * This script automates the testing workflow:
 * 1. Validate JSON structure
 * 2. Check server connectivity
 * 3. Optionally submit to API
 * 4. Provide client_1 testing instructions
 * 
 * Usage:
 *   node tv-quick-start.js
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

console.clear();
console.log("\n" + "🎬 SONY BRAVIA 5 TV - QUICK START TESTING".padEnd(70, "="));
console.log("Version: 1.0.0");
console.log("Date: 2026-08-26\n");

// ============================================================
// Step 1: Validate JSON
// ============================================================

console.log("📋 STEP 1: Validating JSON Structure\n");
console.log("Running: node test-sony-bravia-5-tv.js\n");

try {
  const result = execSync('node test-sony-bravia-5-tv.js', { encoding: 'utf-8' });
  
  // Extract key lines
  const lines = result.split('\n');
  const summaryStart = lines.findIndex(l => l.includes('FINAL SUMMARY'));
  if (summaryStart > -1) {
    console.log(lines.slice(summaryStart, summaryStart + 10).join('\n'));
  }
  
  if (result.includes('✅ READY FOR API SUBMISSION')) {
    console.log("\n✅ JSON validation PASSED\n");
  }
} catch (error) {
  console.log(`❌ Error during validation: ${error.message}\n`);
  process.exit(1);
}

// ============================================================
// Step 2: Check Server Status
// ============================================================

console.log("🔌 STEP 2: Checking Server Connectivity\n");

const http = require('http');

function checkServer() {
  return new Promise((resolve) => {
    const req = http.request('http://localhost:5000', (res) => {
      resolve(true);
    });
    req.on('error', () => resolve(false));
    req.setTimeout(2000);
    req.end();
  });
}

(async () => {
  const serverRunning = await checkServer();
  
  if (serverRunning) {
    console.log("✅ Server is running on http://localhost:5000\n");
  } else {
    console.log("⚠️  Server not found on port 5000");
    console.log("   To start server:\n");
    console.log("   cd d:\\technxt\\server");
    console.log("   npm start\n");
  }

  // ============================================================
  // Step 3: API Testing Options
  // ============================================================

  console.log("🚀 STEP 3: API Testing Options\n");
  console.log("Option A: Demo Mode (preview payload)");
  console.log("  Command: node test-tv-api.js --demo\n");
  
  console.log("Option B: Live API Submission");
  console.log("  1. Login to client_1: http://localhost:5173");
  console.log("  2. Get JWT token from browser DevTools (Network tab)");
  console.log("  3. Run: JWT_TOKEN=\"your_token\" node test-tv-api.js\n");

  // ============================================================
  // Step 4: Admin UI Testing
  // ============================================================

  console.log("🎨 STEP 4: Admin UI Testing (client_1)\n");
  console.log("Method 1: Manual Form Entry");
  console.log("  1. Open: http://localhost:5173 (or http://localhost:5174)");
  console.log("  2. Login with admin credentials");
  console.log("  3. Navigate: Menu → Products → Create New TV");
  console.log("  4. Fill form with Sony BRAVIA 5 details (see guide below)");
  console.log("  5. Add 2 variants (55\", 65\") with stores");
  console.log("  6. Submit → Check database\n");

  console.log("Method 2: API Integration");
  console.log("  1. Run: JWT_TOKEN=\"your_token\" node test-tv-api.js");
  console.log("  2. Verify in database");
  console.log("  3. Check in admin UI (should appear in TV list)\n");

  // ============================================================
  // Step 5: Verification
  // ============================================================

  console.log("✅ STEP 5: Verification Checklist\n");
  console.log("Database Verification:");
  console.log("  ✓ 1 TV product created (Sony BRAVIA 5)");
  console.log("  ✓ 2 variants created (55\", 65\")");
  console.log("  ✓ 5 store_price records inserted");
  console.log("  ✓ All URLs correctly stored");
  console.log("  ✓ Prices correctly normalized\n");

  console.log("Admin UI Verification:");
  console.log("  ✓ Product appears in TV list");
  console.log("  ✓ Both variants visible");
  console.log("  ✓ Store pricing displays correctly");
  console.log("  ✓ Edit form pre-populated with data\n");

  console.log("Frontend Verification:");
  console.log("  ✓ Product page loads");
  console.log("  ✓ Variant selector works");
  console.log("  ✓ Store prices update on variant change");
  console.log("  ✓ \"Buy Now\" links work\n");

  // ============================================================
  // Quick Reference
  // ============================================================

  console.log("📚 QUICK REFERENCE\n");
  console.log("Files Generated:");
  console.log("  • test-sony-bravia-5-tv.js - Full validation test (27 assertions)");
  console.log("  • test-tv-api.js - API integration test");
  console.log("  • SONY-BRAVIA-5-TV-TEST-GUIDE.md - Complete testing guide\n");

  console.log("Key URLs:");
  console.log("  • Server API: http://localhost:5000/api/tvs");
  console.log("  • Admin UI: http://localhost:5173");
  console.log("  • Frontend: http://localhost:5173\n");

  console.log("Important Endpoints:");
  console.log("  • POST /api/tvs - Create TV with variants & stores");
  console.log("  • GET /api/tvs - List all TVs");
  console.log("  • GET /api/tvs/:id - Get TV details\n");

  // ============================================================
  // Test Data
  // ============================================================

  console.log("📊 TEST DATA SUMMARY\n");
  console.log("Product: Sony BRAVIA 5 55-inch 4K Mini LED Smart Google TV");
  console.log("Category: 4K Smart Mini LED TV");
  console.log("Brand: Sony\n");

  console.log("Variant 1: 55 inch");
  console.log("  Model: K-55XR50");
  console.log("  Base Price: ₹134,990");
  console.log("  Stores: 3");
  console.log("    • Sony India Official @ ₹134,990");
  console.log("    • Amazon India @ ₹134,990");
  console.log("    • Flipkart @ ₹183,900\n");

  console.log("Variant 2: 65 inch");
  console.log("  Model: K-65XR50");
  console.log("  Base Price: ₹139,990");
  console.log("  Stores: 2");
  console.log("    • Amazon India @ ₹139,990");
  console.log("    • Flipkart @ ₹141,990\n");

  console.log("Total: 2 variants, 5 store records\n");

  // ============================================================
  // Next Steps
  // ============================================================

  console.log("🎯 RECOMMENDED NEXT STEPS\n");
  console.log("1. Run validation: node test-sony-bravia-5-tv.js");
  console.log("2. Start server: cd server && npm start");
  console.log("3. Test with demo: node test-tv-api.js --demo");
  console.log("4. Get JWT from client_1 login");
  console.log("5. Submit API: JWT_TOKEN=\"token\" node test-tv-api.js");
  console.log("6. Verify in database (check 5 store_price records)");
  console.log("7. Test in admin UI: http://localhost:5173");
  console.log("8. View in frontend: Check public product page\n");

  console.log("📖 For detailed guide: See SONY-BRAVIA-5-TV-TEST-GUIDE.md\n");

  console.log("=" .repeat(70));
  console.log("✅ Quick Start Complete!");
  console.log("=" .repeat(70) + "\n");
})();
