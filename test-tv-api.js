/**
 * TV API Integration Test
 * Submits Sony BRAVIA 5 TV to POST /api/tvs endpoint
 * 
 * Usage:
 *   node test-tv-api.js                                    // Uses JWT_TOKEN env var
 *   JWT_TOKEN="your_token" node test-tv-api.js             // Pass token directly
 *   node test-tv-api.js --demo                             // Demo mode (no submission)
 */

const http = require('http');
const payload = require('./test-sony-bravia-5-tv.js');

const args = process.argv.slice(2);
const isDemo = args.includes('--demo');
const jwtToken = process.env.JWT_TOKEN || 'YOUR_JWT_TOKEN_HERE';

console.log("\n" + "🚀 TV API INTEGRATION TEST".padEnd(60, "="));
console.log(`Server: http://localhost:5000/api/tvs`);
console.log(`Mode: ${isDemo ? 'DEMO (no actual submission)' : 'LIVE API SUBMISSION'}`);
console.log(`JWT Token: ${jwtToken.substring(0, 20)}...`);
console.log(`Payload: Sony BRAVIA 5 (2 variants, 5 stores)\n`);

// ============================================================
// STEP 1: SERVER CONNECTIVITY CHECK
// ============================================================

console.log("1️⃣  SERVER CONNECTIVITY CHECK");
console.log("-".repeat(60));

function checkServerConnectivity() {
  return new Promise((resolve) => {
    const req = http.request('http://localhost:5000', (res) => {
      console.log(`✅ Server is running (port 5000)`);
      resolve(true);
    });
    
    req.on('error', (err) => {
      console.log(`❌ Server not found on port 5000`);
      console.log(`   Error: ${err.message}`);
      console.log(`   Make sure to run: npm start (in d:\\technxt\\server)\n`);
      resolve(false);
    });
    
    req.end();
  });
}

// ============================================================
// STEP 2: API SUBMISSION
// ============================================================

function submitToAPI() {
  return new Promise((resolve) => {
    if (isDemo) {
      console.log("\n2️⃣  DEMO MODE - PAYLOAD PREVIEW");
      console.log("-".repeat(60));
      console.log(JSON.stringify(payload.testJsonPayload, null, 2).substring(0, 500) + "...\n");
      resolve({
        demo: true,
        status: 200,
        body: { success: true, message: "Demo mode - no API submission" }
      });
      return;
    }

    console.log("\n2️⃣  API SUBMISSION");
    console.log("-".repeat(60));
    console.log("Sending POST /api/tvs...");

    const postData = JSON.stringify(payload.testJsonPayload);

    const options = {
      hostname: 'localhost',
      port: 5000,
      path: '/api/tvs',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': postData.length,
        'Authorization': `Bearer ${jwtToken}`
      }
    };

    const req = http.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const response = JSON.parse(data);
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: response
          });
        } catch (e) {
          resolve({
            status: res.statusCode,
            body: data
          });
        }
      });
    });

    req.on('error', (err) => {
      resolve({
        error: err.message,
        status: 0
      });
    });

    req.write(postData);
    req.end();
  });
}

// ============================================================
// STEP 3: RESPONSE PROCESSING
// ============================================================

async function processResponse(response) {
  console.log("3️⃣  API RESPONSE");
  console.log("-".repeat(60));

  if (response.error) {
    console.log(`❌ API Error: ${response.error}`);
    return false;
  }

  if (response.demo) {
    console.log("✅ Demo mode complete");
    return true;
  }

  console.log(`Status Code: ${response.status}`);

  if (response.status === 200 || response.status === 201) {
    console.log(`✅ Success!`);
    if (response.body.id || response.body.product_id || response.body.message) {
      console.log(`\nResponse Details:`);
      Object.entries(response.body).forEach(([key, value]) => {
        if (typeof value === 'object') {
          console.log(`  ${key}: ${JSON.stringify(value).substring(0, 50)}...`);
        } else {
          console.log(`  ${key}: ${value}`);
        }
      });
    }
    return true;
  } else if (response.status === 401) {
    console.log(`❌ Authentication Failed (401)`);
    console.log(`   Please provide valid JWT token via JWT_TOKEN environment variable`);
    console.log(`   Usage: JWT_TOKEN="your_token" node test-tv-api.js\n`);
    return false;
  } else if (response.status === 400) {
    console.log(`⚠️  Bad Request (400)`);
    console.log(`Response:`, JSON.stringify(response.body, null, 2));
    return false;
  } else {
    console.log(`⚠️  Unexpected Status: ${response.status}`);
    console.log(`Response:`, response.body);
    return false;
  }
}

// ============================================================
// STEP 4: VERIFICATION
// ============================================================

async function verifyInDatabase() {
  console.log("\n4️⃣  DATABASE VERIFICATION (Optional)");
  console.log("-".repeat(60));
  console.log("After API submission, verify in database:");
  console.log("  SELECT * FROM products WHERE product_name LIKE 'Sony BRAVIA%';");
  console.log("  SELECT * FROM tvs WHERE model_number LIKE 'K-%XR50%';");
  console.log("  SELECT * FROM variant_store_prices WHERE variant_id IN (SELECT id FROM product_variants...);");
}

// ============================================================
// MAIN EXECUTION
// ============================================================

async function main() {
  try {
    // Check server
    const serverOk = await checkServerConnectivity();
    if (!serverOk && !isDemo) {
      console.log("\n❌ Cannot proceed: Server not running\n");
      process.exit(1);
    }

    // Submit to API
    const response = await submitToAPI();
    const success = await processResponse(response);

    // Verification steps
    if (success || isDemo) {
      await verifyInDatabase();
    }

    // Final summary
    console.log("\n" + "=".repeat(60));
    console.log("📊 NEXT STEPS");
    console.log("=".repeat(60));
    
    if (isDemo) {
      console.log("Demo mode complete. To test with live API:");
      console.log("  1. Get your JWT token from client_1 login");
      console.log("  2. Run: JWT_TOKEN=\"your_token\" node test-tv-api.js\n");
    } else if (success) {
      console.log("✅ TV successfully submitted!");
      console.log("  1. Check database for inserted records");
      console.log("  2. Verify in client_1 admin UI: TV management section");
      console.log("  3. Check store prices are correctly inserted\n");
    } else {
      console.log("⚠️  API submission failed. Troubleshooting:");
      console.log("  1. Verify server is running: npm start (in server/)\n");
      console.log("  2. Check JWT token is valid");
      console.log("  3. Review server logs for validation errors\n");
    }

  } catch (error) {
    console.log(`\n❌ Error: ${error.message}\n`);
  }
}

main();
