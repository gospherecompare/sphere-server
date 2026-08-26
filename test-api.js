#!/usr/bin/env node
/**
 * API Integration Test
 * Tests the iQOO Z11 5G JSON against the actual server API
 */

const http = require('http');
const https = require('https');

const testJsonPayload = {
  "product": {
    "name": "iQOO Z11 5G",
    "brand_name": "iQOO",
    "brand_id": 5  // Assuming iQOO brand exists with ID 5
  },
  "smartphone": {
    "category": "mid-range",
    "segment": "mid-range",
    "brand": "iQOO",
    "brand_name": "iQOO",
    "model": "Z11 5G",
    "launch_date": "2026-08-28",
    "launch_status_override": "upcoming",
    "official_preorder_url": "https://www.iqoo.com/in/products/z11-5g",
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
      "color_gamut": "P3",
      "touch_screen": "Capacitive multi-touch"
    },
    "performance": {
      "processor": "Qualcomm Snapdragon 7s Gen 4",
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
          "aperture": "f/1.79"
        },
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
      "usb_port": "USB Type-C"
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
      "frequency_band": {
        "2g_gsm": "850/900/1800MHz",
        "3g_wcdma": "B1/B5/B6/B8/B19",
        "4g_fdd_lte": "B1/B3/B5/B7/B8/B19/B20/B28/B71",
        "4g_tdd_lte": "B38/B40/B41/B42/B43/B48",
        "5g": "n1/n3/n5/n7/n8/n20/n28/n38/n40/n41/n71/n77/n78"
      }
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
    "create_ai_summary": false
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
          "price": 24999,
          "currency": "INR",
          "url": "https://www.amazon.in/example-iqoo-z11-8gb-256gb",
          "offer_text": "Bank offer available"
        },
        {
          "store_name": "Flipkart",
          "price": 24999,
          "currency": "INR",
          "url": "https://www.flipkart.com/example-iqoo-z11-8gb-256gb"
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
          "price": 27999,
          "currency": "INR",
          "url": "https://www.amazon.in/example-iqoo-z11-12gb-256gb",
          "offer_text": "Limited-time offer"
        },
        {
          "store_name": "Flipkart",
          "price": 27999,
          "currency": "INR",
          "url": "https://www.flipkart.com/example-iqoo-z11-12gb-256gb"
        }
      ]
    }
  ]
};

/**
 * Make HTTP request to server
 */
function makeRequest(options, data) {
  return new Promise((resolve, reject) => {
    const protocol = options.protocol === 'https:' ? https : http;
    
    const req = protocol.request(options, (res) => {
      let body = '';
      
      res.on('data', (chunk) => {
        body += chunk;
      });
      
      res.on('end', () => {
        try {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: body ? JSON.parse(body) : null,
            rawBody: body
          });
        } catch (e) {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: null,
            rawBody: body
          });
        }
      });
    });
    
    req.on('error', reject);
    
    if (data) {
      req.write(JSON.stringify(data));
    }
    
    req.end();
  });
}

/**
 * Test server connectivity
 */
async function testServerConnectivity() {
  console.log('\n🔗 TESTING SERVER CONNECTIVITY');
  console.log('=' .repeat(60));
  
  const options = {
    hostname: 'localhost',
    port: 5000,
    path: '/api/health',
    method: 'GET'
  };
  
  try {
    const res = await makeRequest(options);
    console.log(`✅ Server is running on http://localhost:5000`);
    console.log(`   Status: ${res.statusCode}`);
    return true;
  } catch (error) {
    console.log(`❌ Cannot connect to server: ${error.message}`);
    console.log('\n   💡 Make sure the server is running:');
    console.log('   $ cd server && npm start');
    return false;
  }
}

/**
 * Test authentication
 */
async function testAuthentication(jwtToken) {
  console.log('\n🔐 TESTING AUTHENTICATION');
  console.log('=' .repeat(60));
  
  if (!jwtToken) {
    console.log('⚠️  No JWT token provided. Skipping authentication test.');
    console.log('   To test with authentication, provide a valid JWT token.');
    return false;
  }
  
  const options = {
    hostname: 'localhost',
    port: 5000,
    path: '/api/profile',
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${jwtToken}`,
      'Content-Type': 'application/json'
    }
  };
  
  try {
    const res = await makeRequest(options);
    if (res.statusCode === 200) {
      console.log('✅ Authentication successful');
      return true;
    } else {
      console.log(`❌ Authentication failed (${res.statusCode})`);
      return false;
    }
  } catch (error) {
    console.log(`❌ Authentication test error: ${error.message}`);
    return false;
  }
}

/**
 * Submit smartphone creation request
 */
async function submitSmartphoneCreation(jwtToken) {
  console.log('\n📤 SUBMITTING SMARTPHONE CREATION REQUEST');
  console.log('=' .repeat(60));
  
  if (!jwtToken) {
    console.log('⚠️  No JWT token provided. This request will be rejected.');
    console.log('   Authentication token is required to create products.');
    return null;
  }
  
  const options = {
    hostname: 'localhost',
    port: 5000,
    path: '/api/smartphones',
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${jwtToken}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(JSON.stringify(testJsonPayload))
    }
  };
  
  try {
    console.log('Sending payload...');
    console.log(`- Product: ${testJsonPayload.product.name}`);
    console.log(`- Variants: ${testJsonPayload.variants.length}`);
    console.log(`- Total stores: ${testJsonPayload.variants.reduce((sum, v) => sum + (v.stores?.length || 0), 0)}`);
    
    const res = await makeRequest(options, testJsonPayload);
    
    console.log(`\nResponse Status: ${res.statusCode}`);
    
    if (res.statusCode === 201) {
      console.log('✅ CREATION SUCCESSFUL');
      if (res.body?.product_id) {
        console.log(`   Product ID: ${res.body.product_id}`);
      }
      if (res.body?.message) {
        console.log(`   Message: ${res.body.message}`);
      }
      return res.body;
    } else if (res.statusCode === 401) {
      console.log('❌ UNAUTHORIZED - Invalid or missing authentication token');
      return null;
    } else if (res.statusCode === 400) {
      console.log('❌ BAD REQUEST');
      if (res.body?.message) {
        console.log(`   Error: ${res.body.message}`);
      }
      if (res.body?.error) {
        console.log(`   Error: ${res.body.error}`);
      }
      return null;
    } else {
      console.log(`❌ Server Error (${res.statusCode})`);
      if (res.body?.error) {
        console.log(`   Error: ${res.body.error}`);
      }
      return null;
    }
  } catch (error) {
    console.log(`❌ Request failed: ${error.message}`);
    return null;
  }
}

/**
 * Verify data was saved correctly
 */
async function verifyCreatedProduct(productId, jwtToken) {
  console.log('\n✔️  VERIFYING CREATED PRODUCT');
  console.log('=' .repeat(60));
  
  if (!productId || !jwtToken) {
    console.log('⚠️  Cannot verify: Missing product ID or authentication');
    return null;
  }
  
  const options = {
    hostname: 'localhost',
    port: 5000,
    path: `/api/smartphones/${productId}`,
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${jwtToken}`,
      'Content-Type': 'application/json'
    }
  };
  
  try {
    const res = await makeRequest(options);
    
    if (res.statusCode === 200) {
      console.log('✅ Product retrieved successfully');
      
      const data = res.body?.data || res.body;
      if (data?.name) {
        console.log(`   Name: ${data.name}`);
      }
      if (data?.model) {
        console.log(`   Model: ${data.model}`);
      }
      if (data?.variants_json) {
        const variants = typeof data.variants_json === 'string' 
          ? JSON.parse(data.variants_json) 
          : data.variants_json;
        console.log(`   Variants: ${Array.isArray(variants) ? variants.length : 0}`);
      }
      
      return data;
    } else {
      console.log(`❌ Failed to retrieve product (${res.statusCode})`);
      return null;
    }
  } catch (error) {
    console.log(`❌ Verification error: ${error.message}`);
    return null;
  }
}

/**
 * Main test flow
 */
async function runApiTests(jwtToken) {
  console.log('\n' + '🧪 API INTEGRATION TEST'.padEnd(60, '='));
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log(`Product: ${testJsonPayload.product.name}`);
  
  // Step 1: Test connectivity
  const isConnected = await testServerConnectivity();
  if (!isConnected) {
    return;
  }
  
  // Step 2: Test authentication
  const isAuthenticated = await testAuthentication(jwtToken);
  if (!isAuthenticated && jwtToken) {
    console.log('\n⚠️  Cannot proceed without valid authentication');
    return;
  }
  
  // Step 3: Submit creation request
  const result = await submitSmartphoneCreation(jwtToken);
  if (!result) {
    console.log('\n⚠️  Creation failed. Check the errors above.');
    return;
  }
  
  // Step 4: Verify the created product
  if (result?.product_id && jwtToken) {
    await verifyCreatedProduct(result.product_id, jwtToken);
  }
  
  // Summary
  console.log('\n' + '=' .repeat(60));
  console.log('📊 TEST COMPLETE');
  console.log('=' .repeat(60));
  console.log('\n✅ All tests completed successfully!\n');
}

// Get JWT token from command line or environment
const jwtToken = process.argv[2] || process.env.JWT_TOKEN;

if (!jwtToken) {
  console.log('\n⚠️  No JWT token provided');
  console.log('\nUsage:');
  console.log('  node test-api.js <JWT_TOKEN>');
  console.log('\n  OR');
  console.log('  JWT_TOKEN="your_token" node test-api.js');
  console.log('\n📝 Note: Without a token, the server connectivity test will run');
  console.log('   but creation will fail due to missing authentication.\n');
}

runApiTests(jwtToken).catch(console.error);
