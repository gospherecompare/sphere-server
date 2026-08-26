#!/usr/bin/env node
/**
 * Quick Start: How to Get Authentication & Test API
 * 
 * This file shows step-by-step how to:
 * 1. Get a JWT token from your auth system
 * 2. Test the API with real data
 * 3. Verify the product was created
 */

console.log(`
╔════════════════════════════════════════════════════════════════╗
║  SMARTPHONE JSON TESTING - QUICK START GUIDE                   ║
║  Product: iQOO Z11 5G                                          ║
║  Variants: 8GB/256GB, 12GB/256GB                               ║
║  Stores: Amazon, Flipkart                                      ║
╚════════════════════════════════════════════════════════════════╝
`);

console.log(`
STEP 1️⃣  VALIDATE JSON STRUCTURE (No server needed)
─────────────────────────────────────────────────────
Command:
  $ node test-json-validation.js

Expected:
  ✅ OVERALL RESULT: PASS - JSON is ready for API submission

This confirms:
  ✅ JSON is valid
  ✅ All variants have stores
  ✅ All stores have name, url, and price
  ✅ Client-side normalization works
  ✅ Database will accept the records
`);

console.log(`
STEP 2️⃣  START THE SERVER
─────────────────────────────────────────────────────
Commands:
  $ cd server
  $ npm start

Expected output:
  Server running on port 5000
  Database connected
  
Wait for the server to fully start before proceeding.
`);

console.log(`
STEP 3️⃣  GET AUTHENTICATION TOKEN
─────────────────────────────────────────────────────

Option A: Using curl to login
  $ curl -X POST http://localhost:5000/api/login \\
    -H "Content-Type: application/json" \\
    -d '{
      "email": "admin@example.com",
      "password": "your-password"
    }'

Option B: Using node/fetch
  const response = await fetch('http://localhost:5000/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: 'admin@example.com',
      password: 'your-password'
    })
  });
  const data = await response.json();
  const token = data.token; // JWT token

Expected response:
  {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "user": { "id": 1, "role": "admin" }
  }

💡 Copy the token value for Step 4
`);

console.log(`
STEP 4️⃣  TEST API WITH AUTHENTICATION
─────────────────────────────────────────────────────
Command (replace YOUR_TOKEN):
  $ node test-api.js "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

Or using environment variable:
  $ JWT_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." node test-api.js

Expected output:
  ✅ Server is running on http://localhost:5000
  ✅ Authentication successful
  ✅ CREATION SUCCESSFUL
     Product ID: 12345
  ✅ Product retrieved successfully

This confirms:
  ✅ Server is running and accessible
  ✅ Authentication token is valid
  ✅ Smartphone was created in database
  ✅ Both variants were saved
  ✅ All store prices were saved
`);

console.log(`
STEP 5️⃣  VERIFY IN DATABASE
─────────────────────────────────────────────────────

You can verify the data was saved correctly:

PostgreSQL command:
  $ psql -h localhost -U postgres -d technxt

SQL queries:
  -- Check if product exists
  SELECT id, name FROM products WHERE name = 'iQOO Z11 5G';
  
  -- Check variants
  SELECT * FROM product_variants WHERE product_id = <PRODUCT_ID>;
  
  -- Check store prices
  SELECT * FROM variant_store_prices WHERE variant_id IN (
    SELECT id FROM product_variants WHERE product_id = <PRODUCT_ID>
  );

Expected:
  - 1 product record
  - 2 variant records (8GB and 12GB)
  - 4 variant_store_prices records (Amazon & Flipkart for each)
`);

console.log(`
STEP 6️⃣  VERIFY IN FRONTEND
─────────────────────────────────────────────────────

Open browser:
  http://localhost:5173/products/12345 (or your frontend port)

Check:
  ✅ Product name displays: "iQOO Z11 5G"
  ✅ Variants show with prices:
     - 8 GB / 256 GB - ₹24,999
     - 12 GB / 256 GB - ₹27,999
  ✅ Store links appear in variant card
  ✅ "Buy on Amazon" link is clickable
  ✅ "Buy on Flipkart" link is clickable
  ✅ URLs open to correct product pages
`);

console.log(`
TROUBLESHOOTING
─────────────────────────────────────────────────────

Issue: "Cannot connect to server"
Solution: Make sure server is running:
  $ cd server && npm start

Issue: "UNAUTHORIZED - Invalid authentication"  
Solution: Use a valid JWT token:
  1. Login via /api/login endpoint
  2. Copy the token from response
  3. Pass to test-api.js

Issue: "BAD REQUEST - Invalid brand_id"
Solution: Check that brand_id references an existing brand:
  SELECT id, name FROM brands WHERE name = 'iQOO';

Issue: "Store prices not showing in frontend"
Solution: Verify database has records:
  SELECT * FROM variant_store_prices 
  WHERE variant_id IN (
    SELECT id FROM product_variants WHERE product_id = <ID>
  );

Issue: "Buy links don't work"
Solution: Check URLs in database:
  SELECT store_name, url FROM variant_store_prices 
  WHERE variant_id = <VARIANT_ID>;
  
  URLs should be:
  https://www.amazon.in/example-iqoo-z11-8gb-256gb
  https://www.flipkart.com/example-iqoo-z11-8gb-256gb
`);

console.log(`
WHAT GETS SAVED
─────────────────────────────────────────────────────

Database Tables:
  products
    ├─ id: 123
    ├─ name: "iQOO Z11 5G"
    ├─ product_type: "smartphone"
    └─ brand_id: 5

  smartphones (detailed specs)
    ├─ product_id: 123
    ├─ model: "Z11 5G"
    ├─ display: { size, panel, resolution, ... }
    ├─ camera: { rear, front, ... }
    ├─ battery: { capacity, charging_power, ... }
    └─ ... (all specs)

  product_variants
    ├─ id: 456
    ├─ product_id: 123
    ├─ variant_key: "8_256"
    ├─ attributes: { ram: "8 GB", storage: "256 GB" }
    └─ base_price: 24999

  variant_store_prices (what shows in Buy button)
    ├─ variant_id: 456
    ├─ store_name: "Amazon"
    ├─ price: 24999
    └─ url: "https://www.amazon.in/example-iqoo-z11-8gb-256gb"
`);

console.log(`
API RESPONSE STRUCTURE
─────────────────────────────────────────────────────

Success (201 Created):
  {
    "message": "Smartphone created successfully",
    "product_id": 123,
    "ai_summary_status": "pending"
  }

Error (400 Bad Request):
  {
    "message": "Invalid field value or missing required field"
  }

Error (401 Unauthorized):
  {
    "message": "Authentication required"
  }

Error (500 Server Error):
  {
    "error": "Database or server error message"
  }
`);

console.log(`
NEXT STEPS
─────────────────────────────────────────────────────

After successful creation:

1. Update product visibility:
   PATCH /api/products/<ID>/publish

2. Assign news articles:
   POST /api/product-news

3. Configure in homepage:
   Add to featured products carousel

4. Test full purchase flow:
   Click store links → Verify redirects
   Add to cart → Verify variant prices

5. Monitor analytics:
   Track clicks on affiliate links
   Monitor conversion rates

6. Future improvements:
   Add more variants (different colors/storage)
   Update prices dynamically
   Add comparative ratings
`);

console.log(`
QUICK COMMANDS REFERENCE
─────────────────────────────────────────────────────

# Validate JSON
node test-json-validation.js

# Test API connectivity
node test-api.js

# Test with authentication
JWT_TOKEN="your_token" node test-api.js

# Check server is running
curl http://localhost:5000/api/health

# Login and get token
curl -X POST http://localhost:5000/api/login \\
  -H "Content-Type: application/json" \\
  -d '{"email":"user@example.com","password":"pass"}'

# View created product
curl http://localhost:5000/api/smartphones/<ID> \\
  -H "Authorization: Bearer <TOKEN>"

# Start server
cd server && npm start

# Start frontend
cd frontend && npm run dev

# Stop server
CTRL+C (in terminal running server)
`);

console.log(`
═══════════════════════════════════════════════════════════════
Ready to test? Start with: node test-json-validation.js
═══════════════════════════════════════════════════════════════
`);
