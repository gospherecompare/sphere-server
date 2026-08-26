# ✅ Sony BRAVIA 5 TV - Complete Testing Checklist

## 📋 Pre-Testing Setup

### Prerequisites
- [ ] Node.js installed and working
- [ ] Server can start (`npm start` in `d:\technxt\server`)
- [ ] Client_1 admin UI running (`http://localhost:5173` or port 5174)
- [ ] PostgreSQL database running and accessible
- [ ] No other services blocking port 5000 (server), 5173 (client_1)

### Files Available
- [ ] `test-sony-bravia-5-tv.js` - Validation test
- [ ] `test-tv-api.js` - API integration test
- [ ] `tv-quick-start.js` - Interactive guide
- [ ] `SONY-BRAVIA-5-TV-TEST-GUIDE.md` - Complete guide
- [ ] `ADMIN-UI-TESTING-GUIDE.md` - UI step-by-step
- [ ] `SONY-BRAVIA-5-TESTING-README.md` - Quick reference

---

## 🧪 PHASE 1: Validation Testing (Terminal)

### Step 1.1: JSON Structure Validation
```bash
cd d:\technxt
node test-sony-bravia-5-tv.js
```

**Checklist:**
- [ ] Tests run without errors
- [ ] All 27 tests pass (✅ marks)
- [ ] Schema validation: 15/15 PASS
- [ ] Database simulation: 5 records ready
- [ ] Status shows "✅ READY FOR API SUBMISSION"

**Expected Output:** 100% pass rate, 5 store records

---

### Step 1.2: Server Connectivity
```bash
# Terminal 1: Start server
cd d:\technxt\server
npm start

# Look for: "Server running on port 5000"
```

**Checklist:**
- [ ] Server starts without errors
- [ ] Output shows "Server running on port 5000"
- [ ] No database connection errors
- [ ] Console shows no warnings

---

### Step 1.3: API Demo Test
```bash
# Terminal 2: (keep server running in Terminal 1)
cd d:\technxt
node test-tv-api.js --demo
```

**Checklist:**
- [ ] Test runs successfully
- [ ] Server connectivity check passes
- [ ] Payload preview displays (first 500 chars)
- [ ] Shows sample field values

**Expected:** Demo mode complete, payload preview shown

---

## 🔐 PHASE 2: API Authentication

### Step 2.1: Get JWT Token
```
1. Open: http://localhost:5173 (client_1 admin UI)
2. Login with admin credentials
3. Open DevTools: F12 → Network tab
4. Make any API call (click a button, navigate)
5. Look for API request with Authorization header
6. Copy the header value (format: Bearer eyJ...)
```

**Checklist:**
- [ ] Successfully logged in to client_1
- [ ] DevTools Network tab visible
- [ ] Can see API requests
- [ ] Authorization header found
- [ ] Token copied (eyJ... format)

**Token Format:** `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...`

---

### Step 2.2: Test JWT Token
```bash
# In Terminal 2:
JWT_TOKEN="your_full_token" node test-tv-api.js --demo

# Should show: "✅ Demo mode complete"
```

**Checklist:**
- [ ] No "Unauthorized" error
- [ ] Test completes successfully
- [ ] Token is valid format

---

## 🚀 PHASE 3: API Submission Test

### Step 3.1: Live API Submission
```bash
# In Terminal 2:
JWT_TOKEN="your_full_token" node test-tv-api.js
```

**Checklist:**
- [ ] Server connectivity: ✅ Connected
- [ ] API submission: Sent to POST /api/tvs
- [ ] Response status: 200 or 201 (Success)
- [ ] Response body shows success message
- [ ] No validation errors

**Expected Response:**
```json
{
  "success": true,
  "id": 123,
  "message": "TV product created successfully",
  "product_id": 123,
  "variants_created": 2,
  "stores_created": 5
}
```

---

### Step 3.2: Database Verification
```sql
-- Query 1: Product exists
SELECT id, product_name, category FROM products 
WHERE product_name LIKE 'Sony BRAVIA%'
LIMIT 1;
-- Expected: 1 row with product_name = 'Sony BRAVIA 5 55-inch 4K Mini LED Smart Google TV'

-- Query 2: Variants created
SELECT id, product_id, screen_size, base_price FROM product_variants 
WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%');
-- Expected: 2 rows
--   55 inch, base_price: 134990
--   65 inch, base_price: 139990

-- Query 3: Store prices (MOST IMPORTANT)
SELECT variant_id, store_name, price, url FROM variant_store_prices 
WHERE variant_id IN (
  SELECT id FROM product_variants 
  WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%')
)
ORDER BY variant_id, store_name;
-- Expected: 5 rows
--   V1: Sony India Official, 134990, https://www.sony.co.in/...
--   V1: Amazon India, 134990, https://www.amazon.in/...
--   V1: Flipkart, 183900, https://www.flipkart.com/...
--   V2: Amazon India, 139990, https://www.amazon.in/...
--   V2: Flipkart, 141990, https://www.flipkart.com/...
```

**Checklist:**
- [ ] Product found in database
- [ ] Exactly 2 variants exist
- [ ] Both variant prices correct (134990, 139990)
- [ ] Exactly 5 store_price records created
- [ ] All URLs populated (no NULL values)
- [ ] All prices are numeric (not NULL)
- [ ] All store_names populated

---

## 🎨 PHASE 4: Admin UI Testing

### Step 4.1: View Product in List
```
1. Open: http://localhost:5173 (if not already open)
2. Navigate: Products → TVs
3. Search for: "Sony BRAVIA"
```

**Checklist:**
- [ ] Product appears in TV list
- [ ] Shows product name, brand, category
- [ ] Has action buttons (Edit, Delete, View)
- [ ] Clicking product opens detail view

---

### Step 4.2: View Product Details
```
1. Click on "Sony BRAVIA 5 TV" in list
2. Review all sections
```

**Checklist:**
- [ ] Basic info displays correctly
  - [ ] Product name: Sony BRAVIA 5 55-inch...
  - [ ] Brand: Sony
  - [ ] Category: 4K Smart Mini LED TV
  - [ ] Model: K-55XR50

- [ ] Specifications sections present
  - [ ] Display specs visible
  - [ ] Video engine specs visible
  - [ ] Audio specs visible
  - [ ] Smart TV features visible
  - [ ] Gaming features visible
  - [ ] Ports info visible
  - [ ] Connectivity visible
  - [ ] Power specs visible
  - [ ] Physical specs visible
  - [ ] Warranty info visible

---

### Step 4.3: View Variants
```
1. Scroll to "Variants" section
2. Check both variants listed
```

**Checklist:**
- [ ] Variant 1 (55 inch) appears
  - [ ] Screen size: 55 inch
  - [ ] Model number: K-55XR50
  - [ ] Base price: ₹134,990

- [ ] Variant 2 (65 inch) appears
  - [ ] Screen size: 65 inch
  - [ ] Model number: K-65XR50
  - [ ] Base price: ₹139,990

---

### Step 4.4: View Store Pricing
```
1. Expand Variant 1 (55 inch)
2. Look for stores section
3. Verify all 3 stores display
4. Expand Variant 2 (65 inch)
5. Verify 2 stores display
```

**Checklist for 55" Variant:**
- [ ] Store 1: Sony India Official
  - [ ] Price: ₹134,990
  - [ ] URL present and clickable

- [ ] Store 2: Amazon India
  - [ ] Price: ₹134,990
  - [ ] URL present and clickable

- [ ] Store 3: Flipkart
  - [ ] Price: ₹183,900
  - [ ] URL present and clickable

**Checklist for 65" Variant:**
- [ ] Store 1: Amazon India
  - [ ] Price: ₹139,990
  - [ ] URL present and clickable

- [ ] Store 2: Flipkart
  - [ ] Price: ₹141,990
  - [ ] URL present and clickable

---

### Step 4.5: Edit Functionality
```
1. Click "Edit" button on product
2. Form opens with all fields populated
```

**Checklist:**
- [ ] All basic fields pre-filled
- [ ] All specifications pre-filled
- [ ] Both variants loaded
- [ ] All store prices visible
- [ ] Can modify fields without errors
- [ ] Can click "Cancel" or "Save Changes"

---

### Step 4.6: Price Update on Variant Change
```
1. Go to product detail page (public or admin)
2. Select variant "55 inch"
3. Check store prices displayed
4. Select variant "65 inch"
5. Check store prices changed
```

**Checklist:**
- [ ] Selecting 55" shows 3 stores
  - [ ] Sony India: ₹134,990
  - [ ] Amazon: ₹134,990
  - [ ] Flipkart: ₹183,900

- [ ] Selecting 65" shows 2 stores
  - [ ] Amazon: ₹139,990
  - [ ] Flipkart: ₹141,990

- [ ] Prices update correctly when variant changes
- [ ] No delays or errors in UI

---

## 🌐 PHASE 5: Frontend Testing

### Step 5.1: Product Search
```
1. Open frontend: http://localhost:3000 (or public view)
2. Search for: "Sony BRAVIA"
```

**Checklist:**
- [ ] Product appears in search results
- [ ] Shows product image, name, price
- [ ] Can click to view product page

---

### Step 5.2: Product Detail Page
```
1. Click on "Sony BRAVIA 5 TV" product
2. Product page loads
```

**Checklist:**
- [ ] Page loads without errors
- [ ] All specifications display
- [ ] Product images visible (if added)
- [ ] Variant selector visible

---

### Step 5.3: Variant Selection
```
1. Select "55 inch" variant
2. Observe store prices
3. Select "65 inch" variant
4. Observe store prices change
```

**Checklist:**
- [ ] Variant dropdown works
- [ ] Selecting variants updates display
- [ ] Store prices correct for each variant
- [ ] "Buy Now" buttons present

---

### Step 5.4: Store Links
```
1. For 55" variant, click "Buy on Sony India" link
2. Should open: https://www.sony.co.in/bravia/products/bravia-5-xr50
3. Back, select Amazon link for 55"
4. Should open: https://www.amazon.in/dp/B0F7X7WM8N
```

**Checklist:**
- [ ] Sony link opens correct URL
- [ ] Amazon link opens correct URL
- [ ] Flipkart link opens correct URL
- [ ] All links are functional (no 404s)
- [ ] Links open in new tabs

---

## 🔍 PHASE 6: Data Integrity Checks

### Step 6.1: URL Validation
```sql
-- Check no NULL URLs
SELECT COUNT(*) as null_urls FROM variant_store_prices 
WHERE variant_id IN (
  SELECT id FROM product_variants 
  WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%')
)
AND url IS NULL;
-- Expected: 0 (zero null URLs)
```

**Checklist:**
- [ ] Result shows 0 NULL URLs
- [ ] All URLs are populated

---

### Step 6.2: Price Validation
```sql
-- Check no NULL prices
SELECT COUNT(*) as null_prices FROM variant_store_prices 
WHERE variant_id IN (
  SELECT id FROM product_variants 
  WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%')
)
AND price IS NULL;
-- Expected: 0 (zero null prices)
```

**Checklist:**
- [ ] Result shows 0 NULL prices
- [ ] All prices are numeric values

---

### Step 6.3: Store Name Validation
```sql
-- Check no NULL store names
SELECT COUNT(*) as null_stores FROM variant_store_prices 
WHERE variant_id IN (
  SELECT id FROM product_variants 
  WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%')
)
AND store_name IS NULL;
-- Expected: 0 (zero null store names)
```

**Checklist:**
- [ ] Result shows 0 NULL store names
- [ ] All store_names are populated

---

### Step 6.4: Variant Count
```sql
-- Check exactly 2 variants
SELECT COUNT(*) as variant_count FROM product_variants 
WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%');
-- Expected: 2
```

**Checklist:**
- [ ] Result shows exactly 2 variants
- [ ] No duplicate variants created

---

### Step 6.5: Store Count
```sql
-- Check exactly 5 store records
SELECT COUNT(*) as store_count FROM variant_store_prices 
WHERE variant_id IN (
  SELECT id FROM product_variants 
  WHERE product_id = (SELECT id FROM products WHERE product_name LIKE 'Sony BRAVIA%')
);
-- Expected: 5
```

**Checklist:**
- [ ] Result shows exactly 5 store records
- [ ] No duplicate store entries

---

## 🎯 PHASE 7: Final Verification

### Overall Status Checklist

**Terminal Tests:**
- [ ] ✅ JSON validation: 27/27 PASS
- [ ] ✅ API demo: Payload verified
- [ ] ✅ API live: Submission successful
- [ ] ✅ Database: 5 records inserted

**Database Verification:**
- [ ] ✅ 1 product created
- [ ] ✅ 2 variants created
- [ ] ✅ 5 store prices created
- [ ] ✅ All URLs populated
- [ ] ✅ All prices populated
- [ ] ✅ All store names populated

**Admin UI:**
- [ ] ✅ Product visible in list
- [ ] ✅ Product details display correctly
- [ ] ✅ Both variants visible
- [ ] ✅ All 5 store prices visible
- [ ] ✅ Edit form works
- [ ] ✅ Price updates on variant change

**Frontend:**
- [ ] ✅ Product searchable
- [ ] ✅ Product detail page loads
- [ ] ✅ Variant selection works
- [ ] ✅ Store links functional
- [ ] ✅ Prices update correctly

---

## ❌ If Any Test Fails

### Troubleshooting Guide

**Terminal Test Fails:**
1. Check server is running: `npm start` in `d:\technxt\server`
2. Verify database connection in server console
3. Run: `node test-sony-bravia-5-tv.js` again

**API Submission Fails:**
1. Check JWT token is valid (get fresh from client_1)
2. Review server logs for validation errors
3. Ensure all variants have base_price
4. Ensure all stores have store_name AND url

**Database Records Missing:**
1. Check database connection
2. Run query: `SELECT * FROM products WHERE product_name LIKE 'Sony BRAVIA%';`
3. Verify server logs show INSERT statements
4. Check for any database constraints violations

**Admin UI Not Showing Data:**
1. Clear browser cache (Ctrl+Shift+Delete)
2. Refresh page (F5)
3. Restart server (`npm start`)
4. Check browser console for JavaScript errors

**Frontend Links Not Working:**
1. Check URLs in database are complete (https://...)
2. Verify URLs are in variant_store_prices table
3. Test URLs manually in browser
4. Check for typos in URL paths

---

## 📊 Test Summary

| Phase | Component | Status | Notes |
|-------|-----------|--------|-------|
| 1 | Validation | [ ] | Run test-sony-bravia-5-tv.js |
| 2 | Server | [ ] | npm start in server/ |
| 3 | API Demo | [ ] | node test-tv-api.js --demo |
| 2 | JWT Token | [ ] | Get from client_1 DevTools |
| 3 | API Live | [ ] | JWT_TOKEN=... node test-tv-api.js |
| 3 | Database | [ ] | Check 5 store records |
| 4 | Admin UI List | [ ] | Search for Sony BRAVIA |
| 4 | Admin UI Detail | [ ] | View product specs |
| 4 | Admin UI Variants | [ ] | Check both variants |
| 4 | Admin UI Stores | [ ] | Verify 5 store records |
| 5 | Frontend Search | [ ] | Search for product |
| 5 | Frontend Detail | [ ] | Load product page |
| 5 | Frontend Variants | [ ] | Change screen sizes |
| 5 | Frontend Links | [ ] | Test store URLs |
| 6 | Data Integrity | [ ] | No NULL values |

---

## 🏆 SUCCESS CRITERIA

**All tests pass when:**
- ✅ All 27 JSON validation tests pass
- ✅ API submission returns 200/201 status
- ✅ Database contains exactly 5 store_price records
- ✅ No NULL values in critical fields (url, price, store_name)
- ✅ Product visible in admin UI
- ✅ All 5 store prices display correctly in UI
- ✅ Frontend product page loads
- ✅ Variant selection works
- ✅ Store links are functional

---

**Last Updated:** 2026-08-26  
**Test Status:** Ready for execution  
**Expected Duration:** 30-45 minutes for complete testing
