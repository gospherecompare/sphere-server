# JSON Validation & API Testing Guide

This guide explains how to validate and test the iQOO Z11 5G smartphone JSON against both server and client-side logic.

## Test Files

### 1. `test-json-validation.js` - JSON Structure Validation

This file validates the JSON structure against:
- Server API expectations
- Client-side normalization logic  
- Database insertion rules

**Run without server (no dependencies):**
```bash
node test-json-validation.js
```

**Expected Output:**
```
✅ OVERALL RESULT: PASS - JSON is ready for API submission
```

**What it checks:**
- ✅ JSON schema completeness
- ✅ Variant structure (stores vs store_prices)
- ✅ Required fields (store_name, url, price)
- ✅ Field normalization (aliases like affiliate_url, link, etc)
- ✅ Database record insertion simulation

---

### 2. `test-api.js` - Live API Integration Test

This file tests the JSON against the actual running server API.

**Prerequisites:**
1. Server must be running on `localhost:5000`
2. You need a valid JWT authentication token

**Start the server:**
```bash
cd server
npm start
```

**Run the test (without authentication - connectivity check only):**
```bash
node test-api.js
```

**Run with authentication (full test including creation):**
```bash
node test-api.js "your_jwt_token_here"
```

Or using environment variable:
```bash
JWT_TOKEN="your_jwt_token" node test-api.js
```

**What it tests:**
- 🔗 Server connectivity on `http://localhost:5000`
- 🔐 Authentication with provided JWT token
- 📤 Submitting the smartphone creation request
- ✔️ Retrieving and verifying the created product

---

## JSON Structure Reference

The test JSON includes:

### Required Fields
| Field | Type | Example |
|-------|------|---------|
| `product.name` | string | "iQOO Z11 5G" |
| `product.brand_id` | number | 5 |
| `smartphone.model` | string | "Z11 5G" |
| `smartphone.brand` | string | "iQOO" |
| `variants` | array | [...] |

### Variants Structure
Each variant must have:
```json
{
  "ram": "8 GB",
  "storage": "256 GB",
  "base_price": 24999,
  "stores": [
    {
      "store_name": "Amazon",      // Required
      "price": 24999,              // Recommended
      "url": "https://...",        // Required
      "offer_text": "..."          // Optional
    }
  ]
}
```

### Store Fields Support

The API accepts either `stores` or `store_prices`:
```javascript
// Option 1: Using 'stores'
"stores": [...]

// Option 2: Using 'store_prices'  
"store_prices": [...]
```

Both are normalized by the server. The API also supports field aliases:
- `store_name` ← `store | storeName | display_store_name`
- `url` ← `link | affiliate_link | affiliateUrl`
- `price` ← `current_price | sale_price`

---

## Validation Checklist

Before submitting the JSON to the server:

- [ ] Run `node test-json-validation.js` and confirm **PASS**
- [ ] Check all variants have `store_name` and `url`
- [ ] Verify `base_price` and at least one `store.price` are numeric
- [ ] Ensure `brand_id` references an existing brand
- [ ] All JSON is valid (no syntax errors)
- [ ] All required spec sections are included (display, performance, camera, etc.)

---

## Common Issues & Solutions

### ❌ "FAIL - JSON is invalid"
**Solution:** Check JSON syntax with `node -e "console.log(JSON.parse(require('fs').readFileSync('your-file.json')))"`

### ❌ "Cannot connect to server"
**Solution:** Start the server first:
```bash
cd server && npm start
```

### ❌ "UNAUTHORIZED - Invalid authentication"
**Solution:** Use a valid JWT token from your authentication system

### ❌ "Variant N, Store M: Missing URL"
**Solution:** Ensure each store has a non-null `url` field

### ❌ "Missing store_name"
**Solution:** Each store must have a `store_name` field

---

## Expected Test Results

### Validation Test Success
```
✅ JSON Structure: VALID
✅ Server Expectations: PASS
✅ Client Normalization: PASS
✅ Database Records: 4 records ready for insertion
✅ OVERALL RESULT: PASS
```

### API Test Success (with JWT)
```
✅ Server is running
✅ Authentication successful
✅ CREATION SUCCESSFUL
   Product ID: 12345
✅ Product retrieved successfully
```

---

## How the Server Processes Your JSON

1. **Receives request** → `POST /api/smartphones`
2. **Normalizes store field** → accepts both `stores` and `store_prices`
3. **Normalizes field aliases** → `link` → `url`, etc.
4. **Validates required fields** → skips rows without `store_name` AND `url`
5. **Inserts into database**:
   - `products` table
   - `smartphones` table
   - `product_variants` table
   - `variant_store_prices` table
6. **Returns** → `product_id` on success

---

## Database Flow

```
Your JSON
    ↓
Server Normalization (stores/store_prices → stores)
    ↓
Field Alias Resolution (link → url, etc)
    ↓
Validation (store_name AND url required)
    ↓
Database Insert
    ├─ products
    ├─ smartphones
    ├─ product_variants
    └─ variant_store_prices
    ↓
GET API Returns
    ↓
Frontend Displays with Buy Links
```

---

## Testing Workflow

1. **Validate structure:**
   ```bash
   node test-json-validation.js
   ```

2. **Check server connectivity:**
   ```bash
   node test-api.js
   ```

3. **Test with authentication (if available):**
   ```bash
   node test-api.js "eyJhbGc..."
   ```

4. **Verify in frontend:**
   - Navigate to product page
   - Check variant prices display correctly
   - Click "Buy" button to verify store links work

---

## Support

If tests fail:
1. Check the error messages above
2. Run individual validation tests
3. Review the JSON structure against examples
4. Ensure server is running and accessible
5. Verify JWT token is valid and has admin privileges

For API issues, check `server/index.js` around line 14003 for the `POST /api/smartphones` endpoint.
