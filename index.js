// index _fixed.js
require("dotenv").config();
const express = require("express");
const cors = require("cors");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const ExcelJS = require("exceljs");
const { client, db } = require("./db");
const multer = require("multer");
const { sendRegistrationMail } = require("./utils/mailer");
const { authenticateCustomer, authenticate } = require("./middleware/auth");

const SECRET = process.env.JWT_SECRET || "smartarena_secret_key_25";
const PORT = process.env.PORT || 5000;

const app = express();

app.set("trust proxy", 1);

app.use(
  cors({
    origin: ["http://localhost:3000", "http://localhost:5173"],
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

app.use(express.json());

// important for preflight

const upload = multer({ storage: multer.memoryStorage() });

/* -----------------------
  Utilities
------------------------*/
function formatDateForExcel(input) {
  if (!input) return "";
  const d = new Date(input);
  if (isNaN(d)) return "";
  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();
  return `${day} ${month} ${year}`;
}

function parseDateForImport(val) {
  if (!val && val !== 0) return null;
  if (val instanceof Date) {
    if (isNaN(val)) return null;
    return val.toISOString();
  }
  const s = String(val).trim();
  if (!s) return null;
  // Accept YYYY-MM-DD or ISO or common formats
  // Try ISO first
  const iso = new Date(s);
  if (!isNaN(iso)) return iso.toISOString();
  // fallback to dd/mm/yyyy or dd-mm-yyyy
  const parts = s.match(/^(\d{1,2})[\s\/\-](\d{1,2})[\s\/\-](\d{4})$/);
  if (parts) {
    const day = Number(parts[1]);
    const month = Number(parts[2]) - 1;
    const year = Number(parts[3]);
    const d = new Date(Date.UTC(year, month, day));
    if (!isNaN(d)) return d.toISOString();
  }
  return null;
}

function normalizeBodyKeys(obj) {
  const out = {};
  if (!obj || typeof obj !== "object") return out;
  for (const k of Object.keys(obj)) {
    const nk = k.toLowerCase().replace(/[^a-z0-9]/g, "");
    out[nk] = obj[k];
  }
  return out;
}

/* -----------------------
  Migrations (all tables with   suffix)
------------------------*/
async function runMigrations() {
  try {
    // Helper to run migration queries but ignore duplicate pg_type errors
    async function safeQuery(sql) {
      try {
        await db.query(sql);
      } catch (err) {
        // Postgres may raise a unique violation on pg_type when a previous
        // failed attempt left a composite type with the same name.
        if (
          err &&
          err.code === "23505" &&
          err.constraint === "pg_type_typname_nsp_index"
        ) {
          console.warn(
            "Migration warning: duplicate pg_type detected, skipping:",
            err.detail || err.message
          );
          return;
        }
        throw err;
      }
    }
    // Migrations - create tables in dependency order

    // 1) brands
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS brands (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        logo TEXT,
        category TEXT,
        status TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // categories - simple taxonomy table used by admin UI
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        product_type TEXT UNIQUE,
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
      );`);

    // online_stores - stores used for variant store prices and links
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS online_stores (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        logo TEXT,
        status TEXT DEFAULT 'active',
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS store(
        id SERIAL PRIMARY KEY,
        store_name TEXT NOT NULL,
        logo_url TEXT NOT NULL,
        status TEXT,
        created_at TIMESTAMP DEFAULT now()
        );
        `);
    // 2) products (depends on brands)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        product_type TEXT CHECK (
          product_type IN ('smartphone','laptop','networking','home_appliance','accessories')
        ) NOT NULL,
        brand_id INT REFERENCES brands(id),
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 3) product_variants (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_variants (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        variant_key TEXT NOT NULL,
        attributes JSONB,
        base_price NUMERIC,
        created_at TIMESTAMP DEFAULT now(),
        CONSTRAINT unique_product_variant UNIQUE (product_id, variant_key)
      );
    `);

    // 4) product_images (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_images (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        image_url TEXT NOT NULL,
        position INT,
        UNIQUE (product_id, image_url)
      );
    `);

    // 5) smartphones (depends on products)
    // Provide an internal id PK and keep product_id as FK to products
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS smartphones (
        id SERIAL PRIMARY KEY,
        product_id INT UNIQUE REFERENCES products(id) ON DELETE CASCADE,
        category TEXT,
        brand TEXT,
        model TEXT,
        launch_date DATE,
        images JSONB,
        colors JSONB,
        build_design JSONB,
        display JSONB,
        performance JSONB,
        camera JSONB,
        battery JSONB,
        connectivity_network JSONB,
        ports JSONB,
        audio JSONB,
        multimedia JSONB,
        sensors JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 6) smartphone_variants (depends on smartphones)

    // 7) variant_store_prices (depends on smartphone_variants)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS variant_store_prices (
        id SERIAL PRIMARY KEY,
        variant_id INT NOT NULL REFERENCES product_variants(id) ON DELETE CASCADE,
        store_name TEXT NOT NULL,
        price NUMERIC,
        url TEXT,
        offer_text TEXT,
        delivery_info TEXT,
        created_at TIMESTAMP DEFAULT now(),
        UNIQUE (variant_id, store_name)
      );
    `);

    // 8) smartphone_publish (depends on smartphones)

    // 8) Customers (customers table used by ratings)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS Customers(
        id SERIAL PRIMARY KEY,
        f_name TEXT NOT NULL,
        l_name TEXT NOT NULL,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        city TEXT,
        country TEXT,
        state TEXT,
        zip_code TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 9) smartphone_ratings (depends on smartphones and Customers)

    // 10) home_appliance (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS home_appliance (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        appliance_type TEXT CHECK (
          appliance_type IN ('washing_machine','air_conditioner','refrigerator','television')
        ),
        model_number TEXT,
        release_year INT,
        country_of_origin TEXT,
        specifications JSONB,
        features JSONB,
        performance JSONB,
        physical_details JSONB,
        warranty JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 11) laptop (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS laptop (
        product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
        cpu JSONB,
        display JSONB,
        memory JSONB,
        storage JSONB,
        battery JSONB,
        connectivity JSONB,
        physical JSONB,
        software JSONB,
        features JSONB,
        warranty JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    //12) networking (depends on products)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS networking (
        product_id INT PRIMARY KEY
          REFERENCES products(id)
          ON DELETE CASCADE,
      
          device_type TEXT NOT NULL CHECK (
            device_type IN (
              'router',
              'modem',
              'switch',
              'mesh',
              'extender'
            )
          ),
                  model_number TEXT,
        release_year INT,
        country_of_origin TEXT,
      
        specifications JSONB,   -- speeds, ports, bands
        features JSONB,         -- QoS, parental control, MU-MIMO
        performance JSONB,      -- throughput, coverage
        connectivity JSONB,     -- wifi standards, ethernet
        physical_details JSONB, -- dimensions, weight
        warranty JSONB,
      
        created_at TIMESTAMP DEFAULT now()
      );
      `);

    // 12) ram_storage_long
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS ram_storage_long (
        id SERIAL PRIMARY KEY,
        ram TEXT,
        storage TEXT,
        long TEXT, --- add type here later
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // 13) user (application users/admins)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS "user" (
        id SERIAL PRIMARY KEY,
        user_name TEXT,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        gender TEXT,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'admin',
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_publish (
      product_id INT PRIMARY KEY REFERENCES products(id),
      is_published BOOLEAN DEFAULT FALSE,
      published_by INT REFERENCES "user"(id),
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    `);

    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_ratings (
        id SERIAL PRIMARY KEY,
        product_id INT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        user_id INT NOT NULL REFERENCES "user"(id),
        overall_rating INT CHECK (overall_rating BETWEEN 1 AND 5),
        review TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (product_id, user_id)
      );
      `);

    console.log("✅ Migrations to   completed");
  } catch (err) {
    console.error("Migration error:", err);
    throw err;
  }
}

/* -----------------------
  Auth Middleware + Role-Based Access Control (RBAC)
------------------------*/

/* -----------------------
  AUTH Routes
------------------------*/
app.post("/api/auth/register", async (req, res) => {
  try {
    const b = req.body || {};
    const user_name = b.user_name || null;
    const first_name = b.first_name || null;
    const last_name = b.last_name || null;
    const phone = b.phone || null;
    const gender = b.gender || null;
    const email = b.email;
    const password = b.password || `${Math.random()}${Date.now()}`;
    const role = b.role || "admin";

    if (!email || !password) {
      return res.status(400).json({ message: "email and password required" });
    }

    const hashed = await bcrypt.hash(password, 10);

    const result = await db.query(
      `INSERT INTO "user"
        (user_name, first_name, last_name, phone, gender, email, password, role)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING id, email, role`,
      [user_name, first_name, last_name, phone, gender, email, hashed, role]
    );

    res.status(201).json({
      message: "User registered successfully. Email sent.",
      user: result.rows[0],
    });
  } catch (err) {
    if (err.code === "23505") {
      return res.status(409).json({ message: "Email already registered" });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password)
      return res.status(400).json({ message: "email and password required" });

    const result = await db.query('SELECT * FROM "user" WHERE email = $1', [
      email,
    ]);
    if (!result.rows.length)
      return res.status(401).json({ message: "Invalid credentials" });

    const user = result.rows[0];
    const match = await bcrypt.compare(password, user.password);
    if (!match) return res.status(401).json({ message: "Invalid credentials" });

    const token = jwt.sign(
      {
        id: user.id,
        email: user.email,
        role: user.role,
        username: user.user_name,
      },
      SECRET,
      { expiresIn: "1h" }
    );

    res.json({
      message: "Login successful",
      token,
      user: {
        id: user.id,
        email: user.email,
        role: user.role,
        username: user.user_name,
      },
    });
  } catch (err) {
    console.error("Login error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ----customer auth */
app.post("/api/auth/customer/register", async (req, res) => {
  try {
    const {
      f_name,
      l_name,
      username: rawUsername,
      email: rawEmail,
      password,
      city,
      country,
      state,
      zip_code,
    } = req.body || {};

    // Basic required fields
    if (!f_name || !l_name || !rawUsername || !rawEmail || !password) {
      return res.status(400).json({
        message: "f_name, l_name, username, email, and password are required",
      });
    }

    // Normalize inputs
    const username = String(rawUsername).trim();
    const email = String(rawEmail).trim().toLowerCase();

    // Validate username: allow letters, numbers and underscores, 3-30 chars
    const usernameRegex = /^[A-Za-z0-9_]{3,30}$/;
    if (!usernameRegex.test(username)) {
      return res.status(400).json({
        message:
          "Invalid username. Use 3-30 characters: letters, numbers, and underscores only",
      });
    }

    // Validate email simple format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Validate password length
    if (String(password).length < 6) {
      return res
        .status(400)
        .json({ message: "Password must be at least 6 characters long" });
    }

    // Check uniqueness against Customers table
    const exists = await db.query(
      `SELECT id FROM Customers WHERE email = $1 OR username = $2`,
      [email, username]
    );

    if (exists.rows.length > 0) {
      return res
        .status(409)
        .json({ message: "Email or username already registered" });
    }

    // 3️⃣ Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // 4️⃣ Insert customer
    const result = await db.query(
      `INSERT INTO Customers
       (f_name, l_name, username, email, password, city, country, state, zip_code)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       RETURNING id, f_name, l_name, username, email, created_at`,
      [
        f_name,
        l_name,
        username,
        email,
        hashedPassword,
        city || null,
        country || null,
        state || null,
        zip_code || null,
      ]
    );

    // 5️⃣ Success response
    res.status(201).json({
      message: "Customer registered successfully",
      customer: result.rows[0],
    });
  } catch (err) {
    console.error("Customer register error:", err);
    res.status(500).json({ message: "Internal server error" });
  }
});

// Check username availability for signup form
app.get("/api/auth/check-username", async (req, res) => {
  try {
    const q = String(req.query.username || "").trim();
    if (!q) return res.json({ available: false });
    const usernameRegex = /^[A-Za-z0-9_]{3,30}$/;
    if (!usernameRegex.test(q)) return res.json({ available: false });

    const result = await db.query(
      `SELECT id FROM Customers WHERE username = $1 LIMIT 1`,
      [q]
    );
    return res.json({ available: result.rows.length === 0 });
  } catch (err) {
    console.error("check-username error:", err);
    return res.status(500).json({ available: false });
  }
});

// Check email availability for signup form
app.get("/api/auth/check-email", async (req, res) => {
  try {
    const q = String(req.query.email || "")
      .trim()
      .toLowerCase();
    if (!q) return res.json({ available: false });
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(q)) return res.json({ available: false });

    const result = await db.query(
      `SELECT id FROM Customers WHERE email = $1 LIMIT 1`,
      [q]
    );
    return res.json({ available: result.rows.length === 0 });
  } catch (err) {
    console.error("check-email error:", err);
    return res.status(500).json({ available: false });
  }
});
app.post("/api/auth/customer/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password)
      return res.status(400).json({ message: "email and password required" });

    const result = await db.query("SELECT * FROM customers WHERE email = $1", [
      email,
    ]);
    if (!result.rows.length)
      return res.status(401).json({ message: "Invalid credentials" });

    const customer = result.rows[0];
    const match = await bcrypt.compare(password, customer.password);
    if (!match) return res.status(401).json({ message: "Invalid credentials" });

    const token = jwt.sign(
      {
        email: customer.email,
        role: customer.role,
        username: customer.user_name,
      },
      SECRET,
      { expiresIn: "7d" }
    );

    res.json({
      message: "Login successful",
      token,
      user: {
        id: customer.id,
        email: customer.email,
        role: customer.role,
        username: customer.user_name,
      },
    });
  } catch (err) {
    console.error("Customer login error:", err);
    res.status(500).json({ error: err.message });
  }
});

/*--- ratings smartphones  ---*/
app.post(
  "/api/public/smartphone/:smartphoneId/rating",
  authenticateCustomer,
  async (req, res) => {
    console.log(req.body);
    try {
      const smartphoneId = Number(req.params.smartphoneId);

      if (!smartphoneId)
        return res.status(400).json({ message: "Invalid smartphone id" });

      // Prefer authenticated customer info when available
      const customer = req.customer || {};
      const user_id =
        customer.id || (req.body.user_id ? Number(req.body.user_id) : null);
      const user_name = customer.username || req.body.user_name || null;

      // Expect rating values in body
      const { display, performance, camera, battery, design } = req.body;

      if (!user_id || !user_name) {
        return res
          .status(400)
          .json({ message: "user_id and user_name required" });
      }

      const ratings = [display, performance, camera, battery, design];
      if (ratings.some((r) => typeof r !== "number" || r < 0 || r > 5)) {
        return res.status(400).json({
          message: "All ratings must be numbers between 0 and 5",
        });
      }

      // Calculate overall rating
      const overallRating =
        (display + performance + camera + battery + design) / 5;

      await db.query(
        `
      INSERT INTO smartphone_ratings 
        (smartphone_id, user_id, user_name, display, performance, camera, battery, design, overall_rating)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9);
      `,
        [
          smartphoneId,
          user_id,
          user_name,
          display,
          performance,
          camera,
          battery,
          design,
          overallRating,
        ]
      );

      res.status(201).json({
        message: "Rating submitted successfully",
        overallRating: Number(overallRating.toFixed(1)),
      });
    } catch (err) {
      console.error("POST rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }
);

app.get("/api/public/smartphone/:smartphoneId/rating", async (req, res) => {
  try {
    const smartphoneId = Number(req.params.smartphoneId);
    if (!smartphoneId)
      return res.status(400).json({ message: "Invalid smartphone id" });

    const result = await db.query(
      `
      SELECT
        ROUND(AVG(overall_rating), 1) AS "averageRating",
        COUNT(*) AS "totalRatings",
        ROUND(AVG(display), 1) AS display,
        ROUND(AVG(performance), 1) AS performance,
        ROUND(AVG(camera), 1) AS camera,
        ROUND(AVG(battery), 1) AS battery,
        ROUND(AVG(design), 1) AS design
      FROM smartphone_ratings 
      WHERE smartphone_id = $1;
      `,
      [smartphoneId]
    );

    res.json({
      smartphoneId,
      ...result.rows[0],
    });
  } catch (err) {
    console.error("Public GET rating error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put(
  "/api/private/smartphone/:smartphoneId/rating",
  authenticate,

  async (req, res) => {
    console.log(req.body);
    console.log(req.params.smartphoneId);
    try {
      const smartphoneId = Number(req.params.smartphoneId);
      if (!smartphoneId) {
        return res.status(400).json({ message: "Invalid smartphone id" });
      }

      const { display, performance, camera, battery, design } = req.body;

      // Validation
      const ratings = [display, performance, camera, battery, design];
      if (ratings.some((r) => typeof r !== "number" || r < 0 || r > 5)) {
        return res.status(400).json({
          message: "All rating values must be numbers between 0 and 5",
        });
      }

      // Calculate overall rating
      const overallRating =
        (display + performance + camera + battery + design) / 5;

      const result = await db.query(
        `
        UPDATE smartphone_ratings 
        SET
          display = $1,
          performance = $2,
          camera = $3,
          battery = $4,
          design = $5,
          overall_rating = $6
        WHERE id = (
          SELECT id
          FROM smartphone_ratings 
          WHERE smartphone_id = $7
          ORDER BY created_at DESC
          LIMIT 1
        )
        RETURNING *;
        `,
        [
          display,
          performance,
          camera,
          battery,
          design,
          Number(overallRating.toFixed(2)),
          smartphoneId,
        ]
      );

      if (result.rowCount === 0) {
        return res.status(404).json({
          message: "No rating found to update for this smartphone",
        });
      }

      res.json({
        message: "Rating updated successfully",
        data: result.rows[0],
      });
    } catch (err) {
      console.error("PUT rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }
);

app.delete("/api/private/smartphone/:smartphoneId/rating", async (req, res) => {
  await db.query(`DELETE FROM smartphone_ratings  WHERE smartphone_id=$1`, [
    req.params.smartphoneId,
  ]);

  res.json({ message: "All ratings deleted" });
});

/* -----------------------
  Smartphones CRUD (Option B input format) - tables with   suffix
------------------------*/

// Create smartphone (with variants + variant_store_prices )
app.post("/api/smartphones", authenticate, async (req, res) => {
  const client = await db.connect();

  try {
    const { product, smartphone, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* ---------- 1. INSERT PRODUCT ---------- */
    const productRes = await client.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1, 'smartphone', $2)
      RETURNING id
      `,
      [product.name, product.brand_id]
    );

    const productId = productRes.rows[0].id;

    /* ---------- 2. INSERT SMARTPHONE ---------- */
    const smartphoneRes = await client.query(
      `
      INSERT INTO smartphones (
        product_id, category, brand, model, launch_date,
        images, colors, build_design, display, performance,
        camera, battery, connectivity_network,
        ports, audio, multimedia, sensors
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17
      )
      RETURNING id
      `,
      [
        productId,
        smartphone.category || null,
        smartphone.brand || null,
        smartphone.model || null,
        smartphone.launch_date || null,
        JSON.stringify(images || []),
        JSON.stringify(smartphone.colors || []),
        JSON.stringify(smartphone.build_design || {}),
        JSON.stringify(smartphone.display || {}),
        JSON.stringify(smartphone.performance || {}),
        JSON.stringify(smartphone.camera || {}),
        JSON.stringify(smartphone.battery || {}),
        JSON.stringify(smartphone.connectivity_network || {}),
        JSON.stringify(smartphone.ports || {}),
        JSON.stringify(smartphone.audio || {}),
        JSON.stringify(smartphone.multimedia || {}),
        // sensors as 16th param (if present)
        smartphone.sensors === null
          ? null
          : JSON.stringify(smartphone.sensors || []),
      ]
    );

    const smartphoneId = smartphoneRes.rows[0].id;

    /* ---------- 3. INSERT PRODUCT IMAGES ---------- */
    for (const url of images) {
      await client.query(
        `
        INSERT INTO product_images (product_id, image_url)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        `,
        [productId, url]
      );
    }

    /* ---------- 4. INSERT VARIANTS + STORE PRICES (into product_variants) ---------- */
    for (const v of variants) {
      // Build a stable variant_key. Prefer explicit `variant_key` from request,
      // otherwise fallback to ram_storage combination.
      const variantKey =
        (v.variant_key && String(v.variant_key).trim()) ||
        `${String(v.ram || "na")}_${String(v.storage || "na")}`;

      // Build attributes JSON merging any provided `attributes` object
      // with well-known fields like ram/storage.
      const attrsObj = Object.assign({}, v.attributes || {});
      if (v.ram !== undefined) attrsObj.ram = v.ram;
      if (v.storage !== undefined) attrsObj.storage = v.storage;

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (product_id, variant_key)
        DO UPDATE SET base_price = EXCLUDED.base_price, attributes = EXCLUDED.attributes
        RETURNING id
        `,
        [productId, variantKey, JSON.stringify(attrsObj), v.base_price || null]
      );

      const variantId = variantRes.rows[0].id;

      for (const sp of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          ON CONFLICT (variant_id, store_name)
          DO UPDATE SET
            price = EXCLUDED.price,
            url = EXCLUDED.url,
            offer_text = EXCLUDED.offer_text
          `,
          [
            variantId,
            sp.store_name,
            sp.price || null,
            sp.url || null,
            sp.offer_text || null,
          ]
        );
      }
    }

    await client.query("COMMIT");

    res.status(201).json({
      message: "Smartphone created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

//posting product to table products
app.post("/api/products", authenticate, async (req, res) => {
  try {
    const { name, product_type, brand_id } = req.body;

    if (!name || !product_type || !brand_id) {
      return res.status(400).json({
        message: "name, product_type and brand_id are required",
      });
    }

    const allowedTypes = [
      "smartphone",
      "laptop",
      "networking",
      "home_appliance",
      "accessories",
    ];

    if (!allowedTypes.includes(product_type)) {
      return res.status(400).json({ message: "Invalid product_type" });
    }

    const r = await db.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1,$2,$3)
      RETURNING *
      `,
      [name, product_type, brand_id]
    );

    res.status(201).json({
      message: "Product created",
      data: r.rows[0],
    });
  } catch (err) {
    console.error("POST /api/products error:", err);

    if (err.code === "23503") {
      return res.status(400).json({ message: "Invalid brand_id" });
    }

    res.status(500).json({ error: err.message });
  }
});

// Get published smartphones (public) - returns flattened variants as separate rows
// Get published smartphones (public) - nested structure
app.get("/api/smartphones", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        s.category,
        s.model,
        s.launch_date,
        s.colors,
        s.build_design,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity_network,
        s.ports,
        s.audio,
        s.multimedia,
        s.sensors,
        s.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Rating ---------- */
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'ram', v.attributes->>'ram',
              'storage', v.attributes->>'storage',
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN smartphones s
        ON s.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      WHERE p.product_type = 'smartphone'

      GROUP BY
        p.id, b.name,
        s.category, s.model, s.launch_date,
        s.colors, s.build_design, s.display, s.performance,
        s.camera, s.battery, s.connectivity_network,
        s.ports, s.audio, s.multimedia, s.sensors, s.created_at

      ORDER BY p.id DESC;
    `);

    res.json({ smartphones: result.rows });
  } catch (err) {
    console.error("GET /api/smartphones error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get all smartphones (authenticated) — full data
app.get("/api/smartphone", authenticate, async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        s.category,
        s.model,
        s.launch_date,
        s.colors,
        s.build_design,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.connectivity_network,
        s.ports,
        s.audio,
        s.multimedia,
        s.sensors,
        s.created_at,

        COALESCE(pub.is_published, false) AS is_published

      FROM products p
      INNER JOIN smartphones s
        ON s.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      WHERE p.product_type = 'smartphone'

      ORDER BY p.id DESC
    `);

    res.json({ smartphones: result.rows });
  } catch (err) {
    console.error("GET /api/smartphones error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get smartphone by id with variants and store prices
// Accept either internal `smartphones.id` or `product_id` (product's id).
app.get("/api/smartphone/:id", async (req, res) => {
  try {
    const rawId = req.params.id;
    const sid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    // Try to find by internal smartphones.id OR by product_id (product's id)
    const sres = await db.query(
      "SELECT * FROM smartphones WHERE id = $1 OR product_id = $1 LIMIT 1",
      [sid]
    );
    if (!sres.rows.length) {
      // If the id wasn't numeric or no match by numeric id, also try matching by model or product id string
      if (isNaN(sid)) {
        const sres2 = await db.query(
          "SELECT * FROM smartphones WHERE model = $1 OR brand = $1 LIMIT 1",
          [rawId]
        );
        if (!sres2.rows.length)
          return res.status(404).json({ message: "Not found" });
        const smartphone = sres2.rows[0];
        const productId = smartphone.product_id;
        const variantsRes = await db.query(
          "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
          [productId]
        );

        const variants = [];
        for (const v of variantsRes.rows) {
          const stores = await db.query(
            "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
            [v.id]
          );
          const ram = v.attributes ? v.attributes.ram || null : null;
          const storage = v.attributes ? v.attributes.storage || null : null;
          variants.push({ ...v, ram, storage, store_prices: stores.rows });
        }

        return res.json({ data: { ...smartphone, variants } });
      }
      return res.status(404).json({ message: "Not found" });
    }

    const smartphone = sres.rows[0];
    const productId = smartphone.product_id;
    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId]
    );

    const variants = [];
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
        [v.id]
      );
      const ram = v.attributes ? v.attributes.ram || null : null;
      const storage = v.attributes ? v.attributes.storage || null : null;
      variants.push({ ...v, ram, storage, store_prices: stores.rows });
    }

    return res.json({ data: { ...smartphone, variants } });
  } catch (err) {
    console.error("GET /api/smartphone/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.post("/api/laptops", authenticate, async (req, res) => {
  const client = await db.connect();

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const { product, laptop, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* 1️⃣ Product */
    const productRes = await client.query(
      `
      INSERT INTO products (name, product_type, brand_id)
      VALUES ($1, 'laptop', $2)
      RETURNING id
      `,
      [product.name, product.brand_id]
    );
    const productId = productRes.rows[0].id;

    /* 2️⃣ Laptop table (JSONB SAFE) */
    await client.query(
      `
      INSERT INTO laptop (
        product_id, cpu, display, memory, storage, battery,
        connectivity, physical, software, features, warranty
      )
      VALUES (
        $1,
        $2::jsonb,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,
        $7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb
      )
      `,
      [
        productId,
        JSON.stringify(laptop.cpu || {}),
        JSON.stringify(laptop.display || {}),
        JSON.stringify(laptop.memory || {}),
        JSON.stringify(laptop.storage || {}),
        JSON.stringify(laptop.battery || {}),
        JSON.stringify(laptop.connectivity || {}),
        JSON.stringify(laptop.physical || {}),
        JSON.stringify(laptop.software || {}),
        JSON.stringify(laptop.features || []),
        JSON.stringify(laptop.warranty || {}),
      ]
    );

    /* 3️⃣ Images */
    for (const url of images) {
      await client.query(
        `INSERT INTO product_images (product_id, image_url)
         VALUES ($1,$2)`,
        [productId, url]
      );
    }

    /* 4️⃣ Variants + Store Prices */
    for (const v of variants) {
      const variantKey = `${v.ram}_${v.storage}`; // 🔥 FIXED (NOT NULL)

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [
          productId,
          variantKey,
          JSON.stringify({ ram: v.ram, storage: v.storage }),
          v.base_price || null,
        ]
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [
            variantId,
            s.store_name,
            s.price || null,
            s.url || null,
            s.offer_text || null,
          ]
        );
      }
    }

    /* 5️⃣ Publish default */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId]
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Laptop created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("POST /api/laptops error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/laptops", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Rating ---------- */
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'ram', v.attributes->>'ram',
              'storage', v.attributes->>'storage',
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN laptop l
        ON l.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      WHERE p.product_type = 'laptop'

      GROUP BY
        p.id,
        b.name,
        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.created_at

      ORDER BY p.created_at DESC;
    `);

    res.json({ laptops: result.rows });
  } catch (err) {
    console.error("GET /api/laptops error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/homeappliances", authenticate, async (req, res) => {
  const client = await db.connect();
  const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const { product, home_appliance, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* ---------- 1️⃣ Insert product ---------- */
    const productRes = await client.query(
      `
      INSERT INTO products (name, brand_id, product_type)
      VALUES ($1,$2,'home_appliance')
      RETURNING id
      `,
      [product.name, product.brand_id]
    );

    const productId = productRes.rows[0].id;

    /* ---------- 2️⃣ Insert home appliance ---------- */
    await client.query(
      `
      INSERT INTO home_appliance (
        product_id,
        appliance_type,
        model_number,
        release_year,
        country_of_origin,
        specifications,
        features,
        performance,
        physical_details,
        warranty
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb
      )
      `,
      [
        productId,
        home_appliance.appliance_type,
        home_appliance.model_number,
        home_appliance.release_year,
        home_appliance.country_of_origin,
        toJSON(home_appliance.specifications),
        toJSON(home_appliance.features),
        toJSON(home_appliance.performance),
        toJSON(home_appliance.physical_details),
        toJSON(home_appliance.warranty),
      ]
    );

    /* ---------- 3️⃣ Images ---------- */
    for (const url of images) {
      await client.query(
        `INSERT INTO product_images (product_id, image_url) VALUES ($1,$2)`,
        [productId, url]
      );
    }

    /* ---------- 4️⃣ Variants + Stores ---------- */
    for (const v of variants) {
      if (!v.variant_key) {
        throw new Error("variant_key is required");
      }

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [productId, v.variant_key, JSON.stringify(v), v.base_price]
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [variantId, s.store_name, s.price, s.url, s.offer_text || null]
        );
      }
    }

    /* ---------- 5️⃣ Publish default ---------- */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId]
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Home appliance created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("POST /api/home-appliances error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/homeappliances", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,

        ha.appliance_type,
        ha.model_number,
        ha.release_year,
        ha.country_of_origin,
        ha.specifications,
        ha.features,
        ha.performance,
        ha.physical_details,
        ha.warranty,
        ha.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Rating ---------- */
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'attributes', v.attributes,
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p
      INNER JOIN home_appliance ha ON ha.product_id = p.id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN product_variants v ON v.product_id = p.id

      WHERE p.product_type = 'home_appliance'

      GROUP BY
        p.id, b.name,
        ha.appliance_type,
        ha.model_number,
        ha.release_year,
        ha.country_of_origin,
        ha.specifications,
        ha.features,
        ha.performance,
        ha.physical_details,
        ha.warranty,
        ha.created_at

      ORDER BY p.id DESC;
    `);

    res.json({ home_appliances: result.rows });
  } catch (err) {
    console.error("GET /api/home-appliances error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/networking", authenticate, async (req, res) => {
  const client = await db.connect();
  const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const { product, networking, images = [], variants = [] } = req.body;

    await client.query("BEGIN");

    /* ---------- 1️⃣ Insert product ---------- */
    const productRes = await client.query(
      `
      INSERT INTO products (name, brand_id, product_type)
      VALUES ($1,$2,'networking')
      RETURNING id
      `,
      [product.name, product.brand_id]
    );

    const productId = productRes.rows[0].id;

    /* ---------- 2️⃣ Insert networking (DB validates device_type) ---------- */
    await client.query(
      `
      INSERT INTO networking (
        product_id,
        device_type,
        model_number,
        release_year,
        country_of_origin,
        specifications,
        features,
        performance,
        connectivity,
        physical_details,
        warranty
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6::jsonb,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb
      )
      `,
      [
        productId,
        networking.device_type, // CHECK constraint enforces validity
        networking.model_number,
        networking.release_year,
        networking.country_of_origin,
        toJSON(networking.specifications),
        toJSON(networking.features),
        toJSON(networking.performance),
        toJSON(networking.connectivity),
        toJSON(networking.physical_details),
        toJSON(networking.warranty),
      ]
    );

    /* ---------- 3️⃣ Images ---------- */
    for (const url of images) {
      await client.query(
        `INSERT INTO product_images (product_id, image_url)
         VALUES ($1,$2)`,
        [productId, url]
      );
    }

    /* ---------- 4️⃣ Variants + Store Prices ---------- */
    for (const v of variants) {
      if (!v.variant_key) {
        throw new Error("variant_key is required");
      }

      const variantRes = await client.query(
        `
        INSERT INTO product_variants
          (product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3::jsonb,$4)
        RETURNING id
        `,
        [productId, v.variant_key, JSON.stringify(v), v.base_price]
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [variantId, s.store_name, s.price, s.url, s.offer_text || null]
        );
      }
    }

    /* ---------- 5️⃣ Publish default false ---------- */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId]
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Networking product created successfully",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");

    /* ---------- Handle CHECK constraint error ---------- */
    if (err.code === "23514") {
      return res.status(400).json({
        error: "Invalid device_type value",
      });
    }

    console.error("POST /api/networking error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.get("/api/networking", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        n.device_type,
        n.model_number,
        n.release_year,
        n.country_of_origin,

        /* ---------- JSONB Columns ---------- */
        n.specifications,
        n.features,
        n.performance,
        n.connectivity,
        n.physical_details,
        n.warranty,

        n.created_at,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

        /* ---------- Rating ---------- */
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,

        /* ---------- Variants + Store Prices ---------- */
        COALESCE(
          json_agg(
            DISTINCT jsonb_build_object(
              'variant_id', v.id,
              'variant_key', v.variant_key,
              'attributes', v.attributes,
              'base_price', v.base_price,
              'store_prices', (
                SELECT COALESCE(
                  json_agg(
                    jsonb_build_object(
                      'id', sp.id,
                      'store_name', sp.store_name,
                      'price', sp.price,
                      'url', sp.url,
                      'offer_text', sp.offer_text,
                      'delivery_info', sp.delivery_info
                    )
                  ),
                  '[]'::json
                )
                FROM variant_store_prices sp
                WHERE sp.variant_id = v.id
              )
            )
          ) FILTER (WHERE v.id IS NOT NULL),
          '[]'::json
        ) AS variants

      FROM products p

      INNER JOIN networking n
        ON n.product_id = p.id

      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      WHERE p.product_type = 'networking'

      GROUP BY
        p.id, b.name,
        n.device_type,
        n.model_number,
        n.release_year,
        n.country_of_origin,
        n.specifications,
        n.features,
        n.performance,
        n.connectivity,
        n.physical_details,
        n.warranty,
        n.created_at

      ORDER BY p.id DESC;
    `);

    res.json({ networking: result.rows });
  } catch (err) {
    console.error("GET /api/networking error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Update smartphone (replace variants & variant_store_prices  if provided) - authenticated
app.put("/api/smartphone/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  console.log(req.body);
  try {
    await client.query("BEGIN");

    const sid = Number(req.params.id);
    if (!sid || Number.isNaN(sid)) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Invalid id" });
    }

    const n = normalizeBodyKeys(req.body || {});
    const name = n.name || req.body.name;
    if (!name) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Name is required" });
    }

    /* ---------- UPDATE SMARTPHONE (PARENT) ---------- */
    const updatePhoneSQL = `
      UPDATE smartphones  SET
        name=$1, category=$2, brand=$3, model=$4, launch_date=$5,
        images=$6, colors=$7, build_design=$8, display=$9, performance=$10,
        camera=$11, battery=$12, connectivity_network=$13, ports=$14,
        audio=$15, multimedia=$16, sensors=$17
      WHERE id=$18
      RETURNING *;
    `;

    const phoneRes = await client.query(updatePhoneSQL, [
      name,
      req.body.category || null,
      req.body.brand || null,
      req.body.model || null,
      parseDateForImport(req.body.launch_date),
      JSON.stringify(req.body.images || []),
      JSON.stringify(req.body.colors || []),
      JSON.stringify(req.body.build_design || {}),
      JSON.stringify(req.body.display || {}),
      JSON.stringify(req.body.performance || {}),
      JSON.stringify(req.body.camera || {}),
      JSON.stringify(req.body.battery || {}),
      JSON.stringify(req.body.connectivity_network || {}),
      JSON.stringify(req.body.ports || {}),
      JSON.stringify(req.body.audio || {}),
      JSON.stringify(req.body.multimedia || {}),
      req.body.sensors === null ? null : JSON.stringify(req.body.sensors || []),
      sid,
    ]);

    if (!phoneRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    /* ---------- UPSERT VARIANTS ---------- */
    if (Array.isArray(req.body.variants)) {
      // Ensure we have the product_id for this smartphone
      const productId = phoneRes.rows[0].product_id || null;
      if (!productId) {
        await client.query("ROLLBACK");
        return res
          .status(500)
          .json({ message: "Missing product_id for smartphone" });
      }

      const variantUpsertSQL = `
        INSERT INTO product_variants 
          (id, product_id, variant_key, attributes, base_price)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (id)
        DO UPDATE SET
          variant_key=EXCLUDED.variant_key,
          attributes=EXCLUDED.attributes,
          base_price=EXCLUDED.base_price
        RETURNING id;
      `;

      const insertVariantSQL = `
        INSERT INTO product_variants 
          (product_id, variant_key, attributes, base_price)
        VALUES ($1, $2, $3, $4)
        RETURNING id;
      `;

      // Map input variant index -> DB id (useful when client sends variant indices)
      const variantIdMap = [];

      for (let vi = 0; vi < req.body.variants.length; vi++) {
        const v = req.body.variants[vi];
        const ram = v.ram || null;
        const storageVal = v.storage || v.storage_size || null;
        const base_price = v.base_price ?? null;

        const variantKey =
          (v.variant_key && String(v.variant_key).trim()) ||
          `${String(ram || "na")}_${String(storageVal || "na")}`;
        const attrsObj = Object.assign({}, v.attributes || {});
        if (ram !== null) attrsObj.ram = ram;
        if (storageVal !== null) attrsObj.storage = storageVal;

        if (v.id) {
          const r = await client.query(variantUpsertSQL, [
            v.id,
            productId,
            variantKey,
            JSON.stringify(attrsObj),
            base_price,
          ]);
          variantIdMap[vi] = r.rows[0].id;
        } else {
          const r = await client.query(insertVariantSQL, [
            productId,
            variantKey,
            JSON.stringify(attrsObj),
            base_price,
          ]);
          variantIdMap[vi] = r.rows[0].id;
        }
      }

      // expose the mapping for later price handling
      req._variantIdMap = variantIdMap;
    }

    /* ---------- UPSERT STORE PRICES ---------- */
    if (Array.isArray(req.body.variant_store_prices)) {
      const priceUpsertSQL = `
        INSERT INTO variant_store_prices 
          (id, variant_id, store_name, price, url, offer_text)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (id)
        DO UPDATE SET
          store_name=EXCLUDED.store_name,
          price=EXCLUDED.price,
          url=EXCLUDED.url,
          offer_text=EXCLUDED.offer_text;
      `;

      const insertPriceSQL = `
        INSERT INTO variant_store_prices 
          (variant_id, store_name, price, url, offer_text)
        VALUES ($1,$2,$3,$4,$5)
        RETURNING id;
      `;

      const variantIdMap = req._variantIdMap || [];

      for (const sp of req.body.variant_store_prices) {
        // Resolve variant id: accept either a DB id or an input index (like 0)
        let resolvedVariantId = null;
        if (sp.variant_id !== undefined && sp.variant_id !== null) {
          const vnum = Number(sp.variant_id);
          if (!Number.isNaN(vnum)) {
            if (variantIdMap[vnum]) resolvedVariantId = variantIdMap[vnum];
            else resolvedVariantId = vnum; // treat as DB id
          }
        } else if (
          sp.variant_index !== undefined &&
          sp.variant_index !== null
        ) {
          const idx = Number(sp.variant_index);
          if (!Number.isNaN(idx) && variantIdMap[idx])
            resolvedVariantId = variantIdMap[idx];
        }

        if (!resolvedVariantId) continue; // cannot resolve target variant

        const store_name = sp.store_name || sp.store || null;
        const price = sp.price !== undefined ? Number(sp.price) : null;
        const url = sp.url || null;
        const offer_text = sp.offer_text || sp.offer || null;

        if (sp.id) {
          await client.query(priceUpsertSQL, [
            sp.id,
            resolvedVariantId,
            store_name,
            price,
            url,
            offer_text,
          ]);
        } else {
          await client.query(insertPriceSQL, [
            resolvedVariantId,
            store_name,
            price,
            url,
            offer_text,
          ]);
        }
      }
    }

    await client.query("COMMIT");
    return res.json({
      message: "Smartphone updated successfully",
      data: phoneRes.rows[0],
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/smartphone/:id error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete smartphone
app.delete("/api/smartphone/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  console.log(req.params.id);
  try {
    const sid = Number(req.params.id);
    if (!sid || Number.isNaN(sid)) {
      return res.status(400).json({ message: "Invalid id" });
    }

    await client.query("BEGIN");

    // resolve product_id from smartphone
    const sres = await client.query(
      "SELECT product_id FROM smartphones WHERE id = $1",
      [sid]
    );
    if (!sres.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const productId = sres.rows[0].product_id;

    // check publish status from product_publish table
    const pub = await client.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId]
    );

    if (pub.rows.length && pub.rows[0].is_published) {
      await client.query("ROLLBACK");
      return res
        .status(403)
        .json({ message: "Cannot delete: Smartphone is published" });
    }

    // delete any publish record for the product
    await client.query("DELETE FROM product_publish WHERE product_id = $1", [
      productId,
    ]);

    // delete the product (cascades to smartphones, product_variants, product_images via FK ON DELETE CASCADE)
    await client.query("DELETE FROM products WHERE id = $1", [productId]);

    await client.query("COMMIT");
    return res.json({
      message: "Unpublished smartphone and product deleted successfully",
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("DELETE /api/smartphone/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Delete a color from a smartphone's colors JSONB by index
app.get("/api/laptop", authenticate, async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        l.cpu,
        l.display,
        l.memory,
        l.storage,
        l.battery,
        l.connectivity,
        l.physical,
        l.software,
        l.features,
        l.warranty,
        l.created_at,

        COALESCE(pub.is_published, false) AS is_published

      FROM products p
      INNER JOIN laptop l
        ON l.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      WHERE p.product_type = 'laptop'

      ORDER BY p.id DESC
    `);

    res.json({ laptops: result.rows });
  } catch (err) {
    console.error("GET /api/laptop error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/homeappliance", authenticate, async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,

        b.name AS brand_name,

        h.appliance_type,
        h.model_number,
        h.release_year,
        h.country_of_origin,
        h.specifications,
        h.features,
        h.performance,
        h.physical_details,
        h.warranty,
        h.created_at,

        COALESCE(pub.is_published, false) AS is_published

      FROM products p
      INNER JOIN home_appliance h
        ON h.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      WHERE p.product_type = 'home_appliance'

      ORDER BY p.id DESC
    `);

    res.json({ home_appliances: result.rows });
  } catch (err) {
    console.error("GET /api/home-appliance error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Ram/Storage/Long API
------------------------*/

// Get all specs (public)
app.get("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const r = await db.query(
      "SELECT * FROM ram_storage_long  ORDER BY id DESC"
    );
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/specs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Create a spec entry (authenticated)
app.post("/api/ram-storage-config", authenticate, async (req, res) => {
  console.log(req.body);
  try {
    const { ram, storage } = req.body;
    // accept multiple possible keys from client: 'long', 'long_term_storage', or 'description'
    const long_term_storage =
      req.body.long || req.body.long_term_storage || req.body.description || "";

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const longVal = String(long_term_storage).trim();

    // Check if the same ram+storage combination already exists
    const exists = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 LIMIT 1`,
      [ramVal, storageVal]
    );
    if (exists.rows.length > 0) {
      return res.status(409).json({
        message: "This ram/storage combination already exists",
        id: exists.rows[0].id,
      });
    }

    const r = await db.query(
      `INSERT INTO ram_storage_long (ram, storage, long) VALUES ($1, $2, $3) RETURNING *`,
      [ramVal, storageVal, longVal]
    );

    return res.status(201).json({ message: "Spec created", data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/specs error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update a spec entry (authenticated)
app.put("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const { ram, storage } = req.body;
    const long_term_storage =
      req.body.long || req.body.long_term_storage || req.body.description || "";

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const longVal = String(long_term_storage).trim();

    // Check duplicate combination on other rows
    const dup = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 AND id <> $3 LIMIT 1`,
      [ramVal, storageVal, id]
    );
    if (dup.rows.length > 0) {
      return res.status(409).json({
        message: "Another entry with same ram/storage exists",
        id: dup.rows[0].id,
      });
    }

    const result = await db.query(
      `UPDATE ram_storage_long SET ram = $1, storage = $2, long = $3 WHERE id = $4 RETURNING *`,
      [ramVal, storageVal, longVal, id]
    );

    if (result.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });

    return res.json({ message: "Spec updated", data: result.rows[0] });
  } catch (err) {
    console.error("PUT /api/ram-storage-config/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a spec entry (authenticated) - path expected by client
app.delete("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const r = await db.query("DELETE FROM ram_storage_long WHERE id = $1", [
      id,
    ]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });
    return res.json({ message: "Spec deleted" });
  } catch (err) {
    console.error("DELETE /api/ram-storage-config/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Categories CRUD
------------------------*/
// Get all categories (public)
app.get("/api/categories", authenticate, async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM categories ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/categories error:", err);
    return res.status(500).json({ error: err.message });
  }
});
// Create category (authenticated)
app.post("/api/categories", authenticate, async (req, res) => {
  console.log(req.body);
  try {
    const { name, type, description } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const nameVal = String(name).trim();
    const typeVal = type ? String(type).trim() : null;
    const descVal = description ? String(description).trim() : null;

    const exists = await db.query(
      "SELECT id FROM categories WHERE name = $1 OR product_type = $2 LIMIT 1",
      [nameVal, typeVal]
    );
    if (exists.rows.length > 0)
      return res.status(409).json({ message: "Category already exists" });

    const r = await db.query(
      `INSERT INTO categories (name, product_type, description) VALUES ($1,$2,$3) RETURNING *`,
      [nameVal, typeVal, descVal]
    );
    return res.status(201).json({ data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/categories error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update category (authenticated)
app.put("/api/categories/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const { name, type, description } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const nameVal = String(name).trim();
    const typeVal = type ? String(type).trim() : null;
    const descVal = description ? String(description).trim() : null;

    const dup = await db.query(
      "SELECT id FROM categories WHERE (name = $1 OR product_type = $2) AND id <> $3 LIMIT 1",
      [nameVal, typeVal, id]
    );
    if (dup.rows.length > 0)
      return res
        .status(409)
        .json({ message: "Another category exists with same name/type" });

    const r = await db.query(
      `UPDATE categories SET name=$1, product_type=$2, description=$3 WHERE id=$4 RETURNING *`,
      [nameVal, typeVal, descVal, id]
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Category not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/categories/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete category (authenticated)
app.delete("/api/categories/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const r = await db.query("DELETE FROM categories WHERE id=$1", [id]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Category not found" });
    return res.json({ message: "Category deleted" });
  } catch (err) {
    console.error("DELETE /api/categories/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Online Stores CRUD
------------------------*/
// Get all online stores (public)
app.get("/api/online-stores", authenticate, async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM online_stores ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Create an online store (authenticated)
app.post("/api/online-stores", authenticate, async (req, res) => {
  try {
    const { name, logo, status } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });
    if (!logo) return res.status(400).json({ message: "logo is required" });

    const r = await db.query(
      `INSERT INTO online_stores (name, logo,status) VALUES ($1,$2,$3) RETURNING *`,
      [String(name).trim(), logo || null, status || "active"]
    );
    return res.status(201).json({ data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update an online store (authenticated)
app.put("/api/online-stores/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const { name, logo, website, description, status } = req.body || {};
    if (!name) return res.status(400).json({ message: "name is required" });

    const r = await db.query(
      `UPDATE online_stores SET name=$1, logo=$2,  status=$3 WHERE id=$4 RETURNING *`,
      [String(name).trim(), logo || null, status || "active", id]
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/online-stores/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete an online store (authenticated)
app.delete("/api/online-stores/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    const r = await db.query("DELETE FROM online_stores WHERE id = $1", [id]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ message: "Store deleted" });
  } catch (err) {
    console.error("DELETE /api/online-stores/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Patch status for an online store (authenticated)
app.patch("/api/online-stores/:id/status", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { status } = req.body || {};
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });
    if (!status) return res.status(400).json({ message: "status is required" });

    const r = await db.query(
      "UPDATE online_stores SET status = $1 WHERE id = $2 RETURNING *",
      [status, id]
    );
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Store not found" });
    return res.json({ data: r.rows[0] });
  } catch (err) {
    console.error("PATCH /api/online-stores/:id/status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a spec entry (authenticated)
app.delete("/api/specs/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id || Number.isNaN(id))
      return res.status(400).json({ message: "Invalid id" });

    const r = await db.query("DELETE FROM ram_storage_long  WHERE id = $1", [
      id,
    ]);
    if (r.rowCount === 0)
      return res.status(404).json({ message: "Spec not found" });
    return res.json({ message: "Spec deleted" });
  } catch (err) {
    console.error("DELETE /api/specs/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a variant by id (will cascade-delete store prices via FK)
app.delete("/api/variant/:id", authenticate, async (req, res) => {
  try {
    const vid = Number(req.params.id);
    if (!vid || Number.isNaN(vid))
      return res.status(400).json({ message: "Invalid variant id" });

    const result = await db.query(
      "DELETE FROM product_variants WHERE id = $1 RETURNING product_id;",
      [vid]
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Variant not found" });

    return res.json({
      message: "Variant deleted",
      product_id: result.rows[0].product_id,
    });
  } catch (err) {
    console.error("DELETE variant error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Delete a store price entry by id
app.delete("/api/storeprice/:id", authenticate, async (req, res) => {
  try {
    const pid = Number(req.params.id);
    if (!pid || Number.isNaN(pid))
      return res.status(400).json({ message: "Invalid price id" });

    const result = await db.query(
      "DELETE FROM variant_store_prices  WHERE id = $1 RETURNING variant_id;",
      [pid]
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Store price not found" });

    return res.json({
      message: "Store price deleted",
      variant_id: result.rows[0].variant_id,
    });
  } catch (err) {
    console.error("DELETE storeprice error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Variants & Store Price endpoints (single-item helpers) -  
------------------------*/

// Create a single variant store price
/* -----------------------
------------------------ */
app.get("/api/publish/status", async (req, res) => {
  try {
    const r = await db.query(
      "SELECT * FROM smartphone_publish  ORDER BY smartphone_id DESC"
    );
    return res.json({ publish: r.rows });
  } catch (err) {
    console.error("GET /api/publish/status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.patch("/api/products/:id/publish", authenticate, async (req, res) => {
  try {
    const productId = Number(req.params.id);
    if (!productId || Number.isNaN(productId)) {
      return res.status(400).json({ message: "Invalid product id" });
    }

    const { is_published } = req.body;
    if (typeof is_published !== "boolean") {
      return res.status(400).json({ message: "is_published must be boolean" });
    }

    if (req.user.role !== "admin") {
      return res.status(403).json({ message: "Admin access required" });
    }

    const product = await db.query("SELECT id FROM products WHERE id = $1", [
      productId,
    ]);

    if (product.rowCount === 0) {
      return res.status(404).json({ message: "Product not found" });
    }

    const result = await db.query(
      `
      INSERT INTO product_publish (product_id, is_published, published_by)
      VALUES ($1, $2, $3)
      ON CONFLICT (product_id)
      DO UPDATE SET
        is_published = EXCLUDED.is_published,
        published_by = EXCLUDED.published_by,
        updated_at = now()
      RETURNING *;
      `,
      [productId, is_published, req.user.id]
    );

    return res.json({
      message: "Publish status updated successfully",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("PATCH publish error:", err);
    return res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  CSV / XLSX Export & Import -  
------------------------*/
// Export (CSV) - authenticated
/* -----------------------
  Brands (categories)
------------------------*/
app.get("/api/brands", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        b.id,
        b.name,
        b.logo,
        COUNT(pp.product_id) AS published_products
      FROM brands b
      LEFT JOIN products p
        ON p.brand_id = b.id
      LEFT JOIN product_publish pp
        ON pp.product_id = p.id
       AND pp.is_published = true
      GROUP BY b.id
      ORDER BY b.name ASC
    `);

    res.json({ brands: result.rows });
  } catch (err) {
    console.error("GET /api/brands error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Misc helper endpoints -  
------------------------*/
app.post("/api/brands", authenticate, async (req, res) => {
  try {
    const { name, logo, category, status } = req.body;
    if (!name) {
      return res.status(400).json({ message: "Brand name required" });
    }

    const r = await db.query(
      `
      INSERT INTO brands (name, logo, category, status)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (name) DO UPDATE
      SET logo = EXCLUDED.logo
      RETURNING *;
      `,
      [name, logo || null, category || null, status || "active"]
    );

    res.json({ message: "Brand saved", data: r.rows[0] });
  } catch (err) {
    console.error("POST /api/brands error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/brands/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid brand id" });

    const { name, logo, category, status } = req.body;
    const updates = [];
    const values = [];
    let idx = 1;

    for (const [k, v] of Object.entries({ name, logo, category, status })) {
      if (v !== undefined) {
        updates.push(`${k} = $${idx++}`);
        values.push(v);
      }
    }

    if (!updates.length) {
      return res.status(400).json({ message: "No fields to update" });
    }

    values.push(id);

    const r = await db.query(
      `UPDATE brands SET ${updates.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );

    if (!r.rows.length) {
      return res.status(404).json({ message: "Brand not found" });
    }

    res.json({ message: "Brand updated", data: r.rows[0] });
  } catch (err) {
    console.error("PUT /api/brands/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/brands/:id", authenticate, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid brand id" });

    const r = await db.query("DELETE FROM brands WHERE id = $1", [id]);

    if (!r.rowCount) {
      return res.status(404).json({ message: "Brand not found" });
    }

    res.json({ message: "Brand deleted" });
  } catch (err) {
    console.error("DELETE /api/brands/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Reports
------------------------*/
// Products grouped by category (smartphone categories + totals by product_type)
app.get("/api/reports/products-by-category", authenticate, async (req, res) => {
  try {
    const catRes = await db.query(`
      SELECT COALESCE(s.category, 'Uncategorized') AS category, COUNT(*) AS count
      FROM smartphones s
      GROUP BY COALESCE(s.category, 'Uncategorized')
      ORDER BY count DESC
    `);

    const totalsRes = await db.query(`
      SELECT p.product_type, COUNT(*) AS count
      FROM products p
      GROUP BY p.product_type
      ORDER BY p.product_type
    `);

    return res.json({ categories: catRes.rows, totals: totalsRes.rows });
  } catch (err) {
    console.error("GET /api/reports/products-by-category error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Publish status grouped by product_type
app.get("/api/reports/publish-status", authenticate, async (req, res) => {
  try {
    const r = await db.query(`
      SELECT p.product_type,
             COUNT(*) AS total,
             COALESCE(SUM(CASE WHEN pp.is_published THEN 1 ELSE 0 END),0) AS published,
             COALESCE(SUM(CASE WHEN pp.is_published THEN 0 ELSE 1 END),0) AS drafts
      FROM products p
      LEFT JOIN product_publish pp ON pp.product_id = p.id
      GROUP BY p.product_type
      ORDER BY p.product_type
    `);

    return res.json({ publish_by_type: r.rows });
  } catch (err) {
    console.error("GET /api/reports/publish-status error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Number of published products by user
app.get("/api/reports/published-by-user", authenticate, async (req, res) => {
  try {
    const r = await db.query(`
      SELECT u.id, u.user_name, u.email, COUNT(*) AS published_count
      FROM product_publish pp
      JOIN "user" u ON u.id = pp.published_by
      WHERE pp.is_published = true
      GROUP BY u.id, u.user_name, u.email
      ORDER BY published_count DESC
      LIMIT 100
    `);

    return res.json({ published_by_user: r.rows });
  } catch (err) {
    console.error("GET /api/reports/published-by-user error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Recent publish activity (who published what)
app.get(
  "/api/reports/recent-publish-activity",
  authenticate,
  async (req, res) => {
    try {
      const r = await db.query(`
      SELECT pp.product_id, pp.is_published, pp.published_by, pp.updated_at,
             p.name AS product_name, p.product_type, u.user_name, u.email
      FROM product_publish pp
      LEFT JOIN products p ON p.id = pp.product_id
      LEFT JOIN "user" u ON u.id = pp.published_by
      ORDER BY pp.updated_at DESC
      LIMIT 100
    `);

      return res.json({ recent_publish_activity: r.rows });
    } catch (err) {
      console.error("GET /api/reports/recent-publish-activity error:", err);
      return res.status(500).json({ error: err.message });
    }
  }
);

// Get variants for a smartphone
app.get("/api/smartphone/:id/variants", async (req, res) => {
  try {
    const sid = Number(req.params.id);
    if (!sid) return res.status(400).json({ message: "Invalid id" });
    // Resolve product_id from smartphone id then fetch product_variants
    const sres = await db.query(
      "SELECT product_id FROM smartphones WHERE id = $1",
      [sid]
    );
    if (!sres.rows.length)
      return res.status(404).json({ message: "Smartphone not found" });
    const productId = sres.rows[0].product_id;
    const r = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId]
    );
    return res.json(r.rows);
  } catch (err) {
    console.error("GET variants error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Get store prices for variant
app.get("/api/variant/:id/store-prices", async (req, res) => {
  try {
    const vid = Number(req.params.id);
    if (!vid) return res.status(400).json({ message: "Invalid id" });
    const r = await db.query(
      "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
      [vid]
    );
    return res.json(r.rows);
  } catch (err) {
    console.error("GET variant store prices error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Simple global search endpoint with suggestions
// In your server routes (e.g., server.js or routes/search.js)
app.get("/api/search", async (req, res) => {
  try {
    const q = (req.query.q || "").trim();
    if (!q) return res.json({ results: [] });

    const term = `%${q}%`;

    // Search products by name, model, and brand with image
    const products = await db.query(
      `SELECT 
        p.id, 
        p.name, 
        p.product_type,
        b.name AS brand_name,
        pi.image_url
       FROM products p
       LEFT JOIN brands b ON b.id = p.brand_id
       LEFT JOIN product_images pi ON pi.product_id = p.id AND pi.is_primary = true
       WHERE p.name ILIKE $1 
          OR b.name ILIKE $1
          OR EXISTS (
            SELECT 1 FROM smartphones s 
            WHERE s.product_id = p.id AND s.model ILIKE $1
          )
          OR EXISTS (
            SELECT 1 FROM home_appliances ha 
            WHERE ha.product_id = p.id AND ha.model_number ILIKE $1
          )
       GROUP BY p.id, p.name, p.product_type, b.name, pi.image_url
       ORDER BY p.name ASC
       LIMIT 10`,
      [term]
    );

    // Search brands only
    const brands = await db.query(
      `SELECT id, name FROM brands
       WHERE name ILIKE $1
       ORDER BY name ASC
       LIMIT 6`,
      [term]
    );

    const results = [];

    // Add products to results
    for (const r of products.rows) {
      results.push({
        type: "product",
        id: r.id,
        name: r.name,
        product_type: r.product_type,
        brand_name: r.brand_name || null,
        image_url: r.image_url || null,
      });
    }

    // Add brands to results (avoid duplicates)
    for (const b of brands.rows) {
      const brandExists = results.some(
        (item) => item.type === "brand" && item.name === b.name
      );
      const productExists = results.some(
        (item) => item.type === "product" && item.brand_name === b.name
      );

      if (!brandExists && !productExists) {
        results.push({
          type: "brand",
          id: b.id,
          name: b.name,
        });
      }
    }

    res.json({ results });
  } catch (err) {
    console.error("GET /api/search error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* -----------------------
  Start server
------------------------*/
async function start() {
  try {
    // Wait for DB to be reachable before running migrations
    try {
      await db.waitForConnection(
        Number(process.env.DB_CONN_RETRIES) || 5,
        Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000
      );
    } catch (err) {
      console.error("DB not reachable after retries:", err);
      throw err;
    }

    await runMigrations();
  } catch (err) {
    console.error("Migrations failed:", err);
    process.exit(1);
  }

  app.listen(PORT, async () => {
    console.log(`🚀 Server running at http://localhost:${PORT}`);
    try {
      const r = await db.query("SELECT now()");
      console.log("DB time:", r.rows[0].now);
    } catch (err) {
      console.error("DB health check failed:", err);
    }
  });
}

start();

module.exports = app;
