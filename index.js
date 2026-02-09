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
const helmet = require("helmet");
const xss = require("xss-clean");
const { clean: xssClean } = require("xss-clean/lib/xss");

const SECRET = process.env.JWT_SECRET || "smartarena_secret_key_25";
const PORT = process.env.PORT || 5000;

const app = express();

app.set("trust proxy", 1);

app.use(
  cors({
    origin: [
      "http://localhost:3000",
      "http://localhost:5173",
      "https://main.d2jgd4xy0rohx4.amplifyapp.com",
      "https://main.d2ecrzwmegqlb.amplifyapp.com",
    ],
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

// Security middlewares
app.disable("x-powered-by");
app.use(helmet());
// Limit JSON body size to mitigate large payload abuse
app.use(express.json({ limit: "10kb" }));
// Parse URL-encoded bodies (for form submissions)
app.use(express.urlencoded({ extended: true }));
// Apply XSS sanitization after body and query parsers. Use the underlying
// `clean` function and avoid reassigning `req.query` if it's getter-only.
app.use(function xssSafe(req, res, next) {
  try {
    if (req.body) req.body = xssClean(req.body);
  } catch (err) {
    // ignore malformed body sanitization errors
  }

  if (req.query && typeof req.query === "object") {
    try {
      // try to replace whole query object
      req.query = xssClean(req.query);
    } catch (err) {
      // if replacing fails (getter-only), sanitize in-place
      try {
        const cleaned = xssClean(req.query);
        for (const k of Object.keys(cleaned)) {
          try {
            req.query[k] = cleaned[k];
          } catch (e) {
            /* skip */
          }
        }
      } catch (e) {
        // give up silently
      }
    }
  }

  try {
    if (req.params) req.params = xssClean(req.params);
  } catch (err) {
    // ignore
  }

  next();
});

// Note: rate-limiting removed — no express-rate-limit middleware applied.

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
            err.detail || err.message,
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
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Add description column if it doesn't exist (ALTER TABLE method)
    await safeQuery(`
      ALTER TABLE brands
      ADD COLUMN IF NOT EXISTS description TEXT;
    `);

    // categories
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        product_type TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure there is no unique constraint on product_type (allow multiple
    // categories per product type). Drop the constraint if it exists.
    await safeQuery(`
      ALTER TABLE categories
      DROP CONSTRAINT IF EXISTS categories_product_type_key;
    `);

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
        connectivity JSONB,
        network JSONB,
        ports JSONB,
        audio JSONB,
        multimedia JSONB,
        sensors JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure new `connectivity` and `network` columns exist for existing installations
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS connectivity JSONB;`,
    );
    await safeQuery(
      `ALTER TABLE smartphones ADD COLUMN IF NOT EXISTS network JSONB;`,
    );

    // If older `connectivity_network` column exists, copy its data into `connectivity` (preserve existing connectivity)
    await safeQuery(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name='smartphones' AND column_name='connectivity_network'
        ) THEN
          UPDATE smartphones SET connectivity = connectivity_network WHERE connectivity IS NULL;
          -- optionally preserve the original column; no drop performed automatically
        END IF;
      END$$;
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

    await safeQuery(
      `CREATE TABLE IF NOT EXISTS product_sphere_ratings (
        id SERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
      
        design JSONB,
        display JSONB,
        performance JSONB,
        camera JSONB,
        battery JSONB,
        connectivity JSONB,
        network JSONB,
      
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      
        UNIQUE(product_id)
      );
      `,
    );

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
        meta JSONB,
        created_at TIMESTAMP DEFAULT now()
      );
    `);

    // Ensure `meta` column exists for existing installations
    await safeQuery(`ALTER TABLE laptop ADD COLUMN IF NOT EXISTS meta JSONB;`);

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
    // Ensure legacy `long` column is renamed to `product_type` if present,
    // then create the table with `product_type` column.
    await safeQuery(`
      

      CREATE TABLE IF NOT EXISTS ram_storage_long (
        id SERIAL PRIMARY KEY,
        ram TEXT,
        storage TEXT,
        product_type TEXT,
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
        user_id INT NOT NULL REFERENCES Customers(id),
        overall_rating INT CHECK (overall_rating BETWEEN 1 AND 5),
        review TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (product_id, user_id)
      );
      `);
    // Ensure the foreign key for product_ratings.user_id references the customers table

    // trnding products
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_views (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        viewed_at TIMESTAMP DEFAULT now()
      );
      `);
    //compared products table (cascade deletes to avoid FK blocks)
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS product_comparisons (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        compared_with INT REFERENCES products(id) ON DELETE CASCADE,
        compared_at TIMESTAMP DEFAULT now()
      );
      `);
    await safeQuery(`
      CREATE TABLE IF NOT EXISTS wishlist (
        id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL
          REFERENCES customers(id)
          ON DELETE CASCADE,
      
        product_id INT NOT NULL
          REFERENCES products(id)
          ON DELETE CASCADE,
      
        created_at TIMESTAMP DEFAULT now(),
      
        UNIQUE (customer_id, product_id)
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
      [user_name, first_name, last_name, phone, gender, email, hashed, role],
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
      { expiresIn: "1h" },
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

/* ---- ADMIN Profile Endpoints ---- */
app.get("/api/auth/profile", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;

    const result = await db.query('SELECT * FROM "user" WHERE id = $1', [
      userId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const user = result.rows[0];

    res.json({
      success: true,
      message: "Admin profile retrieved successfully",
      user: {
        id: user.id,
        username: user.user_name,
        email: user.email,
        phone: user.phone || "",
        first_name: user.first_name || "",
        last_name: user.last_name || "",
        gender: user.gender || "",
        role: user.role || "",
      },
    });
  } catch (err) {
    console.error("Get admin profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- UPDATE Admin Profile ---- */
app.put("/api/auth/profile", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;
    const { email, phone, first_name, last_name, gender } = req.body;

    // Validate required fields
    if (!email) {
      return res.status(400).json({
        message: "Email is required",
      });
    }

    // Validate email format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      'SELECT id FROM "user" WHERE email = $1 AND id != $2',
      [email, userId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update admin profile
    const result = await db.query(
      `UPDATE "user" 
       SET email = $1, phone = $2, first_name = $3, last_name = $4, gender = $5
       WHERE id = $6
       RETURNING id, user_name, email, phone, first_name, last_name, gender, role`,
      [
        email,
        phone || null,
        first_name || null,
        last_name || null,
        gender || null,
        userId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const updatedUser = result.rows[0];

    res.json({
      success: true,
      message: "Profile updated successfully",
      user: {
        id: updatedUser.id,
        username: updatedUser.user_name,
        email: updatedUser.email,
        phone: updatedUser.phone || "",
        first_name: updatedUser.first_name || "",
        last_name: updatedUser.last_name || "",
        gender: updatedUser.gender || "",
        role: updatedUser.role || "",
      },
    });
  } catch (err) {
    console.error("Update admin profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- CHANGE Admin Password ---- */
app.post("/api/auth/change-password", authenticate, async (req, res) => {
  try {
    const userId = req.user.id;
    const { currentPassword, newPassword } = req.body;

    // Validate inputs
    if (!currentPassword || !newPassword) {
      return res.status(400).json({
        message: "Current password and new password are required",
      });
    }

    // Fetch user
    const userResult = await db.query(
      'SELECT password FROM "user" WHERE id = $1',
      [userId],
    );

    if (!userResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const user = userResult.rows[0];

    // Verify current password
    const passwordMatch = await bcrypt.compare(currentPassword, user.password);

    if (!passwordMatch) {
      return res.status(401).json({ message: "Current password is incorrect" });
    }

    // Check if new password is same as current
    const samePassword = await bcrypt.compare(newPassword, user.password);
    if (samePassword) {
      return res.status(400).json({
        message: "New password must be different from current password",
      });
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(newPassword, 10);

    // Update password
    await db.query('UPDATE "user" SET password = $1 WHERE id = $2', [
      hashedPassword,
      userId,
    ]);

    res.json({
      success: true,
      message: "Password changed successfully",
    });
  } catch (err) {
    console.error("Change admin password error:", err);
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
      [email, username],
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
       RETURNING id, f_name, l_name, username, email, city, country, state, zip_code`,
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
      ],
    );

    const customer = result.rows[0];

    // 5️⃣ Generate JWT token for automatic login
    const token = jwt.sign(
      {
        id: customer.id,
        email: customer.email,
        username: customer.username,
        type: "customer",
      },
      SECRET,
      { expiresIn: "7d" },
    );

    // 6️⃣ Success response with token and user data
    res.status(201).json({
      message: "Customer registered successfully. Automatically logged in.",
      token,
      user: customer,
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
      [q],
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
      [q],
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
        id: customer.id,
        email: customer.email,
        username: customer.username,
        type: "customer",
      },
      SECRET,
      { expiresIn: "7d" },
    );

    res.json({
      message: "Login successful",
      token,
      user: {
        id: customer.id,
        f_name: customer.f_name,
        l_name: customer.l_name,
        username: customer.username,
        email: customer.email,
        city: customer.city,
        country: customer.country,
        state: customer.state,
        zip_code: customer.zip_code,
      },
    });
  } catch (err) {
    console.error("Customer login error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- GET User Profile ---- */
app.get("/api/auth/user-profile", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;

    const result = await db.query("SELECT * FROM customers WHERE id = $1", [
      customerId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const customer = result.rows[0];

    res.json({
      message: "User profile retrieved successfully",
      user: {
        id: customer.id,
        f_name: customer.f_name,
        l_name: customer.l_name,
        username: customer.username,
        email: customer.email,
        phone: customer.phone || "",
        city: customer.city || "",
        state: customer.state || "",
        country: customer.country || "",
        zip_code: customer.zip_code || "",
      },
    });
  } catch (err) {
    console.error("Get profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- UPDATE User Profile ---- */
app.put("/api/auth/update-profile", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;
    const { f_name, l_name, email, phone, city, state, country, zip_code } =
      req.body;

    // Validate required fields
    if (!f_name || !l_name || !email) {
      return res.status(400).json({
        message: "First name, last name, and email are required",
      });
    }

    // Validate email format
    const emailRegex = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      "SELECT id FROM customers WHERE email = $1 AND id != $2",
      [email, customerId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update customer profile
    const result = await db.query(
      `UPDATE customers 
       SET f_name = $1, l_name = $2, email = $3, phone = $4, city = $5, state = $6, country = $7, zip_code = $8
       WHERE id = $9
       RETURNING id, f_name, l_name, username, email, phone, city, state, country, zip_code`,
      [
        f_name,
        l_name,
        email,
        phone || null,
        city || null,
        state || null,
        country || null,
        zip_code || null,
        customerId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const updatedCustomer = result.rows[0];

    res.json({
      message: "Profile updated successfully",
      user: {
        id: updatedCustomer.id,
        f_name: updatedCustomer.f_name,
        l_name: updatedCustomer.l_name,
        username: updatedCustomer.username,
        email: updatedCustomer.email,
        phone: updatedCustomer.phone || "",
        city: updatedCustomer.city || "",
        state: updatedCustomer.state || "",
        country: updatedCustomer.country || "",
        zip_code: updatedCustomer.zip_code || "",
      },
    });
  } catch (err) {
    console.error("Update profile error:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ---- CHANGE Password ---- */
app.post("/api/change-password", authenticateCustomer, async (req, res) => {
  try {
    const customerId = req.customer.id;
    const { currentPassword, newPassword } = req.body;

    // Validate inputs
    if (!currentPassword || !newPassword) {
      return res.status(400).json({
        message: "Current password and new password are required",
      });
    }

    // Validate new password strength
    const passwordRegex =
      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$/;
    if (!passwordRegex.test(newPassword)) {
      return res.status(400).json({
        message:
          "New password must be at least 8 characters with uppercase, lowercase, number, and special character",
      });
    }

    // Fetch customer
    const customerResult = await db.query(
      "SELECT password FROM customers WHERE id = $1",
      [customerId],
    );

    if (!customerResult.rows.length) {
      return res.status(404).json({ message: "User not found" });
    }

    const customer = customerResult.rows[0];

    // Verify current password
    const passwordMatch = await bcrypt.compare(
      currentPassword,
      customer.password,
    );

    if (!passwordMatch) {
      return res.status(401).json({ message: "Current password is incorrect" });
    }

    // Check if new password is same as current
    const samePassword = await bcrypt.compare(newPassword, customer.password);
    if (samePassword) {
      return res.status(400).json({
        message: "New password must be different from current password",
      });
    }

    // Hash new password
    const hashedPassword = await bcrypt.hash(newPassword, 10);

    // Update password
    await db.query("UPDATE customers SET password = $1 WHERE id = $2", [
      hashedPassword,
      customerId,
    ]);

    res.json({
      message: "Password changed successfully",
    });
  } catch (err) {
    console.error("Change password error:", err);
    res.status(500).json({ error: err.message });
  }
});

/*--- ADMIN Customer Management ---*/
app.get("/api/admin/customers", authenticate, async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, f_name, l_name, username, email, phone, city, state, country, zip_code, created_at 
       FROM Customers ORDER BY created_at DESC`,
    );

    res.json({
      success: true,
      message: "Customers retrieved successfully",
      customers: result.rows,
    });
  } catch (err) {
    console.error("Get customers error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;

    const result = await db.query(`SELECT * FROM Customers WHERE id = $1`, [
      customerId,
    ]);

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      customer: result.rows[0],
    });
  } catch (err) {
    console.error("Get customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.put("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;
    const { f_name, l_name, email, phone, city, state, country, zip_code } =
      req.body;

    // Validate required fields
    if (!f_name || !l_name || !email) {
      return res.status(400).json({
        message: "First name, last name, and email are required",
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ message: "Invalid email address" });
    }

    // Check if email already exists for another user
    const emailCheck = await db.query(
      "SELECT id FROM Customers WHERE email = $1 AND id != $2",
      [email, customerId],
    );

    if (emailCheck.rows.length > 0) {
      return res.status(400).json({ message: "Email already in use" });
    }

    // Update customer
    const result = await db.query(
      `UPDATE Customers 
       SET f_name = $1, l_name = $2, email = $3, phone = $4, city = $5, state = $6, country = $7, zip_code = $8
       WHERE id = $9
       RETURNING id, f_name, l_name, username, email, phone, city, state, country, zip_code`,
      [
        f_name,
        l_name,
        email,
        phone || null,
        city || null,
        state || null,
        country || null,
        zip_code || null,
        customerId,
      ],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      message: "Customer updated successfully",
      customer: result.rows[0],
    });
  } catch (err) {
    console.error("Update customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/admin/customers/:id", authenticate, async (req, res) => {
  try {
    const customerId = req.params.id;

    const result = await db.query(
      "DELETE FROM Customers WHERE id = $1 RETURNING id",
      [customerId],
    );

    if (!result.rows.length) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json({
      success: true,
      message: "Customer deleted successfully",
    });
  } catch (err) {
    console.error("Delete customer error:", err);
    res.status(500).json({ error: err.message });
  }
});

/*--- ratings smartphones  ---*/
app.post(
  "/api/public/products/:productId/ratings",
  authenticateCustomer,
  async (req, res) => {
    try {
      const productId = Number(req.params.productId);
      const userId = req.customer.id;
      const { overall, review } = req.body;

      if (!productId) {
        return res.status(400).json({ message: "Invalid product id" });
      }

      if (typeof overall !== "number" || overall < 1 || overall > 5) {
        return res
          .status(400)
          .json({ message: "Rating must be between 1 and 5" });
      }

      await db.query(
        `
        INSERT INTO product_ratings (product_id, user_id, overall_rating, review)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (product_id, user_id)
        DO UPDATE SET
          overall_rating = EXCLUDED.overall_rating,
          review = EXCLUDED.review,
          created_at = CURRENT_TIMESTAMP
        `,
        [productId, userId, Math.round(overall), review || null],
      );

      res.status(201).json({
        message: "Rating submitted successfully",
      });
    } catch (err) {
      console.error("POST rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  },
);

app.get("/api/brand", async (req, res) => {
  const result = await db.query(`SELECT * FROM brands`);
  res.json(result.rows);
});

app.get("/api/public/products/:productId/ratings", async (req, res) => {
  try {
    const productId = Number(req.params.productId);
    if (!productId) {
      return res.status(400).json({ message: "Invalid product id" });
    }

    // Optional customer authentication: if token provided and valid, identify customer id
    let requestingCustomerId = null;
    try {
      const authHeader = req.headers.authorization;
      const token = authHeader && authHeader.split(" ")[1];
      if (token) {
        const decoded = jwt.verify(token, SECRET);
        if (decoded && decoded.type === "customer") {
          requestingCustomerId = decoded.id;
        }
      }
    } catch (e) {
      // ignore token errors for public endpoint
      requestingCustomerId = null;
    }

    // Aggregate average and total
    const agg = await db.query(
      `SELECT ROUND(AVG(overall_rating)::numeric, 1) AS average_rating, COUNT(*) AS total_ratings
       FROM product_ratings WHERE product_id = $1`,
      [productId],
    );

    // Fetch individual reviews with reviewer info
    const reviewsRes = await db.query(
      `SELECT pr.id, pr.user_id, pr.overall_rating, pr.review, pr.created_at,
              c.f_name, c.l_name, c.username
       FROM product_ratings pr
       LEFT JOIN customers c ON pr.user_id = c.id
       WHERE pr.product_id = $1
       ORDER BY pr.created_at DESC`,
      [productId],
    );

    const reviews = reviewsRes.rows.map((r) => ({
      id: r.id,
      user_id: r.user_id,
      name: r.f_name || r.username || null,
      username: r.username || null,
      overall_rating: r.overall_rating,
      review: r.review,
      created_at: r.created_at,
      is_user_review: requestingCustomerId
        ? Number(r.user_id) === Number(requestingCustomerId)
        : false,
    }));

    res.json({
      productId,
      averageRating: agg.rows[0].average_rating || 0,
      totalRatings: Number(agg.rows[0].total_ratings || 0),
      reviews,
    });
  } catch (err) {
    console.error("GET ratings error:", err);
    res.status(500).json({ error: "Internal server error" });
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

      // Resolve product_id
      const sres = await db.query(
        "SELECT product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [smartphoneId],
      );
      n;
      if (!sres.rows.length) {
        return res.status(404).json({ message: "Smartphone not found" });
      }
      const productId = sres.rows[0].product_id;

      // For admin/private update, accept overall rating and review
      const overall =
        Math.round(
          ((display + performance + camera + battery + design) / 5) * 10,
        ) / 10;

      const result = await db.query(
        `
        UPDATE product_ratings
        SET overall_rating = $1, review = $2, created_at = CURRENT_TIMESTAMP
        WHERE id = (
          SELECT id FROM product_ratings WHERE product_id = $3 ORDER BY created_at DESC LIMIT 1
        )
        RETURNING *;
        `,
        [overall, req.body.review || null, productId],
      );

      if (result.rowCount === 0) {
        return res
          .status(404)
          .json({ message: "No rating found to update for this product" });
      }

      res.json({
        message: "Rating updated successfully",
        data: result.rows[0],
      });
    } catch (err) {
      console.error("PUT rating error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  },
);

app.delete(
  "/api/private/smartphone/:smartphoneId/rating",
  authenticate,
  async (req, res) => {
    try {
      const smartphoneId = Number(req.params.smartphoneId);
      if (!smartphoneId)
        return res.status(400).json({ message: "Invalid smartphone id" });

      const sres = await db.query(
        "SELECT product_id FROM smartphones WHERE id = $1 LIMIT 1",
        [smartphoneId],
      );
      if (!sres.rows.length)
        return res.status(404).json({ message: "Smartphone not found" });

      const productId = sres.rows[0].product_id;
      await db.query(`DELETE FROM product_ratings WHERE product_id = $1`, [
        productId,
      ]);

      res.json({ message: "All ratings deleted" });
    } catch (err) {
      console.error("DELETE ratings error:", err);
      res.status(500).json({ error: err.message });
    }
  },
);

// Delete a single review (only by the owning customer)
app.delete("/api/reviews/:id", authenticateCustomer, async (req, res) => {
  try {
    const reviewId = Number(req.params.id);
    if (!reviewId)
      return res.status(400).json({ message: "Invalid review id" });

    const result = await db.query(
      "SELECT user_id, product_id FROM product_ratings WHERE id = $1",
      [reviewId],
    );
    if (!result.rows.length)
      return res.status(404).json({ message: "Review not found" });

    const review = result.rows[0];
    if (Number(review.user_id) !== Number(req.customer.id)) {
      return res
        .status(403)
        .json({ message: "Not allowed to delete this review" });
    }

    await db.query("DELETE FROM product_ratings WHERE id = $1", [reviewId]);
    res.json({ message: "Review deleted successfully" });
  } catch (err) {
    console.error("DELETE review error:", err);
    res.status(500).json({ error: err.message });
  }
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
      [product.name, product.brand_id],
    );

    const productId = productRes.rows[0].id;

    /* ---------- 2. INSERT SMARTPHONE ---------- */
    const smartphoneRes = await client.query(
      `
      INSERT INTO smartphones (
        product_id, category, brand, model, launch_date,
        images, colors, build_design, display, performance,
        camera, battery, connectivity, network,
        ports, audio, multimedia, sensors
      )
      VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18
      )
      RETURNING id
      `,
      [
        productId,
        smartphone.category || smartphone.segment || null,
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
        JSON.stringify(smartphone.connectivity || {}),
        JSON.stringify(smartphone.network || {}),
        JSON.stringify(smartphone.ports || {}),
        JSON.stringify(smartphone.audio || {}),
        JSON.stringify(smartphone.multimedia || {}),
        // sensors as 16th param (if present)
        smartphone.sensors === null
          ? null
          : JSON.stringify(smartphone.sensors || []),
      ],
    );

    const smartphoneId = smartphoneRes.rows[0].id;

    // ---------- 2.b UPSERT SPHERE RATINGS (per-section JSON objects) ----------
    try {
      const upsertRes = await client.query(
        `INSERT INTO product_sphere_ratings
          (product_id, design, display, performance, camera, battery, connectivity, network)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (product_id) DO UPDATE SET
           design = EXCLUDED.design,
           display = EXCLUDED.display,
           performance = EXCLUDED.performance,
           camera = EXCLUDED.camera,
           battery = EXCLUDED.battery,
           connectivity = EXCLUDED.connectivity,
           network = EXCLUDED.network,
           updated_at = CURRENT_TIMESTAMP
        `,
        [
          productId,
          JSON.stringify(smartphone.build_design?.sphere_rating || null),
          JSON.stringify(smartphone.display?.sphere_rating || null),
          JSON.stringify(smartphone.performance?.sphere_rating || null),
          JSON.stringify(smartphone.camera?.sphere_rating || null),
          JSON.stringify(smartphone.battery?.sphere_rating || null),
          JSON.stringify(smartphone.connectivity?.sphere_rating || null),
          JSON.stringify(smartphone.network?.sphere_rating || null),
        ],
      );
    } catch (esr) {
      console.error(
        "Sphere ratings upsert error (create):",
        esr.message || esr,
      );
    }

    /* ---------- 3. INSERT PRODUCT IMAGES ---------- */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `
        INSERT INTO product_images (product_id, image_url, position)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        `,
        [productId, url, i + 1],
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
        [productId, variantKey, JSON.stringify(attrsObj), v.base_price || null],
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
          ],
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
      [name, product_type, brand_id],
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
        s.connectivity,
        s.network,
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
        s.camera, s.battery, s.connectivity, s.network,
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
        s.connectivity,
        s.network,
        s.ports,
        s.audio,
        s.multimedia,
        s.sensors,
        s.created_at,

        COALESCE(pub.is_published, false) AS is_published,

        /* ---------- Images ---------- */
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,

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

      INNER JOIN smartphones s
        ON s.product_id = p.id

      LEFT JOIN brands b
        ON b.id = p.brand_id

      LEFT JOIN product_publish pub
        ON pub.product_id = p.id

      LEFT JOIN product_variants v
        ON v.product_id = p.id

      WHERE p.product_type = 'smartphone'

      GROUP BY
        p.id, b.name,
        s.category, s.model, s.launch_date,
        s.colors, s.build_design, s.display, s.performance,
        s.camera, s.battery, s.connectivity, s.network,
        s.ports, s.audio, s.multimedia, s.sensors, s.created_at, pub.is_published

      ORDER BY p.id DESC;
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
      [sid],
    );
    if (!sres.rows.length) {
      // If the id wasn't numeric or no match by numeric id, also try matching by model or product id string
      if (isNaN(sid)) {
        const sres2 = await db.query(
          "SELECT * FROM smartphones WHERE model = $1 OR brand = $1 LIMIT 1",
          [rawId],
        );
        if (!sres2.rows.length)
          return res.status(404).json({ message: "Not found" });
        const smartphone = sres2.rows[0];
        const productId = smartphone.product_id;
        // Fetch product name from products table (ensure response includes product name)
        const prodRes2 = await db.query(
          "SELECT name FROM products WHERE id = $1 LIMIT 1",
          [productId],
        );
        const productName2 = prodRes2.rows[0] ? prodRes2.rows[0].name : null;
        const variantsRes = await db.query(
          "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
          [productId],
        );

        const variants = [];
        for (const v of variantsRes.rows) {
          const stores = await db.query(
            "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
            [v.id],
          );
          const ram = v.attributes ? v.attributes.ram || null : null;
          const storage = v.attributes ? v.attributes.storage || null : null;
          variants.push({ ...v, ram, storage, store_prices: stores.rows });
        }

        return res.json({
          data: { ...smartphone, name: productName2, variants },
        });
      }
      return res.status(404).json({ message: "Not found" });
    }

    const smartphone = sres.rows[0];
    const productId = smartphone.product_id;
    // Fetch product name from products table and include in response
    const prodRes = await db.query(
      "SELECT name, brand_id FROM products WHERE id = $1 LIMIT 1",
      [productId],
    );
    const productName = prodRes.rows[0] ? prodRes.rows[0].name : null;
    const productBrandId = prodRes.rows[0] ? prodRes.rows[0].brand_id : null;
    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variants = [];
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices  WHERE variant_id = $1 ORDER BY id ASC",
        [v.id],
      );
      const ram = v.attributes ? v.attributes.ram || null : null;
      const storage = v.attributes ? v.attributes.storage || null : null;
      variants.push({ ...v, ram, storage, store_prices: stores.rows });
    }

    // Sanitize response (remove internal ids)
    const sanitize = (smartphoneObj, variantsArr) => {
      const { id, product_id, ...rest } = smartphoneObj || {};
      // sanitize colors
      let colors = rest.colors;
      if (Array.isArray(colors)) {
        colors = colors.map((c) => {
          if (c && typeof c === "object") {
            const { id: _cid, ...rc } = c;
            return rc;
          }
          return c;
        });
      }

      const sanitizedVariants = (variantsArr || []).map((v) => {
        const { id: _vid, product_id: _vpid, store_prices, ...rv } = v || {};
        const sanitizedStorePrices = Array.isArray(store_prices)
          ? store_prices.map((sp) => {
              const { id: _spid, variant_id: _vref, ...rsp } = sp || {};
              return rsp;
            })
          : store_prices;
        return { ...rv, store_prices: sanitizedStorePrices };
      });

      return { ...rest, colors, variants: sanitizedVariants };
    };

    const sanitized = sanitize(smartphone, variants);
    sanitized.name = productName;

    return res.json({ data: sanitized });
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
      [product.name, product.brand_id],
    );
    const productId = productRes.rows[0].id;

    /* 2️⃣ Laptop table (JSONB SAFE) */
    await client.query(
      `
      INSERT INTO laptop (
        product_id, cpu, display, memory, storage, battery,
        connectivity, physical, software, features, warranty, meta
      )
      VALUES (
        $1,
        $2::jsonb,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,
        $7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,$12::jsonb
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
        JSON.stringify({
          category: laptop.category || null,
          brand: laptop.brand || null,
          model: laptop.model || null,
          launch_date: laptop.launch_date || null,
          colors: laptop.colors || [],
        }),
      ],
    );

    /* 3️⃣ Images */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position)
         VALUES ($1,$2,$3)`,
        [productId, url, i + 1],
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
        ],
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
          ],
        );
      }
    }

    /* 5️⃣ Publish default */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId],
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

// Get laptop by id (accepts internal laptop.id or product_id)
app.get("/api/laptops/:id", authenticate, async (req, res) => {
  try {
    const rawId = req.params.id;
    const lid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const lres = await db.query(
      "SELECT * FROM laptop WHERE product_id = $1 LIMIT 1",
      [lid],
    );

    if (!lres.rows.length) {
      return res.status(404).json({ message: "Not found" });
    }

    const laptop = lres.rows[0];
    const productId = laptop.product_id;

    // product name
    const prodRes = await db.query(
      "SELECT name, brand_id FROM products WHERE id = $1 LIMIT 1",
      [productId],
    );
    const productName = prodRes.rows[0] ? prodRes.rows[0].name : null;
    const productBrandId = prodRes.rows[0] ? prodRes.rows[0].brand_id : null;

    // images
    const imagesRes = await db.query(
      "SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC",
      [productId],
    );
    const images = imagesRes.rows.map((r) => r.image_url);

    // variants + store prices
    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variants = [];
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY id ASC",
        [v.id],
      );
      const ram = v.attributes ? v.attributes.ram || null : null;
      const storage = v.attributes ? v.attributes.storage || null : null;
      variants.push({ ...v, ram, storage, stores: stores.rows });
    }

    // publish info
    const pubRes = await db.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    const published = pubRes.rows[0] ? pubRes.rows[0].is_published : false;

    // Prepare sanitized response
    const sanitize = (lobj, variantsArr) => {
      const { id, product_id, meta, ...rest } = lobj || {};
      const metaObj = meta || {};
      return { ...rest, ...metaObj, variants: variantsArr || [] };
    };

    const sanitized = sanitize(laptop, variants);
    sanitized.name = productName;

    return res.json({
      product: { name: productName },
      laptop: sanitized,
      images,
      variants,
      published,
    });
  } catch (err) {
    console.error("GET /api/laptops/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update laptop (product, laptop jsonb, images, variants, publish)
app.put("/api/laptops/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    if (req.user.role !== "admin")
      return res.status(403).json({ message: "Admin access required" });

    const rawId = req.params.id;
    const lid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const lres = await db.query(
      "SELECT * FROM laptop WHERE product_id = $1 LIMIT 1",
      [lid],
    );
    if (!lres.rows.length)
      return res.status(404).json({ message: "Not found" });

    const laptopRow = lres.rows[0];
    const productId = laptopRow.product_id;

    const {
      product = {},
      laptop = {},
      images = [],
      variants = [],
      published,
    } = req.body;

    await client.query("BEGIN");

    // Update product
    if (product.name || product.brand_id !== undefined) {
      await client.query(
        "UPDATE products SET name = $1, brand_id = $2 WHERE id = $3",
        [product.name || null, product.brand_id || null, productId],
      );
    }

    // Update laptop JSONB fields and meta
    await client.query(
      `
      UPDATE laptop SET
        cpu = $1::jsonb,
        display = $2::jsonb,
        memory = $3::jsonb,
        storage = $4::jsonb,
        battery = $5::jsonb,
        connectivity = $6::jsonb,
        physical = $7::jsonb,
        software = $8::jsonb,
        features = $9::jsonb,
        warranty = $10::jsonb,
        meta = $11::jsonb
      WHERE product_id = $12
      `,
      [
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
        JSON.stringify({
          category: laptop.category || null,
          brand: laptop.brand || null,
          model: laptop.model || null,
          launch_date: laptop.launch_date || null,
          colors: laptop.colors || [],
        }),
        productId,
      ],
    );

    // Replace images: delete existing and insert new
    await client.query("DELETE FROM product_images WHERE product_id = $1", [
      productId,
    ]);
    for (let i = 0; i < (images || []).length; i++) {
      await client.query(
        "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
        [productId, images[i], i + 1],
      );
    }

    // Replace variants: delete existing variant store prices and variants, then insert new ones
    const oldVarRes = await client.query(
      "SELECT id FROM product_variants WHERE product_id = $1",
      [productId],
    );
    for (const v of oldVarRes.rows) {
      await client.query(
        "DELETE FROM variant_store_prices WHERE variant_id = $1",
        [v.id],
      );
    }
    await client.query("DELETE FROM product_variants WHERE product_id = $1", [
      productId,
    ]);

    for (const v of variants || []) {
      const variantKey = `${v.ram || ""}_${v.storage || ""}`;
      const variantRes = await client.query(
        `INSERT INTO product_variants (product_id, variant_key, attributes, base_price) VALUES ($1,$2,$3::jsonb,$4) RETURNING id`,
        [
          productId,
          variantKey,
          JSON.stringify({ ram: v.ram, storage: v.storage }),
          v.base_price || null,
        ],
      );
      const variantId = variantRes.rows[0].id;
      for (const s of v.stores || []) {
        await client.query(
          `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info) VALUES ($1,$2,$3,$4,$5,$6)`,
          [
            variantId,
            s.store_name || null,
            s.price || null,
            s.url || null,
            s.offer_text || null,
            s.delivery_info || null,
          ],
        );
      }
    }

    // Update publish state
    if (published !== undefined) {
      const up = await client.query(
        "UPDATE product_publish SET is_published = $1 WHERE product_id = $2",
        [published, productId],
      );
      if (up.rowCount === 0) {
        await client.query(
          "INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)",
          [productId, published],
        );
      }
    }

    await client.query("COMMIT");

    return res.json({ message: "Laptop updated", product_id: productId });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/laptops/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
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
      [product.name, product.brand_id],
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
      ],
    );

    /* ---------- 3️⃣ Images ---------- */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)`,
        [productId, url, i + 1],
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
        [productId, v.variant_key, JSON.stringify(v), v.base_price],
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [variantId, s.store_name, s.price, s.url, s.offer_text || null],
        );
      }
    }

    /* ---------- 5️⃣ Publish default ---------- */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId],
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
      [product.name, product.brand_id],
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
      ],
    );

    /* ---------- 3️⃣ Images ---------- */
    for (let i = 0; i < images.length; i++) {
      const url = images[i];
      await client.query(
        `INSERT INTO product_images (product_id, image_url, position)
         VALUES ($1,$2,$3)`,
        [productId, url, i + 1],
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
        [productId, v.variant_key, JSON.stringify(v), v.base_price],
      );

      const variantId = variantRes.rows[0].id;

      for (const s of v.stores || []) {
        await client.query(
          `
          INSERT INTO variant_store_prices
            (variant_id, store_name, price, url, offer_text)
          VALUES ($1,$2,$3,$4,$5)
          `,
          [variantId, s.store_name, s.price, s.url, s.offer_text || null],
        );
      }
    }

    /* ---------- 5️⃣ Publish default false ---------- */
    await client.query(
      `INSERT INTO product_publish (product_id, is_published)
       VALUES ($1,false)`,
      [productId],
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
  // Accept payloads that wrap a single smartphone inside { smartphones: [ { ... } ] }
  if (
    req.body &&
    Array.isArray(req.body.smartphones) &&
    req.body.smartphones.length > 0
  ) {
    const first = req.body.smartphones[0];

    // deep-merge `first` into `req.body` but do not overwrite existing scalar values
    const isPlainObject = (v) =>
      v && typeof v === "object" && !Array.isArray(v);

    const mergeInto = (target, source) => {
      for (const key of Object.keys(source)) {
        const srcVal = source[key];
        const tgtVal = target[key];

        if (tgtVal === undefined) {
          // if target missing, copy entire value
          target[key] = srcVal;
          continue;
        }

        // if both are plain objects, recurse to preserve nested fields
        if (isPlainObject(tgtVal) && isPlainObject(srcVal)) {
          mergeInto(tgtVal, srcVal);
          continue;
        }

        // if target is array and source is array, prefer source only if target is empty
        if (Array.isArray(tgtVal) && Array.isArray(srcVal)) {
          if (tgtVal.length === 0 && srcVal.length > 0) target[key] = srcVal;
          continue;
        }

        // otherwise leave existing target value intact
      }
    };

    mergeInto(req.body, first);
  }
  try {
    await client.query("BEGIN");

    // Resolve the smartphone record by either internal id or product_id
    const rawId = req.params.id;
    const parsedId = Number(rawId);
    if (Number.isNaN(parsedId) || parsedId <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Invalid id" });
    }

    const findRes = await client.query(
      "SELECT id, product_id FROM smartphones WHERE id = $1 OR product_id = $1 LIMIT 1",
      [parsedId],
    );
    if (!findRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const sid = findRes.rows[0].id; // internal smartphone id

    const n = normalizeBodyKeys(req.body || {});
    // Accept several name aliases: `name`, `product_name`, `productName`, or normalized variants
    const name =
      n.name ||
      n.productname ||
      req.body.name ||
      req.body.product_name ||
      req.body.productName ||
      req.body.productName?.toString();
    if (!name) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Name is required" });
    }

    /* ---------- UPDATE SMARTPHONE (PARENT) ---------- */
    const updatePhoneSQL = `
      UPDATE smartphones SET
        category=$1, brand=$2, model=$3, launch_date=$4,
        images=$5, colors=$6, build_design=$7, display=$8, performance=$9,
        camera=$10, battery=$11, connectivity=$12, network=$13, ports=$14,
        audio=$15, multimedia=$16, sensors=$17
      WHERE id=$18
      RETURNING *;
    `;

    const phoneRes = await client.query(updatePhoneSQL, [
      req.body.category || req.body.segment || null,
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
      JSON.stringify(req.body.connectivity || {}),
      JSON.stringify(req.body.network || {}),
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

    // Update product name in products table (product name stored on products)
    const productId = phoneRes.rows[0].product_id || null;
    if (productId && name) {
      await client.query(`UPDATE products SET name = $1 WHERE id = $2`, [
        name,
        productId,
      ]);
    }

    // Replace product_images to reflect new images array (if provided)
    try {
      if (productId) {
        await client.query("DELETE FROM product_images WHERE product_id = $1", [
          productId,
        ]);
        const imgs = Array.isArray(req.body.images) ? req.body.images : [];
        for (let i = 0; i < imgs.length; i++) {
          await client.query(
            "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
            [productId, imgs[i], i + 1],
          );
        }
      }
    } catch (piErr) {
      console.error(
        "Failed to replace product_images:",
        piErr.message || piErr,
      );
      // non-fatal: continue without aborting the whole update
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
        ON CONFLICT (product_id, variant_key)
        DO UPDATE SET
          attributes = EXCLUDED.attributes,
          base_price = EXCLUDED.base_price
        RETURNING id;
      `;

      // Map input variant index -> DB id (useful when client sends variant indices)
      const variantIdMap = [];
      const keepVariantIds = [];

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
          keepVariantIds.push(r.rows[0].id);
        } else {
          const r = await client.query(insertVariantSQL, [
            productId,
            variantKey,
            JSON.stringify(attrsObj),
            base_price,
          ]);
          variantIdMap[vi] = r.rows[0].id;
          keepVariantIds.push(r.rows[0].id);
        }
      }

      // expose the mapping for later price handling
      req._variantIdMap = variantIdMap;

      // Replace semantics: if `variants` array is provided, remove variants not present anymore.
      // This enables "delete variant" from the admin UI by simply omitting that variant from the payload.
      const keepIds = keepVariantIds
        .map((x) => Number(x))
        .filter((n) => Number.isInteger(n) && n > 0);
      if (keepIds.length === 0) {
        await client.query(
          "DELETE FROM product_variants WHERE product_id = $1",
          [productId],
        );
      } else {
        await client.query(
          "DELETE FROM product_variants WHERE product_id = $1 AND NOT (id = ANY($2::int[]))",
          [productId, keepIds],
        );
      }
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
        ON CONFLICT (variant_id, store_name)
        DO UPDATE SET
          price = EXCLUDED.price,
          url = EXCLUDED.url,
          offer_text = EXCLUDED.offer_text
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

    // If client provided `published` flag, reflect it in product_publish table
    if (req.body.published !== undefined) {
      try {
        const productId = phoneRes.rows[0].product_id || null;
        if (productId) {
          await client.query(
            `
            INSERT INTO product_publish (product_id, is_published, published_by)
            VALUES ($1, $2, $3)
            ON CONFLICT (product_id)
            DO UPDATE SET
              is_published = EXCLUDED.is_published,
              published_by = EXCLUDED.published_by,
              updated_at = now();
            `,
            [
              productId,
              req.body.published,
              req.user && req.user.id ? req.user.id : null,
            ],
          );
        }
      } catch (pubErr) {
        console.error(
          "Failed to update product_publish for smartphone:",
          pubErr,
        );
        // don't fail the whole update for publish tracking issues
      }
    }

    // ---------- UPDATE SPHERE RATINGS (if provided in request body) ----------
    try {
      const productId = phoneRes.rows[0].product_id || null;
      if (productId) {
        await client.query(
          `INSERT INTO product_sphere_ratings
            (product_id, design, display, performance, camera, battery, connectivity, network)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (product_id) DO UPDATE SET
             design = COALESCE(EXCLUDED.design, product_sphere_ratings.design),
             display = COALESCE(EXCLUDED.display, product_sphere_ratings.display),
             performance = COALESCE(EXCLUDED.performance, product_sphere_ratings.performance),
             camera = COALESCE(EXCLUDED.camera, product_sphere_ratings.camera),
             battery = COALESCE(EXCLUDED.battery, product_sphere_ratings.battery),
             connectivity = COALESCE(EXCLUDED.connectivity, product_sphere_ratings.connectivity),
             network = COALESCE(EXCLUDED.network, product_sphere_ratings.network),
             updated_at = CURRENT_TIMESTAMP
          `,
          [
            productId,
            JSON.stringify(req.body.build_design?.sphere_rating || null),
            JSON.stringify(req.body.display?.sphere_rating || null),
            JSON.stringify(req.body.performance?.sphere_rating || null),
            JSON.stringify(req.body.camera?.sphere_rating || null),
            JSON.stringify(req.body.battery?.sphere_rating || null),
            JSON.stringify(req.body.connectivity?.sphere_rating || null),
            JSON.stringify(req.body.network?.sphere_rating || null),
          ],
        );
      }
    } catch (srup) {
      console.error(
        "Sphere ratings upsert error (update):",
        srup.message || srup,
      );
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
    if (Number.isNaN(sid) || sid <= 0) {
      return res.status(400).json({ message: "Invalid id" });
    }

    await client.query("BEGIN");

    // resolve product_id from smartphone
    // Accept either internal smartphones.id or the linked products.id (product_id)
    const sres = await client.query(
      "SELECT product_id FROM smartphones WHERE id = $1 OR product_id = $1 LIMIT 1",
      [sid],
    );
    if (!sres.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Smartphone not found" });
    }

    const productId = sres.rows[0].product_id;

    // check publish status from product_publish table
    const pub = await client.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
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

    // delete any product comparisons referencing this product (either side)
    await client.query(
      "DELETE FROM product_comparisons WHERE product_id = $1 OR compared_with = $1",
      [productId],
    );

    // delete any sphere ratings associated with this product
    await client.query(
      "DELETE FROM product_sphere_ratings WHERE product_id = $1",
      [productId],
    );

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

// Get single home appliance by id (accepts product_id)
app.get("/api/home-appliances/:id", authenticate, async (req, res) => {
  try {
    const rawId = req.params.id;
    const pid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const har = await db.query(
      "SELECT * FROM home_appliance WHERE product_id = $1 LIMIT 1",
      [pid],
    );
    if (!har.rows.length) return res.status(404).json({ message: "Not found" });

    const home = har.rows[0];
    const productId = home.product_id;

    const prodRes = await db.query(
      "SELECT name, brand_id FROM products WHERE id = $1 LIMIT 1",
      [productId],
    );
    const productName = prodRes.rows[0] ? prodRes.rows[0].name : null;
    const productBrandId = prodRes.rows[0] ? prodRes.rows[0].brand_id : null;

    const imagesRes = await db.query(
      "SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC",
      [productId],
    );
    const images = imagesRes.rows.map((r) => r.image_url);

    const variantsRes = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
    );

    const variants = [];
    for (const v of variantsRes.rows) {
      const stores = await db.query(
        "SELECT * FROM variant_store_prices WHERE variant_id = $1 ORDER BY id ASC",
        [v.id],
      );
      variants.push({ ...v, stores: stores.rows });
    }

    const pubRes = await db.query(
      "SELECT is_published FROM product_publish WHERE product_id = $1 LIMIT 1",
      [productId],
    );
    const published = pubRes.rows[0] ? pubRes.rows[0].is_published : false;

    return res.json({
      product: { name: productName, brand_id: productBrandId },
      home_appliance: home,
      images,
      variants,
      published,
    });
  } catch (err) {
    console.error("GET /api/home-appliances/:id error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Update home appliance (product, home_appliance jsonb, images, variants, publish)
app.put("/api/home-appliances/:id", authenticate, async (req, res) => {
  const client = await db.connect();
  try {
    if (req.user.role !== "admin")
      return res.status(403).json({ message: "Admin access required" });

    const rawId = req.params.id;
    const pid = Number(rawId);
    if (!rawId || rawId.trim() === "")
      return res.status(400).json({ message: "Invalid id" });

    const har = await db.query(
      "SELECT * FROM home_appliance WHERE product_id = $1 LIMIT 1",
      [pid],
    );
    if (!har.rows.length) return res.status(404).json({ message: "Not found" });

    const productId = har.rows[0].product_id;

    const {
      product = {},
      home_appliance = {},
      images = [],
      variants = [],
      published,
    } = req.body;

    await client.query("BEGIN");

    // Update product
    if (product.name || product.brand_id !== undefined) {
      await client.query(
        "UPDATE products SET name = $1, brand_id = $2 WHERE id = $3",
        [product.name || null, product.brand_id || null, productId],
      );
    }

    const toJSON = (v) => (v === undefined ? null : JSON.stringify(v));

    // Update home_appliance JSONB fields
    await client.query(
      `
      UPDATE home_appliance SET
        appliance_type = $1,
        model_number = $2,
        release_year = $3,
        country_of_origin = $4,
        specifications = $5::jsonb,
        features = $6::jsonb,
        performance = $7::jsonb,
        physical_details = $8::jsonb,
        warranty = $9::jsonb
      WHERE product_id = $10
      `,
      [
        home_appliance.appliance_type || null,
        home_appliance.model_number || null,
        home_appliance.release_year || null,
        home_appliance.country_of_origin || null,
        toJSON(home_appliance.specifications),
        toJSON(home_appliance.features),
        toJSON(home_appliance.performance),
        toJSON(home_appliance.physical_details),
        toJSON(home_appliance.warranty),
        productId,
      ],
    );

    // Replace images
    await client.query("DELETE FROM product_images WHERE product_id = $1", [
      productId,
    ]);
    for (let i = 0; i < (images || []).length; i++) {
      await client.query(
        "INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3)",
        [productId, images[i], i + 1],
      );
    }

    // Replace variants
    const oldVarRes = await client.query(
      "SELECT id FROM product_variants WHERE product_id = $1",
      [productId],
    );
    for (const v of oldVarRes.rows) {
      await client.query(
        "DELETE FROM variant_store_prices WHERE variant_id = $1",
        [v.id],
      );
    }
    await client.query("DELETE FROM product_variants WHERE product_id = $1", [
      productId,
    ]);

    for (const v of variants || []) {
      const variantKey = v.variant_key || `${v.ram || ""}_${v.storage || ""}`;
      const variantRes = await client.query(
        `INSERT INTO product_variants (product_id, variant_key, attributes, base_price) VALUES ($1,$2,$3::jsonb,$4) RETURNING id`,
        [
          productId,
          variantKey,
          JSON.stringify(v.attributes || { ram: v.ram, storage: v.storage }),
          v.base_price || null,
        ],
      );
      const variantId = variantRes.rows[0].id;
      for (const s of v.stores || []) {
        await client.query(
          `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info) VALUES ($1,$2,$3,$4,$5,$6)`,
          [
            variantId,
            s.store_name || null,
            s.price || null,
            s.url || null,
            s.offer_text || null,
            s.delivery_info || null,
          ],
        );
      }
    }

    // Update publish state
    if (published !== undefined) {
      const up = await client.query(
        "UPDATE product_publish SET is_published = $1 WHERE product_id = $2",
        [published, productId],
      );
      if (up.rowCount === 0) {
        await client.query(
          "INSERT INTO product_publish (product_id, is_published) VALUES ($1,$2)",
          [productId, published],
        );
      }
    }

    await client.query("COMMIT");

    return res.json({
      message: "Home appliance updated",
      product_id: productId,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("PUT /api/home-appliances/:id error:", err);
    return res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

/* -----------------------
  Ram/Storage/Long API
------------------------*/

// Get all specs (public)
app.get("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const r = await db.query(
      "SELECT * FROM ram_storage_long  ORDER BY id DESC",
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
    // accept multiple possible keys from client: 'product_type', 'long', or 'description'
    const product_type =
      req.body.product_type ||
      req.body.long ||
      req.body.long_term_storage ||
      req.body.description ||
      null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const productTypeVal = product_type ? String(product_type).trim() : null;

    // Check if the same ram+storage combination already exists
    const exists = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 LIMIT 1`,
      [ramVal, storageVal],
    );
    if (exists.rows.length > 0) {
      return res.status(409).json({
        message: "This ram/storage combination already exists",
        id: exists.rows[0].id,
      });
    }

    const r = await db.query(
      `INSERT INTO ram_storage_long (ram, storage, product_type) VALUES ($1, $2, $3) RETURNING *`,
      [ramVal, storageVal, productTypeVal],
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
    const product_type =
      req.body.product_type ||
      req.body.long ||
      req.body.long_term_storage ||
      req.body.description ||
      null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const ramVal = String(ram).trim();
    const storageVal = String(storage).trim();
    const productTypeVal = product_type ? String(product_type).trim() : null;

    // Check duplicate combination on other rows
    const dup = await db.query(
      `SELECT id FROM ram_storage_long WHERE ram = $1 AND storage = $2 AND id <> $3 LIMIT 1`,
      [ramVal, storageVal, id],
    );
    if (dup.rows.length > 0) {
      return res.status(409).json({
        message: "Another entry with same ram/storage exists",
        id: dup.rows[0].id,
      });
    }

    const result = await db.query(
      `UPDATE ram_storage_long SET ram = $1, storage = $2, product_type = $3 WHERE id = $4 RETURNING *`,
      [ramVal, storageVal, productTypeVal, id],
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

// Public categories endpoint (no authentication) - useful for public site
app.get("/api/category", async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM categories ORDER BY id DESC");
    return res.json({ data: r.rows });
  } catch (err) {
    console.error("GET /api/category error:", err);
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

    // Only treat duplicate by name (case-insensitive). Multiple categories
    // can share the same product_type.
    const exists = await db.query(
      "SELECT id FROM categories WHERE LOWER(name) = LOWER($1) LIMIT 1",
      [nameVal],
    );
    if (exists.rows.length > 0)
      return res.status(409).json({ message: "Category already exists" });

    const r = await db.query(
      `INSERT INTO categories (name, product_type, description) VALUES ($1,$2,$3) RETURNING *`,
      [nameVal, typeVal, descVal],
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

    // Only check for another category with the same name (case-insensitive)
    const dup = await db.query(
      "SELECT id FROM categories WHERE LOWER(name) = LOWER($1) AND id <> $2 LIMIT 1",
      [nameVal, id],
    );
    if (dup.rows.length > 0)
      return res
        .status(409)
        .json({ message: "Another category exists with same name/type" });

    const r = await db.query(
      `UPDATE categories SET name=$1, product_type=$2, description=$3 WHERE id=$4 RETURNING *`,
      [nameVal, typeVal, descVal, id],
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
app.get("/api/public/online-stores", async (req, res) => {
  try {
    const r = await db.query("SELECT * FROM online_stores ORDER BY id DESC");
    const sanitized = (r.rows || []).map((row) => {
      const { created_at, createdAt, ...rest } = row || {};
      return rest;
    });
    return res.json({ data: sanitized });
  } catch (err) {
    console.error("GET /api/public/online-stores error:", err);
    return res.status(500).json({ error: err.message });
  }
});

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
      [String(name).trim(), logo || null, status || "active"],
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
      [String(name).trim(), logo || null, status || "active", id],
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
      [status, id],
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
      [vid],
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
      [pid],
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
      "SELECT * FROM smartphone_publish  ORDER BY smartphone_id DESC",
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
      [productId, is_published, req.user.id],
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
// Mount import routers
// Export (CSV) - authenticated
/* -----------------------
  Brands (categories)
------------------------*/
app.get("/api/brands", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        id,
        name,
        logo,
        description
      FROM brands
      ORDER BY name ASC
    `);

    res.json({ brands: result.rows });
  } catch (err) {
    console.error("GET /api/brands error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/brand", authenticate, async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        b.id,
        b.name,
        b.logo,
        b.description,
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
    const { name, logo, category, status, description } = req.body;
    if (!name) {
      return res.status(400).json({ message: "Brand name required" });
    }

    const r = await db.query(
      `
      INSERT INTO brands (name, logo, category, status, description)
      VALUES ($1,$2,$3,$4,$5)
      ON CONFLICT (name) DO UPDATE
      SET logo = EXCLUDED.logo,
          description = EXCLUDED.description
      RETURNING *;
      `,
      [
        name,
        logo || null,
        category || null,
        status || "active",
        description || null,
      ],
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

    const { name, logo, category, status, description } = req.body;
    const updates = [];
    const values = [];
    let idx = 1;

    for (const [k, v] of Object.entries({
      name,
      logo,
      category,
      status,
      description,
    })) {
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
      values,
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
  },
);

// Record a product view (public)

app.post("/api/public/product/:id/view", async (req, res) => {
  const rawId = req.params.id;
  const productId = Number(rawId);

  if (!rawId || !Number.isInteger(productId) || productId <= 0) {
    console.warn("Invalid product id in view request:", rawId);
    return res.status(400).json({ message: "Invalid product id" });
  }

  console.log("Recording view for product id:", productId);
  try {
    await db.query(`INSERT INTO product_views (product_id) VALUES ($1)`, [
      productId,
    ]);
    return res.json({ message: "View recorded" });
  } catch (err) {
    console.error("Error recording product view:", err);
    return res.status(500).json({ message: "Failed to record view" });
  }
});

app.get("/api/public/trending-products", async (req, res) => {
  const result = await db.query(`
    SELECT 
      p.id,
      p.name,
      COUNT(v.id) AS views
    FROM product_views v
    JOIN products p ON p.id = v.product_id
    WHERE v.viewed_at >= now() - INTERVAL '7 days'
    GROUP BY p.id
    ORDER BY views DESC
    LIMIT 10;
  `);

  res.json({ trending: result.rows });
});

// Trending Smartphones (variant-based: storage)
app.get("/api/public/trending/smartphones", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        v.id AS variant_id,
        p.name,
        b.name AS brand,
        s.model,

        v.attributes->>'ram' AS ram,
        v.attributes->>'storage' AS storage,
        v.base_price,

        -- ✅ product image
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC
          LIMIT 1
        ) AS image_url,

        COUNT(pv.id) AS views

      FROM product_views pv
      INNER JOIN products p
        ON p.id = pv.product_id
      INNER JOIN smartphones s
        ON s.product_id = p.id
      INNER JOIN product_variants v
        ON v.product_id = p.id
      LEFT JOIN brands b
        ON b.id = p.brand_id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true

      WHERE p.product_type = 'smartphone'

      GROUP BY
        p.id,
        v.id,
        b.name,
        s.model,
        v.attributes,
        v.base_price

      ORDER BY views DESC
      LIMIT 20;
    `);

    res.json({
      success: true,
      trending: result.rows,
    });
  } catch (err) {
    console.error("Trending smartphones (variant) error:", err);
    res.status(500).json({
      success: false,
      message: "Failed to fetch trending smartphones",
    });
  }
});

// Trending Laptops
app.get("/api/public/trending/laptops", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
          AND p.product_type = 'laptop'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 12
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        p.name AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        (
          SELECT COALESCE(v.attributes->>'storage', v.attributes->>'storage_size', '')
          FROM product_variants v
          WHERE v.product_id = p.id
          ORDER BY v.id ASC
          LIMIT 1
        ) AS storage,
        COALESCE(pi.image_url, NULL) AS image,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN laptop l ON l.product_id = p.id
      LEFT JOIN product_images pi ON pi.product_id = p.id AND pi.position = 1
      ORDER BY tp.views DESC, price ASC NULLS LAST
      LIMIT 50;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/laptops error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending Home Appliances
app.get("/api/public/trending/appliances", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
          AND p.product_type = 'home_appliance'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 12
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        ha.model_number AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        COALESCE(pi.image_url, NULL) AS image,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN home_appliance ha ON ha.product_id = p.id
      LEFT JOIN product_images pi ON pi.product_id = p.id AND pi.position = 1
      ORDER BY tp.views DESC, price ASC NULLS LAST
      LIMIT 50;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/appliances error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending Networking
app.get("/api/public/trending/networking", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
          AND p.product_type = 'networking'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 12
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        n.model_number AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        COALESCE(pi.image_url, NULL) AS image,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN networking n ON n.product_id = p.id
      LEFT JOIN product_images pi ON pi.product_id = p.id AND pi.position = 1
      ORDER BY tp.views DESC, price ASC NULLS LAST
      LIMIT 50;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/networking error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Smartphones
app.get("/api/public/new/smartphones", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        s.model AS model,
        s.launch_date,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN smartphones s ON s.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'smartphone'
      ORDER BY COALESCE(s.launch_date, p.created_at) DESC
      LIMIT 20;
    `);

    return res.json({ new: result.rows });
  } catch (err) {
    console.error("GET /api/public/new/smartphones error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Laptops
app.get("/api/public/new/laptops", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        l.created_at AS launch_date,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN laptop l ON l.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'laptop'
      ORDER BY COALESCE(l.created_at, p.created_at) DESC
      LIMIT 20;
    `);

    return res.json({ new: result.rows });
  } catch (err) {
    console.error("GET /api/public/new/laptops error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Home Appliances
app.get("/api/public/new/appliances", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        ha.release_year AS launch_date,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN home_appliance ha ON ha.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'home_appliance'
      ORDER BY COALESCE(ha.release_year::text, p.created_at::text) DESC
      LIMIT 20;
    `);

    return res.json({ new: result.rows });
  } catch (err) {
    console.error("GET /api/public/new/appliances error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// New Launches - Networking
app.get("/api/public/new/networking", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        b.name AS brand,
        n.created_at AS launch_date,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN networking n ON n.product_id = p.id
      INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
      WHERE p.product_type = 'networking'
      ORDER BY COALESCE(n.created_at, p.created_at) DESC
      LIMIT 20;
    `);

    return res.json({ new: result.rows });
  } catch (err) {
    console.error("GET /api/public/new/networking error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Trending All Types (smartphones, laptops, networking, appliances, etc.)
app.get("/api/public/trending/all", async (req, res) => {
  try {
    const result = await db.query(`
      WITH top_products AS (
        SELECT p.id AS product_id, COUNT(v.id) AS views
        FROM product_views v
        JOIN products p ON p.id = v.product_id
        WHERE v.viewed_at >= now() - INTERVAL '7 days'
        GROUP BY p.id
        ORDER BY views DESC
        LIMIT 50
      )
      SELECT
        p.id AS product_id,
        p.name AS product_name,
        p.product_type,
        b.name AS brand,
        COALESCE(s.model, ha.model_number, n.model_number, p.name) AS model,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(sp.price)
          FROM product_variants v
          LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id
          WHERE v.product_id = p.id AND sp.price IS NOT NULL
        ) AS price,
        tp.views
      FROM top_products tp
      JOIN products p ON p.id = tp.product_id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN smartphones s ON s.product_id = p.id
      LEFT JOIN home_appliance ha ON ha.product_id = p.id
      LEFT JOIN networking n ON n.product_id = p.id
      ORDER BY tp.views DESC, price ASC NULLS LAST;
    `);

    return res.json({ trending: result.rows });
  } catch (err) {
    console.error("GET /api/public/trending/all error:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.get("/api/public/trending/smartphones", async (req, res) => {
  const result = await db.query(`
    SELECT
      p.id,
      p.name,
      p.slug,
      b.name AS brand,
      COUNT(v.id) AS views,
      s.display_size,
      s.processor,
      (
        SELECT MIN(price)
        FROM product_variants pv
        WHERE pv.product_id = p.id
      ) AS starting_price
    FROM product_views v
    JOIN products p ON p.id = v.product_id
    JOIN smartphones s ON s.product_id = p.id
    LEFT JOIN brands b ON b.id = p.brand_id
    WHERE
      p.product_type = 'smartphone'
      AND v.viewed_at >= NOW() - INTERVAL '7 days'
    GROUP BY p.id, s.id, b.name
    ORDER BY views DESC
    LIMIT 10;
  `);

  res.json(result.rows);
});

app.post("/api/public/compare", async (req, res) => {
  try {
    // Support two payload shapes:
    // 1) { products: [1,2,3] } -> record pairwise comparisons (existing behavior)
    // 2) { left_product_id: 1, right_product_id: 2, product_type: 'smartphone' }
    const body = req.body || {};
    console.log("Comparison payload:", body);

    if (body.left_product_id && body.right_product_id) {
      const left = Number(body.left_product_id);
      const right = Number(body.right_product_id);
      if (
        !Number.isInteger(left) ||
        !Number.isInteger(right) ||
        left <= 0 ||
        right <= 0
      ) {
        return res.status(400).json({ message: "Invalid product ids" });
      }

      // normalize order so A vs B == B vs A
      const [l, r] = [left, right].sort((a, b) => a - b);

      try {
        await db.query(
          `INSERT INTO product_comparisons (product_id, compared_with)
           VALUES ($1, $2)`,
          [l, r],
        );
        return res.json({ message: "Comparison recorded" });
      } catch (err) {
        console.error("POST /api/public/compare insert failed:", err);
        return res.status(500).json({ message: "Failed to record comparison" });
      }
    }

    // Backwards-compatible: accept { products: [1,2,3...] }
    const raw = Array.isArray(body.products) ? body.products : [];
    // sanitize and normalize product ids
    const nums = raw
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n) && n > 0);
    const unique = Array.from(new Set(nums));

    if (unique.length < 2 || unique.length > 4) {
      return res.status(400).json({
        message: "Select minimum 2 and maximum 4 valid products",
      });
    }

    for (let i = 0; i < unique.length; i++) {
      for (let j = i + 1; j < unique.length; j++) {
        await db.query(
          `INSERT INTO product_comparisons (product_id, compared_with)
           VALUES ($1, $2)`,
          [unique[i], unique[j]],
        );
      }
    }

    return res.json({ message: "Comparison recorded" });
  } catch (err) {
    console.error("POST /api/public/compare error:", err);
    return res.status(500).json({ message: "Failed to record comparison" });
  }
});

app.get("/api/public/trending/most-compared", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        p1.id AS product_id,
        p1.name AS product_name,
        (
          SELECT image_url
          FROM product_images
          WHERE product_id = p1.id
          ORDER BY position ASC NULLS LAST, id ASC
          LIMIT 1
        ) AS product_image,
        p2.id AS compared_product_id,
        p2.name AS compared_product_name,
        (
          SELECT image_url
          FROM product_images
          WHERE product_id = p2.id
          ORDER BY position ASC NULLS LAST, id ASC
          LIMIT 1
        ) AS compared_product_image,
        COUNT(pc.id) AS compare_count
      FROM product_comparisons pc
      JOIN products p1 ON p1.id = pc.product_id
      JOIN products p2 ON p2.id = pc.compared_with
      WHERE pc.compared_at >= now() - INTERVAL '7 days'
      GROUP BY p1.id, p1.name, p2.id, p2.name
      ORDER BY compare_count DESC
    `);

    res.json({
      mostCompared: result.rows,
    });
  } catch (err) {
    console.error("Most compared error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get variants for a smartphone
app.get("/api/smartphone/:id/variants", async (req, res) => {
  try {
    const sid = Number(req.params.id);
    if (!sid) return res.status(400).json({ message: "Invalid id" });
    // Resolve product_id from smartphone id then fetch product_variants
    const sres = await db.query(
      "SELECT product_id FROM smartphones WHERE id = $1",
      [sid],
    );
    if (!sres.rows.length)
      return res.status(404).json({ message: "Smartphone not found" });
    const productId = sres.rows[0].product_id;
    const r = await db.query(
      "SELECT * FROM product_variants WHERE product_id = $1 ORDER BY id ASC",
      [productId],
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
      [vid],
    );
    return res.json(r.rows);
  } catch (err) {
    console.error("GET variant store prices error:", err);
    return res.status(500).json({ error: err.message });
  }
});

// PUBLIC: Get smartphone/product details by ID (no auth required)
app.get("/api/public/product/:id", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ message: "Invalid id" });

    console.log(`Fetching public product ${id}`);

    // Fetch product with all details
    const pRes = await db.query(
      `SELECT p.id, p.name, p.product_type, b.name AS brand, b.id AS brand_id
       FROM products p
       INNER JOIN product_publish pub
         ON pub.product_id = p.id
        AND pub.is_published = true
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.id = $1 LIMIT 1`,
      [id],
    );

    if (!pRes.rows.length) {
      console.log(`Product ${id} not found`);
      return res.status(404).json({ message: "Product not found" });
    }

    const product = pRes.rows[0];

    // Fetch product images
    const imgRes = await db.query(
      `SELECT image_url FROM product_images WHERE product_id = $1 ORDER BY position ASC`,
      [id],
    );

    // Fetch product variants
    const varRes = await db.query(
      `SELECT id, variant_key, attributes->>'ram' AS ram, attributes->>'storage' AS storage, base_price
       FROM product_variants WHERE product_id = $1 ORDER BY id ASC`,
      [id],
    );

    // For smartphones, fetch smartphone details
    let smartphoneDetails = null;
    if (product.product_type === "smartphone") {
      const smRes = await db.query(
        `SELECT * FROM smartphones WHERE product_id = $1 LIMIT 1`,
        [id],
      );
      if (smRes.rows.length) {
        smartphoneDetails = smRes.rows[0];
      }
    }

    // Merge all into a single flat object for compatibility with mapSingle
    const computedSlug = product.name
      ? String(product.name)
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/(^-|-$)/g, "")
      : `product-${product.id}`;

    const responseData = {
      id: product.id,
      name: product.name,
      slug: computedSlug,
      product_type: product.product_type,
      brand: product.brand,
      brand_id: product.brand_id,
      images: imgRes.rows.map((r) => r.image_url),
      variants: varRes,
      ...smartphoneDetails,
      // Include the smartphone object as well for backward compatibility
      smartphone: smartphoneDetails,
    };

    res.json(responseData);
  } catch (err) {
    console.error("GET /api/public/product/:id error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Simple global search endpoint with suggestions
app.get("/api/search", async (req, res) => {
  try {
    const q = (req.query.q || "").trim();
    if (!q) return res.json({ results: [] });

    const term = `%${q}%`;

    // Search products by name and brand with image (simplified query)
    const products = await db.query(
      `SELECT DISTINCT
        p.id, 
        p.name, 
        p.product_type,
        b.name AS brand_name,
        (SELECT image_url FROM product_images WHERE product_id = p.id AND position = 1 LIMIT 1) AS image_url
       FROM products p
       INNER JOIN product_publish pub
         ON pub.product_id = p.id
        AND pub.is_published = true
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.name ILIKE $1 
          OR b.name ILIKE $1
       ORDER BY p.name ASC
       LIMIT 10`,
      [term],
    );

    // Search brands only
    const brands = await db.query(
      `SELECT b.id, b.name
       FROM brands b
       WHERE b.name ILIKE $1
         AND EXISTS (
           SELECT 1
           FROM products p
           INNER JOIN product_publish pub
             ON pub.product_id = p.id
            AND pub.is_published = true
           WHERE p.brand_id = b.id
         )
       ORDER BY b.name ASC
       LIMIT 6`,
      [term],
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
        (item) => item.type === "brand" && item.name === b.name,
      );
      const productExists = results.some(
        (item) => item.type === "product" && item.brand_name === b.name,
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

// Wishlist
app.post("/api/wishlist", authenticateCustomer, async (req, res) => {
  const customerId = req.customer.id;
  const { product_id } = req.body;

  console.log(
    `Customer ${customerId} adding product ${product_id} to wishlist`,
  );

  if (!product_id) {
    return res.status(400).json({ message: "Product id required" });
  }

  try {
    await db.query(
      `
      INSERT INTO wishlist (customer_id, product_id)
      VALUES ($1, $2)
      ON CONFLICT (customer_id, product_id) DO NOTHING
      `,
      [customerId, product_id],
    );

    // Return the newly added wishlist item with product summary so client can append it
    const result = await db.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        COALESCE(
          (
            SELECT json_agg(pi.image_url)
            FROM product_images pi
            WHERE pi.product_id = p.id
          ),
          '[]'::json
        ) AS images,
        (
          SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
          FROM product_ratings r
          WHERE r.product_id = p.id
        ) AS rating,
        (
          SELECT MIN(v.base_price)
          FROM product_variants v
          WHERE v.product_id = p.id
        ) AS base_price
      FROM products p
      LEFT JOIN brands b ON b.id = p.brand_id
      WHERE p.id = $1
      `,
      [product_id],
    );

    const item = result.rows[0] || { product_id: product_id };
    res.json({ item });
  } catch (err) {
    console.error("POST /api/wishlist error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.delete(
  "/api/wishlist/:productId",
  authenticateCustomer,
  async (req, res) => {
    const customerId = req.customer.id;
    const productId = Number(req.params.productId);

    await db.query(
      `
      DELETE FROM wishlist
      WHERE customer_id = $1
        AND product_id = $2
      `,
      [customerId, productId],
    );

    res.json({ message: "Removed from wishlist" });
  },
);

app.get("/api/wishlist", authenticateCustomer, async (req, res) => {
  const customerId = req.customer.id;

  const result = await db.query(
    `
    SELECT
      p.id AS product_id,
      p.name,
      p.product_type,
      b.name AS brand_name,

      /* Images */
      COALESCE(
        (
          SELECT json_agg(pi.image_url)
          FROM product_images pi
          WHERE pi.product_id = p.id
        ),
        '[]'::json
      ) AS images,

      /* Rating */
      (
        SELECT ROUND(AVG(r.overall_rating)::numeric, 1)
        FROM product_ratings r
        WHERE r.product_id = p.id
      ) AS rating,

      /* When was this item added to wishlist (latest) */
      MAX(w.created_at) AS added_at,

      /* Variants */
      COALESCE(
        json_agg(
          DISTINCT jsonb_build_object(
            'variant_id', v.id,
            'variant_key', v.variant_key,
            'base_price', v.base_price
          )
        ) FILTER (WHERE v.id IS NOT NULL),
        '[]'::json
      ) AS variants

    FROM wishlist w
    JOIN products p ON p.id = w.product_id
    LEFT JOIN brands b ON b.id = p.brand_id
    LEFT JOIN product_variants v ON v.product_id = p.id

    WHERE w.customer_id = $1
    GROUP BY p.id, b.name
    ORDER BY added_at DESC;
  `,
    [customerId],
  );

  res.json({ wishlist: result.rows });
});

/* -----------------------
  Start server
------------------------*/
/* -----------------------
  RAM & Storage Config API
------------------------*/
// List configs
app.get("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, ram, storage, product_type, created_at FROM ram_storage_long ORDER BY id DESC`,
    );
    return res.json(result.rows);
  } catch (err) {
    console.error("Get ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Create config
app.post("/api/ram-storage-config", authenticate, async (req, res) => {
  try {
    const { ram, storage } = req.body || {};
    const product_type = req.body.product_type || req.body.long || null;

    if (!ram || !storage) {
      return res.status(400).json({ message: "ram and storage are required" });
    }

    const result = await db.query(
      `INSERT INTO ram_storage_long (ram, storage, product_type) VALUES ($1,$2,$3) RETURNING id, ram, storage, product_type, created_at`,
      [ram, storage, product_type],
    );

    return res.status(201).json({ data: result.rows[0] });
  } catch (err) {
    console.error("Create ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Update config
app.put("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = req.params.id;
    const { ram, storage } = req.body || {};
    const product_type = req.body.product_type || req.body.long || null;

    const existing = await db.query(
      `SELECT id FROM ram_storage_long WHERE id = $1`,
      [id],
    );
    if (!existing.rows.length)
      return res.status(404).json({ message: "Not found" });

    const result = await db.query(
      `UPDATE ram_storage_long SET ram = $1, storage = $2, product_type = $3 WHERE id = $4 RETURNING id, ram, storage, product_type, created_at`,
      [ram || null, storage || null, product_type, id],
    );

    return res.json({ data: result.rows[0] });
  } catch (err) {
    console.error("Update ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

// Delete config
app.delete("/api/ram-storage-config/:id", authenticate, async (req, res) => {
  try {
    const id = req.params.id;
    await db.query(`DELETE FROM ram_storage_long WHERE id = $1`, [id]);
    return res.json({ message: "Deleted" });
  } catch (err) {
    console.error("Delete ram-storage-config error:", err);
    return res.status(500).json({ message: "Internal server error" });
  }
});

const importSmartphonesRouter = require("./routes/importSmartphones");
const importLaptopsRouter = require("./routes/importLaptop");
const smartphonesReqRouter = require("./routes/smartphonesReq");
app.use("/api/import", authenticate, importSmartphonesRouter);
app.use("/api/import", authenticate, importLaptopsRouter);
app.use("/api/smartphones", authenticate, smartphonesReqRouter);

async function start() {
  try {
    // Wait for DB to be reachable before running migrations
    try {
      await db.waitForConnection(
        Number(process.env.DB_CONN_RETRIES) || 5,
        Number(process.env.DB_CONN_RETRY_DELAY_MS) || 5000,
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

  app.listen(PORT, "127.0.0.1", async () => {
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
