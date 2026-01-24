const express = require("express");
const ExcelJS = require("exceljs");
const multer = require("multer");
const { db } = require("../db");
const { authenticate } = require("../middleware/auth");

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

/* ---------- helpers ---------- */
const parseJSON = (val, col) => {
  if (!val) return {};
  if (typeof val === "object") return val;
  try {
    return JSON.parse(String(val));
  } catch {
    throw new Error(`Invalid JSON in column: ${col}`);
  }
};

const parseDate = (val) => {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d)) return null;
  return d.toISOString().split("T")[0];
};

/* ---------- route ---------- */
router.post(
  "/laptops",
  authenticate,
  upload.single("file"),
  async (req, res) => {
    if (!req.file?.buffer) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(req.file.buffer);
    const sheet = workbook.worksheets[0];

    if (!sheet) {
      return res.status(400).json({ message: "Worksheet not found" });
    }

    /* ---------- header map ---------- */
    const headers = {};
    sheet.getRow(1).eachCell((cell, i) => {
      headers[String(cell.value).trim().toLowerCase()] = i;
    });

    const getVal = (row, name) => {
      const idx = headers[name];
      if (!idx) return null;
      const v = row.getCell(idx).value;
      return v?.text ?? v;
    };

    let inserted = 0;
    let skipped = 0;
    let failed = 0;
    const rows = [];

    const client = await db.connect();

    try {
      for (let i = 2; i <= sheet.rowCount; i++) {
        const row = sheet.getRow(i);

        try {
          await client.query("BEGIN");

          /* ---------- required fields ---------- */
          const product_name = String(getVal(row, "product_name") || "").trim();
          const brand_name = String(getVal(row, "brand_name") || "").trim();
          const model = String(getVal(row, "model") || "").trim();
          const category = String(getVal(row, "category") || "").trim();
          const launch_date = parseDate(getVal(row, "launch_date"));

          if (!product_name || !brand_name || !model) {
            throw new Error("Missing product_name / brand_name / model");
          }

          /* ---------- brand (FK) ---------- */
          const brandRes = await client.query(
            "SELECT id FROM brands WHERE LOWER(name)=LOWER($1)",
            [brand_name],
          );
          if (!brandRes.rowCount) {
            throw new Error(`Brand not found: ${brand_name}`);
          }
          const brand_id = brandRes.rows[0].id;

          /* ---------- product (name UNIQUE safe) ---------- */
          let productId;
          const prodChk = await client.query(
            "SELECT id FROM products WHERE LOWER(name)=LOWER($1)",
            [product_name],
          );

          if (prodChk.rowCount) {
            productId = prodChk.rows[0].id;
          } else {
            const prodIns = await client.query(
              `
              INSERT INTO products (name, product_type, brand_id)
              VALUES ($1, 'laptop', $2)
              RETURNING id
              `,
              [product_name, brand_id],
            );
            productId = prodIns.rows[0].id;
          }

          /* ---------- duplicate laptop ---------- */
          const lapChk = await client.query(
            "SELECT product_id FROM laptop WHERE product_id=$1",
            [productId],
          );
          if (lapChk.rowCount) {
            await client.query("ROLLBACK");
            skipped++;
            rows.push({
              row: i,
              status: "SKIPPED",
              reason: "Laptop already exists",
            });
            continue;
          }

          /* ---------- JSONB columns ---------- */
          const cpu = parseJSON(getVal(row, "cpu_json"), "cpu_json");
          const display = parseJSON(
            getVal(row, "display_json"),
            "display_json",
          );
          const memory = parseJSON(getVal(row, "memory_json"), "memory_json");
          const storage = parseJSON(
            getVal(row, "storage_json"),
            "storage_json",
          );
          const battery = parseJSON(
            getVal(row, "battery_json"),
            "battery_json",
          );
          const connectivity = parseJSON(
            getVal(row, "connectivity_json"),
            "connectivity_json",
          );
          const physical = parseJSON(
            getVal(row, "physical_json"),
            "physical_json",
          );
          const software = parseJSON(
            getVal(row, "software_json"),
            "software_json",
          );
          const features = parseJSON(
            getVal(row, "features_json"),
            "features_json",
          );
          const warranty = parseJSON(
            getVal(row, "warranty_json"),
            "warranty_json",
          );

          /* ---------- meta JSON (IMPORTANT) ---------- */
          const meta = {
            model,
            brand: brand_name,
            category,
            launch_date,
            ...(parseJSON(getVal(row, "meta_json"), "meta_json") || {}),
          };

          /* ---------- insert laptop ---------- */
          await client.query(
            `
            INSERT INTO laptop (
              product_id,
              cpu,
              display,
              memory,
              storage,
              battery,
              connectivity,
              physical,
              software,
              features,
              warranty,
              meta
            )
            VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
            )
            `,
            [
              productId,
              JSON.stringify(cpu),
              JSON.stringify(display),
              JSON.stringify(memory),
              JSON.stringify(storage),
              JSON.stringify(battery),
              JSON.stringify(connectivity),
              JSON.stringify(physical),
              JSON.stringify(software),
              JSON.stringify(features),
              JSON.stringify(warranty),
              JSON.stringify(meta),
            ],
          );

          await client.query("COMMIT");
          inserted++;
          rows.push({ row: i, status: "INSERTED" });
        } catch (err) {
          await client.query("ROLLBACK");
          failed++;
          rows.push({ row: i, status: "FAILED", error: err.message });
        }
      }

      res.json({
        summary: {
          total_rows: sheet.rowCount - 1,
          inserted,
          skipped,
          failed,
        },
        rows,
      });
    } finally {
      client.release();
    }
  },
);

module.exports = router;
