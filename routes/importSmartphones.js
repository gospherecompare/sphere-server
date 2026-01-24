const express = require("express");
const ExcelJS = require("exceljs");
const multer = require("multer");
const { db } = require("../db");
const { authenticate } = require("../middleware/auth");

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

function parseDateForImport(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d)) return null;
  return d.toISOString().split("T")[0]; // DATE safe
}

router.post(
  "/smartphones",
  authenticate,
  upload.single("file"),
  async (req, res) => {
    if (!req.file?.buffer) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(req.file.buffer);
    const sheet =
      workbook.getWorksheet("smartphones_import") || workbook.worksheets[0];

    if (!sheet) {
      return res.status(400).json({ message: "Worksheet not found" });
    }

    const headerRow = sheet.getRow(1);
    const headers = {};
    headerRow.eachCell((cell, col) => {
      headers[String(cell.value).trim().toLowerCase()] = col;
    });

    const getCell = (row, name) => {
      const col = headers[name];
      if (!col) return null;
      const v = row.getCell(col).value;
      return v?.text ?? v;
    };

    const parseJSON = (row, key, def) => {
      const raw = getCell(row, key);
      if (!raw) return def;
      try {
        return typeof raw === "object" ? raw : JSON.parse(String(raw));
      } catch {
        throw new Error(`Invalid JSON in column ${key}`);
      }
    };

    let inserted = 0,
      skipped = 0,
      failed = 0;
    const results = [];

    const client = await db.connect();

    try {
      for (let i = 2; i <= sheet.rowCount; i++) {
        const row = sheet.getRow(i);

        try {
          await client.query("BEGIN");

          const product_name = String(
            getCell(row, "product_name") || "",
          ).trim();
          const brand_name = String(getCell(row, "brand_name") || "").trim();
          const category = String(getCell(row, "category") || "").trim();
          const model = String(getCell(row, "model") || "").trim();
          const modelKey = model.replace(/\s+/g, "").toLowerCase();

          if (!product_name || !brand_name || !model) {
            throw new Error("Missing required fields");
          }

          // --- Brand ---
          const brandRes = await client.query(
            "SELECT id FROM brands WHERE LOWER(name)=LOWER($1)",
            [brand_name],
          );
          if (!brandRes.rowCount) throw new Error("Brand not found");
          const brand_id = brandRes.rows[0].id;

          // --- Existing product check (NAME UNIQUE) ---
          let productId = null;

          const byName = await client.query(
            "SELECT id FROM products WHERE LOWER(name)=LOWER($1)",
            [product_name],
          );
          if (byName.rowCount) {
            productId = byName.rows[0].id;
          }

          // --- Insert product if new ---
          if (!productId) {
            const pRes = await client.query(
              `INSERT INTO products (name, product_type, brand_id)
               VALUES ($1,'smartphone',$2) RETURNING id`,
              [product_name, brand_id],
            );
            productId = pRes.rows[0].id;
          }

          // --- Smartphone exists check (MODEL) ---
          const sCheck = await client.query(
            `SELECT id FROM smartphones
             WHERE product_id=$1 OR REPLACE(LOWER(model),' ','')=$2`,
            [productId, modelKey],
          );

          if (!sCheck.rowCount) {
            // --- JSON fields ---
            const images = parseJSON(row, "images_json", []);
            const colors = parseJSON(row, "colors_json", []);
            const build_design = parseJSON(row, "build_design_json", {});
            const display = parseJSON(row, "display_json", {});
            const performance = parseJSON(row, "performance_json", {});
            const camera = parseJSON(row, "camera_json", {});
            const battery = parseJSON(row, "battery_json", {});
            const ports = parseJSON(row, "ports_json", {});
            const audio = parseJSON(row, "audio_json", {});
            const multimedia = parseJSON(row, "multimedia_json", {});

            // --- Connectivity merge (Option A) ---
            const connectivity = parseJSON(row, "connectivity_json", {});
            const network = parseJSON(row, "network_json", {});
            const connectivity_network = {
              ...connectivity,
              network,
            };

            // --- Sensors TEXT → JSONB ---
            const sensorsRaw = getCell(row, "sensors");
            const sensors = sensorsRaw
              ? JSON.stringify(
                  String(sensorsRaw)
                    .split(",")
                    .map((s) => s.trim()),
                )
              : null;

            await client.query(
              `INSERT INTO smartphones
               (product_id, category, brand, model, launch_date,
                images, colors, build_design, display, performance,
                camera, battery, connectivity_network, ports,
                audio, multimedia, sensors)
               VALUES
               ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
              [
                productId,
                category,
                brand_name,
                model,
                parseDateForImport(getCell(row, "launch_date")),
                JSON.stringify(images),
                JSON.stringify(colors),
                JSON.stringify(build_design),
                JSON.stringify(display),
                JSON.stringify(performance),
                JSON.stringify(camera),
                JSON.stringify(battery),
                JSON.stringify(connectivity_network),
                JSON.stringify(ports),
                JSON.stringify(audio),
                JSON.stringify(multimedia),
                sensors,
              ],
            );
          }

          // --- Variants (MANDATORY) ---
          const variants = parseJSON(row, "variants_json", []);
          if (!Array.isArray(variants) || !variants.length) {
            throw new Error("variants_json required");
          }

          let variantInserted = false;

          for (const v of variants) {
            const key =
              v.variant_key || `${v.ram || "na"}_${v.storage || "na"}`;

            const vRes = await client.query(
              `INSERT INTO product_variants
               (product_id, variant_key, attributes, base_price)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (product_id, variant_key) DO NOTHING
               RETURNING id`,
              [
                productId,
                key,
                JSON.stringify(
                  v.attributes || { ram: v.ram, storage: v.storage },
                ),
                v.base_price || null,
              ],
            );

            if (vRes.rowCount) {
              variantInserted = true;
              const variantId = vRes.rows[0].id;

              for (const sp of v.stores || []) {
                await client.query(
                  `INSERT INTO variant_store_prices
                   (variant_id, store_name, price, url)
                   VALUES ($1,$2,$3,$4)
                   ON CONFLICT (variant_id, store_name)
                   DO UPDATE SET price=EXCLUDED.price, url=EXCLUDED.url`,
                  [variantId, sp.store_name, sp.price || null, sp.url || null],
                );
              }
            }
          }

          if (!variantInserted) {
            await client.query("ROLLBACK");
            skipped++;
            results.push({ row: i, status: "SKIPPED" });
            continue;
          }

          await client.query("COMMIT");
          inserted++;
          results.push({ row: i, status: "INSERTED" });
        } catch (err) {
          await client.query("ROLLBACK");
          failed++;
          results.push({ row: i, status: "FAILED", error: err.message });
        }
      }

      res.json({
        summary: {
          total_rows: sheet.rowCount - 1,
          inserted,
          skipped,
          failed,
        },
        rows: results,
      });
    } finally {
      client.release();
    }
  },
);

module.exports = router;
