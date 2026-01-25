const express = require("express");
const ExcelJS = require("exceljs");
const multer = require("multer");
const { db } = require("../db");
const { authenticate } = require("../middleware/auth");

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

/* -------------------------
  Helpers
-------------------------- */

function parseDateForImport(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d)) return null;
  return d.toISOString().split("T")[0];
}

function safeJSONParse(raw, column) {
  if (!raw) return null;
  try {
    return typeof raw === "object" ? raw : JSON.parse(String(raw));
  } catch {
    throw new Error(`Invalid JSON in column: ${column}`);
  }
}

/* -------------------------
  IMPORT SMARTPHONES
-------------------------- */

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

    /* -------------------------
      Header mapping
    -------------------------- */
    const headers = {};
    sheet.getRow(1).eachCell((cell, col) => {
      headers[String(cell.value).trim().toLowerCase()] = col;
    });

    const getCell = (row, name) => {
      const col = headers[name];
      if (!col) return null;
      const v = row.getCell(col).value;
      return v?.text ?? v;
    };

    let inserted = 0;
    let skipped = 0;
    let failed = 0;
    const report = [];
    const missingValues = new Set();

    const client = await db.connect();

    try {
      for (let i = 2; i <= sheet.rowCount; i++) {
        const row = sheet.getRow(i);

        try {
          await client.query("BEGIN");

          /* -------------------------
            Required fields
          -------------------------- */
          const product_name = String(
            getCell(row, "product_name") || "",
          ).trim();
          const brand_name = String(getCell(row, "brand_name") || "").trim();
          const category = String(getCell(row, "category") || "").trim();
          const model = String(getCell(row, "model") || "").trim();

          const missingFields = [];
          if (!product_name) missingFields.push("product_name");
          if (!brand_name) missingFields.push("brand_name");
          if (!model) missingFields.push("model");

          if (missingFields.length) {
            missingFields.forEach((f) => missingValues.add(f));
            await client.query("ROLLBACK");
            skipped++;
            report.push({
              row: i,
              status: "MISSING_FIELD",
              fields: missingFields,
            });
            continue;
          }

          /* -------------------------
            Brand
          -------------------------- */
          const brandRes = await client.query(
            "SELECT id FROM brands WHERE LOWER(name)=LOWER($1)",
            [brand_name],
          );
          if (!brandRes.rowCount) {
            missingValues.add(brand_name);
            await client.query("ROLLBACK");
            skipped++;
            report.push({ row: i, status: "MISSING_BRAND", brand: brand_name });
            continue;
          }
          const brand_id = brandRes.rows[0].id;

          /* -------------------------
            Product
          -------------------------- */
          let productId;
          const productCheck = await client.query(
            "SELECT id FROM products WHERE LOWER(name)=LOWER($1)",
            [product_name],
          );

          if (productCheck.rowCount) {
            productId = productCheck.rows[0].id;
          } else {
            const pRes = await client.query(
              `INSERT INTO products (name, product_type, brand_id)
               VALUES ($1,'smartphone',$2)
               RETURNING id`,
              [product_name, brand_id],
            );
            productId = pRes.rows[0].id;
          }

          /* -------------------------
            Parse JSON fields (used for rating extraction)
          -------------------------- */
          const images =
            safeJSONParse(getCell(row, "images_json"), "images_json") || [];

          const build_design =
            safeJSONParse(
              getCell(row, "build_design_json"),
              "build_design_json",
            ) || {};

          const display =
            safeJSONParse(getCell(row, "display_json"), "display_json") || {};

          const performance =
            safeJSONParse(
              getCell(row, "performance_json"),
              "performance_json",
            ) || {};

          const camera =
            safeJSONParse(getCell(row, "camera_json"), "camera_json") || {};

          const battery =
            safeJSONParse(getCell(row, "battery_json"), "battery_json") || {};

          const connectivity =
            safeJSONParse(
              getCell(row, "connectivity_json"),
              "connectivity_json",
            ) || {};

          const network =
            safeJSONParse(getCell(row, "network_json"), "network_json") || {};

          // connectivity and network are stored separately

          const ports =
            safeJSONParse(getCell(row, "ports_json"), "ports_json") || {};

          const audio =
            safeJSONParse(getCell(row, "audio_json"), "audio_json") || {};

          const multimedia =
            safeJSONParse(getCell(row, "multimedia_json"), "multimedia_json") ||
            {};

          /* -------------------------
            Smartphone exists?
          -------------------------- */
          const modelKey = model.replace(/\s+/g, "").toLowerCase();
          const phoneCheck = await client.query(
            `SELECT id FROM smartphones
             WHERE product_id=$1 OR REPLACE(LOWER(model),' ','')=$2`,
            [productId, modelKey],
          );

          if (!phoneCheck.rowCount) {
            /* -------------------------
              JSON columns
            -------------------------- */

            const images =
              safeJSONParse(getCell(row, "images_json"), "images_json") || [];

            if (!Array.isArray(images)) {
              throw new Error("images_json must be a JSON ARRAY");
            }

            const build_design =
              safeJSONParse(
                getCell(row, "build_design_json"),
                "build_design_json",
              ) || {};

            const display =
              safeJSONParse(getCell(row, "display_json"), "display_json") || {};

            const performance =
              safeJSONParse(
                getCell(row, "performance_json"),
                "performance_json",
              ) || {};

            const camera =
              safeJSONParse(getCell(row, "camera_json"), "camera_json") || {};

            const battery =
              safeJSONParse(getCell(row, "battery_json"), "battery_json") || {};

            const connectivity =
              safeJSONParse(
                getCell(row, "connectivity_json"),
                "connectivity_json",
              ) || {};

            const network =
              safeJSONParse(getCell(row, "network_json"), "network_json") || {};

            // connectivity and network are stored separately

            const ports =
              safeJSONParse(getCell(row, "ports_json"), "ports_json") || {};

            const audio =
              safeJSONParse(getCell(row, "audio_json"), "audio_json") || {};

            const multimedia =
              safeJSONParse(
                getCell(row, "multimedia_json"),
                "multimedia_json",
              ) || {};

            const sensorsRaw = getCell(row, "sensors");
            const sensors = sensorsRaw
              ? JSON.stringify(
                  String(sensorsRaw)
                    .split("|")
                    .map((s) => s.trim()),
                )
              : null;

            /* -------------------------
              Insert smartphone
            -------------------------- */
            await client.query(
              `INSERT INTO smartphones
               (product_id, category, brand, model, launch_date,
                images, build_design, display, performance,
                camera, battery, connectivity, network, ports, audio, multimedia, sensors)
               VALUES
               ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
              [
                productId,
                category,
                brand_name,
                model,
                parseDateForImport(getCell(row, "launch_date")),
                JSON.stringify(images),
                JSON.stringify(build_design),
                JSON.stringify(display),
                JSON.stringify(performance),
                JSON.stringify(camera),
                JSON.stringify(battery),
                JSON.stringify(connectivity),
                JSON.stringify(network),
                JSON.stringify(ports),
                JSON.stringify(audio),
                JSON.stringify(multimedia),
                sensors,
              ],
            );

            /* -------------------------
              Product images table
            -------------------------- */
            for (let p = 0; p < images.length; p++) {
              await client.query(
                `INSERT INTO product_images
                 (product_id, image_url, position)
                 VALUES ($1,$2,$3)
                 ON CONFLICT DO NOTHING`,
                [productId, images[p], p + 1],
              );
            }

            // Upsert per-section sphere ratings if present in parsed JSONs
            try {
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
                  JSON.stringify(build_design?.sphere_rating || null),
                  JSON.stringify(display?.sphere_rating || null),
                  JSON.stringify(performance?.sphere_rating || null),
                  JSON.stringify(camera?.sphere_rating || null),
                  JSON.stringify(battery?.sphere_rating || null),
                  JSON.stringify(connectivity?.sphere_rating || null),
                  JSON.stringify(network?.sphere_rating || null),
                ],
              );
            } catch (uir) {
              console.error(
                "Import sphere ratings upsert error:",
                uir.message || uir,
              );
            }
          }

          /* -------------------------
            Variants (MANDATORY)
          -------------------------- */
          const variants = safeJSONParse(
            getCell(row, "variants_json"),
            "variants_json",
          );

          if (!Array.isArray(variants) || !variants.length) {
            missingValues.add("variants_json");
            await client.query("ROLLBACK");
            skipped++;
            report.push({
              row: i,
              status: "MISSING_FIELD",
              fields: ["variants_json"],
            });
            continue;
          }

          let variantInserted = false;

          for (const v of variants) {
            const variantKey =
              v.variant_key || `${v.ram || "na"}_${v.storage || "na"}`;

            const vRes = await client.query(
              `INSERT INTO product_variants
               (product_id, variant_key, attributes, base_price)
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (product_id, variant_key) DO NOTHING
               RETURNING id`,
              [
                productId,
                variantKey,
                JSON.stringify({ ram: v.ram, storage: v.storage }),
                v.price || null,
              ],
            );

            if (vRes.rowCount) {
              variantInserted = true;
            }
          }

          if (!variantInserted) {
            await client.query("ROLLBACK");
            skipped++;
            report.push({ row: i, status: "SKIPPED" });
            continue;
          }

          await client.query("COMMIT");
          inserted++;
          report.push({ row: i, status: "INSERTED" });
        } catch (err) {
          await client.query("ROLLBACK");
          failed++;
          report.push({ row: i, status: "FAILED", error: err.message });
        }
      }

      res.json({
        summary: {
          total_rows: sheet.rowCount - 1,
          inserted,
          skipped,
          failed,
        },
        rows: report,
        missing: Array.from(missingValues),
      });
    } finally {
      client.release();
    }
  },
);

module.exports = router;
