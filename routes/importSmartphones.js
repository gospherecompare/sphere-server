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

function safeJSONParse(raw, column, row) {
  if (raw === null || raw === undefined || raw === "") return null;
  try {
    return typeof raw === "object" ? raw : JSON.parse(String(raw));
  } catch (err) {
    // try a forgiving fallback: replace single quotes with double quotes
    try {
      const alt = String(raw)
        .replace(/\u2018|\u2019|\u201C|\u201D/g, '"')
        .replace(/'/g, '"');
      return JSON.parse(alt);
    } catch (err2) {
      console.warn(
        `Row ${row || "?"}: Invalid JSON in column: ${column} - ${err.message}`,
      );
      return null;
    }
  }
}

function parseSensors(raw, row) {
  if (raw === null || raw === undefined || raw === "") return null;
  if (Array.isArray(raw))
    return JSON.stringify(raw.map((s) => String(s).trim()));
  if (typeof raw === "object") return JSON.stringify(raw);
  const str = String(raw).trim();
  try {
    const parsed = JSON.parse(str);
    if (Array.isArray(parsed))
      return JSON.stringify(parsed.map((s) => String(s).trim()));
  } catch (e) {
    // not JSON array, continue
  }
  const parts = str
    .split(/[|,;]+/)
    .map((p) => p.trim())
    .filter(Boolean);
  return parts.length ? JSON.stringify(parts) : null;
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

        let inTransaction = false;
        try {
          await client.query("BEGIN");
          inTransaction = true;

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
            Brand (lenient match)
          -------------------------- */
          let brandRes = await client.query(
            "SELECT id FROM brands WHERE LOWER(name)=LOWER($1)",
            [brand_name],
          );
          if (!brandRes.rowCount) {
            // try partial match to avoid strict failures
            brandRes = await client.query(
              "SELECT id FROM brands WHERE LOWER(name) LIKE $1 LIMIT 1",
              [`%${brand_name.toLowerCase()}%`],
            );
          }
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
            Parse JSON fields (tolerant)
          -------------------------- */
          let images = safeJSONParse(
            getCell(row, "images_json"),
            "images_json",
            i,
          );
          const build_design =
            safeJSONParse(
              getCell(row, "build_design_json"),
              "build_design_json",
              i,
            ) || {};
          const display =
            safeJSONParse(getCell(row, "display_json"), "display_json", i) ||
            {};
          const performance =
            safeJSONParse(
              getCell(row, "performance_json"),
              "performance_json",
              i,
            ) || {};
          const camera =
            safeJSONParse(getCell(row, "camera_json"), "camera_json", i) || {};
          const battery =
            safeJSONParse(getCell(row, "battery_json"), "battery_json", i) ||
            {};
          const connectivity =
            safeJSONParse(
              getCell(row, "connectivity_json"),
              "connectivity_json",
              i,
            ) || {};
          const network =
            safeJSONParse(getCell(row, "network_json"), "network_json", i) ||
            {};
          const ports =
            safeJSONParse(getCell(row, "ports_json"), "ports_json", i) || {};
          const audio =
            safeJSONParse(getCell(row, "audio_json"), "audio_json", i) || {};
          const multimedia =
            safeJSONParse(
              getCell(row, "multimedia_json"),
              "multimedia_json",
              i,
            ) || {};

          /* -------------------------
            Smartphone exists?
          -------------------------- */
          const modelKey = model.replace(/\s+/g, "").toLowerCase();
          const phoneCheck = await client.query(
            `SELECT id FROM smartphones
             WHERE product_id=$1 OR REPLACE(LOWER(model),' ','')=$2`,
            [productId, modelKey],
          );

          let createdPhone = false;

          if (!phoneCheck.rowCount) {
            /* -------------------------
              Validate/normalise parsed JSON columns and sensors
            -------------------------- */
            if (!Array.isArray(images)) {
              console.warn(
                `Row ${i}: images_json is not an array; coercing to []`,
              );
              images = Array.isArray(images) ? images : [];
            }

            const sensorsRaw = getCell(row, "sensors");
            const sensors = parseSensors(sensorsRaw, i);

            /* -------------------------
              Insert smartphone (core)
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

            createdPhone = true;

            // commit core product+smartphone so variant failures won't rollback this
            await client.query("COMMIT");
            inTransaction = false;

            if (createdPhone) {
              inserted++;
            }

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
          } else {
            // If phone already exists, commit the select-only transaction to clear state
            await client.query("COMMIT");
            inTransaction = false;
          }
          /* -------------------------
            Variants (optional, non-fatal)
            Process after smartphone/product committed, per-variant errors logged
          -------------------------- */
          const variantsRaw =
            getCell(row, "variants_json") ?? getCell(row, "variants");
          const variantsHeaderExists =
            headers["variants_json"] || headers["variants"];
          const variantsColName = headers["variants_json"]
            ? "variants_json"
            : "variants";
          let variants = safeJSONParse(variantsRaw, variantsColName, i);
          const variantReport = { inserted: 0, failed: 0, errors: [] };

          // If parsed object wraps the array: { variants: [ ... ] }
          if (
            variants &&
            typeof variants === "object" &&
            !Array.isArray(variants) &&
            Array.isArray(variants.variants)
          ) {
            variants = variants.variants;
          }

          // If parsing failed but raw contains an array-like substring, try to extract it
          if (
            (!variants || !Array.isArray(variants)) &&
            typeof variantsRaw === "string"
          ) {
            const arrMatch = variantsRaw.match(/\[([\s\S]*)\]/);
            if (arrMatch) {
              try {
                const cand = arrMatch[0]
                  .replace(/\u2018|\u2019|\u201C|\u201D/g, '"')
                  .replace(/'/g, '"');
                const parsedCand = JSON.parse(cand);
                if (Array.isArray(parsedCand)) variants = parsedCand;
              } catch (e) {
                console.warn(
                  `Row ${i}: failed to extract variants array from raw cell: ${e.message}`,
                );
              }
            }
          }

          if (!Array.isArray(variants) || !variants.length) {
            if (!variantsHeaderExists) {
              missingValues.add("variants_json");
            } else {
              console.warn(
                `Row ${i}: variants column present but could not parse variants array.`,
              );
            }
            // do not rollback or skip; variants are optional for import stability
          } else {
            for (const v of variants) {
              try {
                const variantKey =
                  v.variant_key || `${v.ram || "na"}_${v.storage || "na"}`;
                const attributes = { ram: v.ram, storage: v.storage };
                if (v.variant_id !== undefined && v.variant_id !== null) {
                  attributes.external_variant_id = v.variant_id;
                }
                const basePrice = v.base_price ?? v.price ?? null;

                const vRes = await client.query(
                  `INSERT INTO product_variants
                   (product_id, variant_key, attributes, base_price)
                   VALUES ($1,$2,$3,$4)
                   ON CONFLICT (product_id, variant_key) DO UPDATE SET
                     attributes = COALESCE(EXCLUDED.attributes, product_variants.attributes),
                     base_price = COALESCE(EXCLUDED.base_price, product_variants.base_price)
                   RETURNING id`,
                  [
                    productId,
                    variantKey,
                    JSON.stringify(attributes),
                    basePrice,
                  ],
                );

                if (vRes.rowCount) {
                  const variantId = vRes.rows[0].id;
                  variantReport.inserted++;

                  if (Array.isArray(v.store_prices) && v.store_prices.length) {
                    for (const sp of v.store_prices) {
                      try {
                        const storeName =
                          sp.store_name || sp.store || sp.name || null;
                        if (!storeName) continue;
                        await client.query(
                          `INSERT INTO variant_store_prices
                           (variant_id, store_name, price, url, offer_text, delivery_info)
                           VALUES ($1,$2,$3,$4,$5,$6)
                           ON CONFLICT (variant_id, store_name) DO UPDATE SET
                             price = EXCLUDED.price,
                             url = EXCLUDED.url,
                             offer_text = EXCLUDED.offer_text,
                             delivery_info = EXCLUDED.delivery_info`,
                          [
                            variantId,
                            storeName,
                            sp.price ?? null,
                            sp.url ?? null,
                            sp.offer_text ?? null,
                            sp.delivery_info ?? null,
                          ],
                        );
                      } catch (spErr) {
                        variantReport.failed++;
                        const msg = `Row ${i} variant ${variantKey} store_price error: ${spErr.message}`;
                        variantReport.errors.push(msg);
                        console.warn(msg);
                      }
                    }
                  }
                }
              } catch (verr) {
                variantReport.failed++;
                const msg = `Row ${i} variant upsert error: ${verr.message}`;
                variantReport.errors.push(msg);
                console.warn(msg);
              }
            }
          }

          // Finalize row report: consider phone insertion status
          const rowStatus = createdPhone ? "INSERTED" : "EXISTS";
          report.push({ row: i, status: rowStatus, variants: variantReport });
        } catch (err) {
          try {
            if (inTransaction) await client.query("ROLLBACK");
          } catch (rbErr) {
            console.error("Rollback error:", rbErr.message || rbErr);
          }
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
