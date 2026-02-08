const express = require("express");
const { db } = require("../db");
const { authenticate } = require("../middleware/auth");

const router = express.Router();

function safeJSONParse(raw) {
  if (raw === null || raw === undefined || raw === "") return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch (err) {
    try {
      const alt = String(raw)
        .replace(/\u2018|\u2019|\u201C|\u201D/g, '"')
        .replace(/'/g, '"');
      return JSON.parse(alt);
    } catch (err2) {
      return null;
    }
  }
}

function parseSensors(raw) {
  if (raw === null || raw === undefined || raw === "") return null;
  if (Array.isArray(raw))
    return JSON.stringify(raw.map((s) => String(s).trim()));
  if (typeof raw === "object") return JSON.stringify(raw);
  const str = String(raw).trim();
  const parts = str
    .split(/[|,;]+/)
    .map((p) => p.trim())
    .filter(Boolean);
  return parts.length ? JSON.stringify(parts) : null;
}

function parseDateForImport(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d)) return null;
  return d.toISOString().split("T")[0];
}

router.post("/req", authenticate, async (req, res) => {
  const b = req.body || {};

  const product_name = (b.product_name || b.name || "").trim();
  const brand_name = (b.brand_name || b.brand || "").trim();
  const model = (b.model || "").trim();

  if (!product_name || !brand_name || !model) {
    return res
      .status(400)
      .json({ message: "product_name, brand_name and model are required" });
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    // brand (create if missing)
    let brandRes = await client.query(
      "SELECT id FROM brands WHERE LOWER(name)=LOWER($1) LIMIT 1",
      [brand_name],
    );
    let brand_id;
    if (brandRes.rowCount) {
      brand_id = brandRes.rows[0].id;
    } else {
      const ins = await client.query(
        "INSERT INTO brands (name) VALUES ($1) RETURNING id",
        [brand_name],
      );
      brand_id = ins.rows[0].id;
    }

    // product
    let productId;
    const productCheck = await client.query(
      "SELECT id FROM products WHERE LOWER(name)=LOWER($1)",
      [product_name],
    );
    if (productCheck.rowCount) {
      productId = productCheck.rows[0].id;
    } else {
      const pRes = await client.query(
        `INSERT INTO products (name, product_type, brand_id) VALUES ($1,'smartphone',$2) RETURNING id`,
        [product_name, brand_id],
      );
      productId = pRes.rows[0].id;
    }

    // prepare JSON fields
    const images =
      safeJSONParse(b.images_json) || safeJSONParse(b.images) || [];
    const build_design = safeJSONParse(b.build_design_json) || {};
    const display = safeJSONParse(b.display_json) || {};
    const performance = safeJSONParse(b.performance_json) || {};
    const camera = safeJSONParse(b.camera_json) || {};
    const battery = safeJSONParse(b.battery_json) || {};
    const connectivity = safeJSONParse(b.connectivity_json) || {};
    const network = safeJSONParse(b.network_json) || {};
    const ports =
      safeJSONParse(b.port_json) || safeJSONParse(b.ports_json) || {};
    const audio = safeJSONParse(b.audio_json) || {};
    const multimedia = safeJSONParse(b.multimedia_json) || {};
    const sensors = parseSensors(b.sensors || null);

    // prevent duplicates
    const modelKey = model.replace(/\s+/g, "").toLowerCase();
    const phoneCheck = await client.query(
      `SELECT id FROM smartphones WHERE product_id=$1 OR REPLACE(LOWER(model),' ','')=$2`,
      [productId, modelKey],
    );
    if (phoneCheck.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({ message: "Smartphone already exists" });
    }

    await client.query(
      `INSERT INTO smartphones
         (product_id, category, brand, model, launch_date, images, build_design, display, performance, camera, battery, connectivity, network, ports, audio, multimedia, sensors)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
      [
        productId,
        b.category || null,
        brand_name,
        model,
        parseDateForImport(b.launch_date),
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

    // insert product images
    if (Array.isArray(images) && images.length) {
      for (let i = 0; i < images.length; i++) {
        await client.query(
          `INSERT INTO product_images (product_id, image_url, position) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
          [productId, images[i], i + 1],
        );
      }
    }

    // upsert product sphere ratings
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
    } catch (e) {
      // non-fatal
      console.warn("sphere ratings upsert failed:", e.message || e);
    }

    // variants
    const variants =
      safeJSONParse(b.variants_json) || safeJSONParse(b.variants) || [];
    if (Array.isArray(variants) && variants.length) {
      for (const v of variants) {
        try {
          const variantKey =
            v.variant_key || `${v.ram || "na"}_${v.storage || "na"}`;
          const attributes = { ram: v.ram, storage: v.storage };
          const basePrice = v.base_price ?? v.price ?? null;

          const variantRes = await client.query(
            `INSERT INTO product_variants (product_id, variant_key, attributes, base_price)
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (product_id, variant_key)
             DO UPDATE SET
               attributes = COALESCE(EXCLUDED.attributes, product_variants.attributes),
               base_price = COALESCE(EXCLUDED.base_price, product_variants.base_price)
             RETURNING id`,
            [productId, variantKey, JSON.stringify(attributes), basePrice],
          );

          const variantId = variantRes.rows[0]?.id;

          // Insert / upsert store prices for this variant
          const storePrices =
            Array.isArray(v.store_prices) && v.store_prices.length
              ? v.store_prices
              : Array.isArray(v.stores)
                ? v.stores
                : [];

          if (variantId && storePrices.length) {
            for (const sp of storePrices) {
              try {
                const storeName =
                  sp.store_name ||
                  sp.store ||
                  sp.storeName ||
                  "Store";
                await client.query(
                  `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info)
                   VALUES ($1,$2,$3,$4,$5,$6)
                   ON CONFLICT (variant_id, store_name)
                   DO UPDATE SET
                     price = EXCLUDED.price,
                     url = EXCLUDED.url,
                     offer_text = EXCLUDED.offer_text,
                     delivery_info = EXCLUDED.delivery_info`,
                  [
                    variantId,
                    storeName,
                    sp.price ?? null,
                    sp.url ?? null,
                    sp.offer_text ?? sp.offerText ?? null,
                    sp.delivery_info ?? sp.deliveryInfo ?? null,
                  ],
                );
              } catch (spErr) {
                console.warn(
                  "store_price upsert failed:",
                  spErr.message || spErr,
                );
              }
            }
          }
        } catch (verr) {
          console.warn("variant upsert failed:", verr.message || verr);
        }
      }
    }

    await client.query("COMMIT");
    return res.json({
      success: true,
      message: "Smartphone inserted",
      product_id: productId,
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (rb) {
      /* ignore */
    }
    console.error("/api/smartphones/req error:", err.message || err);
    return res.status(500).json({ error: err.message || String(err) });
  } finally {
    client.release();
  }
});

module.exports = router;
