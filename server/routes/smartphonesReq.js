const express = require("express");
const { db } = require("../db");
const { authenticate } = require("../middleware/auth");

const router = express.Router();

const hasOwn = (obj, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

const isPlainObject = (value) =>
  value && typeof value === "object" && !Array.isArray(value);

const pickFirstPresent = (...values) => {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && !value.trim()) continue;
    return value;
  }
  return null;
};

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

function mergeSectionObjects(...parts) {
  const out = {};
  for (const part of parts) {
    if (part && typeof part === "object" && !Array.isArray(part)) {
      Object.assign(out, part);
    }
  }
  return out;
}

function toNumericPrice(val) {
  if (val === null || val === undefined || val === "") return null;
  if (typeof val === "number") return Number.isFinite(val) ? val : null;

  const cleaned = String(val)
    .replace(/[^\d.]/g, "")
    .trim();
  if (!cleaned) return null;

  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseDateForImport(val) {
  if (!val) return null;
  const d = new Date(val);
  if (isNaN(d)) return null;
  return d.toISOString().split("T")[0];
}

function toArrayValue(raw) {
  if (raw === null || raw === undefined || raw === "") return [];
  if (Array.isArray(raw)) return raw;
  const parsed = safeJSONParse(raw);
  if (Array.isArray(parsed)) return parsed;
  if (typeof raw === "string") {
    return raw
      .split(/[|,;]+/)
      .map((part) => part.trim())
      .filter(Boolean);
  }
  return [];
}

function pickSectionObject(body, ...keys) {
  for (const key of keys) {
    const parsed = safeJSONParse(body?.[key]);
    if (isPlainObject(parsed)) return parsed;
  }
  return {};
}

function normalizeSingleSmartphoneBody(rawBody) {
  const base = isPlainObject(rawBody) ? { ...rawBody } : {};
  const collection = Array.isArray(base.smartphones)
    ? base.smartphones.filter(isPlainObject)
    : [];

  if (collection.length > 1) {
    return {
      body: null,
      error:
        "Send one smartphone object only. Do not post the full smartphones array.",
    };
  }

  if (collection.length === 1) {
    const { smartphones: _smartphones, ...rest } = base;
    return {
      body: {
        ...rest,
        ...collection[0],
      },
      error: null,
    };
  }

  return { body: base, error: null };
}

const LAUNCH_STATUS_VALUES = new Set([
  "rumored",
  "announced",
  "upcoming",
  "released",
  "available",
]);
function normalizeLaunchStatusOverride(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (/(upcoming|coming soon|expected|scheduled)/i.test(raw))
    return "upcoming";
  return LAUNCH_STATUS_VALUES.has(raw) ? raw : null;
}

router.post("/req", authenticate, async (req, res) => {
  const normalizedRequest = normalizeSingleSmartphoneBody(req.body || {});
  if (normalizedRequest.error) {
    return res.status(400).json({ message: normalizedRequest.error });
  }

  const b = normalizedRequest.body || {};
  const product = isPlainObject(b.product) ? b.product : {};
  const basicInfo = pickSectionObject(b, "basic_info_json");

  const product_name = String(
    pickFirstPresent(
      b.product_name,
      b.name,
      product.name,
      basicInfo.product_name,
      basicInfo.model_name,
    ) || "",
  ).trim();
  const brand_name = String(
    pickFirstPresent(
      b.brand_name,
      b.brand,
      product.brand_name,
      product.brand,
      basicInfo.brand_name,
      basicInfo.brand,
    ) || "",
  ).trim();
  const model = String(
    pickFirstPresent(b.model, basicInfo.model, basicInfo.model_number) || "",
  ).trim();

  if (!product_name || !brand_name || !model) {
    return res
      .status(400)
      .json({ message: "name, brand_name and model are required" });
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
      toArrayValue(pickFirstPresent(b.images, b.images_json)) || [];
    const build_design = pickSectionObject(
      b,
      "build_design",
      "build_design_json",
    );
    const display = pickSectionObject(b, "display", "display_json");
    const performance = pickSectionObject(b, "performance", "performance_json");
    const camera = pickSectionObject(b, "camera", "camera_json");
    const battery = pickSectionObject(b, "battery", "battery_json");
    const connectivity = mergeSectionObjects(
      pickSectionObject(b, "connectivity"),
      safeJSONParse(b.connectivity_json),
      safeJSONParse(b.network_connectivity_json),
      safeJSONParse(b.connectivity),
    );
    const network = mergeSectionObjects(
      pickSectionObject(b, "network"),
      safeJSONParse(b.network_json),
      safeJSONParse(b.navigation_json),
      safeJSONParse(b.network),
    );
    const ports = pickSectionObject(b, "ports", "ports_json", "port_json");
    const audio = pickSectionObject(b, "audio", "audio_json");
    const multimedia = pickSectionObject(b, "multimedia", "multimedia_json");
    const sensorsJson = safeJSONParse(b.sensors_json);
    const sensorsInput =
      b.sensors ??
      (Array.isArray(sensorsJson?.sensors)
        ? sensorsJson.sensors
        : sensorsJson || null);
    const sensors = parseSensors(sensorsInput);
    const colors = toArrayValue(
      pickFirstPresent(
        b.colors,
        build_design.colors,
        safeJSONParse(b.colors_json),
      ),
    );
    const launchStatusOverride = normalizeLaunchStatusOverride(
      pickFirstPresent(
        b.launch_status_override,
        b.launchStatusOverride,
        b.launch_status,
        b.launchStatus,
      ),
    );

    const publish = hasOwn(b, "publish")
      ? Boolean(b.publish)
      : hasOwn(b, "published")
        ? Boolean(b.published)
        : hasOwn(b, "is_published")
          ? Boolean(b.is_published)
          : false;

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
         (product_id, category, brand, model, launch_date, launch_status_override, images, colors, build_design, display, performance, camera, battery, connectivity, network, ports, audio, multimedia, sensors)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
      [
        productId,
        b.category || null,
        brand_name,
        model,
        parseDateForImport(b.launch_date),
        launchStatusOverride,
        JSON.stringify(images),
        JSON.stringify(colors),
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
          const basePrice = toNumericPrice(v.base_price ?? v.price);

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
                : v.store || v.store_name || v.storeName || v.price != null
                  ? [
                      {
                        store_name:
                          v.store_name || v.store || v.storeName || "Store",
                        price: v.price ?? v.base_price ?? null,
                        url: v.url ?? null,
                        offer_text: v.offer_text ?? v.offerText ?? null,
                        delivery_info:
                          v.delivery_info ?? v.deliveryInfo ?? null,
                      },
                    ]
                  : [];

          if (variantId && storePrices.length) {
            for (const sp of storePrices) {
              try {
                const storeName =
                  sp.store_name || sp.store || sp.storeName || "Store";
                await client.query(
                  `INSERT INTO variant_store_prices (variant_id, store_name, price, url, offer_text, delivery_info, sale_start_date)
                   VALUES ($1,$2,$3,$4,$5,$6,$7)
                   ON CONFLICT (variant_id, store_name)
                   DO UPDATE SET
                     price = EXCLUDED.price,
                     url = EXCLUDED.url,
                     offer_text = EXCLUDED.offer_text,
                     delivery_info = EXCLUDED.delivery_info,
                     sale_start_date = EXCLUDED.sale_start_date`,
                  [
                    variantId,
                    storeName,
                    toNumericPrice(sp.price),
                    sp.url ?? null,
                    sp.offer_text ?? sp.offerText ?? null,
                    sp.delivery_info ?? sp.deliveryInfo ?? null,
                    parseDateForImport(
                      sp.sale_start_date ??
                        sp.sale_date ??
                        sp.saleStartDate ??
                        null,
                    ),
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

    // publish row upsert (lets GET /api/smartphones include when publish=true)
    await client.query(
      `INSERT INTO product_publish (product_id, is_published, published_by)
       VALUES ($1,$2,$3)
       ON CONFLICT (product_id)
       DO UPDATE SET
         is_published = EXCLUDED.is_published,
         published_by = COALESCE(EXCLUDED.published_by, product_publish.published_by),
         updated_at = CURRENT_TIMESTAMP`,
      [productId, publish, req.user?.id || null],
    );

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
