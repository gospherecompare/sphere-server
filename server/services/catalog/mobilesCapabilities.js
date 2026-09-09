"use strict";

const {
  normalizeCompareScoreConfig,
  buildCompareRanking,
} = require("../../utils/compareScoring");
const {
  buildDecisionComparison,
} = require("../../utils/compareDecisionEngine");

const PRODUCT_TYPES = Object.freeze({ smartphone: "smartphone", tv: "tv" });

const numberOrNull = (value) => {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
};

const normalizeQuery = (value) =>
  String(value || "")
    .trim()
    .toLowerCase();

const createMobilesCapabilities = ({ db, compareConfig = {} } = {}) => {
  if (!db || typeof db.query !== "function")
    throw new Error("db.query is required");

  const listProducts = async ({
    domain,
    filters = {},
    limit = 20,
    recommend = false,
    sort = "default",
    query = null,
  }) => {
    const productType = PRODUCT_TYPES[domain];
    if (!productType) throw new Error(`Unsupported catalog domain: ${domain}`);

    const params = [productType];
    const where = ["p.product_type = $1", "pub.is_published = true"];
    const addParam = (value) => {
      params.push(value);
      return `$${params.length}`;
    };

    if (filters.brand) {
      where.push(`LOWER(b.name) = LOWER(${addParam(filters.brand)})`);
    }
    if (query) {
      const queryParam = addParam(`%${String(query).trim()}%`);
      where.push(`(
        LOWER(p.name) LIKE LOWER(${queryParam})
        OR LOWER(COALESCE(b.name, '')) LIKE LOWER(${queryParam})
        OR LOWER(COALESCE(${domain === "smartphone" ? "s.model" : "t.model"}, '')) LIKE LOWER(${queryParam})
      )`);
    }
    if (numberOrNull(filters.maxPrice) !== null) {
      where.push(`EXISTS (
        SELECT 1 FROM product_variants filter_v
        LEFT JOIN variant_store_prices filter_sp ON filter_sp.variant_id = filter_v.id
        WHERE filter_v.product_id = p.id
          AND COALESCE(filter_sp.price, filter_v.base_price) <= ${addParam(numberOrNull(filters.maxPrice))}
      )`);
    }
    if (numberOrNull(filters.minPrice) !== null) {
      where.push(`EXISTS (
        SELECT 1 FROM product_variants filter_v
        LEFT JOIN variant_store_prices filter_sp ON filter_sp.variant_id = filter_v.id
        WHERE filter_v.product_id = p.id
          AND COALESCE(filter_sp.price, filter_v.base_price) >= ${addParam(numberOrNull(filters.minPrice))}
      )`);
    }
    if (filters.ram) {
      where.push(`EXISTS (
        SELECT 1 FROM product_variants filter_v
        WHERE filter_v.product_id = p.id
          AND LOWER(COALESCE(filter_v.attributes->>'ram', filter_v.attributes->>'RAM', filter_v.attributes->>'memory', '')) = LOWER(${addParam(`${filters.ram} GB`)})
      )`);
    }
    if (filters.storage) {
      const storageGb = Number.parseFloat(
        String(filters.storage).replace(/[^0-9.]/g, ""),
      );
      where.push(`EXISTS (
        SELECT 1 FROM product_variants filter_v
        WHERE filter_v.product_id = p.id
          AND (
            LOWER(COALESCE(filter_v.attributes->>'storage', filter_v.attributes->>'rom', filter_v.attributes->>'internal_storage', '')) LIKE LOWER(${addParam(`%${filters.storage}%`)})
            ${Number.isFinite(storageGb) ? `OR LOWER(COALESCE(filter_v.attributes->>'storage', filter_v.attributes->>'rom', filter_v.attributes->>'internal_storage', '')) LIKE LOWER(${addParam(`${storageGb}`)})` : ""}
          )
      )`);
    }
    if (domain === "smartphone" && filters.color) {
      where.push(`s.colors::text ILIKE ${addParam(`%${filters.color}%`)}`);
    }
    if (domain === "smartphone" && filters.processor) {
      where.push(
        `s.performance::text ILIKE ${addParam(`%${filters.processor}%`)}`,
      );
    }
    if (domain === "smartphone" && filters.network) {
      where.push(`s.network::text ILIKE ${addParam(`%${filters.network}%`)}`);
    }
    if (domain === "smartphone" && filters.display) {
      where.push(`s.display::text ILIKE ${addParam(`%${filters.display}%`)}`);
    }
    if (domain === "smartphone" && filters.camera) {
      where.push(`s.camera::text ILIKE ${addParam(`%${filters.camera}%`)}`);
    }
    if (domain === "smartphone" && filters.battery) {
      where.push(`s.battery::text ILIKE ${addParam(`%${filters.battery}%`)}`);
    }
    if (domain === "smartphone" && filters.resolution) {
      where.push(
        `s.display::text ILIKE ${addParam(`%${filters.resolution}%`)}`,
      );
    }
    if (domain === "smartphone" && filters.refreshRate) {
      where.push(
        `s.display::text ILIKE ${addParam(`%${filters.refreshRate}%`)}`,
      );
    }
    if (
      domain === "smartphone" &&
      (filters.additionalFeatures || filters.feature)
    ) {
      where.push(
        `(s.connectivity::text || ' ' || s.network::text || ' ' || s.ports::text || ' ' || s.audio::text || ' ' || s.multimedia::text || ' ' || s.sensors::text || ' ' || s.display::text || ' ' || s.camera::text) ILIKE ${addParam(`%${filters.additionalFeatures || filters.feature}%`)}`,
      );
    }
    if (domain === "tv" && filters.screenSize) {
      where.push(`EXISTS (
        SELECT 1 FROM product_variants filter_v
        WHERE filter_v.product_id = p.id
          AND regexp_replace(COALESCE(filter_v.attributes->>'screen_size', filter_v.attributes->>'size', filter_v.variant_key, ''), '[^0-9.]', '', 'g') = ${addParam(String(filters.screenSize))}
      )`);
    }
    if (domain === "tv" && filters.resolution)
      where.push(
        `(t.display_json::text || ' ' || t.video_engine_json::text || ' ' || t.product_details_json::text) ILIKE ${addParam(`%${filters.resolution}%`)}`,
      );
    if (domain === "tv" && filters.refreshRate)
      where.push(
        `(t.display_json::text || ' ' || t.video_engine_json::text) ILIKE ${addParam(`%${filters.refreshRate}%`)}`,
      );
    if (domain === "tv" && filters.display)
      where.push(
        `t.display_json::text ILIKE ${addParam(`%${filters.display}%`)}`,
      );
    if (domain === "tv" && filters.additionalFeatures)
      where.push(
        `(t.display_json::text || ' ' || t.video_engine_json::text || ' ' || t.audio_json::text || ' ' || t.smart_tv_json::text || ' ' || t.gaming_json::text || ' ' || t.ports_json::text || ' ' || t.connectivity_json::text) ILIKE ${addParam(`%${filters.additionalFeatures}%`)}`,
      );

    const safeLimit = Math.min(50, Math.max(1, Number(limit) || 20));
    params.push(safeLimit);
    const limitParam = `$${params.length}`;
    const order =
      sort === "trending"
        ? "COALESCE(ts.manual_priority, 0) DESC, COALESCE(ts.manual_boost::int, 0) DESC, COALESCE(ts.trending_score, 0) DESC, COALESCE(ds.trend_velocity, 0) DESC, p.id DESC"
        : sort === "latest"
          ? domain === "smartphone"
            ? "s.launch_date DESC NULLS LAST, p.id DESC"
            : "p.id DESC"
          : recommend
            ? "COALESCE(ds.hook_score, 0) DESC, COALESCE(ds.buyer_intent, 0) DESC, COALESCE(ds.trend_velocity, 0) DESC, COALESCE(ds.freshness, 0) DESC, p.id DESC"
            : "p.id DESC";

    const result = await db.query(
      `SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        COALESCE(ds.hook_score, 0) AS hook_score,
        COALESCE(ds.buyer_intent, 0) AS buyer_intent,
        COALESCE(ds.trend_velocity, 0) AS trend_velocity,
        COALESCE(ds.freshness, 0) AS freshness,
        COALESCE(ts.trending_score, 0) AS trending_score,
        COALESCE((SELECT MIN(sp.price) FROM product_variants v LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id WHERE v.product_id = p.id), (SELECT MIN(v.base_price) FROM product_variants v WHERE v.product_id = p.id)) AS price,
        COALESCE((SELECT json_agg(jsonb_build_object('id', v.id, 'variant_key', v.variant_key, 'attributes', v.attributes, 'base_price', v.base_price) ORDER BY v.id) FROM product_variants v WHERE v.product_id = p.id), '[]'::json) AS variants,
        ${productType === "smartphone" ? "s.model, s.display, s.performance, s.camera, s.battery," : "t.model, t.display_json AS display, t.video_engine_json AS performance, '{}'::jsonb AS camera, t.power_json AS battery,"}
        COALESCE((SELECT json_agg(pi.image_url ORDER BY pi.position ASC NULLS LAST, pi.id ASC) FROM product_images pi WHERE pi.product_id = p.id), '[]'::json) AS images
      FROM products p
      INNER JOIN product_publish pub ON pub.product_id = p.id
      LEFT JOIN brands b ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds ON ds.product_id = p.id
      LEFT JOIN product_trending_score ts ON ts.product_id = p.id
      ${productType === "smartphone" ? "LEFT JOIN smartphones s ON s.product_id = p.id" : "LEFT JOIN tvs t ON t.product_id = p.id"}
      WHERE ${where.join(" AND ")}
      ORDER BY ${order}
      LIMIT ${limitParam}`,
      params,
    );
    return result.rows || [];
  };

  const recommendProducts = (input = {}) =>
    listProducts({ ...input, recommend: true, sort: "default" });

  const searchProducts = (input = {}) =>
    listProducts({ ...input, query: input.query || input.message });

  const getTrendingProducts = (input = {}) =>
    listProducts({ ...input, recommend: false, sort: "trending" });

  const getLatestProducts = (input = {}) =>
    listProducts({ ...input, recommend: false, sort: "latest" });

  const resolveProducts = async ({ names = [], domain }) => {
    const productType = PRODUCT_TYPES[domain];
    if (!productType || !names.length) return [];
    const result = await db.query(
      `SELECT p.id AS product_id, p.name, p.product_type, b.name AS brand_name
       FROM products p
       INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE p.product_type = $1
         AND EXISTS (
           SELECT 1 FROM unnest($2::text[]) requested
           WHERE LOWER(p.name) LIKE '%' || LOWER(requested) || '%'
              OR LOWER(COALESCE(b.name, '')) LIKE '%' || LOWER(requested) || '%'
         )
       ORDER BY p.id DESC`,
      [productType, names.map(normalizeQuery).filter(Boolean)],
    );
    return result.rows || [];
  };

  const getProduct = async (id) => {
    const result = await db.query(
      `SELECT p.id AS product_id, p.name, p.product_type, b.name AS brand_name,
              s.*, t.display_json AS tv_display, t.key_specs_json, t.smart_tv_json,
              t.gaming_json, t.audio_json, t.ports_json, t.connectivity_json,
              COALESCE((SELECT MIN(sp.price) FROM product_variants v LEFT JOIN variant_store_prices sp ON sp.variant_id = v.id WHERE v.product_id = p.id), (SELECT MIN(v.base_price) FROM product_variants v WHERE v.product_id = p.id)) AS price,
              COALESCE((SELECT json_agg(jsonb_build_object('id', v.id, 'variant_key', v.variant_key, 'attributes', v.attributes, 'base_price', v.base_price) ORDER BY v.id) FROM product_variants v WHERE v.product_id = p.id), '[]'::json) AS variants
       FROM products p
       INNER JOIN product_publish pub ON pub.product_id = p.id AND pub.is_published = true
       LEFT JOIN brands b ON b.id = p.brand_id
       LEFT JOIN smartphones s ON s.product_id = p.id
       LEFT JOIN tvs t ON t.product_id = p.id
       WHERE p.id = $1 LIMIT 1`,
      [id],
    );
    return result.rows[0] || null;
  };

  const getSmartphoneSpecScore = async ({ id, names = [] } = {}) => {
    const resolved = Number.isInteger(Number(id))
      ? { product_id: Number(id) }
      : (await resolveProducts({ names, domain: "smartphone" }))[0];
    if (!resolved?.product_id) return null;
    return getProduct(resolved.product_id);
  };

  const compareProducts = async (ids) => {
    const products = (
      await Promise.all(ids.map((id) => getProduct(id)))
    ).filter(Boolean);
    if (products.length < 2) return null;
    const productTypes = new Set(
      products.map((product) => product.product_type),
    );
    if (productTypes.size !== 1)
      throw new Error("Products must have the same type");
    const selections = Object.fromEntries(
      products.map((product) => [
        String(product.product_id),
        { product_id: product.product_id },
      ]),
    );
    const ranking = buildCompareRanking(
      products,
      selections,
      normalizeCompareScoreConfig(compareConfig),
    );
    return buildDecisionComparison({ devices: products, selections, ranking });
  };

  const searchNews = async ({
    brand,
    topic,
    product,
    query,
    date,
    limit = 20,
  }) => {
    const params = [];
    const where = ["bl.is_published = true"];
    const addParam = (value) => {
      params.push(value);
      return `$${params.length}`;
    };
    if (topic)
      where.push(
        `LOWER(BTRIM(bl.category)) LIKE '%' || LOWER(${addParam(topic)}) || '%'`,
      );
    if (date === "today") where.push("bl.published_at::date = CURRENT_DATE");
    if (date === "yesterday")
      where.push("bl.published_at::date = CURRENT_DATE - INTERVAL '1 day'");
    if (date === "this_week")
      where.push("bl.published_at >= date_trunc('week', CURRENT_DATE)");
    if (brand) {
      const brandParam = addParam(brand);
      where.push(`(
        LOWER(COALESCE(bl.brand_name, '')) LIKE '%' || LOWER(${brandParam}) || '%'
        OR EXISTS (
          SELECT 1 FROM products bp
          LEFT JOIN brands bb ON bb.id = bp.brand_id
          WHERE bp.id = bl.product_id
            AND LOWER(COALESCE(bb.name, '')) LIKE '%' || LOWER(${brandParam}) || '%'
        )
      )`);
    }
    if (product) {
      const productParam = addParam(product);
      where.push(`(
        EXISTS (
          SELECT 1 FROM products np
          WHERE np.id = bl.product_id
            AND LOWER(np.name) LIKE '%' || LOWER(${productParam}) || '%'
        )
      )`);
    }
    if (query)
      where.push(
        `(LOWER(bl.title) LIKE LOWER(${addParam(`%${query}%`)} ) OR LOWER(COALESCE(bl.excerpt, '')) LIKE LOWER(${params[params.length - 1]}))`,
      );
    params.push(Math.min(50, Math.max(1, Number(limit) || 20)));
    const result = await db.query(
      `SELECT bl.id, bl.slug, bl.title, bl.excerpt, bl.category, bl.hero_image, bl.published_at,
              COALESCE(bl.brand_name, b.name) AS brand_name, p.name AS product_name
       FROM blogs bl
       LEFT JOIN products p ON p.id = bl.product_id
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE ${where.join(" AND ")}
       ORDER BY bl.published_at DESC NULLS LAST, bl.updated_at DESC
       LIMIT $${params.length}`,
      params,
    );
    return result.rows || [];
  };

  const getArticle = async ({
    id = null,
    slug = null,
    currentResults = [],
    message = "",
  } = {}) => {
    const contextArticle = Array.isArray(currentResults)
      ? currentResults.find((item) => item?.slug || item?.id)
      : null;
    const slugHint = slug || contextArticle?.slug || null;
    const idHint = Number.isInteger(Number(id))
      ? Number(id)
      : Number(contextArticle?.id);
    const params = [];
    let where = "bl.is_published = true";
    if (slugHint) {
      params.push(String(slugHint));
      where += ` AND bl.slug = $${params.length}`;
    } else if (Number.isInteger(idHint) && idHint > 0) {
      params.push(idHint);
      where += ` AND bl.id = $${params.length}`;
    } else {
      const candidate = String(message || "")
        .match(/(?:article|story|news)\s+(?:about|on|for)\s+(.+)$/i)?.[1]
        ?.replace(/[?.!,]+$/g, "")
        .trim();
      if (!candidate) return null;
      params.push(`%${candidate}%`);
      where += ` AND (LOWER(bl.title) LIKE LOWER($${params.length}) OR LOWER(bl.slug) LIKE LOWER($${params.length}))`;
    }
    const result = await db.query(
      `SELECT bl.id, bl.slug, bl.title, bl.excerpt, bl.content_rendered AS content, bl.category,
              bl.hero_image, bl.published_at, COALESCE(bl.brand_name, b.name) AS brand_name,
              p.name AS product_name
       FROM blogs bl
       LEFT JOIN products p ON p.id = bl.product_id
       LEFT JOIN brands b ON b.id = p.brand_id
       WHERE ${where}
       ORDER BY bl.published_at DESC NULLS LAST, bl.updated_at DESC
       LIMIT 1`,
      params,
    );
    return result.rows[0] || null;
  };

  return {
    listProducts,
    searchProducts,
    recommendProducts,
    getTrendingProducts,
    getLatestProducts,
    getArticle,
    getSmartphoneSpecScore,
    resolveProducts,
    getProduct,
    compareProducts,
    searchNews,
  };
};

module.exports = { createMobilesCapabilities };
