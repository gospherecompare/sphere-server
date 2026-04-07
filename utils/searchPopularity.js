"use strict";

const DEFAULT_DAYS = 30;
const DEFAULT_LIMIT = 5;

const PRODUCT_TYPE_ALIASES = new Map([
  ["", null],
  ["all", null],
  ["smartphone", "smartphone"],
  ["smartphones", "smartphone"],
  ["mobile", "smartphone"],
  ["mobiles", "smartphone"],
  ["phone", "smartphone"],
  ["phones", "smartphone"],
  ["laptop", "laptop"],
  ["laptops", "laptop"],
  ["tv", "tv"],
  ["tvs", "tv"],
  ["television", "tv"],
  ["televisions", "tv"],
  ["appliance", "tv"],
  ["appliances", "tv"],
  ["networking", "networking"],
  ["network", "networking"],
  ["router", "networking"],
  ["routers", "networking"],
]);

const clampInt = (value, fallback, min, max) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(parsed)));
};

const toNumber = (value, fallback = 0) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const cleanText = (value, maxLength = 160) =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);

const cleanToken = (value, maxLength = 64) =>
  String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, maxLength);

const toProductSlug = (name, id) => {
  const slug = String(name || "")
    .toLowerCase()
    .replace(/\bprice[-\s]+in[-\s]+india\b/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");

  return slug || `product-${id}`;
};

const normalizeProductType = (value) => {
  const raw = cleanToken(value, 32);
  if (!PRODUCT_TYPE_ALIASES.has(raw)) return undefined;
  return PRODUCT_TYPE_ALIASES.get(raw);
};

const normalizeSearchQuery = (value) => {
  const text = cleanText(value, 180).toLowerCase();
  if (!text) return "";

  return text
    .replace(/\bprice\s+in\s+india\b/g, "")
    .replace(/-price-in-india$/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/(^-|-$)/g, "");
};

const getCategoryPath = (productType) => {
  switch (productType) {
    case "laptop":
      return "/laptops";
    case "tv":
      return "/tvs";
    case "networking":
      return "/networking";
    case "smartphone":
    default:
      return "/smartphones";
  }
};

const buildProductDetailPath = (productType, name, id) => {
  const slug = toProductSlug(name, id);
  const basePath = getCategoryPath(productType);

  if (productType === "smartphone") {
    return `${basePath}/${slug}-price-in-india?id=${id}`;
  }

  return `${basePath}/${slug}?id=${id}`;
};

const badgeForScore = ({
  score,
  searchCount,
  viewCount,
  compareCount,
  dwellSeconds,
}) => {
  if (searchCount >= 5 && score >= 75) return "Most Searched";
  if (compareCount >= 3 && score >= 65) return "Highly Compared";
  if (dwellSeconds >= 90 && score >= 60) return "High Interest";
  if (viewCount >= 5 || score >= 50) return "Popular";
  return "Rising";
};

async function resolveSearchInterestProduct(db, options = {}) {
  if (!db || typeof db.query !== "function") {
    throw new Error("resolveSearchInterestProduct: db.query required");
  }

  const requestedType = normalizeProductType(options.productType);
  const providedProductId = Number(options.productId);
  const normalizedQuery = normalizeSearchQuery(options.query);

  if (Number.isInteger(providedProductId) && providedProductId > 0) {
    const byId = await db.query(
      `
      SELECT p.id AS product_id, p.product_type
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      WHERE p.id = $1
      LIMIT 1
      `,
      [providedProductId],
    );

    if (byId.rows?.length) {
      return {
        product_id: Number(byId.rows[0].product_id),
        product_type: byId.rows[0].product_type || requestedType || null,
      };
    }
  }

  if (!normalizedQuery) return null;

  const params = [normalizedQuery];
  let typeWhere = "";
  if (requestedType) {
    params.push(requestedType);
    typeWhere = `AND p.product_type = $${params.length}`;
  }

  const result = await db.query(
    `
    SELECT p.id AS product_id, p.product_type
    FROM products p
    INNER JOIN product_publish pub
      ON pub.product_id = p.id
     AND pub.is_published = true
    LEFT JOIN smartphones s
      ON s.product_id = p.id
    LEFT JOIN laptop l
      ON l.product_id = p.id
    LEFT JOIN tvs t
      ON t.product_id = p.id
    LEFT JOIN networking n
      ON n.product_id = p.id
    WHERE (
      regexp_replace(lower(COALESCE(p.name, '')), '[^a-z0-9]+', '-', 'g') = $1
      OR regexp_replace(
        lower(COALESCE(s.model, l.model, t.model, n.model_number, '')),
        '[^a-z0-9]+',
        '-',
        'g'
      ) = $1
    )
    ${typeWhere}
    ORDER BY
      CASE
        WHEN regexp_replace(
          lower(COALESCE(s.model, l.model, t.model, n.model_number, '')),
          '[^a-z0-9]+',
          '-',
          'g'
        ) = $1
          THEN 0
        ELSE 1
      END,
      p.id DESC
    LIMIT 1
    `,
    params,
  );

  if (!result.rows?.length) return null;

  return {
    product_id: Number(result.rows[0].product_id),
    product_type: result.rows[0].product_type || requestedType || null,
  };
}

async function getSearchPopularityDevices(db, options = {}) {
  if (!db || typeof db.query !== "function") {
    throw new Error("getSearchPopularityDevices: db.query required");
  }

  const normalizedType = normalizeProductType(options.productType);
  if (normalizedType === undefined) {
    throw new Error("Invalid productType");
  }

  const days = clampInt(
    options.days,
    DEFAULT_DAYS,
    1,
    90,
  );
  const limit = clampInt(
    options.limit,
    DEFAULT_LIMIT,
    1,
    100,
  );

  const result = await db.query(
    `
    WITH search_stats AS (
      SELECT
        se.product_id,
        COUNT(*)::int AS search_count_30d,
        MAX(se.created_at) AS last_search_at
      FROM search_interest_events se
      WHERE se.product_id IS NOT NULL
        AND se.created_at >= now() - ($1::int * interval '1 day')
      GROUP BY se.product_id
    ),
    view_stats AS (
      SELECT
        pv.product_id,
        COUNT(DISTINCT COALESCE(pv.visitor_key, pv.id::text))::int AS views_30d,
        MAX(pv.viewed_at) AS last_view_at
      FROM product_views pv
      WHERE pv.viewed_at >= now() - ($1::int * interval '1 day')
      GROUP BY pv.product_id
    ),
    compare_stats AS (
      SELECT
        c.product_id,
        COUNT(*)::int AS compare_count_30d,
        MAX(c.compared_at) AS last_compare_at
      FROM (
        SELECT product_id, compared_at
        FROM product_comparisons
        WHERE compared_at >= now() - ($1::int * interval '1 day')
        UNION ALL
        SELECT compared_with AS product_id, compared_at
        FROM product_comparisons
        WHERE compared_at >= now() - ($1::int * interval '1 day')
      ) c
      GROUP BY c.product_id
    ),
    engagement_stats AS (
      SELECT
        pe.product_id,
        COUNT(*)::int AS engagement_count_30d,
        ROUND(AVG(pe.duration_ms)::numeric / 1000, 1) AS avg_dwell_seconds,
        MAX(pe.created_at) AS last_engagement_at
      FROM page_engagement_events pe
      WHERE pe.created_at >= now() - ($1::int * interval '1 day')
      GROUP BY pe.product_id
    ),
    base AS (
      SELECT
        p.id AS product_id,
        p.name,
        p.product_type,
        b.name AS brand_name,
        img.image_url,
        COALESCE(ss.search_count_30d, 0) AS search_count_30d,
        COALESCE(vs.views_30d, 0) AS views_30d,
        COALESCE(cs.compare_count_30d, 0) AS compare_count_30d,
        COALESCE(es.engagement_count_30d, 0) AS engagement_count_30d,
        COALESCE(es.avg_dwell_seconds, 0) AS avg_dwell_seconds,
        LEAST(
          100::numeric,
          GREATEST(
            0::numeric,
            COALESCE(
              ds.freshness,
              CASE
                WHEN p.created_at >= now() - INTERVAL '30 days' THEN 100
                WHEN p.created_at >= now() - INTERVAL '90 days' THEN 80
                WHEN p.created_at >= now() - INTERVAL '180 days' THEN 60
                WHEN p.created_at >= now() - INTERVAL '365 days' THEN 40
                ELSE 20
              END
            )::numeric
          )
        ) AS freshness_score,
        ss.last_search_at,
        vs.last_view_at,
        cs.last_compare_at,
        es.last_engagement_at
      FROM products p
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      LEFT JOIN LATERAL (
        SELECT image_url
        FROM product_images
        WHERE product_id = p.id
        ORDER BY position ASC NULLS LAST, id ASC
        LIMIT 1
      ) img ON true
      LEFT JOIN search_stats ss
        ON ss.product_id = p.id
      LEFT JOIN view_stats vs
        ON vs.product_id = p.id
      LEFT JOIN compare_stats cs
        ON cs.product_id = p.id
      LEFT JOIN engagement_stats es
        ON es.product_id = p.id
      WHERE ($2::text IS NULL OR p.product_type = $2)
    ),
    filtered AS (
      SELECT *
      FROM base
      WHERE search_count_30d > 0
         OR views_30d > 0
         OR compare_count_30d > 0
         OR engagement_count_30d > 0
    ),
    maxed AS (
      SELECT
        *,
        MAX(search_count_30d) OVER () AS max_search_count_30d,
        MAX(views_30d) OVER () AS max_views_30d,
        MAX(compare_count_30d) OVER () AS max_compare_count_30d,
        MAX(avg_dwell_seconds) OVER () AS max_avg_dwell_seconds
      FROM filtered
    ),
    weighted AS (
      SELECT
        *,
        CASE
          WHEN max_search_count_30d > 0
            THEN (search_count_30d::numeric / max_search_count_30d::numeric) * 100
          ELSE 0
        END AS search_weight,
        CASE
          WHEN max_views_30d > 0
            THEN (views_30d::numeric / max_views_30d::numeric) * 100
          ELSE 0
        END AS view_weight,
        CASE
          WHEN max_compare_count_30d > 0
            THEN (compare_count_30d::numeric / max_compare_count_30d::numeric) * 100
          ELSE 0
        END AS compare_weight,
        CASE
          WHEN max_avg_dwell_seconds > 0
            THEN (avg_dwell_seconds::numeric / max_avg_dwell_seconds::numeric) * 100
          ELSE 0
        END AS dwell_weight
      FROM maxed
    )
    SELECT
      product_id,
      name,
      product_type,
      brand_name,
      image_url,
      search_count_30d,
      views_30d,
      compare_count_30d,
      engagement_count_30d,
      avg_dwell_seconds,
      freshness_score,
      search_weight,
      view_weight,
      compare_weight,
      dwell_weight,
      ROUND(
        (search_weight * 0.40) +
        (view_weight * 0.25) +
        (compare_weight * 0.20) +
        (dwell_weight * 0.10) +
        (freshness_score * 0.05),
        2
      ) AS search_popularity_score,
      last_search_at,
      last_view_at,
      last_compare_at,
      last_engagement_at
    FROM weighted
    ORDER BY
      search_popularity_score DESC,
      search_count_30d DESC,
      views_30d DESC,
      compare_count_30d DESC,
      avg_dwell_seconds DESC,
      product_id DESC
    LIMIT $3
    `,
    [days, normalizedType, limit],
  );

  const devices = (result.rows || []).map((row, index) => {
    const productId = toNumber(row.product_id, null);
    const score = toNumber(row.search_popularity_score, 0);
    const searchCount = toNumber(row.search_count_30d, 0);
    const viewCount = toNumber(row.views_30d, 0);
    const compareCount = toNumber(row.compare_count_30d, 0);
    const dwellSeconds = toNumber(row.avg_dwell_seconds, 0);

    return {
      id: productId,
      product_id: productId,
      name: row.name || "Device",
      product_type: row.product_type || null,
      brand_name: row.brand_name || null,
      image_url: row.image_url || null,
      detail_path: buildProductDetailPath(row.product_type, row.name, productId),
      hero_rank: index + 1,
      search_count_30d: searchCount,
      views_30d: viewCount,
      click_count_30d: viewCount,
      compare_count_30d: compareCount,
      engagement_count_30d: toNumber(row.engagement_count_30d, 0),
      avg_dwell_seconds: dwellSeconds,
      freshness_score: toNumber(row.freshness_score, 0),
      search_weight: toNumber(row.search_weight, 0),
      view_weight: toNumber(row.view_weight, 0),
      compare_weight: toNumber(row.compare_weight, 0),
      dwell_weight: toNumber(row.dwell_weight, 0),
      search_popularity_score: score,
      badge: badgeForScore({
        score,
        searchCount,
        viewCount,
        compareCount,
        dwellSeconds,
      }),
      last_search_at: row.last_search_at || null,
      last_view_at: row.last_view_at || null,
      last_compare_at: row.last_compare_at || null,
      last_engagement_at: row.last_engagement_at || null,
    };
  });

  return {
    productType: normalizedType,
    days,
    limit,
    devices,
  };
}

module.exports = {
  cleanText,
  cleanToken,
  getSearchPopularityDevices,
  normalizeProductType,
  normalizeSearchQuery,
  resolveSearchInterestProduct,
};
