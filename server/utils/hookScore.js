"use strict";

const DEFAULT_DAYS = 7;
const DEFAULT_SMOOTHING = 5;
const DEFAULT_FRESHNESS_HALF_LIFE_DAYS = 365;

const DEFAULT_BUYER_INTENT_WEIGHTS = {
  views: 0.55,
  compares: 0.3,
  wishlist: 0.15,
};

const DEFAULT_TREND_VELOCITY_WEIGHTS = {
  views: 0.7,
  compares: 0.3,
};

const DEFAULT_HOOK_SCORE_WEIGHTS = {
  buyer_intent: 0.5,
  trend_velocity: 0.3,
  freshness: 0.2,
};

const PRODUCT_TYPE_CONFIG = {
  smartphone: {
    joinClause: "INNER JOIN smartphones s ON s.product_id = p.id",
    launchAtSql: "COALESCE(s.launch_date, s.created_at, p.created_at)",
    lockOffset: 1,
  },
  laptop: {
    joinClause: "INNER JOIN laptop l ON l.product_id = p.id",
    launchAtSql: `
      COALESCE(
        CASE
          WHEN NULLIF(TRIM(l.meta->>'launch_date'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN (l.meta->>'launch_date')::timestamptz
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(l.spec_sections#>>'{basic_info_json,launch_date}'), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN (l.spec_sections#>>'{basic_info_json,launch_date}')::timestamptz
          ELSE NULL
        END,
        l.created_at,
        p.created_at
      )
    `,
    lockOffset: 2,
  },
  tv: {
    joinClause: "INNER JOIN tvs t ON t.product_id = p.id",
    launchAtSql: `
      COALESCE(
        CASE
          WHEN NULLIF(TRIM(t.product_details_json->>'launch_year'), '') ~ '^[0-9]{4}$'
            THEN make_timestamp(
              (t.product_details_json->>'launch_year')::int,
              1, 1, 0, 0, 0
            )::timestamptz
          ELSE NULL
        END,
        CASE
          WHEN NULLIF(TRIM(t.basic_info_json->>'launch_year'), '') ~ '^[0-9]{4}$'
            THEN make_timestamp(
              (t.basic_info_json->>'launch_year')::int,
              1, 1, 0, 0, 0
            )::timestamptz
          ELSE NULL
        END,
        t.created_at,
        p.created_at
      )
    `,
    lockOffset: 3,
  },
};

const toPositiveInteger = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
};

const toNonNegativeNumber = (value, fallback) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return parsed;
};

const normalizeWeights = (weights, defaults) => {
  const merged = { ...defaults, ...(weights || {}) };
  const normalized = {};
  let total = 0;

  for (const [key, defaultValue] of Object.entries(defaults)) {
    const safe = toNonNegativeNumber(merged[key], defaultValue);
    normalized[key] = safe;
    total += safe;
  }

  if (total <= 0) {
    const fallbackTotal = Object.values(defaults).reduce((sum, n) => sum + n, 0);
    return { ...defaults, total: fallbackTotal || 1 };
  }

  return { ...normalized, total };
};

const getHookLockKey = (productType) => {
  const baseLockRaw = Number(process.env.HOOK_SCORE_LOCK_KEY ?? 84616031);
  const baseLock =
    Number.isFinite(baseLockRaw) && Math.abs(baseLockRaw) <= 2147483600
      ? Math.trunc(baseLockRaw)
      : 84616031;

  const offset = PRODUCT_TYPE_CONFIG[productType]?.lockOffset || 0;
  const lockKey = baseLock + offset;

  if (Math.abs(lockKey) > 2147483647) {
    return baseLock;
  }

  return lockKey;
};

async function recomputeProductDynamicScoreByType(db, productType, opts = {}) {
  if (!db || typeof db.connect !== "function") {
    throw new Error("recomputeProductDynamicScoreByType: db.connect required");
  }

  const config = PRODUCT_TYPE_CONFIG[productType];
  if (!config) {
    throw new Error(
      `recomputeProductDynamicScoreByType: unsupported product type "${productType}"`,
    );
  }

  const days = toPositiveInteger(
    opts.days ?? process.env.HOOK_SCORE_DAYS,
    DEFAULT_DAYS,
  );
  const lookbackDays = days * 2;
  const smoothing = toNonNegativeNumber(
    opts.smoothing ?? process.env.HOOK_SCORE_SMOOTHING,
    DEFAULT_SMOOTHING,
  );
  const freshnessHalfLifeDays = toPositiveInteger(
    opts.freshnessHalfLifeDays ??
      process.env.HOOK_SCORE_FRESHNESS_HALF_LIFE_DAYS,
    DEFAULT_FRESHNESS_HALF_LIFE_DAYS,
  );

  const buyerIntentWeights = normalizeWeights(
    opts.buyerIntentWeights,
    DEFAULT_BUYER_INTENT_WEIGHTS,
  );
  const trendVelocityWeights = normalizeWeights(
    opts.trendVelocityWeights,
    DEFAULT_TREND_VELOCITY_WEIGHTS,
  );
  const hookScoreWeights = normalizeWeights(
    opts.hookScoreWeights,
    DEFAULT_HOOK_SCORE_WEIGHTS,
  );

  const lockKey = getHookLockKey(productType);

  const client = await db.connect();
  let locked = false;

  try {
    const lockRes = await client.query(
      "SELECT pg_try_advisory_lock($1) AS locked",
      [lockKey],
    );
    locked = Boolean(lockRes.rows?.[0]?.locked);

    if (!locked) {
      return {
        ok: true,
        skipped: true,
        reason: "lock_unavailable",
        product_type: productType,
        updated: 0,
      };
    }

    const sql = `
      WITH candidate_products AS (
        SELECT
          p.id AS product_id,
          ${config.launchAtSql} AS launch_at
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        ${config.joinClause}
        WHERE p.product_type = $1
      ),
      views AS (
        SELECT
          v.product_id,
          COUNT(DISTINCT COALESCE(v.visitor_key, v.id::text)) FILTER (
            WHERE v.viewed_at >= now() - ($2::int * interval '1 day')
          )::int AS views_recent,
          COUNT(DISTINCT COALESCE(v.visitor_key, v.id::text)) FILTER (
            WHERE v.viewed_at >= now() - ($3::int * interval '1 day')
              AND v.viewed_at <  now() - ($2::int * interval '1 day')
          )::int AS views_previous
        FROM product_views v
        INNER JOIN candidate_products cp
          ON cp.product_id = v.product_id
        WHERE v.viewed_at >= now() - ($3::int * interval '1 day')
        GROUP BY v.product_id
      ),
      compares_raw AS (
        SELECT pc.product_id, pc.compared_at
        FROM product_comparisons pc
        UNION ALL
        SELECT pc.compared_with AS product_id, pc.compared_at
        FROM product_comparisons pc
      ),
      compares AS (
        SELECT
          cr.product_id,
          COUNT(*) FILTER (
            WHERE cr.compared_at >= now() - ($2::int * interval '1 day')
          )::int AS compares_recent,
          COUNT(*) FILTER (
            WHERE cr.compared_at >= now() - ($3::int * interval '1 day')
              AND cr.compared_at <  now() - ($2::int * interval '1 day')
          )::int AS compares_previous
        FROM compares_raw cr
        INNER JOIN candidate_products cp
          ON cp.product_id = cr.product_id
        WHERE cr.compared_at >= now() - ($3::int * interval '1 day')
        GROUP BY cr.product_id
      ),
      wishlists AS (
        SELECT
          w.product_id,
          COUNT(*) FILTER (
            WHERE w.created_at >= now() - ($2::int * interval '1 day')
          )::int AS wishlist_recent
        FROM wishlist w
        INNER JOIN candidate_products cp
          ON cp.product_id = w.product_id
        WHERE w.created_at >= now() - ($2::int * interval '1 day')
        GROUP BY w.product_id
      ),
      base AS (
        SELECT
          cp.product_id,
          GREATEST(
            0::numeric,
            EXTRACT(EPOCH FROM (now() - COALESCE(cp.launch_at, now())))::numeric / 86400.0
          ) AS age_days,
          COALESCE(v.views_recent, 0) AS views_recent,
          COALESCE(v.views_previous, 0) AS views_previous,
          COALESCE(c.compares_recent, 0) AS compares_recent,
          COALESCE(c.compares_previous, 0) AS compares_previous,
          COALESCE(w.wishlist_recent, 0) AS wishlist_recent,
          GREATEST(
            0::numeric,
            (
              ((COALESCE(v.views_recent, 0) + $4::numeric) / (COALESCE(v.views_previous, 0) + $4::numeric))
              - 1
            )
          ) AS views_velocity_raw,
          GREATEST(
            0::numeric,
            (
              ((COALESCE(c.compares_recent, 0) + $4::numeric) / (COALESCE(c.compares_previous, 0) + $4::numeric))
              - 1
            )
          ) AS compares_velocity_raw
        FROM candidate_products cp
        LEFT JOIN views v
          ON v.product_id = cp.product_id
        LEFT JOIN compares c
          ON c.product_id = cp.product_id
        LEFT JOIN wishlists w
          ON w.product_id = cp.product_id
      ),
      maxed AS (
        SELECT
          product_id,
          age_days,
          views_recent,
          compares_recent,
          wishlist_recent,
          views_velocity_raw,
          compares_velocity_raw,
          MAX(views_recent) OVER () AS max_views_recent,
          MAX(compares_recent) OVER () AS max_compares_recent,
          MAX(wishlist_recent) OVER () AS max_wishlist_recent,
          MAX(views_velocity_raw) OVER () AS max_views_velocity_raw,
          MAX(compares_velocity_raw) OVER () AS max_compares_velocity_raw
        FROM base
      ),
      normalized AS (
        SELECT
          product_id,
          CASE
            WHEN max_views_recent > 0
              THEN (views_recent::numeric / max_views_recent::numeric) * 100
            ELSE 0
          END AS norm_views,
          CASE
            WHEN max_compares_recent > 0
              THEN (compares_recent::numeric / max_compares_recent::numeric) * 100
            ELSE 0
          END AS norm_compares,
          CASE
            WHEN max_wishlist_recent > 0
              THEN (wishlist_recent::numeric / max_wishlist_recent::numeric) * 100
            ELSE 0
          END AS norm_wishlist,
          CASE
            WHEN max_views_velocity_raw > 0
              THEN (views_velocity_raw / max_views_velocity_raw) * 100
            ELSE 0
          END AS norm_views_velocity,
          CASE
            WHEN max_compares_velocity_raw > 0
              THEN (compares_velocity_raw / max_compares_velocity_raw) * 100
            ELSE 0
          END AS norm_compares_velocity,
          GREATEST(
            0::numeric,
            LEAST(
              100::numeric,
              (100::numeric * EXP((-1 * age_days) / $5::numeric))
            )
          ) AS freshness
        FROM maxed
      ),
      scored AS (
        SELECT
          product_id,
          GREATEST(
            0::numeric,
            LEAST(
              100::numeric,
              (
                (norm_views * ${buyerIntentWeights.views}) +
                (norm_compares * ${buyerIntentWeights.compares}) +
                (norm_wishlist * ${buyerIntentWeights.wishlist})
              ) / ${buyerIntentWeights.total}
            )
          ) AS buyer_intent,
          GREATEST(
            0::numeric,
            LEAST(
              100::numeric,
              (
                (norm_views_velocity * ${trendVelocityWeights.views}) +
                (norm_compares_velocity * ${trendVelocityWeights.compares})
              ) / ${trendVelocityWeights.total}
            )
          ) AS trend_velocity,
          freshness
        FROM normalized
      ),
      final_score AS (
        SELECT
          product_id,
          buyer_intent,
          trend_velocity,
          freshness,
          GREATEST(
            0::numeric,
            LEAST(
              100::numeric,
              (
                (buyer_intent * ${hookScoreWeights.buyer_intent}) +
                (trend_velocity * ${hookScoreWeights.trend_velocity}) +
                (freshness * ${hookScoreWeights.freshness})
              ) / ${hookScoreWeights.total}
            )
          ) AS hook_score
        FROM scored
      )
      INSERT INTO product_dynamic_score (
        product_id,
        buyer_intent,
        trend_velocity,
        freshness,
        hook_score,
        calculated_at
      )
      SELECT
        product_id,
        buyer_intent,
        trend_velocity,
        freshness,
        hook_score,
        now()
      FROM final_score
      ON CONFLICT (product_id)
      DO UPDATE SET
        buyer_intent = EXCLUDED.buyer_intent,
        trend_velocity = EXCLUDED.trend_velocity,
        freshness = EXCLUDED.freshness,
        hook_score = EXCLUDED.hook_score,
        calculated_at = now()
      RETURNING product_id;
    `;

    const result = await client.query(sql, [
      productType,
      days,
      lookbackDays,
      smoothing,
      freshnessHalfLifeDays,
    ]);

    return {
      ok: true,
      skipped: false,
      product_type: productType,
      updated: result.rowCount || 0,
      days,
    };
  } finally {
    if (locked) {
      try {
        await client.query("SELECT pg_advisory_unlock($1)", [lockKey]);
      } catch (_) {
        // ignore unlock errors
      }
    }
    client.release();
  }
}

async function recomputeProductDynamicScoreSmartphones(db, opts = {}) {
  return recomputeProductDynamicScoreByType(db, "smartphone", opts);
}

async function recomputeProductDynamicScoreLaptops(db, opts = {}) {
  return recomputeProductDynamicScoreByType(db, "laptop", opts);
}

async function recomputeProductDynamicScoreTVs(db, opts = {}) {
  return recomputeProductDynamicScoreByType(db, "tv", opts);
}

async function recomputeProductDynamicScore(db, opts = {}) {
  const smartphones = await recomputeProductDynamicScoreSmartphones(
    db,
    opts.smartphone || opts,
  );
  const laptops = await recomputeProductDynamicScoreLaptops(
    db,
    opts.laptop || opts,
  );
  const tvs = await recomputeProductDynamicScoreTVs(
    db,
    opts.tv || opts.tvs || opts,
  );
  return {
    ok: true,
    updated:
      (smartphones.updated || 0) + (laptops.updated || 0) + (tvs.updated || 0),
    results: {
      smartphones,
      laptops,
      tvs,
    },
  };
}

module.exports = {
  recomputeProductDynamicScoreByType,
  recomputeProductDynamicScoreSmartphones,
  recomputeProductDynamicScoreLaptops,
  recomputeProductDynamicScoreTVs,
  recomputeProductDynamicScore,
};
