"use strict";

const DEFAULT_SMOOTHING = 5;
const DEFAULT_WEIGHTS = {
  buyerIntent: 0.35,
  trendVelocity: 0.25,
  freshness: 0.2,
};

function normalizeWeights(weights) {
  const w = { ...DEFAULT_WEIGHTS, ...(weights || {}) };
  const buyerIntent = Number(w.buyerIntent);
  const trendVelocity = Number(w.trendVelocity);
  const freshness = Number(w.freshness);

  const safe = (n) => (Number.isFinite(n) && n >= 0 ? n : 0);
  const bw = safe(buyerIntent);
  const tw = safe(trendVelocity);
  const fw = safe(freshness);
  const total = bw + tw + fw;

  if (total <= 0) {
    return { ...DEFAULT_WEIGHTS, total: 0.8 };
  }

  return { buyerIntent: bw, trendVelocity: tw, freshness: fw, total };
}

async function recomputeProductDynamicScoreSmartphones(db, opts = {}) {
  if (!db || typeof db.connect !== "function") {
    throw new Error("recomputeProductDynamicScoreSmartphones: db.connect required");
  }

  const smoothingRaw = Number(
    opts.smoothing ?? process.env.HOOK_SCORE_SMOOTHING ?? DEFAULT_SMOOTHING,
  );
  const smoothing =
    Number.isFinite(smoothingRaw) && smoothingRaw > 0 ? smoothingRaw : DEFAULT_SMOOTHING;

  const weights = normalizeWeights(opts.weights);
  const weightTotal = weights.total || 0.8;

  // Advisory lock must be held on a single DB session (client)
  const lockKeyRaw = Number(process.env.HOOK_SCORE_LOCK_KEY ?? 84626043);
  const lockKey =
    Number.isFinite(lockKeyRaw) && Math.abs(lockKeyRaw) <= 2147483647
      ? Math.trunc(lockKeyRaw)
      : 84626043;

  const client = await db.connect();
  let locked = false;

  try {
    const lockRes = await client.query(
      "SELECT pg_try_advisory_lock($1) AS locked",
      [lockKey],
    );
    locked = Boolean(lockRes.rows?.[0]?.locked);
    if (!locked) {
      return { ok: true, skipped: true, reason: "lock_unavailable", updated: 0 };
    }

    const recomputeSql = `
      WITH views AS (
        SELECT
          product_id,
          COUNT(*) FILTER (WHERE viewed_at >= now() - interval '7 days')::int AS views_7d,
          COUNT(*) FILTER (
            WHERE viewed_at >= now() - interval '14 days'
              AND viewed_at <  now() - interval '7 days'
          )::int AS views_prev_7d
        FROM product_views
        WHERE viewed_at >= now() - interval '14 days'
        GROUP BY product_id
      ),
      comparisons AS (
        SELECT product_id, COUNT(*)::int AS compares_7d
        FROM (
          SELECT product_id
          FROM product_comparisons
          WHERE compared_at >= now() - interval '7 days'
          UNION ALL
          SELECT compared_with AS product_id
          FROM product_comparisons
          WHERE compared_at >= now() - interval '7 days'
        ) t
        GROUP BY product_id
      ),
      base AS (
        SELECT
          p.id AS product_id,
          COALESCE(v.views_7d, 0) AS views_7d,
          COALESCE(v.views_prev_7d, 0) AS views_prev_7d,
          COALESCE(c.compares_7d, 0) AS compares_7d,
          (CURRENT_DATE - COALESCE(s.launch_date, p.created_at::date))::int AS days_since_launch,
          CASE
            WHEN (CURRENT_DATE - COALESCE(s.launch_date, p.created_at::date)) <= 30 THEN 100
            WHEN (CURRENT_DATE - COALESCE(s.launch_date, p.created_at::date)) <= 90 THEN 60
            WHEN (CURRENT_DATE - COALESCE(s.launch_date, p.created_at::date)) <= 180 THEN 30
            ELSE 10
          END::numeric AS freshness,
          (
            (COALESCE(v.views_7d, 0) * 0.4) +
            (COALESCE(c.compares_7d, 0) * 0.6)
          )::numeric AS buyer_intent_raw,
          GREATEST(
            0,
            (
              ((COALESCE(v.views_7d, 0) + $1)::numeric / (COALESCE(v.views_prev_7d, 0) + $1)::numeric)
              - 1
            )
          ) AS trend_velocity_raw
        FROM products p
        INNER JOIN smartphones s ON s.product_id = p.id
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN views v ON v.product_id = p.id
        LEFT JOIN comparisons c ON c.product_id = p.id
        WHERE p.product_type = 'smartphone'
      ),
      scored AS (
        SELECT
          product_id,
          freshness,
          buyer_intent_raw,
          trend_velocity_raw,
          MAX(buyer_intent_raw) OVER () AS max_buyer_intent_raw,
          MAX(trend_velocity_raw) OVER () AS max_trend_velocity_raw
        FROM base
      ),
      normalized AS (
        SELECT
          product_id,
          CASE
            WHEN max_buyer_intent_raw > 0
              THEN (buyer_intent_raw / max_buyer_intent_raw) * 100
            ELSE 0
          END AS buyer_intent,
          CASE
            WHEN max_trend_velocity_raw > 0
              THEN (trend_velocity_raw / max_trend_velocity_raw) * 100
            ELSE 0
          END AS trend_velocity,
          freshness
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
        (
          (
            (buyer_intent * ${weights.buyerIntent}) +
            (trend_velocity * ${weights.trendVelocity}) +
            (freshness * ${weights.freshness})
          ) / ${weightTotal}
        ) AS hook_score,
        now()
      FROM normalized
      ON CONFLICT (product_id)
      DO UPDATE SET
        buyer_intent = EXCLUDED.buyer_intent,
        trend_velocity = EXCLUDED.trend_velocity,
        freshness = EXCLUDED.freshness,
        hook_score = EXCLUDED.hook_score,
        calculated_at = now()
      RETURNING product_id;
    `;

    const result = await client.query(recomputeSql, [smoothing]);
    return { ok: true, skipped: false, updated: result.rowCount || 0 };
  } finally {
    if (locked) {
      try {
        await client.query("SELECT pg_advisory_unlock($1)", [lockKey]);
      } catch (err) {
        // ignore unlock errors
      }
    }
    client.release();
  }
}

module.exports = { recomputeProductDynamicScoreSmartphones };

