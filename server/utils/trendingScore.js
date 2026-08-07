"use strict";

const DEFAULT_DAYS = 7;
const DEFAULT_SMOOTHING = 5;
const DEFAULT_WEIGHTS = {
  views: 0.4,
  compares: 0.4,
  velocity: 0.2,
};

function normalizeWeights(weights) {
  const w = { ...DEFAULT_WEIGHTS, ...(weights || {}) };
  const views = Number(w.views);
  const compares = Number(w.compares);
  const velocity = Number(w.velocity);

  const safe = (n) => (Number.isFinite(n) && n >= 0 ? n : 0);
  const vw = safe(views);
  const cw = safe(compares);
  const velw = safe(velocity);
  const total = vw + cw + velw;

  if (total <= 0) {
    return { ...DEFAULT_WEIGHTS, total: 1 };
  }

  return { views: vw, compares: cw, velocity: velw, total };
}

async function recomputeProductTrendingScores(db, opts = {}) {
  if (!db || typeof db.connect !== "function") {
    throw new Error("recomputeProductTrendingScores: db.connect required");
  }

  const daysRaw = Number(opts.days ?? process.env.TRENDING_SCORE_DAYS ?? DEFAULT_DAYS);
  const days = Number.isFinite(daysRaw) && daysRaw > 0 ? Math.floor(daysRaw) : DEFAULT_DAYS;
  const lookbackDays = days * 2;

  const smoothingRaw = Number(
    opts.smoothing ?? process.env.TRENDING_SCORE_SMOOTHING ?? DEFAULT_SMOOTHING,
  );
  const smoothing =
    Number.isFinite(smoothingRaw) && smoothingRaw >= 0
      ? smoothingRaw
      : DEFAULT_SMOOTHING;

  const weights = normalizeWeights(opts.weights);
  const weightTotal = weights.total || 1;

  // Advisory lock must be held on a single DB session (client)
  const lockKeyRaw = Number(process.env.TRENDING_SCORE_LOCK_KEY ?? 84626044);
  const lockKey =
    Number.isFinite(lockKeyRaw) && Math.abs(lockKeyRaw) <= 2147483647
      ? Math.trunc(lockKeyRaw)
      : 84626044;

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

    const sql = `
      WITH views AS (
        SELECT
          product_id,
          COUNT(DISTINCT COALESCE(visitor_key, id::text)) FILTER (
            WHERE viewed_at >= now() - ($1::int * interval '1 day')
          )::int AS views_7d,
          COUNT(DISTINCT COALESCE(visitor_key, id::text)) FILTER (
            WHERE viewed_at >= now() - ($2::int * interval '1 day')
              AND viewed_at <  now() - ($1::int * interval '1 day')
          )::int AS views_prev_7d
        FROM product_views
        WHERE viewed_at >= now() - ($2::int * interval '1 day')
        GROUP BY product_id
      ),
      comparisons AS (
        SELECT product_id, COUNT(*)::int AS compares_7d
        FROM (
          SELECT product_id
          FROM product_comparisons
          WHERE compared_at >= now() - ($1::int * interval '1 day')
          UNION ALL
          SELECT compared_with AS product_id
          FROM product_comparisons
          WHERE compared_at >= now() - ($1::int * interval '1 day')
        ) t
        GROUP BY product_id
      ),
      base AS (
        SELECT
          p.id AS product_id,
          p.product_type,
          COALESCE(v.views_7d, 0) AS views_7d,
          COALESCE(v.views_prev_7d, 0) AS views_prev_7d,
          COALESCE(c.compares_7d, 0) AS compares_7d,
          GREATEST(
            0,
            (
              ((COALESCE(v.views_7d, 0) + $3)::numeric / (COALESCE(v.views_prev_7d, 0) + $3)::numeric)
              - 1
            )
          ) AS velocity_raw
        FROM products p
        INNER JOIN product_publish pub
          ON pub.product_id = p.id
         AND pub.is_published = true
        LEFT JOIN views v ON v.product_id = p.id
        LEFT JOIN comparisons c ON c.product_id = p.id
      ),
      maxed AS (
        SELECT
          product_id,
          product_type,
          views_7d,
          compares_7d,
          views_prev_7d,
          velocity_raw,
          MAX(views_7d) OVER (PARTITION BY product_type) AS max_views_7d,
          MAX(compares_7d) OVER (PARTITION BY product_type) AS max_compares_7d,
          MAX(velocity_raw) OVER (PARTITION BY product_type) AS max_velocity_raw
        FROM base
      ),
      normalized AS (
        SELECT
          product_id,
          views_7d,
          compares_7d,
          views_prev_7d,
          velocity_raw AS velocity,
          CASE
            WHEN max_views_7d > 0
              THEN (views_7d::numeric / max_views_7d::numeric) * 100
            ELSE 0
          END AS norm_views,
          CASE
            WHEN max_compares_7d > 0
              THEN (compares_7d::numeric / max_compares_7d::numeric) * 100
            ELSE 0
          END AS norm_compares,
          CASE
            WHEN max_velocity_raw > 0
              THEN (velocity_raw / max_velocity_raw) * 100
            ELSE 0
          END AS norm_velocity
        FROM maxed
      ),
      scored AS (
        SELECT
          product_id,
          views_7d,
          compares_7d,
          views_prev_7d,
          velocity,
          (
            (
              (norm_views * ${weights.views}) +
              (norm_compares * ${weights.compares}) +
              (norm_velocity * ${weights.velocity})
            ) / ${weightTotal}
          ) AS trending_score
        FROM normalized
      )
      INSERT INTO product_trending_score (
        product_id,
        views_7d,
        compares_7d,
        views_prev_7d,
        velocity,
        trending_score,
        calculated_at
      )
      SELECT
        product_id,
        views_7d,
        compares_7d,
        views_prev_7d,
        velocity,
        trending_score,
        now()
      FROM scored
      ON CONFLICT (product_id)
      DO UPDATE SET
        views_7d = EXCLUDED.views_7d,
        compares_7d = EXCLUDED.compares_7d,
        views_prev_7d = EXCLUDED.views_prev_7d,
        velocity = EXCLUDED.velocity,
        trending_score = EXCLUDED.trending_score,
        calculated_at = now()
      RETURNING product_id;
    `;

    const result = await client.query(sql, [days, lookbackDays, smoothing]);
    return { ok: true, skipped: false, updated: result.rowCount || 0, days };
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

module.exports = { recomputeProductTrendingScores };
