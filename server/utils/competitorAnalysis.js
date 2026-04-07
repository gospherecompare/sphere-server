const SCORE_WEIGHTS = Object.freeze({
  specSimilarity: 0.5,
  priceProximity: 0.3,
  compareFrequency: 0.2,
});

const SPEC_SIMILARITY_WEIGHTS = Object.freeze({
  processor: 0.4,
  battery: 0.2,
  display: 0.2,
  camera: 0.2,
});

const PROCESSOR_SCORE_RULES = Object.freeze([
  { pattern: /snapdragon\s*8\s*elite|8\s*elite/i, score: 100 },
  { pattern: /snapdragon\s*8\s*gen\s*4/i, score: 98 },
  { pattern: /snapdragon\s*8\s*gen\s*3/i, score: 95 },
  { pattern: /dimensity\s*9400/i, score: 97 },
  { pattern: /dimensity\s*9300/i, score: 92 },
  { pattern: /apple\s*a18|a18\s*pro|a17\s*pro/i, score: 98 },
  { pattern: /snapdragon\s*8\s*gen\s*2|dimensity\s*9200|apple\s*a16/i, score: 89 },
  { pattern: /snapdragon\s*7\s*gen\s*3|dimensity\s*8300/i, score: 75 },
  { pattern: /snapdragon\s*7|dimensity\s*8|tensor\s*g2|tensor\s*g3/i, score: 72 },
  { pattern: /snapdragon\s*6|dimensity\s*7|exynos\s*13/i, score: 62 },
  { pattern: /snapdragon\s*4|helio|unisoc|exynos\s*8/i, score: 50 },
]);

const FEATURE_RULES = Object.freeze([
  {
    key: "processor_score",
    label: "Processor Tier",
    higherIsBetter: true,
    toleranceAbs: 3,
    impact: 1,
    unit: "",
  },
  {
    key: "display_brightness_nits",
    label: "Brightness",
    higherIsBetter: true,
    toleranceAbs: 120,
    impact: 0.95,
    unit: "nits",
  },
  {
    key: "main_camera_mp",
    label: "Rear Camera",
    higherIsBetter: true,
    toleranceAbs: 4,
    impact: 0.9,
    unit: "MP",
  },
  {
    key: "battery_mah",
    label: "Battery Capacity",
    higherIsBetter: true,
    toleranceAbs: 150,
    impact: 0.9,
    unit: "mAh",
  },
  {
    key: "weight_g",
    label: "Weight",
    higherIsBetter: false,
    toleranceAbs: 5,
    impact: 0.8,
    unit: "g",
  },
  {
    key: "charging_watt",
    label: "Charging Speed",
    higherIsBetter: true,
    toleranceAbs: 5,
    impact: 0.8,
    unit: "W",
  },
  {
    key: "storage_gb",
    label: "Storage",
    higherIsBetter: true,
    toleranceAbs: 32,
    impact: 0.65,
    unit: "GB",
  },
  {
    key: "display_size_in",
    label: "Screen Size",
    higherIsBetter: true,
    toleranceAbs: 0.1,
    impact: 0.55,
    unit: "in",
  },
  {
    key: "display_refresh_hz",
    label: "Refresh Rate",
    higherIsBetter: true,
    toleranceAbs: 5,
    impact: 0.65,
    unit: "Hz",
  },
]);

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

const roundOne = (value) =>
  Math.round((Number(value || 0) + Number.EPSILON) * 10) / 10;

const toFiniteNumber = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const match = String(value).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeText = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

const toObject = (value) => {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value !== "string") return {};
  const raw = value.trim();
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed
      : {};
  } catch (err) {
    return {};
  }
};

const compareKey = (leftId, rightId) => {
  const a = Number(leftId);
  const b = Number(rightId);
  if (!Number.isInteger(a) || !Number.isInteger(b)) return "";
  const left = Math.min(a, b);
  const right = Math.max(a, b);
  return `${left}:${right}`;
};

const collectNumbers = (value, bucket = []) => {
  if (value == null) return bucket;
  if (typeof value === "number" && Number.isFinite(value)) {
    bucket.push(value);
    return bucket;
  }
  if (typeof value === "string") {
    const matches = value.match(/-?\d+(?:\.\d+)?/g);
    if (matches) {
      for (const item of matches) {
        const n = Number(item);
        if (Number.isFinite(n)) bucket.push(n);
      }
    }
    return bucket;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectNumbers(item, bucket);
    return bucket;
  }
  if (typeof value === "object") {
    for (const nested of Object.values(value)) collectNumbers(nested, bucket);
  }
  return bucket;
};

const resolvePath = (obj, path) => {
  if (!obj || typeof obj !== "object" || !path) return undefined;
  const parts = String(path).split(".");
  let current = obj;
  for (const part of parts) {
    if (current == null || typeof current !== "object") return undefined;
    current = current[part];
  }
  return current;
};

const pickFirstNumber = (obj, paths = []) => {
  for (const path of paths) {
    const raw = resolvePath(obj, path);
    const parsed = toFiniteNumber(raw);
    if (parsed != null) return parsed;
  }
  return null;
};

const pickLargestNumber = (value, min = null, max = null) => {
  const nums = collectNumbers(value, []);
  const filtered = nums.filter((n) => {
    if (min != null && n < min) return false;
    if (max != null && n > max) return false;
    return true;
  });
  if (!filtered.length) return null;
  return Math.max(...filtered);
};

const parseStorageGb = (value) => {
  const direct = toFiniteNumber(value);
  if (direct != null) {
    if (direct >= 16 && direct <= 4096) return direct;
  }

  const text = String(value || "").toLowerCase();
  if (!text) return null;
  const tbMatch = text.match(/(\d+(?:\.\d+)?)\s*tb/);
  if (tbMatch) {
    const n = Number(tbMatch[1]);
    if (Number.isFinite(n)) return n * 1024;
  }

  const gbMatch = text.match(/(\d+(?:\.\d+)?)\s*gb/);
  if (gbMatch) {
    const n = Number(gbMatch[1]);
    if (Number.isFinite(n)) return n;
  }

  const fallback = toFiniteNumber(text);
  if (fallback != null && fallback >= 16 && fallback <= 4096) return fallback;
  return null;
};

const scoreProcessorTier = (processorText) => {
  const text = normalizeText(processorText);
  if (!text) return 45;

  for (const rule of PROCESSOR_SCORE_RULES) {
    if (rule.pattern.test(text)) return rule.score;
  }

  const snapdragonGenMatch = text.match(/snapdragon\s*([0-9])\s*gen\s*([0-9]+)/i);
  if (snapdragonGenMatch) {
    const series = Number(snapdragonGenMatch[1]);
    const gen = Number(snapdragonGenMatch[2]);
    return clamp(50 + series * 8 + gen * 2, 52, 96);
  }

  const dimensityMatch = text.match(/dimensity\s*([0-9]{4})/i);
  if (dimensityMatch) {
    const model = Number(dimensityMatch[1]);
    if (model >= 9400) return 97;
    if (model >= 9300) return 92;
    if (model >= 8300) return 78;
    if (model >= 8200) return 74;
    if (model >= 7300) return 66;
    return 58;
  }

  const appleMatch = text.match(/apple\s*a([0-9]{2})|a([0-9]{2})\s*(pro|bionic)?/i);
  if (appleMatch) {
    const chipNum = Number(appleMatch[1] || appleMatch[2]);
    return clamp(74 + (chipNum - 14) * 4, 70, 99);
  }

  if (text.includes("tensor")) return 74;
  if (text.includes("exynos")) return 68;
  return 60;
};

const scoreDisplayComposite = (profile) => {
  const refreshScore =
    profile.display_refresh_hz == null
      ? 55
      : clamp(((profile.display_refresh_hz - 60) / (165 - 60)) * 100, 0, 100);

  const brightnessScore =
    profile.display_brightness_nits == null
      ? 50
      : clamp(
          ((profile.display_brightness_nits - 300) / (6000 - 300)) * 100,
          0,
          100,
        );

  const sizeScore =
    profile.display_size_in == null
      ? 50
      : clamp(((profile.display_size_in - 5) / (8 - 5)) * 100, 0, 100);

  return roundOne(refreshScore * 0.5 + brightnessScore * 0.35 + sizeScore * 0.15);
};

const extractMainCameraMp = (camera) => {
  const source = toObject(camera);

  const direct = pickFirstNumber(source, [
    "main_camera_megapixels",
    "main.megapixels",
    "main.resolution_mp",
    "primary.megapixels",
    "rear_camera.main.megapixels",
    "rear_camera.main.resolution_mp",
  ]);
  if (direct != null) return direct;

  const fromRear = pickLargestNumber(source.rear_camera, 1, 250);
  if (fromRear != null) return fromRear;

  return pickLargestNumber(source, 1, 250);
};

const extractBatteryMah = (battery) => {
  const source = toObject(battery);
  const direct = pickFirstNumber(source, [
    "battery_capacity_mah",
    "capacity_mah",
    "capacity_mAh",
    "battery_capacity",
    "capacity",
    "mAh",
  ]);
  if (direct != null) return direct;
  return pickLargestNumber(source, 300, 15000);
};

const extractChargingWatt = (battery) => {
  const source = toObject(battery);
  const direct = pickFirstNumber(source, [
    "fast_charging_watt",
    "charging_watt",
    "charging_power",
    "fast_charging",
    "wired_charging",
  ]);
  if (direct != null) return direct;
  return pickLargestNumber(source.fast_charging, 5, 350);
};

const extractDisplayRefreshHz = (display) => {
  const source = toObject(display);
  const direct = pickFirstNumber(source, [
    "refresh_rate",
    "refreshRate",
    "max_refresh_rate",
    "screen_refresh_rate",
    "frame_rate",
  ]);
  if (direct != null) return direct;
  return pickLargestNumber(source.refresh_rate, 30, 240);
};

const extractDisplaySizeIn = (display) => {
  const source = toObject(display);
  const direct = pickFirstNumber(source, ["size", "screen_size", "display_size"]);
  if (direct != null) return direct;
  return pickLargestNumber(source, 3, 12);
};

const extractDisplayBrightnessNits = (display) => {
  const source = toObject(display);
  const direct = pickFirstNumber(source, [
    "peak_brightness_nits",
    "max_brightness_nits",
    "typical_brightness_nits",
    "brightness_nits",
  ]);
  if (direct != null) return direct;

  return pickLargestNumber(source.brightness || source, 100, 12000);
};

const extractWeightGrams = (buildDesign) => {
  const source = toObject(buildDesign);
  const direct = pickFirstNumber(source, [
    "weight_g",
    "weight_grams",
    "weight",
    "body.weight",
  ]);
  if (direct != null) return direct;
  return pickLargestNumber(source.weight || source, 80, 500);
};

const computeBestPrice = (row) => {
  const store = toFiniteNumber(row.min_store_price);
  const base = toFiniteNumber(row.min_base_price);
  if (store != null && base != null) return Math.min(store, base);
  if (store != null) return store;
  if (base != null) return base;
  return null;
};

const buildProfile = (row) => {
  const display = toObject(row.display);
  const performance = toObject(row.performance);
  const camera = toObject(row.camera);
  const battery = toObject(row.battery);
  const buildDesign = toObject(row.build_design);

  const processorText = String(
    performance.processor ||
      performance.chipset ||
      performance.cpu ||
      performance.soc ||
      "",
  ).trim();

  const storageFromVariant = toFiniteNumber(row.max_variant_storage_gb);
  const storageFromPerformance = parseStorageGb(performance.storage);
  const storageGb =
    storageFromVariant != null
      ? storageFromVariant
      : storageFromPerformance != null
        ? storageFromPerformance
        : null;

  const profile = {
    id: Number(row.product_id),
    name: row.name || "Smartphone",
    brand_name: row.brand_name || null,
    category: row.category || null,
    image_url: row.image_url || null,
    hook_score: toFiniteNumber(row.hook_score) || 0,
    price: computeBestPrice(row),
    processor_text: processorText || null,
    processor_score: scoreProcessorTier(processorText),
    display_refresh_hz: extractDisplayRefreshHz(display),
    display_size_in: extractDisplaySizeIn(display),
    display_brightness_nits: extractDisplayBrightnessNits(display),
    main_camera_mp: extractMainCameraMp(camera),
    battery_mah: extractBatteryMah(battery),
    charging_watt: extractChargingWatt(battery),
    weight_g: extractWeightGrams(buildDesign),
    storage_gb: storageGb,
  };

  profile.display_score = scoreDisplayComposite(profile);
  return profile;
};

const similarityFromDistance = (left, right, scale, fallback = 50) => {
  if (left == null || right == null) return fallback;
  const distance = Math.abs(left - right);
  const score = 100 - (distance / Math.max(scale, 1)) * 100;
  return clamp(score, 0, 100);
};

const calculateSpecSimilarityScore = (base, candidate) => {
  const processorSimilarity = similarityFromDistance(
    base.processor_score,
    candidate.processor_score,
    40,
    50,
  );
  const batterySimilarity = similarityFromDistance(
    base.battery_mah,
    candidate.battery_mah,
    2500,
    50,
  );
  const displaySimilarity = similarityFromDistance(
    base.display_score,
    candidate.display_score,
    40,
    55,
  );
  const cameraSimilarity = similarityFromDistance(
    base.main_camera_mp,
    candidate.main_camera_mp,
    150,
    50,
  );

  return roundOne(
    processorSimilarity * SPEC_SIMILARITY_WEIGHTS.processor +
      batterySimilarity * SPEC_SIMILARITY_WEIGHTS.battery +
      displaySimilarity * SPEC_SIMILARITY_WEIGHTS.display +
      cameraSimilarity * SPEC_SIMILARITY_WEIGHTS.camera,
  );
};

const calculatePriceProximityScore = (basePrice, candidatePrice) => {
  if (basePrice == null || candidatePrice == null) return 40;
  const distance = Math.abs(basePrice - candidatePrice);
  const tolerance = Math.max(basePrice * 0.22, 5000);
  return roundOne(clamp(100 - (distance / tolerance) * 100, 0, 100));
};

const formatFeatureValue = (rule, value) => {
  if (value == null) return "N/A";
  if (rule.unit === "in") return `${roundOne(value)} in`;
  if (!rule.unit) return `${Math.round(value)}`;
  return `${Math.round(value)} ${rule.unit}`;
};

const buildFeatureInsights = (base, candidate) => {
  const advantages = [];
  const disadvantages = [];
  const common = [];

  for (const rule of FEATURE_RULES) {
    const left = base[rule.key];
    const right = candidate[rule.key];
    if (left == null || right == null) continue;

    const distance = Math.abs(right - left);
    const normalizedImpact =
      (distance / Math.max(Math.abs(left), Math.abs(right), 1)) * rule.impact;

    if (distance <= rule.toleranceAbs) {
      if (rule.key === "processor_score") {
        common.push({
          text: "Similar processor tier",
          impact: normalizedImpact,
        });
      } else {
        common.push({
          text: `${rule.label}: ${formatFeatureValue(rule, right)}`,
          impact: normalizedImpact,
        });
      }
      continue;
    }

    const candidateBetter = rule.higherIsBetter ? right > left : right < left;
    if (rule.key === "processor_score") {
      const text = candidateBetter
        ? `Stronger processor tier (${candidate.processor_text || "higher tier"})`
        : "Weaker processor tier";
      (candidateBetter ? advantages : disadvantages).push({
        text,
        impact: normalizedImpact,
      });
      continue;
    }

    const prefix = candidateBetter
      ? rule.higherIsBetter
        ? "Higher"
        : "Lower"
      : rule.higherIsBetter
        ? "Lower"
        : "Higher";

    const text = `${prefix} ${rule.label}: ${formatFeatureValue(
      rule,
      right,
    )} vs ${formatFeatureValue(rule, left)}`;

    (candidateBetter ? advantages : disadvantages).push({
      text,
      impact: normalizedImpact,
    });
  }

  const sortByImpact = (items) =>
    [...items]
      .sort((a, b) => b.impact - a.impact)
      .slice(0, 3)
      .map((item) => item.text);

  return {
    advantages: sortByImpact(advantages),
    disadvantages: sortByImpact(disadvantages),
    common_features: sortByImpact(common),
  };
};

const buildReason = (base, candidate, scores) => {
  const parts = [];
  if (scores.spec_similarity_score >= 70) {
    parts.push("Very similar processor and core specs");
  } else if (scores.spec_similarity_score >= 55) {
    parts.push("Good spec similarity");
  }

  if (scores.price_proximity_score >= 65) {
    parts.push("Close in price range");
  }

  if (scores.compare_frequency_score >= 50) {
    parts.push("Frequently compared by users");
  }

  if (base.category && candidate.category && base.category === candidate.category) {
    parts.push("Same segment");
  }

  if (!parts.length) {
    parts.push("Balanced competitor across price and specs");
  }

  return parts.slice(0, 2).join(" • ");
};

const shouldIncludeCandidate = (base, candidate, specSimilarity, compareCount) => {
  if (base.id === candidate.id) return false;

  const hasBothPrices = base.price != null && candidate.price != null;
  if (!hasBothPrices) return true;

  const priceGap = Math.abs(base.price - candidate.price);
  const wideGap = priceGap > Math.max(base.price * 0.75, 22000);
  if (wideGap && compareCount <= 0 && specSimilarity < 55) return false;

  return true;
};

const buildTopCompetitors = (base, profiles, compareMap, limit = 3) => {
  const peers = profiles.filter((row) => row.id !== base.id);
  const maxCompareCount = peers.reduce((max, peer) => {
    const count = compareMap.get(compareKey(base.id, peer.id)) || 0;
    return Math.max(max, count);
  }, 0);

  const candidates = [];

  for (const peer of peers) {
    const pairCount = compareMap.get(compareKey(base.id, peer.id)) || 0;
    const specSimilarityScore = calculateSpecSimilarityScore(base, peer);
    if (!shouldIncludeCandidate(base, peer, specSimilarityScore, pairCount)) {
      continue;
    }

    const priceProximityScore = calculatePriceProximityScore(base.price, peer.price);
    const compareFrequencyScore =
      maxCompareCount > 0 ? roundOne((pairCount / maxCompareCount) * 100) : 0;

    let competitionScore =
      specSimilarityScore * SCORE_WEIGHTS.specSimilarity +
      priceProximityScore * SCORE_WEIGHTS.priceProximity +
      compareFrequencyScore * SCORE_WEIGHTS.compareFrequency;

    if (base.category && peer.category && base.category === peer.category) {
      competitionScore += 2;
    }

    const featureInsights = buildFeatureInsights(base, peer);
    const roundedCompetition = roundOne(clamp(competitionScore, 0, 100));
    const reason = buildReason(base, peer, {
      spec_similarity_score: specSimilarityScore,
      price_proximity_score: priceProximityScore,
      compare_frequency_score: compareFrequencyScore,
    });

    candidates.push({
      product_id: base.id,
      competitor_id: peer.id,
      competition_score: roundedCompetition,
      spec_similarity_score: specSimilarityScore,
      price_proximity_score: priceProximityScore,
      compare_frequency_score: compareFrequencyScore,
      reason,
      analysis_json: {
        ...featureInsights,
        compare_count: pairCount,
        feature_values: {
          base: {
            processor_score: base.processor_score,
            battery_mah: base.battery_mah,
            display_score: base.display_score,
            main_camera_mp: base.main_camera_mp,
            price: base.price,
          },
          competitor: {
            processor_score: peer.processor_score,
            battery_mah: peer.battery_mah,
            display_score: peer.display_score,
            main_camera_mp: peer.main_camera_mp,
            price: peer.price,
          },
        },
      },
    });
  }

  return candidates
    .sort((a, b) => {
      if (b.competition_score !== a.competition_score) {
        return b.competition_score - a.competition_score;
      }
      if (b.spec_similarity_score !== a.spec_similarity_score) {
        return b.spec_similarity_score - a.spec_similarity_score;
      }
      return a.competitor_id - b.competitor_id;
    })
    .slice(0, limit);
};

const parseIds = (value) => {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map((v) => Number(v))
        .filter((v) => Number.isInteger(v) && v > 0),
    ),
  );
};

const loadPublishedSmartphones = async (db) => {
  const result = await db.query(
    `
      SELECT
        p.id AS product_id,
        p.name,
        b.name AS brand_name,
        s.category,
        s.display,
        s.performance,
        s.camera,
        s.battery,
        s.build_design,
        (
          SELECT MIN(vsp.price)::numeric
          FROM product_variants pv
          INNER JOIN variant_store_prices vsp
            ON vsp.variant_id = pv.id
          WHERE pv.product_id = p.id
            AND vsp.price IS NOT NULL
        ) AS min_store_price,
        (
          SELECT MIN(pv.base_price)::numeric
          FROM product_variants pv
          WHERE pv.product_id = p.id
            AND pv.base_price IS NOT NULL
        ) AS min_base_price,
        (
          SELECT MAX(
            NULLIF(
              regexp_replace(COALESCE(pv.attributes->>'storage', ''), '[^0-9.]', '', 'g'),
              ''
            )::numeric
          )
          FROM product_variants pv
          WHERE pv.product_id = p.id
        ) AS max_variant_storage_gb,
        (
          SELECT pi.image_url
          FROM product_images pi
          WHERE pi.product_id = p.id
          ORDER BY pi.position ASC NULLS LAST, pi.id ASC
          LIMIT 1
        ) AS image_url,
        COALESCE(ds.hook_score, 0) AS hook_score
      FROM products p
      INNER JOIN smartphones s
        ON s.product_id = p.id
      INNER JOIN product_publish pub
        ON pub.product_id = p.id
       AND pub.is_published = true
      LEFT JOIN brands b
        ON b.id = p.brand_id
      LEFT JOIN product_dynamic_score ds
        ON ds.product_id = p.id
      WHERE p.product_type = 'smartphone'
      ORDER BY p.id ASC;
    `,
  );

  return (result.rows || []).map(buildProfile).filter((row) => Number.isInteger(row.id));
};

const loadCompareCounts = async (db) => {
  const result = await db.query(
    `
      SELECT
        LEAST(pc.product_id, pc.compared_with) AS left_id,
        GREATEST(pc.product_id, pc.compared_with) AS right_id,
        COUNT(*)::int AS compare_count
      FROM product_comparisons pc
      INNER JOIN products p1
        ON p1.id = pc.product_id
       AND p1.product_type = 'smartphone'
      INNER JOIN products p2
        ON p2.id = pc.compared_with
       AND p2.product_type = 'smartphone'
      WHERE pc.compared_at >= now() - interval '180 days'
      GROUP BY 1, 2;
    `,
  );

  const map = new Map();
  for (const row of result.rows || []) {
    const key = compareKey(row.left_id, row.right_id);
    if (!key) continue;
    map.set(key, Number(row.compare_count) || 0);
  }
  return map;
};

async function recomputeSmartphoneCompetitorAnalysis(db, options = {}) {
  const limitRaw = Number(options.limit);
  const limit = Number.isFinite(limitRaw) ? clamp(Math.floor(limitRaw), 1, 10) : 3;
  const requestedIds = parseIds(options.productIds);

  const profiles = await loadPublishedSmartphones(db);
  const compareMap = await loadCompareCounts(db);

  const productIds =
    requestedIds.length > 0
      ? requestedIds.filter((id) => profiles.some((row) => row.id === id))
      : profiles.map((row) => row.id);

  if (!productIds.length) {
    return {
      ok: true,
      processed_products: 0,
      updated_products: 0,
      inserted_rows: 0,
      limit,
    };
  }

  const byId = new Map(profiles.map((row) => [row.id, row]));
  const client = await db.connect();
  let updatedProducts = 0;
  let insertedRows = 0;

  try {
    await client.query("BEGIN");

    for (const productId of productIds) {
      const base = byId.get(productId);
      if (!base) continue;

      const competitors = buildTopCompetitors(base, profiles, compareMap, limit);

      await client.query("DELETE FROM competitor_analysis WHERE product_id = $1", [
        productId,
      ]);

      for (const competitor of competitors) {
        await client.query(
          `
            INSERT INTO competitor_analysis (
              product_id,
              competitor_id,
              competition_score,
              spec_similarity_score,
              price_proximity_score,
              compare_frequency_score,
              reason,
              analysis_json,
              computed_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb, now())
            ON CONFLICT (product_id, competitor_id)
            DO UPDATE SET
              competition_score = EXCLUDED.competition_score,
              spec_similarity_score = EXCLUDED.spec_similarity_score,
              price_proximity_score = EXCLUDED.price_proximity_score,
              compare_frequency_score = EXCLUDED.compare_frequency_score,
              reason = EXCLUDED.reason,
              analysis_json = EXCLUDED.analysis_json,
              computed_at = now();
          `,
          [
            competitor.product_id,
            competitor.competitor_id,
            competitor.competition_score,
            competitor.spec_similarity_score,
            competitor.price_proximity_score,
            competitor.compare_frequency_score,
            competitor.reason,
            JSON.stringify(competitor.analysis_json || {}),
          ],
        );
      }

      updatedProducts += 1;
      insertedRows += competitors.length;
    }

    await client.query("COMMIT");
    return {
      ok: true,
      processed_products: productIds.length,
      updated_products: updatedProducts,
      inserted_rows: insertedRows,
      limit,
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackErr) {
      // ignore rollback errors
    }
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  FEATURE_RULES,
  SCORE_WEIGHTS,
  SPEC_SIMILARITY_WEIGHTS,
  recomputeSmartphoneCompetitorAnalysis,
};
