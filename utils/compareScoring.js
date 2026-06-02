const WEIGHT_KEYS = ["performance", "display", "camera", "battery", "priceValue"];

const DEFAULT_COMPARE_WEIGHTS = Object.freeze({
  performance: 0.36,
  display: 0.2,
  camera: 0.2,
  battery: 0.14,
  priceValue: 0.1,
});

const DEFAULT_CHIPSET_RULES = Object.freeze([
  { keyword: "snapdragon 8 elite", score: 100 },
  { keyword: "snapdragon 8 gen 4", score: 98 },
  { keyword: "dimensity 9400", score: 97 },
  { keyword: "a18 pro", score: 98 },
  { keyword: "apple a18", score: 98 },
  { keyword: "snapdragon 8 gen 3", score: 95 },
  { keyword: "dimensity 9300", score: 92 },
  { keyword: "a17 pro", score: 98 },
  { keyword: "snapdragon 8 gen 2", score: 89 },
  { keyword: "dimensity 9200", score: 89 },
  { keyword: "apple a16", score: 89 },
  { keyword: "snapdragon 7 gen 3", score: 75 },
  { keyword: "dimensity 8300", score: 75 },
  { keyword: "snapdragon 7", score: 72 },
  { keyword: "dimensity 8", score: 72 },
  { keyword: "tensor g3", score: 74 },
  { keyword: "tensor g2", score: 72 },
  { keyword: "snapdragon 6", score: 62 },
  { keyword: "dimensity 7", score: 62 },
  { keyword: "exynos 13", score: 62 },
  { keyword: "snapdragon 4", score: 50 },
  { keyword: "helio", score: 50 },
  { keyword: "unisoc", score: 50 },
  { keyword: "exynos 8", score: 50 },
]);

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

const roundOne = (value) => Math.round((value + Number.EPSILON) * 10) / 10;

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
  if (typeof value === "string") {
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
  }
  return {};
};

const cloneDefaultChipsetRules = () =>
  DEFAULT_CHIPSET_RULES.map((rule) => ({ ...rule }));

const normalizeWeights = (input) => {
  const source = input && typeof input === "object" ? input : {};
  const raw = {};

  for (const key of WEIGHT_KEYS) {
    const value = toFiniteNumber(
      source[key] ?? (key === "priceValue" ? source.price_value : null),
    );
    raw[key] = value != null && value >= 0 ? value : DEFAULT_COMPARE_WEIGHTS[key];
  }

  let total = WEIGHT_KEYS.reduce((acc, key) => acc + raw[key], 0);
  if (total > 1.5) {
    for (const key of WEIGHT_KEYS) raw[key] = raw[key] / 100;
    total = WEIGHT_KEYS.reduce((acc, key) => acc + raw[key], 0);
  }

  if (!Number.isFinite(total) || total <= 0) {
    return { ...DEFAULT_COMPARE_WEIGHTS };
  }

  const normalized = {};
  for (const key of WEIGHT_KEYS) {
    normalized[key] = clamp(raw[key] / total, 0, 1);
  }

  const sum = WEIGHT_KEYS.reduce((acc, key) => acc + normalized[key], 0);
  if (sum > 0 && sum !== 1) {
    const adjustKey = WEIGHT_KEYS[0];
    normalized[adjustKey] = clamp(normalized[adjustKey] + (1 - sum), 0, 1);
  }

  return normalized;
};

const normalizeChipsetRules = (input) => {
  const source = Array.isArray(input) ? input : [];
  const seen = new Set();
  const rules = [];

  for (const row of source) {
    const keywordRaw = row?.keyword ?? row?.match ?? row?.pattern ?? "";
    const keyword = normalizeText(keywordRaw).slice(0, 120);
    if (!keyword || seen.has(keyword)) continue;

    const scoreRaw = toFiniteNumber(row?.score);
    const score = clamp(
      Math.round(scoreRaw == null ? 60 : Number(scoreRaw)),
      0,
      100,
    );

    seen.add(keyword);
    rules.push({ keyword, score });
  }

  if (!rules.length) return cloneDefaultChipsetRules();
  return rules.slice(0, 200);
};

const weightsToPercent = (weights) => ({
  performance: roundOne((weights.performance || 0) * 100),
  display: roundOne((weights.display || 0) * 100),
  camera: roundOne((weights.camera || 0) * 100),
  battery: roundOne((weights.battery || 0) * 100),
  priceValue: roundOne((weights.priceValue || 0) * 100),
});

const normalizeCompareScoreConfig = (raw) => {
  const source = raw && typeof raw === "object" ? raw : {};
  const weights = normalizeWeights(source.weights || source);
  const chipsetRules = normalizeChipsetRules(
    source.chipset_rules ?? source.chipsetRules ?? source.chipsets ?? [],
  );

  return {
    weights,
    chipsetRules,
  };
};

const scoreChipset = (processorText, chipsetRules) => {
  const text = normalizeText(processorText);
  if (!text) return 45;

  for (const rule of chipsetRules) {
    if (!rule?.keyword) continue;
    if (text.includes(rule.keyword)) return clamp(Number(rule.score) || 0, 0, 100);
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

const extractRefreshRate = (display) => {
  const source = toObject(display);
  const candidates = [
    source.refresh_rate,
    source.refreshRate,
    source.max_refresh_rate,
    source.screen_refresh_rate,
    source.frame_rate,
    source.refresh,
  ];

  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }

  return null;
};

const scoreRefreshRate = (refreshRate) => {
  if (refreshRate == null) return 28;
  const normalized = clamp(refreshRate, 60, 165);
  return Math.round(20 + ((normalized - 60) / (165 - 60)) * 40);
};

const detectPanelScore = (display) => {
  const source = toObject(display);
  const text = normalizeText(
    source.panel_type || source.panel || source.type || source.technology,
  );
  if (!text) return 20;
  if (text.includes("ltpo")) return 40;
  if (text.includes("amoled")) return 34;
  if (text.includes("oled")) return 32;
  if (text.includes("mini led") || text.includes("mini-led")) return 33;
  if (text.includes("ips") || text.includes("lcd")) return 24;
  if (text.includes("tft")) return 16;
  return 22;
};

const collectMegapixelValues = (value, bucket) => {
  if (value == null) return;
  if (typeof value === "number" && Number.isFinite(value)) {
    bucket.push(value);
    return;
  }
  if (typeof value === "string") {
    const matches = value.match(/(\d+(?:\.\d+)?)\s*mp/gi);
    if (matches) {
      matches.forEach((entry) => {
        const n = toFiniteNumber(entry);
        if (n != null) bucket.push(n);
      });
      return;
    }
    const n = toFiniteNumber(value);
    if (n != null && n <= 250) bucket.push(n);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectMegapixelValues(item, bucket));
    return;
  }
  if (typeof value === "object") {
    Object.values(value).forEach((nested) => collectMegapixelValues(nested, bucket));
  }
};

const extractMainMegapixel = (camera) => {
  const source = toObject(camera);
  const values = [];
  collectMegapixelValues(source.main_camera_megapixels, values);
  collectMegapixelValues(source.main, values);
  collectMegapixelValues(source.rear_camera, values);
  collectMegapixelValues(source.primary, values);
  if (!values.length) return null;
  return Math.max(...values);
};

const countCameraSensors = (camera) => {
  const source = toObject(camera);
  const rear = source.rear_camera;
  if (rear && typeof rear === "object" && !Array.isArray(rear)) {
    return Object.entries(rear).filter(([, val]) => val != null && val !== "").length;
  }
  if (Array.isArray(rear)) return rear.filter(Boolean).length;

  const fallback = [
    source.main,
    source.ultra_wide,
    source.telephoto,
    source.periscope,
    source.macro,
    source.depth,
  ].filter((value) => value != null && value !== "").length;

  return fallback > 0 ? fallback : 1;
};

const extractBatteryCapacity = (battery) => {
  const source = toObject(battery);
  const candidates = [
    source.battery_capacity_mah,
    source.capacity_mah,
    source.capacity,
    source.mAh,
    source.value,
  ];

  for (const candidate of candidates) {
    const value = toFiniteNumber(candidate);
    if (value != null) return value;
  }

  return null;
};

const scoreBatteryCapacity = (capacity) => {
  if (capacity == null) return 35;
  if (capacity <= 3000) return 25;
  if (capacity <= 4000) return 45;
  if (capacity <= 4500) return 60;
  if (capacity <= 5000) return 75;
  if (capacity <= 5500) return 86;
  if (capacity <= 6000) return 94;
  return 100;
};

const pickVariant = (variants, selection) => {
  const list = Array.isArray(variants) ? variants : [];
  if (!list.length) return null;

  const variantId = Number(selection?.variant_id ?? selection?.variantId);
  if (Number.isInteger(variantId) && variantId > 0) {
    const byId = list.find((variant) => Number(variant?.id) === variantId);
    if (byId) return byId;
  }

  const variantIndex = Number(selection?.variant_index ?? selection?.variantIndex);
  if (Number.isInteger(variantIndex) && variantIndex >= 0) {
    return list[variantIndex] || list[0];
  }

  return list[0];
};

const extractPrice = (device, variantSelection = {}) => {
  const productId = String(device?.product_id ?? device?.id ?? "");
  const selection = variantSelection[productId] || null;
  const variants = Array.isArray(device?.variants) ? device.variants : [];
  const selectedVariant = pickVariant(variants, selection);

  const selectedVariantPrice = toFiniteNumber(
    selectedVariant?.base_price ?? selectedVariant?.price,
  );
  if (selectedVariantPrice != null && selectedVariantPrice > 0) {
    return selectedVariantPrice;
  }

  const minVariantPrice = variants.reduce((acc, variant) => {
    const candidate = toFiniteNumber(variant?.base_price ?? variant?.price);
    if (candidate == null || candidate <= 0) return acc;
    if (acc == null) return candidate;
    return Math.min(acc, candidate);
  }, null);

  if (minVariantPrice != null) return minVariantPrice;

  const devicePrice = toFiniteNumber(device?.min_price ?? device?.price);
  if (devicePrice != null && devicePrice > 0) return devicePrice;

  return null;
};

const extractProcessorText = (device) => {
  const performance = toObject(device?.performance);
  const cpu = toObject(device?.cpu);

  const candidates = [
    performance.processor,
    performance.chipset,
    performance.soc,
    performance.cpu,
    cpu.processor,
    cpu.chipset,
    cpu.model,
    cpu.name,
    device?.processor,
  ];

  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (text) return text;
  }

  return "";
};

const buildCompareRanking = (devices = [], variantSelection = {}, config = {}) => {
  const normalizedConfig = normalizeCompareScoreConfig(config);
  const weights = normalizedConfig.weights;
  const chipsetRules = normalizedConfig.chipsetRules;

  const scored = (devices || []).map((device) => {
    const processorText = extractProcessorText(device);
    const chipsetScore = scoreChipset(processorText, chipsetRules);

    const display = toObject(device?.display);
    const refreshRate = extractRefreshRate(display);
    const refreshRateScore = scoreRefreshRate(refreshRate);
    const panelScore = detectPanelScore(display);
    const displayScore = clamp(refreshRateScore + panelScore, 0, 100);

    const camera = toObject(device?.camera);
    const mainMegapixel = extractMainMegapixel(camera);
    const cameraSensors = countCameraSensors(camera);
    const megapixelScore =
      mainMegapixel == null ? 24 : clamp((mainMegapixel / 108) * 65, 18, 65);
    const sensorScore = clamp(cameraSensors * 8.75, 10, 35);
    const cameraScore = clamp(megapixelScore + sensorScore, 0, 100);

    const batteryCapacity = extractBatteryCapacity(device?.battery);
    const batteryScore = scoreBatteryCapacity(batteryCapacity);

    const baseSpecScore =
      chipsetScore * 0.4 +
      displayScore * 0.2 +
      cameraScore * 0.25 +
      batteryScore * 0.15;

    const price = extractPrice(device, variantSelection);
    const valueRaw = price && price > 0 ? baseSpecScore / price : null;

    return {
      productId: String(device?.product_id ?? device?.id ?? ""),
      deviceName: String(device?.name || device?.model || "Device"),
      price,
      valueRaw,
      breakdown: {
        performance: roundOne(chipsetScore),
        display: roundOne(displayScore),
        camera: roundOne(cameraScore),
        battery: roundOne(batteryScore),
      },
    };
  });

  const validValueRows = scored.filter((row) => row.valueRaw != null);
  const minValue = validValueRows.length
    ? Math.min(...validValueRows.map((row) => row.valueRaw))
    : null;
  const maxValue = validValueRows.length
    ? Math.max(...validValueRows.map((row) => row.valueRaw))
    : null;

  const scoredWithValue = scored.map((row) => {
    let valueScore = 50;
    if (row.valueRaw == null) {
      valueScore = 45;
    } else if (minValue != null && maxValue != null) {
      if (maxValue === minValue) valueScore = 70;
      else valueScore = 35 + ((row.valueRaw - minValue) / (maxValue - minValue)) * 65;
    }

    const totalScore =
      row.breakdown.performance * weights.performance +
      row.breakdown.display * weights.display +
      row.breakdown.camera * weights.camera +
      row.breakdown.battery * weights.battery +
      roundOne(clamp(valueScore, 0, 100)) * weights.priceValue;

    return {
      ...row,
      overallScore: roundOne(totalScore),
    };
  });

  const ranked = [...scoredWithValue].sort((a, b) => {
    if (b.overallScore !== a.overallScore) return b.overallScore - a.overallScore;
    const aPrice = a.price ?? Number.POSITIVE_INFINITY;
    const bPrice = b.price ?? Number.POSITIVE_INFINITY;
    if (aPrice !== bPrice) return aPrice - bPrice;
    return a.deviceName.localeCompare(b.deviceName);
  });

  return ranked.map((row, index) => ({
    ...row,
    rank: index + 1,
  }));
};

module.exports = {
  DEFAULT_COMPARE_WEIGHTS,
  DEFAULT_CHIPSET_RULES,
  normalizeWeights,
  normalizeChipsetRules,
  normalizeCompareScoreConfig,
  buildCompareRanking,
  weightsToPercent,
};
